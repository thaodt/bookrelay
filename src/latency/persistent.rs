//! Reference A survives candidate replacement. All arrivals share one origin;
//! candidate generations are fenced so trailing frames cannot enter a new trial.
use super::*;

async fn drain_candidate(
    task: &mut tokio::task::JoinHandle<anyhow::Result<FeedReport>>,
    rx: &mut mpsc::Receiver<Event>,
) -> Result<FeedReport, String> {
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        tokio::select! {
            result=&mut *task=>return result.map_err(|e|e.to_string())?.map_err(|e|e.to_string()),
            _=rx.recv()=>{},
            _=tokio::time::sleep_until(deadline)=>{task.abort();let _=task.await;return Err("probe close deadline exceeded".into());},
        }
    }
}

pub(super) async fn run(
    options: Options,
    auth: Arc<Credentials>,
    mut stop: watch::Receiver<bool>,
) -> anyhow::Result<Report> {
    let mut report = new_report(&options);
    let origin = Instant::now();
    let (tx, mut rx) = mpsc::channel(1024);
    let (ref_cancel, ref_stop) = watch::channel(false);
    let mut reference = tokio::spawn(feed(
        0,
        1,
        options.clone(),
        auth.clone(),
        origin,
        None,
        tx.clone(),
        ref_stop,
    ));
    let mut reference_done = false;
    let mut reference_ready = false;
    for index in 0..options.trials {
        if *stop.borrow() || reference_done {
            break;
        }
        let generation = index as u64 + 1;
        let port = if options.ports.is_empty() {
            None
        } else {
            Some(options.ports[index % options.ports.len()])
        };
        let (cancel, candidate_stop) = watch::channel(false);
        let mut candidate = tokio::spawn(feed(
            1,
            generation,
            options.clone(),
            auth.clone(),
            origin,
            port,
            tx.clone(),
            candidate_stop,
        ));
        let trial_started = Instant::now();
        let mut candidate_done = None;
        let mut start = None;
        let mut candidate_ready = false;
        let mut matcher = Matcher::new(options.pending_cap, options.pending_bytes, options.ttl);
        let mut samples = vec![];
        let mut retained = 0;
        let mut failure = None;
        let mut reason = "duration".to_owned();
        loop {
            let deadline = start
                .map(|at: Instant| at + options.duration)
                .unwrap_or(trial_started + Duration::from_secs(60));
            tokio::select! {
                _=stopped(&mut stop)=>{reason="cancelled".into();break;},
                _=tokio::time::sleep_until(deadline)=>{if start.is_none(){failure=Some("candidate/reference initialization timeout".into());}break;},
                done=&mut reference=>{
                    reference_done=true;reason="reference_failure".into();
                    match done {
                        Ok(Ok(row))=>{failure=Some(row.failure.clone().unwrap_or_else(||"persistent reference ended".into()));report.reference=Some(row);},
                        Ok(Err(e))=>failure=Some(e.to_string()),Err(e)=>failure=Some(e.to_string()),
                    }
                    report.reference_failure=failure.clone();break;
                },
                done=&mut candidate=>{
                    reason="candidate_failure".into();candidate_done=Some(done.map_err(|e|e.to_string()).and_then(|r|r.map_err(|e|e.to_string())));
                    failure=Some(match candidate_done.as_ref().unwrap(){Ok(row)=>row.failure.clone().unwrap_or_else(||"candidate ended".into()),Err(e)=>e.clone()});break;
                },
                event=rx.recv()=>match event {
                    Some(Event::Ready(0,1))=>{reference_ready=true;if candidate_ready {start=Some(Instant::now()+options.warmup);}},
                    Some(Event::Ready(1,g)) if g==generation=>{candidate_ready=true;if reference_ready {start=Some(Instant::now()+options.warmup);}},
                    Some(Event::Delta(event))=>{
                        if (event.feed==0 && event.generation!=1) || (event.feed==1 && event.generation!=generation)
                            || start.is_none_or(|start|origin+Duration::from_nanos(event.ns)<start) {continue;}
                        match matcher.push(event) {
                            Ok(matched)=>for sample in matched {
                                let cost=std::mem::size_of::<Sample>()+sample.symbol.capacity()+32;
                                if samples.len()>=options.sample_cap || retained+cost>32<<20 {reason="sample_cap".into();break;}
                                retained+=cost;samples.push(sample);
                            },Err(e)=>{failure=Some(e.to_string());reason="matcher_failure".into();break;},
                        }
                        if reason=="sample_cap" {break;}
                    },Some(_)=>{},None=>{failure=Some("observer channel closed".into());break;},
                }
            }
        }
        let _ = cancel.send(true);
        let done = if let Some(done) = candidate_done {
            done
        } else {
            drain_candidate(&mut candidate, &mut rx).await
        };
        let mut feeds = vec![];
        match done {
            Ok(row) => {
                if let Some(e) = &row.failure {
                    failure.get_or_insert(e.clone());
                }
                feeds.push(row);
            }
            Err(e) => {
                failure.get_or_insert(e);
            }
        }
        matcher.finish();
        let split = start
            .map(|s| s + options.duration / 2)
            .unwrap_or(trial_started);
        let split_ns = split.duration_since(origin).as_nanos() as u64;
        let all: Vec<_> = samples.iter().collect();
        let training = distribution(
            &all.iter()
                .copied()
                .filter(|s| s.a_ns.max(s.b_ns) < split_ns)
                .collect::<Vec<_>>(),
        );
        let holdout = distribution(
            &all.iter()
                .copied()
                .filter(|s| s.a_ns.min(s.b_ns) >= split_ns)
                .collect::<Vec<_>>(),
        );
        let selected = if training.n == 0 || training.p50_ns == 0 {
            None
        } else {
            Some(if training.p50_ns < 0 { "A" } else { "B" }.into())
        };
        let not_retained = matcher.counts.matched.saturating_sub(samples.len() as u64);
        let trial = Trial {
            index,
            candidate_port: port,
            reference_generation: 1,
            candidate_generation: generation,
            elapsed_ms: trial_started.elapsed().as_millis(),
            stop_reason: reason,
            failure,
            feeds,
            counts: matcher.counts,
            all: distribution(&all),
            training,
            holdout,
            selected_on_training: selected,
            retained_sample_bytes: retained,
            matched_not_retained: not_retained,
            samples,
        };
        eprintln!(
            "persistent-reference trial {index}, candidate generation {generation}: pairs={} failure={:?}",
            trial.all.n, trial.failure
        );
        report.trials.push(trial);
        if reference_done || *stop.borrow() {
            break;
        }
        // Continue consuming A between trials; sleeping without draining would
        // eventually overflow the bounded observer queue on a busy market.
        let until = Instant::now() + Duration::from_secs(1);
        loop {
            tokio::select! {
                _=stopped(&mut stop)=>break,
                _=tokio::time::sleep_until(until)=>break,
                _=rx.recv()=>{},
            }
        }
    }
    let _ = ref_cancel.send(true);
    if !reference_done {
        match drain_candidate(&mut reference, &mut rx).await {
            Ok(row) => {
                if row.failure.is_some() {
                    report.reference_failure = row.failure.clone();
                }
                report.reference = Some(row);
            }
            Err(e) => report.reference_failure = Some(e),
        }
    }
    report.rss_bytes = crate::pilot::rss_bytes();
    Ok(report)
}
