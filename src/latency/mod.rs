//! Bounded, same-host application-arrival comparison. No exchange/NIC latency claim.
mod persistent;
use crate::{
    Side,
    auth::Credentials,
    book::OrderBook,
    collector::stopped,
    exchange::{
        Control, ExchangeAdapter,
        kalshi::{Kalshi, SequenceTracker},
    },
    model::{BookEvent, QuantityUpdate},
    transport::{self, ConnectOptions},
};
use anyhow::{Context, ensure};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
    time::{Instant, MissedTickBehavior},
};
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone)]
pub struct Options {
    pub ws_url: String,
    pub symbols: Vec<String>,
    pub trials: usize,
    pub warmup: Duration,
    pub duration: Duration,
    pub ports: Vec<u16>,
    pub connect: ConnectOptions,
    pub sample_cap: usize,
    pub pending_cap: usize,
    pub pending_bytes: usize,
    pub ttl: Duration,
    pub persistent_reference: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Payload {
    side: bool,
    price: i64,
    change: i64,
}
#[derive(Clone)]
struct Arrival {
    feed: usize,
    generation: u64,
    symbol: String,
    clock: u64,
    payload: Payload,
    ns: u64,
}
struct Group {
    clock: u64,
    items: Vec<(Payload, u64)>,
    anchored: bool,
    bytes: usize,
    last: u64,
}
#[derive(Debug, Serialize)]
pub struct Sample {
    pub symbol: String,
    pub clock: u64,
    pub ordinal: usize,
    pub a_ns: u64,
    pub b_ns: u64,
    pub a_minus_b_ns: i64,
}
#[derive(Default, Debug, Serialize)]
pub struct MatchCounts {
    pub matched: u64,
    pub unmatched: u64,
    pub ambiguous: u64,
    pub boundary_censored: u64,
    pub evicted: u64,
    pub pending_peak_bytes: usize,
}

struct Matcher {
    current: HashMap<(usize, String), Group>,
    closed: HashMap<(String, u64), [Option<Group>; 2]>,
    bytes: usize,
    entries: usize,
    cap: usize,
    byte_cap: usize,
    ttl_ns: u64,
    next_expiry_ns: u64,
    counts: MatchCounts,
}
impl Matcher {
    fn new(cap: usize, byte_cap: usize, ttl: Duration) -> Self {
        Self {
            current: HashMap::new(),
            closed: HashMap::new(),
            bytes: 0,
            entries: 0,
            cap,
            byte_cap,
            ttl_ns: ttl.as_nanos().min(u64::MAX as u128) as u64,
            next_expiry_ns: 0,
            counts: MatchCounts::default(),
        }
    }
    fn discard(&mut self, g: Group, boundary: bool) {
        self.bytes -= g.bytes;
        self.entries -= g.items.len();
        if boundary {
            self.counts.boundary_censored += g.items.len() as u64;
        } else {
            self.counts.unmatched += g.items.len() as u64;
        }
    }
    fn expire(&mut self, now: u64) {
        let keys: Vec<_> = self
            .closed
            .iter()
            .filter(|(_, v)| {
                v.iter()
                    .flatten()
                    .any(|g| now.saturating_sub(g.last) > self.ttl_ns)
            })
            .map(|(k, _)| k.clone())
            .collect();
        for k in keys {
            for g in self.closed.remove(&k).unwrap().into_iter().flatten() {
                self.discard(g, false);
            }
        }
        // A long, unclosed group is censored, never guessed complete. Retain an
        // empty unanchored boundary until the next clock establishes alignment.
        for g in self.current.values_mut() {
            if !g.items.is_empty() && now.saturating_sub(g.last) > self.ttl_ns {
                self.bytes -= g.bytes;
                self.entries -= g.items.len();
                self.counts.boundary_censored += g.items.len() as u64;
                g.items.clear();
                g.bytes = 0;
                g.anchored = false;
            }
        }
    }
    fn push(&mut self, event: Arrival) -> anyhow::Result<Vec<Sample>> {
        if event.ns >= self.next_expiry_ns {
            self.expire(event.ns);
            self.next_expiry_ns = event
                .ns
                .saturating_add((self.ttl_ns / 4).clamp(1, 100_000_000));
        }
        let key = (event.feed, event.symbol.clone());
        let mut samples = vec![];
        if let Some(g) = self.current.get(&key) {
            ensure!(
                event.clock >= g.clock,
                "market clock reset: stop epoch instead of cross-reset matching"
            );
        }
        if self
            .current
            .get(&key)
            .is_some_and(|g| g.clock != event.clock)
        {
            let old = self.current.remove(&key).unwrap();
            if !old.anchored {
                self.discard(old, true);
            } else {
                let closed_key = (event.symbol.clone(), old.clock);
                let slot = self
                    .closed
                    .entry(closed_key.clone())
                    .or_insert_with(|| [None, None]);
                ensure!(slot[event.feed].is_none(), "repeated closed clock group");
                slot[event.feed] = Some(old);
                if slot.iter().all(Option::is_some) {
                    let [a, b] = self.closed.remove(&closed_key).unwrap();
                    let (a, b) = (a.unwrap(), b.unwrap());
                    self.bytes -= a.bytes + b.bytes;
                    self.entries -= a.items.len() + b.items.len();
                    if a.items.len() == b.items.len()
                        && a.items
                            .iter()
                            .zip(&b.items)
                            .any(|(a, b)| a.1.abs_diff(b.1) > self.ttl_ns)
                    {
                        self.counts.unmatched += (a.items.len() + b.items.len()) as u64;
                    } else if a.items.len() != b.items.len()
                        || a.items.iter().zip(&b.items).any(|(a, b)| a.0 != b.0)
                    {
                        self.counts.ambiguous += (a.items.len() + b.items.len()) as u64;
                    } else {
                        for (ordinal, ((_, an), (_, bn))) in
                            a.items.into_iter().zip(b.items).enumerate()
                        {
                            samples.push(Sample {
                                symbol: event.symbol.clone(),
                                clock: a.clock,
                                ordinal,
                                a_ns: an,
                                b_ns: bn,
                                a_minus_b_ns: an as i64 - bn as i64,
                            });
                            self.counts.matched += 1;
                        }
                    }
                }
            }
            self.current.insert(
                key.clone(),
                Group {
                    clock: event.clock,
                    items: vec![],
                    anchored: true,
                    bytes: 0,
                    last: event.ns,
                },
            );
        }
        let cost = std::mem::size_of::<(Payload, u64)>() + event.symbol.len() + 128;
        if self.entries >= self.cap || self.bytes + cost > self.byte_cap {
            self.counts.evicted += 1;
            // Failing the trial is safer than silently matching a partial group.
            anyhow::bail!("matcher entry/byte cap exhausted");
        }
        let group = self.current.entry(key).or_insert_with(|| Group {
            clock: event.clock,
            items: vec![],
            anchored: false,
            bytes: 0,
            last: event.ns,
        });
        group.items.push((event.payload, event.ns));
        group.bytes += cost;
        group.last = event.ns;
        self.bytes += cost;
        self.entries += 1;
        self.counts.pending_peak_bytes = self.counts.pending_peak_bytes.max(self.bytes);
        Ok(samples)
    }
    fn finish(&mut self) {
        for (_, g) in std::mem::take(&mut self.current) {
            self.discard(g, true);
        }
        for (_, groups) in std::mem::take(&mut self.closed) {
            for g in groups.into_iter().flatten() {
                self.discard(g, false);
            }
        }
    }
}

#[derive(Clone, Default, Debug, Serialize)]
pub struct FeedReport {
    pub feed: usize,
    pub generation: u64,
    pub local: String,
    pub peer: String,
    pub ready_ms: u128,
    pub frames: u64,
    pub updates: u64,
    pub frame_bytes: u64,
    pub gaps: u64,
    pub clean_close: bool,
    pub failure: Option<String>,
}
enum Event {
    Ready(usize, u64),
    Delta(Arrival),
}

#[allow(clippy::too_many_arguments)]
async fn feed(
    index: usize,
    generation: u64,
    options: Options,
    auth: Arc<Credentials>,
    origin: Instant,
    port: Option<u16>,
    tx: mpsc::Sender<Event>,
    mut stop: watch::Receiver<bool>,
) -> anyhow::Result<FeedReport> {
    if index > 0 {
        tokio::select! {_=stopped(&mut stop)=>return Ok(FeedReport::default()),_=tokio::time::sleep(Duration::from_millis(250))=>{}}
    }
    let mut connect = options.connect.clone();
    connect.source_port = port;
    let mut connection = tokio::select! {_=stopped(&mut stop)=>return Ok(FeedReport::default()),r=transport::connect(&options.ws_url,&auth,&connect)=>r?};
    let mut report = FeedReport {
        feed: index,
        generation,
        local: connection.local.to_string(),
        peer: connection.peer.to_string(),
        ..FeedReport::default()
    };
    let result=async {
        connection.socket.send(Message::Text(Kalshi.subscribe(1,&options.symbols)?.to_string().into())).await?;
        let mut books:HashMap<_,_>=options.symbols.iter().map(|s|(s.clone(),OrderBook::new(s.clone()))).collect();
        let mut tracker=SequenceTracker::default();let mut ready=false;let mut ack=false;let mut sid=None;
        let mut ping=tokio::time::interval(Duration::from_secs(10));ping.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut health=tokio::time::interval(Duration::from_secs(1));let mut last_frame=Instant::now();
        loop {
            tokio::select! {
                _=stopped(&mut stop)=>return Ok::<_,anyhow::Error>(()),
                _=ping.tick()=>connection.socket.send(Message::Ping(vec![].into())).await?,
                _=health.tick()=>{ensure!(last_frame.elapsed()<Duration::from_secs(30),"probe heartbeat timeout");},
                message=connection.socket.next()=>{
                    let ns=origin.elapsed().as_nanos() as u64;let receive=Utc::now();last_frame=Instant::now();
                    let text=match message.context("probe websocket ended")?? {
                        Message::Text(t)=>t.to_string(),Message::Ping(p)=>{connection.socket.send(Message::Pong(p)).await?;continue;},Message::Pong(_)=>continue,
                        Message::Close(_)=>anyhow::bail!("probe peer closed"),_=>continue,
                    };
                    report.frames+=1;report.frame_bytes+=text.len() as u64;
                    let frame=Kalshi.decode(&text,receive)?;
                    if let Some((s,q))=frame.sequence {
                        ensure!(sid.is_none_or(|expected|expected==s),"probe sid changed");
                        if tracker.observe(s,q){report.gaps+=1;anyhow::bail!("sequence gap invalidates probe epoch");}
                    }
                    match frame.control {
                        Control::Subscribed(s) if frame.request_id==Some(1)=>{ack=true;sid=Some(s);},
                        Control::Error {code,..}=>anyhow::bail!("probe command rejected: {code}"),_=>{},
                    }
                    for event in frame.events {
                        let book=books.get_mut(event.symbol()).context("unrequested probe market")?;
                        match event {
                            BookEvent::Snapshot(s)=>{ensure!(!ready,"unexpected snapshot resets probe epoch");book.replace(s)?;},
                            BookEvent::Update(u)=>{
                                let arrival=Arrival {feed:index,generation,symbol:u.symbol.clone(),clock:u.clock.context("probe requires real-time market clock")?,
                                    payload:Payload {side:u.side==Side::Buy,price:u.price,change:match u.quantity {QuantityUpdate::Change(q)=>q,_=>anyhow::bail!("expected change")}},ns};
                                book.apply(u)?;report.updates+=1;
                                if ready {tx.try_send(Event::Delta(arrival)).context("probe observer queue full/closed; trial invalid")?;}
                            },
                        }
                    }
                    if !ready && ack && books.values().all(OrderBook::is_valid) {
                        ready=true;report.ready_ms=origin.elapsed().as_millis();tx.try_send(Event::Ready(index,generation))?;
                    }
                }
            }
        }
    }.await;
    if let Err(e) = result {
        report.failure = Some(e.to_string());
    }
    match crate::collector::close(&mut connection.socket).await {
        Ok(()) => report.clean_close = true,
        Err(e) => {
            report
                .failure
                .get_or_insert_with(|| format!("probe close failed: {e:#}"));
        }
    }
    Ok(report)
}

#[derive(Default, Serialize)]
pub struct Distribution {
    pub n: usize,
    pub p50_ns: i64,
    pub p95_ns: i64,
    pub p99_ns: i64,
    pub min_ns: i64,
    pub max_ns: i64,
    pub a_win_fraction: f64,
}
fn distribution(samples: &[&Sample]) -> Distribution {
    if samples.is_empty() {
        return Distribution::default();
    }
    let mut values: Vec<_> = samples.iter().map(|s| s.a_minus_b_ns).collect();
    values.sort_unstable();
    let n = values.len();
    let q = |percent: usize| values[(n * percent).div_ceil(100).saturating_sub(1)];
    Distribution {
        n,
        p50_ns: q(50),
        p95_ns: q(95),
        p99_ns: q(99),
        min_ns: values[0],
        max_ns: values[n - 1],
        a_win_fraction: values.iter().filter(|&&n| n < 0).count() as f64 / n as f64,
    }
}
#[derive(Serialize)]
pub struct Trial {
    pub index: usize,
    pub candidate_port: Option<u16>,
    pub reference_generation: u64,
    pub candidate_generation: u64,
    pub elapsed_ms: u128,
    pub stop_reason: String,
    pub failure: Option<String>,
    pub feeds: Vec<FeedReport>,
    pub counts: MatchCounts,
    pub all: Distribution,
    pub training: Distribution,
    pub holdout: Distribution,
    pub selected_on_training: Option<String>,
    pub retained_sample_bytes: usize,
    pub matched_not_retained: u64,
    #[serde(skip)]
    pub samples: Vec<Sample>,
}
#[derive(Serialize)]
pub struct Report {
    pub schema_version: u32,
    pub started_utc: String,
    pub ws_url: String,
    pub symbols: Vec<String>,
    pub trials: Vec<Trial>,
    pub clock_semantics: String,
    pub warmup_seconds: f64,
    pub duration_seconds: f64,
    pub sample_cap_per_trial: usize,
    pub pending_byte_cap: usize,
    pub rss_bytes: Option<u64>,
    pub persistent_reference: bool,
    pub reference: Option<FeedReport>,
    pub reference_failure: Option<String>,
}

pub async fn run(
    options: Options,
    auth: Arc<Credentials>,
    mut stop: watch::Receiver<bool>,
) -> anyhow::Result<Report> {
    ensure!(
        !options.symbols.is_empty()
            && options.symbols.len() <= 100
            && options.trials > 0
            && options.trials <= 20,
        "probe requires 1..100 unique markets and 1..20 trials"
    );
    ensure!(
        !options.duration.is_zero()
            && options.duration <= Duration::from_secs(3600)
            && options.warmup <= Duration::from_secs(600)
            && options.sample_cap > 0
            && options.sample_cap <= 500_000
            && options.pending_cap > 0
            && options.pending_bytes > 0
            && !options.ttl.is_zero(),
        "invalid probe caps"
    );
    ensure!(
        options
            .symbols
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
            == options.symbols.len(),
        "duplicate probe symbols"
    );
    if options.persistent_reference {
        return persistent::run(options, auth, stop).await;
    }
    let mut report = new_report(&options);
    for index in 0..options.trials {
        if *stop.borrow() {
            break;
        }
        let port = if options.ports.is_empty() {
            None
        } else {
            Some(options.ports[index % options.ports.len()])
        };
        let origin = Instant::now();
        let (tx, mut rx) = mpsc::channel(1024);
        let (cancel, local_stop) = watch::channel(false);
        let mut tasks = JoinSet::new();
        for feed_index in 0..2 {
            tasks.spawn(feed(
                feed_index,
                if feed_index == 0 { 1 } else { index as u64 + 1 },
                options.clone(),
                auth.clone(),
                origin,
                if feed_index == 1 { port } else { None },
                tx.clone(),
                local_stop.clone(),
            ));
        }
        drop(tx);
        let mut ready = [false; 2];
        let mut start = None;
        let mut matcher = Matcher::new(options.pending_cap, options.pending_bytes, options.ttl);
        let mut samples = vec![];
        let mut failure = None;
        let mut reason = "duration".to_string();
        let mut retained = 0;
        loop {
            let deadline = start
                .map(|s: Instant| s + options.duration)
                .unwrap_or(origin + Duration::from_secs(60));
            tokio::select! {
                _=stopped(&mut stop)=>{reason="cancelled".into();break;},
                _=tokio::time::sleep_until(deadline)=>{if start.is_none(){failure=Some("probe initialization timeout".into());}break;},
                done=tasks.join_next()=>{failure=Some(format!("probe feed ended during measurement: {done:?}"));break;},
                event=rx.recv()=>match event {
                    Some(Event::Ready(i,_generation))=>{ready[i]=true;if ready.iter().all(|v|*v){start=Some(Instant::now()+options.warmup);}},
                    Some(Event::Delta(event))=>{
                        if start.is_none_or(|start|origin+Duration::from_nanos(event.ns)<start){continue;}
                        match matcher.push(event) {
                            Ok(matched)=>for sample in matched {
                                let cost=std::mem::size_of::<Sample>()+sample.symbol.capacity()+32;
                                if samples.len()>=options.sample_cap || retained+cost>32<<20 {reason="sample_cap".into();break;}
                                retained+=cost;samples.push(sample);
                            },
                            Err(e)=>{failure=Some(e.to_string());break;},
                        }
                        if reason=="sample_cap"{break;}
                    },
                    None=>{failure=Some("probe feeds closed observer channel".into());break;},
                }
            }
        }
        let _ = cancel.send(true);
        let mut feeds = vec![];
        let drained = tokio::time::timeout(Duration::from_secs(12), async {
            while let Some(result) = tasks.join_next().await {
                match result {
                    Ok(Ok(row)) => {
                        if let Some(e) = &row.failure {
                            failure.get_or_insert(e.clone());
                        }
                        feeds.push(row);
                    }
                    Ok(Err(e)) => {
                        failure.get_or_insert_with(|| e.to_string());
                    }
                    Err(e) => {
                        failure.get_or_insert_with(|| e.to_string());
                    }
                }
            }
        })
        .await;
        if drained.is_err() {
            tasks.abort_all();
            failure = Some("probe close deadline exceeded".into());
        }
        matcher.finish();
        let split = start.map(|s| s + options.duration / 2).unwrap_or(origin);
        let split_ns = split.duration_since(origin).as_nanos() as u64;
        let all: Vec<_> = samples.iter().collect();
        let training: Vec<_> = all
            .iter()
            .copied()
            .filter(|s| s.a_ns.max(s.b_ns) < split_ns)
            .collect();
        let holdout: Vec<_> = all
            .iter()
            .copied()
            .filter(|s| s.a_ns.min(s.b_ns) >= split_ns)
            .collect();
        let training = distribution(&training);
        let selected = if training.n == 0 || training.p50_ns == 0 {
            None
        } else {
            Some(if training.p50_ns < 0 { "A" } else { "B" }.to_string())
        };
        let not_retained = matcher.counts.matched.saturating_sub(samples.len() as u64);
        let trial = Trial {
            index,
            candidate_port: port,
            reference_generation: 1,
            candidate_generation: index as u64 + 1,
            elapsed_ms: origin.elapsed().as_millis(),
            stop_reason: reason,
            failure,
            feeds,
            counts: matcher.counts,
            all: distribution(&all),
            training,
            holdout: distribution(&holdout),
            selected_on_training: selected,
            retained_sample_bytes: retained,
            matched_not_retained: not_retained,
            samples,
        };
        eprintln!(
            "latency trial {index}: matched={} unmatched={} ambiguous={} p50={}ns failure={:?}",
            trial.counts.matched,
            trial.counts.unmatched,
            trial.counts.ambiguous,
            trial.all.p50_ns,
            trial.failure
        );
        report.trials.push(trial);
        if *stop.borrow() {
            break;
        }
        tokio::select! {_=stopped(&mut stop)=>break,_=tokio::time::sleep(Duration::from_secs(1))=>{}}
    }
    report.rss_bytes = crate::pilot::rss_bytes();
    Ok(report)
}

fn new_report(options: &Options) -> Report {
    Report {schema_version:2,started_utc:Utc::now().to_rfc3339(),ws_url:options.ws_url.clone(),symbols:options.symbols.clone(),trials:vec![],
        clock_semantics:"same-process monotonic application arrivals; A-B < 0 means A earlier; no NIC/one-way latency claim".into(),
        warmup_seconds:options.warmup.as_secs_f64(),duration_seconds:options.duration.as_secs_f64(),sample_cap_per_trial:options.sample_cap,
        pending_byte_cap:options.pending_bytes,rss_bytes:None,persistent_reference:options.persistent_reference,reference:None,reference_failure:None}
}

#[cfg(test)]
mod tests {
    use super::*;
    fn e(feed: usize, clock: u64, ordinal: i64, ns: u64) -> Arrival {
        Arrival {
            feed,
            generation: 1,
            symbol: "A".into(),
            clock,
            payload: Payload {
                side: true,
                price: 42,
                change: ordinal,
            },
            ns,
        }
    }
    #[test]
    fn complete_clock_groups_preserve_identical_payload_ordinals_and_signed_skew() {
        let mut m = Matcher::new(100, 100_000, Duration::from_secs(1));
        let mut samples = vec![];
        for clock in 1..=4 {
            for ordinal in 0..2 {
                for feed in 0..2 {
                    samples.extend(
                        m.push(e(feed, clock, ordinal, clock * 100 + feed as u64 * 5))
                            .unwrap(),
                    );
                }
            }
        }
        assert_eq!(samples.len(), 4);
        assert!(samples.iter().all(|s| s.a_minus_b_ns == -5));
        assert_eq!(
            samples.iter().map(|s| s.ordinal).collect::<Vec<_>>(),
            [0, 1, 0, 1]
        );
        m.finish();
        assert_eq!(m.counts.boundary_censored, 8);
        assert_eq!(m.bytes, 0);
        assert_eq!(m.entries, 0);
    }
    #[test]
    fn missing_mismatched_reset_and_caps_never_create_fake_pairs() {
        let mut m = Matcher::new(100, 100_000, Duration::from_nanos(1000));
        for feed in 0..2 {
            m.push(e(feed, 1, 1, 0)).unwrap();
            m.push(e(feed, 2, feed as i64, 10)).unwrap();
        }
        m.push(e(0, 3, 1, 20)).unwrap();
        assert!(m.push(e(1, 3, 1, 21)).unwrap().is_empty());
        assert_eq!(m.counts.ambiguous, 2);
        m.push(e(0, 4, 1, 30)).unwrap();
        m.finish();
        assert!(m.counts.unmatched > 0);
        let mut m = Matcher::new(1, 1000, Duration::from_secs(1));
        m.push(e(0, 5, 1, 1)).unwrap();
        assert!(m.push(e(0, 5, 2, 2)).is_err());
        assert!(m.push(e(0, 4, 1, 3)).is_err());
    }
    #[test]
    fn batched_expiry_never_matches_a_pair_outside_the_exact_ttl() {
        let mut m = Matcher::new(100, 100_000, Duration::from_nanos(1000));
        for event in [
            e(0, 1, 1, 0),
            e(0, 2, 1, 100),
            e(0, 3, 1, 200),
            e(1, 1, 1, 1000),
            e(1, 2, 1, 1101),
            e(1, 3, 1, 1102),
        ] {
            assert!(m.push(event).unwrap().is_empty());
        }
        assert_eq!(m.counts.matched, 0);
        assert!(m.counts.unmatched >= 2);
    }
    #[test]
    fn nearest_rank_small_sample_does_not_hide_the_tail() {
        let samples = [
            Sample {
                symbol: "A".into(),
                clock: 1,
                ordinal: 0,
                a_ns: 0,
                b_ns: 0,
                a_minus_b_ns: 1,
            },
            Sample {
                symbol: "A".into(),
                clock: 2,
                ordinal: 0,
                a_ns: 0,
                b_ns: 0,
                a_minus_b_ns: 1000,
            },
        ];
        let d = distribution(&samples.iter().collect::<Vec<_>>());
        assert_eq!(d.p99_ns, 1000);
    }
}
