//! A moving fast publication floor needs an ingress pause before a lagging
//! survivor can cover it. The provider never stops its common ordered tail.
use ed25519_dalek::{SigningKey, pkcs8::EncodePrivateKey};
use engineering_challenge::{
    auth::Credentials,
    collector::{self, Options, RegistryOptions},
    health,
    output::AuditRow,
    replay,
    transport::ConnectOptions,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{broadcast, watch},
    time::{Instant, MissedTickBehavior},
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

type Socket = WebSocketStream<tokio::net::TcpStream>;

#[derive(Clone, Copy)]
struct Change {
    clock: u64,
    at: Instant,
}

#[derive(Clone, Default, Debug)]
struct Connections {
    active: BTreeSet<usize>,
    ready: BTreeSet<usize>,
    planned_closes: BTreeSet<usize>,
    closed: BTreeSet<usize>,
    retired_slots: Vec<usize>,
}

#[derive(Default, Debug)]
struct Flow {
    delivered: BTreeMap<usize, u64>,
    initial_fast_lead_samples: usize,
    initial_fast_lead_max: u64,
}

async fn event(ws: &mut Socket, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

async fn command(ws: &mut Socket) -> Value {
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(payload) => ws.send(Message::Pong(payload)).await.unwrap(),
            other => panic!("unexpected native initialization frame: {other:?}"),
        }
    }
}

async fn snapshot(ws: &mut Socket, seq: &mut u64, clock: u64) {
    *seq += 1;
    event(
        ws,
        json!({"type":"orderbook_snapshot", "sid":7, "seq":*seq,
        "sending_ts_ms":1700000000000i64,
        "msg":{"market_ticker":"A", "clock":clock,
            "yes_dollars_fp":[["0.42",(100+clock).to_string()]]}}),
    )
    .await;
}

async fn feed(
    mut ws: Socket,
    identity: (usize, usize),
    provider: Arc<Mutex<Change>>,
    changes: broadcast::Sender<Change>,
    connections: watch::Sender<Connections>,
    flow: Arc<Mutex<Flow>>,
    stopping: watch::Receiver<bool>,
) {
    let (connection, physical_slot) = identity;
    connections.send_modify(|state| {
        state.active.insert(connection);
    });
    let subscribe = command(&mut ws).await;
    assert_eq!(subscribe["cmd"], "subscribe");
    assert_eq!(subscribe["params"]["market_tickers"], json!(["A"]));
    event(
        &mut ws,
        json!({"id":subscribe["id"], "type":"subscribed", "msg":{"sid":7}}),
    )
    .await;
    if connection == 1 {
        // A real control-frame barrier on the fast feed places its initial
        // snapshot/tail in the common relay before the lagging baseline.
        let mut observed = connections.subscribe();
        loop {
            if observed.borrow().ready.contains(&0) {
                break;
            }
            observed.changed().await.unwrap();
        }
    }
    let lag = if physical_slot == 0 {
        Duration::ZERO
    } else {
        Duration::from_millis(20)
    };
    // Subscribe before sampling state: an update racing the snapshot is either
    // included in that snapshot or retained in this bounded ordered tail.
    let mut tail = changes.subscribe();
    let initial = *provider.lock().unwrap();
    tokio::time::sleep_until(initial.at + lag).await;
    let mut seq = 0;
    snapshot(&mut ws, &mut seq, initial.clock).await;
    let mut last = initial.clock;
    let mut next: Option<Change> = None;
    let mut marker_sent = false;
    loop {
        let due = next.map_or_else(Instant::now, |change| change.at + lag);
        tokio::select! {
            change = tail.recv(), if next.is_none() => {
                let change = change.expect("bounded provider tail overflowed");
                if change.clock > last { next = Some(change); }
            }
            _ = tokio::time::sleep_until(due), if next.is_some() => {
                let change = next.take().unwrap();
                if change.clock <= last { continue; }
                assert_eq!(change.clock, last + 1, "provider tail skipped a clock");
                seq += 1;
                event(&mut ws, json!({"type":"orderbook_delta", "sid":7, "seq":seq,
                    "sending_ts_ms":1700000000000i64,
                    "msg":{"market_ticker":"A", "clock":change.clock, "side":"yes",
                        "price_dollars":"0.42", "delta_fp":"1"}})).await;
                last = change.clock;
                {
                    let mut flow = flow.lock().unwrap();
                    flow.delivered.insert(connection, last);
                    if connection == 0 && let Some(&slow) = flow.delivered.get(&1) {
                        let lead = last.saturating_sub(slow);
                        if lead >= 2 { flow.initial_fast_lead_samples += 1; }
                        flow.initial_fast_lead_max = flow.initial_fast_lead_max.max(lead);
                    }
                }
                if !marker_sent {
                    // A baseline alone is insufficient: the replacement must
                    // consume a real ordered tail before requesting shutdown.
                    marker_sent = true;
                    ws.send(Message::Ping(vec![connection as u8].into())).await.unwrap();
                }
            }
            message = ws.next() => match message {
                Some(Ok(Message::Pong(payload))) if payload.as_ref() == [connection as u8] => {
                    connections.send_modify(|state| { state.ready.insert(connection); });
                }
                Some(Ok(Message::Ping(payload))) => ws.send(Message::Pong(payload)).await.unwrap(),
                Some(Ok(Message::Text(text))) => {
                    let request: Value = serde_json::from_str(&text).unwrap();
                    assert_eq!(request["params"]["action"], "get_snapshot", "{request}");
                    // Refreshes sample the same provider and preserve the
                    // feed's delivery delay; they cannot teleport a peer ahead.
                    let current = *provider.lock().unwrap();
                    seq += 1;
                    event(&mut ws, json!({"id":request["id"], "type":"ok", "sid":7, "seq":seq, "msg":{}})).await;
                    tokio::time::sleep_until(current.at + lag).await;
                    snapshot(&mut ws, &mut seq, current.clock).await;
                    last = current.clock;
                    if next.is_some_and(|change| change.clock <= last) { next = None; }
                }
                Some(Ok(Message::Close(_))) => {
                    flow.lock().unwrap().delivered.remove(&connection);
                    connections.send_modify(|state| {
                        assert!(state.active.remove(&connection));
                        state.closed.insert(connection);
                        if !*stopping.borrow() {
                            assert!(!state.active.is_empty(), "planned rotation removed both feeds");
                            state.planned_closes.insert(connection);
                            state.retired_slots.push(physical_slot);
                        }
                    });
                    ws.flush().await.unwrap();
                    return;
                }
                other => panic!("native socket ended without a Close handshake: {other:?}"),
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingress_pause_lets_a_lagging_survivor_complete_both_feed_rotations() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let options = Options {
        symbols: vec!["A".into()],
        rest_url: "unused".into(),
        ws_url: format!("ws://{}/trade-api/ws/v2", listener.local_addr().unwrap()),
        symbols_per_connection: 1,
        max_connections: 2,
        max_symbols: 1,
        replicas: 2,
        replica_lag: Duration::from_millis(100),
        failover_test_after: None,
        output: temp.path().join("capture.sbe"),
        audit: Some(temp.path().join("audit.jsonl")),
        summary: Some(temp.path().join("summary.json")),
        snapshot_interval: Duration::from_millis(100),
        flush_interval: Duration::from_secs(60),
        connect: ConnectOptions::default(),
        registry: RegistryOptions {
            command_interval: Duration::from_millis(1),
            ..RegistryOptions::default()
        },
        health: health::Options {
            window: Duration::from_secs(4),
            warmup: Duration::from_secs(4),
            cooldown: Duration::from_secs(4),
            rotation_interval: Duration::from_secs(1),
            rotation_timeout: Duration::from_secs(3),
            ..health::Options::default()
        },
    };
    let pem = SigningKey::from_bytes(&[9; 32])
        .to_pkcs8_pem(Default::default())
        .unwrap();
    let auth =
        Arc::new(Credentials::from_pem("localhost-pause-fixture".into(), pem.as_str()).unwrap());
    let (cancel, stop) = watch::channel(false);
    let (server_cancel, server_stop) = watch::channel(false);
    let (connections, mut observed) = watch::channel(Connections::default());
    let flow = Arc::new(Mutex::new(Flow::default()));
    let provider = Arc::new(Mutex::new(Change {
        clock: 1,
        at: Instant::now(),
    }));
    let (changes, _) = broadcast::channel::<Change>(4096);
    let mut producer_stop = server_stop.clone();
    let producer_provider = provider.clone();
    let producer_changes = changes.clone();
    let producer = async move {
        let mut tick = tokio::time::interval(Duration::from_millis(5));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = collector::stopped(&mut producer_stop) => break,
                _ = tick.tick() => {
                    let change = {
                        let mut state = producer_provider.lock().unwrap();
                        state.clock += 1; state.at = Instant::now(); *state
                    };
                    let _ = producer_changes.send(change);
                }
            }
        }
    };
    let mut accept_stop = server_stop;
    let observer_stop = stop.clone();
    let server_flow = flow.clone();
    let request_stop = cancel.clone();
    let server = async move {
        let mut peers = tokio::task::JoinSet::new();
        let mut count = 0usize;
        let mut finished = false;
        loop {
            tokio::select! {
                _ = collector::stopped(&mut accept_stop) => break,
                changed = observed.changed(), if !finished => {
                    changed.unwrap();
                    let state = observed.borrow();
                    if [0,1].iter().all(|id| state.planned_closes.contains(id))
                        && [2,3].iter().all(|id| state.ready.contains(id))
                    {
                        finished = true;
                        request_stop.send(true).unwrap();
                    }
                }
                accepted = listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    assert!(count < 4, "unexpected fifth connection: retry or extra rotation");
                    let connection = count; count += 1;
                // Native task scheduling can reverse initial accept order.
                // Replacements inherit their actual retired physical slot's
                // delay, rather than assuming accept order equals replica ID.
                let physical_slot = if connection < 2 { connection } else {
                    connections.borrow().retired_slots[connection - 2]
                };
                    let (provider, changes, connections, flow, stopping) =
                        (provider.clone(), changes.clone(), connections.clone(), server_flow.clone(), observer_stop.clone());
                    peers.spawn(async move {
                        let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                    feed(ws, (connection, physical_slot), provider, changes, connections, flow, stopping).await;
                    });
                }
            }
        }
        while let Some(peer) = peers.join_next().await {
            peer.unwrap();
        }
        assert!(
            finished,
            "collector ended before both replacements consumed ordered tails"
        );
        assert_eq!(count, 4);
        let state = connections.borrow().clone();
        assert!(state.active.is_empty());
        assert_eq!(state.closed, BTreeSet::from([0, 1, 2, 3]));
        state
    };
    let collect = async {
        let result = collector::run(options.clone(), auth, stop, cancel).await;
        // Quiescence must continue receiving the moving common provider tail.
        // Stop the provider/server only after collector has closed and joined.
        server_cancel.send(true).unwrap();
        result
    };
    let started = Instant::now();
    let (result, connections, ()) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(collect, server, producer)
    })
    .await
    .expect("planned handoff failed to complete both rotations within 20s");
    result.unwrap();
    let flow = flow.lock().unwrap();
    assert!(
        flow.initial_fast_lead_samples >= 20,
        "fixture did not sustain a faster initial publisher: {flow:?}"
    );
    assert!(flow.initial_fast_lead_max >= 2);
    let summary: Value =
        serde_json::from_slice(&std::fs::read(options.summary.as_ref().unwrap()).unwrap()).unwrap();
    assert_eq!(summary["complete_at_cutoff"], true);
    let shard = &summary["shards"][0];
    assert_eq!(shard["replica_initialized"], json!([1, 1]));
    assert!(shard["completed_rotations"].as_u64().unwrap() >= 2);
    assert!(shard["handoff_pauses"].as_u64().unwrap() > 0);
    assert!(shard["pause_us_max"].as_u64().unwrap() > 0);
    for key in [
        "gaps",
        "continuity_gaps",
        "clock_regressions",
        "hard_resets",
        "rotation_timeouts",
        "pause_expirations",
        "maintenance_pending",
        "unacknowledged_closes",
        "ack_timeouts",
    ] {
        assert_eq!(shard[key], 0, "{key}: {shard}");
    }
    let report = replay::verify(&options.output, options.audit.as_deref(), 1).unwrap();
    assert!(report.periodic_checkpoints_verified > 0);
    assert!(report.failover_baselines_verified > 0);
    assert!(report.updates > 100);
    for line in std::fs::read_to_string(options.audit.as_ref().unwrap())
        .unwrap()
        .lines()
    {
        let row: AuditRow = serde_json::from_str(line).unwrap();
        assert_eq!(row.source.unwrap().continuity_epoch, 0);
    }
    eprintln!(
        "planned pause fixture completed in {:?}; {connections:?}; {flow:?}; shard={shard}; replay={report:?}",
        started.elapsed()
    );
}
