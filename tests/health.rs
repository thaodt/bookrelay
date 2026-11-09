//! Real WS boundaries: health intent -> delete ACK -> add -> fresh snapshot,
//! and peer snapshot handoff -> old Close -> replacement initialization.
use ed25519_dalek::{SigningKey, pkcs8::EncodePrivateKey};
use engineering_challenge::{
    auth::Credentials,
    collector::{self, Options, RegistryOptions},
    health, replay,
    transport::ConnectOptions,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
type Socket = WebSocketStream<tokio::net::TcpStream>;

async fn event(ws: &mut Socket, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn command(ws: &mut Socket) -> Value {
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(p) => ws.send(Message::Pong(p)).await.unwrap(),
            _ => {}
        }
    }
}
fn snapshot(seq: u64, clock: u64, quantity: u64) -> Value {
    json!({"type":"orderbook_snapshot","sid":7,"seq":seq,"sending_ts_ms":1700000000000i64,
        "msg":{"market_ticker":"A","clock":clock,"yes_dollars_fp":[["0.42",quantity.to_string()]]}})
}
fn delta(seq: u64, clock: u64, quantity: &str) -> Value {
    json!({"type":"orderbook_delta","sid":7,"seq":seq,"sending_ts_ms":1700000000000i64,
        "msg":{"market_ticker":"A","clock":clock,"side":"yes","price_dollars":"0.42","delta_fp":quantity}})
}
fn options(temp: &tempfile::TempDir, address: std::net::SocketAddr) -> Options {
    Options {
        symbols: vec!["A".into()],
        rest_url: "unused".into(),
        ws_url: format!("ws://{address}/trade-api/ws/v2"),
        symbols_per_connection: 10,
        max_connections: 2,
        max_symbols: 10,
        replicas: 2,
        replica_lag: Duration::from_millis(250),
        failover_test_after: None,
        output: temp.path().join("out.sbe"),
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
            rotation_interval: Duration::ZERO,
            ..health::Options::default()
        },
    }
}
fn auth() -> Arc<Credentials> {
    let pem = SigningKey::from_bytes(&[8; 32])
        .to_pkcs8_pem(Default::default())
        .unwrap();
    Arc::new(Credentials::from_pem("test".into(), pem.as_str()).unwrap())
}
fn verify(options: &Options) -> Value {
    let replay = replay::verify(&options.output, options.audit.as_deref(), 10).unwrap();
    assert!(replay.periodic_checkpoints_verified > 0);
    let summary: Value =
        serde_json::from_slice(&std::fs::read(options.summary.as_ref().unwrap()).unwrap()).unwrap();
    assert_eq!(summary["complete_at_cutoff"], true);
    assert_eq!(summary["shards"][0]["gaps"], 0);
    assert_eq!(summary["shards"][0]["continuity_gaps"], 0);
    assert_eq!(summary["shards"][0]["maintenance_pending"], 0);
    assert_eq!(summary["shards"][0]["maintenance_quiesced"], true);
    summary
}

#[tokio::test]
async fn quiet_subscription_resubscribes_without_reconnecting_or_publishing_old_data() {
    run_subscription_recovery(false, 0).await;
}

#[tokio::test]
async fn snapshot_timeout_resubscribes_affected_symbols_without_restarting_the_socket() {
    run_subscription_recovery(true, 1).await;
}

#[tokio::test]
async fn persistent_missing_add_acks_exhaust_the_symbol_budget_then_replace_the_socket() {
    run_subscription_recovery(true, usize::MAX).await;
}

async fn run_subscription_recovery(drop_snapshot: bool, missing_add_acks: usize) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (cancel, stop) = tokio::sync::watch::channel(false);
    let finish = cancel.clone();
    // Clock and quantity describe one state and need one lock, not two atomics.
    let book = Arc::new(Mutex::new((1u64, 100u64)));
    let (changes, _) = tokio::sync::broadcast::channel::<u64>(128);
    let server = tokio::spawn(async move {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (mut ready_sender, mut ready_receiver) = (Some(tx), Some(rx));
        let mut tasks = tokio::task::JoinSet::new();
        for connection in 0..if missing_add_acks == usize::MAX { 3 } else { 2 } {
            let (stream, _) = listener.accept().await.unwrap();
            let finish = finish.clone();
            let book = book.clone();
            let changes = changes.clone();
            let mut ready_sender = if connection == 1 {
                ready_sender.take()
            } else {
                None
            };
            let ready_receiver = if connection == 0 {
                ready_receiver.take()
            } else {
                None
            };
            tasks.spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                let c = command(&mut ws).await;
                event(&mut ws, json!({"id":c["id"],"type":"subscribed","msg":{"sid":7}})).await;
                // A replacement has a real ordered tail. A static snapshot
                // alone may be behind the peer's already published frontier.
                let mut replacement_tail = changes.subscribe();
                let (clock, quantity) = *book.lock().unwrap();
                let initial_clock = clock;
                event(&mut ws, snapshot(1, clock, quantity)).await;
                let mut seq = 1;
                let mut adds = 0;
                if connection == 1 {
                    ws.send(Message::Ping(vec![88].into())).await.unwrap();
                } else if connection == 0 && drop_snapshot {
                    // The peer's snapshot was admitted to the common relay before
                    // this injected decode failure, so recovery has a warm peer.
                    ready_receiver.unwrap().await.unwrap();
                    ws.send(Message::Text("broken".into())).await.unwrap();
                }
                let mut tick = tokio::time::interval(Duration::from_millis(25));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                enum FeedEvent { Tick, Tail(u64), Message(Message) }
                loop {
                    let next = tokio::select! {
                        _ = tick.tick(), if connection == 1 => FeedEvent::Tick,
                        update = replacement_tail.recv(), if connection == 2 => FeedEvent::Tail(update.unwrap()),
                        message = ws.next() => FeedEvent::Message(message.unwrap().unwrap()),
                    };
                    match next {
                        FeedEvent::Tick => {
                            let clock = {
                                let mut state = book.lock().unwrap();
                                state.0 += 1;
                                state.1 += 1;
                                state.0
                            };
                            seq += 1;
                            event(&mut ws, delta(seq, clock, "1")).await;
                            let _ = changes.send(clock);
                        }
                        FeedEvent::Tail(clock) => {
                            if clock <= initial_clock { continue; }
                            seq += 1;
                            event(&mut ws, delta(seq, clock, "1")).await;
                            ws.send(Message::Ping(vec![42].into())).await.unwrap();
                        }
                        FeedEvent::Message(Message::Text(text)) => {
                            let c: Value = serde_json::from_str(&text).unwrap();
                            if c["cmd"] == "get_snapshot" || c["params"]["action"] == "get_snapshot" {
                                seq += 1;
                                if drop_snapshot {
                                    // An ACK alone cannot satisfy the snapshot wait.
                                    event(&mut ws, json!({"id":c["id"],"type":"ok","sid":7,"seq":seq,"msg":{}})).await;
                                } else {
                                    let (clock, quantity) = *book.lock().unwrap();
                                    event(&mut ws, snapshot(seq, clock, quantity)).await;
                                }
                            } else if c["params"]["action"] == "delete_markets" {
                                assert_eq!(connection, 0);
                                assert_eq!(c["params"]["market_tickers"], json!(["A"]));
                                // Ordered before the delete ACK, but locally revoked.
                                seq += 1;
                                event(&mut ws, delta(seq, 10_000_000, "99999")).await;
                                seq += 1;
                                event(&mut ws, json!({"id":c["id"],"type":"ok","sid":7,"seq":seq,"msg":{}})).await;
                            } else {
                                assert_eq!(c["params"]["action"], "add_markets");
                                let (clock, quantity) = *book.lock().unwrap();
                                seq += 1;
                                event(&mut ws, snapshot(seq, clock, quantity)).await;
                                adds += 1;
                                if adds <= missing_add_acks { continue; }
                                seq += 1;
                                event(&mut ws, json!({"id":c["id"],"type":"ok","sid":7,"seq":seq,"msg":{}})).await;
                                ws.send(Message::Ping(vec![42].into())).await.unwrap();
                            }
                        }
                        FeedEvent::Message(Message::Pong(p)) if p.as_ref() == [88] => {
                            if let Some(sender) = ready_sender.take() { let _ = sender.send(()); }
                        }
                        FeedEvent::Message(Message::Pong(p)) if p.as_ref() == [42] => {
                            finish.send(true).unwrap();
                        }
                        FeedEvent::Message(Message::Ping(p)) => ws.send(Message::Pong(p)).await.unwrap(),
                        FeedEvent::Message(Message::Close(_)) => { let _ = ws.flush().await; break; }
                        _ => {}
                    }
                }
            });
        }
        while let Some(task) = tasks.join_next().await {
            task.unwrap();
        }
    });
    let temp = tempfile::tempdir().unwrap();
    let mut options = options(&temp, address);
    if drop_snapshot {
        options.health.ack_timeout = Duration::from_secs(1);
    }
    tokio::time::timeout(
        Duration::from_secs(20),
        collector::run(options.clone(), auth(), stop, cancel),
    )
    .await
    .unwrap()
    .unwrap();
    server.await.unwrap();
    let s = verify(&options);
    assert_eq!(
        s["shards"][0]["connects"],
        if missing_add_acks == usize::MAX { 3 } else { 2 }
    );
    assert!(s["shards"][0]["pongs_received"].as_u64().unwrap() > 0);
    if drop_snapshot {
        assert!(s["shards"][0]["ack_timeouts"].as_u64().unwrap() >= 1);
        assert!(s["shards"][0]["ack_age_ms_max"].as_u64().unwrap() >= 1000);
    } else {
        assert!(s["shards"][0]["health_resubscriptions"].as_u64().unwrap() >= 1);
    }
    assert_eq!(
        s["shards"][0]["resubscribe_completed"],
        if missing_add_acks == usize::MAX { 0 } else { 1 }
    );
    if missing_add_acks > 0 {
        assert!(s["shards"][0]["ack_timeouts"].as_u64().unwrap() >= 2);
    }
    assert!(s["shards"][0]["retired_messages"].as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn rolling_reconnect_initializes_replacement_while_the_peer_keeps_serving() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (cancel, stop) = tokio::sync::watch::channel(false);
    let finish = cancel.clone();
    let observing_stop = stop.clone();
    let active = Arc::new(Mutex::new(std::collections::BTreeSet::new()));
    let server = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        for connection in 0..3 {
            let (stream, _) = listener.accept().await.unwrap();
            let finish = finish.clone();
            let active = active.clone();
            let observing_stop = observing_stop.clone();
            tasks.spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                active.lock().unwrap().insert(connection);
                let c = command(&mut ws).await;
                assert_eq!(c["cmd"], "subscribe");
                event(
                    &mut ws,
                    json!({"id":c["id"],"type":"subscribed","msg":{"sid":7}}),
                )
                .await;
                event(&mut ws, snapshot(1, 1, 100)).await;
                if connection == 2 {
                    ws.send(Message::Ping(vec![43].into())).await.unwrap();
                }
                while let Some(message) = ws.next().await {
                    match message.unwrap() {
                        Message::Pong(p) if p.as_ref() == [43] => {
                            finish.send(true).unwrap();
                        }
                        Message::Ping(p) => ws.send(Message::Pong(p)).await.unwrap(),
                        Message::Close(_) => {
                            {
                                let mut active = active.lock().unwrap();
                                active.remove(&connection);
                                if !*observing_stop.borrow() {
                                    assert!(!active.is_empty(), "planned close dropped both feeds");
                                }
                            }
                            let _ = ws.flush().await;
                            break;
                        }
                        _ => {}
                    }
                }
            });
        }
        while let Some(task) = tasks.join_next().await {
            task.unwrap();
        }
    });
    let temp = tempfile::tempdir().unwrap();
    let mut options = options(&temp, address);
    options.health.rotation_interval = Duration::from_secs(1);
    tokio::time::timeout(
        Duration::from_secs(20),
        collector::run(options.clone(), auth(), stop, cancel),
    )
    .await
    .unwrap()
    .unwrap();
    server.await.unwrap();
    let s = verify(&options);
    assert_eq!(s["shards"][0]["connects"], 3);
    assert_eq!(s["shards"][0]["completed_rotations"], 1);
    assert_eq!(s["shards"][0]["unacknowledged_closes"], 0);
}

#[tokio::test]
async fn regressed_standby_book_refreshes_only_that_symbol_and_replay_stays_continuous() {
    run_regressed_standby(false).await;
}

#[tokio::test]
async fn semantic_recovery_exhaustion_closes_and_joins_both_sources_before_returning_error() {
    run_regressed_standby(true).await;
}

async fn run_regressed_standby(exhaust: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (cancel, stop) = tokio::sync::watch::channel(false);
    let finish = cancel.clone();
    let closes = Arc::new(Mutex::new(0usize));
    let observed_closes = closes.clone();
    let server = tokio::spawn(async move {
        let (ready, wait) = tokio::sync::oneshot::channel();
        let mut ready = Some(ready);
        let mut wait = Some(wait);
        let mut tasks = tokio::task::JoinSet::new();
        for connection in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let ready = if connection == 0 { ready.take() } else { None };
            let wait = if connection == 1 { wait.take() } else { None };
            let finish = finish.clone();
            let closes = closes.clone();
            tasks.spawn(async move {
                let mut ready = ready;
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                let c = command(&mut ws).await;
                event(
                    &mut ws,
                    json!({"id":c["id"],"type":"subscribed","msg":{"sid":7}}),
                )
                .await;
                if let Some(wait) = wait {
                    wait.await.unwrap();
                }
                for (index, symbol) in ["A", "B", "C"].iter().enumerate() {
                    let mut value = snapshot(index as u64 + 1, 100, 100);
                    value["msg"]["market_ticker"] = json!(symbol);
                    event(&mut ws, value).await;
                }
                ws.send(Message::Ping(
                    vec![if connection == 0 { 88 } else { 99 }].into(),
                ))
                .await
                .unwrap();
                let mut response_seq = 4;
                while let Some(message) = ws.next().await {
                    match message.unwrap() {
                        Message::Pong(p) if p.as_ref() == [88] => {
                            ready.take().unwrap().send(()).unwrap();
                        }
                        Message::Pong(p) if p.as_ref() == [99] => {
                            // Both initial rosters precede this in the shared
                            // FIFO relay. This feed is the warm standby.
                            event(&mut ws, snapshot(4, 0, 100)).await;
                        }
                        Message::Text(text) => {
                            let c: Value = serde_json::from_str(&text).unwrap();
                            assert_eq!(connection, 1);
                            assert_eq!(c["params"]["action"], "get_snapshot");
                            assert_eq!(c["params"]["market_tickers"], json!(["A"]));
                            response_seq += 1;
                            event(
                                &mut ws,
                                snapshot(response_seq, if exhaust { 0 } else { 100 }, 100),
                            )
                            .await;
                            if !exhaust {
                                ws.send(Message::Ping(vec![42].into())).await.unwrap();
                            }
                        }
                        Message::Pong(p) if p.as_ref() == [42] => {
                            finish.send(true).unwrap();
                        }
                        Message::Ping(p) => ws.send(Message::Pong(p)).await.unwrap(),
                        Message::Close(_) => {
                            *closes.lock().unwrap() += 1;
                            let _ = ws.flush().await;
                            break;
                        }
                        _ => {}
                    }
                }
            });
        }
        while let Some(task) = tasks.join_next().await {
            task.unwrap();
        }
    });
    let temp = tempfile::tempdir().unwrap();
    let mut options = options(&temp, address);
    options.symbols = vec!["A".into(), "B".into(), "C".into()];
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        collector::run(options.clone(), auth(), stop, cancel),
    )
    .await
    .unwrap();
    if exhaust {
        assert!(format!("{:#}", outcome.unwrap_err()).contains("semantic recovery exhausted"));
    } else {
        outcome.unwrap();
    }
    server.await.unwrap();
    assert_eq!(*observed_closes.lock().unwrap(), 2);
    if exhaust {
        return;
    }
    let report = replay::verify(&options.output, options.audit.as_deref(), 10).unwrap();
    assert_eq!(report.symbols, 3);
    let summary: Value =
        serde_json::from_slice(&std::fs::read(options.summary.as_ref().unwrap()).unwrap()).unwrap();
    assert_eq!(summary["complete_at_cutoff"], true);
    let shard = &summary["shards"][0];
    assert_eq!(shard["connects"], 2);
    assert_eq!(shard["clock_regressions"], 1);
    assert_eq!(shard["hard_resets"], 0);
    assert_eq!(shard["continuity_gaps"], 0);
    assert_eq!(shard["gaps"], 0);
    assert_eq!(shard["replica_initialized"], json!([3, 3]));
    for row in std::fs::read_to_string(options.audit.unwrap())
        .unwrap()
        .lines()
    {
        let row: Value = serde_json::from_str(row).unwrap();
        assert_eq!(row["source"]["continuity_epoch"], 0);
    }
}
