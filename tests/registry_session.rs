//! Deterministic exchange boundaries: REST churn -> real WS membership ACKs -> SBE replay.
use ed25519_dalek::{SigningKey, pkcs8::EncodePrivateKey};
use engineering_challenge::{
    auth::Credentials,
    collector::{self, Options, RegistryOptions},
    replay,
    transport::ConnectOptions,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;

async fn command(ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>) -> Value {
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(t) => return serde_json::from_str(&t).unwrap(),
            Message::Ping(p) => ws.send(Message::Pong(p)).await.unwrap(),
            _ => {}
        }
    }
}
async fn event(ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>, v: Value) {
    ws.send(Message::Text(v.to_string().into())).await.unwrap();
}
fn snapshot(symbol: &str, seq: u64, quantity: &str) -> Value {
    json!({"type":"orderbook_snapshot","sid":7,"seq":seq,"sending_ts_ms":1700000000000i64,"msg":{"market_ticker":symbol,"clock":seq,"yes_dollars_fp":[["0.42",quantity]]}})
}
fn delta(symbol: &str, seq: u64, quantity: &str) -> Value {
    json!({"type":"orderbook_delta","sid":7,"seq":seq,"sending_ts_ms":1700000000000i64,"msg":{"market_ticker":symbol,"clock":seq,"side":"yes","price_dollars":"0.42","delta_fp":quantity}})
}

#[tokio::test]
async fn closed_market_is_deleted_late_data_is_fenced_and_new_market_gets_snapshot_first() {
    let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http.local_addr().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = listener.local_addr().unwrap();
    let opened = Arc::new(AtomicBool::new(false));
    let http_opened = opened.clone();
    let (cancel, stop) = tokio::sync::watch::channel(false);
    let mut http_stop = stop.clone();
    let http_server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = tokio::select! {r=http.accept()=>r.unwrap(),_=collector::stopped(&mut http_stop)=>break};
            let mut request = vec![];
            let mut byte = [0; 1];
            while !request.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
            }
            let request = String::from_utf8(request).unwrap();
            let body=if request.contains("status=open") {json!({"markets":[{"ticker":"A","status":"active"}]})}
                else {
                    assert!(request.contains("min_updated_ts=") && request.contains("max_updated_ts="));
                    assert!(!request.contains("status="));
                    while !http_opened.load(Ordering::Acquire){tokio::time::sleep(Duration::from_millis(1)).await;}
                    json!({"markets":[{"ticker":"A","status":"closed"},{"ticker":"B","status":"active"}]})
                }.to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        }
    });
    let server_cancel = cancel.clone();
    let ws_server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let c = command(&mut ws).await;
        assert_eq!(c["params"]["market_tickers"], json!(["A"]));
        event(
            &mut ws,
            json!({"id":c["id"],"type":"subscribed","msg":{"sid":7}}),
        )
        .await;
        event(&mut ws, snapshot("A", 1, "100")).await;
        event(&mut ws, delta("A", 2, "-25")).await;
        // Ping is an application receive barrier behind the preceding frames.
        ws.send(Message::Ping(vec![1, 2, 3].into())).await.unwrap();
        loop {
            if let Some(Ok(Message::Pong(p))) = ws.next().await
                && p.as_ref() == [1, 2, 3]
            {
                break;
            }
        }
        opened.store(true, Ordering::Release);
        let c = command(&mut ws).await;
        assert_eq!(c["params"]["action"], "delete_markets");
        assert_eq!(c["params"]["market_tickers"], json!(["A"]));
        event(&mut ws, delta("A", 3, "999")).await; // Late data before delete ACK must not escape.
        event(
            &mut ws,
            json!({"id":c["id"],"type":"ok","sid":7,"seq":4,"msg":{}}),
        )
        .await;
        let c = command(&mut ws).await;
        assert_eq!(c["params"]["action"], "add_markets");
        assert_eq!(c["params"]["market_tickers"], json!(["B"]));
        event(&mut ws, snapshot("B", 5, "40")).await; // Snapshot may precede the membership ACK.
        event(
            &mut ws,
            json!({"id":c["id"],"type":"ok","sid":7,"seq":6,"msg":{}}),
        )
        .await;
        event(&mut ws, delta("B", 7, "5")).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        server_cancel.send(true).unwrap();
        while let Some(message) = ws.next().await {
            if matches!(message, Ok(Message::Close(_))) {
                ws.flush().await.unwrap();
                break;
            }
        }
    });
    let pem = SigningKey::from_bytes(&[7; 32])
        .to_pkcs8_pem(Default::default())
        .unwrap();
    let auth = Arc::new(Credentials::from_pem("test".into(), pem.as_str()).unwrap());
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("out.sbe");
    let audit = temp.path().join("audit.jsonl");
    let summary = temp.path().join("summary.json");
    let options = Options {
        health: engineering_challenge::health::Options::default(),
        replicas: 1,
        replica_lag: Duration::from_secs(2),
        failover_test_after: None,
        symbols: vec![],
        rest_url: format!("http://{http_addr}"),
        ws_url: format!("ws://{ws_addr}/trade-api/ws/v2"),
        symbols_per_connection: 10,
        max_connections: 1,
        max_symbols: 10,
        output: output.clone(),
        audit: Some(audit.clone()),
        summary: Some(summary.clone()),
        snapshot_interval: Duration::from_millis(100),
        flush_interval: Duration::from_secs(60),
        connect: ConnectOptions::default(),
        registry: RegistryOptions {
            poll_interval: Duration::from_millis(50),
            full_interval: Duration::from_secs(3600),
            command_interval: Duration::from_millis(1),
            command_batch: 10,
        },
    };
    tokio::time::timeout(
        Duration::from_secs(10),
        collector::run(options, auth, stop, cancel),
    )
    .await
    .unwrap()
    .unwrap();
    ws_server.await.unwrap();
    http_server.await.unwrap();
    let verified = replay::verify(&output, Some(&audit), 10).unwrap();
    assert_eq!(verified.updates, 2);
    assert_eq!(verified.symbols, 2);
    assert!(verified.periodic_checkpoints_verified > 0);
    let summary: Value = serde_json::from_reader(std::fs::File::open(summary).unwrap()).unwrap();
    assert_eq!(summary["desired"], 1);
    assert_eq!(summary["shards"][0]["retired_messages"], 1);
    assert_eq!(summary["complete_at_cutoff"], true);
}

#[tokio::test]
async fn multiple_shards_share_one_global_flush_timer() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel(2);
    let server = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let ready = ready_tx.clone();
            tasks.spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                let c = command(&mut ws).await;
                let symbol = c["params"]["market_tickers"][0].as_str().unwrap();
                event(
                    &mut ws,
                    json!({"id":c["id"],"type":"subscribed","msg":{"sid":7}}),
                )
                .await;
                event(&mut ws, snapshot(symbol, 1, "100")).await;
                ws.send(Message::Ping(vec![9].into())).await.unwrap();
                while let Some(message) = ws.next().await {
                    match message {
                        Ok(Message::Pong(p)) if p.as_ref() == [9] => {
                            let _ = ready.send(()).await;
                        }
                        Ok(Message::Ping(p)) => ws.send(Message::Pong(p)).await.unwrap(),
                        Ok(Message::Close(_)) => {
                            let _ = ws.flush().await;
                            break;
                        }
                        _ => {}
                    }
                }
            });
        }
        while let Some(r) = tasks.join_next().await {
            r.unwrap();
        }
    });
    let (cancel, stop) = tokio::sync::watch::channel(false);
    let pem = SigningKey::from_bytes(&[4; 32])
        .to_pkcs8_pem(Default::default())
        .unwrap();
    let auth = Arc::new(Credentials::from_pem("test".into(), pem.as_str()).unwrap());
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("out.sbe");
    let summary = temp.path().join("summary.json");
    let options = Options {
        health: engineering_challenge::health::Options::default(),
        replicas: 1,
        replica_lag: Duration::from_secs(2),
        failover_test_after: None,
        symbols: vec!["A".into(), "B".into()],
        rest_url: "unused".into(),
        ws_url: format!("ws://{addr}/trade-api/ws/v2"),
        symbols_per_connection: 1,
        max_connections: 2,
        max_symbols: 10,
        output: output.clone(),
        audit: None,
        summary: Some(summary.clone()),
        snapshot_interval: Duration::from_secs(60),
        flush_interval: Duration::from_secs(3),
        connect: ConnectOptions::default(),
        registry: RegistryOptions {
            command_interval: Duration::from_millis(1),
            ..RegistryOptions::default()
        },
    };
    let work = tokio::spawn(collector::run(options, auth, stop, cancel.clone()));
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(5), ready_rx.recv())
            .await
            .unwrap()
            .unwrap();
    }
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(3)).await;
    // Keep a runnable task while waiting for real disk I/O: an idle paused
    // runtime would otherwise auto-advance its timers past the writer's work.
    let wall_deadline = std::time::Instant::now() + Duration::from_secs(10);
    let chunk_size = engineering_challenge::output::CHUNK_SIZE as u64;
    while std::fs::metadata(&output).unwrap().len() < chunk_size
        && std::time::Instant::now() < wall_deadline
    {
        tokio::task::yield_now().await;
    }
    let timer_flushed = std::fs::metadata(&output).unwrap().len() >= chunk_size;
    // Shutdown joins a real OS thread. Its 15-second Tokio timeout must use
    // wall time, rather than auto-expiring as soon as async tasks are idle.
    tokio::time::resume();
    cancel.send(true).unwrap();
    work.await.unwrap().unwrap();
    server.await.unwrap();
    assert!(timer_flushed, "global timer did not flush a chunk in 10s");
    let report: Value = serde_json::from_reader(std::fs::File::open(summary).unwrap()).unwrap();
    assert_eq!(report["writer"]["flush_requests"], 1);
    assert_eq!(
        std::fs::metadata(output).unwrap().len(),
        engineering_challenge::output::CHUNK_SIZE as u64
    );
}
