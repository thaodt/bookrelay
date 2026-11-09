//! A paced public connection must subscribe before its first text heartbeat.
use engineering_challenge::{
    collector::{self, Options, RegistryOptions},
    exchange::{Configuration, polymarket::Polymarket},
    health, replay,
    transport::ConnectOptions,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn every_paced_public_feed_subscribes_before_sending_text_ping() {
    tokio::time::timeout(Duration::from_secs(12), run())
        .await
        .unwrap();
}

async fn run() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (ready_tx, mut ready_rx) = mpsc::channel(2);
    let (cancel, stop) = watch::channel(false);
    let server = tokio::spawn(async move {
        let mut peers = tokio::task::JoinSet::new();
        for connection in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let ready = ready_tx.clone();
            peers.spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                let mut first_text = None;
                let mut subscribed = false;
                let mut ready = Some(ready);
                while let Some(message) = ws.next().await {
                    match message.unwrap() {
                        Message::Text(text) => {
                            first_text.get_or_insert_with(|| text.to_string());
                            if text == "PING" {
                                ws.send(Message::Text("PONG".into())).await.unwrap();
                                continue;
                            }
                            assert!(!subscribed, "unexpected in-place public command");
                            let command: Value = serde_json::from_str(&text).unwrap();
                            assert_eq!(command, json!({"type":"market", "assets_ids":["123"], "initial_dump":true, "custom_feature_enabled":true}));
                            subscribed = true;
                            ws.send(Message::Text(json!({"event_type":"book", "asset_id":"123", "timestamp":"1700000000000", "bids":[{"price":"0.4","size":"10"}], "asks":[]}).to_string().into())).await.unwrap();
                            ws.send(Message::Ping(vec![connection].into())).await.unwrap();
                        }
                        Message::Pong(p) if p.as_ref() == [connection] => {
                            // This proves the preceding baseline entered the
                            // shared FIFO relay before requesting shutdown.
                            ready.take().unwrap().send(()).await.unwrap();
                        }
                        Message::Ping(p) => ws.send(Message::Pong(p)).await.unwrap(),
                        Message::Close(_) => {
                            ws.flush().await.unwrap();
                            assert!(subscribed);
                            return (connection, first_text.unwrap());
                        }
                        other => panic!("unexpected public startup frame: {other:?}"),
                    }
                }
                panic!("public source dropped before its Close handshake");
            });
        }
        drop(ready_tx);
        let mut first_frames = vec![];
        while let Some(peer) = peers.join_next().await {
            first_frames.push(peer.unwrap());
        }
        first_frames.sort_by_key(|(connection, _)| *connection);
        first_frames
    });
    let temp = tempfile::tempdir().unwrap();
    let options = Options {
        symbols: vec!["123".into()],
        rest_url: "unused".into(),
        ws_url: format!("ws://{address}/ws/market"),
        symbols_per_connection: 1,
        max_connections: 2,
        max_symbols: 1,
        replicas: 2,
        replica_lag: Duration::from_millis(250),
        failover_test_after: None,
        output: temp.path().join("out.sbe"),
        audit: Some(temp.path().join("audit.jsonl")),
        summary: Some(temp.path().join("summary.json")),
        snapshot_interval: Duration::from_secs(60),
        flush_interval: Duration::from_secs(60),
        connect: ConnectOptions::default(),
        registry: RegistryOptions {
            command_interval: Duration::from_secs(1),
            command_batch: 1,
            ..RegistryOptions::default()
        },
        health: health::Options {
            rotation_interval: Duration::ZERO,
            ..health::Options::default()
        },
    };
    let work = collector::run_with_exchange(
        options.clone(),
        Configuration {
            protocol: Arc::new(Polymarket),
            auth: None,
            catalog: None,
        },
        stop,
        cancel.clone(),
    );
    tokio::pin!(work);
    let initialized = async {
        ready_rx.recv().await.unwrap();
        ready_rx.recv().await.unwrap();
        cancel.send(true).unwrap();
    };
    // No detached collector task: cancellation or a test panic drops its
    // future and its owned source tasks together.
    let (result, ()) = tokio::join!(&mut work, initialized);
    result.unwrap();
    let first_frames = server.await.unwrap();
    let report = replay::verify(&options.output, options.audit.as_deref(), 1).unwrap();
    assert_eq!(report.symbols, 1);
    assert!(report.snapshot_only_baselines > 0);
    assert_eq!(report.comparable_clock_baselines_verified, 0);
    let summary: Value =
        serde_json::from_slice(&std::fs::read(options.summary.as_ref().unwrap()).unwrap()).unwrap();
    assert_eq!(summary["complete_at_cutoff"], true);
    assert_eq!(summary["shards"][0]["replica_initialized"], json!([1, 1]));
    assert_eq!(summary["shards"][0]["unacknowledged_closes"], 0);
    assert_eq!(first_frames.len(), 2);
    for (connection, first) in first_frames {
        assert_ne!(
            first, "PING",
            "public connection {connection} sent a heartbeat before its initial Subscribe"
        );
        let initial: Value = serde_json::from_str(&first).unwrap();
        assert_eq!(initial["type"], "market");
    }
}
