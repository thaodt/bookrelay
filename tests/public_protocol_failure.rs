//! A useful public asset cannot keep an unrecoverable peer payload retrying.
//! These localhost failures exercise the shared runtime and its real output.
use engineering_challenge::{
    Side,
    collector::{self, Options, RegistryOptions},
    exchange::{Configuration, polymarket::Polymarket},
    health, replay,
    transport::ConnectOptions,
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
    },
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{io::AsyncWriteExt, net::TcpListener, sync::watch};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

type Socket = WebSocketStream<tokio::net::TcpStream>;
const DEADLINE: Duration = Duration::from_secs(8);

#[derive(Clone, Copy)]
enum Failure {
    InvalidBookSize,
    OversizedFrame,
}

async fn connect(listener: &TcpListener) -> Socket {
    let (stream, _) = listener.accept().await.unwrap();
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(text) if text == "PING" => {
                ws.send(Message::Text("PONG".into())).await.unwrap();
            }
            Message::Text(text) => {
                let command: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(
                    command,
                    json!({"type":"market", "assets_ids":["123","456"],
                        "initial_dump":true, "custom_feature_enabled":true})
                );
                return ws;
            }
            Message::Ping(payload) => ws.send(Message::Pong(payload)).await.unwrap(),
            other => panic!("unexpected public initialization command: {other:?}"),
        }
    }
}

async fn event(ws: &mut Socket, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

fn book(asset: &str, size: &str) -> Value {
    json!({"event_type":"book", "asset_id":asset, "timestamp":"1700000000000",
        "bids":[{"price":"0.4","size":size}], "asks":[{"price":"0.6","size":"8"}]})
}

// The source admits preceding data to its shared FIFO before answering Ping.
async fn barrier(ws: &mut Socket) {
    ws.send(Message::Ping(vec![7].into())).await.unwrap();
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Pong(payload) if payload.as_ref() == [7] => return,
            Message::Text(text) if text == "PING" => {
                ws.send(Message::Text("PONG".into())).await.unwrap();
            }
            Message::Ping(payload) => ws.send(Message::Pong(payload)).await.unwrap(),
            other => panic!("unexpected public control before the fault: {other:?}"),
        }
    }
}

async fn close_echo(ws: &mut Socket) {
    loop {
        match ws.next().await {
            Some(Ok(Message::Close(_))) => {
                ws.flush().await.unwrap();
                return;
            }
            Some(Ok(Message::Text(text))) if text == "PING" => {
                ws.send(Message::Text("PONG".into())).await.unwrap();
            }
            Some(Ok(Message::Ping(payload))) => ws.send(Message::Pong(payload)).await.unwrap(),
            other => panic!("public failure did not complete the Close handshake: {other:?}"),
        }
    }
}

async fn unrecoverable_public_payload(failure: Failure) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let options = Options {
        symbols: vec!["123".into(), "456".into()],
        rest_url: "unused".into(),
        ws_url: format!("ws://{}/ws/market", listener.local_addr().unwrap()),
        symbols_per_connection: 2,
        max_connections: 2,
        max_symbols: 2,
        replicas: 2,
        replica_lag: Duration::from_secs(2),
        failover_test_after: None,
        output: temp.path().join("capture.sbe"),
        audit: Some(temp.path().join("audit.jsonl")),
        summary: Some(temp.path().join("summary.json")),
        snapshot_interval: Duration::from_secs(60),
        flush_interval: Duration::from_secs(60),
        connect: ConnectOptions::default(),
        registry: RegistryOptions {
            command_interval: Duration::from_millis(1),
            ..RegistryOptions::default()
        },
        health: health::Options {
            rotation_interval: Duration::ZERO,
            ..health::Options::default()
        },
    };
    let (cancel, stop) = watch::channel(false);
    let collect = collector::run_with_exchange(
        options.clone(),
        Configuration {
            protocol: Arc::new(Polymarket),
            auth: None,
            catalog: None,
        },
        stop,
        cancel,
    );
    let exchange = async {
        let mut first = connect(&listener).await;
        let mut second = connect(&listener).await;
        for ws in [&mut first, &mut second] {
            event(ws, book("123", "10")).await;
            event(
                ws,
                json!({"event_type":"price_change", "timestamp":"1700000000001",
                    "price_changes":[{"asset_id":"123", "side":"BUY", "price":"0.4", "size":"12"}]}),
            )
            .await;
            barrier(ws).await;
        }
        // Both feeds have already initialized A. B has no usable baseline on
        // either feed, and the same unrecoverable payload must not be retried.
        for ws in [&mut first, &mut second] {
            match failure {
                Failure::InvalidBookSize => event(ws, book("456", "-1")).await,
                Failure::OversizedFrame => {
                    // A complete unmasked binary header advertises 4 MiB + 1.
                    // The frame cap must reject it before reading/allocating
                    // its body; no multi-megabyte fixture allocation is needed.
                    let mut header = vec![0x82, 127];
                    header.extend_from_slice(&((4u64 << 20) + 1).to_be_bytes());
                    ws.get_mut().write_all(&header).await.unwrap();
                }
            }
        }
        tokio::join!(close_echo(&mut first), close_echo(&mut second));
    };
    let (result, ()) = tokio::time::timeout(DEADLINE, async { tokio::join!(collect, exchange) })
        .await
        .expect("public protocol failure did not close, drain and join within 8s");
    let error = format!(
        "{:#}",
        result.expect_err("unrecoverable payload was accepted")
    );
    match failure {
        Failure::InvalidBookSize => {
            assert!(
                error.contains("public protocol normalization failed"),
                "{error}"
            );
            assert!(error.contains("negative Polymarket quantity"), "{error}");
        }
        Failure::OversizedFrame => assert!(error.contains("Message too long"), "{error}"),
    }
    assert!(!error.contains("reconnect budget exhausted"), "{error}");
    // The runtime has returned after joining its sources. Any extra dial would
    // now be observable in this listener's backlog; there is no surviving task
    // allowed to start a third connection later.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );

    let summary: Value =
        serde_json::from_slice(&std::fs::read(options.summary.as_ref().unwrap()).unwrap()).unwrap();
    assert_eq!(summary["complete_at_cutoff"], false);
    assert_eq!(summary["desired"], 2);
    let shard = &summary["shards"][0];
    assert_eq!(shard["desired"], 2);
    assert_eq!(shard["connects"], 2);
    assert_eq!(shard["unacknowledged_closes"], 0);
    assert!(shard["initialized"].as_u64().unwrap() < 2);
    assert!(
        shard["replica_initialized"]
            .as_array()
            .unwrap()
            .iter()
            .all(|count| count.as_u64().unwrap() < 2)
    );
    let report = replay::verify(&options.output, options.audit.as_deref(), 2).unwrap();
    assert_eq!(report.symbols, 1);
    assert!(report.updates > 0);
    assert!(report.snapshot_only_baselines > 0);
    assert_eq!(report.comparable_clock_baselines_verified, 0);
    assert_eq!(report.legacy_unspecified_baselines, 0);
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&options.output).unwrap();
    let mut final_bid = None;
    while let Some(record) = reader.read_msg().unwrap() {
        match record {
            AnyMessage::Snapshot(decoded) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoded).unwrap();
                assert_eq!(snapshot.symbol, "123", "B must never initialize");
                final_bid = snapshot
                    .bids
                    .iter()
                    .find(|level| level.price_level == 400_000)
                    .map(|level| level.quantity);
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap();
                assert_eq!(update.symbol, "123", "B must never publish a delta");
                assert_eq!(update.side, Side::Buy);
                assert_eq!(update.price_level, 400_000);
                final_bid = Some(update.quantity);
            }
            _ => panic!("unexpected SBE message"),
        }
    }
    assert_eq!(final_bid, Some(12_000_000));
}

#[tokio::test]
async fn malformed_public_book_after_a_valid_asset_fails_without_reconnecting() {
    unrecoverable_public_payload(Failure::InvalidBookSize).await;
}

#[tokio::test]
async fn oversized_public_frame_after_a_valid_asset_fails_before_body_without_reconnecting() {
    unrecoverable_public_payload(Failure::OversizedFrame).await;
}
