use ed25519_dalek::{SigningKey, pkcs8::EncodePrivateKey};
use engineering_challenge::{
    auth::Credentials,
    collector::{self, Options, RegistryOptions},
    output::CHUNK_SIZE,
    replay,
    transport::ConnectOptions,
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain},
    },
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn sequence_gap_quarantines_deltas_and_recovers_in_place_before_clean_shutdown() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (cancel, stop) = tokio::sync::watch::channel(false);
    let server_stop = cancel.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        loop {
            if let Message::Text(text) = ws.next().await.unwrap().unwrap() {
                assert_eq!(
                    serde_json::from_str::<Value>(&text).unwrap()["cmd"],
                    "subscribe"
                );
                break;
            }
        }
        let snapshot = |seq, quantity: &str| {
            json!({"type":"orderbook_snapshot","sid":1,"seq":seq,"sending_ts_ms":1700000000000i64,
            "msg":{"market_ticker":"TEST","clock":seq,"yes_dollars_fp":[["0.42",quantity]]}})
        };
        let delta = |seq, quantity: &str| {
            json!({"type":"orderbook_delta","sid":1,"seq":seq,"sending_ts_ms":1700000000000i64,
            "msg":{"market_ticker":"TEST","clock":seq,"side":"yes","price_dollars":"0.42","delta_fp":quantity}})
        };
        for event in [
            json!({"id":1,"type":"subscribed","msg":{"sid":1}}),
            snapshot(1, "100"),
            delta(2, "-25"),
            delta(4, "-10"),
        ] {
            ws.send(Message::Text(event.to_string().into()))
                .await
                .unwrap();
        }
        let snapshot_request = loop {
            if let Message::Text(text) = ws.next().await.unwrap().unwrap() {
                let command: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(command["cmd"], "update_subscription");
                assert_eq!(command["params"]["action"], "get_snapshot");
                assert_eq!(command["params"]["market_tickers"], json!(["TEST"]));
                break command["id"].clone();
            }
        };
        let mut recovery = snapshot(6, "60");
        recovery["id"] = snapshot_request.clone();
        for event in [
            json!({"id": snapshot_request, "type":"ok","sid":1,"seq":5,"msg":{}}),
            recovery,
            delta(7, "5"),
        ] {
            ws.send(Message::Text(event.to_string().into()))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
        server_stop.send(true).unwrap();
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(_))) => {
                    ws.flush().await.unwrap();
                    break;
                }
                Some(Ok(_)) => {}
                _ => break,
            }
        }
    });
    let key = SigningKey::from_bytes(&[9; 32])
        .to_pkcs8_pem(Default::default())
        .unwrap();
    let auth = Arc::new(Credentials::from_pem("test-key-id".into(), key.as_str()).unwrap());
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("capture.sbe");
    let audit = temp.path().join("audit.jsonl");
    let options = Options {
        health: engineering_challenge::health::Options::default(),
        replicas: 1,
        replica_lag: Duration::from_secs(2),
        failover_test_after: None,
        symbols: vec!["TEST".into()],
        rest_url: "unused".into(),
        ws_url: format!("ws://{address}/trade-api/ws/v2"),
        symbols_per_connection: 10,
        max_connections: 1,
        max_symbols: 10,
        output: output.clone(),
        audit: Some(audit.clone()),
        snapshot_interval: Duration::from_millis(100),
        flush_interval: Duration::from_secs(60),
        registry: collector::RegistryOptions::default(),
        summary: None,
        connect: ConnectOptions {
            source_port: Some(0),
            remote_ip: None,
        },
    };
    tokio::time::timeout(
        Duration::from_secs(10),
        collector::run(options, auth, stop, cancel),
    )
    .await
    .unwrap()
    .unwrap();
    server.await.unwrap();
    let report = replay::verify(&output, Some(&audit), 10).unwrap();
    assert_eq!(report.updates, 2);
    assert!(report.periodic_checkpoints_verified > 0);
    assert_eq!(report.explicit_baseline_resets, 2);
    assert_eq!(std::fs::metadata(&output).unwrap().len(), CHUNK_SIZE as u64);
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&output).unwrap();
    let mut quantities = vec![];
    while let Some(event) = reader.read_msg().unwrap() {
        if let AnyMessage::PriceLevelUpdate(u) = event {
            quantities.push(
                PriceLevelUpdateDomain::try_from_decoder(u)
                    .unwrap()
                    .quantity,
            );
        }
    }
    assert_eq!(quantities, [75_000_000, 65_000_000]);
}

#[tokio::test]
async fn fragmented_snapshot_survives_control_frames_before_publication() {
    use tokio::io::AsyncWriteExt;
    async fn fragment(
        ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        opcode: u8,
        payload: &[u8],
    ) {
        let mut bytes = vec![opcode]; // Server frames are unmasked.
        if payload.len() < 126 {
            bytes.push(payload.len() as u8);
        } else {
            bytes.push(126);
            bytes.extend_from_slice(&u16::try_from(payload.len()).unwrap().to_be_bytes());
        }
        bytes.extend_from_slice(payload);
        ws.get_mut().write_all(&bytes).await.unwrap();
    }
    async fn pong(
        ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        marker: &[u8],
    ) {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Pong(p) if p.as_ref() == marker => return,
                Message::Ping(p) => ws.send(Message::Pong(p)).await.unwrap(),
                _ => {}
            }
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (cancel, stop) = tokio::sync::watch::channel(false);
    let server_stop = cancel.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let id = loop {
            if let Message::Text(t) = ws.next().await.unwrap().unwrap() {
                let command: Value = serde_json::from_str(&t).unwrap();
                assert_eq!(command["cmd"], "subscribe");
                break command["id"].clone();
            }
        };
        ws.send(Message::Text(
            json!({"id":id,"type":"subscribed","msg":{"sid":1}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let snapshot = json!({"type":"orderbook_snapshot","sid":1,"seq":1,"sending_ts_ms":1700000000000i64,"msg":{"market_ticker":"TEST","clock":1,"yes_dollars_fp":[["0.42","100"]]}}).to_string();
        let split = snapshot.len() / 2;
        fragment(&mut ws, 0x01, &snapshot.as_bytes()[..split]).await;
        // A control frame is legal between message fragments. The client must
        // handle Ping, leave that select branch, and retain the incomplete JSON.
        ws.send(Message::Ping(vec![71].into())).await.unwrap();
        pong(&mut ws, &[71]).await;
        fragment(&mut ws, 0x80, &snapshot.as_bytes()[split..]).await;
        let delta = json!({"type":"orderbook_delta","sid":1,"seq":2,"sending_ts_ms":1700000000000i64,"msg":{"market_ticker":"TEST","clock":2,"side":"yes","price_dollars":"0.42","delta_fp":"5"}});
        ws.send(Message::Text(delta.to_string().into()))
            .await
            .unwrap();
        ws.send(Message::Ping(vec![72].into())).await.unwrap();
        pong(&mut ws, &[72]).await;
        server_stop.send(true).unwrap();
        while let Some(message) = ws.next().await {
            if matches!(message, Ok(Message::Close(_))) {
                ws.flush().await.unwrap();
                break;
            }
        }
    });
    let pem = SigningKey::from_bytes(&[8; 32])
        .to_pkcs8_pem(Default::default())
        .unwrap();
    let auth = Arc::new(Credentials::from_pem("test".into(), pem.as_str()).unwrap());
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("out.sbe");
    let audit = temp.path().join("audit.jsonl");
    let options = Options {
        health: engineering_challenge::health::Options::default(),
        symbols: vec!["TEST".into()],
        rest_url: "unused".into(),
        ws_url: format!("ws://{address}/trade-api/ws/v2"),
        symbols_per_connection: 10,
        max_connections: 1,
        max_symbols: 10,
        output: output.clone(),
        audit: Some(audit.clone()),
        summary: None,
        snapshot_interval: Duration::from_secs(60),
        flush_interval: Duration::from_secs(60),
        connect: ConnectOptions::default(),
        registry: RegistryOptions::default(),
        replicas: 1,
        replica_lag: Duration::from_secs(2),
        failover_test_after: None,
    };
    tokio::time::timeout(
        Duration::from_secs(5),
        collector::run(options, auth, stop, cancel),
    )
    .await
    .unwrap()
    .unwrap();
    server.await.unwrap();
    let report = replay::verify(&output, Some(&audit), 10).unwrap();
    assert_eq!(report.snapshots, 1);
    assert_eq!(report.updates, 1);
    assert_eq!(report.source_baselines_verified, 1);
}
