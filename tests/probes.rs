use ed25519_dalek::{SigningKey, pkcs8::EncodePrivateKey};
use engineering_challenge::{auth::Credentials, latency, pilot, transport::ConnectOptions};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio_tungstenite::tungstenite::Message;
fn auth() -> Arc<Credentials> {
    let pem = SigningKey::from_bytes(&[3; 32])
        .to_pkcs8_pem(Default::default())
        .unwrap();
    Arc::new(Credentials::from_pem("test".into(), pem.as_str()).unwrap())
}
fn snapshot(symbol: &str, seq: u64) -> Value {
    json!({"type":"orderbook_snapshot","sid":1,"seq":seq,"sending_ts_ms":1700000000000i64,"msg":{"market_ticker":symbol,"clock":0,"yes_dollars_fp":[["0.42","100"]]}})
}
async fn next_command(ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>) -> Value {
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(t) => return serde_json::from_str(&t).unwrap(),
            Message::Ping(p) => ws.send(Message::Pong(p)).await.unwrap(),
            _ => {}
        }
    }
}
#[tokio::test]
async fn capacity_reports_verified_floor_and_stops_at_expected_rejection() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let c = next_command(&mut ws).await;
        assert_eq!(c["params"]["market_tickers"], json!(["A"]));
        ws.send(Message::Text(
            json!({"id":c["id"],"type":"subscribed","msg":{"sid":1}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        ws.send(Message::Text(snapshot("A", 1).to_string().into()))
            .await
            .unwrap();
        let c = next_command(&mut ws).await;
        assert_eq!(c["params"]["action"], "add_markets");
        ws.send(Message::Text(
            json!({"id":c["id"],"type":"error","msg":{"code":26,"msg":"test cap"}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        while let Some(message) = ws.next().await {
            if matches!(message, Ok(Message::Close(_))) {
                ws.flush().await.unwrap();
                break;
            }
        }
    });
    let (_cancel, stop) = tokio::sync::watch::channel(false);
    let report = pilot::run(
        pilot::Options {
            ws_url: format!("ws://{addr}/trade-api/ws/v2"),
            symbols: vec!["A".into(), "B".into()],
            steps: vec![1, 2],
            dwell: Duration::from_millis(10),
            step_timeout: Duration::from_secs(2),
            command_gap: Duration::from_millis(100),
            connect: ConnectOptions::default(),
        },
        &auth(),
        stop,
    )
    .await
    .unwrap();
    assert_eq!(report.largest_verified, 1);
    assert_eq!(report.rejection_code, Some(26));
    assert!(report.clean_close);
    server.await.unwrap();
}

#[tokio::test]
async fn paired_probe_bootstraps_both_feeds_matches_groups_and_drains_cleanly() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let server = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let barrier = barrier.clone();
            connections.spawn(async move {
                let mut ws=tokio_tungstenite::accept_async(stream).await.unwrap();let c=next_command(&mut ws).await;
                ws.send(Message::Text(json!({"id":c["id"],"type":"subscribed","msg":{"sid":1}}).to_string().into())).await.unwrap();
                ws.send(Message::Text(snapshot("A",1).to_string().into())).await.unwrap();barrier.wait().await;
                let mut tick=tokio::time::interval(Duration::from_millis(5));let mut seq=1;
                loop {tokio::select! {
                    _=tick.tick()=>{seq+=1;let frame=json!({"type":"orderbook_delta","sid":1,"seq":seq,"sending_ts_ms":1700000000000i64,"msg":{"market_ticker":"A","clock":seq,"side":"yes","price_dollars":"0.42","delta_fp":"1"}});
                        if ws.send(Message::Text(frame.to_string().into())).await.is_err(){break;}
                    },
                    message=ws.next()=>match message {
                        Some(Ok(Message::Ping(p)))=>{ws.send(Message::Pong(p)).await.unwrap();},
                        Some(Ok(Message::Close(_)))=>{let _=ws.flush().await;break;},None|Some(Err(_))=>break,_=>{},
                    }
                }}
            });
        }
        while let Some(result) = connections.join_next().await {
            result.unwrap();
        }
    });
    let (_cancel, stop) = tokio::sync::watch::channel(false);
    let report = tokio::time::timeout(
        Duration::from_secs(5),
        latency::run(
            latency::Options {
                persistent_reference: false,
                ws_url: format!("ws://{addr}/trade-api/ws/v2"),
                symbols: vec!["A".into()],
                trials: 1,
                warmup: Duration::ZERO,
                duration: Duration::from_millis(250),
                ports: vec![],
                connect: ConnectOptions::default(),
                sample_cap: 1000,
                pending_cap: 1000,
                pending_bytes: 1 << 20,
                ttl: Duration::from_secs(1),
            },
            auth(),
            stop,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let trial = &report.trials[0];
    assert!(trial.failure.is_none(), "{:?}", trial.failure);
    assert!(trial.all.n > 10);
    assert!(trial.holdout.n > 0);
    assert_eq!(trial.feeds.len(), 2);
    assert!(trial.feeds.iter().all(|f| f.clean_close));
    server.await.unwrap();
}
