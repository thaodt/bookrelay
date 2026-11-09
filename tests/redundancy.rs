use ed25519_dalek::{SigningKey, pkcs8::EncodePrivateKey};
use engineering_challenge::{
    auth::Credentials,
    collector::{self, Options, RegistryOptions},
    output::{AuditRow, SnapshotReason},
    replay,
    transport::ConnectOptions,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn active_connection_fault_switches_with_a_snapshot_then_recovers_redundancy() {
    run_case(None).await;
}

#[tokio::test]
async fn invalid_sid_timeout_and_buffer_overflow_recreate_the_subscription() {
    tokio::join!(run_case(Some(7)), run_case(Some(18)), run_case(Some(25)));
}

async fn run_case(reject_once: Option<u64>) {
    let rejected = Arc::new(AtomicBool::new(false));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let latest = Arc::new(AtomicU64::new(1));
    let clock = latest.clone();
    let (cancel, stop) = tokio::sync::watch::channel(false);
    let mut clock_stop = stop.clone();
    let ticker = tokio::spawn(async move {
        loop {
            tokio::select! {_=collector::stopped(&mut clock_stop)=>break,_=tokio::time::sleep(Duration::from_millis(5))=>{clock.fetch_add(1,Ordering::Relaxed);}}
        }
    });
    let mut server_stop = stop.clone();
    let server = tokio::spawn(async move {
        let mut feeds = tokio::task::JoinSet::new();
        loop {
            let (stream, _) = tokio::select! {r=listener.accept()=>r.unwrap(),_=collector::stopped(&mut server_stop)=>break};
            let latest = latest.clone();
            let rejected = rejected.clone();
            feeds.spawn(async move {
                let mut ws=tokio_tungstenite::accept_async(stream).await.unwrap();let mut seq=0u64;let mut last=0u64;let mut subscribed=false;
                let mut tick=tokio::time::interval(Duration::from_millis(5));
                loop {tokio::select! {
                    message=ws.next()=>match message {
                        Some(Ok(Message::Text(text)))=>{
                            let c:Value=serde_json::from_str(&text).unwrap();let is_sub=c["cmd"]=="subscribe";
                            if is_sub && let Some(code) = reject_once && !rejected.swap(true, Ordering::Relaxed) {
                                ws.send(Message::Text(json!({"id":c["id"],"type":"error","msg":{"code":code,"msg":"injected recoverable rejection"}}).to_string().into())).await.unwrap();
                                continue;
                            }
                            if is_sub {ws.send(Message::Text(json!({"id":c["id"],"type":"subscribed","msg":{"sid":1}}).to_string().into())).await.unwrap();}
                            else {seq+=1;ws.send(Message::Text(json!({"id":c["id"],"sid":1,"seq":seq,"type":"ok","msg":{}}).to_string().into())).await.unwrap();}
                            last=latest.load(Ordering::Relaxed);seq+=1;subscribed=true;
                            ws.send(Message::Text(json!({"type":"orderbook_snapshot","sid":1,"seq":seq,"sending_ts_ms":1700000000000i64,"msg":{"market_ticker":"A","clock":last,"yes_dollars_fp":[["0.42",(100+last).to_string()]]}}).to_string().into())).await.unwrap();
                        },
                        Some(Ok(Message::Ping(p)))=>{if ws.send(Message::Pong(p)).await.is_err(){break;}},
                        Some(Ok(Message::Close(_)))=>{let _=ws.flush().await;break;},None|Some(Err(_))=>break,_=>{},
                    },
                    _=tick.tick(),if subscribed=>{
                        let now=latest.load(Ordering::Relaxed);
                        while last<now {
                            last+=1;seq+=1;
                            if ws.send(Message::Text(json!({"type":"orderbook_delta","sid":1,"seq":seq,"sending_ts_ms":1700000000000i64,"msg":{"market_ticker":"A","clock":last,"side":"yes","price_dollars":"0.42","delta_fp":"1"}}).to_string().into())).await.is_err(){return;}
                        }
                    }
                }}
            });
        }
        while let Some(result) = feeds.join_next().await {
            result.unwrap();
        }
    });
    let pem = SigningKey::from_bytes(&[5; 32])
        .to_pkcs8_pem(Default::default())
        .unwrap();
    let auth = Arc::new(Credentials::from_pem("test".into(), pem.as_str()).unwrap());
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("out.sbe");
    let audit = temp.path().join("audit.jsonl");
    let summary = temp.path().join("summary.json");
    let options = Options {
        health: engineering_challenge::health::Options::default(),
        replicas: 2,
        replica_lag: Duration::from_millis(500),
        failover_test_after: reject_once.is_none().then_some(Duration::from_millis(1000)),
        symbols: vec!["A".into()],
        rest_url: "unused".into(),
        ws_url: format!("ws://{address}/trade-api/ws/v2"),
        symbols_per_connection: 10,
        max_connections: 2,
        max_symbols: 10,
        output: output.clone(),
        audit: Some(audit.clone()),
        summary: Some(summary.clone()),
        snapshot_interval: Duration::from_millis(100),
        flush_interval: Duration::from_secs(60),
        connect: ConnectOptions::default(),
        registry: RegistryOptions {
            command_interval: Duration::from_millis(1),
            ..RegistryOptions::default()
        },
    };
    let finish = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(4)).await;
        finish.send(true).unwrap();
    });
    tokio::time::timeout(
        Duration::from_secs(10),
        collector::run(options, auth, stop, cancel),
    )
    .await
    .unwrap()
    .unwrap();
    server.await.unwrap();
    ticker.await.unwrap();
    let replay = replay::verify(&output, Some(&audit), 10).unwrap();
    assert!(replay.updates > 100);
    assert!(replay.periodic_checkpoints_verified > 0);
    if reject_once.is_none() {
        assert!(replay.failover_baselines_verified > 0);
    }
    let report: Value = serde_json::from_reader(std::fs::File::open(summary).unwrap()).unwrap();
    assert_eq!(report["complete_at_cutoff"], true);
    assert_eq!(
        report["shards"][0]["injected_disconnects"],
        u64::from(reject_once.is_none())
    );
    assert_eq!(report["shards"][0]["replica_initialized"], json!([1, 1]));
    assert!(report["shards"][0]["connects"].as_u64().unwrap() >= 3);
    assert_eq!(report["shards"][0]["continuity_gaps"], 0);
    let rows: Vec<AuditRow> = std::fs::read_to_string(&audit)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    if reject_once.is_none() {
        assert!(rows.iter().any(|r| r.reason == SnapshotReason::Failover));
    }
    assert!(rows.iter().all(|r| r.source.is_some()));
    // A source change hidden inside a periodic checkpoint must fail even when
    // its quantities still agree: correctness includes provenance/barriers.
    let mut tampered: Vec<Value> = std::fs::read_to_string(&audit)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    let row = tampered
        .iter_mut()
        .find(|r| r["reason"] == "periodic")
        .unwrap();
    row["source"]["replica"] = json!(99);
    let bad = temp.path().join("bad.audit.jsonl");
    std::fs::write(
        &bad,
        tampered
            .iter()
            .map(|r| r.to_string() + "\n")
            .collect::<String>(),
    )
    .unwrap();
    assert!(
        replay::verify(&output, Some(&bad), 10)
            .unwrap_err()
            .to_string()
            .contains("source without a baseline")
    );
}

#[tokio::test]
async fn a_peer_that_never_echoes_close_is_counted_and_the_cut_still_succeeds() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (release, held) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let text = loop {
            if let Message::Text(text) = ws.next().await.unwrap().unwrap() {
                break text;
            }
        };
        let command: Value = serde_json::from_str(&text).unwrap();
        for reply in [
            json!({"id":command["id"],"type":"subscribed","msg":{"sid":1}}),
            json!({"type":"orderbook_snapshot","sid":1,"seq":1,"sending_ts_ms":1700000000000i64,
                "msg":{"market_ticker":"A","clock":1,"yes_dollars_fp":[["0.42","100"]]}}),
        ] {
            ws.send(Message::Text(reply.to_string().into()))
                .await
                .unwrap();
        }
        // The socket stays open but is never read again: Close gets no echo.
        let _ = held.await;
    });
    let pem = SigningKey::from_bytes(&[5; 32])
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
        replica_lag: Duration::from_millis(500),
        failover_test_after: None,
        symbols: vec!["A".into()],
        rest_url: "unused".into(),
        ws_url: format!("ws://{address}/trade-api/ws/v2"),
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
            command_interval: Duration::from_millis(1),
            ..RegistryOptions::default()
        },
    };
    let (cancel, stop) = tokio::sync::watch::channel(false);
    let finish = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        finish.send(true).unwrap();
    });
    tokio::time::timeout(
        Duration::from_secs(20),
        collector::run(options, auth, stop, cancel),
    )
    .await
    .unwrap()
    .unwrap();
    drop(release);
    server.await.unwrap();
    let replay = replay::verify(&output, Some(&audit), 10).unwrap();
    assert!(replay.periodic_checkpoints_verified > 0);
    let report: Value = serde_json::from_reader(std::fs::File::open(summary).unwrap()).unwrap();
    assert_eq!(report["complete_at_cutoff"], true);
    assert_eq!(report["shards"][0]["unacknowledged_closes"], 1);
}
