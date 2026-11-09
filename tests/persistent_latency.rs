use ed25519_dalek::{SigningKey, pkcs8::EncodePrivateKey};
use engineering_challenge::{
    auth::Credentials, collector::stopped, latency, transport::ConnectOptions,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn one_reference_survives_three_distinct_candidate_generations_without_queue_overflow() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let server_count = accepted.clone();
    let clock = Arc::new(AtomicU64::new(1));
    let timer_clock = clock.clone();
    let (cancel, stop) = tokio::sync::watch::channel(false);
    let mut timer_stop = stop.clone();
    let timer = tokio::spawn(async move {
        loop {
            tokio::select! {_=stopped(&mut timer_stop)=>break,_=tokio::time::sleep(Duration::from_millis(2))=>{timer_clock.fetch_add(1,Ordering::Relaxed);}}
        }
    });
    let mut server_stop = stop.clone();
    let server = tokio::spawn(async move {
        let mut feeds = tokio::task::JoinSet::new();
        loop {
            let (stream, _) =
                tokio::select! {r=listener.accept()=>r.unwrap(),_=stopped(&mut server_stop)=>break};
            server_count.fetch_add(1, Ordering::Relaxed);
            let clock = clock.clone();
            feeds.spawn(async move {
                let mut ws=tokio_tungstenite::accept_async(stream).await.unwrap();let mut seq=0;let mut last=0;let mut ready=false;let mut tick=tokio::time::interval(Duration::from_millis(2));
                loop {tokio::select! {
                    message=ws.next()=>match message {
                        Some(Ok(Message::Text(t)))=>{
                            let command:Value=serde_json::from_str(&t).unwrap();assert_eq!(command["cmd"],"subscribe");
                            last=clock.load(Ordering::Relaxed);seq+=1;ready=true;
                            for v in [json!({"id":1,"type":"subscribed","msg":{"sid":1}}),json!({"type":"orderbook_snapshot","sid":1,"seq":seq,"sending_ts_ms":1700000000000i64,"msg":{"market_ticker":"A","clock":last,"yes_dollars_fp":[["0.42",(100+last).to_string()]]}})] {ws.send(Message::Text(v.to_string().into())).await.unwrap();}
                        },
                        Some(Ok(Message::Ping(p)))=>{let _=ws.send(Message::Pong(p)).await;},
                        Some(Ok(Message::Close(_)))=>{let _=ws.flush().await;break;},None|Some(Err(_))=>break,_=>{},
                    },
                    _=tick.tick(),if ready=>{
                        let now=clock.load(Ordering::Relaxed);
                        while last<now {last+=1;seq+=1;let v=json!({"type":"orderbook_delta","sid":1,"seq":seq,"sending_ts_ms":1700000000000i64,"msg":{"market_ticker":"A","clock":last,"side":"yes","price_dollars":"0.42","delta_fp":"1"}});
                            if ws.send(Message::Text(v.to_string().into())).await.is_err(){return;}
                        }
                    }
                }}
            });
        }
        while let Some(result) = feeds.join_next().await {
            result.unwrap();
        }
    });
    let pem = SigningKey::from_bytes(&[6; 32])
        .to_pkcs8_pem(Default::default())
        .unwrap();
    let auth = Arc::new(Credentials::from_pem("test".into(), pem.as_str()).unwrap());
    let report = tokio::time::timeout(
        Duration::from_secs(10),
        latency::run(
            latency::Options {
                persistent_reference: true,
                ws_url: format!("ws://{address}/trade-api/ws/v2"),
                symbols: vec!["A".into()],
                trials: 3,
                warmup: Duration::ZERO,
                duration: Duration::from_millis(150),
                ports: vec![],
                connect: ConnectOptions::default(),
                sample_cap: 1000,
                pending_cap: 1000,
                pending_bytes: 1 << 20,
                ttl: Duration::from_secs(1),
            },
            auth,
            stop,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    cancel.send(true).unwrap();
    server.await.unwrap();
    timer.await.unwrap();
    assert_eq!(accepted.load(Ordering::Relaxed), 4);
    assert!(
        report.reference_failure.is_none(),
        "{:?}",
        report.reference_failure
    );
    assert_eq!(report.trials.len(), 3);
    assert_eq!(report.reference.as_ref().unwrap().generation, 1);
    assert!(report.reference.as_ref().unwrap().clean_close);
    for (index, trial) in report.trials.iter().enumerate() {
        assert_eq!(trial.reference_generation, 1);
        assert_eq!(trial.candidate_generation, index as u64 + 1);
        assert!(trial.failure.is_none(), "{:?}", trial.failure);
        assert!(trial.all.n > 5);
        assert!(trial.holdout.n > 0);
        assert_eq!(trial.feeds.len(), 1);
        assert!(trial.feeds[0].clean_close);
    }
}
