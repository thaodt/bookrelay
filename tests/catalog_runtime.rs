//! Catalog uncertainty through the real shared collector and public WS path.
//! These injected inventories test policy, not public catalog/live acceptance.
use engineering_challenge::{
    catalog::{Catalog, Instrument, Inventory},
    collector::{self, Options, RegistryOptions},
    exchange::{Configuration, polymarket::Polymarket},
    health,
    output::{AuditRow, SnapshotReason},
    replay,
    transport::ConnectOptions,
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
    },
};
use futures_util::{SinkExt, StreamExt, future::BoxFuture};
use serde_json::{Value, json};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{Mutex, mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

type Socket = WebSocketStream<tokio::net::TcpStream>;
const DEADLINE: Duration = Duration::from_secs(8);

#[derive(Clone, Copy)]
enum Scan {
    Initial,
    Uncertain,
    Clean,
}
impl Scan {
    fn metadata(self) -> Value {
        match self {
            Self::Initial => {
                json!({"scan":"initial", "assets":[{"asset_id":"123"}], "malformed_records":0})
            }
            Self::Uncertain => {
                json!({"scan":"uncertain", "assets":[{"asset_id":"456"}], "malformed_records":1})
            }
            Self::Clean => {
                json!({"scan":"clean", "assets":[{"asset_id":"456"}], "malformed_records":0})
            }
        }
    }
    fn inventory(self) -> Inventory {
        let (symbol, watermark, complete) = match self {
            Self::Initial => ("123", 1700000000, true),
            Self::Uncertain => ("456", 1700000001, false),
            Self::Clean => ("456", 1700000002, true),
        };
        Inventory {
            instruments: vec![Instrument {
                symbol: symbol.into(),
                eligible: true,
            }],
            watermark,
            pages: 1,
            elapsed: Duration::ZERO,
            authoritative_for_retirement: complete,
            complete,
            metadata: Some(self.metadata()),
        }
    }
}
struct Reply {
    scan: Scan,
    observed: oneshot::Sender<()>,
}
struct ScriptedCatalog(Mutex<mpsc::Receiver<Reply>>);
impl Catalog for ScriptedCatalog {
    fn scan(&self, since: Option<i64>) -> BoxFuture<'_, anyhow::Result<Inventory>> {
        Box::pin(async move {
            assert!(since.is_none(), "this fixture supplies full catalog scans");
            let reply = self
                .0
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("fixture catalog closed"))?;
            let _ = reply.observed.send(());
            Ok(reply.scan.inventory())
        })
    }
}
async fn scan(tx: &mpsc::Sender<Reply>, scan: Scan) {
    let (observed, wait) = oneshot::channel();
    tx.send(Reply { scan, observed }).await.unwrap();
    tokio::time::timeout(DEADLINE, wait).await.unwrap().unwrap();
}

fn options(temp: &tempfile::TempDir, address: SocketAddr) -> Options {
    Options {
        symbols: vec![],
        rest_url: "unused".into(),
        ws_url: format!("ws://{address}/ws/market"),
        symbols_per_connection: 10,
        max_connections: 1,
        max_symbols: 10,
        replicas: 1,
        replica_lag: Duration::from_millis(250),
        failover_test_after: None,
        output: temp.path().join("out.sbe"),
        audit: Some(temp.path().join("audit.jsonl")),
        summary: Some(temp.path().join("summary.json")),
        snapshot_interval: Duration::from_secs(60),
        flush_interval: Duration::from_secs(60),
        connect: ConnectOptions::default(),
        registry: RegistryOptions {
            poll_interval: Duration::from_millis(1),
            command_interval: Duration::from_millis(1),
            command_batch: 1,
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
struct Running {
    cancel: watch::Sender<bool>,
    task: Option<JoinHandle<anyhow::Result<()>>>,
}
impl Running {
    fn start(options: Options, catalog: Arc<dyn Catalog>) -> Self {
        let (cancel, stop) = watch::channel(false);
        let task = tokio::spawn(collector::run_with_exchange(
            options,
            Configuration {
                protocol: Arc::new(Polymarket),
                auth: None,
                catalog: Some(catalog),
            },
            stop,
            cancel.clone(),
        ));
        Self {
            cancel,
            task: Some(task),
        }
    }
    async fn finish(mut self, mut ws: Socket) {
        let mut peers = tokio::task::JoinSet::new();
        peers.spawn(async move {
            assert!(matches!(frame(&mut ws).await, Message::Close(_)));
            ws.flush().await.unwrap();
        });
        self.cancel.send(true).unwrap();
        tokio::time::timeout(DEADLINE, self.task.as_mut().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.task.take();
        peers.join_next().await.unwrap().unwrap();
        assert!(peers.is_empty());
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
async fn frame(ws: &mut Socket) -> Message {
    tokio::time::timeout(DEADLINE, async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) if text == "PING" => {
                    ws.send(Message::Text("PONG".into())).await.unwrap()
                }
                Message::Ping(p) => ws.send(Message::Pong(p)).await.unwrap(),
                message => return message,
            }
        }
    })
    .await
    .unwrap()
}
async fn connect(listener: &TcpListener, roster: &[&str]) -> Socket {
    let (stream, _) = tokio::time::timeout(DEADLINE, listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
    let Message::Text(text) = frame(&mut ws).await else {
        panic!("initial public subscription expected")
    };
    let subscription: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        subscription,
        json!({"type":"market", "assets_ids":roster, "initial_dump":true, "custom_feature_enabled":true})
    );
    ws
}
async fn event(ws: &mut Socket, event: Value) {
    ws.send(Message::Text(event.to_string().into()))
        .await
        .unwrap();
}
fn book(asset: &str, quantity: &str) -> Value {
    json!({"event_type":"book", "asset_id":asset, "timestamp":"1700000000000", "hash":"opaque", "bids":[{"price":"0.4","size":quantity}], "asks":[]})
}
fn update(asset: &str, quantity: &str) -> Value {
    json!({"event_type":"price_change", "timestamp":"1700000000001", "price_changes":[{"asset_id":asset, "side":"BUY", "price":"0.4", "size":quantity}]})
}
async fn barrier(ws: &mut Socket, id: u8) {
    ws.send(Message::Ping(vec![id].into())).await.unwrap();
    let Message::Pong(p) = frame(ws).await else {
        panic!("data admission barrier expected")
    };
    assert_eq!(p.as_ref(), [id]);
}
async fn retired(mut ws: Socket) {
    assert!(matches!(frame(&mut ws).await, Message::Close(_)));
    ws.flush().await.unwrap();
}

#[tokio::test]
async fn incomplete_inventory_retains_absent_assets_and_last_complete_catalog() {
    tokio::time::timeout(Duration::from_secs(25), run_case(false))
        .await
        .unwrap();
}
#[tokio::test]
async fn a_later_authoritative_catalog_retires_absent_assets_and_restores_completeness() {
    tokio::time::timeout(Duration::from_secs(25), run_case(true))
        .await
        .unwrap();
}

async fn run_case(later_clean: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let options = options(&temp, listener.local_addr().unwrap());
    let (catalog_tx, replies) = mpsc::channel(1);
    let running = Running::start(
        options.clone(),
        Arc::new(ScriptedCatalog(Mutex::new(replies))),
    );
    scan(&catalog_tx, Scan::Initial).await;
    let mut initial = connect(&listener, &["123"]).await;
    event(&mut initial, book("123", "10")).await;
    barrier(&mut initial, 1).await;

    // 123 is absent from an uncertain scan; 456 is still a valid addition.
    scan(&catalog_tx, Scan::Uncertain).await;
    retired(initial).await;
    let mut current = connect(&listener, &["123", "456"]).await;
    event(&mut current, update("456", "777")).await;
    event(&mut current, json!([book("123", "12"), book("456", "20")])).await;
    event(
        &mut current,
        json!([update("123", "13"), update("456", "21")]),
    )
    .await;
    barrier(&mut current, 2).await;
    if later_clean {
        scan(&catalog_tx, Scan::Clean).await;
        // Repeated identical scans make the metadata lane advance beyond the
        // first clean response, without adding any new registry epochs.
        scan(&catalog_tx, Scan::Clean).await;
        scan(&catalog_tx, Scan::Clean).await;
        // The old generation can still have in-flight data after retirement.
        // Removed 123 must never reappear as a publisher or be reinitialized.
        event(&mut current, book("123", "999")).await;
        retired(current).await;
        current = connect(&listener, &["456"]).await;
        event(&mut current, update("456", "888")).await;
        event(&mut current, book("456", "21")).await;
        event(&mut current, update("456", "22")).await;
        barrier(&mut current, 3).await;
    }
    running.finish(current).await;

    let summary: Value =
        serde_json::from_slice(&std::fs::read(options.summary.as_ref().unwrap()).unwrap()).unwrap();
    assert_eq!(summary["discovery_complete"], later_clean);
    assert_eq!(summary["complete_at_cutoff"], later_clean);
    let known = if later_clean { 1 } else { 2 };
    assert_eq!(summary["desired"], known);
    let shard = &summary["shards"][0];
    assert_eq!(shard["desired"], known);
    assert_eq!(shard["initialized"], known);
    assert_eq!(shard["replica_initialized"], json!([known]));
    assert_eq!(shard["subscribed"], known);
    assert_eq!(shard["maintenance_pending"], 0);
    assert_eq!(shard["unacknowledged_closes"], 0);
    assert_eq!(shard["connects"], if later_clean { 3 } else { 2 });
    if later_clean {
        assert_eq!(summary["catalog_metadata"], Scan::Clean.metadata());
        assert!(
            summary
                .get("last_complete_catalog")
                .is_none_or(Value::is_null)
        );
    } else {
        assert_eq!(summary["catalog_metadata"], Scan::Uncertain.metadata());
        assert_eq!(summary["last_complete_catalog"], Scan::Initial.metadata());
    }

    let report = replay::verify(&options.output, options.audit.as_deref(), 10).unwrap();
    assert_eq!(report.symbols, 2);
    assert!(report.snapshot_only_baselines > 0);
    assert_eq!(report.comparable_clock_baselines_verified, 0);
    let rows: Vec<AuditRow> = std::fs::read_to_string(options.audit.as_ref().unwrap())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let a_lifetimes: Vec<_> = rows
        .iter()
        .filter(|row| row.symbol == "123" && row.reason == SnapshotReason::Initial)
        .collect();
    assert_eq!(
        a_lifetimes.len(),
        1,
        "uncertain absence must not retire and recreate 123"
    );
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&options.output).unwrap();
    let mut output_updates = vec![];
    while let Some(message) = reader.read_msg().unwrap() {
        match message {
            AnyMessage::Snapshot(decoded) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoded).unwrap();
                assert!(
                    snapshot
                        .bids
                        .iter()
                        .all(|level| level.quantity != 999_000_000)
                );
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap();
                output_updates.push((update.symbol, update.quantity));
            }
            _ => panic!("unexpected SBE record in catalog fixture"),
        }
    }
    let a_updates: Vec<_> = output_updates
        .iter()
        .filter(|(symbol, _)| symbol == "123")
        .map(|(_, quantity)| *quantity)
        .collect();
    if later_clean {
        // The supervisor's retirement cut may revoke 123's already-relayed
        // update before Owner handles it. A source Ping barrier proves relay
        // admission, not publication ahead of an independent catalog lane.
        assert!(a_updates.iter().all(|quantity| *quantity == 13_000_000));
    } else {
        assert_eq!(a_updates, [13_000_000]);
    }
    assert_eq!(
        output_updates
            .iter()
            .filter(|(symbol, _)| symbol == "456")
            .map(|(_, quantity)| *quantity)
            .collect::<Vec<_>>(),
        if later_clean {
            vec![21_000_000, 22_000_000]
        } else {
            vec![21_000_000]
        }
    );
}
