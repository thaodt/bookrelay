//! A public renewal waiting for an obsolete roster must not block a wholly
//! disjoint catalog. Real WS generations carry the evidence; no ACK is invented.
use engineering_challenge::{
    catalog::{Catalog, Instrument, Inventory},
    collector::{self, Options, RegistryOptions},
    exchange::{Configuration, polymarket::Polymarket},
    health,
    output::{AuditRow, OrderingEvidence, SnapshotReason},
    replay,
    transport::ConnectOptions,
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
    },
};
use futures_util::{SinkExt, StreamExt, future::BoxFuture};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{Mutex, mpsc, oneshot, watch},
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

type Socket = WebSocketStream<tokio::net::TcpStream>;
const STEP_DEADLINE: Duration = Duration::from_secs(8);

struct Reply {
    symbol: &'static str,
    observed: oneshot::Sender<()>,
}
struct ScriptedCatalog(Mutex<mpsc::Receiver<Reply>>);
impl Catalog for ScriptedCatalog {
    fn scan(&self, since: Option<i64>) -> BoxFuture<'_, anyhow::Result<Inventory>> {
        Box::pin(async move {
            assert!(since.is_none());
            let reply = self
                .0
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("fixture catalog closed"))?;
            let inventory = Inventory {
                instruments: vec![Instrument {
                    symbol: reply.symbol.into(),
                    eligible: true,
                }],
                watermark: if reply.symbol == "123" {
                    1700000000
                } else {
                    1700000001
                },
                pages: 1,
                elapsed: Duration::ZERO,
                authoritative_for_retirement: true,
                complete: true,
                metadata: Some(json!({"scope":"controlled_full_scan", "asset":reply.symbol})),
            };
            let _ = reply.observed.send(());
            Ok(inventory)
        })
    }
}
async fn inventory(tx: &mpsc::Sender<Reply>, symbol: &'static str) {
    let (observed, wait) = oneshot::channel();
    tx.send(Reply { symbol, observed }).await.unwrap();
    tokio::time::timeout(STEP_DEADLINE, wait)
        .await
        .unwrap()
        .unwrap();
}

struct StopOnDrop(watch::Sender<bool>);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

async fn frame(ws: &mut Socket) -> Message {
    tokio::time::timeout(STEP_DEADLINE, async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) if text == "PING" => {
                    ws.send(Message::Text("PONG".into())).await.unwrap()
                }
                Message::Ping(payload) => ws.send(Message::Pong(payload)).await.unwrap(),
                other => return other,
            }
        }
    })
    .await
    .expect("public transport did not make bounded progress")
}
async fn connect(listener: &TcpListener, symbol: &str) -> Socket {
    let (stream, _) = tokio::time::timeout(STEP_DEADLINE, listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
    let Message::Text(text) = frame(&mut ws).await else {
        panic!("initial subscription expected");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap(),
        json!({"type":"market", "assets_ids":[symbol], "initial_dump":true, "custom_feature_enabled":true})
    );
    ws
}
async fn event(ws: &mut Socket, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
fn book(asset: &str, quantity: &str) -> Value {
    json!({"event_type":"book", "asset_id":asset, "timestamp":"1700000000000",
        "bids":[{"price":"0.4", "size":quantity}], "asks":[]})
}
fn update(asset: &str, quantity: &str) -> Value {
    json!({"event_type":"price_change", "timestamp":"1700000000001",
        "price_changes":[{"asset_id":asset, "side":"BUY", "price":"0.4", "size":quantity}]})
}
// This confirms Source admitted earlier frames, not that Owner published them.
async fn barrier(ws: &mut Socket, id: u8) {
    ws.send(Message::Ping(vec![id].into())).await.unwrap();
    let Message::Pong(payload) = frame(ws).await else {
        panic!("source barrier expected");
    };
    assert_eq!(payload.as_ref(), [id]);
}
async fn next_close(first: &mut Socket, second: &mut Socket) -> usize {
    tokio::time::timeout(STEP_DEADLINE, async {
        loop {
            let (slot, message) = tokio::select! {
                message=first.next() => (0,message),
                message=second.next() => (1,message),
            };
            let ws = if slot == 0 { &mut *first } else { &mut *second };
            match message.unwrap().unwrap() {
                Message::Close(_) => {
                    ws.flush().await.unwrap();
                    return slot;
                }
                Message::Text(text) if text == "PING" => {
                    ws.send(Message::Text("PONG".into())).await.unwrap()
                }
                Message::Ping(payload) => ws.send(Message::Pong(payload)).await.unwrap(),
                other => panic!("fresh-session renewal sent an in-place command: {other:?}"),
            }
        }
    })
    .await
    .expect("obsolete replacement blocked the disjoint roster")
}
async fn close_echo(ws: &mut Socket) {
    assert!(matches!(frame(ws).await, Message::Close(_)));
    ws.flush().await.unwrap();
}

#[tokio::test]
async fn a_disjoint_catalog_supersedes_a_written_renewal_without_a_usable_old_baseline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let options = Options {
        symbols: vec![],
        rest_url: "unused".into(),
        ws_url: format!("ws://{}/ws/market", listener.local_addr().unwrap()),
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
            poll_interval: Duration::from_millis(1),
            command_interval: Duration::from_millis(1),
            ..RegistryOptions::default()
        },
        health: health::Options {
            window: Duration::from_secs(4),
            warmup: Duration::from_secs(4),
            cooldown: Duration::from_secs(4),
            rotation_interval: Duration::from_secs(1),
            rotation_timeout: Duration::from_secs(10),
            ..health::Options::default()
        },
    };
    let (catalog_tx, replies) = mpsc::channel(1);
    let (cancel, stop) = watch::channel(false);
    let _cleanup = StopOnDrop(cancel.clone());
    let collect = collector::run_with_exchange(
        options.clone(),
        Configuration {
            protocol: Arc::new(Polymarket),
            auth: None,
            catalog: Some(Arc::new(ScriptedCatalog(Mutex::new(replies)))),
        },
        stop,
        cancel.clone(),
    );
    let exchange = async {
        inventory(&catalog_tx, "123").await;
        let mut first = connect(&listener, "123").await;
        event(&mut first, book("123", "10")).await;
        barrier(&mut first, 1).await;
        let mut second = connect(&listener, "123").await;
        event(&mut second, book("123", "10")).await;
        barrier(&mut second, 2).await;

        // An automatic renewal really closes one ready original. Its new
        // stream completes Subscribe[A], but never receives A's book dump.
        let retired = next_close(&mut first, &mut second).await;
        let mut original = if retired == 0 { second } else { first };
        let mut obsolete = connect(&listener, "123").await;
        barrier(&mut obsolete, 3).await;
        inventory(&catalog_tx, "456").await;
        inventory(&catalog_tx, "456").await;

        // Either old stream may renew first, according to its existing
        // cooldown. No retained A book is offered as a bridge to B.
        let retired = next_close(&mut original, &mut obsolete).await;
        let mut old_roster = if retired == 0 { obsolete } else { original };
        let mut first_b = connect(&listener, "456").await;
        // Subscribe[B] proves the authoritative replacement reached assignment.
        // These old-generation frames cannot initialize B or resurrect A.
        event(
            &mut old_roster,
            json!([book("123", "999"), book("456", "888")]),
        )
        .await;
        barrier(&mut old_roster, 4).await;
        event(&mut first_b, update("456", "777")).await;
        event(&mut first_b, book("456", "20")).await;
        event(&mut first_b, update("456", "21")).await;
        barrier(&mut first_b, 5).await;

        assert_eq!(
            next_close(&mut old_roster, &mut first_b).await,
            0,
            "a ready B survivor must remain while the obsolete roster renews"
        );
        let mut second_b = connect(&listener, "456").await;
        event(&mut second_b, book("123", "999")).await;
        event(&mut second_b, update("456", "777")).await;
        event(&mut second_b, book("456", "21")).await;
        barrier(&mut second_b, 6).await;
        event(&mut first_b, update("456", "22")).await;
        barrier(&mut first_b, 7).await;
        event(&mut second_b, update("456", "22")).await;
        barrier(&mut second_b, 8).await;
        cancel.send(true).unwrap();
        tokio::join!(close_echo(&mut first_b), close_echo(&mut second_b));
    };
    let started = tokio::time::Instant::now();
    let (result, ()) = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(collect, exchange)
    })
    .await
    .expect("disjoint public supersession did not close, drain and join in 15s");
    result.unwrap();
    let summary: Value =
        serde_json::from_slice(&std::fs::read(options.summary.as_ref().unwrap()).unwrap()).unwrap();
    assert_eq!(summary["complete_at_cutoff"], true);
    assert_eq!(summary["discovery_complete"], true);
    assert_eq!(summary["desired"], 1);
    let shard = &summary["shards"][0];
    assert_eq!(shard["replica_initialized"], json!([1, 1]));
    assert_eq!(shard["connects"], 5);
    assert_eq!(shard["planned_rotations"], 3);
    assert_eq!(
        shard["completed_rotations"], 2,
        "obsolete A replacement must be aborted, not credited as complete"
    );
    for key in [
        "maintenance_pending",
        "unacknowledged_closes",
        "ack_timeouts",
        "rotation_timeouts",
        "handoff_pauses",
        "pause_expirations",
        "pause_us_max",
    ] {
        assert_eq!(shard[key], 0, "{key}: {shard}");
    }
    let report = replay::verify(&options.output, options.audit.as_deref(), 2).unwrap();
    assert_eq!(report.symbols, 2);
    assert!(report.snapshot_only_baselines > 0);
    assert!(report.periodic_checkpoints_verified > 0);
    assert_eq!(report.comparable_clock_baselines_verified, 0);
    assert_eq!(report.legacy_unspecified_baselines, 0);
    let audit: Vec<AuditRow> = std::fs::read_to_string(options.audit.as_ref().unwrap())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        audit
            .iter()
            .filter(|row| row.symbol == "123" && row.reason == SnapshotReason::Initial)
            .count(),
        1
    );
    assert_eq!(
        audit
            .iter()
            .filter(|row| row.symbol == "456" && row.reason == SnapshotReason::Initial)
            .count(),
        1
    );
    assert!(
        audit
            .iter()
            .all(|row| row.source.as_ref().unwrap().evidence == OrderingEvidence::SnapshotOnly)
    );

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&options.output).unwrap();
    let mut b_started = false;
    let mut final_b_quantity = None;
    while let Some(message) = reader.read_msg().unwrap() {
        let (symbol, quantity) = match message {
            AnyMessage::Snapshot(decoded) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoded).unwrap();
                (
                    snapshot.symbol,
                    snapshot.bids.first().map(|level| level.quantity),
                )
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap();
                (update.symbol, Some(update.quantity))
            }
            _ => panic!("unexpected SBE record"),
        };
        assert!(!matches!(
            quantity,
            Some(999_000_000 | 888_000_000 | 777_000_000)
        ));
        if symbol == "456" {
            b_started = true;
            final_b_quantity = quantity;
        } else {
            assert_eq!(symbol, "123");
            assert!(!b_started, "retired A appeared after B initialized");
        }
    }
    assert!(b_started);
    assert_eq!(final_b_quantity, Some(22_000_000));
    eprintln!(
        "public disjoint supersession completed in {:?}; shard={shard}; replay={report:?}",
        started.elapsed()
    );
}
