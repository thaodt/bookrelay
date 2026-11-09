//! Public CLOB wire frames through the shared source/owner/SBE/replay runtime.
//! These localhost fixtures prove runtime behavior, not venue acceptance.
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
use futures_util::{SinkExt, StreamExt};
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

fn options(temp: &tempfile::TempDir, address: SocketAddr) -> Options {
    Options {
        symbols: vec!["123".into(), "456".into()],
        rest_url: "unused".into(),
        ws_url: format!("ws://{address}/ws/market"),
        symbols_per_connection: 10,
        max_connections: 2,
        max_symbols: 10,
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
            command_interval: Duration::from_millis(1),
            // FreshSession must still send one entire initial roster.
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
    fn start(
        options: Options,
        catalog: Option<Arc<dyn engineering_challenge::catalog::Catalog>>,
    ) -> Self {
        let (cancel, stop) = watch::channel(false);
        let task = tokio::spawn(collector::run_with_exchange(
            options,
            Configuration {
                protocol: Arc::new(Polymarket),
                auth: None,
                catalog,
            },
            stop,
            cancel.clone(),
        ));
        Self {
            cancel,
            task: Some(task),
        }
    }
    async fn finish(mut self, sockets: Vec<Socket>) {
        let mut peers = tokio::task::JoinSet::new();
        for mut ws in sockets {
            peers.spawn(async move {
                while let Some(message) = ws.next().await {
                    match message.unwrap() {
                        Message::Text(text) if text == "PING" => {
                            ws.send(Message::Text("PONG".into())).await.unwrap();
                        }
                        Message::Ping(p) => ws.send(Message::Pong(p)).await.unwrap(),
                        Message::Close(_) => {
                            let _ = ws.flush().await;
                            return;
                        }
                        other => panic!("unexpected public control during shutdown: {other:?}"),
                    }
                }
                panic!("collector dropped a public socket without Close");
            });
        }
        self.cancel.send(true).unwrap();
        // Incomplete desired coverage deliberately spends the bounded 15s
        // quiescence window; complete fixtures normally finish immediately.
        tokio::time::timeout(Duration::from_secs(20), self.task.as_mut().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.task.take();
        while let Some(peer) = tokio::time::timeout(DEADLINE, peers.join_next())
            .await
            .unwrap()
        {
            peer.unwrap();
        }
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

// The handshake callback's error response type is fixed by tungstenite's API.
#[allow(clippy::result_large_err)]
async fn connect(listener: &TcpListener, expected: &[&str]) -> Socket {
    let (stream, _) = tokio::time::timeout(DEADLINE, listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut ws = tokio_tungstenite::accept_hdr_async(
        stream,
        |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
            for name in request.headers().keys() {
                let name = name.as_str().to_ascii_lowercase();
                assert!(
                    !name.contains("authorization")
                        && !name.starts_with("kalshi")
                        && !name.starts_with("poly-"),
                    "public socket received an authentication header: {name}"
                );
            }
            Ok(response)
        },
    )
    .await
    .unwrap();
    let initial = tokio::time::timeout(DEADLINE, async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) if text == "PING" => {
                    ws.send(Message::Text("PONG".into())).await.unwrap()
                }
                Message::Text(text) => break serde_json::from_str::<Value>(&text).unwrap(),
                Message::Ping(p) => ws.send(Message::Pong(p)).await.unwrap(),
                other => panic!("unexpected initial public frame: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        initial,
        json!({"type":"market", "assets_ids":expected, "initial_dump":true, "custom_feature_enabled":true})
    );
    ws
}
async fn event(ws: &mut Socket, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
fn book(asset: &str, quantity: &str) -> Value {
    json!({"event_type":"book", "asset_id":asset, "market":"0xfixture", "timestamp":"1700000000000", "hash":"opaque-state-hash", "bids":[{"price":"0.4","size":quantity}], "asks":[{"price":"0.6","size":"8"}]})
}
fn changes(asset: &str, quantities: &[&str]) -> Value {
    json!({"event_type":"price_change", "market":"0xfixture", "timestamp":"1700000000001", "price_changes":quantities.iter().map(|quantity| json!({"asset_id":asset, "side":"BUY", "price":"0.4", "size":quantity, "hash":"opaque-change-hash"})).collect::<Vec<_>>()})
}
// Source admits every preceding data frame to the shared FIFO relay before
// replying to this control-frame ping. No sleeps establish data ordering.
async fn barrier(ws: &mut Socket, id: u8) {
    ws.send(Message::Ping(vec![id].into())).await.unwrap();
    tokio::time::timeout(DEADLINE, async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Pong(p) if p.as_ref() == [id] => break,
                Message::Text(text) if text == "PING" => {
                    ws.send(Message::Text("PONG".into())).await.unwrap()
                }
                Message::Ping(p) => ws.send(Message::Pong(p)).await.unwrap(),
                other => panic!("public feed sent an in-place control: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
}
fn audit(options: &Options) -> Vec<AuditRow> {
    std::fs::read_to_string(options.audit.as_ref().unwrap())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
fn summary(options: &Options) -> Value {
    serde_json::from_slice(&std::fs::read(options.summary.as_ref().unwrap()).unwrap()).unwrap()
}
fn updates(options: &Options) -> Vec<PriceLevelUpdateDomain> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&options.output).unwrap();
    let mut updates = vec![];
    while let Some(message) = reader.read_msg().unwrap() {
        if let AnyMessage::PriceLevelUpdate(decoded) = message {
            updates.push(PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap());
        }
    }
    updates
}
fn snapshots(options: &Options) -> Vec<SnapshotDomain> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&options.output).unwrap();
    let mut snapshots = vec![];
    while let Some(message) = reader.read_msg().unwrap() {
        if let AnyMessage::Snapshot(decoded) = message {
            snapshots.push(SnapshotDomain::try_from_decoder(decoded).unwrap());
        }
    }
    snapshots
}
fn public_evidence(options: &Options) -> replay::ReplayReport {
    let report = replay::verify(&options.output, options.audit.as_deref(), 10).unwrap();
    assert!(report.snapshot_only_baselines > 0);
    assert_eq!(report.comparable_clock_baselines_verified, 0);
    assert_eq!(report.legacy_unspecified_baselines, 0);
    for row in audit(options) {
        let source = row.source.unwrap();
        assert_eq!(source.evidence, OrderingEvidence::SnapshotOnly);
        assert!(source.clock.is_none() && source.ordinal.is_none() && !source.complete_clock);
    }
    report
}

#[tokio::test]
async fn two_public_feeds_share_absolute_sbe_output_without_native_acks_or_clocks() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let options = options(&temp, listener.local_addr().unwrap());
    let running = Running::start(options.clone(), None);
    let mut first = connect(&listener, &["123", "456"]).await;
    event(&mut first, changes("123", &["999"])).await;
    event(
        &mut first,
        json!([
            book("123", "10"),
            book("456", "0"),
            changes("123", &["3", "3", "0", "4"]),
            book("123", "4"),
            book("123", "6"),
            book("123", "6")
        ]),
    )
    .await;
    barrier(&mut first, 1).await;
    let mut second = connect(&listener, &["123", "456"]).await;
    event(&mut second, json!([book("123", "6"), book("456", "0")])).await;
    barrier(&mut second, 2).await;
    running.finish(vec![first, second]).await;

    let report = public_evidence(&options);
    assert_eq!(report.symbols, 2);
    assert_eq!(report.updates, 4);
    assert_eq!(
        updates(&options)
            .iter()
            .map(|u| u.quantity)
            .collect::<Vec<_>>(),
        [3_000_000, 3_000_000, 0, 4_000_000]
    );
    let rows = audit(&options);
    let primary: Vec<_> = rows
        .iter()
        .filter(|r| r.symbol == "123" && r.reason != SnapshotReason::Periodic)
        .collect();
    assert_eq!(
        primary.len(),
        2,
        "equal books must confirm, changed books must restate"
    );
    assert_eq!(primary[0].reason, SnapshotReason::Initial);
    assert_eq!(primary[1].reason, SnapshotReason::Recovery);
    assert_eq!(primary[0].source.as_ref().unwrap().output_epoch, 1);
    assert_eq!(primary[1].source.as_ref().unwrap().output_epoch, 2);
    assert!(!primary[1].source.as_ref().unwrap().unverified_cut);
    assert_eq!(report.unverified_continuity_transitions, 0);
    let states: Vec<_> = snapshots(&options)
        .into_iter()
        .filter(|s| s.symbol == "123")
        .collect();
    assert_eq!(states.last().unwrap().bids[0].quantity, 6_000_000);
    let summary = summary(&options);
    assert_eq!(summary["complete_at_cutoff"], true);
    assert_eq!(summary["shards"][0]["replica_initialized"], json!([2, 2]));
    assert_eq!(summary["shards"][0]["ack_timeouts"], 0);
    assert_eq!(summary["shards"][0]["connects"], 2);
    assert!(summary["shards"][0]["source_quarantined"].as_u64().unwrap() >= 1);
    assert!(
        summary["shards"][0]["snapshot_confirmations"]
            .as_u64()
            .unwrap()
            >= 2
    );
    assert_eq!(summary["shards"][0]["state_reconciliations"], 1);
}

#[tokio::test]
async fn public_peer_close_is_echoed_before_failover_with_unverified_continuity() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let options = options(&temp, listener.local_addr().unwrap());
    let running = Running::start(options.clone(), None);
    let mut first = connect(&listener, &["123", "456"]).await;
    event(&mut first, json!([book("123", "10"), book("456", "20")])).await;
    barrier(&mut first, 10).await;
    let mut second = connect(&listener, &["123", "456"]).await;
    event(&mut second, json!([book("123", "10"), book("456", "20")])).await;
    barrier(&mut second, 11).await;
    first.close(None).await.unwrap();
    tokio::time::timeout(DEADLINE, async {
        loop {
            match first.next().await.unwrap().unwrap() {
                Message::Close(_) => break,
                Message::Text(text) if text == "PING" => {
                    first.send(Message::Text("PONG".into())).await.unwrap();
                }
                Message::Ping(p) => first.send(Message::Pong(p)).await.unwrap(),
                other => panic!("unexpected frame before peer Close echo: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    // As the server, finish the closing handshake by dropping TCP only after
    // the client's echoed Close arrives. A reset instead of an echo must fail.
    drop(first);
    let mut replacement = connect(&listener, &["123", "456"]).await;
    event(
        &mut replacement,
        json!([book("123", "10"), book("456", "20")]),
    )
    .await;
    barrier(&mut replacement, 12).await;
    event(&mut second, changes("123", &["7"])).await;
    barrier(&mut second, 13).await;
    running.finish(vec![second, replacement]).await;
    let report = public_evidence(&options);
    assert!(report.unverified_continuity_transitions >= 2);
    assert!(
        audit(&options)
            .iter()
            .any(|row| row.reason != SnapshotReason::Initial
                && row.source.as_ref().unwrap().unverified_cut)
    );
    assert_eq!(summary(&options)["complete_at_cutoff"], true);
    assert_eq!(summary(&options)["shards"][0]["connects"], 3);
    assert_eq!(summary(&options)["shards"][0]["unacknowledged_closes"], 0);
}

async fn next_close(first: &mut Socket, second: &mut Socket) -> usize {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let (slot, message) = tokio::select! {
                message = first.next() => (0, message),
                message = second.next() => (1, message),
            };
            let ws = if slot == 0 { &mut *first } else { &mut *second };
            match message.unwrap().unwrap() {
                Message::Close(_) => {
                    let _ = ws.flush().await;
                    return slot;
                }
                Message::Text(text) if text == "PING" => {
                    ws.send(Message::Text("PONG".into())).await.unwrap()
                }
                Message::Ping(p) => ws.send(Message::Pong(p)).await.unwrap(),
                other => panic!("renewal must close the transport, received {other:?}"),
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn public_planned_rotation_renews_one_transport_and_records_the_weaker_handoff() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut options = options(&temp, listener.local_addr().unwrap());
    options.health.rotation_interval = Duration::from_secs(1);
    let running = Running::start(options.clone(), None);
    let mut first = connect(&listener, &["123", "456"]).await;
    event(&mut first, json!([book("123", "10"), book("456", "20")])).await;
    barrier(&mut first, 20).await;
    let mut second = connect(&listener, &["123", "456"]).await;
    event(&mut second, json!([book("123", "10"), book("456", "20")])).await;
    barrier(&mut second, 21).await;
    let retired = next_close(&mut first, &mut second).await;
    let mut replacement = connect(&listener, &["123", "456"]).await;
    event(
        &mut replacement,
        json!([book("123", "10"), book("456", "20")]),
    )
    .await;
    barrier(&mut replacement, 22).await;
    let survivor = if retired == 0 { second } else { first };
    running.finish(vec![survivor, replacement]).await;
    let report = public_evidence(&options);
    assert!(report.unverified_continuity_transitions >= 2);
    let summary = summary(&options);
    assert_eq!(summary["complete_at_cutoff"], true);
    assert_eq!(summary["shards"][0]["planned_rotations"], 1);
    assert_eq!(summary["shards"][0]["completed_rotations"], 1);
    assert_eq!(summary["shards"][0]["maintenance_pending"], 0);
    assert_eq!(summary["shards"][0]["ack_timeouts"], 0);
}

#[tokio::test]
async fn one_missing_public_dump_preserves_good_books_and_reports_incomplete_coverage() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut options = options(&temp, listener.local_addr().unwrap());
    options.replicas = 1;
    options.max_connections = 1;
    options.health.ack_timeout = Duration::from_millis(100);
    options.health.max_resubscriptions = 1;
    let running = Running::start(options.clone(), None);
    let mut first = connect(&listener, &["123", "456"]).await;
    event(&mut first, book("123", "10")).await;
    barrier(&mut first, 30).await;
    // Missing 456 earns one bounded fresh-session retry, not deletion from
    // desired membership and not a native membership ACK timeout.
    tokio::time::timeout(DEADLINE, async {
        loop {
            match first.next().await.unwrap().unwrap() {
                Message::Close(_) => {
                    let _ = first.flush().await;
                    break;
                }
                Message::Text(text) if text == "PING" => {
                    first.send(Message::Text("PONG".into())).await.unwrap()
                }
                Message::Ping(p) => first.send(Message::Pong(p)).await.unwrap(),
                other => panic!("unexpected missing-dump control: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    let mut replacement = connect(&listener, &["123", "456"]).await;
    event(&mut replacement, book("123", "20")).await;
    barrier(&mut replacement, 31).await;
    // This is a deadline test: allow the 100ms dump deadline and source's
    // 100ms health timer to expire again, after its one retry is exhausted.
    tokio::time::sleep(Duration::from_millis(350)).await;
    event(&mut replacement, changes("123", &["21"])).await;
    barrier(&mut replacement, 32).await;
    running.finish(vec![replacement]).await;

    let report = public_evidence(&options);
    assert_eq!(report.symbols, 1);
    assert_eq!(updates(&options).last().unwrap().quantity, 21_000_000);
    assert!(snapshots(&options).iter().all(|s| s.symbol == "123"));
    let summary = summary(&options);
    assert_eq!(summary["complete_at_cutoff"], false);
    assert_eq!(summary["desired"], 2);
    assert_eq!(summary["shards"][0]["desired"], 2);
    assert_eq!(summary["shards"][0]["initialized"], 1);
    assert_eq!(summary["shards"][0]["unavailable_replica_assets"], 1);
    assert!(
        summary["shards"][0]["unavailable_sample"]
            .as_array()
            .unwrap()
            .iter()
            .any(|symbol| symbol == "456")
    );
    assert_eq!(summary["shards"][0]["connects"], 2);
    assert_eq!(summary["shards"][0]["ack_timeouts"], 0);
}

struct CatalogReply {
    symbols: Vec<String>,
    observed: oneshot::Sender<()>,
}
struct ScriptedCatalog {
    replies: Mutex<mpsc::Receiver<CatalogReply>>,
}
impl Catalog for ScriptedCatalog {
    fn scan(
        &self,
        _since: Option<i64>,
    ) -> futures_util::future::BoxFuture<'_, anyhow::Result<Inventory>> {
        Box::pin(async move {
            let reply = self
                .replies
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("fixture catalog closed"))?;
            let inventory = Inventory {
                instruments: reply
                    .symbols
                    .into_iter()
                    .map(|symbol| Instrument {
                        symbol,
                        eligible: true,
                    })
                    .collect(),
                watermark: 1700000000,
                pages: 1,
                elapsed: Duration::ZERO,
                authoritative_for_retirement: true,
                complete: true,
                metadata: None,
            };
            let _ = reply.observed.send(());
            Ok(inventory)
        })
    }
}
async fn catalog_reply(tx: &mpsc::Sender<CatalogReply>, symbols: &[&str]) {
    let (observed, wait) = oneshot::channel();
    tx.send(CatalogReply {
        symbols: symbols.iter().map(|symbol| (*symbol).into()).collect(),
        observed,
    })
    .await
    .unwrap();
    tokio::time::timeout(DEADLINE, wait).await.unwrap().unwrap();
}

#[tokio::test]
async fn roster_changes_coalesce_and_old_generations_cannot_initialize_readded_assets() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut options = options(&temp, listener.local_addr().unwrap());
    options.symbols.clear();
    options.registry.poll_interval = Duration::from_millis(1);
    let (catalog_tx, replies) = mpsc::channel(1);
    let running = Running::start(
        options.clone(),
        Some(Arc::new(ScriptedCatalog {
            replies: Mutex::new(replies),
        })),
    );
    catalog_reply(&catalog_tx, &["123", "456"]).await;
    let mut first = connect(&listener, &["123", "456"]).await;
    event(&mut first, json!([book("123", "10"), book("456", "20")])).await;
    barrier(&mut first, 40).await;
    let mut second = connect(&listener, &["123", "456"]).await;
    event(&mut second, json!([book("123", "10"), book("456", "20")])).await;
    barrier(&mut second, 41).await;

    catalog_reply(&catalog_tx, &["456"]).await;
    let retired = next_close(&mut first, &mut second).await;
    let mut survivor = if retired == 0 { second } else { first };
    let mut removed_roster = connect(&listener, &["456"]).await;
    // Hold its baseline so the one-operation coordinator cannot renew the
    // survivor while the catalog replaces the pending roster twice.
    catalog_reply(&catalog_tx, &["123", "456", "789"]).await;
    catalog_reply(&catalog_tx, &["123", "456", "888"]).await;
    // The next scan starts only after the preceding scan entered the bounded
    // supervisor channel. Identical scans must not create extra renewals.
    catalog_reply(&catalog_tx, &["123", "456", "888"]).await;
    catalog_reply(&catalog_tx, &["123", "456", "888"]).await;
    event(
        &mut survivor,
        json!([book("123", "999"), book("789", "999"), book("888", "999")]),
    )
    .await;
    barrier(&mut survivor, 42).await;
    event(&mut removed_roster, book("456", "20")).await;
    barrier(&mut removed_roster, 43).await;

    let retired = next_close(&mut survivor, &mut removed_roster).await;
    assert_eq!(
        retired, 0,
        "replacement must initialize before renewing the survivor"
    );
    let mut latest = connect(&listener, &["123", "456", "888"]).await;
    event(&mut latest, changes("123", &["777"])).await;
    event(
        &mut latest,
        json!([book("123", "30"), book("456", "20"), book("888", "40")]),
    )
    .await;
    barrier(&mut latest, 44).await;
    // The first replacement still submitted [456], so it also needs a fresh
    // generation. Its per-feed cooldown bounds rapid catalog changes.
    let retired = next_close(&mut removed_roster, &mut latest).await;
    assert_eq!(retired, 0);
    let mut final_feed = connect(&listener, &["123", "456", "888"]).await;
    event(
        &mut final_feed,
        json!([book("123", "30"), book("456", "20"), book("888", "40")]),
    )
    .await;
    barrier(&mut final_feed, 45).await;
    running.finish(vec![latest, final_feed]).await;
    drop(catalog_tx);

    let report = public_evidence(&options);
    assert_eq!(report.symbols, 3);
    let rows = audit(&options);
    let lifetimes: Vec<_> = rows
        .iter()
        .filter(|row| row.symbol == "123" && row.reason == SnapshotReason::Initial)
        .map(|row| row.source.as_ref().unwrap())
        .collect();
    assert_eq!(lifetimes.len(), 2);
    assert!(lifetimes[1].incarnation > lifetimes[0].incarnation);
    assert_eq!(lifetimes[1].output_epoch, 1);
    let states = snapshots(&options);
    assert!(!states.iter().any(|s| s.symbol == "789"));
    assert!(
        states
            .iter()
            .flat_map(|s| &s.bids)
            .all(|level| level.quantity != 999_000_000 && level.quantity != 777_000_000)
    );
    assert_eq!(
        states.iter().find(|s| s.symbol == "888").unwrap().bids[0].quantity,
        40_000_000
    );
    let summary = summary(&options);
    assert_eq!(summary["complete_at_cutoff"], true);
    assert_eq!(summary["desired"], 3);
    assert_eq!(summary["shards"][0]["replica_initialized"], json!([3, 3]));
    assert_eq!(summary["shards"][0]["connects"], 5);
    assert_eq!(summary["shards"][0]["maintenance_pending"], 0);
}
