//! Owned fault at the 2,250-market default and balanced 3,000-market stress scale
//! (default 250-market
//! command batches) on a localhost exchange.
//!
//! Clock model: the fixture provider uses ONE exchange-wide comparable clock.
//! That is an inferred model of the live native feed (one 15-minute market
//! carried clock 18,710,915,846), not a documented provider contract. Under
//! it, a quiet market's later snapshot on one connection has a higher clock
//! and unchanged levels; the peer-ahead grace publishes from that connection
//! and the other replica holds an older complete clock its tail cannot move.
//!
//! When the publisher of such books faults, the survivor needs a covering
//! snapshot. The independently reproduced R1 failure (Codex, 2026-11-08):
//! the request invalidated the only survivor, so each book became a gap and
//! failed over with a Recovery baseline. Every snapshot here samples the one
//! provider state when sent; each connection is one ordered, delayed tail.
use engineering_challenge::{
    catalog::{Catalog, Instrument, Inventory},
    collector::{self, Options, RegistryOptions},
    exchange::{Configuration, kalshi::Kalshi},
    health,
    output::{AuditRow, SnapshotReason},
    replay,
    replica::Frontier,
    transport::ConnectOptions,
};
use futures_util::{SinkExt, StreamExt, future::BoxFuture};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{broadcast, watch},
    time::{Instant, MissedTickBehavior},
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

type Socket = WebSocketStream<tokio::net::TcpStream>;

const ACTIVE: usize = 16;
const QUIET: usize = 2984;

fn kalshi_default_capacity() -> usize {
    engineering_challenge::exchange::kalshi::DEFAULT_SYMBOLS_PER_CONNECTION
}

// Prefix-disjoint namespaces: no quiet index can spell a churn market.
fn active(i: usize) -> String {
    format!("ACT-{i:02}")
}
fn quiet(i: usize) -> String {
    format!("QT-{i:05}")
}
fn churned(i: usize) -> String {
    format!("CH-{i}")
}
fn quiet_index(market: &str) -> Option<usize> {
    market.strip_prefix("QT-")?.parse().ok()
}

/// Which initial subscriptions receive a later, newer-clock sample.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Late {
    /// Only the first subscribe command (Codex's independent fixture).
    FirstCommand,
    /// Every 250-market subscribe/add batch: settled exposure ~half quiet.
    Everywhere,
}

/// Registry churn while maintenance runs: retire and add quiet markets.
struct ChurningCatalog {
    started: Instant,
    quiet: usize,
}
impl ChurningCatalog {
    fn roster(&self) -> (i64, Vec<String>) {
        let elapsed = self.started.elapsed();
        let mut markets: Vec<_> = (0..ACTIVE)
            .map(active)
            .chain((0..self.quiet).map(quiet))
            .collect();
        let mut version = 1;
        if elapsed >= Duration::from_millis(2500) {
            version = 2;
            markets.retain(|m| *m != quiet(1) && *m != quiet(2));
            markets.extend((0..3).map(churned));
        }
        if elapsed >= Duration::from_millis(6000) {
            version = 3;
            markets.retain(|m| *m != quiet(3));
            markets.extend((3..5).map(churned));
        }
        (version, markets)
    }
}
impl Catalog for ChurningCatalog {
    fn scan(&self, _since: Option<i64>) -> BoxFuture<'_, anyhow::Result<Inventory>> {
        Box::pin(async move {
            let (version, markets) = self.roster();
            Ok(Inventory {
                instruments: markets
                    .into_iter()
                    .map(|symbol| Instrument {
                        symbol,
                        eligible: true,
                    })
                    .collect(),
                watermark: 1_700_000_000 + version,
                pages: 1,
                elapsed: Duration::ZERO,
                authoritative_for_retirement: true,
                complete: true,
                metadata: None,
            })
        })
    }
}

#[derive(Clone)]
struct Change {
    clock: u64,
    market: String,
    at: Instant,
}

/// One exchange: a global clock and every market's current level.
struct Provider {
    clock: u64,
    quantity: BTreeMap<String, i64>,
}

#[derive(Clone, Default, Debug)]
struct Connections {
    active: BTreeSet<usize>,
    subscribed: BTreeMap<usize, Instant>,
    ready: BTreeSet<usize>,
    planned_closes: BTreeSet<usize>,
    closed: BTreeSet<usize>,
    retired_slots: Vec<usize>,
    probed_markets: usize,
    probe_commands: usize,
    first_probe: Option<Instant>,
    last_probe: Option<Instant>,
}

struct World {
    provider: Mutex<Provider>,
    changes: broadcast::Sender<Change>,
    connections: watch::Sender<Connections>,
    stopping: watch::Receiver<bool>,
    late: Late,
    quiet: usize,
    /// First sample of each market on the two first connections.
    initial: Mutex<BTreeMap<(usize, String), (u64, Instant)>>,
}

enum Outgoing {
    Subscribed(Value),
    Ok(Value),
    Snapshot {
        market: String,
        clock: u64,
        quantity: i64,
    },
    Delta {
        market: String,
        clock: u64,
    },
}

async fn send(ws: &mut Socket, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

/// Sample one market's complete state now. Subsequent deltas on this
/// connection must have a strictly larger global clock.
fn sample(
    world: &World,
    connection: usize,
    market: &str,
    known: &mut BTreeMap<String, u64>,
) -> Outgoing {
    let state = world.provider.lock().unwrap();
    let clock = state.clock;
    let quantity = state.quantity[market];
    known.insert(market.to_owned(), clock);
    if connection < 2 {
        world
            .initial
            .lock()
            .unwrap()
            .entry((connection, market.to_owned()))
            .or_insert((clock, Instant::now()));
    }
    Outgoing::Snapshot {
        market: market.to_owned(),
        clock,
        quantity,
    }
}

fn is_late(world: &World, connection: usize, market: &str, subscribe: bool) -> bool {
    connection < 2
        && quiet_index(market).is_some_and(|i| i < world.quiet && i % 2 == connection)
        && (subscribe || world.late == Late::Everywhere)
}

async fn feed(mut ws: Socket, connection: usize, slot: usize, world: Arc<World>) {
    world.connections.send_modify(|state| {
        state.active.insert(connection);
    });
    let lag = if slot == 0 {
        Duration::ZERO
    } else {
        Duration::from_millis(30)
    };
    // Subscribe before sampling: a racing change is in the snapshot or tail.
    let mut tail = world.changes.subscribe();
    let mut known = BTreeMap::<String, u64>::new();
    let mut outbox = VecDeque::<(Instant, Outgoing)>::new();
    let mut late = Vec::<String>::new();
    let mut late_tick = tokio::time::interval(Duration::from_millis(25));
    let mut seq = 0u64;
    let mut delivered_delta = false;
    let mut marker_sent = false;
    loop {
        let due = outbox.front().map(|(at, _)| *at);
        tokio::select! {
            _ = tokio::time::sleep_until(due.unwrap_or_else(Instant::now)), if due.is_some() => {
                let (_, outgoing) = outbox.pop_front().unwrap();
                let value = match outgoing {
                    Outgoing::Subscribed(value) => value,
                    Outgoing::Ok(mut value) => {
                        seq += 1;
                        value["seq"] = json!(seq);
                        value
                    }
                    Outgoing::Snapshot { market, clock, quantity } => {
                        seq += 1;
                        json!({"type":"orderbook_snapshot", "sid":7, "seq":seq,
                            "sending_ts_ms":1700000000000i64,
                            "msg":{"market_ticker":market, "clock":clock,
                                "yes_dollars_fp":[["0.42", quantity.to_string()]]}})
                    }
                    Outgoing::Delta { market, clock } => {
                        seq += 1;
                        delivered_delta = true;
                        json!({"type":"orderbook_delta", "sid":7, "seq":seq,
                            "sending_ts_ms":1700000000000i64,
                            "msg":{"market_ticker":market, "clock":clock, "side":"yes",
                                "price_dollars":"0.42", "delta_fp":"1"}})
                    }
                };
                send(&mut ws, value).await;
                if delivered_delta && late.is_empty() && !marker_sent {
                    // Ready means the Source consumed a real post-baseline tail.
                    marker_sent = true;
                    ws.send(Message::Ping(vec![connection as u8].into())).await.unwrap();
                }
            }
            _ = late_tick.tick(), if !late.is_empty() => {
                // The other first connection already sampled (and may publish)
                // these markets; this later sample carries a newer clock.
                let now = Instant::now();
                let flush: Vec<String> = match world.late {
                    Late::FirstCommand => {
                        let state = world.connections.borrow();
                        let due = state.subscribed.get(&connection).zip(state.subscribed.get(&(1 - connection)))
                            .map(|(a, b)| (*a).max(*b) + Duration::from_millis(150));
                        drop(state);
                        if due.is_some_and(|due| now >= due) { std::mem::take(&mut late) } else { vec![] }
                    }
                    Late::Everywhere => {
                        let initial = world.initial.lock().unwrap();
                        let (flush, keep): (Vec<_>, Vec<_>) = late.drain(..).partition(|m| {
                            initial.get(&(1 - connection, m.clone()))
                                .is_some_and(|(_, at)| now >= *at + Duration::from_millis(100))
                        });
                        late = keep;
                        flush
                    }
                };
                for market in flush {
                    let outgoing = sample(&world, connection, &market, &mut known);
                    outbox.push_back((now + lag, outgoing));
                }
            }
            change = tail.recv() => {
                let change = change.expect("bounded provider tail overflowed");
                if known.get(&change.market).is_some_and(|&clock| change.clock > clock) {
                    known.insert(change.market.clone(), change.clock);
                    outbox.push_back((change.at + lag, Outgoing::Delta { market: change.market, clock: change.clock }));
                }
            }
            message = ws.next() => match message {
                Some(Ok(Message::Text(text))) => {
                    let request: Value = serde_json::from_str(&text).unwrap();
                    let tickers: Vec<String> = request["params"]["market_tickers"]
                        .as_array().unwrap().iter().map(|m| m.as_str().unwrap().to_owned()).collect();
                    let now = Instant::now();
                    if request["cmd"] == "subscribe" {
                        outbox.push_back((now + lag, Outgoing::Subscribed(
                            json!({"id":request["id"], "type":"subscribed", "msg":{"sid":7}}))));
                        for market in tickers {
                            if is_late(&world, connection, &market, true) {
                                late.push(market);
                            } else {
                                let outgoing = sample(&world, connection, &market, &mut known);
                                outbox.push_back((now + lag, outgoing));
                            }
                        }
                        world.connections.send_modify(|state| { state.subscribed.insert(connection, now); });
                        continue;
                    }
                    assert_eq!(request["cmd"], "update_subscription", "{request}");
                    let ok = Outgoing::Ok(json!({"id":request["id"], "type":"ok", "sid":7, "msg":{}}));
                    match request["params"]["action"].as_str().unwrap() {
                        "get_snapshot" => {
                            world.connections.send_modify(|state| {
                                state.probed_markets += tickers.len();
                                state.probe_commands += 1;
                                state.first_probe.get_or_insert(now);
                                state.last_probe = Some(now);
                            });
                            outbox.push_back((now + lag, ok));
                            for market in tickers {
                                let outgoing = sample(&world, connection, &market, &mut known);
                                outbox.push_back((now + lag, outgoing));
                            }
                        }
                        "add_markets" => {
                            outbox.push_back((now + lag, ok));
                            for market in tickers {
                                if is_late(&world, connection, &market, false) {
                                    late.push(market);
                                } else {
                                    let outgoing = sample(&world, connection, &market, &mut known);
                                    outbox.push_back((now + lag, outgoing));
                                }
                            }
                        }
                        "delete_markets" => {
                            for market in &tickers {
                                known.remove(market);
                                late.retain(|m| m != market);
                            }
                            outbox.push_back((now + lag, ok));
                        }
                        other => panic!("unexpected native action {other}: {request}"),
                    }
                }
                Some(Ok(Message::Pong(payload))) if payload.as_ref() == [connection as u8] => {
                    world.connections.send_modify(|state| { state.ready.insert(connection); });
                }
                Some(Ok(Message::Ping(payload))) => ws.send(Message::Pong(payload)).await.unwrap(),
                Some(Ok(Message::Close(_))) => {
                    world.connections.send_modify(|state| {
                        assert!(state.active.remove(&connection));
                        state.closed.insert(connection);
                        if !*world.stopping.borrow() {
                            assert!(!state.active.is_empty(), "both feeds closed while live");
                            state.planned_closes.insert(connection);
                            state.retired_slots.push(slot);
                        }
                    });
                    ws.flush().await.unwrap();
                    return;
                }
                Some(Ok(_)) => {}
                other => panic!("native socket ended without a Close handshake: {other:?}"),
            }
        }
    }
}

struct Scenario {
    quiet: usize,
    capacity: usize,
    late: Late,
    failover_after: Duration,
    window: Duration,
    command_interval: Duration,
}

struct Outcome {
    quiet: usize,
    result: anyhow::Result<()>,
    state: Connections,
    summary: Value,
    initial: BTreeMap<(usize, String), (u64, Instant)>,
    output: std::path::PathBuf,
    audit: std::path::PathBuf,
    started: Instant,
    _temp: tempfile::TempDir,
}

async fn owned_fault(scenario: Scenario) -> Outcome {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let options = Options {
        symbols: vec![],
        rest_url: "unused".into(),
        ws_url: format!("ws://{}/trade-api/ws/v2", listener.local_addr().unwrap()),
        symbols_per_connection: scenario.capacity,
        max_connections: 2,
        max_symbols: scenario.capacity,
        replicas: 2,
        replica_lag: Duration::from_secs(2),
        failover_test_after: Some(scenario.failover_after),
        output: temp.path().join("capture.sbe"),
        audit: Some(temp.path().join("audit.jsonl")),
        summary: Some(temp.path().join("summary.json")),
        snapshot_interval: Duration::from_secs(1),
        flush_interval: Duration::from_secs(60),
        connect: ConnectOptions::default(),
        registry: RegistryOptions {
            poll_interval: Duration::from_millis(250),
            command_interval: scenario.command_interval,
            ..RegistryOptions::default()
        },
        health: health::Options {
            window: Duration::from_secs(4),
            warmup: Duration::from_secs(4),
            cooldown: Duration::from_secs(4),
            rotation_interval: Duration::ZERO,
            ..health::Options::default()
        },
    };
    assert_eq!(options.registry.command_batch, 250, "default batch size");
    assert_eq!(options.health.rotation_timeout, Duration::from_secs(30));
    let (cancel, stop) = watch::channel(false);
    let (server_cancel, server_stop) = watch::channel(false);
    let (connections, _) = watch::channel(Connections::default());
    let quantity: BTreeMap<String, i64> = (0..ACTIVE)
        .map(active)
        .chain((0..scenario.quiet).map(quiet))
        .chain((0..5).map(churned))
        .map(|market| (market, 10))
        .collect();
    let (changes, _) = broadcast::channel::<Change>(1 << 15);
    let world = Arc::new(World {
        provider: Mutex::new(Provider {
            clock: 1_000_000,
            quantity,
        }),
        changes,
        connections,
        stopping: stop.clone(),
        late: scenario.late,
        quiet: scenario.quiet,
        initial: Mutex::new(BTreeMap::new()),
    });
    let producer_world = world.clone();
    let mut producer_stop = server_stop.clone();
    let producer = async move {
        let mut tick = tokio::time::interval(Duration::from_millis(4));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut next = 0usize;
        loop {
            tokio::select! {
                _ = collector::stopped(&mut producer_stop) => break,
                _ = tick.tick() => {
                    let change = {
                        let mut state = producer_world.provider.lock().unwrap();
                        state.clock += 1;
                        let market = active(next % ACTIVE);
                        next += 1;
                        *state.quantity.get_mut(&market).unwrap() += 1;
                        Change { clock: state.clock, market, at: Instant::now() }
                    };
                    let _ = producer_world.changes.send(change);
                }
            }
        }
    };
    let server_world = world.clone();
    let request_stop = cancel.clone();
    let mut accept_stop = server_stop;
    let window = scenario.window;
    let server = async move {
        let mut peers = tokio::task::JoinSet::new();
        let mut count = 0usize;
        // Observe the whole bounded window after the owned fault: a
        // replacement's first tail is not proof every standby recovered.
        let deadline = tokio::time::sleep(window);
        tokio::pin!(deadline);
        let mut stopped = false;
        loop {
            tokio::select! {
                _ = collector::stopped(&mut accept_stop) => break,
                _ = &mut deadline, if !stopped => {
                    stopped = true;
                    let _ = request_stop.send(true);
                }
                accepted = listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    assert!(count < 24, "unexpected reconnect storm");
                    let connection = count;
                    count += 1;
                    let slot = if connection < 2 {
                        connection
                    } else {
                        server_world.connections.borrow().retired_slots[connection - 2]
                    };
                    let world = server_world.clone();
                    peers.spawn(async move {
                        let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                        feed(ws, connection, slot, world).await;
                    });
                }
            }
        }
        while let Some(peer) = peers.join_next().await {
            peer.unwrap();
        }
        server_world.connections.borrow().clone()
    };
    let catalog = Arc::new(ChurningCatalog {
        started: Instant::now(),
        quiet: scenario.quiet,
    });
    let collect = async {
        let result = collector::run_with_exchange(
            options.clone(),
            Configuration {
                protocol: Arc::new(Kalshi),
                auth: None,
                catalog: Some(catalog),
            },
            stop,
            cancel,
        )
        .await;
        server_cancel.send(true).unwrap();
        result
    };
    let started = Instant::now();
    let (result, state, ()) = tokio::time::timeout(window + Duration::from_secs(30), async {
        tokio::join!(collect, server, producer)
    })
    .await
    .expect("collector failed to stop and join");
    let summary: Value =
        serde_json::from_slice(&std::fs::read(options.summary.as_ref().unwrap()).unwrap()).unwrap();
    let initial = world.initial.lock().unwrap().clone();
    Outcome {
        quiet: scenario.quiet,
        result,
        state,
        summary,
        initial,
        output: options.output.clone(),
        audit: options.audit.clone().unwrap(),
        started,
        _temp: temp,
    }
}

/// Count stale-behind quiet markets each first connection could publish.
fn later_samples(
    initial: &BTreeMap<(usize, String), (u64, Instant)>,
    count: usize,
    a: usize,
) -> usize {
    (0..count)
        .map(quiet)
        .filter(|q| {
            initial
                .get(&(a, q.clone()))
                .zip(initial.get(&(1 - a, q.clone())))
                .is_some_and(|((x, _), (y, _))| x > y)
        })
        .count()
}

fn assert_continuous(outcome: &Outcome) -> (usize, usize) {
    let shard = &outcome.summary["shards"][0];
    for key in [
        "rotation_timeouts",
        "pause_expirations",
        "continuity_gaps",
        "hard_resets",
        "clock_regressions",
        "gaps",
        "maintenance_pending",
        "ack_timeouts",
        "unacknowledged_closes",
    ] {
        assert_eq!(shard[key], 0, "{key}: {shard}");
    }
    assert_eq!(shard["injected_disconnects"], 1, "{shard}");
    assert_eq!(shard["handoff_pauses"], 0, "rotation is disabled: {shard}");
    let state = &outcome.state;
    assert!(
        state.planned_closes.iter().any(|id| *id < 2) && state.ready.iter().any(|id| *id >= 2),
        "the injected source must close and a replacement consume a tail: {state:?}"
    );
    let epoch = outcome.summary["epoch"].as_u64().unwrap();
    assert!(epoch >= 2, "registry churn never reached the shard");
    assert_eq!(
        outcome.summary["desired"],
        (ACTIVE + outcome.quiet) as u64 + epoch - 1
    );
    assert_eq!(shard["desired"], outcome.summary["desired"]);
    assert_eq!(
        outcome.summary["complete_at_cutoff"], true,
        "{}",
        outcome.summary
    );
    let report = replay::verify(&outcome.output, Some(&outcome.audit), 4096).unwrap();
    assert!(report.periodic_checkpoints_verified > 0);
    assert_eq!(report.unverified_continuity_transitions, 0);
    assert_eq!(report.snapshot_only_baselines, 0);
    assert_eq!(report.legacy_unspecified_baselines, 0);
    let (mut failovers, mut recoveries) = (0usize, 0usize);
    let (mut reconnect_recoveries, mut reconciliations) = (0usize, 0usize);
    let mut published =
        BTreeMap::<(String, u64), engineering_challenge::output::SourceStamp>::new();
    for line in std::fs::read_to_string(&outcome.audit).unwrap().lines() {
        let row: AuditRow = serde_json::from_str(line).unwrap();
        let source = row.source.unwrap();
        assert_eq!(source.continuity_epoch, 0);
        let key = (row.symbol, source.incarnation);
        if let Some(previous) = published.get(&key) {
            let same_replica = source.replica == previous.replica;
            if !same_replica {
                assert_eq!(
                    row.reason,
                    SnapshotReason::Failover,
                    "a cross-replica cut must not be relabelled Recovery: {key:?}"
                );
            } else {
                assert!(source.generation >= previous.generation);
            }
            let frontier = |s: &engineering_challenge::output::SourceStamp| Frontier {
                clock: s.clock.expect("native comparable clock"),
                ordinal: if s.complete_clock {
                    0
                } else {
                    s.ordinal.expect("native prefix ordinal")
                },
                complete: s.complete_clock,
            };
            assert!(
                frontier(&source).covers(frontier(previous)),
                "even a Recovery-labelled snapshot must not reset the frontier: {key:?}"
            );
            assert!(source.output_epoch >= previous.output_epoch);
            if row.reason == SnapshotReason::Recovery {
                assert!(same_replica, "Recovery changed replica: {key:?}");
                assert!(source.output_epoch > previous.output_epoch);
                if source.generation == previous.generation {
                    reconciliations += 1;
                } else {
                    reconnect_recoveries += 1;
                }
                if recoveries < 3 {
                    eprintln!(
                        "covered same-replica Recovery: {key:?}, previous={previous:?}, next={source:?}"
                    );
                }
            }
        } else {
            assert_eq!(row.reason, SnapshotReason::Initial);
        }
        failovers += usize::from(row.reason == SnapshotReason::Failover);
        recoveries += usize::from(row.reason == SnapshotReason::Recovery);
        published.insert(key, source);
    }
    assert_eq!(failovers as u64, report.failover_baselines_verified);
    assert!(failovers > 0);
    // Recovery also names direct_baseline on the old replica's new generation,
    // provided it covers the floor, and a new snapshot on an active stream.
    // Neither is the destructive-refresh gap from R1.
    assert_eq!(recoveries, reconnect_recoveries + reconciliations);
    eprintln!(
        "owned fault audit: {failovers} Failover baselines, {reconnect_recoveries} same-replica reconnect recoveries, {reconciliations} same-generation reconciliations; every cross-replica cut is Failover, all frontiers cover and continuity epochs remain zero"
    );
    (failovers, recoveries)
}

fn describe(label: &str, outcome: &Outcome) {
    let state = &outcome.state;
    let probe_window = state
        .first_probe
        .zip(state.last_probe)
        .map(|(a, b)| b.duration_since(a));
    eprintln!(
        "{label}: ran {:?}; later samples conn0={} conn1={}; probes={} markets in {} commands over {probe_window:?}; connections={state:?}; shard={}",
        outcome.started.elapsed(),
        later_samples(&outcome.initial, outcome.quiet, 0),
        later_samples(&outcome.initial, outcome.quiet, 1),
        state.probed_markets,
        state.probe_commands,
        outcome.summary["shards"][0]
    );
}

/// First-command exposure at the new default shard bound. The old 3,000-book
/// variant could expose >2,500 stale books, exceeding the unchanged five-second
/// recovery throughput bound; its failed artifacts remain in the repair logs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_fault_preserves_coverage_at_the_default_2250_symbol_shard_limit() {
    let outcome = owned_fault(Scenario {
        quiet: kalshi_default_capacity() - ACTIVE - 2,
        capacity: kalshi_default_capacity(),
        late: Late::FirstCommand,
        failover_after: Duration::from_secs(2),
        window: Duration::from_secs(30),
        command_interval: Duration::from_millis(1),
    })
    .await;
    describe("owned fault (first-command exposure)", &outcome);
    if let Err(error) = &outcome.result {
        panic!("collector failed: {error:#}");
    }
    assert!(
        later_samples(&outcome.initial, outcome.quiet, 0) > 0
            && later_samples(&outcome.initial, outcome.quiet, 1) > 0,
        "fixture did not stage later quiet samples on both first connections"
    );
    assert_continuous(&outcome);
    assert!(
        outcome.state.probed_markets > 0,
        "survivor covering requests are real provider snapshots"
    );
}

/// Settled exposure: later samples in every batch and a fault only after the
/// peer-ahead grace has moved roughly half the quiet books to each replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_fault_after_settling_probes_over_a_thousand_survivor_books_without_gaps() {
    let outcome = owned_fault(Scenario {
        quiet: QUIET,
        capacity: 4096,
        late: Late::Everywhere,
        failover_after: Duration::from_secs(16),
        window: Duration::from_secs(32),
        command_interval: RegistryOptions::default().command_interval,
    })
    .await;
    describe("owned fault (settled exposure)", &outcome);
    if let Err(error) = &outcome.result {
        panic!("collector failed: {error:#}");
    }
    assert!(later_samples(&outcome.initial, outcome.quiet, 0) > 1000);
    assert!(later_samples(&outcome.initial, outcome.quiet, 1) > 1000);
    assert_continuous(&outcome);
    assert!(
        outcome.state.probed_markets > 1000,
        "the fault did not expose >1,000 stale survivor books: {:?}",
        outcome.state
    );
    assert!(outcome.state.probe_commands >= 5, "250-market batches");
}
