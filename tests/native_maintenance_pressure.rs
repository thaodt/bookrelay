//! Native planned handoff at shard scale on a localhost exchange.
//!
//! Clock model: the fixture provider uses ONE exchange-wide comparable clock.
//! That is an inferred model of the live native feed (one 15-minute market
//! carried clock 18,710,915,846), not a documented provider contract. Under
//! it, a quiet market's later snapshot on one connection has a higher clock
//! and unchanged levels. That standby becomes the publisher after the ordinary
//! peer-ahead grace; the other replica then holds an older complete clock that
//! no ordered tail can advance, so a planned pause of that publisher could
//! only expire unless the peer first obtains a covering snapshot.
//!
//! Every snapshot samples the one provider state when it is sent; each
//! connection is a single ordered tail with its physical slot's delay. Nothing
//! teleports a peer ahead and no cut is accepted without current-floor cover.
use engineering_challenge::{
    catalog::{Catalog, Instrument, Inventory},
    collector::{self, Options, RegistryOptions},
    exchange::{Configuration, kalshi::Kalshi},
    health,
    output::{AuditRow, SnapshotReason},
    replay,
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
    /// Only the first subscribe command.
    FirstCommand,
    /// Every 250-market subscribe/add batch: settled exposure ~half quiet.
    Everywhere,
    /// Recovery-witness mode: only the first replacement's guard baseline,
    /// sampled once that socket's delayed get_snapshot request is captured.
    DelayedProbe,
}

/// Optional registry churn while maintenance runs.
struct Catalogue {
    started: Instant,
    quiet: usize,
    churn: bool,
}
impl Catalogue {
    fn roster(&self) -> (i64, Vec<String>) {
        let elapsed = self.started.elapsed();
        let mut markets: Vec<_> = (0..ACTIVE)
            .map(active)
            .chain((0..self.quiet).map(quiet))
            .collect();
        let mut version = 1;
        if self.churn && elapsed >= Duration::from_millis(2500) {
            version = 2;
            markets.retain(|m| *m != quiet(1) && *m != quiet(2));
            markets.extend((0..3).map(churned));
        }
        if self.churn && elapsed >= Duration::from_millis(6000) {
            version = 3;
            markets.retain(|m| *m != quiet(3));
            markets.extend((3..5).map(churned));
        }
        (version, markets)
    }
}
impl Catalog for Catalogue {
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
    first_planned_close: Option<Instant>,
    /// Recovery-witness protocol events, totally ordered by this watch's lock.
    barriers: Vec<Barrier>,
    replacement_sample: Option<u64>,
    delayed_clock: Option<u64>,
}
impl Connections {
    fn reached(&self, barrier: Barrier) -> bool {
        self.barriers.contains(&barrier)
    }
}

/// Happens-before chain of the Recovery-witness mode. Accept order is not
/// replica identity (both Sources race the shared pacer) and the first
/// rotation always targets replica 0, so the staging cannot rest on the two
/// original sockets. It uses connection 2, the replacement from the first
/// planned Close and therefore always replica 0 generation 2. The survivor is
/// then the publisher that the second rotation cuts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Barrier {
    /// Connection 2 sampled the delayed market; its outbox stays held.
    ReplacementSampled,
    /// Pong after the survivor sent a newer delta for that market: the delta
    /// is relayed, so connection 2's older baseline is a genuine acquisition.
    PublisherAhead,
    /// The real get_snapshot was ACKed and its data answer withheld.
    ProbeHeld,
    /// Connection 2's guard baseline, which every cut needs, was released.
    GuardReleased,
    /// Second planned Close, which follows the Owner's admitted output cut.
    CutClosed,
    /// The answer, freshly sampled after the cut, was written to the socket.
    AnswerSent,
    /// Pong after the answer: Source relayed it to the Owner.
    AnswerRelayed,
    /// Connection 3 may now deliver; it cannot have preempted the answer.
    ReplacementReleased,
}
const PUBLISHER_AHEAD: u8 = 0xA1;
const ANSWER_RELAYED: u8 = 0xA2;

type ProbeReply = (usize, u64, String, u64);

struct World {
    probe_replies: Mutex<Vec<ProbeReply>>,
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
        probe: bool,
    },
    Delta {
        market: String,
        clock: u64,
    },
    /// In-band Ping: on its Pong, Source has relayed everything queued earlier.
    Marker(u8),
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
        probe: false,
    }
}

fn is_late(world: &World, connection: usize, market: &str, subscribe: bool) -> bool {
    if world.late == Late::DelayedProbe {
        return connection == 2 && quiet_index(market) == Some(0);
    }
    connection < 2
        && quiet_index(market).is_some_and(|i| i < world.quiet && i % 2 == connection)
        && (subscribe || world.late == Late::Everywhere)
}

async fn feed(mut ws: Socket, connection: usize, slot: usize, world: Arc<World>) {
    world.connections.send_modify(|state| {
        state.active.insert(connection);
    });
    // This fixture has no failed dials: each planned Close advances exactly
    // one physical slot's generation. Unexpected retries fail witness matching.
    let generation = 1 + world
        .connections
        .borrow()
        .retired_slots
        .iter()
        .filter(|&&retired| retired == slot)
        .count() as u64;
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
    let witness = world.late == Late::DelayedProbe;
    let mut gate = world.connections.subscribe();
    let (mut probe_taken, mut answer_held) = (false, false);
    let (mut publisher_marked, mut replacement_released) = (false, false);
    loop {
        let mut held = false;
        if witness {
            let (ahead, cut, relayed) = {
                let state = world.connections.borrow();
                (
                    state.reached(Barrier::PublisherAhead),
                    state.reached(Barrier::CutClosed),
                    state.reached(Barrier::AnswerRelayed),
                )
            };
            if answer_held && cut {
                answer_held = false;
                // Fresh state: every queued delta precedes this clock and
                // `known` drops the racing ones, so this socket stays FIFO.
                let mut outgoing = sample(&world, connection, &active(0), &mut known);
                if let Outgoing::Snapshot { clock, probe, .. } = &mut outgoing {
                    *probe = true;
                    let clock = *clock;
                    world
                        .connections
                        .send_modify(|state| state.delayed_clock = Some(clock));
                }
                let at = Instant::now() + lag;
                outbox.push_back((at, outgoing));
                outbox.push_back((at, Outgoing::Marker(ANSWER_RELAYED)));
            }
            if connection == 3 && relayed && !replacement_released {
                replacement_released = true;
                world
                    .connections
                    .send_modify(|state| state.barriers.push(Barrier::ReplacementReleased));
            }
            held = connection == 2 && !ahead || connection == 3 && !relayed;
        }
        let due = outbox.front().filter(|_| !held).map(|(at, _)| *at);
        tokio::select! {
            _ = tokio::time::sleep_until(due.unwrap_or_else(Instant::now)), if due.is_some() => {
                let (_, outgoing) = outbox.pop_front().unwrap();
                let mut publisher_ahead = false;
                let mut answer_sent = false;
                let value = match outgoing {
                    Outgoing::Marker(mark) => {
                        ws.send(Message::Ping(vec![connection as u8, mark].into())).await.unwrap();
                        continue;
                    }
                    Outgoing::Subscribed(value) => value,
                    Outgoing::Ok(mut value) => {
                        seq += 1;
                        value["seq"] = json!(seq);
                        value
                    }
                    Outgoing::Snapshot { market, clock, quantity, probe } => {
                        if probe {
                            world.probe_replies.lock().unwrap().push((slot, generation, market.clone(), clock));
                            if witness && connection == 2 && market == active(0) {
                                answer_sent = true;
                            }
                        }
                        seq += 1;
                        json!({"type":"orderbook_snapshot", "sid":7, "seq":seq,
                            "sending_ts_ms":1700000000000i64,
                            "msg":{"market_ticker":market, "clock":clock,
                                "yes_dollars_fp":[["0.42", quantity.to_string()]]}})
                    }
                    Outgoing::Delta { market, clock } => {
                        seq += 1;
                        delivered_delta = true;
                        if witness && connection < 2 && !publisher_marked && market == active(0) {
                            let sample = world.connections.borrow().replacement_sample;
                            publisher_ahead = sample.is_some_and(|sampled| clock > sampled);
                        }
                        json!({"type":"orderbook_delta", "sid":7, "seq":seq,
                            "sending_ts_ms":1700000000000i64,
                            "msg":{"market_ticker":market, "clock":clock, "side":"yes",
                                "price_dollars":"0.42", "delta_fp":"1"}})
                    }
                };
                send(&mut ws, value).await;
                if answer_sent {
                    world.connections.send_modify(|state| state.barriers.push(Barrier::AnswerSent));
                }
                if publisher_ahead {
                    publisher_marked = true;
                    ws.send(Message::Ping(vec![connection as u8, PUBLISHER_AHEAD].into())).await.unwrap();
                }
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
                    Late::DelayedProbe => {
                        if world.connections.borrow().reached(Barrier::ProbeHeld) {
                            world.connections.send_modify(|state| state.barriers.push(Barrier::GuardReleased));
                            std::mem::take(&mut late)
                        } else {
                            vec![]
                        }
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
                        if witness && connection == 2 {
                            let sampled = known[&active(0)];
                            world.connections.send_modify(|state| {
                                state.replacement_sample = Some(sampled);
                                state.barriers.push(Barrier::ReplacementSampled);
                            });
                        }
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
                                if witness && connection == 2 && !probe_taken && market == active(0) {
                                    (probe_taken, answer_held) = (true, true);
                                    world.connections.send_modify(|state| state.barriers.push(Barrier::ProbeHeld));
                                    continue;
                                }
                                let mut outgoing = sample(&world, connection, &market, &mut known);
                                if let Outgoing::Snapshot { probe, .. } = &mut outgoing { *probe = true; }
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
                Some(Ok(Message::Pong(payload))) if witness && payload.len() == 2 && payload[0] == connection as u8 => {
                    let barrier = if payload[1] == PUBLISHER_AHEAD { Barrier::PublisherAhead } else { Barrier::AnswerRelayed };
                    world.connections.send_modify(|state| state.barriers.push(barrier));
                }
                Some(Ok(Message::Ping(payload))) => ws.send(Message::Pong(payload)).await.unwrap(),
                Some(Ok(Message::Close(_))) => {
                    world.connections.send_modify(|state| {
                        assert!(state.active.remove(&connection));
                        state.closed.insert(connection);
                        if !*world.stopping.borrow() {
                            assert!(!state.active.is_empty(), "planned rotation removed both feeds");
                            state.planned_closes.insert(connection);
                            state.retired_slots.push(slot);
                            state.first_planned_close.get_or_insert_with(Instant::now);
                            if witness && state.retired_slots.len() == 2 {
                                state.barriers.push(Barrier::CutClosed);
                            }
                        }
                    });
                    ws.flush().await.unwrap();
                    return;
                }
                Some(Ok(_)) => {}
                other => panic!("native socket ended without a Close handshake: {other:?}"),
            },
            changed = gate.changed(), if witness => changed.unwrap(),
        }
    }
}

struct Scenario {
    quiet: usize,
    late: Late,
    churn: bool,
    deadline: Duration,
    finished: fn(&Connections) -> bool,
    configure: fn(&mut Options),
}

struct Outcome {
    probe_replies: BTreeMap<ProbeReply, usize>,
    result: anyhow::Result<()>,
    state: Connections,
    summary: Value,
    initial: BTreeMap<(usize, String), (u64, Instant)>,
    options: Options,
    started: Instant,
    _temp: tempfile::TempDir,
}

async fn planned(scenario: Scenario) -> Outcome {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut options = Options {
        symbols: vec![],
        rest_url: "unused".into(),
        ws_url: format!("ws://{}/trade-api/ws/v2", listener.local_addr().unwrap()),
        symbols_per_connection: 4096,
        max_connections: 2,
        max_symbols: 4096,
        replicas: 2,
        replica_lag: Duration::from_millis(100),
        failover_test_after: None,
        output: temp.path().join("capture.sbe"),
        audit: Some(temp.path().join("audit.jsonl")),
        summary: Some(temp.path().join("summary.json")),
        snapshot_interval: Duration::from_secs(1),
        flush_interval: Duration::from_secs(60),
        connect: ConnectOptions::default(),
        registry: RegistryOptions {
            poll_interval: Duration::from_millis(250),
            ..RegistryOptions::default()
        },
        health: health::Options {
            window: Duration::from_secs(4),
            warmup: Duration::from_secs(4),
            cooldown: Duration::from_secs(4),
            ..health::Options::default()
        },
    };
    (scenario.configure)(&mut options);
    let (cancel, stop) = watch::channel(false);
    let (server_cancel, server_stop) = watch::channel(false);
    let (connections, mut observed) = watch::channel(Connections::default());
    let quantity: BTreeMap<String, i64> = (0..ACTIVE)
        .map(active)
        .chain((0..scenario.quiet).map(quiet))
        .chain((0..5).map(churned))
        .map(|market| (market, 10))
        .collect();
    let (changes, _) = broadcast::channel::<Change>(1 << 15);
    let world = Arc::new(World {
        probe_replies: Mutex::new(Vec::new()),
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
    let (deadline_after, finished) = (scenario.deadline, scenario.finished);
    let server = async move {
        let mut peers = tokio::task::JoinSet::new();
        let mut count = 0usize;
        let mut done = false;
        let deadline = tokio::time::sleep(deadline_after);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                _ = collector::stopped(&mut accept_stop) => break,
                _ = &mut deadline, if !done => {
                    // Let the collector report its honest state instead of hanging.
                    done = true;
                    let _ = request_stop.send(true);
                }
                changed = observed.changed(), if !done => {
                    changed.unwrap();
                    if finished(&observed.borrow()) {
                        done = true;
                        let _ = request_stop.send(true);
                    }
                }
                accepted = listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    assert!(count < 6, "unexpected reconnect storm");
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
    let catalog = Arc::new(Catalogue {
        started: Instant::now(),
        quiet: scenario.quiet,
        churn: scenario.churn,
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
    let (result, state, ()) =
        tokio::time::timeout(deadline_after + Duration::from_secs(30), async {
            tokio::join!(collect, server, producer)
        })
        .await
        .expect("collector failed to stop and join");
    let summary: Value =
        serde_json::from_slice(&std::fs::read(options.summary.as_ref().unwrap()).unwrap()).unwrap();
    let initial = world.initial.lock().unwrap().clone();
    let mut probe_replies = BTreeMap::new();
    for (slot, generation, market, clock) in world.probe_replies.lock().unwrap().iter() {
        let key = (
            replica_of(&state, *slot),
            *generation,
            market.clone(),
            *clock,
        );
        *probe_replies.entry(key).or_insert(0) += 1;
    }
    Outcome {
        probe_replies,
        result,
        state,
        summary,
        initial,
        options,
        started,
        _temp: temp,
    }
}

/// Stale-behind quiet markets connection `a` could publish (fixture check).
fn later_samples(
    initial: &BTreeMap<(usize, String), (u64, Instant)>,
    quiet_markets: usize,
    a: usize,
) -> usize {
    (0..quiet_markets)
        .map(quiet)
        .filter(|q| {
            initial
                .get(&(a, q.clone()))
                .zip(initial.get(&(1 - a, q.clone())))
                .is_some_and(|((x, _), (y, _))| x > y)
        })
        .count()
}

/// Fixture slots are accept-order labels, but audit rows carry the real
/// replica. The first planned Close is always replica 0: `rotation_slot` starts
/// at 0 and moves only when a rotation completes.
fn replica_of(state: &Connections, slot: usize) -> usize {
    match state.retired_slots.first() {
        Some(&first) => usize::from(slot != first),
        None => slot,
    }
}

fn consume_probe_reply(replies: &mut BTreeMap<ProbeReply, usize>, key: ProbeReply) -> bool {
    if let Some(count) = replies.get_mut(&key)
        && *count > 0
    {
        *count -= 1;
        true
    } else {
        false
    }
}

#[test]
fn probe_reply_witness_rejects_wrong_lifetime_clock_and_duplicate_credit() {
    let key = (1, 2, "A".to_string(), 10);
    let mut replies = BTreeMap::from([(key.clone(), 1)]);
    for wrong in [
        (0, 2, "A".into(), 10),
        (1, 1, "A".into(), 10),
        (1, 2, "B".into(), 10),
        (1, 2, "A".into(), 9),
    ] {
        assert!(!consume_probe_reply(&mut replies, wrong));
    }
    assert!(consume_probe_reply(&mut replies, key.clone()));
    assert!(!consume_probe_reply(&mut replies, key));
}

/// Strict replay; returns (Failover, Recovery) baseline counts. A late valid
/// probe can arrive after its standby becomes publisher. That active provider
/// snapshot is honestly Recovery, not a destructive cut. Every such row needs
/// a unique real get_snapshot reply witness on the same slot/generation/clock.
fn verify_output(outcome: &Outcome) -> (usize, Vec<ProbeReply>) {
    let options = &outcome.options;
    let report = replay::verify(&options.output, options.audit.as_deref(), 4096).unwrap();
    assert!(report.periodic_checkpoints_verified > 0);
    assert!(report.failover_baselines_verified > 0);
    assert_eq!(report.unverified_continuity_transitions, 0);
    assert_eq!(report.snapshot_only_baselines, 0);
    assert_eq!(report.legacy_unspecified_baselines, 0);
    let (mut failovers, mut recoveries) = (0usize, Vec::new());
    let mut probe_replies = outcome.probe_replies.clone();
    for line in std::fs::read_to_string(options.audit.as_ref().unwrap())
        .unwrap()
        .lines()
    {
        let row: AuditRow = serde_json::from_str(line).unwrap();
        let source = row.source.unwrap();
        assert_eq!(source.continuity_epoch, 0);
        if row.reason == SnapshotReason::Recovery {
            let key = (
                source.replica,
                source.generation,
                row.symbol,
                source.clock.unwrap(),
            );
            assert!(
                consume_probe_reply(&mut probe_replies, key.clone()),
                "Recovery lacks an unused real provider probe reply witness: {key:?}"
            );
            recoveries.push(key);
        }
        failovers += usize::from(row.reason == SnapshotReason::Failover);
    }
    eprintln!(
        "native replay: {failovers} Failover baselines; {} Recovery rows matched unique provider probe replies",
        recoveries.len()
    );
    (failovers, recoveries)
}

fn assert_clean(shard: &Value) {
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scale_planned_handoff_obtains_covering_peer_baselines_for_quiet_markets_under_churn() {
    const QUIET: usize = 304;
    let outcome = planned(Scenario {
        quiet: QUIET,
        late: Late::FirstCommand,
        churn: true,
        deadline: Duration::from_secs(25),
        finished: |state| {
            [0, 1].iter().all(|id| state.planned_closes.contains(id))
                && [2, 3].iter().all(|id| state.ready.contains(id))
        },
        configure: |options| {
            options.registry.command_interval = Duration::from_millis(1);
            options.registry.command_batch = 1024;
            options.health.rotation_interval = Duration::from_secs(2);
            options.health.rotation_timeout = Duration::from_secs(3);
        },
    })
    .await;
    let shard = outcome.summary["shards"][0].clone();
    eprintln!(
        "scale handoff fixture ran {:?}; connections={:?}; shard={shard}",
        outcome.started.elapsed(),
        outcome.state
    );
    if let Err(error) = &outcome.result {
        panic!("collector failed: {error:#}");
    }
    assert!(
        later_samples(&outcome.initial, QUIET, 0) > 0
            && later_samples(&outcome.initial, QUIET, 1) > 0,
        "fixture did not stage later quiet snapshots on both first connections"
    );
    assert_clean(&shard);
    assert!(
        shard["completed_rotations"].as_u64().unwrap() >= 2,
        "{shard}"
    );
    assert!(shard["handoff_pauses"].as_u64().unwrap() > 0, "{shard}");
    let state = &outcome.state;
    assert!(
        [0, 1].iter().all(|id| state.planned_closes.contains(id))
            && [2, 3].iter().all(|id| state.ready.contains(id)),
        "both replicas must rotate and replacements consume ordered tails: {state:?}"
    );
    assert!(
        state.probed_markets > 0,
        "covering peer baselines are real provider snapshots: {state:?}"
    );
    // Registry churn was applied while maintenance ran; each epoch nets +1.
    let epoch = outcome.summary["epoch"].as_u64().unwrap();
    assert!(epoch >= 2, "registry churn never reached the shard");
    assert_eq!(
        outcome.summary["desired"],
        (ACTIVE + QUIET) as u64 + epoch - 1
    );
    assert_eq!(shard["desired"], outcome.summary["desired"]);
    assert_eq!(outcome.summary["complete_at_cutoff"], true);
    // Planned cuts remain Failover. Legitimate late provider replies are
    // checked against exact wire witnesses inside verify_output, not waived.
    verify_output(&outcome);
}

/// R2 batch-scale proof (Fable Finding 5): 3,000 markets, default 250-market
/// command batches, default 30 s rotation timeout and 5 s lease, rotation only
/// after the peer-ahead grace has settled >1,000 stale witnesses onto slot 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settled_three_thousand_market_rotation_probes_over_a_thousand_witnesses_at_batch_250() {
    const QUIET: usize = 2984;
    let outcome = planned(Scenario {
        quiet: QUIET,
        late: Late::Everywhere,
        churn: false,
        deadline: Duration::from_secs(60),
        finished: |state| {
            state.planned_closes.iter().any(|id| *id < 2) && state.ready.iter().any(|id| *id >= 2)
        },
        configure: |options| {
            options.replica_lag = Duration::from_secs(2);
            options.snapshot_interval = Duration::from_secs(5);
            // Rotation begins only after initial batches and peer-ahead settle.
            options.health.rotation_interval = Duration::from_secs(20);
        },
    })
    .await;
    let options = &outcome.options;
    assert_eq!(options.registry.command_batch, 250);
    assert_eq!(options.health.rotation_timeout, Duration::from_secs(30));
    let shard = outcome.summary["shards"][0].clone();
    let state = &outcome.state;
    let probe_window = state
        .first_probe
        .zip(state.last_probe)
        .map(|(a, b)| b.duration_since(a));
    let probe_to_cut = state
        .first_probe
        .zip(state.first_planned_close)
        .map(|(a, b)| b.saturating_duration_since(a));
    eprintln!(
        "settled 3000 rotation ran {:?}; later samples conn0={} conn1={}; probes={} markets in {} commands over {probe_window:?}; first probe to planned close {probe_to_cut:?}; connections={state:?}; shard={shard}",
        outcome.started.elapsed(),
        later_samples(&outcome.initial, QUIET, 0),
        later_samples(&outcome.initial, QUIET, 1),
        state.probed_markets,
        state.probe_commands,
    );
    if let Err(error) = &outcome.result {
        panic!("collector failed: {error:#}");
    }
    assert!(later_samples(&outcome.initial, QUIET, 0) > 1000);
    assert!(later_samples(&outcome.initial, QUIET, 1) > 1000);
    assert_clean(&shard);
    assert!(
        shard["completed_rotations"].as_u64().unwrap() >= 1,
        "{shard}"
    );
    assert!(
        state.probed_markets > 1000,
        "rotation did not need >1,000 real covering probes: {state:?}"
    );
    assert!(state.probe_commands >= 5, "250-market probe batches");
    assert!(
        state.first_probe.unwrap() < state.first_planned_close.unwrap(),
        "probes precede the destructive close"
    );
    assert_eq!(outcome.summary["complete_at_cutoff"], true);
    // Planned cuts remain Failover. Legitimate late provider replies are
    // checked against exact wire witnesses inside verify_output, not waived.
    verify_output(&outcome);
}

/// R3 (Fable F1): the positive wire path of the Recovery provenance oracle.
/// One real get_snapshot answer is withheld across a planned handoff, then
/// sampled fresh and released after the old publisher's Close. Each ordering
/// step below is a protocol barrier; see `Barrier`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_probe_answer_held_across_a_planned_handoff_is_one_witnessed_recovery() {
    const QUIET: usize = 1;
    let outcome = planned(Scenario {
        quiet: QUIET,
        late: Late::DelayedProbe,
        churn: false,
        deadline: Duration::from_secs(40),
        finished: |state| state.reached(Barrier::AnswerRelayed) && state.ready.contains(&3),
        configure: |options| {
            // The production grace: only a planned cut moves the held market,
            // absent a two-second stall of the publisher's whole relay path.
            options.replica_lag = Duration::from_secs(2);
            options.health.rotation_interval = Duration::from_secs(2);
        },
    })
    .await;
    let options = &outcome.options;
    assert_eq!(options.registry.command_batch, 250);
    assert_eq!(
        options.registry.command_interval,
        Duration::from_millis(100)
    );
    assert_eq!(options.health.rotation_timeout, Duration::from_secs(30));
    let shard = outcome.summary["shards"][0].clone();
    let state = &outcome.state;
    eprintln!(
        "recovery witness fixture ran {:?}; connections={state:?}; shard={shard}",
        outcome.started.elapsed()
    );
    if let Err(error) = &outcome.result {
        panic!("collector failed: {error:#}");
    }
    assert_clean(&shard);
    let mut order = state.barriers.iter();
    for barrier in [
        Barrier::ReplacementSampled,
        Barrier::PublisherAhead,
        Barrier::ProbeHeld,
        Barrier::GuardReleased,
        Barrier::CutClosed,
        Barrier::AnswerSent,
        Barrier::AnswerRelayed,
        Barrier::ReplacementReleased,
    ] {
        assert!(
            order.any(|seen| *seen == barrier),
            "barrier {barrier:?} missing or out of order: {:?}",
            state.barriers
        );
    }
    assert!(state.planned_closes.len() >= 2, "{state:?}");
    assert!(state.ready.contains(&3), "{state:?}");
    for key in ["planned_rotations", "completed_rotations"] {
        assert!(shard[key].as_u64().unwrap() >= 2, "{key}: {shard}");
    }
    assert!(shard["handoff_pauses"].as_u64().unwrap() > 0, "{shard}");
    assert!(
        shard["failovers"].as_u64().unwrap() >= ACTIVE as u64,
        "{shard}"
    );
    assert_eq!(shard["desired"], (ACTIVE + QUIET) as u64);
    assert_eq!(outcome.summary["desired"], (ACTIVE + QUIET) as u64);
    assert_eq!(outcome.summary["complete_at_cutoff"], true);

    // Connection 2 is replica 0 generation 2 under either accept order.
    let clock = state
        .delayed_clock
        .expect("the delayed answer was never sampled");
    let key: ProbeReply = (0, 2, active(0), clock);
    let answers = outcome
        .probe_replies
        .iter()
        .filter(|((replica, generation, market, _), _)| {
            *replica == key.0 && *generation == key.1 && *market == key.2
        })
        .collect::<Vec<_>>();
    assert_eq!(
        answers,
        [(&key, &1)],
        "the delayed market must have exactly one real probe reply"
    );
    let (failovers, recoveries) = verify_output(&outcome);
    assert!(failovers >= ACTIVE, "planned Failovers: {failovers}");
    assert_eq!(
        recoveries.iter().filter(|seen| **seen == key).count(),
        1,
        "the delayed key must be exactly one Recovery row: {recoveries:?}"
    );

    // Its standby had already become the publisher: the row directly follows
    // that source's Failover baseline in the output's own epoch sequence.
    let rows: Vec<AuditRow> = std::fs::read_to_string(options.audit.as_ref().unwrap())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let baseline = |reason: SnapshotReason| {
        let (position, row) = rows
            .iter()
            .enumerate()
            .find(|(_, row)| {
                row.reason == reason
                    && row.symbol == key.2
                    && row.source.as_ref().is_some_and(|source| {
                        source.replica == key.0
                            && source.generation == key.1
                            && (reason != SnapshotReason::Recovery || source.clock == Some(clock))
                    })
            })
            .unwrap_or_else(|| panic!("no {reason:?} baseline for {key:?}"));
        (position, row.source.as_ref().unwrap().output_epoch)
    };
    let (failover, failover_epoch) = baseline(SnapshotReason::Failover);
    let (recovery, recovery_epoch) = baseline(SnapshotReason::Recovery);
    assert!(failover < recovery, "Recovery preceded its Failover");
    assert_eq!(recovery_epoch, failover_epoch + 1);
    eprintln!(
        "positive recovery witness: key={key:?}; {failovers} Failovers; Recovery rows={recoveries:?}; audit rows {failover}->{recovery}; barriers={:?}",
        state.barriers
    );
}
