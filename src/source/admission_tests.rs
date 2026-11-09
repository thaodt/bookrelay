//! Admission through the real `source::run` path: loopback WebSockets, real
//! Kalshi framing, one shared 100ms Pacer.
//!
//! The scale test deliberately runs on the wall clock for over a minute. Paused
//! Tokio time auto-advances past localhost IO and manufactures heartbeat and
//! ACK timeouts. Its deadlines are a test envelope, not a latency guarantee.
use super::*;
use crate::{collector::RegistryOptions, exchange::polymarket::Polymarket, relay::Admitted};
use serde_json::{Value, json};
use std::{collections::HashSet, sync::Mutex};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tokio_tungstenite::{WebSocketStream, accept_async};

const SID: u64 = 7;
const INTERVAL: Duration = Duration::from_millis(100);
const WARM: usize = 165;
const LARGE: usize = 2250;

type Ws = WebSocketStream<TcpStream>;

fn options(ws_url: String, command_batch: usize) -> Options {
    Options {
        symbols: vec![],
        rest_url: "http://127.0.0.1:1".into(),
        ws_url,
        symbols_per_connection: LARGE,
        max_connections: 196,
        max_symbols: 1 << 20,
        output: "unused-admission-test.sbe".into(),
        audit: None,
        snapshot_interval: Duration::from_secs(60),
        flush_interval: Duration::from_secs(60),
        connect: crate::transport::ConnectOptions::default(),
        registry: RegistryOptions {
            command_batch,
            ..Default::default()
        },
        summary: None,
        replicas: 2,
        replica_lag: Duration::from_secs(2),
        failover_test_after: None,
        health: crate::health::Options::default(),
    }
}

fn assignment(epoch: u64, symbols: Vec<String>) -> Assignment {
    Assignment {
        epoch,
        incarnations: symbols.iter().map(|s| (s.clone(), 1)).collect(),
        symbols,
    }
}

fn snapshot(seq: u64, market: &str) -> Value {
    json!({"type":"orderbook_snapshot", "sid":SID, "seq":seq,
        "sending_ts_ms":1700000000000i64,
        "msg":{"market_ticker":market, "clock":seq + 100,
            "yes_dollars_fp":[["0.42", "10"]]}})
}

async fn send(ws: &mut Ws, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

async fn eventually(limit: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + limit;
    loop {
        if done() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// ---- Eligibility: no socket, paused clock ---------------------------------

fn session_runtime(protocol: Arc<dyn ExchangeAdapter>) -> (Runtime, mpsc::Receiver<Admitted>) {
    let (relay, rx) = relay::channel(Arc::new(Semaphore::new(1 << 20)));
    (
        Runtime {
            protocol,
            shard: 0,
            options: Arc::new(options("ws://127.0.0.1:1/".into(), 1)),
            auth: None,
            pacer: Arc::new(Pacer::new(INTERVAL)),
            relay,
        },
        rx,
    )
}

fn session_with<'a>(
    runtime: &'a Runtime,
    membership: Membership,
    symbols: Vec<String>,
) -> Session<'a> {
    Session {
        runtime,
        source: Source {
            replica: 0,
            generation: 1,
        },
        membership,
        assignment: assignment(1, symbols),
        tracker: SequenceTracker::default(),
        initialized: BTreeSet::new(),
        unavailable: BTreeSet::new(),
        dirty: BTreeSet::new(),
        invalid_since: HashMap::new(),
        attempts: HashMap::new(),
        observed_refresh: BTreeMap::new(),
        observed_resubscribe: BTreeMap::new(),
        observed_probe: BTreeMap::new(),
        resubscribe_started: BTreeMap::new(),
        restart: 0,
        fence: 0,
        next_command: Instant::now(),
        urgent: None,
        last_frame: Instant::now(),
        last_pong: Instant::now(),
        pause: None,
        pause_high_water: 0,
        release_high_water: 0,
        pause_not_before: Instant::now(),
    }
}

/// Subscribe ACKed for A with B still to Add: bootstrapping, real work due.
fn bootstrapping_session(runtime: &Runtime) -> Session<'_> {
    let symbols = vec!["A".to_owned(), "B".to_owned()];
    let mut membership = Membership::new(&symbols);
    membership.command(&BTreeSet::new(), 1).unwrap().unwrap();
    membership.command_written();
    membership
        .control(Some(1), &Reply::Subscribed(SID))
        .unwrap();
    assert!(membership.bootstrapping());
    assert!(membership.needs_command(&BTreeSet::new()));
    session_with(runtime, membership, symbols)
}

fn pause_request(nonce: u64) -> PauseRequest {
    PauseRequest {
        generation: 1,
        nonce,
        acquire_until: Instant::now() + Duration::from_secs(2),
        max_duration: MAX_PAUSE,
    }
}

#[tokio::test(start_paused = true)]
async fn denied_bootstrap_polls_keep_one_demand_until_admission() {
    let (runtime, _rx) = session_runtime(Arc::new(crate::exchange::kalshi::Kalshi));
    let mut live = bootstrapping_session(&runtime);
    assert!(runtime.pacer.admit(), "close the first global slot");
    for _ in 0..3 {
        assert!(!live.admit_command());
        assert_eq!(runtime.pacer.urgent_waiters(), 1, "persisted, not stacked");
    }
    tokio::time::advance(INTERVAL).await;
    assert!(
        !runtime.pacer.admit(),
        "routine yields the opening to demand"
    );
    assert!(live.admit_command());
    assert_eq!(runtime.pacer.urgent_waiters(), 0, "admission ends demand");
    assert!(live.urgent.is_none());
}

#[tokio::test(start_paused = true)]
async fn ineligible_bootstrap_work_never_publishes_demand() {
    let (runtime, _rx) = session_runtime(Arc::new(crate::exchange::kalshi::Kalshi));
    let mut live = bootstrapping_session(&runtime);
    live.next_command = Instant::now() + Duration::from_millis(500);
    assert!(!live.admit_command(), "the local floor has not passed");
    assert_eq!(runtime.pacer.urgent_waiters(), 0);
    live.next_command = Instant::now();
    live.membership
        .command(&BTreeSet::new(), 1)
        .unwrap()
        .unwrap();
    live.membership.command_written();
    assert!(!live.admit_command(), "waiting for the ACK");
    assert_eq!(runtime.pacer.urgent_waiters(), 0);
    live.membership.pending = None;
    live.membership.replace(&["A".into()]);
    assert!(!live.admit_command(), "the roster already matches the wire");
    assert_eq!(runtime.pacer.urgent_waiters(), 0);
}

#[tokio::test(start_paused = true)]
async fn pause_backoff_and_lost_work_withdraw_demand_without_a_health_tick() {
    let (runtime, _rx) = session_runtime(Arc::new(crate::exchange::kalshi::Kalshi));
    let mut live = bootstrapping_session(&runtime);
    let mut stats = Stats::default();
    assert!(runtime.pacer.admit());

    assert!(!live.admit_command());
    assert_eq!(runtime.pacer.urgent_waiters(), 1);
    live.pause_controls(Some(pause_request(1)), None, false, &mut stats)
        .unwrap();
    assert!(live.pause.is_some());
    live.release_stale_urgent();
    assert_eq!(runtime.pacer.urgent_waiters(), 0, "pause holds no demand");
    let release = PauseId {
        generation: 1,
        nonce: 1,
    };
    live.pause_controls(None, Some(release), false, &mut stats)
        .unwrap();

    assert!(!live.admit_command());
    assert_eq!(runtime.pacer.urgent_waiters(), 1, "republished after pause");
    let backoff = json!({"type":"error", "msg":{"code":27}}).to_string();
    live.receive(&backoff, Utc::now(), Instant::now(), &mut stats)
        .await
        .unwrap();
    live.release_stale_urgent();
    assert_eq!(runtime.pacer.urgent_waiters(), 0, "backoff holds no demand");
    assert!(!live.admit_command(), "the backoff floor has not passed");
    assert_eq!(runtime.pacer.urgent_waiters(), 0);

    let mut lost = bootstrapping_session(&runtime);
    assert!(!lost.admit_command());
    assert_eq!(runtime.pacer.urgent_waiters(), 1);
    lost.membership.replace(&["A".into()]);
    lost.release_stale_urgent();
    assert_eq!(runtime.pacer.urgent_waiters(), 0, "no real command remains");
}

#[tokio::test(start_paused = true)]
async fn dropping_a_session_withdraws_its_demand() {
    let (runtime, _rx) = session_runtime(Arc::new(crate::exchange::kalshi::Kalshi));
    let mut live = bootstrapping_session(&runtime);
    assert!(runtime.pacer.admit());
    assert!(!live.admit_command());
    assert_eq!(runtime.pacer.urgent_waiters(), 1);
    drop(live);
    assert_eq!(runtime.pacer.urgent_waiters(), 0);
}

#[tokio::test(start_paused = true)]
async fn old_session_guard_withdraws_only_itself_after_a_replacement_registers() {
    let (runtime, _rx) = session_runtime(Arc::new(crate::exchange::kalshi::Kalshi));
    let mut old = bootstrapping_session(&runtime);
    let mut replacement = bootstrapping_session(&runtime);
    assert!(runtime.pacer.admit());
    assert!(!old.admit_command());
    assert!(!replacement.admit_command());
    assert_eq!(runtime.pacer.urgent_waiters(), 2);
    drop(old);
    assert_eq!(runtime.pacer.urgent_waiters(), 1, "the replacement remains");
    tokio::time::advance(INTERVAL).await;
    assert!(!runtime.pacer.admit(), "routine still yields to it");
    assert!(replacement.admit_command());
    assert_eq!(runtime.pacer.urgent_waiters(), 0);
}

#[tokio::test(start_paused = true)]
async fn rejected_add_that_completes_the_roster_leaves_snapshot_recovery_routine() {
    let (runtime, _rx) = session_runtime(Arc::new(crate::exchange::kalshi::Kalshi));
    let mut membership = Membership::new(&["A".to_owned(), "B".to_owned()]);
    membership.command(&BTreeSet::new(), 1).unwrap().unwrap();
    membership.command_written();
    membership
        .control(Some(1), &Reply::Subscribed(SID))
        .unwrap();
    membership.command(&BTreeSet::new(), 1).unwrap().unwrap();
    membership.command_written();
    membership.replace(&["A".into()]);
    membership
        .control(
            Some(2),
            &Reply::Error {
                code: 27,
                message: "rejected".into(),
            },
        )
        .unwrap();
    let mut live = session_with(&runtime, membership, vec!["A".into()]);
    live.dirty.insert("A".into());
    assert!(runtime.pacer.admit());
    assert!(!live.admit_command());
    assert_eq!(runtime.pacer.urgent_waiters(), 0, "the roster was settled");
    tokio::time::advance(INTERVAL).await;
    assert!(live.admit_command());
}

#[tokio::test(start_paused = true)]
async fn settled_native_churn_stays_routine_and_yields_to_demand() {
    let (runtime, _rx) = session_runtime(Arc::new(crate::exchange::kalshi::Kalshi));
    let symbols = vec!["A".to_owned()];
    let mut membership = Membership::new(&symbols);
    membership.command(&BTreeSet::new(), 1).unwrap().unwrap();
    membership.command_written();
    membership
        .control(Some(1), &Reply::Subscribed(SID))
        .unwrap();
    membership.replace(&["A".into(), "B".into()]);
    assert!(!membership.bootstrapping());
    let mut live = session_with(&runtime, membership, vec!["A".into(), "B".into()]);
    assert!(runtime.pacer.admit());
    let other = runtime.pacer.urgent();
    tokio::time::advance(INTERVAL).await;
    assert!(!live.admit_command(), "routine must yield to a real demand");
    assert_eq!(runtime.pacer.urgent_waiters(), 1, "and publish none itself");
    drop(other);
    assert!(live.admit_command());
}

#[tokio::test(start_paused = true)]
async fn public_sessions_keep_regular_admission() {
    let (runtime, _rx) = session_runtime(Arc::new(Polymarket));
    let symbols = vec!["A".to_owned()];
    let membership = Membership::with_strategy(&symbols, MembershipStrategy::FreshSession);
    let mut live = session_with(&runtime, membership, symbols);
    assert!(runtime.pacer.admit());
    assert!(!live.admit_command());
    assert_eq!(runtime.pacer.urgent_waiters(), 0);
    tokio::time::advance(INTERVAL).await;
    assert!(live.admit_command());
}

// ---- One real Source while it waits for a slot ----------------------------

async fn next_text(ws: &mut Ws) -> Value {
    loop {
        match timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("no command from source")
            .unwrap()
            .unwrap()
        {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(_) | Message::Pong(_) => {}
            other => panic!("unexpected frame: {other:?}"),
        }
    }
}

async fn expect_input(rx: &mut mpsc::Receiver<Admitted>, mut wanted: impl FnMut(&Input) -> bool) {
    timeout(Duration::from_secs(5), async {
        loop {
            let admitted = rx.recv().await.expect("relay closed");
            if wanted(&admitted.input) {
                return;
            }
        }
    })
    .await
    .expect("expected relay input did not arrive");
}

async fn finish_close(ws: &mut Ws) {
    timeout(Duration::from_secs(5), async {
        while let Some(frame) = ws.next().await {
            match frame {
                Ok(Message::Close(_)) => {
                    let _ = ws.flush().await;
                    return;
                }
                Ok(_) => {}
                Err(_) => return,
            }
        }
    })
    .await
    .expect("source sent no Close");
}

struct WaitingBootstrap {
    ws: Ws,
    rx: mpsc::Receiver<Admitted>,
    pacer: Arc<Pacer>,
    assign: watch::Sender<Assignment>,
    control: watch::Sender<Control>,
    cancel: watch::Sender<bool>,
    source: JoinHandle<anyhow::Result<()>>,
}

/// A real Source, past Subscribe, with its follow-up Add locally due while the
/// shared pacer stays closed for about 30 seconds of reservations.
async fn waiting_bootstrap() -> WaitingBootstrap {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    let pacer = Arc::new(Pacer::new(INTERVAL));
    let (relay, rx) = relay::channel(Arc::new(Semaphore::new(1 << 20)));
    let runtime = Runtime {
        protocol: Arc::new(crate::exchange::kalshi::Kalshi),
        shard: 0,
        options: Arc::new(options(url, 1)),
        auth: None,
        pacer: pacer.clone(),
        relay,
    };
    let (assign, assigned) = watch::channel(assignment(1, vec!["A".into(), "B".into()]));
    let (control, controlled) = watch::channel(Control::default());
    let (cancel, stop) = watch::channel(false);
    let (progress, _) = watch::channel(Stats::default());
    let source = tokio::spawn(run(0, runtime, assigned, controlled, stop, progress));
    let (stream, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("source did not dial")
        .unwrap();
    let mut ws = accept_async(stream).await.unwrap();
    let subscribe = next_text(&mut ws).await;
    assert_eq!(subscribe["cmd"], "subscribe");
    for _ in 0..300 {
        pacer.reserve();
    }
    send(
        &mut ws,
        json!({"id":subscribe["id"], "type":"subscribed", "msg":{"sid":SID}}),
    )
    .await;
    send(&mut ws, snapshot(1, "A")).await;
    assert!(
        eventually(Duration::from_secs(3), || pacer.urgent_waiters() == 1).await,
        "bootstrap Add never published demand"
    );
    WaitingBootstrap {
        ws,
        rx,
        pacer,
        assign,
        control,
        cancel,
        source,
    }
}

impl WaitingBootstrap {
    async fn stops_after_close(mut self) {
        finish_close(&mut self.ws).await;
        timeout(Duration::from_secs(5), self.source)
            .await
            .expect("source did not join")
            .unwrap()
            .unwrap();
        assert_eq!(self.pacer.urgent_waiters(), 0);
    }
}

#[tokio::test]
async fn waiting_bootstrap_stays_live_and_pause_and_stop_withdraw_demand() {
    let mut h = waiting_bootstrap().await;
    // Socket frames and native control frames are still consumed.
    send(
        &mut h.ws,
        json!({"type":"orderbook_delta", "sid":SID, "seq":2,
            "sending_ts_ms":1700000000000i64,
            "msg":{"market_ticker":"A", "clock":102, "side":"yes",
                "price_dollars":"0.42", "delta_fp":"1"}}),
    )
    .await;
    expect_input(&mut h.rx, |i| {
        matches!(
            i,
            Input::Data {
                event: BookEvent::Update(_),
                ..
            }
        )
    })
    .await;
    h.ws.send(Message::Ping(b"live".to_vec().into()))
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            match h.ws.next().await.unwrap().unwrap() {
                Message::Pong(payload) if payload.as_ref() == b"live" => break,
                _ => {}
            }
        }
    })
    .await
    .expect("source stopped reading while waiting for a slot");
    // Owner controls are still consumed.
    h.control.send_modify(|c| c.fence = 1);
    expect_input(&mut h.rx, |i| matches!(i, Input::Fence { request: 1, .. })).await;
    assert_eq!(h.pacer.urgent_waiters(), 1);

    h.control.send_modify(|c| c.pause = Some(pause_request(1)));
    expect_input(&mut h.rx, |i| {
        matches!(i, Input::PauseStarted { nonce: 1, .. })
    })
    .await;
    assert!(
        eventually(Duration::from_secs(1), || h.pacer.urgent_waiters() == 0).await,
        "paused ingress retained demand"
    );
    h.control.send_modify(|c| {
        c.pause = None;
        c.pause_release = Some(PauseId {
            generation: 1,
            nonce: 1,
        });
    });
    expect_input(&mut h.rx, |i| {
        matches!(i, Input::PauseEnded { nonce: 1, .. })
    })
    .await;
    assert!(
        eventually(Duration::from_secs(1), || h.pacer.urgent_waiters() == 1).await,
        "resumed bootstrap did not publish demand again"
    );

    h.cancel.send(true).unwrap();
    assert!(
        eventually(Duration::from_secs(1), || h.pacer.urgent_waiters() == 0).await,
        "stop retained demand"
    );
    assert!(!h.source.is_finished(), "Close handshake is still ahead");
    h.stops_after_close().await;
}

#[tokio::test]
async fn waiting_bootstrap_withdraws_demand_on_backoff() {
    let mut h = waiting_bootstrap().await;
    send(&mut h.ws, json!({"type":"error", "msg":{"code":27}})).await;
    assert!(
        eventually(Duration::from_secs(1), || h.pacer.urgent_waiters() == 0).await,
        "exchange backoff retained demand"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h.pacer.urgent_waiters(), 0, "backoff republished demand");
    h.cancel.send(true).unwrap();
    h.stops_after_close().await;
}

#[tokio::test]
async fn waiting_bootstrap_withdraws_demand_when_its_work_disappears() {
    let h = waiting_bootstrap().await;
    h.assign.send_replace(assignment(2, vec!["A".into()]));
    assert!(
        eventually(Duration::from_secs(1), || h.pacer.urgent_waiters() == 0).await,
        "a settled roster retained demand"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h.pacer.urgent_waiters(), 0, "no real work remains");
    h.cancel.send(true).unwrap();
    h.stops_after_close().await;
}

#[tokio::test]
async fn waiting_bootstrap_withdraws_demand_on_an_empty_assignment() {
    let mut h = waiting_bootstrap().await;
    h.assign.send_replace(assignment(2, vec![]));
    assert!(
        eventually(Duration::from_secs(1), || h.pacer.urgent_waiters() == 0).await,
        "a retired session retained demand"
    );
    finish_close(&mut h.ws).await;
    h.cancel.send(true).unwrap();
    timeout(Duration::from_secs(5), h.source)
        .await
        .expect("source did not join")
        .unwrap()
        .unwrap();
    assert_eq!(h.pacer.urgent_waiters(), 0);
}

#[tokio::test]
async fn cancelling_a_waiting_bootstrap_withdraws_its_demand() {
    let h = waiting_bootstrap().await;
    h.source.abort();
    assert!(h.source.await.unwrap_err().is_cancelled());
    assert_eq!(h.pacer.urgent_waiters(), 0);
}

fn dialing_source(
    url: String,
    pacer: Arc<Pacer>,
) -> (
    watch::Sender<bool>,
    JoinHandle<anyhow::Result<()>>,
    mpsc::Receiver<Admitted>,
) {
    let (relay, rx) = relay::channel(Arc::new(Semaphore::new(1 << 20)));
    let runtime = Runtime {
        protocol: Arc::new(crate::exchange::kalshi::Kalshi),
        shard: 0,
        options: Arc::new(options(url, 250)),
        auth: None,
        pacer,
        relay,
    };
    let (assign, assigned) = watch::channel(assignment(1, vec!["A".into()]));
    let (control, controlled) = watch::channel(Control::default());
    let (cancel, stop) = watch::channel(false);
    let (progress, _) = watch::channel(Stats::default());
    let source = tokio::spawn(async move {
        // Owner-side senders stay alive for the whole Source lifetime.
        let _owner = (assign, control);
        run(0, runtime, assigned, controlled, stop, progress).await
    });
    (cancel, source, rx)
}

#[tokio::test]
async fn reconnect_pacing_wait_publishes_demand_and_stop_or_cancel_withdraw_it() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    let pacer = Arc::new(Pacer::new(INTERVAL));
    for _ in 0..300 {
        pacer.reserve();
    }
    let (cancel, source, _rx) = dialing_source(url.clone(), pacer.clone());
    assert!(eventually(Duration::from_secs(2), || pacer.urgent_waiters() == 1).await);
    cancel.send(true).unwrap();
    timeout(Duration::from_secs(1), source)
        .await
        .expect("stop must interrupt reconnect pacing")
        .unwrap()
        .unwrap();
    assert_eq!(pacer.urgent_waiters(), 0);

    let (_cancel, source, _rx) = dialing_source(url, pacer.clone());
    assert!(eventually(Duration::from_secs(2), || pacer.urgent_waiters() == 1).await);
    source.abort();
    assert!(source.await.unwrap_err().is_cancelled());
    assert_eq!(pacer.urgent_waiters(), 0);
}

#[tokio::test]
async fn reconnect_demand_ends_before_the_connection_attempt() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    let pacer = Arc::new(Pacer::new(INTERVAL));
    let (cancel, source, _rx) = dialing_source(url, pacer.clone());
    // TCP is accepted and the WebSocket handshake is left unanswered, so the
    // Source stays inside its connection attempt for this whole check.
    let (_stream, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("source did not dial")
        .unwrap();
    assert_eq!(pacer.urgent_waiters(), 0, "demand held across TCP/TLS");
    cancel.send(true).unwrap();
    timeout(Duration::from_secs(1), source)
        .await
        .expect("stop must interrupt the connection attempt")
        .unwrap()
        .unwrap();
}

// ---- One real Source waiting to dial --------------------------------------

struct WaitingToDial {
    listener: TcpListener,
    rx: mpsc::Receiver<Admitted>,
    pacer: Arc<Pacer>,
    assign: watch::Sender<Assignment>,
    _control: watch::Sender<Control>,
    cancel: watch::Sender<bool>,
    source: JoinHandle<anyhow::Result<()>>,
}

/// The real `run`, with the shared pacer closed for `closed_slots` intervals.
async fn waiting_to_dial(protocol: Arc<dyn ExchangeAdapter>, closed_slots: usize) -> WaitingToDial {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut runtime, rx) = session_runtime(protocol);
    Arc::make_mut(&mut runtime.options).ws_url =
        format!("ws://{}/", listener.local_addr().unwrap());
    let pacer = runtime.pacer.clone();
    for _ in 0..closed_slots {
        pacer.reserve();
    }
    let (assign, assigned) = watch::channel(assignment(1, vec!["A".into()]));
    let (_control, controlled) = watch::channel(Control::default());
    let (cancel, stop) = watch::channel(false);
    let (progress, _) = watch::channel(Stats::default());
    let source = tokio::spawn(run(0, runtime, assigned, controlled, stop, progress));
    WaitingToDial {
        listener,
        rx,
        pacer,
        assign,
        _control,
        cancel,
        source,
    }
}

fn kalshi() -> Arc<dyn ExchangeAdapter> {
    Arc::new(crate::exchange::kalshi::Kalshi)
}

/// The same ticker as a new market lifetime.
fn reincarnated(epoch: u64, incarnation: u64) -> Assignment {
    Assignment {
        epoch,
        symbols: vec!["A".into()],
        incarnations: BTreeMap::from([("A".into(), incarnation)]),
    }
}

/// Runs every ready task without advancing a paused clock. These waits have
/// no socket: the closed pacer keeps the Source from dialing.
async fn settle() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn coalesced_withdrawal_and_new_incarnation_keep_one_registration() {
    let h = waiting_to_dial(kalshi(), 300).await;
    settle().await;
    assert_eq!(h.pacer.urgent_waiters(), 1);
    // No await separates these: the watch can only expose the second value.
    h.assign.send_replace(assignment(2, vec![]));
    h.assign.send_replace(reincarnated(3, 2));
    settle().await;
    assert_eq!(h.pacer.urgent_waiters(), 1, "real work, one registration");
    assert!(!h.source.is_finished());
    h.cancel.send(true).unwrap();
    h.source.await.unwrap().unwrap();
    assert_eq!(h.pacer.urgent_waiters(), 0);
}

#[tokio::test(start_paused = true)]
async fn observed_withdrawals_park_unregistered_and_new_work_registers_once() {
    let h = waiting_to_dial(kalshi(), 300).await;
    settle().await;
    assert_eq!(h.pacer.urgent_waiters(), 1);
    for epoch in (2..10).step_by(2) {
        h.assign.send_replace(assignment(epoch, vec![]));
        settle().await;
        assert_eq!(h.pacer.urgent_waiters(), 0, "parked with no work");
        h.assign.send_replace(reincarnated(epoch + 1, epoch));
        settle().await;
        assert_eq!(h.pacer.urgent_waiters(), 1, "one replacement, no stacking");
    }
    h.assign.send_replace(assignment(11, vec![]));
    settle().await;
    assert_eq!(h.pacer.urgent_waiters(), 0);
    h.cancel.send(true).unwrap();
    h.source.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn assignment_churn_during_the_wait_keeps_exactly_one_registration() {
    let h = waiting_to_dial(kalshi(), 300).await;
    settle().await;
    for epoch in 2..8 {
        h.assign
            .send_replace(assignment(epoch, vec![format!("M-{epoch}")]));
        settle().await;
        assert_eq!(h.pacer.urgent_waiters(), 1, "epoch {epoch}");
    }
    h.cancel.send(true).unwrap();
    h.source.await.unwrap().unwrap();
    assert_eq!(h.pacer.urgent_waiters(), 0);
}

#[tokio::test(start_paused = true)]
async fn withdrawal_during_the_wait_spends_no_slot_on_either_admission_class() {
    let public: Arc<dyn ExchangeAdapter> = Arc::new(Polymarket);
    for (protocol, registered) in [(kalshi(), 1), (public, 0)] {
        let h = waiting_to_dial(protocol, 3).await;
        settle().await;
        assert_eq!(h.pacer.urgent_waiters(), registered);
        h.assign.send_replace(assignment(2, vec![]));
        settle().await;
        assert_eq!(h.pacer.urgent_waiters(), 0);
        tokio::time::advance(INTERVAL * 4).await;
        settle().await;
        assert!(h.pacer.admit(), "the opening was spent on no work");
        h.cancel.send(true).unwrap();
        h.source.await.unwrap().unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn closing_the_assignment_owner_during_the_wait_fails_without_spinning() {
    for withdrawn_first in [false, true] {
        let h = waiting_to_dial(kalshi(), 300).await;
        settle().await;
        assert_eq!(h.pacer.urgent_waiters(), 1);
        if withdrawn_first {
            h.assign.send_replace(assignment(2, vec![]));
        }
        drop(h.assign);
        settle().await;
        assert!(h.source.is_finished(), "a closed owner must end the wait");
        let error = h.source.await.unwrap().unwrap_err();
        assert!(
            format!("{error:#}").contains("assignment owner stopped"),
            "{error:#}"
        );
        assert_eq!(h.pacer.urgent_waiters(), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn owner_already_closed_on_entry_spends_no_open_slot() {
    let h = waiting_to_dial(kalshi(), 0).await;
    drop(h.assign);
    settle().await;
    assert!(
        h.source.is_finished(),
        "a dead owner has nothing to dial for"
    );
    let error = h.source.await.unwrap().unwrap_err();
    assert!(
        format!("{error:#}").contains("assignment owner stopped"),
        "{error:#}"
    );
    assert!(h.pacer.admit(), "the open slot was left unspent");
}

#[tokio::test(start_paused = true)]
async fn stop_wins_over_a_simultaneous_owner_closure() {
    // Repeated because an unbiased select would pass by chance half the time.
    for _ in 0..16 {
        let h = waiting_to_dial(kalshi(), 300).await;
        settle().await;
        h.cancel.send(true).unwrap();
        drop(h.assign);
        h.source.await.unwrap().unwrap();
        assert_eq!(h.pacer.urgent_waiters(), 0);
    }
}

#[tokio::test]
async fn replacement_work_dials_once_and_serves_the_new_incarnation() {
    for observed in [false, true] {
        let mut h = waiting_to_dial(kalshi(), 10).await;
        assert!(eventually(Duration::from_secs(2), || h.pacer.urgent_waiters() == 1).await);
        h.assign.send_replace(assignment(2, vec![]));
        if observed {
            assert!(eventually(Duration::from_millis(300), || h.pacer.urgent_waiters() == 0).await);
        }
        h.assign.send_replace(reincarnated(3, 2));
        let (stream, _) = timeout(Duration::from_secs(5), h.listener.accept())
            .await
            .expect("replacement work never dialed")
            .unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        let subscribe = next_text(&mut ws).await;
        assert_eq!(subscribe["cmd"], "subscribe");
        assert_eq!(subscribe["params"]["market_tickers"], json!(["A"]));
        send(
            &mut ws,
            json!({"id":subscribe["id"], "type":"subscribed", "msg":{"sid":SID}}),
        )
        .await;
        send(&mut ws, snapshot(1, "A")).await;
        expect_input(&mut h.rx, |i| {
            matches!(i, Input::Data { incarnation: 2, .. })
        })
        .await;
        assert_eq!(h.pacer.urgent_waiters(), 0, "admission ended the wait");
        assert!(
            timeout(Duration::from_millis(50), h.listener.accept())
                .await
                .is_err(),
            "one admission, one dial"
        );
        h.cancel.send(true).unwrap();
        finish_close(&mut ws).await;
        timeout(Duration::from_secs(5), h.source)
            .await
            .expect("source did not join")
            .unwrap()
            .unwrap();
    }
}

/// A publication cannot recall IO already in flight: the socket still opens.
/// What it can prevent is any membership command on that dead session.
#[tokio::test]
async fn withdrawal_during_a_started_dial_cannot_recall_it_but_sends_no_command() {
    let mut h = waiting_to_dial(kalshi(), 0).await;
    let (stream, _) = timeout(Duration::from_secs(5), h.listener.accept())
        .await
        .expect("source did not dial")
        .unwrap();
    assert_eq!(h.pacer.urgent_waiters(), 0, "ended before TCP/TLS");
    h.assign.send_replace(assignment(2, vec![]));
    let mut ws = accept_async(stream).await.unwrap();
    let first = timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("source neither commanded nor closed")
        .unwrap()
        .unwrap();
    assert!(matches!(first, Message::Close(_)), "{first:?}");
    let _ = ws.flush().await;
    expect_input(&mut h.rx, |i| {
        matches!(i, Input::Invalid { symbols: None, .. })
    })
    .await;
    h.cancel.send(true).unwrap();
    timeout(Duration::from_secs(5), h.source)
        .await
        .expect("source did not join")
        .unwrap()
        .unwrap();
    assert_eq!(h.pacer.urgent_waiters(), 0);
}

// ---- Universe-scale burst on one shared Pacer ------------------------------

#[derive(Default)]
struct Wire {
    commands: Vec<Instant>,
    large_commands: Vec<Instant>,
    warm_deletes: usize,
    warm_adds: usize,
    last_warm_command: Option<Instant>,
    closes: usize,
}

struct Exchange {
    wire: Mutex<Wire>,
    large_acked: watch::Sender<Option<Instant>>,
}

async fn serve(stream: TcpStream, exchange: Arc<Exchange>) {
    let mut ws = accept_async(stream).await.unwrap();
    let mut seq = 0u64;
    let mut large = false;
    while let Some(frame) = ws.next().await {
        let text = match frame {
            Ok(Message::Text(text)) => text,
            Ok(Message::Close(_)) => {
                let _ = ws.flush().await;
                exchange.wire.lock().unwrap().closes += 1;
                return;
            }
            // tungstenite queues the Pong for a Ping and flushes it on read.
            Ok(_) => continue,
            Err(_) => return,
        };
        let request: Value = serde_json::from_str(&text).unwrap();
        let tickers: Vec<String> = request["params"]["market_tickers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m.as_str().unwrap().to_owned())
            .collect();
        let now = Instant::now();
        large |= tickers.first().is_some_and(|t| t.starts_with("BOOT-"));
        let action = request["params"]["action"].as_str().unwrap_or("subscribe");
        {
            let mut wire = exchange.wire.lock().unwrap();
            wire.commands.push(now);
            if large {
                wire.large_commands.push(now);
            } else if action != "subscribe" {
                match action {
                    "delete_markets" => wire.warm_deletes += 1,
                    "add_markets" => wire.warm_adds += 1,
                    other => panic!("unexpected warm action {other}"),
                }
                wire.last_warm_command = Some(now);
            }
        }
        if request["cmd"] == "subscribe" {
            send(
                &mut ws,
                json!({"id":request["id"], "type":"subscribed", "msg":{"sid":SID}}),
            )
            .await;
            if large {
                exchange.large_acked.send_replace(Some(Instant::now()));
            }
        } else {
            assert_eq!(request["cmd"], "update_subscription", "{request}");
            seq += 1;
            send(
                &mut ws,
                json!({"id":request["id"], "type":"ok", "sid":SID, "seq":seq, "msg":{}}),
            )
            .await;
            if action == "delete_markets" {
                continue;
            }
        }
        for market in &tickers {
            seq += 1;
            send(&mut ws, snapshot(seq, market)).await;
        }
    }
}

async fn accept_all(listener: TcpListener, exchange: Arc<Exchange>) {
    let mut servers = JoinSet::new();
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        servers.spawn(serve(stream, exchange.clone()));
    }
}

#[derive(Default)]
struct Delivered {
    symbols: HashSet<String>,
    invalid: Vec<String>,
    large: usize,
    large_ready: Option<Instant>,
    warm_new: usize,
    warm_ready: Option<Instant>,
}

/// Plays the Owner side of the relay: every snapshot it records was really
/// admitted by a Source.
async fn drain(mut rx: mpsc::Receiver<Admitted>, delivered: Arc<Mutex<Delivered>>) {
    while let Some(admitted) = rx.recv().await {
        match admitted.input {
            Input::Data {
                event: BookEvent::Snapshot(snapshot),
                ..
            } => {
                let mut d = delivered.lock().unwrap();
                if !d.symbols.insert(snapshot.symbol.clone()) {
                    continue;
                }
                if snapshot.symbol.starts_with("BOOT-") {
                    d.large += 1;
                    if d.large == LARGE {
                        d.large_ready = Some(Instant::now());
                    }
                } else if snapshot.symbol.starts_with("NEW-") {
                    d.warm_new += 1;
                    if d.warm_new == WARM {
                        d.warm_ready = Some(Instant::now());
                    }
                }
            }
            Input::Invalid { reason, .. } => delivered.lock().unwrap().invalid.push(reason),
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_sources_on_one_pacer_finish_bootstrap_inside_a_warm_universe_burst() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    let pacer = Arc::new(Pacer::new(INTERVAL));
    let (relay, rx) = relay::channel(Arc::new(Semaphore::new(1 << 28)));
    let exchange = Arc::new(Exchange {
        wire: Mutex::default(),
        large_acked: watch::channel(None).0,
    });
    let delivered = Arc::new(Mutex::new(Delivered::default()));
    let accepting = tokio::spawn(accept_all(listener, exchange.clone()));
    let draining = tokio::spawn(drain(rx, delivered.clone()));
    let options = Arc::new(options(url, 250));
    let runtime = |shard| Runtime {
        protocol: Arc::new(crate::exchange::kalshi::Kalshi),
        shard,
        options: options.clone(),
        auth: None,
        pacer: pacer.clone(),
        relay: relay.clone(),
    };
    let (_control, controlled) = watch::channel(Control::default());
    let (cancel, stop) = watch::channel(false);

    let mut sources = Vec::with_capacity(WARM + 1);
    let mut warm = Vec::with_capacity(WARM);
    let mut warm_progress = Vec::with_capacity(WARM);
    for i in 0..WARM {
        let (assign, assigned) = watch::channel(assignment(
            1,
            vec![format!("KEEP-{i:03}"), format!("OLD-{i:03}")],
        ));
        let (progress, observed) = watch::channel(Stats::default());
        sources.push(tokio::spawn(run(
            0,
            runtime(i),
            assigned,
            controlled.clone(),
            stop.clone(),
            progress,
        )));
        warm.push(assign);
        warm_progress.push(observed);
    }
    assert!(
        eventually(Duration::from_secs(60), || {
            delivered.lock().unwrap().symbols.len() == 2 * WARM
        })
        .await,
        "warm sessions did not establish; delivered {}",
        delivered.lock().unwrap().symbols.len()
    );

    let symbols: Vec<_> = (0..LARGE).map(|i| format!("BOOT-{i:04}")).collect();
    let (_large_assign, large_assigned) = watch::channel(assignment(1, symbols));
    let (large_progress, large_observed) = watch::channel(Stats::default());
    sources.push(tokio::spawn(run(
        1,
        runtime(WARM),
        large_assigned,
        controlled.clone(),
        stop.clone(),
        large_progress,
    )));
    let burst_started = timeout(Duration::from_secs(10), async {
        let mut acked = exchange.large_acked.subscribe();
        *acked
            .wait_for(Option::is_some)
            .await
            .unwrap()
            .as_ref()
            .unwrap()
    })
    .await
    .expect("large session was never subscribed");
    // Equivalent of registry epoch 7: every warm session has work to do while
    // the rest of the large roster is still outstanding.
    for (i, assign) in warm.iter().enumerate() {
        assign.send_replace(assignment(
            2,
            vec![format!("KEEP-{i:03}"), format!("NEW-{i:03}")],
        ));
    }

    let large_ready = timeout(Duration::from_secs(20), async {
        loop {
            if let Some(at) = delivered.lock().unwrap().large_ready {
                return at;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        // Released before panicking: the relay drain still needs this lock.
        let (large, large_commands, warm_commands) = {
            let d = delivered.lock().unwrap();
            let w = exchange.wire.lock().unwrap();
            (d.large, w.large_commands.len(), w.warm_deletes + w.warm_adds)
        };
        panic!(
            "bootstrap not ready 20s after its Subscribe ACK: large_symbols={large}/{LARGE}, large_commands={large_commands}, warm_commands={warm_commands}"
        )
    });
    assert!(
        eventually(Duration::from_secs(45), || {
            let d = delivered.lock().unwrap();
            let w = exchange.wire.lock().unwrap();
            d.warm_ready.is_some() && w.warm_deletes == WARM && w.warm_adds == WARM
        })
        .await,
        "warm churn did not finish: delivered_new={}",
        delivered.lock().unwrap().warm_new
    );

    cancel.send(true).unwrap();
    timeout(Duration::from_secs(30), async {
        for source in sources {
            source.await.unwrap().unwrap();
        }
    })
    .await
    .expect("sources did not stop and join");
    assert_eq!(pacer.urgent_waiters(), 0);
    accepting.abort();
    let _ = accepting.await;
    drop(relay);
    timeout(Duration::from_secs(5), draining)
        .await
        .expect("relay did not drain after every Source joined")
        .unwrap();

    let d = delivered.lock().unwrap();
    let mut w = exchange.wire.lock().unwrap();
    assert_eq!(d.invalid, Vec::<String>::new());
    assert_eq!(w.closes, WARM + 1, "every session finished Close");
    let large_finished = large_ready - burst_started;
    let last_warm = w.last_warm_command.unwrap();
    let warm_finished = last_warm - burst_started;
    eprintln!(
        "source admission: large_ready_after={large_finished:?}, last_warm_command_after={warm_finished:?}, large_commands={}, warm_commands={}",
        w.large_commands.len(),
        w.warm_deletes + w.warm_adds
    );
    assert!(large_finished <= Duration::from_secs(20));
    assert!(warm_finished <= Duration::from_secs(45));
    assert_eq!(w.large_commands.len(), 9, "2250 books / 250 per command");
    assert_eq!(w.warm_deletes + w.warm_adds, 2 * WARM);

    // The repair only reorders: global and local spacing still hold on the
    // wire. A 10-slot window tolerates scheduling jitter at its two ends.
    w.commands.sort();
    for window in w.commands.windows(11) {
        let spread = window[10] - window[0];
        assert!(
            spread >= Duration::from_millis(900),
            "global pacing exceeded: 10 slots in {spread:?}"
        );
    }
    for pair in w.large_commands.windows(2) {
        let gap = pair[1] - pair[0];
        assert!(
            gap >= Duration::from_millis(400),
            "local floor broken: {gap:?}"
        );
    }

    let large = large_observed.borrow();
    assert_eq!(
        (
            large.connects,
            large.commands,
            large.gaps,
            large.ack_timeouts
        ),
        (1, 9, 0, 0)
    );
    assert_eq!(large.unacknowledged_closes, 0);
    for (i, observed) in warm_progress.iter().enumerate() {
        let stats = observed.borrow();
        assert_eq!(
            (
                stats.connects,
                stats.commands,
                stats.gaps,
                stats.ack_timeouts
            ),
            (1, 3, 0, 0),
            "warm session {i}"
        );
    }
}

#[tokio::test]
async fn withdrawing_assignment_while_waiting_to_dial_releases_urgent_demand() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut runtime, _rx) = session_runtime(Arc::new(crate::exchange::kalshi::Kalshi));
    Arc::make_mut(&mut runtime.options).ws_url =
        format!("ws://{}/", listener.local_addr().unwrap());
    let pacer = runtime.pacer.clone();
    for _ in 0..300 {
        pacer.reserve();
    }
    let (assign, assigned) = watch::channel(assignment(1, vec!["A".into()]));
    let (_control, controlled) = watch::channel(Control::default());
    let (cancel, stop) = watch::channel(false);
    let (progress, _observed) = watch::channel(Stats::default());
    let source = tokio::spawn(run(0, runtime, assigned, controlled, stop, progress));
    assert!(eventually(Duration::from_secs(2), || pacer.urgent_waiters() == 1).await);
    assign.send_replace(assignment(2, vec![]));
    let withdrawn = eventually(Duration::from_millis(300), || pacer.urgent_waiters() == 0).await;
    // Cleanup precedes the assertion, including on the red path.
    cancel.send(true).unwrap();
    timeout(Duration::from_secs(1), source)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        withdrawn,
        "latest assignment is empty but the reconnect pacing wait still advertises urgent demand"
    );
    assert!(
        timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}
