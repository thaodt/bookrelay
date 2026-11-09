//! One transport owner: membership, integrity, heartbeat and reconnect. It sends
//! normalized events; the logical shard owns the independent replica books.
use crate::{
    collector::{Options, Pacer, UrgentDemand, stopped},
    exchange::{
        Control as Reply, ExchangeAdapter, MembershipStrategy, OrderingStrategy, Rejection,
        SequenceTracker,
    },
    membership::{Kind, Membership},
    model::BookEvent,
    registry::Assignment,
    relay::{self, Control, Input, PauseEnd, PauseId, PauseRequest, Resubscribe, Source},
    transport::{self, Socket},
};
use anyhow::{Context, ensure};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::watch,
    time::{Instant, MissedTickBehavior},
};
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone, Default, Serialize)]
pub(crate) struct Stats {
    pub connects: u64,
    pub frames: u64,
    pub bytes: u64,
    pub gaps: u64,
    pub resync: u64,
    pub commands: u64,
    pub quarantined: u64,
    pub retired: u64,
    pub restarts: u64,
    pub unacknowledged_closes: u64,
    pub connected: bool,
    pub resubscriptions: u64,
    pub resubscribe_completed: u64,
    pub resubscribe_unready_ms_max: u128,
    pub ack_timeouts: u64,
    pub pings_sent: u64,
    pub pongs_received: u64,
    pub frame_age_ms_max: u128,
    pub pong_age_ms_max: u128,
    pub ack_age_ms_max: u128,
    pub initializations: u64,
    pub unavailable_assets: usize,
    pub handoff_pauses: u64,
    pub pause_expirations: u64,
    pub pause_us_max: u128,
    /// Streams retired at a requested cut (already dropped, no Close sent).
    /// Each is also an unacknowledged close; neither fails the run.
    pub stop_retirements: u64,
    /// Accepted non-destructive covering-snapshot requests.
    pub probes: u64,
}
#[derive(Clone)]
pub(crate) struct Shared {
    pub protocol: Arc<dyn ExchangeAdapter>,
    pub options: Arc<Options>,
    pub auth: Option<Arc<dyn transport::RequestAuthenticator>>,
    pub pacer: Arc<Pacer>,
    pub recovery_pacer: Arc<Pacer>,
}
#[derive(Clone)]
pub(crate) struct Runtime {
    pub protocol: Arc<dyn ExchangeAdapter>,
    pub shard: usize,
    pub options: Arc<Options>,
    pub auth: Option<Arc<dyn transport::RequestAuthenticator>>,
    pub pacer: Arc<Pacer>,
    pub relay: relay::Sender,
}

#[derive(Debug)]
struct Rejected(u64, bool);
impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "exchange rejected command {}", self.0)
    }
}
impl std::error::Error for Rejected {}
#[derive(Debug)]
struct InvalidProtocol;
impl std::fmt::Display for InvalidProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "public protocol normalization failed; transport retry cannot repair this frame",
        )
    }
}
impl std::error::Error for InvalidProtocol {}
/// The byte stream cannot be used again, including for a graceful Close. This
/// covers both a cancelled write and inability to preserve pause-marker order.
#[derive(Debug)]
struct RetireTransport(String);
impl std::fmt::Display for RetireTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "retire websocket transport: {}", self.0)
    }
}
impl std::error::Error for RetireTransport {}

async fn write(socket: &mut Socket, message: Message, limit: Duration) -> anyhow::Result<()> {
    // Never select a control/pause against an in-flight Sink write. On either
    // timeout or write error, drop the stream rather than sending another frame.
    match tokio::time::timeout(limit, socket.send(message)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            Err(anyhow::Error::new(error).context(RetireTransport("write failed".into())))
        }
        Err(_) => Err(RetireTransport(format!("write timed out after {limit:?}")).into()),
    }
}
fn retryable(e: &anyhow::Error) -> bool {
    if e.is::<InvalidProtocol>()
        || matches!(
            e.downcast_ref(),
            Some(tokio_tungstenite::tungstenite::Error::Capacity(_))
        )
    {
        return false;
    }
    if let Some(rejected) = e.downcast_ref::<Rejected>() {
        return rejected.1;
    }
    if let Some(tokio_tungstenite::tungstenite::Error::Http(r)) = e.downcast_ref() {
        return r.status().as_u16() == 429 || r.status().is_server_error();
    }
    true
}

pub(crate) async fn run(
    replica: usize,
    runtime: Runtime,
    mut assignment: watch::Receiver<Assignment>,
    mut control: watch::Receiver<Control>,
    mut stop: watch::Receiver<bool>,
    progress: watch::Sender<Stats>,
) -> anyhow::Result<()> {
    let shard = runtime.shard;
    let mut stats = Stats::default();
    let mut generation = 0u64;
    let mut failures = 0;
    'reconnect: loop {
        if *stop.borrow() {
            return Ok(());
        }
        if assignment.borrow().symbols.is_empty() {
            tokio::select! {_=stopped(&mut stop)=>return Ok(()),r=assignment.changed()=>{r?;continue;}}
        }
        {
            // A native reconnect is the start of a bootstrap. The registration
            // is scoped to this block: it ends before TCP/TLS, Close or backoff,
            // and before a later attempt can register a replacement.
            let demand = (runtime.protocol.contract().membership
                == MembershipStrategy::Acknowledged)
                .then(|| runtime.pacer.urgent());
            loop {
                // The latest state decides, immediately before a slot is spent
                // and with no await until the dial starts. A later publication
                // cannot recall that IO: `session` reads the assignment again.
                assignment
                    .has_changed()
                    .context("assignment owner stopped")?;
                if assignment.borrow().symbols.is_empty() {
                    continue 'reconnect;
                }
                if demand
                    .as_ref()
                    .map_or_else(|| runtime.pacer.admit(), |demand| demand.admit())
                {
                    break;
                }
                // `changed` observes the latest unseen version; publications
                // can coalesce. A closed owner fails once that version is seen.
                // This loop and `session` both reread the latest assignment.
                tokio::select! {
                    biased;
                    _ = stopped(&mut stop) => return Ok(()),
                    changed = assignment.changed() => changed.context("assignment owner stopped")?,
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                }
            }
        }
        let admission = control.borrow().restart;
        let connected = tokio::select! {_=stopped(&mut stop)=>return Ok(()),r=transport::connect_with_authenticator(&runtime.options.ws_url,runtime.auth.as_deref(),&runtime.options.connect)=>r};
        let result: anyhow::Result<()> = match connected {
            Ok(mut connection) => {
                generation = generation
                    .checked_add(1)
                    .context("source generation overflow")?;
                let source = Source {
                    replica,
                    generation,
                };
                stats.connects += 1;
                stats.connected = true;
                progress.send_replace(stats.clone());
                // Nothing from this generation precedes Begin and it holds no
                // pause, so momentary byte-budget pressure is awaited (bounded).
                runtime
                    .relay
                    .send_waiting(Input::Begin { source, admission })
                    .await?;
                eprintln!(
                    "shard {shard} replica {replica} generation {generation} connected; peer={}, local_port={}",
                    connection.peer,
                    connection.local.port()
                );
                let before = stats.initializations;
                let outcome = session(
                    &runtime,
                    &mut connection.socket,
                    source,
                    admission,
                    &mut assignment,
                    &mut control,
                    &mut stop,
                    &mut stats,
                    &progress,
                )
                .await;
                stats.connected = false;
                progress.send_replace(stats.clone());
                let immediate_retirement = outcome
                    .as_ref()
                    .is_err_and(|error| error.downcast_ref::<RetireTransport>().is_some());
                if immediate_retirement {
                    // In particular, do not spend the 15s ordinary relay wait
                    // or a 10s Close handshake while an end marker is blocked.
                    drop(connection);
                    if *stop.borrow() {
                        // Retired at the requested cut (typically its ordered
                        // end marker met a full relay). Data admitted before
                        // the cut stands and no stream remains; this is a
                        // counted unacknowledged close, not a run failure.
                        stats.stop_retirements += 1;
                        stats.unacknowledged_closes += 1;
                        progress.send_replace(stats.clone());
                        eprintln!(
                            "shard {shard} replica {replica} generation {generation} retired at stop without Close: {:#}",
                            outcome.as_ref().unwrap_err()
                        );
                        return Ok(());
                    }
                    // Retired while live: the stream no longer exists, so no
                    // paused ingress, lease or unread tail is held open by
                    // waiting. Owner must still learn this generation ended,
                    // even if stop arrives meanwhile: otherwise a frozen
                    // publisher is silently counted ready at the cut.
                    runtime
                        .relay
                        .send_waiting(Input::Invalid {
                            source,
                            symbols: None,
                            reason: outcome.as_ref().unwrap_err().to_string(),
                        })
                        .await
                        .context("retired transport could not report its fault")?;
                } else {
                    // This marker follows every admitted event from this
                    // generation. A session that ended before the cut owes it
                    // even if stop arrives during this bounded wait.
                    if !*stop.borrow() {
                        runtime
                            .relay
                            .send_waiting(Input::Invalid {
                                source,
                                symbols: None,
                                reason: outcome
                                    .as_ref()
                                    .err()
                                    .map(|e| e.to_string())
                                    .unwrap_or_else(|| "source closed".into()),
                            })
                            .await?;
                    }
                    // Publication from this generation is already cut. A peer that
                    // never echoes Close cannot change accepted data: the transport
                    // is dropped and the miss is counted, not turned into a failed
                    // run that would also delay the final flush.
                    if let Err(e) = crate::collector::close(&mut connection.socket).await {
                        stats.unacknowledged_closes += 1;
                        progress.send_replace(stats.clone());
                        eprintln!(
                            "shard {shard} replica {replica} close not acknowledged: {e:#}; dropping transport"
                        );
                    }
                }
                if stats.initializations > before {
                    failures = 0;
                }
                match outcome {
                    Ok(()) => {
                        if *stop.borrow() {
                            return Ok(());
                        }
                        stats.restarts += 1;
                        continue;
                    }
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        };
        let error = result.unwrap_err();
        if !retryable(&error) {
            return Err(error);
        }
        failures += 1;
        ensure!(
            failures <= 8,
            "shard {shard} replica {replica} reconnect budget exhausted: {error:#}"
        );
        let delay =
            Duration::from_millis((500u64 << failures.min(5)) + rand::random::<u8>() as u64);
        eprintln!(
            "shard {shard} replica {replica} unavailable: {error:#}; reconnect in {}ms",
            delay.as_millis()
        );
        tokio::select! {_=stopped(&mut stop)=>return Ok(()),_=tokio::time::sleep(delay)=>{}}
    }
}

struct Session<'a> {
    runtime: &'a Runtime,
    source: Source,
    membership: Membership,
    assignment: Assignment,
    tracker: SequenceTracker,
    initialized: BTreeSet<String>,
    unavailable: BTreeSet<String>,
    dirty: BTreeSet<String>,
    invalid_since: HashMap<String, Instant>,
    attempts: HashMap<String, u32>,
    observed_refresh: BTreeMap<String, crate::relay::Refresh>,
    observed_resubscribe: BTreeMap<String, Resubscribe>,
    observed_probe: BTreeMap<String, crate::relay::Probe>,
    resubscribe_started: BTreeMap<String, Recovery>,
    restart: u64,
    fence: u64,
    next_command: Instant,
    /// Held only while `urgent_eligible`, across denied health polls.
    urgent: Option<UrgentDemand<'a>>,
    last_frame: Instant,
    last_pong: Instant,
    pause: Option<IngressPause>,
    pause_high_water: u64,
    release_high_water: u64,
    pause_not_before: Instant,
}

struct IngressPause {
    nonce: u64,
    started: Instant,
    until: Instant,
}

const MAX_PAUSE: Duration = Duration::from_secs(5);
const HEALTH_MARGIN: Duration = Duration::from_secs(1);

struct Recovery {
    incarnation: u64,
    started: Instant,
}

fn stale_memberships(old: &Assignment, new: &Assignment, membership: &Membership) -> Vec<String> {
    let mut stale = Vec::new();
    for (symbol, version) in &new.incarnations {
        if old.incarnations.get(symbol) != Some(version) && membership.retains(symbol) {
            stale.push(symbol.clone());
        }
    }
    stale
}
impl<'a> Session<'a> {
    /// This Session could spend a global slot on a real bootstrap command now.
    fn urgent_eligible(&self) -> bool {
        self.pause.is_none()
            && Instant::now() >= self.next_command
            && self.membership.bootstrapping()
            && self.membership.needs_command(&self.dirty)
    }

    /// Withdraw demand the moment any event ends eligibility, rather than
    /// leaving it published until a health tick that a pause would disable.
    fn release_stale_urgent(&mut self) {
        if self.urgent.is_some() && !self.urgent_eligible() {
            self.urgent = None;
        }
    }

    /// One attempt per health tick, never awaited: the Session keeps reading
    /// frames, controls and stop between denied polls.
    fn admit_command(&mut self) -> bool {
        if !self.urgent_eligible() {
            self.urgent = None;
            return Instant::now() >= self.next_command
                && self.membership.needs_command(&self.dirty)
                && self.runtime.pacer.admit();
        }
        let runtime: &'a Runtime = self.runtime;
        let admitted = self
            .urgent
            .get_or_insert_with(|| runtime.pacer.urgent())
            .admit();
        if admitted {
            self.urgent = None;
        }
        admitted
    }

    fn pause_marker(&self, marker: Input) -> anyhow::Result<()> {
        self.pause_marker_with_elapsed(marker, None)
    }

    fn pause_marker_with_elapsed(
        &self,
        marker: Input,
        elapsed_us: Option<u128>,
    ) -> anyhow::Result<()> {
        let detail = match &marker {
            Input::PauseStarted { nonce, until, .. } => Some((
                *nonce,
                "started",
                None,
                until.saturating_duration_since(Instant::now()).as_micros(),
            )),
            Input::PauseEnded { nonce, reason, .. } => Some((*nonce, "ended", Some(*reason), 0)),
            _ => None,
        };
        let admitted = self.runtime.relay.try_send(marker);
        if let Some((nonce, phase, reason, remaining_us)) = detail {
            eprintln!(
                "shard {} source {:?} pause {phase}: nonce={nonce} reason={reason:?} remaining_us={remaining_us} elapsed_us={elapsed_us:?} admitted={}",
                self.runtime.shard,
                self.source,
                admitted.is_ok()
            );
        }
        admitted.map_err(|error| {
            RetireTransport(format!("pause marker admission failed: {error:#}")).into()
        })
    }

    fn end_pause(&mut self, reason: PauseEnd, stats: &mut Stats) -> anyhow::Result<()> {
        let Some(pause) = self.pause.take() else {
            return Ok(());
        };
        let elapsed_us = pause.started.elapsed().as_micros();
        stats.pause_us_max = stats.pause_us_max.max(elapsed_us);
        if reason == PauseEnd::Expired {
            stats.pause_expirations += 1;
        }
        self.pause_not_before = Instant::now() + self.runtime.options.health.cooldown;
        self.pause_marker_with_elapsed(
            Input::PauseEnded {
                source: self.source,
                nonce: pause.nonce,
                reason,
            },
            Some(elapsed_us),
        )
    }

    fn pause_controls(
        &mut self,
        request: Option<PauseRequest>,
        release: Option<PauseId>,
        destructive: bool,
        stats: &mut Stats,
    ) -> anyhow::Result<()> {
        if let Some(release) = release.filter(|release| {
            release.generation == self.source.generation && release.nonce > self.release_high_water
        }) {
            self.release_high_water = release.nonce;
            let consumed = release.nonce <= self.pause_high_water;
            self.pause_high_water = self.pause_high_water.max(release.nonce);
            if self
                .pause
                .as_ref()
                .is_some_and(|pause| pause.nonce == release.nonce)
            {
                self.end_pause(PauseEnd::Released, stats)?;
            } else if !consumed {
                // The request coalesced away before Source saw it; Owner still
                // holds that reservation and needs this one terminal marker.
                self.pause_marker(Input::PauseEnded {
                    source: self.source,
                    nonce: release.nonce,
                    reason: PauseEnd::Released,
                })?;
            }
            // Otherwise this generation already emitted the nonce's single
            // ordered end (Started..Ended, Busy or Expired). Owner allows one
            // outstanding pause per shard and resolves it only from that marker
            // or a generation retirement, so a second end adds only FIFO
            // pressure. After expiry the stream is resumed, so that pressure
            // previously retired a healthy transport (2026-11-08 universe).
        }
        if destructive {
            self.end_pause(PauseEnd::Superseded, stats)?;
        }
        let Some(request) = request.filter(|request| {
            request.generation == self.source.generation && request.nonce > self.pause_high_water
        }) else {
            return Ok(());
        };
        // This high-water mark outlives expiry, cancellation and Busy. A watch
        // still holding the same token can never reacquire or extend its lease.
        self.pause_high_water = request.nonce;
        let was_paused = self.pause.is_some();
        if was_paused {
            self.end_pause(PauseEnd::Superseded, stats)?;
        }
        let now = Instant::now();
        let duration = request.max_duration.min(MAX_PAUSE);
        let reason = if now >= request.acquire_until {
            Some(PauseEnd::Expired)
        } else if destructive
            || was_paused
            || duration.is_zero()
            || now < self.pause_not_before
            || self.membership.pending.is_some()
            || self.membership.sid.is_none()
            || self.last_frame.elapsed() + duration + HEALTH_MARGIN >= Duration::from_secs(30)
            || self.last_pong.elapsed() + duration + HEALTH_MARGIN >= Duration::from_secs(30)
            || self
                .invalid_since
                .values()
                .any(|since| since.elapsed() + duration + HEALTH_MARGIN >= Duration::from_secs(90))
        {
            Some(PauseEnd::Busy)
        } else {
            None
        };
        if let Some(reason) = reason {
            return self.pause_marker(Input::PauseEnded {
                source: self.source,
                nonce: request.nonce,
                reason,
            });
        }
        let until = now + duration;
        self.pause_marker(Input::PauseStarted {
            source: self.source,
            nonce: request.nonce,
            until,
        })?;
        self.pause = Some(IngressPause {
            nonce: request.nonce,
            started: now,
            until,
        });
        stats.handoff_pauses += 1;
        Ok(())
    }

    fn finish_initialization(&mut self, symbol: &str, stats: &mut Stats) {
        if !self.initialized.contains(symbol)
            || !self.membership.members().contains(symbol)
            || !self.membership.accepts(symbol)
        {
            return;
        }
        if self.invalid_since.remove(symbol).is_some() {
            stats.initializations += 1;
        }
        self.unavailable.remove(symbol);
        stats.unavailable_assets = self.unavailable.len();
        self.attempts.remove(symbol);
        if let Some(recovery) = self.resubscribe_started.remove(symbol) {
            stats.resubscribe_completed += 1;
            stats.resubscribe_unready_ms_max = stats
                .resubscribe_unready_ms_max
                .max(recovery.started.elapsed().as_millis());
        }
    }
    async fn invalidate(
        &mut self,
        symbols: Option<Vec<String>>,
        reason: &str,
    ) -> anyhow::Result<()> {
        let affected = symbols.as_deref().unwrap_or(&self.assignment.symbols);
        for symbol in affected {
            self.initialized.remove(symbol);
            self.dirty.insert(symbol.clone());
            self.invalid_since
                .entry(symbol.clone())
                .or_insert_with(Instant::now);
        }
        self.runtime
            .relay
            .send(Input::Invalid {
                source: self.source,
                symbols,
                reason: reason.into(),
            })
            .await
    }
    async fn sync(&mut self, current: Assignment, stats: &mut Stats) -> anyhow::Result<()> {
        if self.assignment.incarnations != current.incarnations
            || self.assignment.symbols != current.symbols
        {
            // No resumed old-incarnation data can precede this ordered end.
            self.end_pause(PauseEnd::Superseded, stats)?;
        }

        let restarted = stale_memberships(&self.assignment, &current, &self.membership);
        for s in &restarted {
            self.membership.restart(s);
            // A ticker reused by a new incarnation does not inherit the old
            // lifetime's initialization failure count or expired readiness cut.
            self.invalid_since.remove(s);
            self.attempts.remove(s);
            self.initialized.remove(s);
            self.unavailable.remove(s);
        }
        self.membership.replace(&current.symbols);
        self.initialized.retain(|s| self.membership.desired(s));
        self.unavailable.retain(|s| self.membership.desired(s));
        self.dirty.retain(|s| self.membership.desired(s));
        self.invalid_since.retain(|s, _| self.membership.desired(s));
        self.attempts.retain(|s, _| self.membership.desired(s));
        self.observed_refresh.retain(|s, r| {
            current.incarnations.get(s) == Some(&r.incarnation)
                && r.generation == self.source.generation
        });
        self.observed_resubscribe
            .retain(|s, r| current.incarnations.get(s) == Some(&r.incarnation));
        self.observed_probe.retain(|s, r| {
            current.incarnations.get(s) == Some(&r.incarnation)
                && r.generation == self.source.generation
        });
        self.resubscribe_started
            .retain(|s, r| current.incarnations.get(s) == Some(&r.incarnation));
        self.assignment = current;
        if !restarted.is_empty() {
            self.invalidate(Some(restarted), "new market incarnation")
                .await?;
        }
        Ok(())
    }
    async fn controls(&mut self, current: Control, stats: &mut Stats) -> anyhow::Result<bool> {
        if current.restart != self.restart {
            self.end_pause(PauseEnd::Superseded, stats)?;
            return Ok(true);
        }
        let destructive = current.refresh.iter().any(|(symbol, request)| {
            self.assignment.incarnations.get(symbol) == Some(&request.incarnation)
                && request.generation == self.source.generation
                && self.observed_refresh.get(symbol) != Some(request)
        }) || current.resubscribe.iter().any(|(symbol, request)| {
            self.assignment.incarnations.get(symbol) == Some(&request.incarnation)
                && request.generation == self.source.generation
                && self.observed_resubscribe.get(symbol) != Some(request)
        });
        self.pause_controls(current.pause, current.pause_release, destructive, stats)?;
        let mut refresh = vec![];
        for (s, request) in current.refresh {
            if self.assignment.incarnations.get(&s) == Some(&request.incarnation)
                && request.generation == self.source.generation
                && self.observed_refresh.get(&s) != Some(&request)
            {
                self.observed_refresh.insert(s.clone(), request);
                refresh.push(s);
            }
        }
        if !refresh.is_empty() {
            self.invalidate(Some(refresh), "requested snapshot refresh")
                .await?;
        }
        // A probe asks for one covering snapshot while the book stays
        // initialized: no Invalid marker, no readiness loss, and its ordered
        // tail keeps flowing. The answer replaces the Owner book through the
        // ordinary Data path. Same identity checks as refresh, before consuming.
        for (symbol, request) in current.probe {
            if self.assignment.incarnations.get(&symbol) == Some(&request.incarnation)
                && request.generation == self.source.generation
                && self.observed_probe.get(&symbol).is_none_or(|seen| {
                    seen.incarnation != request.incarnation
                        || seen.generation != request.generation
                        || seen.nonce < request.nonce
                })
            {
                self.observed_probe.insert(symbol.clone(), request);
                stats.probes += 1;
                // An uninitialized book already has its own snapshot pending.
                if self.initialized.contains(&symbol) && self.membership.accepts(&symbol) {
                    self.dirty.insert(symbol);
                }
            }
        }
        let mut resubscribe = vec![];
        let mut started = BTreeMap::new();
        for (symbol, request) in current.resubscribe {
            if self.assignment.incarnations.get(&symbol) == Some(&request.incarnation)
                && request.generation == self.source.generation
                && self.observed_resubscribe.get(&symbol) != Some(&request)
            {
                self.observed_resubscribe.insert(symbol.clone(), request);
                self.membership.restart(&symbol);
                self.resubscribe_started
                    .entry(symbol.clone())
                    .or_insert_with(|| Recovery {
                        incarnation: request.incarnation,
                        started: Instant::now(),
                    });
                stats.resubscriptions += 1;
                started.insert(symbol.clone(), request);
                resubscribe.push(symbol);
            }
        }
        if !resubscribe.is_empty() {
            self.invalidate(Some(resubscribe), "low activity resubscribe")
                .await?;
            self.runtime
                .relay
                .send(Input::RecoveryStarted {
                    source: self.source,
                    requests: started,
                })
                .await?;
        }
        if current.fence > self.fence {
            self.fence = current.fence;
            let marker = Input::Fence {
                source: self.source,
                request: current.fence,
                at: Utc::now(),
            };
            if self.pause.is_some() {
                self.pause_marker(marker)?;
            } else {
                self.runtime.relay.send(marker).await?;
            }
        }
        Ok(false)
    }
    async fn health(&mut self, socket: &mut Socket, stats: &mut Stats) -> anyhow::Result<()> {
        stats.frame_age_ms_max = stats
            .frame_age_ms_max
            .max(self.last_frame.elapsed().as_millis());
        stats.pong_age_ms_max = stats
            .pong_age_ms_max
            .max(self.last_pong.elapsed().as_millis());
        if let Some(pending) = &self.membership.pending {
            stats.ack_age_ms_max = stats.ack_age_ms_max.max(pending.sent.elapsed().as_millis());
        }
        ensure!(
            self.last_frame.elapsed() < Duration::from_secs(30),
            "websocket heartbeat timeout"
        );
        if self
            .membership
            .expired(self.runtime.options.health.ack_timeout)
        {
            stats.ack_timeouts += 1;
            let kind = self.membership.pending.as_ref().map(|p| p.kind.clone());
            let symbols = self
                .membership
                .recover_timeout(self.runtime.options.health.ack_timeout)
                .context("subscribe ACK timeout without a recoverable subscription id")?;
            for symbol in &symbols {
                let attempts = self.attempts.entry(symbol.clone()).or_default();
                *attempts += 1;
                ensure!(
                    *attempts <= 3,
                    "membership timeout retry budget exhausted: {symbol}"
                );
                self.resubscribe_started
                    .entry(symbol.clone())
                    .or_insert_with(|| Recovery {
                        incarnation: self.assignment.incarnations[symbol],
                        started: Instant::now(),
                    });
            }
            eprintln!(
                "membership timeout: {kind:?}; resubscribing {} affected symbols",
                symbols.len()
            );
            stats.resubscriptions += symbols.len() as u64;
            self.invalidate(Some(symbols), "membership ACK/snapshot timeout")
                .await?;
        }
        if self.runtime.protocol.contract().membership == MembershipStrategy::FreshSession {
            let expired = self
                .invalid_since
                .iter()
                .filter(|(s, at)| {
                    self.membership.accepts(s)
                        && at.elapsed() >= self.runtime.options.health.ack_timeout
                })
                .map(|(s, _)| s.clone())
                .collect::<Vec<_>>();
            ensure!(
                expired.is_empty() || !self.initialized.is_empty(),
                "fresh session initialized no assets before dump deadline"
            );
            for symbol in expired {
                if self.unavailable.insert(symbol.clone()) {
                    self.runtime
                        .relay
                        .send(Input::Unavailable {
                            source: self.source,
                            incarnation: self.assignment.incarnations[&symbol],
                            symbol,
                        })
                        .await?;
                }
            }
            stats.unavailable_assets = self.unavailable.len();
        } else {
            for (s, since) in &self.invalid_since {
                ensure!(
                    since.elapsed() < Duration::from_secs(90),
                    "snapshot readiness timeout: {s}"
                );
            }
        }
        if !self.admit_command() {
            return Ok(());
        }
        if let Some(command) = self.membership.command_with(
            self.runtime.protocol.as_ref(),
            &self.dirty,
            self.runtime.options.registry.command_batch,
        )? {
            if let Some(p) = &self.membership.pending {
                for s in &p.symbols {
                    // A probed book is still initialized and usable: it gets
                    // no readiness deadline. Lost answers still hit the ACK
                    // timeout recovery below.
                    let probed = p.kind == Kind::Snapshot && self.initialized.contains(s);
                    if p.kind != Kind::Delete && !probed {
                        self.invalid_since
                            .entry(s.clone())
                            .or_insert_with(Instant::now);
                    }
                    if p.kind == Kind::Snapshot {
                        let count = self.attempts.entry(s.clone()).or_default();
                        *count += 1;
                        ensure!(*count <= 3, "snapshot retry budget exhausted: {s}");
                    }
                }
            }
            let encoded = command.to_string();
            ensure!(
                encoded.len() <= 4 << 20,
                "encoded membership command exceeds 4 MiB"
            );
            write(
                socket,
                Message::Text(encoded.into()),
                Duration::from_secs(10),
            )
            .await?;
            stats.commands += 1;
            if self
                .membership
                .pending
                .as_ref()
                .is_some_and(|p| p.kind == Kind::Snapshot)
            {
                stats.resync += 1;
            }
            self.membership.command_written();
            if self.runtime.protocol.contract().membership == MembershipStrategy::FreshSession {
                let now = Instant::now();
                for s in self.membership.members() {
                    self.invalid_since.insert(s.clone(), now);
                }
                self.runtime
                    .relay
                    .send(Input::Membership {
                        roster: Some(self.assignment.incarnations.clone()),
                        source: self.source,
                        wire: self.membership.members().clone(),
                        at: Utc::now(),
                    })
                    .await?;
            }
        }
        // Also after an admission that produced no command. `needs_command` is
        // kept truthful so this should not occur, but a consumed slot must
        // never turn into a retry on every tick.
        self.next_command = Instant::now() + Duration::from_millis(500);
        Ok(())
    }
    async fn receive(
        &mut self,
        text: &str,
        received_at: chrono::DateTime<Utc>,
        received_tick: Instant,
        stats: &mut Stats,
    ) -> anyhow::Result<()> {
        let frame = match self.runtime.protocol.decode(text, received_at) {
            Ok(f) => f,
            Err(e) => {
                if self.runtime.protocol.contract().membership == MembershipStrategy::FreshSession {
                    return Err(e.context(InvalidProtocol));
                }
                self.invalidate(None, &format!("decode failure: {e:#}"))
                    .await?;
                return Ok(());
            }
        };
        if self.runtime.protocol.contract().ordering == OrderingStrategy::ComparableClock {
            ensure!(
                frame.events.iter().all(|event| match event {
                    BookEvent::Snapshot(s) => s.clock.is_some(),
                    BookEvent::Update(u) => u.clock.is_some(),
                }),
                "comparable-clock data lacks exchange clock"
            );
        }
        if self.runtime.protocol.contract().ordering == OrderingStrategy::SnapshotOnly {
            ensure!(
                frame.events.iter().all(|e| match e {
                    BookEvent::Snapshot(s) => s.clock.is_none(),
                    BookEvent::Update(u) => u.clock.is_none(),
                }),
                "snapshot-only data manufactured an ordering clock"
            );
        }
        if let Some((sid, seq)) = frame.sequence {
            ensure!(
                self.membership.sid.is_none_or(|expected| sid == expected),
                "unexpected subscription ID"
            );
            if self.tracker.observe(sid, seq) {
                stats.gaps += 1;
                if self.runtime.protocol.contract().membership == MembershipStrategy::FreshSession {
                    return Err(
                        anyhow::anyhow!("sequence gap/repeat on a fresh-session feed")
                            .context(InvalidProtocol),
                    );
                }
                self.invalidate(None, "sequence gap/repeat").await?;
            }
        }
        let acknowledged = matches!(frame.control, Reply::Subscribed(_) | Reply::Ok);
        let acknowledged_symbols: Vec<_> = self
            .membership
            .pending
            .as_ref()
            .filter(|p| {
                acknowledged
                    && frame.request_id == Some(p.id)
                    && matches!(p.kind, Kind::Subscribe | Kind::Add)
            })
            .map(|p| p.symbols.iter().cloned().collect())
            .unwrap_or_default();
        self.membership.control(frame.request_id, &frame.control)?;
        match frame.control {
            Reply::Pong => {
                self.last_pong = received_tick;
                stats.pongs_received += 1;
            }
            Reply::Error { code, .. } => match self.runtime.protocol.rejection(code) {
                Rejection::Backoff(delay) => self.next_command = Instant::now() + delay,
                Rejection::Reconnect => return Err(Rejected(code, true).into()),
                Rejection::Fatal => return Err(Rejected(code, false).into()),
            },
            _ => {}
        }
        if acknowledged {
            self.runtime
                .relay
                .send(Input::Membership {
                    roster: None,
                    source: self.source,
                    wire: self.membership.wire.clone(),
                    at: received_at,
                })
                .await?;
        }
        for event in frame.events {
            let symbol = event.symbol().to_owned();
            if !self.membership.accepts(&symbol) {
                stats.retired += 1;
                continue;
            }
            match &event {
                BookEvent::Snapshot(_) => {
                    self.initialized.insert(symbol.clone());
                    self.dirty.remove(&symbol);
                    self.membership.snapshot(&symbol);
                    self.finish_initialization(&symbol, stats);
                }
                BookEvent::Update(_) if !self.initialized.contains(&symbol) => {
                    stats.quarantined += 1;
                    self.dirty.insert(symbol);
                    continue;
                }
                _ => {}
            }
            let incarnation = self.assignment.incarnations[&symbol];
            self.runtime
                .relay
                .send(Input::Data {
                    source: self.source,
                    incarnation,
                    received_tick,
                    event,
                })
                .await?;
        }
        for symbol in acknowledged_symbols {
            self.finish_initialization(&symbol, stats);
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn session(
    runtime: &Runtime,
    socket: &mut Socket,
    source: Source,
    admission: u64,
    assignment: &mut watch::Receiver<Assignment>,
    control: &mut watch::Receiver<Control>,
    stop: &mut watch::Receiver<bool>,
    stats: &mut Stats,
    progress: &watch::Sender<Stats>,
) -> anyhow::Result<()> {
    let current = assignment.borrow_and_update().clone();
    let commands = control.borrow_and_update().clone();
    if commands.restart != admission {
        return Ok(());
    }
    let mut live = Session {
        runtime,
        source,
        membership: Membership::with_strategy(
            &current.symbols,
            runtime.protocol.contract().membership,
        ),
        assignment: current,
        tracker: SequenceTracker::default(),
        initialized: BTreeSet::new(),
        unavailable: BTreeSet::new(),
        dirty: BTreeSet::new(),
        invalid_since: HashMap::new(),
        attempts: HashMap::new(),
        observed_refresh: BTreeMap::new(),
        observed_resubscribe: commands.resubscribe.clone(),
        // A fresh session subscribes with fresh snapshots; old probes are moot.
        observed_probe: commands.probe.clone(),
        resubscribe_started: BTreeMap::new(),
        restart: commands.restart,
        fence: commands.fence,
        next_command: Instant::now(),
        urgent: None,
        last_frame: Instant::now(),
        last_pong: Instant::now(),
        pause: None,
        pause_high_water: 0,
        release_high_water: 0,
        pause_not_before: Instant::now(),
    };
    // A reconnect itself provides a fresh baseline; only subsequent refresh changes need commands.
    live.observed_refresh = commands.refresh.clone();
    live.controls(commands, stats).await?;
    let mut ping = tokio::time::interval(Duration::from_secs(10));
    let mut health = tokio::time::interval(Duration::from_millis(100));
    ping.set_missed_tick_behavior(MissedTickBehavior::Skip);
    health.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        // Controls which clear a pause and carry destructive intent must be
        // consumed together before normal socket polling becomes eligible.
        if *stop.borrow() {
            live.end_pause(PauseEnd::Released, stats)?;
            return Ok(());
        }
        if assignment
            .has_changed()
            .map_err(|error| RetireTransport(format!("assignment owner stopped: {error}")))?
        {
            let current = assignment.borrow_and_update().clone();
            live.sync(current, stats).await?;
        }
        if control
            .has_changed()
            .map_err(|error| RetireTransport(format!("control owner stopped: {error}")))?
        {
            let current = control.borrow_and_update().clone();
            if live.controls(current, stats).await? {
                return Ok(());
            }
        }
        if live.assignment.symbols.is_empty() {
            live.end_pause(PauseEnd::Superseded, stats)?;
            return Ok(());
        }
        if live
            .pause
            .as_ref()
            .is_some_and(|pause| Instant::now() >= pause.until)
        {
            live.end_pause(PauseEnd::Expired, stats)?;
        }
        live.release_stale_urgent();
        let paused = live.pause.is_some();
        let pause_deadline = live
            .pause
            .as_ref()
            .map(|pause| pause.until)
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(86400));
        tokio::select! {
            biased;
            _ = stopped(stop) => {
                live.end_pause(PauseEnd::Released, stats)?;
                return Ok(());
            },
            changed = assignment.changed() => {
                changed.map_err(|error| RetireTransport(format!("assignment owner stopped: {error}")))?;
                let current = assignment.borrow_and_update().clone();
                live.sync(current, stats).await?;
                if live.assignment.symbols.is_empty() {
                    return Ok(());
                }
            },
            changed = control.changed() => {
                changed.map_err(|error| RetireTransport(format!("control owner stopped: {error}")))?;
                // Both watch lanes can be ready in the same select turn. A
                // scoped recovery must use the latest incarnation before it
                // revokes membership or prepares any wire command.
                if assignment.has_changed().map_err(|error| RetireTransport(format!("assignment owner stopped: {error}")))? {
                    let latest = assignment.borrow_and_update().clone();
                    live.sync(latest, stats).await?;
                }
                if live.assignment.symbols.is_empty() { return Ok(()); }
                let current = control.borrow_and_update().clone();
                if live.controls(current, stats).await? {
                    return Ok(());
                }
            },
            _ = tokio::time::sleep_until(pause_deadline), if paused => {
                live.end_pause(PauseEnd::Expired, stats)?;
            },
            // Public application PING follows the completed initial write;
            // native immediate control-frame Ping remains unchanged.
            _ = ping.tick(), if !paused && (runtime.protocol.contract().membership == MembershipStrategy::Acknowledged || !live.membership.members().is_empty()) => {
                write(socket, runtime.protocol.heartbeat(), Duration::from_secs(5)).await?;
                stats.pings_sent += 1;
            },
            _ = health.tick(), if !paused => {
                if assignment.has_changed()? {
                    let latest = assignment.borrow_and_update().clone();
                    live.sync(latest, stats).await?;
                }
                if live.assignment.symbols.is_empty() { return Ok(()); }
                live.health(socket, stats).await?;
                progress.send_replace(stats.clone());
            },
            message = socket.next(), if !paused => {
                let received_at = Utc::now();
                let received_tick = Instant::now();
                live.last_frame = Instant::now();
                let text = match message.context("websocket ended")?? {
                    Message::Text(t) => t.to_string(),
                    Message::Binary(b) => String::from_utf8(b.to_vec())?,
                    Message::Ping(p) => {
                        write(socket, Message::Pong(p), Duration::from_secs(5)).await?;
                        continue;
                    },
                    Message::Pong(_) => {live.last_pong = Instant::now();stats.pongs_received += 1;continue;},
                    Message::Close(_) => anyhow::bail!("peer closed websocket"),
                    _ => continue,
                };
                stats.frames += 1;
                stats.bytes += text.len() as u64;
                if assignment.has_changed()? {
                    let current = assignment.borrow_and_update().clone();
                    live.sync(current, stats).await?;
                }
                live.receive(&text, received_at, received_tick, stats).await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retire_and_readd_before_delete_admission_cannot_reuse_wire_membership() {
        let mut membership = Membership::new(&["A".into(), "B".into()]);
        membership.command(&BTreeSet::new(), 10).unwrap();
        membership.control(Some(1), &Reply::Subscribed(1)).unwrap();
        // The source observed retirement, but its paced delete has not run yet.
        membership.replace(&["B".into()]);
        let old = Assignment {
            epoch: 2,
            symbols: vec!["B".into()],
            incarnations: BTreeMap::from([("B".into(), 1)]),
        };
        let new = Assignment {
            epoch: 3,
            symbols: vec!["A".into(), "B".into()],
            incarnations: BTreeMap::from([("A".into(), 3), ("B".into(), 1)]),
        };
        let stale = stale_memberships(&old, &new, &membership);
        assert_eq!(stale, vec!["A"]);
        for symbol in &stale {
            membership.restart(symbol);
        }
        membership.replace(&new.symbols);
        assert!(!membership.accepts("A"));
        assert!(membership.accepts("B"));
        assert_eq!(
            membership.command(&BTreeSet::new(), 10).unwrap().unwrap()["params"]["action"],
            "delete_markets"
        );
        membership.control(Some(2), &Reply::Ok).unwrap();
        assert!(!membership.accepts("A"));
        assert_eq!(
            membership.command(&BTreeSet::new(), 10).unwrap().unwrap()["params"]["action"],
            "add_markets"
        );
        membership.control(Some(3), &Reply::Ok).unwrap();
        assert!(membership.accepts("A"));
    }

    fn runtime_for_session_tests() -> (Runtime, tokio::sync::mpsc::Receiver<crate::relay::Admitted>)
    {
        let options = Options {
            symbols: vec!["A".into(), "B".into()],
            rest_url: "http://127.0.0.1:1".into(),
            ws_url: "ws://127.0.0.1:1".into(),
            symbols_per_connection: 2,
            max_connections: 2,
            max_symbols: 2,
            output: "unused-session-test.sbe".into(),
            audit: None,
            snapshot_interval: Duration::from_secs(60),
            flush_interval: Duration::from_secs(60),
            connect: crate::transport::ConnectOptions::default(),
            registry: crate::collector::RegistryOptions::default(),
            summary: None,
            replicas: 2,
            replica_lag: Duration::from_secs(2),
            failover_test_after: None,
            health: crate::health::Options::default(),
        };
        let (relay, receiver) = relay::channel(Arc::new(tokio::sync::Semaphore::new(1 << 20)));
        (
            Runtime {
                protocol: Arc::new(crate::exchange::kalshi::Kalshi),
                shard: 0,
                options: Arc::new(options),
                auth: None,
                pacer: Arc::new(Pacer::new(Duration::from_millis(100))),
                relay,
            },
            receiver,
        )
    }

    fn initialized_session_for_test(runtime: &Runtime) -> Session<'_> {
        let symbols = vec!["A".into(), "B".into()];
        let mut membership = Membership::new(&symbols);
        membership.command(&BTreeSet::new(), 10).unwrap();
        membership.command_written();
        membership.control(Some(1), &Reply::Subscribed(1)).unwrap();
        Session {
            runtime,
            source: Source {
                replica: 0,
                generation: 1,
            },
            membership,
            assignment: Assignment {
                epoch: 1,
                symbols,
                incarnations: BTreeMap::from([("A".into(), 1), ("B".into(), 1)]),
            },
            tracker: SequenceTracker::default(),
            initialized: BTreeSet::from(["A".into(), "B".into()]),
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

    #[tokio::test(start_paused = true)]
    async fn covering_probe_preserves_readiness_and_rejects_consumed_or_stale_identities() {
        let (runtime, mut receiver) = runtime_for_session_tests();
        let mut session = initialized_session_for_test(&runtime);
        let mut stats = Stats::default();
        let request = crate::relay::Probe {
            incarnation: 1,
            generation: 1,
            nonce: 1,
        };
        let control = |request| Control {
            probe: BTreeMap::from([("A".into(), request)]),
            ..Control::default()
        };
        for stale in [
            crate::relay::Probe {
                incarnation: 2,
                ..request
            },
            crate::relay::Probe {
                generation: 2,
                ..request
            },
        ] {
            assert!(!session.controls(control(stale), &mut stats).await.unwrap());
            assert!(session.dirty.is_empty());
        }
        assert!(
            !session
                .controls(control(request), &mut stats)
                .await
                .unwrap()
        );
        assert_eq!(stats.probes, 1);
        assert!(session.initialized.contains("A"));
        assert!(session.initialized.contains("B"));
        assert!(session.dirty.contains("A"));
        assert!(!session.dirty.contains("B"));
        assert!(session.invalid_since.is_empty());
        assert!(receiver.try_recv().is_err(), "probe must not emit Invalid");
        session.dirty.remove("A");
        for consumed in [
            request,
            crate::relay::Probe {
                nonce: 0,
                ..request
            },
        ] {
            assert!(
                !session
                    .controls(control(consumed), &mut stats)
                    .await
                    .unwrap()
            );
            assert!(session.dirty.is_empty());
        }
        assert_eq!(stats.probes, 1);
        let mut next = session.assignment.clone();
        next.incarnations.insert("A".into(), 2);
        session.sync(next, &mut stats).await.unwrap();
        assert!(!session.observed_probe.contains_key("A"));
        session
            .controls(control(request), &mut stats)
            .await
            .unwrap();
        assert_eq!(stats.probes, 1, "old incarnation re-applied");
        let new = crate::relay::Probe {
            incarnation: 2,
            ..request
        };
        session.controls(control(new), &mut stats).await.unwrap();
        assert_eq!(session.observed_probe.get("A"), Some(&new));
        assert_eq!(stats.probes, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn reincarnation_restarts_source_deadline_and_attempts_without_resetting_other_symbols() {
        let (runtime, mut receiver) = runtime_for_session_tests();
        let mut session = initialized_session_for_test(&runtime);
        let old_a_deadline = Instant::now();
        let old_b_deadline = old_a_deadline + Duration::from_secs(17);
        session.invalid_since.insert("A".into(), old_a_deadline);
        session.invalid_since.insert("B".into(), old_b_deadline);
        session.attempts.insert("A".into(), 3);
        session.attempts.insert("B".into(), 2);
        session.dirty.insert("B".into());
        // A coalesced remove/readd arrives just before the old lifetime's
        // readiness deadline. Its symbol spelling is unchanged; its identity is not.
        tokio::time::advance(Duration::from_secs(89)).await;
        let new_lifetime_started = Instant::now();
        let mut assignment = session.assignment.clone();
        assignment.epoch = 2;
        assignment.incarnations.insert("A".into(), 2);
        session
            .sync(assignment, &mut Stats::default())
            .await
            .unwrap();

        let admitted = receiver.try_recv().unwrap();
        assert!(matches!(
            admitted.input,
            Input::Invalid { source: Source { replica: 0, generation: 1 }, symbols: Some(ref symbols), .. }
                if symbols == &["A".to_owned()]
        ));
        assert!(!session.initialized.contains("A"));
        assert!(!session.membership.accepts("A"));
        assert!(session.dirty.contains("A"));
        assert_eq!(session.assignment.incarnations["A"], 2);

        // B is the same lifetime: neither a new A deadline nor its fresh retry
        // budget is permission to reset unrelated recovery state.
        assert!(session.initialized.contains("B"));
        assert!(session.membership.accepts("B"));
        assert!(session.dirty.contains("B"));
        assert_eq!(session.invalid_since.get("B"), Some(&old_b_deadline));
        assert_eq!(session.attempts.get("B"), Some(&2));
        assert_eq!(session.assignment.incarnations["B"], 1);

        assert_eq!(
            (
                session.attempts.get("A").copied(),
                session.invalid_since.get("A").copied()
            ),
            (None, Some(new_lifetime_started)),
            "new incarnation inherited the old lifetime's attempt count or readiness deadline"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_controls_require_current_incarnation_and_generation_before_consuming_nonce() {
        fn refresh_control(request: crate::relay::Refresh) -> Control {
            Control {
                refresh: BTreeMap::from([("A".into(), request)]),
                ..Control::default()
            }
        }

        let (runtime, mut receiver) = runtime_for_session_tests();
        let mut session = initialized_session_for_test(&runtime);
        session.source.generation = 2;
        session.assignment.incarnations.insert("A".into(), 2);
        let mut stats = Stats::default();
        let current = crate::relay::Refresh {
            incarnation: 2,
            generation: 2,
            nonce: 7,
        };
        // A reused nonce does not make a delayed old lifetime or generation
        // control authoritative for the new, already initialized A book.
        for stale in [
            crate::relay::Refresh {
                incarnation: 1,
                ..current
            },
            crate::relay::Refresh {
                generation: 1,
                ..current
            },
        ] {
            assert!(
                !session
                    .controls(refresh_control(stale), &mut stats)
                    .await
                    .unwrap()
            );
            assert!(session.initialized.contains("A"));
            assert!(!session.dirty.contains("A"));
            assert!(!session.invalid_since.contains_key("A"));
            assert!(!session.observed_refresh.contains_key("A"));
            assert!(matches!(
                receiver.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ));
        }

        let current_deadline = Instant::now();
        assert!(
            !session
                .controls(refresh_control(current), &mut stats)
                .await
                .unwrap()
        );
        assert_eq!(session.observed_refresh.get("A"), Some(&current));
        assert!(!session.initialized.contains("A"));
        assert!(session.dirty.contains("A"));
        assert_eq!(session.invalid_since.get("A"), Some(&current_deadline));
        let admitted = receiver.try_recv().unwrap();
        assert!(matches!(
            admitted.input,
            Input::Invalid { source: Source { replica: 0, generation: 2 }, symbols: Some(ref symbols), .. }
                if symbols == &["A".to_owned()]
        ));
        assert!(session.initialized.contains("B"));
        assert!(!session.dirty.contains("B"));
        assert!(!session.invalid_since.contains_key("B"));

        // Replaying the consumed current control cannot invalidate a recovered
        // baseline for a second time without a new scoped nonce.
        session.initialized.insert("A".into());
        session.dirty.remove("A");
        session.invalid_since.remove("A");
        assert!(
            !session
                .controls(refresh_control(current), &mut stats)
                .await
                .unwrap()
        );
        assert!(session.initialized.contains("A"));
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }
    fn fixture() -> (Runtime, tokio::sync::mpsc::Receiver<relay::Admitted>) {
        fixture_with_bytes(Arc::new(tokio::sync::Semaphore::new(1 << 20)))
    }
    fn fixture_with_bytes(
        bytes: Arc<tokio::sync::Semaphore>,
    ) -> (Runtime, tokio::sync::mpsc::Receiver<relay::Admitted>) {
        let (relay, rx) = relay::channel(bytes);
        let options = Options {
            symbols: vec!["A".into()],
            rest_url: "unused".into(),
            ws_url: "ws://127.0.0.1:1/".into(),
            symbols_per_connection: 10,
            max_connections: 2,
            max_symbols: 10,
            replicas: 2,
            replica_lag: Duration::from_millis(250),
            failover_test_after: None,
            output: "/unused.sbe".into(),
            audit: None,
            summary: None,
            snapshot_interval: Duration::from_secs(60),
            flush_interval: Duration::from_secs(60),
            connect: crate::transport::ConnectOptions::default(),
            registry: crate::collector::RegistryOptions {
                command_interval: Duration::from_millis(1),
                ..Default::default()
            },
            health: crate::health::Options {
                cooldown: Duration::from_secs(1),
                rotation_interval: Duration::ZERO,
                ..Default::default()
            },
        };
        (
            Runtime {
                protocol: Arc::new(crate::exchange::kalshi::Kalshi),
                shard: 0,
                options: Arc::new(options),
                auth: None,
                pacer: Arc::new(Pacer::new(Duration::from_millis(1))),
                relay,
            },
            rx,
        )
    }

    fn ready_session(runtime: &Runtime) -> Session<'_> {
        let mut membership = Membership::new(&["A".into()]);
        membership.command(&BTreeSet::new(), 10).unwrap();
        membership.command_written();
        membership.control(Some(1), &Reply::Subscribed(7)).unwrap();
        membership.snapshot("A");
        let mut tracker = SequenceTracker::default();
        assert!(!tracker.observe(7, 1));
        Session {
            runtime,
            source: Source {
                replica: 0,
                generation: 1,
            },
            membership,
            assignment: Assignment {
                epoch: 1,
                symbols: vec!["A".into()],
                incarnations: BTreeMap::from([("A".into(), 1)]),
            },
            tracker,
            initialized: BTreeSet::from(["A".into()]),
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
    fn request(nonce: u64) -> PauseRequest {
        PauseRequest {
            generation: 1,
            nonce,
            acquire_until: Instant::now() + Duration::from_secs(2),
            max_duration: MAX_PAUSE,
        }
    }
    fn next(rx: &mut tokio::sync::mpsc::Receiver<relay::Admitted>) -> Input {
        rx.try_recv().expect("missing FIFO source marker").input
    }

    #[tokio::test(start_paused = true)]
    async fn pause_expiry_and_fence_watch_aba_never_reacquire_a_consumed_nonce() {
        let (runtime, mut rx) = fixture();
        let mut live = ready_session(&runtime);
        let mut stats = Stats::default();
        let mut token = request(1);
        token.max_duration = Duration::from_secs(60);
        live.pause_controls(Some(token), None, false, &mut stats)
            .unwrap();
        let until = match next(&mut rx) {
            Input::PauseStarted {
                nonce: 1, until, ..
            } => until,
            other => panic!("{other:?}"),
        };
        assert_eq!(until - Instant::now(), MAX_PAUSE);
        tokio::time::advance(MAX_PAUSE).await;
        live.end_pause(PauseEnd::Expired, &mut stats).unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseEnded {
                nonce: 1,
                reason: PauseEnd::Expired,
                ..
            }
        ));
        // An unrelated watch update still carries the exact expired token.
        live.controls(
            Control {
                pause: Some(token),
                fence: 1,
                ..Default::default()
            },
            &mut stats,
        )
        .await
        .unwrap();
        assert!(matches!(next(&mut rx), Input::Fence { request: 1, .. }));
        assert!(rx.try_recv().is_err());
        assert!(live.pause.is_none());
        assert_eq!(stats.handoff_pauses, 1);
        assert_eq!(stats.pause_expirations, 1);
        assert_eq!(stats.pause_us_max, 5_000_000);
    }

    #[tokio::test(start_paused = true)]
    async fn coalesced_release_is_acknowledged_and_old_release_cannot_end_a_new_lease() {
        let (runtime, mut rx) = fixture();
        let mut live = ready_session(&runtime);
        let mut stats = Stats::default();
        let release = PauseId {
            generation: 1,
            nonce: 4,
        };
        live.pause_controls(None, Some(release), false, &mut stats)
            .unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseEnded {
                nonce: 4,
                reason: PauseEnd::Released,
                ..
            }
        ));
        live.pause_controls(Some(request(4)), Some(release), false, &mut stats)
            .unwrap();
        assert!(rx.try_recv().is_err());
        live.pause_controls(Some(request(6)), None, false, &mut stats)
            .unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseStarted { nonce: 6, .. }
        ));
        live.pause_controls(
            Some(request(6)),
            Some(PauseId {
                generation: 1,
                nonce: 5,
            }),
            false,
            &mut stats,
        )
        .unwrap();
        // Nonce 5 is below the consumed high-water mark: no Owner reservation
        // can still await it, so no duplicate end marker enters the FIFO.
        assert!(rx.try_recv().is_err());
        assert_eq!(live.pause.as_ref().unwrap().nonce, 6);
        // Neither an old-generation request nor release changes this lease.
        live.pause_controls(
            Some(PauseRequest {
                generation: 0,
                nonce: 100,
                ..request(100)
            }),
            Some(PauseId {
                generation: 0,
                nonce: 100,
            }),
            false,
            &mut stats,
        )
        .unwrap();
        assert!(rx.try_recv().is_err());
        assert_eq!(live.pause_high_water, 6);
        assert_eq!(live.pause.as_ref().unwrap().nonce, 6);
        live.pause_controls(
            None,
            Some(PauseId {
                generation: 1,
                nonce: 6,
            }),
            false,
            &mut stats,
        )
        .unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseEnded {
                nonce: 6,
                reason: PauseEnd::Released,
                ..
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn consumed_nonce_release_after_busy_or_expiry_emits_exactly_one_end_marker() {
        let (runtime, mut rx) = fixture();
        let mut live = ready_session(&runtime);
        let mut stats = Stats::default();
        // Busy is nonce 1's terminal marker; Owner's later release is cleanup.
        live.membership.pending = Some(crate::membership::Pending {
            id: 2,
            kind: Kind::Add,
            written: true,
            symbols: BTreeSet::from(["A".into()]),
            sent: Instant::now(),
        });
        live.pause_controls(Some(request(1)), None, false, &mut stats)
            .unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseEnded {
                nonce: 1,
                reason: PauseEnd::Busy,
                ..
            }
        ));
        let release = |nonce| PauseId {
            generation: 1,
            nonce,
        };
        live.pause_controls(None, Some(release(1)), false, &mut stats)
            .unwrap();
        assert!(rx.try_recv().is_err());
        live.membership.pending = None;
        // Expired is nonce 2's terminal marker, even when release-only arrives.
        live.pause_controls(Some(request(2)), None, false, &mut stats)
            .unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseStarted { nonce: 2, .. }
        ));
        tokio::time::advance(MAX_PAUSE).await;
        live.end_pause(PauseEnd::Expired, &mut stats).unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseEnded {
                nonce: 2,
                reason: PauseEnd::Expired,
                ..
            }
        ));
        live.pause_controls(None, Some(release(2)), false, &mut stats)
            .unwrap();
        assert!(rx.try_recv().is_err());
        // A reservation whose request coalesced away still gets its one end.
        live.pause_controls(None, Some(release(3)), false, &mut stats)
            .unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseEnded {
                nonce: 3,
                reason: PauseEnd::Released,
                ..
            }
        ));
        assert!(rx.try_recv().is_err());
        // Neither cleanup re-armed or extended a lease.
        live.pause_controls(Some(request(3)), None, false, &mut stats)
            .unwrap();
        assert!(rx.try_recv().is_err());
        assert!(live.pause.is_none());
        assert_eq!((stats.handoff_pauses, stats.pause_expirations), (1, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn newer_tokens_end_then_report_busy_instead_of_extending_continuous_hold() {
        let (runtime, mut rx) = fixture();
        let mut live = ready_session(&runtime);
        let mut stats = Stats::default();
        live.pause_controls(Some(request(1)), None, false, &mut stats)
            .unwrap();
        next(&mut rx);
        tokio::time::advance(Duration::from_secs(1)).await;
        live.pause_controls(Some(request(2)), None, false, &mut stats)
            .unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseEnded {
                nonce: 1,
                reason: PauseEnd::Superseded,
                ..
            }
        ));
        assert!(matches!(
            next(&mut rx),
            Input::PauseEnded {
                nonce: 2,
                reason: PauseEnd::Busy,
                ..
            }
        ));
        assert_eq!(live.last_frame.elapsed(), Duration::from_secs(1));
        assert_eq!(live.last_pong.elapsed(), Duration::from_secs(1));
        live.pause_controls(Some(request(3)), None, false, &mut stats)
            .unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseEnded {
                nonce: 3,
                reason: PauseEnd::Busy,
                ..
            }
        ));
        tokio::time::advance(runtime.options.health.cooldown).await;
        live.pause_controls(Some(request(3)), None, false, &mut stats)
            .unwrap();
        assert!(rx.try_recv().is_err());
        live.pause_controls(Some(request(4)), None, false, &mut stats)
            .unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseStarted { nonce: 4, .. }
        ));
        assert_eq!(stats.handoff_pauses, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn pending_ack_expired_acquisition_and_near_heartbeat_are_visible_terminal_deferrals() {
        let (runtime, mut rx) = fixture();
        let mut live = ready_session(&runtime);
        let mut stats = Stats::default();
        live.membership.pending = Some(crate::membership::Pending {
            id: 2,
            kind: Kind::Add,
            written: true,
            symbols: BTreeSet::from(["A".into()]),
            sent: Instant::now(),
        });
        live.pause_controls(Some(request(1)), None, false, &mut stats)
            .unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseEnded {
                nonce: 1,
                reason: PauseEnd::Busy,
                ..
            }
        ));
        live.membership.pending = None;
        live.pause_controls(Some(request(1)), None, false, &mut stats)
            .unwrap();
        assert!(rx.try_recv().is_err());
        let expired = PauseRequest {
            acquire_until: Instant::now(),
            ..request(2)
        };
        live.pause_controls(Some(expired), None, false, &mut stats)
            .unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseEnded {
                nonce: 2,
                reason: PauseEnd::Expired,
                ..
            }
        ));
        live.last_frame = Instant::now() - Duration::from_secs(25);
        live.pause_controls(Some(request(3)), None, false, &mut stats)
            .unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseEnded {
                nonce: 3,
                reason: PauseEnd::Busy,
                ..
            }
        ));
        assert_eq!(stats.handoff_pauses, 0);
        live.last_frame = Instant::now();
        live.pause_controls(Some(request(4)), None, false, &mut stats)
            .unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseStarted { nonce: 4, .. }
        ));
    }

    #[tokio::test]
    async fn local_lifetime_change_ends_pause_before_invalidation_but_global_epoch_does_not() {
        let (runtime, mut rx) = fixture();
        let mut live = ready_session(&runtime);
        let mut stats = Stats::default();
        live.pause_controls(Some(request(1)), None, false, &mut stats)
            .unwrap();
        next(&mut rx);
        let mut assignment = live.assignment.clone();
        assignment.epoch += 1;
        live.sync(assignment.clone(), &mut stats).await.unwrap();
        assert!(live.pause.is_some());
        assert!(rx.try_recv().is_err());
        assignment.incarnations.insert("A".into(), 2);
        live.sync(assignment, &mut stats).await.unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseEnded {
                nonce: 1,
                reason: PauseEnd::Superseded,
                ..
            }
        ));
        assert!(matches!(next(&mut rx), Input::Invalid { .. }));
        assert!(!live.membership.accepts("A"));
    }

    #[tokio::test]
    async fn full_relay_rejects_start_and_end_without_waiting_or_unmarked_data() {
        let (runtime, mut rx) = fixture();
        let mut live = ready_session(&runtime);
        let mut stats = Stats::default();
        live.pause_controls(Some(request(1)), None, false, &mut stats)
            .unwrap();
        next(&mut rx);
        for request in 0..512 {
            runtime
                .relay
                .try_send(Input::Fence {
                    source: live.source,
                    request,
                    at: Utc::now(),
                })
                .unwrap();
        }
        let error = live.end_pause(PauseEnd::Expired, &mut stats).unwrap_err();
        assert!(error.downcast_ref::<RetireTransport>().is_some());
        for _ in 0..512 {
            assert!(matches!(next(&mut rx), Input::Fence { .. }));
        }
        assert!(rx.try_recv().is_err());
        live.pause_not_before = Instant::now();
        for request in 0..512 {
            runtime
                .relay
                .try_send(Input::Fence {
                    source: live.source,
                    request,
                    at: Utc::now(),
                })
                .unwrap();
        }
        assert!(
            live.pause_controls(Some(request(2)), None, false, &mut stats)
                .unwrap_err()
                .downcast_ref::<RetireTransport>()
                .is_some()
        );
        assert!(live.pause.is_none());
        assert_eq!(stats.handoff_pauses, 1);
    }

    async fn socket_pair() -> (
        Socket,
        tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/", listener.local_addr().unwrap());
        let options = crate::transport::ConnectOptions::default();
        let connect = transport::connect_public(&url, &options);
        let accept = async {
            let (stream, _) = listener.accept().await.unwrap();
            tokio_tungstenite::accept_async(stream).await.unwrap()
        };
        let (client, server) = tokio::join!(connect, accept);
        (client.unwrap().socket, server)
    }
    fn delta(seq: u64) -> String {
        serde_json::json!({"type":"orderbook_delta", "sid":7, "seq":seq,
            "sending_ts_ms":1700000000000i64, "msg":{"market_ticker":"A", "clock":100+seq,
            "side":"yes", "price_dollars":"0.4", "delta_fp":"1"}})
        .to_string()
    }
    fn frame_bytes(opcode: u8, final_frame: bool, payload: &[u8]) -> Vec<u8> {
        let mut frame = vec![opcode | if final_frame { 0x80 } else { 0 }];
        if payload.len() < 126 {
            frame.push(payload.len() as u8);
        } else {
            frame.push(126);
            frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
        frame.extend_from_slice(payload);
        frame
    }

    #[tokio::test]
    async fn cancelled_partial_header_payload_and_fragment_reads_resume_on_the_same_native_stream()
    {
        use tokio::io::AsyncWriteExt;
        for part in 0..3 {
            let (runtime, mut rx) = fixture();
            let mut live = ready_session(&runtime);
            let mut stats = Stats::default();
            let (mut client, mut server) = socket_pair().await;
            let text = delta(2);
            let (prefix, tail) = if part == 2 {
                let split = text.len() / 2;
                (
                    frame_bytes(1, false, &text.as_bytes()[..split]),
                    frame_bytes(0, true, &text.as_bytes()[split..]),
                )
            } else {
                let frame = frame_bytes(1, true, text.as_bytes());
                let split = if part == 0 { 1 } else { frame.len() / 2 };
                (frame[..split].to_vec(), frame[split..].to_vec())
            };
            server.get_mut().write_all(&prefix).await.unwrap();
            let tokio_tungstenite::MaybeTlsStream::Plain(tcp) = client.get_ref() else {
                unreachable!()
            };
            tcp.readable().await.unwrap();
            // Poll until Pending with readable bytes already present, then drop
            // Next. Verify parser consumption rather than guessing via sleeps.
            std::future::poll_fn(|cx| {
                assert!(client.poll_next_unpin(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            let tokio_tungstenite::MaybeTlsStream::Plain(tcp) = client.get_ref() else {
                unreachable!()
            };
            assert_eq!(
                tcp.try_read(&mut [0]).unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
            live.pause_controls(Some(request(1)), None, false, &mut stats)
                .unwrap();
            server.get_mut().write_all(&tail).await.unwrap();
            live.pause_controls(
                None,
                Some(PauseId {
                    generation: 1,
                    nonce: 1,
                }),
                false,
                &mut stats,
            )
            .unwrap();
            let Message::Text(resumed) =
                tokio::time::timeout(Duration::from_secs(1), client.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap()
            else {
                panic!("native text expected")
            };
            assert_eq!(resumed, text);
            live.receive(&resumed, Utc::now(), Instant::now(), &mut stats)
                .await
                .unwrap();
            assert!(matches!(
                next(&mut rx),
                Input::PauseStarted { nonce: 1, .. }
            ));
            assert!(matches!(
                next(&mut rx),
                Input::PauseEnded {
                    nonce: 1,
                    reason: PauseEnd::Released,
                    ..
                }
            ));
            assert!(matches!(
                next(&mut rx),
                Input::Data {
                    source: Source { generation: 1, .. },
                    incarnation: 1,
                    ..
                }
            ));
            server.send(Message::Text(delta(3).into())).await.unwrap();
            let Message::Text(tail) = client.next().await.unwrap().unwrap() else {
                panic!("native tail expected")
            };
            live.receive(&tail, Utc::now(), Instant::now(), &mut stats)
                .await
                .unwrap();
            assert!(matches!(next(&mut rx), Input::Data { .. }));
            assert_eq!(live.membership.sid, Some(7));
            assert_eq!(stats.gaps, 0);
            // A real native sequence skip after resume still invalidates the
            // sid's books and quarantines the update; pause is no reset witness.
            server.send(Message::Text(delta(5).into())).await.unwrap();
            let Message::Text(gap) = client.next().await.unwrap().unwrap() else {
                panic!("native gap expected")
            };
            live.receive(&gap, Utc::now(), Instant::now(), &mut stats)
                .await
                .unwrap();
            assert_eq!(stats.gaps, 1);
            assert!(matches!(next(&mut rx), Input::Invalid { .. }));
            assert!(rx.try_recv().is_err());
            let close = transport::close(&mut client);
            let echo = async {
                assert!(matches!(
                    server.next().await.unwrap().unwrap(),
                    Message::Close(_)
                ));
                server.flush().await.unwrap();
            };
            let (close, ()) = tokio::join!(close, echo);
            close.unwrap();
        }
    }

    async fn command(
        ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    ) -> serde_json::Value {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => return serde_json::from_str(&text).unwrap(),
                Message::Ping(payload) => ws.send(Message::Pong(payload)).await.unwrap(),
                other => panic!("unexpected native startup frame: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn actual_paused_source_orders_end_before_resumed_delta_and_preserves_sequence() {
        run_paused_source(false).await;
    }

    #[tokio::test]
    async fn full_relay_at_stop_drops_paused_transport_and_joins_without_a_hidden_marker_wait() {
        run_paused_source(true).await;
    }

    #[tokio::test]
    async fn release_and_scoped_recovery_in_one_watch_revoke_publication_before_resumed_input() {
        let (runtime, mut rx) = fixture();
        let mut live = ready_session(&runtime);
        let mut stats = Stats::default();
        live.pause_controls(Some(request(1)), None, false, &mut stats)
            .unwrap();
        next(&mut rx);
        live.controls(
            Control {
                pause_release: Some(PauseId {
                    generation: 1,
                    nonce: 1,
                }),
                resubscribe: BTreeMap::from([(
                    "A".into(),
                    Resubscribe {
                        generation: 1,
                        incarnation: 1,
                        nonce: 1,
                    },
                )]),
                ..Default::default()
            },
            &mut stats,
        )
        .await
        .unwrap();
        assert!(matches!(
            next(&mut rx),
            Input::PauseEnded {
                nonce: 1,
                reason: PauseEnd::Released,
                ..
            }
        ));
        assert!(matches!(next(&mut rx), Input::Invalid { .. }));
        assert!(matches!(next(&mut rx), Input::RecoveryStarted { .. }));
        live.receive(&delta(2), Utc::now(), Instant::now(), &mut stats)
            .await
            .unwrap();
        assert!(!live.membership.accepts("A"));
        assert!(rx.try_recv().is_err());
        assert_eq!(stats.gaps, 0);
        assert_eq!(stats.retired, 1);
    }

    #[tokio::test]
    async fn pending_sink_write_is_not_cancelled_for_pause_and_timeout_retires_its_stream() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpSocket::new_v4().unwrap();
        listener.set_recv_buffer_size(1024).unwrap();
        listener.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = listener.listen(1).unwrap();
        let (mut runtime, mut rx) = fixture();
        Arc::make_mut(&mut runtime.options).ws_url =
            format!("ws://{}/", listener.local_addr().unwrap());
        // A deliberate large command fills the small receive window. The
        // fixture reads only its header, so Sink::send cannot complete.
        let huge = "A".repeat((4 << 20) - 4096);
        let (_assign, assigned) = watch::channel(Assignment {
            epoch: 1,
            symbols: vec![huge.clone()],
            incarnations: BTreeMap::from([(huge, 1)]),
        });
        let (control, controlled) = watch::channel(Control::default());
        let (cancel, stop) = watch::channel(false);
        let (progress, observed) = watch::channel(Stats::default());
        let source = tokio::spawn(run(0, runtime, assigned, controlled, stop, progress));
        let (stream, _) = listener.accept().await.unwrap();
        let mut server = tokio_tungstenite::accept_async(stream).await.unwrap();
        assert!(matches!(
            rx.recv().await.unwrap().input,
            Input::Begin { .. }
        ));
        let frame_length = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let mut header = [0u8; 2];
                server.get_mut().read_exact(&mut header).await.unwrap();
                let opcode = header[0] & 0x0f;
                let length = match header[1] & 0x7f {
                    126 => {
                        let mut bytes = [0; 2];
                        server.get_mut().read_exact(&mut bytes).await.unwrap();
                        u16::from_be_bytes(bytes) as u64
                    }
                    127 => {
                        let mut bytes = [0; 8];
                        server.get_mut().read_exact(&mut bytes).await.unwrap();
                        u64::from_be_bytes(bytes)
                    }
                    length => length as u64,
                };
                assert_ne!(header[1] & 0x80, 0);
                server.get_mut().read_exact(&mut [0; 4]).await.unwrap();
                if opcode == 1 {
                    break length;
                }
                assert_eq!(opcode, 9);
                assert_eq!(length, 0);
            }
        })
        .await
        .unwrap();
        assert!(frame_length > (4 << 20) - 4096);
        tokio::time::pause();
        control.send_modify(|c| c.pause = Some(request(1)));
        tokio::task::yield_now().await;
        assert!(
            rx.try_recv().is_err(),
            "pause must not interrupt a partially sent command"
        );
        tokio::time::advance(Duration::from_secs(10)).await;
        let invalid = rx.recv().await.unwrap().input;
        assert!(
            matches!(invalid, Input::Invalid { reason, .. } if reason.contains("write timed out"))
        );
        cancel.send(true).unwrap();
        source.await.unwrap().unwrap();
        assert_eq!(observed.borrow().handoff_pauses, 0);
        assert_eq!(observed.borrow().connects, 1);
        tokio::time::resume();
        let mut accepted_payload = 0u64;
        let mut bytes = [0; 65536];
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let count = server.get_mut().read(&mut bytes).await.unwrap();
                if count == 0 {
                    break;
                }
                accepted_payload += count as u64;
            }
        })
        .await
        .unwrap();
        assert!(
            accepted_payload < frame_length,
            "fixture did not force a partial write"
        );
        // No same-stream frame retry or Close was appended after cancellation.
        assert!(rx.try_recv().is_err());
        // This is a TCP half-close, not a new WebSocket application frame.
        let _ = server.get_mut().shutdown().await;
    }

    async fn run_paused_source(full_at_stop: bool) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut runtime, mut rx) = fixture();
        Arc::make_mut(&mut runtime.options).ws_url =
            format!("ws://{}/", listener.local_addr().unwrap());
        let assignment = ready_session(&runtime).assignment;
        let (_assign, assigned) = watch::channel(assignment);
        let (control, controlled) = watch::channel(Control::default());
        let (cancel, stop) = watch::channel(false);
        let (progress, observed) = watch::channel(Stats::default());
        let relay = runtime.relay.clone();
        let source = tokio::spawn(run(0, runtime, assigned, controlled, stop, progress));
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let c = command(&mut ws).await;
        for event in [
            serde_json::json!({"id":c["id"], "type":"subscribed", "msg":{"sid":7}}),
            serde_json::json!({"type":"orderbook_snapshot", "sid":7, "seq":1, "sending_ts_ms":1700000000000i64,
                "msg":{"market_ticker":"A", "clock":101, "yes_dollars_fp":[["0.4","10"]]}}),
        ] {
            ws.send(Message::Text(event.to_string().into()))
                .await
                .unwrap();
        }
        while !matches!(rx.recv().await.unwrap().input, Input::Data { .. }) {}
        control.send_modify(|c| c.pause = Some(request(1)));
        assert!(matches!(
            rx.recv().await.unwrap().input,
            Input::PauseStarted { nonce: 1, .. }
        ));
        if full_at_stop {
            for request in 0..512 {
                relay
                    .try_send(Input::Fence {
                        source: Source {
                            replica: 0,
                            generation: 1,
                        },
                        request,
                        at: Utc::now(),
                    })
                    .unwrap();
            }
            cancel.send(true).unwrap();
            tokio::time::timeout(Duration::from_secs(1), source)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            for _ in 0..512 {
                assert!(matches!(next(&mut rx), Input::Fence { .. }));
            }
            assert!(rx.try_recv().is_err());
            // Forced retirement deliberately drops rather than appending a
            // Close frame while the ordered End marker cannot be admitted.
            while let Some(frame) = ws.next().await {
                if frame.is_err() {
                    break;
                }
            }
        } else {
            ws.send(Message::Text(delta(2).into())).await.unwrap();
            control.send_modify(|c| {
                c.pause = None;
                c.pause_release = Some(PauseId {
                    generation: 1,
                    nonce: 1,
                });
            });
            assert!(matches!(
                rx.recv().await.unwrap().input,
                Input::PauseEnded {
                    nonce: 1,
                    reason: PauseEnd::Released,
                    ..
                }
            ));
            assert!(matches!(
                rx.recv().await.unwrap().input,
                Input::Data {
                    incarnation: 1,
                    source: Source { generation: 1, .. },
                    ..
                }
            ));
            cancel.send(true).unwrap();
            while let Some(frame) = ws.next().await {
                match frame.unwrap() {
                    Message::Ping(p) => ws.send(Message::Pong(p)).await.unwrap(),
                    Message::Close(_) => {
                        ws.flush().await.unwrap();
                        break;
                    }
                    other => panic!("unexpected native resume frame: {other:?}"),
                }
            }
            tokio::time::timeout(Duration::from_secs(1), source)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        assert_eq!(observed.borrow().handoff_pauses, 1);
        assert_eq!(observed.borrow().gaps, 0);
    }

    /// One real `run` supervisor plus its localhost native exchange. The test
    /// body plays Owner: it reads the shared shard FIFO and writes controls.
    struct Supervised {
        listener: tokio::net::TcpListener,
        rx: tokio::sync::mpsc::Receiver<relay::Admitted>,
        relay: relay::Sender,
        control: watch::Sender<Control>,
        cancel: watch::Sender<bool>,
        observed: watch::Receiver<Stats>,
        source: tokio::task::JoinHandle<anyhow::Result<()>>,
        _assign: watch::Sender<Assignment>,
    }

    type Exchange = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;

    async fn serve_native(listener: &tokio::net::TcpListener) -> Exchange {
        let (stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("source did not dial")
            .unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let c = command(&mut ws).await;
        assert_eq!(c["cmd"], "subscribe");
        for event in [
            serde_json::json!({"id":c["id"], "type":"subscribed", "msg":{"sid":7}}),
            serde_json::json!({"type":"orderbook_snapshot", "sid":7, "seq":1, "sending_ts_ms":1700000000000i64,
                "msg":{"market_ticker":"A", "clock":101, "yes_dollars_fp":[["0.4","10"]]}}),
        ] {
            ws.send(Message::Text(event.to_string().into()))
                .await
                .unwrap();
        }
        ws
    }

    async fn recv(rx: &mut tokio::sync::mpsc::Receiver<relay::Admitted>) -> Input {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("source FIFO stalled")
            .expect("source FIFO closed")
            .input
    }

    async fn supervised(bytes: Arc<tokio::sync::Semaphore>) -> (Supervised, Exchange) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut runtime, mut rx) = fixture_with_bytes(bytes);
        Arc::make_mut(&mut runtime.options).ws_url =
            format!("ws://{}/", listener.local_addr().unwrap());
        let assignment = ready_session(&runtime).assignment;
        let (assign, assigned) = watch::channel(assignment);
        let (control, controlled) = watch::channel(Control::default());
        let (cancel, stop) = watch::channel(false);
        let (progress, observed) = watch::channel(Stats::default());
        let relay = runtime.relay.clone();
        let source = tokio::spawn(run(0, runtime, assigned, controlled, stop, progress));
        let ws = serve_native(&listener).await;
        assert!(matches!(
            recv(&mut rx).await,
            Input::Begin {
                source: Source { generation: 1, .. },
                ..
            }
        ));
        while !matches!(recv(&mut rx).await, Input::Data { .. }) {}
        (
            Supervised {
                listener,
                rx,
                relay,
                control,
                cancel,
                observed,
                source,
                _assign: assign,
            },
            ws,
        )
    }

    /// The peer replica shares this shard FIFO; its admitted input fills it.
    fn fill_from_peer(relay: &relay::Sender) {
        for request in 0..512 {
            relay
                .try_send(Input::Fence {
                    source: Source {
                        replica: 1,
                        generation: 1,
                    },
                    request,
                    at: Utc::now(),
                })
                .unwrap();
        }
        assert!(
            relay
                .try_send(Input::Fence {
                    source: Source {
                        replica: 1,
                        generation: 1
                    },
                    request: 512,
                    at: Utc::now(),
                })
                .is_err(),
            "fixture must saturate the 512-item shard FIFO"
        );
    }

    async fn drain_peer(rx: &mut tokio::sync::mpsc::Receiver<relay::Admitted>) {
        for _ in 0..512 {
            assert!(matches!(
                recv(rx).await,
                Input::Fence {
                    source: Source { replica: 1, .. },
                    ..
                }
            ));
        }
    }

    async fn still_supervising(source: &mut tokio::task::JoinHandle<anyhow::Result<()>>) {
        if let Ok(ended) = tokio::time::timeout(Duration::from_millis(300), source).await {
            panic!("source supervisor exited under shard pressure: {ended:?}");
        }
    }

    async fn close_echo(ws: &mut Exchange) {
        while let Some(frame) = ws.next().await {
            match frame.unwrap() {
                Message::Ping(p) => ws.send(Message::Pong(p)).await.unwrap(),
                Message::Close(_) => {
                    ws.flush().await.unwrap();
                    return;
                }
                _ => {}
            }
        }
    }

    fn snapshot_frame(seq: u64, clock: u64) -> String {
        serde_json::json!({"type":"orderbook_snapshot", "sid":7, "seq":seq,
            "sending_ts_ms":1700000000000i64,
            "msg":{"market_ticker":"A", "clock":clock, "yes_dollars_fp":[["0.4","10"]]}})
        .to_string()
    }

    fn snapshot_clock(input: &Input) -> Option<u64> {
        match input {
            Input::Data {
                event: BookEvent::Snapshot(snapshot),
                ..
            } => snapshot.clock,
            _ => None,
        }
    }

    /// The Owner's validity epoch leans on this per-source order. A frame read
    /// before a refresh is consumed precedes that refresh's Invalid, even when
    /// the Owner publishes the control while Source is parked on a full relay.
    /// The answer to the command issued afterwards follows the marker.
    #[tokio::test]
    async fn refresh_invalid_separates_frames_read_before_the_control_from_its_answer() {
        let bytes = Arc::new(tokio::sync::Semaphore::new(1 << 20));
        let (mut s, mut ws) = supervised(bytes.clone()).await;
        fill_from_peer(&s.relay);
        let before_frame = bytes.available_permits();
        ws.send(Message::Text(snapshot_frame(2, 150).into()))
            .await
            .unwrap();
        // Only this Source can acquire bytes now: peer inputs fill the FIFO,
        // and no other producer runs. Its retained-byte permit proves the
        // frame was read and is parked on item admission before the control.
        tokio::time::timeout(Duration::from_secs(5), async {
            while bytes.available_permits() >= before_frame {
                assert!(
                    !s.source.is_finished(),
                    "Source ended before frame admission"
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Source did not park the pre-control frame on the full FIFO");
        s.control.send_modify(|c| {
            c.refresh.insert(
                "A".into(),
                crate::relay::Refresh {
                    incarnation: 1,
                    generation: 1,
                    nonce: 1,
                },
            );
        });
        drain_peer(&mut s.rx).await;

        assert_eq!(
            snapshot_clock(&recv(&mut s.rx).await),
            Some(150),
            "a frame read before the control moved behind its Invalid"
        );
        assert!(matches!(
            recv(&mut s.rx).await,
            Input::Invalid {
                source: Source { replica: 0, generation: 1 },
                symbols: Some(ref symbols),
                ref reason,
            } if symbols == &["A".to_owned()] && reason == "requested snapshot refresh"
        ));

        let c = command(&mut ws).await;
        assert_eq!(c["params"]["action"], "get_snapshot");
        ws.send(Message::Text(snapshot_frame(3, 160).into()))
            .await
            .unwrap();
        assert_eq!(snapshot_clock(&recv(&mut s.rx).await), Some(160));
        assert_eq!(s.observed.borrow().gaps, 0);

        s.cancel.send(true).unwrap();
        close_echo(&mut ws).await;
        tokio::time::timeout(Duration::from_secs(2), s.source)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    /// Live failure shape (shard 2, 2026-11-08): lease expires, the resumed tail
    /// and the peer fill the FIFO, then Owner's expiry cleanup coalesces to a
    /// release-only watch. Source already emitted this nonce's ordered end.
    #[tokio::test]
    async fn expired_lease_ignores_a_coalesced_release_for_its_consumed_nonce_at_full_relay() {
        let (mut s, mut ws) = supervised(Arc::new(tokio::sync::Semaphore::new(1 << 20))).await;
        s.control.send_modify(|c| {
            c.pause = Some(PauseRequest {
                max_duration: Duration::from_millis(200),
                ..request(1)
            })
        });
        assert!(matches!(
            recv(&mut s.rx).await,
            Input::PauseStarted { nonce: 1, .. }
        ));
        ws.send(Message::Text(delta(2).into())).await.unwrap();
        assert!(matches!(
            recv(&mut s.rx).await,
            Input::PauseEnded {
                nonce: 1,
                reason: PauseEnd::Expired,
                ..
            }
        ));
        // The resumed backlog follows its ordered end marker.
        assert!(matches!(
            recv(&mut s.rx).await,
            Input::Data {
                source: Source { generation: 1, .. },
                ..
            }
        ));
        fill_from_peer(&s.relay);
        // Owner's lease cleanup and an unrelated fence coalesce in one watch.
        s.control.send_modify(|c| {
            c.pause = None;
            c.pause_release = Some(PauseId {
                generation: 1,
                nonce: 1,
            });
            c.fence = 1;
        });
        still_supervising(&mut s.source).await;
        drain_peer(&mut s.rx).await;
        // No duplicate terminal marker: the fence is the next ordered input.
        assert!(matches!(
            recv(&mut s.rx).await,
            Input::Fence {
                source: Source {
                    replica: 0,
                    generation: 1
                },
                request: 1,
                ..
            }
        ));
        ws.send(Message::Text(delta(3).into())).await.unwrap();
        assert!(matches!(
            recv(&mut s.rx).await,
            Input::Data {
                source: Source { generation: 1, .. },
                ..
            }
        ));
        let stats = s.observed.borrow().clone();
        assert_eq!(
            (
                stats.connects,
                stats.gaps,
                stats.handoff_pauses,
                stats.pause_expirations
            ),
            (1, 0, 1, 1)
        );
        s.cancel.send(true).unwrap();
        close_echo(&mut ws).await;
        tokio::time::timeout(Duration::from_secs(2), s.source)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(s.rx.try_recv().is_err());
    }

    #[derive(Clone, Copy, Debug)]
    enum Pressure {
        Items,
        Bytes,
    }

    /// A pause marker that cannot be admitted retires its stream. The fault
    /// report then hits the same shard pressure; it must wait (bounded, after
    /// the stream is gone) rather than end this replica permanently.
    async fn retired_stream_reports_after_pressure_then_reconnects(pressure: Pressure) {
        let bytes = Arc::new(tokio::sync::Semaphore::new(1 << 20));
        let (mut s, mut ws) = supervised(bytes.clone()).await;
        let held = match pressure {
            Pressure::Items => {
                fill_from_peer(&s.relay);
                None
            }
            Pressure::Bytes => Some(
                bytes
                    .clone()
                    .try_acquire_many_owned(bytes.available_permits() as u32)
                    .unwrap(),
            ),
        };
        s.control.send_modify(|c| c.pause = Some(request(1)));
        // Forced retirement drops TCP without appending a Close frame.
        let ended = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("retired stream stayed open");
        assert!(
            matches!(ended, None | Some(Err(_))),
            "{pressure:?}: {ended:?}"
        );
        still_supervising(&mut s.source).await;
        match held {
            None => drain_peer(&mut s.rx).await,
            Some(permit) => {
                assert!(s.rx.try_recv().is_err());
                drop(permit);
            }
        }
        let invalid = recv(&mut s.rx).await;
        assert!(
            matches!(&invalid, Input::Invalid {
                source: Source { replica: 0, generation: 1 },
                symbols: None,
                reason,
            } if reason.contains("pause marker admission failed")),
            "{pressure:?}: {invalid:?}"
        );
        // Recovery uses the ordinary reconnect path and a fresh generation.
        let mut replacement = serve_native(&s.listener).await;
        assert!(matches!(
            recv(&mut s.rx).await,
            Input::Begin {
                source: Source {
                    replica: 0,
                    generation: 2
                },
                admission: 0
            }
        ));
        while !matches!(
            recv(&mut s.rx).await,
            Input::Data {
                source: Source { generation: 2, .. },
                ..
            }
        ) {}
        let stats = s.observed.borrow().clone();
        assert_eq!((stats.connects, stats.handoff_pauses), (2, 0));
        s.cancel.send(true).unwrap();
        close_echo(&mut replacement).await;
        tokio::time::timeout(Duration::from_secs(2), s.source)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn retired_stream_fault_report_waits_for_item_capacity_and_reconnects() {
        retired_stream_reports_after_pressure_then_reconnects(Pressure::Items).await;
    }

    #[tokio::test]
    async fn retired_stream_fault_report_waits_for_byte_budget_and_reconnects() {
        retired_stream_reports_after_pressure_then_reconnects(Pressure::Bytes).await;
    }

    #[tokio::test]
    async fn a_closed_owner_relay_during_retirement_is_a_terminal_error() {
        let (mut s, mut ws) = supervised(Arc::new(tokio::sync::Semaphore::new(1 << 20))).await;
        fill_from_peer(&s.relay);
        s.control.send_modify(|c| c.pause = Some(request(1)));
        let ended = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("retired stream stayed open");
        assert!(matches!(ended, None | Some(Err(_))), "{ended:?}");
        still_supervising(&mut s.source).await;
        s.rx.close();
        let error = tokio::time::timeout(Duration::from_secs(1), s.source)
            .await
            .expect("closed Owner relay did not end the bounded fault admission")
            .unwrap()
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("retired transport could not report its fault"),
            "{error:#}"
        );
        drain_peer(&mut s.rx).await;
        assert!(
            s.rx.try_recv().is_err(),
            "closed relay admitted a fault report"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(200), s.listener.accept())
                .await
                .is_err(),
            "a stopped supervisor dialed again"
        );
        assert_eq!(s.observed.borrow().connects, 1);
    }

    /// R2 policy (Codex): a stream retired AT the requested cut, already
    /// dropped, is counted and shut down cleanly instead of failing the run.
    #[tokio::test]
    async fn graceful_stop_counts_a_dropped_paused_transport_and_returns_cleanly() {
        let (mut s, mut ws) = supervised(Arc::new(tokio::sync::Semaphore::new(1 << 20))).await;
        s.control.send_modify(|c| c.pause = Some(request(1)));
        assert!(matches!(
            recv(&mut s.rx).await,
            Input::PauseStarted { nonce: 1, .. }
        ));
        fill_from_peer(&s.relay);
        s.cancel.send(true).unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(1), s.source)
            .await
            .expect("stop with a blocked end marker did not join promptly")
            .unwrap();
        assert!(
            joined.is_ok(),
            "cutoff retirement failed the run: {joined:?}"
        );
        // Dropped without a Close frame: an unacknowledged close, counted.
        let ended = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("retired stream stayed open");
        assert!(matches!(ended, None | Some(Err(_))), "{ended:?}");
        drain_peer(&mut s.rx).await;
        assert!(s.rx.try_recv().is_err(), "no marker after the cut");
        let stats = s.observed.borrow().clone();
        assert_eq!((stats.unacknowledged_closes, stats.handoff_pauses), (1, 1));
    }

    /// A stream retired BEFORE the cut still owes Owner its fault report: if
    /// stop is abandoned here, a frozen publisher is silently claimed ready.
    #[tokio::test]
    async fn a_live_retirement_report_pending_at_stop_still_reaches_the_owner() {
        let (mut s, mut ws) = supervised(Arc::new(tokio::sync::Semaphore::new(1 << 20))).await;
        fill_from_peer(&s.relay);
        s.control.send_modify(|c| c.pause = Some(request(1)));
        let ended = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("retired stream stayed open");
        assert!(matches!(ended, None | Some(Err(_))), "{ended:?}");
        still_supervising(&mut s.source).await;
        s.cancel.send(true).unwrap();
        // Stop does not discard the already-owed report.
        still_supervising(&mut s.source).await;
        drain_peer(&mut s.rx).await;
        let invalid = recv(&mut s.rx).await;
        assert!(
            matches!(&invalid, Input::Invalid {
                source: Source { replica: 0, generation: 1 },
                symbols: None,
                reason,
            } if reason.contains("pause marker admission failed")),
            "{invalid:?}"
        );
        tokio::time::timeout(Duration::from_secs(1), s.source)
            .await
            .expect("source did not finish after its report")
            .unwrap()
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(200), s.listener.accept())
                .await
                .is_err(),
            "a stopped supervisor dialed again"
        );
        assert_eq!(s.observed.borrow().unacknowledged_closes, 0);
    }

    /// Real terminal errors before the cut stay run errors.
    #[tokio::test]
    async fn a_fatal_exchange_rejection_before_stop_remains_a_run_error() {
        let (s, mut ws) = supervised(Arc::new(tokio::sync::Semaphore::new(1 << 20))).await;
        ws.send(Message::Text(
            serde_json::json!({"type":"error", "msg":{"code":6, "msg":"fixture fatal"}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        close_echo(&mut ws).await;
        let error = tokio::time::timeout(Duration::from_secs(2), s.source)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        let _ = s.cancel.send(true);
        assert!(
            format!("{error:#}").contains("exchange rejected command 6"),
            "{error:#}"
        );
    }
}

#[cfg(test)]
mod admission_tests;
