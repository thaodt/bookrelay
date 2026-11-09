//! One logical shard owns N independent books. A producer fence defines the
//! promotion cut: all preceding standby input is folded into the snapshot.
use crate::{
    book::OrderBook,
    collector::{ShardStats, WorkerPhase},
    exchange::{Contract, MembershipStrategy, OrderingStrategy},
    health::{self, Activity},
    model::BookEvent,
    output::{OrderingEvidence, Output, Record, SnapshotReason, SourceStamp},
    registry::Assignment,
    relay::{self, Control, Input, Source},
    source,
};
use anyhow::{Context, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Semaphore, watch},
    task::JoinSet,
    time::{Instant, MissedTickBehavior},
};

mod integrity;
mod maintenance;
use maintenance::{CoverageProbe, PauseReservation, Planned, Rotation, ScheduledRecovery};

/// A server snapshot covers the ENTIRE clock. A local cut can cover only a
/// prefix of that clock, so it must retain its ordinal instead of claiming completeness.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frontier {
    pub clock: u64,
    pub ordinal: u64,
    pub complete: bool,
}
impl Frontier {
    pub fn snapshot(clock: u64) -> Self {
        Self {
            clock,
            ordinal: 0,
            complete: true,
        }
    }
    pub fn delta(previous: Option<Self>, clock: u64) -> anyhow::Result<Self> {
        if let Some(p) = previous {
            ensure!(clock >= p.clock, "market clock regressed");
            ensure!(
                clock != p.clock || !p.complete,
                "delta at/below complete snapshot clock"
            );
            if clock == p.clock {
                return Ok(Self {
                    clock,
                    ordinal: p.ordinal.checked_add(1).context("clock ordinal overflow")?,
                    complete: false,
                });
            }
        }
        Ok(Self {
            clock,
            ordinal: 0,
            complete: false,
        })
    }
    pub fn covers(self, floor: Self) -> bool {
        self.clock > floor.clock
            || self.clock == floor.clock
                && (self.complete || !floor.complete && self.ordinal >= floor.ordinal)
    }
}

struct Book {
    value: OrderBook,
    frontier: Option<Frontier>,
    activity: Activity,
    min_generation: u64,
    min_admission: u64,
    rejected_baselines: u32,
    recovery: Option<(crate::relay::Resubscribe, bool)>,
    /// Frozen recovery goal and when the book last became valid against it.
    /// The clock belongs to one validity epoch: a snapshot reviving an invalid
    /// book restarts it, a snapshot onto a still-valid book never does.
    catchup: Option<(Frontier, Instant)>,
    initialization_failures: u32,
    /// Frozen goal of one non-destructive request. Control holds its scoped
    /// identity; this book is itself generation/incarnation fenced. An ordinary
    /// delta below the goal must not clear it and allow duplicate requests.
    probe: Option<Frontier>,
    /// A covering probe or exhausted started health recovery quarantined this
    /// standby while another publisher was ready. Preserve that verdict across
    /// reconnects until a genuinely usable baseline clears it.
    probe_quarantined: bool,
}
impl Book {
    fn new(symbol: &str) -> Self {
        Self {
            value: OrderBook::new(symbol.into()),
            frontier: None,
            activity: Activity::default(),
            min_generation: 1,
            min_admission: 0,
            rejected_baselines: 0,
            recovery: None,
            catchup: None,
            initialization_failures: 0,
            probe: None,
            probe_quarantined: false,
        }
    }
}
struct Feed {
    pause: Option<PauseReservation>,
    applied: Option<BTreeMap<String, u64>>,
    next_renewal: Instant,
    generation: u64,
    admission: u64,
    min_generation: u64,
    books: HashMap<String, Book>,
    wire: BTreeSet<String>,
    control: watch::Sender<Control>,
    pending_fence: Option<(Source, u64, Instant)>,
}
struct Selection {
    unverified_cut: bool,
    incarnation: u64,
    active: Option<Source>,
    previous: Option<Source>,
    floor: Option<Frontier>,
    epoch: u64,
    ever: bool,
    recovery: bool,
    candidate: Option<usize>,
    peer_ahead: Option<Instant>,
    peer_mark: Option<Frontier>,
    behind_since: Option<Instant>,
    /// Actual publisher loss, distinct from a standby acquisition or retry
    /// observation. Repeated Invalid markers cannot restart this timer.
    unpublished_since: Option<Instant>,
    continuity_epoch: u64,
}
impl Selection {
    fn new(incarnation: u64) -> Self {
        Self {
            unverified_cut: false,
            incarnation,
            active: None,
            previous: None,
            floor: None,
            epoch: 0,
            ever: false,
            recovery: false,
            candidate: None,
            peer_ahead: None,
            peer_mark: None,
            behind_since: None,
            unpublished_since: None,
            continuity_epoch: 0,
        }
    }
}

enum Emit {
    Update(crate::util::dtos::challenge_orderbook::PriceLevelUpdateDomain),
    Snapshot {
        symbol: String,
        source: Source,
        reason: SnapshotReason,
        at: DateTime<Utc>,
        stamp: SourceStamp,
    },
}
struct Owner {
    contract: Contract,
    needs_renewal: BTreeSet<usize>,
    needs_refresh: BTreeSet<usize>,
    feeds: Vec<Feed>,
    selected: HashMap<String, Selection>,
    assignment: Assignment,
    stats: ShardStats,
    next_fence: u64,
    next_pause: u64,
    lag: Duration,
    replicas: usize,
    health: health::Options,
    origin: Instant,
    health_epoch: Option<u64>,
    rotation_due: Option<Instant>,
    rotation_slot: usize,
    rotation: Option<Rotation>,
    quiescing: bool,
    recovery_pacer: Option<Arc<crate::collector::Pacer>>,
    scheduled_recovery: Option<ScheduledRecovery>,
    planned: Option<Planned>,
    coverage: Option<CoverageProbe>,
    maintenance_deferred_until: Option<Instant>,
    reset_restarts: BTreeSet<usize>,
    waiting_catchup: BTreeSet<(usize, String)>,
}

impl Owner {
    fn new(
        assignment: Assignment,
        controls: Vec<watch::Sender<Control>>,
        lag: Duration,
        contract: Contract,
    ) -> Self {
        let selected = assignment
            .incarnations
            .iter()
            .map(|(s, &v)| (s.clone(), Selection::new(v)))
            .collect();
        let feeds = controls
            .into_iter()
            .map(|control| Feed {
                pause: None,
                applied: None,
                next_renewal: Instant::now(),
                generation: 0,
                admission: 0,
                min_generation: 1,
                books: assignment
                    .symbols
                    .iter()
                    .map(|s| (s.clone(), Book::new(s)))
                    .collect(),
                wire: BTreeSet::new(),
                control,
                pending_fence: None,
            })
            .collect::<Vec<_>>();
        let replicas = feeds.len();
        Self {
            contract,
            needs_renewal: BTreeSet::new(),
            needs_refresh: BTreeSet::new(),
            feeds,
            selected,
            assignment,
            stats: ShardStats::default(),
            next_fence: 0,
            next_pause: 0,
            lag,
            replicas,
            health: health::Options::default(),
            origin: Instant::now(),
            health_epoch: None,
            rotation_due: None,
            rotation_slot: 0,
            rotation: None,
            quiescing: false,
            recovery_pacer: None,
            scheduled_recovery: None,
            planned: None,
            coverage: None,
            maintenance_deferred_until: None,
            reset_restarts: BTreeSet::new(),
            waiting_catchup: BTreeSet::new(),
        }
    }
    fn sync(&mut self, assignment: Assignment) {
        if assignment.incarnations != self.assignment.incarnations {
            self.cancel_planned();
            self.cancel_pauses();
        }
        for feed in &mut self.feeds {
            feed.books
                .retain(|s, _| assignment.incarnations.contains_key(s));
            feed.wire
                .retain(|s| assignment.incarnations.contains_key(s));
            let mut control = feed.control.borrow().clone();
            control
                .refresh
                .retain(|s, request| assignment.incarnations.get(s) == Some(&request.incarnation));
            control
                .resubscribe
                .retain(|s, r| assignment.incarnations.get(s) == Some(&r.incarnation));
            control
                .probe
                .retain(|s, p| assignment.incarnations.get(s) == Some(&p.incarnation));
            feed.control.send_replace(control);
            for (symbol, version) in &assignment.incarnations {
                if self.assignment.incarnations.get(symbol) != Some(version) {
                    feed.books.insert(symbol.clone(), Book::new(symbol));
                }
            }
        }
        self.selected
            .retain(|s, _| assignment.incarnations.contains_key(s));
        for (symbol, &version) in &assignment.incarnations {
            if self
                .selected
                .get(symbol)
                .is_none_or(|s| s.incarnation != version)
            {
                self.selected
                    .insert(symbol.clone(), Selection::new(version));
            }
        }
        self.assignment = assignment;
        if self.contract.membership == MembershipStrategy::FreshSession {
            for (slot, feed) in self.feeds.iter().enumerate() {
                if let Some(applied) = &feed.applied {
                    if applied == &self.assignment.incarnations {
                        self.needs_renewal.remove(&slot);
                    } else {
                        self.needs_renewal.insert(slot);
                    }
                }
            }
        }
        self.waiting_catchup.retain(|(slot, symbol)| {
            self.feeds[*slot]
                .books
                .get(symbol)
                .is_some_and(|book| book.catchup.is_some())
        });
        if self.selected.is_empty() {
            self.cancel_planned();
            self.rotation = None;
            self.coverage = None;
            self.reset_restarts.clear();
            self.needs_renewal.clear();
            self.needs_refresh.clear();
        }
    }
    fn accepts(&self, source: Source) -> bool {
        self.feeds.get(source.replica).is_some_and(|f| {
            source.generation == f.generation && source.generation >= f.min_generation
        })
    }
    fn valid_book(&self, replica: usize, symbol: &str) -> bool {
        let f = &self.feeds[replica];
        f.generation >= f.min_generation
            && f.wire.contains(symbol)
            && f.books.get(symbol).is_some_and(|b| {
                b.value.is_valid()
                    && f.generation >= b.min_generation
                    && f.admission >= b.min_admission
                    && b.recovery.is_none_or(|(_, started)| started)
            })
    }
    fn ready(&self, replica: usize, symbol: &str) -> bool {
        self.valid_book(replica, symbol) && self.feeds[replica].books[symbol].catchup.is_none()
    }
    /// Non-destructive, identity-scoped request for a valid admitted native
    /// book. An initial standby can still be acquiring its frozen floor;
    /// that does not make it publishable. Its book and ordered tail stay.
    /// The answer replaces the book only through the ordinary Data checks
    /// (regression, current-floor catch-up). One outstanding probe per book.
    /// Public FreshSession has no snapshot request; it never probes.
    fn probe(&mut self, replica: usize, symbol: &str) -> bool {
        if self.quiescing
            || self.contract.membership != MembershipStrategy::Acknowledged
            || !self.valid_book(replica, symbol)
            || self.feeds[replica].books[symbol].probe.is_some()
        {
            return false;
        }
        let Some(goal) = self.selected[symbol].floor else {
            return false;
        };
        let incarnation = self.assignment.incarnations[symbol];
        let feed = &mut self.feeds[replica];
        let generation = feed.generation;
        feed.control.send_modify(|c| {
            let nonce = c
                .probe
                .get(symbol)
                .map_or(0, |p| p.nonce)
                .checked_add(1)
                .expect("probe nonce overflow");
            let request = crate::relay::Probe {
                incarnation,
                generation,
                nonce,
            };
            c.probe.insert(symbol.into(), request);
        });
        feed.books.get_mut(symbol).unwrap().probe = Some(goal);
        true
    }
    fn acquire_standby(&mut self, replica: usize, symbol: &str) {
        let book = &self.feeds[replica].books[symbol];
        if book.catchup.is_some()
            && book.probe.is_none()
            && book.rejected_baselines == 0
            && book.recovery.is_none()
            && self.selected[symbol].active.is_some_and(|publisher| {
                publisher.replica != replica && self.ready(publisher.replica, symbol)
            })
        {
            self.probe(replica, symbol);
        }
    }
    fn refresh(&mut self, replica: usize, symbol: &str) {
        self.cancel_planned();
        let book = self.feeds[replica].books.get_mut(symbol).unwrap();
        book.value.invalidate();
        book.activity.invalidate();
        book.probe = None;
        // Explicit semantic recovery is distinct from the quarantined probe;
        // its existing rejection budget stays intact.
        book.probe_quarantined = false;
        if self.contract.membership == MembershipStrategy::FreshSession {
            self.needs_refresh.insert(replica);
            return;
        }
        let incarnation = self.assignment.incarnations[symbol];
        let generation = self.feeds[replica].generation;
        self.feeds[replica].control.send_modify(|c| {
            let nonce = c
                .refresh
                .get(symbol)
                .map_or(0, |r| r.nonce)
                .checked_add(1)
                .expect("refresh nonce overflow");
            c.refresh.insert(
                symbol.into(),
                crate::relay::Refresh {
                    incarnation,
                    generation,
                    nonce,
                },
            );
        });
    }
    fn inject_fault(&mut self) -> anyhow::Result<Option<Source>> {
        if !self
            .selected
            .keys()
            .all(|symbol| (0..self.replicas).all(|slot| self.survivor_ready(slot, symbol)))
        {
            return Ok(None);
        }
        let Some(source) = self.selected.values().find_map(|s| s.active) else {
            return Ok(None);
        };
        let generation = source
            .generation
            .checked_add(1)
            .context("fault generation overflow")?;
        let restart = self.feeds[source.replica]
            .control
            .borrow()
            .restart
            .checked_add(1)
            .context("fault restart overflow")?;
        self.cancel_planned();
        // Claim unavailability in the same Owner turn as the readiness check.
        // Maintenance cannot reserve a source whose owned close is already
        // committed, even before its asynchronous Invalid marker arrives.
        self.invalidate(source, None);
        self.feeds[source.replica].min_generation = generation;
        let symbols: Vec<_> = self.selected.keys().cloned().collect();
        for symbol in symbols {
            self.choose(&symbol)?;
        }
        self.feeds[source.replica]
            .control
            .send_modify(|c| c.restart = restart);
        self.stats.injected_disconnects += 1;
        Ok(Some(source))
    }
    fn fence(&mut self, replica: usize) -> anyhow::Result<()> {
        if self.feeds[replica].pending_fence.is_some() {
            return Ok(());
        }
        self.next_fence = self
            .next_fence
            .checked_add(1)
            .context("fence request overflow")?;
        let source = Source {
            replica,
            generation: self.feeds[replica].generation,
        };
        self.feeds[replica].pending_fence = Some((source, self.next_fence, Instant::now()));
        self.feeds[replica]
            .control
            .send_modify(|c| c.fence = self.next_fence);
        Ok(())
    }
    fn invalidate(&mut self, source: Source, symbols: Option<&[String]>) {
        // A peer's logical quarantine is already enough to revoke a planned
        // survivor claim; do not wait for a later transport Invalid marker.
        self.cancel_planned();
        let affected: Vec<_> = symbols
            .map(|v| v.to_vec())
            .unwrap_or_else(|| self.assignment.symbols.clone());
        for symbol in affected {
            if let Some(book) = self.feeds[source.replica].books.get_mut(&symbol) {
                book.value.invalidate();
                book.activity.invalidate();
                book.probe = None;
            }
            if let Some(s) = self.selected.get_mut(&symbol) {
                if s.active == Some(source) {
                    s.previous = s.active.take();
                    s.candidate = None;
                    // The five-second failover bound starts when publication
                    // is lost. A block recorded while a publisher still served
                    // (planned or optional peer-ahead) is not time without one.
                    s.behind_since = None;
                    s.unpublished_since = Some(Instant::now());
                }
                if s.candidate == Some(source.replica) {
                    s.candidate = None;
                }
                if s.ever
                    && s.active.is_none()
                    && !s.recovery
                    && !(0..self.replicas).any(|i| self.ready(i, &symbol))
                {
                    self.selected.get_mut(&symbol).unwrap().recovery = true;
                    self.stats.continuity_gaps += 1;
                }
            }
        }
    }
    /// A complete snapshot outranks a delta prefix of its own clock, but rank is
    /// not evidence of missed data: equal levels at one clock are one state.
    fn restates(&self, symbol: &str, complete: usize, prefix: usize) -> bool {
        let (complete, prefix) = (
            &self.feeds[complete].books[symbol],
            &self.feeds[prefix].books[symbol],
        );
        complete
            .frontier
            .zip(prefix.frontier)
            .is_some_and(|(c, p)| c.complete && c.clock == p.clock)
            && complete.value.same_levels(&prefix.value)
    }
    fn note_progress(&mut self, symbol: &str, source: Source) {
        let next = self.feeds[source.replica].books[symbol].frontier;
        let state = &self.selected[symbol];
        let Some(active) = state.active else {
            return;
        };
        if active == source {
            let reached = state.peer_mark.zip(next).is_some_and(|(mark, next)| {
                next.covers(mark)
                    || (0..self.replicas).any(|peer| {
                        peer != active.replica
                            && self.feeds[peer].books[symbol].frontier == Some(mark)
                            && self.restates(symbol, peer, active.replica)
                    })
            });
            if reached {
                let state = self.selected.get_mut(symbol).unwrap();
                state.peer_mark = None;
                state.peer_ahead = None;
            }
        } else if state.peer_ahead.is_none()
            && state
                .floor
                .zip(next)
                .is_some_and(|(floor, next)| next.covers(floor) && next != floor)
            && !self.restates(symbol, source.replica, active.replica)
        {
            // Freeze this witness. Moving the target on every peer update would
            // make an ordinary fast peer look perpetually ahead.
            let state = self.selected.get_mut(symbol).unwrap();
            state.peer_mark = next;
            state.peer_ahead = Some(Instant::now());
        }
    }
    fn choose(&mut self, symbol: &str) -> anyhow::Result<()> {
        let state = &self.selected[symbol];
        if state.candidate.is_some() {
            return Ok(());
        }
        let candidate = if state.active.is_none() {
            (0..self.replicas).find(|&i| {
                !self.retiring_generation(i)
                    && self.feeds[i].pause.is_none()
                    && self.ready(i, symbol)
                    && (state.behind_since.is_none_or(|at| at.elapsed() >= self.lag)
                        || state
                            .floor
                            .zip(self.feeds[i].books[symbol].frontier)
                            .is_some_and(|(floor, next)| next.covers(floor)))
            })
        } else if state.peer_ahead.is_some_and(|t| t.elapsed() >= self.lag) {
            (0..self.replicas).find(|&i| {
                !self.retiring_generation(i)
                    && self.feeds[i].pause.is_none()
                    && Some(i) != state.active.map(|s| s.replica)
                    && self.ready(i, symbol)
                    && state
                        .floor
                        .zip(self.feeds[i].books[symbol].frontier)
                        .is_some_and(|(floor, next)| next.covers(floor) && next != floor)
            })
        } else {
            None
        };
        if let Some(i) = candidate {
            self.selected.get_mut(symbol).unwrap().candidate = Some(i);
            self.fence(i)?;
        }
        Ok(())
    }
    fn stamp(&self, symbol: &str, source: Source) -> SourceStamp {
        let selection = &self.selected[symbol];
        let frontier = self.feeds[source.replica].books[symbol].frontier;
        SourceStamp {
            evidence: match self.contract.ordering {
                OrderingStrategy::ComparableClock => OrderingEvidence::ComparableClock,
                OrderingStrategy::SnapshotOnly => OrderingEvidence::SnapshotOnly,
            },
            unverified_cut: selection.unverified_cut,
            replica: source.replica,
            generation: source.generation,
            output_epoch: selection.epoch,
            incarnation: selection.incarnation,
            continuity_epoch: selection.continuity_epoch,
            clock: frontier.map(|f| f.clock),
            ordinal: frontier.filter(|f| !f.complete).map(|f| f.ordinal),
            complete_clock: frontier.is_some_and(|f| f.complete),
        }
    }
    fn direct_baseline(&mut self, symbol: &str, source: Source, at: DateTime<Utc>) -> Option<Emit> {
        if self.retiring_generation(source.replica) || !self.ready(source.replica, symbol) {
            return None;
        }
        let frontier = self.feeds[source.replica].books[symbol].frontier;
        let state = &self.selected[symbol];
        if state.ever
            && (state.active.is_some()
                || state.previous.is_none_or(|p| p.replica != source.replica)
                || state
                    .floor
                    .is_some_and(|floor| frontier.is_none_or(|f| !f.covers(floor))))
        {
            return None;
        }
        let state = self.selected.get_mut(symbol).unwrap();
        state.unverified_cut =
            state.ever && self.contract.ordering == OrderingStrategy::SnapshotOnly;
        if state.unverified_cut {
            self.stats.unverified_transitions += 1;
        }
        let reason = if state.ever {
            SnapshotReason::Recovery
        } else {
            SnapshotReason::Initial
        };
        if state.ever && state.previous != Some(source) {
            self.stats.failovers += 1;
        }
        state.epoch = state.epoch.checked_add(1).expect("output epoch overflow");
        state.active = Some(source);
        state.previous = Some(source);
        state.floor = frontier;
        state.ever = true;
        state.candidate = None;
        state.recovery = false;
        state.peer_ahead = None;
        state.peer_mark = None;
        state.behind_since = None;
        state.unpublished_since = None;
        Some(Emit::Snapshot {
            symbol: symbol.into(),
            source,
            reason,
            at,
            stamp: self.stamp(symbol, source),
        })
    }
    fn promote(
        &mut self,
        source: Source,
        request: u64,
        at: DateTime<Utc>,
    ) -> anyhow::Result<Vec<Emit>> {
        let symbols: Vec<_> = self
            .selected
            .iter()
            .filter(|(_, s)| s.candidate == Some(source.replica))
            .map(|(s, _)| s.clone())
            .collect();
        let mut emitted = vec![];
        for symbol in symbols {
            if !self.ready(source.replica, &symbol) {
                self.selected.get_mut(&symbol).unwrap().candidate = None;
                continue;
            }
            let next = self.feeds[source.replica].books[&symbol].frontier;
            let floor = self.selected[&symbol].floor;
            if self.replicas > 1 && floor.is_some_and(|floor| next.is_none_or(|f| !f.covers(floor)))
            {
                self.stats.promotions_blocked += 1;
                let planned = self.planned_candidate(&symbol, source, request);
                let selection = self.selected.get_mut(&symbol).unwrap();
                selection.candidate = None;
                if planned {
                    // The planned target still publishes: this block is not
                    // time without a publisher and arms no failover timer.
                    self.defer_planned_fence();
                    continue;
                }
                if selection.active.is_none() {
                    let behind = *selection.behind_since.get_or_insert_with(Instant::now);
                    let unpublished = selection.unpublished_since.unwrap_or(behind);
                    if unpublished.elapsed() >= Duration::from_secs(5) {
                        // Honest bound, unchanged: no covering survivor in time.
                        self.reset_symbol(&symbol, "candidate cannot cover published frontier")?;
                    } else if floor.is_some_and(|floor| floor.complete)
                        || behind.elapsed() >= self.lag
                    {
                        // A valid survivor keeps its book, readiness and
                        // ordered tail while it asks for a covering snapshot:
                        // invalidating the only survivor would itself be the
                        // gap. No tail can reach a complete snapshot floor it
                        // does not cover, so ask at once; a delta floor first
                        // gets the replica grace to arrive by tail.
                        self.probe(source.replica, &symbol);
                    }
                }
                continue;
            }
            let planned = self.planned_candidate(&symbol, source, request);
            if planned
                && (!self.ready(source.replica, &symbol)
                    || self.feeds[source.replica].books[&symbol]
                        .activity
                        .recovering)
            {
                continue;
            }
            // choose() never selects a reserved/retiring feed, but a candidate
            // chosen before a planned reservation can still have its fence
            // answered afterwards. An optional peer-ahead switch onto that
            // target only defeats the handoff draining it (2026-11-08 scale).
            let reserved = self.feeds[source.replica].pause.is_some()
                || self.retiring_generation(source.replica);
            let selection = self.selected.get_mut(&symbol).unwrap();
            // If a healthy active source caught up while its peer fence was in
            // flight, retain it. Do not turn a transient skew into a switch.
            if !planned
                && selection.active.is_some()
                && (reserved
                    || selection
                        .peer_ahead
                        .is_none_or(|at| at.elapsed() < self.lag))
            {
                selection.candidate = None;
                continue;
            }
            let reason = if !selection.ever {
                SnapshotReason::Initial
            } else if selection.recovery || selection.previous == Some(source) {
                SnapshotReason::Recovery
            } else {
                SnapshotReason::Failover
            };
            selection.unverified_cut =
                selection.ever && self.contract.ordering == OrderingStrategy::SnapshotOnly;
            if selection.unverified_cut {
                self.stats.unverified_transitions += 1;
            }
            if selection.ever && selection.previous.or(selection.active) != Some(source) {
                self.stats.failovers += 1;
            }
            if floor.zip(next).is_some_and(|(old, new)| old != new) {
                self.stats.handoff_advances += 1;
            }
            selection.epoch = selection
                .epoch
                .checked_add(1)
                .context("output epoch overflow")?;
            selection.active = Some(source);
            selection.previous = Some(source);
            selection.floor = next;
            selection.ever = true;
            selection.candidate = None;
            selection.peer_ahead = None;
            selection.peer_mark = None;
            selection.behind_since = None;
            selection.unpublished_since = None;
            selection.recovery = false;
            let stamp = self.stamp(&symbol, source);
            emitted.push(Emit::Snapshot {
                symbol,
                source,
                reason,
                at,
                stamp,
            });
        }
        Ok(emitted)
    }
    fn input(&mut self, input: Input) -> anyhow::Result<Vec<Emit>> {
        let mut emitted = vec![];
        if let Input::Begin { source, admission } = input {
            let feed = &mut self.feeds[source.replica];
            if source.generation < feed.min_generation
                || source.generation <= feed.generation
                || admission != feed.control.borrow().restart
            {
                self.stats.stale_events += 1;
                return Ok(emitted);
            }
            let old = Source {
                replica: source.replica,
                generation: feed.generation,
            };
            self.cancel_planned();
            self.retire_pause(old);
            self.invalidate(old, None);
            let feed = &mut self.feeds[source.replica];
            feed.generation = source.generation;
            feed.applied = None;
            feed.admission = admission;
            feed.pending_fence = None;
            feed.wire.clear();
            for b in feed.books.values_mut() {
                b.value.invalidate();
                b.frontier = None;
                b.activity = Activity::default();
                b.recovery = None;
                b.probe = None;
            }
            return Ok(emitted);
        }
        // End resolves the exact reservation even after its generation has
        // been fenced for restart. A newer Begin still makes old Ends stale.
        if let Input::PauseEnded {
            source,
            nonce,
            reason,
        } = input
        {
            self.pause_ended(source, nonce, reason)?;
            return Ok(emitted);
        }
        let source = match &input {
            Input::Data { source, .. }
            | Input::Invalid { source, .. }
            | Input::Membership { source, .. }
            | Input::Fence { source, .. }
            | Input::PauseStarted { source, .. }
            | Input::PauseEnded { source, .. }
            | Input::Unavailable { source, .. }
            | Input::RecoveryStarted { source, .. } => *source,
            Input::Begin { .. } => unreachable!(),
        };
        if !self.accepts(source) {
            self.stats.stale_events += 1;
            return Ok(emitted);
        }
        match input {
            Input::PauseStarted { nonce, until, .. } => {
                self.pause_started(source, nonce, until)?;
            }
            Input::Unavailable {
                symbol,
                incarnation,
                ..
            } => {
                ensure!(
                    self.contract.membership == MembershipStrategy::FreshSession,
                    "unavailable dump on acknowledged membership"
                );
                if self
                    .selected
                    .get(&symbol)
                    .is_some_and(|s| s.incarnation == incarnation)
                {
                    let b = self.feeds[source.replica].books.get_mut(&symbol).unwrap();
                    b.initialization_failures = b.initialization_failures.saturating_add(1);
                    if b.initialization_failures <= self.health.max_resubscriptions {
                        self.needs_refresh.insert(source.replica);
                    }
                    self.replacement_unavailable(source.replica);
                } else {
                    self.stats.stale_events += 1;
                }
            }
            Input::RecoveryStarted { requests, .. } => {
                for (symbol, request) in requests {
                    if let Some(book) = self.feeds[source.replica].books.get_mut(&symbol)
                        && book.recovery == Some((request, false))
                        && self.assignment.incarnations.get(&symbol) == Some(&request.incarnation)
                    {
                        book.recovery = Some((request, true));
                    } else {
                        self.stats.stale_events += 1;
                    }
                }
            }
            Input::Invalid {
                symbols, reason, ..
            } => {
                eprintln!("source {source:?} invalid: {reason}");
                self.cancel_planned();
                self.retire_pause(source);
                self.invalidate(source, symbols.as_deref());
                // Source owns transport retry after a disconnect. Invalidating
                // permission is not an extra planned-renewal request; semantic
                // book rejection and roster changes have their own paths.
                for symbol in symbols.unwrap_or_else(|| self.assignment.symbols.clone()) {
                    if self.selected.contains_key(&symbol) {
                        self.choose(&symbol)?;
                    }
                }
            }
            Input::Membership {
                mut wire,
                roster,
                at,
                ..
            } => {
                if let Some(roster) = roster {
                    self.needs_refresh.remove(&source.replica);
                    wire.retain(|s| roster.get(s) == self.assignment.incarnations.get(s));
                    if roster == self.assignment.incarnations {
                        self.needs_renewal.remove(&source.replica);
                    } else {
                        self.needs_renewal.insert(source.replica);
                    }
                    self.feeds[source.replica].applied = Some(roster);
                }
                self.feeds[source.replica].wire = wire;
                let symbols: Vec<_> = self.selected.keys().cloned().collect();
                for symbol in symbols {
                    self.usable_baseline(source.replica, &symbol);
                    self.acquire_standby(source.replica, &symbol);
                    if let Some(event) = self.direct_baseline(&symbol, source, at) {
                        emitted.push(event);
                    }
                }
            }
            Input::Fence { request, at, .. } => {
                self.drive_planned()?;
                if self.feeds[source.replica]
                    .pending_fence
                    .is_some_and(|(s, id, _)| s == source && id == request)
                {
                    self.feeds[source.replica].pending_fence = None;
                    self.stats.barrier_cuts += 1;
                    emitted = self.promote(source, request, at)?;
                } else {
                    self.stats.stale_events += 1;
                }
            }
            Input::Data {
                incarnation,
                received_tick,
                event,
                ..
            } => {
                ensure!(
                    !self.feeds[source.replica]
                        .pause
                        .as_ref()
                        .is_some_and(|p| p.started()),
                    "source delivered Data after PauseStarted without PauseEnded"
                );
                self.stats.ingress_to_owner_us_max = self
                    .stats
                    .ingress_to_owner_us_max
                    .max(received_tick.elapsed().as_micros());
                let symbol = event.symbol().to_owned();
                if self
                    .selected
                    .get(&symbol)
                    .is_none_or(|s| s.incarnation != incarnation)
                {
                    self.stats.stale_events += 1;
                    return Ok(emitted);
                }
                let feed = &self.feeds[source.replica];
                let book = &feed.books[&symbol];
                if source.generation < book.min_generation
                    || feed.admission < book.min_admission
                    || book.recovery.is_some_and(|(_, started)| !started)
                {
                    self.stats.stale_events += 1;
                    return Ok(emitted);
                }
                let active = self.selected[&symbol].active == Some(source);
                if self.contract.ordering == OrderingStrategy::SnapshotOnly {
                    ensure!(
                        match &event {
                            BookEvent::Snapshot(s) => s.clock.is_none(),
                            BookEvent::Update(u) => u.clock.is_none(),
                        },
                        "snapshot-only event contains clock"
                    );
                }
                let book = self.feeds[source.replica].books.get_mut(&symbol).unwrap();
                match event {
                    BookEvent::Snapshot(s) => {
                        let at = s.received_at;
                        let next = s.clock.map(Frontier::snapshot);
                        if next
                            .zip(book.frontier)
                            .is_some_and(|(next, old)| next.clock < old.clock)
                            || self.replicas == 1
                                && next
                                    .zip(self.selected[&symbol].floor)
                                    .is_some_and(|(next, old)| next.clock < old.clock)
                        {
                            self.reject_clock(
                                &symbol,
                                source,
                                next,
                                Some((s.bids.len(), s.asks.len())),
                                "snapshot market clock regressed",
                            )?;
                            return Ok(vec![]);
                        }
                        if self.contract.ordering == OrderingStrategy::ComparableClock {
                            ensure!(next.is_some(), "comparable snapshot lacks clock");
                        }
                        let confirmation = if self.contract.ordering
                            == OrderingStrategy::SnapshotOnly
                            && active
                            && book.value.is_valid()
                            && book.recovery.is_none()
                        {
                            let mut replacement = OrderBook::new(symbol.clone());
                            replacement.replace(s.clone()).is_ok()
                                && book.value.same_levels(&replacement)
                        } else {
                            false
                        };
                        let revived = !book.value.is_valid();
                        if let Err(e) = book.value.replace(s) {
                            self.reject_book(source, &symbol, &format!("snapshot: {e:#}"))?;
                            return Ok(emitted);
                        }
                        book.frontier = next;
                        // This baseline starts a new validity epoch, so it owns
                        // a full deadline. The frozen goal and budget carry on.
                        if revived && let Some((_, acquired)) = &mut book.catchup {
                            *acquired = Instant::now();
                        }
                        if confirmation {
                            self.stats.snapshot_confirmations += 1;
                        }
                        // Readiness resolves the frozen question we asked.
                        // Publication still checks the current selection floor
                        // in direct_baseline/promote; these are distinct cuts.
                        if let Some(floor) = book.probe.or(self.selected[&symbol].floor)
                            && next.is_none_or(|f| !f.covers(floor))
                            && book.catchup.is_none()
                        {
                            book.catchup = Some((floor, Instant::now()));
                            self.waiting_catchup
                                .insert((source.replica, symbol.clone()));
                        }
                        if active && !confirmation {
                            if self.contract.ordering == OrderingStrategy::SnapshotOnly {
                                self.stats.state_reconciliations += 1;
                            }
                            let state = self.selected.get_mut(&symbol).unwrap();
                            state.unverified_cut = false;
                            state.floor = next;
                            state.epoch = state
                                .epoch
                                .checked_add(1)
                                .context("output epoch overflow")?;
                            emitted.push(Emit::Snapshot {
                                symbol: symbol.clone(),
                                source,
                                reason: SnapshotReason::Recovery,
                                at,
                                stamp: self.stamp(&symbol, source),
                            });
                        } else if let Some(event) = self.direct_baseline(&symbol, source, at) {
                            emitted.push(event);
                        }
                    }
                    BookEvent::Update(u) => {
                        if !book.value.is_valid() {
                            self.stats.quarantined_updates += 1;
                            return Ok(emitted);
                        }
                        let next = if let Some(clock) = u.clock {
                            match Frontier::delta(book.frontier, clock) {
                                Ok(f) => Some(f),
                                Err(_) => {
                                    self.reject_clock(
                                        &symbol,
                                        source,
                                        Some(Frontier {
                                            clock,
                                            ordinal: 0,
                                            complete: false,
                                        }),
                                        None,
                                        "delta clock/prefix regressed",
                                    )?;
                                    return Ok(vec![]);
                                }
                            }
                        } else {
                            ensure!(
                                self.contract.ordering == OrderingStrategy::SnapshotOnly,
                                "comparable delta lacks clock"
                            );
                            None
                        };
                        match book.value.apply(u) {
                            Ok(data) => {
                                book.frontier = next;
                                book.activity.window.record(
                                    received_tick
                                        .saturating_duration_since(self.origin)
                                        .as_secs()
                                        / self.health.bucket_seconds(),
                                );
                                if active {
                                    let state = self.selected.get_mut(&symbol).unwrap();
                                    state.floor = next;
                                    emitted.push(Emit::Update(data));
                                } else {
                                    self.stats.standby_updates += 1;
                                }
                            }
                            Err(e) => {
                                self.reject_book(source, &symbol, &format!("update: {e:#}"))?;
                            }
                        }
                    }
                }
                self.usable_baseline(source.replica, &symbol);
                self.acquire_standby(source.replica, &symbol);
                self.note_progress(&symbol, source);
                self.choose(&symbol)?;
            }
            Input::Begin { .. } | Input::PauseEnded { .. } => unreachable!(),
        }
        Ok(emitted)
    }
    fn tick(&mut self) -> anyhow::Result<()> {
        self.integrity_tick()?;
        for feed in &mut self.feeds {
            if feed
                .pending_fence
                .is_some_and(|(_, _, at)| at.elapsed() > Duration::from_secs(5))
            {
                feed.pending_fence = None;
            }
        }
        let symbols: Vec<_> = self
            .selected
            .iter()
            .filter(|(_, s)| s.active.is_none() || s.candidate.is_some() || s.peer_ahead.is_some())
            .map(|(symbol, _)| symbol.clone())
            .collect();
        for symbol in symbols {
            if self.selected[&symbol]
                .candidate
                .is_some_and(|i| self.feeds[i].pending_fence.is_none())
            {
                self.selected.get_mut(&symbol).unwrap().candidate = None;
            }
            self.choose(&symbol)?;
        }
        Ok(())
    }
    async fn publish(&mut self, output: &Output, emitted: Vec<Emit>) -> anyhow::Result<()> {
        let result = async {
            for (index, event) in emitted.into_iter().enumerate() {
                if index % 32 == 0 {
                    tokio::task::yield_now().await;
                }
                let record = match event {
                    Emit::Update(data) => {
                        self.stats.updates += 1;
                        Record::Update(data)
                    }
                    Emit::Snapshot {
                        symbol,
                        source,
                        reason,
                        at,
                        stamp,
                    } => {
                        self.stats.snapshots += 1;
                        Record::SourcedSnapshot {
                            data: self.feeds[source.replica].books[&symbol]
                                .value
                                .snapshot(at)?,
                            reason,
                            source: stamp,
                        }
                    }
                };
                tokio::time::timeout(Duration::from_secs(15), output.send(record))
                    .await
                    .context("output backpressure deadline")??;
            }
            self.release_planned()
        }
        .await;
        if result.is_err() {
            self.cancel_planned();
            self.cancel_pauses();
        }
        result
    }
    async fn checkpoints(&mut self, output: &Output) -> anyhow::Result<()> {
        let mut emitted = vec![];
        for (symbol, state) in &self.selected {
            if let Some(source) = state.active
                && self.ready(source.replica, symbol)
            {
                emitted.push(Emit::Snapshot {
                    symbol: symbol.clone(),
                    source,
                    reason: SnapshotReason::Periodic,
                    at: Utc::now(),
                    stamp: self.stamp(symbol, source),
                });
            }
        }
        self.publish(output, emitted).await
    }
    fn report_cutoff(
        &mut self,
        assignment: &mut watch::Receiver<Assignment>,
        sources: &[watch::Receiver<source::Stats>],
        progress: &watch::Sender<ShardStats>,
    ) {
        // Once closing starts, the assignment select branch is disabled. A
        // quiet shard may have no more relay events to consume a pending watch
        // update, so synchronize the supervisor's frozen membership explicitly.
        self.sync(assignment.borrow_and_update().clone());
        self.report(sources, progress);
    }

    fn report(
        &mut self,
        sources: &[watch::Receiver<source::Stats>],
        progress: &watch::Sender<ShardStats>,
    ) {
        self.stats.desired = self.selected.len();
        let unavailable = self
            .feeds
            .iter()
            .flat_map(|f| f.books.iter())
            .filter(|(_, b)| b.initialization_failures > 0 && !b.value.is_valid());
        self.stats.unavailable_replica_assets = unavailable.clone().count();
        self.stats.unavailable_sample = unavailable.take(8).map(|(s, _)| s.clone()).collect();
        self.stats.epoch = self.assignment.epoch;
        self.stats.maintenance_quiesced = self.quiescing;
        self.stats.maintenance_pending = usize::from(self.rotation.is_some())
            + usize::from(self.scheduled_recovery.is_some())
            + usize::from(self.planned.is_some())
            + self.reset_restarts.len()
            + self.feeds.iter().filter(|f| f.pause.is_some()).count()
            + self.needs_renewal.len()
            + self.needs_refresh.len()
            + self
                .feeds
                .iter()
                .flat_map(|f| f.books.values())
                .filter(|b| b.activity.recovering)
                .count();
        self.stats.initialized = self
            .selected
            .iter()
            .filter(|(s, state)| {
                state
                    .active
                    .is_some_and(|source| self.ready(source.replica, s))
            })
            .count();
        self.stats.subscribed = self.stats.initialized;
        self.stats.replica_initialized = (0..self.replicas)
            .map(|i| self.selected.keys().filter(|s| self.ready(i, s)).count())
            .collect();
        let rows: Vec<_> = sources.iter().map(|s| s.borrow().clone()).collect();
        self.stats.connects = rows.iter().map(|s| s.connects).sum();
        self.stats.frames = rows.iter().map(|s| s.frames).sum();
        self.stats.frame_bytes = rows.iter().map(|s| s.bytes).sum();
        self.stats.gaps = rows.iter().map(|s| s.gaps).sum();
        self.stats.resync_requests = rows.iter().map(|s| s.resync).sum();
        self.stats.covering_probes = rows.iter().map(|s| s.probes).sum();
        self.stats.commands = rows.iter().map(|s| s.commands).sum();
        self.stats.retired_messages = rows.iter().map(|s| s.retired).sum();
        self.stats.source_quarantined = rows.iter().map(|s| s.quarantined).sum();
        self.stats.source_restarts = rows.iter().map(|s| s.restarts).sum();
        self.stats.handoff_pauses = rows.iter().map(|s| s.handoff_pauses).sum();
        self.stats.pause_expirations = rows.iter().map(|s| s.pause_expirations).sum();
        self.stats.pause_us_max = rows.iter().map(|s| s.pause_us_max).max().unwrap_or(0);
        self.stats.unacknowledged_closes = rows.iter().map(|s| s.unacknowledged_closes).sum();
        self.stats.stop_retirements = rows.iter().map(|s| s.stop_retirements).sum();
        self.stats.resubscriptions = rows.iter().map(|s| s.resubscriptions).sum();
        self.stats.resubscribe_completed = rows.iter().map(|s| s.resubscribe_completed).sum();
        self.stats.resubscribe_unready_ms_max = rows
            .iter()
            .map(|s| s.resubscribe_unready_ms_max)
            .max()
            .unwrap_or(0);
        self.stats.ack_timeouts = rows.iter().map(|s| s.ack_timeouts).sum();
        self.stats.pings_sent = rows.iter().map(|s| s.pings_sent).sum();
        self.stats.pongs_received = rows.iter().map(|s| s.pongs_received).sum();
        self.stats.frame_age_ms_max = rows.iter().map(|s| s.frame_age_ms_max).max().unwrap_or(0);
        self.stats.pong_age_ms_max = rows.iter().map(|s| s.pong_age_ms_max).max().unwrap_or(0);
        self.stats.ack_age_ms_max = rows.iter().map(|s| s.ack_age_ms_max).max().unwrap_or(0);
        progress.send_replace(self.stats.clone());
    }
}

pub(crate) async fn run(
    index: usize,
    mut assignment: watch::Receiver<Assignment>,
    shared: source::Shared,
    output: Output,
    mut stop: watch::Receiver<WorkerPhase>,
    progress: watch::Sender<ShardStats>,
    relay_bytes: Arc<Semaphore>,
) -> anyhow::Result<ShardStats> {
    let (relay, mut input) = relay::channel(relay_bytes);
    let runtime = source::Runtime {
        protocol: shared.protocol.clone(),
        shard: index,
        options: shared.options,
        auth: shared.auth,
        pacer: shared.pacer,
        relay: relay.clone(),
    };
    let mut tasks = JoinSet::new();
    let (cancel, local_stop) = watch::channel(false);
    let mut controls = vec![];
    let mut rows = vec![];
    for replica in 0..runtime.options.replicas {
        let (tx, rx) = watch::channel(Control::default());
        controls.push(tx);
        let (stats_tx, stats_rx) = watch::channel(source::Stats::default());
        rows.push(stats_rx);
        let mut r = runtime.clone();
        r.relay = relay.clone();
        let targets = assignment.clone();
        let source_stop = local_stop.clone();
        let source_cancel = cancel.clone();
        tasks.spawn(async move {
            let job = source::run(replica, r, targets, rx, source_stop, stats_tx);
            (
                replica,
                crate::source_lifecycle::supervise(job, source_cancel).await,
            )
        });
    }
    drop(relay);
    let mut owner = Owner::new(
        assignment.borrow_and_update().clone(),
        controls,
        runtime.options.replica_lag,
        runtime.protocol.contract(),
    );
    owner.recovery_pacer = Some(shared.recovery_pacer);
    owner.configure_health(
        runtime.options.health.clone(),
        index,
        runtime.options.max_connections / runtime.options.replicas,
    );
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let logical_budget = runtime.options.max_connections / runtime.options.replicas;
    let phase = runtime
        .options
        .snapshot_interval
        .mul_f64(index as f64 / logical_budget as f64);
    let first = if phase.is_zero() {
        runtime.options.snapshot_interval
    } else {
        phase
    };
    let mut snapshots =
        tokio::time::interval_at(Instant::now() + first, runtime.options.snapshot_interval);
    snapshots.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let fault_deadline = runtime
        .options
        .failover_test_after
        .map(|d| Instant::now() + d);
    let mut fault_done = index != 0;
    let mut last_report = Instant::now();
    let mut closing = false;
    let mut first_error = None;
    let mut input_open = true;
    loop {
        let phase = *stop.borrow_and_update();
        let was_quiescing = owner.quiescing;
        owner.quiescing = phase != WorkerPhase::Running;
        if owner.quiescing && !was_quiescing {
            owner.scheduled_recovery = None;
            owner.coverage = None;
            owner.cancel_planned();
            owner.cancel_pauses();
            owner.report(&rows, &progress);
            last_report = Instant::now();
        }
        if phase == WorkerPhase::Stopping && !closing {
            closing = true;
            let _ = cancel.send(true);
        }
        if closing && tasks.is_empty() && input.is_empty() {
            break;
        }
        // Policy/output failure still owns source teardown. Keep draining the
        // bounded relay so a backpressured producer can observe cancellation.
        let handled: anyhow::Result<()> = async { tokio::select! {
            phase = stop.changed(), if !closing => {phase?;},
            change = assignment.changed(), if !closing => {
                change?;
                owner.sync(assignment.borrow_and_update().clone());
            },
            admitted = input.recv(), if input_open => if let Some(admitted) = admitted {
                if first_error.is_some() && closing { return Ok(()); }
                if assignment.has_changed()? {
                    owner.sync(assignment.borrow_and_update().clone());
                }
                owner.stats.relay_wait_us_max = owner.stats.relay_wait_us_max.max(admitted.attempted_at.elapsed().as_micros());
                let emitted = owner.input(admitted.input)?;
                owner.publish(&output, emitted).await?;
            } else { input_open = false; },
            _ = tick.tick(), if !closing => {
                owner.tick()?;
                owner.activity_tick()?;
                owner.rotation_tick()?;
                owner.release_planned()?;
                if last_report.elapsed() >= Duration::from_secs(1) {
                    owner.report(&rows, &progress);
                    last_report = Instant::now();
                }
                if !fault_done && fault_deadline.is_some_and(|t| Instant::now() >= t)
                    && let Some(source) = owner.inject_fault()?
                {
                    fault_done = true;
                    eprintln!("fault pilot: shard {index}, closing own active replica {source:?}");
                }
            },
            due = snapshots.tick(), if !closing => {
                owner.stats.snapshot_lateness_ms = owner.stats.snapshot_lateness_ms.max(due.elapsed().as_millis());
                owner.checkpoints(&output).await?;
            },
            done = tasks.join_next(), if !tasks.is_empty() => {
                match done {
                    Some(Ok((slot, Err(e)))) => {
                        eprintln!("shard {index} replica {slot} exhausted: {e:#}");
                        first_error.get_or_insert(e);
                        owner.cancel_planned();
                        owner.retire_pause(Source { replica: slot, generation: owner.feeds[slot].generation });
                        // An error surfacing after the cut is still a run error,
                        // but cannot invalidate state accepted before SIGINT.
                        if *stop.borrow() != WorkerPhase::Stopping {
                            let source = Source { replica: slot, generation: owner.feeds[slot].generation };
                            owner.invalidate(source, None);
                            for symbol in owner.assignment.symbols.clone() {
                                owner.choose(&symbol)?;
                            }
                        }
                        // Source returns Err only after its own retry policy is
                        // spent or the failure is non-retryable; nothing respawns
                        // it. The run is already failed, so stop and join the
                        // surviving peer now instead of collecting degraded
                        // single-replica data until an external cutoff.
                        if !closing {
                            closing = true;
                            let _ = cancel.send(true);
                        }
                    },
                    Some(Err(e)) => {
                        owner.cancel_planned();
                        owner.cancel_pauses();
                        first_error.get_or_insert(e.into());
                        closing = true;
                        let _ = cancel.send(true);
                    },
                    Some(Ok((slot, Ok(())))) => {
                        owner.cancel_planned();
                        owner.retire_pause(Source { replica: slot, generation: owner.feeds[slot].generation });
                    },
                    _ => {},
                }
                if tasks.is_empty() && !closing {
                    closing = true;
                    let _ = cancel.send(true);
                }
            }
        }; Ok(()) }.await;
        if let Err(error) = handled {
            owner.cancel_planned();
            owner.cancel_pauses();
            first_error.get_or_insert(error);
            closing = true;
            let _ = cancel.send(true);
        }
    }
    owner.report_cutoff(&mut assignment, &rows, &progress);
    if let Some(e) = first_error {
        return Err(e);
    }
    Ok(owner.stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::ExchangeAdapter;
    use crate::{
        Side,
        model::{Level, QuantityUpdate, Snapshot, Update},
    };
    fn owner() -> Owner {
        let assignment = Assignment {
            epoch: 1,
            symbols: vec!["A".into()],
            incarnations: std::collections::BTreeMap::from([("A".into(), 1)]),
        };
        let mut controls = vec![];
        for _ in 0..2 {
            controls.push(watch::channel(Control::default()).0);
        }
        Owner::new(
            assignment,
            controls,
            Duration::from_secs(2),
            crate::exchange::kalshi::Kalshi.contract(),
        )
    }
    fn source(replica: usize) -> Source {
        Source {
            replica,
            generation: 1,
        }
    }
    fn multibook_owner() -> Owner {
        let mut o = owner();
        let mut assignment = o.assignment.clone();
        assignment.symbols.extend(["B".into(), "C".into()]);
        assignment
            .incarnations
            .extend([("B".into(), 1), ("C".into(), 1)]);
        o.sync(assignment);
        for slot in 0..2 {
            begin(&mut o, source(slot));
            o.input(Input::Membership {
                roster: None,
                source: source(slot),
                wire: BTreeSet::from(["A".into(), "B".into(), "C".into()]),
                at: Utc::now(),
            })
            .unwrap();
            for symbol in ["A", "B", "C"] {
                o.input(Input::Data {
                    source: source(slot),
                    incarnation: 1,
                    received_tick: Instant::now(),
                    event: BookEvent::Snapshot(Snapshot {
                        symbol: symbol.into(),
                        received_at: Utc::now(),
                        exchange_at: Utc::now(),
                        clock: Some(100),
                        bids: vec![Level {
                            price: 420_000,
                            quantity: 10,
                        }],
                        asks: vec![],
                    }),
                })
                .unwrap();
            }
        }
        o
    }
    #[test]
    fn a_regressed_standby_snapshot_does_not_reset_other_symbols() {
        let mut o = multibook_owner();
        assert!(snap(&mut o, source(1), 0, 10).is_empty());
        assert_eq!(o.stats.hard_resets, 0);
        assert_eq!(o.stats.continuity_gaps, 0);
        assert_eq!(o.selected["B"].active, Some(source(0)));
        assert_eq!(o.selected["C"].floor, Some(Frontier::snapshot(100)));
        assert!(!o.ready(1, "A"));
        assert!(o.ready(1, "B"));
    }
    #[tokio::test(start_paused = true)]
    async fn planned_health_cut_rechecks_current_floor_before_unsubscribe() {
        let mut o = owner();
        o.health.window = Duration::from_secs(4);
        o.health.warmup = Duration::from_secs(4);
        o.health.cooldown = Duration::from_secs(4);
        o.health.expected_points = 100;
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 10, 20);
        tokio::time::advance(Duration::from_secs(4)).await;
        o.activity_tick().unwrap();
        assert!(o.feeds[0].control.borrow().resubscribe.is_empty());
        assert_eq!(o.selected["A"].active, Some(source(0)));
        delta(&mut o, source(0), 11, 1);
        assert!(cut(&mut o, 1).is_empty());
        assert_eq!(o.selected["A"].active, Some(source(0)));
        assert_eq!(o.stats.hard_resets, 0);
        assert_eq!(o.stats.health_resubscriptions, 0);
        assert!(o.feeds[0].control.borrow().resubscribe.is_empty());
    }
    #[test]
    fn connection_opened_before_a_reset_cannot_satisfy_its_fresh_admission() {
        let mut o = owner();
        start(&mut o, 0, 100, 20);
        start(&mut o, 1, 100, 20);
        o.reset_symbol("A", "maintenance").unwrap();
        o.rotation_tick().unwrap();
        o.release_planned().unwrap();
        // This dial already started under restart nonce zero. A larger local
        // generation alone cannot prove it belongs to the reset's new context.
        let queued = Source {
            replica: 0,
            generation: 2,
        };
        o.input(Input::Begin {
            source: queued,
            admission: 0,
        })
        .unwrap();
        assert_eq!(o.feeds[0].generation, 1);
        assert!(snap(&mut o, queued, 100, 20).is_empty());
        let fresh = Source {
            replica: 0,
            generation: 3,
        };
        begin(&mut o, fresh);
        o.input(Input::Membership {
            roster: None,
            source: fresh,
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        assert!(matches!(
            &snap(&mut o, fresh, 1, 20)[..],
            [Emit::Snapshot {
                reason: SnapshotReason::Recovery,
                ..
            }]
        ));
        assert_eq!(o.selected["A"].active, Some(fresh));
    }
    #[tokio::test(start_paused = true)]
    async fn quiesce_finishes_existing_rotation_and_prevents_new_maintenance() {
        let mut o = owner();
        o.health.rotation_interval = Duration::from_secs(1);
        o.health.window = Duration::from_secs(4);
        o.health.warmup = Duration::from_secs(4);
        o.health.expected_points = 100;
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 10, 20);
        assert!(o.start_rotation(0).unwrap());
        cut(&mut o, 1);
        o.release_planned().unwrap(); // model successful baseline admission
        o.quiescing = true;
        o.rotation_tick().unwrap();
        let fresh = Source {
            replica: 0,
            generation: 2,
        };
        begin(&mut o, fresh);
        o.input(Input::Membership {
            roster: None,
            source: fresh,
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        snap(&mut o, fresh, 10, 20);
        o.rotation_tick().unwrap();
        assert_eq!(o.stats.completed_rotations, 1);
        tokio::time::advance(Duration::from_secs(10)).await;
        o.activity_tick().unwrap();
        o.rotation_tick().unwrap();
        assert_eq!(o.stats.planned_rotations, 1);
        assert_eq!(o.stats.health_resubscriptions, 0);
    }
    #[test]
    fn rolling_close_waits_for_peer_snapshot_handoff_then_restores_both_feeds() {
        let mut o = owner();
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 10, 20);
        assert!(o.start_rotation(0).unwrap());
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        assert_eq!(o.selected["A"].active, Some(source(0)));
        // Before the peer fence the old healthy publisher keeps serving.
        snap(&mut o, source(0), 10, 20);
        assert_eq!(o.selected["A"].active, Some(source(0)));
        acknowledge_pause(&mut o);
        let id = o.feeds[1].control.borrow().fence;
        let emitted = o
            .input(Input::Fence {
                source: source(1),
                request: id,
                at: Utc::now(),
            })
            .unwrap();
        assert!(matches!(
            &emitted[..],
            [Emit::Snapshot {
                source: Source { replica: 1, .. },
                ..
            }]
        ));
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        o.release_planned().unwrap(); // model successful baseline admission
        o.rotation_tick().unwrap();
        assert_eq!(o.feeds[0].control.borrow().restart, 1);
        let next = Source {
            replica: 0,
            generation: 2,
        };
        begin(&mut o, next);
        o.input(Input::Membership {
            roster: None,
            source: next,
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        snap(&mut o, next, 10, 20);
        o.rotation_tick().unwrap();
        assert!(o.rotation.is_none());
        assert_eq!(o.stats.completed_rotations, 1);
        assert_eq!(o.stats.continuity_gaps, 0);
        assert!(o.ready(0, "A") && o.ready(1, "A"));
    }
    #[test]
    fn rolling_renewal_defers_when_peer_cannot_cover_published_state() {
        let mut o = owner();
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 9, 20);
        assert!(!o.start_rotation(0).unwrap());
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        assert_eq!(o.selected["A"].active, Some(source(0)));
    }
    #[test]
    fn pending_symbol_recovery_blocks_rotation_of_its_surviving_peer() {
        let mut o = owner();
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 10, 20);
        // The book is still valid while its recovery intent awaits the source.
        o.feeds[0]
            .books
            .get_mut("A")
            .unwrap()
            .activity
            .admitted(Instant::now(), &o.health);
        assert!(o.ready(0, "A"));
        assert!(!o.start_rotation(1).unwrap());
        assert_eq!(o.feeds[1].control.borrow().restart, 0);
    }
    #[tokio::test(start_paused = true)]
    async fn stalled_handoff_keeps_old_publisher_without_forced_refresh() {
        let mut o = owner();
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 10, 20);
        assert!(o.start_rotation(0).unwrap());
        tokio::time::advance(Duration::from_secs(31)).await;
        o.rotation_tick().unwrap();
        assert!(o.rotation.is_none());
        assert_eq!(o.stats.rotation_timeouts, 1);
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        assert!(o.feeds[0].control.borrow().refresh.is_empty());
        assert_eq!(o.selected["A"].active, Some(source(0)));
        assert_eq!(o.stats.hard_resets, 0);
        assert!(matches!(
            &snap(&mut o, source(0), 11, 20)[..],
            [Emit::Snapshot {
                reason: SnapshotReason::Recovery,
                ..
            }]
        ));
    }
    #[tokio::test(start_paused = true)]
    async fn low_rate_recovery_is_symbol_scoped_and_does_not_restart_the_connection() {
        let mut o = owner();
        o.health.window = Duration::from_secs(4);
        o.health.warmup = Duration::from_secs(4);
        o.health.cooldown = Duration::from_secs(4);
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 10, 20);
        for _ in 0..20 {
            o.feeds[1]
                .books
                .get_mut("A")
                .unwrap()
                .activity
                .window
                .record(0);
        }
        tokio::time::advance(Duration::from_secs(4)).await;
        o.activity_tick().unwrap();
        assert_eq!(o.stats.health_resubscriptions, 0);
        cut(&mut o, 1);
        o.release_planned().unwrap(); // model successful baseline admission
        assert_eq!(o.stats.health_resubscriptions, 1);
        let control = o.feeds[0].control.borrow();
        assert_eq!(control.resubscribe["A"].incarnation, 1);
        assert_eq!(control.restart, 0);
    }
    #[tokio::test(start_paused = true)]
    async fn deferred_recovery_keeps_one_slot_and_rechecks_current_rate() {
        let mut o = owner();
        o.health.window = Duration::from_secs(4);
        o.health.warmup = Duration::from_secs(4);
        o.health.cooldown = Duration::from_secs(4);
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 10, 20);
        for _ in 0..20 {
            o.feeds[1]
                .books
                .get_mut("A")
                .unwrap()
                .activity
                .window
                .record(3);
        }
        tokio::time::advance(Duration::from_secs(4)).await;
        let pacer = Arc::new(crate::collector::Pacer::new(Duration::from_secs(4)));
        let now = Instant::now();
        assert_eq!(pacer.reserve(), now); // another shard
        o.recovery_pacer = Some(pacer.clone());
        o.activity_tick().unwrap();
        assert!(o.scheduled_recovery.is_some());
        assert_eq!(o.stats.health_resubscriptions, 0);
        o.activity_tick().unwrap();
        assert_eq!(pacer.reserve(), now + Duration::from_secs(8));
        // The source recovers while waiting. Its old deficit is no authority to
        // unsubscribe a healthy symbol once the reserved time arrives.
        for _ in 0..20 {
            for i in 0..2 {
                o.feeds[i]
                    .books
                    .get_mut("A")
                    .unwrap()
                    .activity
                    .window
                    .record(4);
            }
        }
        tokio::time::advance(Duration::from_secs(4)).await;
        o.activity_tick().unwrap();
        assert!(o.scheduled_recovery.is_none());
        assert_eq!(o.stats.health_resubscriptions, 0);
    }
    #[tokio::test(start_paused = true)]
    async fn delayed_health_intent_cannot_resubscribe_a_new_transport_generation() {
        let mut o = owner();
        o.health.window = Duration::from_secs(4);
        o.health.warmup = Duration::from_secs(4);
        o.health.cooldown = Duration::from_secs(4);
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 10, 20);
        for _ in 0..20 {
            o.feeds[1]
                .books
                .get_mut("A")
                .unwrap()
                .activity
                .window
                .record(3);
        }
        tokio::time::advance(Duration::from_secs(4)).await;
        let pacer = Arc::new(crate::collector::Pacer::new(Duration::from_secs(4)));
        pacer.reserve();
        o.recovery_pacer = Some(pacer);
        o.activity_tick().unwrap();
        let fresh = Source {
            replica: 0,
            generation: 2,
        };
        begin(&mut o, fresh);
        o.input(Input::Membership {
            roster: None,
            source: fresh,
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        snap(&mut o, fresh, 10, 20);
        for _ in 0..20 {
            o.feeds[1]
                .books
                .get_mut("A")
                .unwrap()
                .activity
                .window
                .record(4);
        }
        tokio::time::advance(Duration::from_secs(4)).await;
        o.activity_tick().unwrap();
        assert_eq!(o.stats.health_resubscriptions, 0);
        assert!(o.feeds[0].control.borrow().resubscribe.is_empty());
    }
    #[test]
    fn cutoff_report_consumes_unread_epoch_and_retirement_without_another_frame() {
        for retired in [false, true] {
            let mut o = owner();
            start(&mut o, 0, 10, 20);
            start(&mut o, 1, 10, 20);
            let (assignments, mut pending) = watch::channel(o.assignment.clone());
            let mut latest = o.assignment.clone();
            latest.epoch = 2;
            if retired {
                latest.symbols.clear();
                latest.incarnations.clear();
            }
            assignments.send_replace(latest);
            assert!(pending.has_changed().unwrap());
            let (progress, report) = watch::channel(ShardStats::default());

            // Both cancellation and a watch update may be ready at the cut.
            // No more relay input arrives to consume this assignment.
            o.report_cutoff(&mut pending, &[], &progress);

            let row = report.borrow();
            let remaining = usize::from(!retired);
            assert_eq!(row.epoch, 2);
            assert_eq!(row.desired, remaining);
            assert_eq!(row.initialized, remaining);
            assert_eq!(row.replica_initialized, vec![remaining; 2]);
            assert!(!pending.has_changed().unwrap());
        }
    }
    #[test]
    fn cutoff_report_does_not_invent_readiness_for_added_or_reactivated_markets() {
        for reactivated in [false, true] {
            let mut o = owner();
            start(&mut o, 0, 10, 20);
            start(&mut o, 1, 10, 20);
            let (assignments, mut pending) = watch::channel(o.assignment.clone());
            let mut latest = o.assignment.clone();
            latest.epoch = 2;
            if reactivated {
                latest.incarnations.insert("A".into(), 2);
            } else {
                latest.symbols.push("B".into());
                latest.incarnations.insert("B".into(), 2);
            }
            assignments.send_replace(latest);
            let (progress, report) = watch::channel(ShardStats::default());

            o.report_cutoff(&mut pending, &[], &progress);

            let row = report.borrow();
            let initialized = usize::from(!reactivated);
            assert_eq!(row.epoch, 2);
            assert_eq!(row.desired, if reactivated { 1 } else { 2 });
            assert_eq!(row.initialized, initialized);
            assert_eq!(row.replica_initialized, vec![initialized; 2]);
            assert!(row.initialized < row.desired);
        }
    }
    fn begin(owner: &mut Owner, source: Source) {
        let admission = owner.feeds[source.replica].control.borrow().restart;
        owner.input(Input::Begin { source, admission }).unwrap();
    }
    fn start(owner: &mut Owner, slot: usize, clock: u64, quantity: i64) -> Vec<Emit> {
        let source = source(slot);
        begin(owner, source);
        owner
            .input(Input::Membership {
                roster: None,
                source,
                wire: BTreeSet::from(["A".into()]),
                at: Utc::now(),
            })
            .unwrap();
        snap(owner, source, clock, quantity)
    }
    fn snap(owner: &mut Owner, source: Source, clock: u64, quantity: i64) -> Vec<Emit> {
        finish_pause_release(owner, source);
        owner
            .input(Input::Data {
                source,
                incarnation: 1,
                received_tick: Instant::now(),
                event: BookEvent::Snapshot(Snapshot {
                    symbol: "A".into(),
                    received_at: Utc::now(),
                    exchange_at: Utc::now(),
                    clock: Some(clock),
                    bids: vec![Level {
                        price: 420_000,
                        quantity,
                    }],
                    asks: vec![],
                }),
            })
            .unwrap()
    }
    fn delta(owner: &mut Owner, source: Source, clock: u64, change: i64) -> Vec<Emit> {
        finish_pause_release(owner, source);
        owner
            .input(Input::Data {
                source,
                incarnation: 1,
                received_tick: Instant::now(),
                event: BookEvent::Update(Update {
                    symbol: "A".into(),
                    received_at: Utc::now(),
                    exchange_at: Utc::now(),
                    clock: Some(clock),
                    side: Side::Buy,
                    price: 420_000,
                    quantity: QuantityUpdate::Change(change),
                }),
            })
            .unwrap()
    }
    fn cut(owner: &mut Owner, slot: usize) -> Vec<Emit> {
        acknowledge_pause(owner);
        owner.tick().unwrap();
        let (source, request, _) = owner.feeds[slot].pending_fence.unwrap();
        owner
            .input(Input::Fence {
                source,
                request,
                at: Utc::now(),
            })
            .unwrap()
    }
    fn acknowledge_pause(owner: &mut Owner) {
        let requested = owner.feeds.iter().enumerate().find_map(|(slot, f)| {
            f.pause.filter(|p| !p.started()).and_then(|_| {
                f.control.borrow().pause.map(|p| {
                    (
                        Source {
                            replica: slot,
                            generation: f.generation,
                        },
                        p,
                    )
                })
            })
        });
        if let Some((source, request)) = requested {
            owner
                .input(Input::PauseStarted {
                    source,
                    nonce: request.nonce,
                    until: Instant::now() + request.max_duration,
                })
                .unwrap();
        }
    }
    fn finish_pause_release(owner: &mut Owner, source: Source) {
        let feed = &owner.feeds[source.replica];
        let release = if feed.pause.is_some() && feed.control.borrow().pause.is_none() {
            feed.control.borrow().pause_release
        } else {
            None
        };
        if let Some(id) = release
            && id.generation == source.generation
        {
            owner
                .input(Input::PauseEnded {
                    source,
                    nonce: id.nonce,
                    reason: crate::relay::PauseEnd::Released,
                })
                .unwrap();
        }
    }
    #[test]
    fn complete_snapshot_is_stronger_than_a_same_clock_delta_prefix() {
        let snapshot = Frontier::snapshot(10);
        let first = Frontier {
            clock: 10,
            ordinal: 0,
            complete: false,
        };
        let second = Frontier {
            ordinal: 1,
            ..first
        };
        assert!(!first.covers(second));
        assert!(second.covers(first));
        assert!(snapshot.covers(second));
        assert!(!second.covers(snapshot));
        assert!(Frontier::delta(Some(snapshot), 10).is_err());
    }
    #[test]
    fn standby_backlog_before_fence_is_in_snapshot_and_postcut_delta_is_once() {
        let mut o = owner();
        assert_eq!(start(&mut o, 0, 10, 10).len(), 1);
        assert!(start(&mut o, 1, 10, 10).is_empty());
        assert_eq!(delta(&mut o, source(0), 11, 5).len(), 1);
        assert!(delta(&mut o, source(1), 11, 5).is_empty());
        assert!(delta(&mut o, source(1), 11, 7).is_empty());
        o.input(Input::Invalid {
            source: source(0),
            symbols: None,
            reason: "test disconnect".into(),
        })
        .unwrap();
        o.tick().unwrap();
        assert!(delta(&mut o, source(1), 12, 3).is_empty());
        let emitted = cut(&mut o, 1);
        assert!(
            matches!(&emitted[..],[Emit::Snapshot {reason:SnapshotReason::Failover,stamp,..}] if stamp.output_epoch==2 && !stamp.complete_clock)
        );
        assert_eq!(
            o.feeds[1].books["A"]
                .value
                .snapshot(Utc::now())
                .unwrap()
                .bids[0]
                .quantity,
            25
        );
        let emitted = delta(&mut o, source(1), 12, 2);
        assert!(matches!(&emitted[..],[Emit::Update(u)] if u.quantity==27));
        assert!(delta(&mut o, source(0), 11, 7).is_empty());
        assert_eq!(o.stats.failovers, 1);
    }
    #[tokio::test(start_paused = true)]
    async fn persistently_lagging_same_clock_candidate_requires_fresh_snapshot() {
        let mut o = owner();
        start(&mut o, 0, 10, 10);
        start(&mut o, 1, 10, 10);
        delta(&mut o, source(0), 11, 5);
        delta(&mut o, source(0), 11, 7);
        delta(&mut o, source(1), 11, 5);
        o.input(Input::Invalid {
            source: source(0),
            symbols: None,
            reason: "gap".into(),
        })
        .unwrap();
        assert!(cut(&mut o, 1).is_empty());
        assert_eq!(o.stats.promotions_blocked, 1);
        assert!(o.selected["A"].active.is_none());
        assert!(o.feeds[1].control.borrow().refresh.is_empty());
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(cut(&mut o, 1).is_empty());
        assert_eq!(o.feeds[1].control.borrow().probe["A"].nonce, 1);
        assert!(o.ready(1, "A"));
        assert!(snap(&mut o, source(1), 11, 22).is_empty());
        let emitted = cut(&mut o, 1);
        assert!(matches!(
            &emitted[..],
            [Emit::Snapshot {
                reason: SnapshotReason::Failover,
                ..
            }]
        ));
        assert!(matches!(&delta(&mut o,source(1),12,1)[..],[Emit::Update(u)] if u.quantity==23));
    }
    #[tokio::test(start_paused = true)]
    async fn valid_standby_catches_up_after_fault_without_discarding_its_tail() {
        let mut o = owner();
        start(&mut o, 0, 10, 10);
        start(&mut o, 1, 10, 10);
        delta(&mut o, source(0), 11, 5);
        delta(&mut o, source(0), 11, 7);
        delta(&mut o, source(1), 11, 5);
        o.input(Input::Invalid {
            source: source(0),
            symbols: None,
            reason: "fault".into(),
        })
        .unwrap();
        assert!(cut(&mut o, 1).is_empty());
        // A first cut cannot cover the active prefix. No busy fence loop or
        // snapshot request while the healthy standby drains its ordered tail.
        o.tick().unwrap();
        assert!(o.feeds[1].pending_fence.is_none());
        assert!(o.feeds[1].control.borrow().refresh.is_empty());
        tokio::time::advance(Duration::from_millis(100)).await;
        assert!(delta(&mut o, source(1), 11, 7).is_empty());
        assert!(matches!(
            &cut(&mut o, 1)[..],
            [Emit::Snapshot {
                reason: SnapshotReason::Failover,
                ..
            }]
        ));
        assert_eq!(o.stats.continuity_gaps, 0);
        assert_eq!(o.stats.failovers, 1);
        assert!(o.feeds[1].control.borrow().refresh.is_empty());
        assert!(matches!(&delta(&mut o,source(1),12,1)[..],[Emit::Update(u)] if u.quantity==23));
    }
    #[tokio::test(start_paused = true)]
    async fn quiet_feeds_do_not_rotate_and_a_peer_must_stay_ahead_for_the_grace_period() {
        let mut o = owner();
        start(&mut o, 0, 10, 10);
        start(&mut o, 1, 10, 10);
        tokio::time::advance(Duration::from_secs(20)).await;
        o.tick().unwrap();
        assert_eq!(o.selected["A"].active, Some(source(0)));
        delta(&mut o, source(1), 11, 1);
        o.tick().unwrap();
        assert!(o.feeds[1].pending_fence.is_none());
        tokio::time::advance(Duration::from_secs(2)).await;
        o.tick().unwrap();
        assert!(o.feeds[1].pending_fence.is_some());
        // A recovers before the fence: retain A instead of gratuitously switching.
        delta(&mut o, source(0), 11, 1);
        assert!(cut(&mut o, 1).is_empty());
        assert_eq!(o.selected["A"].active, Some(source(0)));
    }
    #[tokio::test(start_paused = true)]
    async fn continuously_buffered_active_feed_switches_when_it_cannot_reach_a_frozen_peer_mark() {
        let mut o = owner();
        start(&mut o, 0, 10, 10);
        start(&mut o, 1, 10, 10);
        delta(&mut o, source(1), 20, 10); // The peer saw clock 20 now.
        tokio::time::advance(Duration::from_secs(1)).await;
        delta(&mut o, source(0), 11, 1);
        tokio::time::advance(Duration::from_secs(1)).await;
        delta(&mut o, source(0), 12, 1);
        // A is still flowing, but it has not reached the peer's two-second-old witness.
        assert!(matches!(
            &cut(&mut o, 1)[..],
            [Emit::Snapshot {
                reason: SnapshotReason::Failover,
                ..
            }]
        ));
        assert_eq!(o.selected["A"].active, Some(source(1)));
    }
    #[tokio::test(start_paused = true)]
    async fn a_standby_snapshot_restating_the_published_clock_does_not_rotate() {
        let mut o = owner();
        start(&mut o, 0, 10, 10);
        delta(&mut o, source(0), 11, 5); // Published prefix of clock 11: quantity 15.
        start(&mut o, 1, 11, 15); // Complete clock 11 holds exactly that state.
        tokio::time::advance(Duration::from_secs(5)).await;
        o.tick().unwrap();
        assert!(o.feeds[1].pending_fence.is_none());
        assert_eq!(o.selected["A"].active, Some(source(0)));
        assert_eq!(o.stats.failovers, 0);
    }
    #[tokio::test(start_paused = true)]
    async fn a_standby_snapshot_holding_more_of_the_published_clock_rotates_after_the_grace() {
        let mut o = owner();
        start(&mut o, 0, 10, 10);
        delta(&mut o, source(0), 11, 5);
        start(&mut o, 1, 11, 17); // Clock 11 contained a change A has not published.
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(matches!(
            &cut(&mut o, 1)[..],
            [Emit::Snapshot {
                reason: SnapshotReason::Failover,
                ..
            }]
        ));
        assert_eq!(o.selected["A"].active, Some(source(1)));
    }
    #[tokio::test(start_paused = true)]
    async fn an_active_feed_completing_the_snapshot_clock_keeps_publication() {
        let mut o = owner();
        start(&mut o, 0, 10, 10);
        delta(&mut o, source(0), 11, 5);
        start(&mut o, 1, 11, 17);
        delta(&mut o, source(0), 11, 2); // A delivers the rest of clock 11 in time.
        tokio::time::advance(Duration::from_secs(5)).await;
        o.tick().unwrap();
        assert!(o.feeds[1].pending_fence.is_none());
        assert_eq!(o.selected["A"].active, Some(source(0)));
    }
    #[test]
    fn stale_generations_and_coalesced_incarnations_cannot_publish() {
        let mut o = owner();
        start(&mut o, 0, 10, 10);
        start(&mut o, 1, 10, 10);
        let next = Source {
            replica: 1,
            generation: 2,
        };
        begin(&mut o, next);
        assert!(delta(&mut o, source(1), 11, 1).is_empty());
        assert_eq!(o.stats.stale_events, 1);
        let mut assignment = o.assignment.clone();
        assignment.epoch = 3;
        assignment.incarnations.insert("A".into(), 3);
        o.sync(assignment);
        assert!(snap(&mut o, source(0), 12, 90).is_empty());
        assert!(!o.selected["A"].ever);
        assert_eq!(o.stats.stale_events, 2);
    }
    #[test]
    fn both_sources_unavailable_report_a_gap_and_recover_from_a_fresh_baseline() {
        let mut o = owner();
        start(&mut o, 0, 10, 10);
        start(&mut o, 1, 10, 10);
        o.input(Input::Invalid {
            source: source(0),
            symbols: None,
            reason: "A down".into(),
        })
        .unwrap();
        o.input(Input::Invalid {
            source: source(1),
            symbols: None,
            reason: "B down".into(),
        })
        .unwrap();
        assert!(o.selected["A"].active.is_none());
        assert_eq!(o.stats.continuity_gaps, 1);
        let fresh = Source {
            replica: 0,
            generation: 2,
        };
        begin(&mut o, fresh);
        o.input(Input::Membership {
            roster: None,
            source: fresh,
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        assert!(
            matches!(&snap(&mut o,fresh,11,20)[..],[Emit::Snapshot {reason:SnapshotReason::Recovery,stamp,..}] if stamp.output_epoch==2)
        );
    }
    #[test]
    fn single_source_reconnect_cannot_hide_a_maintenance_clock_reset() {
        let mut o = owner();
        o.feeds.truncate(1);
        o.replicas = 1;
        start(&mut o, 0, 100, 10);
        o.input(Input::Invalid {
            source: source(0),
            symbols: None,
            reason: "disconnect".into(),
        })
        .unwrap();
        let fresh = Source {
            replica: 0,
            generation: 2,
        };
        begin(&mut o, fresh);
        o.input(Input::Membership {
            roster: None,
            source: fresh,
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        // The transport book has no frontier yet, but published state still
        // belongs to clock 100. A reset must change the continuity epoch.
        assert!(snap(&mut o, fresh, 1, 20).is_empty());
        assert_eq!(o.stats.hard_resets, 1);
        o.rotation_tick().unwrap();
        o.release_planned().unwrap();
        let clean = Source {
            replica: 0,
            generation: 3,
        };
        begin(&mut o, clean);
        o.input(Input::Membership {
            roster: None,
            source: clean,
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        assert!(
            matches!(&snap(&mut o,clean,1,20)[..], [Emit::Snapshot {reason: SnapshotReason::Recovery, stamp,..}] if stamp.continuity_epoch==1 && stamp.output_epoch==2)
        );
    }
    #[test]
    fn explicit_symbol_reset_fences_old_generations_and_exposes_a_gap() {
        let mut o = owner();
        start(&mut o, 0, 100, 10);
        start(&mut o, 1, 100, 10);
        o.reset_symbol("A", "maintenance").unwrap();
        o.rotation_tick().unwrap();
        o.release_planned().unwrap();
        assert_eq!(o.stats.hard_resets, 1);
        assert_eq!(o.stats.continuity_gaps, 1);
        assert!(delta(&mut o, source(1), 101, 1).is_empty());
        assert!(o.selected["A"].active.is_none());
        let fresh = Source {
            replica: 0,
            generation: 2,
        };
        begin(&mut o, fresh);
        o.input(Input::Membership {
            roster: None,
            source: fresh,
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        assert!(
            matches!(&snap(&mut o,fresh,2,30)[..],[Emit::Snapshot {reason:SnapshotReason::Recovery,stamp,..}] if stamp.output_epoch==2)
        );
    }
    #[test]
    fn active_clock_regression_uses_covering_peer_without_resetting_other_books() {
        let mut o = multibook_owner();
        assert!(delta(&mut o, source(0), 1, 1).is_empty());
        assert_eq!(o.stats.hard_resets, 0);
        assert_eq!(o.stats.continuity_gaps, 0);
        let handoff = cut(&mut o, 1);
        assert!(
            matches!(&handoff[..], [Emit::Snapshot { symbol, stamp, .. }]
            if symbol == "A" && stamp.continuity_epoch == 0)
        );
        assert_eq!(o.selected["A"].active, Some(source(1)));
        assert_eq!(o.selected["B"].active, Some(source(0)));
        assert_eq!(o.selected["C"].floor, Some(Frontier::snapshot(100)));
    }
    #[test]
    fn scoped_reset_does_not_relabel_a_pre_cut_dial_or_change_other_epochs() {
        let mut o = multibook_owner();
        o.reset_symbol("A", "maintenance").unwrap();
        assert_eq!(o.stats.continuity_gaps, 1);
        assert_eq!(o.selected["A"].continuity_epoch, 1);
        assert_eq!(o.selected["B"].continuity_epoch, 0);
        assert_eq!(o.selected["C"].active, Some(source(0)));
        assert!(snap(&mut o, source(1), 101, 20).is_empty());
        let pre_cut_dial = Source {
            replica: 1,
            generation: 2,
        };
        o.input(Input::Begin {
            source: pre_cut_dial,
            admission: 0,
        })
        .unwrap();
        o.input(Input::Membership {
            roster: None,
            source: pre_cut_dial,
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        assert!(snap(&mut o, pre_cut_dial, 1, 20).is_empty());
        assert!(o.selected["A"].active.is_none());
        assert_eq!(o.selected["B"].active, Some(source(0)));
    }
    #[tokio::test(start_paused = true)]
    async fn stale_fresh_baseline_is_not_ready_and_cannot_reset_semantic_retry_budget() {
        let mut o = owner();
        start(&mut o, 0, 100, 20);
        start(&mut o, 1, 100, 20);
        snap(&mut o, source(1), 0, 20);
        assert_eq!(o.feeds[1].books["A"].rejected_baselines, 1);
        let fresh = Source {
            replica: 1,
            generation: 2,
        };
        begin(&mut o, fresh);
        o.input(Input::Membership {
            roster: None,
            source: fresh,
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        snap(&mut o, fresh, 0, 20);
        assert!(!o.ready(1, "A"));
        assert_eq!(o.feeds[1].books["A"].rejected_baselines, 1);
        tokio::time::advance(Duration::from_secs(6)).await;
        o.tick().unwrap();
        assert_eq!(o.feeds[1].books["A"].rejected_baselines, 2);
        snap(&mut o, fresh, 0, 20);
        // Each rejection ends the validity epoch, so its answer gets a full
        // window. Budget, readiness and fatal assertions here are unchanged.
        tokio::time::advance(Duration::from_secs(5)).await;
        o.tick().unwrap();
        assert_eq!(o.feeds[1].books["A"].rejected_baselines, 3);
        snap(&mut o, fresh, 0, 20);
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(
            o.tick()
                .unwrap_err()
                .to_string()
                .contains("semantic recovery exhausted")
        );
        assert_eq!(o.stats.hard_resets, 0);
        assert_eq!(o.selected["A"].active, Some(source(0)));
    }
    #[tokio::test]
    async fn output_admission_failure_cannot_release_a_destructive_restart() {
        let mut o = owner();
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 10, 20);
        o.start_rotation(0).unwrap();
        let baseline = cut(&mut o, 1);
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        let temp = tempfile::tempdir().unwrap();
        let writer =
            crate::output::Writer::start(&temp.path().join("out.sbe"), None, 1, 1).unwrap();
        assert!(o.publish(&writer.output, baseline).await.is_err());
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        writer.finish().await.unwrap();
    }
    #[tokio::test(start_paused = true)]
    async fn health_baseline_enters_output_before_control_and_source_start_fences_old_data() {
        let mut o = owner();
        o.health.window = Duration::from_secs(4);
        o.health.warmup = Duration::from_secs(4);
        o.health.cooldown = Duration::from_secs(4);
        o.health.expected_points = 100;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("out.sbe");
        let audit = temp.path().join("audit.jsonl");
        let writer = crate::output::Writer::start(&path, Some(&audit), 16, 1 << 20).unwrap();
        let initial = start(&mut o, 0, 10, 20);
        o.publish(&writer.output, initial).await.unwrap();
        start(&mut o, 1, 10, 20);
        tokio::time::advance(Duration::from_secs(4)).await;
        o.activity_tick().unwrap();
        let baseline = cut(&mut o, 1);
        assert_eq!(o.stats.health_resubscriptions, 0);
        assert!(o.feeds[0].control.borrow().resubscribe.is_empty());
        o.publish(&writer.output, baseline).await.unwrap();
        assert_eq!(o.stats.health_resubscriptions, 1);
        let request = o.feeds[0].control.borrow().resubscribe["A"];
        // Admitted before Source consumed the control: even a convincing book
        // cannot complete this recovery until the matching FIFO start marker.
        assert!(snap(&mut o, source(0), 1000, 99).is_empty());
        let mut wrong = request;
        wrong.nonce += 1;
        o.input(Input::RecoveryStarted {
            source: source(0),
            requests: std::collections::BTreeMap::from([("A".into(), wrong)]),
        })
        .unwrap();
        assert!(snap(&mut o, source(0), 11, 21).is_empty());
        o.input(Input::RecoveryStarted {
            source: source(0),
            requests: std::collections::BTreeMap::from([("A".into(), request)]),
        })
        .unwrap();
        o.input(Input::Membership {
            roster: None,
            source: source(0),
            wire: BTreeSet::new(),
            at: Utc::now(),
        })
        .unwrap();
        snap(&mut o, source(0), 11, 21);
        assert!(!o.ready(0, "A"));
        assert!(o.feeds[0].books["A"].activity.recovering);
        o.input(Input::Membership {
            roster: None,
            source: source(0),
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        assert!(o.ready(0, "A"));
        assert!(!o.feeds[0].books["A"].activity.recovering);
        assert_eq!(o.stats.continuity_gaps, 0);
        tokio::time::resume();
        writer.finish().await.unwrap();
        let report = crate::replay::verify(&path, Some(&audit), 10).unwrap();
        assert_eq!(report.failover_baselines_verified, 1);
    }
    #[test]
    fn regressed_active_clock_gives_a_behind_peer_time_to_catch_up() {
        let mut o = owner();
        start(&mut o, 0, 100, 20);
        start(&mut o, 1, 100, 20);
        delta(&mut o, source(0), 101, 1);
        snap(&mut o, source(0), 0, 20);
        assert!(cut(&mut o, 1).is_empty());
        assert_eq!(o.stats.hard_resets, 0);
        delta(&mut o, source(1), 101, 1);
        let handoff = cut(&mut o, 1);
        assert!(matches!(
            &handoff[..],
            [Emit::Snapshot {
                source: Source { replica: 1, .. },
                ..
            }]
        ));
        assert_eq!(o.stats.continuity_gaps, 0);
    }
    #[test]
    fn scoped_reset_renews_both_sources_without_resetting_other_symbols() {
        let mut o = multibook_owner();
        o.reset_symbol("A", "maintenance").unwrap();
        for slot in 0..2 {
            o.rotation_tick().unwrap();
            cut(&mut o, 1 - slot);
            o.release_planned().unwrap(); // model admitted handoff baselines
            let fresh = Source {
                replica: slot,
                generation: 2,
            };
            begin(&mut o, fresh);
            o.input(Input::Membership {
                roster: None,
                source: fresh,
                wire: BTreeSet::from(["A".into(), "B".into(), "C".into()]),
                at: Utc::now(),
            })
            .unwrap();
            for symbol in ["A", "B", "C"] {
                o.input(Input::Data {
                    source: fresh,
                    incarnation: 1,
                    received_tick: Instant::now(),
                    event: BookEvent::Snapshot(Snapshot {
                        symbol: symbol.into(),
                        received_at: Utc::now(),
                        exchange_at: Utc::now(),
                        clock: Some(if symbol == "A" { 1 } else { 100 }),
                        bids: vec![Level {
                            price: 420_000,
                            quantity: 10,
                        }],
                        asks: vec![],
                    }),
                })
                .unwrap();
            }
            o.rotation_tick().unwrap();
        }
        assert!(o.reset_restarts.is_empty());
        assert_eq!(o.stats.completed_rotations, 2);
        assert_eq!(o.stats.continuity_gaps, 1);
        for slot in 0..2 {
            for symbol in ["A", "B", "C"] {
                assert!(o.ready(slot, symbol));
            }
        }
        assert_eq!(o.selected["A"].continuity_epoch, 1);
        assert_eq!(o.selected["B"].continuity_epoch, 0);
    }
    #[tokio::test(start_paused = true)]
    async fn a_health_proposal_that_recovers_while_fencing_spends_no_attempt() {
        let mut o = owner();
        o.health.window = Duration::from_secs(4);
        o.health.warmup = Duration::from_secs(4);
        o.health.cooldown = Duration::from_secs(4);
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 10, 20);
        for _ in 0..20 {
            o.feeds[1]
                .books
                .get_mut("A")
                .unwrap()
                .activity
                .window
                .record(3);
        }
        tokio::time::advance(Duration::from_secs(4)).await;
        o.activity_tick().unwrap();
        for slot in 0..2 {
            for _ in 0..20 {
                o.feeds[slot]
                    .books
                    .get_mut("A")
                    .unwrap()
                    .activity
                    .window
                    .record(4);
            }
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        cut(&mut o, 1);
        o.release_planned().unwrap();
        assert_eq!(o.stats.health_resubscriptions, 0);
        assert!(o.feeds[0].control.borrow().resubscribe.is_empty());
        assert!(!o.feeds[0].books["A"].activity.recovering);
    }
    #[test]
    fn usable_baseline_clears_semantic_budget_at_frozen_witness_not_a_moving_peer() {
        let mut o = owner();
        start(&mut o, 0, 100, 20);
        start(&mut o, 1, 100, 20);
        snap(&mut o, source(1), 0, 20);
        let fresh = Source {
            replica: 1,
            generation: 2,
        };
        begin(&mut o, fresh);
        o.input(Input::Membership {
            roster: None,
            source: fresh,
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        snap(&mut o, fresh, 90, 20);
        assert!(!o.ready(1, "A"));
        delta(&mut o, source(0), 101, 1);
        snap(&mut o, fresh, 100, 20);
        assert!(o.ready(1, "A"));
        assert_eq!(o.feeds[1].books["A"].rejected_baselines, 0);
        assert_eq!(o.selected["A"].active, Some(source(0)));
        assert_eq!(o.selected["A"].floor.unwrap().clock, 101);
        assert!(o.waiting_catchup.is_empty());
    }
    #[test]
    fn comparable_profile_requires_clocks_even_with_one_replica() {
        for replicas in [1, 2] {
            for snapshot in [true, false] {
                let mut o = owner();
                o.feeds.truncate(replicas);
                o.replicas = replicas;
                start(&mut o, 0, 10, 5);
                let event = if snapshot {
                    BookEvent::Snapshot(Snapshot {
                        symbol: "A".into(),
                        received_at: Utc::now(),
                        exchange_at: Utc::now(),
                        clock: None,
                        bids: vec![Level {
                            price: 420_000,
                            quantity: 6,
                        }],
                        asks: vec![],
                    })
                } else {
                    BookEvent::Update(Update {
                        symbol: "A".into(),
                        received_at: Utc::now(),
                        exchange_at: Utc::now(),
                        clock: None,
                        side: Side::Buy,
                        price: 420_000,
                        quantity: QuantityUpdate::Change(1),
                    })
                };
                let error = o
                    .input(Input::Data {
                        source: source(0),
                        incarnation: 1,
                        received_tick: Instant::now(),
                        event,
                    })
                    .err()
                    .expect("native data without clock must fail");
                assert!(error.to_string().contains("lacks clock"));
                assert_eq!(o.selected["A"].floor.unwrap().clock, 10);
            }
        }
    }
    #[tokio::test(start_paused = true)]
    async fn planned_rotation_can_stage_partial_cuts_while_one_market_catches_up() {
        let mut o = multibook_owner();
        delta(&mut o, source(0), 101, 1);
        // Admission is readiness, not a simultaneous all-market frontier cut.
        assert!(o.start_rotation(0).unwrap());
        let first = cut(&mut o, 1);
        assert_eq!(first.len(), 2);
        assert_eq!(o.selected["A"].active, Some(source(0)));
        assert_eq!(o.selected["B"].active, Some(source(1)));
        o.release_planned().unwrap(); // model admission of B/C baselines
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        delta(&mut o, source(1), 101, 1);
        tokio::time::advance(o.lag).await;
        o.rotation_tick().unwrap();
        let last = cut(&mut o, 1);
        assert!(matches!(&last[..], [Emit::Snapshot { symbol, .. }] if symbol=="A"));
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        o.release_planned().unwrap(); // model admission of A's covering baseline
        assert_eq!(o.feeds[0].control.borrow().restart, 1);
        assert_eq!(o.stats.continuity_gaps, 0);
        assert_eq!(o.stats.hard_resets, 0);
    }

    #[test]
    fn published_clockless_lifetime_keeps_last_ready_feed_while_failover_fence_is_pending() {
        let mut o = owner();
        o.contract = crate::exchange::polymarket::Polymarket.contract();
        // Publish from slot 1 first; slot 0 is a usable warm standby. A public
        // baseline has no comparable clock even after publication.
        for slot in [1, 0] {
            begin(&mut o, source(slot));
            o.input(Input::Membership {
                roster: Some(o.assignment.incarnations.clone()),
                source: source(slot),
                wire: BTreeSet::from(["A".into()]),
                at: Utc::now(),
            })
            .unwrap();
            let emitted = o
                .input(Input::Data {
                    source: source(slot),
                    incarnation: 1,
                    received_tick: Instant::now(),
                    event: BookEvent::Snapshot(Snapshot {
                        symbol: "A".into(),
                        received_at: Utc::now(),
                        exchange_at: Utc::now(),
                        clock: None,
                        bids: vec![Level {
                            price: 420_000,
                            quantity: 10,
                        }],
                        asks: vec![],
                    }),
                })
                .unwrap();
            assert_eq!(emitted.len(), usize::from(slot == 1));
        }
        assert!(o.selected["A"].ever);
        assert_eq!(o.selected["A"].active, Some(source(1)));
        assert!(o.selected["A"].floor.is_none());
        assert!(o.ready(0, "A") && o.ready(1, "A"));

        // A real roster change queues renewal of both feeds. B has never been
        // published and therefore must not block renewing its initial roster.
        let mut assignment = o.assignment.clone();
        assignment.epoch += 1;
        assignment.symbols.push("B".into());
        assignment.incarnations.insert("B".into(), 1);
        o.sync(assignment);
        assert_eq!(o.needs_renewal, BTreeSet::from([0, 1]));
        o.input(Input::Invalid {
            source: source(1),
            symbols: None,
            reason: "actual active connection failure".into(),
        })
        .unwrap();
        assert!(o.selected["A"].active.is_none());
        assert_eq!(o.selected["A"].candidate, Some(0));
        assert!(o.feeds[0].pending_fence.is_some());
        assert!(o.ready(0, "A"));
        assert!(!o.ready(1, "A"));
        assert_eq!(o.stats.continuity_gaps, 0);

        // Interleave the maintenance tick before slot 0's queued fence arrives.
        // It must not restart the only usable feed before selecting/admitting
        // a survivor baseline for the already-published A lifetime.
        o.tick().unwrap();
        o.rotation_tick().unwrap();
        o.release_planned().unwrap();
        assert_eq!(
            o.feeds[0].control.borrow().restart,
            0,
            "planned renewal removed the last ready clockless feed before its failover fence"
        );
        assert!(o.ready(0, "A"));
        assert_eq!(o.stats.continuity_gaps, 0);
    }

    #[test]
    fn public_transport_failure_does_not_request_a_duplicate_planned_restart() {
        let mut o = owner();
        o.contract = crate::exchange::polymarket::Polymarket.contract();
        begin(&mut o, source(0));
        o.input(Input::Invalid {
            source: source(0),
            symbols: None,
            reason: "transport is already reconnecting".into(),
        })
        .unwrap();
        assert!(o.needs_refresh.is_empty());
        o.rotation_tick().unwrap();
        assert_eq!(o.stats.planned_rotations, 0);
    }

    #[test]
    fn refresh_controls_do_not_survive_a_ticker_reincarnation() {
        let mut o = owner();
        start(&mut o, 0, 10, 5);
        o.refresh(0, "A");
        let old = o.feeds[0].control.borrow().refresh["A"];
        let mut next = o.assignment.clone();
        next.epoch += 1;
        next.incarnations.insert("A".into(), 2);
        o.sync(next);
        assert!(o.feeds[0].control.borrow().refresh.is_empty());
        o.refresh(0, "A");
        let new = o.feeds[0].control.borrow().refresh["A"];
        assert_eq!(old.nonce, new.nonce); // nonce reuse is safe only with identity
        assert_ne!(old, new);
        assert_eq!(new.incarnation, 2);
    }

    #[test]
    fn public_rotation_completion_requires_actual_write_and_a_usable_new_baseline() {
        let mut o = owner();
        o.contract = crate::exchange::polymarket::Polymarket.contract();
        begin(&mut o, source(0));
        assert!(o.start_rotation(0).unwrap());
        o.release_planned().unwrap();
        let fresh = Source {
            replica: 0,
            generation: 2,
        };
        begin(&mut o, fresh);
        o.rotation_tick().unwrap();
        assert_eq!(
            o.stats.completed_rotations, 0,
            "dialing a new generation is not completed renewal"
        );
        o.input(Input::Membership {
            source: fresh,
            roster: Some(o.assignment.incarnations.clone()),
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        o.rotation_tick().unwrap();
        assert_eq!(
            o.stats.completed_rotations, 0,
            "a local write is not a usable baseline"
        );
        o.input(Input::Data {
            source: fresh,
            incarnation: 1,
            received_tick: Instant::now(),
            event: BookEvent::Snapshot(Snapshot {
                symbol: "A".into(),
                received_at: Utc::now(),
                exchange_at: Utc::now(),
                clock: None,
                bids: vec![],
                asks: vec![],
            }),
        })
        .unwrap();
        o.rotation_tick().unwrap();
        assert_eq!(o.stats.completed_rotations, 1);
    }
    #[test]
    fn owned_fault_marks_the_closing_feed_unavailable_before_maintenance_can_claim_it() {
        let mut o = owner();
        start(&mut o, 0, 100, 10);
        start(&mut o, 1, 100, 10);
        assert_eq!(o.inject_fault().unwrap(), Some(source(0)));
        assert!(
            !o.ready(0, "A"),
            "a published close intent must not remain a usable survivor"
        );
        assert!(delta(&mut o, source(0), 101, 1).is_empty());
        assert!(!o.start_rotation(1).unwrap());
        assert_eq!(o.feeds[1].control.borrow().restart, 0);
        assert_eq!(o.stats.continuity_gaps, 0);
        let emitted = cut(&mut o, 1);
        assert!(matches!(
            &emitted[..],
            [Emit::Snapshot {
                reason: SnapshotReason::Failover,
                ..
            }]
        ));
        assert_eq!(o.stats.continuity_gaps, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn planned_pause_freezes_a_moving_floor_until_the_slower_peer_can_cover() {
        let mut o = owner();
        start(&mut o, 0, 100, 10);
        start(&mut o, 1, 100, 10);
        assert!(o.start_rotation(0).unwrap());
        assert!(o.feeds[0].control.borrow().pause.is_some());
        assert!(o.feeds[1].pending_fence.is_none());
        // Until Source consumes the pause, its existing ordered tail may move
        // the publisher ahead of every earlier peer observation.
        for clock in 101..=104 {
            delta(&mut o, source(0), clock, 1);
        }
        delta(&mut o, source(1), 101, 1);
        acknowledge_pause(&mut o);
        assert_eq!(o.selected["A"].floor.unwrap().clock, 104);
        assert!(cut(&mut o, 1).is_empty());
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        for clock in 102..=104 {
            delta(&mut o, source(1), clock, 1);
        }
        tokio::time::advance(o.lag).await;
        o.rotation_tick().unwrap();
        let baseline = cut(&mut o, 1);
        assert!(
            matches!(&baseline[..], [Emit::Snapshot { source: Source { replica: 1, .. }, stamp, .. }] if stamp.clock == Some(104))
        );
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        o.release_planned().unwrap(); // model successful output queue admission
        assert_eq!(o.feeds[0].control.borrow().restart, 1);
        assert!(o.feeds[0].control.borrow().pause.is_none());
        assert!(o.feeds[0].control.borrow().pause_release.is_some());
        assert_eq!(o.stats.continuity_gaps, 0);
        assert_eq!(o.stats.hard_resets, 0);
    }

    #[test]
    fn a_pre_pause_fence_cannot_authorize_the_planned_cut() {
        let mut o = owner();
        start(&mut o, 0, 100, 10);
        start(&mut o, 1, 100, 10);
        o.fence(1).unwrap();
        let old = o.feeds[1].pending_fence.unwrap().1;
        assert!(o.start_rotation(0).unwrap());
        assert!(
            o.input(Input::Fence {
                source: source(1),
                request: old,
                at: Utc::now()
            })
            .unwrap()
            .is_empty()
        );
        assert_eq!(o.selected["A"].active, Some(source(0)));
        acknowledge_pause(&mut o);
        let fresh = o.feeds[1].pending_fence.unwrap().1;
        assert!(fresh > old);
        assert!(
            o.input(Input::Fence {
                source: source(1),
                request: old,
                at: Utc::now()
            })
            .unwrap()
            .is_empty()
        );
        assert_eq!(o.selected["A"].active, Some(source(0)));
        assert_eq!(cut(&mut o, 1).len(), 1);
    }

    #[test]
    fn cancelled_pause_stays_reserved_until_end_and_old_end_cannot_release_its_successor() {
        let mut o = owner();
        start(&mut o, 0, 100, 10);
        start(&mut o, 1, 100, 10);
        assert!(o.start_rotation(0).unwrap());
        let old = o.feeds[0].control.borrow().pause.unwrap();
        assert!(o.ready(0, "A"));
        assert!(!o.survivor_ready(0, "A"));
        assert!(o.inject_fault().unwrap().is_none());
        o.cancel_planned(); // Source may not have consumed the request at all.
        assert!(o.feeds[0].control.borrow().pause.is_none());
        assert_eq!(
            o.feeds[0].control.borrow().pause_release,
            Some(crate::relay::PauseId {
                generation: 1,
                nonce: old.nonce
            })
        );
        assert!(!o.start_rotation(1).unwrap());
        assert!(!o.survivor_ready(0, "A"));
        o.input(Input::PauseEnded {
            source: source(0),
            nonce: old.nonce,
            reason: crate::relay::PauseEnd::Released,
        })
        .unwrap();
        assert!(o.survivor_ready(0, "A"));
        assert!(o.start_rotation(0).unwrap());
        let current = o.feeds[0].control.borrow().pause.unwrap();
        assert!(current.nonce > old.nonce);
        o.input(Input::PauseEnded {
            source: source(0),
            nonce: old.nonce,
            reason: crate::relay::PauseEnd::Expired,
        })
        .unwrap();
        o.input(Input::PauseStarted {
            source: source(0),
            nonce: old.nonce,
            until: Instant::now() + Duration::from_secs(5),
        })
        .unwrap();
        assert_eq!(o.feeds[0].control.borrow().pause, Some(current));
        assert!(!o.survivor_ready(0, "A"));
        acknowledge_pause(&mut o);
        assert_eq!(cut(&mut o, 1).len(), 1);
    }

    #[test]
    fn data_after_a_pause_marker_without_an_end_is_an_invariant_failure() {
        let mut o = owner();
        start(&mut o, 0, 100, 10);
        start(&mut o, 1, 100, 10);
        o.start_rotation(0).unwrap();
        acknowledge_pause(&mut o);
        let error = o
            .input(Input::Data {
                source: source(0),
                incarnation: 1,
                received_tick: Instant::now(),
                event: BookEvent::Update(Update {
                    symbol: "A".into(),
                    received_at: Utc::now(),
                    exchange_at: Utc::now(),
                    clock: Some(101),
                    side: Side::Buy,
                    price: 420_000,
                    quantity: QuantityUpdate::Change(1),
                }),
            })
            .err()
            .expect("paused Source must not deliver more Data");
        assert!(error.to_string().contains("without PauseEnded"));
        assert_eq!(o.selected["A"].floor.unwrap().clock, 100);
    }

    #[tokio::test(start_paused = true)]
    async fn expired_uncut_pause_resumes_without_a_forced_reset_or_a_destructive_control() {
        let mut o = owner();
        start(&mut o, 0, 100, 10);
        start(&mut o, 1, 100, 10);
        o.start_rotation(0).unwrap();
        let token = o.feeds[0].control.borrow().pause.unwrap();
        acknowledge_pause(&mut o);
        tokio::time::advance(Duration::from_secs(6)).await;
        o.rotation_tick().unwrap();
        assert!(o.planned.is_none());
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        assert_eq!(o.selected["A"].active, Some(source(0)));
        assert!(o.feeds[0].pause.is_some());
        assert!(!o.survivor_ready(0, "A"));
        o.input(Input::PauseEnded {
            source: source(0),
            nonce: token.nonce,
            reason: crate::relay::PauseEnd::Expired,
        })
        .unwrap();
        assert!(o.feeds[0].pause.is_none());
        delta(&mut o, source(0), 101, 1);
        assert_eq!(o.selected["A"].floor.unwrap().clock, 101);
        assert_eq!(o.stats.hard_resets, 0);
        assert_eq!(o.stats.continuity_gaps, 0);
        assert_eq!(o.stats.rotation_timeouts, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_cut_baseline_admitted_after_lease_expiry_still_precedes_its_wire_action() {
        let mut o = owner();
        let temp = tempfile::tempdir().unwrap();
        let writer =
            crate::output::Writer::start(&temp.path().join("pause.sbe"), None, 16, 1 << 20)
                .unwrap();
        let initial = start(&mut o, 0, 100, 10);
        o.publish(&writer.output, initial).await.unwrap();
        start(&mut o, 1, 100, 10);
        o.start_rotation(0).unwrap();
        let token = o.feeds[0].control.borrow().pause.unwrap();
        let baseline = cut(&mut o, 1);
        assert_eq!(o.selected["A"].active, Some(source(1)));
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        // Model output backpressure: the logical cut happened while paused;
        // queue acceptance happens later. Target ingress no longer publishes.
        tokio::time::advance(Duration::from_secs(6)).await;
        o.publish(&writer.output, baseline).await.unwrap();
        assert_eq!(o.feeds[0].control.borrow().restart, 1);
        o.input(Input::PauseEnded {
            source: source(0),
            nonce: token.nonce,
            reason: crate::relay::PauseEnd::Expired,
        })
        .unwrap();
        assert_eq!(o.stats.rotation_timeouts, 0);
        assert_eq!(o.stats.continuity_gaps, 0);
        tokio::time::resume();
        writer.finish().await.unwrap();
    }

    #[test]
    fn peer_fault_and_roster_change_cancel_the_pause_without_restarting_the_publisher() {
        for roster_change in [false, true] {
            let mut o = owner();
            start(&mut o, 0, 100, 10);
            start(&mut o, 1, 100, 10);
            o.start_rotation(0).unwrap();
            let token = o.feeds[0].control.borrow().pause.unwrap();
            acknowledge_pause(&mut o);
            if roster_change {
                let mut assignment = o.assignment.clone();
                assignment.epoch += 1;
                assignment.symbols.push("B".into());
                assignment.incarnations.insert("B".into(), 1);
                o.sync(assignment);
            } else {
                o.input(Input::Invalid {
                    source: source(1),
                    symbols: None,
                    reason: "real peer fault during pause".into(),
                })
                .unwrap();
            }
            assert!(o.planned.is_none());
            assert!(o.feeds[0].control.borrow().pause.is_none());
            assert_eq!(o.feeds[0].control.borrow().restart, 0);
            assert!(o.feeds[0].pause.is_some());
            o.input(Input::PauseEnded {
                source: source(0),
                nonce: token.nonce,
                reason: crate::relay::PauseEnd::Released,
            })
            .unwrap();
            delta(&mut o, source(0), 101, 1);
            assert_eq!(o.selected["A"].floor.unwrap().clock, 101);
            assert_eq!(o.stats.continuity_gaps, 0);
        }
    }

    #[test]
    fn quiescence_keeps_a_cancelled_pause_pending_until_source_acknowledges_resume() {
        let mut o = owner();
        start(&mut o, 0, 100, 10);
        start(&mut o, 1, 100, 10);
        o.start_rotation(0).unwrap();
        let token = o.feeds[0].control.borrow().pause.unwrap();
        acknowledge_pause(&mut o);
        o.quiescing = true;
        o.cancel_planned();
        o.cancel_pauses();
        let (progress, row) = watch::channel(ShardStats::default());
        o.report(&[], &progress);
        assert_eq!(row.borrow().maintenance_pending, 1);
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        o.input(Input::PauseEnded {
            source: source(0),
            nonce: token.nonce,
            reason: crate::relay::PauseEnd::Released,
        })
        .unwrap();
        o.report(&[], &progress);
        assert_eq!(row.borrow().maintenance_pending, 0);
        assert_eq!(o.stats.continuity_gaps, 0);
    }

    #[test]
    fn a_wholly_standby_rotation_needs_no_ingress_pause() {
        let mut o = owner();
        start(&mut o, 0, 100, 10);
        start(&mut o, 1, 100, 10);
        assert!(o.start_rotation(1).unwrap());
        assert!(
            o.feeds
                .iter()
                .all(|f| f.pause.is_none() && f.control.borrow().pause.is_none())
        );
        o.release_planned().unwrap();
        assert_eq!(o.feeds[1].control.borrow().restart, 1);
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        assert_eq!(o.stats.continuity_gaps, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn acquisition_timeout_keeps_reservation_and_rejects_a_late_started_marker() {
        let mut o = owner();
        start(&mut o, 0, 100, 10);
        start(&mut o, 1, 100, 10);
        o.start_rotation(0).unwrap();
        let token = o.feeds[0].control.borrow().pause.unwrap();
        tokio::time::advance(Duration::from_secs(6)).await;
        o.rotation_tick().unwrap();
        assert!(o.planned.is_none());
        assert!(!o.survivor_ready(0, "A"));
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        o.input(Input::PauseStarted {
            source: source(0),
            nonce: token.nonce,
            until: Instant::now() + Duration::from_secs(5),
        })
        .unwrap();
        assert!(o.planned.is_none());
        assert!(o.feeds[1].pending_fence.is_none());
        assert!(o.feeds[0].pause.is_some());
        o.input(Input::PauseEnded {
            source: source(0),
            nonce: token.nonce,
            reason: crate::relay::PauseEnd::Released,
        })
        .unwrap();
        assert!(o.survivor_ready(0, "A"));
        assert_eq!(o.stats.rotation_timeouts, 1);
        assert_eq!(o.stats.continuity_gaps, 0);
    }

    #[test]
    fn replacement_generation_resolves_old_pause_and_rejects_its_late_markers() {
        let mut o = owner();
        start(&mut o, 0, 100, 10);
        start(&mut o, 1, 100, 10);
        o.start_rotation(0).unwrap();
        let old = o.feeds[0].control.borrow().pause.unwrap();
        acknowledge_pause(&mut o);
        let fresh = Source {
            replica: 0,
            generation: 2,
        };
        begin(&mut o, fresh);
        assert!(o.planned.is_none());
        assert!(o.feeds[0].pause.is_none());
        o.input(Input::Membership {
            roster: None,
            source: fresh,
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        snap(&mut o, fresh, 100, 10);
        assert!(o.ready(0, "A"));
        o.start_rotation(0).unwrap();
        let current = o.feeds[0].control.borrow().pause.unwrap();
        assert_eq!(current.generation, 2);
        assert!(current.nonce > old.nonce);
        o.input(Input::PauseEnded {
            source: source(0),
            nonce: old.nonce,
            reason: crate::relay::PauseEnd::Expired,
        })
        .unwrap();
        assert_eq!(o.feeds[0].control.borrow().pause, Some(current));
        assert!(!o.survivor_ready(0, "A"));
        acknowledge_pause(&mut o);
        assert_eq!(cut(&mut o, 1).len(), 1);
    }

    #[test]
    fn source_busy_defers_pause_without_spending_recovery_or_restarting() {
        let mut o = owner();
        start(&mut o, 0, 100, 10);
        start(&mut o, 1, 100, 10);
        o.start_rotation(0).unwrap();
        let request = o.feeds[0].control.borrow().pause.unwrap();
        o.input(Input::PauseEnded {
            source: source(0),
            nonce: request.nonce,
            reason: crate::relay::PauseEnd::Busy,
        })
        .unwrap();
        assert!(o.planned.is_none());
        assert!(o.rotation.is_none());
        assert!(o.feeds[0].pause.is_none());
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        assert_eq!(o.stats.rotation_timeouts, 0);
        assert_eq!(o.stats.health_resubscriptions, 0);
        assert_eq!(o.stats.deferred_rotations, 1);
        assert_eq!(o.stats.continuity_gaps, 0);
    }

    #[test]
    fn public_handoff_uses_a_fresh_fence_without_native_pause_and_preserves_uncertainty() {
        let mut o = owner();
        o.contract = crate::exchange::polymarket::Polymarket.contract();
        for slot in 0..2 {
            begin(&mut o, source(slot));
            o.input(Input::Membership {
                source: source(slot),
                roster: Some(o.assignment.incarnations.clone()),
                wire: BTreeSet::from(["A".into()]),
                at: Utc::now(),
            })
            .unwrap();
            o.input(Input::Data {
                source: source(slot),
                incarnation: 1,
                received_tick: Instant::now(),
                event: BookEvent::Snapshot(Snapshot {
                    symbol: "A".into(),
                    received_at: Utc::now(),
                    exchange_at: Utc::now(),
                    clock: None,
                    bids: vec![Level {
                        price: 420_000,
                        quantity: 10,
                    }],
                    asks: vec![],
                }),
            })
            .unwrap();
        }
        o.fence(1).unwrap();
        let old = o.feeds[1].pending_fence.unwrap().1;
        assert!(o.start_rotation(0).unwrap());
        let fresh = o.feeds[1].pending_fence.unwrap().1;
        assert!(fresh > old);
        assert!(o.feeds.iter().all(|feed| feed.pause.is_none()
            && feed.control.borrow().pause.is_none()
            && feed.control.borrow().pause_release.is_none()));
        assert!(
            o.input(Input::Fence {
                source: source(1),
                request: old,
                at: Utc::now()
            })
            .unwrap()
            .is_empty()
        );
        assert_eq!(o.selected["A"].active, Some(source(0)));
        let baseline = cut(&mut o, 1);
        assert!(
            matches!(&baseline[..], [Emit::Snapshot { stamp, reason: SnapshotReason::Failover, .. }]
            if stamp.evidence == OrderingEvidence::SnapshotOnly && stamp.clock.is_none()
            && stamp.ordinal.is_none() && !stamp.complete_clock && stamp.unverified_cut)
        );
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        o.release_planned().unwrap();
        assert_eq!(o.feeds[0].control.borrow().restart, 1);
        assert!(o.feeds.iter().all(|feed| feed.pause.is_none()
            && feed.control.borrow().pause.is_none()
            && feed.control.borrow().pause_release.is_none()));
        assert_eq!(o.next_pause, 0);
        assert_eq!(o.stats.unverified_transitions, 1);
    }

    #[test]
    fn peer_clock_or_book_quarantine_immediately_releases_a_native_target_pause() {
        for malformed_book in [false, true] {
            let mut o = owner();
            start(&mut o, 0, 100, 10);
            start(&mut o, 1, 100, 10);
            o.start_rotation(0).unwrap();
            let token = o.feeds[0].control.borrow().pause.unwrap();
            acknowledge_pause(&mut o);
            o.input(Input::Data {
                source: source(1),
                incarnation: 1,
                received_tick: Instant::now(),
                event: BookEvent::Snapshot(Snapshot {
                    symbol: "A".into(),
                    received_at: Utc::now(),
                    exchange_at: Utc::now(),
                    clock: Some(if malformed_book { 100 } else { 0 }),
                    bids: vec![Level {
                        price: 700_000,
                        quantity: if malformed_book { -1 } else { 10 },
                    }],
                    asks: if malformed_book {
                        vec![Level {
                            price: 600_000,
                            quantity: 10,
                        }]
                    } else {
                        vec![]
                    },
                }),
            })
            .unwrap();
            assert!(o.planned.is_none());
            assert!(o.feeds[0].control.borrow().pause.is_none());
            assert!(o.feeds[0].pause.is_some());
            assert!(!o.survivor_ready(0, "A"));
            assert_eq!(o.feeds[0].control.borrow().restart, 0);
            assert_eq!(o.selected["A"].active, Some(source(0)));
            assert_eq!(o.selected["A"].floor.unwrap().clock, 100);
            o.input(Input::PauseEnded {
                source: source(0),
                nonce: token.nonce,
                reason: crate::relay::PauseEnd::Released,
            })
            .unwrap();
            delta(&mut o, source(0), 101, 1);
            assert_eq!(o.selected["A"].floor.unwrap().clock, 101);
            assert_eq!(o.stats.hard_resets, 0);
            assert_eq!(o.stats.continuity_gaps, 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn wholly_superseded_public_replacement_releases_coordinator_without_crediting_completion()
     {
        fn public_book(source: Source, symbol: &str, incarnation: u64) -> Input {
            Input::Data {
                source,
                incarnation,
                received_tick: Instant::now(),
                event: BookEvent::Snapshot(Snapshot {
                    symbol: symbol.into(),
                    received_at: Utc::now(),
                    exchange_at: Utc::now(),
                    clock: None,
                    bids: vec![],
                    asks: vec![],
                }),
            }
        }

        for reincarnated in [false, true] {
            let mut o = owner();
            o.contract = crate::exchange::polymarket::Polymarket.contract();
            let written = o.assignment.incarnations.clone();
            for slot in 0..2 {
                begin(&mut o, source(slot));
                o.input(Input::Membership {
                    source: source(slot),
                    roster: Some(written.clone()),
                    wire: BTreeSet::from(["A".into()]),
                    at: Utc::now(),
                })
                .unwrap();
                o.input(public_book(source(slot), "A", 1)).unwrap();
            }
            assert!(o.start_rotation(0).unwrap());
            assert_eq!(cut(&mut o, 1).len(), 1);
            o.release_planned().unwrap();
            let replacement = Source {
                replica: 0,
                generation: 2,
            };
            begin(&mut o, replacement);
            o.input(Input::Membership {
                source: replacement,
                roster: Some(written.clone()),
                wire: BTreeSet::from(["A".into()]),
                at: Utc::now(),
            })
            .unwrap();
            assert!(!o.ready(0, "A")); // written; its first baseline is still in flight

            let (current_symbol, current_incarnation) =
                if reincarnated { ("A", 2) } else { ("B", 1) };
            let mut next = o.assignment.clone();
            next.epoch += 1;
            next.symbols = vec![current_symbol.into()];
            next.incarnations = BTreeMap::from([(current_symbol.into(), current_incarnation)]);
            o.sync(next);
            assert!(o.needs_renewal.contains(&0));
            assert!(!o.selected[current_symbol].ever);
            o.rotation_tick().unwrap();
            assert!(
                o.rotation.is_none(),
                "entirely superseded written roster retained the renewal coordinator: reincarnation={reincarnated}"
            );
            assert_eq!(o.stats.completed_rotations, 0);
            assert_eq!(o.stats.completed_health_renewals, 0);
            assert_eq!(o.stats.deferred_rotations, 1);
            assert!(o.needs_renewal.contains(&0));

            // Late old-generation evidence cannot complete the cancelled
            // operation or claim that its obsolete roster is current.
            o.input(Input::Membership {
                source: replacement,
                roster: Some(written.clone()),
                wire: BTreeSet::from(["A".into()]),
                at: Utc::now(),
            })
            .unwrap();
            o.input(Input::Begin {
                source: replacement,
                admission: 1,
            })
            .unwrap();
            o.input(public_book(replacement, "A", 1)).unwrap();
            assert!(o.needs_renewal.contains(&0));
            assert_eq!(o.stats.completed_rotations, 0);
            assert!(o.rotation.is_none());
            assert!(
                !o.start_rotation(0).unwrap(),
                "per-feed renewal cooldown was bypassed"
            );

            tokio::time::advance(o.health.cooldown).await;
            assert!(o.start_rotation(0).unwrap());
            o.release_planned().unwrap(); // no old published lifetime remains
            assert_eq!(o.feeds[0].control.borrow().restart, 2);
            let current = Source {
                replica: 0,
                generation: 3,
            };
            o.input(Input::Begin {
                source: current,
                admission: 1,
            })
            .unwrap();
            assert_eq!(
                o.feeds[0].generation, 2,
                "pre-cut dial escaped its restart fence"
            );
            assert!(o.needs_renewal.contains(&0));
            begin(&mut o, current);
            o.input(Input::Membership {
                source: current,
                roster: Some(o.assignment.incarnations.clone()),
                wire: BTreeSet::from([current_symbol.into()]),
                at: Utc::now(),
            })
            .unwrap();
            assert!(!o.needs_renewal.contains(&0));
            o.input(public_book(current, current_symbol, current_incarnation))
                .unwrap();
            o.rotation_tick().unwrap();
            assert_eq!(o.stats.completed_rotations, 1);
            assert_eq!(o.stats.rotation_timeouts, 0);
            assert_eq!(o.stats.continuity_gaps, 0);
            assert!(o.feeds.iter().all(|f| f.pause.is_none()));
        }
    }

    #[test]
    fn partially_superseded_public_replacement_still_requires_its_published_baseline() {
        let mut o = owner();
        o.contract = crate::exchange::polymarket::Polymarket.contract();
        let written = o.assignment.incarnations.clone();
        for slot in 0..2 {
            begin(&mut o, source(slot));
            o.input(Input::Membership {
                source: source(slot),
                roster: Some(written.clone()),
                wire: BTreeSet::from(["A".into()]),
                at: Utc::now(),
            })
            .unwrap();
            o.input(Input::Data {
                source: source(slot),
                incarnation: 1,
                received_tick: Instant::now(),
                event: BookEvent::Snapshot(Snapshot {
                    symbol: "A".into(),
                    received_at: Utc::now(),
                    exchange_at: Utc::now(),
                    clock: None,
                    bids: vec![],
                    asks: vec![],
                }),
            })
            .unwrap();
        }
        o.start_rotation(0).unwrap();
        assert_eq!(cut(&mut o, 1).len(), 1);
        o.release_planned().unwrap();
        let replacement = Source {
            replica: 0,
            generation: 2,
        };
        begin(&mut o, replacement);
        o.input(Input::Membership {
            source: replacement,
            roster: Some(written),
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        let mut next = o.assignment.clone();
        next.epoch += 1;
        next.symbols.push("B".into());
        next.incarnations.insert("B".into(), 1);
        o.sync(next);
        assert!(o.selected["A"].ever);
        assert!(!o.ready(0, "A"));
        assert!(!o.selected["B"].ever);
        o.rotation_tick().unwrap();
        assert!(
            o.rotation.is_some(),
            "unchanged published A still requires a replacement baseline"
        );
        assert_eq!(o.stats.completed_rotations, 0);
        assert_eq!(o.stats.deferred_rotations, 0);
        o.input(Input::Data {
            source: replacement,
            incarnation: 1,
            received_tick: Instant::now(),
            event: BookEvent::Snapshot(Snapshot {
                symbol: "A".into(),
                received_at: Utc::now(),
                exchange_at: Utc::now(),
                clock: None,
                bids: vec![],
                asks: vec![],
            }),
        })
        .unwrap();
        o.rotation_tick().unwrap();
        assert_eq!(o.stats.completed_rotations, 1);
        assert!(
            o.needs_renewal.contains(&0),
            "new B still requires the latest roster"
        );
        assert!(!o.ready(0, "B"));
        assert_eq!(o.stats.continuity_gaps, 0);
    }

    fn book_snapshot(o: &mut Owner, source: Source, symbol: &str, clock: u64, quantity: i64) {
        let bids = if quantity > 0 {
            vec![Level {
                price: 420_000,
                quantity,
            }]
        } else {
            vec![]
        };
        o.input(Input::Data {
            source,
            incarnation: o.selected[symbol].incarnation,
            received_tick: Instant::now(),
            event: BookEvent::Snapshot(Snapshot {
                symbol: symbol.into(),
                received_at: Utc::now(),
                exchange_at: Utc::now(),
                clock: Some(clock),
                bids,
                asks: vec![],
            }),
        })
        .unwrap();
    }
    fn book_delta(o: &mut Owner, source: Source, symbol: &str, clock: u64) {
        o.input(Input::Data {
            source,
            incarnation: o.selected[symbol].incarnation,
            received_tick: Instant::now(),
            event: BookEvent::Update(Update {
                symbol: symbol.into(),
                received_at: Utc::now(),
                exchange_at: Utc::now(),
                clock: Some(clock),
                side: Side::Buy,
                price: 420_000,
                quantity: QuantityUpdate::Change(1),
            }),
        })
        .unwrap();
    }
    fn answer_fence(o: &mut Owner, slot: usize) -> Vec<Emit> {
        let (source, request, _) = o.feeds[slot].pending_fence.unwrap();
        o.input(Input::Fence {
            source,
            request,
            at: Utc::now(),
        })
        .unwrap()
    }
    /// Inferred exchange-wide clock model: replica 1 samples at 100, replica 0
    /// later at 150 with identical levels. After the ordinary peer-ahead grace
    /// replica 0 publishes, and replica 1's quiet books stay at clock 100.
    fn quiet_markets_published_from_later_samples(symbols: &[&str]) -> Owner {
        let mut o = owner();
        let mut assignment = o.assignment.clone();
        assignment.symbols = symbols.iter().map(|s| s.to_string()).collect();
        assignment.incarnations = symbols.iter().map(|s| (s.to_string(), 1)).collect();
        o.sync(assignment);
        for (slot, clock) in [(1, 100), (0, 150)] {
            begin(&mut o, source(slot));
            o.input(Input::Membership {
                roster: None,
                source: source(slot),
                wire: symbols.iter().map(|s| s.to_string()).collect(),
                at: Utc::now(),
            })
            .unwrap();
            for symbol in symbols {
                book_snapshot(&mut o, source(slot), symbol, clock, 10);
            }
        }
        o
    }
    async fn switch_to_later_samples(o: &mut Owner) {
        tokio::time::advance(o.lag).await;
        o.tick().unwrap();
        let switched = answer_fence(o, 0);
        assert_eq!(switched.len(), o.selected.len());
        assert!(o.selected.values().all(|s| s.active == Some(source(0))));
    }
    fn requested_probes(o: &Owner, slot: usize) -> BTreeSet<String> {
        o.feeds[slot]
            .control
            .borrow()
            .probe
            .keys()
            .cloned()
            .collect()
    }
    fn no_pause_requested(o: &Owner) -> bool {
        o.feeds
            .iter()
            .all(|f| f.pause.is_none() && f.control.borrow().pause.is_none())
    }

    #[tokio::test(start_paused = true)]
    async fn a_quiet_standby_behind_a_later_snapshot_gets_a_real_peer_baseline_before_any_pause() {
        let mut o = quiet_markets_published_from_later_samples(&["A", "Q1", "Q2"]);
        switch_to_later_samples(&mut o).await;
        // A keeps trading: both ordered tails reach the same delta floor.
        book_delta(&mut o, source(0), "A", 151);
        book_delta(&mut o, source(1), "A", 151);
        assert!(!o.selected["A"].floor.unwrap().complete);
        assert_eq!(o.selected["Q1"].floor, Some(Frontier::snapshot(150)));
        assert_eq!(
            o.feeds[1].books["Q1"].frontier,
            Some(Frontier::snapshot(100))
        );

        // Old behavior paused here; no tail can bring Q1/Q2 to clock 150.
        assert!(!o.start_rotation(0).unwrap());
        assert!(no_pause_requested(&o));
        assert!(requested_probes(&o, 1).is_empty());
        tokio::time::advance(o.lag).await;
        assert!(!o.start_rotation(0).unwrap());
        assert!(no_pause_requested(&o));
        assert_eq!(
            requested_probes(&o, 1),
            BTreeSet::from(["Q1".into(), "Q2".into()])
        );
        assert!(requested_probes(&o, 0).is_empty());
        // Publication and the standby tail stay valid while the probe waits.
        assert_eq!(o.selected["Q1"].active, Some(source(0)));
        assert_eq!(o.selected["Q1"].floor, Some(Frontier::snapshot(150)));
        assert!(o.ready(1, "Q1"));
        assert!(o.feeds[1].books["Q1"].probe.is_some());

        // Source 1 consumes the scoped probe without an Invalid envelope.
        assert!(!o.start_rotation(0).unwrap());
        for symbol in ["Q1", "Q2"] {
            book_snapshot(&mut o, source(1), symbol, 160, 10);
        }
        assert!(o.start_rotation(0).unwrap());
        acknowledge_pause(&mut o);
        let cut = answer_fence(&mut o, 1);
        assert_eq!(cut.len(), 3, "A by its tail, Q1/Q2 by fresh baselines");
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        o.release_planned().unwrap(); // model baseline admission to output
        assert_eq!(o.feeds[0].control.borrow().restart, 1);
        assert_eq!(o.stats.promotions_blocked, 0);
        assert_eq!(o.stats.rotation_timeouts, 0);
        assert_eq!(o.stats.deferred_rotations, 0);
        assert_eq!(o.stats.clock_regressions, 0);
        assert_eq!(o.stats.continuity_gaps, 0);
        assert_eq!(o.stats.hard_resets, 0);
        assert!(o.coverage.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_quiet_book_that_moves_onto_the_target_during_the_probe_is_also_witnessed() {
        let mut o = owner();
        let mut assignment = o.assignment.clone();
        assignment.symbols = vec!["Q1".into(), "Q2".into()];
        assignment.incarnations = BTreeMap::from([("Q1".into(), 1), ("Q2".into(), 1)]);
        o.sync(assignment);
        for slot in [1, 0] {
            begin(&mut o, source(slot));
            o.input(Input::Membership {
                roster: None,
                source: source(slot),
                wire: BTreeSet::from(["Q1".into(), "Q2".into()]),
                at: Utc::now(),
            })
            .unwrap();
        }
        for symbol in ["Q1", "Q2"] {
            book_snapshot(&mut o, source(1), symbol, 100, 10);
        }
        book_snapshot(&mut o, source(0), "Q1", 150, 10);
        tokio::time::advance(o.lag).await;
        o.tick().unwrap();
        answer_fence(&mut o, 0);
        assert_eq!(o.selected["Q1"].active, Some(source(0)));
        assert_eq!(o.selected["Q2"].active, Some(source(1)));
        assert!(!o.start_rotation(0).unwrap());
        // Q2's later sample arrives on the target while the probe waits.
        book_snapshot(&mut o, source(0), "Q2", 160, 10);
        tokio::time::advance(o.lag).await;
        assert!(!o.start_rotation(0).unwrap());
        assert_eq!(requested_probes(&o, 1), BTreeSet::from(["Q1".into()]));
        // No reservation exists yet, so the ordinary peer-ahead switch runs.
        o.tick().unwrap();
        answer_fence(&mut o, 0);
        assert_eq!(o.selected["Q2"].active, Some(source(0)));
        o.input(Input::Invalid {
            source: source(1),
            symbols: Some(vec!["Q1".into()]),
            reason: "requested snapshot refresh".into(),
        })
        .unwrap();
        book_snapshot(&mut o, source(1), "Q1", 170, 10);
        // Q1 is covered, but Q2 is now an uncovered complete floor too.
        assert!(!o.start_rotation(0).unwrap(), "Q2 would block the pause");
        assert!(no_pause_requested(&o));
        tokio::time::advance(o.lag).await;
        assert!(!o.start_rotation(0).unwrap());
        assert_eq!(
            requested_probes(&o, 1),
            BTreeSet::from(["Q1".into(), "Q2".into()])
        );
        o.input(Input::Invalid {
            source: source(1),
            symbols: Some(vec!["Q2".into()]),
            reason: "requested snapshot refresh".into(),
        })
        .unwrap();
        book_snapshot(&mut o, source(1), "Q2", 180, 10);
        assert!(o.start_rotation(0).unwrap());
        acknowledge_pause(&mut o);
        assert_eq!(answer_fence(&mut o, 1).len(), 2);
        o.release_planned().unwrap();
        assert_eq!(o.feeds[0].control.borrow().restart, 1);
        assert_eq!(o.stats.promotions_blocked, 0);
        assert_eq!(o.stats.rotation_timeouts, 0);
        assert_eq!(o.stats.continuity_gaps, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_progressing_standby_is_left_to_its_tail_and_never_refreshed() {
        let mut o = quiet_markets_published_from_later_samples(&["Q1"]);
        switch_to_later_samples(&mut o).await;
        assert!(!o.start_rotation(0).unwrap());
        // A real change reaches the peer within the grace; the frozen witness
        // is covered by its ordered tail even before the publisher sees it.
        book_delta(&mut o, source(1), "Q1", 155);
        tokio::time::advance(o.lag).await;
        assert!(o.start_rotation(0).unwrap());
        assert!(requested_probes(&o, 1).is_empty());
        assert!(o.feeds[0].control.borrow().pause.is_some());
        assert_eq!(o.stats.deferred_rotations, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn an_uncovering_fresh_peer_baseline_defers_honestly_without_lowering_the_floor() {
        let mut o = quiet_markets_published_from_later_samples(&["Q1"]);
        switch_to_later_samples(&mut o).await;
        assert!(!o.start_rotation(0).unwrap());
        tokio::time::advance(o.lag).await;
        assert!(!o.start_rotation(0).unwrap());
        assert_eq!(requested_probes(&o, 1), BTreeSet::from(["Q1".into()]));
        // Live 2026-11-08 shape: an empty clock-zero baseline. Existing native
        // quarantine policy handles it; the probe must not accept it.
        book_snapshot(&mut o, source(1), "Q1", 0, 0);
        assert_eq!(o.stats.clock_regressions, 1);
        assert!(!o.ready(1, "Q1"));
        tokio::time::advance(o.health.rotation_timeout).await;
        assert!(!o.start_rotation(0).unwrap());
        assert!(no_pause_requested(&o));
        assert_eq!(o.feeds[0].control.borrow().restart, 0);
        assert_eq!(o.stats.deferred_rotations, 1);
        assert!(
            o.maintenance_deferred_until
                .is_some_and(|at| at > Instant::now())
        );
        assert_eq!(o.selected["Q1"].active, Some(source(0)));
        assert_eq!(o.selected["Q1"].floor, Some(Frontier::snapshot(150)));
        assert_eq!(o.stats.rotation_timeouts, 0);
        assert_eq!(o.stats.continuity_gaps, 0);
        assert_eq!(o.stats.hard_resets, 0);
        assert!(o.coverage.is_none());
    }

    /// Second scale cause (red in the full-suite run): a peer-ahead candidate
    /// chosen before the rotation's reservation has its fence answered after
    /// it, and the switch moved quiet books ONTO the paused target. Its peer is
    /// stale-behind for exactly those books, so the planned cut could only time out.
    #[tokio::test(start_paused = true)]
    async fn a_pre_reservation_peer_ahead_fence_cannot_move_books_onto_the_rotation_target() {
        for acknowledged_first in [false, true] {
            let mut o = owner();
            let mut assignment = o.assignment.clone();
            assignment.symbols = vec!["A".into(), "Q1".into()];
            assignment.incarnations = BTreeMap::from([("A".into(), 1), ("Q1".into(), 1)]);
            o.sync(assignment);
            for slot in [0, 1] {
                begin(&mut o, source(slot));
                o.input(Input::Membership {
                    roster: None,
                    source: source(slot),
                    wire: BTreeSet::from(["A".into(), "Q1".into()]),
                    at: Utc::now(),
                })
                .unwrap();
            }
            // A is first published by replica 0 and keeps trading on both tails.
            book_snapshot(&mut o, source(0), "A", 100, 10);
            book_snapshot(&mut o, source(1), "A", 100, 10);
            book_delta(&mut o, source(0), "A", 151);
            book_delta(&mut o, source(1), "A", 151);
            // Quiet Q1 is first published by replica 1; replica 0's later
            // exchange-wide sample is peer-ahead with identical levels.
            book_snapshot(&mut o, source(1), "Q1", 100, 10);
            book_snapshot(&mut o, source(0), "Q1", 150, 10);
            tokio::time::advance(o.lag).await;
            o.tick().unwrap();
            let (_, opportunistic, _) = o.feeds[0].pending_fence.unwrap();
            assert_eq!(o.selected["Q1"].candidate, Some(0));
            assert_eq!(o.selected["A"].active, Some(source(0)));
            assert_eq!(o.selected["Q1"].active, Some(source(1)));
            assert!(o.start_rotation(0).unwrap(), "no complete floor to witness");
            if acknowledged_first {
                acknowledge_pause(&mut o);
            }
            o.input(Input::Fence {
                source: source(0),
                request: opportunistic,
                at: Utc::now(),
            })
            .unwrap();
            assert_eq!(
                o.selected["Q1"].active,
                Some(source(1)),
                "acknowledged_first={acknowledged_first}: switched onto the planned target"
            );
            assert_eq!(o.selected["Q1"].candidate, None);
            acknowledge_pause(&mut o);
            o.tick().unwrap();
            assert_eq!(answer_fence(&mut o, 1).len(), 1, "A cuts by its tail");
            o.release_planned().unwrap();
            assert_eq!(o.feeds[0].control.borrow().restart, 1);
            assert_eq!(o.stats.promotions_blocked, 0);
            assert_eq!(o.stats.rotation_timeouts, 0);
            assert_eq!(o.stats.continuity_gaps, 0);
        }
    }

    /// Covering requests issued for one book, by any identity-scoped control.
    fn outstanding_requests(o: &Owner, slot: usize, symbol: &str) -> u64 {
        let control = o.feeds[slot].control.borrow();
        control.refresh.get(symbol).map_or(0, |r| r.nonce)
            + control.probe.get(symbol).map_or(0, |r| r.nonce)
    }
    fn fault(o: &mut Owner, slot: usize) {
        o.input(Input::Invalid {
            source: source(slot),
            symbols: None,
            reason: "real publisher fault".into(),
        })
        .unwrap();
    }

    /// R2 Finding 1 (Codex 247-gap fixture): the unplanned covering request
    /// invalidated the only survivor, so each quiet market became a gap.
    #[tokio::test(start_paused = true)]
    async fn failover_probe_keeps_the_sole_survivor_ready_and_fails_over_without_a_gap() {
        let mut o = quiet_markets_published_from_later_samples(&["Q1"]);
        switch_to_later_samples(&mut o).await;
        fault(&mut o, 0);
        assert_eq!(o.selected["Q1"].candidate, Some(1));
        assert!(answer_fence(&mut o, 1).is_empty());
        assert_eq!(o.stats.promotions_blocked, 1);
        // Another fence round after the replica grace: still blocked.
        tokio::time::advance(o.lag).await;
        o.tick().unwrap();
        assert!(answer_fence(&mut o, 1).is_empty());
        assert!(
            o.ready(1, "Q1"),
            "a covering request must not invalidate the only survivor"
        );
        assert_eq!(o.stats.continuity_gaps, 0);
        assert!(!o.selected["Q1"].recovery);
        // The provider's answer is a real later sample on that ordered stream.
        book_snapshot(&mut o, source(1), "Q1", 160, 10);
        let failover = answer_fence(&mut o, 1);
        assert!(
            matches!(&failover[..], [Emit::Snapshot { reason: SnapshotReason::Failover,
                source: Source { replica: 1, .. }, stamp, .. }] if stamp.clock == Some(160)),
            "{} emitted",
            failover.len()
        );
        assert_eq!((o.stats.continuity_gaps, o.stats.hard_resets), (0, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn pending_probe_retains_the_tail_and_is_not_cleared_by_an_uncovering_delta() {
        let mut o = quiet_markets_published_from_later_samples(&["Q1"]);
        switch_to_later_samples(&mut o).await;
        fault(&mut o, 0);
        answer_fence(&mut o, 1);
        assert_eq!(outstanding_requests(&o, 1, "Q1"), 1);
        assert!(o.ready(1, "Q1"));
        book_delta(&mut o, source(1), "Q1", 120);
        assert!(o.feeds[1].books["Q1"].probe.is_some());
        tokio::time::advance(o.lag).await;
        o.tick().unwrap();
        answer_fence(&mut o, 1);
        assert_eq!(outstanding_requests(&o, 1, "Q1"), 1);
        book_delta(&mut o, source(1), "Q1", 151);
        assert!(o.feeds[1].books["Q1"].probe.is_none());
        let emitted = answer_fence(&mut o, 1);
        assert!(matches!(
            &emitted[..],
            [Emit::Snapshot {
                reason: SnapshotReason::Failover,
                ..
            }]
        ));
        assert_eq!((o.stats.continuity_gaps, o.stats.hard_resets), (0, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn a_covered_probe_goal_does_not_authorize_a_newer_publication_floor() {
        let mut o = quiet_markets_published_from_later_samples(&["Q1"]);
        switch_to_later_samples(&mut o).await;
        o.start_rotation(0).unwrap();
        tokio::time::advance(o.lag).await;
        o.start_rotation(0).unwrap();
        assert_eq!(outstanding_requests(&o, 1, "Q1"), 1);
        book_snapshot(&mut o, source(0), "Q1", 200, 10);
        book_snapshot(&mut o, source(1), "Q1", 160, 10);
        assert!(o.ready(1, "Q1"), "the answer covered its frozen 150 goal");
        assert_eq!(o.selected["Q1"].floor, Some(Frontier::snapshot(200)));
        assert!(!o.start_rotation(0).unwrap());
        assert!(
            no_pause_requested(&o),
            "160 cannot authorize the current 200 cut"
        );
        tokio::time::advance(o.lag).await;
        assert!(!o.start_rotation(0).unwrap());
        assert_eq!(
            outstanding_requests(&o, 1, "Q1"),
            2,
            "a resolved old witness must not suppress the next scoped goal"
        );
        book_snapshot(&mut o, source(1), "Q1", 210, 10);
        assert!(o.start_rotation(0).unwrap());
        acknowledge_pause(&mut o);
        let emitted = answer_fence(&mut o, 1);
        assert!(matches!(&emitted[..], [Emit::Snapshot {
            reason: SnapshotReason::Failover, stamp, ..
        }] if stamp.clock == Some(210)));
        assert_eq!(
            o.feeds[0].control.borrow().restart,
            0,
            "output admission still owns release"
        );
        assert_eq!((o.stats.clock_regressions, o.stats.continuity_gaps), (0, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn an_uncovering_probe_answer_keeps_the_frozen_catchup_floor() {
        let mut o = quiet_markets_published_from_later_samples(&["Q1"]);
        switch_to_later_samples(&mut o).await;
        o.start_rotation(0).unwrap();
        tokio::time::advance(o.lag).await;
        o.start_rotation(0).unwrap();
        assert_eq!(outstanding_requests(&o, 1, "Q1"), 1);
        book_snapshot(&mut o, source(1), "Q1", 140, 10);
        assert!(!o.ready(1, "Q1"));
        assert_eq!(
            o.feeds[1].books["Q1"].catchup.unwrap().0,
            Frontier::snapshot(150)
        );
        assert!(o.feeds[1].books["Q1"].probe.is_some());
        assert_eq!(o.selected["Q1"].floor, Some(Frontier::snapshot(150)));
        assert!(no_pause_requested(&o));
        book_delta(&mut o, source(1), "Q1", 151);
        assert!(o.ready(1, "Q1"));
        assert!(o.feeds[1].books["Q1"].catchup.is_none());
        assert!(o.feeds[1].books["Q1"].probe.is_none());
        assert_eq!((o.stats.clock_regressions, o.stats.continuity_gaps), (0, 0));
    }

    #[test]
    fn the_last_publisher_still_exhausts_the_semantic_recovery_budget() {
        let mut o = owner();
        start(&mut o, 0, 100, 20);
        start(&mut o, 1, 100, 20);
        for _ in 0..o.health.max_resubscriptions {
            snap(&mut o, source(0), 0, 20);
        }
        let error = o
            .input(Input::Data {
                source: source(0),
                incarnation: 1,
                received_tick: Instant::now(),
                event: BookEvent::Snapshot(Snapshot {
                    symbol: "A".into(),
                    received_at: Utc::now(),
                    exchange_at: Utc::now(),
                    clock: Some(0),
                    bids: vec![],
                    asks: vec![],
                }),
            })
            .err()
            .expect("the last publisher must exhaust its semantic budget");
        assert!(error.to_string().contains("semantic recovery exhausted"));
        assert_eq!(
            o.feeds[0].books["A"].rejected_baselines,
            o.health.max_resubscriptions + 1
        );
        assert_eq!(o.stats.hard_resets, 0);
    }

    /// R2 Finding 2 (live stderr 205-206): a planned block armed behind_since;
    /// the next real failover then reset instantly instead of waiting 5 s.
    #[tokio::test(start_paused = true)]
    async fn a_cancelled_planned_block_cannot_turn_a_later_real_failover_into_an_instant_reset() {
        let mut o = owner();
        start(&mut o, 0, 100, 10);
        start(&mut o, 1, 100, 10);
        delta(&mut o, source(0), 101, 1); // delayed peer: delta floor not covered
        assert!(o.start_rotation(0).unwrap());
        acknowledge_pause(&mut o);
        assert!(answer_fence(&mut o, 1).is_empty(), "planned cut blocked");
        tokio::time::advance(Duration::from_secs(6)).await;
        o.rotation_tick().unwrap();
        assert!(o.planned.is_none(), "lease expiry cancelled the plan");
        fault(&mut o, 0);
        assert_eq!(o.selected["A"].candidate, Some(1));
        assert!(answer_fence(&mut o, 1).is_empty());
        assert_eq!(
            o.stats.hard_resets, 0,
            "a planned block's timer leaked into the failover"
        );
        // The delayed tail arrives inside the honest bound: real failover.
        delta(&mut o, source(1), 101, 1);
        let failover = answer_fence(&mut o, 1);
        assert!(matches!(
            &failover[..],
            [Emit::Snapshot {
                reason: SnapshotReason::Failover,
                ..
            }]
        ));
        assert_eq!((o.stats.continuity_gaps, o.stats.hard_resets), (0, 0));
    }

    /// R2 Finding 3: clock-zero answers to standby covering requests drove
    /// reject -> refresh -> reject until the fatal semantic budget.
    #[tokio::test(start_paused = true)]
    async fn clock_zero_standby_answers_quarantine_without_a_fatal_re_request_chain() {
        let mut o = quiet_markets_published_from_later_samples(&["Q1"]);
        switch_to_later_samples(&mut o).await;
        assert!(!o.start_rotation(0).unwrap());
        tokio::time::advance(o.lag).await;
        assert!(!o.start_rotation(0).unwrap());
        let mut answered = 0;
        let mut answers = 0;
        // Live KXDOGE15M shape: every answer is an empty clock-zero book.
        // Answer exactly once per request the Owner actually issued.
        while outstanding_requests(&o, 1, "Q1") > answered && answers < 6 {
            answered = outstanding_requests(&o, 1, "Q1");
            answers += 1;
            let result = o.input(Input::Data {
                source: source(1),
                incarnation: 1,
                received_tick: Instant::now(),
                event: BookEvent::Snapshot(Snapshot {
                    symbol: "Q1".into(),
                    received_at: Utc::now(),
                    exchange_at: Utc::now(),
                    clock: Some(0),
                    bids: vec![],
                    asks: vec![],
                }),
            });
            if let Err(error) = result {
                panic!("answer {answers} drove a fatal chain: {error:#}");
            }
        }
        assert_eq!(answers, 1, "an unusable standby answer was re-requested");
        assert_eq!(o.stats.clock_regressions, 1);
        assert_eq!(o.feeds[1].books["Q1"].rejected_baselines, 1);
        assert!(!o.ready(1, "Q1"));
        assert_eq!(o.selected["Q1"].active, Some(source(0)));
        assert_eq!(o.selected["Q1"].floor, Some(Frontier::snapshot(150)));
        assert_eq!((o.stats.continuity_gaps, o.stats.hard_resets), (0, 0));
        let requests = outstanding_requests(&o, 1, "Q1");
        for attempt in 0..3 {
            tokio::time::advance(o.health.rotation_timeout + o.health.cooldown + o.lag).await;
            assert!(!o.start_rotation(0).unwrap(), "attempt {attempt}");
            assert!(no_pause_requested(&o));
        }
        assert_eq!(
            outstanding_requests(&o, 1, "Q1"),
            requests,
            "a quarantined witness was re-probed on a later cooldown"
        );
        assert!(o.stats.deferred_rotations >= 3);
    }

    #[tokio::test(start_paused = true)]
    async fn a_late_membership_ack_admits_a_standby_probe_unless_quiescing() {
        for quiescing in [false, true] {
            let mut o = owner();
            start(&mut o, 0, 150, 10);
            begin(&mut o, source(1));
            snap(&mut o, source(1), 100, 10);
            assert_eq!(
                outstanding_requests(&o, 1, "A"),
                0,
                "probe before membership"
            );
            o.quiescing = quiescing;
            o.input(Input::Membership {
                source: source(1),
                roster: None,
                wire: BTreeSet::from(["A".into()]),
                at: Utc::now(),
            })
            .unwrap();
            assert_eq!(outstanding_requests(&o, 1, "A"), u64::from(!quiescing));
            assert!(!o.ready(1, "A"));
            assert_eq!(o.selected["A"].floor, Some(Frontier::snapshot(150)));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_fresh_older_standby_acquires_cover_without_a_false_clock_quarantine() {
        let mut o = owner();
        start(&mut o, 0, 150, 10);
        start(&mut o, 1, 100, 10);
        assert!(!o.ready(1, "A"));
        tokio::time::advance(Duration::from_secs(6)).await;
        o.tick().unwrap();
        assert_eq!(o.stats.clock_regressions, 0, "no stream clock regressed");
        assert_eq!(outstanding_requests(&o, 1, "A"), 1);
        assert!(o.feeds[1].books["A"].value.is_valid());
        assert_eq!(
            o.feeds[1].books["A"].catchup.unwrap().0,
            Frontier::snapshot(150)
        );
        snap(&mut o, source(1), 160, 10);
        assert!(o.ready(1, "A"));
        assert_eq!(o.stats.continuity_gaps, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn standby_acquisition_age_cannot_be_reused_as_time_without_a_publisher() {
        let mut o = owner();
        start(&mut o, 0, 150, 10);
        start(&mut o, 1, 100, 10);
        tokio::time::advance(Duration::from_secs(6)).await;
        o.tick().unwrap();
        fault(&mut o, 0);
        tokio::time::advance(Duration::from_secs(4)).await;
        o.tick().unwrap();
        assert_eq!(
            o.stats.hard_resets, 0,
            "standby acquisition donated an old timer"
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        o.tick().unwrap();
        assert_eq!(
            o.stats.hard_resets, 1,
            "genuine five-second bound was widened"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn probe_quarantine_survives_reconnect_and_ends_only_at_a_usable_baseline() {
        let mut o = quiet_markets_published_from_later_samples(&["Q1"]);
        switch_to_later_samples(&mut o).await;
        assert!(o.probe(1, "Q1"));
        book_snapshot(&mut o, source(1), "Q1", 0, 0);
        assert!(o.feeds[1].books["Q1"].probe_quarantined);
        let fresh = Source {
            replica: 1,
            generation: 2,
        };
        begin(&mut o, fresh);
        o.input(Input::Membership {
            source: fresh,
            roster: None,
            wire: BTreeSet::from(["Q1".into()]),
            at: Utc::now(),
        })
        .unwrap();
        for rejected in 2..=4 {
            book_snapshot(&mut o, fresh, "Q1", 0, 0);
            assert!(!o.ready(1, "Q1"));
            tokio::time::advance(Duration::from_secs(6)).await;
            o.tick().unwrap();
            assert_eq!(o.feeds[1].books["Q1"].rejected_baselines, rejected);
            assert!(o.feeds[1].books["Q1"].probe_quarantined);
            assert!(o.feeds[1].control.borrow().refresh.is_empty());
            assert_eq!(o.selected["Q1"].active, Some(source(0)));
        }
        assert_eq!(o.stats.clock_regressions, 4);
        book_snapshot(&mut o, fresh, "Q1", 160, 10);
        assert!(o.ready(1, "Q1"));
        assert!(!o.feeds[1].books["Q1"].probe_quarantined);
        assert_eq!(o.feeds[1].books["Q1"].rejected_baselines, 0);
        // A later, unrelated regression retains the old finite recovery path.
        book_snapshot(&mut o, fresh, "Q1", 0, 0);
        assert_eq!(o.feeds[1].control.borrow().refresh["Q1"].nonce, 1);
        assert_eq!(o.feeds[1].books["Q1"].rejected_baselines, 1);
        assert_eq!((o.stats.continuity_gaps, o.stats.hard_resets), (0, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn quiescence_issues_no_coverage_refresh() {
        let mut o = quiet_markets_published_from_later_samples(&["Q1"]);
        switch_to_later_samples(&mut o).await;
        o.quiescing = true;
        o.start_rotation(0).unwrap();
        tokio::time::advance(o.lag).await;
        o.rotation_tick().unwrap();
        assert!(requested_probes(&o, 1).is_empty());
        assert!(o.coverage.is_none());
        assert!(no_pause_requested(&o), "quiescence admits no new pause");
    }

    // Native rotation scope: the operation owes readiness only for the
    // (symbol, incarnation) lifetimes its destructive restart released.
    fn reassign(o: &mut Owner, markets: &[(&str, u64)]) {
        let mut next = o.assignment.clone();
        next.epoch += 1;
        next.symbols = markets.iter().map(|(s, _)| s.to_string()).collect();
        next.incarnations = markets.iter().map(|&(s, i)| (s.to_string(), i)).collect();
        o.sync(next);
    }
    fn release_native_rotation(o: &mut Owner) {
        assert!(o.start_rotation(0).unwrap());
        cut(o, 1);
        o.release_planned().unwrap(); // model admission of the handoff baselines
        assert_eq!(o.feeds[0].control.borrow().restart, 1);
        assert!(o.rotation.is_some() && o.planned.is_none());
    }
    fn replacement(o: &mut Owner, wire: &[&str]) -> Source {
        let fresh = Source {
            replica: 0,
            generation: 2,
        };
        begin(o, fresh);
        o.input(Input::Membership {
            roster: None,
            source: fresh,
            wire: wire.iter().map(|s| s.to_string()).collect(),
            at: Utc::now(),
        })
        .unwrap();
        fresh
    }

    /// One pass of the runtime timer arm, in its exact callback order.
    fn timer_pass(o: &mut Owner) {
        o.tick().unwrap();
        o.activity_tick().unwrap();
        o.rotation_tick().unwrap();
        o.release_planned().unwrap();
    }

    /// Scheduled restart of standby replica 1, the slot that later cooldown
    /// retries also target. Replica 0 keeps every publisher, so the plan needs
    /// no cut and is released on the first pass.
    fn release_standby_restart(o: &mut Owner) {
        o.rotation_slot = 1;
        assert!(o.start_rotation(1).unwrap());
        o.release_planned().unwrap();
        assert!(o.rotation.is_some() && o.planned.is_none());
    }
    /// Next generation of replica 1, admitted under the current restart nonce.
    fn standby_replacement(o: &mut Owner, wire: &[&str]) -> Source {
        let fresh = Source {
            replica: 1,
            generation: o.feeds[1].generation + 1,
        };
        begin(o, fresh);
        o.input(Input::Membership {
            roster: None,
            source: fresh,
            wire: wire.iter().map(|s| s.to_string()).collect(),
            at: Utc::now(),
        })
        .unwrap();
        assert_eq!(o.feeds[1].generation, fresh.generation);
        fresh
    }
    /// Standby A and B are covered by the replacement. C is answered with the
    /// documented no-recorded-change clock, far below its published floor, and
    /// gets exactly one real scoped snapshot request.
    fn clock_zero_standby_replacement(o: &mut Owner) -> Source {
        for symbol in ["A", "B", "C"] {
            assert_eq!(o.selected[symbol].active, Some(source(0)));
        }
        release_standby_restart(o);
        let fresh = standby_replacement(o, &["A", "B", "C"]);
        book_snapshot(o, fresh, "A", 100, 10);
        book_snapshot(o, fresh, "B", 100, 10);
        book_snapshot(o, fresh, "C", 0, 0);
        assert!(o.ready(1, "A") && o.ready(1, "B") && !o.ready(1, "C"));
        assert_eq!(requested_probes(o, 1), BTreeSet::from(["C".into()]));
        assert_eq!(outstanding_requests(o, 1, "C"), 1);
        fresh
    }
    fn pending_maintenance(o: &mut Owner) -> usize {
        let (progress, row) = watch::channel(ShardStats::default());
        o.report(&[], &progress);
        row.borrow().maintenance_pending
    }
    fn restarts(o: &Owner, slot: usize) -> u64 {
        o.feeds[slot].control.borrow().restart
    }
    /// Every wire-visible control, for proving a transition issued none.
    #[allow(clippy::type_complexity)]
    fn wire_controls(
        o: &Owner,
    ) -> Vec<(
        u64,
        u64,
        Option<u64>,
        BTreeMap<String, crate::relay::Refresh>,
        BTreeMap<String, crate::relay::Resubscribe>,
        BTreeMap<String, crate::relay::Probe>,
    )> {
        o.feeds
            .iter()
            .map(|f| {
                let c = f.control.borrow();
                (
                    c.fence,
                    c.restart,
                    c.pause.map(|p| p.nonce),
                    c.refresh.clone(),
                    c.resubscribe.clone(),
                    c.probe.clone(),
                )
            })
            .collect()
    }
    async fn advance_to_rotation_due(o: &Owner) {
        let due = o.rotation_due.expect("no rotation was scheduled");
        tokio::time::advance(due.saturating_duration_since(Instant::now())).await;
    }
    /// One further destructive retry of replica 1 for clock-zero C, past the
    /// carried quarantine's five-second bound and the 30 s operation deadline.
    async fn fail_clock_zero_retry(o: &mut Owner) -> Source {
        advance_to_rotation_due(o).await;
        timer_pass(o);
        let fresh = standby_replacement(o, &["A", "B", "C"]);
        book_snapshot(o, fresh, "A", 100, 10);
        book_snapshot(o, fresh, "B", 100, 10);
        book_snapshot(o, fresh, "C", 0, 0);
        tokio::time::advance(Duration::from_secs(5)).await;
        timer_pass(o);
        assert!(o.rotation.is_some(), "quarantine alone ended the operation");
        tokio::time::advance(o.health.rotation_timeout - Duration::from_secs(5)).await;
        timer_pass(o);
        fresh
    }

    /// The live wedge's observable. This test compiles and runs on the
    /// pre-repair source, where the last assertion fails.
    #[tokio::test(start_paused = true)]
    async fn quarantined_replacement_stops_holding_shard_maintenance_at_the_deadline() {
        let mut o = multibook_owner();
        clock_zero_standby_replacement(&mut o);
        let deadline = o.health.rotation_timeout;
        assert_eq!(deadline, Duration::from_secs(30));
        assert_eq!(pending_maintenance(&mut o), 1);
        tokio::time::advance(deadline - Duration::from_millis(1)).await;
        timer_pass(&mut o);
        assert_eq!(pending_maintenance(&mut o), 1, "deadline moved earlier");
        tokio::time::advance(Duration::from_millis(1)).await;
        timer_pass(&mut o);
        assert_eq!(
            pending_maintenance(&mut o),
            0,
            "a quarantined replacement held all shard maintenance past its deadline"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn quarantined_replacement_fails_exactly_once_without_any_wire_control() {
        let mut o = multibook_owner();
        clock_zero_standby_replacement(&mut o);
        let controls = wire_controls(&o);
        tokio::time::advance(o.health.rotation_timeout).await;
        timer_pass(&mut o);

        assert!(o.rotation.is_none() && o.planned.is_none());
        assert_eq!(o.stats.failed_rotations, 1);
        assert_eq!(o.stats.rotation_timeouts, 1);
        assert_eq!(o.stats.completed_rotations, 0);
        assert_eq!(o.stats.completed_health_renewals, 0);
        assert_eq!(o.stats.health_transport_renewals, 0);
        assert_eq!(o.stats.planned_rotations, 1);
        assert_eq!(o.stats.deferred_rotations, 0);
        assert_eq!(o.rotation_slot, 1, "a failed operation advanced the slot");
        assert_eq!(pending_maintenance(&mut o), 0);

        // The failure is one logical decision: no wire action and no safety change.
        assert_eq!(wire_controls(&o), controls);
        assert!(no_pause_requested(&o));
        let book = &o.feeds[1].books["C"];
        assert!(book.probe_quarantined && !book.value.is_valid());
        assert_eq!(
            book.rejected_baselines, 1,
            "budget must survive the failure"
        );
        assert_eq!(book.frontier, Some(Frontier::snapshot(0)));
        for symbol in ["A", "B", "C"] {
            assert_eq!(o.selected[symbol].active, Some(source(0)));
            assert_eq!(o.selected[symbol].floor, Some(Frontier::snapshot(100)));
            assert!(o.ready(0, symbol));
        }
        assert_eq!(
            (o.stats.continuity_gaps, o.stats.hard_resets),
            (0, 0),
            "the failure changed publication"
        );

        let retry = Instant::now() + o.health.cooldown;
        assert_eq!(o.maintenance_deferred_until, Some(retry));
        assert_eq!(o.rotation_due, Some(retry));
    }

    #[tokio::test(start_paused = true)]
    async fn failed_re_rotations_spend_the_lifetime_budget_and_then_stop_destructive_work() {
        let mut o = multibook_owner();
        let (cooldown, deadline) = (o.health.cooldown, o.health.rotation_timeout);
        let budget = u64::from(o.health.max_resubscriptions);
        assert_eq!(budget, 3);
        clock_zero_standby_replacement(&mut o);
        tokio::time::advance(deadline).await;
        timer_pass(&mut o);
        assert_eq!(o.stats.failed_rotations, 1);

        // Repeated timer passes cannot count one failure or timeout twice.
        for _ in 0..8 {
            tokio::time::advance(cooldown / 16).await;
            timer_pass(&mut o);
        }
        assert_eq!(o.stats.failed_rotations, 1);
        assert_eq!(o.stats.rotation_timeouts, 1);
        assert_eq!(restarts(&o, 1), 1);
        assert_eq!(o.stats.planned_rotations, 1);

        // Each later failure is proved by a real restart and a real rejection.
        for round in 2..=budget {
            fail_clock_zero_retry(&mut o).await;
            assert_eq!(restarts(&o, 1), round, "retry {round} did not restart");
            assert_eq!(o.stats.planned_rotations, round);
            assert_eq!(o.stats.failed_rotations, round);
            assert_eq!(o.stats.rotation_timeouts, round);
            assert_eq!(
                u64::from(o.feeds[1].books["C"].rejected_baselines),
                round,
                "a failed retry must spend one rejection"
            );
            assert_eq!(outstanding_requests(&o, 1, "C"), 1, "retry re-probed");
        }
        assert_eq!(o.stats.deferred_rotations, 0);

        // The budget is spent: every later attempt defers without work.
        let controls = wire_controls(&o);
        for attempt in 1..=3 {
            advance_to_rotation_due(&o).await;
            timer_pass(&mut o);
            assert_eq!(o.stats.deferred_rotations, attempt);
            let retry = Instant::now() + cooldown;
            assert_eq!(o.maintenance_deferred_until, Some(retry));
            assert_eq!(o.rotation_due, Some(retry));
            for _ in 0..4 {
                tokio::time::advance(cooldown / 8).await;
                timer_pass(&mut o);
            }
            assert_eq!(
                o.stats.deferred_rotations, attempt,
                "attempted inside cooldown"
            );
            assert!(o.rotation.is_none() && o.planned.is_none());
            assert_eq!(pending_maintenance(&mut o), 0);
        }
        assert_eq!(
            wire_controls(&o),
            controls,
            "exhausted retry reached the wire"
        );
        assert_eq!(o.stats.planned_rotations, budget);
        assert_eq!(o.stats.failed_rotations, budget);
        assert_eq!(o.stats.rotation_timeouts, budget);
        assert!(no_pause_requested(&o));
        assert_eq!(o.selected["C"].active, Some(source(0)));
    }

    #[tokio::test(start_paused = true)]
    async fn an_exhausted_book_does_not_hold_back_an_explicit_reset_restart() {
        let mut o = multibook_owner();
        clock_zero_standby_replacement(&mut o);
        tokio::time::advance(o.health.rotation_timeout).await;
        timer_pass(&mut o);
        for _ in 2..=o.health.max_resubscriptions {
            fail_clock_zero_retry(&mut o).await;
        }
        assert!(o.terminally_quarantined(1, "C"));
        let planned = o.stats.planned_rotations;
        o.reset_restarts.insert(1);
        advance_to_rotation_due(&o).await;
        timer_pass(&mut o);
        assert_eq!(o.stats.planned_rotations, planned + 1);
        assert_eq!(o.stats.deferred_rotations, 0);
        assert!(
            !o.reset_restarts.contains(&1),
            "reset restart was held back"
        );
    }

    /// A single-book roster, so no other unready book masks the terminal check.
    #[tokio::test(start_paused = true)]
    async fn a_carried_quarantine_cannot_fail_a_generation_still_awaiting_its_first_baseline() {
        let mut o = owner();
        start(&mut o, 0, 100, 10);
        start(&mut o, 1, 100, 10);
        release_standby_restart(&mut o);
        let fresh = standby_replacement(&mut o, &["A"]);
        book_snapshot(&mut o, fresh, "A", 0, 0);
        let deadline = o.health.rotation_timeout;
        tokio::time::advance(deadline).await;
        timer_pass(&mut o);
        assert_eq!(o.stats.failed_rotations, 1);

        advance_to_rotation_due(&o).await;
        timer_pass(&mut o);
        assert_eq!(restarts(&o, 1), 2);
        // The old generation's frontier and carried quarantine remain on the
        // book until Begin; a replacement that never connected is not a verdict.
        assert!(o.feeds[1].books["A"].frontier.is_some());
        tokio::time::advance(deadline).await;
        timer_pass(&mut o);
        assert!(
            o.rotation.is_some(),
            "no replacement yet: the operation waits"
        );
        assert_eq!(o.stats.failed_rotations, 1);
        assert_eq!(o.stats.rotation_timeouts, 2);

        // The new socket exists and owes a first baseline; nothing has arrived.
        let fresh = Source {
            replica: 1,
            generation: o.feeds[1].generation + 1,
        };
        begin(&mut o, fresh);
        assert!(
            o.feeds[1].books["A"].probe_quarantined,
            "carried across Begin"
        );
        assert_eq!(o.feeds[1].books["A"].frontier, None);
        timer_pass(&mut o);
        assert!(o.rotation.is_some(), "no baseline yet: the operation waits");
        assert_eq!(o.stats.failed_rotations, 1);
        assert_eq!(o.stats.rotation_timeouts, 2, "timeout is one-shot");

        // A covering baseline still waiting for membership is not a verdict.
        book_snapshot(&mut o, fresh, "A", 100, 10);
        assert!(o.feeds[1].books["A"].probe_quarantined);
        assert!(o.feeds[1].books["A"].frontier.is_some());
        assert!(o.feeds[1].books["A"].value.is_valid());
        timer_pass(&mut o);
        assert!(o.rotation.is_some(), "an unacknowledged baseline failed it");
        assert_eq!(o.stats.failed_rotations, 1);

        // Usable evidence on this generation finishes the operation normally.
        o.input(Input::Membership {
            roster: None,
            source: fresh,
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        timer_pass(&mut o);
        assert_eq!(o.stats.completed_rotations, 1);
        assert_eq!(o.stats.failed_rotations, 1);
        assert!(!o.feeds[1].books["A"].probe_quarantined);
    }

    #[tokio::test(start_paused = true)]
    async fn an_outstanding_probe_keeps_the_operation_open_until_it_resolves() {
        for answered_usably in [false, true] {
            let mut o = multibook_owner();
            release_standby_restart(&mut o);
            let fresh = standby_replacement(&mut o, &["A", "B", "C"]);
            book_snapshot(&mut o, fresh, "A", 100, 10);
            book_snapshot(&mut o, fresh, "B", 0, 0);
            tokio::time::advance(Duration::from_secs(10)).await;
            book_snapshot(&mut o, fresh, "C", 0, 0);
            assert_eq!(
                requested_probes(&o, 1),
                BTreeSet::from(["B".into(), "C".into()])
            );

            // B is terminal at the deadline; C's probe still has ten seconds.
            tokio::time::advance(Duration::from_secs(20)).await;
            timer_pass(&mut o);
            assert!(o.feeds[1].books["B"].probe_quarantined);
            assert!(o.feeds[1].books["C"].probe.is_some());
            assert!(o.rotation.is_some(), "an outstanding probe was abandoned");
            assert_eq!(o.stats.rotation_timeouts, 1);
            assert_eq!(o.stats.failed_rotations, 0);

            tokio::time::advance(Duration::from_secs(10) - Duration::from_millis(1)).await;
            timer_pass(&mut o);
            assert!(o.rotation.is_some());
            assert_eq!(o.stats.failed_rotations, 0);

            if answered_usably {
                book_snapshot(&mut o, fresh, "C", 100, 10);
                assert!(o.ready(1, "C"));
            }
            tokio::time::advance(Duration::from_millis(1)).await;
            timer_pass(&mut o);
            assert!(o.rotation.is_none(), "answered_usably={answered_usably}");
            assert_eq!(o.stats.failed_rotations, 1);
            assert_eq!(o.stats.rotation_timeouts, 1, "timeout is one-shot");
            assert_eq!(o.stats.completed_rotations, 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_armed_catchup_keeps_the_operation_open_past_the_deadline() {
        // `invalidated` is a Source-reported loss after the uncovering baseline:
        // the value is gone but its catch-up is still armed, so nothing was judged.
        for invalidated in [false, true] {
            let mut o = multibook_owner();
            clock_zero_standby_replacement(&mut o);
            let deadline = o.health.rotation_timeout;
            tokio::time::advance(deadline).await;
            timer_pass(&mut o);
            assert_eq!(o.stats.failed_rotations, 1);

            advance_to_rotation_due(&o).await;
            timer_pass(&mut o);
            let fresh = standby_replacement(&mut o, &["A", "B", "C"]);
            book_snapshot(&mut o, fresh, "A", 100, 10);
            book_snapshot(&mut o, fresh, "B", 100, 10);
            // One second before the deadline, so the carried five-second bound
            // on this catch-up outlives the operation's own deadline.
            tokio::time::advance(deadline - Duration::from_secs(1)).await;
            book_snapshot(&mut o, fresh, "C", 0, 0);
            assert!(o.feeds[1].books["C"].catchup.is_some());
            if invalidated {
                o.input(Input::Invalid {
                    source: fresh,
                    symbols: Some(vec!["C".into()]),
                    reason: "source-reported loss".into(),
                })
                .unwrap();
                assert!(!o.feeds[1].books["C"].value.is_valid());
            }
            tokio::time::advance(Duration::from_secs(1)).await;
            timer_pass(&mut o);
            assert!(o.rotation.is_some(), "armed catch-up was abandoned");
            assert_eq!(o.stats.rotation_timeouts, 2);
            assert_eq!(o.stats.failed_rotations, 1);
            tokio::time::advance(Duration::from_secs(4) - Duration::from_millis(1)).await;
            timer_pass(&mut o);
            assert!(o.rotation.is_some());
            assert_eq!(o.stats.rotation_timeouts, 2, "timeout is one-shot");
            tokio::time::advance(Duration::from_millis(1)).await;
            timer_pass(&mut o);
            if invalidated {
                // Only Begin retires this state: the catch-up stays outstanding.
                assert!(o.rotation.is_some(), "a live catch-up was abandoned");
                assert_eq!(o.stats.failed_rotations, 1);
            } else {
                assert!(o.rotation.is_none());
                assert_eq!(o.stats.failed_rotations, 2);
            }
            assert_eq!(o.stats.rotation_timeouts, 2);
        }
    }

    /// Neither request is reachable alongside a terminal book in production.
    /// Each is set directly to prove the terminal predicate requires its absence.
    #[tokio::test(start_paused = true)]
    async fn an_outstanding_probe_or_recovery_blocks_the_terminal_decision() {
        for recovery in [true, false] {
            let mut o = multibook_owner();
            clock_zero_standby_replacement(&mut o);
            tokio::time::advance(o.health.rotation_timeout - Duration::from_millis(1)).await;
            timer_pass(&mut o);
            let book = o.feeds[1].books.get_mut("C").unwrap();
            if recovery {
                book.recovery = Some((
                    crate::relay::Resubscribe {
                        incarnation: 1,
                        generation: 2,
                        nonce: 1,
                    },
                    true,
                ));
            }
            tokio::time::advance(Duration::from_millis(1)).await;
            o.tick().unwrap();
            assert!(o.feeds[1].books["C"].probe_quarantined);
            if !recovery {
                o.feeds[1].books.get_mut("C").unwrap().probe = Some(Frontier::snapshot(100));
            }
            o.rotation_tick().unwrap();
            assert!(o.rotation.is_some(), "a request in flight was abandoned");
            assert_eq!(o.stats.rotation_timeouts, 1);
            assert_eq!(o.stats.failed_rotations, 0);
            let book = o.feeds[1].books.get_mut("C").unwrap();
            book.recovery = None;
            book.probe = None;
            timer_pass(&mut o);
            assert!(o.rotation.is_none());
            assert_eq!(o.stats.failed_rotations, 1);
            assert_eq!(o.stats.rotation_timeouts, 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_late_baseline_heals_the_book_but_only_a_new_operation_earns_credit() {
        let mut o = multibook_owner();
        let fresh = clock_zero_standby_replacement(&mut o);
        tokio::time::advance(o.health.rotation_timeout).await;
        timer_pass(&mut o);
        assert_eq!(o.stats.failed_rotations, 1);
        let frontier = o.feeds[1].books["C"].frontier;

        // Old generation, old admission and old incarnation cannot heal it.
        let stale = o.stats.stale_events;
        book_snapshot(&mut o, source(1), "C", 1000, 10);
        o.input(Input::Begin {
            source: Source {
                replica: 1,
                generation: fresh.generation + 1,
            },
            admission: restarts(&o, 1) - 1,
        })
        .unwrap();
        o.input(Input::Data {
            source: fresh,
            incarnation: 9,
            received_tick: Instant::now(),
            event: BookEvent::Snapshot(Snapshot {
                symbol: "C".into(),
                received_at: Utc::now(),
                exchange_at: Utc::now(),
                clock: Some(1000),
                bids: vec![],
                asks: vec![],
            }),
        })
        .unwrap();
        assert_eq!(o.stats.stale_events, stale + 3);
        assert_eq!(o.feeds[1].generation, fresh.generation);
        assert_eq!(o.feeds[1].books["C"].frontier, frontier);
        assert!(!o.ready(1, "C"));
        assert!(o.feeds[1].books["C"].probe_quarantined);

        // Same generation, usable: readiness returns, the failure does not leave.
        book_snapshot(&mut o, fresh, "C", 100, 10);
        timer_pass(&mut o);
        assert!(o.ready(1, "C"));
        assert!(!o.feeds[1].books["C"].probe_quarantined);
        assert_eq!(o.feeds[1].books["C"].rejected_baselines, 0);
        assert!(o.rotation.is_none());
        assert_eq!(o.stats.completed_rotations, 0, "retroactive completion");
        assert_eq!(o.stats.failed_rotations, 1);
        assert_eq!(o.stats.rotation_timeouts, 1);

        // A later independently admitted operation can earn its own credit.
        advance_to_rotation_due(&o).await;
        timer_pass(&mut o);
        assert_eq!(restarts(&o, 1), 2);
        let next = standby_replacement(&mut o, &["A", "B", "C"]);
        for symbol in ["A", "B", "C"] {
            book_snapshot(&mut o, next, symbol, 100, 10);
        }
        timer_pass(&mut o);
        assert_eq!(o.stats.completed_rotations, 1);
        assert_eq!(o.stats.failed_rotations, 1);
        assert_eq!(o.stats.rotation_timeouts, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn publisher_loss_during_a_quarantined_retry_keeps_the_five_second_reset() {
        let mut o = owner();
        start(&mut o, 0, 100, 10);
        start(&mut o, 1, 100, 10);
        release_standby_restart(&mut o);
        let fresh = standby_replacement(&mut o, &["A"]);
        book_snapshot(&mut o, fresh, "A", 0, 0);
        tokio::time::advance(o.health.rotation_timeout).await;
        timer_pass(&mut o);
        assert_eq!(o.stats.failed_rotations, 1);

        advance_to_rotation_due(&o).await;
        timer_pass(&mut o);
        let fresh = standby_replacement(&mut o, &["A"]);
        book_snapshot(&mut o, fresh, "A", 0, 0);
        tokio::time::advance(Duration::from_secs(5)).await;
        timer_pass(&mut o);
        assert!(o.terminally_quarantined(1, "A"));
        assert!(o.rotation.is_some());

        // The publisher fails while the rotation is open and the deadline is near.
        tokio::time::advance(Duration::from_secs(20)).await;
        timer_pass(&mut o);
        fault(&mut o, 0);
        assert_eq!(o.selected["A"].active, None);
        assert_eq!(
            o.selected["A"].candidate, None,
            "promoted a quarantined book"
        );
        assert!(o.selected["A"].unpublished_since.is_some());

        // The operation deadline passes without a publisher: the terminal
        // decision yields to the publication-loss path.
        tokio::time::advance(Duration::from_secs(5) - Duration::from_millis(1)).await;
        timer_pass(&mut o);
        assert_eq!(o.stats.hard_resets, 0, "the five-second bound shrank");
        assert!(o.rotation.is_some());
        tokio::time::advance(Duration::from_millis(1)).await;
        timer_pass(&mut o);
        assert_eq!(
            o.stats.failed_rotations, 1,
            "failed with no publisher to retain"
        );
        assert_eq!(o.stats.rotation_timeouts, 2);
        assert!(o.rotation.is_some());

        // A stale reconnect of the old publisher trips the unchanged bound.
        let publisher = Source {
            replica: 0,
            generation: 2,
        };
        begin(&mut o, publisher);
        o.input(Input::Membership {
            roster: None,
            source: publisher,
            wire: BTreeSet::from(["A".into()]),
            at: Utc::now(),
        })
        .unwrap();
        book_snapshot(&mut o, publisher, "A", 50, 10);
        assert_eq!(o.selected["A"].active, None, "uncovering book published");
        timer_pass(&mut o);
        assert_eq!(o.stats.hard_resets, 1);
        assert_eq!(o.selected["A"].floor, None);
        // The reset ended that operation and its restart was not held back.
        assert_eq!(restarts(&o, 0), 1);
        assert_eq!(o.stats.failed_rotations, 1);
        assert_eq!(o.stats.completed_rotations, 0);
        assert!(!o.feeds[1].books["A"].probe_quarantined, "reset clears it");
    }

    #[tokio::test(start_paused = true)]
    async fn superseding_every_admitted_lifetime_defers_and_never_counts_as_failed() {
        let mut o = owner();
        start(&mut o, 0, 100, 10);
        start(&mut o, 1, 100, 10);
        release_standby_restart(&mut o);
        let fresh = standby_replacement(&mut o, &["A"]);
        book_snapshot(&mut o, fresh, "A", 0, 0);
        tokio::time::advance(o.health.rotation_timeout).await;
        timer_pass(&mut o);
        assert_eq!(o.stats.failed_rotations, 1);

        advance_to_rotation_due(&o).await;
        timer_pass(&mut o);
        let fresh = standby_replacement(&mut o, &["A"]);
        book_snapshot(&mut o, fresh, "A", 0, 0);
        tokio::time::advance(Duration::from_secs(5)).await;
        timer_pass(&mut o);
        assert!(o.terminally_quarantined(1, "A"));
        let timeouts = o.stats.rotation_timeouts;

        reassign(&mut o, &[("A", 2)]);
        tokio::time::advance(o.health.rotation_timeout).await;
        timer_pass(&mut o);
        assert!(o.rotation.is_none());
        assert_eq!(o.stats.deferred_rotations, 1);
        assert_eq!(
            o.stats.failed_rotations, 1,
            "supersession counted as failure"
        );
        assert_eq!(o.stats.completed_rotations, 0);
        assert_eq!(o.stats.rotation_timeouts, timeouts);
    }

    #[tokio::test(start_paused = true)]
    async fn a_retired_peer_lifetime_does_not_excuse_a_surviving_quarantined_book() {
        let mut o = multibook_owner();
        clock_zero_standby_replacement(&mut o);
        reassign(&mut o, &[("B", 1), ("C", 1)]);
        tokio::time::advance(o.health.rotation_timeout).await;
        timer_pass(&mut o);
        assert!(o.rotation.is_none());
        assert_eq!(o.stats.failed_rotations, 1);
        assert_eq!(o.stats.deferred_rotations, 0);
        assert_eq!(o.stats.completed_rotations, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn health_maintenance_recovers_another_book_after_a_failed_rotation() {
        let mut o = multibook_owner();
        o.health.window = Duration::from_secs(4);
        o.health.warmup = Duration::from_secs(4);
        o.health.cooldown = Duration::from_secs(4);
        clock_zero_standby_replacement(&mut o);
        tokio::time::advance(o.health.rotation_timeout).await;
        timer_pass(&mut o);
        assert_eq!(o.stats.failed_rotations, 1);
        assert_eq!(o.stats.health_resubscriptions, 0);

        // B's publisher (replica 0) falls silent while its peer keeps trading.
        tokio::time::advance(o.health.cooldown).await;
        let epoch = Instant::now().duration_since(o.origin).as_secs() / o.health.bucket_seconds();
        for _ in 0..20 {
            o.feeds[1]
                .books
                .get_mut("B")
                .unwrap()
                .activity
                .window
                .record(epoch - 1);
        }
        timer_pass(&mut o);
        assert!(
            o.planned.is_some(),
            "health work stayed blocked after the failure"
        );
        cut(&mut o, 1);
        o.release_planned().unwrap();
        assert_eq!(o.stats.health_resubscriptions, 1);
        let control = o.feeds[0].control.borrow();
        assert_eq!(control.resubscribe["B"].incarnation, 1);
        assert_eq!(control.restart, 0, "health work restarted the publisher");
        assert!(!control.resubscribe.contains_key("C"));
    }

    /// Real native Sources over localhost WebSockets: each replacement answers
    /// its subscribe and `get_snapshot` for one quiet market with clock zero.
    /// The surviving publisher socket is never disturbed; the watched progress
    /// row and server-side counts are the typed barriers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clock_zero_replacement_answers_release_maintenance_and_bound_reconnects() {
        use futures_util::{SinkExt, StreamExt};
        use std::sync::Mutex;
        use tokio_tungstenite::tungstenite::Message;
        type Exchange = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;
        #[derive(Default)]
        struct Wire {
            accepted: usize,
            closed: usize,
            get_snapshots: usize,
        }
        async fn serve(mut ws: Exchange, connection: usize, wire: Arc<Mutex<Wire>>) {
            let replacement = connection >= 2;
            let mut seq = 0u64;
            while let Some(Ok(frame)) = ws.next().await {
                let text = match frame {
                    Message::Text(text) => text,
                    Message::Ping(p) => {
                        ws.send(Message::Pong(p)).await.ok();
                        continue;
                    }
                    Message::Close(_) => {
                        ws.flush().await.ok();
                        wire.lock().unwrap().closed += 1;
                        return;
                    }
                    _ => continue,
                };
                let request: serde_json::Value = serde_json::from_str(&text).unwrap();
                let tickers: Vec<String> = request["params"]["market_tickers"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|m| m.as_str().unwrap().to_owned())
                    .collect();
                let mut replies = vec![];
                if request["cmd"] == "subscribe" {
                    replies.push(serde_json::json!({"id":request["id"], "type":"subscribed", "msg":{"sid":7}}));
                } else {
                    assert_eq!(request["params"]["action"], "get_snapshot", "{request}");
                    wire.lock().unwrap().get_snapshots += 1;
                    seq += 1;
                    replies.push(serde_json::json!({"id":request["id"], "type":"ok", "sid":7, "seq":seq, "msg":{}}));
                }
                for market in tickers {
                    seq += 1;
                    let body = if replacement && market == "QZ" {
                        serde_json::json!({"market_ticker":market, "clock":0,
                            "no_dollars_fp":[["0.99","4457.41"]]})
                    } else {
                        serde_json::json!({"market_ticker":market, "clock":1000,
                            "yes_dollars_fp":[["0.40","10"]]})
                    };
                    replies.push(serde_json::json!({"type":"orderbook_snapshot", "sid":7,
                        "seq":seq, "sending_ts_ms":1700000000000i64, "msg":body}));
                }
                for reply in replies {
                    if ws
                        .send(Message::Text(reply.to_string().into()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
        async fn reported(
            row: &mut watch::Receiver<ShardStats>,
            what: &str,
            done: impl Fn(&ShardStats) -> bool,
        ) -> ShardStats {
            tokio::time::timeout(Duration::from_secs(45), async {
                loop {
                    if done(&row.borrow_and_update()) {
                        return row.borrow().clone();
                    }
                    row.changed().await.expect("shard stopped reporting");
                }
            })
            .await
            .unwrap_or_else(|_| panic!("timed out at {what}: {:?}", row.borrow()))
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_url = format!("ws://{}/", listener.local_addr().unwrap());
        let wire = Arc::new(Mutex::new(Wire::default()));
        let server_wire = wire.clone();
        let server = tokio::spawn(async move {
            let mut connection = 0;
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                server_wire.lock().unwrap().accepted += 1;
                let wire = server_wire.clone();
                tokio::spawn(async move {
                    if let Ok(ws) = tokio_tungstenite::accept_async(stream).await {
                        serve(ws, connection, wire).await;
                    }
                });
                connection += 1;
            }
        });

        let symbols = vec!["G1".to_owned(), "QZ".to_owned()];
        let temp = tempfile::tempdir().unwrap();
        let writer =
            crate::output::Writer::start(&temp.path().join("wire.sbe"), None, 64, 1 << 20).unwrap();
        let options = crate::collector::Options {
            symbols: symbols.clone(),
            rest_url: "unused".into(),
            ws_url,
            symbols_per_connection: 2,
            max_connections: 2,
            max_symbols: 2,
            replicas: 2,
            replica_lag: Duration::from_millis(100),
            failover_test_after: None,
            output: temp.path().join("unused.sbe"),
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
                window: Duration::from_secs(4),
                warmup: Duration::from_secs(4),
                cooldown: Duration::from_secs(4),
                max_resubscriptions: 2,
                rotation_interval: Duration::from_secs(1),
                rotation_timeout: Duration::from_secs(1),
                ..Default::default()
            },
        };
        let shared = source::Shared {
            protocol: Arc::new(crate::exchange::kalshi::Kalshi),
            options: Arc::new(options),
            auth: None,
            pacer: Arc::new(crate::collector::Pacer::new(Duration::from_millis(1))),
            recovery_pacer: Arc::new(crate::collector::Pacer::new(Duration::from_millis(500))),
        };
        let (_assign, assignment) = watch::channel(Assignment {
            epoch: 1,
            incarnations: symbols.iter().map(|s| (s.clone(), 1)).collect(),
            symbols,
        });
        let (phase_tx, phase) = watch::channel(WorkerPhase::Running);
        let (progress, mut row) = watch::channel(ShardStats::default());
        let shard = run(
            0,
            assignment,
            shared,
            writer.output.clone(),
            phase,
            progress,
            Arc::new(Semaphore::new(1 << 20)),
        );
        let driver = async {
            let failed = reported(&mut row, "first failed rotation", |s| {
                s.failed_rotations >= 1 && s.maintenance_pending == 0
            })
            .await;
            assert_eq!(failed.failed_rotations, 1, "{failed:?}");
            assert_eq!(failed.planned_rotations, 1, "{failed:?}");
            assert_eq!(failed.completed_rotations, 0, "{failed:?}");
            assert_eq!(failed.rotation_timeouts, 1, "{failed:?}");
            assert_eq!(failed.initialized, 2, "publisher retained: {failed:?}");
            let (accepted, get_snapshots) = {
                let wire = wire.lock().unwrap();
                (wire.accepted, wire.get_snapshots)
            };
            assert_eq!(accepted, 3, "two originals and one replacement");
            assert_eq!(get_snapshots, 1, "one real scoped snapshot request");

            let bounded = reported(&mut row, "retry budget deferral", |s| {
                s.deferred_rotations >= 2
            })
            .await;
            let wire = wire.lock().unwrap();
            assert_eq!(wire.accepted, 4, "reconnects were not bounded: {bounded:?}");
            assert_eq!(wire.closed, 2, "the publisher socket closed: {bounded:?}");
            assert_eq!(wire.get_snapshots, 1, "a retry re-probed: {bounded:?}");
            drop(wire);
            assert_eq!(bounded.planned_rotations, 2, "{bounded:?}");
            assert_eq!(bounded.failed_rotations, 2, "{bounded:?}");
            assert_eq!(bounded.completed_rotations, 0, "{bounded:?}");
            assert_eq!(bounded.maintenance_pending, 0, "{bounded:?}");
            assert_eq!(bounded.initialized, 2, "publisher retained: {bounded:?}");
            assert_eq!(bounded.replica_initialized, vec![1, 2], "{bounded:?}");
            assert_eq!(
                (bounded.gaps, bounded.continuity_gaps, bounded.hard_resets),
                (0, 0, 0),
                "{bounded:?}"
            );
            phase_tx.send(WorkerPhase::Stopping).unwrap();
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(90), async {
            tokio::join!(shard, driver)
        })
        .await
        .expect("shard did not stop and join");
        server.abort();
        result.expect("shard run failed");
        writer.finish().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn rotation_scope_completes_admitted_roster_without_waiting_for_a_later_addition() {
        let mut o = owner();
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 10, 20);
        release_native_rotation(&mut o);
        reassign(&mut o, &[("A", 1), ("B", 2)]);
        let fresh = replacement(&mut o, &["A"]);
        o.rotation_tick().unwrap();
        assert!(o.rotation.is_some(), "admitted A still owes readiness");
        assert_eq!(o.stats.completed_rotations, 0);
        snap(&mut o, fresh, 10, 20);
        o.rotation_tick().unwrap();
        assert_eq!(
            o.stats.completed_rotations, 1,
            "a market added after release held the admitted rotation open"
        );
        assert!(o.rotation.is_none());
        assert_eq!(o.stats.rotation_timeouts, 0);
        assert_eq!(o.stats.deferred_rotations, 0);
        // The addition is separate work and stays visible as missing.
        assert!(!o.ready(0, "B") && !o.ready(1, "B"));
        let (progress, row) = watch::channel(ShardStats::default());
        o.report(&[], &progress);
        for cutoff in [false, true] {
            if cutoff {
                let (_latest, mut pending) = watch::channel(o.assignment.clone());
                o.report_cutoff(&mut pending, &[], &progress);
            }
            let row = row.borrow();
            assert_eq!(row.desired, 2);
            assert_eq!(row.initialized, 1);
            assert_eq!(row.replica_initialized, vec![1, 1]);
            assert_eq!(row.maintenance_pending, 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rotation_scope_admitted_book_unready_at_the_deadline_times_out_exactly_once() {
        for uncovered_baseline in [false, true] {
            let mut o = owner();
            start(&mut o, 0, 10, 20);
            start(&mut o, 1, 10, 20);
            release_native_rotation(&mut o);
            let deadline = o.health.rotation_timeout;
            // The deadline runs from release; a later Begin cannot restart it.
            let before_begin = Duration::from_secs(10);
            tokio::time::advance(before_begin).await;
            let fresh = replacement(&mut o, &["A"]);
            if uncovered_baseline {
                snap(&mut o, fresh, 5, 20); // below the frozen floor of 10
            }
            assert!(!o.ready(0, "A"));
            reassign(&mut o, &[("A", 1), ("B", 2)]);
            tokio::time::advance(deadline - before_begin - Duration::from_millis(1)).await;
            o.rotation_tick().unwrap();
            assert_eq!(o.stats.rotation_timeouts, 0, "deadline moved earlier");
            tokio::time::advance(Duration::from_millis(1)).await;
            o.rotation_tick().unwrap();
            assert_eq!(o.stats.rotation_timeouts, 1, "deadline was extended");
            assert!(o.rotation.is_some());
            tokio::time::advance(deadline).await;
            o.rotation_tick().unwrap();
            assert_eq!(o.stats.rotation_timeouts, 1, "timeout is one-shot");
            snap(&mut o, fresh, 10, 20);
            assert!(o.ready(0, "A"));
            o.rotation_tick().unwrap();
            assert_eq!(
                o.stats.completed_rotations, 1,
                "later readiness of admitted A finishes the operation"
            );
            assert_eq!(o.stats.rotation_timeouts, 1, "completion erased a timeout");
            assert!(o.rotation.is_none());
            assert!(!o.ready(0, "B"));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rotation_scope_partial_retirement_keeps_the_surviving_admitted_obligation() {
        let mut o = multibook_owner();
        release_native_rotation(&mut o);
        reassign(&mut o, &[("A", 1), ("C", 1)]);
        let fresh = replacement(&mut o, &["A", "C"]);
        book_snapshot(&mut o, fresh, "C", 100, 10);
        o.rotation_tick().unwrap();
        assert!(o.rotation.is_some(), "surviving admitted A was forgiven");
        assert_eq!(o.stats.completed_rotations, 0);
        assert_eq!(o.stats.deferred_rotations, 0);
        tokio::time::advance(o.health.rotation_timeout).await;
        o.rotation_tick().unwrap();
        assert_eq!(o.stats.rotation_timeouts, 1);
        book_snapshot(&mut o, fresh, "A", 100, 10);
        o.rotation_tick().unwrap();
        assert_eq!(o.stats.completed_rotations, 1);
        assert_eq!(o.stats.rotation_timeouts, 1);
        assert_eq!(o.stats.deferred_rotations, 0);
    }

    async fn no_surviving_lifetime_defers_without_credit(
        market: (&str, u64),
        replacement_ready: bool,
    ) {
        let (symbol, _) = market;
        for timed_out_first in [false, true] {
            let mut o = owner();
            start(&mut o, 0, 10, 20);
            start(&mut o, 1, 10, 20);
            release_native_rotation(&mut o);
            reassign(&mut o, &[market]);
            if timed_out_first {
                tokio::time::advance(o.health.rotation_timeout).await;
                o.rotation_tick().unwrap();
                assert_eq!(o.stats.rotation_timeouts, 1);
            }
            let fresh = replacement(&mut o, &[symbol]);
            if replacement_ready {
                book_snapshot(&mut o, fresh, symbol, 10, 10);
                assert!(o.ready(0, symbol));
            }
            o.rotation_tick().unwrap();
            assert_eq!(
                o.stats.completed_rotations, 0,
                "completion credit without a surviving admitted lifetime"
            );
            assert_eq!(o.stats.completed_health_renewals, 0);
            assert!(
                o.rotation.is_none(),
                "an operation with no admitted lifetime left stayed open"
            );
            assert_eq!(o.stats.deferred_rotations, 1);
            assert_eq!(
                o.stats.rotation_timeouts,
                if timed_out_first { 1 } else { 0 },
                "deferral changed the timeout record"
            );
            assert_eq!(o.ready(0, symbol), replacement_ready);
            assert_eq!(o.feeds[0].control.borrow().restart, 1);
            // Bounded retry through the existing maintenance cooldown.
            let cooldown = o.health.cooldown;
            let retry_at = Instant::now() + cooldown;
            assert_eq!(o.rotation_due, Some(retry_at));
            assert_eq!(o.maintenance_deferred_until, Some(retry_at));
            tokio::time::advance(cooldown - Duration::from_millis(1)).await;
            o.rotation_tick().unwrap();
            assert_eq!(o.stats.planned_rotations, 1, "cooldown was bypassed");
            tokio::time::advance(Duration::from_millis(1)).await;
            o.rotation_tick().unwrap();
            if replacement_ready {
                // The replacement now has one publisher and no covering peer.
                assert_eq!(o.stats.planned_rotations, 1);
                assert_eq!(o.stats.deferred_rotations, 2);
                assert_eq!(o.feeds[0].control.borrow().restart, 1);
            } else {
                assert_eq!(o.stats.planned_rotations, 2, "retry was never scheduled");
            }
            assert_eq!(o.stats.completed_rotations, 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rotation_scope_all_retired_with_unready_replacement_defers() {
        no_surviving_lifetime_defers_without_credit(("B", 2), false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn rotation_scope_all_retired_with_ready_replacement_earns_no_credit() {
        no_surviving_lifetime_defers_without_credit(("B", 2), true).await;
    }

    #[tokio::test(start_paused = true)]
    async fn rotation_scope_reincarnation_with_unready_replacement_defers() {
        no_surviving_lifetime_defers_without_credit(("A", 2), false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn rotation_scope_reincarnation_with_ready_replacement_earns_no_credit() {
        no_surviving_lifetime_defers_without_credit(("A", 2), true).await;
    }

    #[test]
    fn rotation_scope_empty_assignment_keeps_its_cancellation_without_credit() {
        let mut o = owner();
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 10, 20);
        release_native_rotation(&mut o);
        reassign(&mut o, &[]);
        o.rotation_tick().unwrap();
        assert!(o.rotation.is_none());
        assert_eq!(o.stats.completed_rotations, 0);
        assert_eq!(o.stats.deferred_rotations, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn rotation_scope_late_old_evidence_cannot_satisfy_the_operation_or_a_replacement() {
        let mut o = multibook_owner();
        release_native_rotation(&mut o);
        let floor = o.selected["C"].floor;
        // B retires and A is replaced; C is the only surviving admitted lifetime.
        reassign(&mut o, &[("A", 2), ("C", 1)]);
        let stale = o.stats.stale_events;
        // Old generation, fenced by the release, before its replacement begins.
        book_snapshot(&mut o, source(0), "C", 999, 77);
        // A dial admitted before the restart nonce cannot be the replacement.
        o.input(Input::Begin {
            source: Source {
                replica: 0,
                generation: 2,
            },
            admission: 0,
        })
        .unwrap();
        assert_eq!(o.feeds[0].generation, 1);
        assert_eq!(o.stats.stale_events, stale + 2);
        let fresh = replacement(&mut o, &["A", "C"]);
        book_snapshot(&mut o, source(0), "C", 999, 77); // old generation again
        snap(&mut o, fresh, 500, 77); // A incarnation 1 on the new generation
        assert_eq!(o.stats.stale_events, stale + 4);
        assert!(!o.ready(0, "A") && !o.ready(0, "C"));
        assert!(!o.selected["A"].ever);
        assert_eq!(o.selected["A"].floor, None);
        assert_eq!(o.selected["C"].floor, floor, "late evidence moved a floor");
        o.rotation_tick().unwrap();
        assert!(o.rotation.is_some(), "late evidence satisfied surviving C");
        assert_eq!(o.stats.completed_rotations, 0);
        book_snapshot(&mut o, fresh, "C", 100, 10);
        o.rotation_tick().unwrap();
        assert_eq!(
            o.stats.completed_rotations, 1,
            "a replacement incarnation held the admitted rotation open"
        );
        assert!(!o.ready(0, "A"), "old incarnation initialized A2");
        assert!(!o.selected["A"].ever);
        assert_eq!(o.selected["A"].floor, None);
    }

    #[tokio::test(start_paused = true)]
    async fn rotation_scope_sole_publisher_of_a_later_market_survives_the_next_rotation() {
        let mut o = owner();
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 10, 20);
        release_native_rotation(&mut o);
        reassign(&mut o, &[("A", 1), ("B", 2)]);
        o.input(Input::Membership {
            roster: None,
            source: source(1),
            wire: BTreeSet::from(["A".into(), "B".into()]),
            at: Utc::now(),
        })
        .unwrap();
        book_snapshot(&mut o, source(1), "B", 50, 10);
        assert_eq!(o.selected["B"].active, Some(source(1)));
        let fresh = replacement(&mut o, &["A"]);
        snap(&mut o, fresh, 10, 20);
        o.rotation_tick().unwrap();
        assert_eq!(o.stats.completed_rotations, 1);
        assert!(!o.ready(0, "B"));
        // The completed rotation is no authority over B's only publisher.
        assert!(!o.start_rotation(1).unwrap());
        assert!(no_pause_requested(&o));
        assert_eq!(o.feeds[1].control.borrow().restart, 0);
        o.input(Input::Membership {
            roster: None,
            source: fresh,
            wire: BTreeSet::from(["A".into(), "B".into()]),
            at: Utc::now(),
        })
        .unwrap();
        book_snapshot(&mut o, fresh, "B", 40, 10); // does not cover the floor of 50
        assert!(!o.ready(0, "B"));
        assert!(!o.start_rotation(1).unwrap());
        assert!(no_pause_requested(&o));
        assert_eq!(o.feeds[1].control.borrow().restart, 0);
        assert_eq!(o.selected["B"].active, Some(source(1)));
        book_snapshot(&mut o, fresh, "B", 50, 10);
        assert!(o.ready(0, "B"));
        assert!(o.start_rotation(1).unwrap());
        assert_eq!(o.feeds[1].control.borrow().restart, 0);
        assert_eq!(cut(&mut o, 0).len(), 2);
        o.release_planned().unwrap();
        assert_eq!(o.feeds[1].control.borrow().restart, 1);
        assert_eq!(o.selected["B"].active, Some(fresh));
        assert_eq!((o.stats.continuity_gaps, o.stats.hard_resets), (0, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn rotation_scope_completion_uses_readiness_and_authorizes_no_later_cut() {
        let mut o = owner();
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 10, 20);
        release_native_rotation(&mut o);
        let fresh = replacement(&mut o, &["A"]);
        snap(&mut o, fresh, 10, 20);
        // The surviving publisher moves on after the frozen goal was met.
        delta(&mut o, source(1), 11, 1);
        delta(&mut o, source(1), 12, 1);
        assert!(o.ready(0, "A"));
        assert_eq!(o.selected["A"].floor.unwrap().clock, 12);
        o.rotation_tick().unwrap();
        assert_eq!(
            o.stats.completed_rotations, 1,
            "completion chased the moving publication floor"
        );
        // That completion cannot authorize cutting onto the uncovering book.
        assert!(o.start_rotation(1).unwrap());
        let blocked = o.stats.promotions_blocked;
        assert!(cut(&mut o, 0).is_empty());
        assert_eq!(o.stats.promotions_blocked, blocked + 1);
        assert_eq!(o.selected["A"].active, Some(source(1)));
        o.release_planned().unwrap();
        assert_eq!(o.feeds[1].control.borrow().restart, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn rotation_scope_pre_release_catalog_change_cannot_become_admitted_scope() {
        let changes: [&[(&str, u64)]; 3] = [&[("A", 1), ("B", 2)], &[("B", 2)], &[("A", 2)]];
        for markets in changes {
            let mut o = owner();
            start(&mut o, 0, 10, 20);
            start(&mut o, 1, 10, 20);
            assert!(o.start_rotation(0).unwrap());
            let token = o.feeds[0].control.borrow().pause.unwrap();
            assert_eq!(cut(&mut o, 1).len(), 1);
            reassign(&mut o, markets);
            assert!(o.planned.is_none(), "{markets:?}");
            assert!(o.rotation.is_none(), "{markets:?}");
            o.release_planned().unwrap();
            let control = o.feeds[0].control.borrow().clone();
            assert_eq!(control.restart, 0, "{markets:?}");
            assert!(control.pause.is_none(), "{markets:?}");
            assert_eq!(
                control.pause_release,
                Some(crate::relay::PauseId {
                    generation: 1,
                    nonce: token.nonce
                })
            );
            assert!(o.feeds[0].pause.is_some(), "reservation awaits its end");
            o.input(Input::PauseEnded {
                source: source(0),
                nonce: token.nonce,
                reason: crate::relay::PauseEnd::Released,
            })
            .unwrap();
            o.rotation_tick().unwrap();
            assert_eq!(o.feeds[0].control.borrow().restart, 0, "{markets:?}");
            assert!(o.rotation.is_none(), "{markets:?}");
            assert_eq!(o.stats.completed_rotations, 0, "{markets:?}");
            assert_eq!(o.stats.rotation_timeouts, 0, "{markets:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rotation_scope_epoch_only_update_keeps_the_planned_rotation() {
        let mut o = owner();
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 10, 20);
        assert!(o.start_rotation(0).unwrap());
        assert_eq!(cut(&mut o, 1).len(), 1);
        reassign(&mut o, &[("A", 1)]);
        assert!(o.planned.is_some() && o.rotation.is_some());
        o.release_planned().unwrap();
        assert_eq!(o.feeds[0].control.borrow().restart, 1);
        let fresh = replacement(&mut o, &["A"]);
        snap(&mut o, fresh, 10, 20);
        o.rotation_tick().unwrap();
        assert_eq!(o.stats.completed_rotations, 1);
        assert_eq!(o.stats.deferred_rotations, 0);
        assert_eq!(o.stats.rotation_timeouts, 0);
    }

    #[test]
    fn a_late_native_probe_answer_can_be_a_legitimate_active_recovery_baseline() {
        let mut o = owner();
        start(&mut o, 0, 10, 20);
        start(&mut o, 1, 5, 20);
        assert_eq!(outstanding_requests(&o, 1, "A"), 1);
        // The ordered tail resolves acquisition while a snapshot command/reply
        // is still in flight. Resolution does not recall bytes already handed off.
        delta(&mut o, source(1), 11, 1);
        assert!(o.ready(1, "A"));
        assert!(o.start_rotation(0).unwrap());
        assert!(matches!(
            &cut(&mut o, 1)[..],
            [Emit::Snapshot {
                reason: SnapshotReason::Failover,
                ..
            }]
        ));
        o.release_planned().unwrap();
        assert_eq!(o.selected["A"].active, Some(source(1)));
        let late = snap(&mut o, source(1), 12, 21);
        assert!(matches!(
            &late[..],
            [Emit::Snapshot {
                reason: SnapshotReason::Recovery,
                ..
            }]
        ));
        assert_eq!(o.selected["A"].floor.unwrap().clock, 12);
        assert_eq!(
            (
                o.stats.hard_resets,
                o.stats.continuity_gaps,
                o.stats.clock_regressions,
                o.stats.invalid_books
            ),
            (0, 0, 0, 0)
        );
    }

    /// The 2026-11-08 universe kept 24 shards running on one replica for up
    /// to ~2300 s after a source task returned, then exited 1 anyway. A task
    /// that has ended terminally is an explicit shard failure now: stop and
    /// join both sources promptly instead of continuing degraded.
    #[tokio::test]
    async fn a_terminally_failed_source_task_stops_its_shard_instead_of_running_degraded() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;
        type Exchange = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;
        async fn subscribe(ws: &mut Exchange) -> serde_json::Value {
            loop {
                match ws.next().await.unwrap().unwrap() {
                    Message::Text(text) => return serde_json::from_str(&text).unwrap(),
                    Message::Ping(p) => ws.send(Message::Pong(p)).await.unwrap(),
                    other => panic!("unexpected native startup frame: {other:?}"),
                }
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
            panic!("source dropped its socket without a Close handshake");
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let temp = tempfile::tempdir().unwrap();
        let writer =
            crate::output::Writer::start(&temp.path().join("dead.sbe"), None, 16, 1 << 20).unwrap();
        let options = crate::collector::Options {
            symbols: vec!["A".into()],
            rest_url: "unused".into(),
            ws_url: format!("ws://{}/", listener.local_addr().unwrap()),
            symbols_per_connection: 1,
            max_connections: 2,
            max_symbols: 1,
            replicas: 2,
            replica_lag: Duration::from_millis(100),
            failover_test_after: None,
            output: temp.path().join("unused.sbe"),
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
                rotation_interval: Duration::ZERO,
                ..Default::default()
            },
        };
        let shared = source::Shared {
            protocol: Arc::new(crate::exchange::kalshi::Kalshi),
            options: Arc::new(options),
            auth: None,
            pacer: Arc::new(crate::collector::Pacer::new(Duration::from_millis(1))),
            recovery_pacer: Arc::new(crate::collector::Pacer::new(Duration::from_millis(500))),
        };
        let (_assign, assignment) = watch::channel(Assignment {
            epoch: 1,
            symbols: vec!["A".into()],
            incarnations: BTreeMap::from([("A".into(), 1)]),
        });
        let (_phase, phase) = watch::channel(WorkerPhase::Running);
        let (progress, _row) = watch::channel(ShardStats::default());
        let shard = run(
            0,
            assignment,
            shared,
            writer.output.clone(),
            phase,
            progress,
            Arc::new(Semaphore::new(1 << 20)),
        );
        let exchange = async {
            let mut sockets = vec![];
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                sockets.push(tokio_tungstenite::accept_async(stream).await.unwrap());
            }
            let mut healthy = sockets.pop().unwrap();
            let mut fatal = sockets.pop().unwrap();
            let c = subscribe(&mut healthy).await;
            for event in [
                serde_json::json!({"id":c["id"], "type":"subscribed", "msg":{"sid":7}}),
                serde_json::json!({"type":"orderbook_snapshot", "sid":7, "seq":1,
                    "sending_ts_ms":1700000000000i64,
                    "msg":{"market_ticker":"A", "clock":101, "yes_dollars_fp":[["0.4","10"]]}}),
            ] {
                healthy
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .unwrap();
            }
            // Non-retryable by the native contract: this Source returns.
            let c = subscribe(&mut fatal).await;
            fatal
                .send(Message::Text(
                    serde_json::json!({"id":c["id"], "type":"error",
                        "msg":{"code":6, "msg":"fixture fatal rejection"}})
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            tokio::join!(close_echo(&mut fatal), close_echo(&mut healthy));
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(shard, exchange)
        })
        .await
        .expect("shard kept its surviving replica running after a terminal source failure");
        let error = format!(
            "{:#}",
            result.expect_err("terminal source failure was hidden")
        );
        assert!(error.contains("exchange rejected command 6"), "{error}");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err(),
            "no source may dial after the shard has joined"
        );
        writer.finish().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn codex_exhausted_lifetime_cannot_buy_a_rotation_through_a_new_begin() {
        let mut o = multibook_owner();
        clock_zero_standby_replacement(&mut o);
        tokio::time::advance(o.health.rotation_timeout).await;
        timer_pass(&mut o);
        for _ in 2..=o.health.max_resubscriptions {
            fail_clock_zero_retry(&mut o).await;
        }
        let budget = o.health.max_resubscriptions;
        assert_eq!(o.feeds[1].books["C"].rejected_baselines, budget);
        assert_eq!(restarts(&o, 1), u64::from(budget));
        // An independently observed reconnect is a physical generation change,
        // not a new logical book lifetime or a new semantic-recovery budget.
        let transport = Source {
            replica: 1,
            generation: o.feeds[1].generation + 1,
        };
        begin(&mut o, transport);
        assert!(o.feeds[1].books["C"].frontier.is_none());
        assert!(o.feeds[1].books["C"].probe_quarantined);
        assert_eq!(o.feeds[1].books["C"].rejected_baselines, budget);
        let controls = wire_controls(&o);
        advance_to_rotation_due(&o).await;
        timer_pass(&mut o);
        assert_eq!(
            wire_controls(&o),
            controls,
            "an exhausted lifetime gained a destructive retry by clearing frontier at Begin"
        );
        assert!(o.rotation.is_none() && o.planned.is_none());
        assert_eq!(o.stats.planned_rotations, u64::from(budget));
    }

    // Native health recovery, Owner lifecycle. Admission is the real path; each
    // consumed control is then answered with the FIFO its Source emits. Time is
    // paused throughout, so a deadline moves only when a test advances it.
    const STALE: u64 = 100;
    const FLOOR: u64 = 150;
    const ATTEMPT: Duration = Duration::from_secs(5);
    const TICK: Duration = Duration::from_millis(250);

    /// Steps `run`'s integrity cadence, so a book is judged at every point a
    /// live Owner could judge it. A long `advance` with no tick checks only
    /// event ordering, because a live Owner would have ticked inside that gap.
    async fn ticks(o: &mut Owner, count: u32) {
        for _ in 0..count {
            tokio::time::advance(TICK).await;
            o.tick().unwrap();
        }
    }
    /// Lands exactly on a five-second boundary from 4.75 s of ticks: one
    /// millisecond short survives, the boundary itself is judged.
    async fn survives_then_expires(o: &mut Owner, slot: usize, symbol: &str, before: u32) {
        tokio::time::advance(TICK - Duration::from_millis(1)).await;
        o.tick().unwrap();
        assert_eq!(rejected(o, slot, symbol), before, "judged before 5 s");
        tokio::time::advance(Duration::from_millis(1)).await;
        o.tick().unwrap();
        assert_eq!(rejected(o, slot, symbol), before + 1, "not judged at 5 s");
    }
    fn rejected(o: &Owner, slot: usize, symbol: &str) -> u32 {
        o.feeds[slot].books[symbol].rejected_baselines
    }
    fn refreshes(o: &Owner, slot: usize, symbol: &str) -> u64 {
        o.feeds[slot]
            .control
            .borrow()
            .refresh
            .get(symbol)
            .map_or(0, |r| r.nonce)
    }
    fn replica_readiness(o: &mut Owner) -> Vec<usize> {
        let (progress, row) = watch::channel(ShardStats::default());
        o.report(&[], &progress);
        row.borrow().replica_initialized.clone()
    }
    /// Proposes, admits and releases one health resubscribe on `slot`. Standby
    /// slot 1 drifts below an active peer and needs no cut. Publisher slot 0
    /// gets the planned handoff first, as the deployed path does.
    async fn admit_recovery(o: &mut Owner, symbols: &[&str], slot: usize) -> Source {
        o.health.window = Duration::from_secs(4);
        o.health.warmup = Duration::from_secs(4);
        o.health.cooldown = Duration::from_secs(4);
        if slot == 0 {
            o.health.expected_points = 100;
        } else {
            for symbol in symbols {
                let book = o.feeds[0].books.get_mut(*symbol).unwrap();
                for _ in 0..20 {
                    book.activity.window.record(3);
                }
            }
        }
        tokio::time::advance(Duration::from_secs(4)).await;
        o.activity_tick().unwrap();
        if slot == 0 && o.replicas > 1 {
            cut(o, 1);
        }
        o.release_planned().unwrap();
        assert_eq!(o.stats.health_resubscriptions, symbols.len() as u64);
        for symbol in symbols {
            let book = &o.feeds[slot].books[*symbol];
            assert!(book.activity.recovering);
            assert!(book.recovery.is_some_and(|(_, started)| !started));
        }
        Source {
            replica: slot,
            generation: o.feeds[slot].generation,
        }
    }
    /// What a Source emits after consuming the admitted controls, through the
    /// delete and add acknowledgements that precede its snapshot request.
    fn source_started_recovery(o: &mut Owner, target: Source, symbols: &[&str]) {
        let names: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();
        let requests = {
            let control = o.feeds[target.replica].control.borrow();
            names
                .iter()
                .map(|s| (s.clone(), control.resubscribe[s]))
                .collect()
        };
        o.input(Input::Invalid {
            source: target,
            symbols: Some(names.clone()),
            reason: "low activity resubscribe".into(),
        })
        .unwrap();
        o.input(Input::RecoveryStarted {
            source: target,
            requests,
        })
        .unwrap();
        let mut wire = o.feeds[target.replica].wire.clone();
        for symbol in &names {
            wire.remove(symbol);
        }
        o.input(Input::Membership {
            roster: None,
            source: target,
            wire: wire.clone(),
            at: Utc::now(),
        })
        .unwrap();
        wire.extend(names);
        o.input(Input::Membership {
            roster: None,
            source: target,
            wire,
            at: Utc::now(),
        })
        .unwrap();
    }
    /// A consumed refresh is one FIFO invalidation, then its snapshot answer.
    fn source_answers_refresh(o: &mut Owner, target: Source, symbol: &str, clock: u64) {
        o.input(Input::Invalid {
            source: target,
            symbols: Some(vec![symbol.into()]),
            reason: "requested snapshot refresh".into(),
        })
        .unwrap();
        book_snapshot(o, target, symbol, clock, 10);
    }
    /// One answer per accepted attempt, each given its full deadline, until the
    /// rejection that spends the budget. Returns that final tick's verdict.
    async fn spend_recovery_budget(
        o: &mut Owner,
        target: Source,
        symbol: &str,
        clock: u64,
    ) -> anyhow::Result<()> {
        book_snapshot(o, target, symbol, clock, 10);
        tokio::time::advance(ATTEMPT).await;
        o.tick()?;
        for _ in 0..o.health.max_resubscriptions {
            source_answers_refresh(o, target, symbol, clock);
            tokio::time::advance(ATTEMPT).await;
            o.tick()?;
        }
        Ok(())
    }

    /// Live shape, first boundary. The answer that revives the book owns its
    /// own deadline; it cannot inherit the expired acquisition, and snapshots
    /// onto the still-valid book cannot buy another. On the deployed runtime
    /// this fails at the first answer: the rejection needs no time to pass.
    #[tokio::test(start_paused = true)]
    async fn each_accepted_refresh_owns_one_deadline_that_repeats_cannot_extend() {
        let mut o = multibook_owner();
        book_delta(&mut o, source(0), "A", FLOOR);
        let target = admit_recovery(&mut o, &["A"], 1).await;
        source_started_recovery(&mut o, target, &["A"]);
        let floor = o.selected["A"].floor.unwrap();
        book_snapshot(&mut o, target, "A", STALE, 10);
        assert!(!o.ready(1, "A"));
        for _ in 0..4 {
            tokio::time::advance(Duration::from_secs(1)).await;
            book_snapshot(&mut o, target, "A", STALE, 10);
            o.tick().unwrap();
            assert_eq!(rejected(&o, 1, "A"), 0, "attempt judged early");
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        o.tick().unwrap();
        assert_eq!(rejected(&o, 1, "A"), 1, "repeats moved the first deadline");
        assert_eq!(refreshes(&o, 1, "A"), 1);

        // The publisher keeps moving; the recovery goal stays frozen.
        book_delta(&mut o, source(0), "A", FLOOR + 50);
        source_answers_refresh(&mut o, target, "A", STALE);
        o.tick().unwrap();
        assert_eq!(
            rejected(&o, 1, "A"),
            1,
            "an accepted refresh inherited the expired acquisition timer"
        );
        assert_eq!(o.feeds[1].books["A"].catchup.unwrap().0, floor);
        for _ in 0..2 {
            tokio::time::advance(Duration::from_secs(2)).await;
            book_snapshot(&mut o, target, "A", STALE, 10);
            o.tick().unwrap();
            assert_eq!(rejected(&o, 1, "A"), 1, "second attempt judged early");
        }
        tokio::time::advance(Duration::from_secs(1) - Duration::from_millis(1)).await;
        o.tick().unwrap();
        assert_eq!(rejected(&o, 1, "A"), 1, "second deadline moved earlier");
        tokio::time::advance(Duration::from_millis(1)).await;
        o.tick().unwrap();
        assert_eq!(rejected(&o, 1, "A"), 2, "second deadline was extended");
        assert_eq!(refreshes(&o, 1, "A"), 2);
        assert_eq!(o.selected["A"].active, Some(source(0)));
        assert_eq!((o.stats.hard_resets, o.stats.continuity_gaps), (0, 0));
    }

    /// Live shape, final boundary. Each attempt here gets its full deadline, so
    /// on the deployed runtime this fails only at rejection four: the fatal
    /// exit is independent of the timer reuse.
    #[tokio::test(start_paused = true)]
    async fn exhausted_optional_recovery_ends_honestly_behind_a_healthy_publisher() {
        for slot in [1, 0] {
            let mut o = owner();
            start(&mut o, 0, STALE, 10);
            start(&mut o, 1, STALE, 10);
            if slot == 1 {
                book_delta(&mut o, source(0), "A", FLOOR);
            }
            let target = admit_recovery(&mut o, &["A"], slot).await;
            source_started_recovery(&mut o, target, &["A"]);
            // A handed-off publisher sat at the floor; its stale replacement
            // restates an older own clock instead of regressing the frontier.
            let clock = if slot == 1 { STALE } else { 90 };
            if slot == 0 {
                o.feeds[0].books.get_mut("A").unwrap().frontier = Some(Frontier::snapshot(90));
            }
            let publisher = source(1 - slot);
            let floor = o.selected["A"].floor;
            let budget = o.health.max_resubscriptions;
            let request = o.feeds[slot].control.borrow().resubscribe["A"];

            book_snapshot(&mut o, target, "A", clock, 10);
            tokio::time::advance(ATTEMPT).await;
            o.tick().unwrap();
            for attempt in 2..=budget {
                source_answers_refresh(&mut o, target, "A", clock);
                tokio::time::advance(ATTEMPT).await;
                o.tick().unwrap();
                assert_eq!(rejected(&o, slot, "A"), attempt);
                assert_eq!(refreshes(&o, slot, "A"), u64::from(attempt));
                assert!(o.feeds[slot].books["A"].recovery.is_some());
                assert_eq!(pending_maintenance(&mut o), 1, "recovery still open");
            }
            source_answers_refresh(&mut o, target, "A", clock);
            let controls = wire_controls(&o);
            tokio::time::advance(ATTEMPT).await;
            o.tick().unwrap();

            let book = &o.feeds[slot].books["A"];
            assert_eq!(book.rejected_baselines, budget + 1);
            assert!(book.probe_quarantined && !book.value.is_valid());
            assert!(book.recovery.is_none() && !book.activity.recovering);
            assert!(book.catchup.is_none());
            assert!(!o.waiting_catchup.contains(&(slot, "A".to_owned())));
            assert!(!o.ready(slot, "A"));
            assert!(o.terminally_quarantined(slot, "A"));

            // The publisher, its floor and every counter stay honest.
            assert_eq!(o.selected["A"].active, Some(publisher));
            assert_eq!(o.selected["A"].floor, floor);
            assert!(o.ready(publisher.replica, "A"));
            assert_eq!((o.stats.hard_resets, o.stats.continuity_gaps), (0, 0));
            assert_eq!(o.stats.clock_regressions, u64::from(budget) + 1);

            // Only the logical operation ended: the Source keeps what it owns.
            assert_eq!(wire_controls(&o), controls, "ending it reached the wire");
            assert_eq!(o.feeds[slot].control.borrow().resubscribe["A"], request);
            assert!(o.feeds[slot].wire.contains("A"));
            assert_eq!(o.feeds[slot].generation, target.generation);

            // No completion credit and no route back to a further proposal.
            assert_eq!(pending_maintenance(&mut o), 0, "maintenance stayed wedged");
            assert_eq!(o.stats.completed_rotations, 0);
            assert_eq!(o.stats.completed_health_renewals, 0);
            let mut readiness = vec![1, 1];
            readiness[slot] = 0;
            assert_eq!(replica_readiness(&mut o), readiness);
            tokio::time::advance(o.health.cooldown * 2).await;
            o.activity_tick().unwrap();
            o.release_planned().unwrap();
            assert_eq!(o.stats.health_resubscriptions, 1);

            // A straggling stale answer is judged, but buys no request.
            book_snapshot(&mut o, target, "A", clock, 10);
            tokio::time::advance(ATTEMPT).await;
            o.tick().unwrap();
            assert_eq!(rejected(&o, slot, "A"), budget + 2);
            assert_eq!(wire_controls(&o), controls, "a straggler re-requested");
            assert_eq!(o.selected["A"].active, Some(publisher));

            // Rotation defers through the existing exhausted-budget path.
            assert!(!o.start_rotation(slot).unwrap());
            assert_eq!(o.stats.deferred_rotations, 1);
            assert_eq!(wire_controls(&o), controls);
        }
    }

    /// A baseline landing between plan and release arms a clock. Admission ends
    /// that epoch, so the book cannot expire while Source has yet to consume the
    /// control. Data ahead of the start marker cannot touch the clock either,
    /// and the real baseline owns a whole window.
    #[tokio::test(start_paused = true)]
    async fn an_admitted_resubscribe_does_not_inherit_a_deadline_armed_before_release() {
        let mut o = multibook_owner();
        book_delta(&mut o, source(0), "A", FLOOR);
        o.health.window = Duration::from_secs(4);
        o.health.warmup = Duration::from_secs(4);
        o.health.cooldown = Duration::from_secs(4);
        let record = |o: &mut Owner, epoch| {
            let book = o.feeds[0].books.get_mut("A").unwrap();
            for _ in 0..20 {
                book.activity.window.record(epoch);
            }
        };
        record(&mut o, 3);
        tokio::time::advance(Duration::from_secs(4)).await;
        o.activity_tick().unwrap();
        assert!(o.planned.is_some());
        book_snapshot(&mut o, source(1), "A", STALE, 10);
        let armed = o.feeds[1].books["A"].catchup.unwrap();
        // Live ticks run up to just inside that baseline's window.
        ticks(&mut o, 19).await;
        assert_eq!(rejected(&o, 1, "A"), 0);
        // Still a live deficit at release, so the plan is admitted.
        record(&mut o, 7);
        o.release_planned().unwrap();
        assert_eq!(o.stats.health_resubscriptions, 1);
        assert!(!o.feeds[1].books["A"].value.is_valid());

        // The old clock passes its deadline while the control is still queued.
        ticks(&mut o, 2).await;
        assert_eq!(rejected(&o, 1, "A"), 0, "judged an admitted, invalid book");

        // Ahead of the matching start marker a baseline is pre-fence data.
        let stale = o.stats.stale_events;
        book_snapshot(&mut o, source(1), "A", STALE, 10);
        assert_eq!(o.stats.stale_events, stale + 1);
        assert_eq!(o.feeds[1].books["A"].catchup, Some(armed));

        source_started_recovery(&mut o, source(1), &["A"]);
        ticks(&mut o, 1).await;
        book_snapshot(&mut o, source(1), "A", STALE, 10);
        o.tick().unwrap();
        assert_eq!(
            rejected(&o, 1, "A"),
            0,
            "admission inherited an earlier age"
        );
        assert_eq!(o.feeds[1].books["A"].catchup.unwrap().0, armed.0);
        ticks(&mut o, 19).await;
        survives_then_expires(&mut o, 1, "A", 0).await;
    }

    #[tokio::test(start_paused = true)]
    async fn regressing_answers_spend_the_same_budget_without_waiting_for_a_deadline() {
        let mut o = multibook_owner();
        book_delta(&mut o, source(0), "A", FLOOR);
        let target = admit_recovery(&mut o, &["A"], 1).await;
        source_started_recovery(&mut o, target, &["A"]);
        let budget = o.health.max_resubscriptions;
        for attempt in 1..=budget {
            book_snapshot(&mut o, target, "A", 90, 10);
            assert_eq!(rejected(&o, 1, "A"), attempt);
            assert_eq!(refreshes(&o, 1, "A"), u64::from(attempt));
            o.input(Input::Invalid {
                source: target,
                symbols: Some(vec!["A".into()]),
                reason: "requested snapshot refresh".into(),
            })
            .unwrap();
        }
        book_snapshot(&mut o, target, "A", 90, 10);
        assert_eq!(rejected(&o, 1, "A"), budget + 1);
        assert_eq!(refreshes(&o, 1, "A"), u64::from(budget));
        assert!(o.terminally_quarantined(1, "A"));
        assert_eq!(o.selected["A"].active, Some(source(0)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_covering_late_answer_heals_an_exhausted_recovery_without_credit() {
        let mut o = multibook_owner();
        book_delta(&mut o, source(0), "A", FLOOR);
        let target = admit_recovery(&mut o, &["A"], 1).await;
        source_started_recovery(&mut o, target, &["A"]);
        spend_recovery_budget(&mut o, target, "A", STALE)
            .await
            .unwrap();
        assert!(o.terminally_quarantined(1, "A"));
        let controls = wire_controls(&o);

        book_snapshot(&mut o, target, "A", FLOOR + 1, 10);
        assert!(o.ready(1, "A"));
        let book = &o.feeds[1].books["A"];
        assert_eq!(book.rejected_baselines, 0);
        assert!(!book.probe_quarantined && book.recovery.is_none());
        assert!(!book.activity.recovering);
        assert_eq!(wire_controls(&o), controls);
        assert_eq!(o.selected["A"].active, Some(source(0)));
        assert_eq!(o.stats.completed_health_renewals, 0);
        assert_eq!((o.stats.hard_resets, o.stats.continuity_gaps), (0, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn an_ordered_tail_completes_recovery_only_by_reaching_the_frozen_floor_in_time() {
        for reaches in [true, false] {
            let mut o = multibook_owner();
            book_delta(&mut o, source(0), "A", FLOOR);
            let target = admit_recovery(&mut o, &["A"], 1).await;
            source_started_recovery(&mut o, target, &["A"]);
            let floor = o.selected["A"].floor.unwrap();
            book_snapshot(&mut o, target, "A", STALE, 10);
            for second in 1..5 {
                tokio::time::advance(Duration::from_secs(1)).await;
                book_delta(&mut o, target, "A", STALE + second);
                o.tick().unwrap();
                assert_eq!(rejected(&o, 1, "A"), 0);
            }
            // The goal is frozen: a publisher moving on cannot chase the tail.
            book_delta(&mut o, source(0), "A", FLOOR + 50);
            if reaches {
                book_delta(&mut o, target, "A", FLOOR + 1);
                assert!(o.ready(1, "A"));
                let book = &o.feeds[1].books["A"];
                assert_eq!(book.rejected_baselines, 0);
                assert!(book.recovery.is_none() && !book.activity.recovering);
                assert_eq!(pending_maintenance(&mut o), 0);
                tokio::time::advance(ATTEMPT).await;
                o.tick().unwrap();
                assert_eq!(rejected(&o, 1, "A"), 0);
            } else {
                // Activity is not evidence: progress short of the floor cannot
                // renew the deadline it is running against.
                tokio::time::advance(Duration::from_secs(1)).await;
                o.tick().unwrap();
                assert_eq!(rejected(&o, 1, "A"), 1);
                assert_eq!(refreshes(&o, 1, "A"), 1);
                assert_eq!(o.feeds[1].books["A"].catchup.unwrap().0, floor);
            }
        }
    }

    /// Source owns the no-answer bound through its ACK and readiness timers;
    /// the Owner must neither judge missing evidence nor age the next answer.
    #[tokio::test(start_paused = true)]
    async fn an_unanswered_refresh_judges_nothing_and_its_late_answer_gets_one_deadline() {
        let mut o = multibook_owner();
        book_delta(&mut o, source(0), "A", FLOOR);
        let target = admit_recovery(&mut o, &["A"], 1).await;
        source_started_recovery(&mut o, target, &["A"]);
        book_snapshot(&mut o, target, "A", STALE, 10);
        tokio::time::advance(ATTEMPT).await;
        o.tick().unwrap();
        assert_eq!(rejected(&o, 1, "A"), 1);
        for _ in 0..6 {
            tokio::time::advance(Duration::from_secs(10)).await;
            o.tick().unwrap();
            assert_eq!(rejected(&o, 1, "A"), 1, "judged an absent answer");
        }
        assert_eq!(refreshes(&o, 1, "A"), 1);
        assert!(o.feeds[1].books["A"].recovery.is_some());
        assert_eq!(
            pending_maintenance(&mut o),
            1,
            "the operation is still open"
        );

        source_answers_refresh(&mut o, target, "A", STALE);
        o.tick().unwrap();
        assert_eq!(rejected(&o, 1, "A"), 1, "late answer inherited dead time");
        tokio::time::advance(ATTEMPT - Duration::from_millis(1)).await;
        o.tick().unwrap();
        assert_eq!(rejected(&o, 1, "A"), 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        o.tick().unwrap();
        assert_eq!(rejected(&o, 1, "A"), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn markers_of_another_generation_or_request_cannot_start_or_judge_recovery() {
        let mut o = multibook_owner();
        book_delta(&mut o, source(0), "A", FLOOR);
        let target = admit_recovery(&mut o, &["A"], 1).await;
        let request = o.feeds[1].control.borrow().resubscribe["A"];
        let started = o.stats.stale_events;

        // Ahead of the matching start marker a baseline is pre-fence data.
        book_snapshot(&mut o, target, "A", STALE, 10);
        assert!(o.feeds[1].books["A"].catchup.is_none());
        assert_eq!(rejected(&o, 1, "A"), 0);
        for wrong in [
            crate::relay::Resubscribe {
                nonce: request.nonce + 1,
                ..request
            },
            crate::relay::Resubscribe {
                generation: request.generation + 1,
                ..request
            },
            crate::relay::Resubscribe {
                incarnation: request.incarnation + 1,
                ..request
            },
        ] {
            o.input(Input::RecoveryStarted {
                source: target,
                requests: BTreeMap::from([("A".into(), wrong)]),
            })
            .unwrap();
            assert_eq!(o.feeds[1].books["A"].recovery, Some((request, false)));
        }
        o.input(Input::RecoveryStarted {
            source: Source {
                generation: target.generation + 1,
                ..target
            },
            requests: BTreeMap::from([("A".into(), request)]),
        })
        .unwrap();
        assert_eq!(o.feeds[1].books["A"].recovery, Some((request, false)));
        assert_eq!(o.stats.stale_events, started + 5);

        source_started_recovery(&mut o, target, &["A"]);
        book_snapshot(&mut o, target, "A", STALE, 10);
        ticks(&mut o, 18).await;

        // A replaced transport ends the epoch inside its window.
        let fresh = standby_replacement(&mut o, &["A", "B", "C"]);
        let stale = o.stats.stale_events;
        book_snapshot(&mut o, target, "A", 1000, 10);
        assert_eq!(o.stats.stale_events, stale + 1, "old generation was heard");
        assert!(o.feeds[1].books["A"].frontier.is_none());

        // The old clock passes its deadline with no book to judge.
        ticks(&mut o, 3).await;
        assert_eq!(rejected(&o, 1, "A"), 0);
        book_snapshot(&mut o, fresh, "A", STALE, 10);
        o.tick().unwrap();
        assert_eq!(rejected(&o, 1, "A"), 0, "inherited an expired acquisition");
        assert!(o.feeds[1].books["A"].probe.is_some());
        tokio::time::advance(o.health.rotation_timeout - Duration::from_millis(1)).await;
        o.tick().unwrap();
        assert_eq!(
            rejected(&o, 1, "A"),
            0,
            "the new generation's window started before its first baseline"
        );
        tokio::time::advance(Duration::from_millis(1)).await;
        o.tick().unwrap();
        assert_eq!(rejected(&o, 1, "A"), 1);
        assert!(o.feeds[1].books["A"].probe_quarantined);
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_incarnation_inherits_no_attempt_budget_or_late_marker() {
        let mut o = multibook_owner();
        book_delta(&mut o, source(0), "A", FLOOR);
        let target = admit_recovery(&mut o, &["A"], 1).await;
        let request = o.feeds[1].control.borrow().resubscribe["A"];
        source_started_recovery(&mut o, target, &["A"]);
        book_snapshot(&mut o, target, "A", STALE, 10);
        tokio::time::advance(ATTEMPT).await;
        o.tick().unwrap();
        assert_eq!(rejected(&o, 1, "A"), 1);

        reassign(&mut o, &[("A", 2), ("B", 1), ("C", 1)]);
        assert!(o.waiting_catchup.is_empty());
        let book = &o.feeds[1].books["A"];
        assert_eq!(book.rejected_baselines, 0);
        assert!(book.catchup.is_none() && book.recovery.is_none());
        assert!(o.feeds[1].control.borrow().refresh.is_empty());

        let stale = o.stats.stale_events;
        o.input(Input::RecoveryStarted {
            source: target,
            requests: BTreeMap::from([("A".into(), request)]),
        })
        .unwrap();
        o.input(Input::Data {
            source: target,
            incarnation: 1,
            received_tick: Instant::now(),
            event: BookEvent::Snapshot(Snapshot {
                symbol: "A".into(),
                received_at: Utc::now(),
                exchange_at: Utc::now(),
                clock: Some(1000),
                bids: vec![],
                asks: vec![],
            }),
        })
        .unwrap();
        assert_eq!(o.stats.stale_events, stale + 2);
        assert!(o.feeds[1].books["A"].frontier.is_none());
        assert!(o.feeds[1].books["A"].recovery.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn publisher_loss_keeps_its_five_second_bound_through_every_attempt_state() {
        // Armed attempt: the attempt's own expiry never decides without a publisher.
        let mut o = multibook_owner();
        book_delta(&mut o, source(0), "A", FLOOR);
        let target = admit_recovery(&mut o, &["A"], 1).await;
        source_started_recovery(&mut o, target, &["A"]);
        book_snapshot(&mut o, target, "A", STALE, 10);
        tokio::time::advance(Duration::from_secs(1)).await;
        fault(&mut o, 0);
        tokio::time::advance(Duration::from_secs(4)).await;
        o.tick().unwrap();
        assert_eq!(rejected(&o, 1, "A"), 0, "attempt timer ran the loss path");
        assert_eq!(o.stats.hard_resets, 0, "the five-second bound shrank");
        tokio::time::advance(Duration::from_secs(1) - Duration::from_millis(1)).await;
        o.tick().unwrap();
        assert_eq!(o.stats.hard_resets, 0, "the five-second bound shrank");
        tokio::time::advance(Duration::from_millis(1)).await;
        o.tick().unwrap();
        assert_eq!(o.stats.hard_resets, 1, "the five-second bound grew");
        assert_eq!(o.selected["A"].floor, None);

        // Accepted refresh with no answer yet: a fresh attempt buys no time.
        let mut o = multibook_owner();
        book_delta(&mut o, source(0), "A", FLOOR);
        let target = admit_recovery(&mut o, &["A"], 1).await;
        source_started_recovery(&mut o, target, &["A"]);
        book_snapshot(&mut o, target, "A", STALE, 10);
        tokio::time::advance(ATTEMPT).await;
        o.tick().unwrap();
        assert_eq!(rejected(&o, 1, "A"), 1);
        fault(&mut o, 0);
        tokio::time::advance(Duration::from_secs(1)).await;
        source_answers_refresh(&mut o, target, "A", STALE);
        tokio::time::advance(Duration::from_secs(4) - Duration::from_millis(1)).await;
        o.tick().unwrap();
        assert_eq!(o.stats.hard_resets, 0);
        tokio::time::advance(Duration::from_millis(1)).await;
        o.tick().unwrap();
        assert_eq!(
            o.stats.hard_resets, 1,
            "a new attempt extended the loss bound"
        );

        // Terminal quarantine: the book is never promoted, and the bound holds.
        let mut o = multibook_owner();
        book_delta(&mut o, source(0), "A", FLOOR);
        let target = admit_recovery(&mut o, &["A"], 1).await;
        source_started_recovery(&mut o, target, &["A"]);
        spend_recovery_budget(&mut o, target, "A", STALE)
            .await
            .unwrap();
        assert!(o.terminally_quarantined(1, "A"));
        fault(&mut o, 0);
        assert_eq!(o.selected["A"].active, None);
        assert_eq!(
            o.selected["A"].candidate, None,
            "promoted a quarantined book"
        );
        let publisher = Source {
            replica: 0,
            generation: 2,
        };
        begin(&mut o, publisher);
        o.input(Input::Membership {
            roster: None,
            source: publisher,
            wire: BTreeSet::from(["A".into(), "B".into(), "C".into()]),
            at: Utc::now(),
        })
        .unwrap();
        book_snapshot(&mut o, publisher, "A", STALE, 10);
        assert_eq!(o.selected["A"].active, None, "uncovering book published");
        tokio::time::advance(ATTEMPT).await;
        timer_pass(&mut o);
        assert_eq!(o.stats.hard_resets, 1);
        assert_eq!(o.selected["A"].floor, None);
    }

    #[tokio::test(start_paused = true)]
    async fn one_books_exhausted_recovery_cannot_touch_a_sibling_or_a_bystander() {
        let mut o = multibook_owner();
        book_delta(&mut o, source(0), "A", FLOOR);
        book_delta(&mut o, source(0), "B", FLOOR);
        let target = admit_recovery(&mut o, &["A", "B"], 1).await;
        source_started_recovery(&mut o, target, &["A", "B"]);
        book_snapshot(&mut o, target, "A", STALE, 10);
        book_snapshot(&mut o, target, "B", STALE, 10);
        tokio::time::advance(ATTEMPT).await;
        o.tick().unwrap();
        assert_eq!((rejected(&o, 1, "A"), rejected(&o, 1, "B")), (1, 1));

        source_answers_refresh(&mut o, target, "B", FLOOR + 1);
        assert!(o.ready(1, "B"));
        assert_eq!(rejected(&o, 1, "B"), 0);
        assert!(o.feeds[1].books["B"].recovery.is_none());
        let sibling_refreshes = refreshes(&o, 1, "B");

        let budget = o.health.max_resubscriptions;
        for attempt in 2..=budget + 1 {
            assert_eq!(pending_maintenance(&mut o), 1, "A is still recovering");
            source_answers_refresh(&mut o, target, "A", STALE);
            tokio::time::advance(ATTEMPT).await;
            o.tick().unwrap();
            assert_eq!(rejected(&o, 1, "A"), attempt);
            assert_eq!(rejected(&o, 1, "B"), 0, "budget leaked to a sibling");
        }
        assert!(o.terminally_quarantined(1, "A"));
        assert_eq!(pending_maintenance(&mut o), 0);
        assert!(o.waiting_catchup.is_empty());
        assert_eq!(refreshes(&o, 1, "B"), sibling_refreshes);
        for symbol in ["B", "C"] {
            assert!(o.ready(1, symbol), "{symbol} lost readiness");
            assert!(o.ready(0, symbol), "{symbol} lost its publisher");
            assert_eq!(o.selected[symbol].active, Some(source(0)));
        }
        assert_eq!(o.selected["C"].floor, Some(Frontier::snapshot(STALE)));
        assert_eq!(rejected(&o, 1, "C"), 0);
        assert_eq!(replica_readiness(&mut o), vec![3, 2]);
    }

    // Negative controls: the exception covers only a started optional recovery
    // behind a different, ready publisher.
    #[tokio::test(start_paused = true)]
    async fn corrupt_books_stay_fatal_even_during_optional_recovery() {
        let mut o = multibook_owner();
        book_delta(&mut o, source(0), "A", FLOOR);
        let target = admit_recovery(&mut o, &["A"], 1).await;
        source_started_recovery(&mut o, target, &["A"]);
        let corrupt = |o: &mut Owner| {
            o.input(Input::Data {
                source: target,
                incarnation: 1,
                received_tick: Instant::now(),
                event: BookEvent::Snapshot(Snapshot {
                    symbol: "A".into(),
                    received_at: Utc::now(),
                    exchange_at: Utc::now(),
                    clock: Some(STALE),
                    bids: vec![Level {
                        price: -1,
                        quantity: 1,
                    }],
                    asks: vec![],
                }),
            })
        };
        let budget = o.health.max_resubscriptions;
        for attempt in 1..=budget {
            corrupt(&mut o).unwrap();
            assert_eq!(rejected(&o, 1, "A"), attempt);
        }
        let error = corrupt(&mut o).err().expect("corruption must stay fatal");
        assert!(
            error
                .to_string()
                .contains("semantic book recovery exhausted")
        );
        assert!(!o.feeds[1].books["A"].probe_quarantined);
        assert_eq!(o.selected["A"].active, Some(source(0)));
    }

    #[tokio::test(start_paused = true)]
    async fn without_a_healthy_publisher_recovery_keeps_the_fatal_budget() {
        let mut o = multibook_owner();
        book_delta(&mut o, source(0), "A", FLOOR);
        let target = admit_recovery(&mut o, &["A"], 1).await;
        source_started_recovery(&mut o, target, &["A"]);
        fault(&mut o, 0);
        assert_eq!(o.selected["A"].active, None);
        let budget = o.health.max_resubscriptions;
        for attempt in 1..=budget {
            book_snapshot(&mut o, target, "A", 90, 10);
            assert_eq!(rejected(&o, 1, "A"), attempt);
        }
        let error = o
            .input(Input::Data {
                source: target,
                incarnation: 1,
                received_tick: Instant::now(),
                event: BookEvent::Snapshot(Snapshot {
                    symbol: "A".into(),
                    received_at: Utc::now(),
                    exchange_at: Utc::now(),
                    clock: Some(90),
                    bids: vec![],
                    asks: vec![],
                }),
            })
            .err()
            .expect("no publisher remains to retain");
        assert!(error.to_string().contains("semantic recovery exhausted"));
        assert!(!o.feeds[1].books["A"].probe_quarantined);
    }

    #[tokio::test(start_paused = true)]
    async fn a_sole_replica_recovery_keeps_its_reset_path_with_no_peer_to_retain() {
        let mut o = owner();
        o.feeds.truncate(1);
        o.replicas = 1;
        start(&mut o, 0, STALE, 10);
        let target = admit_recovery(&mut o, &["A"], 0).await;
        source_started_recovery(&mut o, target, &["A"]);
        book_snapshot(&mut o, target, "A", 90, 10);
        assert_eq!(rejected(&o, 0, "A"), 1);
        assert_eq!(o.stats.hard_resets, 1);
        assert_eq!(o.selected["A"].floor, None);
        assert!(!o.feeds[0].books["A"].probe_quarantined);
    }

    /// Unstarted work cannot reach this through Data, which is fenced until the
    /// start marker, so the Owner decision is driven directly.
    #[tokio::test(start_paused = true)]
    async fn an_unstarted_recovery_keeps_the_fatal_budget_behind_a_healthy_publisher() {
        let mut o = multibook_owner();
        book_delta(&mut o, source(0), "A", FLOOR);
        let target = admit_recovery(&mut o, &["A"], 1).await;
        let budget = o.health.max_resubscriptions;
        o.feeds[1].books.get_mut("A").unwrap().rejected_baselines = budget;
        let error = o
            .reject_clock("A", target, None, None, "fence check")
            .expect_err("unstarted work is not spendable optional recovery");
        assert!(error.to_string().contains("semantic recovery exhausted"));
        let book = &o.feeds[1].books["A"];
        assert!(!book.probe_quarantined && book.activity.recovering);
        assert!(book.recovery.is_some_and(|(_, started)| !started));
        assert_eq!(o.selected["A"].active, Some(source(0)));
    }

    // Validity-epoch boundaries, from the actual Source order: Invalid(low
    // activity), then RecoveryStarted, then the delete/add acknowledgements.
    async fn codex_enter_real_health_catchup() -> Owner {
        let mut o = owner();
        o.health.window = Duration::from_secs(4);
        o.health.warmup = Duration::from_secs(4);
        o.health.cooldown = Duration::from_secs(4);
        start(&mut o, 0, 100, 20);
        start(&mut o, 1, 100, 20);
        delta(&mut o, source(0), 200, 1);
        for _ in 0..20 {
            o.feeds[0]
                .books
                .get_mut("A")
                .unwrap()
                .activity
                .window
                .record(0);
        }
        tokio::time::advance(Duration::from_secs(4)).await;
        o.activity_tick().unwrap();
        acknowledge_pause(&mut o);
        o.tick().unwrap();
        o.release_planned().unwrap();
        assert_eq!(o.stats.health_resubscriptions, 1);
        let request = o.feeds[1].control.borrow().resubscribe["A"];
        assert_eq!(o.feeds[1].books["A"].recovery, Some((request, false)));
        o.input(Input::Invalid {
            source: source(1),
            symbols: Some(vec!["A".into()]),
            reason: "low activity resubscribe".into(),
        })
        .unwrap();
        o.input(Input::RecoveryStarted {
            source: source(1),
            requests: BTreeMap::from([("A".into(), request)]),
        })
        .unwrap();
        for wire in [BTreeSet::new(), BTreeSet::from(["A".into()])] {
            o.input(Input::Membership {
                source: source(1),
                roster: None,
                wire,
                at: Utc::now(),
            })
            .unwrap();
        }
        snap(&mut o, source(1), 100, 20);
        assert!(o.feeds[1].books["A"].catchup.is_some());
        assert_eq!(o.selected["A"].active, Some(source(0)));
        o
    }
    fn source_invalidates(o: &mut Owner, replica: usize, reason: &str) {
        o.input(Input::Invalid {
            source: source(replica),
            symbols: Some(vec!["A".into()]),
            reason: reason.into(),
        })
        .unwrap();
    }

    /// The original timing defect: rejection two arrived 250 ms after the first
    /// because the fresh answer inherited the expired acquisition.
    #[tokio::test(start_paused = true)]
    async fn codex_health_refresh_must_not_spend_second_budget_in_250ms() {
        let mut o = codex_enter_real_health_catchup().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        o.tick().unwrap();
        assert_eq!(o.stats.clock_regressions, 1);
        source_invalidates(&mut o, 1, "requested snapshot refresh");
        tokio::time::advance(Duration::from_millis(250)).await;
        snap(&mut o, source(1), 100, 20);
        o.tick().unwrap();
        assert_eq!(
            o.stats.clock_regressions, 1,
            "fresh response immediately inherited expired catchup timer"
        );
    }

    /// Event order only. A live Owner would have ticked inside the six-second
    /// gap, which the faithful-tick trace below does.
    #[tokio::test(start_paused = true)]
    async fn codex_queued_precontrol_snapshot_cannot_start_the_next_refresh_deadline() {
        let mut o = codex_enter_real_health_catchup().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        o.tick().unwrap();
        assert_eq!(o.stats.clock_regressions, 1);
        // Data already in the Source FIFO reaches the Owner after it queued a
        // refresh but before the Source consumes that scoped control.
        tokio::time::advance(Duration::from_millis(250)).await;
        snap(&mut o, source(1), 100, 20);
        tokio::time::advance(Duration::from_secs(6)).await;
        source_invalidates(&mut o, 1, "requested snapshot refresh");
        snap(&mut o, source(1), 100, 20);
        o.tick().unwrap();
        assert_eq!(
            o.stats.clock_regressions, 1,
            "the post-Invalid baseline was judged on the pre-control baseline's clock"
        );
    }

    /// Faithful FIFO counterexample at the live tick cadence. The pre-control
    /// snapshot may start an epoch, but the Source's Invalid ends it, so the
    /// real baseline owns a full window, and repeats on it renew nothing.
    #[tokio::test(start_paused = true)]
    async fn codex_postinvalid_baseline_gets_full_window_with_faithful_ticks() {
        let mut o = codex_enter_real_health_catchup().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        o.tick().unwrap();
        assert_eq!(o.stats.clock_regressions, 1);
        let original_floor = o.feeds[1].books["A"].catchup.unwrap().0;
        tokio::time::advance(Duration::from_millis(250)).await;
        snap(&mut o, source(1), 100, 20); // queued pre-control baseline
        o.tick().unwrap();
        for _ in 0..3 {
            tokio::time::advance(Duration::from_millis(250)).await;
            o.tick().unwrap();
        }
        source_invalidates(&mut o, 1, "requested snapshot refresh");
        for _ in 0..18 {
            tokio::time::advance(Duration::from_millis(250)).await;
            o.tick().unwrap();
            assert_eq!(
                o.stats.clock_regressions, 1,
                "invalid book spent a judgment"
            );
        }
        snap(&mut o, source(1), 100, 20);
        o.tick().unwrap();
        assert_eq!(
            o.stats.clock_regressions, 1,
            "new post-invalid baseline was judged immediately"
        );
        let acquisition = o.feeds[1].books["A"].catchup.unwrap();
        assert_eq!(acquisition.0, original_floor);
        // Repeated snapshots on an already-valid book do not renew anything.
        for _ in 0..19 {
            tokio::time::advance(Duration::from_millis(250)).await;
            snap(&mut o, source(1), 100, 20);
            o.tick().unwrap();
            assert_eq!(o.feeds[1].books["A"].catchup.unwrap(), acquisition);
            assert_eq!(o.stats.clock_regressions, 1);
        }
        tokio::time::advance(Duration::from_millis(249)).await;
        o.tick().unwrap();
        assert_eq!(o.stats.clock_regressions, 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        o.tick().unwrap();
        assert_eq!(o.stats.clock_regressions, 2);
        assert_eq!(o.selected["A"].active, Some(source(0)));
        assert_eq!((o.stats.continuity_gaps, o.stats.hard_resets), (0, 0));
    }

    /// The other side of that bound. If the Source stays slow to consume the
    /// refresh, the pre-control baseline really does get judged, after its own
    /// full window, and the late Invalid still gives the next one a full window.
    #[tokio::test(start_paused = true)]
    async fn a_precontrol_baseline_is_judged_only_after_its_own_full_window() {
        let mut o = codex_enter_real_health_catchup().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        o.tick().unwrap();
        assert_eq!(rejected(&o, 1, "A"), 1);
        ticks(&mut o, 1).await;
        snap(&mut o, source(1), 100, 20);
        o.tick().unwrap();
        ticks(&mut o, 19).await;
        survives_then_expires(&mut o, 1, "A", 1).await;
        assert_eq!(o.feeds[1].control.borrow().refresh["A"].nonce, 2);

        source_invalidates(&mut o, 1, "requested snapshot refresh");
        ticks(&mut o, 2).await;
        assert_eq!(rejected(&o, 1, "A"), 2, "an invalid book spent budget");
        snap(&mut o, source(1), 100, 20);
        o.tick().unwrap();
        ticks(&mut o, 19).await;
        survives_then_expires(&mut o, 1, "A", 2).await;
        assert_eq!(o.selected["A"].active, Some(source(0)));
    }

    /// A Source-originated loss (gap, ACK timeout) is not Owner evidence: it
    /// charges no budget, keeps the frozen goal, and leaves the tuple alone
    /// until a baseline actually revives the book.
    #[tokio::test(start_paused = true)]
    async fn a_source_invalid_ends_the_epoch_without_charging_the_budget() {
        let mut o = codex_enter_real_health_catchup().await;
        let armed = o.feeds[1].books["A"].catchup.unwrap();
        ticks(&mut o, 18).await;
        source_invalidates(&mut o, 1, "sequence gap/repeat");
        ticks(&mut o, 4).await;
        assert_eq!(
            rejected(&o, 1, "A"),
            0,
            "the old clock judged an invalid book"
        );
        assert_eq!(o.feeds[1].books["A"].catchup, Some(armed));

        snap(&mut o, source(1), 100, 20);
        o.tick().unwrap();
        let revived = o.feeds[1].books["A"].catchup.unwrap();
        assert_eq!(revived.0, armed.0, "the frozen goal moved");
        assert_eq!(revived.1, armed.1 + Duration::from_millis(5500));
        assert_eq!(rejected(&o, 1, "A"), 0);
        ticks(&mut o, 19).await;
        survives_then_expires(&mut o, 1, "A", 0).await;
        assert_eq!(o.feeds[1].control.borrow().refresh["A"].nonce, 1);
    }

    /// A replaced connection carries the floor and spent budget, but cannot be
    /// judged by the previous connection's clock.
    #[tokio::test(start_paused = true)]
    async fn a_new_begin_restarts_the_window_and_carries_floor_and_budget() {
        let mut o = codex_enter_real_health_catchup().await;
        let floor = o.feeds[1].books["A"].catchup.unwrap().0;
        tokio::time::advance(Duration::from_secs(5)).await;
        o.tick().unwrap();
        assert_eq!(rejected(&o, 1, "A"), 1);
        source_invalidates(&mut o, 1, "requested snapshot refresh");
        snap(&mut o, source(1), 100, 20);
        ticks(&mut o, 18).await;
        assert_eq!(rejected(&o, 1, "A"), 1);

        let fresh = standby_replacement(&mut o, &["A"]);
        assert_eq!(rejected(&o, 1, "A"), 1, "Begin spent or refunded budget");
        assert_eq!(o.feeds[1].books["A"].catchup.unwrap().0, floor);
        ticks(&mut o, 3).await;
        assert_eq!(rejected(&o, 1, "A"), 1, "the old clock judged no book");

        snap(&mut o, fresh, 100, 20);
        o.tick().unwrap();
        assert_eq!(
            rejected(&o, 1, "A"),
            1,
            "a new generation inherited the old connection's clock"
        );
        assert_eq!(o.feeds[1].books["A"].catchup.unwrap().0, floor);
        ticks(&mut o, 19).await;
        survives_then_expires(&mut o, 1, "A", 1).await;
        assert_eq!(o.selected["A"].active, Some(source(0)));
    }

    /// Identity fences run before the arming site, so data that belongs to
    /// another lifetime cannot revive the epoch clock or the book.
    #[tokio::test(start_paused = true)]
    async fn stale_identity_data_cannot_move_the_epoch_clock() {
        let mut o = codex_enter_real_health_catchup().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        o.tick().unwrap();
        let armed = o.feeds[1].books["A"].catchup.unwrap();
        ticks(&mut o, 1).await;
        let stale = o.stats.stale_events;

        let old = Source {
            replica: 1,
            generation: 0,
        };
        snap(&mut o, old, 300, 20);
        o.input(Input::Data {
            source: source(1),
            incarnation: 99,
            received_tick: Instant::now(),
            event: BookEvent::Snapshot(Snapshot {
                symbol: "A".into(),
                received_at: Utc::now(),
                exchange_at: Utc::now(),
                clock: Some(300),
                bids: vec![],
                asks: vec![],
            }),
        })
        .unwrap();

        assert_eq!(o.stats.stale_events, stale + 2);
        let book = &o.feeds[1].books["A"];
        assert_eq!(book.catchup, Some(armed));
        assert!(!book.value.is_valid());
    }

    /// Readiness answers the frozen question even after a publisher moved on,
    /// and the shared budget is cleared only by that evidence.
    #[tokio::test(start_paused = true)]
    async fn a_revived_answer_covering_the_frozen_floor_is_ready_below_the_current_cut() {
        let mut o = multibook_owner();
        book_delta(&mut o, source(0), "A", FLOOR);
        let target = admit_recovery(&mut o, &["A"], 1).await;
        source_started_recovery(&mut o, target, &["A"]);
        let frozen = o.selected["A"].floor.unwrap();
        book_snapshot(&mut o, target, "A", STALE, 10);
        tokio::time::advance(ATTEMPT).await;
        o.tick().unwrap();
        assert_eq!(rejected(&o, 1, "A"), 1);
        assert_eq!(o.feeds[1].books["A"].catchup.unwrap().0, frozen);

        book_delta(&mut o, source(0), "A", FLOOR + 50);
        let published = o.selected["A"].floor;
        source_answers_refresh(&mut o, target, "A", FLOOR + 1);
        assert!(o.ready(1, "A"));
        let book = &o.feeds[1].books["A"];
        assert_eq!(book.rejected_baselines, 0);
        assert!(book.catchup.is_none() && book.recovery.is_none());
        assert!(o.waiting_catchup.is_empty());
        assert_eq!(o.selected["A"].active, Some(source(0)));
        assert_eq!(o.selected["A"].floor, published);
    }
}
