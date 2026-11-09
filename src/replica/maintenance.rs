//! One shard-owned planned operation at a time. A healthy publisher remains
//! selected until a covering peer fence; destructive controls follow output admission.
use super::{Frontier, Owner, Selection};
use crate::{
    exchange::{MembershipStrategy, OrderingStrategy},
    health,
    relay::{PauseEnd, PauseId, PauseRequest, Resubscribe, Source},
};
use anyhow::{Context, ensure};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use tokio::time::Instant;

/// Pre-pause cover witness for one native rotation target.
///
/// The live-evidence model infers an exchange-wide comparable clock: a
/// quiet market's later snapshot on one replica carries a higher clock with
/// unchanged levels, and the peer-ahead grace then publishes from it. Its peer
/// holds an older complete clock that no ordered tail will ever advance, so an
/// ingress pause of the publisher could only expire (2026-11-08 universe).
/// Only complete-snapshot floors are witnessed. A standby with an unchanged
/// frontier over the replica grace gets one real scoped snapshot probe
/// before any pause is requested. Nothing here lowers a floor or accepts a
/// cut: `Frontier::covers` against the current floor still decides at the cut,
/// and an uncovering fresh baseline keeps the existing catch-up/quarantine.
pub(super) struct CoverageProbe {
    target: Source,
    peer: Source,
    started: Instant,
    witness: BTreeMap<String, Witness>,
    refreshed: BTreeSet<String>,
}
/// One standby book's frozen goal: the publication floor when first observed.
struct Witness {
    incarnation: u64,
    floor: Frontier,
    then: Option<Frontier>,
    since: Instant,
}
enum Coverage {
    Clear,
    Wait,
    Expired,
}

pub(super) struct Rotation {
    health: bool,
    slot: usize,
    generation: u64,
    started: Instant,
    restarting: bool,
    timed_out: bool,
    /// Native readiness debt: the (symbol, incarnation) lifetimes the
    /// destructive restart released, moved from its validated plan. Empty
    /// before release. FreshSession judges its written roster instead.
    roster: BTreeMap<String, u64>,
}
struct RecoveryPlan {
    slot: usize,
    symbol: String,
    incarnation: u64,
    generation: u64,
}
pub(super) struct ScheduledRecovery {
    at: Instant,
    plans: Vec<RecoveryPlan>,
}
enum Action {
    Rotate,
    Resubscribe(Vec<RecoveryPlan>),
}
pub(super) struct Planned {
    target: Source,
    symbols: std::collections::BTreeMap<String, u64>,
    action: Action,
    started: Instant,
    retry_at: Instant,
    pause: Option<PauseId>,
    fence: Option<(Source, u64)>,
}
#[derive(Clone, Copy)]
pub(super) struct PauseReservation {
    source: Source,
    id: PauseId,
    acquire_until: Instant,
    until: Option<Instant>,
    resuming: bool,
}
impl PauseReservation {
    pub(super) fn started(&self) -> bool {
        self.until.is_some()
    }
}
const PAUSE_LIMIT: Duration = Duration::from_secs(5);
impl Owner {
    fn requires_survivor(&self, state: &Selection) -> bool {
        // An explicit native continuity reset fences a new, unpublished epoch.
        // A missing clock on SnapshotOnly says nothing about publication: that
        // profile has no clock even after publishing a healthy lifetime.
        state.ever
            && !(self.contract.ordering == crate::exchange::OrderingStrategy::ComparableClock
                && state.recovery
                && state.floor.is_none()
                && state.active.is_none())
    }
    pub(super) fn replacement_unavailable(&mut self, slot: usize) {
        if self
            .rotation
            .as_ref()
            .is_some_and(|r| r.slot == slot && r.restarting)
        {
            self.rotation = None;
            self.rotation_due = Some(Instant::now() + self.health.cooldown);
        }
    }
    fn clear_pause_control(control: &mut crate::relay::Control, id: PauseId) {
        if control
            .pause
            .is_some_and(|p| p.generation == id.generation && p.nonce == id.nonce)
        {
            control.pause = None;
        }
        if control.pause_release.is_none_or(|old| {
            old.generation < id.generation
                || old.generation == id.generation && old.nonce <= id.nonce
        }) {
            control.pause_release = Some(id);
        }
    }
    fn cancel_pause(&mut self, slot: usize, id: PauseId) {
        if let Some(pause) = &mut self.feeds[slot].pause
            && pause.id == id
        {
            pause.resuming = true;
            self.feeds[slot]
                .control
                .send_modify(|c| Self::clear_pause_control(c, id));
        }
    }
    pub(super) fn cancel_pauses(&mut self) {
        for slot in 0..self.replicas {
            if let Some(p) = self.feeds[slot].pause {
                self.cancel_pause(slot, p.id);
            }
        }
    }
    pub(super) fn retire_pause(&mut self, source: Source) {
        if self.feeds[source.replica]
            .pause
            .is_some_and(|p| p.source == source)
        {
            let p = self.feeds[source.replica].pause.take().unwrap();
            self.feeds[source.replica]
                .control
                .send_modify(|c| Self::clear_pause_control(c, p.id));
        }
    }
    pub(super) fn cancel_planned(&mut self) {
        if let Some(p) = self.planned.take() {
            if let Some(id) = p.pause {
                self.cancel_pause(p.target.replica, id);
            }
            if let Some((source, request)) = p.fence {
                let feed = &mut self.feeds[source.replica];
                if feed
                    .pending_fence
                    .is_some_and(|(s, id, _)| s == source && id == request)
                {
                    feed.pending_fence = None;
                }
                for (symbol, incarnation) in &p.symbols {
                    if let Some(state) = self.selected.get_mut(symbol)
                        && state.incarnation == *incarnation
                    {
                        if state.candidate == Some(source.replica) {
                            state.candidate = None;
                        }
                        // A planned block while a publisher still serves is
                        // not time spent without a publisher. Do not carry its
                        // timer into a later genuine failover, or reset a real
                        // failover timer that already owns a publisher loss.
                        if state.active.is_some() {
                            state.behind_since = None;
                        }
                    }
                }
            }
            if matches!(p.action, Action::Rotate)
                && self.rotation.as_ref().is_some_and(|r| !r.restarting)
            {
                self.rotation = None;
            }
        }
    }
    pub(super) fn pause_started(
        &mut self,
        source: Source,
        nonce: u64,
        until: Instant,
    ) -> anyhow::Result<()> {
        ensure!(
            self.contract.membership == crate::exchange::MembershipStrategy::Acknowledged,
            "native pause marker on fresh-session membership"
        );
        let id = PauseId {
            generation: source.generation,
            nonce,
        };
        let Some(reservation) = &mut self.feeds[source.replica].pause else {
            self.stats.stale_events += 1;
            return Ok(());
        };
        if reservation.source != source || reservation.id != id {
            self.stats.stale_events += 1;
            return Ok(());
        }
        ensure!(
            reservation.until.is_none(),
            "duplicate PauseStarted for one pause token"
        );
        ensure!(
            until <= Instant::now() + PAUSE_LIMIT,
            "source pause exceeds five-second lease"
        );
        reservation.until = Some(until);
        if reservation.resuming
            || Instant::now() >= reservation.acquire_until
            || until <= Instant::now()
            || self
                .planned
                .as_ref()
                .is_none_or(|p| p.target != source || p.pause != Some(id))
        {
            self.cancel_planned();
            self.cancel_pause(source.replica, id);
            return Ok(());
        }
        self.drive_planned()
    }
    pub(super) fn pause_ended(
        &mut self,
        source: Source,
        nonce: u64,
        reason: PauseEnd,
    ) -> anyhow::Result<()> {
        let id = PauseId {
            generation: source.generation,
            nonce,
        };
        if self.feeds.get(source.replica).is_none_or(|f| {
            f.generation != source.generation
                || f.pause.is_none_or(|p| p.source != source || p.id != id)
        }) {
            self.stats.stale_events += 1;
            return Ok(());
        }
        self.feeds[source.replica].pause = None;
        self.feeds[source.replica]
            .control
            .send_modify(|c| Self::clear_pause_control(c, id));
        if self
            .planned
            .as_ref()
            .is_some_and(|p| p.target == source && p.pause == Some(id))
        {
            if reason == PauseEnd::Expired && self.rotation.is_some() {
                self.stats.rotation_timeouts += 1;
            } else {
                self.stats.deferred_rotations += 1;
            }
            self.cancel_planned();
            self.maintenance_deferred_until = Some(Instant::now() + self.health.cooldown);
            self.rotation_due = self.maintenance_deferred_until;
        }
        for symbol in self.assignment.symbols.clone() {
            self.choose(&symbol)?;
        }
        Ok(())
    }
    pub(super) fn retiring_generation(&self, replica: usize) -> bool {
        self.rotation.as_ref().is_some_and(|r| {
            r.restarting && r.slot == replica && self.feeds[replica].generation <= r.generation
        })
    }
    pub(super) fn configure_health(
        &mut self,
        options: health::Options,
        index: usize,
        budget: usize,
    ) {
        self.health = options;
        if !self.health.rotation_interval.is_zero() {
            let phase = self
                .health
                .rotation_interval
                .mul_f64(index as f64 / budget as f64);
            self.rotation_due = Some(Instant::now() + self.health.rotation_interval + phase);
        }
    }
    pub(super) fn survivor_ready(&self, slot: usize, symbol: &str) -> bool {
        self.feeds[slot].pause.is_none()
            && self.ready(slot, symbol)
            && !self.feeds[slot].books[symbol].activity.recovering
    }
    pub(super) fn planned_candidate(&self, symbol: &str, source: Source, request: u64) -> bool {
        self.planned.as_ref().is_some_and(|p| {
            p.target.replica != source.replica
                && p.fence == Some((source, request))
                && (self.contract.membership == crate::exchange::MembershipStrategy::FreshSession
                    || p.pause.is_some_and(|id| {
                        self.feeds[p.target.replica].pause.is_some_and(|r| {
                            r.id == id
                                && !r.resuming
                                && r.until.is_some_and(|until| Instant::now() < until)
                        })
                    }))
                && p.symbols.get(symbol).is_some_and(|incarnation| {
                    self.selected
                        .get(symbol)
                        .is_some_and(|s| s.incarnation == *incarnation)
                })
        })
    }
    pub(super) fn defer_planned_fence(&mut self) {
        if let Some(p) = &mut self.planned {
            // This is a local FIFO observation while target ingress is held,
            // not a new network command or the opportunistic failover grace.
            p.retry_at = Instant::now() + self.lag.min(Duration::from_millis(100));
        }
    }
    fn plan(
        &mut self,
        slot: usize,
        symbols: Vec<(String, u64)>,
        action: Action,
    ) -> anyhow::Result<()> {
        ensure!(self.planned.is_none(), "overlapping planned operation");
        self.planned = Some(Planned {
            target: Source {
                replica: slot,
                generation: self.feeds[slot].generation,
            },
            symbols: symbols.into_iter().collect(),
            action,
            started: Instant::now(),
            retry_at: Instant::now(),
            pause: None,
            fence: None,
        });
        self.drive_planned()
    }
    pub(super) fn drive_planned(&mut self) -> anyhow::Result<()> {
        let Some(p) = &self.planned else {
            return Ok(());
        };
        if self.feeds[p.target.replica].generation != p.target.generation
            || p.symbols
                .iter()
                .any(|(s, i)| self.selected.get(s).is_none_or(|v| v.incarnation != *i))
        {
            self.cancel_planned();
            return Ok(());
        }
        let needs_cut = p
            .symbols
            .iter()
            .any(|(s, _)| self.selected[s].active == Some(p.target));
        let lease_expired = p.pause.is_some_and(|id| {
            self.feeds[p.target.replica].pause.is_none_or(|r| {
                r.id != id
                    || r.resuming
                    || r.until.map_or(Instant::now() >= r.acquire_until, |until| {
                        Instant::now() >= until
                    })
            })
        });
        if p.started.elapsed() >= self.health.rotation_timeout || needs_cut && lease_expired {
            let rotation = matches!(p.action, Action::Rotate);
            eprintln!(
                "planned handoff deferred: target={:?}; old publisher retained",
                p.target
            );
            self.cancel_planned();
            if rotation {
                self.rotation = None;
                self.stats.rotation_timeouts += 1;
            }
            self.maintenance_deferred_until = Some(Instant::now() + self.health.cooldown);
            self.rotation_due = self.maintenance_deferred_until;
            return Ok(());
        }
        if self.replicas == 1 || Instant::now() < p.retry_at {
            return Ok(());
        }
        if !needs_cut {
            return Ok(());
        }
        if self.contract.membership == crate::exchange::MembershipStrategy::Acknowledged
            && p.pause.is_none()
        {
            let target = p.target;
            if self.quiescing || self.feeds.iter().any(|f| f.pause.is_some()) {
                return Ok(());
            }
            self.next_pause = self
                .next_pause
                .checked_add(1)
                .context("pause nonce overflow")?;
            let id = PauseId {
                generation: target.generation,
                nonce: self.next_pause,
            };
            let acquire_until = Instant::now() + PAUSE_LIMIT.min(self.health.rotation_timeout);
            self.feeds[target.replica].pause = Some(PauseReservation {
                source: target,
                id,
                acquire_until,
                until: None,
                resuming: false,
            });
            self.planned.as_mut().unwrap().pause = Some(id);
            self.feeds[target.replica].control.send_modify(|c| {
                c.pause = Some(PauseRequest {
                    generation: id.generation,
                    nonce: id.nonce,
                    acquire_until,
                    max_duration: PAUSE_LIMIT,
                })
            });
            return Ok(());
        }
        if self.contract.membership == crate::exchange::MembershipStrategy::Acknowledged {
            let id = p
                .pause
                .context("native planned handoff lacks pause token")?;
            if self.feeds[p.target.replica]
                .pause
                .is_none_or(|r| r.id != id || r.resuming || r.until.is_none())
            {
                return Ok(());
            }
        }
        let peer = 1 - p.target.replica;
        let symbols: Vec<_> = p
            .symbols
            .iter()
            .filter(|&(s, incarnation)| {
                self.selected.get(s).is_some_and(|state| {
                    state.incarnation == *incarnation && state.active == Some(p.target)
                }) && self.survivor_ready(peer, s)
            })
            .map(|(s, _)| s.clone())
            .collect();
        if !symbols.is_empty() {
            if p.fence.is_some_and(|(source, id)| {
                self.feeds[source.replica]
                    .pending_fence
                    .is_some_and(|(s, current, _)| s == source && current == id)
            }) {
                return Ok(());
            }
            for symbol in symbols {
                self.selected.get_mut(&symbol).unwrap().candidate = Some(peer);
            }
            // Supersede any pre-pause fence. Only this freshly issued request
            // can authorize the planned cut for the accepted pause token.
            self.feeds[peer].pending_fence = None;
            self.fence(peer)?;
            let (source, request, _) = self.feeds[peer].pending_fence.unwrap();
            self.planned.as_mut().unwrap().fence = Some((source, request));
        }
        Ok(())
    }
    /// Called only after all emitted baselines have entered the ordered writer
    /// queue (or a tick with no publication cut). No socket await occurs here.
    pub(super) fn release_planned(&mut self) -> anyhow::Result<()> {
        let Some(p) = self.planned.take() else {
            return Ok(());
        };
        if self.feeds[p.target.replica].generation != p.target.generation
            || p.symbols
                .iter()
                .any(|(s, i)| self.selected.get(s).is_none_or(|v| v.incarnation != *i))
        {
            self.planned = Some(p);
            self.cancel_planned();
            self.rotation_due = Some(Instant::now() + self.health.cooldown);
            return Ok(());
        }
        let safe = self.replicas == 1
            || p.symbols.iter().all(|(symbol, _)| {
                let state = &self.selected[symbol];
                if !self.requires_survivor(state) {
                    return true;
                }
                state.active.is_some_and(|a| {
                    a != p.target
                        && self.survivor_ready(a.replica, symbol)
                        && state.floor.is_none_or(|floor| {
                            self.feeds[a.replica].books[symbol]
                                .frontier
                                .is_some_and(|f| f.covers(floor))
                        })
                })
            });
        if !safe {
            self.planned = Some(p);
            return Ok(());
        }
        let slot = p.target.replica;
        if let Some(id) = p.pause
            && let Some(reservation) = &mut self.feeds[slot].pause
            && reservation.id == id
        {
            reservation.resuming = true;
        }
        match p.action {
            Action::Rotate => {
                if self.rotation.as_ref().is_some_and(|r| r.health) {
                    self.stats.health_transport_renewals += 1;
                }
                self.invalidate(p.target, None);
                let feed = &mut self.feeds[slot];
                feed.next_renewal = Instant::now() + self.health.cooldown;
                feed.min_generation = feed
                    .generation
                    .checked_add(1)
                    .context("generation floor overflow")?;
                let restart = feed
                    .control
                    .borrow()
                    .restart
                    .checked_add(1)
                    .context("restart nonce overflow")?;
                feed.control.send_modify(|c| {
                    if let Some(id) = p.pause {
                        Self::clear_pause_control(c, id);
                    }
                    c.restart = restart;
                    c.refresh.clear();
                    c.resubscribe.clear();
                });
                self.reset_restarts.remove(&slot);
                let r = self
                    .rotation
                    .as_mut()
                    .context("rotation lost before release")?;
                r.restarting = true;
                r.started = Instant::now();
                r.roster = p.symbols;
                eprintln!("rotation: admitted peer baseline; reconnecting replica {slot}");
            }
            Action::Resubscribe(plans) => {
                let mut requests = std::collections::BTreeMap::new();
                for plan in plans {
                    let now = Instant::now();
                    let epoch = now.saturating_duration_since(self.origin).as_secs()
                        / self.health.bucket_seconds();
                    let peer = (0..self.replicas)
                        .filter(|&i| i != slot && self.survivor_ready(i, &plan.symbol))
                        .map(|i| {
                            self.feeds[i].books[&plan.symbol]
                                .activity
                                .window
                                .completed(epoch)
                        })
                        .max();
                    if self.feeds[slot].books[&plan.symbol]
                        .activity
                        .preview(epoch, now, peer, &self.health)
                        .is_none_or(|d| d.action != health::Action::Resubscribe)
                    {
                        continue;
                    }
                    let nonce = self.feeds[slot]
                        .control
                        .borrow()
                        .resubscribe
                        .get(&plan.symbol)
                        .map_or(0, |r| r.nonce)
                        .checked_add(1)
                        .context("resubscribe nonce overflow")?;
                    let request = Resubscribe {
                        incarnation: plan.incarnation,
                        generation: plan.generation,
                        nonce,
                    };
                    let book = self.feeds[slot].books.get_mut(&plan.symbol).unwrap();
                    book.recovery = Some((request, false));
                    book.activity.admitted(Instant::now(), &self.health);
                    self.stats.health_resubscriptions += 1;
                    requests.insert(plan.symbol, request);
                }
                let symbols = requests.keys().cloned().collect::<Vec<_>>();
                if !symbols.is_empty() {
                    self.invalidate(p.target, Some(&symbols));
                }
                self.feeds[slot].control.send_modify(|c| {
                    if let Some(id) = p.pause {
                        Self::clear_pause_control(c, id);
                    }
                    c.resubscribe.extend(requests);
                });
            }
        }
        Ok(())
    }
    fn covers_floor(&self, peer: usize, symbol: &str, floor: Frontier) -> bool {
        self.survivor_ready(peer, symbol)
            && self.feeds[peer].books[symbol]
                .frontier
                .is_some_and(|f| f.covers(floor))
    }
    /// Runs only with no rotation, plan or pause outstanding (checked by the
    /// caller). A scoped probe retains the standby's valid book and tail.
    fn coverage_probe(&mut self, slot: usize) -> Coverage {
        if self.quiescing
            || self.contract.membership != MembershipStrategy::Acknowledged
            || self.contract.ordering != OrderingStrategy::ComparableClock
        {
            self.coverage = None;
            return Coverage::Clear;
        }
        let peer = 1 - slot;
        let target = Source {
            replica: slot,
            generation: self.feeds[slot].generation,
        };
        let peer_source = Source {
            replica: peer,
            generation: self.feeds[peer].generation,
        };
        let now = Instant::now();
        let mut probe = match self.coverage.take() {
            Some(probe) if probe.target == target && probe.peer == peer_source => probe,
            _ => CoverageProbe {
                target,
                peer: peer_source,
                started: now,
                witness: BTreeMap::new(),
                refreshed: BTreeSet::new(),
            },
        };
        // A witness stops constraining this cut once its peer covers the frozen
        // floor, publication left the target, or the market lifetime changed.
        let refreshed = &mut probe.refreshed;
        probe.witness.retain(|symbol, w| {
            let keep = self
                .selected
                .get(symbol)
                .is_some_and(|s| s.incarnation == w.incarnation && s.active == Some(target))
                && !self.covers_floor(peer, symbol, w.floor);
            if !keep {
                // A new floor for the same spelling is a new question. Its
                // resolved predecessor must not consume that question's one
                // allowed probe in this acquisition window.
                refreshed.remove(symbol);
            }
            keep
        });
        // A delta floor at clock d is always reachable by the peer's own
        // ordered tail (d <= its snapshot clock, or d is in flight), so partial
        // cuts stay unblocked. Only a complete snapshot floor the peer does not
        // cover needs a future market change or a refresh. Re-scan at every
        // decision: a quiet book may move onto the target during this grace.
        for (symbol, state) in &self.selected {
            let Some(floor) = state.floor.filter(|floor| floor.complete) else {
                continue;
            };
            if state.active == Some(target)
                && !probe.witness.contains_key(symbol)
                && self.requires_survivor(state)
                && self.survivor_ready(peer, symbol)
                && !self.covers_floor(peer, symbol, floor)
            {
                probe.witness.insert(
                    symbol.clone(),
                    Witness {
                        incarnation: state.incarnation,
                        floor,
                        then: self.feeds[peer].books[symbol].frontier,
                        since: now,
                    },
                );
            }
        }
        let witness = &probe.witness;
        probe
            .refreshed
            .retain(|symbol| witness.contains_key(symbol));
        if probe.witness.is_empty() {
            return Coverage::Clear;
        }
        if probe.started.elapsed() >= self.health.rotation_timeout {
            eprintln!(
                "rotation coverage deferred: target={target:?}; {} standby books never reached their frozen floor",
                probe.witness.len()
            );
            return Coverage::Expired;
        }
        // Unchanged frontier over the whole grace: nothing more is in flight
        // for it on this ordered tail. A progressing standby is left to the pause.
        let stale: Vec<String> = probe
            .witness
            .iter()
            .filter(|(symbol, w)| {
                w.since.elapsed() >= self.lag
                    && !probe.refreshed.contains(*symbol)
                    && self.survivor_ready(peer, symbol)
                    && self.feeds[peer].books[*symbol].frontier == w.then
            })
            .map(|(symbol, _)| symbol.clone())
            .collect();
        if !stale.is_empty() {
            eprintln!(
                "rotation coverage: target={target:?}; requesting {} fresh peer baselines (first={}) before any ingress pause",
                stale.len(),
                stale[0]
            );
        }
        for symbol in &stale {
            // Standby book on the peer: publication is unchanged. Its new
            // snapshot must still pass the frozen catch-up floor checks.
            self.probe(peer, symbol);
        }
        probe.refreshed.extend(stale);
        let waiting = !probe.refreshed.is_empty()
            || probe.witness.values().any(|w| w.since.elapsed() < self.lag);
        self.coverage = Some(probe);
        if waiting {
            Coverage::Wait
        } else {
            Coverage::Clear
        }
    }
    /// This generation delivered a baseline, the Owner judged it unusable and
    /// no request is outstanding, so nothing can still make it ready. `frontier`
    /// separates that from a new generation awaiting its first baseline with a
    /// quarantine carried across Begin. A valid value is a covering baseline
    /// still waiting for its membership ACK, which is not a judged failure.
    pub(super) fn terminally_quarantined(&self, slot: usize, symbol: &str) -> bool {
        self.feeds[slot].books.get(symbol).is_some_and(|book| {
            book.probe_quarantined
                && book.frontier.is_some()
                && book.probe.is_none()
                && book.recovery.is_none()
                && book.catchup.is_none()
                && !book.value.is_valid()
        })
    }
    /// A replacement that already spent this lifetime's owner-held semantic
    /// budget gets no more destructive retries; only a new lifetime or a usable
    /// baseline clears it. An explicit continuity reset is separate work: its
    /// fresh admission needs this restart, and each reset queues at most one.
    fn defer_exhausted_rotation(&mut self, slot: usize) -> bool {
        if self.contract.membership != MembershipStrategy::Acknowledged
            || self.reset_restarts.contains(&slot)
        {
            return false;
        }
        let limit = self.health.max_resubscriptions;
        let Some(symbol) = self.feeds[slot]
            .books
            .iter()
            .find(|&(symbol, book)| {
                book.rejected_baselines >= limit
                    && book.probe_quarantined
                    && self.selected.contains_key(symbol)
            })
            .map(|(symbol, _)| symbol.clone())
        else {
            return false;
        };
        self.stats.deferred_rotations += 1;
        let retry = Instant::now() + self.health.cooldown;
        self.maintenance_deferred_until = Some(retry);
        self.rotation_due = Some(retry);
        eprintln!(
            "rotation deferred: replica={slot} generation={} first={symbol}; unusable baseline exhausted the semantic budget, publisher retained",
            self.feeds[slot].generation
        );
        true
    }
    pub(super) fn start_rotation(&mut self, slot: usize) -> anyhow::Result<bool> {
        if self.contract.membership == crate::exchange::MembershipStrategy::FreshSession
            && Instant::now() < self.feeds[slot].next_renewal
        {
            return Ok(false);
        }
        if self.rotation.is_some()
            || self.planned.is_some()
            || self.feeds.iter().any(|f| f.pause.is_some())
            || self.selected.is_empty()
            || self
                .feeds
                .iter()
                .any(|f| f.books.values().any(|b| b.activity.recovering))
        {
            return Ok(false);
        }
        if self.defer_exhausted_rotation(slot) {
            return Ok(false);
        }
        if self.replicas > 1 {
            match self.coverage_probe(slot) {
                Coverage::Wait => return Ok(false),
                Coverage::Expired => {
                    // Honest deferral: no ingress pause, no destructive control.
                    self.stats.deferred_rotations += 1;
                    self.maintenance_deferred_until = Some(Instant::now() + self.health.cooldown);
                    self.rotation_due = self.maintenance_deferred_until;
                    return Ok(false);
                }
                Coverage::Clear => self.coverage = None,
            }
            let peer = 1 - slot;
            if !self
                .selected
                .iter()
                .all(|(s, state)| !self.requires_survivor(state) || self.survivor_ready(peer, s))
            {
                self.stats.deferred_rotations += 1;
                self.rotation_due = Some(Instant::now() + self.health.cooldown);
                return Ok(false);
            }
        }
        self.rotation = Some(Rotation {
            health: false,
            slot,
            generation: self.feeds[slot].generation,
            started: Instant::now(),
            restarting: false,
            timed_out: false,
            roster: BTreeMap::new(),
        });
        self.stats.planned_rotations += 1;
        self.plan(
            slot,
            self.selected
                .iter()
                .map(|(s, v)| (s.clone(), v.incarnation))
                .collect(),
            Action::Rotate,
        )?;
        Ok(true)
    }
    fn current_lifetime(&self, symbol: &str, incarnation: u64) -> bool {
        self.selected
            .get(symbol)
            .is_some_and(|state| state.incarnation == incarnation)
    }
    pub(super) fn rotation_tick(&mut self) -> anyhow::Result<()> {
        self.drive_planned()?;
        let Some(rotation) = &self.rotation else {
            if self
                .maintenance_deferred_until
                .is_some_and(|at| Instant::now() < at)
            {
                return Ok(());
            }
            if let Some(&slot) = self.reset_restarts.first() {
                self.start_rotation(slot)?;
            } else if let Some(slot) = self
                .needs_renewal
                .union(&self.needs_refresh)
                .copied()
                .find(|&slot| Instant::now() >= self.feeds[slot].next_renewal)
            {
                self.start_rotation(slot)?;
            } else if !self.quiescing && self.rotation_due.is_some_and(|at| Instant::now() >= at) {
                self.start_rotation(self.rotation_slot)?;
            }
            return Ok(());
        };
        let (slot, old) = (rotation.slot, rotation.generation);
        if self.selected.is_empty() {
            self.rotation = None;
            self.cancel_planned();
            return Ok(());
        }
        if self.contract.membership == crate::exchange::MembershipStrategy::FreshSession
            && rotation.restarting
            && self.feeds[slot].generation > old
            && self.feeds[slot].applied.as_ref().is_some_and(|written| {
                written.iter().all(|(symbol, incarnation)| {
                    self.assignment.incarnations.get(symbol) != Some(incarnation)
                })
            })
        {
            // The actual written roster has no remaining lifetime to initialize.
            // Keep the latest roster queued; an obsolete operation is not a
            // completed renewal. next_renewal retains the existing feed cooldown.
            self.rotation = None;
            self.cancel_planned();
            self.needs_renewal.insert(slot);
            self.stats.deferred_rotations += 1;
            eprintln!("public renewal superseded: replica={slot}; latest roster remains queued");
            return Ok(());
        }
        let replaced = rotation.restarting && self.feeds[slot].generation > old;
        if replaced
            && self.contract.membership == MembershipStrategy::Acknowledged
            && !rotation
                .roster
                .iter()
                .any(|(symbol, &incarnation)| self.current_lifetime(symbol, incarnation))
        {
            // Every lifetime this restart admitted was retired or replaced, so
            // nothing is left to certify. The current selection is separate
            // work; retry after the cooldown. A counted timeout stays counted.
            self.rotation = None;
            self.cancel_planned();
            self.stats.deferred_rotations += 1;
            self.maintenance_deferred_until = Some(Instant::now() + self.health.cooldown);
            self.rotation_due = self.maintenance_deferred_until;
            eprintln!("native rotation superseded: replica={slot}; no admitted lifetime remains");
            return Ok(());
        }
        let complete = replaced
            && match self.contract.membership {
                // A newer assignment can supersede this operation's roster
                // while its write/baselines are in flight. Finish the admitted
                // renewal; needs_renewal still tracks the newer roster. Requiring
                // exact latest equality here would block the next renewal forever.
                MembershipStrategy::FreshSession => {
                    self.feeds[slot].applied.is_some()
                        && self.selected.keys().any(|s| self.ready(slot, s))
                        && self
                            .selected
                            .iter()
                            .all(|(s, state)| self.ready(slot, s) || !state.ever)
                }
                // Only surviving admitted lifetimes owe readiness to this
                // operation. Later additions and replacement incarnations stay
                // visible in the report and are guarded at the next destructive
                // cut. `ready` is the frozen initialization goal, not the
                // moving publication floor every switch still checks.
                MembershipStrategy::Acknowledged => {
                    rotation.roster.iter().all(|(symbol, &incarnation)| {
                        !self.current_lifetime(symbol, incarnation) || self.ready(slot, symbol)
                    })
                }
            };
        if complete {
            self.stats.completed_rotations += 1;
            if rotation.health {
                self.stats.completed_health_renewals += 1;
            }
            self.rotation = None;
            self.rotation_slot = (slot + 1) % self.replicas;
            let jitter = self
                .health
                .rotation_interval
                .mul_f64(f64::from(rand::random::<u8>()) / 2550.0);
            self.rotation_due = if self.health.rotation_interval.is_zero() {
                None
            } else {
                Some(Instant::now() + self.health.rotation_interval + jitter)
            };
        } else if rotation.restarting && rotation.started.elapsed() >= self.health.rotation_timeout
        {
            if !rotation.timed_out {
                self.stats.rotation_timeouts += 1;
                self.rotation.as_mut().unwrap().timed_out = true;
            }
            if replaced && self.contract.membership == MembershipStrategy::Acknowledged {
                self.fail_quarantined_rotation(slot, old);
            }
        }
        Ok(())
    }
    /// Ends a released native rotation that nothing can finish. This consumes
    /// the logical operation only: the Source job, its generation and every
    /// admission fence are unchanged, and no control is sent. Later evidence
    /// may heal a book but cannot complete this operation retroactively.
    ///
    /// Scans only the admitted roster, only past the deadline. A book without a
    /// healthy publisher stays with the publication-loss path, whose reset ends
    /// the operation itself without a cooldown ahead of the required restart.
    fn fail_quarantined_rotation(&mut self, slot: usize, old: u64) {
        let Some(rotation) = &self.rotation else {
            return;
        };
        let (mut first, mut quarantined) = (None, 0usize);
        for (symbol, &incarnation) in &rotation.roster {
            if !self.current_lifetime(symbol, incarnation) || self.ready(slot, symbol) {
                continue;
            }
            let published = self.selected[symbol]
                .active
                .is_some_and(|publisher| self.ready(publisher.replica, symbol));
            if !published || !self.terminally_quarantined(slot, symbol) {
                return;
            }
            first.get_or_insert_with(|| symbol.clone());
            quarantined += 1;
        }
        let Some(first) = first else {
            return;
        };
        self.rotation = None;
        self.stats.failed_rotations += 1;
        let retry = Instant::now() + self.health.cooldown;
        self.maintenance_deferred_until = Some(retry);
        self.rotation_due = Some(retry);
        eprintln!(
            "rotation failed: replica={slot} old_generation={old} generation={} quarantined={quarantined} first={first}; publisher retained, no completion credit",
            self.feeds[slot].generation
        );
    }
    pub(super) fn activity_tick(&mut self) -> anyhow::Result<()> {
        if self.quiescing
            || self.rotation.is_some()
            || self.planned.is_some()
            || self.feeds.iter().any(|f| f.pause.is_some())
            || self
                .maintenance_deferred_until
                .is_some_and(|at| Instant::now() < at)
            || self
                .feeds
                .iter()
                .any(|f| f.books.values().any(|b| b.activity.recovering))
        {
            return Ok(());
        }
        let now = Instant::now();
        let epoch =
            now.saturating_duration_since(self.origin).as_secs() / self.health.bucket_seconds();
        if let Some(scheduled) = self.scheduled_recovery.take() {
            if now < scheduled.at {
                self.scheduled_recovery = Some(scheduled);
            } else {
                self.apply_recovery(scheduled.plans, epoch, now)?;
            }
            return Ok(());
        }
        if self.health_epoch == Some(epoch) {
            return Ok(());
        }
        self.health_epoch = Some(epoch);
        let mut actions: Vec<RecoveryPlan> = Vec::new();
        let mut low = 0;
        for (symbol, state) in &self.selected {
            for slot in 0..self.replicas {
                let peer = (0..self.replicas)
                    .filter(|&i| i != slot && self.survivor_ready(i, symbol))
                    .map(|i| self.feeds[i].books[symbol].activity.window.completed(epoch))
                    .max();
                if self.feeds[slot]
                    .books
                    .get_mut(symbol)
                    .unwrap()
                    .activity
                    .evaluate(epoch, now, peer, &self.health)
                    .is_some()
                {
                    low += 1;
                    if actions.len() < 4
                        && (self.replicas == 1 || peer.is_some())
                        && actions.first().is_none_or(|a| a.slot == slot)
                    {
                        actions.push(RecoveryPlan {
                            slot,
                            symbol: symbol.clone(),
                            incarnation: state.incarnation,
                            generation: self.feeds[slot].generation,
                        });
                    }
                    break;
                }
            }
        }
        self.stats.health_recovery_candidates = low;
        if actions.is_empty() {
            return Ok(());
        }
        if let Some(pacer) = &self.recovery_pacer {
            let at = pacer.reserve();
            if at > Instant::now() {
                self.scheduled_recovery = Some(ScheduledRecovery { at, plans: actions });
                return Ok(());
            }
        }
        self.apply_recovery(actions, epoch, now)
    }
    fn apply_recovery(
        &mut self,
        plans: Vec<RecoveryPlan>,
        epoch: u64,
        now: Instant,
    ) -> anyhow::Result<()> {
        let mut accepted = vec![];
        for plan in plans {
            let RecoveryPlan {
                slot,
                ref symbol,
                incarnation,
                generation,
            } = plan;
            if self
                .selected
                .get(symbol)
                .is_none_or(|s| s.incarnation != incarnation)
                || self.feeds[slot].generation != generation
                || !self.ready(slot, symbol)
            {
                continue;
            }
            let peer = (0..self.replicas)
                .filter(|&i| i != slot && self.survivor_ready(i, symbol))
                .map(|i| self.feeds[i].books[symbol].activity.window.completed(epoch))
                .max();
            if self.replicas > 1 && peer.is_none() {
                continue;
            }
            let Some(decision) =
                self.feeds[slot].books[symbol]
                    .activity
                    .preview(epoch, now, peer, &self.health)
            else {
                continue;
            };
            eprintln!(
                "health proposal: replica={slot} symbol={symbol} reason={} points={} peer_points={} baseline={} attempt={} action={:?}",
                decision.reason,
                decision.points,
                decision.peer_points,
                decision.baseline,
                decision.attempt,
                decision.action
            );
            if self.contract.membership == crate::exchange::MembershipStrategy::FreshSession {
                if accepted.is_empty() && self.start_rotation(slot)? {
                    self.rotation.as_mut().unwrap().health = true;
                }
                break;
            }
            if decision.action == health::Action::Escalate {
                if accepted.is_empty() && self.start_rotation(slot)? {
                    self.stats.health_escalations += 1;
                }
                break;
            }
            accepted.push(plan);
        }
        if let Some(first) = accepted.first() {
            let slot = first.slot;
            let symbols = accepted
                .iter()
                .map(|p| (p.symbol.clone(), p.incarnation))
                .collect();
            self.plan(slot, symbols, Action::Resubscribe(accepted))?;
        }
        Ok(())
    }
}
