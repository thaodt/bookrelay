//! One in-flight membership command per connection. Desired changes never wait
//! for an ACK to revoke local publication permission.
use anyhow::{Context, ensure};
use serde_json::Value;
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::time::Instant;

use crate::exchange::{Control, ExchangeAdapter, MembershipStrategy, kalshi::Kalshi};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Kind {
    Subscribe,
    Add,
    Delete,
    Snapshot,
}

#[cfg_attr(test, derive(Clone))]
pub struct Pending {
    pub id: u64,
    pub kind: Kind,
    pub symbols: BTreeSet<String>,
    pub sent: Instant,
    pub written: bool,
}

#[cfg_attr(test, derive(Clone))]
pub struct Membership {
    pub desired: BTreeSet<String>,
    pub wire: BTreeSet<String>,
    pub pending: Option<Pending>,
    pub sid: Option<u64>,
    next_id: u64,
    restarts: BTreeSet<String>,
    /// Native only: the wire is exactly the desired roster and carries no
    /// restarted incarnation. Kept current by every write to those sets so the
    /// idle poll in `needs_command` never compares them.
    settled: bool,
    /// Latched the first time a native roster is settled against acknowledged
    /// state; never cleared, so later catalog churn stays routine. Holds after
    /// every mutation: `settled && sid.is_some() && pending.is_none()` implies
    /// this flag.
    bootstrapped: bool,
    strategy: MembershipStrategy,
    started: bool,
    pub written: BTreeSet<String>,
}
impl Default for Membership {
    fn default() -> Self {
        Self {
            desired: BTreeSet::new(),
            wire: BTreeSet::new(),
            pending: None,
            sid: None,
            next_id: 0,
            restarts: BTreeSet::new(),
            settled: true,
            bootstrapped: false,
            strategy: MembershipStrategy::Acknowledged,
            started: false,
            written: BTreeSet::new(),
        }
    }
}

impl Membership {
    pub fn new(symbols: &[String]) -> Self {
        Self::with_strategy(symbols, MembershipStrategy::Acknowledged)
    }
    pub fn with_strategy(symbols: &[String], strategy: MembershipStrategy) -> Self {
        Self {
            desired: symbols.iter().cloned().collect(),
            settled: symbols.is_empty(),
            strategy,
            ..Self::default()
        }
    }
    pub fn members(&self) -> &BTreeSet<String> {
        match self.strategy {
            MembershipStrategy::Acknowledged => &self.wire,
            MembershipStrategy::FreshSession => &self.written,
        }
    }
    /// Call only after the complete websocket write. This is not a server ACK.
    pub fn command_written(&mut self) {
        self.started = true;
        if self.strategy == MembershipStrategy::FreshSession {
            if let Some(p) = self.pending.take() {
                self.written = p.symbols;
            }
        } else if let Some(p) = &mut self.pending {
            p.written = true;
            p.sent = Instant::now();
        }
    }
    pub fn desired(&self, symbol: &str) -> bool {
        self.desired.contains(symbol)
    }
    pub(crate) fn retains(&self, symbol: &str) -> bool {
        self.members().contains(symbol)
            || self.pending.as_ref().is_some_and(|p| {
                matches!(p.kind, Kind::Subscribe | Kind::Add) && p.symbols.contains(symbol)
            })
    }
    pub fn replace(&mut self, symbols: &[String]) {
        if !symbols.iter().eq(self.desired.iter()) {
            self.desired = symbols.iter().cloned().collect();
            self.restarts.retain(|s| self.desired.contains(s));
            self.refresh_settled();
            self.prune_snapshot_request();
        }
    }
    pub fn restart(&mut self, symbol: &str) {
        self.restarts.insert(symbol.to_owned());
        // A restart adds reconciliation work only for a symbol on the wire.
        self.settled &= !self.wire.contains(symbol);
        self.prune_snapshot_request();
    }
    fn refresh_settled(&mut self) {
        self.settled = self.wire == self.desired && self.restarts.is_disjoint(&self.wire);
        self.latch_bootstrap();
    }
    /// `sid` exists only after a real Subscribe ACK, and an in-flight command
    /// may still change the wire. Constant time, because `settled` is already
    /// current: neither the snapshot path nor the idle poll compares the sets.
    fn latch_bootstrap(&mut self) {
        self.bootstrapped |= self.settled && self.sid.is_some() && self.pending.is_none();
    }
    /// Resolving a command without changing the sets, as a rejection does, can
    /// be what makes an already acknowledged wire the complete roster.
    fn clear_pending(&mut self) {
        self.pending = None;
        self.latch_bootstrap();
    }
    /// A native session that has never reached its roster on acknowledged
    /// state. FreshSession has no ACKs to settle against and is never urgent.
    pub(crate) fn bootstrapping(&self) -> bool {
        self.strategy == MembershipStrategy::Acknowledged && !self.bootstrapped
    }
    fn prune_snapshot_request(&mut self) {
        if let Some(p) = &mut self.pending
            && p.kind == Kind::Snapshot
        {
            p.symbols
                .retain(|s| self.desired.contains(s) && !self.restarts.contains(s));
            if p.symbols.is_empty() {
                self.clear_pending();
            }
        }
    }
    pub fn accepts(&self, symbol: &str) -> bool {
        if self.strategy == MembershipStrategy::FreshSession {
            return self.desired(symbol)
                && self.started
                && self.written.contains(symbol)
                && !self.restarts.contains(symbol);
        }
        self.desired(symbol)
            && !self.restarts.contains(symbol)
            && (self.wire.contains(symbol)
                || self.pending.as_ref().is_some_and(|p| {
                    matches!(p.kind, Kind::Subscribe | Kind::Add) && p.symbols.contains(symbol)
                }))
    }
    pub fn needs_command(&self, dirty: &BTreeSet<String>) -> bool {
        if self.strategy == MembershipStrategy::FreshSession {
            return !self.started && self.pending.is_none() && !self.desired.is_empty();
        }
        self.pending.is_none() && (!self.settled || !dirty.is_disjoint(&self.wire))
    }
    pub fn expired(&self, timeout: Duration) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|p| p.written && p.sent.elapsed() > timeout)
    }
    /// A lost ACK makes the command's symbols uncertain, not every symbol on
    /// the subscription. Conservatively delete that union before adding it
    /// again. A subscribe without a known sid still needs a new connection.
    pub fn recover_timeout(&mut self, timeout: Duration) -> Option<Vec<String>> {
        if !self.expired(timeout)
            || self.sid.is_none()
            || self
                .pending
                .as_ref()
                .is_some_and(|p| p.kind == Kind::Subscribe)
        {
            return None;
        }
        let pending = self.pending.take()?;
        let symbols: Vec<_> = pending
            .symbols
            .into_iter()
            .filter(|s| self.desired(s))
            .collect();
        self.wire.extend(symbols.iter().cloned());
        self.restarts.extend(symbols.iter().cloned());
        self.refresh_settled();
        Some(symbols)
    }
    pub fn command(
        &mut self,
        dirty: &BTreeSet<String>,
        batch: usize,
    ) -> anyhow::Result<Option<Value>> {
        self.command_with(&Kalshi, dirty, batch)
    }
    pub fn command_with(
        &mut self,
        protocol: &dyn ExchangeAdapter,
        dirty: &BTreeSet<String>,
        batch: usize,
    ) -> anyhow::Result<Option<Value>> {
        ensure!(
            protocol.contract().membership == self.strategy,
            "membership strategy does not match protocol contract"
        );
        if self.pending.is_some() || self.desired.is_empty() && self.wire.is_empty() {
            return Ok(None);
        }
        ensure!(batch > 0, "zero membership batch");
        if self.strategy == MembershipStrategy::FreshSession && self.started {
            return Ok(None);
        }
        let (kind, symbols): (Kind, BTreeSet<String>) = if self.sid.is_none() {
            (
                Kind::Subscribe,
                self.desired
                    .iter()
                    .take(if self.strategy == MembershipStrategy::FreshSession {
                        self.desired.len()
                    } else {
                        batch
                    })
                    .cloned()
                    .collect(),
            )
        } else {
            let removed = self
                .wire
                .iter()
                .filter(|s| !self.desired.contains(*s) || self.restarts.contains(*s))
                .take(batch)
                .cloned()
                .collect::<BTreeSet<_>>();
            let added = self
                .desired
                .difference(&self.wire)
                .take(batch)
                .cloned()
                .collect::<BTreeSet<_>>();
            if !removed.is_empty() {
                (Kind::Delete, removed)
            } else if !added.is_empty() {
                (Kind::Add, added)
            } else {
                (
                    Kind::Snapshot,
                    dirty
                        .intersection(&self.wire)
                        .take(batch)
                        .cloned()
                        .collect(),
                )
            }
        };
        if symbols.is_empty() {
            return Ok(None);
        }
        self.next_id = self.next_id.checked_add(1).context("command ID overflow")?;
        let list: Vec<_> = symbols.iter().cloned().collect();
        let id = self.next_id;
        let command = match kind {
            Kind::Subscribe => protocol.subscribe(id, &list)?,
            Kind::Snapshot => protocol.get_snapshot(id, self.sid.unwrap(), &list)?,
            Kind::Add | Kind::Delete => {
                protocol.update_subscription(id, self.sid.unwrap(), &list, kind == Kind::Add)?
            }
        };
        self.pending = Some(Pending {
            id,
            kind,
            symbols,
            sent: Instant::now(),
            written: false,
        });
        Ok(Some(command))
    }
    pub fn control(&mut self, id: Option<u64>, control: &Control) -> anyhow::Result<()> {
        if self.strategy == MembershipStrategy::FreshSession {
            ensure!(
                !matches!(control, Control::Subscribed(_) | Control::Ok),
                "native ACK on a write-only membership contract"
            );
            return Ok(());
        }
        // get_snapshot data can carry the command ID. A snapshot is not an
        // ACK: the data path retires its pending symbol after processing it.
        if matches!(control, Control::Other) {
            return Ok(());
        }
        if matches!(control, Control::Error { .. }) {
            // Errors without a request ID cannot be safely attributed to a command.
            if id.is_some() && self.pending.as_ref().is_some_and(|p| Some(p.id) == id) {
                self.clear_pending();
            }
            return Ok(());
        }
        let Some(pending) = &self.pending else {
            return Ok(());
        };
        if id != Some(pending.id) {
            return Ok(());
        }
        match control {
            Control::Subscribed(sid) if pending.kind == Kind::Subscribe => {
                ensure!(self.sid.is_none(), "duplicate subscription creation");
                self.sid = Some(*sid);
                self.wire.extend(pending.symbols.iter().cloned());
            }
            Control::Ok if pending.kind == Kind::Add => {
                self.wire.extend(pending.symbols.iter().cloned())
            }
            Control::Ok if pending.kind == Kind::Delete => {
                self.wire.retain(|s| !pending.symbols.contains(s));
                self.restarts.retain(|s| !pending.symbols.contains(s));
            }
            // get_snapshot ACK does not mean the snapshots have arrived.
            Control::Ok if pending.kind == Kind::Snapshot => return Ok(()),
            _ => anyhow::bail!("unexpected response to membership command {}", pending.id),
        }
        self.pending = None;
        self.refresh_settled();
        Ok(())
    }
    pub fn snapshot(&mut self, symbol: &str) {
        if let Some(p) = &mut self.pending
            && p.kind == Kind::Snapshot
        {
            p.symbols.remove(symbol);
            if p.symbols.is_empty() {
                self.clear_pending();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn fresh_membership_writes_one_initial_roster_without_inventing_sid_or_ack() {
        let mut m =
            Membership::with_strategy(&["A".into(), "B".into()], MembershipStrategy::FreshSession);
        let protocol = crate::exchange::polymarket::Polymarket;
        let initial = m
            .command_with(&protocol, &BTreeSet::new(), 1)
            .unwrap()
            .unwrap();
        assert_eq!(initial["assets_ids"], json!(["A", "B"]));
        assert!(!m.accepts("A"));
        assert!(m.wire.is_empty());
        m.command_written();
        assert!(m.accepts("A") && m.accepts("B"));
        assert!(m.sid.is_none());
        assert!(m.pending.is_none());
        assert!(m.wire.is_empty());
        assert_eq!(m.written.len(), 2);
        assert!(m.control(Some(1), &Control::Subscribed(0)).is_err());
        m.replace(&["B".into(), "C".into()]);
        assert!(!m.accepts("A") && !m.accepts("C") && m.accepts("B"));
        assert!(!m.needs_command(&BTreeSet::from(["B".into()])));
        assert!(
            m.command_with(&protocol, &BTreeSet::new(), 1)
                .unwrap()
                .is_none()
        );
        m.restart("B");
        assert!(!m.accepts("B"));
    }
    #[tokio::test(start_paused = true)]
    async fn ack_deadline_begins_after_write_and_fresh_mode_has_no_ack_timer() {
        let mut native = Membership::new(&["A".into()]);
        native.command(&BTreeSet::new(), 1).unwrap();
        tokio::time::advance(Duration::from_secs(40)).await;
        assert!(!native.expired(Duration::from_secs(30)));
        native.command_written();
        assert!(!native.expired(Duration::from_secs(30)));
        tokio::time::advance(Duration::from_secs(31)).await;
        assert!(native.expired(Duration::from_secs(30)));
        let mut fresh = Membership::with_strategy(&["A".into()], MembershipStrategy::FreshSession);
        fresh
            .command_with(
                &crate::exchange::polymarket::Polymarket,
                &BTreeSet::new(),
                1,
            )
            .unwrap();
        fresh.command_written();
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(!fresh.expired(Duration::from_secs(30)));
    }
    #[tokio::test(start_paused = true)]
    async fn lost_add_ack_deletes_uncertain_symbols_before_resubscribing() {
        let mut m = Membership::new(&["A".into()]);
        let dirty = BTreeSet::new();
        m.command(&dirty, 10).unwrap();
        m.control(Some(1), &Control::Subscribed(7)).unwrap();
        m.replace(&["A".into(), "B".into()]);
        m.command(&dirty, 10).unwrap();
        m.command_written();
        tokio::time::advance(Duration::from_secs(31)).await;
        assert_eq!(
            m.recover_timeout(Duration::from_secs(30)),
            Some(vec!["B".into()])
        );
        assert!(!m.accepts("B"));
        assert!(m.accepts("A"));
        let delete = m.command(&dirty, 10).unwrap().unwrap();
        assert_eq!(delete["params"]["action"], "delete_markets");
        assert_eq!(delete["params"]["market_tickers"], json!(["B"]));
        m.control(Some(2), &Control::Ok).unwrap(); // Late old ACK cannot finish delete.
        assert!(m.pending.is_some());
        assert!(!m.accepts("B"));
        m.control(Some(3), &Control::Ok).unwrap();
        assert_eq!(
            m.command(&dirty, 10).unwrap().unwrap()["params"]["action"],
            "add_markets"
        );
    }
    #[tokio::test(start_paused = true)]
    async fn missing_initial_sid_requires_transport_replacement() {
        let mut m = Membership::new(&["A".into()]);
        m.command(&BTreeSet::new(), 10).unwrap();
        m.command_written();
        tokio::time::advance(Duration::from_secs(31)).await;
        assert!(m.recover_timeout(Duration::from_secs(30)).is_none());
        assert!(m.expired(Duration::from_secs(30)));
    }
    #[test]
    fn retirement_during_pending_add_and_reactivation_require_fresh_snapshot() {
        let mut m = Membership::new(&["A".into()]);
        let dirty = BTreeSet::new();
        m.command(&dirty, 10).unwrap();
        m.control(Some(1), &Control::Subscribed(7)).unwrap();
        m.replace(&["A".into(), "B".into()]);
        m.command(&dirty, 10).unwrap();
        assert!(m.accepts("B"));
        m.replace(&["A".into()]);
        assert!(!m.accepts("B"));
        m.control(Some(2), &Control::Ok).unwrap(); // Late add ACK cannot restore desired.
        let remove = m.command(&dirty, 10).unwrap().unwrap();
        assert_eq!(remove["params"]["action"], "delete_markets");
        m.replace(&["A".into(), "B".into()]);
        m.control(Some(3), &Control::Ok).unwrap();
        assert!(!m.accepts("B"));
        m.command(&dirty, 10).unwrap();
        assert!(m.accepts("B"));
        m.control(Some(999), &Control::Ok).unwrap();
        assert!(m.pending.is_some());
        m.control(Some(4), &Control::Ok).unwrap();
        assert!(m.wire.contains("B"));
    }
    #[test]
    fn incarnation_restart_forces_delete_ack_then_add_even_when_desired_set_is_unchanged() {
        let mut m = Membership::new(&["A".into()]);
        let dirty = BTreeSet::new();
        m.command(&dirty, 10).unwrap();
        m.control(Some(1), &Control::Subscribed(7)).unwrap();
        m.restart("A");
        m.replace(&["A".into()]);
        assert!(!m.accepts("A"));
        assert_eq!(
            m.command(&dirty, 10).unwrap().unwrap()["params"]["action"],
            "delete_markets"
        );
        m.control(Some(2), &Control::Ok).unwrap();
        assert!(!m.accepts("A"));
        assert_eq!(
            m.command(&dirty, 10).unwrap().unwrap()["params"]["action"],
            "add_markets"
        );
        assert!(m.accepts("A"));
    }
    #[test]
    fn snapshot_wait_does_not_block_retirement_or_a_new_incarnation() {
        let mut m = Membership::new(&["A".into(), "B".into()]);
        m.command(&BTreeSet::new(), 10).unwrap();
        m.control(Some(1), &Control::Subscribed(2)).unwrap();
        let dirty = BTreeSet::from(["A".into(), "B".into()]);
        m.command(&dirty, 10).unwrap();
        m.replace(&["B".into()]);
        assert_eq!(
            m.pending.as_ref().unwrap().symbols,
            BTreeSet::from(["B".into()])
        );
        m.restart("B");
        assert!(m.pending.is_none());
        assert_eq!(
            m.command(&dirty, 10).unwrap().unwrap()["params"]["action"],
            "delete_markets"
        );
    }
    #[test]
    fn snapshot_ack_is_not_readiness_and_commands_are_batched() {
        let mut m = Membership::new(&["A".into(), "B".into()]);
        m.command(&BTreeSet::new(), 1).unwrap();
        m.control(Some(1), &Control::Subscribed(2)).unwrap();
        assert_eq!(m.wire.len(), 1);
        m.command(&BTreeSet::new(), 1).unwrap();
        m.control(Some(2), &Control::Ok).unwrap();
        m.command(&BTreeSet::from(["A".into(), "B".into()]), 10)
            .unwrap();
        m.control(Some(3), &Control::Ok).unwrap();
        assert!(m.pending.is_some());
        // A data envelope may echo the same request ID; it is not a second ACK.
        m.control(Some(3), &Control::Other).unwrap();
        assert_eq!(m.pending.as_ref().unwrap().symbols.len(), 2);
        m.snapshot("A");
        assert!(m.pending.is_some());
        m.snapshot("B");
        assert!(m.pending.is_none());
    }
    fn rejected() -> Control {
        Control::Error {
            code: 1,
            message: "rejected".into(),
        }
    }
    #[test]
    fn add_withdrawn_in_flight_leaves_the_outcome_to_its_ack() {
        for acknowledged in [true, false] {
            let mut m = Membership::new(&["A".into()]);
            let dirty = BTreeSet::new();
            m.command(&dirty, 10).unwrap();
            m.control(Some(1), &Control::Subscribed(7)).unwrap();
            m.replace(&["A".into(), "B".into()]);
            m.command(&dirty, 10).unwrap();
            // The wire equals desired here, but the Add may still land.
            m.replace(&["A".into()]);
            assert!(!m.needs_command(&dirty));
            if acknowledged {
                m.control(Some(2), &Control::Ok).unwrap();
                assert!(m.needs_command(&dirty));
                let delete = m.command(&dirty, 10).unwrap().unwrap();
                assert_eq!(delete["params"]["action"], "delete_markets");
                assert_eq!(delete["params"]["market_tickers"], json!(["B"]));
            } else {
                m.control(Some(2), &rejected()).unwrap();
                assert!(!m.needs_command(&dirty));
                assert!(m.command(&dirty, 10).unwrap().is_none());
            }
        }
    }
    #[test]
    fn delete_reinstated_in_flight_leaves_the_outcome_to_its_ack() {
        for acknowledged in [true, false] {
            let mut m = Membership::new(&["A".into(), "B".into()]);
            let dirty = BTreeSet::new();
            m.command(&dirty, 10).unwrap();
            m.control(Some(1), &Control::Subscribed(7)).unwrap();
            m.replace(&["A".into()]);
            m.command(&dirty, 10).unwrap();
            // The wire equals desired here, but the Delete may still land.
            m.replace(&["A".into(), "B".into()]);
            assert!(!m.needs_command(&dirty));
            if acknowledged {
                m.control(Some(2), &Control::Ok).unwrap();
                assert!(m.needs_command(&dirty));
                let add = m.command(&dirty, 10).unwrap().unwrap();
                assert_eq!(add["params"]["action"], "add_markets");
                assert_eq!(add["params"]["market_tickers"], json!(["B"]));
            } else {
                m.control(Some(2), &rejected()).unwrap();
                assert!(!m.needs_command(&dirty));
                assert!(m.command(&dirty, 10).unwrap().is_none());
            }
        }
    }
    #[test]
    fn incarnation_restart_outlives_a_roster_that_returns_to_the_wire() {
        let mut m = Membership::new(&["A".into()]);
        let dirty = BTreeSet::new();
        m.command(&dirty, 10).unwrap();
        m.control(Some(1), &Control::Subscribed(7)).unwrap();
        assert!(!m.needs_command(&dirty));
        m.restart("A");
        assert!(m.needs_command(&dirty));
        m.replace(&["A".into(), "TRANSIENT".into()]);
        m.replace(&["A".into()]);
        // The rosters match, but the A on the wire is the old incarnation.
        assert!(m.needs_command(&dirty));
        assert_eq!(
            m.command(&dirty, 10).unwrap().unwrap()["params"]["action"],
            "delete_markets"
        );
        m.control(Some(2), &Control::Ok).unwrap();
        assert!(m.needs_command(&dirty));
        assert_eq!(
            m.command(&dirty, 10).unwrap().unwrap()["params"]["action"],
            "add_markets"
        );
        m.control(Some(3), &Control::Ok).unwrap();
        assert!(!m.needs_command(&dirty));
    }
    #[tokio::test(start_paused = true)]
    async fn timed_out_add_requests_recovery_only_while_still_desired() {
        for withdrawn in [false, true] {
            let mut m = Membership::new(&["A".into()]);
            let dirty = BTreeSet::new();
            m.command(&dirty, 10).unwrap();
            m.control(Some(1), &Control::Subscribed(7)).unwrap();
            m.replace(&["A".into(), "B".into()]);
            m.command(&dirty, 10).unwrap();
            m.command_written();
            if withdrawn {
                m.replace(&["A".into()]);
            }
            tokio::time::advance(Duration::from_secs(31)).await;
            let recovered = m.recover_timeout(Duration::from_secs(30)).unwrap();
            assert_eq!(recovered.is_empty(), withdrawn);
            assert_eq!(m.needs_command(&dirty), !withdrawn);
            assert_eq!(m.command(&dirty, 10).unwrap().is_some(), !withdrawn);
        }
    }
    #[test]
    fn empty_native_roster_never_requests_a_command() {
        let mut m = Membership::new(&[]);
        let dirty = BTreeSet::new();
        assert!(!m.needs_command(&dirty));
        m.replace(&["A".into()]);
        assert!(m.needs_command(&dirty));
        m.replace(&[]);
        assert!(!m.needs_command(&dirty));
        assert!(m.command(&dirty, 10).unwrap().is_none());
    }
    /// Explores a finite two-symbol model with abstract request IDs and time;
    /// checks the bootstrap latch beyond hand-written transition examples.
    #[tokio::test(start_paused = true)]
    async fn bootstrap_latch_invariants_hold_in_every_reachable_state() {
        let long_ago = Instant::now();
        let timeout = Duration::from_secs(1);
        tokio::time::advance(timeout * 2).await;
        let key = |m: &Membership| {
            format!(
                "{:?}",
                (
                    (&m.desired, &m.wire, &m.restarts),
                    (m.sid.is_some(), m.settled, m.bootstrapped),
                    m.pending
                        .as_ref()
                        .map(|p| (p.kind.clone(), &p.symbols, p.written)),
                )
            )
        };
        let rosters: [Vec<String>; 4] = [
            vec![],
            vec!["A".into()],
            vec!["B".into()],
            vec!["A".into(), "B".into()],
        ];
        let dirty_sets = [
            BTreeSet::new(),
            BTreeSet::from(["A".to_owned(), "B".to_owned()]),
        ];
        let mut seen = std::collections::HashSet::new();
        let mut frontier: Vec<_> = rosters.iter().map(|r| Membership::new(r)).collect();
        while let Some(m) = frontier.pop() {
            if !seen.insert(key(&m)) {
                continue;
            }
            let state = key(&m);
            assert_eq!(
                m.settled,
                m.wire == m.desired && m.restarts.is_disjoint(&m.wire),
                "settled is stale: {state}"
            );
            assert!(
                !m.settled || m.sid.is_none() || m.pending.is_some() || m.bootstrapped,
                "settled acknowledged roster did not latch: {state}"
            );
            assert!(!m.bootstrapped || m.sid.is_some(), "latched: {state}");
            let mut next = vec![];
            let mut then = |change: &mut dyn FnMut(&mut Membership)| {
                let mut after = m.clone();
                change(&mut after);
                next.push(after);
            };
            for roster in &rosters {
                then(&mut |n| n.replace(roster));
            }
            for symbol in ["A", "B"] {
                then(&mut |n| n.restart(symbol));
                then(&mut |n| n.snapshot(symbol));
            }
            then(&mut |n| n.command_written());
            for dirty in &dirty_sets {
                for batch in 1..=2 {
                    let mut probe = m.clone();
                    let command = probe.command(dirty, batch).unwrap();
                    assert!(
                        !m.needs_command(dirty) || command.is_some(),
                        "phantom work: {state}"
                    );
                    then(&mut |n| {
                        n.command(dirty, batch).unwrap();
                    });
                }
            }
            if let Some(p) = &m.pending {
                let ack = || match p.kind {
                    Kind::Subscribe => Control::Subscribed(7),
                    _ => Control::Ok,
                };
                let stale = p.id + 1000;
                for (id, reply) in [
                    (Some(p.id), ack()),
                    (Some(stale), ack()),
                    (None, ack()),
                    (Some(p.id), rejected()),
                    (Some(stale), rejected()),
                    (None, rejected()),
                ] {
                    then(&mut |n| n.control(id, &reply).unwrap());
                }
                then(&mut |n| {
                    if let Some(p) = n.pending.as_mut().filter(|p| p.written) {
                        p.sent = long_ago;
                    }
                    n.recover_timeout(timeout);
                });
            }
            for after in next {
                assert!(
                    !m.bootstrapped || after.bootstrapped,
                    "latch cleared: {state} -> {}",
                    key(&after)
                );
                assert!(
                    m.bootstrapped
                        || !after.bootstrapped
                        || after.settled && after.sid.is_some() && after.pending.is_none(),
                    "latched early: {state} -> {}",
                    key(&after)
                );
                frontier.push(after);
            }
        }
        assert!(seen.len() > 200, "explored only {} states", seen.len());
    }
}
