//! Deterministic command-admission reproductions. The scheduling model uses
//! actual Pacer and Membership, immediate writes/ACKs, 100ms Source health polls
//! and the real 500ms local command floor. It isolates admission, not the wire
//! or Source/Owner loop, and is not a replay of the live packet schedule.
use super::{Pacer, UrgentDemand};
use crate::{
    exchange::{Control, MembershipStrategy, polymarket::Polymarket},
    membership::Membership,
};
use std::{collections::BTreeSet, time::Duration};
use tokio::time::Instant;

const INTERVAL: Duration = Duration::from_millis(100);

fn issue_and_ack(membership: &mut Membership) {
    issue_batch_and_ack(membership, 250);
}

fn issue_batch_and_ack(membership: &mut Membership, batch: usize) {
    let command = membership
        .command(&BTreeSet::new(), batch)
        .unwrap()
        .expect("ready membership must have an actual command");
    let id = command["id"].as_u64().unwrap();
    let first = membership.sid.is_none();
    membership.command_written();
    membership
        .control(
            Some(id),
            &if first {
                Control::Subscribed(7)
            } else {
                Control::Ok
            },
        )
        .unwrap();
}

#[test]
fn withdrawn_unwritten_add_does_not_keep_requesting_global_slots() {
    let mut membership = Membership::new(&["A".into()]);
    issue_and_ack(&mut membership);
    membership.replace(&["A".into(), "TRANSIENT".into()]);
    assert!(membership.needs_command(&BTreeSet::new()));
    // Registry withdraws the new asset before its Add was admitted. The real
    // wire roster is already exactly the new desired roster, with no pending
    // command or incarnation restart to reconcile.
    membership.replace(&["A".into()]);
    assert!(membership.pending.is_none());
    assert_eq!(membership.wire, membership.desired);
    assert!(
        !membership.needs_command(&BTreeSet::new()),
        "an empty membership operation must not compete for global quota"
    );
}

struct Client<'p> {
    membership: Membership,
    next_command: Instant,
    issued: usize,
    demand: Option<UrgentDemand<'p>>,
}

impl<'p> Client<'p> {
    fn new(membership: Membership, next_command: Instant) -> Self {
        Self {
            membership,
            next_command,
            issued: 0,
            demand: None,
        }
    }

    /// Mirrors `Session::admit_command`: urgent demand only while a real
    /// bootstrap command is locally due, persisted across denied polls.
    fn poll(&mut self, pacer: &'p Pacer) {
        let due =
            Instant::now() >= self.next_command && self.membership.needs_command(&BTreeSet::new());
        let urgent = due && self.membership.bootstrapping();
        if !urgent {
            self.demand = None;
        }
        if !due {
            return;
        }
        let admitted = if urgent {
            self.demand.get_or_insert_with(|| pacer.urgent()).admit()
        } else {
            pacer.admit()
        };
        if admitted {
            self.demand = None;
            issue_and_ack(&mut self.membership);
            self.issued += 1;
            self.next_command = Instant::now() + Duration::from_millis(500);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn reconnect_bootstrap_is_not_starved_by_a_universe_membership_burst() {
    let pacer = Pacer::new(INTERVAL);
    let start = Instant::now();
    // 83 shards * 2 replicas: one is reconnecting, 165 already admitted
    // sessions each reconcile one removal and one addition. The live event
    // had 2,632 additions / 2,648 retirements distributed across 83 shards.
    // Two <=250-book commands/session are a favorable lower bound on work.
    let mut routine: Vec<_> = (0..165)
        .map(|i| {
            let mut membership = Membership::new(&[format!("KEEP-{i}"), format!("OLD-{i}")]);
            issue_and_ack(&mut membership);
            membership.replace(&[format!("KEEP-{i}"), format!("NEW-{i}")]);
            Client::new(membership, start)
        })
        .collect();
    let symbols: Vec<_> = (0..2250).map(|i| format!("BOOT-{i:04}")).collect();
    let mut bootstrap = Client::new(Membership::new(&symbols), start);
    let mut bootstrap_finished = None;
    while start.elapsed() < Duration::from_secs(30) {
        // A legal repeatable ordering of aligned 100ms Source health polls.
        // Routine sessions have a publisher; this reconnect has a deadline.
        for client in &mut routine {
            client.poll(&pacer);
        }
        bootstrap.poll(&pacer);
        if bootstrap_finished.is_none() && bootstrap.membership.wire.len() == symbols.len() {
            bootstrap_finished = Some(start.elapsed());
        }
        tokio::time::advance(INTERVAL).await;
    }
    eprintln!(
        "admission isolation: normal_commands={}, bootstrap_commands={}, elapsed={:?}",
        routine.iter().map(|c| c.issued).sum::<usize>(),
        bootstrap.issued,
        start.elapsed()
    );
    assert_eq!(
        bootstrap.membership.wire.len(),
        symbols.len(),
        "reconnecting session must finish its nine membership batches inside the unchanged 30s rotation bound under this bounded routine burst"
    );
    assert_eq!(bootstrap.issued, 9);
    let finished = bootstrap_finished.unwrap();
    assert!(
        finished <= Duration::from_secs(6),
        "bootstrap took {finished:?}"
    );
    while routine.iter().map(|c| c.issued).sum::<usize>() < 330
        && start.elapsed() < Duration::from_secs(36)
    {
        for client in &mut routine {
            client.poll(&pacer);
        }
        tokio::time::advance(INTERVAL).await;
    }
    let issued = routine.iter().map(|c| c.issued).sum::<usize>();
    eprintln!(
        "class-aware admission: bootstrap_finished={finished:?}, routine_commands={issued}, routine_finished={:?}",
        start.elapsed()
    );
    assert_eq!(issued, 330, "routine burst must still drain");
    assert_eq!(pacer.urgent_waiters(), 0);
}

#[tokio::test(start_paused = true)]
async fn routine_only_admission_matches_the_previous_timestamp_rule() {
    let pacer = Pacer::new(INTERVAL);
    let mut model_next = Instant::now();
    for step in [0, 1, 99, 100, 7, 250, 100, 1, 1000, 33, 67, 100, 99, 1] {
        tokio::time::advance(Duration::from_millis(step)).await;
        let now = Instant::now();
        let expected = now >= model_next;
        if expected {
            model_next = now + INTERVAL;
        }
        assert_eq!(pacer.admit(), expected, "step {step}");
    }
}

#[tokio::test(start_paused = true)]
async fn unpolled_urgent_registrant_delays_routine_by_at_most_one_extra_interval() {
    let pacer = Pacer::new(INTERVAL);
    assert!(pacer.admit());
    let _stale = pacer.urgent();
    for _ in 0..5 {
        tokio::time::advance(INTERVAL).await;
        assert!(!pacer.admit(), "routine yields the opening to live demand");
        tokio::time::advance(INTERVAL - Duration::from_millis(1)).await;
        assert!(!pacer.admit(), "the yield lasts until two intervals");
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(pacer.admit(), "the aged slot must reach routine work");
    }
}

#[tokio::test(start_paused = true)]
async fn attended_demand_takes_the_opening_slot_then_routine_resumes() {
    let pacer = Pacer::new(INTERVAL);
    assert!(pacer.admit());
    let demand = pacer.urgent();
    tokio::time::advance(INTERVAL).await;
    assert!(!pacer.admit(), "routine polled first must yield");
    assert!(demand.admit());
    assert!(!demand.admit(), "one slot is one admission");
    drop(demand);
    assert!(!pacer.admit(), "spacing applies across classes");
    tokio::time::advance(INTERVAL).await;
    assert!(pacer.admit(), "without demand routine admits on time");
}

#[tokio::test(start_paused = true)]
async fn competing_urgent_demands_share_slots_and_keep_global_spacing() {
    let pacer = Pacer::new(INTERVAL);
    let (first, second) = (pacer.urgent(), pacer.urgent());
    assert!(first.admit());
    assert!(!second.admit());
    tokio::time::advance(INTERVAL).await;
    assert!(second.admit());
    assert!(!first.admit());
}

#[tokio::test(start_paused = true)]
async fn late_polls_keep_spacing_and_earn_no_catch_up_slots() {
    let pacer = Pacer::new(INTERVAL);
    let demand = pacer.urgent();
    assert!(demand.admit());
    tokio::time::advance(INTERVAL * 50).await;
    assert!(demand.admit());
    assert!(!demand.admit(), "a stalled poller earns no burst");
    assert!(!pacer.admit());
    tokio::time::advance(INTERVAL - Duration::from_millis(1)).await;
    assert!(!demand.admit(), "the next slot is anchored on actual time");
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(demand.admit());
    drop(demand);
    tokio::time::advance(INTERVAL * 7).await;
    assert!(pacer.admit());
    assert!(!pacer.admit(), "routine has no catch-up either");
}

#[tokio::test(start_paused = true)]
async fn dropping_a_guard_withdraws_only_its_own_demand_and_never_underflows() {
    let pacer = Pacer::new(INTERVAL);
    assert!(pacer.admit());
    let (first, second) = (pacer.urgent(), pacer.urgent());
    assert_eq!(pacer.urgent_waiters(), 2);
    drop(first);
    assert_eq!(pacer.urgent_waiters(), 1);
    tokio::time::advance(INTERVAL).await;
    assert!(
        !pacer.admit(),
        "routine still yields to the surviving demand"
    );
    drop(second);
    assert_eq!(pacer.urgent_waiters(), 0);
    assert!(pacer.admit(), "no demand remains to yield to");
    for _ in 0..1000 {
        drop((0..3).map(|_| pacer.urgent()).collect::<Vec<_>>());
    }
    assert_eq!(pacer.urgent_waiters(), 0);
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_task_that_holds_demand_withdraws_it() {
    let pacer = std::sync::Arc::new(Pacer::new(INTERVAL));
    let task = tokio::spawn({
        let pacer = pacer.clone();
        async move {
            let _demand = pacer.urgent();
            std::future::pending::<()>().await;
        }
    });
    tokio::task::yield_now().await;
    assert_eq!(pacer.urgent_waiters(), 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(pacer.urgent_waiters(), 0);
}

#[tokio::test(start_paused = true)]
async fn health_recovery_reservations_ignore_urgent_demand() {
    let pacer = Pacer::new(INTERVAL);
    let start = Instant::now();
    let _demand = pacer.urgent();
    assert_eq!(pacer.reserve(), start);
    assert_eq!(pacer.reserve(), start + INTERVAL);
}

fn bootstrap_of(symbols: &[&str]) -> Membership {
    Membership::new(&symbols.iter().map(|s| s.to_string()).collect::<Vec<_>>())
}

fn step(membership: &mut Membership) {
    issue_batch_and_ack(membership, 1);
}

#[test]
fn native_bootstrap_ends_once_on_the_first_acknowledged_roster() {
    let mut m = bootstrap_of(&["A", "B", "C"]);
    assert!(m.bootstrapping());
    step(&mut m);
    assert!(m.bootstrapping(), "one of three batches is not a roster");
    step(&mut m);
    assert!(m.bootstrapping());
    step(&mut m);
    assert!(!m.bootstrapping());
    // Catalog churn and incarnation restarts after that stay routine.
    m.replace(&["A".into(), "B".into(), "C".into(), "D".into()]);
    assert!(m.needs_command(&BTreeSet::new()));
    assert!(!m.bootstrapping());
    step(&mut m);
    m.restart("A");
    assert!(m.needs_command(&BTreeSet::new()));
    assert!(!m.bootstrapping());
}

#[test]
fn bootstrap_is_not_ended_by_a_roster_match_that_has_no_acknowledgement() {
    // Settled only because nothing is desired yet; no Subscribe was ACKed.
    let mut empty = bootstrap_of(&[]);
    assert!(empty.bootstrapping());
    empty.replace(&["A".into()]);
    assert!(empty.bootstrapping());
    empty.replace(&[]);
    assert!(empty.bootstrapping());
    // A pending Add can still change the wire when the roster matches it.
    let mut m = bootstrap_of(&["A", "B"]);
    step(&mut m);
    m.command(&BTreeSet::new(), 1).unwrap().unwrap();
    m.command_written();
    m.replace(&["A".into()]);
    assert_eq!(m.wire, m.desired);
    assert!(m.bootstrapping(), "the Add is still in flight");
    let id = m.pending.as_ref().unwrap().id;
    m.control(Some(id), &Control::Ok).unwrap();
    assert!(m.bootstrapping(), "the landed Add must still be removed");
    step(&mut m);
    assert!(!m.bootstrapping());
}

#[test]
fn rejected_subscribe_stays_bootstrapping() {
    let mut m = bootstrap_of(&["A"]);
    m.command(&BTreeSet::new(), 1).unwrap().unwrap();
    m.command_written();
    let id = m.pending.as_ref().unwrap().id;
    let rejected = Control::Error {
        code: 27,
        message: "rate limited".into(),
    };
    m.control(Some(id), &rejected).unwrap();
    assert!(m.bootstrapping());
    assert!(m.needs_command(&BTreeSet::new()));
}

#[test]
fn fresh_session_membership_is_never_classified_as_bootstrapping() {
    let mut m = Membership::with_strategy(&["A".into()], MembershipStrategy::FreshSession);
    assert!(!m.bootstrapping());
    m.command_with(&Polymarket, &BTreeSet::new(), 1)
        .unwrap()
        .unwrap();
    m.command_written();
    assert!(!m.bootstrapping());
}

// Codex post-handoff additions: classify only genuinely pending work, and
// release global demand after the watched assignment is withdrawn before dial.
#[test]
fn rejected_partial_bootstrap_add_latches_the_already_acknowledged_roster() {
    let mut m = bootstrap_of(&["A", "B"]);
    step(&mut m);
    let command = m.command(&BTreeSet::new(), 1).unwrap().unwrap();
    m.command_written();
    m.replace(&["A".into()]);
    assert!(m.bootstrapping(), "the Add is still in flight");
    m.control(
        command["id"].as_u64(),
        &Control::Error {
            code: 27,
            message: "rejected".into(),
        },
    )
    .unwrap();
    assert!(m.pending.is_none());
    assert_eq!(m.wire, m.desired);
    assert!(!m.needs_command(&BTreeSet::new()));
    assert!(
        !m.bootstrapping(),
        "the only remaining roster was really ACKed; the rejected Add is now gone"
    );
    m.replace(&["A".into(), "C".into()]);
    assert!(
        !m.bootstrapping(),
        "later metadata additions must stay routine"
    );
}

fn rejection() -> Control {
    Control::Error {
        code: 27,
        message: "rejected".into(),
    }
}

/// `step` for one symbol, leaving the follow-up command pending and written.
fn in_flight(m: &mut Membership) -> u64 {
    let id = m.command(&BTreeSet::new(), 1).unwrap().unwrap()["id"]
        .as_u64()
        .unwrap();
    m.command_written();
    id
}

#[test]
fn rejected_add_with_work_remaining_stays_bootstrapping() {
    let mut m = bootstrap_of(&["A", "B"]);
    step(&mut m);
    let id = in_flight(&mut m);
    m.control(Some(id), &rejection()).unwrap();
    assert!(m.pending.is_none());
    assert!(m.needs_command(&BTreeSet::new()), "B was never added");
    assert!(m.bootstrapping());
}

#[test]
fn rejected_delete_after_reinstatement_latches_the_unchanged_wire() {
    let mut m = bootstrap_of(&["A", "B", "C"]);
    issue_batch_and_ack(&mut m, 2);
    m.replace(&["A".into()]);
    let id = in_flight(&mut m);
    m.replace(&["A".into(), "B".into()]);
    assert_eq!(m.wire, m.desired);
    assert!(m.bootstrapping(), "the Delete may still land");
    m.control(Some(id), &rejection()).unwrap();
    assert!(!m.needs_command(&BTreeSet::new()));
    assert!(!m.bootstrapping(), "the rejected Delete left B in place");
}

#[test]
fn rejection_cannot_latch_a_restarted_incarnation() {
    let mut m = bootstrap_of(&["A", "B"]);
    step(&mut m);
    let id = in_flight(&mut m);
    m.replace(&["A".into()]);
    m.restart("A");
    m.control(Some(id), &rejection()).unwrap();
    assert!(m.pending.is_none());
    assert_eq!(m.wire, m.desired);
    assert!(m.needs_command(&BTreeSet::new()));
    assert!(
        m.bootstrapping(),
        "the A on the wire is the old incarnation"
    );
}

#[test]
fn only_the_matching_rejection_resolves_the_pending_command() {
    let mut m = bootstrap_of(&["A", "B"]);
    step(&mut m);
    let id = in_flight(&mut m);
    m.replace(&["A".into()]);
    for stale in [None, Some(id - 1), Some(id + 1)] {
        m.control(stale, &rejection()).unwrap();
        assert!(m.pending.is_some(), "{stale:?} names no live command");
        assert!(m.bootstrapping());
    }
    m.control(Some(id), &rejection()).unwrap();
    assert!(!m.bootstrapping());
    // The rejected command's late ACK has nothing left to complete.
    m.control(Some(id), &Control::Ok).unwrap();
    assert_eq!(m.wire, BTreeSet::from(["A".to_owned()]));
    assert!(!m.needs_command(&BTreeSet::new()));
}

#[test]
fn rejected_subscribe_on_an_emptied_roster_has_no_sid_to_latch() {
    let mut m = bootstrap_of(&["A"]);
    let id = in_flight(&mut m);
    m.replace(&[]);
    m.control(Some(id), &rejection()).unwrap();
    assert!(m.pending.is_none());
    assert_eq!(m.wire, m.desired);
    assert!(m.bootstrapping(), "no Subscribe was ever acknowledged");
    m.replace(&["B".into()]);
    assert!(m.needs_command(&BTreeSet::new()) && m.bootstrapping());
}

#[test]
fn fresh_session_rejections_stay_outside_the_native_latch() {
    let mut m = Membership::with_strategy(&["A".into()], MembershipStrategy::FreshSession);
    m.command_with(&Polymarket, &BTreeSet::new(), 1)
        .unwrap()
        .unwrap();
    m.control(Some(1), &rejection()).unwrap();
    assert!(m.pending.is_some(), "FreshSession has no ACK channel");
    m.command_written();
    m.control(Some(1), &rejection()).unwrap();
    assert!(m.sid.is_none() && m.wire.is_empty());
    assert!(!m.bootstrapping());
}

#[tokio::test(start_paused = true)]
async fn urgent_pollers_have_no_registration_order() {
    let pacer = Pacer::new(INTERVAL);
    assert!(pacer.admit());
    let (older, newer) = (pacer.urgent(), pacer.urgent());
    tokio::time::advance(INTERVAL).await;
    assert!(newer.admit(), "the first poll after the opening wins it");
    assert!(!older.admit(), "an older registration confers no order");
    assert_eq!(pacer.urgent_waiters(), 2, "admission withdraws neither");
}
