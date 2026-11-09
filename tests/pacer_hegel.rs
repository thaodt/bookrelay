//! Generated bounded lifecycle traces of the SAME production Pacer.
//! This is property testing and shrinking, not a weak-memory scheduler.
use hegel::{TestCase, generators as gs};
use std::{
    ops::Add,
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering::SeqCst},
    },
    time::Duration,
};
static CLOCK: AtomicU64 = AtomicU64::new(0);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Instant(u64);
impl Instant {
    fn now() -> Self {
        Self(CLOCK.load(SeqCst))
    }
}
impl Add<Duration> for Instant {
    type Output = Self;
    fn add(self, duration: Duration) -> Self {
        Self(
            self.0
                .checked_add(duration.as_nanos().try_into().unwrap())
                .unwrap(),
        )
    }
}
#[path = "../src/collector/pacer.rs"]
mod pacer;
use pacer::Pacer;
const INTERVAL: Duration = Duration::from_nanos(2);

#[hegel::test(test_cases = 500, derandomize = true)]
fn generated_guard_lifecycles_preserve_accounting_and_quota(tc: TestCase) {
    // One property test in this binary: no parallel test shares this clock.
    CLOCK.store(0, SeqCst);
    let pacer = Pacer::new(INTERVAL);
    let mut guards = [None, None, None];
    let mut claims = Vec::<u64>::new();
    // Force both overlap and 1->0->1 before generated events; dropping an old
    // guard must not erase an independently held new guard.
    guards[0] = Some(pacer.urgent());
    guards[1] = Some(pacer.urgent());
    drop(guards[0].take());
    assert_eq!(pacer.urgent_waiters(), 1);
    assert!(!pacer.admit());
    drop(guards[1].take());
    guards[2] = Some(pacer.urgent());
    assert_eq!(pacer.urgent_waiters(), 1);
    drop(guards[2].take());

    let actions = tc.draw(gs::vecs(gs::integers::<u8>()).max_size(64));
    for action in actions {
        let slot = usize::from(action % 3);
        match action / 3 % 7 {
            0 => {
                // Replacement withdraws exactly the old registration first.
                drop(guards[slot].take());
                guards[slot] = Some(pacer.urgent());
            }
            1 => drop(guards[slot].take()),
            2 => {
                if pacer.admit() {
                    claims.push(CLOCK.load(SeqCst));
                }
            }
            3 => {
                if let Some(guard) = &guards[slot]
                    && guard.admit()
                {
                    claims.push(CLOCK.load(SeqCst));
                }
            }
            4 => claims.push(pacer.reserve().0),
            5 => {
                CLOCK.fetch_add(u64::from(action % 5), SeqCst);
            }
            6 => {
                // Isolate an unaged opening with controlled, known history.
                // Other traces exercise arbitrary prior reservations.
                let opening = Pacer::new(INTERVAL);
                let guard = opening.urgent();
                assert!(!opening.admit());
                CLOCK.fetch_add(2, SeqCst);
                assert!(opening.admit());
                assert!(!guard.admit());
                drop(guard);
                assert_eq!(opening.urgent_waiters(), 0);
            }
            _ => unreachable!(),
        }
        assert_eq!(
            pacer.urgent_waiters(),
            guards.iter().filter(|g| g.is_some()).count()
        );
        assert!(
            claims.windows(2).all(|p| p[1] >= p[0] + 2),
            "nonmonotonic or overlapping slots: {claims:?}"
        );
    }
    drop(guards);
    assert_eq!(pacer.urgent_waiters(), 0);
    // Once all registrations end, the actual timebase remains conserved.
    let at = pacer.reserve();
    CLOCK.store(at.0 + 2, SeqCst);
    assert!(pacer.admit());
}
