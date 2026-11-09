//! Same production Pacer, modeled mutex/clock only. No Tokio/IO is modeled.
//! Each model uses <=3 threads including main; no preemption/permutation/time
//! pruning. The clock is monotonic, discrete, and has a tiny finite range.
use loom::{
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst},
        mpsc,
    },
    thread,
};
use std::{cell::Cell, ops::Add, time::Duration};

loom::lazy_static! {
    static ref CLOCK: AtomicU64 = AtomicU64::new(0);
    static ref REGISTERED: AtomicBool = AtomicBool::new(false);
}
loom::thread_local! {
    static LAST_NOW: Cell<u64> = Cell::new(0);
    static REG_AT_NOW: Cell<bool> = Cell::new(false);
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Instant(u64);
impl Instant {
    fn now() -> Self {
        let tick = CLOCK.load(SeqCst);
        LAST_NOW.with(|last| last.set(tick));
        REG_AT_NOW.with(|last| last.set(REGISTERED.load(SeqCst)));
        Self(tick)
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
const INTERVAL: Duration = Duration::from_nanos(1);
fn model(check: impl Fn() + Send + Sync + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.max_threads = 3;
    builder.max_branches = 1000;
    builder.preemption_bound = None;
    builder.max_permutations = None;
    builder.max_duration = None;
    builder.check(check);
}
fn assert_spacing(mut ticks: Vec<u64>) {
    ticks.sort_unstable();
    assert!(
        ticks.windows(2).all(|pair| pair[1] > pair[0]),
        "double claim: {ticks:?}"
    );
}

#[test]
fn routine_claims_at_one_tick_are_unique() {
    model(|| {
        let pacer = Arc::new(Pacer::new(INTERVAL));
        let admitted = Arc::new(AtomicUsize::new(0));
        let spawn = || {
            let (pacer, admitted) = (pacer.clone(), admitted.clone());
            thread::spawn(move || {
                if pacer.admit() {
                    admitted.fetch_add(1, SeqCst);
                }
            })
        };
        let a = spawn();
        let b = spawn();
        a.join().unwrap();
        b.join().unwrap();
        assert_eq!(admitted.load(SeqCst), 1);
        assert_eq!(pacer.urgent_waiters(), 0);
    });
}

#[test]
fn reserve_and_urgent_share_the_same_quota() {
    model(|| {
        let pacer = Arc::new(Pacer::new(INTERVAL));
        let claims = Arc::new(Mutex::new(Vec::new()));
        let (p, c) = (pacer.clone(), claims.clone());
        let a = thread::spawn(move || c.lock().unwrap().push(p.reserve().0));
        let (p, c) = (pacer.clone(), claims.clone());
        let b = thread::spawn(move || {
            let demand = p.urgent();
            if demand.admit() {
                c.lock().unwrap().push(LAST_NOW.with(Cell::get));
            }
        });
        a.join().unwrap();
        b.join().unwrap();
        assert_eq!(pacer.urgent_waiters(), 0);
        CLOCK.store(2, SeqCst);
        assert!(pacer.admit());
        claims.lock().unwrap().push(LAST_NOW.with(Cell::get));
        assert_spacing(claims.lock().unwrap().clone());
    });
}

#[test]
fn registration_before_the_claim_clock_read_blocks_routine() {
    model(|| {
        let pacer = Arc::new(Pacer::new(INTERVAL));
        let (done, completed) = mpsc::channel();
        let p = pacer.clone();
        let a = thread::spawn(move || {
            let demand = p.urgent();
            REGISTERED.store(true, SeqCst);
            completed.recv().unwrap(); // keep this registration alive through the claim
            drop(demand);
        });
        let p = pacer.clone();
        let b = thread::spawn(move || {
            let admitted = p.admit();
            // The marker is published AFTER urgent() returns and cannot be
            // revoked until this call finishes. Sampling it in Instant::now,
            // inside the production lock, establishes a valid one-way witness.
            if REG_AT_NOW.with(Cell::get) {
                assert!(
                    !admitted,
                    "routine used a pre-registration check for a later claim"
                );
            }
            done.send(()).unwrap();
        });
        a.join().unwrap();
        b.join().unwrap();
        assert_eq!(pacer.urgent_waiters(), 0);
    });
}

#[test]
fn old_guard_drop_cannot_erase_a_new_registration() {
    model(|| {
        let pacer = Arc::new(Pacer::new(INTERVAL));
        let (ready, started) = mpsc::channel();
        let p = pacer.clone();
        let a = thread::spawn(move || {
            let mut old = Some(p.urgent());
            ready.send(()).unwrap();
            drop(old.take()); // cancellation-equivalent Drop, not Tokio cancellation
        });
        let p = pacer.clone();
        let b = thread::spawn(move || {
            started.recv().unwrap();
            let new = p.urgent(); // races the old Drop; includes 1->0->1
            a.join().unwrap();
            assert_eq!(p.urgent_waiters(), 1, "old Drop erased the new guard");
            assert!(!p.admit(), "a live new guard lost priority");
            assert!(new.admit());
            assert!(!new.admit());
            drop(new);
        });
        b.join().unwrap();
        assert_eq!(pacer.urgent_waiters(), 0);
    });
}

#[test]
fn clock_advance_racing_admission_and_reservation_preserves_spacing() {
    model(|| {
        let pacer = Arc::new(Pacer::new(INTERVAL));
        let claims = Arc::new(Mutex::new(Vec::new()));
        let (p, c) = (pacer.clone(), claims.clone());
        let a = thread::spawn(move || {
            for _ in 0..2 {
                if p.admit() {
                    c.lock().unwrap().push(LAST_NOW.with(Cell::get));
                }
                thread::yield_now();
            }
        });
        let (p, c) = (pacer.clone(), claims.clone());
        let b = thread::spawn(move || {
            CLOCK.store(1, SeqCst);
            c.lock().unwrap().push(p.reserve().0);
        });
        a.join().unwrap();
        b.join().unwrap();
        assert_spacing(claims.lock().unwrap().clone());
        assert_eq!(pacer.urgent_waiters(), 0);
    });
}

#[test]
fn unattended_demand_has_only_the_documented_aged_slot_escape() {
    model(|| {
        let pacer = Pacer::new(INTERVAL);
        let demand = pacer.urgent();
        assert!(!pacer.admit());
        CLOCK.store(1, SeqCst);
        assert!(pacer.admit());
        assert!(!demand.admit());
        CLOCK.store(2, SeqCst);
        assert!(demand.admit());
        drop(demand);
        assert_eq!(pacer.urgent_waiters(), 0);
    });
}
