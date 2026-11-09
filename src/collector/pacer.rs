//! Global command pacing. Its environment is the four names imported from the
//! parent module, so `tests/pacer_loom.rs` and `tests/pacer_hegel.rs` can compile
//! this exact file against an instrumented mutex and a manual clock.

use super::{Duration, Instant, Mutex, MutexGuard};

/// Process-wide conservative command pacing; no sleeping while holding a lock.
///
/// One mutex is the single linearization point for spacing and urgent demand,
/// so a slot decision can never act on a demand count from a different moment.
/// Every decision reads the clock inside that critical section: its time, count
/// and `next` are one snapshot, and admissions are serialized in clock order.
pub(crate) struct Pacer {
    state: Mutex<PacerState>,
    interval: Duration,
}

struct PacerState {
    next: Instant,
    urgent_waiters: usize,
}

/// One registration of urgent demand, which routine admission yields to. It is
/// neither a reservation nor exclusive ownership of any slot, and it carries no
/// identity or position. Dropping it, on any exit path including task
/// cancellation, withdraws exactly this registration. It is not `Clone` and the
/// count can only rise through [`Pacer::urgent`], so drops cannot outnumber
/// registrations.
#[must_use = "dropping the guard withdraws the urgent demand"]
pub(crate) struct UrgentDemand<'a> {
    pacer: &'a Pacer,
}

impl UrgentDemand<'_> {
    /// Same gate and spacing as routine admission, with no yield to other
    /// urgent demand. There is no FIFO or arrival order among urgent pollers:
    /// whichever polls first after an opening wins it, however old its
    /// registration is.
    pub(crate) fn admit(&self) -> bool {
        let mut state = self.pacer.lock();
        let now = Instant::now();
        if now < state.next {
            return false;
        }
        state.next = now + self.pacer.interval;
        true
    }
}

impl Drop for UrgentDemand<'_> {
    fn drop(&mut self) {
        self.pacer.lock().urgent_waiters -= 1;
    }
}

impl Pacer {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            state: Mutex::new(PacerState {
                next: Instant::now(),
                urgent_waiters: 0,
            }),
            interval,
        }
    }
    /// The critical sections only do arithmetic. Tolerating poison keeps
    /// `UrgentDemand::drop` from panicking during an unrelated unwind.
    fn lock(&self) -> MutexGuard<'_, PacerState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Reserve one bounded future slot. Each shard retains at most one health
    /// reservation; reconnects keep this shared timebase instead of resetting it.
    pub(crate) fn reserve(&self) -> Instant {
        let mut state = self.lock();
        let admitted = state.next.max(Instant::now());
        state.next = admitted + self.interval;
        admitted
    }
    /// Routine admission. Keeps the global spacing and additionally yields the
    /// slot to published urgent demand, but only for one interval past the
    /// slot's opening: a registrant that stops polling cannot hold routine
    /// work more than two intervals behind the prior admission.
    pub(crate) fn admit(&self) -> bool {
        let mut state = self.lock();
        let now = Instant::now();
        if now < state.next || (state.urgent_waiters > 0 && now < state.next + self.interval) {
            false
        } else {
            state.next = now + self.interval;
            true
        }
    }
    /// Register urgent demand; poll for a slot through the returned guard.
    pub(crate) fn urgent(&self) -> UrgentDemand<'_> {
        self.lock().urgent_waiters += 1;
        UrgentDemand { pacer: self }
    }
    #[cfg(test)]
    pub(crate) fn urgent_waiters(&self) -> usize {
        self.lock().urgent_waiters
    }
}
