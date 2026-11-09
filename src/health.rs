//! Bounded per-symbol activity accounting. Four completed time buckets form a
//! window; the fifth bucket holds current arrivals. No per-event queue or timer.
use anyhow::ensure;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Debug)]
pub struct Options {
    pub window: Duration,
    pub warmup: Duration,
    pub cooldown: Duration,
    pub expected_points: u64,
    pub minimum_peer_points: u64,
    pub minimum_peer_percent: u8,
    pub max_resubscriptions: u32,
    pub rotation_interval: Duration,
    pub rotation_timeout: Duration,
    pub ack_timeout: Duration,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            window: Duration::from_secs(60),
            warmup: Duration::from_secs(120),
            cooldown: Duration::from_secs(120),
            expected_points: 0,
            minimum_peer_points: 5,
            minimum_peer_percent: 20,
            max_resubscriptions: 3,
            rotation_interval: Duration::from_secs(1800),
            rotation_timeout: Duration::from_secs(30),
            ack_timeout: Duration::from_secs(30),
        }
    }
}
impl Options {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.window.subsec_nanos() == 0
                && (4..=86400).contains(&self.window.as_secs())
                && self.window.as_secs().is_multiple_of(4),
            "health window must be whole seconds, divisible by four, in 4..=86400"
        );
        ensure!(
            self.warmup >= self.window,
            "health warmup must cover a window"
        );
        ensure!(
            self.cooldown >= self.window,
            "health cooldown must cover a window"
        );
        ensure!(
            self.warmup <= Duration::from_secs(7 * 86400)
                && self.cooldown <= Duration::from_secs(7 * 86400),
            "health deadlines exceed seven days"
        );
        ensure!(
            self.minimum_peer_points > 0
                && (1..=100).contains(&self.minimum_peer_percent)
                && self.max_resubscriptions > 0,
            "invalid health peer threshold or retry budget"
        );
        ensure!(
            self.rotation_interval <= Duration::from_secs(7 * 86400)
                && !self.rotation_timeout.is_zero()
                && self.rotation_timeout <= Duration::from_secs(300),
            "invalid rotation interval/timeout"
        );
        ensure!(
            !self.ack_timeout.is_zero() && self.ack_timeout <= Duration::from_secs(300),
            "invalid ACK timeout"
        );
        Ok(())
    }
    pub(crate) fn bucket_seconds(&self) -> u64 {
        self.window.as_secs() / 4
    }
}

#[derive(Clone, Default)]
struct Bucket {
    epoch: u64,
    count: u64,
}
#[derive(Clone, Default)]
pub(crate) struct RateWindow {
    buckets: [Bucket; 5],
}
impl RateWindow {
    pub(crate) fn record(&mut self, epoch: u64) {
        let bucket = &mut self.buckets[(epoch % 5) as usize];
        if bucket.epoch != epoch {
            *bucket = Bucket { epoch, count: 0 };
        }
        bucket.count = bucket.count.saturating_add(1);
    }
    pub(crate) fn completed(&self, epoch: u64) -> u64 {
        self.buckets
            .iter()
            .filter(|b| b.epoch < epoch && epoch - b.epoch <= 4)
            .fold(0u64, |n, b| n.saturating_add(b.count))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Resubscribe,
    Escalate,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct Decision {
    pub action: Action,
    pub points: u64,
    pub peer_points: u64,
    pub baseline: u64,
    pub attempt: u32,
    pub reason: &'static str,
}

pub(crate) struct Activity {
    pub window: RateWindow,
    initialized: Option<Instant>,
    next_attempt: Instant,
    attempts: u32,
    last_evaluation: Option<u64>,
    baseline: u64,
    pub recovering: bool,
}
impl Default for Activity {
    fn default() -> Self {
        Self {
            window: RateWindow::default(),
            initialized: None,
            next_attempt: Instant::now(),
            attempts: 0,
            last_evaluation: None,
            baseline: 0,
            recovering: false,
        }
    }
}
impl Activity {
    pub(crate) fn initialized(&mut self, now: Instant) {
        if self.initialized.is_none() {
            self.initialized = Some(now);
            self.recovering = false;
        }
    }
    pub(crate) fn invalidate(&mut self) {
        self.initialized = None;
        self.window = RateWindow::default();
        self.last_evaluation = None;
    }
    pub(crate) fn evaluate(
        &mut self,
        epoch: u64,
        now: Instant,
        peer: Option<u64>,
        options: &Options,
    ) -> Option<Decision> {
        let since = self.initialized?;
        if self.recovering
            || now.duration_since(since) < options.warmup
            || self.last_evaluation == Some(epoch)
        {
            return None;
        }
        self.last_evaluation = Some(epoch);
        let points = self.window.completed(epoch);
        let previous_baseline = self.baseline;
        self.baseline = ((u128::from(self.baseline) * 3 + u128::from(points)) / 4) as u64;
        if rate_reason(points, peer, options).is_none() && points > 0 {
            self.attempts = 0;
        }
        let mut decision = self.preview(epoch, now, peer, options)?;
        decision.baseline = previous_baseline;
        Some(decision)
    }
    /// Revalidate delayed proposals against current data before side effects.
    pub(crate) fn preview(
        &self,
        epoch: u64,
        now: Instant,
        peer: Option<u64>,
        options: &Options,
    ) -> Option<Decision> {
        let since = self.initialized?;
        if self.recovering || now.duration_since(since) < options.warmup || now < self.next_attempt
        {
            return None;
        }
        let points = self.window.completed(epoch);
        let reason = rate_reason(points, peer, options)?;
        let action = if self.attempts >= options.max_resubscriptions {
            Action::Escalate
        } else {
            Action::Resubscribe
        };
        Some(Decision {
            action,
            points,
            peer_points: peer.unwrap_or(0),
            baseline: self.baseline,
            attempt: self.attempts.saturating_add(1),
            reason,
        })
    }
    /// Advance the retry budget only after the owner accepts the recovery
    /// intent. A bounded batch may defer proposals without consuming attempts.
    pub(crate) fn admitted(&mut self, now: Instant, options: &Options) {
        self.attempts = self.attempts.saturating_add(1);
        self.next_attempt = now + options.cooldown;
        self.recovering = true;
    }
}

fn rate_reason(points: u64, peer: Option<u64>, options: &Options) -> Option<&'static str> {
    if peer.is_some_and(|n| {
        n >= options.minimum_peer_points
            && u128::from(points) * 100 < u128::from(n) * u128::from(options.minimum_peer_percent)
    }) {
        Some("peer activity deficit")
    } else if options.expected_points > 0 && points < options.expected_points {
        Some("configured activity floor")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn options() -> Options {
        Options {
            window: Duration::from_secs(4),
            warmup: Duration::from_secs(4),
            cooldown: Duration::from_secs(4),
            ..Options::default()
        }
    }
    #[test]
    fn completed_windows_keep_current_bucket_and_expire_idle_history() {
        let mut w = RateWindow::default();
        for epoch in 0..5 {
            for _ in 0..3 {
                w.record(epoch);
            }
        }
        assert_eq!(w.completed(4), 12);
        assert_eq!(w.completed(5), 12);
        w.record(5);
        assert_eq!(w.completed(6), 10);
        assert_eq!(w.completed(10), 0);
    }
    #[tokio::test(start_paused = true)]
    async fn quiet_peers_and_burst_then_idle_do_not_trigger_recovery() {
        let mut a = Activity::default();
        let options = options();
        let start = Instant::now();
        a.initialized(start);
        for _ in 0..100 {
            a.window.record(0);
        }
        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(a.evaluate(4, Instant::now(), Some(100), &options).is_none());
        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(a.evaluate(8, Instant::now(), Some(0), &options).is_none());
        assert!(a.evaluate(9, Instant::now(), None, &options).is_none());
    }
    #[tokio::test(start_paused = true)]
    async fn hot_peer_deficit_is_cooldown_limited_and_escalates_after_retry_budget() {
        let mut a = Activity::default();
        let options = options();
        a.initialized(Instant::now());
        for attempt in 1..=4 {
            tokio::time::advance(Duration::from_secs(4)).await;
            let d = a
                .evaluate(attempt * 4, Instant::now(), Some(100), &options)
                .unwrap();
            assert_eq!(d.attempt, attempt as u32);
            assert_eq!(
                d.action,
                if attempt <= 3 {
                    Action::Resubscribe
                } else {
                    Action::Escalate
                }
            );
            a.admitted(Instant::now(), &options);
            assert!(
                a.evaluate(attempt * 4 + 1, Instant::now(), Some(100), &options)
                    .is_none()
            );
            a.invalidate();
            a.initialized(Instant::now());
        }
    }
    #[tokio::test(start_paused = true)]
    async fn new_snapshot_warms_up_without_resetting_failed_attempt_budget() {
        let mut a = Activity::default();
        let mut options = options();
        options.max_resubscriptions = 1;
        a.initialized(Instant::now());
        tokio::time::advance(Duration::from_secs(4)).await;
        assert_eq!(
            a.evaluate(4, Instant::now(), Some(100), &options)
                .unwrap()
                .action,
            Action::Resubscribe
        );
        a.admitted(Instant::now(), &options);
        a.invalidate();
        a.initialized(Instant::now());
        assert!(a.evaluate(5, Instant::now(), Some(100), &options).is_none());
        tokio::time::advance(Duration::from_secs(4)).await;
        assert_eq!(
            a.evaluate(8, Instant::now(), Some(100), &options)
                .unwrap()
                .action,
            Action::Escalate
        );
    }
    #[test]
    fn invalid_config_is_rejected() {
        let mut options = options();
        assert!(options.validate().is_ok());
        options.window = Duration::from_secs(3);
        assert!(options.validate().is_err());
        options.window = Duration::from_secs(8);
        assert!(options.validate().is_err());
    }
    #[tokio::test(start_paused = true)]
    async fn deferred_proposals_do_not_spend_retry_budget() {
        let mut a = Activity::default();
        let options = options();
        a.initialized(Instant::now());
        for epoch in [4, 8, 12] {
            tokio::time::advance(Duration::from_secs(4)).await;
            assert_eq!(
                a.evaluate(epoch, Instant::now(), Some(100), &options)
                    .unwrap()
                    .attempt,
                1
            );
        }
        a.admitted(Instant::now(), &options);
        assert!(a.recovering);
    }
}
