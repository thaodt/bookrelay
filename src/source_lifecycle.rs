//! One Tellus actor per Source worker, owning one whole `source::run` future.
//!
//! The seam is lifecycle only. `init` starts the job exactly once on a
//! [`TaskTracker`] and pipes the real Tokio join handle to the actor itself;
//! `receive` stops on the completion notice. The domain reconnect loop and its
//! generations, the ordered byte-bounded relay, the control watches, the single
//! socket writer and the shard `JoinSet` are untouched. Tellus contributes a
//! typed actor, `Stop` supervision and termination observation. It carries no
//! data, makes no shard decisions, and does not join the job.
//!
//! Everything whose loss would matter is kept outside the actor:
//! - the job outcome goes through a one-shot slot, written by the piped
//!   conversion *before* the actor is told, so neither a stopped actor nor a
//!   dropped message can swallow it;
//! - the join obligation is the [`TaskTracker`], held by [`SourceLifecycle`];
//! - the shard cancellation sender is held by the drop guard.
//!
//! Mailbox `Bounded(2)`: one forced `Joined` (one piped job per actor
//! lifetime) plus at most one ordinary `Stop` (told once, by the guard or by
//! `join` after a lost outcome). Forced items count against the bound, so 2 is
//! the smallest capacity at which that `Stop` can never be refused. The actor
//! watches nothing, so no terminated signal is queued. There are no asks, so
//! no `MailboxFull` or `Timeout` can consume cleanup ownership.

use std::{
    fmt,
    future::Future,
    num::NonZeroUsize,
    pin::pin,
    sync::{Mutex, PoisonError},
};

use tellus::{
    Actor, ActorConfig, ActorContext, ActorRef, ActorSystem, Control, Incoming, MailboxCapacity,
    SupervisionStrategy, Untimed,
};
use tokio::sync::{oneshot, watch};
use tokio_util::task::TaskTracker;

const MAILBOX: NonZeroUsize = NonZeroUsize::new(2).expect("2 is not zero");
const ACTOR_LOST: &str = "source lifecycle actor ended before its job; source cancelled and joined";
const OUTCOME_LOST: &str = "source lifecycle lost the job outcome; source cancelled and joined";
const SECOND_INIT: &str = "source lifecycle init ran twice; one job per actor lifetime";

/// Run one Source job under its lifecycle actor and return the job's own
/// result. See [`SourceLifecycle::join`].
///
/// # Errors
/// The job's error, unchanged; a job panic as a [`tokio::task::JoinError`]
/// source; or [`LifecycleFault`] if the actor ended before the job did.
pub async fn supervise<F>(job: F, cancel: watch::Sender<bool>) -> anyhow::Result<()>
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    SourceLifecycle::start(job, cancel).join().await
}

/// The lifecycle actor failed its contract. Distinct from any Source error.
#[derive(Debug)]
pub struct LifecycleFault(&'static str);

impl fmt::Display for LifecycleFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for LifecycleFault {}

/// One started Source job and the lifecycle actor that started it.
#[must_use = "dropping requests cancellation but cannot join the job"]
pub struct SourceLifecycle {
    system: ActorSystem<Message>,
    outcome: oneshot::Receiver<anyhow::Result<()>>,
    tracker: TaskTracker,
    guard: Guard,
}

impl SourceLifecycle {
    /// Spawn the lifecycle actor; its `init` starts `job`. `cancel` is the
    /// shard's cancellation watch, used only if the actor is lost or this
    /// helper is dropped. Must be called inside a Tokio runtime.
    pub fn start<F>(job: F, cancel: watch::Sender<bool>) -> Self
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let tracker = TaskTracker::new();
        let (recorded, outcome) = oneshot::channel();
        let worker = Worker(Mutex::new(Some(Start {
            job,
            tracker: tracker.clone(),
            recorded,
        })));
        let config = ActorConfig::default()
            .with_mailbox_capacity(MailboxCapacity::Bounded(MAILBOX))
            .with_supervision_strategy(SupervisionStrategy::Stop);
        let system = ActorSystem::with_config(worker, config);
        let guard = Guard {
            cancel: Some(cancel),
            actor: Some(system.root().clone()),
        };
        Self {
            system,
            outcome,
            tracker,
            guard,
        }
    }

    /// Wait for the job's outcome, the actor's termination and the job's join,
    /// then return the job's own result unchanged.
    ///
    /// If the actor ends before the job, request shard cancellation, wait for
    /// the job to leave through its own stop path and deadlines, and report
    /// [`LifecycleFault`], keeping the job's error as the source if it has one.
    /// This never touches the data relay: the caller keeps draining it while
    /// this future is pending, exactly as for the bare Source today.
    ///
    /// Dropping this future requests cancellation and actor stop, but cannot
    /// join; see [`Guard`].
    ///
    /// # Errors
    /// As for [`supervise`].
    pub async fn join(self) -> anyhow::Result<()> {
        let Self {
            system,
            mut outcome,
            tracker,
            mut guard,
        } = self;
        let mut terminated = pin!(system.terminated());
        // `terminated` fails only if termination cannot be observed, e.g. the
        // runtime dropped the actor task; the actor is gone either way.
        let (first, termination) = tokio::select! {
            biased;
            recorded = &mut outcome => (Some(recorded.ok()), None),
            ended = &mut terminated => (None, Some(ended)),
        };
        let mut actor_lost = false;
        let recorded = match first {
            Some(recorded) => recorded,
            None => {
                guard.actor = None;
                match outcome.try_recv() {
                    // `Joined` is only told after the outcome is recorded: an
                    // ordinary stop, or a forced one that raced the job's end.
                    Ok(recorded) => Some(recorded),
                    Err(oneshot::error::TryRecvError::Closed) => None,
                    Err(oneshot::error::TryRecvError::Empty) => {
                        actor_lost = true;
                        guard.cancel_source();
                        outcome.await.ok()
                    }
                }
            }
        };
        if recorded.is_some() {
            // The job has finished; a later drop must not cancel the shard.
            guard.cancel = None;
        } else {
            // No `Joined` will come, and a started job would be unreported.
            guard.cancel_source();
            guard.stop_actor();
        }
        let termination = match termination {
            Some(ended) => ended,
            None => terminated.await,
        };
        guard.actor = None;
        tracker.close();
        tracker.wait().await;
        guard.cancel = None;
        let result = match (recorded, actor_lost) {
            (Some(result), false) => result,
            (Some(Err(error)), true) => Err(error.context(LifecycleFault(ACTOR_LOST))),
            (Some(Ok(())), true) => Err(LifecycleFault(ACTOR_LOST).into()),
            (None, _) => Err(LifecycleFault(OUTCOME_LOST).into()),
        };
        // A completed job must not turn a failure to observe actor termination
        // into success. Preserve a genuine earlier job/lifecycle error first.
        result?;
        termination.map_err(|error| {
            anyhow::Error::new(error).context("source lifecycle termination was not observable")
        })
    }
}

/// Requests teardown when `join` does not finish. `Drop` is synchronous: it
/// cannot await, so it never claims a join. A started job keeps running,
/// unjoined, until it observes cancellation and leaves through its own stop
/// path and write deadlines. No detached cleanup task is spawned and the job
/// is never aborted, so no partially written stream is abandoned mid-frame.
struct Guard {
    /// Present until the job's outcome is known.
    cancel: Option<watch::Sender<bool>>,
    /// Present until termination is observed or `Stop` has been told.
    actor: Option<ActorRef<Message>>,
}

impl Guard {
    fn cancel_source(&self) {
        if let Some(cancel) = &self.cancel {
            cancel.send_replace(true);
        }
    }

    fn stop_actor(&mut self) {
        if let Some(actor) = self.actor.take() {
            actor.tell(Message::Stop);
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.cancel_source();
        self.stop_actor();
    }
}

enum Message {
    /// The job task was joined; its outcome is already recorded.
    Joined,
    /// Stop without waiting for the job.
    Stop,
    #[cfg(test)]
    Fail,
}

struct Start<F> {
    job: F,
    tracker: TaskTracker,
    recorded: oneshot::Sender<anyhow::Result<()>>,
}

struct Worker<F>(Mutex<Option<Start<F>>>);

impl<F> Actor for Worker<F>
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    type Message = Message;
    type Timers = Untimed;
    type State = ();
    type Error = LifecycleFault;

    fn init(&self, context: &ActorContext<Message>) -> Result<(), LifecycleFault> {
        let start = self.0.lock().unwrap_or_else(PoisonError::into_inner).take();
        let Start {
            job,
            tracker,
            recorded,
        } = start.ok_or(LifecycleFault(SECOND_INIT))?;
        let joined = tracker.spawn(job);
        context.pipe_to_self(joined, move |joined| {
            let result = joined.unwrap_or_else(|error| {
                Err(anyhow::Error::new(error).context("source task did not complete"))
            });
            if let Err(Err(error)) = recorded.send(result) {
                eprintln!("source job ended after its lifecycle helper was dropped: {error:#}");
            }
            Message::Joined
        });
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Message>,
        incoming: Incoming<Message>,
        (): (),
    ) -> Result<Control<()>, LifecycleFault> {
        match incoming {
            Incoming::Message(Message::Joined | Message::Stop) => Ok(Control::Stop),
            #[cfg(test)]
            Incoming::Message(Message::Fail) => Err(LifecycleFault("injected receive failure")),
            // Nothing is watched; Tellus drops unwatched signals before `receive`.
            Incoming::Terminated(_) => Ok(Control::Continue(())),
        }
    }
}

#[cfg(test)]
impl SourceLifecycle {
    fn actor(&self) -> ActorRef<Message> {
        self.system.root().clone()
    }

    fn tracker(&self) -> TaskTracker {
        self.tracker.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        convert::Infallible,
        num::NonZeroU32,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering::SeqCst},
        },
        time::Duration,
    };
    use tellus::{Backoff, Nothing, RestartPolicy};
    use tokio::{
        runtime::Builder,
        sync::mpsc,
        task::{JoinError, JoinSet},
    };

    /// Watchdog only: turns a hang into a failure. No assertion depends on it.
    const HANG: Duration = Duration::from_secs(20);

    fn on_both_runtimes<S, Fut>(scenario: S)
    where
        S: Fn() -> Fut,
        Fut: Future<Output = ()>,
    {
        let current = Builder::new_current_thread().enable_time().build();
        let multi = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_time()
            .build();
        for runtime in [current, multi] {
            let runtime = runtime.expect("tokio runtime");
            runtime.block_on(async {
                tokio::time::timeout(HANG, scenario())
                    .await
                    .expect("scenario hung");
            });
        }
    }

    #[derive(Debug, PartialEq)]
    struct Domain(&'static str);

    impl fmt::Display for Domain {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for Domain {}

    /// Counts live job futures: zero proves the runtime has dropped them.
    #[derive(Clone, Default)]
    struct Live(Arc<AtomicUsize>);

    struct Alive(Arc<AtomicUsize>);

    impl Live {
        fn enter(&self) -> Alive {
            self.0.fetch_add(1, SeqCst);
            Alive(self.0.clone())
        }

        fn count(&self) -> usize {
            self.0.load(SeqCst)
        }
    }

    impl Drop for Alive {
        fn drop(&mut self) {
            self.0.fetch_sub(1, SeqCst);
        }
    }

    /// The contract of `collector::stopped`.
    async fn stopped(stop: &mut watch::Receiver<bool>) {
        while !*stop.borrow_and_update() {
            if stop.changed().await.is_err() {
                return;
            }
        }
    }

    /// Tellus death watch: resolves once `target` has terminated, at once if
    /// it already has.
    async fn terminated(target: ActorRef<Message>) {
        struct Observer(ActorRef<Message>);

        impl Actor for Observer {
            type Message = Nothing;
            type Timers = Untimed;
            type State = ();
            type Error = Infallible;

            fn init(&self, context: &ActorContext<Nothing>) -> Result<(), Infallible> {
                context.watch(&self.0);
                Ok(())
            }

            fn receive(
                &self,
                _: &ActorContext<Nothing>,
                incoming: Incoming<Nothing>,
                (): (),
            ) -> Result<Control<()>, Infallible> {
                match incoming {
                    Incoming::Terminated(_) => Ok(Control::Stop),
                    Incoming::Message(never) => match never {},
                }
            }
        }

        ActorSystem::new(Observer(target))
            .terminated()
            .await
            .expect("observer terminates");
    }

    /// A Source that, once cancelled, still finishes an in-flight write until
    /// released, as a Sink write runs to its own deadline.
    struct Held {
        started: oneshot::Receiver<()>,
        cancelled: oneshot::Receiver<()>,
        release: oneshot::Sender<anyhow::Result<()>>,
    }

    fn held_job(
        mut stop: watch::Receiver<bool>,
        live: &Live,
    ) -> (
        impl Future<Output = anyhow::Result<()>> + Send + 'static,
        Held,
    ) {
        let (started_tx, started) = oneshot::channel();
        let (cancelled_tx, cancelled) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let live = live.clone();
        let job = async move {
            let _alive = live.enter();
            let _ = started_tx.send(());
            stopped(&mut stop).await;
            let _ = cancelled_tx.send(());
            released.await.expect("test releases the in-flight write")
        };
        let held = Held {
            started,
            cancelled,
            release,
        };
        (job, held)
    }

    #[test]
    fn returns_the_exact_domain_result_after_actor_end_and_job_join() {
        on_both_runtimes(|| async {
            for failure in [None, Some("protocol violation: non-retryable")] {
                let (cancel, observed) = watch::channel(false);
                let live = Live::default();
                let alive = live.clone();
                let lifecycle = SourceLifecycle::start(
                    async move {
                        let _alive = alive.enter();
                        failure.map_or(Ok(()), |text| Err(Domain(text).into()))
                    },
                    cancel,
                );
                let (actor, tracker) = (lifecycle.actor(), lifecycle.tracker());
                let result = lifecycle.join().await;
                match failure {
                    None => assert!(result.is_ok()),
                    Some(text) => {
                        let error = result.expect_err("domain error");
                        assert_eq!(error.downcast_ref::<Domain>(), Some(&Domain(text)));
                        assert_eq!(format!("{error:#}"), text, "no context added");
                    }
                }
                assert!(tracker.is_closed() && tracker.is_empty());
                assert_eq!(live.count(), 0);
                assert!(!*observed.borrow(), "shard policy stays with the Owner");
                terminated(actor).await;
            }
        });
    }

    async fn panicking_source() -> anyhow::Result<()> {
        panic!("socket writer invariant broken")
    }

    #[test]
    fn a_panicking_job_surfaces_as_an_error_instead_of_a_hang() {
        on_both_runtimes(|| async {
            let (cancel, _observed) = watch::channel(false);
            let lifecycle = SourceLifecycle::start(panicking_source(), cancel);
            let tracker = lifecycle.tracker();
            let error = lifecycle.join().await.expect_err("a panic is terminal");
            let joined = error.downcast_ref::<JoinError>().expect("JoinError kept");
            assert!(joined.is_panic());
            assert!(format!("{error:#}").contains("socket writer invariant broken"));
            assert!(tracker.is_closed() && tracker.is_empty());
        });
    }

    #[test]
    fn a_lost_actor_cancels_the_source_and_joins_only_after_its_in_flight_write() {
        on_both_runtimes(|| async {
            for crash in [false, true] {
                for write_fails in [false, true] {
                    let (cancel, observed) = watch::channel(false);
                    let live = Live::default();
                    let (job, held) = held_job(cancel.subscribe(), &live);
                    let lifecycle = SourceLifecycle::start(job, cancel);
                    let (actor, tracker) = (lifecycle.actor(), lifecycle.tracker());
                    let joining = tokio::spawn(lifecycle.join());
                    held.started.await.expect("job started");
                    // Neither the helper nor the Owner asked for this.
                    actor.tell(if crash { Message::Fail } else { Message::Stop });
                    terminated(actor).await;
                    held.cancelled.await.expect("helper cancelled the source");
                    assert!(*observed.borrow());
                    // The write is still in flight, so nothing may claim a join.
                    assert!(!joining.is_finished());
                    assert_eq!((tracker.len(), live.count()), (1, 1));
                    let finish = if write_fails {
                        Err(Domain("write failed").into())
                    } else {
                        Ok(())
                    };
                    held.release.send(finish).expect("job awaits release");
                    let error = joining
                        .await
                        .expect("join task")
                        .expect_err("losing the actor is an error");
                    assert!(error.downcast_ref::<LifecycleFault>().is_some());
                    assert_eq!(error.downcast_ref::<Domain>().is_some(), write_fails);
                    assert!(tracker.is_closed() && tracker.is_empty());
                    assert_eq!(live.count(), 0);
                }
            }
        });
    }

    #[test]
    fn dropping_the_helper_requests_cancellation_and_stop_without_claiming_a_join() {
        on_both_runtimes(|| async {
            for while_joining in [false, true] {
                let (cancel, observed) = watch::channel(false);
                let live = Live::default();
                let (job, held) = held_job(cancel.subscribe(), &live);
                let lifecycle = SourceLifecycle::start(job, cancel);
                let (actor, tracker) = (lifecycle.actor(), lifecycle.tracker());
                held.started.await.expect("job started");
                if while_joining {
                    // As when the shard JoinSet is dropped with the helper pending.
                    let joining = tokio::spawn(lifecycle.join());
                    joining.abort();
                    assert!(joining.await.expect_err("aborted").is_cancelled());
                } else {
                    drop(lifecycle);
                }
                assert!(*observed.borrow(), "Drop requested cancellation");
                terminated(actor).await;
                held.cancelled.await.expect("source observed cancellation");
                // Nobody joined: the in-flight write still runs, unawaited.
                assert_eq!((tracker.len(), live.count()), (1, 1));
                assert!(!tracker.is_closed(), "the helper never reached its join");
                held.release.send(Ok(())).expect("job awaits release");
                // Only an explicit waiter observes the end; Drop could not.
                tracker.close();
                tracker.wait().await;
                assert_eq!(live.count(), 0);
            }
        });
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Event {
        Begin(u64),
        End(u64),
    }

    enum Command {
        Reconnect,
        Fatal,
    }

    struct Probe {
        live: Live,
        max_live: AtomicUsize,
        runs: AtomicUsize,
    }

    /// The domain reconnect loop stays inside the job: generations are minted
    /// here, each closed before the next dial, and never tied to the actor.
    async fn simulated_source(
        replica: usize,
        relay: mpsc::Sender<(usize, Event)>,
        mut commands: mpsc::UnboundedReceiver<Command>,
        mut stop: watch::Receiver<bool>,
        probe: Arc<Probe>,
    ) -> anyhow::Result<()> {
        probe.runs.fetch_add(1, SeqCst);
        let mut generation = 0;
        loop {
            generation += 1;
            let _session = probe.live.enter();
            probe.max_live.fetch_max(probe.live.count(), SeqCst);
            relay.send((replica, Event::Begin(generation))).await?;
            let command = tokio::select! {
                biased;
                () = stopped(&mut stop) => None,
                command = commands.recv() => command,
            };
            // The terminal marker follows every event of its generation and
            // may wait on a full relay, also after cancellation.
            relay.send((replica, Event::End(generation))).await?;
            match command {
                Some(Command::Reconnect) => {}
                Some(Command::Fatal) => return Err(Domain("non-retryable").into()),
                None => return Ok(()),
            }
        }
    }

    #[test]
    fn two_workers_keep_generations_serial_and_drain_on_a_fatal_error() {
        on_both_runtimes(|| async {
            let (cancel, _) = watch::channel(false);
            // One slot, so any marker, also after cancellation, may wait on the Owner.
            let (relay, mut input) = mpsc::channel(1);
            let mut tasks = JoinSet::new();
            let mut commands = Vec::new();
            let mut probes = Vec::new();
            let mut trackers = Vec::new();
            for replica in 0..2 {
                let (command, commands_rx) = mpsc::unbounded_channel();
                let probe = Arc::new(Probe {
                    live: Live::default(),
                    max_live: AtomicUsize::new(0),
                    runs: AtomicUsize::new(0),
                });
                let job = simulated_source(
                    replica,
                    relay.clone(),
                    commands_rx,
                    cancel.subscribe(),
                    probe.clone(),
                );
                let lifecycle = SourceLifecycle::start(job, cancel.clone());
                trackers.push(lifecycle.tracker());
                tasks.spawn(async move { (replica, lifecycle.join().await) });
                commands.push(command);
                probes.push(probe);
            }
            drop(relay);
            for _ in 0..2 {
                commands[0].send(Command::Reconnect).expect("worker 0");
            }
            for _ in 0..3 {
                commands[1].send(Command::Reconnect).expect("worker 1");
            }
            // The Owner loop of `replica::run`, reduced to drain and join.
            let mut events: [Vec<Event>; 2] = Default::default();
            let (mut first_error, mut closing, mut input_open) = (None, false, true);
            while !(closing && tasks.is_empty() && !input_open) {
                tokio::select! {
                    received = input.recv(), if input_open => match received {
                        Some((replica, event)) => {
                            events[replica].push(event);
                            if (replica, event) == (1, Event::Begin(4)) {
                                commands[0].send(Command::Fatal).expect("worker 0");
                            }
                        }
                        None => input_open = false,
                    },
                    done = tasks.join_next(), if !tasks.is_empty() => {
                        let (replica, result) = done.expect("a task").expect("helper task");
                        if let Err(error) = result {
                            assert!(!*cancel.borrow(), "the helper leaves policy to the Owner");
                            first_error.get_or_insert((replica, error));
                            closing = true;
                            cancel.send_replace(true);
                        }
                    }
                }
            }
            let (replica, error) = first_error.expect("the fatal error surfaced");
            assert_eq!(replica, 0);
            assert_eq!(
                error.downcast_ref::<Domain>(),
                Some(&Domain("non-retryable"))
            );
            for (replica, generations) in [(0, 3), (1, 4)] {
                let expected: Vec<_> = (1..=generations)
                    .flat_map(|g| [Event::Begin(g), Event::End(g)])
                    .collect();
                assert_eq!(events[replica], expected, "replica {replica}");
                let probe = &probes[replica];
                assert_eq!(probe.runs.load(SeqCst), 1, "one job per actor lifetime");
                assert_eq!(probe.max_live.load(SeqCst), 1, "no overlapping generations");
                assert_eq!(probe.live.count(), 0);
                assert!(trackers[replica].is_closed() && trackers[replica].is_empty());
            }
        });
    }

    #[test]
    fn even_a_restart_strategy_cannot_start_a_second_job() {
        on_both_runtimes(|| async {
            let starts = Arc::new(AtomicUsize::new(0));
            let counted = starts.clone();
            let (release, released) = oneshot::channel::<()>();
            let job = async move {
                counted.fetch_add(1, SeqCst);
                released.await.expect("test releases the job");
                Ok(())
            };
            let tracker = TaskTracker::new();
            let (recorded, outcome) = oneshot::channel();
            let worker = Worker(Mutex::new(Some(Start {
                job,
                tracker: tracker.clone(),
                recorded,
            })));
            let backoff = Backoff::new(Duration::from_millis(1), Duration::from_millis(1))
                .expect("valid bounds");
            let policy = RestartPolicy::new(NonZeroU32::new(3).expect("3 is not zero"))
                .with_backoff(backoff);
            let config = ActorConfig::default()
                .with_supervision_strategy(SupervisionStrategy::Restart(policy));
            let system = ActorSystem::with_config(worker, config);
            system.root().tell(Message::Fail);
            // Each re-init finds no job and fails until the limit stops the actor.
            system.terminated().await.expect("actor terminates");
            assert_eq!(tracker.len(), 1, "the first job still runs");
            release.send(()).expect("job awaits release");
            assert!(outcome.await.expect("outcome recorded").is_ok());
            tracker.close();
            tracker.wait().await;
            assert_eq!(starts.load(SeqCst), 1);
        });
    }
}
