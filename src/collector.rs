//! Dynamic registry with stable shard ownership, serialized membership commands,
//! snapshot-first books, in-place recovery, and cooperative output draining.

use crate::{
    auth::Credentials,
    catalog::{Catalog, Inventory},
    exchange::Configuration,
    output::{Output, Writer},
    registry::{Assignment, Registry},
    replica, source,
    transport::ConnectOptions,
};
use anyhow::{Context, ensure};
use serde::Serialize;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::{
    sync::{Semaphore, mpsc, watch},
    task::JoinSet,
    time::{Instant, MissedTickBehavior},
};

#[derive(Clone)]
pub struct Options {
    pub symbols: Vec<String>,
    pub rest_url: String,
    pub ws_url: String,
    pub symbols_per_connection: usize,
    pub max_connections: usize,
    pub max_symbols: usize,
    pub output: PathBuf,
    pub audit: Option<PathBuf>,
    pub snapshot_interval: Duration,
    pub flush_interval: Duration,
    pub connect: ConnectOptions,
    pub registry: RegistryOptions,
    pub summary: Option<PathBuf>,
    pub replicas: usize,
    pub replica_lag: Duration,
    pub failover_test_after: Option<Duration>,
    pub health: crate::health::Options,
}

#[derive(Clone)]
pub struct RegistryOptions {
    pub poll_interval: Duration,
    pub full_interval: Duration,
    pub command_interval: Duration,
    pub command_batch: usize,
}
impl Default for RegistryOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(15),
            full_interval: Duration::from_secs(1800),
            command_interval: Duration::from_millis(100),
            command_batch: 250,
        }
    }
}

mod pacer;
pub(crate) use pacer::{Pacer, UrgentDemand};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WorkerPhase {
    Running,
    Quiescing,
    Stopping,
}

#[derive(Clone, Default, Debug, Serialize)]
pub struct ShardStats {
    pub connects: u64,
    pub snapshots: u64,
    pub updates: u64,
    pub gaps: u64,
    pub resync_requests: u64,
    /// Identity-scoped covering probes consumed by Sources; not wire ACKs.
    pub covering_probes: u64,
    pub quarantined_updates: u64,
    pub frames: u64,
    pub frame_bytes: u64,
    pub retired_messages: u64,
    pub commands: u64,
    pub desired: usize,
    pub subscribed: usize,
    pub initialized: usize,
    pub epoch: u64,
    pub snapshot_lateness_ms: u128,
    pub replica_initialized: Vec<usize>,
    pub failovers: u64,
    pub continuity_gaps: u64,
    pub standby_updates: u64,
    pub stale_events: u64,
    pub barrier_cuts: u64,
    pub handoff_advances: u64,
    pub promotions_blocked: u64,
    pub hard_resets: u64,
    pub clock_regressions: u64,
    pub invalid_books: u64,
    pub snapshot_confirmations: u64,
    pub state_reconciliations: u64,
    pub unverified_transitions: u64,
    pub health_transport_renewals: u64,
    pub completed_health_renewals: u64,
    pub unavailable_replica_assets: usize,
    pub unavailable_sample: Vec<String>,
    pub source_quarantined: u64,
    pub source_restarts: u64,
    pub unacknowledged_closes: u64,
    pub stop_retirements: u64,
    pub injected_disconnects: u64,
    pub health_resubscriptions: u64,
    pub health_escalations: u64,
    pub health_recovery_candidates: usize,
    pub planned_rotations: u64,
    pub completed_rotations: u64,
    pub deferred_rotations: u64,
    pub rotation_timeouts: u64,
    /// Released native rotations ended unsuccessfully at their deadline. The
    /// operation is consumed; this makes no claim that a socket closed.
    pub failed_rotations: u64,
    pub handoff_pauses: u64,
    pub pause_expirations: u64,
    pub pause_us_max: u128,
    pub resubscriptions: u64,
    pub ack_timeouts: u64,
    pub pings_sent: u64,
    pub pongs_received: u64,
    pub frame_age_ms_max: u128,
    pub pong_age_ms_max: u128,
    pub ack_age_ms_max: u128,
    pub ingress_to_owner_us_max: u128,
    pub resubscribe_completed: u64,
    pub resubscribe_unready_ms_max: u128,
    pub relay_wait_us_max: u128,
    pub maintenance_pending: usize,
    pub maintenance_quiesced: bool,
}

pub async fn stopped(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}

struct Scan {
    inventory: Inventory,
    full: bool,
}

async fn metadata_loop(
    client: Arc<dyn Catalog>,
    mut watermark: i64,
    options: RegistryOptions,
    tx: mpsc::Sender<anyhow::Result<Scan>>,
    mut stop: watch::Receiver<bool>,
) {
    let mut last_full = Instant::now();
    let mut failures = 0;
    let mut force_full = false;
    loop {
        let full = force_full
            || !client.supports_incremental()
            || last_full.elapsed() >= options.full_interval;
        let scan = tokio::select! { _=stopped(&mut stop)=>return, r=client.scan(if full {None} else {Some(watermark.saturating_sub(10))})=>r };
        match scan {
            Ok(inventory) => {
                let complete =
                    inventory.complete && (!full || inventory.authoritative_for_retirement);
                let next = if complete {
                    inventory.watermark
                } else {
                    watermark
                };
                force_full = !complete;
                if tokio::select! {_=stopped(&mut stop)=>return,r=tx.send(Ok(Scan {inventory,full}))=>r}.is_err(){return;}
                watermark = next;
                failures = 0;
                if full && complete {
                    last_full = Instant::now();
                }
            }
            Err(error) => {
                failures += 1;
                eprintln!(
                    "metadata scan failed ({failures}/3); preserving membership/watermark: {error:#}"
                );
                if failures >= 3 {
                    tokio::select! {_=stopped(&mut stop)=>{},_=tx.send(Err(error.context("metadata freshness budget exhausted")))=>{}};
                    return;
                }
            }
        }
        tokio::select! {_=stopped(&mut stop)=>return,_=tokio::time::sleep(options.poll_interval)=>{}}
    }
}

struct Worker {
    assignment: watch::Sender<Assignment>,
    stats: watch::Receiver<ShardStats>,
}

#[derive(Serialize)]
struct Summary<'a> {
    contract: crate::exchange::Contract,
    epoch: u64,
    desired: usize,
    metadata_scans: u64,
    discovery_complete: bool,
    catalog_metadata: Option<&'a serde_json::Value>,
    last_complete_catalog: Option<&'a serde_json::Value>,
    shards: Vec<ShardStats>,
    complete_at_cutoff: bool,
    writer: Option<&'a crate::output::WriterSummary>,
}

fn assign_workers(
    registry: &Registry,
    workers: &mut Vec<Worker>,
    tasks: &mut JoinSet<anyhow::Result<(usize, ShardStats)>>,
    shared: &source::Shared,
    output: &Output,
    stop: &watch::Receiver<WorkerPhase>,
    relay_bytes: &Arc<Semaphore>,
) {
    for (index, assignment) in registry.assignments().into_iter().enumerate() {
        if let Some(worker) = workers.get(index) {
            worker.assignment.send_replace(assignment);
            continue;
        }
        let (tx, rx) = watch::channel(assignment);
        let (stats_tx, stats_rx) = watch::channel(ShardStats::default());
        workers.push(Worker {
            assignment: tx,
            stats: stats_rx,
        });
        let (shared, output, stop, relay_bytes) = (
            shared.clone(),
            output.clone(),
            stop.clone(),
            relay_bytes.clone(),
        );
        tasks.spawn(async move {
            replica::run(index, rx, shared, output, stop, stats_tx, relay_bytes)
                .await
                .map(|s| (index, s))
        });
    }
}

pub async fn run(
    options: Options,
    auth: Arc<Credentials>,
    stop: watch::Receiver<bool>,
    cancel: watch::Sender<bool>,
) -> anyhow::Result<()> {
    ensure!(
        options.max_connections <= 196,
        "Kalshi socket budget leaves quota headroom at 196"
    );
    let exchange =
        crate::exchange::kalshi::configure(auth, &options.rest_url, options.max_symbols)?;
    run_with_exchange(options, exchange, stop, cancel).await
}

/// Shared collector implementation; exchange selection belongs to its caller.
pub async fn run_with_exchange(
    options: Options,
    exchange: Configuration,
    mut stop: watch::Receiver<bool>,
    cancel: watch::Sender<bool>,
) -> anyhow::Result<()> {
    exchange.protocol.contract().validate()?;
    crate::transport::validate_ws_url(&options.ws_url)?;
    options.health.validate()?;
    let contract = exchange.protocol.contract();
    ensure!(
        options.symbols_per_connection > 0
            && options.max_connections > 0
            && (1..=2).contains(&options.replicas)
            && options.max_connections >= options.replicas
            && !options.replica_lag.is_zero(),
        "invalid shard/connection budget"
    );
    ensure!(
        !options.snapshot_interval.is_zero()
            && !options.flush_interval.is_zero()
            && !options.registry.poll_interval.is_zero()
            && !options.registry.full_interval.is_zero()
            && !options.registry.command_interval.is_zero()
            && options.registry.command_batch > 0,
        "invalid timer/batch configuration"
    );
    let client = exchange.catalog;
    let mut registry = Registry::new(
        options.symbols_per_connection,
        options.max_connections / options.replicas,
        options.max_symbols,
    )?;
    let dynamic = options.symbols.is_empty();
    let mut watermark = 0;
    let mut scans = 0;
    let mut discovery_complete = true;
    let mut catalog_metadata: Option<Arc<serde_json::Value>> = None;
    let mut last_complete_catalog: Option<Arc<serde_json::Value>> = None;
    if dynamic {
        let catalog = client
            .as_ref()
            .context("dynamic registry requires a catalog")?;
        let inventory =
            tokio::select! {_=stopped(&mut stop)=>return Ok(()),r=catalog.scan(None)=>r?};
        watermark = inventory.watermark;
        scans += 1;
        eprintln!(
            "startup inventory: {} pages in {:.1}s",
            inventory.pages,
            inventory.elapsed.as_secs_f64()
        );
        discovery_complete = inventory.complete && inventory.authoritative_for_retirement;
        registry.reconcile(&inventory.instruments, discovery_complete)?;
        catalog_metadata = inventory.metadata.map(Arc::new);
        if discovery_complete {
            last_complete_catalog = catalog_metadata.clone();
        }
    } else {
        registry.replace(options.symbols.iter().cloned().collect())?;
        if let Some(catalog) = client.as_ref().filter(|c| c.validates_fixed_symbols()) {
            let inventory =
                tokio::select! {_=stopped(&mut stop)=>return Ok(()),r=catalog.scan(None)=>r?};
            ensure!(
                inventory.complete && inventory.authoritative_for_retirement,
                "fixed instrument validation is incomplete"
            );
            ensure!(
                options.symbols.iter().all(|s| inventory
                    .instruments
                    .iter()
                    .any(|i| i.symbol == *s && i.eligible)),
                "fixed instrument is missing or ineligible"
            );
            scans += 1;
            catalog_metadata = inventory.metadata.map(Arc::new);
            last_complete_catalog = catalog_metadata.clone();
        }
    }
    ensure!(
        options.connect.source_port.is_none()
            || options.max_connections == 1 && options.replicas == 1,
        "explicit source port requires max-connections=1"
    );
    let options = Arc::new(options);
    let mut writer = Writer::start(&options.output, options.audit.as_deref(), 1024, 64 << 20)?;
    let pacer = Arc::new(Pacer::new(options.registry.command_interval));
    let shared = source::Shared {
        protocol: exchange.protocol,
        options: options.clone(),
        auth: exchange.auth,
        pacer,
        recovery_pacer: Arc::new(Pacer::new(Duration::from_millis(500))),
    };
    let relay_bytes = Arc::new(Semaphore::new(64 << 20));
    let mut tasks = JoinSet::new();
    let mut workers = vec![];
    // The supervisor freezes registry publication before releasing workers to
    // stop. External cancellation must not let a shard finish before the last
    // assignment has been committed by this task.
    let (worker_cancel, worker_stop) = watch::channel(WorkerPhase::Running);
    assign_workers(
        &registry,
        &mut workers,
        &mut tasks,
        &shared,
        &writer.output,
        &worker_stop,
        &relay_bytes,
    );
    let (scan_tx, mut scan_rx) = mpsc::channel(1);
    let metadata = if dynamic {
        Some(tokio::spawn(metadata_loop(
            client.context("dynamic registry requires a catalog")?,
            watermark,
            options.registry.clone(),
            scan_tx,
            stop.clone(),
        )))
    } else {
        None
    };
    let mut diagnostics = tokio::time::interval(Duration::from_secs(10));
    diagnostics.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Flush belongs to the single output stream, not to every shard: each
    // nonempty helper flush pads a full 64 MiB chunk.
    let mut flush = tokio::time::interval_at(
        Instant::now() + options.flush_interval,
        options.flush_interval,
    );
    flush.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut error = None;
    let mut stopping = false;
    let mut prepare_until: Option<Instant> = None;
    let mut cut_check = tokio::time::interval(Duration::from_millis(100));
    cut_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        if stopping {
            if error.is_some() {
                prepare_until = None;
            }
            if let Some(deadline) = prepare_until {
                let rows: Vec<_> = workers.iter().map(|w| w.stats.borrow().clone()).collect();
                let ready = rows.iter().all(|r| r.maintenance_quiesced)
                    && complete(&rows, registry.epoch, registry.len(), options.replicas);
                if ready || Instant::now() >= deadline {
                    prepare_until = None;
                }
            }
            let phase = if prepare_until.is_some() {
                WorkerPhase::Quiescing
            } else {
                WorkerPhase::Stopping
            };
            worker_cancel.send_if_modified(|current| {
                let changed = *current != phase;
                *current = phase;
                changed
            });
        }
        if stopping && tasks.is_empty() {
            break;
        }
        tokio::select! {
            _=stopped(&mut stop),if !stopping=>{stopping=true;prepare_until=Some(Instant::now()+Duration::from_secs(15));let _=cancel.send(true);},
            _=cut_check.tick(),if prepare_until.is_some()=>{},
            scan=scan_rx.recv(),if dynamic && !stopping=>{
                let result=match scan {Some(Ok(scan))=>{
                    scans+=1;
                    if !scan.inventory.complete || scan.full && !scan.inventory.authoritative_for_retirement { discovery_complete=false; }
                    else if scan.full { discovery_complete=true; }
                    if let Some(metadata)=scan.inventory.metadata { catalog_metadata=Some(Arc::new(metadata)); }
                    if scan.full && discovery_complete { last_complete_catalog=catalog_metadata.clone(); }
                    registry.reconcile(&scan.inventory.instruments,scan.full && scan.inventory.complete && scan.inventory.authoritative_for_retirement).map(|changes|{
                        eprintln!("registry: {}",serde_json::to_string(&changes).unwrap());
                        if changes.added>0 || changes.retired>0 {assign_workers(&registry,&mut workers,&mut tasks,&shared,&writer.output,&worker_stop,&relay_bytes);}
                    })
                },Some(Err(e))=>Err(e),None if *stop.borrow()=>{stopping=true;prepare_until=Some(Instant::now()+Duration::from_secs(15));Ok(())},None=>Err(anyhow::anyhow!("metadata worker ended unexpectedly"))};
                if let Err(e)=result {error=Some(e);stopping=true;let _=cancel.send(true);}
            },
            result=tasks.join_next(),if !tasks.is_empty()=>match result {
                Some(Ok(Ok((index,stats))))=>{
                    eprintln!("shard {index} finished: {}",serde_json::to_string(&stats)?);
                    if !stopping && !*stop.borrow() {error=Some(anyhow::anyhow!("shard ended unexpectedly"));stopping=true;let _=cancel.send(true);}
                },
                Some(Ok(Err(e)))=>{if error.is_none(){error=Some(e);}stopping=true;let _=cancel.send(true);},
                Some(Err(e))=>{if error.is_none(){error=Some(e.into());}stopping=true;let _=cancel.send(true);},
                None=>{},
            },
            failure=writer.failures.changed()=>{
                error=Some(anyhow::anyhow!(writer.failures.borrow().clone().unwrap_or_else(||format!("writer ended: {failure:?}"))));
                stopping=true;let _=cancel.send(true);
            },
            _=flush.tick(),if !stopping=>{
                let result=tokio::time::timeout(Duration::from_secs(15),writer.output.flush()).await;
                if !matches!(result,Ok(Ok(()))) {error=Some(anyhow::anyhow!("output flush admission failed: {result:?}"));stopping=true;let _=cancel.send(true);}
            },
            _=diagnostics.tick(),if !stopping=>{
                let rows:Vec<_>=workers.iter().map(|w|w.stats.borrow().clone()).collect();
                eprintln!("collector progress: epoch={} desired={} discovery_complete={} shards={}",registry.epoch,registry.len(),discovery_complete,serde_json::to_string(&rows)?);
            }
        }
    }
    if let Some(metadata) = metadata {
        metadata.await.context("metadata task panicked")?;
    }
    let finished = writer.finish().await;
    let rows: Vec<_> = workers.iter().map(|w| w.stats.borrow().clone()).collect();
    let complete = complete(&rows, registry.epoch, registry.len(), options.replicas);
    if let Some(path) = &options.summary {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        serde_json::to_writer_pretty(
            file,
            &Summary {
                contract,
                epoch: registry.epoch,
                desired: registry.len(),
                metadata_scans: scans,
                discovery_complete,
                catalog_metadata: catalog_metadata.as_deref(),
                last_complete_catalog: last_complete_catalog.as_deref().filter(|_| !matches!((&catalog_metadata, &last_complete_catalog), (Some(a),Some(b)) if Arc::ptr_eq(a,b))),
                shards: rows,
                complete_at_cutoff: complete && discovery_complete && error.is_none() && finished.is_ok(),
                writer: finished.as_ref().ok(),
            },
        )?;
    }
    if let Some(error) = error {
        return Err(error);
    }
    eprintln!("writer finished: {}", serde_json::to_string(&finished?)?);
    Ok(())
}

fn complete(rows: &[ShardStats], epoch: u64, desired: usize, replicas: usize) -> bool {
    rows.iter().map(|s| s.desired).sum::<usize>() == desired
        && rows.iter().all(|s| {
            s.epoch == epoch
                && s.desired == s.initialized
                && s.desired == s.subscribed
                && s.replica_initialized.len() == replicas
                && s.replica_initialized.iter().all(|&n| n == s.desired)
                && s.maintenance_pending == 0
        })
}

pub(crate) use crate::transport::close;

#[cfg(test)]
mod pacing_tests;
