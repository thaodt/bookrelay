use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
    thread,
};

use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

use crate::util::{
    AlignedSbeWriter,
    dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain},
};

pub const CHUNK_SIZE: usize = 64 << 20;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotReason {
    Initial,
    Periodic,
    Recovery,
    Failover,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderingEvidence {
    #[default]
    LegacyUnspecified,
    ComparableClock,
    SnapshotOnly,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SourceStamp {
    #[serde(default)]
    pub evidence: OrderingEvidence,
    #[serde(default)]
    pub unverified_cut: bool,
    pub replica: usize,
    pub generation: u64,
    pub output_epoch: u64,
    pub incarnation: u64,
    #[serde(default)]
    pub continuity_epoch: u64,
    pub clock: Option<u64>,
    pub ordinal: Option<u64>,
    pub complete_clock: bool,
}

pub enum Record {
    Snapshot {
        data: SnapshotDomain,
        reason: SnapshotReason,
    },
    SourcedSnapshot {
        data: SnapshotDomain,
        reason: SnapshotReason,
        source: SourceStamp,
    },
    Update(PriceLevelUpdateDomain),
}

impl Record {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::Snapshot { data, .. } | Self::SourcedSnapshot { data, .. } => {
                    data.symbol.capacity()
                        + data.bids.capacity()
                            * std::mem::size_of::<
                                crate::util::dtos::challenge_orderbook::SnapshotBidsEntryDomain,
                            >()
                        + data.asks.capacity()
                            * std::mem::size_of::<
                                crate::util::dtos::challenge_orderbook::SnapshotAsksEntryDomain,
                            >()
                }
                Self::Update(data) => data.symbol.capacity(),
            }
    }
}

#[derive(Deserialize, Serialize)]
pub struct AuditRow {
    pub record: u64,
    pub symbol: String,
    pub reason: SnapshotReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceStamp>,
}

enum Command {
    Record(Record, OwnedSemaphorePermit),
    Flush,
}

#[derive(Clone)]
pub struct Output {
    tx: mpsc::Sender<Command>,
    bytes: Arc<Semaphore>,
    byte_limit: usize,
}

#[derive(Debug, Default, Serialize)]
pub struct WriterSummary {
    pub records: u64,
    pub snapshots: u64,
    pub updates: u64,
    pub flush_requests: u64,
}

pub struct Writer {
    pub output: Output,
    pub failures: watch::Receiver<Option<String>>,
    join: thread::JoinHandle<anyhow::Result<WriterSummary>>,
}

impl Writer {
    pub fn start(
        path: &Path,
        audit_path: Option<&Path>,
        items: usize,
        byte_limit: usize,
    ) -> anyhow::Result<Self> {
        ensure!(
            items > 0 && byte_limit > 0 && byte_limit <= u32::MAX as usize,
            "invalid output queue capacity"
        );
        let writer =
            AlignedSbeWriter::create(path, CHUNK_SIZE).context("cannot create SBE output")?;
        let audit = audit_path
            .map(|p| {
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(p)
                    .map(BufWriter::new)
            })
            .transpose()
            .context("cannot create new audit file")?;
        let (tx, rx) = mpsc::channel(items);
        let (failure_tx, failures) = watch::channel(None);
        let join = thread::Builder::new()
            .name("sbe-writer".into())
            .spawn(move || {
                let result = write_loop(writer, audit, rx);
                if let Err(error) = &result {
                    let _ = failure_tx.send(Some(format!("{error:#}")));
                }
                result
            })?;
        Ok(Self {
            output: Output {
                tx,
                bytes: Arc::new(Semaphore::new(byte_limit)),
                byte_limit,
            },
            failures,
            join,
        })
    }

    /// All producer clones must be dropped before joining.
    pub async fn finish(self) -> anyhow::Result<WriterSummary> {
        drop(self.output);
        // A blocking pipe write cannot be cancelled. Poll the OS thread instead of
        // stranding a spawn_blocking join that would also block runtime shutdown.
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            while !self.join.is_finished() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .context("writer did not drain in 15s; output may be incomplete")?;
        self.join
            .join()
            .map_err(|_| anyhow::anyhow!("writer thread panicked"))?
    }
}

impl Output {
    pub async fn send(&self, record: Record) -> anyhow::Result<()> {
        let bytes = record.retained_bytes().max(1);
        ensure!(
            bytes <= self.byte_limit,
            "record exceeds output byte budget"
        );
        // Do not park an unaccounted heap-owning producer waiting for byte admission.
        // Item-channel waits retain this permit; byte exhaustion is an explicit error.
        let permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(u32::try_from(bytes)?)
            .context("output retained-byte budget exhausted")?;
        self.tx
            .send(Command::Record(record, permit))
            .await
            .map_err(|_| anyhow::anyhow!("writer stopped before accepting record"))
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        self.tx
            .send(Command::Flush)
            .await
            .map_err(|_| anyhow::anyhow!("writer stopped before flush"))
    }
}

fn write_loop(
    mut writer: AlignedSbeWriter,
    mut audit: Option<BufWriter<File>>,
    mut rx: mpsc::Receiver<Command>,
) -> anyhow::Result<WriterSummary> {
    let mut summary = WriterSummary::default();
    while let Some(command) = rx.blocking_recv() {
        match command {
            Command::Flush => {
                summary.flush_requests += 1;
                writer.flush()?;
                if let Some(audit) = &mut audit {
                    audit.flush()?;
                }
            }
            Command::Record(record, _permit) => {
                let (record, source) = match record {
                    Record::SourcedSnapshot {
                        data,
                        reason,
                        source,
                    } => (Record::Snapshot { data, reason }, Some(source)),
                    record => (record, None),
                };
                match record {
                    Record::Snapshot { data, reason } => {
                        ensure!(
                            data.bids.len() <= u16::MAX as usize
                                && data.asks.len() <= u16::MAX as usize,
                            "snapshot exceeds SBE group count"
                        );
                        writer.write_msg(&data)?;
                        if let Some(audit) = &mut audit {
                            serde_json::to_writer(
                                &mut *audit,
                                &AuditRow {
                                    record: summary.records,
                                    symbol: data.symbol,
                                    reason,
                                    source,
                                },
                            )?;
                            audit.write_all(b"\n")?;
                        }
                        summary.snapshots += 1;
                    }
                    Record::SourcedSnapshot { .. } => unreachable!(),
                    Record::Update(data) => {
                        writer.write_msg(&data)?;
                        summary.updates += 1;
                    }
                }
                summary.records += 1;
            }
        }
    }
    writer.flush()?;
    if let Some(audit) = &mut audit {
        audit.flush()?;
    }
    Ok(summary)
}
