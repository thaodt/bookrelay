use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader, Read},
    path::Path,
};

use anyhow::{Context, ensure};
use serde::Serialize;

use crate::{
    book::OrderBook,
    model::{Level, QuantityUpdate, Snapshot, Update},
    output::{AuditRow, CHUNK_SIZE, OrderingEvidence, SnapshotReason},
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
    },
};

#[derive(Debug, Default, Serialize)]
pub struct ReplayReport {
    pub comparable_clock_baselines_verified: u64,
    pub snapshot_only_baselines: u64,
    pub legacy_unspecified_baselines: u64,
    pub unverified_continuity_transitions: u64,
    pub records: u64,
    pub snapshots: u64,
    pub updates: u64,
    pub periodic_checkpoints_verified: u64,
    pub explicit_baseline_resets: u64,
    pub unclassified_snapshot_changes: u64,
    pub symbols: usize,
    pub source_baselines_verified: u64,
    pub failover_baselines_verified: u64,
}

struct Audit {
    comparable: u64,
    snapshot_only: u64,
    legacy: u64,
    unverified: u64,
    reader: BufReader<File>,
    next: Option<AuditRow>,
    sources: HashMap<String, crate::output::SourceStamp>,
    source_checks: u64,
    failovers: u64,
}

impl Audit {
    fn advance(&mut self) -> anyhow::Result<()> {
        let mut line = String::new();
        let count = self.reader.by_ref().take(16_385).read_line(&mut line)?;
        ensure!(count <= 16_384, "audit row exceeds 16 KiB");
        self.next = if count == 0 {
            None
        } else {
            Some(serde_json::from_str(&line)?)
        };
        Ok(())
    }

    fn reason(&mut self, ordinal: u64, symbol: &str) -> anyhow::Result<SnapshotReason> {
        let row = self.next.take().context("snapshot missing audit row")?;
        ensure!(
            row.record == ordinal && row.symbol == symbol,
            "audit snapshot ordinal/symbol mismatch"
        );
        if let Some(source) = row.source {
            match source.evidence {
                OrderingEvidence::ComparableClock => {
                    ensure!(
                        source.clock.is_some() && !source.unverified_cut,
                        "invalid comparable-clock evidence"
                    );
                    self.comparable += 1;
                }
                OrderingEvidence::SnapshotOnly => {
                    ensure!(
                        source.clock.is_none()
                            && source.ordinal.is_none()
                            && !source.complete_clock,
                        "snapshot-only provenance manufactured a clock"
                    );
                    self.snapshot_only += 1;
                }
                OrderingEvidence::LegacyUnspecified => self.legacy += 1,
            }
            if row.reason != SnapshotReason::Periodic && source.unverified_cut {
                self.unverified += 1;
            }
            ensure!(
                source.generation > 0 && source.output_epoch > 0 && source.incarnation > 0,
                "invalid source provenance identity"
            );
            ensure!(
                !source.complete_clock || source.clock.is_some() && source.ordinal.is_none(),
                "invalid complete-clock provenance"
            );
            ensure!(
                source.ordinal.is_none() || source.clock.is_some() && !source.complete_clock,
                "invalid partial-clock provenance"
            );
            if let Some(old) = self.sources.get(symbol) {
                ensure!(
                    source.incarnation >= old.incarnation,
                    "market incarnation regressed in audit"
                );
                if source.incarnation == old.incarnation {
                    ensure!(
                        row.reason != SnapshotReason::Initial,
                        "initial baseline reused a live incarnation"
                    );
                    if source.replica == old.replica {
                        ensure!(
                            source.generation >= old.generation,
                            "source generation regressed"
                        );
                    }
                    ensure!(
                        source.continuity_epoch >= old.continuity_epoch,
                        "continuity epoch regressed"
                    );
                    if row.reason == SnapshotReason::Periodic {
                        ensure!(
                            source.output_epoch == old.output_epoch
                                && source.evidence == old.evidence
                                && source.unverified_cut == old.unverified_cut
                                && source.replica == old.replica
                                && source.generation == old.generation
                                && source.continuity_epoch == old.continuity_epoch,
                            "periodic snapshot changed source without a baseline barrier"
                        );
                    } else {
                        if source.evidence == OrderingEvidence::SnapshotOnly
                            && (source.replica != old.replica
                                || source.generation != old.generation)
                        {
                            ensure!(
                                source.unverified_cut,
                                "snapshot-only source transition hid continuity uncertainty"
                            );
                        }
                        ensure!(
                            Some(source.output_epoch) == old.output_epoch.checked_add(1),
                            "source baseline output epoch did not advance exactly once"
                        );
                        if row.reason == SnapshotReason::Failover {
                            ensure!(
                                source.replica != old.replica
                                    || source.generation != old.generation,
                                "failover did not change source"
                            );
                        }
                    }
                    if source.continuity_epoch == old.continuity_epoch {
                        if let (Some(clock), Some(previous)) = (source.clock, old.clock) {
                            let next = crate::replica::Frontier {
                                clock,
                                ordinal: source.ordinal.unwrap_or(0),
                                complete: source.complete_clock,
                            };
                            let floor = crate::replica::Frontier {
                                clock: previous,
                                ordinal: old.ordinal.unwrap_or(0),
                                complete: old.complete_clock,
                            };
                            ensure!(
                                next.covers(floor),
                                "audit snapshot regressed within a continuity epoch"
                            );
                        }
                    } else {
                        ensure!(
                            row.reason == SnapshotReason::Recovery,
                            "continuity reset requires explicit recovery baseline"
                        );
                    }
                } else {
                    ensure!(
                        row.reason == SnapshotReason::Initial && source.output_epoch == 1,
                        "new incarnation lacks initial baseline"
                    );
                }
            } else {
                ensure!(
                    row.reason == SnapshotReason::Initial && source.output_epoch == 1,
                    "source audit starts without initial baseline"
                );
            }
            self.source_checks += 1;
            if row.reason == SnapshotReason::Failover {
                self.failovers += 1;
            }
            self.sources.insert(symbol.into(), source);
        } else {
            self.legacy += 1;
            ensure!(
                !self.sources.contains_key(symbol),
                "source provenance disappeared from audit"
            );
        }
        let reason = row.reason;
        self.advance()?;
        Ok(reason)
    }
}

pub fn verify(
    path: &Path,
    audit_path: Option<&Path>,
    max_symbols: usize,
) -> anyhow::Result<ReplayReport> {
    ensure!(
        std::fs::metadata(path)?.len() % (CHUNK_SIZE as u64) == 0,
        "truncated SBE file: incomplete chunk"
    );
    let mut audit = audit_path
        .map(|p| -> anyhow::Result<Audit> {
            let mut a = Audit {
                comparable: 0,
                snapshot_only: 0,
                legacy: 0,
                unverified: 0,
                reader: BufReader::new(File::open(p)?),
                next: None,
                sources: HashMap::new(),
                source_checks: 0,
                failovers: 0,
            };
            a.advance()?;
            Ok(a)
        })
        .transpose()?;
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    let mut books = HashMap::<String, OrderBook>::new();
    let mut report = ReplayReport::default();
    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoded) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoded)?;
                let reason = audit
                    .as_mut()
                    .map(|a| a.reason(report.records, &snapshot.symbol))
                    .transpose()?;
                ensure!(
                    books.contains_key(&snapshot.symbol) || books.len() < max_symbols,
                    "verifier symbol budget exceeded"
                );
                let book = books
                    .entry(snapshot.symbol.clone())
                    .or_insert_with(|| OrderBook::new(snapshot.symbol.clone()));
                let matches = if book.is_valid() {
                    let previous = book.snapshot(snapshot.received_at)?;
                    previous.bids == snapshot.bids && previous.asks == snapshot.asks
                } else {
                    false
                };
                match reason {
                    Some(SnapshotReason::Periodic) => {
                        ensure!(
                            matches,
                            "periodic checkpoint disagrees with preceding replay state for {}",
                            snapshot.symbol
                        );
                        report.periodic_checkpoints_verified += 1;
                    }
                    Some(_) => report.explicit_baseline_resets += 1,
                    None => {
                        if book.is_valid() && !matches {
                            report.unclassified_snapshot_changes += 1;
                        }
                    }
                }
                book.replace(Snapshot {
                    symbol: snapshot.symbol,
                    received_at: snapshot.received_at,
                    exchange_at: snapshot.exchange_timestamp,
                    clock: None,
                    bids: snapshot
                        .bids
                        .into_iter()
                        .map(|e| Level {
                            price: e.price_level,
                            quantity: e.quantity,
                        })
                        .collect(),
                    asks: snapshot
                        .asks
                        .into_iter()
                        .map(|e| Level {
                            price: e.price_level,
                            quantity: e.quantity,
                        })
                        .collect(),
                })?;
                report.snapshots += 1;
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(decoded)?;
                let book = books
                    .get_mut(&u.symbol)
                    .context("delta before symbol baseline")?;
                book.apply(Update {
                    symbol: u.symbol,
                    received_at: u.backtest_timestamp,
                    exchange_at: u.exchange_timestamp,
                    clock: None,
                    side: u.side,
                    price: u.price_level,
                    quantity: QuantityUpdate::Set(u.quantity),
                })?;
                report.updates += 1;
            }
            _ => anyhow::bail!("unknown SBE message"),
        }
        report.records += 1;
        if let Some(audit) = &audit {
            ensure!(
                audit
                    .next
                    .as_ref()
                    .is_none_or(|row| row.record >= report.records),
                "unconsumed or out-of-order audit row"
            );
        }
    }
    ensure!(
        audit.as_ref().is_none_or(|a| a.next.is_none()),
        "audit extends beyond SBE records"
    );
    if let Some(audit) = &audit {
        report.comparable_clock_baselines_verified = audit.comparable;
        report.snapshot_only_baselines = audit.snapshot_only;
        report.legacy_unspecified_baselines = audit.legacy;
        report.unverified_continuity_transitions = audit.unverified;
        report.source_baselines_verified = audit.source_checks;
        report.failover_baselines_verified = audit.failovers;
    }
    report.symbols = books.len();
    Ok(report)
}
