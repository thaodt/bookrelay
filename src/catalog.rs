//! Exchange-neutral discovery contract. Native eligibility and pagination
//! semantics stay in the catalog implementation.
use futures_util::future::BoxFuture;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Instrument {
    pub symbol: String,
    pub eligible: bool,
}

pub struct Inventory {
    pub instruments: Vec<Instrument>,
    /// Scan start, not end: changes during pagination belong in the next scan.
    pub watermark: i64,
    pub pages: usize,
    pub elapsed: Duration,
    /// Only a clean full scan may retire an instrument by its absence.
    /// Explicit `eligible=false` observations remain usable on uncertain scans.
    pub authoritative_for_retirement: bool,
    /// False when quarantined records leave desired coverage unknown.
    pub complete: bool,
    /// Bounded exchange-specific identity mappings and scan diagnostics.
    pub metadata: Option<serde_json::Value>,
}

pub trait Catalog: Send + Sync {
    fn supports_incremental(&self) -> bool {
        false
    }
    /// Fixed instruments still need catalog validation for some namespaces.
    fn validates_fixed_symbols(&self) -> bool {
        false
    }
    fn scan(&self, since: Option<i64>) -> BoxFuture<'_, anyhow::Result<Inventory>>;
}
