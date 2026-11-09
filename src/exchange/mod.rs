pub mod kalshi;
pub mod polymarket;
pub mod polymarket_catalog;

use crate::model::BookEvent;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::{collections::HashMap, time::Duration};
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Default)]
pub enum Control {
    Subscribed(u64),
    Ok,
    Pong,
    Error {
        code: u64,
        message: String,
    },
    #[default]
    Other,
}

#[derive(Debug)]
pub struct Frame {
    pub sequence: Option<(u64, u64)>,
    pub request_id: Option<u64>,
    pub events: Vec<BookEvent>,
    pub control: Control,
}

#[derive(Clone, Copy, Debug)]
pub enum Rejection {
    Reconnect,
    Backoff(Duration),
    Fatal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum MembershipStrategy {
    Acknowledged,
    FreshSession,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum OrderingStrategy {
    ComparableClock,
    SnapshotOnly,
}
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct Contract {
    pub exchange: &'static str,
    pub profile: &'static str,
    pub instrument_namespace: &'static str,
    pub membership: MembershipStrategy,
    pub ordering: OrderingStrategy,
    pub sequence_detection: bool,
}
impl Contract {
    pub fn validate(self) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(
                (self.membership, self.ordering, self.sequence_detection),
                (
                    MembershipStrategy::Acknowledged,
                    OrderingStrategy::ComparableClock,
                    true
                ) | (
                    MembershipStrategy::FreshSession,
                    OrderingStrategy::SnapshotOnly,
                    false
                )
            ),
            "unsupported membership/ordering contract"
        );
        Ok(())
    }
}

/// Protocol-specific normalization stays outside the book and output layers.
pub trait ExchangeAdapter: Send + Sync {
    fn contract(&self) -> Contract;
    fn heartbeat(&self) -> Message;
    fn rejection(&self, code: u64) -> Rejection;
    fn decode(&self, text: &str, received_at: DateTime<Utc>) -> anyhow::Result<Frame>;
    fn subscribe(&self, request_id: u64, symbols: &[String]) -> anyhow::Result<Value>;
    fn update_subscription(
        &self,
        request_id: u64,
        sid: u64,
        symbols: &[String],
        add: bool,
    ) -> anyhow::Result<Value>;
    /// Absence of a native snapshot command must be handled by an explicit
    /// unsubscribe/resubscribe transaction, never a fabricated protocol command.
    fn get_snapshot(&self, request_id: u64, sid: u64, symbols: &[String]) -> anyhow::Result<Value>;
}

/// Scoped to a single connection generation; sequence is not per market.
#[derive(Default)]
pub struct SequenceTracker {
    last: HashMap<u64, u64>,
}

impl SequenceTracker {
    /// Returns true on gaps, repeats, or regressions; callers invalidate the whole sid.
    pub fn observe(&mut self, sid: u64, seq: u64) -> bool {
        self.last
            .insert(sid, seq)
            .is_some_and(|previous| previous.checked_add(1) != Some(seq))
    }
}

/// One configured exchange supplies protocol, authentication and catalog.
/// Fixed-symbol collectors may omit the catalog entirely.
pub struct Configuration {
    pub protocol: std::sync::Arc<dyn ExchangeAdapter>,
    pub auth: Option<std::sync::Arc<dyn crate::transport::RequestAuthenticator>>,
    pub catalog: Option<std::sync::Arc<dyn crate::catalog::Catalog>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn incompatible_contracts_cannot_advertise_native_guarantees() {
        let mut contract = polymarket::Polymarket.contract();
        assert!(contract.validate().is_ok());
        contract.ordering = OrderingStrategy::ComparableClock;
        assert!(contract.validate().is_err());
        let mut native = kalshi::Kalshi.contract();
        assert!(native.validate().is_ok());
        native.sequence_detection = false;
        assert!(native.validate().is_err());
    }
}
