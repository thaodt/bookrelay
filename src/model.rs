use chrono::{DateTime, Utc};

use crate::Side;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Level {
    pub price: i64,
    pub quantity: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub symbol: String,
    pub received_at: DateTime<Utc>,
    pub exchange_at: DateTime<Utc>,
    pub clock: Option<u64>,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuantityUpdate {
    Change(i64),
    Set(i64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Update {
    pub symbol: String,
    pub received_at: DateTime<Utc>,
    pub exchange_at: DateTime<Utc>,
    pub clock: Option<u64>,
    pub side: Side,
    pub price: i64,
    pub quantity: QuantityUpdate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BookEvent {
    Snapshot(Snapshot),
    Update(Update),
}

impl BookEvent {
    pub fn symbol(&self) -> &str {
        match self {
            Self::Snapshot(s) => &s.symbol,
            Self::Update(u) => &u.symbol,
        }
    }
}
