//! A book has one mutable owner. No transport, timers, or locks live here.

use std::collections::BTreeMap;

use anyhow::{Context, ensure};
use chrono::{DateTime, Utc};

use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};
use crate::{
    Side,
    model::{Level, QuantityUpdate, Snapshot, Update},
};

#[derive(Debug)]
pub struct OrderBook {
    symbol: String,
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    exchange_at: Option<DateTime<Utc>>,
    valid: bool,
    pub clock: Option<u64>,
}

impl OrderBook {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            exchange_at: None,
            valid: false,
            clock: None,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.valid
    }

    pub fn invalidate(&mut self) {
        self.valid = false;
    }

    /// Compares price levels of two trusted books; clocks and timestamps are
    /// the caller's concern.
    pub fn same_levels(&self, other: &Self) -> bool {
        self.valid && other.valid && self.bids == other.bids && self.asks == other.asks
    }

    pub fn replace(&mut self, snapshot: Snapshot) -> anyhow::Result<()> {
        let replacement = (|| {
            ensure!(
                snapshot.symbol == self.symbol,
                "snapshot belongs to a different symbol"
            );
            Ok::<_, anyhow::Error>((levels(snapshot.bids)?, levels(snapshot.asks)?))
        })();
        match replacement {
            Ok((bids, asks)) => {
                self.bids = bids;
                self.asks = asks;
                self.exchange_at = Some(snapshot.exchange_at);
                self.clock = snapshot.clock;
                self.valid = true;
                Ok(())
            }
            Err(error) => {
                self.invalidate();
                Err(error)
            }
        }
    }

    pub fn apply(&mut self, update: Update) -> anyhow::Result<PriceLevelUpdateDomain> {
        let result = self.apply_checked(update);
        if result.is_err() {
            self.invalidate();
        }
        result
    }

    fn apply_checked(&mut self, update: Update) -> anyhow::Result<PriceLevelUpdateDomain> {
        ensure!(self.valid, "update without a valid snapshot");
        ensure!(
            update.symbol == self.symbol,
            "update belongs to a different symbol"
        );
        validate_price(update.price)?;
        let side = match update.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let quantity = match update.quantity {
            QuantityUpdate::Change(change) => side
                .get(&update.price)
                .copied()
                .unwrap_or(0)
                .checked_add(change)
                .context("quantity overflow")?,
            QuantityUpdate::Set(quantity) => quantity,
        };
        ensure!(
            quantity >= 0,
            "negative resulting quantity; book must be resynchronized"
        );
        if quantity == 0 {
            side.remove(&update.price);
        } else {
            side.insert(update.price, quantity);
        }
        self.exchange_at = Some(update.exchange_at);
        self.clock = update.clock.or(self.clock);
        Ok(PriceLevelUpdateDomain {
            backtest_timestamp: update.received_at,
            exchange_timestamp: update.exchange_at,
            price_level: update.price,
            quantity,
            side: update.side,
            symbol: self.symbol.clone(),
        })
    }

    pub fn snapshot(&self, received_at: DateTime<Utc>) -> anyhow::Result<SnapshotDomain> {
        ensure!(self.valid, "cannot publish an invalid book");
        Ok(SnapshotDomain {
            received_at,
            exchange_timestamp: self.exchange_at.context("book has no exchange timestamp")?,
            symbol: self.symbol.clone(),
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
        })
    }
}

fn validate_price(price: i64) -> anyhow::Result<()> {
    ensure!(price >= 0, "negative book price");
    Ok(())
}

fn levels(entries: Vec<Level>) -> anyhow::Result<BTreeMap<i64, i64>> {
    let mut levels = BTreeMap::new();
    for entry in entries {
        validate_price(entry.price)?;
        ensure!(entry.quantity >= 0, "negative snapshot quantity");
        ensure!(
            levels.insert(entry.price, entry.quantity).is_none(),
            "duplicate snapshot price"
        );
    }
    levels.retain(|_, quantity| *quantity != 0);
    Ok(levels)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timestamp() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
    }
    fn initial() -> Snapshot {
        Snapshot {
            symbol: "TEST".into(),
            received_at: timestamp(),
            exchange_at: timestamp(),
            clock: Some(10),
            bids: vec![Level {
                price: 420_000,
                quantity: 100_000_000,
            }],
            asks: vec![],
        }
    }
    fn update(quantity: QuantityUpdate) -> Update {
        Update {
            symbol: "TEST".into(),
            received_at: timestamp(),
            exchange_at: timestamp(),
            clock: Some(11),
            side: Side::Buy,
            price: 420_000,
            quantity,
        }
    }

    #[test]
    fn generic_book_supports_prices_above_one_while_codecs_validate_venue_ranges() {
        let mut snapshot = initial();
        snapshot.bids[0].price = 123_450_000;
        let mut book = OrderBook::new("TEST".into());
        book.replace(snapshot).unwrap();
        let mut change = update(QuantityUpdate::Set(7_000_000));
        change.price = 123_450_000;
        let record = book.apply(change).unwrap();
        assert_eq!(record.price_level, 123_450_000);
        assert_eq!(record.quantity, 7_000_000);
    }

    #[test]
    fn converts_changes_to_absolute_quantities_and_emits_zero_on_deletion() {
        let mut book = OrderBook::new("TEST".into());
        book.replace(initial()).unwrap();
        assert_eq!(
            book.apply(update(QuantityUpdate::Change(-25_000_000)))
                .unwrap()
                .quantity,
            75_000_000
        );
        assert_eq!(
            book.apply(update(QuantityUpdate::Change(-75_000_000)))
                .unwrap()
                .quantity,
            0
        );
        assert!(book.snapshot(timestamp()).unwrap().bids.is_empty());
    }

    #[test]
    fn set_updates_do_not_accumulate_and_snapshots_replace_both_sides() {
        let mut book = OrderBook::new("TEST".into());
        book.replace(initial()).unwrap();
        for _ in 0..2 {
            assert_eq!(
                book.apply(update(QuantityUpdate::Set(7))).unwrap().quantity,
                7
            );
        }
        let mut replacement = initial();
        replacement.bids.clear();
        replacement.asks.push(Level {
            price: 440_000,
            quantity: 17_000_000,
        });
        book.replace(replacement).unwrap();
        let snap = book.snapshot(timestamp()).unwrap();
        assert!(snap.bids.is_empty());
        assert_eq!(snap.asks[0].quantity, 17_000_000);
    }

    #[test]
    fn invalid_state_requires_a_new_snapshot() {
        let mut book = OrderBook::new("TEST".into());
        assert!(book.apply(update(QuantityUpdate::Change(1))).is_err());
        book.replace(initial()).unwrap();
        assert!(
            book.apply(update(QuantityUpdate::Change(-100_000_001)))
                .is_err()
        );
        assert!(!book.is_valid());
        assert!(book.snapshot(timestamp()).is_err());
        assert!(book.apply(update(QuantityUpdate::Set(1))).is_err());
        book.replace(initial()).unwrap();
        assert!(book.is_valid());
    }

    #[test]
    fn overflow_and_malformed_replacements_fail_closed() {
        let mut book = OrderBook::new("TEST".into());
        book.replace(initial()).unwrap();
        book.apply(update(QuantityUpdate::Set(i64::MAX))).unwrap();
        assert!(book.apply(update(QuantityUpdate::Change(1))).is_err());
        let mut invalid = initial();
        invalid.bids.push(invalid.bids[0].clone());
        assert!(book.replace(invalid).is_err());
        assert!(!book.is_valid());
        let mut invalid = initial();
        invalid.asks.push(Level {
            price: 440_000,
            quantity: -1,
        });
        assert!(book.replace(invalid).is_err());
    }

    #[test]
    fn snapshots_are_sorted_best_first_and_preserve_receive_time() {
        let mut s = initial();
        s.bids.push(Level {
            price: 430_000,
            quantity: 2,
        });
        s.asks = vec![
            Level {
                price: 450_000,
                quantity: 2,
            },
            Level {
                price: 440_000,
                quantity: 3,
            },
        ];
        let mut book = OrderBook::new("TEST".into());
        book.replace(s).unwrap();
        let out = book.snapshot(timestamp()).unwrap();
        assert_eq!(out.bids[0].price_level, 430_000);
        assert_eq!(out.asks[0].price_level, 440_000);
        let mut u = update(QuantityUpdate::Set(1));
        u.received_at += chrono::Duration::milliseconds(5);
        let expected = u.received_at;
        assert_eq!(book.apply(u).unwrap().backtest_timestamp, expected);
    }

    #[test]
    fn mixed_update_sequences_match_an_independent_vector_reference() {
        let mut book = OrderBook::new("TEST".into());
        let mut empty = initial();
        empty.bids.clear();
        book.replace(empty).unwrap();
        let mut reference = Vec::<(Side, i64, i64)>::new();
        let mut seed = 0x5eed_u64;
        for step in 0..2000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let side = if seed & 1 == 0 { Side::Buy } else { Side::Sell };
            let price = 100_000 + ((seed >> 4) % 15) as i64 * 10_000;
            let quantity = ((seed >> 12) % 20) as i64 * 250_000;
            let previous = reference
                .iter()
                .find(|&&(s, p, _)| s == side && p == price)
                .map_or(0, |entry| entry.2);
            let mut event = update(if seed & 2 == 0 {
                QuantityUpdate::Change(quantity - previous)
            } else {
                QuantityUpdate::Set(quantity)
            });
            event.side = side;
            event.price = price;
            assert_eq!(book.apply(event).unwrap().quantity, quantity);
            reference.retain(|&(s, p, _)| s != side || p != price);
            if quantity > 0 {
                reference.push((side, price, quantity));
            }
            if step % 31 == 0 {
                let actual = book.snapshot(timestamp()).unwrap();
                let mut bids: Vec<_> = reference
                    .iter()
                    .filter(|e| e.0 == Side::Buy)
                    .map(|e| (e.1, e.2))
                    .collect();
                let mut asks: Vec<_> = reference
                    .iter()
                    .filter(|e| e.0 == Side::Sell)
                    .map(|e| (e.1, e.2))
                    .collect();
                bids.sort_by_key(|a| std::cmp::Reverse(a.0));
                asks.sort();
                assert_eq!(
                    actual
                        .bids
                        .iter()
                        .map(|e| (e.price_level, e.quantity))
                        .collect::<Vec<_>>(),
                    bids
                );
                assert_eq!(
                    actual
                        .asks
                        .iter()
                        .map(|e| (e.price_level, e.quantity))
                        .collect::<Vec<_>>(),
                    asks
                );
            }
        }
    }
}
