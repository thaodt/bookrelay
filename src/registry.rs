//! Desired membership is committed only after a complete metadata scan.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::ensure;
use serde::Serialize;

use crate::catalog::Instrument;

#[derive(Clone, Debug, Default)]
pub struct Assignment {
    pub epoch: u64,
    pub symbols: Vec<String>,
    pub incarnations: BTreeMap<String, u64>,
}

#[derive(Clone)]
pub struct Registry {
    owners: BTreeMap<String, usize>,
    incarnations: BTreeMap<String, u64>,
    shards: Vec<BTreeSet<String>>,
    capacity: usize,
    max_shards: usize,
    max_symbols: usize,
    pub epoch: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct Changes {
    pub added: usize,
    pub retired: usize,
    pub desired: usize,
    pub shards: usize,
    pub epoch: u64,
}

impl Registry {
    pub fn new(capacity: usize, max_shards: usize, max_symbols: usize) -> anyhow::Result<Self> {
        ensure!(
            capacity > 0 && max_shards > 0 && max_symbols > 0,
            "invalid registry capacity"
        );
        Ok(Self {
            owners: BTreeMap::new(),
            incarnations: BTreeMap::new(),
            shards: vec![],
            capacity,
            max_shards,
            max_symbols,
            epoch: 0,
        })
    }

    pub fn assignments(&self) -> Vec<Assignment> {
        self.shards
            .iter()
            .map(|s| Assignment {
                epoch: self.epoch,
                symbols: s.iter().cloned().collect(),
                incarnations: s
                    .iter()
                    .map(|symbol| (symbol.clone(), self.incarnations[symbol]))
                    .collect(),
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.owners.len()
    }
    pub fn is_empty(&self) -> bool {
        self.owners.is_empty()
    }

    pub fn reconcile(&mut self, markets: &[Instrument], full: bool) -> anyhow::Result<Changes> {
        let mut desired: BTreeSet<String> = if full {
            BTreeSet::new()
        } else {
            self.owners.keys().cloned().collect()
        };
        for market in markets {
            if market.eligible {
                desired.insert(market.symbol.clone());
            } else {
                desired.remove(&market.symbol);
            }
        }
        self.replace(desired)
    }

    pub fn replace(&mut self, desired: BTreeSet<String>) -> anyhow::Result<Changes> {
        ensure!(
            desired.iter().all(|s| !s.is_empty()),
            "empty registry ticker"
        );
        ensure!(
            desired.len() <= self.max_symbols
                && desired.len().div_ceil(self.capacity) <= self.max_shards,
            "desired universe exceeds registry/connection budget"
        );
        if self.owners.keys().eq(desired.iter()) {
            return Ok(Changes {
                desired: self.len(),
                shards: self.shards.len(),
                epoch: self.epoch,
                ..Changes::default()
            });
        }
        let before = self.owners.len();
        let retired: Vec<_> = self
            .owners
            .keys()
            .filter(|s| !desired.contains(*s))
            .cloned()
            .collect();
        // Validate the whole transaction before changing any assignment.
        let epoch = self
            .epoch
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("registry epoch overflow"))?;
        for symbol in &retired {
            let owner = self.owners.remove(symbol).unwrap();
            self.shards[owner].remove(symbol);
            self.incarnations.remove(symbol);
        }
        let needed = desired.len().div_ceil(self.capacity);
        while self.shards.len() < needed {
            self.shards.push(BTreeSet::new());
        }
        for symbol in desired {
            if self.owners.contains_key(&symbol) {
                continue;
            }
            // Stable existing ownership; distribute additions by occupancy. Observed
            // per-shard rates are reported separately; never migrate a live book silently.
            let owner = self
                .shards
                .iter()
                .enumerate()
                .filter(|(_, s)| s.len() < self.capacity)
                .min_by_key(|(i, s)| (s.len(), *i))
                .map(|(i, _)| i)
                .unwrap();
            self.shards[owner].insert(symbol.clone());
            self.incarnations.insert(symbol.clone(), epoch);
            self.owners.insert(symbol, owner);
        }
        self.epoch = epoch;
        Ok(Changes {
            added: self.owners.len() + retired.len() - before,
            retired: retired.len(),
            desired: self.len(),
            shards: self.shards.len(),
            epoch,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn set(s: &[&str]) -> BTreeSet<String> {
        s.iter().map(|s| s.to_string()).collect()
    }
    #[test]
    fn membership_is_stable_bounded_and_capacity_failure_is_atomic() {
        let mut r = Registry::new(2, 2, 4).unwrap();
        r.replace(set(&["A", "B", "C"])).unwrap();
        let owner = r.owners["A"];
        r.replace(set(&["A", "C", "D", "E"])).unwrap();
        assert_eq!(r.owners["A"], owner);
        assert!(r.shards.iter().all(|s| s.len() <= 2));
        let previous = r.owners.clone();
        let epoch = r.epoch;
        assert!(r.replace(set(&["A", "B", "C", "D", "E"])).is_err());
        assert_eq!(r.owners, previous);
        assert_eq!(r.epoch, epoch);
        r.replace(BTreeSet::new()).unwrap();
        r.replace(set(&["A"])).unwrap();
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn coalesced_remove_and_readd_changes_incarnation() {
        let mut r = Registry::new(10, 1, 10).unwrap();
        r.replace(set(&["A"])).unwrap();
        let first = r.assignments()[0].incarnations["A"];
        r.replace(BTreeSet::new()).unwrap();
        r.replace(set(&["A"])).unwrap();
        assert!(r.assignments()[0].incarnations["A"] > first);
    }
    #[test]
    fn incremental_absence_does_not_retire_and_metadata_can_remove() {
        let mut r = Registry::new(10, 1, 10).unwrap();
        r.replace(set(&["A", "B"])).unwrap();
        let closed = Instrument {
            symbol: "A".into(),
            eligible: false,
        };
        r.reconcile(&[closed], false).unwrap();
        assert_eq!(r.owners.keys().cloned().collect::<Vec<_>>(), ["B"]);
        r.reconcile(&[], false).unwrap();
        assert_eq!(r.len(), 1);
        r.reconcile(&[], true).unwrap();
        assert!(r.is_empty());
    }
}
