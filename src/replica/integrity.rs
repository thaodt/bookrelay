//! Native integrity recovery never clears an unrelated symbol's publication floor.
use super::{Frontier, Owner};
use crate::{health::Activity, relay::Source};
use anyhow::{Context, ensure};

impl Owner {
    pub(super) fn reject_book(
        &mut self,
        source: Source,
        symbol: &str,
        why: &str,
    ) -> anyhow::Result<()> {
        let book = self.feeds[source.replica].books.get_mut(symbol).unwrap();
        book.rejected_baselines = book.rejected_baselines.saturating_add(1);
        let rejected = book.rejected_baselines;
        self.stats.invalid_books += 1;
        eprintln!(
            "book quarantine: symbol={symbol} source={source:?} incarnation={} rejection={rejected} reason={why}",
            self.selected[symbol].incarnation
        );
        self.invalidate(source, Some(std::slice::from_ref(&symbol.to_owned())));
        ensure!(
            rejected <= self.health.max_resubscriptions,
            "semantic book recovery exhausted: {symbol}"
        );
        self.refresh(source.replica, symbol);
        self.choose(symbol)
    }
    pub(super) fn usable_baseline(&mut self, slot: usize, symbol: &str) {
        let book = self.feeds[slot].books.get_mut(symbol).unwrap();
        if book
            .catchup
            .is_some_and(|(floor, _)| book.frontier.is_some_and(|f| f.covers(floor)))
        {
            book.catchup = None;
            self.waiting_catchup.remove(&(slot, symbol.to_owned()));
        }
        // Readiness already requires the frozen initialization witness and
        // native membership. A continuously advancing peer must not move that
        // recovery goal forever; publication switches still check current floor.
        if self.ready(slot, symbol) {
            let book = self.feeds[slot].books.get_mut(symbol).unwrap();
            book.rejected_baselines = 0;
            book.initialization_failures = 0;
            book.recovery = None;
            book.probe_quarantined = false;
            if book
                .probe
                .is_some_and(|goal| book.frontier.is_some_and(|f| f.covers(goal)))
            {
                book.probe = None;
            }
            book.activity.initialized(tokio::time::Instant::now());
        }
    }

    pub(super) fn reject_clock(
        &mut self,
        symbol: &str,
        source: Source,
        next: Option<Frontier>,
        // Incoming (bid, ask) counts when this path measured them; None must
        // not be read as an empty book.
        levels: Option<(usize, usize)>,
        kind: &str,
    ) -> anyhow::Result<()> {
        let book = &self.feeds[source.replica].books[symbol];
        let state = &self.selected[symbol];
        let healthy_publisher = state
            .active
            .is_some_and(|publisher| publisher != source && self.ready(publisher.replica, symbol));
        let probe_rejected = book.probe.is_some() || book.probe_quarantined;
        let rejected = book.rejected_baselines.saturating_add(1);
        // A started health resubscribe is optional work. Spending its budget
        // ends that operation rather than the process, but only while a
        // different ready publisher serves; an unstarted one is still fenced.
        let recovery_spent = rejected > self.health.max_resubscriptions
            && book.recovery.is_some_and(|(_, started)| started);
        eprintln!(
            "native clock quarantine: symbol={symbol} source={source:?} incarnation={} active={} old_frontier={:?} published_floor={:?} next={next:?} levels={levels:?} recovery={:?} planned={} rejection={rejected} kind={kind}",
            state.incarnation,
            state.active == Some(source),
            book.frontier,
            state.floor,
            book.recovery,
            self.planned.is_some(),
        );
        self.stats.clock_regressions += 1;
        self.feeds[source.replica]
            .books
            .get_mut(symbol)
            .unwrap()
            .rejected_baselines = rejected;
        self.invalidate(source, Some(std::slice::from_ref(&symbol.to_owned())));
        if healthy_publisher && (probe_rejected || recovery_spent) {
            // The failed standby is genuinely unusable, and the rejection
            // counter remains owner-owned across reconnects. Do not create a
            // reject -> refresh -> reject loop while another publisher still
            // serves the frozen floor. Deltas remain quarantined until a real
            // usable snapshot arrives; planned work honestly defers meanwhile.
            let book = self.feeds[source.replica].books.get_mut(symbol).unwrap();
            book.probe_quarantined = true;
            book.catchup = None;
            if recovery_spent {
                // Only the logical operation ends. The Source keeps its accepted
                // subscription and controls, and no readiness or completion is
                // credited: Activity stays uninitialized until a usable baseline.
                book.recovery = None;
                book.activity.recovering = false;
                eprintln!(
                    "health recovery exhausted: symbol={symbol} source={source:?} rejected={rejected}; publisher retained, no completion credit"
                );
            }
            self.waiting_catchup
                .remove(&(source.replica, symbol.to_owned()));
            return self.choose(symbol);
        }
        // Raw snapshots and reconnects cannot clear this owner-owned budget.
        ensure!(
            rejected <= self.health.max_resubscriptions,
            "native semantic recovery exhausted: symbol={symbol} source={source:?} rejected={rejected}"
        );
        if self.replicas == 1 {
            self.reset_symbol(symbol, kind)?;
        } else {
            self.refresh(source.replica, symbol);
            self.choose(symbol)?;
        }
        Ok(())
    }

    pub(super) fn reset_symbol(&mut self, symbol: &str, why: &str) -> anyhow::Result<()> {
        self.cancel_planned();
        self.cancel_pauses();
        self.rotation = None;
        let state = self.selected.get_mut(symbol).unwrap();
        eprintln!(
            "symbol continuity reset: symbol={symbol} reason={why}; fencing fresh source admissions"
        );
        self.stats.hard_resets += 1;
        if state.ever && !state.recovery {
            self.stats.continuity_gaps += 1;
        }
        state.continuity_epoch = state
            .continuity_epoch
            .checked_add(1)
            .context("continuity epoch overflow")?;
        state.previous = state.active.take().or(state.previous);
        state.floor = None;
        state.recovery = state.ever;
        state.candidate = None;
        state.peer_ahead = None;
        state.peer_mark = None;
        state.behind_since = None;
        state.unpublished_since = None;
        for (slot, feed) in self.feeds.iter_mut().enumerate() {
            let book = feed.books.get_mut(symbol).unwrap();
            book.min_generation = feed
                .generation
                .checked_add(1)
                .context("generation floor overflow")?;
            book.min_admission = feed
                .control
                .borrow()
                .restart
                .checked_add(1)
                .context("admission floor overflow")?;
            book.value.invalidate();
            book.frontier = None;
            book.recovery = None;
            book.catchup = None;
            book.probe = None;
            book.probe_quarantined = false;
            book.activity = Activity::default();
            self.reset_restarts.insert(slot);
        }
        self.waiting_catchup.retain(|(_, s)| s != symbol);
        Ok(())
    }
    pub(super) fn integrity_tick(&mut self) -> anyhow::Result<()> {
        // Only currently waiting books enter this bounded index. Healthy books
        // incur no extra full-universe scan or timer task on every shard tick.
        let expired: Vec<_> = self
            .waiting_catchup
            .iter()
            .filter_map(|(slot, symbol)| {
                let feed = &self.feeds[*slot];
                let book = &feed.books[symbol];
                let (_, acquisition) = book.catchup?;
                let state = &self.selected[symbol];
                let healthy_publisher = state.active.is_some_and(|publisher| {
                    publisher.replica != *slot && self.ready(publisher.replica, symbol)
                });
                // A healthy publisher gives its standby the existing bounded
                // probe-acquisition window. This is never paused ingress or
                // time without a publisher. Real publication loss owns its
                // separate five-second timer, even after an old acquisition.
                let (since, limit) = if state.active.is_none() {
                    (
                        state.unpublished_since.unwrap_or(acquisition),
                        std::time::Duration::from_secs(5),
                    )
                } else if healthy_publisher && book.probe.is_some() && book.rejected_baselines == 0
                {
                    (acquisition, self.health.rotation_timeout)
                } else {
                    (acquisition, std::time::Duration::from_secs(5))
                };
                (book.value.is_valid() && since.elapsed() >= limit).then(|| {
                    (
                        symbol.clone(),
                        Source {
                            replica: *slot,
                            generation: feed.generation,
                        },
                        book.frontier,
                    )
                })
            })
            .collect();
        for (symbol, source, frontier) in expired {
            // Earlier expiry handling may have reset this symbol already.
            if self.feeds[source.replica].books[&symbol].catchup.is_some() {
                let covering = (0..self.replicas).any(|slot| {
                    self.ready(slot, &symbol)
                        && self.selected[&symbol].floor.is_none_or(|floor| {
                            self.feeds[slot].books[&symbol]
                                .frontier
                                .is_some_and(|f| f.covers(floor))
                        })
                });
                if self.selected[&symbol].active.is_none() && !covering {
                    self.reset_symbol(&symbol, "fresh baseline failed frozen publication floor")?;
                } else {
                    self.reject_clock(
                        &symbol,
                        source,
                        frontier,
                        None,
                        "fresh baseline failed frozen publication floor",
                    )?;
                }
            }
        }
        Ok(())
    }
}
