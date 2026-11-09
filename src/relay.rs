//! Bounded normalized-feed relay. Books live in the logical shard owner, not
//! both here and in each connection task.
use crate::model::BookEvent;
use anyhow::Context;
use chrono::{DateTime, Utc};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Source {
    pub replica: usize,
    pub generation: u64,
}

#[derive(Clone, Default)]
pub struct Control {
    pub pause: Option<PauseRequest>,
    pub pause_release: Option<PauseId>,
    pub fence: u64,
    pub restart: u64,
    pub refresh: BTreeMap<String, Refresh>,
    pub resubscribe: BTreeMap<String, Resubscribe>,
    /// Non-destructive native covering-snapshot requests. Unlike `refresh`,
    /// Source keeps the book initialized and its ordered tail flowing.
    pub probe: BTreeMap<String, Probe>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PauseId {
    pub generation: u64,
    pub nonce: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PauseRequest {
    pub generation: u64,
    pub nonce: u64,
    pub acquire_until: tokio::time::Instant,
    pub max_duration: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PauseEnd {
    Released,
    Expired,
    Superseded,
    Busy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resubscribe {
    pub incarnation: u64,
    pub generation: u64,
    pub nonce: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Refresh {
    pub incarnation: u64,
    pub generation: u64,
    pub nonce: u64,
}

/// Same identity tuple as `Refresh`: a consumed nonce, an old incarnation or
/// an old generation can never authorize a request for a new lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Probe {
    pub incarnation: u64,
    pub generation: u64,
    pub nonce: u64,
}

#[derive(Debug)]
pub enum Input {
    PauseStarted {
        source: Source,
        nonce: u64,
        until: tokio::time::Instant,
    },
    PauseEnded {
        source: Source,
        nonce: u64,
        reason: PauseEnd,
    },
    Unavailable {
        source: Source,
        symbol: String,
        incarnation: u64,
    },
    Begin {
        source: Source,
        admission: u64,
    },
    Data {
        source: Source,
        incarnation: u64,
        received_tick: tokio::time::Instant,
        event: BookEvent,
    },
    Invalid {
        source: Source,
        symbols: Option<Vec<String>>,
        reason: String,
    },
    Membership {
        source: Source,
        /// Present only for a locally completed fresh-session subscription;
        /// no native ACK or sid is manufactured by this evidence.
        roster: Option<BTreeMap<String, u64>>,
        wire: BTreeSet<String>,
        at: DateTime<Utc>,
    },
    Fence {
        source: Source,
        request: u64,
        at: DateTime<Utc>,
    },
    /// FIFO evidence that Source consumed these exact recovery controls.
    RecoveryStarted {
        source: Source,
        requests: BTreeMap<String, Resubscribe>,
    },
}
impl Input {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::Unavailable { symbol, .. } => symbol.capacity(),
                Self::Data { event, .. } => match event {
                    BookEvent::Snapshot(s) => {
                        s.symbol.capacity()
                            + (s.bids.capacity() + s.asks.capacity())
                                * std::mem::size_of::<crate::model::Level>()
                    }
                    BookEvent::Update(u) => u.symbol.capacity(),
                },
                Self::Invalid {
                    symbols, reason, ..
                } => {
                    reason.capacity()
                        + symbols
                            .as_ref()
                            .map(|s| {
                                s.capacity() * std::mem::size_of::<String>()
                                    + s.iter().map(String::capacity).sum::<usize>()
                            })
                            .unwrap_or(0)
                }
                Self::Membership { wire, roster, .. } => {
                    wire.iter().map(|s| s.capacity() + 96).sum::<usize>()
                        + roster.as_ref().map_or(0, |r| {
                            1024 + r.keys().map(|s| s.capacity() + 128).sum::<usize>()
                        })
                }
                Self::RecoveryStarted { requests, .. } => {
                    requests.keys().map(|s| s.capacity() + 128).sum()
                }
                _ => 0,
            }
    }
}

pub struct Admitted {
    pub input: Input,
    pub attempted_at: tokio::time::Instant,
    _permit: OwnedSemaphorePermit,
}
#[derive(Clone)]
pub struct Sender {
    tx: mpsc::Sender<Admitted>,
    bytes: Arc<Semaphore>,
}
pub fn channel(bytes: Arc<Semaphore>) -> (Sender, mpsc::Receiver<Admitted>) {
    let (tx, rx) = mpsc::channel(512);
    (Sender { tx, bytes }, rx)
}
impl Sender {
    fn admitted(&self, input: Input) -> anyhow::Result<Admitted> {
        let attempted_at = tokio::time::Instant::now();
        let n = u32::try_from(input.retained_bytes().max(1))?;
        let permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(n)
            .context("normalized relay byte budget exhausted")?;
        Ok(Admitted {
            input,
            attempted_at,
            _permit: permit,
        })
    }

    /// FIFO admission without waiting for either item or retained-byte space.
    /// Pause markers use this path so relay pressure cannot extend a lease.
    pub fn try_send(&self, input: Input) -> anyhow::Result<()> {
        self.tx
            .try_send(self.admitted(input)?)
            .context("normalized relay cannot immediately accept input")
    }

    pub async fn send(&self, input: Input) -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(15), self.tx.send(self.admitted(input)?))
            .await
            .context("normalized relay deadline exceeded")?
            .context("logical shard stopped before accepting input")
    }

    /// Same FIFO, bounded wait for BOTH retained bytes and one item slot.
    /// Contract: the caller holds no paused ingress stream, so waiting can
    /// never extend a pause lease. Actual `source::run` call sites:
    /// - `Begin`, after connect and before this generation subscribes;
    /// - a retirement fault report, after the retired stream was dropped;
    /// - the terminal `Invalid` of a session that ended without retirement.
    ///   Its socket is still open but unpolled and holds no pause (a paused
    ///   session can only end through a retirement or an ordered end marker);
    ///   Close follows. This is the bounded wait the plain `send` already had.
    ///
    /// Dropping the future returns any partially acquired bytes and its queue
    /// position.
    pub async fn send_waiting(&self, input: Input) -> anyhow::Result<()> {
        let attempted_at = tokio::time::Instant::now();
        let n = u32::try_from(input.retained_bytes().max(1))?;
        tokio::time::timeout(Duration::from_secs(15), async {
            let permit = self
                .bytes
                .clone()
                .acquire_many_owned(n)
                .await
                .context("normalized relay byte budget closed")?;
            let slot = self
                .tx
                .reserve()
                .await
                .context("logical shard stopped before accepting input")?;
            slot.send(Admitted {
                input,
                attempted_at,
                _permit: permit,
            });
            anyhow::Ok(())
        })
        .await
        .context("normalized relay deadline exceeded")?
    }
}
