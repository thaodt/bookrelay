use std::time::Duration;

use anyhow::{Context, ensure};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{Control, ExchangeAdapter, Frame, Rejection};
use crate::{
    Side,
    fixed_point::{SCALE, parse},
    model::{BookEvent, Level, QuantityUpdate, Snapshot, Update},
};
use tokio_tungstenite::tungstenite::Message;

pub const REST_URL: &str = "https://external-api.kalshi.com/trade-api/v2";
pub const WS_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
/// Nine 250-market snapshot batches at 500 ms spacing leave about one second
/// of the existing five-second failover bound for RTT and scheduling. The
/// socket admission cap is unchanged; oversize discovery fails explicitly.
pub const DEFAULT_SYMBOLS_PER_CONNECTION: usize = 2250;

#[derive(Default)]
pub struct Kalshi;

pub fn configure(
    auth: std::sync::Arc<crate::auth::Credentials>,
    rest_url: &str,
    max_symbols: usize,
) -> anyhow::Result<super::Configuration> {
    Ok(super::Configuration {
        protocol: std::sync::Arc::new(Kalshi),
        auth: Some(auth),
        catalog: Some(std::sync::Arc::new(crate::discovery::Discovery::new(
            rest_url,
            max_symbols,
        )?)),
    })
}

#[derive(Deserialize)]
struct Envelope {
    #[serde(rename = "type")]
    kind: String,
    sid: Option<u64>,
    seq: Option<u64>,
    id: Option<u64>,
    sending_ts_ms: Option<i64>,
    #[serde(default)]
    msg: Value,
}

#[derive(Deserialize)]
struct WireSnapshot {
    market_ticker: String,
    clock: Option<u64>,
    yes_dollars_fp: Option<Vec<(String, String)>>,
    no_dollars_fp: Option<Vec<(String, String)>>,
}

#[derive(Deserialize)]
struct WireDelta {
    market_ticker: String,
    clock: Option<u64>,
    side: String,
    price_dollars: String,
    delta_fp: String,
    ts_ms: Option<i64>,
    ts: Option<String>,
}

fn time(
    ms: Option<i64>,
    iso: Option<&str>,
    sending_ms: Option<i64>,
) -> anyhow::Result<DateTime<Utc>> {
    let value = if let Some(ms) = ms {
        DateTime::from_timestamp_millis(ms).context("invalid event timestamp")?
    } else if let Some(iso) = iso {
        DateTime::parse_from_rfc3339(iso)
            .context("invalid event timestamp")?
            .with_timezone(&Utc)
    } else {
        DateTime::from_timestamp_millis(
            sending_ms.context("data message has no exchange timestamp")?,
        )
        .context("invalid sending timestamp")?
    };
    ensure!(
        value.timestamp() >= 0,
        "exchange timestamp predates Unix epoch"
    );
    Ok(value)
}

fn price(input: &str, complement: bool) -> anyhow::Result<i64> {
    let value = parse(input)?;
    ensure!((0..=SCALE).contains(&value), "price outside [0,1]");
    Ok(if complement { SCALE - value } else { value })
}

impl ExchangeAdapter for Kalshi {
    fn contract(&self) -> super::Contract {
        super::Contract {
            exchange: "kalshi",
            profile: "real-time-v2",
            instrument_namespace: "market_ticker",
            membership: super::MembershipStrategy::Acknowledged,
            ordering: super::OrderingStrategy::ComparableClock,
            sequence_detection: true,
        }
    }
    fn heartbeat(&self) -> Message {
        Message::Ping(vec![].into())
    }
    fn rejection(&self, code: u64) -> Rejection {
        match code {
            7 | 18 | 25 => Rejection::Reconnect,
            27 => Rejection::Backoff(Duration::from_secs(10)),
            _ => Rejection::Fatal,
        }
    }

    fn decode(&self, text: &str, received_at: DateTime<Utc>) -> anyhow::Result<Frame> {
        let envelope: Envelope = serde_json::from_str(text).context("invalid Kalshi envelope")?;
        let sequence = match (envelope.sid, envelope.seq) {
            (Some(sid), Some(seq)) => Some((sid, seq)),
            (_, None) => None,
            (None, Some(_)) => anyhow::bail!("sequence without subscription ID"),
        };
        if matches!(
            envelope.kind.as_str(),
            "orderbook_snapshot" | "orderbook_delta"
        ) {
            ensure!(sequence.is_some(), "orderbook message missing sid/seq");
        }
        let mut frame = Frame {
            sequence,
            request_id: envelope.id,
            events: vec![],
            control: Control::Other,
        };
        match envelope.kind.as_str() {
            "subscribed" => {
                frame.control = Control::Subscribed(
                    envelope
                        .msg
                        .get("sid")
                        .and_then(Value::as_u64)
                        .context("ACK missing sid")?,
                )
            }
            "ok" => frame.control = Control::Ok,
            "error" => {
                frame.control = Control::Error {
                    code: envelope
                        .msg
                        .get("code")
                        .and_then(Value::as_u64)
                        .context("error missing code")?,
                    message: envelope
                        .msg
                        .get("msg")
                        .and_then(Value::as_str)
                        .unwrap_or("exchange error")
                        .to_owned(),
                }
            }
            "orderbook_snapshot" => {
                let wire: WireSnapshot = serde_json::from_value(envelope.msg)?;
                ensure!(!wire.market_ticker.is_empty(), "empty market ticker");
                let levels = |entries: Option<Vec<(String, String)>>,
                              complement|
                 -> anyhow::Result<Vec<Level>> {
                    entries
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(p, q)| {
                            let quantity = parse(&q)?;
                            ensure!(quantity >= 0, "negative snapshot quantity");
                            Ok(Level {
                                price: price(&p, complement)?,
                                quantity,
                            })
                        })
                        .collect()
                };
                frame.events.push(BookEvent::Snapshot(Snapshot {
                    symbol: wire.market_ticker,
                    received_at,
                    exchange_at: time(None, None, envelope.sending_ts_ms)?,
                    clock: wire.clock,
                    bids: levels(wire.yes_dollars_fp, false)?,
                    asks: levels(wire.no_dollars_fp, true)?,
                }));
            }
            "orderbook_delta" => {
                let wire: WireDelta = serde_json::from_value(envelope.msg)?;
                let side = match wire.side.as_str() {
                    "yes" => Side::Buy,
                    "no" => Side::Sell,
                    _ => anyhow::bail!("unknown Kalshi side"),
                };
                ensure!(!wire.market_ticker.is_empty(), "empty market ticker");
                frame.events.push(BookEvent::Update(Update {
                    symbol: wire.market_ticker,
                    received_at,
                    exchange_at: time(wire.ts_ms, wire.ts.as_deref(), envelope.sending_ts_ms)?,
                    clock: wire.clock,
                    side,
                    price: price(&wire.price_dollars, side == Side::Sell)?,
                    quantity: QuantityUpdate::Change(parse(&wire.delta_fp)?),
                }));
            }
            _ => {}
        }
        Ok(frame)
    }

    fn subscribe(&self, request_id: u64, symbols: &[String]) -> anyhow::Result<Value> {
        Ok(
            json!({"id":request_id,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":symbols,"update_interval_ms":0,"use_yes_price":false}}),
        )
    }

    fn update_subscription(
        &self,
        request_id: u64,
        sid: u64,
        symbols: &[String],
        add: bool,
    ) -> anyhow::Result<Value> {
        Ok(
            json!({"id":request_id,"cmd":"update_subscription","params":{
            "sid":sid,"market_tickers":symbols,
            "action":if add {"add_markets"} else {"delete_markets"}}}),
        )
    }

    fn get_snapshot(&self, request_id: u64, sid: u64, symbols: &[String]) -> anyhow::Result<Value> {
        Ok(
            json!({"id":request_id,"cmd":"update_subscription","params":{"sid":sid,"market_tickers":symbols,"action":"get_snapshot"}}),
        )
    }
}

pub use super::SequenceTracker;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_side_and_no_to_yes_complement_are_normalized() {
        let raw = r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"sending_ts_ms":1700000000000,"msg":{"market_ticker":"TEST","no_dollars_fp":[["0.5600","17.25"]]}}"#;
        let frame = Kalshi.decode(raw, Utc::now()).unwrap();
        let BookEvent::Snapshot(s) = &frame.events[0] else {
            panic!("expected snapshot")
        };
        assert!(s.bids.is_empty());
        assert_eq!(
            s.asks,
            vec![Level {
                price: 440_000,
                quantity: 17_250_000
            }]
        );
        assert_eq!(frame.sequence, Some((1, 1)));
    }

    #[test]
    fn delta_preserves_negative_change_and_event_time_without_float_conversion() {
        let raw = r#"{"type":"orderbook_delta","sid":1,"seq":2,"sending_ts_ms":1700000000001,"msg":{"market_ticker":"TEST","clock":9007199254740993,"side":"no","price_dollars":"0.5600","delta_fp":"-2.25","ts_ms":1700000000000}}"#;
        let receive = Utc::now();
        let frame = Kalshi.decode(raw, receive).unwrap();
        let BookEvent::Update(u) = &frame.events[0] else {
            panic!("expected update")
        };
        assert_eq!(u.side, Side::Sell);
        assert_eq!(u.price, 440_000);
        assert_eq!(u.quantity, QuantityUpdate::Change(-2_250_000));
        assert_eq!(u.exchange_at.timestamp_millis(), 1700000000000);
        assert_eq!(u.received_at, receive);
        assert_eq!(u.clock, Some(9007199254740993));
    }

    #[test]
    fn sequence_includes_control_envelopes_and_is_scoped_by_sid() {
        let mut tracker = SequenceTracker::default();
        assert!(!tracker.observe(1, 1));
        assert!(!tracker.observe(2, 40));
        let ack = Kalshi
            .decode(r#"{"type":"ok","sid":1,"seq":2,"msg":{}}"#, Utc::now())
            .unwrap();
        let (sid, seq) = ack.sequence.unwrap();
        assert!(!tracker.observe(sid, seq));
        assert!(!tracker.observe(1, 3));
        assert!(tracker.observe(1, 5));
        assert!(tracker.observe(1, 5));
        assert!(!tracker.observe(2, 41));
        assert!(!SequenceTracker::default().observe(1, 1));
    }

    #[test]
    fn snapshot_recovery_does_not_reissue_subscribe() {
        let command = Kalshi.get_snapshot(8, 2, &["TEST".into()]).unwrap();
        assert_eq!(command["cmd"], "update_subscription");
        assert_eq!(command["params"]["action"], "get_snapshot");
        assert_eq!(command["params"]["sid"], 2);
    }

    #[test]
    fn data_without_sequence_cannot_initialize_a_trusted_book() {
        let raw = r#"{"type":"orderbook_snapshot","sending_ts_ms":1700000000000,"msg":{"market_ticker":"TEST"}}"#;
        assert!(Kalshi.decode(raw, Utc::now()).is_err());
    }
}
