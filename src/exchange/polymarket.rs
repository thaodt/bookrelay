//! Raw public CLOB market wire format, not the unified SDK's camelCase events.
//! Asset IDs are instruments. Hashes/timestamps are not fabricated sequences.
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

pub const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
pub const REST_URL: &str = "https://gamma-api.polymarket.com";

#[derive(Default)]
pub struct Polymarket;

/// Public CLOB v1 market data uses Gamma metadata and no request signer.
pub fn configure(
    rest_url: &str,
    max_symbols: usize,
    symbols: Vec<String>,
) -> anyhow::Result<super::Configuration> {
    Ok(super::Configuration {
        protocol: std::sync::Arc::new(Polymarket),
        auth: None,
        catalog: Some(std::sync::Arc::new(
            super::polymarket_catalog::PolymarketCatalog::new(rest_url, max_symbols, symbols)?,
        )),
    })
}

#[derive(Deserialize)]
struct OrderSummary {
    price: String,
    size: String,
}
#[derive(Deserialize)]
struct WireBook {
    asset_id: String,
    timestamp: String,
    bids: Vec<OrderSummary>,
    asks: Vec<OrderSummary>,
}
#[derive(Deserialize)]
struct WireChange {
    asset_id: String,
    price: String,
    size: String,
    side: String,
}
#[derive(Deserialize)]
struct WireChanges {
    timestamp: String,
    price_changes: Vec<WireChange>,
}

fn timestamp(text: &str) -> anyhow::Result<DateTime<Utc>> {
    let ms: i64 = text.parse().context("invalid Polymarket timestamp")?;
    ensure!(ms >= 0, "exchange timestamp predates Unix epoch");
    DateTime::from_timestamp_millis(ms).context("invalid Polymarket timestamp")
}
fn price(text: &str) -> anyhow::Result<i64> {
    let price = parse(text)?;
    ensure!((0..=SCALE).contains(&price), "price outside [0,1]");
    Ok(price)
}
fn quantity(text: &str) -> anyhow::Result<i64> {
    let quantity = parse(text)?;
    ensure!(quantity >= 0, "negative Polymarket quantity");
    Ok(quantity)
}
fn instrument(asset_id: String) -> anyhow::Result<String> {
    ensure!(!asset_id.is_empty(), "empty Polymarket asset ID");
    Ok(asset_id)
}
fn levels(wire: Vec<OrderSummary>) -> anyhow::Result<Vec<Level>> {
    wire.into_iter()
        .map(|level| {
            Ok(Level {
                price: price(&level.price)?,
                quantity: quantity(&level.size)?,
            })
        })
        .collect()
}
fn event(
    value: Value,
    received_at: DateTime<Utc>,
    events: &mut Vec<BookEvent>,
) -> anyhow::Result<()> {
    match value
        .get("event_type")
        .and_then(Value::as_str)
        .context("missing raw event_type")?
    {
        "book" => {
            let book: WireBook =
                serde_json::from_value(value).context("invalid Polymarket book")?;
            events.push(BookEvent::Snapshot(Snapshot {
                symbol: instrument(book.asset_id)?,
                received_at,
                exchange_at: timestamp(&book.timestamp)?,
                clock: None,
                bids: levels(book.bids)?,
                asks: levels(book.asks)?,
            }));
        }
        "price_change" => {
            let message: WireChanges =
                serde_json::from_value(value).context("invalid Polymarket price changes")?;
            let exchange_at = timestamp(&message.timestamp)?;
            for change in message.price_changes {
                let side = match change.side.as_str() {
                    "BUY" => Side::Buy,
                    "SELL" => Side::Sell,
                    _ => anyhow::bail!("unknown Polymarket side"),
                };
                events.push(BookEvent::Update(Update {
                    symbol: instrument(change.asset_id)?,
                    received_at,
                    exchange_at,
                    clock: None,
                    side,
                    price: price(&change.price)?,
                    quantity: QuantityUpdate::Set(quantity(&change.size)?),
                }));
            }
        }
        // Trades, tick-size notices and lifecycle messages are not level changes.
        _ => {}
    }
    Ok(())
}

impl ExchangeAdapter for Polymarket {
    fn contract(&self) -> super::Contract {
        super::Contract {
            exchange: "polymarket",
            profile: "public-clob-v1",
            instrument_namespace: "clobTokenIds",
            membership: super::MembershipStrategy::FreshSession,
            ordering: super::OrderingStrategy::SnapshotOnly,
            sequence_detection: false,
        }
    }
    fn heartbeat(&self) -> Message {
        Message::Text("PING".into())
    }
    fn rejection(&self, _code: u64) -> Rejection {
        Rejection::Fatal
    }

    fn decode(&self, text: &str, received_at: DateTime<Utc>) -> anyhow::Result<Frame> {
        let mut frame = Frame {
            sequence: None,
            request_id: None,
            events: vec![],
            control: Control::Other,
        };
        if text.trim() == "PONG" {
            frame.control = Control::Pong;
            return Ok(frame);
        }
        let value: Value = serde_json::from_str(text).context("invalid Polymarket message")?;
        match value {
            Value::Array(batch) => {
                for value in batch {
                    event(value, received_at, &mut frame.events)?;
                }
            }
            value => event(value, received_at, &mut frame.events)?,
        }
        Ok(frame)
    }
    fn subscribe(&self, _request_id: u64, symbols: &[String]) -> anyhow::Result<Value> {
        Ok(
            json!({"type":"market","assets_ids":symbols,"initial_dump":true,"custom_feature_enabled":true}),
        )
    }
    fn update_subscription(
        &self,
        _request_id: u64,
        _sid: u64,
        symbols: &[String],
        add: bool,
    ) -> anyhow::Result<Value> {
        Ok(
            json!({"operation":if add {"subscribe"} else {"unsubscribe"},"assets_ids":symbols,"custom_feature_enabled":true}),
        )
    }
    fn get_snapshot(
        &self,
        _request_id: u64,
        _sid: u64,
        _symbols: &[String],
    ) -> anyhow::Result<Value> {
        anyhow::bail!(
            "Polymarket has no native get_snapshot command; unsubscribe/resubscribe for initialization"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::OrderBook;
    const BOOK: &str = include_str!("../../tests/fixtures/polymarket/book.json");
    const CHANGES: &str = include_str!("../../tests/fixtures/polymarket/price_change.json");
    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp_millis(1700000000100).unwrap()
    }

    #[test]
    fn factory_uses_the_public_v1_contract_and_never_installs_a_signer() {
        let exchange = configure(
            "http://127.0.0.1:1",
            10,
            vec!["340282366920938463463374607431768211456".into()],
        )
        .unwrap();
        let contract = exchange.protocol.contract();
        assert_eq!(contract.exchange, "polymarket");
        assert_eq!(contract.profile, "public-clob-v1");
        assert_eq!(contract.instrument_namespace, "clobTokenIds");
        contract.validate().unwrap();
        assert!(exchange.auth.is_none());
        assert!(exchange.catalog.is_some());
        assert!(!exchange.catalog.unwrap().supports_incremental());
    }

    #[test]
    fn factory_rejects_invalid_public_configuration_before_any_request() {
        for symbols in [vec!["condition-or-slug".into()], vec!["".into()]] {
            assert!(configure("http://127.0.0.1:1", 10, symbols).is_err());
        }
        assert!(configure("http://127.0.0.1:1", 0, vec![]).is_err());
        assert!(configure("not-a-base-url", 10, vec![]).is_err());
    }

    #[test]
    fn outcome_id_is_the_instrument_and_sides_do_not_use_kalshi_complements() {
        let frame = Polymarket.decode(BOOK, now()).unwrap();
        assert!(frame.sequence.is_none());
        assert!(frame.request_id.is_none());
        let BookEvent::Snapshot(snapshot) = &frame.events[0] else {
            panic!("snapshot")
        };
        assert_eq!(snapshot.symbol, "123");
        assert_eq!(snapshot.received_at, now());
        assert_eq!(snapshot.exchange_at.timestamp_millis(), 1700000000000);
        assert_eq!(
            snapshot.bids[0],
            Level {
                price: 400_000,
                quantity: 10_500_000
            }
        );
        assert_eq!(
            snapshot.asks[0],
            Level {
                price: 620_000,
                quantity: 8_000_000
            }
        );
        assert!(snapshot.clock.is_none());
    }

    #[test]
    fn changes_set_absolute_quantities_and_zero_removes_the_level() {
        let BookEvent::Snapshot(snapshot) =
            Polymarket.decode(BOOK, now()).unwrap().events.remove(0)
        else {
            panic!("snapshot")
        };
        let mut book = OrderBook::new(snapshot.symbol.clone());
        book.replace(snapshot).unwrap();
        let frame = Polymarket.decode(CHANGES, now()).unwrap();
        assert_eq!(frame.events.len(), 3);
        let BookEvent::Update(update) = &frame.events[0] else {
            panic!("update")
        };
        assert_eq!(update.quantity, QuantityUpdate::Set(3_000_000));
        assert_eq!(book.apply(update.clone()).unwrap().quantity, 3_000_000);
        assert_eq!(book.apply(update.clone()).unwrap().quantity, 3_000_000);
        let BookEvent::Update(delete) = &frame.events[1] else {
            panic!("update")
        };
        assert_eq!(book.apply(delete.clone()).unwrap().quantity, 0);
        assert!(book.snapshot(now()).unwrap().asks.is_empty());
        assert_eq!(frame.events[2].symbol(), "456");
    }

    #[test]
    fn batches_preserve_order_without_manufacturing_native_acks_or_sequences() {
        let frame = Polymarket
            .decode(&format!("[{BOOK},{CHANGES}]"), now())
            .unwrap();
        assert_eq!(frame.events.len(), 4);
        assert!(matches!(frame.events[0], BookEvent::Snapshot(_)));
        assert_eq!(
            frame
                .events
                .iter()
                .map(BookEvent::symbol)
                .collect::<Vec<_>>(),
            vec!["123", "123", "123", "456"]
        );
        assert!(matches!(frame.control, Control::Other));
        assert!(frame.sequence.is_none());
        assert!(matches!(
            Polymarket.decode("PONG", now()).unwrap().control,
            Control::Pong
        ));
    }

    #[test]
    fn commands_follow_the_raw_schema_and_snapshot_recovery_is_explicit() {
        let assets = vec!["123".into(), "456".into()];
        assert_eq!(
            Polymarket.subscribe(88, &assets).unwrap(),
            json!({"type":"market","assets_ids":assets,"initial_dump":true,"custom_feature_enabled":true})
        );
        assert_eq!(
            Polymarket
                .update_subscription(89, 99, &assets, false)
                .unwrap(),
            json!({"operation":"unsubscribe","assets_ids":assets,"custom_feature_enabled":true})
        );
        assert_eq!(
            Polymarket
                .update_subscription(90, 99, &assets, true)
                .unwrap()["operation"],
            "subscribe"
        );
        assert!(Polymarket.get_snapshot(91, 99, &assets).is_err());
        assert!(
            Polymarket
                .subscribe(88, &assets)
                .unwrap()
                .get("id")
                .is_none()
        );
    }

    #[test]
    fn invalid_known_events_fail_the_whole_message_without_partial_output() {
        for (field, value) in [
            ("side", json!("NO")),
            ("size", json!("-1")),
            ("size", json!(3)),
            ("price", json!("1.000001")),
            ("asset_id", json!("")),
        ] {
            let mut changes: Value = serde_json::from_str(CHANGES).unwrap();
            changes["price_changes"][2][field] = value;
            assert!(
                Polymarket
                    .decode(&format!("[{BOOK},{changes}]"), now())
                    .is_err(),
                "{field}"
            );
        }
        for text in [
            BOOK.replace("1700000000000", "bad"),
            BOOK.replace("\"asks\"", "\"offers\""),
            BOOK.replace("\"asset_id\"", "\"assetId\""),
        ] {
            assert!(Polymarket.decode(&text, now()).is_err());
        }
        assert!(
            Polymarket
                .decode(r#"{"event_type":"last_trade_price"}"#, now())
                .unwrap()
                .events
                .is_empty()
        );
    }
}
