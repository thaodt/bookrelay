//! Raw public Gamma keyset discovery for the explicit CLOB/CTF v1 profile.
//!
//! `/markets/keyset` is not an atomic universe snapshot. Keep filters on every
//! page and publish nothing until the scan succeeds. An uncertain record makes
//! the result non-authoritative for absence retirement, without hiding valid
//! additions or explicit ineligible observations from the other records.
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    time::{Duration, Instant},
};

use anyhow::{Context, ensure};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::catalog::{Catalog, Instrument, Inventory};

const PAGE_BYTES: usize = 16 << 20;
const PAGE_SIZE: usize = 100;
const MAX_PAGES: usize = 10_000;
const MAX_MARKET_RECORDS: usize = 100_000;
const MAX_ASSET_RECORDS: usize = 100_000;
const MAX_METADATA_BYTES: usize = 16 << 20;
const METADATA_ENVELOPE_BYTES: usize = 64 << 10;
const MAX_FIELD_BYTES: usize = 256;
const MAX_ARRAY_BYTES: usize = 4096;
const MAX_CURSOR_BYTES: usize = 4096;
const MAX_DIAGNOSTIC_EXAMPLES: usize = 20;
const REQUEST_INTERVAL: Duration = Duration::from_millis(100);
const MAX_RETRIES: usize = 5;
const MAX_RETRY_AFTER: Duration = Duration::from_secs(300);

#[derive(Clone)]
pub struct PolymarketCatalog {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    max_symbols: usize,
    symbols: Vec<String>,
}

#[derive(Deserialize)]
struct Page {
    markets: Vec<Value>,
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct Asset {
    asset_id: String,
    market_id: String,
    condition_id: String,
    outcome: String,
    outcome_index: usize,
    version: &'static str,
    eligible: bool,
}

impl Asset {
    fn same_identity(&self, other: &Self) -> bool {
        self.asset_id == other.asset_id
            && self.market_id == other.market_id
            && self.condition_id == other.condition_id
            && self.outcome == other.outcome
            && self.outcome_index == other.outcome_index
            && self.version == other.version
    }
}

#[derive(Default, Serialize)]
struct Diagnostics {
    market_records: usize,
    out_of_profile_markets: usize,
    both_id_fields_markets: usize,
    ineligible_markets: usize,
    ineligible_without_ids: usize,
    quarantined_markets: usize,
    duplicate_assets: usize,
    changed_eligibility_observations: usize,
    rate_limit_retries: usize,
    quarantine_examples: Vec<Value>,
}

impl Diagnostics {
    fn quarantine(&mut self, market: &Value, reason: &str) {
        self.quarantined_markets += 1;
        if self.quarantine_examples.len() < MAX_DIAGNOSTIC_EXAMPLES {
            // Strings in examples are bounded separately from raw response data.
            let id = market.get("id").and_then(Value::as_str).unwrap_or("");
            self.quarantine_examples.push(json!({
                "market_id": id.chars().take(MAX_FIELD_BYTES).collect::<String>(),
                "reason": reason.chars().take(MAX_FIELD_BYTES).collect::<String>(),
            }));
        }
    }
}

impl PolymarketCatalog {
    pub fn new(base: &str, max_symbols: usize, symbols: Vec<String>) -> anyhow::Result<Self> {
        ensure!(
            max_symbols > 0,
            "public catalog symbol budget must be positive"
        );
        ensure!(
            symbols.len() <= MAX_ASSET_RECORDS,
            "fixed public inventory exceeds catalog record budget"
        );
        let mut unique = BTreeSet::new();
        for symbol in symbols {
            validate_asset_id(&symbol).context("invalid explicit v1 CLOB asset ID")?;
            unique.insert(symbol);
        }
        ensure!(
            unique.len() <= max_symbols,
            "fixed public inventory exceeds symbol budget"
        );
        let mut endpoint = reqwest::Url::parse(base).context("invalid public catalog base URL")?;
        ensure!(
            matches!(endpoint.scheme(), "http" | "https")
                && endpoint.host_str().is_some()
                && endpoint.query().is_none()
                && endpoint.fragment().is_none()
                && endpoint.username().is_empty()
                && endpoint.password().is_none(),
            "public catalog base must be an HTTP(S) URL without query, fragment or credentials"
        );
        endpoint.set_path(&format!(
            "{}/markets/keyset",
            endpoint.path().trim_end_matches('/')
        ));
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .gzip(true)
                .user_agent("voluntas-challenge/0.2")
                .build()?,
            endpoint,
            max_symbols,
            symbols: unique.into_iter().collect(),
        })
    }

    async fn page(
        &self,
        query: &[(String, String)],
        next_request: &mut tokio::time::Instant,
        diagnostics: &mut Diagnostics,
    ) -> anyhow::Result<Page> {
        for attempt in 0..=MAX_RETRIES {
            // No spawned retry task: dropping the scan also cancels sends/sleeps.
            tokio::time::sleep_until(*next_request).await;
            *next_request = tokio::time::Instant::now() + REQUEST_INTERVAL;
            let response = self
                .client
                .get(self.endpoint.clone())
                .query(query)
                .send()
                .await?;
            if response.status().as_u16() == 429 {
                ensure!(
                    attempt < MAX_RETRIES,
                    "public catalog rate limit retry budget exhausted"
                );
                diagnostics.rate_limit_retries += 1;
                let delay = retry_delay(response.headers(), attempt)?;
                *next_request = (*next_request).max(tokio::time::Instant::now() + delay);
                continue;
            }
            if response.status().is_server_error() && attempt < MAX_RETRIES {
                *next_request = (*next_request)
                    .max(tokio::time::Instant::now() + Duration::from_secs(1 << attempt));
                continue;
            }
            let mut response = response.error_for_status()?;
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                ensure!(
                    chunk.len() <= PAGE_BYTES.saturating_sub(bytes.len()),
                    "decoded public metadata page exceeds 16 MiB"
                );
                bytes.extend_from_slice(&chunk);
            }
            return serde_json::from_slice(&bytes).context("invalid public markets keyset page");
        }
        unreachable!("bounded retry loop always returns")
    }

    pub async fn scan(&self, since: Option<i64>) -> anyhow::Result<Inventory> {
        ensure!(
            since.is_none(),
            "public v1 catalog does not support incremental timestamps"
        );
        let watermark = chrono::Utc::now().timestamp();
        let started = Instant::now();
        let mut assets = BTreeMap::<String, (Asset, usize)>::new();
        let selected: HashSet<_> = self.symbols.iter().map(String::as_str).collect();
        let mut diagnostics = Diagnostics::default();
        let mut metadata_bytes = METADATA_ENVELOPE_BYTES;
        let mut pages = 0;
        let mut next_request = tokio::time::Instant::now();
        // Each identifier filter accepts at most 100 values. An empty scope is
        // the open v1 universe; closed=any is only documented with identifiers.
        let scopes: Vec<&[String]> = if self.symbols.is_empty() {
            vec![&[]]
        } else {
            self.symbols.chunks(100).collect()
        };
        for scope in scopes {
            let mut query = vec![
                ("version".to_owned(), "v1".to_owned()),
                (
                    "closed".to_owned(),
                    if scope.is_empty() { "false" } else { "any" }.to_owned(),
                ),
                ("limit".to_owned(), PAGE_SIZE.to_string()),
            ];
            query.extend(
                scope
                    .iter()
                    .map(|id| ("clob_token_ids".to_owned(), id.clone())),
            );
            let mut cursors = HashSet::new();
            loop {
                ensure!(pages < MAX_PAGES, "public catalog page budget exceeded");
                let page = self
                    .page(&query, &mut next_request, &mut diagnostics)
                    .await
                    .with_context(|| format!("public catalog page {} failed", pages + 1))?;
                pages += 1;
                diagnostics.market_records = diagnostics
                    .market_records
                    .checked_add(page.markets.len())
                    .context("public catalog record count overflow")?;
                ensure!(
                    diagnostics.market_records <= MAX_MARKET_RECORDS,
                    "public catalog market record budget exceeded"
                );
                for market in page.markets {
                    let parsed = classify_market(&market, &mut diagnostics);
                    let record_assets = match parsed {
                        Ok(assets) => assets,
                        Err(error) => {
                            diagnostics.quarantine(&market, &format!("{error:#}"));
                            continue;
                        }
                    };
                    for asset in record_assets {
                        if !selected.is_empty() && !selected.contains(asset.asset_id.as_str()) {
                            continue;
                        }
                        if let Some((previous, size)) = assets.get_mut(&asset.asset_id) {
                            ensure!(
                                previous.same_identity(&asset),
                                "conflicting stable identity for public asset {}",
                                asset.asset_id
                            );
                            diagnostics.duplicate_assets += 1;
                            if previous.eligible != asset.eligible {
                                diagnostics.changed_eligibility_observations += 1;
                            }
                            // Keyset reads are not atomic. Keep the later
                            // explicit status; volume/price fields never enter
                            // the stable identity comparison.
                            let new_size = serde_json::to_vec(&asset)?.len() + 1;
                            metadata_bytes = metadata_bytes - *size + new_size;
                            *previous = asset;
                            *size = new_size;
                        } else {
                            ensure!(
                                assets.len() < self.max_symbols && assets.len() < MAX_ASSET_RECORDS,
                                "public catalog asset inventory exceeds symbol/record budget"
                            );
                            let size = serde_json::to_vec(&asset)?.len() + 1;
                            metadata_bytes = metadata_bytes
                                .checked_add(size)
                                .context("public catalog metadata size overflow")?;
                            assets.insert(asset.asset_id.clone(), (asset, size));
                        }
                        ensure!(
                            metadata_bytes <= MAX_METADATA_BYTES,
                            "public catalog identity metadata exceeds 16 MiB"
                        );
                    }
                }
                let Some(cursor) = page.next_cursor.filter(|cursor| !cursor.is_empty()) else {
                    break;
                };
                ensure!(
                    cursor.len() <= MAX_CURSOR_BYTES,
                    "public catalog cursor exceeds byte budget"
                );
                ensure!(
                    cursors.insert(cursor.clone()),
                    "public catalog pagination cursor cycle"
                );
                // Retain all version/identifier filters when advancing.
                if let Some((_, value)) = query.iter_mut().find(|(name, _)| name == "after_cursor")
                {
                    *value = cursor;
                } else {
                    query.push(("after_cursor".to_owned(), cursor));
                }
            }
        }
        if !selected.is_empty() {
            for symbol in &self.symbols {
                let (asset, _) = assets.get(symbol).with_context(|| {
                    format!("explicit v1 CLOB asset ID {symbol} is missing, incompatible or malformed in Gamma lookup")
                })?;
                ensure!(
                    asset.eligible,
                    "explicit v1 CLOB asset ID {symbol} is ineligible for live orderbook collection"
                );
            }
        }
        let complete = diagnostics.quarantined_markets == 0;
        let instruments = assets
            .values()
            .map(|(asset, _)| Instrument {
                symbol: asset.asset_id.clone(),
                eligible: asset.eligible,
            })
            .collect();
        let metadata = json!({
            "profile": "clob-ctf-v1",
            "instrument_namespace": "clobTokenIds",
            "complete": complete,
            "authoritative_for_retirement": complete,
            "atomic_snapshot": false,
            "scope": if selected.is_empty() { "open-v1-universe" } else { "fixed-clob-token-ids" },
            "limits": {
                "decoded_page_bytes": PAGE_BYTES,
                "pages": MAX_PAGES,
                "market_records": MAX_MARKET_RECORDS,
                "asset_records": MAX_ASSET_RECORDS,
                "metadata_bytes": MAX_METADATA_BYTES,
                "field_bytes": MAX_FIELD_BYTES,
                "diagnostic_examples": MAX_DIAGNOSTIC_EXAMPLES,
            },
            "diagnostics": diagnostics,
            "assets": assets.into_values().map(|(asset, _)| asset).collect::<Vec<_>>(),
        });
        Ok(Inventory {
            instruments,
            watermark,
            pages,
            elapsed: started.elapsed(),
            authoritative_for_retirement: complete,
            complete,
            metadata: Some(metadata),
        })
    }
}

impl Catalog for PolymarketCatalog {
    fn validates_fixed_symbols(&self) -> bool {
        true
    }

    fn scan(&self, since: Option<i64>) -> BoxFuture<'_, anyhow::Result<Inventory>> {
        Box::pin(PolymarketCatalog::scan(self, since))
    }
}

fn retry_delay(headers: &reqwest::header::HeaderMap, attempt: usize) -> anyhow::Result<Duration> {
    let exponential = Duration::from_secs(1 << attempt);
    let Some(value) = headers.get(reqwest::header::RETRY_AFTER) else {
        return Ok(exponential);
    };
    let value = value.to_str().context("invalid Retry-After header")?;
    let delay = if let Ok(seconds) = value.parse::<u64>() {
        Duration::from_secs(seconds)
    } else {
        let date =
            chrono::DateTime::parse_from_rfc2822(value).context("invalid Retry-After value")?;
        (date.with_timezone(&chrono::Utc) - chrono::Utc::now())
            .to_std()
            .unwrap_or_default()
    };
    // Do not retry earlier than the server requested. Excessive backoff is a
    // bounded scan failure; a caller may try a fresh scan later.
    ensure!(
        delay <= MAX_RETRY_AFTER,
        "Retry-After exceeds public catalog wait budget"
    );
    Ok(delay.max(exponential))
}

fn validate_asset_id(value: &str) -> anyhow::Result<()> {
    // The v1 namespace is a decimal string in primary API examples. This is
    // lexical validation only: no integer conversion or normalization.
    ensure!(
        !value.is_empty()
            && value.len() <= MAX_FIELD_BYTES
            && value.bytes().all(|c| c.is_ascii_digit()),
        "CLOB asset ID must be a nonempty decimal string of at most {MAX_FIELD_BYTES} bytes"
    );
    Ok(())
}

fn bounded_string<'a>(market: &'a Value, field: &str) -> anyhow::Result<&'a str> {
    let value = market
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("missing or malformed {field}"))?;
    ensure!(
        !value.is_empty() && value.len() <= MAX_FIELD_BYTES,
        "invalid length for {field}"
    );
    Ok(value)
}

fn string_array(market: &Value, field: &str) -> anyhow::Result<Vec<String>> {
    let value = market
        .get(field)
        .with_context(|| format!("missing {field}"))?;
    let decoded;
    let array = if let Some(encoded) = value.as_str() {
        ensure!(
            encoded.len() <= MAX_ARRAY_BYTES,
            "{field} encoded array exceeds byte budget"
        );
        decoded = serde_json::from_str::<Value>(encoded)
            .with_context(|| format!("invalid {field} JSON array"))?;
        decoded.as_array()
    } else {
        value.as_array()
    }
    .with_context(|| format!("{field} must be an array or JSON-encoded array"))?;
    ensure!(
        array.len() == 2,
        "{field} must contain exactly two binary outcomes"
    );
    array
        .iter()
        .map(|value| {
            let value = value
                .as_str()
                .with_context(|| format!("non-string entry in {field}"))?;
            ensure!(
                !value.is_empty() && value.len() <= MAX_FIELD_BYTES,
                "invalid entry length in {field}"
            );
            Ok(value.to_owned())
        })
        .collect()
}

fn classify_market(market: &Value, diagnostics: &mut Diagnostics) -> anyhow::Result<Vec<Asset>> {
    ensure!(market.is_object(), "market record is not an object");
    let version = market
        .get("version")
        .and_then(Value::as_str)
        .context("missing or malformed market version")?;
    ensure!(
        !version.is_empty() && version.len() <= MAX_FIELD_BYTES,
        "missing or malformed market version"
    );
    if version != "v1" {
        diagnostics.out_of_profile_markets += 1;
        return Ok(vec![]);
    }
    if market
        .get("clobTokenIds")
        .is_some_and(|value| !value.is_null())
        && market
            .get("positionIds")
            .is_some_and(|value| !value.is_null())
    {
        diagnostics.both_id_fields_markets += 1;
    }
    let state = [
        ("active", true),
        ("closed", false),
        ("enableOrderBook", true),
        ("acceptingOrders", true),
    ];
    let explicitly_ineligible = state.iter().any(|(field, expected)| {
        market
            .get(*field)
            .and_then(Value::as_bool)
            .is_some_and(|value| value != *expected)
    });
    let eligible = !explicitly_ineligible;
    if eligible {
        for (field, expected) in state {
            ensure!(
                market.get(field).and_then(Value::as_bool) == Some(expected),
                "missing or malformed trading state {field}"
            );
        }
    } else {
        diagnostics.ineligible_markets += 1;
    }
    let ids = string_array(market, "clobTokenIds").and_then(|ids| {
        for id in &ids {
            validate_asset_id(id)?;
        }
        Ok(ids)
    });
    let ids = match ids {
        Ok(ids) => ids,
        Err(_) if !eligible => {
            // Pre-open books may legitimately have no IDs. Their absence is
            // not a broken live instrument and cannot produce a fake binding.
            diagnostics.ineligible_without_ids += 1;
            return Ok(vec![]);
        }
        Err(error) => return Err(error),
    };
    let outcomes = string_array(market, "outcomes")?;
    let market_id = bounded_string(market, "id")?;
    let condition_id = bounded_string(market, "conditionId")?;
    Ok(ids
        .into_iter()
        .zip(outcomes)
        .enumerate()
        .map(|(index, (asset_id, outcome))| Asset {
            asset_id,
            market_id: market_id.to_owned(),
            condition_id: condition_id.to_owned(),
            outcome,
            outcome_index: index,
            version: "v1",
            eligible,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn market(id: &str, yes: &str, no: &str) -> Value {
        json!({
            "id": id,
            "conditionId": format!("condition-{id}"),
            "version": "v1",
            "active": true,
            "closed": false,
            "enableOrderBook": true,
            "acceptingOrders": true,
            "outcomes": "[\"Yes\",\"No\"]",
            "clobTokenIds": serde_json::to_string(&[yes, no]).unwrap(),
        })
    }

    fn page(markets: Vec<Value>, cursor: Option<&str>) -> String {
        json!({"markets": markets, "next_cursor": cursor}).to_string()
    }

    struct Reply {
        status: u16,
        body: String,
        retry_after: Option<&'static str>,
    }

    impl Reply {
        fn ok(body: String) -> Self {
            Self {
                status: 200,
                body,
                retry_after: None,
            }
        }
    }

    async fn fixture(replies: Vec<Reply>) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut requests = vec![];
            for reply in replies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = vec![];
                let mut byte = [0; 1];
                while !request.ends_with(b"\r\n\r\n") {
                    assert!(request.len() < 64 << 10);
                    socket.read_exact(&mut byte).await.unwrap();
                    request.push(byte[0]);
                }
                requests.push(String::from_utf8(request).unwrap());
                let retry = reply
                    .retry_after
                    .map(|value| format!("Retry-After: {value}\r\n"))
                    .unwrap_or_default();
                let response = format!(
                    "HTTP/1.1 {} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{retry}\r\n{}",
                    reply.status,
                    reply.body.len(),
                    reply.body,
                );
                // Cancellation/oversized-body tests can close the socket early.
                let _ = socket.write_all(response.as_bytes()).await;
            }
            requests
        });
        (format!("http://{address}"), task)
    }

    fn query(request: &str) -> Vec<(String, String)> {
        let target = request
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap();
        let url = reqwest::Url::parse(&format!("http://fixture{target}")).unwrap();
        assert_eq!(url.path(), "/markets/keyset");
        url.query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect()
    }

    fn diagnostics(inventory: &Inventory) -> &Value {
        &inventory.metadata.as_ref().unwrap()["diagnostics"]
    }

    #[tokio::test]
    async fn primary_v1_sample_selects_clob_ids_even_when_position_ids_are_present() {
        // Trimmed from the saved primary keyset-v1-sample.json. Keep the fixture
        // inline so tests do not depend on ignored raw experiment artifacts.
        let sample = json!({
            "markets": [{
                "id": "559651",
                "conditionId": "0xa467b14d51f01b957109d9cbb1d6c124fab2a089d52ed8f471d23c2812e743b7",
                "version": "v1",
                "active": true,
                "closed": false,
                "enableOrderBook": true,
                "acceptingOrders": true,
                "outcomes": "[\"Yes\", \"No\"]",
                "clobTokenIds": "[\"32338220190071351435772801779725302244575775216413325951443816017994629993401\", \"25659310674993675562345759665114759892400026242514633218387667107987341231962\"]",
                "positionIds": [
                    "895257453734540292493143124621755605615887197499956109032745153687078305792",
                    "895257453734540292493143124621755605615887197499956109032745153687078305793",
                ],
            }],
            "next_cursor": null,
        });
        let (base, server) = fixture(vec![Reply::ok(sample.to_string())]).await;
        let catalog = PolymarketCatalog::new(&base, 10, vec![]).unwrap();
        assert!(!catalog.supports_incremental());
        assert!(catalog.validates_fixed_symbols());
        let result = catalog.scan(None).await.unwrap();
        assert!(result.complete && result.authoritative_for_retirement);
        assert_eq!(result.instruments.len(), 2);
        assert!(result.instruments.iter().all(|asset| asset.eligible));
        assert!(result.instruments.iter().any(|asset| asset.symbol
            == "32338220190071351435772801779725302244575775216413325951443816017994629993401"));
        assert!(!result.instruments.iter().any(|asset| asset.symbol
            == "895257453734540292493143124621755605615887197499956109032745153687078305792"));
        assert_eq!(diagnostics(&result)["both_id_fields_markets"], 1);
        let metadata = result.metadata.unwrap();
        let yes = metadata["assets"].as_array().unwrap().iter()
            .find(|asset| asset["asset_id"] == "32338220190071351435772801779725302244575775216413325951443816017994629993401").unwrap();
        assert_eq!(yes["market_id"], "559651");
        assert_eq!(yes["outcome"], "Yes");
        assert_eq!(yes["outcome_index"], 0);
        assert_eq!(yes["version"], "v1");
        let requests = server.await.unwrap();
        let params = query(&requests[0]);
        assert!(params.contains(&("version".into(), "v1".into())));
        assert!(params.contains(&("closed".into(), "false".into())));
        assert!(
            requests[0]
                .to_ascii_lowercase()
                .contains("accept-encoding: gzip")
        );
    }

    #[tokio::test]
    async fn malformed_trading_records_are_visible_without_hiding_valid_observations() {
        let mut malformed = market("bad", "30", "31");
        malformed["clobTokenIds"] = json!("[\"30\",null]");
        malformed["positionIds"] = json!(["130", "131"]);
        let mut missing_state = market("state", "40", "41");
        missing_state
            .as_object_mut()
            .unwrap()
            .remove("acceptingOrders");
        let mut closed = market("closed", "20", "21");
        closed["closed"] = json!(true);
        let mut future = market("future", "50", "51");
        future["acceptingOrders"] = json!(false);
        future["clobTokenIds"] = Value::Null;
        let mut v2 = market("v2", "60", "61");
        v2["version"] = json!("v2");
        v2["positionIds"] = json!(["70", "71"]);
        let (base, server) = fixture(vec![Reply::ok(page(
            vec![
                market("good", "10", "11"),
                malformed,
                missing_state,
                closed,
                future,
                v2,
            ],
            None,
        ))])
        .await;
        let result = PolymarketCatalog::new(&base, 10, vec![])
            .unwrap()
            .scan(None)
            .await
            .unwrap();
        assert!(!result.complete && !result.authoritative_for_retirement);
        assert_eq!(
            result.instruments,
            vec![
                Instrument {
                    symbol: "10".into(),
                    eligible: true
                },
                Instrument {
                    symbol: "11".into(),
                    eligible: true
                },
                Instrument {
                    symbol: "20".into(),
                    eligible: false
                },
                Instrument {
                    symbol: "21".into(),
                    eligible: false
                },
            ]
        );
        assert_eq!(diagnostics(&result)["quarantined_markets"], 2);
        assert_eq!(diagnostics(&result)["out_of_profile_markets"], 1);
        assert_eq!(diagnostics(&result)["ineligible_markets"], 2);
        assert_eq!(diagnostics(&result)["ineligible_without_ids"], 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn an_out_of_profile_record_alone_is_not_incomplete() {
        let (base, server) =
            fixture(vec![Reply::ok(page(vec![json!({"version": "v2"})], None))]).await;
        let result = PolymarketCatalog::new(&base, 10, vec![])
            .unwrap()
            .scan(None)
            .await
            .unwrap();
        assert!(result.complete && result.authoritative_for_retirement);
        assert!(result.instruments.is_empty());
        assert_eq!(diagnostics(&result)["out_of_profile_markets"], 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn pages_retain_filters_and_opaque_cursor_and_dedupe_only_stable_identity() {
        let first = market(
            "m",
            "000000000000000000000000000000000001",
            "999999999999999999999999999999999999999999999999999999999999999999999999999999",
        );
        let mut changed_price = first.clone();
        changed_price["volume"] = json!(123.5);
        changed_price["outcomePrices"] = json!("[\"0.25\",\"0.75\"]");
        let (base, server) = fixture(vec![
            Reply::ok(page(vec![first], Some("next cursor&+/?="))),
            Reply::ok(page(vec![changed_price], None)),
        ])
        .await;
        let result = PolymarketCatalog::new(&base, 10, vec![])
            .unwrap()
            .scan(None)
            .await
            .unwrap();
        assert_eq!(result.pages, 2);
        assert_eq!(result.instruments.len(), 2);
        assert_eq!(
            result.instruments[0].symbol,
            "000000000000000000000000000000000001"
        );
        assert_eq!(
            result.instruments[1].symbol,
            "999999999999999999999999999999999999999999999999999999999999999999999999999999"
        );
        assert_eq!(diagnostics(&result)["duplicate_assets"], 2);
        assert!(result.complete);
        let requests = server.await.unwrap();
        let first_query = query(&requests[0]);
        let second_query = query(&requests[1]);
        for pair in first_query {
            assert!(second_query.contains(&pair));
        }
        assert!(second_query.contains(&("after_cursor".into(), "next cursor&+/?=".into())));
    }

    #[tokio::test]
    async fn failed_page_cursor_loop_bad_root_and_conflicts_never_return_partial_inventory() {
        let good = market("m", "10", "11");
        let mut conflicting = good.clone();
        conflicting["conditionId"] = json!("different-condition");
        for second in [
            Reply {
                status: 400,
                body: "{}".into(),
                retry_after: None,
            },
            Reply::ok(page(vec![], Some("same"))),
            Reply::ok("[]".into()),
            Reply::ok(page(vec![conflicting], None)),
            Reply::ok(json!({"markets": [], "next_cursor": 7}).to_string()),
        ] {
            let (base, server) = fixture(vec![
                Reply::ok(page(vec![good.clone()], Some("same"))),
                second,
            ])
            .await;
            assert!(
                PolymarketCatalog::new(&base, 10, vec![])
                    .unwrap()
                    .scan(None)
                    .await
                    .is_err()
            );
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn duplicate_id_at_different_outcome_indices_is_a_conflicting_identity() {
        let (base, server) =
            fixture(vec![Reply::ok(page(vec![market("m", "10", "10")], None))]).await;
        let error = PolymarketCatalog::new(&base, 10, vec![])
            .unwrap()
            .scan(None)
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("conflicting stable identity"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn every_eligibility_flag_prevents_admission_independently() {
        for (field, value) in [
            ("active", false),
            ("closed", true),
            ("enableOrderBook", false),
            ("acceptingOrders", false),
        ] {
            let mut record = market("m", "10", "11");
            record[field] = json!(value);
            let (base, server) = fixture(vec![Reply::ok(page(vec![record], None))]).await;
            let result = PolymarketCatalog::new(&base, 10, vec![])
                .unwrap()
                .scan(None)
                .await
                .unwrap();
            assert!(result.complete && result.authoritative_for_retirement);
            assert_eq!(result.instruments.len(), 2);
            assert!(
                result.instruments.iter().all(|asset| !asset.eligible),
                "{field}"
            );
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn fixed_lookup_returns_only_requested_ids_and_retains_identifiers_on_pages() {
        let wanted = "00012345678901234567890123456789012345678901234567890";
        let good = market("m", wanted, "11");
        let (base, server) = fixture(vec![
            Reply::ok(page(vec![good.clone()], Some("fixed&cursor"))),
            Reply::ok(page(vec![good], None)),
        ])
        .await;
        let result = PolymarketCatalog::new(&base, 1, vec![wanted.into()])
            .unwrap()
            .scan(None)
            .await
            .unwrap();
        assert_eq!(
            result.instruments,
            vec![Instrument {
                symbol: wanted.into(),
                eligible: true
            }]
        );
        let requests = server.await.unwrap();
        for request in &requests {
            let params = query(request);
            assert!(params.contains(&("clob_token_ids".into(), wanted.into())));
            assert!(params.contains(&("version".into(), "v1".into())));
            assert!(params.contains(&("closed".into(), "any".into())));
        }
        assert!(query(&requests[1]).contains(&("after_cursor".into(), "fixed&cursor".into())));
    }

    #[tokio::test]
    async fn fixed_ids_missing_ineligible_incompatible_or_malformed_fail_clearly() {
        let mut closed = market("m", "10", "11");
        closed["closed"] = json!(true);
        let mut v2 = market("m", "10", "11");
        v2["version"] = json!("v2");
        let mut malformed = market("m", "10", "11");
        malformed["clobTokenIds"] = json!([10, "11"]);
        for records in [vec![], vec![closed], vec![v2], vec![malformed]] {
            let (base, server) = fixture(vec![Reply::ok(page(records, None))]).await;
            let error = PolymarketCatalog::new(&base, 10, vec!["10".into()])
                .unwrap()
                .scan(None)
                .await
                .err()
                .unwrap();
            assert!(error.to_string().contains("explicit v1 CLOB asset ID 10"));
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn fixed_lookup_batches_at_most_one_hundred_identifiers() {
        let selected: Vec<_> = (1..=101).map(|id| format!("{id:03}")).collect();
        let records: Vec<_> = selected
            .iter()
            .map(|id| market(id, id, &format!("9{id}")))
            .collect();
        let (base, server) = fixture(vec![
            Reply::ok(page(records[..100].to_vec(), None)),
            Reply::ok(page(records[100..].to_vec(), None)),
        ])
        .await;
        let result = PolymarketCatalog::new(&base, 101, selected.clone())
            .unwrap()
            .scan(None)
            .await
            .unwrap();
        assert_eq!(result.instruments.len(), 101);
        let requests = server.await.unwrap();
        let first: Vec<_> = query(&requests[0])
            .into_iter()
            .filter(|(key, _)| key == "clob_token_ids")
            .map(|(_, value)| value)
            .collect();
        let second: Vec<_> = query(&requests[1])
            .into_iter()
            .filter(|(key, _)| key == "clob_token_ids")
            .map(|(_, value)| value)
            .collect();
        assert_eq!(first, selected[..100]);
        assert_eq!(second, selected[100..]);
    }

    #[tokio::test]
    async fn page_and_inventory_limits_fail_instead_of_truncating() {
        let (base, server) =
            fixture(vec![Reply::ok(page(vec![market("m", "10", "11")], None))]).await;
        let error = PolymarketCatalog::new(&base, 1, vec![])
            .unwrap()
            .scan(None)
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("budget"));
        server.await.unwrap();
        let (base, server) = fixture(vec![Reply::ok(" ".repeat(PAGE_BYTES + 1))]).await;
        let error = PolymarketCatalog::new(&base, 10, vec![])
            .unwrap()
            .scan(None)
            .await
            .err()
            .unwrap();
        assert!(format!("{error:#}").contains("exceeds 16 MiB"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rate_limit_retries_honor_retry_after_without_changing_query() {
        let (base, server) = fixture(vec![
            Reply {
                status: 429,
                body: "{}".into(),
                retry_after: Some("1"),
            },
            Reply::ok(page(vec![market("m", "10", "11")], None)),
        ])
        .await;
        let started = Instant::now();
        let result = PolymarketCatalog::new(&base, 10, vec![])
            .unwrap()
            .scan(None)
            .await
            .unwrap();
        assert!(started.elapsed() >= Duration::from_secs(1));
        assert_eq!(diagnostics(&result)["rate_limit_retries"], 1);
        let requests = server.await.unwrap();
        assert_eq!(query(&requests[0]), query(&requests[1]));
    }

    #[tokio::test]
    async fn dropping_scan_cancels_rate_limit_wait() {
        let (base, server) = fixture(vec![Reply {
            status: 429,
            body: "{}".into(),
            retry_after: Some("120"),
        }])
        .await;
        let catalog = PolymarketCatalog::new(&base, 10, vec![]).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(200), catalog.scan(None))
                .await
                .is_err()
        );
        assert_eq!(server.await.unwrap().len(), 1);
    }

    #[test]
    fn explicit_ids_are_lexically_validated_without_numeric_conversion() {
        for symbol in [
            "",
            "1.0",
            "1e12",
            " 1",
            "1 ",
            "-1",
            "abc",
            &"1".repeat(MAX_FIELD_BYTES + 1),
        ] {
            assert!(
                PolymarketCatalog::new("http://localhost", 10, vec![symbol.into()]).is_err(),
                "{symbol}"
            );
        }
        let symbol = "0".repeat(MAX_FIELD_BYTES);
        let catalog =
            PolymarketCatalog::new("http://localhost", 1, vec![symbol.clone(), symbol.clone()])
                .unwrap();
        assert_eq!(catalog.symbols, [symbol]);
        assert!(PolymarketCatalog::new("http://localhost", 0, vec![]).is_err());
        assert!(PolymarketCatalog::new("http://localhost?query=x", 1, vec![]).is_err());
    }

    #[test]
    fn retry_after_parses_seconds_and_http_date_and_rejects_unbounded_wait() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "2".parse().unwrap());
        assert_eq!(retry_delay(&headers, 0).unwrap(), Duration::from_secs(2));
        let date = (chrono::Utc::now() + chrono::Duration::seconds(4)).to_rfc2822();
        headers.insert(reqwest::header::RETRY_AFTER, date.parse().unwrap());
        assert!(retry_delay(&headers, 0).unwrap() > Duration::from_secs(2));
        headers.insert(reqwest::header::RETRY_AFTER, "301".parse().unwrap());
        assert!(retry_delay(&headers, 0).is_err());
        headers.insert(reqwest::header::RETRY_AFTER, "invalid".parse().unwrap());
        assert!(retry_delay(&headers, 0).is_err());
    }

    #[test]
    fn malformed_record_examples_are_bounded() {
        let mut diagnostics = Diagnostics::default();
        let malformed = json!({"id": "m".repeat(1000)});
        for _ in 0..MAX_DIAGNOSTIC_EXAMPLES + 5 {
            diagnostics.quarantine(&malformed, &"r".repeat(1000));
        }
        assert_eq!(diagnostics.quarantined_markets, MAX_DIAGNOSTIC_EXAMPLES + 5);
        assert_eq!(
            diagnostics.quarantine_examples.len(),
            MAX_DIAGNOSTIC_EXAMPLES
        );
        assert_eq!(
            diagnostics.quarantine_examples[0]["market_id"]
                .as_str()
                .unwrap()
                .len(),
            MAX_FIELD_BYTES
        );
        assert_eq!(
            diagnostics.quarantine_examples[0]["reason"]
                .as_str()
                .unwrap()
                .len(),
            MAX_FIELD_BYTES
        );
    }
}
