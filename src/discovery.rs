use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::{Context, ensure};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Market {
    pub ticker: String,
    pub status: String,
    pub mve_collection_ticker: Option<String>,
    #[serde(default)]
    pub mve_selected_legs: Option<Vec<serde_json::Value>>,
}

impl Market {
    pub fn regular(&self) -> bool {
        self.mve_collection_ticker
            .as_ref()
            .is_none_or(String::is_empty)
            && self.mve_selected_legs.as_ref().is_none_or(Vec::is_empty)
    }
    pub fn trading(&self) -> bool {
        matches!(self.status.as_str(), "open" | "active")
    }
}

#[derive(Deserialize)]
struct Page {
    markets: Vec<Market>,
    #[serde(default)]
    cursor: String,
}

#[derive(Clone)]
pub struct Discovery {
    client: reqwest::Client,
    base: String,
    max_symbols: usize,
}

pub use crate::catalog::Inventory;
use crate::catalog::{Catalog, Instrument};

impl Discovery {
    pub fn new(base: &str, max_symbols: usize) -> anyhow::Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .gzip(true)
                .user_agent("voluntas-challenge/0.2")
                .build()?,
            base: base.trim_end_matches('/').to_owned(),
            max_symbols,
        })
    }

    /// `since=None`: full open inventory. Incremental scans deliberately omit
    /// status, so paused/closed markets can retire existing subscriptions.
    pub async fn scan(&self, since: Option<i64>) -> anyhow::Result<Inventory> {
        let watermark = chrono::Utc::now().timestamp();
        let started = std::time::Instant::now();
        let mut markets = HashMap::<String, Market>::new();
        let mut cursors = HashSet::new();
        let mut cursor = String::new();
        let mut pages = 0;
        loop {
            let mut query = vec![
                ("mve_filter", "exclude".to_owned()),
                ("limit", "1000".to_owned()),
                ("cursor", cursor.clone()),
            ];
            if let Some(since) = since {
                query.push(("min_updated_ts", since.to_string()));
                query.push(("max_updated_ts", watermark.to_string()));
            } else {
                query.push(("status", "open".to_owned()));
            }
            let mut attempt = 0;
            let page = loop {
                let response = self
                    .client
                    .get(format!("{}/markets", self.base))
                    .query(&query)
                    .send()
                    .await?;
                if (response.status().as_u16() == 429 || response.status().is_server_error())
                    && attempt < 5
                {
                    tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                    attempt += 1;
                    continue;
                }
                let mut response = response.error_for_status()?;
                let mut bytes = Vec::new();
                while let Some(chunk) = response.chunk().await? {
                    ensure!(
                        bytes.len() + chunk.len() <= 16 << 20,
                        "metadata page exceeds 16 MiB"
                    );
                    bytes.extend_from_slice(&chunk);
                }
                break serde_json::from_slice::<Page>(&bytes).context("invalid markets page")?;
            };
            pages += 1;
            for market in page.markets {
                ensure!(
                    !market.ticker.is_empty(),
                    "empty market ticker in discovery"
                );
                if let Some(previous) = markets.get(&market.ticker) {
                    ensure!(
                        previous == &market,
                        "conflicting duplicate market metadata; retry whole scan"
                    );
                } else {
                    markets.insert(market.ticker.clone(), market);
                }
                ensure!(
                    markets.len() <= self.max_symbols,
                    "metadata inventory exceeds symbol budget"
                );
            }
            if page.cursor.is_empty() {
                break;
            }
            ensure!(
                pages < 10_000 && cursors.insert(page.cursor.clone()),
                "pagination cursor cycle or page budget exceeded"
            );
            cursor = page.cursor;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let mut markets: Vec<_> = markets.into_values().collect();
        markets.sort_by(|a, b| a.ticker.cmp(&b.ticker));
        let instruments = markets
            .into_iter()
            .map(|market| {
                let eligible = market.regular() && market.trading();
                Instrument {
                    symbol: market.ticker,
                    eligible,
                }
            })
            .collect();
        Ok(Inventory {
            instruments,
            watermark,
            pages,
            elapsed: started.elapsed(),
            authoritative_for_retirement: true,
            complete: true,
            metadata: None,
        })
    }
}

pub async fn all_live(base: &str, max_symbols: usize) -> anyhow::Result<Vec<String>> {
    let inventory = Discovery::new(base, max_symbols)?.scan(None).await?;
    let symbols: Vec<_> = inventory
        .instruments
        .into_iter()
        .filter(|m| m.eligible)
        .map(|m| m.symbol)
        .collect();
    eprintln!(
        "discovery: {} pages, {} unique regular live markets",
        inventory.pages,
        symbols.len()
    );
    Ok(symbols)
}

impl Catalog for Discovery {
    fn supports_incremental(&self) -> bool {
        true
    }
    fn scan(
        &self,
        since: Option<i64>,
    ) -> futures_util::future::BoxFuture<'_, anyhow::Result<Inventory>> {
        Box::pin(Discovery::scan(self, since))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn classification_uses_metadata_and_accepts_active_status() {
        let regular: Market =
            serde_json::from_str(r#"{"ticker":"SOME-MVE-NAME","status":"active"}"#).unwrap();
        assert!(regular.regular() && regular.trading());
        let mve: Market = serde_json::from_str(
            r#"{"ticker":"COMBO","status":"active","mve_collection_ticker":"COLLECTION"}"#,
        )
        .unwrap();
        assert!(!mve.regular());
        let closed: Market =
            serde_json::from_str(r#"{"ticker":"TEST","status":"closed"}"#).unwrap();
        assert!(!closed.trading());
        let null:Market=serde_json::from_str(r#"{"ticker":"TEST","status":"active","mve_collection_ticker":null,"mve_selected_legs":null}"#).unwrap();
        assert!(null.regular());
    }

    async fn fake_pages(pages: Vec<(u16, String)>) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            for (status, body) in pages {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = vec![];
                let mut byte = [0; 1];
                while !request.ends_with(b"\r\n\r\n") {
                    assert!(request.len() < 8192);
                    socket.read_exact(&mut byte).await.unwrap();
                    request.push(byte[0]);
                }
                let request = String::from_utf8(request).unwrap().to_lowercase();
                assert!(request.contains("mve_filter=exclude"));
                assert!(request.contains("gzip"));
                let response = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{address}"), task)
    }

    #[tokio::test]
    async fn pagination_deduplicates_and_keeps_regular_mve_named_tickers() {
        let (base,server)=fake_pages(vec![
            (200,r#"{"markets":[{"ticker":"A","status":"active"}],"cursor":"next cursor&"}"#.into()),
            (200,r#"{"markets":[{"ticker":"A","status":"active"},{"ticker":"NAME-MVE","status":"active"},{"ticker":"CLOSED","status":"closed"},{"ticker":"COMBO","status":"active","mve_collection_ticker":"C"}],"cursor":""}"#.into()),
        ]).await;
        assert_eq!(all_live(&base, 10).await.unwrap(), ["A", "NAME-MVE"]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn failed_page_and_cursor_cycle_never_return_a_partial_inventory() {
        for second in [
            (503, "{}".into()),
            (200, r#"{"markets":[],"cursor":"same"}"#.into()),
        ] {
            let (base, server) = fake_pages(vec![
                (
                    200,
                    r#"{"markets":[{"ticker":"A","status":"active"}],"cursor":"same"}"#.into(),
                ),
                second,
            ])
            .await;
            assert!(all_live(&base, 10).await.is_err());
            server.await.unwrap();
        }
    }
}
