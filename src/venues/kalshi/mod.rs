// SPDX-License-Identifier: AGPL-3.0-only
//
// DRADIS — autonomous trading engine for crypto prediction markets.
// Copyright (C) 2026 Michael Bordash
//
// This file is part of DRADIS. DRADIS is free software: you can redistribute it
// and/or modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.
//
// DRADIS is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
// A PARTICULAR PURPOSE. See the GNU Affero General Public License for details.
//
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

//! Kalshi venue — CFTC-regulated US exchange with hourly + 15-min crypto
//! strike markets (`KXBTCD`, `KXBTC15M`, `KXETHD`, …).
//!
//! | Surface   | Production                                         | Demo                                               |
//! |-----------|----------------------------------------------------|----------------------------------------------------|
//! | REST      | `https://external-api.kalshi.com/trade-api/v2`     | `https://external-api.demo.kalshi.co/trade-api/v2` |
//! | WebSocket | `wss://external-api-ws.kalshi.com/trade-api/ws/v2` | `wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2` |
//!
//! Auth: RSA-PSS signed headers (see [`auth`]). Public market data (markets,
//! series, orderbook) requires no auth. Orders use the V2 endpoint
//! `/portfolio/events/orders` (bid/ask sides, fixed-point dollar strings).
//!
//! Env:
//! * `KALSHI_API_KEY_ID` / `KALSHI_PRIVATE_KEY_PATH` (or `KALSHI_PRIVATE_KEY`)
//! * `KALSHI_DEMO=1` → use the demo environment
//! * `KALSHI_BASE_URL` / `KALSHI_WS_URL` → explicit overrides (rare)

pub mod auth;
pub mod orders;
pub mod trader;
pub mod types;
pub mod ws;

use anyhow::Context;

pub use auth::KalshiAuth;

/// Production REST base (recommended host; `api.elections.kalshi.com` is the
/// legacy shared alias).
pub const PROD_BASE_URL: &str = "https://external-api.kalshi.com/trade-api/v2";
/// Demo REST base — mock funds, separate API keys.
pub const DEMO_BASE_URL: &str = "https://external-api.demo.kalshi.co/trade-api/v2";
/// Production WebSocket endpoint.
pub const PROD_WS_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
/// Demo WebSocket endpoint.
pub const DEMO_WS_URL: &str = "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2";

/// Resolve the REST base URL from env (`KALSHI_BASE_URL` > `KALSHI_DEMO` > prod).
pub fn base_url() -> String {
    if let Ok(url) = std::env::var("KALSHI_BASE_URL") {
        return url;
    }
    if demo_mode() {
        DEMO_BASE_URL.to_string()
    } else {
        PROD_BASE_URL.to_string()
    }
}

/// Resolve the WS URL from env (`KALSHI_WS_URL` > `KALSHI_DEMO` > prod).
pub fn ws_url() -> String {
    if let Ok(url) = std::env::var("KALSHI_WS_URL") {
        return url;
    }
    if demo_mode() {
        DEMO_WS_URL.to_string()
    } else {
        PROD_WS_URL.to_string()
    }
}

/// True when `KALSHI_DEMO` is set to a truthy value.
pub fn demo_mode() -> bool {
    matches!(
        std::env::var("KALSHI_DEMO").as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    )
}

/// The Kalshi venue. Implements [`crate::venues::core::Execution`].
#[derive(Debug)]
pub struct KalshiVenue {
    pub(crate) auth: std::sync::Arc<KalshiAuth>,
    pub(crate) http: reqwest::Client,
    pub(crate) base_url: String,
    /// Path prefix for signing (base URL minus scheme+host), e.g. `/trade-api/v2`.
    pub(crate) api_root: String,
    /// Venue-lifetime fill fan-out; [`crate::venues::core::Execution::subscribe_fills`]
    /// hands out receivers, [`Self::start_fill_feed`] pumps it from the WS.
    pub(crate) fills_tx: tokio::sync::broadcast::Sender<crate::venues::core::FillEvent>,
}

impl KalshiVenue {
    /// Construct from environment (key id + private key + demo flag).
    pub fn from_env() -> anyhow::Result<Self> {
        let auth = std::sync::Arc::new(KalshiAuth::from_env()?);
        let base = base_url();
        let api_root = api_root_of(&base);
        tracing::info!(
            "🏛️ Kalshi venue: base={} key_id={} demo={}",
            base,
            auth.key_id(),
            demo_mode()
        );
        Ok(Self {
            auth,
            http: reqwest::Client::new(),
            base_url: base,
            api_root,
            fills_tx: tokio::sync::broadcast::channel(256).0,
        })
    }

    /// Spawn the venue-lifetime private fill feed (WS `fill` channel).
    pub fn start_fill_feed(&self, cancel: tokio_util::sync::CancellationToken) {
        ws::spawn_fill_feed(ws_url(), self.auth.clone(), self.fills_tx.clone(), cancel);
    }

    // ── Signed HTTP helpers ──────────────────────────────────────────────────

    /// Signed GET. `path_qs` starts after the API root (e.g.
    /// `/markets?limit=5`); signing strips the query string per spec.
    pub(crate) async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        path_qs: &str,
    ) -> anyhow::Result<T> {
        let sign_path = format!("{}{}", self.api_root, path_qs);
        let headers = self.auth.signed_headers("GET", &sign_path);
        let url = format!("{}{}", self.base_url, path_qs);
        let resp = self
            .http
            .get(&url)
            .header(&headers[0].0, &headers[0].1)
            .header(&headers[1].0, &headers[1].1)
            .header(&headers[2].0, &headers[2].1)
            .send()
            .await
            .with_context(|| format!("Kalshi GET {path_qs} failed"))?;
        let status = resp.status();
        let text = resp.text().await.context("Kalshi response read failed")?;
        if !status.is_success() {
            anyhow::bail!("Kalshi GET {path_qs} → HTTP {status}: {}", truncate(&text, 300));
        }
        serde_json::from_str(&text)
            .with_context(|| format!("Kalshi GET {path_qs} JSON parse failed: {}", truncate(&text, 300)))
    }

    /// Signed POST with a JSON body.
    pub(crate) async fn post_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> anyhow::Result<T> {
        let sign_path = format!("{}{}", self.api_root, path);
        let headers = self.auth.signed_headers("POST", &sign_path);
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .http
            .post(&url)
            .header(&headers[0].0, &headers[0].1)
            .header(&headers[1].0, &headers[1].1)
            .header(&headers[2].0, &headers[2].1)
            .json(body)
            .send()
            .await
            .with_context(|| format!("Kalshi POST {path} failed"))?;
        let status = resp.status();
        let text = resp.text().await.context("Kalshi response read failed")?;
        if !status.is_success() {
            anyhow::bail!("Kalshi POST {path} → HTTP {status}: {}", truncate(&text, 300));
        }
        serde_json::from_str(&text)
            .with_context(|| format!("Kalshi POST {path} JSON parse failed: {}", truncate(&text, 300)))
    }

    /// Signed DELETE.
    pub(crate) async fn delete_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
    ) -> anyhow::Result<T> {
        let sign_path = format!("{}{}", self.api_root, path);
        let headers = self.auth.signed_headers("DELETE", &sign_path);
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .http
            .delete(&url)
            .header(&headers[0].0, &headers[0].1)
            .header(&headers[1].0, &headers[1].1)
            .header(&headers[2].0, &headers[2].1)
            .send()
            .await
            .with_context(|| format!("Kalshi DELETE {path} failed"))?;
        let status = resp.status();
        let text = resp.text().await.context("Kalshi response read failed")?;
        if !status.is_success() {
            anyhow::bail!("Kalshi DELETE {path} → HTTP {status}: {}", truncate(&text, 300));
        }
        serde_json::from_str(&text)
            .with_context(|| format!("Kalshi DELETE {path} JSON parse failed: {}", truncate(&text, 300)))
    }

    // ── Market discovery ─────────────────────────────────────────────────────

    /// Open markets for one series (`KXBTC15M`, `KXBTCD`, …), cursor-paginated.
    pub async fn markets_for_series(
        &self,
        series_ticker: &str,
    ) -> anyhow::Result<Vec<types::KalshiMarket>> {
        let mut out = Vec::new();
        let mut cursor = String::new();
        for _page in 0..10 {
            let path = if cursor.is_empty() {
                format!("/markets?series_ticker={series_ticker}&status=open&limit=200")
            } else {
                format!("/markets?series_ticker={series_ticker}&status=open&limit=200&cursor={cursor}")
            };
            let resp: types::MarketsResponse = self.get_json(&path).await?;
            let n = resp.markets.len();
            out.extend(resp.markets);
            if resp.cursor.is_empty() || n == 0 {
                break;
            }
            cursor = resp.cursor;
        }
        Ok(out)
    }

    /// One market by ticker.
    pub async fn market(&self, ticker: &str) -> anyhow::Result<types::KalshiMarket> {
        let resp: types::MarketResponse = self.get_json(&format!("/markets/{ticker}")).await?;
        Ok(resp.market)
    }

    /// Current orderbook for a market.
    pub async fn orderbook(&self, ticker: &str) -> anyhow::Result<types::OrderbookFp> {
        let resp: types::OrderbookResponse = self
            .get_json(&format!("/markets/{ticker}/orderbook"))
            .await?;
        Ok(resp.orderbook_fp)
    }
}

/// Strip scheme+host from a base URL, leaving the API-root path used in
/// signature payloads (e.g. `/trade-api/v2`).
fn api_root_of(base: &str) -> String {
    base.find("://")
        .and_then(|i| base[i + 3..].find('/').map(|j| base[i + 3 + j..].to_string()))
        .unwrap_or_else(|| "/trade-api/v2".to_string())
}

pub(crate) fn truncate(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

// ─── MarketId scheme ─────────────────────────────────────────────────────────
//
// DRADIS's neutral `MarketId` carries one string per tradeable leg. Kalshi has
// a single book per market with `bid`/`ask` sides, so we encode the leg as
// `{ticker}#yes` / `{ticker}#no`:
//   buy  {t}#yes → V2 bid  at P        (buy YES at P)
//   buy  {t}#no  → V2 ask  at 1−P      (sell YES ≡ buy NO at P)

/// Split a neutral market id into `(ticker, is_yes)`.
pub(crate) fn split_market_id(id: &str) -> (String, bool) {
    match id.rsplit_once('#') {
        Some((t, "no")) => (t.to_string(), false),
        Some((t, "yes")) => (t.to_string(), true),
        _ => (id.to_string(), true),
    }
}

/// Compose a neutral market id for a Kalshi ticker leg.
pub fn leg_id(ticker: &str, yes: bool) -> String {
    format!("{ticker}#{}", if yes { "yes" } else { "no" })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_root_parses() {
        assert_eq!(
            api_root_of("https://external-api.kalshi.com/trade-api/v2"),
            "/trade-api/v2"
        );
        assert_eq!(
            api_root_of("https://demo-api.kalshi.co/trade-api/v2"),
            "/trade-api/v2"
        );
    }

    #[test]
    fn market_id_scheme_round_trips() {
        assert_eq!(
            split_market_id(&leg_id("KXBTC15M-26AUG081330-30", true)),
            ("KXBTC15M-26AUG081330-30".to_string(), true)
        );
        assert_eq!(
            split_market_id(&leg_id("KXBTCD-26AUG0814-T73799.99", false)),
            ("KXBTCD-26AUG0814-T73799.99".to_string(), false)
        );
        // Bare ticker defaults to the YES leg.
        assert_eq!(split_market_id("KXBTC-X"), ("KXBTC-X".to_string(), true));
    }
}
// Live smoke test vs demo.kalshi.co — run with: cargo test --features kalshi kalshi_demo_live_smoke -- --ignored --nocapture
#[cfg(test)]
#[tokio::test]
#[ignore]
async fn kalshi_demo_live_smoke() {
    dotenv::dotenv().ok();
    let v = crate::venues::kalshi::KalshiVenue::from_env().expect("venue");
    use crate::venues::core::Execution;
    let bal = v.collateral().await.expect("balance");
    println!("BALANCE: {bal}");
    let pos = v.positions().await.expect("positions");
    println!("POSITIONS: {}", pos.len());
    let oo = v.open_orders().await.expect("open orders");
    println!("OPEN ORDERS: {}", oo.len());
    let mkts = v.markets_for_series("KXBTC15M").await.expect("markets");
    println!("KXBTC15M open markets: {}", mkts.len());
    if let Some(m) = mkts.first() {
        println!("first: {} strike={:?} close={:?}", m.ticker, m.strike(), m.close_time_utc());
        let ask = v.best_ask(&crate::venues::core::MarketId::new(crate::venues::kalshi::leg_id(&m.ticker, true))).await.expect("ask");
        println!("best yes ask: {ask:?}");
    }
}
