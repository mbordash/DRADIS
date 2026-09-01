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

    /// Every open market under the given Kalshi categories, paged.
    ///
    /// Kalshi organizes discovery by series ticker, and there are thousands of
    /// them — 2,226 under Politics, 3,491 under Sports — so fetching markets
    /// series by series is not viable. `/events?with_nested_markets=true`
    /// carries the category on each event and nests its markets, so one sweep
    /// covers everything and the filtering happens here.
    ///
    /// Categories are matched case-insensitively. Returns `(category, market)`
    /// so the caller can label what it found.
    pub async fn open_markets_for_categories(
        &self,
        categories: &[&str],
    ) -> anyhow::Result<Vec<(String, types::KalshiMarket)>> {
        let wanted: Vec<String> = categories.iter().map(|c| c.to_ascii_lowercase()).collect();
        let mut out = Vec::new();
        let mut cursor = String::new();
        // Bounded: the open-event set is a few hundred pages at most, and an
        // unbounded loop here would hang market discovery on a bad cursor.
        for _page in 0..12 {
            let path = if cursor.is_empty() {
                "/events?status=open&with_nested_markets=true&limit=200".to_string()
            } else {
                format!("/events?status=open&with_nested_markets=true&limit=200&cursor={cursor}")
            };
            let resp: types::EventsResponse = self.get_json(&path).await?;
            for ev in resp.events {
                if !wanted.iter().any(|w| ev.category.eq_ignore_ascii_case(w)) {
                    continue;
                }
                for m in ev.markets {
                    out.push((ev.category.clone(), m));
                }
            }
            cursor = resp.cursor;
            if cursor.is_empty() {
                break;
            }
        }
        Ok(out)
    }

    /// One market by ticker.
    pub async fn market(&self, ticker: &str) -> anyhow::Result<types::KalshiMarket> {
        let resp: types::MarketResponse = self.get_json(&format!("/markets/{ticker}")).await?;
        Ok(resp.market)
    }

    /// What the exchange says a leg of `market_id` settled at, if it settled.
    ///
    /// Kalshi settlement mechanics: there is no redemption step. Once the
    /// exchange determines a market's result, winning contracts pay $1.00 each
    /// and losing contracts pay $0.00, credited straight to the account balance
    /// — which is exactly why a settled position vanishes from
    /// `/portfolio/positions` between two dashboard sweeps with no event the
    /// trader ever observes. The market's `result` field is the exchange's own
    /// record of which side paid, and the authoritative input here.
    pub async fn settlement_resolution(&self, market_id: &str) -> crate::venues::core::TokenResolution {
        let (ticker, is_yes) = split_market_id(market_id);
        match self.market(&ticker).await {
            Ok(m) => interpret_settlement(&m.result, &m.status, is_yes, market_id),
            // No answer is not "no settlement" — a transient REST failure must
            // not route a possibly-settled position onto a path that books an
            // estimate or deletes the row. The caller defers and retries.
            Err(e) => {
                tracing::debug!("Kalshi settlement lookup failed for {market_id}: {e}");
                crate::venues::core::TokenResolution::Unknown
            }
        }
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

/// Map a market's `result` + `status` to the leg's settlement answer.
///
/// Pure so the mapping is testable without the exchange. The discipline is the
/// one on [`crate::venues::core::TokenResolution`]: only a decisive exchange
/// answer books, "cannot have settled" is distinct from "no answer yet", and
/// everything else defers.
///
///   * `result` `yes`/`no` — decisive. A binary contract's settlement value is
///     fully determined by which side paid: this leg is worth exactly $1.00 if
///     it matches the result, $0.00 if it does not. (Verified live 2026-08-31:
///     finalized `KXBTC15M` markets carry `result: "yes"` with
///     `settlement_value_dollars: "1.0000"`.)
///   * `result` empty, market still trading (or not yet open) — the position
///     cannot have settled, because settlement only exists after determination.
///     It left the account by a trade, which belongs on the mark-priced
///     reconciliation path.
///   * `result` empty, market `closed` or beyond — determination is imminent
///     (minutes on the crypto hourlies). Defer and ask again rather than
///     booking a guess; nothing can trade after close, so the row is not an
///     off-strategy sale waiting to be reconciled.
///   * `result` `void` — every contract is refunded at its purchase price, so
///     a $1.00/$0.00 booking would fabricate a win or a loss that never
///     happened. Deferred; past the age bound the row falls back to
///     mark-priced reconciliation, which at least books an estimate.
///   * anything else — a scalar or unmodeled result; same treatment as void.
fn interpret_settlement(
    result: &str,
    status: &str,
    is_yes: bool,
    market_id: &str,
) -> crate::venues::core::TokenResolution {
    use crate::venues::core::TokenResolution as R;
    use rust_decimal::Decimal;
    match result {
        "yes" => R::Resolved(if is_yes { Decimal::ONE } else { Decimal::ZERO }),
        "no" => R::Resolved(if is_yes { Decimal::ZERO } else { Decimal::ONE }),
        "" => match status {
            "initialized" | "unopened" | "open" | "active" | "paused" => R::NotClosed,
            _ => R::Unknown,
        },
        other => {
            tracing::warn!(
                "⚠️ Kalshi market {market_id} carries non-binary result {other:?} — \
                 cannot book a $1/$0 settlement from it; deferring"
            );
            R::Unknown
        }
    }
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

    /// A determined result prices each leg at exactly $1.00 or $0.00.
    ///
    /// The whole point of the settlement sweep: a winner that Kalshi paid and
    /// dropped from the portfolio between two sweeps must book its real value,
    /// not a last mark and not nothing (the 2026-08-31 incident class).
    #[test]
    fn a_determined_result_prices_both_legs_decisively() {
        use crate::venues::core::TokenResolution as R;
        use rust_decimal::Decimal;
        assert_eq!(interpret_settlement("yes", "finalized", true, "T#yes"), R::Resolved(Decimal::ONE));
        assert_eq!(interpret_settlement("yes", "finalized", false, "T#no"), R::Resolved(Decimal::ZERO));
        assert_eq!(interpret_settlement("no", "settled", true, "T#yes"), R::Resolved(Decimal::ZERO));
        assert_eq!(interpret_settlement("no", "settled", false, "T#no"), R::Resolved(Decimal::ONE));
    }

    /// While trading is open the position cannot have settled — it left by a
    /// trade, and belongs on the mark-priced reconciliation path. Collapsing
    /// this into Unknown would defer an ordinary sale for a day.
    #[test]
    fn an_undetermined_open_market_is_not_closed_rather_than_unknown() {
        use crate::venues::core::TokenResolution as R;
        assert_eq!(interpret_settlement("", "active", true, "T#yes"), R::NotClosed);
        assert_eq!(interpret_settlement("", "initialized", false, "T#no"), R::NotClosed);
    }

    /// Closed-but-undetermined must DEFER: determination is imminent, and
    /// booking from the last mark now would misprice the settlement that
    /// arrives minutes later. Same for an unknown future status string —
    /// guessing "still open" from a status we do not recognize would route a
    /// possibly-settled position to an estimated booking.
    #[test]
    fn closed_but_undetermined_defers_until_the_result_exists() {
        use crate::venues::core::TokenResolution as R;
        assert_eq!(interpret_settlement("", "closed", true, "T#yes"), R::Unknown);
        assert_eq!(interpret_settlement("", "determined", true, "T#yes"), R::Unknown);
        assert_eq!(interpret_settlement("", "some_future_status", true, "T#yes"), R::Unknown);
    }

    /// A void market refunds every contract at cost. $1/$0 cannot express a
    /// refund, so booking either side would fabricate a win or a loss that
    /// never happened — the answer is "no answer", never a guess.
    #[test]
    fn a_void_result_never_books_a_win_or_a_loss() {
        use crate::venues::core::TokenResolution as R;
        assert_eq!(interpret_settlement("void", "finalized", true, "T#yes"), R::Unknown);
        assert_eq!(interpret_settlement("void", "finalized", false, "T#no"), R::Unknown);
        // Scalar results on non-binary markets get the same treatment.
        assert_eq!(interpret_settlement("73800", "finalized", true, "T#yes"), R::Unknown);
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

// Live smoke for the settlement sweep's exchange lookup — run with:
// cargo test --no-default-features --features kalshi kalshi_settlement_resolution_live_smoke -- --ignored --nocapture
//
// Finds a recently SETTLED KXBTC15M market on the exchange and asserts the
// resolution path prices its two legs decisively and complementarily, and an
// OPEN market's legs as NotClosed. This is the runtime path `sync_dashboard`
// exercises for a vanished position; a green unit suite alone cannot prove the
// exchange still sends `result` where this code looks for it.
#[cfg(test)]
#[tokio::test]
#[ignore]
async fn kalshi_settlement_resolution_live_smoke() {
    use crate::venues::core::TokenResolution as R;
    dotenv::dotenv().ok();
    let v = KalshiVenue::from_env().expect("venue");

    // A settled market: both legs must resolve, decisively and complementarily.
    let settled: types::MarketsResponse = v
        .get_json("/markets?limit=1&status=settled&series_ticker=KXBTC15M")
        .await
        .expect("settled markets");
    let m = settled.markets.first().expect("at least one settled KXBTC15M market");
    println!("settled: {} status={} result={}", m.ticker, m.status, m.result);
    let yes = v.settlement_resolution(&leg_id(&m.ticker, true)).await;
    let no = v.settlement_resolution(&leg_id(&m.ticker, false)).await;
    println!("yes leg: {yes:?}, no leg: {no:?}");
    match (&yes, &no) {
        (R::Resolved(y), R::Resolved(n)) => {
            assert_eq!(*y + *n, rust_decimal::Decimal::ONE, "legs must be complementary");
        }
        other => panic!("settled market must price both legs decisively, got {other:?}"),
    }

    // An open market: neither leg may claim a settlement.
    let open = v.markets_for_series("KXBTC15M").await.expect("open markets");
    if let Some(m) = open.first() {
        let r = v.settlement_resolution(&leg_id(&m.ticker, true)).await;
        println!("open {}: {r:?}", m.ticker);
        assert_eq!(r, R::NotClosed, "an open market's position left by a trade, not settlement");
    }
}
