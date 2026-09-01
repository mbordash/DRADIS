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

//! US retail venue — custodial, CFTC-regulated Polymarket US platform.
//!
//! Web2 custodial execution (per-request Ed25519 signatures via the `X-PM-*`
//! headers) against `api.prod.polymarketexchange.com`. No `alloy`, no Polymarket
//! SDK, no EIP-712 — all crypto identity is the portal API key in [`auth::UsAuth`].
//!
//! ## Market identity
//! The neutral [`MarketId`] carries the instrument **symbol**
//! (e.g. `tec-nfl-sbw-2026-02-08-kc-yes`) — the id the positions feed, the WS
//! streams, and the batched-order token arrays all use. The live API accepts
//! symbol-addressed orders via `outcomeSide` + `action` (the older
//! `market_slug` + `intent` pairing is no longer required), so the venue derives
//! the outcome leg directly from the symbol with a pure mapping — no network
//! catalog round-trip (decision D5).
//!
//! Spec: `docs/us_retail_api.md` + live-API order-routing/auth update.

pub mod auth;
pub mod markets;
pub mod trader;
pub mod types;
pub mod ws;

use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use polymarket_us::PolymarketUsClient;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::str::FromStr;
use tracing::{debug, info, warn};

use crate::venues::core::{
    Execution, Fill, FillStream, MarketId, OpenOrder, OrderId, OrderIntent, Position, Side,
    TimeInForce,
};

use auth::UsAuth;

/// Default authenticated API base (per developer portal).
const DEFAULT_BASE_URL: &str = "https://api.polymarket.us";
/// Public (unauthenticated) gateway host — the SDK routes public reference
/// endpoints like `/v1/search` here; they 404 on the authenticated api host.
const GATEWAY_BASE_URL: &str = "https://gateway.polymarket.us";
/// Override the gateway base URL (staging / mock).
const ENV_BASE_URL: &str = "POLYMARKET_US_BASE_URL";
/// Minimum cumulative volume (USD) a market must have to be considered for
/// trading. Low default — freshly-listed open markets have little volume yet;
/// high-volume markets tend to be already-closed resolved events. Override via env.
const ENV_MIN_VOLUME: &str = "POLYMARKET_US_MIN_VOLUME";
const DEFAULT_MIN_VOLUME: f64 = 5_000.0;
/// How many days back to look for recently-listed markets. Newly posted games
/// are the ones still open; stale listings are resolved events awaiting settlement.
const MARKET_START_LOOKBACK_DAYS: i64 = 7;

/// The custodial US retail venue (web2 auth, no signer).
pub struct UsRetailVenue {
    client: PolymarketUsClient,
    base_url: String,
    auth: Arc<UsAuth>,
    /// Shared HTTP client — used for raw market-discovery requests that bypass
    /// the SDK's typed deserializers (which are too strict for the live API).
    http: Arc<reqwest::Client>,
    /// Fan-out sender for the private account fill feed (`/v1/ws/private`).
    /// The pump task is spawned in [`Self::connect`] and lives for the venue's
    /// lifetime; [`Execution::subscribe_fills`] hands out receivers.
    fills_tx: tokio::sync::broadcast::Sender<crate::venues::core::FillEvent>,
}

impl UsRetailVenue {
    /// Bootstrap the US venue: read custodial credentials from the environment,
    /// verify gateway connectivity, and validate auth with a signed probe.
    pub async fn connect(http: Arc<reqwest::Client>) -> Result<Self> {
        let base_url = std::env::var(ENV_BASE_URL)
            .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string());
        let auth = UsAuth::from_env().context("US retail auth bootstrap failed")?;
        // NOTE: the shared `http` client is reqwest 0.13 (workspace) while the
        // polymarket-us SDK still pins reqwest 0.12 — the two `Client` types
        // don't unify, so the SDK builds its own internal client. The shared
        // client is still used for every non-SDK call on this venue.
        let client = PolymarketUsClient::builder()
            .api_base_url(base_url.clone())
            .gateway_base_url(base_url.clone())
            .auth(auth.clone())
            .build()
            .map_err(|e| anyhow!("US retail SDK client bootstrap failed: {e}"))?;

        let venue = Self { client, base_url, auth: Arc::new(auth), http, fills_tx: tokio::sync::broadcast::channel(256).0 };

        venue.health_check().await.context("US retail health check failed")?;
        // Validate the Ed25519 API key with a signed balances probe. We use the
        // balances endpoint (not positions) because it's the auth-bearing call
        // the trader actually depends on for collateral, and it fails
        // independently of the sometimes-flaky positions service.
        venue
            .fetch_balances()
            .await
            .context("US retail auth validation failed (signed account balance probe)")?;
        info!("✅ Authenticated on Polymarket US. Key ID: {}", venue.auth.key_id());

        // Start the account-wide private fill feed (spec §4.2). It outlives any
        // single market patrol — rotations keep the same venue handle — so it is
        // spawned once here with a never-cancelled token.
        ws::spawn_private_fill_feed(
            ws::private_ws_url_from_base(&venue.base_url),
            Arc::clone(&venue.auth),
            venue.fills_tx.clone(),
            tokio_util::sync::CancellationToken::new(),
        );

        Ok(venue)
    }

    /// Full `wss://…/v1/ws/markets` endpoint for [`ws::spawn_market_feed`].
    pub fn markets_ws_url(&self) -> String {
        ws::ws_url_from_base(&self.base_url)
    }

    /// Shared Ed25519 signer for authenticating the market-data WS handshake.
    ///
    /// The US gateway rejects an unauthenticated WS upgrade with `401`, so the
    /// streaming feed must sign the handshake with the same `X-PM-*` headers as
    /// REST. The signer is re-used (re-signing per reconnect) by the feed task.
    pub fn ws_auth(&self) -> Arc<UsAuth> {
        Arc::clone(&self.auth)
    }

    /// Discover active binary (`LONG`/`SHORT`) markets via `GET /v1/markets`.
    ///
    /// This is public reference data (no auth required per spec), but the production
    /// gateway returns 401 without auth headers, so we attach them anyway.
    ///
    /// We intentionally bypass the SDK's typed `markets_list_authenticated()` here
    /// because the live API returns `"outcomes":"[...]"` as a JSON-encoded *string*,
    /// not a JSON array.  The SDK's strict `Vec<Value>` field rejects the string and
    /// the whole response fails to deserialize.  Using a raw HTTP call lets us parse
    /// into our own lenient `types::MarketsResponse` where `outcomes: Value` accepts
    /// any JSON shape without error.
    pub async fn discover_binary_markets(&self) -> Result<Vec<markets::UsMarketPair>> {
        self.discover_binary_markets_filtered(&[], None).await
    }

    /// Category-filtered variant of [`Self::discover_binary_markets`].
    ///
    /// `categories` maps to the gateway's repeated `categories=` query params
    /// (SDK: `MarketsListParams.categories`); empty = no category filter.
    /// `min_volume` overrides the default volume floor — the crypto wing passes
    /// `Some(0.0)` because hourly crypto markets rotate every hour and start
    /// near zero volume, so the sports-tuned floor (plus `orderBy=closed`
    /// pagination dominated by thousands of sports listings) buried them
    /// entirely (2026-08-08: 3000 pairs discovered, zero crypto).
    pub async fn discover_binary_markets_filtered(
        &self,
        categories: &[&str],
        min_volume: Option<f64>,
    ) -> Result<Vec<markets::UsMarketPair>> {
        const PAGE_LIMIT: usize = 200;
        const MAX_PAGES: usize = 20; // safety cap — the API may cycle
        let path = "/v1/markets";
        let mut all_markets: Vec<types::UsMarket> = Vec::new();
        let mut page = 1usize;
        let mut prev_first_slug = String::new();

        loop {
            // endDate = settlement date (can be days/weeks after the game).
            // startDate = when the market was listed — recently-listed markets
            // are the ones whose events are still upcoming. Using startDateMin
            // focuses the query on fresh listings, avoiding the mass of resolved
            // events that settled recently. volumeNumMin is kept low because
            // open markets for today's games start with minimal volume.
            let now = chrono::Utc::now();
            let start_min = (now - chrono::Duration::days(MARKET_START_LOOKBACK_DAYS))
                .format("%Y-%m-%dT%H:%M:%SZ");
            let end_min = now.format("%Y-%m-%dT%H:%M:%SZ");
            let end_max = (now + chrono::Duration::days(21)).format("%Y-%m-%dT%H:%M:%SZ");
            let min_vol = min_volume.unwrap_or_else(|| {
                std::env::var(ENV_MIN_VOLUME)
                    .ok()
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(DEFAULT_MIN_VOLUME)
            });
            let category_params: String = categories
                .iter()
                .map(|c| format!("&categories={c}"))
                .collect();
            let url = format!(
                "{}{}?startDateMin={}&endDateMin={}&endDateMax={}&volumeNumMin={}&orderBy=closed&limit={}&page={}{}",
                self.base_url, path, start_min, end_min, end_max, min_vol, PAGE_LIMIT, page,
                category_params
            );
            // Auth headers are signed against the path only (no query string).
            let signed = self.auth.signed_headers("GET", path);

            let response = self.http
                .get(&url)
                .header(signed[0].0, &signed[0].1)
                .header(signed[1].0, &signed[1].1)
                .header(signed[2].0, &signed[2].1)
                .header("Content-Type", "application/json")
                .send()
                .await
                .with_context(|| format!("markets HTTP request failed (page {page})"))?;

            let http_status = response.status();
            let text = response.text().await.context("markets response read failed")?;

            if !http_status.is_success() {
                anyhow::bail!("markets endpoint returned HTTP {}: {}", http_status, text);
            }

            // One-shot record of what the gateway actually sends. This venue's
            // schema has drifted more than once — `asks` vs `offers`, prices
            // nested under `px.value`, two spellings of volume — and each time
            // the symptom was a silently-zero or silently-empty field rather
            // than a parse error. Logging the key set of the first market on the
            // first page turns the next such drift into one log line instead of
            // an investigation.
            if page == 1 {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(first) = v.get("markets").and_then(|m| m.as_array()).and_then(|a| a.first()) {
                        if let Some(obj) = first.as_object() {
                            let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
                            keys.sort_unstable();
                            info!("🔎 US /v1/markets field set: {}", keys.join(", "));
                        }
                    }
                }
            }

            let parsed: types::MarketsResponse = serde_json::from_str(&text)
                .context("markets JSON parse failed")?;

            let count = parsed.markets.len();

            // Detect API pagination cycling: if the first slug repeats from the
            // previous page, the server is ignoring the `page` param and looping.
            let first_slug = parsed.markets.first().map(|m| m.slug.clone()).unwrap_or_default();
            if page > 1 && first_slug == prev_first_slug {
                info!("US market discovery: API is cycling at page {page} — stopping pagination");
                break;
            }
            prev_first_slug = first_slug;

            // Count open vs closed so we can tell at a glance what the API returned.
            let open_count = parsed.markets.iter().filter(|m| !m.closed).count();
            let sample: Vec<_> = parsed.markets.iter().take(3)
                .map(|m| format!("\"{}\" (closed={})", m.question, m.closed))
                .collect();
            info!("US market discovery page {page}: {count} markets ({open_count} open) — sample: {}", sample.join(", "));
            all_markets.extend(parsed.markets);

            // Stop when: last page (fewer than limit), safety cap, or entire page was closed
            // (API isn't filtering properly and there's nothing more to find).
            if count < PAGE_LIMIT || page >= MAX_PAGES || open_count == 0 {
                if open_count == 0 && count == PAGE_LIMIT {
                    info!("US market discovery: full page returned but all closed — stopping (API filter ineffective)");
                }
                break;
            }
            page += 1;
        }

        let raw_total = all_markets.len();

        // Category census — one line showing the gateway's actual taxonomy, so
        // a category filter that the server ignores (or names differently) is
        // immediately visible in the logs instead of silently returning the
        // wrong domain.
        {
            let mut census: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
            for m in &all_markets {
                let key = if m.category.is_empty() { "(empty)" } else { m.category.as_str() };
                *census.entry(key).or_default() += 1;
            }
            let mut counts: Vec<_> = census.into_iter().collect();
            counts.sort_by(|a, b| b.1.cmp(&a.1));
            let summary: Vec<String> = counts.iter().take(12)
                .map(|(c, n)| format!("{c}={n}"))
                .collect();
            info!("US market discovery: categories seen: {}", summary.join(", "));
            if !categories.is_empty() {
                let requested: Vec<String> = categories.iter().map(|c| c.to_lowercase()).collect();
                let foreign = all_markets.iter()
                    .filter(|m| !requested.contains(&m.category.to_lowercase()))
                    .count();
                if foreign > 0 {
                    tracing::warn!(
                        "US market discovery: requested categories {categories:?} but {foreign}/{raw_total} \
                         markets came back with other categories — gateway may ignore the filter"
                    );
                }
            }
        }

        let pairs = markets::pair_markets(all_markets);
        info!(
            "US market discovery: {raw_total} raw markets across {page} page(s) → {} tradeable pairs",
            pairs.len()
        );
        Ok(pairs)
    }

    /// What the venue says `leg_symbol` settled at, if its market settled.
    ///
    /// Polymarket US settlement mechanics: the venue is CUSTODIAL — there is no
    /// on-chain redemption and nothing for the account holder to do. When a
    /// market resolves, the gateway cash-settles positions internally and drops
    /// them from `/v1/portfolio/positions`, so a settled winner vanishes
    /// between two dashboard sweeps with no event the trader observes. The
    /// venue's settlement record is the market object itself: at
    /// `MARKET_STATUS_RESOLVED` each side's `price` is pinned to its payout
    /// (verified live 2026-08-31; see [`markets::settlement_from_market`]).
    ///
    /// NOT used: the SDK's dedicated `/v1/markets/{symbol}/settlement`
    /// endpoint. Probed live first, it returns `200` with an EMPTY price even
    /// for resolved markets — designing around it untested would have shipped a
    /// sweep that reports coverage while checking nothing.
    ///
    /// Fetched raw with the `?slug=` filter — verified to return exactly the
    /// one market, and ZERO for an unknown slug, unlike `symbol=`/`search=`
    /// which the gateway ignores and answers with a default 20-market page
    /// (booking from THAT would read some unrelated market's resolution).
    /// Raw rather than through the SDK for the same reason as
    /// [`Self::discover_binary_markets`]: the SDK's strict deserializers reject
    /// the live API's string-encoded arrays.
    pub(crate) async fn settlement_resolution(
        &self,
        leg_symbol: &str,
    ) -> crate::venues::core::TokenResolution {
        use crate::venues::core::TokenResolution as R;

        // The leg must be identifiable or no settlement value can ever be
        // attributed — and retrying never changes a symbol, so deferring would
        // only delay the fallback. `NotClosed` leaves the row to the
        // pre-existing mark-priced path, which at least books an estimate.
        let is_long = match Self::outcome_side_from_symbol(leg_symbol) {
            Ok(polymarket_us::types::OrderSide::Long) => true,
            Ok(_) => false,
            Err(e) => {
                warn!("US settlement sweep: cannot infer side for '{leg_symbol}' ({e}) — leaving to mark-priced reconciliation");
                return R::NotClosed;
            }
        };
        let slug = markets::bare_symbol(leg_symbol);

        let path = "/v1/markets";
        let url = format!("{}{}?slug={}", self.base_url, path, slug);
        // Auth headers are signed against the path only (no query string) —
        // same contract as the discovery fetches above.
        let signed = self.auth.signed_headers("GET", path);
        let mut rb = self.http.get(&url).header("Content-Type", "application/json");
        for (name, value) in signed {
            rb = rb.header(name, value);
        }
        let text = match rb.send().await {
            Ok(resp) if resp.status().is_success() => match resp.text().await {
                Ok(t) => t,
                Err(e) => {
                    debug!("US settlement lookup read failed for {slug}: {e}");
                    return R::Unknown;
                }
            },
            Ok(resp) => {
                debug!("US settlement lookup for {slug} returned HTTP {}", resp.status());
                return R::Unknown;
            }
            Err(e) => {
                debug!("US settlement lookup request failed for {slug}: {e}");
                return R::Unknown;
            }
        };
        let parsed: types::MarketsResponse = match serde_json::from_str(&text) {
            Ok(p) => p,
            Err(e) => {
                debug!("US settlement lookup parse failed for {slug}: {e}");
                return R::Unknown;
            }
        };
        match parsed.markets.first() {
            Some(m) => markets::settlement_from_market(m, is_long),
            // An empty filtered answer means the gateway does not list this
            // market (at all, or any more). That is NOT proof it is still open,
            // so it must not be `NotClosed` — defer, and past the age bound the
            // row falls back to mark-priced reconciliation with a warning.
            None => R::Unknown,
        }
    }

    /// Crypto market discovery via `GET /v1/search` (the gateway ignores the
    /// `categories=` filter on `/v1/markets`, see 2026-08-08 field logs).
    ///
    /// The search response embeds full market records (`marketSides` included),
    /// so we pair directly from it — the api host also ignores the `eventSlug=`
    /// filter on `/v1/markets`, so a second fetch would return random sports
    /// markets (2026-08-08: 200 raw, "World Series Champion", zero crypto).
    pub async fn discover_crypto_markets_via_search(&self) -> Result<Vec<markets::UsMarketPair>> {
        self.discover_via_search(&["bitcoin", "ethereum", "solana", "xrp", "crypto"], "crypto").await
    }

    /// Politics markets, via the same search path and for the same reason.
    ///
    /// `/v1/markets` is sports-dominated — a live fetch returned 60 pairs with
    /// not one in the politics domain, while `/v1/search?query=politics` returns
    /// 133 open markets with real books. The politics wing found nothing at all
    /// until it stopped relying on the default listing.
    pub async fn discover_politics_markets_via_search(&self) -> Result<Vec<markets::UsMarketPair>> {
        self.discover_via_search(
            &["politics", "election", "senate", "president", "congress"],
            "politics",
        ).await
    }

    /// Shared search-based discovery. `label` only names the wing in logs.
    async fn discover_via_search(
        &self,
        queries: &[&str],
        label: &str,
    ) -> Result<Vec<markets::UsMarketPair>> {

        let mut event_count = 0usize;
        let mut all_markets: Vec<types::UsMarket> = Vec::new();
        let mut seen_slugs: std::collections::HashSet<String> = std::collections::HashSet::new();

        for q in queries {
            // /v1/search is public and lives on the gateway host (no auth —
            // it 404s on the authenticated api host).
            let url = format!("{GATEWAY_BASE_URL}/v1/search?query={q}&status=active&limit=100");
            let response = self.http
                .get(&url)
                .header("Content-Type", "application/json")
                .send()
                .await
                .with_context(|| format!("search HTTP request failed (query={q})"))?;
            let http_status = response.status();
            let text = response.text().await.context("search response read failed")?;
            if !http_status.is_success() {
                tracing::warn!("US search '{q}' returned HTTP {http_status}: {}",
                    text.chars().take(200).collect::<String>());
                continue;
            }
            let parsed: types::SearchResponse = match serde_json::from_str(&text) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!("US search '{q}' JSON parse failed: {e} — body: {}",
                        text.chars().take(200).collect::<String>());
                    continue;
                }
            };
            let hits = parsed.events.len();
            let mut added = 0usize;
            for ev in parsed.events {
                if ev.closed {
                    continue;
                }
                for mut m in ev.markets {
                    if m.slug.is_empty() || !seen_slugs.insert(m.slug.clone()) {
                        continue;
                    }
                    // Templated events blank the variant into the question
                    // ("Will Bitcoin be above ___ in 2026?") and put the real
                    // value in `title` ("$200,000"). Merge them so display and
                    // strike extraction see the full question.
                    if !m.title.is_empty() {
                        if m.question.contains("___") {
                            m.question = m.question.replace("___", &m.title);
                        } else if !m.question.contains(&m.title) {
                            m.question = format!("{} — {}", m.question, m.title);
                        }
                    }
                    all_markets.push(m);
                    added += 1;
                }
                event_count += 1;
            }
            info!("US {label} search '{q}': {hits} events, {added} new markets");
        }

        let raw_total = all_markets.len();
        let pairs = markets::pair_markets(all_markets);
        info!(
            "US {label} search discovery: {event_count} active event(s) → {raw_total} raw markets → {} tradeable pairs",
            pairs.len()
        );
        Ok(pairs)
    }

    /// Public connectivity probe (`GET /v1/health`, no auth).
    pub async fn health_check(&self) -> Result<()> {
        let body = self.client.health().await.context("health request failed")?;
        debug!("US retail gateway healthy ({}) @ {}", body.status, body.timestamp);
        Ok(())
    }

    // ── Neutral → wire mapping ───────────────────────────────────────────────

    /// Derive the instrument outcome leg (`LONG`/`SHORT`) from a `MarketId`
    /// symbol suffix — the symbol uniquely identifies the side, so no catalog
    /// lookup is needed. Recognizes the `yes/long/up` and `no/short/down`
    /// conventions Polymarket US uses across sports and crypto markets.
    fn outcome_side_from_symbol(symbol: &str) -> Result<polymarket_us::types::OrderSide> {
        // The leg suffix is authoritative. Live symbols end in a TEAM
        // abbreviation — `atc-lal-elc-fcb-2026-08-23-fcb`, `aec-nfl-lac-ten-…`
        // — so the dash convention below could never recover the side from a
        // real market and bailed on every order. It is kept only for symbols
        // that genuinely encode the side that way.
        if let Some(is_long) = markets::leg_is_long(symbol) {
            return Ok(if is_long {
                polymarket_us::types::OrderSide::Long
            } else {
                polymarket_us::types::OrderSide::Short
            });
        }
        let last = symbol.rsplit('-').next().unwrap_or("").to_ascii_lowercase();
        match last.as_str() {
            "yes" | "long" | "up" => Ok(polymarket_us::types::OrderSide::Long),
            "no" | "short" | "down" => Ok(polymarket_us::types::OrderSide::Short),
            _ => bail!("US retail: cannot infer outcome side from symbol '{symbol}'"),
        }
    }

    fn map_action(side: Side) -> polymarket_us::types::OrderAction {
        match side {
            Side::Buy => polymarket_us::types::OrderAction::Buy,
            Side::Sell => polymarket_us::types::OrderAction::Sell,
        }
    }

    fn map_tif(tif: TimeInForce) -> polymarket_us::types::TimeInForce {
        match tif {
            TimeInForce::Gtc => polymarket_us::types::TimeInForce::GoodTillCancel,
            TimeInForce::Gtd => polymarket_us::types::TimeInForce::GoodTillDate,
            TimeInForce::Fak => polymarket_us::types::TimeInForce::ImmediateOrCancel,
            TimeInForce::Fok => polymarket_us::types::TimeInForce::FillOrKill,
        }
    }

    /// US contracts trade in whole units; convert a neutral `Decimal` quantity to
    /// the integer share count the gateway expects (rejecting non-positive sizes).
    fn map_quantity(quantity: Decimal) -> Result<u64> {
        let rounded = quantity.round();
        let n = rounded
            .to_u64()
            .ok_or_else(|| anyhow!("US retail: invalid order quantity {quantity}"))?;
        if n == 0 {
            bail!("US retail: order quantity rounds to zero ({quantity})");
        }
        Ok(n)
    }

    /// Build the JSON order body for one neutral intent (pure — no network).
    fn build_order(intent: &OrderIntent) -> Result<types::PlaceOrderRequest> {
        // Side comes from the leg suffix; the wire gets the bare symbol, which is
        // the only form the venue recognizes.
        let outcome_side = Self::outcome_side_from_symbol(intent.market.as_str())?;
        let symbol = markets::bare_symbol(intent.market.as_str()).to_string();
        let quantity = Self::map_quantity(intent.quantity)?;
        let expires_at = if matches!(intent.tif, TimeInForce::Gtd) && intent.expiration_secs > 0 {
            Some((chrono::Utc::now().timestamp() as u64).saturating_add(intent.expiration_secs))
        } else {
            None
        };

        Ok(types::PlaceOrderRequest {
            symbol,
            action: Self::map_action(intent.side),
            outcome_side: outcome_side,
            order_type: polymarket_us::types::OrderType::Limit,
            price: types::Money {
                value: intent.price.normalize().to_string(),
                currency: "USD".to_string(),
            },
            quantity,
            tif: Self::map_tif(intent.tif),
            client_order_id: None,
            post_only: intent.post_only,
            expires_at,
        })
    }

    /// POST a single prepared order and map the ack to a neutral `Fill`.
    async fn submit_order(&self, intent: &OrderIntent) -> Result<Fill> {
        let body = Self::build_order(intent)?;
        let ack = self.client.orders().place(&body).await.context("order POST failed")?;

        Ok(Fill {
            order_id: OrderId(ack.order_id),
            market: intent.market.clone(),
            filled: resolve_filled(ack.filled_quantity, intent),
            price: intent.price, fee: Decimal::ZERO
        })
    }

    /// Fetch account balances (`GET /v1/account/balances`) and return the
    /// available collateral (`buyingPower`).
    ///
    /// This is the canonical auth-validation + collateral probe: it touches only
    /// the balances endpoint, so a transient outage on the *positions* endpoint
    /// can't break venue bring-up or collateral reads (the two are independent
    /// gateway services and fail independently).
    async fn fetch_balances(&self) -> Result<f64> {
        let bal_data = self
            .client
            .account()
            .balances()
            .await
            .context("account balances request failed")?;
        // Use buyingPower as the available collateral.
        Ok(bal_data.balances.first().map(|b| b.buying_power).unwrap_or(0.0))
    }

    /// Fetch open positions (`GET /v1/portfolio/positions`).
    ///
    /// Kept independent from [`fetch_balances`] so a transient `5xx` here only
    /// affects the positions view (dashboard sync skips its reconcile/purge
    /// pass on error) and never the auth/collateral path.
    async fn fetch_positions(&self) -> Result<Vec<types::UsPosition>> {
        let pos_data = self
            .client
            .portfolio()
            .positions()
            .await
            .context("portfolio positions request failed")?;

        let mut positions = Vec::new();
        // Positions map might have entries; also check availablePositions array.
        for (symbol, mut pos) in pos_data.positions {
            if pos.symbol.is_empty() {
                pos.symbol = symbol;
            }
            positions.push(pos);
        }
        positions.extend(pos_data.available_positions);
        Ok(positions)
    }
}

#[async_trait]
impl Execution for UsRetailVenue {
    async fn place_order(&self, intent: OrderIntent) -> Result<Fill> {
        self.submit_order(&intent).await
    }

    async fn place_atomic(&self, legs: [OrderIntent; 2]) -> Result<[Fill; 2]> {
        // Engine-atomic two-leg placement via `/v1/orders/batched` (atomic=true):
        // the gateway places both legs or neither, eliminating the single-sided
        // orphan risk that a network-parallel pair of single POSTs would carry.
        let [a, b] = legs;
        let body = types::BatchedOrderRequest {
            orders: vec![Self::build_order(&a)?, Self::build_order(&b)?],
            atomic: true,
        };
        let ack = self
            .client
            .orders()
            .place_batch(&body)
            .await
            .context("batched order POST failed")?;
        if ack.orders.len() != 2 {
            bail!(
                "US retail batched order: expected 2 acks, got {}",
                ack.orders.len()
            );
        }

        let to_fill = |ack: &types::PlaceOrderResponse, intent: &OrderIntent| Fill {
            order_id: OrderId(ack.order_id.clone()),
            market: intent.market.clone(),
            filled: resolve_filled(ack.filled_quantity, intent),
            price: intent.price, fee: Decimal::ZERO
        };
        Ok([to_fill(&ack.orders[0], &a), to_fill(&ack.orders[1], &b)])
    }

    /// Resting orders as the VENUE reports them.
    ///
    /// Implemented against `polymarket-us` 0.8, where `GET /v1/orders/open` now
    /// deserializes into a full `OpenOrder`. Before that the SDK typed the response
    /// as the place-order acknowledgement, which carries no market, side or price,
    /// so this venue inherited the trait default and reported nothing — and the
    /// startup order sweep silently swept nothing while logging that the account
    /// was clean.
    ///
    /// Orders whose market or side cannot be resolved are SKIPPED rather than
    /// guessed at: this list drives cancellation, and a wrong market id would
    /// either cancel the wrong order or hide a real one behind a filter that can
    /// never match.
    async fn open_orders(&self) -> Result<Vec<OpenOrder>> {
        let resp = self
            .client
            .orders()
            .open(None::<&()>)
            .await
            .context("open orders GET failed")?;

        let mut out = Vec::with_capacity(resp.orders.len());
        for o in resp.orders {
            if o.id.is_empty() || o.market_slug.is_empty() {
                continue;
            }
            // DRADIS names a US leg `{slug}#long` / `{slug}#short`; the API reports
            // the outcome as YES / NO. See `markets::leg_is_long`.
            let leg = match o.outcome_side {
                Some(types::OutcomeSide::Yes) => markets::LEG_LONG,
                Some(types::OutcomeSide::No) => markets::LEG_SHORT,
                _ => continue,
            };
            let side = match o.side {
                Some(types::OrderSideDirection::Buy) => Side::Buy,
                Some(types::OrderSideDirection::Sell) => Side::Sell,
                _ => continue,
            };
            let tif = match o.tif {
                Some(types::TimeInForce::GoodTillCancel) => TimeInForce::Gtc,
                Some(types::TimeInForce::GoodTillDate) => TimeInForce::Gtd,
                Some(types::TimeInForce::ImmediateOrCancel) => TimeInForce::Fak,
                Some(types::TimeInForce::FillOrKill) => TimeInForce::Fok,
                // An order the venue is still reporting as open is resting, whatever
                // it calls its time-in-force. Treating an unknown value as GTC keeps
                // it visible to `is_resting`, so the sweep can still cancel it.
                _ => TimeInForce::Gtc,
            };
            let price = o
                .price
                .as_ref()
                .and_then(|p| Decimal::from_str(&p.value).ok())
                .unwrap_or(Decimal::ZERO);

            out.push(OpenOrder {
                order_id: OrderId(o.id),
                market: MarketId::new(format!("{}{}", o.market_slug, leg)),
                side,
                price,
                original_qty: Decimal::try_from(o.quantity).unwrap_or(Decimal::ZERO),
                filled_qty: Decimal::try_from(o.cum_quantity).unwrap_or(Decimal::ZERO),
                tif,
                pair_market: None,
            });
        }
        Ok(out)
    }

    async fn cancel(&self, id: OrderId) -> Result<()> {
        let ack = self
            .client
            .orders()
            .cancel_trading(&id.0)
            .await
            .context("cancel DELETE failed")?;
        debug!("US retail: order {} → {}", ack.order_id, ack.status);
        Ok(())
    }

    async fn collateral(&self) -> Result<Decimal> {
        let buying_power = self.fetch_balances().await?;
        Decimal::try_from(buying_power)
            .map_err(|e| anyhow!("US retail: invalid buying_power: {e}"))
    }

    async fn positions(&self) -> Result<Vec<Position>> {
        let raw = self.fetch_positions().await?;
        let mut out = Vec::with_capacity(raw.len());
        for p in raw {
            if p.quantity == 0 {
                continue;
            }
            let avg_price = Decimal::from_str(p.avg_entry_price.trim()).unwrap_or(Decimal::ZERO);
            out.push(Position {
                market: MarketId::new(p.symbol),
                shares: Decimal::from(p.quantity),
                avg_price,
            });
        }
        Ok(out)
    }

    fn subscribe_fills(&self) -> Option<FillStream> {
        Some(self.fills_tx.subscribe())
    }

    async fn best_ask(&self, market: &MarketId) -> Result<Option<Decimal>> {
        let bbo = self
            .client
            .markets()
            .bbo(market.as_str())
            .await
            .with_context(|| format!("bbo query failed for {market}"))?;
        Ok(bbo
            .ask
            .and_then(|lvl| Decimal::from_str(lvl.price.trim()).ok())
            .filter(|p| *p > Decimal::ZERO))
    }
}

/// Resolve a venue-acknowledged fill quantity, honoring resting semantics.
///
/// Resting (`Gtc`/`Gtd`) orders report their **real** filled amount (0 = still
/// resting on the book) so the US lifecycle reconciler confirms the actual fill
/// later from the positions endpoint — never fabricating one. Immediate
/// (`Fak`/`Fok`) acks that report 0 fall back to the requested size, since a
/// success on an immediate order means it took liquidity.
fn resolve_filled(filled_quantity: u64, intent: &OrderIntent) -> Decimal {
    match intent.tif {
        TimeInForce::Gtc | TimeInForce::Gtd => Decimal::from(filled_quantity),
        TimeInForce::Fak | TimeInForce::Fok => {
            if filled_quantity > 0 {
                Decimal::from(filled_quantity)
            } else {
                intent.quantity
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_us::types as sdk;

    #[test]
    fn outcome_side_inferred_from_symbol_suffix() {
        assert_eq!(
            UsRetailVenue::outcome_side_from_symbol("tec-nfl-sbw-2026-02-08-kc-yes").unwrap(),
            sdk::OrderSide::Long
        );
        assert_eq!(
            UsRetailVenue::outcome_side_from_symbol("btc-up-or-down-2026-06-15-no").unwrap(),
            sdk::OrderSide::Short
        );
        assert_eq!(
            UsRetailVenue::outcome_side_from_symbol("eth-hourly-up").unwrap(),
            sdk::OrderSide::Long
        );
        assert_eq!(
            UsRetailVenue::outcome_side_from_symbol("eth-hourly-down").unwrap(),
            sdk::OrderSide::Short
        );
        assert!(UsRetailVenue::outcome_side_from_symbol("mystery-symbol-xyz").is_err());
    }

    /// The case the dash convention above could never handle.
    ///
    /// Live Polymarket US symbols end in a TEAM abbreviation — `…-2026-08-23-fcb`
    /// for FC Barcelona, `aec-nfl-lac-ten-…` — because the venue puts the side in
    /// a `long` boolean on `marketSides`, not in the symbol. Every real order
    /// therefore failed with "cannot infer outcome side", while the tests above
    /// passed on symbols no market actually has.
    #[test]
    fn a_real_team_suffixed_symbol_resolves_by_its_leg() {
        use polymarket_us::types::OrderSide;
        let base = "atc-lal-elc-fcb-2026-08-23-fcb";

        // Unsuffixed: genuinely ambiguous, and must say so rather than guess.
        assert!(UsRetailVenue::outcome_side_from_symbol(base).is_err());

        assert_eq!(
            UsRetailVenue::outcome_side_from_symbol(&format!("{base}#long")).unwrap(),
            OrderSide::Long,
        );
        assert_eq!(
            UsRetailVenue::outcome_side_from_symbol(&format!("{base}#short")).unwrap(),
            OrderSide::Short,
        );
    }

    /// The suffix is DRADIS's own bookkeeping; the venue must never see it.
    #[test]
    fn the_order_sends_the_bare_symbol_with_the_side_in_its_own_field() {
        use crate::venues::core::{MarketId, OrderIntent, Side, TimeInForce};
        let base = "atc-lal-elc-fcb-2026-08-23-fcb";
        let intent = OrderIntent {
            market: MarketId::new(format!("{base}#short")),
            side: Side::Buy,
            quantity: rust_decimal_macros::dec!(5),
            price: rust_decimal_macros::dec!(0.42),
            tif: TimeInForce::Fak,
            post_only: false,
            expiration_secs: 0,
            fee_bps: 0,
            is_neg_risk: false,
        };
        let req = UsRetailVenue::build_order(&intent).expect("order must build");
        assert_eq!(req.symbol, base, "leg suffix leaked onto the wire");
        assert_eq!(req.outcome_side, polymarket_us::types::OrderSide::Short);
    }

    #[test]
    fn action_maps_from_side() {
        assert_eq!(UsRetailVenue::map_action(Side::Buy), sdk::OrderAction::Buy);
        assert_eq!(UsRetailVenue::map_action(Side::Sell), sdk::OrderAction::Sell);
    }

    #[test]
    fn tif_maps_to_protocol_enums() {
        assert_eq!(UsRetailVenue::map_tif(TimeInForce::Gtc), sdk::TimeInForce::GoodTillCancel);
        assert_eq!(UsRetailVenue::map_tif(TimeInForce::Gtd), sdk::TimeInForce::GoodTillDate);
        assert_eq!(UsRetailVenue::map_tif(TimeInForce::Fak), sdk::TimeInForce::ImmediateOrCancel);
        assert_eq!(UsRetailVenue::map_tif(TimeInForce::Fok), sdk::TimeInForce::FillOrKill);
    }

    #[test]
    fn quantity_rounds_and_rejects_zero() {
        use rust_decimal_macros::dec;
        assert_eq!(UsRetailVenue::map_quantity(dec!(100)).unwrap(), 100);
        assert_eq!(UsRetailVenue::map_quantity(dec!(99.6)).unwrap(), 100);
        assert!(UsRetailVenue::map_quantity(dec!(0)).is_err());
        assert!(UsRetailVenue::map_quantity(dec!(0.2)).is_err());
    }

    #[test]
    fn build_order_produces_symbol_addressed_body() {
        use rust_decimal_macros::dec;
        let intent = OrderIntent {
            market: MarketId::new("tec-nfl-sbw-2026-02-08-kc-yes"),
            side: Side::Buy,
            quantity: dec!(100),
            price: dec!(0.55),
            tif: TimeInForce::Gtc,
            post_only: true,
            expiration_secs: 0,
            is_neg_risk: false,
            fee_bps: 0,
        };
        let body = UsRetailVenue::build_order(&intent).unwrap();
        assert_eq!(body.symbol, "tec-nfl-sbw-2026-02-08-kc-yes");
        assert_eq!(body.action, sdk::OrderAction::Buy);
        assert_eq!(body.outcome_side, sdk::OrderSide::Long);
        assert_eq!(body.order_type, sdk::OrderType::Limit);
        assert_eq!(body.quantity, 100);
        assert_eq!(body.price.value, "0.55");
        assert!(body.post_only);
        assert!(body.expires_at.is_none());
    }

    #[test]
    fn batched_pair_serializes_atomic_with_two_legs() {
        use rust_decimal_macros::dec;
        let mk = |sym: &str, px| OrderIntent {
            market: MarketId::new(sym),
            side: Side::Buy,
            quantity: dec!(10),
            price: px,
            tif: TimeInForce::Fok,
            post_only: false,
            expiration_secs: 0,
            is_neg_risk: false,
            fee_bps: 0,
        };
        let body = types::BatchedOrderRequest {
            orders: vec![
                UsRetailVenue::build_order(&mk("game-yes", dec!(0.55))).unwrap(),
                UsRetailVenue::build_order(&mk("game-no", dec!(0.42))).unwrap(),
            ],
            atomic: true,
        };
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["atomic"], true);
        assert_eq!(json["orders"].as_array().unwrap().len(), 2);
        assert_eq!(json["orders"][0]["outcomeSide"], "LONG");
        assert_eq!(json["orders"][1]["outcomeSide"], "SHORT");
    }

    #[test]
    fn resting_orders_never_fabricate_fills() {
        use rust_decimal_macros::dec;
        let intent = |tif| OrderIntent {
            market: MarketId::new("game-yes"),
            side: Side::Buy,
            quantity: dec!(100),
            price: dec!(0.55),
            tif,
            post_only: false,
            expiration_secs: 0,
            is_neg_risk: false,
            fee_bps: 0,
        };
        // Resting (GTC/GTD) acks reporting 0 filled stay 0 — no fabricated fill.
        assert_eq!(resolve_filled(0, &intent(TimeInForce::Gtc)), dec!(0));
        assert_eq!(resolve_filled(0, &intent(TimeInForce::Gtd)), dec!(0));
        // Resting partial fill is reported as-is.
        assert_eq!(resolve_filled(40, &intent(TimeInForce::Gtc)), dec!(40));
        // Immediate (FAK/FOK) acks reporting 0 fall back to requested size.
        assert_eq!(resolve_filled(0, &intent(TimeInForce::Fok)), dec!(100));
        assert_eq!(resolve_filled(25, &intent(TimeInForce::Fak)), dec!(25));
    }
}


// Live smoke for the settlement sweep's gateway lookup — run with:
// cargo test --no-default-features --features us_retail us_settlement_resolution_live_smoke -- --ignored --nocapture
//
// Asserts the resolution path against real gateway data: a known-resolved
// market's legs price decisively and complementarily, an open market's legs
// answer NotClosed, and an unknown slug answers Unknown (the `?slug=` filter
// returning empty — NOT the default 20-market page, which would have this
// sweep reading some unrelated market's resolution). This is the runtime path
// `sync_dashboard` exercises for a vanished position; a green unit suite alone
// cannot prove the gateway still sends what this code reads.
#[cfg(test)]
#[tokio::test]
#[ignore]
async fn us_settlement_resolution_live_smoke() {
    use crate::venues::core::TokenResolution as R;
    dotenv::dotenv().ok();
    let v = UsRetailVenue::connect(Arc::new(reqwest::Client::new()))
        .await
        .expect("venue");

    // A long-resolved NFL market (2025-11-02, Chargers over Titans — long side
    // paid $1). Resolved markets stay in the catalog; verified still listed on
    // 2026-08-31. If the gateway ever delists it this prints Unknown and the
    // assertion tells you the fixture needs refreshing.
    let resolved = "aec-nfl-lac-ten-2025-11-02";
    let long = v.settlement_resolution(&format!("{resolved}{}", markets::LEG_LONG)).await;
    let short = v.settlement_resolution(&format!("{resolved}{}", markets::LEG_SHORT)).await;
    println!("resolved market: long={long:?} short={short:?}");
    match (&long, &short) {
        (R::Resolved(l), R::Resolved(s)) => {
            assert_eq!(*l + *s, rust_decimal::Decimal::ONE, "legs must be complementary");
        }
        other => panic!("resolved market must price both legs decisively, got {other:?}"),
    }

    // An open market: neither leg may claim a settlement.
    if let Some(pair) = v
        .discover_crypto_markets_via_search()
        .await
        .expect("crypto discovery")
        .first()
    {
        let r = v.settlement_resolution(pair.long.as_str()).await;
        println!("open {}: {r:?}", pair.slug);
        assert_eq!(r, R::NotClosed, "an open market's position left by a trade, not settlement");
    }

    // An unknown slug must answer Unknown — never NotClosed (it is not
    // verifiably open) and never some other market's resolution.
    let ghost = v.settlement_resolution("dradis-smoke-no-such-market#long").await;
    println!("unknown slug: {ghost:?}");
    assert_eq!(ghost, R::Unknown);
}
