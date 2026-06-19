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
use tracing::{debug, info};

use crate::venues::core::{
    Execution, Fill, MarketId, OrderId, OrderIntent, Position, Side, TimeInForce,
};

use auth::UsAuth;

/// Default authenticated API base (per developer portal).
const DEFAULT_BASE_URL: &str = "https://api.polymarket.us";
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
    /// the SDK's typed deserialisers (which are too strict for the live API).
    http: Arc<reqwest::Client>,
}

impl UsRetailVenue {
    /// Bootstrap the US venue: read custodial credentials from the environment,
    /// verify gateway connectivity, and validate auth with a signed probe.
    pub async fn connect(http: Arc<reqwest::Client>) -> Result<Self> {
        let base_url = std::env::var(ENV_BASE_URL)
            .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string());
        let auth = UsAuth::from_env().context("US retail auth bootstrap failed")?;
        let client = PolymarketUsClient::builder()
            .api_base_url(base_url.clone())
            .gateway_base_url(base_url.clone())
            .http_client(http.as_ref().clone())
            .auth(auth.clone())
            .build()
            .map_err(|e| anyhow!("US retail SDK client bootstrap failed: {e}"))?;

        let venue = Self { client, base_url, auth: Arc::new(auth), http };

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
    /// the whole response fails to deserialise.  Using a raw HTTP call lets us parse
    /// into our own lenient `types::MarketsResponse` where `outcomes: Value` accepts
    /// any JSON shape without error.
    pub async fn discover_binary_markets(&self) -> Result<Vec<markets::UsMarketPair>> {
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
            let min_vol = std::env::var(ENV_MIN_VOLUME)
                .ok()
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(DEFAULT_MIN_VOLUME);
            let url = format!(
                "{}{}?startDateMin={}&endDateMin={}&endDateMax={}&volumeNumMin={}&orderBy=closed&limit={}&page={}",
                self.base_url, path, start_min, end_min, end_max, min_vol, PAGE_LIMIT, page
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
        let pairs = markets::pair_markets(all_markets);
        info!(
            "US market discovery: {raw_total} raw markets across {page} page(s) → {} tradeable pairs",
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
    /// lookup is needed. Recognises the `yes/long/up` and `no/short/down`
    /// conventions Polymarket US uses across sports and crypto markets.
    fn outcome_side_from_symbol(symbol: &str) -> Result<polymarket_us::types::OrderSide> {
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
        let symbol = intent.market.as_str().to_string();
        let outcome_side = Self::outcome_side_from_symbol(&symbol)?;
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
            price: intent.price,
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
    /// affects the positions view (dashboard sync tolerates it via
    /// `unwrap_or_default`) and never the auth/collateral path.
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
            price: intent.price,
        };
        Ok([to_fill(&ack.orders[0], &a), to_fill(&ack.orders[1], &b)])
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

