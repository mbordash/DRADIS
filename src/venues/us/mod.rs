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
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::str::FromStr;
use tracing::{debug, info};

use crate::venues::core::{
    Execution, Fill, MarketId, OrderId, OrderIntent, Position, Side, TimeInForce,
};

use auth::UsAuth;
use types::{order_action, order_type, outcome as oc, tif as tif_const};

/// Default authenticated API base (per developer portal).
const DEFAULT_BASE_URL: &str = "https://api.polymarket.us";
/// Override the gateway base URL (staging / mock).
const ENV_BASE_URL: &str = "POLYMARKET_US_BASE_URL";

/// The custodial US retail venue (web2 auth, no signer).
pub struct UsRetailVenue {
    http: Arc<reqwest::Client>,
    base_url: String,
    auth: Arc<UsAuth>,
}

impl UsRetailVenue {
    /// Bootstrap the US venue: read custodial credentials from the environment,
    /// verify gateway connectivity, and validate auth with a signed probe.
    pub async fn connect(http: Arc<reqwest::Client>) -> Result<Self> {
        let base_url = std::env::var(ENV_BASE_URL)
            .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string());
        let auth = UsAuth::from_env().context("US retail auth bootstrap failed")?;

        let venue = Self { http, base_url, auth: Arc::new(auth) };

        venue.health_check().await.context("US retail health check failed")?;
        // Validate the Ed25519 API key with a signed account balance probe.
        venue
            .fetch_portfolio()
            .await
            .context("US retail auth validation failed (signed account balance probe)")?;
        info!("✅ Authenticated on Polymarket US. Key ID: {}", venue.auth.key_id());

        Ok(venue)
    }

    // ── HTTP plumbing ────────────────────────────────────────────────────────

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
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
    pub async fn discover_binary_markets(&self) -> Result<Vec<markets::UsMarketPair>> {
        let rb = self
            .http
            .get(self.url("/v1/markets"))
            .query(&[("status", "ACTIVE"), ("limit", "500")]);
        let rb = self.authed(rb, "GET", "/v1/markets");
        let resp = rb
            .send()
            .await
            .context("markets request failed")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("US retail markets returned HTTP {status}: {body}");
        }
        let body = resp.text().await.context("reading markets response")?;

        // Parse as Value first to handle any JSON quirks
        let mut json_val: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| {
                let preview = &body.chars().take(4000).collect::<String>();
                anyhow!("markets JSON parse failed: {e}\nFirst 4000 chars: {preview}")
            })?;

        // Fix stringified arrays in market objects if needed
        if let Some(markets_array) = json_val.get_mut("markets").and_then(|v| v.as_array_mut()) {
            for market in markets_array.iter_mut() {
                // Check all string fields for stringified JSON arrays/objects
                if let Some(obj) = market.as_object_mut() {
                    for (_, val) in obj.iter_mut() {
                        if let Some(s) = val.as_str() {
                            // If it's a stringified array or object, try to parse it
                            if (s.starts_with('[') && s.ends_with(']'))
                                || (s.starts_with('{') && s.ends_with('}'))
                            {
                                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(s) {
                                    *val = parsed;
                                }
                            }
                        }
                    }
                }
            }
        }

        let parsed: types::MarketsResponse = serde_json::from_value(json_val)
            .map_err(|e| anyhow!("markets response decode failed: {e}"))?;
        Ok(markets::pair_markets(parsed.markets))
    }

    /// Attach the `X-PM-*` Ed25519 signature headers to an authenticated request.
    ///
    /// Signing is local and stateless (no token round-trip), so this is sync.
    /// The signature covers `method + path` (e.g., `"GET/v1/account/balance"`).
    fn authed(&self, mut rb: reqwest::RequestBuilder, method: &str, path: &str) -> reqwest::RequestBuilder {
        for (name, value) in self.auth.signed_headers(method, path) {
            rb = rb.header(name, value);
        }
        rb.header("Content-Type", "application/json")
    }

    /// Public connectivity probe (`GET /v1/health`, no auth).
    pub async fn health_check(&self) -> Result<()> {
        let resp = self
            .http
            .get(self.url("/v1/health"))
            .send()
            .await
            .context("health request failed")?;
        let status = resp.status();
        let body: types::HealthResponse = resp
            .json()
            .await
            .context("health response decode failed")?;
        if !status.is_success() {
            bail!("US retail health returned HTTP {status} ({})", body.status);
        }
        debug!("US retail gateway healthy ({}) @ {}", body.status, body.timestamp);
        Ok(())
    }

    // ── Neutral → wire mapping ───────────────────────────────────────────────

    /// Derive the instrument outcome leg (`LONG`/`SHORT`) from a `MarketId`
    /// symbol suffix — the symbol uniquely identifies the side, so no catalog
    /// lookup is needed. Recognises the `yes/long/up` and `no/short/down`
    /// conventions Polymarket US uses across sports and crypto markets.
    fn outcome_side_from_symbol(symbol: &str) -> Result<&'static str> {
        let last = symbol.rsplit('-').next().unwrap_or("").to_ascii_lowercase();
        match last.as_str() {
            "yes" | "long" | "up" => Ok(oc::LONG),
            "no" | "short" | "down" => Ok(oc::SHORT),
            _ => bail!("US retail: cannot infer outcome side from symbol '{symbol}'"),
        }
    }

    fn map_action(side: Side) -> &'static str {
        match side {
            Side::Buy => order_action::BUY,
            Side::Sell => order_action::SELL,
        }
    }

    fn map_tif(tif: TimeInForce) -> &'static str {
        match tif {
            TimeInForce::Gtc => tif_const::GTC,
            TimeInForce::Gtd => tif_const::GTD,
            TimeInForce::Fak => tif_const::FAK,
            TimeInForce::Fok => tif_const::FOK,
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
            action: Self::map_action(intent.side).to_string(),
            outcome_side: outcome_side.to_string(),
            order_type: order_type::LIMIT.to_string(),
            price: types::Money {
                value: intent.price.normalize().to_string(),
                currency: "USD".to_string(),
            },
            quantity,
            tif: Self::map_tif(intent.tif).to_string(),
            client_order_id: None,
            post_only: intent.post_only,
            expires_at,
        })
    }

    /// POST a single prepared order and map the ack to a neutral `Fill`.
    async fn submit_order(&self, intent: &OrderIntent) -> Result<Fill> {
        let body = Self::build_order(intent)?;
        let rb = self.http.post(self.url("/v1/trading/orders")).json(&body);
        let rb = self.authed(rb, "POST", "/v1/trading/orders");
        let resp = rb.send().await.context("order POST failed")?;
        let status = resp.status();
        if !status.is_success() {
            let err = resp.text().await.unwrap_or_default();
            bail!("US retail order rejected (HTTP {status}): {err}");
        }
        let ack: types::PlaceOrderResponse =
            resp.json().await.context("order response decode failed")?;

        // `filled_quantity` is the venue-acknowledged fill; fall back to the
        // requested size for resting (GTC) acks that report 0 filled immediately.
        let filled = if ack.filled_quantity > 0 {
            Decimal::from(ack.filled_quantity)
        } else {
            intent.quantity
        };

        Ok(Fill {
            order_id: OrderId(ack.order_id),
            market: intent.market.clone(),
            filled,
            price: intent.price,
        })
    }

    /// Fetch positions and balances (combines two API calls).
    async fn fetch_portfolio(&self) -> Result<types::PortfolioResponse> {
        // Fetch positions
        let pos_rb = self.http.get(self.url("/v1/portfolio/positions"));
        let pos_rb = self.authed(pos_rb, "GET", "/v1/portfolio/positions");
        let pos_resp = pos_rb.send().await.context("portfolio positions request failed")?;
        let pos_status = pos_resp.status();
        if !pos_status.is_success() {
            let err = pos_resp.text().await.unwrap_or_default();
            bail!("US portfolio/positions failed (HTTP {pos_status}): {err}");
        }
        let pos_data: types::PortfolioPositionsResponse =
            pos_resp.json().await.context("portfolio positions decode failed")?;

        // Fetch balances
        let bal_rb = self.http.get(self.url("/v1/account/balances"));
        let bal_rb = self.authed(bal_rb, "GET", "/v1/account/balances");
        let bal_resp = bal_rb.send().await.context("account balances request failed")?;
        let bal_status = bal_resp.status();
        if !bal_status.is_success() {
            let err = bal_resp.text().await.unwrap_or_default();
            bail!("US account/balances failed (HTTP {bal_status}): {err}");
        }
        let bal_data: types::AccountBalancesResponse =
            bal_resp.json().await.context("account balances decode failed")?;

        // Combine into a unified view
        let mut positions = Vec::new();
        // Positions map might have entries; also check availablePositions array
        for (symbol, mut pos) in pos_data.positions {
            if pos.symbol.is_empty() {
                pos.symbol = symbol;
            }
            positions.push(pos);
        }
        positions.extend(pos_data.available_positions);

        // Use buyingPower as the available collateral
        let buying_power = bal_data.balances.first()
            .map(|b| b.buying_power)
            .unwrap_or(0.0);

        Ok(types::PortfolioResponse {
            positions,
            buying_power,
        })
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
        let rb = self.http.post(self.url("/v1/orders/batched")).json(&body);
        let rb = self.authed(rb, "POST", "/v1/orders/batched");
        let resp = rb.send().await.context("batched order POST failed")?;
        let status = resp.status();
        if !status.is_success() {
            let err = resp.text().await.unwrap_or_default();
            bail!("US retail batched order rejected (HTTP {status}): {err}");
        }
        let ack: types::BatchedOrderResponse =
            resp.json().await.context("batched order response decode failed")?;
        if ack.orders.len() != 2 {
            bail!(
                "US retail batched order: expected 2 acks, got {}",
                ack.orders.len()
            );
        }

        let to_fill = |ack: &types::PlaceOrderResponse, intent: &OrderIntent| Fill {
            order_id: OrderId(ack.order_id.clone()),
            market: intent.market.clone(),
            filled: if ack.filled_quantity > 0 {
                Decimal::from(ack.filled_quantity)
            } else {
                intent.quantity
            },
            price: intent.price,
        };
        Ok([to_fill(&ack.orders[0], &a), to_fill(&ack.orders[1], &b)])
    }

    async fn cancel(&self, id: OrderId) -> Result<()> {
        let path = format!("/v1/trading/orders/{}", id.0);
        let rb = self.http.delete(self.url(&path));
        let rb = self.authed(rb, "DELETE", &path);
        let resp = rb.send().await.context("cancel DELETE failed")?;
        let status = resp.status();
        if !status.is_success() {
            let err = resp.text().await.unwrap_or_default();
            bail!("US retail cancel failed (HTTP {status}): {err}");
        }
        let ack: types::CancelOrderResponse =
            resp.json().await.context("cancel response decode failed")?;
        debug!("US retail: order {} → {}", ack.order_id, ack.status);
        Ok(())
    }

    async fn collateral(&self) -> Result<Decimal> {
        let portfolio = self.fetch_portfolio().await?;
        Decimal::try_from(portfolio.buying_power)
            .map_err(|e| anyhow!("US retail: invalid buying_power: {e}"))
    }

    async fn positions(&self) -> Result<Vec<Position>> {
        let portfolio = self.fetch_portfolio().await?;
        let mut out = Vec::with_capacity(portfolio.positions.len());
        for p in portfolio.positions {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_side_inferred_from_symbol_suffix() {
        assert_eq!(
            UsRetailVenue::outcome_side_from_symbol("tec-nfl-sbw-2026-02-08-kc-yes").unwrap(),
            oc::LONG
        );
        assert_eq!(
            UsRetailVenue::outcome_side_from_symbol("btc-up-or-down-2026-06-15-no").unwrap(),
            oc::SHORT
        );
        assert_eq!(
            UsRetailVenue::outcome_side_from_symbol("eth-hourly-up").unwrap(),
            oc::LONG
        );
        assert_eq!(
            UsRetailVenue::outcome_side_from_symbol("eth-hourly-down").unwrap(),
            oc::SHORT
        );
        assert!(UsRetailVenue::outcome_side_from_symbol("mystery-symbol-xyz").is_err());
    }

    #[test]
    fn action_maps_from_side() {
        assert_eq!(UsRetailVenue::map_action(Side::Buy), order_action::BUY);
        assert_eq!(UsRetailVenue::map_action(Side::Sell), order_action::SELL);
    }

    #[test]
    fn tif_maps_to_protocol_enums() {
        assert_eq!(UsRetailVenue::map_tif(TimeInForce::Gtc), tif_const::GTC);
        assert_eq!(UsRetailVenue::map_tif(TimeInForce::Gtd), tif_const::GTD);
        assert_eq!(UsRetailVenue::map_tif(TimeInForce::Fak), tif_const::FAK);
        assert_eq!(UsRetailVenue::map_tif(TimeInForce::Fok), tif_const::FOK);
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
        assert_eq!(body.action, order_action::BUY);
        assert_eq!(body.outcome_side, oc::LONG);
        assert_eq!(body.order_type, order_type::LIMIT);
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
        assert_eq!(json["orders"][0]["outcomeSide"], oc::LONG);
        assert_eq!(json["orders"][1]["outcomeSide"], oc::SHORT);
    }
}

