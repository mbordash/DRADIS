//! US retail venue — custodial, CFTC-regulated Polymarket US platform.
//!
//! Web2 custodial execution (Ed25519-minted JWT + `x-participant-id`) against
//! `api.prod.polymarketexchange.com`. No `alloy`, no Polymarket SDK, no EIP-712 —
//! all crypto identity is replaced by the short-lived token in [`auth::UsAuth`].
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

/// Default production REST/WS gateway (live-API update).
const DEFAULT_BASE_URL: &str = "https://api.prod.polymarketexchange.com";
/// Override the gateway base URL (staging / mock).
const ENV_BASE_URL: &str = "POLYMARKET_US_BASE_URL";

/// The custodial US retail venue (web2 auth, no signer).
pub struct UsRetailVenue {
    http: Arc<reqwest::Client>,
    base_url: String,
    auth: UsAuth,
}

impl UsRetailVenue {
    /// Bootstrap the US venue: read custodial credentials from the environment,
    /// verify gateway connectivity, and validate auth by minting an initial JWT.
    pub async fn connect(http: Arc<reqwest::Client>) -> Result<Self> {
        let base_url = std::env::var(ENV_BASE_URL)
            .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string());
        let auth = UsAuth::from_env(Arc::clone(&http), base_url.clone())
            .context("US retail auth bootstrap failed")?;

        let venue = Self { http, base_url, auth };

        venue.health_check().await.context("US retail health check failed")?;
        // Validate the Ed25519 credentials up-front by minting a token now.
        venue.auth.mint().await.context("US retail auth mint failed")?;
        info!("Authenticated on Polymarket US. Participant: {}", venue.auth.participant_id());

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

    /// Discover active binary (`LONG`/`SHORT`) markets via `GET /v1/markets`.
    ///
    /// This is public reference data (no auth required). Returns venue-neutral
    /// [`markets::UsMarketPair`]s the arbitrage loop can subscribe and trade.
    pub async fn discover_binary_markets(&self) -> Result<Vec<markets::UsMarketPair>> {
        let resp = self
            .http
            .get(self.url("/v1/markets"))
            .query(&[("status", "ACTIVE"), ("limit", "500")])
            .send()
            .await
            .context("markets request failed")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("US retail markets returned HTTP {status}: {body}");
        }
        let parsed: types::MarketsResponse =
            resp.json().await.context("markets response decode failed")?;
        Ok(markets::pair_markets(parsed.markets))
    }

    /// Attach the bearer + participant headers to an authenticated request.
    async fn authed(&self, rb: reqwest::RequestBuilder) -> Result<reqwest::RequestBuilder> {
        let bearer = self.auth.bearer().await?;
        Ok(rb
            .header("Authorization", format!("Bearer {bearer}"))
            .header("x-participant-id", self.auth.participant_id())
            .header("Content-Type", "application/json"))
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
        let rb = self.authed(rb).await?;
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

    /// Shared portfolio fetch used by both `collateral` and `positions`.
    async fn fetch_portfolio(&self) -> Result<types::PortfolioResponse> {
        let rb = self.http.get(self.url("/v1/portfolio/positions"));
        let rb = self.authed(rb).await?;
        let resp = rb.send().await.context("portfolio request failed")?;
        let status = resp.status();
        if !status.is_success() {
            let err = resp.text().await.unwrap_or_default();
            bail!("US retail portfolio failed (HTTP {status}): {err}");
        }
        resp.json().await.context("portfolio response decode failed")
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
        let rb = self.authed(rb).await?;
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
        let rb = self
            .http
            .delete(self.url(&format!("/v1/trading/orders/{}", id.0)));
        let rb = self.authed(rb).await?;
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
        Decimal::from_str(portfolio.balances.available_margin_usd.trim())
            .map_err(|e| anyhow!("US retail: invalid available_margin_usd: {e}"))
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

