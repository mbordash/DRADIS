//! US retail venue — custodial, CFTC-regulated Polymarket US platform.
//!
//! Web2 custodial execution (bearer token + `x-participant-id`) against
//! `api.polymarket.us`. No `alloy`, no Polymarket SDK, no EIP-712 — all crypto
//! identity is replaced by the short-lived token in [`auth::UsAuth`].
//!
//! ## Market identity
//! The neutral [`MarketId`] carries the instrument **symbol**
//! (e.g. `tec-nfl-sbw-2026-02-08-kc-yes`) — the id the positions feed and the WS
//! streams use. Order placement, however, addresses a market by
//! `market_slug` + an `intent` that fuses direction with the outcome leg
//! (`LONG`/`SHORT`). The venue therefore keeps a `symbol → (slug, outcome)`
//! catalog primed from `GET /v1/markets`, so a strategy only ever needs to hand
//! over a `MarketId` (decision D5).
//!
//! Spec: `docs/us_retail_api.md`.

pub mod auth;
pub mod types;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::str::FromStr;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::venues::core::{
    Execution, Fill, MarketId, OrderId, OrderIntent, Position, Side, TimeInForce,
};

use auth::UsAuth;
use types::{order_intent, order_type, outcome as oc, tif as tif_const};

/// Default production REST/WS gateway (spec §1).
const DEFAULT_BASE_URL: &str = "https://api.polymarket.us";
/// Override the gateway base URL (staging / mock).
const ENV_BASE_URL: &str = "POLYMARKET_US_BASE_URL";

/// Resolved metadata for one tradeable instrument leg.
#[derive(Clone, Debug)]
struct InstrumentMeta {
    market_slug: String,
    /// `LONG` (YES) or `SHORT` (NO).
    outcome: String,
}

/// The custodial US retail venue (web2 auth, no signer).
pub struct UsRetailVenue {
    http: Arc<reqwest::Client>,
    base_url: String,
    auth: UsAuth,
    /// `symbol → (market_slug, outcome)` catalog primed from `GET /v1/markets`.
    catalog: RwLock<HashMap<String, InstrumentMeta>>,
}

impl UsRetailVenue {
    /// Bootstrap the US venue: read custodial credentials from the environment,
    /// verify gateway connectivity, and prime the instrument catalog.
    pub async fn connect(http: Arc<reqwest::Client>) -> Result<Self> {
        let base_url = std::env::var(ENV_BASE_URL)
            .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string());
        let auth = UsAuth::from_env(Arc::clone(&http), base_url.clone())
            .context("US retail auth bootstrap failed")?;

        let venue = Self {
            http,
            base_url,
            auth,
            catalog: RwLock::new(HashMap::new()),
        };

        venue.health_check().await.context("US retail health check failed")?;
        info!("Authenticated on Polymarket US. Participant: {}", venue.auth.participant_id());

        // Best-effort catalog prime — a miss is recoverable lazily on first order.
        if let Err(e) = venue.refresh_catalog().await {
            warn!("⚠️ US retail: initial market catalog prime failed: {e} (will retry lazily)");
        }

        Ok(venue)
    }

    // ── HTTP plumbing ────────────────────────────────────────────────────────

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
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

    // ── Catalog (symbol → slug/outcome) ──────────────────────────────────────

    /// Refresh the full instrument catalog from `GET /v1/markets`.
    pub async fn refresh_catalog(&self) -> Result<()> {
        let rb = self
            .http
            .get(self.url("/v1/markets"))
            .query(&[("status", "ACTIVE"), ("limit", "500")]);
        let rb = self.authed(rb).await?;
        let resp = rb.send().await.context("markets request failed")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("US retail markets returned HTTP {status}: {body}");
        }
        let markets: types::MarketsResponse =
            resp.json().await.context("markets response decode failed")?;

        let mut next: HashMap<String, InstrumentMeta> = HashMap::new();
        for m in markets.markets {
            for inst in m.instruments {
                next.insert(
                    inst.symbol,
                    InstrumentMeta {
                        market_slug: m.market_slug.clone(),
                        outcome: inst.outcome.to_uppercase(),
                    },
                );
            }
        }
        let count = next.len();
        *self.catalog.write().await = next;
        debug!("US retail: catalog primed with {count} instrument(s)");
        Ok(())
    }

    /// Resolve a `MarketId` (instrument symbol) to its `(slug, outcome)`,
    /// refreshing the catalog once on a miss.
    async fn resolve(&self, market: &MarketId) -> Result<InstrumentMeta> {
        let symbol = market.as_str();
        if let Some(meta) = self.catalog.read().await.get(symbol) {
            return Ok(meta.clone());
        }
        // Cold miss — refresh once and retry.
        self.refresh_catalog().await?;
        self.catalog
            .read()
            .await
            .get(symbol)
            .cloned()
            .ok_or_else(|| anyhow!("US retail: unknown instrument symbol '{symbol}'"))
    }

    // ── Neutral → wire mapping ───────────────────────────────────────────────

    /// Fuse direction (`Side`) with the instrument outcome (`LONG`/`SHORT`) into
    /// the gateway's `ORDER_INTENT_*` enum.
    fn map_intent(side: Side, outcome: &str) -> Result<&'static str> {
        Ok(match (side, outcome) {
            (Side::Buy, oc::LONG) => order_intent::BUY_LONG,
            (Side::Sell, oc::LONG) => order_intent::SELL_LONG,
            (Side::Buy, oc::SHORT) => order_intent::BUY_SHORT,
            (Side::Sell, oc::SHORT) => order_intent::SELL_SHORT,
            (_, other) => bail!("US retail: unsupported instrument outcome '{other}'"),
        })
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

    /// Build the JSON order body for one neutral intent.
    async fn build_order(&self, intent: &OrderIntent) -> Result<types::PlaceOrderRequest> {
        let meta = self.resolve(&intent.market).await?;
        let order_intent = Self::map_intent(intent.side, &meta.outcome)?;
        let quantity = Self::map_quantity(intent.quantity)?;
        let expires_at = if matches!(intent.tif, TimeInForce::Gtd) && intent.expiration_secs > 0 {
            Some((chrono::Utc::now().timestamp() as u64).saturating_add(intent.expiration_secs))
        } else {
            None
        };

        Ok(types::PlaceOrderRequest {
            market_slug: meta.market_slug,
            intent: order_intent.to_string(),
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
        let body = self.build_order(intent).await?;
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
        // The US gateway exposes no engine-atomic two-leg primitive, so the legs
        // are submitted concurrently (network-parallel, not engine-atomic). The
        // arbitrage Viper's orphan handler reconciles a single-sided fill — the
        // same guarantee the intl venue's `place_atomic` provides.
        let [a, b] = legs;
        let (ra, rb) = tokio::join!(self.submit_order(&a), self.submit_order(&b));
        Ok([ra?, rb?])
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
    fn intent_fuses_side_and_outcome() {
        assert_eq!(
            UsRetailVenue::map_intent(Side::Buy, oc::LONG).unwrap(),
            order_intent::BUY_LONG
        );
        assert_eq!(
            UsRetailVenue::map_intent(Side::Sell, oc::LONG).unwrap(),
            order_intent::SELL_LONG
        );
        assert_eq!(
            UsRetailVenue::map_intent(Side::Buy, oc::SHORT).unwrap(),
            order_intent::BUY_SHORT
        );
        assert_eq!(
            UsRetailVenue::map_intent(Side::Sell, oc::SHORT).unwrap(),
            order_intent::SELL_SHORT
        );
        assert!(UsRetailVenue::map_intent(Side::Buy, "WAT").is_err());
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
}

