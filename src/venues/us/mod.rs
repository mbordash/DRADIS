//! US retail venue — custodial, CFTC-regulated Polymarket US platform.
//!
//! Stub only. The full `Execution` implementation (API-key/secret/session-token
//! auth, HMAC request signing, custodial balance/order endpoints, UUID/slug
//! market IDs) lands in Step 3 of the venue-abstraction rollout. No `alloy` or
//! Polymarket SDK dependencies are pulled into this build.

use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::venues::core::{Execution, Fill, OrderId, OrderIntent, Position};

/// The custodial US retail venue (web2 auth, no signer).
pub struct UsRetailVenue {
    _http: reqwest::Client,
}

#[async_trait]
impl Execution for UsRetailVenue {
    async fn place_order(&self, _intent: OrderIntent) -> Result<Fill> {
        anyhow::bail!("UsRetailVenue: not yet implemented (Step 3)")
    }
    async fn place_atomic(&self, _legs: [OrderIntent; 2]) -> Result<[Fill; 2]> {
        anyhow::bail!("UsRetailVenue: not yet implemented (Step 3)")
    }
    async fn cancel(&self, _id: OrderId) -> Result<()> {
        anyhow::bail!("UsRetailVenue: not yet implemented (Step 3)")
    }
    async fn collateral(&self) -> Result<Decimal> {
        anyhow::bail!("UsRetailVenue: not yet implemented (Step 3)")
    }
    async fn positions(&self) -> Result<Vec<Position>> {
        anyhow::bail!("UsRetailVenue: not yet implemented (Step 3)")
    }
}

