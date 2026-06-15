//! Custodial Web2 authentication for the Polymarket US retail venue.
//!
//! Unlike `intl_clob` (EIP-712 wallet signatures over Polygon), the US gateway
//! issues short-lived bearer access tokens scoped to a *participant* identity
//! (`firms/<FIRM>/users/<USER>`). Per the spec (§2) tokens expire every ~3
//! minutes, so the token is held behind an `RwLock` and stamped with an expiry
//! the request path checks before every authenticated call.
//!
//! The token-mint / refresh transport (Ed25519 request-signing vs. an OAuth-style
//! exchange) is not yet finalised in the spec, so this module loads a bearer
//! token from the environment and exposes a single `refresh` seam where the real
//! mint call slots in without touching any call site.

use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use tokio::sync::RwLock;

/// Environment variable names (custodial credentials are never hard-coded).
pub const ENV_ACCESS_TOKEN: &str = "POLYMARKET_US_ACCESS_TOKEN";
pub const ENV_PARTICIPANT_ID: &str = "POLYMARKET_US_PARTICIPANT_ID";
/// Optional override of the token lifetime (seconds); defaults to the spec's 180s.
pub const ENV_TOKEN_TTL_SECS: &str = "POLYMARKET_US_TOKEN_TTL_SECS";

/// Default access-token lifetime per spec §2 ("expire every 3 minutes").
const DEFAULT_TOKEN_TTL_SECS: i64 = 180;
/// Refresh this many seconds *before* expiry so an in-flight request never races
/// a 401. Generous relative to typical round-trip latency.
const REFRESH_SKEW_SECS: i64 = 30;

struct TokenState {
    access_token: String,
    /// Wall-clock instant after which the token must be refreshed.
    expires_at: DateTime<Utc>,
}

/// Auth state for one US retail session.
pub struct UsAuth {
    /// `firms/<FIRM_NAME>/users/<USER_ID>` — sent as the `x-participant-id` header.
    participant_id: String,
    token: RwLock<TokenState>,
    ttl_secs: i64,
    #[allow(dead_code)] // used by the refresh seam (see `refresh`)
    http: Arc<reqwest::Client>,
    #[allow(dead_code)]
    base_url: String,
}

impl UsAuth {
    /// Build auth state from environment credentials.
    pub fn from_env(http: Arc<reqwest::Client>, base_url: String) -> Result<Self> {
        let access_token = std::env::var(ENV_ACCESS_TOKEN)
            .with_context(|| format!("{ENV_ACCESS_TOKEN} not set"))?;
        let participant_id = std::env::var(ENV_PARTICIPANT_ID)
            .with_context(|| format!("{ENV_PARTICIPANT_ID} not set"))?;
        let ttl_secs = std::env::var(ENV_TOKEN_TTL_SECS)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .filter(|s| *s > REFRESH_SKEW_SECS)
            .unwrap_or(DEFAULT_TOKEN_TTL_SECS);

        let token = RwLock::new(TokenState {
            access_token,
            expires_at: Utc::now() + Duration::seconds(ttl_secs),
        });

        Ok(Self { participant_id, token, ttl_secs, http, base_url })
    }

    /// The participant identity header value.
    pub fn participant_id(&self) -> &str {
        &self.participant_id
    }

    /// Return a currently-valid bearer token, refreshing first if it is within
    /// the refresh-skew window of expiry.
    pub async fn bearer(&self) -> Result<String> {
        {
            let guard = self.token.read().await;
            if Utc::now() < guard.expires_at - Duration::seconds(REFRESH_SKEW_SECS) {
                return Ok(guard.access_token.clone());
            }
        }
        self.refresh().await
    }

    /// Force-refresh the access token and return the new value.
    ///
    /// **Refresh transport is not yet wired** (the mint endpoint is undocumented
    /// in the current spec). For now this re-reads the env token and re-stamps
    /// the expiry, which keeps long-running sessions alive when the operator
    /// rotates `POLYMARKET_US_ACCESS_TOKEN` out-of-band. Replace the body with
    /// the real Ed25519/OAuth exchange against `self.base_url` when finalised —
    /// no call site changes.
    pub async fn refresh(&self) -> Result<String> {
        let fresh = std::env::var(ENV_ACCESS_TOKEN)
            .with_context(|| format!("{ENV_ACCESS_TOKEN} not set during refresh"))?;
        let mut guard = self.token.write().await;
        guard.access_token = fresh.clone();
        guard.expires_at = Utc::now() + Duration::seconds(self.ttl_secs);
        Ok(fresh)
    }
}

