//! Custodial Web2 authentication for the Polymarket US retail venue.
//!
//! The US gateway uses an **asymmetric challenge-response** scheme rather than
//! `intl_clob`'s EIP-712 wallet signatures: an Ed25519 key pair bound to the
//! developer profile signs a fresh millisecond nonce, and the gateway returns a
//! short-lived JWT (180s) plus a refresh token (spec: `docs/us_retail_api.md` §2
//! + live-API update).
//!
//! Mint flow (`POST /v1/auth/mint`):
//!   1. `nonce` = current Unix-millis timestamp (string).
//!   2. `signature` = Base64( Ed25519_sign(private_key, nonce_bytes) ).
//!   3. Gateway checks the nonce is within ±5s of exchange clock + verifies the
//!      signature against the public key on file → issues `{ access_token,
//!      refresh_token }`.
//!
//! The token is held behind an `RwLock` and re-minted automatically when within
//! the refresh-skew window of expiry, so call sites only ever call [`UsAuth::bearer`].

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use base64::Engine as _;
use chrono::{DateTime, Duration, Utc};
use ed25519_dalek::{Signer, SigningKey};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::debug;

/// Environment variable names (custodial credentials are never hard-coded).
pub const ENV_PARTICIPANT_ID: &str = "POLYMARKET_US_PARTICIPANT_ID";
/// Base64 of the 32-byte Ed25519 private seed bound to the developer profile.
pub const ENV_ED25519_SEED: &str = "POLYMARKET_US_ED25519_PRIVATE_KEY";

/// Default access-token lifetime per spec §2 (JWT, 180s).
const DEFAULT_TOKEN_TTL_SECS: i64 = 180;
/// Re-mint this many seconds *before* expiry so an in-flight request never races
/// a 401.
const REFRESH_SKEW_SECS: i64 = 30;
/// Mint endpoint path.
const MINT_PATH: &str = "/v1/auth/mint";

#[derive(Serialize)]
struct MintRequest<'a> {
    participant_id: &'a str,
    /// Unix-millis timestamp string used as the signed challenge nonce.
    nonce: &'a str,
    /// Base64 Ed25519 signature over `nonce`.
    signature: String,
}

#[derive(Deserialize)]
struct MintResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    /// Optional explicit lifetime (seconds); falls back to the 180s default.
    #[serde(default)]
    expires_in: Option<i64>,
}

struct TokenState {
    access_token: String,
    #[allow(dead_code)] // retained for a future dedicated rotation endpoint
    refresh_token: Option<String>,
    /// Wall-clock instant after which the token must be re-minted.
    expires_at: DateTime<Utc>,
}

/// Auth state for one US retail session.
pub struct UsAuth {
    /// `firms/<FIRM_NAME>/users/<USER_ID>` — sent as the `x-participant-id` header.
    participant_id: String,
    signing_key: SigningKey,
    http: Arc<reqwest::Client>,
    base_url: String,
    token: RwLock<Option<TokenState>>,
}

impl UsAuth {
    /// Build auth state from environment credentials. Does **not** mint yet — the
    /// first [`bearer`](Self::bearer) (or an explicit [`mint`](Self::mint)) does.
    pub fn from_env(http: Arc<reqwest::Client>, base_url: String) -> Result<Self> {
        let participant_id = std::env::var(ENV_PARTICIPANT_ID)
            .with_context(|| format!("{ENV_PARTICIPANT_ID} not set"))?;
        let seed_b64 = std::env::var(ENV_ED25519_SEED)
            .with_context(|| format!("{ENV_ED25519_SEED} not set"))?;

        let seed = base64::engine::general_purpose::STANDARD
            .decode(seed_b64.trim())
            .context("POLYMARKET_US_ED25519_PRIVATE_KEY is not valid Base64")?;
        let seed: [u8; 32] = seed
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("Ed25519 private seed must decode to exactly 32 bytes"))?;
        let signing_key = SigningKey::from_bytes(&seed);

        Ok(Self {
            participant_id,
            signing_key,
            http,
            base_url,
            token: RwLock::new(None),
        })
    }

    /// The participant identity header value.
    pub fn participant_id(&self) -> &str {
        &self.participant_id
    }

    /// Return a currently-valid bearer token, minting first if absent or within
    /// the refresh-skew window of expiry.
    pub async fn bearer(&self) -> Result<String> {
        {
            let guard = self.token.read().await;
            if let Some(state) = guard.as_ref() {
                if Utc::now() < state.expires_at - Duration::seconds(REFRESH_SKEW_SECS) {
                    return Ok(state.access_token.clone());
                }
            }
        }
        self.mint().await
    }

    /// Perform the Ed25519 challenge-response mint and cache the resulting JWT.
    pub async fn mint(&self) -> Result<String> {
        let nonce = Utc::now().timestamp_millis().to_string();
        let signature = base64::engine::general_purpose::STANDARD
            .encode(self.signing_key.sign(nonce.as_bytes()).to_bytes());

        let body = MintRequest {
            participant_id: &self.participant_id,
            nonce: &nonce,
            signature,
        };

        let resp = self
            .http
            .post(format!("{}{}", self.base_url, MINT_PATH))
            .json(&body)
            .send()
            .await
            .context("auth mint request failed")?;
        let status = resp.status();
        if !status.is_success() {
            let err = resp.text().await.unwrap_or_default();
            return Err(anyhow!("US retail auth mint rejected (HTTP {status}): {err}"));
        }
        let minted: MintResponse = resp.json().await.context("auth mint decode failed")?;

        let ttl = minted.expires_in.filter(|s| *s > 0).unwrap_or(DEFAULT_TOKEN_TTL_SECS);
        let access_token = minted.access_token.clone();
        *self.token.write().await = Some(TokenState {
            access_token: minted.access_token,
            refresh_token: minted.refresh_token,
            expires_at: Utc::now() + Duration::seconds(ttl),
        });
        debug!("US retail: minted access token (ttl={ttl}s)");
        Ok(access_token)
    }
}


