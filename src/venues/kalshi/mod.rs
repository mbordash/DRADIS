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
///
/// Skeleton — REST client, discovery, and the Execution impl land next.
#[derive(Debug)]
pub struct KalshiVenue {
    pub(crate) auth: std::sync::Arc<KalshiAuth>,
    pub(crate) http: reqwest::Client,
    pub(crate) base_url: String,
}

impl KalshiVenue {
    /// Construct from environment (key id + private key + demo flag).
    pub fn from_env() -> anyhow::Result<Self> {
        let auth = std::sync::Arc::new(KalshiAuth::from_env()?);
        let base = base_url();
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
        })
    }
}
