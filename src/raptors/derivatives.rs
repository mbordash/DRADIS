/// Derivatives Raptor — Binance perpetual futures open-interest & taker-flow poller.
///
/// A *macro* Raptor that flies above the spot telemetry. Where the Price Raptor
/// reports the wake of a passing ship (1s/5s velocity) and the Funding Raptor
/// reports smart-money lean, the Derivatives Raptor reports the **structural
/// pressure** building in the perp book that tends to drive the 10-minute
/// regime shifts the Vipers care about.
///
/// Polls two Binance FAPI endpoints every `DERIVATIVES_POLL_SECS` and broadcasts
/// a single `DerivativesSnapshot` via a `watch` channel:
///
/// │ Field         │ Source                          │ Interpretation                         │
/// │───────────────│─────────────────────────────────│────────────────────────────────────────│
/// │ open_interest │ /fapi/v1/openInterest           │ contracts open — raw positioning size  │
/// │ oi_delta_pct  │ Δ vs previous poll (fraction)    │ >0 OI building, <0 OI unwinding         │
/// │ cvd_ratio     │ /futures/data/takerlongshortRatio│ taker buy÷sell vol; >1 buy aggression  │
///
/// Reading the two together is the point — the Viper decides:
///   • OI ↑ + price ↑ + cvd>1  → fresh longs, trend continuation likely
///   • OI ↑ + price ↓ + cvd<1  → fresh shorts, "falling knife" pressure
///   • OI ↓                     → de-leveraging / squeeze, regime exhaustion
///
/// Like the Funding Raptor this degrades silently to its `Default` (all-zero,
/// `cvd_ratio = 0` meaning "no data") when Binance FAPI is unreachable (e.g. a
/// geo-block on the server). The bot keeps running; Vipers treat zero as neutral.
/// Primary host is `fapi.binance.com`; on failure it retries `fapi.binance.us`.
use std::str::FromStr;
use std::sync::Arc;
use std::collections::HashMap;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::watch;
use tracing::{debug, warn};

use crate::config;
use crate::api::server::AssetRaptorHealth;

/// Normalised derivatives-market snapshot broadcast to every consuming Viper.
///
/// `Copy` so the `watch` channel hands out cheap value clones, and `Default`
/// (all-zero) so the channel can be seeded before the first successful poll.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct DerivativesSnapshot {
    /// Current open interest, in base contracts (e.g. BTC). Raw positioning size.
    pub open_interest: Decimal,
    /// Fractional change in open interest since the previous poll
    /// (e.g. `0.01` = +1%). `0` on the first poll or when OI is flat/unknown.
    pub oi_delta_pct: Decimal,
    /// Taker buy ÷ sell volume ratio over the trailing window.
    /// `> 1` → buyers lifting offers (bullish aggression); `< 1` → sellers
    /// hitting bids (bearish aggression); `0` → no data (FAPI unreachable).
    pub cvd_ratio: Decimal,
}

pub async fn run_derivatives_raptor(
    http: Arc<reqwest::Client>,
    crypto_filter: String,
    deriv_tx: watch::Sender<DerivativesSnapshot>,
    raptor_health_tx: Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
) {
    let symbol = match crypto_filter.as_str() {
        "eth" => "ETHUSDT",
        "sol" => "SOLUSDT",
        _     => "BTCUSDT",
    };
    // Open interest — primary host + regional mirror fallback.
    let oi_primary  = format!("https://fapi.binance.com/fapi/v1/openInterest?symbol={}", symbol);
    let oi_fallback = format!("https://fapi.binance.us/fapi/v1/openInterest?symbol={}", symbol);
    // Taker long/short (buy vs sell) volume ratio — perp aggression proxy (CVD).
    let cvd_primary  = format!("https://fapi.binance.com/futures/data/takerlongshortRatio?symbol={}&period=5m&limit=1", symbol);
    let cvd_fallback = format!("https://fapi.binance.us/futures/data/takerlongshortRatio?symbol={}&period=5m&limit=1", symbol);

    let mut prev_oi: Option<Decimal> = None;
    let mut consecutive_failures: u32 = 0;

    loop {
        // Open interest is the regime backbone; CVD is best-effort context.
        let oi = try_fetch_open_interest(&http, &oi_primary).await
            .or(try_fetch_open_interest(&http, &oi_fallback).await);
        let cvd = try_fetch_cvd_ratio(&http, &cvd_primary).await
            .or(try_fetch_cvd_ratio(&http, &cvd_fallback).await)
            .unwrap_or(dec!(0));

        match oi {
            Some(open_interest) => {
                consecutive_failures = 0;
                let oi_delta_pct = match prev_oi {
                    Some(p) if p > dec!(0) => (open_interest - p) / p,
                    _ => dec!(0),
                };
                prev_oi = Some(open_interest);

                let snap = DerivativesSnapshot { open_interest, oi_delta_pct, cvd_ratio: cvd };
                let _ = deriv_tx.send(snap);
                raptor_health_tx.send_modify(|map| {
                    let h = map.entry(crypto_filter.clone()).or_default();
                    h.deriv_connected = true;
                    h.open_interest   = open_interest;
                    h.oi_delta_pct    = oi_delta_pct;
                    h.cvd_ratio       = cvd;
                });
                debug!(
                    "📡 Derivatives Raptor {}: OI={} ΔOI={:.3}% CVD={:.3}",
                    symbol, open_interest, oi_delta_pct * dec!(100), cvd,
                );
            }
            None => {
                consecutive_failures += 1;
                raptor_health_tx.send_modify(|map| {
                    map.entry(crypto_filter.clone()).or_default().deriv_connected = false;
                });
                if consecutive_failures == 1 {
                    warn!("⚠️ Derivatives Raptor poll failed (will retry silently). Vipers treat signal as neutral.");
                } else {
                    debug!("📡 Derivatives Raptor unavailable (attempt {}), using neutral snapshot", consecutive_failures);
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(config::DERIVATIVES_POLL_SECS)).await;
    }
}

/// Fetch current open interest (base contracts) with a 5s timeout.
async fn try_fetch_open_interest(http: &reqwest::Client, url: &str) -> Option<Decimal> {
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        http.get(url).send(),
    ).await.ok()?.ok()?;

    let v = resp.json::<serde_json::Value>().await.ok()?;
    let oi_str = v.get("openInterest").and_then(|r| r.as_str())?;
    Decimal::from_str(oi_str).ok()
}

/// Fetch the latest taker buy/sell volume ratio (perp aggression / CVD proxy).
/// The endpoint returns an array of buckets; we read the most recent one.
async fn try_fetch_cvd_ratio(http: &reqwest::Client, url: &str) -> Option<Decimal> {
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        http.get(url).send(),
    ).await.ok()?.ok()?;

    let v = resp.json::<serde_json::Value>().await.ok()?;
    let latest = v.as_array()?.last()?;
    let ratio_str = latest.get("buySellRatio").and_then(|r| r.as_str())?;
    Decimal::from_str(ratio_str).ok()
}

