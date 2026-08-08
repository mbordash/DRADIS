/// Funding Raptor — Binance perpetual futures funding rate poller.
///
/// Polls `/fapi/v1/premiumIndex` every `BASIS_FUNDING_POLL_SECS` (60s) and
/// broadcasts the current funding rate via a `watch` channel.
///
/// │ Signal        │ Interpretation                                     │
/// │───────────────│────────────────────────────────────────────────────│
/// │ rate < 0      │ Shorts paying longs — bearish smart-money lean     │
/// │ rate > 0      │ Longs paying shorts — bullish smart-money lean     │
/// │ rate = 0      │ Neutral, or Binance FAPI unreachable (geo-block)   │
///
/// Falls back to `dec!(0)` silently if no source is reachable.  Primary URL is
/// `fapi.binance.com`; on failure retries OKX (`BTC-USDT-SWAP` funding rate),
/// which is reachable from US IPs where Binance FAPI is geo-blocked (451).
/// (`fapi.binance.us` does not exist — Binance.US has no futures.)
use std::str::FromStr;
use std::sync::Arc;
use std::collections::HashMap;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::watch;
use tracing::{debug, warn};

use crate::config;
use crate::api::server::AssetRaptorHealth;

pub async fn run_funding_raptor(
    http: Arc<reqwest::Client>,
    crypto_filter: String,
    funding_tx: watch::Sender<Decimal>,
    raptor_health_tx: Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
) {
    let symbol = match crypto_filter.as_str() {
        "eth" => "ETHUSDT",
        "sol" => "SOLUSDT",
        _     => "BTCUSDT",
    };
    let okx_inst = match crypto_filter.as_str() {
        "eth" => "ETH-USDT-SWAP",
        "sol" => "SOL-USDT-SWAP",
        _     => "BTC-USDT-SWAP",
    };
    // Primary: Binance FAPI (futures). Fallback: OKX (US-reachable).
    let url_primary  = format!("https://fapi.binance.com/fapi/v1/premiumIndex?symbol={}", symbol);
    let url_fallback = format!("https://www.okx.com/api/v5/public/funding-rate?instId={}", okx_inst);

    let mut consecutive_failures: u32 = 0;

    loop {
        let result = match try_fetch_funding(&http, &url_primary).await {
            Some(rate) => Some(rate),
            None => try_fetch_funding_okx(&http, &url_fallback).await,
        };

        match result {
            Some(rate) => {
                consecutive_failures = 0;
                let _ = funding_tx.send(rate);
                // Mark funding raptor healthy on successful poll and mirror the
                // latest rate into the shared map for GET /api/telemetry.
                raptor_health_tx.send_modify(|map| {
                    let h = map.entry(crypto_filter.clone()).or_default();
                    h.funding_connected = true;
                    h.funding_rate = rate;
                });
                debug!("📡 Funding Raptor {}: {:.6}%", symbol, rate * dec!(100));
            }
            None => {
                consecutive_failures += 1;
                // Mark funding raptor unhealthy when poll fails.
                raptor_health_tx.send_modify(|map| {
                    map.entry(crypto_filter.clone()).or_default().funding_connected = false;
                });
                // Warn on first failure so the operator knows; degrade to debug after that
                // to avoid flooding logs if the server has a persistent geo-block.
                if consecutive_failures == 1 {
                    warn!("⚠️ Funding Raptor poll failed (will retry silently). Bot continues with rate=0.");
                } else {
                    debug!("📡 Funding Raptor unavailable (attempt {}), using rate=0", consecutive_failures);
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(config::BASIS_FUNDING_POLL_SECS)).await;
    }
}

/// Attempt a single funding rate fetch with a 5s timeout.
async fn try_fetch_funding(http: &reqwest::Client, url: &str) -> Option<Decimal> {
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        http.get(url).send(),
    ).await.ok()?.ok()?;

    let v = resp.json::<serde_json::Value>().await.ok()?;
    let rate_str = v.get("lastFundingRate").and_then(|r| r.as_str())?;
    Decimal::from_str(rate_str).ok()
}

/// OKX fallback: `GET /api/v5/public/funding-rate?instId=…` with a 5s timeout.
/// Response: `{"code":"0","data":[{"fundingRate":"0.0000249",…}]}`.
async fn try_fetch_funding_okx(http: &reqwest::Client, url: &str) -> Option<Decimal> {
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        http.get(url).send(),
    ).await.ok()?.ok()?;

    let v = resp.json::<serde_json::Value>().await.ok()?;
    let rate_str = v.get("data")?.as_array()?.first()?
        .get("fundingRate")?.as_str()?;
    Decimal::from_str(rate_str).ok()
}
