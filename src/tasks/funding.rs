/// Background task: Binance perpetual futures funding rate poller.
///
/// Polls /fapi/v1/premiumIndex every BASIS_FUNDING_POLL_SECS (60s).
/// Negative rate = shorts paying longs (bearish smart money).
/// Positive rate = longs paying shorts (bullish smart money).
/// Falls back to dec!(0) silently if Binance fapi is unreachable (e.g. geo-block on server).
use std::str::FromStr;
use std::sync::Arc;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::watch;
use tracing::{debug, warn};

use crate::config;

pub async fn run_funding_poller(
    http: Arc<reqwest::Client>,
    crypto_filter: String,
    funding_tx: watch::Sender<Decimal>,
) {
    let symbol = match crypto_filter.as_str() {
        "eth" => "ETHUSDT",
        "sol" => "SOLUSDT",
        _     => "BTCUSDT",
    };
    // Primary: Binance FAPI (futures). Fallback: Binance FAPI regional mirror.
    let url_primary  = format!("https://fapi.binance.com/fapi/v1/premiumIndex?symbol={}", symbol);
    let url_fallback = format!("https://fapi.binance.us/fapi/v1/premiumIndex?symbol={}", symbol);

    let mut consecutive_failures: u32 = 0;

    loop {
        let result = try_fetch_funding(&http, &url_primary).await
            .or(try_fetch_funding(&http, &url_fallback).await);

        match result {
            Some(rate) => {
                consecutive_failures = 0;
                let _ = funding_tx.send(rate);
                debug!("📡 Funding rate {}: {:.6}%", symbol, rate * dec!(100));
            }
            None => {
                consecutive_failures += 1;
                // Warn on first failure so the operator knows; degrade to debug after that
                // to avoid flooding logs if the server has a persistent geo-block.
                if consecutive_failures == 1 {
                    warn!("⚠️ Funding rate poll failed (will retry silently). Bot continues with rate=0.");
                } else {
                    debug!("📡 Funding rate poll unavailable (attempt {}), using rate=0", consecutive_failures);
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
