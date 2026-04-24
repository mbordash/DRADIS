/// Background task: Binance perpetual futures funding rate poller.
///
/// Polls /fapi/v1/premiumIndex every BASIS_FUNDING_POLL_SECS (60s).
/// Negative rate = shorts paying longs (bearish smart money).
/// Positive rate = longs paying shorts (bullish smart money).
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
    let url = format!("https://fapi.binance.com/fapi/v1/premiumIndex?symbol={}", symbol);

    loop {
        match http.get(&url).send().await {
            Ok(resp) => {
                if let Ok(v) = resp.json::<serde_json::Value>().await {
                    if let Some(rate_str) = v.get("lastFundingRate").and_then(|r| r.as_str()) {
                        if let Ok(rate) = Decimal::from_str(rate_str) {
                            let _ = funding_tx.send(rate);
                            debug!("📡 Funding rate {}: {:.6}%", symbol, rate * dec!(100));
                        }
                    }
                }
            }
            Err(e) => warn!("⚠️ Funding rate poll failed: {}", e),
        }
        tokio::time::sleep(std::time::Duration::from_secs(config::BASIS_FUNDING_POLL_SECS)).await;
    }
}
