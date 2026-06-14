/// Background task: market switch monitor.
///
/// Polls the Gamma API every 90 seconds and broadcasts a new market tuple
/// via the watch channel whenever the active hourly or maker market changes.
/// `main.rs` breaks its inner trading loop when it sees the channel updated.
use std::sync::Arc;

use alloy::primitives::U256;
use chrono::Utc;
use rust_decimal::Decimal;
use tokio::sync::watch;
use tracing::info;

use crate::config;
use crate::helpers::market::{get_market_pair, MarketCandidate};
use crate::helpers::time::{fetch_historical_strike_price, fetch_strike_price_from_close_time};
use crate::venues::core::MarketId;
use crate::venues::intl::market_id_from_u256;

/// Hard cap on how long a single `get_market_pair` scan may run.
///
/// `fetch_simplified_crypto_candidates` pages through GAMMA_API_MARKET_SCAN_PAGES (currently 30)
/// Gamma API pages sequentially, each with a 20-second reqwest timeout.  In the worst
/// case (all 30 pages stall until timeout) the scan takes 600 s.  Without an outer cap
/// the market_monitor task can go silent for up to 10 minutes, missing market switches.
///
/// 90 s = 1 full 90-second monitor interval.  If the scan hasn't finished in 90 s
/// something is badly wrong with the Gamma API; log a warning and retry next tick.
const MARKET_SCAN_TIMEOUT_SECS: u64 = 90;

/// The shared market state tuple broadcast on the watch channel.
pub type MarketState = (
    MarketId,                    // yes_token (venue-neutral)
    MarketId,                    // no_token  (venue-neutral)
    String,                      // market_name
    Option<chrono::DateTime<Utc>>, // market_close_time
    Option<Decimal>,             // strike_price
    String,                      // description
    Option<MarketCandidate>,     // maker_market_candidate
    String,                      // condition_id (NEW)
);

pub async fn run_market_monitor(
    http: Arc<reqwest::Client>,
    crypto_filter: String,
    market_tx: watch::Sender<MarketState>,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(90));
    loop {
        interval.tick().await;
        // Hard cap on market scan — see MARKET_SCAN_TIMEOUT_SECS comment above.
        let scan_result = tokio::time::timeout(
            std::time::Duration::from_secs(MARKET_SCAN_TIMEOUT_SECS),
            get_market_pair(&http, &crypto_filter),
        ).await;
        let (candidate, maker_candidate) = match scan_result {
            Ok(pair) => pair,
            Err(_) => {
                tracing::warn!(
                    "⚠️ Market monitor: get_market_pair timed out after {}s — skipping this poll cycle",
                    MARKET_SCAN_TIMEOUT_SECS
                );
                continue;
            }
        };
        if candidate.yes_token == market_id_from_u256(U256::ZERO) { continue; }

        let (cur_yes, _, cur_name, cur_close_time, _, _, _, _cur_cid) = market_tx.borrow().clone();

        if candidate.yes_token == cur_yes {
            // Hourly market unchanged — still check if maker market changed
            let cur_maker_yes = market_tx.borrow().6.as_ref().map(|m| m.yes_token.clone());
            let new_maker_yes = maker_candidate.as_ref().map(|m| m.yes_token.clone());
            if cur_maker_yes != new_maker_yes {
                if let Some(ref mk) = maker_candidate {
                    info!("🏦 Maker market updated: \"{}\"", mk.name);
                }
                let (y, n, nm, ct, sp, ds, _, cid) = market_tx.borrow().clone();
                let _ = market_tx.send((y, n, nm, ct, sp, ds, maker_candidate, cid));
            }
            continue;
        }

        let now_ts = Utc::now();
        let cur_secs_left = cur_close_time.map_or(9999i64, |ct| (ct - now_ts).num_seconds());
        let new_secs_left = candidate.close_time.map_or(9999i64, |ct| (ct - now_ts).num_seconds());

        let candidate_is_binary = candidate.name.to_lowercase().contains("up or down");
        let current_is_binary = cur_name.to_lowercase().contains("up or down");
        let candidate_is_range = config::is_range_market(&candidate.name);

        let time_based_upgrade = new_secs_left > cur_secs_left + 1800
            && !(current_is_binary && !candidate_is_binary);

        // Detect the daily-as-substitute case: we're running on a long-lived daily/window market
        // (used as a fallback during bootstrap when no hourly was published yet) and a real hourly
        // market has now appeared.  Force an upgrade so MomentumStrategy and other hourly-venue
        // strategies can participate.  Without this, the time_based_upgrade check would NEVER fire
        // because the daily's secs_left >> hourly's secs_left, and the bot would stay on the daily
        // for the entire hour even after the 12PM-ET (or any other) hourly market is listed.
        let current_is_daily_sub = config::is_daily_market(&cur_name) || config::is_window_market(&cur_name);
        let candidate_is_hourly = !config::is_daily_market(&candidate.name)
            && !config::is_window_market(&candidate.name)
            && !config::is_ultra_short_window_market(&candidate.name);
        let daily_to_hourly_upgrade = current_is_daily_sub && candidate_is_hourly && new_secs_left > 600;

        let should_switch = cur_secs_left < config::FINAL_EXPIRY_WINDOW_SECS
            || cur_secs_left <= 0
            || time_based_upgrade
            || daily_to_hourly_upgrade
            || (candidate_is_binary && !current_is_binary && !candidate_is_range
                && new_secs_left > 600 && cur_secs_left > 300);

        if !should_switch { continue; }

        info!("🔄 Market Switch Detected: {} -> {}", cur_name, candidate.name);
        let mut strike = crate::helpers::market::extract_strike_price(&candidate.name);
        if strike.is_none() {
            strike = fetch_historical_strike_price(&http, &crypto_filter, &candidate.description).await;
        }
        if strike.is_none() {
            strike = fetch_historical_strike_price(&http, &crypto_filter, &candidate.name).await;
        }
        if strike.is_none() {
            strike = fetch_strike_price_from_close_time(&http, &crypto_filter, candidate.close_time).await;
        }
        let _ = market_tx.send((
            candidate.yes_token, candidate.no_token,
            candidate.name.clone(), candidate.close_time,
            strike, candidate.description.clone(),
            maker_candidate,
            candidate.condition_id.clone(),
        ));
    }
}
