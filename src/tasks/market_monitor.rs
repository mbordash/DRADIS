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

/// The shared market state tuple broadcast on the watch channel.
pub type MarketState = (
    U256,                        // yes_token
    U256,                        // no_token
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
        let (candidate, maker_candidate) = get_market_pair(&http).await;
        if candidate.yes_token == U256::ZERO { continue; }

        let (cur_yes, _, cur_name, cur_close_time, _, _, _, _cur_cid) = market_tx.borrow().clone();

        if candidate.yes_token == cur_yes {
            // Hourly market unchanged — still check if maker market changed
            let cur_maker_yes = market_tx.borrow().6.as_ref().map(|m| m.yes_token);
            let new_maker_yes = maker_candidate.as_ref().map(|m| m.yes_token);
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

        let should_switch = cur_secs_left < config::FINAL_EXPIRY_WINDOW_SECS
            || cur_secs_left <= 0
            || time_based_upgrade
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
