use chrono::{DateTime, Utc};
use alloy::primitives::U256;
use tracing::{info, debug, warn, error};
use std::cmp::Ordering;
use std::env;

use crate::config;
use crate::helpers::json::{extract_token_ids_u256, extract_close_time, extract_start_time, get_enable_orderbook};
use crate::helpers::price::value_to_f64;

/// Helper function to generate market names for hourly crypto events
pub async fn fetch_specific_hourly_market(
    http: &reqwest::Client,
    crypto_filter: &str,
    now: DateTime<Utc>,
) -> Option<(Vec<U256>, String, String, f64, bool, Option<DateTime<Utc>>, String)> {
    let candidate_names = crate::helpers::time::generate_hourly_market_names(crypto_filter, now);

    for name_query in candidate_names {
        debug!("Attempting direct search for market: \"{}\"", name_query);
        let url = format!("https://gamma-api.polymarket.com/markets?search={}&active=true&closed=false&limit=1", urlencoding::encode(&name_query));

        let resp = match http.get(&url).send().await {
            Ok(r) => r,
            Err(e) => { warn!("⚠️ Direct market search failed for \"{}\": {}", name_query, e); continue; }
        };
        if !resp.status().is_success() { continue; }

        let data: serde_json::Value = match resp.json().await {
            Ok(d) => d,
            Err(e) => { error!("❌ JSON failed for direct search \"{}\": {}", name_query, e); continue; }
        };

        let markets: Vec<&serde_json::Value> = if let Some(arr) = data.as_array() {
            arr.iter().collect()
        } else if let Some(arr) = data.get("data").and_then(|v| v.as_array()) {
            arr.iter().collect()
        } else {
            continue;
        };

        if let Some(market) = markets.into_iter().next() {
            let name = market.get("question").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let description = market.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let event = market.get("event").unwrap_or(&serde_json::Value::Null);
            let event_title = event.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string();

            // Re-apply essential filters
            if config::is_bad_market(&name) || config::is_bad_market(&event_title) { continue; }
            if config::is_ultra_short_window_market(&name) { continue; }
            if !get_enable_orderbook(market) { continue; }

            let token_ids = extract_token_ids_u256(market);
            if token_ids.len() < 2 { continue; }

            let volume = market.get("volume24hrClob").and_then(value_to_f64)
                .or_else(|| market.get("volume24hr").and_then(value_to_f64))
                .unwrap_or(0.0);

            let close_time = extract_close_time(event, market);
            let start_time = extract_start_time(event, market);
            let seconds_left = close_time.map_or(0i64, |ct| (ct - now).num_seconds());

            if let Some(st) = start_time {
                if now < st {
                    debug!("  ⏭️ Skipping market \"{}\" - hasn't started yet (Starts in {}s)", name, (st - now).num_seconds());
                    continue;
                }
            }

            if seconds_left < config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY { continue; }
            if seconds_left > config::MAX_SECONDS_TO_EXPIRY_FOR_ENTRY { continue; }

            let hot = config::is_high_priority_text(&name) || config::is_high_priority_text(&event_title);
            let link = market.get("slug").and_then(|v| v.as_str()).map(|s| format!("https://polymarket.com/{}", s)).unwrap_or_default();

            return Some((token_ids, name.clone(), link, volume, hot, close_time, description));
        }
    }
    None
}

/// A classified market candidate returned by the scanner.
#[derive(Clone, Debug)]
pub struct MarketCandidate {
    pub yes_token: U256,
    pub no_token: U256,
    pub name: String,
    pub link: String,
    pub description: String,
    pub is_hot: bool,
    pub close_time: Option<DateTime<Utc>>,
    pub volume: f64,
    /// Polymarket condition ID (bytes32 hex) — required for on-chain merge operations.
    pub condition_id: String,
}

/// Market type classification used to route strategies to the right venue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketKind {
    /// Standard hourly "Up or Down" market — ideal for Momentum, Basis, TimeDecay.
    Hourly,
    /// Multi-hour window market (e.g. "4:00PM-8:00PM ET") — ideal for Maker.
    Window,
    /// Daily market (e.g. "Up or Down on April 21?") — Maker fallback when no window exists.
    Daily,
}

impl MarketCandidate {
    pub fn kind(&self) -> MarketKind {
        if config::is_window_market(&self.name) {
            MarketKind::Window
        } else if config::is_daily_market(&self.name) {
            MarketKind::Daily
        } else {
            MarketKind::Hourly
        }
    }
}

/// Returns `(hourly_market, Option<maker_market>)`.
///
/// `hourly_market` is the best active hourly "Up or Down" market for the given crypto.
/// `maker_market` is the best window or daily market if one exists concurrently,
/// giving the Maker strategy a slower-moving venue with wider spreads.
/// Both are `None`/fallback when no valid market is found.
pub async fn get_market_pair(
    http: &reqwest::Client,
) -> (MarketCandidate, Option<MarketCandidate>) {
    let crypto_filter = env::var("CRYPTO_FILTER")
        .unwrap_or_else(|_| "all".to_string())
        .to_lowercase();

    info!("🚀 Scanning Gamma API for markets (FILTER: {})", crypto_filter);
    let now = Utc::now();

    // 1. Try fast-path for the hourly market first
    let hourly_fast = fetch_specific_hourly_market(http, &crypto_filter, now).await
        .map(|m| MarketCandidate {
            yes_token: m.0[0],
            no_token: m.0[1],
            name: m.1,
            link: m.2,
            description: m.6,
            is_hot: m.4,
            close_time: m.5,
            volume: 0.0,
            condition_id: String::new(),
        });

    // 2. Full scan to find all candidates (needed to find window/daily markets)
    let all_candidates = fetch_simplified_crypto_candidates(http, &crypto_filter).await;

    // Separate into buckets
    let mut hourly_candidates: Vec<_> = all_candidates.iter()
        .filter(|c| {
            let kind = if config::is_window_market(&c.1) { MarketKind::Window }
                       else if config::is_daily_market(&c.1) { MarketKind::Daily }
                       else { MarketKind::Hourly };
            kind == MarketKind::Hourly
        })
        .collect();

    let mut maker_candidates: Vec<_> = all_candidates.iter()
        .filter(|c| config::is_window_market(&c.1) || config::is_daily_market(&c.1))
        .collect();

    // Sort hourly by the existing priority logic
    hourly_candidates.sort_by(|a, b| {
        let a_secs = a.5.map_or(9999, |t| (t - now).num_seconds());
        let b_secs = b.5.map_or(9999, |t| (t - now).num_seconds());
        let a_up = a.1.to_lowercase().contains("up or down");
        let b_up = b.1.to_lowercase().contains("up or down");
        if a_up != b_up { return b_up.cmp(&a_up); }
        let a_sweet = a_secs > 1800 && a_secs < 3600;
        let b_sweet = b_secs > 1800 && b_secs < 3600;
        if a_sweet != b_sweet { return b_sweet.cmp(&a_sweet); }
        b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal)
    });

    // Sort maker candidates: window markets preferred over daily; more time left preferred
    maker_candidates.sort_by(|a, b| {
        let a_window = config::is_window_market(&a.1);
        let b_window = config::is_window_market(&b.1);
        if a_window != b_window { return b_window.cmp(&a_window); }
        // More time remaining = better for passive orders
        let a_secs = a.5.map_or(0, |t| (t - now).num_seconds());
        let b_secs = b.5.map_or(0, |t| (t - now).num_seconds());
        b_secs.cmp(&a_secs)
    });

    // Build the hourly result — prefer fast-path, fall back to scanned
    let hourly = if let Some(fast) = hourly_fast {
        fast
    } else if let Some(best) = hourly_candidates.first() {
        info!("🏆 Selected market: \"{}\"", best.1);
        MarketCandidate {
            yes_token: best.0[0],
            no_token: best.0[1],
            name: best.1.clone(),
            link: best.2.clone(),
            description: best.6.clone(),
            is_hot: best.4,
            close_time: best.5,
            volume: best.3,
            condition_id: best.7.clone(),
        }
    } else {
        warn!("⚠️ No valid markets found matching filters.");
        return (MarketCandidate {
            yes_token: U256::ZERO, no_token: U256::ZERO,
            name: String::new(), link: String::new(), description: String::new(),
            is_hot: false, close_time: None, volume: 0.0,
            condition_id: String::new(),
        }, None);
    };

    // Build the maker market result (window or daily, if available)
    let maker_market = maker_candidates.first().map(|best| {
        let kind = if config::is_window_market(&best.1) { "Window" } else { "Daily" };
        info!("🏦 Maker {} market selected: \"{}\"", kind, best.1);
        MarketCandidate {
            yes_token: best.0[0],
            no_token: best.0[1],
            name: best.1.clone(),
            link: best.2.clone(),
            description: best.6.clone(),
            is_hot: best.4,
            close_time: best.5,
            volume: best.3,
            condition_id: best.7.clone(),
        }
    });

    (hourly, maker_market)
}

/// Backwards-compatible wrapper — returns the hourly market only.
/// Existing call sites that don't need the maker market can continue using this.
pub async fn get_top_market(http: &reqwest::Client) -> (U256, U256, String, String, String, bool, Option<DateTime<Utc>>) {
    let crypto_filter = env::var("CRYPTO_FILTER")
        .unwrap_or_else(|_| "all".to_string())
        .to_lowercase();

    info!("🚀 Scanning Gamma API for markets (FILTER: {})", crypto_filter);
    let now = Utc::now();

    // 1. Try specifically targeted hourly markets first (Fastest)
    if let Some(market) = fetch_specific_hourly_market(http, &crypto_filter, now).await {
        info!("🏆 Selected market: \"{}\"", market.1);
        return (market.0[0], market.0[1], market.1, market.2, market.6, market.4, market.5);
    }

    // 2. Fallback to general scan
    let candidates = fetch_simplified_crypto_candidates(http, &crypto_filter).await;

    if candidates.is_empty() {
        warn!("⚠️ No valid markets found matching filters.");
        return (U256::ZERO, U256::ZERO, String::new(), String::new(), String::new(), false, None);
    }

    let mut sorted = candidates;
    sorted.sort_by(|a, b| {
        let a_secs = a.5.map_or(9999, |t| (t - now).num_seconds());
        let b_secs = b.5.map_or(9999, |t| (t - now).num_seconds());
        let a_up_or_down = a.1.to_lowercase().contains("up or down");
        let b_up_or_down = b.1.to_lowercase().contains("up or down");
        if a_up_or_down != b_up_or_down { return b_up_or_down.cmp(&a_up_or_down); }
        let a_range = config::is_range_market(&a.1);
        let b_range = config::is_range_market(&b.1);
        if a_range != b_range { return a_range.cmp(&b_range); }
        let a_sweet = a_secs > 1800 && a_secs < 3600;
        let b_sweet = b_secs > 1800 && b_secs < 3600;
        if a_sweet != b_sweet { return b_sweet.cmp(&a_sweet); }
        b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal)
    });

    let best = &sorted[0];
    info!("🏆 Selected market: \"{}\"", best.1);
    (best.0[0], best.0[1], best.1.clone(), best.2.clone(), best.6.clone(), best.4, best.5)
}

/// Fetch candidate markets that meet basic filters
pub async fn fetch_simplified_crypto_candidates(
    http: &reqwest::Client,
    crypto_filter: &str,
) -> Vec<(Vec<U256>, String, String, f64, bool, Option<DateTime<Utc>>, String, String)> {
    let mut out = vec![];
    let now = Utc::now();
    let mut total_scanned = 0;

    for page in 0..config::GAMMA_API_MARKET_SCAN_PAGES {
        let offset = page * 100;
        let url = format!(
            "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=100&offset={}&order=volume24hrClob&ascending=false&include=event",
            offset
        );

        let resp = match http.get(&url).send().await {
            Ok(r) => r,
            Err(e) => { warn!("⚠️ Markets page {} failed: {}", page, e); continue; }
        };
        if !resp.status().is_success() { break; }

        let data: serde_json::Value = match resp.json().await {
            Ok(d) => d,
            Err(e) => { error!("❌ JSON failed: {}", e); continue; }
        };

        let markets: Vec<&serde_json::Value> = if let Some(arr) = data.as_array() {
            arr.iter().collect()
        } else if let Some(arr) = data.get("data").and_then(|v| v.as_array()) {
            arr.iter().collect()
        } else {
            break;
        };

        total_scanned += markets.len();

        for market in markets {
            let name = market.get("question").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let event = market.get("event").unwrap_or(&serde_json::Value::Null);
            let event_title = event.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string();

            debug!("🔍 Evaluating candidate: \"{}\" (Event: \"{}\")", name, event_title);

            // Use market validator module for comprehensive validation
            let token_ids = extract_token_ids_u256(market);
            let close_time = extract_close_time(event, market);
            let start_time = extract_start_time(event, market);
            let volume = market.get("volume24hrClob").and_then(value_to_f64)
                .or_else(|| market.get("volume24hr").and_then(value_to_f64))
                .unwrap_or(0.0);

            // Create validation context
            let validation_ctx = crate::market_validator::ValidationContext {
                now,
                crypto_filter: crypto_filter.to_string(),
                min_seconds_to_expiry: config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY,
                max_seconds_to_expiry: config::MAX_SECONDS_TO_EXPIRY_FOR_ENTRY,
                safety_buffer_secs: config::MARKET_EXPIRY_SAFETY_BUFFER_SECS,
                min_volume: config::MIN_MARKET_VOLUME,
            };

            // Define blocked keywords
            let blocked_keywords = vec![
                "presidential", "nomination", "election",
                "democratic", "republican",
                "masters", "tournament", "spieth", "jordan",
                "5-minute", "5 minute", "5m",
                "2026", "finals", "cup", "stanley"
            ];

            // Comprehensive market validation
            let (is_valid, status, msg) = crate::market_validator::validate_market(
                &name,
                &event_title,
                &token_ids,
                close_time,
                volume,
                &validation_ctx,
                &blocked_keywords
            );

            if !is_valid {
                debug!("  ⏭️ Rejected ({}): {}", status, msg);
                continue;
            }

            // Hard-exclude range/price-band markets (e.g. "between $X and $Y").
            // These are often already settled and unsuitable for directional strategies.
            // Previously these were only deprioritised in the sort — now we exclude them.
            if config::is_range_market(&name) {
                debug!("  ⏭️ Rejected (range market): {}", name);
                continue;
            }

            // Hard-exclude ultra-short-window markets (e.g. "6:00PM-6:15PM ET").
            // These 15-minute windows reprice too fast for reliable entry/exit.
            if config::is_ultra_short_window_market(&name) {
                debug!("  ⏭️ Rejected (ultra-short window): {}", name);
                continue;
            }

            // Check if market has started
            if let Some(st) = start_time {
                if now < st {
                    debug!("  ⏭️ Skipping candidate \"{}\" - hasn't started yet (Starts in {}s)", name, (st - now).num_seconds());
                    continue;
                }
            }

            // Check orderbook availability
            if !get_enable_orderbook(market) {
                debug!("  ⏭️ Rejected: No orderbook available");
                continue;
            }

            let link = market.get("slug").and_then(|v| v.as_str()).map(|s| format!("https://polymarket.com/{}", s)).unwrap_or_else(|| "https://polymarket.com/".to_string());
            let description = market.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let condition_id = market.get("conditionId").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let hot = config::is_high_priority_text(&name) || config::is_high_priority_text(&event_title);

            debug!("  ✅ Valid market passed all checks");
            out.push((token_ids, name.clone(), link, volume, hot, close_time, description, condition_id));
        }
    }
    info!("✅ Total scanned: {} | Candidates after filters: {}", total_scanned, out.len());
    out
}
