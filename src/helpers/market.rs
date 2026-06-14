/// Market Helper Module
///
/// Handles market discovery from the Gamma API, classification (Hourly/Window/Daily),
/// and comprehensive validation logic.

use chrono::{DateTime, Utc};
use alloy::primitives::U256;
use tracing::{info, debug, warn};
use std::cmp::Ordering;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use rust_decimal::Decimal;
use regex::Regex;
use std::str::FromStr;

/// Epoch-seconds timestamp of the last "no maker venue" log, used to debounce a message
/// that fires every 90 s but is fully expected when no daily market exists for the asset.
static LAST_NO_MAKER_VENUE_LOG: AtomicU64 = AtomicU64::new(0);

use crate::config; // Keep this for constants
use crate::helpers::json::{extract_token_ids_u256, extract_close_time, get_enable_orderbook};
use crate::helpers::price::value_to_f64;
use crate::venues::core::MarketId;
use crate::venues::intl::market_id_from_u256;
// Import the moved functions from config_helpers via crate::helpers
use crate::helpers::{
    is_window_market, is_daily_market, is_ultra_short_window_market,
    is_high_priority_text, is_range_market, is_bad_market
};

// ============================================================================
// MARKET VALIDATION TYPES
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketValidationStatus {
    Valid,
    NoTokenIds,
    NoOrderbook,
    Expired,
    ExpiringSoon,
    NotStarted,
    OutsideTimeWindow,
    WrongCrypto,
    NoStrike,
    InsufficientLiquidity,
    Blocked,
}

impl std::fmt::Display for MarketValidationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarketValidationStatus::Valid => write!(f, "Valid"),
            MarketValidationStatus::NoTokenIds => write!(f, "NoTokenIds"),
            MarketValidationStatus::NoOrderbook => write!(f, "NoOrderbook"),
            MarketValidationStatus::Expired => write!(f, "Expired"),
            MarketValidationStatus::ExpiringSoon => write!(f, "ExpiringSoon"),
            MarketValidationStatus::NotStarted => write!(f, "NotStarted"),
            MarketValidationStatus::OutsideTimeWindow => write!(f, "OutsideTimeWindow"),
            MarketValidationStatus::WrongCrypto => write!(f, "WrongCrypto"),
            MarketValidationStatus::NoStrike => write!(f, "NoStrike"),
            MarketValidationStatus::InsufficientLiquidity => write!(f, "InsufficientLiquidity"),
            MarketValidationStatus::Blocked => write!(f, "Blocked"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ValidationContext {
    pub now: DateTime<Utc>,
    pub crypto_filter: String,
    pub min_seconds_to_expiry: i64,
    pub max_seconds_to_expiry: i64,
    pub safety_buffer_secs: i64,
    pub min_volume: f64,
}

// ============================================================================
// VALIDATION LOGIC
// ============================================================================

pub fn extract_strike_price(market_name: &str) -> Option<Decimal> {
    let lower_name = market_name.to_lowercase();
    let re1 = Regex::new(r"(?:\$|above\s|below\s|at\s)(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d{3,}(?:\.\d+)?)").unwrap();
    if let Some(cap) = re1.captures(&lower_name) {
        if let Some(num_str) = cap.get(1) {
            let cleaned = num_str.as_str().replace(",", "");
            if let Ok(price) = Decimal::from_str(&cleaned) {
                if price > Decimal::from(100) { return Some(price); }
            }
        }
    }
    let re2 = Regex::new(r"\[(?:BTC|ETH|SOL)?\s*(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d{3,}(?:\.\d+)?)\]").unwrap();
    if let Some(cap) = re2.captures(&lower_name) {
        if let Some(num_str) = cap.get(1) {
            let cleaned = num_str.as_str().replace(",", "");
            if let Ok(price) = Decimal::from_str(&cleaned) {
                if price > Decimal::from(100) { return Some(price); }
            }
        }
    }
    let re3 = Regex::new(r"\bat\s+(\d+(?:\.\d+)?)(?:\s|$)").unwrap();
    if let Some(cap) = re3.captures(&lower_name) {
        if let Some(num_str) = cap.get(1) {
            if let Ok(price) = Decimal::from_str(num_str.as_str()) {
                if price > Decimal::from(100) { return Some(price); }
            }
        }
    }
    None
}

pub fn has_valid_strike_or_binary(market_name: &str) -> bool {
    if extract_strike_price(market_name).is_some() { return true; }
    let lower = market_name.to_lowercase();
    if lower.contains("up or down") { return true; }
    false
}

pub fn validate_expiry(
    close_time: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    min_secs: i64,
    max_secs: i64,
    buffer: i64,
) -> (MarketValidationStatus, i64) {
    match close_time {
        None => (MarketValidationStatus::Valid, 0),
        Some(ct) => {
            let left = (ct - now).num_seconds();
            if left < 0 { (MarketValidationStatus::Expired, 0) }
            else if left < buffer { (MarketValidationStatus::ExpiringSoon, left) }
            else if left < min_secs || left > max_secs { (MarketValidationStatus::OutsideTimeWindow, left) }
            else { (MarketValidationStatus::Valid, left) }
        }
    }
}

pub fn validate_time_window(market_name: &str) -> bool {
    // Use the imported functions directly
    if is_window_market(market_name) || is_daily_market(market_name) {
        return true;
    }

    let lower = market_name.to_lowercase();
    let has_et = lower.contains(" et");
    let has_time = lower.contains(":") && (lower.contains("am") || lower.contains("pm"));
    if has_et && has_time {
        if lower.contains("12:00pm") || lower.contains("am-12:") || lower.contains("pm-12:") { return false; }
        return true;
    }
    lower.contains("hour") || lower.contains("et")
}

pub fn validate_market(
    market_name: &str,
    event_title: &str,
    token_ids: &[impl core::fmt::Debug],
    close_time: Option<DateTime<Utc>>,
    volume: f64,
    ctx: &ValidationContext,
) -> (bool, MarketValidationStatus, String) {
    let combined = format!("{} {}", market_name, event_title).to_lowercase();
    if is_bad_market(&combined) { return (false, MarketValidationStatus::Blocked, format!("Blocked: '{}'", combined)); }

    let lower = market_name.to_lowercase();
    let match_crypto = match ctx.crypto_filter.as_str() {
        "btc" | "bitcoin" => lower.contains("bitcoin") || lower.contains("btc"),
        "eth" | "ethereum" => lower.contains("ethereum") || lower.contains("eth"),
        "sol" | "solana" => lower.contains("solana") || lower.contains("sol"),
        _ => true,
    };
    if !match_crypto { return (false, MarketValidationStatus::WrongCrypto, "Crypto mismatch".to_string()); }

    if token_ids.len() < 2 { return (false, MarketValidationStatus::NoTokenIds, "Missing tokens".to_string()); }
    if !validate_time_window(market_name) { return (false, MarketValidationStatus::OutsideTimeWindow, "Not short-term".to_string()); }

    let (exp_status, left) = validate_expiry(close_time, ctx.now, ctx.min_seconds_to_expiry, ctx.max_seconds_to_expiry, ctx.safety_buffer_secs);
    if exp_status != MarketValidationStatus::Valid { return (false, exp_status, format!("Expiry fail: {}s left", left)); }

    if volume < ctx.min_volume { return (false, MarketValidationStatus::InsufficientLiquidity, "Low volume".to_string()); }
    if !has_valid_strike_or_binary(market_name) { return (false, MarketValidationStatus::NoStrike, "No strike".to_string()); }

    (true, MarketValidationStatus::Valid, "Valid".to_string())
}

// ============================================================================
// API FETCHING & CLASSIFICATION
// ============================================================================

#[derive(Clone, Debug)]
pub struct MarketCandidate {
    pub yes_token: MarketId,
    pub no_token: MarketId,
    pub name: String,
    pub link: String,
    pub description: String,
    pub is_hot: bool,
    pub close_time: Option<DateTime<Utc>>,
    pub volume: f64,
    pub condition_id: String,
    pub strike_price: Option<Decimal>, // Added strike_price field
}

/// Directly fetch today's (or tomorrow's) daily "Up or Down on [date]?" maker venue
/// by constructing the deterministic Polymarket event slug and querying the events endpoint.
///
/// The Gamma API `search=` parameter uses fuzzy full-text matching that returns completely
/// unrelated results for date-based queries.  The events endpoint with an exact slug is
/// reliable and low-latency.  Slug format: `bitcoin-up-or-down-on-april-29-2026`
pub async fn fetch_specific_window_daily_market(
    http: &reqwest::Client,
    crypto_filter: &str,
    now: DateTime<Utc>,
) -> Option<MarketCandidate> {
    let slugs = crate::helpers::time::generate_daily_event_slugs(crypto_filter, now);
    for slug in &slugs {
        let url = format!(
            "https://gamma-api.polymarket.com/events?slug={}&active=true&closed=false",
            slug
        );
        let resp = match http.get(&url).send().await {
            Ok(r) => r,
            Err(e) => { warn!("⚠️ Daily event slug fetch failed for '{}': {}", slug, e); continue; }
        };
        let data: serde_json::Value = match resp.json().await { Ok(d) => d, Err(_) => continue };
        let events = data.as_array().or_else(|| data.get("data").and_then(|v| v.as_array()));
        let event = match events.and_then(|a| a.first()) { Some(e) => e, None => continue };

        let markets_arr = match event.get("markets").and_then(|v| v.as_array()) {
            Some(a) if !a.is_empty() => a,
            _ => continue,
        };

        for m in markets_arr {
            let name = m.get("question").and_then(|v| v.as_str()).unwrap_or_default().to_string();
            // Use the imported function directly
            if is_bad_market(&name) || !get_enable_orderbook(m) { continue; }
            let tokens = extract_token_ids_u256(m);
            if tokens.len() < 2 { continue; }
            // Use the event's endDate as authoritative close time (market-level endDate is identical)
            let close = extract_close_time(event, m);
            let left = close.map_or(0, |ct| (ct - now).num_seconds());
            if left < config::MAKER_MIN_SECS_TO_EXPIRY || left > config::MAKER_MAX_SECS_TO_EXPIRY {
                debug!("⏭ Daily market '{}' skipped: {}s left (need {}-{})", name, left, config::MAKER_MIN_SECS_TO_EXPIRY, config::MAKER_MAX_SECS_TO_EXPIRY);
                continue;
            }
            let cond_id = m.get("conditionId").and_then(|v| v.as_str()).unwrap_or_default().to_string();
            info!("🗓 Found daily maker venue via slug '{}': \"{}\" ({}s left)", slug, name, left);
            return Some(MarketCandidate {
                yes_token: market_id_from_u256(tokens[0]),
                no_token: market_id_from_u256(tokens[1]),
                name,
                link: m.get("slug").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                description: m.get("description").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                is_hot: false,
                close_time: close,
                volume: 0.0,
                condition_id: cond_id,
                strike_price: None, // Initialize strike_price to None
            });
        }
    }
    debug!("No daily maker venue found via slug lookup (tried: {:?})", slugs);
    None
}

pub async fn get_market_pair(http: &reqwest::Client, asset_filter: &str) -> (MarketCandidate, Option<MarketCandidate>) {
    // Use the per-squadron asset filter passed in. Fall back to CRYPTO_FILTER env var
    // only if the passed filter is empty (backward-compat for single-asset mode).
    let filter = if !asset_filter.is_empty() {
        asset_filter.to_lowercase()
    } else {
        env::var("CRYPTO_FILTER").unwrap_or_else(|_| "all".to_string()).to_lowercase()
    };
    let now = Utc::now();

    // Primary scan: volume-sorted (good for established markets with accumulated volume).
    // Secondary scan: createdAt-sorted (finds fresh hourly markets that have zero 24h volume
    // and therefore rank below the bottom of the volume-sorted pages).
    // Merging both ensures we never miss the current-hour market regardless of its age.
    let (all, recent) = tokio::join!(
        fetch_simplified_crypto_candidates(http, &filter),
        fetch_recent_crypto_candidates(http, &filter),
    );

    // Deduplicate by conditionId — prefer recent entry if conditionId matches, since it carries
    // the validated time-window context.
    let mut seen_cids: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut merged: Vec<_> = all.iter().collect();
    for entry in &recent {
        let cid = &entry.7;
        if cid.is_empty() || seen_cids.insert(cid.clone()) {
            // Only add from recent if not already in volume scan
            if !all.iter().any(|a| !a.7.is_empty() && a.7 == *cid) {
                merged.push(entry);
            }
        }
    }
    // Also populate seen_cids from all
    for entry in &all { seen_cids.insert(entry.7.clone()); }

    let mut hourly_c: Vec<_> = merged.iter().filter(|c|
        // Use the imported functions directly
        !is_window_market(&c.1)
            && !is_daily_market(&c.1)
            && !is_ultra_short_window_market(&c.1)
    ).collect();
    // Use the imported functions directly
    let mut maker_c: Vec<_> = merged.iter().filter(|c| is_window_market(&c.1) || is_daily_market(&c.1)).collect();

    // Sort hourly by:
    //   1. Binary "Up or Down" markets first (is_high_priority_text, field .4)
    //      Guarantees a freshly-published "Up or Down" beats any low-volume strike market.
    //      Today's log: bot ran 18 min on "Bitcoin above 83,800 (vol=15)" because the
    //      9PM "Up or Down" had just appeared with vol=0 and ranked below it on pure volume.
    //   2. Volume desc (high-volume markets are liquidity-safe)
    //   3. Time left desc as tiebreak
    hourly_c.sort_by(|a, b| {
        b.4.cmp(&a.4)
            .then_with(|| b.3.partial_cmp(&a.3).unwrap_or(Ordering::Equal))
            .then_with(|| b.5.cmp(&a.5))
    });
    maker_c.sort_by(|a, b| b.5.cmp(&a.5)); // prefer more time left

    // Soft vol24h floor: prefer hourly markets that meet the minimum volume threshold.
    // If none qualify (e.g. a brand-new market with zero 24h vol at session open),
    // fall back to the full sorted list so the bot doesn't sit idle.
    let vol_floor = config::MIN_HOURLY_MARKET_VOL24H;
    let high_vol_hourly: Vec<_> = hourly_c.iter().filter(|c| c.3 >= vol_floor).cloned().collect();
    let hourly_final = if !high_vol_hourly.is_empty() { &high_vol_hourly } else { &hourly_c };

    let hourly = hourly_final.first()
        .map(|b| MarketCandidate { yes_token: market_id_from_u256(b.0[0]), no_token: market_id_from_u256(b.0[1]), name: b.1.clone(), link: b.2.clone(), description: b.6.clone(), is_hot: b.4, close_time: b.5, volume: b.3, condition_id: b.7.clone(), strike_price: None })
        .unwrap_or(MarketCandidate { yes_token: market_id_from_u256(U256::ZERO), no_token: market_id_from_u256(U256::ZERO), name: String::new(), link: String::new(), description: String::new(), is_hot: false, close_time: None, volume: 0.0, condition_id: String::new(), strike_price: None });

    if hourly.yes_token != market_id_from_u256(U256::ZERO) {
        info!("📈 Hourly market selected: \"{}\" (vol24h={:.0})", hourly.name, hourly.volume);
    }

    // Prefer a direct targeted search for today's daily market (high confidence, volume-independent),
    // falling back to whatever the volume-scan turned up.
    let daily_direct = fetch_specific_window_daily_market(http, &filter, now).await;
    let maker = daily_direct.or_else(|| {
        if let Some(b) = maker_c.first() {
            info!("📋 Using volume-scan window/daily fallback for maker venue");
            Some(MarketCandidate { yes_token: market_id_from_u256(b.0[0]), no_token: market_id_from_u256(b.0[1]), name: b.1.clone(), link: b.2.clone(), description: b.6.clone(), is_hot: b.4, close_time: b.5, volume: b.3, condition_id: b.7.clone(), strike_price: None })
        } else {
            // No daily/window maker venue — expected for assets (e.g. BTC) where Polymarket
            // only lists hourly "Up or Down" markets.  Log at INFO at most once per hour to
            // avoid log spam; the bot operates normally on the hourly market instead.
            let now_secs = now.timestamp() as u64;
            let last = LAST_NO_MAKER_VENUE_LOG.load(AtomicOrdering::Relaxed);
            if now_secs.saturating_sub(last) >= 3600 {
                LAST_NO_MAKER_VENUE_LOG.store(now_secs, AtomicOrdering::Relaxed);
                info!("ℹ️ No window/daily maker venue available for [{}] — operating on hourly market only", filter.to_uppercase());
            }
            None
        }
    });

    (hourly, maker)
}

/// Fetch the most recently *created* active crypto markets.
///
/// A fresh hourly market (e.g. "Bitcoin Up or Down - April 29, 11AM ET") is published
/// minutes before the hour starts.  It has zero 24h volume and therefore falls completely
/// outside the volume-sorted scan used by `fetch_simplified_crypto_candidates`.
///
/// Sorting by `createdAt desc` guarantees the newest markets appear on page 1, so a single
/// 100-market request reliably surfaces the current hour's market regardless of volume.
pub async fn fetch_recent_crypto_candidates(http: &reqwest::Client, filter: &str) -> Vec<(Vec<U256>, String, String, f64, bool, Option<DateTime<Utc>>, String, String)> {
    let mut out = vec![];
    let now = Utc::now();
    let url = "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=100&order=createdAt&ascending=false&include=event";
    let resp = match http.get(url).send().await { Ok(r) => r, Err(_) => return out };
    let data: serde_json::Value = match resp.json().await { Ok(d) => d, Err(_) => return out };
    let markets = data.as_array().or_else(|| data.get("data").and_then(|v| v.as_array()));
    if let Some(arr) = markets {
        for m in arr {
            let name = m.get("question").and_then(|v| v.as_str()).unwrap_or_default().to_string();
            let event = m.get("event").unwrap_or(&serde_json::Value::Null);
            let tokens = extract_token_ids_u256(m);
            let close = extract_close_time(event, m);
            let vol = m.get("volume24hrClob").and_then(value_to_f64).unwrap_or(0.0);
            // Use the imported functions directly
            let is_maker_venue = is_window_market(&name) || is_daily_market(&name);
            let min_secs = if is_maker_venue { config::MAKER_MIN_SECS_TO_EXPIRY } else { config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY };
            let max_secs = if is_maker_venue { config::MAKER_MAX_SECS_TO_EXPIRY } else { config::MAX_SECONDS_TO_EXPIRY_FOR_ENTRY };
            let ctx = ValidationContext {
                now,
                crypto_filter: filter.to_string(),
                min_seconds_to_expiry: min_secs,
                max_seconds_to_expiry: max_secs,
                safety_buffer_secs: config::MARKET_EXPIRY_SAFETY_BUFFER_SECS,
                min_volume: 0.0, // allow zero-volume fresh markets
            };
            let event_title = event.get("title").and_then(|v| v.as_str()).unwrap_or_default();
            // Removed `blocked` argument from `validate_market` call
            let (valid, _, _) = validate_market(&name, event_title, &tokens, close, vol, &ctx);
            if valid && !is_range_market(&name) && !is_ultra_short_window_market(&name) && get_enable_orderbook(m) {
                out.push((tokens, name.clone(), m.get("slug").and_then(|v| v.as_str()).unwrap_or_default().to_string(), vol, is_high_priority_text(&name), close, m.get("description").and_then(|v| v.as_str()).unwrap_or_default().to_string(), m.get("conditionId").and_then(|v| v.as_str()).unwrap_or_default().to_string()));
            }
        }
    }
    out
}

pub async fn fetch_simplified_crypto_candidates(http: &reqwest::Client, filter: &str) -> Vec<(Vec<U256>, String, String, f64, bool, Option<DateTime<Utc>>, String, String)> {
    let mut out = vec![];
    let now = Utc::now();
    for page in 0..config::GAMMA_API_MARKET_SCAN_PAGES {
        let url = format!("https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=100&offset={}&order=volume24hrClob&ascending=false&include=event", page * 100);
        let resp = match http.get(&url).send().await { Ok(r) => r, Err(_) => continue };
        let data: serde_json::Value = match resp.json().await { Ok(d) => d, Err(_) => break };
        let markets = data.as_array().or_else(|| data.get("data").and_then(|v| v.as_array()));
        if let Some(arr) = markets {
            for m in arr {
                let name = m.get("question").and_then(|v| v.as_str()).unwrap_or_default().to_string();
                let event = m.get("event").unwrap_or(&serde_json::Value::Null);
                let tokens = extract_token_ids_u256(m);
                let close = extract_close_time(event, m);
                let vol = m.get("volume24hrClob").and_then(value_to_f64).unwrap_or(0.0);
                // Use the imported functions directly
                let is_maker_venue = is_window_market(&name) || is_daily_market(&name);
                let min_secs = if is_maker_venue { config::MAKER_MIN_SECS_TO_EXPIRY } else { config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY };
                let max_secs = if is_maker_venue { config::MAKER_MAX_SECS_TO_EXPIRY } else { config::MAX_SECONDS_TO_EXPIRY_FOR_ENTRY };
                let ctx = ValidationContext {
                    now,
                    crypto_filter: filter.to_string(),
                    min_seconds_to_expiry: min_secs,
                    max_seconds_to_expiry: max_secs,
                    safety_buffer_secs: config::MARKET_EXPIRY_SAFETY_BUFFER_SECS,
                    min_volume: config::MIN_MARKET_VOLUME,
                };
                let event_title = event.get("title").and_then(|v| v.as_str()).unwrap_or_default();
                // Removed `blocked` argument from `validate_market` call
                let (valid, _, _) = validate_market(&name, event_title, &tokens, close, vol, &ctx);
                if valid && !is_range_market(&name) && !is_ultra_short_window_market(&name) && get_enable_orderbook(m) {
                    out.push((tokens, name.clone(), m.get("slug").and_then(|v| v.as_str()).unwrap_or_default().to_string(), vol, is_high_priority_text(&name), close, m.get("description").and_then(|v| v.as_str()).unwrap_or_default().to_string(), m.get("conditionId").and_then(|v| v.as_str()).unwrap_or_default().to_string()));
                }
            }
        }
    }
    out
}
