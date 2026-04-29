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

use crate::config;
use crate::helpers::json::{extract_token_ids_u256, extract_close_time, get_enable_orderbook};
use crate::helpers::price::value_to_f64;

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
    if config::is_window_market(market_name) || config::is_daily_market(market_name) {
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
    blocked: &[&str],
) -> (bool, MarketValidationStatus, String) {
    let combined = format!("{} {}", market_name, event_title).to_lowercase();
    for kw in blocked { if combined.contains(&kw.to_lowercase()) { return (false, MarketValidationStatus::Blocked, format!("Blocked: '{}'", kw)); } }

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
    pub yes_token: U256,
    pub no_token: U256,
    pub name: String,
    pub link: String,
    pub description: String,
    pub is_hot: bool,
    pub close_time: Option<DateTime<Utc>>,
    pub volume: f64,
    pub condition_id: String,
}

pub async fn fetch_specific_hourly_market(
    http: &reqwest::Client,
    crypto_filter: &str,
    now: DateTime<Utc>,
) -> Option<(Vec<U256>, String, String, f64, bool, Option<DateTime<Utc>>, String)> {
    let candidate_names = crate::helpers::time::generate_hourly_market_names(crypto_filter, now);
    for q in candidate_names {
        let url = format!("https://gamma-api.polymarket.com/markets?search={}&active=true&closed=false&limit=1", urlencoding::encode(&q));
        let resp = match http.get(&url).send().await { Ok(r) => r, Err(_) => continue };
        let data: serde_json::Value = match resp.json().await { Ok(d) => d, Err(_) => continue };
        let markets = data.as_array().or_else(|| data.get("data").and_then(|v| v.as_array()));
        if let Some(m) = markets.and_then(|a| a.first()) {
            let name = m.get("question").and_then(|v| v.as_str()).unwrap_or_default().to_string();
            if config::is_bad_market(&name) || config::is_ultra_short_window_market(&name) || !get_enable_orderbook(m) { continue; }
            let tokens = extract_token_ids_u256(m);
            if tokens.len() < 2 { continue; }
            let close = extract_close_time(m.get("event").unwrap_or(&serde_json::Value::Null), m);
            let left = close.map_or(0, |ct| (ct - now).num_seconds());
            if left < config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY || left > config::MAX_SECONDS_TO_EXPIRY_FOR_ENTRY { continue; }
            return Some((tokens, name, m.get("slug").and_then(|v| v.as_str()).unwrap_or_default().to_string(), 0.0, true, close, m.get("description").and_then(|v| v.as_str()).unwrap_or_default().to_string()));
        }
    }
    None
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
            if config::is_bad_market(&name) || !get_enable_orderbook(m) { continue; }
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
                yes_token: tokens[0],
                no_token: tokens[1],
                name,
                link: m.get("slug").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                description: m.get("description").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                is_hot: false,
                close_time: close,
                volume: 0.0,
                condition_id: cond_id,
            });
        }
    }
    debug!("No daily maker venue found via slug lookup (tried: {:?})", slugs);
    None
}

pub async fn get_market_pair(http: &reqwest::Client) -> (MarketCandidate, Option<MarketCandidate>) {
    let filter = env::var("CRYPTO_FILTER").unwrap_or_else(|_| "all".to_string()).to_lowercase();
    let now = Utc::now();
    let hourly_fast = fetch_specific_hourly_market(http, &filter, now).await.map(|m| MarketCandidate { yes_token: m.0[0], no_token: m.0[1], name: m.1, link: m.2, description: m.6, is_hot: m.4, close_time: m.5, volume: 0.0, condition_id: String::new() });
    let all = fetch_simplified_crypto_candidates(http, &filter).await;
    let mut hourly_c: Vec<_> = all.iter().filter(|c|
        !config::is_window_market(&c.1)
            && !config::is_daily_market(&c.1)
            && !config::is_ultra_short_window_market(&c.1)
    ).collect();
    let mut maker_c: Vec<_> = all.iter().filter(|c| config::is_window_market(&c.1) || config::is_daily_market(&c.1)).collect();

    hourly_c.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap_or(Ordering::Equal));
    maker_c.sort_by(|a, b| b.5.cmp(&a.5)); // prefer more time left

    let hourly = hourly_fast.or_else(|| hourly_c.first().map(|b| MarketCandidate { yes_token: b.0[0], no_token: b.0[1], name: b.1.clone(), link: b.2.clone(), description: b.6.clone(), is_hot: b.4, close_time: b.5, volume: b.3, condition_id: b.7.clone() }))
        .unwrap_or(MarketCandidate { yes_token: U256::ZERO, no_token: U256::ZERO, name: String::new(), link: String::new(), description: String::new(), is_hot: false, close_time: None, volume: 0.0, condition_id: String::new() });

    // Prefer a direct targeted search for today's daily market (high confidence, volume-independent),
    // falling back to whatever the volume-scan turned up.
    let daily_direct = fetch_specific_window_daily_market(http, &filter, now).await;
    let maker = daily_direct.or_else(|| {
        if let Some(b) = maker_c.first() {
            info!("📋 Using volume-scan window/daily fallback for maker venue");
            Some(MarketCandidate { yes_token: b.0[0], no_token: b.0[1], name: b.1.clone(), link: b.2.clone(), description: b.6.clone(), is_hot: b.4, close_time: b.5, volume: b.3, condition_id: b.7.clone() })
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
                let is_maker_venue = config::is_window_market(&name) || config::is_daily_market(&name);
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
                let blocked = vec!["presidential", "nomination", "election", "democratic", "republican"];
                let event_title = event.get("title").and_then(|v| v.as_str()).unwrap_or_default();
                let (valid, _, _) = validate_market(&name, event_title, &tokens, close, vol, &ctx, &blocked);
                if valid && !config::is_range_market(&name) && !config::is_ultra_short_window_market(&name) && get_enable_orderbook(m) {
                    out.push((tokens, name.clone(), m.get("slug").and_then(|v| v.as_str()).unwrap_or_default().to_string(), vol, config::is_high_priority_text(&name), close, m.get("description").and_then(|v| v.as_str()).unwrap_or_default().to_string(), m.get("conditionId").and_then(|v| v.as_str()).unwrap_or_default().to_string()));
                }
            }
        }
    }
    out
}
