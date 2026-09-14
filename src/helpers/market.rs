// SPDX-License-Identifier: AGPL-3.0-only
//
// DRADIS — autonomous trading engine for crypto prediction markets.
// Copyright (C) 2026 Michael Bordash
//
// This file is part of DRADIS. DRADIS is free software: you can redistribute it
// and/or modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.
//
// DRADIS is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
// A PARTICULAR PURPOSE. See the GNU Affero General Public License for details.
//
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

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

// `extract_strike_price` moved to `helpers::time` (venue-neutral — the US
// crypto wing needs it without the intl_clob feature); re-exported here so
// existing intl call sites keep working.
pub use crate::helpers::time::extract_strike_price;

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

/// One validated Gamma market in the shape the selection chain consumes:
/// `(token_ids, question, slug, vol24h, is_high_priority, close_time, description, condition_id)`.
pub type GammaCandidate = (Vec<U256>, String, String, f64, bool, Option<DateTime<Utc>>, String, String);

/// How many windows past the current one the slug lookup probes.
///
/// Not a knob: rotation is structurally satisfied by the next hour. The monitor
/// moves off the current market once it has less than
/// `MIN_SECONDS_TO_EXPIRY_FOR_ENTRY` left, and the next window opens within
/// minutes of that, so the market it needs is always the one after the current.
/// Polymarket creates hourly markets two days ahead (the 2026-09-13 4PM ET
/// market carries `createdAt` 2026-09-11T20:00:00Z), so the next slug resolves
/// long before it is needed.
const HOURLY_SLUG_LOOKAHEAD_HOURS: i64 = 1;

/// The assets that have hourly "Up or Down" markets, as discovery filters.
/// `"all"` (the single-asset `CRYPTO_FILTER` fallback) probes every one.
fn hourly_assets_for(filter: &str) -> Vec<&'static str> {
    match filter {
        "btc" | "bitcoin" => vec!["btc"],
        "eth" | "ethereum" => vec!["eth"],
        "sol" | "solana" => vec!["sol"],
        _ => vec!["btc", "eth", "sol"],
    }
}

/// Parse and validate one Gamma market object into a candidate.
///
/// The single path every discovery source goes through, so a market found by
/// slug is held to exactly the validation the listing scans apply: crypto
/// filter, blocked names, two token ids, short-term name shape, expiry window
/// (maker or hourly bounds by market class), `min_volume`, strike or binary,
/// range and ultra-short exclusions, and an enabled order book. `include=event`
/// on the listing endpoints embeds the parent as `event`; the slug endpoint
/// embeds it as `events[]`. Either is accepted.
pub fn candidate_from_gamma_market(
    m: &serde_json::Value,
    filter: &str,
    now: DateTime<Utc>,
    min_volume: f64,
) -> Option<GammaCandidate> {
    let null = serde_json::Value::Null;
    let name = m.get("question").and_then(|v| v.as_str()).unwrap_or_default().to_string();
    let event = m.get("event")
        .or_else(|| m.get("events").and_then(|v| v.as_array()).and_then(|a| a.first()))
        .unwrap_or(&null);
    let tokens = extract_token_ids_u256(m);
    let close = extract_close_time(event, m);
    let vol = m.get("volume24hrClob").and_then(value_to_f64).unwrap_or(0.0);
    let is_maker_venue = is_window_market(&name) || is_daily_market(&name);
    let min_secs = if is_maker_venue { config::MAKER_MIN_SECS_TO_EXPIRY } else { config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY };
    let max_secs = if is_maker_venue { config::MAKER_MAX_SECS_TO_EXPIRY } else { config::MAX_SECONDS_TO_EXPIRY_FOR_ENTRY };
    let ctx = ValidationContext {
        now,
        crypto_filter: filter.to_string(),
        min_seconds_to_expiry: min_secs,
        max_seconds_to_expiry: max_secs,
        safety_buffer_secs: config::MARKET_EXPIRY_SAFETY_BUFFER_SECS,
        min_volume,
    };
    let event_title = event.get("title").and_then(|v| v.as_str()).unwrap_or_default();
    let (valid, _, _) = validate_market(&name, event_title, &tokens, close, vol, &ctx);
    if !(valid && !is_range_market(&name) && !is_ultra_short_window_market(&name) && get_enable_orderbook(m)) {
        return None;
    }
    let hot = is_high_priority_text(&name);
    Some((
        tokens,
        name,
        m.get("slug").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
        vol,
        hot,
        close,
        m.get("description").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
        m.get("conditionId").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
    ))
}

/// The market array of a Gamma `/markets` response, or empty when the body is
/// not one (Gamma answers an over-large offset with an error object, not an
/// array).
fn gamma_market_array(data: &serde_json::Value) -> Vec<serde_json::Value> {
    data.as_array().cloned()
        .or_else(|| data.get("data").and_then(|v| v.as_array()).cloned())
        .unwrap_or_default()
}

/// A candidate whose name is an hourly market (not window, daily, or ultra-short).
pub fn is_hourly_candidate(c: &GammaCandidate) -> bool {
    !is_window_market(&c.1) && !is_daily_market(&c.1) && !is_ultra_short_window_market(&c.1)
}

/// A candidate whose name is a maker venue (window or daily market).
pub fn is_maker_venue_candidate(c: &GammaCandidate) -> bool {
    is_window_market(&c.1) || is_daily_market(&c.1)
}

/// Merge discovery sources into one list, first source wins on a shared
/// condition id. Candidates with no condition id are kept from every source.
pub fn merge_candidates(sources: Vec<Vec<GammaCandidate>>) -> Vec<GammaCandidate> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut merged = Vec::new();
    for source in sources {
        for c in source {
            if c.7.is_empty() || seen.insert(c.7.clone()) {
                merged.push(c);
            }
        }
    }
    merged
}

/// The hourly market to trade, from the merged candidates. Pure.
///
/// Sort:
///   1. Binary "Up or Down" markets first (`is_high_priority_text`, field .4).
///      Guarantees a freshly-published "Up or Down" beats any low-volume strike
///      market. One log had the bot run 18 min on "Bitcoin above 83,800 (vol=15)"
///      because the 9PM "Up or Down" had just appeared with vol=0 and ranked
///      below it on pure volume.
///   2. Volume desc (high-volume markets are liquidity-safe).
///   3. Time left desc as tiebreak.
///
/// Then the three-tier chain:
///   - Prefer hourly markets at or above `vol_floor` (`MIN_HOURLY_MARKET_VOL24H`).
///   - Else any candidate with SOME volume, or a high-priority "Up or Down" at
///     zero. Those legitimately start at vol24h=0: they are published fresh
///     each hour and quoted immediately. A *strike* market at zero is a
///     different animal: nobody has traded it and nobody is quoting it, so it
///     has no order book to receive. On 2026-08-25 the fallback landed on
///     "Bitcoin above 76,600 on August 25, 7PM ET?" (vol24h=0) one rotation
///     after a market doing 26k, and the feed went dark for twenty minutes
///     with every strategy pointed at an empty book.
///   - Else NOTHING. This used to fall through to the unfiltered list, which
///     is the branch that actually bites: in the gap between hourly markets
///     every remaining strike market has zero volume and no "Up or Down" has
///     opened yet. Ireland, 2026-08-27: the 5PM "Up or Down" (vol24h=14877)
///     closed, the very next selection took "Bitcoin above 78,000 on August
///     27, 7PM ET?" at vol24h=0, re-picked it every ~90s, and left the feed
///     dark for 26 minutes. An empty selection is a supported state: the
///     caller checks for it and the squadron sits idle until the next hourly
///     market opens, which is the truthful state rather than a dead market
///     dressed up as one.
///
/// Before any of that, only the soonest-closing "Up or Down" market competes;
/// a later hour's is dropped. The slug lookup returns the current hour and the
/// next, both high priority, and the next hour could otherwise win on volume
/// or on the time-left tiebreak, whereupon the market monitor's
/// `time_based_upgrade` switches to it and the hour that has just opened goes
/// untraded. Production, 2026-09-13 22:00:16 ET: the 10PM market (vol24h 0 at
/// its last selection) lost to the 11PM market (vol24h 0) on the tiebreak.
/// 2026-09-14 01:00:16 ET: the 1AM market lost to the 2AM market on volume,
/// $8 against $4. FairValue priced only the daily venue for both hours. A
/// current hour inside `MIN_SECONDS_TO_EXPIRY_FOR_ENTRY` already fails
/// validation, so the next hour still takes over when it should.
pub fn pick_hourly_candidate(merged: &[GammaCandidate], vol_floor: f64) -> Option<GammaCandidate> {
    let mut hourly: Vec<&GammaCandidate> = merged.iter().filter(|c| is_hourly_candidate(c)).collect();
    if let Some(soonest) = hourly.iter().filter(|c| c.4).filter_map(|c| c.5).min() {
        hourly.retain(|c| !c.4 || c.5.map_or(true, |close| close == soonest));
    }
    hourly.sort_by(|a, b| {
        b.4.cmp(&a.4)
            .then_with(|| b.3.partial_cmp(&a.3).unwrap_or(Ordering::Equal))
            .then_with(|| b.5.cmp(&a.5))
    });
    hourly.iter().find(|c| c.3 >= vol_floor)
        .or_else(|| hourly.iter().find(|c| c.3 > 0.0 || c.4))
        .map(|c| (*c).clone())
}

fn to_market_candidate(b: &GammaCandidate) -> MarketCandidate {
    MarketCandidate {
        yes_token: market_id_from_u256(b.0[0]),
        no_token: market_id_from_u256(b.0[1]),
        name: b.1.clone(),
        link: b.2.clone(),
        description: b.6.clone(),
        is_hot: b.4,
        close_time: b.5,
        volume: b.3,
        condition_id: b.7.clone(),
        strike_price: None,
    }
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

    // Three sources, merged in this order of authority:
    //   1. Slug lookup: the current hour's "Up or Down" market and the next,
    //      addressed directly. Volume-independent and independent of how many
    //      markets Gamma lists. This is the source that finds the hourly market.
    //   2. Volume-sorted scan: established markets with accumulated volume
    //      (strike markets, window and daily maker venues).
    //   3. createdAt-sorted scan: whatever is newest. It used to be how fresh
    //      hourly markets were found, until Polymarket's catalogue outgrew it;
    //      see `fetch_recent_crypto_candidates`.
    let (by_slug, all, recent) = tokio::join!(
        fetch_hourly_candidates_by_slug(http, &filter, now),
        fetch_simplified_crypto_candidates(http, &filter),
        fetch_recent_crypto_candidates(http, &filter),
    );
    let merged = merge_candidates(vec![by_slug, all, recent]);

    let hourly_count = merged.iter().filter(|c| is_hourly_candidate(c)).count();
    // Prefer more time left; on a tie the earlier-listed candidate, as the
    // stable descending sort this replaces did.
    let maker_c = merged.iter()
        .filter(|c| is_maker_venue_candidate(c))
        .min_by(|a, b| b.5.cmp(&a.5));

    let hourly = pick_hourly_candidate(&merged, config::MIN_HOURLY_MARKET_VOL24H)
        .map(|b| to_market_candidate(&b))
        .unwrap_or(MarketCandidate { yes_token: market_id_from_u256(U256::ZERO), no_token: market_id_from_u256(U256::ZERO), name: String::new(), link: String::new(), description: String::new(), is_hot: false, close_time: None, volume: 0.0, condition_id: String::new(), strike_price: None });

    if hourly.yes_token == market_id_from_u256(U256::ZERO) && hourly_count > 0 {
        info!(
            "⏳ No tradeable hourly market right now ({} candidate(s), all zero-volume strike \
             markets) — waiting for the next one rather than quoting into an empty book",
            hourly_count,
        );
    }
    if hourly.yes_token != market_id_from_u256(U256::ZERO) {
        info!("📈 Hourly market selected: \"{}\" (vol24h={:.0})", hourly.name, hourly.volume);
    }

    // Prefer a direct targeted search for today's daily market (high confidence, volume-independent),
    // falling back to whatever the volume-scan turned up.
    let daily_direct = fetch_specific_window_daily_market(http, &filter, now).await;
    let maker = daily_direct.or_else(|| {
        if let Some(b) = maker_c {
            info!("📋 Using volume-scan window/daily fallback for maker venue");
            Some(to_market_candidate(b))
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

/// The current hour's "Up or Down" market and the next, fetched by slug.
///
/// Production, 2026-09-13 16:00 ET: the BTC squadron released the expired 3PM
/// market with "no replacement available" and every restart for the next
/// quarter hour logged "No active hourly or maker market found", while Gamma
/// listed the 4PM and 5PM markets as active and accepting orders. Neither
/// listing scan could reach them. The createdAt scan's 100 newest markets all
/// fell inside a 20-second span (Polymarket now creates markets faster than
/// that, and creates hourly markets two days ahead, so the current hour is
/// never among the newest). The volume scan's 2100 reachable markets (Gamma
/// caps `offset` at 2000) bottomed out at $1770 of 24h volume; the 4PM market
/// had $1361 and the 5PM $11. Earlier hours that day were found only because
/// they happened to clear that floor.
///
/// The slug is deterministic (`helpers::time::hourly_market_slug`), and one
/// `markets?slug=` request per window answers regardless of listing volume or
/// order. Each hit goes through `candidate_from_gamma_market` with a zero
/// volume floor, so it is validated exactly as a scanned market would be.
pub async fn fetch_hourly_candidates_by_slug(
    http: &reqwest::Client,
    filter: &str,
    now: DateTime<Utc>,
) -> Vec<GammaCandidate> {
    let slugs: Vec<String> = hourly_assets_for(filter)
        .into_iter()
        .flat_map(|asset| crate::helpers::time::generate_hourly_market_slugs(asset, now, HOURLY_SLUG_LOOKAHEAD_HOURS))
        .collect();
    let mut out = Vec::new();
    for slug in &slugs {
        let url = format!("https://gamma-api.polymarket.com/markets?slug={}", slug);
        let resp = match http.get(&url).send().await {
            Ok(r) => r,
            Err(e) => { warn!("⚠️ Hourly slug fetch failed for '{}': {}", slug, e); continue; }
        };
        let data: serde_json::Value = match resp.json().await {
            Ok(d) => d,
            Err(e) => { warn!("⚠️ Hourly slug response unreadable for '{}': {}", slug, e); continue; }
        };
        let markets = gamma_market_array(&data);
        if markets.is_empty() {
            debug!("Hourly slug '{}' is not listed (yet)", slug);
            continue;
        }
        for m in &markets {
            match candidate_from_gamma_market(m, filter, now, 0.0) {
                Some(c) => {
                    debug!("🎯 Hourly slug '{}' → \"{}\" (vol24h={:.0})", slug, c.1, c.3);
                    out.push(c);
                }
                None => debug!("Hourly slug '{}' listed but failed validation", slug),
            }
        }
    }
    out
}

/// Fetch the most recently *created* active crypto markets.
///
/// Kept as a supplementary source, and no longer the way hourly markets are
/// found. Its premise was that a fresh hourly market is published minutes
/// before the hour and so sits among the 100 newest markets. On 2026-09-13 the
/// 100 newest were all created inside a 20-second span and the current hour's
/// market had been created two days earlier. See
/// `fetch_hourly_candidates_by_slug` for the source that replaced it.
pub async fn fetch_recent_crypto_candidates(http: &reqwest::Client, filter: &str) -> Vec<GammaCandidate> {
    let now = Utc::now();
    let url = "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=100&order=createdAt&ascending=false&include=event";
    let resp = match http.get(url).send().await { Ok(r) => r, Err(_) => return Vec::new() };
    let data: serde_json::Value = match resp.json().await { Ok(d) => d, Err(_) => return Vec::new() };
    gamma_market_array(&data).iter()
        .filter_map(|m| candidate_from_gamma_market(m, filter, now, 0.0)) // allow zero-volume fresh markets
        .collect()
}

/// Fetch active crypto markets in descending 24h volume, page by page.
///
/// Finds established markets: strike markets and the window and daily maker
/// venues. Fresh hourly markets rank below the reachable floor; they come from
/// `fetch_hourly_candidates_by_slug`. Paging stops at the first empty or
/// non-array page: Gamma rejects `offset` beyond 2000 with an error object, so
/// with `GAMMA_API_MARKET_SCAN_PAGES` above 21 the remaining requests were
/// answering nothing on every scan.
pub async fn fetch_simplified_crypto_candidates(http: &reqwest::Client, filter: &str) -> Vec<GammaCandidate> {
    let mut out = vec![];
    let now = Utc::now();
    for page in 0..config::GAMMA_API_MARKET_SCAN_PAGES {
        let url = format!("https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=100&offset={}&order=volume24hrClob&ascending=false&include=event", page * 100);
        let resp = match http.get(&url).send().await { Ok(r) => r, Err(_) => continue };
        let data: serde_json::Value = match resp.json().await { Ok(d) => d, Err(_) => break };
        let markets = gamma_market_array(&data);
        if markets.is_empty() { break; }
        out.extend(markets.iter().filter_map(|m| candidate_from_gamma_market(m, filter, now, config::MIN_MARKET_VOLUME)));
    }
    out
}

// The three-state settlement answer lives in `venues::core` so the Kalshi and
// Polymarket US sweeps (compiled without this intl-only module) share the same
// type and the same collapsing-states discipline. Re-exported here because this
// module's `resolution_for_token` is the intl authority that produces it.
pub use crate::venues::core::TokenResolution;

/// Final resolved price for ONE CLOB token.
///
/// Keyed by token rather than by condition, because
/// `open_positions` rows carry a token id and no condition id, and the
/// settlement path needs an answer for a position whose market has already resolved
/// and redeemed — at which point the chain no longer reports it and there is nothing
/// left to look the condition up from.
///
/// `closed=true` is required for the same reason as the sibling: Gamma's default
/// filter excludes closed markets, so the settled markets this exists to price are
/// exactly the ones it would otherwise omit. `clob_token_ids=` is the plural form;
/// the singular is silently ignored and returns the unfiltered market list.
///
/// # Why a decisive price is required
///
/// Gamma's `closed` flag can flip when TRADING ends, ahead of oracle resolution, and
/// in that window `outcomePrices` may still carry a mid or a 0.5/0.5 placeholder. The
/// settlement booking downstream snaps at 0.5, so a YES position last marked 0.49 in a
/// market that ultimately resolves YES would be booked as a total loss — and the row is
/// deleted, so nothing can correct it afterwards. A price that is not within a cent of
/// $0 or $1 is therefore reported as `Unknown` and retried, not treated as final.
///
/// # Why "not closed" is corroborated against the market's schedule
///
/// The `closed` flag also lags in the OPPOSITE direction, and trusting the lagging
/// answer books real money wrong. Production, 2026-09-01 (trades row 16): a FairValue
/// NO leg held to settlement on "Bitcoin Up or Down - September 1, 8PM ET" was
/// auto-redeemed on-chain minutes after the 21:00 EDT end; Gamma's own record carries
/// `closedTime` 21:11:21 EDT, yet at 21:15:21 this query still answered "not closed" —
/// so the sweep concluded the token "left the wallet by a trade" and booked a
/// mark-priced ChainReconcile exit at $0.9995 instead of settlement at $1.00, then
/// deleted the row, leaving the estimate as the permanent record and the genuine
/// settlement booking suppressed forever by the double-book guard.
///
/// So a "not closed" reading is only believed for a market still inside its scheduled
/// life. Past `endDate`, "not closed" cannot mean "still trading" — it means the
/// resolution is not visible YET (UMA pending, or Gamma's flag/CDN lagging the chain),
/// and the truthful answer is `Unknown`, which the sweep defers (bounded by
/// `SETTLEMENT_DEFER_MAX_SECS`) until Gamma prices the market decisively.
pub async fn resolution_for_token(
    http: &reqwest::Client,
    token_id: &str,
) -> TokenResolution {
    use rust_decimal::Decimal;
    use std::str::FromStr;

    // Anything that fails below is genuinely unknown rather than open.
    macro_rules! unknown {
        ($e:expr) => { match $e { Some(v) => v, None => return TokenResolution::Unknown } };
    }

    let url = format!(
        "https://gamma-api.polymarket.com/markets?clob_token_ids={}&closed=true",
        token_id
    );
    let resp = unknown!(http.get(&url).send().await.ok());
    let data: serde_json::Value = unknown!(resp.json().await.ok());
    let arr = unknown!(data.as_array().cloned()
        .or_else(|| data.get("data").and_then(|v| v.as_array()).cloned()));

    // An EMPTY result under `closed=true` is Gamma saying "this market is not
    // closed" — the filter excluded it. Before 2026-09-01 that was trusted
    // outright; now it is corroborated against the market's own schedule (see
    // the doc above), because during the resolution race this very answer came
    // back four minutes AFTER Gamma's recorded closedTime.
    let Some(m) = arr.first() else {
        return not_closed_or_pending(http, token_id).await;
    };

    if !m.get("closed").and_then(|v| v.as_bool()).unwrap_or(false) {
        // The market object is in hand here, so corroborate from its own endDate
        // without a second request.
        return corroborate_not_closed(parse_end_date(m), chrono::Utc::now());
    }

    // Both arrive as JSON arrays that are sometimes string-encoded.
    let as_vec = |v: &serde_json::Value| -> Option<Vec<serde_json::Value>> {
        if let Some(a) = v.as_array() { return Some(a.clone()); }
        v.as_str().and_then(|s| serde_json::from_str::<Vec<serde_json::Value>>(s).ok())
    };
    let toks = unknown!(m.get("clobTokenIds").and_then(as_vec));
    let prices = unknown!(m.get("outcomePrices").and_then(as_vec));

    // Position matters: index i of clobTokenIds prices at index i of outcomePrices.
    let idx = unknown!(toks.iter().position(|t| t.as_str() == Some(token_id)));
    let raw = unknown!(prices.get(idx).and_then(|p| p.as_str()));
    let px = unknown!(Decimal::from_str(raw).ok());

    // Closed but not yet decisive — see the note above.
    if px > Decimal::new(1, 2) && px < Decimal::new(99, 2) {
        return TokenResolution::Unknown;
    }
    TokenResolution::Resolved(px)
}

/// The current Gamma mark for `token_id` on a market that is still trading.
///
/// For the chain sweep's off-strategy exit booking: a position that left the
/// wallet by a trade while its market is open is booked at "last mark", and
/// the row only carries one if a chain-sync pass refreshed it while the
/// position was live. A position opened and closed between two passes has
/// none (2026-09-13: GBoost's row was 3.5 minutes old when the sweep found
/// it), so the sweep asks the market instead. `None` on any failure: the
/// caller then books with the mark it has, or labels the exit unknown.
pub async fn open_market_mark_for_token(http: &reqwest::Client, token_id: &str) -> Option<rust_decimal::Decimal> {
    let url = format!("https://gamma-api.polymarket.com/markets?clob_token_ids={}", token_id);
    let resp = http.get(&url).send().await.ok()?;
    let data: serde_json::Value = resp.json().await.ok()?;
    let arr = data.as_array().cloned()
        .or_else(|| data.get("data").and_then(|v| v.as_array()).cloned())?;
    mark_from_gamma_market(arr.first()?, token_id)
}

/// `outcomePrices[i]` for the `clobTokenIds[i]` that is `token_id`, from a
/// Gamma market object. Both arrays arrive as JSON arrays that are sometimes
/// string-encoded. A price outside (0, 1) is not a mark.
pub fn mark_from_gamma_market(m: &serde_json::Value, token_id: &str) -> Option<rust_decimal::Decimal> {
    use std::str::FromStr;
    let as_vec = |v: &serde_json::Value| -> Option<Vec<serde_json::Value>> {
        if let Some(a) = v.as_array() { return Some(a.clone()); }
        v.as_str().and_then(|s| serde_json::from_str::<Vec<serde_json::Value>>(s).ok())
    };
    let toks = m.get("clobTokenIds").and_then(as_vec)?;
    let prices = m.get("outcomePrices").and_then(as_vec)?;
    let idx = toks.iter().position(|t| t.as_str() == Some(token_id))?;
    let raw = prices.get(idx)?;
    let px = match raw.as_str() {
        Some(s) => rust_decimal::Decimal::from_str(s).ok()?,
        None => rust_decimal::Decimal::try_from(raw.as_f64()?).ok()?,
    };
    (px > rust_decimal::Decimal::ZERO && px < rust_decimal::Decimal::ONE).then_some(px)
}

/// The market's scheduled end, from a Gamma market object. `None` when the
/// field is absent or unparseable — which corroboration treats as "cannot
/// confirm still trading", never as an answer.
fn parse_end_date(m: &serde_json::Value) -> Option<chrono::DateTime<chrono::Utc>> {
    m.get("endDate")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&chrono::Utc))
}

/// Second opinion on a "not closed" answer: fetch the market WITHOUT the closed
/// filter (Gamma's default view shows only open markets — verified live against
/// the 2026-09-01 token: `closed=true` returns the resolved market, the bare
/// query returns `[]`) and judge by its `endDate`.
///
/// The mid-flip race maps to `Unknown` by construction: a market being flipped
/// can be momentarily invisible in BOTH views (excluded from the open view
/// already, not yet served by the closed view), and invisible-everywhere is
/// precisely "no answer", not "still trading".
async fn not_closed_or_pending(http: &reqwest::Client, token_id: &str) -> TokenResolution {
    let url = format!(
        "https://gamma-api.polymarket.com/markets?clob_token_ids={}",
        token_id
    );
    let end_date = async {
        let resp = http.get(&url).send().await.ok()?;
        let data: serde_json::Value = resp.json().await.ok()?;
        let arr = data.as_array().cloned()
            .or_else(|| data.get("data").and_then(|v| v.as_array()).cloned())?;
        parse_end_date(arr.first()?)
    }
    .await;
    corroborate_not_closed(end_date, chrono::Utc::now())
}

/// What a "not closed" reading from Gamma is worth, given the market's own
/// schedule.
///
/// `NotClosed` is a load-bearing answer downstream: the sweep reads it as "the
/// position verifiably left the wallet by a TRADE" and books a mark-priced
/// off-strategy exit, then deletes the row — unappealable. That inference is
/// sound only while the market is actually trading. Past `endDate` nothing
/// trades, so a token that vanished can only have settled; reporting
/// `NotClosed` there is how the 2026-09-01 FairValue leg was booked at its
/// $0.9995 mark instead of its $1.00 settlement (see `resolution_for_token`).
///
/// The deliberate trade-off: an OFF-STRATEGY sale in the final seconds before
/// `endDate`, discovered only after it, is now deferred and eventually booked
/// at the resolved price rather than its last mark. Both are estimates; near
/// expiry they differ by at most the final spread, and the currently-closed
/// window (after Gamma's flag flips) already books such sales at the resolved
/// price today. The settled-and-redeemed position booked at a stale mark was
/// the real and observed loss.
fn corroborate_not_closed(
    end_date: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) -> TokenResolution {
    match end_date {
        Some(end) if now < end => TokenResolution::NotClosed,
        // Past its end, or no schedule visible at all: resolution pending —
        // defer, never book from a mark.
        _ => TokenResolution::Unknown,
    }
}

#[cfg(test)]
mod hourly_selection_waits_tests {
    /// The three-tier chain, as `pick_markets` applies it.
    /// Candidates are `(vol24h, is_up_or_down)`.
    fn chosen(cands: &[(f64, bool)], vol_floor: f64) -> Option<(f64, bool)> {
        let high: Vec<_> = cands.iter().filter(|c| c.0 >= vol_floor).copied().collect();
        let tradeable: Vec<_> = cands.iter().filter(|c| c.0 > 0.0 || c.1).copied().collect();
        if !high.is_empty() { high.first().copied() }
        else if !tradeable.is_empty() { tradeable.first().copied() }
        else { None }
    }

    /// The gap between hourly markets: the liquid "Up or Down" has closed and
    /// only zero-volume strike markets remain. Selecting nothing is correct —
    /// the squadron waits a few minutes for the next market to open.
    ///
    /// Ireland, 2026-08-27: the 5PM Up-or-Down (vol24h=14877) closed and the next
    /// selection took a vol24h=0 strike market, re-picked it every ~90s, and left
    /// the feed dark for 26 minutes with every strategy on an empty book.
    #[test]
    fn nothing_tradeable_selects_nothing() {
        let only_dead_strikes = [(0.0, false), (0.0, false), (0.0, false)];
        assert_eq!(chosen(&only_dead_strikes, 500.0), None,
                   "a dead market must not be selected just to have one");
    }

    /// A freshly published "Up or Down" legitimately has zero 24h volume and is
    /// quoted immediately — it must still be selectable, or the bot would idle
    /// through every rotation.
    #[test]
    fn a_fresh_up_or_down_is_still_chosen_at_zero_volume() {
        assert_eq!(chosen(&[(0.0, false), (0.0, true)], 500.0), Some((0.0, true)));
    }

    /// A thin but genuinely traded strike market is still acceptable.
    #[test]
    fn a_thin_traded_market_is_chosen_over_nothing() {
        assert_eq!(chosen(&[(0.0, false), (15.0, false)], 500.0), Some((15.0, false)));
    }

    /// The liquid tier still wins when it has anything in it.
    #[test]
    fn a_liquid_market_wins() {
        assert_eq!(chosen(&[(14877.0, true), (15.0, false)], 500.0), Some((14877.0, true)));
    }
}

#[cfg(test)]
mod hourly_fallback_tests {
    /// The fallback predicate from `pick_markets`: a candidate is tradeable if it
    /// has any 24h volume, or is a high-priority "Up or Down" market.
    fn tradeable(vol24h: f64, high_priority: bool) -> bool {
        vol24h > 0.0 || high_priority
    }

    /// A freshly published "Up or Down" market is quoted immediately even though
    /// its 24h volume is still zero — that is the case the soft floor's fallback
    /// exists for, and it must keep working.
    #[test]
    fn a_fresh_up_or_down_market_is_still_selectable_at_zero_volume() {
        assert!(tradeable(0.0, true));
    }

    /// A strike market at zero volume is a different animal: nobody has traded
    /// it and nobody is quoting it, so it has no order book to receive.
    ///
    /// On 2026-08-25 the fallback rotated from a market doing 26k onto
    /// "Bitcoin above 76,600 on August 25, 7PM ET?" at vol24h=0. The feed went
    /// dark for twenty minutes with every strategy pointed at an empty book, and
    /// it read as a venue outage. Falling back exists so the bot does not sit
    /// idle — but an untradeable market buys nothing over sitting idle.
    #[test]
    fn a_zero_volume_strike_market_is_rejected() {
        assert!(!tradeable(0.0, false));
    }

    /// Low volume is still acceptable in the fallback: the soft floor above
    /// prefers liquid markets, and this tier only excludes the untradeable.
    #[test]
    fn a_thin_but_traded_strike_market_is_still_acceptable() {
        assert!(tradeable(15.0, false));
    }
}

#[cfg(test)]
mod resolution_tests {
    use super::TokenResolution;
    use rust_decimal::Decimal;

    /// The three states must stay distinct. Collapsing NotClosed into Unknown routes
    /// an off-strategy sale onto the settlement path, where it books at $1.00 or
    /// $0.00 instead of its actual sale price — and the row is then deleted, so
    /// nothing can correct it.
    #[test]
    fn the_three_states_are_distinct() {
        assert_ne!(TokenResolution::NotClosed, TokenResolution::Unknown);
        assert_ne!(
            TokenResolution::Resolved(Decimal::ONE),
            TokenResolution::Resolved(Decimal::ZERO),
        );
    }

    /// Only a decisive price counts as resolved.
    ///
    /// Gamma's `closed` flag can flip when trading ends, ahead of oracle resolution,
    /// and `outcomePrices` may still carry a mid. The booking downstream snaps at
    /// 0.5, so a YES position last marked 0.49 in a market that resolves YES would be
    /// booked as a total loss, irreversibly.
    #[test]
    fn an_indecisive_price_is_not_a_resolution() {
        // Mirrors the gate inside `resolution_for_token`.
        let decisive = |px: Decimal| !(px > Decimal::new(1, 2) && px < Decimal::new(99, 2));
        assert!(decisive(Decimal::ZERO), "$0.00 is final");
        assert!(decisive(Decimal::ONE), "$1.00 is final");
        assert!(decisive(Decimal::new(1, 2)), "$0.01 is final enough");
        assert!(decisive(Decimal::new(99, 2)), "$0.99 is final enough");
        assert!(!decisive(Decimal::new(49, 2)), "$0.49 is a mid, not a resolution");
        assert!(!decisive(Decimal::new(50, 2)), "$0.50 placeholder must never book");
        assert!(!decisive(Decimal::new(95, 2)), "$0.95 is still a mark");
    }

    /// The 2026-09-01 settlement race, with the production values. FairValue's
    /// NO leg (5.093297 sh @ $0.9339…) on "Bitcoin Up or Down - September 1,
    /// 8PM ET" (endDate 2026-09-02T01:00:00Z) was auto-redeemed at $1.00, but
    /// at 01:15:21Z Gamma's closed-markets view still answered "not closed" —
    /// four minutes after the market's own recorded closedTime of 01:11:21Z.
    /// Trusting that answer booked a mark-priced exit at $0.9995 and deleted
    /// the row, permanently suppressing the genuine settlement booking. Past
    /// endDate, "not closed" must defer, not book.
    #[test]
    fn a_not_closed_answer_after_the_markets_end_is_deferred_not_booked_from_a_mark() {
        use chrono::{DateTime, Utc};
        let end: DateTime<Utc> = "2026-09-02T01:00:00Z".parse().unwrap();
        let reconcile_fired_at: DateTime<Utc> = "2026-09-02T01:15:21Z".parse().unwrap();
        assert_eq!(
            super::corroborate_not_closed(Some(end), reconcile_fired_at),
            TokenResolution::Unknown,
            "15 minutes past endDate nothing trades — a vanished token can only have settled"
        );
    }

    /// Inside the market's scheduled life "not closed" keeps its original
    /// meaning, so a genuine off-strategy sale still reaches the mark-priced
    /// reconcile path instead of being frozen behind a deferral.
    #[test]
    fn a_not_closed_answer_before_the_markets_end_still_means_trading() {
        use chrono::{DateTime, Utc};
        let end: DateTime<Utc> = "2026-09-02T01:00:00Z".parse().unwrap();
        let mid_session: DateTime<Utc> = "2026-09-02T00:30:00Z".parse().unwrap();
        assert_eq!(
            super::corroborate_not_closed(Some(end), mid_session),
            TokenResolution::NotClosed,
        );
    }

    /// A market invisible in BOTH Gamma views (excluded from the open view,
    /// not yet served by the closed view — the mid-flip race) yields no
    /// endDate at all. That is "no answer", never "still trading": defer.
    #[test]
    fn a_market_invisible_to_both_gamma_views_defers_rather_than_guesses() {
        assert_eq!(
            super::corroborate_not_closed(None, chrono::Utc::now()),
            TokenResolution::Unknown,
        );
    }
}

#[cfg(test)]
mod gamma_mark_tests {
    use super::mark_from_gamma_market;
    use rust_decimal_macros::dec;

    /// Gamma string-encodes both arrays on the live endpoint; a plain array
    /// must parse the same way, and the token's own index is what prices it.
    #[test]
    fn the_mark_is_the_outcome_price_at_the_tokens_index() {
        let encoded = serde_json::json!({
            "clobTokenIds": "[\"111\", \"222\"]",
            "outcomePrices": "[\"0.405\", \"0.595\"]",
        });
        assert_eq!(mark_from_gamma_market(&encoded, "222"), Some(dec!(0.595)));
        assert_eq!(mark_from_gamma_market(&encoded, "111"), Some(dec!(0.405)));
        let plain = serde_json::json!({ "clobTokenIds": ["111", "222"], "outcomePrices": ["0.4", "0.6"] });
        assert_eq!(mark_from_gamma_market(&plain, "222"), Some(dec!(0.6)));
    }

    /// An unknown token, a missing array or a resolved price ($0/$1) is not a
    /// mark for an open market: the sweep labels the exit unknown instead.
    #[test]
    fn no_mark_without_the_token_or_inside_a_binary_price() {
        let m = serde_json::json!({ "clobTokenIds": ["111", "222"], "outcomePrices": ["1", "0"] });
        assert_eq!(mark_from_gamma_market(&m, "333"), None);
        assert_eq!(mark_from_gamma_market(&m, "111"), None, "a settled $1.00 is not an open-market mark");
        assert_eq!(mark_from_gamma_market(&serde_json::json!({ "clobTokenIds": ["111"] }), "111"), None);
    }
}

#[cfg(test)]
mod hourly_slug_discovery_tests {
    use super::*;

    fn utc(s: &str) -> DateTime<Utc> { s.parse().unwrap() }

    const FOUR_PM_CID: &str = "0xe4cefddbe0083990a47828ca4e4fdc37c9e2fa31ed45c08c9dc5aad8f89a1046";
    const FIVE_PM_CID: &str = "0x4bab9f5308f4b079f40a0d16e26d0f375c18ffe2b7625d345793628ddb2c4a01";

    /// A Gamma market object as `markets?slug=` returns it: parent event under
    /// `events[]`, token ids as a JSON-encoded string.
    fn slug_market(question: &str, slug: &str, end: &str, created: &str, vol: f64, cid: &str, yes: &str, no: &str) -> serde_json::Value {
        serde_json::json!({
            "question": question, "slug": slug, "conditionId": cid,
            "endDate": end, "createdAt": created,
            "active": true, "closed": false, "acceptingOrders": true, "enableOrderBook": true,
            "volume24hrClob": vol,
            "clobTokenIds": format!("[\"{yes}\", \"{no}\"]"),
            "description": format!("This market will resolve to Up if the Binance 1 minute candle for BTC/USDT ... {question}"),
            "events": [{ "slug": slug, "title": question, "endDate": end }],
        })
    }

    /// A Gamma market object as the listing endpoints return it with
    /// `include=event`: parent event under `event`.
    fn listed_market(question: &str, slug: &str, end: &str, created: &str, vol: f64, cid: &str) -> serde_json::Value {
        serde_json::json!({
            "question": question, "slug": slug, "conditionId": cid,
            "endDate": end, "createdAt": created,
            "active": true, "closed": false, "enableOrderBook": true,
            "volume24hrClob": vol,
            "clobTokenIds": "[\"1000000000000000000000000000000000000000000000000000000000000000000000000001\", \"1000000000000000000000000000000000000000000000000000000000000000000000000002\"]",
            "description": "",
            "event": { "slug": slug, "title": question, "endDate": end },
        })
    }

    /// The two markets Gamma served by slug at 16:17 ET on 2026-09-13, values
    /// as returned live: both created two days ahead, the 4PM at $1361 of 24h
    /// volume and the 5PM at $11.
    fn four_pm() -> serde_json::Value {
        slug_market(
            "Bitcoin Up or Down - September 13, 4PM ET",
            "bitcoin-up-or-down-september-13-2026-4pm-et",
            "2026-09-13T21:00:00Z", "2026-09-11T20:00:00.262264Z", 1361.078, FOUR_PM_CID,
            "77411799041470897868285929183687023254603857718033275284667243778086863802644",
            "92182535393765438688240390781184158122538616088656169175442611934961656256751",
        )
    }
    fn five_pm() -> serde_json::Value {
        slug_market(
            "Bitcoin Up or Down - September 13, 5PM ET",
            "bitcoin-up-or-down-september-13-2026-5pm-et",
            "2026-09-13T22:00:00Z", "2026-09-11T21:00:00.351839Z", 11.764707, FIVE_PM_CID,
            "60436167959328677949501156435995009211532660197781615923929345465000095351628",
            "83148093899217001583989567968425046575893348327871346899637942449162612303123",
        )
    }

    /// The createdAt scan as replayed from the production box at 16:16 ET:
    /// all 100 newest markets created inside a 20-second span, none of them a
    /// BTC hourly market.
    fn newest_hundred() -> Vec<serde_json::Value> {
        (0..100).map(|i| {
            let created = utc("2026-09-13T20:09:59Z") + chrono::Duration::milliseconds(i * 200);
            listed_market(
                &format!("Will entry {i} win its September 13 heat?"),
                &format!("entry-{i}-september-13-heat"),
                "2026-09-14T04:00:00Z",
                &created.to_rfc3339(),
                0.0,
                &format!("0x{i:064x}"),
            )
        }).collect()
    }

    /// The volume scan's reachable floor: liquid markets, none a BTC hourly.
    /// The one BTC market in it is a long-dated strike market, rejected on the
    /// expiry window as it should be.
    fn volume_ranked() -> Vec<serde_json::Value> {
        let mut v: Vec<serde_json::Value> = (0..99).map(|i| listed_market(
            &format!("Will team {i} win its September 13 game?"),
            &format!("team-{i}-september-13"),
            "2026-09-14T04:00:00Z", "2026-09-10T12:00:00Z",
            83_196.0 - i as f64 * 800.0,
            &format!("0xa{i:063x}"),
        )).collect();
        v.push(listed_market(
            "Will Bitcoin be above $120,000 on December 31?",
            "bitcoin-above-120000-december-31",
            "2026-12-31T17:00:00Z", "2026-01-05T12:00:00Z",
            40_000.0,
            "0xb000000000000000000000000000000000000000000000000000000000000001",
        ));
        v
    }

    /// The 2026-09-13 16:03 ET outage, reproduced from the evidence. Both
    /// listing scans come back with no BTC hourly market; the slug lookup
    /// finds the 4PM and 5PM markets, and the 4PM market is selected.
    #[test]
    fn the_current_hour_is_found_by_slug_when_both_listing_scans_miss_it() {
        let now = utc("2026-09-13T20:03:00Z"); // 16:03 ET, three minutes into the 4PM window

        let recent: Vec<GammaCandidate> = newest_hundred().iter()
            .filter_map(|m| candidate_from_gamma_market(m, "btc", now, 0.0)).collect();
        let by_volume: Vec<GammaCandidate> = volume_ranked().iter()
            .filter_map(|m| candidate_from_gamma_market(m, "btc", now, config::MIN_MARKET_VOLUME)).collect();
        assert!(recent.is_empty(), "the failure shape: the newest 100 carry no BTC hourly market");
        assert!(by_volume.is_empty(), "the failure shape: the volume-ranked pages carry no BTC hourly market");

        let by_slug: Vec<GammaCandidate> = [four_pm(), five_pm()].iter()
            .filter_map(|m| candidate_from_gamma_market(m, "btc", now, 0.0)).collect();
        assert_eq!(by_slug.len(), 2, "both slug hits validate: {:?}", by_slug.iter().map(|c| &c.1).collect::<Vec<_>>());

        let merged = merge_candidates(vec![by_slug, by_volume, recent]);
        let pick = pick_hourly_candidate(&merged, config::MIN_HOURLY_MARKET_VOL24H)
            .expect("the 4PM market is live, accepting orders, and inside the entry window");
        assert_eq!(pick.7, FOUR_PM_CID);
        assert_eq!(pick.1, "Bitcoin Up or Down - September 13, 4PM ET");
        assert!(pick.4, "an Up or Down market is high priority");
        assert_eq!(pick.5, Some(utc("2026-09-13T21:00:00Z")));
    }

    /// Rotation: once the 4PM market is inside the entry floor it drops out of
    /// validation, and the 5PM market, at $11 of 24h volume, is the tradeable
    /// fallback (an "Up or Down" needs no volume to be selectable).
    #[test]
    fn the_next_hour_takes_over_once_the_current_one_is_inside_the_entry_floor() {
        let now = utc("2026-09-13T21:00:00Z") - chrono::Duration::seconds(config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY - 1);
        let by_slug: Vec<GammaCandidate> = [four_pm(), five_pm()].iter()
            .filter_map(|m| candidate_from_gamma_market(m, "btc", now, 0.0)).collect();
        assert_eq!(by_slug.len(), 1, "the 4PM market is inside the entry floor and must not validate");
        let pick = pick_hourly_candidate(&merge_candidates(vec![by_slug]), config::MIN_HOURLY_MARKET_VOL24H).unwrap();
        assert_eq!(pick.7, FIVE_PM_CID);
    }

    /// Before the 5PM market is needed, the 4PM one outranks it: both are high
    /// priority, so volume decides, and a market's volume accrues while it is
    /// the live window.
    #[test]
    fn the_live_window_outranks_the_next_one() {
        let now = utc("2026-09-13T20:30:00Z");
        let by_slug: Vec<GammaCandidate> = [five_pm(), four_pm()].iter()
            .filter_map(|m| candidate_from_gamma_market(m, "btc", now, 0.0)).collect();
        let pick = pick_hourly_candidate(&merge_candidates(vec![by_slug]), config::MIN_HOURLY_MARKET_VOL24H).unwrap();
        assert_eq!(pick.7, FOUR_PM_CID, "source order must not decide; the sort does");
    }

    /// A pair of consecutive BTC hourly markets as the slug lookup returns them.
    fn hour_pair(first: (&str, &str, &str, f64), second: (&str, &str, &str, f64)) -> [serde_json::Value; 2] {
        let mk = |(question, slug, end, vol): (&str, &str, &str, f64), cid: &str, yes: &str, no: &str| {
            slug_market(question, slug, end, "2026-09-11T00:00:00Z", vol, cid, yes, no)
        };
        [
            mk(first, "0x00000000000000000000000000000000000000000000000000000000000000a1",
               "11111111111111111111111111111111111111111111111111111111111111111111111111101",
               "11111111111111111111111111111111111111111111111111111111111111111111111111102"),
            mk(second, "0x00000000000000000000000000000000000000000000000000000000000000a2",
               "11111111111111111111111111111111111111111111111111111111111111111111111111103",
               "11111111111111111111111111111111111111111111111111111111111111111111111111104"),
        ]
    }
    const FIRST_HOUR_CID: &str = "0x00000000000000000000000000000000000000000000000000000000000000a1";

    fn pick_at(now: DateTime<Utc>, markets: &[serde_json::Value]) -> GammaCandidate {
        let by_slug: Vec<GammaCandidate> = markets.iter()
            .filter_map(|m| candidate_from_gamma_market(m, "btc", now, 0.0)).collect();
        assert_eq!(by_slug.len(), markets.len(), "every market validates: {:?}", by_slug.iter().map(|c| &c.1).collect::<Vec<_>>());
        pick_hourly_candidate(&merge_candidates(vec![by_slug]), config::MIN_HOURLY_MARKET_VOL24H).unwrap()
    }

    /// Production, 2026-09-13 22:00:16 ET: sixteen seconds into the 10PM window
    /// the slug lookup returned the 10PM and 11PM markets, both at zero volume,
    /// and the time-left tiebreak picked 11PM. The monitor switched and the 10PM
    /// hour went untraded.
    #[test]
    fn a_just_opened_hour_is_not_abandoned_on_the_tiebreak() {
        let markets = hour_pair(
            ("Bitcoin Up or Down - September 13, 10PM ET", "bitcoin-up-or-down-september-13-2026-10pm-et", "2026-09-14T03:00:00Z", 0.0),
            ("Bitcoin Up or Down - September 13, 11PM ET", "bitcoin-up-or-down-september-13-2026-11pm-et", "2026-09-14T04:00:00Z", 0.0),
        );
        assert_eq!(pick_at(utc("2026-09-14T02:00:16Z"), &markets).7, FIRST_HOUR_CID);
    }

    /// Production, 2026-09-14 01:00:16 ET: the 2AM market ($8 of 24h volume)
    /// outranked the just-opened 1AM market ($4) and the 1AM hour went untraded.
    #[test]
    fn a_just_opened_hour_is_not_abandoned_on_volume() {
        let markets = hour_pair(
            ("Bitcoin Up or Down - September 14, 1AM ET", "bitcoin-up-or-down-september-14-2026-1am-et", "2026-09-14T06:00:00Z", 4.0),
            ("Bitcoin Up or Down - September 14, 2AM ET", "bitcoin-up-or-down-september-14-2026-2am-et", "2026-09-14T07:00:00Z", 8.0),
        );
        assert_eq!(pick_at(utc("2026-09-14T05:00:16Z"), &markets).7, FIRST_HOUR_CID);
    }

    /// The next hour clearing the volume floor while the live one does not is
    /// the same abandonment by the first tier; before either window opens the
    /// sooner one is still the pick.
    #[test]
    fn the_next_hour_above_the_volume_floor_does_not_take_the_live_one() {
        let markets = hour_pair(
            ("Bitcoin Up or Down - September 14, 1AM ET", "bitcoin-up-or-down-september-14-2026-1am-et", "2026-09-14T06:00:00Z", 10.0),
            ("Bitcoin Up or Down - September 14, 2AM ET", "bitcoin-up-or-down-september-14-2026-2am-et", "2026-09-14T07:00:00Z", config::MIN_HOURLY_MARKET_VOL24H + 5000.0),
        );
        assert_eq!(pick_at(utc("2026-09-14T05:30:00Z"), &markets).7, FIRST_HOUR_CID);
        assert_eq!(pick_at(utc("2026-09-14T04:52:00Z"), &markets).7, FIRST_HOUR_CID);
    }

    fn bare(name: &str, vol: f64, close: Option<DateTime<Utc>>, cid: &str) -> GammaCandidate {
        (Vec::new(), name.to_string(), String::new(), vol, is_high_priority_text(name), close, String::new(), cid.to_string())
    }

    /// Several assets' current hours share a close and all stay in contention:
    /// volume chooses among them as before, and each asset's next hour drops out.
    #[test]
    fn every_assets_current_hour_competes_and_no_next_hour_does() {
        let merged = vec![
            bare("Bitcoin Up or Down - September 14, 1AM ET", 100.0, Some(utc("2026-09-14T06:00:00Z")), "btc-1am"),
            bare("Ethereum Up or Down - September 14, 1AM ET", 50.0, Some(utc("2026-09-14T06:00:00Z")), "eth-1am"),
            bare("Bitcoin Up or Down - September 14, 2AM ET", 50_000.0, Some(utc("2026-09-14T07:00:00Z")), "btc-2am"),
        ];
        assert!(merged.iter().all(|c| c.4), "fixture: all are Up or Down");
        assert_eq!(pick_hourly_candidate(&merged, config::MIN_HOURLY_MARKET_VOL24H).unwrap().7, "btc-1am");
    }

    /// An "Up or Down" candidate without a close time neither sets the soonest
    /// close nor is dropped by it: it competes on volume as it did before.
    #[test]
    fn an_up_or_down_without_a_close_time_is_left_to_the_volume_sort() {
        let merged = vec![
            bare("Bitcoin Up or Down - September 14, 1AM ET", 10.0, Some(utc("2026-09-14T06:00:00Z")), "dated"),
            bare("Bitcoin Up or Down - September 14, 2AM ET", 20.0, None, "undated"),
        ];
        assert_eq!(pick_hourly_candidate(&merged, config::MIN_HOURLY_MARKET_VOL24H).unwrap().7, "undated");
    }

    /// The same market reached by slug and by a listing scan is one candidate,
    /// and the slug copy (first source) is the one kept. Both Gamma shapes for
    /// the parent event parse.
    #[test]
    fn a_market_found_by_slug_and_by_scan_is_one_candidate() {
        let now = utc("2026-09-13T20:03:00Z");
        let by_slug = candidate_from_gamma_market(&four_pm(), "btc", now, 0.0).unwrap();
        let listed = listed_market(
            "Bitcoin Up or Down - September 13, 4PM ET",
            "bitcoin-up-or-down-september-13-2026-4pm-et",
            "2026-09-13T21:00:00Z", "2026-09-11T20:00:00.262264Z", 1361.078, FOUR_PM_CID,
        );
        let by_scan = candidate_from_gamma_market(&listed, "btc", now, config::MIN_MARKET_VOLUME).unwrap();
        assert_eq!(by_scan.5, Some(utc("2026-09-13T21:00:00Z")), "the `event` shape yields the close time");
        let merged = merge_candidates(vec![vec![by_slug.clone()], vec![by_scan]]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].0, by_slug.0, "the slug copy's tokens are the ones kept");
    }

    /// A slug hit is not trusted for being addressed directly: it is held to
    /// every check a scanned market gets.
    #[test]
    fn a_slug_hit_is_held_to_the_same_validation_as_a_scanned_market() {
        let now = utc("2026-09-13T20:03:00Z");
        assert!(candidate_from_gamma_market(&four_pm(), "btc", now, 0.0).is_some());

        let mut no_book = four_pm();
        no_book["enableOrderBook"] = serde_json::json!(false);
        assert!(candidate_from_gamma_market(&no_book, "btc", now, 0.0).is_none(), "order book disabled");

        assert!(candidate_from_gamma_market(&four_pm(), "eth", now, 0.0).is_none(), "wrong asset");

        let after_close = utc("2026-09-13T21:00:01Z");
        assert!(candidate_from_gamma_market(&four_pm(), "btc", after_close, 0.0).is_none(), "expired");

        let too_early = utc("2026-09-13T21:00:00Z") - chrono::Duration::seconds(config::MAX_SECONDS_TO_EXPIRY_FOR_ENTRY + 1);
        assert!(candidate_from_gamma_market(&four_pm(), "btc", too_early, 0.0).is_none(), "beyond the entry horizon");

        let mut one_token = four_pm();
        one_token["clobTokenIds"] = serde_json::json!("[\"77411799041470897868285929183687023254603857718033275284667243778086863802644\"]");
        assert!(candidate_from_gamma_market(&one_token, "btc", now, 0.0).is_none(), "one token id");
    }

    /// Gamma answers an offset past 2000 with an error object. That is the end
    /// of the listing, not a page of markets.
    #[test]
    fn an_error_object_is_an_empty_page() {
        let err = serde_json::json!({ "type": "validation error", "error": "offset too large, use /markets/keyset for deeper pagination" });
        assert!(gamma_market_array(&err).is_empty());
        assert_eq!(gamma_market_array(&serde_json::json!([four_pm()])).len(), 1);
        assert_eq!(gamma_market_array(&serde_json::json!({ "data": [four_pm()] })).len(), 1);
    }

    /// The single-asset `CRYPTO_FILTER` fallback of `"all"` probes every asset
    /// that has hourly markets; a named asset probes only itself.
    #[test]
    fn the_all_filter_probes_every_hourly_asset() {
        assert_eq!(hourly_assets_for("all"), vec!["btc", "eth", "sol"]);
        assert_eq!(hourly_assets_for("btc"), vec!["btc"]);
        assert_eq!(hourly_assets_for("ethereum"), vec!["eth"]);
        assert_eq!(hourly_assets_for("sol"), vec!["sol"]);
    }
}

/// Live check against Gamma, run by hand: `cargo test live_gamma -- --ignored --nocapture`.
///
/// Exercises `get_market_pair`, the exact call the CAG bootstrap and the market
/// monitor make, for each asset with hourly markets, and reports what the slug
/// source alone found. Not part of the normal suite: it needs the network and
/// its answer depends on the clock.
#[cfg(test)]
mod live_gamma_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "hits the live Gamma API"]
    async fn live_gamma_slug_lookup_finds_the_current_hourly_market() {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(20))
            .build().unwrap();
        let now = Utc::now();
        for asset in ["btc", "eth", "sol"] {
            let by_slug = fetch_hourly_candidates_by_slug(&http, asset, now).await;
            println!("[{asset}] slug source at {}: {} candidate(s)", now.to_rfc3339(), by_slug.len());
            for c in &by_slug {
                println!("[{asset}]   \"{}\" vol24h={:.0} closes={:?} cid={}", c.1, c.3, c.5.map(|t| t.to_rfc3339()), c.7);
            }
            assert!(!by_slug.is_empty(), "[{asset}] the current hour's market must resolve by slug");

            let (hourly, maker) = get_market_pair(&http, asset).await;
            println!("[{asset}] get_market_pair → hourly=\"{}\" vol24h={:.0} closes={:?} | maker={:?}",
                hourly.name, hourly.volume, hourly.close_time.map(|t| t.to_rfc3339()), maker.as_ref().map(|m| &m.name));
            assert_ne!(hourly.yes_token, market_id_from_u256(U256::ZERO), "[{asset}] get_market_pair must select an hourly market");
            assert!(hourly.name.to_lowercase().contains("up or down"), "[{asset}] selected: {}", hourly.name);
        }
    }
}
