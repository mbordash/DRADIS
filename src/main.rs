use anyhow::Result;
use std::borrow::Cow;

use polymarket_client_sdk::clob::{Client as ClobClient, Config};
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::{OrderType, Side, SignatureType, Order, SignedOrder};
use polymarket_client_sdk::{POLYGON, PRIVATE_KEY_VAR, derive_safe_wallet};
use polymarket_client_sdk::clob::types::request::{
    BalanceAllowanceRequest,
};
use polymarket_client_sdk::clob::types::AssetType;

use futures::StreamExt as _;
use polymarket_client_sdk::clob::ws::Client as WsClient;

use alloy::primitives::{U256, Address, address};
use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;
use alloy::dyn_abi::Eip712Domain;
use alloy::sol_types::SolStruct;

use chrono::{DateTime, Utc, TimeZone, Datelike, Timelike};
use chrono_tz::US::Eastern;
use reqwest;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;

use std::collections::{HashMap, VecDeque};
use std::env;
use std::str::FromStr as _;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tokio::time::{interval, Instant, Duration};

use tracing::{error, info, warn, debug};

use rustpolybot::config;
use rustpolybot::risk::RiskEngine;
use rustpolybot::notifications::send_notification;

use rustls::crypto::ring;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use regex::Regex;
use urlencoding;

type PriceState = (Decimal, Decimal, Decimal); // (Bid, Ask, AskDepth)

#[derive(Debug, Clone)]
struct Position {
    shares: Decimal,
    avg_entry: Decimal,
    #[allow(dead_code)]
    opened_at: DateTime<Utc>,
    close_time: Option<DateTime<Utc>>,
    market_name: String,
    pair_token_id: U256,
}

const ORDER_NAME: &str = "Polymarket CTF Exchange";
const VERSION: &str = "1";
const USDC_DECIMALS: u32 = 6;

// Verified Exchange Addresses from SDK
const EXCHANGE_NORMAL: Address = address!("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E");
const EXCHANGE_NEG_RISK: Address = address!("0xC5d563A36AE78145C45a50134d48A1215220f80a");

fn to_fixed_u128(d: Decimal) -> u128 {
    d.normalize()
        .trunc_with_scale(USDC_DECIMALS)
        .mantissa()
        .to_u128()
        .unwrap_or(0)
}

fn parse_balance_from_error(err_msg: &str) -> Option<Decimal> {
    let re = Regex::new(r"(?:balance|available):\s*(\d+)").unwrap();
    if let Some(cap) = re.captures(err_msg) {
        if let Ok(val) = cap[1].parse::<u128>() {
            return Some(Decimal::from(val) / dec!(1_000_000));
        }
    }
    None
}

async fn fetch_next_nonce(http: &reqwest::Client, address: Address) -> Option<u64> {
    let url = format!("{}/nonce?address={}", config::CLOB_API_BASE, address);
    match http.get(&url).send().await {
        Ok(resp) => {
            let status = resp.status();
            if status == reqwest::StatusCode::NOT_FOUND {
                return Some(0);
            }
            let body = resp.text().await.unwrap_or_default();
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                if let Some(n) = json.get("next_nonce").and_then(|n| n.as_u64()) {
                    return Some(n);
                }
                warn!("⚠️ Nonce API response missing next_nonce (Status {}): {}", status, body);
            } else {
                warn!("⚠️ Nonce API returned non-JSON response (Status {}). Account might not be initialized or API is down.", status);
            }
        },
        Err(e) => error!("⚠️ Failed to connect to Nonce API: {:?}", e),
    }
    None
}

async fn sync_nonce_manager(nonce_manager: &Arc<Mutex<u64>>, http: &reqwest::Client, address: Address) {
    if let Some(new_nonce) = fetch_next_nonce(http, address).await {
        let mut guard = nonce_manager.lock().await;
        *guard = new_nonce;
        info!("🔄 Nonce manager synchronized to: {} for address {}", new_nonce, address);
    }
}

async fn sync_position_balance(
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    positions: &Arc<Mutex<HashMap<U256, Position>>>,
    token_id: U256,
) -> Result<()> {
    // Wait longer for the exchange ledger to update after a fill to avoid "balance is 0" race condition
    tokio::time::sleep(Duration::from_millis(2000)).await;

    let mut req = BalanceAllowanceRequest::default();
    req.asset_type = AssetType::Conditional; // Corrected variant
    req.token_id = Some(token_id);

    if let Ok(resp) = client.balance_allowance(req).await {
        let actual_shares = Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000);
        let mut pos_map = positions.lock().await;
        if let Some(pos) = pos_map.get_mut(&token_id) {
            if actual_shares > dec!(0) {
                info!("⚖️ Position Synced: Token {} quantity updated from {} to actual: {}", token_id, pos.shares, actual_shares);
                pos.shares = actual_shares;
            } else {
                warn!("⚠️ Position Sync: Token {} balance is 0. This might be a lag in the indexer. Keeping local position for now.", token_id);
                // Don't remove the position immediately if we just bought it or think we have it.
                // It will be cleaned up by expiry logic or manually if it truly remains 0.
            }
        }
    }
    Ok(())
}

fn value_to_u256(v: &serde_json::Value) -> Option<U256> {
    if let Some(s) = v.as_str() { U256::from_str(s).ok() }
    else if let Some(n) = v.as_u64() { Some(U256::from(n)) }
    else if let Some(n) = v.as_i64().filter(|&n| n >= 0) { Some(U256::from(n as u64)) }
    else { None }
}

fn value_to_f64(v: &serde_json::Value) -> Option<f64> {
    if let Some(n) = v.as_f64() { Some(n) }
    else if let Some(s) = v.as_str() { s.trim().parse::<f64>().ok() }
    else { None }
}

fn get_enable_orderbook(market: &serde_json::Value) -> bool {
    market.get("enableOrderBook").and_then(|v| v.as_bool()).unwrap_or(false) ||
        market.get("enable_order_book").and_then(|v| v.as_bool()).unwrap_or(false)
}

fn parse_dt(v: Option<&serde_json::Value>) -> Option<DateTime<Utc>> {
    let s = v.and_then(|x| x.as_str())?;
    DateTime::parse_from_rfc3339(s).ok().map(|dt| dt.with_timezone(&Utc))
}

fn extract_close_time(event: &serde_json::Value, market: &serde_json::Value) -> Option<DateTime<Utc>> {
    parse_dt(market.get("endDate"))
        .or_else(|| parse_dt(market.get("end_date")))
        .or_else(|| parse_dt(market.get("closeTime")))
        .or_else(|| parse_dt(market.get("close_time")))
        .or_else(|| parse_dt(event.get("endDate")))
        .or_else(|| parse_dt(event.get("end_date")))
        .or_else(|| parse_dt(event.get("closeTime")))
        .or_else(|| parse_dt(event.get("close_time")))
}

fn extract_start_time(event: &serde_json::Value, market: &serde_json::Value) -> Option<DateTime<Utc>> {
    parse_dt(market.get("startDate"))
        .or_else(|| parse_dt(market.get("start_date")))
        .or_else(|| parse_dt(event.get("startDate")))
        .or_else(|| parse_dt(event.get("start_date")))
}

fn extract_token_ids_u256(market: &serde_json::Value) -> Vec<U256> {
    let v = market.get("clobTokenIds")
        .or_else(|| market.get("clob_token_ids"))
        .unwrap_or(&serde_json::Value::Null);

    let mut out = vec![];

    if let Some(arr) = v.as_array() {
        for item in arr {
            if let Some(t) = value_to_u256(item) {
                if t != U256::ZERO { out.push(t); }
            }
        }
        if out.len() >= 2 { return out; }
    }

    if let Some(s) = v.as_str() {
        if let Ok(parsed) = serde_json::from_str::<Vec<serde_json::Value>>(s) {
            for item in parsed {
                if let Some(t) = value_to_u256(&item) {
                    if t != U256::ZERO { out.push(t); }
                }
            }
        }
        else if let Ok(parsed) = serde_json::from_str::<Vec<String>>(s) {
            for item_str in parsed {
                if let Ok(t) = U256::from_str(&item_str) {
                    if t != U256::ZERO { out.push(t); }
                }
            }
        }
    }

    if let Some(t) = value_to_u256(v) {
        if t != U256::ZERO { out.push(t); }
    }

    out
}

async fn fetch_historical_strike_price(http: &reqwest::Client, filter: &str, text_to_scan: &str) -> Option<Decimal> {
    let lower_text = text_to_scan.to_lowercase();

    let re1 = Regex::new(r"([a-z]{3})\s+(\d{1,2})\s+'(\d{2})\s+(\d{1,2}):(\d{2})").unwrap();
    let re2 = Regex::new(r"([a-z]+)\s+(\d{1,2}),\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)").unwrap();

    let (year, month, day, hour, min) = if let Some(cap) = re1.captures(&lower_text) {
        let month_str = cap.get(1).map(|m| m.as_str())?;
        let day: u32 = cap.get(2).map(|m| m.as_str().parse().ok()).flatten()?;
        let year: i32 = 2000 + cap.get(3).map(|m| m.as_str().parse::<i32>().ok()).flatten()?;
        let hour: u32 = cap.get(4).map(|m| m.as_str().parse().ok()).flatten()?;
        let min: u32 = cap.get(5).map(|m| m.as_str().parse().ok()).flatten()?;

        let month = match month_str {
            "jan" => 1, "feb" => 2, "mar" => 3, "apr" => 4, "may" => 5, "jun" => 6,
            "jul" => 7, "aug" => 8, "sep" => 9, "oct" => 10, "nov" => 11, "dec" => 12,
            _ => return None,
        };
        (year, month, day, hour, min)
    } else if let Some(cap) = re2.captures(&lower_text) {
        let month_str = cap.get(1).map(|m| m.as_str())?;
        let day: u32 = cap.get(2).map(|m| m.as_str().parse().ok()).flatten()?;
        let mut hour: u32 = cap.get(3).map(|m| m.as_str().parse().ok()).flatten()?;
        let min: u32 = cap.get(4).map(|m| m.as_str().parse().unwrap_or(0)).unwrap_or(0);
        let ampm = cap.get(5).map(|m| m.as_str())?;

        if ampm == "pm" && hour < 12 { hour += 12; }
        if ampm == "am" && hour == 12 { hour = 0; }

        let month = match &month_str[..3] {
            "jan" => 1, "feb" => 2, "mar" => 3, "apr" => 4, "may" => 5, "jun" => 6,
            "jul" => 7, "aug" => 8, "sep" => 9, "oct" => 10, "nov" => 11, "dec" => 12,
            _ => return None,
        };
        let year = Utc::now().year();
        (year, month, day, hour, min)
    } else {
        return None;
    };

    let et_time = match Eastern.with_ymd_and_hms(year, month, day, hour, min, 0).single() {
        Some(t) => t,
        None => return None,
    };
    let utc_millis = et_time.with_timezone(&Utc).timestamp_millis();

    let binance_symbol = match filter {
        "eth" => "ETHUSDT",
        "sol" => "SOLUSDT",
        _ => "BTCUSDT",
    };

    let url = format!("https://api.binance.com/api/v3/klines?symbol={}&interval=1m&startTime={}&limit=1", binance_symbol, utc_millis);

    if let Ok(resp) = http.get(&url).send().await {
        if let Ok(json) = resp.json::<serde_json::Value>().await {
            if let Some(candle) = json.as_array().and_then(|a| a.first()) {
                if let Some(close_str) = candle.as_array().and_then(|a| a.get(4)).and_then(|v| v.as_str()) {
                    return Decimal::from_str(close_str).ok();
                }
            }
        }
    }
    None
}

fn extract_strike_price_from_name(market_name: &str) -> Option<Decimal> {
    let lower_name = market_name.to_lowercase();
    let re = Regex::new(r"(?:\$|above|below|at)\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d{3,}(?:\.\d+)?)").unwrap();

    if let Some(cap) = re.captures(&lower_name) {
        if let Some(num_str) = cap.get(1) {
            let cleaned_num_str = num_str.as_str().replace(",", "");
            if let Ok(num) = Decimal::from_str(&cleaned_num_str) {
                if num > dec!(100) {
                    return Some(num);
                }
            }
        }
    }
    None
}

// Helper to generate market names for hourly crypto events
fn generate_hourly_market_names(crypto_filter: &str, current_time_utc: DateTime<Utc>) -> Vec<String> {
    let mut names = Vec::new();
    let eastern_time = current_time_utc.with_timezone(&Eastern);

    let crypto_name_long = match crypto_filter {
        "btc" => "Bitcoin",
        "eth" => "Ethereum",
        "sol" => "Solana",
        _ => "Crypto",
    };
    let crypto_name_short = crypto_filter.to_uppercase();

    // Generate names for current hour and next hour
    for i in 0..=1 {
        let target_time = eastern_time.clone() + chrono::Duration::hours(i);
        let hour = target_time.hour();
        let ampm = if hour >= 12 { "PM" } else { "AM" };
        let display_hour = if hour == 0 { 12 } else if hour > 12 { hour - 12 } else { hour };
        let next_hour = if display_hour == 12 { 1 } else { display_hour + 1 };

        let month_name = target_time.format("%B").to_string();
        let day = target_time.day();

        // Standard: "Bitcoin Up or Down - April 3, 5PM ET"
        names.push(format!("{} Up or Down - {} {}, {}{} ET", crypto_name_long, month_name, day, display_hour, ampm));
        // Range: "Bitcoin Up or Down - April 3, 5-6PM ET"
        names.push(format!("{} Up or Down - {} {}, {}-{}{} ET", crypto_name_long, month_name, day, display_hour, next_hour, ampm));
        // Short name versions
        names.push(format!("{} Up or Down - {} {}, {}{} ET", crypto_name_short, month_name, day, display_hour, ampm));
        names.push(format!("{} Up or Down - {} {}, {}-{}{} ET", crypto_name_short, month_name, day, display_hour, next_hour, ampm));
    }
    names
}

async fn fetch_specific_hourly_market(http: &reqwest::Client, crypto_filter: &str, now: DateTime<Utc>) -> Option<(Vec<U256>, String, String, f64, bool, Option<DateTime<Utc>>, String)> {
    let candidate_names = generate_hourly_market_names(crypto_filter, now);

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


async fn get_top_market(http: &reqwest::Client) -> (U256, U256, String, String, String, bool, Option<DateTime<Utc>>) {
    let crypto_filter = env::var("CRYPTO_FILTER")
        .unwrap_or_else(|_| "all".to_string())
        .to_lowercase();

    info!("🚀 Scanning Gamma API for markets (FILTER: {})", crypto_filter);
    let now = Utc::now();

    // 1. Try specifically targeted hourly markets first (Fastest)
    if let Some(market) = fetch_specific_hourly_market(http, &crypto_filter, now).await {
        info!("🏆 Found specific hourly market: \"{}\"", market.1);
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

        let a_sweet = a_secs > 1800 && a_secs < 3600;
        let b_sweet = b_secs > 1800 && b_secs < 3600;

        if a_sweet != b_sweet {
            b_sweet.cmp(&a_sweet)
        } else {
            b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal)
        }
    });

    let best = &sorted[0];
    info!("🏆 Selected market: \"{}\"", best.1);
    (best.0[0], best.0[1], best.1.clone(), best.2.clone(), best.6.clone(), best.4, best.5)
}

async fn fetch_simplified_crypto_candidates(
    http: &reqwest::Client,
    crypto_filter: &str,
) -> Vec<(Vec<U256>, String, String, f64, bool, Option<DateTime<Utc>>, String)> {
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

            let lower = name.to_lowercase();
            let lower_event = event_title.to_lowercase();

            // PRE-FILTER: Only process the coin we care about
            let matches_crypto = match crypto_filter {
                "btc" | "bitcoin" => lower.contains("bitcoin") || lower.contains("btc") || lower_event.contains("bitcoin") || lower_event.contains("btc"),
                "eth" | "ethereum" => lower.contains("ethereum") || lower.contains("eth") || lower_event.contains("ethereum") || lower_event.contains("eth"),
                "sol" | "solana" => lower.contains("solana") || lower.contains("sol") || lower_event.contains("solana") || lower_event.contains("sol"),
                _ => true,
            };
            if !matches_crypto { continue; }

            debug!("🔍 Evaluating candidate: \"{}\" (Event: \"{}\")", name, event_title);

            if config::is_bad_market(&name) || config::is_bad_market(&event_title) { continue; }
            if !get_enable_orderbook(market) { continue; }

            if !lower.contains("up or down") && !lower_event.contains("up or down") {
                debug!("  ⏭️ Rejected: No 'up or down' in question or event title");
                continue;
            }
            if !is_short_term_window(&name) && !is_short_term_window(&event_title) {
                debug!("  ⏭️ Rejected: Not a short-term window");
                continue;
            }

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
                    debug!("  ⏭️ Skipping candidate \"{}\" - hasn't started yet (Starts in {}s)", name, (st - now).num_seconds());
                    continue;
                }
            }

            if seconds_left < config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY { continue; }
            if seconds_left > config::MAX_SECONDS_TO_EXPIRY_FOR_ENTRY { continue; }

            let link = market.get("slug").and_then(|v| v.as_str()).map(|s| format!("https://polymarket.com/{}", s)).unwrap_or_else(|| "https://polymarket.com/".to_string());
            let description = market.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let hot = config::is_high_priority_text(&name) || config::is_high_priority_text(&event_title);

            out.push((token_ids, name.clone(), link, volume, hot, close_time, description));
        }
    }
    info!("✅ Total scanned: {} | Candidates after filters: {}", total_scanned, out.len());
    out
}

fn is_short_term_window(name: &str) -> bool {
    let lower = name.to_lowercase();
    if lower.contains("et") && (lower.contains("am") || lower.contains("pm")) {
        if lower.contains("12:00pm") || lower.contains("am-12:") || lower.contains("pm-12:") {
            return false;
        }
        let parts: Vec<&str> = lower.split(&['-', ':'][..]).collect();
        if parts.len() >= 3 {
            let start_str = parts[parts.len() - 3].trim();
            let _end_str = parts[parts.len() - 2].trim();
            if start_str.contains("am") || start_str.contains("pm") {
                return true;
            }
        }
        if lower.contains(":") && (lower.contains("am") || lower.contains("pm")) {
            return true;
        }
    }
    lower.contains("hour") || lower.contains("et")
}

async fn cleanup_expired_positions(
    positions: Arc<Mutex<HashMap<U256, Position>>>,
    _market_name: String,
    yes_token: U256,
    no_token: U256,
    market_close_time: Option<DateTime<Utc>>,
) {
    let mut pos_map = positions.lock().await;
    let now = Utc::now();
    let mut removed_count = 0;
    let mut tokens_to_remove = Vec::new();

    if let Some(ct) = market_close_time {
        if ct < now {
            if pos_map.contains_key(&yes_token) { tokens_to_remove.push(yes_token); }
            if pos_map.contains_key(&no_token) { tokens_to_remove.push(no_token); }
        }
    }

    for (token_id, pos) in pos_map.iter() {
        if let Some(ct) = pos.close_time {
            if ct < now {
                tokens_to_remove.push(*token_id);
                tokens_to_remove.push(pos.pair_token_id);
            }
        }
    }

    tokens_to_remove.sort_unstable();
    tokens_to_remove.dedup();

    for token_id in tokens_to_remove {
        if let Some(pos) = pos_map.remove(&token_id) {
            info!("🧹 Cleaned up expired position for market \"{}\" (Token: {})", pos.market_name, token_id);
            removed_count += 1;
        }
    }

    if removed_count > 0 {
        info!("✅ Position cleanup complete. Removed {} expired position entries.", removed_count / 2);
    }
}


#[tokio::main]
async fn main() -> Result<()> {
    let clob_host = "clob.polymarket.com";
    let gamma_host = "gamma-api.polymarket.com";

    let mut client_builder = reqwest::Client::builder()
        .user_agent("Mozilla/5.0")
        .timeout(config::http_timeout())
        .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
        .pool_idle_timeout(Some(std::time::Duration::from_secs(90)))
        .pool_max_idle_per_host(10);

    if let Ok(mut addrs) = tokio::net::lookup_host(format!("{}:443", clob_host)).await {
        if let Some(addr) = addrs.next() { client_builder = client_builder.resolve(clob_host, addr); }
    }
    if let Ok(mut addrs) = tokio::net::lookup_host(format!("{}:443", gamma_host)).await {
        if let Some(addr) = addrs.next() { client_builder = client_builder.resolve(gamma_host, addr); }
    }

    let shared_http = Arc::new(client_builder.build()?);
    dotenv::dotenv().ok();
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).init();
    ring::default_provider().install_default().expect("rustls provider");

    let crypto_filter = env::var("CRYPTO_FILTER").unwrap_or_else(|_| "btc".to_string()).to_lowercase();
    let private_key = env::var(PRIVATE_KEY_VAR).expect("POLYMARKET_PRIVATE_KEY");
    let trade_size_usdc: Decimal = env::var("TRADE_SIZE_USDC").unwrap_or_else(|_| "10".to_string()).parse()?;
    let momentum_trade_size_usdc: Decimal = env::var("MOMENTUM_TRADE_SIZE_USDC").unwrap_or_else(|_| "5".to_string()).parse()?;

    let tg_token = env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default();
    let tg_chat_id = env::var("TELEGRAM_CHAT_ID").unwrap_or_default();

    let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
    let eoa_address = signer.address();
    info!("Trading wallet (EOA) address: {}", eoa_address);

    let trading_client = Arc::new(ClobClient::new(config::CLOB_API_BASE, Config::default())?
        .authentication_builder(&signer)
        .signature_type(SignatureType::GnosisSafe)
        .authenticate()
        .await?);

    // For GnosisSafe, the funder (maker) is the Safe address
    let safe_address = derive_safe_wallet(eoa_address, POLYGON).expect("Safe derivation failed");
    info!("Authenticated on Polymarket CLOB. Safe (Maker) address: {}", safe_address);

    // FIX: Manual Nonce Fetch via REST from the Maker (Safe) Address
    let initial_nonce = fetch_next_nonce(&shared_http, safe_address).await.unwrap_or(0);
    info!("🔄 Initialized Nonce from API (Maker/Safe): {}", initial_nonce);
    let nonce_manager = Arc::new(Mutex::new(initial_nonce));

    let starting_collateral = Arc::new(Mutex::new(dec!(0.0)));
    let (balance_tx, balance_rx) = watch::channel(dec!(0));

    let mut startup_balance = dec!(0);
    for i in 1..=3 {
        info!("🔄 Initializing portfolio balance (Attempt {}/3)...", i);
        let mut req = BalanceAllowanceRequest::default();
        req.asset_type = AssetType::Collateral;
        match trading_client.balance_allowance(req).await {
            Ok(resp) => {
                startup_balance = Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(1)) / dec!(1_000_000);
                if startup_balance > dec!(0) { break; }
            },
            Err(e) => warn!("⚠️ Balance fetch failed: {:?}", e),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    *starting_collateral.lock().await = startup_balance;
    let _ = balance_tx.send(startup_balance);
    info!("📈 Starting portfolio value: ${:.2}", startup_balance);

    let trading_client_balance = Arc::clone(&trading_client);
    let balance_tx_bg = balance_tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let mut req = BalanceAllowanceRequest::default();
            req.asset_type = AssetType::Collateral;
            if let Ok(resp) = trading_client_balance.balance_allowance(req).await {
                let usdc = Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000);
                let _ = balance_tx_bg.send(usdc);
            }
        }
    });

    let (oracle_tx, oracle_rx) = watch::channel(dec!(0));
    let (velocity_tx, velocity_rx) = watch::channel(dec!(0));
    let crypto_symbol = crypto_filter.clone();
    tokio::spawn(async move {
        let binance_pair = match crypto_symbol.as_str() {
            "eth" => "ethusdt",
            "sol" => "solusdt",
            _ => "btcusdt",
        };
        let url_str = format!("wss://stream.binance.com:9443/ws/{}@ticker", binance_pair);
        let mut price_history: VecDeque<(Instant, Decimal)> = VecDeque::new();

        loop {
            if let Ok((mut ws_stream, _)) = connect_async(&url_str).await {
                info!("📡 Connected to Binance Oracle for {}", binance_pair.to_uppercase());
                while let Some(Ok(msg)) = ws_stream.next().await {
                    if let Message::Text(text) = msg {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(price_str) = v.get("c").and_then(|p| p.as_str()) {
                                if let Ok(price) = Decimal::from_str(price_str) {
                                    let now = Instant::now();
                                    let _ = oracle_tx.send(price);
                                    price_history.push_back((now, price));
                                    while let Some((t, _)) = price_history.front() {
                                        if now.duration_since(*t).as_secs() >= config::MOMENTUM_WINDOW_SECS {
                                            price_history.pop_front();
                                        } else { break; }
                                    }
                                    if let Some((_, start_price)) = price_history.front() {
                                        let delta = price - start_price;
                                        let _ = velocity_tx.send(delta);
                                        let threshold = match crypto_symbol.as_str() {
                                            "eth" => config::ETH_MOMENTUM_THRESHOLD,
                                            "sol" => config::SOL_MOMENTUM_THRESHOLD,
                                            _ => config::BTC_MOMENTUM_THRESHOLD,
                                        };
                                        if delta.abs() >= threshold {
                                            info!("🔥 MOMENTUM SIGNAL: {} moved ${:.2} in last {}s", binance_pair.to_uppercase(), delta, config::MOMENTUM_WINDOW_SECS);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            warn!("⚠️ Binance Oracle disconnected. Reconnecting in 5s...");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });

    let positions: Arc<Mutex<HashMap<U256, Position>>> = Arc::new(Mutex::new(HashMap::new()));
    let total_pnl: Arc<Mutex<Decimal>> = Arc::new(Mutex::new(dec!(0)));

    let (initial_yes, initial_no, name, _, desc, _, close_time) = loop {
        let candidate = get_top_market(&shared_http).await;
        if candidate.0 != U256::ZERO { break candidate; }
        tokio::time::sleep(std::time::Duration::from_secs(90)).await;
    };

    info!("🧪 Initializing market: {}", name);
    let mut initial_strike = extract_strike_price_from_name(&name);
    if initial_strike.is_none() {
        info!("🔎 Name strike not found, attempting historical description lookup...");
        initial_strike = fetch_historical_strike_price(&shared_http, &crypto_filter, &desc).await;
    }
    if initial_strike.is_none() {
         info!("🔎 Still no strike, trying to parse name as date...");
         initial_strike = fetch_historical_strike_price(&shared_http, &crypto_filter, &name).await;
    }

    let (market_tx, mut market_rx) = watch::channel((initial_yes, initial_no, name, close_time, initial_strike, desc));

    let http_monitor = Arc::clone(&shared_http);
    let market_tx_monitor = market_tx.clone();
    let crypto_filter_monitor = crypto_filter.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(90));
        loop {
            interval.tick().await;
            let candidate = get_top_market(&http_monitor).await;
            if candidate.0 == U256::ZERO { continue; }
            let (cur_yes, _, cur_name, _, _, _) = market_tx_monitor.borrow().clone();
            if candidate.0 != cur_yes || candidate.2 != cur_name {
                info!("🔄 Market Switch Detected: {} -> {}", cur_name, candidate.2);
                let mut strike = extract_strike_price_from_name(&candidate.2);
                if strike.is_none() {
                    strike = fetch_historical_strike_price(&http_monitor, &crypto_filter_monitor, &candidate.4).await;
                }
                if strike.is_none() {
                    strike = fetch_historical_strike_price(&http_monitor, &crypto_filter_monitor, &candidate.2).await;
                }
                let _ = market_tx_monitor.send((candidate.0, candidate.1, candidate.2.clone(), candidate.6, strike, candidate.4.clone()));
            }
        }
    });

    loop {
        let (yes_token, no_token, market_name, _market_close_time, strike_price, _) = market_rx.borrow().clone();
        info!("🚀 Starting Arbitrage Scalper on market: \"{}\"", market_name);

        // Pre-fetch and cache fee rates and neg_risk status to remove from critical path
        let yes_fee_rate = trading_client.fee_rate_bps(yes_token).await.map(|r| r.base_fee).unwrap_or(0);
        let no_fee_rate = trading_client.fee_rate_bps(no_token).await.map(|r| r.base_fee).unwrap_or(0);

        // Determine correct exchange contract for the domain based on neg_risk
        let is_neg_risk = trading_client.neg_risk(yes_token).await.map(|r| r.neg_risk).unwrap_or(false);
        let verifying_contract = if is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };

        info!("✅ Cached Settings: NegRisk: {} | Exchange: {} | YES fee {} bps | NO fee {} bps", is_neg_risk, verifying_contract, yes_fee_rate, no_fee_rate);

        let (yes_price_tx, yes_price_rx) = watch::channel::<PriceState>((dec!(0), dec!(1), dec!(0)));
        let (no_price_tx, no_price_rx) = watch::channel::<PriceState>((dec!(0), dec!(1), dec!(0)));

        for (token, tx) in [(yes_token, yes_price_tx), (no_token, no_price_tx)] {
            tokio::spawn(async move {
                loop {
                    let client = WsClient::default();
                    let stream = match client.subscribe_orderbook(vec![token]) {
                        Ok(s) => s,
                        Err(_) => { tokio::time::sleep(std::time::Duration::from_secs(5)).await; continue; }
                    };
                    let mut stream = Box::pin(stream);
                    info!("✅ WS orderbook subscribed for token {}", token);
                    while let Some(book_result) = stream.next().await {
                        if let Ok(book) = book_result {
                            let bid = book.bids.iter().map(|l| l.price).max().unwrap_or(dec!(0));
                            let (ask, depth) = book.asks.iter()
                                .min_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
                                .map(|l| (l.price, l.size))
                                .unwrap_or((dec!(1), dec!(0)));
                            let _ = tx.send((bid, ask, depth));
                        } else { break; }
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            });
        }

        let mut trade_cooldown = Utc::now();
        let mut ticker = interval(std::time::Duration::from_millis(100));
        let mut status_ticker = interval(std::time::Duration::from_secs(60));
        let mut cleanup_ticker = interval(std::time::Duration::from_secs(300));
        let mut pulse_ticker = interval(std::time::Duration::from_secs(300));

        let mut momentum_fired_for_current_market = false;
        let mut consecutive_momentum_signals = 0u32;

        loop {
            tokio::select! {
                _ = pulse_ticker.tick() => {
                    let start = Instant::now();
                    let mut req = BalanceAllowanceRequest::default();
                    req.asset_type = AssetType::Collateral;
                    let _ = trading_client.balance_allowance(req).await;
                    info!("📍 Network Pulse: {:?} (Bot -> PolyMarket)", start.elapsed());
                }
                _ = cleanup_ticker.tick() => {
                    cleanup_expired_positions(Arc::clone(&positions), market_name.clone(), yes_token, no_token, _market_close_time).await;
                }
                _ = status_ticker.tick() => {
                    let (_, yes_ask, _) = *yes_price_rx.borrow();
                    let (_, no_ask, _) = *no_price_rx.borrow();
                    let binance_price = *oracle_rx.borrow();
                    let binance_velocity = *velocity_rx.borrow();

                    if let Some(strike) = strike_price {
                        let strike_info = format!(" | Strike: ${:.2} | Diff: ${:.2} | Velocity: ${:.2}", strike, binance_price - strike, binance_velocity);
                        if yes_ask != dec!(1) && no_ask != dec!(1) {
                            info!("💓 Heartbeat | Poly Sum ${:.4} | Binance: ${:.2}{}", yes_ask + no_ask, binance_price, strike_info);
                        }
                    } else {
                        debug!("🔎 Market scanning: Strike Unknown for {}", market_name);
                    }
                }
                _ = ticker.tick() => {
                    if Utc::now() < trade_cooldown { continue; }

                    let (yes_bid, yes_ask, yes_ask_depth) = *yes_price_rx.borrow();
                    let (no_bid, no_ask, no_ask_depth) = *no_price_rx.borrow();

                    if yes_ask == dec!(1) || no_ask == dec!(1) { continue; }

                    // --- Momentum Take Profit Logic (EXPLICITLY FIRST) ---
                    {
                        let mut pos_map = positions.lock().await;
                        let yes_pos = pos_map.get(&yes_token).cloned();
                        let no_pos  = pos_map.get(&no_token).cloned();

                        let mut exit_token = None;
                        let mut exit_price = dec!(0);
                        let mut exit_shares = dec!(0);
                        let mut exit_fee_rate = 0;

                        let velocity = *velocity_rx.borrow();
                        let threshold = match crypto_filter.as_str() {
                            "eth" => config::ETH_MOMENTUM_THRESHOLD,
                            "sol" => config::SOL_MOMENTUM_THRESHOLD,
                            _ => config::BTC_MOMENTUM_THRESHOLD,
                        };
                        let reversal_threshold = threshold * config::MOMENTUM_REVERSAL_RATIO;

                        if let Some(yp) = yes_pos {
                            if yp.shares > dec!(0) {
                                let profit_margin = if yp.avg_entry > dec!(0) { (yes_bid - yp.avg_entry) / yp.avg_entry } else { dec!(0) };
                                let target = if yp.avg_entry >= dec!(0.70) { dec!(0.05) } else { config::MOMENTUM_TARGET_PROFIT_PERCENT };
                                let stop_loss = -config::MOMENTUM_STOP_LOSS_PERCENT;

                                if profit_margin >= target || yes_bid >= config::MOMENTUM_TAKE_PROFIT_CEILING {
                                    info!("🎯 Momentum YES Target Reached (Bid: ${:.2}, Profit: {:.2}% vs Target: {:.2}%) - Taking Profit", yes_bid, profit_margin * dec!(100), target * dec!(100));
                                    exit_token = Some(yes_token);
                                    exit_price = (yes_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                    exit_shares = yp.shares;
                                    exit_fee_rate = yes_fee_rate;
                                } else if profit_margin <= stop_loss {
                                    info!("🛑 Momentum YES Stop Loss Hit (Bid: ${:.2}, Loss: {:.2}%)", yes_bid, profit_margin * dec!(100));
                                    exit_token = Some(yes_token);
                                    exit_price = (yes_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                    exit_shares = yp.shares;
                                    exit_fee_rate = yes_fee_rate;
                                } else if velocity < reversal_threshold {
                                    info!("📉 Momentum YES Reversal Detected (Velocity: ${:.2} < Threshold: ${:.2})", velocity, reversal_threshold);
                                    exit_token = Some(yes_token);
                                    exit_price = (yes_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                    exit_shares = yp.shares;
                                    exit_fee_rate = yes_fee_rate;
                                }
                            }
                        }

                        if exit_token.is_none() {
                            if let Some(np) = no_pos {
                                if np.shares > dec!(0) {
                                    let profit_margin = if np.avg_entry > dec!(0) { (no_bid - np.avg_entry) / np.avg_entry } else { dec!(0) };
                                    let target = if np.avg_entry >= dec!(0.70) { dec!(0.05) } else { config::MOMENTUM_TARGET_PROFIT_PERCENT };
                                    let stop_loss = -config::MOMENTUM_STOP_LOSS_PERCENT;

                                    if profit_margin >= target || no_bid >= config::MOMENTUM_TAKE_PROFIT_CEILING {
                                        info!("🎯 Momentum NO Target Reached (Bid: ${:.2}, Profit: {:.2}% vs Target: {:.2}%) - Taking Profit", no_bid, profit_margin * dec!(100), target * dec!(100));
                                        exit_token = Some(no_token);
                                        exit_price = (no_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                        exit_shares = np.shares;
                                        exit_fee_rate = no_fee_rate;
                                    } else if profit_margin <= stop_loss {
                                        info!("🛑 Momentum NO Stop Loss Hit (Bid: ${:.2}, Loss: {:.2}%)", no_bid, profit_margin * dec!(100));
                                        exit_token = Some(no_token);
                                        exit_price = (no_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                        exit_shares = np.shares;
                                        exit_fee_rate = no_fee_rate;
                                    } else if velocity > -reversal_threshold {
                                        info!("📉 Momentum NO Reversal Detected (Velocity: ${:.2} > -${:.2})", velocity, reversal_threshold);
                                        exit_token = Some(no_token);
                                        exit_price = (no_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                        exit_shares = np.shares;
                                        exit_fee_rate = no_fee_rate;
                                    }
                                }
                            }
                        }

                        if let Some(token) = exit_token {
                            let client = Arc::clone(&trading_client);
                            let signer = signer.clone();
                            let nm = Arc::clone(&nonce_manager);
                            let sh = Arc::clone(&shared_http);
                            let owner = client.credentials().key();
                            let tt = tg_token.clone();
                            let tc = tg_chat_id.clone();
                            let pos_handle = Arc::clone(&positions);
                            tokio::spawn(async move {
                                let mut current_shares = exit_shares;
                                for attempt in 0..3 {
                                    if current_shares < config::MIN_ORDER_SHARES { return Ok(()); }

                                    let mut guard = nm.lock().await;
                                    let current_nonce = *guard;

                                    let mut order_struct = Order::default();
                                    order_struct.salt = U256::from(Utc::now().timestamp_millis() & ((1 << 53) - 1));
                                    order_struct.maker = safe_address;
                                    order_struct.signer = eoa_address;
                                    order_struct.taker = Address::ZERO;
                                    order_struct.tokenId = token;
                                    // Round shares to 2 decimals to comply with Polymarket's precision requirements
                                    let rounded_shares = current_shares.round_dp(2);
                                    order_struct.makerAmount = U256::from(to_fixed_u128(rounded_shares));
                                    order_struct.takerAmount = U256::from(to_fixed_u128(rounded_shares * exit_price));
                                    order_struct.expiration = U256::ZERO;
                                    order_struct.nonce = U256::from(current_nonce);
                                    order_struct.feeRateBps = U256::from(exit_fee_rate);
                                    order_struct.side = Side::Sell as u8;
                                    order_struct.signatureType = SignatureType::GnosisSafe as u8;

                                    let domain = Eip712Domain {
                                        name: Some(Cow::Borrowed(ORDER_NAME)),
                                        version: Some(Cow::Borrowed(VERSION)),
                                        chain_id: Some(U256::from(POLYGON)),
                                        verifying_contract: Some(verifying_contract),
                                        ..Eip712Domain::default()
                                    };

                                    let hash = order_struct.eip712_signing_hash(&domain);
                                    if let Ok(signature) = signer.sign_hash(&hash).await {
                                        let signed_order = SignedOrder::builder()
                                            .order(order_struct)
                                            .signature(signature)
                                            .order_type(OrderType::FAK)
                                            .owner(owner)
                                            .build();

                                        match client.post_order(signed_order).await {
                                            Ok(_) => {
                                                info!("🚀 LIVE MOMENTUM EXIT FILLED: {} shares of token {} @ ${:.2}", current_shares, token, exit_price);
                                                let mut pm = pos_handle.lock().await;
                                                pm.remove(&token);
                                                *guard += 1;
                                                return Ok(());
                                            },
                                            Err(e) => {
                                                let err_msg = format!("{:?}", e).to_lowercase();
                                                drop(guard);

                                                if err_msg.contains("invalid nonce") && attempt < 2 {
                                                    warn!("⚠️ Invalid nonce in momentum exit. Re-syncing for Maker {}...", safe_address);
                                                    sync_nonce_manager(&nm, &sh, safe_address).await;
                                                    continue;
                                                } else if (err_msg.contains("not enough balance") || err_msg.contains("not enough allowance")) && attempt < 2 {
                                                    if let Some(actual_balance) = parse_balance_from_error(&err_msg) {
                                                        if actual_balance == dec!(0) {
                                                            warn!("⚠️ Balance is 0 in momentum exit (likely indexer lag). Waiting 2s and retrying...");
                                                            // Don't give up! The indexer might just be catching up
                                                            // Sleep and retry instead of clearing the position
                                                            tokio::time::sleep(Duration::from_millis(2000)).await;
                                                            continue;
                                                        }
                                                        warn!("⚠️ Balance mismatch in momentum exit. Retrying with actual balance: {}", actual_balance);
                                                        current_shares = actual_balance;
                                                        continue;
                                                    }
                                                }
                                                let msg = format!("❌ [RustPolyBot] Momentum Exit Order Failed (Attempt {}): {:?}", attempt + 1, e);
                                                let _ = send_notification(&tt, &tc, &msg).await;
                                                error!("{}", msg);
                                                return Err(anyhow::anyhow!(msg));
                                            }
                                        }
                                    } else {
                                        drop(guard);
                                        let msg = format!("❌ [RustPolyBot] Momentum Exit Order Signing Failed (Attempt {}): {:?}", attempt + 1, token);
                                        let _ = send_notification(&tt, &tc, &msg).await;
                                        error!("{}", msg);
                                        return Err(anyhow::anyhow!(msg));
                                    }
                                }
                                Err(anyhow::anyhow!("Max retries reached for momentum exit"))
                            });
                            // Optimistic clear locally
                            if let Some(p) = pos_map.get_mut(&token) { p.shares = dec!(0); }
                            trade_cooldown = Utc::now() + chrono::Duration::seconds(config::TRADE_COOLDOWN_SECS);
                            continue;
                        }
                    }

                    // --- Momentum Trading Logic (One-Sided) ---
                    if config::ENABLE_MOMENTUM_TRADING && !momentum_fired_for_current_market {
                        let velocity = *velocity_rx.borrow();
                        let binance_price = *oracle_rx.borrow();
                        let threshold = match crypto_filter.as_str() {
                            "eth" => config::ETH_MOMENTUM_THRESHOLD,
                            "sol" => config::SOL_MOMENTUM_THRESHOLD,
                            _ => config::BTC_MOMENTUM_THRESHOLD,
                        };
                        let strike_buffer = match crypto_filter.as_str() {
                            "eth" => config::ETH_STRIKE_BUFFER,
                            "sol" => config::SOL_STRIKE_BUFFER,
                            _ => config::BTC_STRIKE_BUFFER,
                        };

                        if let Some(strike) = strike_price {
                            let mut momentum_token = None;
                            let mut limit_price = dec!(0);
                            let mut target_depth = dec!(0);
                            let mut fee_rate = 0;

                            let mut current_signal_token = None;
                            if velocity > threshold && binance_price > (strike + strike_buffer) && yes_ask <= config::MAX_MOMENTUM_ENTRY_PRICE {
                                current_signal_token = Some(yes_token);
                            } else if velocity < -threshold && binance_price < (strike - strike_buffer) && no_ask <= config::MAX_MOMENTUM_ENTRY_PRICE {
                                current_signal_token = Some(no_token);
                            } else {
                                // Log why momentum signal was rejected at entry level
                                if velocity.abs() >= threshold {
                                    let reason = if velocity > threshold {
                                        if binance_price <= (strike + strike_buffer) {
                                            format!("Price ${:.2} not above strike+buffer ${:.2}", binance_price, strike + strike_buffer)
                                        } else if yes_ask > config::MAX_MOMENTUM_ENTRY_PRICE {
                                            format!("YES ask ${:.2} exceeds max entry price ${:.2}", yes_ask, config::MAX_MOMENTUM_ENTRY_PRICE)
                                        } else {
                                            String::new()
                                        }
                                    } else {
                                        if binance_price >= (strike - strike_buffer) {
                                            format!("Price ${:.2} not below strike-buffer ${:.2}", binance_price, strike - strike_buffer)
                                        } else if no_ask > config::MAX_MOMENTUM_ENTRY_PRICE {
                                            format!("NO ask ${:.2} exceeds max entry price ${:.2}", no_ask, config::MAX_MOMENTUM_ENTRY_PRICE)
                                        } else {
                                            String::new()
                                        }
                                    };
                                    if !reason.is_empty() {
                                        debug!("⏭️ Momentum signal rejected at filter stage: {} (Velocity: ${:.2}, Threshold: ${:.2})", reason, velocity, threshold);
                                    }
                                }
                            }

                            if let Some(token) = current_signal_token {
                                consecutive_momentum_signals += 1;
                                if consecutive_momentum_signals >= config::MOMENTUM_CONFIRMATION_TICKS {
                                    let has_opposing_pos = {
                                        let pos = positions.lock().await;
                                        let opposing_token = if token == yes_token { no_token } else { yes_token };
                                        pos.get(&opposing_token).map(|p| p.shares).unwrap_or(dec!(0)) > dec!(0)
                                    };

                                    if has_opposing_pos {
                                        info!("⏭️ MOMENTUM REJECTED: Already holding opposing position in token {} - Cannot enter new momentum trade", if token == yes_token { no_token } else { yes_token });
                                    } else if !has_opposing_pos {
                                        momentum_token = Some(token);
                                        limit_price = if token == yes_token {
                                            (yes_ask + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE).round_dp(2)
                                        } else {
                                            (no_ask + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE).round_dp(2)
                                        };
                                        target_depth = if token == yes_token { yes_ask_depth } else { no_ask_depth };
                                        fee_rate = if token == yes_token { yes_fee_rate } else { no_fee_rate };
                                    }
                                } else {
                                    debug!("⏳ Momentum signal confirmed, waiting for additional confirmation ({} of {} ticks required)", consecutive_momentum_signals, config::MOMENTUM_CONFIRMATION_TICKS);
                                }
                                } else {
                                    consecutive_momentum_signals = 0;
                                    debug!("⏸️  Momentum signal lost (was at {} ticks)", consecutive_momentum_signals);
                                }

                            if let Some(token) = momentum_token {
                                let current_usdc_balance = *balance_rx.borrow();
                                if current_usdc_balance >= momentum_trade_size_usdc {
                                    let target_shares = (momentum_trade_size_usdc / limit_price).floor();

                                    // CRITICAL: Re-validate entry conditions before placing trade
                                    // Market conditions may have changed since signal confirmation
                                    let should_proceed = if token == yes_token {
                                        velocity > threshold && binance_price > (strike + strike_buffer) && yes_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                                    } else {
                                        velocity < -threshold && binance_price < (strike - strike_buffer) && no_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                                    };

                                    if !should_proceed {
                                        debug!("⏭️ MOMENTUM ENTRY CANCELLED: Market conditions changed (Velocity: ${:.2}, Price: ${:.2} vs Strike+Buffer ${:.2})",
                                            velocity, binance_price, if token == yes_token { strike + strike_buffer } else { strike - strike_buffer });
                                        momentum_token = None;
                                        consecutive_momentum_signals = 0;
                                        continue;
                                    }

                                    if target_depth < (target_shares * config::MIN_LIQUIDITY_FILL_RATIO) {
                                        warn!("⏭️ MOMENTUM REJECTED: Insufficient liquidity at ask (Available: {:.2} shares, Target: {:.2} shares, Ratio: {:.2})", target_depth, target_shares, config::MIN_LIQUIDITY_FILL_RATIO);
                                        continue;
                                    }

                                    if target_shares >= config::MIN_ORDER_SHARES {
                                        let current_exposure = {
                                            let pos = positions.lock().await;
                                            pos.values().map(|p| p.shares * p.avg_entry).sum::<Decimal>()
                                        };

                                        if current_exposure + momentum_trade_size_usdc <= config::MAX_EXPOSURE_PER_TOKEN_USDC {
                                            info!("🎯 MOMENTUM BUY SIGNAL CONFIRMED ({} ticks): Binance ${:.2} {} Strike (Velocity: ${:.2}) -> {}",
                                                consecutive_momentum_signals, binance_price, if token == yes_token { ">" } else { "<" }, velocity, if token == yes_token { "YES" } else { "NO" });
                                            info!("💰 [MOMENTUM SIGNAL] token {} @ ${:.2} (Size: ${:.2})", token, limit_price, momentum_trade_size_usdc);

                                            if !config::GHOST_MODE {
                                                let client = Arc::clone(&trading_client);
                                                let signer = signer.clone();
                                                let nonce_manager = Arc::clone(&nonce_manager);
                                                let amount = to_fixed_u128(target_shares);
                                                let positions_handle = Arc::clone(&positions);
                                                let market_name_handle = market_name.clone();
                                                let close_time_handle = _market_close_time;
                                                let pair_token_handle = if token == yes_token { no_token } else { yes_token };
                                                let shared_http_handle = Arc::clone(&shared_http);
                                                let owner = client.credentials().key();

                                                tokio::spawn(async move {
                                                    for attempt in 0..2 {
                                                        let mut guard = nonce_manager.lock().await;
                                                        let current_nonce = *guard;

                                                        let mut order_struct = Order::default();
                                                        order_struct.salt = U256::from(Utc::now().timestamp_millis() & ((1 << 53) - 1));
                                                        order_struct.maker = safe_address; order_struct.signer = eoa_address; order_struct.tokenId = token;
                                                        order_struct.makerAmount = U256::from(to_fixed_u128(target_shares * limit_price));
                                                        order_struct.takerAmount = U256::from(amount);
                                                        order_struct.expiration = U256::ZERO; order_struct.nonce = U256::from(current_nonce);
                                                        order_struct.feeRateBps = U256::from(fee_rate);
                                                        order_struct.side = Side::Buy as u8; order_struct.signatureType = SignatureType::GnosisSafe as u8;
                                                        let domain = Eip712Domain { name: Some(Cow::Borrowed(ORDER_NAME)), version: Some(Cow::Borrowed(VERSION)), chain_id: Some(U256::from(POLYGON)), verifying_contract: Some(verifying_contract), ..Eip712Domain::default() };
                                                        let hash = order_struct.eip712_signing_hash(&domain);
                                                        if let Ok(signature) = signer.sign_hash(&hash).await {
                                                            let signed_order = SignedOrder::builder().order(order_struct).signature(signature).order_type(OrderType::FAK).owner(owner).build();
                                                            match client.post_order(signed_order).await {
                                                                Ok(_) => {
                                                                    info!("🚀 Momentum order accepted. Syncing final quantity...");
                                                                    {
                                                                        let mut pm = positions_handle.lock().await;
                                                                        pm.insert(token, Position { shares: target_shares, avg_entry: limit_price, opened_at: Utc::now(), close_time: close_time_handle, market_name: market_name_handle, pair_token_id: pair_token_handle });
                                                                    }
                                                                    *guard += 1;
                                                                    let _ = sync_position_balance(&client, &positions_handle, token).await;
                                                                    break;
                                                                },
                                                                Err(e) => {
                                                                    let err_msg = format!("{:?}", e).to_lowercase();
                                                                    drop(guard);
                                                                    if err_msg.contains("invalid nonce") && attempt == 0 {
                                                                        warn!("⚠️ Invalid nonce in momentum entry. Re-syncing for Maker {}...", safe_address);
                                                                        sync_nonce_manager(&nonce_manager, &shared_http_handle, safe_address).await;
                                                                        continue;
                                                                    }
                                                                    // Extract specific error reason for clarity
                                                                    let reason = if err_msg.contains("no orders found") {
                                                                        "No liquidity/sellers at price".to_string()
                                                                    } else if err_msg.contains("insufficient balance") {
                                                                        "Insufficient maker balance".to_string()
                                                                    } else if err_msg.contains("invalid signature") {
                                                                        "Invalid signature".to_string()
                                                                    } else if err_msg.contains("expired") {
                                                                        "Order expired".to_string()
                                                                    } else {
                                                                        format!("{:?}", e)
                                                                    };
                                                                    error!("❌ FAK Order Rejected: {} @ ${:.2}", reason, limit_price);
                                                                    break;
                                                                }
                                                            }
                                                        } else { break; }
                                                    }
                                                });
                                            }
                                            momentum_fired_for_current_market = true;
                                            trade_cooldown = Utc::now() + chrono::Duration::seconds(config::TRADE_COOLDOWN_SECS);
                                            info!("🔒 Momentum cooldown active: Next eligible trade in {}s (until {})", config::TRADE_COOLDOWN_SECS, trade_cooldown.format("%H:%M:%S UTC"));
                                            continue;
                                        } else {
                                            warn!("⏭️ MOMENTUM REJECTED: Exposure limit exceeded (Current: ${:.2}, + Trade: ${:.2} = ${:.2}, Max: ${:.2})", current_exposure, momentum_trade_size_usdc, current_exposure + momentum_trade_size_usdc, config::MAX_EXPOSURE_PER_TOKEN_USDC);
                                            continue;
                                        }
                                    } else {
                                        warn!("⏭️ MOMENTUM REJECTED: Min order size not met (Calculated: {:.2} shares vs Min: {:.2})", target_shares, config::MIN_ORDER_SHARES);
                                        continue;
                                    }
                                } else {
                                    warn!("⏭️ Momentum trade rejected: Insufficient balance (Have: ${:.2}, Need: ${:.2})", current_usdc_balance, momentum_trade_size_usdc);
                                    continue;
                                }
                            }
                        }
                    }

                    // --- Arbitrage Logic ---
                    let combined_ask = yes_ask + no_ask;
                    let profit_margin_no_fees = dec!(1.0) - combined_ask;
                    let yes_fee = yes_ask * (Decimal::from(yes_fee_rate) / dec!(10_000));
                    let no_fee = no_ask * (Decimal::from(no_fee_rate) / dec!(10_000));
                    let profit_margin = profit_margin_no_fees - (yes_fee + no_fee);

                    if profit_margin >= config::ARBITRAGE_PROFIT_THRESHOLD {
                        let current_usdc_balance = *balance_rx.borrow();
                        if current_usdc_balance < trade_size_usdc * dec!(2) {
                            debug!("⏭️ ARB REJECTED: Insufficient balance (Have: ${:.2}, Need: ${:.2} for 2-leg arb)", current_usdc_balance, trade_size_usdc * dec!(2));
                            continue;
                        }

                        let target_shares = (trade_size_usdc / combined_ask).floor();
                        if target_shares < config::MIN_ORDER_SHARES {
                            debug!("⏭️ ARB REJECTED: Min order size not met (Calculated: {:.2} shares vs Min: {:.2})", target_shares, config::MIN_ORDER_SHARES);
                            continue;
                        }

                        if yes_ask_depth < (target_shares * config::MIN_LIQUIDITY_FILL_RATIO) || no_ask_depth < (target_shares * config::MIN_LIQUIDITY_FILL_RATIO) {
                            debug!("⏭️ ARB REJECTED: Insufficient liquidity (YES depth: {:.2}, NO depth: {:.2}, Need: {:.2} each)", yes_ask_depth, no_ask_depth, target_shares * config::MIN_LIQUIDITY_FILL_RATIO);
                            continue;
                        }

                        let current_exposure = {
                            let pos = positions.lock().await;
                            pos.values().map(|p| p.shares * p.avg_entry).sum::<Decimal>()
                        };

                        let risk_engine = RiskEngine::new();
                        if !risk_engine.approve_buy(yes_ask, no_ask, current_exposure, trade_size_usdc, *starting_collateral.lock().await, *total_pnl.lock().await) {
                            continue;
                        }

                        let yes_limit_price = (yes_ask + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE).round_dp(2);
                        let no_limit_price  = (no_ask + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE).round_dp(2);

                        if !config::GHOST_MODE {
                            let client_clone = Arc::clone(&trading_client);
                            let signer_clone = signer.clone();
                            let nonce_manager_clone = Arc::clone(&nonce_manager);
                            let shared_http_clone = Arc::clone(&shared_http);
                            let pos_handle = Arc::clone(&positions);
                            let owner = client_clone.credentials().key();

                            let yes_task = {
                                let client = Arc::clone(&client_clone);
                                let signer = signer_clone.clone();
                                let nonce_manager = Arc::clone(&nonce_manager_clone);
                                let shared_http = Arc::clone(&shared_http_clone);
                                async move {
                                    for attempt in 0..2 {
                                        let mut guard = nonce_manager.lock().await;
                                        let current_nonce = *guard;
                                        let mut order_struct = Order::default();
                                        order_struct.salt = U256::from(Utc::now().timestamp_millis() & ((1 << 53) - 1));
                                        order_struct.maker = safe_address; order_struct.signer = eoa_address; order_struct.tokenId = yes_token;
                                        order_struct.makerAmount = U256::from(to_fixed_u128(target_shares * yes_limit_price));
                                        order_struct.takerAmount = U256::from(to_fixed_u128(target_shares));
                                        order_struct.expiration = U256::ZERO; order_struct.nonce = U256::from(current_nonce);
                                        order_struct.feeRateBps = U256::from(yes_fee_rate);
                                        order_struct.side = Side::Buy as u8; order_struct.signatureType = SignatureType::GnosisSafe as u8;
                                        let domain = Eip712Domain { name: Some(Cow::Borrowed(ORDER_NAME)), version: Some(Cow::Borrowed(VERSION)), chain_id: Some(U256::from(POLYGON)), verifying_contract: Some(verifying_contract), ..Eip712Domain::default() };
                                        let hash = order_struct.eip712_signing_hash(&domain);
                                        if let Ok(signature) = signer.sign_hash(&hash).await {
                                            let signed_order = SignedOrder::builder().order(order_struct).signature(signature).order_type(OrderType::FAK).owner(owner).build();
                                            match client.post_order(signed_order).await {
                                                Ok(_) => { *guard += 1; return Ok(()); },
                                                Err(e) => {
                                                    drop(guard);
                                                    if format!("{:?}", e).to_lowercase().contains("invalid nonce") && attempt == 0 {
                                                        warn!("⚠️ Invalid nonce in arbitrage YES buy. Re-syncing for Maker {}...", safe_address);
                                                        sync_nonce_manager(&nonce_manager, &shared_http, safe_address).await;
                                                        continue;
                                                    }
                                                    return Err(anyhow::anyhow!("{:?}", e));
                                                }
                                            }
                                        } else { return Err(anyhow::anyhow!("Signing failed")); }
                                    }
                                    Err(anyhow::anyhow!("Max retries reached"))
                                }
                            };
                            let no_task = {
                                let client = Arc::clone(&client_clone);
                                let signer = signer_clone.clone();
                                let nonce_manager = Arc::clone(&nonce_manager_clone);
                                let shared_http = Arc::clone(&shared_http_clone);
                                async move {
                                    for attempt in 0..2 {
                                        let mut guard = nonce_manager.lock().await;
                                        let current_nonce = *guard;
                                        let mut order_struct = Order::default();
                                        order_struct.salt = U256::from(Utc::now().timestamp_millis() & ((1 << 53) - 1));
                                        order_struct.maker = safe_address; order_struct.signer = eoa_address; order_struct.tokenId = no_token;
                                        order_struct.makerAmount = U256::from(to_fixed_u128(target_shares * no_limit_price));
                                        order_struct.takerAmount = U256::from(to_fixed_u128(target_shares));
                                        order_struct.expiration = U256::ZERO; order_struct.nonce = U256::from(current_nonce);
                                        order_struct.feeRateBps = U256::from(no_fee_rate);
                                        order_struct.side = Side::Buy as u8; order_struct.signatureType = SignatureType::GnosisSafe as u8;
                                        let domain = Eip712Domain { name: Some(Cow::Borrowed(ORDER_NAME)), version: Some(Cow::Borrowed(VERSION)), chain_id: Some(U256::from(POLYGON)), verifying_contract: Some(verifying_contract), ..Eip712Domain::default() };
                                        let hash = order_struct.eip712_signing_hash(&domain);
                                        if let Ok(signature) = signer.sign_hash(&hash).await {
                                            let signed_order = SignedOrder::builder().order(order_struct).signature(signature).order_type(OrderType::FAK).owner(owner).build();
                                            match client.post_order(signed_order).await {
                                                Ok(_) => { *guard += 1; return Ok(()); },
                                                Err(e) => {
                                                    drop(guard);
                                                    if format!("{:?}", e).to_lowercase().contains("invalid nonce") && attempt == 0 {
                                                        warn!("⚠️ Invalid nonce in arbitrage NO buy. Re-syncing for Maker {}...", safe_address);
                                                        sync_nonce_manager(&nonce_manager, &shared_http, safe_address).await;
                                                        continue;
                                                    }
                                                    return Err(anyhow::anyhow!("{:?}", e));
                                                }
                                            }
                                        } else { return Err(anyhow::anyhow!("Signing failed")); }
                                    }
                                    Err(anyhow::anyhow!("Max retries reached"))
                                }
                            };
                            let (yes_res, no_res) = tokio::join!(yes_task, no_task);
                            if yes_res.is_ok() && no_res.is_ok() {
                                {
                                    let mut pm = pos_handle.lock().await;
                                    let now = Utc::now();
                                    pm.insert(yes_token, Position { shares: target_shares, avg_entry: yes_limit_price, opened_at: now, close_time: _market_close_time, market_name: market_name.clone(), pair_token_id: no_token });
                                    pm.insert(no_token, Position { shares: target_shares, avg_entry: no_limit_price, opened_at: now, close_time: _market_close_time, market_name: market_name.clone(), pair_token_id: yes_token });
                                }
                                info!("📈 Arb legs accepted. Syncing final quantities...");
                                let _ = tokio::join!(sync_position_balance(&client_clone, &pos_handle, yes_token), sync_position_balance(&client_clone, &pos_handle, no_token));
                                {
                                    let mut pnl_guard = total_pnl.lock().await;
                                    *pnl_guard += target_shares * profit_margin;
                                }
                                trade_cooldown = Utc::now() + chrono::Duration::seconds(config::TRADE_COOLDOWN_SECS);
                            }
                        }
                    }

                    // --- Perfect Hedge Early Exit Logic ---
                    {
                        let pos_map = positions.lock().await;
                        let yes_pos = pos_map.get(&yes_token).cloned();
                        let no_pos  = pos_map.get(&no_token).cloned();
                        if let (Some(yp), Some(np)) = (yes_pos, no_pos) {
                            if yp.shares > dec!(0) && np.shares > dec!(0) {
                                let combined_bid = yes_bid + no_bid;
                                if combined_bid >= config::EARLY_EXIT_COMBINED_BID_THRESHOLD {
                                    info!("💰 Bids reached target early exit (sum ${:.4})", combined_bid);
                                    let exit_price_yes = (yes_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                    let exit_price_no  = (no_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                    let client_clone = Arc::clone(&trading_client);
                                    let signer_clone = signer.clone();
                                    let nonce_manager_clone = Arc::clone(&nonce_manager);
                                    let shared_http_clone = Arc::clone(&shared_http);
                                    let pos_handle = Arc::clone(&positions);
                                    let owner = client_clone.credentials().key();

                                    let yes_exit_task = {
                                        let client = Arc::clone(&client_clone);
                                        let signer = signer_clone.clone();
                                        let nonce_manager = Arc::clone(&nonce_manager_clone);
                                        let shared_http = Arc::clone(&shared_http_clone);
                                        let pos_handle = Arc::clone(&pos_handle);
                                        async move {
                                            let mut current_shares = yp.shares;
                                            for attempt in 0..3 {
                                                if current_shares < config::MIN_ORDER_SHARES { return Ok(()); }
                                                let mut guard = nonce_manager.lock().await;
                                                let current_nonce = *guard;
                                                let mut order_struct = Order::default();
                                                order_struct.salt = U256::from(Utc::now().timestamp_millis() & ((1 << 53) - 1));
                                                order_struct.maker = safe_address; order_struct.signer = eoa_address; order_struct.tokenId = yes_token;
                                                order_struct.makerAmount = U256::from(to_fixed_u128(current_shares));
                                                order_struct.takerAmount = U256::from(to_fixed_u128(current_shares * exit_price_yes));
                                                order_struct.expiration = U256::ZERO; order_struct.nonce = U256::from(current_nonce);
                                                order_struct.feeRateBps = U256::from(yes_fee_rate);
                                                order_struct.side = Side::Sell as u8; order_struct.signatureType = SignatureType::GnosisSafe as u8;
                                                let domain = Eip712Domain { name: Some(Cow::Borrowed(ORDER_NAME)), version: Some(Cow::Borrowed(VERSION)), chain_id: Some(U256::from(POLYGON)), verifying_contract: Some(verifying_contract), ..Eip712Domain::default() };
                                                let hash = order_struct.eip712_signing_hash(&domain);
                                                if let Ok(signature) = signer.sign_hash(&hash).await {
                                                    let signed_order = SignedOrder::builder().order(order_struct).signature(signature).order_type(OrderType::FAK).owner(owner).build();
                                                    match client.post_order(signed_order).await {
                                                        Ok(_) => { *guard += 1; return Ok(()); },
                                                        Err(e) => {
                                                            let err_msg = format!("{:?}", e).to_lowercase();
                                                            drop(guard);
                                                            if err_msg.contains("invalid nonce") && attempt < 2 {
                                                                warn!("⚠️ Invalid nonce in YES early exit. Re-syncing for Maker {}...", safe_address);
                                                                sync_nonce_manager(&nonce_manager, &shared_http, safe_address).await;
                                                                continue;
                                                            } else if (err_msg.contains("not enough balance") || err_msg.contains("not enough allowance")) && attempt < 2 {
                                                                 if let Some(actual_balance) = parse_balance_from_error(&err_msg) {
                                                                     if actual_balance == dec!(0) {
                                                                         warn!("⚠️ Balance is 0 in YES early exit (likely indexer lag). Waiting 2s and retrying...");
                                                                         tokio::time::sleep(Duration::from_millis(2000)).await;
                                                                         continue;
                                                                     }
                                                                     current_shares = actual_balance; continue;
                                                                 }
                                                             }
                                                            return Err(anyhow::anyhow!("{:?}", e));
                                                        }
                                                    }
                                                } else { return Err(anyhow::anyhow!("Signing failed")); }
                                            }
                                            Err(anyhow::anyhow!("Max retries reached"))
                                        }
                                    };
                                    let no_exit_task = {
                                        let client = Arc::clone(&client_clone);
                                        let signer = signer_clone.clone();
                                        let nonce_manager = Arc::clone(&nonce_manager_clone);
                                        let shared_http = Arc::clone(&shared_http_clone);
                                        let pos_handle = Arc::clone(&pos_handle);
                                        async move {
                                            let mut current_shares = np.shares;
                                            for attempt in 0..3 {
                                                if current_shares < config::MIN_ORDER_SHARES { return Ok(()); }
                                                let mut guard = nonce_manager.lock().await;
                                                let current_nonce = *guard;
                                                let mut order_struct = Order::default();
                                                order_struct.salt = U256::from(Utc::now().timestamp_millis() & ((1 << 53) - 1));
                                                order_struct.maker = safe_address; order_struct.signer = eoa_address; order_struct.tokenId = no_token;
                                                order_struct.makerAmount = U256::from(to_fixed_u128(current_shares));
                                                order_struct.takerAmount = U256::from(to_fixed_u128(current_shares * exit_price_no));
                                                order_struct.expiration = U256::ZERO; order_struct.nonce = U256::from(current_nonce);
                                                order_struct.feeRateBps = U256::from(no_fee_rate);
                                                order_struct.side = Side::Sell as u8;
                                                order_struct.signatureType = SignatureType::GnosisSafe as u8;

                                                let domain = Eip712Domain { name: Some(Cow::Borrowed(ORDER_NAME)), version: Some(Cow::Borrowed(VERSION)), chain_id: Some(U256::from(POLYGON)), verifying_contract: Some(verifying_contract), ..Eip712Domain::default() };
                                                let hash = order_struct.eip712_signing_hash(&domain);
                                                if let Ok(signature) = signer.sign_hash(&hash).await {
                                                    let signed_order = SignedOrder::builder().order(order_struct).signature(signature).order_type(OrderType::FAK).owner(owner).build();
                                                    match client.post_order(signed_order).await {
                                                        Ok(_) => { *guard += 1; return Ok(()); },
                                                        Err(e) => {
                                                            let err_msg = format!("{:?}", e).to_lowercase();
                                                            drop(guard);
                                                            if err_msg.contains("invalid nonce") && attempt < 2 {
                                                                warn!("⚠️ Invalid nonce in NO early exit. Re-syncing for Maker {}...", safe_address);
                                                                sync_nonce_manager(&nonce_manager, &shared_http, safe_address).await;
                                                                continue;
                                                            } else if (err_msg.contains("not enough balance") || err_msg.contains("not enough allowance")) && attempt < 2 {
                                                                 if let Some(actual_balance) = parse_balance_from_error(&err_msg) {
                                                                     if actual_balance == dec!(0) {
                                                                         warn!("⚠️ Balance is 0 in NO early exit (likely indexer lag). Waiting 2s and retrying...");
                                                                         tokio::time::sleep(Duration::from_millis(2000)).await;
                                                                         continue;
                                                                     }
                                                                     current_shares = actual_balance; continue;
                                                                 }
                                                             }
                                                            return Err(anyhow::anyhow!("{:?}", e));
                                                        }
                                                    }
                                                } else { return Err(anyhow::anyhow!("Signing failed")); }
                                            }
                                            Err(anyhow::anyhow!("Max retries reached"))
                                        }
                                    };
                                    drop(pos_map);
                                    let (yes_res, no_res) = tokio::join!(yes_exit_task, no_exit_task);
                                    if yes_res.is_ok() && no_res.is_ok() {
                                        let mut pm = positions.lock().await;
                                        pm.remove(&yes_token); pm.remove(&no_token);
                                        trade_cooldown = Utc::now() + chrono::Duration::seconds(config::TRADE_COOLDOWN_SECS);
                                    }
                                }
                            }
                        }
                    }
                }
                _ = market_rx.changed() => {
                    let new_market = market_rx.borrow().clone();
                    if new_market.0 != yes_token {
                        info!("🔄 Market switch detected! Restarting loop.");
                        break;
                    }
                }
            }
        }
    }
}
