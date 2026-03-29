use anyhow::Result;

use polymarket_client_sdk::clob::{Client as ClobClient, Config};
use polymarket_client_sdk::clob::types::{Amount, OrderType, Side, SignatureType};
use polymarket_client_sdk::{POLYGON, PRIVATE_KEY_VAR};
use polymarket_client_sdk::clob::types::request::{
    BalanceAllowanceRequest,
};
use polymarket_client_sdk::clob::types::AssetType;

use futures::StreamExt as _;
use polymarket_client_sdk::clob::ws::Client as WsClient;

use alloy::primitives::U256;
use alloy::signers::local::LocalSigner;

use chrono::{DateTime, Utc, TimeZone, Datelike};
use chrono_tz::US::Eastern;
use reqwest;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use std::collections::HashMap;
use std::env;
use std::str::FromStr as _;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tokio::time::{interval, Instant, Duration};

use tracing::{error, info, warn, debug};

use rustpolybot::config;
use rustpolybot::risk::RiskEngine;

use rustls::crypto::ring;

use alloy::signers::Signer;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use regex::Regex;

type PriceState = (Decimal, Decimal);

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
        let month_str = cap.get(1)?.as_str();
        let day: u32 = cap.get(2)?.as_str().parse().ok()?;
        let year: i32 = 2000 + cap.get(3)?.as_str().parse::<i32>().ok()?;
        let hour: u32 = cap.get(4)?.as_str().parse().ok()?;
        let min: u32 = cap.get(5)?.as_str().parse().ok()?;

        let month = match month_str {
            "jan" => 1, "feb" => 2, "mar" => 3, "apr" => 4, "may" => 5, "jun" => 6,
            "jul" => 7, "aug" => 8, "sep" => 9, "oct" => 10, "nov" => 11, "dec" => 12,
            _ => return None,
        };
        (year, month, day, hour, min)
    } else if let Some(cap) = re2.captures(&lower_text) {
        let month_str = cap.get(1)?.as_str();
        let day: u32 = cap.get(2)?.as_str().parse().ok()?;
        let mut hour: u32 = cap.get(3)?.as_str().parse().ok()?;
        let min: u32 = cap.get(4).map(|m| m.as_str().parse().unwrap_or(0)).unwrap_or(0);
        let ampm = cap.get(5)?.as_str();

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


async fn get_top_market(http: &reqwest::Client) -> (U256, U256, String, String, String, bool, Option<DateTime<Utc>>) {
    let crypto_filter = env::var("CRYPTO_FILTER")
        .unwrap_or_else(|_| "all".to_string())
        .to_lowercase();

    info!("🚀 Scanning Gamma API for markets (FILTER: {})", crypto_filter);

    let candidates = fetch_simplified_crypto_candidates(http, &crypto_filter).await;

    if candidates.is_empty() {
        warn!("⚠️ No valid markets found matching filters.");
        return (U256::ZERO, U256::ZERO, String::new(), String::new(), String::new(), false, None);
    }

    let mut sorted = candidates;
    sorted.sort_by(|a, b| {
        let now = Utc::now();
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
    let yes_token = best.0[0];
    let no_token = best.0[1];

    info!("🏆 Selected market: \"{}\"", best.1);
    (yes_token, no_token, best.1.clone(), best.2.clone(), best.6.clone(), best.4, best.5)
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
            let description = market.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let lower = name.to_lowercase();

            if !config::is_crypto_market(&lower) { continue; }
            if config::is_bad_market(&name) { continue; }
            if !get_enable_orderbook(market) { continue; }

            if !lower.contains("up or down") { continue; }
            if !is_short_term_window(&name) { continue; }

            let token_ids = extract_token_ids_u256(market);
            if token_ids.len() < 2 { continue; }

            let volume = market.get("volume24hrClob").and_then(value_to_f64)
                .or_else(|| market.get("volume24hr").and_then(value_to_f64))
                .unwrap_or(0.0);

            if volume < config::MIN_MARKET_VOLUME { continue; }

            let close_time = extract_close_time(&serde_json::Value::Null, market);
            let seconds_left = close_time.map_or(0i64, |ct| (ct - now).num_seconds());

            if seconds_left < config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY { continue; }
            if seconds_left > config::MAX_SECONDS_TO_EXPIRY_FOR_ENTRY { continue; }

            let matches_filter = match crypto_filter {
                "btc" | "bitcoin" => lower.contains("bitcoin") || lower.contains("btc"),
                "eth" | "ethereum" => lower.contains("ethereum") || lower.contains("eth"),
                "sol" | "solana" => lower.contains("solana") || lower.contains("sol"),
                _ => true,
            };
            if !matches_filter { continue; }

            let link = if let Some(slug) = market.get("slug").and_then(|v| v.as_str()) {
                format!("https://polymarket.com/{}", slug)
            } else {
                "https://polymarket.com/".to_string()
            };

            let hot = config::is_high_priority_text(&name);
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
    let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
    info!("Trading wallet address: {}", signer.address());

    let trading_client = Arc::new(ClobClient::new(config::CLOB_API_BASE, Config::default())?
        .authentication_builder(&signer)
        .signature_type(SignatureType::Eoa)
        .authenticate()
        .await?);
    info!("Authenticated on Polymarket CLOB: {}", trading_client.address());

    let starting_collateral = Arc::new(Mutex::new(dec!(0.0)));
    let (balance_tx, mut balance_rx) = watch::channel(dec!(0));

    {
        let mut req = BalanceAllowanceRequest::default();
        req.asset_type = AssetType::Collateral;
        if let Ok(resp) = trading_client.balance_allowance(req).await {
            let usdc = Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000);
            *starting_collateral.lock().await = usdc;
            let _ = balance_tx.send(usdc);
            info!("📈 Starting portfolio value: ${:.2}", usdc);
        }
    }

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
    let crypto_symbol = crypto_filter.clone();
    tokio::spawn(async move {
        let binance_pair = match crypto_symbol.as_str() {
            "eth" => "ethusdt",
            "sol" => "solusdt",
            _ => "btcusdt",
        };
        let url_str = format!("wss://stream.binance.com:9443/ws/{}@ticker", binance_pair);
        loop {
            if let Ok((mut ws_stream, _)) = connect_async(&url_str).await {
                info!("📡 Connected to Binance Oracle for {}", binance_pair.to_uppercase());
                while let Some(Ok(msg)) = ws_stream.next().await {
                    if let Message::Text(text) = msg {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(price_str) = v.get("c").and_then(|p| p.as_str()) {
                                if let Ok(price) = Decimal::from_str(price_str) {
                                    let _ = oracle_tx.send(price);
                                }
                            }
                        }
                    }
                }
            }
            warn!("⚠️ Binance Oracle disconnected. Reconnecting in 5s...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    let positions: Arc<Mutex<HashMap<U256, Position>>> = Arc::new(Mutex::new(HashMap::new()));
    let total_pnl: Arc<Mutex<Decimal>> = Arc::new(Mutex::new(dec!(0)));
    let mut consecutive_failures = 0;

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
                    info!("🔎 Name strike not found for {}, attempting historical description lookup...", candidate.2);
                    strike = fetch_historical_strike_price(&http_monitor, &crypto_filter_monitor, &candidate.4).await;
                }
                if strike.is_none() {
                    info!("🔎 Still no strike for {}, trying to parse name as date...", candidate.2);
                    strike = fetch_historical_strike_price(&http_monitor, &crypto_filter_monitor, &candidate.2).await;
                }
                let _ = market_tx_monitor.send((candidate.0, candidate.1, candidate.2.clone(), candidate.6, strike, candidate.4.clone()));
            }
        }
    });

    loop {
        let (yes_token, no_token, market_name, _market_close_time, strike_price, _) = market_rx.borrow().clone();
        info!("🚀 Starting Arbitrage Scalper on market: \"{}\"", market_name);

        let (yes_price_tx, yes_price_rx) = watch::channel::<PriceState>((dec!(0), dec!(1)));
        let (no_price_tx, no_price_rx) = watch::channel::<PriceState>((dec!(0), dec!(1)));

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
                            let ask = book.asks.iter().map(|l| l.price).min().unwrap_or(dec!(1));
                            let _ = tx.send((bid, ask));
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

        loop {
            tokio::select! {
                _ = pulse_ticker.tick() => {
                    let start = Instant::now();
                    let mut req = BalanceAllowanceRequest::default();
                    req.asset_type = AssetType::Collateral;
                    let _ = trading_client.balance_allowance(req).await;
                    info!("📍 Network Pulse: {:?} (AWS Montreal -> Virginia)", start.elapsed());
                }
                _ = cleanup_ticker.tick() => {
                    cleanup_expired_positions(Arc::clone(&positions), market_name.clone(), yes_token, no_token, _market_close_time).await;
                }
                _ = status_ticker.tick() => {
                    let (_, yes_ask) = *yes_price_rx.borrow();
                    let (_, no_ask) = *no_price_rx.borrow();
                    let binance_price = *oracle_rx.borrow();

                    let strike_info = if let Some(strike) = strike_price {
                        format!(" | Strike: ${:.2} | Diff: ${:.2}", strike, binance_price - strike)
                    } else {
                        " | Strike: Unknown".to_string()
                    };

                    if yes_ask != dec!(1) && no_ask != dec!(1) {
                        info!("💓 Heartbeat | Poly Sum ${:.4} | Binance: ${:.2}{}", yes_ask + no_ask, binance_price, strike_info);
                    }
                }
                _ = ticker.tick() => {
                    if Utc::now() < trade_cooldown { continue; }

                    let (yes_bid, yes_ask) = *yes_price_rx.borrow();
                    let (no_bid, no_ask) = *no_price_rx.borrow();

                    if yes_ask == dec!(1) || no_ask == dec!(1) { continue; }

                    let combined_ask = yes_ask + no_ask;
                    let profit_margin = dec!(1.0) - combined_ask;

                    if profit_margin >= config::ARBITRAGE_PROFIT_THRESHOLD {
                        let arb_signal_start = Instant::now();

                        if consecutive_failures >= config::MAX_CONSECUTIVE_FAILURES {
                            error!("🛑 FATAL: 3 consecutive failures. Emergency stopping.");
                            std::process::exit(1);
                        }

                        let current_usdc_balance = *balance_rx.borrow();
                        if current_usdc_balance < trade_size_usdc * dec!(2) {
                            warn!("📉 Insufficient cached USDC (${:.2}).", current_usdc_balance);
                            trade_cooldown = Utc::now() + chrono::Duration::seconds(60);
                            continue;
                        }

                        info!("💰 Arb opportunity! Margin: {:.4}¢", profit_margin * dec!(100));

                        let target_shares = (trade_size_usdc / combined_ask).floor();
                        if target_shares < config::MIN_ORDER_SHARES { continue; }
                        if (target_shares * yes_ask < config::MIN_ORDER_USDC) || (target_shares * no_ask < config::MIN_ORDER_USDC) { continue; }

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

                        let amount_res = Amount::shares(target_shares);
                        let amount = match amount_res {
                            Ok(a) => a,
                            Err(e) => { error!("❌ Failed to create Amount: {}", e); continue; }
                        };

                        let yes_task = {
                            let client = Arc::clone(&trading_client);
                            let signer = signer.clone();
                            let token = yes_token;
                            let amt = amount.clone();
                            async move {
                                let order = client.market_order().token_id(token).amount(amt).side(Side::Buy).price(yes_limit_price).order_type(OrderType::FAK).build().await?;
                                let signed = client.sign(&signer, order).await?;
                                client.post_order(signed).await
                            }
                        };

                        let no_task = {
                            let client = Arc::clone(&trading_client);
                            let signer = signer.clone();
                            let token = no_token;
                            let amt = amount.clone();
                            async move {
                                let order = client.market_order().token_id(token).amount(amt).side(Side::Buy).price(no_limit_price).order_type(OrderType::FAK).build().await?;
                                let signed = client.sign(&signer, order).await?;
                                client.post_order(signed).await
                            }
                        };

                        let (yes_res, no_res) = tokio::join!(yes_task, no_task);

                        let network_total_time = arb_signal_start.elapsed();

                        let yes_filled = match yes_res {
                            Ok(r) => {
                                let filled = Decimal::from_str(&r.making_amount.to_string()).unwrap_or(dec!(0)) / config::SHARE_SCALE;
                                if filled > dec!(0) { filled } else { Decimal::from_str(&r.taking_amount.to_string()).unwrap_or(dec!(0)) / config::SHARE_SCALE }
                            },
                            Err(e) => { error!("❌ YES Post Failed: {:?}", e); dec!(0) }
                        }.round_dp(2);

                        let no_filled = match no_res {
                            Ok(r) => {
                                let filled = Decimal::from_str(&r.making_amount.to_string()).unwrap_or(dec!(0)) / config::SHARE_SCALE;
                                if filled > dec!(0) { filled } else { Decimal::from_str(&r.taking_amount.to_string()).unwrap_or(dec!(0)) / config::SHARE_SCALE }
                            },
                            Err(e) => { error!("❌ NO Post Failed: {:?}", e); dec!(0) }
                        }.round_dp(2);

                        if yes_filled > dec!(0) && no_filled > dec!(0) {
                            consecutive_failures = 0;
                            let hedged_shares = yes_filled.min(no_filled);
                            let locked_profit = hedged_shares * profit_margin;

                            let approx_cost = (yes_filled * yes_limit_price) + (no_filled * no_limit_price);
                            let _ = balance_tx.send(current_usdc_balance - approx_cost);

                            {
                                let mut pos_map = positions.lock().await;
                                let now = Utc::now();
                                pos_map.entry(yes_token).or_insert_with(|| Position { shares: dec!(0), avg_entry: yes_limit_price, opened_at: now, close_time: _market_close_time, market_name: market_name.clone(), pair_token_id: no_token }).shares += yes_filled;
                                pos_map.entry(no_token).or_insert_with(|| Position { shares: dec!(0), avg_entry: no_limit_price,  opened_at: now, close_time: _market_close_time, market_name: market_name.clone(), pair_token_id: yes_token }).shares += no_filled;
                            }
                            {
                                let mut pnl = total_pnl.lock().await;
                                *pnl += locked_profit;
                            }
                            info!("📈 BOTH LEGS FILLED (Parallel) — Profit ${:.4} | Latency: {:?}", locked_profit, network_total_time);

                            if yes_filled != no_filled {
                                warn!("⚖️ HEDGE IMBALANCE: YES filled {:.2}, NO filled {:.2}. Flattening excess...", yes_filled, no_filled);
                                if yes_filled > no_filled {
                                    let excess = yes_filled - no_filled;
                                    let exit_price = (yes_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                    if let Ok(amt) = Amount::shares(excess) {
                                        if let Ok(order) = trading_client.market_order().token_id(yes_token).amount(amt).side(Side::Sell).price(exit_price).order_type(OrderType::FAK).build().await {
                                            if let Ok(signed) = trading_client.sign(&signer, order).await { let _ = trading_client.post_order(signed).await; }
                                        }
                                    }
                                } else {
                                    let excess = no_filled - yes_filled;
                                    let exit_price = (no_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                    if let Ok(amt) = Amount::shares(excess) {
                                        if let Ok(order) = trading_client.market_order().token_id(no_token).amount(amt).side(Side::Sell).price(exit_price).order_type(OrderType::FAK).build().await {
                                            if let Ok(signed) = trading_client.sign(&signer, order).await { let _ = trading_client.post_order(signed).await; }
                                        }
                                    }
                                }
                            }
                            trade_cooldown = Utc::now() + chrono::Duration::seconds(config::TRADE_COOLDOWN_SECS);
                        } else {
                            consecutive_failures += 1;
                            warn!("⚠️ Trade Failure ({}/3) | Latency: {:?}", consecutive_failures, network_total_time);

                            let (latest_yes_bid, _) = *yes_price_rx.borrow();
                            let (latest_no_bid, _) = *no_price_rx.borrow();

                            if yes_filled > dec!(0) {
                                let exit_price = (latest_yes_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                if let Ok(amt) = Amount::shares(yes_filled) {
                                    if let Ok(order) = trading_client.market_order().token_id(yes_token).amount(amt).side(Side::Sell).price(exit_price).order_type(OrderType::FAK).build().await {
                                        if let Ok(signed) = trading_client.sign(&signer, order).await {
                                            let _ = trading_client.post_order(signed).await;
                                        }
                                    }
                                }
                            }
                            if no_filled > dec!(0) {
                                let exit_price = (latest_no_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                if let Ok(amt) = Amount::shares(no_filled) {
                                    if let Ok(order) = trading_client.market_order().token_id(no_token).amount(amt).side(Side::Sell).price(exit_price).order_type(OrderType::FAK).build().await {
                                        if let Ok(signed) = trading_client.sign(&signer, order).await {
                                            let _ = trading_client.post_order(signed).await;
                                        }
                                    }
                                }
                            }
                            trade_cooldown = Utc::now() + chrono::Duration::seconds(60);
                        }
                    }

                    {
                        let pos_map = positions.lock().await;
                        let yes_pos = pos_map.get(&yes_token);
                        let no_pos  = pos_map.get(&no_token);

                        if let (Some(yes_pos), Some(no_pos)) = (yes_pos, no_pos) {
                            if yes_pos.shares > dec!(0) && no_pos.shares > dec!(0) {
                                let combined_bid = yes_bid + no_bid;
                                if combined_bid >= config::EARLY_EXIT_COMBINED_BID_THRESHOLD {
                                    info!("💰 Bids reached target (sum ${:.4}) — early exit", combined_bid);

                                    let exit_price_yes = (yes_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                    let exit_price_no  = (no_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);

                                    if let (Ok(amt_yes), Ok(amt_no)) = (Amount::shares(yes_pos.shares), Amount::shares(no_pos.shares)) {
                                        let res_yes = trading_client.market_order().token_id(yes_token).amount(amt_yes).side(Side::Sell).price(exit_price_yes).order_type(OrderType::FAK).build().await;
                                        let res_no  = trading_client.market_order().token_id(no_token).amount(amt_no).side(Side::Sell).price(exit_price_no).order_type(OrderType::FAK).build().await;

                                        if let (Ok(oy), Ok(on)) = (res_yes, res_no) {
                                            if let (Ok(sy), Ok(sn)) = (trading_client.sign(&signer, oy).await, trading_client.sign(&signer, on).await) {
                                                let _ = tokio::join!(trading_client.post_order(sy), trading_client.post_order(sn));
                                            }
                                        }

                                        let mut pos_map = positions.lock().await;
                                        if let Some(p) = pos_map.get_mut(&yes_token) { p.shares = dec!(0); }
                                        if let Some(p) = pos_map.get_mut(&no_token) { p.shares = dec!(0); }
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