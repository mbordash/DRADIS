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

use chrono::{DateTime, Utc};
use reqwest;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use std::collections::HashMap;
use std::env;
use std::str::FromStr as _;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tokio::time::{interval, Instant}; // Added Instant

use tracing::{error, info, warn};

use rustpolybot::config;
use rustpolybot::risk::RiskEngine;

use rustls::crypto::ring;

use alloy::signers::Signer;

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


async fn get_top_market(http: &reqwest::Client) -> (U256, U256, String, String, bool, Option<DateTime<Utc>>) {
    let crypto_filter = env::var("CRYPTO_FILTER")
        .unwrap_or_else(|_| "all".to_string())
        .to_lowercase();

    info!("🚀 Starting sniper with CRYPTO_FILTER = '{}' (looking for current hourly crypto market)", crypto_filter);

    let candidates = fetch_simplified_crypto_candidates(http, &crypto_filter).await;

    info!("✅ Total candidates after filters: {}", candidates.len());

    if candidates.is_empty() {
        info!("⏸️ No suitable hourly crypto market right now — staying flat");
        return (U256::ZERO, U256::ZERO, String::new(), String::new(), false, None);
    }

    let mut sorted = candidates;
    sorted.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal));

    let best = &sorted[0];
    let yes_token = best.0[0];
    let no_token = best.0[1];

    info!("🏆 Selected market by volume: \"{}\"", best.1);
    info!("   → {}", best.2);
    info!("   → YES Token: {}", yes_token);
    info!("   → NO Token: {}", no_token);

    (yes_token, no_token, best.1.clone(), best.2.clone(), best.4, best.5)
}

async fn fetch_simplified_crypto_candidates(
    http: &reqwest::Client,
    crypto_filter: &str,
) -> Vec<(Vec<U256>, String, String, f64, bool, Option<DateTime<Utc>>)> {
    let mut out = vec![];
    let now = Utc::now();

    let mut seen = 0usize;
    let mut skipped_not_crypto = 0usize;
    let mut skipped_bad = 0usize;
    let mut skipped_ultra_short = 0usize;
    let mut skipped_no_orderbook = 0usize;
    let mut skipped_no_tokens = 0usize;
    let mut skipped_low_volume = 0usize;
    let mut skipped_filter = 0usize;
    let mut skipped_not_updown = 0usize;
    let mut skipped_long_window = 0usize;

    let mut candidates_debug: Vec<(String, f64, i64)> = vec![];

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

        for market in markets {
            seen += 1;
            let name = market.get("question").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let lower = name.to_lowercase();

            if !config::is_crypto_market(&lower) { skipped_not_crypto += 1; continue; }
            if config::is_bad_market(&name) { skipped_bad += 1; continue; }
            if !get_enable_orderbook(market) { skipped_no_orderbook += 1; continue; }

            if !lower.contains("up or down") {
                skipped_not_updown += 1;
                continue;
            }

            if !is_short_term_window(&name) {
                skipped_long_window += 1;
                continue;
            }

            let token_ids = extract_token_ids_u256(market);
            if token_ids.len() < 2 { skipped_no_tokens += 1; continue; }

            let volume = market.get("volume24hrClob").and_then(value_to_f64)
                .or_else(|| market.get("volume24hr").and_then(value_to_f64))
                .unwrap_or(0.0);

            if volume < config::MIN_MARKET_VOLUME {
                skipped_low_volume += 1;
                continue;
            }

            let close_time = extract_close_time(&serde_json::Value::Null, market);
            let seconds_left = close_time.map_or(0i64, |ct| (ct - now).num_seconds());

            if seconds_left < config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY {
                skipped_ultra_short += 1;
                continue;
            }
            if seconds_left > config::MAX_SECONDS_TO_EXPIRY_FOR_ENTRY {
                skipped_long_window += 1;
                continue;
            }

            let matches_filter = match crypto_filter {
                "btc" | "bitcoin" => lower.contains("bitcoin") || lower.contains("btc"),
                "eth" | "ethereum" => lower.contains("ethereum") || lower.contains("eth"),
                "sol" | "solana" => lower.contains("solana") || lower.contains("sol"),
                _ => true,
            };
            if !matches_filter { skipped_filter += 1; continue; }

            let link = if let Some(slug) = market.get("slug").and_then(|v| v.as_str()) {
                format!("https://polymarket.com/{}", slug)
            } else {
                "https://polymarket.com/".to_string()
            };

            let hot = config::is_high_priority_text(&name);
            out.push((token_ids, name.clone(), link, volume, hot, close_time));

            candidates_debug.push((name, volume, seconds_left));
        }
    }

    info!("🧨 Simplified crypto scan summary:");
    info!("   Seen: {} | Not crypto: {} | Bad: {} | Ultra-short: {} | No orderbook: {}",
          seen, skipped_not_crypto, skipped_bad, skipped_ultra_short, skipped_no_orderbook);
    info!("   Not 'up or down': {} | Long-window: {} | No tokens: {} | Low Volume: {} | Filter mismatch: {}",
          skipped_not_updown, skipped_long_window, skipped_no_tokens, skipped_low_volume, skipped_filter);
    info!("   ✅ Found {} valid candidates", out.len());

    candidates_debug.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    info!("📋 Top 5 short-term Up-or-Down markets considered:");
    for (i, (name, vol, secs)) in candidates_debug.iter().take(5).enumerate() {
        let minutes = *secs as f64 / 60.0;
        info!("   #{} | ${:.0} vol | {:.1} min left → {}", i+1, vol, minutes, name);
    }

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

    // Check current market first
    if let Some(ct) = market_close_time {
        if ct < now {
            if pos_map.contains_key(&yes_token) {
                tokens_to_remove.push(yes_token);
            }
            if pos_map.contains_key(&no_token) {
                tokens_to_remove.push(no_token);
            }
        }
    }

    // Iterate through all other positions to find expired ones
    for (token_id, pos) in pos_map.iter() {
        if let Some(ct) = pos.close_time {
            if ct < now {
                tokens_to_remove.push(*token_id);
                tokens_to_remove.push(pos.pair_token_id); // Ensure the pair is also removed
            }
        }
    }

    // Remove duplicates and then remove from map
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
    // #1 Networking Optimization: Connection Pool and TCP Keep-Alive
    let http = Arc::new(reqwest::Client::builder()
        .user_agent("Mozilla/5.0")
        .timeout(config::http_timeout())
        .tcp_keepalive(Some(config::tcp_keepalive()))
        .pool_idle_timeout(Some(std::time::Duration::from_secs(90)))
        .pool_max_idle_per_host(10)
        .build()?);

    dotenv::dotenv().ok();
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).init();
    ring::default_provider().install_default().expect("rustls provider");

    let private_key = env::var(PRIVATE_KEY_VAR).expect("POLYMARKET_PRIVATE_KEY");
    let trade_size_usdc: Decimal = env::var("TRADE_SIZE_USDC").unwrap_or_else(|_| "3".to_string()).parse()?;
    let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
    info!("Trading wallet address: {}", signer.address());

    let tg_token = env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default();
    let tg_chat_id = env::var("TELEGRAM_CHAT_ID").unwrap_or_default();
    if config::ENABLE_TELEGRAM && !tg_token.is_empty() && !tg_chat_id.is_empty() {
        info!("📱 Telegram notifications ENABLED");
    } else {
        info!("📱 Telegram notifications DISABLED (missing token or chat_id)");
    }

    let trading_client = Arc::new(ClobClient::new(config::CLOB_API_BASE, Config::default())?
        .authentication_builder(&signer)
        .signature_type(SignatureType::GnosisSafe)
        .authenticate()
        .await?);
    info!("Authenticated on Polymarket CLOB: {}", trading_client.address());

    let starting_collateral = Arc::new(Mutex::new(dec!(0.0)));

    // Initialize watch channel for background balance checks
    let (balance_tx, balance_rx) = watch::channel(dec!(0));

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

    // #4 Background Balance Check Task
    let trading_client_balance = Arc::clone(&trading_client);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let mut req = BalanceAllowanceRequest::default();
            req.asset_type = AssetType::Collateral;
            if let Ok(resp) = trading_client_balance.balance_allowance(req).await {
                let usdc = Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000);
                let _ = balance_tx.send(usdc);
            }
        }
    });

    let positions: Arc<Mutex<HashMap<U256, Position>>> = Arc::new(Mutex::new(HashMap::new()));
    let total_pnl: Arc<Mutex<Decimal>> = Arc::new(Mutex::new(dec!(0)));
    let mut consecutive_failures = 0;

    let (initial_yes, initial_no, name, _, _, close_time) = loop {
        let candidate = get_top_market(&http).await;
        if candidate.0 != U256::ZERO { break candidate; }
        info!("No suitable market found. Retrying in 60s...");
        tokio::time::sleep(config::market_switch_interval()).await;
    };

    let (market_tx, mut market_rx) = watch::channel((initial_yes, initial_no, name, close_time));

    let http_monitor = Arc::clone(&http);
    let market_tx_monitor = market_tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config::market_switch_interval());
        loop {
            interval.tick().await;

            let candidate = get_top_market(&http_monitor).await;
            if candidate.0 == U256::ZERO {
                continue;
            }

            let current = market_tx_monitor.borrow().clone();
            let (cur_yes, _, cur_name, _) = current;

            if candidate.0 != cur_yes || candidate.2 != cur_name {
                info!("🔄 Market switch detected! New market: \"{}\"", candidate.2);
                let _ = market_tx_monitor.send((candidate.0, candidate.1, candidate.2.clone(), candidate.5));
            }
        }
    });

    loop {
        let (yes_token, no_token, market_name, _market_close_time) = market_rx.borrow().clone();
        info!("🚀 Starting Arbitrage Scalper on market: \"{}\"", market_name);

        let (yes_price_tx, yes_price_rx) = watch::channel::<PriceState>((dec!(0), dec!(1)));
        let (no_price_tx, no_price_rx) = watch::channel::<PriceState>((dec!(0), dec!(1)));

        for (token, tx) in [(yes_token, yes_price_tx), (no_token, no_price_tx)] {
            tokio::spawn(async move {
                loop {
                    let client = WsClient::default();
                    let stream = match client.subscribe_orderbook(vec![token]) {
                        Ok(s) => s,
                        Err(e) => {
                            error!("WS subscribe failed for {}: {}. Retrying...", token, e);
                            tokio::time::sleep(config::retry_sleep_duration()).await;
                            continue;
                        }
                    };
                    let mut stream = Box::pin(stream);
                    info!("✅ WS orderbook subscribed for token {}", token);
                    while let Some(book_result) = stream.next().await {
                        if let Ok(book) = book_result {
                            let bid = book.bids.iter().map(|l| l.price).max().unwrap_or(dec!(0));
                            let ask = book.asks.iter().map(|l| l.price).min().unwrap_or(dec!(1));
                            let _ = tx.send((bid, ask));
                        } else {
                            error!("WS stream error for {}. Reconnecting...", token);
                            break;
                        }
                    }
                    tokio::time::sleep(config::retry_sleep_duration()).await;
                }
            });
        }

        let mut trade_cooldown = Utc::now();
        let mut ticker = interval(config::main_ticker_interval());
        let mut status_ticker = interval(config::status_log_interval());
        let mut cleanup_ticker = interval(config::periodic_sync_interval());

        loop {
            tokio::select! {
                _ = cleanup_ticker.tick() => {
                    cleanup_expired_positions(
                        Arc::clone(&positions),
                        market_name.clone(),
                        yes_token,
                        no_token,
                        _market_close_time,
                    ).await;
                }
                _ = status_ticker.tick() => {
                    let (_, yes_ask) = *yes_price_rx.borrow();
                    let (_, no_ask) = *no_price_rx.borrow();
                    if yes_ask != dec!(1) && no_ask != dec!(1) {
                        let combined = yes_ask + no_ask;
                        info!("💓 Heartbeat: YES ${:.4} + NO ${:.4} = Combined Ask ${:.4}", yes_ask, no_ask, combined);
                    }
                }
                _ = ticker.tick() => {
                    if Utc::now() < trade_cooldown { continue; }

                    let (yes_bid, yes_ask) = *yes_price_rx.borrow();
                    let (no_bid, no_ask) = *no_price_rx.borrow();

                    if yes_ask == dec!(1) || no_ask == dec!(1) { continue; }

                    let combined_ask = yes_ask + no_ask;
                    let profit_margin = dec!(1.0) - combined_ask;

                    // === ARBITRAGE ENTRY ===
                    if profit_margin >= config::ARBITRAGE_PROFIT_THRESHOLD {
                        let arb_signal_start = Instant::now(); // Latency measurement start

                        // === EMERGENCY STOP CHECK ===
                        if consecutive_failures >= 3 {
                            error!("🛑 FATAL: 3 consecutive failures detected. Emergency stopping bot to protect balance.");
                            std::process::exit(1);
                        }

                        // === SAFETY GUARDS ===
                        if let Some(close_time) = _market_close_time {
                            let secs_left = (close_time - Utc::now()).num_seconds();
                            if secs_left < config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY {
                                info!("⏳ Too close to expiry ({}s left) — skipping new entry", secs_left);
                                continue;
                            }
                        }

                        let already_holding = {
                            let pos = positions.lock().await;
                            pos.get(&yes_token).map_or(dec!(0), |p| p.shares) > dec!(0) ||
                            pos.get(&no_token).map_or(dec!(0), |p| p.shares) > dec!(0)
                        };
                        if already_holding {
                            info!("⛔ Already holding position on this market — skipping new entry");
                            continue;
                        }

                        // === FAST BALANCE CHECK (FROM CACHE) ===
                        let current_usdc_balance = *balance_rx.borrow();

                        if current_usdc_balance < trade_size_usdc * dec!(2) {
                            warn!("📉 Insufficient cached USDC balance (${:.2}) for safe trade (${:.2} needed).", current_usdc_balance, trade_size_usdc * dec!(2));
                            trade_cooldown = Utc::now() + chrono::Duration::seconds(60);
                            continue;
                        }

                        info!("💰 Arb opportunity detected! Margin: {:.4}¢ (YES ${:.4} + NO ${:.4} = Combined ${:.4})",
                              profit_margin * dec!(100), yes_ask, no_ask, combined_ask);

                        let total_ask = yes_ask + no_ask;
                        let target_shares = (trade_size_usdc / total_ask).floor();

                        if target_shares < config::MIN_ORDER_SHARES {
                            info!("⏸️ Calculated target shares ({:.2}) below minimum ({}). Skipping.", target_shares, config::MIN_ORDER_SHARES);
                            continue;
                        }

                        if (target_shares * yes_ask < config::MIN_ORDER_USDC) || (target_shares * no_ask < config::MIN_ORDER_USDC) {
                            info!("⏸️ Calculated order value below minimum (${}). Skipping.", config::MIN_ORDER_USDC);
                            continue;
                        }

                        let current_exposure = {
                            let pos = positions.lock().await;
                            pos.values().map(|p| p.shares * p.avg_entry).sum::<Decimal>()
                        };

                        let session_pnl = *total_pnl.lock().await;

                        let risk_engine = RiskEngine::new();
                        if !risk_engine.approve_buy(
                            yes_ask,
                            no_ask,
                            current_exposure,
                            trade_size_usdc,
                            *starting_collateral.lock().await,
                            session_pnl
                        ) {
                            continue;
                        }

                        let yes_limit_price = (yes_ask + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE).round_dp(2);
                        let no_limit_price  = (no_ask + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE).round_dp(2);

                        let amount_res = Amount::shares(target_shares);
                        if let Err(e) = amount_res {
                            error!("❌ Failed to create Amount object: {}", e);
                            continue;
                        }
                        let amount = amount_res.unwrap();

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

                        let reaction_time = arb_signal_start.elapsed(); // Record reaction latency

                        // Execute PREP and POST in parallel for both legs
                        let (yes_res, no_res) = tokio::join!(yes_task, no_task);

                        let network_total_time = arb_signal_start.elapsed(); // Record total cycle latency

                        let yes_filled = match yes_res {
                            Ok(r) => {
                                let filled = Decimal::from_str(&r.making_amount.to_string()).unwrap_or(dec!(0)) / config::SHARE_SCALE;
                                if filled > dec!(0) { filled } else { Decimal::from_str(&r.taking_amount.to_string()).unwrap_or(dec!(0)) / config::SHARE_SCALE }
                            },
                            Err(e) => { error!("❌ YES Task Failed: {}", e); dec!(0) }
                        }.round_dp(2);

                        let no_filled = match no_res {
                            Ok(r) => {
                                let filled = Decimal::from_str(&r.making_amount.to_string()).unwrap_or(dec!(0)) / config::SHARE_SCALE;
                                if filled > dec!(0) { filled } else { Decimal::from_str(&r.taking_amount.to_string()).unwrap_or(dec!(0)) / config::SHARE_SCALE }
                            },
                            Err(e) => { error!("❌ NO Task Failed: {}", e); dec!(0) }
                        }.round_dp(2);

                        if yes_filled > dec!(0) && no_filled > dec!(0) {
                            consecutive_failures = 0;
                            let hedged_shares = yes_filled.min(no_filled);
                            let locked_profit = hedged_shares * profit_margin;
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
                            info!("📈 BOTH LEGS FILLED — Profit ${:.4} | Timing: Reaction {:?} Total {:?}", locked_profit, reaction_time, network_total_time);
                            if yes_filled != no_filled {
                                warn!("⚖️ HEDGE IMBALANCE: YES filled {:.4}, NO filled {:.4}. Imbalance: {:.4} shares", yes_filled, no_filled, (yes_filled - no_filled).abs());
                            }
                            trade_cooldown = Utc::now() + chrono::Duration::seconds(config::TRADE_COOLDOWN_SECS);
                        } else {
                            consecutive_failures += 1;
                            warn!("⚠️ Trade Failure (Fail {}/3) | Timing: Reaction {:?} Total {:?}", consecutive_failures, reaction_time, network_total_time);

                            let (latest_yes_bid, _) = *yes_price_rx.borrow();
                            let (latest_no_bid, _) = *no_price_rx.borrow();

                            if yes_filled > dec!(0) {
                                let exit_price = (latest_yes_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                if let Ok(amt) = Amount::shares(yes_filled) {
                                    let order = trading_client.market_order().token_id(yes_token).amount(amt).side(Side::Sell).price(exit_price).order_type(OrderType::FAK).build().await.unwrap();
                                    let signed = trading_client.sign(&signer, order).await.unwrap();
                                    let _ = trading_client.post_order(signed).await;
                                }
                            }
                            if no_filled > dec!(0) {
                                let exit_price = (latest_no_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                if let Ok(amt) = Amount::shares(no_filled) {
                                    let order = trading_client.market_order().token_id(no_token).amount(amt).side(Side::Sell).price(exit_price).order_type(OrderType::FAK).build().await.unwrap();
                                    let signed = trading_client.sign(&signer, order).await.unwrap();
                                    let _ = trading_client.post_order(signed).await;
                                }
                            }
                            trade_cooldown = Utc::now() + chrono::Duration::seconds(60);
                        }
                    }

                    // === PAIR CLOSURE CHECK ===
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
                                        let order_yes = trading_client.market_order().token_id(yes_token).amount(amt_yes).side(Side::Sell).price(exit_price_yes).order_type(OrderType::FAK).build().await.unwrap();
                                        let signed_yes = trading_client.sign(&signer, order_yes).await.unwrap();

                                        let order_no = trading_client.market_order().token_id(no_token).amount(amt_no).side(Side::Sell).price(exit_price_no).order_type(OrderType::FAK).build().await.unwrap();
                                        let signed_no = trading_client.sign(&signer, order_no).await.unwrap();

                                        let _ = tokio::join!(trading_client.post_order(signed_yes), trading_client.post_order(signed_no));

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
                        info!("🔄 Market switch! Restarting loop for: \"{}\"", new_market.2);
                        break;
                    }
                }
            }
        }
    }
}