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

use chrono::{DateTime, Utc};
use reqwest;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use std::collections::{HashMap, VecDeque};
use std::env;
use std::str::FromStr as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{watch, Mutex};
use tokio::time::{interval, Instant, Duration};

use tracing::{error, info, warn, debug};

use rustpolybot::config;
use rustpolybot::risk::RiskEngine;
use rustpolybot::notifications::send_notification;
use rustpolybot::strategies::momentum::MomentumStrategy;
use rustpolybot::strategies::arbitrage::ArbitrageStrategy;
use rustpolybot::strategies::time_decay::{TimeDecayStrategy, TimeDecayPosition, ThetaMode};
use rustpolybot::helpers::{
    price::*, json::*, time::*, balance::*, nonce::*
};

use rustls::crypto::ring;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
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
    fill_confirmed_at: Option<DateTime<Utc>>, // When balance was first confirmed on-chain
}

const ORDER_NAME: &str = "Polymarket CTF Exchange";
const VERSION: &str = "1";

// Verified Exchange Addresses from SDK
const EXCHANGE_NORMAL: Address = address!("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E");
const EXCHANGE_NEG_RISK: Address = address!("0xC5d563A36AE78145C45a50134d48A1215220f80a");


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
                // Mark when fill was confirmed
                if pos.fill_confirmed_at.is_none() {
                    pos.fill_confirmed_at = Some(Utc::now());
                }
            } else {
                // Balance is 0. Check if this is indexer lag or order never filled.
                let time_since_open = Utc::now() - pos.opened_at;

                if time_since_open.num_seconds() > 15 {
                    // 15+ seconds since entry order accepted, and balance still 0
                    // Order likely never filled on-chain (API accepted but CLOB didn't match)
                    warn!("⚠️ Position Sync FAILED: Token {} balance still 0 after {}s. Order likely never filled on-chain. Removing position.",
                          token_id, time_since_open.num_seconds());
                    pos_map.remove(&token_id);
                } else if pos.fill_confirmed_at.is_some() {
                    // We previously confirmed a fill, but now balance is 0?
                    // Could indicate liquidation, transfer, or indexer issue
                    warn!("⚠️ Position Sync WARNING: Token {} balance disappeared (was confirmed at {:?}). Possible liquidation or indexer issue.",
                          token_id, pos.fill_confirmed_at);
                } else {
                    // Still within 15 second window, likely just indexer lag
                    warn!("⚠️ Position Sync: Token {} balance is 0 ({}s since open). Might be indexer lag. Keeping local position.",
                          token_id, time_since_open.num_seconds());
                }
            }
        }
    }
    Ok(())
}

/// Helper function to generate market names for hourly crypto events
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

            debug!("🔍 Evaluating candidate: \"{}\" (Event: \"{}\")", name, event_title);

            // Use market validator module for comprehensive validation
            let token_ids = extract_token_ids_u256(market);
            let close_time = extract_close_time(event, market);
            let start_time = extract_start_time(event, market);
            let volume = market.get("volume24hrClob").and_then(value_to_f64)
                .or_else(|| market.get("volume24hr").and_then(value_to_f64))
                .unwrap_or(0.0);

            // Create validation context
            let validation_ctx = rustpolybot::market_validator::ValidationContext {
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
            let (is_valid, status, msg) = rustpolybot::market_validator::validate_market(
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
            let hot = config::is_high_priority_text(&name) || config::is_high_priority_text(&event_title);

            debug!("  ✅ Valid market passed all checks");
            out.push((token_ids, name.clone(), link, volume, hot, close_time, description));
        }
    }
    info!("✅ Total scanned: {} | Candidates after filters: {}", total_scanned, out.len());
    out
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

    // Log active strategies
    let strategies_active = vec![
        if config::ENABLE_MOMENTUM_TRADING { Some("🔥 Momentum Trading") } else { None },
        if config::ARBITRAGE_PROFIT_THRESHOLD > dec!(0) { Some("📈 Arbitrage Trading") } else { None },
        if config::ENABLE_TIME_DECAY_TRADING { Some("💰 Time Decay Trading") } else { None },
    ].into_iter().flatten().collect::<Vec<_>>();

    if strategies_active.is_empty() {
        warn!("⚠️ No trading strategies enabled!");
    } else {
        info!("🎯 Strategies enabled: {}", strategies_active.join(" + "));
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
    let (velocity_tx, velocity_rx) = watch::channel(dec!(0));

    // Shared state for tracking recent momentum signals to improve market selection
    let momentum_signal_times = Arc::new(tokio::sync::Mutex::new(Vec::<DateTime<Utc>>::new()));
    let momentum_signal_times_binance = Arc::clone(&momentum_signal_times);

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
                                            // Record timestamp of this momentum signal for market selection heuristics
                                            tokio::spawn({
                                                let signal_times = Arc::clone(&momentum_signal_times_binance);
                                                async move {
                                                    let mut times = signal_times.lock().await;
                                                    times.push(Utc::now());
                                                }
                                            });
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
    let time_decay_positions: Arc<Mutex<HashMap<U256, TimeDecayPosition>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let (initial_yes, initial_no, name, _, desc, _, close_time) = loop {
        let candidate = get_top_market(&shared_http).await;
        if candidate.0 != U256::ZERO { break candidate; }
        tokio::time::sleep(std::time::Duration::from_secs(90)).await;
    };

    info!("🧪 Initializing market: {}", name);
    // Use new market validator for strike extraction
    let mut initial_strike = rustpolybot::market_validator::extract_strike_price(&name);
    if initial_strike.is_none() {
        if name.to_lowercase().contains("up or down") {
            info!("📊 Binary market detected - checking description and name for reference time...");
        } else {
            info!("🔎 Name strike not found, attempting historical description lookup...");
        }
        // Always try description lookup for all market types (including binary "up or down" markets)
        initial_strike = fetch_historical_strike_price(&shared_http, &crypto_filter, &desc).await;
        // Fallback: also scan the market name itself for date/time patterns
        // e.g. "Bitcoin Up or Down - April 7, 9AM ET" → parses 9AM ET as reference time
        if initial_strike.is_none() {
            initial_strike = fetch_historical_strike_price(&shared_http, &crypto_filter, &name).await;
        }
    }
    if initial_strike.is_none() {
        info!("🔎 Using market close time to fetch strike price from Binance...");
        initial_strike = fetch_strike_price_from_close_time(&shared_http, &crypto_filter, close_time).await;
    }
    if initial_strike.is_some() {
        info!("✅ Strike price resolved: ${}", initial_strike.unwrap());
    } else {
        warn!("⚠️ Could not resolve strike price, trading may have limited signal");
    }

    let (market_tx, mut market_rx) = watch::channel((initial_yes, initial_no, name, close_time, initial_strike, desc));

    let http_monitor = Arc::clone(&shared_http);
    let market_tx_monitor = market_tx.clone();
    let crypto_filter_monitor = crypto_filter.clone();

    // Reuse the shared momentum signal times from above (not creating a new one)
    let momentum_signal_times_monitor = Arc::clone(&momentum_signal_times);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(90));
        loop {
            interval.tick().await;
            let candidate = get_top_market(&http_monitor).await;
            if candidate.0 == U256::ZERO { continue; }
            let (cur_yes, _, cur_name, cur_close_time, _, _) = market_tx_monitor.borrow().clone();

            // Same market by token ID — nothing to do
            if candidate.0 == cur_yes {
                info!("🏆 Selected market: \"{}\"", candidate.2);
                continue;
            }

            // Hysteresis: only switch when current market is truly expiring or new is clearly better
            let now_ts = Utc::now();
            let cur_secs_left = cur_close_time.map_or(9999i64, |ct| (ct - now_ts).num_seconds());
            let new_secs_left = candidate.6.map_or(9999i64, |ct| (ct - now_ts).num_seconds());

            // Check for recent momentum signals (last 60 seconds) to determine if in high-volatility period
            let mut signal_times = momentum_signal_times_monitor.lock().await;
            let cutoff_time = now_ts - chrono::Duration::seconds(60);
            signal_times.retain(|t| t > &cutoff_time);
            let recent_momentum_signals = signal_times.len();
            drop(signal_times);

            // Determine if candidate is a binary "Up or Down" market (better for momentum trading)
            let candidate_is_binary = candidate.2.to_lowercase().contains("up or down");
            let current_is_binary = cur_name.to_lowercase().contains("up or down");

            // Enhanced hysteresis: be more aggressive switching to binary markets during volatility
            let should_switch = cur_secs_left < config::FINAL_EXPIRY_WINDOW_SECS  // current expiring soon
                || cur_secs_left <= 0                                              // current already expired
                || new_secs_left > cur_secs_left + 1800                           // new has 30+ more minutes
                || (candidate_is_binary                                           // switching TO binary market
                    && !current_is_binary                                         // from non-binary (strike)
                    && recent_momentum_signals >= 5                               // during high volatility (5+ signals in 60s)
                    && new_secs_left > 600                                        // and has at least 10 min left
                    && cur_secs_left > 300);                                      // and current market won't expire immediately

            if !should_switch {
                info!("🏆 Selected market: \"{}\" (staying on current: {} — {}s vs {}s, no significant gain){}",
                    candidate.2, cur_name, cur_secs_left, new_secs_left,
                    if recent_momentum_signals > 0 { format!(" [{}x momentum signals in 60s]", recent_momentum_signals) } else { String::new() });
                continue;
            }

            info!("🔄 Market Switch Detected: {} -> {} (Current: {}s left, New: {}s left){}",
                cur_name, candidate.2, cur_secs_left, new_secs_left,
                if candidate_is_binary && !current_is_binary && recent_momentum_signals >= 5 {
                    format!(" [Switching to binary market during high volatility: {} signals in 60s]", recent_momentum_signals)
                } else {
                    String::new()
                });
            // Use new market validator for strike extraction
            let mut strike = rustpolybot::market_validator::extract_strike_price(&candidate.2);
            if strike.is_none() {
                // Try description for date/time patterns (e.g. "april 7, 9am")
                strike = fetch_historical_strike_price(&http_monitor, &crypto_filter_monitor, &candidate.4).await;
            }
            // Fallback: also scan the market name itself (e.g. "Bitcoin Up or Down - April 7, 9AM ET")
            if strike.is_none() {
                strike = fetch_historical_strike_price(&http_monitor, &crypto_filter_monitor, &candidate.2).await;
            }
            if strike.is_none() {
                strike = fetch_strike_price_from_close_time(&http_monitor, &crypto_filter_monitor, candidate.6).await;
            }
            let _ = market_tx_monitor.send((candidate.0, candidate.1, candidate.2.clone(), candidate.6, strike, candidate.4.clone()));
        }
    });

    loop {
        let (yes_token, no_token, market_name, market_close_time, strike_price, _) = market_rx.borrow().clone();

        // CRITICAL: Check market expiry before starting trading
        let now = Utc::now();
        if let Some(close_time) = market_close_time {
            let seconds_until_expiry = (close_time - now).num_seconds();
            if seconds_until_expiry < config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY {
                warn!("⚠️ Market expiring too soon ({}s left)! Skipping until market switch...", seconds_until_expiry);
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                continue;
            }
            info!("⏰ Market closes in {}s (buffer: {}s)", seconds_until_expiry, config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY);
        }

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
        let mut last_momentum_rejection_log = Instant::now() - std::time::Duration::from_secs(10);
        let momentum_exit_in_progress = Arc::new(AtomicBool::new(false));

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
                    cleanup_expired_positions(Arc::clone(&positions), market_name.clone(), yes_token, no_token, market_close_time).await;

                    // Clean up expired time decay positions
                    {
                        let mut td_map = time_decay_positions.lock().await;
                        let before_count = td_map.len();
                        td_map.retain(|_, pos| {
                            if pos.is_expired() {
                                warn!("🧹 Removing expired time decay position (YES: {}, NO: {})", pos.yes_token_id, pos.no_token_id);
                                false
                            } else {
                                true
                            }
                        });
                        if td_map.len() < before_count {
                            info!("✅ Cleaned up {} expired time decay positions", before_count - td_map.len());
                        }
                    }
                }
                _ = status_ticker.tick() => {
                    let (_, yes_ask, _) = *yes_price_rx.borrow();
                    let (_, no_ask, _) = *no_price_rx.borrow();
                    let binance_price = *oracle_rx.borrow();
                    let binance_velocity = *velocity_rx.borrow();

                    if yes_ask != dec!(1) && no_ask != dec!(1) {
                        if let Some(strike) = strike_price {
                            info!("💓 Heartbeat | Poly Sum ${:.4} (Y ${:.2} / N ${:.2}) | Binance: ${:.2} | Strike: ${:.2} | Diff: ${:.2} | Velocity: ${:.2}",
                                yes_ask + no_ask, yes_ask, no_ask, binance_price, strike, binance_price - strike, binance_velocity);
                        } else {
                            info!("💓 Heartbeat | Poly Sum ${:.4} (Y ${:.2} / N ${:.2}) | Binance: ${:.2} | Velocity: ${:.2} (binary, no strike)",
                                yes_ask + no_ask, yes_ask, no_ask, binance_price, binance_velocity);
                        }
                    }
                }
                _ = market_rx.changed() => {
                    info!("🔄 Market change detected in trading loop — restarting for new market...");
                    momentum_exit_in_progress.store(false, Ordering::Relaxed);

                    // Close any open time decay positions before switching markets
                    let td_to_close: Vec<TimeDecayPosition> = {
                        let mut td_map = time_decay_positions.lock().await;
                        td_map.drain().map(|(_, v)| v).collect()
                    };

                    if !td_to_close.is_empty() {
                        let (last_yes_bid, _, _) = *yes_price_rx.borrow();
                        let (last_no_bid, _, _) = *no_price_rx.borrow();
                        let client_switch = Arc::clone(&trading_client);
                        let signer_switch = signer.clone();
                        let nm_switch = Arc::clone(&nonce_manager);
                        let pnl_switch = Arc::clone(&total_pnl);
                        let sh_switch = Arc::clone(&shared_http);

                        tokio::spawn(async move {
                            for td_pos in td_to_close {
                                let exit_yes = (last_yes_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                let exit_no = (last_no_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                let owner = client_switch.credentials().key();
                                info!("🧹 Closing TD position on market switch (YES: {}, NO: {})", td_pos.yes_token_id, td_pos.no_token_id);

                                // Sell YES side
                                let yes_ok = {
                                    let mut guard = nm_switch.lock().await;
                                    let current_nonce = *guard;
                                    let mut order_struct = Order::default();
                                    order_struct.salt = U256::from(Utc::now().timestamp_millis() & ((1 << 53) - 1));
                                    order_struct.maker = safe_address; order_struct.signer = eoa_address;
                                    order_struct.tokenId = td_pos.yes_token_id;
                                    order_struct.makerAmount = U256::from(to_fixed_u128(td_pos.position_size));
                                    order_struct.takerAmount = U256::from(to_fixed_u128(td_pos.position_size * exit_yes));
                                    order_struct.expiration = U256::ZERO; order_struct.nonce = U256::from(current_nonce);
                                    order_struct.feeRateBps = U256::from(yes_fee_rate);
                                    order_struct.side = Side::Sell as u8; order_struct.signatureType = SignatureType::GnosisSafe as u8;
                                    let domain = Eip712Domain { name: Some(Cow::Borrowed(ORDER_NAME)), version: Some(Cow::Borrowed(VERSION)), chain_id: Some(U256::from(POLYGON)), verifying_contract: Some(verifying_contract), ..Eip712Domain::default() };
                                    let hash = order_struct.eip712_signing_hash(&domain);
                                    if let Ok(signature) = signer_switch.sign_hash(&hash).await {
                                        let signed_order = SignedOrder::builder().order(order_struct).signature(signature).order_type(OrderType::FAK).owner(owner).build();
                                        match client_switch.post_order(signed_order).await {
                                            Ok(_) => { *guard += 1; true },
                                            Err(e) => {
                                                warn!("⚠️ TD market-switch YES exit failed: {:?}", e);
                                                drop(guard);
                                                sync_nonce_manager(&nm_switch, &sh_switch, safe_address).await;
                                                false
                                            }
                                        }
                                    } else { false }
                                };

                                // Sell NO side
                                let no_ok = {
                                    let mut guard = nm_switch.lock().await;
                                    let current_nonce = *guard;
                                    let mut order_struct = Order::default();
                                    order_struct.salt = U256::from(Utc::now().timestamp_millis() & ((1 << 53) - 1));
                                    order_struct.maker = safe_address; order_struct.signer = eoa_address;
                                    order_struct.tokenId = td_pos.no_token_id;
                                    order_struct.makerAmount = U256::from(to_fixed_u128(td_pos.position_size));
                                    order_struct.takerAmount = U256::from(to_fixed_u128(td_pos.position_size * exit_no));
                                    order_struct.expiration = U256::ZERO; order_struct.nonce = U256::from(current_nonce);
                                    order_struct.feeRateBps = U256::from(no_fee_rate);
                                    order_struct.side = Side::Sell as u8; order_struct.signatureType = SignatureType::GnosisSafe as u8;
                                    let domain = Eip712Domain { name: Some(Cow::Borrowed(ORDER_NAME)), version: Some(Cow::Borrowed(VERSION)), chain_id: Some(U256::from(POLYGON)), verifying_contract: Some(verifying_contract), ..Eip712Domain::default() };
                                    let hash = order_struct.eip712_signing_hash(&domain);
                                    if let Ok(signature) = signer_switch.sign_hash(&hash).await {
                                        let signed_order = SignedOrder::builder().order(order_struct).signature(signature).order_type(OrderType::FAK).owner(owner).build();
                                        match client_switch.post_order(signed_order).await {
                                            Ok(_) => { *guard += 1; true },
                                            Err(e) => {
                                                warn!("⚠️ TD market-switch NO exit failed: {:?}", e);
                                                false
                                            }
                                        }
                                    } else { false }
                                };

                                // Track P&L for market-switch exits
                                if yes_ok || no_ok {
                                    let realized_pnl = td_pos.position_size * ((exit_yes + exit_no) - (td_pos.yes_entry_price + td_pos.no_entry_price));
                                    *pnl_switch.lock().await += realized_pnl;
                                    info!("📊 TD Market-Switch Exit P&L: ${:.4} ({})", realized_pnl, if yes_ok && no_ok { "both sides filled" } else { "partial fill" });
                                }
                            }
                        });
                    }

                    break;
                }
                _ = ticker.tick() => {
                    if Utc::now() < trade_cooldown { continue; }

                    // CRITICAL: Check market hasn't expired before attempting trades
                    let now = Utc::now();
                    if let Some(close_time) = market_close_time {
                        let seconds_until_expiry = (close_time - now).num_seconds();
                        if seconds_until_expiry < config::MARKET_EXPIRY_SAFETY_BUFFER_SECS {
                            debug!("⚠️ Skipping trades: Market expiring in {}s (safety buffer: {}s)", seconds_until_expiry, config::MARKET_EXPIRY_SAFETY_BUFFER_SECS);
                            continue;
                        }
                    }

                    let (yes_bid, yes_ask, yes_ask_depth) = *yes_price_rx.borrow();
                    let (no_bid, no_ask, no_ask_depth) = *no_price_rx.borrow();

                    if yes_ask == dec!(1) || no_ask == dec!(1) { continue; }

                    // --- Momentum Take Profit Logic (EXPLICITLY FIRST) ---
                    if !momentum_exit_in_progress.load(Ordering::Relaxed) {
                        let pos_map = positions.lock().await;
                        let yes_pos = pos_map.get(&yes_token).cloned();
                        let no_pos  = pos_map.get(&no_token).cloned();

                        let mut exit_token = None;
                        let mut exit_price = dec!(0);
                        let mut exit_shares = dec!(0);
                        let mut exit_fee_rate = 0;
                        let mut exit_avg_entry = dec!(0);

                        let velocity = *velocity_rx.borrow();
                        let threshold = match crypto_filter.as_str() {
                            "eth" => config::ETH_MOMENTUM_THRESHOLD,
                            "sol" => config::SOL_MOMENTUM_THRESHOLD,
                            _ => config::BTC_MOMENTUM_THRESHOLD,
                        };

                        if let Some(yp) = yes_pos {
                            if yp.shares > dec!(0) {
                                if let Some(reason) = MomentumStrategy::should_exit_momentum(yes_bid, yp.avg_entry, velocity, threshold, &crypto_filter) {
                                    match reason {
                                        rustpolybot::strategies::momentum::ExitReason::TakeProfit { bid_price, profit_pct, target_pct } => {
                                            info!("🎯 Momentum YES Target Reached (Bid: ${:.2}, Profit: {:.2}% vs Target: {:.2}%) - Taking Profit", bid_price, profit_pct * dec!(100), target_pct * dec!(100));
                                            exit_token = Some(yes_token);
                                        },
                                        rustpolybot::strategies::momentum::ExitReason::StopLoss { bid_price, loss_pct } => {
                                            info!("🛑 Momentum YES Stop Loss Hit (Bid: ${:.2}, Loss: {:.2}%)", bid_price, loss_pct * dec!(100));
                                            exit_token = Some(yes_token);
                                        },
                                        rustpolybot::strategies::momentum::ExitReason::Reversal { velocity: vel, threshold: thr } => {
                                            info!("📉 Momentum YES Reversal Detected (Velocity: ${:.2} < Threshold: ${:.2})", vel, thr);
                                            exit_token = Some(yes_token);
                                        },
                                    }
                                    exit_price = (yes_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                    exit_shares = yp.shares;
                                    exit_fee_rate = yes_fee_rate;
                                    exit_avg_entry = yp.avg_entry;
                                }
                            }
                        }

                        if exit_token.is_none() {
                            if let Some(np) = no_pos {
                                if np.shares > dec!(0) {
                                    if let Some(reason) = MomentumStrategy::should_exit_momentum(no_bid, np.avg_entry, -velocity, threshold, &crypto_filter) {
                                        match reason {
                                            rustpolybot::strategies::momentum::ExitReason::TakeProfit { bid_price, profit_pct, target_pct } => {
                                                info!("🎯 Momentum NO Target Reached (Bid: ${:.2}, Profit: {:.2}% vs Target: {:.2}%) - Taking Profit", bid_price, profit_pct * dec!(100), target_pct * dec!(100));
                                                exit_token = Some(no_token);
                                            },
                                            rustpolybot::strategies::momentum::ExitReason::StopLoss { bid_price, loss_pct } => {
                                                info!("🛑 Momentum NO Stop Loss Hit (Bid: ${:.2}, Loss: {:.2}%)", bid_price, loss_pct * dec!(100));
                                                exit_token = Some(no_token);
                                            },
                                            rustpolybot::strategies::momentum::ExitReason::Reversal { velocity: vel, threshold: thr } => {
                                                info!("📉 Momentum NO Reversal Detected (Velocity: ${:.2} > -${:.2})", vel, thr);
                                                exit_token = Some(no_token);
                                            },
                                        }
                                        exit_price = (no_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                        exit_shares = np.shares;
                                        exit_fee_rate = no_fee_rate;
                                        exit_avg_entry = np.avg_entry;
                                    }
                                }
                            }
                        }

                        if let Some(token) = exit_token {
                            momentum_exit_in_progress.store(true, Ordering::Relaxed);
                            let eip_handle = Arc::clone(&momentum_exit_in_progress);
                            let client = Arc::clone(&trading_client);
                            let signer = signer.clone();
                            let nm = Arc::clone(&nonce_manager);
                            let sh = Arc::clone(&shared_http);
                            let owner = client.credentials().key();
                            let tt = tg_token.clone();
                            let tc = tg_chat_id.clone();
                            let pos_handle = Arc::clone(&positions);
                            let pnl_handle = Arc::clone(&total_pnl);
                            tokio::spawn(async move {
                                let mut current_shares = exit_shares;
                                let mut current_exit_price = exit_price;
                                for attempt in 0..5 {
                                    if current_shares < config::MIN_ORDER_SHARES {
                                        eip_handle.store(false, Ordering::Relaxed);
                                        return Ok(());
                                    }

                                    let mut guard = nm.lock().await;
                                    let current_nonce = *guard;

                                    let mut order_struct = Order::default();
                                    order_struct.salt = U256::from(Utc::now().timestamp_millis() & ((1 << 53) - 1));
                                    order_struct.maker = safe_address;
                                    order_struct.signer = eoa_address;
                                    order_struct.taker = Address::ZERO;
                                    order_struct.tokenId = token;
                                    // Truncate shares to 2 decimals to ensure we never exceed available balance
                                    // (round_dp could round up and cause "not enough balance" errors)
                                    let truncated_shares = current_shares.trunc_with_scale(2);
                                    if truncated_shares != current_shares && truncated_shares > dec!(0) {
                                        debug!("📍 Precision: Truncating {} → {} shares to fit balance constraint", current_shares, truncated_shares);
                                    }
                                    if truncated_shares < config::MIN_ORDER_SHARES {
                                        eip_handle.store(false, Ordering::Relaxed);
                                        return Ok(());
                                    }
                                    order_struct.makerAmount = U256::from(to_fixed_u128(truncated_shares));
                                    order_struct.takerAmount = U256::from(to_fixed_u128(truncated_shares * current_exit_price));
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
                                                info!("🚀 LIVE MOMENTUM EXIT FILLED: {} shares of token {} @ ${:.2}", current_shares, token, current_exit_price);
                                                let mut pm = pos_handle.lock().await;
                                                pm.remove(&token);
                                                drop(pm);
                                                let realized_pnl = current_shares * (current_exit_price - exit_avg_entry);
                                                *pnl_handle.lock().await += realized_pnl;
                                                info!("📊 Momentum Exit P&L: ${:.4} (entry ${:.2} → exit ${:.2})", realized_pnl, exit_avg_entry, current_exit_price);
                                                *guard += 1;
                                                eip_handle.store(false, Ordering::Relaxed);
                                                return Ok(());
                                            },
                                            Err(e) => {
                                                let err_msg = format!("{:?}", e).to_lowercase();
                                                drop(guard);

                                                if err_msg.contains("invalid nonce") && attempt < 4 {
                                                    warn!("⚠️ Invalid nonce in momentum exit (attempt {}). Re-syncing for Maker {}...", attempt + 1, safe_address);
                                                    sync_nonce_manager(&nm, &sh, safe_address).await;
                                                    tokio::time::sleep(Duration::from_millis(200 * ((attempt as u64) + 1))).await;
                                                    continue;
                                                } else if (err_msg.contains("not enough balance") || err_msg.contains("not enough allowance")) && attempt < 4 {
                                                    if let Some(actual_balance) = parse_balance_from_error(&err_msg) {
                                                        if actual_balance == dec!(0) {
                                                            warn!("⚠️ Balance is 0 in momentum exit (likely indexer lag). Waiting 2s and retrying...");
                                                            tokio::time::sleep(Duration::from_millis(2000)).await;
                                                            continue;
                                                        }
                                                        warn!("⚠️ Balance mismatch in momentum exit. Retrying with actual balance: {}", actual_balance);
                                                        current_shares = actual_balance;
                                                        continue;
                                                    }
                                                } else if (err_msg.contains("no orders found to match") || err_msg.contains("fak")) && attempt < 4 {
                                                    // FAK found no matching bids — lower the sell price and retry
                                                    let new_price = (current_exit_price - dec!(0.02)).max(config::MIN_SELL_LIMIT_PRICE);
                                                    warn!("⚠️ FAK exit found no matching bids at ${:.2}. Lowering to ${:.2} and retrying (attempt {})...", current_exit_price, new_price, attempt + 1);
                                                    current_exit_price = new_price;
                                                    tokio::time::sleep(Duration::from_millis(500)).await;
                                                    continue;
                                                }
                                                // All retries exhausted or unrecoverable error — restore position for main loop retry
                                                warn!("⚠️ Momentum exit failed after attempt {}. Position preserved for retry on next tick.", attempt + 1);
                                                eip_handle.store(false, Ordering::Relaxed);
                                                let msg = format!("❌ [RustPolyBot] Momentum Exit Order Failed (Attempt {}): {:?}", attempt + 1, e);
                                                let _ = send_notification(&tt, &tc, &msg).await;
                                                error!("{}", msg);
                                                return Err(anyhow::anyhow!(msg));
                                            }
                                        }
                                    } else {
                                        drop(guard);
                                        eip_handle.store(false, Ordering::Relaxed);
                                        let msg = format!("❌ [RustPolyBot] Momentum Exit Order Signing Failed (Attempt {}): {:?}", attempt + 1, token);
                                        let _ = send_notification(&tt, &tc, &msg).await;
                                        error!("{}", msg);
                                        return Err(anyhow::anyhow!(msg));
                                    }
                                }
                                // Max retries exhausted — position stays in pos_map for main loop retry
                                eip_handle.store(false, Ordering::Relaxed);
                                warn!("⚠️ Momentum exit max retries reached. Position preserved for retry on next tick.");
                                Err(anyhow::anyhow!("Max retries reached for momentum exit"))
                            });
                            // Don't clear position — the spawned task handles it on success,
                            // or preserves it on failure so the main loop can retry
                            trade_cooldown = Utc::now() + chrono::Duration::seconds(config::TRADE_COOLDOWN_SECS);
                            continue;
                        }
                    }

                    // --- Momentum Trading Logic (One-Sided) ---
                    // NOTE: Momentum trading works WITH OR WITHOUT resolved strike price
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

                        // Momentum strategy works with or without strike price
                        let mut momentum_token = None;
                        let mut limit_price = dec!(0);
                        let mut target_depth = dec!(0);
                        let mut fee_rate = 0;

                        // For momentum, we check velocity + price thresholds regardless of strike
                        let current_signal_token = if strike_price.is_some() {
                            // With strike: use full evaluation
                            MomentumStrategy::evaluate_entry(
                                velocity,
                                binance_price,
                                strike_price,
                                yes_token,
                                no_token,
                                yes_ask,
                                no_ask,
                                &crypto_filter,
                            )
                        } else {
                            // Without strike: simpler velocity-based evaluation
                            // Check if velocity exceeds threshold and ask price is reasonable
                            if velocity > threshold && yes_ask <= config::MAX_MOMENTUM_ENTRY_PRICE {
                                Some(yes_token)
                            } else if velocity < -threshold && no_ask <= config::MAX_MOMENTUM_ENTRY_PRICE {
                                Some(no_token)
                            } else {
                                None
                            }
                        };

                        if current_signal_token.is_none() {
                            // Log why momentum signal was rejected
                            if velocity.abs() >= threshold {
                                let reason = if velocity > threshold {
                                    if yes_ask > config::MAX_MOMENTUM_ENTRY_PRICE {
                                        format!("YES ask ${:.2} exceeds max entry price ${:.2}", yes_ask, config::MAX_MOMENTUM_ENTRY_PRICE)
                                    } else if let Some(s) = strike_price {
                                        if binance_price <= (s + strike_buffer) {
                                            format!("Price ${:.2} not above strike+buffer ${:.2}", binance_price, s + strike_buffer)
                                        } else {
                                            String::new()
                                        }
                                    } else {
                                        String::new()
                                    }
                                } else {
                                    if no_ask > config::MAX_MOMENTUM_ENTRY_PRICE {
                                        format!("NO ask ${:.2} exceeds max entry price ${:.2}", no_ask, config::MAX_MOMENTUM_ENTRY_PRICE)
                                    } else if let Some(s) = strike_price {
                                        if binance_price >= (s - strike_buffer) {
                                            format!("Price ${:.2} not below strike-buffer ${:.2}", binance_price, s - strike_buffer)
                                        } else {
                                            String::new()
                                        }
                                    } else {
                                        String::new()
                                    }
                                };
                                if !reason.is_empty() && last_momentum_rejection_log.elapsed().as_secs() >= 1 {
                                    warn!("⏭️ Momentum signal rejected: {} (Velocity: ${:.2}, Threshold: ${:.2}) | YES ask: ${:.4} | NO ask: ${:.4}", reason, velocity, threshold, yes_ask, no_ask);
                                    last_momentum_rejection_log = Instant::now();
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
                            let prev_ticks = consecutive_momentum_signals;
                            consecutive_momentum_signals = 0;
                            if prev_ticks > 0 {
                                debug!("⏸️  Momentum signal lost (was at {} ticks)", prev_ticks);
                            }
                        }

                        if let Some(token) = momentum_token {
                            let current_usdc_balance = *balance_rx.borrow();
                            if current_usdc_balance >= momentum_trade_size_usdc {
                                let target_shares = (momentum_trade_size_usdc / limit_price).floor();

                                // CRITICAL: Re-validate entry conditions before placing trade
                                // Market conditions may have changed since signal confirmation
                                let should_proceed = if strike_price.is_some() {
                                    let strike = strike_price.unwrap();
                                    // With strike: strict checks
                                    if token == yes_token {
                                        velocity > threshold && binance_price > (strike + strike_buffer) && yes_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                                    } else {
                                        velocity < -threshold && binance_price < (strike - strike_buffer) && no_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                                    }
                                } else {
                                    // Binary market: velocity and ask price checks only
                                    if token == yes_token {
                                        velocity > threshold && yes_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                                    } else {
                                        velocity < -threshold && no_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                                    }
                                };

                                if !should_proceed {
                                    if let Some(s) = strike_price {
                                        warn!("⏭️ MOMENTUM ENTRY CANCELLED: Market conditions changed (Velocity: ${:.2}, Price: ${:.2} vs Strike±Buffer ${:.2}, Ask: ${:.4})",
                                            velocity, binance_price, if token == yes_token { s + strike_buffer } else { s - strike_buffer }, if token == yes_token { yes_ask } else { no_ask });
                                    } else {
                                        warn!("⏭️ MOMENTUM ENTRY CANCELLED: Velocity or ask price threshold not met (Velocity: ${:.2}, Threshold: ${:.2}, Ask: ${:.4})",
                                            velocity.abs(), threshold, if token == yes_token { yes_ask } else { no_ask });
                                    }
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
                                            let close_time_handle = market_close_time;
                                            let pair_token_handle = if token == yes_token { no_token } else { yes_token };
                                            let shared_http_handle = Arc::clone(&shared_http);
                                            let owner = client.credentials().key();

                                            tokio::spawn(async move {
                                                for attempt in 0..3 {
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
                                                                    pm.insert(token, Position { shares: target_shares, avg_entry: limit_price, opened_at: Utc::now(), close_time: close_time_handle, market_name: market_name_handle, pair_token_id: pair_token_handle, fill_confirmed_at: None });
                                                                }
                                                                *guard += 1;
                                                                let _ = sync_position_balance(&client, &positions_handle, token).await;
                                                                break;
                                                            },
                                                            Err(e) => {
                                                                let err_msg = format!("{:?}", e).to_lowercase();
                                                                drop(guard);
                                                                if err_msg.contains("invalid nonce") && attempt < 2 {
                                                                    warn!("⚠️ Invalid nonce in momentum entry (attempt {}). Re-syncing for Maker {}...", attempt + 1, safe_address);
                                                                    sync_nonce_manager(&nonce_manager, &shared_http_handle, safe_address).await;
                                                                    // Add backoff delay after nonce resync
                                                                    tokio::time::sleep(Duration::from_millis(200 * ((attempt as u64) + 1))).await;
                                                                    continue;
                                                                }
                                                                // Retry on transient server errors (500, 502, 503, 504)
                                                                if (err_msg.contains("status_code: 500") || err_msg.contains("status_code: 502") ||
                                                                    err_msg.contains("status_code: 503") || err_msg.contains("status_code: 504")) && attempt < 2 {
                                                                    warn!("⚠️ Transient server error in momentum entry (attempt {}). Retrying with backoff...", attempt + 1);
                                                                    // Exponential backoff: 300ms, 600ms
                                                                    tokio::time::sleep(Duration::from_millis(300 * ((attempt as u64) + 1))).await;
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

                    // --- TIME DECAY (THETA) STRATEGY ---
                    if config::ENABLE_TIME_DECAY_TRADING {
                        if let Some(close_time) = market_close_time {
                            let now = Utc::now();
                            let seconds_to_expiry = (close_time - now).num_seconds();

                            // Check theta timing window (4-30 min to expiry)
                            if TimeDecayStrategy::is_in_theta_window(seconds_to_expiry) {
                                // Fee-aware opportunity detection with dual modes
                                if let Some(signal) = TimeDecayStrategy::calculate_theta_opportunity(
                                    yes_ask, no_ask, yes_fee_rate, no_fee_rate, seconds_to_expiry
                                ) {
                                    let current_usdc_balance = *balance_rx.borrow();
                                    let combined_cost = signal.combined_ask * config::TIME_DECAY_POSITION_SIZE_USDC;

                                    // Check balance (need ~2x for both sides)
                                    if current_usdc_balance >= combined_cost * dec!(2) {
                                        // Check position limits
                                        let td_pos = time_decay_positions.lock().await;
                                        let total_td_exposure: Decimal = td_pos.values()
                                            .map(|p| p.total_invested)
                                            .sum();

                                        if td_pos.len() < config::TIME_DECAY_MAX_POSITIONS
                                            && total_td_exposure + combined_cost <= config::TIME_DECAY_MAX_TOTAL_EXPOSURE_USDC {

                                            let target_shares = config::TIME_DECAY_POSITION_SIZE_USDC;
                                            let yes_limit_price = (yes_ask + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE).round_dp(2);
                                            let no_limit_price = (no_ask + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE).round_dp(2);

                                            let mode_label = match signal.mode {
                                                ThetaMode::Settlement => "SETTLEMENT",
                                                ThetaMode::Convergence => "CONVERGENCE",
                                            };
                                            info!("💰 THETA {} OPPORTUNITY: Combined ${:.4} | Net ${:.4}/sh | Fees ${:.4} | Expires {}s | YES ${:.2} + NO ${:.2}",
                                                mode_label, signal.combined_ask, signal.net_profit_per_share, signal.total_fees, seconds_to_expiry, yes_ask, no_ask);

                                        if !config::GHOST_MODE {
                                            drop(td_pos);
                                            let client_clone = Arc::clone(&trading_client);
                                            let signer_clone = signer.clone();
                                            let nonce_manager_clone = Arc::clone(&nonce_manager);
                                            let shared_http_clone = Arc::clone(&shared_http);
                                            let td_pos_handle = Arc::clone(&time_decay_positions);
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
                                                                        warn!("⚠️ Invalid nonce in time decay YES buy. Re-syncing...");
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
                                                                        warn!("⚠️ Invalid nonce in time decay NO buy. Re-syncing...");
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
                                                let td_position = TimeDecayPosition::new(
                                                    yes_token,
                                                    no_token,
                                                    now,
                                                    close_time,
                                                    yes_limit_price,
                                                    no_limit_price,
                                                    target_shares,
                                                    signal.mode,
                                                );
                                                let mut td_pos_map = td_pos_handle.lock().await;
                                                td_pos_map.insert(yes_token, td_position.clone());
                                                info!("💰 Theta {} position opened: YES {} + NO {} | Expires in {}s",
                                                    mode_label, yes_token, no_token, seconds_to_expiry);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Time Decay Exit Logic
                        {
                            let td_pos_map = time_decay_positions.lock().await;
                            let mut positions_to_close = Vec::new();

                            for (token_id, td_pos) in td_pos_map.iter() {
                                let (unrealized_pnl, pnl_pct) = TimeDecayStrategy::calculate_current_pnl(yes_bid, no_bid, td_pos.yes_entry_price + td_pos.no_entry_price, td_pos.position_size);

                                // Check take profit
                                if pnl_pct >= config::TIME_DECAY_TARGET_PROFIT_PERCENT {
                                    info!("💰 THETA TAKE PROFIT HIT: +{:.2}% (${:.2})", pnl_pct * dec!(100), unrealized_pnl);
                                    positions_to_close.push(*token_id);
                                }
                                // Check stop loss
                                else if pnl_pct <= -config::TIME_DECAY_STOP_LOSS_PERCENT {
                                    warn!("⚠️ THETA STOP LOSS HIT: {:.2}% (${:.2})", pnl_pct * dec!(100), unrealized_pnl);
                                    positions_to_close.push(*token_id);
                                }
                                // Convergence-mode exit: combined bid reached target
                                else if td_pos.mode == ThetaMode::Convergence
                                    && TimeDecayStrategy::should_convergence_exit(yes_bid, no_bid)
                                {
                                    info!("💰 THETA CONVERGENCE EXIT: Combined bid ${:.4} >= target ${:.4} (P&L: ${:.4})",
                                        yes_bid + no_bid, config::TIME_DECAY_CONVERGENCE_EXIT_BID, unrealized_pnl);
                                    positions_to_close.push(*token_id);
                                }
                                // Convergence-mode: exit before global safety buffer blocks us
                                else if td_pos.mode == ThetaMode::Convergence
                                    && td_pos.time_to_expiry() <= config::TIME_DECAY_MIN_SECS_TO_EXPIRY
                                {
                                    info!("⏰ THETA CONVERGENCE EXPIRY EXIT: {}s to close (P&L: ${:.4})", td_pos.time_to_expiry(), unrealized_pnl);
                                    positions_to_close.push(*token_id);
                                }
                                // Settlement-mode: hold to expiry (auto-settles at $1.00)
                                // Only exit if very close and we want to be safe
                                else if td_pos.mode == ThetaMode::Settlement && td_pos.time_to_expiry() <= 30 {
                                    info!("⏰ THETA SETTLEMENT APPROACHING: {}s to close — holding for settlement", td_pos.time_to_expiry());
                                    // Don't close — settlement mode profits from holding
                                    // The position will be cleaned up after expiry
                                }
                            }

                            if !positions_to_close.is_empty() {
                                drop(td_pos_map);
                                for token_to_exit in positions_to_close {
                                    if let Some(td_position) = time_decay_positions.lock().await.remove(&token_to_exit) {
                                        let exit_price_yes = (yes_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                        let exit_price_no = (no_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);

                                        let client_clone = Arc::clone(&trading_client);
                                        let signer_clone = signer.clone();
                                        let nonce_manager_clone = Arc::clone(&nonce_manager);
                                        let owner = client_clone.credentials().key();

                                        let yes_exit_task = {
                                            let client = Arc::clone(&client_clone);
                                            let signer = signer_clone.clone();
                                            let nonce_manager = Arc::clone(&nonce_manager_clone);
                                            async move {
                                                let mut guard = nonce_manager.lock().await;
                                                let current_nonce = *guard;
                                                let mut order_struct = Order::default();
                                                order_struct.salt = U256::from(Utc::now().timestamp_millis() & ((1 << 53) - 1));
                                                order_struct.maker = safe_address; order_struct.signer = eoa_address; order_struct.tokenId = yes_token;
                                                order_struct.makerAmount = U256::from(to_fixed_u128(td_position.position_size));
                                                order_struct.takerAmount = U256::from(to_fixed_u128(td_position.position_size * exit_price_yes));
                                                order_struct.expiration = U256::ZERO; order_struct.nonce = U256::from(current_nonce);
                                                order_struct.feeRateBps = U256::from(yes_fee_rate);
                                                order_struct.side = Side::Sell as u8; order_struct.signatureType = SignatureType::GnosisSafe as u8;
                                                let domain = Eip712Domain { name: Some(Cow::Borrowed(ORDER_NAME)), version: Some(Cow::Borrowed(VERSION)), chain_id: Some(U256::from(POLYGON)), verifying_contract: Some(verifying_contract), ..Eip712Domain::default() };
                                                let hash = order_struct.eip712_signing_hash(&domain);
                                                if let Ok(signature) = signer.sign_hash(&hash).await {
                                                    let signed_order = SignedOrder::builder().order(order_struct).signature(signature).order_type(OrderType::FAK).owner(owner).build();
                                                    match client.post_order(signed_order).await {
                                                        Ok(_) => { *guard += 1; return Ok(()); },
                                                        Err(e) => { return Err(anyhow::anyhow!("{:?}", e)); }
                                                    }
                                                } else { return Err(anyhow::anyhow!("Signing failed")); }
                                            }
                                        };
                                        let no_exit_task = {
                                            let client = Arc::clone(&client_clone);
                                            let signer = signer_clone.clone();
                                            let nonce_manager = Arc::clone(&nonce_manager_clone);
                                            async move {
                                                let mut guard = nonce_manager.lock().await;
                                                let current_nonce = *guard;
                                                let mut order_struct = Order::default();
                                                order_struct.salt = U256::from(Utc::now().timestamp_millis() & ((1 << 53) - 1));
                                                order_struct.maker = safe_address; order_struct.signer = eoa_address; order_struct.tokenId = no_token;
                                                order_struct.makerAmount = U256::from(to_fixed_u128(td_position.position_size));
                                                order_struct.takerAmount = U256::from(to_fixed_u128(td_position.position_size * exit_price_no));
                                                order_struct.expiration = U256::ZERO; order_struct.nonce = U256::from(current_nonce);
                                                order_struct.feeRateBps = U256::from(no_fee_rate);
                                                order_struct.side = Side::Sell as u8; order_struct.signatureType = SignatureType::GnosisSafe as u8;
                                                let domain = Eip712Domain { name: Some(Cow::Borrowed(ORDER_NAME)), version: Some(Cow::Borrowed(VERSION)), chain_id: Some(U256::from(POLYGON)), verifying_contract: Some(verifying_contract), ..Eip712Domain::default() };
                                                let hash = order_struct.eip712_signing_hash(&domain);
                                                if let Ok(signature) = signer.sign_hash(&hash).await {
                                                    let signed_order = SignedOrder::builder().order(order_struct).signature(signature).order_type(OrderType::FAK).owner(owner).build();
                                                    match client.post_order(signed_order).await {
                                                        Ok(_) => { *guard += 1; return Ok(()); },
                                                        Err(e) => { return Err(anyhow::anyhow!("{:?}", e)); }
                                                    }
                                                } else { return Err(anyhow::anyhow!("Signing failed")); }
                                            }
                                        };
                                        let (yes_exit_res, no_exit_res) = tokio::join!(yes_exit_task, no_exit_task);
                                        if yes_exit_res.is_ok() || no_exit_res.is_ok() {
                                            let realized_pnl = td_position.position_size * ((exit_price_yes + exit_price_no) - (td_position.yes_entry_price + td_position.no_entry_price));
                                            *total_pnl.lock().await += realized_pnl;
                                            info!("📊 Time Decay Exit P&L: ${:.4} (entry ${:.4}, exit ${:.4})", realized_pnl,
                                                td_position.yes_entry_price + td_position.no_entry_price, exit_price_yes + exit_price_no);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    }

                    // --- Arbitrage Logic ---
                    if config::ARBITRAGE_PROFIT_THRESHOLD > dec!(0) {
                        let (_, profit_margin) = ArbitrageStrategy::calculate_profit_margin(yes_ask, no_ask, yes_fee_rate, no_fee_rate);

                        if profit_margin >= config::ARBITRAGE_PROFIT_THRESHOLD {
                            let current_usdc_balance = *balance_rx.borrow();
                            let combined_ask = yes_ask + no_ask;
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
                                        pm.insert(yes_token, Position { shares: target_shares, avg_entry: yes_limit_price, opened_at: now, close_time: market_close_time, market_name: market_name.clone(), pair_token_id: no_token, fill_confirmed_at: None });
                                        pm.insert(no_token, Position { shares: target_shares, avg_entry: no_limit_price, opened_at: now, close_time: market_close_time, market_name: market_name.clone(), pair_token_id: yes_token, fill_confirmed_at: None });
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
                    }

                    // --- Perfect Hedge Early Exit Logic ---
                    {
                        let pos_map = positions.lock().await;
                        let yes_pos = pos_map.get(&yes_token).cloned();
                        let no_pos  = pos_map.get(&no_token).cloned();
                        if let (Some(yp), Some(np)) = (yes_pos, no_pos) {
                            if yp.shares > dec!(0) && np.shares > dec!(0) {
                                if ArbitrageStrategy::should_early_exit(yes_bid, no_bid) {
                                    info!("💰 Bids reached target early exit (sum ${:.4})", yes_bid + no_bid);
                                    let exit_price_yes = (yes_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                    let exit_price_no  = (no_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE).round_dp(2);
                                    let client_clone = Arc::clone(&trading_client);
                                    let signer_clone = signer.clone();
                                    let nonce_manager_clone = Arc::clone(&nonce_manager);
                                    let shared_http_clone = Arc::clone(&shared_http);
                                    let owner = client_clone.credentials().key();

                                    let yes_exit_task = {
                                        let client = Arc::clone(&client_clone);
                                        let signer = signer_clone.clone();
                                        let nonce_manager = Arc::clone(&nonce_manager_clone);
                                        let shared_http = Arc::clone(&shared_http_clone);
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
            }
        }
    }
}
