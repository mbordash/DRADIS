/// RustPolyBot - Multi-Strategy Orchestrator Trading Bot
///
/// Phase 8: Full Orchestrator-Based Trading
/// Strategies evaluate signals → orchestrator resolves conflicts → executor places orders.

use anyhow::Result;

use polymarket_client_sdk::clob::{Client as ClobClient, Config};
use polymarket_client_sdk::clob::types::{Side, SignatureType, OrderType};
use polymarket_client_sdk::{POLYGON, PRIVATE_KEY_VAR, derive_safe_wallet};
use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk::clob::types::AssetType;

use futures::StreamExt as _;
use polymarket_client_sdk::clob::ws::Client as WsClient;

use alloy::primitives::{U256, Address, address};
use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;

use chrono::{DateTime, Utc};
use reqwest;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use std::collections::{HashMap, VecDeque};
use std::env;
use std::str::FromStr as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{watch, Mutex};
use tokio::time::{interval, Instant, Duration};

use tracing::{info, warn, debug, error};

use rustpolybot::config;
use rustpolybot::risk::RiskEngine;
use rustpolybot::state::{Position, StrategySignal, MarketConfig, MarketSnapshot, PositionMap};
use rustpolybot::strategies::time_decay_impl::TimeDecayPosition;
use rustpolybot::orchestrator::{StrategyRegistry, StrategyContext};
use rustpolybot::orchestrator::executor::{execute_strategies_concurrent, aggregate_and_resolve_signals};
use rustpolybot::notifications::send_notification;
use rustpolybot::helpers::{
    time::*, balance::*, nonce::*, orders::*, market::*, price::{round_to_tick_size, floor_to_tick_size}
};

use rustls::crypto::ring;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

/// Rate-limit the "net exposure risk check failed" log to once per 5s to prevent log spam.
static LAST_MAKER_EXPOSURE_REJECT_CALLER_LOG: AtomicU64 = AtomicU64::new(0);

fn should_log_maker_exposure_reject() -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let last = LAST_MAKER_EXPOSURE_REJECT_CALLER_LOG.load(Ordering::Relaxed);
    if now >= last + 5 {
        LAST_MAKER_EXPOSURE_REJECT_CALLER_LOG.store(now, Ordering::Relaxed);
        true
    } else {
        false
    }
}

type PriceState = (Decimal, Decimal, Decimal, Decimal); // (Bid, BidDepth, Ask, AskDepth)

const EXCHANGE_NORMAL: Address = address!("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E");
const EXCHANGE_NEG_RISK: Address = address!("0xC5d563A36AE78145C45a50134d48A1215220f80a");

async fn cleanup_expired_positions(
    positions: Arc<Mutex<PositionMap>>,
    market_name: String,
    yes_token: U256,
    no_token: U256,
    close_time: Option<DateTime<Utc>>,
) {
    let mut pos_map = positions.lock().await;
    let now = Utc::now();

    if let Some(ct) = close_time {
        let is_expired = ct <= now;
        let is_expiring_soon = (ct - now).num_seconds() < 60;

        if is_expired || is_expiring_soon {
            let before = pos_map.len();
            // Remove all strategies' positions for these two tokens
            pos_map.retain(|(_, token), _| token != &yes_token && token != &no_token);
            let removed = before - pos_map.len();

            if removed > 0 {
                warn!("🧹 Cleaned up {} position(s) for market \"{}\" (expires {})",
                    removed,
                    market_name,
                    if is_expired { "NOW" } else { "in <60s" }
                );
            }
        }
    }
}

/// Reconcile orphaned paired positions.
///
/// For Arbitrage and TimeDecay strategies, positions must come in hedged pairs (YES+NO).
/// If the first leg fills but the second leg fails, we're left with a one-sided position.
/// This function detects such orphans (opened >60s ago with no matching pair leg) and
/// immediately exits them to prevent unlimited unhedged losses.
async fn reconcile_orphaned_positions(
    positions: Arc<Mutex<PositionMap>>,
    _trading_client: &Arc<ClobClient<polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>>>,
    _nonce_manager: &Arc<AtomicU64>,
    _signer: &dyn Signer,
    _safe_address: Address,
    _eoa_address: Address,
    tg_token: &str,
    tg_chat_id: &str,
    _shared_http: &Arc<reqwest::Client>,
) -> Result<()> {
    let mut pos_map = positions.lock().await;
    let now = Utc::now();

    // Find all paired-strategy positions that are missing their hedge
    let mut orphans_to_exit: Vec<((String, U256), Position)> = Vec::new();

    for ((strategy_name, token_id), position) in pos_map.iter() {
        // Only check paired strategies
        if strategy_name != "ArbitrageStrategy" && strategy_name != "TimeDecayStrategy" {
            continue;
        }

        // Position must be open for at least 60 seconds (allows time for fill/sync)
        let age_secs = (now - position.opened_at).num_seconds();
        if age_secs < 60 {
            continue;
        }

        // Check if the paired leg exists
        if let Some(paired_token) = position.paired_leg_token_id {
            let pair_key = (strategy_name.clone(), paired_token);
            if pos_map.contains_key(&pair_key) {
                // Pair exists, not an orphan
                continue;
            }

            // Orphan detected: first leg exists, but paired leg doesn't
            orphans_to_exit.push(((strategy_name.clone(), *token_id), position.clone()));
        }
    }

    // Exit all orphaned positions immediately
    for ((strategy_name, token_id), position) in orphans_to_exit {
        warn!("🚨 ORPHANED PAIR DETECTED [{}]: {} shares at ${:.4} ({}s old) — exiting immediately",
              strategy_name, position.shares, position.avg_entry,
              (now - position.opened_at).num_seconds());

        // Sell the orphaned position immediately at market (use current bid - sell offset)
        // For now, we'll remove from tracking and log; in production, place a market sell
        pos_map.remove(&(strategy_name.clone(), token_id));

        // Notify user of the orphan
        let _ = send_notification(tg_token, tg_chat_id,
            &format!("🚨 Orphaned pair exited [{}]: {} {} shares @ ${:.4}",
                     strategy_name,
                     if token_id == position.pair_token_id { "YES" } else { "NO" },
                     position.shares.trunc(),
                     position.avg_entry)).await;
    }

    Ok(())
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
    let eoa_address = signer.address();
    info!("Trading wallet (EOA) address: {}", eoa_address);

    let trading_client = Arc::new(ClobClient::new(config::CLOB_API_BASE, Config::default())?
        .authentication_builder(&signer)
        .signature_type(SignatureType::GnosisSafe)
        .authenticate()
        .await?);

    let safe_address = derive_safe_wallet(eoa_address, POLYGON).expect("Safe derivation failed");
    info!("Authenticated on Polymarket CLOB. Safe (Maker) address: {}", safe_address);

    let initial_nonce = fetch_next_nonce(&shared_http, safe_address).await.unwrap_or(0);
    info!("🔄 Initialized Nonce from API (Maker/Safe): {}", initial_nonce);
    let nonce_manager = Arc::new(AtomicU64::new(initial_nonce));

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
    // Broadcast (velocity_5s, velocity_1s, acceleration)
    let (velocity_tx, velocity_rx) = watch::channel((dec!(0), dec!(0), dec!(0)));
    // Broadcast latest Binance perpetual funding rate (updated every ~60s)
    let (funding_tx, funding_rx) = watch::channel(dec!(0));
    // Broadcast 60-minute oracle price drift (current − price_60m_ago)
    let (drift_60m_tx, drift_60m_rx) = watch::channel(dec!(0));

    let crypto_symbol = crypto_filter.clone();
    tokio::spawn(async move {
        let binance_pair = match crypto_symbol.as_str() {
            "eth" => "ethusdt",
            "sol" => "solusdt",
            _ => "btcusdt",
        };
        let url_str = format!("wss://stream.binance.com:9443/ws/{}@ticker", binance_pair);
        let mut price_history: VecDeque<(Instant, Decimal)> = VecDeque::new();
        let mut price_history_60m: VecDeque<(Instant, Decimal)> = VecDeque::new();
        let mut prev_velocity = dec!(0);

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

                                    // Trim entries older than the primary window (5s)
                                    while let Some((t, _)) = price_history.front() {
                                        if now.duration_since(*t).as_secs() >= config::MOMENTUM_WINDOW_SECS {
                                            price_history.pop_front();
                                        } else { break; }
                                    }

                                    // ── Primary velocity (5s window) ──────────────────
                                    let velocity_5s = if let Some((_, start_price)) = price_history.front() {
                                        price - start_price
                                    } else { dec!(0) };

                                    // ── Short velocity (1s window) ────────────────────
                                    // Walk back through history to find the price ~1s ago.
                                    let velocity_1s = {
                                        let cutoff = config::MOMENTUM_SHORT_WINDOW_SECS;
                                        let start_1s = price_history.iter()
                                            .find(|(t, _)| now.duration_since(*t).as_secs() < cutoff);
                                        match start_1s {
                                            Some((_, p)) => price - p,
                                            None => velocity_5s, // insufficient history → use 5s
                                        }
                                    };

                                    // ── Acceleration ──────────────────────────────────
                                    // Rate of change of velocity: positive = building, negative = fading
                                    let acceleration = velocity_5s - prev_velocity;
                                    prev_velocity = velocity_5s;

                                    let _ = velocity_tx.send((velocity_5s, velocity_1s, acceleration));

                                    // ── 60-minute drift ──────────────────────────────
                                    price_history_60m.push_back((now, price));
                                    while let Some((t, _)) = price_history_60m.front() {
                                        if now.duration_since(*t).as_secs() > 3600 {
                                            price_history_60m.pop_front();
                                        } else { break; }
                                    }
                                    let drift_60m = if price_history_60m.len() > 1 {
                                        if let Some((oldest_t, oldest_p)) = price_history_60m.front() {
                                            if now.duration_since(*oldest_t).as_secs() >= 3600 {
                                                price - oldest_p
                                            } else { dec!(0) }
                                        } else { dec!(0) }
                                    } else { dec!(0) };
                                    let _ = drift_60m_tx.send(drift_60m);
                                }
                            }
                        }
                    }
                }
            }
            warn!("⚠️ Binance Oracle disconnected. Reconnecting in 5s...");
            prev_velocity = dec!(0);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });

    // ── Binance Futures Funding Rate Poller ────────────────────────────────
    // Polls /fapi/v1/premiumIndex every BASIS_FUNDING_POLL_SECS (60s) to get
    // lastFundingRate.  Negative rate = shorts paying longs (bearish smart money).
    {
        let http_funding = Arc::clone(&shared_http);
        let funding_tx_bg = funding_tx.clone();
        let symbol_funding = match crypto_filter.as_str() {
            "eth" => "ETHUSDT",
            "sol" => "SOLUSDT",
            _     => "BTCUSDT",
        };
        tokio::spawn(async move {
            let url = format!(
                "https://fapi.binance.com/fapi/v1/premiumIndex?symbol={}",
                symbol_funding
            );
            loop {
                match http_funding.get(&url).send().await {
                    Ok(resp) => {
                        if let Ok(v) = resp.json::<serde_json::Value>().await {
                            if let Some(rate_str) = v.get("lastFundingRate").and_then(|r| r.as_str()) {
                                if let Ok(rate) = Decimal::from_str(rate_str) {
                                    let _ = funding_tx_bg.send(rate);
                                    debug!("📡 Funding rate {}: {:.6}%", symbol_funding, rate * dec!(100));
                                }
                            }
                        }
                    }
                    Err(e) => warn!("⚠️ Funding rate poll failed: {}", e),
                }
                tokio::time::sleep(std::time::Duration::from_secs(config::BASIS_FUNDING_POLL_SECS)).await;
            }
        });
    }

    let positions: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(PositionMap::new()));
    let total_pnl: Arc<Mutex<Decimal>> = Arc::new(Mutex::new(dec!(0)));
    let phantom_cooldowns: rustpolybot::helpers::balance::PhantomCooldowns = Arc::new(Mutex::new(HashMap::new()));
    let time_decay_positions: Arc<Mutex<HashMap<U256, TimeDecayPosition>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let (initial_hourly, initial_maker_market) = loop {
        let pair = get_market_pair(&shared_http).await;
        if pair.0.yes_token != U256::ZERO { break pair; }
        tokio::time::sleep(std::time::Duration::from_secs(90)).await;
    };

    let (initial_yes, initial_no, name, close_time) = (
        initial_hourly.yes_token, initial_hourly.no_token,
        initial_hourly.name.clone(), initial_hourly.close_time,
    );
    let desc = initial_hourly.description.clone();

    info!("🧪 Initializing market: {}", name);
    let mut initial_strike = rustpolybot::market_validator::extract_strike_price(&name);
    if initial_strike.is_none() {
        initial_strike = fetch_historical_strike_price(&shared_http, &crypto_filter, &desc).await;
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
    }

    let (market_tx, mut market_rx) = watch::channel((initial_yes, initial_no, name, close_time, initial_strike, desc, initial_maker_market));

    let http_monitor = Arc::clone(&shared_http);
    let market_tx_monitor = market_tx.clone();
    let crypto_filter_monitor = crypto_filter.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(90));
        loop {
            interval.tick().await;
            let (candidate, maker_candidate) = get_market_pair(&http_monitor).await;
            if candidate.yes_token == U256::ZERO { continue; }
            let (cur_yes, _, cur_name, cur_close_time, _, _, _) = market_tx_monitor.borrow().clone();

            if candidate.yes_token == cur_yes {
                // Hourly market unchanged — still check if maker market changed
                let cur_maker = market_tx_monitor.borrow().6.clone();
                let cur_maker_yes = cur_maker.as_ref().map(|m| m.yes_token);
                let new_maker_yes = maker_candidate.as_ref().map(|m| m.yes_token);
                if cur_maker_yes != new_maker_yes {
                    if let Some(ref mk) = maker_candidate {
                        info!("🏦 Maker market updated: \"{}\"", mk.name);
                    }
                    let (y, n, nm, ct, sp, ds, _) = market_tx_monitor.borrow().clone();
                    let _ = market_tx_monitor.send((y, n, nm, ct, sp, ds, maker_candidate));
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
                || (candidate_is_binary && !current_is_binary && !candidate_is_range && new_secs_left > 600 && cur_secs_left > 300);

            if !should_switch {
                continue;
            }

            info!("🔄 Market Switch Detected: {} -> {}", cur_name, candidate.name);
            let mut strike = rustpolybot::market_validator::extract_strike_price(&candidate.name);
            if strike.is_none() {
                strike = fetch_historical_strike_price(&http_monitor, &crypto_filter_monitor, &candidate.description).await;
            }
            if strike.is_none() {
                strike = fetch_historical_strike_price(&http_monitor, &crypto_filter_monitor, &candidate.name).await;
            }
            if strike.is_none() {
                strike = fetch_strike_price_from_close_time(&http_monitor, &crypto_filter_monitor, candidate.close_time).await;
            }
            let _ = market_tx_monitor.send((
                candidate.yes_token, candidate.no_token,
                candidate.name.clone(), candidate.close_time,
                strike, candidate.description.clone(),
                maker_candidate,
            ));
        }
    });

    loop {
        let (yes_token, no_token, market_name, market_close_time, strike_price, _, maker_market_candidate) = market_rx.borrow().clone();

        let now = Utc::now();
        if let Some(close_time) = market_close_time {
            let seconds_until_expiry = (close_time - now).num_seconds();
            if seconds_until_expiry < config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY {
                warn!("⚠️ Market expiring too soon ({}s left)!", seconds_until_expiry);
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                continue;
            }
            info!("⏰ Market closes in {}s", seconds_until_expiry);
        }

        info!("🚀 Starting Orchestrated Trading on market: \"{}\"", market_name);
        let market_started_at = Utc::now();

        let yes_fee_rate = trading_client.fee_rate_bps(yes_token).await.map(|r| r.base_fee).unwrap_or(0);
        let no_fee_rate = trading_client.fee_rate_bps(no_token).await.map(|r| r.base_fee).unwrap_or(0);
        let is_neg_risk = trading_client.neg_risk(yes_token).await.map(|r| r.neg_risk).unwrap_or(false);
        let verifying_contract = if is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };

        info!("✅ Cached Settings: NegRisk: {} | YES fee {} bps | NO fee {} bps", is_neg_risk, yes_fee_rate, no_fee_rate);

        let (yes_price_tx, yes_price_rx) = watch::channel::<PriceState>((dec!(0), dec!(0), dec!(1), dec!(0)));
        let (no_price_tx, no_price_rx) = watch::channel::<PriceState>((dec!(0), dec!(0), dec!(1), dec!(0)));

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
                            let (bid, bid_depth) = book.bids.iter()
                                .max_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
                                .map(|l| (l.price, l.size))
                                .unwrap_or((dec!(0), dec!(0)));
                            let (ask, ask_depth) = book.asks.iter()
                                .min_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
                                .map(|l| (l.price, l.size))
                                .unwrap_or((dec!(1), dec!(0)));
                            let _ = tx.send((bid, bid_depth, ask, ask_depth));
                        } else { break; }
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            });
        }

        // ── Maker market WS subscriptions (window/daily venue) ───────────────
        // When a window or daily market is available, subscribe its orderbook so
        // MakerStrategy can post quotes with live bid/ask data from that venue.
        let (maker_yes_price_rx, maker_no_price_rx) = if let Some(ref mk) = maker_market_candidate {
            let (mk_yes_tx, mk_yes_rx) = watch::channel::<PriceState>((dec!(0), dec!(0), dec!(1), dec!(0)));
            let (mk_no_tx, mk_no_rx) = watch::channel::<PriceState>((dec!(0), dec!(0), dec!(1), dec!(0)));
            for (token, tx) in [(mk.yes_token, mk_yes_tx), (mk.no_token, mk_no_tx)] {
                tokio::spawn(async move {
                    loop {
                        let client = WsClient::default();
                        let stream = match client.subscribe_orderbook(vec![token]) {
                            Ok(s) => s,
                            Err(_) => { tokio::time::sleep(std::time::Duration::from_secs(5)).await; continue; }
                        };
                        let mut stream = Box::pin(stream);
                        info!("✅ WS orderbook subscribed for maker token {}", token);
                        while let Some(book_result) = stream.next().await {
                            if let Ok(book) = book_result {
                                let (bid, bid_depth) = book.bids.iter()
                                    .max_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
                                    .map(|l| (l.price, l.size))
                                    .unwrap_or((dec!(0), dec!(0)));
                                let (ask, ask_depth) = book.asks.iter()
                                    .min_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
                                    .map(|l| (l.price, l.size))
                                    .unwrap_or((dec!(1), dec!(0)));
                                let _ = tx.send((bid, bid_depth, ask, ask_depth));
                            } else { break; }
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                });
            }
            (Some(mk_yes_rx), Some(mk_no_rx))
        } else {
            (None, None)
        };

        // Fetch maker market fee/risk settings if we have one
        let maker_market_config: Option<MarketConfig> = if let Some(ref mk) = maker_market_candidate {
            let mk_yes_fee = trading_client.fee_rate_bps(mk.yes_token).await.map(|r| r.base_fee).unwrap_or(0);
            let mk_no_fee = trading_client.fee_rate_bps(mk.no_token).await.map(|r| r.base_fee).unwrap_or(0);
            let mk_neg_risk = trading_client.neg_risk(mk.yes_token).await.map(|r| r.neg_risk).unwrap_or(false);
            info!("✅ Maker market settings: \"{}\" | NegRisk: {} | YES {} bps | NO {} bps",
                mk.name, mk_neg_risk, mk_yes_fee, mk_no_fee);
            Some(MarketConfig {
                yes_token: mk.yes_token,
                no_token: mk.no_token,
                market_name: mk.name.clone(),
                market_close_time: mk.close_time,
                strike_price,
                is_neg_risk: mk_neg_risk,
                yes_fee_bps: mk_yes_fee,
                no_fee_bps: mk_no_fee,
            })
        } else {
            None
        };

        let mut ticker = interval(config::main_ticker_interval());
        let mut status_ticker = interval(std::time::Duration::from_secs(60));
        let mut cleanup_ticker = interval(std::time::Duration::from_secs(300));
        let mut pulse_ticker = interval(std::time::Duration::from_secs(300));

        // ── Orchestrator state ──
        let strategies = StrategyRegistry::create_all_strategies();
        let risk_engine = RiskEngine::new();
        let mut last_trade_time: HashMap<String, Instant> = HashMap::new();
        // Tracks the last stop-loss exit time per strategy.
        // MakerStrategy enforces MAKER_STOP_LOSS_COOLDOWN_SECS before re-entering
        // after a stop-loss to avoid chasing adverse directional moves.
        let mut last_stop_loss_time: HashMap<String, Instant> = HashMap::new();
        let mut momentum_confirmation_count: u32 = 0;
        let mut last_momentum_signal_token: Option<U256> = None;
        let mut consecutive_failures: u32 = 0;
        let tg_token = env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default();
        let tg_chat_id = env::var("TELEGRAM_CHAT_ID").unwrap_or_default();

        info!("🤖 Orchestrator ready: {} strategies loaded", strategies.len());

        loop {
            tokio::select! {
                _ = market_rx.changed() => {
                    info!("🔄 Market switch detected — restarting trading loop with new market");
                    break; // break inner loop → outer loop picks up the new market
                }                _ = pulse_ticker.tick() => {
                    let start = Instant::now();
                    let mut req = BalanceAllowanceRequest::default();
                    req.asset_type = AssetType::Collateral;
                    let _ = trading_client.balance_allowance(req).await;
                    info!("📍 Network Pulse: {:?}", start.elapsed());
                }
                _ = cleanup_ticker.tick() => {
                    cleanup_expired_positions(Arc::clone(&positions), market_name.clone(), yes_token, no_token, market_close_time).await;

                    // Reconcile orphaned paired positions (Arbitrage/TimeDecay strategies)
                    if let Err(e) = reconcile_orphaned_positions(
                        Arc::clone(&positions),
                        &trading_client,
                        &nonce_manager,
                        &signer,
                        safe_address,
                        eoa_address,
                        &tg_token,
                        &tg_chat_id,
                        &shared_http,
                    ).await {
                        warn!("⚠️ Orphan reconciliation error: {}", e);
                    }

                    let mut td_map = time_decay_positions.lock().await;
                    let before_count = td_map.len();
                    td_map.retain(|_, pos| {
                        if pos.is_expired() {
                            false
                        } else {
                            true
                        }
                    });
                }
                _ = status_ticker.tick() => {
                    let (yes_bid, _, yes_ask, _) = *yes_price_rx.borrow();
                    let (no_bid,  _, no_ask,  _) = *no_price_rx.borrow();
                    let binance_price = *oracle_rx.borrow();

                    if yes_ask != dec!(1) && no_ask != dec!(1) {
                        if let Some(strike) = strike_price {
                            let ask_sum = yes_ask + no_ask;
                            let bid_sum = yes_bid + no_bid;
                            // ask_sum < 1.00 → "buy-both" arb opportunity (current strategy)
                            // bid_sum > 1.00 → "sell-both" (reverse arb via mint+sell) opportunity
                            info!("💓 Heartbeat | Ask Sum ${:.4} (Y ask ${:.2} / N ask ${:.2}) | Bid Sum ${:.4} (Y bid ${:.2} / N bid ${:.2}) | Binance: ${:.2}",
                                ask_sum, yes_ask, no_ask, bid_sum, yes_bid, no_bid, binance_price);
                            if bid_sum > dec!(1.0) {
                                info!("🔔 Reverse-Arb Signal: YES bid ${:.3} + NO bid ${:.3} = ${:.4} > $1.00 — mint+sell opportunity (check fees!)",
                                    yes_bid, no_bid, bid_sum);
                            }
                        }
                    }
                }
                _ = ticker.tick() => {
                    // If the market changed while we were waiting, break immediately
                    // instead of firing orders on a stale market token.
                    if market_rx.has_changed().unwrap_or(false) {
                        info!("🔄 Market switch detected during ticker — restarting trading loop");
                        break;
                    }

                    let (yes_bid, yes_bid_depth, yes_ask, yes_depth) = *yes_price_rx.borrow();
                    let (no_bid, no_bid_depth, no_ask, no_depth) = *no_price_rx.borrow();
                    let oracle_price = *oracle_rx.borrow();
                    let (velocity, velocity_1s, acceleration) = *velocity_rx.borrow();
                    let funding_rate = *funding_rx.borrow();
                    let oracle_drift_60m = *drift_60m_rx.borrow();

                    // Skip if prices not yet initialised from the orderbook WS
                    if yes_ask == dec!(1) && no_ask == dec!(1) {
                        continue;
                    }

                    // ── Build StrategyContext ──
                    let snapshot = MarketSnapshot {
                        yes_bid, yes_bid_depth, yes_ask, yes_ask_depth: yes_depth,
                        no_bid, no_bid_depth, no_ask, no_ask_depth: no_depth,
                        oracle_price, velocity, velocity_1s, acceleration,
                        funding_rate,
                        oracle_drift_60m,
                        timestamp: Utc::now(),
                    };
                    let market_cfg = MarketConfig {
                        yes_token, no_token,
                        market_name: market_name.clone(),
                        market_close_time,
                        strike_price,
                        is_neg_risk,
                        yes_fee_bps: yes_fee_rate,
                        no_fee_bps: no_fee_rate,
                    };

                    // Build optional maker snapshot from the window/daily venue prices
                    let maker_snapshot = match (&maker_yes_price_rx, &maker_no_price_rx) {
                        (Some(my_rx), Some(mn_rx)) => {
                            let (mk_yes_bid, mk_yes_bid_depth, mk_yes_ask, mk_yes_depth) = *my_rx.borrow();
                            let (mk_no_bid, mk_no_bid_depth, mk_no_ask, mk_no_depth) = *mn_rx.borrow();
                            Some(MarketSnapshot {
                                yes_bid: mk_yes_bid, yes_bid_depth: mk_yes_bid_depth,
                                yes_ask: mk_yes_ask, yes_ask_depth: mk_yes_depth,
                                no_bid: mk_no_bid, no_bid_depth: mk_no_bid_depth,
                                no_ask: mk_no_ask, no_ask_depth: mk_no_depth,
                                oracle_price, velocity, velocity_1s, acceleration,
                                funding_rate,
                                oracle_drift_60m,
                                timestamp: Utc::now(),
                            })
                        }
                        _ => None,
                    };

                    let ctx = StrategyContext {
                        market: market_cfg,
                        snapshot,
                        positions: Arc::clone(&positions),
                        crypto_filter: crypto_filter.clone(),
                        market_started_at,
                        maker_market: maker_market_config.clone(),
                        maker_snapshot,
                    };

                    // ── Evaluate all strategies ──
                    let eval_result = match execute_strategies_concurrent(&strategies, &ctx, 500).await {
                        Ok(r) => r,
                        Err(e) => { warn!("⚠️ Strategy evaluation error: {}", e); continue; }
                    };
                    let (resolved_signals, conflicts) = aggregate_and_resolve_signals(&eval_result);
                    for c in &conflicts {
                        warn!("⚠️ Signal conflict: {:?}", c);
                    }

                    if resolved_signals.is_empty() {
                        // No signals from any strategy — reset momentum confirmation counter.
                        momentum_confirmation_count = 0;
                        last_momentum_signal_token = None;
                        continue;
                    }

                    // Reset per-tick failure counter so circuit breaker counts across ticks, not signals within one tick
                    let mut tick_failures: u32 = 0;

                    // ── Process each resolved signal ──
                    for (strategy_name, signal) in &resolved_signals {
                        // ArbitrageStrategy: route to the window/daily maker market when available.
                        // Shadow the outer hourly-market variables for this loop iteration only so
                        // all downstream order-placement and exit logic uses the correct venue.
                        let (yes_token, no_token, yes_ask, no_ask, yes_bid, no_bid,
                             yes_depth, no_depth, yes_fee_rate, no_fee_rate,
                             market_close_time, market_name) =
                            if strategy_name == "ArbitrageStrategy" {
                                if let (Some(ref mk), Some(ref mky_rx), Some(ref mkn_rx)) =
                                    (&maker_market_config, &maker_yes_price_rx, &maker_no_price_rx)
                                {
                                    let (mky_bid, _, mky_ask, mky_depth) = *mky_rx.borrow();
                                    let (mkn_bid, _, mkn_ask, mkn_depth) = *mkn_rx.borrow();
                                    (mk.yes_token, mk.no_token,
                                     mky_ask, mkn_ask, mky_bid, mkn_bid,
                                     mky_depth, mkn_depth,
                                     mk.yes_fee_bps, mk.no_fee_bps,
                                     mk.market_close_time, mk.market_name.clone())
                                } else {
                                    (yes_token, no_token, yes_ask, no_ask, yes_bid, no_bid,
                                     yes_depth, no_depth, yes_fee_rate, no_fee_rate,
                                     market_close_time, market_name.clone())
                                }
                            } else {
                                (yes_token, no_token, yes_ask, no_ask, yes_bid, no_bid,
                                 yes_depth, no_depth, yes_fee_rate, no_fee_rate,
                                 market_close_time, market_name.clone())
                            };

                        match signal {
                            // ════════════════════ EXIT ════════════════════
                            StrategySignal::Exit { token_id, reason } => {
                                // ── Maker venue routing for EXIT ──────────────────────────────────
                                // MakerStrategy opens positions on the window/daily maker venue
                                // (mk_yes_token / mk_no_token). The generic yes_token/no_token here
                                // are the HOURLY market tokens. If we compute bid or fee from the
                                // wrong venue we get a stale/wrong price and the FAK sell is
                                // rejected → circuit breaker fires → shares are orphaned on-chain.
                                let (exit_bid, exit_fee_bps, exit_verifying) = {
                                    let mk_yes = maker_market_config.as_ref().map(|m| m.yes_token).unwrap_or(yes_token);
                                    let mk_no  = maker_market_config.as_ref().map(|m| m.no_token).unwrap_or(no_token);
                                    let mk_neg = maker_market_config.as_ref().map(|m| m.is_neg_risk).unwrap_or(is_neg_risk);
                                    let mk_vc  = if mk_neg { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };

                                    if *token_id == mk_yes {
                                        let b = maker_yes_price_rx.as_ref()
                                            .map(|rx| rx.borrow().0)
                                            .unwrap_or(yes_bid);
                                        let f = maker_market_config.as_ref().map(|m| m.yes_fee_bps as u16).unwrap_or(yes_fee_rate as u16);
                                        (b, f, mk_vc)
                                    } else if *token_id == mk_no {
                                        let b = maker_no_price_rx.as_ref()
                                            .map(|rx| rx.borrow().0)
                                            .unwrap_or(no_bid);
                                        let f = maker_market_config.as_ref().map(|m| m.no_fee_bps as u16).unwrap_or(no_fee_rate as u16);
                                        (b, f, mk_vc)
                                    } else if *token_id == yes_token {
                                        (yes_bid, yes_fee_rate as u16, verifying_contract)
                                    } else {
                                        (no_bid, no_fee_rate as u16, verifying_contract)
                                    }
                                };
                                let bid = exit_bid;
                                let sell_price = (bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE);
                                let fee_bps = exit_fee_bps;
                                let exit_vc = exit_verifying;
                                let pos_key = (strategy_name.clone(), *token_id);

                                let shares = {
                                    let pos_map = positions.lock().await;
                                    match pos_map.get(&pos_key) {
                                        Some(p) => p.shares,
                                        None => continue, // no position to exit
                                    }
                                };

                                // Dust position guard: if shares are too small to sell,
                                // just clean up the position instead of sending an order
                                // that the exchange will reject with "invalid amounts".
                                if shares < config::MIN_ORDER_SHARES {
                                    let mut pos_map = positions.lock().await;
                                    if let Some(pos) = pos_map.remove(&pos_key) {
                                        let pnl = (bid - pos.avg_entry) * pos.shares;
                                        *total_pnl.lock().await += pnl;
                                        warn!("🧹 EXIT [{}]: Dust position removed ({:.6} shares < min {}). PnL ${:.4}",
                                            strategy_name, shares, config::MIN_ORDER_SHARES, pnl);
                                    }
                                    continue;
                                }

                                // Dead market guard: if bid is zero the market is expired/resolved
                                // and there are no buyers. Write off the position instead of
                                // sending unsellable orders that trigger the circuit breaker.
                                if bid <= Decimal::ZERO {
                                    let mut pos_map = positions.lock().await;
                                    if let Some(pos) = pos_map.remove(&pos_key) {
                                        let pnl = -pos.avg_entry * pos.shares;
                                        *total_pnl.lock().await += pnl;
                                        warn!("🧹 EXIT [{}]: Position written off (bid=$0, market expired/resolved). shares={:.2}, loss=${:.4}",
                                            strategy_name, pos.shares, pnl);
                                    }
                                    consecutive_failures = 0;
                                    continue;
                                }

                                info!("📤 EXIT [{}]: {} | shares={:.2}, bid=${:.4} | {}", strategy_name, market_name, shares, bid, reason);

                                if !config::GHOST_MODE {
                                    if let Err(e) = place_limit_order(
                                        &trading_client, &nonce_manager, &signer, safe_address, eoa_address,
                                        exit_vc, *token_id, Side::Sell, shares, sell_price, fee_bps, OrderType::FAK, false, 0,
                                        &shared_http,
                                    ).await {
                                        let err_str = e.to_string();
                                        if err_str.contains("not enough balance") || err_str.contains("balance: 0") {
                                            let mut pos_map = positions.lock().await;
                                            if let Some(pos) = pos_map.remove(&pos_key) {
                                                if pos.fill_confirmed_at.is_some() {
                                                    // Position was confirmed held on-chain (fill_confirmed_at set),
                                                    // meaning these shares were real and are now confirmed sold
                                                    // (exchange balance=0).  This happens when:
                                                    //   1. A FAK exit returned 200 OK but filled 0 shares
                                                    //   2. PARTIAL EXIT re-inserted the position
                                                    //   3. The re-exit FAK sold the shares but returned a transient
                                                    //      500, so the subsequent retry hit "not enough balance"
                                                    // Record PnL at current bid (best approximation of exit price).
                                                    let pnl = (bid - pos.avg_entry) * pos.shares;
                                                    *total_pnl.lock().await += pnl;
                                                    warn!("🧹 EXIT [{}]: Position sold (balance=0 confirmed). PnL ${:.4} | token {}", strategy_name, pnl, token_id);
                                                } else {
                                                    // True phantom: GTD order never filled, balance was always 0.
                                                    warn!("🧹 EXIT [{}]: Phantom position removed for token {} (exchange balance=0). No shares owned.", strategy_name, token_id);
                                                }
                                            }
                                            // Apply cooldown so the strategy doesn't immediately re-enter
                                            // on the next tick while the market is still in a crashed state.
                                            last_trade_time.insert(strategy_name.clone(), Instant::now());
                                            consecutive_failures = 0;
                                            tick_failures = 0;
                                            continue;
                                        }
                                        // "invalid price" means the market is likely expired/resolved
                                        // (bid=$0 → sell_price=$0.01 but no valid orderbook).
                                        // Clean up to prevent infinite retry loops.
                                        if err_str.contains("invalid price") {
                                            let mut pos_map = positions.lock().await;
                                            if let Some(pos) = pos_map.remove(&pos_key) {
                                                let pnl = (bid - pos.avg_entry) * pos.shares;
                                                *total_pnl.lock().await += pnl;
                                                warn!("🧹 EXIT [{}]: Position removed after invalid price error (market likely expired). PnL ${:.4}", strategy_name, pnl);
                                            }
                                            last_trade_time.insert(strategy_name.clone(), Instant::now());
                                            consecutive_failures = 0;
                                            tick_failures = 0;
                                            continue;
                                        }
                                        warn!("⚠️ Exit order failed: {}", e);
                                        // "no orders found to match with FAK order" means the market bid
                                        // moved between snapshot time and order arrival — a normal market
                                        // microstructure event, NOT a system failure.  Don't charge the
                                        // circuit breaker; the next tick will re-evaluate at the fresh bid.
                                        if err_str.contains("no orders found") {
                                            continue; // skip failure counters
                                        }
                                        tick_failures += 1;
                                        consecutive_failures += 1;
                                        if consecutive_failures >= config::MAX_CONSECUTIVE_FAILURES {
                                            error!("🚨 Circuit breaker: {} consecutive failures (EXIT) — pausing", consecutive_failures);
                                            {
                                                let mut pos_map = positions.lock().await;
                                                if !pos_map.is_empty() {
                                                    warn!("🧹 Circuit breaker: clearing {} local positions to resync with exchange", pos_map.len());
                                                    pos_map.clear();
                                                }
                                            }
                                            let _ = send_notification(&tg_token, &tg_chat_id,
                                                &format!("🚨 Circuit breaker hit after {} EXIT failures on {}", consecutive_failures, market_name)).await;
                                            tokio::select! {
                                                _ = tokio::time::sleep(Duration::from_secs(config::FAILURE_COOLDOWN_SECS as u64)) => {
                                                    sync_nonce_manager(&nonce_manager, &shared_http, safe_address).await;
                                                }
                                                _ = market_rx.changed() => {
                                                    info!("🔄 Market switch detected during circuit breaker cooldown");
                                                }
                                            }
                                            consecutive_failures = 0;
                                            tick_failures = 0;
                                        }
                                        continue;
                                    }
                                }

                                // Update positions & PnL
                                // FAK exits may partially fill if book depth < order size.
                                // We remove the position immediately (prevents double-sell on the
                                // next 50ms tick), then spawn a deferred balance check that
                                // re-inserts any remaining on-chain shares so the exit handler
                                // can retry on the next signal rather than letting them expire.
                                let (removed_avg_entry, removed_shares, removed_close_time) = {
                                    let mut pos_map = positions.lock().await;
                                    if let Some(pos) = pos_map.remove(&pos_key) {
                                        let pnl = (bid - pos.avg_entry) * pos.shares;
                                        *total_pnl.lock().await += pnl;
                                        info!("💰 Position closed [{}]: PnL ${:.4}", strategy_name, pnl);
                                        (pos.avg_entry, pos.shares, pos.close_time)
                                    } else {
                                        (dec!(0), dec!(0), None)
                                    }
                                };
                                // Spawn post-sell balance check to catch FAK partial fills
                                if removed_shares > dec!(0) {
                                    let positions_ps  = Arc::clone(&positions);
                                    let client_ps     = Arc::clone(&trading_client);
                                    let total_pnl_ps  = Arc::clone(&total_pnl);
                                    let strategy_ps   = strategy_name.clone();
                                    let token_ps      = *token_id;
                                    let bid_ps        = bid;
                                    let mkt_name_ps   = market_name.clone();
                                    let tg_tok_ps     = tg_token.clone();
                                    let tg_chat_ps    = tg_chat_id.clone();
                                    tokio::spawn(async move {
                                        tokio::time::sleep(Duration::from_millis(2500)).await;
                                        let mut req = BalanceAllowanceRequest::default();
                                        req.asset_type = AssetType::Conditional;
                                        req.token_id = Some(token_ps);
                                        let remaining = match client_ps.balance_allowance(req).await {
                                            Ok(resp) => Decimal::from_str(&resp.balance.to_string())
                                                            .unwrap_or(dec!(0)) / dec!(1_000_000),
                                            Err(_) => return, // can't confirm — leave removed
                                        };
                                        if remaining >= crate::config::MIN_ORDER_SHARES {
                                            // Partial fill: correct overcounted PnL and re-insert.
                                            //
                                            // The first close booked PnL on `removed_shares`.  We need to
                                            // reverse only the portion that was NOT actually sold — i.e. the
                                            // shares that are still on-chain AND were counted in the first close.
                                            //
                                            // IMPORTANT: `remaining` can be GREATER than `removed_shares` when
                                            // a slow-settling GTD order was still filling during the exit window
                                            // (Polygon delivers fills in batches).  In that case the first close
                                            // only booked PnL for `removed_shares`, so the correction ceiling is
                                            // `removed_shares`, not `remaining`.  Using `remaining` directly
                                            // over-corrects and understates the true loss.
                                            //
                                            // Example (this exact bug, 2026-04-24):
                                            //   removed_shares = 2.45  (position snapped mid-settlement)
                                            //   remaining      = 24.55 (22.1 more shares arrived 2.5s later)
                                            //   filled         = 0     (FAK was killed — 0 sold)
                                            //   OLD correction = -((bid - entry) × 24.55) = +$0.491  ← wrong
                                            //   NEW correction = -((bid - entry) × 2.45)  = +$0.049  ← correct
                                            //   Net recorded OLD: -$0.049 + $0.491 - $0.491 = -$0.049  (off by $0.442)
                                            //   Net recorded NEW: -$0.049 + $0.049 - $0.491 = -$0.491  ✓
                                            let filled = (removed_shares - remaining).max(dec!(0));
                                            let over_booked_shares = remaining.min(removed_shares);
                                            let pnl_correction = -((bid_ps - removed_avg_entry) * over_booked_shares);
                                            *total_pnl_ps.lock().await += pnl_correction;
                                            warn!("⚠️ PARTIAL EXIT [{}]: FAK sold {:.4}/{:.4} shares; \
                                                   {:.4} remain on-chain. PnL corrected by ${:.4}. Re-inserting for re-exit.",
                                                  strategy_ps, filled, removed_shares, remaining, pnl_correction);
                                            let pos_key_ps = (strategy_ps.clone(), token_ps);
                                            let mut pos_map = positions_ps.lock().await;
                                            if !pos_map.contains_key(&pos_key_ps) {
                                                pos_map.insert(pos_key_ps, Position {
                                                    shares: remaining,
                                                    avg_entry: removed_avg_entry,
                                                    opened_at: Utc::now(),
                                                    close_time: removed_close_time,
                                                    market_name: mkt_name_ps,
                                                    pair_token_id: token_ps,
                                                    fill_confirmed_at: Some(Utc::now()),
                                                    paired_leg_token_id: None,
                                                });
                                            }
                                            drop(pos_map);
                                            let _ = send_notification(&tg_tok_ps, &tg_chat_ps,
                                                &format!("⚠️ Partial exit [{strategy_ps}]: {filled:.4}/{removed_shares:.4} shares sold. \
                                                          {remaining:.4} remain on-chain — re-inserted for re-exit."),
                                            ).await;
                                        }
                                    });
                                }

                                // For paired strategies, also exit the other leg
                                let is_paired = strategy_name == "ArbitrageStrategy" || strategy_name == "TimeDecayStrategy";
                                if is_paired {
                                    let pair_token = if *token_id == yes_token { no_token } else { yes_token };
                                    let pair_bid = if pair_token == yes_token { yes_bid } else { no_bid };
                                    let pair_sell = (pair_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE);
                                    let pair_fee = if pair_token == yes_token { yes_fee_rate as u16 } else { no_fee_rate as u16 };
                                    let pair_key = (strategy_name.clone(), pair_token);

                                    let pair_shares = {
                                        let pos_map = positions.lock().await;
                                        pos_map.get(&pair_key).map(|p| p.shares)
                                    };
                                    if let Some(ps) = pair_shares {
                                        info!("📤 EXIT (paired) [{}]: {} | shares={:.2}, bid=${:.4}", strategy_name, market_name, ps, pair_bid);
                                        if !config::GHOST_MODE {
                                            if let Err(e) = place_limit_order(
                                                &trading_client, &nonce_manager, &signer, safe_address, eoa_address,
                                                verifying_contract, pair_token, Side::Sell, ps, pair_sell, pair_fee, OrderType::FAK, false, 0,
                                                &shared_http,
                                            ).await {
                                                warn!("⚠️ Paired exit order failed: {}", e);
                                            }
                                        }
                                        let (pair_avg, pair_shares_rm, pair_close) = {
                                            let mut pos_map = positions.lock().await;
                                            if let Some(pos) = pos_map.remove(&pair_key) {
                                                let pnl = (pair_bid - pos.avg_entry) * pos.shares;
                                                *total_pnl.lock().await += pnl;
                                                info!("💰 Paired position closed [{}]: PnL ${:.4}", strategy_name, pnl);
                                                (pos.avg_entry, pos.shares, pos.close_time)
                                            } else { (dec!(0), dec!(0), None) }
                                        };
                                        if pair_shares_rm > dec!(0) {
                                            let positions_pp  = Arc::clone(&positions);
                                            let client_pp     = Arc::clone(&trading_client);
                                            let total_pnl_pp  = Arc::clone(&total_pnl);
                                            let strategy_pp   = strategy_name.clone();
                                            let mkt_pp        = market_name.clone();
                                            tokio::spawn(async move {
                                                tokio::time::sleep(Duration::from_millis(2500)).await;
                                                let mut req = BalanceAllowanceRequest::default();
                                                req.asset_type = AssetType::Conditional;
                                                req.token_id = Some(pair_token);
                                                let remaining = match client_pp.balance_allowance(req).await {
                                                    Ok(resp) => Decimal::from_str(&resp.balance.to_string())
                                                                    .unwrap_or(dec!(0)) / dec!(1_000_000),
                                                    Err(_) => return,
                                                };
                                                if remaining >= crate::config::MIN_ORDER_SHARES {
                                                    let filled = (pair_shares_rm - remaining).max(dec!(0));
                                                    let correction = -((pair_bid - pair_avg) * remaining);
                                                    *total_pnl_pp.lock().await += correction;
                                                    warn!("⚠️ PARTIAL EXIT (paired) [{}]: FAK sold {:.4}/{:.4} shares; \
                                                           {:.4} remain. PnL corrected by ${:.4}. Re-inserting.",
                                                          strategy_pp, filled, pair_shares_rm, remaining, correction);
                                                    let pk = (strategy_pp.clone(), pair_token);
                                                    let mut pos_map = positions_pp.lock().await;
                                                    if !pos_map.contains_key(&pk) {
                                                        pos_map.insert(pk, Position {
                                                            shares: remaining,
                                                            avg_entry: pair_avg,
                                                            opened_at: Utc::now(),
                                                            close_time: pair_close,
                                                            market_name: mkt_pp,
                                                            pair_token_id: pair_token,
                                                            fill_confirmed_at: Some(Utc::now()),
                                                            paired_leg_token_id: None,
                                                        });
                                                    }
                                                }
                                            });
                                        }
                                    }
                                }

                                consecutive_failures = 0;
                                momentum_confirmation_count = 0;
                                last_momentum_signal_token = None;
                                last_trade_time.insert(strategy_name.clone(), Instant::now());

                                // Record stop-loss time so the strategy waits MAKER_STOP_LOSS_COOLDOWN_SECS
                                // before re-entering, preventing immediate re-entry into an adverse trend.
                                if reason.contains("stop-loss") {
                                    last_stop_loss_time.insert(strategy_name.clone(), Instant::now());
                                }

                                let session_pnl = *total_pnl.lock().await;
                                let msg = format!("📤 EXIT [{}] {} | bid=${:.4} | reason: {} | Session PnL: ${:.4}", strategy_name, market_name, bid, reason, session_pnl);
                                let _ = send_notification(&tg_token, &tg_chat_id, &msg).await;
                            }

                            // ════════════════════ ENTRY ════════════════════
                            StrategySignal::Entry { token_id } => {
                                // Close-time guard: block new entries if market expires within
                                // MIN_SECONDS_TO_EXPIRY_FOR_ENTRY seconds to avoid the
                                // "could not run the execution" 500 error from Polymarket's CLOB
                                // when placing orders on an expiring/resolving market.
                                if let Some(close_time) = market_close_time {
                                    let secs_left = (close_time - Utc::now()).num_seconds();
                                    if secs_left < config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY {
                                        debug!("⏸️ ENTRY [{}]: blocked — market closes in {}s (<{}s threshold)",
                                            strategy_name, secs_left, config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY);
                                        continue;
                                    }
                                }

                                // Per-strategy cooldown gate
                                if let Some(lt) = last_trade_time.get(strategy_name.as_str()) {
                                    let elapsed = lt.elapsed();
                                    let cooldown = Duration::from_secs(config::TRADE_COOLDOWN_SECS as u64);
                                    if elapsed < cooldown {
                                        debug!("⏸️ ENTRY [{}]: signal suppressed — cooldown ({:.1}s remaining)",
                                            strategy_name, (cooldown - elapsed).as_secs_f32());
                                        continue;
                                    }
                                }

                                // Phantom cooldown gate: block re-entry after an unfilled GTD order
                                // was removed by sync_position_balance, to prevent phantom loops.
                                {
                                    let pc = phantom_cooldowns.lock().await;
                                    let cooldown_key = format!("{}:{}", strategy_name, token_id);   // ← NEW: per-token key
                                    if let Some(removed_at) = pc.get(&cooldown_key) {
                                        let elapsed = removed_at.elapsed();
                                        let cooldown = Duration::from_secs(rustpolybot::helpers::balance::PHANTOM_COOLDOWN_SECS);
                                        if elapsed < cooldown {
                                            debug!("⏸️ ENTRY [{} | Token {}]: signal suppressed — phantom cooldown ({:.0}s remaining)",
                                                strategy_name, token_id, (cooldown - elapsed).as_secs_f32());
                                            continue;
                                        }
                                    }
                                }

                                // Stop-loss cooldown gate (MakerStrategy only): after a stop-loss
                                // exit, block re-entry for MAKER_STOP_LOSS_COOLDOWN_SECS to avoid
                                // immediately re-posting into the same adverse directional move.
                                if strategy_name == "MakerStrategy" {
                                    if let Some(sl_time) = last_stop_loss_time.get(strategy_name.as_str()) {
                                        let elapsed = sl_time.elapsed();
                                        let cooldown = Duration::from_secs(config::MAKER_STOP_LOSS_COOLDOWN_SECS as u64);
                                        if elapsed < cooldown {
                                            debug!("⏸️ ENTRY [{}]: stop-loss cooldown ({:.0}s remaining)",
                                                strategy_name, (cooldown - elapsed).as_secs_f32());
                                            continue;
                                        }
                                    }
                                }

                                // Momentum confirmation gate
                                if strategy_name == "MomentumStrategy" {
                                    if last_momentum_signal_token == Some(*token_id) {
                                        momentum_confirmation_count += 1;
                                    } else {
                                        momentum_confirmation_count = 1;
                                        last_momentum_signal_token = Some(*token_id);
                                    }
                                    if momentum_confirmation_count < config::MOMENTUM_CONFIRMATION_TICKS {
                                        info!("⏳ Momentum confirmation {}/{}", momentum_confirmation_count, config::MOMENTUM_CONFIRMATION_TICKS);
                                        continue;
                                    }
                                }

                                // Compute order params (no lock needed)
                                let ask = if *token_id == yes_token { yes_ask } else { no_ask };
                                let bid = if *token_id == yes_token { yes_bid } else { no_bid };
                                let depth = if *token_id == yes_token { yes_depth } else { no_depth };
                                let fee_bps = if *token_id == yes_token { yes_fee_rate as u16 } else { no_fee_rate as u16 };

                                let is_maker = strategy_name == "MakerStrategy";
                                let order_base_price = if is_maker { bid } else { ask };

                                if order_base_price <= dec!(0) { continue; }

                                // ── Fractional Kelly sizing (MomentumStrategy only) ──────────────
                                // Scale trade size proportionally to signal strength.
                                // At 1× threshold → MOMENTUM_MIN_TRADE_SIZE_USDC ($5).
                                // At MOMENTUM_KELLY_MAX_MULTIPLIER× threshold → MOMENTUM_MAX_TRADE_SIZE_USDC ($25).
                                // All other strategies use the flat env-var trade size.
                                let effective_trade_size = if strategy_name == "MomentumStrategy" {
                                    let threshold = match crypto_filter.as_str() {
                                        "eth" => config::ETH_MOMENTUM_THRESHOLD,
                                        "sol" => config::SOL_MOMENTUM_THRESHOLD,
                                        _     => config::BTC_MOMENTUM_THRESHOLD,
                                    };
                                    rustpolybot::strategies::momentum_impl::kelly_momentum_size(velocity, threshold)
                                } else if strategy_name == "BasisStrategy" {
                                    // Compute current yes_mid skew to drive Kelly sizing
                                    let yes_mid = if yes_bid > dec!(0) && yes_ask < dec!(1) {
                                        (yes_bid + yes_ask) / dec!(2)
                                    } else { dec!(0.5) };
                                    let skew_abs = (yes_mid - dec!(0.50)).abs();
                                    rustpolybot::strategies::basis_impl::basis_trade_size(skew_abs)
                                } else {
                                    trade_size_usdc
                                };

                                // ── Late-market size reduction ────────────────────────────────────
                                // When approaching expiry, scale down position size to limit
                                // adverse-selection exposure in illiquid/decided markets.
                                // Linear taper: 100% at ≥ LATE_MARKET_SIZE_THRESHOLD_SECS, 50% at
                                // MIN_SECONDS_TO_EXPIRY_FOR_ENTRY. Maker GTD orders are exempt
                                // because MAKER_MIN_SECS_TO_EXPIRY already enforces a safe window.
                                let effective_trade_size = if !is_maker {
                                    if let Some(close_time) = market_close_time {
                                        let secs_left = (close_time - Utc::now()).num_seconds().max(0);
                                        let threshold = config::LATE_MARKET_SIZE_THRESHOLD_SECS;
                                        let floor = config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY;
                                        if secs_left < threshold && secs_left >= floor {
                                            let range = (threshold - floor) as f64;
                                            let remaining = (secs_left - floor) as f64;
                                            let scale_f = 0.5 + 0.5 * (remaining / range);
                                            let scale = Decimal::try_from(scale_f).unwrap_or(dec!(1));
                                            let scaled = effective_trade_size * scale;
                                            if scaled < config::MIN_ORDER_USDC {
                                                debug!("⏸️ ENTRY [{}]: late-market size ${:.2} below MIN_ORDER_USDC — skipping",
                                                    strategy_name, scaled);
                                                continue;
                                            }
                                            debug!("📉 ENTRY [{}]: late-market size taper {:.0}% ({}s left) → ${:.2}",
                                                strategy_name, scale_f * 100.0, secs_left, scaled);
                                            scaled
                                        } else {
                                            effective_trade_size
                                        }
                                    } else {
                                        effective_trade_size
                                    }
                                } else {
                                    effective_trade_size
                                };

                                let shares = effective_trade_size / order_base_price;
                                if shares < config::MIN_ORDER_SHARES || effective_trade_size < config::MIN_ORDER_USDC { continue; }
                                if !is_maker && depth < shares * config::MIN_LIQUIDITY_FILL_RATIO { continue; }
                                // `effective_trade_size` is used for risk checks and order placement below.

                                // ── Dynamic spread-relative bid improvement (Maker only) ──────────
                                // Use a fraction of the live spread so we always post below the ask,
                                // even in tight-spread markets where a fixed tick would cross the book.
                                let maker_improvement = if is_maker {
                                    let spread = ask - bid;
                                    if spread > dec!(0) {
                                        (spread * config::MAKER_BID_IMPROVEMENT_RATIO)
                                            .max(config::MAKER_MIN_BID_IMPROVEMENT)
                                            .min(config::MAKER_MAX_BID_IMPROVEMENT)
                                    } else {
                                        config::MAKER_BID_IMPROVEMENT // fallback: zero-spread market
                                    }
                                } else {
                                    dec!(0)
                                };

                                let buy_price = if is_maker {
                                    (bid + maker_improvement).min(config::MAX_BUY_LIMIT_PRICE)
                                } else if strategy_name == "MomentumStrategy" {
                                    (ask + config::BUY_PRICE_OFFSET + config::MOMENTUM_BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE)
                                } else {
                                    (ask + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE)
                                };
                                let side_label = if *token_id == yes_token { "YES" } else { "NO" };

                                // ── Determine if this is a paired strategy ──────────────────────────
                                let is_paired = strategy_name == "ArbitrageStrategy" || strategy_name == "TimeDecayStrategy";

                                // ── Atomic check-and-reserve (TOCTOU fix) ────────────────────────
                                // Per-strategy key: only block re-entry within the same strategy's
                                // own book.  Other strategies can enter the same token independently.
                                let pos_key = (strategy_name.clone(), *token_id);
                                let collateral = *starting_collateral.lock().await;
                                let session_pnl = *total_pnl.lock().await;
                                let (risk_yes_price, risk_no_price) = if is_maker { (yes_bid, no_bid) } else { (yes_ask, no_ask) };
                                let strategy_budget = RiskEngine::strategy_max_exposure(strategy_name);
                                {
                                    let mut pos_map = positions.lock().await;
                                    if pos_map.contains_key(&pos_key) { continue; }
                                    // Exposure: only count this strategy's own positions
                                    let current_exposure = pos_map.iter()
                                        .filter(|((s, _), _)| s == strategy_name)
                                        .map(|(_, p)| p.shares * p.avg_entry)
                                        .sum::<Decimal>();
                                    if !risk_engine.approve_buy(risk_yes_price, risk_no_price, current_exposure, effective_trade_size, collateral, session_pnl, strategy_budget,
                                        strategy_name != "ArbitrageStrategy" && strategy_name != "TimeDecayStrategy") {
                                        info!("🚫 ENTRY [{}]: signal suppressed — risk check failed (exposure=${:.4}, budget=${:.4}, trade=${:.4})",
                                            strategy_name, current_exposure, strategy_budget, effective_trade_size);
                                        if strategy_name == "MomentumStrategy" {
                                            momentum_confirmation_count = 0;
                                            last_momentum_signal_token = None;
                                        }
                                        // Apply cooldown after risk rejection to prevent log flooding
                                        last_trade_time.insert(strategy_name.clone(), Instant::now());
                                        continue;
                                    }
                                    // Reserve the slot atomically
                                    // For Maker, use buy_price (bid + dynamic improvement) as avg_entry
                                    // since that's the actual limit price posted, not the raw bid.
                                    let sentinel_entry = if is_maker {
                                        (bid + maker_improvement).min(config::MAX_BUY_LIMIT_PRICE)
                                    } else {
                                        order_base_price
                                    };
                                    pos_map.insert(pos_key.clone(), Position {
                                        shares,
                                        avg_entry: sentinel_entry,
                                        opened_at: Utc::now(),
                                        close_time: market_close_time,
                                        market_name: market_name.clone(),
                                        pair_token_id: *token_id,
                                        fill_confirmed_at: None,
                                        paired_leg_token_id: if is_paired {
                                            Some(if *token_id == yes_token { no_token } else { yes_token })
                                        } else {
                                            None
                                        },
                                    });
                                }
                                // ─────────────────────────────────────────────────────────────────

                                let rounded_price = if is_maker { floor_to_tick_size(buy_price) } else { round_to_tick_size(buy_price) };
                                // Patch sentinel avg_entry to the floored price so stop-loss math is accurate.
                                // Previously, avg_entry was set to the pre-floor buy_price, causing the
                                // stop-loss to fire prematurely (e.g. at -3.1% when threshold is higher)
                                // because the actual order fills at rounded_price, not buy_price.
                                if is_maker && rounded_price != buy_price {
                                    if let Some(pos) = positions.lock().await.get_mut(&pos_key) {
                                        pos.avg_entry = rounded_price;
                                    }
                                }
                                if (rounded_price - buy_price).abs() > rust_decimal::Decimal::ZERO {
                                    info!("📥 ENTRY [{}]: {} {} | shares={:.2}, price=${:.4} (rounded from ${:.10})", strategy_name, side_label, market_name, shares, rounded_price, buy_price);
                                } else if is_maker {
                                    let spread = ask - bid;
                                    info!("📥 ENTRY [{}]: {} {} | shares={:.2}, price=${:.4} (spread=${:.4}, improvement=${:.4})", strategy_name, side_label, market_name, shares, buy_price, spread, maker_improvement);
                                } else {
                                    info!("📥 ENTRY [{}]: {} {} | shares={:.2}, price=${:.4}", strategy_name, side_label, market_name, shares, buy_price);
                                }

                                // Snapshot the pre-order on-chain balance so the sync task can compute
                                // the true fill delta (actual - baseline) rather than the raw total.
                                // This prevents residual shares from previous trades inflating positions.
                                let baseline_shares = {
                                    let mut req = BalanceAllowanceRequest::default();
                                    req.asset_type = AssetType::Conditional;
                                    req.token_id = Some(*token_id);
                                    match trading_client.balance_allowance(req).await {
                                        Ok(resp) => Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000),
                                        Err(_) => dec!(0),
                                    }
                                };

                                if !config::GHOST_MODE {
                                    let (order_type, post_only, exp) = if is_maker { (OrderType::GTD, true, config::MAKER_GTD_TTL_SECS) } else { (OrderType::FAK, false, 0u64) };
                                    // Makers are never charged fees on Polymarket — embed 0 bps in the
                                    // signed order struct for post-only orders.  Takers embed the market
                                    // fee rate so the exchange can validate and collect it on fill.
                                // Polymarket fee rates are per-market, not per-order-type.
                                // Some markets (e.g. hourly crypto) charge maker fees (up to 1000 bps).
                                // Always use the actual market fee rate from the API.
                                let order_fee_bps = fee_bps;

                                    if let Err(e) = place_limit_order(
                                        &trading_client, &nonce_manager, &signer, safe_address, eoa_address,
                                        verifying_contract, *token_id, Side::Buy, shares, buy_price, order_fee_bps, order_type, post_only, exp,
                                        &shared_http,
                                    ).await {
                                        let err_str = e.to_string();
                                        // "crosses book" is a market-microstructure rejection, NOT a system failure.
                                        // Apply a short cooldown and skip — do NOT count toward the circuit breaker.
                                        if err_str.contains("crosses book") || err_str.contains("post-only") {
                                            warn!("⚠️ Maker post-only rejected (spread too tight): {} — cooling down {}s", err_str, config::CROSSES_BOOK_COOLDOWN_SECS);
                                            positions.lock().await.remove(&pos_key);
                                            last_trade_time.insert(
                                                strategy_name.clone(),
                                                Instant::now() - Duration::from_secs(config::TRADE_COOLDOWN_SECS as u64)
                                                    + Duration::from_secs(config::CROSSES_BOOK_COOLDOWN_SECS as u64),
                                            );
                                            continue;
                                        }
                                        warn!("⚠️ Entry order failed: {}", e);
                                        // Roll back sentinel
                                        positions.lock().await.remove(&pos_key);
                                        last_trade_time.insert(strategy_name.clone(), Instant::now());
                                        tick_failures += 1;
                                        consecutive_failures += 1;
                                        if consecutive_failures >= config::MAX_CONSECUTIVE_FAILURES {
                                            error!("🚨 Circuit breaker: {} consecutive failures (ENTRY) — pausing", consecutive_failures);
                                            let _ = send_notification(&tg_token, &tg_chat_id,
                                                &format!("🚨 Circuit breaker hit after {} ENTRY failures on {}", consecutive_failures, market_name)).await;
                                            tokio::select! {
                                                _ = tokio::time::sleep(Duration::from_secs(config::FAILURE_COOLDOWN_SECS as u64)) => {
                                                    sync_nonce_manager(&nonce_manager, &shared_http, safe_address).await;
                                                }
                                                _ = market_rx.changed() => {
                                                    info!("🔄 Market switch detected during circuit breaker cooldown");
                                                }
                                            }
                                            consecutive_failures = 0;
                                            tick_failures = 0;
                                        }
                                        continue;
                                    }
                                }

                                // Position already recorded in the map above (sentinel is now real)

                                // Spawn async balance sync
                                {
                                    let client_sync = Arc::clone(&trading_client);
                                    let positions_sync = Arc::clone(&positions);
                                    let phantom_cooldowns_sync = Arc::clone(&phantom_cooldowns);
                                    let strategy_sync = strategy_name.clone();
                                    let token_sync = *token_id;
                                    tokio::spawn(async move {
                                        let _ = sync_position_balance(&client_sync, &positions_sync, &strategy_sync, token_sync, Some(&phantom_cooldowns_sync), baseline_shares).await;
                                    });
                                }

                                // For paired strategies, also buy the other leg
                                let is_paired = strategy_name == "ArbitrageStrategy" || strategy_name == "TimeDecayStrategy";
                                if is_paired {
                                    let pair_token = if *token_id == yes_token { no_token } else { yes_token };
                                    let pair_ask = if pair_token == yes_token { yes_ask } else { no_ask };
                                    let pair_fee = if pair_token == yes_token { yes_fee_rate as u16 } else { no_fee_rate as u16 };
                                    let pair_key = (strategy_name.clone(), pair_token);
                                    if pair_ask > dec!(0) {
                                        let pair_shares = trade_size_usdc / pair_ask;
                                        let pair_buy = (pair_ask + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE);
                                        let pair_label = if pair_token == yes_token { "YES" } else { "NO" };

                                        info!("📥 ENTRY (paired) [{}]: {} {} | shares={:.2}, price=${:.4}", strategy_name, pair_label, market_name, pair_shares, pair_buy);

                                        if !config::GHOST_MODE {
                                            if let Err(e) = place_limit_order(
                                                &trading_client, &nonce_manager, &signer, safe_address, eoa_address,
                                                verifying_contract, pair_token, Side::Buy, pair_shares, pair_buy, pair_fee, OrderType::FAK, false, 0,
                                                &shared_http,
                                            ).await {
                                                warn!("⚠️ Paired entry order failed: {} — first leg is now one-sided!", e);
                                                let _ = send_notification(&tg_token, &tg_chat_id,
                                                    &format!("⚠️ Paired entry PARTIAL: {} first leg filled but second leg failed on {}", strategy_name, market_name)).await;
                                            }
                                        }

                                        let mut pos_map = positions.lock().await;
                                        pos_map.insert(pair_key.clone(), Position {
                                            shares: pair_shares,
                                            avg_entry: pair_ask,
                                            opened_at: Utc::now(),
                                            close_time: market_close_time,
                                            market_name: market_name.clone(),
                                            pair_token_id: pair_token,
                                            fill_confirmed_at: None,
                                            paired_leg_token_id: Some(*token_id),
                                        });

                                        let client_sync = Arc::clone(&trading_client);
                                        let positions_sync = Arc::clone(&positions);
                                        let phantom_cooldowns_sync = Arc::clone(&phantom_cooldowns);
                                        let strategy_sync = strategy_name.clone();
                                        tokio::spawn(async move {
                                            let _ = sync_position_balance(&client_sync, &positions_sync, &strategy_sync, pair_token, Some(&phantom_cooldowns_sync), dec!(0)).await;
                                        });
                                    }
                                }

                                consecutive_failures = 0;
                                momentum_confirmation_count = 0;
                                last_momentum_signal_token = None;
                                last_trade_time.insert(strategy_name.clone(), Instant::now());
                                // Clear phantom cooldown on successful entry
                                phantom_cooldowns.lock().await.remove(strategy_name.as_str());

                                let msg = format!("📥 ENTRY [{}] {} {} | ${:.4} x {:.1}", strategy_name, side_label, market_name, order_base_price, shares);
                                let _ = send_notification(&tg_token, &tg_chat_id, &msg).await;
                            }

                            StrategySignal::NoSignal => {}

                            // ════════════════════ MAKER QUOTE (two-sided) ════════════════════
                            StrategySignal::MakerQuote { yes_bid_price, no_bid_price } => {
                                // Resolve which venue (window/daily vs hourly) Maker is quoting on
                                let mk_yes_token = maker_market_config.as_ref().map(|m| m.yes_token).unwrap_or(yes_token);
                                let mk_no_token  = maker_market_config.as_ref().map(|m| m.no_token).unwrap_or(no_token);
                                let mk_yes_fee   = maker_market_config.as_ref().map(|m| m.yes_fee_bps as u16).unwrap_or(yes_fee_rate as u16);
                                let mk_no_fee    = maker_market_config.as_ref().map(|m| m.no_fee_bps as u16).unwrap_or(no_fee_rate as u16);
                                let mk_neg_risk  = maker_market_config.as_ref().map(|m| m.is_neg_risk).unwrap_or(is_neg_risk);
                                let mk_verifying = if mk_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                let mk_market_name = maker_market_config.as_ref().map(|m| m.market_name.clone()).unwrap_or(market_name.clone());

                                // Per-strategy cooldown gate
                                if let Some(lt) = last_trade_time.get(strategy_name.as_str()) {
                                    let elapsed = lt.elapsed();
                                    let cooldown = Duration::from_secs(config::TRADE_COOLDOWN_SECS as u64);
                                    if elapsed < cooldown {
                                        debug!("⏸️ MakerQuote [{}]: cooldown ({:.1}s remaining)",
                                            strategy_name, (cooldown - elapsed).as_secs_f32());
                                        continue;
                                    }
                                }
                                // Phantom cooldown gate
                                {
                                    let pc = phantom_cooldowns.lock().await;
                                    if let Some(removed_at) = pc.get(strategy_name.as_str()) {
                                        let elapsed = removed_at.elapsed();
                                        let cooldown = Duration::from_secs(rustpolybot::helpers::balance::PHANTOM_COOLDOWN_SECS);
                                        if elapsed < cooldown {
                                            debug!("⏸️ MakerQuote [{}]: phantom cooldown ({:.0}s remaining)",
                                                strategy_name, (cooldown - elapsed).as_secs_f32());
                                            continue;
                                        }
                                    }
                                }
                                // Stop-loss cooldown gate
                                if let Some(sl_time) = last_stop_loss_time.get(strategy_name.as_str()) {
                                    let elapsed = sl_time.elapsed();
                                    let cooldown = Duration::from_secs(config::MAKER_STOP_LOSS_COOLDOWN_SECS as u64);
                                    if elapsed < cooldown {
                                        debug!("⏸️ MakerQuote [{}]: stop-loss cooldown ({:.0}s remaining)",
                                            strategy_name, (cooldown - elapsed).as_secs_f32());
                                        continue;
                                    }
                                }

                                // Compute current inventory values for net exposure check
                                let (yes_inv_value, no_inv_value) = {
                                    let pos_map = positions.lock().await;
                                    let yv = pos_map.get(&(strategy_name.clone(), mk_yes_token))
                                        .map(|p| p.shares * p.avg_entry).unwrap_or(dec!(0));
                                    let nv = pos_map.get(&(strategy_name.clone(), mk_no_token))
                                        .map(|p| p.shares * p.avg_entry).unwrap_or(dec!(0));
                                    (yv, nv)
                                };

                                // USDC value of the proposed new orders
                                let yes_new_value = yes_bid_price.map(|_| trade_size_usdc).unwrap_or(dec!(0));
                                let no_new_value  = no_bid_price.map(|_| trade_size_usdc).unwrap_or(dec!(0));

                                let collateral = *starting_collateral.lock().await;
                                let session_pnl = *total_pnl.lock().await;

                                if !risk_engine.approve_maker_net_exposure(
                                    yes_inv_value, no_inv_value,
                                    yes_new_value, no_new_value,
                                    session_pnl, collateral,
                                ) {
                                    if should_log_maker_exposure_reject() {
                                        info!("🚫 MakerQuote [{}]: net exposure risk check failed (YES=${:.2} NO=${:.2})",
                                            strategy_name, yes_inv_value + yes_new_value, no_inv_value + no_new_value);
                                    }
                                    continue;
                                }

                                let mut any_placed = false;

                                // ── YES side ───────────────────────────────────────────────────────
                                if let Some(&yes_price) = yes_bid_price.as_ref() {
                                    let pos_key = (strategy_name.clone(), mk_yes_token);
                                    let already_open = positions.lock().await.contains_key(&pos_key);
                                    if !already_open && yes_price > dec!(0) {
                                        let shares = trade_size_usdc / yes_price;
                                        if shares >= config::MIN_ORDER_SHARES && trade_size_usdc >= config::MIN_ORDER_USDC {
                                            let rounded_price = floor_to_tick_size(yes_price);
                                            let baseline_shares = {
                                                let mut req = BalanceAllowanceRequest::default();
                                                req.asset_type = AssetType::Conditional;
                                                req.token_id = Some(mk_yes_token);
                                                match trading_client.balance_allowance(req).await {
                                                    Ok(resp) => Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000),
                                                    Err(_) => dec!(0),
                                                }
                                            };
                                            info!("📥 MakerQuote YES [{}]: {} | shares={:.2}, bid=${:.4}", strategy_name, mk_market_name, shares, rounded_price);
                                            positions.lock().await.insert(pos_key.clone(), Position {
                                                shares, avg_entry: rounded_price, opened_at: Utc::now(),
                                                close_time: maker_market_config.as_ref().and_then(|m| m.market_close_time).or(market_close_time),
                                                market_name: mk_market_name.clone(),
                                                pair_token_id: mk_yes_token, fill_confirmed_at: None,
                                                paired_leg_token_id: Some(mk_no_token),
                                            });
                                            if !config::GHOST_MODE {
                                                match place_limit_order(
                                                    &trading_client, &nonce_manager, &signer, safe_address, eoa_address,
                                    mk_verifying, mk_yes_token, Side::Buy, shares, rounded_price,
                                                     mk_yes_fee, OrderType::GTD, true, config::MAKER_GTD_TTL_SECS, &shared_http,
                                                ).await {
                                                    Ok(_) => {
                                                        any_placed = true;
                                                        let cs = Arc::clone(&trading_client); let ps = Arc::clone(&positions);
                                                        let pcs = Arc::clone(&phantom_cooldowns); let ss = strategy_name.clone();
                                                        tokio::spawn(async move { let _ = sync_position_balance(&cs, &ps, &ss, mk_yes_token, Some(&pcs), baseline_shares).await; });
                                                    }
                                                    Err(e) => {
                                                        positions.lock().await.remove(&pos_key);
                                                        let err_str = e.to_string();
                                                        if err_str.contains("crosses book") || err_str.contains("post-only") {
                                                            warn!("⚠️ MakerQuote YES crosses book — cooling down {}s", config::CROSSES_BOOK_COOLDOWN_SECS);
                                                            last_trade_time.insert(strategy_name.clone(),
                                                                Instant::now() - Duration::from_secs(config::TRADE_COOLDOWN_SECS as u64)
                                                                    + Duration::from_secs(config::CROSSES_BOOK_COOLDOWN_SECS as u64));
                                                        } else {
                                                            warn!("⚠️ MakerQuote YES order failed: {}", e);
                                                            consecutive_failures += 1;
                                                        }
                                                    }
                                                }
                                            } else { any_placed = true; }
                                        }
                                    }
                                }

                                // ── NO side ────────────────────────────────────────────────────────
                                if let Some(&no_price) = no_bid_price.as_ref() {
                                    let pos_key = (strategy_name.clone(), mk_no_token);
                                    let already_open = positions.lock().await.contains_key(&pos_key);
                                    if !already_open && no_price > dec!(0) {
                                        let shares = trade_size_usdc / no_price;
                                        if shares >= config::MIN_ORDER_SHARES && trade_size_usdc >= config::MIN_ORDER_USDC {
                                            let rounded_price = floor_to_tick_size(no_price);
                                            let baseline_shares = {
                                                let mut req = BalanceAllowanceRequest::default();
                                                req.asset_type = AssetType::Conditional;
                                                req.token_id = Some(mk_no_token);
                                                match trading_client.balance_allowance(req).await {
                                                    Ok(resp) => Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000),
                                                    Err(_) => dec!(0),
                                                }
                                            };
                                            info!("📥 MakerQuote NO [{}]: {} | shares={:.2}, bid=${:.4}", strategy_name, mk_market_name, shares, rounded_price);
                                            positions.lock().await.insert(pos_key.clone(), Position {
                                                shares, avg_entry: rounded_price, opened_at: Utc::now(),
                                                close_time: maker_market_config.as_ref().and_then(|m| m.market_close_time).or(market_close_time),
                                                market_name: mk_market_name.clone(),
                                                pair_token_id: mk_no_token, fill_confirmed_at: None,
                                                paired_leg_token_id: Some(mk_yes_token),
                                            });
                                            if !config::GHOST_MODE {
                                                match place_limit_order(
                                                    &trading_client, &nonce_manager, &signer, safe_address, eoa_address,
                                    mk_verifying, mk_no_token, Side::Buy, shares, rounded_price,
                                                     mk_no_fee, OrderType::GTD, true, config::MAKER_GTD_TTL_SECS, &shared_http,
                                                ).await {
                                                    Ok(_) => {
                                                        any_placed = true;
                                                        let cs = Arc::clone(&trading_client); let ps = Arc::clone(&positions);
                                                        let pcs = Arc::clone(&phantom_cooldowns); let ss = strategy_name.clone();
                                                        tokio::spawn(async move { let _ = sync_position_balance(&cs, &ps, &ss, mk_no_token, Some(&pcs), baseline_shares).await; });
                                                    }
                                                    Err(e) => {
                                                        positions.lock().await.remove(&pos_key);
                                                        let err_str = e.to_string();
                                                        if err_str.contains("crosses book") || err_str.contains("post-only") {
                                                            warn!("⚠️ MakerQuote NO crosses book — cooling down {}s", config::CROSSES_BOOK_COOLDOWN_SECS);
                                                            if !any_placed {
                                                                last_trade_time.insert(strategy_name.clone(),
                                                                    Instant::now() - Duration::from_secs(config::TRADE_COOLDOWN_SECS as u64)
                                                                        + Duration::from_secs(config::CROSSES_BOOK_COOLDOWN_SECS as u64));
                                                            }
                                                        } else {
                                                            warn!("⚠️ MakerQuote NO order failed: {}", e);
                                                            consecutive_failures += 1;
                                                        }
                                                    }
                                                }
                                            } else { any_placed = true; }
                                        }
                                    }
                                }

                                if any_placed {
                                    consecutive_failures = 0;
                                    last_trade_time.insert(strategy_name.clone(), Instant::now());
                                    phantom_cooldowns.lock().await.remove(strategy_name.as_str());
                                    let yes_str = yes_bid_price.map(|p| format!("YES@${:.4}", p)).unwrap_or_default();
                                    let no_str  = no_bid_price.map(|p| format!("NO@${:.4}", p)).unwrap_or_default();
                                    let msg = format!("📥 MakerQuote [{}] {} {} | {}", strategy_name, yes_str, no_str, mk_market_name);
                                    let _ = send_notification(&tg_token, &tg_chat_id, &msg).await;
                                }

                                // Circuit breaker check
                                if consecutive_failures >= config::MAX_CONSECUTIVE_FAILURES {
                                    error!("🚨 Circuit breaker: {} consecutive MakerQuote failures — pausing", consecutive_failures);
                                    let _ = send_notification(&tg_token, &tg_chat_id,
                                        &format!("🚨 Circuit breaker: {} MakerQuote failures on {}", consecutive_failures, mk_market_name)).await;
                                    tokio::select! {
                                        _ = tokio::time::sleep(Duration::from_secs(config::FAILURE_COOLDOWN_SECS as u64)) => {
                                            sync_nonce_manager(&nonce_manager, &shared_http, safe_address).await;
                                        }
                                        _ = market_rx.changed() => {}
                                    }
                                    consecutive_failures = 0;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
