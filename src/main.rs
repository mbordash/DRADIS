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
use rustpolybot::state::{Position, StrategySignal, MarketConfig, MarketSnapshot, PositionMap, OrderParams};
use rustpolybot::strategies::time_decay_impl::TimeDecayPosition;
use rustpolybot::orchestrator::{StrategyRegistry, StrategyContext};
use rustpolybot::orchestrator::executor::{execute_strategies_concurrent, aggregate_and_resolve_signals};
use rustpolybot::helpers::{time::*, balance::*, nonce::*, orders::*, market::*, price::{round_to_tick_size, floor_to_tick_size}, notifications::send_notification, market};

use rustls::crypto::ring;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

type PriceState = (Decimal, Decimal, Decimal, Decimal); // (Bid, BidDepth, Ask, AskDepth)

const EXCHANGE_NORMAL: Address = address!("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E");
const EXCHANGE_NEG_RISK: Address = address!("0xC5d563A36AE78145C45a50134d48A1215220f80a");


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

    let starting_collateral_store = Arc::new(Mutex::new(dec!(0.0)));
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
    *starting_collateral_store.lock().await = startup_balance;
    let _ = balance_tx.send(startup_balance);
    info!("📈 Starting portfolio value: ${:.2}", startup_balance);

    let (oracle_tx, oracle_rx) = watch::channel(dec!(0));
    let (velocity_tx, velocity_rx) = watch::channel((dec!(0), dec!(0), dec!(0)));
    let (funding_tx, funding_rx) = watch::channel(dec!(0));
    let (drift_60m_tx, drift_60m_rx) = watch::channel(dec!(0));

    tokio::spawn(rustpolybot::tasks::oracle::run_oracle(
        crypto_filter.clone(),
        oracle_tx,
        velocity_tx,
        drift_60m_tx,
    ));

    tokio::spawn(rustpolybot::tasks::funding::run_funding_poller(
        Arc::clone(&shared_http),
        crypto_filter.clone(),
        funding_tx,
    ));

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
    let initial_condition_id = initial_hourly.condition_id.clone();

    info!("🧪 Initializing market: {}", name);
    let mut initial_strike = market::extract_strike_price(&name);
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

    let (market_tx, mut market_rx) = watch::channel((initial_yes, initial_no, name, close_time, initial_strike, desc, initial_maker_market, initial_condition_id));
    let mut current_hourly_cid: String = String::new();
    let mut current_maker_cid: String = String::new();

    tokio::spawn(rustpolybot::tasks::market_monitor::run_market_monitor(
        Arc::clone(&shared_http),
        crypto_filter.clone(),
        market_tx.clone(),
    ));

    loop {
        let (yes_token, no_token, market_name, market_close_time, strike_price, _, maker_market_candidate, condition_id) = market_rx.borrow().clone();

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
                condition_id: mk.condition_id.clone(),
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

        let strategies = StrategyRegistry::create_all_strategies();
        let mut last_trade_time: HashMap<String, Instant> = HashMap::new();
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
                    let (.., new_maker_opt, new_condition_id) = market_rx.borrow().clone();
                    if new_condition_id == current_hourly_cid &&
                       new_maker_opt.as_ref().map_or("", |m| m.condition_id.as_str()) == current_maker_cid {
                        continue;
                    }
                    info!("🔄 Market switch required — restarting trading loop with new market");
                    if let Err(e) = trading_client.as_ref().cancel_all_orders().await { warn!("⚠️ Failed to cancel all orders: {}", e); }
                    { phantom_cooldowns.lock().await.clear(); }
                    current_hourly_cid = new_condition_id.clone();
                    current_maker_cid = new_maker_opt.as_ref().map_or_else(String::new, |m| m.condition_id.clone());
                    break;
                }
                _ = pulse_ticker.tick() => {
                    let start = Instant::now();
                    let mut req = BalanceAllowanceRequest::default();
                    req.asset_type = AssetType::Collateral;
                    let _ = trading_client.balance_allowance(req).await;
                    info!("📍 Network Pulse: {:?}", start.elapsed());
                }
                _ = cleanup_ticker.tick() => {
                    rustpolybot::tasks::cleanup::cleanup_expired_positions(Arc::clone(&positions), market_name.clone(), yes_token, no_token, market_close_time).await;
                    if let Err(e) = rustpolybot::tasks::cleanup::reconcile_orphaned_positions(Arc::clone(&positions), &tg_token, &tg_chat_id).await { warn!("⚠️ Orphan reconciliation error: {}", e); }
                    rustpolybot::tasks::cleanup::cleanup_time_decay_positions(Arc::clone(&time_decay_positions)).await;
                }
                _ = status_ticker.tick() => {
                    let (yb, _, ya, _) = *yes_price_rx.borrow();
                    let (nb, _, na, _) = *no_price_rx.borrow();
                    if ya != dec!(1) && na != dec!(1) {
                        info!("💓 Heartbeat | Ask Sum ${:.4} (Y ask ${:.2} / N ask ${:.2}) | Bid Sum ${:.4} (Y bid ${:.2} / N bid ${:.2}) | Binance: ${:.2}",
                            ya + na, ya, na, yb + nb, yb, nb, *oracle_rx.borrow());
                    }
                }
                _ = ticker.tick() => {
                    if market_rx.has_changed().unwrap_or(false) { break; }
                    let (yb, ybd, ya, yad) = *yes_price_rx.borrow();
                    let (nb, nbd, na, nad) = *no_price_rx.borrow();
                    if ya == dec!(1) && na == dec!(1) { continue; }

                    let snapshot = MarketSnapshot {
                        yes_bid: yb, yes_bid_depth: ybd, yes_ask: ya, yes_ask_depth: yad,
                        no_bid: nb, no_bid_depth: nbd, no_ask: na, no_ask_depth: nad,
                        oracle_price: *oracle_rx.borrow(),
                        velocity: velocity_rx.borrow().0,
                        velocity_1s: velocity_rx.borrow().1,
                        acceleration: velocity_rx.borrow().2,
                        funding_rate: *funding_rx.borrow(),
                        oracle_drift_60m: *drift_60m_rx.borrow(),
                        timestamp: Utc::now(),
                    };
                    let ctx = StrategyContext {
                        market: MarketConfig {
                            yes_token, no_token, market_name: market_name.clone(), market_close_time, strike_price, is_neg_risk, condition_id: condition_id.clone(), yes_fee_bps: yes_fee_rate, no_fee_bps: no_fee_rate,
                        },
                        snapshot: snapshot.clone(),
                        positions: Arc::clone(&positions),
                        session_pnl: *total_pnl.lock().await,
                        starting_collateral: *starting_collateral_store.lock().await,
                        crypto_filter: crypto_filter.clone(),
                        market_started_at,
                        maker_market: maker_market_config.clone(),
                        maker_snapshot: match (&maker_yes_price_rx, &maker_no_price_rx) {
                            (Some(my), Some(mn)) => Some(MarketSnapshot {
                                yes_bid: my.borrow().0, yes_bid_depth: my.borrow().1, yes_ask: my.borrow().2, yes_ask_depth: my.borrow().3,
                                no_bid: mn.borrow().0, no_bid_depth: mn.borrow().1, no_ask: mn.borrow().2, no_ask_depth: mn.borrow().3,
                                oracle_price: *oracle_rx.borrow(), velocity: velocity_rx.borrow().0, velocity_1s: velocity_rx.borrow().1, acceleration: velocity_rx.borrow().2,
                                funding_rate: *funding_rx.borrow(), oracle_drift_60m: *drift_60m_rx.borrow(), timestamp: Utc::now(),
                            }),
                            _ => None,
                        },
                    };

                    let eval_result = match execute_strategies_concurrent(&strategies, &ctx, 500).await {
                        Ok(r) => r,
                        Err(e) => { warn!("⚠️ Strategy evaluation error: {}", e); continue; }
                    };
                    let (resolved_signals, _) = aggregate_and_resolve_signals(&eval_result);
                    if resolved_signals.is_empty() { momentum_confirmation_count = 0; last_momentum_signal_token = None; continue; }

                    for (strategy_name, signal) in resolved_signals {
                        let sn = strategy_name.clone();
                        match signal {
                            StrategySignal::Exit { params, reason, exit_pair } => {
                                let tid = params.token_id;
                                let pos_key = (sn.clone(), tid);
                                let shares = { let map = positions.lock().await; match map.get(&pos_key) { Some(p) => p.shares, None => continue } };
                                if shares < config::MIN_ORDER_SHARES || params.price <= dec!(0) {
                                    let mut map = positions.lock().await; if let Some(p) = map.remove(&pos_key) { *total_pnl.lock().await += (params.price - p.avg_entry) * p.shares; } continue;
                                }
                                info!("📤 EXIT [{}]: {} | shares={:.2}, bid=${:.4} | {}", sn, params.market_name, shares, params.price, reason);
                                let vc = if params.is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                if !config::GHOST_MODE {
                                    if let Err(e) = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc, tid, Side::Sell, shares, (params.price - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE), params.fee_bps, OrderType::FAK, false, 0, &shared_http).await {
                                        let es = e.to_string();
                                        if es.contains("not enough balance") || es.contains("balance: 0") || es.contains("invalid price") {
                                            let mut map = positions.lock().await; if let Some(p) = map.remove(&pos_key) { if p.fill_confirmed_at.is_some() { *total_pnl.lock().await += (params.price - p.avg_entry) * p.shares; } }
                                            last_trade_time.insert(sn.clone(), Instant::now()); continue;
                                        }
                                        if !es.contains("no orders found") { consecutive_failures += 1; } continue;
                                    }
                                }
                                let (re, rs, rc) = { let mut map = positions.lock().await; if let Some(p) = map.remove(&pos_key) { let pnl = (params.price - p.avg_entry) * p.shares; *total_pnl.lock().await += pnl; info!("💰 Position closed [{}]: PnL ${:.4}", sn, pnl); (p.avg_entry, p.shares, p.close_time) } else { (dec!(0), dec!(0), None) } };
                                if rs > dec!(0) {
                                    let ps = Arc::clone(&positions); let cl = Arc::clone(&trading_client); let tp = Arc::clone(&total_pnl); let m_name = params.market_name.clone(); let tkt = tg_token.clone(); let tkc = tg_chat_id.clone();
                                    let sn_async = sn.clone();
                                    tokio::spawn(async move {
                                        tokio::time::sleep(Duration::from_millis(2500)).await;
                                        let mut req = BalanceAllowanceRequest::default(); req.asset_type = AssetType::Conditional; req.token_id = Some(tid);
                                        let rem = match cl.balance_allowance(req).await { Ok(r) => Decimal::from_str(&r.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000), Err(_) => return };
                                        if rem >= config::MIN_ORDER_SHARES {
                                            let fill = (rs - rem).max(dec!(0)); let pnlc = -((params.price - re) * rem.min(rs)); *tp.lock().await += pnlc;
                                            if fill < config::MIN_ORDER_SHARES { warn!("⚠️ PARTIAL EXIT [{}]: FAK filled 0/{:.4} shares — retry on next loop.", sn_async, rs); }
                                            else { warn!("⚠️ PARTIAL EXIT [{}]: sold {:.4}/{:.4} — re-inserting.", sn_async, fill, rs); let mut map = ps.lock().await; if !map.contains_key(&(sn_async.clone(), tid)) { map.insert((sn_async.clone(), tid), Position { shares: rem, avg_entry: re, opened_at: Utc::now(), close_time: rc, market_name: m_name, pair_token_id: tid, fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: None }); } }
                                        }
                                    });
                                }
                                if exit_pair {
                                    let other_tid = if tid == yes_token { no_token } else { yes_token };
                                    let pk = (sn.clone(), other_tid); let ps = { let map = positions.lock().await; map.get(&pk).map(|p| p.shares) };
                                    if let Some(s) = ps {
                                        let other_bid = if other_tid == yes_token { yb } else { nb };
                                        let other_fee_bps = if other_tid == yes_token { yes_fee_rate as u16 } else { no_fee_rate as u16 };
                                        let other_vc = if is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                        if !config::GHOST_MODE { let _ = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, other_vc, other_tid, Side::Sell, s, (other_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE), other_fee_bps, OrderType::FAK, false, 0, &shared_http).await; }
                                        let mut map = positions.lock().await; if let Some(p) = map.remove(&pk) { *total_pnl.lock().await += (other_bid - p.avg_entry) * p.shares; }
                                    }
                                }
                                if reason.contains("stop-loss") { last_stop_loss_time.insert(sn.clone(), Instant::now()); }
                                last_trade_time.insert(sn.clone(), Instant::now());
                                let _ = send_notification(&tg_token, &tg_chat_id, &format!("📤 EXIT [{}] {} | bid=${:.4} | reason: {} | Session PnL: ${:.4}", sn, params.market_name, params.price, reason, *total_pnl.lock().await)).await;
                            }

                            StrategySignal::Entry { params, pair_params } => {
                                if let Some(close_time) = market_close_time { if (close_time - Utc::now()).num_seconds() < config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY { continue; } }
                                if let Some(lt) = last_trade_time.get(&sn) { if lt.elapsed() < Duration::from_secs(config::TRADE_COOLDOWN_SECS as u64) { continue; } }

                                let pos_key = (sn.clone(), params.token_id);
                                {
                                    let mut map = positions.lock().await; if map.contains_key(&pos_key) { continue; }
                                    map.insert(pos_key.clone(), Position { shares: params.shares, avg_entry: params.price, opened_at: Utc::now(), close_time: market_close_time, market_name: params.market_name.clone(), pair_token_id: params.token_id, fill_confirmed_at: None, paired_leg_token_id: pair_params.as_ref().map(|p| p.token_id) });
                                }
                                let vc = if params.is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                if !config::GHOST_MODE {
                                    if let Err(e) = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc, params.token_id, Side::Buy, params.shares, (params.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE), params.fee_bps, OrderType::FAK, false, 0, &shared_http).await {
                                        positions.lock().await.remove(&pos_key); consecutive_failures += 1; continue;
                                    }
                                }
                                let cl_s = Arc::clone(&trading_client); let ps_s = Arc::clone(&positions); let pc_s = Arc::clone(&phantom_cooldowns); let sn_s = sn.clone(); let tn_s = params.token_id;
                                tokio::spawn(async move { let _ = sync_position_balance(&cl_s, &ps_s, &sn_s, tn_s, Some(&pc_s), dec!(0), rustpolybot::helpers::balance::MAX_WAIT_SECS_HOURLY).await; });

                                if let Some(pp) = pair_params {
                                    let vc_p = if pp.is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                    if !config::GHOST_MODE { let _ = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc_p, pp.token_id, Side::Buy, pp.shares, (pp.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE), pp.fee_bps, OrderType::FAK, false, 0, &shared_http).await; }
                                    positions.lock().await.insert((sn.clone(), pp.token_id), Position { shares: pp.shares, avg_entry: pp.price, opened_at: Utc::now(), close_time: market_close_time, market_name: pp.market_name.clone(), pair_token_id: pp.token_id, fill_confirmed_at: None, paired_leg_token_id: Some(params.token_id) });
                                    let sn_p = sn.clone(); let tn_p = pp.token_id; let ps_p = Arc::clone(&positions); let cl_p = Arc::clone(&trading_client); let pc_p = Arc::clone(&phantom_cooldowns);
                                    tokio::spawn(async move { let _ = sync_position_balance(&cl_p, &ps_p, &sn_p, tn_p, Some(&pc_p), dec!(0), rustpolybot::helpers::balance::MAX_WAIT_SECS_HOURLY).await; });
                                }
                                last_trade_time.insert(sn.clone(), Instant::now());
                                let _ = send_notification(&tg_token, &tg_chat_id, &format!("📥 ENTRY [{}] {} | ${:.4} x {:.1}", sn, params.market_name, params.price, params.shares)).await;
                            }

                            StrategySignal::MakerQuote { yes, no } => {
                                let mut placed = false;
                                for p in [yes, no].into_iter().flatten() {
                                    let pk = (sn.clone(), p.token_id);
                                    if !positions.lock().await.contains_key(&pk) {
                                        info!("📥 MakerQuote [{}]: {} | shares={:.2}, bid=${:.4}", sn, p.market_name, p.shares, p.price);
                                        positions.lock().await.insert(pk.clone(), Position { shares: p.shares, avg_entry: p.price, opened_at: Utc::now(), close_time: None, market_name: p.market_name.clone(), pair_token_id: p.token_id, fill_confirmed_at: None, paired_leg_token_id: None });
                                        let _ = rustpolybot::helpers::balance::quick_confirm_fill(&trading_client, &sn, p.token_id, &positions, &p.condition_id).await;
                                        let vc = if p.is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                        if !config::GHOST_MODE {
                                            if let Err(e) = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc, p.token_id, Side::Buy, p.shares, p.price, p.fee_bps, OrderType::GTC, true, 0, &shared_http).await {
                                                positions.lock().await.remove(&pk); if !e.to_string().contains("crosses book") { consecutive_failures += 1; } continue;
                                            }
                                            let cl_m = Arc::clone(&trading_client); let ps_m = Arc::clone(&positions); let pc_m = Arc::clone(&phantom_cooldowns);
                                            let sn_m = sn.clone();
                                            tokio::spawn(async move { let _ = sync_position_balance(&cl_m, &ps_m, &sn_m, p.token_id, Some(&pc_m), dec!(0), rustpolybot::helpers::balance::MAX_WAIT_SECS_WINDOW).await; });
                                        }
                                        placed = true;
                                    }
                                }
                                if placed { last_trade_time.insert(sn.clone(), Instant::now()); }
                                if consecutive_failures >= config::MAX_CONSECUTIVE_FAILURES { error!("🚨 Circuit breaker hit!"); tokio::time::sleep(Duration::from_secs(60)).await; consecutive_failures = 0; }
                            }
                            StrategySignal::NoSignal => {}
                        }
                    }
                }
            }
        }
    }
}
