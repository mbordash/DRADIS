/// dradis - Multi-Strategy Orchestrator Trading Bot
///
/// Phase 8: Full Orchestrator-Based Trading
/// Strategies evaluate signals → orchestrator resolves conflicts → executor places orders.

use anyhow::Result;

use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config};
use polymarket_client_sdk_v2::clob::types::{Side, SignatureType, OrderType};
use polymarket_client_sdk_v2::{POLYGON, PRIVATE_KEY_VAR, derive_safe_wallet};
use polymarket_client_sdk_v2::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk_v2::clob::types::AssetType;

use futures::StreamExt as _;
use polymarket_client_sdk_v2::clob::ws::Client as WsClient;

use alloy::primitives::{U256, Address, address};
use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;

use chrono::Utc;
use chrono_tz::US::Eastern;
use reqwest;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use std::collections::HashMap;
use std::env;
use std::str::FromStr as _;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::{watch, Mutex};
use tokio::time::{interval, Instant, Duration};

use tracing::{info, warn, error, debug};

use dradis::config;
use dradis::state::{Position, StrategySignal, MarketConfig, MarketSnapshot, PositionMap};
use dradis::strategies::time_decay_impl::TimeDecayPosition;
use dradis::orchestrator::{StrategyRegistry, StrategyContext};
use dradis::orchestrator::executor::{execute_strategies_concurrent, aggregate_and_resolve_signals};

// New paths for helpers
use dradis::helpers::{
    time::*, balance::*, nonce::*, orders::*, market::*,
    notifications::send_notification, metrics,
};

use rustls::crypto::ring;


type PriceState = (Decimal, Decimal, Decimal, Decimal); // (Bid, BidDepth, Ask, AskDepth)

/// Custom tracing timer that formats log timestamps in US/Eastern (ET/EDT).
/// Ensures all log output is in the same timezone as Polymarket's market names,
/// making it straightforward to correlate log lines with market events.
struct EasternTime;

impl tracing_subscriber::fmt::time::FormatTime for EasternTime {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        let now = Utc::now().with_timezone(&Eastern);
        write!(w, "{}", now.format("%Y-%m-%d %H:%M:%S %Z"))
    }
}

fn print_banner() {
    println!(r#"
  ██████╗ ██████╗  █████╗ ██████╗ ██╗███████╗
  ██╔══██╗██╔══██╗██╔══██╗██╔══██╗██║██╔════╝
  ██║  ██║██████╔╝███████║██║  ██║██║███████╗
  ██║  ██║██╔══██╗██╔══██║██║  ██║██║╚════██║
  ██████╔╝██║  ██║██║  ██║██████╔╝██║███████║
  ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝ ╚═╝╚══════╝
  Direct Reaction And Dynamic Intelligence System  v{}
  ─────────────────────────────────────────────────────

          ·  ·  ·  ·  ·  ·  ·  ·  ·
       ·     ·        |        ·     ·
     ·    ·     ·     |     ·     ·    ·
    ·   ·    ·    · ──●── ·    ·    ·   ·
     ·    ·     ·     |     ·     ·    ·
       ·     ·        |        ·     ·
          ·  ·  ·  ·  ·  ·  ·  ·  ·
              C O M B A T   I N F O R M A T I O N   C E N T E R

  ┌─────────────────────┐   ┌─────────────────────┐
  │   Binance Oracle    │   │  Polymarket CLOB    │
  │  (Price / Funding)  │   │  (WebSocket Feed)   │
  └──────────┬──────────┘   └──────────┬──────────┘
             └─────────────┬───────────┘
                           ▼
              ┌────────────────────────┐
              │   Orchestrator (CIC)   │
              │     50ms Heartbeat     │
              └─────────────┬──────────┘
             parallel dispatch to Viper squadrons
                           ▼
              ┌───────────────────────┐
              │    Execution Layer    │
              │  OBI · Fee · Breaker  │
              └───────────────────────┘

  ╔═══════════════════════════════════════════════════╗
  ║  "It's not enough to survive.                     ║
  ║   One has to be worthy of survival."              ║
  ║                         — Admiral William Adama   ║
  ╚═══════════════════════════════════════════════════╝
                    So say we all.
  "#, env!("CARGO_PKG_VERSION"));
}

// V2 CTF Exchange contracts (pUSD collateral, EIP-712 domain version "2")
const EXCHANGE_NORMAL: Address = address!("0xE111180000d2663C0091e4f400237545B87B996B");
const EXCHANGE_NEG_RISK: Address = address!("0xe2222d279d744050d28e00520010520000310F59");


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
    tracing_subscriber::fmt()
        .with_timer(EasternTime)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    ring::default_provider().install_default().expect("rustls provider");
    print_banner();

    let crypto_filter = env::var("CRYPTO_FILTER").unwrap_or_else(|_| "btc".to_string()).to_lowercase();
    let private_key = env::var(PRIVATE_KEY_VAR).expect("POLYMARKET_PRIVATE_KEY");
    let _trade_size_usdc: Decimal = env::var("TRADE_SIZE_USDC").unwrap_or_else(|_| "10".to_string()).parse()?;

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
    let (balance_tx, _balance_rx) = watch::channel(dec!(0));

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

    tokio::spawn(dradis::tasks::oracle::run_oracle(
        crypto_filter.clone(),
        oracle_tx,
        velocity_tx,
        drift_60m_tx,
    ));

    tokio::spawn(dradis::tasks::funding::run_funding_poller(
        Arc::clone(&shared_http),
        crypto_filter.clone(),
        funding_tx,
    ));

    let positions: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(PositionMap::new()));
    // Local Pending Map to debounce rapid-fire orders (log flood protection)
    let pending_orders: Arc<Mutex<HashMap<(String, U256), Instant>>> = Arc::new(Mutex::new(HashMap::new()));

    let total_pnl: Arc<Mutex<Decimal>> = Arc::new(Mutex::new(dec!(0)));
    // Live pUSD balance — updated every 60s in the status ticker so strategies can self-gate
    // when the wallet cannot afford even the minimum trade, preventing 400 CLOB rejections.
    let live_collateral: Arc<Mutex<Decimal>> = Arc::new(Mutex::new(startup_balance));
    let phantom_cooldowns: dradis::helpers::balance::PhantomCooldowns = Arc::new(Mutex::new(HashMap::new()));
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
    let mut initial_strike = extract_strike_price(&name);
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

    tokio::spawn(dradis::tasks::market_monitor::run_market_monitor(
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
        let _verifying_contract = if is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };

        info!("✅ Cached Settings: NegRisk: {} | YES fee {} bps | NO fee {} bps", is_neg_risk, yes_fee_rate, no_fee_rate);

        let (yes_price_tx, yes_price_rx) = watch::channel::<PriceState>((dec!(0), dec!(0), dec!(1), dec!(0)));
        let (no_price_tx, no_price_rx) = watch::channel::<PriceState>((dec!(0), dec!(0), dec!(1), dec!(0)));

        for (token, tx) in [(yes_token, yes_price_tx), (no_token, no_price_tx)] {
            tokio::spawn(async move {
                loop {
                    let client = WsClient::default();
                    let stream = match client.subscribe_orderbook(vec![token]) {
                        Ok(s) => s,
                        Err(e) => {
                            warn!("⚠️ WS subscribe failed for token {}: {}. Retrying in 5s...", token, e);
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            continue;
                        }
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
                        } else {
                            warn!("⚠️ WS stream error for token {}. Restarting...", token);
                            break;
                        }
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
                            Err(e) => {
                                warn!("⚠️ WS Maker subscribe failed for token {}: {}. Retrying in 5s...", token, e);
                                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                continue;
                            }
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
                            } else {
                                warn!("⚠️ WS Maker stream error for token {}. Restarting...", token);
                                break;
                            }
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
            warn!("⚠️ No maker venue selected (window/daily unavailable). Non-momentum strategies will fallback to hourly market context.");
            None
        };

        let mut ticker = interval(config::main_ticker_interval());
        let mut status_ticker = interval(std::time::Duration::from_secs(60));
        let mut cleanup_ticker = interval(std::time::Duration::from_secs(300));
        let mut pulse_ticker = interval(std::time::Duration::from_secs(300));

        let strategies = StrategyRegistry::create_all_strategies();
        let adoption_order = StrategyRegistry::strategy_names();
        let live_collateral = Arc::clone(&live_collateral);

        // Scan on-chain balances at loop start (startup + market rotation) and adopt any
        // untracked positions so no strategy double-enters on top of existing on-chain shares.
        // Split into two calls so each venue's positions are tagged with the correct close_time
        // and market_name — preventing BasisExpiry from firing against the hourly market's
        // close time when the actual position lives in the daily/maker venue.
        // adoption_order comes from the registry — no hardcoded strategy list here.
        tokio::time::sleep(Duration::from_secs(2)).await; // allow CLOB API to be ready
        reconcile_orphaned_positions(
            &trading_client, &positions,
            &[(yes_token, "YES"), (no_token, "NO")],
            &market_name, market_close_time, &[], &adoption_order,
        ).await;
        if let Some(ref mk) = maker_market_config {
            reconcile_orphaned_positions(
                &trading_client, &positions,
                &[(mk.yes_token, "YES(maker)"), (mk.no_token, "NO(maker)")],
                &mk.market_name, mk.market_close_time, &[], &adoption_order,
            ).await;
        }

        let mut last_trade_time: HashMap<String, Instant> = HashMap::new();
        let mut last_stop_loss_time: HashMap<String, Instant> = HashMap::new();
        let mut last_expiry_exit_time: HashMap<String, Instant> = HashMap::new(); // 5-min block after expiry exits
        let mut consecutive_failures: u32 = 0;
        let mut last_executor_summary = String::new(); // change-detection for 📊 INFO tick summary
        let tg_token = env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default();
        let tg_chat_id = env::var("TELEGRAM_CHAT_ID").unwrap_or_default();

        info!("🤖 Orchestrator ready: {} strategies loaded", strategies.len());
        info!("🧭 Strategy venue attachments:");
        for strategy in &strategies {
            let sn = strategy.name();
            let (venue, market_name_attached, budget, risk_model) = match sn.as_str() {
                "MomentumStrategy" => (
                    "Hourly",
                    market_name.clone(),
                    config::MOMENTUM_MAX_EXPOSURE_USDC,
                    "Gross one-sided",
                ),
                "MakerStrategy" => {
                    let attached_name = maker_market_config
                        .as_ref()
                        .map(|m| m.market_name.clone())
                        .unwrap_or_else(|| market_name.clone());
                    let venue_name = if maker_market_config.is_some() { "Window/Daily" } else { "Hourly (fallback)" };
                    (venue_name, attached_name, config::MAKER_MAX_EXPOSURE_USDC, "Net |YES-NO|")
                }
                "ArbitrageStrategy" => {
                    let attached_name = maker_market_config
                        .as_ref()
                        .map(|m| m.market_name.clone())
                        .unwrap_or_else(|| market_name.clone());
                    let venue_name = if maker_market_config.is_some() { "Window/Daily" } else { "Hourly (fallback)" };
                    (venue_name, attached_name, config::ARBITRAGE_MAX_EXPOSURE_USDC, "Gross hedged (per leg)")
                }
                "TimeDecayStrategy" => {
                    let attached_name = maker_market_config
                        .as_ref()
                        .map(|m| m.market_name.clone())
                        .unwrap_or_else(|| market_name.clone());
                    let venue_name = if maker_market_config.is_some() { "Window/Daily" } else { "Hourly (fallback)" };
                    (venue_name, attached_name, config::TIME_DECAY_MAX_EXPOSURE_USDC, "Gross hedged (per leg)")
                }
                "BasisStrategy" => {
                    let attached_name = maker_market_config
                        .as_ref()
                        .map(|m| m.market_name.clone())
                        .unwrap_or_else(|| market_name.clone());
                    let venue_name = if maker_market_config.is_some() { "Window/Daily" } else { "Hourly (fallback)" };
                    (venue_name, attached_name, config::BASIS_MAX_EXPOSURE_USDC, "Gross one-sided")
                }
                _ => ("Unknown", market_name.clone(), dec!(0), "Unknown"),
            };

            info!(
                "  - {} => venue={} | market=\"{}\" | budget=${} | risk={}",
                sn,
                venue,
                market_name_attached,
                budget,
                risk_model,
            );
        }

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
                    { pending_orders.lock().await.clear(); } // Clear pending locks on market switch
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
                    dradis::tasks::cleanup::cleanup_expired_positions(Arc::clone(&positions), market_name.clone(), yes_token, no_token, market_close_time).await;
                    if let Err(e) = dradis::tasks::cleanup::reconcile_orphaned_positions(Arc::clone(&positions), &tg_token, &tg_chat_id).await { warn!("⚠️ Orphan reconciliation error: {}", e); }
                    dradis::tasks::cleanup::cleanup_time_decay_positions(Arc::clone(&time_decay_positions)).await;

                    // Periodically clean up expired pending order locks
                    {
                        let mut pending = pending_orders.lock().await;
                        pending.retain(|_, &mut instant| instant > Instant::now());
                    }
                }
                _ = status_ticker.tick() => {
                    let (yb, ybd, ya, yad) = *yes_price_rx.borrow();
                    let (nb, nbd, na, nad) = *no_price_rx.borrow();
                    // Compute OBI for heartbeat visibility so thresholds can be tuned empirically.
                    let yes_obi = if ybd + yad > dec!(0) { (ybd - yad) / (ybd + yad) } else { dec!(0) };
                    let no_obi  = if nbd + nad > dec!(0) { (nbd - nad) / (nbd + nad) } else { dec!(0) };
                    info!("💓 Heartbeat | Ask Sum ${:.4} (Y ask ${:.2} / N ask ${:.2}) | Bid Sum ${:.4} (Y bid ${:.2} / N bid ${:.2}) | Binance: ${:.2} | OBI Y={:.2} N={:.2}",
                        ya + na, ya, na, yb + nb, yb, nb, *oracle_rx.borrow(), yes_obi, no_obi);
                    // Refresh live pUSD balance so strategies can self-gate on insufficient funds.
                    let mut bal_req = BalanceAllowanceRequest::default();
                    bal_req.asset_type = AssetType::Collateral;
                    if let Ok(resp) = trading_client.balance_allowance(bal_req).await {
                        let bal = Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000);
                        *live_collateral.lock().await = bal;
                        debug!("💰 Live pUSD balance: ${:.4}", bal);
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
                        available_collateral: *live_collateral.lock().await,
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

                    let eval_result = match execute_strategies_concurrent(&strategies, &ctx, 500, &mut last_executor_summary).await {
                        Ok(r) => r,
                        Err(e) => { warn!("⚠️ Strategy evaluation error: {}", e); continue; }
                    };
                    let (resolved_signals, _) = aggregate_and_resolve_signals(&eval_result);
                    if resolved_signals.is_empty() { continue; }

                    for (strategy_name, signal) in resolved_signals {
                        let sn = strategy_name.clone();
                        match signal {
                            // ════════════════════ EXIT ════════════════════
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
                                        if es.contains("no orders found") {
                                            // FAK sell couldn't find buyers at the current ask level.
                                            // Position stays in the map so the exit re-fires next heartbeat,
                                            // BUT we impose a full stop-loss cooldown so the strategy cannot
                                            // flip to a new ENTRY while on-chain shares may still exist.
                                            // This was the root cause of 49-share position compounding.
                                            warn!("⚠️ EXIT FAK miss [{}]: no buyers at ${:.4} — holding position, cooldown {}s", sn, params.price, config::STOP_LOSS_COOLDOWN_SECS);
                                            last_trade_time.insert(sn.clone(), Instant::now());
                                            if reason.to_lowercase().contains("sl") || reason.to_lowercase().contains("stop") || reason.to_lowercase().contains("toxic") {
                                                last_stop_loss_time.insert(sn.clone(), Instant::now());
                                            }
                                        } else {
                                            consecutive_failures += 1;
                                        }
                                        continue;
                                    }
                                }

                                let re_m;
                                let rs_m;
                                let rc_m;
                                let pnl_m;

                                {
                                    let mut map = positions.lock().await;
                                    if let Some(p) = map.remove(&pos_key) {
                                        let pnl = (params.price - p.avg_entry) * p.shares;
                                        *total_pnl.lock().await += pnl;
                                        info!("💰 Position closed [{}]: PnL ${:.4}", sn, pnl);

                                        re_m = p.avg_entry;
                                        rs_m = p.shares;
                                        rc_m = p.close_time;
                                        pnl_m = pnl;

                                        // Record trade metrics asynchronously
                                        let sn_task = sn.clone();
                                        let m_name = params.market_name.clone();
                                        let sid = if tid == yes_token { "YES".to_string() } else { "NO".to_string() };
                                        let rp = params.price;
                                        let r_m = reason.clone();
                                        tokio::spawn(async move { metrics::record_trade(sn_task, m_name, sid, re_m, rp, rs_m, pnl_m, r_m).await; });
                                    } else { continue; }
                                }

                                if rs_m > dec!(0) {
                                    let ps = Arc::clone(&positions); let cl = Arc::clone(&trading_client); let tp = Arc::clone(&total_pnl); let m_name = params.market_name.clone();
                                    let sn_async = sn.clone();
                                    tokio::spawn(async move {
                                        tokio::time::sleep(Duration::from_millis(2500)).await;
                                        let mut req = BalanceAllowanceRequest::default(); req.asset_type = AssetType::Conditional; req.token_id = Some(tid);
                                        let rem = match cl.balance_allowance(req).await { Ok(r) => Decimal::from_str(&r.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000), Err(_) => return };
                                        if rem >= config::MIN_ORDER_SHARES {
                                            let fill = (rs_m - rem).max(dec!(0)); let pnlc = -((params.price - re_m) * rem.min(rs_m)); *tp.lock().await += pnlc;
                                            if fill < config::MIN_ORDER_SHARES { warn!("⚠️ PARTIAL EXIT [{}]: FAK filled 0/{:.4} shares — retry on next loop.", sn_async, rs_m); }
                                            else { warn!("⚠️ PARTIAL EXIT [{}]: sold {:.4}/{:.4} — re-inserting.", sn_async, fill, rs_m); let mut map = ps.lock().await; if !map.contains_key(&(sn_async.clone(), tid)) { map.insert((sn_async.clone(), tid), Position { shares: rem, avg_entry: re_m, opened_at: Utc::now(), close_time: rc_m, market_name: m_name, pair_token_id: tid, fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: None }); } }
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
                                        let mut map = positions.lock().await; if let Some(p) = map.remove(&pk) { let pnl = (other_bid - p.avg_entry) * p.shares; *total_pnl.lock().await += pnl;
                                            let sn_pm = sn.clone(); let m_name = params.market_name.clone(); let sid = if other_tid == yes_token { "YES".to_string() } else { "NO".to_string() }; let p_avg = p.avg_entry; let o_bid = other_bid; let p_shares = p.shares; let pn = pnl;
                                            tokio::spawn(async move { metrics::record_trade(sn_pm, m_name, sid, p_avg, o_bid, p_shares, pn, "Convergence/PairedExit".to_string()).await; });
                                        }
                                    }
                                }
                                // Trigger 180s cooldown for any stop-loss variant: BasisSL, Maker SL,
                                // Time Decay SL, ToxicFill, BasisSkewCollapse. Must match the same
                                // predicate used in the FAK-miss path above (line ~601) so both the
                                // successful-exit and the FAK-miss path are consistent.
                                if reason.to_lowercase().contains("sl")
                                    || reason.to_lowercase().contains("stop")
                                    || reason.to_lowercase().contains("toxic")
                                    || reason.to_lowercase().contains("skewcollapse")
                                {
                                    last_stop_loss_time.insert(sn.clone(), Instant::now());
                                }
                                // After an expiry exit, block re-entry for 5 minutes via a separate map.
                                if reason.to_lowercase().contains("expir") { last_expiry_exit_time.insert(sn.clone(), Instant::now()); }
                                last_trade_time.insert(sn.clone(), Instant::now());
                                let _ = send_notification(&tg_token, &tg_chat_id, &format!("📤 EXIT [{}] {} | bid=${:.4} | reason: {} | Session PnL: ${:.4}", sn, params.market_name, params.price, reason, *total_pnl.lock().await)).await;
                            }

                            // ════════════════════ ENTRY ════════════════════
                            StrategySignal::Entry { params, pair_params } => {
                                if let Some(close_time) = market_close_time { if (close_time - Utc::now()).num_seconds() < config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY { continue; } }
                                if let Some(lt) = last_trade_time.get(&sn) { if lt.elapsed() < Duration::from_secs(config::TRADE_COOLDOWN_SECS as u64) { continue; } }
                                // Block re-entry for STOP_LOSS_COOLDOWN_SECS after any stop-loss (successful or FAK miss).
                                // Prevents compounding into a token where on-chain shares may still exist.
                                if let Some(lt) = last_stop_loss_time.get(&sn) { if lt.elapsed() < Duration::from_secs(config::STOP_LOSS_COOLDOWN_SECS) { continue; } }
                                // 5-minute block after an expiry exit — the market was closing, re-entry is pointless
                                if let Some(lt) = last_expiry_exit_time.get(&sn) { if lt.elapsed() < Duration::from_secs(300) { continue; } }

                                let pos_key = (sn.clone(), params.token_id);

                                // Debounce: check if an order is already pending for this token
                                {
                                    let pending = pending_orders.lock().await;
                                    if let Some(expiry) = pending.get(&pos_key) {
                                        if expiry > &Instant::now() { continue; }
                                    }
                                }

                                {
                                    let mut map = positions.lock().await; if map.contains_key(&pos_key) { continue; }
                                    // Use the maker market's close_time when the entry token belongs to the maker
                                    // venue — prevents BasisExpiry from firing against the hourly market's close
                                    // time when the position actually lives in the daily/window market.
                                    let pos_close_time = maker_market_config.as_ref()
                                        .filter(|mk| params.token_id == mk.yes_token || params.token_id == mk.no_token)
                                        .and_then(|mk| mk.market_close_time)
                                        .or(market_close_time);
                                    map.insert(pos_key.clone(), Position { shares: params.shares, avg_entry: params.price, opened_at: Utc::now(), close_time: pos_close_time, market_name: params.market_name.clone(), pair_token_id: params.token_id, fill_confirmed_at: None, paired_leg_token_id: pair_params.as_ref().map(|p| p.token_id) });
                                }

                                // Set pending lock for 3 seconds
                                {
                                    pending_orders.lock().await.insert(pos_key.clone(), Instant::now() + Duration::from_secs(3));
                                }

                                info!("📥 ENTRY [{}]: {} | ${:.4} x {:.1}", sn, params.market_name, params.price, params.shares);
                                let vc = if params.is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                if !config::GHOST_MODE {
                                    if let Err(e) = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc, params.token_id, Side::Buy, params.shares, (params.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE), params.fee_bps, OrderType::FAK, false, 0, &shared_http).await {
                                        warn!("⚠️ ENTRY order failed [{}]: {}", sn, e);
                                        positions.lock().await.remove(&pos_key);
                                        pending_orders.lock().await.remove(&pos_key);
                                        // Impose cooldown so the strategy backs off before retrying
                                        last_trade_time.insert(sn.clone(), Instant::now());
                                        consecutive_failures += 1; continue;
                                    }
                                }
                                let cl_s = Arc::clone(&trading_client); let ps_s = Arc::clone(&positions); let pc_s = Arc::clone(&phantom_cooldowns); let sn_s = sn.clone(); let tn_s = params.token_id;
                                tokio::spawn(async move { let _ = sync_position_balance(&cl_s, &ps_s, &sn_s, tn_s, Some(&pc_s), dec!(0), dradis::helpers::balance::MAX_WAIT_SECS_HOURLY).await; });

                                if let Some(pp) = pair_params {
                                    let vc_p = if pp.is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };

                                    // Capture Leg B result explicitly — previously discarded with `let _ = ...`.
                                    // On failure we spawn a Flash-Exit task that sells Leg A as soon as the
                                    // Polymarket indexer reflects the fill (~5-12 s), instead of waiting 60 s
                                    // for the cleanup task to notice the orphan.
                                    let leg_b_result = if !config::GHOST_MODE {
                                        place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc_p, pp.token_id, Side::Buy, pp.shares, (pp.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE), pp.fee_bps, OrderType::FAK, false, 0, &shared_http).await
                                    } else { Ok(()) };

                                    match leg_b_result {
                                        Err(ref e) => {
                                            // Leg B explicitly rejected — spawn Flash-Exit for Leg A.
                                            // The task polls Leg A's on-chain balance every FLASH_EXIT_POLL_MS;
                                            // once confirmed, it fires an emergency FAK sell and removes the
                                            // position.  If Leg A was also a phantom (no fill), the task exits
                                            // after FLASH_EXIT_CONFIRM_MS and sync_position_balance handles cleanup.
                                            warn!("⚡ Leg B FAILED [{}]: {} — Flash-Exit spawned for Leg A (token {})", sn, e, params.token_id);
                                            let cl_fe   = Arc::clone(&trading_client);
                                            let nm_fe   = Arc::clone(&nonce_manager);
                                            let ps_fe   = Arc::clone(&positions);
                                            let http_fe = Arc::clone(&shared_http);
                                            let signer_fe = signer.clone();
                                            let sn_fe   = sn.clone();
                                            let tok_a   = params.token_id;
                                            // Best bid on the Leg A token at the moment of failure.
                                            // Stale by the time the fill is confirmed (5-12 s), so we apply
                                            // an extra haircut (FLASH_EXIT_EXTRA_OFFSET) to guarantee the
                                            // emergency FAK crosses the spread and fills immediately.
                                            let bid_a   = if tok_a == yes_token { yb } else { nb };
                                            let fee_a   = params.fee_bps;
                                            let vc_a    = vc; // verifying contract for Leg A
                                            tokio::spawn(async move {
                                                let deadline = tokio::time::Instant::now()
                                                    + Duration::from_millis(config::FLASH_EXIT_CONFIRM_MS);
                                                loop {
                                                    if tokio::time::Instant::now() >= deadline {
                                                        // Leg A also phantom — sync_position_balance will clean it up
                                                        warn!("⚡ Flash-Exit: Leg A phantom (no fill in {}ms) [{}]",
                                                              config::FLASH_EXIT_CONFIRM_MS, sn_fe);
                                                        break;
                                                    }
                                                    let mut req = BalanceAllowanceRequest::default();
                                                    req.asset_type = AssetType::Conditional;
                                                    req.token_id = Some(tok_a);
                                                    if let Ok(resp) = cl_fe.balance_allowance(req).await {
                                                        let shares = Decimal::from_str(&resp.balance.to_string())
                                                            .unwrap_or(dec!(0)) / dec!(1_000_000);
                                                        if shares >= config::MIN_ORDER_SHARES {
                                                            // Leg A confirmed — emergency FAK sell
                                                            let sell_price = (bid_a
                                                                - config::SELL_PRICE_OFFSET
                                                                - config::FLASH_EXIT_EXTRA_OFFSET)
                                                                .max(config::MIN_SELL_LIMIT_PRICE);
                                                            info!("⚡ Flash-Exit SELLING [{}]: {:.2} shares @ ${:.4} (bid was ${:.4})",
                                                                  sn_fe, shares, sell_price, bid_a);
                                                            match place_limit_order(
                                                                &cl_fe, &nm_fe, &signer_fe,
                                                                safe_address, eoa_address, vc_a,
                                                                tok_a, Side::Sell, shares, sell_price,
                                                                fee_a, OrderType::FAK, false, 0, &http_fe,
                                                            ).await {
                                                                Ok(_)  => info!("⚡ Flash-Exit sold Leg A [{}] ✓", sn_fe),
                                                                Err(e) => warn!("⚡ Flash-Exit sell FAILED [{}]: {} — cleanup task will catch it", sn_fe, e),
                                                            }
                                                            ps_fe.lock().await.remove(&(sn_fe.clone(), tok_a));
                                                            break;
                                                        }
                                                    }
                                                    tokio::time::sleep(Duration::from_millis(config::FLASH_EXIT_POLL_MS)).await;
                                                }
                                            });
                                            // Leg B was not filled — do NOT insert its position
                                        },
                                        Ok(_) => {
                                            // Normal path: Leg B accepted — insert position and start sync
                                            let pp_close_time = maker_market_config.as_ref()
                                                .filter(|mk| pp.token_id == mk.yes_token || pp.token_id == mk.no_token)
                                                .and_then(|mk| mk.market_close_time)
                                                .or(market_close_time);
                                            positions.lock().await.insert((sn.clone(), pp.token_id), Position { shares: pp.shares, avg_entry: pp.price, opened_at: Utc::now(), close_time: pp_close_time, market_name: pp.market_name.clone(), pair_token_id: pp.token_id, fill_confirmed_at: None, paired_leg_token_id: Some(params.token_id) });
                                            let sn_p = sn.clone(); let tn_p = pp.token_id; let ps_p = Arc::clone(&positions); let cl_p = Arc::clone(&trading_client); let pc_p = Arc::clone(&phantom_cooldowns);
                                            tokio::spawn(async move { let _ = sync_position_balance(&cl_p, &ps_p, &sn_p, tn_p, Some(&pc_p), dec!(0), dradis::helpers::balance::MAX_WAIT_SECS_HOURLY).await; });
                                        }
                                    }
                                }
                                last_trade_time.insert(sn.clone(), Instant::now());
                                let _ = send_notification(&tg_token, &tg_chat_id, &format!("📥 ENTRY [{}] {} | ${:.4} x {:.1}", sn, params.market_name, params.price, params.shares)).await;
                            }

                            // ════════════════════ MAKER QUOTE ════════════════════
                            StrategySignal::MakerQuote { yes, no } => {
                                let mut placed = false;
                                for p in [yes, no].into_iter().flatten() {
                                    let pk = (sn.clone(), p.token_id);

                                    // Debounce: check if an order is already pending for this token
                                    {
                                        let pending = pending_orders.lock().await;
                                        if let Some(expiry) = pending.get(&pk) {
                                            if expiry > &Instant::now() { continue; }
                                        }
                                    }

                                    if !positions.lock().await.contains_key(&pk) {
                                        info!("📥 MakerQuote [{}]: {} | shares={:.2}, bid=${:.4}", sn, p.market_name, p.shares, p.price);
                                        positions.lock().await.insert(pk.clone(), Position { shares: p.shares, avg_entry: p.price, opened_at: Utc::now(), close_time: None, market_name: p.market_name.clone(), pair_token_id: p.token_id, fill_confirmed_at: None, paired_leg_token_id: None });

                                        // Set pending lock for 3 seconds
                                        {
                                            pending_orders.lock().await.insert(pk.clone(), Instant::now() + Duration::from_secs(3));
                                        }

                                        let _ = dradis::helpers::balance::quick_confirm_fill(&trading_client, &sn, p.token_id, &positions, &p.condition_id).await;
                                        let vc = if p.is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                        if !config::GHOST_MODE {
                                            if let Err(e) = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc, p.token_id, Side::Buy, p.shares, p.price, p.fee_bps, OrderType::GTC, true, 0, &shared_http).await {
                                                positions.lock().await.remove(&pk);
                                                pending_orders.lock().await.remove(&pk);
                                                if !e.to_string().contains("crosses book") { consecutive_failures += 1; } continue;
                                            }
                                            let cl_m = Arc::clone(&trading_client); let ps_m = Arc::clone(&positions); let pc_m = Arc::clone(&phantom_cooldowns);
                                            let sn_m = sn.clone();
                                            tokio::spawn(async move { let _ = sync_position_balance(&cl_m, &ps_m, &sn_m, p.token_id, Some(&pc_m), dec!(0), dradis::helpers::balance::MAX_WAIT_SECS_WINDOW).await; });
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
