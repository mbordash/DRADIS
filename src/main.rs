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
use alloy::providers::ProviderBuilder;
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
use dradis::helpers::dynamic_config::DynamicConfig;

// New paths for helpers
use dradis::helpers::{
    time::*, balance::*, nonce::*, orders::*, market::*,
    notifications::send_notification, notifications::tweet_trade, metrics,
    db,
};

use rustls::crypto::ring;

// Import MarketState type from market_monitor
use dradis::tasks::market_monitor::MarketState;


type PriceState = (Decimal, Decimal, Decimal, Decimal, chrono::DateTime<chrono::Utc>); // (Bid, BidDepth, Ask, AskDepth, WsUpdateTimestamp)

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
  ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝ ╚╝╚══════╝
  Direct Reaction And Dynamic Intelligence System  v{}
  ─────────────────────────────────────────────────────

          ·  ·  ·  ·  ·  ·  ·  ·  ·
       ·     ·        |        ·     ·
     ·    ·     ·     |     ·     ·    ·
    ·   ·    ·    · ──●── ·    ·    ·   ·
     ·    ·     ·     |     ·     ·    ·
       ·     ·        |        ·     ·
          ·  ·  ·  ·  ·  ·  ·  ·  ·
 P O L Y M A R K E T  C L O B  I N F O  C E N T E R

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

// Constants for cancel_all_orders retry logic
const MAX_CANCEL_RETRIES: u32 = 5;
const BASE_CANCEL_RETRY_DELAY_MS: u64 = 200; // Start with 200ms

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

    // ── SQLite + DynamicConfig ────────────────────────────────────────────────
    // Init DB first so DynamicConfig::load_or_default can read from it.
    if let Err(e) = db::init("logs/dradis.db").await {
        tracing::warn!("⚠️  SQLite init failed (metrics will CSV-only): {}", e);
    }
    // Register this process start as a new session.  Every restart is a clean
    // session boundary: session-scoped P&L, LLM analysis, and config snapshots
    // are all anchored to this ID for the lifetime of the process.
    let _session_id = db::init_session(Some("dradis startup")).await;

    // Snapshot the compile-time constants (config.rs) into config_history.
    // This runs BEFORE DynamicConfig::load_or_default() so both snapshots land
    // in the same session with the static one first.  Because these constants can
    // only change via recompile+restart, diffing two consecutive startup_static
    // rows immediately shows what the developer tuned between sessions.
    if let Some(pool) = db::pool() {
        db::record_static_config_snapshot(pool).await;
    }

    let initial_dyn_cfg = DynamicConfig::load_or_default().await;
    // Wrap the sender in Arc so it can be shared with the axum API server.
    let (config_tx, config_rx) = watch::channel(initial_dyn_cfg);
    let config_tx = Arc::new(config_tx);

    // ── Strategy→market status channel (feeds /api/status) ───────────────────
    let (markets_tx, markets_rx) = watch::channel::<std::collections::HashMap<String, String>>(std::collections::HashMap::new());
    let markets_tx = Arc::new(markets_tx);

    // ── Spawn Control Tower API server ───────────────────────────────────────
    tokio::spawn(dradis::api::server::run_api_server(
        Arc::clone(&config_tx),
        config_rx.clone(),  // API server gets its own watch receiver
        markets_rx,
    ));

    let crypto_filter = env::var("CRYPTO_FILTER").unwrap_or_else(|_| "btc".to_string()).to_lowercase();
    let private_key = env::var(PRIVATE_KEY_VAR).expect("POLYMARKET_PRIVATE_KEY");
    let _trade_size_usdc: Decimal = env::var("TRADE_SIZE_USDC").unwrap_or_else(|_| "10".to_string()).parse()?;

    let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
    let eoa_address = signer.address();
    info!("Trading wallet (EOA) address: {}", eoa_address);

    let polygon_rpc_url = env::var("POLYGON_RPC_URL")
        .map_err(|_| anyhow::anyhow!("❌ POLYGON_RPC_URL not set in .env. Required for auto-settlement transactions. Use a paid RPC service like Helius (https://www.helius-rpc.com) or QuickNode. Example: POLYGON_RPC_URL=https://mainnet.helius-rpc.com/?api-key=YOUR_KEY"))?;

    let wallet_provider = ProviderBuilder::new()
        .wallet(signer.clone())
        .connect(&polygon_rpc_url)
        .await?;
    info!("✅ CTF auto-settlement client ready (rpc={})", polygon_rpc_url);

    let trading_client = Arc::new(ClobClient::new(config::CLOB_API_BASE, Config::default())?
        .authentication_builder(&signer)
        .signature_type(SignatureType::GnosisSafe)
        .authenticate()
        .await?);

    let safe_address = derive_safe_wallet(eoa_address, POLYGON).expect("Safe derivation failed");
    info!("Authenticated on Polymarket CLOB. Safe (Maker) address: {}", safe_address);

    let initial_nonce = fetch_next_nonce(&shared_http, safe_address).await.unwrap_or(0);
    info!(" Initialized Nonce from API (Maker/Safe): {}", initial_nonce);
    let nonce_manager = Arc::new(AtomicU64::new(initial_nonce));

    let starting_collateral_store = Arc::new(Mutex::new(dec!(0.0)));
    let (balance_tx, _balance_rx) = watch::channel(dec!(0));

    let mut startup_balance = dec!(0);
    for i in 1..=3 {
        info!(" Initializing portfolio balance (Attempt {}/3)...", i);
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
    info!(" Starting portfolio value: ${:.2}", startup_balance);

    // ── Startup: cancel any GTC orders left over from the previous session ───
    // Without this, resting orders from a previous restart count against the
    // CLOB balance allowance and cause Leg B "not enough balance" failures on
    // the very first entry of the new session.
    info!(" Cancelling any leftover open orders from previous session...");
    for i in 0..MAX_CANCEL_RETRIES {
        let delay = BASE_CANCEL_RETRY_DELAY_MS * (1 << i);
        match tokio::time::timeout(Duration::from_secs(8), trading_client.as_ref().cancel_all_orders()).await {
            Ok(Ok(_)) => { info!("✅ Startup cancel complete (attempt {}).", i + 1); break; }
            Ok(Err(e)) => {
                warn!("⚠️ Startup cancel failed (attempt {}/{}): {}", i + 1, MAX_CANCEL_RETRIES, e);
                if i < MAX_CANCEL_RETRIES - 1 { tokio::time::sleep(Duration::from_millis(delay)).await; }
            }
            Err(_) => {
                warn!("⚠️ Startup cancel timed out (attempt {}/{})", i + 1, MAX_CANCEL_RETRIES);
                if i < MAX_CANCEL_RETRIES - 1 { tokio::time::sleep(Duration::from_millis(delay)).await; }
            }
        }
    }

    // ── Startup: rebuild open_positions DB from on-chain state (LIVE mode only) ──
    //
    // In LIVE mode (GHOST_MODE = false) the local DB is NOT the source of truth —
    // Polymarket is.  Rows written by prior sessions (orders that placed but never
    // filled, crashes before close_open_position ran, orphan accumulations) pollute
    // the UI and LLM analysis with phantom positions.
    //
    // Strategy:
    //   1. Nuke ALL ghost_mode=0 rows — clean slate, no stale prior-session garbage.
    //   2. sync_open_positions_with_chain() re-adopts every token that is actually
    //      held on-chain right now, so the Control Tower shows exactly what the
    //      wallet holds before the first strategy tick fires.
    //   3. During the session, record_open_position() is only called AFTER a fill
    //      is confirmed on-chain (inside the sync_position_balance spawn), so the DB
    //      never gets a row that doesn't reflect a real on-chain position.
    //
    // Ghost mode keeps its own rows (ghost_mode=1) untouched across restarts so
    // simulated trade history remains coherent.
    if !config::GHOST_MODE {
        if let Some(pool) = db::pool() {
            let purged = db::purge_all_live_open_positions(pool).await;
            if purged > 0 {
                info!("️  Cleared {} stale live open_position row(s) from prior session(s)", purged);
            }
        }
    }
    info!(" Syncing open_positions DB with on-chain holdings...");
    dradis::tasks::cleanup::sync_open_positions_with_chain(safe_address).await;

    let (oracle_tx, oracle_rx) = watch::channel(dec!(0));
    let (velocity_tx, velocity_rx) = watch::channel((dec!(0), dec!(0), dec!(0)));
    let (funding_tx, funding_rx) = watch::channel(dec!(0));
    let (drift_tx, drift_rx) = watch::channel((dec!(0), dec!(0)));

    tokio::spawn(dradis::tasks::oracle::run_oracle(
        crypto_filter.clone(),
        oracle_tx,
        velocity_tx,
        drift_tx,
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

    // ── LLM Advisor (optional) ────────────────────────────────────────────────
    // Spawned unconditionally; exits immediately when ENABLE_LLM_ADVISOR = false,
    // so there is zero overhead when the feature is disabled.
    tokio::spawn(dradis::helpers::llm_advisor::run_llm_advisor_loop(
        env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default(),
        env::var("TELEGRAM_CHAT_ID").unwrap_or_default(),
        Arc::clone(&total_pnl),
        Arc::clone(&starting_collateral_store),
        config_rx.clone(),
    ));

    // when the wallet cannot afford even the minimum trade, preventing 400 CLOB rejections.
    let live_collateral: Arc<Mutex<Decimal>> = Arc::new(Mutex::new(startup_balance));
    let phantom_cooldowns: dradis::helpers::balance::PhantomCooldowns = Arc::new(Mutex::new(HashMap::new()));
    let time_decay_positions: Arc<Mutex<HashMap<U256, TimeDecayPosition>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // Cooldown maps live OUTSIDE the market-rotation loop so they survive market switches.
    // Previously declared inside the loop which reset them on every hourly market transition,
    // allowing BasisStrategy to re-enter immediately after a stop-loss via a market switch.
    let mut last_trade_time: HashMap<String, Instant> = HashMap::new();
    let mut last_stop_loss_time: HashMap<String, Instant> = HashMap::new();
    let mut last_expiry_exit_time: HashMap<String, Instant> = HashMap::new();
    // Throttle exit retries: when a FAK sell misses, the position stays in the map
    // and evaluate_exit re-fires every 50ms heartbeat — without this guard that floods the
    // log with ~1200 identical EXIT lines per minute and hammers the API.
    let mut last_exit_attempt_time: HashMap<String, Instant> = HashMap::new();

    // Watchdog: tracks when the inner trading loop last emitted a heartbeat tick.
    // If the inner loop goes silent for >LOOP_WATCHDOG_SECS, the outer loop logs an
    // alarm and attempts a forced restart by breaking into a new market iteration.
    let last_heartbeat_at: Arc<Mutex<Instant>> = Arc::new(Mutex::new(Instant::now()));
    const LOOP_WATCHDOG_SECS: u64 = 180; // alert after 3 min of silence

    // Bootstrap: poll until we have a valid hourly market OR a valid maker market.
    // If only a maker market is found, the hourly market will be an empty MarketCandidate.

    let mut bootstrap_attempts = 0u32;
    let (mut initial_hourly_candidate, mut initial_maker_candidate) = loop {
        let (hourly_cand, maker_cand) = get_market_pair(&shared_http).await;

        if hourly_cand.yes_token != U256::ZERO {
            info!("✅ Found initial hourly market: \"{}\"", hourly_cand.name);
            break (hourly_cand, maker_cand);
        }

        if maker_cand.is_some() {
            info!("⚠️ No hourly market found, but a maker market is available. Starting with maker market context.");
            // Return an empty hourly candidate if only maker is available
            break (MarketCandidate { yes_token: U256::ZERO, no_token: U256::ZERO, name: String::new(), link: String::new(), description: String::new(), is_hot: false, close_time: None, volume: 0.0, condition_id: String::new(), strike_price: None }, maker_cand);
        }

        bootstrap_attempts += 1;
        warn!("⏳ No active hourly or maker market found (attempt {}) — retrying in 30s...", bootstrap_attempts);
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    };

    // Resolve strike price for the initial hourly market if it exists
    if initial_hourly_candidate.yes_token != U256::ZERO {
        let mut strike = extract_strike_price(&initial_hourly_candidate.name);
        if strike.is_none() {
            strike = fetch_historical_strike_price(&shared_http, &crypto_filter, &initial_hourly_candidate.description).await;
            if strike.is_none() {
                strike = fetch_historical_strike_price(&shared_http, &crypto_filter, &initial_hourly_candidate.name).await;
            }
        }
        initial_hourly_candidate.strike_price = strike;
        if strike.is_some() {
            info!("✅ Hourly market strike price resolved: ${}", strike.unwrap());
        }
    } else {
        info!(" No initial hourly market found. Waiting for market monitor to find one.");
    }

    // Resolve strike price for the initial maker market if it exists
    if let Some(ref mut mk) = initial_maker_candidate {
        let mut strike = extract_strike_price(&mk.name);
        if strike.is_none() {
            strike = fetch_historical_strike_price(&shared_http, &crypto_filter, &mk.description).await;
            if strike.is_none() {
                strike = fetch_historical_strike_price(&shared_http, &crypto_filter, &mk.name).await;
            }
        }
        mk.strike_price = strike;
        if strike.is_some() {
            info!("✅ Maker market strike price resolved: ${}", strike.unwrap());
        }
    } else {
        info!(" No initial maker market found. Waiting for market monitor to find one.");
    }

    // Construct the initial MarketState tuple for the watch channel
    let initial_market_state_for_channel: MarketState = (
        initial_hourly_candidate.yes_token,
        initial_hourly_candidate.no_token,
        initial_hourly_candidate.name.clone(),
        initial_hourly_candidate.close_time,
        initial_hourly_candidate.strike_price,
        initial_hourly_candidate.description.clone(),
        initial_maker_candidate, // This is Option<MarketCandidate>
        initial_hourly_candidate.condition_id.clone(),
    );

    let (market_tx, mut market_rx) = watch::channel(initial_market_state_for_channel);

    // current_hourly_cid and current_maker_cid are used to detect market switches.
    // Initialize them with the condition IDs of the initial markets.
    // NOTE: The order of these was swapped in the previous incorrect version.
    // `market_rx.borrow().7` is the hourly_condition_id
    // `market_rx.borrow().6` is the Option<MarketCandidate> for maker, so we need to map its condition_id
    let mut current_hourly_cid: String = market_rx.borrow().7.clone();
    let mut current_maker_cid: String = market_rx.borrow().6.as_ref().map_or_else(String::new, |m| m.condition_id.clone());


    tokio::spawn(dradis::tasks::market_monitor::run_market_monitor(
        Arc::clone(&shared_http),
        crypto_filter.clone(),
        market_tx.clone(),
    ));

    // Label the outer market-rotation loop so inner `continue 'market_loop` can
    // restart initialization if any CLOB API call times out during setup.
    // Previously this was an unlabelled `loop {}`, which meant there was no way
    // to escape a stalled .await without killing the process.
    'market_loop: loop {
        // Destructure the 8-element MarketState tuple from the channel
        let (
            hourly_yes_token,
            hourly_no_token,
            hourly_market_name,
            hourly_market_close_time,
            hourly_strike_price,
            _hourly_desc, // This is the description from the hourly market
            maker_market_candidate_from_channel, // This is Option<MarketCandidate>
            hourly_condition_id,
        ) = market_rx.borrow().clone();

        // If no hourly market is available, skip the hourly-dependent setup
        let (hourly_yes_token, hourly_no_token, hourly_market_name, hourly_market_close_time, hourly_strike_price, _hourly_desc, hourly_condition_id, hourly_is_neg_risk, hourly_yes_fee_rate, hourly_no_fee_rate) = if hourly_yes_token != U256::ZERO {
            let now = Utc::now();
            if let Some(close_time) = hourly_market_close_time {
                let seconds_until_expiry = (close_time - now).num_seconds();
                if seconds_until_expiry < config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY {
                    warn!("⚠️ Hourly market expiring too soon ({}s left)!", seconds_until_expiry);
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    continue 'market_loop;
                }
                info!("⏰ Hourly market closes in {}s", seconds_until_expiry);
            }

            info!(" Starting Orchestrated Trading on hourly market: \"{}\"", hourly_market_name);

            let yes_fee_rate = match tokio::time::timeout(
                Duration::from_secs(10),
                trading_client.fee_rate_bps(hourly_yes_token),
            ).await {
                Ok(Ok(r))  => r.base_fee,
                Ok(Err(e)) => { warn!("⚠️ hourly fee_rate YES error: {} — retrying init in 5s: {}", e, e); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
                Err(_)     => { warn!("⚠️ hourly fee_rate YES timed out (10s) — retrying init in 5s"); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
            };
            let no_fee_rate = match tokio::time::timeout(
                Duration::from_secs(10),
                trading_client.fee_rate_bps(hourly_no_token),
            ).await {
                Ok(Ok(r))  => r.base_fee,
                Ok(Err(e)) => { warn!("⚠️ hourly fee_rate NO error: {} — retrying init in 5s: {}", e, e); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
                Err(_)     => { warn!("⚠️ hourly fee_rate NO timed out (10s) — retrying init in 5s"); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
            };
            let is_neg_risk = match tokio::time::timeout(
                Duration::from_secs(10),
                trading_client.neg_risk(hourly_yes_token),
            ).await {
                Ok(Ok(r))  => r.neg_risk,
                Ok(Err(e)) => { warn!("⚠️ hourly neg_risk error: {} — retrying init in 5s: {}", e, e); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
                Err(_)     => { warn!("⚠️ hourly neg_risk timed out (10s) — retrying init in 5s"); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
            };
            info!("✅ Hourly market settings: NegRisk: {} | YES fee {} bps | NO fee {} bps", is_neg_risk, yes_fee_rate, no_fee_rate);
            (hourly_yes_token, hourly_no_token, hourly_market_name, hourly_market_close_time, hourly_strike_price, _hourly_desc, hourly_condition_id, is_neg_risk, yes_fee_rate, no_fee_rate)
        } else {
            info!("⚠️ No active hourly market found. Hourly-dependent strategies will be inactive.");
            (U256::ZERO, U256::ZERO, String::new(), None, None, String::new(), String::new(), false, 0, 0)
        };

        let market_started_at = Utc::now(); // This timestamp is for the current trading session, not necessarily market creation

        // Cancellation flag for all WS tasks belonging to THIS market iteration.
        // When we rotate to a new market (inner loop break), we send `true` so old
        // WS tasks stop reconnecting and release their memory / TCP connections.
        // Without this, each market rotation leaks 4 WS tasks that loop-reconnect
        // forever, gradually exhausting heap and triggering OOM kills.
        let (ws_cancel_tx, ws_cancel_rx) = watch::channel(false);

        let (yes_price_tx, yes_price_rx) = watch::channel::<PriceState>((dec!(0), dec!(0), dec!(1), dec!(0), Utc::now()));
        let (no_price_tx, no_price_rx) = watch::channel::<PriceState>((dec!(0), dec!(0), dec!(1), dec!(0), Utc::now()));

        // Only subscribe to hourly market WS if an hourly market is present
        if hourly_yes_token != U256::ZERO {
            for (token, tx) in [(hourly_yes_token, yes_price_tx.clone()), (hourly_no_token, no_price_tx.clone())] {
                let mut cancel_rx = ws_cancel_rx.clone();
                tokio::spawn(async move {
                    loop {
                        if *cancel_rx.borrow() { return; }
                        let client = WsClient::default();
                        let stream = match client.subscribe_orderbook(vec![token]) {
                            Ok(s) => s,
                            Err(e) => {
                                warn!("⚠️ WS subscribe failed for hourly token {}: {}. Retrying in 5s...", token, e);
                                tokio::select! {
                                    _ = cancel_rx.changed() => return,
                                    _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                                }
                                continue;
                            }
                        };
                        let mut stream = Box::pin(stream);
                        info!("✅ WS orderbook subscribed for hourly token {}", token);
                        loop {
                            tokio::select! {
                                biased;
                                _ = cancel_rx.changed() => { return; }
                                result = stream.next() => {
                                    match result {
                                        Some(Ok(book)) => {
                                            let (bid, bid_depth) = book.bids.iter()
                                                .max_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
                                                .map(|l| (l.price, l.size))
                                                .unwrap_or((dec!(0), dec!(0)));
                                            let (ask, ask_depth) = book.asks.iter()
                                                .min_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
                                                .map(|l| (l.price, l.size))
                                                .unwrap_or((dec!(1), dec!(0)));
                                            // Stamp the WebSocket orderbook update time here — NOT at tick time.
                                            let _ = tx.send((bid, bid_depth, ask, ask_depth, Utc::now()));
                                        }
                                        Some(Err(_)) | None => {
                                            warn!("⚠️ WS stream error for hourly token {}. Restarting...", token);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        tokio::select! {
                            _ = cancel_rx.changed() => return,
                            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                        }
                    }
                });
            }
        }

        let (maker_yes_price_rx, maker_no_price_rx) = if let Some(ref mk) = maker_market_candidate_from_channel {
            let (mk_yes_tx, mk_yes_rx) = watch::channel::<PriceState>((dec!(0), dec!(0), dec!(1), dec!(0), Utc::now()));
            let (mk_no_tx, mk_no_rx) = watch::channel::<PriceState>((dec!(0), dec!(0), dec!(1), dec!(0), Utc::now()));
            for (token, tx) in [(mk.yes_token, mk_yes_tx), (mk.no_token, mk_no_tx)] {
                let mut cancel_rx = ws_cancel_rx.clone();
                tokio::spawn(async move {
                    loop {
                        if *cancel_rx.borrow() { return; }
                        let client = WsClient::default();
                        let stream = match client.subscribe_orderbook(vec![token]) {
                            Ok(s) => s,
                            Err(e) => {
                                warn!("⚠️ WS Maker subscribe failed for token {}: {}. Retrying in 5s...", token, e);
                                tokio::select! {
                                    _ = cancel_rx.changed() => return,
                                    _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                                }
                                continue;
                            }
                        };
                        let mut stream = Box::pin(stream);
                        info!("✅ WS orderbook subscribed for maker token {}", token);
                        loop {
                            tokio::select! {
                                biased;
                                _ = cancel_rx.changed() => { return; }
                                result = stream.next() => {
                                    match result {
                                        Some(Ok(book)) => {
                                            let (bid, bid_depth) = book.bids.iter()
                                                .max_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
                                                .map(|l| (l.price, l.size))
                                                .unwrap_or((dec!(0), dec!(0)));
                                            let (ask, ask_depth) = book.asks.iter()
                                                .min_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
                                                .map(|l| (l.price, l.size))
                                                .unwrap_or((dec!(1), dec!(0)));
                                            // Stamp the WebSocket orderbook update time
                                            let _ = tx.send((bid, bid_depth, ask, ask_depth, Utc::now()));
                                        }
                                        Some(Err(_)) | None => {
                                            warn!("⚠️ WS Maker stream error for token {}. Restarting...", token);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        tokio::select! {
                            _ = cancel_rx.changed() => return,
                            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                        }
                    }
                });
            }
            (Some(mk_yes_rx), Some(mk_no_rx))
        } else {
            (None, None)
        };

        let maker_market_config: Option<MarketConfig> = if let Some(ref mk) = maker_market_candidate_from_channel {
            let mk_yes_fee = match tokio::time::timeout(
                Duration::from_secs(10),
                trading_client.fee_rate_bps(mk.yes_token),
            ).await {
                Ok(Ok(r))  => r.base_fee,
                Ok(Err(e)) => { warn!("⚠️ maker fee_rate YES error: {} — retrying init in 5s: {}", e, e); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
                Err(_)     => { warn!("⚠️ maker fee_rate YES timed out (10s) — retrying init in 5s"); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
            };
            let mk_no_fee = match tokio::time::timeout(
                Duration::from_secs(10),
                trading_client.fee_rate_bps(mk.no_token),
            ).await {
                Ok(Ok(r))  => r.base_fee,
                Ok(Err(e)) => { warn!("⚠️ maker fee_rate NO error: {} — retrying init in 5s: {}", e, e); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
                Err(_)     => { warn!("⚠️ maker fee_rate NO timed out (10s) — retrying init in 5s"); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
            };
            let mk_neg_risk = match tokio::time::timeout(
                Duration::from_secs(10),
                trading_client.neg_risk(mk.yes_token),
            ).await {
                Ok(Ok(r))  => r.neg_risk,
                Ok(Err(e)) => { warn!("⚠️ maker neg_risk error: {} — retrying init in 5s: {}", e, e); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
                Err(_)     => { warn!("⚠️ maker neg_risk timed out (10s) — retrying init in 5s"); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
            };
            info!("✅ Maker market settings: \"{}\" | NegRisk: {} | YES {} bps | NO {} bps",
                mk.name, mk_neg_risk, mk_yes_fee, mk_no_fee);
            Some(MarketConfig {
                yes_token: mk.yes_token,
                no_token: mk.no_token,
                market_name: mk.name.clone(),
                market_close_time: mk.close_time,
                strike_price: mk.strike_price,
                is_neg_risk: mk_neg_risk,
                condition_id: mk.condition_id.clone(),
                yes_fee_bps: mk_yes_fee,
                no_fee_bps: mk_no_fee,
            })
        } else {
            warn!("⚠️ No maker venue selected (window/daily unavailable). Strategies requiring maker venue will be inactive.");
            None
        };

        let mut ticker = interval(config::main_ticker_interval());
        let mut status_ticker = interval(std::time::Duration::from_secs(60));
        let mut cleanup_ticker = interval(std::time::Duration::from_secs(300));
        let mut settlement_ticker = interval(std::time::Duration::from_secs(config::MERGE_SCAN_INTERVAL_SECS));
        let mut pulse_ticker = interval(std::time::Duration::from_secs(300));
        // Watchdog ticker: checks every 120s if the strategy ticker has been alive.
        // If the inner loop goes silent (e.g., blocked on a stalled .await), this breaks
        // it out so the outer loop can restart with a fresh market context.

        let mut watchdog_ticker = interval(std::time::Duration::from_secs(120));
        watchdog_ticker.tick().await; // consume the immediate first tick
        *last_heartbeat_at.lock().await = Instant::now(); // reset watchdog on market start

        let strategies = StrategyRegistry::create_all_strategies();
        let adoption_order = StrategyRegistry::strategy_names();
        let live_collateral = Arc::clone(&live_collateral);

        // Scan on-chain balances at loop start (startup + market rotation) and adopt any
        // untracked positions so no strategy double-enters on top of existing on-chain shares.
        // Split into two calls so each venue's positions are tagged with the correct close_time
        // and market_name — preventing BasisExpiry from firing against the hourly market's

        // close time when the actual position lives in the daily/maker venue.
        // adoption_order comes from the registry — no hardcoded strategy list here.
        // Allow CLOB API and WS orderbook snapshots to settle before reconciling.
        // 5 s gives the WS subscribers spawned above time to receive at least one
        // book snapshot so token_bids contains real prices instead of the 0.50
        // fallback.  The fallback is still safe, but real bids produce a more
        // accurate discount heuristic when the DB/CSV entry lookup fails.
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Build bid slices from the WS receivers.  If the WS hasn't received its
        // first snapshot yet the bid is still dec!(0) — the reconcile helper's
        // `.filter(|b| *b > dec!(0))` gate will drop those and fall back to 0.50,
        // preserving the original behaviour while capturing real prices when possible.
        let hourly_token_bids: Vec<(U256, Decimal)> = if hourly_yes_token != U256::ZERO {
            vec![
                (hourly_yes_token, yes_price_rx.borrow().0),
                (hourly_no_token,  no_price_rx.borrow().0),
            ]
        } else {
            vec![]
        };

        let maker_token_bids: Vec<(U256, Decimal)> = match (&maker_yes_price_rx, &maker_no_price_rx, &maker_market_config) {
            (Some(yes_rx), Some(no_rx), Some(mk)) => vec![
                (mk.yes_token, yes_rx.borrow().0),
                (mk.no_token,  no_rx.borrow().0),
            ],
            _ => vec![],
        };

        // Reconcile for hourly market if it exists
        if hourly_yes_token != U256::ZERO {
            reconcile_orphaned_positions(
                &trading_client, &positions,
                &[(hourly_yes_token, "YES"), (hourly_no_token, "NO")],
                &hourly_market_name, hourly_market_close_time, &hourly_token_bids, &adoption_order,
            ).await;
        }
        // Reconcile for maker market if it exists
        if let Some(ref mk_config) = maker_market_config {
            reconcile_orphaned_positions(
                &trading_client, &positions,
                &[(mk_config.yes_token, "YES(maker)"), (mk_config.no_token, "NO(maker)")],
                &mk_config.market_name, mk_config.market_close_time, &maker_token_bids, &adoption_order,
            ).await;
        }

        // last_trade_time / last_stop_loss_time / last_expiry_exit_time are declared above the
        // outer loop so they survive market switches. Do NOT re-declare them here.
        let mut consecutive_failures: u32 = 0;
        let mut last_executor_summary = String::new(); // change-detection for  INFO tick summary
        let tg_token = env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default();
        let tg_chat_id = env::var("TELEGRAM_CHAT_ID").unwrap_or_default();
        // X (Twitter) credentials (optional — only used when ENABLE_X = true).
        let tw_api_key             = env::var("X_API_KEY").unwrap_or_default();
        let tw_api_secret          = env::var("X_API_SECRET").unwrap_or_default();
        let tw_access_token        = env::var("X_ACCESS_TOKEN").unwrap_or_default();
        let tw_access_token_secret = env::var("X_ACCESS_TOKEN_SECRET").unwrap_or_default();

        info!(" Orchestrator ready: {} strategies loaded", strategies.len());
        info!(" Strategy venue attachments:");
        let mut strategy_markets_map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        for strategy in &strategies {
            let sn = strategy.name();
            let venue = strategy.venue();
            let market_name_attached = match venue {
                "Hourly" => hourly_market_name.clone(),
                "Window/Daily" => maker_market_config.as_ref().map_or_else(String::new, |m| m.market_name.clone()),
                _ => String::from("Unknown"),
            };

            // Build the status key used by the UI (strip "Strategy" suffix, lowercase)
            let status_key = sn
                .strip_suffix("Strategy")
                .unwrap_or(&sn)
                .to_lowercase()
                .replace("timedecay", "time_decay");
            strategy_markets_map.insert(status_key, market_name_attached.clone());

            info!(
                "  - {} => venue={} | market=\"{}\" | budget=${} | risk={}",
                sn,
                venue,
                market_name_attached,
                strategy.max_exposure(),
                strategy.risk_model(),
            );
        }
        let _ = markets_tx.send(strategy_markets_map);

        loop {
            tokio::select! {
                _ = market_rx.changed() => {
                    // Destructure the 8-element MarketState tuple from the channel
                    let (
                        _new_hourly_yes_token,
                        _new_hourly_no_token,
                        _new_hourly_market_name,
                        _new_hourly_market_close_time,
                        _new_hourly_strike_price,
                        _new_hourly_desc,
                        new_maker_market_candidate,
                        new_hourly_condition_id,
                    ) = market_rx.borrow().clone();

                    let new_maker_cid = new_maker_market_candidate.as_ref().map_or_else(String::new, |m| m.condition_id.clone());

                    if new_hourly_condition_id == current_hourly_cid && new_maker_cid == current_maker_cid {
                        continue;
                    }
                    info!(" Market switch detected — restarting trading loop with new market context");
                    // Implement retry logic for cancel_all_orders
                    let mut cancel_success = false;
                    for i in 0..MAX_CANCEL_RETRIES {
                        let delay = BASE_CANCEL_RETRY_DELAY_MS * (1 << i); // Exponential backoff
                        match tokio::time::timeout(
                            Duration::from_secs(8), // Keep the 8-second timeout for each attempt
                            trading_client.as_ref().cancel_all_orders(),
                        ).await {
                            Ok(Ok(_)) => {
                                info!("✅ Successfully cancelled all orders after {} retries.", i);
                                cancel_success = true;
                                break;
                            },
                            Ok(Err(e)) => {
                                warn!("⚠️ Failed to cancel all orders (attempt {}/{}) with error: {}", i + 1, MAX_CANCEL_RETRIES, e);
                                if i < MAX_CANCEL_RETRIES - 1 {
                                    tokio::time::sleep(Duration::from_millis(delay)).await;
                                }
                            },
                            Err(_) => {
                                warn!("⚠️ cancel_all_orders timed out (8s) (attempt {}/{}) — retrying in {}ms", i + 1, MAX_CANCEL_RETRIES, delay);
                                if i < MAX_CANCEL_RETRIES - 1 {
                                    tokio::time::sleep(Duration::from_millis(delay)).await;
                                }
                            }
                        }
                    }

                    if !cancel_success {
                        error!("❌ Failed to cancel all orders after {} attempts. Proceeding with market switch, but orders may remain open.", MAX_CANCEL_RETRIES);
                    }

                    { phantom_cooldowns.lock().await.clear(); }
                    { pending_orders.lock().await.clear(); } // Clear pending locks on market switch
                    current_hourly_cid = new_hourly_condition_id;
                    current_maker_cid = new_maker_cid;
                    let _ = ws_cancel_tx.send(true); // Stop WS tasks for the old market before rotating
                    break;
                }
                _ = pulse_ticker.tick() => {
                    let start = Instant::now();
                    let mut req = BalanceAllowanceRequest::default();
                    req.asset_type = AssetType::Collateral;
                    // Hard 10s timeout — same fix as the 2026-05-01 overnight freeze (status_ticker arm).
                    // Without this, a TCP-level CLOB API stall blocks the entire select! loop,
                    // including the watchdog ticker, silently for as long as the OS connection timeout.
                    match tokio::time::timeout(Duration::from_secs(10), trading_client.balance_allowance(req)).await {
                        Ok(_) => {}
                        Err(_) => warn!("⚠️ Network Pulse: balance_allowance timed out (10s) — CLOB API stall suspected"),
                    }
                    info!(" Network Pulse: {:?}", start.elapsed());
                }
                _ = cleanup_ticker.tick() => {
                    // Wrap the entire cleanup arm in a 45s outer timeout.
                    // Belt-and-suspenders guard: even if an individual CLOB call inside
                    // reconcile_orphaned_positions gains a new unguarded .await in the future,
                    // the select! loop cannot block longer than 45s.
                    // (Individual calls already have their own 10s timeouts; this is the backstop.)
                    match tokio::time::timeout(Duration::from_secs(45), async {
                        // Cleanup for hourly market if it exists
                        if hourly_yes_token != U256::ZERO {
                            dradis::tasks::cleanup::cleanup_expired_positions(Arc::clone(&positions), hourly_market_name.clone(), hourly_yes_token, hourly_no_token, hourly_market_close_time).await;
                        }
                        // Cleanup for maker market if it exists
                        if let Some(ref mk_config) = maker_market_config {
                            dradis::tasks::cleanup::cleanup_expired_positions(Arc::clone(&positions), mk_config.market_name.clone(), mk_config.yes_token, mk_config.no_token, mk_config.market_close_time).await;
                        }

                        if let Err(e) = dradis::tasks::cleanup::reconcile_orphaned_positions(Arc::clone(&positions), &trading_client, &phantom_cooldowns, &tg_token, &tg_chat_id).await { warn!("⚠️ Orphan reconciliation error: {}", e); }
                        dradis::tasks::cleanup::cleanup_time_decay_positions(Arc::clone(&time_decay_positions)).await;
                        dradis::tasks::cleanup::sync_open_positions_with_chain(safe_address).await;

                        // Periodically clean up expired pending order locks
                        {
                            let mut pending = pending_orders.lock().await;
                            pending.retain(|_, &mut instant| instant > Instant::now());
                        }
                    }).await {
                        Ok(_) => {}
                        Err(_) => warn!("⚠️ cleanup_ticker arm timed out (45s) — CLOB/Data API stall suspected; select! loop unblocked"),
                    }
                }
                _ = settlement_ticker.tick() => {
                    match tokio::time::timeout(Duration::from_secs(60), async {
                        let settled = dradis::tasks::cleanup::auto_settle_closed_positions(
                            wallet_provider.clone(),
                            safe_address,
                            eoa_address,
                        ).await;

                        if settled {
                            // Keep Control Tower's open_positions mirror current right after settlement.
                            dradis::tasks::cleanup::sync_open_positions_with_chain(safe_address).await;
                        }
                    }).await {
                        Ok(_) => {}
                        Err(_) => warn!("⚠️ settlement_ticker arm timed out (60s) — skipping this cycle"),
                    }
                }
                _ = status_ticker.tick() => {
                    *last_heartbeat_at.lock().await = Instant::now();
                    let (yb, ybd, ya, yad, _) = *yes_price_rx.borrow();
                    let (nb, nbd, na, nad, _) = *no_price_rx.borrow();
                    // Compute OBI for heartbeat visibility so thresholds can be tuned empirically.
                    let yes_obi = if ybd + yad > dec!(0) { (ybd - yad) / (ybd + yad) } else { dec!(0) };
                    let no_obi  = if nbd + nad > dec!(0) { (nbd - nad) / (nbd + nad) } else { dec!(0) };
                    info!(" Heartbeat | Ask Sum ${:.4} (Y ask ${:.2} / N ask ${:.2}) | Bid Sum ${:.4} (Y bid ${:.2} / N bid ${:.2}) | Binance: ${:.2} | OBI Y={:.2} N={:.2}",
                        ya + na, ya, na, yb + nb, yb, nb, *oracle_rx.borrow(), yes_obi, no_obi);
                    // Refresh live pUSD balance so strategies can self-gate on insufficient funds.
                    // Root cause of the overnight freeze (2026-05-01): this balance_allowance call
                    // had no timeout. When the CLOB API stalled mid-request (the status_ticker arm
                    // had just logged  Heartbeat and then hit this .await), the entire select loop
                    // blocked — including the watchdog_ticker — and the bot went silent for 8+ hours.
                    // Fix: hard 10s timeout; on stall, skip the balance update for this tick.
                    let mut bal_req = BalanceAllowanceRequest::default();
                    bal_req.asset_type = AssetType::Collateral;
                    match tokio::time::timeout(Duration::from_secs(10), trading_client.balance_allowance(bal_req)).await {
                        Ok(Ok(resp)) => {
                            let bal = Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000);
                            *live_collateral.lock().await = bal;
                            debug!(" Live pUSD balance: ${:.4}", bal);
                            // Guard DB writes so a blocked SQLite call cannot stall status_ticker.
                            if let Some(pool) = db::pool() {
                                let pnl_snap = *total_pnl.lock().await;
                                if tokio::time::timeout(
                                    Duration::from_secs(3),
                                    db::record_pnl_snapshot(pool, pnl_snap, bal),
                                ).await.is_err() {
                                    warn!("⚠️ record_pnl_snapshot timed out (3s) — skipping this checkpoint");
                                }
                            }
                        }
                        Ok(Err(e)) => warn!("⚠️ balance_allowance error in status ticker: {}", e),
                        Err(_) => warn!("⚠️ balance_allowance timed out (10s) in status ticker — skipping balance update this tick"),
                    }
                }
                _ = ticker.tick() => {
                    if market_rx.has_changed().unwrap_or(false) { break; }
                    *last_heartbeat_at.lock().await = Instant::now();

                    // Get hourly market snapshot
                    let (hourly_yb, hourly_ybd, hourly_ya, hourly_yad, hourly_yes_ws_ts) = *yes_price_rx.borrow();
                    let (hourly_nb, hourly_nbd, hourly_na, hourly_nad, hourly_no_ws_ts) = *no_price_rx.borrow();
                    // Use the older of YES/NO WS update timestamps as the authoritative snapshot age.
                    // This is the WebSocket orderbook update time — NOT the tick heartbeat time.
                    // Previously `timestamp: Utc::now()` was set at tick time which caused
                    // GBOOST_MAX_SNAPSHOT_AGE_SECS to always read ~0s, never blocking entries
                    // on stale data (root cause of "entry_hb_age_sec=27, gate=10, fired anyway").
                    let hourly_snap_ts = hourly_yes_ws_ts.min(hourly_no_ws_ts);

                    // Get maker market snapshot if available
                    let (maker_yb, maker_ybd, maker_ya, maker_yad, maker_yes_ws_ts) = maker_yes_price_rx.as_ref().map_or((dec!(0), dec!(0), dec!(1), dec!(0), Utc::now()), |rx| *rx.borrow());
                    let (maker_nb, maker_nbd, maker_na, maker_nad, maker_no_ws_ts) = maker_no_price_rx.as_ref().map_or((dec!(0), dec!(0), dec!(1), dec!(0), Utc::now()), |rx| *rx.borrow());
                    let maker_snap_ts = maker_yes_ws_ts.min(maker_no_ws_ts);

                    // Only proceed if at least one market has valid prices
                    if (hourly_ya == dec!(1) && hourly_na == dec!(1)) && (maker_ya == dec!(1) && maker_na == dec!(1)) { continue; }

                    let hourly_market_config_for_ctx = MarketConfig {
                        yes_token: hourly_yes_token, no_token: hourly_no_token, market_name: hourly_market_name.clone(), market_close_time: hourly_market_close_time, strike_price: hourly_strike_price, is_neg_risk: hourly_is_neg_risk, condition_id: hourly_condition_id.clone(), yes_fee_bps: hourly_yes_fee_rate, no_fee_bps: hourly_no_fee_rate,
                    };

                    let maker_market_config_for_ctx = maker_market_config.clone();

                    // Snapshot the latest DynamicConfig once per tick — zero-cost Arc clone
                    let dyn_cfg = config_rx.borrow().clone();

                    let ctx = StrategyContext {
                        market: hourly_market_config_for_ctx.clone(), // Clone here to resolve the borrow of moved value error
                        snapshot: MarketSnapshot { // Snapshot for the hourly market
                            yes_bid: hourly_yb, yes_bid_depth: hourly_ybd, yes_ask: hourly_ya, yes_ask_depth: hourly_yad,
                            no_bid: hourly_nb, no_bid_depth: hourly_nbd, no_ask: hourly_na, no_ask_depth: hourly_nad,
                            oracle_price: *oracle_rx.borrow(),
                            velocity: velocity_rx.borrow().0,
                            velocity_1s: velocity_rx.borrow().1,
                            acceleration: velocity_rx.borrow().2,
                            funding_rate: *funding_rx.borrow(),
                            oracle_drift_60m: drift_rx.borrow().0,
                            oracle_drift_10m: drift_rx.borrow().1,
                            secs_to_expiry: hourly_market_close_time
                                .map(|t| (t - Utc::now()).num_seconds())
                                .unwrap_or(0),
                            timestamp: hourly_snap_ts, // WS orderbook update time (not tick time)
                        },
                        positions: Arc::clone(&positions),
                        session_pnl: *total_pnl.lock().await,
                        starting_collateral: *starting_collateral_store.lock().await,
                        available_collateral: *live_collateral.lock().await,
                        crypto_filter: crypto_filter.clone(),
                        market_started_at,
                        maker_market: maker_market_config_for_ctx, // This is the maker market
                        maker_snapshot: maker_market_config.as_ref().map(|mk| MarketSnapshot { // Snapshot for the maker market
                            yes_bid: maker_yb, yes_bid_depth: maker_ybd, yes_ask: maker_ya, yes_ask_depth: maker_yad,
                            no_bid: maker_nb, no_bid_depth: maker_nbd, no_ask: maker_na, no_ask_depth: maker_nad,
                            oracle_price: *oracle_rx.borrow(), velocity: velocity_rx.borrow().0, velocity_1s: velocity_rx.borrow().1, acceleration: velocity_rx.borrow().2,
                            funding_rate: *funding_rx.borrow(), oracle_drift_60m: drift_rx.borrow().0, oracle_drift_10m: drift_rx.borrow().1,
                            secs_to_expiry: mk.market_close_time
                                .map(|t| (t - Utc::now()).num_seconds())
                                .unwrap_or(0),
                            timestamp: maker_snap_ts, // WS orderbook update time (not tick time)
                        }),
                        dynamic_config: dyn_cfg,
                    };

                    let eval_result = match execute_strategies_concurrent(&strategies, &ctx, 500, &mut last_executor_summary).await {
                        Ok(r) => r,
                        Err(e) => { warn!("⚠️ Strategy evaluation error: {}", e); continue; }
                    };
                    let (resolved_signals, _) = aggregate_and_resolve_signals(&eval_result);
                    if resolved_signals.is_empty() { continue; }

                    for (strategy_name, signal) in resolved_signals {

                        let sn = strategy_name.clone();
                        // Determine which market context to use for this signal based on strategy's venue
                        let (target_yes_token, target_no_token, target_market_close_time, target_is_neg_risk, target_yes_fee_bps, target_no_fee_bps) = {
                            let strategy_venue = strategies.iter().find(|s| s.name() == sn).map(|s| s.venue()).unwrap_or("Hourly"); // Default to Hourly
                            if strategy_venue == "Window/Daily" && maker_market_config.is_some() {
                                let mk = maker_market_config.as_ref().unwrap();
                                (mk.yes_token, mk.no_token, mk.market_close_time, mk.is_neg_risk, mk.yes_fee_bps, mk.no_fee_bps)
                            } else {
                                (hourly_yes_token, hourly_no_token, hourly_market_close_time, hourly_is_neg_risk, hourly_yes_fee_rate, hourly_no_fee_rate)
                            }
                        };

                        match signal {
                            // ════════════════════ EXIT ════════════════════
                            StrategySignal::Exit { params, reason, exit_pair } => {
                                // Throttle exit retries to prevent log floods when FAK misses.
                                // The position stays in the map after a miss so evaluate_exit
                                // re-fires every heartbeat — without this guard that's ~20/s.
                                if let Some(lt) = last_exit_attempt_time.get(&sn) {
                                    if lt.elapsed() < Duration::from_secs(config::EXIT_RETRY_COOLDOWN_SECS) {
                                        continue;
                                    }
                                }
                                last_exit_attempt_time.insert(sn.clone(), Instant::now());
                                let tid = params.token_id;
                                let pos_key = (sn.clone(), tid);
                                let shares = { let map = positions.lock().await; match map.get(&pos_key) { Some(p) => p.shares, None => continue } };
                                if shares < config::MIN_ORDER_SHARES || params.price <= dec!(0) {
                                    let mut map = positions.lock().await; if let Some(p) = map.remove(&pos_key) { let aep = (params.price - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE); *total_pnl.lock().await += (aep - p.avg_entry) * p.shares; } continue;
                                }
                                info!(" EXIT [{}]: {} | shares={:.2}, bid=${:.4} | {}", sn, params.market_name, shares, params.price, reason);
                                let vc = if target_is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };

                                if !config::GHOST_MODE {
                                    if let Err(e) = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc, tid, Side::Sell, shares, (params.price - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE), target_yes_fee_bps as u16, params.order_type, params.post_only, 0, &shared_http).await { // Use target_yes_fee_bps for simplicity, assuming it's the correct fee for the token
                                        let es = e.to_string();
                                        if es.contains("not enough balance") || es.contains("balance: 0") || es.contains("invalid price") {
                                            let mut map = positions.lock().await; if let Some(p) = map.remove(&pos_key) { if p.fill_confirmed_at.is_some() { let aep3 = (params.price - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE); *total_pnl.lock().await += (aep3 - p.avg_entry) * p.shares; } }
                                            last_trade_time.insert(sn.clone(), Instant::now()); continue;
                                        }
                                        if es.contains("no orders found") {
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

                                {
                                    let re_m;
                                    let rs_m;
                                    let rc_m;
                                    let pnl_m;

                                    {
                                        let mut map = positions.lock().await;
                                        if let Some(p) = map.remove(&pos_key) {
                                            // Use actual sell price (bid - sell offset) for PnL to match real proceeds.
                                            let actual_exit_price = (params.price - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE);
                                            let pnl = (actual_exit_price - p.avg_entry) * p.shares;
                                            *total_pnl.lock().await += pnl;
                                            // Primary leg P&L logged below after paired leg is computed (combined log).

                                            re_m = p.avg_entry;
                                            rs_m = p.shares;
                                            rc_m = p.close_time;
                                            pnl_m = pnl;

                                            // Record trade metrics for both real and ghost mode (ghost trades
                                             // must appear in the Control Tower trades table for monitoring).
                                            {
                                                let sn_task = sn.clone();
                                                let m_name = params.market_name.clone();
                                                let sid = if tid == target_yes_token { "YES".to_string() } else { "NO".to_string() };
                                                let rp = actual_exit_price;
                                                let r_m = reason.clone();
                                                tokio::spawn(async move { metrics::record_trade(sn_task, m_name, sid, re_m, rp, rs_m, pnl_m, r_m).await; });
                                            }
                                            // Remove from open_positions on exit
                                            {
                                                let sn_close = sn.clone();
                                                let tid_close = tid.to_string();
                                                tokio::spawn(async move { if let Some(pool) = db::pool() { db::close_open_position(pool, &sn_close, &tid_close).await; } });
                                            }
                                        } else { continue; }
                                    }

                                    if rs_m > dec!(0) && !config::GHOST_MODE { // Only sync balance if not in ghost mode
                                        let ps = Arc::clone(&positions); let cl = Arc::clone(&trading_client); let tp = Arc::clone(&total_pnl); let m_name = params.market_name.clone();
                                        let sn_async = sn.clone();
                                        tokio::spawn(async move {
                                            tokio::time::sleep(Duration::from_millis(2500)).await;
                                            let mut req = BalanceAllowanceRequest::default(); req.asset_type = AssetType::Conditional; req.token_id = Some(tid);
                                            let rem = match cl.balance_allowance(req).await { Ok(r) => Decimal::from_str(&r.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000), Err(_) => return };
                                            if rem >= config::MIN_ORDER_SHARES {
                                                let fill = (rs_m - rem).max(dec!(0)); let aep2 = (params.price - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE); let pnlc = -((aep2 - re_m) * rem.min(rs_m)); *tp.lock().await += pnlc;
                                                // FAK filled 0: re-insert the FULL position so the exit retries next heartbeat.
                                                // Previously this logged and moved on, leaving shares orphaned on-chain — the
                                                // strategy would believe it was flat and open a new position on top of them.
                                                if fill < config::MIN_ORDER_SHARES {
                                                    warn!("⚠️ PARTIAL EXIT [{}]: FAK filled 0/{:.4} shares — re-inserting for retry.", sn_async, rs_m);
                                                    let mut map = ps.lock().await;
                                                    if !map.contains_key(&(sn_async.clone(), tid)) {
                                                        map.insert((sn_async.clone(), tid), Position { shares: rem, avg_entry: re_m, opened_at: Utc::now(), close_time: rc_m, market_name: m_name, pair_token_id: tid, fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: None });
                                                    }
                                                } else { warn!("⚠️ PARTIAL EXIT [{}]: sold {:.4}/{:.4} — re-inserting.", sn_async, fill, rs_m); let mut map = ps.lock().await; if !map.contains_key(&(sn_async.clone(), tid)) { map.insert((sn_async.clone(), tid), Position { shares: rem, avg_entry: re_m, opened_at: Utc::now(), close_time: rc_m, market_name: m_name, pair_token_id: tid, fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: None }); } }
                                            }
                                        });
                                    }
                                    // Accumulate paired leg P&L so we can log a single combined "Position closed" line.
                                    let mut paired_pnl = dec!(0);
                                    if exit_pair {
                                        let other_tid = if tid == target_yes_token { target_no_token } else { target_yes_token };
                                        let pk = (sn.clone(), other_tid); let ps = { let map = positions.lock().await; map.get(&pk).map(|p| p.shares) };
                                        if let Some(s) = ps {
                                            // Use the snapshot that matches the target market:
                                            //   - Hourly strategies (TimeDecay) target the hourly market → use ctx.snapshot
                                            //   - Window/Daily strategies (Arbitrage, Basis …) → use maker_snapshot
                                            // Previously always used maker_snapshot, which caused TimeDecay's NO-leg exit
                                            // to be priced against the daily market (~$0.16) instead of the hourly market
                                            // (~$0.69), inflating the paired-leg loss by ~$11 and tripping the drawdown guard.
                                            let exit_snap = if target_yes_token == ctx.market.yes_token {
                                                &ctx.snapshot  // hourly strategy — use hourly orderbook
                                            } else {
                                                ctx.maker_snapshot.as_ref().unwrap_or(&ctx.snapshot)
                                            };
                                            let other_bid = if other_tid == target_yes_token { exit_snap.yes_bid } else { exit_snap.no_bid };
                                            let other_fee_bps = if other_tid == target_yes_token { target_yes_fee_bps as u16 } else { target_no_fee_bps as u16 };
                                            let other_vc = if target_is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                            if !config::GHOST_MODE { let _ = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, other_vc, other_tid, Side::Sell, s, (other_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE), other_fee_bps, OrderType::FAK, false, 0, &shared_http).await; }
                                                let mut map = positions.lock().await; if let Some(p) = map.remove(&pk) { let actual_other_exit = (other_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE); let pnl = (actual_other_exit - p.avg_entry) * p.shares; paired_pnl = pnl; *total_pnl.lock().await += pnl;
                                                // Record paired-leg trade for both real and ghost mode
                                                {
                                                    let sn_pm = sn.clone(); let m_name = params.market_name.clone(); let sid = if other_tid == target_yes_token { "YES".to_string() } else { "NO".to_string() }; let p_avg = p.avg_entry; let o_bid = actual_other_exit; let p_shares = p.shares; let pn = pnl;
                                                    tokio::spawn(async move { metrics::record_trade(sn_pm, m_name, sid, p_avg, o_bid, p_shares, pn, "Convergence/PairedExit".to_string()).await; });
                                                }
                                                // Remove paired leg from open_positions
                                                {
                                                    let sn_cp = sn.clone();
                                                    let tid_cp = other_tid.to_string();
                                                    tokio::spawn(async move { if let Some(pool) = db::pool() { db::close_open_position(pool, &sn_cp, &tid_cp).await; } });
                                                }
                                            }
                                        }
                                    }
                                    // Log combined P&L (primary + paired). For single-leg exits paired_pnl == 0.
                                    info!(" Position closed [{}]: PnL ${:.4}", sn, pnl_m + paired_pnl);
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
                                    // Detached: Telegram latency must never block the trading select! loop.
                                    { let tok = tg_token.clone(); let cid = tg_chat_id.clone(); let msg = format!(" EXIT [{}] {} | bid=${:.4} | reason: {} | Session PnL: ${:.4}", sn, params.market_name, params.price, reason, *total_pnl.lock().await); tokio::spawn(async move { let _ = send_notification(&tok, &cid, &msg).await; }); }
                                    // Twitter/X — single combined trade recap on close.
                                    { let session_pnl = *total_pnl.lock().await; tweet_trade(tw_api_key.clone(), tw_api_secret.clone(), tw_access_token.clone(), tw_access_token_secret.clone(), sn.clone(), params.market_name.clone(), re_m, params.price, reason.clone(), pnl_m + paired_pnl, session_pnl); }
                                }
                            }

                            // ════════════════════ ENTRY ════════════════════
                            StrategySignal::Entry { params, pair_params } => {
                                if let Some(close_time) = target_market_close_time { if (close_time - Utc::now()).num_seconds() < config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY { continue; } }
                                if let Some(lt) = last_trade_time.get(&sn) { if lt.elapsed() < Duration::from_secs(config::TRADE_COOLDOWN_SECS as u64) { continue; } }
                                // Block re-entry for STOP_LOSS_COOLDOWN_SECS after any stop-loss (successful or FAK miss).
                                // Prevents compounding into a token where on-chain shares may still exist.
                                if let Some(lt) = last_stop_loss_time.get(&sn) { if lt.elapsed() < Duration::from_secs(config::STOP_LOSS_COOLDOWN_SECS) { continue; } }
                                // 5-minute block after an expiry exit — the market was closing, re-entry is pointless
                                if let Some(lt) = last_expiry_exit_time.get(&sn) { if lt.elapsed() < Duration::from_secs(300) { continue; } }

                                // Block re-entry if either leg token is still in phantom cooldown.
                                // phantom_cooldowns are set when sync_position_balance gives up on a token
                                // (order never confirmed on-chain) AND when orphan cleanup removes a
                                // half-filled paired position.  Without this check, the strategy fires a new
                                // entry immediately after cleanup — potentially buying on top of untracked
                                // on-chain shares from the failed leg.
                                {
                                    let cd = phantom_cooldowns.lock().await;
                                    let a_key = format!("{}:{}", sn, params.token_id);
                                    let a_on_cd = cd.get(&a_key)
                                        .map(|t| t.elapsed().as_secs() < dradis::helpers::balance::PHANTOM_COOLDOWN_SECS)
                                        .unwrap_or(false);
                                    let pair_on_cd = pair_params.as_ref().map(|pp| {
                                        let p_key = format!("{}:{}", sn, pp.token_id);
                                        cd.get(&p_key)
                                            .map(|t| t.elapsed().as_secs() < dradis::helpers::balance::PHANTOM_COOLDOWN_SECS)
                                            .unwrap_or(false)
                                    }).unwrap_or(false);
                                    if a_on_cd || pair_on_cd {
                                        debug!("⏳ ENTRY blocked by phantom cooldown [{}] — skipping tick", sn);
                                        continue;
                                    }
                                }

                                // ── Cross-position guard ─────────────────────────────────────────
                                // Prevent a directional strategy from opening both YES and NO legs
                                // in the same market simultaneously. This stops GBoost/Momentum
                                // from zigzagging (buy YES → buy NO → buy YES …) and creating a
                                // de-facto unintended arb that then spams the settlement scanner.
                                // Paired strategies (Arbitrage) are exempt because they insert BOTH
                                // legs atomically inside the `pair_params` branch further below.
                                if pair_params.is_none() {
                                    let pm = positions.lock().await;
                                    let other_token = if params.token_id == target_yes_token {
                                        target_no_token
                                    } else {
                                        target_yes_token
                                    };
                                    if pm.contains_key(&(sn.clone(), other_token)) {
                                        debug!("⏳ ENTRY blocked — already hold opposite leg in same market [{}] — must exit first", sn);
                                        continue;
                                    }
                                }

                                let pos_key = (sn.clone(), params.token_id);

                                // Debounce: check if an order is already pending for this token
                                {
                                    let pending = pending_orders.lock().await;
                                    if let Some(expiry) = pending.get(&pos_key) {
                                        if expiry > &Instant::now() { continue; }
                                    }
                                }

                                // Simulate position tracking in GHOST_MODE, or place real order otherwise
                                if config::GHOST_MODE {
                                    if positions.lock().await.contains_key(&pos_key) { continue; }

                                    // Use the target market's close_time
                                    let pos_close_time = target_market_close_time;
                                    // Store the actual fill price as avg_entry so P&L reflects true cost basis.
                                    // Maker GTC/post-only orders fill AT the posted bid price (no offset needed).
                                    // FAK taker orders need +BUY_PRICE_OFFSET to model the aggressive fill premium.
                                    let actual_entry_price = if params.post_only {
                                        params.price  // maker: fills at exactly the posted bid
                                    } else {
                                        (params.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE)
                                    };
                                    positions.lock().await.insert(pos_key.clone(), Position { shares: params.shares, avg_entry: actual_entry_price, opened_at: Utc::now(), close_time: pos_close_time, market_name: params.market_name.clone(), pair_token_id: params.token_id, fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: pair_params.as_ref().map(|p| p.token_id) });

                                    let side_g = if params.token_id == target_yes_token { "YES" } else { "NO" };
                                    info!(" GHOST_MODE ENTRY {} [{}]: {} | ${:.4} x {:.1} (simulated)",
                                        side_g, sn, params.market_name, params.price, params.shares);
                                    // Record ghost entries to DB — so the UI activity log and LLM advisor
                                    // can see in-flight positions before they close as completed trades.
                                    {
                                        let side_g = if params.token_id == target_yes_token { "YES" } else { "NO" };
                                        let sn_g = sn.clone(); let tid_g = params.token_id.to_string(); let mn_g = params.market_name.clone(); let side_gs = side_g.to_string(); let ep_g = actual_entry_price; let sh_g = params.shares;
                                        tokio::spawn(async move { metrics::record_entry(sn_g, tid_g, mn_g, side_gs, ep_g, sh_g).await; });
                                    }
                                    if let Some(pool) = db::pool() {
                                        let side_g = if params.token_id == target_yes_token { "YES" } else { "NO" };
                                        db::record_open_position(pool, &sn, &params.token_id.to_string(), &params.market_name, side_g, actual_entry_price, params.shares, true).await;
                                    }
                                    if let Some(pp) = pair_params {
                                        let pp_close_time = target_market_close_time;
                                        let actual_paired_entry_price = if pp.post_only {
                                            pp.price
                                        } else {
                                            (pp.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE)
                                        };
                                        positions.lock().await.insert((sn.clone(), pp.token_id), Position { shares: pp.shares, avg_entry: actual_paired_entry_price, opened_at: Utc::now(), close_time: pp_close_time, market_name: pp.market_name.clone(), pair_token_id: pp.token_id, fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: Some(params.token_id) });
                                        let side_gp = if pp.token_id == target_yes_token { "YES" } else { "NO" };
                                        info!(" GHOST_MODE ENTRY {} (paired) [{}]: {} | ${:.4} x {:.1} (simulated)", side_gp, sn, pp.market_name, pp.price, pp.shares);
                                        // Record paired ghost entry to DB
                                        {
                                            let side_gp = if pp.token_id == target_yes_token { "YES" } else { "NO" };
                                            let sn_gp = sn.clone(); let tid_gp = pp.token_id.to_string(); let mn_gp = pp.market_name.clone(); let side_gps = side_gp.to_string(); let ep_gp = actual_paired_entry_price; let sh_gp = pp.shares;
                                            tokio::spawn(async move { metrics::record_entry(sn_gp, tid_gp, mn_gp, side_gps, ep_gp, sh_gp).await; });
                                        }
                                        if let Some(pool) = db::pool() {
                                            let side_gp = if pp.token_id == target_yes_token { "YES" } else { "NO" };
                                            db::record_open_position(pool, &sn, &pp.token_id.to_string(), &pp.market_name, side_gp, actual_paired_entry_price, pp.shares, true).await;
                                        }
                                    }
                                    last_trade_time.insert(sn.clone(), Instant::now());
                                    // No actual order placement or balance sync in ghost mode
                                } else {
                                    // Real trading logic
                                    // Maker GTC/post-only orders must NOT have BUY_PRICE_OFFSET applied —
                                    // adding +$0.01 to yes_bid pushes the price above the current ask,
                                    // causing every post-only GTC order to be rejected with
                                    // "invalid post-only order: order crosses book".
                                    // BUY_PRICE_OFFSET is only appropriate for FAK/taker orders.
                                    let actual_entry_price = if params.post_only {
                                        params.price  // maker: post at exactly the bid — never cross
                                    } else {
                                        (params.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE)
                                    };
                                    {
                                        let mut map = positions.lock().await; if map.contains_key(&pos_key) { continue; }
                                        let pos_close_time = target_market_close_time;
                                        map.insert(pos_key.clone(), Position { shares: params.shares, avg_entry: actual_entry_price, opened_at: Utc::now(), close_time: pos_close_time, market_name: params.market_name.clone(), pair_token_id: params.token_id, fill_confirmed_at: None, paired_leg_token_id: pair_params.as_ref().map(|p| p.token_id) });
                                    }

                                    // Set pending lock for 3 seconds
                                    {
                                        pending_orders.lock().await.insert(pos_key.clone(), Instant::now() + Duration::from_secs(3));
                                    }

                                    info!(" ENTRY [{}]: {} | ${:.4} x {:.1}", sn, params.market_name, params.price, params.shares);
                                    // Snapshot on-chain balance BEFORE placing the order so that
                                    // sync_position_balance can subtract it as a baseline.  Without
                                    // this, a pre-existing position in the same token held by a
                                    // different strategy (e.g. Momentum holding YES on the daily)
                                    // would be counted as a fill for this new order, causing a false
                                    // confirmation and masking a genuine no-fill situation.
                                    let primary_baseline = {
                                        let mut req = BalanceAllowanceRequest::default();
                                        req.asset_type = AssetType::Conditional;
                                        req.token_id = Some(params.token_id);
                                        match tokio::time::timeout(Duration::from_secs(10), trading_client.balance_allowance(req)).await {
                                            Ok(Ok(resp)) => Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000),
                                            Ok(Err(e)) => {
                                                warn!("⚠️ entry baseline balance_allowance error [{}]: {}", sn, e);
                                                dec!(0)
                                            }
                                            Err(_) => {
                                                warn!("⚠️ entry baseline balance_allowance timed out (10s) [{}]", sn);
                                                dec!(0)
                                            }
                                        }
                                    };
                                    let vc = if target_is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };

                                    if let Some(pp) = pair_params {
                                        // ── Atomic two-leg placement ─────────────────────────────────────
                                        // Both legs are submitted in a single POST /orders request.
                                        // Polymarket processes the batch atomically: either both orders
                                        // reach the book or neither does — no partial state, no orphans,
                                        // no cancel/flash-exit safety net needed.
                                        let actual_pair_entry_price = if pp.post_only {
                                            pp.price
                                        } else {
                                            (pp.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE)
                                        };
                                        let vc_p = if pp.is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };

                                        let pair_baseline = {
                                            let mut req = BalanceAllowanceRequest::default();
                                            req.asset_type = AssetType::Conditional;
                                            req.token_id = Some(pp.token_id);
                                            match tokio::time::timeout(Duration::from_secs(10), trading_client.balance_allowance(req)).await {
                                                Ok(Ok(resp)) => Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000),
                                                Ok(Err(e)) => {
                                                    warn!("⚠️ pair baseline balance_allowance error [{}]: {}", sn, e);
                                                    dec!(0)
                                                }
                                                Err(_) => {
                                                    warn!("⚠️ pair baseline balance_allowance timed out (10s) [{}]", sn);
                                                    dec!(0)
                                                }
                                            }
                                        };

                                        // ── Orphan accumulation guard ─────────────────────────────────────
                                        // For GTC paired strategies (ArbitrageStrategy), legs fill at very
                                        // different rates on directional markets.  If the NO leg fills quickly
                                        // but the YES leg sits unfilled for >MAX_WAIT_SECS, sync_position_balance
                                        // cancels YES and phantom-removes it.  The NO shares stay in the wallet
                                        // as untracked orphans.  Without this guard, new arb entries keep firing
                                        // (the exposure check only sees the position map, not on-chain reality),
                                        // and each cycle adds 10 more NO shares — the root cause of the
                                        // "9 YES vs 40 NO" imbalance observed on 2026-05-19.
                                        //
                                        // Fix: if EITHER leg already has an on-chain balance > MIN_ORDER_SHARES,
                                        // it is an orphaned fill from a previous cycle.  Block entry until the
                                        // cleanup cycle sells or untrack the orphan AND the phantom cooldown clears.
                                        if primary_baseline >= config::MIN_ORDER_SHARES || pair_baseline >= config::MIN_ORDER_SHARES {
                                            warn!(" Paired entry BLOCKED [{}]: orphan accumulation guard — \
                                                   primary on-chain={:.4} pair on-chain={:.4} for \"{}\" \
                                                   (re-checking in {}s)",
                                                sn, primary_baseline, pair_baseline, params.market_name,
                                                dradis::helpers::balance::PHANTOM_COOLDOWN_SECS);
                                            positions.lock().await.remove(&pos_key);
                                            pending_orders.lock().await.remove(&pos_key);
                                            // Set a full phantom_cooldown on BOTH legs so the strategy
                                            // backs off for PHANTOM_COOLDOWN_SECS rather than retrying
                                            // every minute.  Without this, the guard fires ~60× per hour
                                            // for the rest of the day whenever an unresolved orphaned
                                            // on-chain leg (e.g. a YES fill whose NO leg never arrived)
                                            // persists until market close — burning API budget and
                                            // flooding the log with identical WARN lines.
                                            {
                                                let mut cd = phantom_cooldowns.lock().await;
                                                cd.insert(format!("{}:{}", sn, params.token_id), tokio::time::Instant::now());
                                                cd.insert(format!("{}:{}", sn, pp.token_id), tokio::time::Instant::now());
                                            }
                                            last_trade_time.insert(sn.clone(), Instant::now());
                                            continue;
                                        }

                                        match place_limit_orders_atomic(
                                            &trading_client, &nonce_manager, &signer,
                                            safe_address, eoa_address,
                                            // Leg A (primary)
                                            vc, params.token_id, Side::Buy,
                                            params.shares, actual_entry_price,
                                            params.order_type.clone(), params.post_only, 0,
                                            // Leg B (pair)
                                            vc_p, pp.token_id, Side::Buy,
                                            pp.shares, actual_pair_entry_price,
                                            pp.order_type.clone(), pp.post_only, 0,
                                            &shared_http,
                                        ).await {
                                            Err(e) => {
                                                // Batch was rejected atomically — neither leg is live.
                                                warn!("⚠️ Atomic arb entry FAILED [{}]: {} — no orders placed, no cleanup needed", sn, e);
                                                positions.lock().await.remove(&pos_key);
                                                pending_orders.lock().await.remove(&pos_key);
                                                last_trade_time.insert(sn.clone(), Instant::now());
                                                consecutive_failures += 1; continue;
                                            }
                                            Ok((_leg_a_id, _leg_b_id)) => {
                                                // Both legs are on the book. Spawn fill-confirmation watchers.
                                                let primary_wait_secs = if target_yes_token == hourly_yes_token {
                                                    dradis::helpers::balance::MAX_WAIT_SECS_HOURLY
                                                } else {
                                                    dradis::helpers::balance::MAX_WAIT_SECS_WINDOW
                                                };
                                                // Leg A fill-confirmation watcher.
                                                // record_open_position is intentionally written AFTER
                                                // on-chain fill is confirmed (Ok return) — NOT at order
                                                // placement time — so the DB only ever holds real positions.
                                                let cl_s = Arc::clone(&trading_client); let ps_s = Arc::clone(&positions); let pc_s = Arc::clone(&phantom_cooldowns); let sn_s = sn.clone(); let tn_s = params.token_id;
                                                let db_sn_a = sn.clone(); let db_tid_a = params.token_id.to_string(); let db_mn_a = params.market_name.clone();
                                                let db_side_a = if params.token_id == target_yes_token { "YES" } else { "NO" }; let db_ep_a = actual_entry_price; let db_sh_a = params.shares;
                                                tokio::spawn(async move {
                                                    if sync_position_balance(&cl_s, &ps_s, &sn_s, tn_s, Some(&pc_s), primary_baseline, primary_wait_secs).await.is_ok() {
                                                        if let Some(pool) = db::pool() {
                                                            db::record_open_position(pool, &db_sn_a, &db_tid_a, &db_mn_a, db_side_a, db_ep_a, db_sh_a, false).await;
                                                        }
                                                    }
                                                });

                                                let pp_close_time = target_market_close_time;
                                                positions.lock().await.insert((sn.clone(), pp.token_id), Position { shares: pp.shares, avg_entry: actual_pair_entry_price, opened_at: Utc::now(), close_time: pp_close_time, market_name: pp.market_name.clone(), pair_token_id: pp.token_id, fill_confirmed_at: None, paired_leg_token_id: Some(params.token_id) });

                                                let pair_wait_secs = if pp.token_id == hourly_yes_token || pp.token_id == hourly_no_token {
                                                    dradis::helpers::balance::MAX_WAIT_SECS_HOURLY
                                                } else {
                                                    dradis::helpers::balance::MAX_WAIT_SECS_WINDOW
                                                };
                                                // Leg B fill-confirmation watcher (same confirmed-fill-only DB write pattern).
                                                let sn_p = sn.clone(); let tn_p = pp.token_id; let ps_p = Arc::clone(&positions); let cl_p = Arc::clone(&trading_client); let pc_p = Arc::clone(&phantom_cooldowns);
                                                let db_sn_b = sn.clone(); let db_tid_b = pp.token_id.to_string(); let db_mn_b = pp.market_name.clone();
                                                let db_side_b = if pp.token_id == target_yes_token { "YES" } else { "NO" }; let db_ep_b = actual_pair_entry_price; let db_sh_b = pp.shares;
                                                tokio::spawn(async move {
                                                    if sync_position_balance(&cl_p, &ps_p, &sn_p, tn_p, Some(&pc_p), pair_baseline, pair_wait_secs).await.is_ok() {
                                                        if let Some(pool) = db::pool() {
                                                            db::record_open_position(pool, &db_sn_b, &db_tid_b, &db_mn_b, db_side_b, db_ep_b, db_sh_b, false).await;
                                                        }
                                                    }
                                                });

                                                // Metrics entries (record_entry at order-placement time is fine —
                                                // entries table is for price-lookup history, not live-position display).
                                                { let sn_e = sn.clone(); let tid_e = params.token_id.to_string(); let mn_e = params.market_name.clone(); let side_e = if params.token_id == target_yes_token { "YES" } else { "NO" }.to_string(); let ep_e = actual_entry_price; let sh_e = params.shares; tokio::spawn(async move { metrics::record_entry(sn_e, tid_e, mn_e, side_e, ep_e, sh_e).await; }); }
                                                { let sn_eb = sn.clone(); let tid_eb = pp.token_id.to_string(); let mn_eb = pp.market_name.clone(); let side_eb = if pp.token_id == target_yes_token { "YES" } else { "NO" }.to_string(); let ep_eb = actual_pair_entry_price; let sh_eb = pp.shares; tokio::spawn(async move { metrics::record_entry(sn_eb, tid_eb, mn_eb, side_eb, ep_eb, sh_eb).await; }); }
                                            }
                                        }
                                    } else {
                                        // ── Single-leg placement (non-paired strategies) ─────────────────
                                        let leg_a_order_id = match place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc, params.token_id, Side::Buy, params.shares, actual_entry_price, target_yes_fee_bps as u16, params.order_type, params.post_only, 0, &shared_http).await {
                                            Err(e) => {
                                                warn!("⚠️ ENTRY order failed [{}]: {}", sn, e);
                                                positions.lock().await.remove(&pos_key);
                                                pending_orders.lock().await.remove(&pos_key);
                                                last_trade_time.insert(sn.clone(), Instant::now());
                                                consecutive_failures += 1; continue;
                                            }
                                            Ok(id) => id,
                                        };
                                        let _ = leg_a_order_id; // order ID available for future cancel use
                                        // Single-leg fill-confirmation watcher — DB write on confirmed fill only.
                                        let cl_s = Arc::clone(&trading_client); let ps_s = Arc::clone(&positions); let pc_s = Arc::clone(&phantom_cooldowns); let sn_s = sn.clone(); let tn_s = params.token_id;
                                        let primary_wait_secs = if target_yes_token == hourly_yes_token {
                                            dradis::helpers::balance::MAX_WAIT_SECS_HOURLY
                                        } else {
                                            dradis::helpers::balance::MAX_WAIT_SECS_WINDOW
                                        };
                                        let db_sn_s = sn.clone(); let db_tid_s = params.token_id.to_string(); let db_mn_s = params.market_name.clone();
                                        let db_side_s = if params.token_id == target_yes_token { "YES" } else { "NO" }; let db_ep_s = actual_entry_price; let db_sh_s = params.shares;
                                        tokio::spawn(async move {
                                            if sync_position_balance(&cl_s, &ps_s, &sn_s, tn_s, Some(&pc_s), primary_baseline, primary_wait_secs).await.is_ok() {
                                                if let Some(pool) = db::pool() {
                                                    db::record_open_position(pool, &db_sn_s, &db_tid_s, &db_mn_s, db_side_s, db_ep_s, db_sh_s, false).await;
                                                }
                                            }
                                        });

                                        { let sn_e = sn.clone(); let tid_e = params.token_id.to_string(); let mn_e = params.market_name.clone(); let side_e = if params.token_id == target_yes_token { "YES" } else { "NO" }.to_string(); let ep_e = actual_entry_price; let sh_e = params.shares; tokio::spawn(async move { metrics::record_entry(sn_e, tid_e, mn_e, side_e, ep_e, sh_e).await; }); }
                                    }
                                    last_trade_time.insert(sn.clone(), Instant::now());
                                    { let tok = tg_token.clone(); let cid = tg_chat_id.clone(); let msg = format!(" ENTRY [{}] {} | ${:.4} x {:.1}", sn, params.market_name, params.price, params.shares); tokio::spawn(async move { let _ = send_notification(&tok, &cid, &msg).await; }); }
                                }
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

                                    if config::GHOST_MODE {
                                        if positions.lock().await.contains_key(&pk) { continue; }
                                        positions.lock().await.insert(pk.clone(), Position { shares: p.shares, avg_entry: p.price, opened_at: Utc::now(), close_time: None, market_name: p.market_name.clone(), pair_token_id: p.token_id, fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: None });
                                        info!(" GHOST_MODE MakerQuote [{}]: {} | shares={:.2}, bid=${:.4} (simulated)", sn, p.market_name, p.shares, p.price);
                                        placed = true;
                                    } else {
                                        // Real trading logic
                                        if !positions.lock().await.contains_key(&pk) {
                                            info!(" MakerQuote [{}]: {} | shares={:.2}, bid=${:.4}", sn, p.market_name, p.shares, p.price);
                                            positions.lock().await.insert(pk.clone(), Position { shares: p.shares, avg_entry: p.price, opened_at: Utc::now(), close_time: None, market_name: p.market_name.clone(), pair_token_id: p.token_id, fill_confirmed_at: None, paired_leg_token_id: None });

                                            // Set pending lock for 3 seconds
                                            {
                                                pending_orders.lock().await.insert(pk.clone(), Instant::now() + Duration::from_secs(3));
                                            }

                                            let _ = tokio::time::timeout(
                                                Duration::from_secs(10),
                                                dradis::helpers::balance::quick_confirm_fill(&trading_client, &sn, p.token_id, &positions, &p.condition_id, p.order_type.clone()),
                                            ).await;
                                            let vc = if p.is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                            if let Err(e) = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc, p.token_id, Side::Buy, p.shares, p.price, target_yes_fee_bps as u16, p.order_type, true, 0, &shared_http).await { // Use target_yes_fee_bps for simplicity
                                                positions.lock().await.remove(&pk);
                                                pending_orders.lock().await.remove(&pk);
                                                if !e.to_string().contains("crosses book") { consecutive_failures += 1; } continue;
                                            }
                                            let cl_m = Arc::clone(&trading_client); let ps_m = Arc::clone(&positions); let pc_m = Arc::clone(&phantom_cooldowns);
                                            let sn_m = sn.clone();
                                            tokio::spawn(async move { let _ = sync_position_balance(&cl_m, &ps_m, &sn_m, p.token_id, Some(&pc_m), dec!(0), dradis::helpers::balance::MAX_WAIT_SECS_WINDOW).await; });
                                            { let sn_em = sn.clone(); let tid_em = p.token_id.to_string(); let mn_em = p.market_name.clone(); let side_em = if p.token_id == target_yes_token { "YES" } else { "NO" }.to_string(); let ep_em = p.price; let sh_em = p.shares; tokio::spawn(async move { metrics::record_entry(sn_em, tid_em, mn_em, side_em, ep_em, sh_em).await; }); }
                                        }
                                        placed = true;
                                    }
                                }
                                if placed { last_trade_time.insert(sn.clone(), Instant::now()); }
                                if consecutive_failures >= config::MAX_CONSECUTIVE_FAILURES { error!(" Circuit breaker hit!"); tokio::time::sleep(Duration::from_secs(60)).await; consecutive_failures = 0; }
                            }
                            StrategySignal::NoSignal => {}
                        }
                    }
                }
                _ = watchdog_ticker.tick() => {
                    // If the strategy ticker hasn't fired in LOOP_WATCHDOG_SECS, the inner loop
                    // may be stuck on a blocking .await or a stalled tokio task. Force a break so
                    // the outer loop restarts the trading context with a fresh market.
                    let elapsed = last_heartbeat_at.lock().await.elapsed().as_secs();
                    if elapsed > LOOP_WATCHDOG_SECS {
                        error!(" WATCHDOG: inner loop silent for {}s — forcing restart", elapsed);
                        let _ = ws_cancel_tx.send(true); // Release WS tasks before restarting
                        break;
                    }
                }
            }
        }
    }
}
