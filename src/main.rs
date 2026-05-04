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

// Import MarketState type from market_monitor
use dradis::tasks::market_monitor::MarketState;


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
        info!("🧪 No initial hourly market found. Waiting for market monitor to find one.");
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
        info!("🧪 No initial maker market found. Waiting for market monitor to find one.");
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

            info!("🚀 Starting Orchestrated Trading on hourly market: \"{}\"", hourly_market_name);

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

        let (yes_price_tx, yes_price_rx) = watch::channel::<PriceState>((dec!(0), dec!(0), dec!(1), dec!(0)));
        let (no_price_tx, no_price_rx) = watch::channel::<PriceState>((dec!(0), dec!(0), dec!(1), dec!(0)));

        // Only subscribe to hourly market WS if an hourly market is present
        if hourly_yes_token != U256::ZERO {
            for (token, tx) in [(hourly_yes_token, yes_price_tx.clone()), (hourly_no_token, no_price_tx.clone())] {
                tokio::spawn(async move {
                    loop {
                        let client = WsClient::default();
                        let stream = match client.subscribe_orderbook(vec![token]) {
                            Ok(s) => s,
                            Err(e) => {
                                warn!("⚠️ WS subscribe failed for hourly token {}: {}. Retrying in 5s...", token, e);
                                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                continue;
                            }
                        };
                        let mut stream = Box::pin(stream);
                        info!("✅ WS orderbook subscribed for hourly token {}", token);
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
                                warn!("⚠️ WS stream error for hourly token {}. Restarting...", token);
                                break;
                            }
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                });
            }
        }

        let (maker_yes_price_rx, maker_no_price_rx) = if let Some(ref mk) = maker_market_candidate_from_channel {
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
        tokio::time::sleep(Duration::from_secs(2)).await; // allow CLOB API to be ready

        // Reconcile for hourly market if it exists
        if hourly_yes_token != U256::ZERO {
            reconcile_orphaned_positions(
                &trading_client, &positions,
                &[(hourly_yes_token, "YES"), (hourly_no_token, "NO")],
                &hourly_market_name, hourly_market_close_time, &[], &adoption_order,
            ).await;
        }
        // Reconcile for maker market if it exists
        if let Some(ref mk_config) = maker_market_config {
            reconcile_orphaned_positions(
                &trading_client, &positions,
                &[(mk_config.yes_token, "YES(maker)"), (mk_config.no_token, "NO(maker)")],
                &mk_config.market_name, mk_config.market_close_time, &[], &adoption_order,
            ).await;
        }

        // last_trade_time / last_stop_loss_time / last_expiry_exit_time are declared above the
        // outer loop so they survive market switches. Do NOT re-declare them here.
        let mut consecutive_failures: u32 = 0;
        let mut last_executor_summary = String::new(); // change-detection for 📊 INFO tick summary
        let tg_token = env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default();
        let tg_chat_id = env::var("TELEGRAM_CHAT_ID").unwrap_or_default();

        info!("🤖 Orchestrator ready: {} strategies loaded", strategies.len());
        info!("🧭 Strategy venue attachments:");
        for strategy in &strategies {
            let sn = strategy.name();
            let venue = strategy.venue();
            let market_name_attached = match venue {
                "Hourly" => hourly_market_name.clone(),
                "Window/Daily" => maker_market_config.as_ref().map_or_else(String::new, |m| m.market_name.clone()),
                _ => String::from("Unknown"),
            };

            info!(
                "  - {} => venue={} | market=\"{}\" | budget=${} | risk={}",
                sn,
                venue,
                market_name_attached,
                strategy.max_exposure(),
                strategy.risk_model(),
            );
        }

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
                    info!("🔄 Market switch detected — restarting trading loop with new market context");
                    // Timeout the cancel so a stalled CLOB response cannot block the switch.
                    match tokio::time::timeout(
                        Duration::from_secs(8),
                        trading_client.as_ref().cancel_all_orders(),
                    ).await {
                        Ok(Err(e)) => warn!("⚠️ Failed to cancel all orders: {}", e),
                        Err(_)     => warn!("⚠️ cancel_all_orders timed out (8s) — proceeding with market switch"),
                        Ok(Ok(_))  => {}
                    }

                    { phantom_cooldowns.lock().await.clear(); }
                    { pending_orders.lock().await.clear(); } // Clear pending locks on market switch
                    current_hourly_cid = new_hourly_condition_id;
                    current_maker_cid = new_maker_cid;
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
                    // Cleanup for hourly market if it exists
                    if hourly_yes_token != U256::ZERO {
                        dradis::tasks::cleanup::cleanup_expired_positions(Arc::clone(&positions), hourly_market_name.clone(), hourly_yes_token, hourly_no_token, hourly_market_close_time).await;
                    }
                    // Cleanup for maker market if it exists
                    if let Some(ref mk_config) = maker_market_config {
                        dradis::tasks::cleanup::cleanup_expired_positions(Arc::clone(&positions), mk_config.market_name.clone(), mk_config.yes_token, mk_config.no_token, mk_config.market_close_time).await;
                    }

                    if let Err(e) = dradis::tasks::cleanup::reconcile_orphaned_positions(Arc::clone(&positions), &tg_token, &tg_chat_id).await { warn!("⚠️ Orphan reconciliation error: {}", e); }
                    dradis::tasks::cleanup::cleanup_time_decay_positions(Arc::clone(&time_decay_positions)).await;

                    // Periodically clean up expired pending order locks
                    {
                        let mut pending = pending_orders.lock().await;
                        pending.retain(|_, &mut instant| instant > Instant::now());
                    }
                }
                _ = status_ticker.tick() => {
                    *last_heartbeat_at.lock().await = Instant::now();
                    let (yb, ybd, ya, yad) = *yes_price_rx.borrow();
                    let (nb, nbd, na, nad) = *no_price_rx.borrow();
                    // Compute OBI for heartbeat visibility so thresholds can be tuned empirically.
                    let yes_obi = if ybd + yad > dec!(0) { (ybd - yad) / (ybd + yad) } else { dec!(0) };
                    let no_obi  = if nbd + nad > dec!(0) { (nbd - nad) / (nbd + nad) } else { dec!(0) };
                    info!("💓 Heartbeat | Ask Sum ${:.4} (Y ask ${:.2} / N ask ${:.2}) | Bid Sum ${:.4} (Y bid ${:.2} / N bid ${:.2}) | Binance: ${:.2} | OBI Y={:.2} N={:.2}",
                        ya + na, ya, na, yb + nb, yb, nb, *oracle_rx.borrow(), yes_obi, no_obi);
                    // Refresh live pUSD balance so strategies can self-gate on insufficient funds.
                    // Root cause of the overnight freeze (2026-05-01): this balance_allowance call
                    // had no timeout. When the CLOB API stalled mid-request (the status_ticker arm
                    // had just logged 💓 Heartbeat and then hit this .await), the entire select loop
                    // blocked — including the watchdog_ticker — and the bot went silent for 8+ hours.
                    // Fix: hard 10s timeout; on stall, skip the balance update for this tick.
                    let mut bal_req = BalanceAllowanceRequest::default();
                    bal_req.asset_type = AssetType::Collateral;
                    match tokio::time::timeout(Duration::from_secs(10), trading_client.balance_allowance(bal_req)).await {
                        Ok(Ok(resp)) => {
                            let bal = Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000);
                            *live_collateral.lock().await = bal;
                            debug!("💰 Live pUSD balance: ${:.4}", bal);
                        }
                        Ok(Err(e)) => warn!("⚠️ balance_allowance error in status ticker: {}", e),
                        Err(_) => warn!("⚠️ balance_allowance timed out (10s) in status ticker — skipping balance update this tick"),
                    }
                }
                _ = ticker.tick() => {
                    if market_rx.has_changed().unwrap_or(false) { break; }
                    *last_heartbeat_at.lock().await = Instant::now();

                    // Get hourly market snapshot
                    let (hourly_yb, hourly_ybd, hourly_ya, hourly_yad) = *yes_price_rx.borrow();
                    let (hourly_nb, hourly_nbd, hourly_na, hourly_nad) = *no_price_rx.borrow();

                    // Get maker market snapshot if available
                    let (maker_yb, maker_ybd, maker_ya, maker_yad) = maker_yes_price_rx.as_ref().map_or((dec!(0), dec!(0), dec!(1), dec!(0)), |rx| *rx.borrow());
                    let (maker_nb, maker_nbd, maker_na, maker_nad) = maker_no_price_rx.as_ref().map_or((dec!(0), dec!(0), dec!(1), dec!(0)), |rx| *rx.borrow());

                    // Only proceed if at least one market has valid prices
                    if (hourly_ya == dec!(1) && hourly_na == dec!(1)) && (maker_ya == dec!(1) && maker_na == dec!(1)) { continue; }

                    let hourly_market_config_for_ctx = MarketConfig {
                        yes_token: hourly_yes_token, no_token: hourly_no_token, market_name: hourly_market_name.clone(), market_close_time: hourly_market_close_time, strike_price: hourly_strike_price, is_neg_risk: hourly_is_neg_risk, condition_id: hourly_condition_id.clone(), yes_fee_bps: hourly_yes_fee_rate, no_fee_bps: hourly_no_fee_rate,
                    };

                    let maker_market_config_for_ctx = maker_market_config.clone();

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
                            oracle_drift_60m: *drift_60m_rx.borrow(),
                            timestamp: Utc::now(),
                        },
                        positions: Arc::clone(&positions),
                        session_pnl: *total_pnl.lock().await,
                        starting_collateral: *starting_collateral_store.lock().await,
                        available_collateral: *live_collateral.lock().await,
                        crypto_filter: crypto_filter.clone(),
                        market_started_at,
                        maker_market: maker_market_config_for_ctx, // This is the maker market
                        maker_snapshot: maker_market_config.as_ref().map(|_| MarketSnapshot { // Snapshot for the maker market
                            yes_bid: maker_yb, yes_bid_depth: maker_ybd, yes_ask: maker_ya, yes_ask_depth: maker_yad,
                            no_bid: maker_nb, no_bid_depth: maker_nbd, no_ask: maker_na, no_ask_depth: maker_nad,
                            oracle_price: *oracle_rx.borrow(), velocity: velocity_rx.borrow().0, velocity_1s: velocity_rx.borrow().1, acceleration: velocity_rx.borrow().2,
                            funding_rate: *funding_rx.borrow(), oracle_drift_60m: *drift_60m_rx.borrow(), timestamp: Utc::now(),
                        }),
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
                                info!("📤 EXIT [{}]: {} | shares={:.2}, bid=${:.4} | {}", sn, params.market_name, shares, params.price, reason);
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
                                            info!("💰 Position closed [{}]: PnL ${:.4}", sn, pnl);

                                            re_m = p.avg_entry;
                                            rs_m = p.shares;
                                            rc_m = p.close_time;
                                            pnl_m = pnl;

                                            if !config::GHOST_MODE {
                                                // Record trade metrics asynchronously only in real mode
                                                let sn_task = sn.clone();
                                                let m_name = params.market_name.clone();
                                                let sid = if tid == target_yes_token { "YES".to_string() } else { "NO".to_string() };
                                                let rp = actual_exit_price;
                                                let r_m = reason.clone();
                                                tokio::spawn(async move { metrics::record_trade(sn_task, m_name, sid, re_m, rp, rs_m, pnl_m, r_m).await; });
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
                                    if exit_pair {
                                        let other_tid = if tid == target_yes_token { target_no_token } else { target_yes_token };
                                        let pk = (sn.clone(), other_tid); let ps = { let map = positions.lock().await; map.get(&pk).map(|p| p.shares) };
                                        if let Some(s) = ps {
                                            let other_bid = if other_tid == target_yes_token { ctx.snapshot.yes_bid } else { ctx.snapshot.no_bid }; // This needs to be from the correct snapshot
                                            let other_fee_bps = if other_tid == target_yes_token { target_yes_fee_bps as u16 } else { target_no_fee_bps as u16 };
                                            let other_vc = if target_is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                            if !config::GHOST_MODE { let _ = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, other_vc, other_tid, Side::Sell, s, (other_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE), other_fee_bps, OrderType::FAK, false, 0, &shared_http).await; }
                                            let mut map = positions.lock().await; if let Some(p) = map.remove(&pk) { let actual_other_exit = (other_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE); let pnl = (actual_other_exit - p.avg_entry) * p.shares; *total_pnl.lock().await += pnl;
                                                if !config::GHOST_MODE {
                                                    let sn_pm = sn.clone(); let m_name = params.market_name.clone(); let sid = if other_tid == target_yes_token { "YES".to_string() } else { "NO".to_string() }; let p_avg = p.avg_entry; let o_bid = actual_other_exit; let p_shares = p.shares; let pn = pnl;
                                                    tokio::spawn(async move { metrics::record_trade(sn_pm, m_name, sid, p_avg, o_bid, p_shares, pn, "Convergence/PairedExit".to_string()).await; });
                                                }
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
                                    // Detached: Telegram latency must never block the trading select! loop.
                                    { let tok = tg_token.clone(); let cid = tg_chat_id.clone(); let msg = format!("📤 EXIT [{}] {} | bid=${:.4} | reason: {} | Session PnL: ${:.4}", sn, params.market_name, params.price, reason, *total_pnl.lock().await); tokio::spawn(async move { let _ = send_notification(&tok, &cid, &msg).await; }); }
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
                                    // Store the actual fill price (signal price + buy offset) as avg_entry
                                    // so PnL calculations reflect true cost basis, not the signal price.
                                    let actual_entry_price = (params.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE);
                                    positions.lock().await.insert(pos_key.clone(), Position { shares: params.shares, avg_entry: actual_entry_price, opened_at: Utc::now(), close_time: pos_close_time, market_name: params.market_name.clone(), pair_token_id: params.token_id, fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: pair_params.as_ref().map(|p| p.token_id) });

                                    info!("👻 GHOST_MODE ENTRY [{}]: {} | ${:.4} x {:.1} (simulated)",
                                        sn, params.market_name, params.price, params.shares);
                                    if let Some(pp) = pair_params {
                                        let pp_close_time = target_market_close_time;
                                        positions.lock().await.insert((sn.clone(), pp.token_id), Position { shares: pp.shares, avg_entry: pp.price, opened_at: Utc::now(), close_time: pp_close_time, market_name: pp.market_name.clone(), pair_token_id: pp.token_id, fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: Some(params.token_id) });
                                        info!("👻 GHOST_MODE ENTRY (paired) [{}]: {} | ${:.4} x {:.1} (simulated)", sn, pp.market_name, pp.price, pp.shares);
                                    }
                                    last_trade_time.insert(sn.clone(), Instant::now());
                                    // No actual order placement or balance sync in ghost mode
                                } else {
                                    // Real trading logic
                                    {
                                        let mut map = positions.lock().await; if map.contains_key(&pos_key) { continue; }
                                        let pos_close_time = target_market_close_time;
                                        let actual_entry_price = (params.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE);
                                        map.insert(pos_key.clone(), Position { shares: params.shares, avg_entry: actual_entry_price, opened_at: Utc::now(), close_time: pos_close_time, market_name: params.market_name.clone(), pair_token_id: params.token_id, fill_confirmed_at: None, paired_leg_token_id: pair_params.as_ref().map(|p| p.token_id) });
                                    }

                                    // Set pending lock for 3 seconds
                                    {
                                        pending_orders.lock().await.insert(pos_key.clone(), Instant::now() + Duration::from_secs(3));
                                    }

                                    info!("📥 ENTRY [{}]: {} | ${:.4} x {:.1}", sn, params.market_name, params.price, params.shares);
                                    let vc = if target_is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                    if let Err(e) = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc, params.token_id, Side::Buy, params.shares, (params.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE), target_yes_fee_bps as u16, params.order_type, params.post_only, 0, &shared_http).await { // Use target_yes_fee_bps for simplicity
                                        warn!("⚠️ ENTRY order failed [{}]: {}", sn, e);
                                        positions.lock().await.remove(&pos_key);
                                        pending_orders.lock().await.remove(&pos_key);
                                        last_trade_time.insert(sn.clone(), Instant::now());
                                        consecutive_failures += 1; continue;
                                    }
                                    let cl_s = Arc::clone(&trading_client); let ps_s = Arc::clone(&positions); let pc_s = Arc::clone(&phantom_cooldowns); let sn_s = sn.clone(); let tn_s = params.token_id;
                                    tokio::spawn(async move { let _ = sync_position_balance(&cl_s, &ps_s, &sn_s, tn_s, Some(&pc_s), dec!(0), dradis::helpers::balance::MAX_WAIT_SECS_HOURLY).await; });

                                    { let sn_e = sn.clone(); let tid_e = params.token_id.to_string(); let mn_e = params.market_name.clone(); let side_e = if params.token_id == target_yes_token { "YES" } else { "NO" }.to_string(); let ep_e = (params.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE); let sh_e = params.shares; tokio::spawn(async move { metrics::record_entry(sn_e, tid_e, mn_e, side_e, ep_e, sh_e).await; }); }

                                    if let Some(pp) = pair_params {
                                        let vc_p = if pp.is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                        let leg_b_result = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc_p, pp.token_id, Side::Buy, pp.shares, (pp.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE), target_yes_fee_bps as u16, pp.order_type, pp.post_only, 0, &shared_http).await; // Use target_yes_fee_bps for simplicity

                                        match leg_b_result {
                                            Err(ref e) => {
                                                warn!("⚡ Leg B FAILED [{}]: {} — Flash-Exit spawned for Leg A (token {})", sn, e, params.token_id);
                                                let cl_fe   = Arc::clone(&trading_client);
                                                let nm_fe   = Arc::clone(&nonce_manager);
                                                let ps_fe   = Arc::clone(&positions);
                                                let http_fe = Arc::clone(&shared_http);
                                                let signer_fe = signer.clone();
                                                let sn_fe   = sn.clone();
                                                let tok_a   = params.token_id;
                                                let bid_a   = if tok_a == target_yes_token { ctx.snapshot.yes_bid } else { ctx.snapshot.no_bid }; // This needs to be from the correct snapshot
                                                let fee_a   = target_yes_fee_bps; // Use target_yes_fee_bps for simplicity
                                                let vc_a    = vc;
                                                tokio::spawn(async move {
                                                    let deadline = tokio::time::Instant::now()
                                                        + Duration::from_millis(config::FLASH_EXIT_CONFIRM_MS);
                                                    loop {
                                                        if tokio::time::Instant::now() >= deadline {
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
                                                                    fee_a as u16, OrderType::FAK, false, 0, &http_fe,
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
                                            }
                                            Ok(_) => {
                                                let pp_close_time = target_market_close_time;
                                                positions.lock().await.insert((sn.clone(), pp.token_id), Position { shares: pp.shares, avg_entry: pp.price, opened_at: Utc::now(), close_time: pp_close_time, market_name: pp.market_name.clone(), pair_token_id: pp.token_id, fill_confirmed_at: None, paired_leg_token_id: Some(params.token_id) });
                                                let sn_p = sn.clone(); let tn_p = pp.token_id; let ps_p = Arc::clone(&positions); let cl_p = Arc::clone(&trading_client); let pc_p = Arc::clone(&phantom_cooldowns);
                                                tokio::spawn(async move { let _ = sync_position_balance(&cl_p, &ps_p, &sn_p, tn_p, Some(&pc_p), dec!(0), dradis::helpers::balance::MAX_WAIT_SECS_HOURLY).await; });
                                                { let sn_eb = sn.clone(); let tid_eb = pp.token_id.to_string(); let mn_eb = pp.market_name.clone(); let side_eb = if pp.token_id == target_yes_token { "YES" } else { "NO" }.to_string(); let ep_eb = pp.price; let sh_eb = pp.shares; tokio::spawn(async move { metrics::record_entry(sn_eb, tid_eb, mn_eb, side_eb, ep_eb, sh_eb).await; }); }
                                            }
                                        }
                                    }
                                    last_trade_time.insert(sn.clone(), Instant::now());
                                    { let tok = tg_token.clone(); let cid = tg_chat_id.clone(); let msg = format!("📥 ENTRY [{}] {} | ${:.4} x {:.1}", sn, params.market_name, params.price, params.shares); tokio::spawn(async move { let _ = send_notification(&tok, &cid, &msg).await; }); }
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
                                        info!("👻 GHOST_MODE MakerQuote [{}]: {} | shares={:.2}, bid=${:.4} (simulated)", sn, p.market_name, p.shares, p.price);
                                        placed = true;
                                    } else {
                                        // Real trading logic
                                        if !positions.lock().await.contains_key(&pk) {
                                            info!("📥 MakerQuote [{}]: {} | shares={:.2}, bid=${:.4}", sn, p.market_name, p.shares, p.price);
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
                                if consecutive_failures >= config::MAX_CONSECUTIVE_FAILURES { error!("🚨 Circuit breaker hit!"); tokio::time::sleep(Duration::from_secs(60)).await; consecutive_failures = 0; }
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
                        error!("🚨 WATCHDOG: inner loop silent for {}s — forcing restart", elapsed);
                        break;
                    }
                }
            }
        }
    }
}
