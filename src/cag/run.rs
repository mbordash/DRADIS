/// CAG run — the market-rotation loop extracted from `main.rs` (Phase 3f-5).
///
/// `run_market_loop` is the single entry-point that drives the full lifecycle
/// of a trading session: initial market bootstrap → WS subscription →
/// `Squadron::patrol()` delegation → market rotation → repeat.
///
/// `main.rs` (Phase 3f-5+) is reduced to a thin bootstrapper that assembles
/// the infrastructure handles bundled in `RunArgs` and calls this function once.
/// It is expected to loop for the lifetime of the process.

use std::collections::HashMap;
use std::sync::{Arc, atomic::AtomicU64, RwLock};

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::signers::local::LocalSigner;
use chrono::Utc;
use rust_decimal_macros::dec;
use tokio::sync::{watch, Mutex};
use tokio::time::{Duration, Instant};
use tracing::{info, warn};

use polymarket_client_sdk_v2::clob::Client as ClobClient;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;

use crate::cag::{Cag, SessionState};
use crate::helpers::dynamic_config::DynamicConfig;
use crate::helpers::market::{get_market_pair, extract_strike_price, MarketCandidate};
use crate::helpers::time::fetch_historical_strike_price;
use crate::state::{MarketConfig, PriceState};
use crate::tasks::market_monitor::{run_market_monitor, MarketState};
use crate::squadron::{Squadron, SquadronConfig, SquadronRaptors, CryptoAsset, PatrolContext, MarketPriceFeeds};
use tokio_util::sync::CancellationToken;

// ─── RunArgs ──────────────────────────────────────────────────────────────────

/// All infrastructure handles that `run_market_loop` needs to drive the
/// full market-rotation lifecycle.
///
/// Constructed by `main.rs` (Phase 3f-5+) and passed once to
/// `run_market_loop`, which loops forever until the process exits.
pub struct RunArgs<P> {
    /// CAG registry — squadrons are registered/updated here on each rotation.
    pub cag: Cag,
    /// Authenticated CLOB REST client.
    pub trading_client: Arc<ClobClient<Authenticated<Normal>>>,
    /// Shared reqwest HTTP client (DNS pre-pinned in main.rs).
    pub shared_http: Arc<reqwest::Client>,
    /// Session-scoped nonce manager (Maker/Safe wallet).
    pub nonce_manager: Arc<AtomicU64>,
    /// EOA signing key — cheaply cloned per order placement.
    pub signer: LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    /// Polymarket Safe (maker) address.
    pub safe_address: Address,
    /// Externally owned account address.
    pub eoa_address: Address,
    /// Alloy wallet provider — used only by the settlement task.
    pub wallet_provider: P,
    /// Lowercase crypto symbol (e.g. `"btc"`) for market discovery.
    pub crypto_filter: String,
    /// Raptor signal receivers — cloned into each new Squadron on rotation.
    pub raptor_signals: SquadronRaptors,
    /// Session-scoped shared state (positions, PnL, collateral, etc.).
    pub session: SessionState,
    // Note: Squadron configs are now per-squadron, loaded from DB on deployment.
    // Global config_tx/config_rx removed in favor of squadron-scoped DynamicConfig.
    /// Broadcasts the strategy→market mapping to the Control Tower status feed.
    pub markets_tx: Arc<watch::Sender<HashMap<String, String>>>,
    // ── Notification credentials ─────────────────────────────────────────────
    pub tg_token:               String,
    pub tg_chat_id:             String,
    pub tw_api_key:             String,
    pub tw_api_secret:          String,
    pub tw_access_token:        String,
    pub tw_access_token_secret: String,
    /// UNIX epoch seconds updated by the status task — read by the OS-thread watchdog.
    pub process_heartbeat_secs: Arc<AtomicU64>,
    /// External cancellation token — fired by `Cag::stand_down_asset()` to
    /// request a graceful exit of this asset's entire market-rotation loop.
    /// Checked at the top of every `'market_loop` iteration before any I/O.
    pub cancel: CancellationToken,
}

// ─── run_market_loop ──────────────────────────────────────────────────────────

/// Drive the full market-rotation lifecycle for one DRADIS session.
///
/// This function contains the logic of `main.rs`'s `'market_loop` (Phase 3f-5),
/// promoted into the CAG layer so `main.rs` is reduced to a thin bootstrapper.
///
/// Flow:
///  1. Bootstrap: poll Gamma API until a valid initial market is available.
///  2. Resolve strike prices for hourly + maker markets.
///  3. Create the `market_rx` watch channel seeded with the initial state.
///  4. Spawn the background `market_monitor` task.
///  5. Construct `PatrolContext` (lives outside the rotation loop so cooldown
///     maps survive market switches).
///  6. `'market_loop`: fetch fee rates → build `Squadron` → call `patrol()`.
///     Repeats on every market rotation.
///
/// **Expected to loop for the process lifetime.** Only exits on `process::exit`.
pub async fn run_market_loop<P>(args: RunArgs<P>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    let RunArgs {
        cag,
        trading_client,
        shared_http,
        nonce_manager,
        signer,
        safe_address,
        eoa_address,
        wallet_provider,
        crypto_filter,
        raptor_signals,
        session,
        markets_tx,
        tg_token,
        tg_chat_id,
        tw_api_key,
        tw_api_secret,
        tw_access_token,
        tw_access_token_secret,
        process_heartbeat_secs,
        cancel,
    } = args;

    // ── Sentinel price feeds — replaced on first WS subscription ─────────────
    // PatrolContext requires `watch::Receiver<PriceState>` for its `feeds` field.
    // We seed it with zeroed sentinels here; the real receivers are installed by
    // `squadron.subscribe_markets()` inside 'market_loop before patrol() starts.
    let (_sentinel_tx, sentinel_rx) = watch::channel::<PriceState>(
        (dec!(0), dec!(0), dec!(1), dec!(0), Utc::now()));

    // ── Bootstrap: poll Gamma API until we have a valid initial market ────────
    let mut bootstrap_attempts = 0u32;
    let (mut initial_hourly_candidate, mut initial_maker_candidate) = loop {
        let (hourly_cand, maker_cand) = get_market_pair(&shared_http).await;

        if hourly_cand.yes_token != U256::ZERO {
            info!("✅ Found initial hourly market: \"{}\"", hourly_cand.name);
            break (hourly_cand, maker_cand);
        }

        if maker_cand.is_some() {
            info!("⚠️ No hourly market found, but a maker market is available. Starting with maker market context.");
            break (
                MarketCandidate {
                    yes_token: U256::ZERO, no_token: U256::ZERO,
                    name: String::new(), link: String::new(),
                    description: String::new(), is_hot: false,
                    close_time: None, volume: 0.0,
                    condition_id: String::new(), strike_price: None,
                },
                maker_cand,
            );
        }

        bootstrap_attempts += 1;
        warn!("⏳ No active hourly or maker market found (attempt {}) — retrying in 30s...", bootstrap_attempts);
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    };

    // ── Resolve strike prices ─────────────────────────────────────────────────
    if initial_hourly_candidate.yes_token != U256::ZERO {
        let mut strike = extract_strike_price(&initial_hourly_candidate.name);
        if strike.is_none() {
            strike = fetch_historical_strike_price(&shared_http, &crypto_filter, &initial_hourly_candidate.description).await;
            if strike.is_none() {
                strike = fetch_historical_strike_price(&shared_http, &crypto_filter, &initial_hourly_candidate.name).await;
            }
        }
        initial_hourly_candidate.strike_price = strike;
        if strike.is_some() { info!("✅ Hourly market strike price resolved: ${}", strike.unwrap()); }
    } else {
        info!("⏳ No initial hourly market found. Waiting for market monitor to find one.");
    }

    if let Some(ref mut mk) = initial_maker_candidate {
        let mut strike = extract_strike_price(&mk.name);
        if strike.is_none() {
            strike = fetch_historical_strike_price(&shared_http, &crypto_filter, &mk.description).await;
            if strike.is_none() {
                strike = fetch_historical_strike_price(&shared_http, &crypto_filter, &mk.name).await;
            }
        }
        mk.strike_price = strike;
        if strike.is_some() { info!("✅ Maker market strike price resolved: ${}", strike.unwrap()); }
    } else {
        info!("⏳ No initial maker market found. Waiting for market monitor to find one.");
    }

    // ── Create the market watch channel seeded with the initial state ─────────
    let initial_market_state: MarketState = (
        initial_hourly_candidate.yes_token,
        initial_hourly_candidate.no_token,
        initial_hourly_candidate.name.clone(),
        initial_hourly_candidate.close_time,
        initial_hourly_candidate.strike_price,
        initial_hourly_candidate.description.clone(),
        initial_maker_candidate,
        initial_hourly_candidate.condition_id.clone(),
    );
    let (market_tx, market_rx) = watch::channel(initial_market_state);

    // Spawn the background market monitor — polls Gamma API and updates
    // `market_tx` whenever a new hourly or maker market becomes available.
    tokio::spawn(run_market_monitor(
        Arc::clone(&shared_http),
        crypto_filter.clone(),
        market_tx.clone(),
    ));

    // ── Construct PatrolContext (lives OUTSIDE 'market_loop) ─────────────────
    // Per-market fields (`feeds`, `maker_market_config`, `market_started_at`)
    // are updated in-place before each `patrol()` invocation so cooldown maps
    // survive market rotations without Arc/Mutex overhead.
    let mut patrol_ctx = PatrolContext {
        session:        session.clone(),
        trading_client: Arc::clone(&trading_client),
        nonce_manager:  Arc::clone(&nonce_manager),
        signer:         signer.clone(),
        safe_address,
        eoa_address,
        shared_http:    Arc::clone(&shared_http),
        wallet_provider: wallet_provider.clone(),
        market_rx:       market_rx.clone(),
        dynamic_config:  Arc::new(RwLock::new(DynamicConfig::default())), // placeholder, set per squadron
        markets_tx:      Arc::clone(&markets_tx),
        crypto_filter:   crypto_filter.clone(),
        tg_token:              tg_token.clone(),
        tg_chat_id:            tg_chat_id.clone(),
        tw_api_key:            tw_api_key.clone(),
        tw_api_secret:         tw_api_secret.clone(),
        tw_access_token:       tw_access_token.clone(),
        tw_access_token_secret: tw_access_token_secret.clone(),
        process_heartbeat_secs: Arc::clone(&process_heartbeat_secs),
        last_heartbeat_at:      Arc::new(Mutex::new(Instant::now())),
        // Per-market fields — updated inside 'market_loop before each patrol():
        feeds: MarketPriceFeeds {
            hourly_yes: sentinel_rx.clone(),
            hourly_no:  sentinel_rx.clone(),
            maker_yes:  None,
            maker_no:   None,
        },
        maker_market_config: None,
        market_started_at:   Utc::now(),
        cag: cag.clone(),
        // Cooldown maps — start empty; survive market rotations because they
        // live here, outside 'market_loop, not inside patrol().
        last_trade_time:        HashMap::new(),
        last_stop_loss_time:    HashMap::new(),
        last_expiry_exit_time:  HashMap::new(),
        last_exit_attempt_time: HashMap::new(),
    };

    // ── Market-rotation loop ──────────────────────────────────────────────────
    // Restarts on every market rotation (market_rx.changed() fires inside
    // patrol()) or when the loop-watchdog forces a restart via patrol_cancel.
    'market_loop: loop {
        // Check external cancellation token — fired by Cag::stand_down_asset().
        // Checked first so a stand-down during bootstrap or fee-rate fetch is
        // honoured immediately without waiting for the next I/O timeout.
        if cancel.is_cancelled() {
            info!("🛬  run_market_loop [{}]: cancellation token fired — exiting loop", crypto_filter.to_uppercase());
            break 'market_loop;
        }

        let (
            hourly_yes_token,
            hourly_no_token,
            hourly_market_name,
            hourly_market_close_time,
            hourly_strike_price,
            _hourly_desc,
            maker_market_candidate_from_channel,
            hourly_condition_id,
        ) = patrol_ctx.market_rx.borrow().clone();

        let (hourly_yes_token, hourly_no_token, hourly_market_name, hourly_market_close_time,
             hourly_strike_price, _hourly_desc, hourly_condition_id,
             hourly_is_neg_risk, hourly_yes_fee_rate, hourly_no_fee_rate)
            = if hourly_yes_token != U256::ZERO
        {
            let now = Utc::now();
            if let Some(close_time) = hourly_market_close_time {
                let seconds_until_expiry = (close_time - now).num_seconds();
                if seconds_until_expiry < crate::config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY {
                    warn!("⚠️ Hourly market expiring too soon ({}s left)!", seconds_until_expiry);
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    continue 'market_loop;
                }
                info!("⏰ Hourly market closes in {}s", seconds_until_expiry);
            }

            info!("🛫 Starting Orchestrated Trading on hourly market: \"{}\"", hourly_market_name);

            let yes_fee_rate = match tokio::time::timeout(
                Duration::from_secs(10),
                patrol_ctx.trading_client.fee_rate_bps(hourly_yes_token),
            ).await {
                Ok(Ok(r))  => r.base_fee,
                Ok(Err(e)) => { warn!("⚠️ hourly fee_rate YES error: {} — retrying init in 5s: {}", e, e); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
                Err(_)     => { warn!("⚠️ hourly fee_rate YES timed out (10s) — retrying init in 5s"); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
            };
            let no_fee_rate = match tokio::time::timeout(
                Duration::from_secs(10),
                patrol_ctx.trading_client.fee_rate_bps(hourly_no_token),
            ).await {
                Ok(Ok(r))  => r.base_fee,
                Ok(Err(e)) => { warn!("⚠️ hourly fee_rate NO error: {} — retrying init in 5s: {}", e, e); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
                Err(_)     => { warn!("⚠️ hourly fee_rate NO timed out (10s) — retrying init in 5s"); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
            };
            let is_neg_risk = match tokio::time::timeout(
                Duration::from_secs(10),
                patrol_ctx.trading_client.neg_risk(hourly_yes_token),
            ).await {
                Ok(Ok(r))  => r.neg_risk,
                Ok(Err(e)) => { warn!("⚠️ hourly neg_risk error: {} — retrying init in 5s: {}", e, e); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
                Err(_)     => { warn!("⚠️ hourly neg_risk timed out (10s) — retrying init in 5s"); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
            };
            info!("✅ Hourly market settings: NegRisk: {} | YES fee {} bps | NO fee {} bps",
                is_neg_risk, yes_fee_rate, no_fee_rate);
            (hourly_yes_token, hourly_no_token, hourly_market_name, hourly_market_close_time,
             hourly_strike_price, _hourly_desc, hourly_condition_id, is_neg_risk, yes_fee_rate, no_fee_rate)
        } else {
            info!("⚠️ No active hourly market found. Hourly-dependent strategies will be inactive.");
            (U256::ZERO, U256::ZERO, String::new(), None, None, String::new(), String::new(), false, 0, 0)
        };

        let _market_started_at = Utc::now();

        // ── Assemble the hourly MarketConfig and Squadron ─────────────────────
        let hourly_market_config_for_squadron = MarketConfig {
            yes_token:         hourly_yes_token,
            no_token:          hourly_no_token,
            market_name:       hourly_market_name.clone(),
            market_close_time: hourly_market_close_time,
            strike_price:      hourly_strike_price,
            is_neg_risk:       hourly_is_neg_risk,
            condition_id:      hourly_condition_id.clone(),
            yes_fee_bps:       hourly_yes_fee_rate,
            no_fee_bps:        hourly_no_fee_rate,
        };
        let mut squadron = Squadron::new(
            patrol_ctx.crypto_filter.parse::<CryptoAsset>().unwrap_or(CryptoAsset::Btc),
            SquadronConfig::full_wing(
                format!("Full Wing — {}", patrol_ctx.crypto_filter.to_uppercase())
            ),
            hourly_market_config_for_squadron,
            SquadronRaptors::full(
                raptor_signals.oracle.clone(),
                raptor_signals.velocity.clone(),
                raptor_signals.drift.clone(),
                raptor_signals.funding.clone().expect("funding raptor always present"),
            ),
        );

        // Initialize squadron-scoped config (copy from config.rs defaults)
        let squadron_config = DynamicConfig::init_for_squadron(&squadron.id).await;
        patrol_ctx.dynamic_config = Arc::new(RwLock::new((*squadron_config).clone()));

        squadron.start_patrol();
        info!("🛫  Squadron [{}] → state={}", squadron.id, squadron.state);

        // Register with CAG so GET /api/squadrons shows this deployment.
        let _squadron_id = patrol_ctx.cag.register(&squadron);

        // ── Subscribe to WS orderbook feeds for this market rotation ──────────
        let maker_tokens = maker_market_candidate_from_channel
            .as_ref()
            .map(|mk| (mk.yes_token, mk.no_token));
        patrol_ctx.feeds = squadron.subscribe_markets(hourly_yes_token, hourly_no_token, maker_tokens);

        // ── Resolve maker market config ───────────────────────────────────────
        let maker_market_config: Option<MarketConfig> = if let Some(ref mk) = maker_market_candidate_from_channel {
            let mk_yes_fee = match tokio::time::timeout(
                Duration::from_secs(10),
                patrol_ctx.trading_client.fee_rate_bps(mk.yes_token),
            ).await {
                Ok(Ok(r))  => r.base_fee,
                Ok(Err(e)) => { warn!("⚠️ maker fee_rate YES error: {} — retrying init in 5s: {}", e, e); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
                Err(_)     => { warn!("⚠️ maker fee_rate YES timed out (10s) — retrying init in 5s"); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
            };
            let mk_no_fee = match tokio::time::timeout(
                Duration::from_secs(10),
                patrol_ctx.trading_client.fee_rate_bps(mk.no_token),
            ).await {
                Ok(Ok(r))  => r.base_fee,
                Ok(Err(e)) => { warn!("⚠️ maker fee_rate NO error: {} — retrying init in 5s: {}", e, e); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
                Err(_)     => { warn!("⚠️ maker fee_rate NO timed out (10s) — retrying init in 5s"); tokio::time::sleep(Duration::from_secs(5)).await; continue 'market_loop; }
            };
            let mk_neg_risk = match tokio::time::timeout(
                Duration::from_secs(10),
                patrol_ctx.trading_client.neg_risk(mk.yes_token),
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

        // Finalise per-market PatrolContext fields.
        patrol_ctx.maker_market_config = maker_market_config;
        patrol_ctx.market_started_at   = Utc::now();

        // Now that the maker venue is resolved, update the CAG registry so the
        // Control Tower shows both the hourly AND the window/daily market name.
        if let Some(ref mk) = patrol_ctx.maker_market_config {
            patrol_ctx.cag.update_maker_market(&_squadron_id, mk.market_name.clone());
        }

        // Delegate the inner tick loop to Squadron::patrol().
        // Returns when market_rx.changed() fires (rotation) or the watchdog
        // forces a restart via patrol_cancel.
        let patrol_cancel = tokio_util::sync::CancellationToken::new();
        squadron.patrol(patrol_cancel, &mut patrol_ctx).await;
    }
}

