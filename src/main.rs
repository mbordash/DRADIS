/// dradis - Multi-Strategy Orchestrator Trading Bot
///
/// Phase 3f-7: Per-asset SQLite DB pools + Control Tower multi-asset selector.
/// Set ASSETS=btc,eth,sol (or just CRYPTO_FILTER=btc for single-asset mode).
/// Each asset gets its own raptors, session state, SQLite pool, and patrol loop.
/// Shared: wallet/nonce, CLOB client, CAG registry, API server.

use anyhow::Result;

#[cfg(feature = "intl_clob")]
use polymarket_client_sdk_v2::clob::types::request::BalanceAllowanceRequest;
#[cfg(feature = "intl_clob")]
use polymarket_client_sdk_v2::clob::types::AssetType;

#[cfg(feature = "intl_clob")]
use alloy::providers::ProviderBuilder;

use chrono::Utc;
use chrono_tz::US::Eastern;
use reqwest;
use rust_decimal::Decimal;
#[cfg(feature = "intl_clob")]
use rust_decimal_macros::dec;

use std::env;
#[cfg(feature = "intl_clob")]
use std::str::FromStr as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use tokio::sync::watch;
#[cfg(feature = "intl_clob")]
use tokio::time::Duration;

#[cfg(feature = "intl_clob")]
use tracing::{info, warn};

use dradis::config;
#[cfg(feature = "intl_clob")]
use dradis::squadron::{SquadronRaptors};
#[cfg(feature = "intl_clob")]
use dradis::cag::{Cag, SessionState, RunArgs, run_market_loop};
#[cfg(feature = "us_retail")]
use dradis::cag::Cag;
#[cfg(feature = "intl_clob")]
use dradis::venues::intl::IntlClobVenue;
use dradis::helpers::dynamic_config::DynamicConfig;
use dradis::api::server::AssetRaptorHealth;
#[cfg(feature = "intl_clob")]
use tokio_util::sync::CancellationToken;

use dradis::helpers::{
    db,
};

use rustls::crypto::ring;



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

// V2 CTF Exchange contracts are defined in squadron/patrol_tasks.rs (used by peripheral tasks)

// Constants for cancel_all_orders retry logic (intl self-custody startup only).
#[cfg(feature = "intl_clob")]
const MAX_CANCEL_RETRIES: u32 = 5;
#[cfg(feature = "intl_clob")]
const BASE_CANCEL_RETRY_DELAY_MS: u64 = 200; // Start with 200ms

// Force at least 8 worker threads.  With multiple concurrent asset loops each
// running peripheral tasks (status, cleanup, settlement, pulse, watchdog) plus
// WS reconnect loops, a larger pool prevents one blocking call from starving
// the rest.  Previously 4 was sufficient for single-asset; 8 covers BTC+ETH+SOL.
#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
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

    // ── OS-thread watchdog — immune to tokio runtime deadlocks ───────────────
    // Root cause of the May 28 overnight freeze: the tokio runtime ran with 1
    // worker thread on a single-core t2.small.  Any call that blocked that thread
    // synchronously (TCP stall, std::sync::Mutex contention during GBoost retrain,
    // Polymarket WS reconnect loop) froze the ENTIRE runtime — watchdog_ticker,
    // timeouts, heartbeat, select! arms, all silenced.  The container became
    // (unhealthy) but `--restart unless-stopped` only restarts on process exit
    // (not on health-check failure), so it sat dead for 10+ hours.
    //
    // This watchdog runs on a native OS thread, completely outside tokio.
    // It checks an AtomicU64 wall-clock heartbeat every 60 s.  If the trading
    // loop hasn't updated it in 300 s (5 min) the watchdog calls process::exit(1),
    // which DOES trigger Docker's `--restart unless-stopped` restart policy.
    let process_heartbeat_secs = Arc::new(AtomicU64::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    ));
    {
        let hb = Arc::clone(&process_heartbeat_secs);
        std::thread::spawn(move || {
            const PROCESS_WATCHDOG_TIMEOUT_SECS: u64 = 300; // 5 minutes
            loop {
                std::thread::sleep(std::time::Duration::from_secs(60));
                let last_beat = hb.load(AtomicOrdering::Relaxed);
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let silent_secs = now_secs.saturating_sub(last_beat);
                if silent_secs > PROCESS_WATCHDOG_TIMEOUT_SECS {
                    eprintln!(
                        "🚨 OS WATCHDOG: trading loop silent for {}s (limit={}s) \
                         — calling process::exit(1) to trigger Docker restart",
                        silent_secs, PROCESS_WATCHDOG_TIMEOUT_SECS
                    );
                    std::process::exit(1);
                }
            }
        });
    }

    // ── SQLite + DynamicConfig ────────────────────────────────────────────────
    // Init DB first so DynamicConfig::load_or_default can read from it.
    //
    // Phase 3f-6: parse the asset list here (early) so the primary asset's slug
    // can be used to name the DB file.  ASSETS=btc,eth,sol overrides CRYPTO_FILTER.
    // The DB global singleton covers the PRIMARY asset only; secondary assets run
    // CSV-only metrics.  Per-asset DB pools are a Phase 3f-7 concern.
    let crypto_filter = env::var("CRYPTO_FILTER").unwrap_or_else(|_| "btc".to_string()).to_lowercase();
    let assets: Vec<String> = env::var("ASSETS")
        .unwrap_or_else(|_| crypto_filter.clone())
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    let primary_asset = assets.first().cloned().unwrap_or_else(|| crypto_filter.clone());

    // Phase 3f-7: initialise a per-asset SQLite pool for EVERY asset in the fleet.
    // The first call claims the global "primary" pool slot for backward-compat
    // callers that use db::pool() (API handlers, LLM advisor, etc.).
    for asset in &assets {
        let db_path = format!("logs/{}-dradis.db", asset);
        if let Err(e) = db::init_for_asset(asset, &db_path).await {
            tracing::warn!("⚠️  SQLite init failed for {} (metrics will CSV-only): {}", asset, e);
        }
    }
    // Keep a reference to the primary DB path for the session init call below.
    let _db_path = format!("logs/{}-dradis.db", primary_asset);
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

    // ── Raptor health channel (feeds /api/status raptors field) ──────────────
    // Populated by the Price and Funding Raptors for every active asset.
    let (raptor_health_tx, raptor_health_rx) = watch::channel::<std::collections::HashMap<String, AssetRaptorHealth>>(
        std::collections::HashMap::new(),
    );
    let raptor_health_tx = Arc::new(raptor_health_tx);

    // NOTE: API server is spawned after safe_address is derived below so it can
    // be passed in for the /api/positions/sync endpoint.

    let _trade_size_usdc: Decimal = env::var("TRADE_SIZE_USDC").unwrap_or_else(|_| "10".to_string()).parse()?;

    // ── Instantiate CAG (shared by both venues) ─────────────────────────────
    // Created here so both the intl bootstrap and the us_retail API-only path
    // can hand it to the Control Tower API server.
    let cag = Cag::new();

    // ── US retail: Control-Tower-only mode ───────────────────────────────────
    // The custodial US venue's trading bootstrap (auth, market discovery,
    // execution) is implemented in Step 3b.  For now we bring up the API server
    // so the dashboard is reachable, then park the main task so the process
    // stays alive serving it.
    #[cfg(feature = "us_retail")]
    {
        // These are consumed only by the intl bootstrap / per-asset loops below.
        let _ = (&markets_tx, &raptor_health_tx);
        tokio::spawn(dradis::api::server::run_api_server(
            Arc::clone(&config_tx),
            config_rx.clone(),
            markets_rx,
            raptor_health_rx,
            cag.clone(),
        ));

        // ── Connect the custodial US retail venue (Step 3b) ──────────────────
        // Best-effort: a connect failure (missing creds, gateway down) is logged
        // but does not crash the process — the Control Tower API stays up so the
        // operator can diagnose. The full market-discovery + WS patrol loop for
        // this venue is Step 3c; for now we prove auth + gateway reachability and
        // surface live collateral once.
        match dradis::venues::us::UsRetailVenue::connect(Arc::clone(&shared_http)).await {
            Ok(venue) => {
                use dradis::venues::core::Execution as _;
                match venue.collateral().await {
                    Ok(c)  => tracing::info!("✅ US retail venue connected — available margin ${:.2}", c),
                    Err(e) => tracing::warn!("⚠️ US retail connected but collateral query failed: {e}"),
                }
            }
            Err(e) => tracing::warn!("⚠️ US retail venue connect failed (Control Tower still live): {e}"),
        }

        tracing::warn!(
            "⚠️  UsRetailVenue trading loops not yet wired (Step 3c). \
             Control Tower API is live; venue is connected but idle."
        );
        std::future::pending::<()>().await;
    }

    // ── Intl CLOB bootstrap (self-custody EIP-712 over Polygon) ──────────────
    #[cfg(feature = "intl_clob")]
    {
    let polygon_rpc_url = env::var("POLYGON_RPC_URL")
        .map_err(|_| anyhow::anyhow!("❌ POLYGON_RPC_URL not set in .env. Required for auto-settlement transactions. Use a paid RPC service like Helius (https://www.helius-rpc.com) or QuickNode. Example: POLYGON_RPC_URL=https://mainnet.helius-rpc.com/?api-key=YOUR_KEY"))?;

    // ── Connect the compile-time-selected execution venue ────────────────────
    // For `intl_clob` this loads the EOA signer, authenticates the CLOB client,
    // derives the Safe (maker) address, and seeds the order nonce from the API —
    // the bootstrap that previously lived inline here (see VENUE_ABSTRACTION.md).
    // The raw infra is re-exposed via accessors so the settlement provider,
    // RunArgs, and startup balance/cancel flows below stay unchanged.
    let venue = Arc::new(IntlClobVenue::connect(Arc::clone(&shared_http)).await?);
    let signer         = venue.signer().clone();
    let eoa_address    = venue.eoa_address();
    let safe_address   = venue.safe_address();
    let trading_client = Arc::clone(venue.trading_client());
    let nonce_manager  = Arc::clone(venue.nonce_manager());

    let wallet_provider = ProviderBuilder::new()
        .with_nonce_management(alloy::providers::fillers::SimpleNonceManager::default())  // Auto-refresh nonce from chain; prevents "nonce too low" on auto-settle
        .wallet(signer.clone())
        .connect(&polygon_rpc_url)
        .await?;
    info!("✅ CTF auto-settlement client ready (rpc={})", polygon_rpc_url);


    // ── Spawn Control Tower API server ───────────────────────────────────────
    // Spawned here (after safe_address is derived) so it can be passed to
    // the /api/positions/sync endpoint for on-demand chain reconciliation.
    tokio::spawn(dradis::api::server::run_api_server(
        Arc::clone(&config_tx),
        config_rx.clone(),
        markets_rx,
        raptor_health_rx,
        safe_address,
        cag.clone(),
    ));

    let initial_nonce = nonce_manager.load(AtomicOrdering::SeqCst);
    info!(" Order nonce ready (Maker/Safe): {}", initial_nonce);

    let mut startup_balance = dec!(0);
    for i in 1..=3 {
        info!("🔍 Initializing portfolio balance (Attempt {}/3)...", i);
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
    info!("💰 Starting portfolio value: ${:.2}", startup_balance);

    // ── Startup: cancel any GTC orders left over from the previous session ───
    info!("🧹 Cancelling any leftover open orders from previous session...");
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

    // ── Startup: sync open_positions DB with on-chain state (LIVE mode only) ──
    // NOTE: We intentionally do NOT call purge_all_live_open_positions here.
    //
    // The open_positions table is the authoritative source for strategy→token
    // assignments during reconcile_orphaned_positions.  Wiping it before
    // sync_open_positions_with_chain destroys the exact data needed to correctly
    // re-assign a restarted position to the strategy that opened it.
    //
    // purge_stale_open_positions (called inside sync_open_positions_with_chain)
    // already removes any rows whose tokens are no longer on-chain, which covers
    // all the crash/orphan cases the blanket purge was originally intended to handle.
    info!("🔗 Syncing open_positions DB with on-chain holdings...");
    dradis::tasks::cleanup::sync_open_positions_with_chain(safe_address).await;

    // ── Phase 3f-6: Spawn one market loop per asset ──────────────────────────
    // Each asset gets its own:
    //   • Price + funding raptors   (different Binance WS symbols)
    //   • SessionState              (positions, PnL, collateral tracked independently)
    //   • run_market_loop task      (independent market bootstrap + patrol loop)
    //
    // Shared across all assets:
    //   • trading_client            (same Polymarket wallet)
    //   • nonce_manager             (CLOB order-signing nonce; AtomicU64 is thread-safe)
    //   • wallet_provider           (same Polygon RPC for auto-settlement)
    //   • cag                       (unified CAG registry — all squadrons visible in UI)
    //   • config_rx / markets_tx    (shared dynamic config + status broadcast)
    //   • process_heartbeat_secs    (process-level OS watchdog — ANY asset tick counts)
    //   • LLM Advisor               (ONE global loop reading all asset DBs — spawned after this loop)
    //
    // ⚠️  DB: Phase 3f-7 — each asset has its own SQLite pool initialised above.
    //     The pools are looked up by asset slug in patrol_impl / patrol_tasks
    //     via db::pool_for(&asset_lc) so secondary assets write to their own DB.
    info!("🗺️  Asset fleet: [{}] ({} asset{})",
        assets.join(", ").to_uppercase(),
        assets.len(),
        if assets.len() == 1 { "" } else { "s" });

    let mut loop_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::with_capacity(assets.len());

    // Store first asset's session for LLM Advisor (P&L tracking reference)
    let mut primary_session: Option<SessionState> = None;

    for asset in assets.iter() {
        // ── Per-asset raptor signal feeds ─────────────────────────────────────
        let (oracle_tx, oracle_rx)     = watch::channel(dec!(0));
        let (velocity_tx, velocity_rx) = watch::channel((dec!(0), dec!(0), dec!(0)));
        let (funding_tx, funding_rx)   = watch::channel(dec!(0));
        let (drift_tx, drift_rx)       = watch::channel((dec!(0), dec!(0)));

        tokio::spawn(dradis::raptors::price::run_price_raptor(
            asset.clone(), oracle_tx, velocity_tx, drift_tx,
            Arc::clone(&raptor_health_tx),
        ));
        tokio::spawn(dradis::raptors::funding::run_funding_raptor(
            Arc::clone(&shared_http), asset.clone(), funding_tx,
            Arc::clone(&raptor_health_tx),
        ));
        let raptor_signals = SquadronRaptors::full(oracle_rx, velocity_rx, drift_rx, funding_rx);

        // ── Per-asset session state ────────────────────────────────────────────
        // startup_balance is the real wallet balance at process start — used as
        // the starting-collateral reference for drawdown calculations per asset.
        // live_collateral is refreshed from the CLOB every ~60 s so strategies
        // gate on actual available balance regardless of how many assets are active.
        //
        // Step 1 (venue abstraction): SessionState now holds the execution venue
        // (formerly the trading_client/signer/nonce/http quartet) so the API can
        // execute manual "Return to Base" exits via authenticated orders.
        let asset_session = SessionState::new(
            startup_balance,
            asset.as_str(),
            Arc::clone(&venue),
        );

        // Register EVERY asset's session with the CAG so API handlers can
        // query per-asset data via ?asset= query params.
        cag.set_session(asset_session.clone());

        // Capture first asset's session as the primary reference for global LLM advisor P&L
        if primary_session.is_none() {
            primary_session = Some(asset_session.clone());
        }

        // ── Build RunArgs and spawn the market loop ───────────────────────────
        let loop_cancel = CancellationToken::new();
        let args = RunArgs {
            cag:            cag.clone(),
            trading_client: Arc::clone(&trading_client),
            shared_http:    Arc::clone(&shared_http),
            nonce_manager:  Arc::clone(&nonce_manager),
            signer:         signer.clone(),
            safe_address,
            eoa_address,
            wallet_provider: wallet_provider.clone(),
            crypto_filter:  asset.clone(),
            raptor_signals,
            session:        asset_session,
            markets_tx:     Arc::clone(&markets_tx),
            tg_token:              env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default(),
            tg_chat_id:            env::var("TELEGRAM_CHAT_ID").unwrap_or_default(),
            tw_api_key:            env::var("X_API_KEY").unwrap_or_default(),
            tw_api_secret:         env::var("X_API_SECRET").unwrap_or_default(),
            tw_access_token:       env::var("X_ACCESS_TOKEN").unwrap_or_default(),
            tw_access_token_secret: env::var("X_ACCESS_TOKEN_SECRET").unwrap_or_default(),
            process_heartbeat_secs: Arc::clone(&process_heartbeat_secs),
            cancel:         loop_cancel.clone(),
        };

        info!("🚀 Spawning market loop for asset: {}", asset.to_uppercase());
        let handle = tokio::spawn(run_market_loop(args));

        // Register the AbortHandle + cancel token with the CAG so stand_down_asset()
        // can gracefully exit or forcibly abort this loop at any time.
        // main.rs retains the JoinHandle for awaiting; the CAG holds the AbortHandle.
        cag.register_loop_task(asset, handle.abort_handle(), loop_cancel);
        loop_tasks.push(handle);
    }

    // ── Spawn global LLM Advisor (reads all asset DBs, writes to primary) ────
    // Spawned once after all assets are initialised so it can iterate over
    // db::available_assets().  Uses the first asset's SessionState as the P&L
    // reference for combined portfolio analysis.
    if let Some(ref session) = primary_session {
        tokio::spawn(dradis::helpers::llm_advisor::run_llm_advisor_loop(
            env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default(),
            env::var("TELEGRAM_CHAT_ID").unwrap_or_default(),
            session.total_pnl.clone(),
            session.starting_collateral.clone(),
            config_rx.clone(),
        ));
    }

    // Block until ALL market loops exit (expected: never — each loops forever).
    // The CAG owns AbortHandles for control; main.rs retains JoinHandles here
    // for awaiting.  If a loop task panics, log it and let the remaining assets
    // continue.  The OS-thread watchdog will restart the entire process if the
    // heartbeat goes silent for >300 s.
    for task in loop_tasks {
        if let Err(e) = task.await {
            tracing::error!("❌ Market loop task exited unexpectedly: {:?}", e);
        }
    }
    } // end intl_clob bootstrap block

    Ok(())
}

