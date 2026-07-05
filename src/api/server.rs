/// Control Tower REST API
///
/// Endpoints
/// ─────────────────────────────────────────────────────────────────────────────
///   GET    /api/health                — liveness check
///   GET    /api/assets                — list of initialised asset pools (Phase 3f-7)
///   GET    /api/config                — current DynamicConfig as JSON
///   PATCH  /api/config               — JSON merge-patch; hot-reloads strategies
///   GET    /api/config/schema         — editable-config field schema (drives Advanced UI)
///   GET    /api/pnl/history           — recent P&L snapshots  (?limit=200&asset=btc)
///   GET    /api/trades                — recent completed trades (?limit=100&asset=btc)
///   GET    /api/positions             — current open positions (?asset=btc)
///   DELETE /api/positions/{token_id}  — purge a specific stale row from open_positions
///   POST   /api/positions/sync        — trigger immediate chain-sync against Polymarket wallet
///   POST   /api/positions/manual-exit — manual "Return to Base" exit (FAK market sell)
///   GET    /api/llm/recommendations   — recent LLM Advisor analyses (?limit=10&asset=btc)
///   GET    /api/squadrons             — list all active squadrons (Phase 3d)
///   GET    /api/squadrons/{id}        — get one squadron by id    (Phase 3d)
///
/// All data endpoints accept an optional `?asset=btc` query param (Phase 3f-7).
/// When absent, the primary (first initialised) asset pool is used.
///
/// The server binds to 0.0.0.0:$API_PORT (default 9000).
/// CORS is open so the Next.js Control Tower on any port can reach it.

use axum::{
    Router,
    routing::{get, delete},
    extract::{State, Query, Path, Request},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
    http::{StatusCode, Method, header},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info, warn};
#[cfg(feature = "intl_clob")]
use alloy::primitives::{Address, U256};
#[cfg(feature = "intl_clob")]
use polymarket_client_sdk_v2::clob::types::{Side};
use rust_decimal::Decimal;
#[cfg(feature = "intl_clob")]
use rust_decimal::prelude::FromStr;

use crate::helpers::dynamic_config::DynamicConfig;
use crate::helpers::db;
#[cfg(feature = "intl_clob")]
use crate::helpers::orders::place_limit_order;
#[cfg(feature = "intl_clob")]
use crate::helpers::price::round_to_tick_size;
#[cfg(feature = "intl_clob")]
use crate::helpers::metrics;
#[cfg(feature = "intl_clob")]
use crate::tasks::cleanup::sync_open_positions_with_chain;
use crate::cag::Cag;

// ─── Raptor health types ──────────────────────────────────────────────────────

/// Connection health for a single asset's pair of Binance Raptors.
///
/// `price_connected`   — true when the Price Raptor WebSocket is live and
///                       delivering ticker messages from Binance Spot.
/// `funding_connected` — true when the Funding Raptor last polled
///                       Binance FAPI successfully.
///
/// The remaining fields carry the **latest signal values** broadcast by each
/// Raptor, so the Control Tower Telemetry view can graph them without a
/// separate persistence layer (the frontend builds its own rolling buffer).
/// All default to `0` until the first tick arrives.
#[derive(Serialize, Clone, Default, Debug)]
pub struct AssetRaptorHealth {
    pub price_connected:   bool,
    pub funding_connected: bool,
    pub deriv_connected:   bool,

    // ── Live Price Raptor signal snapshot (Binance Spot WS) ────────────────
    /// Current spot price (oracle).
    pub oracle_price:  Decimal,
    /// 5-second price velocity (Δprice over the 5s window).
    pub velocity_5s:   Decimal,
    /// 1-second price velocity (short window).
    pub velocity_1s:   Decimal,
    /// Acceleration — rate of change of 5s velocity.
    pub acceleration:  Decimal,
    /// 60-minute drift (Δprice over the trailing hour).
    pub drift_60m:     Decimal,
    /// 10-minute drift (fills the 5s–60m temporal gap).
    pub drift_10m:     Decimal,

    // ── Live Funding Raptor signal snapshot (Binance FAPI) ─────────────────
    /// Perpetual funding rate (fraction; ×100 for percent).
    pub funding_rate:  Decimal,

    // ── Live Derivatives Raptor signal snapshot (Binance FAPI) ─────────────
    /// Open interest (base contracts) — raw perp positioning size.
    pub open_interest: Decimal,
    /// Fractional change in open interest since the previous poll (×100 = %).
    pub oi_delta_pct:  Decimal,
    /// Taker buy÷sell volume ratio (CVD proxy); >1 buy aggression, 0 = no data.
    pub cvd_ratio:     Decimal,

    // ── Live Tide Raptor signal snapshot (synthetic iNAV vs IEX ETF prints) ──
    /// Tide Raptor has at least one fresh, in-session ETF premium this tick.
    pub tide_connected:      bool,
    /// True during the US cash session (09:30–16:00 ET); false ⇒ pulse held 0.
    pub tide_market_open:    bool,
    /// Volume-weighted, vol-normalized aggregate premium z-score (signed).
    pub institutional_pulse: Decimal,
    /// Agreement of the Big Three premium signs (0..1); high = conviction.
    pub tide_coherence:      Decimal,
    /// Per-ETF premium vs synthetic iNAV, basis points.
    pub ibit_premium_bps:    Decimal,
    pub fbtc_premium_bps:    Decimal,
    pub arkb_premium_bps:    Decimal,

    // ── Live Sports Raptor signal snapshot (The Odds API line movement) ──────
    /// Sports Raptor has a fresh cross-book consensus this poll (observe-only).
    pub sports_connected:     bool,
    /// Vig-free consensus implied prob of the tracked event's reference outcome (0..1).
    pub sports_consensus_prob: Decimal,
    /// Δ consensus_prob since the previous poll for the same event (signed line drift).
    pub sports_line_drift:     Decimal,
    /// Spread of per-book implied probs (0..1); high = soft/disagreeing line.
    pub sports_book_dispersion: Decimal,
    /// Number of bookmakers in the sample (0 = no data).
    pub sports_num_books:      Decimal,
    /// Tracked event label, e.g. "Colorado Rockies vs Los Angeles Dodgers".
    #[serde(default)]
    pub sports_event:          String,
    /// The outcome the consensus/drift refer to (first-listed h2h outcome).
    #[serde(default)]
    pub sports_reference:      String,
    /// Sport title from the feed, e.g. "MLB" ("upcoming" mixes sports).
    #[serde(default)]
    pub sports_sport:          String,
    /// ISO-8601 UTC kickoff time of the tracked event.
    #[serde(default)]
    pub sports_commence:       String,
    /// Comma-separated bookmaker titles in the consensus (e.g. "DraftKings, FanDuel").
    #[serde(default)]
    pub sports_books:          String,
}

// ─── Telemetry ring buffer ────────────────────────────────────────────────────

/// One timestamped snapshot of every Raptor signal for a single asset.
///
/// Stored in the server-side ring buffer (`TelemetryHistory`) and served by
/// `GET /api/telemetry/history`, giving the Control Tower Telemetry view durable,
/// scrubable history that survives browser reloads (the live snapshot in
/// `AssetRaptorHealth` only ever holds the latest tick).
#[derive(Serialize, Clone, Debug)]
pub struct TelemetrySample {
    /// Sample time — epoch milliseconds (UTC).
    pub t:             i64,
    pub oracle_price:  Decimal,
    pub velocity_5s:   Decimal,
    pub velocity_1s:   Decimal,
    pub acceleration:  Decimal,
    pub drift_60m:     Decimal,
    pub drift_10m:     Decimal,
    pub funding_rate:  Decimal,
    pub open_interest: Decimal,
    pub oi_delta_pct:  Decimal,
    pub cvd_ratio:     Decimal,
    pub price_connected:   bool,
    pub funding_connected: bool,
    pub deriv_connected:   bool,

    // ── Tide Raptor (Institutional Pulse) ──
    pub tide_connected:      bool,
    pub tide_market_open:    bool,
    pub institutional_pulse: Decimal,
    pub tide_coherence:      Decimal,
    pub ibit_premium_bps:    Decimal,
    pub fbtc_premium_bps:    Decimal,
    pub arkb_premium_bps:    Decimal,

    // ── Sports Raptor (line movement) ──
    pub sports_connected:      bool,
    pub sports_consensus_prob: Decimal,
    pub sports_line_drift:     Decimal,
    pub sports_book_dispersion: Decimal,
    pub sports_num_books:      Decimal,
    #[serde(default)]
    pub sports_event:          String,
    #[serde(default)]
    pub sports_reference:      String,
    #[serde(default)]
    pub sports_sport:          String,
    #[serde(default)]
    pub sports_commence:       String,
    #[serde(default)]
    pub sports_books:          String,
}

/// Per-asset rolling history of telemetry samples.
/// Bounded to `TELEMETRY_HISTORY_CAP` entries per asset by the sampler task.
pub type TelemetryHistory = Arc<Mutex<HashMap<String, VecDeque<TelemetrySample>>>>;

/// Sampler cadence — how often the background task snapshots the live signals.
const TELEMETRY_SAMPLE_SECS: u64 = 2;
/// Retention cap per asset (samples). 1800 × 2s = 1 hour of scrubable history.
const TELEMETRY_HISTORY_CAP: usize = 1800;

/// Background task — every `TELEMETRY_SAMPLE_SECS`, snapshot the current Raptor
/// signal values into the per-asset ring buffer. Spawned once by
/// `run_api_server`; runs for the life of the process.
async fn run_telemetry_sampler(
    raptor_health_rx: watch::Receiver<HashMap<String, AssetRaptorHealth>>,
    history: TelemetryHistory,
) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(TELEMETRY_SAMPLE_SECS));
    loop {
        ticker.tick().await;
        let now = chrono::Utc::now().timestamp_millis();
        let snapshot = raptor_health_rx.borrow().clone();
        if snapshot.is_empty() { continue; }
        let mut hist = match history.lock() {
            Ok(h) => h,
            Err(poisoned) => poisoned.into_inner(),
        };
        for (asset, h) in snapshot.iter() {
            let buf = hist.entry(asset.clone()).or_default();
            buf.push_back(TelemetrySample {
                t: now,
                oracle_price: h.oracle_price,
                velocity_5s:  h.velocity_5s,
                velocity_1s:  h.velocity_1s,
                acceleration: h.acceleration,
                drift_60m:    h.drift_60m,
                drift_10m:    h.drift_10m,
                funding_rate: h.funding_rate,
                open_interest: h.open_interest,
                oi_delta_pct:  h.oi_delta_pct,
                cvd_ratio:     h.cvd_ratio,
                price_connected:   h.price_connected,
                funding_connected: h.funding_connected,
                deriv_connected:   h.deriv_connected,
                tide_connected:      h.tide_connected,
                tide_market_open:    h.tide_market_open,
                institutional_pulse: h.institutional_pulse,
                tide_coherence:      h.tide_coherence,
                ibit_premium_bps:    h.ibit_premium_bps,
                fbtc_premium_bps:    h.fbtc_premium_bps,
                arkb_premium_bps:    h.arkb_premium_bps,
                sports_connected:      h.sports_connected,
                sports_consensus_prob: h.sports_consensus_prob,
                sports_line_drift:     h.sports_line_drift,
                sports_book_dispersion: h.sports_book_dispersion,
                sports_num_books:      h.sports_num_books,
                sports_event:          h.sports_event.clone(),
                sports_reference:      h.sports_reference.clone(),
                sports_sport:          h.sports_sport.clone(),
                sports_commence:       h.sports_commence.clone(),
                sports_books:          h.sports_books.clone(),
            });
            let len = buf.len();
            if len > TELEMETRY_HISTORY_CAP {
                buf.drain(0..len - TELEMETRY_HISTORY_CAP);
            }
        }
    }
}

// ─── Shared state ────────────────────────────────────────────────────────────

/// Cloneable handle passed to every axum handler via `State<ApiState>`.
#[derive(Clone)]
pub struct ApiState {
    /// Broadcast sender — PATCH handler calls `.send()` to hot-reload strategies.
    pub config_tx: Arc<watch::Sender<Arc<DynamicConfig>>>,
    /// Receiver — GET handler reads the latest snapshot without blocking.
    pub config_rx: watch::Receiver<Arc<DynamicConfig>>,
    /// Receiver — maps strategy key ("time_decay", "momentum", …) to current market name.
    pub markets_rx: watch::Receiver<HashMap<String, String>>,
    /// Receiver — maps asset symbol (e.g. "btc") to its pair of Raptor health flags.
    /// Updated by the Price and Funding Raptors in real-time.
    pub raptor_health_rx: watch::Receiver<HashMap<String, AssetRaptorHealth>>,
    /// Optional API key read from `DRADIS_API_KEY` env var at startup.
    /// When `Some`, every request must include `X-API-Key: <value>`.
    /// When `None`, no authentication is required (default for local dev).
    pub api_key: Option<String>,
    /// When true (`DRADIS_READ_ONLY=true`), every mutating request (any method
    /// other than GET/HEAD) is rejected with 403. Powers the public read-only
    /// demo at dradis.live — the live raptor telemetry streams, but no visitor
    /// can patch config, toggle vipers, or exit positions.
    pub read_only: bool,
    /// Gnosis Safe wallet address — used by POST /api/positions/sync to fetch live
    /// on-chain holdings and purge stale open_positions rows without a restart.
    /// Intl-only: the US custodial venue has no self-custody wallet address.
    #[cfg(feature = "intl_clob")]
    pub safe_address: Address,
    /// CAG (Carrier Air Group) — squadron registry.
    /// Phase 3d: exposes GET /api/squadrons and GET /api/squadrons/{id}.
    /// Phase 3f: will also handle POST/DELETE once patrol() is fully wired.
    pub cag: Cag,
    /// Server-side ring buffer of Raptor signal samples (per asset).
    /// Populated by the telemetry sampler task; served by
    /// GET /api/telemetry/history so the UI survives reloads and can scrub.
    pub telemetry_history: TelemetryHistory,
}

// ─── API-key middleware ──────────────────────────────────────────────────────

/// Optional `X-API-Key` authentication gate.
///
/// When `DRADIS_API_KEY` is set in the environment, every request must carry a
/// matching `X-API-Key` header — including requests from OpenClaw or any other
/// external tool.  When the env var is absent the middleware is a no-op, keeping
/// local-dev workflow unchanged.
///
/// CORS pre-flight (`OPTIONS`) requests bypass this check because they are handled
/// by `CorsLayer` (the outer layer) before this middleware is reached.
async fn require_api_key(
    State(s): State<ApiState>,
    req: Request,
    next: Next,
) -> Response {
    if let Some(ref expected) = s.api_key {
        let provided = req
            .headers()
            .get("x-api-key")
            .and_then(|v| v.to_str().ok());
        if provided != Some(expected.as_str()) {
            warn!(" API key rejected — invalid or missing X-API-Key header");
            return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
        }
    }
    next.run(req).await
}

/// Read-only demo gate.
///
/// When `DRADIS_READ_ONLY=true`, any state-mutating request (any HTTP method
/// other than the safe `GET`/`HEAD`) is rejected with `403 Forbidden` and a
/// small JSON body. This is the single server-side chokepoint that makes the
/// public demo safe: even if a visitor bypasses the UI and hits the API
/// directly, no write (config patch, viper toggle, position exit, chain sync)
/// can land. A no-op when the env var is unset/false, so normal deployments are
/// unchanged.
///
/// CORS pre-flight (`OPTIONS`) is handled by the outer `CorsLayer` and never
/// reaches this middleware.
async fn enforce_read_only(
    State(s): State<ApiState>,
    req: Request,
    next: Next,
) -> Response {
    if s.read_only && !matches!(*req.method(), Method::GET | Method::HEAD) {
        warn!(
            " Read-only demo: rejected {} {}",
            req.method(),
            req.uri().path()
        );
        return (
            StatusCode::FORBIDDEN,
            [(header::CONTENT_TYPE, "application/json")],
            r#"{"error":"read-only demo — deploy your own at github.com/mbordash/DRADIS"}"#,
        )
            .into_response();
    }
    next.run(req).await
}


/// Query params for asset-scoped endpoints.
#[derive(Deserialize)]
struct AssetQuery {
    asset: Option<String>,
    limit: Option<i64>,
}

/// Request body for manual "Return to Base" exit.
///
/// POST /api/positions/manual-exit
///
/// Executes an immediate FAK (Fill-And-Kill) market sell order for the
/// specified position, records the trade with actual exit price and P&L,
/// and closes the position in the database.
#[derive(Deserialize)]
#[cfg(feature = "intl_clob")]
struct ManualExitRequest {
    /// Token ID (decimal U256 string)
    token_id: String,
    /// Asset symbol (e.g. "btc") for DB pool selection
    asset: String,
    /// Strategy name for position lookup
    strategy: String,
    /// Market name for trade recording
    market: String,
    /// Side (YES/NO) for trade recording
    side: String,
    /// Current bid price (for FAK sell order)
    current_bid: String,
    /// Verifying contract address (exchange address)
    verifying_contract: String,
}

// ─── Handlers ────────────────────────────────────────────────────────────────

/// GET /api/health
async fn health() -> &'static str {
    debug!("Received GET /api/health request");
    "ok"
}

/// GET /api/assets
///
/// Returns the list of asset symbols for which a SQLite pool has been
/// initialised, sorted alphabetically.  The Control Tower uses this to
/// populate the asset selector tabs.
///
/// Response: `["btc", "eth", "sol"]`
async fn get_assets() -> Response {
    debug!("Received GET /api/assets request");
    Json(db::available_assets()).into_response()
}

/// GET /api/config
///
/// Returns the full DynamicConfig as a flat JSON object.
/// Field names match the struct fields (snake_case).
async fn get_config(State(s): State<ApiState>) -> Response {
    debug!("Received GET /api/config request");
    let cfg = s.config_rx.borrow().clone();
    match serde_json::to_value(cfg.as_ref()) {
        Ok(val) => {
            debug!("Successfully processed GET /api/config");
            (StatusCode::OK, Json(val)).into_response()
        },
        Err(e)  => {
            error!("Error processing GET /api/config: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        },
    }
}

/// PATCH /api/config
///
/// Body: a partial JSON object with only the fields to change, e.g.
///   `{"ghost_mode": false, "basis_stop_loss_pct": "0.08"}`
///
/// Uses JSON merge-patch semantics: unknown keys are ignored.
/// On success, broadcasts the new config on the watch channel so all
/// in-flight strategy tick contexts pick it up within 50 ms.
async fn patch_config(State(s): State<ApiState>, body: String) -> Response {
    debug!("Received PATCH /api/config request with body: {}", body);
    let current = s.config_rx.borrow().clone();
    match DynamicConfig::apply_patch(&current, &body).await {
        Ok(new_cfg) => {
            // Broadcast to all strategy tick loops.
            let _ = s.config_tx.send(new_cfg.clone());
            match serde_json::to_value(new_cfg.as_ref()) {
                Ok(val) => {
                    debug!("Successfully processed PATCH /api/config");
                    (StatusCode::OK, Json(val)).into_response()
                },
                Err(e)  => {
                    error!("Error serializing new config after PATCH /api/config: {}", e);
                    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                },
            }
        }
        Err(e) => {
            error!("Error applying patch for PATCH /api/config: {}", e);
            (StatusCode::BAD_REQUEST, e.to_string()).into_response()
        },
    }
}

/// GET /api/pnl/history?limit=200&asset=btc
///
/// Returns up to `limit` P&L snapshots, newest first.
/// Each row: { ts, session_pnl, collateral, total_value }
///
/// When `asset` query param is omitted, returns aggregated global P&L history
/// (collateral + sum of all assets' positions_value per timestamp).
async fn get_pnl_history(Query(q): Query<AssetQuery>) -> Response {
    debug!("Received GET /api/pnl/history request with limit: {:?}, asset: {:?}", q.limit, q.asset);
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);

    // If asset is specified, return single-asset history (legacy behavior)
    if let Some(asset_name) = q.asset.as_deref() {
        match db::pool_for_opt(Some(asset_name)) {
            Some(pool) => {
                let history = db::get_pnl_history(&pool, limit).await;
                debug!("Successfully retrieved PNL history for asset: {}", asset_name);
                return Json(history).into_response();
            },
            None => {
                error!("Database pool not available for asset: {}", asset_name);
                return Json(Vec::<db::PnlSnapshotRow>::new()).into_response();
            },
        }
    }

    // No asset specified → return aggregated global P&L history
    use rust_decimal::Decimal;
    use std::str::FromStr;

    let assets = db::available_assets();
    if assets.is_empty() {
        warn!("GET /api/pnl/history (global): no assets available");
        return Json(Vec::<db::PnlSnapshotRow>::new()).into_response();
    }

    // Fetch snapshots from all assets
    let mut all_snapshots: Vec<(String, Vec<db::PnlSnapshotRow>)> = vec![];
    for asset in &assets {
        if let Some(pool) = db::pool_for(asset) {
            let snaps = db::get_pnl_history(&pool, limit).await;
            all_snapshots.push((asset.clone(), snaps));
        }
    }

    if all_snapshots.is_empty() {
        warn!("GET /api/pnl/history (global): no snapshots from any asset");
        return Json(Vec::<db::PnlSnapshotRow>::new()).into_response();
    }

    // Use primary asset's timestamps as the base timeline
    let primary_snaps = &all_snapshots[0].1;

    // For each primary timestamp, aggregate positions_value from all assets
    let aggregated: Vec<db::PnlSnapshotRow> = primary_snaps.iter().map(|primary| {
        let ts = &primary.ts;
        let collateral = &primary.collateral;

        // Find nearest snapshot from each asset within ±2 minutes of this timestamp
        let window_secs = 120;
        let primary_time = chrono::DateTime::parse_from_rfc3339(ts)
            .map(|dt| dt.timestamp())
            .unwrap_or(0);

        let mut total_positions_value = Decimal::ZERO;
        let mut total_session_pnl = Decimal::ZERO;

        for (asset, snaps) in &all_snapshots {
            // Find closest snapshot within window
            if let Some(snap) = snaps.iter().find(|s| {
                chrono::DateTime::parse_from_rfc3339(&s.ts)
                    .map(|dt| (dt.timestamp() - primary_time).abs() <= window_secs)
                    .unwrap_or(false)
            }) {
                // Extract positions_value = total_value - collateral
                if let (Some(tv_str), Ok(coll)) = (
                    snap.total_value.as_ref(),
                    Decimal::from_str(&snap.collateral),
                ) {
                    if let Ok(tv) = Decimal::from_str(tv_str) {
                        let pos_val = (tv - coll).max(Decimal::ZERO);
                        total_positions_value += pos_val;
                        debug!("[{}] @ {} positions_value={}", asset, ts, pos_val);
                    }
                }

                // Sum session P&L from each asset
                if let Ok(pnl) = Decimal::from_str(&snap.session_pnl) {
                    total_session_pnl += pnl;
                }
            }
        }

        // Global total = collateral + sum(positions across all assets)
        let coll_dec = Decimal::from_str(collateral).unwrap_or(Decimal::ZERO);
        let global_total = coll_dec + total_positions_value;

        db::PnlSnapshotRow {
            ts: ts.clone(),
            session_pnl: total_session_pnl.to_string(),
            collateral: collateral.clone(),
            total_value: Some(global_total.to_string()),
        }
    }).collect();

    debug!("Successfully retrieved global aggregated PNL history ({} points)", aggregated.len());
    Json(aggregated).into_response()
}

/// GET /api/status
///
/// Returns the current market name each strategy is attached to, the session
/// start timestamp (RFC-3339), and per-asset Raptor connection health.
///
/// Response:
/// ```json
/// {
///   "strategy_markets": { "time_decay": "Will BTC…", … },
///   "session_started_at": "2026-06-02T14:32:01Z",
///   "raptors": {
///     "btc": { "price_connected": true, "funding_connected": true }
///   }
/// }
/// ```
#[derive(Serialize)]
struct StatusResponse {
    strategy_markets: HashMap<String, String>,
    /// RFC-3339 timestamp of the current session start (= process startup).
    session_started_at: String,
    /// Per-asset Binance Raptor connection health.
    raptors: HashMap<String, AssetRaptorHealth>,
}

async fn get_status(State(s): State<ApiState>) -> Response {
    debug!("Received GET /api/status request");
    let markets = s.markets_rx.borrow().clone();
    let raptors = s.raptor_health_rx.borrow().clone();
    let session_started_at = db::current_session_id().to_string();
    debug!("Successfully retrieved status");
    Json(StatusResponse { strategy_markets: markets, session_started_at, raptors }).into_response()
}

/// GET /api/telemetry
///
/// Returns the live signal snapshot for every asset's Raptors — the same
/// `AssetRaptorHealth` map exposed under `/api/status.raptors`, but on a
/// dedicated lightweight endpoint the Control Tower Telemetry view can poll at
/// a high cadence (every ~2 s) to build rolling signal graphs.
///
/// Response (keyed by asset symbol):
/// ```json
/// {
///   "btc": {
///     "price_connected": true, "funding_connected": true,
///     "oracle_price": 64210.5, "velocity_5s": 12.3, "velocity_1s": 4.1,
///     "acceleration": 1.2, "drift_60m": 305.0, "drift_10m": 88.0,
///     "funding_rate": 0.00012
///   }
/// }
/// ```
async fn get_telemetry(State(s): State<ApiState>) -> Response {
    debug!("Received GET /api/telemetry request");
    let raptors = s.raptor_health_rx.borrow().clone();
    Json(raptors).into_response()
}

/// Query params for telemetry history.
#[derive(Deserialize)]
struct TelemetryHistoryQuery {
    asset: Option<String>,
    limit: Option<usize>,
}

/// GET /api/telemetry/history?asset=btc&limit=900
///
/// Returns up to `limit` most-recent telemetry samples (oldest→newest) for the
/// given asset from the server-side ring buffer. Defaults to the primary asset
/// and the full retained window (1 hour). This durable history lets the Control
/// Tower Telemetry view survive reloads and scrub back over past signal windows.
async fn get_telemetry_history(
    State(s): State<ApiState>,
    Query(q): Query<TelemetryHistoryQuery>,
) -> Response {
    debug!("Received GET /api/telemetry/history request");
    let limit = q.limit.unwrap_or(TELEMETRY_HISTORY_CAP).clamp(1, TELEMETRY_HISTORY_CAP);
    let hist = match s.telemetry_history.lock() {
        Ok(h) => h,
        Err(poisoned) => poisoned.into_inner(),
    };
    // Resolve asset: explicit param (lowercased), else the first (primary) key.
    let key = q.asset
        .map(|a| a.to_lowercase())
        .or_else(|| hist.keys().next().cloned());
    let samples: Vec<TelemetrySample> = match key.as_deref().and_then(|k| hist.get(k)) {
        Some(buf) => {
            let start = buf.len().saturating_sub(limit);
            buf.iter().skip(start).cloned().collect()
        }
        None => Vec::new(),
    };
    Json(samples).into_response()
}

/// GET /api/trades?limit=100&asset=btc
///
/// Returns up to `limit` completed trades, newest first.
/// Each row: { ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason }
async fn get_trades(Query(q): Query<AssetQuery>) -> Response {
    debug!("Received GET /api/trades request with limit: {:?}", q.limit);
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    match db::pool_for_opt(q.asset.as_deref()) {
        Some(pool) => {
            let trades = db::get_recent_trades(&pool, limit).await;
            debug!("Successfully retrieved trades");
            Json(trades).into_response()
        },
        None       => {
            error!("Database pool not available for GET /api/trades");
            Json(Vec::<db::TradeRow>::new()).into_response()
        },
    }
}

/// GET /api/positions?asset=btc
///
/// Returns all currently open positions for this session (inserted on entry, removed on exit).
/// Covers all strategies and both ghost/live modes so the UI always has a complete picture
/// of in-flight positions even before they appear as completed trades.
async fn get_open_positions(Query(q): Query<AssetQuery>) -> Response {
    debug!("Received GET /api/positions request");
    match db::pool_for_opt(q.asset.as_deref()) {
        Some(pool) => {
            let positions = db::get_open_positions(&pool).await;
            Json(positions).into_response()
        },
        None => {
            error!("Database pool not available for GET /api/positions");
            Json(Vec::<db::OpenPositionRow>::new()).into_response()
        },
    }
}

/// GET /api/positions/pending?asset=btc
///
/// Returns only pending positions (Viper Launches) - orders placed but not yet confirmed on-chain.
async fn get_pending_positions(Query(q): Query<AssetQuery>) -> Response {
    debug!("Received GET /api/positions/pending request");
    match db::pool_for_opt(q.asset.as_deref()) {
        Some(pool) => {
            let positions = db::get_pending_positions(&pool).await;
            Json(positions).into_response()
        },
        None => {
            error!("Database pool not available for GET /api/positions/pending");
            Json(Vec::<db::OpenPositionRow>::new()).into_response()
        },
    }
}

/// GET /api/positions/confirmed?asset=btc
///
/// Returns only confirmed positions (Viper Missions In-Flight) - verified on-chain.
async fn get_confirmed_positions(Query(q): Query<AssetQuery>) -> Response {
    debug!("Received GET /api/positions/confirmed request");
    match db::pool_for_opt(q.asset.as_deref()) {
        Some(pool) => {
            let positions = db::get_confirmed_positions(&pool).await;
            Json(positions).into_response()
        },
        None => {
            error!("Database pool not available for GET /api/positions/confirmed");
            Json(Vec::<db::OpenPositionRow>::new()).into_response()
        },
    }
}

/// DELETE /api/positions/{token_id}?asset=btc
///
/// Purges a specific row from `open_positions` by token_id (decimal U256 string).
async fn delete_open_position(Path(token_id): Path<String>, Query(q): Query<AssetQuery>) -> Response {
    debug!("Received DELETE /api/positions/{}", token_id);
    let pool = match db::pool_for_opt(q.asset.as_deref()) {
        Some(p) => p,
        None => {
            error!("Database pool not available for DELETE /api/positions");
            return (StatusCode::SERVICE_UNAVAILABLE, "DB unavailable").into_response();
        }
    };
    match sqlx::query("DELETE FROM open_positions WHERE token_id = ?")
        .bind(&token_id)
        .execute(&pool)
        .await
    {
        Ok(r) if r.rows_affected() > 0 => {
            info!("️ Purged stale open_position row for token {}", &token_id[..token_id.len().min(20)]);
            (StatusCode::OK, format!("Deleted {} row(s)", r.rows_affected())).into_response()
        }
        Ok(_) => {
            warn!("DELETE /api/positions/{}: token_id not found in open_positions", token_id);
            (StatusCode::NOT_FOUND, "token_id not found").into_response()
        }
        Err(e) => {
            error!("DELETE /api/positions/{} DB error: {}", token_id, e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// POST /api/positions/sync
///
/// Triggers an immediate two-way reconciliation of the `open_positions` DB table
/// against the wallet's live on-chain holdings via the Polymarket Data API:
///
///   PURGE  — removes rows for tokens no longer held on-chain (settled, expired,
///            redeemed, or sold externally on the Polymarket UI).
///   ADOPT  — re-inserts on-chain positions that have no DB row.
///
/// Normally runs automatically at startup and every 300 s.  Call this endpoint
/// after manually "clearing" settled losses in the Polymarket UI to immediately
/// reflect the cleared state in DRADIS without waiting for a bot restart.
///
/// Returns: `{ "message": "Chain sync complete" }`
#[cfg(feature = "intl_clob")]
async fn sync_positions(State(s): State<ApiState>) -> Response {
    info!(" Manual chain-sync triggered via POST /api/positions/sync");
    sync_open_positions_with_chain(s.safe_address).await;
    (StatusCode::OK, Json(serde_json::json!({ "message": "Chain sync complete" }))).into_response()
}

/// POST /api/positions/manual-exit
///
/// Execute a manual "Return to Base" exit for a specific position.
///
/// Flow:
///  1. Lookup position in DB to get entry price and shares
///  2. Place FAK (Fill-And-Kill) market sell order at current bid
///  3. Wait up to 10s for order to fill
///  4. Record trade with actual exit price and P&L
///  5. Close position in DB
///
/// Returns 200 with trade details on success, 4xx/5xx on failure.
#[cfg(feature = "intl_clob")]
async fn manual_exit(
    State(s): State<ApiState>,
    Json(req): Json<ManualExitRequest>,
) -> Response {
    info!(" RTB: Manual exit request for token {} [{}]", &req.token_id[..req.token_id.len().min(20)], req.strategy);

    // ── Step 1: Get session for this asset ────────────────────────────────────
    let session = match s.cag.session_for_asset(&req.asset) {
        Some(sess) => sess,
        None => {
            warn!("RTB: Asset '{}' not found in CAG sessions", req.asset);
            return (StatusCode::BAD_REQUEST, "Asset not found").into_response();
        }
    };

    // ── Step 2: Lookup position in DB to get entry price and shares ───────────
    let pool = match db::pool_for(&req.asset) {
        Some(p) => p,
        None => {
            error!("RTB: Database pool not available for asset {}", req.asset);
            return (StatusCode::SERVICE_UNAVAILABLE, "DB unavailable").into_response();
        }
    };

    #[derive(sqlx::FromRow)]
    struct PositionRow {
        entry_price: String,
        shares: String,
    }

    let pos_result = sqlx::query_as::<_, PositionRow>(
        "SELECT entry_price, shares FROM open_positions WHERE token_id = ? AND strategy = ?"
    )
    .bind(&req.token_id)
    .bind(&req.strategy)
    .fetch_one(&pool)
    .await;

    let (entry_price, shares) = match pos_result {
        Ok(row) => {
            let entry = match Decimal::from_str(&row.entry_price) {
                Ok(p) => p,
                Err(e) => {
                    error!("RTB: Invalid entry_price in DB: {}", e);
                    return (StatusCode::INTERNAL_SERVER_ERROR, "Invalid entry price").into_response();
                }
            };
            let shares = match Decimal::from_str(&row.shares) {
                Ok(s) => s,
                Err(e) => {
                    error!("RTB: Invalid shares in DB: {}", e);
                    return (StatusCode::INTERNAL_SERVER_ERROR, "Invalid shares").into_response();
                }
            };
            (entry, shares)
        }
        Err(sqlx::Error::RowNotFound) => {
            warn!("RTB: Position not found in DB (token={}, strategy={})", req.token_id, req.strategy);
            return (StatusCode::NOT_FOUND, "Position not found").into_response();
        }
        Err(e) => {
            error!("RTB: Database error: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, "Database error").into_response();
        }
    };

    // ── Step 3: Parse inputs ───────────────────────────────────────────────────
    let token_id = match U256::from_str(&req.token_id) {
        Ok(t) => t,
        Err(e) => {
            error!("RTB: Invalid token_id: {}", e);
            return (StatusCode::BAD_REQUEST, "Invalid token_id").into_response();
        }
    };

    let current_bid = match Decimal::from_str(&req.current_bid) {
        Ok(b) => b,
        Err(e) => {
            error!("RTB: Invalid current_bid: {}", e);
            return (StatusCode::BAD_REQUEST, "Invalid current_bid").into_response();
        }
    };

    let verifying_contract = match req.verifying_contract.parse::<Address>() {
        Ok(a) => a,
        Err(e) => {
            error!("RTB: Invalid verifying_contract: {}", e);
            return (StatusCode::BAD_REQUEST, "Invalid verifying_contract").into_response();
        }
    };

    // ── Step 4: Place FAK market sell order ────────────────────────────────────
    let sell_price = round_to_tick_size(current_bid);
    info!(" RTB: Placing FAK sell order — {} shares @ ${:.4}", shares, sell_price);

    let order_result = place_limit_order(
        session.venue.trading_client(),
        session.venue.nonce_manager(),
        session.venue.signer(),
        s.safe_address,
        s.safe_address, // eoa_address = safe_address for this context
        verifying_contract,
        &crate::venues::intl::market_id_from_u256(token_id),
        Side::Sell,
        shares,
        sell_price,
        0, // fee_rate_bps (unused in V2)
        crate::venues::core::TimeInForce::Fak,
        false, // not post-only
        0, // expiration_secs (FAK doesn't need expiration)
        session.venue.shared_http(),
    ).await;

    let order_id = match order_result {
        Ok(id) => {
            info!("✅ RTB: FAK sell order placed (order_id={})", id);
            id
        }
        Err(e) => {
            error!("RTB: Order placement failed: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("Order failed: {}", e)).into_response();
        }
    };

    // ── Step 5: Wait for fill confirmation ─────────────────────────────────────
    // FAK orders fill immediately or cancel. Give it 10s to confirm on-chain.
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    // ── Step 6: Calculate P&L and record trade ────────────────────────────────
    let pnl = (sell_price - entry_price) * shares;
    info!(" RTB: Trade recorded — {} shares | entry ${:.4} → exit ${:.4} | P&L ${:.4}",
          shares, entry_price, sell_price, pnl);

    metrics::record_trade(
        &req.asset,
        req.strategy.clone(),
        req.market.clone(),
        req.side.clone(),
        entry_price,
        sell_price,
        shares,
        pnl,
        "Manual RTB (Return to Base)".to_string(),
    ).await;

    // ── Step 7: Close position in DB ───────────────────────────────────────────
    db::close_open_position(&pool, &req.strategy, &req.token_id).await;

    // ── Step 8: Remove from in-memory positions map ────────────────────────────
    {
        let mut pos_map = session.positions.lock().await;
        pos_map.remove(&(req.strategy.clone(), crate::venues::intl::market_id_from_u256(token_id)));
    }

    info!("✅ RTB: Manual exit complete — order_id={}", order_id);

    #[derive(Serialize)]
    struct ExitResponse {
        success: bool,
        order_id: String,
        exit_price: String,
        shares: String,
        pnl: String,
    }

    Json(ExitResponse {
        success: true,
        order_id,
        exit_price: sell_price.to_string(),
        shares: shares.to_string(),
        pnl: pnl.to_string(),
    }).into_response()
}

/// GET /api/llm/recommendations?limit=10&asset=btc
///
/// Returns up to `limit` LLM Advisor analyses, newest first.
/// Each row: { id, ts, model, trade_count, session_pnl, analysis }
async fn get_llm_recommendations(Query(q): Query<AssetQuery>) -> Response {    debug!("Received GET /api/llm/recommendations request with limit: {:?}", q.limit);
    let limit = q.limit.unwrap_or(10).clamp(1, 50);
    match db::pool_for_opt(q.asset.as_deref()) {
        Some(pool) => {
            let recs = db::get_recent_llm_recommendations(&pool, limit).await;
            debug!("Successfully retrieved {} LLM recommendations", recs.len());
            Json(recs).into_response()
        },
        None => {
            error!("Database pool not available for GET /api/llm/recommendations");
            Json(Vec::<db::LlmRecommendationRow>::new()).into_response()
        },
    }
}

/// GET /api/portfolio
///
/// Returns aggregated portfolio value across all assets:
/// - collateral: total pUSD cash
/// - positions_value: sum of (shares × current_mid_price) for all open positions
/// - total_value: collateral + positions_value
/// - unrealized_pnl: sum of (shares × (current_mid - entry_price))
/// - position_count: total number of open positions
/// - prices_live: true if CLOB prices are fresh
///
/// This endpoint aggregates data from all asset pools (BTC, ETH, SOL, etc.)
#[derive(Serialize)]
struct PortfolioValue {
    collateral: String,
    positions_value: String,
    total_value: String,
    unrealized_pnl: String,
    position_count: usize,
    prices_live: bool,
}

async fn get_portfolio_value(State(s): State<ApiState>) -> Response {
    debug!("Received GET /api/portfolio request");
    // `s` is only consulted for the intl on-chain balance probe below.
    #[cfg(feature = "us_retail")]
    let _ = &s;

    use rust_decimal::Decimal;
    use std::str::FromStr;
    use chrono::{Utc, Duration};

    let assets = db::available_assets();

    // Fetch live wallet collateral as ground truth (10s timeout)
    // US custodial venue exposes no on-chain wallet balance here yet (Step 3b);
    // fall back to the DB-tracked collateral snapshot below.
    #[cfg(feature = "us_retail")]
    let live_collateral: Option<Decimal> = None;
    #[cfg(feature = "intl_clob")]
    let live_collateral = {
        use polymarket_client_sdk_v2::clob::types::request::BalanceAllowanceRequest;
        use polymarket_client_sdk_v2::clob::types::AssetType;

        // Get first available session to access trading_client
        let session = assets.iter().find_map(|a| s.cag.session_for_asset(a));

        if let Some(sess) = session {
            let mut req = BalanceAllowanceRequest::default();
            req.asset_type = AssetType::Collateral;

            match tokio::time::timeout(
                std::time::Duration::from_secs(10),
                sess.venue.trading_client().balance_allowance(req),
            ).await {
                Ok(Ok(resp)) => {
                    let balance = Decimal::from_str(&resp.balance.to_string())
                        .unwrap_or(Decimal::ZERO) / Decimal::from_str("1000000").unwrap();
                    debug!(" Live wallet collateral from CLOB: ${:.4}", balance);
                    Some(balance)
                }
                Ok(Err(e)) => {
                    warn!("⚠️ CLOB balance fetch failed in /api/portfolio: {}", e);
                    None
                }
                Err(_) => {
                    warn!("⚠️ CLOB balance fetch timed out (10s) in /api/portfolio");
                    None
                }
            }
        } else {
            None
        }
    };

    let mut latest_collateral: Option<(String, Decimal)> = None;
    let mut total_positions_value = Decimal::ZERO;
    let mut total_unrealized_pnl = Decimal::ZERO;
    let mut total_position_count = 0;
    let mut all_prices_live = true;

    // Freshness threshold: snapshots older than this are considered stale
    let freshness_threshold = Utc::now() - Duration::minutes(5);

    // Aggregate across all asset pools
    for asset in &assets {
        let pool = match db::pool_for(asset) {
            Some(p) => p,
            None => continue,
        };

        // Collateral is wallet-global, not asset-scoped. Each asset DB stores the
        // same wallet cash snapshot, so we keep only the freshest one (but we'll
        // prefer live_collateral from CLOB if available).
        let pnl_snapshots = db::get_pnl_history(&pool, 1).await;
        if let Some(snap) = pnl_snapshots.first() {
            if let Ok(collateral) = Decimal::from_str(&snap.collateral) {
                match &latest_collateral {
                    Some((ts, _)) if ts >= &snap.ts => {}
                    _ => latest_collateral = Some((snap.ts.clone(), collateral)),
                }
            }
        }

        // Build a deduped view of positions by token_id for count/fallback valuation.
        // The on-chain wallet holds ONE balance per token, so rows that share a
        // token_id (e.g. an Arbitrage leg and a TrendCapture leg on the same outcome)
        // must be valued ONCE — never summed (double-count) nor arbitrarily dropped.
        //
        // Prefer the CHAIN-ADOPTED row: chain-sync stamps it to the wallet's real
        // on-chain size (and purges it when the token is no longer held), so it is the
        // authoritative reflection of holdings. A non-adopted strategy row may be a
        // phantom that never settled on-chain (e.g. a same-token leg that overlaps an
        // existing position). Among rows with equal adoption status, prefer larger
        // shares. The genuinely-additive case self-heals on the next chain-sync, which
        // stamps every row for the token to the full on-chain size.
        let mut deduped_positions: std::collections::HashMap<String, db::OpenPositionRow> =
            std::collections::HashMap::new();
        for pos in db::get_open_positions(&pool).await {
            // Skip UNCONFIRMED phantoms: still `status='pending'` AND not chain-adopted
            // means the order was placed but never confirmed on-chain (never filled or
            // rejected). Valuing it inflates the portfolio with non-existent profit until
            // the 60-min purge grace elapses. Mirrors calculate_positions_value() so the
            // banner and snapshots stay one source of truth.
            if pos.status == "pending" && !pos.chain_adopted {
                continue;
            }
            match deduped_positions.get(&pos.token_id) {
                None => {
                    deduped_positions.insert(pos.token_id.clone(), pos);
                }
                Some(existing) => {
                    let existing_shares = Decimal::from_str(&existing.shares).unwrap_or(Decimal::ZERO);
                    let candidate_shares = Decimal::from_str(&pos.shares).unwrap_or(Decimal::ZERO);
                    let replace = (!existing.chain_adopted && pos.chain_adopted)
                        || (existing.chain_adopted == pos.chain_adopted && candidate_shares > existing_shares);
                    if replace {
                        deduped_positions.insert(pos.token_id.clone(), pos);
                    }
                }
            }
        }
        total_position_count += deduped_positions.len();

        // Check snapshot freshness before trusting mark-to-market valuation
        let snapshot_is_fresh = pnl_snapshots.first().and_then(|snap| {
            chrono::DateTime::parse_from_rfc3339(&snap.ts)
                .ok()
                .map(|dt| dt.with_timezone(&Utc) > freshness_threshold)
        }).unwrap_or(false);

        // Compute positions value and unrealized P&L from deduped positions.
        // Prefer current_price (live mark-to-market from chain-sync) when available.
        // Fall back to fresh pnl_snapshot total_value - collateral, then to cost basis.
        let mut asset_positions_value = Decimal::ZERO;
        let mut asset_unrealized_pnl = Decimal::ZERO;
        let mut has_live_prices = false;

        for (_, pos) in &deduped_positions {
            if let (Ok(shares), Ok(entry_price)) = (
                Decimal::from_str(&pos.shares),
                Decimal::from_str(&pos.entry_price),
            ) {
                let cost_basis = shares * entry_price;
                if let Some(ref cp_str) = pos.current_price {
                    if let Ok(cur_price) = Decimal::from_str(cp_str) {
                        if cur_price > Decimal::ZERO {
                            let market_value = shares * cur_price;
                            asset_positions_value += market_value;
                            asset_unrealized_pnl += market_value - cost_basis;
                            has_live_prices = true;
                            debug!(" [{}] token {} {} shares × cur=${:.4} = ${:.4} (entry=${:.4} pnl={:+.4})",
                                   asset.to_uppercase(), &pos.token_id[..pos.token_id.len().min(12)],
                                   shares, cur_price, market_value, entry_price,
                                   market_value - cost_basis);
                            continue;
                        }
                    }
                }
                // No current_price — fall through to snapshot or cost basis below
                // (tracked separately so we can mix per-position accuracy)
                asset_positions_value += cost_basis;
            }
        }

        if !has_live_prices && deduped_positions.is_empty() {
            // No positions — nothing to value
        } else if !has_live_prices {
            // No current_price on any position — try snapshot, then cost basis
            if snapshot_is_fresh {
                if let Some(snap) = pnl_snapshots.first() {
                    if let (Some(tv), Ok(collateral)) = (
                        snap.total_value.as_ref().and_then(|v| Decimal::from_str(v).ok()),
                        Decimal::from_str(&snap.collateral),
                    ) {
                        asset_positions_value = (tv - collateral).max(Decimal::ZERO);
                        debug!("✅ [{}] Fresh snapshot (no cur_price): positions_value = ${:.4}",
                               asset.to_uppercase(), asset_positions_value);
                    }
                }
            } else {
                all_prices_live = false;
                warn!("⚠️ [{}] No current_price and stale/missing snapshot — using cost basis",
                      asset.to_uppercase());
            }
        }

        if !has_live_prices {
            all_prices_live = false;
        }

        total_positions_value += asset_positions_value;
        total_unrealized_pnl  += asset_unrealized_pnl;
        debug!(" [{}] positions=${:.4} unrealized_pnl={:+.4}",
               asset.to_uppercase(), asset_positions_value, asset_unrealized_pnl);
    }

    // Use live CLOB collateral if available, otherwise fall back to latest snapshot
    let total_collateral = if let Some(live_bal) = live_collateral {
        live_bal
    } else {
        all_prices_live = false;
        latest_collateral
            .map(|(_, c)| c)
            .unwrap_or(Decimal::ZERO)
    };

    let total_value = total_collateral + total_positions_value;

    debug!(" Portfolio summary: collateral=${:.4} positions=${:.4} total=${:.4} count={} live={}",
           total_collateral, total_positions_value, total_value, total_position_count, all_prices_live);

    Json(PortfolioValue {
        collateral: total_collateral.to_string(),
        positions_value: total_positions_value.to_string(),
        total_value: total_value.to_string(),
        unrealized_pnl: total_unrealized_pnl.to_string(),
        position_count: total_position_count,
        prices_live: all_prices_live,
    }).into_response()
}

// ─── Squadron handlers (Phase 3d) ────────────────────────────────────────────

/// GET /api/squadrons
///
/// Returns a JSON array of all currently registered squadrons, sorted by
/// deployment time (oldest first).  Each entry is a `SquadronSummary`:
///
/// ```json
/// [
///   {
///     "id":          "btc-hourly-2026-05-29T14:00:00Z",
///     "asset":       "BTC",
///     "name":        "Full Wing — Will BTC …",
///     "state":       "PATROLLING",
///     "market_name": "Will BTC exceed $70,000 at 3 PM ET?",
///     "deployed_at": "2026-05-29T14:00:01Z"
///   }
/// ]
/// ```
async fn get_squadrons(State(s): State<ApiState>) -> Response {
    debug!("Received GET /api/squadrons");
    let mut list = s.cag.list_squadrons();
    for summary in &mut list {
        enrich_taxonomy(summary).await;
    }
    Json(list).into_response()
}

/// GET /api/squadrons/{id}
///
/// Returns the `SquadronSummary` for a single squadron, or 404 if unknown.
async fn get_squadron_by_id(
    State(s): State<ApiState>,
    Path(id): Path<String>,
) -> Response {
    debug!("Received GET /api/squadrons/{}", id);
    match s.cag.get_squadron(&id) {
        Some(mut summary) => {
            enrich_taxonomy(&mut summary).await;
            Json(summary).into_response()
        }
        None => {
            warn!("GET /api/squadrons/{}: not found", id);
            (StatusCode::NOT_FOUND, format!("squadron '{}' not found", id)).into_response()
        }
    }
}

/// Populate a squadron summary's market taxonomy (`market_class` + the
/// `raptors`/`vipers` meaningful for it) from the DB at request time.
///
/// The class is resolved once at registration by `Squadron::classify_and_link`
/// and persisted on the `squadron_configs` row; here we read it back and expand
/// it through the join tables so the UI can render data-driven cards instead of
/// a hardcoded set. Falls back to `"unknown"` (→ venue-agnostic vipers only).
async fn enrich_taxonomy(summary: &mut crate::cag::SquadronSummary) {
    let Some(pool) = db::pool() else { return };
    let class = db::get_squadron_market_class(pool, &summary.id)
        .await
        .unwrap_or_else(|| "unknown".to_string());
    summary.raptors = db::raptors_for_class(pool, &class).await;
    summary.vipers = db::vipers_for_class(pool, &class).await;
    summary.market_class = class;
}

/// GET /api/config/schema
///
/// Returns the editable-config field schema — the single source of truth describing
/// every `DynamicConfig` field (group, label, type, unit, min/max, advanced flag).
/// The Control Tower renders Basic panels + the Advanced modal from this, so new
/// Rust config fields surface automatically without a hand-maintained frontend list.
async fn get_config_schema() -> Response {
    debug!("Received GET /api/config/schema");
    Json(crate::api::config_schema::config_schema()).into_response()
}

/// GET /api/squadrons/{id}/config
///
/// Returns the squadron's DynamicConfig as JSON, or 404 if squadron not found.
async fn get_squadron_config(
    Path(id): Path<String>,
) -> Response {
    debug!("Received GET /api/squadrons/{}/config", id);
    match db::pool() {
        Some(pool) => {
            if let Some(json) = db::squadron_config_get(&pool, &id).await {
                match serde_json::from_str::<DynamicConfig>(&json) {
                    Ok(cfg) => match serde_json::to_value(&cfg) {
                        Ok(val) => {
                            debug!("Successfully retrieved squadron config for {}", id);
                            (StatusCode::OK, Json(val)).into_response()
                        },
                        Err(e) => {
                            error!("Error serializing squadron config for {}: {}", id, e);
                            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                        },
                    },
                    Err(e) => {
                        error!("Error parsing squadron config for {}: {}", id, e);
                        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                    },
                }
            } else {
                warn!("GET /api/squadrons/{}/config: not found", id);
                (StatusCode::NOT_FOUND, format!("squadron '{}' config not found", id)).into_response()
            }
        },
        None => {
            error!("Database pool not available for GET /api/squadrons/{}/config", id);
            (StatusCode::INTERNAL_SERVER_ERROR, "database unavailable").into_response()
        },
    }
}

/// PATCH /api/squadrons/{id}/config
///
/// Body: a partial JSON object with only the fields to change, e.g.
///   `{"time_decay_position_size_usdc": "8.0"}`
///
/// Applies squadron-specific config changes.
async fn patch_squadron_config(
    Path(id): Path<String>,
    body: String,
) -> Response {
    debug!("Received PATCH /api/squadrons/{}/config with body: {}", id, body);
    match DynamicConfig::apply_squadron_patch(&id, &body).await {
        Ok(new_cfg) => {
            match serde_json::to_value(new_cfg.as_ref()) {
                Ok(val) => {
                    debug!("Successfully patched squadron config for {}", id);
                    (StatusCode::OK, Json(val)).into_response()
                },
                Err(e) => {
                    error!("Error serializing new squadron config for {}: {}", id, e);
                    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                },
            }
        },
        Err(e) => {
            error!("Error applying patch for squadron {}: {}", id, e);
            (StatusCode::BAD_REQUEST, e.to_string()).into_response()
        },
    }
}

// ─── Server startup ──────────────────────────────────────────────────────────

/// Spawn the Control Tower axum server.
///
/// Call once from `main()` via `tokio::spawn(run_api_server(...))`.
/// The function runs forever; errors are logged but do not crash the process.
pub async fn run_api_server(
    config_tx: Arc<watch::Sender<Arc<DynamicConfig>>>,
    config_rx: watch::Receiver<Arc<DynamicConfig>>,
    markets_rx: watch::Receiver<HashMap<String, String>>,
    raptor_health_rx: watch::Receiver<HashMap<String, AssetRaptorHealth>>,
    #[cfg(feature = "intl_clob")] safe_address: Address,
    cag: Cag,
) {
    let port = std::env::var("API_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(9000);

    let api_key = std::env::var("DRADIS_API_KEY").ok();
    if api_key.is_some() {
        tracing::info!(" API key authentication enabled (DRADIS_API_KEY is set)");
    } else {
        tracing::info!(" API key authentication disabled (set DRADIS_API_KEY to enable)");
    }

    let read_only = std::env::var("DRADIS_READ_ONLY")
        .ok()
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    if read_only {
        tracing::info!(" READ-ONLY demo mode ENABLED — all mutating requests will be rejected (403)");
    }

    // Server-side ring buffer for Raptor signal telemetry (durable across reloads).
    let telemetry_history: TelemetryHistory = Arc::new(Mutex::new(HashMap::new()));

    let state = ApiState { config_tx, config_rx, markets_rx, raptor_health_rx: raptor_health_rx.clone(), api_key, read_only, #[cfg(feature = "intl_clob")] safe_address, cag, telemetry_history: telemetry_history.clone() };

    // Spawn the telemetry sampler — snapshots live Raptor signals into the ring
    // buffer every TELEMETRY_SAMPLE_SECS so the Control Tower has durable,
    // scrubable history that survives browser reloads.
    tokio::spawn(run_telemetry_sampler(raptor_health_rx, telemetry_history));

    // /api/health is intentionally public — no API key required.
    // Docker HEALTHCHECK, load balancers, and uptime monitors all probe this
    // endpoint without credentials; gating it would mark every container unhealthy.
    // /api/assets is also public — it contains no sensitive data and is queried
    // by the Control Tower before authentication is established.
    let public_routes = Router::new()
        .route("/api/health", get(health))
        .route("/api/assets", get(get_assets));

    // All other routes require X-API-Key when DRADIS_API_KEY is set.
    let protected_routes = Router::new()
        .route("/api/config",                get(get_config).patch(patch_config))
        .route("/api/config/schema",         get(get_config_schema))
        .route("/api/pnl/history",           get(get_pnl_history))
        .route("/api/trades",                get(get_trades))
        .route("/api/positions",             get(get_open_positions))
        .route("/api/positions/pending",     get(get_pending_positions))
        .route("/api/positions/confirmed",   get(get_confirmed_positions))
        .route("/api/positions/{token_id}",  delete(delete_open_position))
        .route("/api/status",                get(get_status))
        .route("/api/telemetry",             get(get_telemetry))
        .route("/api/telemetry/history",     get(get_telemetry_history))
        .route("/api/portfolio",             get(get_portfolio_value))
        .route("/api/llm/recommendations",   get(get_llm_recommendations))
        // ── Phase 3d: Squadron registry endpoints ──────────────────────────
        .route("/api/squadrons",             get(get_squadrons))
        .route("/api/squadrons/{id}",        get(get_squadron_by_id))
        .route("/api/squadrons/{id}/config", get(get_squadron_config).patch(patch_squadron_config));

    // Intl-only endpoints: self-custody chain-sync + manual on-chain FAK exit.
    // The US custodial venue performs settlement/exit differently (Step 3b).
    #[cfg(feature = "intl_clob")]
    let protected_routes = protected_routes
        .route("/api/positions/sync",        axum::routing::post(sync_positions))
        .route("/api/positions/manual-exit", axum::routing::post(manual_exit));

    let protected_routes = protected_routes
        // API-key check applied to all matched routes (inner layer — runs after CORS).
        // No-op when DRADIS_API_KEY is unset so local-dev workflow is unchanged.
        .layer(axum::middleware::from_fn_with_state(state.clone(), require_api_key))
        // Read-only demo gate — rejects all mutating methods when DRADIS_READ_ONLY=true.
        // No-op otherwise. All mutating endpoints live in protected_routes.
        .layer(axum::middleware::from_fn_with_state(state.clone(), enforce_read_only))
        .with_state(state.clone());

    let app = public_routes
        .merge(protected_routes)
        // Permissive CORS (outer layer — runs first, handles OPTIONS pre-flight
        // before the API-key middleware is reached).
        .layer(CorsLayer::permissive());

    let addr = format!("0.0.0.0:{}", port);
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l)  => l,
        Err(e) => {
            tracing::error!(" Control Tower API: failed to bind on {}: {}", addr, e);
            return;
        }
    };

    tracing::info!(" Control Tower API listening on port {}", port);

    if let Err(e) = axum::serve(listener, app.into_make_service()).await {
        tracing::error!(" Control Tower API error: {}", e);
    }
}
