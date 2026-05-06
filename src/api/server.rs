/// Control Tower REST API
///
/// Endpoints
/// ─────────────────────────────────────────────────────────────────────────────
///   GET  /api/health          — liveness check
///   GET  /api/config          — current DynamicConfig as JSON
///   PATCH /api/config         — JSON merge-patch; hot-reloads strategies
///   GET  /api/pnl/history     — recent P&L snapshots  (?limit=200)
///   GET  /api/trades          — recent completed trades (?limit=100)
///
/// The server binds to 0.0.0.0:$API_PORT (default 9000).
/// CORS is open so the Next.js dev server on any port can reach it.

use axum::{
    Router,
    routing::get,
    extract::{State, Query},
    response::{IntoResponse, Response},
    Json,
    http::StatusCode,
};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::watch;
use tower_http::cors::CorsLayer;

use crate::helpers::dynamic_config::DynamicConfig;
use crate::helpers::db;

// ─── Shared state ────────────────────────────────────────────────────────────

/// Cloneable handle passed to every axum handler via `State<ApiState>`.
#[derive(Clone)]
pub struct ApiState {
    /// Broadcast sender — PATCH handler calls `.send()` to hot-reload strategies.
    pub config_tx: Arc<watch::Sender<Arc<DynamicConfig>>>,
    /// Receiver — GET handler reads the latest snapshot without blocking.
    pub config_rx: watch::Receiver<Arc<DynamicConfig>>,
}

// ─── Query params ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<i64>,
}

// ─── Handlers ────────────────────────────────────────────────────────────────

/// GET /api/health
async fn health() -> &'static str {
    "ok"
}

/// GET /api/config
///
/// Returns the full DynamicConfig as a flat JSON object.
/// Field names match the struct fields (snake_case).
async fn get_config(State(s): State<ApiState>) -> Response {
    let cfg = s.config_rx.borrow().clone();
    match serde_json::to_value(cfg.as_ref()) {
        Ok(val) => (StatusCode::OK, Json(val)).into_response(),
        Err(e)  => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
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
    let current = s.config_rx.borrow().clone();
    match DynamicConfig::apply_patch(&current, &body).await {
        Ok(new_cfg) => {
            // Broadcast to all strategy tick loops.
            let _ = s.config_tx.send(new_cfg.clone());
            match serde_json::to_value(new_cfg.as_ref()) {
                Ok(val) => (StatusCode::OK, Json(val)).into_response(),
                Err(e)  => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

/// GET /api/pnl/history?limit=200
///
/// Returns up to `limit` P&L snapshots, newest first.
/// Each row: { ts, session_pnl, collateral }
async fn get_pnl_history(Query(q): Query<LimitQuery>) -> Response {
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    match db::pool() {
        Some(pool) => Json(db::get_pnl_history(pool, limit).await).into_response(),
        None       => Json(Vec::<db::PnlSnapshotRow>::new()).into_response(),
    }
}

/// GET /api/trades?limit=100
///
/// Returns up to `limit` completed trades, newest first.
/// Each row: { ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason }
async fn get_trades(Query(q): Query<LimitQuery>) -> Response {
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    match db::pool() {
        Some(pool) => Json(db::get_recent_trades(pool, limit).await).into_response(),
        None       => Json(Vec::<db::TradeRow>::new()).into_response(),
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
) {
    let port = std::env::var("API_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(9000);

    let state = ApiState { config_tx, config_rx };

    let app = Router::new()
        .route("/api/health",      get(health))
        .route("/api/config",      get(get_config).patch(patch_config))
        .route("/api/pnl/history", get(get_pnl_history))
        .route("/api/trades",      get(get_trades))
        // Permissive CORS so the Next.js Control Tower (any port) can reach the API.
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l)  => l,
        Err(e) => {
            tracing::error!("🌐 Control Tower API: failed to bind on {}: {}", addr, e);
            return;
        }
    };

    tracing::info!("🌐 Control Tower API listening on port {}", port);

    if let Err(e) = axum::serve(listener, app).await {
        tracing::error!("🌐 Control Tower API error: {}", e);
    }
}


