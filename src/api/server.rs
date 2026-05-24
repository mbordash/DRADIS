/// Control Tower REST API
///
/// Endpoints
/// ─────────────────────────────────────────────────────────────────────────────
///   GET    /api/health                — liveness check
///   GET    /api/config                — current DynamicConfig as JSON
///   PATCH  /api/config               — JSON merge-patch; hot-reloads strategies
///   GET    /api/pnl/history           — recent P&L snapshots  (?limit=200)
///   GET    /api/trades                — recent completed trades (?limit=100)
///   GET    /api/positions             — current open positions (all strategies, ghost+live)
///   DELETE /api/positions/{token_id}  — purge a specific stale row from open_positions
///   POST   /api/positions/sync        — trigger immediate chain-sync against Polymarket wallet
///   GET    /api/llm/recommendations   — recent LLM Advisor analyses (?limit=10)
///
/// The server binds to 0.0.0.0:$API_PORT (default 9000).
/// CORS is open so the Next.js Control Tower on any port can reach it.

use axum::{
    Router,
    routing::{get, post, delete},
    extract::{State, Query, Path, Request},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info, warn};
use alloy::primitives::Address;

use crate::helpers::dynamic_config::DynamicConfig;
use crate::helpers::db;
use crate::tasks::cleanup::sync_open_positions_with_chain;

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
    /// Optional API key read from `DRADIS_API_KEY` env var at startup.
    /// When `Some`, every request must include `X-API-Key: <value>`.
    /// When `None`, no authentication is required (default for local dev).
    pub api_key: Option<String>,
    /// Gnosis Safe wallet address — used by POST /api/positions/sync to fetch live
    /// on-chain holdings and purge stale open_positions rows without a restart.
    pub safe_address: Address,
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

// ─── Query params ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<i64>,
}

// ─── Handlers ────────────────────────────────────────────────────────────────

/// GET /api/health
async fn health() -> &'static str {
    debug!("Received GET /api/health request");
    "ok"
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

/// GET /api/pnl/history?limit=200
///
/// Returns up to `limit` P&L snapshots, newest first.
/// Each row: { ts, session_pnl, collateral }
async fn get_pnl_history(Query(q): Query<LimitQuery>) -> Response {
    debug!("Received GET /api/pnl/history request with limit: {:?}", q.limit);
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    match db::pool() {
        Some(pool) => {
            let history = db::get_pnl_history(pool, limit).await;
            debug!("Successfully retrieved PNL history");
            Json(history).into_response()
        },
        None       => {
            error!("Database pool not available for GET /api/pnl/history");
            Json(Vec::<db::PnlSnapshotRow>::new()).into_response()
        },
    }
}

/// GET /api/status
///
/// Returns the current market name each strategy is attached to.
/// Response: `{ "strategy_markets": { "time_decay", "momentum", … } }`
#[derive(Serialize)]
struct StatusResponse {
    strategy_markets: HashMap<String, String>,
}

async fn get_status(State(s): State<ApiState>) -> Response {
    debug!("Received GET /api/status request");
    let markets = s.markets_rx.borrow().clone();
    debug!("Successfully retrieved status");
    Json(StatusResponse { strategy_markets: markets }).into_response()
}

/// GET /api/trades?limit=100
///
/// Returns up to `limit` completed trades, newest first.
/// Each row: { ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason }
async fn get_trades(Query(q): Query<LimitQuery>) -> Response {
    debug!("Received GET /api/trades request with limit: {:?}", q.limit);
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    match db::pool() {
        Some(pool) => {
            let trades = db::get_recent_trades(pool, limit).await;
            debug!("Successfully retrieved trades");
            Json(trades).into_response()
        },
        None       => {
            error!("Database pool not available for GET /api/trades");
            Json(Vec::<db::TradeRow>::new()).into_response()
        },
    }
}

/// GET /api/positions
///
/// Returns all currently open positions for this session (inserted on entry, removed on exit).
/// Covers all strategies and both ghost/live modes so the UI always has a complete picture
/// of in-flight positions even before they appear as completed trades.
async fn get_open_positions() -> Response {
    debug!("Received GET /api/positions request");
    match db::pool() {
        Some(pool) => {
            let positions = db::get_open_positions(pool).await;
            Json(positions).into_response()
        },
        None => {
            error!("Database pool not available for GET /api/positions");
            Json(Vec::<db::OpenPositionRow>::new()).into_response()
        },
    }
}

/// DELETE /api/positions/{token_id}
///
/// Purges a specific row from `open_positions` by token_id (decimal U256 string).
/// Use this to clear stale DB rows for positions that were already closed/settled
/// on-chain — e.g. when the bot was stopped before `sync_open_positions_with_chain`
/// ran, leaving behind rows for expired or redeemed markets.
///
/// This only removes the DB row — it does NOT place a sell order on the CLOB.
/// To also sell an active (not-yet-settled) position, use the Polymarket UI or
/// wait for the bot's automated exit logic to trigger.
async fn delete_open_position(Path(token_id): Path<String>) -> Response {
    debug!("Received DELETE /api/positions/{}", token_id);
    let pool = match db::pool() {
        Some(p) => p,
        None => {
            error!("Database pool not available for DELETE /api/positions");
            return (StatusCode::SERVICE_UNAVAILABLE, "DB unavailable").into_response();
        }
    };
    match sqlx::query("DELETE FROM open_positions WHERE token_id = ?")
        .bind(&token_id)
        .execute(pool)
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
async fn sync_positions(State(s): State<ApiState>) -> Response {
    info!(" Manual chain-sync triggered via POST /api/positions/sync");
    sync_open_positions_with_chain(s.safe_address).await;
    (StatusCode::OK, Json(serde_json::json!({ "message": "Chain sync complete" }))).into_response()
}

/// GET /api/llm/recommendations?limit=10
///
/// Returns up to `limit` LLM Advisor analyses, newest first.
/// Each row: { id, ts, model, trade_count, session_pnl, analysis }
async fn get_llm_recommendations(Query(q): Query<LimitQuery>) -> Response {    debug!("Received GET /api/llm/recommendations request with limit: {:?}", q.limit);
    let limit = q.limit.unwrap_or(10).clamp(1, 50);
    match db::pool() {
        Some(pool) => {
            let recs = db::get_recent_llm_recommendations(pool, limit).await;
            debug!("Successfully retrieved {} LLM recommendations", recs.len());
            Json(recs).into_response()
        },
        None => {
            error!("Database pool not available for GET /api/llm/recommendations");
            Json(Vec::<db::LlmRecommendationRow>::new()).into_response()
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
    safe_address: Address,
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

    let state = ApiState { config_tx, config_rx, markets_rx, api_key, safe_address };

    // /api/health is intentionally public — no API key required.
    // Docker HEALTHCHECK, load balancers, and uptime monitors all probe this
    // endpoint without credentials; gating it would mark every container unhealthy.
    let public_routes = Router::new()
        .route("/api/health", get(health));

    // All other routes require X-API-Key when DRADIS_API_KEY is set.
    let protected_routes = Router::new()
        .route("/api/config",                get(get_config).patch(patch_config))
        .route("/api/pnl/history",           get(get_pnl_history))
        .route("/api/trades",                get(get_trades))
        .route("/api/positions",             get(get_open_positions))
        .route("/api/positions/sync",        post(sync_positions))
        .route("/api/positions/{token_id}",  delete(delete_open_position))
        .route("/api/status",                get(get_status))
        .route("/api/llm/recommendations",   get(get_llm_recommendations))
        // API-key check applied to all matched routes (inner layer — runs after CORS).
        // No-op when DRADIS_API_KEY is unset so local-dev workflow is unchanged.
        .layer(axum::middleware::from_fn_with_state(state.clone(), require_api_key))
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
