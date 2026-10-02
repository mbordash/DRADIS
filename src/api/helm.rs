// SPDX-License-Identifier: AGPL-3.0-only
//
// DRADIS — autonomous trading engine for crypto prediction markets.
// Copyright (C) 2026 Michael Bordash
//
// This file is part of DRADIS. DRADIS is free software: you can redistribute it
// and/or modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.
//
// DRADIS is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
// A PARTICULAR PURPOSE. See the GNU Affero General Public License for details.
//
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

//! Helm intents over HTTP: the write surface the operator's form will drive.
//!
//! Every route here is mounted under the protected router, so the `X-API-Key`
//! gate and the read-only gate apply as they do to every other mutating route.
//! Nothing here places an order or emits a signal: an intent is a record, and
//! the strategy that reads it (`vipers::helm_impl`) is inert until the entry
//! increment. What these routes do decide is the squadron's life: when the
//! last open intent reaches a terminal status the squadron retires
//! (`RetireReason::HelmComplete`), so `cancel` on the only intent stands the
//! squadron down once it is flat.
//!
//! Prices and sizes are JSON strings (`"0.55"`), as everywhere else in this
//! API; `rust_decimal` also accepts numbers.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::api::server::ApiState;
use crate::helpers::helm::{self, HelmIntent, IntentContent, IntentEvent, IntentRevision, IntentStatus, NewIntent};

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/helm/intents",                  get(list_intents).post(create_intent))
        .route("/api/helm/intents/{id}",             get(get_intent))
        .route("/api/helm/intents/{id}/acknowledge", post(acknowledge_intent))
        .route("/api/helm/intents/{id}/revise",      post(revise_intent))
        .route("/api/helm/intents/{id}/cancel",      post(cancel_intent))
        .route("/api/helm/intents/{id}/supersede",   post(supersede_intent))
}

/// The pool Helm rows live in. A Helm squadron's asset aliases to the primary
/// pool at spawn (`alias_pool("helm", primary)`), so this and the strategy's
/// `pool_for(crypto_filter)` resolve to the same database.
fn helm_pool() -> Option<sqlx::SqlitePool> {
    crate::helpers::db::pool_for(crate::vipers::helm_impl::KIND)
        .or_else(|| crate::helpers::db::pool().cloned())
}

fn venue_label() -> &'static str {
    #[cfg(feature = "intl_clob")]
    { "intl_clob" }
    #[cfg(all(not(feature = "intl_clob"), feature = "us_retail"))]
    { "us_retail" }
    #[cfg(all(not(feature = "intl_clob"), not(feature = "us_retail")))]
    { "kalshi" }
}

fn err(status: StatusCode, msg: impl Into<String>) -> Response {
    let msg = msg.into();
    warn!("Helm API: {} — {}", status.as_u16(), msg);
    (status, Json(serde_json::json!({ "error": msg }))).into_response()
}

/// A live Helm squadron the operator may give an intent to, or why not.
///
/// The registry copy of a deployed squadron carries its deploy `market_type`
/// as `market_class`, so a Helm deploy reads `helm` without a taxonomy round
/// trip; the persisted class is consulted as a fallback for a copy that does
/// not. A squadron of any other class is refused: an intent on a crypto
/// squadron would be a record nothing reads, on a page that says otherwise.
async fn helm_squadron(s: &ApiState, squadron_id: &str) -> Result<crate::cag::SquadronSummary, Response> {
    let Some(summary) = s.cag.get_squadron(&squadron_id.to_string()) else {
        return Err(err(StatusCode::NOT_FOUND, format!("squadron '{squadron_id}' not found")));
    };
    if summary.state == "STOOD_DOWN" {
        return Err(err(StatusCode::CONFLICT, format!("squadron '{squadron_id}' has stood down")));
    }
    let kind = crate::vipers::helm_impl::KIND;
    let mut is_helm = summary.market_class.eq_ignore_ascii_case(kind) || summary.asset.eq_ignore_ascii_case(kind);
    if !is_helm {
        if let Some(pool) = crate::helpers::db::pool() {
            is_helm = crate::helpers::db::get_squadron_market_class(pool, squadron_id).await
                .is_some_and(|c| c.eq_ignore_ascii_case(kind));
        }
    }
    if !is_helm {
        return Err(err(
            StatusCode::BAD_REQUEST,
            format!(
                "squadron '{squadron_id}' is not a Helm squadron (class {:?}); deploy one with market_type \"helm\"",
                summary.market_class,
            ),
        ));
    }
    Ok(summary)
}

/// Whether the squadron is simulating, as the intent should record it.
fn squadron_is_ghost(squadron_id: &str) -> bool {
    crate::config::GHOST_MODE
        || crate::helpers::dynamic_config::squadron_config_snapshot(squadron_id)
            .map(|c| c.ghost_mode)
            .unwrap_or_else(crate::helpers::dynamic_config::ghosting_now)
}

// ── Create ───────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct CreateIntentRequest {
    squadron_id: String,
    /// `YES` or `NO`.
    side: String,
    #[serde(flatten)]
    content: IntentContent,
}

/// POST /api/helm/intents — open an intent as `proposed`.
async fn create_intent(State(s): State<ApiState>, Json(req): Json<CreateIntentRequest>) -> Response {
    info!("📥 POST /api/helm/intents squadron={} side={}", req.squadron_id, req.side);
    let summary = match helm_squadron(&s, &req.squadron_id).await {
        Ok(sq) => sq,
        Err(r) => return r,
    };
    if let Err(errs) = helm::shape_check(&req.side, &req.content) {
        return err(StatusCode::BAD_REQUEST, format!("intent is malformed: {}", errs.join("; ")));
    }
    let Some(pool) = helm_pool() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };
    let new = NewIntent {
        squadron_id: req.squadron_id.clone(),
        market_id: helm::market_id_for_squadron(&pool, &req.squadron_id).await.unwrap_or_default(),
        market_name: summary.market_name.clone(),
        side: req.side.clone(),
        content: req.content.clone(),
        ghost: squadron_is_ghost(&req.squadron_id),
        venue: venue_label().to_string(),
    };
    match helm::create(&pool, &new).await {
        Ok(id) => {
            info!(
                "🧭 Helm intent #{id} opened for squadron [{}]: side={} confidence={:.2} horizon={} entry={} size=${} status=proposed{}",
                new.squadron_id, new.side.to_ascii_uppercase(), new.content.confidence,
                new.content.horizon.as_str(), new.content.entry_kind.as_str(), new.content.size_usdc,
                if new.ghost { " (ghost)" } else { "" },
            );
            match helm::get(&pool, id).await {
                Ok(Some(intent)) => (StatusCode::CREATED, Json(intent)).into_response(),
                _ => err(StatusCode::INTERNAL_SERVER_ERROR, format!("intent #{id} written but not readable")),
            }
        }
        Err(e) => err(StatusCode::BAD_REQUEST, e.to_string()),
    }
}

// ── Read ─────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct ListQuery {
    squadron_id: Option<String>,
    /// Default false: only open intents.
    #[serde(default)]
    include_terminal: bool,
    #[serde(default = "default_limit")]
    limit: i64,
}

fn default_limit() -> i64 { 100 }

/// GET /api/helm/intents?squadron_id=…&include_terminal=true
async fn list_intents(State(_s): State<ApiState>, Query(q): Query<ListQuery>) -> Response {
    let Some(pool) = helm_pool() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };
    let intents = match q.squadron_id.as_deref() {
        Some(sq) => helm::list_for_squadron(&pool, sq, q.include_terminal).await,
        None => {
            let all = helm::list_all(&pool, q.limit.clamp(1, 1000)).await;
            if q.include_terminal { all } else { all.into_iter().filter(|i| !i.status.is_terminal()).collect() }
        }
    };
    Json(intents).into_response()
}

#[derive(Serialize)]
struct IntentDetail {
    intent: HelmIntent,
    /// What the engine reads: version 1 or the latest revision.
    current: IntentContent,
    revisions: Vec<IntentRevision>,
    events: Vec<IntentEvent>,
}

/// GET /api/helm/intents/{id}
async fn get_intent(State(_s): State<ApiState>, Path(id): Path<i64>) -> Response {
    let Some(pool) = helm_pool() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };
    match helm::get(&pool, id).await {
        Ok(Some(intent)) => {
            let current = helm::current_content(&pool, &intent).await;
            let revisions = helm::revisions(&pool, id).await;
            let events = helm::events(&pool, id).await;
            Json(IntentDetail { intent, current, revisions, events }).into_response()
        }
        Ok(None) => err(StatusCode::NOT_FOUND, format!("intent #{id} not found")),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

// ── Transitions ──────────────────────────────────────────────────────────────

async fn transition_response(pool: &sqlx::SqlitePool, id: i64, to: IntentStatus, detail: Option<&str>) -> Response {
    match helm::transition(pool, id, to, detail).await {
        Ok(intent) => {
            info!(
                "🧭 Helm intent #{id} → {}{}",
                to.as_str(),
                detail.map(|d| format!(" ({d})")).unwrap_or_default(),
            );
            Json(intent).into_response()
        }
        Err(e) => {
            let msg = e.to_string();
            let status = if msg.contains("not found") { StatusCode::NOT_FOUND } else { StatusCode::CONFLICT };
            err(status, msg)
        }
    }
}

/// POST /api/helm/intents/{id}/acknowledge — `proposed → acknowledged`.
///
/// The step at which the critique will be shown and must be read (a later
/// increment). Today it records that the operator confirmed.
async fn acknowledge_intent(State(_s): State<ApiState>, Path(id): Path<i64>) -> Response {
    info!("📥 POST /api/helm/intents/{id}/acknowledge");
    let Some(pool) = helm_pool() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };
    transition_response(&pool, id, IntentStatus::Acknowledged, Some("acknowledged by operator")).await
}

#[derive(Deserialize, Default)]
struct CancelRequest {
    #[serde(default)]
    reason: Option<String>,
}

/// POST /api/helm/intents/{id}/cancel — any open status `→ closed`.
///
/// When this closes the squadron's last open intent and the squadron holds
/// nothing, the patrol retires the squadron within `HELM_INTENT_POLL_SECS`.
async fn cancel_intent(
    State(_s): State<ApiState>,
    Path(id): Path<i64>,
    body: Option<Json<CancelRequest>>,
) -> Response {
    info!("📥 POST /api/helm/intents/{id}/cancel");
    let Some(pool) = helm_pool() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };
    let reason = body
        .and_then(|Json(b)| b.reason)
        .map(|r| r.trim().to_string())
        .filter(|r| !r.is_empty())
        .unwrap_or_else(|| "cancelled by operator".to_string());
    transition_response(&pool, id, IntentStatus::Closed, Some(&reason)).await
}

#[derive(Deserialize)]
struct ReviseRequest {
    /// Why. Shown beside the diff in the retrospective; required.
    reason: String,
    #[serde(flatten)]
    content: IntentContent,
}

/// POST /api/helm/intents/{id}/revise — a new version; the first submission
/// is untouched.
async fn revise_intent(State(_s): State<ApiState>, Path(id): Path<i64>, Json(req): Json<ReviseRequest>) -> Response {
    info!("📥 POST /api/helm/intents/{id}/revise");
    let Some(pool) = helm_pool() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };
    match helm::revise(&pool, id, &req.content, &req.reason).await {
        Ok(version) => {
            info!("🧭 Helm intent #{id} revised to v{version}: {}", req.reason.trim());
            match helm::get(&pool, id).await {
                Ok(Some(intent)) => Json(IntentDetail {
                    current: helm::current_content(&pool, &intent).await,
                    revisions: helm::revisions(&pool, id).await,
                    events: helm::events(&pool, id).await,
                    intent,
                }).into_response(),
                _ => err(StatusCode::INTERNAL_SERVER_ERROR, format!("intent #{id} revised but not readable")),
            }
        }
        Err(e) => {
            let msg = e.to_string();
            let status = if msg.contains("not found") { StatusCode::NOT_FOUND } else { StatusCode::BAD_REQUEST };
            err(status, msg)
        }
    }
}

#[derive(Deserialize)]
struct SupersedeRequest {
    /// Why the old intent no longer stands; required.
    reason: String,
    /// Defaults to the old intent's side.
    #[serde(default)]
    side: Option<String>,
    #[serde(flatten)]
    content: IntentContent,
}

/// POST /api/helm/intents/{id}/supersede — close the old as `superseded`,
/// open a new one as `proposed` on the same squadron. A loosening is
/// expressed this way: a new stated reason, not an edit.
async fn supersede_intent(State(s): State<ApiState>, Path(id): Path<i64>, Json(req): Json<SupersedeRequest>) -> Response {
    info!("📥 POST /api/helm/intents/{id}/supersede");
    let Some(pool) = helm_pool() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };
    let old = match helm::get(&pool, id).await {
        Ok(Some(o)) => o,
        Ok(None) => return err(StatusCode::NOT_FOUND, format!("intent #{id} not found")),
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    // The squadron must still be live: a superseding intent on a retired
    // squadron would be a record nothing will ever read.
    if let Err(r) = helm_squadron(&s, &old.squadron_id).await {
        return r;
    }
    let side = req.side.clone().unwrap_or_else(|| old.side.clone());
    if let Err(errs) = helm::shape_check(&side, &req.content) {
        return err(StatusCode::BAD_REQUEST, format!("intent is malformed: {}", errs.join("; ")));
    }
    let new = NewIntent {
        squadron_id: old.squadron_id.clone(),
        market_id: old.market_id.clone(),
        market_name: old.market_name.clone(),
        side,
        content: req.content.clone(),
        ghost: squadron_is_ghost(&old.squadron_id),
        venue: venue_label().to_string(),
    };
    match helm::supersede(&pool, id, &new, &req.reason).await {
        Ok(new_id) => {
            info!("🧭 Helm intent #{id} superseded by #{new_id}: {}", req.reason.trim());
            match helm::get(&pool, new_id).await {
                Ok(Some(intent)) => (StatusCode::CREATED, Json(intent)).into_response(),
                _ => err(StatusCode::INTERNAL_SERVER_ERROR, format!("intent #{new_id} written but not readable")),
            }
        }
        Err(e) => err(StatusCode::CONFLICT, e.to_string()),
    }
}
