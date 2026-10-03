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

//! Helm intents over HTTP: the write surface the operator's form drives.
//!
//! Every route here is mounted under the protected router, so the `X-API-Key`
//! gate and the read-only gate apply as they do to every other mutating route.
//! Nothing here places an order: an intent is a record, and the strategy that
//! acts on it (`vipers::helm_impl`) emits its own signals through the patrol
//! loop. What these routes do decide:
//!
//! - **Whether the form was filled honestly.** `shape_check` (structure) and
//!   `validate_against_book` (consistency with a live quote) run at creation
//!   and again at acknowledgement, and block with every failing rule named.
//!   Those two functions live in `helpers::helm` and the strategy calls the
//!   same ones at entry, so no rule exists in two places.
//! - **What the operator is shown before committing.** The fee verdict is
//!   recorded on the row; the critique is requested in the background under a
//!   hard timeout and recorded as text or as `unavailable: <why>`. Acknowledging
//!   requires having read it (or its unavailability) — `read_critique: true` —
//!   and is never held past the timeout.
//! - **The squadron's life.** When the last open intent reaches a terminal
//!   status the squadron retires (`RetireReason::HelmComplete`).
//!
//! Prices and sizes are JSON strings (`"0.55"`), as everywhere else in this
//! API; `rust_decimal` also accepts numbers.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
// The venue is reached through its trait, which is what keeps this file free of
// `#[cfg]` arms: `session.venue` is the concrete per-venue type and dispatch is
// static, so nothing here names a venue.
use crate::venues::core::Execution;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, warn};

use crate::api::server::ApiState;
use crate::helpers::helm::{self, BookFacts, HelmIntent, IntentContent, IntentEvent, IntentRevision, IntentStatus, NewIntent};
use crate::helpers::helm_critique::{self, CritiqueInput};

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/helm/intents",                       get(list_intents).post(create_intent))
        .route("/api/helm/summary",                       get(summary))
        .route("/api/helm/intents/{id}",                  get(get_intent))
        .route("/api/helm/intents/{id}/acknowledge",      post(acknowledge_intent))
        .route("/api/helm/intents/{id}/revise",           post(revise_intent))
        .route("/api/helm/intents/{id}/cancel",           post(cancel_intent))
        .route("/api/helm/intents/{id}/supersede",        post(supersede_intent))
        .route("/api/helm/intents/{id}/critique-outcome", post(critique_outcome))
}

/// The pool Helm rows live in. A Helm squadron's asset aliases to the primary
/// pool at spawn (`alias_pool("helm", primary)`), so this and the strategy's
/// `pool_for(crypto_filter)` resolve to the same database.
fn helm_pool() -> Option<sqlx::SqlitePool> {
    crate::helpers::db::pool_for(crate::vipers::helm_impl::KIND)
        .or_else(|| crate::helpers::db::pool().cloned())
}



fn err(status: StatusCode, msg: impl Into<String>) -> Response {
    let msg = msg.into();
    warn!("Helm API: {} — {}", status.as_u16(), msg);
    (status, Json(serde_json::json!({ "error": msg }))).into_response()
}

/// A refusal that names every failing rule, so the form shows them all.
fn refused(what: &str, violations: Vec<String>) -> Response {
    warn!("Helm API: 400 — {what}: {}", violations.join("; "));
    (StatusCode::BAD_REQUEST, Json(serde_json::json!({
        "error": format!("{what}: {}", violations.join("; ")),
        "violations": violations,
    }))).into_response()
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

// ── The live book ────────────────────────────────────────────────────────────

/// The market as it stands, for the validator: the side's best bid and ask
/// from the venue, the close time from Gamma, the engine's minimums from the
/// global row. Phase 1 is Polymarket International; the other builds refuse
/// here, before anything is written, with the reason.
async fn book_facts(s: &ApiState, market_id: &str, side: &str) -> Result<BookFacts, Response> {
    if market_id.is_empty() {
        return Err(err(
            StatusCode::SERVICE_UNAVAILABLE,
            "this squadron's deployment row records no market id (it was deployed before the engine wrote one); stand it down and deploy it again",
        ));
    }
    // Everything below goes through the venue, so this function is venue-neutral.
    //
    // It used to call Gamma for the tokens and reach into
    // `venues::intl::u256_from_market_id` plus `api::server::fetch_side_price`
    // for the book, which is why Helm entry was gated to one venue and why this
    // file carried `#[cfg]` arms the project reserves for `src/venues/mod.rs`.
    // `SessionState` has held `venue: Arc<ActiveVenue>` since the venue
    // abstraction landed — the concrete per-venue type, static dispatch, no
    // vtable — so the handle was already here and simply unused.
    //
    // Deliberately the primary session rather than a Helm one: there is none.
    // Sessions are registered at boot for the fleet's assets only
    // (`main.rs` -> `Cag::set_session`), while a Helm squadron is deployed at
    // runtime under asset `helm` and patrols on the same primary session. A book
    // is venue-wide, not asset-specific.
    let Some(session) = s.cag.session() else {
        return Err(err(
            StatusCode::SERVICE_UNAVAILABLE,
            "no venue session is registered yet — the engine is still starting; retry in a few seconds",
        ));
    };
    let market = crate::venues::core::MarketId::new(market_id);
    let facts = match session.venue.market_facts(&market).await {
        Ok(Some(f)) => f,
        Ok(None) => return Err(err(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("the venue could not resolve market {market_id}; try again"),
        )),
        Err(e) => return Err(err(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("reading market {market_id} from the venue failed: {e}"),
        )),
    };
    // Prove the facts describe the market asked about. Squadron ids are reused
    // across redeploys, so a mismatch here is the difference between validating
    // a posture against this market and against its predecessor.
    if facts.market_id != market {
        return Err(err(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("the venue answered for {} when asked about {market_id}", facts.market_id),
        ));
    }
    let token = if side.trim().eq_ignore_ascii_case("YES") { facts.yes_token } else { facts.no_token };
    // `quote_both`, not `best_bid` + `best_ask`: on Polymarket International
    // `best_ask` deliberately keeps the trait default, because implementing it
    // would arm the shared lifecycle's naked-leg re-hedge on a venue that already
    // re-hedges in its own arbiter sweep. A read-only validator must not decide
    // that. It also costs one round trip instead of two.
    let (bid, ask) = match session.venue.quote_both(&token).await {
        Ok(pair) => pair,
        Err(e) => {
            warn!("Helm: book read failed for {token}: {e}");
            (None, None)
        }
    };
    if bid.is_none() && ask.is_none() {
        return Err(err(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("the venue returned no bid and no ask for the {} side of {market_id}; the book may be dark or the venue slow — try again", side.trim().to_ascii_uppercase()),
        ));
    }
    let dc = s.config_rx.borrow().clone();
    Ok(BookFacts {
        bid: bid.unwrap_or_default(),
        ask: ask.unwrap_or_default(),
        close_time: facts.close_time,
        now: Utc::now(),
        min_shares: crate::venues::min_order_shares(),
        min_secs_to_close: dc.helm_min_secs_to_close,
    })
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
///
/// Blocks on structure (`shape_check`) and on consistency with the live book
/// (`validate_against_book`), naming every failing rule. Records the fee
/// verdict and requests the critique in the background; neither blocks.
/// The fee verdict, and the 400 it becomes when it refuses and the knob is on.
///
/// Every path that computes a verdict goes through here, so create, revise and
/// supersede cannot disagree about whether a fee-dominated entry is allowed.
/// The verdict is still recorded when enforcement is off: the point of the knob
/// is to study a rule, not to get an entry past it.
fn fee_gate(
    content: &helm::IntentContent,
    book: &helm::BookFacts,
    dc: &crate::helpers::dynamic_config::DynamicConfig,
) -> (helm::FeeVerdict, Option<Response>) {
    let v = helm::fee_verdict(content, book, dc.helm_fee_max_ratio, dc.helm_fee_max_notional_pct);
    let block = match (&v.refusal, dc.helm_fee_verdict_enforce) {
        (Some(why), true) => Some(refused("the fee would dominate this entry", vec![why.clone()])),
        _ => None,
    };
    (v, block)
}

async fn create_intent(State(s): State<ApiState>, Json(req): Json<CreateIntentRequest>) -> Response {
    info!("📥 POST /api/helm/intents squadron={} side={}", req.squadron_id, req.side);
    let summary = match helm_squadron(&s, &req.squadron_id).await {
        Ok(sq) => sq,
        Err(r) => return r,
    };
    if let Err(errs) = helm::shape_check(&req.side, &req.content) {
        return refused("intent is malformed", errs);
    }
    let Some(pool) = helm_pool() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };
    // The open-intent limit, read from the global row: the instance-wide
    // figure, since Helm squadrons share one session. Two is a conviction.
    let (max_open, critique_timeout) = {
        let dc = s.config_rx.borrow();
        (dc.helm_max_open_intents as i64, dc.helm_critique_timeout_secs)
    };
    let open = helm::open_intents_total(&pool).await;
    if open >= max_open {
        return err(
            StatusCode::CONFLICT,
            format!("open-intent limit reached: {open} open of {max_open} allowed (helm_max_open_intents); close or supersede one first"),
        );
    }
    let market_id = helm::market_id_for_squadron(&pool, &req.squadron_id).await.unwrap_or_default();
    let book = match book_facts(&s, &market_id, &req.side).await {
        Ok(b) => b,
        Err(r) => return r,
    };
    if let Err(errs) = helm::validate_against_book(&req.side, &req.content, &book) {
        return refused("posture is inconsistent with the market", errs);
    }
    let (verdict, blocked) = fee_gate(&req.content, &book, &s.config_rx.borrow());
    if let Some(r) = blocked { return r; }
    let new = NewIntent {
        squadron_id: req.squadron_id.clone(),
        market_id,
        market_name: summary.market_name.clone(),
        side: req.side.clone(),
        content: req.content.clone(),
        ghost: squadron_is_ghost(&req.squadron_id),
        venue: crate::venues::venue_name().to_string(),
    };
    match helm::create(&pool, &new).await {
        Ok(id) => {
            let _ = helm::set_fee_verdict(&pool, id, &verdict.text).await;
            info!(
                "🧭 Helm intent #{id} opened for squadron [{}]: side={} confidence={:.2} horizon={} entry={} size=${} status=proposed{} | fee: {}",
                new.squadron_id, new.side.to_ascii_uppercase(), new.content.confidence,
                new.content.horizon.as_str(), new.content.entry_kind.as_str(), new.content.size_usdc,
                if new.ghost { " (ghost)" } else { "" }, verdict.text,
            );
            helm_critique::request(pool.clone(), id, CritiqueInput {
                market_name: new.market_name.clone(),
                side: new.side.clone(),
                current_price: helm::entry_price(&new.content, &book),
                thesis: new.content.thesis.clone(),
                confidence: new.content.confidence,
                horizon: new.content.horizon.as_str().to_string(),
                falsification: new.content.falsification.clone(),
            }, Duration::from_secs(critique_timeout));
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
    /// Seconds the critique has been pending, when it is; the form polls
    /// until this is absent (answered, or past the timeout at acknowledgement).
    critique_pending_secs: Option<i64>,
}

async fn detail(pool: &sqlx::SqlitePool, intent: HelmIntent) -> IntentDetail {
    let id = intent.id;
    IntentDetail {
        current: helm::current_content(pool, &intent).await,
        revisions: helm::revisions(pool, id).await,
        events: helm::events(pool, id).await,
        critique_pending_secs: intent.critique_pending_for(Utc::now()),
        intent,
    }
}

/// GET /api/helm/intents/{id}
async fn get_intent(State(_s): State<ApiState>, Path(id): Path<i64>) -> Response {
    let Some(pool) = helm_pool() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };
    match helm::get(&pool, id).await {
        Ok(Some(intent)) => Json(detail(&pool, intent).await).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, format!("intent #{id} not found")),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(Serialize)]
struct Summary {
    total: i64,
    open: i64,
    /// Closed intents that held a position: the calibration denominator.
    resolved: i64,
    calibration_min_resolved: usize,
    /// Whether a calibration figure may be shown at all.
    calibration_visible: bool,
    /// Always null in phase 1: the rows are recorded, the dashboard is not
    /// built, and below the threshold nothing would be shown anyway.
    calibration: Option<serde_json::Value>,
}

/// GET /api/helm/summary — counts and the calibration gate, no figures.
async fn summary(State(s): State<ApiState>) -> Response {
    let Some(pool) = helm_pool() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };
    let all = helm::list_all(&pool, 100_000).await;
    let total = all.len() as i64;
    let open = all.iter().filter(|i| !i.status.is_terminal()).count() as i64;
    let resolved = helm::resolved_count(&pool).await;
    let min = s.config_rx.borrow().helm_calibration_min_resolved;
    Json(Summary {
        total, open, resolved,
        calibration_min_resolved: min,
        calibration_visible: helm::calibration_visible(resolved, min),
        calibration: None,
    }).into_response()
}

// ── Transitions ──────────────────────────────────────────────────────────────

async fn transition_response(pool: &sqlx::SqlitePool, id: i64, to: IntentStatus, detail_text: Option<&str>) -> Response {
    match helm::transition(pool, id, to, detail_text).await {
        Ok(intent) => {
            info!(
                "🧭 Helm intent #{id} → {}{}",
                to.as_str(),
                detail_text.map(|d| format!(" ({d})")).unwrap_or_default(),
            );
            Json(detail(pool, intent).await).into_response()
        }
        Err(e) => {
            let msg = e.to_string();
            let status = if msg.contains("not found") { StatusCode::NOT_FOUND } else { StatusCode::CONFLICT };
            err(status, msg)
        }
    }
}

#[derive(Deserialize, Default)]
struct AcknowledgeRequest {
    /// The operator confirms having read the critique, or its unavailability.
    #[serde(default)]
    read_critique: bool,
}

/// POST /api/helm/intents/{id}/acknowledge — `proposed → acknowledged`.
///
/// Three things happen, in this order, and none of them can hold the
/// operator past the critique timeout:
/// 1. The critique must have answered. Still pending inside its timeout →
///    409 with `retry_after_secs`. Unanswered past it → recorded as
///    unavailable and the acknowledgement proceeds.
/// 2. The operator must say they read it (`read_critique: true`).
/// 3. The posture is validated against the live book again — it may have
///    moved since creation — and the fee verdict is refreshed.
async fn acknowledge_intent(
    State(s): State<ApiState>,
    Path(id): Path<i64>,
    body: Option<Json<AcknowledgeRequest>>,
) -> Response {
    info!("📥 POST /api/helm/intents/{id}/acknowledge");
    let Some(pool) = helm_pool() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };
    let intent = match helm::get(&pool, id).await {
        Ok(Some(i)) => i,
        Ok(None) => return err(StatusCode::NOT_FOUND, format!("intent #{id} not found")),
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    if intent.status != IntentStatus::Proposed {
        return err(StatusCode::CONFLICT, format!("intent #{id} is {} and cannot be acknowledged", intent.status.as_str()));
    }
    let timeout = s.config_rx.borrow().helm_critique_timeout_secs as i64;
    match intent.critique_pending_for(Utc::now()) {
        Some(secs) if secs < timeout => {
            return (StatusCode::CONFLICT, Json(serde_json::json!({
                "error": format!("critique pending ({secs}s of {timeout}s); read it before acknowledging"),
                "retry_after_secs": (timeout - secs).max(1),
            }))).into_response();
        }
        Some(secs) => {
            // Asked for, never answered: record it so the row says so, and proceed.
            let why = format!("unavailable: no answer within {secs}s (timeout {timeout}s)");
            let _ = helm::set_critique(&pool, id, &why, "unavailable").await;
            info!("🧭 Helm critique #{id} {why}");
        }
        None if intent.critique.is_none() => {
            let _ = helm::set_critique(&pool, id, "unavailable: not requested", "unavailable").await;
        }
        None => {}
    }
    if !body.map(|Json(b)| b.read_critique).unwrap_or(false) {
        return err(StatusCode::BAD_REQUEST, "acknowledging requires read_critique: true — read the critique (or its unavailability) first");
    }
    let content = helm::current_content(&pool, &intent).await;
    let book = match book_facts(&s, &intent.market_id, &intent.side).await {
        Ok(b) => b,
        Err(r) => return r,
    };
    if let Err(errs) = helm::validate_against_book(&intent.side, &content, &book) {
        return refused("the market has moved and the posture is no longer consistent with it; revise and acknowledge again", errs);
    }
    let (verdict, blocked) = fee_gate(&content, &book, &s.config_rx.borrow());
    let _ = helm::set_fee_verdict(&pool, id, &verdict.text).await;
    // Re-checked here because the book moves between proposing and
    // acknowledging, and this is the last point before money commits.
    if let Some(r) = blocked { return r; }
    transition_response(&pool, id, IntentStatus::Acknowledged, Some("acknowledged by operator; critique read")).await
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
    // An intent with an order or a position is not cancellable: closing it
    // would leave the position held with no posture to exit it, which is the
    // one thing the engine must never do on the operator's behalf. The
    // position leaves by its posture, by the manual exit (RTB), or by
    // settlement, and the strategy closes the intent when it does.
    match helm::get(&pool, id).await {
        Ok(Some(i)) if i.is_in_flight() => {
            return err(
                StatusCode::CONFLICT,
                format!(
                    "intent #{id} is {} with an order or position on {}; it cannot be cancelled. \
                     Revise its posture, exit the position (RTB), or let the posture run — the intent closes when the position leaves.",
                    i.status.as_str(), i.token_id.as_deref().unwrap_or("its token"),
                ),
            );
        }
        Ok(Some(_)) => {}
        Ok(None) => return err(StatusCode::NOT_FOUND, format!("intent #{id} not found")),
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
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
/// is untouched. Validated against the live book like a creation: a revision
/// that would fire at once is refused the same way.
async fn revise_intent(State(s): State<ApiState>, Path(id): Path<i64>, Json(req): Json<ReviseRequest>) -> Response {
    info!("📥 POST /api/helm/intents/{id}/revise");
    let Some(pool) = helm_pool() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };
    let intent = match helm::get(&pool, id).await {
        Ok(Some(i)) => i,
        Ok(None) => return err(StatusCode::NOT_FOUND, format!("intent #{id} not found")),
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    if let Err(errs) = helm::shape_check(&intent.side, &req.content) {
        return refused("revision is malformed", errs);
    }
    // A held position's posture is judged against the book too, except the
    // entry rules, which have nothing left to act on: the validator's entry
    // checks use the stored entry price for an in-flight intent.
    let book = match book_facts(&s, &intent.market_id, &intent.side).await {
        Ok(b) => b,
        Err(r) => return r,
    };
    let book = if intent.is_in_flight() {
        // The entry already happened at `entry_price`; judge the posture from it.
        BookFacts { ask: intent.entry_price.unwrap_or(book.ask), min_secs_to_close: 0, ..book }
    } else { book };
    let mut judged = req.content.clone();
    if intent.is_in_flight() {
        // Entry kind and size are recorded on the revision but cannot change a
        // position that exists; validate the posture as if entered at the
        // recorded price.
        judged.entry_kind = helm::EntryKind::Taker;
        judged.entry_limit_price = None;
    }
    if let Err(errs) = helm::validate_against_book(&intent.side, &judged, &book) {
        return refused("revised posture is inconsistent with the market", errs);
    }
    // The fee gate applies to a revision that has not bought anything yet.
    //
    // A `proposed` or `acknowledged` intent holds no position, so revising it is
    // just editing the plan and the gate belongs there — otherwise create with a
    // take-profit, acknowledge, then revise to a time-limit-only posture and the
    // entry happens on content the gate never saw. That reproduces intent #3 of
    // 2026-10-02 with one extra call.
    //
    // An in-flight intent is deliberately exempt. Its entry fee is already paid,
    // so a round-trip test would be measuring money that is spent, and refusing
    // the revision would strand a live position under the posture the operator
    // is trying to fix.
    if !intent.is_in_flight() {
        let (_, blocked) = fee_gate(&judged, &book, &s.config_rx.borrow());
        if let Some(r) = blocked { return r; }
    }
    match helm::revise(&pool, id, &req.content, &req.reason).await {
        Ok(version) => {
            info!("🧭 Helm intent #{id} revised to v{version}: {}", req.reason.trim());
            match helm::get(&pool, id).await {
                Ok(Some(intent)) => Json(detail(&pool, intent).await).into_response(),
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
/// expressed this way: a new stated reason, not an edit. The new intent is
/// validated, given its fee verdict and its own critique like any creation.
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
    // An intent that owns an order or a position cannot be replaced: the new
    // intent would enter a second position while the old one lost the intent
    // that exits it. A held position's posture changes by revision (reason
    // required, every version kept); supersede is for intents not yet in flight.
    if old.is_in_flight() {
        return err(
            StatusCode::CONFLICT,
            format!(
                "intent #{id} is {} with an order or position; revise its posture instead of superseding it",
                old.status.as_str(),
            ),
        );
    }
    // The squadron must still be live: a superseding intent on a retired
    // squadron would be a record nothing will ever read.
    if let Err(r) = helm_squadron(&s, &old.squadron_id).await {
        return r;
    }
    let side = req.side.clone().unwrap_or_else(|| old.side.clone());
    if let Err(errs) = helm::shape_check(&side, &req.content) {
        return refused("intent is malformed", errs);
    }
    let book = match book_facts(&s, &old.market_id, &side).await {
        Ok(b) => b,
        Err(r) => return r,
    };
    if let Err(errs) = helm::validate_against_book(&side, &req.content, &book) {
        return refused("posture is inconsistent with the market", errs);
    }
    let (verdict, blocked) = fee_gate(&req.content, &book, &s.config_rx.borrow());
    if let Some(r) = blocked { return r; }
    let critique_timeout = s.config_rx.borrow().helm_critique_timeout_secs;
    let new = NewIntent {
        squadron_id: old.squadron_id.clone(),
        market_id: old.market_id.clone(),
        market_name: old.market_name.clone(),
        side,
        content: req.content.clone(),
        ghost: squadron_is_ghost(&old.squadron_id),
        venue: crate::venues::venue_name().to_string(),
    };
    match helm::supersede(&pool, id, &new, &req.reason).await {
        Ok(new_id) => {
            let _ = helm::set_fee_verdict(&pool, new_id, &verdict.text).await;
            info!("🧭 Helm intent #{id} superseded by #{new_id}: {} | fee: {}", req.reason.trim(), verdict.text);
            helm_critique::request(pool.clone(), new_id, CritiqueInput {
                market_name: new.market_name.clone(),
                side: new.side.clone(),
                current_price: helm::entry_price(&new.content, &book),
                thesis: new.content.thesis.clone(),
                confidence: new.content.confidence,
                horizon: new.content.horizon.as_str().to_string(),
                falsification: new.content.falsification.clone(),
            }, Duration::from_secs(critique_timeout));
            match helm::get(&pool, new_id).await {
                Ok(Some(intent)) => (StatusCode::CREATED, Json(intent)).into_response(),
                _ => err(StatusCode::INTERNAL_SERVER_ERROR, format!("intent #{new_id} written but not readable")),
            }
        }
        Err(e) => err(StatusCode::CONFLICT, e.to_string()),
    }
}

#[derive(Deserialize)]
struct CritiqueOutcomeRequest {
    /// `named_it`, `missed_it` or `no_critique`.
    outcome: String,
}

/// POST /api/helm/intents/{id}/critique-outcome — after the intent resolved,
/// did the critique name the failure mode that happened? Recorded so the
/// critique is a calibration record of its own.
async fn critique_outcome(State(_s): State<ApiState>, Path(id): Path<i64>, Json(req): Json<CritiqueOutcomeRequest>) -> Response {
    info!("📥 POST /api/helm/intents/{id}/critique-outcome {}", req.outcome);
    let Some(pool) = helm_pool() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };
    match helm::set_critique_outcome(&pool, id, req.outcome.trim()).await {
        Ok(()) => match helm::get(&pool, id).await {
            Ok(Some(intent)) => Json(detail(&pool, intent).await).into_response(),
            _ => err(StatusCode::NOT_FOUND, format!("intent #{id} not found")),
        },
        Err(e) => {
            let msg = e.to_string();
            let status = if msg.contains("not found") { StatusCode::NOT_FOUND } else { StatusCode::BAD_REQUEST };
            err(status, msg)
        }
    }
}
