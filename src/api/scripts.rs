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

//! [E86] The Build a Viper API: list, read, check, save, delete and dry-run the
//! operator's scripted vipers. Behind the API key like every other config
//! write; the read-only middleware refuses every write here as it does
//! elsewhere.
//!
//! A viper's switches and parameters (`enabled`, `live`, its cap and its
//! parameter values) are ordinary config, written through the config PATCH as
//! `custom_vipers.<name>`; this module only handles the script itself.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

use crate::api::server::ApiState;
use crate::helpers::db;
use crate::vipers::scripted::{adapter, manifest::Manifest, paper, store};

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/vipers/scripts", get(list))
        .route("/api/vipers/paper-trades", get(paper_trades))
        .route("/api/vipers/scripts/validate", post(validate))
        .route("/api/vipers/scripts/templates", get(templates))
        .route("/api/vipers/scripts/{name}", get(read).put(save).delete(delete))
        .route("/api/vipers/scripts/{name}/dry-run", post(dry_run))
        .route("/api/vipers/scripts/{name}/reset-state", post(reset_state))
}

fn bad(status: StatusCode, problems: Vec<String>) -> Response {
    (status, Json(serde_json::json!({ "ok": false, "problems": problems }))).into_response()
}

/// The paper summary of one script, summed over every database.
async fn paper_summary(name: &str) -> paper::Summary {
    let mut out = paper::Summary::default();
    for pool in db::all_pools() {
        let s = paper::summary(&pool, name).await;
        out.open += s.open;
        out.closed += s.closed;
        out.scored += s.scored;
        out.wins += s.wins;
        out.pnl += s.pnl;
        out.fees += s.fees;
    }
    out
}

/// Open order-path positions held under this script's strategy name.
async fn open_positions(name: &str) -> i64 {
    let strategy = store::strategy_name(name);
    let mut n = 0;
    for pool in db::all_pools() {
        n += sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM open_positions WHERE strategy = ?")
            .bind(&strategy).fetch_one(&pool).await.unwrap_or(0);
    }
    n
}

/// Where a script runs: each squadron loop it is attached to, with that
/// squadron's latest status row for it (last tick, last reason, refusals).
fn running_on(name: &str) -> Vec<serde_json::Value> {
    let strategy = store::strategy_name(name);
    store::attachments_of(name).into_iter().map(|a| {
        let status = crate::helpers::viper_status::snapshot(Some(&a.status_asset))
            .into_iter().find(|r| r.strategy == strategy);
        serde_json::json!({
            "squadron": a.squadron,
            "class": a.class,
            "attached_at": a.attached_at,
            "hot": a.hot,
            "status": status,
        })
    }).collect()
}

async fn list() -> Response {
    let mut scripts = Vec::new();
    for s in store::all() {
        scripts.push(serde_json::json!({
            "name": s.manifest.name,
            "strategy": store::strategy_name(&s.manifest.name),
            "manifest": s.manifest,
            "version": s.hash,
            "saved_at": s.saved_at,
            "paper": paper_summary(&s.manifest.name).await,
            "running_on": running_on(&s.manifest.name),
        }));
    }
    Json(serde_json::json!({
        "scripts": scripts,
        "squadrons": store::running_loops(),
        "broken": store::broken(),
        "venue": crate::venues::venue_name(),
        "dry_run_squadrons": store::context_squadrons()
            .into_iter().map(|(s, at)| serde_json::json!({ "squadron": s, "context_at": at })).collect::<Vec<_>>(),
    })).into_response()
}

#[derive(Deserialize)]
struct LimitQuery { limit: Option<i64> }

/// GET /api/vipers/paper-trades?limit=200
///
/// Every script's paper trades, newest first, for the Trade Log. A route of its
/// own, never folded into `/api/trades`: that endpoint feeds the dashboard's
/// totals, and paper trades are not money.
///
/// Scoped by `asset` exactly as `/api/trades` is, so the Trade Log files each
/// paper row under the same shard as the real trades it reads beside it.
async fn paper_trades(Query(q): Query<PaperQuery>) -> Response {
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    let pools = match q.asset.as_deref() {
        Some(a) => db::pool_for_opt_retry(Some(a)).await.into_iter().collect::<Vec<_>>(),
        None => db::all_pools(),
    };
    let mut rows = Vec::new();
    for pool in pools {
        rows.extend(paper::recent_all(&pool, limit).await);
    }
    rows.sort_by(|a, b| b.opened_at.cmp(&a.opened_at));
    rows.truncate(limit as usize);
    Json(rows).into_response()
}

#[derive(Deserialize)]
struct PaperQuery { limit: Option<i64>, asset: Option<String> }

async fn read(Path(name): Path<String>, Query(q): Query<LimitQuery>) -> Response {
    let Some(s) = store::get(&name) else {
        if let Some(b) = store::broken().into_iter().find(|b| b.name == name) {
            let src = std::fs::read_to_string(store::dir().join(format!("{name}.rhai"))).ok();
            let man = std::fs::read_to_string(store::dir().join(format!("{name}.json"))).ok();
            return Json(serde_json::json!({ "name": name, "broken": b.error, "source": src, "manifest_raw": man })).into_response();
        }
        return bad(StatusCode::NOT_FOUND, vec![format!("no script '{name}'")]);
    };
    let mut rows = Vec::new();
    for pool in db::all_pools() {
        rows.extend(paper::recent(&pool, &name, q.limit.unwrap_or(100)).await);
    }
    rows.sort_by(|a, b| b.opened_at.cmp(&a.opened_at));
    Json(serde_json::json!({
        "name": name,
        "strategy": store::strategy_name(&name),
        "manifest": s.manifest,
        "source": s.source,
        "version": s.hash,
        "saved_at": s.saved_at,
        "paper": paper_summary(&name).await,
        "paper_rows": rows,
        "open_positions": open_positions(&name).await,
    })).into_response()
}

#[derive(Deserialize)]
struct ScriptBody {
    manifest: Manifest,
    source: String,
}

async fn validate(Json(b): Json<ScriptBody>) -> Response {
    match store::check(&b.manifest, &b.source) {
        Ok(_) => Json(serde_json::json!({ "ok": true, "problems": [] })).into_response(),
        Err(p) => bad(StatusCode::OK, p),
    }
}

#[derive(Deserialize)]
struct SaveQuery {
    /// The page is creating a viper, not editing one: refuse a name that is
    /// already taken rather than replace that viper's code.
    #[serde(default)]
    create: bool,
}

async fn save(Path(name): Path<String>, Query(q): Query<SaveQuery>, Json(b): Json<ScriptBody>) -> Response {
    if b.manifest.name != name {
        return bad(StatusCode::BAD_REQUEST, vec![format!("the manifest names '{}', the URL '{name}'", b.manifest.name)]);
    }
    let current = store::get(&name);
    if q.create && (current.is_some() || store::broken().iter().any(|x| x.name == name)) {
        return bad(StatusCode::CONFLICT, vec![format!("a viper named '{name}' already exists; pick another name or open it to edit")]);
    }
    let new = current.is_none();
    // A viper is attached to squadrons by its classes and venues. Narrowing
    // them while it holds a real position would detach it at the next rebuild
    // and leave that position with no stop or take-profit.
    if let Some(cur) = current {
        let venue = crate::venues::venue_name();
        let drops_class = cur.manifest.classes.iter().any(|c| !b.manifest.classes.contains(c));
        let drops_venue = cur.manifest.runs_on(venue) && !b.manifest.runs_on(venue);
        if drops_class || drops_venue {
            let held = open_positions(&name).await;
            if held > 0 {
                return bad(StatusCode::CONFLICT, vec![format!(
                    "'{name}' holds {held} position(s); it cannot drop a class or this venue until they have closed or settled"
                )]);
            }
        }
    }
    match store::save(b.manifest, b.source) {
        Ok(s) => Json(serde_json::json!({
            "ok": true,
            "version": s.hash,
            "note": if new {
                "saved; it joins every running squadron of its classes on that squadron's next tick and paper-trades until its Live switch is on"
            } else {
                "saved; the new version runs from the next tick"
            },
        })).into_response(),
        Err(p) => bad(StatusCode::UNPROCESSABLE_ENTITY, p),
    }
}

async fn delete(Path(name): Path<String>) -> Response {
    // Real positions block a delete: nothing would manage their exits. Paper
    // rows do not; they are closed unscored, with the reason on the record.
    let held = open_positions(&name).await;
    if held > 0 {
        return bad(StatusCode::CONFLICT, vec![format!(
            "'{name}' still holds {held} position(s); switch it off (its exits keep running) \
             and delete it once they have closed or settled"
        )]);
    }
    for pool in db::all_pools() {
        paper::close_all_unscored(&pool, &name, "script deleted, not scored").await;
    }
    match store::delete(&name) {
        Ok(true) => { store::reset_state(&name); Json(serde_json::json!({ "ok": true })).into_response() }
        Ok(false) => bad(StatusCode::NOT_FOUND, vec![format!("no script '{name}'")]),
        Err(e) => bad(StatusCode::INTERNAL_SERVER_ERROR, vec![e.to_string()]),
    }
}

#[derive(Deserialize)]
struct DryRunBody {
    squadron: String,
    source: Option<String>,
    /// The draft's manifest, so a test run of unsaved work reads its own
    /// parameters; the saved manifest otherwise.
    manifest: Option<Manifest>,
}

async fn dry_run(State(s): State<ApiState>, Path(name): Path<String>, Json(b): Json<DryRunBody>) -> Response {
    let manifest = match b.manifest.or_else(|| store::get(&name).map(|x| x.manifest.clone())) {
        Some(m) => m,
        None => return bad(StatusCode::NOT_FOUND, vec![format!("no script '{name}' and no manifest given")]),
    };
    let overrides = s.config_rx.borrow().custom_vipers.get(&name).map(|c| c.params.clone()).unwrap_or_default();
    let params = manifest.resolve_params(&overrides);
    match adapter::dry_run(&b.squadron, &name, b.source.as_deref(), &params) {
        Ok(v) => Json(v).into_response(),
        Err(e) => bad(StatusCode::UNPROCESSABLE_ENTITY, vec![e]),
    }
}

async fn reset_state(Path(name): Path<String>) -> Response {
    store::reset_state(&name);
    Json(serde_json::json!({ "ok": true })).into_response()
}

async fn templates() -> Response {
    Json(crate::vipers::scripted::templates::all()).into_response()
}
