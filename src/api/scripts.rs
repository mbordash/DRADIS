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
use crate::vipers::scripted::{adapter, bundle, manifest::Manifest, paper, registry, store};

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/vipers/scripts", get(list))
        .route("/api/vipers/paper-trades", get(paper_trades))
        .route("/api/vipers/scripts/validate", post(validate))
        .route("/api/vipers/scripts/templates", get(templates))
        .route("/api/vipers/scripts/{name}", get(read).put(save).delete(delete))
        .route("/api/vipers/scripts/{name}/dry-run", post(dry_run))
        .route("/api/vipers/scripts/{name}/reset-state", post(reset_state))
        .route("/api/vipers/scripts/{name}/export", get(export))
        // [E96] Outside `/api/vipers/scripts/`, so no script name can collide
        // with them. Bodies are a bundle plus a few fields; anything larger is
        // refused before it is parsed.
        .route("/api/vipers/import/check", post(import_check).layer(axum::extract::DefaultBodyLimit::max(IMPORT_BODY_LIMIT)))
        .route("/api/vipers/registry", get(registry_index))
        .route("/api/vipers/registry/{publisher}/{name}/{version}", get(registry_bundle))
        .route("/api/vipers/import", post(import).layer(axum::extract::DefaultBodyLimit::max(IMPORT_BODY_LIMIT)))
}

/// The bundle and the import's own fields.
const IMPORT_BODY_LIMIT: usize = bundle::MAX_BUNDLE_BYTES + 4 * 1024;

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
            "digest": bundle::content_digest(&s.manifest, &s.source),
            "provenance": bundle::provenance(&s.manifest, &s.source),
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
    // The Trade Log files every row by venue, class and underlying. The paper
    // ledger stores none of them, and none needs storing: the ledger lives in
    // this engine's database, so the venue is this build's; the class is the
    // running squadron's, or crypto when the market names an underlying; the
    // underlying reads from the market's name as the squadrons' own does.
    let loops = store::running_loops();
    let out: Vec<_> = rows.into_iter().map(|r| {
        let underlying = crate::squadron::underlying_from_market_name(&r.market);
        let class = loops.iter().find(|l| l.squadron == r.squadron_id).map(|l| l.class.clone())
            .or_else(|| underlying.map(|_| "crypto".to_string()));
        PaperTradeOut { venue: crate::venues::ledger_venue(), market_class: class, underlying, row: r }
    }).collect();
    Json(out).into_response()
}

#[derive(serde::Serialize)]
struct PaperTradeOut {
    venue: &'static str,
    market_class: Option<String>,
    underlying: Option<&'static str>,
    #[serde(flatten)]
    row: paper::PaperRow,
}

#[derive(Deserialize)]
struct PaperQuery { limit: Option<i64>, asset: Option<String> }

async fn read(Path(name): Path<String>, Query(q): Query<LimitQuery>) -> Response {
    let Some(s) = store::get(&name) else {
        if let Some(b) = store::broken().into_iter().find(|b| b.name == name) {
            // The file names come from the loader's own record of what is in
            // the folder, never from the URL.
            let src = std::fs::read_to_string(store::dir().join(format!("{}.rhai", b.name))).ok();
            let man = std::fs::read_to_string(store::dir().join(format!("{}.json", b.name))).ok();
            return Json(serde_json::json!({ "name": b.name, "broken": b.error, "source": src, "manifest_raw": man })).into_response();
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
        "digest": bundle::content_digest(&s.manifest, &s.source),
        "provenance": bundle::provenance(&s.manifest, &s.source),
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
    match store::check_for_save(&b.manifest, &b.source) {
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
    match adapter::dry_run(&b.squadron, &name, b.source.as_deref(), &params, manifest.market) {
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

// ── [E96] Export and import ──────────────────────────────────────────────────

/// GET /api/vipers/scripts/{name}/export — the viper as a bundle file.
async fn export(Path(name): Path<String>) -> Response {
    let Some(s) = store::get(&name) else {
        return bad(StatusCode::NOT_FOUND, vec![format!("no script '{name}'")]);
    };
    let b = bundle::export(&s);
    let body = match serde_json::to_vec_pretty(&b) {
        Ok(v) => v,
        Err(e) => return bad(StatusCode::INTERNAL_SERVER_ERROR, vec![e.to_string()]),
    };
    // `name` passed `store::get`, so it is a valid script name: no quotes,
    // separators or anything else that could break the header.
    (
        [
            (axum::http::header::CONTENT_TYPE, "application/json".to_string()),
            (axum::http::header::CONTENT_DISPOSITION, format!("attachment; filename=\"{name}.dradis-viper.json\"")),
        ],
        body,
    ).into_response()
}

#[derive(Deserialize)]
struct ImportBody {
    bundle: bundle::Bundle,
    /// The local name; the bundle's own name when absent.
    #[serde(default)]
    name: Option<String>,
    /// Replace the viper of that name, if there is one.
    #[serde(default)]
    replace: bool,
}

fn taken(name: &str) -> bool {
    store::get(name).is_some() || store::broken().iter().any(|b| b.name == name)
}

/// `bundle::check_import` plus the registry signature: a bundle that carries
/// one must verify (else it was changed after publishing, and is refused); a
/// verified one records the registry as its origin and its signed publisher.
fn checked(b: &bundle::Bundle, name: &str) -> (bundle::Checked, Option<registry::Signed>) {
    let mut c = bundle::check_import(b, Some(name), chrono::Utc::now());
    let signed = match registry::verify_bundle(b) {
        Ok(s) => s,
        Err(e) => { c.problems.push(e); None }
    };
    if let (Some(s), Some(o)) = (&signed, c.manifest.origin.as_mut()) {
        o.source = registry::SOURCE.into();
        o.publisher = Some(s.publisher.clone());
        o.version = Some(s.version.clone());
    }
    (c, signed)
}

/// POST /api/vipers/import/check — what an import would do. Changes nothing.
async fn import_check(Json(b): Json<ImportBody>) -> Response {
    let name = b.name.clone().unwrap_or_else(|| b.bundle.manifest.name.clone());
    let (c, signed) = checked(&b.bundle, &name);
    let current = store::get(&name);
    Json(serde_json::json!({
        "ok": c.problems.is_empty(),
        "problems": c.problems,
        "name": name,
        "exists": taken(&name),
        "free_name": bundle::free_name(&name, taken),
        "digest": c.digest,
        "manifest": c.manifest,
        "source": c.source,
        "readme": b.bundle.readme,
        "license": b.bundle.license,
        "publisher": b.bundle.publisher,
        "version": b.bundle.version,
        // The registry's signature, checked against this build's trusted
        // keys whichever way the bundle arrived; null when it carries none.
        "signed": signed,
        // A viper declared for other venues imports fine but never attaches here.
        "runs_here": c.manifest.runs_on(crate::venues::venue_name()),
        "venue": crate::venues::venue_name(),
        // What it would replace, for the page's diff.
        "current": current.map(|s| serde_json::json!({ "manifest": s.manifest, "source": s.source })),
    })).into_response()
}

/// POST /api/vipers/import — import a bundle. A new viper arrives switched
/// off on the instance and every squadron, and on paper; replacing one keeps
/// its on/off switches, puts it back on paper and clears its memory. Nothing
/// here fetches anything: the bundle is the request body.
async fn import(State(st): State<ApiState>, Json(b): Json<ImportBody>) -> Response {
    let name = b.name.clone().unwrap_or_else(|| b.bundle.manifest.name.clone());
    let (c, signed) = checked(&b.bundle, &name);
    if !c.problems.is_empty() {
        return bad(StatusCode::UNPROCESSABLE_ENTITY, c.problems);
    }
    let exists = taken(&name);
    if exists && !b.replace {
        return bad(StatusCode::CONFLICT, vec![format!(
            "a viper named '{name}' already exists; import it as '{}' or choose to replace it",
            bundle::free_name(&name, taken)
        )]);
    }
    if exists {
        // A replacement changes the code that manages any position it holds.
        let held = open_positions(&name).await;
        if held > 0 {
            return bad(StatusCode::CONFLICT, vec![format!(
                "'{name}' holds {held} position(s); replace it once they have closed or settled"
            )]);
        }
    }
    // The switches go first. The global row is broadcast at once; squadron
    // rows are written now but Kalshi and Polymarket US squadrons re-read
    // theirs every ~30 s, so until then they find no entry and fall back to
    // the global one (`DynamicConfig::custom_viper`), not to the default "on".
    let switches = if exists {
        serde_json::json!({ "custom_vipers": { &name: { "live": false } } })
    } else {
        serde_json::json!({ "custom_vipers": { &name: { "enabled": false, "live": false } } })
    };
    if let Err(e) = crate::api::server::apply_global_patch(&st, &switches.to_string(), true).await {
        return bad(StatusCode::INTERNAL_SERVER_ERROR, vec![format!("could not set the imported viper's switches: {e}")]);
    }
    match store::save(c.manifest, c.source) {
        Ok(s) => {
            if exists { store::reset_state(&name); }
            tracing::info!("📜 Imported viper '{name}' from {} (digest {}…){}",
                signed.as_ref().map_or("an unsigned bundle".to_string(), |s| format!("{}'s signed {} {}", registry::SOURCE, s.publisher, s.version)),
                &c.digest[..12],
                if exists { ", replacing the local one; back on paper" } else { "; off and on paper until switched on" });
            Json(serde_json::json!({
                "ok": true,
                "name": name,
                "version": s.hash,
                "digest": c.digest,
                "note": if exists {
                    "replaced; it keeps its on/off switches, is back on paper, and its memory was cleared. Its paper ledger carries on under the same name, so its totals mix both versions"
                } else {
                    "imported; it is off on every squadron and on paper until you switch it on"
                },
            })).into_response()
        }
        Err(p) => {
            // Best effort: a new import that did not save leaves no settings
            // behind for a script that does not exist.
            if !exists {
                let undo = serde_json::json!({ "custom_vipers": { &name: null } });
                if let Err(e) = crate::api::server::apply_global_patch(&st, &undo.to_string(), true).await {
                    tracing::warn!("📜 Import of '{name}' failed and its settings were not removed: {e}");
                }
            }
            bad(StatusCode::UNPROCESSABLE_ENTITY, p)
        }
    }
}

// ── [E96] phase 2: the community registry ───────────────────────────────────
//
// Both routes fetch from the compiled-in registry origin only, on the
// operator's action; nothing here polls. A registry bundle is imported through
// the same `/api/vipers/import` as a file: the Control Tower hands it back as
// the request body, and the engine checks its signature again there.

/// GET /api/vipers/registry — the signed index, with what this deployment can
/// run and which entries it already has.
async fn registry_index() -> Response {
    let index = match registry::fetch_index().await {
        Ok(i) => i,
        Err(e) => return bad(StatusCode::BAD_GATEWAY, vec![e]),
    };
    let venue = crate::venues::venue_name();
    let local = store::all();
    let vipers: Vec<serde_json::Value> = index.vipers.iter().map(|e| {
        let installed: Vec<serde_json::Value> = local.iter()
            // Only signed imports: an unsigned file can claim any publisher.
            .filter(|s| s.manifest.origin.as_ref().is_some_and(|o| o.source == registry::SOURCE
                && o.publisher.as_deref() == Some(e.publisher.as_str()) && o.name == e.name))
            .map(|s| serde_json::json!({
                "name": s.manifest.name,
                "version": s.manifest.origin.as_ref().and_then(|o| o.version.clone()),
                "provenance": bundle::provenance(&s.manifest, &s.source),
            }))
            .collect();
        serde_json::json!({
            "entry": e,
            "available": e.available().map(|v| v.version.clone()),
            "runs_here": e.venues.is_empty() || e.venues.iter().any(|v| v == venue),
            "installed": installed,
        })
    }).collect();
    Json(serde_json::json!({
        "registry": registry::SOURCE,
        "generated_at": index.generated_at,
        "api_version": bundle::API_VERSION,
        "venue": venue,
        "vipers": vipers,
    })).into_response()
}

/// GET /api/vipers/registry/{publisher}/{name}/{version} — one published
/// version, checked against its signature and the signed index (listed, same
/// digest, not yanked). Read-only: the import is a separate step.
async fn registry_bundle(Path((publisher, name, version)): Path<(String, String, String)>) -> Response {
    match registry::fetch_bundle(&publisher, &name, &version).await {
        Ok((b, signed)) => Json(serde_json::json!({ "bundle": b, "signed": signed })).into_response(),
        Err(e) => {
            use registry::FetchError::*;
            let status = match &e {
                Invalid(_) => StatusCode::BAD_REQUEST,
                NotFound(_) => StatusCode::NOT_FOUND,
                Yanked(_) => StatusCode::GONE,
                Upstream(_) => StatusCode::BAD_GATEWAY,
            };
            bad(status, vec![e.to_string()])
        }
    }
}
