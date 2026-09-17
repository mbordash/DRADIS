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

//! Instance migration API (E64). The logic lives in `helpers::migration`; these
//! handlers retire and back up this instance, stream the archive out, and take
//! an archive in to restore. All of them sit behind the Setup admin gate.
//!
//! The archive routes stream both ways. The Control Tower's generic `/api` proxy
//! buffers bodies as text, so it has dedicated streaming routes for these two.

use std::path::Path;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::json;
use tokio::io::AsyncWriteExt;
use tracing::{error, info, warn};

use crate::api::server::ApiState;
use crate::helpers::migration as mig;

pub fn admin_routes() -> Router<ApiState> {
    Router::new()
        .route("/api/migration/status",          get(status))
        .route("/api/migration/prepare",         post(prepare))
        .route("/api/migration/archive",         get(archive))
        .route("/api/migration/resume",          post(resume))
        .route("/api/migration/restore",         post(restore_upload))
        .route("/api/migration/restore/apply",   post(restore_apply))
        .route("/api/migration/restore/discard", post(restore_discard))
        .layer(axum::middleware::from_fn(crate::api::setup::require_admin))
}

/// The engine runs from its install directory; `logs/` and `data/` hang off it.
fn work() -> &'static Path {
    Path::new(".")
}

fn error_response(code: StatusCode, message: impl Into<String>) -> Response {
    (code, Json(json!({ "error": message.into() }))).into_response()
}

async fn instance_counts() -> (i64, i64) {
    let (mut trades, mut open_positions) = (0i64, 0i64);
    for pool in crate::helpers::db::all_pools() {
        trades += sqlx::query_scalar::<_, i64>("SELECT count(*) FROM trades").fetch_one(&pool).await.unwrap_or(0);
        open_positions += sqlx::query_scalar::<_, i64>("SELECT count(*) FROM open_positions")
            .fetch_one(&pool)
            .await
            .unwrap_or(0);
    }
    (trades, open_positions)
}

/// GET /api/migration/status: this instance's ledger size, retirement, the
/// backup in progress or ready, and any staged or applied restore.
async fn status() -> Response {
    let (trades, open_positions) = instance_counts().await;
    Json(json!({
        "venue": crate::api::setup::build_venue(),
        "app_version": env!("CARGO_PKG_VERSION"),
        "trades": trades,
        "open_positions": open_positions,
        "state": mig::local_state(work()),
    }))
    .into_response()
}

#[derive(Deserialize)]
struct PrepareRequest {
    #[serde(default = "include_training_by_default")]
    include_training_data: bool,
}

fn include_training_by_default() -> bool {
    true
}

/// POST /api/migration/prepare: retire this instance, cancel resting orders and
/// build the backup in the background. Poll `status` for progress.
async fn prepare(State(s): State<ApiState>, body: axum::body::Bytes) -> Response {
    if mig::backup_in_progress() {
        return error_response(StatusCode::CONFLICT, "a backup is already being built");
    }
    let include_training_data = if body.is_empty() {
        true
    } else {
        match serde_json::from_slice::<PrepareRequest>(&body) {
            Ok(r) => r.include_training_data,
            Err(e) => return error_response(StatusCode::BAD_REQUEST, format!("invalid request: {e}")),
        }
    };

    mig::set_progress("retiring");
    if let Err(e) = mig::retire("migration") {
        mig::fail_progress(&format!("{e:#}"));
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("could not retire this instance: {e:#}"));
    }
    // Retirement already refuses every new order; standing the squadrons down
    // stops them evaluating at all, so the databases go quiet before the snapshot.
    //
    // Squadrons only: `stand_down_all()` also aborts the per-asset loop tasks that
    // `main.rs` awaits as the last statement of `run()`, which ends the process and
    // destroys the backup task spawned just below before it can write anything.
    s.cag.stand_down_squadrons();
    info!("📦 Migration: preparing a backup (training data {})", if include_training_data { "included" } else { "left out" });

    let cag = s.cag.clone();
    tokio::spawn(async move {
        mig::set_progress("cancelling_orders");
        crate::helpers::shutdown::run().await;
        // Wait for the patrols to leave the registry, so an order already in flight
        // settles before the snapshot rather than landing in a ledger the backup
        // has already copied. Bounded, because a venue loop that never registers
        // with the CAG must not hold the backup forever.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
        while cag.squadron_count() > 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let inputs = mig::BackupInputs {
            include_training_data,
            venue: crate::api::setup::build_venue().to_string(),
            app_version: env!("CARGO_PKG_VERSION").to_string(),
            bundle: crate::api::setup::config_bundle_value().await,
            pools: crate::helpers::db::all_pools(),
        };
        // The watchdog stands down for the duration: every heartbeat store site is
        // inside a squadron patrol, and they are all down by now.
        //
        // The park is released by a DROP GUARD rather than a straight-line call.
        // Only the two heaviest steps of `build_backup` run under `spawn_blocking`,
        // which turns a panic into an `Err`; everything else (the VACUUM queries,
        // the manifest write, the free-space check) panics straight through the
        // await. A missed unpark would leave `PARKED_FOR_BACKUP` true for the life
        // of the process, silently disarming the OS watchdog — the one recovery
        // path that survives a wedged tokio runtime. Trading would continue with no
        // crash safety net and nothing in the log to say so.
        struct ParkGuard;
        impl Drop for ParkGuard {
            fn drop(&mut self) {
                crate::helpers::watchdog::unpark_from_backup();
                // A panic unwinds past `set_progress("ready")`, so the phase would
                // stay mid-flight and `backup_in_progress()` would answer 409 to
                // every later attempt. Nothing restarts the engine to clear that
                // now, so the wedge would be permanent.
                if mig::backup_in_progress() {
                    mig::fail_progress("the backup task ended unexpectedly (panic)");
                }
            }
        }
        crate::helpers::watchdog::park_for_backup();
        let _park = ParkGuard;
        if let Err(e) = mig::build_backup(work(), inputs).await {
            error!("❌ Instance backup failed: {e:#}");
            mig::fail_progress(&format!("{e:#}"));
        }
    });

    (StatusCode::ACCEPTED, Json(json!({ "ok": true, "state": mig::local_state(work()) }))).into_response()
}

/// GET /api/migration/archive: stream the latest backup. It holds credentials
/// and the full ledger, so it is admin-gated like the config bundle.
async fn archive() -> Response {
    let Some((path, latest)) = mig::latest_archive(work()) else {
        return error_response(StatusCode::NOT_FOUND, "no backup has been built on this instance");
    };
    let file = match tokio::fs::File::open(&path).await {
        Ok(f) => f,
        Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("opening the backup: {e}")),
    };
    (
        [
            (header::CONTENT_TYPE, "application/gzip".to_string()),
            (header::CONTENT_LENGTH, latest.archive_bytes.to_string()),
            (header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}\"", latest.archive_name)),
        ],
        Body::from_stream(tokio_util::io::ReaderStream::new(file)),
    )
        .into_response()
}

fn restart_soon(why: &'static str) {
    warn!("🔄 Migration: restarting the engine ({why})");
    tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        crate::helpers::shutdown::run().await;
        std::process::exit(0);
    });
}

/// POST /api/migration/resume: undo a retirement on this instance and restart,
/// so squadrons come back through their normal startup.
async fn resume() -> Response {
    if mig::backup_in_progress() {
        return error_response(StatusCode::CONFLICT, "wait for the backup to finish before resuming trading");
    }
    if let Err(e) = mig::unretire() {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("could not clear the retirement: {e:#}"));
    }
    restart_soon("migration retirement cleared");
    Json(json!({ "ok": true, "message": "trading resumes after the restart, in about a minute" })).into_response()
}

#[derive(Deserialize)]
struct RestoreQuery {
    #[serde(default)]
    overwrite: bool,
    /// The upload's size as the browser declares it, so a full disk is refused
    /// before the transfer starts. The stream is checked as it arrives either way.
    size: Option<u64>,
}

/// How often free space is rechecked while an upload streams in.
const SPACE_CHECK_EVERY: u64 = 256 * 1024 * 1024;

/// Phrase a size mismatch the way an operator reads it, so the message says what
/// went wrong rather than only quoting two numbers.
fn describe_shortfall(received: u64, expected: u64) -> String {
    if received > expected {
        return "more arrived than the browser said it would send".to_string();
    }
    let missing = expected - received;
    match expected {
        0 => "nothing arrived".to_string(),
        _ => format!("{:.0}% of the backup is missing", (missing as f64 / expected as f64) * 100.0),
    }
}

/// POST /api/migration/restore?overwrite=: receive an archive as the raw request
/// body, verify it and stage it for the next restart. Answers 409 with
/// `needs_overwrite` when this instance already has trades.
async fn restore_upload(Query(q): Query<RestoreQuery>, body: Body) -> Response {
    if mig::is_retired() {
        return error_response(
            StatusCode::CONFLICT,
            "this instance is retired for migration; restore the backup on the new instance",
        );
    }
    let dir = mig::migration_dir(work());
    if let Err(e) = tokio::fs::create_dir_all(&dir).await {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("creating {}: {e}", dir.display()));
    }
    // Room for the upload, its extracted copy and the files a restore moves aside.
    if let Some(size) = q.size {
        if size > mig::MAX_ARCHIVE_BYTES {
            return error_response(StatusCode::PAYLOAD_TOO_LARGE, "the upload is larger than any DRADIS backup");
        }
        if let Err(e) = mig::ensure_free_space(&dir, size.saturating_mul(3)) {
            return error_response(StatusCode::INSUFFICIENT_STORAGE, format!("{e:#}"));
        }
    }
    let path = dir.join(mig::INCOMING_ARCHIVE);
    let mut file = match tokio::fs::File::create(&path).await {
        Ok(f) => f,
        Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("saving the upload: {e}")),
    };
    let mut stream = body.into_data_stream();
    let mut received = 0u64;
    let mut next_space_check = SPACE_CHECK_EVERY;
    while let Some(chunk) = stream.next().await {
        let failure = match chunk {
            Err(e) => Some((StatusCode::BAD_REQUEST, format!("the upload was interrupted: {e}"))),
            Ok(bytes) => {
                received += bytes.len() as u64;
                let low_space = received >= next_space_check && {
                    next_space_check += SPACE_CHECK_EVERY;
                    mig::ensure_free_space(&dir, 0).is_err()
                };
                if received > mig::MAX_ARCHIVE_BYTES {
                    Some((StatusCode::PAYLOAD_TOO_LARGE, "the upload is larger than any DRADIS backup".to_string()))
                } else if low_space {
                    Some((
                        StatusCode::INSUFFICIENT_STORAGE,
                        "not enough free disk space to finish receiving the upload; free space and try again".to_string(),
                    ))
                } else {
                    file.write_all(&bytes).await.err().map(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("saving the upload: {e}")))
                }
            }
        };
        if let Some((code, message)) = failure {
            drop(file);
            let _ = tokio::fs::remove_file(&path).await;
            return error_response(code, message);
        }
    }
    if let Err(e) = file.sync_all().await {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("saving the upload: {e}"));
    }
    drop(file);

    // A stream that ends early is not an error on the stream: the loop above just
    // stops. Without this the short file is staged as though it were whole, and the
    // operator sees a raw zlib failure from deep inside the extract instead of being
    // told the transfer did not finish.
    if let Some(size) = q.size {
        if received != size {
            warn!("📥 Migration: upload did not finish — {received} bytes of {size} arrived; the partial file was discarded");
            let _ = tokio::fs::remove_file(&path).await;
            return error_response(
                StatusCode::BAD_REQUEST,
                format!(
                    "the upload did not finish: {received} bytes of {size} arrived ({}). \
                     Nothing was restored. Check the connection and upload the backup again.",
                    describe_shortfall(received, size),
                ),
            );
        }
    }
    info!("📥 Migration: upload received in full ({received} bytes); verifying the archive");

    let (existing_trades, _) = instance_counts().await;
    let venue = crate::api::setup::build_venue();
    let version = env!("CARGO_PKG_VERSION");
    let overwrite = q.overwrite;
    let staged = tokio::task::spawn_blocking(move || {
        mig::stage_restore(work(), &path, venue, version, existing_trades, overwrite)
    })
    .await;
    match staged {
        Ok(Ok(manifest)) => Json(json!({ "ok": true, "received_bytes": received, "manifest": manifest })).into_response(),
        Ok(Err(mig::StageError::NeedsOverwrite { existing_trades })) => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": mig::StageError::NeedsOverwrite { existing_trades }.to_string(),
                "needs_overwrite": true,
                "existing_trades": existing_trades,
            })),
        )
            .into_response(),
        Ok(Err(e)) => {
            warn!("📥 Migration: the uploaded archive was rejected: {e}");
            error_response(StatusCode::BAD_REQUEST, e.to_string())
        }
        Err(e) => {
            error!("📥 Migration: verifying the upload panicked: {e}");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, format!("verifying the upload: {e}"))
        }
    }
}

/// POST /api/migration/restore/apply: restart so the staged restore is applied
/// at boot, before any database is opened.
async fn restore_apply() -> Response {
    if mig::local_state(work()).restore_staged.is_none() {
        return error_response(StatusCode::CONFLICT, "no restore is staged; upload a backup first");
    }
    restart_soon("applying a staged instance restore");
    Json(json!({ "ok": true, "message": "restarting to apply the restore, back in about a minute" })).into_response()
}

/// POST /api/migration/restore/discard: drop a staged restore without applying it.
async fn restore_discard() -> Response {
    mig::discard_staged_restore(work());
    Json(json!({ "ok": true })).into_response()
}

#[cfg(test)]
mod tests {
    use super::describe_shortfall;

    #[test]
    fn a_truncated_upload_is_described_as_a_shortfall() {
        assert_eq!(describe_shortfall(43_022_448, 71_704_081), "40% of the backup is missing");
        assert_eq!(describe_shortfall(0, 71_704_081), "100% of the backup is missing");
        assert_eq!(describe_shortfall(71_704_082, 71_704_081), "more arrived than the browser said it would send");
    }

    #[test]
    fn a_zero_length_declaration_does_not_divide_by_zero() {
        assert_eq!(describe_shortfall(0, 0), "nothing arrived");
    }
}
