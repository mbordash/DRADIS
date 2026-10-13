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
        .route("/api/migration/archive/ticket",  post(archive_ticket))
        .route("/api/migration/resume",          post(resume))
        .route("/api/migration/restore",         post(restore_upload))
        .route("/api/migration/restore/apply",   post(restore_apply))
        .route("/api/migration/restore/discard", post(restore_discard))
        .layer(axum::middleware::from_fn(crate::api::setup::require_admin))
        // Added AFTER the layer on purpose: the archive checks the admin gate
        // itself, because it also admits a one-time ticket (see `archive`).
        // The X-API-Key and read-only layers in `server.rs` still wrap it.
        .route("/api/migration/archive",         get(archive))
}

/// How long a download ticket opens the archive for.
///
/// Long enough for the browser to turn the ticket into a navigation (well under
/// a second), short enough that a ticket copied out of a proxy log or the
/// browser history is dead by the time anyone reads it. The download itself
/// is not bounded by this: the ticket is checked once, when the stream starts.
pub(crate) const ARCHIVE_TICKET_TTL_SECS: u64 = 60;

/// Outstanding tickets the jar keeps at most. An operator clicking Download
/// repeatedly mints one per click; anything past this many unredeemed tickets
/// forgets the oldest, so a stuck button cannot grow the process.
const ARCHIVE_TICKET_CAP: usize = 32;

/// One-time tickets that open the archive download without the admin session.
///
/// A browser navigation is the only way to stream a multi-hundred-megabyte
/// archive straight to disk; a `fetch` lands the whole thing in a tab-memory
/// blob first, which is where the 119 MB serving model plus the 85 MB archived
/// model plus the ledger stop working (E69). But a navigation cannot carry the
/// `X-Admin-Token` header the admin gate wants. So the Control Tower first
/// asks, with its admin session, for a ticket, then navigates to
/// `/api/migration/archive?ticket=…`. A ticket is 32 random bytes, lives
/// [`ARCHIVE_TICKET_TTL_SECS`], and is consumed by its first use, so the URL
/// that lands in a log opens nothing afterward. The archive still holds
/// credentials and the full ledger, which is why the ticket is not a weaker
/// gate than the session: it is minted BY the session, once, for one download.
#[derive(Default)]
pub(crate) struct TicketJar {
    tickets: Vec<(String, std::time::Instant)>,
}

impl TicketJar {
    /// Mint a ticket valid until `now + TTL`.
    pub(crate) fn issue(&mut self, now: std::time::Instant) -> String {
        self.prune(now);
        let ticket = mint_ticket();
        self.tickets.push((ticket.clone(), now + std::time::Duration::from_secs(ARCHIVE_TICKET_TTL_SECS)));
        if self.tickets.len() > ARCHIVE_TICKET_CAP {
            let excess = self.tickets.len() - ARCHIVE_TICKET_CAP;
            self.tickets.drain(..excess);
        }
        ticket
    }

    /// Consume `ticket` if it is outstanding and unexpired. True at most once
    /// per ticket.
    pub(crate) fn redeem(&mut self, ticket: &str, now: std::time::Instant) -> bool {
        self.prune(now);
        match self.tickets.iter().position(|(t, _)| eq_constant_time(t, ticket)) {
            Some(i) => {
                self.tickets.remove(i);
                true
            }
            None => false,
        }
    }

    /// Tickets outstanding, for the tests.
    #[cfg(test)]
    fn outstanding(&self) -> usize {
        self.tickets.len()
    }

    fn prune(&mut self, now: std::time::Instant) {
        self.tickets.retain(|(_, expires)| *expires > now);
    }
}

/// 32 bytes from the OS RNG, hex. The same source the admin session key uses.
fn mint_ticket() -> String {
    use argon2::password_hash::rand_core::RngCore;
    let mut bytes = [0u8; 32];
    argon2::password_hash::rand_core::OsRng.fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Equality that takes the same time for every unequal pair of the same
/// length, so a ticket cannot be guessed a byte at a time off the timing. The
/// length check short-circuits, which gives away only the length, and every
/// ticket has the same one.
fn eq_constant_time(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn ticket_jar() -> std::sync::MutexGuard<'static, TicketJar> {
    static JAR: std::sync::OnceLock<std::sync::Mutex<TicketJar>> = std::sync::OnceLock::new();
    JAR.get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The engine runs from its install directory; `logs/` and `data/` hang off it.
fn work() -> &'static Path {
    Path::new(".")
}

fn error_response(code: StatusCode, message: impl Into<String>) -> Response {
    (code, Json(json!({ "error": message.into() }))).into_response()
}

/// Trades and open positions across every shard, or why they could not be
/// counted. A failed count is not zero: it used to read as 0, which showed
/// "0 trades" in the retire dialog and let a restore skip the confirmation
/// that protects an existing ledger from being replaced ([B43]).
async fn instance_counts() -> Result<(i64, i64), String> {
    let (mut trades, mut open_positions) = (0i64, 0i64);
    for pool in crate::helpers::db::all_pools() {
        trades += sqlx::query_scalar::<_, i64>("SELECT count(*) FROM trades")
            .fetch_one(&pool)
            .await
            .map_err(|e| format!("counting trades: {e}"))?;
        open_positions += sqlx::query_scalar::<_, i64>("SELECT count(*) FROM open_positions")
            .fetch_one(&pool)
            .await
            .map_err(|e| format!("counting open positions: {e}"))?;
    }
    Ok((trades, open_positions))
}

/// GET /api/migration/status: this instance's ledger size, retirement, the
/// backup in progress or ready, and any staged or applied restore.
async fn status() -> Response {
    let counts = instance_counts().await;
    if let Err(e) = &counts {
        warn!("📦 Migration status: {e}");
    }
    Json(json!({
        "venue": crate::api::setup::build_venue(),
        "app_version": env!("CARGO_PKG_VERSION"),
        "trades": counts.as_ref().ok().map(|c| c.0),
        "open_positions": counts.as_ref().ok().map(|c| c.1),
        "counts_error": counts.as_ref().err(),
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

    // Before anything else: the previous backup stops being offered the moment a
    // new one is asked for. Leaving it downloadable is how a restored instance
    // hands back the near-empty archive it made before the restore.
    mig::invalidate_latest_backup(work());

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

/// POST /api/migration/archive/ticket: mint a one-time ticket that opens the
/// archive download for the next minute. Admin-gated; see [`TicketJar`].
async fn archive_ticket() -> Response {
    if mig::latest_archive(work()).is_none() {
        return error_response(StatusCode::NOT_FOUND, "no backup has been built on this instance");
    }
    let ticket = ticket_jar().issue(std::time::Instant::now());
    Json(json!({ "ticket": ticket, "expires_in_secs": ARCHIVE_TICKET_TTL_SECS })).into_response()
}

#[derive(Deserialize)]
struct ArchiveQuery {
    ticket: Option<String>,
}

/// GET /api/migration/archive: stream the latest backup. It holds credentials
/// and the full ledger, so it is admin-gated like the config bundle: the admin
/// session as everywhere else, or a one-time ticket minted by that session
/// (`?ticket=`), which is how a browser navigation downloads it straight to
/// disk. The ticket is consumed before the first byte, whatever happens next.
async fn archive(Query(q): Query<ArchiveQuery>, req: axum::extract::Request) -> Response {
    // `get()` also answers HEAD. A download manager that probes with HEAD first
    // would spend the ticket and then be refused on its GET, so HEAD is refused
    // before the jar is consulted; browsers never HEAD a navigation.
    if req.method() != axum::http::Method::GET {
        return error_response(StatusCode::METHOD_NOT_ALLOWED, "the archive answers GET only");
    }
    let admitted = !crate::api::setup::admin_gate_active()
        || crate::api::setup::request_is_admin(&req)
        || q.ticket.as_deref().is_some_and(|t| ticket_jar().redeem(t, std::time::Instant::now()));
    if !admitted {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": "admin session or a valid download ticket required",
                "code": "session_required",
                "login": "/api/auth/login",
            })),
        ).into_response();
    }
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

    let existing_trades = match instance_counts().await {
        Ok((trades, _)) => trades,
        // Unknown is not empty: without an explicit overwrite, refuse as if the
        // ledger held trades, so a failed count can never skip the confirmation.
        Err(e) if !q.overwrite => {
            let _ = tokio::fs::remove_file(&path).await;
            warn!("📥 Migration: could not count this instance's ledger ({e}); asking for an explicit overwrite");
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": format!(
                        "this instance's ledger could not be counted ({e}); restoring replaces it, so confirm the overwrite"
                    ),
                    "needs_overwrite": true,
                    "existing_trades": serde_json::Value::Null,
                })),
            )
                .into_response();
        }
        Err(_) => 0, // overwrite confirmed: the count no longer gates anything
    };
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
    use super::{TicketJar, ARCHIVE_TICKET_TTL_SECS};
    use std::time::{Duration, Instant};

    /// E69: a ticket opens the archive exactly once. The second attempt with the
    /// same ticket, the one a copied URL would make, is refused.
    #[test]
    fn a_ticket_opens_the_archive_once() {
        let mut jar = TicketJar::default();
        let now = Instant::now();
        let t = jar.issue(now);
        assert_eq!(t.len(), 64, "32 random bytes, hex: {t}");
        assert!(t.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(jar.redeem(&t, now + Duration::from_secs(1)));
        assert!(!jar.redeem(&t, now + Duration::from_secs(2)), "spent");
        assert_eq!(jar.outstanding(), 0);
    }

    /// A ticket is good for the TTL and not a second longer.
    #[test]
    fn an_expired_ticket_is_refused() {
        let mut jar = TicketJar::default();
        let now = Instant::now();
        let t = jar.issue(now);
        assert!(!jar.redeem(&t, now + Duration::from_secs(ARCHIVE_TICKET_TTL_SECS + 1)));
        let t = jar.issue(now);
        assert!(jar.redeem(&t, now + Duration::from_secs(ARCHIVE_TICKET_TTL_SECS - 1)));
    }

    /// Two tickets never collide, a near miss opens nothing, and the empty
    /// string (a bare `?ticket=`) opens nothing.
    #[test]
    fn a_wrong_ticket_is_refused() {
        let mut jar = TicketJar::default();
        let now = Instant::now();
        let a = jar.issue(now);
        let b = jar.issue(now);
        assert_ne!(a, b);
        let mut near = a.clone();
        near.replace_range(63..64, if a.ends_with('0') { "1" } else { "0" });
        assert!(!jar.redeem(&near, now));
        assert!(!jar.redeem("", now));
        assert!(!jar.redeem(&a[..63], now), "a prefix is not a ticket");
        assert!(jar.redeem(&a, now) && jar.redeem(&b, now), "the real ones still work");
    }

    /// A stuck Download button cannot grow the process: the jar keeps the
    /// newest tickets and forgets the oldest.
    #[test]
    fn the_jar_is_bounded() {
        let mut jar = TicketJar::default();
        let now = Instant::now();
        let first = jar.issue(now);
        let mut last = String::new();
        for _ in 0..100 {
            last = jar.issue(now);
        }
        assert!(jar.outstanding() <= super::ARCHIVE_TICKET_CAP);
        assert!(!jar.redeem(&first, now), "the oldest was forgotten");
        assert!(jar.redeem(&last, now), "the newest still opens");
    }

    /// The archive route sits outside the `require_admin` layer, because it
    /// has to admit a ticket, and it must therefore do the admin check itself.
    /// Asserted at source level: the defect would be a missing call.
    #[test]
    fn the_archive_route_checks_the_gate_itself() {
        let src = include_str!("migration.rs");
        let routes = src.split("pub fn admin_routes()").nth(1).expect("admin_routes must exist");
        let routes = &routes[..routes.find("\n}\n").unwrap_or(routes.len())];
        let layer = routes.find("require_admin").expect("the admin layer must be applied");
        let archive = routes.find("get(archive))").expect("the archive route must exist");
        assert!(archive > layer, "the archive route must be added after the admin layer, or no ticket can open it");
        let ticket = routes.find("post(archive_ticket)").expect("the ticket route must exist");
        assert!(ticket < layer, "the ticket route must sit behind the admin layer, or anyone could mint one");
        let body = src.split("async fn archive(").nth(1).expect("archive must exist");
        let body = &body[..body.find("\n}\n").unwrap_or(body.len())];
        assert!(body.contains("admin_gate_active()") && body.contains("request_is_admin(&req)") && body.contains("redeem("),
            "archive must consult the admin gate, the session and the ticket jar");
        assert!(body.find("Method::GET").is_some_and(|m| m < body.find("redeem(").unwrap()),
            "HEAD must be refused before a ticket is spent");
    }

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

    /// `prepare` must withdraw the previous backup before it starts building the
    /// next one, or a restored instance keeps offering the near-empty archive it
    /// made before the restore — the 2026-09-17 failure, where an operator
    /// downloaded a zero-trade backup of an instance holding 52 trades.
    ///
    /// Asserted at source level, and deliberately: the defect is a missing call,
    /// and no unit test of `invalidate_latest_backup` can catch its caller not
    /// calling it. Ordering matters as much as presence, so both are checked.
    #[test]
    fn prepare_withdraws_the_previous_backup_before_building_the_next() {
        let src = include_str!("migration.rs");
        let body = src.split("async fn prepare(").nth(1).expect("prepare must exist");
        let body = &body[..body.find("\n}\n").unwrap_or(body.len())];

        let withdraw = body
            .find("invalidate_latest_backup")
            .expect("prepare must withdraw the previous backup, or a stale one stays downloadable");

        // After the in-progress guard: a request refused with 409 must not throw
        // away a good backup the operator has not downloaded yet.
        let guard = body.find("backup_in_progress()").expect("prepare must refuse a concurrent build");
        assert!(
            guard < withdraw,
            "the previous backup must be withdrawn only after the concurrent-build check, not before it",
        );

        // Before the build is spawned: withdrawing afterwards would leave a window
        // in which the stale archive is still being offered.
        let spawn = body.find("tokio::spawn").expect("prepare must spawn the backup task");
        assert!(
            withdraw < spawn,
            "the previous backup must be withdrawn before the new build starts, not after",
        );
    }
}
