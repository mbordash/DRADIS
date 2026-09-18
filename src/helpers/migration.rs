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

//! Instance migration (roadmap E64): retire an instance and back up its data, or
//! restore that backup onto a fresh instance.
//!
//! An AWS Marketplace customer upgrades by launching a new instance, because an
//! AMI never updates itself. The config bundle (`api/setup.rs`) carries the
//! credentials and settings. This carries the data that cannot be rebuilt: every
//! venue database (the trade ledger, open positions with their strategy labels,
//! sessions, config history), GBoost's serving and archived models, and by
//! default its training data.
//!
//! Both production migrations on 2026-09-13 and 2026-09-14 were done by hand over
//! SSH. Both times the old and new instance traded the same wallet for minutes,
//! and before the database was copied the new instance re-adopted an open
//! FairValue position under the wrong strategy.
//!
//! **Old instance.** [`retire`] sets a persistent flag. While it is set, every
//! venue refuses new orders ([`refuse_if_retired`] sits in each venue's order
//! submission), squadrons do not start and GBoost does not train, across
//! restarts. The API then cancels resting orders, and [`build_backup`] snapshots
//! each database with `VACUUM INTO` and writes one `.tar.gz` whose manifest
//! records every file's size and sha256. Open positions stay in the wallet; the
//! restored ledger keeps their labels.
//!
//! **New instance.** [`stage_restore`] extracts an uploaded archive and verifies
//! it (kind, schema, venue, app version, checksums, and that every path is one a
//! backup may carry). [`apply_pending_restore`] applies it at the next boot,
//! before any database is opened, and moves everything it replaces into a
//! `pre-restore-*` folder rather than deleting it.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{error, info, warn};

pub const BACKUP_KIND: &str = "dradis-instance-backup";
pub const BACKUP_SCHEMA_VERSION: u32 = 1;

/// The error every venue returns for an order placed while retired. Stated in
/// full because it reaches the patrol log, where an operator reads it.
pub const RETIRED_REFUSAL: &str =
    "instance retired for migration: new orders are refused (resume trading from Setup to undo)";

/// Largest archive a restore accepts. Four months of BTC training data, two
/// models and a database come to roughly 300 MB before compression.
pub const MAX_ARCHIVE_BYTES: u64 = 4 * 1024 * 1024 * 1024;

const MIGRATION_SUBDIR: &str = "logs/migration";
const PENDING_FILE: &str = "restore-pending.json";
const FAILED_FILE: &str = "restore-failed.json";
const REPORT_FILE: &str = "restore-report.json";
const LATEST_BACKUP_FILE: &str = "latest-backup.json";
const STAGING_DIR: &str = "restore-staging";
pub const INCOMING_ARCHIVE: &str = "incoming.tar.gz";

static RETIRED: AtomicBool = AtomicBool::new(false);
static PROGRESS: Mutex<Option<BackupProgress>> = Mutex::new(None);

/// `logs/migration` under the instance's working directory.
pub fn migration_dir(work: &Path) -> PathBuf {
    work.join(MIGRATION_SUBDIR)
}

fn data_dir() -> PathBuf {
    PathBuf::from(std::env::var("DRADIS_DATA_DIR").unwrap_or_else(|_| "./data".to_string()))
}

// ─── Retired flag ────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RetiredRecord {
    pub retired_at: String,
    pub reason: String,
}

/// Is this instance retired for migration? Read on every order, so it is an
/// atomic load, not a file read.
pub fn is_retired() -> bool {
    RETIRED.load(Ordering::Relaxed)
}

/// Refuse an order while retired. Called at the top of each venue's order
/// submission, the one path every order on that venue goes through, so no
/// strategy, repair path or manual exit can trade a wallet that a replacement
/// instance is about to take over.
pub fn refuse_if_retired() -> Result<()> {
    refusal(is_retired())
}

fn refusal(retired: bool) -> Result<()> {
    if retired {
        bail!(RETIRED_REFUSAL);
    }
    Ok(())
}

fn retired_path(data: &Path) -> PathBuf {
    data.join("retired.json")
}

/// The retired record under `data`, if the flag is set.
///
/// A flag file that exists but cannot be read counts as retired. Failing open
/// here would let a half-migrated instance trade the same wallet as the instance
/// replacing it.
pub fn read_retired(data: &Path) -> Option<RetiredRecord> {
    let path = retired_path(data);
    if !path.exists() {
        return None;
    }
    Some(read_json(&path).unwrap_or(RetiredRecord {
        retired_at: String::new(),
        reason: "retired flag present but unreadable".to_string(),
    }))
}

/// Load the flag from `$DRADIS_DATA_DIR` at startup, before anything can trade.
pub fn load_retired_flag() -> Option<RetiredRecord> {
    let rec = read_retired(&data_dir());
    RETIRED.store(rec.is_some(), Ordering::Relaxed);
    rec
}

/// Retire this instance. The flag lives in the data volume, so it survives the
/// restarts an operator might do while copying the archive.
pub fn retire(reason: &str) -> Result<RetiredRecord> {
    let rec = RetiredRecord { retired_at: chrono::Utc::now().to_rfc3339(), reason: reason.to_string() };
    write_json_atomic(&retired_path(&data_dir()), &rec)?;
    RETIRED.store(true, Ordering::Relaxed);
    warn!("🛬 Instance retired for migration: new orders are refused and squadrons stay down, across restarts");
    Ok(rec)
}

/// Clear the flag. The caller restarts the engine so squadrons come back through
/// their normal startup path.
pub fn unretire() -> Result<()> {
    match fs::remove_file(retired_path(&data_dir())) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    RETIRED.store(false, Ordering::Relaxed);
    info!("🛫 Migration retirement cleared: this instance trades again after its restart");
    Ok(())
}

// ─── Manifest and records ────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ManifestFile {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Manifest {
    pub kind: String,
    pub schema_version: u32,
    pub app_version: String,
    pub venue: String,
    pub created_at: String,
    pub include_training_data: bool,
    pub trades: i64,
    pub open_positions: i64,
    pub files: Vec<ManifestFile>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LatestBackup {
    pub archive_name: String,
    pub archive_bytes: u64,
    pub manifest: Manifest,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Pending {
    pub staged_at: String,
    pub manifest: Manifest,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RestoreReport {
    pub applied_at: String,
    pub backup_dir: String,
    pub restored: Vec<String>,
    pub source_created_at: String,
    pub source_app_version: String,
    pub source_trades: i64,
    pub source_open_positions: i64,
    pub secrets_merged: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct BackupProgress {
    /// `retiring`, `cancelling_orders`, `snapshotting`, `copying`, `archiving`, `ready` or `failed`.
    pub phase: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub error: Option<String>,
}

/// Everything the Control Tower's migration panel shows, read from disk so it is
/// right after a restart.
#[derive(Serialize, Clone, Debug)]
pub struct LocalState {
    pub retired: Option<RetiredRecord>,
    pub backup: Option<BackupProgress>,
    pub latest_backup: Option<LatestBackup>,
    pub restore_staged: Option<Pending>,
    pub last_restore: Option<RestoreReport>,
    pub restore_failed: Option<serde_json::Value>,
}

pub fn local_state(work: &Path) -> LocalState {
    let mig = migration_dir(work);
    LocalState {
        retired: read_retired(&data_dir()),
        backup: progress(),
        latest_backup: read_json::<LatestBackup>(&mig.join(LATEST_BACKUP_FILE))
            .filter(|b| mig.join(&b.archive_name).exists()),
        restore_staged: read_json(&mig.join(PENDING_FILE)),
        last_restore: read_json(&mig.join(REPORT_FILE)),
        restore_failed: read_json(&mig.join(FAILED_FILE)),
    }
}

pub fn progress() -> Option<BackupProgress> {
    PROGRESS.lock().ok().and_then(|p| p.clone())
}

pub fn set_progress(phase: &str) {
    if let Ok(mut p) = PROGRESS.lock() {
        let now = chrono::Utc::now().to_rfc3339();
        let entry = p.get_or_insert_with(BackupProgress::default);
        if phase == "retiring" {
            *entry = BackupProgress { started_at: Some(now.clone()), ..Default::default() };
        }
        entry.phase = phase.to_string();
        if phase == "ready" {
            entry.finished_at = Some(now);
        }
    }
}

pub fn fail_progress(error: &str) {
    if let Ok(mut p) = PROGRESS.lock() {
        let entry = p.get_or_insert_with(BackupProgress::default);
        entry.phase = "failed".to_string();
        entry.error = Some(error.to_string());
        entry.finished_at = Some(chrono::Utc::now().to_rfc3339());
    }
}

/// Is a backup being built right now?
pub fn backup_in_progress() -> bool {
    progress().is_some_and(|p| !matches!(p.phase.as_str(), "ready" | "failed" | ""))
}

/// Stop offering the previous backup the moment a new one is requested.
///
/// `latest-backup.json` is the only thing that makes an archive downloadable,
/// and it lives on disk while the build's progress lives in memory (`PROGRESS`).
/// Anything that restarts the engine — applying a restore does exactly that —
/// clears the progress but leaves the pointer, so the Control Tower shows the
/// previous archive as a finished, downloadable backup with no build running.
///
/// On a fresh instance that stale archive is harmless. On one that has since
/// restored a real ledger it is a near-empty backup of the instance as it was
/// before, presented in the same panel, with the same button, as the operator's
/// actual data. 2026-09-17: an operator migrating off a restored instance
/// downloaded a 19,863-byte, zero-trade archive that way while the instance held
/// 52 trades, and only the manifest inside it revealed which one it was.
///
/// Clearing the pointer first means a stale ledger can never be served as
/// current: until the new backup finishes there is simply nothing to download.
/// The archive bytes are left alone — `build_backup` removes the superseded file
/// once the replacement is written.
pub fn invalidate_latest_backup(work: &Path) {
    let pointer = migration_dir(work).join(LATEST_BACKUP_FILE);
    match fs::remove_file(&pointer) {
        Ok(()) => info!("📦 Previous backup withdrawn; it is superseded by the one now being built"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => warn!("📦 Could not withdraw the previous backup pointer {}: {e}", pointer.display()),
    }
}

/// The archive `latest-backup.json` names, if it is still on disk.
pub fn latest_archive(work: &Path) -> Option<(PathBuf, LatestBackup)> {
    let mig = migration_dir(work);
    let latest: LatestBackup = read_json(&mig.join(LATEST_BACKUP_FILE))?;
    let path = mig.join(&latest.archive_name);
    path.exists().then_some((path, latest))
}

/// Remove a staged restore that the operator decided not to apply.
pub fn discard_staged_restore(work: &Path) {
    let mig = migration_dir(work);
    let _ = fs::remove_file(mig.join(PENDING_FILE));
    let _ = fs::remove_dir_all(mig.join(STAGING_DIR));
    let _ = fs::remove_file(mig.join(INCOMING_ARCHIVE));
}

// ─── Paths, versions, checksums ──────────────────────────────────────────────

/// Only the files a backup is allowed to carry, at the paths they occupy on an
/// instance: databases and GBoost models directly under `logs/`, anything under
/// `logs/gboost_planb/<asset>/` except the last training candidate, and the
/// config bundle. Everything else in an archive is refused, which is what keeps
/// an archive from writing outside `logs/` or over this instance's identity.
pub fn allowed_payload_path(p: &str) -> bool {
    let normal = !p.is_empty()
        && !p.contains('\\')
        && Path::new(p).components().all(|c| matches!(c, Component::Normal(_)));
    if !normal {
        return false;
    }
    let parts: Vec<&str> = p.split('/').collect();
    match parts.as_slice() {
        ["bundle.json"] => true,
        ["logs", f] => f.ends_with("-dradis.db") || f.ends_with("-gboost_planb_v1.json"),
        ["logs", "gboost_planb", _asset, rest @ ..] => !rest.is_empty() && rest != ["candidate.json"],
        _ => false,
    }
}

fn version_tuple(v: &str) -> Option<(u64, u64, u64)> {
    let core = v.split(['-', '+']).next()?;
    let mut it = core.split('.');
    let t = (it.next()?.parse().ok()?, it.next()?.parse().ok()?, it.next()?.parse().ok()?);
    it.next().is_none().then_some(t)
}

/// A backup restores onto the same version or a newer one, never an older one:
/// an older build would open a database whose schema it has never seen.
/// Pre-release tags are ignored, so a 1.2.0 backup restores onto 1.2.0-rc.1.
pub fn backup_is_restorable_on(backup_version: &str, running_version: &str) -> bool {
    matches!((version_tuple(backup_version), version_tuple(running_version)), (Some(b), Some(r)) if b <= r)
}

fn sha256_file(path: &Path) -> Result<(u64, String)> {
    let mut f = fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut total = 0u64;
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total += n as u64;
    }
    Ok((total, format!("{:x}", hasher.finalize())))
}

fn walk_files(root: &Path, out: &mut Vec<PathBuf>) -> io::Result<()> {
    if !root.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            walk_files(&entry.path(), out)?;
        } else if kind.is_file() {
            out.push(entry.path());
        }
    }
    Ok(())
}

fn rel(base: &Path, p: &Path) -> String {
    p.strip_prefix(base).unwrap_or(p).to_string_lossy().replace('\\', "/")
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(value)?)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Option<T> {
    fs::read(path).ok().and_then(|b| serde_json::from_slice(&b).ok())
}

// ─── Backup (old instance) ───────────────────────────────────────────────────

pub struct BackupInputs {
    pub include_training_data: bool,
    pub venue: String,
    pub app_version: String,
    /// The config bundle `api/setup.rs` exports (credentials and settings).
    pub bundle: serde_json::Value,
    /// Every open database pool. Each distinct file is snapshotted once.
    pub pools: Vec<sqlx::SqlitePool>,
}

/// Snapshot the databases, copy the models (and the training data when asked),
/// write the manifest and pack everything into `logs/migration/dradis-backup-*.tar.gz`.
pub async fn build_backup(work: &Path, inputs: BackupInputs) -> Result<(PathBuf, Manifest)> {
    let stamp = chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let mig = migration_dir(work);
    let staging = mig.join(format!("staging-{stamp}"));
    fs::create_dir_all(staging.join("logs"))?;

    let result = build_into(work, &staging, &stamp, inputs).await;
    let _ = fs::remove_dir_all(&staging);
    result
}

async fn build_into(work: &Path, staging: &Path, stamp: &str, inputs: BackupInputs) -> Result<(PathBuf, Manifest)> {
    let BackupInputs { include_training_data, venue, app_version, bundle, pools } = inputs;

    // Decide what travels before writing anything, so a volume too small for
    // the backup is refused up front instead of filling part-way.
    let sources = backup_sources(work, include_training_data)?;
    let mut estimate: u64 = sources.iter().filter_map(|p| fs::metadata(p).ok()).map(|m| m.len()).sum();
    for pool in &pools {
        estimate += fs::metadata(pool.connect_options().get_filename()).map(|m| m.len()).unwrap_or(0);
    }
    // A staging copy of everything, then the archive: at most twice the data.
    ensure_free_space(staging, estimate.saturating_mul(2))?;

    set_progress("snapshotting");
    let (mut trades, mut open_positions) = (0i64, 0i64);
    for pool in &pools {
        let live = pool.connect_options().get_filename().to_path_buf();
        let Some(name) = live.file_name().map(|n| n.to_string_lossy().to_string()) else { continue };
        if !name.ends_with("-dradis.db") {
            continue;
        }
        let dest = staging.join("logs").join(&name);
        if dest.exists() {
            continue;
        }
        // A consistent copy of a database that is open and being written: SQLite
        // builds it inside one read transaction, and the result has no WAL.
        let sql = format!("VACUUM INTO '{}'", dest.to_string_lossy().replace('\'', "''"));
        sqlx::query(&sql)
            .execute(pool)
            .await
            .with_context(|| format!("snapshotting {}", live.display()))?;
        // The manifest's counts are what the operator checks the restore against,
        // so a count that cannot be read fails the backup rather than reading 0.
        trades += sqlx::query_scalar::<_, i64>("SELECT count(*) FROM trades")
            .fetch_one(pool)
            .await
            .with_context(|| format!("counting trades in {}", live.display()))?;
        open_positions += sqlx::query_scalar::<_, i64>("SELECT count(*) FROM open_positions")
            .fetch_one(pool)
            .await
            .with_context(|| format!("counting open positions in {}", live.display()))?;
    }

    set_progress("copying");
    // Copying and hashing hundreds of megabytes would otherwise hold a runtime
    // worker for seconds.
    let (work_c, staging_c) = (work.to_path_buf(), staging.to_path_buf());
    let files = tokio::task::spawn_blocking(move || stage_files(&work_c, &staging_c, &sources, &bundle)).await??;
    if !files.iter().any(|f| f.path.ends_with("-dradis.db")) {
        bail!("no database is open on this instance, so there is nothing to back up");
    }
    let manifest = Manifest {
        kind: BACKUP_KIND.to_string(),
        schema_version: BACKUP_SCHEMA_VERSION,
        app_version,
        venue,
        created_at: chrono::Utc::now().to_rfc3339(),
        include_training_data,
        trades,
        open_positions,
        files,
    };
    fs::write(staging.join("manifest.json"), serde_json::to_vec_pretty(&manifest)?)?;

    set_progress("archiving");
    let mig = migration_dir(work);
    let archive_name = format!("dradis-backup-{}-{stamp}.tar.gz", manifest.venue);
    let archive = mig.join(&archive_name);
    let (staging_c, archive_c) = (staging.to_path_buf(), archive.clone());
    tokio::task::spawn_blocking(move || write_archive(&staging_c, &archive_c)).await??;
    // One backup at a time is kept: an older archive holds a ledger that is now stale.
    if let Some((old, _)) = latest_archive(work) {
        if old != archive {
            let _ = fs::remove_file(old);
        }
    }
    let archive_bytes = fs::metadata(&archive)?.len();
    write_json_atomic(
        &mig.join(LATEST_BACKUP_FILE),
        &LatestBackup { archive_name, archive_bytes, manifest: manifest.clone() },
    )?;
    set_progress("ready");
    info!(
        "📦 Instance backup ready: {} ({} files, {} trades, {} open positions, {} bytes)",
        archive.display(), manifest.files.len(), manifest.trades, manifest.open_positions, archive_bytes,
    );
    Ok((archive, manifest))
}

/// The model and training files a backup carries, as paths on this instance.
/// The serving models always travel, and so does each archived model, the only
/// copy of the model its serving one replaced. The rest of a GBoost asset folder
/// is data a new instance could rebuild, and travels when asked.
fn backup_sources(work: &Path, include_training_data: bool) -> Result<Vec<PathBuf>> {
    let logs = work.join("logs");
    let mut sources: Vec<PathBuf> = Vec::new();
    if let Ok(dir) = fs::read_dir(&logs) {
        for entry in dir.flatten() {
            let p = entry.path();
            if p.is_file() && p.file_name().is_some_and(|n| n.to_string_lossy().ends_with("-gboost_planb_v1.json")) {
                sources.push(p);
            }
        }
    }
    let mut training = Vec::new();
    walk_files(&logs.join("gboost_planb"), &mut training)?;
    for p in training {
        let r = rel(work, &p);
        if !allowed_payload_path(&r) || r.ends_with(".tmp") {
            continue;
        }
        if include_training_data || r.split('/').nth(3) == Some("archive") {
            sources.push(p);
        }
    }
    Ok(sources)
}

/// Copy `sources` and the config bundle into `staging` (which already holds the
/// database snapshots) and return the manifest entry for every staged file.
fn stage_files(work: &Path, staging: &Path, sources: &[PathBuf], bundle: &serde_json::Value) -> Result<Vec<ManifestFile>> {
    for src in sources {
        let r = rel(work, src);
        let dest = staging.join(&r);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(src, &dest).with_context(|| format!("copying {r}"))?;
    }
    write_json_atomic(&staging.join("bundle.json"), bundle)?;

    let mut staged = Vec::new();
    walk_files(staging, &mut staged)?;
    staged.sort();
    let mut files = Vec::new();
    for p in &staged {
        let (bytes, sha256) = sha256_file(p)?;
        files.push(ManifestFile { path: rel(staging, p), bytes, sha256 });
    }
    Ok(files)
}

/// Headroom left on the volume after a backup or restore has written what it needs.
const SPACE_MARGIN_BYTES: u64 = 256 * 1024 * 1024;

/// Refuse before writing when the volume holding `dir` cannot take `needed`
/// bytes plus a margin. A Marketplace instance's root volume is fixed at launch,
/// and a backup or restore that fills it part-way leaves the engine unable to
/// write its own database.
pub fn ensure_free_space(dir: &Path, needed: u64) -> Result<()> {
    let mut probe = dir.to_path_buf();
    while !probe.exists() {
        if !probe.pop() {
            probe = PathBuf::from(".");
            break;
        }
    }
    let available = fs4::available_space(&probe).with_context(|| format!("reading free space at {}", probe.display()))?;
    let required = needed.saturating_add(SPACE_MARGIN_BYTES);
    if available < required {
        const MB: u64 = 1024 * 1024;
        bail!(
            "not enough free disk space: this needs about {} MB and the volume has {} MB free",
            required.div_ceil(MB),
            available / MB,
        );
    }
    Ok(())
}

fn write_archive(staging: &Path, archive: &Path) -> Result<()> {
    let partial = archive.with_extension("partial");
    let file = fs::File::create(&partial)?;
    let gz = flate2::write::GzEncoder::new(io::BufWriter::new(file), flate2::Compression::fast());
    let mut tar = tar::Builder::new(gz);
    // The manifest goes first so a reader can refuse a wrong archive early.
    tar.append_path_with_name(staging.join("manifest.json"), "manifest.json")?;
    let mut files = Vec::new();
    walk_files(staging, &mut files)?;
    files.sort();
    for p in files {
        let r = rel(staging, &p);
        if r != "manifest.json" {
            tar.append_path_with_name(&p, &r)?;
        }
    }
    let writer = tar.into_inner()?.finish()?;
    let file = writer.into_inner().map_err(|e| anyhow!("flushing the archive: {e}"))?;
    file.sync_all()?;
    fs::rename(&partial, archive)?;
    Ok(())
}

// ─── Restore (new instance) ──────────────────────────────────────────────────

#[derive(Debug)]
pub enum StageError {
    /// This instance already has trades; restoring would replace its ledger.
    NeedsOverwrite { existing_trades: i64 },
    /// The archive is not one this instance can restore.
    Invalid(String),
}

impl std::fmt::Display for StageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StageError::NeedsOverwrite { existing_trades } => write!(
                f,
                "this instance already has {existing_trades} trade(s); restoring replaces its ledger, so confirm the overwrite"
            ),
            StageError::Invalid(m) => f.write_str(m),
        }
    }
}

/// Extract and verify an uploaded archive, then mark it pending for the next boot.
///
/// Any refusal removes the upload and everything extracted from it: an archive
/// meant for another instance carries that instance's credentials in its
/// `bundle.json`, and a rejected one must not leave them on the logs volume.
pub fn stage_restore(
    work: &Path,
    archive: &Path,
    venue: &str,
    app_version: &str,
    existing_trades: i64,
    overwrite: bool,
) -> std::result::Result<Manifest, StageError> {
    let staged = stage_restore_checked(work, archive, venue, app_version, existing_trades, overwrite);
    if staged.is_err() {
        discard_staged_restore(work);
    }
    staged
}

fn stage_restore_checked(
    work: &Path,
    archive: &Path,
    venue: &str,
    app_version: &str,
    existing_trades: i64,
    overwrite: bool,
) -> std::result::Result<Manifest, StageError> {
    let invalid = StageError::Invalid;
    let mig = migration_dir(work);
    let staging = mig.join(STAGING_DIR);
    let _ = fs::remove_dir_all(&staging);
    let _ = fs::remove_file(mig.join(PENDING_FILE));
    fs::create_dir_all(&staging).map_err(|e| invalid(format!("creating the staging folder: {e}")))?;

    let file = fs::File::open(archive).map_err(|e| invalid(format!("opening the upload: {e}")))?;
    let mut reader = tar::Archive::new(flate2::read::GzDecoder::new(io::BufReader::new(file)));
    let entries = reader.entries().map_err(|e| invalid(format!("not a DRADIS backup archive: {e}")))?;
    let mut expanded = 0u64;
    for entry in entries {
        let mut entry = entry.map_err(|e| invalid(format!("the archive is damaged: {e}")))?;
        let kind = entry.header().entry_type();
        let path = entry
            .path()
            .map_err(|e| invalid(format!("the archive has an unreadable path: {e}")))?
            .to_string_lossy()
            .replace('\\', "/");
        if kind.is_dir() {
            continue;
        }
        if !kind.is_file() {
            return Err(invalid(format!("the archive entry {path} is not a regular file")));
        }
        if path != "manifest.json" && !allowed_payload_path(&path) {
            return Err(invalid(format!("the archive carries a file a backup may not contain: {path}")));
        }
        expanded = expanded.saturating_add(entry.header().size().unwrap_or(0));
        // Market data and klines compress three to five times, so twice the
        // largest upload is already far beyond any real backup.
        if expanded > MAX_ARCHIVE_BYTES * 2 {
            return Err(invalid("the archive expands beyond the size limit".to_string()));
        }
        let dest = staging.join(&path);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).map_err(|e| invalid(format!("extracting {path}: {e}")))?;
        }
        let mut out = fs::File::create(&dest).map_err(|e| invalid(format!("extracting {path}: {e}")))?;
        io::copy(&mut entry, &mut out).map_err(|e| invalid(format!("extracting {path}: {e}")))?;
    }

    let manifest: Manifest = read_json(&staging.join("manifest.json"))
        .ok_or_else(|| invalid("the archive has no readable manifest.json".to_string()))?;
    verify_manifest(&staging, &manifest, venue, app_version).map_err(invalid)?;
    if existing_trades > 0 && !overwrite {
        return Err(StageError::NeedsOverwrite { existing_trades });
    }
    write_json_atomic(
        &mig.join(PENDING_FILE),
        &Pending { staged_at: chrono::Utc::now().to_rfc3339(), manifest: manifest.clone() },
    )
    .map_err(|e| invalid(format!("recording the staged restore: {e}")))?;
    let _ = fs::remove_file(mig.join(FAILED_FILE));
    info!(
        "📦 Instance restore staged from a {} backup of {} ({} files, {} trades); applied at the next restart",
        manifest.app_version, manifest.created_at, manifest.files.len(), manifest.trades,
    );
    Ok(manifest)
}

/// Check a staged archive against its manifest and this instance.
pub fn verify_manifest(
    staging: &Path,
    m: &Manifest,
    venue: &str,
    app_version: &str,
) -> std::result::Result<(), String> {
    if m.kind != BACKUP_KIND {
        return Err("this is not a DRADIS instance backup".to_string());
    }
    if m.schema_version == 0 || m.schema_version > BACKUP_SCHEMA_VERSION {
        return Err(format!(
            "unsupported backup schema_version {} (this build reads 1 to {BACKUP_SCHEMA_VERSION})",
            m.schema_version
        ));
    }
    if m.venue != venue {
        return Err(format!("the backup is from a '{}' instance but this instance runs '{venue}'", m.venue));
    }
    if !backup_is_restorable_on(&m.app_version, app_version) {
        return Err(format!(
            "the backup was made by version {} and this instance runs {app_version}; restore it on the same or a newer version",
            m.app_version
        ));
    }
    let mut listed: BTreeMap<String, &ManifestFile> = BTreeMap::new();
    for f in &m.files {
        if !allowed_payload_path(&f.path) {
            return Err(format!("the manifest lists a file a backup may not contain: {}", f.path));
        }
        listed.insert(f.path.clone(), f);
    }
    if !listed.keys().any(|p| p.ends_with("-dradis.db")) {
        return Err("the backup contains no database".to_string());
    }
    let mut present = Vec::new();
    walk_files(staging, &mut present).map_err(|e| e.to_string())?;
    for p in present {
        let r = rel(staging, &p);
        if r == "manifest.json" {
            continue;
        }
        let Some(expected) = listed.remove(&r) else {
            return Err(format!("the archive contains {r}, which its manifest does not list"));
        };
        let (bytes, sha256) = sha256_file(&p).map_err(|e| e.to_string())?;
        if bytes != expected.bytes || sha256 != expected.sha256 {
            return Err(format!("{r} does not match its checksum: the archive is damaged or was altered"));
        }
    }
    if let Some(missing) = listed.keys().next() {
        return Err(format!("the manifest lists {missing}, which the archive does not contain"));
    }
    Ok(())
}

/// What a restore replaces. Databases and models are single files. A GBoost
/// asset folder is replaced whole when the backup carries its training data, so
/// none of this instance's partial data mixes with the restored set. When the
/// backup carries only the archived model, that file alone is restored and
/// whatever this instance has backfilled since it launched is kept.
fn restore_units(manifest: &Manifest) -> Vec<String> {
    let mut archive_only: BTreeMap<String, bool> = BTreeMap::new();
    for f in &manifest.files {
        let parts: Vec<&str> = f.path.split('/').collect();
        if parts.len() > 3 && parts[1] == "gboost_planb" {
            let only = archive_only.entry(parts[..3].join("/")).or_insert(true);
            *only = *only && parts[3] == "archive";
        }
    }
    let mut units: Vec<String> = Vec::new();
    for f in &manifest.files {
        if f.path == "bundle.json" {
            continue;
        }
        let parts: Vec<&str> = f.path.split('/').collect();
        let whole_folder = parts.len() > 3
            && parts[1] == "gboost_planb"
            && !archive_only.get(&parts[..3].join("/")).copied().unwrap_or(true);
        let unit = if whole_folder { parts[..3].join("/") } else { f.path.clone() };
        if !units.contains(&unit) {
            units.push(unit);
        }
    }
    units
}

/// Apply a staged restore, if one is pending. Runs at boot, before any database
/// is opened, so no pool holds a file this replaces.
///
/// See [`restore_units`] for what is replaced. Re-running after an interruption
/// is safe: units already moved in are skipped. Everything replaced, including a
/// database's `-wal` and
/// `-shm`, moves into `logs/migration/pre-restore-<time>/`. The config bundle's
/// credentials are merged through `merge_secrets`; this instance's own identity
/// (admin password, session key, TLS certificate) is never touched.
pub fn apply_pending_restore(
    work: &Path,
    merge_secrets: &dyn Fn(&serde_json::Value) -> Result<usize>,
) -> Result<Option<RestoreReport>> {
    let mig = migration_dir(work);
    let pending_path = mig.join(PENDING_FILE);
    if !pending_path.exists() {
        return Ok(None);
    }
    let Some(pending) = read_json::<Pending>(&pending_path) else {
        let _ = fs::rename(&pending_path, mig.join(FAILED_FILE));
        bail!("the staged restore record is unreadable; nothing was changed");
    };
    let staging = mig.join(STAGING_DIR);
    let stamp = chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let backup = mig.join(format!("pre-restore-{stamp}"));

    let applied = (|| -> Result<RestoreReport> {
        let units = restore_units(&pending.manifest);
        for unit in &units {
            // A unit whose staged copy is gone but whose target exists was moved in
            // by an earlier run of this same restore that stopped before clearing
            // the pending record. Only a unit missing from both places is a fault.
            if !staging.join(unit).exists() && !work.join(unit).exists() {
                bail!("the staged copy of {unit} is missing");
            }
        }
        fs::create_dir_all(&backup)?;
        for unit in &units {
            if !staging.join(unit).exists() {
                continue;
            }
            let target = work.join(unit);
            let mut displaced = vec![target.clone()];
            if unit.ends_with("-dradis.db") {
                displaced.push(PathBuf::from(format!("{}-wal", target.to_string_lossy())));
                displaced.push(PathBuf::from(format!("{}-shm", target.to_string_lossy())));
            }
            for d in displaced {
                if d.exists() {
                    let to = backup.join(rel(work, &d));
                    if let Some(parent) = to.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    fs::rename(&d, &to).with_context(|| format!("moving {} aside", d.display()))?;
                }
            }
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::rename(staging.join(unit), &target).with_context(|| format!("restoring {unit}"))?;
        }
        let bundle: serde_json::Value = read_json(&staging.join("bundle.json")).unwrap_or(serde_json::Value::Null);
        let secrets_merged = if bundle.is_object() { merge_secrets(&bundle)? } else { 0 };
        Ok(RestoreReport {
            applied_at: chrono::Utc::now().to_rfc3339(),
            backup_dir: rel(work, &backup),
            restored: units,
            source_created_at: pending.manifest.created_at.clone(),
            source_app_version: pending.manifest.app_version.clone(),
            source_trades: pending.manifest.trades,
            source_open_positions: pending.manifest.open_positions,
            secrets_merged,
        })
    })();

    match applied {
        Ok(report) => {
            write_json_atomic(&mig.join(REPORT_FILE), &report)?;
            let _ = fs::remove_file(&pending_path);
            let _ = fs::remove_dir_all(&staging);
            let _ = fs::remove_file(mig.join(INCOMING_ARCHIVE));
            Ok(Some(report))
        }
        Err(e) => {
            // Never retried at the next boot: a restore that failed part-way is for
            // an operator to look at, not for a restart loop to repeat.
            let _ = write_json_atomic(
                &mig.join(FAILED_FILE),
                &serde_json::json!({ "failed_at": chrono::Utc::now().to_rfc3339(), "error": format!("{e:#}"), "replaced_files_kept_in": rel(work, &backup) }),
            );
            let _ = fs::remove_file(&pending_path);
            Err(e.context(format!("the restore failed part-way; replaced files are kept in {}", backup.display())))
        }
    }
}

/// The boot hook `main` calls. Returns true when a restore was applied, so the
/// caller reloads the secrets it merged.
pub fn apply_pending_restore_at_boot() -> bool {
    let merge = |bundle: &serde_json::Value| crate::api::setup::merge_bundle_secrets(bundle);
    match apply_pending_restore(Path::new("."), &merge) {
        Ok(Some(r)) => {
            info!(
                "📦 Instance restore applied: {} item(s) from a {} backup made {} ({} trades, {} open positions); replaced files kept in {}",
                r.restored.len(), r.source_app_version, r.source_created_at, r.source_trades, r.source_open_positions, r.backup_dir,
            );
            true
        }
        Ok(None) => false,
        Err(e) => {
            error!("❌ Instance restore not applied: {e:#}");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh working directory per call. Unique by a counter, not by the clock:
    /// macOS reports time in microseconds, and tests running in parallel that
    /// started inside one microsecond were handed the same directory and built
    /// their backups over each other.
    fn temp_dir(name: &str) -> PathBuf {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("dradis-migration-{name}-{}-{n}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("logs")).unwrap();
        dir
    }

    async fn ledger(path: &Path, trades: usize, open: usize) -> sqlx::SqlitePool {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY, strategy TEXT)").execute(&pool).await.unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS open_positions (id INTEGER PRIMARY KEY, strategy TEXT)").execute(&pool).await.unwrap();
        for _ in 0..trades {
            sqlx::query("INSERT INTO trades (strategy) VALUES ('FairValueStrategy')").execute(&pool).await.unwrap();
        }
        for _ in 0..open {
            sqlx::query("INSERT INTO open_positions (strategy) VALUES ('FairValueStrategy')").execute(&pool).await.unwrap();
        }
        pool
    }

    fn write(path: &Path, body: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, body).unwrap();
    }

    /// An old instance as production held it: a ledger, the serving model, the
    /// archived model, training data and a last training candidate.
    async fn old_instance(include_training_data: bool) -> (PathBuf, PathBuf, Manifest) {
        let work = temp_dir("old");
        let pool = ledger(&work.join("logs/btc-dradis.db"), 3, 1).await;
        write(&work.join("logs/btc-gboost_planb_v1.json"), "{\"model\":\"serving\"}");
        write(&work.join("logs/gboost_planb/btc/markets/1789390800.json"), "{\"market\":1}");
        write(&work.join("logs/gboost_planb/btc/archive/incumbent.json"), "{\"model\":\"incumbent\"}");
        write(&work.join("logs/gboost_planb/btc/candidate.json"), "{\"model\":\"candidate\"}");
        let inputs = BackupInputs {
            include_training_data,
            venue: "intl".to_string(),
            app_version: "1.2.0".to_string(),
            bundle: serde_json::json!({ "kind": "dradis-config-bundle", "secrets": { "POLYMARKET_PRIVATE_KEY": "k" } }),
            pools: vec![pool],
        };
        let (archive, manifest) = build_backup(&work, inputs).await.unwrap();
        (work, archive, manifest)
    }

    #[test]
    fn only_backup_paths_are_allowed() {
        for ok in [
            "bundle.json",
            "logs/btc-dradis.db",
            "logs/kalshi-dradis.db",
            "logs/btc-gboost_planb_v1.json",
            "logs/gboost_planb/btc/archive/incumbent.json",
            "logs/gboost_planb/btc/markets/1789390800.json",
        ] {
            assert!(allowed_payload_path(ok), "{ok}");
        }
        for bad in [
            "",
            "../logs/btc-dradis.db",
            "/etc/passwd",
            "./logs/btc-dradis.db",
            "logs/../data/secrets.env",
            "data/secrets.env",
            "data/tls/key.pem",
            "logs/btc-dradis.log",
            "logs/migration/restore-pending.json",
            "logs/gboost_planb/btc/candidate.json",
            "logs/gboost_planb/btc",
            "logs\\btc-dradis.db",
        ] {
            assert!(!allowed_payload_path(bad), "{bad}");
        }
    }

    #[test]
    fn a_backup_restores_onto_the_same_or_a_newer_version_only() {
        assert!(backup_is_restorable_on("1.1.7", "1.2.0"));
        assert!(backup_is_restorable_on("1.2.0", "1.2.0"));
        assert!(backup_is_restorable_on("1.2.0-rc.1", "1.2.0"));
        assert!(!backup_is_restorable_on("1.2.1", "1.2.0"));
        assert!(!backup_is_restorable_on("2.0.0", "1.9.9"));
        assert!(!backup_is_restorable_on("garbage", "1.2.0"));
    }

    #[test]
    fn a_retired_instance_refuses_orders_and_an_unreadable_flag_counts_as_retired() {
        assert!(refusal(false).is_ok());
        assert_eq!(refusal(true).unwrap_err().to_string(), RETIRED_REFUSAL);

        let data = temp_dir("flag");
        assert_eq!(read_retired(&data), None);
        write(&retired_path(&data), "not json");
        assert!(read_retired(&data).is_some(), "a flag that exists must hold, however it reads");
        let rec = RetiredRecord { retired_at: "2026-09-15T00:00:00Z".into(), reason: "migration".into() };
        write_json_atomic(&retired_path(&data), &rec).unwrap();
        assert_eq!(read_retired(&data), Some(rec));
    }

    /// 2026-09-17: an operator migrating off a restored instance pressed backup
    /// and was handed the 19,863-byte, zero-trade archive the instance had made
    /// before its restore, while it held 52 trades. `latest-backup.json` lives on
    /// disk and the build progress lives in memory, so the restore's own restart
    /// cleared the progress and left the pointer, and the panel showed a finished
    /// backup that was not the ledger. Requesting a new backup must withdraw the
    /// old one first, so a stale ledger can never be downloaded as the current one.
    #[tokio::test]
    async fn requesting_a_backup_withdraws_the_previous_one() {
        let (old, archive, manifest) = old_instance(true).await;

        // The instance is offering a finished backup, exactly as after a restart.
        assert!(latest_archive(&old).is_some(), "the fixture should start with a downloadable backup");
        assert!(archive.exists());
        assert_eq!(manifest.trades, 3);

        invalidate_latest_backup(&old);

        // Nothing is downloadable any more: the pointer is what serves an archive.
        assert!(
            latest_archive(&old).is_none(),
            "a superseded backup must not stay downloadable while a new one is built",
        );
        assert!(
            !migration_dir(&old).join(LATEST_BACKUP_FILE).exists(),
            "the pointer file itself should be gone",
        );
        // The bytes are left for `build_backup` to clear once it has a replacement.
        assert!(archive.exists(), "the archive file is cleaned up by the next successful build, not here");

        // Idempotent: a second request, or one on an instance that never backed
        // up, must not error.
        invalidate_latest_backup(&old);
        invalidate_latest_backup(&temp_dir("never-backed-up"));
    }

    /// The whole operator round trip, end to end, because the last step of it had
    /// never been executed by anything until 2026-09-17, when an operator ran it
    /// for real and it handed back the wrong ledger.
    ///
    /// A new instance backs itself up while nearly empty, restores a fuller
    /// backup, and is then asked for another backup — the migrate-again case. The
    /// second backup must describe the restored ledger, and at no point may the
    /// pre-restore archive be downloadable as the current one.
    #[tokio::test]
    async fn backing_up_again_after_a_restore_captures_the_restored_ledger() {
        // A new instance with a ledger of its own, backed up before any restore.
        let new = temp_dir("migrate-again");
        let fresh = ledger(&new.join("logs/btc-dradis.db"), 0, 0).await;
        let first = build_backup(&new, BackupInputs {
            include_training_data: true,
            venue: "intl".to_string(),
            app_version: "1.2.0".to_string(),
            bundle: serde_json::json!({ "kind": "dradis-config-bundle", "secrets": {} }),
            pools: vec![fresh.clone()],
        }).await.unwrap().1;
        assert_eq!(first.trades, 0, "the pre-restore backup is the near-empty one");
        let (stale_path, stale) = latest_archive(&new).expect("the instance is offering its own backup");
        assert_eq!(stale.manifest.trades, 0);
        fresh.close().await;

        // It restores a real ledger from elsewhere. Applying a restore restarts
        // the engine in production, which is what clears the in-memory progress
        // and leaves the on-disk pointer behind.
        let (_old, archive, source) = old_instance(true).await;
        stage_restore(&new, &archive, "intl", "1.2.0", 0, true).unwrap();
        let merge = |_: &serde_json::Value| Ok(0usize);
        let report = apply_pending_restore(&new, &merge).unwrap().expect("the restore applies");
        assert_eq!(report.source_trades, source.trades);

        // The operator now migrates onward and asks for a backup. First the
        // previous one is withdrawn, exactly as `prepare` does.
        invalidate_latest_backup(&new);
        assert!(
            latest_archive(&new).is_none(),
            "the pre-restore archive must not be downloadable once a new backup is requested",
        );

        let restored = ledger(&new.join("logs/btc-dradis.db"), 0, 0).await;
        let (second_path, second) = build_backup(&new, BackupInputs {
            include_training_data: true,
            venue: "intl".to_string(),
            app_version: "1.2.0".to_string(),
            bundle: serde_json::json!({ "kind": "dradis-config-bundle", "secrets": {} }),
            pools: vec![restored.clone()],
        }).await.unwrap();
        restored.close().await;

        // The second backup is the restored ledger, not the empty one it replaced.
        assert_eq!(
            second.trades, source.trades,
            "backing up after a restore must capture the restored ledger, not the pre-restore one",
        );
        // Archive names carry second resolution, so a test that builds both
        // backups inside one second gets one filename for both and the second
        // simply replaces the first in place. Assert on content, which is what
        // actually matters, rather than on the paths differing.
        let _ = (&stale_path, &second_path);

        // And what the panel would now offer is that second backup.
        let (offered_path, offered) = latest_archive(&new).expect("the new backup is downloadable");
        assert_eq!(offered_path, second_path);
        assert_eq!(offered.manifest.trades, source.trades);

        // The models and training data came across the restore and travel again.
        let paths: Vec<&str> = second.files.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"logs/btc-gboost_planb_v1.json"), "the restored model travels onward: {paths:?}");
        assert!(paths.contains(&"logs/gboost_planb/btc/markets/1789390800.json"), "restored training data travels onward");
    }

    #[tokio::test]
    async fn a_backup_moves_the_ledger_models_and_training_data_onto_a_new_instance() {
        let (_old, archive, manifest) = old_instance(true).await;
        let paths: Vec<&str> = manifest.files.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"logs/btc-dradis.db"));
        assert!(paths.contains(&"logs/btc-gboost_planb_v1.json"));
        assert!(paths.contains(&"logs/gboost_planb/btc/archive/incumbent.json"));
        assert!(paths.contains(&"logs/gboost_planb/btc/markets/1789390800.json"));
        assert!(paths.contains(&"bundle.json"));
        assert!(!paths.iter().any(|p| p.ends_with("candidate.json")), "the last candidate never travels");
        assert_eq!((manifest.trades, manifest.open_positions), (3, 1));

        // The new instance already ran for a while: a fresh ledger with a WAL, and
        // some training data of its own that must not mix with the restored set.
        let new = temp_dir("new");
        let fresh = ledger(&new.join("logs/btc-dradis.db"), 0, 0).await;
        fresh.close().await;
        write(&new.join("logs/btc-dradis.db-wal"), "stale wal");
        write(&new.join("logs/gboost_planb/btc/markets/1789000000.json"), "{\"market\":0}");

        let staged = stage_restore(&new, &archive, "intl", "1.2.0", 0, false).unwrap();
        assert_eq!(staged.files.len(), manifest.files.len());

        let merged = std::cell::Cell::new(0usize);
        let merge = |bundle: &serde_json::Value| {
            merged.set(bundle["secrets"].as_object().map(|o| o.len()).unwrap_or(0));
            Ok(merged.get())
        };
        let report = apply_pending_restore(&new, &merge).unwrap().expect("a pending restore is applied");
        assert_eq!(report.secrets_merged, 1);
        assert_eq!((report.source_trades, report.source_open_positions), (3, 1));

        let restored = ledger(&new.join("logs/btc-dradis.db"), 0, 0).await;
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM trades").fetch_one(&restored).await.unwrap();
        assert_eq!(n, 3, "the ledger came across");
        assert!(new.join("logs/btc-gboost_planb_v1.json").exists());
        assert!(new.join("logs/gboost_planb/btc/archive/incumbent.json").exists());
        assert!(new.join("logs/gboost_planb/btc/markets/1789390800.json").exists());
        assert!(!new.join("logs/gboost_planb/btc/markets/1789000000.json").exists(), "the asset folder is replaced whole");
        assert!(!new.join("logs/btc-dradis.db-wal").exists(), "a stale WAL never sits beside a restored database");

        let kept = new.join(&report.backup_dir);
        assert!(kept.join("logs/btc-dradis.db").exists(), "the replaced ledger is kept, not deleted");
        assert!(kept.join("logs/btc-dradis.db-wal").exists());
        assert!(kept.join("logs/gboost_planb/btc/markets/1789000000.json").exists());

        assert!(apply_pending_restore(&new, &merge).unwrap().is_none(), "a restore is applied once");
        assert!(read_json::<RestoreReport>(&migration_dir(&new).join(REPORT_FILE)).is_some());
    }

    #[test]
    fn a_volume_that_cannot_hold_the_work_is_refused_before_anything_is_written() {
        let dir = temp_dir("space");
        assert!(ensure_free_space(&dir, 0).is_ok());
        let refused = ensure_free_space(&dir, u64::MAX / 2).unwrap_err().to_string();
        assert!(refused.starts_with("not enough free disk space"), "{refused}");
        // A folder that does not exist yet is measured at its nearest existing parent.
        assert!(ensure_free_space(&dir.join("logs/migration/not-yet"), 0).is_ok());
    }

    #[tokio::test]
    async fn a_rejected_archive_leaves_nothing_on_disk() {
        let (_old, archive, _manifest) = old_instance(true).await;
        let new = temp_dir("rejected");
        let upload = migration_dir(&new).join(INCOMING_ARCHIVE);
        fs::create_dir_all(upload.parent().unwrap()).unwrap();
        fs::copy(&archive, &upload).unwrap();

        assert!(stage_restore(&new, &upload, "kalshi", "1.2.0", 0, false).is_err());
        let mig = migration_dir(&new);
        assert!(!mig.join(STAGING_DIR).exists(), "the foreign bundle.json and its credentials are gone");
        assert!(!upload.exists(), "the upload is gone");
        assert!(!mig.join(PENDING_FILE).exists());
    }

    #[tokio::test]
    async fn a_restore_interrupted_after_moving_files_in_completes_without_a_false_failure() {
        let (_old, archive, _manifest) = old_instance(true).await;
        let new = temp_dir("interrupted");
        let staged = stage_restore(&new, &archive, "intl", "1.2.0", 0, false).unwrap();
        let no_secrets = |_: &serde_json::Value| Ok(0usize);
        apply_pending_restore(&new, &no_secrets).unwrap().unwrap();

        // The engine stopped after moving everything in and before the pending
        // record was cleared: the record is back, the staged copies are not.
        write_json_atomic(
            &migration_dir(&new).join(PENDING_FILE),
            &Pending { staged_at: chrono::Utc::now().to_rfc3339(), manifest: staged },
        )
        .unwrap();
        let rerun = apply_pending_restore(&new, &no_secrets).unwrap();
        assert!(rerun.is_some(), "the re-run completes the restore");
        assert!(!migration_dir(&new).join(FAILED_FILE).exists(), "a restore that succeeded is never recorded as failed");
        assert!(!migration_dir(&new).join(PENDING_FILE).exists());
        assert!(new.join("logs/btc-dradis.db").exists());
    }

    #[tokio::test]
    async fn without_training_data_the_new_instances_own_backfill_is_kept() {
        let (_old, archive, _manifest) = old_instance(false).await;
        let new = temp_dir("keep-backfill");
        write(&new.join("logs/gboost_planb/btc/markets/1789000000.json"), "{\"market\":0}");
        write(&new.join("logs/gboost_planb/btc/status.json"), "{\"phase\":\"idle\"}");

        stage_restore(&new, &archive, "intl", "1.2.0", 0, false).unwrap();
        let no_secrets = |_: &serde_json::Value| Ok(0usize);
        apply_pending_restore(&new, &no_secrets).unwrap().unwrap();

        assert!(new.join("logs/gboost_planb/btc/archive/incumbent.json").exists(), "the archived model came across");
        assert!(new.join("logs/btc-gboost_planb_v1.json").exists(), "the serving model came across");
        assert!(new.join("logs/gboost_planb/btc/markets/1789000000.json").exists(), "this instance's backfill is kept");
        assert!(new.join("logs/gboost_planb/btc/status.json").exists());
    }

    #[tokio::test]
    async fn leaving_the_training_data_out_still_carries_both_models() {
        let (_old, _archive, manifest) = old_instance(false).await;
        let paths: Vec<&str> = manifest.files.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"logs/btc-gboost_planb_v1.json"));
        assert!(paths.contains(&"logs/gboost_planb/btc/archive/incumbent.json"));
        assert!(!paths.contains(&"logs/gboost_planb/btc/markets/1789390800.json"));
        assert!(!manifest.include_training_data);
    }

    #[tokio::test]
    async fn a_wrong_venue_an_older_build_an_existing_ledger_and_an_altered_file_are_refused() {
        let (_old, archive, _manifest) = old_instance(true).await;
        let new = temp_dir("refusals");

        let venue = stage_restore(&new, &archive, "kalshi", "1.2.0", 0, false).unwrap_err().to_string();
        assert!(venue.contains("'intl' instance"), "{venue}");

        let older = stage_restore(&new, &archive, "intl", "1.1.7", 0, false).unwrap_err().to_string();
        assert!(older.contains("same or a newer version"), "{older}");

        match stage_restore(&new, &archive, "intl", "1.2.0", 12, false) {
            Err(StageError::NeedsOverwrite { existing_trades: 12 }) => {}
            other => panic!("an instance with trades must ask before its ledger is replaced, got {other:?}"),
        }
        assert!(stage_restore(&new, &archive, "intl", "1.2.0", 12, true).is_ok());

        // Alter one staged file after extraction: the checksum catches it.
        let staging = migration_dir(&new).join(STAGING_DIR);
        let manifest: Manifest = read_json(&staging.join("manifest.json")).unwrap();
        write(&staging.join("logs/btc-gboost_planb_v1.json"), "{\"model\":\"tampered\"}");
        let altered = verify_manifest(&staging, &manifest, "intl", "1.2.0").unwrap_err();
        assert!(altered.contains("does not match its checksum"), "{altered}");

        // A file the manifest does not list is refused too.
        write(&staging.join("logs/eth-dradis.db"), "extra");
        let mut honest = manifest.clone();
        honest.files.retain(|f| f.path != "logs/btc-gboost_planb_v1.json");
        let _ = fs::remove_file(staging.join("logs/btc-gboost_planb_v1.json"));
        let extra = verify_manifest(&staging, &honest, "intl", "1.2.0").unwrap_err();
        assert!(extra.contains("does not list"), "{extra}");
    }
}
