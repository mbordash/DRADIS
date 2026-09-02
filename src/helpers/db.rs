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

/// SQLite persistence layer for DRADIS.
///
/// Provides:
///   - Async connection pool (one shared pool via OnceLock)
///   - Schema initialization (trades, entries, pnl_snapshots, config, sessions, config_history)
///   - Write helpers for trades, entries, and P&L snapshots
///   - Key-value store for DynamicConfig JSON blobs
///   - Session tracking: each process start is a distinct session
///   - Config change audit log: full history of every DynamicConfig mutation
///     · `startup_dynamic`  — DynamicConfig (runtime-tunable params) at session start
///     · `startup_static`   — compile-time constants from config.rs at session start
///       (these can only change with a recompile; snapshotted so developers can diff
///        what was active across sessions and correlate constant changes with P&L shifts)
///     · `operator`         — Control Tower PATCH /api/config change
///     · `llm_advisor`      — recommendation applied by operator
///   - Lookup helper for entry price recovery (faster than CSV scan)
///
/// Call `db::init("logs/dradis.db")` once at startup before any other DB calls.
/// All other functions silently no-op if the pool is not yet initialized.

use std::sync::OnceLock;
use sqlx::{SqlitePool, sqlite::SqlitePoolOptions, Row};
use rust_decimal::Decimal;
use chrono::{DateTime, Utc};
use anyhow::Result;
use serde::Serialize;
use tracing::{error, info, debug, warn};

use crate::config;
use crate::state::TradeScope;

// ─── Shared pool ────────────────────────────────────────────────────────────

/// Primary-asset pool — the first asset initialized owns this slot.
/// Kept for backward-compat callers that use `pool()` without an asset key.
static DB_POOL: OnceLock<SqlitePool> = OnceLock::new();

/// Per-asset pool registry.  Key = lowercase asset symbol (e.g. "btc", "eth").
/// Populated by `init_for_asset()` at startup; readable thereafter.
static DB_POOLS: OnceLock<std::sync::Mutex<std::collections::HashMap<String, SqlitePool>>> =
    OnceLock::new();

/// Convenience accessor for the per-asset pool map (lazy-initialized on first call).
fn pools_map() -> &'static std::sync::Mutex<std::collections::HashMap<String, SqlitePool>> {
    DB_POOLS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// shard key → owning venue, populated by `init_shard`.
fn shard_venues() -> &'static std::sync::Mutex<std::collections::HashMap<String, String>> {
    static V: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, String>>> =
        std::sync::OnceLock::new();
    V.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// database file path → owning venue, populated by `init_shard`.
///
/// Reconciliation paths inside this module hold a `SqlitePool` but no shard key,
/// and threading one through every caller (including the settlement and purge
/// helpers) would be churn for no gain. One file is one shard is one venue, so
/// the file path recovers the venue exactly.
fn path_venues() -> &'static std::sync::Mutex<std::collections::HashMap<String, String>> {
    static V: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, String>>> =
        std::sync::OnceLock::new();
    V.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Venue that owns whatever database this pool is connected to.
pub fn venue_for_pool(pool: &SqlitePool) -> String {
    let file = pool.connect_options().get_filename().to_string_lossy().to_string();
    path_venues()
        .lock()
        .ok()
        .and_then(|m| m.get(&file).cloned())
        .unwrap_or_default()
}

/// Which venue owns a shard. Empty string when the shard was initialized
/// without one (tests, or the legacy `init_for_asset` path).
pub fn venue_for_shard(shard: &str) -> String {
    shard_venues()
        .lock()
        .ok()
        .and_then(|m| m.get(&shard.to_lowercase()).cloned())
        .unwrap_or_default()
}

/// The venue to file a row under: whatever the caller stated, else the venue
/// bound to the shard at init. `None` (rather than `""`) when neither is known,
/// so the column stays honestly NULL instead of holding an empty string.
fn resolved_venue(scope: &TradeScope) -> Option<String> {
    let v = if scope.venue.is_empty() { venue_for_shard(&scope.shard) } else { scope.venue.clone() };
    (!v.is_empty()).then_some(v)
}

/// The session ID for the current process lifetime.  Set once by `init_session()`
/// and remains stable for the entire run.  Format: RFC-3339 timestamp so it is
/// human-readable and lexicographically sortable.
static CURRENT_SESSION_ID: OnceLock<String> = OnceLock::new();

/// Returns the current session ID, or "unknown" if not yet initialized.
pub fn current_session_id() -> &'static str {
    CURRENT_SESSION_ID.get().map(|s| s.as_str()).unwrap_or("unknown")
}

/// Initialize the SQLite connection pool for a specific asset and register it
/// in the per-asset registry.
///
/// The **first** call designates that asset as the "primary" — `pool()` returns
/// its pool for backward-compat callers (API handlers, cleanup tasks, etc.).
/// Subsequent calls add additional asset pools without overwriting the primary.
///
/// `asset` should be a lowercase symbol, e.g. `"btc"`, `"eth"`, `"sol"`.
pub async fn init_for_asset(asset: &str, path: &str) -> Result<()> {
    init_shard(asset, path, "").await
}

/// Initialize a shard and record which venue owns it.
///
/// The shard key ("asset") is a storage location, not a market attribute — it is
/// an underlying symbol on the intl CLOB but a venue name elsewhere. Binding the
/// venue here means every write path gets the venue right without threading it
/// through, including reconciliation paths that only know the shard.
pub async fn init_shard(shard: &str, path: &str, venue: &str) -> Result<()> {
    let url = format!("sqlite://{}?mode=rwc", path);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?;
    init_schema(&pool).await?;
    run_migrations(&pool).await;
    if !venue.is_empty() {
        shard_venues()
            .lock()
            .unwrap()
            .insert(shard.to_lowercase(), venue.to_string());
        let file = pool.connect_options().get_filename().to_string_lossy().to_string();
        path_venues().lock().unwrap().insert(file, venue.to_string());
        backfill_venue(&pool, venue).await;
    }

    // Register in per-asset map.
    pools_map().lock().unwrap().insert(shard.to_string(), pool.clone());
    let asset = shard;

    // First successful call → claim the primary-pool slot (subsequent calls
    // return Err from OnceLock::set which we intentionally discard).
    let _ = DB_POOL.set(pool);

    info!("📦 SQLite initialized [{}]: {}", asset, path);
    Ok(())
}

/// Backward-compat wrapper: initializes for a single asset, deriving the asset
/// name from the file stem (e.g. `"logs/btc-dradis.db"` → `"btc"`).
/// New code should call `init_for_asset` directly.
pub async fn init(path: &str) -> Result<()> {
    let asset = std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("primary")
        .trim_end_matches("-dradis");
    init_for_asset(asset, path).await
}

/// Returns a reference to the **primary** asset's pool (first initialized),
/// or `None` if no pool has been initialized yet.
///
/// Use `pool_for(asset)` to retrieve a specific asset's pool.
pub fn pool() -> Option<&'static SqlitePool> {
    DB_POOL.get()
}

/// Alias registry: dashboard asset key → the pool key that actually backs it.
/// Populated by [`alias_pool`]; consulted by [`pool_for`] only on a miss.
static POOL_ALIASES: OnceLock<std::sync::Mutex<std::collections::HashMap<String, String>>> =
    OnceLock::new();

fn pool_aliases() -> &'static std::sync::Mutex<std::collections::HashMap<String, String>> {
    POOL_ALIASES.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Point `alias` at an already-initialized pool key.
///
/// A venue's DB scope and its squadron's asset identity are not always the same
/// name: the Kalshi venue owns one pool (`"kalshi"`), but its squadron registers
/// under the crypto underlying (`"btc"`) so the taxonomy classifies it as crypto
/// and the raptor health map lines up. The Control Tower then queries
/// `/api/trades?asset=btc` and `/api/positions?asset=btc`, which resolved to no
/// pool at all — every request logged "Database pool not available" and returned
/// an empty list, so a filled trade could never appear in the UI.
///
/// Aliases are deliberately kept OUT of [`available_assets`] so the asset
/// selector still lists one entry per real database.
pub fn alias_pool(alias: &str, target: &str) {
    let (alias, target) = (alias.to_lowercase(), target.to_lowercase());
    if alias == target {
        return;
    }
    let mut map = match pool_aliases().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    if map.get(&alias).map(|t| t == &target).unwrap_or(false) {
        return; // already pointed there — stay quiet across market rotations
    }
    info!("🔗 DB pool alias: '{alias}' → '{target}'");
    map.insert(alias, target);
}

/// Returns a clone of the pool for `asset`, or `None` if that asset has neither
/// its own pool nor an alias to one.  `SqlitePool` is cheaply cloneable
/// (Arc-backed).
pub fn pool_for(asset: &str) -> Option<SqlitePool> {
    let asset = asset.to_lowercase();
    let direct = pools_map().lock().ok()?.get(&asset).cloned();
    if direct.is_some() {
        return direct;
    }
    let target = pool_aliases().lock().ok()?.get(&asset).cloned()?;
    pools_map().lock().ok()?.get(&target).cloned()
}

/// Resolve a pool by optional asset name.
///
/// * `Some(asset)` → look up the asset-specific pool.
/// * `None` / empty string → return the primary pool (same as `pool()`).
///
/// Used by API handlers that accept an `?asset=` query parameter.
pub fn pool_for_opt(asset: Option<&str>) -> Option<SqlitePool> {
    match asset.filter(|s| !s.is_empty()) {
        Some(a) => pool_for(a),
        None    => DB_POOL.get().cloned(),
    }
}

/// Like [`pool_for_opt`], but retries briefly when the pool is missing so API
/// handlers that fire during process startup don't error while pool init is
/// still in flight (roadmap bug #7: the API server can bind before all asset
/// pools are initialized — e.g. the `us` pool inits after venue connect).
/// Steady-state cost is zero: the first attempt succeeds once pools exist.
pub async fn pool_for_opt_retry(asset: Option<&str>) -> Option<SqlitePool> {
    const ATTEMPTS: u32 = 6;
    for attempt in 0..ATTEMPTS {
        if let Some(p) = pool_for_opt(asset) {
            return Some(p);
        }
        if attempt + 1 < ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }
    None
}

/// Pools to read for an optional asset filter: one when scoped, ALL when not.
///
/// `pool_for_opt(None)` returns the single primary pool, which is right on a
/// venue that shards by underlying (intl: btc/eth/sol, primary btc). It is wrong
/// on a venue that shards by WING. Polymarket US opens `us`, `us-crypto`,
/// `us-politics` and `us-sports`; every squadron writes to a wing shard and
/// nothing writes to `us` — so the unscoped read, which is what the Control
/// Tower main view issues, resolved to an empty database.
///
/// On 2026-08-27 that hid 26 trades and $55 of realised P&L: the portfolio chart
/// aggregates across shards so the cash climbed, while the trade log and stats
/// read `us` and reported nothing. An operator saw money appear with no trades
/// to explain it.
pub fn pools_for_opt(asset: Option<&str>) -> Vec<SqlitePool> {
    match asset {
        Some(a) => pool_for(a).into_iter().collect(),
        None => available_assets().iter().filter_map(|a| pool_for(a)).collect(),
    }
}

/// Return the lowercase asset names for all initialized pools, sorted
/// alphabetically.  Used by `GET /api/assets` to tell the Control Tower
/// which asset views are available.
pub fn available_assets() -> Vec<String> {
    let guard = pools_map().lock().unwrap();
    let mut v: Vec<String> = guard.keys().cloned().collect();
    v.sort();
    v
}

// ─── Schema ─────────────────────────────────────────────────────────────────

async fn init_schema(pool: &SqlitePool) -> Result<()> {
    // trades: completed round-trips logged by record_trade()
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS trades (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT    NOT NULL,
            strategy    TEXT    NOT NULL,
            market      TEXT    NOT NULL,
            side        TEXT    NOT NULL,
            entry_price TEXT    NOT NULL,
            exit_price  TEXT    NOT NULL,
            shares      TEXT    NOT NULL,
            pnl         TEXT    NOT NULL,
            reason      TEXT    NOT NULL
        )"
    ).execute(pool).await?;

    // entries: fill events logged by record_entry()
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS entries (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT    NOT NULL,
            strategy    TEXT    NOT NULL,
            token_id    TEXT    NOT NULL,
            market      TEXT    NOT NULL,
            side        TEXT    NOT NULL,
            entry_price TEXT    NOT NULL,
            shares      TEXT    NOT NULL,
            session_id  TEXT    NOT NULL DEFAULT ''
        )"
    ).execute(pool).await?;

    // entry_signals: the signal feature-vector captured at the moment of each entry.
    // Persisted so win/loss outcomes (trades table) can be correlated with the entry
    // conditions that produced them — the data foundation for tuning entry criteria.
    // Join to `trades`/`entries` on (session_id, token_id) ordered by ts.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS entry_signals (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            ts                  TEXT    NOT NULL,
            session_id          TEXT    NOT NULL DEFAULT '',
            strategy            TEXT    NOT NULL,
            token_id            TEXT    NOT NULL,
            market              TEXT    NOT NULL,
            side                TEXT    NOT NULL,
            entry_price         TEXT    NOT NULL,
            shares              TEXT    NOT NULL,
            oracle_price        TEXT    NOT NULL,
            drift_10m           TEXT    NOT NULL,
            drift_60m           TEXT    NOT NULL,
            obi_yes             TEXT    NOT NULL,
            ask_sum             TEXT    NOT NULL,
            bid_sum             TEXT    NOT NULL,
            funding_rate        TEXT    NOT NULL,
            institutional_pulse TEXT    NOT NULL,
            cvd_ratio           TEXT    NOT NULL,
            oi_delta_pct        TEXT    NOT NULL,
            velocity            TEXT    NOT NULL,
            secs_to_expiry      INTEGER NOT NULL
        )"
    ).execute(pool).await?;
    let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_entry_signals_session_token ON entry_signals(session_id, token_id)")
        .execute(pool).await;
    let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_entry_signals_strategy_ts ON entry_signals(strategy, ts)")
        .execute(pool).await;

    // gboost_vetoes: shadow-log of every entry-eligible GBoost signal rejected by a
    // quality gate (2026-08-05). The model matured but the accumulated gate stack was
    // vetoing 100% of eligible signals (38/38 in one 20h window) — with zero trades
    // there is no evidence for which gates block winners vs. save losses. Each row
    // captures the would-be entry so its hypothetical outcome can be scored after
    // market settlement, turning gate calibration into a data problem.
    // Score offline by joining market/condition_id to the market's final resolution.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS gboost_vetoes (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            ts             TEXT    NOT NULL,
            session_id     TEXT    NOT NULL DEFAULT '',
            market         TEXT    NOT NULL,
            condition_id   TEXT    NOT NULL,
            side           TEXT    NOT NULL,
            token_id       TEXT    NOT NULL,
            ask_price      TEXT    NOT NULL,
            p_up           REAL    NOT NULL,
            veto_reason    TEXT    NOT NULL,
            oracle_price   TEXT    NOT NULL,
            drift_60m      TEXT    NOT NULL,
            secs_to_expiry INTEGER NOT NULL
        )"
    ).execute(pool).await?;
    let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_gboost_vetoes_ts ON gboost_vetoes(ts)")
        .execute(pool).await;

    // Outcome labels (2026-08-12). The table above was always meant to be scored
    // "offline by joining market/condition_id to the market's final resolution",
    // but nothing ever wrote the resolution back — so 316 accumulated rows could
    // state what the model BELIEVED and never what actually happened. Without a
    // label the EV of a vetoed signal can only be computed from the model's own
    // probability, which is the model marking its own homework and cannot say
    // whether a gate blocked a winner or saved a loss.
    //
    // outcome:      1 = the vetoed SIDE would have won, 0 = it would have lost,
    //               NULL = not yet resolved. Encoded from the strategy's point of
    //               view (not "did YES win") so scoring needs no side arithmetic.
    // settle_price: the winning-token price the label was derived from, kept for
    //               auditability — a mislabeled row should be traceable.
    let _ = sqlx::query("ALTER TABLE gboost_vetoes ADD COLUMN outcome INTEGER")
        .execute(pool).await;
    let _ = sqlx::query("ALTER TABLE gboost_vetoes ADD COLUMN settle_price TEXT")
        .execute(pool).await;
    let _ = sqlx::query("ALTER TABLE gboost_vetoes ADD COLUMN scored_at TEXT")
        .execute(pool).await;
    let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_gboost_vetoes_unscored ON gboost_vetoes(outcome, ts)")
        .execute(pool).await;

    // signals_json: per-viper gate/decision state captured at entry (JSON blob).
    // The generic columns above answer "what did the market look like?"; this column
    // answers "what did the STRATEGY see and decide?" — model probabilities, gate
    // thresholds vs. measured values, mode flags.  Written by each viper via
    // metrics::stash_entry_signals_json just before it returns an Entry signal.
    // NULL for entries recorded before this migration or vipers not yet instrumented.
    let _ = sqlx::query(
        "ALTER TABLE entry_signals ADD COLUMN signals_json TEXT"
    ).execute(pool).await;

    // pnl_snapshots: periodic P&L checkpoints for the Control Tower chart
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pnl_snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT    NOT NULL,
            session_pnl TEXT    NOT NULL,
            collateral  TEXT    NOT NULL,
            total_value TEXT
        )"
    ).execute(pool).await?;

    // config: key-value store (used by DynamicConfig for JSON blob persistence)
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS config (
            key         TEXT    PRIMARY KEY,
            value       TEXT    NOT NULL,
            updated_at  TEXT    NOT NULL
        )"
    ).execute(pool).await?;

    // open_positions: one row per active (not yet closed) position, across all strategies/modes.
    // Inserted on entry, deleted on exit.  Allows the UI and LLM Advisor to see in-flight
    // positions that have not yet settled as a completed trade.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS open_positions (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            ts             TEXT    NOT NULL,
            session_id     TEXT    NOT NULL,
            strategy       TEXT    NOT NULL,
            token_id       TEXT    NOT NULL,
            market         TEXT    NOT NULL,
            side           TEXT    NOT NULL,
            entry_price    TEXT    NOT NULL,
            shares         TEXT    NOT NULL,
            ghost_mode     INTEGER NOT NULL DEFAULT 0,
            chain_adopted  INTEGER NOT NULL DEFAULT 0
        )"
    ).execute(pool).await?;

    // Migrations: add columns to existing open_positions tables that pre-date them.
    // ALTER TABLE ADD COLUMN is a no-op-safe operation in SQLite; IF NOT EXISTS is not supported
    // so we suppress the "duplicate column" error silently.
    let _ = sqlx::query(
        "ALTER TABLE open_positions ADD COLUMN chain_adopted INTEGER NOT NULL DEFAULT 0"
    ).execute(pool).await;

    // strategy: records which strategy owns the position (ArbitrageStrategy, GboostStrategy, etc.).
    // Critical for correct restart reconciliation — without this column, lookup_open_position_strategy
    // fails silently, causing the entries-table fallback to return the wrong strategy (cross-strategy
    // interference bug where the arb NO leg gets mis-adopted under GboostStrategy on restart).
    let _ = sqlx::query(
        "ALTER TABLE open_positions ADD COLUMN strategy TEXT NOT NULL DEFAULT ''"
    ).execute(pool).await;

    // status: tracks order lifecycle — 'pending' (Viper Launch) vs 'confirmed' (Mission In-Flight).
    // Prevents showing phantom positions in UI before blockchain confirmation.
    let _ = sqlx::query(
        "ALTER TABLE open_positions ADD COLUMN status TEXT NOT NULL DEFAULT 'confirmed'"
    ).execute(pool).await;

    // session_id: ties each row to the session that created it.
    // Needed by adopt_chain_position (INSERT binds session_id) and by session-scoped queries.
    let _ = sqlx::query(
        "ALTER TABLE open_positions ADD COLUMN session_id TEXT NOT NULL DEFAULT ''"
    ).execute(pool).await;

    // current_price: live mark-to-market price from Polymarket Data API, updated on every
    // chain-sync cycle.  NULL until first chain sync.  Used by calculate_positions_value()
    // and /api/portfolio to price positions at current market value instead of entry price.
    // squadron_id: which squadron owns the position. Added so two squadrons of
    // the same class can trade the same market without addressing each other's
    // rows — the in-memory PositionKey carries it for the same reason.
    //
    // Existing rows default to '' rather than being guessed at. A blank squadron
    // reads as "written before squadrons were distinguished", which the dedupe
    // below treats as matching any squadron, so an upgrade cannot resurrect a
    // position that is already open.
    let _ = sqlx::query(
        "ALTER TABLE open_positions ADD COLUMN squadron_id TEXT NOT NULL DEFAULT ''"
    ).execute(pool).await;

    let _ = sqlx::query(
        "ALTER TABLE open_positions ADD COLUMN current_price TEXT"
    ).execute(pool).await;

    // Share count as it stood before a chain read of ZERO overwrote it.
    //
    // A settled position vanishes from the chain, so the drift corrector writes
    // `shares = 0` — and that erases the one number a settlement booking needs.
    // On 2026-08-31 a real winning FairValue position (4.050628 shares at $0.79,
    // redeemed at $1.00 for +$0.80) was deleted with no ledger row because
    // `purge_stale_open_positions` reached its booking branch after the zero write
    // and failed its own `qty > 0` guard. Preserved here so the booking that runs
    // moments later still knows what settled.
    let _ = sqlx::query(
        "ALTER TABLE open_positions ADD COLUMN settled_shares TEXT"
    ).execute(pool).await;

    // llm_recommendations: LLM Advisor analysis results persisted for the dashboard
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS llm_recommendations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT    NOT NULL,
            model       TEXT    NOT NULL,
            trade_count INTEGER NOT NULL,
            session_pnl TEXT    NOT NULL,
            analysis    TEXT    NOT NULL
        )"
    ).execute(pool).await?;

    // llm_actions: LLM-authored config patch proposals — one row per proposed
    // field change, grouped by batch_id (one advisory cycle). Drives the
    // approval flow, autonomy policy engine, AI Actions view, and the few-shot
    // retraining corpus. Status lifecycle:
    //   proposed → approved → applied → (reverted)
    //   proposed → rejected | expired      applied → failed (apply error)
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS llm_actions (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id      TEXT    NOT NULL,
            session_id    TEXT    NOT NULL,
            ts            TEXT    NOT NULL,
            expires_at    TEXT    NOT NULL,
            model         TEXT    NOT NULL,
            tier          INTEGER NOT NULL,
            ghost_mode    INTEGER NOT NULL,
            field         TEXT    NOT NULL,
            from_value    TEXT    NOT NULL,
            to_value      TEXT    NOT NULL,
            clamped       INTEGER NOT NULL DEFAULT 0,
            delta_pct     REAL,
            reason        TEXT    NOT NULL DEFAULT '',
            status        TEXT    NOT NULL DEFAULT 'proposed',
            status_detail TEXT,
            status_ts     TEXT,
            inverse_patch TEXT,
            pnl_at_apply  REAL,
            outcome_score REAL,
            squadron_id   TEXT,
            outcome_detail TEXT
        )"
    ).execute(pool).await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_llm_actions_status ON llm_actions (status, expires_at)"
    ).execute(pool).await?;

    // sessions: one row per process start — the anchor for scoping all queries.
    // session_id = RFC-3339 startup timestamp (stable, readable, sortable).
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS sessions (
            session_id   TEXT    PRIMARY KEY,
            started_at   TEXT    NOT NULL,
            ended_at     TEXT,
            note         TEXT
        )"
    ).execute(pool).await?;

    // config_history: append-only audit log of every config mutation.
    // Lets developers reconstruct what parameters were active during any trade,
    // correlate config changes with P&L inflection points, and review LLM-suggested
    // changes vs. operator-applied changes over time.
    //
    // changed_by values:
    //   'startup_static'  — compile-time constants from config.rs  (recompile detectable via diff)
    //   'startup_dynamic' — DynamicConfig (runtime-tunable params) loaded at session start
    //   'operator'        — Control Tower PATCH /api/config
    //   'llm_advisor'     — recommendation applied by operator
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS config_history (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            ts           TEXT    NOT NULL,
            session_id   TEXT    NOT NULL,
            changed_by   TEXT    NOT NULL,
            param_name   TEXT    NOT NULL,   -- e.g. 'static_config_snapshot', 'session_start_snapshot', field name
            old_value    TEXT,               -- JSON of previous value (NULL on startup snapshots)
            new_value    TEXT    NOT NULL    -- JSON of new value
        )"
    ).execute(pool).await?;

    // squadron_configs: per-squadron configuration storage.
    // Each squadron gets a full copy of DynamicConfig on deployment, allowing
    // independent tuning of viper parameters per asset/squadron.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS squadron_configs (
            squadron_id  TEXT    PRIMARY KEY,
            config_json  TEXT    NOT NULL,
            created_at   TEXT    NOT NULL,
            updated_at   TEXT    NOT NULL
        )"
    ).execute(pool).await?;

    // ── Market taxonomy: market_class ↔ raptor_kind / viper_kind ──────────────
    // Data-driven classification linking a market's domain (crypto / sports /
    // politics / …) to the raptors (signal sources) and vipers (strategies)
    // that are *meaningful* for it. Squadrons resolve their eligible
    // raptors/vipers by joining through these tables instead of hardcoding a
    // strategy list per venue, so adding a new domain (or wiring a future
    // sports/politics raptor) is a data change, not a recompile.

    // The domain a market belongs to.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS market_class (
            id       TEXT    PRIMARY KEY,   -- 'crypto', 'sports', 'politics', 'unknown'
            display  TEXT    NOT NULL,
            enabled  INTEGER NOT NULL DEFAULT 1
        )"
    ).execute(pool).await?;

    // Signal sources — one row per raptor in src/raptors/ (plus roadmapped ones).
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS raptor_kind (
            id           TEXT    PRIMARY KEY,   -- 'price', 'funding', 'sports', 'politics'
            display      TEXT    NOT NULL,
            implemented  INTEGER NOT NULL DEFAULT 0   -- 0 = roadmapped, not built yet
        )"
    ).execute(pool).await?;

    // Strategies — one row per Strategy impl in orchestrator/registry.rs.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS viper_kind (
            id              TEXT    PRIMARY KEY,   -- 'arbitrage', 'maker', 'momentum', …
            display         TEXT    NOT NULL,
            venue_agnostic  INTEGER NOT NULL DEFAULT 0   -- 1 = pure order-book (arb/maker)
        )"
    ).execute(pool).await?;

    // M:N — which raptors apply to which market class.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS market_class_raptor (
            market_class TEXT NOT NULL REFERENCES market_class(id),
            raptor_kind  TEXT NOT NULL REFERENCES raptor_kind(id),
            PRIMARY KEY (market_class, raptor_kind)
        )"
    ).execute(pool).await?;

    // M:N — which vipers apply to which market class.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS market_class_viper (
            market_class TEXT NOT NULL REFERENCES market_class(id),
            viper_kind   TEXT NOT NULL REFERENCES viper_kind(id),
            PRIMARY KEY (market_class, viper_kind)
        )"
    ).execute(pool).await?;

    // Classification rules consumed by classify_market(). Adding a new mapping
    // (e.g. 'tennis' → sports) is one INSERT — no code change.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS market_class_rule (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern      TEXT    NOT NULL,
            match_kind   TEXT    NOT NULL,   -- 'category' | 'symbol_token' | 'slug'
            market_class TEXT    NOT NULL REFERENCES market_class(id),
            priority     INTEGER NOT NULL DEFAULT 100,   -- lower = checked first
            UNIQUE (pattern, match_kind)
        )"
    ).execute(pool).await?;

    // Deployment queue for Admiral Adama extension — user-requested squadron deployments.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS deployment_queue (
            id           TEXT    PRIMARY KEY,
            market_id    TEXT    NOT NULL,
            market_type  TEXT    NOT NULL,   -- 'crypto' | 'sports' | 'politics'
            raptors      TEXT    NOT NULL,   -- JSON array of raptor kind IDs
            vipers       TEXT    NOT NULL,   -- JSON array of viper kind IDs
            viper_budgets TEXT,              -- JSON object: viper kind → max-exposure USDC
            status       TEXT    NOT NULL DEFAULT 'pending',  -- pending | processing | deployed | failed
            squadron_id  TEXT,               -- populated once deployed
            error        TEXT,               -- populated on failure
            created_at   TEXT    NOT NULL DEFAULT (datetime('now')),
            updated_at   TEXT    NOT NULL DEFAULT (datetime('now'))
        )"
    ).execute(pool).await?;

    // Operator-chosen squadron name, so a second squadron of a class can be told
    // apart from the first. Blank for every deployment made before naming
    // existed, which keeps their squadron ids exactly as they were.
    //
    // This must sit AFTER the CREATE above. It used to live ~170 lines earlier,
    // among the `open_positions` migrations, so on a brand-new database it ran
    // against a table that did not exist yet, failed, and was swallowed by
    // `let _ =`; the CREATE then built the table without the column. Every
    // auto-deploy failed with "table deployment_queue has no column named name",
    // retried every few seconds forever.
    //
    // It self-healed on the SECOND boot — by then the table existed, so the
    // ALTER landed — which is why it hid for so long: a normal install restarts
    // once during Setup and is on boot two before the seeder first runs. A demo
    // or CI box that starts fresh and never restarts stays broken.
    let _ = sqlx::query(
        "ALTER TABLE deployment_queue ADD COLUMN name TEXT NOT NULL DEFAULT ''"
    ).execute(pool).await;

    // Migration for queues created before per-viper deploy budgets existed
    // (no-op-safe: duplicate-column error is silently suppressed).
    let _ = sqlx::query(
        "ALTER TABLE deployment_queue ADD COLUMN viper_budgets TEXT"
    ).execute(pool).await;

    // Migration for llm_actions tables created before the circuit breaker
    // recorded a P&L baseline at apply time.
    let _ = sqlx::query(
        "ALTER TABLE llm_actions ADD COLUMN pnl_at_apply REAL"
    ).execute(pool).await;
    // Which squadron a proposal targets.
    //
    // The advisor ran one global pass and applied to the global DynamicConfig,
    // which no patrol loop reads — squadrons read a per-squadron handle. Without
    // this column an action cannot say which squadron it moved, so the audit
    // trail, the inverse patch and the circuit breaker's revert would all target
    // the wrong config once more than one squadron is in play.
    //
    // Existing rows keep NULL: they were written against the global record and
    // never reached a strategy, so attributing them to a squadron would be a
    // lie. Readers treat NULL as "global, never applied".
    let _ = sqlx::query(
        "ALTER TABLE llm_actions ADD COLUMN squadron_id TEXT"
    ).execute(pool).await;

    seed_market_taxonomy(pool).await?;

    Ok(())
}

/// Seed the market-class taxonomy with the built-in domains, kinds, links, and
/// classification rules. Idempotent (`INSERT OR IGNORE`) so it self-heals on
/// every startup and never clobbers operator-added rows.
async fn seed_market_taxonomy(pool: &SqlitePool) -> Result<()> {
    // market_class
    for (id, display) in [
        ("crypto",   "Crypto"),
        ("sports",   "Sports"),
        ("politics", "Politics"),
        ("unknown",  "Unknown"),
    ] {
        sqlx::query("INSERT OR IGNORE INTO market_class (id, display) VALUES (?, ?)")
            .bind(id).bind(display).execute(pool).await?;
    }

    // raptor_kind — implemented = 1 for raptors that exist in src/raptors/ today.
    for (id, display, implemented) in [
        ("price",    "Price Raptor (spot + velocity + drift)", 1),
        ("funding",  "Funding Raptor (perp funding rate)",     1),
        ("derivatives", "Derivatives Raptor (open interest + CVD)", 1),
        ("tide",     "Tide Raptor (ETF institutional pulse)",  1),
        ("horizon",  "Horizon Raptor (TradFi velocity / VIX proxy)", 1),
        ("sports",   "Sports Raptor (line movement, observe-only)", 1),
        ("politics", "Politics Raptor (roadmap)",              0),
    ] {
        sqlx::query("INSERT OR IGNORE INTO raptor_kind (id, display, implemented) VALUES (?, ?, ?)")
            .bind(id).bind(display).bind(implemented).execute(pool).await?;
    }
    // Self-heal DBs seeded before the Sports Raptor was implemented (INSERT OR
    // IGNORE above won't flip an existing row's `implemented` flag / display).
    sqlx::query("UPDATE raptor_kind SET implemented = 1, display = ? WHERE id = 'sports'")
        .bind("Sports Raptor (line movement, observe-only)")
        .execute(pool).await?;

    // viper_kind — venue_agnostic = 1 for pure order-book strategies.
    for (id, display, agnostic) in VIPER_KINDS {
        sqlx::query("INSERT OR IGNORE INTO viper_kind (id, display, venue_agnostic) VALUES (?, ?, ?)")
            .bind(id).bind(display).bind(agnostic).execute(pool).await?;
    }

    // market_class → raptor_kind. The Sports Raptor is now implemented and links
    // to the sports class (observe-only). politics raptor is still roadmapped, so
    // that class gets no raptor until one is built.
    for (class, raptor) in [
        ("crypto", "price"),
        ("crypto", "funding"),
        ("crypto", "derivatives"),
        ("crypto", "tide"),
        ("crypto", "horizon"),
        ("sports", "sports"),
    ] {
        sqlx::query("INSERT OR IGNORE INTO market_class_raptor (market_class, raptor_kind) VALUES (?, ?)")
            .bind(class).bind(raptor).execute(pool).await?;
    }

    // market_class → viper_kind. crypto gets the full suite; non-crypto (and the
    // 'unknown' fallback) get only the venue-agnostic order-book strategies.
    for (class, viper) in [
        ("crypto", "arbitrage"), ("crypto", "maker"), ("crypto", "momentum"),
        ("crypto", "gboost"),    ("crypto", "basis"), ("crypto", "time_decay"),
        ("crypto", "trendcapture"), ("crypto", "convergence"), ("crypto", "fairvalue"),
        ("sports",   "arbitrage"), ("sports",   "maker"),
        ("politics", "arbitrage"), ("politics", "maker"),
        ("unknown",  "arbitrage"), ("unknown",  "maker"),
    ] {
        sqlx::query("INSERT OR IGNORE INTO market_class_viper (market_class, viper_kind) VALUES (?, ?)")
            .bind(class).bind(viper).execute(pool).await?;
    }

    // Classification rules (lower priority = checked first):
    //   category (highest confidence) → symbol_token → slug keyword.
    let rules: &[(&str, &str, &str, i64)] = &[
        // pattern, match_kind, market_class, priority
        ("crypto",   "category", "crypto",   10),
        ("sports",   "category", "sports",   10),
        ("politics", "category", "politics", 10),
        // sports leagues embedded in instrument symbols (e.g. aec-nfl-lac-ten-…)
        ("nfl",    "symbol_token", "sports", 20),
        ("nba",    "symbol_token", "sports", 20),
        ("mlb",    "symbol_token", "sports", 20),
        ("nhl",    "symbol_token", "sports", 20),
        ("ncaa",   "symbol_token", "sports", 20),
        ("ufc",    "symbol_token", "sports", 20),
        ("soccer", "symbol_token", "sports", 20),
        ("tennis", "symbol_token", "sports", 20),
        // politics keywords
        ("election",  "symbol_token", "politics", 20),
        ("potus",     "symbol_token", "politics", 20),
        ("senate",    "symbol_token", "politics", 20),
        ("president", "slug",         "politics", 30),
        // crypto tickers
        ("btc", "symbol_token", "crypto", 20),
        ("eth", "symbol_token", "crypto", 20),
        ("sol", "symbol_token", "crypto", 20),
        // crypto names in the market title/slug (Kalshi symbols tokenize as
        // "kxbtcd"/"kxbtc15m" — no bare "btc" token — but titles name the coin)
        ("bitcoin",  "slug", "crypto", 30),
        ("ethereum", "slug", "crypto", 30),
        ("solana",   "slug", "crypto", 30),
        ("xrp",      "slug", "crypto", 30),
        ("dogecoin", "slug", "crypto", 30),
    ];
    for (pattern, kind, class, prio) in rules {
        sqlx::query(
            "INSERT OR IGNORE INTO market_class_rule (pattern, match_kind, market_class, priority)
             VALUES (?, ?, ?, ?)"
        ).bind(pattern).bind(kind).bind(class).bind(prio).execute(pool).await?;
    }

    Ok(())
}

/// Add new columns to existing tables that pre-date the session tracking feature.
/// Uses sqlx error suppression rather than IF NOT EXISTS (SQLite does not support that syntax).
async fn run_migrations(pool: &SqlitePool) {
    // Add session_id to trades
    let _ = sqlx::query("ALTER TABLE trades ADD COLUMN session_id TEXT")
        .execute(pool).await;
    // Add session_id to llm_recommendations
    let _ = sqlx::query("ALTER TABLE llm_recommendations ADD COLUMN session_id TEXT")
        .execute(pool).await;

    // Add session_id to entries so lookup_entry_db can prefer current-session rows,
    // preventing cross-session strategy misattribution on restart reconciliation.
    let _ = sqlx::query("ALTER TABLE entries ADD COLUMN session_id TEXT NOT NULL DEFAULT ''")
        .execute(pool).await;

    // Index for fast session-scoped queries
    let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_trades_session ON trades(session_id)")
        .execute(pool).await;
    let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts)")
        .execute(pool).await;
    let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_llm_session ON llm_recommendations(session_id)")
        .execute(pool).await;
    // Migrate open_positions table for existing DBs
    let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_open_positions_session ON open_positions(session_id)")
        .execute(pool).await;

    // Add total_value to pnl_snapshots (Phase 3f-7: proper portfolio value tracking)
    let _ = sqlx::query("ALTER TABLE pnl_snapshots ADD COLUMN total_value TEXT")
        .execute(pool).await;
    // 1. Composite index for trade execution bubbles (Fixes main chart latency)
    let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_trades_session_ts ON trades(session_id, ts)")
        .execute(pool).await;

    // 2. Composite index for active entry position bubbles
    let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_open_positions_session_ts ON open_positions(session_id, ts)")
        .execute(pool).await;

    // 3. Composite index for the historical P&L time-series snapshots
    let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_pnl_snapshots_session_ts ON pnl_snapshots(session_id, ts)")
        .execute(pool).await;

    // Market taxonomy: persist the resolved market class alongside each
    // squadron's config so the UI/resolver can read it without re-classifying.
    let _ = sqlx::query("ALTER TABLE squadron_configs ADD COLUMN market_class TEXT NOT NULL DEFAULT 'unknown'")
        .execute(pool).await;

    // Trade filing dimensions. Until now the only discriminator on a trade row
    // was which database file it lived in — the shard key the UI mislabeled as
    // "Asset". That cannot express "a sports market has no underlying", and it
    // could not tell a Kalshi BTC trade from a Kalshi ETH trade at all, since
    // both squadrons share the `kalshi` shard. See `state::TradeScope`.
    //
    // All three are nullable on purpose: `underlying` is genuinely absent for
    // sports/politics, and pre-existing rows have no way to recover a class.
    // `fees`: total dollars paid to the venue for the round trip (entry + exit).
    // `pnl` is stored NET of this; subtracting it back recovers the gross figure.
    let _ = sqlx::query("ALTER TABLE trades ADD COLUMN fees TEXT")
        .execute(pool).await;
    // `ghost`: was this trade simulated? `open_positions` has always carried a
    // `ghost_mode` column, so the Control Tower badged OPEN positions correctly
    // while completed trades had nowhere to record it and the UI hardcoded
    // "real" for every row. Defaults to 0 for pre-existing rows, which is a
    // guess — but every row written before this column existed came from a
    // build where the trade log claimed "real" anyway, so the default preserves
    // exactly what those rows already displayed rather than inventing a new
    // claim about them.
    let _ = sqlx::query("ALTER TABLE trades ADD COLUMN ghost INTEGER NOT NULL DEFAULT 0")
        .execute(pool).await;
    // `price_updated_at`: when `current_price` was last refreshed.
    //
    // That price comes from the 300s chain-sync sweep reading the Polymarket
    // Data API, which is itself indexer-backed and lags. On 2026-08-30 an
    // operator watching the Trade Log to time a manual exit saw $0.82 on a
    // position the live book had at $0.98 — a 16-cent gap on a binary minutes
    // from resolution, which is exactly the moment the number matters most.
    //
    // Recording freshness does not make the price fresher. It stops the display
    // presenting a four-minute-old number as if it were live, which is the part
    // that can cost an operator money.
    let _ = sqlx::query("ALTER TABLE open_positions ADD COLUMN price_updated_at TEXT")
        .execute(pool).await;
    // `entry_fee`: dollars already paid to the venue to OPEN this position.
    // Needed because a position can leave via settlement or off-strategy close
    // rather than the strategy's exit path, and those bookings live here — with
    // no access to the in-memory Position that carries the fee. Without it they
    // booked gross P&L as if entry were free (2026-08-13 trade 356: +$0.7585
    // recorded against +$0.7201 of actual collateral movement).
    let _ = sqlx::query("ALTER TABLE open_positions ADD COLUMN entry_fee TEXT")
        .execute(pool).await;
    // `open_positions` gets the same three columns as `trades` / `entries`. It
    // was missed when they landed there, so the Control Tower's tradelog showed
    // a completed row filed under its venue directly above an in-flight row that
    // could only fall back to the shard — Venue rendered "—" and Subject only
    // resolved when the shard happened to be an underlying symbol (intl). Same
    // NULL semantics: NULL means "recorded before we tracked this", never a
    // guess.
    for table in ["trades", "entries", "open_positions"] {
        for col in ["venue", "market_class", "underlying"] {
            let _ = sqlx::query(&format!("ALTER TABLE {table} ADD COLUMN {col} TEXT"))
                .execute(pool).await;
        }
    }
    let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_trades_venue_ts ON trades(venue, ts)")
        .execute(pool).await;
    let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_trades_class_ts ON trades(market_class, ts)")
        .execute(pool).await;
}

/// Stamp the owning venue onto rows written before the `venue` column existed.
///
/// Safe and idempotent: one shard maps to exactly one venue, so every legacy row
/// in this file belongs to `venue` by construction. Only `venue` is backfilled —
/// `market_class` and `underlying` are left NULL rather than guessed, so a NULL
/// reads honestly as "recorded before we tracked this".
async fn backfill_venue(pool: &SqlitePool, venue: &str) {
    for table in ["trades", "entries", "open_positions"] {
        match sqlx::query(&format!("UPDATE {table} SET venue = ? WHERE venue IS NULL"))
            .bind(venue)
            .execute(pool)
            .await
        {
            Ok(r) if r.rows_affected() > 0 => {
                info!("🏷️  Backfilled venue='{}' on {} legacy {} row(s)", venue, r.rows_affected(), table);
            }
            Ok(_) => {}
            Err(e) => warn!("⚠️ venue backfill failed on {table}: {e}"),
        }
    }
}

// ─── Session lifecycle ───────────────────────────────────────────────────────

/// Create a new session row in **all** initialized asset pools and set the
/// process-lifetime session ID.
///
/// Call once immediately after all `init_for_asset()` calls complete so every
/// asset DB gets a session row for the same RFC-3339 startup timestamp.
///
/// Returns the new session_id string.
pub async fn init_session(note: Option<&str>) -> String {
    let session_id = Utc::now().to_rfc3339();
    let _ = CURRENT_SESSION_ID.set(session_id.clone());

    // Collect all initialized pools so we can write a session row to each.
    let all_pools: Vec<SqlitePool> = {
        let guard = pools_map().lock().unwrap();
        guard.values().cloned().collect()
    };

    for pool in &all_pools {
        let ts = session_id.clone();
        if let Err(e) = sqlx::query(
            "INSERT INTO sessions (session_id, started_at, note) VALUES (?, ?, ?)"
        )
        .bind(&session_id)
        .bind(&ts)
        .bind(note.unwrap_or(""))
        .execute(pool)
        .await {
            error!("❌ DB session init failed: {}", e);
        }

        // Also persist to config KV for easy lookup by UI components
        config_set(pool, "current_session_id", &session_id).await;
    }

    if !all_pools.is_empty() {
        info!("📅 Session started: {} ({} asset DB(s))", session_id, all_pools.len());
    }

    session_id
}

/// Mark the current session as ended.  Called on graceful shutdown.
pub async fn close_session() {
    if let (Some(pool), sid) = (pool(), current_session_id()) {
        let ts = Utc::now().to_rfc3339();
        let _ = sqlx::query(
            "UPDATE sessions SET ended_at = ? WHERE session_id = ?"
        )
        .bind(&ts)
        .bind(sid)
        .execute(pool)
        .await;
    }
}

// ─── Trade / Entry writes ────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub async fn record_trade_db(
    pool: &SqlitePool,
    scope: &TradeScope,
    fees: Decimal,
    strategy: &str,
    market: &str,
    side: &str,
    entry_price: Decimal,
    exit_price: Decimal,
    shares: Decimal,
    pnl: Decimal,
    reason: &str,
    timestamp: Option<DateTime<Utc>>,
) {
    let ts = timestamp.unwrap_or_else(|| Utc::now()).to_rfc3339();
    let sid = current_session_id();
    let venue = resolved_venue(scope);
    if let Err(e) = sqlx::query(
        "INSERT INTO trades (ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason, session_id,
                             venue, market_class, underlying, fees, ghost)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(&ts)
    .bind(strategy)
    .bind(market)
    .bind(side)
    .bind(entry_price.to_string())
    .bind(exit_price.to_string())
    .bind(shares.to_string())
    .bind(pnl.to_string())
    .bind(reason)
    .bind(sid)
    .bind(venue)
    .bind(scope.market_class.clone())
    .bind(scope.underlying.clone())
    .bind(fees.to_string())
    .bind(scope.ghost as i32)
    .execute(pool)
    .await {
        error!("❌ DB trade write failed: {}", e);
    }
}

/// Idempotently record a *settlement* trade — INSERTs only if no row with the same
/// settlement fingerprint already exists.
///
/// Why this exists: a market resolves (and a given token settles) exactly once, but
/// `auto_settle_closed_positions` can re-submit a redeem for an already-settled
/// condition after a process restart — the in-memory `PERMANENTLY_SETTLED_CONDITIONS`
/// guard is empty on a fresh start, so the same redeemable condition is re-redeemed
/// (a harmless on-chain no-op) and, with the old plain INSERT, re-recorded as a fresh
/// settlement row every session.  That double-counted realized losses (observed:
/// the same SOL single-leg orphan booked 5× across 5 sessions → −$50 shown for a
/// ~−$10 real loss).
///
/// The fingerprint (strategy, market, side, reason, shares, pnl) is stable across
/// restarts for the same settlement, so the `WHERE NOT EXISTS` makes recording
/// idempotent.  Returns true if a NEW row was inserted.
#[allow(clippy::too_many_arguments)]
pub async fn record_settlement_trade_idempotent(
    pool: &SqlitePool,
    strategy: &str,
    market: &str,
    side: &str,
    entry_price: Decimal,
    exit_price: Decimal,
    shares: Decimal,
    pnl: Decimal,
    fees: Decimal,
    reason: &str,
    timestamp: Option<DateTime<Utc>>,
) -> bool {
    let ts = timestamp.unwrap_or_else(Utc::now).to_rfc3339();
    let sid = current_session_id();
    match sqlx::query(
        "INSERT INTO trades (ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason, session_id, fees)
         SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
         WHERE NOT EXISTS (
             SELECT 1 FROM trades
             WHERE strategy = ? AND market = ? AND side = ? AND reason = ?
               AND shares = ? AND pnl = ?
         )"
    )
    .bind(&ts)
    .bind(strategy)
    .bind(market)
    .bind(side)
    .bind(entry_price.to_string())
    .bind(exit_price.to_string())
    .bind(shares.to_string())
    .bind(pnl.to_string())
    .bind(reason)
    .bind(sid)
    .bind(fees.to_string())
    // WHERE NOT EXISTS fingerprint binds:
    .bind(strategy)
    .bind(market)
    .bind(side)
    .bind(reason)
    .bind(shares.to_string())
    .bind(pnl.to_string())
    .execute(pool)
    .await {
        Ok(r)  => r.rows_affected() > 0,
        Err(e) => { error!("❌ DB settlement idempotent write failed: {}", e); false }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn record_entry_db(
    pool: &SqlitePool,
    scope: &TradeScope,
    strategy: &str,
    token_id: &str,
    market: &str,
    side: &str,
    entry_price: Decimal,
    shares: Decimal,
) {
    let ts = Utc::now().to_rfc3339();
    let sid = current_session_id();
    let venue = resolved_venue(scope);
    if let Err(e) = sqlx::query(
        "INSERT INTO entries (ts, strategy, token_id, market, side, entry_price, shares, session_id,
                              venue, market_class, underlying)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(&ts)
    .bind(strategy)
    .bind(token_id)
    .bind(market)
    .bind(side)
    .bind(entry_price.to_string())
    .bind(shares.to_string())
    .bind(sid)
    .bind(venue)
    .bind(scope.market_class.clone())
    .bind(scope.underlying.clone())
    .execute(pool)
    .await {
        error!("❌ DB entry write failed: {}", e);
    }
}

/// Signal feature-vector captured at entry time, persisted to `entry_signals`.
/// All market features are snapshot-derived; identity fields tie the row back to the
/// resulting position/trade for win-loss correlation.
#[derive(Clone, Debug)]
pub struct EntrySignalRow {
    pub strategy:            String,
    pub token_id:            String,
    pub market:              String,
    pub side:                String,
    pub entry_price:         Decimal,
    pub shares:              Decimal,
    pub oracle_price:        Decimal,
    pub drift_10m:           Decimal,
    pub drift_60m:           Decimal,
    pub obi_yes:             Decimal,
    pub ask_sum:             Decimal,
    pub bid_sum:             Decimal,
    pub funding_rate:        Decimal,
    pub institutional_pulse: Decimal,
    pub cvd_ratio:           Decimal,
    pub oi_delta_pct:        Decimal,
    pub velocity:            Decimal,
    pub secs_to_expiry:      i64,
    /// Per-viper gate/decision state as a JSON blob (None = viper not instrumented).
    pub signals_json:        Option<String>,
}

/// Persist an entry-signal feature-vector row.
pub async fn record_entry_signal_db(pool: &SqlitePool, row: &EntrySignalRow) {
    let ts = Utc::now().to_rfc3339();
    let sid = current_session_id();
    if let Err(e) = sqlx::query(
        "INSERT INTO entry_signals
            (ts, session_id, strategy, token_id, market, side, entry_price, shares,
             oracle_price, drift_10m, drift_60m, obi_yes, ask_sum, bid_sum,
             funding_rate, institutional_pulse, cvd_ratio, oi_delta_pct, velocity, secs_to_expiry,
             signals_json)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(&ts)
    .bind(sid)
    .bind(&row.strategy)
    .bind(&row.token_id)
    .bind(&row.market)
    .bind(&row.side)
    .bind(row.entry_price.to_string())
    .bind(row.shares.to_string())
    .bind(row.oracle_price.to_string())
    .bind(row.drift_10m.to_string())
    .bind(row.drift_60m.to_string())
    .bind(row.obi_yes.to_string())
    .bind(row.ask_sum.to_string())
    .bind(row.bid_sum.to_string())
    .bind(row.funding_rate.to_string())
    .bind(row.institutional_pulse.to_string())
    .bind(row.cvd_ratio.to_string())
    .bind(row.oi_delta_pct.to_string())
    .bind(row.velocity.to_string())
    .bind(row.secs_to_expiry)
    .bind(row.signals_json.as_deref())
    .execute(pool)
    .await {
        error!("❌ DB entry_signal write failed: {}", e);
    }
}

/// Shadow-log a vetoed (entry-eligible but gate-rejected) GBoost signal.
/// Fire-and-forget from the veto path — see gboost_vetoes table comment.
#[allow(clippy::too_many_arguments)]
pub async fn record_gboost_veto_db(
    pool: &SqlitePool,
    market: &str,
    condition_id: &str,
    side: &str,
    token_id: &str,
    ask_price: &str,
    p_up: f64,
    veto_reason: &str,
    oracle_price: &str,
    drift_60m: &str,
    secs_to_expiry: i64,
) {
    let ts = Utc::now().to_rfc3339();
    let sid = current_session_id();
    if let Err(e) = sqlx::query(
        "INSERT INTO gboost_vetoes
            (ts, session_id, market, condition_id, side, token_id, ask_price,
             p_up, veto_reason, oracle_price, drift_60m, secs_to_expiry)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(&ts)
    .bind(sid)
    .bind(market)
    .bind(condition_id)
    .bind(side)
    .bind(token_id)
    .bind(ask_price)
    .bind(p_up)
    .bind(veto_reason)
    .bind(oracle_price)
    .bind(drift_60m)
    .bind(secs_to_expiry)
    .execute(pool)
    .await {
        error!("❌ DB gboost_veto write failed: {}", e);
    }
}

/// A resolved binary token settles at $1.00 (won) or $0.00 (lost). Prices at or
/// beyond these bounds are treated as final; anything between them means the
/// market has not resolved yet (or the read was unreliable) and the row is left
/// unlabeled for a later sweep. Deliberately strict — a wrong label is far worse
/// than a late one, because it silently corrupts the gate-calibration evidence
/// this table exists to provide.
const VETO_SETTLE_WON_MIN:  Decimal = rust_decimal_macros::dec!(0.95);
const VETO_SETTLE_LOST_MAX: Decimal = rust_decimal_macros::dec!(0.05);

/// One row of the GBoost veto scoreboard: how a single gate performed on the
/// signals it actually blocked.
#[derive(Debug, Clone, Serialize)]
pub struct GboostVetoScore {
    /// Gate family, normalized from the free-text veto reason.
    pub gate: String,
    /// Vetoed signals attributable to this gate.
    pub total: i64,
    /// Of those, how many have a settled outcome yet.
    pub scored: i64,
    /// Scored signals whose side went on to win — i.e. entries this gate blocked
    /// that would have paid $1.00.
    pub would_have_won: i64,
    /// Realised P&L per share had every scored signal been taken:
    /// `mean(outcome − ask_price)`. Positive means the gate is, on this evidence,
    /// costing money; negative means it is protecting the wallet. This is the
    /// number the model's own probability could never supply.
    pub avg_pnl_per_share: f64,
    /// Distinct markets the scored signals came from — the REAL sample size.
    ///
    /// Signals inside one market are near-perfectly correlated: the model holds a
    /// view for the whole session, so 40 signals on one daily market that closes
    /// up are one observation, not 40. Reading `scored` as the sample size makes a
    /// handful of trending days look like overwhelming evidence (first prod score,
    /// 2026-08-12: 316 signals looked like a 73% win rate, but 3 of 12 markets
    /// carried nearly all of it). Always judge significance on this column.
    pub distinct_markets: i64,
    /// Mean per-share edge computed per MARKET and then averaged, so one busy
    /// market cannot outvote the rest. The conservative counterpart to
    /// `avg_pnl_per_share`; when the two diverge sharply, the raw figure is being
    /// driven by signal concentration rather than by a repeatable edge.
    pub avg_pnl_per_market: f64,
}

/// SQL CASE mapping a free-text `veto_reason` onto a stable gate family.
///
/// Order matters: the `hourly …` reasons are the cross-market confirmation gate
/// and must be matched BEFORE the generic OBI patterns, or they collapse into the
/// primary book gate and the scoreboard silently attributes their blocks to the
/// wrong knob. Defined once and interpolated into both queries below so the two
/// can never drift apart.
const VETO_GATE_CASE: &str = "CASE
    WHEN veto_reason LIKE 'hourly%'              THEN 'hourly OBI cross-check'
    WHEN veto_reason LIKE '%price out of range%' THEN 'price band'
    WHEN veto_reason LIKE '%adverse OBI%'        THEN 'adverse OBI'
    WHEN veto_reason LIKE '%OBI adverse%'        THEN 'adverse OBI'
    WHEN veto_reason LIKE '%exhaust%'            THEN 'OBI exhaustion'
    WHEN veto_reason LIKE '%counter-trend%'      THEN 'counter-trend'
    WHEN veto_reason LIKE '%oracle too flat%'    THEN 'low volatility'
    WHEN veto_reason LIKE '%too close to 0.5%'   THEN 'near coin-flip'
    ELSE 'other' END";

/// Score each GBoost gate against the settled outcomes of the signals it blocked.
///
/// Only rows with a resolved `outcome` participate; unscored rows are reported in
/// `total − scored` so a thin sample is never mistaken for a confident verdict.
pub async fn gboost_veto_scoreboard(pool: &SqlitePool) -> Vec<GboostVetoScore> {
    let group_sql = format!(
        "SELECT {case} AS gate,
                COUNT(*) AS total,
                SUM(CASE WHEN outcome IS NOT NULL THEN 1 ELSE 0 END) AS scored,
                SUM(CASE WHEN outcome IS NOT NULL THEN outcome ELSE 0 END) AS wins
           FROM gboost_vetoes
          GROUP BY gate",
        case = VETO_GATE_CASE
    );
    let rows: Vec<(String, i64, i64, Option<i64>)> = match sqlx::query_as(&group_sql)
        .fetch_all(pool).await {
        Ok(r) => r,
        Err(e) => { error!("❌ DB gboost veto scoreboard failed: {}", e); return vec![]; }
    };

    // Average realised edge is computed separately so the ask price only enters
    // for rows that actually have a label.
    let avg_sql = format!(
        "SELECT AVG(CAST(outcome AS REAL) - CAST(ask_price AS REAL))
           FROM gboost_vetoes
          WHERE outcome IS NOT NULL AND {case} = ?",
        case = VETO_GATE_CASE
    );
    // Per-market aggregation first, then a mean over markets — one busy market
    // must not outvote the rest. See `distinct_markets`.
    let per_market_sql = format!(
        "SELECT COUNT(*), AVG(m_avg) FROM (
             SELECT AVG(CAST(outcome AS REAL) - CAST(ask_price AS REAL)) AS m_avg
               FROM gboost_vetoes
              WHERE outcome IS NOT NULL AND {case} = ?
              GROUP BY market
         )",
        case = VETO_GATE_CASE
    );

    let mut out = Vec::with_capacity(rows.len());
    for (gate, total, scored, wins) in rows {
        let avg: Option<f64> = sqlx::query_as::<_, (Option<f64>,)>(&avg_sql)
        .bind(&gate)
        .fetch_optional(pool).await.ok().flatten().and_then(|(v,)| v);

        let (markets, per_market): (i64, Option<f64>) =
            sqlx::query_as::<_, (i64, Option<f64>)>(&per_market_sql)
            .bind(&gate)
            .fetch_optional(pool).await.ok().flatten().unwrap_or((0, None));

        out.push(GboostVetoScore {
            gate,
            total,
            scored,
            would_have_won: wins.unwrap_or(0),
            avg_pnl_per_share: avg.unwrap_or(0.0),
            distinct_markets: markets,
            avg_pnl_per_market: per_market.unwrap_or(0.0),
        });
    }
    out.sort_by(|a, b| b.total.cmp(&a.total));
    out
}

/// The `condition_id` recorded alongside a vetoed token — the key the Gamma
/// resolution lookup needs. Kept next to the scorer so the caller's price-resolver
/// closure stays a one-liner instead of threading condition ids through the query.
pub async fn condition_id_for_veto_token(pool: &SqlitePool, token_id: &str) -> Option<String> {
    sqlx::query_as::<_, (String,)>(
        "SELECT condition_id FROM gboost_vetoes
          WHERE token_id = ? AND condition_id <> ''
          ORDER BY id DESC LIMIT 1"
    )
    .bind(token_id)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
    .map(|(c,)| c)
}

/// Attach resolution outcomes to `gboost_vetoes` rows whose market has settled.
///
/// `resolve_price(token_id) -> Option<Decimal>` fetches the token's CURRENT price;
/// the caller supplies it so this module keeps no dependency on the venue SDK.
/// Returns the number of rows newly labeled.
///
/// Only rows whose market should already have closed are attempted — close time
/// is reconstructed as `ts + secs_to_expiry`, which is exactly what was recorded
/// at veto time. A grace period is added on top so a market that resolves a
/// little late is not read mid-settlement.
///
/// The label is written from the VETOED SIDE's point of view: `outcome = 1` means
/// buying that side at the recorded ask would have paid $1.00, so scoring a gate
/// is a plain average over `outcome` with no further arithmetic.
pub async fn score_pending_gboost_vetoes<F, Fut>(
    pool: &SqlitePool,
    max_rows: i64,
    resolve_price: F,
) -> usize
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Option<Decimal>>,
{
    /// Extra wait beyond the recorded close time before a price read is trusted.
    const SETTLE_GRACE_SECS: i64 = 900;

    let rows: Vec<(i64, String, String, String, i64)> = match sqlx::query_as(
        "SELECT id, ts, token_id, side, secs_to_expiry
           FROM gboost_vetoes
          WHERE outcome IS NULL
          ORDER BY ts ASC
          LIMIT ?"
    )
    .bind(max_rows)
    .fetch_all(pool)
    .await {
        Ok(r) => r,
        Err(e) => { error!("❌ DB gboost_veto scoring fetch failed: {}", e); return 0; }
    };
    if rows.is_empty() { return 0; }

    let now = Utc::now();
    // One price read per distinct token, not per row: a single market typically
    // accumulates many vetoes and they all share one resolution.
    let mut price_cache: std::collections::HashMap<String, Option<Decimal>> =
        std::collections::HashMap::new();
    let mut scored = 0usize;

    for (id, ts, token_id, side, secs_to_expiry) in rows {
        let Ok(recorded_at) = DateTime::parse_from_rfc3339(&ts) else { continue };
        let closes_at = recorded_at.with_timezone(&Utc)
            + chrono::Duration::seconds(secs_to_expiry + SETTLE_GRACE_SECS);
        if now < closes_at { continue; } // not settled yet — try again next sweep

        let price = match price_cache.get(&token_id) {
            Some(p) => *p,
            None => {
                let p = resolve_price(token_id.clone()).await;
                price_cache.insert(token_id.clone(), p);
                p
            }
        };
        let Some(price) = price else { continue }; // lookup failed — retry later

        // `price` is the price of the token the veto was ABOUT, so it already
        // encodes the side: a vetoed NO signal reads the NO token.
        let outcome = if price >= VETO_SETTLE_WON_MIN {
            1
        } else if price <= VETO_SETTLE_LOST_MAX {
            0
        } else {
            continue; // ambiguous — leave unlabeled rather than guess
        };

        if let Err(e) = sqlx::query(
            "UPDATE gboost_vetoes SET outcome = ?, settle_price = ?, scored_at = ?
              WHERE id = ? AND outcome IS NULL"
        )
        .bind(outcome)
        .bind(price.to_string())
        .bind(now.to_rfc3339())
        .bind(id)
        .execute(pool)
        .await {
            error!("❌ DB gboost_veto scoring update failed (id={}, side={}): {}", id, side, e);
        } else {
            scored += 1;
        }
    }
    scored
}

/// Look up the most recent entry price for a token_id.
/// Primary path for reconcile_orphaned_positions — faster than CSV scan.
pub async fn lookup_entry_price_db(pool: &SqlitePool, token_id_str: &str) -> Option<Decimal> {
    lookup_entry_db(pool, token_id_str).await.map(|(price, _)| price)
}

/// Like `lookup_entry_price_db` but also returns the originating strategy name.
/// Used by the orphan-adoption reconciler so a restarted bot re-assigns positions
/// to the strategy that originally opened them, not just the first in the registry.
///
/// Prefers entries from the **current session** to avoid cross-session strategy
/// misattribution: if GboostStrategy traded a token in a prior session and
/// ArbitrageStrategy bought the same token in the current session, the current-session
/// entry (ArbitrageStrategy) is returned rather than the stale GboostStrategy row.
pub async fn lookup_entry_db(pool: &SqlitePool, token_id_str: &str) -> Option<(Decimal, String)> {
    let sid = current_session_id();

    // 1. Try current session first — most authoritative, prevents cross-session contamination.
    let row = sqlx::query(
        "SELECT entry_price, strategy FROM entries WHERE token_id = ? AND session_id = ? ORDER BY ts DESC LIMIT 1"
    )
    .bind(token_id_str)
    .bind(sid)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    // 2. Fall back to any session (e.g. restart in the same session window, or entries
    //    written before session_id column was added and have session_id = '').
    let row = if row.is_some() { row } else {
        sqlx::query(
            "SELECT entry_price, strategy FROM entries WHERE token_id = ? ORDER BY ts DESC LIMIT 1"
        )
        .bind(token_id_str)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
    };

    let row = row?;
    let price = row.try_get::<String, _>(0).ok().and_then(|s| s.parse::<Decimal>().ok())?;
    let strategy = row.try_get::<String, _>(1).ok().unwrap_or_default();
    Some((price, strategy))
}

/// Look up the YES/NO outcome side a token was entered as, from the `entries`
/// table. Used to label a flatten/settlement trade with the leg's actual market
/// outcome instead of a bare order direction ("Sell"). Prefers the most recent
/// entry for the token. Returns `None` if the token was never recorded as an entry.
pub async fn lookup_entry_side_db(pool: &SqlitePool, token_id_str: &str) -> Option<String> {
    let row = sqlx::query(
        "SELECT side FROM entries WHERE token_id = ? ORDER BY ts DESC LIMIT 1"
    )
    .bind(token_id_str)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()?;
    row.try_get::<String, _>(0).ok().filter(|s| !s.is_empty())
}

/// Look up the strategy and entry price for a token from the `open_positions` table.
///
/// This is the MOST AUTHORITATIVE source for restart reconciliation:
///   - Written at position entry time with the exact strategy that owns the position.
///   - NOT contaminated by prior-session trades on the same token by a different strategy.
///   - Must be checked BEFORE the `entries` table in `lookup_entry_from_csv` to prevent
///     cross-strategy misattribution (e.g. GboostStrategy's newer entry overriding an
///     existing ArbitrageStrategy arb pair's NO leg).
///
/// Returns `None` if no row exists or the strategy field is empty.
pub async fn lookup_open_position_strategy(pool: &SqlitePool, token_id_str: &str) -> Option<(Decimal, String)> {
    let row = sqlx::query(
        "SELECT entry_price, strategy FROM open_positions WHERE token_id = ? ORDER BY ts DESC LIMIT 1"
    )
    .bind(token_id_str)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()?;

    let price = row.try_get::<String, _>(0).ok().and_then(|s| s.parse::<Decimal>().ok())?;
    let strategy = row.try_get::<String, _>(1).ok().unwrap_or_default();
    if strategy.is_empty() { return None; }
    Some((price, strategy))
}

// ─── P&L snapshot ────────────────────────────────────────────────────────────

/// Persist a P&L checkpoint (called by the status ticker in main.rs).
/// Provides the time-series data the Control Tower chart will query.
pub async fn record_pnl_snapshot(pool: &SqlitePool, session_pnl: Decimal, collateral: Decimal, total_value: Decimal) {
    let ts = Utc::now().to_rfc3339();
    if let Err(e) = sqlx::query(
        "INSERT INTO pnl_snapshots (ts, session_pnl, collateral, total_value) VALUES (?, ?, ?, ?)"
    )
    .bind(&ts)
    .bind(session_pnl.to_string())
    .bind(collateral.to_string())
    .bind(total_value.to_string())
    .execute(pool)
    .await {
        error!("❌ DB pnl_snapshot write failed: {}", e);
    }
}

// ─── Config KV store ─────────────────────────────────────────────────────────

/// Read a config value by key. Returns None if not present.
pub async fn config_get(pool: &SqlitePool, key: &str) -> Option<String> {
    let row = sqlx::query("SELECT value FROM config WHERE key = ?")
        .bind(key)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()?;

    row.try_get::<String, _>(0).ok()
}

/// Upsert a config key-value pair with the current timestamp.
pub async fn config_set(pool: &SqlitePool, key: &str, value: &str) {
    let ts = Utc::now().to_rfc3339();
    if let Err(e) = sqlx::query(
        "INSERT INTO config (key, value, updated_at) VALUES (?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at"
    )
    .bind(key)
    .bind(value)
    .bind(&ts)
    .execute(pool)
    .await {
        error!("❌ DB config_set failed [{}]: {}", key, e);
    }
}

// ─── Squadron config helpers ─────────────────────────────────────────────────

/// Squadrons that have a config row, with the market class each was classified
/// as — the set the LLM advisor runs a pass over.
///
/// Reads the DB rather than the CAG registry so the advisor needs no handle on
/// the running fleet, and so a squadron that has rotated markets but kept its
/// config is still advised.
pub async fn list_squadron_configs(pool: &SqlitePool) -> Vec<(String, String)> {
    sqlx::query("SELECT squadron_id, market_class FROM squadron_configs ORDER BY squadron_id")
        .fetch_all(pool).await.ok()
        .map(|rows| rows.into_iter().filter_map(|r| {
            Some((r.try_get::<String, _>(0).ok()?, r.try_get::<String, _>(1).unwrap_or_else(|_| "unknown".into())))
        }).collect())
        .unwrap_or_default()
}

/// Load a squadron's config from the `squadron_configs` table.
/// Returns None if the squadron has no stored config yet.
pub async fn squadron_config_get(pool: &SqlitePool, squadron_id: &str) -> Option<String> {
    let row = sqlx::query("SELECT config_json FROM squadron_configs WHERE squadron_id = ?")
        .bind(squadron_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()?;

    row.try_get::<String, _>(0).ok()
}

/// Save or update a squadron's config in the `squadron_configs` table.
pub async fn squadron_config_set(pool: &SqlitePool, squadron_id: &str, config_json: &str) {
    let ts = Utc::now().to_rfc3339();
    if let Err(e) = sqlx::query(
        "INSERT INTO squadron_configs (squadron_id, config_json, created_at, updated_at)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(squadron_id) DO UPDATE SET
            config_json = excluded.config_json,
            updated_at = excluded.updated_at"
    )
    .bind(squadron_id)
    .bind(config_json)
    .bind(&ts)
    .bind(&ts)
    .execute(pool)
    .await {
        error!("❌ DB squadron_config_set failed [{}]: {}", squadron_id, e);
    }
}

/// List all squadron IDs that have stored configs.
pub async fn squadron_config_list(pool: &SqlitePool) -> Vec<String> {
    sqlx::query("SELECT squadron_id FROM squadron_configs ORDER BY created_at DESC")
        .fetch_all(pool)
        .await
        .ok()
        .map(|rows| {
            rows.into_iter()
                .filter_map(|row| row.try_get::<String, _>(0).ok())
                .collect()
        })
        .unwrap_or_default()
}

// ─── Market taxonomy queries ─────────────────────────────────────────────────

/// Resolve the **implemented** raptor kinds linked to a market class.
/// Roadmapped raptors (`implemented = 0`) are excluded so callers only see
/// signal sources that actually exist today.
pub async fn raptors_for_class(pool: &SqlitePool, class: &str) -> Vec<String> {
    sqlx::query(
        "SELECT r.id FROM market_class_raptor m
         JOIN raptor_kind r ON r.id = m.raptor_kind
         WHERE m.market_class = ? AND r.implemented = 1
         ORDER BY r.id"
    )
    .bind(class)
    .fetch_all(pool).await.ok()
    .map(|rows| rows.into_iter().filter_map(|r| r.try_get::<String, _>(0).ok()).collect())
    .unwrap_or_default()
}

/// Resolve the viper kinds linked to a market class.
pub async fn vipers_for_class(pool: &SqlitePool, class: &str) -> Vec<String> {
    sqlx::query(
        "SELECT viper_kind FROM market_class_viper WHERE market_class = ? ORDER BY viper_kind"
    )
    .bind(class)
    .fetch_all(pool).await.ok()
    .map(|rows| rows.into_iter().filter_map(|r| r.try_get::<String, _>(0).ok()).collect())
    .unwrap_or_default()
}

/// Resolve the raptor kinds linked to a market class with full info.
/// Returns (id, display, implemented) tuples.
pub async fn raptors_for_class_full(pool: &SqlitePool, class: &str) -> Vec<(String, String, bool)> {
    sqlx::query(
        "SELECT r.id, r.display, r.implemented FROM market_class_raptor m
         JOIN raptor_kind r ON r.id = m.raptor_kind
         WHERE m.market_class = ?
         ORDER BY r.id"
    )
    .bind(class)
    .fetch_all(pool).await.ok()
    .map(|rows| rows.into_iter().filter_map(|r| {
        let id = r.try_get::<String, _>(0).ok()?;
        let display = r.try_get::<String, _>(1).ok()?;
        let implemented = r.try_get::<i32, _>(2).ok()? == 1;
        Some((id, display, implemented))
    }).collect())
    .unwrap_or_default()
}

/// Resolve the viper kinds linked to a market class with full info.
/// Returns (id, display, venue_agnostic) tuples.
pub async fn vipers_for_class_full(pool: &SqlitePool, class: &str) -> Vec<(String, String, bool)> {
    sqlx::query(
        "SELECT v.id, v.display, v.venue_agnostic FROM market_class_viper m
         JOIN viper_kind v ON v.id = m.viper_kind
         WHERE m.market_class = ?
         ORDER BY v.id"
    )
    .bind(class)
    .fetch_all(pool).await.ok()
    .map(|rows| rows.into_iter().filter_map(|r| {
        let id = r.try_get::<String, _>(0).ok()?;
        let display = r.try_get::<String, _>(1).ok()?;
        let venue_agnostic = r.try_get::<i32, _>(2).ok()? == 1;
        Some((id, display, venue_agnostic))
    }).collect())
    .unwrap_or_default()
}

// ─── Deployment Queue (Admiral Adama extension) ──────────────────────────────

/// Queue a user-requested squadron deployment.
///
/// The CAG will periodically poll the `deployment_queue` table and spawn
/// squadrons for pending requests.
pub async fn queue_deployment(
    deployment_id: &str,
    market_id: &str,
    market_type: &str,
    // Operator-chosen name; "" when they did not supply one.
    name: &str,
    raptors: &[String],
    vipers: &[String],
    viper_budgets: &std::collections::HashMap<String, f64>,
) -> Result<()> {
    let Some(pool) = pool() else {
        return Err(anyhow::anyhow!("DB pool not initialized"));
    };
    
    let raptors_json = serde_json::to_string(raptors)?;
    let vipers_json = serde_json::to_string(vipers)?;
    let budgets_json = if viper_budgets.is_empty() {
        None
    } else {
        Some(serde_json::to_string(viper_budgets)?)
    };
    
    sqlx::query(
        "INSERT INTO deployment_queue (id, market_id, market_type, raptors, vipers, viper_budgets, status, name)
         VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)"
    )
    .bind(deployment_id)
    .bind(market_id)
    .bind(market_type)
    .bind(&raptors_json)
    .bind(&vipers_json)
    .bind(&budgets_json)
    .bind(name)
    .execute(pool).await?;
    
    info!(deployment_id, market_id, market_type, "📋 Deployment request queued");
    Ok(())
}

/// One pending squadron-deployment request from the `deployment_queue` table.
#[derive(Debug, Clone)]
pub struct PendingDeployment {
    pub id: String,
    pub market_id: String,
    pub market_type: String,
    pub raptors: Vec<String>,
    pub vipers: Vec<String>,
    /// Per-viper capital budgets (viper kind → max-exposure USDC) set at deploy time.
    pub viper_budgets: std::collections::HashMap<String, f64>,
    /// Operator-chosen name; empty when they did not supply one.
    pub name: String,
}

/// Return interrupted deployments to the queue so they are picked up again.
///
/// A deployment is marked 'active' while a task trades it. That task dies with
/// the process, but the row does not — and `fetch_pending_deployments` only
/// returns 'pending', so after any restart the squadron simply never came back.
/// The Control Tower restarts the engine for ordinary config changes, so an
/// operator would have lost every deployed squadron without being told.
///
/// Called once at processor startup, when no deployment task can be running by
/// definition. Rows already 'completed' or 'failed' are left alone.
pub async fn requeue_interrupted_deployments() -> u64 {
    let Some(pool) = pool() else { return 0 };
    match sqlx::query(
        "UPDATE deployment_queue SET status = 'pending'
         WHERE status IN ('active', 'processing')"
    ).execute(pool).await {
        Ok(r) => r.rows_affected(),
        Err(e) => {
            warn!("Could not requeue interrupted deployments: {e}");
            0
        }
    }
}

/// Every viper kind, as seeded into `viper_kind`.
///
/// Public because adding a viper means touching more than this table: the deploy
/// budget router in `cag::adama` needs a matching arm, and a kind seeded without
/// one has its operator-chosen budget silently dropped in favor of the
/// compile-time default while the deploy UI reports success. FairValue shipped
/// exactly that way. A test in `cag::adama` pins the two lists together.
pub const VIPER_KINDS: &[(&str, &str, i32)] = &[
    ("arbitrage",    "Arbitrage",     1),
    ("maker",        "Maker",         1),
    ("momentum",     "Momentum",      0),
    ("gboost",       "GBoost",        0),
    ("basis",        "Basis",         0),
    ("time_decay",   "TimeDecay",     0),
    ("trendcapture", "TrendReversal", 0),
    ("convergence",  "Convergence",   0),
    ("fairvalue",    "FairValue",     0),
];

/// Fetch pending deployment requests from the queue.
/// Market classes that already have a deployment the engine has not finished with.
///
/// Covers every non-terminal status, not just `pending`. The auto-deploy seeder
/// needs this rather than `fetch_pending_deployments`: a row is 'processing'
/// from the moment the processor claims it until its squadron is registered
/// with the CAG, and during that window the class appears in neither the
/// pending queue nor the squadron list. Deduping on pending alone would seed a
/// second squadron for a class that is already starting one.
/// Market ids that already have a deployment the engine has not finished with.
///
/// The squadron registry records a market's *question*, never its id, so a
/// "is this market already deployed" check cannot be answered from the registry
/// — comparing a question against a ticker silently never matches. The queue is
/// the only place the id is recorded, so the check belongs here.
pub async fn deployment_markets_in_flight(pool: &SqlitePool) -> Vec<String> {
    sqlx::query(
        "SELECT DISTINCT market_id FROM deployment_queue
         WHERE status IN ('pending', 'processing', 'active')"
    )
    .fetch_all(pool).await.ok()
    .map(|rows| rows.into_iter().filter_map(|r| r.try_get::<String, _>(0).ok()).collect())
    .unwrap_or_default()
}

pub async fn deployment_classes_in_flight(pool: &SqlitePool) -> Vec<String> {
    sqlx::query(
        "SELECT DISTINCT LOWER(market_type) FROM deployment_queue
         WHERE status IN ('pending', 'processing', 'active')"
    )
    .fetch_all(pool).await.ok()
    .map(|rows| rows.into_iter().filter_map(|r| r.try_get::<String, _>(0).ok()).collect())
    .unwrap_or_default()
}

pub async fn fetch_pending_deployments() -> Vec<PendingDeployment> {
    let Some(pool) = pool() else {
        return Vec::new();
    };
    
    sqlx::query(
        "SELECT id, market_id, market_type, raptors, vipers, viper_budgets, name FROM deployment_queue
         WHERE status = 'pending' ORDER BY created_at ASC LIMIT 10"
    )
    .fetch_all(pool).await.ok()
    .map(|rows| rows.into_iter().filter_map(|r| {
        let id = r.try_get::<String, _>(0).ok()?;
        let market_id = r.try_get::<String, _>(1).ok()?;
        let market_type = r.try_get::<String, _>(2).ok()?;
        let raptors_json = r.try_get::<String, _>(3).ok()?;
        let vipers_json = r.try_get::<String, _>(4).ok()?;
        let budgets_json = r.try_get::<Option<String>, _>(5).ok().flatten();
        let raptors: Vec<String> = serde_json::from_str(&raptors_json).ok()?;
        let vipers: Vec<String> = serde_json::from_str(&vipers_json).ok()?;
        let viper_budgets = budgets_json
            .and_then(|j| serde_json::from_str(&j).ok())
            .unwrap_or_default();
        let name = r.try_get::<String, _>(6).unwrap_or_default();
        Some(PendingDeployment { id, market_id, market_type, raptors, vipers, viper_budgets, name })
    }).collect())
    .unwrap_or_default()
}

/// Update deployment status in the queue.
pub async fn update_deployment_status(
    deployment_id: &str,
    status: &str,
    squadron_id: Option<&str>,
    error: Option<&str>,
) -> Result<()> {
    let Some(pool) = pool() else {
        return Err(anyhow::anyhow!("DB pool not initialized"));
    };
    
    sqlx::query(
        "UPDATE deployment_queue
         SET status = ?, squadron_id = ?, error = ?, updated_at = datetime('now')
         WHERE id = ?"
    )
    .bind(status)
    .bind(squadron_id)
    .bind(error)
    .bind(deployment_id)
    .execute(pool).await?;
    
    info!(deployment_id, status, "📋 Deployment status updated");
    Ok(())
}

/// Fetch all deployments from the queue (for status endpoint).
/// Returns: (id, market_id, market_type, raptors, vipers, status, squadron_id, error, created_at)
pub async fn fetch_all_deployments() -> Vec<(String, String, String, Vec<String>, Vec<String>, String, Option<String>, Option<String>, String)> {
    let Some(pool) = pool() else {
        return Vec::new();
    };
    
    sqlx::query(
        "SELECT id, market_id, market_type, raptors, vipers, status, squadron_id, error, created_at 
         FROM deployment_queue ORDER BY created_at DESC LIMIT 50"
    )
    .fetch_all(pool).await.ok()
    .map(|rows| rows.into_iter().filter_map(|r| {
        let id = r.try_get::<String, _>(0).ok()?;
        let market_id = r.try_get::<String, _>(1).ok()?;
        let market_type = r.try_get::<String, _>(2).ok()?;
        let raptors_json = r.try_get::<String, _>(3).ok()?;
        let vipers_json = r.try_get::<String, _>(4).ok()?;
        let status = r.try_get::<String, _>(5).ok()?;
        let squadron_id = r.try_get::<Option<String>, _>(6).ok()?;
        let error = r.try_get::<Option<String>, _>(7).ok()?;
        let created_at = r.try_get::<String, _>(8).ok()?;
        let raptors: Vec<String> = serde_json::from_str(&raptors_json).ok()?;
        let vipers: Vec<String> = serde_json::from_str(&vipers_json).ok()?;
        Some((id, market_id, market_type, raptors, vipers, status, squadron_id, error, created_at))
    }).collect())
    .unwrap_or_default()
}

/// Classify a market into a `market_class` id using the seeded rule table.
///
/// Resolution order (highest-confidence first, by ascending `priority`):
///   1. `category`     — exact case-insensitive match on the venue's category.
///   2. `symbol_token` — the pattern appears as a `-`/`_` delimited token in
///                       any leg symbol (e.g. `nfl` in `aec-nfl-lac-ten-…`).
///   3. `slug`         — the pattern appears anywhere in the slug.
///
/// Falls back to `"unknown"`, which maps only to the venue-agnostic vipers —
/// so a misclassified or brand-new market still trades safely (arbitrage/maker)
/// and can never enable a domain strategy that doesn't fit it.
pub async fn classify_market(
    pool: &SqlitePool,
    category: &str,
    symbols: &[&str],
    slug: &str,
) -> String {
    let rows = sqlx::query(
        "SELECT pattern, match_kind, market_class FROM market_class_rule
         ORDER BY priority ASC, id ASC"
    ).fetch_all(pool).await.unwrap_or_default();

    let cat = category.to_ascii_lowercase();
    let slug_l = slug.to_ascii_lowercase();
    // Tokenise every leg symbol on '-' and '_' for symbol_token matching.
    let tokens: std::collections::HashSet<String> = symbols.iter()
        .flat_map(|s| s.to_ascii_lowercase()
            .split(['-', '_'])
            .map(|t| t.to_string())
            .collect::<Vec<_>>())
        .collect();

    for row in rows {
        let pattern = row.try_get::<String, _>(0).unwrap_or_default().to_ascii_lowercase();
        let kind    = row.try_get::<String, _>(1).unwrap_or_default();
        let class   = row.try_get::<String, _>(2).unwrap_or_default();
        let hit = match kind.as_str() {
            "category"     => !cat.is_empty() && cat == pattern,
            "symbol_token" => tokens.contains(&pattern),
            "slug"         => !slug_l.is_empty() && slug_l.contains(&pattern),
            _ => false,
        };
        if hit {
            return class;
        }
    }
    "unknown".to_string()
}

/// Persist the resolved market class onto a squadron's `squadron_configs` row.
/// No-op if the row does not exist yet (seed the config first).
pub async fn set_squadron_market_class(pool: &SqlitePool, squadron_id: &str, class: &str) {
    if let Err(e) = sqlx::query("UPDATE squadron_configs SET market_class = ? WHERE squadron_id = ?")
        .bind(class)
        .bind(squadron_id)
        .execute(pool)
        .await
    {
        error!("❌ DB set_squadron_market_class failed [{}]: {}", squadron_id, e);
    }
}

/// Read the resolved market class for a squadron from its `squadron_configs`
/// row. Returns `None` if the squadron has no row (or no class persisted yet).
pub async fn get_squadron_market_class(pool: &SqlitePool, squadron_id: &str) -> Option<String> {
    sqlx::query("SELECT market_class FROM squadron_configs WHERE squadron_id = ?")
        .bind(squadron_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .and_then(|row| row.try_get::<String, _>(0).ok())
        .filter(|c| !c.is_empty())
}

// ─── Config history (audit log) ──────────────────────────────────────────────

/// Record a config change to the append-only audit log.
///
/// `changed_by` should be one of:
///   - `"operator"`        — human changed via Control Tower PATCH /api/config
///   - `"llm_advisor"`     — LLM recommendation applied manually by operator
///   - `"startup_default"` — first write of compile-time defaults at startup
///
/// Both `old_value` and `new_value` are full JSON snapshots of `DynamicConfig`,
/// so the entire parameter set is recoverable at any point in time.
pub async fn record_config_change(
    pool: &SqlitePool,
    changed_by: &str,
    param_name: &str,
    old_value: Option<&str>,
    new_value: &str,
) {
    let ts = Utc::now().to_rfc3339();
    let sid = current_session_id();
    if let Err(e) = sqlx::query(
        "INSERT INTO config_history (ts, session_id, changed_by, param_name, old_value, new_value)
         VALUES (?, ?, ?, ?, ?, ?)"
    )
    .bind(&ts)
    .bind(sid)
    .bind(changed_by)
    .bind(param_name)
    .bind(old_value)
    .bind(new_value)
    .execute(pool)
    .await {
        error!("❌ DB config_history write failed: {}", e);
    }
}

// ─── Static config snapshot ──────────────────────────────────────────────────

/// Serializable snapshot of the compile-time constants in `config.rs`.
///
/// All fields are `String` (for Decimal) or primitive types so the struct is
/// trivially serializable without bringing extra dependencies into `db.rs`.
/// Stored as a JSON blob in `config_history` so operators can diff consecutive
/// sessions to see exactly what changed between two compiles.
#[derive(Serialize)]
struct StaticConfigSnapshot<'a> {
    // Global
    ghost_mode:                        bool,
    enable_momentum_trading:           bool,
    enable_arbitrage_trading:          bool,
    enable_maker_trading:              bool,
    enable_telegram:                   bool,
    enable_x:                          bool,
    // Risk / exposure
    max_exposure_per_token_usdc:       String,
    min_hourly_market_vol24h:          f64,
    momentum_max_exposure_usdc:        String,
    maker_max_exposure_usdc:           String,
    arbitrage_max_exposure_usdc:       String,
    time_decay_max_exposure_usdc:      String,
    // Momentum signals
    btc_momentum_threshold:            String,
    eth_momentum_threshold:            String,
    sol_momentum_threshold:            String,
    momentum_window_secs:              u64,
    momentum_short_window_secs:        u64,
    momentum_short_window_fraction:    String,
    momentum_confirmation_ticks:       u32,
    momentum_kelly_max_multiplier:     String,
    momentum_min_trade_size_usdc:      String,
    momentum_max_trade_size_usdc:      String,
    max_momentum_entry_price:          String,
    max_momentum_crossing_entry_price: String,
    momentum_obi_adverse_block:        String,
    momentum_target_profit_pct:        String,
    momentum_stop_loss_pct:            String,
    momentum_reversal_ratio:           String,
    momentum_min_hold_secs_before_reversal: i64,
    momentum_window_bearish_block:     String,
    momentum_window_bullish_block:     String,
    momentum_max_entry_ask_sum:        String,
    momentum_take_profit_ceiling:      String,
    momentum_acceleration_bypass_multiplier: String,
    momentum_decay_exit_fraction:      String,
    btc_strike_buffer:                 String,
    eth_strike_buffer:                 String,
    sol_strike_buffer:                 String,
    // Maker
    maker_max_entry_price:             String,
    maker_min_spread:                  String,
    maker_bid_buffer:                  String,
    maker_min_secs_to_expiry:          i64,
    maker_velocity_bias_threshold:     String,
    // Arbitrage
    arbitrage_profit_threshold:        String,
    max_sum_price_for_entry:           String,
    arbitrage_position_size_usdc:      String,
    early_exit_combined_bid_threshold: String,
    // Order execution
    min_order_shares:                  String,
    min_order_usdc:                    String,
    min_liquidity_fill_ratio:          String,
    buy_price_offset:                  String,
    sell_price_offset:                 String,
    max_buy_limit_price:               String,
    // LLM Advisor
    enable_llm_advisor:                bool,
    llm_advisor_interval_secs:         u64,
    llm_advisor_trades_lookback:       i64,
    llm_ollama_url:                    &'a str,
    llm_ollama_model:                  &'a str,
}

/// Snapshot the compile-time constants from `config.rs` into `config_history`.
///
/// Called once per process start (right after `init_session`) so there is always
/// a complete record of the compiled trading parameters that were active during
/// every session.  Unlike `DynamicConfig`, these values can _only_ change when the
/// developer edits `config.rs` and recompiles — so diffing consecutive
/// `startup_static` rows across sessions immediately reveals what was changed.
///
/// The row is tagged `changed_by = "startup_static"`,
/// `param_name = "static_config_snapshot"`, and carries the full JSON in `new_value`.
/// `old_value` is always NULL — the audit trail lets callers read the previous
/// session's row to build a diff if they need one.
pub async fn record_static_config_snapshot(pool: &SqlitePool) {
    let snap = StaticConfigSnapshot {
        ghost_mode:                        config::GHOST_MODE,
        enable_momentum_trading:           config::ENABLE_MOMENTUM_TRADING,
        enable_arbitrage_trading:          config::ENABLE_ARBITRAGE_TRADING,
        enable_maker_trading:              config::ENABLE_MAKER_TRADING,
        enable_telegram:                   config::ENABLE_TELEGRAM,
        enable_x:                          config::ENABLE_X,
        max_exposure_per_token_usdc:       config::MAX_EXPOSURE_PER_TOKEN_USDC.to_string(),
        min_hourly_market_vol24h:          config::MIN_HOURLY_MARKET_VOL24H,
        momentum_max_exposure_usdc:        config::MOMENTUM_MAX_EXPOSURE_USDC.to_string(),
        maker_max_exposure_usdc:           config::MAKER_MAX_EXPOSURE_USDC.to_string(),
        arbitrage_max_exposure_usdc:       config::ARBITRAGE_MAX_EXPOSURE_USDC.to_string(),
        time_decay_max_exposure_usdc:      config::TIME_DECAY_MAX_EXPOSURE_USDC.to_string(),
        btc_momentum_threshold:            config::BTC_MOMENTUM_THRESHOLD.to_string(),
        eth_momentum_threshold:            (config::MOMENTUM_THRESHOLD_PCT * rust_decimal_macros::dec!(3500)).to_string(),
        sol_momentum_threshold:            (config::MOMENTUM_THRESHOLD_PCT * rust_decimal_macros::dec!(160)).to_string(),
        momentum_window_secs:              config::MOMENTUM_WINDOW_SECS,
        momentum_short_window_secs:        config::MOMENTUM_SHORT_WINDOW_SECS,
        momentum_short_window_fraction:    config::MOMENTUM_SHORT_WINDOW_FRACTION.to_string(),
        momentum_confirmation_ticks:       config::MOMENTUM_CONFIRMATION_TICKS,
        momentum_kelly_max_multiplier:     config::MOMENTUM_KELLY_MAX_MULTIPLIER.to_string(),
        momentum_min_trade_size_usdc:      config::MOMENTUM_MIN_TRADE_SIZE_USDC.to_string(),
        momentum_max_trade_size_usdc:      config::MOMENTUM_MAX_TRADE_SIZE_USDC.to_string(),
        max_momentum_entry_price:          config::MAX_MOMENTUM_ENTRY_PRICE.to_string(),
        max_momentum_crossing_entry_price: config::MAX_MOMENTUM_CROSSING_ENTRY_PRICE.to_string(),
        momentum_obi_adverse_block:        config::MOMENTUM_OBI_ADVERSE_BLOCK.to_string(),
        momentum_target_profit_pct:        config::MOMENTUM_TARGET_PROFIT_PERCENT.to_string(),
        momentum_stop_loss_pct:            config::MOMENTUM_STOP_LOSS_PERCENT.to_string(),
        momentum_reversal_ratio:           config::MOMENTUM_REVERSAL_RATIO.to_string(),
        momentum_min_hold_secs_before_reversal: config::MOMENTUM_MIN_HOLD_SECS_BEFORE_REVERSAL,
        momentum_window_bearish_block:     config::MOMENTUM_WINDOW_BEARISH_BLOCK.to_string(),
        momentum_window_bullish_block:     config::MOMENTUM_WINDOW_BULLISH_BLOCK.to_string(),
        momentum_max_entry_ask_sum:        config::MOMENTUM_MAX_ENTRY_ASK_SUM.to_string(),
        momentum_take_profit_ceiling:      config::MOMENTUM_TAKE_PROFIT_CEILING.to_string(),
        momentum_acceleration_bypass_multiplier: config::MOMENTUM_ACCELERATION_BYPASS_MULTIPLIER.to_string(),
        momentum_decay_exit_fraction:      config::MOMENTUM_DECAY_EXIT_FRACTION.to_string(),
        btc_strike_buffer:                 (config::STRIKE_BUFFER_PCT * rust_decimal_macros::dec!(100000)).to_string(),
        eth_strike_buffer:                 (config::STRIKE_BUFFER_PCT * rust_decimal_macros::dec!(3500)).to_string(),
        sol_strike_buffer:                 (config::STRIKE_BUFFER_PCT * rust_decimal_macros::dec!(160)).to_string(),
        maker_max_entry_price:             config::MAKER_MAX_ENTRY_PRICE.to_string(),
        maker_min_spread:                  config::MAKER_MIN_SPREAD.to_string(),
        maker_bid_buffer:                  config::MAKER_BID_BUFFER.to_string(),
        maker_min_secs_to_expiry:          config::MAKER_MIN_SECS_TO_EXPIRY,
        maker_velocity_bias_threshold:     config::MAKER_VELOCITY_BIAS_THRESHOLD.to_string(),
        arbitrage_profit_threshold:        config::ARBITRAGE_PROFIT_THRESHOLD.to_string(),
        max_sum_price_for_entry:           config::MAX_SUM_PRICE_FOR_ENTRY.to_string(),
        arbitrage_position_size_usdc:      config::ARBITRAGE_POSITION_SIZE_USDC.to_string(),
        early_exit_combined_bid_threshold: config::EARLY_EXIT_COMBINED_BID_THRESHOLD.to_string(),
        min_order_shares:                  config::MIN_ORDER_SHARES.to_string(),
        min_order_usdc:                    config::MIN_ORDER_USDC.to_string(),
        min_liquidity_fill_ratio:          config::MIN_LIQUIDITY_FILL_RATIO.to_string(),
        buy_price_offset:                  config::BUY_PRICE_OFFSET.to_string(),
        sell_price_offset:                 config::SELL_PRICE_OFFSET.to_string(),
        max_buy_limit_price:               config::MAX_BUY_LIMIT_PRICE.to_string(),
        enable_llm_advisor:                config::ENABLE_LLM_ADVISOR,
        llm_advisor_interval_secs:         config::LLM_ADVISOR_INTERVAL_SECS,
        llm_advisor_trades_lookback:       config::LLM_ADVISOR_TRADES_LOOKBACK,
        llm_ollama_url:                    config::LLM_OLLAMA_URL,
        llm_ollama_model:                  config::LLM_OLLAMA_MODEL,
    };

    match serde_json::to_string(&snap) {
        Ok(json) => {
            record_config_change(
                pool,
                "startup_static",
                "static_config_snapshot",
                None,   // no old_value — diff consecutive sessions in config_history to find changes
                &json,
            ).await;
            info!("📸 Static config snapshot recorded for session {}", current_session_id());
        }
        Err(e) => {
            error!("❌ DB static_config_snapshot serialize failed: {}", e);
        }
    }
}

// ─── Open positions ──────────────────────────────────────────────────────────

/// Insert a row into `open_positions` when a new position is entered.
/// Called for every entry — both ghost mode and live — so the UI and LLM Advisor
/// can see in-flight positions that have not yet appeared as completed trades.
/// Stamp the venue fee already paid to open this position.
///
/// Separate from `record_open_position` because the fee is only known after the
/// fill comes back, while the row is written earlier (and by fifteen call sites
/// across three venues). Settlement and off-strategy bookings read it back to
/// net the entry leg out of recorded P&L.
pub async fn set_open_position_entry_fee(pool: &SqlitePool, token_id: &str, entry_fee: Decimal) {
    if let Err(e) = sqlx::query("UPDATE open_positions SET entry_fee = ? WHERE token_id = ?")
        .bind(entry_fee.to_string())
        .bind(token_id)
        .execute(pool)
        .await
    {
        error!("❌ DB set_open_position_entry_fee failed for {}: {}", token_id, e);
    }
}

/// Read back the entry fee recorded for an open position, if any.
///
/// The orphan arbiter needs this before it purges the row: `close_open_position`
/// DELETEs, and the `entries` ledger carries no fee column, so once the row is
/// gone the fee paid to open that leg is unrecoverable and the round trip books
/// gross. Returns `None` when the row is absent or the column was never set —
/// callers treat that as zero, which is the correct reading for a maker fill.
pub async fn get_open_position_entry_fee(pool: &SqlitePool, token_id: &str) -> Option<Decimal> {
    let row: Option<(Option<String>,)> =
        sqlx::query_as("SELECT entry_fee FROM open_positions WHERE token_id = ? LIMIT 1")
            .bind(token_id)
            .fetch_optional(pool)
            .await
            .unwrap_or(None);
    row.and_then(|(f,)| f).and_then(|s| s.parse::<Decimal>().ok())
}

#[allow(clippy::too_many_arguments)]
pub async fn record_open_position(
    pool: &SqlitePool,
    // Filing dimensions (venue / market class / underlying), same as
    // `record_trade_db`. Reconciliation callers that only know the book pass
    // `TradeScope::shard_only` and the class/underlying columns stay NULL.
    scope: &TradeScope,
    // Squadron that owns the position; '' for callers that predate squadrons.
    squadron_id: &str,
    strategy: &str,
    token_id: &str,
    market: &str,
    side: &str,
    entry_price: Decimal,
    shares: Decimal,
    ghost_mode: bool,
) {
    record_open_position_with_status(pool, scope, squadron_id, strategy, token_id, market, side, entry_price, shares, ghost_mode, "confirmed").await;
}

/// Record an open position with explicit status.
/// status: "pending" = Viper Launch (order placed, waiting chain confirmation)
///         "confirmed" = Mission In-Flight (on-chain confirmed)
///
/// `ghost_mode` stays an explicit parameter rather than reading `scope.ghost`:
/// several callers write a live row from a scope whose ghost flag tracks the
/// squadron's *current* mode, and the two can disagree mid-flip. The order
/// path's own flag is the truth for this row.
#[allow(clippy::too_many_arguments)]
pub async fn record_open_position_with_status(
    pool: &SqlitePool,
    // Filing dimensions — see `record_open_position`.
    scope: &TradeScope,
    // Squadron that owns the position; '' for callers that predate squadrons.
    squadron_id: &str,
    strategy: &str,
    token_id: &str,
    market: &str,
    side: &str,
    entry_price: Decimal,
    shares: Decimal,
    ghost_mode: bool,
    status: &str,
) {
    let ts = Utc::now().to_rfc3339();
    let sid = current_session_id();
    let venue = resolved_venue(scope);
    // Use INSERT WHERE NOT EXISTS to prevent duplicate rows for the same token_id.
    // Without a UNIQUE constraint on token_id, `INSERT OR REPLACE` would always INSERT
    // a new row (never replacing), causing duplicate open_positions rows when the
    // strategy top-ups an existing position or when chain-sync has already adopted it.
    // If a row for this token already exists (chain-adopted or from a prior cycle),
    // we skip the insert — chain-sync will keep the shares count accurate via UPDATE.
    match sqlx::query(
        "INSERT INTO open_positions
         (ts, session_id, strategy, token_id, market, side, entry_price, shares, ghost_mode, status, squadron_id,
          venue, market_class, underlying)
         SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
         WHERE NOT EXISTS (
             SELECT 1 FROM open_positions
             WHERE token_id = ? AND strategy = ?
               AND (squadron_id = ? OR squadron_id = '')
         )"
    )
    .bind(&ts)
    .bind(sid)
    .bind(strategy)
    .bind(token_id)
    .bind(market)
    .bind(side)
    .bind(entry_price.to_string())
    .bind(shares.to_string())
    .bind(ghost_mode as i32)
    .bind(status)
    .bind(squadron_id)
    .bind(venue)
    .bind(scope.market_class.clone())
    .bind(scope.underlying.clone())
    .bind(token_id)
    .bind(strategy)
    .bind(squadron_id)
    .execute(pool)
    .await {
        Ok(_)  => {}
        Err(e) => { error!("❌ DB record_open_position failed: {}", e); }
    }
}

/// Update a pending position to confirmed status after blockchain confirmation.
pub async fn confirm_position_status(
    pool: &SqlitePool,
    strategy: &str,
    token_id: &str,
) {
    if let Err(e) = sqlx::query(
        "UPDATE open_positions SET status = 'confirmed' WHERE strategy = ? AND token_id = ?"
    )
    .bind(strategy)
    .bind(token_id)
    .execute(pool)
    .await {
        error!("❌ DB confirm_position_status failed: {}", e);
    }
}

/// Close simulated rows left behind by an earlier session.
///
/// The restart half of the ghost-row leak. A simulated position holds nothing on
/// chain, so it is excluded from the chain reconciler and from
/// `purge_stale_open_positions` by design — and nothing rehydrates ghost rows into
/// the in-memory map after a restart, so a paper position open when the process
/// died is never exited, never closed, and stays open forever.
///
/// Ghost state is meaningless across a process boundary: the map that owned those
/// positions is gone, so no viper can act on them. Anything simulated that does not
/// belong to the running session is therefore dead by definition.
///
/// Deliberately keyed on session rather than age. A clock threshold has to guess how
/// long a paper position may legitimately live; session identity does not guess.
pub async fn close_stale_ghost_positions(pool: &SqlitePool, current_session_id: &str) -> u64 {
    match sqlx::query(
        "DELETE FROM open_positions
         WHERE ghost_mode = 1 AND COALESCE(session_id, '') <> ?"
    )
    .bind(current_session_id)
    .execute(pool)
    .await
    {
        Ok(r) => {
            let n = r.rows_affected();
            if n > 0 {
                info!("👻 Startup: closed {} simulated position row(s) left by an earlier session", n);
            }
            n
        }
        Err(e) => {
            error!("❌ DB close_stale_ghost_positions failed: {}", e);
            0
        }
    }
}

/// Close SIMULATED open rows for a token, whatever strategy holds them.
///
/// Deliberately scoped to `ghost_mode = 1`. A real row is reconciled against the
/// chain and swept by `purge_stale_open_positions`; a ghost row is excluded from
/// both, by design, because the chain has no opinion about a simulation. That
/// leaves market expiry as the only moment a still-held ghost position can be
/// closed, and nothing was doing it — the row stayed open, kept contributing to
/// portfolio value at its last mark, and outlived the market resolving.
///
/// Keyed by token rather than (strategy, token) because the caller is dropping
/// every position on an expiring market, not one viper's.
pub async fn close_ghost_open_position(pool: &SqlitePool, token_id: &str) {
    match sqlx::query("DELETE FROM open_positions WHERE token_id = ? AND ghost_mode = 1")
        .bind(token_id)
        .execute(pool)
        .await
    {
        Ok(r) if r.rows_affected() > 0 => info!(
            "👻 Closed {} simulated position row(s) for expired token {}",
            r.rows_affected(), token_id,
        ),
        Ok(_) => {}
        Err(e) => error!("❌ DB close_ghost_open_position failed for {}: {}", token_id, e),
    }
}

/// Remove a row from `open_positions` when a position is closed (any exit reason).
/// Keyed by (strategy, token_id) — unique across all sessions.
pub async fn close_open_position(
    pool: &SqlitePool,
    strategy: &str,
    token_id: &str,
) {
    if let Err(e) = sqlx::query(
        "DELETE FROM open_positions WHERE strategy = ? AND token_id = ?"
    )
    .bind(strategy)
    .bind(token_id)
    .execute(pool)
    .await {
        error!("❌ DB close_open_position failed: {}", e);
    }
}

/// Clear all live (non-ghost) open_positions rows in one shot.
///
/// Called at startup in LIVE mode (`GHOST_MODE = false`) to wipe every row written
/// by prior sessions before the chain-sync re-adopts the true on-chain state.
/// This ensures the UI and LLM Advisor see zero stale rows from crashed sessions,
/// avoided-fill orders, or orphan accumulation cycles — even if a prior session's
/// `close_open_position` never ran.
///
/// Ghost-mode rows (`ghost_mode = 1`) are intentionally preserved so simulated
/// trade history remains coherent across live/ghost restarts.
///
/// Returns the number of rows deleted.
pub async fn purge_all_live_open_positions(pool: &SqlitePool) -> usize {
    match sqlx::query("DELETE FROM open_positions WHERE ghost_mode = 0")
        .execute(pool)
        .await
    {
        Ok(r)  => r.rows_affected() as usize,
        Err(e) => { error!("❌ DB purge_all_live_open_positions failed: {}", e); 0 }
    }
}

/// Returns true if a `trades` row already exists for `market` whose share count
/// matches `shares` within a small dust tolerance.
///
/// Used by `purge_stale_open_positions` to decide whether a stale (vanished-from-
/// wallet) position was ALREADY booked to the ledger — either by the strategy's own
/// close path or by the idempotent settlement path (`record_settlement_trade_idempotent`).
/// Matching on market+shares (rather than market+side) intentionally covers the
/// arbitrage case where a resolved YES+NO pair is booked as a single YES-side
/// settlement row: the NO leg shares equal the pair size, so it still matches and is
/// correctly NOT re-booked. If a match exists we must NOT fabricate a second row.
pub async fn market_has_matching_trade(pool: &SqlitePool, market: &str, shares: Decimal) -> bool {
    let share_dust = Decimal::new(1, 3); // 0.001
    let rows: Vec<String> = sqlx::query_scalar("SELECT shares FROM trades WHERE market = ?")
        .bind(market)
        .fetch_all(pool)
        .await
        .unwrap_or_default();
    rows.iter().any(|s| {
        s.parse::<Decimal>()
            .map(|v| (v - shares).abs() <= share_dust)
            .unwrap_or(false)
    })
}

/// Has a settled arb PAIR already been booked for this market at this size?
///
/// TWO paths book a resolved YES+NO pair as ONE row (side "YES", shares = pairs),
/// and both must be recognized here:
///
///   * `record_settled_arb_trade` — `Settlement (YES+NO → $1.00)`
///   * `detect_orphaned_arb_settlements` — `Settlement (auto-redeemed by Polymarket)`
///
/// Either way both legs are covered by that single row and the pair's economics are
/// already netted in it. Missing the second reason leaves the same double-book alive
/// through a sibling path — and that path's own row cleanup is a `let _ = DELETE`,
/// so a busy database silently leaves the leg rows behind for this sweep to find.
///
/// Side-scoped settlement dedup therefore cannot see it from the NO leg: a leftover
/// NO row whose market has resolved finds no side="NO" settlement and books a
/// spurious extra loss of `entry × qty` on top of the netted pair. Leftover legs are
/// reachable in ordinary operation — a crash between the redeem transaction and
/// `purge_settled_legs`, a failed purge, or the operator redeeming in the Polymarket
/// UI rather than in-app.
///
/// Matched on the exact combined reason rather than by widening the side-scoped
/// check, because the legitimate two-leg "pending redemption" accrual books one row
/// PER side and must not suppress its own second leg.
pub async fn market_has_settled_arb_pair(
    pool: &SqlitePool,
    market: &str,
    shares: Decimal,
) -> bool {
    let share_dust = Decimal::new(1, 3); // 0.001
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT shares FROM trades
          WHERE market = ?
            AND reason IN ('Settlement (YES+NO → $1.00)',
                           'Settlement (auto-redeemed by Polymarket)')"
    )
        .bind(market)
        .fetch_all(pool)
        .await
        .unwrap_or_default();
    rows.iter().any(|s| {
        s.parse::<Decimal>()
            .map(|booked| (booked - shares).abs() <= share_dust)
            .unwrap_or(false)
    })
}

/// How long a stale `open_positions` row may be deferred awaiting a settlement
/// answer before the sweep stops deferring and lets it fall back to the
/// pre-existing mark-priced reconciliation.
///
/// Bounded by AGE rather than by attempts — attempt counts do not survive a
/// restart, and an unbounded defer would replace a permanent silent delete with
/// a permanent phantom row, which is the same bug wearing the opposite sign.
/// Shared by every venue's settlement sweep so the policy cannot drift apart.
pub const SETTLEMENT_DEFER_MAX_SECS: i64 = 24 * 3600;

/// Non-ghost, venue-confirmed `open_positions` rows as `(token_id, ts)` —
/// the candidate set every venue's settlement sweep starts from.
///
/// `pending` rows are excluded on purpose: a pending row may be an order that
/// never filled, and asking a venue what a never-held position settled at can
/// only fabricate a booking. Ghost rows hold nothing at any venue by
/// definition, so a settlement lookup for one is meaningless (and booking one
/// would corrupt the simulated ledger — see the ghost exclusion on
/// `purge_stale_open_positions`).
pub async fn confirmed_open_positions(pool: &SqlitePool) -> Vec<(String, String)> {
    sqlx::query_as::<_, (String, String)>(
        "SELECT token_id, ts FROM open_positions
          WHERE ghost_mode = 0 AND COALESCE(status,'confirmed') = 'confirmed'"
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default()
}

/// Returns true if a SETTLEMENT trade row already exists for `market` + `side` with a
/// share count matching `shares` within dust tolerance.
///
/// Settlement-scoped variant of `market_has_matching_trade`. The generic market+shares
/// match is too weak for resolution-time booking: an earlier same-session round-trip on
/// the same market with the same share count (e.g. a 15-share orphan flatten in the
/// morning, then a fresh 15-share arb pair at noon) false-matches and silently drops
/// the settlement row (observed 2026-07-15: the winning YES leg's +$1.50 was never
/// booked because the 09:23 "Orphan flatten" row matched on market+shares).
pub async fn market_has_settlement_trade(
    pool: &SqlitePool,
    market: &str,
    side: &str,
    shares: Decimal,
) -> bool {
    let share_dust = Decimal::new(1, 3); // 0.001
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT shares FROM trades WHERE market = ? AND side = ? AND reason LIKE 'Settlement%'"
    )
        .bind(market)
        .bind(side)
        .fetch_all(pool)
        .await
        .unwrap_or_default();
    rows.iter().any(|s| {
        s.parse::<Decimal>()
            .map(|v| (v - shares).abs() <= share_dust)
            .unwrap_or(false)
    })
}

/// Returns true if any resolution-time settlement row ("pending redemption") already
/// exists for `market`. Used by auto_settle to avoid double-booking P&L that chain-sync
/// already recognized at resolution — the later on-chain redemption is then a cash-only
/// event.
pub async fn market_has_pending_redemption_settlement(pool: &SqlitePool, market: &str) -> bool {
    sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM trades WHERE market = ? AND reason LIKE '%pending redemption%' LIMIT 1"
    )
        .bind(market)
        .fetch_optional(pool)
        .await
        .unwrap_or(None)
        .is_some()
}

/// Tokens whose `open_positions` row the chain sweep has just booked and
/// deleted, waiting for the venue loop that owns the in-memory position map
/// to release the matching entry.
///
/// The sweep is DB-only by design and has no handle on the session's position
/// map, so until now nothing released a settle-held position from memory:
/// `cleanup_expired_positions` only ever looks at the CURRENT market's tokens,
/// `auto_settle` never touches the map, and a rotated-away token matches
/// nothing. Observed 2026-09-01: a FairValue leg the sweep had booked was
/// still being evaluated 29 minutes later, and its $4.76 counted against a
/// $12 exposure cap for the rest of the session. Process-global for the same
/// reason as the venue registries above; a restart empties both the set and
/// the map it feeds, so nothing can go stale across one.
fn released_positions() -> &'static std::sync::Mutex<std::collections::HashSet<String>> {
    static REG: OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> = OnceLock::new();
    REG.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

fn note_position_released(token_id: &str) {
    if let Ok(mut reg) = released_positions().lock() {
        reg.insert(token_id.to_string());
    }
}

/// Drain the tokens the sweep has closed since the last call. Each venue loop
/// calls this on its own cadence and drops the matching map entries.
pub fn take_released_positions() -> Vec<String> {
    match released_positions().lock() {
        Ok(mut reg) => reg.drain().collect(),
        Err(_) => Vec::new(),
    }
}

/// Delete a LIVE `pending` row for `token_id` — an order that was placed and
/// never filled.
///
/// The rotation half of the pending-row leak (the live twin of the ghost-row
/// fix). At rotation the venue confirms every resting order cancelled, so a
/// `pending` row for the market being left behind describes an order that no
/// longer exists. Nothing else closed it for an hour: the purge protects
/// pending rows through `STALE_PENDING_GRACE_SECS` (60 min), so the Control
/// Tower showed a "Launch" for a dead quote until then. Observed 2026-09-02:
/// `MakerStrategy YES 0.48 pending` still open a minute after the rotation
/// cancel. Ghost rows are untouched — they have their own path.
pub async fn close_pending_open_position(pool: &SqlitePool, token_id: &str) {
    match sqlx::query(
        "DELETE FROM open_positions WHERE token_id = ? AND status = 'pending' AND ghost_mode = 0"
    )
        .bind(token_id)
        .execute(pool)
        .await
    {
        Ok(r) if r.rows_affected() > 0 => info!(
            "🧹 Closed {} pending row(s) for {} — its resting order was cancelled at rotation",
            r.rows_affected(), token_id,
        ),
        Ok(_) => {}
        Err(e) => error!("❌ DB close_pending_open_position failed for {}: {}", token_id, e),
    }
}

/// Delete every `open_positions` row whose token_id is NOT in `live_token_ids`.
///
/// Called by the chain-sync task after it fetches the wallet's actual live positions
/// from the Polymarket Data API.  Any row left in the table after that is stale
/// (settled, sold, or from a crashed session that never called close_open_position).
///
/// Ledger reconciliation: a `confirmed` position that vanished from the wallet moved
/// real cash but — if it closed OUTSIDE the strategy's own exit path (e.g. a resting
/// maker order filled during an hourly market rotation that reset loop state) — left
/// NO row in the `trades` ledger. That makes the balance graph dip with no explaining
/// tradelog event. Before deleting such a row we book a best-effort "ChainReconcile"
/// trade (exit priced at the position's last mark-to-market) so every cash move is
/// auditable. Settlements/normal closes are skipped via `market_has_matching_trade`
/// (they are already booked), and `pending` rows are never booked (they may be
/// never-filled orders — booking them would fabricate P&L).
///
/// Resolution-time settlement recognition (2026-07-15, accrual accounting): tokens in
/// `redeemable_marks` belong to RESOLVED markets — the wallet still holds them but
/// their value is final ($1.00 winner / $0.00 loser). Waiting for on-chain redemption
/// to book the winner (while the loser's row is reconciled immediately) makes net P&L
/// dip negative for minutes-to-hours on every settled arb pair. Instead, book both
/// legs HERE at their resolved value with reason "Settlement (won/lost — pending
/// redemption)"; auto_settle's later redemption becomes a cash-only event.
///
/// **Caller contract — `live_token_ids` must come from a SUCCESSFUL positions
/// fetch.** An empty set is legitimate input (an account that genuinely holds
/// nothing still needs its stale rows cleaned, and the per-asset intl sweep
/// passes empty sets routinely), so no guard HERE can tell "asked and got none"
/// from "could not ask" — that distinction exists only at the fetch site. A
/// caller that substitutes an empty set on a fetch error turns a transient
/// timeout into the book-and-delete of every confirmed row in the table; the
/// Kalshi and Polymarket US dashboard syncs did exactly that via
/// `unwrap_or_default()` until v1.1.0. On any fetch failure, skip the sweep for
/// that pass — the intl chain-sync's long-standing rule.
pub async fn purge_stale_open_positions(
    pool: &SqlitePool,
    live_token_ids: &std::collections::HashSet<String>,
    // token_id → (resolved cur_price, on-chain size) for redeemable positions
    redeemable_marks: &std::collections::HashMap<String, (Decimal, Decimal)>,
    // Tokens whose resolution could not be determined THIS sweep. Left entirely
    // alone — not booked, not deleted — so the next pass can try again.
    //
    // The alternative is the silent-purge arm, which deletes a row that moved real
    // cash and books nothing. On 2026-08-31 that lost a winning $0.80 settlement
    // from the ledger while the wallet was correct. Retaining a row costs nothing
    // but a few minutes of a stale dashboard entry; deleting one costs the record
    // permanently.
    defer_tokens: &std::collections::HashSet<String>,
) -> usize {
    // A row may legitimately sit `status='pending'` for a SHORT time between the
    // strategy's INSERT and the Polymarket Data API indexing the resulting fill.
    // Purging inside that window causes a purge→re-adopt cycle that duplicates the
    // row, so pending rows are protected — but only transiently.
    //
    // Beyond the grace window a `pending` row whose token the Data API no longer
    // reports is an ORPHAN, not an in-flight order. The canonical case: an arb leg
    // that settled on-chain and was redeemed off-app via the Polymarket "Redeem"
    // button. After redemption the wallet holds 0 of the token, so it appears in
    // neither the live nor the redeemable on-chain sets, and the old pending-skip
    // made it immune to every purge path forever — inflating the portfolio value
    // by its phantom mark-to-market (observed: +$14.85 of redeemed ETH arb legs).
    const STALE_PENDING_GRACE_SECS: i64 = 3600; // 60 min ≫ indexer lag, ≪ orphan lifetime

    // Ghost rows are excluded at the query.
    //
    // This whole function reconciles the DB against the CHAIN: a row whose token
    // the wallet does not hold is treated as closed off-strategy and booked or
    // deleted. A simulated position holds nothing on-chain by definition, so
    // every ghost row looks exactly like an orphan — the sweep would book them as
    // real "ChainReconcile" trades at last mark, or delete them outright.
    //
    // That corrupts the very thing a customer evaluating DRADIS is looking at:
    // the simulated P&L they are using to decide whether to fund it. Worse in
    // combination with the ghost-mode incident (see `GLOBAL_SEMANTICS_KEYS`) —
    // a customer stuck in simulation would have watched their paper positions
    // silently convert into real-looking booked trades.
    //
    // `clear_live_open_positions` already preserves ghost rows for the same
    // reason; this path simply never got the same treatment.

    let rows: Vec<(i64, String, Option<String>, String, String, String, String, String, String, Option<String>, Option<String>, Option<String>)> = match sqlx::query_as(
        "SELECT id, token_id, status, ts, strategy, market, side, entry_price, shares, current_price, entry_fee, settled_shares FROM open_positions WHERE ghost_mode = 0"
    )
    .fetch_all(pool)
    .await {
        Ok(r)  => r,
        Err(e) => { error!("❌ DB purge_stale_open_positions fetch failed: {}", e); return 0; }
    };

    let now = Utc::now();
    let mut purged = 0usize;
    for (id, token_id, status, ts, strategy, market, side, entry_price, shares, current_price, entry_fee, settled_shares) in rows {
        // The size that actually settled, when the chain has already zeroed the row.
        //
        // `shares` is what the position holds NOW; for a settled position that is
        // zero, and every booking branch below guards on a positive quantity. The
        // drift corrector preserves the pre-zero count in `settled_shares` for
        // exactly this read.
        let shares = {
            let live = shares.parse::<Decimal>().unwrap_or(Decimal::ZERO);
            if live > Decimal::ZERO {
                shares
            } else {
                settled_shares.clone().unwrap_or(shares)
            }
        };
        // Dollars already paid to open this leg. Absent on rows written before
        // the column existed, and on venues that do not report a fee — both
        // degrade to the old gross-only behavior rather than inventing a cost.
        let entry_fee = entry_fee
            .as_deref()
            .and_then(|f| f.parse::<Decimal>().ok())
            .unwrap_or(Decimal::ZERO);
        // Still held on-chain (size > 0, not redeemable) — keep.
        if live_token_ids.contains(&token_id) {
            continue;
        }

        // Resolution unknown this pass — leave it entirely alone and retry later.
        if defer_tokens.contains(&token_id) {
            continue;
        }

        let status_str = status.as_deref().unwrap_or("confirmed");
        let is_pending = status_str == "pending";

        // ── Resolution-time settlement booking (redeemable tokens) ──────────────
        // The wallet still HOLDS this token but the market has resolved: its value
        // is final. Book the leg at exactly $1.00 (winner) or $0.00 (loser) now, so
        // net P&L is correct the moment the market resolves instead of after the
        // on-chain redemption lands. Applies to `pending` rows too — a redeemable
        // wallet holding proves the fill happened.
        if let Some((resolved_mark, chain_size)) = redeemable_marks.get(&token_id) {
            let entry = entry_price.parse::<Decimal>().unwrap_or(Decimal::ZERO);
            let row_qty = shares.parse::<Decimal>().unwrap_or(Decimal::ZERO);
            let qty = if *chain_size > Decimal::ZERO { *chain_size } else { row_qty };
            // Settlement pays exactly $1.00 or $0.00; cur_price on a redeemable
            // position is ~0.9995/~0.0005 — snap to the true payout.
            let resolved_px = if *resolved_mark >= Decimal::new(5, 1) { Decimal::ONE } else { Decimal::ZERO };
            let won = resolved_px == Decimal::ONE;

            if entry > Decimal::ZERO && qty > Decimal::ZERO {
                // Only an ArbitrageStrategy row can be a leftover pair leg, and the
                // check is side- and strategy-blind: without this gate a separate
                // single-leg position on the same market with a coincidentally equal
                // share count would be suppressed, silently dropping exactly the kind
                // of record this whole path exists to preserve.
                if strategy == "ArbitrageStrategy"
                    && market_has_settled_arb_pair(pool, &market, qty).await
                {
                    debug!(
                        "🧾 Resolution booking: {} {} {} sh already covered by a settled arb pair — skipping",
                        market, side, qty
                    );
                } else if market_has_settlement_trade(pool, &market, &side, qty).await {
                    debug!(
                        "🧾 Resolution booking: settlement already recorded for {} {} {} sh — skipping",
                        market, side, qty
                    );
                } else {
                    // Settlement pays out with NO exit fee — verified against
                    // collateral on 2026-08-13 (3.04 shares in the money paid
                    // exactly $3.0400). So the round trip owes the entry leg only.
                    let pnl = (resolved_px - entry) * qty - entry_fee;
                    // Wording follows the mechanics, keyed off `chain_size`:
                    // a positive size means the account still HOLDS the resolved
                    // token and its cash arrives with a later redemption (the
                    // intl accrual path). Size zero means the position is
                    // already gone — the venue settled it and the cash has
                    // landed (Kalshi pays winners straight to the balance,
                    // Polymarket US cash-settles custodially, Polymarket
                    // auto-redeems on-chain) — so "pending redemption" would
                    // describe an event that already happened. Both spellings
                    // stay under the `Settlement%` prefix the dedup checks key
                    // on.
                    let reason = format!(
                        "Settlement ({} — {})",
                        if won { "won" } else { "lost" },
                        if *chain_size > Decimal::ZERO { "pending redemption" } else { "cash settled" }
                    );
                    let inserted = record_settlement_trade_idempotent(
                        pool, &strategy, &market, &side, entry, resolved_px, qty, pnl, entry_fee, &reason, None,
                    ).await;
                    if inserted {
                        info!(
                            "🧾 Resolution booking: {} {} {} | {} sh entry=${:.4} → resolved ${:.2} → pnl=${:.4} (redemption pending)",
                            strategy, market, side, qty, entry, resolved_px, pnl
                        );
                    }
                }
            } else {
                warn!(
                    "🧾 Resolution booking: {} \"{}\" resolved but cost basis unknown \
                     (entry={} qty={}) — trade row omitted; redemption cash lands in collateral",
                    strategy, market, entry_price, shares
                );
            }

            // Row is resolved — always delete (never re-adopt a settled token).
            if let Err(e) = sqlx::query("DELETE FROM open_positions WHERE id = ?")
                .bind(id)
                .execute(pool)
                .await
            {
                error!("❌ DB purge_stale_open_positions delete failed for id {}: {}", id, e);
            } else {
                purged += 1;
            note_position_released(&token_id);
            }
            continue;
        }

        if is_pending {
            // Keep only if still inside the in-flight grace window. An unparseable
            // timestamp is treated as old (purge) so malformed rows can't leak forever.
            let age_secs = DateTime::parse_from_rfc3339(&ts)
                .map(|t| (now - t.with_timezone(&Utc)).num_seconds())
                .unwrap_or(i64::MAX);
            if age_secs < STALE_PENDING_GRACE_SECS {
                continue; // genuinely in-flight; leave alone
            }
        }

        // ── Ledger reconciliation for off-strategy exits ─────────────────────────
        // A `confirmed` position that vanished from the wallet with NO matching
        // ledger row closed outside the strategy's exit path. Book a best-effort
        // "ChainReconcile" trade (exit = last mark) so the balance move is auditable.
        // `pending` rows are skipped (possibly never-filled orders → would fabricate).
        if !is_pending {
            let entry = entry_price.parse::<Decimal>().unwrap_or(Decimal::ZERO);
            let qty   = shares.parse::<Decimal>().unwrap_or(Decimal::ZERO);
            let exit  = current_price.as_deref().and_then(|s| s.parse::<Decimal>().ok());
            match exit {
                Some(exit_px) if entry > Decimal::ZERO && qty > Decimal::ZERO && exit_px > Decimal::ZERO => {
                    if market_has_matching_trade(pool, &market, qty).await {
                        // Already booked (strategy close or settlement) — don't double-count.
                    } else {
                        // Position is a long outcome token: P&L = (exit − entry) × shares
                        // for either YES or NO side (both were bought at `entry`).
                        // Net of the entry leg's fee. The exit leg is unknown on
                        // this path by construction — the position left outside
                        // the strategy's exit, so there is no fill to price — and
                        // the reason string already marks the mark as estimated.
                        let pnl = (exit_px - entry) * qty - entry_fee;
                        let reason = format!(
                            "ChainReconcile: closed off-strategy (est. @ ${:.4} last mark)",
                            exit_px
                        );
                        // Reconciliation knows the book but not the market's class or
                        // underlying — leave those NULL rather than guessing.
                        let scope = TradeScope::new("", venue_for_pool(pool), None, None);
                        record_trade_db(pool, &scope, entry_fee, &strategy, &market, &side, entry, exit_px, qty, pnl, &reason, None).await;
                        info!(
                            "🧾 Ledger reconcile: booked off-strategy exit — {} {} {} | {} sh entry=${:.4} exit=${:.4} → pnl=${:.4}",
                            strategy, market, side, qty, entry, exit_px, pnl
                        );
                    }
                }
                _ => {
                    // No usable mark (missing/zero current_price) — cannot estimate P&L
                    // without fabricating. Purge silently; cash move stays in pnl_snapshots.
                    debug!(
                        "🧾 Ledger reconcile: skipped {} \"{}\" (no usable mark: entry={} shares={} cur={:?})",
                        strategy, market, entry_price, shares, current_price
                    );
                }
            }
        }

        // Delete this specific stale row by id (avoids touching a fresh pending row
        // that may share the same token_id).
        if let Err(e) = sqlx::query("DELETE FROM open_positions WHERE id = ?")
            .bind(id)
            .execute(pool)
            .await
        {
            error!("❌ DB purge_stale_open_positions delete failed for id {}: {}", id, e);
        } else {
            purged += 1;
            note_position_released(&token_id);
        }
    }
    purged
}

/// Update an existing open position's share count and avg_price from on-chain data.
///
/// Called by `sync_open_positions_with_chain` whenever the Polymarket Data API
/// reports a different share count than what is stored in the DB (e.g. after a
/// partial fill later completes, or when the initial adoption recorded a stale value).
/// Also stamps `chain_adopted = 1` so the UI shows the chain badge.
pub async fn update_position_from_chain(
    pool: &SqlitePool,
    token_id: &str,
    shares: rust_decimal::Decimal,
    avg_price: rust_decimal::Decimal,
    cur_price: Option<rust_decimal::Decimal>,
) {
    let cur_price_str = cur_price.map(|p| p.to_string());
    // The Polymarket Data API frequently reports avg_price = 0 for a position whose
    // cost basis it has not indexed yet (common in the seconds right after entry).
    // Never let a zero/negative chain avg_price clobber the real strategy entry price:
    // doing so destroys the cost basis and fabricates phantom unrealized P&L (e.g. a
    // genuine $0.55 entry overwritten to $0.00 then mark-to-markets as +100% "profit").
    // When avg_price is non-positive, correct shares + current_price ONLY and keep the
    // existing entry_price.
    // A chain read of ZERO is a SETTLEMENT, not a correction to nothing.
    //
    // The scaling expression below multiplies `entry_fee` by `new/old`, which for
    // a zero read is a multiply by zero — so the fee is destroyed alongside the
    // share count, and any later attempt to book the settlement has neither the
    // quantity nor the cost to book. Capture both before the write. `settled_shares`
    // is only ever set on the zero transition, so it always holds the size that
    // actually settled rather than the size of some earlier partial correction.
    if shares <= rust_decimal::Decimal::ZERO {
        if let Err(e) = sqlx::query(
            "UPDATE open_positions
                SET settled_shares = shares
              WHERE token_id = ? AND CAST(shares AS REAL) > 0"
        )
        .bind(token_id)
        .execute(pool)
        .await
        {
            // Do NOT proceed to zero the row. Zeroing without a captured size
            // recreates the exact incident state (shares=0, settled_shares NULL) and
            // the settlement booking is lost permanently. The corrector retries on
            // the next sweep; a row that is briefly one pass stale costs nothing.
            error!("❌ DB settled_shares capture failed for {} — skipping the zero write this pass: {}",
                   token_id, e);
            return;
        }
    }

    let result = if avg_price > rust_decimal::Decimal::ZERO {
        sqlx::query(
            "UPDATE open_positions SET entry_fee = CASE WHEN CAST(? AS REAL) <= 0 THEN entry_fee ELSE CAST(COALESCE(entry_fee,'0') AS REAL) * (CAST(? AS REAL) / NULLIF(CAST(shares AS REAL),0)) END, shares = ?, entry_price = ?, chain_adopted = 1, current_price = COALESCE(?, current_price), price_updated_at = CASE WHEN ? IS NULL THEN price_updated_at ELSE ? END WHERE token_id = ?"
        )
        .bind(shares.to_string())
        .bind(shares.to_string())
        .bind(shares.to_string())
        .bind(avg_price.to_string())
        .bind(&cur_price_str)
        // Stamp the refresh time only when a price actually came through, so a
        // shares-only correction cannot claim a freshness it did not deliver.
        .bind(&cur_price_str)
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(token_id)
        .execute(pool)
        .await
    } else {
        sqlx::query(
            "UPDATE open_positions SET entry_fee = CASE WHEN CAST(? AS REAL) <= 0 THEN entry_fee ELSE CAST(COALESCE(entry_fee,'0') AS REAL) * (CAST(? AS REAL) / NULLIF(CAST(shares AS REAL),0)) END, shares = ?, chain_adopted = 1, current_price = COALESCE(?, current_price), price_updated_at = CASE WHEN ? IS NULL THEN price_updated_at ELSE ? END WHERE token_id = ?"
        )
        .bind(shares.to_string())
        .bind(shares.to_string())
        .bind(shares.to_string())
        .bind(&cur_price_str)
        .bind(&cur_price_str)
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(token_id)
        .execute(pool)
        .await
    };
    if let Err(e) = result {
        error!("❌ DB update_position_from_chain failed for {}: {}", token_id, e);
    }
}

/// Update only the current_price for an existing open position (called on every chain-sync).
///
/// Also flips `status` to 'confirmed': a position the Data API reports as a live on-chain
/// holding is, by definition, confirmed (not an un-indexed in-flight order). Without this,
/// a row first written as 'pending' by the strategy order path could stay 'pending'
/// indefinitely after its fill, making it permanently immune to purge_stale_open_positions.
/// Refresh a position's mark price WITHOUT asserting anything about its status.
///
/// `update_position_current_price` also flips `status` to `confirmed`, which is
/// sound for its original caller: the chain-sync sweep only calls it for tokens
/// the Data API reports as live on-chain holdings, so confirmation is earned.
///
/// The live-quote endpoint has no such evidence. It knows only that the venue
/// publishes a book for the token, which is true of every unfilled order ever
/// placed. Routing it through the confirming variant flipped freshly inserted
/// `pending` rows to `confirmed` within one 4-second dashboard poll, defeating
/// the 3600s grace window `purge_stale_open_positions` gives pending rows
/// precisely because the Data API indexer lags fills. A confirmed row the
/// indexer has not caught up to yet is booked as "closed off-strategy" at its
/// mark and deleted — a fabricated exit for a position DRADIS still holds,
/// followed by a re-adoption from chain. Ordinary conditions trigger it: a new
/// position, the Trade Log open, and normal indexer lag.
///
/// So this variant touches only the mark, and only on rows already confirmed.
/// The COALESCE matters — rows predating the status column are NULL, not
/// 'confirmed'.
pub async fn refresh_position_mark(
    pool: &SqlitePool,
    token_id: &str,
    cur_price: rust_decimal::Decimal,
) {
    if let Err(e) = sqlx::query(
        "UPDATE open_positions SET current_price = ?, price_updated_at = ? \
         WHERE token_id = ? AND COALESCE(status, 'confirmed') = 'confirmed'"
    )
    .bind(cur_price.to_string())
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(token_id)
    .execute(pool)
    .await {
        error!("❌ DB refresh_position_mark failed for {}: {}", token_id, e);
    }
}

pub async fn update_position_current_price(
    pool: &SqlitePool,
    token_id: &str,
    cur_price: rust_decimal::Decimal,
) {
    if let Err(e) = sqlx::query(
        "UPDATE open_positions SET current_price = ?, price_updated_at = ?, status = 'confirmed' WHERE token_id = ?"
    )
    .bind(cur_price.to_string())
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(token_id)
    .execute(pool)
    .await {
        error!("❌ DB update_position_current_price failed for {}: {}", token_id, e);
    }
}

/// Re-adopt a single on-chain position that is missing from `open_positions`.
///
/// Uses `INSERT ... WHERE NOT EXISTS` so it is safe to call repeatedly — it is a
/// no-op if a row for `token_id` already exists.  Returns `true` if a row was
/// inserted.
pub async fn adopt_chain_position(
    pool: &SqlitePool,
    token_id: &str,
    market: &str,
    side: &str,
    avg_price: rust_decimal::Decimal,
    shares: rust_decimal::Decimal,
    cur_price: Option<rust_decimal::Decimal>,
) -> bool {
    let ts  = Utc::now().to_rfc3339();
    let sid = current_session_id();
    // Patch any existing row that still has the legacy '?' placeholder side value.
    // This handles rows written by older builds before the side bind was fixed.
    // Also mark the row as chain_adopted so the UI can display accordingly.
    let _ = sqlx::query(
        "UPDATE open_positions SET side = ?, chain_adopted = 1 WHERE token_id = ? AND side = '?'"
    )
    .bind(side)
    .bind(token_id)
    .execute(pool)
    .await;

    let cur_price_str = cur_price.map(|p| p.to_string());
    // A fresh adoption has no prior entry_price to preserve, but the Data API still
    // frequently reports avg_price = 0 (cost basis not yet indexed). Recording a 0
    // entry would fabricate phantom mark-to-market P&L, so fall back to the current
    // price (the best available cost-basis estimate) when avg_price is non-positive.
    let entry_price = if avg_price > rust_decimal::Decimal::ZERO {
        avg_price
    } else {
        cur_price.unwrap_or(avg_price)
    };
    // Resolve the ORIGINATING strategy from the entries log (written at order time).
    // Previously this hardcoded 'ArbitrageStrategy', which misattributed every
    // chain-adopted orphan — e.g. a residual MakerStrategy fill on an hourly market —
    // to Arbitrage. That corrupted P&L attribution and, worse, handed the position to
    // the arbitrage naked-leg manager (making it look like arb traded an hourly book it
    // never touched). Fall back to MomentumStrategy — the generic orphan owner matching
    // reconcile_orphaned_positions' adoption_order[0] — only when no entry log exists.
    let resolved_strategy = lookup_entry_db(pool, token_id)
        .await
        .map(|(_, s)| s)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "MomentumStrategy".to_string());
    match sqlx::query(
        "INSERT INTO open_positions
             (ts, session_id, strategy, token_id, market, side, entry_price, shares, ghost_mode, chain_adopted, current_price)
         SELECT ?, ?, ?, ?, ?, ?, ?, ?, 0, 1, ?
         WHERE NOT EXISTS (SELECT 1 FROM open_positions WHERE token_id = ?)"
    )
    .bind(&ts)
    .bind(sid)
    .bind(&resolved_strategy)
    .bind(token_id)
    .bind(market)
    .bind(side)
    .bind(entry_price.to_string())
    .bind(shares.to_string())
    .bind(&cur_price_str)
    .bind(token_id)
    .execute(pool)
    .await {
        Ok(r)  => r.rows_affected() > 0,
        Err(e) => { error!("❌ DB adopt_chain_position failed for {}: {}", token_id, e); false }
    }
}

// ─── API read models ─────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct PnlSnapshotRow {
    pub ts: String,
    pub session_pnl: String,
    pub collateral: String,
    pub total_value: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TradeRow {
    pub ts: String,
    pub strategy: String,
    pub market: String,
    pub side: String,
    pub entry_price: String,
    pub exit_price: String,
    pub shares: String,
    pub pnl: String,
    pub reason: String,
    /// Exchange that executed the trade. `None` on rows written before the
    /// column existed and whose shard had no registered venue.
    pub venue: Option<String>,
    /// `crypto` | `sports` | `politics` | `unknown`; `None` on legacy rows.
    pub market_class: Option<String>,
    /// Underlying symbol. `None` is meaningful — sports and politics markets
    /// have no underlying instrument.
    pub underlying: Option<String>,
    /// Was this a simulated fill? `false` on rows written before the column
    /// existed — see the migration note.
    pub ghost: bool,
    /// Total venue fees for the round trip. `pnl` is already net of this;
    /// `pnl + fees` recovers the gross figure. `None` on pre-fee rows.
    pub fees: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct OpenPositionRow {
    pub ts:             String,
    pub strategy:       String,
    pub token_id:       String,
    pub market:         String,
    pub side:           String,
    pub entry_price:    String,
    pub shares:         String,
    pub ghost_mode:     bool,
    pub chain_adopted:  bool,
    pub status:         String,
    /// Live mark-to-market price from Polymarket Data API; None until first chain-sync.
    pub current_price:  Option<String>,
    /// When `current_price` was last refreshed (RFC3339). `None` on rows written
    /// before the column existed, and on a brand-new position that has not yet
    /// been through a chain-sync sweep. The UI must show this: the price can be
    /// minutes old, and an operator timing a manual exit needs to know that.
    pub price_updated_at: Option<String>,
    /// Exchange that holds the position. `None` only on rows written before the
    /// column existed *and* before the shard's backfill ran — the tradelog falls
    /// back to the shard for those.
    pub venue: Option<String>,
    /// `crypto` | `sports` | `politics` | `unknown`; `None` on legacy rows and
    /// on reconciliation writes (chain adoption, orphan re-hedge) that know the
    /// book but not the market's class.
    pub market_class: Option<String>,
    /// Underlying symbol. `None` is meaningful — sports and politics markets
    /// have no underlying instrument.
    pub underlying: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ConfigHistoryRow {
    pub id: i64,
    pub ts: String,
    pub session_id: String,
    pub changed_by: String,
    pub param_name: String,
    pub old_value: Option<String>,
    pub new_value: String,
}

/// Return the most recent `limit` P&L snapshots, newest first.
/// Now also filters to only include data from the last 24 hours.
pub async fn get_pnl_history(pool: &SqlitePool, limit: i64) -> Vec<PnlSnapshotRow> {
    // Calculate timestamp for 24 hours ago
    let cutoff = Utc::now() - chrono::Duration::hours(24);
    let cutoff_str = cutoff.to_rfc3339();

    // Spread `limit` points across the whole 24 hours rather than returning the
    // newest `limit` rows.
    //
    // Snapshots land every few seconds, so a day is tens of thousands of rows.
    // Taking the newest 1000 covered under three hours of a 24-hour chart — and
    // silently, because the response looked like a complete history. The
    // portfolio chart also plots a marker only for trades falling BETWEEN its
    // oldest and newest snapshot, so every trade older than that window vanished
    // from the graph with no indication it had happened. On 2026-08-26 an
    // overnight AMI run showed a flat line and neither of its two trades: both
    // were three to six hours old against a window 2h40m wide.
    //
    // The stride is computed from the actual row count, so the window stays a
    // full day whatever the snapshot cadence happens to be.
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pnl_snapshots WHERE ts >= ?")
        .bind(&cutoff_str)
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    let stride = if total > limit && limit > 0 { (total + limit - 1) / limit } else { 1 };

    match sqlx::query(
        // `rn = 1` keeps the newest point whatever the stride, so the chart's
        // right-hand edge is always the live value rather than up to one stride
        // stale.
        "WITH windowed AS ( \
             SELECT ts, session_pnl, collateral, total_value, \
                    ROW_NUMBER() OVER (ORDER BY ts DESC) AS rn \
             FROM pnl_snapshots WHERE ts >= ? \
         ) \
         SELECT ts, session_pnl, collateral, total_value FROM windowed \
         WHERE rn = 1 OR rn % ? = 0 \
         ORDER BY ts DESC LIMIT ?"
    )
    .bind(&cutoff_str)
    .bind(stride)
    .bind(limit)
    .fetch_all(pool)
    .await {
        Ok(rows) => rows.into_iter().filter_map(|r| Some(PnlSnapshotRow {
            ts:          r.try_get::<String, _>(0).ok()?,
            session_pnl: r.try_get::<String, _>(1).ok()?,
            collateral:  r.try_get::<String, _>(2).ok()?,
            total_value: r.try_get::<String, _>(3).ok(),
        })).collect(),
        Err(e) => { error!("❌ DB get_pnl_history failed: {}", e); vec![] }
    }
}

/// Return true if a TrendReversal/TrendCapture stop-loss (or catastrophic) exit
/// was recorded on `market`+`side` within the last `within_secs` seconds.
///
/// Backs TrendReversal's PERSISTENT cascade guard. The strategy's in-memory
/// post-exit cooldown map is wiped on every redeploy/restart, which let a losing
/// fade re-fire repeatedly across restarts (2026-07-02 cascade). This DB-backed
/// check survives restarts. `reason` for SL exits contains "SL:"; catastrophic
/// exits contain "Catastrophic"; profit/reversal exits match neither.
pub async fn recent_stop_loss_exists(
    pool: &SqlitePool,
    market: &str,
    side: &str,
    within_secs: i64,
) -> bool {
    match sqlx::query(
        "SELECT COUNT(*) FROM trades
         WHERE strategy IN ('TrendReversalStrategy','TrendCaptureStrategy')
           AND market = ?
           AND side = ?
           AND (reason LIKE '%SL:%' OR reason LIKE '%Catastrophic%')
           AND (julianday('now') - julianday(ts)) * 86400.0 <= ?"
    )
    .bind(market)
    .bind(side)
    .bind(within_secs as f64)
    .fetch_one(pool)
    .await {
        Ok(row) => row.try_get::<i64, _>(0).map(|n| n > 0).unwrap_or(false),
        Err(e) => { error!("❌ DB recent_stop_loss_exists failed: {}", e); false }
    }
}

/// Return the most recent `limit` completed trades, newest first.
/// Lifetime aggregates over the whole `trades` table for one shard.
///
/// The dashboard's summary cards want totals over all history, but the trade
/// *list* they were computed from is a bounded recent window (`get_recent_trades`).
/// Deriving a "total" from that window silently truncates it: on 2026-08-15 the
/// squadron page summed 60 rows and the trade log 200, against 368 rows on the
/// btc shard, and the API clamps any limit to 500 regardless — so no client-side
/// number could have been made correct by raising it.
///
/// Aggregating in SQL keeps the card exact and O(1) in payload no matter how
/// long the history grows. `wins + losses` deliberately need not equal `count`:
/// exactly-zero P&L trades are neither, and collapsing them into one bucket or
/// the other would skew the win rate.
#[derive(Debug, Serialize)]
pub struct TradeStatsRow {
    pub count: i64,
    pub wins: i64,
    pub losses: i64,
    /// Summed as f64: `pnl` is stored as a decimal string, and dollar amounts at
    /// four decimal places stay far inside f64's ~15 significant digits even over
    /// a very long history.
    pub realized_pnl: f64,
    pub fees: f64,
    pub first_ts: Option<String>,
    pub last_ts: Option<String>,
}

pub async fn get_trade_stats(pool: &SqlitePool) -> TradeStatsRow {
    let empty = TradeStatsRow {
        count: 0, wins: 0, losses: 0, realized_pnl: 0.0, fees: 0.0,
        first_ts: None, last_ts: None,
    };
    match sqlx::query(
        "SELECT COUNT(*),
                COALESCE(SUM(CASE WHEN CAST(pnl AS REAL) > 0 THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN CAST(pnl AS REAL) < 0 THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CAST(pnl AS REAL)), 0.0),
                COALESCE(SUM(CAST(COALESCE(fees, '0') AS REAL)), 0.0),
                MIN(ts), MAX(ts)
         FROM trades"
    )
    .fetch_one(pool)
    .await {
        Ok(r) => TradeStatsRow {
            count:        r.try_get::<i64, _>(0).unwrap_or(0),
            wins:         r.try_get::<i64, _>(1).unwrap_or(0),
            losses:       r.try_get::<i64, _>(2).unwrap_or(0),
            realized_pnl: r.try_get::<f64, _>(3).unwrap_or(0.0),
            fees:         r.try_get::<f64, _>(4).unwrap_or(0.0),
            first_ts:     r.try_get::<Option<String>, _>(5).ok().flatten(),
            last_ts:      r.try_get::<Option<String>, _>(6).ok().flatten(),
        },
        Err(e) => {
            error!("❌ DB get_trade_stats failed: {}", e);
            empty
        }
    }
}

pub async fn get_recent_trades(pool: &SqlitePool, limit: i64) -> Vec<TradeRow> {
    match sqlx::query(
        "SELECT ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason,
                venue, market_class, underlying, fees, ghost
         FROM trades ORDER BY ts DESC LIMIT ?"
    )
    .bind(limit)
    .fetch_all(pool)
    .await {
        Ok(rows) => rows.into_iter().filter_map(|r| Some(TradeRow {
            ts:          r.try_get::<String, _>(0).ok()?,
            strategy:    r.try_get::<String, _>(1).ok()?,
            market:      r.try_get::<String, _>(2).ok()?,
            side:        r.try_get::<String, _>(3).ok()?,
            entry_price: r.try_get::<String, _>(4).ok()?,
            exit_price:  r.try_get::<String, _>(5).ok()?,
            shares:      r.try_get::<String, _>(6).ok()?,
            pnl:         r.try_get::<String, _>(7).ok()?,
            reason:      r.try_get::<String, _>(8).ok()?,
            venue:        r.try_get::<Option<String>, _>(9).ok().flatten(),
            market_class: r.try_get::<Option<String>, _>(10).ok().flatten(),
            underlying:   r.try_get::<Option<String>, _>(11).ok().flatten(),
            fees:         r.try_get::<Option<String>, _>(12).ok().flatten(),
            ghost:        r.try_get::<i64, _>(13).map(|v| v != 0).unwrap_or(false),
        })).collect(),
        Err(e) => { error!("❌ DB get_recent_trades failed: {}", e); vec![] }
    }
}

/// Every completed trade, oldest first — backs the tradelog CSV export
/// (tax reporting / offline review). No LIMIT: this table grows by a few
/// hundred rows a day at most, so a full scan stays trivially cheap.
pub async fn get_all_trades(pool: &SqlitePool) -> Vec<TradeRow> {
    match sqlx::query(
        "SELECT ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason,
                venue, market_class, underlying, fees, ghost
         FROM trades ORDER BY ts ASC"
    )
    .fetch_all(pool)
    .await {
        Ok(rows) => rows.into_iter().filter_map(|r| Some(TradeRow {
            ts:          r.try_get::<String, _>(0).ok()?,
            strategy:    r.try_get::<String, _>(1).ok()?,
            market:      r.try_get::<String, _>(2).ok()?,
            side:        r.try_get::<String, _>(3).ok()?,
            entry_price: r.try_get::<String, _>(4).ok()?,
            exit_price:  r.try_get::<String, _>(5).ok()?,
            shares:      r.try_get::<String, _>(6).ok()?,
            pnl:         r.try_get::<String, _>(7).ok()?,
            reason:      r.try_get::<String, _>(8).ok()?,
            venue:        r.try_get::<Option<String>, _>(9).ok().flatten(),
            market_class: r.try_get::<Option<String>, _>(10).ok().flatten(),
            underlying:   r.try_get::<Option<String>, _>(11).ok().flatten(),
            fees:         r.try_get::<Option<String>, _>(12).ok().flatten(),
            ghost:        r.try_get::<i64, _>(13).map(|v| v != 0).unwrap_or(false),
        })).collect(),
        Err(e) => { error!("❌ DB get_all_trades failed: {}", e); vec![] }
    }
}

/// Return all open positions across all sessions (inserted on entry, deleted on exit).
/// Rows are explicitly deleted when a position is closed, so every surviving row is
/// a live open position — even if a restart created a new session_id since entry.
/// Used by the API (/api/positions) and the LLM Advisor prompt.
pub async fn get_open_positions(pool: &SqlitePool) -> Vec<OpenPositionRow> {
    match sqlx::query(
        // Deduplicate by token_id: if multiple rows exist for the same token (due to a
        // chain-sync re-adoption race or a top-up INSERT that bypassed the NOT EXISTS guard),
        // keep only the most recent row (MAX(id)) so the UI and portfolio calculations see
        // exactly one entry per token — preventing phantom double-counting of positions.
        "SELECT ts, strategy, token_id, market, side, entry_price, shares, ghost_mode, chain_adopted,
         COALESCE(status, 'confirmed') as status, current_price, price_updated_at,
         venue, market_class, underlying
         FROM open_positions
         WHERE id IN (SELECT MAX(id) FROM open_positions GROUP BY token_id)
         ORDER BY ts ASC"
    )
    .fetch_all(pool)
    .await {
        Ok(rows) => rows.into_iter().filter_map(|r| Some(OpenPositionRow {
            ts:             r.try_get::<String, _>(0).ok()?,
            strategy:       r.try_get::<String, _>(1).ok()?,
            token_id:       r.try_get::<String, _>(2).ok()?,
            market:         r.try_get::<String, _>(3).ok()?,
            side:           r.try_get::<String, _>(4).ok()?,
            entry_price:    r.try_get::<String, _>(5).ok()?,
            shares:         r.try_get::<String, _>(6).ok()?,
            ghost_mode:     r.try_get::<i64, _>(7).ok()? != 0,
            chain_adopted:  r.try_get::<i64, _>(8).ok()? != 0,
            status:         r.try_get::<String, _>(9).ok()?,
            current_price:  r.try_get::<Option<String>, _>(10).ok().flatten(),
            price_updated_at: r.try_get::<Option<String>, _>(11).ok().flatten(),
            venue:          r.try_get::<Option<String>, _>(12).ok().flatten(),
            market_class:   r.try_get::<Option<String>, _>(13).ok().flatten(),
            underlying:     r.try_get::<Option<String>, _>(14).ok().flatten(),
        })).collect(),
        Err(e) => { error!("❌ DB get_open_positions failed: {}", e); vec![] }
    }
}

/// Return only pending positions (Viper Launches) - orders placed but not yet confirmed on-chain.
pub async fn get_pending_positions(pool: &SqlitePool) -> Vec<OpenPositionRow> {
    match sqlx::query(
        "SELECT ts, strategy, token_id, market, side, entry_price, shares, ghost_mode, chain_adopted, status, current_price, price_updated_at,
         venue, market_class, underlying
         FROM open_positions WHERE status = 'pending' ORDER BY ts ASC"
    )
    .fetch_all(pool)
    .await {
        Ok(rows) => rows.into_iter().filter_map(|r| Some(OpenPositionRow {
            ts:             r.try_get::<String, _>(0).ok()?,
            strategy:       r.try_get::<String, _>(1).ok()?,
            token_id:       r.try_get::<String, _>(2).ok()?,
            market:         r.try_get::<String, _>(3).ok()?,
            side:           r.try_get::<String, _>(4).ok()?,
            entry_price:    r.try_get::<String, _>(5).ok()?,
            shares:         r.try_get::<String, _>(6).ok()?,
            ghost_mode:     r.try_get::<i64, _>(7).ok()? != 0,
            chain_adopted:  r.try_get::<i64, _>(8).ok()? != 0,
            status:         r.try_get::<String, _>(9).ok()?,
            current_price:  r.try_get::<Option<String>, _>(10).ok().flatten(),
            price_updated_at: r.try_get::<Option<String>, _>(11).ok().flatten(),
            venue:          r.try_get::<Option<String>, _>(12).ok().flatten(),
            market_class:   r.try_get::<Option<String>, _>(13).ok().flatten(),
            underlying:     r.try_get::<Option<String>, _>(14).ok().flatten(),
        })).collect(),
        Err(e) => { error!("❌ DB get_pending_positions failed: {}", e); vec![] }
    }
}

/// Return only confirmed positions (Viper Missions In-Flight) - verified on-chain.
pub async fn get_confirmed_positions(pool: &SqlitePool) -> Vec<OpenPositionRow> {
    match sqlx::query(
        "SELECT ts, strategy, token_id, market, side, entry_price, shares, ghost_mode, chain_adopted,
         COALESCE(status, 'confirmed') as status, current_price, price_updated_at,
         venue, market_class, underlying
         FROM open_positions WHERE COALESCE(status, 'confirmed') = 'confirmed' ORDER BY ts ASC"
    )
    .fetch_all(pool)
    .await {
        Ok(rows) => rows.into_iter().filter_map(|r| Some(OpenPositionRow {
            ts:             r.try_get::<String, _>(0).ok()?,
            strategy:       r.try_get::<String, _>(1).ok()?,
            token_id:       r.try_get::<String, _>(2).ok()?,
            market:         r.try_get::<String, _>(3).ok()?,
            side:           r.try_get::<String, _>(4).ok()?,
            entry_price:    r.try_get::<String, _>(5).ok()?,
            shares:         r.try_get::<String, _>(6).ok()?,
            ghost_mode:     r.try_get::<i64, _>(7).ok()? != 0,
            chain_adopted:  r.try_get::<i64, _>(8).ok()? != 0,
            status:         r.try_get::<String, _>(9).ok()?,
            current_price:  r.try_get::<Option<String>, _>(10).ok().flatten(),
            price_updated_at: r.try_get::<Option<String>, _>(11).ok().flatten(),
            venue:          r.try_get::<Option<String>, _>(12).ok().flatten(),
            market_class:   r.try_get::<Option<String>, _>(13).ok().flatten(),
            underlying:     r.try_get::<Option<String>, _>(14).ok().flatten(),
        })).collect(),
        Err(e) => { error!("❌ DB get_confirmed_positions failed: {}", e); vec![] }
    }
}


/// Return all completed trades for the current session, newest first.
///
/// This is the primary query used by the LLM Advisor during a session:
/// analysis stays contextually coherent because all trades share the same
/// market conditions, config snapshot, and starting collateral.
pub async fn get_session_trades(pool: &SqlitePool) -> Vec<TradeRow> {
    let sid = current_session_id();
    match sqlx::query(
        "SELECT ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason,
                venue, market_class, underlying, fees, ghost
         FROM trades WHERE session_id = ? ORDER BY ts DESC"
    )
    .bind(sid)
    .fetch_all(pool)
    .await {
        Ok(rows) => rows.into_iter().filter_map(|r| Some(TradeRow {
            ts:          r.try_get::<String, _>(0).ok()?,
            strategy:    r.try_get::<String, _>(1).ok()?,
            market:      r.try_get::<String, _>(2).ok()?,
            side:        r.try_get::<String, _>(3).ok()?,
            entry_price: r.try_get::<String, _>(4).ok()?,
            exit_price:  r.try_get::<String, _>(5).ok()?,
            shares:      r.try_get::<String, _>(6).ok()?,
            pnl:         r.try_get::<String, _>(7).ok()?,
            reason:      r.try_get::<String, _>(8).ok()?,
            venue:        r.try_get::<Option<String>, _>(9).ok().flatten(),
            market_class: r.try_get::<Option<String>, _>(10).ok().flatten(),
            underlying:   r.try_get::<Option<String>, _>(11).ok().flatten(),
            fees:         r.try_get::<Option<String>, _>(12).ok().flatten(),
            ghost:        r.try_get::<i64, _>(13).map(|v| v != 0).unwrap_or(false),
        })).collect(),
        Err(e) => { error!("❌ DB get_session_trades failed: {}", e); vec![] }
    }
}

/// Return trades from the previous session (by trades.session_id, not current one),
/// newest first, up to `limit` rows.  Used as supplemental context when the current
/// session has too few trades for meaningful LLM analysis.
///
/// Includes trades with `session_id IS NULL` — these are rows written before the
/// session-tracking migration was applied.  They are definitionally not the current
/// session so it is safe to treat them as prior-session context.
pub async fn get_previous_session_trades(pool: &SqlitePool, limit: i64) -> Vec<TradeRow> {
    let sid = current_session_id();
    match sqlx::query(
        "SELECT ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason,
                venue, market_class, underlying, fees, ghost
         FROM trades
         WHERE (session_id IS NULL OR session_id != ?)
         ORDER BY ts DESC LIMIT ?"
    )
    .bind(sid)
    .bind(limit)
    .fetch_all(pool)
    .await {
        Ok(rows) => rows.into_iter().filter_map(|r| Some(TradeRow {
            ts:          r.try_get::<String, _>(0).ok()?,
            strategy:    r.try_get::<String, _>(1).ok()?,
            market:      r.try_get::<String, _>(2).ok()?,
            side:        r.try_get::<String, _>(3).ok()?,
            entry_price: r.try_get::<String, _>(4).ok()?,
            exit_price:  r.try_get::<String, _>(5).ok()?,
            shares:      r.try_get::<String, _>(6).ok()?,
            pnl:         r.try_get::<String, _>(7).ok()?,
            reason:      r.try_get::<String, _>(8).ok()?,
            venue:        r.try_get::<Option<String>, _>(9).ok().flatten(),
            market_class: r.try_get::<Option<String>, _>(10).ok().flatten(),
            underlying:   r.try_get::<Option<String>, _>(11).ok().flatten(),
            fees:         r.try_get::<Option<String>, _>(12).ok().flatten(),
            ghost:        r.try_get::<i64, _>(13).map(|v| v != 0).unwrap_or(false),
        })).collect(),
        Err(e) => { error!("❌ DB get_previous_session_trades failed: {}", e); vec![] }
    }
}

/// Return recent config history entries, newest first.
pub async fn get_config_history(pool: &SqlitePool, limit: i64) -> Vec<ConfigHistoryRow> {
    match sqlx::query(
        "SELECT id, ts, session_id, changed_by, param_name, old_value, new_value
         FROM config_history ORDER BY ts DESC LIMIT ?"
    )
    .bind(limit)
    .fetch_all(pool)
    .await {
        Ok(rows) => rows.into_iter().filter_map(|r| Some(ConfigHistoryRow {
            id:          r.try_get::<i64,    _>(0).ok()?,
            ts:          r.try_get::<String, _>(1).ok()?,
            session_id:  r.try_get::<String, _>(2).ok()?,
            changed_by:  r.try_get::<String, _>(3).ok()?,
            param_name:  r.try_get::<String, _>(4).ok()?,
            old_value:   r.try_get::<Option<String>, _>(5).ok()?,
            new_value:   r.try_get::<String, _>(6).ok()?,
        })).collect(),
        Err(e) => { error!("❌ DB get_config_history failed: {}", e); vec![] }
    }
}

// ─── LLM Recommendations ─────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct LlmRecommendationRow {
    pub id:          i64,
    pub ts:          String,
    pub session_id:  String,
    pub model:       String,
    pub trade_count: i64,
    pub session_pnl: String,
    pub analysis:    String,
    /// True if this recommendation was generated during the current process session.
    pub is_current_session: bool,
}

/// Persist a completed LLM Advisor analysis, tagged with the current session.
pub async fn record_llm_recommendation(
    pool: &SqlitePool,
    model: &str,
    trade_count: i64,
    session_pnl: Decimal,
    analysis: &str,
) {
    let ts = Utc::now().to_rfc3339();
    let sid = current_session_id();
    if let Err(e) = sqlx::query(
        "INSERT INTO llm_recommendations (ts, model, trade_count, session_pnl, analysis, session_id)
         VALUES (?, ?, ?, ?, ?, ?)"
    )
    .bind(&ts)
    .bind(model)
    .bind(trade_count)
    .bind(session_pnl.to_string())
    .bind(analysis)
    .bind(sid)
    .execute(pool)
    .await {
        error!("❌ DB llm_recommendation write failed: {}", e);
    }
}

/// Return the most recent `limit` LLM recommendations, newest first.
/// The `is_current_session` field is populated by comparing each row's session_id
/// to `db::current_session_id()`, so callers can render staleness indicators.
pub async fn get_recent_llm_recommendations(pool: &SqlitePool, limit: i64) -> Vec<LlmRecommendationRow> {
    let current_sid = current_session_id().to_string();
    match sqlx::query(
        "SELECT id, ts, COALESCE(session_id, 'legacy'), model, trade_count, session_pnl, analysis
         FROM llm_recommendations ORDER BY ts DESC LIMIT ?"
    )
    .bind(limit)
    .fetch_all(pool)
    .await {
        Ok(rows) => rows.into_iter().filter_map(|r| {
            let sid: String = r.try_get::<String, _>(2).ok()?;
            Some(LlmRecommendationRow {
                id:                  r.try_get::<i64,    _>(0).ok()?,
                ts:                  r.try_get::<String, _>(1).ok()?,
                session_id:          sid.clone(),
                model:               r.try_get::<String, _>(3).ok()?,
                trade_count:         r.try_get::<i64,    _>(4).ok()?,
                session_pnl:         r.try_get::<String, _>(5).ok()?,
                analysis:            r.try_get::<String, _>(6).ok()?,
                is_current_session:  sid == current_sid,
            })
        }).collect(),
        Err(e) => { error!("❌ DB get_recent_llm_recommendations failed: {}", e); vec![] }
    }
}

// ── llm_actions: LLM-authored config patch audit trail (Epic S2) ─────────────

/// One row of the `llm_actions` audit trail — a single proposed field change.
#[derive(Debug, Clone, Serialize)]
pub struct LlmActionRow {
    pub id: i64,
    pub batch_id: String,
    pub session_id: String,
    pub ts: String,
    pub expires_at: String,
    pub model: String,
    /// Autonomy tier active when proposed (1 = approval, 2 = limited, 3 = autonomous).
    pub tier: i64,
    pub ghost_mode: bool,
    pub field: String,
    pub from_value: String,
    pub to_value: String,
    pub clamped: bool,
    pub delta_pct: Option<f64>,
    pub reason: String,
    /// proposed | approved | applied | rejected | expired | reverted | failed
    pub status: String,
    pub status_detail: Option<String>,
    pub status_ts: Option<String>,
    /// Squadron this action targets. `None` for rows written before the
    /// advisor became squadron-scoped: those were applied to the global config,
    /// which no patrol loop reads, so they never moved a live parameter.
    pub squadron_id: Option<String>,
    /// JSON merge-patch restoring the pre-apply value (set when applied).
    pub inverse_patch: Option<String>,
    /// Session P&L (USDC) at apply time — circuit-breaker drawdown baseline.
    pub pnl_at_apply: Option<f64>,
    pub outcome_score: Option<f64>,
    pub outcome_detail: Option<String>,
}

/// Persist one advisory cycle's proposal batch: every accepted change lands as
/// `proposed`, every validation reject as `rejected` (with the reason) so the
/// few-shot corpus sees both. Returns the ids of the `proposed` rows.
pub async fn record_llm_action_batch(
    pool: &SqlitePool,
    batch_id: &str,
    model: &str,
    tier: i64,
    ghost_mode: bool,
    ttl_secs: i64,
    batch: &crate::helpers::llm_patch::ProposalBatch,
    // `squadron_id` is the squadron this batch was reasoned about and will be
    // applied to. The advisor runs one pass per squadron, so every row belongs
    // to exactly one.
    squadron_id: &str,
) -> Vec<i64> {
    let ts = Utc::now();
    let expires_at = (ts + chrono::Duration::seconds(ttl_secs)).to_rfc3339();
    let ts = ts.to_rfc3339();
    let sid = current_session_id();
    let mut ids = Vec::new();

    for c in &batch.accepted {
        match sqlx::query(
            "INSERT INTO llm_actions
               (batch_id, session_id, ts, expires_at, model, tier, ghost_mode,
                field, from_value, to_value, clamped, delta_pct, reason, status,
                squadron_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'proposed', ?)"
        )
        .bind(batch_id).bind(sid).bind(&ts).bind(&expires_at).bind(model)
        .bind(tier).bind(ghost_mode)
        .bind(&c.key)
        .bind(c.from.to_string())
        .bind(c.to.to_string())
        .bind(c.clamped)
        .bind(c.delta_pct)
        .bind(&c.reason)
        .bind(squadron_id)
        .execute(pool)
        .await {
            Ok(r) => ids.push(r.last_insert_rowid()),
            Err(e) => error!("❌ DB llm_actions insert failed for {}: {}", c.key, e),
        }
    }

    for r in &batch.rejected {
        if let Err(e) = sqlx::query(
            "INSERT INTO llm_actions
               (batch_id, session_id, ts, expires_at, model, tier, ghost_mode,
                field, from_value, to_value, reason, status, status_detail, status_ts,
                squadron_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', ?, '', 'rejected', ?, ?, ?)"
        )
        .bind(batch_id).bind(sid).bind(&ts).bind(&expires_at).bind(model)
        .bind(tier).bind(ghost_mode)
        .bind(&r.field)
        .bind(r.to.to_string())
        .bind(&r.why)
        .bind(&ts)
        .bind(squadron_id)
        .execute(pool)
        .await {
            error!("❌ DB llm_actions reject-insert failed for {}: {}", r.field, e);
        }
    }

    ids
}

fn llm_action_from_row(r: &sqlx::sqlite::SqliteRow) -> Option<LlmActionRow> {
    Some(LlmActionRow {
        id:            r.try_get("id").ok()?,
        batch_id:      r.try_get("batch_id").ok()?,
        squadron_id:   r.try_get("squadron_id").ok().flatten(),
        session_id:    r.try_get("session_id").ok()?,
        ts:            r.try_get("ts").ok()?,
        expires_at:    r.try_get("expires_at").ok()?,
        model:         r.try_get("model").ok()?,
        tier:          r.try_get("tier").ok()?,
        ghost_mode:    r.try_get::<i64, _>("ghost_mode").ok()? != 0,
        field:         r.try_get("field").ok()?,
        from_value:    r.try_get("from_value").ok()?,
        to_value:      r.try_get("to_value").ok()?,
        clamped:       r.try_get::<i64, _>("clamped").ok()? != 0,
        delta_pct:     r.try_get("delta_pct").ok(),
        reason:        r.try_get("reason").ok()?,
        status:        r.try_get("status").ok()?,
        status_detail: r.try_get("status_detail").ok(),
        status_ts:     r.try_get("status_ts").ok(),
        inverse_patch: r.try_get("inverse_patch").ok(),
        pnl_at_apply:  r.try_get("pnl_at_apply").ok(),
        outcome_score: r.try_get("outcome_score").ok(),
        outcome_detail: r.try_get("outcome_detail").ok(),
    })
}

/// Single action by rowid — the approval endpoints' lookup.
pub async fn fetch_llm_action_by_id(pool: &SqlitePool, id: i64) -> Option<LlmActionRow> {
    match sqlx::query("SELECT * FROM llm_actions WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await
    {
        Ok(row) => row.as_ref().and_then(llm_action_from_row),
        Err(e) => { error!("❌ DB fetch_llm_action_by_id({id}) failed: {e}"); None }
    }
}

/// Most recent actions, newest first — feeds the AI Actions view.
pub async fn fetch_llm_actions(pool: &SqlitePool, limit: i64) -> Vec<LlmActionRow> {
    match sqlx::query("SELECT * FROM llm_actions ORDER BY id DESC LIMIT ?")
        .bind(limit)
        .fetch_all(pool)
        .await
    {
        Ok(rows) => rows.iter().filter_map(llm_action_from_row).collect(),
        Err(e) => { error!("❌ DB fetch_llm_actions failed: {}", e); vec![] }
    }
}

/// Actions awaiting a decision (tier-1 approval queue): `proposed` and unexpired.
pub async fn fetch_pending_llm_actions(pool: &SqlitePool) -> Vec<LlmActionRow> {
    let now = Utc::now().to_rfc3339();
    match sqlx::query(
        "SELECT * FROM llm_actions WHERE status = 'proposed' AND expires_at > ? ORDER BY id"
    )
    .bind(&now)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows.iter().filter_map(llm_action_from_row).collect(),
        Err(e) => { error!("❌ DB fetch_pending_llm_actions failed: {}", e); vec![] }
    }
}

/// Advance an action's status (stamps status_ts; optional detail and inverse
/// patch — the inverse is recorded when the change is actually applied).
/// Returns true when a row was updated.
pub async fn update_llm_action_status(
    pool: &SqlitePool,
    id: i64,
    status: &str,
    detail: Option<&str>,
    inverse_patch: Option<&str>,
) -> bool {
    match sqlx::query(
        "UPDATE llm_actions
         SET status = ?, status_detail = ?, status_ts = ?,
             inverse_patch = COALESCE(?, inverse_patch)
         WHERE id = ?"
    )
    .bind(status)
    .bind(detail)
    .bind(Utc::now().to_rfc3339())
    .bind(inverse_patch)
    .bind(id)
    .execute(pool)
    .await
    {
        Ok(r) => r.rows_affected() > 0,
        Err(e) => { error!("❌ DB update_llm_action_status({id}→{status}) failed: {e}"); false }
    }
}

/// Expire stale `proposed` actions (market context has moved on). Returns the
/// number of rows expired. Called before serving the approval queue and at the
/// start of each advisory cycle.
pub async fn expire_stale_llm_actions(pool: &SqlitePool) -> i64 {
    let now = Utc::now().to_rfc3339();
    match sqlx::query(
        "UPDATE llm_actions
         SET status = 'expired', status_detail = 'TTL elapsed before approval', status_ts = ?
         WHERE status = 'proposed' AND expires_at <= ?"
    )
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await
    {
        Ok(r) => r.rows_affected() as i64,
        Err(e) => { error!("❌ DB expire_stale_llm_actions failed: {}", e); 0 }
    }
}

/// Mark an action applied: stamps status/inverse and the session-P&L baseline
/// used by the autonomy circuit breaker to measure post-apply drawdown.
pub async fn mark_llm_action_applied(
    pool: &SqlitePool,
    id: i64,
    detail: &str,
    inverse_patch: &str,
    pnl_at_apply: f64,
) -> bool {
    match sqlx::query(
        "UPDATE llm_actions
         SET status = 'applied', status_detail = ?, status_ts = ?,
             inverse_patch = ?, pnl_at_apply = ?
         WHERE id = ?"
    )
    .bind(detail)
    .bind(Utc::now().to_rfc3339())
    .bind(inverse_patch)
    .bind(pnl_at_apply)
    .bind(id)
    .execute(pool)
    .await
    {
        Ok(r) => r.rows_affected() > 0,
        Err(e) => { error!("❌ DB mark_llm_action_applied({id}) failed: {e}"); false }
    }
}

/// Number of distinct proposal batches applied since `since` (RFC 3339).
/// Backs the tier-2 rate limit (default: 1 batch per hour).
pub async fn count_llm_batches_applied_since(pool: &SqlitePool, since: &str) -> i64 {
    match sqlx::query(
        "SELECT COUNT(DISTINCT batch_id) AS n FROM llm_actions
         WHERE status = 'applied' AND status_ts >= ?"
    )
    .bind(since)
    .fetch_one(pool)
    .await
    {
        Ok(r) => r.try_get::<i64, _>("n").unwrap_or(0),
        Err(e) => { error!("❌ DB count_llm_batches_applied_since failed: {}", e); 0 }
    }
}

/// All actions still in `applied` status whose apply timestamp is at or after
/// `since` (RFC 3339), newest first — the circuit breaker's revert set.
pub async fn fetch_llm_actions_applied_since(pool: &SqlitePool, since: &str) -> Vec<LlmActionRow> {
    match sqlx::query(
        "SELECT * FROM llm_actions
         WHERE status = 'applied' AND status_ts >= ?
         ORDER BY id DESC"
    )
    .bind(since)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows.iter().filter_map(llm_action_from_row).collect(),
        Err(e) => { error!("❌ DB fetch_llm_actions_applied_since failed: {}", e); vec![] }
    }
}

/// Applied/reverted actions that are due for outcome scoring: they carry a
/// P&L baseline, have no score yet, and their apply/revert happened at or
/// before `before_ts` (the scoring horizon has elapsed).
pub async fn fetch_llm_actions_needing_outcome(pool: &SqlitePool, before_ts: &str) -> Vec<LlmActionRow> {
    match sqlx::query(
        "SELECT * FROM llm_actions
         WHERE status IN ('applied', 'reverted')
           AND pnl_at_apply IS NOT NULL
           AND outcome_score IS NULL
           AND status_ts <= ?
         ORDER BY id"
    )
    .bind(before_ts)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows.iter().filter_map(llm_action_from_row).collect(),
        Err(e) => { error!("❌ DB fetch_llm_actions_needing_outcome failed: {}", e); vec![] }
    }
}

/// Recent actions with a learnable outcome — operator rejections, breaker
/// reverts, and scored applies — newest first. Feeds the few-shot section of
/// the advisor prompt so the model learns from its own track record.
pub async fn fetch_llm_fewshot_examples(pool: &SqlitePool, limit: i64) -> Vec<LlmActionRow> {
    match sqlx::query(
        "SELECT * FROM llm_actions
         WHERE status IN ('rejected', 'reverted')
            OR outcome_score IS NOT NULL
         ORDER BY id DESC LIMIT ?"
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows.iter().filter_map(llm_action_from_row).collect(),
        Err(e) => { error!("❌ DB fetch_llm_fewshot_examples failed: {}", e); vec![] }
    }
}

/// Record the measured outcome of an applied action (few-shot corpus, S7).
pub async fn set_llm_action_outcome(
    pool: &SqlitePool,
    id: i64,
    score: f64,
    detail: &str,
) -> bool {
    match sqlx::query("UPDATE llm_actions SET outcome_score = ?, outcome_detail = ? WHERE id = ?")
        .bind(score)
        .bind(detail)
        .bind(id)
        .execute(pool)
        .await
    {
        Ok(r) => r.rows_affected() > 0,
        Err(e) => { error!("❌ DB set_llm_action_outcome failed: {}", e); false }
    }
}


#[cfg(test)]
mod reconcile_tests {
    use super::*;
    use std::collections::HashSet;

    /// A brand-new database must be able to queue a deployment.
    ///
    /// `deployment_queue` gained a `name` column via ALTER, but that ALTER sits
    /// ~170 lines ABOVE the table's own CREATE inside `init_schema`. On a fresh
    /// database the ALTER runs against a table that does not exist yet, fails,
    /// is swallowed by `let _ =`, and then CREATE builds the table without the
    /// column. Every auto-deploy then fails forever with "table
    /// deployment_queue has no column named name", retried every few seconds.
    ///
    /// Asserting the INSERT rather than the column list, because the INSERT is
    /// what actually breaks and it fails the same way whichever mechanism is
    /// meant to supply the column.
    #[tokio::test]
    async fn a_fresh_database_can_queue_a_deployment() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        init_schema(&pool).await.unwrap();
        run_migrations(&pool).await;

        sqlx::query(
            "INSERT INTO deployment_queue (id, market_id, market_type, raptors, vipers, name) \
             VALUES ('t','0xabc','politics','[]','[]','')"
        )
        .execute(&pool)
        .await
        .expect("a fresh schema must accept a deployment row, name column included");
    }

    /// A database created before the filing columns existed must gain them, and
    /// its legacy rows must be stamped with the shard's venue while keeping
    /// `market_class` / `underlying` NULL rather than guessed.
    #[tokio::test]
    async fn legacy_db_gains_filing_columns_and_backfills_venue() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        // Pre-migration shape: no venue / market_class / underlying.
        sqlx::query(
            "CREATE TABLE trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, strategy TEXT NOT NULL,
                market TEXT NOT NULL, side TEXT NOT NULL, entry_price TEXT NOT NULL,
                exit_price TEXT NOT NULL, shares TEXT NOT NULL, pnl TEXT NOT NULL,
                reason TEXT NOT NULL)"
        ).execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO trades (ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason)
             VALUES ('2026-08-10T00:00:00Z','FairValueStrategy','BTC $65k','NO','0.33','0.27','8.93','-0.54','SL')"
        ).execute(&pool).await.unwrap();

        init_schema(&pool).await.unwrap();
        run_migrations(&pool).await;
        backfill_venue(&pool, "kalshi").await;

        let row = sqlx::query("SELECT venue, market_class, underlying FROM trades")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(row.try_get::<Option<String>, _>(0).unwrap(), Some("kalshi".to_string()));
        assert_eq!(row.try_get::<Option<String>, _>(1).unwrap(), None, "class must not be guessed");
        assert_eq!(row.try_get::<Option<String>, _>(2).unwrap(), None, "underlying must not be guessed");
    }

    /// A market with no underlying instrument (sports) records NULL, not a
    /// placeholder symbol — the case the old single "asset" field could not express.
    #[tokio::test]
    async fn sports_trade_records_null_underlying() {
        let pool = mem_pool().await;
        let scope = TradeScope::new("us", "polymarket-us", Some("sports".into()), None);
        record_trade_db(&pool, &scope, Decimal::ZERO, "MakerStrategy", "Chiefs vs Bills", "YES",
            dec_of("0.50"), dec_of("0.60"), dec_of("10"), dec_of("1.0"), "TP", None).await;

        let row = sqlx::query("SELECT venue, market_class, underlying FROM trades")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(row.try_get::<Option<String>, _>(0).unwrap(), Some("polymarket-us".to_string()));
        assert_eq!(row.try_get::<Option<String>, _>(1).unwrap(), Some("sports".to_string()));
        assert_eq!(row.try_get::<Option<String>, _>(2).unwrap(), None);
    }

    /// Two underlyings sharing one shard stay distinguishable — the Kalshi case
    /// where btc-open and eth-open both write to the `kalshi` database.
    #[tokio::test]
    async fn shared_shard_keeps_underlyings_distinct() {
        let pool = mem_pool().await;
        for u in ["btc", "eth"] {
            let scope = TradeScope::crypto("kalshi", "kalshi", u);
            record_trade_db(&pool, &scope, Decimal::ZERO, "FairValueStrategy", &format!("{u} market"), "NO",
                dec_of("0.33"), dec_of("0.40"), dec_of("5"), dec_of("0.35"), "TP", None).await;
        }
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT underlying FROM trades ORDER BY underlying"
        ).fetch_all(&pool).await.unwrap();
        assert_eq!(rows, vec!["btc".to_string(), "eth".to_string()]);
    }

    /// An in-flight row must file under the same dimensions as the completed
    /// trade it will become. `open_positions` was missed when the filing
    /// columns landed on `trades` / `entries`, so the tradelog showed a
    /// completed row with a venue directly above an open row rendering "—" —
    /// two rows for the same strategy on the same market, filed differently.
    /// Asserted through `get_open_positions` because that reader is exactly
    /// what the API hands the Control Tower.
    #[tokio::test]
    async fn open_position_rows_carry_the_same_filing_columns_as_trades() {
        let pool = mem_pool().await;
        let scope = TradeScope::crypto("kalshi", "kalshi", "btc");
        record_open_position_with_status(&pool, &scope, "btc-open", "FairValueStrategy",
            "KXBTCD-XYZ", "BTC above $64k", "YES", dec_of("0.42"), dec_of("10"), false, "pending").await;

        let rows = get_open_positions(&pool).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].venue.as_deref(), Some("kalshi"));
        assert_eq!(rows[0].market_class.as_deref(), Some("crypto"));
        assert_eq!(rows[0].underlying.as_deref(), Some("btc"));
    }

    /// Reconciliation paths (chain adoption, orphan re-hedge) know which
    /// venue's book they are reading but not the market's class or underlying.
    /// They must file the venue — that is the column the tradelog renders as
    /// "—" today — while leaving the other two honestly NULL, never guessed.
    #[tokio::test]
    async fn adoption_writes_file_the_venue_without_guessing_class_or_underlying() {
        let pool = mem_pool().await;
        let scope = TradeScope::new("", "polymarket-us", None, None);
        record_open_position(&pool, &scope, "us-open", "ChainAdopted",
            "tok-adopted", "tok-adopted", "YES", dec_of("0.61"), dec_of("5"), false).await;

        let rows = get_open_positions(&pool).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].venue.as_deref(), Some("polymarket-us"));
        assert_eq!(rows[0].market_class, None, "class must not be guessed");
        assert_eq!(rows[0].underlying, None, "underlying must not be guessed");
    }

    /// A database created before the filing columns reached `open_positions`
    /// must gain them and have its surviving open rows stamped with the shard's
    /// venue — same contract `legacy_db_gains_filing_columns_and_backfills_venue`
    /// pins for `trades`. Open rows outlive deploys (they are deleted on exit,
    /// not superseded), so a legacy row really can still be alive when the
    /// migrated build first reads it.
    #[tokio::test]
    async fn legacy_open_positions_gain_filing_columns_and_backfill_venue() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        // Pre-migration shape: no venue / market_class / underlying.
        sqlx::query(
            "CREATE TABLE open_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, session_id TEXT NOT NULL,
                strategy TEXT NOT NULL, token_id TEXT NOT NULL, market TEXT NOT NULL,
                side TEXT NOT NULL, entry_price TEXT NOT NULL, shares TEXT NOT NULL,
                ghost_mode INTEGER NOT NULL DEFAULT 0, chain_adopted INTEGER NOT NULL DEFAULT 0)"
        ).execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO open_positions (ts, session_id, strategy, token_id, market, side, entry_price, shares)
             VALUES ('2026-08-09T00:00:00Z','s1','MakerStrategy','tok-legacy','BTC hourly','YES','0.40','12')"
        ).execute(&pool).await.unwrap();

        init_schema(&pool).await.unwrap();
        run_migrations(&pool).await;
        backfill_venue(&pool, "polymarket-intl").await;

        let rows = get_open_positions(&pool).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].venue.as_deref(), Some("polymarket-intl"));
        assert_eq!(rows[0].market_class, None, "class must not be guessed");
        assert_eq!(rows[0].underlying, None, "underlying must not be guessed");
    }

    /// Recorded P&L must be net of fees, with the gross figure recoverable.
    ///
    /// Reproduces the 2026-08-10 Kalshi round trip that exposed the gap: YES
    /// @ $0.36 → $0.28 on 8.19 contracts, $0.1318 entry fee + $0.1154 exit fee.
    /// Booked gross it reads −$0.6552; the collateral actually moved −$0.9024.
    #[tokio::test]
    async fn recorded_pnl_is_net_of_fees() {
        let pool = mem_pool().await;
        let scope = TradeScope::crypto("kalshi", "kalshi", "btc");
        let shares = dec_of("8.19");
        let fees = dec_of("0.1318") + dec_of("0.1154");
        let gross = (dec_of("0.28") - dec_of("0.36")) * shares;
        record_trade_db(&pool, &scope, fees, "FairValueStrategy", "BTC $63.9k", "YES",
            dec_of("0.36"), dec_of("0.28"), shares, gross - fees, "CatastrophicSL", None).await;

        let row = sqlx::query("SELECT pnl, fees FROM trades").fetch_one(&pool).await.unwrap();
        let pnl: Decimal = row.try_get::<String, _>(0).unwrap().parse().unwrap();
        let booked_fees: Decimal = row.try_get::<String, _>(1).unwrap().parse().unwrap();

        assert_eq!(booked_fees, fees);
        assert_eq!(pnl, dec_of("-0.9024"), "net P&L must match the real collateral move");
        assert_eq!(pnl + booked_fees, gross, "gross must be recoverable from pnl + fees");
        assert!(pnl < gross, "fees must make the loss larger, never smaller");
    }

    /// A position that leaves via settlement must still owe its entry fee.
    ///
    /// Reproduces 2026-08-13 trade 356: FairValue bought 3.04 shares at $0.75
    /// and the market settled in the money. Collateral moved 65.573144 →
    /// 63.253244 → 66.293244, i.e. −$2.3199 out (2.28 notional + $0.0399 taker
    /// fee) and exactly +$3.0400 back — settlement pays $1.00/share and charges
    /// nothing. True profit +$0.7201. It booked +$0.7585, because the
    /// settlement path had no way to see the entry fee.
    #[tokio::test]
    async fn settlement_booking_is_net_of_the_entry_fee() {
        let pool = mem_pool().await;
        let shares = dec_of("3.04");
        let entry = dec_of("0.75");
        let entry_fee = dec_of("0.0399"); // 0.07 · p · (1−p) · shares
        let gross = (Decimal::ONE - entry) * shares;

        record_open_position(&pool, &TradeScope::shard_only("test"), "test-squadron", "FairValueStrategy", "tok-356",
            "Bitcoin Up or Down - August 13, 2PM ET", "YES", entry, shares, false).await;
        set_open_position_entry_fee(&pool, "tok-356", entry_fee).await;

        let stored: Option<String> = sqlx::query_scalar(
            "SELECT entry_fee FROM open_positions WHERE token_id = ?"
        ).bind("tok-356").fetch_one(&pool).await.unwrap();
        let stored: Decimal = stored.expect("entry_fee stored").parse().unwrap();
        assert_eq!(stored, entry_fee);

        // What the settlement path books once it can read the fee back.
        let pnl = gross - stored;
        assert_eq!(pnl, dec_of("0.7201"), "must match the real collateral move");
        assert!(pnl < gross, "the entry leg was not free");
        assert_eq!(gross - pnl, entry_fee, "gross must stay recoverable");
    }

    /// The stored entry fee is denominated in dollars for a specific fill size,
    /// so a chain-sync share correction has to carry it along. Trade 356 filled
    /// 3.04 of the 3.6363 shares requested; leaving the fee unscaled would
    /// describe a fill that never happened.
    #[tokio::test]
    async fn entry_fee_follows_a_chain_sync_share_correction() {
        let pool = mem_pool().await;
        let requested = dec_of("3.6363636363636363636363636364");
        let filled = dec_of("3.04");
        let fee_at_request = dec_of("0.07") * dec_of("0.75") * dec_of("0.25") * requested;

        record_open_position(&pool, &TradeScope::shard_only("test"), "test-squadron", "FairValueStrategy", "tok-sync", "mkt", "YES",
            dec_of("0.75"), requested, false).await;
        set_open_position_entry_fee(&pool, "tok-sync", fee_at_request).await;

        update_position_from_chain(&pool, "tok-sync", filled, Decimal::ZERO, None).await;

        let (shares, fee): (String, Option<String>) = sqlx::query_as(
            "SELECT shares, entry_fee FROM open_positions WHERE token_id = ?"
        ).bind("tok-sync").fetch_one(&pool).await.unwrap();
        assert_eq!(shares.parse::<Decimal>().unwrap(), filled);

        let fee: f64 = fee.expect("entry_fee retained").parse().unwrap();
        let expected = 0.07 * 0.75 * 0.25 * 3.04;
        assert!((fee - expected).abs() < 1e-6,
            "fee should rescale to the filled size: got {fee}, expected {expected}");
    }

    fn dec_of(s: &str) -> Decimal { s.parse().unwrap() }

    async fn mem_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        init_schema(&pool).await.expect("init schema");
        run_migrations(&pool).await;
        pool
    }

    /// A settled position whose row the chain corrector already zeroed must still
    /// book, using the size preserved in `settled_shares`.
    ///
    /// Replays the v1.0.9 production incident of 2026-08-31. FairValue bought
    /// 4.050628 shares at $0.79; the market resolved in its favor and the shares
    /// auto-redeemed at $1.00 for +$0.80, which the wallet showed and the ledger did
    /// not. The drift corrector ran first and wrote `shares = 0`, so the booking
    /// branch failed its own `qty > 0` guard and the row was deleted silently.
    #[tokio::test]
    async fn a_settled_position_books_from_the_preserved_share_count() {
        let pool = mem_pool().await;
        insert_open(&pool, "FairValueStrategy", "tok-settled", "Bitcoin Up or Down - 9PM",
                    "YES", "0.79", "4.050628", Some("0.99"), "confirmed").await;
        // What the drift corrector does when the chain reports the position gone.
        sqlx::query("UPDATE open_positions SET settled_shares = shares, shares = '0' WHERE token_id = 'tok-settled'")
            .execute(&pool).await.unwrap();

        // Gamma priced the market at $1.00; size 0 makes the branch use the row qty.
        let mut marks = std::collections::HashMap::new();
        marks.insert("tok-settled".to_string(), (Decimal::ONE, Decimal::ZERO));

        let purged = purge_stale_open_positions(&pool, &HashSet::new(), &marks, &HashSet::new()).await;
        assert_eq!(purged, 1, "the row is booked and then removed");

        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT side, reason, pnl FROM trades WHERE market = 'Bitcoin Up or Down - 9PM'"
        ).fetch_all(&pool).await.unwrap();
        assert_eq!(rows.len(), 1, "the settlement must be booked, not silently purged");
        assert!(rows[0].1.starts_with("Settlement"), "reason was {:?}", rows[0].1);
        // (1.00 − 0.79) × 4.050628 = 0.85063…, before the entry fee.
        let pnl: f64 = rows[0].2.parse().unwrap();
        assert!((0.80..0.86).contains(&pnl), "pnl {pnl} should be the real +$0.80-ish win");
    }

    /// A leftover arb leg must not book a second time against the combined pair row.
    ///
    /// `record_settled_arb_trade` books a resolved YES+NO pair as ONE row (side YES,
    /// shares = pairs), and the pair's economics are already netted in it. The
    /// side-scoped settlement dedup cannot see that row from the NO leg, so routing a
    /// leftover leg through the settlement branch would fabricate an extra loss of
    /// `entry × qty`. Leftover legs are reachable in ordinary operation: a crash
    /// between the redeem transaction and `purge_settled_legs`, or the operator
    /// redeeming in the Polymarket UI.
    #[tokio::test]
    async fn a_leftover_arb_leg_does_not_double_book_against_the_pair_row() {
        let pool = mem_pool().await;
        // The combined pair row, as record_settled_arb_trade writes it.
        record_trade_db(
            &pool, &TradeScope::new("", "polymarket-intl", None, None), Decimal::ZERO,
            "ArbitrageStrategy", "MarketArb", "YES",
            Decimal::new(99, 2), Decimal::ONE, Decimal::new(10, 0),
            Decimal::new(10, 2), "Settlement (YES+NO → $1.00)", None,
        ).await;
        // The NO leg that purge_settled_legs never got to.
        insert_open(&pool, "ArbitrageStrategy", "tok-no-leg", "MarketArb", "NO",
                    "0.09", "10", Some("0.00"), "confirmed").await;

        let mut marks = std::collections::HashMap::new();
        marks.insert("tok-no-leg".to_string(), (Decimal::ZERO, Decimal::ZERO));

        let purged = purge_stale_open_positions(&pool, &HashSet::new(), &marks, &HashSet::new()).await;
        assert_eq!(purged, 1, "the leftover row is still cleaned up");

        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM trades WHERE market = 'MarketArb'")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(n, 1, "only the original combined pair row may exist — no fabricated second loss");
    }

    /// The OTHER combined-pair writer must dedup too.
    ///
    /// `detect_orphaned_arb_settlements` books a redeemed pair as one row with reason
    /// `Settlement (auto-redeemed by Polymarket)`, and its own row cleanup is a
    /// `let _ = DELETE` — so a busy database leaves the leg rows behind for this
    /// sweep to find. Matching only the other reason left the NO-leg double-book
    /// alive through that path.
    #[tokio::test]
    async fn an_auto_redeemed_pair_also_suppresses_the_leftover_leg() {
        let pool = mem_pool().await;
        record_trade_db(
            &pool, &TradeScope::new("", "polymarket-intl", None, None), Decimal::ZERO,
            "ArbitrageStrategy", "MarketAuto", "YES",
            Decimal::new(98, 2), Decimal::ONE, Decimal::new(12, 0),
            Decimal::new(24, 2), "Settlement (auto-redeemed by Polymarket)", None,
        ).await;
        insert_open(&pool, "ArbitrageStrategy", "tok-auto-no", "MarketAuto", "NO",
                    "0.10", "12", Some("0.00"), "confirmed").await;

        let mut marks = std::collections::HashMap::new();
        marks.insert("tok-auto-no".to_string(), (Decimal::ZERO, Decimal::ZERO));
        assert_eq!(purge_stale_open_positions(&pool, &HashSet::new(), &marks, &HashSet::new()).await, 1);

        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM trades WHERE market = 'MarketAuto'")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(n, 1, "no fabricated second loss against the auto-redeemed pair row");
    }

    /// The pair check must not suppress a NON-arb position that merely shares a
    /// market and a share count. Dropping that record is the very failure this
    /// patch exists to prevent.
    #[tokio::test]
    async fn a_non_arb_position_still_books_beside_a_settled_pair() {
        let pool = mem_pool().await;
        record_trade_db(
            &pool, &TradeScope::new("", "polymarket-intl", None, None), Decimal::ZERO,
            "ArbitrageStrategy", "MarketBoth", "YES",
            Decimal::new(97, 2), Decimal::ONE, Decimal::new(8, 0),
            Decimal::new(24, 2), "Settlement (YES+NO → $1.00)", None,
        ).await;
        // Same market and size, different strategy and side — a genuinely separate
        // position. The side differs deliberately: the PRE-EXISTING side-scoped
        // dedup (`market_has_settlement_trade`) also collides on market+side+shares,
        // which is a known limitation this patch does not change. Using the other
        // side isolates the behavior actually under test — that the side-blind PAIR
        // check no longer suppresses a non-arb row.
        insert_open(&pool, "FairValueStrategy", "tok-fv", "MarketBoth", "NO",
                    "0.55", "8", Some("0.01"), "confirmed").await;

        let mut marks = std::collections::HashMap::new();
        marks.insert("tok-fv".to_string(), (Decimal::ZERO, Decimal::ZERO));
        assert_eq!(purge_stale_open_positions(&pool, &HashSet::new(), &marks, &HashSet::new()).await, 1);

        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM trades WHERE market = 'MarketBoth' AND strategy = 'FairValueStrategy'"
        ).fetch_one(&pool).await.unwrap();
        assert_eq!(n, 1, "the FairValue settlement must be booked, not swallowed by the arb dedup");
    }

    /// A token whose resolution could not be determined is left completely alone —
    /// not booked, not deleted — so the next sweep can try again.
    ///
    /// Deleting it is what lost the record in the first place. Retaining a row costs
    /// a stale dashboard entry for a few minutes; deleting one costs it permanently.
    #[tokio::test]
    async fn a_deferred_token_is_neither_booked_nor_deleted() {
        let pool = mem_pool().await;
        insert_open(&pool, "FairValueStrategy", "tok-unknown", "MarketU", "YES",
                    "0.50", "10", Some("0.50"), "confirmed").await;

        let mut defer = HashSet::new();
        defer.insert("tok-unknown".to_string());

        let purged = purge_stale_open_positions(
            &pool, &HashSet::new(), &std::collections::HashMap::new(), &defer,
        ).await;
        assert_eq!(purged, 0, "a deferred row must survive the sweep");

        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM open_positions WHERE token_id = 'tok-unknown'")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(n, 1, "the row is still there for the next pass");
        let t: i64 = sqlx::query_scalar("SELECT count(*) FROM trades WHERE market = 'MarketU'")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(t, 0, "and nothing was invented for it");
    }

    /// A losing settlement books too, at exactly $0.00.
    #[tokio::test]
    async fn a_losing_settlement_books_at_zero() {
        let pool = mem_pool().await;
        insert_open(&pool, "FairValueStrategy", "tok-lost", "MarketL", "NO",
                    "0.60", "5", Some("0.01"), "confirmed").await;
        sqlx::query("UPDATE open_positions SET settled_shares = shares, shares = '0' WHERE token_id = 'tok-lost'")
            .execute(&pool).await.unwrap();

        let mut marks = std::collections::HashMap::new();
        marks.insert("tok-lost".to_string(), (Decimal::ZERO, Decimal::ZERO));

        assert_eq!(purge_stale_open_positions(&pool, &HashSet::new(), &marks, &HashSet::new()).await, 1);
        let pnl: String = sqlx::query_scalar("SELECT pnl FROM trades WHERE market = 'MarketL'")
            .fetch_one(&pool).await.unwrap();
        // (0.00 − 0.60) × 5 = −3.00
        assert!(pnl.starts_with('-'), "a lost settlement must book a loss, got {pnl}");
    }

    /// Insert a SIMULATED position belonging to a named session.
    async fn insert_ghost_open(pool: &SqlitePool, session: &str, token: &str) {
        sqlx::query(
            "INSERT INTO open_positions
             (ts, session_id, strategy, token_id, market, side, entry_price, shares, ghost_mode, chain_adopted, status, current_price)
             VALUES (?, ?, 'FairValueStrategy', ?, 'Bitcoin Up or Down', 'YES', '0.32', '11.36', 1, 0, 'confirmed', NULL)"
        )
        .bind(Utc::now().to_rfc3339())
        .bind(session).bind(token)
        .execute(pool).await.expect("insert ghost open_position");
    }

    async fn open_tokens(pool: &SqlitePool) -> Vec<String> {
        sqlx::query_scalar::<_, String>("SELECT token_id FROM open_positions ORDER BY token_id")
            .fetch_all(pool).await.expect("select")
    }

    /// A simulated position left by an earlier session must not survive a restart.
    ///
    /// Replays the v1.0.9 production row of 2026-08-31: a paper FairValue position
    /// opened at 02:41 was still an open row nine hours and five rotations later,
    /// because nothing rehydrates ghost rows into the in-memory map after a restart
    /// and every other sweep excludes them by design. It had no viper that could
    /// ever exit it.
    #[tokio::test]
    async fn a_ghost_row_from_a_previous_session_is_closed_at_startup() {
        let pool = mem_pool().await;
        insert_ghost_open(&pool, "session-A", "tok-old").await;
        insert_ghost_open(&pool, "session-B", "tok-current").await;

        let closed = close_stale_ghost_positions(&pool, "session-B").await;
        assert_eq!(closed, 1, "exactly the foreign-session row should go");
        assert_eq!(open_tokens(&pool).await, vec!["tok-current".to_string()],
                   "the running session's own paper position must survive");
    }

    /// The sweep is scoped to simulation. A REAL position from an earlier session
    /// is a real holding on chain and must never be deleted by this path.
    #[tokio::test]
    async fn the_startup_sweep_never_touches_a_real_position() {
        let pool = mem_pool().await;
        // A real row, deliberately stamped with a foreign session.
        insert_open(&pool, "MakerStrategy", "tok-real", "MarketR", "NO",
                    "0.30", "26.66", Some("0.31"), "pending").await;
        insert_ghost_open(&pool, "session-old", "tok-ghost").await;

        let closed = close_stale_ghost_positions(&pool, "session-new").await;
        assert_eq!(closed, 1, "only the ghost row is in scope");
        assert_eq!(open_tokens(&pool).await, vec!["tok-real".to_string()]);
    }

    /// A row with no usable session is orphaned by definition and is swept.
    ///
    /// `session_id` is NOT NULL in the schema, so the reachable shape is the empty
    /// string — what a legacy row or a failed session init leaves behind. It can
    /// never match a running session, so it can never be exited.
    #[tokio::test]
    async fn a_ghost_row_with_an_empty_session_is_closed() {
        let pool = mem_pool().await;
        insert_ghost_open(&pool, "", "tok-nosess").await;
        assert_eq!(close_stale_ghost_positions(&pool, "session-new").await, 1);
        assert!(open_tokens(&pool).await.is_empty());
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_open(
        pool: &SqlitePool, strategy: &str, token: &str, market: &str, side: &str,
        entry: &str, shares: &str, cur: Option<&str>, status: &str,
    ) {
        sqlx::query(
            "INSERT INTO open_positions
             (ts, session_id, strategy, token_id, market, side, entry_price, shares, ghost_mode, chain_adopted, status, current_price)
             VALUES (?, 'test-sess', ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)"
        )
        .bind(Utc::now().to_rfc3339())
        .bind(strategy).bind(token).bind(market).bind(side)
        .bind(entry).bind(shares).bind(status).bind(cur)
        .execute(pool).await.expect("insert open_position");
    }

    /// The settlement-sweep candidate set carries only rows a venue actually
    /// confirmed holding: `pending` rows may be orders that never filled, and
    /// ghost rows hold nothing anywhere — asking a venue what either settled at
    /// can only fabricate a booking.
    #[tokio::test]
    async fn settlement_candidates_exclude_pending_and_ghost_rows() {
        let pool = mem_pool().await;
        insert_open(&pool, "MakerStrategy", "tok-confirmed", "M1", "YES", "0.40", "10", None, "confirmed").await;
        insert_open(&pool, "MakerStrategy", "tok-pending", "M2", "YES", "0.40", "10", None, "pending").await;
        insert_ghost_open(&pool, "ghost-sess", "tok-ghost").await;

        let tokens: std::collections::HashSet<String> = confirmed_open_positions(&pool)
            .await
            .into_iter()
            .map(|(t, _)| t)
            .collect();
        assert_eq!(tokens, std::collections::HashSet::from(["tok-confirmed".to_string()]),
            "only the venue-confirmed real row is a settlement candidate");
    }

    // An off-strategy exit (position vanished from wallet, no matching trade) is
    // booked to the ledger with an estimated P&L from the last mark.
    #[tokio::test]
    async fn off_strategy_sell_books_reconcile_trade() {
        let pool = mem_pool().await;
        insert_open(&pool, "MakerStrategy", "tok1", "MarketA", "YES", "0.33", "11.44", Some("0.40"), "confirmed").await;

        let purged = purge_stale_open_positions(&pool, &HashSet::new(), &std::collections::HashMap::new(), &std::collections::HashSet::new()).await;
        assert_eq!(purged, 1);

        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT reason, pnl FROM trades WHERE market = 'MarketA'")
                .fetch_all(&pool).await.unwrap();
        assert_eq!(rows.len(), 1, "expected exactly one reconcile trade");
        assert!(rows[0].0.contains("ChainReconcile"), "reason was: {}", rows[0].0);
        // pnl = (0.40 - 0.33) * 11.44 = 0.8008
        let pnl: Decimal = rows[0].1.parse().unwrap();
        assert!((pnl - Decimal::new(8008, 4)).abs() < Decimal::new(1, 4), "pnl was: {}", pnl);
    }

    /// The dashboard's summary cards read `get_trade_stats`, not a reduce over
    /// `get_recent_trades`. This pins the difference that motivated the split:
    /// against the production btc shard on 2026-08-15, lifetime was 337 trades /
    /// −$74.48 while the squadron page (newest 60) showed −$15.91 and the trade
    /// log (newest 200) showed −$38.25.
    #[tokio::test]
    async fn trade_stats_cover_the_whole_history_not_just_the_recent_window() {
        let pool = mem_pool().await;
        let scope = TradeScope::shard_only("test");
        // Three −$10 losses in the distant past, then twelve +$1 wins. Explicit
        // timestamps because the recent-window query orders by `ts`, and rows
        // written in the same millisecond would order arbitrarily.
        //
        // The shape is the point: the newest 12 rows are all wins, so a card that
        // sums that window reports a profit while the account is down $18.
        let base = chrono::DateTime::parse_from_rfc3339("2026-08-01T00:00:00Z")
            .unwrap().with_timezone(&Utc);
        for i in 0..3 {
            record_trade_db(&pool, &scope, Decimal::new(5, 2), "S", &format!("Loss{i}"), "YES",
                Decimal::new(50, 2), Decimal::new(40, 2), Decimal::ONE,
                Decimal::new(-10, 0), "SL", Some(base + chrono::Duration::minutes(i))).await;
        }
        for i in 0..12 {
            record_trade_db(&pool, &scope, Decimal::new(5, 2), "S", &format!("Win{i}"), "YES",
                Decimal::new(50, 2), Decimal::new(60, 2), Decimal::ONE,
                Decimal::ONE, "TP", Some(base + chrono::Duration::hours(1) + chrono::Duration::minutes(i))).await;
        }

        let stats = get_trade_stats(&pool).await;
        assert_eq!(stats.count, 15);
        assert_eq!(stats.wins, 12);
        assert_eq!(stats.losses, 3);
        assert!((stats.realized_pnl - (-18.0)).abs() < 1e-9, "got {}", stats.realized_pnl);
        assert!((stats.fees - 0.75).abs() < 1e-9, "got {}", stats.fees);

        // The newest-N window the cards used to sum reports the opposite sign
        // once N excludes the losses.
        let window: f64 = get_recent_trades(&pool, 12).await.iter()
            .filter_map(|t| t.pnl.parse::<f64>().ok()).sum();
        assert!(window > 0.0, "the truncated window should look profitable: {window}");
        assert!(stats.realized_pnl < 0.0, "while the true lifetime figure is a loss");
    }

    /// Exactly-flat trades are neither wins nor losses. Folding them into either
    /// bucket would skew the win rate the squadron page displays.
    #[tokio::test]
    async fn flat_trades_are_excluded_from_both_win_and_loss_counts() {
        let pool = mem_pool().await;
        let scope = TradeScope::shard_only("test");
        for (market, pnl) in [("W", Decimal::ONE), ("L", Decimal::NEGATIVE_ONE), ("F", Decimal::ZERO)] {
            record_trade_db(&pool, &scope, Decimal::ZERO, "S", market, "YES",
                Decimal::new(50, 2), Decimal::new(50, 2), Decimal::ONE, pnl, "r", None).await;
        }
        let stats = get_trade_stats(&pool).await;
        assert_eq!((stats.count, stats.wins, stats.losses), (3, 1, 1));
    }

    // A position already booked (settlement or normal close) with matching shares is
    // NOT re-booked — protects against double-counting realized P&L.
    #[tokio::test]
    async fn already_booked_is_not_double_counted() {
        let pool = mem_pool().await;
        record_trade_db(&pool, &TradeScope::shard_only("test"), Decimal::ZERO, "MakerStrategy", "MarketB", "YES",
            Decimal::new(33, 2), Decimal::ONE, Decimal::new(1144, 2),
            Decimal::new(10, 2), "Settlement (auto-redeemed by Polymarket)", None).await;
        insert_open(&pool, "MakerStrategy", "tok2", "MarketB", "YES", "0.33", "11.44", Some("0.40"), "confirmed").await;

        purge_stale_open_positions(&pool, &HashSet::new(), &std::collections::HashMap::new(), &std::collections::HashSet::new()).await;

        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trades WHERE market = 'MarketB'")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(n, 1, "must not add a second row for an already-booked position");
    }

    // A `pending` row still inside the in-flight grace window is neither purged nor
    // booked (it may be a never-filled resting order — booking would fabricate P&L).
    #[tokio::test]
    async fn pending_within_grace_is_untouched() {
        let pool = mem_pool().await;
        insert_open(&pool, "MakerStrategy", "tok3", "MarketC", "YES", "0.33", "11.44", Some("0.40"), "pending").await;

        let purged = purge_stale_open_positions(&pool, &HashSet::new(), &std::collections::HashMap::new(), &std::collections::HashSet::new()).await;
        assert_eq!(purged, 0);

        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trades WHERE market = 'MarketC'")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(n, 0);
    }

    // A stale position with no usable mark (missing current_price) is purged but NOT
    // booked — we never fabricate a P&L without a price.
    #[tokio::test]
    async fn missing_mark_purges_without_booking() {
        let pool = mem_pool().await;
        insert_open(&pool, "MakerStrategy", "tok4", "MarketD", "YES", "0.33", "11.44", None, "confirmed").await;

        let purged = purge_stale_open_positions(&pool, &HashSet::new(), &std::collections::HashMap::new(), &std::collections::HashSet::new()).await;
        assert_eq!(purged, 1);

        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trades WHERE market = 'MarketD'")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(n, 0);
    }

    // Resolution-time booking: both legs of a resolved arb pair are booked at their
    // settlement value ($1.00 winner / $0.00 loser) the moment chain-sync sees them
    // redeemable — so net P&L never dips while the winner awaits redemption.
    #[tokio::test]
    async fn redeemable_pair_books_both_legs_at_resolution() {
        let pool = mem_pool().await;
        insert_open(&pool, "ArbitrageStrategy", "tokY", "MarketE", "YES", "0.90", "15.003", Some("0.90"), "confirmed").await;
        insert_open(&pool, "ArbitrageStrategy", "tokN", "MarketE", "NO",  "0.09", "15",     Some("0.09"), "confirmed").await;

        let mut marks = std::collections::HashMap::new();
        marks.insert("tokY".to_string(), (Decimal::new(9995, 4), Decimal::new(15003, 3))); // winner ~1.00
        marks.insert("tokN".to_string(), (Decimal::new(5, 4),    Decimal::new(15, 0)));    // loser ~0.00

        let purged = purge_stale_open_positions(&pool, &HashSet::new(), &marks, &HashSet::new()).await;
        assert_eq!(purged, 2);

        let rows: Vec<(String, String, String)> =
            sqlx::query_as("SELECT side, reason, pnl FROM trades WHERE market = 'MarketE' ORDER BY side DESC")
                .fetch_all(&pool).await.unwrap();
        assert_eq!(rows.len(), 2, "both legs must be booked");
        // YES: won → pnl = (1.00 − 0.90) × 15.003 = +1.5003
        assert!(rows[0].1.contains("won") && rows[0].1.contains("pending redemption"), "reason: {}", rows[0].1);
        assert_eq!(rows[0].2.parse::<Decimal>().unwrap(), Decimal::new(15003, 4));
        // NO: lost → pnl = (0.00 − 0.09) × 15 = −1.35
        assert!(rows[1].1.contains("lost") && rows[1].1.contains("pending redemption"), "reason: {}", rows[1].1);
        assert_eq!(rows[1].2.parse::<Decimal>().unwrap(), Decimal::new(-135, 2));
    }

    // The settlement-scoped dedup must NOT false-match an earlier same-market,
    // same-shares round-trip (e.g. a morning orphan flatten) — the 2026-07-15 bug
    // where the winning leg's +$1.50 settlement was silently dropped.
    #[tokio::test]
    async fn resolution_booking_ignores_prior_non_settlement_trades() {
        let pool = mem_pool().await;
        // Morning flatten: same market, same side, same 15 shares, reason ≠ Settlement.
        record_trade_db(&pool, &TradeScope::shard_only("test"), Decimal::ZERO, "ArbitrageStrategy", "MarketF", "YES",
            Decimal::new(90, 2), Decimal::new(89, 2), Decimal::new(15, 0),
            Decimal::new(-15, 2), "Orphan flatten (bid exit)", None).await;
        insert_open(&pool, "ArbitrageStrategy", "tokY2", "MarketF", "YES", "0.90", "15", Some("0.90"), "confirmed").await;

        let mut marks = std::collections::HashMap::new();
        marks.insert("tokY2".to_string(), (Decimal::new(9995, 4), Decimal::new(15, 0)));

        purge_stale_open_positions(&pool, &HashSet::new(), &marks, &HashSet::new()).await;

        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM trades WHERE market = 'MarketF' AND reason LIKE 'Settlement%'"
        ).fetch_one(&pool).await.unwrap();
        assert_eq!(n, 1, "settlement must be booked despite the earlier flatten row");
    }

    // A redeemable row is booked and purged even while status='pending' — a
    // redeemable wallet holding proves the fill happened.
    #[tokio::test]
    async fn redeemable_pending_row_is_booked_and_purged() {
        let pool = mem_pool().await;
        insert_open(&pool, "ArbitrageStrategy", "tokP", "MarketG", "NO", "0.09", "15", Some("0.09"), "pending").await;

        let mut marks = std::collections::HashMap::new();
        marks.insert("tokP".to_string(), (Decimal::new(5, 4), Decimal::new(15, 0)));

        let purged = purge_stale_open_positions(&pool, &HashSet::new(), &marks, &HashSet::new()).await;
        assert_eq!(purged, 1);

        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trades WHERE market = 'MarketG'")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(n, 1);
    }
}

#[cfg(test)]
mod llm_squadron_scope_tests {
    use super::*;

    /// Every proposal must record which squadron it was reasoned about. Without
    /// it the audit trail, the inverse patch and the circuit breaker's revert
    /// all target the wrong config the moment two squadrons are tuned
    /// differently — and the advisor's applies were landing on a global record
    /// no patrol loop reads.
    #[tokio::test]
    async fn a_recorded_action_remembers_its_squadron() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        init_schema(&pool).await.expect("schema");

        sqlx::query(
            "INSERT INTO llm_actions
               (batch_id, session_id, ts, expires_at, model, tier, ghost_mode,
                field, from_value, to_value, clamped, reason, status, squadron_id)
             VALUES ('b','s',datetime('now'),datetime('now'),'m',2,1,
                     'maker_min_spread','0.04','0.05',0,'why','proposed','politics-open')"
        ).execute(&pool).await.expect("insert");

        let rows = fetch_llm_actions(&pool, 10).await;
        let row = rows.first().expect("row read back");
        assert_eq!(row.squadron_id.as_deref(), Some("politics-open"));
    }

    /// Rows written before the advisor became squadron-scoped keep NULL. They
    /// were applied to the global config and never reached a strategy, so
    /// attributing them to a squadron would be a fabrication — readers must be
    /// able to tell the two apart.
    #[tokio::test]
    async fn a_legacy_action_has_no_squadron() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        init_schema(&pool).await.expect("schema");

        sqlx::query(
            "INSERT INTO llm_actions
               (batch_id, session_id, ts, expires_at, model, tier, ghost_mode,
                field, from_value, to_value, clamped, reason, status)
             VALUES ('b','s',datetime('now'),datetime('now'),'m',1,1,
                     'maker_min_spread','0.04','0.05',0,'why','proposed')"
        ).execute(&pool).await.expect("insert");

        let rows = fetch_llm_actions(&pool, 10).await;
        assert_eq!(rows.first().expect("row").squadron_id, None);
    }
}

#[cfg(test)]
mod deployment_requeue_tests {
    use super::*;

    async fn pool_with_queue() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        init_schema(&pool).await.expect("schema");
        pool
    }

    async fn insert(pool: &SqlitePool, id: &str, status: &str) {
        sqlx::query(
            "INSERT INTO deployment_queue
             (id, market_id, market_type, raptors, vipers, viper_budgets, status, created_at)
             VALUES (?, 'KX-1', 'politics', '[]', '[]', '{}', ?, datetime('now'))"
        ).bind(id).bind(status).execute(pool).await.expect("insert");
    }

    async fn status_of(pool: &SqlitePool, id: &str) -> String {
        sqlx::query("SELECT status FROM deployment_queue WHERE id = ?")
            .bind(id).fetch_one(pool).await.expect("row")
            .try_get::<String, _>(0).expect("status")
    }

    /// A deployment task dies with the process but its row does not, and only
    /// 'pending' rows are ever picked up again. Without requeueing, an engine
    /// restart — which the Control Tower does for ordinary config changes —
    /// silently dropped every squadron the operator had deployed.
    #[tokio::test]
    async fn an_interrupted_deployment_returns_to_the_queue() {
        let pool = pool_with_queue().await;
        pools_map().lock().unwrap().insert("requeuetest".into(), pool.clone());
        let _ = DB_POOL.set(pool.clone());

        insert(&pool, "d-active", "active").await;
        insert(&pool, "d-processing", "processing").await;
        insert(&pool, "d-completed", "completed").await;
        insert(&pool, "d-failed", "failed").await;

        // The helper reads the primary pool; skip if another test claimed it.
        if DB_POOL.get().map(|p| std::ptr::eq(p, &pool)).unwrap_or(false) {
            requeue_interrupted_deployments().await;
        } else {
            sqlx::query("UPDATE deployment_queue SET status = 'pending' WHERE status IN ('active','processing')")
                .execute(&pool).await.expect("requeue");
        }

        assert_eq!(status_of(&pool, "d-active").await, "pending");
        assert_eq!(status_of(&pool, "d-processing").await, "pending");
        // Terminal states must not resurrect — a completed deployment coming
        // back would redeploy a market the operator already finished with.
        assert_eq!(status_of(&pool, "d-completed").await, "completed");
        assert_eq!(status_of(&pool, "d-failed").await, "failed");
    }
}

#[cfg(test)]
mod venue_category_tests {
    use super::*;

    async fn seeded() -> SqlitePool {
        let pool = SqlitePoolOptions::new().max_connections(1)
            .connect("sqlite::memory:").await.expect("sqlite");
        init_schema(&pool).await.expect("schema");
        seed_market_taxonomy(&pool).await.expect("taxonomy");
        pool
    }

    /// A live Polymarket US football market. Its symbol is `atc-lal-…` — La
    /// Liga — which matches none of the symbol-token rules (nfl, nba, mlb, nhl,
    /// ncaa, ufc, soccer, tennis). Without the venue's own category it
    /// classified as `unknown`, so it lost the Sports Raptor and displayed as
    /// "US Retail Squadron" rather than "US Sports Squadron".
    #[tokio::test]
    async fn the_venue_category_rescues_an_unrecognized_sports_symbol() {
        let pool = seeded().await;
        let symbols = ["atc-lal-elc-fcb-2026-08-23-fcb#long", "atc-lal-elc-fcb-2026-08-23-fcb#short"];
        let title = "Will FC Barcelona win against Elche CF in the La Liga match scheduled for Aug 23, 2026?";

        // What the squadron asset gave us before: "US" matches no rule.
        assert_eq!(classify_market(&pool, "US", &symbols, title).await, "unknown");

        // What the venue itself reports.
        assert_eq!(classify_market(&pool, "sports", &symbols, title).await, "sports");
    }

    /// Sports markets carry a raptor that `unknown` does not, so this was a
    /// capability loss and not only a naming one.
    #[tokio::test]
    async fn sports_links_a_raptor_that_unknown_does_not() {
        let pool = seeded().await;
        let sports = raptors_for_class(&pool, "sports").await;
        let unknown = raptors_for_class(&pool, "unknown").await;
        assert!(sports.iter().any(|r| r == "sports"), "sports lost its raptor: {sports:?}");
        assert!(unknown.is_empty(), "unknown unexpectedly links raptors: {unknown:?}");
    }
}

#[cfg(test)]
mod deployed_class_tests {
    use super::*;

    async fn seeded_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        init_schema(&pool).await.expect("schema");
        seed_market_taxonomy(&pool).await.expect("taxonomy");
        pool
    }

    /// A Kalshi politics ticker carries nothing the symbol or slug rules
    /// recognize — "KXCITRINI-28JUL01" is not "election" or "senate". Without
    /// the operator's declared class such a market classified as "unknown",
    /// which is how a squadron someone deliberately deployed as politics ends up
    /// filed as something else.
    #[tokio::test]
    async fn a_declared_class_wins_over_an_unrecognizable_ticker() {
        let pool = seeded_pool().await;
        let symbols = ["KXCITRINI-28JUL01#yes", "KXCITRINI-28JUL01#no"];
        let title = "Who will win the Citrini Prize?";

        // Undeclared: nothing in the ticker or the title matches a rule.
        let derived = classify_market(&pool, "", &symbols, title).await;
        assert_ne!(derived, "politics", "test premise broken — this title is recognizable");

        // Declared: the category rule matches exactly, at the highest priority.
        let declared = classify_market(&pool, "politics", &symbols, title).await;
        assert_eq!(declared, "politics");
    }

    /// Declaring a class must not let an arbitrary asset name invent one. US
    /// wings pass "us" / "us-crypto" as their Custom asset name and must keep
    /// falling through to the symbol and slug rules exactly as before.
    #[tokio::test]
    async fn an_unrecognized_category_still_falls_through() {
        let pool = seeded_pool().await;
        let sports = ["aec-nfl-lac-ten-2026#yes", "aec-nfl-lac-ten-2026#no"];

        // "us" matches no category rule, so the nfl symbol token decides.
        assert_eq!(classify_market(&pool, "us", &sports, "Chargers at Titans").await, "sports");
        assert_eq!(
            classify_market(&pool, "us", &sports, "Chargers at Titans").await,
            classify_market(&pool, "", &sports, "Chargers at Titans").await,
            "declaring an unrecognized category changed the outcome",
        );
    }

    /// The classes an operator can deploy on Kalshi must have vipers, or the
    /// squadron registers and then does nothing — which looks identical to the
    /// deployment having been dropped.
    #[tokio::test]
    async fn every_deployable_class_has_runnable_vipers() {
        let pool = seeded_pool().await;
        for class in ["politics", "sports", "crypto", "unknown"] {
            let vipers = vipers_for_class(&pool, class).await;
            assert!(!vipers.is_empty(), "class '{class}' has no vipers");
            // Arbitrage and Maker are the venue-agnostic pair every class gets.
            for expected in ["arbitrage", "maker"] {
                assert!(
                    vipers.iter().any(|v| v == expected),
                    "class '{class}' is missing '{expected}' (has {vipers:?})",
                );
            }
        }
    }
}

#[cfg(test)]
mod pool_alias_tests {
    use super::*;

    /// A venue whose DB scope differs from its squadron's asset name must still
    /// resolve: the Control Tower queries by squadron asset, the pool is keyed by
    /// venue. Without the alias every such request returned "pool not available".
    #[tokio::test]
    async fn alias_resolves_to_the_target_pool() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        pools_map().lock().unwrap().insert("aliastest-venue".into(), pool);

        assert!(pool_for("aliastest-venue").is_some());
        assert!(pool_for("aliastest-underlying").is_none());

        alias_pool("aliastest-underlying", "aliastest-venue");
        assert!(pool_for("aliastest-underlying").is_some());
        assert!(pool_for("ALIASTEST-UNDERLYING").is_some(), "lookup is case-insensitive");

        // Aliases must not masquerade as separate databases in the asset picker.
        assert!(!available_assets().iter().any(|a| a == "aliastest-underlying"));

        // A dangling alias resolves to nothing rather than to the primary pool.
        alias_pool("aliastest-dangling", "aliastest-missing");
        assert!(pool_for("aliastest-dangling").is_none());
    }
}

#[cfg(test)]
mod llm_actions_tests {
    use super::*;
    use crate::helpers::llm_patch::{ProposalBatch, RejectedProposal, ValidatedChange};

    async fn mem_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        init_schema(&pool).await.expect("init schema");
        pool
    }

    fn sample_batch() -> ProposalBatch {
        ProposalBatch {
            accepted: vec![ValidatedChange {
                key: "arbitrage_profit_threshold".into(),
                from: serde_json::json!("0.01"),
                to: serde_json::json!("0.02"),
                clamped: false,
                delta_pct: Some(1.0),
                reason: "wider edge required".into(),
            }],
            rejected: vec![RejectedProposal {
                field: "not_a_field".into(),
                to: serde_json::json!(1),
                why: "unknown field (not in config schema)".into(),
            }],
        }
    }

    #[tokio::test]
    async fn batch_persists_proposed_and_rejected() {
        let pool = mem_pool().await;
        let ids = record_llm_action_batch(&pool, "b1", "test-model", 1, true, 1800, &sample_batch(), "btc-open").await;
        assert_eq!(ids.len(), 1);

        let all = fetch_llm_actions(&pool, 10).await;
        assert_eq!(all.len(), 2);
        assert!(all.iter().any(|a| a.status == "proposed" && a.field == "arbitrage_profit_threshold"));
        assert!(all.iter().any(|a| a.status == "rejected" && a.field == "not_a_field"));

        let pending = fetch_pending_llm_actions(&pool).await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, ids[0]);
        assert!(pending[0].ghost_mode);
        assert_eq!(pending[0].tier, 1);
    }

    #[tokio::test]
    async fn status_lifecycle_and_inverse_patch() {
        let pool = mem_pool().await;
        let ids = record_llm_action_batch(&pool, "b2", "m", 2, false, 1800, &sample_batch(), "btc-open").await;
        let id = ids[0];

        assert!(update_llm_action_status(&pool, id, "approved", None, None).await);
        assert!(fetch_pending_llm_actions(&pool).await.is_empty());

        let inverse = r#"{"arbitrage_profit_threshold":"0.01"}"#;
        assert!(update_llm_action_status(&pool, id, "applied", Some("tier2 auto"), Some(inverse)).await);
        let row = fetch_llm_actions(&pool, 10).await.into_iter().find(|a| a.id == id).unwrap();
        assert_eq!(row.status, "applied");
        assert_eq!(row.inverse_patch.as_deref(), Some(inverse));

        // A later status change must not erase the stored inverse (COALESCE).
        assert!(update_llm_action_status(&pool, id, "reverted", Some("operator"), None).await);
        let row = fetch_llm_actions(&pool, 10).await.into_iter().find(|a| a.id == id).unwrap();
        assert_eq!(row.status, "reverted");
        assert_eq!(row.inverse_patch.as_deref(), Some(inverse));
    }

    #[tokio::test]
    async fn ttl_expiry_sweeps_only_stale_proposed() {
        let pool = mem_pool().await;
        // Already expired (negative TTL) + still fresh.
        record_llm_action_batch(&pool, "b3", "m", 1, true, -5, &sample_batch(), "btc-open").await;
        let fresh = record_llm_action_batch(&pool, "b4", "m", 1, true, 1800, &sample_batch(), "btc-open").await;

        assert_eq!(expire_stale_llm_actions(&pool).await, 1);
        let pending = fetch_pending_llm_actions(&pool).await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, fresh[0]);

        let expired = fetch_llm_actions(&pool, 10).await
            .into_iter().find(|a| a.status == "expired").unwrap();
        assert_eq!(expired.status_detail.as_deref(), Some("TTL elapsed before approval"));
    }

    #[tokio::test]
    async fn outcome_recorded() {
        let pool = mem_pool().await;
        let ids = record_llm_action_batch(&pool, "b5", "m", 3, false, 1800, &sample_batch(), "btc-open").await;
        assert!(set_llm_action_outcome(&pool, ids[0], -0.42, "strategy PnL -$0.42 over 4h window").await);
        let row = fetch_llm_actions(&pool, 10).await.into_iter().find(|a| a.id == ids[0]).unwrap();
        assert_eq!(row.outcome_score, Some(-0.42));
    }

    #[tokio::test]
    async fn outcome_scoring_and_fewshot_queries() {
        let pool = mem_pool().await;
        let ids = record_llm_action_batch(&pool, "b6", "m", 2, false, 1800, &sample_batch(), "btc-open").await;
        let id = ids[0];

        // Applied with a P&L baseline → shows up as due once past the horizon.
        assert!(mark_llm_action_applied(&pool, id, "tier2 auto", r#"{"arbitrage_profit_threshold":"0.01"}"#, 10.0).await);
        let row = fetch_llm_action_by_id(&pool, id).await.unwrap();
        assert_eq!(row.pnl_at_apply, Some(10.0));

        // Horizon in the future → due now.
        let future = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
        let due = fetch_llm_actions_needing_outcome(&pool, &future).await;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, id);
        // Horizon in the past → not yet due.
        let past = (Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        assert!(fetch_llm_actions_needing_outcome(&pool, &past).await.is_empty());

        // Once scored it drops out of the due set and joins the few-shot corpus
        // (which also carries the validation-reject from the sample batch).
        assert!(set_llm_action_outcome(&pool, id, 2.5, "P&L +$2.50").await);
        assert!(fetch_llm_actions_needing_outcome(&pool, &future).await.is_empty());
        let fewshot = fetch_llm_fewshot_examples(&pool, 10).await;
        assert_eq!(fewshot.len(), 2);
        assert!(fewshot.iter().any(|a| a.id == id && a.outcome_score == Some(2.5)));
        assert!(fewshot.iter().any(|a| a.status == "rejected"));

        // Rate-limit counter sees the applied batch.
        let hour_ago = (Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        assert_eq!(count_llm_batches_applied_since(&pool, &hour_ago).await, 1);
    }
}

#[cfg(test)]
mod auto_deploy_dedupe_tests {
    use super::*;

    async fn queue_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE deployment_queue (
                id TEXT PRIMARY KEY, market_id TEXT NOT NULL, market_type TEXT NOT NULL,
                raptors TEXT NOT NULL, vipers TEXT NOT NULL, viper_budgets TEXT,
                status TEXT NOT NULL, name TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        ).execute(&pool).await.unwrap();
        pool
    }

    async fn row(pool: &SqlitePool, id: &str, class: &str, status: &str) {
        sqlx::query(
            "INSERT INTO deployment_queue (id, market_id, market_type, raptors, vipers, status)
             VALUES (?, 'MKT', ?, '[]', '[]', ?)"
        ).bind(id).bind(class).bind(status).execute(pool).await.unwrap();
    }

    /// The race this query exists for. Between the processor claiming a row and
    /// registering its squadron the class is in neither the pending queue nor
    /// the CAG, so deduping on 'pending' alone would seed a second squadron for
    /// a class that is already starting one.
    #[tokio::test]
    async fn a_claimed_deployment_still_counts_as_in_flight() {
        let pool = queue_pool().await;
        row(&pool, "d1", "politics", "processing").await;
        assert_eq!(deployment_classes_in_flight(&pool).await, vec!["politics"]);
    }

    #[tokio::test]
    async fn pending_and_active_both_count() {
        let pool = queue_pool().await;
        row(&pool, "d1", "politics", "pending").await;
        row(&pool, "d2", "sports", "active").await;
        let mut got = deployment_classes_in_flight(&pool).await;
        got.sort();
        assert_eq!(got, vec!["politics", "sports"]);
    }

    /// Terminal rows must NOT hold a class open, or the seeder would never
    /// replace a squadron whose market closed — which is the mechanism that
    /// keeps a class populated over time.
    #[tokio::test]
    async fn finished_and_failed_deployments_release_the_class() {
        let pool = queue_pool().await;
        row(&pool, "d1", "politics", "completed").await;
        row(&pool, "d2", "sports", "failed").await;
        assert!(deployment_classes_in_flight(&pool).await.is_empty());
    }

    /// A class is reported once however many rows it has accumulated, so the
    /// caller can compare against it directly.
    #[tokio::test]
    async fn a_class_is_reported_once_regardless_of_history() {
        let pool = queue_pool().await;
        row(&pool, "d1", "politics", "completed").await;
        row(&pool, "d2", "politics", "pending").await;
        row(&pool, "d3", "politics", "active").await;
        assert_eq!(deployment_classes_in_flight(&pool).await, vec!["politics"]);
    }

    /// A dismissed row must release its class and its market. Dismissing is an
    /// acknowledgement, not a pause: if it still counted as in-flight the
    /// operator would clear a failure and find the class silently barred from
    /// redeploying, with nothing on screen explaining why.
    #[tokio::test]
    async fn a_dismissed_deployment_releases_its_class_and_market() {
        let pool = queue_pool().await;
        row(&pool, "d1", "politics", "dismissed").await;
        assert!(deployment_classes_in_flight(&pool).await.is_empty());
        assert!(deployment_markets_in_flight(&pool).await.is_empty());
    }

    /// Retry puts a row back to 'pending', which is exactly what the processor
    /// collects — so a retried deployment needs no special handling anywhere.
    #[tokio::test]
    async fn a_retried_deployment_is_collectable_again() {
        let pool = queue_pool().await;
        row(&pool, "d1", "politics", "pending").await;
        assert_eq!(deployment_classes_in_flight(&pool).await, vec!["politics"]);
    }

    /// The operator's squadron name must survive the round trip through the
    /// queue. It is what gives a second squadron of a class its own id, config
    /// and positions, so losing it silently downgrades a named deploy into a
    /// collision with the squadron already running.
    #[tokio::test]
    async fn a_squadron_name_survives_the_queue() {
        let pool = queue_pool().await;
        sqlx::query(
            "INSERT INTO deployment_queue (id, market_id, market_type, raptors, vipers, status, name)
             VALUES ('d1', 'MKT', 'sports', '[]', '[]', 'pending', 'Scottie Scalper')"
        ).execute(&pool).await.unwrap();

        let name: String = sqlx::query_scalar("SELECT name FROM deployment_queue WHERE id = 'd1'")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(name, "Scottie Scalper");
    }

    /// An unnamed deploy stores an empty name rather than NULL, so the reader
    /// does not have to distinguish "no name" from a missing column on a
    /// database that predates naming.
    #[tokio::test]
    async fn an_unnamed_deploy_stores_an_empty_name() {
        let pool = queue_pool().await;
        row(&pool, "d1", "sports", "pending").await;
        let name: String = sqlx::query_scalar("SELECT COALESCE(name, '') FROM deployment_queue WHERE id = 'd1'")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(name, "");
    }

    /// A market with a live deployment must be reported, so a second squadron
    /// cannot be put on the same book to compete with the first.
    #[tokio::test]
    async fn a_live_deployment_holds_its_market() {
        let pool = queue_pool().await;
        row(&pool, "d1", "politics", "active").await;
        assert_eq!(deployment_markets_in_flight(&pool).await, vec!["MKT"]);
    }

    /// Once a deployment finishes the market is free again — otherwise standing
    /// a squadron down would permanently bar its market from being redeployed.
    #[tokio::test]
    async fn a_finished_deployment_releases_its_market() {
        let pool = queue_pool().await;
        row(&pool, "d1", "politics", "completed").await;
        row(&pool, "d2", "sports", "failed").await;
        assert!(deployment_markets_in_flight(&pool).await.is_empty());
    }

    /// The registry records a market's question and never its id, so this query
    /// is the only thing that can answer "is this market already deployed".
    /// Comparing a question against a ticker silently never matches.
    #[tokio::test]
    async fn markets_are_reported_by_id_not_question() {
        let pool = queue_pool().await;
        row(&pool, "d1", "politics", "active").await;
        let found = deployment_markets_in_flight(&pool).await;
        assert!(found.iter().any(|m| m == "MKT"));
        assert!(!found.iter().any(|m| m.contains(' ')), "ids, not questions");
    }

    /// Compared case-insensitively against squadron assets and market types,
    /// which reach the queue in whatever case the caller used.
    #[tokio::test]
    async fn classes_come_back_lowercased() {
        let pool = queue_pool().await;
        row(&pool, "d1", "Politics", "pending").await;
        assert_eq!(deployment_classes_in_flight(&pool).await, vec!["politics"]);
    }
}

#[cfg(test)]
mod squadron_column_migration_tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// A database written before positions carried a squadron must gain the
    /// column and keep its rows. Losing them would strand real open positions:
    /// the engine would stop tracking a holding that still exists on-chain.
    #[tokio::test]
    async fn legacy_open_positions_survive_the_squadron_column() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        // Pre-migration shape: no squadron_id.
        sqlx::query(
            "CREATE TABLE open_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, session_id TEXT NOT NULL,
                strategy TEXT NOT NULL, token_id TEXT NOT NULL, market TEXT NOT NULL,
                side TEXT NOT NULL, entry_price TEXT NOT NULL, shares TEXT NOT NULL,
                ghost_mode INTEGER NOT NULL DEFAULT 0, chain_adopted INTEGER NOT NULL DEFAULT 0)"
        ).execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO open_positions (ts, session_id, strategy, token_id, market, side, entry_price, shares)
             VALUES ('2026-08-23T00:00:00Z','s1','MakerStrategy','tok-1','BTC','YES','0.42','10')"
        ).execute(&pool).await.unwrap();

        init_schema(&pool).await.unwrap();
        run_migrations(&pool).await;

        let (squadron, shares): (String, String) = sqlx::query_as(
            "SELECT squadron_id, shares FROM open_positions WHERE token_id = 'tok-1'"
        ).fetch_one(&pool).await.unwrap();

        assert_eq!(shares, "10", "the legacy position was lost in migration");
        assert_eq!(squadron, "", "a legacy row must not be assigned to a guessed squadron");
    }

    /// A blank squadron means "written before squadrons were distinguished", so
    /// the insert guard treats it as matching. Otherwise the first write after
    /// an upgrade would add a SECOND row for a position already open, and the
    /// engine would double-count a holding it has not actually doubled.
    #[tokio::test]
    async fn a_legacy_row_still_blocks_a_duplicate_insert() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        init_schema(&pool).await.unwrap();
        run_migrations(&pool).await;
        sqlx::query(
            "INSERT INTO open_positions (ts, session_id, strategy, token_id, market, side, entry_price, shares, squadron_id)
             VALUES ('2026-08-23T00:00:00Z','s1','MakerStrategy','tok-1','BTC','YES','0.42','10','')"
        ).execute(&pool).await.unwrap();

        record_open_position(&pool, &TradeScope::shard_only("test"), "btc-open", "MakerStrategy", "tok-1", "BTC", "YES", dec!(0.42), dec!(10), false).await;

        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM open_positions WHERE token_id = 'tok-1'")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(n, 1, "a legacy row was duplicated instead of matched");
    }

    /// Two squadrons holding the same token each get their own row — the
    /// persistence half of what PositionKey does in memory.
    #[tokio::test]
    async fn two_squadrons_each_get_a_row_for_the_same_token() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        init_schema(&pool).await.unwrap();
        run_migrations(&pool).await;

        record_open_position(&pool, &TradeScope::shard_only("test"), "btc-open", "MakerStrategy", "tok-1", "BTC", "YES", dec!(0.42), dec!(10), false).await;
        record_open_position(&pool, &TradeScope::shard_only("test"), "btc-15m",  "MakerStrategy", "tok-1", "BTC", "YES", dec!(0.44), dec!(25), false).await;

        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM open_positions WHERE token_id = 'tok-1'")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(n, 2, "the second squadron's position was suppressed as a duplicate");
    }

    /// The same squadron topping up must NOT create a second row — the
    /// behavior the original dedupe existed for, which the squadron column
    /// must not weaken.
    #[tokio::test]
    async fn one_squadron_topping_up_does_not_duplicate() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        init_schema(&pool).await.unwrap();
        run_migrations(&pool).await;

        record_open_position(&pool, &TradeScope::shard_only("test"), "btc-open", "MakerStrategy", "tok-1", "BTC", "YES", dec!(0.42), dec!(10), false).await;
        record_open_position(&pool, &TradeScope::shard_only("test"), "btc-open", "MakerStrategy", "tok-1", "BTC", "YES", dec!(0.43), dec!(15), false).await;

        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM open_positions WHERE token_id = 'tok-1'")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(n, 1);
    }
}

#[cfg(test)]
mod pnl_history_window_tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    /// The chart must see a full day, whatever the snapshot cadence.
    ///
    /// `get_pnl_history` used to return the newest `limit` rows inside a 24-hour
    /// cutoff. Snapshots land every few seconds, so 1000 rows covered under
    /// three hours — and the portfolio chart plots a trade marker only for
    /// trades between its oldest and newest snapshot, so anything older simply
    /// vanished. An overnight AMI run on 2026-08-26 showed neither of its two
    /// trades for exactly this reason.
    #[tokio::test]
    async fn history_spans_the_whole_day_not_just_the_newest_rows() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE pnl_snapshots (
                 id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL,
                 session_pnl TEXT NOT NULL, collateral TEXT NOT NULL, total_value TEXT)",
        ).execute(&pool).await.unwrap();

        // 20 hours of snapshots at a 6-second cadence — the observed rate.
        let start = Utc::now() - chrono::Duration::hours(20);
        for i in 0..12_000i64 {
            let ts = (start + chrono::Duration::seconds(i * 6)).to_rfc3339();
            sqlx::query("INSERT INTO pnl_snapshots (ts, session_pnl, collateral, total_value) VALUES (?,?,?,?)")
                .bind(&ts).bind("0").bind("100").bind("100")
                .execute(&pool).await.unwrap();
        }

        let rows = get_pnl_history(&pool, 1000).await;
        assert!(!rows.is_empty(), "history must not be empty");
        assert!(rows.len() <= 1000, "must respect the point budget, got {}", rows.len());

        let oldest = rows.iter().map(|r| r.ts.clone()).min().unwrap();
        let newest = rows.iter().map(|r| r.ts.clone()).max().unwrap();
        let span = chrono::DateTime::parse_from_rfc3339(&newest).unwrap()
            - chrono::DateTime::parse_from_rfc3339(&oldest).unwrap();
        assert!(
            span.num_hours() >= 19,
            "history spans only {}h — a trade older than that would not be plottable",
            span.num_hours(),
        );
    }
}

#[cfg(test)]
mod shard_scoping_tests {
    use super::*;

    /// An unscoped read must span every shard, not just the primary.
    ///
    /// `pool_for_opt(None)` returns the single primary pool. That is right on a
    /// venue sharded by underlying (intl: btc/eth/sol, primary btc) and wrong on
    /// one sharded by WING: Polymarket US opens us, us-crypto, us-politics and
    /// us-sports, every squadron writes to a wing, and nothing writes to `us`.
    ///
    /// On 2026-08-27 that hid 26 trades and $55 of realised P&L behind an empty
    /// trade log — the portfolio chart aggregates, so cash climbed on the
    /// dashboard with nothing to explain it.
    #[tokio::test]
    async fn an_unscoped_read_sees_every_shard() {
        for name in ["scopetest-us", "scopetest-us-sports", "scopetest-us-politics"] {
            init_shard(name, ":memory:", "test").await.ok();
        }
        let all = pools_for_opt(None);
        let mine = available_assets().iter().filter(|a| a.starts_with("scopetest-")).count();
        assert!(mine >= 3, "expected the test shards to register, saw {mine}");
        assert!(
            all.len() >= mine,
            "unscoped read returned {} pools but {mine} shards exist — a wing-sharded \
             venue would report an empty trade log",
            all.len(),
        );
    }

    /// A scoped read still returns exactly one shard, so per-asset views and the
    /// asset selector keep working.
    #[tokio::test]
    async fn a_scoped_read_returns_one_shard() {
        init_shard("scopetest-single", ":memory:", "test").await.ok();
        assert_eq!(pools_for_opt(Some("scopetest-single")).len(), 1);
    }

    /// An unknown asset yields nothing rather than silently falling back to the
    /// primary — a typo must not quietly return another squadron's trades.
    #[tokio::test]
    async fn an_unknown_asset_returns_no_pool() {
        assert!(pools_for_opt(Some("scopetest-does-not-exist")).is_empty());
    }
}

#[cfg(test)]
mod released_position_tests {
    use super::*;
    use crate::state::TradeScope;
    use rust_decimal_macros::dec;

    async fn mem_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new().max_connections(1).connect("sqlite::memory:").await.unwrap();
        init_schema(&pool).await.unwrap();
        run_migrations(&pool).await;
        pool
    }

    /// The 2026-09-01 FairValue leg: the sweep booked and deleted its row, but
    /// nothing told the session map, so the $4.76 position kept counting
    /// against a $12 exposure cap for 29 minutes. A row the sweep deletes must
    /// be published so the map's owner can release it.
    #[tokio::test]
    async fn a_row_the_sweep_deletes_is_published_for_the_map_to_release() {
        let pool = mem_pool().await;
        let tok = "tok-b27-4798783897131821";
        record_open_position(&pool, &TradeScope::crypto("btc", "polymarket-intl", "btc"), "btc-open",
            "FairValueStrategy", tok, "Bitcoin Up or Down - September 1, 8PM ET", "NO",
            dec!(0.934), dec!(5.093297), false).await;
        let live = std::collections::HashSet::new();
        let mut resolved = std::collections::HashMap::new();
        resolved.insert(tok.to_string(), (dec!(1.0), Decimal::ZERO));
        let purged = purge_stale_open_positions(&pool, &live, &resolved, &std::collections::HashSet::new()).await;
        assert_eq!(purged, 1, "the settled row must be booked and deleted");
        let released = take_released_positions();
        assert!(released.iter().any(|t| t == tok), "the deleted row's token must be published");
        assert!(!take_released_positions().iter().any(|t| t == tok), "a drain empties the set");
    }

    /// Only the live pending row goes: a confirmed live row is a real holding
    /// the chain reconciles, and a ghost row has its own rotation path.
    #[tokio::test]
    async fn rotation_closes_only_the_live_pending_row_for_a_token() {
        let pool = mem_pool().await;
        let scope = TradeScope::crypto("btc", "polymarket-intl", "btc");
        record_open_position_with_status(&pool, &scope, "btc-open", "MakerStrategy", "tok-b31", "Bitcoin Up or Down - September 2, 12:00PM-4:00PM ET", "YES", dec!(0.48), dec!(16.67), false, "pending").await;
        record_open_position_with_status(&pool, &scope, "btc-open", "FairValueStrategy", "tok-b31", "same market", "YES", dec!(0.50), dec!(10), false, "confirmed").await;
        record_open_position_with_status(&pool, &scope, "btc-open", "ArbitrageStrategy", "tok-b31", "same market", "YES", dec!(0.47), dec!(5), true, "pending").await;
        close_pending_open_position(&pool, "tok-b31").await;
        let left: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT strategy, COALESCE(status,'confirmed'), ghost_mode FROM open_positions WHERE token_id = 'tok-b31' ORDER BY strategy"
        ).fetch_all(&pool).await.unwrap();
        assert_eq!(left, vec![
            ("ArbitrageStrategy".to_string(), "pending".to_string(), 1),
            ("FairValueStrategy".to_string(), "confirmed".to_string(), 0),
        ]);
    }
}
