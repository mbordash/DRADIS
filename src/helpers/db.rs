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
use tracing::{error, info};

use crate::config;

// ─── Shared pool ────────────────────────────────────────────────────────────

/// Primary-asset pool — the first asset initialised owns this slot.
/// Kept for backward-compat callers that use `pool()` without an asset key.
static DB_POOL: OnceLock<SqlitePool> = OnceLock::new();

/// Per-asset pool registry.  Key = lowercase asset symbol (e.g. "btc", "eth").
/// Populated by `init_for_asset()` at startup; readable thereafter.
static DB_POOLS: OnceLock<std::sync::Mutex<std::collections::HashMap<String, SqlitePool>>> =
    OnceLock::new();

/// Convenience accessor for the per-asset pool map (lazy-initialised on first call).
fn pools_map() -> &'static std::sync::Mutex<std::collections::HashMap<String, SqlitePool>> {
    DB_POOLS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
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
    let url = format!("sqlite://{}?mode=rwc", path);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?;
    init_schema(&pool).await?;
    run_migrations(&pool).await;

    // Register in per-asset map.
    pools_map().lock().unwrap().insert(asset.to_string(), pool.clone());

    // First successful call → claim the primary-pool slot (subsequent calls
    // return Err from OnceLock::set which we intentionally discard).
    let _ = DB_POOL.set(pool);

    info!("📦 SQLite initialized [{}]: {}", asset, path);
    Ok(())
}

/// Backward-compat wrapper: initialises for a single asset, deriving the asset
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

/// Returns a reference to the **primary** asset's pool (first initialised),
/// or `None` if no pool has been initialised yet.
///
/// Use `pool_for(asset)` to retrieve a specific asset's pool.
pub fn pool() -> Option<&'static SqlitePool> {
    DB_POOL.get()
}

/// Returns a clone of the pool for `asset`, or `None` if that asset has not
/// been initialised.  `SqlitePool` is cheaply cloneable (Arc-backed).
pub fn pool_for(asset: &str) -> Option<SqlitePool> {
    pools_map().lock().ok()?.get(asset).cloned()
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

/// Return the lowercase asset names for all initialised pools, sorted
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
    let _ = sqlx::query(
        "ALTER TABLE open_positions ADD COLUMN current_price TEXT"
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
        ("sports",   "Sports Raptor (roadmap)",                0),
        ("politics", "Politics Raptor (roadmap)",              0),
    ] {
        sqlx::query("INSERT OR IGNORE INTO raptor_kind (id, display, implemented) VALUES (?, ?, ?)")
            .bind(id).bind(display).bind(implemented).execute(pool).await?;
    }

    // viper_kind — venue_agnostic = 1 for pure order-book strategies.
    for (id, display, agnostic) in [
        ("arbitrage",    "Arbitrage",    1),
        ("maker",        "Maker",        1),
        ("momentum",     "Momentum",     0),
        ("gboost",       "GBoost",       0),
        ("basis",        "Basis",        0),
        ("time_decay",   "TimeDecay",    0),
        ("trendcapture", "TrendCapture", 0),
        ("convergence",  "Convergence",  0),
    ] {
        sqlx::query("INSERT OR IGNORE INTO viper_kind (id, display, venue_agnostic) VALUES (?, ?, ?)")
            .bind(id).bind(display).bind(agnostic).execute(pool).await?;
    }

    // market_class → raptor_kind. sports/politics raptors are roadmapped, so
    // they have no links yet (squadrons there get no raptor until one is built).
    for (class, raptor) in [
        ("crypto", "price"),
        ("crypto", "funding"),
    ] {
        sqlx::query("INSERT OR IGNORE INTO market_class_raptor (market_class, raptor_kind) VALUES (?, ?)")
            .bind(class).bind(raptor).execute(pool).await?;
    }

    // market_class → viper_kind. crypto gets the full suite; non-crypto (and the
    // 'unknown' fallback) get only the venue-agnostic order-book strategies.
    for (class, viper) in [
        ("crypto", "arbitrage"), ("crypto", "maker"), ("crypto", "momentum"),
        ("crypto", "gboost"),    ("crypto", "basis"), ("crypto", "time_decay"),
        ("crypto", "trendcapture"), ("crypto", "convergence"),
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
}

// ─── Session lifecycle ───────────────────────────────────────────────────────

/// Create a new session row in **all** initialised asset pools and set the
/// process-lifetime session ID.
///
/// Call once immediately after all `init_for_asset()` calls complete so every
/// asset DB gets a session row for the same RFC-3339 startup timestamp.
///
/// Returns the new session_id string.
pub async fn init_session(note: Option<&str>) -> String {
    let session_id = Utc::now().to_rfc3339();
    let _ = CURRENT_SESSION_ID.set(session_id.clone());

    // Collect all initialised pools so we can write a session row to each.
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

pub async fn record_trade_db(
    pool: &SqlitePool,
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
    if let Err(e) = sqlx::query(
        "INSERT INTO trades (ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason, session_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
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
    reason: &str,
    timestamp: Option<DateTime<Utc>>,
) -> bool {
    let ts = timestamp.unwrap_or_else(Utc::now).to_rfc3339();
    let sid = current_session_id();
    match sqlx::query(
        "INSERT INTO trades (ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason, session_id)
         SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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

pub async fn record_entry_db(
    pool: &SqlitePool,
    strategy: &str,
    token_id: &str,
    market: &str,
    side: &str,
    entry_price: Decimal,
    shares: Decimal,
) {
    let ts = Utc::now().to_rfc3339();
    let sid = current_session_id();
    if let Err(e) = sqlx::query(
        "INSERT INTO entries (ts, strategy, token_id, market, side, entry_price, shares, session_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(&ts)
    .bind(strategy)
    .bind(token_id)
    .bind(market)
    .bind(side)
    .bind(entry_price.to_string())
    .bind(shares.to_string())
    .bind(sid)
    .execute(pool)
    .await {
        error!("❌ DB entry write failed: {}", e);
    }
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

/// Serialisable snapshot of the compile-time constants in `config.rs`.
///
/// All fields are `String` (for Decimal) or primitive types so the struct is
/// trivially serialisable without bringing extra dependencies into `db.rs`.
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
            error!("❌ DB static_config_snapshot serialise failed: {}", e);
        }
    }
}

// ─── Open positions ──────────────────────────────────────────────────────────

/// Insert a row into `open_positions` when a new position is entered.
/// Called for every entry — both ghost mode and live — so the UI and LLM Advisor
/// can see in-flight positions that have not yet appeared as completed trades.
pub async fn record_open_position(
    pool: &SqlitePool,
    strategy: &str,
    token_id: &str,
    market: &str,
    side: &str,
    entry_price: Decimal,
    shares: Decimal,
    ghost_mode: bool,
) {
    record_open_position_with_status(pool, strategy, token_id, market, side, entry_price, shares, ghost_mode, "confirmed").await;
}

/// Record an open position with explicit status.
/// status: "pending" = Viper Launch (order placed, waiting chain confirmation)
///         "confirmed" = Mission In-Flight (on-chain confirmed)
pub async fn record_open_position_with_status(
    pool: &SqlitePool,
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
    // Use INSERT WHERE NOT EXISTS to prevent duplicate rows for the same token_id.
    // Without a UNIQUE constraint on token_id, `INSERT OR REPLACE` would always INSERT
    // a new row (never replacing), causing duplicate open_positions rows when the
    // strategy top-ups an existing position or when chain-sync has already adopted it.
    // If a row for this token already exists (chain-adopted or from a prior cycle),
    // we skip the insert — chain-sync will keep the shares count accurate via UPDATE.
    match sqlx::query(
        "INSERT INTO open_positions
         (ts, session_id, strategy, token_id, market, side, entry_price, shares, ghost_mode, status)
         SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
         WHERE NOT EXISTS (SELECT 1 FROM open_positions WHERE token_id = ? AND strategy = ?)"
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
    .bind(token_id)
    .bind(strategy)
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

/// Delete every `open_positions` row whose token_id is NOT in `live_token_ids`.
///
/// Called by the chain-sync task after it fetches the wallet's actual live positions
/// from the Polymarket Data API.  Any row left in the table after that is stale
/// (settled, sold, or from a crashed session that never called close_open_position).
pub async fn purge_stale_open_positions(
    pool: &SqlitePool,
    live_token_ids: &std::collections::HashSet<String>,
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

    let rows: Vec<(i64, String, Option<String>, String)> = match sqlx::query_as(
        "SELECT id, token_id, status, ts FROM open_positions"
    )
    .fetch_all(pool)
    .await {
        Ok(r)  => r,
        Err(e) => { error!("❌ DB purge_stale_open_positions fetch failed: {}", e); return 0; }
    };

    let now = Utc::now();
    let mut purged = 0usize;
    for (id, token_id, status, ts) in rows {
        // Still held on-chain (size > 0, not redeemable) — keep.
        if live_token_ids.contains(&token_id) {
            continue;
        }

        let is_pending = status.as_deref().unwrap_or("confirmed") == "pending";
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
        }
    }
    purged
}

/// Re-adopt a single on-chain position that is missing from `open_positions`.
///
/// Uses `INSERT ... WHERE NOT EXISTS` so it is safe to call repeatedly — it is a
/// no-op if a row for `token_id` already exists.  Returns `true` if a row was
/// inserted.
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
    if let Err(e) = sqlx::query(
        "UPDATE open_positions SET shares = ?, entry_price = ?, chain_adopted = 1, current_price = COALESCE(?, current_price) WHERE token_id = ?"
    )
    .bind(shares.to_string())
    .bind(avg_price.to_string())
    .bind(&cur_price_str)
    .bind(token_id)
    .execute(pool)
    .await {
        error!("❌ DB update_position_from_chain failed for {}: {}", token_id, e);
    }
}

/// Update only the current_price for an existing open position (called on every chain-sync).
///
/// Also flips `status` to 'confirmed': a position the Data API reports as a live on-chain
/// holding is, by definition, confirmed (not an un-indexed in-flight order). Without this,
/// a row first written as 'pending' by the strategy order path could stay 'pending'
/// indefinitely after its fill, making it permanently immune to purge_stale_open_positions.
pub async fn update_position_current_price(
    pool: &SqlitePool,
    token_id: &str,
    cur_price: rust_decimal::Decimal,
) {
    if let Err(e) = sqlx::query(
        "UPDATE open_positions SET current_price = ?, status = 'confirmed' WHERE token_id = ?"
    )
    .bind(cur_price.to_string())
    .bind(token_id)
    .execute(pool)
    .await {
        error!("❌ DB update_position_current_price failed for {}: {}", token_id, e);
    }
}

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
    match sqlx::query(
        "INSERT INTO open_positions
             (ts, session_id, strategy, token_id, market, side, entry_price, shares, ghost_mode, chain_adopted, current_price)
         SELECT ?, ?, 'ArbitrageStrategy', ?, ?, ?, ?, ?, 0, 1, ?
         WHERE NOT EXISTS (SELECT 1 FROM open_positions WHERE token_id = ?)"
    )
    .bind(&ts)
    .bind(sid)
    .bind(token_id)
    .bind(market)
    .bind(side)
    .bind(avg_price.to_string())
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

    match sqlx::query(
        "SELECT ts, session_pnl, collateral, total_value FROM pnl_snapshots WHERE ts >= ? ORDER BY ts DESC LIMIT ?"
    )
    .bind(&cutoff_str)
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

/// Return the most recent `limit` completed trades, newest first.
pub async fn get_recent_trades(pool: &SqlitePool, limit: i64) -> Vec<TradeRow> {
    match sqlx::query(
        "SELECT ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason
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
        })).collect(),
        Err(e) => { error!("❌ DB get_recent_trades failed: {}", e); vec![] }
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
         COALESCE(status, 'confirmed') as status, current_price
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
        })).collect(),
        Err(e) => { error!("❌ DB get_open_positions failed: {}", e); vec![] }
    }
}

/// Return only pending positions (Viper Launches) - orders placed but not yet confirmed on-chain.
pub async fn get_pending_positions(pool: &SqlitePool) -> Vec<OpenPositionRow> {
    match sqlx::query(
        "SELECT ts, strategy, token_id, market, side, entry_price, shares, ghost_mode, chain_adopted, status, current_price
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
        })).collect(),
        Err(e) => { error!("❌ DB get_pending_positions failed: {}", e); vec![] }
    }
}

/// Return only confirmed positions (Viper Missions In-Flight) - verified on-chain.
pub async fn get_confirmed_positions(pool: &SqlitePool) -> Vec<OpenPositionRow> {
    match sqlx::query(
        "SELECT ts, strategy, token_id, market, side, entry_price, shares, ghost_mode, chain_adopted,
         COALESCE(status, 'confirmed') as status, current_price
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
        "SELECT ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason
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
        "SELECT ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason
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

