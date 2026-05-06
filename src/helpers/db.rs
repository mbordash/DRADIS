/// SQLite persistence layer for DRADIS.
///
/// Provides:
///   - Async connection pool (one shared pool via OnceLock)
///   - Schema initialization (trades, entries, pnl_snapshots, config)
///   - Write helpers for trades, entries, and P&L snapshots
///   - Key-value store for DynamicConfig JSON blobs
///   - Lookup helper for entry price recovery (faster than CSV scan)
///
/// Call `db::init("logs/dradis.db")` once at startup before any other DB calls.
/// All other functions silently no-op if the pool is not yet initialized.

use std::sync::OnceLock;
use sqlx::{SqlitePool, sqlite::SqlitePoolOptions, Row};
use rust_decimal::Decimal;
use chrono::Utc;
use anyhow::Result;
use serde::Serialize;
use tracing::{error, info};

// ─── Shared pool ────────────────────────────────────────────────────────────

static DB_POOL: OnceLock<SqlitePool> = OnceLock::new();

/// Initialize the SQLite connection pool and create schema.
/// Must be called once at startup; subsequent calls are ignored.
pub async fn init(path: &str) -> Result<()> {
    let url = format!("sqlite://{}?mode=rwc", path);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?;
    init_schema(&pool).await?;
    DB_POOL.set(pool).map_err(|_| anyhow::anyhow!("DB pool already initialized"))?;
    info!("📦 SQLite initialized: {}", path);
    Ok(())
}

/// Returns a reference to the shared pool, or None if not yet initialized.
pub fn pool() -> Option<&'static SqlitePool> {
    DB_POOL.get()
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
            shares      TEXT    NOT NULL
        )"
    ).execute(pool).await?;

    // pnl_snapshots: periodic P&L checkpoints for the Control Tower chart
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pnl_snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT    NOT NULL,
            session_pnl TEXT    NOT NULL,
            collateral  TEXT    NOT NULL
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

    Ok(())
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
) {
    let ts = Utc::now().to_rfc3339();
    if let Err(e) = sqlx::query(
        "INSERT INTO trades (ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
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
    .execute(pool)
    .await {
        error!("❌ DB trade write failed: {}", e);
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
    if let Err(e) = sqlx::query(
        "INSERT INTO entries (ts, strategy, token_id, market, side, entry_price, shares)
         VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(&ts)
    .bind(strategy)
    .bind(token_id)
    .bind(market)
    .bind(side)
    .bind(entry_price.to_string())
    .bind(shares.to_string())
    .execute(pool)
    .await {
        error!("❌ DB entry write failed: {}", e);
    }
}

/// Look up the most recent entry price for a token_id.
/// Primary path for reconcile_orphaned_positions — faster than CSV scan.
pub async fn lookup_entry_price_db(pool: &SqlitePool, token_id_str: &str) -> Option<Decimal> {
    let row = sqlx::query(
        "SELECT entry_price FROM entries WHERE token_id = ? ORDER BY ts DESC LIMIT 1"
    )
    .bind(token_id_str)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()?;

    row.try_get::<String, _>(0)
        .ok()
        .and_then(|s| s.parse::<Decimal>().ok())
}

// ─── P&L snapshot ────────────────────────────────────────────────────────────

/// Persist a P&L checkpoint (called by the status ticker in main.rs).
/// Provides the time-series data the Control Tower chart will query.
pub async fn record_pnl_snapshot(pool: &SqlitePool, session_pnl: Decimal, collateral: Decimal) {
    let ts = Utc::now().to_rfc3339();
    if let Err(e) = sqlx::query(
        "INSERT INTO pnl_snapshots (ts, session_pnl, collateral) VALUES (?, ?, ?)"
    )
    .bind(&ts)
    .bind(session_pnl.to_string())
    .bind(collateral.to_string())
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

// ─── API read models ─────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct PnlSnapshotRow {
    pub ts: String,
    pub session_pnl: String,
    pub collateral: String,
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

/// Return the most recent `limit` P&L snapshots, newest first.
pub async fn get_pnl_history(pool: &SqlitePool, limit: i64) -> Vec<PnlSnapshotRow> {
    match sqlx::query(
        "SELECT ts, session_pnl, collateral FROM pnl_snapshots ORDER BY ts DESC LIMIT ?"
    )
    .bind(limit)
    .fetch_all(pool)
    .await {
        Ok(rows) => rows.into_iter().filter_map(|r| Some(PnlSnapshotRow {
            ts:          r.try_get::<String, _>(0).ok()?,
            session_pnl: r.try_get::<String, _>(1).ok()?,
            collateral:  r.try_get::<String, _>(2).ok()?,
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

