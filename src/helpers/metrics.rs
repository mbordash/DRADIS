/// Metrics utility for tracking bot performance and trade stats.
/// Dual-writes to both the legacy CSV files and the SQLite database.
/// Fully asynchronous and non-blocking for high-frequency trading.

use tokio::fs::{OpenOptions, create_dir_all};
use tokio::io::AsyncWriteExt;
use std::path::Path;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromStr;
use chrono::{Utc, DateTime, Datelike, Duration as ChronoDuration};
use chrono_tz::US::Eastern;
use tracing::{error, info, warn};
use serde::Serialize;
use std::env;
use crate::helpers::db;

#[derive(Serialize)]
pub struct TradeRecord {
    pub timestamp: String,
    pub strategy: String,
    pub market: String,
    pub side: String,
    pub entry_price: Decimal,
    pub exit_price: Decimal,
    pub shares: Decimal,
    pub profit_usdc: Decimal,
    pub reason: String,
}

/// Records a completed trade to a daily CSV file asynchronously in a 'logs' directory.
/// Prefixes filename with the asset symbol to avoid collision in multi-asset setups.
///
/// `asset` — lowercase crypto symbol, e.g. `"btc"`.  Drives both the CSV filename
/// prefix and the SQLite pool selection so trades land in the correct asset DB.
pub async fn record_trade(
    asset: &str,
    strategy: String,
    market: String,
    side: String,
    entry_price: Decimal,
    exit_price: Decimal,
    shares: Decimal,
    profit_usdc: Decimal,
    reason: String,
) {
    record_trade_with_timestamp(asset, strategy, market, side, entry_price, exit_price, shares, profit_usdc, reason, None).await;
}

/// Record a trade with an explicit timestamp (for retrospective settlements).
/// If `timestamp` is None, uses current time.
pub async fn record_trade_with_timestamp(
    asset: &str,
    strategy: String,
    market: String,
    side: String,
    entry_price: Decimal,
    exit_price: Decimal,
    shares: Decimal,
    profit_usdc: Decimal,
    reason: String,
    timestamp: Option<DateTime<Utc>>,
) {
    let db_strategy = strategy.clone();
    let db_market   = market.clone();
    let db_side     = side.clone();
    let db_reason   = reason.clone();
    let db_asset    = asset.to_string();
    let now = timestamp.unwrap_or_else(|| Utc::now());
    // Use Eastern date for the filename so the daily log file matches
    // Polymarket's ET-based trading day (avoids the file rolling at 8PM ET / midnight UTC).
    let now_et = now.with_timezone(&Eastern);
    let crypto = asset.to_lowercase();

    // Ensure logs directory exists
    let log_dir = "logs";
    if let Err(e) = create_dir_all(log_dir).await {
        error!("❌ Failed to create logs directory: {}", e);
        return;
    }

    // Filename: logs/btc-trades_2024-04-26.csv  (date in ET)
    let filename = format!("{}/{}-trades_{:04}-{:02}-{:02}.csv", log_dir, crypto, now_et.year(), now_et.month(), now_et.day());
    let path = Path::new(&filename);

    let file_exists = path.exists();

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await;

    match file {
        Ok(mut f) => {
            if !file_exists {
                let header = "timestamp,strategy,market,side,entry_price,exit_price,shares,profit_usdc,reason\n";
                if let Err(e) = f.write_all(header.as_bytes()).await {
                    error!("❌ Failed to write CSV header: {}", e);
                    return;
                }
            }

            let record = TradeRecord {
                timestamp: now.to_rfc3339(),
                strategy,
                market,
                side,
                entry_price,
                exit_price,
                shares,
                profit_usdc,
                reason,
            };

            let row = format!(
                "\"{}\",\"{}\",\"{}\",\"{}\",{},{},{},{},\"{}\"\n",
                record.timestamp,
                record.strategy,
                record.market,
                record.side,
                record.entry_price,
                record.exit_price,
                record.shares,
                record.profit_usdc,
                record.reason
            );

            if let Err(e) = f.write_all(row.as_bytes()).await {
                error!("❌ Failed to write CSV row: {}", e);
            } else {
                let _ = f.flush().await;
                info!("📊 Trade recorded to {}", filename);
            }
        }
        Err(e) => error!("❌ Failed to open/create CSV file {}: {}", filename, e),
    }

    // ── SQLite dual-write ────────────────────────────────────────────────────
    if let Some(pool) = db::pool_for(&db_asset) {
        db::record_trade_db(&pool, &db_strategy, &db_market, &db_side, entry_price, exit_price, shares, profit_usdc, &db_reason).await;
    }
}

/// Records a position entry event to a daily entry-log CSV so that `reconcile_orphaned_positions`
/// can recover real entry prices after a bot restart instead of falling back to the discount heuristic.
///
/// `asset` — lowercase crypto symbol, e.g. `"btc"`.  Drives CSV filename prefix
/// and SQLite pool selection.
///
/// File: logs/{asset}-entries_{ET-date}.csv
/// Columns: timestamp, strategy, token_id, market, side, entry_price, shares
///
/// token_id is stored as its decimal string representation (same as U256::to_string()).
pub async fn record_entry(
    asset: &str,
    strategy: String,
    token_id: String,
    market: String,
    side: String,
    entry_price: Decimal,
    shares: Decimal,
) {
    let now = Utc::now();
    let now_et = now.with_timezone(&Eastern);
    let crypto = asset.to_lowercase();
    let db_asset_e = asset.to_string();

    let log_dir = "logs";
    if let Err(e) = create_dir_all(log_dir).await {
        error!("❌ Failed to create logs directory: {}", e);
        return;
    }

    let filename = format!("{}/{}-entries_{:04}-{:02}-{:02}.csv",
        log_dir, crypto, now_et.year(), now_et.month(), now_et.day());
    let path = Path::new(&filename);
    let file_exists = path.exists();

    match OpenOptions::new().create(true).append(true).open(path).await {
        Ok(mut f) => {
            if !file_exists {
                let header = "timestamp,strategy,token_id,market,side,entry_price,shares\n";
                if let Err(e) = f.write_all(header.as_bytes()).await {
                    error!("❌ Failed to write entry CSV header: {}", e);
                    return;
                }
            }
            let row = format!(
                "\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",{},{}\n",
                now.to_rfc3339(), strategy, token_id, market, side, entry_price, shares
            );
            if let Err(e) = f.write_all(row.as_bytes()).await {
                error!("❌ Failed to write entry CSV row: {}", e);
            } else {
                let _ = f.flush().await;
            }
        }
        Err(e) => error!("❌ Failed to open/create entry CSV {}: {}", filename, e),
    }

    // ── SQLite dual-write ────────────────────────────────────────────────────
    if let Some(pool) = db::pool_for(&db_asset_e) {
        db::record_entry_db(&pool, &strategy, &token_id, &market, &side, entry_price, shares).await;
    }
}

/// Scans the last two ET-days of entry logs for the given token_id (decimal string).
/// Returns `(entry_price, strategy_name)`, or None if no log entry exists.
///
/// SQLite is checked first (O(log n) index lookup); falls back to CSV scan
/// for entries made before the DB was introduced.
///
/// Returning the originating strategy lets `reconcile_orphaned_positions` in balance.rs
/// re-assign a restarted position to the CORRECT strategy instead of defaulting to
/// whichever strategy happens to be first in the registry adoption order.
pub async fn lookup_entry_from_csv(token_id_str: &str) -> Option<(Decimal, String)> {
    // ── open_positions table (highest authority) ──────────────────────────────
    // Try the primary pool; for multi-asset the cleanup task routes DB reads
    // through the asset-specific pool, but startup reconciliation uses the primary.
    if let Some(pool) = db::pool() {
        if let Some((price, strategy)) = db::lookup_open_position_strategy(pool, token_id_str).await {
            info!("📦 DB: found open_position strategy={} entry_price={} for token {}", strategy, price, token_id_str);
            return Some((price, strategy));
        }
    }

    // ── entries table (secondary / fallback) ─────────────────────────────────
    if let Some(pool) = db::pool() {
        if let Some((price, strategy)) = db::lookup_entry_db(pool, token_id_str).await {
            info!("📦 DB: found entry_price={} strategy={} for token {}", price, strategy, token_id_str);
            return Some((price, strategy));
        }
    }

    // ── CSV fallback (legacy entries pre-SQLite) ──────────────────────────────
    // Use the primary asset name from available_assets() or fall back to CRYPTO_FILTER.
    let crypto = db::available_assets().into_iter().next()
        .or_else(|| env::var("CRYPTO_FILTER").ok())
        .unwrap_or_else(|| "unknown".to_string())
        .to_lowercase();
    let now_et = Utc::now().with_timezone(&Eastern);

    // Check today and yesterday in ET so a restart around midnight doesn't miss the entry.
    let dates = [
        now_et.date_naive(),
        (now_et - ChronoDuration::days(1)).date_naive(),
    ];

    let mut best_ts: Option<String> = None;
    let mut best_price: Option<Decimal> = None;
    let mut best_strategy: Option<String> = None;

    for date in &dates {
        let filename = format!("logs/{}-entries_{:04}-{:02}-{:02}.csv",
            crypto, date.year(), date.month(), date.day());
        let contents = match tokio::fs::read_to_string(&filename).await {
            Ok(c) => c,
            Err(_) => continue, // file doesn't exist — skip
        };
        for line in contents.lines().skip(1) { // skip header
            // Expected columns: timestamp,strategy,token_id,market,side,entry_price,shares
            // Fields are quoted; split on `","` to handle commas inside market names.
            let cols: Vec<&str> = line.splitn(7, ',').collect();
            if cols.len() < 6 { continue; }
            let ts           = cols[0].trim_matches('"');
            let strategy_col = cols[1].trim_matches('"');
            let tid          = cols[2].trim_matches('"');
            let price_str    = cols[5].trim_matches('"');
            if tid == token_id_str {
                if let Ok(price) = Decimal::from_str(price_str) {
                    // Keep the most recent timestamp (ISO 8601 string comparison works lexicographically).
                    if best_ts.is_none() || ts > best_ts.as_deref().unwrap_or("") {
                        best_ts = Some(ts.to_string());
                        best_price = Some(price);
                        best_strategy = Some(strategy_col.to_string());
                    }
                }
            }
        }
    }

    if let Some(price) = best_price {
        let strategy = best_strategy.unwrap_or_default();
        info!("📂 Entry log: found entry_price={} strategy={} for token {}", price, strategy, token_id_str);
        Some((price, strategy))
    } else {
        warn!("📂 Entry log: no record found for token {} — will use discount heuristic", token_id_str);
        None
    }
}

/// Convenience wrapper — returns only the entry price (for callers that don't need the strategy).
pub async fn lookup_entry_price_from_csv(token_id_str: &str) -> Option<Decimal> {
    lookup_entry_from_csv(token_id_str).await.map(|(price, _)| price)
}

