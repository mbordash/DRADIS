/// Metrics utility for tracking bot performance and trade stats.
/// Database-only architecture — all reads and writes use SQLite.
/// Fully asynchronous and non-blocking for high-frequency trading.

use rust_decimal::Decimal;
use chrono::{DateTime, Utc};
use tracing::info;
use crate::helpers::db;

/// Records a completed trade to the SQLite database.
///
/// `asset` — lowercase crypto symbol, e.g. `"btc"`.  Drives the SQLite pool selection.
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
    if let Some(pool) = db::pool_for(asset) {
        db::record_trade_db(&pool, &strategy, &market, &side, entry_price, exit_price, shares, profit_usdc, &reason, timestamp).await;
        info!("📊 Trade recorded to database: {} {} {}", strategy, market, side);
    }
}

/// Records a position entry event to the database for recovery after bot restarts.
///
/// `asset` — lowercase crypto symbol, e.g. `"btc"`.  Drives SQLite pool selection.
/// `token_id` — stored as decimal string representation (same as U256::to_string()).
pub async fn record_entry(
    asset: &str,
    strategy: String,
    token_id: String,
    market: String,
    side: String,
    entry_price: Decimal,
    shares: Decimal,
) {
    if let Some(pool) = db::pool_for(asset) {
        db::record_entry_db(&pool, &strategy, &token_id, &market, &side, entry_price, shares).await;
    }
}

/// Looks up entry data from the database for the given token_id (decimal string).
/// Returns `(entry_price, strategy_name)`, or None if no record exists.
///
/// Checks open_positions table first (highest authority), then falls back to entries table.
/// Used by `reconcile_orphaned_positions` to recover entry prices and assign positions
/// to the correct strategy after bot restarts.
pub async fn lookup_entry_from_csv(token_id_str: &str) -> Option<(Decimal, String)> {
    // ── open_positions table (highest authority) ──────────────────────────────
    if let Some(pool) = db::pool() {
        if let Some((price, strategy)) = db::lookup_open_position_strategy(pool, token_id_str).await {
            info!("📦 DB: found open_position strategy={} entry_price={} for token {}", strategy, price, token_id_str);
            return Some((price, strategy));
        }
    }

    // ── entries table (fallback) ──────────────────────────────────────────────
    if let Some(pool) = db::pool() {
        if let Some((price, strategy)) = db::lookup_entry_db(pool, token_id_str).await {
            info!("📦 DB: found entry_price={} strategy={} for token {}", price, strategy, token_id_str);
            return Some((price, strategy));
        }
    }

    None
}

/// Convenience wrapper — returns only the entry price (for callers that don't need the strategy).
pub async fn lookup_entry_price_from_csv(token_id_str: &str) -> Option<Decimal> {
    lookup_entry_from_csv(token_id_str).await.map(|(price, _)| price)
}

