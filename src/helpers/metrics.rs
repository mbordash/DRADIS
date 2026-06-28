/// Metrics utility for tracking bot performance and trade stats.
/// Database-only architecture — all reads and writes use SQLite.
/// Fully asynchronous and non-blocking for high-frequency trading.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use chrono::{DateTime, Utc};
use tracing::info;
use crate::helpers::db;
use crate::state::MarketSnapshot;

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

/// Captures the entry-time signal feature-vector and persists it to `entry_signals`,
/// so trade outcomes can later be correlated with the conditions that produced them.
///
/// `snap` is the venue-appropriate orderbook/oracle snapshot the strategy evaluated
/// (maker snapshot for Window/Daily strategies, hourly snapshot otherwise).
#[allow(clippy::too_many_arguments)]
pub async fn record_entry_signal(
    asset: &str,
    strategy: String,
    token_id: String,
    market: String,
    side: String,
    entry_price: Decimal,
    shares: Decimal,
    snap: &MarketSnapshot,
) {
    if let Some(pool) = db::pool_for(asset) {
        // Order-book imbalance for the YES token: (bid_depth − ask_depth) / total.
        // Zero when depth is unavailable (avoids divide-by-zero).
        let yes_depth = snap.yes_bid_depth + snap.yes_ask_depth;
        let obi_yes = if yes_depth > dec!(0) {
            (snap.yes_bid_depth - snap.yes_ask_depth) / yes_depth
        } else {
            dec!(0)
        };
        let row = db::EntrySignalRow {
            strategy,
            token_id,
            market,
            side,
            entry_price,
            shares,
            oracle_price:        snap.oracle_price,
            drift_10m:           snap.oracle_drift_10m,
            drift_60m:           snap.oracle_drift_60m,
            obi_yes,
            ask_sum:             snap.yes_ask + snap.no_ask,
            bid_sum:             snap.yes_bid + snap.no_bid,
            funding_rate:        snap.funding_rate,
            institutional_pulse: snap.institutional_pulse,
            cvd_ratio:           snap.cvd_ratio,
            oi_delta_pct:        snap.oi_delta_pct,
            velocity:            snap.velocity,
            secs_to_expiry:      snap.secs_to_expiry,
        };
        db::record_entry_signal_db(&pool, &row).await;
    }
}

/// Looks up entry data from the database for the given token_id (decimal string).
/// Returns `(entry_price, strategy_name)`, or None if no record exists.
///
/// Checks open_positions table first (highest authority), then falls back to entries table.
/// Searches ALL asset-specific DBs (btc, eth, sol) so that ETH/SOL position records
/// are found correctly — not just the primary (BTC) pool.
/// Used by `reconcile_orphaned_positions` to recover entry prices and assign positions
/// to the correct strategy after bot restarts.
pub async fn lookup_entry_from_csv(token_id_str: &str) -> Option<(Decimal, String)> {
    // ── open_positions table: search ALL asset DBs (highest authority) ────────
    // Bug fix (2026-06-12): previously only checked db::pool() which is the primary
    // (BTC) pool.  ETH/SOL open_position records were never found, causing all
    // reconciled ETH/SOL positions to fall back to "discount@25%" and be wrongly
    // attributed to MomentumStrategy instead of the originating strategy.
    for asset in db::available_assets() {
        if let Some(pool) = db::pool_for(&asset) {
            if let Some((price, strategy)) = db::lookup_open_position_strategy(&pool, token_id_str).await {
                info!("📦 DB: found open_position strategy={} entry_price={} for token {} (asset={})", strategy, price, token_id_str, asset);
                return Some((price, strategy));
            }
        }
    }

    // ── entries table: search ALL asset DBs (fallback) ───────────────────────
    for asset in db::available_assets() {
        if let Some(pool) = db::pool_for(&asset) {
            if let Some((price, strategy)) = db::lookup_entry_db(&pool, token_id_str).await {
                info!("📦 DB: found entry_price={} strategy={} for token {} (asset={})", price, strategy, token_id_str, asset);
                return Some((price, strategy));
            }
        }
    }

    None
}

/// Convenience wrapper — returns only the entry price (for callers that don't need the strategy).
pub async fn lookup_entry_price_from_csv(token_id_str: &str) -> Option<Decimal> {
    lookup_entry_from_csv(token_id_str).await.map(|(price, _)| price)
}

