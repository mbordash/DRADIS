/// Metrics utility for tracking bot performance and trade stats in CSV format.
/// Fully asynchronous and non-blocking for high-frequency trading.

use tokio::fs::{OpenOptions, create_dir_all};
use tokio::io::AsyncWriteExt;
use std::path::Path;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromStr;
use chrono::{Utc, Datelike, Duration as ChronoDuration};
use chrono_tz::US::Eastern;
use tracing::{error, info, warn};
use serde::Serialize;
use std::env;

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
/// Prefixes filename with CRYPTO_FILTER to avoid collision in multi-container setups.
pub async fn record_trade(
    strategy: String,
    market: String,
    side: String,
    entry_price: Decimal,
    exit_price: Decimal,
    shares: Decimal,
    profit_usdc: Decimal,
    reason: String,
) {
    let now = Utc::now();
    // Use Eastern date for the filename so the daily log file matches
    // Polymarket's ET-based trading day (avoids the file rolling at 8PM ET / midnight UTC).
    let now_et = now.with_timezone(&Eastern);
    let crypto = env::var("CRYPTO_FILTER").unwrap_or_else(|_| "unknown".to_string()).to_lowercase();

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
}

/// Records a position entry event to a daily entry-log CSV so that `reconcile_orphaned_positions`
/// can recover real entry prices after a bot restart instead of falling back to the discount heuristic.
///
/// File: logs/{crypto}-entries_{ET-date}.csv
/// Columns: timestamp, strategy, token_id, market, side, entry_price, shares
///
/// token_id is stored as its decimal string representation (same as U256::to_string()).
pub async fn record_entry(
    strategy: String,
    token_id: String,
    market: String,
    side: String,
    entry_price: Decimal,
    shares: Decimal,
) {
    let now = Utc::now();
    let now_et = now.with_timezone(&Eastern);
    let crypto = env::var("CRYPTO_FILTER").unwrap_or_else(|_| "unknown".to_string()).to_lowercase();

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
}

/// Scans the last two ET-days of entry logs for the given token_id (decimal string).
/// Returns the most recent recorded entry_price, or None if no log entry exists.
///
/// Called by `reconcile_orphaned_positions` to recover real entry prices after a restart.
pub async fn lookup_entry_price_from_csv(token_id_str: &str) -> Option<Decimal> {
    let crypto = env::var("CRYPTO_FILTER").unwrap_or_else(|_| "unknown".to_string()).to_lowercase();
    let now_et = Utc::now().with_timezone(&Eastern);

    // Check today and yesterday in ET so a restart around midnight doesn't miss the entry.
    let dates = [
        now_et.date_naive(),
        (now_et - ChronoDuration::days(1)).date_naive(),
    ];

    let mut best_ts: Option<String> = None;
    let mut best_price: Option<Decimal> = None;

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
            let ts    = cols[0].trim_matches('"');
            let tid   = cols[2].trim_matches('"');
            let price_str = cols[5].trim_matches('"');
            if tid == token_id_str {
                if let Ok(price) = Decimal::from_str(price_str) {
                    // Keep the most recent timestamp (ISO 8601 string comparison works lexicographically).
                    if best_ts.is_none() || ts > best_ts.as_deref().unwrap_or("") {
                        best_ts = Some(ts.to_string());
                        best_price = Some(price);
                    }
                }
            }
        }
    }

    if best_price.is_some() {
        info!("📂 Entry log: found real entry_price={} for token {}", best_price.unwrap(), token_id_str);
    } else {
        warn!("📂 Entry log: no record found for token {} — will use discount heuristic", token_id_str);
    }
    best_price
}

