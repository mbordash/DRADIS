/// Metrics utility for tracking bot performance and trade stats in CSV format.
/// Fully asynchronous and non-blocking for high-frequency trading.

use tokio::fs::{OpenOptions, create_dir_all};
use tokio::io::AsyncWriteExt;
use std::path::Path;
use rust_decimal::Decimal;
use chrono::{Utc, Datelike};
use tracing::{error, info};
use serde::Serialize;

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

    // Ensure logs directory exists
    let log_dir = "logs";
    if let Err(e) = create_dir_all(log_dir).await {
        error!("❌ Failed to create logs directory: {}", e);
        return;
    }

    let filename = format!("{}/trades_{:04}-{:02}-{:02}.csv", log_dir, now.year(), now.month(), now.day());
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
