// Strategy modules for DRADIS
//
// Architecture (orchestrator-based):
//   - momentum_impl, arbitrage_impl, time_decay_impl, maker_impl, basis_impl (implement Strategy trait)

pub mod momentum_impl;
pub mod arbitrage_impl;
pub mod time_decay_impl;
pub mod maker_impl;
pub mod basis_impl;
pub mod gboost_impl;

use rust_decimal::Decimal;
use crate::config;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

static LAST_DRAWDOWN_REJECT_LOG: AtomicU64 = AtomicU64::new(0);

/// Shared risk utility for all strategies to check global drawdown.
pub fn is_drawdown_limit_hit(session_pnl: Decimal, starting_collateral: Decimal) -> bool {
    let max_dd = config::max_session_drawdown(starting_collateral);
    if session_pnl <= -max_dd {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        let last = LAST_DRAWDOWN_REJECT_LOG.load(Ordering::Relaxed);
        if now >= last + 60 { // Rate limit: 1 minute
            LAST_DRAWDOWN_REJECT_LOG.store(now, Ordering::Relaxed);
            info!("🛡️ Risk Reject: Global Session Drawdown ${:.2} >= Max ${:.2}", session_pnl.abs(), max_dd);
        }
        return true;
    }
    false
}
