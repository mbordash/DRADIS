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
pub mod trendcapture_impl;
pub mod convergence_impl;

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

/// Shared liquidity/timing entry gate for directional vipers (2026-06-29).
///
/// Returns `Some(reason)` when a new entry should be BLOCKED because the book is
/// likely to gap straight through the stop, or `None` when entry is allowed.
///
/// Two checks:
///   1. **Near-resolution**: block when fewer than `ENTRY_MIN_SECS_TO_RESOLUTION`
///      seconds remain before the market resolves (books gap violently into close).
///   2. **Exit-side depth**: block when the resting depth on the side we would sell
///      into to exit (`exit_bid_depth`, in shares) is less than our intended
///      position size × `ENTRY_MIN_EXIT_BID_DEPTH_RATIO`. If we are larger than the
///      resting bid, a stop walks an empty book and gaps through.
///
/// `secs_left` is `None` for open-ended (no close time) markets, which skip check 1.
pub fn entry_liquidity_gate(
    secs_left: Option<i64>,
    intended_shares: Decimal,
    exit_bid_depth: Decimal,
) -> Option<String> {
    if let Some(s) = secs_left {
        if s < config::ENTRY_MIN_SECS_TO_RESOLUTION {
            return Some(format!(
                "near-resolution ({}s < {}s) — gap-through risk",
                s, config::ENTRY_MIN_SECS_TO_RESOLUTION
            ));
        }
    }
    let required = intended_shares * config::ENTRY_MIN_EXIT_BID_DEPTH_RATIO;
    if exit_bid_depth < required {
        return Some(format!(
            "thin exit book (bid_depth={:.1}sh < required {:.1}sh for {:.1}sh position) — stop would gap through",
            exit_bid_depth, required, intended_shares
        ));
    }
    None
}
