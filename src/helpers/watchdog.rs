//! Lock-free activity breadcrumb for diagnosing runtime freezes.
//!
//! When the tokio runtime fully deadlocks (e.g. a `std::sync::Mutex`/`RwLock`
//! contended across the 2 worker threads, or a CPU-bound spin), EVERY async task
//! goes silent — heartbeat, timeouts, `select!` arms and all logging. The OS-thread
//! watchdog (see `main.rs`) is the only thing still alive, but until now it could
//! only report "silent for Ns" without saying WHAT froze, leaving us blind.
//!
//! This module exposes three plain atomics that any code path can update with a
//! single relaxed store — cheap enough for the hot tick loop and, crucially, safe
//! to read from the native watchdog thread even while the runtime is wedged on a
//! `std::sync` primitive (the atomics themselves never lock). On a stall the
//! watchdog dumps the last phase, how long we have been in it, and a monotonic
//! sequence number, which pinpoints the frozen operation.

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Coarse phase of the trading loop. Kept as a `u8` so it is a lock-free atomic.
#[repr(u8)]
#[derive(Clone, Copy, Debug)]
pub enum Phase {
    /// Waiting in the patrol `select!` (no work in flight) — a stall here is external.
    Idle = 0,
    /// Evaluating strategy signals for a tick (see `detail` for the specific viper).
    SignalEval = 1,
    /// GBoost retrain trigger — sample collection + lock acquisition on the loop
    /// thread, before the `spawn_blocking` fit. The #1 historical stall suspect.
    GboostRetrain = 2,
    /// Placing an order (CLOB round-trip).
    OrderPlace = 3,
    /// Cancelling resting orders.
    OrderCancel = 4,
    /// Market rotation / trading-loop restart.
    MarketRotate = 5,
    /// Chain-sync of open_positions against on-chain holdings.
    ChainSync = 6,
    /// Auto-settlement / on-chain redemption.
    Settlement = 7,
    /// Periodic cleanup / orphan reconciliation.
    Cleanup = 8,
    /// WebSocket (re)connect.
    WsReconnect = 9,
    Other = 255,
}

impl Phase {
    fn name(code: u8) -> &'static str {
        match code {
            0 => "IDLE",
            1 => "SIGNAL_EVAL",
            2 => "GBOOST_RETRAIN",
            3 => "ORDER_PLACE",
            4 => "ORDER_CANCEL",
            5 => "MARKET_ROTATE",
            6 => "CHAIN_SYNC",
            7 => "SETTLEMENT",
            8 => "CLEANUP",
            9 => "WS_RECONNECT",
            _ => "OTHER",
        }
    }
}

static CURRENT_PHASE: AtomicU8 = AtomicU8::new(Phase::Idle as u8);
static PHASE_SINCE_SECS: AtomicU64 = AtomicU64::new(0);
static PHASE_SEQ: AtomicU64 = AtomicU64::new(0);
/// Optional sub-detail (e.g. the strategy index during `SignalEval`). 255 = none.
static DETAIL: AtomicU8 = AtomicU8::new(255);

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Mark the start of a phase. Lock-free; safe to call from anywhere, including
/// code that already holds `std::sync` locks. Clears any stale detail tag.
#[inline]
pub fn enter(phase: Phase) {
    CURRENT_PHASE.store(phase as u8, Ordering::Relaxed);
    PHASE_SINCE_SECS.store(now_secs(), Ordering::Relaxed);
    DETAIL.store(255, Ordering::Relaxed);
    PHASE_SEQ.fetch_add(1, Ordering::Relaxed);
}

/// Attach a sub-detail to the current phase (e.g. which strategy is evaluating).
/// Rendered as `PHASE/detail` in the stall dump.
#[inline]
pub fn set_detail(detail: u8) {
    DETAIL.store(detail, Ordering::Relaxed);
}

/// Convenience: enter `SignalEval` with a strategy detail in one shot (ordered so
/// the detail is never briefly cleared by a racing watchdog read).
#[inline]
pub fn enter_eval(detail: u8) {
    CURRENT_PHASE.store(Phase::SignalEval as u8, Ordering::Relaxed);
    PHASE_SINCE_SECS.store(now_secs(), Ordering::Relaxed);
    DETAIL.store(detail, Ordering::Relaxed);
    PHASE_SEQ.fetch_add(1, Ordering::Relaxed);
}

/// Map a strategy struct name (e.g. "GboostStrategy") to its `SignalEval` detail
/// code. Kept in sync with `detail_name`. Unknown names → 255 (no detail).
pub fn signal_detail_for(strategy_name: &str) -> u8 {
    let n = strategy_name.to_ascii_lowercase();
    if n.contains("momentum") { 0 }
    else if n.contains("arbitrage") { 1 }
    else if n.contains("timedecay") || n.contains("time_decay") { 2 }
    else if n.contains("maker") { 3 }
    else if n.contains("basis") { 4 }
    else if n.contains("gboost") { 5 }
    else if n.contains("trendreversal") || n.contains("trend_reversal") { 6 }
    else if n.contains("convergence") { 7 }
    else if n.contains("trendcapture") || n.contains("trend_capture") { 8 }
    else { 255 }
}

/// Human-readable label for a `SignalEval` detail code. Extend as vipers change.
fn detail_name(d: u8) -> Option<&'static str> {
    match d {
        0 => Some("momentum"),
        1 => Some("arbitrage"),
        2 => Some("time_decay"),
        3 => Some("maker"),
        4 => Some("basis"),
        5 => Some("gboost"),
        6 => Some("trend_reversal"),
        7 => Some("convergence"),
        8 => Some("trend_capture"),
        _ => None,
    }
}

/// Lock-free snapshot for the OS watchdog: `(phase_label, seconds_in_phase, seq)`.
/// `phase_label` includes the sub-detail when present (e.g. `SIGNAL_EVAL/gboost`).
pub fn snapshot() -> (String, u64, u64) {
    let code = CURRENT_PHASE.load(Ordering::Relaxed);
    let since = PHASE_SINCE_SECS.load(Ordering::Relaxed);
    let seq = PHASE_SEQ.load(Ordering::Relaxed);
    let detail = DETAIL.load(Ordering::Relaxed);
    let secs = now_secs().saturating_sub(since);
    let label = match detail_name(detail) {
        Some(d) => format!("{}/{}", Phase::name(code), d),
        None => Phase::name(code).to_string(),
    };
    (label, secs, seq)
}
