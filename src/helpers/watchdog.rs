// SPDX-License-Identifier: AGPL-3.0-only
//
// DRADIS — autonomous trading engine for crypto prediction markets.
// Copyright (C) 2026 Michael Bordash
//
// This file is part of DRADIS. DRADIS is free software: you can redistribute it
// and/or modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.
//
// DRADIS is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
// A PARTICULAR PURPOSE. See the GNU Affero General Public License for details.
//
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

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

/// Setup-step detail codes for the `SignalEval` section that runs BEFORE the
/// executor is reached.
///
/// A stall inside a viper renders as `SIGNAL_EVAL/gboost` because the executor
/// tags the running strategy. A stall in the snapshot-and-context section ahead
/// of it rendered as a bare `SIGNAL_EVAL`, which told us only that the freeze
/// was somewhere in 150 lines containing one std `RwLock` read and five tokio
/// mutex acquisitions. These name the individual step, so the next occurrence
/// identifies the exact primitive instead of the region.
pub const STEP_BOOK_SNAPSHOT: u8 = 100;
pub const STEP_CONFIG_READ: u8 = 101;
pub const STEP_PNL_LOCK: u8 = 102;
pub const STEP_COLLATERAL_LOCK: u8 = 103;
pub const STEP_POSITIONS_LOCK: u8 = 104;
pub const STEP_QUOTE_EPOCHS_LOCK: u8 = 105;
pub const STEP_CTX_BUILD: u8 = 106;

/// Human-readable label for a `SignalEval` detail code. Extend as vipers change.
fn detail_name(d: u8) -> Option<&'static str> {
    match d {
        STEP_BOOK_SNAPSHOT => Some("setup:book_snapshot"),
        STEP_CONFIG_READ => Some("setup:config_read"),
        STEP_PNL_LOCK => Some("setup:pnl_lock"),
        STEP_COLLATERAL_LOCK => Some("setup:collateral_lock"),
        STEP_POSITIONS_LOCK => Some("setup:positions_lock"),
        STEP_QUOTE_EPOCHS_LOCK => Some("setup:quote_epochs_lock"),
        STEP_CTX_BUILD => Some("setup:ctx_build"),
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

/// Best-effort dump of what every thread in this process is doing.
///
/// Called by the OS watchdog when the trading loop has gone silent. The point is
/// that the phase breadcrumb names the region, but not who is holding the lock —
/// on 2026-08-25 the intl venue froze in `SIGNAL_EVAL` for 358s and the process
/// was killed with no way to tell which primitive was held or by which task.
///
/// Everything here is deliberately non-blocking and failure-tolerant: it runs
/// inside a process that is already wedged, and a diagnostic that can itself
/// hang would turn a recoverable restart into a hang. Nothing acquires a lock
/// the trading loop could be holding.
pub fn dump_thread_states() {
    eprintln!("── OS WATCHDOG: thread dump ──────────────────────────────────");

    #[cfg(target_os = "linux")]
    {
        // /proc is the dependency-free route and is what the AMI actually runs.
        // `stat` field 3 is the scheduler state (D = uninterruptible sleep, the
        // signature of a thread blocked in the kernel); `wchan` names the kernel
        // function it is parked in, which is usually enough to tell a futex wait
        // from a socket read.
        match std::fs::read_dir("/proc/self/task") {
            Ok(entries) => {
                for e in entries.flatten() {
                    let tid = e.file_name().to_string_lossy().to_string();
                    let base = format!("/proc/self/task/{tid}");
                    let comm = std::fs::read_to_string(format!("{base}/comm"))
                        .unwrap_or_default().trim().to_string();
                    let wchan = std::fs::read_to_string(format!("{base}/wchan"))
                        .unwrap_or_default().trim().to_string();
                    let state = std::fs::read_to_string(format!("{base}/stat"))
                        .ok()
                        .and_then(|s| s.rsplit(')').next().map(|r| r.trim().to_string()))
                        .and_then(|r| r.split_whitespace().next().map(String::from))
                        .unwrap_or_default();
                    eprintln!("  tid={tid:<8} state={state:<2} thread={comm:<20} wchan={wchan}");
                }
            }
            Err(e) => eprintln!("  (could not read /proc/self/task: {e})"),
        }
    }

    #[cfg(target_os = "macos")]
    {
        // No /proc. `sample` ships with macOS and gives real per-thread stacks;
        // it is bounded so it cannot hang the exit path. Local dev only — the
        // AMI is Linux and takes the branch above.
        let pid = std::process::id().to_string();
        match std::process::Command::new("/usr/bin/sample")
            .args([&pid, "1", "-mayDie"])
            .stderr(std::process::Stdio::null())
            .output()
        {
            Ok(out) if out.status.success() => {
                let text = String::from_utf8_lossy(&out.stdout);
                // Only the call-graph section is useful; the header is noise.
                for line in text.lines().take(400) {
                    eprintln!("  {line}");
                }
            }
            Ok(_) | Err(_) => eprintln!("  (`sample` unavailable — no per-thread stacks on this host)"),
        }
    }

    eprintln!("── end thread dump ───────────────────────────────────────────");
}

#[cfg(test)]
mod stall_label_tests {
    use super::*;

    /// A stall in the setup section must name the step, not just the region.
    ///
    /// The intl freeze on 2026-08-25 reported a bare `SIGNAL_EVAL`, which
    /// covered ~150 lines holding one std `RwLock` read and five tokio mutex
    /// acquisitions — enough to know the loop was wedged, not enough to know on
    /// what. These codes make the next one self-identifying.
    #[test]
    fn setup_steps_render_a_distinct_label() {
        for (code, want) in [
            (STEP_BOOK_SNAPSHOT, "SIGNAL_EVAL/setup:book_snapshot"),
            (STEP_CONFIG_READ, "SIGNAL_EVAL/setup:config_read"),
            (STEP_PNL_LOCK, "SIGNAL_EVAL/setup:pnl_lock"),
            (STEP_COLLATERAL_LOCK, "SIGNAL_EVAL/setup:collateral_lock"),
            (STEP_POSITIONS_LOCK, "SIGNAL_EVAL/setup:positions_lock"),
            (STEP_QUOTE_EPOCHS_LOCK, "SIGNAL_EVAL/setup:quote_epochs_lock"),
        ] {
            enter_eval(code);
            let (label, _, _) = snapshot();
            assert_eq!(label, want);
        }
    }

    /// Setup codes must not collide with the viper codes the executor sets, or a
    /// stall inside a strategy would be reported as a setup step.
    #[test]
    fn setup_codes_do_not_collide_with_viper_codes() {
        for name in ["MomentumStrategy", "GboostStrategy", "MakerStrategy", "ConvergenceStrategy"] {
            let viper = signal_detail_for(name);
            assert!(viper < STEP_BOOK_SNAPSHOT, "{name} code {viper} overlaps the setup range");
        }
    }

    /// The dump must be safe to call from the watchdog thread of a wedged
    /// process — it must never panic, whatever the platform provides.
    #[test]
    fn thread_dump_does_not_panic() {
        dump_thread_states();
    }
}
