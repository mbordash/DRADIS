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

/// Executor - Orchestrates strategy evaluation and signal handling
///
/// Provides high-level methods for evaluating all strategies and collecting their signals.
/// Simplifies integration into the main trading loop.
///
/// Phase 6 Enhancement: Concurrent task spawning for parallel strategy evaluation

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::StrategySignal;
use crate::venues::core::MarketId;
use anyhow::Result;
use tracing::{info, debug, warn};
use std::time::Instant;
use tokio::time::Duration;

/// Result of evaluating all strategies
#[derive(Debug, Clone)]
pub struct StrategyEvaluationResult {
    /// Entry signals from all strategies
    pub entry_signals: Vec<(String, StrategySignal)>,
    /// Exit signals from all strategies
    pub exit_signals: Vec<(String, StrategySignal)>,
}

/// Evaluate all strategies for entry/exit signals
pub async fn evaluate_strategies(
    strategies: &[Box<dyn Strategy>],
    ctx: &StrategyContext,
) -> Result<StrategyEvaluationResult> {
    let mut entry_signals = Vec::new();
    let mut exit_signals = Vec::new();

    for strategy in strategies {
        let strategy_name = strategy.name().to_string();

        // Evaluate entry
        match strategy.evaluate_entry(ctx).await {
            Ok(signal) => {
                crate::helpers::viper_status::record_eval(
                    &ctx.crypto_filter,
                    &strategy_name,
                    if matches!(signal, StrategySignal::NoSignal) {
                        crate::helpers::viper_status::EvalOutcome::NoSignal
                    } else {
                        crate::helpers::viper_status::EvalOutcome::Signal
                    },
                );
                if !matches!(signal, StrategySignal::NoSignal) {
                    debug!("📍 {} entry signal: {:?}", strategy_name, signal);
                    entry_signals.push((strategy_name.clone(), signal));
                }
            }
            Err(e) => {
                crate::helpers::viper_status::record_eval(
                    &ctx.crypto_filter, &strategy_name, crate::helpers::viper_status::EvalOutcome::Error);
                warn!("⚠️ {} entry evaluation error: {}", strategy_name, e);
            }
        }

        // Evaluate exit
        match strategy.evaluate_exit(ctx).await {
            Ok(signal) => {
                if !matches!(signal, StrategySignal::NoSignal) {
                    debug!("📍 {} exit signal: {:?}", strategy_name, signal);
                    exit_signals.push((strategy_name.clone(), signal));
                }
            }
            Err(e) => {
                warn!("⚠️ {} exit evaluation error: {}", strategy_name, e);
            }
        }
    }

    Ok(StrategyEvaluationResult {
        entry_signals,
        exit_signals,
    })
}

/// Throttle for the re-entry suppression notices below.
///
/// Suppression fires on EVERY tick while an exit is in flight, and the maker
/// re-quotes each tick, so the steady state is not one notice but hundreds: on
/// 2026-08-27 the Ireland box logged 380 in 40 minutes (~9/min) at WARN, which
/// reads like a fault and buries real signals.
///
/// The condition is worth surfacing — it is a guard preventing a real re-entry
/// race — so the level stays WARN and only the repetition is cut. Keyed by
/// strategy AND which legs were blocked, so a change in the pattern still reports
/// immediately. Mirrors `time_decay_gate_log_permitted`.
fn reentry_log_state()
    -> &'static std::sync::Mutex<std::collections::HashMap<String, (String, Instant)>>
{
    static REG: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, (String, Instant)>>,
    > = std::sync::OnceLock::new();
    REG.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// True when this notice should be logged: on any change of pattern, or once per
/// `REENTRY_SUPPRESS_LOG_INTERVAL_SECS` while the pattern is unchanged.
fn reentry_log_permitted(strategy_name: &str, pattern: &str) -> bool {
    let mut reg = match reentry_log_state().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    match reg.get(strategy_name) {
        Some((prev, at))
            if prev == pattern
                && at.elapsed().as_secs()
                    < crate::config::REENTRY_SUPPRESS_LOG_INTERVAL_SECS =>
        {
            false
        }
        _ => {
            reg.insert(strategy_name.to_string(), (pattern.to_string(), Instant::now()));
            true
        }
    }
}

/// Execute all strategies concurrently
///
/// High-level function that spawns all strategies, waits for results,
/// and converts them back to StrategyEvaluationResult format for compatibility.
///
/// Phase 6 Note: For full concurrent execution, strategies should be Arc-wrapped
/// at the StrategyRegistry level. This MVP version uses tokio::join! for true
/// parallelism at the entry/exit evaluation level per strategy.
/// Drop any part of an entry signal that would re-open a token the same
/// strategy is closing on this tick.
///
/// `tokio::join!` runs entry and exit evaluation concurrently against one
/// context snapshot, so the entry arm cannot see that the exit arm is about to
/// stop the position out. A loss exit arms a re-entry lockout precisely to stop
/// the maker re-quoting a falling side, but that lockout is armed while the
/// entry decision is already in flight — it protects the next tick, not this
/// one.
///
/// Observed on the AMI QA box 2026-08-27: the maker stopped out of YES at
/// -31.37% and re-entered the identical position in the same second, booking
/// -$3.76 twice, 31 seconds apart. Ghost mode made it cheap to see; the race is
/// the same with real money.
///
/// A two-sided maker quote loses only the overlapping leg — closing YES is no
/// reason to stop quoting NO.
fn suppress_reentry(
    strategy_name: &str,
    signal: StrategySignal,
    closing: &[MarketId],
) -> StrategySignal {
    if closing.is_empty() {
        return signal;
    }
    let blocked = |t: &MarketId| closing.iter().any(|c| c == t);

    match signal {
        StrategySignal::MakerQuote { yes, no } => {
            let yes_blocked = yes.as_ref().is_some_and(|p| blocked(&p.token_id));
            let no_blocked = no.as_ref().is_some_and(|p| blocked(&p.token_id));
            if (yes_blocked || no_blocked)
                && reentry_log_permitted(
                    strategy_name,
                    &format!("quote:{yes_blocked}:{no_blocked}"),
                )
            {
                warn!(
                    "🚫 {strategy_name}: dropping maker quote leg(s) being closed this tick \
                     (yes={yes_blocked} no={no_blocked}) — re-entry would rebook the exit's \
                     loss (repeats quieted for {}s)",
                    crate::config::REENTRY_SUPPRESS_LOG_INTERVAL_SECS,
                );
            }
            let yes = if yes_blocked { None } else { yes };
            let no = if no_blocked { None } else { no };
            if yes.is_none() && no.is_none() {
                StrategySignal::NoSignal
            } else {
                StrategySignal::MakerQuote { yes, no }
            }
        }
        other if other.tokens_opened().iter().any(blocked) => {
            if reentry_log_permitted(strategy_name, "entry") {
                warn!(
                    "🚫 {strategy_name}: dropping entry into a position being closed this tick \
                     — re-entry would rebook the exit's loss (repeats quieted for {}s)",
                    crate::config::REENTRY_SUPPRESS_LOG_INTERVAL_SECS,
                );
            }
            StrategySignal::NoSignal
        }
        other => other,
    }
}

pub async fn execute_strategies_concurrent(
    strategies: &[Box<dyn Strategy>],
    ctx: &StrategyContext,
    timeout_ms: u64,
    last_summary: &mut String,
) -> Result<StrategyEvaluationResult> {
    let mut entry_signals = Vec::new();
    let mut exit_signals = Vec::new();
    let start_all = Instant::now();

    // INFO: Info-level Diagnostic Output — tracks each strategy's result for the tick summary.
    let mut info_parts: Vec<String> = Vec::with_capacity(strategies.len());

    for strategy in strategies {
        let strategy_name = strategy.name().to_string();
        let start = Instant::now();

        // Watchdog breadcrumb: name the strategy currently evaluating. A synchronous
        // std::sync-lock stall inside evaluate_* can't be interrupted by the timeout
        // below (the future never yields), so this atomic is what lets the OS-thread
        // watchdog report WHICH strategy froze instead of just "silent for Ns".
        crate::helpers::watchdog::enter_eval(
            crate::helpers::watchdog::signal_detail_for(&strategy_name),
        );

        // Evaluate entry and exit in parallel using tokio::join!, wrapped in a hard timeout.
        // Previously `timeout_ms` was silently ignored (prefixed `_timeout_ms`), meaning a
        // single hung strategy evaluation (e.g. StdMutex contention during GBoost retrain)
        // could freeze the entire tokio::select! loop — including the watchdog ticker.
        let join_result = tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            async {
                tokio::join!(
                    strategy.evaluate_entry(ctx),
                    strategy.evaluate_exit(ctx)
                )
            },
        ).await;

        let (entry_result, exit_result) = match join_result {
            Ok(pair) => pair,
            Err(_) => {
                warn!("⚠️ {} evaluation timed out after {}ms — skipping this tick", strategy_name, timeout_ms);
                crate::helpers::viper_status::record_eval(
                    &ctx.crypto_filter, &strategy_name, crate::helpers::viper_status::EvalOutcome::Timeout);
                let label = strategy_name.trim_end_matches("Strategy");
                info_parts.push(format!("{}:⏱️⏱️", label));
                continue;
            }
        };

        let evaluation_time_ms = start.elapsed().as_millis();

        let mut entry_tag = "⬜";
        let mut exit_tag  = "⬜";

        let mut closing_tokens: Vec<MarketId> = Vec::new();

        // Exit is handled BEFORE entry, and an entry that would re-open a token
        // this same tick is closing is dropped.
        //
        // `tokio::join!` above evaluates entry and exit concurrently against one
        // context snapshot, so the entry arm decides without knowing the exit arm
        // is about to stop the position out. A loss exit arms a re-entry lockout
        // (`arm_maker_toxic_cooldown`) precisely to stop the maker re-quoting a
        // falling side — but the lockout is armed while the entry decision is
        // already in flight, so it only protects the NEXT tick.
        //
        // Observed on the AMI QA box 2026-08-27: the maker stopped out of YES at
        // -31.37% and re-entered the identical position in the SAME second,
        // booking the same -$3.76 loss twice 31 seconds apart. Ghost mode made it
        // cheap to see; the race is identical with real money.
        //
        // The exit wins: whatever the entry arm concluded, it was reasoning about
        // a position that is being closed out from under it.
        // Handle exit result
        match exit_result {
            Ok(signal) => {
                if !matches!(signal, StrategySignal::NoSignal) {
                    exit_tag = "🟨";
                    // Signal detail at DEBUG — actual exit is logged at INFO by main.rs (💰 Position closed)
                    debug!("📍 {} exit signal: {:?} ({}ms)", strategy_name, signal, evaluation_time_ms);
                    closing_tokens = signal.tokens_closed();
                    exit_signals.push((strategy_name.clone(), signal));
                }
            }
            Err(e) => {
                exit_tag = "🔴";
                warn!("⚠️ {} exit evaluation error: {}", strategy_name, e);
            }
        }

        // Handle entry result
        match entry_result {
            Ok(signal) => {
                // "Why aren't we trading?" registry: record liveness + outcome
                // for every viper every tick (GET /api/vipers/status).
                crate::helpers::viper_status::record_eval(
                    &ctx.crypto_filter,
                    &strategy_name,
                    if matches!(signal, StrategySignal::NoSignal) {
                        crate::helpers::viper_status::EvalOutcome::NoSignal
                    } else {
                        crate::helpers::viper_status::EvalOutcome::Signal
                    },
                );
                let signal = suppress_reentry(&strategy_name, signal, &closing_tokens);
                if !matches!(signal, StrategySignal::NoSignal) {
                    entry_tag = "🟩";
                    // Signal detail at DEBUG — actual placement is logged at INFO by main.rs (📥 ENTRY)
                    debug!("📍 {} entry signal: {:?} ({}ms)", strategy_name, signal, evaluation_time_ms);
                    entry_signals.push((strategy_name.clone(), signal));
                }
            }
            Err(e) => {
                entry_tag = "🔴";
                crate::helpers::viper_status::record_eval(
                    &ctx.crypto_filter, &strategy_name, crate::helpers::viper_status::EvalOutcome::Error);
                warn!("⚠️ {} entry evaluation error: {}", strategy_name, e);
            }
        }

        debug!("✅ {} evaluation completed in {}ms", strategy_name, evaluation_time_ms);

        // Abbreviated strategy label for the compact tick line (e.g. "Momentum", "Maker")
        let label = strategy_name.trim_end_matches("Strategy");
        info_parts.push(format!("{}:{}{}", label, entry_tag, exit_tag));
    }

    let total_time_ms = start_all.elapsed().as_millis();
    // Build a pattern-only key (without timing) for change detection
    let pattern_key = format!("{} | maker_mkt={}",
        info_parts.join(" | "),
        if ctx.maker_market.is_some() { "✅" } else { "❌" });
    let summary = format!("📊 INFO [{}ms] {}", total_time_ms, pattern_key);

    // Only emit at INFO when signal pattern changes (new signal fires or clears).
    // Sustained identical patterns log at DEBUG to avoid flooding.
    let has_signal = !entry_signals.is_empty() || !exit_signals.is_empty();
    if pattern_key != *last_summary {
        if has_signal { info!("{}", summary); } else { debug!("{}", summary); }
        *last_summary = pattern_key;
    } else {
        debug!("{}", summary);
    }

    Ok(StrategyEvaluationResult {
        entry_signals,
        exit_signals,
    })
}

/// Priority for signal handling (exit first, then entry)
pub fn prioritize_signals(result: &StrategyEvaluationResult) -> Vec<(&str, &StrategySignal)> {
    let mut signals = Vec::new();

    // Exit signals take priority
    for (name, signal) in &result.exit_signals {
        signals.push((name.as_str(), signal));
    }

    // Then entry signals
    for (name, signal) in &result.entry_signals {
        signals.push((name.as_str(), signal));
    }

    signals
}

/// Signal conflict detection and resolution results
#[derive(Debug, Clone)]
pub struct SignalConflictInfo {
    pub token_id: MarketId,
    pub signal_type: String,
    pub conflicting_strategies: Vec<String>,
    pub resolution: String,
}

/// Aggregate signals from all strategies.
///
/// With per-strategy position namespaces (Option A), each strategy owns its own
/// book so there are no cross-strategy entry OR exit conflicts — two strategies
/// exiting the same token are selling from their own independent position slots.
///
/// This function therefore simply passes all signals through with exit signals
/// prioritized before entry signals.  The `conflicts` vec is always empty but
/// kept in the return type for API compatibility.
pub fn aggregate_and_resolve_signals(
    eval_result: &StrategyEvaluationResult,
) -> (Vec<(String, StrategySignal)>, Vec<SignalConflictInfo>) {
    let mut final_signals: Vec<(String, StrategySignal)> = Vec::new();

    // Exits first — always higher priority than entries
    for (strategy_name, signal) in &eval_result.exit_signals {
        final_signals.push((strategy_name.clone(), signal.clone()));
    }

    // Then entries — each strategy has its own slot, no deduplication needed
    for (strategy_name, signal) in &eval_result.entry_signals {
        final_signals.push((strategy_name.clone(), signal.clone()));
    }

    (final_signals, vec![])
}

#[cfg(test)]
mod reentry_suppression_tests {
    use super::*;
    use crate::state::OrderParams;
    use crate::venues::core::MarketId;
    use rust_decimal_macros::dec;

    fn params(token: &str) -> OrderParams {
        OrderParams {
            token_id: MarketId::new(token), price: dec!(0.51), shares: dec!(20),
            fee_bps: 0, is_neg_risk: false, market_name: "m".into(),
            condition_id: String::new(),
            order_type: crate::venues::core::TimeInForce::Fak,
            post_only: false, ghost_mode: true,
        }
    }

    /// The bug this exists for: stop out of YES, re-enter YES in the same tick.
    ///
    /// On the AMI QA box the maker exited at -31.37% and immediately rebought
    /// the identical position, booking -$3.76 twice 31 seconds apart. The
    /// re-entry lockout the exit arms cannot help — it is armed while this
    /// entry decision is already in flight.
    #[test]
    fn an_entry_into_a_token_being_closed_is_dropped() {
        let entry = StrategySignal::Entry { params: params("yes"), pair_params: None };
        let out = suppress_reentry("MakerStrategy", entry, &[MarketId::new("yes")]);
        assert!(matches!(out, StrategySignal::NoSignal));
    }

    /// Closing YES is no reason to stop quoting NO — only the overlapping leg goes.
    #[test]
    fn a_two_sided_quote_loses_only_the_closing_leg() {
        let q = StrategySignal::MakerQuote { yes: Some(params("yes")), no: Some(params("no")) };
        match suppress_reentry("MakerStrategy", q, &[MarketId::new("yes")]) {
            StrategySignal::MakerQuote { yes, no } => {
                assert!(yes.is_none(), "the closing leg must be dropped");
                assert!(no.is_some(), "the untouched leg must survive");
            }
            other => panic!("expected a MakerQuote, got {other:?}"),
        }
    }

    /// Both legs closing leaves nothing to send.
    #[test]
    fn a_quote_with_both_legs_closing_becomes_no_signal() {
        let q = StrategySignal::MakerQuote { yes: Some(params("yes")), no: Some(params("no")) };
        let out = suppress_reentry("MakerStrategy", q,
            &[MarketId::new("yes"), MarketId::new("no")]);
        assert!(matches!(out, StrategySignal::NoSignal));
    }

    /// An ordinary tick with no exit must pass through untouched — this must not
    /// become a filter that quietly suppresses normal trading.
    #[test]
    fn an_entry_with_no_exit_this_tick_is_untouched() {
        let entry = StrategySignal::Entry { params: params("yes"), pair_params: None };
        let out = suppress_reentry("MakerStrategy", entry, &[]);
        assert!(matches!(out, StrategySignal::Entry { .. }));
    }

    /// An entry into a DIFFERENT token is unaffected by the exit.
    #[test]
    fn an_unrelated_entry_survives() {
        let entry = StrategySignal::Entry { params: params("other"), pair_params: None };
        let out = suppress_reentry("MakerStrategy", entry, &[MarketId::new("yes")]);
        assert!(matches!(out, StrategySignal::Entry { .. }));
    }

    /// A paired entry (Arbitrage/TimeDecay) is dropped if EITHER leg is closing —
    /// half a hedge is worse than none.
    #[test]
    fn a_paired_entry_is_dropped_if_either_leg_is_closing() {
        let entry = StrategySignal::Entry {
            params: params("yes"), pair_params: Some(params("no")),
        };
        let out = suppress_reentry("ArbitrageStrategy", entry, &[MarketId::new("no")]);
        assert!(matches!(out, StrategySignal::NoSignal));
    }
}

#[cfg(test)]
mod reentry_log_throttle_tests {
    use super::reentry_log_permitted;

    /// The first notice is always logged; an identical repeat on the very next tick
    /// is not. This is the 380-lines-in-40-minutes case from 2026-08-27.
    #[test]
    fn an_identical_repeat_is_quieted() {
        let s = "throttle-test-identical";
        assert!(reentry_log_permitted(s, "quote:true:false"));
        assert!(!reentry_log_permitted(s, "quote:true:false"));
        assert!(!reentry_log_permitted(s, "quote:true:false"));
    }

    /// A CHANGE in which legs are blocked reports immediately — the throttle cuts
    /// repetition, never a new condition.
    #[test]
    fn a_changed_pattern_reports_immediately() {
        let s = "throttle-test-changed";
        assert!(reentry_log_permitted(s, "quote:true:false"));
        assert!(reentry_log_permitted(s, "quote:false:true"));
        assert!(reentry_log_permitted(s, "entry"));
    }

    /// Throttling is per strategy, so a noisy maker cannot mask a different
    /// strategy's first notice.
    #[test]
    fn strategies_are_throttled_independently() {
        assert!(reentry_log_permitted("throttle-test-a", "entry"));
        assert!(reentry_log_permitted("throttle-test-b", "entry"));
        assert!(!reentry_log_permitted("throttle-test-a", "entry"));
    }
}
