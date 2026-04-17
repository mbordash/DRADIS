/// Executor - Orchestrates strategy evaluation and signal handling
///
/// Provides high-level methods for evaluating all strategies and collecting their signals.
/// Simplifies integration into the main trading loop.
///
/// Phase 6 Enhancement: Concurrent task spawning for parallel strategy evaluation

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::StrategySignal;
use anyhow::Result;
use tracing::{info, debug, warn};
use std::time::Instant;
use alloy::primitives::U256;

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
                if !matches!(signal, StrategySignal::NoSignal) {
                    debug!("📍 {} entry signal: {:?}", strategy_name, signal);
                    entry_signals.push((strategy_name.clone(), signal));
                }
            }
            Err(e) => {
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

/// Execute all strategies concurrently
///
/// High-level function that spawns all strategies, waits for results,
/// and converts them back to StrategyEvaluationResult format for compatibility.
///
/// Phase 6 Note: For full concurrent execution, strategies should be Arc-wrapped
/// at the StrategyRegistry level. This MVP version uses tokio::join! for true
/// parallelism at the entry/exit evaluation level per strategy.
pub async fn execute_strategies_concurrent(
    strategies: &[Box<dyn Strategy>],
    ctx: &StrategyContext,
    _timeout_ms: u64,
) -> Result<StrategyEvaluationResult> {
    let mut entry_signals = Vec::new();
    let mut exit_signals = Vec::new();
    let start_all = Instant::now();

    for strategy in strategies {
        let strategy_name = strategy.name().to_string();
        let start = Instant::now();

        // Evaluate entry and exit in parallel using tokio::join!
        let (entry_result, exit_result) = tokio::join!(
            strategy.evaluate_entry(ctx),
            strategy.evaluate_exit(ctx)
        );

        let evaluation_time_ms = start.elapsed().as_millis();

        // Handle entry result
        match entry_result {
            Ok(signal) => {
                if !matches!(signal, StrategySignal::NoSignal) {
                    debug!("📍 {} entry signal: {:?} ({}ms)", strategy_name, signal, evaluation_time_ms);
                    entry_signals.push((strategy_name.clone(), signal));
                }
            }
            Err(e) => {
                warn!("⚠️ {} entry evaluation error: {}", strategy_name, e);
            }
        }

        // Handle exit result
        match exit_result {
            Ok(signal) => {
                if !matches!(signal, StrategySignal::NoSignal) {
                    debug!("📍 {} exit signal: {:?} ({}ms)", strategy_name, signal, evaluation_time_ms);
                    exit_signals.push((strategy_name.clone(), signal));
                }
            }
            Err(e) => {
                warn!("⚠️ {} exit evaluation error: {}", strategy_name, e);
            }
        }

        debug!("✅ {} evaluation completed in {}ms", strategy_name, evaluation_time_ms);
    }

    let total_time_ms = start_all.elapsed().as_millis();
    debug!("📊 All {} strategies evaluated in {}ms", strategies.len(), total_time_ms);

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
    pub token_id: U256,
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
/// prioritised before entry signals.  The `conflicts` vec is always empty but
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

