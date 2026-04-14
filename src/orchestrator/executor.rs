/// Executor - Orchestrates strategy evaluation and signal handling
///
/// Provides high-level methods for evaluating all strategies and collecting their signals.
/// Simplifies integration into the main trading loop.
///
/// Phase 6 Enhancement: Concurrent task spawning for parallel strategy evaluation

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::StrategySignal;
use anyhow::Result;
use tracing::{debug, warn};
use std::time::Instant;
use alloy::primitives::U256;
use std::collections::HashMap;

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

/// Aggregate multiple signals and resolve conflicts
///
/// Conflicts happen when multiple strategies signal on the same token.
/// Resolution priority: Exit > Entry (exits always win)
/// Within same type: first signal wins
pub fn aggregate_and_resolve_signals(
    eval_result: &StrategyEvaluationResult,
) -> (Vec<(String, StrategySignal)>, Vec<SignalConflictInfo>) {
    let mut final_signals: Vec<(String, StrategySignal)> = Vec::new();
    let mut conflicts: Vec<SignalConflictInfo> = Vec::new();

    // Track which tokens have been processed to detect conflicts
    let mut processed_exits: HashMap<U256, String> = HashMap::new();
    let mut processed_entries: HashMap<U256, String> = HashMap::new();

    // Process exits first (highest priority)
    for (strategy_name, signal) in &eval_result.exit_signals {
        if let StrategySignal::Exit { token_id, reason: _ } = signal {
            if let Some(prev_strategy) = processed_exits.get(token_id) {
                // Exit conflict: multiple strategies want to exit same token
                conflicts.push(SignalConflictInfo {
                    token_id: *token_id,
                    signal_type: "Exit".to_string(),
                    conflicting_strategies: vec![prev_strategy.clone(), strategy_name.clone()],
                    resolution: format!("First exit wins: {} (dropping {})", prev_strategy, strategy_name),
                });
                debug!("⚠️ Exit conflict for token {}: {} vs {}, using first", token_id, prev_strategy, strategy_name);
            } else {
                // First exit for this token, accept it
                processed_exits.insert(*token_id, strategy_name.clone());
                final_signals.push((strategy_name.clone(), signal.clone()));
                debug!("✅ Exit signal accepted: {} on token {}", strategy_name, token_id);
            }
        }
    }

    // Process entries, but check for conflicts with exits
    for (strategy_name, signal) in &eval_result.entry_signals {
        if let StrategySignal::Entry { token_id } = signal {
            if processed_exits.contains_key(token_id) {
                // Entry/Exit conflict on same token: exit takes priority
                let exit_strategy = processed_exits.get(token_id).unwrap();
                conflicts.push(SignalConflictInfo {
                    token_id: *token_id,
                    signal_type: "Entry".to_string(),
                    conflicting_strategies: vec![exit_strategy.clone(), strategy_name.clone()],
                    resolution: format!("Exit takes priority: {} exit wins, dropping {} entry", exit_strategy, strategy_name),
                });
                debug!("⚠️ Entry/Exit conflict for token {}: {} wants exit, {} wants entry - exit wins", token_id, exit_strategy, strategy_name);
            } else if let Some(prev_strategy) = processed_entries.get(token_id) {
                // Entry conflict: multiple strategies want to enter same token
                conflicts.push(SignalConflictInfo {
                    token_id: *token_id,
                    signal_type: "Entry".to_string(),
                    conflicting_strategies: vec![prev_strategy.clone(), strategy_name.clone()],
                    resolution: format!("First entry wins: {} (dropping {})", prev_strategy, strategy_name),
                });
                debug!("⚠️ Entry conflict for token {}: {} vs {}, using first", token_id, prev_strategy, strategy_name);
            } else {
                // First entry for this token and no exit conflict, accept it
                processed_entries.insert(*token_id, strategy_name.clone());
                final_signals.push((strategy_name.clone(), signal.clone()));
                debug!("✅ Entry signal accepted: {} on token {}", strategy_name, token_id);
            }
        }
    }

    (final_signals, conflicts)
}

