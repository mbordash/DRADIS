//! Per-viper "why aren't we trading?" status registry.
//!
//! Answers the most common operator question during quiet markets: is the
//! engine broken, or is it correctly sitting out? Every strategy evaluation
//! tick records liveness + outcome here (from the orchestrator executor), and
//! instrumented vipers additionally report the *named gate* that vetoed their
//! most recent entry attempt ("oracle too flat", "edge below required", …).
//!
//! Exposed via `GET /api/vipers/status` and surfaced in the Control Tower
//! Viper Activity panel. Purely in-memory — resets on restart by design.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use chrono::{DateTime, Utc};
use serde::Serialize;

/// Outcome of a single entry evaluation, recorded by the executor every tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvalOutcome {
    /// Produced an actionable entry signal this tick.
    Signal,
    /// Evaluated cleanly but chose not to trade.
    NoSignal,
    /// evaluate_entry returned an error.
    Error,
    /// Evaluation exceeded the executor's hard timeout.
    Timeout,
}

#[derive(Debug, Clone)]
struct ViperStatus {
    last_eval_at: DateTime<Utc>,
    last_outcome: EvalOutcome,
    /// Most recent named veto/idle reason reported by the viper's own gates.
    /// Only instrumented vipers populate this; others show liveness only.
    last_reason: Option<String>,
    last_reason_at: Option<DateTime<Utc>>,
    /// Last time this viper produced an actionable entry signal.
    last_signal_at: Option<DateTime<Utc>>,
}

static REGISTRY: OnceLock<Mutex<HashMap<String, ViperStatus>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<String, ViperStatus>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Record the outcome of one entry evaluation (called by the executor per tick).
pub fn record_eval(strategy: &str, outcome: EvalOutcome) {
    let now = Utc::now();
    let mut map = match registry().lock() {
        Ok(m) => m,
        Err(p) => p.into_inner(),
    };
    let entry = map.entry(strategy.to_string()).or_insert_with(|| ViperStatus {
        last_eval_at: now,
        last_outcome: outcome,
        last_reason: None,
        last_reason_at: None,
        last_signal_at: None,
    });
    entry.last_eval_at = now;
    entry.last_outcome = outcome;
    if outcome == EvalOutcome::Signal {
        entry.last_signal_at = Some(now);
        // A fresh signal supersedes any stale veto reason.
        entry.last_reason = None;
        entry.last_reason_at = None;
    }
}

/// Report the named gate that vetoed the current entry attempt.
/// Called from inside instrumented vipers' entry gates; cheap overwrite.
pub fn report_reason(strategy: &str, reason: &str) {
    let now = Utc::now();
    let mut map = match registry().lock() {
        Ok(m) => m,
        Err(p) => p.into_inner(),
    };
    let entry = map.entry(strategy.to_string()).or_insert_with(|| ViperStatus {
        last_eval_at: now,
        last_outcome: EvalOutcome::NoSignal,
        last_reason: None,
        last_reason_at: None,
        last_signal_at: None,
    });
    entry.last_reason = Some(reason.to_string());
    entry.last_reason_at = Some(now);
}

/// One viper's status row as served by `GET /api/vipers/status`.
#[derive(Serialize)]
pub struct ViperStatusView {
    pub strategy: String,
    pub last_eval_at: String,
    pub last_eval_secs_ago: i64,
    pub last_outcome: EvalOutcome,
    pub last_reason: Option<String>,
    pub last_reason_secs_ago: Option<i64>,
    pub last_signal_at: Option<String>,
    pub last_signal_secs_ago: Option<i64>,
}

/// Snapshot of every viper seen since startup, sorted by strategy name.
pub fn snapshot() -> Vec<ViperStatusView> {
    let now = Utc::now();
    let map = match registry().lock() {
        Ok(m) => m,
        Err(p) => p.into_inner(),
    };
    let mut rows: Vec<ViperStatusView> = map.iter().map(|(name, st)| ViperStatusView {
        strategy: name.clone(),
        last_eval_at: st.last_eval_at.to_rfc3339(),
        last_eval_secs_ago: (now - st.last_eval_at).num_seconds(),
        last_outcome: st.last_outcome,
        last_reason: st.last_reason.clone(),
        last_reason_secs_ago: st.last_reason_at.map(|t| (now - t).num_seconds()),
        last_signal_at: st.last_signal_at.map(|t| t.to_rfc3339()),
        last_signal_secs_ago: st.last_signal_at.map(|t| (now - t).num_seconds()),
    }).collect();
    rows.sort_by(|a, b| a.strategy.cmp(&b.strategy));
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_snapshots() {
        record_eval("TestStrategyA", EvalOutcome::NoSignal);
        report_reason("TestStrategyA", "edge below required");
        let snap = snapshot();
        let row = snap.iter().find(|r| r.strategy == "TestStrategyA").unwrap();
        assert_eq!(row.last_outcome, EvalOutcome::NoSignal);
        assert_eq!(row.last_reason.as_deref(), Some("edge below required"));
        assert!(row.last_eval_secs_ago >= 0);
    }

    #[test]
    fn signal_clears_stale_reason() {
        record_eval("TestStrategyB", EvalOutcome::NoSignal);
        report_reason("TestStrategyB", "cooldown");
        record_eval("TestStrategyB", EvalOutcome::Signal);
        let snap = snapshot();
        let row = snap.iter().find(|r| r.strategy == "TestStrategyB").unwrap();
        assert_eq!(row.last_outcome, EvalOutcome::Signal);
        assert!(row.last_reason.is_none());
        assert!(row.last_signal_at.is_some());
    }
}
