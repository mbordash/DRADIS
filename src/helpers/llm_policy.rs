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

//! Autonomy policy engine for LLM-authored config patches (Epic S3).
//!
//! The LLM is never trusted to limit itself — every proposed change passes
//! through this server-side gate before touching the live `DynamicConfig`.
//!
//! Tier semantics (per validated change):
//! - **Tier 1 — Recommend**: nothing auto-applies. Rows stay `proposed` and
//!   wait for a human approve/reject (S4) until their TTL expires.
//! - **Tier 2 — Limited**: schema-clamped changes apply immediately, but
//!   money fields are held, per-field delta is capped (default ±20 %), and
//!   applies are rate-limited (default 1 batch/hour). Held changes stay
//!   `proposed` so a human can still approve them before TTL.
//! - **Tier 3 — Autonomous**: everything applies (still schema-clamped by
//!   validation) except mode flips (`ghost_mode`), with no rate limit — but
//!   the circuit breaker watches post-apply P&L and reverts + demotes to
//!   tier 1 on a drawdown trip.
//!
//! The kill switch (`LLM_AUTONOMY_KILL=1`) and a breaker demotion both force
//! tier-1 behaviour regardless of the configured tier.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{Duration as ChronoDuration, Utc};
use tracing::{info, warn};
use serde_json::{json, Value};
use sqlx::SqlitePool;
use tokio::sync::watch;

use crate::helpers::db;
use crate::helpers::dynamic_config::DynamicConfig;
use crate::helpers::llm_patch::{ProposalBatch, ValidatedChange};

/// Set when the circuit breaker trips; forces tier-1 behaviour until restart
/// (or until an operator clears it via the Setup view, S5).
static BREAKER_DEMOTED: AtomicBool = AtomicBool::new(false);

pub fn breaker_demoted() -> bool {
    BREAKER_DEMOTED.load(Ordering::Relaxed)
}

/// Operator reset (surfaced in the Setup view, S5).
pub fn reset_breaker_demotion() {
    BREAKER_DEMOTED.store(false, Ordering::Relaxed);
}

/// Runtime-tunable policy knobs, resolved from env each cycle so they can be
/// adjusted without a rebuild. All have conservative defaults.
#[derive(Debug, Clone)]
pub struct PolicyKnobs {
    /// Hard stop: no auto-applies at any tier.
    pub kill_switch: bool,
    /// Tier-2 rate limit: max applied batches per rolling hour.
    pub max_batches_per_hour: i64,
    /// Tier-2 per-field relative delta cap (fraction, e.g. 0.20 = ±20 %).
    pub max_delta_pct: f64,
    /// Circuit breaker: session-P&L drawdown (USDC) after an apply that trips
    /// a revert + demotion.
    pub breaker_drawdown_usdc: f64,
    /// Circuit breaker: how far back (secs) applied actions are watched.
    pub breaker_window_secs: i64,
}

impl PolicyKnobs {
    pub fn from_env() -> Self {
        fn envf(key: &str, default: f64) -> f64 {
            std::env::var(key).ok().and_then(|v| v.trim().parse().ok()).unwrap_or(default)
        }
        fn envi(key: &str, default: i64) -> i64 {
            std::env::var(key).ok().and_then(|v| v.trim().parse().ok()).unwrap_or(default)
        }
        Self {
            kill_switch: std::env::var("LLM_AUTONOMY_KILL")
                .map(|v| matches!(v.trim(), "1" | "true" | "yes"))
                .unwrap_or(false),
            max_batches_per_hour: envi("LLM_MAX_PATCHES_PER_HOUR", 1).max(0),
            max_delta_pct: envf("LLM_MAX_DELTA_PCT", 0.20).abs(),
            breaker_drawdown_usdc: envf("LLM_BREAKER_DRAWDOWN_USDC", 25.0).abs(),
            breaker_window_secs: envi("LLM_BREAKER_WINDOW_SECS", 4 * 3600).max(60),
        }
    }
}

/// Money fields never auto-apply at tier 2 ("no cash changes"). Matches the
/// USDC-denominated exposure/sizing keys in the schema registry.
pub fn is_money_field(key: &str) -> bool {
    key.ends_with("_usdc") || key.contains("budget") || key.contains("collateral")
}

/// Mode flips (paper ↔ live) are an operator decision at every tier.
pub fn is_mode_field(key: &str) -> bool {
    key == "ghost_mode"
}

/// Per-change policy verdict.
#[derive(Debug, PartialEq)]
pub enum Verdict {
    /// Auto-apply now.
    Apply,
    /// Leave `proposed` for human approval (S4) until TTL.
    Hold(String),
}

/// Evaluate one validated change against the effective tier.
/// `tier` must already account for kill switch / breaker demotion.
pub fn evaluate_change(tier: i64, c: &ValidatedChange, knobs: &PolicyKnobs) -> Verdict {
    if is_mode_field(&c.key) {
        return Verdict::Hold("mode flips require human approval at every tier".into());
    }
    match tier {
        3 => Verdict::Apply,
        2 => {
            if is_money_field(&c.key) {
                return Verdict::Hold("money field — tier 2 makes no cash changes".into());
            }
            if let Some(d) = c.delta_pct {
                if d.abs() > knobs.max_delta_pct {
                    return Verdict::Hold(format!(
                        "delta {:+.1}% exceeds tier-2 cap ±{:.0}%",
                        d * 100.0, knobs.max_delta_pct * 100.0,
                    ));
                }
            }
            Verdict::Apply
        }
        _ => Verdict::Hold("tier 1 — human approval required".into()),
    }
}

/// Outcome summary of a batch enforcement pass (for logs / Telegram note).
#[derive(Debug, Default)]
pub struct EnforceOutcome {
    pub applied: Vec<String>,
    pub held: Vec<String>,
    pub apply_error: Option<String>,
}

impl EnforceOutcome {
    pub fn summary_line(&self) -> Option<String> {
        if self.applied.is_empty() && self.apply_error.is_none() {
            return None;
        }
        let mut s = format!("🤖 Auto-applied {} config change(s): {}",
            self.applied.len(), self.applied.join(", "));
        if !self.held.is_empty() {
            s.push_str(&format!(" — {} held for approval", self.held.len()));
        }
        if let Some(e) = &self.apply_error {
            s = format!("⚠️ Auto-apply failed: {e}");
        }
        Some(s)
    }
}

/// Enforce the autonomy policy over a freshly recorded batch.
///
/// `ids` must be parallel to `batch.accepted` (as returned by
/// `db::record_llm_action_batch`). Applies the allowed subset atomically via
/// `DynamicConfig::apply_patch_as`, broadcasts on the watch channel, and
/// stamps each row `applied` (with its single-field inverse patch and the
/// current session P&L as the breaker baseline). Held rows stay `proposed`
/// with the policy reason in `status_detail`.
pub async fn enforce_batch(
    pool: &SqlitePool,
    batch: &ProposalBatch,
    ids: &[i64],
    configured_tier: i64,
    config_tx: &watch::Sender<Arc<DynamicConfig>>,
    current_pnl: f64,
    knobs: &PolicyKnobs,
) -> EnforceOutcome {
    let mut out = EnforceOutcome::default();

    // Resolve the effective tier: kill switch / breaker demotion win.
    let mut tier = configured_tier.clamp(1, 3);
    if knobs.kill_switch {
        info!("🛑 LLM autonomy kill switch active — all proposals queue for approval");
        tier = 1;
    } else if breaker_demoted() {
        warn!("🧯 LLM autonomy demoted to tier 1 by circuit breaker — operator reset required");
        tier = 1;
    }

    // Tier-2 rate limit is batch-scoped: if the budget is spent, everything
    // queues for human approval rather than silently dropping.
    if tier == 2 && knobs.max_batches_per_hour > 0 {
        let since = (Utc::now() - ChronoDuration::hours(1)).to_rfc3339();
        let used = db::count_llm_batches_applied_since(pool, &since).await;
        if used >= knobs.max_batches_per_hour {
            info!(
                "⏳ LLM autonomy rate limit reached ({used}/{} batch(es) this hour) — batch held",
                knobs.max_batches_per_hour,
            );
            tier = 1;
        }
    }

    let mut to_apply: Vec<(i64, &ValidatedChange)> = Vec::new();
    for (id, c) in ids.iter().zip(batch.accepted.iter()) {
        match evaluate_change(tier, c, knobs) {
            Verdict::Apply => to_apply.push((*id, c)),
            Verdict::Hold(why) => {
                db::update_llm_action_status(
                    pool, *id, "proposed", Some(&format!("held: {why}")), None,
                ).await;
                out.held.push(c.key.clone());
            }
        }
    }
    if to_apply.is_empty() {
        return out;
    }

    // Build the combined patch and apply once (single persist + broadcast).
    let patch = Value::Object(
        to_apply.iter().map(|(_, c)| (c.key.clone(), c.to.clone())).collect(),
    );
    let current = config_tx.borrow().clone();
    match DynamicConfig::apply_patch_as(&current, &patch.to_string(), "llm_advisor").await {
        Ok(new_cfg) => {
            let _ = config_tx.send(new_cfg);
            for (id, c) in &to_apply {
                let inverse = json!({ c.key.clone(): c.from.clone() }).to_string();
                let detail = format!("auto-applied at tier {tier}: {}", c.reason);
                db::mark_llm_action_applied(pool, *id, &detail, &inverse, current_pnl).await;
                info!("🤖 LLM autonomy applied {}: {} → {}", c.key, c.from, c.to);
                out.applied.push(c.key.clone());
            }
        }
        Err(e) => {
            let msg = e.to_string();
            for (id, _) in &to_apply {
                db::update_llm_action_status(
                    pool, *id, "failed", Some(&format!("apply error: {msg}")), None,
                ).await;
            }
            warn!("❌ LLM autonomy apply failed ({} change(s)): {msg}", to_apply.len());
            out.apply_error = Some(msg);
        }
    }
    out
}

/// Circuit breaker: called once per advisory cycle. If session P&L has drawn
/// down more than `breaker_drawdown_usdc` since any still-`applied` action in
/// the watch window, revert them all (oldest values win on key conflicts),
/// demote autonomy to tier 1, and return an alert string for Telegram.
pub async fn circuit_breaker_check(
    pool: &SqlitePool,
    config_tx: &watch::Sender<Arc<DynamicConfig>>,
    current_pnl: f64,
    knobs: &PolicyKnobs,
) -> Option<String> {
    let since = (Utc::now() - ChronoDuration::seconds(knobs.breaker_window_secs)).to_rfc3339();
    let applied = db::fetch_llm_actions_applied_since(pool, &since).await;
    if applied.is_empty() {
        return None;
    }

    let tripped = applied.iter().any(|a| {
        a.pnl_at_apply
            .map(|base| base - current_pnl > knobs.breaker_drawdown_usdc)
            .unwrap_or(false)
    });
    if !tripped {
        return None;
    }

    // Merge inverse patches newest→oldest so the OLDEST original value wins
    // when the same key was patched more than once.
    let mut revert = serde_json::Map::new();
    for a in &applied { // already newest first
        if let Some(inv) = a.inverse_patch.as_deref() {
            if let Ok(Value::Object(m)) = serde_json::from_str::<Value>(inv) {
                for (k, v) in m {
                    revert.insert(k, v); // later (older) inserts overwrite
                }
            }
        }
    }
    if revert.is_empty() {
        return None;
    }

    let patch = Value::Object(revert).to_string();
    let current = config_tx.borrow().clone();
    match DynamicConfig::apply_patch_as(&current, &patch, "llm_breaker").await {
        Ok(new_cfg) => {
            let _ = config_tx.send(new_cfg);
            let fields: Vec<String> = applied.iter().map(|a| a.field.clone()).collect();
            for a in &applied {
                db::update_llm_action_status(
                    pool, a.id, "reverted",
                    Some(&format!(
                        "circuit breaker: P&L drew down > ${:.2} within {}s of apply",
                        knobs.breaker_drawdown_usdc, knobs.breaker_window_secs,
                    )),
                    None,
                ).await;
            }
            BREAKER_DEMOTED.store(true, Ordering::Relaxed);
            let alert = format!(
                "🧯 LLM autonomy CIRCUIT BREAKER tripped — reverted {} change(s) ({}) after a ${:.2}+ P&L drawdown. Autonomy demoted to tier 1 (recommend-only) until operator reset.",
                applied.len(), fields.join(", "), knobs.breaker_drawdown_usdc,
            );
            warn!("{alert}");
            Some(alert)
        }
        Err(e) => {
            warn!("❌ Circuit breaker revert failed: {e}");
            // Demote anyway — the config may be in a bad state, stop the AI.
            BREAKER_DEMOTED.store(true, Ordering::Relaxed);
            Some(format!(
                "🧯 LLM autonomy circuit breaker tripped but REVERT FAILED ({e}). Autonomy demoted to tier 1 — manual config review required.",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change(key: &str, delta_pct: Option<f64>) -> ValidatedChange {
        ValidatedChange {
            key: key.to_string(),
            from: json!("0.10"),
            to: json!("0.12"),
            clamped: false,
            delta_pct,
            reason: "test".to_string(),
        }
    }

    fn knobs() -> PolicyKnobs {
        PolicyKnobs {
            kill_switch: false,
            max_batches_per_hour: 1,
            max_delta_pct: 0.20,
            breaker_drawdown_usdc: 25.0,
            breaker_window_secs: 14400,
        }
    }

    #[test]
    fn tier1_holds_everything() {
        let c = change("maker_min_spread", Some(0.05));
        assert!(matches!(evaluate_change(1, &c, &knobs()), Verdict::Hold(_)));
    }

    #[test]
    fn tier2_applies_small_non_money_change() {
        let c = change("maker_min_spread", Some(0.10));
        assert_eq!(evaluate_change(2, &c, &knobs()), Verdict::Apply);
    }

    #[test]
    fn tier2_holds_money_fields() {
        let c = change("arb_max_exposure_usdc", Some(0.05));
        assert!(matches!(evaluate_change(2, &c, &knobs()), Verdict::Hold(_)));
    }

    #[test]
    fn tier2_holds_oversized_delta() {
        let c = change("maker_min_spread", Some(0.35));
        assert!(matches!(evaluate_change(2, &c, &knobs()), Verdict::Hold(_)));
        let c = change("maker_min_spread", Some(-0.35));
        assert!(matches!(evaluate_change(2, &c, &knobs()), Verdict::Hold(_)));
    }

    #[test]
    fn tier2_applies_bools_without_delta() {
        let c = change("enable_momentum_viper", None);
        assert_eq!(evaluate_change(2, &c, &knobs()), Verdict::Apply);
    }

    #[test]
    fn tier3_applies_money_and_large_deltas() {
        let c = change("arb_max_exposure_usdc", Some(0.50));
        assert_eq!(evaluate_change(3, &c, &knobs()), Verdict::Apply);
    }

    #[test]
    fn ghost_mode_held_at_every_tier() {
        let c = change("ghost_mode", None);
        for tier in 1..=3 {
            assert!(matches!(evaluate_change(tier, &c, &knobs()), Verdict::Hold(_)));
        }
    }

    #[test]
    fn money_field_detection() {
        assert!(is_money_field("arb_max_exposure_usdc"));
        assert!(is_money_field("momentum_max_exposure_usdc"));
        assert!(!is_money_field("maker_min_spread"));
        assert!(!is_money_field("basis_stop_loss_pct"));
    }
}
