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
    /// Not evaluated this tick because the squadron holds no market to
    /// evaluate against. The patrol loop is alive and ticking; there is simply
    /// nothing to trade until a tradeable market opens.
    ///
    /// Recorded every tick the squadron idles, so `last_eval_at` stays fresh
    /// and the Control Tower can tell "waiting" from "wedged": a loop that
    /// stops recording anything at all still ages into STALE, which is the
    /// fault that badge exists to catch.
    Idle,
}

/// The reason attached to every `Idle` row. One string, so the Control Tower
/// can key its "waiting" rendering on the outcome and print this verbatim.
pub const IDLE_NO_MARKET: &str = "waiting for a tradeable market";

/// The reason a viper the operator has switched off reports.
///
/// Each viper writes this itself from its own `evaluate_entry`, but that never
/// runs while the squadron holds no market — the patrol tick `continue`s first.
/// So a viper toggled off DURING an idle window would otherwise keep whatever
/// row it had, age past the staleness threshold and surface as a fault: exactly
/// the false positive the idle path exists to remove. The idle path therefore
/// stamps this itself. Must stay byte-identical to the literal the vipers use,
/// because the Control Tower's health ribbon keys "active" off this exact string.
pub const DISABLED_IN_CONFIG: &str = "disabled in config";

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

/// Registry key: (asset/squadron e.g. "btc", strategy name). Vipers are owned
/// by squadrons — two squadrons running the same viper are distinct instances.
static REGISTRY: OnceLock<Mutex<HashMap<(String, String), ViperStatus>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<(String, String), ViperStatus>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn key(asset: &str, strategy: &str) -> (String, String) {
    (asset.to_lowercase(), strategy.to_string())
}

/// Drop every viper row for an asset whose squadron no longer exists.
///
/// Without this the registry only ever grows. The Kalshi crypto loop rotates
/// between underlyings, and `cag.remove(prev)` retires the old squadron from the
/// CAG — but its nine viper rows stayed. They then counted toward "across N
/// squadrons", stopped being evaluated, aged past the staleness window, and
/// reported themselves as "N stale/error — check squadron detail" for a
/// squadron the operator cannot see and cannot open. On a fresh AMI that is the
/// first thing on screen: a red ribbon blaming a phantom.
///
/// Called wherever a squadron is retired, mirroring `cag.remove`.
pub fn forget(asset: &str) {
    let asset = asset.to_lowercase();
    let mut map = match registry().lock() {
        Ok(m) => m,
        Err(p) => p.into_inner(),
    };
    let before = map.len();
    map.retain(|(a, _), _| a != &asset);
    let dropped = before - map.len();
    if dropped > 0 {
        tracing::info!("🧹 Retired {dropped} viper row(s) for '{asset}' — its squadron is gone");
    }
}

/// Record the outcome of one entry evaluation (called by the executor per tick).
pub fn record_eval(asset: &str, strategy: &str, outcome: EvalOutcome) {
    let now = Utc::now();
    let mut map = match registry().lock() {
        Ok(m) => m,
        Err(p) => p.into_inner(),
    };
    let entry = map.entry(key(asset, strategy)).or_insert_with(|| ViperStatus {
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

/// Record that the squadron ticked but had no market to evaluate `strategy`
/// against. See `EvalOutcome::Idle`.
///
/// Refreshes liveness on every call, but stamps `last_reason_at` only when the
/// row was not already idle for this reason. That timestamp is what the
/// Control Tower shows as the age of the wait, and the wait began when the
/// squadron released its market, not on the most recent 50ms tick. A gap that
/// outlives an hour is worth an operator's attention; a few minutes at the top
/// of the hour is routine, and the age is what tells the two apart.
///
/// Observed on a fresh Marketplace instance 2026-09-04: the BTC hourly expired,
/// no replacement cleared the volume floor, the squadron correctly released
/// the market and waited, and its nine vipers stopped being recorded at all.
/// Their rows aged past the staleness window and the first thing the
/// customer saw was "9 stale/error — check squadron detail" on a system that
/// was healthy and idle by design.
pub fn record_idle(asset: &str, strategy: &str, reason: &str) {
    let now = Utc::now();
    let mut map = match registry().lock() {
        Ok(m) => m,
        Err(p) => p.into_inner(),
    };
    let entry = map.entry(key(asset, strategy)).or_insert_with(|| ViperStatus {
        last_eval_at: now,
        last_outcome: EvalOutcome::Idle,
        last_reason: None,
        last_reason_at: None,
        last_signal_at: None,
    });
    entry.last_eval_at = now;
    let already_idle = entry.last_outcome == EvalOutcome::Idle
        && entry.last_reason.as_deref() == Some(reason);
    entry.last_outcome = EvalOutcome::Idle;
    if !already_idle {
        entry.last_reason = Some(reason.to_string());
        entry.last_reason_at = Some(now);
    }
}

/// Report the named gate that vetoed the current entry attempt.
/// Called from inside instrumented vipers' entry gates; cheap overwrite.
pub fn report_reason(asset: &str, strategy: &str, reason: &str) {
    let now = Utc::now();
    let mut map = match registry().lock() {
        Ok(m) => m,
        Err(p) => p.into_inner(),
    };
    let entry = map.entry(key(asset, strategy)).or_insert_with(|| ViperStatus {
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
    /// Owning squadron's asset (lowercase, e.g. "btc").
    pub asset: String,
    pub strategy: String,
    pub last_eval_at: String,
    pub last_eval_secs_ago: i64,
    pub last_outcome: EvalOutcome,
    pub last_reason: Option<String>,
    pub last_reason_secs_ago: Option<i64>,
    pub last_signal_at: Option<String>,
    pub last_signal_secs_ago: Option<i64>,
}

/// Snapshot of vipers seen since startup, sorted by (asset, strategy).
/// `asset_filter` limits to one squadron's asset; None returns all squadrons.
pub fn snapshot(asset_filter: Option<&str>) -> Vec<ViperStatusView> {
    let now = Utc::now();
    let filter = asset_filter.map(|a| a.to_lowercase());
    let map = match registry().lock() {
        Ok(m) => m,
        Err(p) => p.into_inner(),
    };
    let mut rows: Vec<ViperStatusView> = map.iter()
        .filter(|((asset, _), _)| filter.as_deref().is_none_or(|f| f == asset))
        .map(|((asset, name), st)| ViperStatusView {
        asset: asset.clone(),
        strategy: name.clone(),
        last_eval_at: st.last_eval_at.to_rfc3339(),
        last_eval_secs_ago: (now - st.last_eval_at).num_seconds(),
        last_outcome: st.last_outcome,
        last_reason: st.last_reason.clone(),
        last_reason_secs_ago: st.last_reason_at.map(|t| (now - t).num_seconds()),
        last_signal_at: st.last_signal_at.map(|t| t.to_rfc3339()),
        last_signal_secs_ago: st.last_signal_at.map(|t| (now - t).num_seconds()),
    }).collect();
    rows.sort_by(|a, b| (a.asset.as_str(), a.strategy.as_str()).cmp(&(b.asset.as_str(), b.strategy.as_str())));
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_snapshots() {
        record_eval("BTC", "TestStrategyA", EvalOutcome::NoSignal);
        report_reason("BTC", "TestStrategyA", "edge below required");
        // Same viper under a different squadron is a distinct instance.
        record_eval("eth", "TestStrategyA", EvalOutcome::Signal);
        let snap = snapshot(Some("btc"));
        assert!(snap.iter().all(|r| r.asset == "btc"));
        let row = snap.iter().find(|r| r.strategy == "TestStrategyA").unwrap();
        assert_eq!(row.last_outcome, EvalOutcome::NoSignal);
        assert_eq!(row.last_reason.as_deref(), Some("edge below required"));
        assert!(row.last_eval_secs_ago >= 0);
        let eth = snapshot(Some("eth"));
        let eth_row = eth.iter().find(|r| r.strategy == "TestStrategyA").unwrap();
        assert_eq!(eth_row.last_outcome, EvalOutcome::Signal);
    }

    #[test]
    fn signal_clears_stale_reason() {
        record_eval("btc", "TestStrategyB", EvalOutcome::NoSignal);
        report_reason("btc", "TestStrategyB", "cooldown");
        record_eval("btc", "TestStrategyB", EvalOutcome::Signal);
        let snap = snapshot(None);
        let row = snap.iter().find(|r| r.strategy == "TestStrategyB").unwrap();
        assert_eq!(row.last_outcome, EvalOutcome::Signal);
        assert!(row.last_reason.is_none());
        assert!(row.last_signal_at.is_some());
    }
}

#[cfg(test)]
mod idle_tests {
    use super::*;

    /// An idle tick is liveness, not an evaluation outcome from a dead market.
    /// The row must read as fresh, as idle, and must say why.
    #[test]
    fn idle_refreshes_liveness_and_names_the_wait() {
        record_eval("idletest-a", "MakerStrategy", EvalOutcome::NoSignal);
        report_reason("idletest-a", "MakerStrategy", "secs_to_expiry -43s < min 1800s");
        record_idle("idletest-a", "MakerStrategy", IDLE_NO_MARKET);
        let snap = snapshot(Some("idletest-a"));
        let row = snap.iter().find(|r| r.strategy == "MakerStrategy").unwrap();
        assert_eq!(row.last_outcome, EvalOutcome::Idle);
        assert!(row.last_eval_secs_ago <= 1, "idle must count as a fresh tick");
        assert_eq!(
            row.last_reason.as_deref(),
            Some(IDLE_NO_MARKET),
            "the dead market's last gate must not survive as the displayed reason",
        );
        forget("idletest-a");
    }

    /// A viper switched off DURING an idle window never reaches its own
    /// `evaluate_entry` — the patrol tick `continue`s before the executor runs —
    /// so the idle path has to stamp "disabled in config" for it. Skipping it
    /// would freeze its row and age it into a fault, which is exactly the false
    /// positive the idle path exists to remove.
    ///
    /// The string must stay byte-identical to the literal the vipers write,
    /// because the Control Tower's health ribbon keys "active" off it.
    #[test]
    fn a_viper_disabled_during_an_idle_window_still_reports_fresh() {
        record_eval("idletest-e", "GboostStrategy", EvalOutcome::NoSignal);
        report_reason("idletest-e", "GboostStrategy", "target snapshot stale");
        // Operator toggles it off mid-wait: the idle path stamps it rather than
        // skipping it.
        record_idle("idletest-e", "GboostStrategy", DISABLED_IN_CONFIG);
        let snap = snapshot(Some("idletest-e"));
        let row = snap.iter().find(|r| r.strategy == "GboostStrategy").unwrap();
        assert!(
            row.last_eval_secs_ago <= 1,
            "a disabled viper must keep reporting liveness or it ages into a fault",
        );
        assert_eq!(
            row.last_reason.as_deref(),
            Some(DISABLED_IN_CONFIG),
            "must read as disabled, not as waiting for a market it would not trade",
        );
        assert_ne!(
            row.last_reason.as_deref(),
            Some(IDLE_NO_MARKET),
            "a switched-off viper is not waiting for anything",
        );
        forget("idletest-e");
    }

    /// The reason timestamp marks when the wait BEGAN, so repeated idle ticks
    /// must leave it alone. Otherwise every wait reads as "just now".
    #[test]
    fn repeated_idle_ticks_keep_the_original_wait_start() {
        record_idle("idletest-b", "MomentumStrategy", IDLE_NO_MARKET);
        let first = {
            let map = registry().lock().unwrap();
            map.get(&key("idletest-b", "MomentumStrategy")).unwrap().last_reason_at
        };
        assert!(first.is_some());
        std::thread::sleep(std::time::Duration::from_millis(15));
        record_idle("idletest-b", "MomentumStrategy", IDLE_NO_MARKET);
        let (again, eval_at) = {
            let map = registry().lock().unwrap();
            let st = map.get(&key("idletest-b", "MomentumStrategy")).unwrap();
            (st.last_reason_at, st.last_eval_at)
        };
        assert_eq!(first, again, "the wait start must not move on a later idle tick");
        assert!(eval_at > first.unwrap(), "liveness must still advance");
        forget("idletest-b");
    }

    /// A row that comes back to life must not keep reading as idle: a real
    /// evaluation overwrites the outcome, and a fresh signal clears the reason.
    #[test]
    fn a_real_evaluation_ends_the_idle_state() {
        record_idle("idletest-c", "GboostStrategy", IDLE_NO_MARKET);
        record_eval("idletest-c", "GboostStrategy", EvalOutcome::NoSignal);
        let snap = snapshot(Some("idletest-c"));
        let row = snap.iter().find(|r| r.strategy == "GboostStrategy").unwrap();
        assert_eq!(row.last_outcome, EvalOutcome::NoSignal);
        // The waiting text lingers until a gate or a signal replaces it, exactly
        // like any other reason; the badge is keyed on the outcome, not on it.
        record_eval("idletest-c", "GboostStrategy", EvalOutcome::Signal);
        let snap = snapshot(Some("idletest-c"));
        let row = snap.iter().find(|r| r.strategy == "GboostStrategy").unwrap();
        assert!(row.last_reason.is_none());
        forget("idletest-c");
    }

    #[test]
    fn idle_serializes_as_the_string_the_control_tower_keys_on() {
        assert_eq!(serde_json::to_string(&EvalOutcome::Idle).unwrap(), "\"idle\"");
    }
}

#[cfg(test)]
mod forget_tests {
    use super::*;

    /// A rotated-away underlying must leave nothing behind.
    ///
    /// The Kalshi crypto loop rotates BTC↔ETH and retires the old squadron from
    /// the CAG, but its viper rows used to survive: they counted toward the
    /// ribbon's "across N squadrons", stopped being evaluated, aged past the
    /// staleness window, and then read as "5 stale/error — check squadron
    /// detail" for a squadron that was no longer listed. On a fresh AMI that was
    /// the first thing an operator saw.
    #[test]
    fn forgetting_an_asset_drops_all_of_its_rows() {
        for s in ["MakerStrategy", "ArbitrageStrategy", "GboostStrategy"] {
            record_eval("forgettest-eth", s, EvalOutcome::NoSignal);
            record_eval("forgettest-btc", s, EvalOutcome::NoSignal);
        }
        let mine = |a: &str| snapshot(Some(a)).len();
        assert_eq!(mine("forgettest-eth"), 3);
        assert_eq!(mine("forgettest-btc"), 3);

        forget("forgettest-eth");

        assert_eq!(mine("forgettest-eth"), 0, "the retired underlying must leave no rows");
        assert_eq!(mine("forgettest-btc"), 3, "the surviving squadron must be untouched");
        forget("forgettest-btc");
    }

    /// Case-insensitive, since squadron ids are lowercased while callers may not be.
    ///
    /// The underlying key must be unique to this test. `REGISTRY` is a process
    /// global and cargo runs these two tests on parallel threads, so when this
    /// used "forgettest-ETH" it shared a logical key with the test above — which
    /// records three rows under the same name and then forgets them. The row
    /// counts asserted here would intermittently read 4, or 0, depending on the
    /// interleaving, and the suite failed perhaps one run in three.
    #[test]
    fn forget_matches_regardless_of_case() {
        record_eval("forgettest-case-ETH", "MakerStrategy", EvalOutcome::NoSignal);
        assert_eq!(snapshot(Some("forgettest-case-eth")).len(), 1);
        forget("FORGETTEST-CASE-ETH");
        assert_eq!(snapshot(Some("forgettest-case-eth")).len(), 0);
    }
}
