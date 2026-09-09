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

/// Distinct refusal kinds tracked per viper before the rest are lumped into
/// `other`. Reasons are normalized before keying (see [`normalize_reason`]), so
/// a viper produces a few dozen kinds at most; the cap only guards against a
/// gate that interpolates something the normalizer does not recognize.
const MAX_REFUSAL_KINDS: usize = 32;

/// One refusal kind's running tally.
#[derive(Debug, Clone)]
struct RefusalCounter {
    /// Ticks refused for this reason since the process started.
    total: u64,
    /// Ticks refused since the ledger was last taken by the LLM Advisor.
    since_report: u64,
    /// The most recent verbatim reason, live numbers included.
    last_detail: String,
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
    /// Refusal ledger: how many ticks each named gate has vetoed this viper,
    /// keyed by the reason with its live numbers normalized out.
    ///
    /// `last_reason` answers "what is holding it right now?"; this answers
    /// "what has been holding it, and how often?" — the question the LLM
    /// Advisor needs. Before this existed the advisor was shown executed
    /// trades and knob values only, and from that input the one inference
    /// available was "loosen something": on 2026-08-31 every one of 24 queued
    /// proposals loosened a gate while the engine had logged ~1,100 refusals
    /// with precise causes that never reached the model.
    refusals: HashMap<String, RefusalCounter>,
    /// When `since_report` counters were last zeroed; the advisor prints the
    /// window so the counts have a denominator.
    refusal_window_started_at: DateTime<Utc>,
}

impl ViperStatus {
    fn fresh(now: DateTime<Utc>, outcome: EvalOutcome) -> Self {
        Self {
            last_eval_at: now,
            last_outcome: outcome,
            last_reason: None,
            last_reason_at: None,
            last_signal_at: None,
            refusals: HashMap::new(),
            refusal_window_started_at: now,
        }
    }

    fn bump_refusal(&mut self, reason: &str) {
        // A switched-off viper is not being refused by a gate; its config
        // section already tells the advisor it is off.
        if reason == DISABLED_IN_CONFIG {
            return;
        }
        let kind = normalize_reason(reason);
        let key = if self.refusals.contains_key(&kind) || self.refusals.len() < MAX_REFUSAL_KINDS {
            kind
        } else {
            "other".to_string()
        };
        let c = self.refusals.entry(key).or_insert_with(|| RefusalCounter {
            total: 0,
            since_report: 0,
            last_detail: String::new(),
        });
        c.total += 1;
        c.since_report += 1;
        if c.last_detail != reason {
            c.last_detail = reason.to_string();
        }
    }
}

/// Collapse the live numbers out of a gate's reason so ticks that differ only
/// in the quoted values count as one kind.
///
/// Every maximal run of `0-9 . , + - $` that contains a digit becomes `#`, so
/// a currency sign, a sign and the digits collapse together. So
/// `spread 0.0100 below fee floor 0.0247 — unquotable at any min_spread`
/// becomes `spread # below fee floor # — unquotable at any min_spread`, and
/// `net_exposure $12.50 > max $10.00` becomes `net_exposure # > max #`. Words
/// containing digits (`velocity_5s`, `0.50`) are left alone only when the digit
/// run is glued to letters on both sides — `drift_60m` keeps its name.
pub fn normalize_reason(reason: &str) -> String {
    let chars: Vec<char> = reason.chars().collect();
    let mut out = String::with_capacity(reason.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let numeric = |ch: char| ch.is_ascii_digit() || matches!(ch, '.' | ',' | '+' | '-' | '$');
        if numeric(c) {
            let start = i;
            let mut j = i;
            while j < chars.len() && numeric(chars[j]) {
                j += 1;
            }
            let run: String = chars[start..j].iter().collect();
            let has_digit = run.chars().any(|ch| ch.is_ascii_digit());
            let glued_before = start > 0 && (chars[start - 1].is_ascii_alphabetic() || chars[start - 1] == '_');
            let glued_after = j < chars.len() && chars[j].is_ascii_alphabetic() && glued_before;
            if has_digit && !glued_after {
                out.push('#');
            } else {
                out.push_str(&run);
            }
            i = j;
        } else {
            out.push(c);
            i += 1;
        }
    }
    out
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
    let entry = map.entry(key(asset, strategy)).or_insert_with(|| ViperStatus::fresh(now, outcome));
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
    let entry = map.entry(key(asset, strategy)).or_insert_with(|| ViperStatus::fresh(now, EvalOutcome::Idle));
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
/// Called from inside instrumented vipers' entry gates; cheap overwrite of
/// the displayed reason, plus one tick on that reason's ledger entry.
pub fn report_reason(asset: &str, strategy: &str, reason: &str) {
    let now = Utc::now();
    let mut map = match registry().lock() {
        Ok(m) => m,
        Err(p) => p.into_inner(),
    };
    let entry = map.entry(key(asset, strategy)).or_insert_with(|| ViperStatus::fresh(now, EvalOutcome::NoSignal));
    entry.last_reason = Some(reason.to_string());
    entry.last_reason_at = Some(now);
    entry.bump_refusal(reason);
}

/// Report a refusal that has one displayed reason but several countable
/// causes.
///
/// The Maker quotes two legs and, when neither qualifies, reports a single
/// composite line — `no side qualifies | YES: <a> | NO: <b>` — which is the
/// right thing to display and the wrong thing to count: the advisor needs to
/// know that the YES leg was unquotable under the fee floor 584 times and the
/// NO leg had no seller 691 times, not that "no side qualifies" happened 700
/// times. `composite` becomes `last_reason`; each of `legs` gets a ledger tick.
pub fn report_leg_refusals(asset: &str, strategy: &str, composite: &str, legs: &[&str]) {
    let now = Utc::now();
    let mut map = match registry().lock() {
        Ok(m) => m,
        Err(p) => p.into_inner(),
    };
    let entry = map.entry(key(asset, strategy)).or_insert_with(|| ViperStatus::fresh(now, EvalOutcome::NoSignal));
    entry.last_reason = Some(composite.to_string());
    entry.last_reason_at = Some(now);
    for leg in legs {
        entry.bump_refusal(leg);
    }
}

/// One refusal kind's tally, as served to the Control Tower and the advisor.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RefusalTally {
    /// The reason with live numbers replaced by `#` — see [`normalize_reason`].
    pub reason: String,
    /// Ticks refused for this reason since the process started.
    pub count: u64,
    /// Ticks refused for this reason since the advisor last took the ledger.
    pub count_since_report: u64,
    /// The most recent verbatim reason, live numbers included.
    pub last_detail: String,
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
    /// The refusal ledger, most frequent first, capped at
    /// `VIEW_REFUSAL_KINDS` entries.
    pub refusals: Vec<RefusalTally>,
}

/// How many ledger entries a status row carries. The Control Tower shows the
/// top few; the advisor takes its own, fuller report.
const VIEW_REFUSAL_KINDS: usize = 5;

fn tallies(st: &ViperStatus, by_window: bool, limit: usize) -> Vec<RefusalTally> {
    let mut v: Vec<RefusalTally> = st.refusals.iter()
        .map(|(reason, c)| RefusalTally {
            reason: reason.clone(),
            count: c.total,
            count_since_report: c.since_report,
            last_detail: c.last_detail.clone(),
        })
        .collect();
    // Ties broken by reason text so the order is stable between calls.
    v.sort_by(|a, b| {
        let (ka, kb) = if by_window {
            (a.count_since_report, b.count_since_report)
        } else {
            (a.count, b.count)
        };
        kb.cmp(&ka).then_with(|| a.reason.cmp(&b.reason))
    });
    v.truncate(limit);
    v
}

/// One viper's refusal ledger over the window since the advisor last took it.
#[derive(Debug, Clone, Serialize)]
pub struct ViperRefusalReport {
    pub asset: String,
    pub strategy: String,
    /// Seconds the `since_report` counters have been accumulating.
    pub window_secs: i64,
    /// Refused ticks in the window, all reasons.
    pub ticks: u64,
    /// Most frequent first, by the window count.
    pub reasons: Vec<RefusalTally>,
    /// The viper's current displayed reason, so the report can say what is
    /// holding it right now as well as what has been.
    pub last_reason: Option<String>,
}

/// Distinct reasons carried per viper in an advisor report. Enough to show
/// the shape of the refusals without pasting the whole ledger into a prompt
/// that a 3B-parameter local model has to read.
pub const REPORT_REFUSAL_KINDS: usize = 3;

/// The refusal ledger for one asset's vipers, ranked by the window counts.
///
/// `take` zeroes the window counters and restarts the window, so the next
/// report covers only what happened after this one. The advisor peeks first
/// (to decide whether anything changed) and takes only when it actually
/// spends a provider call, so a skipped cycle lets the window keep growing
/// rather than losing its counts.
pub fn refusal_report(asset: &str, take: bool) -> Vec<ViperRefusalReport> {
    let now = Utc::now();
    let asset = asset.to_lowercase();
    let mut map = match registry().lock() {
        Ok(m) => m,
        Err(p) => p.into_inner(),
    };
    let mut out: Vec<ViperRefusalReport> = map.iter_mut()
        .filter(|((a, _), _)| *a == asset)
        .map(|((a, name), st)| {
            let report = ViperRefusalReport {
                asset: a.clone(),
                strategy: name.clone(),
                window_secs: (now - st.refusal_window_started_at).num_seconds(),
                ticks: st.refusals.values().map(|c| c.since_report).sum(),
                reasons: tallies(st, true, REPORT_REFUSAL_KINDS),
                last_reason: st.last_reason.clone(),
            };
            if take {
                for c in st.refusals.values_mut() {
                    c.since_report = 0;
                }
                st.refusal_window_started_at = now;
            }
            report
        })
        .collect();
    out.sort_by(|a, b| a.strategy.cmp(&b.strategy));
    out
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
        refusals: tallies(st, false, VIEW_REFUSAL_KINDS),
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

#[cfg(test)]
mod refusal_ledger_tests {
    use super::*;

    /// The Maker's fee-floor line carries two live prices that change every
    /// tick. If they were part of the key, every tick would be its own kind and
    /// the ledger would be a log, not a tally.
    #[test]
    fn live_numbers_are_normalized_out_of_the_reason() {
        assert_eq!(
            normalize_reason("spread 0.0100 below fee floor 0.0247 — unquotable at any min_spread"),
            "spread # below fee floor # — unquotable at any min_spread",
        );
        assert_eq!(normalize_reason("market_age 12s < min 600s"), "market_age #s < min #s");
        assert_eq!(normalize_reason("net_exposure $12.50 > max $10.00"), "net_exposure # > max #");
        assert_eq!(normalize_reason("cooldown active (37s left)"), "cooldown active (#s left)");
        assert_eq!(
            normalize_reason("oracle too flat (hist_vol=0.0004 < min=0.0010)"),
            "oracle too flat (hist_vol=# < min=#)",
        );
        assert_eq!(
            normalize_reason("adverse OBI (yes_obi=-0.72 < -0.60)"),
            "adverse OBI (yes_obi=# < #)",
        );
    }

    /// Names that happen to contain digits are identity, not measurement.
    #[test]
    fn digits_inside_identifiers_survive_normalization() {
        assert_eq!(
            normalize_reason("counter-trend (drift_60m=$-1234 < -$500)"),
            "counter-trend (drift_60m=# < #)",
        );
        assert_eq!(normalize_reason("10m/60m drift disagree (counter-trend)"), "#m/#m drift disagree (counter-trend)");
        assert_eq!(normalize_reason("no seller on this leg"), "no seller on this leg");
    }

    /// Ticks that differ only in their numbers accumulate under one kind, and
    /// the latest verbatim line is kept as the example.
    #[test]
    fn repeated_refusals_tally_under_one_kind() {
        let a = "ledgertest-a";
        report_reason(a, "MakerStrategy", "spread 0.0100 below fee floor 0.0247 — unquotable at any min_spread");
        report_reason(a, "MakerStrategy", "spread 0.0090 below fee floor 0.0251 — unquotable at any min_spread");
        report_reason(a, "MakerStrategy", "no seller on this leg");
        let snap = snapshot(Some(a));
        let row = snap.iter().find(|r| r.strategy == "MakerStrategy").unwrap();
        assert_eq!(row.refusals.len(), 2);
        let fee = &row.refusals[0];
        assert_eq!(fee.reason, "spread # below fee floor # — unquotable at any min_spread");
        assert_eq!(fee.count, 2);
        assert_eq!(fee.last_detail, "spread 0.0090 below fee floor 0.0251 — unquotable at any min_spread");
        assert_eq!(row.refusals[1].reason, "no seller on this leg");
        assert_eq!(row.refusals[1].count, 1);
        forget(a);
    }

    /// The Maker's composite "no side qualifies" line is what the operator
    /// sees; the legs are what gets counted.
    #[test]
    fn leg_refusals_display_the_composite_and_count_each_leg() {
        let a = "ledgertest-legs";
        for _ in 0..3 {
            report_leg_refusals(
                a, "MakerStrategy",
                "no side qualifies | YES: spread 0.010 below fee floor 0.025 — unquotable at any min_spread | NO: no seller on this leg",
                &["spread 0.010 below fee floor 0.025 — unquotable at any min_spread", "no seller on this leg"],
            );
        }
        let snap = snapshot(Some(a));
        let row = snap.iter().find(|r| r.strategy == "MakerStrategy").unwrap();
        assert!(row.last_reason.as_deref().unwrap().starts_with("no side qualifies"));
        assert!(row.refusals.iter().all(|t| !t.reason.starts_with("no side qualifies")),
            "the composite must not be tallied as a kind of its own");
        assert_eq!(row.refusals.iter().map(|t| t.count).sum::<u64>(), 6);
        forget(a);
    }

    /// The advisor's report covers the window since it last took the ledger:
    /// a take zeroes the window counts, leaves the lifetime counts alone, and
    /// a peek changes nothing.
    #[test]
    fn taking_the_report_resets_the_window_but_not_the_lifetime_counts() {
        let a = "ledgertest-window";
        for _ in 0..5 {
            report_reason(a, "GboostStrategy", "oracle too flat (hist_vol=0.0004 < min=0.0010)");
        }
        let peek = refusal_report(a, false);
        assert_eq!(peek.len(), 1);
        assert_eq!(peek[0].ticks, 5);
        assert_eq!(peek[0].reasons[0].count_since_report, 5);

        let taken = refusal_report(a, true);
        assert_eq!(taken[0].ticks, 5, "the take itself must still report the window it closes");

        report_reason(a, "GboostStrategy", "oracle too flat (hist_vol=0.0003 < min=0.0010)");
        let next = refusal_report(a, false);
        assert_eq!(next[0].ticks, 1, "only ticks after the take belong to the new window");
        assert_eq!(next[0].reasons[0].count_since_report, 1);
        assert_eq!(next[0].reasons[0].count, 6, "lifetime count is untouched by a take");
        assert_eq!(next[0].last_reason.as_deref(), Some("oracle too flat (hist_vol=0.0003 < min=0.0010)"));
        forget(a);
    }

    /// "Disabled in config" is the operator's choice, not a gate refusing;
    /// counting it would tell the advisor a switched-off viper is being blocked.
    #[test]
    fn a_disabled_viper_is_not_a_refusal() {
        let a = "ledgertest-disabled";
        report_reason(a, "BasisStrategy", DISABLED_IN_CONFIG);
        let report = refusal_report(a, false);
        assert_eq!(report[0].ticks, 0);
        assert!(report[0].reasons.is_empty());
        forget(a);
    }

    /// A gate that interpolates something the normalizer does not fold must
    /// not grow the ledger without bound.
    #[test]
    fn the_ledger_is_bounded() {
        let a = "ledgertest-bound";
        for i in 0..(MAX_REFUSAL_KINDS + 10) {
            report_reason(a, "MomentumStrategy", &format!("reason variant {}", char::from(b'a' + (i % 26) as u8).to_string().repeat(i + 1)));
        }
        let map = registry().lock().unwrap();
        let st = map.get(&key(a, "MomentumStrategy")).unwrap();
        assert!(st.refusals.len() <= MAX_REFUSAL_KINDS + 1, "at most the cap plus the 'other' bucket");
        assert_eq!(st.refusals.values().map(|c| c.total).sum::<u64>() as usize, MAX_REFUSAL_KINDS + 10);
        assert!(st.refusals.contains_key("other"));
        drop(map);
        forget(a);
    }
}
