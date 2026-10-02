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

//! Helm intents: the operator's stated conviction, written down before anything
//! acts on it.
//!
//! An intent is the record a Helm position is built from and judged against. It
//! carries the thesis, a probability, a horizon, the condition that would prove
//! the thesis wrong, and the structured exit posture the engine will enforce.
//! The pre-registration discipline DRADIS applies to its research spikes is
//! applied here to the operator: the claim is stored first, and what is stored
//! is what was believed.
//!
//! **Two rules this module enforces by shape rather than by convention.**
//!
//! 1. *The first submission is immutable.* The `helm_intents` row holds the
//!    content exactly as first submitted and is never updated in place. Every
//!    later edit — after reading the critique, after a tightening — is a row in
//!    `helm_intent_revisions`, numbered from 2, and `current_version` says which
//!    one the engine reads. The retrospective can always see what the operator
//!    first wrote, and whether reading the critique changed it.
//! 2. *Status moves only along the machine.* `IntentStatus::can_transition_to`
//!    is the single definition, `transition` is the single writer, and every
//!    move is appended to `helm_intent_events`. A terminal status has no exit.
//!
//! This module stores and reads. It places no order and emits no signal; the
//! strategy that will act on an intent (`vipers::helm_impl`) is inert while the
//! entry and exit increments are unbuilt.

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Fee-share ceiling for the fee verdict: the round-trip fee as a share of the
/// take-profit target above which an entry is called fee-dominated. The same
/// figure Momentum and Convergence refuse at; Helm shows it and lets the
/// operator decide. One constant, read by the API's verdict and the strategy's
/// entry log alike.
pub const FEE_VERDICT_MAX_RATIO: Decimal = dec!(0.40);
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::str::FromStr;

// ── Status machine ───────────────────────────────────────────────────────────

/// Where an intent is in its life.
///
/// `proposed → acknowledged → working → filled | partial | missed → closed | superseded`
///
/// `closed` and `superseded` are terminal. `partial → filled` is the one lateral
/// move: a partially filled entry that completes is the same intent, not a new
/// one. A `missed` entry does not re-arm in place; the operator supersedes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentStatus {
    /// Submitted. The critique (a later increment) runs against this version.
    Proposed,
    /// The operator has read the critique and confirmed. Eligible for entry.
    Acknowledged,
    /// The entry order is on the book or in flight.
    Working,
    /// The entry filled in full; the exit posture is managing the position.
    Filled,
    /// The entry filled in part; the posture manages what filled.
    Partial,
    /// The entry did not fill in its window.
    Missed,
    /// Done: exited, settled, cancelled or expired. `close_reason` says which.
    Closed,
    /// Replaced by a new intent (`superseded_by`), typically a loosening.
    Superseded,
}

impl IntentStatus {
    pub const ALL: [IntentStatus; 8] = [
        Self::Proposed, Self::Acknowledged, Self::Working, Self::Filled,
        Self::Partial, Self::Missed, Self::Closed, Self::Superseded,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Proposed => "proposed",
            Self::Acknowledged => "acknowledged",
            Self::Working => "working",
            Self::Filled => "filled",
            Self::Partial => "partial",
            Self::Missed => "missed",
            Self::Closed => "closed",
            Self::Superseded => "superseded",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|st| st.as_str() == s.trim().to_ascii_lowercase())
    }

    /// No transitions leave a terminal status.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Closed | Self::Superseded)
    }

    /// The machine, in one place.
    pub fn can_transition_to(self, to: Self) -> bool {
        use IntentStatus::*;
        match (self, to) {
            (Proposed, Acknowledged) => true,
            (Acknowledged, Working) => true,
            (Working, Filled | Partial | Missed) => true,
            (Partial, Filled) => true,
            (from, Closed | Superseded) => !from.is_terminal(),
            _ => false,
        }
    }
}

/// Ride to expiry, or expect to exit sooner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Horizon { Expiry, Sooner }

impl Horizon {
    pub fn as_str(self) -> &'static str {
        match self { Self::Expiry => "expiry", Self::Sooner => "sooner" }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "expiry" => Some(Self::Expiry),
            "sooner" => Some(Self::Sooner),
            _ => None,
        }
    }
}

/// Take the ask now, or rest a bid and wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind { Taker, Resting }

impl EntryKind {
    pub fn as_str(self) -> &'static str {
        match self { Self::Taker => "taker", Self::Resting => "resting" }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "taker" => Some(Self::Taker),
            "resting" => Some(Self::Resting),
            _ => None,
        }
    }
}

// ── Content ──────────────────────────────────────────────────────────────────

/// Everything the operator states. Stored verbatim as version 1 on the intent
/// row; every later edit is a revision carrying a complete copy.
///
/// The posture fields are structured on purpose. "What would make me wrong" is
/// prose, and prose is kept; but the stop the engine enforces is a number the
/// operator typed, not a number a model inferred from the prose.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntentContent {
    /// What you believe and why.
    pub thesis: String,
    /// Probability the thesis resolves in your favor, strictly inside (0, 1).
    pub confidence: f64,
    pub horizon: Horizon,
    /// The falsification condition, in the operator's words.
    pub falsification: String,
    pub entry_kind: EntryKind,
    /// Resting bid price; required for `Resting`, absent for `Taker`.
    #[serde(default)]
    pub entry_limit_price: Option<Decimal>,
    /// Notional to commit, in USDC.
    pub size_usdc: Decimal,
    /// Stop price on the held token, or none.
    #[serde(default)]
    pub stop_price: Option<Decimal>,
    /// Resting take-profit price on the held token, or none.
    #[serde(default)]
    pub take_profit_price: Option<Decimal>,
    /// Exit at this time if still held, or none.
    #[serde(default)]
    pub time_limit_at: Option<DateTime<Utc>>,
    /// Hold to the market's resolution rather than stopping on price.
    #[serde(default)]
    pub hold_to_settlement: bool,
    /// Insurance floor as a fraction of entry (e.g. 0.30), armed under a hold.
    #[serde(default)]
    pub catastrophic_floor_pct: Option<Decimal>,
}

/// What the write surface needs to open an intent.
#[derive(Clone, Debug)]
pub struct NewIntent {
    pub squadron_id: String,
    /// The venue's condition id, when known; the registry does not keep it,
    /// `deployment_queue` does (`market_id_for_squadron`).
    pub market_id: String,
    pub market_name: String,
    /// `YES` or `NO`.
    pub side: String,
    pub content: IntentContent,
    /// Simulated squadron at the time of submission.
    pub ghost: bool,
    pub venue: String,
}

/// One stored intent, as the API returns it.
#[derive(Clone, Debug, Serialize)]
pub struct HelmIntent {
    pub id: i64,
    pub squadron_id: String,
    pub market_id: String,
    pub market_name: String,
    pub side: String,
    /// Version 1, exactly as first submitted. Never changes.
    pub first: IntentContent,
    /// Which version the engine reads; 1 means `first`.
    pub current_version: i64,
    pub status: IntentStatus,
    pub status_detail: Option<String>,
    pub critique: Option<String>,
    pub critique_at: Option<String>,
    pub critique_model: Option<String>,
    pub superseded_by: Option<i64>,
    pub close_reason: Option<String>,
    pub ghost: bool,
    pub venue: String,
    pub session_id: String,
    pub created_at: String,
    pub acknowledged_at: Option<String>,
    pub updated_at: String,
    pub closed_at: Option<String>,
    /// The token the side resolved to when the entry went to work.
    pub token_id: Option<String>,
    /// The price the entry order was priced at.
    pub entry_price: Option<Decimal>,
    /// Shares the entry asked for; a fill below this is `partial`.
    pub entry_shares: Option<Decimal>,
    /// When the entry went to work (RFC 3339).
    pub working_at: Option<String>,
    /// `fee_dominated_entry`'s verdict at creation (and again at
    /// acknowledgement), or "fee share within limits".
    pub fee_verdict: Option<String>,
    /// When the critique was requested; `critique` None past the timeout
    /// means unavailable.
    pub critique_requested_at: Option<String>,
    /// Scored after the fact by the operator: `named_it`, `missed_it`,
    /// `no_critique`.
    pub critique_outcome: Option<String>,
}

/// How the critique is scored once the intent has resolved: did it name the
/// failure mode that actually happened? Recorded now so the retrospective can
/// read the critique as a calibration record of its own.
pub const CRITIQUE_OUTCOMES: [&str; 3] = ["named_it", "missed_it", "no_critique"];

impl HelmIntent {
    /// Has the critique answered (with text or as unavailable), or is it still
    /// pending inside its timeout? `None` when it has not been requested.
    pub fn critique_pending_for(&self, now: DateTime<Utc>) -> Option<i64> {
        if self.critique.is_some() {
            return None;
        }
        let at = DateTime::parse_from_rfc3339(self.critique_requested_at.as_deref()?).ok()?;
        Some((now - at.with_timezone(&Utc)).num_seconds())
    }
    /// Seconds since the entry went to work, or none if it has not.
    pub fn working_for_secs(&self, now: DateTime<Utc>) -> Option<i64> {
        let at = DateTime::parse_from_rfc3339(self.working_at.as_deref()?).ok()?;
        Some((now - at.with_timezone(&Utc)).num_seconds())
    }

    /// The statuses in which the intent owns, or is acquiring, a position.
    pub fn is_in_flight(&self) -> bool {
        matches!(self.status, IntentStatus::Working | IntentStatus::Partial | IntentStatus::Filled)
    }
}

/// A later version of an intent's content.
#[derive(Clone, Debug, Serialize)]
pub struct IntentRevision {
    pub id: i64,
    pub intent_id: i64,
    pub version: i64,
    /// Why the operator revised: shown beside the diff in the retrospective.
    pub reason: String,
    pub content: IntentContent,
    pub created_at: String,
}

/// One status move.
#[derive(Clone, Debug, Serialize)]
pub struct IntentEvent {
    pub id: i64,
    pub intent_id: i64,
    pub at: String,
    pub from_status: Option<IntentStatus>,
    pub to_status: IntentStatus,
    pub detail: Option<String>,
}

/// How many intents a squadron has, and how many are still open.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct IntentTally {
    pub total: i64,
    /// Not terminal.
    pub open: i64,
}

impl IntentTally {
    /// Every intent the squadron was given has reached a terminal status.
    /// False for a squadron that was never given one: it has nothing to be
    /// complete about and retires on the market's close like any other.
    pub fn complete(self) -> bool {
        self.total > 0 && self.open == 0
    }
}

// ── Shape check ──────────────────────────────────────────────────────────────

/// Is the content well formed? Structure only: a probability inside (0, 1), a
/// side that exists, a resting entry with a price, a size above zero, prices
/// inside the market's range. This is not the deterministic validator — that
/// also reads the book (a stop on the held side of the bid, a time limit inside
/// the close) and arrives with the form. Nothing here depends on market state,
/// so nothing here can be gamed except by filling the form honestly.
pub fn shape_check(side: &str, c: &IntentContent) -> Result<(), Vec<String>> {
    let mut errs = Vec::new();
    if !matches!(side.trim().to_ascii_uppercase().as_str(), "YES" | "NO") {
        errs.push(format!("side must be YES or NO, got {side:?}"));
    }
    if c.thesis.trim().is_empty() {
        errs.push("thesis is empty".into());
    }
    if c.falsification.trim().is_empty() {
        errs.push("falsification condition is empty".into());
    }
    if !(c.confidence.is_finite() && c.confidence > 0.0 && c.confidence < 1.0) {
        errs.push(format!("confidence must be strictly inside (0, 1), got {}", c.confidence));
    }
    if c.size_usdc <= Decimal::ZERO {
        errs.push(format!("size_usdc must be above zero, got {}", c.size_usdc));
    }
    let in_range = |p: Decimal| p > Decimal::ZERO && p < Decimal::ONE;
    match (c.entry_kind, c.entry_limit_price) {
        (EntryKind::Resting, None) => errs.push("a resting entry needs entry_limit_price".into()),
        (EntryKind::Resting, Some(p)) if !in_range(p) => errs.push(format!("entry_limit_price must be inside (0, 1), got {p}")),
        (EntryKind::Taker, Some(_)) => errs.push("a taker entry takes no entry_limit_price".into()),
        _ => {}
    }
    for (name, v) in [
        ("stop_price", c.stop_price),
        ("take_profit_price", c.take_profit_price),
        ("catastrophic_floor_pct", c.catastrophic_floor_pct),
    ] {
        if let Some(p) = v {
            if !in_range(p) {
                errs.push(format!("{name} must be inside (0, 1), got {p}"));
            }
        }
    }
    // A position nothing will exit is the one thing the engine must never
    // hold on the operator's behalf. Every posture names its exit: a price, a
    // clock, or the market's own resolution — said, not assumed.
    if !c.hold_to_settlement && c.stop_price.is_none() && c.take_profit_price.is_none() && c.time_limit_at.is_none() {
        errs.push("posture names no exit: set a stop_price, a take_profit_price, a time_limit_at, or hold_to_settlement".into());
    }
    if errs.is_empty() { Ok(()) } else { Err(errs) }
}

// ── Book-consistency validation ──────────────────────────────────────────────

/// What the market says right now, as the validator needs it. Built by the
/// API from a live quote and by the strategy from its tick snapshot, so both
/// run the same rules on the same shape.
#[derive(Clone, Copy, Debug)]
pub struct BookFacts {
    /// Best bid and ask of the side being bought.
    pub bid: Decimal,
    pub ask: Decimal,
    pub close_time: Option<DateTime<Utc>>,
    pub now: DateTime<Utc>,
    /// The venue's minimum order, in shares.
    pub min_shares: Decimal,
    /// No entry inside this many seconds of the close.
    pub min_secs_to_close: i64,
}

/// The price an entry would pay under this book: the ask for a taker, the
/// operator's limit for a resting bid. `None` when the book has no ask and
/// the entry needs one.
pub fn entry_price(c: &IntentContent, b: &BookFacts) -> Option<Decimal> {
    match c.entry_kind {
        EntryKind::Taker => (b.ask > Decimal::ZERO && b.ask < Decimal::ONE).then_some(b.ask),
        EntryKind::Resting => c.entry_limit_price,
    }
}

/// The deterministic validator: is this posture consistent with the market as
/// it stands? Every rule is one the operator can read in full and cannot
/// satisfy except by filling the form honestly; none reads a signal. Returns
/// every failing rule, not the first, so the form can show them all.
///
/// Lives here, once, and is called from the API at creation and
/// acknowledgement (with a live quote) and from `HelmStrategy::entry_plan`
/// at entry (with the tick's book), so the same rule cannot exist in two
/// places with two thresholds. The structural rules are `shape_check`; the
/// engine's risk gates (kill switch, live gate, drawdown, exposure cap,
/// collateral) are the strategy's alone, because they are about the engine's
/// state, not the operator's form.
pub fn validate_against_book(side: &str, c: &IntentContent, b: &BookFacts) -> Result<(), Vec<String>> {
    let mut errs = Vec::new();
    let side_u = side.trim().to_ascii_uppercase();

    // Every exit the posture can take sells into the bid; a market with no bid
    // is one the posture could never leave until settlement.
    if b.bid <= Decimal::ZERO {
        errs.push(format!("no bid on the {side_u} book: the posture could not exit"));
    }
    let price = match (c.entry_kind, entry_price(c, b)) {
        (EntryKind::Taker, None) => {
            errs.push(format!("no ask on the {side_u} book"));
            None
        }
        (EntryKind::Resting, None) => {
            errs.push("resting entry has no limit price".into());
            None
        }
        (EntryKind::Resting, Some(limit)) if b.ask > Decimal::ZERO && limit >= b.ask => {
            errs.push(format!("resting bid ${limit:.4} would cross the book (ask ${:.4})", b.ask));
            Some(limit)
        }
        (_, Some(p)) => Some(p),
    };

    // Hold-to-settlement and a price stop are two different theses about the
    // same position. The floor is the insurance under a hold.
    if c.hold_to_settlement && c.stop_price.is_some() {
        errs.push("hold_to_settlement and stop_price cannot both be set: under a hold the insurance is catastrophic_floor_pct".into());
    }
    if let (Some(stop), Some(p)) = (c.stop_price, price) {
        if !c.hold_to_settlement {
            // On the held side of the bid: below both what we pay and what the
            // book would pay us now, or it fires on the first tick.
            if stop >= p {
                errs.push(format!("stop ${stop:.4} is at or above the entry price ${p:.4}: it would fire at once"));
            } else if b.bid > Decimal::ZERO && stop >= b.bid {
                errs.push(format!("stop ${stop:.4} is at or above the current bid ${:.4}: it would fire at once", b.bid));
            }
        }
    }
    if let (Some(tp), Some(p)) = (c.take_profit_price, price) {
        if tp <= p {
            errs.push(format!("take-profit ${tp:.4} is at or below the entry price ${p:.4}"));
        }
    }
    if let Some(t) = c.time_limit_at {
        if t <= b.now {
            errs.push("time limit is already in the past".into());
        } else if let Some(close) = b.close_time {
            if t >= close {
                errs.push(format!("time limit {} is at or after the market's close {}", t.to_rfc3339(), close.to_rfc3339()));
            }
        }
    }
    match b.close_time {
        None if c.hold_to_settlement => errs.push("hold-to-settlement needs a market with a close time".into()),
        Some(close) => {
            let s = (close - b.now).num_seconds();
            if s < b.min_secs_to_close {
                errs.push(format!("too close to market close ({s}s left, min {})", b.min_secs_to_close));
            }
        }
        None => {}
    }
    if let Some(p) = price {
        if p > Decimal::ZERO {
            let shares = c.size_usdc / p;
            if shares < b.min_shares {
                errs.push(format!(
                    "size ${} buys {shares:.2} shares at ${p:.4}, below the venue minimum of {}",
                    c.size_usdc, b.min_shares,
                ));
            }
        }
    }
    if errs.is_empty() { Ok(()) } else { Err(errs) }
}

/// What the round trip costs, and whether that is reason enough to refuse.
#[derive(Clone, Debug)]
pub struct FeeVerdict {
    /// The sentence recorded on the intent and shown to the operator.
    pub text: String,
    /// `Some` when the entry should be refused, carrying the reason. The caller
    /// decides whether to act on it — `helm_fee_verdict_enforce`.
    pub refusal: Option<String>,
}

impl FeeVerdict {
    fn pass(text: String) -> Self { Self { text, refusal: None } }
    fn refuse(text: String) -> Self { Self { text: text.clone(), refusal: Some(text) } }
}

/// Can anything take this position out at a price before it resolves?
///
/// `hold_to_settlement` on its own means the only exit is resolution, which
/// charges no taker fee. But `posture_action` evaluates the catastrophic floor
/// and the time limit BEFORE it consults `hold_to_settlement` — that flag only
/// suppresses the stop. So "hold to settlement, exit at 3pm" really does exit
/// at 3pm, as a taker, and treating it as a settlement posture would hand the
/// most generous verdict in the system to a trade that pays the full round trip.
fn settles_without_selling(c: &IntentContent) -> bool {
    c.hold_to_settlement && c.time_limit_at.is_none() && c.catastrophic_floor_pct.is_none()
}

/// What the posture will actually be charged, as a fraction of entry notional.
///
/// Legs are counted, not assumed. A post-only order pays nothing — the CLOB
/// charges the taker — so a resting entry is free to open and a resting
/// take-profit is free to close. Charging a flat round trip everywhere refused
/// entries that cost one leg or none at all.
fn expected_fee_pct(c: &IntentContent, p: Decimal, exit_is_taker: bool) -> Decimal {
    let entry = match c.entry_kind {
        EntryKind::Resting => Decimal::ZERO,                      // post-only bid
        EntryKind::Taker   => crate::venues::entry_only_fee_pct(p),
    };
    let exit = if exit_is_taker { crate::venues::exit_only_fee_pct(p) } else { Decimal::ZERO };
    entry + exit
}

/// The fee verdict the operator is shown before acknowledging, and the gate.
///
/// Three postures, and only two of them have a denominator:
///
/// 1. **A take-profit.** The target is stated, so the fee's share of it is the
///    question and `max_ratio` answers it. The take-profit rests post-only, so
///    only the entry leg is charged — and nothing at all for a resting entry.
/// 2. **Hold to settlement, with nothing that can sell early.** The target is
///    the distance to $1.00, the best any binary can pay, and resolution charges
///    no taker fee, so only the entry leg costs anything. The most fee-efficient
///    posture there is, and the ratio almost never binds on it. See
///    `settles_without_selling` for why "hold, but exit at 3pm" is not this.
/// 3. **A stop or a time limit alone.** No price target exists, so there is no
///    ratio to compute. A ratio gate is structurally incapable of judging this
///    case, which is how intent #3 of 2026-10-02 was entered: a time limit, no
///    take-profit, exited flat at $0.1500, and the entire loss was the fee. The
///    only honest gate is absolute — with no stated target the fee IS the hurdle
///    the conviction has to clear, so it is measured against notional and
///    `max_notional_pct` refuses the rest.
///
/// Note which way the curve runs: a leg costs `rate × (1 − p)`, so a cheap
/// longshot is the expensive entry per notional. At the 0.07 intl rate a taker
/// round trip is 11.9% at $0.15 against 1.5% at $0.89, so case 3 bites hardest
/// exactly where a targetless punt is most tempting.
pub fn fee_verdict(
    c: &IntentContent,
    b: &BookFacts,
    max_ratio: Decimal,
    max_notional_pct: Decimal,
) -> FeeVerdict {
    let Some(p) = entry_price(c, b) else {
        // Unreachable from `create`, which refuses a bookless entry first.
        return FeeVerdict::pass("no entry price on the book to measure fees against".to_string());
    };

    // Cases 1 and 2: a measurable target, and an exit that pays no taker fee —
    // a resting take-profit, or resolution.
    let target = c.take_profit_price.filter(|tp| *tp > p).map(|tp| ((tp - p) / p, "take-profit"))
        .or_else(|| settles_without_selling(c).then(|| ((Decimal::ONE - p) / p, "settlement")));

    if let Some((target_pct, which)) = target {
        let fee = expected_fee_pct(c, p, false);
        let share = if target_pct > Decimal::ZERO { fee / target_pct } else { Decimal::ZERO };
        if fee > Decimal::ZERO && share > max_ratio {
            return FeeVerdict::refuse(format!(
                "fee-dominated: fee {:.2}% is {:.0}% of the {:.1}% {which} target at ${p:.2} (max {:.0}%)",
                fee * Decimal::ONE_HUNDRED, share * Decimal::ONE_HUNDRED,
                target_pct * Decimal::ONE_HUNDRED, max_ratio * Decimal::ONE_HUNDRED,
            ));
        }
        return FeeVerdict::pass(format!(
            "fee share within limits: fee {:.2}% is {:.0}% of the {:.1}% {which} target at ${p:.2} (max {:.0}%)",
            fee * Decimal::ONE_HUNDRED, share * Decimal::ONE_HUNDRED,
            target_pct * Decimal::ONE_HUNDRED, max_ratio * Decimal::ONE_HUNDRED,
        ));
    }

    // Case 3: no price target. Whatever closes this crosses the spread, so the
    // exit leg is charged; the entry leg depends on how it was opened.
    let fee = expected_fee_pct(c, p, true);
    if fee > max_notional_pct {
        return FeeVerdict::refuse(format!(
            "fee-dominated: this posture names no price target, so the fee is the hurdle — \
             {:.2}% of notional at ${p:.2} (max {:.2}%). Set a take-profit, or hold to settlement \
             with nothing that sells early.",
            fee * Decimal::ONE_HUNDRED, max_notional_pct * Decimal::ONE_HUNDRED,
        ));
    }
    FeeVerdict::pass(format!(
        "no price target named, so the fee is the hurdle: {:.2}% of notional at ${p:.2} \
         (max {:.2}%). The conviction has to beat the market by at least that much.",
        fee * Decimal::ONE_HUNDRED, max_notional_pct * Decimal::ONE_HUNDRED,
    ))
}

/// Is the calibration figure allowed to be shown? Below the threshold the
/// rows are recorded and nothing is displayed: ten probabilities invite a
/// claim the sample cannot support.
pub fn calibration_visible(resolved: i64, min_resolved: usize) -> bool {
    resolved >= min_resolved as i64
}

// ── Schema ───────────────────────────────────────────────────────────────────

/// Create the Helm tables and add the join columns. Idempotent; called from
/// `db::init_schema` on every pool.
pub(crate) async fn init_schema(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS helm_intents (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            squadron_id            TEXT    NOT NULL,
            market_id              TEXT    NOT NULL DEFAULT '',
            market_name            TEXT    NOT NULL DEFAULT '',
            side                   TEXT    NOT NULL,
            -- version 1, immutable
            thesis                 TEXT    NOT NULL,
            confidence             REAL    NOT NULL,
            horizon                TEXT    NOT NULL,
            falsification          TEXT    NOT NULL,
            entry_kind             TEXT    NOT NULL,
            entry_limit_price      TEXT,
            size_usdc              TEXT    NOT NULL,
            stop_price             TEXT,
            take_profit_price      TEXT,
            time_limit_at          TEXT,
            hold_to_settlement     INTEGER NOT NULL DEFAULT 0,
            catastrophic_floor_pct TEXT,
            -- lifecycle
            current_version        INTEGER NOT NULL DEFAULT 1,
            status                 TEXT    NOT NULL DEFAULT 'proposed',
            status_detail          TEXT,
            critique               TEXT,
            critique_at            TEXT,
            critique_model         TEXT,
            superseded_by          INTEGER,
            close_reason           TEXT,
            ghost                  INTEGER NOT NULL DEFAULT 0,
            venue                  TEXT    NOT NULL DEFAULT '',
            session_id             TEXT    NOT NULL DEFAULT '',
            created_at             TEXT    NOT NULL,
            acknowledged_at        TEXT,
            updated_at             TEXT    NOT NULL,
            closed_at              TEXT
        )"
    ).execute(pool).await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_helm_intents_squadron ON helm_intents (squadron_id, status)"
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS helm_intent_revisions (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            intent_id              INTEGER NOT NULL REFERENCES helm_intents(id),
            version                INTEGER NOT NULL,
            reason                 TEXT    NOT NULL,
            thesis                 TEXT    NOT NULL,
            confidence             REAL    NOT NULL,
            horizon                TEXT    NOT NULL,
            falsification          TEXT    NOT NULL,
            entry_kind             TEXT    NOT NULL,
            entry_limit_price      TEXT,
            size_usdc              TEXT    NOT NULL,
            stop_price             TEXT,
            take_profit_price      TEXT,
            time_limit_at          TEXT,
            hold_to_settlement     INTEGER NOT NULL DEFAULT 0,
            catastrophic_floor_pct TEXT,
            created_at             TEXT    NOT NULL,
            UNIQUE (intent_id, version)
        )"
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS helm_intent_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            intent_id   INTEGER NOT NULL REFERENCES helm_intents(id),
            at          TEXT    NOT NULL,
            from_status TEXT,
            to_status   TEXT    NOT NULL,
            detail      TEXT
        )"
    ).execute(pool).await?;

    // The join the retrospective needs, and the label a purged-and-readopted
    // position keeps. Nullable: every row that is not Helm's stays NULL.
    // `ADD COLUMN` has no IF NOT EXISTS in SQLite; the duplicate-column error
    // on a second start is the no-op, as for every other migration here.
    let _ = sqlx::query("ALTER TABLE open_positions ADD COLUMN intent_id INTEGER").execute(pool).await;
    let _ = sqlx::query("ALTER TABLE trades ADD COLUMN intent_id INTEGER").execute(pool).await;

    // What the entry recorded when the intent went to work: the token the
    // side resolved to, the price the order was priced at, the shares asked
    // for (so a fill can be judged full or partial), and when. Added after the
    // table shipped, hence ALTERs rather than columns in the CREATE.
    for col in [
        "ALTER TABLE helm_intents ADD COLUMN token_id TEXT",
        "ALTER TABLE helm_intents ADD COLUMN entry_price TEXT",
        "ALTER TABLE helm_intents ADD COLUMN entry_shares TEXT",
        "ALTER TABLE helm_intents ADD COLUMN working_at TEXT",
        // The operator surface: the fee verdict shown before acknowledgement,
        // when the critique was asked for (so a row still unanswered past the
        // timeout reads as unavailable), and how the critique scored against
        // what actually happened — did it name the failure mode?
        "ALTER TABLE helm_intents ADD COLUMN fee_verdict TEXT",
        "ALTER TABLE helm_intents ADD COLUMN critique_requested_at TEXT",
        "ALTER TABLE helm_intents ADD COLUMN critique_outcome TEXT",
        "ALTER TABLE helm_intents ADD COLUMN critique_outcome_at TEXT",
    ] {
        let _ = sqlx::query(col).execute(pool).await;
    }
    Ok(())
}

// ── Row mapping ──────────────────────────────────────────────────────────────

fn dec_opt(row: &sqlx::sqlite::SqliteRow, col: &str) -> Option<Decimal> {
    row.try_get::<Option<String>, _>(col).ok().flatten().and_then(|s| Decimal::from_str(&s).ok())
}

fn dec(row: &sqlx::sqlite::SqliteRow, col: &str) -> Decimal {
    dec_opt(row, col).unwrap_or(Decimal::ZERO)
}

fn ts_opt(row: &sqlx::sqlite::SqliteRow, col: &str) -> Option<DateTime<Utc>> {
    row.try_get::<Option<String>, _>(col).ok().flatten()
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|d| d.with_timezone(&Utc))
}

fn content_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<IntentContent> {
    let horizon_s: String = row.try_get("horizon")?;
    let entry_s: String = row.try_get("entry_kind")?;
    Ok(IntentContent {
        thesis: row.try_get("thesis")?,
        confidence: row.try_get("confidence")?,
        horizon: Horizon::parse(&horizon_s).ok_or_else(|| anyhow!("bad horizon {horizon_s:?}"))?,
        falsification: row.try_get("falsification")?,
        entry_kind: EntryKind::parse(&entry_s).ok_or_else(|| anyhow!("bad entry_kind {entry_s:?}"))?,
        entry_limit_price: dec_opt(row, "entry_limit_price"),
        size_usdc: dec(row, "size_usdc"),
        stop_price: dec_opt(row, "stop_price"),
        take_profit_price: dec_opt(row, "take_profit_price"),
        time_limit_at: ts_opt(row, "time_limit_at"),
        hold_to_settlement: row.try_get::<i64, _>("hold_to_settlement").unwrap_or(0) != 0,
        catastrophic_floor_pct: dec_opt(row, "catastrophic_floor_pct"),
    })
}

fn intent_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<HelmIntent> {
    let status_s: String = row.try_get("status")?;
    Ok(HelmIntent {
        id: row.try_get("id")?,
        squadron_id: row.try_get("squadron_id")?,
        market_id: row.try_get("market_id")?,
        market_name: row.try_get("market_name")?,
        side: row.try_get("side")?,
        first: content_from_row(row)?,
        current_version: row.try_get("current_version")?,
        status: IntentStatus::parse(&status_s).ok_or_else(|| anyhow!("bad status {status_s:?}"))?,
        status_detail: row.try_get("status_detail")?,
        critique: row.try_get("critique")?,
        critique_at: row.try_get("critique_at")?,
        critique_model: row.try_get("critique_model")?,
        superseded_by: row.try_get("superseded_by")?,
        close_reason: row.try_get("close_reason")?,
        ghost: row.try_get::<i64, _>("ghost")? != 0,
        venue: row.try_get("venue")?,
        session_id: row.try_get("session_id")?,
        created_at: row.try_get("created_at")?,
        acknowledged_at: row.try_get("acknowledged_at")?,
        updated_at: row.try_get("updated_at")?,
        closed_at: row.try_get("closed_at")?,
        token_id: row.try_get("token_id").unwrap_or(None),
        entry_price: dec_opt(row, "entry_price"),
        entry_shares: dec_opt(row, "entry_shares"),
        working_at: row.try_get("working_at").unwrap_or(None),
        fee_verdict: row.try_get("fee_verdict").unwrap_or(None),
        critique_requested_at: row.try_get("critique_requested_at").unwrap_or(None),
        critique_outcome: row.try_get("critique_outcome").unwrap_or(None),
    })
}

const SELECT_INTENT: &str = "SELECT * FROM helm_intents";

// ── Writes ───────────────────────────────────────────────────────────────────

/// Open an intent as `proposed`. The content is version 1 and never changes.
pub async fn create(pool: &SqlitePool, new: &NewIntent) -> Result<i64> {
    if let Err(errs) = shape_check(&new.side, &new.content) {
        bail!("intent is malformed: {}", errs.join("; "));
    }
    let now = Utc::now().to_rfc3339();
    let c = &new.content;
    let id = sqlx::query(
        "INSERT INTO helm_intents (
            squadron_id, market_id, market_name, side,
            thesis, confidence, horizon, falsification, entry_kind, entry_limit_price,
            size_usdc, stop_price, take_profit_price, time_limit_at, hold_to_settlement,
            catastrophic_floor_pct, status, ghost, venue, session_id, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'proposed', ?, ?, ?, ?, ?)"
    )
    .bind(&new.squadron_id).bind(&new.market_id).bind(&new.market_name)
    .bind(new.side.trim().to_ascii_uppercase())
    .bind(&c.thesis).bind(c.confidence).bind(c.horizon.as_str()).bind(&c.falsification)
    .bind(c.entry_kind.as_str()).bind(c.entry_limit_price.map(|d| d.to_string()))
    .bind(c.size_usdc.to_string()).bind(c.stop_price.map(|d| d.to_string()))
    .bind(c.take_profit_price.map(|d| d.to_string()))
    .bind(c.time_limit_at.map(|t| t.to_rfc3339()))
    .bind(c.hold_to_settlement as i64)
    .bind(c.catastrophic_floor_pct.map(|d| d.to_string()))
    .bind(new.ghost as i64).bind(&new.venue)
    .bind(crate::helpers::db::current_session_id())
    .bind(&now).bind(&now)
    .execute(pool).await?
    .last_insert_rowid();
    record_event(pool, id, None, IntentStatus::Proposed, None).await?;
    Ok(id)
}

async fn record_event(
    pool: &SqlitePool, intent_id: i64, from: Option<IntentStatus>, to: IntentStatus, detail: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO helm_intent_events (intent_id, at, from_status, to_status, detail) VALUES (?, ?, ?, ?, ?)"
    )
    .bind(intent_id).bind(Utc::now().to_rfc3339())
    .bind(from.map(IntentStatus::as_str)).bind(to.as_str()).bind(detail)
    .execute(pool).await?;
    Ok(())
}

/// Move an intent along the machine. Refuses a move the machine does not
/// allow, and refuses to touch a terminal intent at all. `detail` is kept on
/// the row as `status_detail` and, for `Closed`, as `close_reason`.
pub async fn transition(
    pool: &SqlitePool, id: i64, to: IntentStatus, detail: Option<&str>,
) -> Result<HelmIntent> {
    let cur = get(pool, id).await?.ok_or_else(|| anyhow!("intent {id} not found"))?;
    if !cur.status.can_transition_to(to) {
        bail!(
            "intent {id} cannot move {} → {}{}",
            cur.status.as_str(), to.as_str(),
            if cur.status.is_terminal() { " (terminal)" } else { "" },
        );
    }
    let now = Utc::now().to_rfc3339();
    let mut q = String::from("UPDATE helm_intents SET status = ?, status_detail = ?, updated_at = ?");
    if to == IntentStatus::Acknowledged { q.push_str(", acknowledged_at = ?"); }
    if to.is_terminal() { q.push_str(", closed_at = ?"); }
    if to == IntentStatus::Closed { q.push_str(", close_reason = ?"); }
    q.push_str(" WHERE id = ?");
    let mut stmt = sqlx::query(&q).bind(to.as_str()).bind(detail).bind(&now);
    if to == IntentStatus::Acknowledged { stmt = stmt.bind(&now); }
    if to.is_terminal() { stmt = stmt.bind(&now); }
    if to == IntentStatus::Closed { stmt = stmt.bind(detail.unwrap_or("closed")); }
    stmt.bind(id).execute(pool).await?;
    record_event(pool, id, Some(cur.status), to, detail).await?;
    get(pool, id).await?.ok_or_else(|| anyhow!("intent {id} vanished"))
}

/// Add a revision. The first submission is untouched; the new content becomes
/// `current_version`. Refused on a terminal intent, which can no longer be
/// about anything.
pub async fn revise(pool: &SqlitePool, id: i64, content: &IntentContent, reason: &str) -> Result<i64> {
    let cur = get(pool, id).await?.ok_or_else(|| anyhow!("intent {id} not found"))?;
    if cur.status.is_terminal() {
        bail!("intent {id} is {} and cannot be revised", cur.status.as_str());
    }
    if let Err(errs) = shape_check(&cur.side, content) {
        bail!("revision is malformed: {}", errs.join("; "));
    }
    if reason.trim().is_empty() {
        bail!("a revision needs a reason");
    }
    let version = cur.current_version + 1;
    let now = Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO helm_intent_revisions (
            intent_id, version, reason, thesis, confidence, horizon, falsification, entry_kind,
            entry_limit_price, size_usdc, stop_price, take_profit_price, time_limit_at,
            hold_to_settlement, catastrophic_floor_pct, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(id).bind(version).bind(reason.trim())
    .bind(&content.thesis).bind(content.confidence).bind(content.horizon.as_str()).bind(&content.falsification)
    .bind(content.entry_kind.as_str()).bind(content.entry_limit_price.map(|d| d.to_string()))
    .bind(content.size_usdc.to_string()).bind(content.stop_price.map(|d| d.to_string()))
    .bind(content.take_profit_price.map(|d| d.to_string()))
    .bind(content.time_limit_at.map(|t| t.to_rfc3339()))
    .bind(content.hold_to_settlement as i64)
    .bind(content.catastrophic_floor_pct.map(|d| d.to_string()))
    .bind(&now)
    .execute(&mut *tx).await?;
    sqlx::query("UPDATE helm_intents SET current_version = ?, updated_at = ? WHERE id = ?")
        .bind(version).bind(&now).bind(id)
        .execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(version)
}

/// Replace an intent with a new one. The old one becomes `superseded` and
/// points at its successor; the new one opens as `proposed`, so the critique
/// (when it exists) runs on the new thesis too. This is how a loosening is
/// expressed: not as an edit, but as a new stated reason.
pub async fn supersede(pool: &SqlitePool, old_id: i64, new: &NewIntent, reason: &str) -> Result<i64> {
    let old = get(pool, old_id).await?.ok_or_else(|| anyhow!("intent {old_id} not found"))?;
    if old.status.is_terminal() {
        bail!("intent {old_id} is {} and cannot be superseded", old.status.as_str());
    }
    if reason.trim().is_empty() {
        bail!("superseding needs a reason");
    }
    let new_id = create(pool, new).await?;
    sqlx::query("UPDATE helm_intents SET superseded_by = ? WHERE id = ?")
        .bind(new_id).bind(old_id).execute(pool).await?;
    transition(pool, old_id, IntentStatus::Superseded, Some(reason.trim())).await?;
    Ok(new_id)
}

/// The entry is going to the book: `acknowledged → working`, recording what
/// was asked for so the fill can be judged and the miss can be timed. Refused
/// by the machine from any other status, so an entry can be emitted once.
pub async fn mark_working(
    pool: &SqlitePool, id: i64, token_id: &str, entry_price: Decimal, entry_shares: Decimal,
) -> Result<HelmIntent> {
    let detail = format!("entry to the book: {entry_shares:.4} shares @ ${entry_price:.4}");
    transition(pool, id, IntentStatus::Working, Some(&detail)).await?;
    sqlx::query(
        "UPDATE helm_intents SET token_id = ?, entry_price = ?, entry_shares = ?, working_at = ? WHERE id = ?"
    )
    .bind(token_id).bind(entry_price.to_string()).bind(entry_shares.to_string())
    .bind(Utc::now().to_rfc3339()).bind(id)
    .execute(pool).await?;
    get(pool, id).await?.ok_or_else(|| anyhow!("intent {id} vanished"))
}

/// Close every open intent a squadron still has, with one reason. The
/// retirement path calls this when the market, not the intent, ended things.
/// Returns how many were closed.
pub async fn close_open_for_squadron(pool: &SqlitePool, squadron_id: &str, reason: &str) -> usize {
    let mut n = 0;
    for intent in list_for_squadron(pool, squadron_id, false).await {
        if transition(pool, intent.id, IntentStatus::Closed, Some(reason)).await.is_ok() {
            n += 1;
        }
    }
    n
}

/// Open (non-terminal) intents across every squadron: the figure the
/// open-intent limit is measured against.
pub async fn open_intents_total(pool: &SqlitePool) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM helm_intents WHERE status NOT IN ('closed', 'superseded')"
    )
    .fetch_one(pool).await.unwrap_or(0)
}

/// Label the trade rows an exit booked for an intent. The patrol writes the
/// row under `HelmStrategy` for the market and side; this is the join back,
/// run after `on_exit_filled`, and it touches only rows not yet labeled.
pub async fn label_trades(pool: &SqlitePool, market_name: &str, side: &str, intent_id: i64) -> u64 {
    sqlx::query(
        "UPDATE trades SET intent_id = ? WHERE strategy = ? AND market = ? AND side = ? AND intent_id IS NULL"
    )
    .bind(intent_id).bind(crate::vipers::helm_impl::STRATEGY_NAME).bind(market_name).bind(side)
    .execute(pool).await.map(|r| r.rows_affected()).unwrap_or(0)
}

/// The critique has been asked for; the answer follows, or the timeout does.
pub async fn set_critique_requested(pool: &SqlitePool, id: i64) -> Result<()> {
    sqlx::query("UPDATE helm_intents SET critique_requested_at = ?, updated_at = ? WHERE id = ?")
        .bind(Utc::now().to_rfc3339()).bind(Utc::now().to_rfc3339()).bind(id)
        .execute(pool).await?;
    Ok(())
}

/// The fee verdict shown to the operator, recorded as shown.
pub async fn set_fee_verdict(pool: &SqlitePool, id: i64, verdict: &str) -> Result<()> {
    sqlx::query("UPDATE helm_intents SET fee_verdict = ?, updated_at = ? WHERE id = ?")
        .bind(verdict).bind(Utc::now().to_rfc3339()).bind(id)
        .execute(pool).await?;
    Ok(())
}

/// Score the critique against what happened. Only a terminal intent has an
/// outcome to score against; only the three known verdicts are accepted.
pub async fn set_critique_outcome(pool: &SqlitePool, id: i64, outcome: &str) -> Result<()> {
    if !CRITIQUE_OUTCOMES.contains(&outcome) {
        bail!("critique outcome must be one of {:?}, got {outcome:?}", CRITIQUE_OUTCOMES);
    }
    let cur = get(pool, id).await?.ok_or_else(|| anyhow!("intent {id} not found"))?;
    if !cur.status.is_terminal() {
        bail!("intent {id} is {} and has no outcome to score the critique against yet", cur.status.as_str());
    }
    sqlx::query("UPDATE helm_intents SET critique_outcome = ?, critique_outcome_at = ?, updated_at = ? WHERE id = ?")
        .bind(outcome).bind(Utc::now().to_rfc3339()).bind(Utc::now().to_rfc3339()).bind(id)
        .execute(pool).await?;
    Ok(())
}

/// Intents that resolved with a position: closed, having been filled or
/// partially filled at some point. The calibration denominator.
pub async fn resolved_count(pool: &SqlitePool) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(DISTINCT i.id) FROM helm_intents i
         JOIN helm_intent_events e ON e.intent_id = i.id
         WHERE i.status = 'closed' AND e.to_status IN ('filled', 'partial')"
    )
    .fetch_one(pool).await.unwrap_or(0)
}

/// Record the critique against an intent. Observational: changes no status.
pub async fn set_critique(pool: &SqlitePool, id: i64, critique: &str, model: &str) -> Result<()> {
    sqlx::query(
        "UPDATE helm_intents SET critique = ?, critique_at = ?, critique_model = ?, updated_at = ? WHERE id = ?"
    )
    .bind(critique).bind(Utc::now().to_rfc3339()).bind(model).bind(Utc::now().to_rfc3339()).bind(id)
    .execute(pool).await?;
    Ok(())
}

// ── Join plumbing for positions and trades ───────────────────────────────────

/// Label an open position with the intent that produced it. Keyed the way the
/// position table is keyed — token and strategy — so a chain-readopted row can
/// be relabeled by the same call.
pub async fn set_position_intent(pool: &SqlitePool, token_id: &str, strategy: &str, intent_id: i64) -> Result<u64> {
    let r = sqlx::query("UPDATE open_positions SET intent_id = ? WHERE token_id = ? AND strategy = ?")
        .bind(intent_id).bind(token_id).bind(strategy).execute(pool).await?;
    Ok(r.rows_affected())
}

pub async fn position_intent(pool: &SqlitePool, token_id: &str, strategy: &str) -> Option<i64> {
    sqlx::query_scalar::<_, Option<i64>>(
        "SELECT intent_id FROM open_positions WHERE token_id = ? AND strategy = ? ORDER BY id DESC LIMIT 1"
    )
    .bind(token_id).bind(strategy).fetch_optional(pool).await.ok().flatten().flatten()
}

/// Label a booked trade with its intent.
pub async fn set_trade_intent(pool: &SqlitePool, trade_id: i64, intent_id: i64) -> Result<u64> {
    let r = sqlx::query("UPDATE trades SET intent_id = ? WHERE id = ?")
        .bind(intent_id).bind(trade_id).execute(pool).await?;
    Ok(r.rows_affected())
}

// ── Reads ────────────────────────────────────────────────────────────────────

pub async fn get(pool: &SqlitePool, id: i64) -> Result<Option<HelmIntent>> {
    let row = sqlx::query(&format!("{SELECT_INTENT} WHERE id = ?"))
        .bind(id).fetch_optional(pool).await?;
    row.as_ref().map(intent_from_row).transpose()
}

/// A squadron's intents, newest first. `include_terminal` false returns only
/// the ones still open.
pub async fn list_for_squadron(pool: &SqlitePool, squadron_id: &str, include_terminal: bool) -> Vec<HelmIntent> {
    let q = if include_terminal {
        format!("{SELECT_INTENT} WHERE squadron_id = ? ORDER BY id DESC")
    } else {
        format!("{SELECT_INTENT} WHERE squadron_id = ? AND status NOT IN ('closed', 'superseded') ORDER BY id DESC")
    };
    sqlx::query(&q).bind(squadron_id).fetch_all(pool).await
        .map(|rows| rows.iter().filter_map(|r| intent_from_row(r).ok()).collect())
        .unwrap_or_default()
}

/// Every intent, newest first, capped.
pub async fn list_all(pool: &SqlitePool, limit: i64) -> Vec<HelmIntent> {
    sqlx::query(&format!("{SELECT_INTENT} ORDER BY id DESC LIMIT ?"))
        .bind(limit).fetch_all(pool).await
        .map(|rows| rows.iter().filter_map(|r| intent_from_row(r).ok()).collect())
        .unwrap_or_default()
}

pub async fn revisions(pool: &SqlitePool, intent_id: i64) -> Vec<IntentRevision> {
    sqlx::query("SELECT * FROM helm_intent_revisions WHERE intent_id = ? ORDER BY version ASC")
        .bind(intent_id).fetch_all(pool).await
        .map(|rows| rows.iter().filter_map(|r| Some(IntentRevision {
            id: r.try_get("id").ok()?,
            intent_id: r.try_get("intent_id").ok()?,
            version: r.try_get("version").ok()?,
            reason: r.try_get("reason").ok()?,
            content: content_from_row(r).ok()?,
            created_at: r.try_get("created_at").ok()?,
        })).collect())
        .unwrap_or_default()
}

pub async fn events(pool: &SqlitePool, intent_id: i64) -> Vec<IntentEvent> {
    sqlx::query("SELECT * FROM helm_intent_events WHERE intent_id = ? ORDER BY id ASC")
        .bind(intent_id).fetch_all(pool).await
        .map(|rows| rows.iter().filter_map(|r| {
            let to: String = r.try_get("to_status").ok()?;
            let from: Option<String> = r.try_get("from_status").ok()?;
            Some(IntentEvent {
                id: r.try_get("id").ok()?,
                intent_id: r.try_get("intent_id").ok()?,
                at: r.try_get("at").ok()?,
                from_status: from.and_then(|s| IntentStatus::parse(&s)),
                to_status: IntentStatus::parse(&to)?,
                detail: r.try_get("detail").ok()?,
            })
        }).collect())
        .unwrap_or_default()
}

/// The content the engine reads: version 1 unless a revision superseded it.
pub async fn current_content(pool: &SqlitePool, intent: &HelmIntent) -> IntentContent {
    if intent.current_version <= 1 {
        return intent.first.clone();
    }
    revisions(pool, intent.id).await.into_iter()
        .find(|r| r.version == intent.current_version)
        .map(|r| r.content)
        .unwrap_or_else(|| intent.first.clone())
}

/// How many intents the squadron has and how many are still open. The
/// retirement check reads this; see `IntentTally::complete`.
pub async fn squadron_tally(pool: &SqlitePool, squadron_id: &str) -> IntentTally {
    sqlx::query(
        "SELECT COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN status NOT IN ('closed', 'superseded') THEN 1 ELSE 0 END), 0) AS open
         FROM helm_intents WHERE squadron_id = ?"
    )
    .bind(squadron_id).fetch_one(pool).await
    .map(|r| IntentTally {
        total: r.try_get::<i64, _>("total").unwrap_or(0),
        open: r.try_get::<i64, _>("open").unwrap_or(0),
    })
    .unwrap_or_default()
}

/// The condition id a deployed squadron was given, from the deployment row
/// that produced it. The CAG registry does not keep it.
pub async fn market_id_for_squadron(pool: &SqlitePool, squadron_id: &str) -> Option<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT market_id FROM deployment_queue WHERE squadron_id = ? ORDER BY created_at DESC LIMIT 1"
    )
    .bind(squadron_id).fetch_optional(pool).await.ok().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn pool() -> SqlitePool {
        let p = SqlitePoolOptions::new().max_connections(1).connect("sqlite::memory:").await.unwrap();
        crate::helpers::db::init_schema(&p).await.expect("schema");
        p
    }

    fn content() -> IntentContent {
        IntentContent {
            thesis: "BTC is pinned under the strike into the close".into(),
            confidence: 0.7,
            horizon: Horizon::Expiry,
            falsification: "a print above $0.55 on the YES token".into(),
            entry_kind: EntryKind::Taker,
            entry_limit_price: None,
            size_usdc: dec!(4),
            stop_price: Some(dec!(0.55)),
            take_profit_price: None,
            time_limit_at: None,
            hold_to_settlement: false,
            catastrophic_floor_pct: None,
        }
    }

    fn new_intent(squadron: &str) -> NewIntent {
        NewIntent {
            squadron_id: squadron.into(),
            market_id: "0xcond".into(),
            market_name: "Bitcoin Up or Down - October 2, 3PM ET".into(),
            side: "no".into(),
            content: content(),
            ghost: true,
            venue: "intl_clob".into(),
        }
    }

    // ── The machine ─────────────────────────────────────────────────────

    /// Every allowed edge, and nothing else. Written out rather than derived
    /// so a change to the machine has to be made in two places on purpose.
    #[test]
    fn the_status_machine_is_exactly_as_specified() {
        use IntentStatus::*;
        let allowed: &[(IntentStatus, IntentStatus)] = &[
            (Proposed, Acknowledged), (Proposed, Closed), (Proposed, Superseded),
            (Acknowledged, Working), (Acknowledged, Closed), (Acknowledged, Superseded),
            (Working, Filled), (Working, Partial), (Working, Missed), (Working, Closed), (Working, Superseded),
            (Filled, Closed), (Filled, Superseded),
            (Partial, Filled), (Partial, Closed), (Partial, Superseded),
            (Missed, Closed), (Missed, Superseded),
        ];
        for from in IntentStatus::ALL {
            for to in IntentStatus::ALL {
                let expected = allowed.contains(&(from, to));
                assert_eq!(from.can_transition_to(to), expected, "{} → {}", from.as_str(), to.as_str());
            }
        }
    }

    #[test]
    fn terminal_statuses_have_no_exit() {
        for from in [IntentStatus::Closed, IntentStatus::Superseded] {
            assert!(from.is_terminal());
            for to in IntentStatus::ALL {
                assert!(!from.can_transition_to(to), "{} must not leave", from.as_str());
            }
        }
    }

    #[test]
    fn status_round_trips_through_its_string() {
        for s in IntentStatus::ALL {
            assert_eq!(IntentStatus::parse(s.as_str()), Some(s));
        }
        assert_eq!(IntentStatus::parse(" Filled "), Some(IntentStatus::Filled));
        assert_eq!(IntentStatus::parse("done"), None);
    }

    // ── Shape ───────────────────────────────────────────────────────────

    #[test]
    fn a_well_formed_intent_passes_the_shape_check() {
        assert_eq!(shape_check("YES", &content()), Ok(()));
    }

    #[test]
    fn the_shape_check_names_every_defect_at_once() {
        let mut c = content();
        c.confidence = 1.0;
        c.thesis = "  ".into();
        c.size_usdc = dec!(0);
        c.entry_kind = EntryKind::Resting; // and no limit price
        c.stop_price = Some(dec!(1.2));
        let errs = shape_check("maybe", &c).unwrap_err();
        let joined = errs.join("\n");
        for needle in ["side must be", "thesis is empty", "confidence must be", "size_usdc", "resting entry needs", "stop_price must be"] {
            assert!(joined.contains(needle), "missing {needle:?} in {joined}");
        }
        assert_eq!(errs.len(), 6);
    }

    #[test]
    fn a_taker_entry_rejects_a_limit_price() {
        let mut c = content();
        c.entry_limit_price = Some(dec!(0.5));
        assert!(shape_check("YES", &c).is_err());
    }

    // ── Storage ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn create_opens_as_proposed_and_records_the_event() {
        let p = pool().await;
        let id = create(&p, &new_intent("helm-open-trial")).await.unwrap();
        let it = get(&p, id).await.unwrap().unwrap();
        assert_eq!(it.status, IntentStatus::Proposed);
        assert_eq!(it.side, "NO", "side is normalized");
        assert_eq!(it.current_version, 1);
        assert_eq!(it.first, content());
        assert!(it.ghost);
        let ev = events(&p, id).await;
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].from_status, None);
        assert_eq!(ev[0].to_status, IntentStatus::Proposed);
    }

    #[tokio::test]
    async fn a_malformed_intent_is_refused_at_the_write() {
        let p = pool().await;
        let mut n = new_intent("s");
        n.content.confidence = 0.0;
        assert!(create(&p, &n).await.is_err());
        assert!(list_all(&p, 10).await.is_empty());
    }

    #[tokio::test]
    async fn transitions_follow_the_machine_and_stamp_the_row() {
        let p = pool().await;
        let id = create(&p, &new_intent("s")).await.unwrap();
        // Skipping acknowledgement is refused.
        assert!(transition(&p, id, IntentStatus::Working, None).await.is_err());
        let it = transition(&p, id, IntentStatus::Acknowledged, Some("read the critique")).await.unwrap();
        assert!(it.acknowledged_at.is_some());
        assert_eq!(it.status_detail.as_deref(), Some("read the critique"));
        let it = transition(&p, id, IntentStatus::Closed, Some("cancelled by operator")).await.unwrap();
        assert_eq!(it.status, IntentStatus::Closed);
        assert_eq!(it.close_reason.as_deref(), Some("cancelled by operator"));
        assert!(it.closed_at.is_some());
        // Terminal: nothing moves it, including a second close.
        assert!(transition(&p, id, IntentStatus::Closed, None).await.is_err());
        assert!(transition(&p, id, IntentStatus::Acknowledged, None).await.is_err());
        assert_eq!(events(&p, id).await.len(), 3);
    }

    /// The rule the module exists for: the first submission is what the
    /// retrospective sees, however many times the operator edits afterwards.
    #[tokio::test]
    async fn revising_never_touches_the_first_submission() {
        let p = pool().await;
        let id = create(&p, &new_intent("s")).await.unwrap();
        let mut edited = content();
        edited.thesis = "actually, pinned under the strike AND funding is negative".into();
        edited.confidence = 0.6;
        edited.stop_price = Some(dec!(0.52));
        let v = revise(&p, id, &edited, "tightened after reading the critique").await.unwrap();
        assert_eq!(v, 2);

        let it = get(&p, id).await.unwrap().unwrap();
        assert_eq!(it.first, content(), "version 1 is byte-identical");
        assert_eq!(it.current_version, 2);
        assert_eq!(current_content(&p, &it).await, edited, "the engine reads version 2");

        let revs = revisions(&p, id).await;
        assert_eq!(revs.len(), 1);
        assert_eq!(revs[0].version, 2);
        assert_eq!(revs[0].reason, "tightened after reading the critique");

        // A revision without a reason, or on a closed intent, is refused.
        assert!(revise(&p, id, &edited, "   ").await.is_err());
        transition(&p, id, IntentStatus::Closed, Some("cancelled")).await.unwrap();
        assert!(revise(&p, id, &edited, "too late").await.is_err());
    }

    #[tokio::test]
    async fn superseding_closes_the_old_and_opens_the_new_as_proposed() {
        let p = pool().await;
        let old = create(&p, &new_intent("s")).await.unwrap();
        transition(&p, old, IntentStatus::Acknowledged, None).await.unwrap();
        let mut n = new_intent("s");
        n.content.stop_price = None;
        n.content.hold_to_settlement = true;
        let new = supersede(&p, old, &n, "loosening: the thesis is now settlement-only").await.unwrap();
        let o = get(&p, old).await.unwrap().unwrap();
        let nw = get(&p, new).await.unwrap().unwrap();
        assert_eq!(o.status, IntentStatus::Superseded);
        assert_eq!(o.superseded_by, Some(new));
        assert_eq!(nw.status, IntentStatus::Proposed, "the critique runs again on the new thesis");
        assert!(nw.first.hold_to_settlement);
        // A superseded intent cannot be superseded again.
        assert!(supersede(&p, old, &n, "again").await.is_err());
    }

    /// The retirement signal. A squadron with no intents is not complete;
    /// one with any open intent is not complete; one whose every intent is
    /// terminal is.
    #[tokio::test]
    async fn the_tally_says_when_a_squadron_is_complete() {
        let p = pool().await;
        assert_eq!(squadron_tally(&p, "s").await, IntentTally { total: 0, open: 0 });
        assert!(!squadron_tally(&p, "s").await.complete(), "no intents is not complete");

        let a = create(&p, &new_intent("s")).await.unwrap();
        let b = create(&p, &new_intent("s")).await.unwrap();
        create(&p, &new_intent("other-squadron")).await.unwrap();
        assert_eq!(squadron_tally(&p, "s").await, IntentTally { total: 2, open: 2 });

        transition(&p, a, IntentStatus::Closed, Some("cancelled")).await.unwrap();
        assert_eq!(squadron_tally(&p, "s").await, IntentTally { total: 2, open: 1 });
        assert!(!squadron_tally(&p, "s").await.complete());

        transition(&p, b, IntentStatus::Superseded, Some("x")).await.unwrap();
        let t = squadron_tally(&p, "s").await;
        assert_eq!(t, IntentTally { total: 2, open: 0 });
        assert!(t.complete());
        assert_eq!(list_for_squadron(&p, "s", false).await.len(), 0);
        assert_eq!(list_for_squadron(&p, "s", true).await.len(), 2);
    }

    /// The join columns exist on both ledgers and round-trip an id. Nothing
    /// writes them yet; the entry increment will.
    #[tokio::test]
    async fn positions_and_trades_carry_an_intent_id() {
        let p = pool().await;
        let id = create(&p, &new_intent("s")).await.unwrap();
        sqlx::query(
            "INSERT INTO open_positions (ts, session_id, strategy, token_id, market, side, entry_price, shares)
             VALUES ('t', 's', 'HelmStrategy', 'tok', 'm', 'NO', '0.45', '8')"
        ).execute(&p).await.unwrap();
        assert_eq!(position_intent(&p, "tok", "HelmStrategy").await, None);
        assert_eq!(set_position_intent(&p, "tok", "HelmStrategy", id).await.unwrap(), 1);
        assert_eq!(position_intent(&p, "tok", "HelmStrategy").await, Some(id));

        let trade_id = sqlx::query(
            "INSERT INTO trades (ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason)
             VALUES ('t', 'HelmStrategy', 'm', 'NO', '0.45', '0.50', '8', '0.4', 'test')"
        ).execute(&p).await.unwrap().last_insert_rowid();
        assert_eq!(set_trade_intent(&p, trade_id, id).await.unwrap(), 1);
        let got: Option<i64> = sqlx::query_scalar("SELECT intent_id FROM trades WHERE id = ?")
            .bind(trade_id).fetch_one(&p).await.unwrap();
        assert_eq!(got, Some(id));
    }

    /// The one shape rule the engine cannot do without: a posture that names
    /// no exit at all is refused, and naming any one of the four is enough.
    #[test]
    fn a_posture_must_name_an_exit() {
        let mut c = content();
        c.stop_price = None;
        let errs = shape_check("YES", &c).unwrap_err();
        assert!(errs.iter().any(|e| e.contains("names no exit")), "{errs:?}");
        for fix in [
            |c: &mut IntentContent| c.stop_price = Some(dec!(0.5)),
            |c: &mut IntentContent| c.take_profit_price = Some(dec!(0.7)),
            |c: &mut IntentContent| c.time_limit_at = Some(Utc::now()),
            |c: &mut IntentContent| c.hold_to_settlement = true,
        ] {
            let mut d = c.clone();
            fix(&mut d);
            assert_eq!(shape_check("YES", &d), Ok(()));
        }
    }

    /// `mark_working` records what was asked for and moves the status; a
    /// second call is refused by the machine, so an entry is emitted once.
    #[tokio::test]
    async fn mark_working_records_the_order_and_happens_once() {
        let p = pool().await;
        let id = create(&p, &new_intent("s")).await.unwrap();
        assert!(mark_working(&p, id, "tok", dec!(0.42), dec!(9.5)).await.is_err(), "proposed cannot go to work");
        transition(&p, id, IntentStatus::Acknowledged, None).await.unwrap();
        let it = mark_working(&p, id, "tok", dec!(0.42), dec!(9.5)).await.unwrap();
        assert_eq!(it.status, IntentStatus::Working);
        assert_eq!(it.token_id.as_deref(), Some("tok"));
        assert_eq!(it.entry_price, Some(dec!(0.42)));
        assert_eq!(it.entry_shares, Some(dec!(9.5)));
        assert!(it.working_for_secs(Utc::now()).is_some_and(|s| (0..5).contains(&s)));
        assert!(it.is_in_flight());
        assert!(mark_working(&p, id, "tok", dec!(0.42), dec!(9.5)).await.is_err(), "already working");
    }

    /// Retirement on the market's account closes what is open and leaves what
    /// is already terminal alone.
    #[tokio::test]
    async fn closing_a_squadron_s_open_intents_leaves_terminal_ones_alone() {
        let p = pool().await;
        let a = create(&p, &new_intent("s")).await.unwrap();
        let b = create(&p, &new_intent("s")).await.unwrap();
        create(&p, &new_intent("other")).await.unwrap();
        transition(&p, a, IntentStatus::Closed, Some("cancelled")).await.unwrap();
        assert_eq!(close_open_for_squadron(&p, "s", "market closed").await, 1);
        let b = get(&p, b).await.unwrap().unwrap();
        assert_eq!(b.status, IntentStatus::Closed);
        assert_eq!(b.close_reason.as_deref(), Some("market closed"));
        assert_eq!(get(&p, a).await.unwrap().unwrap().close_reason.as_deref(), Some("cancelled"));
        assert_eq!(open_intents_total(&p).await, 1, "the other squadron's intent is untouched");
    }

    /// The exit's trade row is labeled by market and side, only where unlabeled.
    #[tokio::test]
    async fn label_trades_touches_only_unlabeled_helm_rows_for_the_market() {
        let p = pool().await;
        let id = create(&p, &new_intent("s")).await.unwrap();
        for (strategy, market, side, pre) in [
            ("HelmStrategy", "m", "NO", None::<i64>),
            ("HelmStrategy", "m", "NO", Some(99)),
            ("HelmStrategy", "other", "NO", None),
            ("MomentumStrategy", "m", "NO", None),
        ] {
            sqlx::query(
                "INSERT INTO trades (ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason, intent_id)
                 VALUES ('t', ?, ?, ?, '0.4', '0.5', '1', '0.1', 'r', ?)"
            ).bind(strategy).bind(market).bind(side).bind(pre).execute(&p).await.unwrap();
        }
        assert_eq!(label_trades(&p, "m", "NO", id).await, 1);
        let labeled: Vec<Option<i64>> = sqlx::query_scalar("SELECT intent_id FROM trades ORDER BY id")
            .fetch_all(&p).await.unwrap();
        assert_eq!(labeled, vec![Some(id), Some(99), None, None]);
    }

    // ── The book validator ──────────────────────────────────────────────

    fn book() -> BookFacts {
        BookFacts {
            bid: dec!(0.40), ask: dec!(0.42),
            close_time: Some(Utc::now() + chrono::Duration::minutes(50)),
            now: Utc::now(), min_shares: dec!(5), min_secs_to_close: 120,
        }
    }

    #[test]
    fn an_honest_posture_passes_the_book_validator() {
        // The fixture's stop ($0.55) is the falsification price for a YES
        // thesis; against this NO book (bid 0.40 / ask 0.42) an honest stop
        // sits below the bid.
        let mut c = content();
        c.stop_price = Some(dec!(0.30));
        assert_eq!(validate_against_book("NO", &c, &book()), Ok(()));
        let mut c = content();
        c.stop_price = None;
        c.take_profit_price = Some(dec!(0.60));
        c.time_limit_at = Some(Utc::now() + chrono::Duration::minutes(20));
        assert_eq!(validate_against_book("NO", &c, &book()), Ok(()));
    }

    /// Each rule, named, and all of them reported together.
    #[test]
    fn the_book_validator_names_every_inconsistency() {
        let mut c = content();
        c.stop_price = Some(dec!(0.41));            // above the bid: fires at once
        c.take_profit_price = Some(dec!(0.42));     // at the entry price
        c.time_limit_at = Some(Utc::now() + chrono::Duration::hours(2)); // after the close
        let errs = validate_against_book("NO", &c, &book()).unwrap_err();
        let j = errs.join("\n");
        for needle in ["current bid", "at or below the entry price", "after the market's close"] {
            assert!(j.contains(needle), "missing {needle:?} in {j}");
        }
        assert_eq!(errs.len(), 3);
    }

    #[test]
    fn a_stop_at_or_above_entry_fires_at_once_and_is_refused() {
        let mut c = content();
        c.stop_price = Some(dec!(0.45));
        let errs = validate_against_book("NO", &c, &book()).unwrap_err();
        assert!(errs[0].contains("at or above the entry price"), "{errs:?}");
    }

    #[test]
    fn hold_to_settlement_and_a_stop_cannot_both_be_set() {
        let mut c = content();
        c.hold_to_settlement = true; // content() has a stop
        let errs = validate_against_book("NO", &c, &book()).unwrap_err();
        assert!(errs.iter().any(|e| e.contains("cannot both be set")), "{errs:?}");
        c.stop_price = None;
        c.catastrophic_floor_pct = Some(dec!(0.5));
        assert_eq!(validate_against_book("NO", &c, &book()), Ok(()));
        let mut b = book();
        b.close_time = None;
        assert!(validate_against_book("NO", &c, &b).unwrap_err().iter().any(|e| e.contains("close time")));
    }

    #[test]
    fn the_book_itself_is_checked() {
        let mut b = book();
        b.bid = dec!(0);
        assert!(validate_against_book("NO", &content(), &b).unwrap_err()[0].contains("no bid"));
        let mut b = book();
        b.ask = dec!(0);
        assert!(validate_against_book("NO", &content(), &b).unwrap_err().iter().any(|e| e.contains("no ask")));
        let mut b = book();
        b.close_time = Some(Utc::now() + chrono::Duration::seconds(60));
        assert!(validate_against_book("NO", &content(), &b).unwrap_err().iter().any(|e| e.contains("too close")));
        let mut b = book();
        b.min_shares = dec!(20);
        assert!(validate_against_book("NO", &content(), &b).unwrap_err().iter().any(|e| e.contains("venue minimum")));
    }

    #[test]
    fn a_resting_bid_must_sit_below_the_ask_and_its_stop_below_the_limit() {
        let mut c = content();
        c.entry_kind = EntryKind::Resting;
        c.entry_limit_price = Some(dec!(0.42));
        assert!(validate_against_book("NO", &c, &book()).unwrap_err().iter().any(|e| e.contains("cross the book")));
        c.entry_limit_price = Some(dec!(0.35));
        c.stop_price = Some(dec!(0.36));
        assert!(validate_against_book("NO", &c, &book()).unwrap_err().iter().any(|e| e.contains("at or above the entry price")));
        c.stop_price = Some(dec!(0.30));
        assert_eq!(validate_against_book("NO", &c, &book()), Ok(()));
    }

    /// Where a target exists, the fee's share of it is the question.
    #[test]
    fn the_fee_verdict_measures_against_the_take_profit() {
        let mut c = content();
        c.take_profit_price = Some(dec!(0.43)); // ~2.4% target: fee-dominated
        let v = fee_verdict(&c, &book(), dec!(0.40), dec!(0.05));
        assert!(v.text.contains("fee-dominated"), "{}", v.text);
        assert!(v.refusal.is_some(), "a fee-dominated target must refuse, not just report");

        c.take_profit_price = Some(dec!(0.80));
        let v = fee_verdict(&c, &book(), dec!(0.40), dec!(0.05));
        assert!(v.text.contains("within limits"), "{}", v.text);
        assert!(v.refusal.is_none());
    }

    /// A posture with no price target is judged against notional, because there
    /// is no target to take a ratio of.
    ///
    /// This is the case a ratio gate cannot see, and the one that let intent #3
    /// of 2026-10-02 through: a time limit, no take-profit, entered at $0.1500
    /// and exited flat at $0.1500 with the whole $0.238 loss being the fee. The
    /// old verdict said "no take-profit to measure against" and passed.
    #[test]
    fn a_posture_with_no_price_target_is_judged_against_notional() {
        let mut c = content();
        c.take_profit_price = None;
        c.hold_to_settlement = false;
        c.stop_price = Some(dec!(0.30));

        // The book's ask is $0.42, so the round trip is 2 × rate × (1 − 0.42).
        // Whatever the configured rate, the gate must turn on the ceiling and
        // not on the ratio, which has no denominator here.
        let generous = fee_verdict(&c, &book(), dec!(0.40), dec!(0.90));
        assert!(generous.refusal.is_none(), "{}", generous.text);
        assert!(generous.text.contains("no price target named"), "{}", generous.text);
        assert!(generous.text.contains("beat the market by at least that much"), "{}", generous.text);

        // Refused on the ceiling, and the refusal names the two postures that
        // would fix it rather than leaving the operator to guess.
        let strict = fee_verdict(&c, &book(), dec!(0.40), dec!(0.001));
        assert!(strict.refusal.is_some(), "{}", strict.text);
        assert!(strict.text.contains("names no price target"), "{}", strict.text);
        assert!(strict.text.contains("Set a take-profit, or hold to settlement"), "{}", strict.text);

        // The ratio ceiling is irrelevant to this case: moving it changes
        // nothing, which is the whole reason the notional ceiling exists.
        assert_eq!(
            fee_verdict(&c, &book(), dec!(0.01), dec!(0.90)).refusal.is_none(),
            fee_verdict(&c, &book(), dec!(0.99), dec!(0.90)).refusal.is_none(),
        );
    }

    /// Holding to settlement is measured to $1.00 and charged one leg.
    ///
    /// Two reasons it differs: the target is the whole distance to $1.00, the
    /// best any binary can pay, and resolution charges no taker fee, so only the
    /// entry leg costs anything.
    ///
    /// The earlier version of this test asserted `refusal.is_none()` on a branch
    /// that can never refuse — the share is `rate × p`, at most 0.07 against a
    /// 0.40 ceiling — and then compared two `venues` functions to each other
    /// without calling `fee_verdict` at all. It would have passed if this case
    /// had charged a round trip. So the assertion is now on the figure in the
    /// verdict's own text.
    #[test]
    fn holding_to_settlement_is_measured_to_a_dollar_and_pays_one_leg() {
        let mut c = content();
        c.take_profit_price = None;
        c.stop_price = None;
        c.hold_to_settlement = true;

        let v = fee_verdict(&c, &book(), dec!(0.40), dec!(0.05));
        assert!(v.text.contains("settlement target"), "{}", v.text);
        assert!(v.refusal.is_none(), "riding to resolution must not be refused: {}", v.text);

        // One leg, from the verdict itself: the text carries the fee it used, so
        // a change to the round trip would fail here.
        let one_leg = crate::venues::entry_only_fee_pct(dec!(0.42)) * Decimal::ONE_HUNDRED;
        assert!(v.text.contains(&format!("fee {:.2}%", one_leg)),
                "expected the one-leg figure {:.2}% in: {}", one_leg, v.text);
        let round_trip = crate::venues::round_trip_fee_pct(dec!(0.42)) * Decimal::ONE_HUNDRED;
        assert!(!v.text.contains(&format!("fee {:.2}%", round_trip)),
                "the round trip must not be charged to a position that settles: {}", v.text);
    }

    /// "Hold to settlement, but exit at 3pm" is not a settlement posture.
    ///
    /// `posture_action` evaluates the time limit and the catastrophic floor
    /// BEFORE it consults `hold_to_settlement`, which only suppresses the stop.
    /// So the position really does sell at 3pm, as a taker — and handing it the
    /// settlement verdict would give the most generous reading in the system to
    /// a trade that pays the full round trip. This was the hole that let a
    /// targetless posture past the gate by adding one flag.
    #[test]
    fn holding_to_settlement_while_something_sells_early_is_judged_on_notional() {
        let mut c = content();
        c.take_profit_price = None;
        c.stop_price = None;
        c.hold_to_settlement = true;
        c.time_limit_at = Some(book().now + chrono::Duration::minutes(10));

        let v = fee_verdict(&c, &book(), dec!(0.40), dec!(0.05));
        assert!(!v.text.contains("settlement target"),
                "a clock that sells makes this a priced exit, not a settlement: {}", v.text);
        assert!(v.text.contains("names no price target") || v.text.contains("no price target named"),
                "{}", v.text);
        assert!(v.refusal.is_some(), "two taker legs with no target must be refused: {}", v.text);

        // The catastrophic floor is the other early seller.
        let mut c2 = c.clone();
        c2.time_limit_at = None;
        c2.catastrophic_floor_pct = Some(dec!(0.50));
        let v2 = fee_verdict(&c2, &book(), dec!(0.40), dec!(0.05));
        assert!(!v2.text.contains("settlement target"), "{}", v2.text);
    }

    /// A post-only leg pays nothing, and the gate has to count that.
    ///
    /// Charging a flat round trip everywhere refused entries that cost one leg
    /// or none: Helm rests its take-profit post-only, and a resting entry is a
    /// post-only bid. A resting entry with a resting take-profit is free on both
    /// ends, and the gate was pricing it as two taker fills.
    #[test]
    fn post_only_legs_are_not_charged() {
        let b = book();
        let p = dec!(0.35); // the resting limit, below the $0.42 ask

        // Resting entry, resting take-profit: nothing is charged either side.
        let mut resting = content();
        resting.entry_kind = EntryKind::Resting;
        resting.entry_limit_price = Some(p);
        resting.stop_price = None;
        resting.take_profit_price = Some(dec!(0.50));
        let v = fee_verdict(&resting, &b, dec!(0.40), dec!(0.05));
        assert!(v.text.contains("fee 0.00%"), "both legs rest, so nothing is charged: {}", v.text);
        assert!(v.refusal.is_none());

        // A taker entry with the same resting take-profit pays the entry leg
        // only — strictly more than nothing, strictly less than a round trip.
        let mut taker = resting.clone();
        taker.entry_kind = EntryKind::Taker;
        taker.entry_limit_price = None;
        let vt = fee_verdict(&taker, &b, dec!(0.40), dec!(0.05));
        assert!(!vt.text.contains("fee 0.00%"), "a taker entry pays to get in: {}", vt.text);
        let one_leg = crate::venues::entry_only_fee_pct(dec!(0.42)) * Decimal::ONE_HUNDRED;
        assert!(vt.text.contains(&format!("fee {:.2}%", one_leg)), "{}", vt.text);
    }

    #[test]
    fn calibration_is_hidden_below_the_threshold() {
        assert!(!calibration_visible(0, 30));
        assert!(!calibration_visible(29, 30));
        assert!(calibration_visible(30, 30));
    }

    #[tokio::test]
    async fn critique_bookkeeping_and_outcome_scoring() {
        let p = pool().await;
        let id = create(&p, &new_intent("s")).await.unwrap();
        let it = get(&p, id).await.unwrap().unwrap();
        assert_eq!(it.critique_pending_for(Utc::now()), None, "not requested yet");
        set_critique_requested(&p, id).await.unwrap();
        let it = get(&p, id).await.unwrap().unwrap();
        assert!(it.critique_pending_for(Utc::now()).is_some_and(|s| s < 5));
        set_critique(&p, id, "unavailable: timed out after 10s", "unavailable").await.unwrap();
        let it = get(&p, id).await.unwrap().unwrap();
        assert_eq!(it.critique_pending_for(Utc::now()), None, "answered, even as unavailable");
        set_fee_verdict(&p, id, "fee share within limits").await.unwrap();
        assert_eq!(get(&p, id).await.unwrap().unwrap().fee_verdict.as_deref(), Some("fee share within limits"));
        // Scoring needs a terminal intent and a known verdict.
        assert!(set_critique_outcome(&p, id, "named_it").await.is_err());
        transition(&p, id, IntentStatus::Closed, Some("cancelled")).await.unwrap();
        assert!(set_critique_outcome(&p, id, "brilliant").await.is_err());
        set_critique_outcome(&p, id, "named_it").await.unwrap();
        assert_eq!(get(&p, id).await.unwrap().unwrap().critique_outcome.as_deref(), Some("named_it"));
    }

    /// Resolved means closed after a fill; a cancelled proposal is not resolved.
    #[tokio::test]
    async fn resolved_count_counts_closed_intents_that_held_a_position() {
        let p = pool().await;
        let a = create(&p, &new_intent("s")).await.unwrap();
        transition(&p, a, IntentStatus::Closed, Some("cancelled")).await.unwrap();
        let b = create(&p, &new_intent("s")).await.unwrap();
        transition(&p, b, IntentStatus::Acknowledged, None).await.unwrap();
        mark_working(&p, b, "tok", dec!(0.42), dec!(9.5)).await.unwrap();
        transition(&p, b, IntentStatus::Filled, None).await.unwrap();
        assert_eq!(resolved_count(&p).await, 0, "still held");
        transition(&p, b, IntentStatus::Closed, Some("Helm stop")).await.unwrap();
        assert_eq!(resolved_count(&p).await, 1);
    }

    /// Schema init runs on every start; a second run must be a no-op.
    #[tokio::test]
    async fn init_schema_is_idempotent() {
        let p = pool().await;
        init_schema(&p).await.unwrap();
        init_schema(&p).await.unwrap();
        let id = create(&p, &new_intent("s")).await.unwrap();
        assert!(get(&p, id).await.unwrap().is_some());
    }
}
