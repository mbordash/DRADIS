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

//! Helm: the operator's own position, with the engine holding the exit.
//!
//! A Helm position is a single-member squadron. The operator deploys a squadron
//! onto one market with the class `helm`, and that class carries exactly one
//! viper — this one. Every other viper is absent rather than switched off, so a
//! Helm squadron cannot quote, arb or model the operator's market behind their
//! back. The squadron supplies what a position needs and nothing else does: a
//! book tick, a `squadron_id` for its `PositionKey`, persistence, chain
//! reconciliation, the lifecycle FSM and a Control Tower page.
//!
//! # Where the order comes from
//!
//! Nowhere in this file. `evaluate_entry` returns a `StrategySignal::Entry` and
//! the patrol loop places it, exactly as it places every other viper's: token
//! ownership claim, phantom cooldown, the pending map row, the venue order, the
//! fill sync, `record_entry_db`, fee booking, Telegram. The API handler that
//! stores the intent never touches an order. That is deliberate: an API that
//! placed a FAK would bypass every one of those, and the first thing it would
//! bypass is the thing that notices when the order did not fill.
//!
//! # What the strategy does
//!
//! - **Entry.** One `Acknowledged` intent per squadron at a time goes to work:
//!   the risk gates run (`entry_plan`), the intent is marked `Working` in the
//!   database *before* the signal leaves (so a crash between the two leaves a
//!   working intent and no order, which the miss window then closes — never an
//!   order and no record), and the `Entry` is emitted. A taker buys the ask with
//!   a FAK; a resting entry posts a post-only GTC bid at the operator's limit.
//! - **Reconciliation.** Each tick, every in-flight intent is checked against
//!   the position map: a confirmed fill moves it to `Filled` or `Partial`; a
//!   working intent with no row after the entry window is `Missed` then
//!   `Closed`; a filled intent whose position has left the map is `Closed` with
//!   the exit reason the patrol reported through `on_exit_filled`, or with the
//!   settlement reason if nothing did.
//! - **Exit.** `posture_action` applies the intent's stored fields and nothing
//!   else: catastrophic floor (always armed), time limit, stop (unless holding
//!   to settlement), take-profit (a FAK at the bid when the bid has reached it,
//!   a resting post-only ask otherwise). No signal of any Raptor is read.
//!
//! # What makes it safe to ship live
//!
//! `helm_enabled` is the kill switch for the path that spends; `helm_live_enabled`
//! ships off and gates real orders only (a simulated squadron ignores it);
//! `helm_max_exposure_usdc` caps the sum of Helm notional across every Helm
//! squadron (they share one session); the collateral and drawdown gates every
//! viper passes are passed here too; and phase 1 is Polymarket International,
//! so the other two builds refuse every entry by construction.

use crate::helpers::helm::{self, EntryKind, HelmIntent, IntentContent, IntentStatus, IntentTally};
use crate::orchestrator::strategy::{Strategy, StrategyContext};
use crate::state::{OrderParams, PositionKey, StrategySignal, StrategyStatus};
use crate::venues::core::{MarketId, TimeInForce};
use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Registry name, as it appears in every `PositionKey`, `trades` row and log
/// line for a Helm position.
pub const STRATEGY_NAME: &str = "HelmStrategy";

/// Taxonomy id. It is at once the viper kind (`viper_kind.id`), the market
/// class a Helm squadron resolves to (`market_class.id`), and the asset a Helm
/// squadron is deployed under (`CryptoAsset::Custom("helm")`). One string for
/// all three is deliberate: the class exists only to carry this viper, and the
/// asset exists only to declare the class.
pub const KIND: &str = "helm";

/// Refusal reasons the status registry shows. Stable text: the Control Tower
/// groups refusals by reason, so these carry the status and not the id.
/// How long a recorded exit reason stays usable.
///
/// The posture records its reason when it emits the exit, because the position
/// leaves the map in the same tick and `on_exit_filled` arrives after fill
/// verification. A requested exit need not fill, though, so the reason expires:
/// past this, whatever closed the position gets the honest fallback rather than
/// an exit attempt's reason it had nothing to do with. Generous against a slow
/// fill verification, short against a position that settles minutes later.
const EXIT_REASON_TTL: Duration = Duration::from_secs(120);

pub const AWAITING_INTENT: &str = "awaiting operator intent";
pub const INTENT_PROPOSED: &str = "intent proposed, awaiting acknowledgement";
pub const INTENT_WORKING: &str = "intent working: entry on the book, awaiting fill";
pub const INTENT_HELD: &str = "intent filled: position held under its posture";
pub const INTENTS_COMPLETE: &str = "all intents terminal, squadron retiring";
pub const DISABLED: &str = "disabled in config (helm_enabled)";
pub const LIVE_DISABLED: &str = "live orders disabled (helm_live_enabled)";
pub const NO_DATABASE: &str = "database unavailable: cannot record the intent going to work";

/// How often the strategy re-reads its squadron's intents. The patrol ticks
/// every 50 ms; the operator edits intents on a human cadence. Invalidated
/// the moment this strategy writes a transition, so its own moves are seen on
/// the next tick.
const INTENT_REFRESH: Duration = Duration::from_secs(1);

/// A fill below this share of what was asked for is `Partial`.
const FULL_FILL_RATIO: Decimal = dec!(0.98);

/// Fee-share ceiling for the fee verdict logged at entry: the figure the form
/// showed the operator before acknowledgement, from the same constant.
const FEE_VERDICT_MAX_RATIO: Decimal = helm::FEE_VERDICT_MAX_RATIO;

// ── Pure decisions ───────────────────────────────────────────────────────────

/// Everything the entry gates need, lifted out of the context so the rules are
/// a function of values and testable without a squadron.
#[derive(Clone, Debug)]
pub(crate) struct EntryInputs {
    pub ghost: bool,
    pub helm_enabled: bool,
    pub live_enabled: bool,
    pub drawdown_hit: bool,
    pub side_is_yes: bool,
    pub bid: Decimal,
    pub ask: Decimal,
    pub secs_to_close: Option<i64>,
    pub min_secs_to_close: i64,
    pub min_shares: Decimal,
    /// Helm notional already held or pending across every Helm squadron.
    pub helm_exposure: Decimal,
    pub max_exposure: Decimal,
    pub available_collateral: Decimal,
    pub now: DateTime<Utc>,
    /// Does a fee-dominated verdict refuse the entry here, as it does at the API?
    pub fee_enforce: bool,
    pub fee_max_ratio: Decimal,
    pub fee_max_notional_pct: Decimal,
}

/// What the entry will do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EntryPlan {
    pub price: Decimal,
    pub shares: Decimal,
    pub order_type: TimeInForce,
    pub post_only: bool,
}

/// The risk gates, in order, and the order they produce. `Err` is the reason
/// to report; the first failing gate wins. Pure.
pub(crate) fn entry_plan(c: &IntentContent, i: &EntryInputs) -> Result<EntryPlan, String> {
    if !i.helm_enabled {
        return Err(DISABLED.into());
    }
    if !i.ghost && !i.live_enabled {
        return Err(LIVE_DISABLED.into());
    }
    if i.drawdown_hit {
        return Err("session drawdown limit hit".into());
    }
    // The book-consistency rules live in one place, `helm::validate_against_book`,
    // and the API runs the same function on a live quote at creation and at
    // acknowledgement. The book may have moved since, so they run again here
    // on the tick's own snapshot; the first failing rule is the refusal.
    let side = if i.side_is_yes { "YES" } else { "NO" };
    let facts = helm::BookFacts {
        bid: i.bid,
        ask: i.ask,
        close_time: i.secs_to_close.map(|s| i.now + chrono::Duration::seconds(s)),
        now: i.now,
        min_shares: i.min_shares,
        min_secs_to_close: i.min_secs_to_close,
    };
    if let Err(errs) = helm::validate_against_book(side, c, &facts) {
        return Err(errs.into_iter().next().unwrap_or_else(|| "posture is inconsistent with the book".into()));
    }
    // The fee gate again, on the tick's own book — the last point before money
    // commits. The API checks at create, revise and acknowledge, but an intent
    // acknowledged while enforcement was off would otherwise enter happily after
    // the knob was turned on, and the book moves between acknowledgement and the
    // entry anyway. Same function, same knobs, so the two cannot disagree.
    if i.fee_enforce {
        let v = helm::fee_verdict(c, &facts, i.fee_max_ratio, i.fee_max_notional_pct);
        if let Some(why) = v.refusal {
            return Err(why);
        }
    }
    let price = helm::entry_price(c, &facts).ok_or_else(|| format!("no ask on the {side} book"))?;
    let (order_type, post_only) = match c.entry_kind {
        EntryKind::Taker => (TimeInForce::Fak, false),
        EntryKind::Resting => (TimeInForce::Gtc, true),
    };
    let shares = c.size_usdc / price;
    if i.helm_exposure + c.size_usdc > i.max_exposure {
        return Err(format!(
            "Helm exposure cap: ${:.2} held + ${} > ${:.2} (helm_max_exposure_usdc)",
            i.helm_exposure, c.size_usdc, i.max_exposure,
        ));
    }
    let fee = crate::venues::round_trip_fee_pct(price) * c.size_usdc;
    if i.available_collateral < c.size_usdc + fee {
        return Err(format!(
            "insufficient collateral: ${:.2} available < ${} + ${fee:.2} fee",
            i.available_collateral, c.size_usdc,
        ));
    }
    Ok(EntryPlan { price, shares, order_type, post_only })
}

/// What the posture says to do with a held position right now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PostureAction {
    /// Sell at the bid with a FAK, for this reason.
    Exit(String),
    /// Rest a post-only ask at this price.
    RestTakeProfit(Decimal),
    Hold,
}

/// The exit posture, applied in priority order: floor, time, stop, take-profit.
/// A dark book (no bid) never triggers a sale: a stop at $0.00 is not a stop.
/// Pure.
pub(crate) fn posture_action(c: &IntentContent, avg_entry: Decimal, bid: Decimal, now: DateTime<Utc>) -> PostureAction {
    let has_bid = bid > Decimal::ZERO;
    if let Some(f) = c.catastrophic_floor_pct {
        let floor = avg_entry * (Decimal::ONE - f);
        if has_bid && bid <= floor {
            return PostureAction::Exit(format!(
                "Helm catastrophic floor: bid=${bid:.4} <= ${floor:.4} ({:.0}% under entry ${avg_entry:.4})",
                f * Decimal::ONE_HUNDRED,
            ));
        }
    }
    if let Some(t) = c.time_limit_at {
        if has_bid && now >= t {
            return PostureAction::Exit(format!("Helm time limit: {} reached, bid=${bid:.4}", t.to_rfc3339()));
        }
    }
    if !c.hold_to_settlement {
        if let Some(s) = c.stop_price {
            if has_bid && bid <= s {
                return PostureAction::Exit(format!("Helm stop: bid=${bid:.4} <= ${s:.4} (entry ${avg_entry:.4})"));
            }
        }
    }
    if let Some(tp) = c.take_profit_price {
        if has_bid && bid >= tp {
            return PostureAction::Exit(format!("Helm take-profit: bid=${bid:.4} >= ${tp:.4} (entry ${avg_entry:.4})"));
        }
        if tp > bid && tp < Decimal::ONE {
            return PostureAction::RestTakeProfit(tp);
        }
    }
    PostureAction::Hold
}

/// What the position map says about an in-flight intent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Reconcile {
    /// Nothing to change yet.
    Wait,
    Filled,
    Partial,
    /// Working, no row, window elapsed.
    Missed,
    /// Working, no row, window not elapsed: the FAK missed (or the row is not
    /// in yet); re-emit the entry.
    Retry,
    /// Working, a resting bid still pending past the window: pull it and
    /// close the intent as missed. The window, not the venue sync's own
    /// stale-cancel, decides how long the operator's bid waits.
    Pull,
    /// Filled or partial, and the position has left the map.
    Gone,
}

/// What `evaluate_entry` should do after reconciling, besides report.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Follow {
    Nothing,
    /// Re-emit this working taker's entry.
    Retry(i64),
    /// Cancel this token's unfilled resting bid.
    Pull(String),
}

/// Pure: the status, what the map holds (shares and whether the fill is
/// effective), what was asked for, and how long the entry has been working.
pub(crate) fn reconcile(
    status: IntentStatus,
    position: Option<(Decimal, bool)>,
    asked_shares: Option<Decimal>,
    working_for_secs: Option<i64>,
    window_secs: i64,
) -> Reconcile {
    let full = |shares: Decimal| asked_shares.map_or(true, |a| a <= Decimal::ZERO || shares >= a * FULL_FILL_RATIO);
    match (status, position) {
        (IntentStatus::Working, Some((shares, true))) => if full(shares) { Reconcile::Filled } else { Reconcile::Partial },
        (IntentStatus::Working, Some((_, false))) => {
            if working_for_secs.is_some_and(|s| s >= window_secs) { Reconcile::Pull } else { Reconcile::Wait }
        }
        (IntentStatus::Working, None) => {
            if working_for_secs.is_some_and(|s| s >= window_secs) { Reconcile::Missed } else { Reconcile::Retry }
        }
        (IntentStatus::Partial, Some((shares, true))) if full(shares) => Reconcile::Filled,
        (IntentStatus::Partial | IntentStatus::Filled, Some(_)) => Reconcile::Wait,
        (IntentStatus::Partial | IntentStatus::Filled, None) => Reconcile::Gone,
        _ => Reconcile::Wait,
    }
}

// ── View ─────────────────────────────────────────────────────────────────────

/// What the strategy knows about its squadron's intents, refreshed on a timer.
#[derive(Clone, Debug, Default)]
pub(crate) struct Snapshot {
    pub tally: IntentTally,
    /// Open (non-terminal) intents, oldest first.
    pub open: Vec<HelmIntent>,
    /// The content the engine reads for each open intent (latest revision).
    pub contents: HashMap<i64, IntentContent>,
}

impl Snapshot {
    /// The reason to report for this view. Pure.
    pub(crate) fn reason(&self) -> &'static str {
        if self.open.is_empty() {
            return if self.tally.total == 0 { AWAITING_INTENT } else { INTENTS_COMPLETE };
        }
        if self.open.iter().any(|i| matches!(i.status, IntentStatus::Filled | IntentStatus::Partial)) {
            return INTENT_HELD;
        }
        if self.open.iter().any(|i| i.status == IntentStatus::Working) {
            return INTENT_WORKING;
        }
        if self.open.iter().any(|i| i.status == IntentStatus::Acknowledged) {
            // An acknowledged intent is about to go to work; the gate that
            // refuses it reports its own reason instead of this one.
            return INTENT_WORKING;
        }
        INTENT_PROPOSED
    }

    pub(crate) fn in_flight(&self) -> impl Iterator<Item = &HelmIntent> {
        self.open.iter().filter(|i| i.is_in_flight())
    }

    pub(crate) fn intent_for_token(&self, token: &str) -> Option<(&HelmIntent, &IntentContent)> {
        let i = self.open.iter().find(|i| i.is_in_flight() && i.token_id.as_deref() == Some(token))?;
        Some((i, self.contents.get(&i.id)?))
    }

    /// The oldest acknowledged intent, if nothing is in flight.
    pub(crate) fn next_to_enter(&self) -> Option<(&HelmIntent, &IntentContent)> {
        if self.in_flight().next().is_some() {
            return None;
        }
        let i = self.open.iter().find(|i| i.status == IntentStatus::Acknowledged)?;
        Some((i, self.contents.get(&i.id)?))
    }
}

// ── The strategy ─────────────────────────────────────────────────────────────

/// The operator's viper.
pub struct HelmStrategy {
    snapshot: Mutex<Option<(Instant, Arc<Snapshot>)>>,
    /// Exit reasons by token, with the moment each was recorded, until
    /// reconciliation closes the intent with them.
    ///
    /// Timestamped because a requested exit need not fill: an unbounded reason
    /// would sit here and later mislabel whatever did close the position —
    /// a settlement reported as the stop that never filled. Stale entries are
    /// ignored, which puts the position back on the honest fallback.
    exit_reasons: Mutex<HashMap<String, (String, Instant)>>,
    /// Tokens whose position was labeled with its intent already.
    labeled: Mutex<HashMap<String, i64>>,
}

impl HelmStrategy {
    pub fn new() -> Self {
        Self {
            snapshot: Mutex::new(None),
            exit_reasons: Mutex::new(HashMap::new()),
            labeled: Mutex::new(HashMap::new()),
        }
    }

    /// For tests: seed the view without a database.
    #[cfg(test)]
    fn seed(&self, s: Snapshot) {
        *self.snapshot.lock().unwrap() = Some((Instant::now(), Arc::new(s)));
    }

    fn invalidate(&self) {
        if let Ok(mut g) = self.snapshot.lock() {
            *g = None;
        }
    }

    /// The squadron's intents, read from the pool its asset aliases to, no
    /// more often than `INTENT_REFRESH`. A squadron with no pool (tests, a
    /// venue path without one) reads as having no intents.
    async fn snapshot(&self, ctx: &StrategyContext) -> Arc<Snapshot> {
        if let Ok(g) = self.snapshot.lock() {
            if let Some((at, s)) = &*g {
                if at.elapsed() < INTENT_REFRESH {
                    return Arc::clone(s);
                }
            }
        }
        let fresh = match crate::helpers::db::pool_for(&ctx.crypto_filter) {
            Some(pool) => {
                let tally = helm::squadron_tally(&pool, &ctx.squadron_id).await;
                let mut open = helm::list_for_squadron(&pool, &ctx.squadron_id, false).await;
                open.sort_by_key(|i| i.id);
                let mut contents = HashMap::with_capacity(open.len());
                for i in &open {
                    contents.insert(i.id, helm::current_content(&pool, i).await);
                }
                Snapshot { tally, open, contents }
            }
            None => Snapshot::default(),
        };
        let fresh = Arc::new(fresh);
        if let Ok(mut g) = self.snapshot.lock() {
            *g = Some((Instant::now(), Arc::clone(&fresh)));
        }
        fresh
    }

    fn report(&self, ctx: &StrategyContext, reason: &str) {
        crate::helpers::viper_status::report_reason(&ctx.crypto_filter, STRATEGY_NAME, reason);
    }

    /// Helm notional held or pending across every squadron in this session,
    /// which is every Helm squadron on the instance.
    async fn helm_exposure(ctx: &StrategyContext) -> Decimal {
        let now = Utc::now();
        let map = ctx.positions.lock().await;
        map.iter()
            .filter(|(k, _)| k.strategy == STRATEGY_NAME)
            .filter(|(_, p)| p.counts_toward_exposure(now))
            .map(|(_, p)| p.shares * p.avg_entry)
            .sum()
    }

    /// Reconcile every in-flight intent against the position map. Writes the
    /// transitions the map implies and returns the intent (and plan inputs)
    /// whose entry should be re-emitted this tick, if any.
    async fn reconcile_in_flight(&self, ctx: &StrategyContext, snap: &Snapshot, pool: &sqlx::SqlitePool) -> Follow {
        let dc = &ctx.dynamic_config;
        let now = Utc::now();
        let mut follow = Follow::Nothing;
        for intent in snap.in_flight() {
            let Some(token) = intent.token_id.as_deref() else { continue };
            let key = PositionKey::new(&ctx.squadron_id, STRATEGY_NAME, MarketId::new(token));
            let pos = {
                let map = ctx.positions.lock().await;
                map.get(&key).map(|p| (p.shares, p.fill_effective_at(dc.ghost_mode).is_some(), p.avg_entry))
            };
            let outcome = reconcile(
                intent.status,
                pos.map(|(s, f, _)| (s, f)),
                intent.entry_shares,
                intent.working_for_secs(now),
                dc.helm_entry_window_secs,
            );
            match outcome {
                Reconcile::Wait => {}
                Reconcile::Retry => follow = Follow::Retry(intent.id),
                Reconcile::Pull => {
                    let why = format!("resting bid unfilled after {}s", dc.helm_entry_window_secs);
                    let _ = helm::transition(pool, intent.id, IntentStatus::Missed, Some(&why)).await;
                    match helm::transition(pool, intent.id, IntentStatus::Closed, Some("entry missed: window expired, bid pulled")).await {
                        Ok(_) => {
                            warn!("🧭 Helm intent #{} → missed → closed: {why}; pulling the bid", intent.id);
                            notify(format!(
                                "🧭 Helm: intent #{} on \"{}\" missed — {why}. Bid pulled; nothing is held.",
                                intent.id, intent.market_name,
                            ));
                        }
                        Err(e) => warn!("Helm intent #{}: could not close after unfilled rest: {e}", intent.id),
                    }
                    follow = Follow::Pull(token.to_string());
                    self.invalidate();
                }
                Reconcile::Filled | Reconcile::Partial => {
                    let (shares, _, avg) = pos.unwrap_or_default();
                    let to = if outcome == Reconcile::Filled { IntentStatus::Filled } else { IntentStatus::Partial };
                    let detail = format!("{shares:.4} shares @ ${avg:.4}");
                    match helm::transition(pool, intent.id, to, Some(&detail)).await {
                        Ok(_) => info!("🧭 Helm intent #{} → {} ({detail})", intent.id, to.as_str()),
                        Err(e) => warn!("Helm intent #{}: could not record {}: {e}", intent.id, to.as_str()),
                    }
                    let already = self.labeled.lock().map(|g| g.get(token) == Some(&intent.id)).unwrap_or(false);
                    if !already {
                        let n = helm::set_position_intent(pool, token, STRATEGY_NAME, intent.id).await.unwrap_or(0);
                        if n > 0 {
                            if let Ok(mut g) = self.labeled.lock() { g.insert(token.to_string(), intent.id); }
                        }
                    }
                    self.invalidate();
                }
                Reconcile::Missed => {
                    let why = format!("no fill within {}s", dc.helm_entry_window_secs);
                    let _ = helm::transition(pool, intent.id, IntentStatus::Missed, Some(&why)).await;
                    match helm::transition(pool, intent.id, IntentStatus::Closed, Some("entry missed: window expired")).await {
                        Ok(_) => {
                            warn!("🧭 Helm intent #{} → missed → closed: {why}", intent.id);
                            notify(format!(
                                "🧭 Helm: intent #{} on \"{}\" missed — {why}. Closed; nothing is held.",
                                intent.id, intent.market_name,
                            ));
                        }
                        Err(e) => warn!("Helm intent #{}: could not close after miss: {e}", intent.id),
                    }
                    self.invalidate();
                }
                Reconcile::Gone => {
                    let reason = self.exit_reasons.lock().ok()
                        .and_then(|mut g| g.remove(token))
                        .filter(|(_, at)| at.elapsed() <= EXIT_REASON_TTL)
                        .map(|(why, _)| why)
                        .unwrap_or_else(|| "position left the map: settled on chain or exited outside the posture".to_string());
                    match helm::transition(pool, intent.id, IntentStatus::Closed, Some(&reason)).await {
                        Ok(_) => info!("🧭 Helm intent #{} → closed ({reason})", intent.id),
                        Err(e) => warn!("Helm intent #{}: could not close: {e}", intent.id),
                    }
                    helm::label_trades(pool, &intent.market_name, &intent.side, intent.id).await;
                    if let Ok(mut g) = self.labeled.lock() { g.remove(token); }
                    self.invalidate();
                }
            }
        }
        follow
    }

    /// Run the gates for one intent and build its order, or report why not.
    async fn plan_entry(&self, ctx: &StrategyContext, c: &IntentContent, side: &str) -> Result<(EntryPlan, MarketId, u16), String> {
        let dc = &ctx.dynamic_config;
        let (market, snap) = (&ctx.market, &ctx.snapshot);
        let side_is_yes = side.eq_ignore_ascii_case("YES");
        let (token, bid, ask, fee_bps) = if side_is_yes {
            (market.yes_token.clone(), snap.yes_bid, snap.yes_ask, market.yes_fee_bps as u16)
        } else {
            (market.no_token.clone(), snap.no_bid, snap.no_ask, market.no_fee_bps as u16)
        };
        let now = Utc::now();
        let inputs = EntryInputs {
            ghost: dc.ghost_mode,
            helm_enabled: dc.helm_enabled,
            live_enabled: dc.helm_live_enabled,
            drawdown_hit: crate::vipers::is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral),
            side_is_yes,
            bid,
            ask,
            secs_to_close: market.market_close_time.map(|t| (t - now).num_seconds()),
            min_secs_to_close: dc.helm_min_secs_to_close,
            min_shares: crate::venues::min_order_shares(),
            helm_exposure: Self::helm_exposure(ctx).await,
            max_exposure: dc.helm_max_exposure_usdc,
            available_collateral: ctx.available_collateral,
            now,
            fee_enforce: dc.helm_fee_verdict_enforce,
            fee_max_ratio: dc.helm_fee_max_ratio,
            fee_max_notional_pct: dc.helm_fee_max_notional_pct,
        };
        entry_plan(c, &inputs).map(|plan| (plan, token, fee_bps))
    }

    fn order(ctx: &StrategyContext, token: MarketId, fee_bps: u16, plan: &EntryPlan) -> OrderParams {
        OrderParams {
            token_id: token,
            price: plan.price,
            shares: plan.shares,
            fee_bps,
            is_neg_risk: ctx.market.is_neg_risk,
            market_name: ctx.market.market_name.clone(),
            condition_id: ctx.market.condition_id.clone(),
            order_type: plan.order_type.clone(),
            post_only: plan.post_only,
            ghost_mode: ctx.dynamic_config.ghost_mode,
        }
    }
}

/// Telegram, when configured; silent otherwise. Spawned so the tick never
/// waits on the network.
fn notify(message: String) {
    let token = std::env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default();
    let chat = std::env::var("TELEGRAM_CHAT_ID").unwrap_or_default();
    if token.is_empty() || chat.is_empty() {
        return;
    }
    tokio::spawn(async move {
        let _ = crate::helpers::notifications::send_notification(&token, &chat, &message).await;
    });
}

impl Default for HelmStrategy {
    fn default() -> Self { Self::new() }
}

#[async_trait::async_trait]
impl Strategy for HelmStrategy {
    /// Reconcile what is in flight, then put the next acknowledged intent to
    /// work if the gates allow. Phase 1 is Polymarket International: the other
    /// builds reconcile and report but never emit.
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let snap = self.snapshot(ctx).await;
        let Some(pool) = crate::helpers::db::pool_for(&ctx.crypto_filter) else {
            self.report(ctx, if snap.open.is_empty() { AWAITING_INTENT } else { NO_DATABASE });
            return Ok(StrategySignal::NoSignal);
        };

        let follow = self.reconcile_in_flight(ctx, &snap, &pool).await;

        // A resting bid that outlived its window: pull it. A cancel is not new
        // risk, so it is emitted on every build. The patrol pulls only an
        // unfilled row and ignores a confirmed one, so a fill that landed in
        // the same instant is kept and the next tick reconciles it.
        if let Follow::Pull(token) = &follow {
            self.report(ctx, INTENT_WORKING);
            return Ok(StrategySignal::MakerCancel { tokens: vec![MarketId::new(token)] });
        }

        // Every venue from here. The gate that used to stand here reported
        // a phase-1 refusal and returned on any build but Polymarket
        // International, because the API's posture validator reached into
        // `venues::intl` for the book. It now goes through `session.venue`, and
        // nothing in this path names a venue: the book comes from `ctx.market`
        // and the order from `ctx`, both of which the `Strategy` trait supplies
        // venue-neutrally. `helm_live_enabled` still ships false everywhere, so
        // each venue earns a ghost acceptance run before it can place an order.
        {
            // A working taker whose FAK missed: re-emit within the window. The
            // patrol's phantom cooldown paces the retries; the intent is already
            // `Working`, so nothing is recorded twice.
            if let Follow::Retry(id) = follow {
                if let Some(intent) = snap.open.iter().find(|i| i.id == id) {
                    if let Some(c) = snap.contents.get(&id) {
                        if c.entry_kind == EntryKind::Taker {
                            match self.plan_entry(ctx, c, &intent.side).await {
                                Ok((plan, token, fee_bps)) => {
                                    self.report(ctx, INTENT_WORKING);
                                    return Ok(StrategySignal::Entry { params: Self::order(ctx, token, fee_bps, &plan), pair_params: None });
                                }
                                Err(why) => {
                                    self.report(ctx, &why);
                                    return Ok(StrategySignal::NoSignal);
                                }
                            }
                        }
                    }
                }
                self.report(ctx, INTENT_WORKING);
                return Ok(StrategySignal::NoSignal);
            }

            let Some((intent, c)) = snap.next_to_enter() else {
                self.report(ctx, snap.reason());
                return Ok(StrategySignal::NoSignal);
            };

            let (plan, token, fee_bps) = match self.plan_entry(ctx, c, &intent.side).await {
                Ok(p) => p,
                Err(why) => {
                    self.report(ctx, &why);
                    return Ok(StrategySignal::NoSignal);
                }
            };

            // Record first, emit second. A crash in between leaves a working
            // intent and no order, which the miss window closes and reports;
            // the other order would leave an order and no record.
            if let Err(e) = helm::mark_working(&pool, intent.id, token.as_str(), plan.price, plan.shares).await {
                warn!("Helm intent #{}: could not mark working, no entry emitted: {e}", intent.id);
                self.report(ctx, NO_DATABASE);
                return Ok(StrategySignal::NoSignal);
            }
            self.invalidate();

            let fee_note = c.take_profit_price
                .filter(|tp| *tp > plan.price)
                .and_then(|tp| crate::vipers::fee_dominated_entry(plan.price, (tp - plan.price) / plan.price, FEE_VERDICT_MAX_RATIO))
                .map(|v| format!(" | fee verdict: {v}"))
                .unwrap_or_default();
            info!(
                "🧭 Helm intent #{} entering [{}]: {} {} {} shares @ ${:.4} (${} notional){}{}",
                intent.id, ctx.squadron_id, intent.side.to_ascii_uppercase(),
                if plan.post_only { "resting bid" } else { "taker" },
                plan.shares.round_dp(4), plan.price, c.size_usdc,
                if ctx.dynamic_config.ghost_mode { " (ghost)" } else { " (LIVE)" },
                fee_note,
            );
            self.report(ctx, INTENT_WORKING);
            Ok(StrategySignal::Entry { params: Self::order(ctx, token, fee_bps, &plan), pair_params: None })
        }
    }

    /// Apply each held position's posture. One FAK exit per tick leaves; a
    /// resting take-profit is returned only when no exit is due, and the patrol
    /// treats it as idempotent.
    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let snap = self.snapshot(ctx).await;
        let dc = &ctx.dynamic_config;
        let now = Utc::now();
        let mut resting: Option<StrategySignal> = None;
        let held: Vec<(PositionKey, Decimal, Decimal)> = {
            let map = ctx.positions.lock().await;
            map.iter()
                .filter(|(k, _)| k.squadron == ctx.squadron_id && k.strategy == STRATEGY_NAME)
                .filter(|(_, p)| p.fill_effective_at(dc.ghost_mode).is_some())
                .map(|(k, p)| (k.clone(), p.shares, p.avg_entry))
                .collect()
        };
        for (key, shares, avg_entry) in held {
            let Some((market, book)) = crate::vipers::venue_for_token(ctx, &key.market) else {
                crate::vipers::note_position_without_venue(STRATEGY_NAME, &key.market);
                continue;
            };
            let is_yes = key.market == market.yes_token;
            let (bid, fee_bps) = if is_yes { (book.yes_bid, market.yes_fee_bps as u16) } else { (book.no_bid, market.no_fee_bps as u16) };
            let Some((intent, c)) = snap.intent_for_token(key.market.as_str()) else {
                // A Helm position with no live intent: chain-readopted after
                // its intent was lost, or a row the reconciliation has not seen
                // yet. Hold; the market's resolution exits it, and the gap is
                // said out loud rather than sold at the bid.
                if crate::vipers::gate_log_permitted(STRATEGY_NAME, &ctx.crypto_filter, key.market.as_str(), 300) {
                    warn!("🧭 Helm holds {} shares of {} with no open intent — holding to settlement", shares, key.market);
                }
                continue;
            };
            let order = |price: Decimal, order_type: TimeInForce, post_only: bool| OrderParams {
                token_id: key.market.clone(), price, shares, fee_bps,
                is_neg_risk: market.is_neg_risk, market_name: market.market_name.clone(),
                condition_id: market.condition_id.clone(), order_type, post_only, ghost_mode: dc.ghost_mode,
            };
            match posture_action(c, avg_entry, bid, now) {
                PostureAction::Exit(reason) => {
                    let reason = format!("{reason} [intent #{}]", intent.id);
                    // Record the reason now, not on the fill hook.
                    //
                    // `on_exit_filled` runs after fill verification, but the
                    // position leaves the map as soon as the trade is booked —
                    // in the same tick. `Reconcile::Gone` therefore reached for
                    // a reason that had not been written yet and attributed
                    // every posture exit to "position left the map", so a stop
                    // firing was indistinguishable from an orphan or an
                    // on-chain settlement in the one field a retrospective
                    // reads. The posture knows why it is exiting here; the hook
                    // still overwrites this with the booked slice's reason when
                    // it arrives.
                    if let Ok(mut g) = self.exit_reasons.lock() {
                        // Keyed on the raw token, as `reconcile_in_flight` and
                        // `on_exit_filled` both are.
                        g.insert(key.market.as_str().to_string(), (reason.clone(), Instant::now()));
                    }
                    return Ok(StrategySignal::Exit {
                        params: order(bid, TimeInForce::Fak, false),
                        reason,
                        exit_pair: false,
                    });
                }
                PostureAction::RestTakeProfit(tp) if resting.is_none() => {
                    resting = Some(StrategySignal::MakerRestingExit {
                        params: order(tp, TimeInForce::Gtc, true),
                        reason: format!("Helm resting TP @ ${tp:.4} [intent #{}]", intent.id),
                    });
                }
                _ => {}
            }
        }
        Ok(resting.unwrap_or(StrategySignal::NoSignal))
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { STRATEGY_NAME.to_string() }
    fn venue(&self) -> &'static str { "Single market" }
    fn max_exposure(&self) -> Decimal { crate::config::HELM_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Operator intent; engine-enforced exit posture" }

    /// The patrol booked an exit slice. Remember its reason for the intent the
    /// next reconciliation closes; nothing here blocks.
    fn on_exit_filled(&self, fill: &crate::state::ExitFill) {
        if let Ok(mut g) = self.exit_reasons.lock() {
            g.insert(fill.token_id.to_string(), (fill.reason.clone(), Instant::now()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::helm::Horizon;
    use crate::state::{MarketConfig, MarketSnapshot, Position, PositionMap};
    use chrono::Duration as CDuration;
    use tokio::sync::Mutex as TMutex;

    fn ctx() -> StrategyContext {
        StrategyContext {
            market_class: Some(KIND.to_string()),
            squadron_id: "helm-open-trial".to_string(),
            market: MarketConfig {
                yes_token: MarketId::new("helm-yes"), no_token: MarketId::new("helm-no"),
                market_name: "Bitcoin Up or Down - October 2, 3PM ET".to_string(),
                market_close_time: Some(Utc::now() + CDuration::minutes(50)),
                strike_price: None, is_neg_risk: false,
                condition_id: "0xhelm".to_string(), yes_fee_bps: 0, no_fee_bps: 0,
            },
            snapshot: MarketSnapshot {
                yes_bid: dec!(0.58), yes_bid_depth: dec!(200),
                yes_ask: dec!(0.60), yes_ask_depth: dec!(150),
                no_bid: dec!(0.40), no_bid_depth: dec!(180),
                no_ask: dec!(0.42), no_ask_depth: dec!(160),
                yes_bid_depth_total: dec!(1200), yes_ask_depth_total: dec!(900),
                no_bid_depth_total: dec!(1100), no_ask_depth_total: dec!(950),
                oracle_price: dec!(0),
                velocity: dec!(0), velocity_1s: dec!(0), acceleration: dec!(0),
                funding_rate: dec!(0), oracle_drift_60m: dec!(0),
                oracle_drift_10m: dec!(0), hist_vol: dec!(0),
                institutional_pulse: dec!(0), tide_coherence: dec!(0),
                tradfi_velocity: dec!(0), macro_coherence: dec!(0),
                vix_proxy: dec!(0), vix_velocity: dec!(0),
                oi_delta_pct: dec!(0), cvd_ratio: dec!(1),
                secs_to_expiry: 3000, timestamp: Utc::now(),
            },
            positions: Arc::new(TMutex::new(PositionMap::new())),
            session_pnl: dec!(0), starting_collateral: dec!(100),
            available_collateral: dec!(100),
            crypto_filter: KIND.to_string(),
            market_started_at: Utc::now(),
            maker_market: None, maker_snapshot: None,
            dynamic_config: Arc::new(crate::helpers::dynamic_config::DynamicConfig::default()),
            arb_market_lockouts: None,
            sports: None,
        }
    }

    fn content() -> IntentContent {
        IntentContent {
            thesis: "t".into(), confidence: 0.7, horizon: Horizon::Expiry, falsification: "f".into(),
            entry_kind: EntryKind::Taker, entry_limit_price: None, size_usdc: dec!(4),
            stop_price: Some(dec!(0.30)), take_profit_price: None, time_limit_at: None,
            hold_to_settlement: false, catastrophic_floor_pct: None,
        }
    }

    fn intent(id: i64, status: IntentStatus, token: Option<&str>) -> HelmIntent {
        HelmIntent {
            id, squadron_id: "helm-open-trial".into(), market_id: "".into(),
            market_name: "Bitcoin Up or Down - October 2, 3PM ET".into(), side: "NO".into(),
            first: content(), current_version: 1, status, status_detail: None, critique: None,
            critique_at: None, critique_model: None, superseded_by: None, close_reason: None,
            ghost: true, venue: "".into(), session_id: "".into(), created_at: "".into(),
            acknowledged_at: None, updated_at: "".into(), closed_at: None,
            token_id: token.map(str::to_string), entry_price: Some(dec!(0.42)),
            entry_shares: Some(dec!(9.5)), working_at: Some(Utc::now().to_rfc3339()),
            fee_verdict: None, critique_requested_at: None, critique_outcome: None,
        }
    }

    fn inputs() -> EntryInputs {
        EntryInputs {
            ghost: true, helm_enabled: true, live_enabled: false, drawdown_hit: false,
            side_is_yes: false, bid: dec!(0.40), ask: dec!(0.42),
            secs_to_close: Some(3000), min_secs_to_close: 120, min_shares: dec!(5),
            helm_exposure: dec!(0), max_exposure: dec!(25), available_collateral: dec!(100),
            now: Utc::now(),
            // Off in the fixture so each gate's test isolates that gate.
            // `content()` is a stop-only posture at an ask of $0.42, which the
            // fee gate refuses at the shipped 5% ceiling — turning it on here
            // would quietly make every other entry test a fee test.
            // `the_fee_gate_refuses_at_the_tick_as_well_as_at_the_api` turns it on.
            fee_enforce: false,
            fee_max_ratio: dec!(0.40),
            fee_max_notional_pct: dec!(0.05),
        }
    }

    // ── Entry gates ─────────────────────────────────────────────────────

    #[test]
    fn a_taker_entry_buys_the_ask_with_a_fak() {
        let plan = entry_plan(&content(), &inputs()).unwrap();
        assert_eq!(plan, EntryPlan { price: dec!(0.42), shares: dec!(4) / dec!(0.42), order_type: TimeInForce::Fak, post_only: false });
    }

    /// The tick re-checks the fee, because the API's check is not the last word.
    ///
    /// An intent acknowledged while `helm_fee_verdict_enforce` was off would
    /// otherwise enter happily once it was turned on — the API gates at create,
    /// revise and acknowledge, and an acknowledged intent is never re-offered to
    /// any of them. The book also moves between acknowledgement and the entry,
    /// which is why the book rules already run again here.
    #[test]
    fn the_fee_gate_refuses_at_the_tick_as_well_as_at_the_api() {
        let mut i = inputs();
        i.fee_enforce = true;

        // `content()` names a stop and no take-profit: targetless, so the fee is
        // the hurdle and two taker legs at $0.42 clear the 5% ceiling.
        let refused = entry_plan(&content(), &i).unwrap_err();
        assert!(refused.contains("names no price target"), "{refused}");

        // Enforcement off, same posture, same book: the entry proceeds. The
        // verdict is still recorded by the API — the knob studies a rule, it
        // does not get an entry past one.
        let mut off = i.clone();
        off.fee_enforce = false;
        assert!(entry_plan(&content(), &off).is_ok());

        // A take-profit gives the ratio a denominator, and a resting one is free
        // to close, so only the entry leg is charged and this passes.
        let mut c = content();
        c.stop_price = None;
        c.take_profit_price = Some(dec!(0.80));
        assert!(entry_plan(&c, &i).is_ok(), "a real target must not be refused");
    }

    #[test]
    fn a_resting_entry_posts_a_post_only_gtc_at_the_limit() {
        let mut c = content();
        c.entry_kind = EntryKind::Resting;
        c.entry_limit_price = Some(dec!(0.39));
        let plan = entry_plan(&c, &inputs()).unwrap();
        assert_eq!(plan.order_type, TimeInForce::Gtc);
        assert!(plan.post_only);
        assert_eq!(plan.price, dec!(0.39));
        c.entry_limit_price = Some(dec!(0.42));
        assert!(entry_plan(&c, &inputs()).unwrap_err().contains("cross the book"));
    }

    /// The live gate: a simulated squadron ignores it, a live one is refused.
    #[test]
    fn live_orders_are_refused_until_the_operator_turns_them_on() {
        let mut i = inputs();
        i.ghost = false;
        assert_eq!(entry_plan(&content(), &i).unwrap_err(), LIVE_DISABLED);
        i.live_enabled = true;
        assert!(entry_plan(&content(), &i).is_ok());
        i.ghost = true; i.live_enabled = false;
        assert!(entry_plan(&content(), &i).is_ok(), "ghost ignores the live gate");
    }

    #[test]
    fn the_kill_switch_and_drawdown_refuse_first() {
        let mut i = inputs();
        i.helm_enabled = false;
        assert_eq!(entry_plan(&content(), &i).unwrap_err(), DISABLED);
        let mut i = inputs();
        i.drawdown_hit = true;
        assert!(entry_plan(&content(), &i).unwrap_err().contains("drawdown"));
    }

    #[test]
    fn the_exposure_cap_is_the_sum_across_helm_squadrons_plus_this_size() {
        let mut i = inputs();
        i.helm_exposure = dec!(22);
        assert!(entry_plan(&content(), &i).unwrap_err().contains("exposure cap"), "22 + 4 > 25");
        i.helm_exposure = dec!(21);
        assert!(entry_plan(&content(), &i).is_ok(), "21 + 4 <= 25");
    }

    #[test]
    fn collateral_minimum_size_and_close_proximity_are_gates() {
        let mut i = inputs();
        i.available_collateral = dec!(4);
        assert!(entry_plan(&content(), &i).unwrap_err().contains("insufficient collateral"));
        let mut i = inputs();
        i.min_shares = dec!(20);
        assert!(entry_plan(&content(), &i).unwrap_err().contains("venue minimum"));
        let mut i = inputs();
        i.secs_to_close = Some(60);
        assert!(entry_plan(&content(), &i).unwrap_err().contains("too close"));
        let mut i = inputs();
        i.ask = dec!(0);
        assert!(entry_plan(&content(), &i).unwrap_err().contains("no ask"));
        // A book with an ask and no bid is one the posture could never exit.
        let mut i = inputs();
        i.bid = dec!(0);
        assert!(entry_plan(&content(), &i).unwrap_err().contains("no bid"));
    }

    /// A posture that would exit on the first tick is refused at entry.
    #[test]
    fn a_stop_above_entry_or_a_take_profit_below_it_is_refused() {
        let mut c = content();
        c.stop_price = Some(dec!(0.45));
        assert!(entry_plan(&c, &inputs()).unwrap_err().contains("fire at once"));
        let mut c = content();
        c.take_profit_price = Some(dec!(0.40));
        assert!(entry_plan(&c, &inputs()).unwrap_err().contains("at or below"));
        let mut c = content();
        c.hold_to_settlement = true;
        // Hold and a stop together is its own refusal, and comes first.
        assert!(entry_plan(&c, &inputs()).unwrap_err().contains("cannot both be set"));
        c.stop_price = None;
        c.catastrophic_floor_pct = Some(dec!(0.5));
        let mut i = inputs();
        i.secs_to_close = None;
        assert!(entry_plan(&c, &i).unwrap_err().contains("close time"));
    }

    // ── Posture ─────────────────────────────────────────────────────────

    #[test]
    fn the_stop_fires_at_or_below_its_price_and_not_on_a_dark_book() {
        let c = content(); // stop 0.30
        let now = Utc::now();
        assert!(matches!(posture_action(&c, dec!(0.42), dec!(0.30), now), PostureAction::Exit(r) if r.contains("Helm stop")));
        assert_eq!(posture_action(&c, dec!(0.42), dec!(0.31), now), PostureAction::Hold);
        assert_eq!(posture_action(&c, dec!(0.42), dec!(0), now), PostureAction::Hold, "no bid is not a stop");
    }

    #[test]
    fn hold_to_settlement_disables_the_stop_but_not_the_floor_or_the_clock() {
        let mut c = content();
        c.hold_to_settlement = true;
        c.catastrophic_floor_pct = Some(dec!(0.5));
        let now = Utc::now();
        assert_eq!(posture_action(&c, dec!(0.42), dec!(0.30), now), PostureAction::Hold, "stop is off under a hold");
        assert!(matches!(posture_action(&c, dec!(0.42), dec!(0.20), now), PostureAction::Exit(r) if r.contains("catastrophic floor")));
        c.time_limit_at = Some(now - CDuration::seconds(1));
        assert!(matches!(posture_action(&c, dec!(0.42), dec!(0.40), now), PostureAction::Exit(r) if r.contains("time limit")));
    }

    #[test]
    fn the_take_profit_rests_until_the_bid_reaches_it_then_takes() {
        let mut c = content();
        c.take_profit_price = Some(dec!(0.55));
        let now = Utc::now();
        assert_eq!(posture_action(&c, dec!(0.42), dec!(0.50), now), PostureAction::RestTakeProfit(dec!(0.55)));
        assert!(matches!(posture_action(&c, dec!(0.42), dec!(0.55), now), PostureAction::Exit(r) if r.contains("take-profit")));
        // A stop outranks a resting take-profit on the same tick.
        assert!(matches!(posture_action(&c, dec!(0.42), dec!(0.29), now), PostureAction::Exit(r) if r.contains("Helm stop")));
    }

    // ── Reconciliation ──────────────────────────────────────────────────

    #[test]
    fn a_confirmed_fill_is_full_or_partial_by_the_asked_shares() {
        use IntentStatus::*;
        assert_eq!(reconcile(Working, Some((dec!(9.5), true)), Some(dec!(9.5)), Some(1), 300), Reconcile::Filled);
        assert_eq!(reconcile(Working, Some((dec!(9.4), true)), Some(dec!(9.5)), Some(1), 300), Reconcile::Filled, "within 2%");
        assert_eq!(reconcile(Working, Some((dec!(5), true)), Some(dec!(9.5)), Some(1), 300), Reconcile::Partial);
        assert_eq!(reconcile(Partial, Some((dec!(9.5), true)), Some(dec!(9.5)), Some(1), 300), Reconcile::Filled);
        assert_eq!(reconcile(Working, Some((dec!(9.5), false)), Some(dec!(9.5)), Some(1), 300), Reconcile::Wait, "pending row");
    }

    /// A resting bid still pending at the window's end is pulled, not left to
    /// the venue sync's own stale-cancel; a fill that lands first is kept.
    #[test]
    fn a_pending_resting_bid_is_pulled_at_the_window_s_end() {
        use IntentStatus::*;
        assert_eq!(reconcile(Working, Some((dec!(9.5), false)), Some(dec!(9.5)), Some(299), 300), Reconcile::Wait);
        assert_eq!(reconcile(Working, Some((dec!(9.5), false)), Some(dec!(9.5)), Some(300), 300), Reconcile::Pull);
        assert_eq!(reconcile(Working, Some((dec!(9.5), true)), Some(dec!(9.5)), Some(300), 300), Reconcile::Filled);
    }

    #[test]
    fn a_working_entry_with_no_row_retries_inside_the_window_and_misses_after() {
        use IntentStatus::*;
        assert_eq!(reconcile(Working, None, Some(dec!(9.5)), Some(10), 300), Reconcile::Retry);
        assert_eq!(reconcile(Working, None, Some(dec!(9.5)), Some(300), 300), Reconcile::Missed);
        assert_eq!(reconcile(Working, None, Some(dec!(9.5)), None, 300), Reconcile::Retry, "no timestamp: do not miss blindly");
    }

    #[test]
    fn a_held_position_that_leaves_the_map_is_gone() {
        use IntentStatus::*;
        assert_eq!(reconcile(Filled, None, None, None, 300), Reconcile::Gone);
        assert_eq!(reconcile(Partial, None, None, None, 300), Reconcile::Gone);
        assert_eq!(reconcile(Filled, Some((dec!(9.5), true)), None, None, 300), Reconcile::Wait);
        assert_eq!(reconcile(Acknowledged, None, None, None, 300), Reconcile::Wait);
    }

    // ── Strategy glue, without a database ────────────────────────────────

    /// No pool: nothing can be recorded, so nothing is emitted, even with an
    /// acknowledged intent in view. Record first, emit second.
    #[tokio::test]
    async fn without_a_database_no_entry_is_emitted() {
        let s = HelmStrategy::new();
        let mut snap = Snapshot::default();
        snap.tally = IntentTally { total: 1, open: 1 };
        let i = intent(1, IntentStatus::Acknowledged, None);
        snap.contents.insert(1, content());
        snap.open.push(i);
        s.seed(snap);
        let c = ctx();
        assert!(matches!(s.evaluate_entry(&c).await.unwrap(), StrategySignal::NoSignal));
    }

    /// The exit side needs no database: a held position with a live intent in
    /// view is managed by its posture. Stop first.
    #[tokio::test]
    async fn a_held_position_exits_by_its_posture() {
        let s = HelmStrategy::new();
        let c = ctx();
        {
            let mut map = c.positions.lock().await;
            map.insert(PositionKey::new("helm-open-trial", STRATEGY_NAME, MarketId::new("helm-no")), Position {
                shares: dec!(9.5), avg_entry: dec!(0.42), opened_at: Utc::now(), close_time: None,
                market_name: c.market.market_name.clone(), pair_token_id: MarketId::new("helm-no"),
                fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: None, entry_fee: Decimal::ZERO,
            });
        }
        let mut snap = Snapshot::default();
        snap.tally = IntentTally { total: 1, open: 1 };
        let mut cc = content();
        cc.stop_price = Some(dec!(0.41)); // bid is 0.40: the stop is through
        snap.contents.insert(7, cc);
        snap.open.push(intent(7, IntentStatus::Filled, Some("helm-no")));
        s.seed(snap);
        match s.evaluate_exit(&c).await.unwrap() {
            StrategySignal::Exit { params, reason, exit_pair } => {
                assert_eq!(params.token_id, MarketId::new("helm-no"));
                assert_eq!(params.price, dec!(0.40));
                assert_eq!(params.shares, dec!(9.5));
                assert_eq!(params.order_type, TimeInForce::Fak);
                assert!(!exit_pair);
                assert!(reason.contains("Helm stop") && reason.contains("#7"), "{reason}");
            }
            other => panic!("expected an exit, got {other:?}"),
        }

        // The reason is recorded as the exit is emitted, not on the fill hook.
        //
        // `on_exit_filled` runs after fill verification, but the position leaves
        // the position map as soon as the trade is booked — in the same tick. So
        // reconciliation saw `Gone`, found nothing recorded, and closed intent 3
        // of 2026-10-02 with "position left the map: settled on chain or exited
        // outside the posture" while the trade row it had just written said
        // "Helm time limit … reached". A stop firing was indistinguishable from
        // an orphan in the one field a retrospective reads.
        let recorded = s.exit_reasons.lock().unwrap();
        let (why, _) = recorded.get("helm-no").expect("the posture records its reason when it exits");
        assert!(why.contains("Helm stop") && why.contains("#7"),
                "the recorded reason must be the posture's own, not the fallback: {why}");
    }

    /// A reason from an exit that never filled must not outlive its moment.
    ///
    /// A requested exit need not fill. Without the TTL the reason would sit in
    /// the map and be handed to whatever eventually closed the position —
    /// reporting a settlement hours later as a stop that never filled, which is
    /// the same misattribution in the other direction.
    #[test]
    fn a_stale_exit_reason_falls_back_rather_than_lying() {
        let s = HelmStrategy::new();
        {
            let mut g = s.exit_reasons.lock().unwrap();
            g.insert("helm-no".to_string(),
                     ("Helm stop: bid=$0.4000 <= $0.4100 [intent #7]".to_string(),
                      Instant::now() - EXIT_REASON_TTL - Duration::from_secs(1)));
        }

        let picked = s.exit_reasons.lock().unwrap()
            .remove("helm-no")
            .filter(|(_, at)| at.elapsed() <= EXIT_REASON_TTL)
            .map(|(why, _)| why);
        assert!(picked.is_none(),
                "a reason older than the TTL must be ignored so the honest fallback is used");

        // And a fresh one is still taken.
        {
            let mut g = s.exit_reasons.lock().unwrap();
            g.insert("helm-no".to_string(), ("Helm take-profit [intent #8]".to_string(), Instant::now()));
        }
        let picked = s.exit_reasons.lock().unwrap()
            .remove("helm-no")
            .filter(|(_, at)| at.elapsed() <= EXIT_REASON_TTL)
            .map(|(why, _)| why);
        assert_eq!(picked.as_deref(), Some("Helm take-profit [intent #8]"));
    }

    /// With the stop clear and a take-profit set, the resting ask is emitted;
    /// with no intent for the token, the position is held and nothing leaves.
    #[tokio::test]
    async fn a_held_position_rests_its_take_profit_or_holds_without_an_intent() {
        let s = HelmStrategy::new();
        let c = ctx();
        {
            let mut map = c.positions.lock().await;
            map.insert(PositionKey::new("helm-open-trial", STRATEGY_NAME, MarketId::new("helm-no")), Position {
                shares: dec!(9.5), avg_entry: dec!(0.42), opened_at: Utc::now(), close_time: None,
                market_name: c.market.market_name.clone(), pair_token_id: MarketId::new("helm-no"),
                fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: None, entry_fee: Decimal::ZERO,
            });
        }
        // No intent in view: hold.
        s.seed(Snapshot::default());
        assert!(matches!(s.evaluate_exit(&c).await.unwrap(), StrategySignal::NoSignal));

        let mut snap = Snapshot::default();
        let mut cc = content();
        cc.take_profit_price = Some(dec!(0.55));
        snap.contents.insert(7, cc);
        snap.open.push(intent(7, IntentStatus::Filled, Some("helm-no")));
        s.seed(snap);
        match s.evaluate_exit(&c).await.unwrap() {
            StrategySignal::MakerRestingExit { params, reason } => {
                assert_eq!(params.price, dec!(0.55));
                assert!(params.post_only);
                assert_eq!(params.order_type, TimeInForce::Gtc);
                assert!(reason.contains("Helm resting TP"));
            }
            other => panic!("expected a resting take-profit, got {other:?}"),
        }
    }

    /// A pending (unconfirmed) entry row is not a position to exit.
    #[tokio::test]
    async fn a_pending_entry_is_never_exited() {
        let s = HelmStrategy::new();
        let c = ctx();
        {
            let mut map = c.positions.lock().await;
            map.insert(PositionKey::new("helm-open-trial", STRATEGY_NAME, MarketId::new("helm-no")), Position {
                shares: dec!(9.5), avg_entry: dec!(0.42), opened_at: Utc::now(), close_time: None,
                market_name: c.market.market_name.clone(), pair_token_id: MarketId::new("helm-no"),
                fill_confirmed_at: None, paired_leg_token_id: None, entry_fee: Decimal::ZERO,
            });
        }
        let mut dc = crate::helpers::dynamic_config::DynamicConfig::default();
        dc.ghost_mode = false; // ghost would count the row as filled at entry
        let c = StrategyContext { dynamic_config: Arc::new(dc), ..c };
        let mut snap = Snapshot::default();
        let mut cc = content();
        cc.stop_price = Some(dec!(0.41));
        snap.contents.insert(7, cc);
        snap.open.push(intent(7, IntentStatus::Working, Some("helm-no")));
        s.seed(snap);
        assert!(matches!(s.evaluate_exit(&c).await.unwrap(), StrategySignal::NoSignal));
    }

    #[test]
    fn the_reported_reason_follows_the_view() {
        let mut s = Snapshot::default();
        assert_eq!(s.reason(), AWAITING_INTENT);
        s.tally = IntentTally { total: 2, open: 0 };
        assert_eq!(s.reason(), INTENTS_COMPLETE);
        s.open.push(intent(1, IntentStatus::Proposed, None));
        assert_eq!(s.reason(), INTENT_PROPOSED);
        s.open.push(intent(2, IntentStatus::Working, Some("t")));
        assert_eq!(s.reason(), INTENT_WORKING);
        s.open.push(intent(3, IntentStatus::Filled, Some("u")));
        assert_eq!(s.reason(), INTENT_HELD);
    }

    /// One in flight at a time: an acknowledged intent waits while another is
    /// working or held in the same squadron.
    #[test]
    fn next_to_enter_waits_while_anything_is_in_flight() {
        let mut s = Snapshot::default();
        s.open.push(intent(1, IntentStatus::Acknowledged, None));
        s.contents.insert(1, content());
        assert_eq!(s.next_to_enter().map(|(i, _)| i.id), Some(1));
        s.open.push(intent(2, IntentStatus::Filled, Some("t")));
        s.contents.insert(2, content());
        assert!(s.next_to_enter().is_none());
    }

    #[test]
    fn name_and_kind_agree_with_the_registry() {
        assert_eq!(HelmStrategy::new().name(), STRATEGY_NAME);
        assert_eq!(crate::orchestrator::registry::strategy_name_to_kind(STRATEGY_NAME), KIND);
    }
}
