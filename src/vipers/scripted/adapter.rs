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

//! `ScriptedStrategy`: one operator script behind the `Strategy` trait.
//!
//! A script can only ask. Every check that protects money is here, in Rust,
//! because the engine has no central one: `Strategy::max_exposure` is a label
//! in the startup log, and each built-in viper enforces its own budget and
//! drawdown check (B91 was Anchor forgetting the latter). So, whatever the
//! script returns:
//!
//! * **Paper unless live.** With the viper's own Live switch off, nothing
//!   reaches the order path: the viper trades in its paper ledger
//!   (`paper.rs`). With it on, orders go out as real ones only while the
//!   instance is live too; on a simulated instance they are the instance's
//!   ordinary simulated orders.
//! * **Exposure cap.** Open cost basis plus the new order, fee included, must
//!   fit the smaller of the manifest's cap and the operator's. An order that
//!   does not fit is refused, not resized.
//! * **Daily drawdown.** Real-money entries stop with every other viper's.
//! * **Order sanity.** A price strictly inside (0, 1) on the tick, at least the
//!   venue's minimum size, only this squadron's market, one position per side,
//!   and a sale only of what this viper holds.
//!
//! Script errors, malformed returns and refusals become the viper's refusal
//! reason on its card. Nothing a script does can panic the patrol.

use std::collections::HashMap;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tracing::{info, warn};

use chrono::Utc;

use crate::config;
use crate::helpers::dynamic_config::CustomViperConfig;
use crate::helpers::viper_status;
use crate::orchestrator::strategy::{Strategy, StrategyContext};
use crate::state::{OrderParams, StrategySignal, StrategyStatus};
use crate::venues::core::{MarketId, TimeInForce};

use super::context::{self, Account, PosView};
use super::paper::{self, PaperRow};
use super::runtime::{self, Action, Side};
use super::store::{self, Script};

pub struct ScriptedStrategy {
    script: String,
    name: String,
}

impl ScriptedStrategy {
    pub fn new(script: &str) -> Self {
        Self { script: script.to_string(), name: store::strategy_name(script) }
    }
}

/// How this viper's orders are handled right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// The paper ledger; nothing reaches the order path.
    Paper,
    /// The order path, simulated or real as the instance is.
    Orders { ghost: bool },
}

/// [E95] Why a script holds when its squadron does not fly its market.
pub fn no_market_reason(role: crate::orchestrator::strategy::MarketRole) -> String {
    use crate::orchestrator::strategy::MarketRole;
    match role {
        MarketRole::UltraShort => "no ultra-short market on this squadron (this venue lists none for it, or the ultra-short market is off)".into(),
        MarketRole::Window => "no window market on this squadron".into(),
        MarketRole::Hourly => "no market".into(),
    }
}

/// The refusal while the squadron's price feed has no price yet [B95].
pub const ORACLE_UNKNOWN: &str = "oracle price unknown (the price feed has not printed yet)";

pub fn mode(cfg: &CustomViperConfig, instance_ghost: bool) -> Mode {
    if cfg.live { Mode::Orders { ghost: instance_ghost } } else { Mode::Paper }
}

/// A buy, checked and sized; what the adapter will actually do.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckedBuy {
    pub side: Side,
    pub price: Decimal,
    pub shares: Decimal,
    pub fee: Decimal,
    pub take_profit: Option<Decimal>,
}

/// The most a taker order's limit may sit past the touch: two ticks.
pub const MAX_SLIPPAGE: Decimal = rust_decimal_macros::dec!(0.02);

/// Everything about a buy the adapter decides, with the book and the viper's
/// budget as plain values so it can be checked without a squadron.
#[allow(clippy::too_many_arguments)]
pub fn check_buy(
    side: Side, price: Decimal, shares: Decimal, take_profit: Option<Decimal>,
    ask: Decimal, ask_depth: Decimal, paper: bool,
    exposure: Decimal, max_exposure: Decimal, held: Option<Side>,
    collateral: Option<Decimal>, whole_contracts: bool, min_shares: Decimal,
) -> Result<CheckedBuy, String> {
    let s = side.as_str().to_uppercase();
    // One position per market: holding both sides of one market is a box
    // that only pays the fees, and the intl patrol refuses it anyway.
    if let Some(h) = held {
        return Err(format!("already holds {} on this market", h.as_str().to_uppercase()));
    }
    let mut price_r = crate::helpers::price::round_to_tick_size(price);
    // A venue that quotes finer than a cent (Kalshi: $0.9720) puts the ask
    // between ticks. A script buying AT that ask must not be rounded below it
    // and refused as unfillable (production 2026-11-08 00:41 ET: "buy price
    // 0.97 is below the NO ask 0.9720"): its limit is the ask rounded up to
    // the next tick, still inside `MAX_SLIPPAGE`, and the venue fills at the
    // price offered.
    if price_r < ask && price >= ask {
        let tick = rust_decimal_macros::dec!(0.01);
        price_r = (ask / tick).ceil() * tick;
    }
    if price_r <= Decimal::ZERO || price_r >= Decimal::ONE {
        return Err(format!("buy price {price} is not inside (0, 1)"));
    }
    if ask <= Decimal::ZERO || ask >= Decimal::ONE {
        return Err(format!("no {s} ask to buy from"));
    }
    if price_r < ask {
        return Err(format!("buy price {price_r} is below the {s} ask {ask}: a taker order would not fill"));
    }
    let mut shares = if whole_contracts { shares.floor() } else { shares.round_dp(2) };
    // A taker fills at the ask, and only as deep as the ask is. The paper
    // ledger fills what a real order would; a real FAK does this itself, up
    // to its limit, which is never more than `MAX_SLIPPAGE` over the ask: a
    // script asking 0.95 against an ask of 0.42 would otherwise sweep the book,
    // and the paper ledger (filling at the ask) would flatter it.
    let limit = price_r.min(ask + MAX_SLIPPAGE);
    let fill_price = if paper { ask } else { limit };
    if paper && ask_depth > Decimal::ZERO && shares > ask_depth {
        shares = if whole_contracts { ask_depth.floor() } else { ask_depth.round_dp(2) };
    }
    if shares < min_shares {
        return Err(format!("{shares} shares is under the venue minimum {min_shares}"));
    }
    let fee = crate::venues::taker_fee_per_share(fill_price) * shares;
    let cost = fill_price * shares + fee;
    if exposure + cost > max_exposure {
        return Err(format!(
            "exposure cap: ${exposure:.2} open + ${cost:.2} for this order is over ${max_exposure:.2}"
        ));
    }
    if let Some(c) = collateral {
        if cost > c {
            return Err(format!("collateral ${c:.2} does not cover ${cost:.2}"));
        }
    }
    let take_profit = match take_profit {
        None => None,
        Some(tp) => {
            let tp = crate::helpers::price::round_to_tick_size(tp);
            if tp <= fill_price || tp >= Decimal::ONE {
                return Err(format!("take_profit {tp} must be above the entry {fill_price} and below 1"));
            }
            Some(tp)
        }
    };
    Ok(CheckedBuy { side, price: fill_price, shares, fee, take_profit })
}

/// Take-profits of positions held through the order path, by (squadron,
/// script) then token. Kalshi and Polymarket US have no resting exit
/// (`MakerRestingExit` is a no-op there), so the adapter watches the bid and
/// sells as a taker when it reaches the target, the same on every venue and in
/// the paper ledger.
///
/// The target stays armed until the viper no longer holds the token: a sale
/// that does not fill (a FAK miss, the venue loop backing off, a partial fill)
/// is simply tried again next tick. It is written to `script_take_profit` so a
/// restart does not drop it, and read back the first time a (squadron, script)
/// is seen.
#[derive(Default)]
struct TpBook {
    loaded: bool,
    by_token: HashMap<String, (Decimal, chrono::DateTime<Utc>)>,
}

fn tp_books() -> &'static std::sync::Mutex<HashMap<(String, String), TpBook>> {
    static M: std::sync::OnceLock<std::sync::Mutex<HashMap<(String, String), TpBook>>> = std::sync::OnceLock::new();
    M.get_or_init(Default::default)
}

fn tp_lock() -> std::sync::MutexGuard<'static, HashMap<(String, String), TpBook>> {
    tp_books().lock().unwrap_or_else(|p| p.into_inner())
}

/// A target armed this recently is kept even though its position has not
/// appeared yet: the entry is in flight.
const TP_FILL_GRACE_SECS: i64 = 300;

async fn tp_load(pool: &sqlx::SqlitePool, sq: &str, script: &str) {
    let key = (sq.to_string(), script.to_string());
    if tp_lock().get(&key).is_some_and(|b| b.loaded) { return; }
    let rows: Vec<(String, f64, String)> = sqlx::query_as(
        "SELECT token_id, take_profit, set_at FROM script_take_profit WHERE squadron_id = ? AND script = ?"
    ).bind(sq).bind(script).fetch_all(pool).await.unwrap_or_default();
    let mut m = tp_lock();
    let b = m.entry(key).or_default();
    if b.loaded { return; }
    for (t, tp, at) in rows {
        let at = chrono::DateTime::parse_from_rfc3339(&at).map(|x| x.with_timezone(&Utc)).unwrap_or_else(|_| Utc::now());
        b.by_token.entry(t).or_insert((Decimal::from_f64_retain(tp).unwrap_or_default().round_dp(4), at));
    }
    b.loaded = true;
}

fn tp_get(sq: &str, script: &str, token: &str) -> Option<Decimal> {
    tp_lock().get(&(sq.to_string(), script.to_string())).and_then(|b| b.by_token.get(token)).map(|(t, _)| *t)
}

async fn tp_set(pool: Option<&sqlx::SqlitePool>, sq: &str, script: &str, token: &str, tp: Option<Decimal>) {
    {
        let mut m = tp_lock();
        let b = m.entry((sq.to_string(), script.to_string())).or_default();
        match tp {
            Some(t) => { b.by_token.insert(token.to_string(), (t, Utc::now())); }
            None => { b.by_token.remove(token); }
        }
    }
    let Some(pool) = pool else { return };
    let _ = match tp {
        Some(t) => sqlx::query(
            "INSERT INTO script_take_profit (squadron_id, script, token_id, take_profit, set_at) VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(squadron_id, script, token_id) DO UPDATE SET take_profit = excluded.take_profit, set_at = excluded.set_at"
        ).bind(sq).bind(script).bind(token).bind(t.to_f64().unwrap_or(0.0)).bind(Utc::now().to_rfc3339())
            .execute(pool).await,
        None => sqlx::query("DELETE FROM script_take_profit WHERE squadron_id = ? AND script = ? AND token_id = ?")
            .bind(sq).bind(script).bind(token).execute(pool).await,
    };
}

/// Disarm the targets of tokens this viper no longer holds, past the grace an
/// in-flight entry needs.
async fn tp_prune(pool: Option<&sqlx::SqlitePool>, sq: &str, script: &str, held: &[String]) {
    let now = Utc::now();
    let stale: Vec<String> = tp_lock().get(&(sq.to_string(), script.to_string()))
        .map(|b| b.by_token.iter()
            .filter(|(t, (_, at))| !held.contains(t) && (now - *at).num_seconds() > TP_FILL_GRACE_SECS)
            .map(|(t, _)| t.clone()).collect())
        .unwrap_or_default();
    for t in stale { tp_set(pool, sq, script, &t, None).await; }
}

/// This strategy's open cost basis in `open_positions` across every database,
/// fee estimated at the taker rate. Read at most every two seconds per
/// strategy: the entry tick runs every 50 ms.
async fn db_exposure(strategy: &str) -> Decimal {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<HashMap<String, (std::time::Instant, Decimal)>>> = std::sync::OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some((at, v)) = cache.lock().unwrap_or_else(|p| p.into_inner()).get(strategy) {
        if at.elapsed().as_secs() < 2 { return *v; }
    }
    let mut total = Decimal::ZERO;
    for pool in crate::helpers::db::all_pools() {
        let rows: Vec<(String, String)> = sqlx::query_as("SELECT entry_price, shares FROM open_positions WHERE strategy = ?")
            .bind(strategy).fetch_all(&pool).await.unwrap_or_default();
        for (p, n) in rows {
            let (Ok(p), Ok(n)) = (p.parse::<Decimal>(), n.parse::<Decimal>()) else { continue };
            total += p * n + crate::venues::taker_fee_per_share(p) * n;
        }
    }
    cache.lock().unwrap_or_else(|p| p.into_inner()).insert(strategy.to_string(), (std::time::Instant::now(), total));
    total
}

/// One warning per script per minute, so a broken script does not flood the log.
fn warn_throttled(script: &str, msg: &str) {
    static LAST: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, std::time::Instant>>> = std::sync::OnceLock::new();
    let mut m = LAST.get_or_init(Default::default).lock().unwrap_or_else(|p| p.into_inner());
    if m.get(script).is_none_or(|t| t.elapsed().as_secs() >= 60) {
        m.insert(script.to_string(), std::time::Instant::now());
        warn!("📜 Script viper '{script}': {msg}");
    }
}

struct Live {
    script: std::sync::Arc<Script>,
    cfg: CustomViperConfig,
    mode: Mode,
    max_exposure: Decimal,
}

impl ScriptedStrategy {
    fn reason(&self, ctx: &StrategyContext, r: &str) {
        viper_status::report_reason(&ctx.crypto_filter, &self.name, r);
    }

    /// The script, its settings and its mode, whether or not it may enter
    /// here. Exits need only this: a viper switched off, or taken off live,
    /// still manages what it holds.
    fn settings(&self, ctx: &StrategyContext) -> Option<Live> {
        let script = store::get(&self.script)?;
        let cfg = ctx.dynamic_config.custom_viper(&self.script);
        let instance_ghost = config::GHOST_MODE || ctx.dynamic_config.ghost_mode;
        let max_exposure = cfg.max_exposure_usdc
            .map_or(script.manifest.max_exposure_usdc, |m| m.min(script.manifest.max_exposure_usdc));
        Some(Live { mode: mode(&cfg, instance_ghost), script, cfg, max_exposure })
    }

    /// Why this viper may not open anything here, if it may not.
    fn entry_refusal(live: &Live, ctx: &StrategyContext) -> Option<String> {
        if !live.cfg.enabled {
            return Some(viper_status::DISABLED_IN_CONFIG.into());
        }
        if !live.script.manifest.runs_on(crate::venues::venue_name()) {
            return Some(format!("not declared for venue {}", crate::venues::venue_name()));
        }
        if let Some(class) = ctx.market_class.as_deref() {
            if !live.script.manifest.classes.iter().any(|c| c == class) {
                return Some(format!("not declared for class {class}"));
            }
        }
        // [B95] A squadron whose price feed has not printed yet reads an oracle
        // of 0. The context gives the script `()` there, but a comparison with
        // `()` is false rather than an error, so `if ctx.oracle > ctx.strike
        // { .. } else { buy }` would still open on it. The engine refuses the
        // entry itself, as Maker and Time Decay do; exits keep running.
        if ctx.has_price_feed && ctx.snapshot.oracle_price <= Decimal::ZERO {
            return Some(ORACLE_UNKNOWN.into());
        }
        // [E95] A market listed ahead of its window (a venue publishes the
        // next 15-minute market before the current one closes): its reference
        // price does not exist yet, so nothing is entered before it opens.
        if live.script.manifest.market == crate::orchestrator::strategy::MarketRole::UltraShort
            && ctx.market_started_at > Utc::now()
        {
            return Some("the market's window has not opened yet".into());
        }
        None
    }

    fn asset(ctx: &StrategyContext) -> String { ctx.crypto_filter.to_lowercase() }

    /// This viper's order-path positions: on this squadron, or (for the
    /// exposure cap, which is the viper's, not the squadron's) on every one.
    async fn order_positions(&self, ctx: &StrategyContext, every_squadron: bool) -> Vec<(MarketId, crate::state::Position)> {
        let map = ctx.positions.lock().await;
        map.iter()
            .filter(|(k, _)| k.strategy == self.name && (every_squadron || k.squadron == ctx.squadron_id))
            .map(|(k, p)| (k.market.clone(), p.clone()))
            .collect()
    }

    /// This viper's open cost basis, fees included, across every squadron, and
    /// the side it holds on this squadron's market, if any.
    async fn holdings(&self, ctx: &StrategyContext, paper_mode: bool) -> Option<(Decimal, Option<Side>)> {
        if paper_mode {
            let pool = crate::helpers::db::pool_for(&Self::asset(ctx))?;
            let here = paper::cached_open(&pool, &ctx.squadron_id, &self.script).await;
            let exposure = paper::cached_all(&self.script).iter().map(PaperRow::cost).sum();
            Some((exposure, here.iter().find_map(|r| Self::side_of(ctx, &r.token_id))))
        } else {
            // Each market loop (Kalshi, US) and each asset (intl) has its own
            // position map, so the map alone would let the cap multiply by the
            // number of loops. `open_positions` is written by every loop; the
            // larger of the two counts an entry still in flight as well.
            let here = self.order_positions(ctx, true).await;
            let in_map: Decimal = here.iter().map(|(_, p)| p.avg_entry * p.shares + p.entry_fee).sum();
            let exposure = in_map.max(db_exposure(&self.name).await);
            let held = self.order_positions(ctx, false).await.iter().find_map(|(t, _)| Self::side_of(ctx, t.as_str()));
            Some((exposure, held))
        }
    }

    /// [E95] The market and book a held token trades on, from the squadron's
    /// full context (hourly, window or ultra-short), with its side: exits
    /// follow the position's token, not the market the script enters on.
    fn resolve<'a>(full: &'a StrategyContext, token: &str) -> Option<(Side, &'a crate::state::MarketConfig, &'a crate::state::MarketSnapshot)> {
        let (m, s) = crate::vipers::venue_for_token(full, &MarketId::new(token))?;
        let side = if token == m.yes_token.as_str() { Side::Yes } else { Side::No };
        Some((side, m, s))
    }

    fn bid_fee(m: &crate::state::MarketConfig, s: &crate::state::MarketSnapshot, side: Side) -> (Decimal, u32) {
        match side {
            Side::Yes => (s.yes_bid, m.yes_fee_bps),
            Side::No => (s.no_bid, m.no_fee_bps),
        }
    }

    fn side_of(ctx: &StrategyContext, token: &str) -> Option<Side> {
        if token == ctx.market.yes_token.as_str() { Some(Side::Yes) }
        else if token == ctx.market.no_token.as_str() { Some(Side::No) }
        else { None }
    }

    fn book(ctx: &StrategyContext, side: Side) -> (MarketId, Decimal, Decimal, Decimal, u32) {
        let s = &ctx.snapshot;
        let m = &ctx.market;
        match side {
            Side::Yes => (m.yes_token.clone(), s.yes_bid, s.yes_ask, s.yes_ask_depth, m.yes_fee_bps),
            Side::No => (m.no_token.clone(), s.no_bid, s.no_ask, s.no_ask_depth, m.no_fee_bps),
        }
    }

    fn order(ctx: &StrategyContext, token: MarketId, price: Decimal, shares: Decimal, fee_bps: u32, ghost: bool) -> OrderParams {
        Self::order_on(&ctx.market, token, price, shares, fee_bps, ghost)
    }

    fn order_on(m: &crate::state::MarketConfig, token: MarketId, price: Decimal, shares: Decimal, fee_bps: u32, ghost: bool) -> OrderParams {
        OrderParams {
            token_id: token,
            price,
            shares,
            fee_bps: fee_bps as u16,
            is_neg_risk: m.is_neg_risk,
            market_name: m.market_name.clone(),
            condition_id: m.condition_id.clone(),
            order_type: TimeInForce::Fak,
            post_only: false,
            ghost_mode: ghost,
        }
    }

    async fn paper_summary(&self, ctx: &StrategyContext) {
        if let Some(pool) = crate::helpers::db::pool_for(&Self::asset(ctx)) {
            let s = paper::summary(&pool, &self.script).await;
            viper_status::report_detail(&ctx.crypto_filter, &self.name, Some(format!(
                "paper: {} open, {} closed, {} of {} won, net ${:+.2}", s.open, s.closed, s.wins, s.scored, s.pnl
            )));
        }
    }

    async fn entry(&self, ctx: &StrategyContext) -> StrategySignal {
        let Some(live) = self.settings(ctx) else {
            self.reason(ctx, "script removed");
            return StrategySignal::NoSignal;
        };
        if let Some(r) = Self::entry_refusal(&live, ctx) {
            self.reason(ctx, &r);
            return StrategySignal::NoSignal;
        }
        let paper = live.mode == Mode::Paper;
        if !paper && crate::vipers::drawdown_limit_hit(ctx) {
            self.reason(ctx, "daily drawdown limit hit");
            return StrategySignal::NoSignal;
        }
        let asset = Self::asset(ctx);
        let pool = crate::helpers::db::pool_for(&asset);
        let Some((exposure, held)) = self.holdings(ctx, paper).await else {
            self.reason(ctx, "no database for the paper ledger");
            return StrategySignal::NoSignal;
        };

        let cmap = self.context(ctx, &live, exposure);
        store::note_context(&ctx.squadron_id, self.market_role(), &cmap);
        let ast = std::sync::Arc::clone(&live.script.ast);
        let out = store::with_state(&ctx.squadron_id, &self.script, |st| runtime::call_entry(&ast, cmap, st));
        let action = match out {
            Ok(a) => a,
            Err(e) => {
                warn_throttled(&self.script, &e);
                self.reason(ctx, &format!("script error: {e}"));
                return StrategySignal::NoSignal;
            }
        };
        let (side, price, shares, tp, why) = match action {
            Action::Hold(r) => {
                self.reason(ctx, r.as_deref().unwrap_or("holding"));
                return StrategySignal::NoSignal;
            }
            Action::Buy { side, price, shares, take_profit, reason } => (side, price, shares, take_profit, reason),
            other => {
                self.reason(ctx, &format!("entry() may only hold or buy, got {other:?}"));
                return StrategySignal::NoSignal;
            }
        };
        let (token, _bid, ask, ask_depth, fee_bps) = Self::book(ctx, side);
        let checked = check_buy(
            side, price, shares, tp, ask, ask_depth, paper, exposure, live.max_exposure, held,
            (!paper).then_some(ctx.available_collateral),
            // Kalshi and Polymarket US trade whole contracts; only the intl CLOB
            // takes fractional sizes.
            crate::venues::venue_name() != "intl_clob", crate::venues::min_order_shares(),
        );
        let b = match checked {
            Ok(b) => b,
            Err(r) => { self.reason(ctx, &r); return StrategySignal::NoSignal; }
        };
        let why = why.unwrap_or_else(|| "script entry".into());
        let label = side.as_str().to_uppercase();

        if paper {
            let Some(pool) = pool else { return StrategySignal::NoSignal };
            let mut r = PaperRow {
                id: 0, script: self.script.clone(), squadron_id: ctx.squadron_id.clone(),
                condition_id: ctx.market.condition_id.clone(), token_id: token.as_str().to_string(),
                market: ctx.market.market_name.clone(), side: label.clone(), opened_at: Utc::now(),
                close_time: ctx.market.market_close_time, entry_price: b.price, shares: b.shares,
                entry_fee: b.fee, take_profit: b.take_profit, entry_reason: why.clone(),
                script_hash: live.script.hash.clone(), closed_at: None, exit_price: None,
                exit_fee: None, exit_reason: None, close_hash: None, pnl: None,
            };
            match paper::insert(&pool, &asset, &r).await {
                Some(id) => {
                    r.id = id;
                    paper::cache_add(&ctx.squadron_id, &self.script, r);
                    info!("📜 [{}] PAPER {label} {} @ ${:.2} (fee ${:.4}) on {}{} — {why} [v{}]",
                          self.name, b.shares, b.price, b.fee, ctx.market.market_name,
                          b.take_profit.map_or(String::new(), |t| format!(", take-profit ${t:.2}")), live.script.hash);
                    self.paper_summary(ctx).await;
                }
                None => self.reason(ctx, "paper ledger write failed"),
            }
            return StrategySignal::NoSignal;
        }

        let Mode::Orders { ghost } = live.mode else { unreachable!() };
        if let Some(t) = b.take_profit {
            tp_set(pool.as_ref(), &ctx.squadron_id, &self.script, token.as_str(), Some(t)).await;
        }
        info!("📜 [{}] {label} {} @ ${:.2} limit on {}{} — {why} [v{}]{}", self.name, b.shares, b.price,
              ctx.market.market_name, b.take_profit.map_or(String::new(), |t| format!(", take-profit ${t:.2}")),
              live.script.hash, if ghost { " (simulated)" } else { "" });
        StrategySignal::Entry {
            params: Self::order(ctx, token, b.price, b.shares, fee_bps, ghost),
            pair_params: None,
        }
    }

    fn context(&self, ctx: &StrategyContext, live: &Live, exposure: Decimal) -> rhai::Map {
        self.context_as(ctx, live, exposure, !matches!(live.mode, Mode::Orders { ghost: false }))
    }

    /// `ghost` is what `ctx.ghost` tells the script: a position held through the
    /// order path is sold real on a live instance even after the viper went
    /// back to paper, and its exit must say so.
    fn context_as(&self, ctx: &StrategyContext, live: &Live, exposure: Decimal, ghost: bool) -> rhai::Map {
        let params = live.script.manifest.resolve_params(&live.cfg.params);
        let acct = Account {
            ghost,
            exposure, max_exposure: live.max_exposure, day_pnl: crate::vipers::day_pnl(ctx),
        };
        context::build(ctx, &params, &acct)
    }

    /// Exits run for whatever the viper holds, whatever its switches say now:
    /// switching a viper off, or taking it off live, stops new entries only.
    /// A position it opened keeps its stop and its take-profit until it is
    /// closed or settles. Paper rows keep being managed after the viper goes
    /// live, so the paper record stays honest.
    /// `ctx` is what the script's exit() sees (its market's view); `full` is the
    /// squadron's whole context, which each held token is resolved against.
    async fn exit(&self, ctx: &StrategyContext, full: &StrategyContext) -> StrategySignal {
        let Some(live) = self.settings(ctx) else { return StrategySignal::NoSignal };
        let paper_mode = live.mode == Mode::Paper;
        let exposure = self.holdings(ctx, paper_mode).await.map_or(Decimal::ZERO, |(e, _)| e);
        self.paper_exits(ctx, full, &live, &self.context_as(ctx, &live, exposure, true)).await;
        let ghost = config::GHOST_MODE || ctx.dynamic_config.ghost_mode;
        self.order_exits(ctx, full, &live, &self.context_as(ctx, &live, exposure, ghost), ghost).await
    }

    fn call_exit(&self, ctx: &StrategyContext, live: &Live, cmap: &rhai::Map, pos: &PosView) -> Result<Action, String> {
        let ast = std::sync::Arc::clone(&live.script.ast);
        store::with_state(&ctx.squadron_id, &self.script, |st| runtime::call_exit(&ast, cmap.clone(), context::position(pos), st))
    }

    /// Book paper rows whose market has closed, at the venue's resolution.
    async fn paper_settle(&self, ctx: &StrategyContext, hash: &str) {
        let Some(pool) = crate::helpers::db::pool_for(&Self::asset(ctx)) else { return };
        let now = Utc::now();
        for r in paper::cached_open(&pool, &ctx.squadron_id, &self.script).await {
            let since_close = (now - r.close_time.unwrap_or(r.opened_at + chrono::Duration::hours(24))).num_seconds();
            if since_close < paper::SETTLE_GRACE_SECS { continue; }
            if let Some(px) = paper::resolution(&r.condition_id, &r.token_id) {
                let pnl = paper::pnl_at(&r, px, Decimal::ZERO);
                if paper::close(&pool, r.id, px, Decimal::ZERO, "settled", hash, Some(pnl)).await {
                    paper::cache_remove(&ctx.squadron_id, &self.script, r.id);
                    paper::forget_resolution(&r.token_id);
                    info!("📜 [{}] PAPER settled {} {} at ${:.2}: {:+.4}", self.name, r.market, r.side, px, pnl);
                    self.paper_summary(ctx).await;
                }
            } else if since_close >= paper::GIVE_UP_SECS
                && paper::close(&pool, r.id, r.entry_price, Decimal::ZERO, "unresolved after 24h, not scored", hash, None).await
            {
                paper::cache_remove(&ctx.squadron_id, &self.script, r.id);
                warn!("📜 [{}] PAPER row #{} on {} written off unscored: the venue never resolved it", self.name, r.id, r.market);
            }
        }
    }

    async fn paper_exits(&self, ctx: &StrategyContext, full: &StrategyContext, live: &Live, cmap: &rhai::Map) {
        self.paper_settle(ctx, &live.script.hash).await;
        let Some(pool) = crate::helpers::db::pool_for(&Self::asset(ctx)) else { return };
        let now = Utc::now();
        for r in paper::cached_open(&pool, &ctx.squadron_id, &self.script).await {
            let Some((side, m, snap)) = Self::resolve(full, &r.token_id) else { continue };
            let (bid, _) = Self::bid_fee(m, snap, side);
            let decided = if r.take_profit.is_some_and(|tp| bid > Decimal::ZERO && bid >= tp) {
                Some(format!("take-profit ${:.2} reached", r.take_profit.unwrap_or_default()))
            } else {
                let pv = PosView {
                    side: side.as_str(), shares: r.shares, avg_entry: r.entry_price, entry_fee: r.entry_fee,
                    held_secs: (now - r.opened_at).num_seconds(), take_profit: r.take_profit, market_name: r.market.clone(),
                };
                match self.call_exit(ctx, live, cmap, &pv) {
                    Ok(Action::Sell { price, reason }) => match check_sell(price, bid) {
                        Ok(_) => Some(reason.unwrap_or_else(|| "script exit".into())),
                        Err(e) => { self.reason(ctx, &e); None }
                    },
                    Ok(Action::TakeProfit { price }) => {
                        let tp = crate::helpers::price::round_to_tick_size(price);
                        if tp > Decimal::ZERO && tp < Decimal::ONE && r.take_profit != Some(tp)
                            && paper::set_take_profit(&pool, r.id, tp).await {
                            paper::cache_set_tp(&ctx.squadron_id, &self.script, r.id, tp);
                        }
                        None
                    }
                    Ok(Action::Hold(_)) => None,
                    Ok(other) => { self.reason(ctx, &format!("exit() may only hold, sell or set take_profit, got {other:?}")); None }
                    Err(e) => { warn_throttled(&self.script, &e); self.reason(ctx, &format!("script error: {e}")); None }
                }
            };
            // A paper sale fills at the bid, as a taker's would.
            if let Some(why) = decided {
                let fee = crate::venues::taker_fee_per_share(bid) * r.shares;
                let pnl = paper::pnl_at(&r, bid, fee);
                if paper::close(&pool, r.id, bid, fee, &why, &live.script.hash, Some(pnl)).await {
                    paper::cache_remove(&ctx.squadron_id, &self.script, r.id);
                    info!("📜 [{}] PAPER sold {} {} @ ${:.2}: {:+.4} — {why} [v{}]", self.name, r.market, r.side, bid, pnl, live.script.hash);
                    self.paper_summary(ctx).await;
                }
            }
        }
    }

    async fn order_exits(&self, ctx: &StrategyContext, full: &StrategyContext, live: &Live, cmap: &rhai::Map, ghost: bool) -> StrategySignal {
        let pool = crate::helpers::db::pool_for(&Self::asset(ctx));
        let held = self.order_positions(ctx, false).await;
        if let Some(p) = pool.as_ref() { tp_load(p, &ctx.squadron_id, &self.script).await; }
        let tokens: Vec<String> = held.iter().map(|(t, _)| t.as_str().to_string()).collect();
        tp_prune(pool.as_ref(), &ctx.squadron_id, &self.script, &tokens).await;
        let now = Utc::now();
        for (token, p) in held {
            let Some((side, m, snap)) = Self::resolve(full, token.as_str()) else { continue };
            let (bid, fee_bps) = Self::bid_fee(m, snap, side);
            let tp = tp_get(&ctx.squadron_id, &self.script, token.as_str());
            let sell = |price: Decimal, why: String| StrategySignal::Exit {
                params: Self::order_on(m, token.clone(), price, p.shares, fee_bps, ghost),
                reason: format!("{}: {why} [v{}]", self.name, live.script.hash),
                exit_pair: false,
            };
            // The target stays armed: if this sale does not fill, the next
            // tick tries again. It is disarmed once the position is gone.
            if tp.is_some_and(|t| bid > Decimal::ZERO && bid >= t) {
                return sell(bid, format!("take-profit ${:.2} reached, bid ${bid:.2}", tp.unwrap_or_default()));
            }
            let pv = PosView {
                side: side.as_str(), shares: p.shares, avg_entry: p.avg_entry, entry_fee: p.entry_fee,
                held_secs: (now - p.opened_at).num_seconds(), take_profit: tp, market_name: p.market_name.clone(),
            };
            match self.call_exit(ctx, live, cmap, &pv) {
                Ok(Action::Sell { price, reason }) => match check_sell(price, bid) {
                    Ok(limit) => return sell(limit, reason.unwrap_or_else(|| "script exit".into())),
                    Err(e) => self.reason(ctx, &e),
                },
                Ok(Action::TakeProfit { price }) => {
                    let t = crate::helpers::price::round_to_tick_size(price);
                    if t > Decimal::ZERO && t < Decimal::ONE && tp != Some(t) {
                        tp_set(pool.as_ref(), &ctx.squadron_id, &self.script, token.as_str(), Some(t)).await;
                    }
                }
                Ok(Action::Hold(_)) => {}
                Ok(other) => self.reason(ctx, &format!("exit() may only hold, sell or set take_profit, got {other:?}")),
                Err(e) => { warn_throttled(&self.script, &e); self.reason(ctx, &format!("script error: {e}")); }
            }
        }
        StrategySignal::NoSignal
    }
}

/// A taker sale's limit: inside (0, 1), at or under the bid, and never more
/// than `MAX_SLIPPAGE` under it.
pub fn check_sell(price: Decimal, bid: Decimal) -> Result<Decimal, String> {
    let mut p = crate::helpers::price::round_to_tick_size(price);
    // The mirror of `check_buy`: a sale AT a bid between ticks ($0.975) must
    // not round above it and be refused; its limit is the bid rounded down.
    if p > bid && price <= bid {
        p = crate::helpers::price::floor_to_tick_size(bid);
    }
    if p <= Decimal::ZERO || p >= Decimal::ONE {
        return Err(format!("sell price {price} is not inside (0, 1)"));
    }
    if bid <= Decimal::ZERO {
        return Err("no bid to sell into".into());
    }
    if p > bid {
        return Err(format!("sell at {p} is above the bid {bid}: a taker order would not fill"));
    }
    Ok(p.max(bid - MAX_SLIPPAGE))
}

#[async_trait::async_trait]
impl Strategy for ScriptedStrategy {
    // [E95] A script runs against the market it declared: the squadron's
    // context viewed with that market as the primary. Without that market here
    // its entries hold with the reason; its exits still run on the squadron's
    // context, so paper rows on markets already left are settled.
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> anyhow::Result<StrategySignal> {
        let role = self.market_role();
        if role == crate::orchestrator::strategy::MarketRole::Hourly {
            return Ok(self.entry(ctx).await);
        }
        match ctx.view(role) {
            Some(v) => Ok(self.entry(&v).await),
            None => {
                self.reason(ctx, &no_market_reason(role));
                Ok(StrategySignal::NoSignal)
            }
        }
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> anyhow::Result<StrategySignal> {
        let role = self.market_role();
        if role == crate::orchestrator::strategy::MarketRole::Hourly {
            return Ok(self.exit(ctx, ctx).await);
        }
        match ctx.view(role) {
            Some(v) => Ok(self.exit(&v, ctx).await),
            None => Ok(self.exit(ctx, ctx).await),
        }
    }

    fn market_role(&self) -> crate::orchestrator::strategy::MarketRole {
        store::get(&self.script).map(|s| s.manifest.market).unwrap_or_default()
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { self.name.clone() }
    fn venue(&self) -> &'static str { "Script" }
    fn max_exposure(&self) -> Decimal {
        store::get(&self.script).map_or(Decimal::ZERO, |s| s.manifest.max_exposure_usdc)
    }
    fn risk_model(&self) -> &'static str { "Operator script (adapter-enforced cap)" }
}

/// Dry run: evaluate `entry`, and `exit` for each paper position this viper
/// holds on the squadron's current market, against the last context the
/// squadron handed a script, with a copy of the state, and dispatch nothing.
/// `source` evaluates unsaved code. Order-path positions are not visible from
/// here (they live in the venue loop); their exits are evaluated on the tick.
///
/// The recorded context is whichever script last ran on that squadron, so its
/// `params` (and `exposure`) are replaced with this script's: the parameter
/// values `params` resolves (from the draft's manifest when one is being
/// tested), and this viper's paper exposure.
pub fn dry_run(squadron: &str, script: &str, source: Option<&str>,
               params: &std::collections::BTreeMap<String, crate::vipers::scripted::manifest::ParamValue>,
               role: crate::orchestrator::strategy::MarketRole) -> Result<serde_json::Value, String> {
    if source.is_some_and(|s| s.len() > store::MAX_SOURCE_BYTES) {
        return Err(format!("the script is over {} KB", store::MAX_SOURCE_BYTES / 1024));
    }
    let last = store::last_context(squadron, role).ok_or_else(|| format!(
        "no {} context recorded for squadron '{squadron}' yet: a context is recorded each tick a script trading that market runs there",
        role.label(),
    ))?;
    let ast = match source {
        Some(src) => runtime::compile(src)?,
        None => store::get(script).map(|s| (*s.ast).clone()).ok_or_else(|| format!("no script '{script}'"))?,
    };
    let mut ctx = last.ctx.clone();
    ctx.insert("params".into(), context::params_value(params));
    let exposure: Decimal = paper::cached_all(script).iter().map(PaperRow::cost).sum();
    ctx.insert("exposure".into(), rhai::Dynamic::from_decimal(exposure));
    let last = store::LastContext { at: last.at, ctx };
    let mut st = store::state_copy(squadron, script);
    let t = std::time::Instant::now();
    let entry = runtime::call_entry(&ast, last.ctx.clone(), &mut st);
    let show = |r: Result<Action, String>| match r {
        Ok(a) => serde_json::json!({ "ok": format!("{a:?}") }),
        Err(e) => serde_json::json!({ "error": e }),
    };
    let market = last.ctx.get("market_name").map(|m| m.to_string()).unwrap_or_default();
    let now = Utc::now();
    let exits: Vec<serde_json::Value> = paper::cached_for(squadron, script).into_iter()
        .filter(|r| r.market == market)
        .map(|r| {
            let pv = PosView {
                side: if r.side == "YES" { "yes" } else { "no" }, shares: r.shares, avg_entry: r.entry_price,
                entry_fee: r.entry_fee, held_secs: (now - r.opened_at).num_seconds(),
                take_profit: r.take_profit, market_name: r.market.clone(),
            };
            serde_json::json!({ "paper_row": r.id, "side": r.side,
                "exit": show(runtime::call_exit(&ast, last.ctx.clone(), context::position(&pv), &mut st)) })
        })
        .collect();
    let micros = t.elapsed().as_micros();
    Ok(serde_json::json!({
        "squadron": squadron,
        "context_at": last.at,
        "context": serde_json::to_value(rhai::Dynamic::from_map(last.ctx.clone())).unwrap_or(serde_json::Value::Null),
        "entry": show(entry),
        "exits": exits,
        "micros": micros,
        "note": "evaluated against a copy of the viper's state; nothing was sent. The adapter's exposure, size and price checks still apply on a real tick.",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::sync::Arc;
    use crate::helpers::dynamic_config::DynamicConfig;
    use crate::state::{MarketConfig, MarketSnapshot, PositionMap};

    const E2E: &str = r#"
        fn entry(ctx) {
            if ctx.params.armed != true { return #{ hold: "parked" }; }
            #{ buy: "yes", price: ctx.yes.ask, shares: ctx.params.contracts,
               take_profit: ctx.yes.ask + 0.05, reason: `test entry v${ctx.velocity}` }
        }
        fn exit(ctx, pos) { () }
    "#;

    fn manifest(name: &str, cap: Decimal) -> crate::vipers::scripted::manifest::Manifest {
        serde_json::from_value(serde_json::json!({
            "name": name, "display_name": "E2E", "classes": ["crypto"], "max_exposure_usdc": cap,
            "params": {
                "armed": { "type": "bool", "default": true },
                "contracts": { "type": "int", "default": 5, "min": 1, "max": 50 }
            }
        })).unwrap()
    }

    fn ctx(asset: &str, dc: DynamicConfig) -> StrategyContext {
        StrategyContext {
            market_class: Some("crypto".into()),
            has_price_feed: true,
            squadron_id: format!("{asset}-open"),
            market: MarketConfig {
                yes_token: MarketId::new(&format!("{asset}-yes")), no_token: MarketId::new(&format!("{asset}-no")),
                market_name: "Bitcoin above $100,000 at 3PM ET".into(),
                market_close_time: Some(Utc::now() + chrono::Duration::minutes(30)),
                strike_price: Some(dec!(100000)), window_secs: None, is_neg_risk: false,
                condition_id: format!("{asset}-cid"), yes_fee_bps: 0, no_fee_bps: 0,
            },
            snapshot: MarketSnapshot {
                yes_bid: dec!(0.40), yes_bid_depth: dec!(200), yes_ask: dec!(0.42), yes_ask_depth: dec!(150),
                no_bid: dec!(0.57), no_bid_depth: dec!(180), no_ask: dec!(0.59), no_ask_depth: dec!(160),
                yes_bid_depth_total: dec!(1200), yes_ask_depth_total: dec!(900),
                no_bid_depth_total: dec!(1100), no_ask_depth_total: dec!(950),
                // Real units: velocity and drifts are dollar moves, hist_vol the
                // 0-1 flatness index (about 0.003 for ordinary BTC).
                oracle_price: dec!(100100), velocity: dec!(120), velocity_1s: dec!(30), acceleration: dec!(0),
                funding_rate: dec!(0), oracle_drift_60m: dec!(-250), oracle_drift_10m: dec!(80), hist_vol: dec!(0.003),
                institutional_pulse: dec!(0), tide_coherence: dec!(0), tradfi_velocity: dec!(0),
                macro_coherence: dec!(0), vix_proxy: dec!(0), vix_velocity: dec!(0),
                oi_delta_pct: dec!(0), cvd_ratio: dec!(1), secs_to_expiry: 1800, timestamp: Utc::now(),
            },
            positions: Arc::new(tokio::sync::Mutex::new(PositionMap::new())),
            session_pnl: dec!(0), starting_collateral: dec!(100), available_collateral: dec!(100),
            crypto_filter: asset.into(), market_started_at: Utc::now(),
            maker_market: None, maker_snapshot: None,
            ultra_market: None, ultra_snapshot: None,
            dynamic_config: Arc::new(dc), arb_market_lockouts: None, sports: None,
        }
    }

    async fn pool(asset: &str) -> sqlx::SqlitePool {
        let path = std::env::temp_dir().join(format!("dradis-{asset}-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        crate::helpers::db::init_for_asset(asset, path.to_str().unwrap()).await.unwrap();
        crate::helpers::db::pool_for(asset).unwrap()
    }

    /// A tick end to end: a paper buy at the ask with the taker fee, the cap
    /// counting it, a second buy of the same side refused, and the take-profit
    /// sold as a taker once the bid reaches it.
    #[tokio::test]
    async fn a_paper_viper_buys_refuses_a_second_and_takes_its_profit() {
        let asset = "scripte2epaper";
        let pool = pool(asset).await;
        store::insert_hidden_for_test(manifest(asset, dec!(10)), E2E);
        let strat = ScriptedStrategy::new(asset);
        let c = ctx(asset, DynamicConfig::default());

        assert!(matches!(strat.evaluate_entry(&c).await.unwrap(), StrategySignal::NoSignal), "paper never dispatches");
        let rows = paper::open_rows(&pool, &c.squadron_id, asset).await;
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!((r.side.as_str(), r.entry_price, r.shares, r.take_profit), ("YES", dec!(0.42), dec!(5), Some(dec!(0.47))));
        assert_eq!(r.entry_fee, (crate::venues::taker_fee_per_share(dec!(0.42)) * dec!(5)).round_dp(6));
        assert!(r.entry_reason.starts_with("test entry v120"), "{}", r.entry_reason);

        strat.evaluate_entry(&c).await.unwrap();
        assert_eq!(paper::open_rows(&pool, &c.squadron_id, asset).await.len(), 1, "one position per side");
        let status = serde_json::to_string(&viper_status::snapshot(Some(asset))).unwrap();
        assert!(status.contains("already holds YES"), "{status}");

        let mut up = c.clone();
        up.snapshot.yes_bid = dec!(0.48);
        strat.evaluate_exit(&up).await.unwrap();
        assert!(paper::open_rows(&pool, &c.squadron_id, asset).await.is_empty(), "the take-profit sold");
        let done = &paper::recent(&pool, asset, 5).await[0];
        let fee_out = crate::venues::taker_fee_per_share(dec!(0.48)) * dec!(5);
        assert_eq!(done.exit_price, Some(dec!(0.48)));
        assert_eq!(done.pnl, Some(((dec!(0.48) - dec!(0.42)) * dec!(5) - r.entry_fee - fee_out).round_dp(6)));
    }

    /// [E95] A script that declares the ultra-short market trades that market's
    /// own tokens, at its book, with its clock; on a squadron without one it
    /// holds and says why. Its take-profit is watched on that market's book.
    #[tokio::test]
    async fn an_ultra_short_script_trades_the_ultra_market_or_holds_without_one() {
        use crate::orchestrator::strategy::MarketRole;
        let asset = "scriptultra";
        let pool = pool(asset).await;
        let mut m = manifest(asset, dec!(10));
        m.market = MarketRole::UltraShort;
        store::insert_hidden_for_test(m, E2E);
        let strat = ScriptedStrategy::new(asset);
        assert_eq!(strat.market_role(), MarketRole::UltraShort);

        let c = ctx(asset, DynamicConfig::default());
        strat.evaluate_entry(&c).await.unwrap();
        assert!(paper::open_rows(&pool, &c.squadron_id, asset).await.is_empty(), "no ultra market, no entry");
        let status = serde_json::to_string(&viper_status::snapshot(Some(asset))).unwrap();
        assert!(status.contains("no ultra-short market on this squadron"), "{status}");

        let mut u = c.clone();
        let close = Utc::now() + chrono::Duration::minutes(5);
        u.ultra_market = Some(MarketConfig {
            yes_token: MarketId::new("u-yes"), no_token: MarketId::new("u-no"),
            market_name: "BTC price up in next 15 mins?".into(), market_close_time: Some(close),
            strike_price: Some(dec!(100050)), window_secs: Some(900), is_neg_risk: false,
            condition_id: "u-cid".into(), yes_fee_bps: 0, no_fee_bps: 0,
        });
        let mut us = c.snapshot.clone();
        us.yes_ask = dec!(0.30); us.yes_bid = dec!(0.28);
        u.ultra_snapshot = Some(us);
        let v = u.view(MarketRole::UltraShort).unwrap();
        assert_eq!(v.market_started_at, close - chrono::Duration::seconds(900), "the clock is the ultra market's");

        strat.evaluate_entry(&u).await.unwrap();
        let rows = paper::open_rows(&pool, &c.squadron_id, asset).await;
        assert_eq!(rows.len(), 1);
        assert_eq!((rows[0].token_id.as_str(), rows[0].condition_id.as_str(), rows[0].entry_price), ("u-yes", "u-cid", dec!(0.30)));
        assert_eq!(rows[0].market, "BTC price up in next 15 mins?");

        let mut up = u.clone();
        up.ultra_snapshot.as_mut().unwrap().yes_bid = dec!(0.40);
        strat.evaluate_exit(&up).await.unwrap();
        assert!(paper::open_rows(&pool, &c.squadron_id, asset).await.is_empty(), "the take-profit read the ultra book");
    }

    /// [E95] A market listed ahead of its window is not entered before it opens.
    #[tokio::test]
    async fn a_market_whose_window_has_not_opened_is_not_entered() {
        use crate::orchestrator::strategy::MarketRole;
        let asset = "scriptultraearly";
        let pool = pool(asset).await;
        let mut m = manifest(asset, dec!(10));
        m.market = MarketRole::UltraShort;
        store::insert_hidden_for_test(m, E2E);
        let strat = ScriptedStrategy::new(asset);
        let mut u = ctx(asset, DynamicConfig::default());
        u.ultra_market = Some(MarketConfig {
            yes_token: MarketId::new("e-yes"), no_token: MarketId::new("e-no"),
            market_name: "next window".into(), market_close_time: Some(Utc::now() + chrono::Duration::minutes(16)),
            strike_price: None, window_secs: Some(900), is_neg_risk: false,
            condition_id: "e-cid".into(), yes_fee_bps: 0, no_fee_bps: 0,
        });
        u.ultra_snapshot = Some(u.snapshot.clone());
        strat.evaluate_entry(&u).await.unwrap();
        assert!(paper::open_rows(&pool, &u.squadron_id, asset).await.is_empty());
        let status = serde_json::to_string(&viper_status::snapshot(Some(asset))).unwrap();
        assert!(status.contains("has not opened yet"), "{status}");
    }

    /// The cap is the manifest's (or the operator's, if lower), and the Live
    /// switch sends an order through the path with the instance's ghost flag.
    #[tokio::test]
    async fn the_cap_refuses_and_the_live_switch_reaches_the_order_path() {
        let asset = "scripte2elive";
        let _pool = pool(asset).await;
        store::insert_hidden_for_test(manifest(asset, dec!(1)), E2E);
        let strat = ScriptedStrategy::new(asset);
        let c = ctx(asset, DynamicConfig::default());
        strat.evaluate_entry(&c).await.unwrap();
        assert!(serde_json::to_string(&viper_status::snapshot(Some(asset))).unwrap().contains("exposure cap"), "5 × 0.42 is over $1");

        store::insert_hidden_for_test(manifest(asset, dec!(10)), E2E);
        let mut dc = DynamicConfig::default();
        dc.ghost_mode = true;
        dc.custom_vipers.insert(asset.into(), CustomViperConfig { live: true, ..Default::default() });
        let c = ctx(asset, dc.clone());
        let StrategySignal::Entry { params, pair_params: None } = strat.evaluate_entry(&c).await.unwrap() else {
            panic!("the live switch sends an order");
        };
        assert!(params.ghost_mode, "on a simulated instance the order is simulated");
        assert_eq!((params.price, params.shares, params.order_type), (dec!(0.42), dec!(5), TimeInForce::Fak));

        dc.custom_vipers.insert(asset.into(), CustomViperConfig { live: true, max_exposure_usdc: Some(dec!(1)), ..Default::default() });
        assert!(matches!(strat.evaluate_entry(&ctx(asset, dc.clone())).await.unwrap(), StrategySignal::NoSignal),
            "the operator's lower cap wins");
        dc.custom_vipers.insert(asset.into(), CustomViperConfig { enabled: false, live: true, ..Default::default() });
        assert!(matches!(strat.evaluate_entry(&ctx(asset, dc)).await.unwrap(), StrategySignal::NoSignal), "switched off");
    }

    fn buy(price: Decimal, shares: Decimal, ask: Decimal, depth: Decimal, paper: bool, exposure: Decimal) -> Result<CheckedBuy, String> {
        check_buy(Side::Yes, price, shares, Some(price + dec!(0.10)), ask, depth, paper, exposure, dec!(10), None, Some(dec!(100)), true, dec!(1))
    }

    #[test]
    fn a_paper_buy_fills_at_the_ask_as_deep_as_the_ask_with_the_taker_fee() {
        let b = buy(dec!(0.45), dec!(10), dec!(0.42), dec!(6), true, dec!(0)).unwrap();
        assert_eq!(b.price, dec!(0.42), "a taker pays the ask, not its limit");
        assert_eq!(b.shares, dec!(6), "only what the ask shows");
        assert_eq!(b.fee, crate::venues::taker_fee_per_share(dec!(0.42)) * dec!(6));
    }

    #[test]
    fn the_cap_is_the_adapters_and_counts_what_is_already_open() {
        let e = buy(dec!(0.50), dec!(10), dec!(0.50), dec!(100), true, dec!(6)).unwrap_err();
        assert!(e.starts_with("exposure cap: $6.00 open"), "{e}");
        assert!(buy(dec!(0.50), dec!(5), dec!(0.50), dec!(100), true, dec!(0)).is_ok());
    }

    #[test]
    fn prices_sizes_and_targets_are_checked() {
        assert!(buy(dec!(0.40), dec!(5), dec!(0.42), dec!(100), true, dec!(0)).unwrap_err().contains("below the YES ask"));
        assert!(buy(dec!(1.20), dec!(5), dec!(0.42), dec!(100), true, dec!(0)).unwrap_err().contains("not inside (0, 1)"));
        assert!(buy(dec!(0.42), dec!(0.6), dec!(0.42), dec!(100), true, dec!(0)).unwrap_err().contains("venue minimum"),
            "whole contracts on Kalshi: 0.6 floors to 0");
        let e = check_buy(Side::No, dec!(0.42), dec!(5), Some(dec!(0.40)), dec!(0.42), dec!(100), true,
            dec!(0), dec!(10), None, None, true, dec!(1)).unwrap_err();
        assert!(e.contains("take_profit"), "{e}");
        let e = check_buy(Side::No, dec!(0.42), dec!(5), None, dec!(0.42), dec!(100), true,
            dec!(0), dec!(10), Some(Side::Yes), None, true, dec!(1)).unwrap_err();
        assert_eq!(e, "already holds YES on this market", "one position per market, either side");
        let e = check_buy(Side::Yes, dec!(0.42), dec!(5), None, dec!(0.42), dec!(100), false,
            dec!(0), dec!(10), None, Some(dec!(1)), true, dec!(1)).unwrap_err();
        assert!(e.starts_with("collateral"), "{e}");
    }

    /// [production 2026-11-08 00:41 ET] Kalshi quotes to a tenth of a cent. A
    /// buy at an ask of 0.972 was rounded to 0.97 and refused as below the
    /// ask; its limit is now 0.98, and paper fills at the ask itself. A sale at
    /// a bid of 0.975 sells with a 0.97 limit instead of being refused.
    #[test]
    fn a_price_between_ticks_is_rounded_toward_a_fill() {
        let live = check_buy(Side::No, dec!(0.972), dec!(5), None, dec!(0.972), dec!(100), false,
            dec!(0), dec!(10), None, Some(dec!(100)), true, dec!(1)).unwrap();
        assert_eq!(live.price, dec!(0.98), "the limit is the ask rounded up");
        let paper = check_buy(Side::No, dec!(0.972), dec!(5), None, dec!(0.972), dec!(100), true,
            dec!(0), dec!(10), None, Some(dec!(100)), true, dec!(1)).unwrap();
        assert_eq!(paper.price, dec!(0.972), "paper fills at the ask itself");
        assert!(check_buy(Side::No, dec!(0.96), dec!(5), None, dec!(0.972), dec!(100), false,
            dec!(0), dec!(10), None, Some(dec!(100)), true, dec!(1)).unwrap_err().contains("below the NO ask"),
            "a price under the ask is still refused");
        assert_eq!(check_sell(dec!(0.975), dec!(0.975)), Ok(dec!(0.97)));
        assert_eq!(check_sell(dec!(0.42), dec!(0.42)), Ok(dec!(0.42)));
    }

    #[test]
    fn a_limit_never_strays_more_than_two_ticks_past_the_touch() {
        let b = check_buy(Side::Yes, dec!(0.95), dec!(5), None, dec!(0.42), dec!(100), false,
            dec!(0), dec!(10), None, Some(dec!(100)), true, dec!(1)).unwrap();
        assert_eq!(b.price, dec!(0.44), "a buy at 0.95 against an ask of 0.42 is limited to 0.44");
        assert_eq!(check_sell(dec!(0.10), dec!(0.40)), Ok(dec!(0.38)), "a sale at 0.10 into a 0.40 bid sells at 0.38 at worst");
        assert_eq!(check_sell(dec!(0.40), dec!(0.40)), Ok(dec!(0.40)));
        assert!(check_sell(dec!(0.45), dec!(0.40)).unwrap_err().contains("above the bid"));
        assert!(check_sell(dec!(0.30), dec!(0)).unwrap_err().contains("no bid"));
    }

    fn hold(c: &StrategyContext, asset: &str, token: &MarketId) -> crate::state::PositionKey {
        crate::state::PositionKey::new(c.squadron_id.clone(), store::strategy_name(asset), token.clone())
    }

    fn position(c: &StrategyContext) -> crate::state::Position {
        crate::state::Position {
            shares: dec!(5), avg_entry: dec!(0.42), opened_at: Utc::now(), close_time: c.market.market_close_time,
            market_name: c.market.market_name.clone(), pair_token_id: c.market.no_token.clone(),
            fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: None, entry_fee: dec!(0.07),
        }
    }

    /// A real position's take-profit stays armed through a sale that does not
    /// fill, survives a restart (it is read back from the database), and keeps
    /// firing after the viper is switched off and taken off live: switching a
    /// viper off stops entries, never the exits of what it holds.
    #[tokio::test]
    async fn take_profits_stay_armed_persist_and_outlive_the_switches() {
        let asset = "scripte2etp";
        let pool = pool(asset).await;
        store::insert_hidden_for_test(manifest(asset, dec!(10)), E2E);
        let strat = ScriptedStrategy::new(asset);
        let mut dc = DynamicConfig::default();
        dc.custom_vipers.insert(asset.into(), CustomViperConfig { live: true, ..Default::default() });
        let mut c = ctx(asset, dc.clone());
        let yes = c.market.yes_token.clone();
        c.positions.lock().await.insert(hold(&c, asset, &yes), position(&c));
        tp_set(Some(&pool), &c.squadron_id, asset, yes.as_str(), Some(dec!(0.47))).await;

        assert!(matches!(strat.evaluate_exit(&c).await.unwrap(), StrategySignal::NoSignal), "bid 0.40 is under the target");
        c.snapshot.yes_bid = dec!(0.48);
        for attempt in 0..2 {
            let StrategySignal::Exit { params, .. } = strat.evaluate_exit(&c).await.unwrap() else {
                panic!("attempt {attempt}: the target fires and stays armed after a sale that did not fill");
            };
            assert_eq!((params.price, params.shares), (dec!(0.48), dec!(5)));
        }

        tp_lock().clear(); // a restart: memory gone, the database remembers
        let mut off = DynamicConfig::default();
        off.custom_vipers.insert(asset.into(), CustomViperConfig { enabled: false, live: false, ..Default::default() });
        c.dynamic_config = Arc::new(off);
        assert!(matches!(strat.evaluate_exit(&c).await.unwrap(), StrategySignal::Exit { .. }),
            "read back after the restart, and still managed with the viper off and paper");

        c.positions.lock().await.clear();
        assert!(matches!(strat.evaluate_exit(&c).await.unwrap(), StrategySignal::NoSignal), "nothing held, nothing sold");
    }

    /// A paper position still open when its market closes is booked at the
    /// venue's resolution, without an exit fee.
    #[tokio::test]
    async fn a_paper_position_settles_at_the_venues_resolution() {
        let asset = "scripte2esettle";
        let pool = pool(asset).await;
        store::insert_hidden_for_test(manifest(asset, dec!(10)), E2E);
        let strat = ScriptedStrategy::new(asset);
        let mut c = ctx(asset, DynamicConfig::default());
        c.market.market_close_time = Some(Utc::now() - chrono::Duration::minutes(5));
        strat.evaluate_entry(&c).await.unwrap();
        let r = paper::open_rows(&pool, &c.squadron_id, asset).await.remove(0);

        strat.evaluate_exit(&c).await.unwrap();
        assert_eq!(paper::open_rows(&pool, &c.squadron_id, asset).await.len(), 1, "no answer from the venue yet");
        paper::set_resolution_for_test(&r.token_id, dec!(1));
        strat.evaluate_exit(&c).await.unwrap();
        let done = &paper::recent(&pool, asset, 1).await[0];
        assert_eq!(done.exit_reason.as_deref(), Some("settled"));
        assert_eq!(done.pnl, Some(((dec!(1) - r.entry_price) * r.shares - r.entry_fee).round_dp(6)), "no fee at settlement");
    }

    /// The real-money cap counts the viper's positions in `open_positions`,
    /// which every market loop writes, not only this loop's position map.
    #[tokio::test]
    async fn the_live_cap_counts_positions_held_by_other_loops() {
        let asset = "scripte2ecap";
        let pool = pool(asset).await;
        store::insert_hidden_for_test(manifest(asset, dec!(10)), E2E);
        sqlx::query("INSERT INTO open_positions (ts, session_id, strategy, token_id, market, side, entry_price, shares)
                     VALUES (?, 's', ?, 'other-loop-token', 'another market', 'YES', '0.90', '9')")
            .bind(Utc::now().to_rfc3339()).bind(store::strategy_name(asset)).execute(&pool).await.unwrap();
        let mut dc = DynamicConfig::default();
        dc.custom_vipers.insert(asset.into(), CustomViperConfig { live: true, ..Default::default() });
        let strat = ScriptedStrategy::new(asset);
        assert!(matches!(strat.evaluate_entry(&ctx(asset, dc)).await.unwrap(), StrategySignal::NoSignal),
            "$8.10 held elsewhere plus $2.10 here is over $10");
        let status = serde_json::to_string(&viper_status::snapshot(Some(asset))).unwrap();
        assert!(status.contains("exposure cap"), "{status}");
    }

    /// Every starter runs against a real context without a script error: a
    /// template that only compiles could still fail on its first tick.
    #[tokio::test]
    async fn every_template_runs_against_a_real_context() {
        for t in crate::vipers::scripted::templates::all() {
            let asset = format!("scripttpl{}", t.id.replace('_', ""));
            let _pool = pool(&asset).await;
            let mut m: crate::vipers::scripted::manifest::Manifest = serde_json::from_value(t.manifest.clone()).unwrap();
            m.name = asset.clone();
            store::insert_hidden_for_test(m, t.source);
            let strat = ScriptedStrategy::new(&asset);
            let mut c = ctx(&asset, DynamicConfig::default());
            c.market.market_close_time = Some(Utc::now() + chrono::Duration::minutes(5));
            strat.evaluate_entry(&c).await.unwrap();
            strat.evaluate_exit(&c).await.unwrap();
            let status = serde_json::to_string(&viper_status::snapshot(Some(&asset))).unwrap();
            assert!(!status.contains("script error"), "{}: {status}", t.id);
        }
    }

    /// [B95] A feed that has not printed yet (oracle 0 after a start, or while
    /// it is down) is no price at all: every template holds, and the context
    /// says so, instead of reading a level of $0.
    #[tokio::test]
    async fn an_oracle_of_zero_is_no_price_and_every_template_holds() {
        for t in crate::vipers::scripted::templates::all() {
            let asset = format!("scriptzero{}", t.id.replace('_', ""));
            let pool = pool(&asset).await;
            let mut m: crate::vipers::scripted::manifest::Manifest = serde_json::from_value(t.manifest.clone()).unwrap();
            m.name = asset.clone();
            store::insert_hidden_for_test(m.clone(), t.source);
            let strat = ScriptedStrategy::new(&asset);
            let mut c = ctx(&asset, DynamicConfig::default());
            c.market.market_close_time = Some(Utc::now() + chrono::Duration::minutes(5));
            c.snapshot.oracle_price = Decimal::ZERO;
            c.snapshot.velocity = Decimal::ZERO;
            let map = crate::vipers::scripted::context::build(&c, &m.resolve_params(&Default::default()), &crate::vipers::scripted::context::Account { ghost: false, exposure: Decimal::ZERO, max_exposure: dec!(5), day_pnl: Decimal::ZERO });
            assert!(map["oracle"].is_unit(), "{}: oracle must be ()", t.id);
            assert!(map["velocity"].is_unit(), "{}: velocity must be ()", t.id);
            assert_eq!(map["has_price_feed"].as_bool().ok(), Some(false), "{}", t.id);
            strat.evaluate_entry(&c).await.unwrap();
            let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM script_paper").fetch_one(&pool).await.unwrap();
            assert_eq!(n, 0, "{}: no paper entry on a $0 oracle", t.id);
        }
        // A script that compares without checking for `()`: the comparison is
        // false, its else branch buys, and only the engine's refusal stops it.
        let asset = "scriptzerocmp";
        let pool = pool(asset).await;
        let m: crate::vipers::scripted::manifest::Manifest = serde_json::from_value(serde_json::json!({
            "name": asset, "display_name": "Cmp", "classes": ["crypto"], "max_exposure_usdc": 5
        })).unwrap();
        store::insert_hidden_for_test(m, "fn entry(ctx) { if ctx.oracle > ctx.strike { #{ hold: \"above\" } } \
            else { #{ buy: \"no\", price: ctx.no.ask, shares: 5 } } } fn exit(ctx, pos) { () }");
        let strat = ScriptedStrategy::new(asset);
        let mut c = ctx(asset, DynamicConfig::default());
        c.market.market_close_time = Some(Utc::now() + chrono::Duration::minutes(5));
        c.snapshot.oracle_price = Decimal::ZERO;
        strat.evaluate_entry(&c).await.unwrap();
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM script_paper").fetch_one(&pool).await.unwrap();
        assert_eq!(n, 0, "the engine refuses the entry while the feed has no price");
        let status = serde_json::to_string(&viper_status::snapshot(Some(asset))).unwrap();
        assert!(status.contains("oracle price unknown"), "{status}");
    }

    /// The momentum starter in real units: `ctx.velocity` is a dollar move, the
    /// threshold a fraction of the price, so at $100,100 and 0.001 the line is
    /// about $100. A $30 move holds; a $120 move buys the side it points to.
    #[tokio::test]
    async fn the_momentum_starter_reads_velocity_in_dollars() {
        let t = crate::vipers::scripted::templates::all().into_iter().find(|t| t.id == "momentum").unwrap();
        let asset = "scriptmomunits";
        let pool = pool(asset).await;
        let mut m: crate::vipers::scripted::manifest::Manifest = serde_json::from_value(t.manifest.clone()).unwrap();
        m.name = asset.into();
        store::insert_hidden_for_test(m, t.source);
        let strat = ScriptedStrategy::new(asset);

        let mut c = ctx(asset, DynamicConfig::default());
        c.snapshot.velocity = dec!(30);
        strat.evaluate_entry(&c).await.unwrap();
        assert!(paper::open_rows(&pool, &c.squadron_id, asset).await.is_empty(), "$30 in five seconds is noise at $100,100");
        let status = serde_json::to_string(&viper_status::snapshot(Some(asset))).unwrap();
        assert!(status.contains("inside ±$100.10"), "{status}");

        c.snapshot.velocity = dec!(-120);
        strat.evaluate_entry(&c).await.unwrap();
        let rows = paper::open_rows(&pool, &c.squadron_id, asset).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].side, "NO", "a fall buys NO");
    }

    #[test]
    fn only_the_live_switch_reaches_the_order_path_and_the_instance_decides_real() {
        let mut c = CustomViperConfig::default();
        assert_eq!(mode(&c, false), Mode::Paper, "off by default, even on a live instance");
        c.live = true;
        assert_eq!(mode(&c, true), Mode::Orders { ghost: true }, "a simulated instance simulates");
        assert_eq!(mode(&c, false), Mode::Orders { ghost: false }, "real only when both are on");
    }
}
