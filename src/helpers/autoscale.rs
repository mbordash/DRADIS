//! [E91] Capital auto-scaler.
//!
//! Scales each viper's ENTRY orders by its own recent results in its own
//! squadron: a viper that has been making money on the capital it risked trades
//! larger, up to a ceiling; one that has been losing trades smaller, down to a
//! floor. Exits are never touched (they sell what is held).
//!
//! ## Signal: return on capital, not win rate
//!
//! On a binary contract a win rate means nothing without the price paid: a 70%
//! win rate bought at 0.80 loses money, and Anchor's take-profit exits raise its
//! win rate without saying anything about its edge. The scaler reads realized
//! P&L over the dollars risked (`entry_price × shares`) across the viper's last
//! `autoscale_lookback_trades` closed trades.
//!
//! ## Scope: viper × squadron
//!
//! A squadron flies one market class and, for crypto, one underlying, and every
//! trade row is filed under its class and underlying (`TradeScope`). So the
//! record for "Anchor on btc-open" is Anchor's rows filed `crypto`/`btc`. Rows
//! the reconcilers book with no class (chain adoption, retrospective
//! settlement) are left out rather than guessed. Ghost and real ledgers are
//! kept apart: a simulating instance scales on its simulated record, a live one
//! on real money only.
//!
//! ## Limits
//!
//! - Nothing moves before `autoscale_min_trades` closed trades.
//! - The multiplier runs linearly from 1× at zero return to the ceiling
//!   (`autoscale_max_multiplier`) at `+autoscale_full_scale_return`, and to the
//!   floor (`autoscale_min_multiplier`) at the same return negative.
//! - Scaling up never takes one order past `autoscale_max_order_usdc`, never past
//!   the viper's own max exposure (what it already holds counts), and never
//!   happens while the daily drawdown breaker is tripped.
//! - Scaling down never goes below the venue's minimum order size.
//!
//! Settings are instance-wide (the CAG level): read from the global config row,
//! applied to every squadron beneath it. Off by default.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::{Row, SqlitePool};
use tracing::{debug, info, warn};

use crate::helpers::dynamic_config::DynamicConfig;
use crate::orchestrator::StrategyContext;
use crate::state::{OrderParams, StrategySignal};

pub const DEFAULT_ENABLED: bool = false;
pub const DEFAULT_MIN_TRADES: i64 = 20;
pub const DEFAULT_LOOKBACK_TRADES: i64 = 30;
pub const DEFAULT_FULL_SCALE_RETURN: Decimal = dec!(0.10);
pub const DEFAULT_MAX_MULTIPLIER: Decimal = dec!(2.0);
pub const DEFAULT_MIN_MULTIPLIER: Decimal = dec!(0.5);
pub const DEFAULT_MAX_ORDER_USDC: Decimal = dec!(25);

/// How often the cached records are rebuilt from the ledger.
pub const REFRESH_SECS: u64 = 60;
/// Most recent rows read per refresh: far beyond any lookback, bounds the read.
const MAX_ROWS_READ: i64 = 20_000;

/// Which record a squadron's viper scales on.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RecordKey {
    pub strategy: String,
    pub market_class: String,
    /// `None` for a class with no underlying (sports, politics).
    pub underlying: Option<String>,
    pub ghost: bool,
}

impl RecordKey {
    /// Normalised key. Only crypto carries an underlying: Kalshi and US file a
    /// sports or politics row with none, intl with the squadron's asset key
    /// (Fable review 2026-11-05), so for those classes the class alone is the
    /// scope on every venue.
    pub fn new(strategy: impl Into<String>, market_class: &str, underlying: Option<&str>, ghost: bool) -> Self {
        let class = market_class.to_lowercase();
        let underlying = (class == "crypto")
            .then(|| underlying.map(|u| u.to_lowercase()).filter(|u| !u.is_empty()))
            .flatten();
        Self { strategy: strategy.into(), market_class: class, underlying, ghost }
    }
}

/// A viper's last `lookback` closed trades in one squadron's scope.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Record {
    pub trades: i64,
    pub pnl: Decimal,
    /// Dollars risked: Σ entry_price × shares.
    pub risked: Decimal,
}

impl Record {
    /// Realized P&L per dollar risked, or `None` with nothing risked.
    pub fn return_on_capital(&self) -> Option<Decimal> {
        (self.risked > Decimal::ZERO).then(|| self.pnl / self.risked)
    }
}

/// The scaler's settings, read off the global row.
#[derive(Clone, Debug, PartialEq)]
pub struct Settings {
    pub enabled: bool,
    pub min_trades: i64,
    pub lookback: i64,
    pub full_scale_return: Decimal,
    pub max_multiplier: Decimal,
    pub min_multiplier: Decimal,
    pub max_order_usdc: Decimal,
}

impl Settings {
    pub fn from_config(c: &DynamicConfig) -> Self {
        Self {
            enabled: c.autoscale_enabled,
            min_trades: c.autoscale_min_trades.max(1),
            lookback: c.autoscale_lookback_trades.max(1),
            full_scale_return: c.autoscale_full_scale_return,
            // A ceiling below 1× or a floor above it would invert the policy.
            max_multiplier: c.autoscale_max_multiplier.max(Decimal::ONE),
            min_multiplier: c.autoscale_min_multiplier.clamp(Decimal::ZERO, Decimal::ONE),
            max_order_usdc: c.autoscale_max_order_usdc,
        }
    }

    /// The live global settings, or `None` before the global row is published.
    pub fn current() -> Option<Self> {
        crate::helpers::dynamic_config::global_config_tx().map(|tx| Self::from_config(&tx.borrow()))
    }
}

/// The size multiplier a record earns. Pure.
///
/// 1× until the record holds `min_trades`; otherwise linear in the return on
/// capital, reaching the ceiling at `+full_scale_return` and the floor at the
/// same return negative, clamped beyond.
pub fn multiplier(rec: &Record, s: &Settings) -> Decimal {
    if rec.trades < s.min_trades { return Decimal::ONE; }
    let Some(r) = rec.return_on_capital() else { return Decimal::ONE };
    if s.full_scale_return <= Decimal::ZERO { return Decimal::ONE; }
    let reach = (r.abs() / s.full_scale_return).min(Decimal::ONE);
    if r >= Decimal::ZERO {
        Decimal::ONE + (s.max_multiplier - Decimal::ONE) * reach
    } else {
        Decimal::ONE - (Decimal::ONE - s.min_multiplier) * reach
    }
}

/// The factor actually applied to one entry, after every limit. Pure.
///
/// `notional` is the entry's total cost (both legs of a pair), `held` what the
/// viper already holds in this squadron at cost, `max_exposure` its runtime cap
/// (`None` = unknown, so no growth), `max_order_usdc` the instance limit per
/// scaled order (zero = no growth at all), `min_factor` the smallest factor that
/// keeps every leg at the venue's minimum order size.
pub fn applied_factor(
    mult: Decimal,
    notional: Decimal,
    held: Decimal,
    max_exposure: Option<Decimal>,
    max_order_usdc: Decimal,
    drawdown_tripped: bool,
    min_factor: Decimal,
) -> Decimal {
    if notional <= Decimal::ZERO { return Decimal::ONE; }
    if mult > Decimal::ONE {
        if drawdown_tripped { return Decimal::ONE; }
        let Some(cap) = max_exposure else { return Decimal::ONE };
        let f = mult
            .min(max_order_usdc.max(Decimal::ZERO) / notional)
            .min((cap - held) / notional);
        // The limits only stop growth; they never shrink the viper's own order.
        f.max(Decimal::ONE)
    } else if mult < Decimal::ONE {
        // Shrinking stops at the venue minimum, and never grows the order.
        mult.max(min_factor).min(Decimal::ONE)
    } else {
        Decimal::ONE
    }
}

// ── Cached records ───────────────────────────────────────────────────────────

fn records() -> &'static RwLock<HashMap<RecordKey, Record>> {
    static R: OnceLock<RwLock<HashMap<RecordKey, Record>>> = OnceLock::new();
    R.get_or_init(|| RwLock::new(HashMap::new()))
}

/// The factor last applied to an entry, and the record scope it came from.
#[derive(Clone, Debug)]
pub struct Applied {
    pub factor: Decimal,
    pub key: RecordKey,
}

/// Last factor applied per (squadron, strategy), for the API and the cards.
fn applied() -> &'static RwLock<HashMap<(String, String), Applied>> {
    static A: OnceLock<RwLock<HashMap<(String, String), Applied>>> = OnceLock::new();
    A.get_or_init(|| RwLock::new(HashMap::new()))
}

#[cfg(test)]
pub fn set_record_for_test(key: RecordKey, rec: Record) {
    records().write().unwrap().insert(key, rec);
}

pub fn record_for(key: &RecordKey) -> Record {
    records().read().map(|m| m.get(key).cloned().unwrap_or_default()).unwrap_or_default()
}

/// Every cached record, for the API.
pub fn all_records() -> Vec<(RecordKey, Record)> {
    let mut v: Vec<_> = records().read().map(|m| m.iter().map(|(k, r)| (k.clone(), r.clone())).collect()).unwrap_or_default();
    v.sort_by(|a, b| a.0.cmp(&b.0));
    v
}

/// The last factor applied to an entry, per (squadron, strategy).
pub fn last_applied() -> Vec<(String, String, Applied)> {
    let mut v: Vec<_> = applied().read()
        .map(|m| m.iter().map(|((sq, st), a)| (sq.clone(), st.clone(), a.clone())).collect())
        .unwrap_or_default();
    v.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    v
}

/// Fold ledger rows, newest first, into each key's last `lookback` trades. Pure.
pub fn fold_rows(
    rows: impl IntoIterator<Item = (RecordKey, Decimal, Decimal)>,
    lookback: i64,
) -> HashMap<RecordKey, Record> {
    let mut out: HashMap<RecordKey, Record> = HashMap::new();
    for (key, pnl, risked) in rows {
        let rec = out.entry(key).or_default();
        if rec.trades >= lookback { continue; }
        rec.trades += 1;
        rec.pnl += pnl;
        rec.risked += risked;
    }
    out
}

fn dec_of(s: Option<String>) -> Option<Decimal> {
    s.and_then(|v| v.trim().parse::<Decimal>().ok())
}

/// Read one pool's classified trades, newest first.
async fn read_rows(pool: &SqlitePool) -> Vec<(String, RecordKey, Decimal, Decimal)> {
    let rows = match sqlx::query(
        "SELECT strategy, market_class, underlying, COALESCE(ghost, 0),
                CAST(pnl AS TEXT), CAST(entry_price AS TEXT), CAST(shares AS TEXT), ts
         FROM trades
         WHERE market_class IS NOT NULL AND pnl IS NOT NULL
         ORDER BY ts DESC, id DESC
         LIMIT ?",
    )
    .bind(MAX_ROWS_READ)
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => { warn!("⚠️ Auto-scaler: trade read failed: {e}"); return Vec::new(); }
    };
    rows.into_iter().filter_map(|r| {
        let strategy: String = r.try_get(0).ok()?;
        let market_class: String = r.try_get(1).ok()?;
        let underlying: Option<String> = r.try_get(2).ok()?;
        let ghost: i64 = r.try_get(3).unwrap_or(0);
        let pnl = dec_of(r.try_get(4).ok()?)?;
        let price = dec_of(r.try_get(5).ok()?)?;
        let shares = dec_of(r.try_get(6).ok()?)?;
        let ts: String = r.try_get(7).ok()?;
        let risked = (price * shares).abs();
        let key = RecordKey::new(strategy, &market_class, underlying.as_deref(), ghost != 0);
        Some((ts, key, pnl, risked))
    }).collect()
}

/// Rebuild every record from every ledger.
pub async fn refresh(lookback: i64) {
    let mut rows = Vec::new();
    for pool in crate::helpers::db::all_pools() {
        rows.extend(read_rows(&pool).await);
    }
    // One key's rows can sit in several pools (intl keeps a pool per asset), so
    // order them all newest first before the lookback takes each key's latest.
    // RFC 3339 UTC timestamps compare as strings.
    rows.sort_by(|a, b| b.0.cmp(&a.0));
    let folded = fold_rows(rows.into_iter().map(|(_, k, p, r)| (k, p, r)), lookback);
    if let Ok(mut m) = records().write() { *m = folded; }
}

/// Background refresher: rebuilds the records every `REFRESH_SECS`.
pub fn spawn_refresher() {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(REFRESH_SECS));
        loop {
            tick.tick().await;
            let lookback = Settings::current().map(|s| s.lookback).unwrap_or(DEFAULT_LOOKBACK_TRADES);
            refresh(lookback).await;
        }
    });
}

// ── The hook ─────────────────────────────────────────────────────────────────

/// The record key for one viper in the squadron this context belongs to.
pub fn key_for(ctx: &StrategyContext, strategy: &str, ghost: bool) -> Option<RecordKey> {
    let class = ctx.market_class.as_deref()?;
    Some(RecordKey::new(strategy, class, Some(&ctx.crypto_filter), ghost))
}

/// The viper's runtime exposure cap, as its own entry gate reads it.
///
/// Fable review 2026-11-05: `Strategy::max_exposure()` returns each viper's
/// compile-time `config::*_MAX_EXPOSURE_USDC`, while the vipers gate on the
/// operator's `dc.*_max_exposure_usdc`. Scaling against the constant could
/// double an order past a cap the operator had lowered; FairValue and GBoost
/// size to exactly the room left under the runtime cap. A viper not listed here
/// has no known cap and is never scaled UP (shrinking is always safe).
pub fn viper_cap(strategy: &str, dc: &DynamicConfig) -> Option<Decimal> {
    Some(match strategy {
        "AnchorStrategy"        => dc.anchor_max_exposure_usdc,
        "ArbitrageStrategy"     => dc.arbitrage_max_exposure_usdc,
        "BasisStrategy"         => dc.basis_max_exposure_usdc,
        "BooklineStrategy"      => dc.bookline_max_exposure_usdc,
        "ConvergenceStrategy"   => dc.convergence_max_exposure_usdc,
        "FairValueStrategy"     => dc.fairvalue_max_exposure_usdc,
        "GboostStrategy"        => dc.gboost_max_exposure_usdc,
        "MakerStrategy"         => dc.maker_max_exposure_usdc,
        "MomentumStrategy"      => dc.momentum_max_exposure_usdc,
        "TimeDecayStrategy"     => dc.time_decay_max_exposure_usdc,
        "TrendReversalStrategy" => dc.trendcapture_max_exposure_usdc,
        _ => return None,
    })
}

/// Vipers the scaler may resize: every one but Helm, whose size is the
/// operator's own decision on each intent.
pub fn scalable(strategy: &str) -> bool {
    strategy != "HelmStrategy"
}

fn scale_leg(p: &mut OrderParams, factor: Decimal) {
    p.shares = (p.shares * factor).round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToZero);
}

/// Scale one entry signal by its viper's record. Anything that is not an entry
/// passes through unchanged, as does every entry while the scaler is off.
pub async fn scale_entry(ctx: &StrategyContext, strategy: &str, signal: StrategySignal) -> StrategySignal {
    scale_entry_with(ctx, strategy, signal, Settings::current()).await
}

/// `scale_entry` with the settings passed in (the live hook passes the global
/// row's; tests pass their own).
pub async fn scale_entry_with(
    ctx: &StrategyContext,
    strategy: &str,
    signal: StrategySignal,
    settings: Option<Settings>,
) -> StrategySignal {
    let StrategySignal::Entry { mut params, mut pair_params } = signal else { return signal };
    let unchanged = |params, pair_params| StrategySignal::Entry { params, pair_params };
    // Helm is the operator's own conviction, sized by the operator: never resized.
    if !scalable(strategy) { return unchanged(params, pair_params); }
    let Some(s) = settings else { return unchanged(params, pair_params) };
    if !s.enabled { return unchanged(params, pair_params); }
    // The same test the order paths gate on (`scope.ghost` in the venue loops).
    let ghost = crate::config::GHOST_MODE || ctx.dynamic_config.ghost_mode;
    let Some(key) = key_for(ctx, strategy, ghost) else { return unchanged(params, pair_params) };
    let rec = record_for(&key);
    let mult = multiplier(&rec, &s);
    if mult == Decimal::ONE { return unchanged(params, pair_params); }

    let notional = params.price * params.shares
        + pair_params.as_ref().map_or(Decimal::ZERO, |p| p.price * p.shares);
    let held: Decimal = {
        let map = ctx.positions.lock().await;
        map.iter()
            .filter(|(k, _)| k.squadron == ctx.squadron_id && k.strategy == strategy)
            .map(|(_, p)| p.shares * p.avg_entry)
            .sum()
    };
    let min_shares = crate::venues::min_order_shares();
    let smallest_leg = pair_params.as_ref().map_or(params.shares, |p| p.shares.min(params.shares));
    let min_factor = if smallest_leg > Decimal::ZERO { min_shares / smallest_leg } else { Decimal::ONE };
    // The breaker's own condition (the day's loss, B90), without the "Risk
    // Reject" log: the scaler only withholds growth, it rejects nothing.
    let drawdown = crate::vipers::drawdown_breached(ctx);
    let cap = viper_cap(strategy, &ctx.dynamic_config);
    let factor = applied_factor(mult, notional, held, cap, s.max_order_usdc, drawdown, min_factor);

    if let Ok(mut m) = applied().write() {
        m.insert((ctx.squadron_id.clone(), strategy.to_string()), Applied { factor, key: key.clone() });
    }
    if factor == Decimal::ONE {
        debug!("📐 Auto-scaler [{}] {strategy}: ×{mult:.2} earned, held at ×1 by its limits", ctx.squadron_id);
        return unchanged(params, pair_params);
    }
    let before = params.shares;
    scale_leg(&mut params, factor);
    if let Some(p) = pair_params.as_mut() { scale_leg(p, factor); }
    info!(
        "📐 Auto-scaler [{}] {strategy}: ×{factor:.2} (earned ×{mult:.2}; {:+.1}% on ${:.2} risked over {} trades) — {} → {} shares",
        ctx.squadron_id,
        rec.return_on_capital().unwrap_or_default() * dec!(100),
        rec.risked, rec.trades, before, params.shares,
    );
    StrategySignal::Entry { params, pair_params }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s() -> Settings {
        Settings {
            enabled: true, min_trades: 20, lookback: 30,
            full_scale_return: dec!(0.10), max_multiplier: dec!(2), min_multiplier: dec!(0.5),
            max_order_usdc: dec!(25),
        }
    }
    fn rec(trades: i64, pnl: Decimal, risked: Decimal) -> Record { Record { trades, pnl, risked } }

    #[test]
    fn nothing_moves_before_the_minimum_record() {
        assert_eq!(multiplier(&rec(19, dec!(50), dec!(100)), &s()), Decimal::ONE);
        assert_eq!(multiplier(&rec(20, dec!(0), dec!(0)), &s()), Decimal::ONE, "nothing risked");
    }

    #[test]
    fn the_multiplier_is_linear_in_return_on_capital_and_clamped() {
        assert_eq!(multiplier(&rec(20, dec!(0), dec!(100)), &s()), dec!(1));
        assert_eq!(multiplier(&rec(20, dec!(5), dec!(100)), &s()), dec!(1.5), "+5% is half way to 2×");
        assert_eq!(multiplier(&rec(20, dec!(10), dec!(100)), &s()), dec!(2));
        assert_eq!(multiplier(&rec(20, dec!(40), dec!(100)), &s()), dec!(2), "clamped at the ceiling");
        assert_eq!(multiplier(&rec(20, dec!(-5), dec!(100)), &s()), dec!(0.75));
        assert_eq!(multiplier(&rec(20, dec!(-90), dec!(100)), &s()), dec!(0.5), "clamped at the floor");
    }

    /// Anchor on btc-open, production 2026-11-05: +$19.31 on $134.92 over 75
    /// trades is +14.3%, past full scale, so a 2× ceiling.
    #[test]
    fn anchor_on_production_would_earn_the_ceiling() {
        assert_eq!(multiplier(&rec(30, dec!(19.31), dec!(134.92)), &s()), dec!(2));
    }

    #[test]
    fn scaling_up_stops_at_the_order_cap_the_exposure_cap_and_the_breaker() {
        let big = Some(dec!(1000));
        // $10 order, 2× earned, $25 order cap: 2× fits.
        assert_eq!(applied_factor(dec!(2), dec!(10), dec!(0), big, dec!(25), false, dec!(0.1)), dec!(2));
        // $20 order: the cap allows 1.25×.
        assert_eq!(applied_factor(dec!(2), dec!(20), dec!(0), big, dec!(25), false, dec!(0.1)), dec!(1.25));
        // $30 order already past the cap: unchanged, never shrunk by a growth limit.
        assert_eq!(applied_factor(dec!(2), dec!(30), dec!(0), big, dec!(25), false, dec!(0.1)), dec!(1));
        // Viper cap $24 with $8 held: room for $16, so 1.6× on a $10 order.
        assert_eq!(applied_factor(dec!(2), dec!(10), dec!(8), Some(dec!(24)), dec!(100), false, dec!(0.1)), dec!(1.6));
        // Breaker tripped: no growth.
        assert_eq!(applied_factor(dec!(2), dec!(10), dec!(0), big, dec!(25), true, dec!(0.1)), dec!(1));
    }

    /// Fable review: a $0 order cap meant "no limit"; it means no growth.
    /// And a viper with no known runtime cap never grows.
    #[test]
    fn a_zero_order_cap_or_an_unknown_viper_cap_means_no_growth() {
        assert_eq!(applied_factor(dec!(2), dec!(10), dec!(0), Some(dec!(1000)), dec!(0), false, dec!(0.1)), dec!(1));
        assert_eq!(applied_factor(dec!(2), dec!(10), dec!(0), None, dec!(25), false, dec!(0.1)), dec!(1));
        // Shrinking needs no cap at all.
        assert_eq!(applied_factor(dec!(0.5), dec!(10), dec!(0), None, dec!(0), false, dec!(0.1)), dec!(0.5));
    }

    /// Fable review: the cap is the operator's runtime setting, not the
    /// compile-time constant `Strategy::max_exposure()` returns.
    #[test]
    fn the_viper_cap_is_the_runtime_setting() {
        let mut dc = DynamicConfig::default();
        dc.anchor_max_exposure_usdc = dec!(7);
        dc.fairvalue_max_exposure_usdc = dec!(3);
        dc.trendcapture_max_exposure_usdc = dec!(9);
        assert_eq!(viper_cap("AnchorStrategy", &dc), Some(dec!(7)));
        assert_eq!(viper_cap("FairValueStrategy", &dc), Some(dec!(3)));
        assert_eq!(viper_cap("TrendReversalStrategy", &dc), Some(dec!(9)));
        assert_eq!(viper_cap("SomeNewStrategy", &dc), None);
    }

    /// Fable review: intl files a sports row with the squadron's asset key,
    /// Kalshi/US with none; both must land on the same record.
    #[test]
    fn only_crypto_keys_carry_an_underlying() {
        assert_eq!(RecordKey::new("MakerStrategy", "Sports", Some("sports"), false),
                   RecordKey::new("MakerStrategy", "sports", None, false));
        assert_eq!(RecordKey::new("AnchorStrategy", "crypto", Some("BTC"), false).underlying.as_deref(), Some("btc"));
        assert_ne!(RecordKey::new("AnchorStrategy", "crypto", Some("btc"), false),
                   RecordKey::new("AnchorStrategy", "crypto", Some("eth"), false));
    }

    #[test]
    fn the_fold_keeps_each_keys_newest_lookback_trades() {
        let k = |st: &str| RecordKey { strategy: st.into(), market_class: "crypto".into(), underlying: Some("btc".into()), ghost: false };
        let rows = vec![
            (k("A"), dec!(1), dec!(10)), (k("B"), dec!(-1), dec!(10)),
            (k("A"), dec!(2), dec!(10)), (k("A"), dec!(100), dec!(10)), // third A: past lookback 2
        ];
        let m = fold_rows(rows, 2);
        assert_eq!(m[&k("A")], rec(2, dec!(3), dec!(20)));
        assert_eq!(m[&k("B")], rec(1, dec!(-1), dec!(10)));
    }

    // ── The hook, end to end on a real context ───────────────────────────────

    use crate::state::{MarketConfig, MarketSnapshot, PositionMap};
    use crate::venues::core::MarketId;
    use std::sync::Arc;

    fn ctx(squadron: &str) -> StrategyContext { ctx_on(squadron, "btc", DynamicConfig::default()) }

    /// A context on its own underlying, so each test's records stay its own
    /// (the record cache is process-wide).
    fn ctx_on(squadron: &str, underlying: &str, dc: DynamicConfig) -> StrategyContext {
        StrategyContext {
            market_class: Some("crypto".to_string()),
            has_price_feed: true,
            squadron_id: squadron.to_string(),
            market: MarketConfig {
                yes_token: MarketId::new("as-yes"), no_token: MarketId::new("as-no"),
                market_name: "Bitcoin price test".to_string(),
                market_close_time: None, strike_price: None, window_secs: None, is_neg_risk: false,
                condition_id: "0xas".to_string(), yes_fee_bps: 0, no_fee_bps: 0,
            },
            snapshot: MarketSnapshot {
                yes_bid: dec!(0.49), yes_bid_depth: dec!(100), yes_ask: dec!(0.50), yes_ask_depth: dec!(100),
                no_bid: dec!(0.49), no_bid_depth: dec!(100), no_ask: dec!(0.50), no_ask_depth: dec!(100),
                yes_bid_depth_total: dec!(100), yes_ask_depth_total: dec!(100),
                no_bid_depth_total: dec!(100), no_ask_depth_total: dec!(100),
                oracle_price: dec!(0), velocity: dec!(0), velocity_1s: dec!(0), acceleration: dec!(0),
                funding_rate: dec!(0), oracle_drift_60m: dec!(0), oracle_drift_10m: dec!(0), hist_vol: dec!(0),
                institutional_pulse: dec!(0), tide_coherence: dec!(0), tradfi_velocity: dec!(0),
                macro_coherence: dec!(0), vix_proxy: dec!(0), vix_velocity: dec!(0),
                oi_delta_pct: dec!(0), cvd_ratio: dec!(1), secs_to_expiry: 1500, timestamp: chrono::Utc::now(),
            },
            positions: Arc::new(tokio::sync::Mutex::new(PositionMap::new())),
            session_pnl: dec!(0), starting_collateral: dec!(1000), available_collateral: dec!(1000),
            crypto_filter: underlying.to_string(),
            market_started_at: chrono::Utc::now(),
            maker_market: None, maker_snapshot: None,
            dynamic_config: Arc::new(dc),
            arb_market_lockouts: None,
            sports: None,
        }
    }

    fn order(token: &str, price: Decimal, shares: Decimal) -> OrderParams {
        OrderParams {
            token_id: MarketId::new(token), price, shares, fee_bps: 0, is_neg_risk: false,
            market_name: "Bitcoin price test".to_string(), condition_id: "0xas".to_string(),
            order_type: crate::venues::core::TimeInForce::Gtc, post_only: true, ghost_mode: true,
        }
    }

    /// Seed a record under a viper name only this test uses, in the ledger the
    /// context's mode reads.
    fn seed(strategy: &str, c: &StrategyContext, rec: Record) {
        let key = key_for(c, strategy, crate::config::GHOST_MODE || c.dynamic_config.ghost_mode).unwrap();
        set_record_for_test(key, rec);
    }

    fn shares_of(sig: &StrategySignal) -> (Decimal, Option<Decimal>) {
        match sig {
            StrategySignal::Entry { params, pair_params } => (params.shares, pair_params.as_ref().map(|p| p.shares)),
            _ => panic!("not an entry"),
        }
    }

    fn roomy() -> DynamicConfig {
        let mut dc = DynamicConfig::default();
        dc.anchor_max_exposure_usdc = dec!(1000);
        dc.time_decay_max_exposure_usdc = dec!(1000);
        dc
    }

    #[tokio::test]
    async fn a_winning_viper_s_entry_and_both_pair_legs_grow() {
        let c = ctx_on("as-sq-1", "t1", roomy());
        seed("AnchorStrategy", &c, rec(30, dec!(10), dec!(100))); // +10%: 2×
        let single = StrategySignal::Entry { params: order("as-yes", dec!(0.50), dec!(10)), pair_params: None };
        let out = scale_entry_with(&c, "AnchorStrategy", single, Some(s())).await;
        assert_eq!(shares_of(&out), (dec!(20), None), "$5 order, 2× fits under the $25 cap");

        seed("TimeDecayStrategy", &c, rec(30, dec!(10), dec!(100)));
        let pair = StrategySignal::Entry {
            params: order("as-yes", dec!(0.50), dec!(10)),
            pair_params: Some(order("as-no", dec!(0.49), dec!(10))),
        };
        let out = scale_entry_with(&c, "TimeDecayStrategy", pair, Some(s())).await;
        assert_eq!(shares_of(&out), (dec!(20), Some(dec!(20))), "both legs by one factor");
    }

    #[tokio::test]
    async fn off_too_few_trades_exits_helm_and_unknown_vipers_do_not_grow() {
        let c = ctx_on("as-sq-2", "t2", roomy());
        let entry = || StrategySignal::Entry { params: order("as-yes", dec!(0.50), dec!(10)), pair_params: None };
        seed("AnchorStrategy", &c, rec(5, dec!(10), dec!(100)));
        let off = Settings { enabled: false, ..s() };
        assert_eq!(shares_of(&scale_entry_with(&c, "AnchorStrategy", entry(), Some(off)).await).0, dec!(10));
        assert_eq!(shares_of(&scale_entry_with(&c, "AnchorStrategy", entry(), Some(s())).await).0, dec!(10), "5 trades < 20");
        seed("HelmStrategy", &c, rec(30, dec!(10), dec!(100)));
        assert_eq!(shares_of(&scale_entry_with(&c, "HelmStrategy", entry(), Some(s())).await).0, dec!(10));
        seed("SomeNewStrategy", &c, rec(30, dec!(10), dec!(100)));
        assert_eq!(shares_of(&scale_entry_with(&c, "SomeNewStrategy", entry(), Some(s())).await).0, dec!(10), "no known cap");
        let exit = StrategySignal::Exit { params: order("as-yes", dec!(0.50), dec!(10)), reason: "x".into(), exit_pair: false };
        assert!(matches!(scale_entry_with(&c, "AnchorStrategy", exit, Some(s())).await, StrategySignal::Exit { .. }));
    }

    #[tokio::test]
    async fn a_losing_viper_shrinks_and_the_runtime_cap_counts_what_it_holds() {
        let c = ctx_on("as-sq-3", "t3", roomy());
        seed("AnchorStrategy", &c, rec(30, dec!(-10), dec!(100))); // −10%: 0.5×
        let e = StrategySignal::Entry { params: order("as-yes", dec!(0.50), dec!(20)), pair_params: None };
        assert_eq!(shares_of(&scale_entry_with(&c, "AnchorStrategy", e, Some(s())).await).0, dec!(10));

        // The operator's runtime Anchor cap: $12, with $8 already held.
        let mut dc = DynamicConfig::default();
        dc.anchor_max_exposure_usdc = dec!(12);
        let c = ctx_on("as-sq-4", "t4", dc);
        seed("AnchorStrategy", &c, rec(30, dec!(10), dec!(100))); // 2× earned
        c.positions.lock().await.insert(
            crate::state::PositionKey::new("as-sq-4", "AnchorStrategy", MarketId::new("held")),
            crate::state::Position {
                shares: dec!(16), avg_entry: dec!(0.50), opened_at: chrono::Utc::now(), close_time: None,
                market_name: "held".into(), pair_token_id: MarketId::new("held"),
                fill_confirmed_at: None, paired_leg_token_id: None, entry_fee: Decimal::ZERO,
            },
        );
        // Room for $4 on a $2 order: exactly 2×. On a $4 order: 1×.
        let e = |sh| StrategySignal::Entry { params: order("as-yes", dec!(0.50), sh), pair_params: None };
        assert_eq!(shares_of(&scale_entry_with(&c, "AnchorStrategy", e(dec!(4)), Some(s())).await).0, dec!(8));
        assert_eq!(shares_of(&scale_entry_with(&c, "AnchorStrategy", e(dec!(8)), Some(s())).await).0, dec!(8));
    }

    /// The ledger read on a real schema: classified rows fold per viper and
    /// scope, newest first; unclassified reconciler rows and the other ledger
    /// stay out.
    #[tokio::test]
    async fn the_ledger_read_keys_rows_by_viper_scope_and_mode() {
        use crate::helpers::db;
        use crate::state::TradeScope;
        let pool = sqlx::sqlite::SqlitePoolOptions::new().max_connections(1)
            .connect("sqlite::memory:").await.unwrap();
        db::init_schema(&pool).await.unwrap();
        db::run_migrations(&pool).await;
        let btc = TradeScope::new("test", "kalshi", Some("crypto".into()), Some("btc".into()));
        let now = chrono::Utc::now();
        // Oldest first in time: −$1, +$3, +$2 on $5 each (0.50 × 10).
        for (i, pnl) in [dec!(-1), dec!(3), dec!(2)].into_iter().enumerate() {
            db::record_trade_db(&pool, &btc, Decimal::ZERO, "AnchorStrategy", &format!("M{i}"), "NO",
                dec!(0.50), dec!(0.80), dec!(10), pnl, "settled",
                Some(now - chrono::Duration::minutes(10 - i as i64))).await;
        }
        // A reconciler row with no class, and a simulated row: neither counts here.
        db::record_trade_db(&pool, &TradeScope::shard_only("test"), Decimal::ZERO, "AnchorStrategy", "MX", "NO",
            dec!(0.50), dec!(1), dec!(10), dec!(50), "chain", Some(now)).await;
        let ghost = TradeScope { ghost: true, ..btc.clone() };
        db::record_trade_db(&pool, &ghost, Decimal::ZERO, "AnchorStrategy", "MG", "NO",
            dec!(0.50), dec!(0), dec!(10), dec!(-5), "sim", Some(now)).await;

        let m = fold_rows(read_rows(&pool).await.into_iter().map(|(_, k, p, r)| (k, p, r)), 2);
        let real = RecordKey { strategy: "AnchorStrategy".into(), market_class: "crypto".into(), underlying: Some("btc".into()), ghost: false };
        assert_eq!(m[&real], rec(2, dec!(5), dec!(10)), "the newest two: +$2 and +$3");
        assert_eq!(m[&RecordKey { ghost: true, ..real.clone() }].pnl, dec!(-5));
        assert_eq!(m.len(), 2, "the unclassified row is left out");
    }

    #[test]
    fn helm_is_never_scaled() {
        assert!(!scalable("HelmStrategy"));
        assert!(scalable("AnchorStrategy"));
    }

    #[test]
    fn ghost_and_real_records_never_mix() {
        let real = RecordKey { strategy: "A".into(), market_class: "crypto".into(), underlying: Some("btc".into()), ghost: false };
        let ghost = RecordKey { ghost: true, ..real.clone() };
        let m = fold_rows(vec![(real.clone(), dec!(1), dec!(10)), (ghost.clone(), dec!(-5), dec!(10))], 30);
        assert_eq!(m[&real].pnl, dec!(1));
        assert_eq!(m[&ghost].pnl, dec!(-5));
    }
}
