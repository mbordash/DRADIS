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

/// FairValue Strategy — analytic binary-option pricing (2026-08-05)
///
/// # Thesis
///
/// A Polymarket up/down market is a cash-or-nothing binary option. Its fair
/// value has a closed form (drift ≈ 0 over intraday horizons):
///
///   P(S_T > K) = Φ( ln(S/K) / (σ·√T) )
///
/// where S = oracle spot, K = strike, σ = realized vol of oracle log-returns
/// (per √second, self-sampled), T = seconds to expiry. Retail flow prices
/// these markets by vibes; this viper prices them by math and buys whichever
/// side trades at a discount to model fair value.
///
/// # Two regimes, one model
///
/// * **Mid-session**: vol-estimate error is material → demand a wide edge
///   (FAIRVALUE_BASE_EDGE, default 8¢ net of fees).
/// * **Endgame ("settlement snipe")**: √T collapses, the model approaches a
///   step function, and Polymarket's fee (rate·p·(1−p)) collapses with it.
///   The edge requirement tapers to FAIRVALUE_MIN_EDGE, naturally producing
///   entries like "ask $0.96 for a side the model prices at 0.999".
///   A pin-risk guard refuses endgame entries when the spot is within
///   FAIRVALUE_PIN_MIN_SIGMA σ-distances of the strike — the coin-flip zone
///   where one loss at $0.97 erases ~30 wins.
///
/// # Exits
///
/// 1. TP at FAIRVALUE_TARGET_PROFIT_PERCENT — except inside the settlement
///    window with the model still ≥ SETTLE_HOLD_MIN_PROB, where holding to
///    settlement pays $1.00 with zero exit fee (strictly dominates a taker TP).
/// 2. SL at FAIRVALUE_STOP_LOSS_PERCENT (min-hold gated, catastrophic bypass).
/// 3. Model reversal: our side's fair probability has decayed by
///    `fairvalue_model_reversal_decay_pct` from where it stood at entry — the
///    thesis is gone, exit without waiting for the SL. The trigger is
///    **entry-relative**, not an absolute floor: this viper's whole job is to
///    buy a side the model prices above its ask, and on a cheap tail that fair
///    value is legitimately low (0.30 against a 0.18 ask). An absolute floor
///    of 0.40 made every such entry exit-eligible the moment it filled, so the
///    position was closed by the 60s min-hold rather than by any change of
///    thesis (observed 2026-08-12: three round trips, all exited at exactly
///    t+60s, the one "winner" saved only by TP firing at t+10s).
/// 4. Endgame bail-out: final BAIL_SECS with fair probability < BAIL_PROB.

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex as StdMutex, OnceLock};
use std::time::Instant;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams, MarketConfig, MarketSnapshot};
use crate::vipers::is_drawdown_limit_hit;
use crate::config;
use crate::helpers::volatility::{fair_yes_probability, sigma_per_sqrt_sec};
use crate::venues::core::TimeInForce;

/// All FairValue state outlives the strategy object, which is recreated on
/// every market rotation (`create_all_strategies()` in patrol_impl) and would
/// otherwise wipe the vol sampler mid-warmup every ~25-35 min and reset
/// persistence streaks / exit cooldowns. Same pattern as gboost_label_pool and
/// the Maker baselines.
///
/// State is keyed **per asset**, not per process. The CAG runs a squadron per
/// asset (btc-open, eth-open, …) concurrently, and every one of them evaluates
/// this strategy against its own oracle. A single shared `vol_samples` deque
/// therefore interleaved BTC (~$65,000) and ETH (~$1,918) prices, and each
/// crossover contributed a log-return of ±3.5 to the realized-vol estimate.
/// That inflated σ by roughly three orders of magnitude (observed 8.32e-2/√s
/// against a 5.0e-5 floor), which drove `d` to zero and pinned fair value at
/// exactly 0.500 on every evaluation — turning the model into "buy whichever
/// leg trades below 50¢" and producing systematic negative-edge entries into
/// the cheap tail (observed 2026-08-10). `signal_streak` is a single slot and
/// was likewise clobbered between assets.
struct FairValueGlobals {
    /// Self-sampled oracle price history: (sample time, price). One sample per
    /// FAIRVALUE_VOL_SAMPLE_SECS, pruned past FAIRVALUE_VOL_WINDOW_SECS.
    vol_samples: StdMutex<VecDeque<(Instant, f64)>>,
    /// Entry-edge persistence streak: (condition_id, is_yes, first_seen, last_seen).
    signal_streak: StdMutex<Option<(String, bool, Instant, Instant)>>,
    /// Per-token post-exit re-entry cooldowns (armed when an Exit is emitted).
    exit_cooldowns: StdMutex<HashMap<String, Instant>>,
    /// Throttle for the periodic fair-vs-market diagnostic log.
    last_diag_log_at: StdMutex<Option<Instant>>,
    /// Throttle for the entry-signal info log (signal can re-fire every tick).
    last_entry_log_at: StdMutex<Option<Instant>>,
    /// Stop-loss circuit breaker: SL exits per condition_id this process life.
    sl_counts: StdMutex<HashMap<String, u32>>,
    /// Model fair probability of the bought side at the moment the entry signal
    /// was emitted, keyed by token_id. The model-reversal exit measures decay
    /// against this rather than against an absolute floor. Cleared on exit.
    entry_fair: StdMutex<HashMap<String, f64>>,
    /// Per-market model fair-value history: condition_id → (sample time,
    /// fair_yes), one sample per FAIRVALUE_VOL_SAMPLE_SECS, pruned past
    /// FAIRVALUE_EDGE_NOISE_WINDOW_SECS. Feeds the edge-vs-noise gate.
    ///
    /// Keyed per market rather than held in a single slot because the viper
    /// alternates between the hourly and Window/Daily venues from tick to tick,
    /// and those price two different contracts: a single slot would read every
    /// venue flip as a fair-value jump and report noise that is really just the
    /// switch. Stale keys are pruned once their newest sample ages out.
    fair_history: StdMutex<HashMap<String, VecDeque<(Instant, f64)>>>,
    /// Positions already counted toward `sl_counts`, keyed token_id →
    /// `Position::opened_at`.
    ///
    /// A stop-loss exit is re-emitted on every patrol tick until it actually
    /// fills, and an exit does not always fill first try. Counting on emission
    /// therefore counted evaluation ticks rather than stop-outs: on 2026-08-13
    /// a single catastrophic stop whose FAK missed at $0.33 drove the counter
    /// from 1 to 101 in five seconds — the exact span of
    /// `EXIT_RETRY_COOLDOWN_SECS`, during which dispatch was throttled while
    /// `evaluate_exit` kept firing ~20×/s. `opened_at` distinguishes a genuine
    /// second stop-out on a re-entered token from the same stop-out re-emitted,
    /// so a re-entry still counts while a retry does not.
    sl_counted: StdMutex<HashMap<String, DateTime<Utc>>>,
}

impl FairValueGlobals {
    fn new() -> Self {
        Self {
            vol_samples:      StdMutex::new(VecDeque::new()),
            signal_streak:    StdMutex::new(None),
            exit_cooldowns:   StdMutex::new(HashMap::new()),
            last_diag_log_at: StdMutex::new(None),
            last_entry_log_at: StdMutex::new(None),
            sl_counts:        StdMutex::new(HashMap::new()),
            entry_fair:       StdMutex::new(HashMap::new()),
            fair_history:     StdMutex::new(HashMap::new()),
            sl_counted:       StdMutex::new(HashMap::new()),
        }
    }
}

/// Per-asset state, created on first sight of an asset and never dropped.
/// Leaked deliberately so callers keep the `&'static` borrow the old global
/// gave them — there are at most a handful of assets per process.
fn globals(asset: &str) -> &'static FairValueGlobals {
    static G: OnceLock<StdMutex<HashMap<String, &'static FairValueGlobals>>> = OnceLock::new();
    let map = G.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut guard = match map.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    *guard
        .entry(asset.to_ascii_uppercase())
        .or_insert_with(|| Box::leak(Box::new(FairValueGlobals::new())))
}

/// Edge value for a leg that cannot be priced (no ask, or an ask outside
/// `(0, 1)` — an empty or crossed book).
///
/// Deliberately **not** `Decimal::MIN`. `rust_decimal` renders into a fixed
/// 32-char `ArrayString`, and `Decimal::MIN` is 29 digits at scale 0, so the
/// diagnostic log's `{:+.3}` needs 1 sign + 29 digits + 1 point + 3 padding
/// zeros = 34 chars and panics with `CapacityError` inside `to_str_internal`.
/// That took the whole process down the first time a Kalshi leg quoted no ask
/// (2026-08-10). Any value below −1 sorts under every real edge, since a real
/// edge is a probability minus a price and cannot leave [−1, 1].
const NO_EDGE: Decimal = dec!(-100);

pub struct FairValueStrategyImpl;

impl Default for FairValueStrategyImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl FairValueStrategyImpl {
    pub fn new() -> Self {
        Self
    }

    /// σ floor for a given forecast horizon.
    ///
    /// Zero-strength (absolute backstop only) at or below `horizon_secs`,
    /// ramping linearly to the full floor at twice that. Rationale in
    /// `config::FAIRVALUE_SIGMA_FLOOR_HORIZON_SECS`: inside the measurement
    /// window the realized-vol estimate is in-sample and should be trusted;
    /// beyond it, forecast error compounds and the floor earns its keep.
    fn sigma_floor(horizon_secs: i64, secs_left: i64) -> f64 {
        let abs = config::FAIRVALUE_ABSOLUTE_MIN_SIGMA_PER_SQRT_SEC;
        let full = config::FAIRVALUE_MIN_SIGMA_PER_SQRT_SEC;
        if horizon_secs <= 0 {
            // Knob disabled — restore the unconditional floor.
            return full;
        }
        let ramp = ((secs_left - horizon_secs) as f64 / horizon_secs as f64).clamp(0.0, 1.0);
        abs + (full - abs) * ramp
    }

    /// Feed the vol sampler and return the **raw** σ per √second, or None
    /// during warmup / frozen oracle.
    ///
    /// Unfloored on purpose: the sampler is fed before the venue (and therefore
    /// the forecast horizon) is known, and the floor is horizon-dependent.
    /// Callers apply [`sigma_floor`](Self::sigma_floor) once `secs_left` is in
    /// hand.
    fn update_and_read_sigma(&self, asset: &str, oracle_price: f64) -> Option<f64> {
        let mut samples = match globals(asset).vol_samples.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let now = Instant::now();
        let due = samples
            .back()
            .map_or(true, |(t, _)| now.duration_since(*t).as_secs() >= config::FAIRVALUE_VOL_SAMPLE_SECS);
        if due && oracle_price > 0.0 {
            samples.push_back((now, oracle_price));
        }
        while let Some((t, _)) = samples.front() {
            if now.duration_since(*t).as_secs() > config::FAIRVALUE_VOL_WINDOW_SECS {
                samples.pop_front();
            } else {
                break;
            }
        }
        let span_secs = match (samples.front(), samples.back()) {
            (Some((f, _)), Some((b, _))) => b.duration_since(*f).as_secs_f64(),
            _ => return None,
        };
        let prices: Vec<f64> = samples.iter().map(|(_, p)| *p).collect();
        sigma_per_sqrt_sec(&prices, span_secs, config::FAIRVALUE_MIN_VOL_SAMPLES)
    }

    /// Feed the per-market fair-value history and return the model's own
    /// short-horizon noise: the standard deviation of successive Δfair, rescaled
    /// from the sample cadence to `FAIRVALUE_EDGE_NOISE_HORIZON_SECS` by √t.
    ///
    /// This is the yardstick the claimed edge has to beat. Edge is a difference
    /// between the model's fair value and the book; if the model's own output
    /// wanders further than that difference over the horizon the position is
    /// held, the difference carries no information and the entry is a coin flip
    /// paying two taker fees. Measured in prod on 2026-08-13/14: 24% of ~2min
    /// ticks moved the fair value more than the entire 0.08 base edge.
    ///
    /// `None` during warmup — deliberately blocking rather than permissive, see
    /// `FAIRVALUE_EDGE_NOISE_MIN_SAMPLES`.
    fn update_and_read_fair_noise(&self, asset: &str, condition_id: &str, fair_yes: f64) -> Option<f64> {
        let mut hist = match globals(asset).fair_history.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let now = Instant::now();
        let window = config::FAIRVALUE_EDGE_NOISE_WINDOW_SECS;

        let samples = hist.entry(condition_id.to_string()).or_default();
        let due = samples
            .back()
            .map_or(true, |(t, _)| now.duration_since(*t).as_secs() >= config::FAIRVALUE_VOL_SAMPLE_SECS);
        if due {
            samples.push_back((now, fair_yes));
        }
        while let Some((t, _)) = samples.front() {
            if now.duration_since(*t).as_secs() > window {
                samples.pop_front();
            } else {
                break;
            }
        }

        let series: Vec<f64> = samples.iter().map(|(_, f)| *f).collect();

        // Drop markets that have rotated out, so the map does not grow for the
        // life of the process. Done here rather than on a timer because this is
        // the only place the map is held.
        hist.retain(|_, v| v.back().is_some_and(|(t, _)| now.duration_since(*t).as_secs() <= window));

        Self::fair_noise_from(&series, config::FAIRVALUE_EDGE_NOISE_MIN_SAMPLES)
    }

    /// Statistics half of [`update_and_read_fair_noise`], split out because the
    /// sampler half is cadence-gated on `Instant` and cannot be driven from a
    /// test without sleeping through a real 15s window per sample.
    fn fair_noise_from(series: &[f64], min_samples: usize) -> Option<f64> {
        if series.len() < min_samples.max(3) {
            return None;
        }
        let diffs: Vec<f64> = series.windows(2).map(|w| w[1] - w[0]).collect();
        if diffs.len() < 2 {
            return None;
        }
        let mean = diffs.iter().sum::<f64>() / diffs.len() as f64;
        let var = diffs.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / diffs.len() as f64;
        // √t rescale from one sample interval to the reference horizon.
        let scale =
            (config::FAIRVALUE_EDGE_NOISE_HORIZON_SECS as f64 / config::FAIRVALUE_VOL_SAMPLE_SECS as f64).sqrt();
        Some(var.sqrt() * scale)
    }

    /// Time-scaled edge requirement: linear taper MIN_EDGE→BASE_EDGE inside the
    /// taper horizon, then √(T/taper) growth beyond it (capped) — the further
    /// out settlement is, the less the 1-hour realized-vol window can be
    /// trusted, so mid-session entries on daily markets need a much larger
    /// discount than endgame entries.
    fn required_edge(dc: &crate::helpers::dynamic_config::DynamicConfig, secs_left: i64) -> Decimal {
        let base = dc.fairvalue_base_edge;
        let min = dc.fairvalue_min_edge;
        if secs_left >= config::FAIRVALUE_EDGE_TAPER_SECS {
            let scale = (secs_left as f64 / config::FAIRVALUE_EDGE_TAPER_SECS as f64).sqrt();
            let scaled = base * Decimal::from_f64_retain(scale).map(|d| d.round_dp(10)).unwrap_or(dec!(1));
            return scaled.min(config::FAIRVALUE_EDGE_HORIZON_CAP).max(base);
        }
        let frac = Decimal::from(secs_left.max(0)) / Decimal::from(config::FAIRVALUE_EDGE_TAPER_SECS);
        min + (base - min) * frac
    }

    /// Polymarket dynamic taker fee fraction at price p: rate · p · (1−p).
    fn fee_frac(price: Decimal) -> Decimal {
        config::CRYPTO_FEE_RATE * price * (dec!(1) - price)
    }

    /// Does the model still justify holding a position that has hit its stop?
    ///
    /// Returns the live edge when the stop should be vetoed, `None` when it
    /// should fire. Deliberately recomputes the *entry* test — same round-trip
    /// fee treatment, same horizon-scaled requirement — so the two can never
    /// drift apart: what it takes to open a position is what it takes to keep it.
    ///
    /// `confirm` scales the bar: 0 disables the veto entirely, 1.0 demands full
    /// entry-grade edge, above 1.0 is stricter than entry.
    fn stop_vetoed_by_model(
        fair: Option<f64>,
        ask: Decimal,
        req_edge: Decimal,
        confirm: Decimal,
    ) -> Option<Decimal> {
        if confirm <= dec!(0) || ask <= dec!(0) || ask >= dec!(1) {
            return None;
        }
        let fair_dec = Decimal::from_f64_retain(fair?).map(|d| d.round_dp(10))?;
        let live_edge = fair_dec - ask - Self::fee_frac(ask) - Self::fee_frac(fair_dec);
        (live_edge >= req_edge * confirm).then_some(live_edge)
    }

    fn cooldown_active(&self, asset: &str, token_id: &str, cooldown_secs: i64) -> bool {
        let mut reg = match globals(asset).exit_cooldowns.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if let Some(t) = reg.get(token_id) {
            if (t.elapsed().as_secs() as i64) < cooldown_secs {
                return true;
            }
            reg.remove(token_id);
        }
        false
    }

    fn arm_cooldown(&self, asset: &str, token_id: &str) {
        let mut reg = match globals(asset).exit_cooldowns.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        reg.insert(token_id.to_string(), Instant::now());
        // Every call site is an exit emission, so this is also where the
        // position's entry-fair anchor stops being meaningful. Clearing here
        // keeps the map from leaking across market rotations without needing a
        // matching call on all four exit paths.
        self.clear_entry_fair(asset, token_id);
    }

    /// Count one stop-out toward the market's breaker, exactly once per
    /// position however many times the exit is re-emitted before it fills.
    ///
    /// Returns the new count when this stop-out had not been counted yet, and
    /// `None` when it is a re-emission of one already counted — which the
    /// caller uses to suppress a duplicate breaker warning as well as the
    /// duplicate increment.
    fn count_stop_loss_once(
        &self,
        asset: &str,
        token_id: &str,
        opened_at: DateTime<Utc>,
        condition_id: &str,
    ) -> Option<u32> {
        let g = globals(asset);
        {
            let mut seen = match g.sl_counted.lock() {
                Ok(x) => x,
                Err(p) => p.into_inner(),
            };
            // Same token AND same open instant → this is the same stop-out
            // being retried, not a new one.
            if seen.get(token_id) == Some(&opened_at) {
                return None;
            }
            seen.insert(token_id.to_string(), opened_at);
        }
        let mut counts = match g.sl_counts.lock() {
            Ok(x) => x,
            Err(p) => p.into_inner(),
        };
        let n = counts.entry(condition_id.to_string()).or_insert(0);
        *n += 1;
        Some(*n)
    }

    /// Remember the model fair probability of the side we are buying, so the
    /// model-reversal exit can measure decay from the entry thesis instead of
    /// from a fixed floor. Re-firing entry signals overwrite the same key,
    /// which is correct: the position's basis is the most recent fill.
    fn record_entry_fair(&self, asset: &str, token_id: &str, fair: f64) {
        let mut reg = match globals(asset).entry_fair.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        reg.insert(token_id.to_string(), fair);
    }

    /// Baseline the model-reversal exit measures decay against.
    ///
    /// Falls back to the position's entry price when no entry fair is on record
    /// — a chain-adopted position, or one carried across a process restart.
    /// That fallback is conservative and always available: entry required
    /// `fair > ask`, so the recorded average entry price is a lower bound on
    /// what the fair value was when the position was opened. Using it can only
    /// make the exit *later* than the true thesis would, never instant.
    fn reversal_baseline(&self, asset: &str, token_id: &str, avg_entry: Decimal) -> f64 {
        let reg = match globals(asset).entry_fair.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        reg.get(token_id)
            .copied()
            .or_else(|| avg_entry.to_f64())
            .unwrap_or(0.0)
    }

    /// Drop the entry-fair record once a position is closed.
    fn clear_entry_fair(&self, asset: &str, token_id: &str) {
        let mut reg = match globals(asset).entry_fair.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        reg.remove(token_id);
    }

    /// Recompute the model's current fair probability for a held token's side.
    /// None when the model can't price (no strike/vol/time).
    fn fair_prob_for_side(
        &self,
        asset: &str,
        market: &MarketConfig,
        snapshot: &MarketSnapshot,
        token_is_yes: bool,
        sigma_floor_horizon_secs: i64,
    ) -> Option<f64> {
        let strike = market.strike_price?.to_f64()?;
        let spot = snapshot.oracle_price.to_f64()?;
        let secs_left = market
            .market_close_time
            .map(|ct| (ct - Utc::now()).num_seconds())?;
        if secs_left <= 0 {
            // Expired: outcome is the sign of S−K.
            let yes_won = spot > strike;
            return Some(if yes_won == token_is_yes { 1.0 } else { 0.0 });
        }
        // Read σ without feeding a new sample (entry path owns the sampler cadence).
        let samples = match globals(asset).vol_samples.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let span_secs = match (samples.front(), samples.back()) {
            (Some((f, _)), Some((b, _))) => b.duration_since(*f).as_secs_f64(),
            _ => return None,
        };
        let prices: Vec<f64> = samples.iter().map(|(_, p)| *p).collect();
        drop(samples);
        let sigma = sigma_per_sqrt_sec(&prices, span_secs, config::FAIRVALUE_MIN_VOL_SAMPLES)?
            .max(Self::sigma_floor(sigma_floor_horizon_secs, secs_left));
        let fair_yes = fair_yes_probability(spot, strike, sigma, secs_left as f64)?;
        Some(if token_is_yes { fair_yes } else { 1.0 - fair_yes })
    }
}

#[async_trait]
impl Strategy for FairValueStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        // "Why no trades?" registry feed (GET /api/vipers/status).
        let idle = |r: &str| crate::helpers::viper_status::report_reason(&ctx.crypto_filter, &self.name(), r);
        if !dc.enable_fairvalue {
            idle("disabled in config");
            return Ok(StrategySignal::NoSignal);
        }
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            idle("session drawdown limit hit");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Vol sampler feed (BEFORE structural gates) ───────────────────────
        // Warmup must progress even while the venue/strike is temporarily
        // unavailable, otherwise structural hiccups also stall the sampler.
        let spot = match ctx.snapshot.oracle_price.to_f64() {
            Some(s) if s > 0.0 => s,
            _ => { idle("no oracle price"); return Ok(StrategySignal::NoSignal) },
        };
        let sigma_opt = self.update_and_read_sigma(&ctx.crypto_filter, spot);

        // ── Venue selection ──────────────────────────────────────────────────
        // The required edge is horizon-scaled: base × √(T/TAPER), capped at
        // FAIRVALUE_EDGE_HORIZON_CAP. On the Window/Daily venue T is ~6-20 hours,
        // which pins the requirement at the 0.25 cap — a 25% mispricing. Prod
        // telemetry (2026-08-12, 478 evaluations over 16.5h): the best edge ever
        // observed was 0.113 and the median was NEGATIVE, so daily-venue entries
        // are not merely rare, they are arithmetically unreachable.
        //
        // The hourly venue's T taper resolves to roughly 0.03-0.10, which the
        // observed edge distribution does reach. So prefer the hourly whenever it
        // is structurally usable, and fall back to the daily only when it is not.
        // `fairvalue_prefer_hourly` restores the old daily-first order if needed.
        let hourly_viable = ctx.market.strike_price.is_some_and(|s| s > dec!(0))
            && ctx.market.market_close_time
                .is_some_and(|ct| (ct - Utc::now()).num_seconds() >= config::FAIRVALUE_MIN_SECS_TO_EXPIRY);

        let (market, snap) = match (&ctx.maker_market, &ctx.maker_snapshot) {
            (Some(mk_mkt), Some(mk_snap)) => {
                if dc.fairvalue_prefer_hourly && hourly_viable {
                    (&ctx.market, &ctx.snapshot)
                } else {
                    (mk_mkt, mk_snap)
                }
            }
            _ => (&ctx.market, &ctx.snapshot),
        };

        // ── Structural requirements ──────────────────────────────────────────
        let strike = match market.strike_price.and_then(|s| s.to_f64()) {
            Some(s) if s > 0.0 => s,
            _ => { idle("market has no strike price"); return Ok(StrategySignal::NoSignal) },
        };
        let secs_left = match market.market_close_time {
            Some(ct) => (ct - Utc::now()).num_seconds(),
            None => { idle("market has no close time"); return Ok(StrategySignal::NoSignal) },
        };
        if secs_left < config::FAIRVALUE_MIN_SECS_TO_EXPIRY {
            idle("too close to expiry");
            return Ok(StrategySignal::NoSignal);
        }
        let snap_age = (Utc::now() - snap.timestamp).num_seconds();
        if snap_age > config::FAIRVALUE_MAX_SNAPSHOT_AGE_SECS {
            idle("snapshot stale");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Model inputs: self-sampled realized vol (sampled above) ──────────
        // The floor is applied here, not in the sampler, because its strength
        // depends on how far out we are forecasting.
        let sigma = match sigma_opt {
            // warmup complete, oracle alive
            Some(s) => s.max(Self::sigma_floor(dc.fairvalue_sigma_floor_horizon_secs, secs_left)),
            None => {
                // Warmup visibility: without this the viper is totally silent
                // for the first FAIRVALUE_MIN_VOL_SAMPLES × SAMPLE_SECS.
                let mut last = globals(&ctx.crypto_filter).last_diag_log_at.lock().unwrap();
                let due = last.map_or(true, |t| t.elapsed().as_secs() >= config::GBOOST_PRED_LOG_INTERVAL_SECS);
                if due {
                    *last = Some(Instant::now());
                    let n = globals(&ctx.crypto_filter).vol_samples.lock().map(|s| s.len()).unwrap_or(0);
                    tracing::info!(
                        " FairValue: vol warmup {}/{} samples ({}s cadence)",
                        n, config::FAIRVALUE_MIN_VOL_SAMPLES, config::FAIRVALUE_VOL_SAMPLE_SECS,
                    );
                }
                idle("vol warmup in progress");
                return Ok(StrategySignal::NoSignal);
            }
        };

        // ── Fair value ────────────────────────────────────────────────────────
        let fair_yes = match fair_yes_probability(spot, strike, sigma, secs_left as f64) {
            Some(p) => p,
            None => { idle("fair value not computable"); return Ok(StrategySignal::NoSignal) },
        };
        let d_sigma = (spot / strike).ln() / (sigma * (secs_left as f64).sqrt());

        // Fed before the guards below, for the same reason the vol sampler is:
        // a gate that refuses entries must not also starve the measurement it
        // will later be judged against.
        let fair_noise = self.update_and_read_fair_noise(&ctx.crypto_filter, &market.condition_id, fair_yes);

        // ── Pin-risk guard (endgame coin-flip zone) ──────────────────────────
        // Two separate refusals, both guarding the same hazard — buying a coin
        // flip — but on different axes:
        //   * endgame pin: near expiry a strike-hugging spot is unresolvable
        //   * coin-flip floor: |d| below the threshold is noise at ANY horizon
        let endgame_pin = secs_left < config::FAIRVALUE_PIN_GUARD_SECS
            && d_sigma.abs() < config::FAIRVALUE_PIN_MIN_SIGMA;
        let coin_flip = d_sigma.abs() < config::FAIRVALUE_MIN_ABS_SIGMA;
        let pin_blocked = endgame_pin || coin_flip;

        // ── Edge on each side (net of taker entry fee) ───────────────────────
        let req_edge = Self::required_edge(dc, secs_left);
        let fair_yes_dec = Decimal::from_f64_retain(fair_yes).map(|d| d.round_dp(10)).unwrap_or(dec!(0.5));
        // Edge must clear the ROUND TRIP, not just the entry.
        //
        // Charging only the entry fee understated the true hurdle by roughly
        // half. Measured over five Kalshi round trips on 2026-08-10: gross P&L
        // −$0.07, fees −$1.05 — the fees were the entire loss. The exit fee is
        // estimated at the model's own fair value, because that is where the
        // contract trades if the thesis plays out. Holding to settlement pays
        // no exit fee at all, so this errs conservative on purpose.
        let fair_no_dec = dec!(1) - fair_yes_dec;
        let yes_edge = if snap.yes_ask > dec!(0) && snap.yes_ask < dec!(1) {
            fair_yes_dec - snap.yes_ask - Self::fee_frac(snap.yes_ask) - Self::fee_frac(fair_yes_dec)
        } else {
            NO_EDGE
        };
        let no_edge = if snap.no_ask > dec!(0) && snap.no_ask < dec!(1) {
            fair_no_dec - snap.no_ask - Self::fee_frac(snap.no_ask) - Self::fee_frac(fair_no_dec)
        } else {
            NO_EDGE
        };

        // ── Periodic diagnostic (calibration visibility, throttled) ──────────
        {
            let mut last = globals(&ctx.crypto_filter).last_diag_log_at.lock().unwrap();
            let due = last.map_or(true, |t| t.elapsed().as_secs() >= config::GBOOST_PRED_LOG_INTERVAL_SECS);
            if due {
                *last = Some(Instant::now());
                tracing::info!(
                    " FairValue: fair(YES)={:.3} (d={:+.2}σ, σ/√s={:.2e}, T={}s) | yes_ask=${:.2} edge={:+.3} | no_ask=${:.2} edge={:+.3} | req={:.3} | noise{}={}{}",
                    fair_yes, d_sigma, sigma, secs_left,
                    snap.yes_ask, yes_edge, snap.no_ask, no_edge, req_edge,
                    config::FAIRVALUE_EDGE_NOISE_HORIZON_SECS,
                    fair_noise.map_or_else(|| "warmup".to_string(), |n| format!("{:.3}", n)),
                    match (endgame_pin, coin_flip) {
                        (true, _) => " [PIN-GUARD]",
                        (_, true) => " [COIN-FLIP]",
                        _         => "",
                    },
                );
            }
        }

        // ── Pick the better side, if any qualifies ───────────────────────────
        let (want_yes, edge, ask, token_id, fee_bps) = if yes_edge >= no_edge {
            (true, yes_edge, snap.yes_ask, market.yes_token.clone(), market.yes_fee_bps as u16)
        } else {
            (false, no_edge, snap.no_ask, market.no_token.clone(), market.no_fee_bps as u16)
        };
        if edge < req_edge || pin_blocked {
            idle(match (endgame_pin, coin_flip) {
                (true, _) => "pin-risk guard (endgame coin-flip)",
                (_, true) => "coin-flip guard (|d| below floor)",
                _         => "edge below required",
            });
            return Ok(StrategySignal::NoSignal);
        }
        if ask < dc.fairvalue_min_entry_price || ask > dc.fairvalue_max_entry_price {
            idle("ask outside entry price band");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Edge vs the model's own noise ────────────────────────────────────
        // `req_edge` scales with the forecast horizon but knows nothing about
        // how steady the model has actually been on THIS market. Both matter:
        // an 8¢ edge is meaningful when fair value has been drifting 2¢ per
        // tick and meaningless when it has been swinging 18¢. On the 1AM ET
        // market of 2026-08-14 fair(YES) travelled 0.118 → 0.808 in six minutes
        // while the viper took two entries against a ~10¢ edge; both stopped
        // out and the contract settled against the side it had bought.
        if dc.fairvalue_edge_noise_multiple > dec!(0) {
            match fair_noise {
                Some(noise) => {
                    let noise_dec = Decimal::from_f64_retain(noise).map(|d| d.round_dp(10)).unwrap_or(dec!(0));
                    let required = dc.fairvalue_edge_noise_multiple * noise_dec;
                    if edge < required {
                        idle("edge below model noise");
                        return Ok(StrategySignal::NoSignal);
                    }
                }
                None => {
                    idle("fair-value noise warmup");
                    return Ok(StrategySignal::NoSignal);
                }
            }
        }

        // ── Spread guard: never buy into a position that is born stopped out ──
        // Entry crosses the spread at the ask, but every exit rule below marks
        // against the bid, so a wide book prices the position under water the
        // instant it fills. On a thin venue book (Kalshi hourly, 2026-08-10:
        // bought YES at $0.43 with a $0.28 bid) that showed as −34.9% one tick
        // after entry — past 2× the stop — so the catastrophic branch dumped it
        // 30s later and the round-trip realised the spread plus both fees, with
        // the thesis never given a chance to play out. Refuse any entry whose
        // immediate mark-to-bid already sits at or below the stop loss.
        let entry_bid = if want_yes { snap.yes_bid } else { snap.no_bid };
        if entry_bid <= dec!(0) {
            idle("no bid — position would have no exit liquidity");
            return Ok(StrategySignal::NoSignal);
        }
        let instant_mark = (entry_bid - ask) / ask;
        if instant_mark <= -dc.fairvalue_stop_loss_pct {
            idle("spread too wide (entry would mark below the stop loss)");
            return Ok(StrategySignal::NoSignal);
        }
        if self.cooldown_active(&ctx.crypto_filter, token_id.as_str(), dc.fairvalue_post_exit_cooldown_secs) {
            idle("post-exit cooldown active");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Stop-loss circuit breaker: model is miscalibrated for this market ─
        {
            let counts = match globals(&ctx.crypto_filter).sl_counts.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if counts.get(&market.condition_id).copied().unwrap_or(0)
                >= dc.fairvalue_max_stop_losses_per_market
            {
                idle("stop-loss circuit breaker tripped");
                return Ok(StrategySignal::NoSignal);
            }
        }

        // ── No pyramiding: one position per market ───────────────────────────
        // ── Exposure cap ──────────────────────────────────────────────────────
        {
            let pos_map = ctx.positions.lock().await;
            if pos_map.contains_key(&("FairValueStrategy".to_string(), market.yes_token.clone()))
                || pos_map.contains_key(&("FairValueStrategy".to_string(), market.no_token.clone()))
            {
                idle("position already open (no pyramiding)");
                return Ok(StrategySignal::NoSignal);
            }
            let current_exposure: Decimal = pos_map.iter()
                .filter(|((s, _), _)| s == "FairValueStrategy")
                .map(|(_, p)| p.shares * p.avg_entry)
                .sum();
            if current_exposure + dc.fairvalue_trade_size_usdc > dc.fairvalue_max_exposure_usdc {
                idle("exposure cap reached");
                return Ok(StrategySignal::NoSignal);
            }
        }

        // ── Balance gate (fee headroom, mirrors Basis) ───────────────────────
        let fee_headroom = dec!(1) + Decimal::from(fee_bps) / dec!(10000);
        if ctx.available_collateral < dc.fairvalue_trade_size_usdc {
            idle("insufficient collateral");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Edge persistence debounce (anti ask-flicker) ─────────────────────
        let persisted = {
            let now = Instant::now();
            let mut streak = globals(&ctx.crypto_filter).signal_streak.lock().unwrap();
            match streak.as_mut() {
                Some((cid, dir, first_seen, last_seen))
                    if *cid == market.condition_id
                        && *dir == want_yes
                        && last_seen.elapsed().as_secs() <= config::FAIRVALUE_SIGNAL_CONTINUITY_GAP_SECS =>
                {
                    *last_seen = now;
                    first_seen.elapsed().as_secs() >= config::FAIRVALUE_ENTRY_PERSISTENCE_SECS
                }
                _ => {
                    *streak = Some((market.condition_id.clone(), want_yes, now, now));
                    false
                }
            }
        };
        if !persisted {
            idle("edge persistence debounce");
            return Ok(StrategySignal::NoSignal);
        }

        let side = if want_yes { "YES" } else { "NO" };
        let shares = (dc.fairvalue_trade_size_usdc / fee_headroom) / ask;
        // Anchor the model-reversal exit to the thesis we are entering on.
        let entry_fair = if want_yes { fair_yes } else { 1.0 - fair_yes };
        self.record_entry_fair(&ctx.crypto_filter, token_id.as_str(), entry_fair);
        {
            // Throttled: a passed persistence gate re-fires every tick.
            let mut last = globals(&ctx.crypto_filter).last_entry_log_at.lock().unwrap();
            let due = last.map_or(true, |t| t.elapsed().as_secs() >= config::FAIRVALUE_ENTRY_LOG_THROTTLE_SECS);
            if due {
                *last = Some(Instant::now());
                tracing::info!(
                    " FairValue {} entry: fair={:.3} ask=${:.2} edge={:+.3} (req {:.3}) | d={:+.2}σ T={}s | shares={:.2}",
                    side, if want_yes { fair_yes } else { 1.0 - fair_yes }, ask, edge, req_edge, d_sigma, secs_left, shares,
                );
                crate::helpers::metrics::stash_entry_signals_json(token_id.as_str(), serde_json::json!({
                    "viper": "FairValue",
                    "side": side,
                    "fair_yes": fair_yes,
                    "d_sigma": d_sigma,
                    "sigma_per_sqrt_sec": sigma,
                    "secs_left": secs_left,
                    "ask": ask.to_string(),
                    "edge": edge.to_string(),
                    "required_edge": req_edge.to_string(),
                }));
            }
        }

        Ok(StrategySignal::Entry {
            params: OrderParams {
                token_id,
                price: ask,
                shares,
                fee_bps,
                is_neg_risk: market.is_neg_risk,
                market_name: market.market_name.clone(),
                condition_id: market.condition_id.clone(),
                order_type: TimeInForce::Fak,
                post_only: false,
                ghost_mode: dc.ghost_mode,
            },
            pair_params: None,
        })
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        let positions = ctx.positions.lock().await;

        for ((strategy_name, token_id), position) in positions.iter() {
            if strategy_name != "FairValueStrategy" {
                continue;
            }

            // Match the venue holding this token.
            let (market, snap) = if let Some(mk) = &ctx.maker_market {
                if token_id == &mk.yes_token || token_id == &mk.no_token {
                    (mk, ctx.maker_snapshot.as_ref().unwrap())
                } else {
                    (&ctx.market, &ctx.snapshot)
                }
            } else {
                (&ctx.market, &ctx.snapshot)
            };

            let token_is_yes = token_id == &market.yes_token;
            let bid = if token_is_yes { snap.yes_bid } else { snap.no_bid };
            let avg_entry = position.avg_entry;
            if avg_entry <= dec!(0) {
                continue;
            }
            let profit_margin = (bid - avg_entry) / avg_entry;
            let secs_held = (Utc::now() - position.opened_at).num_seconds();
            let secs_left = market
                .market_close_time
                .map(|ct| (ct - Utc::now()).num_seconds())
                .unwrap_or(i64::MAX);
            let fair_side = self.fair_prob_for_side(
                &ctx.crypto_filter, market, snap, token_is_yes,
                dc.fairvalue_sigma_floor_horizon_secs,
            );

            let exit_params = |price: Decimal| OrderParams {
                token_id: token_id.clone(),
                price,
                shares: position.shares,
                fee_bps: if token_is_yes { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 },
                is_neg_risk: market.is_neg_risk,
                market_name: market.market_name.clone(),
                condition_id: market.condition_id.clone(),
                order_type: TimeInForce::Fak,
                post_only: false,
                ghost_mode: dc.ghost_mode,
            };

            // ── 1. Take profit — unless the settlement hold dominates ────────
            if profit_margin >= dc.fairvalue_target_profit_pct {
                let settle_hold = secs_left < config::FAIRVALUE_SETTLE_HOLD_SECS
                    && fair_side.map_or(false, |p| p >= config::FAIRVALUE_SETTLE_HOLD_MIN_PROB);
                if !settle_hold {
                    self.arm_cooldown(&ctx.crypto_filter, token_id.as_str());
                    return Ok(StrategySignal::Exit {
                        params: exit_params(bid),
                        reason: format!("FairValueTP: bid=${:.4}, profit={:.2}%", bid, profit_margin * dec!(100)),
                        exit_pair: false,
                    });
                }
                // Settlement hold: $1.00 payout with zero exit fee beats a taker TP.
                continue;
            }

            // ── 2. Model-reversal exit — thesis gone, don't ride to the SL ───
            // Entry-relative, not an absolute floor: a legitimate cheap-tail
            // entry (fair 0.30 vs a 0.18 ask) must not be born exit-eligible.
            let baseline = self.reversal_baseline(&ctx.crypto_filter, token_id.as_str(), avg_entry);
            let decay_pct = dc.fairvalue_model_reversal_decay_pct.to_f64().unwrap_or(0.0);
            let reversal_floor = baseline * (1.0 - decay_pct);
            if secs_held >= 60
                && bid >= config::FAIRVALUE_MIN_EXIT_BID
                && fair_side.map_or(false, |p| p < reversal_floor)
            {
                self.arm_cooldown(&ctx.crypto_filter, token_id.as_str());
                return Ok(StrategySignal::Exit {
                    params: exit_params(bid),
                    reason: format!(
                        "FairValueReversal: fair={:.3} < {:.3} (entry fair {:.3} −{:.0}%), bid=${:.4} ({:+.2}%)",
                        fair_side.unwrap_or(0.0), reversal_floor, baseline, decay_pct * 100.0,
                        bid, profit_margin * dec!(100)
                    ),
                    exit_pair: false,
                });
            }

            // ── 3. Endgame bail-out — don't gamble a fading side on settlement ─
            if secs_left < config::FAIRVALUE_BAIL_SECS
                && bid >= config::FAIRVALUE_MIN_EXIT_BID
                && fair_side.map_or(false, |p| p < config::FAIRVALUE_BAIL_PROB)
            {
                self.arm_cooldown(&ctx.crypto_filter, token_id.as_str());
                return Ok(StrategySignal::Exit {
                    params: exit_params(bid),
                    reason: format!(
                        "FairValueBail: {}s left, fair={:.3} < {:.2}, bid=${:.4}",
                        secs_left, fair_side.unwrap_or(0.0), config::FAIRVALUE_BAIL_PROB, bid
                    ),
                    exit_pair: false,
                });
            }

            // ── 4. Stop loss (min-hold gated, catastrophic bypass) ───────────
            // The catastrophic branch bypasses the min-hold so a genuine crash
            // isn't ridden down, but it still needs a floor: on a wide book the
            // very first mark after entry is already 2× the stop purely from the
            // spread we crossed, and a bid that flickers away for one tick reads
            // identically. Below the floor, hold and let the normal min-hold
            // gate decide once the book has had a chance to quote back.
            let catastrophic = profit_margin <= -(dc.fairvalue_stop_loss_pct * dec!(2))
                && secs_held >= config::FAIRVALUE_MIN_HOLD_SECS_BEFORE_STOP_LOSS / 2;
            if profit_margin <= -dc.fairvalue_stop_loss_pct
                && (catastrophic || secs_held >= config::FAIRVALUE_MIN_HOLD_SECS_BEFORE_STOP_LOSS)
            {
                // ── Model-confirmation veto ──────────────────────────────────
                // The stop is a price rule in a strategy whose entire thesis is
                // "model > price". If the model STILL sees entry-grade edge at
                // the live ask, the drawdown is the market moving toward us, not
                // away — selling there realises a loss on a position we would
                // buy again at that very price.
                //
                // Deliberately re-uses the entry test verbatim (same edge
                // formula, same horizon-scaled requirement) so entry and exit
                // cannot drift apart: whatever it takes to open is what it takes
                // to keep holding. Catastrophic stops are never vetoed, so the
                // veto only ever spans one to two stop widths.
                let confirm = dc.fairvalue_stop_model_confirm_frac;
                if !catastrophic {
                    let ask = if token_is_yes { snap.yes_ask } else { snap.no_ask };
                    let req = Self::required_edge(dc, secs_left);
                    if let Some(live_edge) =
                        Self::stop_vetoed_by_model(fair_side, ask, req, confirm)
                    {
                        tracing::info!(
                            " FairValue stop vetoed [{}]: model still confirms — edge {:+.3} >= {:.3} ({:.2}x req {:.3}) | bid=${:.4} ({:.2}%) fair={:.3} ask=${:.4} held={}s",
                            market.market_name, live_edge, req * confirm, confirm, req,
                            bid, profit_margin * dec!(100), fair_side.unwrap_or(0.0), ask, secs_held,
                        );
                        continue;
                    }
                }

                if bid < config::FAIRVALUE_MIN_EXIT_BID {
                    // Unfillable — an FAK into a vaporised bid just floods logs.
                    continue;
                }
                self.arm_cooldown(&ctx.crypto_filter, token_id.as_str());
                // Counted once per position, not once per emission — a retried
                // exit is the same stop-out, not another one.
                if let Some(n) = self.count_stop_loss_once(
                    &ctx.crypto_filter, token_id.as_str(), position.opened_at, &market.condition_id,
                ) {
                    if n >= dc.fairvalue_max_stop_losses_per_market {
                        tracing::warn!(
                            " FairValue circuit breaker: {} SL exits on \"{}\" — no further entries this market",
                            n, market.market_name
                        );
                    }
                }
                return Ok(StrategySignal::Exit {
                    params: exit_params(bid),
                    reason: if catastrophic {
                        format!("FairValueCatastrophicSL: bid=${:.4}, loss={:.2}% (min-hold bypassed @ {}s)", bid, profit_margin * dec!(100), secs_held)
                    } else {
                        format!("FairValueSL: bid=${:.4}, loss={:.2}%", bid, profit_margin * dec!(100))
                    },
                    exit_pair: false,
                });
            }
        }

        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { "FairValueStrategy".to_string() }
    fn venue(&self) -> &'static str { "Window/Daily" }
    fn max_exposure(&self) -> Decimal { config::FAIRVALUE_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Gross one-sided" }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Decimal::MIN` cannot be rendered with a precision specifier.
    ///
    /// `rust_decimal` formats into a fixed 32-char `ArrayString`. `Decimal::MIN`
    /// is 29 digits at scale 0, so `{:+.3}` needs 1 sign + 29 digits + 1 point +
    /// 3 padding zeros = 34 chars and panics with `CapacityError` inside
    /// `to_str_internal`. The FairValue diagnostic log formats both edges with
    /// `{:+.3}`, so a market with no ask on one leg crashed the process
    /// (observed 2026-08-10 on the Kalshi build).
    /// The model-reversal exit must not be satisfied the instant a position
    /// fills.
    ///
    /// This was the shape of the 2026-08-12 BTC session: the exit floor was an
    /// absolute 0.40, but the entry gate takes cheap-tail positions whose fair
    /// value is legitimately 0.30-0.34, so all three round trips were closed by
    /// the 60s min-hold rather than by any change in the model. Anchoring the
    /// floor to the entry thesis is what makes the two gates consistent.
    #[test]
    fn reversal_floor_is_below_every_admissible_entry_fair() {
        let decay = config::FAIRVALUE_MODEL_REVERSAL_DECAY_PCT
            .to_f64().expect("decay is a small decimal");
        // The three entries actually taken that session, plus the extremes of
        // the entry price band — an entry is only legal when fair > ask.
        for entry_fair in [0.303_f64, 0.306, 0.340, 0.11, 0.99] {
            let floor = entry_fair * (1.0 - decay);
            assert!(
                floor < entry_fair,
                "entry fair {entry_fair} would be exit-eligible on arrival (floor {floor})"
            );
        }
    }

    /// The fallback baseline, used when no entry fair is on record (a
    /// chain-adopted position, or one carried over a restart), must also never
    /// be instantly triggered. Entry requires `fair > ask`, so the recorded
    /// average entry price is a valid lower bound on the entry thesis.
    #[test]
    fn entry_price_fallback_baseline_is_never_instantly_triggered() {
        let strat = FairValueStrategyImpl::new();
        let decay = config::FAIRVALUE_MODEL_REVERSAL_DECAY_PCT
            .to_f64().expect("decay is a small decimal");
        // Nothing recorded for this token → falls back to avg_entry.
        let baseline = strat.reversal_baseline("TEST-FALLBACK-ASSET", "no-such-token", dec!(0.18));
        assert!((baseline - 0.18).abs() < 1e-9, "expected avg_entry fallback, got {baseline}");
        // A position that entered at 0.18 had fair > 0.18, so the floor sits
        // strictly under the thesis it was opened on.
        assert!(baseline * (1.0 - decay) < 0.18);
    }

    /// A recorded entry fair takes precedence over the price fallback.
    #[test]
    fn recorded_entry_fair_anchors_the_reversal_floor() {
        let strat = FairValueStrategyImpl::new();
        strat.record_entry_fair("TEST-ANCHOR-ASSET", "tok-1", 0.34);
        let baseline = strat.reversal_baseline("TEST-ANCHOR-ASSET", "tok-1", dec!(0.18));
        assert!((baseline - 0.34).abs() < 1e-9, "expected recorded entry fair, got {baseline}");
        // Arming the cooldown on exit clears it, so the next position on the
        // same token starts from its own thesis rather than inheriting one.
        strat.arm_cooldown("TEST-ANCHOR-ASSET", "tok-1");
        let after = strat.reversal_baseline("TEST-ANCHOR-ASSET", "tok-1", dec!(0.18));
        assert!((after - 0.18).abs() < 1e-9, "entry fair should be cleared on exit, got {after}");
    }

    /// The σ floor must not override an in-sample measurement.
    ///
    /// On 2026-08-13 σ sat pinned at exactly the floor (4.20e-5) for all 152
    /// samples of a quiet overnight session while realized vol implied ~2.85e-5.
    /// That inflated the fair value of cheap tails by 5-10¢ and manufactured all
    /// three entries, which closed gross-flat and lost $0.97 to fees.
    ///
    /// SUPERSEDED as a default (2026-08-14), kept as a test of the ramp itself.
    /// The taper rests on the premise that an in-sample realized-vol measurement
    /// can be trusted; prod refuted it. Over 48 hourly evaluations on
    /// 2026-08-13/14 the unfloored estimate ran 1.9-2.6e-5/√s — 0.70% daily,
    /// ~13% annualized BTC — against a book implying 1.76× that, and the model's
    /// resulting over-confident fair values manufactured 15 trades for −$3.79.
    /// A horizon of 3600 also meant the floor never bound on an hourly market at
    /// all, since any `secs_left ≤ 3600` clamps the ramp to zero. The shipped
    /// default is now 0 in every profile; this test pins an explicit horizon so
    /// it still covers the ramp for anyone who turns it back on.
    #[test]
    fn sigma_floor_does_not_bind_inside_the_measurement_window() {
        let h = 3600_i64;
        let abs = config::FAIRVALUE_ABSOLUTE_MIN_SIGMA_PER_SQRT_SEC;
        let full = config::FAIRVALUE_MIN_SIGMA_PER_SQRT_SEC;
        // The three entries of that session, by seconds to expiry.
        for secs_left in [932_i64, 1206, 2371] {
            let f = FairValueStrategyImpl::sigma_floor(h, secs_left);
            assert!(
                (f - abs).abs() < 1e-12,
                "T={secs_left}s is inside the vol window; floor should be the absolute backstop, got {f:e}"
            );
            // The measured σ of that night must survive unclamped.
            assert!(2.85e-5_f64.max(f) < full, "realized σ must not be floored up to {full:e}");
        }
    }

    /// Long horizons keep the protection the floor was written for: a 1-hour vol
    /// window is a poor forecast for a settlement many hours out.
    #[test]
    fn sigma_floor_reaches_full_strength_beyond_twice_the_window() {
        let h = 3600_i64;
        let full = config::FAIRVALUE_MIN_SIGMA_PER_SQRT_SEC;
        for secs_left in [2 * h, 6 * h, 20 * h] {
            let f = FairValueStrategyImpl::sigma_floor(h, secs_left);
            assert!((f - full).abs() < 1e-12, "T={secs_left}s should get the full floor, got {f:e}");
        }
        // Monotone ramp between the window and twice it — no discontinuity that
        // would reprice a market as it crosses the threshold.
        let mid = FairValueStrategyImpl::sigma_floor(h, h + h / 2);
        assert!(mid > FairValueStrategyImpl::sigma_floor(h, h));
        assert!(mid < full);
    }

    /// A steady model leaves the base edge in charge; a thrashing one does not.
    ///
    /// Both series below are real shapes from 2026-08-13/14. The quiet one is
    /// the 12AM ET market that the viper held to a +20% take profit; the violent
    /// one is the 1AM ET market where fair(YES) travelled 0.118 → 0.808 in six
    /// minutes and both entries stopped out against a ~10¢ claimed edge.
    #[test]
    fn noise_gate_separates_a_steady_model_from_a_thrashing_one() {
        let min = config::FAIRVALUE_EDGE_NOISE_MIN_SAMPLES;
        let base = config::FAIRVALUE_BASE_EDGE.to_f64().unwrap();

        // Drifting ~0.005 per 15s sample — the whole point of a fair-value model.
        let quiet: Vec<f64> = (0..min + 5).map(|i| 0.85 + i as f64 * 0.005).collect();
        let quiet_noise = FairValueStrategyImpl::fair_noise_from(&quiet, min).unwrap();
        assert!(
            quiet_noise < base,
            "a steadily drifting model must not veto its own base edge (noise {quiet_noise:.3} vs edge {base:.3})"
        );

        // Alternating ±0.15 — the model has no idea, and any "edge" read off it
        // is a coin flip paying two taker fees.
        let violent: Vec<f64> = (0..min + 5).map(|i| if i % 2 == 0 { 0.15 } else { 0.65 }).collect();
        let violent_noise = FairValueStrategyImpl::fair_noise_from(&violent, min).unwrap();
        assert!(
            violent_noise > base,
            "a thrashing model must veto the base edge (noise {violent_noise:.3} vs edge {base:.3})"
        );
    }

    /// The gate blocks rather than waves through while it has too little
    /// history: a freshly rotated market is exactly when the model is least
    /// stable, so an unmeasured edge there is the one least worth taking.
    #[test]
    fn noise_gate_is_closed_during_warmup() {
        let min = config::FAIRVALUE_EDGE_NOISE_MIN_SAMPLES;
        assert!(min >= 3, "a std-dev of successive diffs needs at least three samples");
        let short: Vec<f64> = (0..min - 1).map(|i| 0.5 + i as f64 * 0.001).collect();
        assert_eq!(FairValueStrategyImpl::fair_noise_from(&short, min), None);
        let just_enough: Vec<f64> = (0..min).map(|i| 0.5 + i as f64 * 0.001).collect();
        assert!(FairValueStrategyImpl::fair_noise_from(&just_enough, min).is_some());
    }

    /// Trade 368 (2026-08-15), the trade that motivated the veto, replayed from
    /// the recorded log line at the instant the stop fired:
    ///
    /// ```text
    /// 04:00:28  fair(YES)=0.380 ... no_ask=$0.44 edge=+0.145 | req=0.140
    /// ```
    ///
    /// Entered NO at $0.50, stopped out at $0.44 (−12%) after 206s with 3,800s
    /// still to run — then settled at $1.00. The model read entry-grade edge at
    /// the very moment the price rule was selling, which is exactly the state
    /// the veto exists to catch.
    #[test]
    fn model_confirmation_vetoes_the_stop_that_sold_a_winner() {
        let fair_no = 1.0 - 0.380;
        let no_ask = dec!(0.44);
        let req = dec!(0.140);

        let edge = FairValueStrategyImpl::stop_vetoed_by_model(Some(fair_no), no_ask, req, dec!(1.0))
            .expect("model still showed entry-grade edge — the stop must be vetoed");
        // Matches the +0.145 the viper itself logged at 04:00:28.
        assert_eq!(edge.round_dp(3), dec!(0.145));

        // A conservative profile demands 1.5x entry-grade edge and would still
        // have taken this stop — the knob genuinely spans both behaviours.
        assert!(
            FairValueStrategyImpl::stop_vetoed_by_model(Some(fair_no), no_ask, req, dec!(1.5))
                .is_none()
        );
    }

    /// Zero is the escape hatch: the veto disappears and the stop is price-only
    /// again, revertible from Control Tower without a redeploy.
    #[test]
    fn zero_confirm_restores_the_price_only_stop() {
        // An overwhelming edge that would certainly veto at any positive setting.
        assert!(
            FairValueStrategyImpl::stop_vetoed_by_model(Some(0.95), dec!(0.10), dec!(0.05), dec!(1.0))
                .is_some()
        );
        assert!(
            FairValueStrategyImpl::stop_vetoed_by_model(Some(0.95), dec!(0.10), dec!(0.05), dec!(0))
                .is_none()
        );
    }

    /// The veto must not fire on a thesis that has actually broken, nor on a
    /// missing model reading or an unquoted book — those all fall through to the
    /// stop, which is the safe direction.
    #[test]
    fn a_broken_thesis_still_stops_out() {
        let req = dec!(0.10);
        // Model has collapsed below the ask: no edge left, take the stop.
        assert!(
            FairValueStrategyImpl::stop_vetoed_by_model(Some(0.40), dec!(0.55), req, dec!(1.0))
                .is_none()
        );
        // Edge exists but is thinner than entry would require.
        assert!(
            FairValueStrategyImpl::stop_vetoed_by_model(Some(0.60), dec!(0.55), req, dec!(1.0))
                .is_none()
        );
        // No model reading at all (vol warmup) — never veto on absent evidence.
        assert!(
            FairValueStrategyImpl::stop_vetoed_by_model(None, dec!(0.44), req, dec!(1.0)).is_none()
        );
        // Vaporised / unquoted book.
        for ask in [dec!(0), dec!(1)] {
            assert!(
                FairValueStrategyImpl::stop_vetoed_by_model(Some(0.95), ask, req, dec!(1.0))
                    .is_none()
            );
        }
    }

    /// Setting the knob to zero restores the old unconditional floor, so the
    /// change can be reverted live without a redeploy.
    #[test]
    fn zero_horizon_restores_the_unconditional_floor() {
        let full = config::FAIRVALUE_MIN_SIGMA_PER_SQRT_SEC;
        for secs_left in [60_i64, 932, 100_000] {
            assert_eq!(FairValueStrategyImpl::sigma_floor(0, secs_left), full);
        }
    }

    /// Percentage stops need to span more than a tick to mean anything, and the
    /// round-trip fee is `14·(1−p)%` of entry price — both argue against the
    /// cheap tail. Trade 355 entered at 11¢, where a 12% stop is 1.3 ticks.
    #[test]
    fn min_entry_price_keeps_the_stop_clear_of_tick_noise() {
        let min_entry = config::FAIRVALUE_MIN_ENTRY_PRICE.to_f64().unwrap();
        let stop = config::FAIRVALUE_STOP_LOSS_PERCENT.to_f64().unwrap();
        let tick = 0.01_f64;
        let ticks = min_entry * stop / tick;
        assert!(ticks >= 2.5, "stop is only {ticks:.1} ticks wide at the minimum entry price");
        // And the round-trip toll must stay under the take-profit target.
        let toll = 2.0 * config::CRYPTO_FEE_RATE.to_f64().unwrap() * (1.0 - min_entry);
        let tp = config::FAIRVALUE_TARGET_PROFIT_PERCENT.to_f64().unwrap();
        assert!(toll < tp, "round-trip toll {toll:.3} must leave something under a {tp:.2} TP");
    }

    /// A stop-out that is re-emitted while its exit retries must count once.
    ///
    /// Reproduces 2026-08-13 on the 4PM BTC market: the catastrophic stop's FAK
    /// missed at $0.33 with no buyers, so the position correctly stayed in the
    /// map. Dispatch was then throttled for `EXIT_RETRY_COOLDOWN_SECS` (5s)
    /// while `evaluate_exit` kept firing at the 50ms patrol tick, driving
    /// `sl_counts` from 1 to 101 and emitting 100 breaker WARNs in five seconds
    /// — against a configured limit of 2.
    #[test]
    fn a_retried_stop_out_counts_once_not_once_per_tick() {
        let strat = FairValueStrategyImpl::new();
        let asset = "TEST-SL-RETRY";
        let (token, condition) = ("tok-4pm", "cond-4pm");
        let opened_at = Utc::now();

        // First emission counts.
        assert_eq!(
            strat.count_stop_loss_once(asset, token, opened_at, condition),
            Some(1),
        );
        // 100 re-emissions over the retry window must all be suppressed.
        for _ in 0..100 {
            assert_eq!(
                strat.count_stop_loss_once(asset, token, opened_at, condition),
                None,
                "a retried exit is the same stop-out"
            );
        }
        // The dedupe must have counted exactly one stop-out, whatever the
        // profile's breaker limit happens to be.
        assert_eq!(
            strat.count_stop_loss_once(asset, token, opened_at + chrono::Duration::seconds(1), condition),
            Some(2),
            "only the first emission was deduped"
        );
    }

    /// A genuine second stop-out on the same token must still count — the
    /// dedupe keys on the position's open instant, not the token alone, so
    /// re-entering a market and stopping again trips the breaker as designed.
    #[test]
    fn a_re_entered_position_counts_again() {
        let strat = FairValueStrategyImpl::new();
        let asset = "TEST-SL-REENTRY";
        let (token, condition) = ("tok-5pm", "cond-5pm");

        let first = Utc::now();
        let second = first + chrono::Duration::seconds(600); // re-entry later
        assert_eq!(strat.count_stop_loss_once(asset, token, first, condition), Some(1));
        assert_eq!(strat.count_stop_loss_once(asset, token, first, condition), None);
        assert_eq!(
            strat.count_stop_loss_once(asset, token, second, condition),
            Some(2),
            "a new position on the same token is a new stop-out"
        );
        assert!(
            2 >= crate::helpers::dynamic_config::DynamicConfig::default().fairvalue_max_stop_losses_per_market,
            "breaker trips no later than the second stop-out"
        );
    }

    /// The breaker is per market: stop-outs on one condition_id must not bleed
    /// into another's count.
    #[test]
    fn stop_loss_counts_are_scoped_per_market() {
        let strat = FairValueStrategyImpl::new();
        let asset = "TEST-SL-SCOPE";
        let now = Utc::now();
        assert_eq!(strat.count_stop_loss_once(asset, "tok-a", now, "cond-a"), Some(1));
        assert_eq!(strat.count_stop_loss_once(asset, "tok-b", now, "cond-b"), Some(1));
    }

    #[test]
    fn unavailable_edge_sentinel_is_formattable() {
        let s = format!("{:+.3}", NO_EDGE);
        assert!(!s.is_empty());
        // Must still sort below every real edge, which lives in [-1.0, 1.0].
        assert!(NO_EDGE < dec!(-1));
    }

    /// The coin-flip floor must reject the two entries that actually lost money
    /// on 2026-08-10, and must not depend on time to expiry.
    #[test]
    fn coin_flip_floor_rejects_the_real_losing_entries() {
        // (d_sigma, secs_left) as logged at entry. Both were >25 min from expiry,
        // so the endgame pin guard (600s) could not see either one.
        for (d_sigma, secs_left) in [(0.20_f64, 1578_i64), (0.07, 2477)] {
            let endgame_pin = secs_left < config::FAIRVALUE_PIN_GUARD_SECS
                && d_sigma.abs() < config::FAIRVALUE_PIN_MIN_SIGMA;
            let coin_flip = d_sigma.abs() < config::FAIRVALUE_MIN_ABS_SIGMA;
            assert!(!endgame_pin, "endgame guard should not fire at T={secs_left}s");
            assert!(coin_flip, "coin-flip floor must reject d={d_sigma}σ");
        }
    }

    /// A settlement snipe — the strategy's whole reason for a wide price band —
    /// must survive the new floor.
    #[test]
    fn coin_flip_floor_admits_high_conviction_entries() {
        let d_sigma = 2.5_f64;
        assert!(d_sigma.abs() >= config::FAIRVALUE_MIN_ABS_SIGMA);
    }

    /// Edge must be charged both legs' fees, so the hurdle is strictly higher
    /// than the old entry-only calculation.
    #[test]
    fn edge_is_net_of_round_trip_fees() {
        // Trade #6: fair(YES)=0.581, ask $0.36.
        let fair = dec!(0.581);
        let ask = dec!(0.36);
        let entry_only = fair - ask - FairValueStrategyImpl::fee_frac(ask);
        let round_trip = entry_only - FairValueStrategyImpl::fee_frac(fair);
        assert!(round_trip < entry_only, "round-trip hurdle must exceed entry-only");
        // The exit fee is material, not a rounding artifact.
        assert!(entry_only - round_trip > dec!(0.01));
    }

    /// Guards the whole diagnostic line, not just the constant — this is the
    /// exact format string and argument shape that panicked.
    #[test]
    fn diagnostic_line_renders_with_both_edges_unavailable() {
        let line = format!(
            " FairValue: fair(YES)={:.3} (d={:+.2}σ, σ/√s={:.2e}, T={}s) | yes_ask=${:.2} edge={:+.3} | no_ask=${:.2} edge={:+.3} | req={:.3}{}",
            0.5_f64, 0.0_f64, 5.0e-5_f64, 2598_i64,
            dec!(0), NO_EDGE, dec!(0), NO_EDGE, dec!(0.12), "",
        );
        assert!(line.contains("FairValue"));
    }
}
