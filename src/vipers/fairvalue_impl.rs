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
/// 3. Model reversal: our side's fair probability < MODEL_REVERSAL_EXIT_PROB —
///    the thesis is gone, exit without waiting for the SL.
/// 4. Endgame bail-out: final BAIL_SECS with fair probability < BAIL_PROB.

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use chrono::Utc;
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

    /// Feed the vol sampler and return σ per √second, or None during warmup /
    /// frozen oracle.
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
            // σ floor: a quiet sampling hour must not make the model delusional
            // about a multi-hour horizon (see FAIRVALUE_MIN_SIGMA_PER_SQRT_SEC).
            .map(|s| s.max(config::FAIRVALUE_MIN_SIGMA_PER_SQRT_SEC))
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

    fn cooldown_active(&self, asset: &str, token_id: &str) -> bool {
        let mut reg = match globals(asset).exit_cooldowns.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if let Some(t) = reg.get(token_id) {
            if (t.elapsed().as_secs() as i64) < config::FAIRVALUE_POST_EXIT_COOLDOWN_SECS {
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
    }

    /// Recompute the model's current fair probability for a held token's side.
    /// None when the model can't price (no strike/vol/time).
    fn fair_prob_for_side(
        &self,
        asset: &str,
        market: &MarketConfig,
        snapshot: &MarketSnapshot,
        token_is_yes: bool,
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
            .max(config::FAIRVALUE_MIN_SIGMA_PER_SQRT_SEC);
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

        // ── Venue: prefer the Window/Daily maker venue (lower fees, has strike) ──
        let (market, snap) = if let (Some(mk_mkt), Some(mk_snap)) = (&ctx.maker_market, &ctx.maker_snapshot) {
            (mk_mkt, mk_snap)
        } else {
            (&ctx.market, &ctx.snapshot)
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
        let sigma = match sigma_opt {
            Some(s) => s, // warmup complete, oracle alive
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
                    " FairValue: fair(YES)={:.3} (d={:+.2}σ, σ/√s={:.2e}, T={}s) | yes_ask=${:.2} edge={:+.3} | no_ask=${:.2} edge={:+.3} | req={:.3}{}",
                    fair_yes, d_sigma, sigma, secs_left,
                    snap.yes_ask, yes_edge, snap.no_ask, no_edge, req_edge,
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
        if self.cooldown_active(&ctx.crypto_filter, token_id.as_str()) {
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
                >= config::FAIRVALUE_MAX_STOP_LOSSES_PER_MARKET
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
            let fair_side = self.fair_prob_for_side(&ctx.crypto_filter, market, snap, token_is_yes);

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
            if secs_held >= 60
                && bid >= config::FAIRVALUE_MIN_EXIT_BID
                && fair_side.map_or(false, |p| p < config::FAIRVALUE_MODEL_REVERSAL_EXIT_PROB)
            {
                self.arm_cooldown(&ctx.crypto_filter, token_id.as_str());
                return Ok(StrategySignal::Exit {
                    params: exit_params(bid),
                    reason: format!(
                        "FairValueReversal: fair={:.3} < {:.2}, bid=${:.4} ({:+.2}%)",
                        fair_side.unwrap_or(0.0), config::FAIRVALUE_MODEL_REVERSAL_EXIT_PROB,
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
                if bid < config::FAIRVALUE_MIN_EXIT_BID {
                    // Unfillable — an FAK into a vaporised bid just floods logs.
                    continue;
                }
                self.arm_cooldown(&ctx.crypto_filter, token_id.as_str());
                {
                    let mut counts = match globals(&ctx.crypto_filter).sl_counts.lock() {
                        Ok(g) => g,
                        Err(p) => p.into_inner(),
                    };
                    let n = counts.entry(market.condition_id.clone()).or_insert(0);
                    *n += 1;
                    if *n >= config::FAIRVALUE_MAX_STOP_LOSSES_PER_MARKET {
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
