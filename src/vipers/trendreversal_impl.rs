/// TrendCapture / TrendReversal Strategy — Oracle-Drift Fade on Window/Daily Markets
///
/// # Thesis
///
/// Live results (60 trades, 22% win, −$19.74) showed that *following* a strong,
/// confirmed oracle drift on these binary markets loses: by the time the 10m AND
/// 60m windows both confirm a move, it is already priced into the YES/NO token and
/// tends to **mean-revert**. A same-entry / opposite-token study over those 60
/// trades flipped the record to 73% wins / +$14.46. So the strategy now **fades**
/// the drift instead of riding it.
///
/// Entry trigger (unchanged): BTC has moved meaningfully over both the 10-minute
/// AND 60-minute windows (sustained, confirmed drift), the token is in the tradable
/// price band, and spot is meaningfully away from the strike.
///
/// Direction (flipped by `config::TRENDREVERSAL_MODE`, default true):
///   - Strong UP drift   → BUY NO  (fade the priced-in up-move)
///   - Strong DOWN drift → BUY YES (fade the priced-in down-move)
///   Set `TRENDREVERSAL_MODE = false` to restore the legacy trend-FOLLOWING
///   behaviour (UP→YES, DOWN→NO).
///
/// Example — BTC crash (fade mode):
///   drift_10m = −$150, drift_60m = −$400 (confirmed downtrend, already priced in)
///   YES on the daily market is cheap (the crash is in the price)
///   → BUY YES, targeting the mean-reversion bounce (wide TP) with a tight stop for
///     when the downtrend instead continues (thesis wrong).
///
/// # Exits (fade mode)
///   Asymmetric "let winners run": wide take-profit (TRENDREVERSAL_TARGET_PROFIT_PCT)
///   to capture the reversion, tight stop (TRENDREVERSAL_STOP_LOSS_PCT) because the
///   failure mode — the trend continuing — is fast. An always-on catastrophic stop
///   (TRENDCAPTURE_CATASTROPHIC_SL_PCT) backstops gap-throughs regardless of the
///   min-hold window. The trend-FOLLOWING reversal exit is disabled in fade mode (a
///   drift flip there is the thesis playing out, not a reason to bail).
///
/// # Venue
///   Window or Daily market (uses `maker_market` / `maker_snapshot` when available;
///   falls back to the hourly market if no window/daily is configured).
///   A longer expiry gives the trade more time to develop before forced resolution.
///
/// # Naming
///   Strategy id is "TrendReversalStrategy" — surfaced in the `trades.strategy`
///   column, the tradelog UI, the config panel, and the startup log. The exit and
///   exposure filters also accept the legacy "TrendCaptureStrategy" tag so any
///   position opened under the old name survives the rename across a deploy.
///   Two internal storage keys are intentionally kept stable to avoid orphaning
///   persisted tuning: the `viper_kind` taxonomy id ("trendcapture") and the
///   DynamicConfig fields (`enable_trendcapture`, `trendcapture_*`) embedded in
///   per-squadron `squadron_configs` JSON. These are identifiers only; their UI
///   display labels read "TrendReversal".
///
/// # Cooldown & re-entry
///   Per-token post-exit cooldown prevents rapid re-entry after a loss.
///   Uses a `HashMap<MarketId, Instant>` protected by `std::sync::Mutex` (no async holds).

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use crate::venues::core::MarketId;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;
use chrono::Utc;
use tracing::debug;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams};
use crate::vipers::is_drawdown_limit_hit;
use crate::config;
use crate::venues::core::TimeInForce;

// ─── Stateful implementation ─────────────────────────────────────────────────

pub struct TrendReversalStrategyImpl {
    /// Per-token cooldown after any exit.
    /// Key: token_id, Value: Instant of last exit.
    post_exit_cooldown: Mutex<HashMap<MarketId, Instant>>,
    /// Consecutive SL loss count per market condition_id.
    /// Resets to 0 on a TP exit; increments on every SL/forced exit.
    consecutive_losses: Mutex<HashMap<String, u32>>,
    /// Viper-level exit signal cooldown.
    /// After evaluate_exit emits an Exit, this is set to now().
    /// Further Exit signals are suppressed for TRENDCAPTURE_EXIT_SIGNAL_COOLDOWN_SECS.
    last_exit_signal_at: Mutex<Option<Instant>>,
}

impl TrendReversalStrategyImpl {
    pub fn new() -> Self {
        Self {
            post_exit_cooldown:  Mutex::new(HashMap::new()),
            consecutive_losses:  Mutex::new(HashMap::new()),
            last_exit_signal_at: Mutex::new(None),
        }
    }
}

impl Default for TrendReversalStrategyImpl {
    fn default() -> Self { Self::new() }
}

// ─── Entry evaluation ─────────────────────────────────────────────────────────

#[async_trait]
impl Strategy for TrendReversalStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        if !dc.enable_trendcapture {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Global drawdown guard ────────────────────────────────────────────
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Venue selection: prefer Window/Daily ─────────────────────────────
        // TrendCapture needs a longer expiry for multi-minute drift trades.
        let (market, snap) = if let (Some(mk), Some(ms)) = (&ctx.maker_market, &ctx.maker_snapshot) {
            (mk, ms)
        } else {
            (&ctx.market, &ctx.snapshot)
        };

        // ── Snapshot staleness gate ───────────────────────────────────────────
        let snap_age = (Utc::now() - snap.timestamp).num_seconds();
        if snap_age > config::TRENDCAPTURE_MAX_SNAPSHOT_AGE_SECS {
            debug!(" TrendCapture blocked: snapshot stale ({}s > {}s)",
                snap_age, config::TRENDCAPTURE_MAX_SNAPSHOT_AGE_SECS);
            return Ok(StrategySignal::NoSignal);
        }

        // ── Oracle staleness guard ────────────────────────────────────────────
        // If the 10m drift is still zero the oracle hasn't had 10 minutes of history
        // yet — we have no trend signal to trade on.
        let drift_10m = ctx.snapshot.oracle_drift_10m;
        let drift_60m = ctx.snapshot.oracle_drift_60m;
        if drift_10m == dec!(0) {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Expiry guard ──────────────────────────────────────────────────────
        let secs_left = if let Some(close_time) = market.market_close_time {
            let s = (close_time - Utc::now()).num_seconds();
            if s < config::TRENDCAPTURE_MIN_SECS_TO_EXPIRY {
                debug!(" TrendCapture blocked: only {}s to expiry (min {}s)",
                    s, config::TRENDCAPTURE_MIN_SECS_TO_EXPIRY);
                return Ok(StrategySignal::NoSignal);
            }
            // Upper bound: a 10m/60m drift signal has no directional relevance to a
            // market resolving many hours out. Block far-from-expiry entries — the
            // dominant loss source in production (61% of entries were >12h out).
            if s > config::TRENDCAPTURE_MAX_SECS_TO_EXPIRY {
                debug!(" TrendCapture blocked: {}s to expiry exceeds max {}s (drift signal horizon mismatch)",
                    s, config::TRENDCAPTURE_MAX_SECS_TO_EXPIRY);
                return Ok(StrategySignal::NoSignal);
            }
            Some(s)
        } else {
            None
        };

        // ── Late-market min price floor ───────────────────────────────────────
        // In the last 2h before close, markets are near-resolved; require higher
        // price floor to avoid buying into decided outcomes.
        let effective_min_price = match secs_left {
            Some(s) if s < config::TRENDCAPTURE_LATE_MARKET_SECS =>
                config::TRENDCAPTURE_LATE_MARKET_MIN_ENTRY_PRICE,
            _ => dc.trendcapture_min_entry_price,
        };

        // ── Market warmup gate ────────────────────────────────────────────────
        let secs_since_market_start = (Utc::now() - ctx.market_started_at).num_seconds();
        if secs_since_market_start < config::TRENDCAPTURE_MARKET_WARMUP_SECS {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Spread gate ───────────────────────────────────────────────────────
        let ask_sum = snap.yes_ask + snap.no_ask;
        if ask_sum > dc.trendcapture_max_entry_ask_sum {
            debug!(" TrendCapture spread gate: ask_sum={:.3} > max {:.3} — book too wide",
                ask_sum, dc.trendcapture_max_entry_ask_sum);
            return Ok(StrategySignal::NoSignal);
        }

        // ── Minimum price floor ───────────────────────────────────────────────
        let yes_ask = snap.yes_ask;
        let no_ask  = snap.no_ask;
        if yes_ask < effective_min_price && no_ask < effective_min_price {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Exposure guard ────────────────────────────────────────────────────
        let current_exposure = {
            let pos_map = ctx.positions.lock().await;
            pos_map.iter()
                // Match this strategy's own positions. Accept the legacy
                // "TrendCaptureStrategy" tag too so any position opened under the old
                // name (pre-rename) is still exposure-counted across a deploy.
                .filter(|((s, _), _)| s == "TrendReversalStrategy" || s == "TrendCaptureStrategy")
                .map(|(_, p)| p.shares * p.avg_entry)
                .sum::<Decimal>()
        };
        if current_exposure >= dc.trendcapture_max_exposure_usdc {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Determine thresholds via oracle-relative scaling ─────────────────
        // All thresholds are expressed as a fraction of the current oracle price,
        // so they stay proportionally calibrated as BTC/ETH/SOL prices change.
        let oracle_price = ctx.snapshot.oracle_price;
        // TrendReversal: the exhaustion multiplier raises the entry trigger so we
        // only fade genuinely over-extended moves (1.0 = legacy trigger).
        let exhaust_mult = if dc.trendreversal_mode {
            config::TRENDREVERSAL_EXHAUSTION_MULT
        } else { dec!(1.0) };
        let bull_drift_10m_thr = config::oracle_threshold(config::TRENDCAPTURE_DRIFT_10M_PCT, oracle_price) * exhaust_mult;
        let bear_drift_10m_thr = -bull_drift_10m_thr;
        let bull_strike_gap    = config::oracle_threshold(dc.trendcapture_strike_gap_pct, oracle_price);
        let bear_strike_gap    = bull_strike_gap;
        let exhaustion_thr     = if dc.trendreversal_mode {
            // Fade mode: tighter falling-knife ceiling — block extreme drift where
            // the move is momentum (keeps running) rather than exhaustion (reverts).
            config::oracle_threshold(config::TRENDREVERSAL_FADE_MAX_DRIFT_60M_PCT, oracle_price)
        } else {
            config::oracle_threshold(config::TRENDCAPTURE_EXHAUSTION_DRIFT_60M_PCT, oracle_price)
        };

        // ── Persistent cross-restart cascade guard ────────────────────────────
        // The in-memory post_exit_cooldown map (checked below) is WIPED on every
        // redeploy/restart. On 2026-07-02 the bot restarted mid-cascade (23:56,
        // 00:09 EDT) and re-entered the SAME losing YES fade 3× as BTC fell
        // 0.50→0.43 — the cooldown that should have blocked it was cleared each
        // restart. Persist the guard in the trades table: if this market+side
        // already stopped out within the cooldown window, stand aside regardless
        // of in-memory state. Runs only when |drift_10m| clears the entry trigger
        // (rare), so the query is cheap. Placed before the std-mutex cooldown locks
        // below so no guard is held across this await.
        if dc.trendreversal_mode {
            let intended_side = if drift_10m <= bear_drift_10m_thr {
                Some("YES")   // drift DOWN → fade UP → buy YES
            } else if drift_10m >= bull_drift_10m_thr {
                Some("NO")    // drift UP → fade DOWN → buy NO
            } else {
                None
            };
            if let Some(side) = intended_side {
                let pool = crate::helpers::db::pool_for(&ctx.crypto_filter.to_lowercase())
                    .or_else(|| crate::helpers::db::pool().cloned());
                if let Some(pool) = pool {
                    let blocked = crate::helpers::db::recent_stop_loss_exists(
                        &pool,
                        &market.market_name,
                        side,
                        config::TRENDCAPTURE_POST_EXIT_COOLDOWN_SECS,
                    ).await;
                    if blocked {
                        debug!(" TrendReversal cascade guard: recent SL on '{}' {} within {}s — standing aside (persistent, survives restart)",
                            market.market_name, side, config::TRENDCAPTURE_POST_EXIT_COOLDOWN_SECS);
                        return Ok(StrategySignal::NoSignal);
                    }
                }
            }
        }
        // A 10m surge that runs counter to the 60m macro direction is a dip/bounce
        // in the larger trend, not a new trend — high whipsaw risk.  Block the entry
        // when the 60m drift meaningfully opposes the intended direction.
        //
        // Example that caused the Jun 18 loss:
        //   drift_10m = −$131  → BEAR signal fired
        //   drift_60m = +$200  → 60m macro was BULLISH (dip in uptrend)
        //   Result: entered NO, reversed within 7 minutes for −6.25%
        //
        // With this gate: BEAR entry requires drift_60m < +alignment_thr.
        let align_thr = config::oracle_threshold(config::TRENDCAPTURE_DRIFT_60M_PCT, oracle_price);
        let drift_60m_misaligned_bull = drift_60m <= -align_thr;   // 60m macro is bearish — don't go BULL
        let drift_60m_misaligned_bear = drift_60m >=  align_thr;   // 60m macro is bullish — don't go BEAR

        // ── 60m drift exhaustion ceiling ─────────────────────────────────────
        // Block when the 60m move is so large the trend is already exhausted
        // (tail-end capitulation risk).
        let drift_60m_blocks_bull = drift_60m >= exhaustion_thr;
        let drift_60m_blocks_bear = drift_60m <= -exhaustion_thr;

        // ── Hard 60m regime confirmation (rework 2026-06-28) ──────────────────
        // Require the 60m drift to actively AGREE with the 10m entry direction by
        // at least align_thr, not merely "not oppose" it. This stands the strategy
        // aside in chop (10m spike + flat 60m), the regime that produced the 22%
        // win rate. Gated by TRENDCAPTURE_REQUIRE_60M_CONFIRMATION.
        let drift_60m_confirms_bull = !config::TRENDCAPTURE_REQUIRE_60M_CONFIRMATION
            || drift_60m >= align_thr;
        let drift_60m_confirms_bear = !config::TRENDCAPTURE_REQUIRE_60M_CONFIRMATION
            || drift_60m <= -align_thr;

        // ── OBI adverse-direction veto ────────────────────────────────────────
        let yes_total_depth = snap.yes_bid_depth + snap.yes_ask_depth;
        let no_total_depth  = snap.no_bid_depth  + snap.no_ask_depth;
        let yes_obi = if yes_total_depth > dec!(0) {
            (snap.yes_bid_depth - snap.yes_ask_depth) / yes_total_depth
        } else { dec!(-1.0) };
        let no_obi = if no_total_depth > dec!(0) {
            (snap.no_bid_depth - snap.no_ask_depth) / no_total_depth
        } else { dec!(-1.0) };

        let obi_blocks_bull = yes_obi < dc.trendcapture_obi_adverse_block;
        let obi_blocks_bear = no_obi  < dc.trendcapture_obi_adverse_block;
        let obi_exhausted_bull = yes_obi > dc.trendcapture_obi_exhaustion_block;
        let obi_exhausted_bear = no_obi  > dc.trendcapture_obi_exhaustion_block;

        // ── Strike price distance requirement ─────────────────────────────────
        let binance_price = ctx.snapshot.oracle_price;
        let strike_price  = market.strike_price;

        // ── Per-token post-exit cooldown ──────────────────────────────────────
        // Checked inside each entry path below.
        let cooldowns = self.post_exit_cooldown.lock().unwrap();
        let consec    = self.consecutive_losses.lock().unwrap();

        // Helper: effective cooldown for a token — extended after consecutive losses
        let effective_cooldown = |condition_id: &str| -> u64 {
            let losses = consec.get(condition_id).copied().unwrap_or(0);
            if losses >= config::TRENDCAPTURE_CONSECUTIVE_LOSS_THRESHOLD {
                config::TRENDCAPTURE_CONSECUTIVE_LOSS_COOLDOWN_SECS as u64
            } else {
                config::TRENDCAPTURE_POST_EXIT_COOLDOWN_SECS as u64
            }
        };

        // ── Position sizing ───────────────────────────────────────────────────
        let trade_size = |drift_abs: Decimal| -> Decimal {
            // Flat sizing when Kelly is disabled — never upsize into a fade.
            if !config::ENABLE_KELLY_SIZING {
                return dc.trendcapture_min_trade_size_usdc;
            }
            let thr = bull_drift_10m_thr.abs().max(Decimal::ONE);
            let strength = (drift_abs / thr)
                .max(Decimal::ONE)
                .min(config::TRENDCAPTURE_KELLY_MAX_MULTIPLIER);
            let fraction = (strength - Decimal::ONE) / (config::TRENDCAPTURE_KELLY_MAX_MULTIPLIER - Decimal::ONE);

            // Scale trade sizing down by 50% to mitigate risk while working with wider stop-losses
            let base_size = dc.trendcapture_min_trade_size_usdc + fraction * (dc.trendcapture_max_trade_size_usdc - dc.trendcapture_min_trade_size_usdc);
            base_size * dec!(0.50)
        };

        // ── Macro: entry OrderParams ───────────────────────────────────────────
        // TrendCapture is a trend-FOLLOWING strategy, so entries must fill *while the
        // drift signal is still live*. A passive `post_only` maker bid (ask − 0.01)
        // only fills when a counterparty SELLS into it — which on a live directional
        // move only happens once the move stalls/reverses. That adverse selection
        // systematically filled us right at the local top/bottom (e.g. Jun 21 trade
        // id 68: rested ~5 min, filled exactly as BTC reversed → instant −13.5% SL).
        //
        // Entries are therefore marketable FAK takers: `price` is set to the touch
        // (ask), patrol adds BUY_PRICE_OFFSET so the order crosses, and FAK fills
        // immediately or kills (no resting order to be adversely selected). The
        // `ask_sum ≤ 1.04` and per-token `spread ≤ 12%` gates above cap the cross cost.
        macro_rules! entry_params {
            ($token:expr, $price:expr, $fee:expr, $size:expr) => {
                OrderParams {
                    token_id:     $token,
                    price:        $price,
                    shares:       $size / $price,
                    fee_bps:      $fee,
                    is_neg_risk:  market.is_neg_risk,
                    market_name:  market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                    order_type:   TimeInForce::Fak,
                    post_only:    false,
                    ghost_mode:   dc.ghost_mode,
                }
            };
        }

        // ── Derivatives confirmation gate (Derivatives Raptor) ───────────────
        // Block the trend entry when the perp book actively contradicts it:
        // aggressive counter-taker flow (cvd) or hard OI unwind (de-leveraging /
        // squeeze → trend exhaustion). Disabled by default; inert on no-data
        // (cvd/oi = 0 → neutral). All-asset. Mirrors the drift-alignment gates.
        let deriv_cvd = ctx.snapshot.cvd_ratio;
        let deriv_oi_unwind = config::DERIV_GATE_ENABLED
            && ctx.snapshot.oi_delta_pct <= config::DERIV_OI_UNWIND_BLOCK;
        let deriv_blocks_bull = config::DERIV_GATE_ENABLED
            && (deriv_oi_unwind
                || (deriv_cvd > dec!(0) && deriv_cvd <= dec!(1) - config::DERIV_CVD_CONFIRM_MARGIN));
        let deriv_blocks_bear = config::DERIV_GATE_ENABLED
            && (deriv_oi_unwind
                || (deriv_cvd > dec!(0) && deriv_cvd >= dec!(1) + config::DERIV_CVD_CONFIRM_MARGIN));

        // ══ BULL entry: buy YES when trend is strongly upward ════════════════
        if drift_10m >= bull_drift_10m_thr
            && !drift_60m_misaligned_bull
            && drift_60m_confirms_bull
            && !drift_60m_blocks_bull
            && !obi_blocks_bull
            && !obi_exhausted_bull
            && !deriv_blocks_bull
        {
            // Strike gap check
            let passes_gap = match strike_price {
                Some(strike) => binance_price >= strike + bull_strike_gap,
                None => true, // no strike data → rely on drift signal alone
            };

            if passes_gap
                && yes_ask >= effective_min_price
                && yes_ask <= dc.trendcapture_max_entry_price
            {
                // ── TrendReversal: fade-token selection ───────────────────────
                // A strong, confirmed UP drift is already priced in and tends to
                // mean-revert on these binaries — so BUY NO (fade) instead of YES.
                // (TRENDREVERSAL_MODE=false restores trend-following: buy YES.)
                let (buy_token, buy_ask, buy_bid, buy_bid_depth, buy_fee) = if dc.trendreversal_mode {
                    (market.no_token.clone(),  no_ask,  snap.no_bid,  snap.no_bid_depth,  market.no_fee_bps as u16)
                } else {
                    (market.yes_token.clone(), yes_ask, snap.yes_bid, snap.yes_bid_depth, market.yes_fee_bps as u16)
                };

                // Per-token spread gate on the BOUGHT token: a hollow bid side
                // guarantees an instant stop-out (SL is measured against the bid).
                let buy_spread = if buy_ask > dec!(0) {
                    (buy_ask - buy_bid) / buy_ask
                } else { Decimal::ONE };
                if buy_spread > dc.trendcapture_max_token_spread_pct {
                    debug!(" TrendReversal BULL→fade blocked: bought-token spread {:.1}% > max {:.1}% (ask={:.3} bid={:.3}) — hollow bid would force instant SL",
                        buy_spread * dec!(100), dc.trendcapture_max_token_spread_pct * dec!(100), buy_ask, buy_bid);
                    return Ok(StrategySignal::NoSignal);
                }

                // Cooldown check (keyed by the bought token)
                let token_id = buy_token;
                let cdl = effective_cooldown(&market.condition_id);
                let in_cooldown = cooldowns.get(&token_id)
                    .map(|t| t.elapsed().as_secs() < cdl)
                    .unwrap_or(false);
                if !in_cooldown {
                    let size = trade_size(drift_10m.abs());
                    let entry_price = buy_ask;
                    // Liquidity / near-resolution gate: don't enter where a stop
                    // would gap through (thin exit bid or too close to close).
                    let intended_shares = if entry_price > dec!(0) { size / entry_price } else { dec!(0) };
                    if let Some(reason) = crate::vipers::entry_liquidity_gate(secs_left, intended_shares, buy_bid_depth) {
                        debug!(" TrendReversal BULL→fade blocked: {}", reason);
                        return Ok(StrategySignal::NoSignal);
                    }
                    debug!(" TrendReversal BULL→fade entry (drift UP, buying NO): drift_10m={:.0} drift_60m={:.0} align_thr={:.0} buy_ask={:.3} entry={:.3} size={:.2}",
                        drift_10m, drift_60m, align_thr, buy_ask, entry_price, size);
                    drop(cooldowns);
                    drop(consec);
                    return Ok(StrategySignal::Entry {
                        params: entry_params!(token_id, entry_price, buy_fee, size),
                        pair_params: None,
                    });
                }
            }
        }

        // ══ BEAR entry: buy NO when trend is strongly downward ═══════════════
        if drift_10m <= bear_drift_10m_thr
            && !drift_60m_misaligned_bear
            && drift_60m_confirms_bear
            && !drift_60m_blocks_bear
            && !obi_blocks_bear
            && !obi_exhausted_bear
            && !deriv_blocks_bear
        {
            let passes_gap = match strike_price {
                Some(strike) => binance_price <= strike - bear_strike_gap,
                None => true,
            };

            if passes_gap
                && no_ask >= effective_min_price
                && no_ask <= dc.trendcapture_max_entry_price
            {
                // ── TrendReversal: fade-token selection ───────────────────────
                // A strong, confirmed DOWN drift is already priced in and tends to
                // mean-revert — so BUY YES (fade) instead of NO.
                // (TRENDREVERSAL_MODE=false restores trend-following: buy NO.)
                let (buy_token, buy_ask, buy_bid, buy_bid_depth, buy_fee) = if dc.trendreversal_mode {
                    (market.yes_token.clone(), yes_ask, snap.yes_bid, snap.yes_bid_depth, market.yes_fee_bps as u16)
                } else {
                    (market.no_token.clone(),  no_ask,  snap.no_bid,  snap.no_bid_depth,  market.no_fee_bps as u16)
                };

                // Per-token spread gate on the BOUGHT token (see Jun 20 id 51:
                // NO ask 0.326 / bid 0.241 = 26% spread → instant −23% stop-out).
                let buy_spread = if buy_ask > dec!(0) {
                    (buy_ask - buy_bid) / buy_ask
                } else { Decimal::ONE };
                if buy_spread > dc.trendcapture_max_token_spread_pct {
                    debug!(" TrendReversal BEAR→fade blocked: bought-token spread {:.1}% > max {:.1}% (ask={:.3} bid={:.3}) — hollow bid would force instant SL",
                        buy_spread * dec!(100), dc.trendcapture_max_token_spread_pct * dec!(100), buy_ask, buy_bid);
                    return Ok(StrategySignal::NoSignal);
                }

                let token_id = buy_token;
                let cdl = effective_cooldown(&market.condition_id);
                let in_cooldown = cooldowns.get(&token_id)
                    .map(|t| t.elapsed().as_secs() < cdl)
                    .unwrap_or(false);
                if !in_cooldown {
                    let size = trade_size(drift_10m.abs());
                    let entry_price = buy_ask;
                    // Liquidity / near-resolution gate: don't enter where a stop
                    // would gap through (thin exit bid or too close to close).
                    let intended_shares = if entry_price > dec!(0) { size / entry_price } else { dec!(0) };
                    if let Some(reason) = crate::vipers::entry_liquidity_gate(secs_left, intended_shares, buy_bid_depth) {
                        debug!(" TrendReversal BEAR→fade blocked: {}", reason);
                        return Ok(StrategySignal::NoSignal);
                    }
                    debug!(" TrendReversal BEAR→fade entry (drift DOWN, buying YES): drift_10m={:.0} drift_60m={:.0} align_thr={:.0} buy_ask={:.3} entry={:.3} size={:.2}",
                        drift_10m, drift_60m, align_thr, buy_ask, entry_price, size);
                    drop(cooldowns);
                    drop(consec);
                    return Ok(StrategySignal::Entry {
                        params: entry_params!(token_id, entry_price, buy_fee, size),
                        pair_params: None,
                    });
                }
            }
        }

        drop(cooldowns);
        drop(consec);
        Ok(StrategySignal::NoSignal)
    }

    // ─── Exit evaluation ──────────────────────────────────────────────────────

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;

        // ── Soft-exit cooldown (reversal / take-profit only) ──────────────────
        // After we emit a *discretionary* Exit signal, suppress further discretionary
        // re-fires for EXIT_SIGNAL_COOLDOWN_SECS so patrol has time to execute before we
        // re-fire (prevents FAK-miss re-fire storms).
        //
        // CRITICAL: safety-critical exits (stop-loss, catastrophic, near-expiry forced)
        // are NEVER gated by this cooldown. A prior soft-exit FAK miss must not freeze the
        // stop-loss while the position bleeds. (Jun 26 trade id 96: a reversal FAK miss at
        // −6.5% froze ALL exit re-evaluation for 180s; the NO bid collapsed $0.43→$0.35 and
        // the position realized −23.9% — nearly 2× the 12% stop — when the cooldown lapsed.)
        let soft_exit_cooldown_active = {
            let last = self.last_exit_signal_at.lock().unwrap();
            match *last {
                Some(t) => t.elapsed().as_secs() < config::TRENDCAPTURE_EXIT_SIGNAL_COOLDOWN_SECS,
                None => false,
            }
        };

        // TrendCapture operates on the maker venue — resolve market/snap for exit checks
        let (market, snap) = if let (Some(mk), Some(ms)) = (&ctx.maker_market, &ctx.maker_snapshot) {
            (mk, ms)
        } else {
            (&ctx.market, &ctx.snapshot)
        };

        let drift_10m = ctx.snapshot.oracle_drift_10m;

        // Per-asset reversal threshold — oracle-relative
        let reversal_thr = config::oracle_threshold(
            dc.trendcapture_reversal_drift_pct,
            ctx.snapshot.oracle_price,
        );

        // Dynamic SL: tighter in the last hour before expiry
        let secs_left_opt = market.market_close_time
            .map(|ct| (ct - Utc::now()).num_seconds());
        // Tradelog/reason tag reflecting the active thesis.
        let tag = if dc.trendreversal_mode { "TrendReversal" } else { "TrendCapture" };

        // Stop-loss percentage. In fade mode use the tight TRENDREVERSAL stop (the
        // failure mode is the trend continuing, which is fast). Otherwise the legacy
        // dynamic stop (tighter near expiry).
        let stop_loss_pct = if dc.trendreversal_mode {
            config::TRENDREVERSAL_STOP_LOSS_PCT
        } else {
            match secs_left_opt {
                Some(s) if s < config::TRENDCAPTURE_LATE_MARKET_SL_SECS => config::TRENDCAPTURE_LATE_MARKET_STOP_LOSS_PERCENT,
                _ => dc.trendcapture_stop_loss_pct,
            }
        };

        // Take-profit target. Fade mode lets the reversion run to a wide target.
        let tp_target = if dc.trendreversal_mode {
            config::TRENDREVERSAL_TARGET_PROFIT_PCT
        } else {
            dc.trendcapture_target_profit_pct
        };

        // Collect exit decision inside the lock scope, then act outside it.
        struct PendingExit {
            token_id:     crate::venues::core::MarketId,
            bid:          Decimal,
            shares:       Decimal,
            fee_bps:      u16,
            is_neg_risk:  bool,
            market_name:  String,
            condition_id: String,
            ghost_mode:   bool,
            reason:       String,
            /// true for SL/forced exits — increments consecutive loss counter
            is_sl:        bool,
        }

        let pending: Option<PendingExit> = {
            let pos_map = ctx.positions.lock().await;
            let mut found: Option<PendingExit> = None;

            'outer: for ((strategy_name, token_id), position) in pos_map.iter() {
                // Accept legacy "TrendCaptureStrategy" too so a position opened under
                // the old name is still exited after the rename.
                if strategy_name != "TrendReversalStrategy" && strategy_name != "TrendCaptureStrategy" { continue; }

                let bid = if token_id == &market.yes_token { snap.yes_bid }
                          else if token_id == &market.no_token { snap.no_bid }
                          else { continue };

                let avg_entry = position.avg_entry;
                if avg_entry <= dec!(0) { continue; }

                let secs_held = (Utc::now() - position.opened_at).num_seconds();

                // Wait for fill confirmation before any non-catastrophic exit
                if position.fill_confirmed_at.is_none() {
                    let loss_pct = (avg_entry - bid) / avg_entry;
                    if loss_pct < dc.trendcapture_catastrophic_sl_pct {
                        continue;
                    }
                }

                let profit_margin = (bid - avg_entry) / avg_entry;
                let fee_bps = if token_id == &market.yes_token { market.yes_fee_bps as u16 }
                              else { market.no_fee_bps as u16 };

                // Helper closure to build the pending exit
                let make_exit = |reason: String, is_sl: bool| PendingExit {
                    token_id:     token_id.clone(),
                    bid,
                    shares:       position.shares,
                    fee_bps,
                    is_neg_risk:  market.is_neg_risk,
                    market_name:  market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                    ghost_mode:   dc.ghost_mode,
                    reason,
                    is_sl,
                };

                // ── Catastrophic stop — ALWAYS active ─────────────────────────
                // Fires regardless of fill-confirmation AND the minimum-hold window.
                // Previously the catastrophic check lived only in the pre-confirmation
                // branch, so a CONFIRMED position in its first FILL_CONFIRM_MIN_HOLD_SECS
                // (30s) had NO stop at all — the exact blackout that let the 2026-06-29
                // 09:30 trade gap from entry to −18% in 34s before the normal 5% stop
                // became eligible. The hard catastrophic floor must never be frozen.
                if profit_margin <= -dc.trendcapture_catastrophic_sl_pct {
                    found = Some(make_exit(format!(
                        "{}Catastrophic: bid=${:.4}, loss={:.2}%",
                        tag, bid, profit_margin * dec!(100)), true));
                    break 'outer;
                }

                // Near-expiry forced exit
                if let Some(close_time) = market.market_close_time {
                    let secs_left = (close_time - Utc::now()).num_seconds();
                    let net_profit = (bid - config::SELL_PRICE_OFFSET - avg_entry) / avg_entry;
                    if secs_left <= config::TRENDCAPTURE_EXPIRY_EXIT_SECS
                        && net_profit < config::TRENDCAPTURE_EXPIRY_MIN_PROFIT_TO_HOLD
                    {
                        found = Some(make_exit(format!("{}NearExpiry: bid=${:.4}, net={:.2}%", tag, bid, net_profit * dec!(100)), true));
                        break 'outer;
                    }
                }

                // Take-profit (discretionary — suppressed during soft-exit cooldown)
                if !soft_exit_cooldown_active
                    && (profit_margin >= tp_target
                        || bid >= dc.trendcapture_take_profit_ceiling)
                {
                    found = Some(make_exit(format!("{}TP: bid=${:.4}, profit={:.2}%", tag, bid, profit_margin * dec!(100)), false));
                    break 'outer;
                }

                // Stop-loss (only after minimum hold)
                if secs_held >= config::TRENDCAPTURE_FILL_CONFIRM_MIN_HOLD_SECS
                    && profit_margin <= -stop_loss_pct
                {
                    found = Some(make_exit(format!("{}SL: bid=${:.4}, loss={:.2}%", tag, bid, profit_margin * dec!(100)), true));
                    break 'outer;
                }

                // Trend-reversal exit — trend-FOLLOWING only. In fade (TrendReversal)
                // mode the entry already fades the drift, so a drift flip is the
                // thesis playing OUT, not a reason to bail; rely on TP/SL/catastrophic.
                if !dc.trendreversal_mode
                    && !soft_exit_cooldown_active
                    && secs_held >= config::TRENDCAPTURE_MIN_HOLD_BEFORE_REVERSAL_SECS {
                    let is_yes = token_id == &market.yes_token;
                    let reversal = if is_yes {
                        drift_10m <= -reversal_thr
                    } else {
                        drift_10m >= reversal_thr
                    };

                    if reversal {
                        let net_profit = (bid - config::SELL_PRICE_OFFSET - avg_entry) / avg_entry;

                        // Profit-protection only (rework 2026-06-28): fire the reversal exit
                        // ONLY when the position is net-profitable, to lock in the gain when
                        // the trend that justified entry has flipped. Underwater positions are
                        // left to the clean 5% stop — the old `|| profit_margin <= -3%` branch
                        // acted as a second, looser stop that bailed at scratch losses on a
                        // drift wiggle (33 reversal exits netted −$3.07).
                        if net_profit > dec!(0) {
                            found = Some(make_exit(format!(
                                "TrendCaptureRev: bid=${:.4}, drift_10m={:.0}, profit={:.2}%",
                                bid, drift_10m, profit_margin * dec!(100)
                            ), false));
                            break 'outer;
                        }
                    }
                }
            }
            found
            // pos_map MutexGuard dropped here
        };

        if let Some(p) = pending {
            self.record_exit(&p.token_id, &p.condition_id, p.is_sl);
            // Stamp the exit signal cooldown to prevent FAK-miss re-fire storm
            if let Ok(mut last) = self.last_exit_signal_at.lock() {
                *last = Some(Instant::now());
            }
            return Ok(StrategySignal::Exit {
                params: OrderParams {
                    token_id:     p.token_id,
                    price:        p.bid,
                    shares:       p.shares,
                    fee_bps:      p.fee_bps,
                    is_neg_risk:  p.is_neg_risk,
                    market_name:  p.market_name,
                    condition_id: p.condition_id,
                    order_type:   TimeInForce::Fak,   // exits are taker — selling at bid crosses book
                    post_only:    false,               // post_only on a sell-at-bid always rejects
                    ghost_mode:   p.ghost_mode,
                },
                reason:    p.reason,
                exit_pair: false,
            });
        }

        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { "TrendReversalStrategy".to_string() }
    fn venue(&self) -> &'static str { "Window/Daily" }
    fn max_exposure(&self) -> Decimal { config::TRENDCAPTURE_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str {
        if config::TRENDREVERSAL_MODE { "Drift fade (mean-reversion)" } else { "One-sided drift" }
    }
}

impl TrendReversalStrategyImpl {
    /// Record exit time for post-exit cooldown tracking.
    /// If `is_sl` is true, increments the consecutive-loss counter for the
    /// given condition_id; a TP exit resets it.
    fn record_exit(&self, token_id: &MarketId, condition_id: &str, is_sl: bool) {
        if let Ok(mut map) = self.post_exit_cooldown.lock() {
            map.insert(token_id.clone(), Instant::now());
        }
        if let Ok(mut losses) = self.consecutive_losses.lock() {
            if is_sl {
                let count = losses.entry(condition_id.to_string()).or_insert(0);
                *count += 1;
            } else {
                // TP or reversal with profit resets the streak
                losses.remove(condition_id);
            }
        }
    }
}

/// Kelly-fractional sizing helper for TrendCapture.
///
/// Scales linearly from `min_size` (at exactly 1× threshold) to `max_size`
/// (at `TRENDCAPTURE_KELLY_MAX_MULTIPLIER`×), capping above that.
pub fn kelly_trendcapture_size(
    drift_abs: Decimal,
    threshold: Decimal,
    min_size:  Decimal,
    max_size:  Decimal,
) -> Decimal {
    if !config::ENABLE_KELLY_SIZING { return min_size; }
    if threshold <= Decimal::ZERO { return min_size; }
    let strength = (drift_abs / threshold)
        .max(Decimal::ONE)
        .min(config::TRENDCAPTURE_KELLY_MAX_MULTIPLIER);
    let fraction = (strength - Decimal::ONE)
        / (config::TRENDCAPTURE_KELLY_MAX_MULTIPLIER - Decimal::ONE);
    min_size + fraction * (max_size - min_size)
}