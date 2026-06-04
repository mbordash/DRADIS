/// Momentum Strategy
///
/// One-sided, non-hedged trades based on Binance price oracle signals.
/// Entry triggers when price velocity exceeds threshold and market conditions align.
/// Exits via take-profit, stop-loss, or reversal detection.

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::debug;
use std::sync::Mutex;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams};
use crate::vipers::is_drawdown_limit_hit;
use crate::config;
use polymarket_client_sdk_v2::clob::types::OrderType; // Import OrderType

/// Stateful Momentum strategy implementation.
///
/// `prev_yes_obi` / `prev_no_obi` track the previous tick's computed OBI so that
/// the OBI-swing gate can detect sudden book-flip events between consecutive evaluations.
/// Uses `std::sync::Mutex` (non-async) because the values are read/written atomically
/// without any await points between lock acquisition and release.
pub struct MomentumStrategyImpl {
    prev_yes_obi: Mutex<Decimal>,
    prev_no_obi:  Mutex<Decimal>,
}

impl MomentumStrategyImpl {
    pub fn new() -> Self {
        Self {
            prev_yes_obi: Mutex::new(dec!(0)),
            prev_no_obi:  Mutex::new(dec!(0)),
        }
    }
}

impl Default for MomentumStrategyImpl {
    fn default() -> Self { Self::new() }
}

#[async_trait]
impl Strategy for MomentumStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        if !dc.enable_momentum {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Global Risk Check ────────────────────────────────────────────────
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            return Ok(StrategySignal::NoSignal);
        }

        let velocity = ctx.snapshot.velocity;
        let velocity_1s = ctx.snapshot.velocity_1s;
        let acceleration = ctx.snapshot.acceleration;
        let binance_price = ctx.snapshot.oracle_price;
        let strike_price = ctx.market.strike_price;
        let crypto_filter = &ctx.crypto_filter;

        let threshold = match crypto_filter.as_str() {
            "eth" => config::ETH_MOMENTUM_THRESHOLD,
            "sol" => config::SOL_MOMENTUM_THRESHOLD,
            _ => config::BTC_MOMENTUM_THRESHOLD,
        };

        let strike_buffer = match crypto_filter.as_str() {
            "eth" => config::ETH_STRIKE_BUFFER,
            "sol" => config::SOL_STRIKE_BUFFER,
            _ => config::BTC_STRIKE_BUFFER,
        };

        let short_min = threshold * config::MOMENTUM_SHORT_WINDOW_FRACTION;
        let short_ok_bull = velocity_1s >= short_min;
        let short_ok_bear = velocity_1s <= -short_min;

        let accel_bypass = threshold * config::MOMENTUM_ACCELERATION_BYPASS_MULTIPLIER;
        let accel_ok_bull = acceleration >= dec!(0) || velocity >= accel_bypass;
        let accel_ok_bear = acceleration <= dec!(0) || velocity <= -accel_bypass;

        let trade_size = kelly_momentum_size(
            velocity, threshold,
            dc.momentum_min_trade_size_usdc,
            dc.momentum_max_trade_size_usdc,
        );

        // ── Strategy Exposure Check ──────────────────────────────────────────
        let current_exposure = {
            let pos_map = ctx.positions.lock().await;
            pos_map.iter()
                .filter(|((s, _), _)| s == "MomentumStrategy")
                .map(|(_, p)| p.shares * p.avg_entry)
                .sum::<Decimal>()
        };

        if current_exposure + trade_size > dc.momentum_max_exposure_usdc {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Macro: build an entry OrderParams for a given token/fee ─────────
        macro_rules! entry_params {
            ($token:expr, $price:expr, $fee:expr) => {
                OrderParams {
                    token_id:    $token,
                    price:       $price,
                    shares:      trade_size / $price,
                    fee_bps:     $fee,
                    is_neg_risk: ctx.market.is_neg_risk,
                    market_name: ctx.market.market_name.clone(),
                    condition_id: ctx.market.condition_id.clone(),
                    order_type:  OrderType::FAK,
                    post_only:   false,
                    ghost_mode:  dc.ghost_mode,
                }
            };
        }

        // ── UNIVERSAL GATES (apply regardless of whether strike price is known) ──
        //
        // Previously, snapshot-age / spread / OBI checks only lived inside the
        // `if let Some(strike)` branch.  When strike resolution fails the bot falls
        // into the `else` branch which silently bypassed ALL these guards — observed
        // in 2026-05-13 session: trades with OBI_Y=-0.80 and OBI_Y=-0.53 entered
        // because the "without strike" path had no OBI veto.

        // ── Expiry guard ──────────────────────────────────────────────────────
        if let Some(close_time) = ctx.market.market_close_time {
            let secs_left = (close_time - chrono::Utc::now()).num_seconds();
            if secs_left < config::MOMENTUM_MIN_SECS_TO_EXPIRY_FOR_ENTRY {
                debug!(" Momentum entry blocked: only {}s to expiry (min {}s)",
                    secs_left, config::MOMENTUM_MIN_SECS_TO_EXPIRY_FOR_ENTRY);
                return Ok(StrategySignal::NoSignal);
            }
        }

        // ── Market warmup gate ────────────────────────────────────────────────
        // After a market switch the WS orderbook subscription has only had a few
        // ticks to populate depth data.  The first evaluation fires within 1–2
        // seconds of the switch: OBI / velocity readings are unreliable, and
        // entries on that first tick reverse immediately (the book is still
        // repricing from the prior market context).
        //
        // Root cause of 2026-05-27 20:11 loss (−$0.7346):
        //   Switch at 20:11:33, entry at 20:11:39 ([0ms] first tick), SL −10%.
        //   Heartbeat showed YES OBI=−0.94 (strongly adverse) but evaluation used
        //   the very first WS depth tick on the new subscription — book hadn't
        //   settled to its equilibrium state yet.
        let secs_since_market_start = (chrono::Utc::now() - ctx.market_started_at).num_seconds();
        if secs_since_market_start < config::MOMENTUM_MARKET_WARMUP_SECS {
            debug!(" Momentum entry blocked: market warmup period ({}s < {}s min)",
                secs_since_market_start, config::MOMENTUM_MARKET_WARMUP_SECS);
            return Ok(StrategySignal::NoSignal);
        }

        // ── Snapshot staleness gate ───────────────────────────────────────────
        let snap_age = (chrono::Utc::now() - ctx.snapshot.timestamp).num_seconds();
        if snap_age > config::MOMENTUM_MAX_SNAPSHOT_AGE_SECS {
            debug!(" Momentum entry blocked: snapshot too stale ({}s > max {}s)",
                snap_age, config::MOMENTUM_MAX_SNAPSHOT_AGE_SECS);
            return Ok(StrategySignal::NoSignal);
        }

        // ── Spread gate: block wide-book entries ──────────────────────────────
        let ask_sum = ctx.snapshot.yes_ask + ctx.snapshot.no_ask;
        if ask_sum > config::MOMENTUM_MAX_ENTRY_ASK_SUM {
            debug!(" Momentum spread gate: ask_sum={:.3} > max {:.3} — book too wide",
                ask_sum, config::MOMENTUM_MAX_ENTRY_ASK_SUM);
            return Ok(StrategySignal::NoSignal);
        }

        // ── Minimum price floor ───────────────────────────────────────────────
        // Block entries on near-zero priced tokens: buying YES at $0.09 creates
        // 100+ shares from a $9 budget; a 1¢ bid move = $1 swing.  Combined with
        // the 30s fill-confirm lock (no exits allowed) this caused a $2.13 loss on
        // 2026-05-13 (Trade #7: YES $0.09 × 106 shares, bid unchanged, -10% locked).
        // MOMENTUM_MIN_ENTRY_PRICE = 0.18 limits entries to 18%–82% probability range.
        let yes_ask = ctx.snapshot.yes_ask;
        let no_ask  = ctx.snapshot.no_ask;
        if yes_ask < config::MOMENTUM_MIN_ENTRY_PRICE && no_ask < config::MOMENTUM_MIN_ENTRY_PRICE {
            debug!(" Momentum min-price blocked: yes_ask={:.3} no_ask={:.3} both below floor {:.3}",
                yes_ask, no_ask, config::MOMENTUM_MIN_ENTRY_PRICE);
            return Ok(StrategySignal::NoSignal);
        }

        // ── OBI adverse-direction veto ────────────────────────────────────────
        // Default to -1.0 (maximally adverse) when depth data is missing.
        let yes_total_depth = ctx.snapshot.yes_bid_depth + ctx.snapshot.yes_ask_depth;
        let no_total_depth  = ctx.snapshot.no_bid_depth  + ctx.snapshot.no_ask_depth;
        let yes_obi = if yes_total_depth > dec!(0) {
            (ctx.snapshot.yes_bid_depth - ctx.snapshot.yes_ask_depth) / yes_total_depth
        } else { dec!(-1.0) };
        let no_obi = if no_total_depth > dec!(0) {
            (ctx.snapshot.no_bid_depth - ctx.snapshot.no_ask_depth) / no_total_depth
        } else { dec!(-1.0) };
        let obi_blocks_bull = yes_obi < config::MOMENTUM_OBI_ADVERSE_BLOCK;
        let obi_blocks_bear = no_obi  < config::MOMENTUM_OBI_ADVERSE_BLOCK;
        if obi_blocks_bull {
            debug!(" Momentum OBI veto (BULL): YES OBI={:.3} < block {:.3} — book fading the pump",
                yes_obi, config::MOMENTUM_OBI_ADVERSE_BLOCK);
        }
        if obi_blocks_bear {
            debug!(" Momentum OBI veto (BEAR): NO OBI={:.3} < block {:.3} — book fading the dump",
                no_obi, config::MOMENTUM_OBI_ADVERSE_BLOCK);
        }

        // ── OBI exhaustion veto ───────────────────────────────────────────────
        // When OBI > MOMENTUM_OBI_EXHAUSTION_BLOCK the book is dominated by bids
        // with no sellers — the momentum move is already spent and a reversal is
        // imminent.  Entering a BULL position into an all-bid book means we are
        // the last buyer before the flush.
        // 2026-05-24 8PM ghost trade: YES OBI=0.86 at entry → price dropped from
        // $0.67 to $0.61 in 30 s, -$0.72 loss.  Blocked at threshold 0.70.
        let obi_exhausted_bull = yes_obi > config::MOMENTUM_OBI_EXHAUSTION_BLOCK;
        let obi_exhausted_bear = no_obi  > config::MOMENTUM_OBI_EXHAUSTION_BLOCK;
        if obi_exhausted_bull {
            debug!(" Momentum OBI exhaustion (BULL): YES OBI={:.3} > threshold {:.3} — buyers exhausted",
                yes_obi, config::MOMENTUM_OBI_EXHAUSTION_BLOCK);
        }
        if obi_exhausted_bear {
            debug!(" Momentum OBI exhaustion (BEAR): NO OBI={:.3} > threshold {:.3} — sellers exhausted",
                no_obi, config::MOMENTUM_OBI_EXHAUSTION_BLOCK);
        }

        // ── OBI oscillation gate ─────────────────────────────────────────────────
        // When the YES OBI has been swinging wildly over the recent book ticks it
        // means informed traders are actively sweeping both sides — an extremely
        // unstable microstructure where momentum entries consistently reverse.
        //
        // Root cause of 2026-06-01 13:39 loss: 6 heartbeats before entry showed
        // OBI_Y cycling: −0.82 → −0.88 → +0.57 → +0.61 → −0.42 → +0.52.
        // The WS snapshot at entry had OBI_Y≈0.70+ (exhaustion) but the last
        // heartbeat recorded only 0.52 — the rapid oscillation masked the true state.
        //
        // Gate: block entry when the absolute difference between the current OBI
        // and the previous OBI snapshot exceeds MOMENTUM_OBI_SWING_BLOCK.  This
        // detects in-progress sweep events where the book is repricing too fast
        // for a safe directional entry.
        let yes_total_depth_check = ctx.snapshot.yes_bid_depth + ctx.snapshot.yes_ask_depth;
        let no_total_depth_check  = ctx.snapshot.no_bid_depth  + ctx.snapshot.no_ask_depth;
        let cur_yes_obi = if yes_total_depth_check > dec!(0) {
            (ctx.snapshot.yes_bid_depth - ctx.snapshot.yes_ask_depth) / yes_total_depth_check
        } else { dec!(-1.0) };
        let cur_no_obi = if no_total_depth_check > dec!(0) {
            (ctx.snapshot.no_bid_depth - ctx.snapshot.no_ask_depth) / no_total_depth_check
        } else { dec!(-1.0) };

        // Read previous OBI and update atomically (non-async Mutex, no await held)
        let (prev_yes_obi_val, prev_no_obi_val) = {
            let mut py = self.prev_yes_obi.lock().unwrap();
            let mut pn = self.prev_no_obi.lock().unwrap();
            let old = (*py, *pn);
            *py = cur_yes_obi;
            *pn = cur_no_obi;
            old
        };

        let yes_obi_swing = (cur_yes_obi - prev_yes_obi_val).abs();
        let no_obi_swing  = (cur_no_obi  - prev_no_obi_val).abs();
        if yes_obi_swing > config::MOMENTUM_OBI_SWING_BLOCK {
            debug!(" Momentum OBI swing gate (BULL): swing={:.3} > block {:.3} — book unstable",
                yes_obi_swing, config::MOMENTUM_OBI_SWING_BLOCK);
        }
        if no_obi_swing > config::MOMENTUM_OBI_SWING_BLOCK {
            debug!(" Momentum OBI swing gate (BEAR): swing={:.3} > block {:.3} — book unstable",
                no_obi_swing, config::MOMENTUM_OBI_SWING_BLOCK);
        }
        let obi_swing_blocks_bull = yes_obi_swing > config::MOMENTUM_OBI_SWING_BLOCK;
        let obi_swing_blocks_bear = no_obi_swing  > config::MOMENTUM_OBI_SWING_BLOCK;

        // ── 10-minute oracle drift alignment gate ─────────────────────────────────
        // A 5-second velocity spike that contradicts the 10-minute oracle trend
        // is a dead-cat bounce / relief rally, not a new directional move.
        // Root cause: 2026-05-27 15:24 loss — BTC had been declining for 10m
        // before the 5s spike that triggered a YES entry at $0.64; the market
        // reversed $0.64→$0.61 in 30 seconds.
        // Threshold = 2× primary velocity threshold (BTC: 2 × $25 = $50/10m).
        let (drift_bull_block, drift_bear_block) = match crypto_filter.as_str() {
            "eth" => (config::MOMENTUM_BULL_DRIFT_10M_BLOCK_ETH, config::MOMENTUM_BEAR_DRIFT_10M_BLOCK_ETH),
            "sol" => (config::MOMENTUM_BULL_DRIFT_10M_BLOCK_SOL, config::MOMENTUM_BEAR_DRIFT_10M_BLOCK_SOL),
            _     => (config::MOMENTUM_BULL_DRIFT_10M_BLOCK_BTC, config::MOMENTUM_BEAR_DRIFT_10M_BLOCK_BTC),
        };
        let drift_10m = ctx.snapshot.oracle_drift_10m;
        // drift_bull_block is negative; block BULL entries when drift < this (BTC declining in last 10m)
        let drift_blocks_bull = drift_bull_block < dec!(0) && drift_10m < drift_bull_block;
        // drift_bear_block is positive; block BEAR entries when drift > this (BTC rising in last 10m)
        let drift_blocks_bear = drift_bear_block > dec!(0) && drift_10m > drift_bear_block;
        if drift_blocks_bull {
            debug!(" Momentum 10m-drift veto (BULL): drift_10m={:.2} < block {:.2} — BTC declining medium-term",
                drift_10m, drift_bull_block);
        }
        if drift_blocks_bear {
            debug!(" Momentum 10m-drift veto (BEAR): drift_10m={:.2} > block {:.2} — BTC rising medium-term",
                drift_10m, drift_bear_block);
        }

        if let Some(strike) = strike_price {
            // ── Window/Daily trend filter ─────────────────────────────────────
            let window_blocks_bull;
            let window_blocks_bear;
            if let (Some(_wm), Some(ws)) = (&ctx.maker_market, &ctx.maker_snapshot) {
                let w_yes_mid = if ws.yes_bid > dec!(0) && ws.yes_ask < dec!(1) {
                    (ws.yes_bid + ws.yes_ask) / dec!(2)
                } else {
                    dec!(0.5)
                };
                window_blocks_bull = config::MOMENTUM_WINDOW_BEARISH_BLOCK > dec!(0)
                    && w_yes_mid < config::MOMENTUM_WINDOW_BEARISH_BLOCK;
                window_blocks_bear = config::MOMENTUM_WINDOW_BULLISH_BLOCK > dec!(0)
                    && w_yes_mid > config::MOMENTUM_WINDOW_BULLISH_BLOCK;
                if window_blocks_bull || window_blocks_bear {
                    debug!(" Momentum window filter: YES_mid={:.3} blocks {}",
                        w_yes_mid, if window_blocks_bull { "BULL" } else { "BEAR" });
                }
            } else {
                window_blocks_bull = false;
                window_blocks_bear = false;
            }

            // Primary entry
            if velocity > threshold && binance_price > (strike + strike_buffer)
                && yes_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                && yes_ask >= config::MOMENTUM_MIN_ENTRY_PRICE
                && short_ok_bull && accel_ok_bull && !window_blocks_bull && !obi_blocks_bull && !obi_exhausted_bull && !obi_swing_blocks_bull && !drift_blocks_bull
            {
                return Ok(StrategySignal::Entry {
                    params: entry_params!(ctx.market.yes_token, yes_ask, ctx.market.yes_fee_bps as u16),
                    pair_params: None,
                });
            } else if velocity < -threshold && binance_price < (strike - strike_buffer)
                && no_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                && no_ask >= config::MOMENTUM_MIN_ENTRY_PRICE
                && short_ok_bear && accel_ok_bear && !window_blocks_bear && !obi_blocks_bear && !obi_exhausted_bear && !obi_swing_blocks_bear && !drift_blocks_bear
            {
                return Ok(StrategySignal::Entry {
                    params: entry_params!(ctx.market.no_token, no_ask, ctx.market.no_fee_bps as u16),
                    pair_params: None,
                });
            }

            // Secondary "strike-crossing" entry
            if velocity > threshold && binance_price > strike
                && yes_ask <= config::MAX_MOMENTUM_CROSSING_ENTRY_PRICE
                && yes_ask >= config::MOMENTUM_MIN_ENTRY_PRICE
                && short_ok_bull && accel_ok_bull && !window_blocks_bull && !obi_blocks_bull && !obi_exhausted_bull && !obi_swing_blocks_bull && !drift_blocks_bull
            {
                return Ok(StrategySignal::Entry {
                    params: entry_params!(ctx.market.yes_token, yes_ask, ctx.market.yes_fee_bps as u16),
                    pair_params: None,
                });
            } else if velocity < -threshold && binance_price < strike
                && no_ask <= config::MAX_MOMENTUM_CROSSING_ENTRY_PRICE
                && no_ask >= config::MOMENTUM_MIN_ENTRY_PRICE
                && short_ok_bear && accel_ok_bear && !window_blocks_bear && !obi_blocks_bear && !obi_exhausted_bear && !obi_swing_blocks_bear && !drift_blocks_bear
            {
                return Ok(StrategySignal::Entry {
                    params: entry_params!(ctx.market.no_token, no_ask, ctx.market.no_fee_bps as u16),
                    pair_params: None,
                });
            }
        } else {
            // Without strike — universal gates already applied above; only
            // velocity + price bounds + drift alignment needed here.
            if velocity > threshold
                && yes_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                && yes_ask >= config::MOMENTUM_MIN_ENTRY_PRICE
                && short_ok_bull && accel_ok_bull && !obi_blocks_bull && !obi_exhausted_bull && !obi_swing_blocks_bull && !drift_blocks_bull
            {
                return Ok(StrategySignal::Entry {
                    params: entry_params!(ctx.market.yes_token, yes_ask, ctx.market.yes_fee_bps as u16),
                    pair_params: None,
                });
            } else if velocity < -threshold
                && no_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                && no_ask >= config::MOMENTUM_MIN_ENTRY_PRICE
                && short_ok_bear && accel_ok_bear && !obi_blocks_bear && !obi_exhausted_bear && !obi_swing_blocks_bear && !drift_blocks_bear
            {
                return Ok(StrategySignal::Entry {
                    params: entry_params!(ctx.market.no_token, no_ask, ctx.market.no_fee_bps as u16),
                    pair_params: None,
                });
            }
        }

        Ok(StrategySignal::NoSignal)
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        let pos_map = ctx.positions.lock().await;

        for ((strategy_name, token_id), position) in pos_map.iter() {
            if strategy_name != "MomentumStrategy" { continue; }
            let bid = if token_id == &ctx.market.yes_token { ctx.snapshot.yes_bid }
                      else if token_id == &ctx.market.no_token { ctx.snapshot.no_bid }
                      else { continue };

            let secs_held = (chrono::Utc::now() - position.opened_at).num_seconds();
            if position.fill_confirmed_at.is_none() {
                let profit_margin_check = (bid - position.avg_entry) / position.avg_entry;
                if secs_held < config::MOMENTUM_FILL_CONFIRM_MIN_HOLD_SECS {
                    // During the fill-confirmation window, allow an immediate escape
                    // only if the loss is catastrophic (> MOMENTUM_CATASTROPHIC_SL_PCT).
                    // Prevents lock-in to large sudden adverse moves while waiting for
                    // the Polymarket indexer to register the balance.
                    // Root cause: 2026-05-13 Trade #3 lost -14% during a 30s lock with
                    // no exit allowed; a catastrophic SL at 8% would have exited at ~5s.
                    if profit_margin_check > -config::MOMENTUM_CATASTROPHIC_SL_PCT {
                        continue; // Not catastrophic yet — wait for fill confirmation
                    }
                    // Fall through: loss > catastrophic threshold → allow exit below
                } else {
                    // After 30s: normal stop-loss gate
                    if profit_margin_check > -dc.momentum_stop_loss_pct { continue; }
                }
            }

            let avg_entry = position.avg_entry;
            let velocity = ctx.snapshot.velocity;
            let velocity_1s = ctx.snapshot.velocity_1s;
            let threshold = match ctx.crypto_filter.as_str() {
                "eth" => config::ETH_MOMENTUM_THRESHOLD,
                "sol" => config::SOL_MOMENTUM_THRESHOLD,
                _ => config::BTC_MOMENTUM_THRESHOLD,
            };

            if avg_entry <= dec!(0) { continue; }
            let profit_margin = (bid - avg_entry) / avg_entry;

            // Macro: build exit params for this token
            macro_rules! exit_params {
                () => {
                    OrderParams {
                        token_id: *token_id,
                        price: bid,
                        shares: position.shares,
                        fee_bps: if token_id == &ctx.market.yes_token { ctx.market.yes_fee_bps as u16 } else { ctx.market.no_fee_bps as u16 },
                        is_neg_risk: ctx.market.is_neg_risk,
                        market_name: ctx.market.market_name.clone(),
                        condition_id: ctx.market.condition_id.clone(),
                        order_type: OrderType::FAK,
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    }
                };
            }

            // Near-expiry forced exit
            // Use net profit (after sell offset) for the hold threshold so we don't
            // artificially "hold" a position that is break-even or negative net of costs.
            if let Some(close_time) = ctx.market.market_close_time {
                let secs_left = (close_time - chrono::Utc::now()).num_seconds();
                let net_profit_for_expiry = (bid - config::SELL_PRICE_OFFSET - avg_entry) / avg_entry;
                if secs_left <= config::MOMENTUM_EXPIRY_EXIT_SECS && net_profit_for_expiry < config::MOMENTUM_EXPIRY_MIN_PROFIT_TO_HOLD {
                    let reason = format!("NearExpiry: bid=${:.4}, net_profit={:.2}%", bid, net_profit_for_expiry * dec!(100));
                    return Ok(StrategySignal::Exit { params: exit_params!(), reason, exit_pair: false });
                }
            }

            let target = if avg_entry >= dec!(0.70) { dec!(0.05) } else { dc.momentum_target_profit_pct };
            let stop_loss = -dc.momentum_stop_loss_pct;
            let reversal_threshold = -(threshold * config::MOMENTUM_REVERSAL_RATIO);

            if profit_margin >= target || bid >= config::MOMENTUM_TAKE_PROFIT_CEILING {
                let reason = format!("MomentumTP: bid=${:.4}, profit={:.2}%", bid, profit_margin * dec!(100));
                return Ok(StrategySignal::Exit { params: exit_params!(), reason, exit_pair: false });
            }

            if secs_held >= config::MOMENTUM_FILL_CONFIRM_MIN_HOLD_SECS && profit_margin <= stop_loss {
                let reason = format!("MomentumSL: bid=${:.4}, loss={:.2}%", bid, profit_margin * dec!(100));
                return Ok(StrategySignal::Exit { params: exit_params!(), reason, exit_pair: false });
            }

            // Momentum Decay exit
            // Use NET profit after SELL_PRICE_OFFSET to match how P&L is actually
            // calculated in main.rs: pnl = (bid - SELL_PRICE_OFFSET - avg_entry) * shares.
            // Previously used raw `profit_margin = (bid - avg_entry) / avg_entry > 0`,
            // which fired when bid was only 1 cent above avg_entry — the entire "profit"
            // was then consumed by SELL_PRICE_OFFSET, producing the observed $0.0000 PnL
            // on MomentumDecay exits (e.g. 2026-05-12 YES @ 0.71 entry, bid=$0.72 exit).
            let decay_min = threshold * config::MOMENTUM_DECAY_EXIT_FRACTION;
            let is_yes = token_id == &ctx.market.yes_token;
            let net_profit_margin = (bid - config::SELL_PRICE_OFFSET - avg_entry) / avg_entry;
            if net_profit_margin > dec!(0) && ((is_yes && velocity_1s < decay_min) || (!is_yes && velocity_1s > -decay_min)) {
                let reason = format!("MomentumDecay: bid=${:.4}, profit={:.2}%", bid, profit_margin * dec!(100));
                return Ok(StrategySignal::Exit { params: exit_params!(), reason, exit_pair: false });
            }

            // Reversal exit
            if secs_held >= config::MOMENTUM_MIN_HOLD_SECS_BEFORE_REVERSAL
                && ((is_yes && velocity < reversal_threshold) || (!is_yes && velocity > -reversal_threshold))
            {
                let reason = format!("MomentumReversal: bid=${:.4}, profit={:.2}%", bid, profit_margin * dec!(100));
                return Ok(StrategySignal::Exit { params: exit_params!(), reason, exit_pair: false });
            }

            // OBI exhaustion in-position exit
            //
            // When the book flips to exhaustion AFTER we enter (OBI > exhaustion_block
            // for a YES position, or < −exhaustion_block for a NO position) it means
            // all buyers have accumulated and a selling reversal is imminent.  If we
            // are at or below breakeven, exit immediately rather than wait for the
            // full stop-loss to hit.
            //
            // Root cause of 2026-06-01 13:39 loss: entry at $0.49 avg, 14 s later
            // OBI_Y=0.85 (above exhaustion=0.70) at bid=$0.48 (−4%), but no exit
            // path existed for this scenario.  The SL eventually fired at −8%
            // ($0.06 worse than the OBI-detected reversal signal).
            //
            // Gate: only fires after fill_confirmed_at to avoid false exits during
            // the 30s indexer-settle window.
            if position.fill_confirmed_at.is_some() && profit_margin <= dec!(0) {
                let yes_total_depth = ctx.snapshot.yes_bid_depth + ctx.snapshot.yes_ask_depth;
                let no_total_depth  = ctx.snapshot.no_bid_depth  + ctx.snapshot.no_ask_depth;
                let yes_obi = if yes_total_depth > dec!(0) {
                    (ctx.snapshot.yes_bid_depth - ctx.snapshot.yes_ask_depth) / yes_total_depth
                } else { dec!(-1.0) };
                let no_obi = if no_total_depth > dec!(0) {
                    (ctx.snapshot.no_bid_depth - ctx.snapshot.no_ask_depth) / no_total_depth
                } else { dec!(-1.0) };

                let obi_exhausted_in_pos =
                    (is_yes  && yes_obi > config::MOMENTUM_OBI_EXHAUSTION_BLOCK) ||
                        (!is_yes && no_obi  > config::MOMENTUM_OBI_EXHAUSTION_BLOCK);

                if obi_exhausted_in_pos {
                    // ── Max adverse move guard for OBI exhaustion exit ─────────────────────
                    // OBIExhaust is intended as an *early* reversal detector (exit before
                    // hitting the normal stop-loss). Without this guard, a late OBI spike
                    // after a large adverse move (e.g. -23% on 2026-06-03 Trade 4) will
                    // still trigger an exit — turning the mechanism into a "we're already
                    // wrecked" exit rather than a protective early one.
                    //
                    // We allow the OBI exit only if the position is not too far underwater.
                    // Using 1.6× the configured stop-loss gives a small buffer past normal
                    // SL while still protecting against deep late-signal losses.
                    let max_adverse_for_obi_exit = config::MOMENTUM_OBI_EXHAUST_MAX_ADVERSE_PCT;

                    if profit_margin >= max_adverse_for_obi_exit {
                        let obi_val = if is_yes { yes_obi } else { no_obi };
                        let reason = format!(
                            "MomentumOBIExhaust: bid=${:.4}, obi={:.3}, profit={:.2}%",
                            bid, obi_val, profit_margin * dec!(100)
                        );
                        return Ok(StrategySignal::Exit { params: exit_params!(), reason, exit_pair: false });
                    }
                    // If we reach here: OBI is exhausted but we're already deep underwater.
                    // Let the normal stop-loss (or catastrophic SL) handle it instead.
                }
            }
        }
        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { "MomentumStrategy".to_string() }
    fn venue(&self) -> &'static str { "Hourly" }
    fn max_exposure(&self) -> rust_decimal::Decimal { crate::config::MOMENTUM_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Gross one-sided" }
}

/// Kelly-fractional position sizing for Momentum.
/// Accepts min/max from DynamicConfig so the caller controls the range.
/// Structural params (KELLY_MAX_MULTIPLIER) remain compile-time constants.
pub fn kelly_momentum_size(
    velocity:  rust_decimal::Decimal,
    threshold: rust_decimal::Decimal,
    min_size:  rust_decimal::Decimal,
    max_size:  rust_decimal::Decimal,
) -> rust_decimal::Decimal {
    if threshold <= rust_decimal::Decimal::ZERO { return min_size; }
    let strength = (velocity.abs() / threshold)
        .max(rust_decimal::Decimal::ONE)
        .min(config::MOMENTUM_KELLY_MAX_MULTIPLIER);
    let fraction = (strength - rust_decimal::Decimal::ONE)
        / (config::MOMENTUM_KELLY_MAX_MULTIPLIER - rust_decimal::Decimal::ONE);
    min_size + fraction * (max_size - min_size)
}
