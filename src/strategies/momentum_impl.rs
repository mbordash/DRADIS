/// Momentum Strategy
///
/// One-sided, non-hedged trades based on Binance price oracle signals.
/// Entry triggers when price velocity exceeds threshold and market conditions align.
/// Exits via take-profit, stop-loss, or reversal detection.

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::info;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams};
use crate::strategies::is_drawdown_limit_hit;
use crate::config;

pub struct MomentumStrategyImpl;

#[async_trait]
impl Strategy for MomentumStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        if !config::ENABLE_MOMENTUM_TRADING {
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

        let trade_size = kelly_momentum_size(velocity, threshold);

        // ── Strategy Exposure Check ──────────────────────────────────────────
        let current_exposure = {
            let pos_map = ctx.positions.lock().await;
            pos_map.iter()
                .filter(|((s, _), _)| s == "MomentumStrategy")
                .map(|(_, p)| p.shares * p.avg_entry)
                .sum::<Decimal>()
        };

        if current_exposure + trade_size > config::MOMENTUM_MAX_EXPOSURE_USDC {
            return Ok(StrategySignal::NoSignal);
        }

        if let Some(strike) = strike_price {
            // Primary entry
            if velocity > threshold && binance_price > (strike + strike_buffer) && ctx.snapshot.yes_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                && short_ok_bull && accel_ok_bull
            {
                return Ok(StrategySignal::Entry {
                    params: OrderParams {
                        token_id: ctx.market.yes_token,
                        price: ctx.snapshot.yes_ask,
                        shares: trade_size / ctx.snapshot.yes_ask,
                        fee_bps: ctx.market.yes_fee_bps as u16,
                        is_neg_risk: ctx.market.is_neg_risk,
                        market_name: ctx.market.market_name.clone(),
                        condition_id: ctx.market.condition_id.clone(),
                    },
                    pair_params: None,
                });
            } else if velocity < -threshold && binance_price < (strike - strike_buffer) && ctx.snapshot.no_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                && short_ok_bear && accel_ok_bear
            {
                return Ok(StrategySignal::Entry {
                    params: OrderParams {
                        token_id: ctx.market.no_token,
                        price: ctx.snapshot.no_ask,
                        shares: trade_size / ctx.snapshot.no_ask,
                        fee_bps: ctx.market.no_fee_bps as u16,
                        is_neg_risk: ctx.market.is_neg_risk,
                        market_name: ctx.market.market_name.clone(),
                        condition_id: ctx.market.condition_id.clone(),
                    },
                    pair_params: None,
                });
            }

            // Secondary "strike-crossing" entry
            if velocity > threshold && binance_price > strike && ctx.snapshot.yes_ask <= config::MAX_MOMENTUM_CROSSING_ENTRY_PRICE
                && short_ok_bull && accel_ok_bull
            {
                return Ok(StrategySignal::Entry {
                    params: OrderParams {
                        token_id: ctx.market.yes_token,
                        price: ctx.snapshot.yes_ask,
                        shares: trade_size / ctx.snapshot.yes_ask,
                        fee_bps: ctx.market.yes_fee_bps as u16,
                        is_neg_risk: ctx.market.is_neg_risk,
                        market_name: ctx.market.market_name.clone(),
                        condition_id: ctx.market.condition_id.clone(),
                    },
                    pair_params: None,
                });
            } else if velocity < -threshold && binance_price < strike && ctx.snapshot.no_ask <= config::MAX_MOMENTUM_CROSSING_ENTRY_PRICE
                && short_ok_bear && accel_ok_bear
            {
                return Ok(StrategySignal::Entry {
                    params: OrderParams {
                        token_id: ctx.market.no_token,
                        price: ctx.snapshot.no_ask,
                        shares: trade_size / ctx.snapshot.no_ask,
                        fee_bps: ctx.market.no_fee_bps as u16,
                        is_neg_risk: ctx.market.is_neg_risk,
                        market_name: ctx.market.market_name.clone(),
                        condition_id: ctx.market.condition_id.clone(),
                    },
                    pair_params: None,
                });
            }
        } else {
            // Without strike
            if velocity > threshold && ctx.snapshot.yes_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                && short_ok_bull && accel_ok_bull
            {
                return Ok(StrategySignal::Entry {
                    params: OrderParams {
                        token_id: ctx.market.yes_token,
                        price: ctx.snapshot.yes_ask,
                        shares: trade_size / ctx.snapshot.yes_ask,
                        fee_bps: ctx.market.yes_fee_bps as u16,
                        is_neg_risk: ctx.market.is_neg_risk,
                        market_name: ctx.market.market_name.clone(),
                        condition_id: ctx.market.condition_id.clone(),
                    },
                    pair_params: None,
                });
            } else if velocity < -threshold && ctx.snapshot.no_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                && short_ok_bear && accel_ok_bear
            {
                return Ok(StrategySignal::Entry {
                    params: OrderParams {
                        token_id: ctx.market.no_token,
                        price: ctx.snapshot.no_ask,
                        shares: trade_size / ctx.snapshot.no_ask,
                        fee_bps: ctx.market.no_fee_bps as u16,
                        is_neg_risk: ctx.market.is_neg_risk,
                        market_name: ctx.market.market_name.clone(),
                        condition_id: ctx.market.condition_id.clone(),
                    },
                    pair_params: None,
                });
            }
        }

        Ok(StrategySignal::NoSignal)
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let pos_map = ctx.positions.lock().await;

        for ((strategy_name, token_id), position) in pos_map.iter() {
            if strategy_name != "MomentumStrategy" { continue; }
            let bid = if token_id == &ctx.market.yes_token { ctx.snapshot.yes_bid } else if token_id == &ctx.market.no_token { ctx.snapshot.no_bid } else { continue };

            let secs_held = (chrono::Utc::now() - position.opened_at).num_seconds();
            if position.fill_confirmed_at.is_none() {
                if secs_held < config::MOMENTUM_FILL_CONFIRM_MIN_HOLD_SECS { continue; }
                let profit_margin_check = (bid - position.avg_entry) / position.avg_entry;
                if profit_margin_check > -config::MOMENTUM_STOP_LOSS_PERCENT { continue; }
            }

            let avg_entry = position.avg_entry;
            let velocity = ctx.snapshot.velocity;
            let velocity_1s = ctx.snapshot.velocity_1s;
            let threshold = match ctx.crypto_filter.as_str() { "eth" => config::ETH_MOMENTUM_THRESHOLD, "sol" => config::SOL_MOMENTUM_THRESHOLD, _ => config::BTC_MOMENTUM_THRESHOLD };

            if avg_entry <= dec!(0) { continue; }
            let profit_margin = (bid - avg_entry) / avg_entry;

            // Near-expiry forced exit
            if let Some(close_time) = ctx.market.market_close_time {
                let secs_left = (close_time - chrono::Utc::now()).num_seconds();
                if secs_left <= config::MOMENTUM_EXPIRY_EXIT_SECS && profit_margin < config::MOMENTUM_EXPIRY_MIN_PROFIT_TO_HOLD {
                    return Ok(StrategySignal::Exit { params: OrderParams { token_id: *token_id, price: bid, shares: position.shares, fee_bps: if token_id == &ctx.market.yes_token { ctx.market.yes_fee_bps as u16 } else { ctx.market.no_fee_bps as u16 }, is_neg_risk: ctx.market.is_neg_risk, market_name: ctx.market.market_name.clone(), condition_id: ctx.market.condition_id.clone() }, reason: "NearExpiry".to_string(), exit_pair: false });
                }
            }

            let target = if avg_entry >= dec!(0.70) { dec!(0.05) } else { config::MOMENTUM_TARGET_PROFIT_PERCENT };
            let stop_loss = -config::MOMENTUM_STOP_LOSS_PERCENT;
            let reversal_threshold = -(threshold * config::MOMENTUM_REVERSAL_RATIO);

            if profit_margin >= target || bid >= config::MOMENTUM_TAKE_PROFIT_CEILING {
                return Ok(StrategySignal::Exit { params: OrderParams { token_id: *token_id, price: bid, shares: position.shares, fee_bps: if token_id == &ctx.market.yes_token { ctx.market.yes_fee_bps as u16 } else { ctx.market.no_fee_bps as u16 }, is_neg_risk: ctx.market.is_neg_risk, market_name: ctx.market.market_name.clone(), condition_id: ctx.market.condition_id.clone() }, reason: "TakeProfit".to_string(), exit_pair: false });
            }

            if profit_margin <= stop_loss {
                return Ok(StrategySignal::Exit { params: OrderParams { token_id: *token_id, price: bid, shares: position.shares, fee_bps: if token_id == &ctx.market.yes_token { ctx.market.yes_fee_bps as u16 } else { ctx.market.no_fee_bps as u16 }, is_neg_risk: ctx.market.is_neg_risk, market_name: ctx.market.market_name.clone(), condition_id: ctx.market.condition_id.clone() }, reason: "StopLoss".to_string(), exit_pair: false });
            }

            // Momentum Decay exit
            let decay_min = threshold * config::MOMENTUM_DECAY_EXIT_FRACTION;
            let is_yes = token_id == &ctx.market.yes_token;
            if profit_margin > dec!(0) && ((is_yes && velocity_1s < decay_min) || (!is_yes && velocity_1s > -decay_min)) {
                return Ok(StrategySignal::Exit { params: OrderParams { token_id: *token_id, price: bid, shares: position.shares, fee_bps: if token_id == &ctx.market.yes_token { ctx.market.yes_fee_bps as u16 } else { ctx.market.no_fee_bps as u16 }, is_neg_risk: ctx.market.is_neg_risk, market_name: ctx.market.market_name.clone(), condition_id: ctx.market.condition_id.clone() }, reason: "MomentumDecay".to_string(), exit_pair: false });
            }

            // Reversal exit
            if secs_held >= config::MOMENTUM_MIN_HOLD_SECS_BEFORE_REVERSAL && ((is_yes && velocity < reversal_threshold) || (!is_yes && velocity > -reversal_threshold)) {
                return Ok(StrategySignal::Exit { params: OrderParams { token_id: *token_id, price: bid, shares: position.shares, fee_bps: if token_id == &ctx.market.yes_token { ctx.market.yes_fee_bps as u16 } else { ctx.market.no_fee_bps as u16 }, is_neg_risk: ctx.market.is_neg_risk, market_name: ctx.market.market_name.clone(), condition_id: ctx.market.condition_id.clone() }, reason: "Reversal".to_string(), exit_pair: false });
            }
        }
        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { "MomentumStrategy".to_string() }
}

pub fn kelly_momentum_size(velocity: rust_decimal::Decimal, threshold: rust_decimal::Decimal) -> rust_decimal::Decimal {
    if threshold <= rust_decimal::Decimal::ZERO { return config::MOMENTUM_MIN_TRADE_SIZE_USDC; }
    let strength = (velocity.abs() / threshold).max(rust_decimal::Decimal::ONE).min(config::MOMENTUM_KELLY_MAX_MULTIPLIER);
    let fraction = (strength - rust_decimal::Decimal::ONE) / (config::MOMENTUM_KELLY_MAX_MULTIPLIER - rust_decimal::Decimal::ONE);
    config::MOMENTUM_MIN_TRADE_SIZE_USDC + fraction * (config::MOMENTUM_MAX_TRADE_SIZE_USDC - config::MOMENTUM_MIN_TRADE_SIZE_USDC)
}
