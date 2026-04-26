/// Time Decay (Theta) Strategy
///
/// Exploits YES+NO price convergence toward $1.00 as hourly markets approach expiry.
/// This version is venue-aware and performs its own risk management.

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use chrono::{DateTime, Utc};
use alloy::primitives::U256;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams};
use crate::strategies::is_drawdown_limit_hit;
use crate::config;

pub struct TimeDecayStrategyImpl;

#[async_trait]
impl Strategy for TimeDecayStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        if !config::ENABLE_TIME_DECAY_TRADING {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Global Risk Check ────────────────────────────────────────────────
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            return Ok(StrategySignal::NoSignal);
        }

        let (market, snap) = if let (Some(mk_mkt), Some(mk_snap)) = (&ctx.maker_market, &ctx.maker_snapshot) {
            (mk_mkt, mk_snap)
        } else {
            (&ctx.market, &ctx.snapshot)
        };

        let seconds_to_expiry = match market.market_close_time {
            Some(close_time) => (close_time - Utc::now()).num_seconds(),
            None => return Ok(StrategySignal::NoSignal),
        };

        if !TimeDecayStrategy::is_in_theta_window(seconds_to_expiry) {
            return Ok(StrategySignal::NoSignal);
        }

        if TimeDecayStrategy::calculate_theta_opportunity(
            snap.yes_ask, snap.no_ask, market.yes_fee_bps, market.no_fee_bps, seconds_to_expiry,
        ).is_some() {
            let trade_size = config::TIME_DECAY_POSITION_SIZE_USDC;

            // ── Strategy Exposure Check ──────────────────────────────────────────
            let current_exposure = {
                let pos_map = ctx.positions.lock().await;
                pos_map.iter()
                    .filter(|((s, _), _)| s == "TimeDecayStrategy")
                    .map(|(_, p)| p.shares * p.avg_entry)
                    .sum::<Decimal>()
            };

            // Hedged exposure: we only count one leg
            if current_exposure + trade_size > config::TIME_DECAY_MAX_EXPOSURE_USDC {
                return Ok(StrategySignal::NoSignal);
            }

            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id: market.yes_token,
                    price: snap.yes_ask,
                    shares: trade_size / snap.yes_ask,
                    fee_bps: market.yes_fee_bps as u16,
                    is_neg_risk: market.is_neg_risk,
                    market_name: market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                },
                pair_params: Some(OrderParams {
                    token_id: market.no_token,
                    price: snap.no_ask,
                    shares: trade_size / snap.no_ask,
                    fee_bps: market.no_fee_bps as u16,
                    is_neg_risk: market.is_neg_risk,
                    market_name: market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                }),
            });
        }
        Ok(StrategySignal::NoSignal)
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let pos_map = ctx.positions.lock().await;

        let (market, snap) = if let Some(mk) = &ctx.maker_market {
            if pos_map.contains_key(&("TimeDecayStrategy".to_string(), mk.yes_token)) {
                (mk, ctx.maker_snapshot.as_ref().unwrap())
            } else { (&ctx.market, &ctx.snapshot) }
        } else { (&ctx.market, &ctx.snapshot) };

        let yes_key = ("TimeDecayStrategy".to_string(), market.yes_token);
        let no_key  = ("TimeDecayStrategy".to_string(), market.no_token);

        if let (Some(yp), Some(_)) = (pos_map.get(&yes_key), pos_map.get(&no_key)) {
            let yes_bid = snap.yes_bid;
            let no_bid = snap.no_bid;

            if TimeDecayStrategy::should_convergence_exit(yes_bid, no_bid) {
                return Ok(StrategySignal::Exit {
                    params: OrderParams { token_id: market.yes_token, price: yes_bid, shares: yp.shares, fee_bps: market.yes_fee_bps as u16, is_neg_risk: market.is_neg_risk, market_name: market.market_name.clone(), condition_id: market.condition_id.clone() },
                    reason: "Time Decay convergence".to_string(),
                    exit_pair: true,
                });
            }

            let combined_bid = yes_bid + no_bid;
            if combined_bid < config::TIME_DECAY_CONVERGENCE_EXIT_BID * (dec!(1) - config::TIME_DECAY_STOP_LOSS_PERCENT) {
                return Ok(StrategySignal::Exit {
                    params: OrderParams { token_id: market.yes_token, price: yes_bid, shares: yp.shares, fee_bps: market.yes_fee_bps as u16, is_neg_risk: market.is_neg_risk, market_name: market.market_name.clone(), condition_id: market.condition_id.clone() },
                    reason: "Time Decay SL".to_string(),
                    exit_pair: true,
                });
            }

            if let Some(close_time) = market.market_close_time {
                if (close_time - Utc::now()).num_seconds() < config::MARKET_EXPIRY_SAFETY_BUFFER_SECS as i64 {
                    return Ok(StrategySignal::Exit {
                        params: OrderParams { token_id: market.yes_token, price: yes_bid, shares: yp.shares, fee_bps: market.yes_fee_bps as u16, is_neg_risk: market.is_neg_risk, market_name: market.market_name.clone(), condition_id: market.condition_id.clone() },
                        reason: "Time Decay Expiry".to_string(),
                        exit_pair: true,
                    });
                }
            }
        }
        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { "TimeDecayStrategy".to_string() }
}

pub struct TimeDecayStrategy;

impl TimeDecayStrategy {
    pub fn calculate_theta_opportunity(yes_ask: Decimal, no_ask: Decimal, y_fee: u32, n_fee: u32, secs: i64) -> Option<ThetaSignal> {
        let comb = yes_ask + no_ask;
        let fees = (yes_ask * Decimal::from(y_fee) / dec!(10_000)) + (no_ask * Decimal::from(n_fee) / dec!(10_000));
        let net = dec!(1.0) - comb - fees;
        if net >= config::MIN_TIME_DECAY_NET_PROFIT { return Some(ThetaSignal { mode: ThetaMode::Settlement, combined_ask: comb, net_profit_per_share: net, total_fees: fees }); }
        if comb <= config::MAX_TIME_DECAY_COMBINED_ASK && secs < config::TIME_DECAY_CONVERGENCE_WINDOW_SECS {
            let target = config::TIME_DECAY_CONVERGENCE_EXIT_BID;
            let est = target - comb - fees;
            if est > dec!(-0.005) { return Some(ThetaSignal { mode: ThetaMode::Convergence, combined_ask: comb, net_profit_per_share: est, total_fees: fees }); }
        }
        None
    }
    pub fn is_in_theta_window(secs: i64) -> bool { secs >= config::TIME_DECAY_MIN_SECS_TO_EXPIRY && secs <= config::TIME_DECAY_MAX_SECS_TO_EXPIRY }
    pub fn should_convergence_exit(yb: Decimal, nb: Decimal) -> bool { yb + nb >= config::TIME_DECAY_CONVERGENCE_EXIT_BID }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ThetaMode { Settlement, Convergence }

pub struct ThetaSignal { pub mode: ThetaMode, pub combined_ask: Decimal, pub net_profit_per_share: Decimal, pub total_fees: Decimal }

pub struct TimeDecayPosition { pub yes_token_id: U256, pub no_token_id: U256, pub entry_time: DateTime<Utc>, pub expiry_time: DateTime<Utc>, pub yes_entry_price: Decimal, pub no_entry_price: Decimal, pub position_size: Decimal, pub total_invested: Decimal, pub mode: ThetaMode }

impl TimeDecayPosition {
    pub fn time_to_expiry(&self) -> i64 { (self.expiry_time - Utc::now()).num_seconds() }
    pub fn is_expired(&self) -> bool { self.time_to_expiry() <= 0 }
}
