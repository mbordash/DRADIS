/// Momentum Trading Strategy
///
/// One-sided, non-hedged trades based on Binance price oracle signals.
/// Entry triggers when price velocity exceeds threshold and market conditions align.
/// Exits via take-profit, stop-loss, or reversal detection.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use alloy::primitives::U256;
use chrono::{DateTime, Utc};

use crate::config;

#[derive(Debug, Clone)]
pub struct MomentumPosition {
    pub token_id: U256,
    pub shares: Decimal,
    pub avg_entry_price: Decimal,
    pub entry_time: DateTime<Utc>,
    pub market_name: String,
}

pub struct MomentumStrategy;

impl MomentumStrategy {
    /// Evaluate if a momentum signal should trigger an entry
    /// Returns Some(token_id) if conditions are met, None otherwise
    pub fn evaluate_entry(
        velocity: Decimal,
        binance_price: Decimal,
        strike_price: Option<Decimal>,
        yes_token: U256,
        no_token: U256,
        yes_ask: Decimal,
        no_ask: Decimal,
        crypto_filter: &str,
    ) -> Option<U256> {
        let threshold = match crypto_filter {
            "eth" => config::ETH_MOMENTUM_THRESHOLD,
            "sol" => config::SOL_MOMENTUM_THRESHOLD,
            _ => config::BTC_MOMENTUM_THRESHOLD,
        };

        let strike_buffer = match crypto_filter {
            "eth" => config::ETH_STRIKE_BUFFER,
            "sol" => config::SOL_STRIKE_BUFFER,
            _ => config::BTC_STRIKE_BUFFER,
        };

        let strike = strike_price?;

        if velocity > threshold && binance_price > (strike + strike_buffer) && yes_ask <= config::MAX_MOMENTUM_ENTRY_PRICE {
            Some(yes_token)
        } else if velocity < -threshold && binance_price < (strike - strike_buffer) && no_ask <= config::MAX_MOMENTUM_ENTRY_PRICE {
            Some(no_token)
        } else {
            None
        }
    }

    /// Check if we should exit a momentum position
    /// Returns Some(exit_reason) if we should exit
    pub fn should_exit_momentum(
        position_bid: Decimal,
        avg_entry: Decimal,
        velocity: Decimal,
        threshold: Decimal,
        _crypto_filter: &str,
    ) -> Option<ExitReason> {
        if avg_entry <= dec!(0) {
            return None;
        }

        let profit_margin = (position_bid - avg_entry) / avg_entry;
        let target = if avg_entry >= dec!(0.70) { dec!(0.05) } else { config::MOMENTUM_TARGET_PROFIT_PERCENT };
        let stop_loss = -config::MOMENTUM_STOP_LOSS_PERCENT;
        let reversal_threshold = threshold * config::MOMENTUM_REVERSAL_RATIO;

        if profit_margin >= target || position_bid >= config::MOMENTUM_TAKE_PROFIT_CEILING {
            Some(ExitReason::TakeProfit {
                bid_price: position_bid,
                profit_pct: profit_margin,
                target_pct: target,
            })
        } else if profit_margin <= stop_loss {
            Some(ExitReason::StopLoss {
                bid_price: position_bid,
                loss_pct: profit_margin,
            })
        } else if velocity.abs() < reversal_threshold {
            Some(ExitReason::Reversal {
                velocity,
                threshold: reversal_threshold,
            })
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub enum ExitReason {
    TakeProfit {
        bid_price: Decimal,
        profit_pct: Decimal,
        target_pct: Decimal,
    },
    StopLoss {
        bid_price: Decimal,
        loss_pct: Decimal,
    },
    Reversal {
        velocity: Decimal,
        threshold: Decimal,
    },
}




