/// Arbitrage Strategy
///
/// Hedged, two-sided trades that exploit the YES+NO spread inefficiency.
/// This version is venue-aware and performs its own risk management.

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams};
use crate::strategies::is_drawdown_limit_hit;
use crate::config;

const STRATEGY_NAME: &str = "ArbitrageStrategy";

pub struct ArbitrageStrategyImpl;

#[async_trait]
impl Strategy for ArbitrageStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        // ── Global Risk Check ────────────────────────────────────────────────
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            return Ok(StrategySignal::NoSignal);
        }

        let market   = ctx.maker_market.as_ref().unwrap_or(&ctx.market);
        let snapshot = ctx.maker_snapshot.as_ref().unwrap_or(&ctx.snapshot);

        let yes_ask = snapshot.yes_ask;
        let no_ask  = snapshot.no_ask;
        let yes_fee_bps = market.yes_fee_bps;
        let no_fee_bps  = market.no_fee_bps;

        if yes_fee_bps > config::ARBITRAGE_MAX_TAKER_FEE_BPS || no_fee_bps > config::ARBITRAGE_MAX_TAKER_FEE_BPS {
            return Ok(StrategySignal::NoSignal);
        }

        if is_arbitrage_profitable(yes_ask, no_ask, yes_fee_bps, no_fee_bps) {
            let trade_size = dec!(10.0);

            // ── Strategy Exposure Check ──────────────────────────────────────────
            let current_exposure = {
                let pos_map = ctx.positions.lock().await;
                pos_map.iter()
                    .filter(|((s, _), _)| s == STRATEGY_NAME)
                    .map(|(_, p)| p.shares * p.avg_entry)
                    .sum::<Decimal>()
            };

            // Hedged exposure: we only count one leg (per legacy risk.rs logic)
            if current_exposure + trade_size > config::ARBITRAGE_MAX_EXPOSURE_USDC {
                return Ok(StrategySignal::NoSignal);
            }

            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id: market.yes_token,
                    price: yes_ask,
                    shares: trade_size / yes_ask,
                    fee_bps: yes_fee_bps as u16,
                    is_neg_risk: market.is_neg_risk,
                    market_name: market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                },
                pair_params: Some(OrderParams {
                    token_id: market.no_token,
                    price: no_ask,
                    shares: trade_size / no_ask,
                    fee_bps: no_fee_bps as u16,
                    is_neg_risk: market.is_neg_risk,
                    market_name: market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                }),
            });
        }

        Ok(StrategySignal::NoSignal)
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let market   = ctx.maker_market.as_ref().unwrap_or(&ctx.market);
        let snapshot = ctx.maker_snapshot.as_ref().unwrap_or(&ctx.snapshot);
        let pos_map = ctx.positions.lock().await;

        let yes_key = (STRATEGY_NAME.to_string(), market.yes_token);
        let no_key  = (STRATEGY_NAME.to_string(), market.no_token);

        if let (Some(yp), Some(_np)) = (pos_map.get(&yes_key), pos_map.get(&no_key)) {
            let yes_bid = snapshot.yes_bid;
            let no_bid = snapshot.no_bid;

            if yes_bid + no_bid >= config::EARLY_EXIT_COMBINED_BID_THRESHOLD {
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: market.yes_token,
                        price: yes_bid,
                        shares: yp.shares,
                        fee_bps: market.yes_fee_bps as u16,
                        is_neg_risk: market.is_neg_risk,
                        market_name: market.market_name.clone(),
                        condition_id: market.condition_id.clone(),
                    },
                    reason: "Arbitrage convergence".to_string(),
                    exit_pair: true,
                });
            }
        }
        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { STRATEGY_NAME.to_string() }
    fn venue(&self) -> &'static str { "Window/Daily" }
    fn max_exposure(&self) -> rust_decimal::Decimal { crate::config::ARBITRAGE_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Gross hedged (per leg)" }
}

fn is_arbitrage_profitable(yes_ask: rust_decimal::Decimal, no_ask: rust_decimal::Decimal, y_fee: u32, n_fee: u32) -> bool {
    let combined_ask = yes_ask + no_ask;
    let fees = (yes_ask * rust_decimal::Decimal::from(y_fee) / dec!(10_000)) + (no_ask * rust_decimal::Decimal::from(n_fee) / dec!(10_000));
    (dec!(1.0) - combined_ask - fees) >= config::ARBITRAGE_PROFIT_THRESHOLD
}
