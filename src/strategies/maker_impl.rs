/// Maker Strategy - Strategy Trait Implementation
///
/// Posts passive limit orders (bids) below the ask price to earn maker rebates
/// instead of paying taker fees. Only fires when the spread is wide enough to
/// justify the adverse-selection risk and expiry is far enough away for a fill.
///
/// Entry: bid = best_bid + MAKER_BID_IMPROVEMENT (posted passively, not lifted)
/// Exit:  take-profit at MAKER_TARGET_PROFIT_PERCENT or stop-loss at MAKER_STOP_LOSS_PERCENT

use async_trait::async_trait;
use anyhow::Result;
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::debug;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus};
use crate::config;

/// Velocity threshold (oracle USD/s) above which we consider the market
/// strongly directional and suppress the adverse maker side.
/// BTC moves ~$75/5s = $15/s in a fast move; $25/s = clearly trending.
const MAKER_VELOCITY_BIAS_THRESHOLD: Decimal = dec!(25.0);

pub struct MakerStrategyImpl;

#[async_trait]
impl Strategy for MakerStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        if !config::ENABLE_MAKER_TRADING {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Fee gate ─────────────────────────────────────────────────────────
        // With high fees the round-trip cost exceeds the take-profit target —
        // no entry will ever be profitable, so block entirely.
        if ctx.market.yes_fee_bps > config::MAKER_MAX_FEE_BPS
            || ctx.market.no_fee_bps > config::MAKER_MAX_FEE_BPS
        {
            debug!("🚫 MakerStrategy blocked: fees too high (YES {}bps / NO {}bps > {}bps threshold)",
                ctx.market.yes_fee_bps, ctx.market.no_fee_bps, config::MAKER_MAX_FEE_BPS);
            return Ok(StrategySignal::NoSignal);
        }

        // ── Market maturation gate ────────────────────────────────────────────
        // New hourly markets have wild initial pricing for the first several minutes.
        // Entering during this window leads to buying local peaks that immediately revert.
        let secs_since_market_start = (Utc::now() - ctx.market_started_at).num_seconds();
        if secs_since_market_start < config::MAKER_MIN_MARKET_AGE_SECS {
            debug!("🚫 MakerStrategy blocked: market too young ({}s < {}s maturation gate)",
                secs_since_market_start, config::MAKER_MIN_MARKET_AGE_SECS);
            return Ok(StrategySignal::NoSignal);
        }

        // Don't enter if market is too close to expiry
        if let Some(close_time) = ctx.market.market_close_time {
            let secs_to_expiry = (close_time - Utc::now()).num_seconds();
            if secs_to_expiry < config::MAKER_MIN_SECS_TO_EXPIRY {
                return Ok(StrategySignal::NoSignal);
            }
        } else {
            // No close time known — skip to be safe
            return Ok(StrategySignal::NoSignal);
        }

        let yes_ask = ctx.snapshot.yes_ask;
        let yes_bid = ctx.snapshot.yes_bid;
        let no_ask = ctx.snapshot.no_ask;
        let no_bid = ctx.snapshot.no_bid;

        // Don't enter if MakerStrategy already holds a position on either token
        {
            let pos_map = ctx.positions.lock().await;
            if pos_map.contains_key(&("MakerStrategy".to_string(), ctx.market.yes_token))
                || pos_map.contains_key(&("MakerStrategy".to_string(), ctx.market.no_token))
            {
                return Ok(StrategySignal::NoSignal);
            }
        }

        let yes_spread = yes_ask - yes_bid;
        let no_spread = no_ask - no_bid;

        // Velocity filter: if the oracle is moving strongly in one direction,
        // suppress the maker bid on the token that would be adversely selected.
        // A strongly FALLING oracle (negative velocity) means YES is likely to
        // drop further — don't post YES bids into that move.
        // A strongly RISING oracle means NO is likely to drop — skip NO bids.
        let velocity = ctx.snapshot.velocity;
        let velocity_bias_strong_negative = velocity <= -MAKER_VELOCITY_BIAS_THRESHOLD;
        let velocity_bias_strong_positive = velocity >= MAKER_VELOCITY_BIAS_THRESHOLD;

        // Evaluate both sides independently, then pick the one with the wider spread.
        // Previously YES was always evaluated first and returned immediately — this caused
        // a hard YES bias even during falling markets where NO was the better side.
        let yes_bid_price = yes_bid + config::MAKER_BID_IMPROVEMENT;
        let yes_qualifies = yes_spread >= config::MAKER_MIN_SPREAD
            && yes_bid_price >= config::MAKER_MIN_ENTRY_PRICE
            && yes_bid_price <= config::MAKER_MAX_ENTRY_PRICE
            && no_bid <= config::MAKER_MAX_ENTRY_PRICE   // complementary check: market not too directional
            && !velocity_bias_strong_negative;           // don't post YES into a falling oracle

        let no_bid_price = no_bid + config::MAKER_BID_IMPROVEMENT;
        let no_qualifies = no_spread >= config::MAKER_MIN_SPREAD
            && no_bid_price >= config::MAKER_MIN_ENTRY_PRICE
            && no_bid_price <= config::MAKER_MAX_ENTRY_PRICE
            && yes_bid <= config::MAKER_MAX_ENTRY_PRICE  // complementary check: market not too directional
            && !velocity_bias_strong_positive;           // don't post NO into a rising oracle

        // Pick the side with the wider spread — more spread = more edge and less adverse selection.
        // If only one side qualifies, use that one. If neither, no signal.
        match (yes_qualifies, no_qualifies) {
            (true, true) => {
                let token_id = if yes_spread >= no_spread {
                    ctx.market.yes_token
                } else {
                    ctx.market.no_token
                };
                return Ok(StrategySignal::Entry { token_id });
            }
            (true, false) => return Ok(StrategySignal::Entry { token_id: ctx.market.yes_token }),
            (false, true) => return Ok(StrategySignal::Entry { token_id: ctx.market.no_token }),
            (false, false) => {}
        }

        Ok(StrategySignal::NoSignal)
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let pos_map = ctx.positions.lock().await;

        for token_id in [ctx.market.yes_token, ctx.market.no_token] {
            let Some(position) = pos_map.get(&("MakerStrategy".to_string(), token_id)) else {
                continue;
            };

            let bid = if token_id == ctx.market.yes_token {
                ctx.snapshot.yes_bid
            } else {
                ctx.snapshot.no_bid
            };

            if position.avg_entry <= dec!(0) {
                continue;
            }

            let profit_pct = (bid - position.avg_entry) / position.avg_entry;
            let secs_since_open = (Utc::now() - position.opened_at).num_seconds();

            // Take-profit: only evaluate after fill is confirmed on-chain.
            // Without this guard, the sentinel position (inserted before the order is sent)
            // would trigger a phantom take-profit on the very next tick.
            if position.fill_confirmed_at.is_some() && profit_pct >= config::MAKER_TARGET_PROFIT_PERCENT {
                return Ok(StrategySignal::Exit {
                    token_id,
                    reason: format!(
                        "Maker take-profit: bid=${:.4}, entry=${:.4}, gain={:.2}%",
                        bid,
                        position.avg_entry,
                        profit_pct * dec!(100)
                    ),
                });
            }

            // Stop-loss: require fill confirmation before firing.
            // Previously we fired on unconfirmed (phantom) positions, which caused
            // a wasteful sell → "balance: 0" → phantom cleanup → cooldown cycle on
            // every GTD order that never matched.  The 60s sync timeout in
            // sync_position_balance already removes true phantoms gracefully.
            if position.fill_confirmed_at.is_some()
                && profit_pct <= -config::MAKER_STOP_LOSS_PERCENT
                && secs_since_open >= config::MIN_HOLD_SECS_BEFORE_STOP_LOSS
            {
                return Ok(StrategySignal::Exit {
                    token_id,
                    reason: format!(
                        "Maker stop-loss: bid=${:.4}, entry=${:.4}, loss={:.2}% ({}s since open)",
                        bid,
                        position.avg_entry,
                        profit_pct * dec!(100),
                        secs_since_open,
                    ),
                });
            }
        }

        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus {
        if config::ENABLE_MAKER_TRADING {
            StrategyStatus::Active
        } else {
            StrategyStatus::Disabled
        }
    }

    fn name(&self) -> String {
        "MakerStrategy".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use alloy::primitives::U256;
    use chrono::{Duration, Utc};
    use crate::state::{MarketConfig, MarketSnapshot, PositionMap};

    fn make_ctx(
        yes_bid: Decimal,
        yes_ask: Decimal,
        no_bid: Decimal,
        no_ask: Decimal,
        secs_to_expiry: i64,
        positions: PositionMap,
    ) -> StrategyContext {
        StrategyContext {
            market: MarketConfig {
                yes_token: U256::from(1u64),
                no_token: U256::from(2u64),
                market_name: "Test Market".to_string(),
                market_close_time: Some(Utc::now() + Duration::seconds(secs_to_expiry)),
                strike_price: None,
                is_neg_risk: false,
                yes_fee_bps: 100,
                no_fee_bps: 100,
            },
            snapshot: MarketSnapshot {
                yes_bid,
                yes_ask,
                yes_ask_depth: dec!(500),
                no_bid,
                no_ask,
                no_ask_depth: dec!(500),
                oracle_price: dec!(74000),
                velocity: dec!(0),
                timestamp: Utc::now(),
            },
            positions: Arc::new(tokio::sync::Mutex::new(positions)),
            crypto_filter: "btc".to_string(),
            // Simulate a market that has been running for 20 minutes (past the maturation gate)
            market_started_at: Utc::now() - Duration::seconds(1200),
        }
    }

    #[tokio::test]
    async fn test_maker_no_entry_fees_too_high() {
        let strategy = MakerStrategyImpl;
        // Wide spread but 1000 bps fees > MAKER_MAX_FEE_BPS (200) ✗
        let mut ctx = make_ctx(dec!(0.30), dec!(0.40), dec!(0.55), dec!(0.58), 2400, PositionMap::new());
        ctx.market.yes_fee_bps = 1000;
        ctx.market.no_fee_bps = 1000;
        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::NoSignal));
    }

    #[tokio::test]
    async fn test_maker_no_entry_market_too_young() {
        let strategy = MakerStrategyImpl;
        // Wide spread, low fees, but market only started 60s ago ✗
        let mut ctx = make_ctx(dec!(0.30), dec!(0.40), dec!(0.55), dec!(0.58), 2400, PositionMap::new());
        ctx.market_started_at = Utc::now() - Duration::seconds(60);
        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::NoSignal));
    }

    #[tokio::test]
    async fn test_maker_entry_wide_spread_yes() {
        let strategy = MakerStrategyImpl;
        // YES spread = 0.40 - 0.30 = 0.10 >= MAKER_MIN_SPREAD (0.05) ✓
        // bid_price = 0.30 + 0.01 = 0.31 <= MAKER_MAX_ENTRY_PRICE (0.65) ✓
        let ctx = make_ctx(dec!(0.30), dec!(0.40), dec!(0.55), dec!(0.58), 2400, PositionMap::new());
        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::Entry { .. }));
    }

    #[tokio::test]
    async fn test_maker_no_entry_tight_spread() {
        let strategy = MakerStrategyImpl;
        // YES spread = 0.51 - 0.50 = 0.01 < MAKER_MIN_SPREAD (0.05) ✗
        // NO spread  = 0.50 - 0.49 = 0.01 < MAKER_MIN_SPREAD (0.05) ✗
        let ctx = make_ctx(dec!(0.50), dec!(0.51), dec!(0.49), dec!(0.50), 2400, PositionMap::new());
        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::NoSignal));
    }

    #[tokio::test]
    async fn test_maker_no_entry_too_close_to_expiry() {
        let strategy = MakerStrategyImpl;
        // Wide spread but only 300s to expiry < MAKER_MIN_SECS_TO_EXPIRY (1800) ✗
        let ctx = make_ctx(dec!(0.30), dec!(0.40), dec!(0.55), dec!(0.58), 300, PositionMap::new());
        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::NoSignal));
    }

    #[tokio::test]
    async fn test_maker_no_entry_price_too_high() {
        let strategy = MakerStrategyImpl;
        // YES bid_price = 0.72 + 0.01 = 0.73 > MAKER_MAX_ENTRY_PRICE (0.55) ✗
        let ctx = make_ctx(dec!(0.72), dec!(0.80), dec!(0.10), dec!(0.13), 2400, PositionMap::new());
        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        // YES blocked by price cap; NO spread = 0.03 < 0.05 also blocked
        assert!(matches!(signal, StrategySignal::NoSignal));
    }

    #[tokio::test]
    async fn test_maker_exit_take_profit() {
        use crate::state::Position;
        let strategy = MakerStrategyImpl;
        let yes_token = U256::from(1u64);
        let mut positions = PositionMap::new();
        positions.insert(("MakerStrategy".to_string(), yes_token), Position {
            shares: dec!(20),
            avg_entry: dec!(0.30),
            opened_at: Utc::now(),
            close_time: None,
            market_name: "Test Market".to_string(),
            pair_token_id: yes_token,
            fill_confirmed_at: Some(Utc::now()),
        });
        // bid = 0.32, entry = 0.30 → profit = 6.67% < MAKER_TARGET_PROFIT_PERCENT (8%) ✗ → no exit yet
        let ctx = make_ctx(dec!(0.30), dec!(0.40), dec!(0.55), dec!(0.58), 2400, positions);
        // Override yes_bid to 0.33 (10% gain) to trigger take-profit
        let mut ctx2 = ctx.clone();
        ctx2.snapshot.yes_bid = dec!(0.33);
        let signal = strategy.evaluate_exit(&ctx2).await.unwrap();
        assert!(matches!(signal, StrategySignal::Exit { .. }));
    }

    #[tokio::test]
    async fn test_maker_exit_stop_loss() {
        use crate::state::Position;
        let strategy = MakerStrategyImpl;
        let yes_token = U256::from(1u64);
        let mut positions = PositionMap::new();
        positions.insert(("MakerStrategy".to_string(), yes_token), Position {
            shares: dec!(20),
            avg_entry: dec!(0.30),
            opened_at: Utc::now() - Duration::seconds(config::MIN_HOLD_SECS_BEFORE_STOP_LOSS),
            close_time: None,
            market_name: "Test Market".to_string(),
            pair_token_id: yes_token,
            fill_confirmed_at: Some(Utc::now()),
        });
        // bid = 0.27, entry = 0.30 → loss = -10% <= -MAKER_STOP_LOSS_PERCENT (-5%) ✓
        let ctx = make_ctx(dec!(0.30), dec!(0.40), dec!(0.55), dec!(0.58), 2400, positions);
        let mut ctx2 = ctx.clone();
        ctx2.snapshot.yes_bid = dec!(0.27);
        let signal = strategy.evaluate_exit(&ctx2).await.unwrap();
        assert!(matches!(signal, StrategySignal::Exit { .. }));
    }
}
