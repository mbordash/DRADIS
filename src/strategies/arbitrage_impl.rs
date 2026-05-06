/// Arbitrage Strategy
///
/// Hedged, two-sided trades that exploit the YES+NO spread inefficiency.
///
/// ── Maker vs Taker ──────────────────────────────────────────────────────────
/// Polymarket charges 0% fees on maker (GTC/post-only) orders, but 1000 bps
/// on taker (FAK) fills.  At 10% round-trip cost, taker arb is structurally
/// impossible on a $1.00 binary market.
///
/// This strategy posts GTC maker bids on BOTH YES and NO tokens simultaneously
/// at their current best-bid prices.  If both legs fill:
///   cost  = YES_bid + NO_bid  (no fee — maker fill)
///   payout = $1.00  (settlement)
///   profit = 1.00 − YES_bid − NO_bid
///
/// Entry fires only when that profit > ARBITRAGE_PROFIT_THRESHOLD (1.5¢).
/// In practice combined bids of 0.97-0.98 are common on daily markets,
/// yielding 2-3¢ net per dollar — viable as maker, not as taker.
///
/// Exit: collect settlement at $1.00 or sell early when bid_sum converges
/// to EARLY_EXIT_COMBINED_BID_THRESHOLD (0.995).  Exit legs use FAK (taker)
/// since we need a guaranteed fill before market close.

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams};
use crate::strategies::is_drawdown_limit_hit;
use crate::config;
use polymarket_client_sdk_v2::clob::types::OrderType;

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

        let yes_bid = snapshot.yes_bid;
        let no_bid  = snapshot.no_bid;

        // ── Maker arb profitability gate (0% fee on GTC fills) ───────────────
        if !is_maker_arb_profitable(yes_bid, no_bid) {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Strategy Exposure Check ──────────────────────────────────────────
        let trade_size = dec!(10.0);
        let current_exposure = {
            let pos_map = ctx.positions.lock().await;
            pos_map.iter()
                .filter(|((s, _), _)| s == STRATEGY_NAME)
                .map(|(_, p)| p.shares * p.avg_entry)
                .sum::<Decimal>()
        };
        if current_exposure + trade_size > config::ARBITRAGE_MAX_EXPOSURE_USDC {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Post GTC maker bids on both legs ─────────────────────────────────
        // Equal shares on both legs are required for a true hedge.
        // Buying equal dollars gives unequal shares (e.g. 24 YES vs 17 NO at 0.41/0.58),
        // leaving the cheaper leg unhedged and creating a directional P&L on settlement.
        // Instead: spend the full budget on N balanced pairs where
        //   N = trade_size / (yes_bid + no_bid)
        // so YES_cost + NO_cost = trade_size and every YES share has one NO share.
        let pair_shares = trade_size / (yes_bid + no_bid);

        return Ok(StrategySignal::Entry {
            params: OrderParams {
                token_id: market.yes_token,
                price: yes_bid,                        // bid at current best bid
                shares: pair_shares,                   // balanced — same count as NO leg
                fee_bps: 0,                            // maker = 0 fees
                is_neg_risk: market.is_neg_risk,
                market_name: market.market_name.clone(),
                condition_id: market.condition_id.clone(),
                order_type: OrderType::GTC,            // rest on the book as maker
                post_only: true,                       // reject if it would cross (no accidental taker)
                ghost_mode: config::GHOST_MODE,
            },
            pair_params: Some(OrderParams {
                token_id: market.no_token,
                price: no_bid,
                shares: pair_shares,                   // same count as YES leg
                fee_bps: 0,
                is_neg_risk: market.is_neg_risk,
                market_name: market.market_name.clone(),
                condition_id: market.condition_id.clone(),
                order_type: OrderType::GTC,
                post_only: true,
                ghost_mode: config::GHOST_MODE,
            }),
        });
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let market   = ctx.maker_market.as_ref().unwrap_or(&ctx.market);
        let snapshot = ctx.maker_snapshot.as_ref().unwrap_or(&ctx.snapshot);
        let pos_map = ctx.positions.lock().await;

        let yes_key = (STRATEGY_NAME.to_string(), market.yes_token);
        let no_key  = (STRATEGY_NAME.to_string(), market.no_token);

        if let (Some(yp), Some(_np)) = (pos_map.get(&yes_key), pos_map.get(&no_key)) {
            let yes_bid = snapshot.yes_bid;
            let no_bid  = snapshot.no_bid;

            // ── Fee-adjusted early-exit gate ──────────────────────────────────
            // Exiting via FAK (taker) incurs a fee on each leg.  The combined bid
            // must exceed $1.00 PLUS the total taker-fee cost for the FAK exit to
            // actually improve on holding to settlement ($1.00, 0% fee).
            //
            //   fee_yes = yes_fee_bps / 10_000 (e.g. 1000 bps → 0.10 / share)
            //   fee_no  = no_fee_bps  / 10_000
            //   threshold = 1.00 + fee_yes + fee_no
            //
            // At 1000 bps per side the threshold is 1.20 — structurally unreachable
            // on a binary market — so positions correctly settle at $1.00 (0% fee).
            // If Polymarket ever lowers taker fees, this will start firing again.
            let yes_fee_rate = Decimal::from(market.yes_fee_bps) / dec!(10000);
            let no_fee_rate  = Decimal::from(market.no_fee_bps)  / dec!(10000);
            let early_exit_threshold = dec!(1.0) + yes_fee_rate + no_fee_rate;

            // Exit early when combined bid has converged enough to cover fees
            if yes_bid + no_bid >= early_exit_threshold {
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: market.yes_token,
                        price: yes_bid,
                        shares: yp.shares,
                        fee_bps: market.yes_fee_bps as u16, // taker exit — fee applies
                        is_neg_risk: market.is_neg_risk,
                        market_name: market.market_name.clone(),
                        condition_id: market.condition_id.clone(),
                        order_type: OrderType::FAK,   // guaranteed exit before close
                        post_only: false,
                        ghost_mode: config::GHOST_MODE,
                    },
                    reason: "Arbitrage convergence".to_string(),
                    exit_pair: true,
                });
            }
        }
        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { "ArbitrageStrategy".to_string() }
    fn venue(&self) -> &'static str { "Window/Daily" }
    fn max_exposure(&self) -> rust_decimal::Decimal { crate::config::ARBITRAGE_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Gross hedged (per leg)" }
}

/// Returns true when posting GTC maker bids on both legs is profitable.
///
/// Maker fills incur 0% fee on Polymarket.  Combined cost = YES_bid + NO_bid.
/// Settlement always pays $1.00 per pair.
/// Profit = 1.00 − YES_bid − NO_bid ≥ ARBITRAGE_PROFIT_THRESHOLD.
fn is_maker_arb_profitable(yes_bid: Decimal, no_bid: Decimal) -> bool {
    (dec!(1.0) - yes_bid - no_bid) >= config::ARBITRAGE_PROFIT_THRESHOLD
}
