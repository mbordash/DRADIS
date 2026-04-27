# Writing a Custom Strategy

This guide walks through implementing and integrating a new trading strategy into RustPolyBot using the new "Strategy is Sovereign" architecture.

---

## How the Orchestrator Works

Every 50ms the main loop:

1. Reads the latest `MarketSnapshot` (Polymarket prices + Binance oracle data).
2. Builds a `StrategyContext` containing everything needed to make a decision.
3. Calls `execute_strategies_concurrent()` — runs **all** strategies' `evaluate_exit()` and `evaluate_entry()` concurrently.
4. Collects the returned `StrategySignal` (which now contains full `OrderParams`).
5. Dispatches signals directly to the execution layer.

Your strategy is a Rust struct that implements the `Strategy` trait. It is responsible for its own venue selection, pricing, sizing, and risk management.

---

## The `Strategy` Trait

```rust
// src/orchestrator/strategy.rs

#[async_trait::async_trait]
pub trait Strategy: Send + Sync {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal>;
    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal>;
    fn status(&self) -> StrategyStatus;
    fn name(&self) -> String;
}
```

---

## What's in `StrategyContext`

```rust
pub struct StrategyContext {
    pub market: MarketConfig,               // Hourly token IDs, name, expiry, fees
    pub snapshot: MarketSnapshot,           // Hourly prices, oracle, velocity
    pub positions: Arc<Mutex<PositionMap>>, // shared position state
    pub session_pnl: Decimal,               // Total profit/loss this session
    pub starting_collateral: Decimal,       // Initial wallet balance
    pub crypto_filter: String,              // "btc" | "eth" | "sol"
    pub market_started_at: DateTime<Utc>,
    pub maker_market: Option<MarketConfig>, // Optional Window/Daily venue
    pub maker_snapshot: Option<MarketSnapshot>, // Prices for the Window venue
}
```

---

## The Signal Types

Strategies return full `OrderParams`, so `main.rs` doesn't need to perform any calculations.

```rust
pub enum StrategySignal {
    Entry {
        params: OrderParams,
        pair_params: Option<OrderParams>, // For hedged strategies (Arbitrage)
    },
    MakerQuote {
        yes: Option<OrderParams>,
        no: Option<OrderParams>,
    },
    Exit {
        params: OrderParams,
        reason: String,
        exit_pair: bool, // If true, also exits the matching pair token
    },
    NoSignal,
}
```

### `OrderParams` Structure
```rust
pub struct OrderParams {
    pub token_id: U256,
    pub price: Decimal,
    pub shares: Decimal,
    pub fee_bps: u16,
    pub is_neg_risk: bool,
    pub market_name: String,
    pub condition_id: String,
}
```

---

## Step-by-Step: Building a New Strategy

### Step 1 — Create the implementation file

Create `src/strategies/my_strategy_impl.rs`. Your strategy should perform its own risk checks using `is_drawdown_limit_hit`.

```rust
use async_trait::async_trait;
use anyhow::Result;
use rust_decimal_macros::dec;
use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams};
use crate::strategies::is_drawdown_limit_hit;
use crate::config;

pub struct MyStrategyImpl;

#[async_trait]
impl Strategy for MyStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        // 1. Global Drawdown Check
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            return Ok(StrategySignal::NoSignal);
        }

        // 2. Logic (Example: Buy YES when BTC jumps)
        if ctx.snapshot.velocity > dec!(100) {
            let price = ctx.snapshot.yes_ask;
            let trade_size = dec!(10.0);
            
            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id: ctx.market.yes_token,
                    price,
                    shares: trade_size / price,
                    fee_bps: ctx.market.yes_fee_bps as u16,
                    is_neg_risk: ctx.market.is_neg_risk,
                    market_name: ctx.market.market_name.clone(),
                    condition_id: ctx.market.condition_id.clone(),
                },
                pair_params: None,
            });
        }
        Ok(StrategySignal::NoSignal)
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        // Implementation for TP/SL...
        Ok(StrategySignal::NoSignal)
    }
    
    fn name(&self) -> String { "MyStrategy".to_string() }
    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
}
```

### Step 2 — Expose the module
Add `pub mod my_strategy_impl;` to `src/strategies/mod.rs`.

### Step 3 — Add to Registry
Add your strategy to `src/orchestrator/registry.rs`:
```rust
strategies.push(Box::new(MyStrategyImpl));
```

---

## Important Constraints

### Venue Selection
Your strategy should choose the best venue. Use `ctx.maker_market` if you want low-fee Window markets, or `ctx.market` for high-volatility Hourly markets.

### Pricing & Rounding
Always apply `floor_to_tick_size` or `round_to_tick_size` from `helpers::price` to your order prices before returning them in `OrderParams`.

### Never hold a MutexGuard across .await
Always drop the lock on `ctx.positions` before performing any async work or returning a signal.

---

## Testing Your Strategy

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use crate::state::{MarketConfig, MarketSnapshot, PositionMap};
    use alloy::primitives::U256;
    use chrono::Utc;

    fn test_ctx() -> StrategyContext {
        StrategyContext {
            market: MarketConfig {
                yes_token: U256::from(1), no_token: U256::from(2),
                market_name: "Test".to_string(), market_close_time: None,
                strike_price: None, is_neg_risk: false,
                condition_id: "".to_string(), yes_fee_bps: 100, no_fee_bps: 100,
            },
            snapshot: MarketSnapshot {
                yes_bid: dec!(0.5), yes_ask: dec!(0.51), yes_bid_depth: dec!(100), yes_ask_depth: dec!(100),
                no_bid: dec!(0.49), no_ask: dec!(0.5), no_bid_depth: dec!(100), no_ask_depth: dec!(100),
                oracle_price: dec!(70000), velocity: dec!(0), velocity_1s: dec!(0),
                acceleration: dec!(0), funding_rate: dec!(0), oracle_drift_60m: dec!(0),
                timestamp: Utc::now(),
            },
            positions: Arc::new(Mutex::new(PositionMap::new())),
            session_pnl: dec!(0),
            starting_collateral: dec!(100),
            crypto_filter: "btc".to_string(),
            market_started_at: Utc::now(),
            maker_market: None,
            maker_snapshot: None,
        }
    }
}
```
