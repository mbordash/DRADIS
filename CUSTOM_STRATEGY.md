# Writing a Custom Strategy

This guide walks through implementing and integrating a new trading strategy into RustPolyBot from scratch. The entire process is five steps and touches four files.

---

## How the Orchestrator Works

Every 50ms the main loop:

1. Reads the latest `MarketSnapshot` (Polymarket orderbook prices + Binance oracle + velocity + funding rate)
2. Calls `execute_strategies_concurrent()` — runs **all** registered strategies' `evaluate_exit()` and `evaluate_entry()` concurrently in separate tasks
3. Collects the returned `StrategySignal` from each
4. Dispatches signals to the execution layer (place order, update positions, send Telegram alert)

Your strategy is just a Rust struct that implements the `Strategy` trait. The orchestrator handles everything else — concurrency, order placement, risk checks, circuit breakers, position tracking.

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

**Rules:**
- Both evaluate methods receive a shared `&StrategyContext` — read-only except for `positions` which is `Arc<Mutex<PositionMap>>` (lock it briefly if you need to inspect open positions).
- Return `Ok(StrategySignal::NoSignal)` whenever you don't want to act. Never return `Err` for business logic — only for unrecoverable I/O failures.
- `name()` must return a **unique string** — it is the namespace key for your positions in the shared `PositionMap`. Two strategies with the same name will share and corrupt each other's position book.
- Both methods must be `Send + Sync` safe. Don't hold `MutexGuard` across `.await` points.

---

## What's in `StrategyContext`

```rust
pub struct StrategyContext {
    pub market: MarketConfig,       // token IDs, name, expiry, strike, fee rates
    pub snapshot: MarketSnapshot,   // live prices, oracle, velocity, funding_rate
    pub positions: Arc<Mutex<PositionMap>>, // shared position state (all strategies)
    pub crypto_filter: String,      // "btc" | "eth" | "sol"
    pub market_started_at: DateTime<Utc>,
}
```

### `MarketSnapshot` fields

| Field | Type | Description |
|-------|------|-------------|
| `yes_bid` / `yes_ask` | `Decimal` | Polymarket YES token best bid/ask |
| `no_bid` / `no_ask` | `Decimal` | Polymarket NO token best bid/ask |
| `yes_ask_depth` / `no_ask_depth` | `Decimal` | Shares available at the best ask |
| `oracle_price` | `Decimal` | Binance spot price (BTC/ETH/SOL in USD) |
| `velocity` | `Decimal` | Binance price change over last 5 seconds |
| `velocity_1s` | `Decimal` | Binance price change over last 1 second |
| `acceleration` | `Decimal` | `velocity` delta since last tick (positive = building) |
| `funding_rate` | `Decimal` | Binance perp funding rate (negative = bearish smart money) |
| `timestamp` | `DateTime<Utc>` | Snapshot creation time |

### `MarketConfig` fields

| Field | Type | Description |
|-------|------|-------------|
| `yes_token` / `no_token` | `U256` | On-chain token IDs |
| `market_name` | `String` | Human-readable market name |
| `market_close_time` | `Option<DateTime<Utc>>` | Expiry time |
| `strike_price` | `Option<Decimal>` | Extracted strike (e.g. $84,000 for BTC) |
| `is_neg_risk` | `bool` | Whether market uses NegRisk exchange contract |
| `yes_fee_bps` / `no_fee_bps` | `u32` | Taker fee rates in basis points |

---

## The Signal Types

```rust
pub enum StrategySignal {
    // Standard taker entry: buy `token_id` at market ask (FAK)
    Entry { token_id: U256 },

    // Two-sided passive maker quote: post GTD post-only bids (use None to skip a side)
    MakerQuote {
        yes_bid_price: Option<Decimal>,
        no_bid_price: Option<Decimal>,
    },

    // Exit: sell `token_id` at market bid (FAK)
    Exit { token_id: U256, reason: String },

    // Do nothing this tick
    NoSignal,
}
```

For most custom strategies, you'll use `Entry` and `Exit`. Use `MakerQuote` only if you want passive GTD post-only orders (see `MakerStrategyImpl` for a full example).

---

## Step-by-Step: Building a New Strategy

### Step 1 — Create the implementation file

Create `src/strategies/my_strategy_impl.rs`:

```rust
use async_trait::async_trait;
use anyhow::Result;
use rust_decimal_macros::dec;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus};
use crate::config;

pub struct MyStrategyImpl;

#[async_trait]
impl Strategy for MyStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        // Example: buy YES when ask is below 0.30 and there's upward velocity
        let yes_ask = ctx.snapshot.yes_ask;
        let velocity = ctx.snapshot.velocity;

        if yes_ask < dec!(0.30) && velocity > dec!(50) {
            return Ok(StrategySignal::Entry { token_id: ctx.market.yes_token });
        }

        Ok(StrategySignal::NoSignal)
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let positions = ctx.positions.lock().await;

        for ((strategy_name, token_id), position) in positions.iter() {
            if strategy_name != "MyStrategy" { continue; }

            let bid = if token_id == &ctx.market.yes_token {
                ctx.snapshot.yes_bid
            } else {
                ctx.snapshot.no_bid
            };

            let profit_margin = (bid - position.avg_entry) / position.avg_entry;

            // Take profit at 10%
            if profit_margin >= dec!(0.10) {
                return Ok(StrategySignal::Exit {
                    token_id: *token_id,
                    reason: format!("TP: {:.2}%", profit_margin * dec!(100)),
                });
            }

            // Stop loss at 5%
            if profit_margin <= dec!(-0.05) {
                return Ok(StrategySignal::Exit {
                    token_id: *token_id,
                    reason: format!("SL: {:.2}%", profit_margin * dec!(100)),
                });
            }
        }

        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus {
        StrategyStatus::Active
    }

    fn name(&self) -> String {
        "MyStrategy".to_string()  // Must be unique across all strategies
    }
}
```

> **Tip:** Inspect open positions scoped to your strategy using `strategy_name != "MyStrategy"`. Never read or modify another strategy's positions — they share the same map but each strategy's capital is independent.

---

### Step 2 — Expose the module

Add your module to `src/strategies/mod.rs`:

```rust
pub mod my_strategy_impl;
```

---

### Step 3 — Add a capital budget and enable flag

Add constants to `src/config.rs`:

```rust
pub const ENABLE_MY_TRADING: bool = true;

/// Maximum USDC exposure for MyStrategy
pub const MY_STRATEGY_MAX_EXPOSURE_USDC: Decimal = dec!(20.0);
```

---

### Step 4 — Register with the risk engine

Add a budget mapping in `src/risk.rs` inside `strategy_max_exposure()`:

```rust
"MyStrategy" => config::MY_STRATEGY_MAX_EXPOSURE_USDC,
```

---

### Step 5 — Register in the orchestrator

Add your strategy to `src/orchestrator/registry.rs`:

```rust
use crate::strategies::my_strategy_impl::MyStrategyImpl;

// Inside create_all_strategies():
if config::ENABLE_MY_TRADING {
    strategies.push(Box::new(MyStrategyImpl));
}
```

That's it. Rebuild and your strategy runs concurrently alongside all others.

---

## Important Constraints and Gotchas

### Never hold a `MutexGuard` across `.await`

This will deadlock. Lock the positions map, read what you need, drop the guard, then do any async work:

```rust
// ✅ Correct
let profit = {
    let positions = ctx.positions.lock().await;
    positions.get(&key).map(|p| p.avg_entry)
}; // guard dropped here
some_async_call().await;

// ❌ Deadlock
let positions = ctx.positions.lock().await;
some_async_call().await; // guard still held — other strategies can't lock
```

### `evaluate_exit` is called before `evaluate_entry` every tick

The orchestrator always evaluates exits first. You don't need to guard against entering while already holding a position — the main loop checks `pos_map.contains_key(&pos_key)` atomically before placing an order. But you should still return `NoSignal` from `evaluate_entry` if your logic doesn't make sense with an open position:

```rust
// Optional defensive guard inside evaluate_entry
let already_open = {
    let pos = ctx.positions.lock().await;
    pos.contains_key(&("MyStrategy".to_string(), ctx.market.yes_token))
};
if already_open { return Ok(StrategySignal::NoSignal); }
```

### Use `ctx.market.strike_price` defensively

Not every market has an extractable strike. Range-band markets, some older daily markets, and newly-launched markets may return `None`. Always handle the `Option`:

```rust
let Some(strike) = ctx.market.strike_price else {
    return Ok(StrategySignal::NoSignal);
};
```

### Respect expiry time

The main loop already blocks entries when the market is expiring within `MIN_SECONDS_TO_EXPIRY_FOR_ENTRY` (300s). But your strategy should also apply its own stricter gate for logic that doesn't make sense near expiry:

```rust
if let Some(close_time) = ctx.market.market_close_time {
    if (close_time - Utc::now()).num_seconds() < 600 {
        return Ok(StrategySignal::NoSignal);
    }
}
```

### Trade size is controlled by the orchestrator

The main loop uses `trade_size_usdc` (from the `TRADE_SIZE_USDC` env var) for standard `Entry` signals. If your strategy needs custom sizing (like Kelly scaling), you need to signal it from outside the `Strategy` trait — the easiest approach is to match on `strategy_name` in the main loop's Kelly block (see how `BasisStrategy` and `MomentumStrategy` do it) and add a `my_trade_size()` helper function exported from your impl module.

---

## Testing Your Strategy

Add a `#[cfg(test)]` block at the bottom of your impl file. Use this helper to build a realistic `StrategyContext` without touching the network:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use crate::state::{MarketConfig, MarketSnapshot, PositionMap};
    use crate::orchestrator::StrategyContext;
    use alloy::primitives::U256;
    use chrono::Utc;

    fn test_ctx(yes_ask: rust_decimal::Decimal, velocity: rust_decimal::Decimal) -> StrategyContext {
        StrategyContext {
            market: MarketConfig {
                yes_token: U256::from(1u64),
                no_token: U256::from(2u64),
                market_name: "BTC Up or Down".to_string(),
                market_close_time: None,
                strike_price: Some(dec!(84000)),
                is_neg_risk: false,
                yes_fee_bps: 1000,
                no_fee_bps: 1000,
            },
            snapshot: MarketSnapshot {
                yes_bid: yes_ask - dec!(0.05),
                yes_ask,
                yes_ask_depth: dec!(100),
                no_bid: dec!(0.70),
                no_ask: dec!(0.75),
                no_ask_depth: dec!(100),
                oracle_price: dec!(84100),
                velocity,
                velocity_1s: velocity,
                acceleration: dec!(0),
                funding_rate: dec!(0),
                timestamp: Utc::now(),
            },
            positions: Arc::new(Mutex::new(PositionMap::new())),
            crypto_filter: "btc".to_string(),
            market_started_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_entry_fires_when_conditions_met() {
        let signal = MyStrategyImpl.evaluate_entry(&test_ctx(dec!(0.25), dec!(75))).await.unwrap();
        assert!(matches!(signal, StrategySignal::Entry { .. }));
    }

    #[tokio::test]
    async fn test_no_signal_below_velocity() {
        let signal = MyStrategyImpl.evaluate_entry(&test_ctx(dec!(0.25), dec!(10))).await.unwrap();
        assert!(matches!(signal, StrategySignal::NoSignal));
    }
}
```

Run with:
```bash
cargo test my_strategy
```

---

## Reference Implementations

| File | What to learn from it |
|------|-----------------------|
| `src/strategies/momentum_impl.rs` | Multi-gate entry logic, Kelly sizing, velocity-decay exit |
| `src/strategies/basis_impl.rs` | Oracle + external signal (funding rate) cross-check, skew-collapse exit |
| `src/strategies/maker_impl.rs` | `MakerQuote` signal, inventory skew, two-sided quoting, combined price guard |
| `src/strategies/arbitrage_impl.rs` | Hedged paired entry (buy both sides), convergence exit |
| `src/strategies/time_decay_impl.rs` | Time-window gating, settlement vs. convergence mode |

