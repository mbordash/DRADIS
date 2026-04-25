# RustPolyBot

An automated trading bot for Polymarket crypto prediction markets, written in Rust. Runs five strategies concurrently — momentum, maker, arbitrage, time decay, and basis/funding — each with its own independent capital budget and position book. An orchestrator manages market selection, WS subscriptions, and order execution.

---

## ⚠️ Read This First

**This is experimental software. You will probably lose money.**

- **Risk**: Momentum trades are directional and can get whiplashed. Arbitrage spreads are thin. Time decay positions can widen against you. None of this is guaranteed profit.
- **US Citizens**: Polymarket is rolling out US access under CFTC regulation via a waitlist. Check [polymarket.com](https://polymarket.com) for your current eligibility — crypto markets may not be available in the initial rollout.
- **Competition**: Polymarket is full of well-funded, low-latency bots. This project is a learning exercise, not an edge.

Use at your own risk.

---

## How It Works

The bot connects to Polymarket's CLOB via WebSocket for real-time orderbook data and to Binance for oracle pricing and perpetual futures funding rates. Every 50ms, the orchestrator evaluates all strategies concurrently, then dispatches the resulting signals to the execution layer. Each strategy operates from its own independent position namespace and capital budget, so they never block each other.

### Strategies

**Momentum** — Detects when Binance price moves sharply before Polymarket reprices. Buys the side that's about to become in-the-money. One-sided (not hedged), so this is the risky one.

Multi-timeframe confirmation gates:
- **5s velocity** (primary): must exceed the per-crypto threshold (`BTC_MOMENTUM_THRESHOLD` = $50/5s)
- **Strike buffer**: requires price to clear the strike by a small margin (`BTC_STRIKE_BUFFER` = $10.0) to catch moves as they happen
- **1s velocity** (short-window): must still be ≥ 40% of threshold, proving the move is happening *right now*
- **Acceleration**: velocity must be building (positive delta), or the 5s move must be so large (≥ 2.5× threshold)
- **Fractional Kelly sizing**: trade size scales with signal strength from `MOMENTUM_MIN_TRADE_SIZE_USDC` ($5) to `MOMENTUM_MAX_TRADE_SIZE_USDC` ($25)
- **Near-expiry profit guard**: momentum positions entering the expiry window (15 min) must show at least 2% profit to be held

Exits on take profit (20%), stop loss (8%), velocity reversal (75% of threshold in the opposite direction), or velocity decay.

**Maker** — Posts passive resting bids on **both YES and NO simultaneously** on the **Window/Maker venue**.

Key behaviors:
- **Orderbook imbalance gate**: skips bids on any side where ask-side depth is more than 3× the bid-side depth.
- **Inventory skew**: if YES inventory outweighs NO, the YES bid is lowered and the NO bid is raised.
- **Combined price guard**: `YES_bid + NO_bid` must be below `MAKER_MAX_COMBINED_BID` (0.90).
- **Net exposure risk**: risk is measured as `|YES_value − NO_value|` (directional imbalance).

Exits on take profit (20%) or stop loss (15%).

**Arbitrage** — Buys both YES and NO when the combined ask is cheap enough that the spread covers fees. Routed to the **Window/Daily venue** to take advantage of lower fees (0-200 bps).

**High-fee market filter**: The strategy skips any market where either leg's taker fee exceeds `ARBITRAGE_MAX_TAKER_FEE_BPS` (200 bps). Profit threshold is tuned to `0.015` (1.5% net) for frequent hits on low-fee Window markets.

**Time Decay** — Near expiry, YES + NO prices converge toward $1.00. Operates on the **Window/Maker venue** to exploit theta convergence without being eaten by 10% hourly fees.

- **Settlement mode**: buys both sides when combined ask < $1.00 after fees.
- **Convergence mode**: activates within the final 20 minutes.

**Basis / Funding-Rate** — Mean-reversion strategy that fades retail skew using Binance perpetual futures data as a confirming signal. Now routed to the **Window/Maker venue** for low-fee mean reversion.

The core thesis: Polymarket Window markets often exhibit **retail skew** — amateur bettors push the price above what Binance justifies.
- Negative funding = shorts paying longs = smart money net-bearish → fade by buying NO
- Positive funding = longs paying shorts = smart money net-bullish → fade by buying YES

Entry gates:
1. YES mid-price > 0.50 + `BASIS_ENTRY_SKEW_THRESHOLD` (3¢)
2. Binance velocity is flat (< `BASIS_BTC_MAX_VELOCITY` = $30/5s)
3. Taker fees ≤ `BASIS_MAX_TAKER_FEE_BPS` (200 bps)

Exits: take profit (+8%), stop loss (−10%), **skew-collapse exit**, or expiry guard.


### Market Selection

Every scan cycle the bot calls `get_market_pair()`, which classifies markets:

1. **Hourly (Primary Venue)**: Used by **MomentumStrategy**. High volatility and fast price action.
2. **Window/Daily (Maker Venue)**: Used by **Maker, Arbitrage, Basis, and Time Decay**. Slower discovery and significantly lower fees (0-200 bps vs 1000 bps).

---

### Strategy Segregation

Each strategy has its own **independent position book** keyed by `(strategy_name, token_id)`.

| Strategy | Capital Budget | Risk Model | Primary Venue |
|---|---|---|---|
| MomentumStrategy | `$50` | Gross one-sided | **Hourly** |
| MakerStrategy | `$20` | Net \|YES−NO\| | **Window** |
| ArbitrageStrategy | `$50` per leg | Gross hedged | **Window** |
| TimeDecayStrategy | `$50` per leg | Gross hedged | **Window** |
| BasisStrategy | `$20` | Gross one-sided | **Window** |

---

## Setup

### Requirements
- Rust 1.91+ (or Docker)
- A Polygon wallet with USDC and MATIC for gas
- Telegram bot token (optional)

### Environment Variables (`.env`)

```
POLYMARKET_PRIVATE_KEY=<your-polygon-eoa-private-key>
TRADE_SIZE_USDC=10
CRYPTO_FILTER=btc          # btc, eth, or sol
RUST_LOG=info
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

### Key Config (`src/config.rs`)

> `src/config.rs` is intentionally gitignored so your personal tuning stays private. Copy one of the three profiles below to create yours.

**Global & Sizing**

| Parameter | What it does | Balanced profile |
|-----------|-------------|---------|
| `GHOST_MODE` | Log trades without executing | `false` |
| `TRADE_COOLDOWN_SECS` | Per-strategy seconds between trades | `8` |
| `MAX_CONSECUTIVE_FAILURES` | Circuit breaker trip count | `3` |
| `MOMENTUM_MIN_TRADE_SIZE_USDC` | Min trade size for Momentum | `$5` |
| `MOMENTUM_MAX_TRADE_SIZE_USDC` | Max trade size for Momentum | `$25` |

### Configuration Profiles

Three ready-to-use starting profiles are provided in the repo. **You must copy one to `src/config.rs` before you can build.**

| Profile | File | Wallet Size | Risk | Strategies Active |
|---------|------|-------------|------|-------------------|
| 🟢 Conservative | `src/config.conservative.rs.example` | < $100 | Low | Maker, Time Decay only |
| 🟡 Balanced | `src/config.balanced.rs.example` | $100–$300 | Medium | All five, moderate sizing |
| 🔴 Aggressive | `src/config.aggressive.rs.example` | $200+ | High | All five, maximum sizing |

```bash
# Pick a starting profile and copy it into place
cp src/config.balanced.rs.example src/config.rs
cargo build --release
```

---

## Running

**Test first** — set `GHOST_MODE = true` in `config.rs`, then:

```bash
cargo build --release
./target/release/rustpolybot
```

Watch the logs. You'll see `📥 ENTRY [MomentumStrategy]`, `📥 ENTRY [BasisStrategy]`, `📤 EXIT [MakerStrategy]` log lines with the strategy name for every trade it *would* have placed.

**Docker (deploys BTC/ETH/SOL containers):**

```bash
./deploy-multi.sh
```

---

## Safety Features

- **Circuit breaker**: Pauses all trading after 3 consecutive order failures; clears local positions to resync with exchange state.
- **Per-strategy risk engine**: Each strategy's exposure is checked against its own budget.
- **TOCTOU-safe entry gate**: Atomic lock scope prevents duplicate orders from concurrent 50ms ticks.
- **Momentum confirmation**: 2 consecutive ticks required to filter single-tick fakeouts.
- **Orphaned pair detection**: Detects one-sided hedged positions (Arb/TimeDecay) and exits after 60s.
- **LCM-aligned order amounts**: Guarantees Polymarket's precision rules at any price.

---

## License

See [LICENSE](LICENSE).
