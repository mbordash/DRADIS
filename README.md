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

Multi-timeframe confirmation gates (added to filter fakeouts and stale signals):
- **5s velocity** (primary): must exceed the per-crypto threshold (`BTC_MOMENTUM_THRESHOLD` = $50/5s)
- **Strike buffer**: requires price to clear the strike by a small margin (`BTC_STRIKE_BUFFER` = $10.0) to catch moves as they happen
- **1s velocity** (short-window): must still be ≥ 40% of threshold, proving the move is happening *right now* and not just a residual from an impulse that already exhausted itself
- **Acceleration**: velocity must be building (positive delta), or the 5s move must be so large (≥ 2.5× threshold) that it's worth entering even if slightly decelerating
- Requires 2 consecutive signal ticks to filter single-tick fakeouts
- **Velocity-decay exit**: if the 1s velocity collapses below 20% of threshold *while in profit*, exit early rather than waiting for a full reversal to eat the gains
- **Fractional Kelly sizing**: trade size scales with signal strength from `MOMENTUM_MIN_TRADE_SIZE_USDC` ($5) at 1× threshold to `MOMENTUM_MAX_TRADE_SIZE_USDC` ($25) at 3× (`MOMENTUM_KELLY_MAX_MULTIPLIER`)
- **Late-market size tapering**: when a market is within `LATE_MARKET_SIZE_THRESHOLD_SECS` (30 min) of expiry, entry size scales linearly from 100% down to 50% at `MIN_SECONDS_TO_EXPIRY_FOR_ENTRY` (5 min)

Exits on take profit (20%), stop loss (8%), velocity reversal (75% of threshold in the opposite direction), or velocity decay.

**Maker** — Posts passive resting bids on **both YES and NO simultaneously** (two-sided market making) on the **best available slow-moving venue**: a multi-hour window market if one is active, a daily market if not, or the hourly market as a last resort.

Key behaviors:
- **Orderbook imbalance gate**: skips bids on any side where ask-side depth is more than 3× the bid-side depth (`MAKER_MAX_BOOK_IMBALANCE_RATIO`).
- **Inventory skew**: if YES inventory outweighs NO, the YES bid is lowered (less aggressive) and the NO bid is raised (more aggressive to rebalance faster).
- **Combined price guard**: `YES_bid + NO_bid` must be below `MAKER_MAX_COMBINED_BID` (0.90).
- **Net exposure risk**: risk is measured as `|YES_value − NO_value|` (directional imbalance), not gross `YES + NO`.
- **GTD TTL**: resting bids stay live for 90 seconds.

Exits on take profit (20%) or stop loss (15%).

**Arbitrage** — Buys both YES and NO when the combined ask is cheap enough that the spread covers fees. Hedged position, lower risk. Exits when combined bid converges toward $1.00.

Operates on the **same window/daily venue as MakerStrategy** when one is available, falling back to the hourly market otherwise. This avoids the 1000 bps fees on BTC/ETH hourly markets which make arbitrage mathematically impossible.

**High-fee market filter**: The strategy skips any market where either leg's taker fee exceeds `ARBITRAGE_MAX_TAKER_FEE_BPS` (200 bps). Profit threshold is tuned to `0.015` (1.5% net) for frequent hits on low-fee Window markets.

**Time Decay** — Near expiry, YES + NO prices converge toward $1.00. Operates on the **Window/Maker venue** to exploit theta convergence without being eaten by 10% hourly fees.

- **Settlement mode**: buys both sides when combined ask < $1.00 after fees.
- **Convergence mode**: activates within the final 20 minutes. Allows combined asks up to `MAX_TIME_DECAY_COMBINED_ASK` ($1.008) — profit comes from bid convergence as the book collapses toward $1.00.

**Basis / Funding-Rate** — Mean-reversion strategy that fades retail skew using Binance perpetual futures data as a confirming signal. Now routed to the **Window/Maker venue** for low-fee mean reversion.

The core thesis: Polymarket Window markets often exhibit **retail skew** — amateur bettors push the price above what Binance justifies.
- Negative funding = shorts paying longs = smart money net-bearish → fade by buying NO
- Positive funding = longs paying shorts = smart money net-bullish → fade by buying YES

Entry gates:
1. YES mid-price > 0.50 + `BASIS_ENTRY_SKEW_THRESHOLD` (3¢) — retail over-bet YES
2. Binance velocity is flat (< `BASIS_BTC_MAX_VELOCITY` = $30/5s)
3. Funding rate confirms bias OR extreme skew (2× threshold) bypasses the gate
4. Taker fees ≤ `BASIS_MAX_TAKER_FEE_BPS` (200 bps)

Exits: take profit (+8%), stop loss (−10%), **skew-collapse exit**, or expiry guard.


### Market Selection

Every scan cycle the bot calls `get_market_pair()`, which classifies markets into two categories:

1. **Hourly (Primary Venue)**: Used by **MomentumStrategy**. High volatility and fast price action.
2. **Window/Daily (Maker Venue)**: Used by **Maker, Arbitrage, Basis, and Time Decay**. Slower discovery and significantly lower fees (0-200 bps vs 1000 bps).

Both markets subscribe separate WS orderbook feeds to ensure all strategies have the most accurate data for their specific venue.

---

### Strategy Segregation

Each strategy has its own **independent position book** keyed by `(strategy_name, token_id)`.

| Strategy | Capital Budget | Risk Model | Primary Venue |
|---|---|---|---|
| MomentumStrategy | `MOMENTUM_MAX_EXPOSURE_USDC` ($50) | Gross one-sided | **Hourly** |
| MakerStrategy | `MAKER_MAX_EXPOSURE_USDC` ($20) | Net \|YES−NO\| | **Window** |
| ArbitrageStrategy | `ARBITRAGE_MAX_EXPOSURE_USDC` ($50 per leg) | Gross hedged | **Window** |
| TimeDecayStrategy | `TIME_DECAY_MAX_EXPOSURE_USDC` ($50 per leg) | Gross hedged | **Window** |
| BasisStrategy | `BASIS_MAX_EXPOSURE_USDC` ($20) | Gross one-sided | **Window** |

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
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

---

## Safety Features

- **Circuit breaker**: Pauses all trading after 3 consecutive order failures.
- **Per-strategy risk engine**: Each strategy has its own independent exposure ceiling.
- **TOCTOU-safe entry gate**: Atomic lock scope prevents duplicate orders from concurrent ticks.
- **Orphaned pair detection**: Detects one-sided hedged positions (Arb/TimeDecay) and exits after 60s.
- **LCM-aligned order amounts**: Guarantees Polymarket's precision rules at any price.

---

## License

See [LICENSE](LICENSE).
