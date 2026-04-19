# RustPolyBot

An automated trading bot for Polymarket crypto prediction markets, written in Rust. Runs four strategies concurrently — momentum, maker, arbitrage, and time decay — each with its own independent capital budget and position book. An orchestrator manages market selection, WS subscriptions, and order execution.

---

## ⚠️ Read This First

**This is experimental software. You will probably lose money.**

- **Risk**: Momentum trades are directional and can get whiplashed. Arbitrage spreads are thin. Time decay positions can widen against you. None of this is guaranteed profit.
- **US Citizens**: Polymarket is rolling out US access under CFTC regulation via a waitlist. Check [polymarket.com](https://polymarket.com) for your current eligibility — crypto markets may not be available in the initial rollout.
- **Competition**: Polymarket is full of well-funded, low-latency bots. This project is a learning exercise, not an edge.

Use at your own risk.

---

## How It Works

The bot connects to Polymarket's CLOB via WebSocket for real-time orderbook data and to Binance for oracle pricing. Every 50ms, the orchestrator evaluates all strategies concurrently, then dispatches the resulting signals to the execution layer. Each strategy operates from its own independent position namespace and capital budget, so they never block each other.

### Strategies

**Momentum** — Detects when Binance price moves sharply before Polymarket reprices. Buys the side that's about to become in-the-money. One-sided (not hedged), so this is the risky one. Requires 2 consecutive signal ticks to filter fakeouts. Exits on take profit (5%), stop loss (10%), or velocity reversal.

**Maker** — Posts passive resting bids on **both YES and NO simultaneously** (two-sided market making). Per Polymarket's fee structure, makers pay 0 fees on entry; only taker exits incur the market fee rate. Filled maker orders also earn daily USDC rebates from Polymarket's Maker Rebates program (paid to your wallet each day, minimum $1 accrued).

Two income streams per filled order: (1) spread profit when the position reaches take-profit, (2) daily rebate on the fill.

Key behaviours:
- **Inventory skew**: if YES inventory outweighs NO, the YES bid is lowered (less aggressive to avoid deepening the skew) and the NO bid is raised (more aggressive to rebalance faster). Skew scales linearly with imbalance up to `MAKER_INVENTORY_SKEW_MAX`.
- **Combined price guard**: `YES_bid + NO_bid` must be below `MAKER_MAX_COMBINED_BID` (0.90). If both sides would sum above the threshold, the side with the tighter spread (less edge) is suppressed. This prevents offering a riskless arb to takers who could sell both legs to us and pocket the $1.00 settlement.
- **Net exposure risk**: risk is measured as `|YES_value − NO_value|` (directional imbalance), not gross `YES + NO`. A balanced two-sided book has near-zero directional risk, so the strategy can quote larger notional without increasing drawdown risk.
- **Market maturation gate**: waits 10 minutes after a new market opens before posting (filters chaotic initial pricing).
- **Expiry gate**: no new quotes within 30 minutes of market close.

Uses GTD post-only orders. Exits on take profit (8%) or stop loss (5%).

**Arbitrage** — Buys both YES and NO when the combined ask is cheap enough that the spread covers fees. Hedged position, lower risk. Exits when combined bid converges toward $1.00.

**Time Decay** — Near expiry, YES + NO prices converge toward $1.00. This strategy buys both sides when the combined ask is attractive and rides the convergence. Only active within a configurable time window before market close (default: 4–30 minutes).

**Custom Strategy** — Develop and link your own strategies within the same codebase.


### Strategy Segregation

Each strategy has its own **independent position book** keyed by `(strategy_name, token_id)`. This means:

- **MomentumStrategy** and **MakerStrategy** can both hold YES simultaneously without collision.
- Each strategy has its own **capital budget** — Maker can't consume USDC that Momentum needs for a taker fill, and vice versa.
- Each strategy has its own **cooldown timer** — a Maker fill doesn't prevent Momentum from firing 1 second later.
- There are no cross-strategy signal conflicts. The orchestrator passes all signals through; exits are always prioritised before entries within each strategy's own book.

| Strategy | Capital Budget | Risk Model | Order Type |
|---|---|---|---|
| MomentumStrategy | `MOMENTUM_MAX_EXPOSURE_USDC` ($25) | Gross one-sided | FAK taker |
| MakerStrategy | `MAKER_MAX_EXPOSURE_USDC` ($15) | Net \|YES−NO\| | GTD post-only (two-sided) |
| ArbitrageStrategy | `ARBITRAGE_MAX_EXPOSURE_USDC` ($50 per leg) | Gross hedged | FAK taker |
| TimeDecayStrategy | `TIME_DECAY_MAX_EXPOSURE_USDC` ($50 per leg) | Gross hedged | FAK taker |

---

## Setup

### Requirements
- Rust 1.91+ (or Docker)
- A Polygon wallet with USDC and MATIC for gas
- Telegram bot token (optional, for trade alerts)

### Environment Variables (`.env`)

```
POLYMARKET_PRIVATE_KEY=<your-polygon-eoa-private-key>
TRADE_SIZE_USDC=10
CRYPTO_FILTER=btc          # btc, eth, or sol
RUST_LOG=info
TELEGRAM_BOT_TOKEN=         # optional
TELEGRAM_CHAT_ID=           # optional
```

### Key Config (`src/config.rs`)

**Global**

| Parameter | What it does | Default |
|-----------|-------------|---------|
| `GHOST_MODE` | Log trades without executing | `false` |
| `TRADE_COOLDOWN_SECS` | Per-strategy seconds between trades | `8` |
| `MAX_CONSECUTIVE_FAILURES` | Circuit breaker trip count | `3` |
| `MIN_LIQUIDITY_FILL_RATIO` | Required book depth ratio | `0.80` |

**Per-strategy capital budgets**

| Parameter | Strategy | Default |
|-----------|----------|---------|
| `MOMENTUM_MAX_EXPOSURE_USDC` | MomentumStrategy | `$25` |
| `MAKER_MAX_EXPOSURE_USDC` | MakerStrategy | `$15` |
| `ARBITRAGE_MAX_EXPOSURE_USDC` | ArbitrageStrategy | `$50` per leg |
| `TIME_DECAY_MAX_EXPOSURE_USDC` | TimeDecayStrategy | `$50` per leg |

**Momentum**

| Parameter | What it does | Default |
|-----------|-------------|---------|
| `MOMENTUM_CONFIRMATION_TICKS` | Consecutive ticks before firing | `2` |
| `MOMENTUM_TARGET_PROFIT_PERCENT` | Take profit | `5%` |
| `MOMENTUM_STOP_LOSS_PERCENT` | Stop loss | `10%` |
| `BTC_MOMENTUM_THRESHOLD` | BTC velocity trigger (USD/5s) | `$75` |
| `MAX_MOMENTUM_ENTRY_PRICE` | Max token ask for entry | `$0.88` |

**Maker**

| Parameter | What it does | Default |
|-----------|-------------|---------|
| `MAKER_MIN_SPREAD` | Min bid-ask spread on a side to post that side | `$0.05` |
| `MAKER_BID_IMPROVEMENT` | Queue priority over best bid (fallback, zero-spread markets) | `$0.01` |
| `MAKER_BID_IMPROVEMENT_RATIO` | Spread fraction used as bid improvement | `0.30` |
| `MAKER_MAX_COMBINED_BID` | Max YES_bid + NO_bid for simultaneous two-sided quote | `$0.90` |
| `MAKER_INVENTORY_SKEW_MAX` | Max per-side price adjustment for inventory rebalancing | `$0.03` |
| `MAKER_MIN_SECS_TO_EXPIRY` | Don't post within this window of expiry | `1800s (30 min)` |
| `MAKER_MIN_MARKET_AGE_SECS` | Wait this long after market opens before posting | `600s (10 min)` |
| `MAKER_MAX_ENTRY_PRICE` | Max bid price for entry on either side | `$0.55` |
| `MAKER_MIN_ENTRY_PRICE` | Min bid price for entry (avoids near-zero resolved tokens) | `$0.10` |
| `MAKER_TARGET_PROFIT_PERCENT` | Take profit | `8%` |
| `MAKER_STOP_LOSS_PERCENT` | Stop loss | `5%` |
| `MIN_HOLD_SECS_BEFORE_STOP_LOSS` | Minimum hold time before stop loss activates | `300s` |
| `MAKER_STOP_LOSS_COOLDOWN_SECS` | Cooldown after a stop loss before re-entry | `600s` |
| `CROSSES_BOOK_COOLDOWN_SECS` | Cooldown after a post-only "crosses book" rejection | `30s` |

**Arbitrage**

| Parameter | What it does | Default |
|-----------|-------------|---------|
| `ARBITRAGE_PROFIT_THRESHOLD` | Min margin after fees | `$0.05` |
| `EARLY_EXIT_COMBINED_BID_THRESHOLD` | Combined bid exit trigger | `$0.995` |

**Time Decay**

| Parameter | What it does | Default |
|-----------|-------------|---------|
| `TIME_DECAY_MIN_SECS_TO_EXPIRY` | Earliest entry window | `240s` |
| `TIME_DECAY_MAX_SECS_TO_EXPIRY` | Latest entry window | `1800s` |
| `TIME_DECAY_TARGET_PROFIT_PERCENT` | Take profit | `1.5%` |

### Running

**Test first** — set `GHOST_MODE = true` in `config.rs`, then:

```bash
cargo build --release
./target/release/rustpolybot
```

Watch the logs. You'll see `📥 ENTRY [MomentumStrategy]` and `📤 EXIT [MakerStrategy]` log lines with the strategy name for every trade it *would* have placed. Once you're comfortable with the signals, flip `GHOST_MODE` to `false` and rebuild.

**Docker** (deploys BTC/ETH/SOL containers):

```bash
./deploy-multi.sh
```

---

## Project Layout

```
src/
├── main.rs                        # Trading loop, WS connections, signal dispatch
├── config.rs                      # All tunable parameters + per-strategy budgets
├── state.rs                       # Shared types: Position, PositionMap (keyed by (strategy, token))
├── lib.rs                         # Module exports
├── risk.rs                        # Per-strategy exposure limits, drawdown checks
├── notifications.rs               # Telegram alerts
├── market_validator.rs            # Market filtering (crypto, expiry, strike extraction)
├── toctou_tests.rs                # Race condition unit tests
├── orchestrator/
│   ├── mod.rs
│   ├── strategy.rs                # Strategy trait definition + StrategyContext
│   ├── registry.rs                # Creates all strategy instances
│   ├── executor.rs                # Concurrent evaluation, signal passthrough
│   └── market_data.rs             # Market data broadcasting
├── strategies/
│   ├── mod.rs
│   ├── momentum_impl.rs           # Momentum: Binance oracle, velocity, strike buffer
│   ├── maker_impl.rs              # Maker: passive GTD bids, spread filter
│   ├── arbitrage_impl.rs          # Arbitrage: combined ask profitability, convergence exit
│   └── time_decay_impl.rs         # Time decay: theta window, settlement vs convergence mode
└── helpers/
    ├── mod.rs
    ├── orders.rs                  # EIP-712 order signing + CLOB placement
    ├── market.rs                  # Gamma API market discovery
    ├── price.rs                   # Price conversions, tick size rounding
    ├── balance.rs                 # On-chain balance sync (per-strategy key aware)
    ├── nonce.rs                   # Nonce management with retry
    ├── time.rs                    # Time/expiry utilities
    └── json.rs                    # JSON parsing helpers
```

---

## Safety Features

- **Circuit breaker**: Pauses all trading after 3 consecutive order failures; clears local positions to resync with exchange state
- **Per-strategy risk engine**: Each strategy's exposure is checked against its own budget — one strategy can't consume another's capital
- **TOCTOU-safe entry gate**: Position check and reservation happen in a single atomic lock scope, preventing duplicate orders from concurrent 50ms ticks
- **Momentum confirmation**: 2 consecutive ticks required, prevents single-tick fakeouts; automatically resets if the risk engine blocks the trade to prevent log flooding
- **Per-strategy cooldowns**: Each strategy has its own 8-second cooldown after a trade — Maker fills don't delay Momentum entries
- **Liquidity check**: Won't fire into thin books — requires 80%+ of order size available at top of book
- **Market filtering**: Skips politics, long-term events, range markets, 5-minute markets
- **Maker post-only guard**: GTD maker orders are flagged `post_only`; if they would cross the spread, the exchange rejects them rather than creating a taker fill
- **LCM-aligned order amounts**: BUY and SELL amounts are computed using `lcm(price_cents, 10000)` alignment to guarantee Polymarket's precision rules (makerAmount max 2dp, takerAmount max 4dp) at any price without rounding errors
- **Lock-free nonce**: nonce manager uses `Arc<AtomicU64>` so simultaneous YES and NO maker orders never contend on the same lock
- **Nonce recovery**: Auto-resyncs on "invalid nonce" errors
- **Phantom position cleanup**: `sync_position_balance` removes positions that remain unfilled on-chain after 60 seconds
- **Telegram alerts**: Notifications on every entry, exit, circuit breaker event, and partial paired fill

---

## FAQ

**Why Rust instead of Python or JavaScript?**

Short answer: the concurrency model and compile-time safety guarantees are a better fit for a multi-strategy bot with shared state.

- **No GIL.** Python's Global Interpreter Lock means threads don't actually run in parallel for CPU-bound work. This bot evaluates four strategies concurrently every 50ms — in Python that's cooperative multitasking at best, not true parallelism. Rust's `tokio` async runtime and `Arc<Mutex<>>` primitives give real concurrent execution across OS threads.
- **No GC pauses.** Python and Node.js both have garbage collectors that can pause execution at unpredictable times. In a 50ms evaluation loop, even a 5–20ms GC pause is a meaningful fraction of your cycle. Rust's ownership model frees memory deterministically, with no stop-the-world pauses.
- **Fearless concurrency.** The borrow checker enforces at compile time that shared state (like the position map) can't be accessed unsafely from multiple threads. The TOCTOU-safe entry gate in this bot is only reliable because Rust makes data races a compile error, not a runtime surprise.
- **Zero-cost abstractions.** Iterators, closures, and async compile down to the same machine code you'd write by hand. Python's abstractions carry runtime overhead; Node.js is better but still JIT-dependent.

Honest caveats: Python would have been faster to build — the trading bot ecosystem (CCXT, pandas, asyncio) is mature and the borrow checker has a real learning curve. And if you're running on a VPS far from Polymarket's infrastructure, network RTT will dominate any language-level latency advantage. The real payoff here is **correctness**: a bot that doesn't corrupt its position state at 3am is worth more than one that's 2ms faster.

**Why isn't the bot trading?**

Check in order: Is `GHOST_MODE` still true? Is the spread wide enough to beat `ARBITRAGE_PROFIT_THRESHOLD` + fees? Is the orderbook thick enough (`MIN_LIQUIDITY_FILL_RATIO`)? For momentum — is the oracle velocity actually hitting the threshold (`BTC_MOMENTUM_THRESHOLD` = $75/5s)? For maker — is the market less than 10 minutes old (`MAKER_MIN_MARKET_AGE_SECS`)? Is the market closing in less than 30 minutes (`MAKER_MIN_SECS_TO_EXPIRY`)? Bump `RUST_LOG=debug` to see what's being filtered.

**Orders keep getting rejected**

Usually latency. The bot uses Fill-or-Kill orders for taker strategies, so if the price moves between signal and execution, the order dies. Deploy closer to Polymarket's infrastructure.

**I see both Momentum and Maker trading the same token — is that a bug?**

No, this is by design. Each strategy has its own independent position slot keyed by `(strategy_name, token_id)`. `MomentumStrategy` and `MakerStrategy` can both hold YES simultaneously, each from their own separate capital budget. Their exits are also independent — they each only close their own position.

**What's the Gnosis Safe thing?**

Polymarket's API trading uses Gnosis Safe proxy wallets. The bot automatically derives your Safe address from your EOA private key. This is standard — the Polymarket web UI does the same thing under the hood.

**How do I run only one strategy?**

Set `ENABLE_MOMENTUM_TRADING = false`, `ENABLE_MAKER_TRADING = false`, and/or `ENABLE_TIME_DECAY_TRADING = false` in config.rs. For arbitrage, set `ARBITRAGE_PROFIT_THRESHOLD` to something unreachable like `dec!(1.0)`.

**How do I adjust each strategy's risk budget?**

Edit the per-strategy constants in `src/config.rs`: `MOMENTUM_MAX_EXPOSURE_USDC`, `MAKER_MAX_EXPOSURE_USDC`, `ARBITRAGE_MAX_EXPOSURE_USDC`, `TIME_DECAY_MAX_EXPOSURE_USDC`.

---

## License

See [LICENSE](LICENSE).
