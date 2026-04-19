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
- **5s velocity** (primary): must exceed the per-crypto threshold (`BTC_MOMENTUM_THRESHOLD` = $75/5s)
- **1s velocity** (short-window): must still be ≥ 40% of threshold, proving the move is happening *right now* and not just a residual from an impulse that already exhausted itself
- **Acceleration**: velocity must be building (positive delta), or the 5s move must be so large (≥ 2.5× threshold) that it's worth entering even if slightly decelerating
- Requires 2 consecutive signal ticks to filter single-tick fakeouts
- **Velocity-decay exit**: if the 1s velocity collapses below 20% of threshold *while in profit*, exit early rather than waiting for a full reversal to eat the gains
- **Fractional Kelly sizing**: trade size scales with signal strength from `MOMENTUM_MIN_TRADE_SIZE_USDC` ($5) at 1× threshold to `MOMENTUM_MAX_TRADE_SIZE_USDC` ($25) at 4×

Exits on take profit (5%), stop loss (10%), velocity reversal (75% of threshold in the opposite direction), or velocity decay.

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

**Basis / Funding-Rate** — Mean-reversion strategy that fades retail skew using Binance perpetual futures data as a confirming signal.

The core thesis: Polymarket hourly "Up or Down" markets frequently exhibit **retail skew** — amateur bettors push the YES price above what the actual Binance move justifies. Binance's perpetual **funding rate** captures the institutional view:
- Negative funding = shorts paying longs = smart money is net-bearish, even while retail is bullish on Polymarket → fade by buying NO
- Positive funding = longs paying shorts = smart money is net-bullish, even while retail is bearish → fade by buying YES

The funding rate is polled from `fapi.binance.com/fapi/v1/premiumIndex` every 60 seconds and injected into every MarketSnapshot as `funding_rate`.

Entry gates (example: YES overpriced):
1. YES mid-price > 0.50 + `BASIS_ENTRY_SKEW_THRESHOLD` (8¢) — retail significantly over-bet YES
2. Binance velocity is flat (< `BASIS_BTC_MAX_VELOCITY` = $30/5s) — real moves are not fades
3. Spot within `BASIS_BTC_ORACLE_STRIKE_BUFFER` ($200) of strike — market not already decided
4. Funding rate < `BASIS_NEGATIVE_FUNDING_THRESHOLD` (−0.01%/8h), confirming bearish institutional bias — **or** skew ≥ 2× threshold (extreme skew bypasses the funding gate)
5. NO ask ≤ `BASIS_MAX_ENTRY_PRICE` ($0.60)

Exits: take profit (+6%), stop loss (−8%), **skew-collapse exit** (YES mid returns within 3¢ of 0.50 while in profit — thesis played out early), or expiry guard (<10 min to close).

Uses fractional Kelly sizing: $5 at 1× threshold → $15 at 3× skew.

**Custom Strategy** — Develop and link your own strategies within the same codebase. See [CUSTOM_STRATEGY.md](docs/CUSTOM_STRATEGY.mdGY.md) for a full developer guide: the `Strategy` trait API, all `StrategyContext` fields, the five integration steps, common gotchas, and a ready-to-run test harness.


### Strategy Segregation

Each strategy has its own **independent position book** keyed by `(strategy_name, token_id)`. This means:

- All five strategies can hold the same token simultaneously without collision.
- Each strategy has its own **capital budget** — Maker can't consume USDC that Momentum needs for a taker fill, and vice versa.
- Each strategy has its own **cooldown timer** — a Maker fill doesn't prevent Momentum from firing 1 second later.
- There are no cross-strategy signal conflicts. The orchestrator passes all signals through; exits are always prioritised before entries within each strategy's own book.

| Strategy | Capital Budget | Risk Model | Order Type |
|---|---|---|---|
| MomentumStrategy | `MOMENTUM_MAX_EXPOSURE_USDC` ($25) | Gross one-sided | FAK taker |
| MakerStrategy | `MAKER_MAX_EXPOSURE_USDC` ($15) | Net \|YES−NO\| | GTD post-only (two-sided) |
| ArbitrageStrategy | `ARBITRAGE_MAX_EXPOSURE_USDC` ($50 per leg) | Gross hedged | FAK taker |
| TimeDecayStrategy | `TIME_DECAY_MAX_EXPOSURE_USDC` ($50 per leg) | Gross hedged | FAK taker |
| BasisStrategy | `BASIS_MAX_EXPOSURE_USDC` ($20) | Gross one-sided | FAK taker |

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
| `BASIS_MAX_EXPOSURE_USDC` | BasisStrategy | `$20` |

**Momentum**

| Parameter | What it does | Default |
|-----------|-------------|---------|
| `MOMENTUM_CONFIRMATION_TICKS` | Consecutive ticks before firing | `2` |
| `MOMENTUM_TARGET_PROFIT_PERCENT` | Take profit | `5%` |
| `MOMENTUM_STOP_LOSS_PERCENT` | Stop loss | `10%` |
| `BTC_MOMENTUM_THRESHOLD` | BTC velocity trigger (USD/5s) | `$75` |
| `MAX_MOMENTUM_ENTRY_PRICE` | Max token ask for entry | `$0.88` |
| `MOMENTUM_SHORT_WINDOW_SECS` | Short-window confirmation window | `1s` |
| `MOMENTUM_SHORT_WINDOW_FRACTION` | Min 1s velocity as fraction of threshold | `0.40` (40%) |
| `MOMENTUM_ACCELERATION_BYPASS_MULTIPLIER` | Bypass accel gate at this signal strength | `2.5×` |
| `MOMENTUM_DECAY_EXIT_FRACTION` | 1s velocity collapse threshold for early exit | `0.20` (20%) |

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

**Basis / Funding-Rate**

| Parameter | What it does | Default |
|-----------|-------------|---------|
| `ENABLE_BASIS_TRADING` | Enable/disable the strategy | `true` |
| `BASIS_ENTRY_SKEW_THRESHOLD` | Min YES mid deviation from 0.50 to consider a trade | `0.08` (8¢) |
| `BASIS_NEGATIVE_FUNDING_THRESHOLD` | Funding rate below which smart money is bearish | `−0.0001` |
| `BASIS_POSITIVE_FUNDING_THRESHOLD` | Funding rate above which smart money is bullish | `+0.0001` |
| `BASIS_BTC_MAX_VELOCITY` | Max Binance velocity still considered "flat" | `$30/5s` |
| `BASIS_BTC_ORACLE_STRIKE_BUFFER` | Max spot distance from strike for entry | `$200` |
| `BASIS_MAX_ENTRY_PRICE` | Max ask for the token being bought | `$0.60` |
| `BASIS_TARGET_PROFIT_PERCENT` | Take profit | `6%` |
| `BASIS_STOP_LOSS_PERCENT` | Stop loss | `8%` |
| `BASIS_SKEW_COLLAPSE_THRESHOLD` | Exit if YES mid returns within this of 0.50 while in profit | `3¢` |
| `BASIS_MIN_SECS_TO_EXPIRY` | No entry within this window of close | `1200s (20 min)` |
| `BASIS_MIN_TRADE_SIZE_USDC` | Trade size at minimum skew signal | `$5` |
| `BASIS_MAX_TRADE_SIZE_USDC` | Trade size at maximum skew signal | `$15` |
| `BASIS_KELLY_MAX_MULTIPLIER` | Skew multiple at which size saturates | `3×` |
| `BASIS_FUNDING_POLL_SECS` | Binance futures polling interval | `60s` |

### Running

**Test first** — set `GHOST_MODE = true` in `config.rs`, then:

```bash
cargo build --release
./target/release/rustpolybot
```

Watch the logs. You'll see `📥 ENTRY [MomentumStrategy]`, `📥 ENTRY [BasisStrategy]`, `📤 EXIT [MakerStrategy]` log lines with the strategy name for every trade it *would* have placed. Once you're comfortable with the signals, flip `GHOST_MODE` to `false` and rebuild.

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
│   ├── momentum_impl.rs           # Momentum: multi-timeframe velocity + acceleration gates
│   ├── maker_impl.rs              # Maker: two-sided GTD bids, inventory skew, combined price guard
│   ├── arbitrage_impl.rs          # Arbitrage: combined ask profitability, convergence exit
│   ├── time_decay_impl.rs         # Time decay: theta window, settlement vs convergence mode
│   └── basis_impl.rs              # Basis/funding: retail skew fade + Binance funding confirmation
└── helpers/
    ├── mod.rs
    ├── orders.rs                  # EIP-712 order signing + CLOB placement (LCM-aligned amounts)
    ├── market.rs                  # Gamma API market discovery
    ├── price.rs                   # Price conversions, tick size rounding
    ├── balance.rs                 # On-chain balance sync (per-strategy key aware)
    ├── nonce.rs                   # Lock-free AtomicU64 nonce management
    ├── time.rs                    # Time/expiry utilities
    └── json.rs                    # JSON parsing helpers
```

---

## Safety Features

- **Circuit breaker**: Pauses all trading after 3 consecutive order failures; clears local positions to resync with exchange state
- **Per-strategy risk engine**: Each strategy's exposure is checked against its own budget — one strategy can't consume another's capital
- **TOCTOU-safe entry gate**: Position check and reservation happen in a single atomic lock scope, preventing duplicate orders from concurrent 50ms ticks
- **Momentum confirmation**: 2 consecutive ticks required, prevents single-tick fakeouts; automatically resets if the risk engine blocks the trade to prevent log flooding
- **Multi-timeframe momentum gates**: 1s short-window gate (move still happening now) + acceleration gate (momentum building, not fading) prevent entries on stale 5s signals
- **Velocity-decay exit**: exits a profitable momentum position early when the 1s velocity collapses, locking in gains before a full reversal
- **Per-strategy cooldowns**: Each strategy has its own 8-second cooldown after a trade — Maker fills don't delay Momentum entries
- **Liquidity check**: Won't fire into thin books — requires 80%+ of order size available at top of book
- **Market filtering**: Skips politics, long-term events, range markets, 5-minute markets
- **Maker post-only guard**: GTD maker orders are flagged `post_only`; if they would cross the spread, the exchange rejects them rather than creating a taker fill
- **LCM-aligned order amounts**: BUY and SELL amounts are computed using `lcm(price_cents, 10000)` alignment to guarantee Polymarket's precision rules (makerAmount max 2dp, takerAmount max 4dp) at any price without rounding errors
- **Lock-free nonce**: nonce manager uses `Arc<AtomicU64>` so simultaneous YES and NO maker orders never contend on the same lock
- **Nonce recovery**: Auto-resyncs on "invalid nonce" errors
- **Phantom position cleanup**: `sync_position_balance` removes positions that remain unfilled on-chain after 60 seconds
- **Basis funding gate**: Basis strategy only fires when Binance funding rate confirms the retail-vs-smart-money divergence, or when retail skew is extreme enough (2×) to bypass the gate
- **Telegram alerts**: Notifications on every entry, exit, circuit breaker event, and partial paired fill

---

## FAQ

**Why Rust instead of Python or JavaScript?**

Short answer: the concurrency model and compile-time safety guarantees are a better fit for a multi-strategy bot with shared state.

- **No GIL.** Python's Global Interpreter Lock means threads don't actually run in parallel for CPU-bound work. This bot evaluates five strategies concurrently every 50ms — in Python that's cooperative multitasking at best, not true parallelism. Rust's `tokio` async runtime and `Arc<Mutex<>>` primitives give real concurrent execution across OS threads.
- **No GC pauses.** Python and Node.js both have garbage collectors that can pause execution at unpredictable times. In a 50ms evaluation loop, even a 5–20ms GC pause is a meaningful fraction of your cycle. Rust's ownership model frees memory deterministically, with no stop-the-world pauses.
- **Fearless concurrency.** The borrow checker enforces at compile time that shared state (like the position map) can't be accessed unsafely from multiple threads. The TOCTOU-safe entry gate in this bot is only reliable because Rust makes data races a compile error, not a runtime surprise.
- **Zero-cost abstractions.** Iterators, closures, and async compile down to the same machine code you'd write by hand. Python's abstractions carry runtime overhead; Node.js is better but still JIT-dependent.

Honest caveats: Python would have been faster to build — the trading bot ecosystem (CCXT, pandas, asyncio) is mature and the borrow checker has a real learning curve. And if you're running on a VPS far from Polymarket's infrastructure, network RTT will dominate any language-level latency advantage. The real payoff here is **correctness**: a bot that doesn't corrupt its position state at 3am is worth more than one that's 2ms faster.

**Why isn't the bot trading?**

Check in order: Is `GHOST_MODE` still true? Is the spread wide enough to beat `ARBITRAGE_PROFIT_THRESHOLD` + fees? Is the orderbook thick enough (`MIN_LIQUIDITY_FILL_RATIO`)? For momentum — is the oracle velocity actually hitting the threshold (`BTC_MOMENTUM_THRESHOLD` = $75/5s) AND the 1s velocity also hitting 40% of that AND acceleration is positive? For maker — is the market less than 10 minutes old (`MAKER_MIN_MARKET_AGE_SECS`)? Is the market closing in less than 30 minutes (`MAKER_MIN_SECS_TO_EXPIRY`)? For basis — is the YES/NO mid-price more than 8¢ from 0.50, and is Binance velocity below $30/5s? Bump `RUST_LOG=debug` to see what's being filtered.

**Orders keep getting rejected**

Usually latency. The bot uses Fill-or-Kill orders for taker strategies, so if the price moves between signal and execution, the order dies. Deploy closer to Polymarket's infrastructure.

**I see both Momentum and Maker trading the same token — is that a bug?**

No, this is by design. Each strategy has its own independent position slot keyed by `(strategy_name, token_id)`. All five strategies can hold the same token simultaneously, each from their own separate capital budget. Their exits are also independent — they each only close their own position.

**What's the Gnosis Safe thing?**

Polymarket's API trading uses Gnosis Safe proxy wallets. The bot automatically derives your Safe address from your EOA private key. This is standard — the Polymarket web UI does the same thing under the hood.

**How do I run only one strategy?**

Set `ENABLE_MOMENTUM_TRADING = false`, `ENABLE_MAKER_TRADING = false`, `ENABLE_BASIS_TRADING = false`, and/or `ENABLE_TIME_DECAY_TRADING = false` in config.rs. For arbitrage, set `ARBITRAGE_PROFIT_THRESHOLD` to something unreachable like `dec!(1.0)`.

**How do I adjust each strategy's risk budget?**

Edit the per-strategy constants in `src/config.rs`: `MOMENTUM_MAX_EXPOSURE_USDC`, `MAKER_MAX_EXPOSURE_USDC`, `ARBITRAGE_MAX_EXPOSURE_USDC`, `TIME_DECAY_MAX_EXPOSURE_USDC`, `BASIS_MAX_EXPOSURE_USDC`.

**What happens if the Binance futures API is down?**

The `funding_rate` field in `MarketSnapshot` defaults to `0.0`. At zero funding, the Basis strategy requires the **extreme skew bypass** (YES/NO mid-price ≥ 2× the 8¢ threshold, i.e., >16¢ from 0.50) to fire an entry. This means the strategy becomes more conservative but doesn't fully disable — extreme retail imbalances are still traded even without funding confirmation.

---

## License

See [LICENSE](LICENSE).
