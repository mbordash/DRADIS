# RustPolyBot

An automated trading bot for Polymarket crypto prediction markets, written in Rust. Runs three strategies concurrently — momentum, arbitrage, and time decay — with an orchestrator that resolves conflicts and manages execution.

---

## ⚠️ Read This First

**This is experimental software. You will probably lose money.**

- **US Citizens**: Polymarket is not available to US persons inside the United States. Check your local laws.
- **Risk**: Momentum trades are directional and can get whiplashed. Arbitrage spreads are thin. Time decay positions can widen against you. None of this is guaranteed profit.
- **Competition**: Polymarket is full of well-funded, low-latency bots. This project is a learning exercise, not an edge.

Use at your own risk.

---

## How It Works

The bot connects to Polymarket's CLOB via WebSocket for real-time orderbook data and to Binance for oracle pricing. Every 50ms, the orchestrator evaluates all three strategies, resolves any conflicting signals (exits always beat entries), and places orders through the CLOB API.

### Strategies

**Momentum** — Detects when Binance price moves sharply before Polymarket reprices. Buys the side that's about to become in-the-money. One-sided (not hedged), so this is the risky one. Requires 2 consecutive signal ticks to filter fakeouts. Exits on take profit (5%), stop loss (10%), or velocity reversal.

**Arbitrage** — Buys both YES and NO when the combined ask is cheap enough that the spread covers fees. Hedged position, lower risk. Exits when combined bid converges toward $1.00.

**Time Decay** — Near expiry, YES + NO prices converge toward $1.00. This strategy buys both sides when the combined ask is attractive and rides the convergence. Only active within a configurable time window before market close (default: 4–30 minutes).

All three run simultaneously. The orchestrator handles the case where, say, momentum wants to enter YES while arbitrage wants to exit it — exits win.

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

| Parameter | What it does | Default |
|-----------|-------------|---------|
| `GHOST_MODE` | Log trades without executing | `false` |
| `ARBITRAGE_PROFIT_THRESHOLD` | Min margin for arb entry | `0.05` |
| `MOMENTUM_CONFIRMATION_TICKS` | Consecutive ticks before momentum fires | `2` |
| `MOMENTUM_TARGET_PROFIT_PERCENT` | Momentum take profit | `0.05` (5%) |
| `MOMENTUM_STOP_LOSS_PERCENT` | Momentum stop loss | `0.10` (10%) |
| `BTC_MOMENTUM_THRESHOLD` | BTC velocity trigger (USD/5s) | `$75` |
| `MIN_LIQUIDITY_FILL_RATIO` | Required book depth ratio | `0.80` |
| `ENABLE_MOMENTUM_TRADING` | Toggle momentum on/off | `true` |
| `ENABLE_TIME_DECAY_TRADING` | Toggle time decay on/off | `true` |
| `MAX_EXPOSURE_PER_TOKEN_USDC` | Per-token risk cap | `$25` |
| `TRADE_COOLDOWN_SECS` | Seconds between trades | `8` |

### Running

**Test first** — set `GHOST_MODE = true` in `config.rs`, then:

```bash
cargo build --release
./target/release/rustpolybot
```

Watch the logs. You'll see `📥 ENTRY` and `📤 EXIT` log lines for trades it *would* have placed. Once you're comfortable with the signals, flip `GHOST_MODE` to `false` and rebuild.

**Docker** (deploys BTC/ETH/SOL containers):

```bash
./deploy-multi.sh
```

---

## Project Layout

```
src/
├── main.rs                        # Trading loop, WS connections, signal dispatch
├── config.rs                      # All tunable parameters
├── state.rs                       # Shared types: Position, MarketSnapshot, StrategySignal
├── lib.rs                         # Module exports
├── risk.rs                        # Exposure limits, drawdown checks
├── notifications.rs               # Telegram alerts
├── market_validator.rs            # Market filtering (crypto, expiry, strike extraction)
├── orchestrator/
│   ├── mod.rs
│   ├── strategy.rs                # Strategy trait definition + StrategyContext
│   ├── registry.rs                # Creates all strategy instances
│   ├── executor.rs                # Concurrent evaluation, signal conflict resolution
│   └── market_data.rs             # Market data broadcasting
├── strategies/
│   ├── mod.rs
│   ├── momentum.rs                # Momentum helper logic
│   ├── momentum_impl.rs           # Strategy trait impl for momentum
│   ├── arbitrage.rs               # Arbitrage helper logic
│   ├── arbitrage_impl.rs          # Strategy trait impl for arbitrage
│   ├── time_decay.rs              # Time decay helper logic
│   └── time_decay_impl.rs         # Strategy trait impl for time decay
└── helpers/
    ├── mod.rs
    ├── orders.rs                  # EIP-712 order signing + CLOB placement
    ├── market.rs                  # Gamma API market discovery
    ├── price.rs                   # Price conversions
    ├── balance.rs                 # On-chain balance sync
    ├── nonce.rs                   # Nonce management with retry
    ├── time.rs                    # Time/expiry utilities
    └── json.rs                    # JSON parsing helpers
```

---

## Safety Features

- **Circuit breaker**: Pauses trading after 3 consecutive order failures
- **Risk engine**: Blocks entries that would exceed per-token exposure or session drawdown limits
- **Liquidity check**: Won't fire into thin books — requires 80%+ of order size available at top of book
- **Momentum confirmation**: 2 consecutive ticks required, prevents single-tick fakeouts
- **Cooldown**: 8-second minimum between trades
- **Market filtering**: Skips politics, long-term events, range markets, 5-minute markets
- **Nonce recovery**: Auto-resyncs on "invalid nonce" errors
- **Telegram alerts**: Notifications on every entry, exit, and circuit breaker event

---

## FAQ

**Why isn't the bot trading?**
Check in order: Is `GHOST_MODE` still true? Is the spread wide enough to beat `ARBITRAGE_PROFIT_THRESHOLD` + fees? Is the orderbook thick enough (`MIN_LIQUIDITY_FILL_RATIO`)? For momentum — is the oracle velocity actually hitting the threshold? Bump `RUST_LOG=debug` to see what's being filtered.

**Orders keep getting rejected**
Usually latency. The bot uses Fill-or-Kill orders, so if the price moves between signal and execution, the order dies. Deploy closer to Polymarket's infrastructure.

**What's the Gnosis Safe thing?**
Polymarket's API trading uses Gnosis Safe proxy wallets. The bot automatically derives your Safe address from your EOA private key. This is standard — the Polymarket web UI does the same thing under the hood.

**How do I run only one strategy?**
Set `ENABLE_MOMENTUM_TRADING = false` and/or `ENABLE_TIME_DECAY_TRADING = false` in config.rs. For arbitrage, set `ARBITRAGE_PROFIT_THRESHOLD` to something unreachable like `dec!(1.0)`.

---

## Ideas / Future Work

- Maker orders (earn rebates instead of paying fees)
- VWAP book walking for larger position sizes
- Multi-outcome market support
- Mean reversion / counter-trend strategy
- Grid-based accumulation

---

## License

See [LICENSE](LICENSE).
