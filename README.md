# RustPolyBot

An automated trading bot for Polymarket crypto prediction markets, written in Rust. Runs five strategies concurrently — momentum, maker, arbitrage, time decay, and basis/funding — each with its own independent capital budget and position book. An orchestrator manages market selection, WS subscriptions, and order execution.

---

## ⚠️ Read This First

**This is experimental software. You will probably lose money.**

- **Risk**: Momentum trades are directional and can get whiplashed. Arbitrage spreads are thin. Time decay positions can widen against you. None of this is guaranteed profit.
- **US Citizens**: Polymarket is rolling out US access under CFTC regulation via a waitlist. Check [polymarket.com](https://polymarket.com) for your current eligibility.
- **Competition**: Polymarket is full of well-funded, low-latency bots. This project is a learning exercise, not an edge.

---

## How It Works

The bot connects to Polymarket's CLOB via WebSocket for real-time orderbook data and to Binance for oracle pricing and perpetual futures funding rates. Every 50ms, the orchestrator evaluates all strategies concurrently, then dispatches the resulting signals to the execution layer.

### Strategies

**Momentum** — Detects when Binance price moves sharply before Polymarket reprices.
- **Velocity trigger**: `BTC_MOMENTUM_THRESHOLD` = $50/5s.
- **Strike buffer**: `BTC_STRIKE_BUFFER` = $10.0.
- **Gates**: Requires building acceleration and a strong 1s short-window confirmation.
- **Sizing**: Fractional Kelly scaling from $5 to $25.

**Maker** — Posts passive resting bids on **both YES and NO simultaneously** on the **Window/Maker venue**.
- **Imbalance gate**: Skips bids if the book is heavily skewed against us.
- **Net exposure**: Risk is measured as the directional imbalance (`|YES − NO|`).

**Arbitrage** — Buys both YES and NO on the **Window/Daily venue** when combined asks are < $1.00 (net of fees).
- **Profit Threshold**: Tuned to 1.5% net profit for high-frequency hits on low-fee venues.

**Time Decay** — Exploits price convergence toward $1.00 as markets approach expiry. Operates on the **Window venue** to avoid 10% hourly fees.

**Basis / Funding-Rate** — Fades retail skew on the **Window venue** using Binance funding rates as confirmation.
- **Sensitivity**: 3¢ skew threshold.
- **Thesis**: Fades amateur over-betting when smart money (funding) disagrees.

**Custom Strategy** — Develop and link your own strategies. See [CUSTOM_STRATEGY.md](docs/CUSTOM_STRATEGY.md).

---

### Strategy Segregation

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
- A Polygon wallet with USDC and MATIC
- Telegram bot token (optional)

### Configuration Profiles

| Profile | File | Wallet Size | Risk | Strategies Active |
|---------|------|-------------|------|-------------------|
| 🟢 Conservative | `src/config.conservative.rs.example` | < $100 | Low | Maker, Time Decay only |
| 🟡 Balanced | `src/config.balanced.rs.example` | $100–$300 | Medium | All five, moderate sizing |
| 🔴 Aggressive | `src/config.aggressive.rs.example` | $200+ | High | All five, maximum sizing |

---

## Running

**Test first** — set `GHOST_MODE = true` in `config.rs`, then:

```bash
cargo build --release
./target/release/rustpolybot
```

**Docker Deployment:** `./deploy-multi.sh`

---

## Safety Features

- **Circuit breaker**: Pauses all trading after 3 consecutive failures.
- **TOCTOU-safe entry**: Atomic lock scope prevents duplicate orders.
- **Orphaned pair detection**: Automatically exits one-sided hedged positions after 60s.
- **Fee Gates**: Blocks taker strategies from entering high-fee (10%+) markets.

---

## FAQ

**Why Rust instead of Python?**
Rust provides **fearless concurrency**. Evaluating five strategies concurrently every 50ms requires a multi-threaded runtime without a Global Interpreter Lock (GIL) or unpredictable Garbage Collection (GC) pauses. Rust ensures that our position state remains consistent even under high-frequency trading.

**Why isn't the bot trading?**
Check in order:
1. Is `GHOST_MODE` true?
2. **Fees**: Is the market charging 1000 bps? Taker strategies (Arb, Basis, Time Decay) will skip these.
3. **Thresholds**: For Momentum, is BTC moving >$50/5s? For Basis, is the skew >3¢?
4. **Venue**: Maker/Arb/Basis require a **Window or Daily** market to be active. Check logs for `🏦 Maker Window market selected`.

**I see Momentum and Maker trading the same token — is that a bug?**
No. Each strategy has its own independent position book. They operate from separate budgets and have independent exit logic. They can "co-habitate" on the same token without collision.

**How do I adjust risk?**
Edit the per-strategy constants in `src/config.rs`, specifically the `_MAX_EXPOSURE_USDC` values.

---

## License
See [LICENSE](LICENSE).
