# RustPolyBot

An automated trading bot for Polymarket crypto prediction markets, written in Rust. Runs five strategies concurrently — momentum, maker, arbitrage, time decay, and basis/funding — each with its own independent capital budget and position book. An orchestrator manages market selection, WS subscriptions, and order execution. It includes a comprehensive interface and guide to craft and link-in your own customer strategy, like, for example, a machine-learning-based sentiment analyzer.

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
- **Velocity trigger**: `BTC_MOMENTUM_THRESHOLD` = $85/5s (example).
- **Strike buffer**: `BTC_STRIKE_BUFFER` = $50.0 (example).
- **Gates**: Requires building acceleration and a strong 1s short-window confirmation.
- **Sizing**: Fractional Kelly scaling based on signal strength.

**Maker** — Posts passive resting bids on **both YES and NO simultaneously** on the **Window/Maker venue**.
- **Imbalance gate**: Skips bids if the book is heavily skewed against us.
- **Net exposure**: Risk is measured as the directional imbalance (`|YES − NO|`).

**Arbitrage** — Buys both YES and NO on the **Window/Daily venue** when combined asks are < $1.00 (net of fees).
- **Profit Threshold**: Exploits small inefficiencies in the low-fee venue.

**Time Decay** — Exploits price convergence toward $1.00 as markets approach expiry. Operates on the **Window venue** to avoid 10% hourly fees.

**Basis / Funding-Rate** — Mean-reversion strategy that fades retail skew on the **Window venue** using Binance funding rates as confirmation.
- **Thesis**: Fades amateur over-betting when smart money (funding) disagrees.

**Custom Strategy** — Develop and link your own strategies. See [CUSTOM_STRATEGY.md](docs/CUSTOM_STRATEGY.md).

---

### Strategy Segregation (Example Profile)

| Strategy | Capital Budget | Risk Model | Primary Venue |
|---|---|---|---|
| MomentumStrategy | `$15` | Gross one-sided | **Hourly** |
| MakerStrategy | `$12` | Net \|YES−NO\| | **Window** |
| ArbitrageStrategy | `$35` per leg | Gross hedged | **Window** |
| TimeDecayStrategy | `$36` per leg | Gross hedged | **Window** |
| BasisStrategy | `$15` | Gross one-sided | **Window** |

---

## Setup

### Requirements
- Rust 1.91+ (or Docker)
- A Polygon wallet with USDC and MATIC
- Telegram bot token (optional)

### Configuration Profiles

**`src/config.rs` is NOT included in this repository.** It is your personal trading configuration and is intentionally gitignored so your own tuning stays private.

Three ready-to-use starting profiles are provided. **You must copy one to `src/config.rs` before you can build.**

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
Rust provides **fearless concurrency**. Evaluating five strategies concurrently every 50ms requires a multi-threaded runtime without a Global Interpreter Lock (GIL) or unpredictable Garbage Collection (GC) pauses.

**Why isn't the bot trading?**
Check in order:
1. Is `GHOST_MODE` true?
2. **Fees**: Taker strategies skip high-fee (1000 bps) markets.
3. **Thresholds**: Check your thresholds in `config.rs`. Momentum and Basis require specific volatility/skew to fire.
4. **Venue**: Maker/Arb/Basis require a **Window or Daily** market to be active.

**I see Momentum and Maker trading the same token — is that a bug?**
No. Each strategy has its own independent position book. They can "co-habitate" on the same token without collision.

**How do I adjust risk?**
Edit the per-strategy constants in `src/config.rs`, specifically the `_MAX_EXPOSURE_USDC` values.

---

## License
See [LICENSE](LICENSE).
