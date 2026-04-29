# DRADIS

Direct Reaction And Dynamic Intelligence System A Low-Latency Multi-Strategy Trading Execution Platform for Prediction Markets, like PolyMarket.

---

🛰️ Tactical Overview

DRADIS is not just a bot; it is a Combat Information Center (CIC) for Polymarket. Built in Rust for maximum concurrency and memory safety, it evaluates the entire "battlespace" every 50ms, coordinating multiple autonomous strategies to defend capital and strike inefficiencies.

Unlike standard linear scripts, DRADIS uses a Tokio-powered orchestrator to manage telemetry (WebSockets), signal processing (Binance Oracles), and tactical execution across five distinct "Viper" strategy classes.

---

🛠️ The Architecture (The CIC)

The core of DRADIS is the Orchestrator. It acts as the ship’s brain, maintaining the primary data link to the Polymarket CLOB and Binance Oracles.

Parallel Dispatch: Every heartbeat (50ms), the CIC polls all registered strategies in parallel.

Isolated Pits: Each strategy operates with its own Independent Capital Budget and Position Book. A "whiplash" in one sector won't compromise the fuel (USDC) of another.

Signal Filtering: Includes a built-in OBI (Order Book Imbalance) Veto at -0.65 to prevent launching into "toxic flow" or distribution walls.

---

🚀 The Viper Squadrons (Strategies)

DRADIS currently deploys five specialized strategy classes:

Momentum (The Interceptor): Scans for high-velocity Binance moves. If a "target" moves $85 in 5 seconds, the Interceptor strikes the Polymarket book before it can reprice.

Maker (The Sentry): Maintains a dual-sided presence on the Window venue, capturing the spread while managing net exposure.

Arbitrage (The Surveyor): Constantly monitors the price sum of YES/NO pairs, looking for sub-$1.00 opportunities in low-fee venues.

Time Decay (The Ghost): Exploits the natural convergence of prediction markets toward expiry, fading retail volatility.

Basis/Funding (The Analyst): Fades retail skew by comparing Polymarket sentiment against Binance perpetual funding rates.

---

🛡️ Safety Systems

Orphaned Position Detection: Automatically "scuttles" one-sided hedged positions after 60s to prevent directional bleeding.

Fee Gates: Hard-coded protection to ensure Taker strategies don't enter high-fee (1000 bps) environments.

Circuit Breaker: Total system lockdown after 3 consecutive execution failures.

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

**Basis / Funding-Rate** — Fades retail skew on the **Window venue** using Binance funding rates as confirmation.
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

## Performance Tracking

The bot automatically records every completed trade into a daily CSV file for easy analysis.

- **Location**: `logs/trades_YYYY-MM-DD.csv`
- **Columns**: Timestamp, Strategy, Market, Side (YES/NO), Entry Price, Exit Price, Shares, Profit (USDC), and Exit Reason.
- **Asynchronous**: Logging is non-blocking and happens in a background thread to maintain high-frequency trading performance.

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
./target/release/dradis
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
