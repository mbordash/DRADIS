# DRADIS

> **Direct Reaction And Dynamic Intelligence System** — A low-latency multi-strategy trading execution platform for prediction markets like Polymarket.

---

## 🛰️ Tactical Overview

DRADIS is not just a bot; it is a comprehensive trading automation platform for prediction markets like Polymarket. Built in Rust for maximum concurrency and memory safety, it evaluates the selected markets every 50ms, coordinating multiple autonomous strategies to preserve capital and place orders where it sees inefficiencies.

Unlike standard linear scripts, DRADIS uses a Tokio-powered orchestrator to manage telemetry (WebSockets), signal processing (Binance Oracles), and tactical execution across five distinct "Viper" strategy classes. You can also build your own Viper using our [implementation guide](docs/CUSTOM_STRATEGY.md).

---

## 🛠️ The Architecture (The CIC)

The core of DRADIS is the Orchestrator. It acts as the ship's brain, maintaining the primary data link to the Polymarket CLOB and Binance Oracles.

- **Parallel Dispatch**: Every heartbeat (50ms), the CIC polls all registered strategies in parallel.
- **Isolated Pits**: Each strategy operates with its own independent capital budget and position book. A "whiplash" in one sector won't compromise the fuel (USDC) of another.
- **Signal Filtering**: Includes a built-in OBI (Order Book Imbalance) Veto at -0.60 to prevent launching into "toxic flow" or distribution walls.

```
┌─────────────────────┐   ┌─────────────────────┐
│   Binance Oracle    │   │  Polymarket CLOB    │
│  (Price / Funding)  │   │  (WebSocket Feed)   │
└──────────┬──────────┘   └──────────┬──────────┘
           │                         │
           └────────────┬────────────┘
                        ▼
           ┌────────────────────────┐
           │   Orchestrator (CIC)   │
           │     50ms Heartbeat     │
           └────────────┬───────────┘
                        │  parallel dispatch
          ┌─────────────┼──────────────┬─────────────┐
          ▼             ▼              ▼             ▼
   ┌────────────┐ ┌──────────┐ ┌──────────────┐ ┌──────────┐
   │ Momentum   │ │  Maker   │ │  Arbitrage / │ │  GBoost  │
   │(Interceptor│ │ (Sentry) │ │  TimeDecay / │ │ (Cylon)  │
   │            │ │          │ │   Basis      │ │          │
   └─────┬──────┘ └────┬─────┘ └──────┬───────┘ └────┬─────┘
         └─────────────┼──────────────┴──────────────┘
                       ▼
           ┌───────────────────────┐
           │    Execution Layer    │
           │  OBI Gate · Fee Gate  │
           │  Circuit Breaker      │
           └───────────────────────┘
```

---

## 🚀 The Viper Squadrons (Strategies)

DRADIS currently deploys six specialized strategy classes:

- **Momentum (The Interceptor)**: Scans for high-velocity Binance moves. If a "target" moves $75 in 5 seconds, the Interceptor strikes the Polymarket book before it can reprice.
- **Maker (The Sentry)**: Maintains a dual-sided presence on the Window venue, capturing the spread while managing net exposure.
- **Arbitrage (The Surveyor)**: Constantly monitors the price sum of YES/NO pairs, looking for sub-$1.00 opportunities in low-fee venues.
- **Time Decay (The Ghost)**: Posts resting maker bids on both YES and NO of the Hourly venue during the theta window, earning the combined bid discount and settling at $1.00 with zero fee drag.
- **Basis/Funding (The Analyst)**: Fades retail skew by comparing Polymarket sentiment against Binance perpetual funding rates.
- **GBoost (The Cylon)**: Online gradient-boosted ML model (LogLoss) that learns from live orderbook + oracle features to predict near-term YES price direction, retraining continuously in the background.

---

## 🛡️ Safety Systems

- **Orphaned Position Detection**: Automatically "scuttles" one-sided hedged positions after 60s to prevent directional bleeding.
- **Fee Gates**: Hard-coded protection to ensure Taker strategies don't enter high-fee (1000 bps) environments.
- **Circuit Breaker**: Total system lockdown after 3 consecutive execution failures.

---

## ⚠️ Read This First

**This is experimental software. You will probably lose money. Start in GHOST mode and tune.**

- **Risk**: Momentum trades are directional and can get whiplashed. Arbitrage spreads are thin. Time decay positions can widen against you. None of this is guaranteed profit.
- **US Citizens**: Polymarket is rolling out US access under CFTC regulation via a waitlist. Check [polymarket.com](https://polymarket.com) for your current eligibility.
- **Competition**: Polymarket is full of well-funded, low-latency bots. This project is a learning exercise, not an edge.

---

## How It Works

The bot connects to Polymarket's CLOB via WebSocket for real-time orderbook data and to Binance for oracle pricing and perpetual futures funding rates. Every 50ms, the orchestrator evaluates all strategies concurrently, then dispatches the resulting signals to the execution layer.

### Strategies

**Momentum** — Detects when Binance price moves sharply before Polymarket reprices.
- **Velocity trigger**: `BTC_MOMENTUM_THRESHOLD` = $75/5s (example).
- **Strike buffer**: `BTC_STRIKE_BUFFER` = $50.0 (example).
- **Gates**: Requires building acceleration and a strong 1s short-window confirmation.
- **Sizing**: Fractional Kelly scaling based on signal strength.

**Maker** — Posts passive resting bids on **both YES and NO simultaneously** on the **Window/Maker venue**.
- **Imbalance gate**: Skips bids if the book is heavily skewed against us.
- **Net exposure**: Risk is measured as the directional imbalance (`|YES − NO|`).

**Arbitrage** — Buys both YES and NO on the **Window/Daily venue** when combined asks are < $1.00 (net of fees).
- **Profit Threshold**: Exploits small inefficiencies in the low-fee venue.

**Time Decay** — Exploits price convergence toward $1.00 as markets approach expiry. Posts resting GTC maker bids on **both YES and NO** on the **Hourly venue** during the theta window, collecting the spread at 0% maker fee and settling at $1.00.

**Basis / Funding-Rate** — Fades retail skew on the **Window venue** using Binance funding rates as confirmation.
- **Thesis**: Fades amateur over-betting when smart money (funding) disagrees.

**GBoost / ML** — Online gradient-boosted binary classifier running on the **Window/Daily venue**.
- **Model**: `perpetual` crate `PerpetualBooster` with `LogLoss` objective.
- **Features (13)**: YES/NO OBI, best ask prices, spreads, Binance 5s/1s velocity, acceleration, funding rate, 60m oracle drift, oracle price, **time-to-expiry (normalised to [0,1])**.
- **Label**: `1.0` if YES bid rises within `GBOOST_LOOKAHEAD_TICKS` ticks, `0.0` otherwise.
- **Retraining**: Every `GBOOST_RETRAIN_EVERY_N` ticks via `tokio::task::spawn_blocking` (never blocks the async executor). Requires `GBOOST_MIN_TRAINING_SAMPLES` snapshots before first model is available.
- **Persistence**: Model serialized to `GBOOST_MODEL_PATH` after each retrain and warm-loaded on startup. The filename is versioned (e.g. `gboost_model_v13f.json`) — see FAQ below.
- **Entry**: Buys YES if `P(UP) ≥ GBOOST_ENTRY_THRESHOLD`; buys NO if `P(UP) ≤ 1 − GBOOST_ENTRY_THRESHOLD`.
- **Exit**: Take-profit at `GBOOST_TARGET_PROFIT_PERCENT`, stop-loss at `GBOOST_STOP_LOSS_PERCENT` (after `GBOOST_MIN_HOLD_SECS`), or signal reversal when model flips conviction.

**Custom Strategy** — Develop and link your own strategies. See [CUSTOM_STRATEGY.md](docs/CUSTOM_STRATEGY.md).

---

### Strategy Segregation (Example Profile)

| Strategy | Capital Budget | Risk Model | Primary Venue |
|---|---|---|---------------|
| MomentumStrategy | `$15` | Gross one-sided | **Hourly**    |
| MakerStrategy | `$12` | Net \|YES−NO\| | **Window**    |
| ArbitrageStrategy | `$35` per leg | Gross hedged | **Window**    |
| TimeDecayStrategy | `$36` per leg | Gross hedged | **Hourly**    |
| BasisStrategy | `$15` | Gross one-sided | **Window**    |
| GboostStrategy | `$4` | Gross one-sided | **Window**    |

---

## Performance Tracking

The bot automatically records every completed trade into a daily CSV file for easy analysis.

- **Location**: `logs/{token}-trades_YYYY-MM-DD.csv` (e.g. `btc-trades_2026-04-29.csv`)
- **Columns**: Timestamp, Strategy, Market, Side (YES/NO), Entry Price, Exit Price, Shares, Profit (USDC), and Exit Reason.
- **Asynchronous**: Logging is non-blocking and happens in a background thread to maintain high-frequency trading performance.

---

## Setup

### Requirements
- Rust 1.95+ (or Docker)
- A Polygon wallet with USDC and MATIC
- Telegram bot token (optional)

### Configuration Profiles

**`src/config.rs` is NOT included in this repository.** It is your personal trading configuration and is intentionally gitignored so your own tuning stays private.

Three ready-to-use starting profiles are provided. **You must copy one to `src/config.rs` before you can build.**

| Profile | File | Wallet Size | Risk | Strategies Active |
|---------|------|-------------|------|-------------------|
| 🟢 Conservative | `src/config.conservative.rs.example` | < $100 | Low | Maker, Time Decay only |
| 🟡 Balanced | `src/config.balanced.rs.example` | $100–$300 | Medium | All six, moderate sizing |
| 🔴 Aggressive | `src/config.aggressive.rs.example` | $200+ | High | All six, maximum sizing |

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

**How can I optimize my host for maximum performance?**

See [docs/PERFORMANCE_TUNING.md](docs/PERFORMANCE_TUNING.md) for a full guide covering kernel `sysctl` tuning, CPU frequency governor, CPU/IRQ affinity pinning, Docker ulimits, and instance selection tips for AWS and OCI.

**Why doesn't DRADIS include a backtesting framework?**

Short answer: **Ghost mode running against live markets is a better substitute than it first appears**, and a traditional backtester would introduce more problems than it solves for prediction-market trading.

Here's why:

| Concern | Backtester | Ghost Mode |
|---|---|---|
| Market data fidelity | Requires storing full L2 orderbook snapshots (expensive, lossy) | Real-time Polymarket CLOB feed — 100% authentic |
| Strategy fidelity | Must mock async execution, cooldown maps, drawdown guards | Full production code path runs unchanged |
| Fill simulation | Assumes fills that may never occur in thin prediction markets | No fills in ghost mode — no wishful thinking |
| Regime coverage | Only covers periods you've collected data for | Every session captures current live regime |
| Build/maintain cost | Significant — separate data pipeline, replay harness, fill model | Zero — `GHOST_MODE = true` in `config.rs` |

**The recommended workflow instead:**

1. Set `GHOST_MODE = true` in `config.rs` and run overnight or across a full session.
2. Download your `session.file` and run `tools/session_parser.py` (see `tools/README.md`) for a per-trade breakdown with market context.
3. Identify loss patterns → tune `config.rs` constants → run another ghost session.
4. Repeat until the strategy shows consistent positive expectancy in ghost mode before enabling live execution.

This loop uses real market data, real strategy logic, and zero capital risk — which is exactly what a backtester promises but rarely delivers cleanly for illiquid, event-driven prediction markets.

**I pulled an update and GBoost is producing garbage predictions / the model won't load.**

The GBoost model is incompatible across feature vector changes.  The model file name in `GBOOST_MODEL_PATH` is intentionally **versioned** (e.g. `gboost_model_v13f.json`) so that a stale on-disk model with the wrong input dimension is never silently loaded against code expecting a different one.

If you pull an update and `NUM_FEATURES` in `src/strategies/gboost_impl.rs` has changed, you must:

1. Check whether the suffix in `GBOOST_MODEL_PATH` (in `src/config.rs` / your example profile) matches the new feature count.
2. If it doesn't — or if the old model file still exists under the old name — **delete the old file** and let the bot retrain from scratch:
   ```bash
   rm -f logs/gboost_model_*.json
   ```
3. Rebuild and restart. The model will cold-start, collect `GBOOST_MIN_TRAINING_SAMPLES` ticks (~16 seconds at 50 ms), then begin predicting.

The safe pattern when adding a new feature: bump the suffix in `GBOOST_MODEL_PATH` (e.g. `v13f` → `v14f`).  The old file is ignored, no manual cleanup needed.

---

## 📜 Credits & Acknowledgments

- **[Perpetual](https://github.com/perpetual-ml/perpetual)** — Provided the core Gradient Boosting implementation for our ML strategy.
- **[Tokio](https://github.com/tokio-rs/tokio)** — Powers our high-concurrency orchestrator.

## License
See [LICENSE](LICENSE).
