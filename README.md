# DRADIS

> **Direct Reaction And Dynamic Intelligence System** — Low-latency Rust prediction-market trading bot for Polymarket. Six autonomous Viper strategies, a Raptor recon layer, a Squadron deployment framework, a real-time Next.js Control Tower, and an LLM Advisor that delivers optimization recommendations via Ollama (local or remote) + Telegram & OpenClaw.

![Rust](https://img.shields.io/badge/Rust-1.95+-orange?logo=rust&logoColor=white)
![Tokio](https://img.shields.io/badge/Tokio-async%20runtime-darkgreen?logo=rust&logoColor=white)
![axum](https://img.shields.io/badge/axum-REST%20API-blue?logo=rust&logoColor=white)
![Next.js](https://img.shields.io/badge/Next.js-15-black?logo=next.js&logoColor=white)
![Tailwind CSS](https://img.shields.io/badge/Tailwind-CSS-38bdf8?logo=tailwindcss&logoColor=white)
![Node.js](https://img.shields.io/badge/Node.js-20-brightgreen?logo=node.js&logoColor=white)
![Ollama](https://img.shields.io/badge/Ollama-LLM%20Advisor-blueviolet?logo=ollama&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-compose-2496ED?logo=docker&logoColor=white)
[![OpenClaw](https://img.shields.io/badge/OpenClaw-AI%20Integration-CC0000?logoColor=white)](https://openclaw.ai)
![License](https://img.shields.io/badge/License-GPLv3-blue)

**WARNING**: This is **ALPHA** software. You will probably lose money. Start in GHOST mode and tune before going live. Make sure to regularly pull updates as our own LLM advises on config and Viper strategy impls daily.

---

## ⚡ Quick Start

```bash
# 1. Clone and configure
git clone https://github.com/youruser/dradis.git && cd dradis
cp .env.example .env          # fill in POLYMARKET_PRIVATE_KEY, POLYGON_RPC_URL, TELEGRAM tokens, etc.
cp deploy-multi.sh.example deploy-multi.sh  # fill in HOST, USER, KEY
```

```bash
# 2. Deploy (builds Rust engine + Control Tower, starts Ollama, pulls model)
chmod +x deploy-multi.sh && ./deploy-multi.sh
```

After ~5 minutes the stack is live:

| Service | URL |
|---|---|
| **Control Tower** | `http://<host>:3002` |
| **DRADIS REST API** | `http://<host>:9000/api/health` |
| **Ollama** | `http://<host>:11434/api/tags` (internal) |

> **Prerequisites:** Docker on the remote host, Rust 1.95+ only needed for local builds.

---

## ️ Tactical Overview

DRADIS is a comprehensive trading automation platform for prediction markets. Built in Rust for maximum concurrency and memory safety, it evaluates selected markets every 50ms, coordinating multiple autonomous strategies to preserve capital and place orders where it sees inefficiencies.

The system is organized around three BSG-inspired tactical layers:

| Layer | Folder | Role |
|---|---|---|
| **Raptors** | `src/raptors/` | Signal scouts — fetch, normalise, broadcast external data |
| **Vipers** | `src/vipers/` | Trading strategies — evaluate signals and place orders |
| **Squadron** | `src/squadron/` | Deployment unit — bundles Raptors + Vipers onto a battle location |

---

## ️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         src/ layout                                 │
│                                                                     │
│  raptors/          ← Signal scouts (Binance WS + FAPI REST)         │
│  vipers/           ← Trading strategies (6 Vipers)                  │
│  squadron/         ← Deployment layer (Raptor+Viper+Market bundle)  │
│  orchestrator/     ← Strategy trait, registry, executor             │
│  tasks/            ← Market monitor, cleanup, chain-sync            │
│  helpers/          ← DB, orders, balance, metrics, notifications    │
│  api/              ← axum REST server (:9000)                       │
└─────────────────────────────────────────────────────────────────────┘
```

```
┌──────────────────────┐   ┌──────────────────────┐
│    src/raptors/      │   │  Polymarket CLOB     │
│  Price Raptor        │   │  (WebSocket Feed)    │
│  (Binance Spot WS)   │   │                      │
│  Funding Raptor      │   │                      │
│  (Binance FAPI REST) │   │                      │
└──────────┬───────────┘   └───────────┬──────────┘
           │  watch channels           │ orderbook WS
           └─────────────┬─────────────┘
                         ▼
           ┌─────────────────────────┐
           │   src/squadron/         │
           │   Squadron descriptor   │  ← SquadronRaptors (signal bundle)
           │   (battle location +    │  ← SquadronConfig  (which Vipers fly)
           │   Raptor+Viper bundle)  │  ← SquadronState   (STAGED→PATROLLING→RTB)
           └─────────────┬───────────┘
                         ▼
           ┌─────────────────────────┐
           │   Orchestrator (CIC)    │◄──── axum REST API (:9000)
           │     50ms Heartbeat      │
           └─────────────┬───────────┘
                         │  parallel dispatch
          ┌──────────────┼───────────────┬───────────────┐
          ▼              ▼               ▼               ▼
   ┌────────────┐ ┌──────────┐ ┌───────────────┐ ┌────────────┐
   │ Momentum   │ │  Maker   │ │  Arbitrage /  │ │   GBoost   │
   │   Viper    │ │  Viper   │ │  TimeDecay /  │ │    Viper   │
   │            │ │          │ │  Basis Vipers │ │   (ML)     │
   └──────┬─────┘ └────┬─────┘ └──────┬────────┘ └─────┬──────┘
          └────────────┼──────────────┴─────────────────┘
                       ▼
           ┌───────────────────────┐
           │    Execution Layer    │
           │  OBI Gate · Fee Gate  │
           │  Circuit Breaker      │
           └───────────────────────┘

           ┌───────────────────────┐
           │   Control Tower UI    │  Next.js dashboard (:3002)
           │  Viper toggles        │  ◄── PATCH /api/config
           │  P&L chart            │  ◄── GET  /api/pnl/history
           │  Open Positions       │  ◄── GET  /api/positions
           │  Trade log            │  ◄── GET  /api/trades
           └───────────────────────┘

           ┌───────────────────────┐     ┌────────────────┐
           │    LLM Advisor        │────►│  Ollama API    │
           │  (background task)    │     │  (your model)  │
           └──────────┬────────────┘     └────────────────┘
                      ▼
           ┌───────────────────────┐
           │   Telegram Channel    │
           └───────────────────────┘
```

### Core design principles

- **Parallel Dispatch**: Every 50ms heartbeat, the CIC evaluates all registered Vipers concurrently.
- **Isolated budgets**: Each Viper has its own independent capital budget and position book — a loss in one sector can't drain another's fuel.
- **OBI Veto**: Built-in Order Book Imbalance gate at −0.60 blocks entries into toxic flow / distribution walls.
- **Strategy Timeout**: Each Viper evaluation is hard-capped at 500ms. A hung Viper is skipped for that tick — the engine never freezes.
- **REST API**: axum server on `:9000` exposes live config, P&L, positions, and trade history to the Control Tower.

---

## 🦅 Raptor Wing (`src/raptors/`)

Raptors are DRADIS's recon layer — lightweight signal scouts that fly ahead of the Vipers and report external intelligence back to the CIC. Each Raptor polls a specific data source on its own schedule and publishes a normalized signal via `watch` channels.

Raptors are intentionally dumb: **fetch, normalise, broadcast** — no trading logic, no position awareness, no side effects.

| Raptor | Source | Signal | Module |
|---|---|---|---|
| **Price Raptor** | Binance Spot WS | spot price, 5s/1s velocity, acceleration, 10m/60m drift | `src/raptors/price.rs` |
| **Funding Raptor** | Binance Perpetuals FAPI | Perpetual funding rate (smart-money sentiment) | `src/raptors/funding.rs` |
| *(future)* **Sports Raptor** | Line movement APIs | Betting line drift, public money % | — |
| *(future)* **Politics Raptor** | Polling aggregators | Approval drift, event probability shifts | — |

When multiple Raptors are active, GBoost and Basis Vipers fuse their signals as features — no single Raptor has veto power alone.

---

## ✈️ Viper Wing (`src/vipers/`)

Six specialized Viper strategy classes. Each Viper is an autonomous tactical unit with its own capital budget, position book, and entry/exit logic.

| Viper | Venue | Description |
|---|---|---|
| **Momentum** | Hourly | Detects high-velocity Binance moves and strikes Polymarket before it reprices |
| **Maker** | Window | Dual-sided passive bids on YES+NO, capturing the spread while managing net exposure |
| **Arbitrage** | Window/Daily | Buys both YES+NO when combined asks are < $1.00 (net of fees) |
| **Time Decay** | Hourly | Posts resting GTC maker bids during the theta window; settles at $1.00 at 0% fee |
| **Basis** | Window | Fades retail skew using Binance funding rates as smart-money confirmation |
| **GBoost** | Window/Daily | Online gradient-boosted ML model retraining continuously on live orderbook + Raptor features |

Build your own: [CUSTOM_STRATEGY.md](docs/CUSTOM_STRATEGY.md).

---

## 🛩️ Squadron Layer (`src/squadron/`)

A **Squadron** is the core deployable unit — it bundles Raptors with Vipers and sends them to a specific Polymarket market (the **battle location**).

```
Squadron
├── Battle Location  →  MarketConfig (yes/no tokens, expiry, fees)
├── SquadronRaptors  →  typed bundle of Raptor watch::Receiver handles
├── SquadronConfig   →  RaptorProfile + ViperProfile composition spec
└── SquadronState    →  STAGED → DEPLOYED → PATROLLING → RTB → STOOD_DOWN
```

### Composition presets

| Preset | Raptors | Vipers |
|---|---|---|
| `full_wing` | Price + Funding | All six Vipers (current default) |
| `momentum_only` | Price only | Momentum + GBoost |
| `arb_wing` | Price + Funding | Arbitrage + Basis |

### Lifecycle states

| State | Meaning |
|---|---|
| `STAGED` | Assembled, waiting for a battle location |
| `DEPLOYED` | Market acquired, WS subscriptions live |
| `PATROLLING` | Active trading tick loop running |
| `RTB` | Returning to base — no new entries, winding down |
| `STOOD_DOWN` | Market expired or manually stood down |

Each market rotation logs: `🛩️ Squadron [btc-hourly-2026-05-23T14:00:00Z] → state=PATROLLING`

> **Phase 2 (current):** Squadron is a typed deployment descriptor constructed at each market rotation. **Phase 3 (CAG)** will promote it into a full async run-unit so multiple Squadrons can patrol different markets concurrently.

---

## ️ Control Tower — The Dashboard

DRADIS ships with a real-time web dashboard called **Control Tower** built on Next.js 15 + Tailwind CSS.

![Control Tower Dashboard](docs/ui-screenshot.png)

| Panel | What it shows |
|---|---|
| **Status Bar** | Engine online/offline, GHOST mode badge, active market, current BTC price, session P&L |
| **P&L Chart** | Rolling equity curve across recent snapshots |
| **Viper Cards** | Live enabled/disabled toggle + all parameters editable inline without a restart |
| **Open Positions** | In-flight positions with entry time, side (YES/NO/UP/DOWN in correct color), entry price, shares |
| **Trade Log** | Last N completed trades with strategy, side, entry/exit prices, shares, P&L, exit reason |

### Live Config Editing

Every parameter in the Viper cards maps directly to the runtime `DynamicConfig`. Editing a value sends `PATCH /api/config` — **no restart required**. Changes take effect on the next 50ms tick.

> **Hot-Enable Design** — All six Vipers are always instantiated at startup. The `DynamicConfig` enable flags are the sole runtime gate. Toggle any Viper on or off during a live session with immediate effect.

### Authentication

```bash
# .env (production)
CT_USERNAME=starbuck
CT_PASSWORD=your-strong-password
```

---

## LLM Advisor

Optional background task. Every `LLM_ADVISOR_INTERVAL_SECS` (default: 30 min) it fetches recent trades from SQLite, analyses them with a local Ollama model, and posts plain-English optimisation recommendations to Telegram.

```rust
// src/config.rs
pub const ENABLE_LLM_ADVISOR: bool = true;
pub const LLM_ADVISOR_INTERVAL_SECS: u64 = 1800;
pub const LLM_ADVISOR_TRADES_LOOKBACK: i64 = 20;
pub const LLM_OLLAMA_URL: &str = "http://localhost:11434";
pub const LLM_OLLAMA_MODEL: &str = "llama3.2";
```

```bash
# Override at runtime without rebuilding
OLLAMA_URL=http://192.168.1.10:11434
OLLAMA_MODEL=mistral
```

---

## ️ Safety Systems

- **Circuit breaker**: Pauses all trading after 3 consecutive execution failures.
- **TOCTOU-safe entry**: Atomic lock scope prevents duplicate orders.
- **Orphaned pair detection**: Automatically scuttles one-sided hedged positions after 60s.
- **Fee Gates**: Blocks Taker Vipers from entering high-fee (10%+) markets.
- **Chain-sync**: Startup and periodic reconciliation against on-chain wallet state — stale DB rows purged, missing positions re-adopted with correct side labels.

---

## ⚠️ Read This First

**This is experimental software. You will probably lose money. Start in GHOST mode and tune.**

- **Risk**: Momentum trades are directional and can get whiplashed. Arbitrage spreads are thin. None of this is guaranteed profit.
- **US Citizens**: Polymarket is rolling out US access under CFTC regulation via a waitlist.
- **Competition**: Polymarket is full of well-funded, low-latency bots. This project is a learning exercise, not an edge.

---

## Setup

### Requirements
- Rust 1.95+ (or Docker)
- A Polygon wallet with USDC and MATIC
- **A paid Polygon RPC endpoint** (required for auto-settlement)
- Telegram bot token (optional)

### RPC Configuration

> ⚠️ **Helius is Solana-only — do not use it for DRADIS.**

Recommended: [Alchemy](https://www.alchemy.com/), [QuickNode](https://www.quicknode.com/), [Infura](https://infura.io/)

```bash
POLYGON_RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_API_KEY
```

### Configuration Profiles

`src/config.rs` is gitignored. Copy one of the provided examples before building:

| Profile | File | Wallet | Risk | Vipers |
|---|---|---|---|---|
| Conservative | `src/config.conservative.rs.example` | < $100 | Low | Maker, Time Decay |
| Balanced | `src/config.balanced.rs.example` | $100–$300 | Medium | All six |
| Aggressive | `src/config.aggressive.rs.example` | $200+ | High | All six |

```bash
cp src/config.balanced.rs.example src/config.rs
cargo build --release
```

---

## Running

### Local Development

```bash
cp .env.example .env
cp src/config.balanced.rs.example src/config.rs
./start-local.sh            # builds, starts engine + Control Tower
tail -f logs/dradis-local.log
./stop-local.sh
```

Log filtering:
```bash
tail -f logs/dradis-local.log | grep -i "trade\|entry\|exit"   # trades
tail -f logs/dradis-local.log | grep "Squadron"                  # deployment lifecycle
tail -f logs/dradis-local.log | grep -E "WARN|ERROR"             # problems
```

### Production (Docker)

```bash
./deploy-multi.sh
```

Dashboard: `http://YOUR_SERVER_IP:3002`  
API health: `http://YOUR_SERVER_IP:9000/api/health`

---

## ️ Roadmap

### Recently shipped

- **Raptor / Viper / Squadron architecture** — Three-layer BSG tactical separation of concerns:
  - `src/raptors/` — Price Raptor (Binance WS) + Funding Raptor (Binance FAPI)
  - `src/vipers/` — six Viper trading strategies
  - `src/squadron/` — `Squadron`, `SquadronRaptors`, `SquadronConfig`, `SquadronState`
  - Each market rotation logs `🛩️ Squadron [...] → state=PATROLLING`
- **Open Positions improvements** — Side column colors YES/UP green and NO/DOWN red; chain-adopted positions show `⛓ adopted`; `chain_adopted` DB column with live migration
- **Side label fix** — `adopt_chain_position` correctly binds the Polymarket outcome string (was storing literal `?`)
- **Viper hot-enable** — All Vipers always instantiated at startup; toggle any live from Control Tower with no restart

### Phase 3 — CAG (next)

The **Commander Air Group** replaces the manual market-rotation loop with a proper async dispatch layer:
- `Squadron::patrol()` — async run-method, independently cancellable
- `Cag::dispatch(market)` — assembles the right Viper/Raptor mix for each market type
- `Cag::rtb(squadron_id)` — clean Squadron teardown on market expiry
- **Multi-Squadron** — two or more Squadrons patrolling different markets concurrently

### Medium-term
- Static deployment profiles (`profiles.toml`) with per-profile P&L tracking
- Profile selector in Control Tower
- LLM live config patches via Telegram approval gate

### Longer-term
- Sports Raptor (line movement feeds)
- Politics Raptor (polling aggregator feeds)
- Market-agnostic Viper interfaces for community-built strategies

---

## Integrations

### OpenClaw (Natural-Language Control)

```bash
openclaw skills install dradis-tactical-command
```

| You say | Effect |
|---|---|
| *"Pause GBoost"* | Stops GBoost entries on next tick |
| *"Enable ghost mode"* | Switches to paper trading instantly |
| *"What's my P&L today?"* | Returns session profit/loss |
| *"Show open positions"* | Lists all in-flight positions |
| *"Tighten GBoost stop loss to 8%"* | Updates risk parameter live |

```bash
# .env — enables API key enforcement for OpenClaw
DRADIS_API_KEY=replace-with-a-strong-random-secret
```

---

## FAQ

**Why Rust?** Fearless concurrency — evaluating six Vipers every 50ms needs a multi-threaded runtime with no GIL or GC pauses.

**Why isn't the bot trading?** Check: (1) `GHOST_MODE` true? (2) High-fee market? (3) Thresholds too tight in `config.rs`? (4) No Window/Daily market for Maker/Arb/Basis?

**I see two Vipers on the same token — is that a bug?** No. Each Viper has its own independent position book.

**How do I adjust risk live?** Use the Control Tower Viper cards or `PATCH /api/config`. No restart needed.

**GBoost producing garbage after an update?** The model file is incompatible across feature vector changes. Delete old files and let it cold-start:
```bash
rm -f logs/gboost_model_*.json
```
The safe pattern: bump the suffix in `GBOOST_MODEL_PATH` (e.g. `v14f` → `v15f`) when adding a new feature in `src/vipers/gboost_impl.rs`.

**Can I enable a Viper mid-session?** Yes — all six are always instantiated. Toggle via Control Tower or `PATCH /api/config`. Takes effect on the next 50ms tick.

**What about Kalshi?** DRADIS operates in the universe of PolyMarket. Kalshi is an alternate universe. Adama isn't ready to build this level of multiverse abstraction just yet.

**Control Tower shows "Offline"?** Check: (1) DRADIS running? (2) `curl http://localhost:9000/api/health`? (3) Docker — same `dradis-net` network?

**How can I tune my instance for maximum performance?** Please see our dedicated performance tuning guide: [PERFORMANCE_TUNING.md](docs/PERFORMANCE_TUNING.md).

**How do I enable the LLM Advisor?**
1. `ollama pull llama3.2`
2. `ENABLE_LLM_ADVISOR = true` in `config.rs`
3. `cargo build --release`
4. Set `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` in `.env`

**Why doesn't DRADIS include a backtesting framework?**

| Concern | Backtester | Ghost Mode |
|---|---|---|
| Market data fidelity | Requires storing full L2 orderbook snapshots | Real-time Polymarket CLOB — 100% authentic |
| Strategy fidelity | Must mock async execution, cooldown maps, drawdown guards | Full production code path runs unchanged |
| Fill simulation | Assumes fills that may never occur in thin markets | No fills in ghost — no wishful thinking |
| Build/maintain cost | Significant | Zero — `GHOST_MODE = true` in `config.rs` |

Workflow: ghost overnight → `tools/session_parser.py` → tune `config.rs` → repeat until positive expectancy.
