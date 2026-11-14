# DRADIS Architecture

How the engine is put together: the four tactical layers, the design rules they share, and the safety systems around them. For the strategies themselves see [docs/VIPERS.md](docs/VIPERS.md); for the REST API see [docs/API.md](docs/API.md).

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         src/ layout                                 │
│                                                                     │
│  raptors/          ← Signal scouts (Binance WS + FAPI, The Odds API)│
│  vipers/           ← Trading strategies (12 Vipers)                 │
│  squadron/         ← Deployment layer (Raptor+Viper+Market bundle)  │
│  cag/              ← Commander (async dispatch, multi-asset)        │
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
│  Derivatives Raptor  │   │                      │
│  (Binance FAPI: OI)  │   │                      │
│  Tide Raptor         │   │                      │
│  (Alpaca IEX + iNAV) │   │                      │
│  Horizon Raptor      │   │                      │
│  (SPY/QQQ/UVXY)      │   │                      │
│  Sports Raptor       │   │                      │
│  (The Odds API)      │   │                      │
└──────────┬───────────┘   └───────────┬──────────┘
           │  watch channels           │ orderbook WS
           └─────────────┬─────────────┘
                         ▼
           ┌─────────────────────────┐
           │   src/cag/              │  ← CAG (Commander Air Group)
           │   run_market_loop()     │  ← one tokio task per asset
           │   SessionState          │  ← per-asset P&L + collateral
           └─────────────┬───────────┘
                         │  (BTC task) (ETH task) (SOL task) …
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
           ┌─────────────┼───────────────┬───────────────┬──────────────┐
           ▼             ▼               ▼               ▼              ▼
    ┌────────────┐ ┌──────────┐ ┌───────────────┐ ┌────────────┐ ┌──────────────┐
    │ Momentum/  │ │  Maker   │ │  Arbitrage /  │ │   GBoost   │ │ TrendCapture │
    │ FairValue  │ │  Viper   │ │  TimeDecay /  │ │    Viper   │ │    Viper     │
    │   Viper    │ │          │ │  Basis Vipers │ │   (ML)     │ │ (drift/trend)│
    └──────┬─────┘ └────┬─────┘ └──────┬────────┘ └─────┬──────┘ └──────┬───────┘
           └────────────┼──────────────┴────────────────┴───────────────┘
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
- **Multi-asset concurrency**: Each asset runs in its own `tokio::spawn`ed task with independent raptors and session state. The tokio runtime sizes its worker pool to the host's core count (floor 2) to avoid oversubscribing small instances; set `TOKIO_WORKER_THREADS` to raise it for multi-asset boxes (e.g. BTC + ETH + SOL on a 4+ vCPU host). Blocking work runs on the dedicated `spawn_blocking` pool, so matching workers to cores is the correct configuration.
- **OS-thread watchdog**: A native OS thread (outside the tokio runtime) checks an atomic heartbeat every 60 s. If the trading loop goes silent for 5 minutes, it calls `process::exit(1)` to trigger Docker's restart policy — immune to tokio runtime deadlocks. It stands down while the engine is deliberately parked awaiting Setup, so a box with no credentials yet is not mistaken for a stalled one. On a stall it dumps every thread's stack before exiting — see Diagnosing a stall below.
- **OBI Veto**: A built-in Order Book Imbalance gate at −0.60 blocks entries into toxic flow / distribution walls.
- **Strategy Timeout**: Each Viper evaluation is hard-capped at 500ms. A hung Viper is skipped for that tick — the engine never freezes.
- **REST API**: axum server on `:9000` exposes live config, P&L, positions, and trade history to the Control Tower.

## Raptor Wing (`src/raptors/`)

Raptors are DRADIS's recon layer — lightweight signal scouts that fly ahead of the Vipers and report external intelligence back to the CIC. Each Raptor polls a specific data source on its own schedule and publishes a normalized signal via `watch` channels.

Raptors are intentionally dumb: **fetch, normalize, broadcast** — no trading logic, no position awareness, no side effects.

| Raptor                         | Source                  | Signal                                                  | Module                   |
|--------------------------------|-------------------------|---------------------------------------------------------|--------------------------|
| **Price Raptor**               | Binance Spot WS         | spot price, 5s/1s velocity, acceleration, 10m/60m drift | `src/raptors/price.rs`   |
| **Funding Raptor**             | Binance Perpetuals FAPI | Perpetual funding rate (smart-money sentiment)          | `src/raptors/funding.rs` |
| **Derivatives Raptor**         | Binance Perpetuals FAPI | Open-interest delta + taker CVD ratio (positioning pressure, all-asset) | `src/raptors/derivatives.rs` |
| **Tide Raptor**                | Alpaca IEX + synthetic iNAV | "Institutional Pulse" + coherence from spot-BTC-ETF (IBIT/FBTC/ARKB) premium vs iNAV — BTC-only, US-hours | `src/raptors/tide.rs` |
| **Horizon Raptor**             | Alpaca IEX (shared)     | TradFi velocity (SPY/QQQ), macro coherence (BTC↔QQQ), VIX proxy (UVXY) — BTC-only, US-hours | `src/raptors/horizon.rs` |
| **Sports Raptor**              | The Odds API (h2h)      | Vig-free cross-book consensus, drift and dispersion per matched moneyline, keyed by outcome token — all three venues; read by **Bookline** | `src/raptors/sports_ledger.rs` |
| **Tennis Raptor**              | Live Tennis API (REST)  | Live tennis event state: score, serving side, break-point flag, feed staleness — venue-neutral, **observe-only** | `src/raptors/tennis.rs` |
| *(future)* **Politics Raptor** | Polling aggregators     | Approval drift, event probability shifts                | —                        |

When multiple Raptors are active, Basis, Momentum and TrendCapture use them as confirmation gates; Maker and TrendCapture consume the Horizon macro signal as preventative gates (VIX-spike / coherent-TradFi-flow quote suppression, fade veto — observe-first, enforcement behind config flags); and the **Convergence** Viper opens directional positions only when the institutional + derivatives stack agrees. No single Raptor has veto power alone.

The **Tide** and **Horizon** Raptors share a single Alpaca IEX WebSocket connection (free tier allows only one per account). Tide tracks BTC-specific institutional flow (ETF premium); Horizon tracks TradFi macro regime (equity velocity, VIX). Together they enable divergence detection — e.g., equities selling off but BTC ETFs at premium suggests institutional flight *into* crypto.

The **Sports Raptor** is the first non-crypto scout. It matches each venue's sports moneyline to a bookmaker event, reduces that event's cross-book h2h odds to a vig-free consensus, and publishes one line per outcome token, so a squadron reads its own game rather than whichever game happens to be next. Matching is per-venue behind one `SportsCatalog` seam, so the same board serves Polymarket International (Gamma token ids), Kalshi (game-series tickers, with kick-off recovered from the ticker or its expiration) and Polymarket US (signed moneyline records). Every snapshot is recorded with the raw per-book prices behind it, so the history can be re-analyzed rather than taken on trust. Snapshots are taken on a schedule relative to kick-off (`SPORTS_LEDGER_SNAPSHOT_OFFSETS_MINS`), tightening as the game approaches, and the raptor stays idle without `ODDS_API_KEY`. One Viper reads it: **Bookline**.

The **Tennis Raptor** reads the event itself rather than the betting line: it polls the Live Tennis API's live-match endpoint (keyed on `LIVETENNIS_API_KEY`), tracks one live match (sticky by id, otherwise the freshest score), and broadcasts sets/games/points, the serving side, and a derived break-point flag (receiver at AD, or receiver at 40 vs a server below 40 — never in tiebreaks). Feed health follows the same stale-feed-reads-as-disconnected rule as the other scouts: a score older than `TENNIS_SCORE_STALENESS_SECS` reports `tennis_connected = false` alongside its age, so a consumer widens or pulls, never holds. Honest tier facts: the free tier is 30 req/min / 100 req/day — the default `TENNIS_POLL_SECS = 900` fits all-day polling inside the free cap, while ~60s polling gives near point-level tracking for only ~100 minutes/day (develop-and-test, or following a few matches; sustained fast polling needs a paid tier). The provider's push WebSocket and model win-probability fields are top-tier features and are **not** used — this raptor is free-tier REST only, observe-only, and degrades silently to a neutral snapshot without a key.

## Squadron Layer (`src/squadron/`)

A **Squadron** is the core deployable unit — it bundles Raptors with Vipers and sends them to a specific Polymarket market (the **battle location**).

```
Squadron
├── Battle Location  →  MarketConfig (yes/no tokens, expiry, fees)
├── SquadronRaptors  →  typed bundle of Raptor watch::Receiver handles
├── SquadronConfig   →  RaptorProfile + ViperProfile composition spec
└── SquadronState    →  STAGED → DEPLOYED → PATROLLING → RTB → STOOD_DOWN
```

### Market Taxonomy

Markets are classified into domains that determine which Raptors and Vipers are meaningful:

| Market Class | Raptors | Vipers                                      |
|--------------|---------|---------------------------------------------|
| `crypto` | Price, Funding, Derivatives, Tide | All crypto Vipers                           |
| `sports` | Sports (cross-book consensus) | Bookline, Arbitrage, Maker (venue-agnostic) |
| `politics` | Politics (roadmap) | Arbitrage, Maker (venue-agnostic)           |

Classification is data-driven via the `market_class_rule` table — add a new mapping (e.g., `tennis → sports`) with one INSERT, no code change.

### Composition presets

| Preset          | Raptors         | Vipers                             |
|-----------------|-----------------|------------------------------------|
| `full_wing`     | Price + Funding + Derivatives + Tide | All crypto Vipers (current default) |
| `momentum_only` | Price only      | Momentum + GBoost                  |
| `arb_wing`      | Price + Funding | Arbitrage + Basis                  |

### Lifecycle states

| State        | Meaning                                          |
|--------------|--------------------------------------------------|
| `STAGED`     | Assembled, waiting for a battle location (user-deployed via UI) |
| `DEPLOYED`   | Market acquired, WS subscriptions live           |
| `PATROLLING` | Active trading tick loop running                 |
| `RTB`        | Returning to base — no new entries, winding down |
| `STOOD_DOWN` | Market expired or manually stood down            |

Each market rotation logs: `Squadron [btc-hourly-2026-05-23T14:00:00Z] → state=PATROLLING`

## CAG Layer (`src/cag/`)

The **Commander Air Group** is the async orchestration layer that sits between `main.rs` and the Squadron/Orchestrator. It owns the market rotation loop for each asset and manages session-level state.

```
CAG
├── Cag              →  global registry (shared across all asset tasks)
├── SessionState     →  per-asset P&L, starting/live collateral, position tracking
├── RunArgs<P>       →  typed bundle passed into each concurrent market-loop task
└── run_market_loop  →  async fn — the full patrol loop for one asset
```

### Multi-asset concurrency

Set `ASSETS=btc,eth,sol` to run three independent patrol loops in parallel. Each asset gets its own:
- Price Raptor + Funding Raptor (watch channels)
- `SessionState` (isolated P&L and collateral tracking)
- LLM Advisor background task
- `tokio::spawn`ed `run_market_loop` task

**Shared** across all assets: `trading_client`, `nonce_manager`, `wallet_provider`, CAG registry, `DynamicConfig` watch channel, axum API server.

```bash
# .env — multi-asset (BTC + ETH + SOL in parallel)
ASSETS=btc,eth,sol

# .env — single-asset fallback (backward-compatible)
CRYPTO_FILTER=btc
```

> Each asset owns its own SQLite DB file (`logs/btc-dradis.db`, `logs/eth-dradis.db`, etc.). The primary asset (first in `ASSETS`) also backs the default REST API view; pass `?asset=eth` query params to scope API responses to a specific asset pool.

### Capital Auto-Scaler

*New in 1.6.0, ships off.* A CAG-level sizing policy (Setup → Risk & Engine) applied to every squadron beneath it: each viper's **entry** orders are scaled by that viper's own recent results in its own squadron.

- **Signal: return on capital**, realized P&L ÷ dollars risked (entry price × shares) over the viper's last *Lookback Trades* (30) closed trades. Not win rate: on a binary a 70% win rate bought at 0.80 loses money.
- **Scope: viper × squadron.** Anchor on the BTC squadron scales on Anchor's BTC trades only; a losing viper cannot ride a winning one's record. Simulated and real ledgers never mix.
- **The multiplier** is 1× until *Min Trades* (20), then runs linearly from 1× at zero return to *Max Multiplier* (2×) at *Full-Scale Return* (+10%), and down to *Min Multiplier* (0.5×) at −10%.
- **Limits on growth:** never past *Max Scaled Order* ($25 per order, both legs of a pair together; 0 = no growth), never past the viper's own Max Exposure counting what it already holds, never while the daily drawdown breaker is tripped. A limit only stops growth; it never shrinks the viper's own order. Shrinking never goes below the venue's minimum order size.
- **Never scaled:** exits, and Helm (the operator's own sizing).
- **Visible:** while the scaler is on, each viper card on a squadron page carries a 📐 badge with the multiplier it earns there (green above 1×, red below, `1× · 12/20` while it builds its record), with the return and the factor last applied in its tooltip. The Risk & Engine card shows every viper's record, return, multiplier and last applied factor; `GET /api/autoscale` returns the same. Each scaled entry logs `📐 Auto-scaler`.

The settings are instance-wide and are not part of the risk profiles, so applying a profile never arms or retunes the scaler.

## Safety Systems

- **Circuit breaker**: Pauses all trading after 3 consecutive execution failures.
- **Daily drawdown limit**: every viper, Anchor included since 1.6.0, stops opening positions once the day's loss reaches the limit: the Daily Drawdown Limit set in Setup (Engine) since 1.8.2, or when that is 0, the profile's limit (conservative: the larger of 0.75% of the starting balance and $3). The day is counted from midnight ET and clears at the next midnight; until 1.6.0 it counted from engine start and held until a restart. Exits are never blocked.
- **TOCTOU-safe entry**: Atomic lock scope prevents duplicate orders.
- **Orphaned pair detection**: Arbiter waits 5s after first-leg confirm before acting on a missing second leg. TimeDecay GTC bids are given the full theta window (up to 30 min) before a resting order is declared orphaned, on every venue since 1.6.0 (Kalshi and Polymarket US canceled the partner after 60 s before).
- **Capital Auto-Scaler limits**: when enabled, a scaled-up entry is bounded by a per-order dollar cap and the viper's own runtime Max Exposure, and never grows while the drawdown breaker is tripped (see [Capital Auto-Scaler](#capital-auto-scaler)).
- **Rescue-profit gate**: Arbitrage entries are blocked when a single-leg failure cannot be rescued into profit (`yes_rescue_cost` or `no_rescue_cost ≥ $1.00` including fees and rehedge buffer).
- **Fee Gates**: Blocks Taker Vipers from entering high-fee (10%+) markets.
- **AI autonomy guardrails**: LLM-authored config changes are schema-clamped at every tier, money fields never auto-apply below full autonomy, and a P&L circuit breaker reverts recent AI changes and demotes to recommend-only on a drawdown (see [LLM Advisor](docs/LLM_ADVISOR.md)).
- **Chain-sync**: Startup and periodic reconciliation against on-chain wallet state — stale DB rows purged, missing positions re-adopted with correct side labels.
