# Getting started from source

Build and run DRADIS yourself. If you would rather launch a configured instance in your own AWS account, use the [AWS Marketplace AMI](https://aws.amazon.com/marketplace/pp/prodview-dv5dats2bmm5q) and its guides at <https://dradis.live/docs> instead.

> **This is experimental software. You will probably lose money. Start in GHOST mode and tune.**

## Requirements

**All builds:**
- Rust 1.95+ (or Docker)
- Telegram bot token (optional)
- Alpaca API key/secret (optional — free tier; powers both **Tide** and **Horizon** Raptors from one connection. Without it both cards stay idle.)
- The Odds API key (optional — free tier; only needed for the **Sports Raptor**'s book-consensus feed. Without it the Sports Raptor pill stays idle.)

**Polymarket International build (`intl_clob`) only:**
- A Polygon wallet with USDC and MATIC
- **A paid Polygon RPC endpoint** (required for auto-settlement)

**Polymarket US Retail build (`us_retail`) only:**
- Polymarket US developer-portal API key (Key ID + Ed25519 Secret Key) — no wallet or RPC needed

**Kalshi build (`kalshi`) only:**
- Kalshi API key (Key ID + RSA private key PEM) — no wallet or RPC needed; start with [demo.kalshi.co](https://demo.kalshi.co) paper trading

## Install

```bash
git clone https://github.com/mbordash/DRADIS.git && cd DRADIS
cp .env.example .env                               # credentials for your venue, Telegram, optional feeds
cp src/config.balanced.rs.example src/config.rs    # or conservative / aggressive, see Configuration profiles
```

## Choosing a venue

DRADIS compiles for **exactly one** execution venue, chosen at build time via a Cargo
feature. All venues share the same strategy/abstraction layers through the venue-neutral
`Execution` trait (`src/venues/core.rs`) and the shared `OrderLifecycle` reconciler
(`src/venues/lifecycle.rs`); only the venue module differs, so the unused venues'
dependencies are stripped from the binary.

| Feature              | Venue                              | Auth                                   | Gateway                              |
|----------------------|------------------------------------|----------------------------------------|--------------------------------------|
| `intl_clob` *(default)* | Polymarket International (self-custody) | EOA wallet + EIP-712 over Polygon      | `clob.polymarket.com`                |
| `us_retail`          | Polymarket US (custodial, CFTC)    | Ed25519 challenge-response → JWT        | `api.prod.polymarketexchange.com`    |
| `kalshi`             | Kalshi (custodial, CFTC)           | RSA-PSS request signing                 | `external-api.kalshi.com` (demo: `external-api.demo.kalshi.co`) |

### Polymarket US Retail configuration (`.env`)

```bash
POLYMARKET_US_KEY_ID=<key-id-uuid>      # developer-portal Key ID (X-PM-Access-Key)
POLYMARKET_US_SECRET_KEY=<base64-secret> # portal Secret Key (Base64 Ed25519 keypair), shown once
# optional:
POLYMARKET_US_BASE_URL=https://api.prod.polymarketexchange.com  # override (staging/mock)
POLYMARKET_US_TRADE_SIZE=10        # contracts per leg          (default 10)
POLYMARKET_US_ARB_EDGE=0.02        # min risk-free edge per pair (default $0.02)
POLYMARKET_US_MARKET_FILTER=chiefs # optional slug/question substring to pick a market
ASSETS=us                          # keep the dashboard pool tidy (US data lives in logs/us-dradis.db)
```

> **Polymarket US Retail status:** the MVP loop (`src/venues/us/trader.rs`) runs the venue-agnostic
> **arbitrage** strategy — discover a binary market → stream both legs over WebSocket →
> buy `YES`+`NO` for < $1 via an **engine-atomic** batched order (`/v1/orders/batched`) →
> reconcile via `OrderLifecycle`. Open positions and portfolio P&L appear in the Control Tower under the **`us`**
> asset selector. The Control Tower API stays live on `:9000` regardless. A second
> **crypto wing** (`us-crypto` asset) hunts crypto-class markets with the full Raptor
> stack, so all 12 crypto Vipers fly on them; among short-dated markets it takes the
> one-hour shape first. GBoost works here as it does on Kalshi: it scores one-hour BTC
> markets, keys its model on the market's underlying, reads the market's own mids from
> the book it streams (this venue publishes neither prints nor history), and records
> every one-hour market it flies so the pipeline can train a model stamped for this
> venue once a week of them has settled.

### Kalshi configuration (`.env`)

Kalshi signs every request locally with **RSA-PSS** — create an API key in your
account settings (Key ID + downloadable RSA private key PEM, shown once).

**Try it risk-free on the demo exchange** — [demo.kalshi.co](https://demo.kalshi.co)
is a full paper-trading environment with play money and the same live crypto markets;
demo credentials are separate from production.

**Fund the right exchange shard.** Kalshi runs separate matching engines
("exchange shards") and keeps a balance on each: crypto markets trade on shard 2, and
Kalshi is moving sports to shard 3 series by series, while politics and most
other markets stay on shard 0. The Kalshi app moves
collateral between shards as you trade; API orders do not, so a deposit sitting on
shard 0 leaves every crypto order rejected with `insufficient_shard_balance`. The
Setup tab shows the balance per shard, marks the shards your deployed squadrons'
markets trade on (read from the venue, market by market), and lets you move funds between them or set Kalshi's automatic
rebalancing target, each behind the admin session with a confirmation. The engine
refuses an order its shard cannot cover and says so on the Viper's card instead of
retrying. For a headless instance, `scripts/kalshi_shards.py` does the same from
the shell with the engine's own credentials and sends nothing without `--yes`.

```bash
KALSHI_API_KEY_ID=<key-id-uuid>          # from account settings (REQUIRED)
KALSHI_PRIVATE_KEY_PATH=data/kalshi-key.pem  # downloaded PEM (REQUIRED; or KALSHI_PRIVATE_KEY=<pem contents>)
KALSHI_DEMO=1                            # 1 = demo.kalshi.co paper trading, unset = production
# optional:
KALSHI_SERIES=KXBTC15M,KXBTCD             # crypto series to hunt (default shown); the rotation flies the first series' underlying only, others are ignored
KALSHI_MARKET_FILTER=bitcoin             # optional ticker/title substring to pick a market
ASSETS=kalshi                            # keep the dashboard pool tidy (data in logs/kalshi-dradis.db)
```

> **Kalshi status:** the loop (`src/venues/kalshi/trader.rs`) is **crypto-first** —
> it discovers the open markets across the configured series (`KXBTC15M` and
> `KXBTCD` for BTC, their ETH twins, by default), reads each market's window from
> its published open and close, and flies the hottest market of the best shape as
> the primary: Kalshi's one-hour above/below listing when one is on the board
> (`KXBTCD` carries both an hourly and a daily listing), the 15-minute market
> otherwise, with the daily market on the same coin beside it for the slower
> Vipers. It classifies both via the shared taxonomy and runs all crypto
> Vipers with the full Raptor intelligence stack. GBoost scores one-hour markets,
> the shape its model is trained on, so it idles and says so between hourly
> listings; a model stamped for another venue trades the shadow lane, settling
> through the venue's own resolution, until one stamped for Kalshi passes the
> holdout. Kalshi publishes no price history, so the training store is written
> live: every one-hour BTC market the loop flies is recorded (mids once a minute,
> the tape at the close, the venue's settlement afterwards) and the pipeline
> trains from those records once a week of them has settled. Order books stream over the
> authenticated WebSocket (`orderbook_delta` with sequence-gap recovery) and fills
> confirm event-precisely via the private `fill` channel. Kalshi's quadratic taker
> fee (max 1.75¢/contract at P=0.50) is priced into Viper edge thresholds. Positions
> and P&L appear under the **`kalshi`** asset selector.

## Configuration profiles

`src/config.rs` is gitignored. Copy one of the provided examples before building:

| Profile      | File                                 | Wallet    | Risk   | Vipers            |
|--------------|--------------------------------------|-----------|--------|-------------------|
| Conservative | `src/config.conservative.rs.example` | < $100    | Low    | Maker, Time Decay |
| Balanced     | `src/config.balanced.rs.example`     | $100–$300 | Medium | All crypto, Anchor off |
| Aggressive   | `src/config.aggressive.rs.example`   | $200+     | High   | All crypto, Anchor off |

```bash
cp src/config.balanced.rs.example src/config.rs
cargo build --release
```

The Docker image compiles the **conservative** profile by default (the Marketplace AMI ships it). A non-customer build can choose another with build arguments; the image records what it compiled in `/app/build-profile`:

```bash
docker build --build-arg DRADIS_PROFILE=balanced -t dradis .                                   # or aggressive
docker build --build-arg DRADIS_PROFILE=local --build-arg DRADIS_FORCE_GHOST=true -t dradis .  # your own src/config.rs, simulation compiled on
```

## Optional data feeds

### Tide + Horizon Raptors (Alpaca IEX) — optional

The Tide and Horizon Raptors share a **single Alpaca IEX WebSocket connection** (free tier allows only one per account). Together they stream 6 symbols:

| Raptor   | Symbols         | Signal |
|----------|-----------------|--------|
| **Tide** | IBIT, FBTC, ARKB | BTC ETF premium vs synthetic iNAV → "Institutional Pulse" |
| **Horizon** | SPY, QQQ, UVXY | TradFi velocity, macro coherence (BTC↔QQQ), VIX proxy |

Both are BTC-only and active during US market hours (09:30–16:00 ET). To enable them, add your Alpaca keys to `.env`:

```bash
ALPACA_API_KEY_ID=your-key-id
ALPACA_API_SECRET_KEY=your-secret-key
```

Tide feeds the Basis tide veto and the **Convergence** Viper. Horizon (TradFi velocity, macro coherence, VIX proxy/velocity) drives preventative gates on **Maker** (VIX-spike / coherent-TradFi-flow quote suppression) and **TrendCapture** (fade veto when TradFi confirms the drift) — the gates run observe-first and enforce once calibrated (`MAKER_HORIZON_GATE_ENFORCE`, `TRENDREVERSAL_HORIZON_VETO_ENFORCE`). Omit the keys and both run idle (neutral snapshots, offline pills).

### Sports Raptor (cross-book consensus) — optional

The Sports Raptor matches every Polymarket International sports moneyline to a
[The Odds API](https://the-odds-api.com) event and records the vig-free cross-book
consensus against Polymarket's own bid and ask, one line per outcome token, plus
**drift** (movement since that game's previous snapshot) and **book dispersion**
(soft-line disagreement). It is **recording only** — no Viper trades on it yet. On
Kalshi and Polymarket US it reports telemetry but feeds no strategy, because
discovery and prices come from Polymarket. Snapshots are taken at fixed offsets
before each kick-off and budgeted against your key's own quota, so it runs on the
free tier as well as a paid plan. To enable it, add your key to `.env`:

```bash
ODDS_API_KEY=your-the-odds-api-key
```

The free tier is a hard ~500 requests/month, so the raptor polls conservatively
every 2 hours (~360 requests/month, leaving headroom). It publishes telemetry only
— no Viper consumes it for sizing yet. The remaining monthly budget is logged each
poll (from the API's `x-requests-remaining` header), with a loud warning when it
runs low. Omit the key and it runs idle (neutral snapshot, offline pill).

### RPC Configuration

Recommended: [Alchemy](https://www.alchemy.com/), [QuickNode](https://www.quicknode.com/), [Infura](https://infura.io/)

```bash
POLYGON_RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_API_KEY
```

## Running locally

```bash
./start-local.sh                  # Polymarket International (default), BTC
./start-local.sh eth              # Polymarket International, ETH
VENUE=us ./start-local.sh         # Polymarket US
VENUE=kalshi ./start-local.sh     # Kalshi
RUST_LOG=debug ./start-local.sh   # verbose logging

tail -f logs/dradis-local.log
./stop-local.sh
```

After a few minutes the stack is live:

| Service             | URL                                       |
|---------------------|-------------------------------------------|
| **Control Tower**   | `http://<host>:3002`                      |
| **DRADIS REST API** | `http://<host>:9000/api/health`           |
| **Ollama**          | `http://<host>:11434/api/tags` (internal) |

### Building manually

```bash
# International CLOB (default)
cargo build --release
cargo test

# US Retail
cargo build  --release --no-default-features --features us_retail
cargo test            --no-default-features --features us_retail

# Kalshi
cargo build  --release --no-default-features --features kalshi
cargo test            --no-default-features --features kalshi
```

### Multi-asset mode

```bash
# .env — run BTC, ETH, and SOL loops concurrently
ASSETS=btc,eth,sol

# Each asset gets its own SQLite DB file:
#   logs/btc-dradis.db  (primary — default REST API / Control Tower view)
#   logs/eth-dradis.db
#   logs/sol-dradis.db
# Use ?asset=eth on API endpoints to scope responses to a specific asset.
```

Log filtering:
```bash
tail -f logs/dradis-local.log | grep -i "trade\|entry\|exit"   # trades
tail -f logs/dradis-local.log | grep "Squadron"                  # deployment lifecycle
tail -f logs/dradis-local.log | grep "btc\|eth\|sol"             # per-asset activity
tail -f logs/dradis-local.log | grep -E "WARN|ERROR"             # problems
```

## Production (Docker)

```bash
cp deploy-multi.sh.example deploy-multi.sh   # fill in HOST, USER, KEY
./deploy-multi.sh
```

Dashboard: `http://YOUR_SERVER_IP:3002`  
API health: `http://YOUR_SERVER_IP:9000/api/health`

Upgrading an existing install: [UPGRADING.md](UPGRADING.md). Something not working: [TROUBLESHOOTING.md](TROUBLESHOOTING.md).
