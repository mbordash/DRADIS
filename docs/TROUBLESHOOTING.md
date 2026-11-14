# Troubleshooting and FAQ

## Diagnosing a stall

If the trading loop goes quiet, the OS-thread watchdog emits a soft warning at
180 s and exits the process at 300 s so the supervisor restarts it. Both points
dump the stack of **every** thread, and the soft warning is usually the more
useful of the two: the process is still alive there, so whatever holds a lock is
still holding it.

The dump names the phase the loop was in (`SIGNAL_EVAL`, `MARKET_ROTATE`,
`ORDER_PLACE`, …) and, inside signal evaluation, which viper or setup step.
That alone is often enough. When it is not, the stacks need symbolizing:

- **Release builds carry line tables** (`[profile.release] debug = 1`). Debug
  info does not change code generation — the binary runs exactly as it would
  without it.
- **The runtime image is stripped**, so the shipped binary stays small. The
  matching **unstripped** binaries are published with each AMI release as
  `dradis-debuginfo-v1.0.0-ami.N.tar.gz`.
- Symbolize a customer's dump against the tarball **for the version they are
  running** — the addresses are meaningless against any other build.

```bash
# macOS
atos -o dradis-intl.debug -arch arm64 -l <LoadAddress> <address>
# Linux
eu-addr2line -e dradis-intl.debug -f -C <address>
```

The load address is printed at the top of the dump. A mismatch between binary
and symbols yields confident nonsense rather than an error, so check the version
before trusting a result.

## FAQ

**Why Rust?** Fearless concurrency — evaluating a dozen Vipers every 50ms needs a multi-threaded runtime with no GIL or GC pauses.

**Can I trade multiple assets at once?** Yes — set `ASSETS=btc,eth,sol` in `.env`. Each asset runs its own independent patrol loop (raptors, session state, LLM advisor, SQLite DB) inside a `tokio::spawn`ed task. The wallet, CLOB client, and API server are shared. Each asset writes to its own DB file (`logs/btc-dradis.db`, `logs/eth-dradis.db`, etc.); pass `?asset=eth` to any API endpoint to scope results to that asset.

**Why isn't the bot trading?** Check: (1) `GHOST_MODE` true? (2) High-fee market? (3) Thresholds too tight in `config.rs`? (4) No Window/Daily market for Maker/Arb/Basis?

**I see two Vipers on the same token — is that a bug?** No. Each Viper has its own independent position book.

**How do I adjust risk live?** Use the Control Tower Viper cards or `PATCH /api/config`. No restart needed.

**GBoost is idle?** It trades only from an offline-trained model file at `logs/{asset}-gboost_planb_v1.json` (`GBOOST_PLANB_MODEL_FILENAME`). Without the file it logs the reason once and stays idle; there is no in-process training or cold start. A model whose stamped feature names do not match the build is refused at load, with the reason in the log.

**Can I enable a Viper mid-session?** Yes — all Vipers are always instantiated. Toggle via Control Tower or `PATCH /api/config`. Takes effect on the next 50ms tick.

**Does DRADIS support the US Polymarket API?** Yes.  Polymarket's **US platform** is a separate, custodial, CFTC-regulated exchange with web2 auth (API key / secret / session token) and string/UUID market IDs. We have **venue abstraction** so a build targets one venue via a Cargo feature flag (`intl_clob` default, `us_retail`, `kalshi`) — single-venue per binary, so the US deployment carries none of the Polygon crypto weight and stays inside its own regulatory/network footprint. Start a US build with `VENUE=us ./start-local.sh`.

**What about Kalshi?** Fully supported. Build with `--features kalshi` (or `VENUE=kalshi ./start-local.sh`) — RSA-PSS signed REST + WebSocket, crypto-first trading loop over Kalshi's hourly, 15-minute and daily BTC/ETH series (the one-hour market is preferred when listed), all crypto Vipers with the full Raptor stack (GBoost scores one-hour markets only, see above), and event-precise fill confirmation. Paper-trade it risk-free against [demo.kalshi.co](https://demo.kalshi.co) with `KALSHI_DEMO=1`. See "Kalshi configuration" above.

**Control Tower shows "Offline"?** Check: (1) DRADIS running? (2) `curl http://localhost:9000/api/health`? (3) Docker — same `dradis-net` network?

**How can I tune my instance for maximum performance?** Please see our dedicated performance tuning guide: [PERFORMANCE_TUNING.md](PERFORMANCE_TUNING.md).

**How do I enable the LLM Advisor?**
1. `ollama pull llama3.2`
2. `ENABLE_LLM_ADVISOR = true` in `config.rs`
3. `cargo build --release`
4. Set `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` in `.env`

**Why doesn't DRADIS include a backtesting framework?**

| Concern              | Backtester                                                | Ghost Mode                                 |
|----------------------|-----------------------------------------------------------|--------------------------------------------|
| Market data fidelity | Requires storing full L2 orderbook snapshots              | Real-time Polymarket CLOB — 100% authentic |
| Strategy fidelity    | Must mock async execution, cooldown maps, drawdown guards | Full production code path runs unchanged   |
| Fill simulation      | Assumes fills that may never occur in thin markets        | Fills ARE simulated, optimistically at the quoted price    |
| Build/maintain cost  | Significant                                               | Zero — `GHOST_MODE = true` in `config.rs`  |

**Read ghost P&L as a smoke test, not an expectancy estimate.** A ghost fill is
booked at the price the strategy quoted, at the moment it quoted — no queue
position, no partial fills, no adverse selection. Real books do not oblige. Ghost
mode answers "does the whole path work end to end: signal → order → position →
exit → booked trade", which is exactly what a backtest cannot tell you. It does
not answer "is this strategy profitable".

Workflow: ghost overnight → `tools/session_parser.py` → tune `config.rs` → repeat until positive expectancy.

**Can DRADIS link with quantum computing?** Possibly. See [QUANTUM.md](QUANTUM.md) for the research proposal, including where a quantum solver could help and the conditions under which the work stops.
