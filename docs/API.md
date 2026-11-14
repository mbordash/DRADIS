# REST API and integrations

The engine serves a REST API on `:9000`; the Control Tower, OpenClaw and the MCP server all use it.

## REST API endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Liveness check |
| `/api/assets` | GET | List initialized asset pools |
| `/api/config` | GET | Current DynamicConfig as JSON |
| `/api/config` | PATCH | JSON merge-patch — hot-reloads strategies |
| `/api/config/schema` | GET | Editable-config field schema |
| `/api/autoscale` | GET | Capital Auto-Scaler: each viper's record per squadron scope, the multiplier it earns, the factor last applied |
| `/api/pnl/history` | GET | P&L snapshots, `?hours=` window (default 24), `?limit=` points spread across it |
| `/api/pnl/span` | GET | Oldest P&L snapshot timestamp, bounds the chart's range control |
| `/api/trades` | GET | Recent completed trades |
| `/api/trades/stats` | GET | Trade count, wins, losses, realized P&L and fees; `?hours=` limits it to trades booked in that window (default lifetime) |
| `/api/positions` | GET | Current open positions |
| `/api/squadrons` | GET | List all active squadrons |
| `/api/squadrons/{id}` | GET | Get one squadron by id |
| `/api/squadrons/{id}/config` | GET/PATCH | Squadron-specific config |
| `/api/squadrons/deploy` | POST | Queue a new squadron deployment |
| `/api/deployments` | GET | List deployment queue status |
| `/api/deployment/region` | GET | Region + available market types |
| `/api/markets/available` | GET | Fetch markets by type from Gamma API |
| `/api/markets/live` | GET | Live markets for the Markets page (`?market_type=crypto|sports|politics&expiry_window=…`), joined with the squadrons flying them |
| `/api/markets/{id}` | GET | One market: facts, trading state, quotes, resolution once closed, DRADIS's own activity, and the engine's view (verdicts, model reading, bookmaker line) |
| `/api/markets/{id}/book` | GET | Depth per side, where the venue publishes it |
| `/api/markets/{id}/prints` | GET | Recent tape (`?limit=`), where the venue publishes it |
| `/api/markets/{id}/history` | GET | Price history per side (`?hours=`), where the venue publishes it |
| `/api/taxonomy/raptors` | GET | Raptor kinds for market class |
| `/api/taxonomy/vipers` | GET | Viper kinds for market class |
| `/api/telemetry` | GET | Live Raptor signal snapshots |
| `/api/llm/recommendations` | GET | Recent LLM Advisor analyses |
| `/api/llm/actions` | GET | AI config-change audit trail (proposed/applied/rejected/…) |
| `/api/llm/actions/{id}/approve` | POST | Approve a proposed AI change (revalidated, then applied) |
| `/api/llm/actions/{id}/reject` | POST | Reject a proposed AI change |
| `/api/setup/autonomy` | GET/PUT | AI autonomy tier, kill switch, breaker reset (admin-gated) |
| `/api/setup/export` | GET | Download portable config bundle — secrets + global/squadron configs (admin-gated) |
| `/api/setup/import` | POST | Restore a config bundle on a new instance; restart applies (admin-gated) |
| `/api/migration/status` | GET | Ledger size, retirement, backup progress, staged or applied restore (admin-gated) |
| `/api/migration/prepare` | POST | Retire this instance (no new orders, squadrons down) and build a full backup (admin-gated) |
| `/api/migration/archive/ticket` | POST | Mint a one-time, one-minute ticket that opens the archive download without the session header, so a browser navigation can stream it to disk (admin-gated) |
| `/api/migration/archive` | GET | Download the latest backup: databases, GBoost models and training data, config bundle (admin session, or `?ticket=` from the route above) |
| `/api/migration/resume` | POST | Clear the retirement and restart so this instance trades again (admin-gated) |
| `/api/migration/restore` | POST | Upload a backup (`?overwrite=true` to replace an instance that has trades); verified and staged (admin-gated) |
| `/api/migration/restore/apply` | POST | Restart and apply the staged restore before any database opens (admin-gated) |
| `/api/migration/restore/discard` | POST | Drop a staged restore (admin-gated) |

All data endpoints accept `?asset=btc` query param to scope to a specific asset pool.

## Integrations

### OpenClaw (Natural-Language Control)

```bash
openclaw skills install dradis-tactical-command
```

| You say                            | Effect                              |
|------------------------------------|-------------------------------------|
| *"Pause GBoost"*                   | Stops GBoost entries on next tick   |
| *"Enable ghost mode"*              | Switches to ghost mode (simulated fills) instantly |
| *"What's my P&L today?"*           | Returns session profit/loss         |
| *"Show open positions"*            | Lists all in-flight positions       |
| *"Tighten GBoost stop loss to 8%"* | Updates risk parameter live         |

```bash
# .env — enables API key enforcement for OpenClaw
DRADIS_API_KEY=replace-with-a-strong-random-secret
```

### MCP Server (Claude Desktop, Claude Code, any MCP client)

Query your running deployment conversationally from any client that speaks the
[Model Context Protocol](https://modelcontextprotocol.io) — 12 read-only tools
covering positions, trades, P&L, raptor telemetry and the AI advisor audit trail.

```bash
cd integrations/mcp && npm install
```

| You ask                                      | Tool used            |
|----------------------------------------------|----------------------|
| *"Why isn't FairValue taking trades?"*       | `get_viper_status`   |
| *"What's my drawdown this session?"*         | `get_pnl_history`    |
| *"Which raptor feeds are offline?"*          | `get_raptor_telemetry` |
| *"Show me every FairValue loss and its exit reason"* | `list_trades` |
| *"Are there pending AI config proposals?"*   | `get_llm_actions`    |

**It runs on your machine, not on the trading box.** The server speaks MCP over
stdio to your client and HTTPS to your DRADIS API, so no new port is opened on
the host holding your wallet keys and your API key never leaves your laptop.

**Read-only by construction.** The server has no code path that issues a
non-GET request, and pairing it with `DRADIS_READ_ONLY=true` makes the engine
reject mutating methods in middleware as well. Configuration changes and order
placement are deliberately not exposed — see [`integrations/mcp/README.md`](../integrations/mcp/README.md) for
setup for why write-capable tools are gated behind future work.
