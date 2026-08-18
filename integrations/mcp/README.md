# DRADIS MCP Server

Read-only conversational access to a DRADIS deployment from any MCP client —
Claude Desktop, Claude Code, Copilot CLI, or anything else that speaks the
Model Context Protocol.

Ask *"why isn't FairValue trading?"*, *"what's my drawdown today?"*, or
*"show me pending AI proposals"* and get answers from the live engine.

## It runs on your machine, not on the trading box

The server speaks MCP over stdio to your client and plain HTTPS to your DRADIS
API. Nothing is installed on the engine and **no new port is opened** on the
machine holding your wallet keys — its attack surface is exactly what it was.
Your API key stays on your own machine.

## Read-only by construction

Two independent guarantees, either of which would be sufficient:

1. **This server has no code path that issues a non-GET request.** There is one
   egress function and it hardcodes `GET`.
2. **The engine enforces it too.** Running the deployment with
   `DRADIS_READ_ONLY=true` rejects every mutating method in middleware, below any
   endpoint logic.

Configuration changes, order placement and credential management are not exposed
over MCP. That is deliberate — see the MCP section of `ROADMAP.md` for why
write-capable tools need a scoped token that does not exist yet.

## Install

```bash
cd integrations/mcp
npm install
```

## Configure your client

**Claude Desktop** — `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "dradis": {
      "command": "node",
      "args": ["/absolute/path/to/DRADIS/integrations/mcp/server.js"],
      "env": {
        "DRADIS_API_URL": "https://your-dradis-host",
        "DRADIS_API_KEY": "your-api-key-if-set"
      }
    }
  }
}
```

**Claude Code:**

```bash
claude mcp add dradis -e DRADIS_API_URL=https://your-dradis-host \
  -e DRADIS_API_KEY=your-api-key -- node /absolute/path/to/integrations/mcp/server.js
```

| Variable | Required | Notes |
|---|---|---|
| `DRADIS_API_URL` | yes | Control Tower API base, no trailing slash |
| `DRADIS_API_KEY` | if set | Sent as `X-API-Key`; omit when the deployment has none |
| `DRADIS_TIMEOUT` | no | Per-request milliseconds, default 15000 |

Restart the client after editing its config.

## Tools

| Tool | Answers |
|---|---|
| `get_engine_status` | Is it running, on what market, with how much collateral |
| `get_viper_status` | **Why isn't it trading?** — per-strategy idle reasons |
| `list_positions` | What's open right now (`scope`: all / pending / confirmed) |
| `list_trades` | Closed trades with exit reasons (`limit`, `strategy`) |
| `get_trade_stats` | Win rate and P&L by strategy |
| `get_pnl_history` | Equity curve and drawdown |
| `get_raptor_telemetry` | Live signal values and which feeds are connected |
| `get_telemetry_history` | Signal trends over time |
| `list_squadrons` | Deployed squadrons, states, budgets |
| `get_config` | Current live tuning parameters |
| `get_llm_actions` | AI advisor audit trail and pending proposals |
| `get_recent_logs` | Raw engine log lines |

Start with `get_viper_status` for "why" questions and `get_trade_stats` for
"how is it doing" — both are pre-summarised, so they beat grepping logs.

## Troubleshooting

**Tools don't appear** — the client only reads its config at startup; restart it.
Check the path in `args` is absolute.

**"Could not reach the DRADIS API"** — the URL is wrong or the host isn't
reachable from your machine. Confirm with
`curl -s $DRADIS_API_URL/api/health`.

**HTTP 401** — the deployment sets `DRADIS_API_KEY` and yours doesn't match.
