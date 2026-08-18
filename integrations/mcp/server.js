#!/usr/bin/env node
// SPDX-License-Identifier: AGPL-3.0-only
//
// DRADIS MCP server — read-only conversational access to a DRADIS deployment.
// Copyright (C) 2026 Michael Bordash
//
// This file is part of DRADIS. DRADIS is free software: you can redistribute it
// and/or modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.

/**
 * Runs on the OPERATOR'S machine, not on the trading box.
 *
 * It speaks MCP over stdio to the client (Claude Desktop, Copilot CLI, …) and
 * plain HTTPS to a DRADIS API. That direction matters: the engine opens no new
 * port, its attack surface is unchanged, and the API key stays on the operator's
 * machine rather than being handed to a remote listener sitting next to wallet
 * keys.
 *
 * READ-ONLY BY CONSTRUCTION. `apiGet` is the only request function in this file
 * and it hardcodes GET; there is no code path that can issue a mutating request.
 * That is deliberately belt-and-braces with the engine's own DRADIS_READ_ONLY
 * middleware, which rejects non-GET methods below any endpoint logic. Either one
 * alone would do; together, a mistake in one is not enough to write anything.
 *
 * Configuration (environment):
 *   DRADIS_API_URL   e.g. https://dradis.example.com  (required)
 *   DRADIS_API_KEY   sent as X-API-Key when the deployment sets one (optional)
 *   DRADIS_TIMEOUT   per-request milliseconds, default 15000
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

const BASE = (process.env.DRADIS_API_URL || "").replace(/\/+$/, "");
const API_KEY = process.env.DRADIS_API_KEY || "";
const TIMEOUT = Number(process.env.DRADIS_TIMEOUT || 15000);

if (!BASE) {
  console.error(
    "dradis-mcp: DRADIS_API_URL is not set.\n" +
    "Point it at your Control Tower API, e.g. https://dradis.example.com",
  );
  process.exit(1);
}

/**
 * The single egress point. Hardcoded GET — see the read-only note above.
 * Errors are returned as text rather than thrown so the model sees the reason
 * (unreachable host, 401, 404) and can say something useful instead of failing
 * opaquely.
 */
async function apiGet(path, query = {}) {
  const url = new URL(BASE + path);
  for (const [k, v] of Object.entries(query)) {
    if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, String(v));
  }
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), TIMEOUT);
  try {
    const res = await fetch(url, {
      method: "GET",
      headers: API_KEY ? { "X-API-Key": API_KEY } : {},
      signal: ctl.signal,
    });
    const body = await res.text();
    if (!res.ok) {
      return `DRADIS API returned HTTP ${res.status} for ${path}.` +
        (res.status === 401 ? " Check DRADIS_API_KEY." : "") +
        (body ? `\n${body.slice(0, 500)}` : "");
    }
    // Pass JSON through untouched. Re-shaping it here would silently drift from
    // the API and give the model a stale mental model of the system.
    return body;
  } catch (e) {
    return e.name === "AbortError"
      ? `Request to ${path} timed out after ${TIMEOUT}ms. Is the deployment reachable?`
      : `Could not reach the DRADIS API at ${BASE}: ${e.message}`;
  } finally {
    clearTimeout(timer);
  }
}

const num = (d) => ({ type: "number", description: d });
const str = (d) => ({ type: "string", description: d });
const NO_ARGS = { type: "object", properties: {} };

/**
 * Tool set is deliberately curated rather than one-per-endpoint. The API exposes
 * ~30 GET routes; surfacing all of them degrades tool selection, so these are
 * the ones that answer the questions an operator actually asks.
 */
const TOOLS = [
  {
    name: "get_engine_status",
    description:
      "Overall DRADIS engine state: connected venue, active market, collateral, session P&L, " +
      "and per-strategy activity. Start here when asked how the bot is doing.",
    inputSchema: NO_ARGS,
    call: () => apiGet("/api/status"),
  },
  {
    name: "list_positions",
    description:
      "Currently open positions with entry price, size and unrealised P&L. " +
      "Use scope='pending' for orders not yet confirmed on-chain, 'confirmed' for settled ones.",
    inputSchema: {
      type: "object",
      properties: { scope: str("all (default), pending, or confirmed") },
    },
    call: (a) =>
      apiGet(
        a.scope === "pending" ? "/api/positions/pending"
        : a.scope === "confirmed" ? "/api/positions/confirmed"
        : "/api/positions",
      ),
  },
  {
    name: "list_trades",
    description:
      "Closed trades, newest first: entry/exit price, shares, net P&L, fees, and the exit reason " +
      "(e.g. FairValueTP, CatastrophicSL). The exit reason is usually the key to 'why did this lose?'.",
    inputSchema: {
      type: "object",
      properties: {
        limit: num("How many trades to return (default 50)"),
        strategy: str("Filter to one viper, e.g. FairValueStrategy"),
      },
    },
    call: (a) => apiGet("/api/trades", { limit: a.limit ?? 50, strategy: a.strategy }),
  },
  {
    name: "get_trade_stats",
    description:
      "Aggregate performance: win rate, total and average P&L, fees, broken down by strategy. " +
      "Use this before list_trades when the question is about overall performance.",
    inputSchema: NO_ARGS,
    call: () => apiGet("/api/trades/stats"),
  },
  {
    name: "get_pnl_history",
    description: "Time series of session P&L and collateral, for drawdown and equity-curve questions.",
    inputSchema: { type: "object", properties: { limit: num("Number of snapshots (default 200)") } },
    call: (a) => apiGet("/api/pnl/history", { limit: a.limit ?? 200 }),
  },
  {
    name: "get_viper_status",
    description:
      "Per-strategy status INCLUDING the reason each viper is not currently trading " +
      "(e.g. 'OBI adverse', 'disabled in config', 'no oracle price'). " +
      "This is the tool for 'why isn't it taking any trades?'.",
    inputSchema: NO_ARGS,
    call: () => apiGet("/api/vipers/status"),
  },
  {
    name: "get_raptor_telemetry",
    description:
      "Latest raptor signal values — oracle price, velocity, funding, open interest, CVD, " +
      "institutional pulse, sports and tennis feeds — plus whether each feed is connected. " +
      "A disconnected raptor usually explains a strategy sitting idle.",
    inputSchema: {
      type: "object",
      properties: { asset: str("Asset or feed key, e.g. btc, eth, sports, tennis") },
    },
    call: (a) => apiGet("/api/telemetry", { asset: a.asset }),
  },
  {
    name: "get_telemetry_history",
    description: "Historical raptor signal series for one asset or feed, for trend questions.",
    inputSchema: {
      type: "object",
      properties: {
        asset: str("Asset or feed key, e.g. btc, sports, tennis"),
        limit: num("Number of samples (default 300)"),
      },
      required: ["asset"],
    },
    call: (a) => apiGet("/api/telemetry/history", { asset: a.asset, limit: a.limit ?? 300 }),
  },
  {
    name: "list_squadrons",
    description: "Deployed squadrons with their market, lifecycle state and per-viper budgets.",
    inputSchema: NO_ARGS,
    call: () => apiGet("/api/squadrons"),
  },
  {
    name: "get_config",
    description:
      "Current live DynamicConfig — every tunable strategy and risk parameter. " +
      "Read-only here; changing config is not exposed over MCP.",
    inputSchema: NO_ARGS,
    call: () => apiGet("/api/config"),
  },
  {
    name: "get_llm_actions",
    description:
      "Audit trail of AI advisor activity: proposed config patches, what was applied or rejected, " +
      "by which actor, and any circuit-breaker reverts. Use for 'show pending AI proposals'.",
    inputSchema: { type: "object", properties: { limit: num("Number of entries (default 50)") } },
    call: (a) => apiGet("/api/llm/actions", { limit: a.limit ?? 50 }),
  },
  {
    name: "get_recent_logs",
    description:
      "Recent engine log lines. Useful for diagnosing a specific moment, but prefer " +
      "get_viper_status or get_raptor_telemetry for 'why' questions — they are already summarised.",
    inputSchema: { type: "object", properties: { limit: num("Number of lines (default 100)") } },
    call: (a) => apiGet("/api/logs", { limit: a.limit ?? 100 }),
  },
];

const server = new Server(
  { name: "dradis", version: "0.1.0" },
  { capabilities: { tools: {} } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: TOOLS.map(({ name, description, inputSchema }) => ({ name, description, inputSchema })),
}));

server.setRequestHandler(CallToolRequestSchema, async (req) => {
  const tool = TOOLS.find((t) => t.name === req.params.name);
  if (!tool) {
    return { isError: true, content: [{ type: "text", text: `Unknown tool: ${req.params.name}` }] };
  }
  const text = await tool.call(req.params.arguments ?? {});
  return { content: [{ type: "text", text }] };
});

const transport = new StdioServerTransport();
await server.connect(transport);
// stderr only — stdout is the MCP transport and any stray write corrupts it.
console.error(`dradis-mcp ready · ${TOOLS.length} read-only tools · ${BASE}`);
