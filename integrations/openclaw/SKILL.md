---
name: dradis-tactical-command
description: Real-time supervisor and control interface for the DRADIS prediction-market trading engine (Polymarket International, Polymarket US, and Kalshi). Full support for DRADIS_API_KEY authentication.
homepage: https://github.com/mbordash/DRADIS
user-invocable: true
---

# Skill: DRADIS Tactical Command (v1.5.1)

Full-featured autonomous supervisor for the **DRADIS** high-frequency prediction market execution engine.

> **What's new in v1.5.1**
> - **Corrected PATCH contract.** Config PATCH bodies are a *flat* partial object, not a wrapped `updates` object. A wrapped body is silently ignored and still returns `200 OK` — see [Applying config changes](#applying-config-changes) for the mandatory verification step.
> - **Corrected squadron ids.** Ids are `{asset}-{cadence}` (e.g. `btc-hourly`), never a bare asset name. Always read them from `list_squadrons`.
> - **`patch_dynamic_config` no longer affects trading.** Strategy execution reads squadron-scoped config exclusively; the global endpoint now only governs the LLM advisor. Use `patch_squadron_config` for anything that changes trading behaviour.
> - **New read tools:** `get_viper_status` (why isn't it trading?), `get_llm_actions` + approve/reject (the AI action queue), `get_telemetry`, `get_telemetry_history`, `get_telemetry_assets`, `get_logs`, `get_latency`, `get_taxonomy_raptors`, `get_taxonomy_vipers`, `get_available_markets`, `get_deployments`, `get_deployment_region`.
> - Documented `DRADIS_READ_ONLY` (403 on writes), public vs. protected routes, query-param defaults and clamps, and per-venue endpoint availability.

## About DRADIS

DRADIS is an open-source, low-latency Rust trading engine for crypto prediction markets.
It features a Viper strategy engine, real-time equity curve, dynamic config hot-reloading, and a built-in LLM advisor.

**Project repository:** [https://github.com/mbordash/DRADIS](https://github.com/mbordash/DRADIS)

### Venues

DRADIS builds against exactly one venue at a time (mutually exclusive Cargo features):

| Build | Venue | Notes |
|---|---|---|
| `intl_clob` (default) | Polymarket International | Self-custody; on-chain wallet balance available |
| `us_retail` | Polymarket US | Custodial; no on-chain wallet probe |
| `kalshi` | Kalshi | Custodial; no on-chain wallet probe |

The agent cannot tell which build is running from the venue name alone — call `get_deployment_region` to find out. This matters because a few endpoints are build-specific (noted per tool below).

### Architecture the agent should understand

- A **CAG** (Commander Air Group) runs one or more **Squadrons**, one per market.
- Each Squadron owns **Raptors** (signal feeds) and **Vipers** (strategies), plus **its own `DynamicConfig`**.
- **Squadron config is the only config that affects trading.** The global `/api/config` object is a separate, legacy-scope record that now only feeds the LLM advisor.

## Publisher Note (Addressing ClawScan Findings)

ClawScan has flagged two medium-risk items (as expected for any live-trading integration):
- Ability to PATCH live strategy parameters
- Forwarding of a sensitive `DRADIS_API_KEY`

These are intentional and documented. The skill **never** applies config changes without explicit human confirmation.

This skill is **monitoring and configuration only, by design.** It deliberately exposes no tool that opens, closes, or settles a position, and none that deploys a squadron. The engine's API does offer such endpoints (`DELETE /api/positions/{token_id}`, `POST /api/positions/manual-exit`, `POST /api/positions/sync`, `POST /api/squadrons/deploy`) — they are omitted here so that no agent turn can spend money or flatten a position. Perform those actions in the Control Tower UI.

I strongly recommend using a dedicated, least-privilege API key and only running this skill against your own trusted DRADIS instance.

## Authentication

DRADIS supports optional API key authentication via the `X-API-Key` header.

- Set `DRADIS_API_KEY` in your OpenClaw configuration.
- The skill automatically adds the header to every request.
- Local use works without a key; remote/production use strongly recommends it.

**Public vs. protected routes.** `check_engine_status` (`/api/health`) and `list_assets` (`/api/assets`) are intentionally unauthenticated so that container health checks and load balancers can probe them. Every other tool requires the key when `DRADIS_API_KEY` is set. A healthy `/api/health` alongside 401s elsewhere means *the key is wrong*, not that the engine is down — report it that way.

### Error codes the agent must distinguish

| Code | Meaning | What to tell the user |
|---|---|---|
| `401` | Missing/incorrect `X-API-Key` | Configure `DRADIS_API_KEY` to match the engine's env var |
| `403` | `DRADIS_READ_ONLY=true` — the engine rejects **every** non-GET request | This is a read-only/demo instance; config changes are disabled at the engine, not by the skill |
| `404` (squadron routes) | Unknown squadron id | Re-read ids from `list_squadrons`; do not retry with a guessed id |
| `409` (LLM actions) | Action is not in `proposed` state, or failed apply-time revalidation | Re-read `get_llm_actions`; the proposal was already handled or is stale |
| `400` (config PATCH) | Body was not valid JSON, or a value failed type coercion | Show the engine's error text verbatim |

## Safety & Usage Guidelines (Critical)

**This skill controls a live trading system with real money at risk.**
The agent **must** follow these guardrails at all times:

- **Never** apply any configuration change without first explicitly confirming the exact update with the human user and receiving clear approval.
- Always call `get_config_schema` before proposing a patch, to validate the field name, type, unit, and min/max bounds.
- Never construct a squadron id. Read ids from `list_squadrons` and use them verbatim.
- Only call tools when the user's request directly relates to monitoring or configuration.
- When reporting numbers, carry the engine's own caveats through: `prices_live: false` in `get_portfolio_value` means positions are marked with stale prices, and `get_viper_status` rows older than 120 seconds mean the evaluation loop is not reporting.

### Applying config changes

The PATCH body is a **flat JSON object containing only the fields to change**. It is *not* wrapped in an `updates` key.

```jsonc
// CORRECT
{"time_decay_position_size_usdc": "8.0", "enable_basis": false}

// WRONG — silently ignored, still returns 200 OK with the UNCHANGED config
{"updates": {"time_decay_position_size_usdc": "8.0"}}
```

The engine merges the body over the current config and **ignores keys it does not recognise**. A misspelled field, or a wrapped body, therefore produces a successful-looking response in which nothing changed.

**Mandatory verification step:** a `200 OK` does **not** mean the change was applied. Both PATCH endpoints return the full resulting config. After every patch, read back the specific field you changed from the response body and confirm it holds the new value. Report success only if it does; if it does not, tell the user the patch was rejected as an unknown field and did not take effect.

### Which config endpoint to use

| Goal | Tool |
|---|---|
| Change how a strategy trades (size, thresholds, enable/disable a viper) | `patch_squadron_config` |
| Change LLM advisor behaviour or autonomy settings | `patch_dynamic_config` |

`patch_dynamic_config` writes the global config record, which **no squadron reads**. Strategy execution loops read a per-squadron config handle, and newly deployed squadrons seed from compile-time defaults rather than the global record. Patching global config to change trading behaviour will appear to succeed and will change nothing. If the user asks to change a strategy parameter without naming a squadron, list the squadrons and ask which one — do not fall back to the global endpoint.

## Example Natural Language Commands

- "What's the current status of DRADIS?"
- "Why isn't DRADIS trading right now?"
- "Show me open positions and session P&L"
- "What markets is each strategy currently trading?"
- "List the last 10 trades"
- "What does the LLM advisor recommend right now?"
- "Are there any AI config changes waiting for my approval?"
- "Show me the last 200 log lines"
- "What's our current latency to the venue?"
- "List my squadrons and their P&L"
- "Show me the btc-hourly squadron's config"
- "What's my total portfolio value right now?"
- "Show pending vs confirmed positions for ETH"
- "Can you increase the adverse block threshold on btc-hourly? Show me the current value first and confirm before patching."

## Configuration

- `DRADIS_API_URL`: Base URL for the engine API (Default: `http://localhost:9000/api`)
- `DRADIS_API_KEY`: API key for authentication (optional locally, **recommended** for remote/production)

---

## Tools

All tools send `X-API-Key: {{DRADIS_API_KEY}}`.

### Engine health & inventory

### 1. check_engine_status
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/health`
- **Response:** the literal text `ok`
- **Note:** Public — succeeds without an API key. Use it to separate "engine down" from "key wrong".

### 2. list_assets
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/assets`
- **Response:** JSON array of asset symbols with an initialised database, e.g. `["btc", "eth", "sol"]`
- **Note:** Public. Includes venue-only databases (e.g. `"kalshi"`) that carry no raptor signal data; `get_telemetry_assets` returns the narrower set that does.

### 3. get_deployment_region
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/deployment/region`
- **Response:** `{"region": "intl" | "us" | "kalshi", "available_types": ["politics", "sports", "crypto"]}`
- **Note:** Call this first when a venue-specific behaviour matters.

### 4. get_strategy_status
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/status`
- **Response:** `{"strategy_markets": {…}, "session_started_at": "…", "raptors": {…}}` — the strategy→market mapping, the current session id, and per-asset raptor connection health.

### 5. get_viper_status
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/vipers/status`
- **Query Param:** `asset` (optional, e.g. `btc`; omit for all squadrons)
- **Response:** JSON array of `{asset, strategy, last_eval_at, last_eval_secs_ago, last_outcome, last_reason, last_reason_secs_ago, last_signal_at, last_signal_secs_ago}`
- **Note:** **This is the correct tool for "why isn't DRADIS trading?"** `last_outcome` is one of `signal | no_signal | error | timeout`. `last_reason` is the named gate that vetoed the most recent entry attempt ("edge below required", "cooldown active"); only instrumented vipers populate it, so `null` means "reports liveness only", not "no veto". Interpretation rules:
  - All vipers evaluating recently with named reasons → the engine is healthy and correctly sitting out. Say so; do not imply a fault.
  - `last_eval_secs_ago > 120` → the evaluation loop is not reporting. Flag as a probable fault.
  - `last_outcome` of `error` or `timeout` → flag as a fault regardless of age.
  - The registry is in-memory and empty after a restart, so an absent viper means "not yet evaluated", not "never trades".

### Positions & performance

### 6. get_open_positions
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/positions`
- **Query Param:** `asset` (optional, e.g. `btc`)
- **Response:** JSON array of all open position records (pending + confirmed)

### 7. get_pending_positions
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/positions/pending`
- **Query Param:** `asset` (optional)
- **Response:** JSON array of positions placed but not yet confirmed on-chain

### 8. get_confirmed_positions
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/positions/confirmed`
- **Query Param:** `asset` (optional)
- **Response:** JSON array of confirmed open positions

### 9. get_portfolio_value
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/portfolio`
- **Response:** `{"collateral", "positions_value", "total_value", "unrealized_pnl", "position_count", "prices_live"}` (decimal values as strings)
- **Note:** On `intl` builds, `collateral` is probed live on-chain. On `us` and `kalshi` builds there is no wallet probe, so it falls back to the database-tracked snapshot. If `prices_live` is `false`, positions are marked at stale prices — say so when quoting `total_value` or `unrealized_pnl`.

### 10. check_session_pnl
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/pnl/history`
- **Query Params:** `asset` (optional), `limit` (optional, default 200, clamped 1–1000)
- **Response:** JSON array of equity snapshots
- **Note:** Omitting `asset` returns the **aggregated** curve across all assets, not the primary asset's. Pass `asset` explicitly whenever the user asks about one asset.

### 11. get_recent_trades
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/trades`
- **Query Params:** `asset` (optional), `limit` (optional, default 100, clamped 1–500)
- **Note:** A `limit` above 500 is silently clamped; never claim more trades were returned than the array contains.

### Squadrons

### 12. list_squadrons
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/squadrons`
- **Response:** JSON array of squadron summaries (id, asset, market class, state, market names, active raptors/vipers)
- **Note:** **The only valid source of squadron ids.** Ids are `{asset}-{cadence}`, e.g. `btc-hourly`, `btc-open`. Call this before any other squadron tool.

### 13. get_squadron
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/squadrons/{id}`
- **Path Param:** `id` — a squadron id from `list_squadrons`, e.g. `btc-hourly`
- **Response:** JSON squadron summary, or 404 if unknown

### 14. get_squadron_config
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/squadrons/{id}/config`
- **Path Param:** `id`, e.g. `btc-hourly`
- **Response:** flat JSON object of the squadron's `DynamicConfig`, or 404 if no config row exists for that id
- **Note:** A 404 here means the id is wrong or the squadron has never been deployed — not that config is unavailable.

### 15. patch_squadron_config
- **Method:** `PATCH`
- **Endpoint:** `{{DRADIS_API_URL}}/squadrons/{id}/config`
- **Path Param:** `id`, e.g. `btc-hourly`
- **Payload Type:** `application/json`
- **Body:** a **flat** partial object of only the fields to change, e.g. `{"time_decay_position_size_usdc": "8.0"}`
- **Response:** the full resulting config
- **Safety Note:** Requires explicit user confirmation before executing. Validate field names and bounds with `get_config_schema` first. **Unknown keys are ignored and still return 200** — read the changed field back out of the response and confirm it before reporting success. Applied changes reach the patrol loop on its next tick.
- **This is the tool that changes trading behaviour.**

### Global config (LLM advisor scope)

### 16. get_current_config
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/config`
- **Response:** flat JSON object of the global `DynamicConfig`
- **Note:** This is the global record, not any squadron's live config. To report what a strategy is actually running, use `get_squadron_config`.

### 17. patch_dynamic_config
- **Method:** `PATCH`
- **Endpoint:** `{{DRADIS_API_URL}}/config`
- **Payload Type:** `application/json`
- **Body:** a **flat** partial object, e.g. `{"llm_autonomy_tier": 1}`
- **Response:** the full resulting config
- **Safety Note:** Requires explicit user confirmation before executing. Same unknown-key caveat and same mandatory read-back as `patch_squadron_config`.
- **Scope warning:** This affects the LLM advisor only. **No squadron reads this record**, so it will not change how any strategy trades. Do not use it as a fallback when a squadron id is unknown — ask the user which squadron instead.

### 18. get_config_schema
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/config/schema`
- **Response:** JSON array describing every editable field: `{key, group, enable_key, label, type, unit, min, max, step, advanced}`
- **Note:** `key` is exactly what a PATCH body expects. `type` is one of `usd | price | pct | decimal | secs | bool`. Use before any PATCH to validate the field name and bounds.

### LLM advisor

### 19. get_llm_recommendations
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/llm/recommendations`
- **Query Params:** `asset` (optional), `limit` (optional, default 10, clamped 1–50)

### 20. get_llm_actions
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/llm/actions`
- **Query Param:** `limit` (optional, default 100, clamped 1–500)
- **Response:** JSON array of the AI action audit trail, newest first: `{id, batch_id, ts, expires_at, model, tier, ghost_mode, field, from_value, to_value, clamped, delta_pct, reason, status, status_detail, …}`
- **Note:** `status` is one of `proposed | approved | applied | rejected | expired | reverted | failed`. Rows with `status: "proposed"` are awaiting human approval. Depending on the configured autonomy tier the engine may have **already applied** changes on its own (`status: "applied"`) — surface those when the user asks what changed. Reads the primary database only; the `asset` param is ignored here.

### 21. approve_llm_action
- **Method:** `POST`
- **Endpoint:** `{{DRADIS_API_URL}}/llm/actions/{id}/approve`
- **Path Param:** `id` — the numeric action id from `get_llm_actions`
- **Safety Note:** This applies a live config change. Requires explicit user confirmation, quoting the field, the from→to values, and the LLM's stated reason. The engine revalidates the proposal against the *current* config at approval time and returns `409` if it is no longer valid — report that as "the proposal went stale", not as an error to retry.
- **Scope warning:** Approval applies through the global config path, which carries the same scope limitation as `patch_dynamic_config`.

### 22. reject_llm_action
- **Method:** `POST`
- **Endpoint:** `{{DRADIS_API_URL}}/llm/actions/{id}/reject`
- **Path Param:** `id`
- **Note:** Only `proposed` actions can be rejected (`409` otherwise). Rejections feed the model's negative-example corpus.

### Telemetry & diagnostics

### 23. get_telemetry
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/telemetry`
- **Response:** live raptor signal snapshot keyed by asset — connection flags plus `oracle_price`, `velocity_5s`, `velocity_1s`, `acceleration`, `drift_60m`, `drift_10m`, `funding_rate`

### 24. get_telemetry_history
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/telemetry/history`
- **Query Params:** `asset` (optional, defaults to the primary asset), `limit` (optional, defaults to the full retained window ≈ 1 hour)
- **Response:** JSON array of telemetry samples, oldest → newest

### 25. get_telemetry_assets
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/telemetry/assets`
- **Response:** JSON array of assets that actually have raptor telemetry — a narrower set than `list_assets`

### 26. get_logs
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/logs`
- **Query Param:** `tail` (optional, default 500, clamped 1–2000)
- **Response:** `{"count": n, "lines": [...]}` from the in-memory ring buffer, oldest first
- **Note:** Log lines print in US/Eastern. They may contain market names and order details; quote only what is relevant.

### 27. get_latency
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/latency`
- **Response:** rolling round-trip latency from the engine host to the trading venue

### Taxonomy & deployment inventory (read-only)

### 28. get_taxonomy_raptors
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/taxonomy/raptors`
- **Query Param:** `market_class` (**required**, e.g. `crypto`, `sports`, `politics`)
- **Response:** JSON array of `{id, display, implemented}`

### 29. get_taxonomy_vipers
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/taxonomy/vipers`
- **Query Param:** `market_class` (**required**)
- **Response:** JSON array of `{id, display, venue_agnostic}`

### 30. get_available_markets
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/markets/available`
- **Query Params:** `market_type` (**required**: `crypto` | `sports` | `politics`), `expiry_window` (optional: `1h` | `4h` | `24h` | `7d`; default varies by type — 24h sports, 30d crypto, 90d politics), `min_liquidity` (optional, default 500)
- **Response:** `{"markets": [...]}` — candidate markets for deployment
- **Note:** Informational only. This skill exposes no deployment tool; deploy from the Control Tower UI.

### 31. get_deployments
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/deployments`
- **Response:** JSON array of deployment requests with status: `{id, market_id, market_type, raptors, vipers, status, squadron_id, error, created_at}`

---

## Endpoints deliberately NOT exposed

These exist on the engine API and are omitted on purpose. If a user asks for them, explain that the skill is monitoring-and-configuration only and direct them to the Control Tower UI.

| Endpoint | Why omitted |
|---|---|
| `DELETE /api/positions/{token_id}` | Removes a tracked position |
| `POST /api/positions/manual-exit` | Places a real exit order (`intl` builds only) |
| `POST /api/positions/sync` | Mutates position records from chain state (`intl` builds only) |
| `POST /api/squadrons/deploy` | Commits capital to a new market |
| `/api/setup/*` (GET/PUT/POST), `POST /api/auth/login` | Reads and writes venue credentials and admin tokens |
| `GET /api/trades/export` | Bulk CSV of the full tradelog; download from the UI instead |
