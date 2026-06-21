---
name: dradis-tactical-command
description: Real-time supervisor and control interface for the DRADIS Polymarket high-frequency trading engine. Full support for DRADIS_API_KEY authentication.
homepage: https://github.com/mbordash/DRADIS
user-invocable: true
---

# Skill: DRADIS Tactical Command (v1.4.0)

Full-featured autonomous supervisor for the **DRADIS** high-frequency prediction market execution engine.

> **What's new in v1.4.0**
> - Per-squadron monitoring & control: `list_squadrons`, `get_squadron`, `get_squadron_config`, `patch_squadron_config` (matches the CAG → Squadron architecture; the global `/config` is now the coarse fallback).
> - Config introspection: `get_config_schema` exposes the field schema so the agent can validate a parameter before patching.
> - Portfolio & position lifecycle: `get_portfolio_value`, `get_pending_positions`, `get_confirmed_positions`, `list_assets`.

## About DRADIS

DRADIS is an open-source, low-latency Rust-based trading engine for Polymarket.  
It features a Viper strategy engine, real-time equity curve, dynamic config hot-reloading, and a built-in LLM advisor.

**Project repository:** [https://github.com/mbordash/DRADIS](https://github.com/mbordash/DRADIS)

## Publisher Note (Addressing ClawScan Findings)

ClawScan has flagged two medium-risk items (as expected for any live-trading integration):
- Ability to PATCH live strategy parameters
- Forwarding of a sensitive `DRADIS_API_KEY`

These are intentional and documented. The skill **never** applies config changes without explicit human confirmation. I strongly recommend using a dedicated, least-privilege API key and only running this skill against your own trusted DRADIS instance.

## Authentication

DRADIS supports optional API key authentication via the `X-API-Key` header.

- Set `DRADIS_API_KEY` in your OpenClaw configuration.
- The skill automatically adds the header to every request.
- Local use works without a key; remote/production use strongly recommends it.

## Safety & Usage Guidelines (Critical)

**This skill controls a live trading system with real money at risk.**  
The agent **must** follow these guardrails at all times:

- `patch_dynamic_config` and `patch_squadron_config` change live strategy parameters without restarting the engine.
  **Never** apply any configuration change without first explicitly confirming the exact update with the human user and receiving clear approval. Prefer `get_config_schema` to validate field names, units, and min/max bounds before proposing a patch.
- Only call tools when the user’s request directly relates to monitoring or configuration.
- If the engine returns 401 Unauthorized, tell the user to configure the `DRADIS_API_KEY`.

## Example Natural Language Commands

- “What’s the current status of DRADIS?”
- “Show me open positions and session P&L”
- “What markets is each strategy currently trading?”
- “List the last 10 trades”
- “What does the LLM advisor recommend right now?”
- “Show me the current config”
- “List my squadrons and their P&L”
- “Show me the BTC squadron’s config”
- “What’s my total portfolio value right now?”
- “Show pending vs confirmed positions for ETH”
- “Can you increase the adverse block threshold? Show me the current config first and confirm before patching.”

## Configuration

- `DRADIS_API_URL`: Base URL for the engine API (Default: `http://localhost:9000/api`)
- `DRADIS_API_KEY`: API key for authentication (optional locally, **recommended** for remote/production)

## Tools

### 1. check_engine_status
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/health`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Response Format:** Text or JSON

### 2. get_current_config
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/config`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Response Format:** JSON object of all configurable fields

### 3. patch_dynamic_config
- **Method:** `PATCH`
- **Endpoint:** `{{DRADIS_API_URL}}/config`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Payload Type:** `application/json`
- **Parameters:** `updates` object (e.g. `{"gboost_obi_adverse_block": -0.15, "enable_basis": false}`)
- **Safety Note:** Requires explicit user confirmation before executing.

### 4. check_session_pnl
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/pnl/history`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Response Format:** JSON array of equity snapshots

### 5. get_recent_trades
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/trades`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Query Params:** `asset` (optional, e.g. `btc`), `limit` (optional, default 100)

### 6. get_open_positions
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/positions`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Query Param:** `asset` (optional, e.g. `btc`)
- **Response Format:** JSON array of open position records

### 7. get_strategy_status
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/status`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Response Format:** JSON object of strategy → market mapping

### 8. get_llm_recommendations
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/llm/recommendations`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Query Param:** `limit` (optional, default 10)

### 9. list_squadrons
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/squadrons`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Response Format:** JSON array of squadron summaries (id, asset/market class, P&L, active raptors/vipers)

### 10. get_squadron
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/squadrons/{id}`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Path Param:** `id` (squadron identifier, e.g. `btc`)
- **Response Format:** JSON squadron summary, or 404 if unknown

### 11. get_squadron_config
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/squadrons/{id}/config`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Path Param:** `id` (squadron identifier)
- **Response Format:** JSON object of the squadron's DynamicConfig, or 404 if unknown

### 12. patch_squadron_config
- **Method:** `PATCH`
- **Endpoint:** `{{DRADIS_API_URL}}/squadrons/{id}/config`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Path Param:** `id` (squadron identifier)
- **Payload Type:** `application/json`
- **Parameters:** partial `updates` object with only the fields to change (e.g. `{"time_decay_position_size_usdc": "8.0"}`)
- **Safety Note:** Requires explicit user confirmation before executing. Validate field names/bounds with `get_config_schema` first.

### 13. get_config_schema
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/config/schema`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Response Format:** JSON schema describing every editable config field (group, label, type, unit, min/max, advanced flag). Use before any PATCH to validate the change.

### 14. get_portfolio_value
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/portfolio`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Response Format:** JSON object with total portfolio / collateral value

### 15. get_pending_positions
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/positions/pending`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Query Param:** `asset` (optional, e.g. `btc`)
- **Response Format:** JSON array of positions awaiting on-chain confirmation

### 16. get_confirmed_positions
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/positions/confirmed`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Query Param:** `asset` (optional, e.g. `btc`)
- **Response Format:** JSON array of confirmed open positions

### 17. list_assets
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/assets`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
- **Response Format:** JSON array of asset symbols, e.g. `["btc", "eth", "sol"]`

