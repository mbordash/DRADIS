---
name: dradis-tactical-command
description: Real-time supervisor and control interface for the DRADIS Polymarket high-frequency trading engine. Full support for DRADIS_API_KEY authentication via X-API-Key header.
user-invocable: true
---

# Skill: DRADIS Tactical Command (v1.1.0)

Full-featured autonomous supervisor for the DRADIS high-frequency prediction market execution engine.

## Authentication (New in v1.1.0)

DRADIS now supports optional API key authentication (see your README).

- Set `DRADIS_API_KEY` in your OpenClaw configuration.
- The skill **automatically** adds the header `X-API-Key: {{DRADIS_API_KEY}}` to **every** request.
- Local development works without a key. Remote or production deployments **should** use the key.

## Safety & Usage Guidelines (Critical)

**This skill controls a live trading system with real money at risk.**  
The agent **must** follow these guardrails at all times:

- `patch_dynamic_config` changes live strategy parameters without restarting the engine.  
  **Never** apply any configuration change without first explicitly confirming the exact update with the human user and receiving clear approval.
- Only call tools when the user’s request directly relates to monitoring or configuration.
- If the engine returns 401 Unauthorized, immediately tell the user they need to configure the `DRADIS_API_KEY`.

## Example Natural Language Commands

- “What’s the current status of DRADIS?”
- “Show me open positions and session P&L”
- “What markets is each strategy currently trading?”
- “List the last 10 trades”
- “What does the LLM advisor recommend right now?”
- “Show me the current config”
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
- **Query Param:** `limit` (optional, default 100)

### 6. get_open_positions
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/positions`
- **Headers:** `X-API-Key: {{DRADIS_API_KEY}}`
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