---
name: dradis-tactical-command
description: Real-time supervisor and control interface for the DRADIS Polymarket high-frequency trading engine. Monitor health, positions, P&L, trades, config, strategy markets, and LLM recommendations. Dynamically adjust parameters via natural language.
user-invocable: true
---

# Skill: DRADIS Tactical Command

Full-featured autonomous supervisor for the DRADIS high-frequency prediction market execution engine.

## Safety & Usage Guidelines (Critical)

**This skill controls a live trading system with real money at risk.**  
The agent **must** follow these guardrails at all times:

- `patch_dynamic_config` changes live strategy parameters without restarting the engine.  
  **Never** apply any configuration change without first explicitly confirming the exact update with the human user and receiving clear approval (e.g., “Do you want me to set gboost_obi_adverse_block to -0.15? Yes/No”).
- Only call tools when the user’s request directly relates to monitoring status, checking P&L/trades/positions, viewing config, or requesting a specific parameter adjustment.
- Respect DRADIS’s built-in circuit breakers, rate limits, and safety mechanisms — the engine will reject unsafe changes.
- If the engine returns an error or unhealthy state, immediately report it and **do not** attempt further config changes until the user is informed.
- The agent should be conservative: prefer status/monitoring tools over config changes.

## Example Natural Language Commands

The skill is designed to respond naturally to commands like:

- “What’s the current status of DRADIS?”
- “Show me open positions and session P&L”
- “What markets is each strategy currently trading?”
- “List the last 10 trades”
- “What does the LLM advisor recommend right now?”
- “Show me the current config”
- “Can you increase the adverse block threshold? Show me the current config first and confirm before patching.”
- “Patch the config to disable basis trading and confirm with me”

## Configuration

- `DRADIS_API_URL`: Base URL for the engine API (Default: `http://localhost:9000/api`)

## Tools

### 1. check_engine_status
Queries overall engine health/liveness.

- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/health`
- **Response Format:** Plain text `"ok"` (or error)

### 2. get_current_config
Returns the full current dynamic configuration (all Viper parameters).

- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/config`
- **Response Format:** JSON object of all configurable fields

### 3. patch_dynamic_config
Dynamically updates Viper strategy parameters on the fly without an engine restart.

- **Method:** `PATCH`
- **Endpoint:** `{{DRADIS_API_URL}}/config`
- **Payload Type:** `application/json`
- **Parameters:**  
  `updates`: Object containing modified strategy variables (e.g. `{"gboost_obi_adverse_block": -0.15, "enable_basis": false}`)
- **Safety Note:** This tool **requires explicit user confirmation** before sending the PATCH request. The agent must summarize the exact change and wait for approval.

### 4. check_session_pnl
Retrieves the real-time equity curve and session P&L snapshots.

- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/pnl/history`
- **Response Format:** JSON array of objects (`{ "ts": number, "session_pnl": number, "collateral": number }`)

### 5. get_recent_trades
Returns a list of recently completed trades.

- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/trades`
- **Query Param:** `limit` (optional, default 100, clamped 1–500)
- **Response Format:** JSON array of trade objects

### 6. get_open_positions
Returns all currently open positions across strategies.

- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/positions`
- **Response Format:** JSON array of open position records

### 7. get_strategy_status
Shows which market each strategy is currently attached to.

- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/status`
- **Response Format:** JSON object `{ "strategy_markets": { "strategy_name": "market_name", ... } }`

### 8. get_llm_recommendations
Returns recent recommendations and analyses from the built-in LLM Advisor.

- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/llm/recommendations`
- **Query Param:** `limit` (optional, default 10, clamped 1–50)
- **Response Format:** JSON array of recommendation objects