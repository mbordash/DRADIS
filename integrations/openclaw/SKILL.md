# Skill: DRADIS Tactical Command

Autonomous supervisor interface for the DRADIS high-frequency prediction market execution engine.
Allows natural language health monitoring, P&L reporting, and dynamic parameter adjustments.

## Configuration
- `DRADIS_API_URL`: Base URL for the engine API (Default: `http://localhost:9000/api`)

## Tools

### 1. check_engine_status
Queries the system health of the execution engine, checking active market states and oracle links.
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/health`
- **Response Format:** JSON containing engine uptime, strategy states, and current oracle signals.

### 2. check_session_pnl
Retrieves the real-time equity curve, session P&L, and recent completed trade counts.
- **Method:** `GET`
- **Endpoint:** `{{DRADIS_API_URL}}/pnl/history`
- **Response Format:** JSON array of equity snapshots.

### 3. patch_dynamic_config
Dynamically adjusts Viper strategy thresholds, entry/exit parameters, or strategy toggles on the fly without an engine restart.
- **Method:** `PATCH`
- **Endpoint:** `{{DRADIS_API_URL}}/config`
- **Payload Type:** `application/json`
- **Parameters:**
    - `updates`: Object containing modified strategy variables (e.g., `{"gboost_obi_adverse_block": -0.15, "enable_basis": false}`)