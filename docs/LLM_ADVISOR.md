# LLM Advisor

Optional background task. Every `LLM_ADVISOR_INTERVAL_SECS` (default: 30 min) it fetches recent trades from SQLite, analyzes them with an LLM, and posts plain-English optimization recommendations to Telegram. Defaults to a local Ollama model; remote providers are supported via env vars.

```rust
// src/config.rs
pub const ENABLE_LLM_ADVISOR: bool = true;
pub const LLM_ADVISOR_INTERVAL_SECS: u64 = 1800;
pub const LLM_ADVISOR_TRADES_LOOKBACK: i64 = 20;
pub const LLM_PROVIDER: &str = "ollama";
pub const LLM_OLLAMA_URL: &str = "http://localhost:11434";
pub const LLM_OLLAMA_MODEL: &str = "llama3.2";
```

```bash
# Ollama (default) — override at runtime without rebuilding
OLLAMA_URL=http://192.168.1.10:11434
OLLAMA_MODEL=mistral

# Any OpenAI-compatible provider (OpenAI, Groq, Together, OpenRouter, vLLM, LM Studio…)
LLM_PROVIDER=openai
LLM_API_BASE=https://api.openai.com/v1   # optional; default shown
LLM_API_KEY=sk-...                       # env only — never persisted or logged
LLM_MODEL=gpt-4o-mini

# Anthropic
LLM_PROVIDER=anthropic
LLM_API_KEY=sk-ant-...
LLM_MODEL=claude-haiku-4-5
```

### AI-Authored Config Patches (Autonomy Tiers)

Beyond prose recommendations, the advisor appends a machine-readable block of
proposed `DynamicConfig` changes to each analysis. Every proposal is validated
against the config schema (exact keys, type coercion, min/max clamping, max 4
per cycle), recorded in the `llm_actions` audit table, and TTL-bound (30 min —
stale advice never applies). What happens next depends on the autonomy tier,
set live from **Setup → AI Autonomy** (no restart):

| Tier | Name | Behavior |
|------|------|-----------|
| 1 | **Recommend** | Nothing auto-applies. Proposals queue in the dashboard; a human presses **apply** (revalidated against the *current* config first) or **reject**. |
| 2 | **Limited** | Safe changes apply immediately: schema-clamped, per-field delta capped (±20% default), rate-limited (1 batch/hour default), **never money fields** (`*_usdc`, budgets, collateral). Everything else queues for approval. |
| 3 | **Autonomous** | The AI applies its changes directly (still schema-clamped; `ghost_mode` flips always require a human). |

Guardrails at every tier:

- **Kill switch** — one click in Setup (or `LLM_AUTONOMY_KILL=1`) forces recommend-only.
- **Circuit breaker** — if session P&L draws down past `LLM_BREAKER_DRAWDOWN_USDC`
  (default $25) within the watch window after an auto-apply, all recent AI changes
  are reverted via their stored inverse patches, autonomy is demoted to tier 1
  until an operator resets it, and a Telegram alert fires.
- **Full audit trail** — the **AI Actions** tab shows every proposal's lifecycle
  (`proposed → applied / rejected / expired / reverted / failed`), and each
  applied change is outcome-scored (session-P&L delta after
  `LLM_OUTCOME_HORIZON_SECS`, default 2h). Rejections, reverts, and scores are
  injected back into future prompts as few-shot examples, so the model learns
  from its own track record.

All tiers run identically in LIVE and GHOST mode (each action is stamped with
the mode), so you can audition tier 2/3 behavior risk-free in ghost first.

```bash
# Autonomy knobs (.env — all optional, shown with defaults)
LLM_AUTONOMY_TIER=1              # 1 recommend / 2 limited / 3 autonomous
LLM_AUTONOMY_KILL=0              # 1 = hard stop for all auto-applies
LLM_MAX_PATCHES_PER_HOUR=1       # tier-2 rate limit (applied batches/hour)
LLM_MAX_DELTA_PCT=0.20           # tier-2 per-field relative delta cap
LLM_BREAKER_DRAWDOWN_USDC=25.0   # P&L drawdown that trips the breaker
LLM_BREAKER_WINDOW_SECS=14400    # how far back applied changes are watched
LLM_OUTCOME_HORIZON_SECS=7200    # when applied changes get outcome-scored
```
