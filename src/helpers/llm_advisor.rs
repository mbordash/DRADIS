// SPDX-License-Identifier: AGPL-3.0-only
//
// DRADIS — autonomous trading engine for crypto prediction markets.
// Copyright (C) 2026 Michael Bordash
//
// This file is part of DRADIS. DRADIS is free software: you can redistribute it
// and/or modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.
//
// DRADIS is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
// A PARTICULAR PURPOSE. See the GNU Affero General Public License for details.
//
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

/// LLM Advisor — periodic trade analysis with pluggable LLM providers + Telegram recommendations.
///
/// ── Overview ─────────────────────────────────────────────────────────────────
/// A background task that wakes every LLM_ADVISOR_INTERVAL_SECS and analyzes
/// trades in two tiers:
///
///   Tier 1 — CURRENT SESSION (primary)
///     All trades since this process started, sharing the same market conditions,
///     starting collateral, and active DynamicConfig.  Always used when ≥ 5 trades
///     are available.  This ensures the LLM gives contextually coherent advice
///     rather than blending patterns from yesterday's volatile session with today's
///     calm session.
///
///   Tier 2 — PRIOR SESSIONS (supplemental context)
///     When the current session has fewer than LLM_ADVISOR_MIN_SESSION_TRADES trades,
///     recent trades from previous sessions are appended as a separate clearly-labeled
///     section.  This prevents the advisor from firing vacuous advice on session start
///     while still surfacing persistent cross-session patterns.
///
/// ── Session model ────────────────────────────────────────────────────────────
/// Every process restart is a new session (stamped by db::init_session()).
/// LLM recommendations are tagged with their session_id so the Control Tower
/// can visually distinguish current-session advice from stale prior-session advice.
/// Old recommendations are NOT deleted — they are a learning record — but they
/// carry `is_current_session: false` in the API response so the UI can grey them out.
///
/// ── Configuration ────────────────────────────────────────────────────────────
///   config.rs:       ENABLE_LLM_ADVISOR, LLM_ADVISOR_INTERVAL_SECS,
///                    LLM_ADVISOR_TRADES_LOOKBACK, LLM_PROVIDER,
///                    LLM_OLLAMA_URL, LLM_OLLAMA_MODEL
///   env overrides:   LLM_PROVIDER=ollama|openai|anthropic (default ollama)
///                    ollama:    OLLAMA_URL, OLLAMA_MODEL
///                    openai:    LLM_API_BASE, LLM_API_KEY, LLM_MODEL
///                    anthropic: LLM_API_BASE, LLM_API_KEY, LLM_MODEL
///   Telegram creds:  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID  (same as rest of bot)
///
/// ── Bring Your Own LLM ───────────────────────────────────────────────────────
/// Default provider is Ollama's /api/chat — any local model works (recommended:
/// llama3.2, mistral, qwen2.5); point OLLAMA_URL at a remote host for GPU
/// inference.  Alternatively set LLM_PROVIDER=openai for any OpenAI-compatible
/// endpoint (OpenAI, Groq, Together, OpenRouter, vLLM, LM Studio…) or
/// LLM_PROVIDER=anthropic for Claude.  API keys live in the environment only —
/// never persisted to DB/config and never logged.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

use crate::config;
use crate::helpers::{db, dynamic_config::DynamicConfig, notifications};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Minimum number of current-session trades before the LLM Advisor fires its
/// first analysis for that session.  Below this threshold the analysis would be
/// too thin to be useful, so the advisor waits for more data.
const LLM_ADVISOR_MIN_SESSION_TRADES: usize = 5;

/// When the current session has fewer than LLM_ADVISOR_MIN_SESSION_TRADES trades,
/// supplement with this many trades from prior sessions as context.
/// Kept low (5) to avoid prompt bloat — a 3b CPU model struggles past ~1500 input tokens.
const LLM_ADVISOR_PRIOR_SESSION_SUPPLEMENT: i64 = 5;

/// How long a `proposed` config change stays actionable before auto-expiring.
/// Markets move fast — approving an hour-old proposal applies advice computed
/// against conditions that no longer exist. One advisory interval is the ceiling.
const LLM_PROPOSAL_TTL_SECS: i64 = 1800;


/// The newest `pnl_snapshots` row across every registered shard.
///
/// A venue may spread its snapshots over several shards (Polymarket US writes
/// one per wing) and may also carry a vestigial shard that nothing writes. Asking
/// the primary shard alone therefore answers "no data" on a venue that is busily
/// recording it. Comparing timestamps across all of them is the only reading that
/// survives both arrangements.
async fn newest_pnl_snapshot_across_shards() -> Option<db::PnlSnapshotRow> {
    let mut newest: Option<db::PnlSnapshotRow> = None;
    for asset in db::available_assets() {
        let Some(pool) = db::pool_for(&asset) else { continue };
        let Some(row) = db::get_pnl_history(&pool, 1).await.into_iter().next() else { continue };
        let replace = match &newest {
            None => true,
            Some(best) => row.ts > best.ts,
        };
        if replace {
            newest = Some(row);
        }
    }
    newest
}

/// Autonomy tier stamped on proposals (1 = human approval, 2 = limited
/// auto-apply, 3 = autonomous). Env placeholder until the Setup-view control
/// ships (Epic S5); the policy engine (S3) is the sole enforcement point.
fn autonomy_tier() -> i64 {
    std::env::var("LLM_AUTONOMY_TIER")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|t| (1..=3).contains(t))
        .unwrap_or(1)
}

/// How long after an apply/revert an action's outcome is scored (env
/// `LLM_OUTCOME_HORIZON_SECS`, default 2h). The score is the session-P&L
/// delta over that window — crude but honest, and it feeds the few-shot
/// track record injected into future prompts.
fn outcome_horizon_secs() -> i64 {
    std::env::var("LLM_OUTCOME_HORIZON_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|s| *s >= 60)
        .unwrap_or(7200)
}

/// Few-shot examples injected into the prompt (rejections, reverts, scored
/// applies). Kept small — each line costs prompt budget on local models.
const LLM_FEWSHOT_EXAMPLES: i64 = 6;

// ── Ollama API types ──────────────────────────────────────────────────────────


/// Truncate to at most `max_bytes`, stopping on a character boundary.
///
/// `&s[..n]` panics outright when byte `n` lands inside a multi-byte character,
/// and every string these call sites slice comes from outside the process:
/// market questions carry accented player and candidate names, and model prose
/// is full of em dashes and quotation marks. A tennis market whose name
/// straddled byte 32 panicked the advisor task, which then took its whole
/// analysis cycle down with it — a display-only truncation killing a background
/// worker.
fn truncate_on_char_boundary(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[derive(Serialize)]
struct OllamaRequest {
    model: String,
    messages: Vec<ChatMessage>,
    stream: bool,
    /// Optional generation parameters — keep output focused and concise.
    options: OllamaOptions,
}

#[derive(Serialize)]
struct OllamaOptions {
    /// Allow fuller recommendations to be persisted to SQLite and shown in Control Tower.
    /// Telegram truncation is handled separately after generation.
    num_predict: u32,
    temperature: f32,
    /// Cap the KV-cache context window.  Smaller = faster prefill on CPU.
    /// 3072 leaves room for the prompt plus a longer recommendation.
    num_ctx: u32,
}

#[derive(Serialize, Deserialize, Clone)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct OllamaResponse {
    message: ChatMessage,
    done_reason: Option<String>,
}

// ── Provider abstraction ──────────────────────────────────────────────────────

/// Where advisor inference runs. Resolved once at startup from `LLM_PROVIDER`
/// (env) falling back to `config::LLM_PROVIDER`. API keys are read from the
/// environment only and are never logged, persisted, or echoed to Telegram.
#[derive(Clone)]
enum LlmProvider {
    /// Local or remote Ollama (`/api/chat`). No API key.
    Ollama { base_url: String, model: String },
    /// Any OpenAI-compatible endpoint (`{base}/chat/completions`):
    /// OpenAI, Groq, Together, OpenRouter, vLLM, LM Studio…
    OpenAiCompatible { base_url: String, api_key: String, model: String },
    /// Anthropic Messages API (`{base}/v1/messages`).
    Anthropic { base_url: String, api_key: String, model: String },
}

/// Uniform inference result across providers.
struct LlmReply {
    content: String,
    /// True when the provider reports the output was cut by the token cap.
    truncated_by_length: bool,
}

/// Normalize an LLM API base URL.
///
/// Operators paste the endpoint from the provider's documentation — the
/// Anthropic docs say `https://api.anthropic.com/v1/messages` — but this code
/// appends the path itself, producing `.../v1/messages/v1/messages` and a
/// bewildering 404 that names neither the model nor the URL. Strip the path
/// back off rather than requiring the operator to know which half we want.
///
/// Deliberately provider-specific and suffix-only: OpenAI-compatible gateways
/// are routinely hosted under a prefix, so nothing is ever appended and no
/// path is removed that the caller does not re-add.
pub fn normalize_llm_base(base: &str, provider: &str) -> String {
    let mut b = base.trim().trim_end_matches('/');
    match provider.trim().to_lowercase().as_str() {
        "anthropic" => {
            // Caller appends "/v1/messages".
            for suffix in ["/v1/messages", "/messages"] {
                if let Some(rest) = b.strip_suffix(suffix) { b = rest.trim_end_matches('/'); break; }
            }
            b = b.strip_suffix("/v1").unwrap_or(b).trim_end_matches('/');
        }
        _ => {
            // Caller appends "/models"; the base keeps its "/v1".
            for suffix in ["/chat/completions", "/completions", "/responses", "/models"] {
                if let Some(rest) = b.strip_suffix(suffix) { b = rest.trim_end_matches('/'); break; }
            }
        }
    }
    b.to_string()
}

impl LlmProvider {
    /// Resolve provider + connection settings from the environment.
    ///
    /// Env surface:
    ///   LLM_PROVIDER = ollama | openai | anthropic   (default: config::LLM_PROVIDER)
    ///   ollama:    OLLAMA_URL, OLLAMA_MODEL          (existing vars, unchanged)
    ///   openai:    LLM_API_BASE (default https://api.openai.com/v1),
    ///              LLM_API_KEY (required), LLM_MODEL (required)
    ///   anthropic: LLM_API_BASE (default https://api.anthropic.com),
    ///              LLM_API_KEY (required), LLM_MODEL (required)
    fn from_env() -> anyhow::Result<Self> {
        let provider = std::env::var("LLM_PROVIDER")
            .unwrap_or_else(|_| config::LLM_PROVIDER.to_string())
            .trim()
            .to_ascii_lowercase();
        match provider.as_str() {
            "" | "ollama" => Ok(Self::Ollama {
                base_url: std::env::var("OLLAMA_URL")
                    .unwrap_or_else(|_| config::LLM_OLLAMA_URL.to_string()),
                model: std::env::var("OLLAMA_MODEL")
                    .unwrap_or_else(|_| config::LLM_OLLAMA_MODEL.to_string()),
            }),
            "openai" | "openai-compatible" => Ok(Self::OpenAiCompatible {
                base_url: normalize_llm_base(
                    &std::env::var("LLM_API_BASE")
                        .unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
                    "openai",
                ),
                api_key: require_api_key(&provider)?,
                model: require_model(&provider)?,
            }),
            "anthropic" => Ok(Self::Anthropic {
                base_url: normalize_llm_base(
                    &std::env::var("LLM_API_BASE")
                        .unwrap_or_else(|_| "https://api.anthropic.com".to_string()),
                    "anthropic",
                ),
                api_key: require_api_key(&provider)?,
                model: require_model(&provider)?,
            }),
            other => Err(anyhow::anyhow!(
                "unknown LLM_PROVIDER '{other}' (expected: ollama | openai | anthropic)"
            )),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Ollama { .. } => "ollama",
            Self::OpenAiCompatible { .. } => "openai",
            Self::Anthropic { .. } => "anthropic",
        }
    }

    fn model(&self) -> &str {
        match self {
            Self::Ollama { model, .. }
            | Self::OpenAiCompatible { model, .. }
            | Self::Anthropic { model, .. } => model,
        }
    }

    /// Endpoint shown in logs — never includes the API key.
    fn display_url(&self) -> &str {
        match self {
            Self::Ollama { base_url, .. }
            | Self::OpenAiCompatible { base_url, .. }
            | Self::Anthropic { base_url, .. } => base_url,
        }
    }

    /// Suggested single-inference timeout. Local CPU Ollama needs minutes
    /// (measured ~360–400s); hosted APIs answer in seconds — a tight timeout
    /// keeps retry cycles short.
    fn inference_timeout(&self) -> Duration {
        match self {
            Self::Ollama { .. } => Duration::from_secs(config::LLM_INFERENCE_TIMEOUT_SECS),
            _ => Duration::from_secs(120),
        }
    }

    /// Cheap pre-flight reachability probe (no inference). For hosted APIs a
    /// TCP+TLS handshake failure is caught by the short connect_timeout on the
    /// inference call itself, so only Ollama gets a dedicated endpoint probe.
    async fn probe(&self, probe_client: &Client) -> anyhow::Result<()> {
        match self {
            Self::Ollama { base_url, .. } => {
                let url = format!("{}/api/tags", base_url.trim_end_matches('/'));
                let resp = probe_client.get(&url).send().await?;
                if resp.status().is_success() {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Ollama /api/tags returned HTTP {}", resp.status()))
                }
            }
            _ => Ok(()),
        }
    }

    async fn chat(&self, client: &Client, user_prompt: &str) -> anyhow::Result<LlmReply> {
        match self {
            Self::Ollama { base_url, model } => {
                call_ollama(client, base_url, model, user_prompt).await
            }
            Self::OpenAiCompatible { base_url, api_key, model } => {
                call_openai_compatible(client, base_url, api_key, model, user_prompt).await
            }
            Self::Anthropic { base_url, api_key, model } => {
                call_anthropic(client, base_url, api_key, model, user_prompt).await
            }
        }
    }
}

fn require_api_key(provider: &str) -> anyhow::Result<String> {
    std::env::var("LLM_API_KEY")
        .ok()
        .map(|k| k.trim().to_string())
        .filter(|k| !k.is_empty())
        .ok_or_else(|| anyhow::anyhow!("LLM_PROVIDER={provider} requires the LLM_API_KEY env var"))
}

fn require_model(provider: &str) -> anyhow::Result<String> {
    std::env::var("LLM_MODEL")
        .ok()
        .map(|m| m.trim().to_string())
        .filter(|m| !m.is_empty())
        .ok_or_else(|| anyhow::anyhow!("LLM_PROVIDER={provider} requires the LLM_MODEL env var (e.g. gpt-4o-mini, claude-haiku-4-5)"))
}

// ── OpenAI-compatible API types ───────────────────────────────────────────────

#[derive(Serialize)]
struct OpenAiRequest {
    model: String,
    messages: Vec<ChatMessage>,
    max_tokens: u32,
    temperature: f32,
}

#[derive(Deserialize)]
struct OpenAiResponse {
    choices: Vec<OpenAiChoice>,
}

#[derive(Deserialize)]
struct OpenAiChoice {
    message: ChatMessage,
    finish_reason: Option<String>,
}

// ── Anthropic API types ───────────────────────────────────────────────────────

#[derive(Serialize)]
struct AnthropicRequest {
    model: String,
    /// Sent as a content-block array rather than a bare string so it can carry a
    /// cache breakpoint. See `AnthropicSystemBlock`.
    system: Vec<AnthropicSystemBlock>,
    messages: Vec<ChatMessage>,
    max_tokens: u32,
    /// Omitted entirely for models that no longer accept sampling parameters.
    /// Anthropic rejects the whole request with HTTP 400 rather than ignoring
    /// the field, so this cannot be sent hopefully.
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
}

/// One system content block, optionally marked as a prompt-cache breakpoint.
///
/// Anthropic bills a cached prefix at roughly a tenth of the input price, and
/// our system prompt is an ideal candidate: it is a raw string literal with no
/// interpolation, so it is byte-identical on every request. Each advisory cycle
/// makes one call per squadron seconds apart, so the second and third read the
/// entry the first one wrote.
///
/// The prompt is around 1,650 tokens. That clears the 1,024-token minimum for
/// `claude-sonnet-5` — but the minimum is NOT uniform across models: it is 512
/// on Opus 5 and 4,096 on Opus 4.6 and Haiku 4.5. On one of those the marker is
/// silently ignored and nothing caches, with no error. That is why
/// `cache_read_input_tokens` is logged: a persistent zero is the only visible
/// symptom.
///
/// Default (five-minute) TTL deliberately. Cycles are an hour apart, which sits
/// exactly on the one-hour TTL boundary, and that TTL costs double to write — a
/// poor trade for a coin flip. The within-cycle reads are the reliable win.
///
/// Nothing per-request may be added ahead of this block: caching is a prefix
/// match, so a single differing byte earlier invalidates everything after it.
#[derive(Serialize)]
struct AnthropicSystemBlock {
    #[serde(rename = "type")]
    kind: &'static str,
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<AnthropicCacheControl>,
}

#[derive(Serialize)]
struct AnthropicCacheControl {
    #[serde(rename = "type")]
    kind: &'static str,
}

/// Does this Anthropic model still accept `temperature`?
///
/// Anthropic removed the sampling parameters from its newer model families:
/// Sonnet 5, Opus 5, Opus 4.7/4.8 and Fable 5 answer a request carrying
/// `temperature` with `400 invalid_request_error: "temperature is deprecated for
/// this model"`, which took the advisor offline entirely rather than degrading.
/// Models at 4.6 and below still accept it.
///
/// Deliberately an ALLOW-list of families known to accept the parameter, not a
/// deny-list of families known to reject it. The two fail in opposite
/// directions: an unrecognized model under an allow-list simply runs with the
/// provider's default sampling, while under a deny-list it repeats exactly the
/// outage above. Anthropic is dropping these parameters as it goes, so the
/// unrecognized model of the future is far more likely to reject than accept.
///
/// The cost of omitting it is that the advisor runs at the model's default
/// sampling rather than the deliberately low `LLM_TEMPERATURE`, so its wording
/// varies more between cycles. Its proposals are parsed structurally and every
/// one is validated against the config schema before it can be applied, so this
/// costs some consistency of prose and nothing in safety.
fn anthropic_accepts_temperature(model: &str) -> bool {
    const SAMPLING_FAMILIES: [&str; 7] = [
        "claude-opus-4-6",
        "claude-sonnet-4-6",
        "claude-opus-4-5",
        "claude-sonnet-4-5",
        "claude-haiku-4-5",
        "claude-3-",   // 3.x: haiku, sonnet, opus
        "claude-2",
    ];
    let m = model.trim().to_ascii_lowercase();
    SAMPLING_FAMILIES.iter().any(|f| m.starts_with(f))
}

#[derive(Deserialize)]
struct AnthropicResponse {
    content: Vec<AnthropicContent>,
    stop_reason: Option<String>,
    /// Absent on providers that do not report it; treated as all-zero.
    #[serde(default)]
    usage: AnthropicUsage,
}

/// Token accounting, kept so caching can be verified rather than assumed.
#[derive(Deserialize, Default)]
struct AnthropicUsage {
    #[serde(default)]
    input_tokens: u32,
    /// Tokens billed at the cache-write premium — nonzero on the call that
    /// populates the entry.
    #[serde(default)]
    cache_creation_input_tokens: u32,
    /// Tokens served from cache at roughly a tenth of input price. A persistent
    /// zero across repeated calls means caching is NOT working, which is the
    /// only symptom a too-short prefix or a silent invalidator produces.
    #[serde(default)]
    cache_read_input_tokens: u32,
}

#[derive(Deserialize)]
struct AnthropicContent {
    #[serde(default)]
    text: String,
}

// ── System prompt ─────────────────────────────────────────────────────────────

/// Build the static system prompt that gives the LLM domain knowledge about
/// DRADIS strategies, the parameters that can be tuned, and the format we
/// expect its recommendations in.
fn system_prompt() -> String {
    r#"You are an expert algorithmic trading advisor for DRADIS, a multi-strategy
prediction-market trading bot operating on Polymarket binary crypto markets
(BTC/ETH/SOL hourly and daily "Up or Down" contracts).

== Platform Context ==
DRADIS trades binary outcome tokens priced 0–1.  YES + NO for the same market
always sum to ~$1.00 at settlement.  Taker fees are ~10% round-trip for most
markets (highly significant — entries need strong edge to overcome this cost).
Maker (GTC/post-only) orders pay 0% fee; taker (FAK) orders pay the dynamic fee.

== Active Strategies ("Vipers") ==
1. MOMENTUM    — Rides short-term BTC/ETH/SOL oracle velocity bursts (FAK taker).
                 Key signals: velocity_5s, velocity_1s, OBI, confirmation ticks.
                 Common failure: adverse OBI at entry; snapshot staleness.
2. MAKER       — Posts passive resting limit bids (GTC maker, 0% fee).
                 Requires wide spread & low directional risk.  Hours-long hold times.
3. ARBITRAGE   — Hedged YES+NO maker bids at combined price < $0.99.
                 Requires combined bid ≤ MAX_SUM_PRICE_FOR_ENTRY to be profitable.
4. TIME DECAY  — Buys YES+NO pairs near expiry at sub-$1.00 combined ask
                 (short-gamma / theta strategy).  Needs flat oracle and calm book.
5. BASIS       — Fades retail-skewed binary probabilities using Binance funding rate
                  as a smart-money confirmation signal.
6. GBOOST      — Online gradient-boosted ML classifier predicts YES price direction
                  from 19 orderbook + oracle features.  Retrained every 30s.
                  Has concept-drift suppression: if market regime shifts significantly,
                  entries are blocked until the next retrain clears the drift flag.
7. TRENDREVERSAL — Fades priced-in oracle drift on Window/Daily markets.
                  Buys NO when 10-min and 60-min drift are strongly BULL, YES when
                  strongly BEAR (the move is already in the token price and tends to
                  mean-revert).  One-sided, maker venue, flat sizing.  Asymmetric
                  exits: wide TP to catch the reversion, tight SL when the trend
                  instead continues, plus an always-on catastrophic stop.
                  Common failure: drift reversal before TP; adverse OBI at entry;
                  position held too long on ranging/sideways oracle.

== Key OBI Concept ==
OBI (Order Book Imbalance) = (bid_depth − ask_depth) / total_depth  ∈ [−1, +1]
Negative OBI on a token means the ask side dominates → smart money is selling.
Entering YES when OBI_y is strongly negative is entering against the book.

== Tunable Parameters (DynamicConfig) ==
These can be adjusted live without restarting the bot:
  Momentum:     stop_loss_pct, target_profit_pct, min/max_trade_size_usdc, max_exposure
  Maker:        max_entry_price, stop_loss_pct, target_profit_pct, max_exposure
  Basis:        stop_loss_pct, target_profit_pct, max_exposure
  GBoost:       entry_threshold (0–1), stop_loss_pct, target_profit_pct, max_exposure
  TimeDecay:    position_size_usdc, stop_loss_pct, max_entry_price, obi_adverse_block
  TrendCapture: stop_loss_pct, target_profit_pct, min/max_trade_size_usdc, max_entry_price, max_exposure
  Global:    ghost_mode (true = paper trading, no real orders)
  Enable flags: enable_momentum, enable_maker, enable_basis, enable_gboost,
                enable_time_decay, enable_arbitrage

IMPORTANT: The CURRENT VALUES of every parameter are provided in the user message
under "== Current Live Configuration ==". Always use those exact values as the
"current" baseline in your recommendations — never guess or assume values.

== Session Context ==
The trade data is scoped to the CURRENT SESSION (process lifetime).  When prior-session
trades are included, they are clearly labeled as "PRIOR SESSION CONTEXT" and should
inform your pattern recognition, but your primary recommendations should address the
current session's trades and conditions.

== Your Role ==
Analyze the recent trades (or absence of trades) provided and:
1. Identify loss patterns (repeated stop-losses, short hold times, common exit reasons).
2. Flag any signals of structural issues (high entry_hb_age_sec, adverse OBI at entry).
3. Suggest 2–5 specific, actionable DynamicConfig parameter changes with rationale.
4. Recommend which strategies to enable/disable given current session conditions.
5. IMPORTANT — if few or zero trades have occurred: assess whether the current parameter
   configuration is too stringent for present market conditions.  Consider that:
   - entry thresholds, min/max_trade_size, stop_loss_pct, and target_profit_pct all gate entry;
   - GBoost entry_threshold near 0.9+ may suppress trades in low-confidence regimes;
   - Momentum velocity thresholds may be too tight for a ranging/low-vol market;
   - Arbitrage MAX_SUM_PRICE may be too low for current book spreads.
   Recommend specific loosening adjustments and explain why inactivity is itself a risk
   (opportunity cost, inability to gather ML training data, stale model).

== Output Format ==
Reply ONLY in this exact structure (no preamble, no markdown headers outside this):

📊 DRADIS LLM ADVISOR
Session P&L: [value]  |  Trades analyzed: [n]

🔍 OBSERVATIONS
• [bullet 1]
• [bullet 2]
• [bullet 3 — max 5 bullets total]
(If trades = 0, focus observations on likely reasons for inactivity given current conditions.)

⚙️ RECOMMENDATIONS
1. [param_name]: [current] → [suggested] — [reason, 1 sentence]
2. [param_name]: [current] → [suggested] — [reason, 1 sentence]
(up to 4 recommendations; if no trades, prioritize recommendations that would unlock entries)

🟢 KEEP ENABLED: [comma-separated strategy names]
🔴 CONSIDER DISABLING: [comma-separated strategy names, or "none"]

After the report, append EXACTLY ONE fenced json block translating your
recommendations into machine-applicable changes:

```json
{"proposals":[{"field":"<exact key>","to":<value>,"reason":"<short reason>"}]}
```

Proposal rules (strict):
- "field" MUST be copied verbatim from the "== Machine-Editable Keys ==" list
  in the user message. Any other key is discarded.
- "to" is a plain JSON number for numeric keys (fractions, not percent strings:
  8% → 0.08) or true/false for boolean keys.
- Maximum 4 proposals; stay within the [min..max] shown for the key.
- If you have no config changes to propose, emit {"proposals":[]}.
- The json block must be the LAST thing in your reply.

Keep the entire response under 250 words (excluding the json block)."#.to_string()
}

// ── Prompt builder ────────────────────────────────────────────────────────────

/// Format recent trades + session stats into a concise user prompt.
/// Accepts both current-session trades and optional prior-session context.
fn build_user_prompt(
    session_trades: &[db::TradeRow],
    prior_trades: Option<&[db::TradeRow]>,
    open_positions: &[db::OpenPositionRow],
    session_pnl: rust_decimal::Decimal,
    starting_collateral: rust_decimal::Decimal,
    session_id: &str,
    dyn_cfg: &DynamicConfig,
    fewshot: &[db::LlmActionRow],
    // Viper kinds linked to this squadron's market class — the only ones worth
    // offering the model, since a proposal into a viper the squadron does not
    // fly can never take effect.
    allowed_vipers: &[String],
) -> String {
    let mut lines = Vec::new();

    lines.push(format!(
        "Session: {}  |  P&L: ${:.2}  |  Starting collateral: ${:.2}  |  Session trades: {}",
        truncate_on_char_boundary(session_id, 16), // trim to readable date+time prefix
        session_pnl, starting_collateral, session_trades.len()
    ));
    lines.push(String::new());

    // ── Current session trades ────────────────────────────────────────────────
    if !session_trades.is_empty() {
        lines.push("=== CURRENT SESSION TRADES (newest first) ===".to_string());
        lines.push(
            "ts | strategy | market | side | entry | exit | shares | pnl | reason".to_string(),
        );
        lines.push("-".repeat(90));
        for t in session_trades {
            lines.push(format!(
                "{} | {} | {} | {} | {} | {} | {} | ${} | {}",
                &t.ts[5..16],
                t.strategy.replace("Strategy", ""),
                truncate_on_char_boundary(&t.market, 32),
                t.side,
                t.entry_price,
                t.exit_price,
                t.shares,
                t.pnl,
                t.reason,
            ));
        }

        // Current session summary
        let (wins, losses, total_pnl_f): (usize, usize, f64) = session_trades.iter().fold(
            (0, 0, 0.0),
            |(w, l, p), t| {
                let pnl: f64 = t.pnl.parse().unwrap_or(0.0);
                if pnl > 0.0 { (w + 1, l, p + pnl) }
                else if pnl < 0.0 { (w, l + 1, p + pnl) }
                else { (w, l, p) }
            },
        );
        let win_rate = if wins + losses > 0 {
            (wins as f64 / (wins + losses) as f64) * 100.0
        } else { 0.0 };

        lines.push(String::new());
        lines.push(format!(
            "Current session: {} wins / {} losses | Win rate: {:.0}% | P&L: ${:.2}",
            wins, losses, win_rate, total_pnl_f
        ));

        // Strategy-level breakdown for current session
        let mut by_strategy: std::collections::HashMap<&str, (usize, usize, f64)> =
            std::collections::HashMap::new();
        for t in session_trades {
            let strat = t.strategy.as_str();
            let pnl: f64 = t.pnl.parse().unwrap_or(0.0);
            let entry = by_strategy.entry(strat).or_insert((0, 0, 0.0));
            if pnl > 0.0 { entry.0 += 1; }
            else if pnl < 0.0 { entry.1 += 1; }
            entry.2 += pnl;
        }
        lines.push("Per-strategy (current session):".to_string());
        let mut strat_entries: Vec<_> = by_strategy.into_iter().collect();
        strat_entries.sort_by(|a, b| b.1.2.partial_cmp(&a.1.2).unwrap_or(std::cmp::Ordering::Equal));
        for (strat, (w, l, p)) in strat_entries {
            lines.push(format!("  {}: {} wins / {} losses / ${:.2}", strat.replace("Strategy", ""), w, l, p));
        }
    } else {
        lines.push("⚠️  NO TRADES this session.".to_string());
        lines.push(
            "The bot has been running but no entries have been triggered. \
             Please assess whether the current parameter configuration is too stringent \
             for the current market conditions and recommend adjustments to unlock trade opportunities."
                .to_string(),
        );
    }

    // ── Prior session context (supplemental) ─────────────────────────────────
    if let Some(prior) = prior_trades {
        if !prior.is_empty() {
            lines.push(String::new());
            lines.push(format!(
                "=== PRIOR SESSION CONTEXT ({} recent trades — for pattern recognition only) ===",
                prior.len()
            ));
            lines.push(
                "ts | strategy | market | side | entry | exit | shares | pnl | reason".to_string(),
            );
            lines.push("-".repeat(90));
            for t in prior {
                lines.push(format!(
                    "{} | {} | {} | {} | {} | {} | {} | ${} | {}",
                    truncate_on_char_boundary(&t.ts, 16),
                    t.strategy.replace("Strategy", ""),
                    truncate_on_char_boundary(&t.market, 32),
                    t.side,
                    t.entry_price,
                    t.exit_price,
                    t.shares,
                    t.pnl,
                    t.reason,
                ));
            }
        }
    }

    lines.push(String::new());
    lines.push("== Current Live Configuration ==".to_string());
    lines.push(format!(
        "Ghost mode: {} | Strategies enabled: Momentum={}, Maker={}, Basis={}, GBoost={}, TimeDecay={}, Arbitrage={}, TrendCapture={}",
        dyn_cfg.ghost_mode,
        dyn_cfg.enable_momentum, dyn_cfg.enable_maker, dyn_cfg.enable_basis,
        dyn_cfg.enable_gboost, dyn_cfg.enable_time_decay, dyn_cfg.enable_arbitrage,
        dyn_cfg.enable_trendcapture,
    ));
    lines.push(format!(
        "Momentum: momentum_stop_loss_pct={:.0}%, momentum_target_profit_pct={:.0}%, momentum_min_trade_size_usdc=${}, momentum_max_trade_size_usdc=${}, momentum_max_exposure_usdc=${}",
        dyn_cfg.momentum_stop_loss_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.momentum_target_profit_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.momentum_min_trade_size_usdc,
        dyn_cfg.momentum_max_trade_size_usdc,
        dyn_cfg.momentum_max_exposure_usdc,
    ));
    lines.push(format!(
        "  Velocity threshold: {:.3}% of oracle price/5s (e.g. BTC@$100k → ${}/5s) | short_window_fraction={} | max_entry_price=${}",
        config::MOMENTUM_THRESHOLD_PCT * rust_decimal_macros::dec!(100),
        config::BTC_MOMENTUM_THRESHOLD,
        config::MOMENTUM_SHORT_WINDOW_FRACTION,
        config::MAX_MOMENTUM_ENTRY_PRICE,
    ));
    lines.push(format!(
        "Maker: maker_max_entry_price=${}, maker_min_entry_price=${}, maker_stop_loss_pct={:.0}%, maker_target_profit_pct={:.0}%, maker_max_exposure_usdc=${}",
        dyn_cfg.maker_max_entry_price, dyn_cfg.maker_min_entry_price,
        dyn_cfg.maker_stop_loss_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.maker_target_profit_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.maker_max_exposure_usdc,
    ));
    lines.push(format!(
        "GBoost: gboost_entry_threshold={}, gboost_stop_loss_pct={:.0}%, gboost_target_profit_pct={:.0}%, gboost_max_exposure_usdc=${}",
        dyn_cfg.gboost_entry_threshold,
        dyn_cfg.gboost_stop_loss_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.gboost_target_profit_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.gboost_max_exposure_usdc,
    ));
    lines.push(format!(
        "Basis: basis_stop_loss_pct={:.0}%, basis_target_profit_pct={:.0}%, basis_max_exposure_usdc=${}",
        dyn_cfg.basis_stop_loss_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.basis_target_profit_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.basis_max_exposure_usdc,
    ));
    lines.push(format!(
        "TimeDecay: time_decay_position_size_usdc=${}, time_decay_stop_loss_pct={:.0}%, time_decay_max_entry_price=${}, time_decay_max_exposure_usdc=${}",
        dyn_cfg.time_decay_position_size_usdc,
        dyn_cfg.time_decay_stop_loss_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.time_decay_max_entry_price,
        dyn_cfg.time_decay_max_exposure_usdc,
    ));
    lines.push(format!(
        "TrendCapture: trendcapture_stop_loss_pct={:.0}%, trendcapture_target_profit_pct={:.0}%, trendcapture_min_trade_size_usdc=${}, trendcapture_max_trade_size_usdc=${}, trendcapture_max_entry_price=${}, trendcapture_max_exposure_usdc=${}",
        dyn_cfg.trendcapture_stop_loss_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.trendcapture_target_profit_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.trendcapture_min_trade_size_usdc,
        dyn_cfg.trendcapture_max_trade_size_usdc,
        dyn_cfg.trendcapture_max_entry_price,
        dyn_cfg.trendcapture_max_exposure_usdc,
    ));

    lines.push(String::new());
    lines.push("Please analyze the above and provide recommendations as instructed.".to_string());

    // ── Open positions (in-flight, not yet closed) ───────────────────────────
    if !open_positions.is_empty() {
        lines.push(String::new());
        lines.push(format!("== Open Positions ({} currently in-flight) ==", open_positions.len()));
        lines.push("strategy | side | market | entry_price | shares | mode".to_string());
        for p in open_positions {
            lines.push(format!(
                "{} | {} | {} | {} | {} | {}",
                p.strategy.replace("Strategy", ""),
                p.side,
                truncate_on_char_boundary(&p.market, 32),
                p.entry_price,
                p.shares,
                if p.ghost_mode { "ghost" } else { "live" },
            ));
        }
        lines.push("Note: these positions are open and awaiting exit/settlement. Account for them in your P&L and risk assessment.".to_string());
    }

    // ── Machine-editable key list for the proposals json block ────────────────
    // Exact serde keys + current values + allowed bounds, restricted to Basic
    // (non-advanced) fields of Global + currently-enabled vipers to keep the
    // prompt compact for small local models. Values are the RAW config values
    // (fractions, not display percentages).
    lines.push(String::new());
    lines.push("== Machine-Editable Keys ==".to_string());
    lines.push("Use ONLY these exact keys in the proposals json block (key = current [min..max]):".to_string());
    lines.push("(A parameter not listed below cannot be changed; do not propose it.)".to_string());
    let cfg_json = serde_json::to_value(dyn_cfg).unwrap_or(serde_json::Value::Null);
    for f in crate::api::config_schema::config_schema() {
        if f.advanced {
            continue;
        }
        // Skip fields of disabled vipers — proposing into a dormant strategy
        // wastes the 4-change budget (enable flags themselves stay listed).
        if let Some(enable_key) = f.enable_key {
            let enabled = cfg_json
                .get(enable_key)
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            if !enabled && f.key != enable_key {
                continue;
            }
            // And skip vipers this squadron's market class does not fly at all.
            // A politics squadron runs Arbitrage and Maker; offering it Momentum
            // or GBoost invites proposals that could never take effect and burns
            // the change budget on them. `enable_maker` → `maker` is exact,
            // which is why the enable key is used rather than the display group.
            if let Some(kind) = enable_key.strip_prefix("enable_") {
                if !allowed_vipers.iter().any(|v| v == kind) {
                    continue;
                }
            }
        }
        let current = cfg_json
            .get(f.key)
            .map(|v| match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .unwrap_or_else(|| "?".to_string());
        let bounds = match (f.min, f.max) {
            (Some(mn), Some(mx)) => format!(" [{mn}..{mx}]"),
            (Some(mn), None) => format!(" [min {mn}]"),
            (None, Some(mx)) => format!(" [max {mx}]"),
            (None, None) => String::new(),
        };
        lines.push(format!("{} = {}{}", f.key, current, bounds));
    }

    // ── Prior proposal track record (few-shot, S7) ────────────────────────────
    // The model's own history: what got applied and how it scored, what a
    // human rejected, what the circuit breaker undid. Explicit instruction to
    // learn from it rather than repeat mistakes.
    if !fewshot.is_empty() {
        lines.push(String::new());
        lines.push("== Prior Proposal Outcomes ==".to_string());
        lines.push("Your recent config proposals and their results. Do NOT repeat rejected or reverted changes; favor patterns that scored positive:".to_string());
        for a in fewshot {
            let change = format!(
                "{}: {} -> {}",
                a.field,
                a.from_value.trim_matches('"'),
                a.to_value.trim_matches('"'),
            );
            let result = match a.status.as_str() {
                "reverted" => "REVERTED by circuit breaker (P&L drawdown after apply)".to_string(),
                "rejected" => format!(
                    "REJECTED — {}",
                    a.status_detail.as_deref().unwrap_or(&a.reason),
                ),
                _ => match a.outcome_score {
                    Some(s) => format!("applied, P&L {}{:.2} over the scoring window", if s >= 0.0 { "+$" } else { "-$" }, s.abs()),
                    None => "applied, outcome pending".to_string(),
                },
            };
            lines.push(format!("- {} — {}", change, result));
        }
    }

    lines.join("\n")
}

// ── Provider API calls ────────────────────────────────────────────────────────

/// Output token cap shared by all providers (Ollama `num_predict`,
/// OpenAI/Anthropic `max_tokens`) — sized for the ≤250-word structured report.
use crate::config::LLM_MAX_OUTPUT_TOKENS;

/// The operator's output-token ceiling, live.
///
/// `llm_max_output_tokens` is a Deployment-group knob, editable in Setup, and
/// all three risk profiles set it — but every provider call read the
/// compile-time `LLM_MAX_OUTPUT_TOKENS` instead, so the knob governed nothing.
/// Its own help text tells the operator to raise it when recommendations arrive
/// truncated, advice that could not work: raising it changed no behavior at all.
///
/// Read from the live global broadcast rather than the DB — this sits on the
/// request path and the value is global by definition. Falls back to the
/// compile-time constant before the sender is registered.
fn max_output_tokens() -> u32 {
    crate::helpers::dynamic_config::global_config_tx()
        .map(|tx| tx.borrow().llm_max_output_tokens)
        .unwrap_or(LLM_MAX_OUTPUT_TOKENS)
}
/// Low temperature: consistent, factual recommendations.
const LLM_TEMPERATURE: f32 = 0.3;

async fn call_ollama(
    client: &Client,
    ollama_base_url: &str,
    model: &str,
    user_prompt: &str,
) -> anyhow::Result<LlmReply> {
    let url = format!("{}/api/chat", ollama_base_url.trim_end_matches('/'));

    let request = OllamaRequest {
        model: model.to_string(),
        messages: vec![
            ChatMessage {
                role: "system".to_string(),
                content: system_prompt(),
            },
            ChatMessage {
                role: "user".to_string(),
                content: user_prompt.to_string(),
            },
        ],
        stream: false,
        options: OllamaOptions {
            num_predict: max_output_tokens(),
            temperature: LLM_TEMPERATURE,
            num_ctx: 4096,     // Room for prompt + machine-editable key list + full recommendation
        },
    };

    let resp = client
        .post(&url)
        .json(&request)
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("Ollama HTTP {}: {}", status, body));
    }

    let ollama_resp: OllamaResponse = resp.json().await?;
    Ok(LlmReply {
        content: ollama_resp.message.content.trim().to_string(),
        truncated_by_length: matches!(ollama_resp.done_reason.as_deref(), Some("length")),
    })
}

async fn call_openai_compatible(
    client: &Client,
    base_url: &str,
    api_key: &str,
    model: &str,
    user_prompt: &str,
) -> anyhow::Result<LlmReply> {
    let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));

    let request = OpenAiRequest {
        model: model.to_string(),
        messages: vec![
            ChatMessage { role: "system".to_string(), content: system_prompt() },
            ChatMessage { role: "user".to_string(), content: user_prompt.to_string() },
        ],
        max_tokens: max_output_tokens(),
        temperature: LLM_TEMPERATURE,
    };

    let resp = client
        .post(&url)
        .bearer_auth(api_key)
        .json(&request)
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        // Body may echo request details but never the key; safe to surface.
        return Err(anyhow::anyhow!("OpenAI-compatible HTTP {}: {}", status, body));
    }

    let parsed: OpenAiResponse = resp.json().await?;
    let choice = parsed
        .choices
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("OpenAI-compatible response had no choices"))?;
    Ok(LlmReply {
        content: choice.message.content.trim().to_string(),
        truncated_by_length: matches!(choice.finish_reason.as_deref(), Some("length")),
    })
}

async fn call_anthropic(
    client: &Client,
    base_url: &str,
    api_key: &str,
    model: &str,
    user_prompt: &str,
) -> anyhow::Result<LlmReply> {
    let url = format!("{}/v1/messages", base_url.trim_end_matches('/'));

    let temperature = anthropic_accepts_temperature(model).then_some(LLM_TEMPERATURE);
    if temperature.is_none() {
        debug!("🤖 LLM Advisor: {model} does not accept `temperature` — using the model's default sampling");
    }
    let request = AnthropicRequest {
        model: model.to_string(),
        // One breakpoint, on the only block that is byte-identical every call.
        system: vec![AnthropicSystemBlock {
            kind: "text",
            text: system_prompt(),
            cache_control: Some(AnthropicCacheControl { kind: "ephemeral" }),
        }],
        messages: vec![ChatMessage { role: "user".to_string(), content: user_prompt.to_string() }],
        max_tokens: max_output_tokens(),
        temperature,
    };

    let resp = client
        .post(&url)
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .json(&request)
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("Anthropic HTTP {}: {}", status, body));
    }

    let parsed: AnthropicResponse = resp.json().await?;
    let content = parsed
        .content
        .iter()
        .map(|c| c.text.as_str())
        .collect::<Vec<_>>()
        .join("")
        .trim()
        .to_string();
    // Report the cache counters every call. A cached prefix bills at roughly a
    // tenth of input price, but the failure mode is silent: too short a prefix,
    // or any byte changing ahead of the breakpoint, simply produces no cache
    // entry with no error. A run of calls where `read` stays 0 is the only
    // evidence that would show, so it is logged rather than presumed.
    let u = &parsed.usage;
    if u.cache_creation_input_tokens > 0 || u.cache_read_input_tokens > 0 {
        debug!(
            "🤖 LLM Advisor: tokens in={} cache write={} cache read={}",
            u.input_tokens, u.cache_creation_input_tokens, u.cache_read_input_tokens,
        );
    } else if u.input_tokens > 0 {
        debug!(
            "🤖 LLM Advisor: tokens in={} — nothing cached this call. Expected on the \
             first call of a cycle; persistent zeros mean the prompt prefix is not \
             caching (model minimum not met, or something varies ahead of the breakpoint).",
            u.input_tokens,
        );
    }

    if content.is_empty() {
        return Err(anyhow::anyhow!("Anthropic response had no text content"));
    }
    Ok(LlmReply {
        content,
        truncated_by_length: matches!(parsed.stop_reason.as_deref(), Some("max_tokens")),
    })
}

// ── Main advisor loop ─────────────────────────────────────────────────────────

/// Per-squadron memory of what the advisor last analyzed, so an unchanged
/// situation is not re-analyzed at full price every interval.
///
/// The loop below used to call the LLM on every tick unconditionally, and the
/// comment defending that said "0 trades is itself a meaningful signal". True the
/// first time; false the twenty-fourth. On 2026-08-28 the Ireland instance made
/// 33 provider calls in 4.5 hours and 24 of them analyzed byte-identical input:
/// the same two trades, the same P&L, the same open positions. The advisor cannot
/// say anything new about state it has already seen, and every customer running
/// this pays their own provider for each repeat.
///
/// Value is (fingerprint of the last analyzed inputs, consecutive skips since).
fn advisor_seen_state() -> &'static std::sync::Mutex<std::collections::HashMap<String, (u64, u32)>> {
    static REG: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, (u64, u32)>>,
    > = std::sync::OnceLock::new();
    REG.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Hash of everything that could change the advisor's answer. Deliberately
/// coarse: trade identities and counts, realized P&L, open exposure, and the
/// squadron's own config, since advice is relative to the settings in force.
fn advisor_input_fingerprint(
    trades: &[db::TradeRow],
    positions: &[db::OpenPositionRow],
    pnl: rust_decimal::Decimal,
    collateral: rust_decimal::Decimal,
    dyn_cfg: &DynamicConfig,
) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    trades.len().hash(&mut h);
    positions.len().hash(&mut h);
    // TradeRow stores everything as strings and has no id column, so identity is
    // the timestamp plus market plus realized P&L. Hashing the rendered decimals
    // rather than floats keeps this exact and avoids float equality entirely.
    pnl.to_string().hash(&mut h);
    collateral.to_string().hash(&mut h);
    for t in trades {
        t.ts.hash(&mut h);
        t.market.hash(&mut h);
        t.pnl.hash(&mut h);
    }
    for p in positions {
        p.token_id.hash(&mut h);
        p.shares.hash(&mut h);
    }
    // Advice is relative to the config in force, so a tuning change warrants a
    // fresh look even when the trades are identical.
    serde_json::to_string(dyn_cfg).unwrap_or_default().hash(&mut h);
    h.finish()
}

/// Whether to spend a provider call on this squadron now.
///
/// Skips while the inputs are unchanged, but not forever: after
/// `LLM_ADVISOR_MAX_SKIPPED_CYCLES` consecutive skips it fires anyway, because
/// market conditions can move underneath an idle engine and that is exactly the
/// case the original always-call comment was protecting.
fn advisor_should_call(squadron_id: &str, fingerprint: u64) -> bool {
    let mut reg = match advisor_seen_state().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    match reg.get(squadron_id) {
        Some((prev, skips))
            if *prev == fingerprint && *skips < config::LLM_ADVISOR_MAX_SKIPPED_CYCLES =>
        {
            let n = skips + 1;
            reg.insert(squadron_id.to_string(), (fingerprint, n));
            debug!(
                "🤖 LLM Advisor: [{}] inputs unchanged, skipping provider call ({}/{} before a \
                 forced refresh)",
                squadron_id, n, config::LLM_ADVISOR_MAX_SKIPPED_CYCLES,
            );
            false
        }
        _ => {
            reg.insert(squadron_id.to_string(), (fingerprint, 0));
            true
        }
    }
}

/// Cycles remaining on a hard provider block, and the reason.
fn advisor_hard_block() -> &'static std::sync::Mutex<Option<(String, u32)>> {
    static REG: std::sync::OnceLock<std::sync::Mutex<Option<(String, u32)>>> =
        std::sync::OnceLock::new();
    REG.get_or_init(|| std::sync::Mutex::new(None))
}

/// True when a provider error is durable rather than transient: billing, quota
/// and authentication failures do not recover by being retried.
///
/// The case that prompted this: an exhausted Anthropic balance returned HTTP 400
/// "Your credit balance is too low" on every squadron, every cycle. Retrying that
/// cannot succeed, and it costs a pair of ERROR lines per squadron forever until
/// a human notices.
fn advisor_error_is_durable(err: &str) -> bool {
    let e = err.to_lowercase();
    [
        "credit balance is too low",
        "insufficient_quota",
        "invalid x-api-key",
        "authentication_error",
        "permission_error",
        "401 unauthorized",
        "403 forbidden",
    ]
    .iter()
    .any(|needle| e.contains(needle))
}

/// Spawn this as a long-running tokio task at startup.
///
/// The task immediately checks ENABLE_LLM_ADVISOR and exits early if disabled,
/// so there is no cost to always registering it in main.rs.
///
/// **Multi-Asset CAG Advisor** — reads trades from ALL registered asset databases,
/// combines them into a unified portfolio analysis, and writes recommendations to
/// the primary DB for display on the CAG overview dashboard.
pub async fn run_llm_advisor_loop(
    tg_token: String,
    tg_chat_id: String,
    // Live session handles, when the venue keeps a SessionState. Only the intl
    // CLOB does; Kalshi and Polymarket US track P&L inside their own trade loops
    // and publish it to `pnl_history` instead, so they pass None and the loop
    // reads the latest snapshot from the database each cycle.
    session_pnl: Option<Arc<Mutex<rust_decimal::Decimal>>>,
    starting_collateral: Option<Arc<Mutex<rust_decimal::Decimal>>>,
    mut config_rx: tokio::sync::watch::Receiver<Arc<DynamicConfig>>,
    config_tx: Arc<tokio::sync::watch::Sender<Arc<DynamicConfig>>>,
    // The live registry, used to advise only on squadrons that actually exist.
    cag: crate::cag::Cag,
) {
    // Resolve the enable flag: the ENABLE_LLM_ADVISOR env var (if set) overrides
    // the compile-time default. This lets a single binary run the advisor on the
    // demo box while disabling it on the live box — the live .env sets
    // ENABLE_LLM_ADVISOR=false and .env.demo re-enables it (demo overrides win).
    // Accepts 1/true/yes/on (case-insensitive) as truthy; anything else is false.
    let advisor_enabled = match std::env::var("ENABLE_LLM_ADVISOR") {
        Ok(v) => matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        Err(_) => config::ENABLE_LLM_ADVISOR,
    };
    if !advisor_enabled {
        info!("🤖 LLM Advisor: disabled (ENABLE_LLM_ADVISOR=false)");
        return;
    }

    // Resolve the LLM provider (ollama / openai / anthropic) from the environment.
    // Misconfiguration (unknown provider, missing key/model) disables the advisor
    // loudly rather than hammering a broken endpoint every interval.
    let provider = match LlmProvider::from_env() {
        Ok(p) => p,
        Err(e) => {
            error!("🤖 LLM Advisor: provider misconfigured — advisor disabled: {}", e);
            return;
        }
    };

    // ── Autonomy scope guard ─────────────────────────────────────────────────
    // Auto-applies (and human approvals) land on the GLOBAL DynamicConfig via
    // `apply_patch_as`, but no patrol loop reads that record — squadrons run
    // their own per-squadron config, and new squadrons seed from compile-time
    // defaults rather than the global row. Of the 53 non-advanced (advisor-
    // tunable) schema fields, 52 are viper params that live in squadron config;
    // the one global field, `ghost_mode`, is held for human approval at every
    // tier. Tiers 2–3 therefore cannot move a single live strategy parameter.
    // Warn loudly rather than let the engine look like it is self-tuning.
    // Fix tracked in ROADMAP.md — "LLM advisor writes to a config no squadron
    // reads" (re-scope the advisor to run per squadron).
    let configured_tier = std::env::var("LLM_AUTONOMY_TIER")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|t| (1..=3).contains(t))
        .unwrap_or(1);
    if configured_tier > 1 {
        // Auto-apply now lands on the squadron's own config and reaches the
        // patrol loop on its next tick, so this is a live-money warning rather
        // than the "this does nothing" notice it replaced.
        warn!(
            "🤖 LLM autonomy tier {} is configured — accepted proposals will be applied to \
             squadron configs automatically and take effect on the next patrol tick. The circuit \
             breaker reverts them and demotes to tier 1 on a P&L drawdown.",
            configured_tier,
        );
    }

    info!(
        "🤖 LLM Advisor started (CAG multi-asset mode) — provider: {} | model: {} @ {} | interval: {}s | session: {}",
        provider.name(),
        provider.model(),
        provider.display_url(),
        config::LLM_ADVISOR_INTERVAL_SECS,
        db::current_session_id(),
    );

    // Two HTTP clients with different timeout profiles:
    //
    // probe_client — used for the pre-flight health-check (Ollama GET /api/tags;
    //   hosted APIs skip the probe — their short connect_timeout catches outages).
    //   connect_timeout: 5 s  (fail fast if the container/host is unreachable)
    //   timeout:        10 s  (total; /api/tags returns in <1 s when healthy)
    //
    // inference_client — used for the actual chat call. Timeout is per-provider:
    //   Ollama: LLM_INFERENCE_TIMEOUT_SECS (480 s — measured on t3.large,
    //           qwen2.5:3b takes ~360–400s; 480s gives a 20% buffer).
    //   Hosted APIs (openai/anthropic): 120 s — they answer in seconds, and a
    //           tight timeout keeps retry cycles short.
    //
    // Previously the Ollama timeout was 360s — exactly at the model's natural
    // completion time — causing every first attempt to time out, triggering
    // needless retry cycles that consumed 12–17 min of a 30-min advisory interval.
    let probe_client = Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    let http_client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(provider.inference_timeout())
        .build()
        .unwrap_or_default();

    // Skip the first tick so we don't fire immediately at startup before any
    // trades have occurred.
    let mut ticker = interval(Duration::from_secs(config::LLM_ADVISOR_INTERVAL_SECS));
    ticker.tick().await;

    loop {
        ticker.tick().await;

        // A durable provider failure (billing, quota, bad key) cannot be fixed by
        // trying again, so stand down instead of burning a call and a pair of
        // ERROR lines per squadron every interval. Self-heals when the backoff
        // expires, so topping up credits needs no restart.
        {
            let mut block = match advisor_hard_block().lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            if let Some((reason, remaining)) = block.clone() {
                if remaining > 0 {
                    *block = Some((reason.clone(), remaining - 1));
                    debug!(
                        "🤖 LLM Advisor: standing down for {} more cycle(s) after a durable \
                         provider failure: {}",
                        remaining - 1, reason,
                    );
                    continue;
                }
                *block = None;
                info!("🤖 LLM Advisor: provider backoff expired, retrying this cycle");
            }
        }

        // ── Gather data from ALL asset databases ──────────────────────────────
        // Collect trades from every registered asset pool (btc, eth, sol, etc.).
        // Tag each trade with its asset for the LLM to provide asset-specific insights.

        let primary_pool = match db::pool() {
            Some(p) => p,
            None => {
                warn!("🤖 LLM Advisor: primary DB pool not available, skipping cycle");
                continue;
            }
        };

        // Get all registered assets from the pool registry
        let registered_assets = db::available_assets();
        if registered_assets.is_empty() {
            warn!("🤖 LLM Advisor: no asset pools registered, skipping cycle");
            continue;
        }

        info!(
            "🤖 LLM Advisor: collecting trades from {} squadron(s): {}",
            registered_assets.len(),
            registered_assets.join(", ").to_uppercase()
        );

        // Collect session trades from all assets
        let mut all_session_trades = Vec::new();
        let mut all_open_positions = Vec::new();

        for asset in &registered_assets {
            if let Some(pool) = db::pool_for(asset) {
                let trades = db::get_session_trades(&pool).await;
                let positions = db::get_open_positions(&pool).await;

                if !trades.is_empty() || !positions.is_empty() {
                    info!(
                        "🤖 LLM Advisor: {} squadron — {} trade(s), {} open position(s)",
                        asset.to_uppercase(), trades.len(), positions.len()
                    );
                }

                // Tag trades with their asset (we'll label them in the prompt)
                all_session_trades.extend(trades);
                all_open_positions.extend(positions);
            }
        }

        let session_id = db::current_session_id().to_string();

        // Determine supplemental context: if current session is thin, pull prior session.
        // NOTE: We always proceed to the LLM call — 0 trades is itself a meaningful signal
        // (settings may be too stringent for the current market).  Prior session data is
        // appended as supplemental context when available; absence of it is not a blocker.
        let prior_trades: Option<Vec<db::TradeRow>> =
            if all_session_trades.len() < LLM_ADVISOR_MIN_SESSION_TRADES {
                // For multi-asset mode, pull prior trades from primary pool only
                // (could extend to all pools but that risks context explosion)
                let prior = db::get_previous_session_trades(&primary_pool, LLM_ADVISOR_PRIOR_SESSION_SUPPLEMENT).await;
                if prior.is_empty() {
                    if all_session_trades.is_empty() {
                        info!(
                            "🤖 LLM Advisor: 0 session trades across all squadrons, no prior history — \
                             firing with market-conditions / settings-stringency prompt"
                        );
                    } else {
                        info!(
                            "🤖 LLM Advisor: only {} session trade(s) (min {}), no prior session data — \
                             proceeding with thin-data analysis",
                            all_session_trades.len(), LLM_ADVISOR_MIN_SESSION_TRADES
                        );
                    }
                    None // proceed without supplemental context
                } else {
                    info!(
                        "🤖 LLM Advisor: {} session trade(s) (below min {}), supplementing with {} prior-session trades",
                        all_session_trades.len(), LLM_ADVISOR_MIN_SESSION_TRADES, prior.len()
                    );
                    Some(prior)
                }
            } else {
                None
            };

        // Session P&L and collateral, from the live handles where they exist and
        // from the newest `pnl_history` row otherwise. Both venues without a
        // SessionState already record that snapshot every dashboard sync, so
        // this is the same number their Control Tower shows — not a stand-in.
        let (current_pnl, collateral) = match (&session_pnl, &starting_collateral) {
            (Some(p), Some(c)) => (*p.lock().await, *c.lock().await),
            _ => {
                // Freshest snapshot across EVERY registered shard, not the
                // primary one.
                //
                // `db::pool()` returns whichever shard registered first, and on
                // Polymarket US that is `us` — a vestigial shard left behind
                // when the wings were renamed to us-sports / us-politics /
                // us-crypto. Nothing has written to it since 2026-08-23, so the
                // advisor found no snapshot and skipped EVERY cycle: that venue
                // produced no recommendation at all, and the tier-3 autonomy an
                // operator had switched on could never fire. It looked like a
                // quiet advisor rather than a broken one.
                //
                // The P&L history endpoint already hit this and already picks
                // the freshest pool for the same reason; this is that fix,
                // applied where it was missed.
                let latest = newest_pnl_snapshot_across_shards().await;
                match latest.as_ref() {
                    Some(row) => (
                        row.session_pnl.parse().unwrap_or(rust_decimal::Decimal::ZERO),
                        row.collateral.parse().unwrap_or(rust_decimal::Decimal::ZERO),
                    ),
                    // No snapshot yet — the venue has not completed a dashboard
                    // sync. Skip rather than reason about a portfolio of zero.
                    None => {
                        info!("🤖 LLM Advisor: no P&L snapshot yet — skipping this cycle");
                        continue;
                    }
                }
            }
        };
        // Kept only to drain the watch channel; the CONFIG the advisor reasons
        // about is now loaded per squadron below. The global record is not read.
        let _ = config_rx.borrow_and_update();

        // ── Autonomy circuit breaker (S3) ────────────────────────────────────
        // Every cycle, check whether session P&L has drawn down past the
        // breaker threshold since any auto-applied change; if so revert them,
        // demote to tier 1, and alert.
        {
            use rust_decimal::prelude::ToPrimitive;
            let knobs = crate::helpers::llm_policy::PolicyKnobs::from_env();
            if let Some(alert) = crate::helpers::llm_policy::circuit_breaker_check(
                &primary_pool,
                &config_tx,
                current_pnl.to_f64().unwrap_or(0.0),
                &knobs,
            ).await {
                if !tg_token.is_empty() && !tg_chat_id.is_empty() {
                    let _ = notifications::send_notification(&tg_token, &tg_chat_id, &alert).await;
                }
            }
        }

        // ── Outcome scoring (S7) ─────────────────────────────────────────────
        // Applied/reverted actions past the horizon get scored with the
        // session-P&L delta since their apply. Scores surface in the AI
        // Actions view and become the few-shot track record below.
        {
            use rust_decimal::prelude::ToPrimitive;
            let pnl_now = current_pnl.to_f64().unwrap_or(0.0);
            let horizon = outcome_horizon_secs();
            let before = (chrono::Utc::now() - chrono::Duration::seconds(horizon)).to_rfc3339();
            for a in db::fetch_llm_actions_needing_outcome(&primary_pool, &before).await {
                let base = a.pnl_at_apply.unwrap_or(pnl_now);
                let score = pnl_now - base;
                let detail = format!(
                    "session P&L Δ ${:.2} over ≥{}h since {} ({})",
                    score, horizon / 3600, a.status, a.field,
                );
                db::set_llm_action_outcome(&primary_pool, a.id, score, &detail).await;
                info!("🤖 LLM Advisor: outcome scored #{} {} → {:+.2}", a.id, a.field, score);
            }
        }

        // Track record for the prompt: recent rejections, reverts, and scored
        // applies — the model learns what worked and what got shot down.
        let fewshot = db::fetch_llm_fewshot_examples(&primary_pool, LLM_FEWSHOT_EXAMPLES).await;

        let session_trade_count = all_session_trades.len();
        let total_trade_count = session_trade_count
            + prior_trades.as_ref().map(|p| p.len()).unwrap_or(0);

        // ── Pre-flight: verify the provider is reachable before a long inference ──
        // Ollama gets a real endpoint probe (GET /api/tags, fast probe_client);
        // hosted APIs skip it. On failure we skip this cycle rather than block.
        if let Err(e) = provider.probe(&probe_client).await {
            warn!(
                "🤖 LLM Advisor: {} unreachable at {} — skipping cycle ({})",
                provider.name(), provider.display_url(), e
            );
            continue;
        }

        // ── One advisory pass PER SQUADRON ───────────────────────────────────
        //
        // The advisor used to run a single pass against the global DynamicConfig
        // and apply back to it. No patrol loop reads that record — squadrons read
        // a per-squadron handle — so tiers 2 and 3 were structurally inert, and
        // worse, each cycle's prompt read back the global record the previous
        // cycle had mutated. The model saw its own past changes as current state
        // while nothing had actually moved, so its reasoning drifted further from
        // reality every cycle.
        //
        // Portfolio context (trades, P&L, open positions) stays fleet-wide — that
        // is genuinely global. The CONFIG half of the prompt, and every apply,
        // is now the squadron's own.
        // `squadron_configs` is a record of every squadron that has EVER run —
        // rows persist so an operator's tuning survives restarts and rotations.
        // Advising from it directly meant advising on squadrons that no longer
        // exist: on 2026-08-24 the intl advisor spent a cycle on `btc-open`, a
        // row left from an earlier era, and produced four recommendations the
        // operator could approve and did. They were written to a config that
        // nothing reads, while every surface reported them applied.
        //
        // Intersect with the live registry so a recommendation can always reach
        // the squadron it is about.
        let live: std::collections::HashSet<String> = cag
            .list_squadrons()
            .into_iter()
            .filter(|sq| sq.state != "STOOD_DOWN")
            .map(|sq| sq.id)
            .collect();
        let all_configured = db::list_squadron_configs(&primary_pool).await;
        let skipped: Vec<&str> = all_configured.iter()
            .filter(|(id, _)| !live.contains(id))
            .map(|(id, _)| id.as_str())
            .collect();
        if !skipped.is_empty() {
            debug!("🤖 LLM Advisor: skipping {} squadron(s) with config but no live squadron: {}",
                   skipped.len(), skipped.join(", "));
        }
        let squadrons: Vec<(String, String)> = all_configured
            .into_iter()
            .filter(|(id, _)| live.contains(id))
            .collect();
        if squadrons.is_empty() {
            info!("🤖 LLM Advisor: no squadrons configured yet — nothing to advise on this cycle");
            continue;
        }
        info!(
            "🤖 LLM Advisor: {} squadron(s) to advise: {}",
            squadrons.len(),
            squadrons.iter().map(|(id, c)| format!("{id} ({c})")).collect::<Vec<_>>().join(", "),
        );

        for (squadron_id, market_class) in squadrons {
        let dyn_cfg = DynamicConfig::load_for_squadron(&squadron_id).await;
        let allowed_vipers = db::vipers_for_class(&primary_pool, &market_class).await;

        // Nothing new to analyze? Do not pay for a restatement. See
        // `advisor_should_call` for why this is not a regression of the
        // deliberate "0 trades is itself a signal" behavior.
        let fingerprint = advisor_input_fingerprint(
            &all_session_trades, &all_open_positions, current_pnl, collateral, &dyn_cfg,
        );
        if !advisor_should_call(&squadron_id, fingerprint) {
            continue;
        }

        // ── Build prompt & call LLM (with retries) ───────────────────────────
        // all_open_positions already collected above from all asset pools
        let user_prompt = build_user_prompt(
            &all_session_trades,
            prior_trades.as_deref(),
            &all_open_positions,
            current_pnl,
            collateral,
            &session_id,
            &dyn_cfg,
            &fewshot,
            &allowed_vipers,
        );

        info!(
            "🤖 LLM Advisor: calling {} ({}) for session {} ({} session + {} prior trades, P&L ${:.2})...",
            provider.model(),
            provider.name(),
            truncate_on_char_boundary(&session_id, 16),
            session_trade_count,
            total_trade_count - session_trade_count,
            current_pnl,
        );

        // Retry up to 2 times with a 30-second backoff on transient errors.
        const MAX_RETRIES: u32 = 2;
        let mut last_err = String::new();
        let mut analysis_opt: Option<String> = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                warn!(
                    "🤖 LLM Advisor: retry {}/{} after error: {}",
                    attempt, MAX_RETRIES, last_err
                );
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            match provider.chat(&http_client, &user_prompt).await {
                Ok(reply) => {
                    if reply.truncated_by_length {
                        warn!(
                            "🤖 LLM Advisor: output hit the {}-token output cap — consider increasing if recommendations still end mid-thought",
                            max_output_tokens(),
                        );
                    }
                    analysis_opt = Some(reply.content);
                    break;
                }
                Err(e) => {
                    last_err = e.to_string();
                }
            }
        }

        match analysis_opt {
            Some(analysis) => {
                info!("🤖 LLM Advisor: analysis received ({} chars)", analysis.len());

                // ── Config patch proposals (S1 parse / S2 persist) ────────────
                // Extract + validate the machine block. Malformed proposals never
                // block the prose report. Accepted changes land as `proposed`
                // rows (TTL-bound), validation rejects as `rejected` — both feed
                // the approval flow (S4) and the few-shot corpus (S7). Tiered
                // application is the policy engine's job (S3).
                use crate::helpers::llm_patch;
                use crate::helpers::llm_policy;
                let mut policy_note: Option<String> = None;
                let expired = db::expire_stale_llm_actions(&primary_pool).await;
                if expired > 0 {
                    info!("🤖 LLM Advisor: {} stale proposal(s) expired (TTL {}s)", expired, LLM_PROPOSAL_TTL_SECS);
                }
                match llm_patch::proposals_from_response(&analysis, &dyn_cfg) {
                    None => info!("🤖 LLM Advisor: no proposal block in response"),
                    Some(Err(e)) => warn!("🤖 LLM Advisor: proposal block rejected — {}", e),
                    Some(Ok(batch)) => {
                        for c in &batch.accepted {
                            info!(
                                "🤖 LLM Advisor: proposal ✔ {}: {} → {}{}{} — {}",
                                c.key, c.from, c.to,
                                if c.clamped { " (clamped to schema bounds)" } else { "" },
                                c.delta_pct.map(|d| format!(" [{:+.1}%]", d * 100.0)).unwrap_or_default(),
                                c.reason,
                            );
                        }
                        for r in &batch.rejected {
                            warn!("🤖 LLM Advisor: proposal ✖ {} → {}: {}", r.field, r.to, r.why);
                        }
                        if batch.is_empty() {
                            info!("🤖 LLM Advisor: proposal block present but empty");
                        } else {
                            let batch_id = format!(
                                "batch-{}", chrono::Utc::now().format("%Y%m%dT%H%M%S%.3fZ")
                            );
                            let ids = db::record_llm_action_batch(
                                &primary_pool,
                                &batch_id,
                                provider.model(),
                                autonomy_tier(),
                                dyn_cfg.ghost_mode,
                                LLM_PROPOSAL_TTL_SECS,
                                &batch,
                                &squadron_id,
                            ).await;
                            info!(
                                "🤖 LLM Advisor: [{}] {} recorded {} proposed / {} rejected change(s) (tier {})",
                                squadron_id, batch_id, ids.len(), batch.rejected.len(), autonomy_tier(),
                            );

                            // ── Policy engine (S3): tier enforcement ─────────
                            use rust_decimal::prelude::ToPrimitive;
                            let knobs = llm_policy::PolicyKnobs::from_env();
                            let outcome = llm_policy::enforce_batch(
                                &primary_pool,
                                &batch,
                                &ids,
                                autonomy_tier(),
                                &squadron_id,
                                current_pnl.to_f64().unwrap_or(0.0),
                                &knobs,
                            ).await;
                            policy_note = outcome.summary_line();
                        }
                    }
                }

                // Human-facing prose (Telegram/log) without the machine block;
                // the DB record keeps the full raw response for the audit trail.
                let prose = llm_patch::strip_proposal_block(&analysis);
                let prose = match &policy_note {
                    Some(note) => format!("{prose}\n\n{note}"),
                    None => prose,
                };

                // Persist to SQLite — tagged with current session_id so the
                // Control Tower can mark prior-session recommendations as stale.
                // Write to primary pool (main CAG dashboard reads from there).
                db::record_llm_recommendation(
                    &primary_pool,
                    provider.model(),
                    total_trade_count as i64,
                    current_pnl,
                    &analysis,
                ).await;

                // Telegram has a 4096-char limit per message; truncate with notice if needed.
                // Name the squadron: with a pass per squadron, an unlabeled
                // Telegram report cannot be told apart from the others.
                let prose = format!("[{squadron_id} · {market_class}]\n{prose}");
                let message = if prose.len() > 4000 {
                    format!("{}\n\n[truncated — full response in logs]", truncate_on_char_boundary(&prose, 3980))
                } else {
                    prose.clone()
                };

                if !tg_token.is_empty() && !tg_chat_id.is_empty() {
                    match notifications::send_notification(&tg_token, &tg_chat_id, &message).await {
                        Ok(_) => info!("🤖 LLM Advisor: recommendations sent to Telegram ✅"),
                        Err(e) => error!("🤖 LLM Advisor: Telegram send failed: {}", e),
                    }
                } else {
                    warn!("🤖 LLM Advisor: no Telegram creds set (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)");
                    info!("🤖 LLM Advisor output:\n{}", prose);
                }
            }
            None => {
                let durable = advisor_error_is_durable(&last_err.to_string());
                error!(
                    "🤖 LLM Advisor: [{}] {} call failed after {} retries ({}@{}): {}",
                    squadron_id, provider.name(), MAX_RETRIES, provider.model(),
                    provider.display_url(), last_err
                );
                if durable {
                    let mut block = match advisor_hard_block().lock() {
                        Ok(g) => g,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    if block.is_none() {
                        *block = Some((
                            last_err.to_string(),
                            config::LLM_ADVISOR_HARD_FAIL_BACKOFF_CYCLES,
                        ));
                        error!(
                            "🤖 LLM Advisor: that failure is durable, not transient — standing \
                             down for {} cycle(s). Fix the provider credentials or billing; no \
                             restart needed.",
                            config::LLM_ADVISOR_HARD_FAIL_BACKOFF_CYCLES,
                        );
                    }
                }
            }
        }
        } // end per-squadron pass
    }
}

#[cfg(test)]
mod prompt_cache_tests {
    use super::*;

    /// The system prompt must be byte-identical every call, or the cache
    /// breakpoint is worthless: caching is a prefix match, so one differing byte
    /// invalidates everything after it. This is the property that makes the
    /// whole optimization work, and it would be quietly lost the first time
    /// someone interpolates a timestamp or a session id into the prompt.
    #[test]
    fn the_system_prompt_is_byte_identical_across_calls() {
        assert_eq!(system_prompt(), system_prompt());
        assert_eq!(system_prompt(), system_prompt(), "second comparison, for a time-dependent value");
    }

    /// It must also clear the model's minimum cacheable prefix or the marker is
    /// silently ignored. 1,024 tokens is the `claude-sonnet-5` minimum, the
    /// shipped default; four chars per token is the conventional rough estimate
    /// and deliberately conservative here.
    #[test]
    fn the_system_prompt_is_long_enough_to_cache() {
        let approx_tokens = system_prompt().len() / 4;
        assert!(
            approx_tokens > 1100,
            "system prompt is ~{approx_tokens} tokens — too close to the 1024-token \
             minimum for claude-sonnet-5 to cache reliably",
        );
    }

    /// The request must carry exactly one breakpoint, on the system block.
    #[test]
    fn the_request_marks_the_system_block_for_caching() {
        let req = AnthropicRequest {
            model: "claude-sonnet-5".into(),
            system: vec![AnthropicSystemBlock {
                kind: "text",
                text: system_prompt(),
                cache_control: Some(AnthropicCacheControl { kind: "ephemeral" }),
            }],
            messages: vec![],
            max_tokens: 100,
            temperature: None,
        };
        let v = serde_json::to_value(&req).expect("serializes");
        assert_eq!(v["system"][0]["type"], "text");
        assert_eq!(
            v["system"][0]["cache_control"]["type"], "ephemeral",
            "the system block must carry the cache breakpoint",
        );
    }

    /// Absent usage must not break parsing — some providers omit it entirely.
    #[test]
    fn a_response_without_usage_still_parses() {
        let r: AnthropicResponse = serde_json::from_str(
            r#"{"content":[{"text":"hi"}],"stop_reason":"end_turn"}"#
        ).expect("parses without a usage field");
        assert_eq!(r.usage.cache_read_input_tokens, 0);
    }

    #[test]
    fn cache_counters_are_read_when_present() {
        let r: AnthropicResponse = serde_json::from_str(
            r#"{"content":[{"text":"hi"}],"stop_reason":"end_turn",
                "usage":{"input_tokens":5000,"cache_creation_input_tokens":1650,
                         "cache_read_input_tokens":1650}}"#
        ).expect("parses");
        assert_eq!(r.usage.cache_creation_input_tokens, 1650);
        assert_eq!(r.usage.cache_read_input_tokens, 1650);
    }
}

#[cfg(test)]
mod base_url_tests {
    use super::normalize_llm_base;

    /// The exact failure from QA: the Anthropic docs publish the *endpoint*, so
    /// that is what gets pasted, and the caller then appended its own path —
    /// yielding `/v1/messages/v1/messages` and a 404 that explained nothing.
    #[test]
    fn the_documented_anthropic_endpoint_is_accepted() {
        for input in [
            "https://api.anthropic.com/v1/messages",
            "https://api.anthropic.com/v1/messages/",
            "https://api.anthropic.com/v1",
            "https://api.anthropic.com/",
            "https://api.anthropic.com",
            "  https://api.anthropic.com/v1/messages  ",
        ] {
            assert_eq!(
                normalize_llm_base(input, "anthropic"),
                "https://api.anthropic.com",
                "input {input:?}",
            );
        }
    }

    /// OpenAI's base keeps its `/v1` — the caller appends only `/models`.
    #[test]
    fn openai_keeps_its_v1_prefix() {
        for input in [
            "https://api.openai.com/v1",
            "https://api.openai.com/v1/",
            "https://api.openai.com/v1/chat/completions",
            "https://api.openai.com/v1/models",
        ] {
            assert_eq!(
                normalize_llm_base(input, "openai"),
                "https://api.openai.com/v1",
                "input {input:?}",
            );
        }
    }

    /// Self-hosted gateways sit behind arbitrary prefixes. Nothing may be
    /// appended and no prefix removed, or a working proxy config breaks.
    #[test]
    fn a_self_hosted_gateway_prefix_is_left_alone() {
        assert_eq!(
            normalize_llm_base("https://gw.internal/llm/openai/v1", "openai"),
            "https://gw.internal/llm/openai/v1",
        );
        assert_eq!(
            normalize_llm_base("https://gw.internal/llm/anthropic", "anthropic"),
            "https://gw.internal/llm/anthropic",
        );
    }
}

#[cfg(test)]
mod anthropic_sampling_tests {
    use super::anthropic_accepts_temperature;

    /// The model that broke the advisor. Anthropic answers a request carrying
    /// `temperature` with a 400 rather than ignoring the field, so this must be
    /// omitted, not merely tolerated.
    #[test]
    fn sonnet_5_does_not_accept_temperature() {
        assert!(!anthropic_accepts_temperature("claude-sonnet-5"));
    }

    /// The rest of the families that removed sampling parameters.
    #[test]
    fn the_newer_families_do_not_accept_temperature() {
        for m in ["claude-opus-5", "claude-opus-4-8", "claude-opus-4-7", "claude-fable-5"] {
            assert!(!anthropic_accepts_temperature(m), "{m} should not be sent temperature");
        }
    }

    /// Models that still accept it keep getting the deliberately low value, so
    /// an operator on an older model does not silently lose response
    /// consistency to this fix.
    #[test]
    fn older_families_still_accept_temperature() {
        for m in [
            "claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5",
            "claude-sonnet-4-5", "claude-3-5-sonnet-20241022",
        ] {
            assert!(anthropic_accepts_temperature(m), "{m} should still be sent temperature");
        }
    }

    /// An unrecognized model omits the parameter rather than sending it. This
    /// is the whole point of an allow-list: the unknown model of the future is
    /// far likelier to reject sampling than to accept it, and omitting costs
    /// only default sampling where sending costs the entire advisor.
    #[test]
    fn an_unknown_model_omits_temperature() {
        assert!(!anthropic_accepts_temperature("claude-something-7"));
        assert!(!anthropic_accepts_temperature(""));
    }

    /// Operators type the model into a config field, so surrounding whitespace
    /// and casing must not decide whether the request is well-formed.
    #[test]
    fn matching_ignores_case_and_whitespace() {
        assert!(anthropic_accepts_temperature("  claude-haiku-4-5  "));
        assert!(anthropic_accepts_temperature("Claude-Haiku-4-5"));
        assert!(!anthropic_accepts_temperature("  claude-sonnet-5  "));
    }
}

#[cfg(test)]
mod truncation_tests {
    use super::truncate_on_char_boundary;

    /// The advisor formats market names into a fixed-width table by slicing
    /// them at byte 32. `&s[..32]` panics when that byte lands inside a
    /// multi-byte character, and prediction markets are full of accented
    /// names — this one panicked the advisor task in a live session.
    #[test]
    fn a_multibyte_character_across_the_cut_does_not_panic() {
        let market = "US Open: Jarrý vs Tsitsipás — who wins?";
        assert!(!market.is_char_boundary(32), "this name must straddle byte 32 to be a regression test");
        let out = truncate_on_char_boundary(market, 32);
        assert!(out.len() <= 32);
        assert!(market.starts_with(out), "truncation must be a prefix");
    }

    #[test]
    fn short_strings_are_returned_whole() {
        assert_eq!(truncate_on_char_boundary("Will Renan win?", 32), "Will Renan win?");
    }

    /// Model prose is truncated at ~4000 bytes and is dense with em dashes.
    #[test]
    fn model_prose_truncates_cleanly() {
        let prose = "—".repeat(2000);
        let out = truncate_on_char_boundary(&prose, 3980);
        assert!(out.len() <= 3980);
        assert!(prose.starts_with(out));
    }
}

#[cfg(test)]
mod advisor_call_economy_tests {
    use super::{advisor_error_is_durable, advisor_should_call};
    use crate::config;

    /// The Ireland case: 33 provider calls in 4.5 hours, 24 of them against
    /// byte-identical input. An unchanged fingerprint must not buy a call.
    #[test]
    fn unchanged_inputs_do_not_buy_a_call() {
        let sq = "economy-test-unchanged";
        assert!(advisor_should_call(sq, 111), "first sight of a state must analyze it");
        assert!(!advisor_should_call(sq, 111));
        assert!(!advisor_should_call(sq, 111));
    }

    /// A new trade, a P&L move or a config change must reach the advisor at once.
    #[test]
    fn changed_inputs_call_immediately() {
        let sq = "economy-test-changed";
        assert!(advisor_should_call(sq, 200));
        assert!(!advisor_should_call(sq, 200));
        assert!(advisor_should_call(sq, 201), "a changed fingerprint must not be skipped");
    }

    /// The skip is a rate limit, not a mute. Market conditions move underneath an
    /// idle engine, which is exactly what the original always-call behavior
    /// protected, so a forced refresh has to arrive on schedule.
    #[test]
    fn an_unchanged_squadron_is_still_refreshed_eventually() {
        let sq = "economy-test-refresh";
        assert!(advisor_should_call(sq, 300));
        for i in 0..config::LLM_ADVISOR_MAX_SKIPPED_CYCLES {
            assert!(!advisor_should_call(sq, 300), "skip {} came too early", i);
        }
        assert!(advisor_should_call(sq, 300), "forced refresh never arrived");
    }

    /// Squadrons are tracked independently, so a quiet one cannot suppress a busy one.
    #[test]
    fn squadrons_are_tracked_independently() {
        assert!(advisor_should_call("economy-test-a", 400));
        assert!(advisor_should_call("economy-test-b", 400));
        assert!(!advisor_should_call("economy-test-a", 400));
    }

    /// The exact body that stood the advisor down on 2026-08-28.
    #[test]
    fn a_billing_failure_is_durable() {
        assert!(advisor_error_is_durable(
            "Anthropic HTTP 400 Bad Request: {\"type\":\"error\",\"error\":{\"type\":\
             \"invalid_request_error\",\"message\":\"Your credit balance is too low to access \
             the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits.\"}}"
        ));
    }

    /// Bad or missing credentials are durable too.
    #[test]
    fn auth_failures_are_durable() {
        assert!(advisor_error_is_durable("401 Unauthorized"));
        assert!(advisor_error_is_durable("authentication_error: invalid x-api-key"));
        assert!(advisor_error_is_durable("403 Forbidden"));
    }

    /// Transient failures must NOT arm the backoff, or a blip would silence the
    /// advisor for twelve cycles.
    #[test]
    fn transient_failures_are_not_durable() {
        assert!(!advisor_error_is_durable("error sending request: connection timed out"));
        assert!(!advisor_error_is_durable("HTTP 529 overloaded_error"));
        assert!(!advisor_error_is_durable("HTTP 500 Internal Server Error"));
        assert!(!advisor_error_is_durable("429 rate_limit_error"));
    }
}
