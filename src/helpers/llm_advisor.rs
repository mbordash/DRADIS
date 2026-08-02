/// LLM Advisor — periodic trade analysis with pluggable LLM providers + Telegram recommendations.
///
/// ── Overview ─────────────────────────────────────────────────────────────────
/// A background task that wakes every LLM_ADVISOR_INTERVAL_SECS and analyses
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
use tracing::{error, info, warn};

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

// ── Ollama API types ──────────────────────────────────────────────────────────

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
                base_url: std::env::var("LLM_API_BASE")
                    .unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
                api_key: require_api_key(&provider)?,
                model: require_model(&provider)?,
            }),
            "anthropic" => Ok(Self::Anthropic {
                base_url: std::env::var("LLM_API_BASE")
                    .unwrap_or_else(|_| "https://api.anthropic.com".to_string()),
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
    system: String,
    messages: Vec<ChatMessage>,
    max_tokens: u32,
    temperature: f32,
}

#[derive(Deserialize)]
struct AnthropicResponse {
    content: Vec<AnthropicContent>,
    stop_reason: Option<String>,
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
Analyse the recent trades (or absence of trades) provided and:
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
Session P&L: [value]  |  Trades analysed: [n]

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
) -> String {
    let mut lines = Vec::new();

    lines.push(format!(
        "Session: {}  |  P&L: ${:.2}  |  Starting collateral: ${:.2}  |  Session trades: {}",
        &session_id[..16.min(session_id.len())], // trim to readable date+time prefix
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
                if t.market.len() > 32 { &t.market[..32] } else { &t.market },
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
                    &t.ts[..16.min(t.ts.len())],
                    t.strategy.replace("Strategy", ""),
                    if t.market.len() > 32 { &t.market[..32] } else { &t.market },
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
        "Momentum: stop_loss={:.0}%, target_profit={:.0}%, min_trade=${}, max_trade=${}, max_exposure=${}",
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
        "Maker: max_entry=${}, min_entry=${}, stop_loss={:.0}%, target_profit={:.0}%, max_exposure=${}",
        dyn_cfg.maker_max_entry_price, dyn_cfg.maker_min_entry_price,
        dyn_cfg.maker_stop_loss_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.maker_target_profit_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.maker_max_exposure_usdc,
    ));
    lines.push(format!(
        "GBoost: entry_threshold={}, stop_loss={:.0}%, target_profit={:.0}%, max_exposure=${}",
        dyn_cfg.gboost_entry_threshold,
        dyn_cfg.gboost_stop_loss_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.gboost_target_profit_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.gboost_max_exposure_usdc,
    ));
    lines.push(format!(
        "Basis: stop_loss={:.0}%, target_profit={:.0}%, max_exposure=${}",
        dyn_cfg.basis_stop_loss_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.basis_target_profit_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.basis_max_exposure_usdc,
    ));
    lines.push(format!(
        "TimeDecay: position_size=${}, stop_loss={:.0}%, max_entry=${}, max_exposure=${}",
        dyn_cfg.time_decay_position_size_usdc,
        dyn_cfg.time_decay_stop_loss_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.time_decay_max_entry_price,
        dyn_cfg.time_decay_max_exposure_usdc,
    ));
    lines.push(format!(
        "TrendCapture: stop_loss={:.0}%, target_profit={:.0}%, min_trade=${}, max_trade=${}, max_entry=${}, max_exposure=${}",
        dyn_cfg.trendcapture_stop_loss_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.trendcapture_target_profit_pct * rust_decimal::Decimal::ONE_HUNDRED,
        dyn_cfg.trendcapture_min_trade_size_usdc,
        dyn_cfg.trendcapture_max_trade_size_usdc,
        dyn_cfg.trendcapture_max_entry_price,
        dyn_cfg.trendcapture_max_exposure_usdc,
    ));

    lines.push(String::new());
    lines.push("Please analyse the above and provide recommendations as instructed.".to_string());

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
                if p.market.len() > 32 { &p.market[..32] } else { &p.market },
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

    lines.join("\n")
}

// ── Provider API calls ────────────────────────────────────────────────────────

/// Output token cap shared by all providers (Ollama `num_predict`,
/// OpenAI/Anthropic `max_tokens`) — sized for the ≤250-word structured report.
const LLM_MAX_OUTPUT_TOKENS: u32 = 900;
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
            num_predict: LLM_MAX_OUTPUT_TOKENS,
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
        max_tokens: LLM_MAX_OUTPUT_TOKENS,
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

    let request = AnthropicRequest {
        model: model.to_string(),
        system: system_prompt(),
        messages: vec![ChatMessage { role: "user".to_string(), content: user_prompt.to_string() }],
        max_tokens: LLM_MAX_OUTPUT_TOKENS,
        temperature: LLM_TEMPERATURE,
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
    if content.is_empty() {
        return Err(anyhow::anyhow!("Anthropic response had no text content"));
    }
    Ok(LlmReply {
        content,
        truncated_by_length: matches!(parsed.stop_reason.as_deref(), Some("max_tokens")),
    })
}

// ── Main advisor loop ─────────────────────────────────────────────────────────

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
    session_pnl: Arc<Mutex<rust_decimal::Decimal>>,
    starting_collateral: Arc<Mutex<rust_decimal::Decimal>>,
    mut config_rx: tokio::sync::watch::Receiver<Arc<DynamicConfig>>,
    config_tx: Arc<tokio::sync::watch::Sender<Arc<DynamicConfig>>>,
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

        let current_pnl = *session_pnl.lock().await;
        let collateral = *starting_collateral.lock().await;
        let dyn_cfg = config_rx.borrow_and_update().clone();

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
        );

        info!(
            "🤖 LLM Advisor: calling {} ({}) for session {} ({} session + {} prior trades, P&L ${:.2})...",
            provider.model(),
            provider.name(),
            &session_id[..16.min(session_id.len())],
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
                            LLM_MAX_OUTPUT_TOKENS,
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
                            ).await;
                            info!(
                                "🤖 LLM Advisor: {} recorded {} proposed / {} rejected change(s) (tier {})",
                                batch_id, ids.len(), batch.rejected.len(), autonomy_tier(),
                            );

                            // ── Policy engine (S3): tier enforcement ─────────
                            use rust_decimal::prelude::ToPrimitive;
                            let knobs = llm_policy::PolicyKnobs::from_env();
                            let outcome = llm_policy::enforce_batch(
                                &primary_pool,
                                &batch,
                                &ids,
                                autonomy_tier(),
                                &config_tx,
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
                let message = if prose.len() > 4000 {
                    format!("{}\n\n[truncated — full response in logs]", &prose[..3980])
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
                error!(
                    "🤖 LLM Advisor: {} call failed after {} retries ({}@{}): {}",
                    provider.name(), MAX_RETRIES, provider.model(), provider.display_url(), last_err
                );
            }
        }
    }
}

