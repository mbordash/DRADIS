/// LLM Advisor — periodic trade analysis with Ollama + Telegram recommendations.
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
///                    LLM_ADVISOR_TRADES_LOOKBACK, LLM_OLLAMA_URL, LLM_OLLAMA_MODEL
///   env overrides:   OLLAMA_URL, OLLAMA_MODEL  (override the defaults above)
///   Telegram creds:  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID  (same as rest of bot)
///
/// ── Bring Your Own LLM ───────────────────────────────────────────────────────
/// The advisor uses the Ollama /api/chat endpoint (OpenAI-compatible).
/// Any model running in Ollama works.  Recommended: llama3.2, mistral, qwen2.5.
/// Point OLLAMA_URL at a remote host for GPU-accelerated inference when running
/// DRADIS on a headless cloud VPS.

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

// ── Ollama API types ──────────────────────────────────────────────────────────

#[derive(Serialize)]
struct OllamaRequest {
    model: String,
    messages: Vec<OllamaMessage>,
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
struct OllamaMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct OllamaResponse {
    message: OllamaMessage,
    done_reason: Option<String>,
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
7. TRENDCAPTURE — Exploits sustained oracle drift on Window/Daily markets.
                  Buys YES (BULL) or NO (BEAR) when 10-min and 60-min drift both
                  exceed asset-specific thresholds.  One-sided, maker venue, uses
                  Kelly-fractional sizing.  Exits via TP, dynamic SL (tighter near
                  expiry), trend-reversal signal, or near-expiry forced exit.
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

Keep the entire response under 250 words."#.to_string()
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

    lines.join("\n")
}

// ── Ollama API call ───────────────────────────────────────────────────────────

/// Quick reachability probe: GET /api/tags with a short timeout.
/// Returns Ok(()) if Ollama is up, Err otherwise.
async fn probe_ollama(probe_client: &Client, ollama_base_url: &str) -> anyhow::Result<()> {
    let url = format!("{}/api/tags", ollama_base_url.trim_end_matches('/'));
    let resp = probe_client.get(&url).send().await?;
    if resp.status().is_success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("Ollama /api/tags returned HTTP {}", resp.status()))
    }
}

async fn call_ollama(
    client: &Client,
    ollama_base_url: &str,
    model: &str,
    user_prompt: &str,
) -> anyhow::Result<OllamaResponse> {
    let url = format!("{}/api/chat", ollama_base_url.trim_end_matches('/'));

    let request = OllamaRequest {
        model: model.to_string(),
        messages: vec![
            OllamaMessage {
                role: "system".to_string(),
                content: system_prompt(),
            },
            OllamaMessage {
                role: "user".to_string(),
                content: user_prompt.to_string(),
            },
        ],
        stream: false,
        options: OllamaOptions {
            num_predict: 900,
            temperature: 0.3, // Low temperature: consistent, factual recommendations
            num_ctx: 3072,     // Room for prompt + full recommendation without frequent length stops
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

    let mut ollama_resp: OllamaResponse = resp.json().await?;
    ollama_resp.message.content = ollama_resp.message.content.trim().to_string();
    Ok(ollama_resp)
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

    // Resolve Ollama connection settings — env vars override compile-time defaults.
    let ollama_url = std::env::var("OLLAMA_URL")
        .unwrap_or_else(|_| config::LLM_OLLAMA_URL.to_string());
    let ollama_model = std::env::var("OLLAMA_MODEL")
        .unwrap_or_else(|_| config::LLM_OLLAMA_MODEL.to_string());

    info!(
        "🤖 LLM Advisor started (CAG multi-asset mode) — model: {} @ {} | interval: {}s | session: {}",
        ollama_model,
        ollama_url,
        config::LLM_ADVISOR_INTERVAL_SECS,
        db::current_session_id(),
    );

    // Two HTTP clients with different timeout profiles:
    //
    // probe_client — used for the pre-flight GET /api/tags health-check.
    //   connect_timeout: 5 s  (fail fast if the container/host is unreachable)
    //   timeout:        10 s  (total; /api/tags returns in <1 s when healthy)
    //
    // inference_client — used for the actual POST /api/chat.
    //   connect_timeout: 10 s  (fast TCP failure; prevents silent hangs)
    //   timeout:        480 s  (LLM_INFERENCE_TIMEOUT_SECS — measured on t3.large
    //                           qwen2.5:3b takes ~360–400s; 480s gives a 20% buffer)
    //
    // Previously the timeout was 360s — exactly at the model's natural completion
    // time — causing every first attempt to time out, triggering needless retry cycles
    // that collectively consumed 12–17 min of a 30-min advisory interval.
    let probe_client = Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    let http_client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(config::LLM_INFERENCE_TIMEOUT_SECS))
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

        let session_trade_count = all_session_trades.len();
        let total_trade_count = session_trade_count
            + prior_trades.as_ref().map(|p| p.len()).unwrap_or(0);

        // ── Pre-flight: verify Ollama is reachable before a 6-min inference call ──
        // Uses the fast probe_client (5 s connect / 10 s total).
        // On failure we skip this cycle entirely rather than blocking the loop.
        if let Err(e) = probe_ollama(&probe_client, &ollama_url).await {
            warn!(
                "🤖 LLM Advisor: Ollama unreachable at {} — skipping cycle ({})",
                ollama_url, e
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
            "🤖 LLM Advisor: calling {} for session {} ({} session + {} prior trades, P&L ${:.2})...",
            ollama_model,
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
            match call_ollama(&http_client, &ollama_url, &ollama_model, &user_prompt).await {
                Ok(resp) => {
                    if matches!(resp.done_reason.as_deref(), Some("length")) {
                        warn!(
                            "🤖 LLM Advisor: output hit Ollama length cap (num_predict={}) — consider increasing if recommendations still end mid-thought",
                            900,
                        );
                    }
                    analysis_opt = Some(resp.message.content);
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

                // Persist to SQLite — tagged with current session_id so the
                // Control Tower can mark prior-session recommendations as stale.
                // Write to primary pool (main CAG dashboard reads from there).
                db::record_llm_recommendation(
                    &primary_pool,
                    &ollama_model,
                    total_trade_count as i64,
                    current_pnl,
                    &analysis,
                ).await;

                // Telegram has a 4096-char limit per message; truncate with notice if needed.
                let message = if analysis.len() > 4000 {
                    format!("{}\n\n[truncated — full response in logs]", &analysis[..3980])
                } else {
                    analysis.clone()
                };

                if !tg_token.is_empty() && !tg_chat_id.is_empty() {
                    match notifications::send_notification(&tg_token, &tg_chat_id, &message).await {
                        Ok(_) => info!("🤖 LLM Advisor: recommendations sent to Telegram ✅"),
                        Err(e) => error!("🤖 LLM Advisor: Telegram send failed: {}", e),
                    }
                } else {
                    warn!("🤖 LLM Advisor: no Telegram creds set (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)");
                    info!("🤖 LLM Advisor output:\n{}", analysis);
                }
            }
            None => {
                error!(
                    "🤖 LLM Advisor: Ollama call failed after {} retries ({}@{}): {}",
                    MAX_RETRIES, ollama_model, ollama_url, last_err
                );
            }
        }
    }
}

