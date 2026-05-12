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
use crate::helpers::{db, notifications};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Minimum number of current-session trades before the LLM Advisor fires its
/// first analysis for that session.  Below this threshold the analysis would be
/// too thin to be useful, so the advisor waits for more data.
const LLM_ADVISOR_MIN_SESSION_TRADES: usize = 5;

/// When the current session has fewer than LLM_ADVISOR_MIN_SESSION_TRADES trades,
/// supplement with this many trades from prior sessions as context.
const LLM_ADVISOR_PRIOR_SESSION_SUPPLEMENT: i64 = 15;

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
    /// Limit output tokens so Telegram messages stay readable.
    num_predict: u32,
    temperature: f32,
}

#[derive(Serialize, Deserialize, Clone)]
struct OllamaMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct OllamaResponse {
    message: OllamaMessage,
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
                 from 14 orderbook + oracle features.  Retrained every 30s.

== Key OBI Concept ==
OBI (Order Book Imbalance) = (bid_depth − ask_depth) / total_depth  ∈ [−1, +1]
Negative OBI on a token means the ask side dominates → smart money is selling.
Entering YES when OBI_y is strongly negative is entering against the book.

== Tunable Parameters (DynamicConfig) ==
These can be adjusted live without restarting the bot:
  Momentum: stop_loss_pct, target_profit_pct, min/max_trade_size_usdc, max_exposure
  Maker:     max_entry_price, stop_loss_pct, target_profit_pct, max_exposure
  Basis:     stop_loss_pct, target_profit_pct, max_exposure
  GBoost:    entry_threshold (0–1), stop_loss_pct, target_profit_pct, max_exposure
  TimeDecay: position_size_usdc, stop_loss_pct, max_entry_price, obi_adverse_block
  Global:    ghost_mode (true = paper trading, no real orders)
  Enable flags: enable_momentum, enable_maker, enable_basis, enable_gboost,
                enable_time_decay, enable_arbitrage

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

Keep the entire response under 400 words."#.to_string()
}

// ── Prompt builder ────────────────────────────────────────────────────────────

/// Format recent trades + session stats into a concise user prompt.
/// Accepts both current-session trades and optional prior-session context.
fn build_user_prompt(
    session_trades: &[db::TradeRow],
    prior_trades: Option<&[db::TradeRow]>,
    session_pnl: rust_decimal::Decimal,
    starting_collateral: rust_decimal::Decimal,
    session_id: &str,
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
    lines.push("Please analyse the above and provide recommendations as instructed.".to_string());

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
) -> anyhow::Result<String> {
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
            num_predict: 450,
            temperature: 0.3, // Low temperature: consistent, factual recommendations
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
    Ok(ollama_resp.message.content.trim().to_string())
}

// ── Main advisor loop ─────────────────────────────────────────────────────────

/// Spawn this as a long-running tokio task at startup.
///
/// The task immediately checks ENABLE_LLM_ADVISOR and exits early if disabled,
/// so there is no cost to always registering it in main.rs.
pub async fn run_llm_advisor_loop(
    tg_token: String,
    tg_chat_id: String,
    session_pnl: Arc<Mutex<rust_decimal::Decimal>>,
    starting_collateral: Arc<Mutex<rust_decimal::Decimal>>,
) {
    if !config::ENABLE_LLM_ADVISOR {
        info!("🤖 LLM Advisor: disabled (set ENABLE_LLM_ADVISOR = true in config.rs to activate)");
        return;
    }

    // Resolve Ollama connection settings — env vars override compile-time defaults.
    let ollama_url = std::env::var("OLLAMA_URL")
        .unwrap_or_else(|_| config::LLM_OLLAMA_URL.to_string());
    let ollama_model = std::env::var("OLLAMA_MODEL")
        .unwrap_or_else(|_| config::LLM_OLLAMA_MODEL.to_string());

    info!(
        "🤖 LLM Advisor started — model: {} @ {} | interval: {}s | session: {}",
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
    //   connect_timeout: 10 s (fast TCP failure; prevents silent 6-min hangs)
    //   timeout:        360 s (CPU inference for a 7B model on t3.large: 3–6 min)
    //
    // Previously only a single 360 s total timeout was set with no connect_timeout.
    // When the ollama container was unreachable (TCP accepted but silent), reqwest
    // waited the full 360 s before surfacing the error — tying up the advisor loop
    // for 6 minutes every cycle.
    let probe_client = Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    let http_client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(360))
        .build()
        .unwrap_or_default();

    // Skip the first tick so we don't fire immediately at startup before any
    // trades have occurred.
    let mut ticker = interval(Duration::from_secs(config::LLM_ADVISOR_INTERVAL_SECS));
    ticker.tick().await;

    loop {
        ticker.tick().await;

        // ── Gather data ───────────────────────────────────────────────────────
        let pool = match db::pool() {
            Some(p) => p,
            None => {
                warn!("🤖 LLM Advisor: DB pool not available, skipping cycle");
                continue;
            }
        };

        // Primary: current-session trades only
        let session_trades = db::get_session_trades(pool).await;
        let session_id = db::current_session_id().to_string();

        // Determine supplemental context: if current session is thin, pull prior session.
        // NOTE: We always proceed to the LLM call — 0 trades is itself a meaningful signal
        // (settings may be too stringent for the current market).  Prior session data is
        // appended as supplemental context when available; absence of it is not a blocker.
        let prior_trades: Option<Vec<db::TradeRow>> =
            if session_trades.len() < LLM_ADVISOR_MIN_SESSION_TRADES {
                let prior = db::get_previous_session_trades(pool, LLM_ADVISOR_PRIOR_SESSION_SUPPLEMENT).await;
                if prior.is_empty() {
                    if session_trades.is_empty() {
                        info!(
                            "🤖 LLM Advisor: 0 session trades, no prior history — \
                             firing with market-conditions / settings-stringency prompt"
                        );
                    } else {
                        info!(
                            "🤖 LLM Advisor: only {} session trades (min {}), no prior session data — \
                             proceeding with thin-data analysis",
                            session_trades.len(), LLM_ADVISOR_MIN_SESSION_TRADES
                        );
                    }
                    None // proceed without supplemental context
                } else {
                    info!(
                        "🤖 LLM Advisor: {} session trades (below min {}), supplementing with {} prior-session trades",
                        session_trades.len(), LLM_ADVISOR_MIN_SESSION_TRADES, prior.len()
                    );
                    Some(prior)
                }
            } else {
                None
            };

        let current_pnl = *session_pnl.lock().await;
        let collateral = *starting_collateral.lock().await;

        let session_trade_count = session_trades.len();
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
        let user_prompt = build_user_prompt(
            &session_trades,
            prior_trades.as_deref(),
            current_pnl,
            collateral,
            &session_id,
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
                Ok(text) => {
                    analysis_opt = Some(text);
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
                if let Some(pool) = db::pool() {
                    db::record_llm_recommendation(
                        pool,
                        &ollama_model,
                        total_trade_count as i64,
                        current_pnl,
                        &analysis,
                    ).await;
                }

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

