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

/// Control Tower REST API
///
/// Endpoints
/// ─────────────────────────────────────────────────────────────────────────────
///   GET    /api/health                — liveness check
///   GET    /api/assets                — list of initialized asset pools (Phase 3f-7)
///   GET    /api/config                — current DynamicConfig as JSON
///   PATCH  /api/config               — JSON merge-patch; hot-reloads strategies
///   GET    /api/config/schema         — editable-config field schema (drives Advanced UI)
///   GET    /api/pnl/history           — recent P&L snapshots  (?limit=200&asset=btc)
///   GET    /api/trades                — recent completed trades (?limit=100&asset=btc)
///   GET    /api/positions             — current open positions (?asset=btc)
///   DELETE /api/positions/{token_id}  — purge a specific stale row from open_positions
///   POST   /api/positions/sync        — trigger immediate chain-sync against Polymarket wallet
///   POST   /api/positions/manual-exit — manual "Return to Base" exit (FAK market sell)
///   GET    /api/llm/recommendations   — recent LLM Advisor analyzes (?limit=10&asset=btc)
///   GET    /api/squadrons             — list all active squadrons (Phase 3d)
///   GET    /api/squadrons/{id}        — get one squadron by id    (Phase 3d)
///
/// All data endpoints accept an optional `?asset=btc` query param (Phase 3f-7).
/// When absent, the primary (first initialized) asset pool is used.
///
/// The server binds to 0.0.0.0:$API_PORT (default 9000).
/// CORS is open so the Next.js Control Tower on any port can reach it.

use axum::{
    Router,
    routing::{get, delete},
    extract::{State, Query, Path, Path as AxumPath, Request},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
    http::{StatusCode, Method, header},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info, warn};
#[cfg(feature = "intl_clob")]
use alloy::primitives::{Address, U256};
#[cfg(feature = "intl_clob")]
use polymarket_client_sdk_v2::clob::types::{Side};
#[cfg(feature = "intl_clob")]
use polymarket_client_sdk_v2::clob::types::request::PriceRequest;
use rust_decimal::Decimal;
#[cfg(feature = "intl_clob")]
use rust_decimal::prelude::FromStr;

use crate::helpers::dynamic_config::DynamicConfig;
use crate::helpers::db;
#[cfg(feature = "intl_clob")]
use crate::helpers::orders::place_limit_order_filled;
// Only `manual_exit` uses this, and that handler is intl-only.
#[cfg(feature = "intl_clob")]
use rust_decimal_macros::dec;
#[cfg(feature = "intl_clob")]
use crate::helpers::price::round_to_tick_size;
#[cfg(feature = "intl_clob")]
use crate::helpers::metrics;
#[cfg(feature = "intl_clob")]
use crate::tasks::cleanup::sync_open_positions_with_chain;
use crate::cag::Cag;

// ─── Raptor health types ──────────────────────────────────────────────────────

/// A market whose order-book feed has stopped arriving.
#[derive(Debug, Clone, Serialize)]
pub struct DarkFeed {
    /// The asset whose feed is dark ("btc").
    pub market: String,
    /// The specific market it was following, when known.
    ///
    /// Without this the banner could only say "btc", which reads as a broken
    /// connection even when every other market on that asset is trading fine.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub market_name: String,
    pub dark_for_secs: u64,
}

/// Connection health for a single asset's pair of Binance Raptors.
///
/// `price_connected`   — true when the Price Raptor WebSocket is live and
///                       delivering ticker messages from Binance Spot.
/// `funding_connected` — true when the Funding Raptor last polled
///                       Binance FAPI successfully.
///
/// The remaining fields carry the **latest signal values** broadcast by each
/// Raptor, so the Control Tower Telemetry view can graph them without a
/// separate persistence layer (the frontend builds its own rolling buffer).
/// All default to `0` until the first tick arrives.
#[derive(Serialize, Clone, Default, Debug)]
pub struct AssetRaptorHealth {
    pub price_connected:   bool,
    pub funding_connected: bool,
    pub deriv_connected:   bool,

    // ── Live Price Raptor signal snapshot (Binance Spot WS) ────────────────
    /// Current spot price (oracle).
    pub oracle_price:  Decimal,
    /// 5-second price velocity (Δprice over the 5s window).
    pub velocity_5s:   Decimal,
    /// 1-second price velocity (short window).
    pub velocity_1s:   Decimal,
    /// Acceleration — rate of change of 5s velocity.
    pub acceleration:  Decimal,
    /// 60-minute drift (Δprice over the trailing hour).
    pub drift_60m:     Decimal,
    /// 10-minute drift (fills the 5s–60m temporal gap).
    pub drift_10m:     Decimal,

    // ── Live Funding Raptor signal snapshot (Binance FAPI) ─────────────────
    /// Perpetual funding rate (fraction; ×100 for percent).
    pub funding_rate:  Decimal,

    // ── Live Derivatives Raptor signal snapshot (Binance FAPI) ─────────────
    /// Open interest (base contracts) — raw perp positioning size.
    pub open_interest: Decimal,
    /// Fractional change in open interest since the previous poll (×100 = %).
    pub oi_delta_pct:  Decimal,
    /// Taker buy÷sell volume ratio (CVD proxy); >1 buy aggression, 0 = no data.
    pub cvd_ratio:     Decimal,

    // ── Live Tide Raptor signal snapshot (synthetic iNAV vs IEX ETF prints) ──
    /// Tide Raptor has at least one fresh, in-session ETF premium this tick.
    pub tide_connected:      bool,
    /// True during the US cash session (09:30–16:00 ET); false ⇒ pulse held 0.
    pub tide_market_open:    bool,
    /// Volume-weighted, vol-normalized aggregate premium z-score (signed).
    pub institutional_pulse: Decimal,
    /// Agreement of the Big Three premium signs (0..1); high = conviction.
    pub tide_coherence:      Decimal,
    /// Per-ETF premium vs synthetic iNAV, basis points.
    pub ibit_premium_bps:    Decimal,
    pub fbtc_premium_bps:    Decimal,
    pub arkb_premium_bps:    Decimal,

    // ── Live Horizon Raptor signal snapshot (TradFi velocity / VIX proxy) ─────
    /// Horizon Raptor has at least one fresh SPY/QQQ/UVXY print this tick.
    pub horizon_connected:   bool,
    /// Volume-weighted 5-second velocity of SPY+QQQ (USD Δprice).
    pub tradfi_velocity:     Decimal,
    /// 10-minute rolling Pearson correlation of QQQ velocity vs BTC velocity.
    pub macro_coherence:     Decimal,
    /// UVXY last trade price (VIX futures ETF proxy).
    pub vix_proxy:           Decimal,
    /// 5-second rate of change of UVXY (VIX velocity).
    pub vix_velocity:        Decimal,

    // ── Live Sports Raptor signal snapshot (The Odds API line movement) ──────
    /// Sports Raptor has a fresh cross-book consensus this poll (observe-only).
    pub sports_connected:     bool,
    /// Vig-free consensus implied prob of the tracked event's reference outcome (0..1).
    pub sports_consensus_prob: Decimal,
    /// Δ consensus_prob since the previous poll for the same event (signed line drift).
    pub sports_line_drift:     Decimal,
    /// Spread of per-book implied probs (0..1); high = soft/disagreeing line.
    pub sports_book_dispersion: Decimal,
    /// Number of bookmakers in the sample (0 = no data).
    pub sports_num_books:      Decimal,
    /// Tracked event label, e.g. "Colorado Rockies vs Los Angeles Dodgers".
    #[serde(default)]
    pub sports_event:          String,
    /// The outcome the consensus/drift refer to (first-listed h2h outcome).
    #[serde(default)]
    pub sports_reference:      String,
    /// Sport title from the feed, e.g. "MLB" ("upcoming" mixes sports).
    #[serde(default)]
    pub sports_sport:          String,
    /// ISO-8601 UTC kickoff time of the tracked event.
    #[serde(default)]
    pub sports_commence:       String,
    /// Comma-separated bookmaker titles in the consensus (e.g. "DraftKings, FanDuel").
    #[serde(default)]
    pub sports_books:          String,

    // ── Live Tennis Raptor signal snapshot (Live Tennis API event state) ─────
    /// Tennis Raptor's last poll succeeded AND the tracked score is fresh
    /// (observe-only). False on failure OR staleness — a stale feed must read
    /// as disconnected so a consumer widens/pulls, never holds on it.
    pub tennis_connected:     bool,
    /// Live matches in the sample (0 = no data / nothing on court — neutral).
    pub tennis_num_live:      Decimal,
    /// Sets won by player 1 / player 2 in the tracked match.
    pub tennis_sets_p1:       Decimal,
    pub tennis_sets_p2:       Decimal,
    /// Games won in the tracked match's current set.
    pub tennis_games_p1:      Decimal,
    pub tennis_games_p2:      Decimal,
    /// Serving side of the tracked match (1/2; 0 = unknown).
    pub tennis_server:        Decimal,
    /// Receiver holds a break point (never true in a tiebreak).
    pub tennis_break_point:   bool,
    /// The tracked match's current game is a tiebreak.
    pub tennis_is_tiebreak:   bool,
    /// Age (seconds) of the tracked score's API timestamp (-1 = unknown).
    pub tennis_feed_age_secs: Decimal,
    /// Tracked match label, e.g. "C. Alcaraz vs J. Sinner".
    #[serde(default)]
    pub tennis_match:         String,
    /// Tournament name from the feed, e.g. "Cincinnati Open".
    #[serde(default)]
    pub tennis_tournament:    String,
    /// Tour of the tracked match ("atp"/"wta"/…); empty when unstated.
    #[serde(default)]
    pub tennis_tour:          String,
    /// In-game points as tennis strings, e.g. "30–40" or "AD–40".
    #[serde(default)]
    pub tennis_points:        String,
    /// ISO-8601 UTC timestamp of the tracked score (last score change).
    #[serde(default)]
    pub tennis_score_at:      String,
}

// ─── Telemetry ring buffer ────────────────────────────────────────────────────

/// One timestamped snapshot of every Raptor signal for a single asset.
///
/// Stored in the server-side ring buffer (`TelemetryHistory`) and served by
/// `GET /api/telemetry/history`, giving the Control Tower Telemetry view durable,
/// scrubable history that survives browser reloads (the live snapshot in
/// `AssetRaptorHealth` only ever holds the latest tick).
#[derive(Serialize, Clone, Debug)]
pub struct TelemetrySample {
    /// Sample time — epoch milliseconds (UTC).
    pub t:             i64,
    pub oracle_price:  Decimal,
    pub velocity_5s:   Decimal,
    pub velocity_1s:   Decimal,
    pub acceleration:  Decimal,
    pub drift_60m:     Decimal,
    pub drift_10m:     Decimal,
    pub funding_rate:  Decimal,
    pub open_interest: Decimal,
    pub oi_delta_pct:  Decimal,
    pub cvd_ratio:     Decimal,
    pub price_connected:   bool,
    pub funding_connected: bool,
    pub deriv_connected:   bool,

    // ── Tide Raptor (Institutional Pulse) ──
    pub tide_connected:      bool,
    pub tide_market_open:    bool,
    pub institutional_pulse: Decimal,
    pub tide_coherence:      Decimal,
    pub ibit_premium_bps:    Decimal,
    pub fbtc_premium_bps:    Decimal,
    pub arkb_premium_bps:    Decimal,

    // ── Sports Raptor (line movement) ──
    pub sports_connected:      bool,
    pub sports_consensus_prob: Decimal,
    pub sports_line_drift:     Decimal,
    pub sports_book_dispersion: Decimal,
    pub sports_num_books:      Decimal,
    #[serde(default)]
    pub sports_event:          String,
    #[serde(default)]
    pub sports_reference:      String,
    #[serde(default)]
    pub sports_sport:          String,
    #[serde(default)]
    pub sports_commence:       String,
    #[serde(default)]
    pub sports_books:          String,

    // ── Tennis Raptor (live event state) ──
    pub tennis_connected:     bool,
    pub tennis_num_live:      Decimal,
    pub tennis_sets_p1:       Decimal,
    pub tennis_sets_p2:       Decimal,
    pub tennis_games_p1:      Decimal,
    pub tennis_games_p2:      Decimal,
    pub tennis_server:        Decimal,
    pub tennis_break_point:   bool,
    pub tennis_is_tiebreak:   bool,
    pub tennis_feed_age_secs: Decimal,
    #[serde(default)]
    pub tennis_match:         String,
    #[serde(default)]
    pub tennis_tournament:    String,
    #[serde(default)]
    pub tennis_tour:          String,
    #[serde(default)]
    pub tennis_points:        String,
    #[serde(default)]
    pub tennis_score_at:      String,

    // ── Horizon Raptor (TradFi velocity / VIX proxy) ──
    pub horizon_connected:   bool,
    pub horizon_market_open: bool,
    pub tradfi_velocity:     Decimal,
    pub macro_coherence:     Decimal,
    pub vix_proxy:           Decimal,
    pub vix_velocity:        Decimal,
}

/// Per-asset rolling history of telemetry samples.
/// Bounded to `TELEMETRY_HISTORY_CAP` entries per asset by the sampler task.
pub type TelemetryHistory = Arc<Mutex<HashMap<String, VecDeque<TelemetrySample>>>>;

/// Sampler cadence — how often the background task snapshots the live signals.
const TELEMETRY_SAMPLE_SECS: u64 = 2;
/// Retention cap per asset (samples). 1800 × 2s = 1 hour of scrubable history.
const TELEMETRY_HISTORY_CAP: usize = 1800;
/// The Sports Raptor polls every ~2h (`config::SPORTS_POLL_SECS`), so sampling it at
/// the 2s crypto cadence would store thousands of identical points and flatline its
/// chart. Its samples are de-duplicated (stored only on a value change, or once per
/// heartbeat), so a much larger cap spans many days for a trivial memory cost.
const SPORTS_HISTORY_CAP: usize = 1440;
/// Force a sports sample at least this often even when the signal is unchanged, so the
/// series keeps advancing in time and the most-recent point stays reasonably fresh.
/// 1440 points × 30 min ≈ 30 days of retained, readable movement.
const SPORTS_TELEMETRY_HEARTBEAT_SECS: i64 = 1800;
/// The Tennis Raptor is another slow poller (`config::TENNIS_POLL_SECS`, 900s
/// default), so it gets the same change-or-heartbeat de-duplication as the
/// Sports feed, with the same heartbeat: nothing can change between polls, so
/// a heartbeat shorter than the poll interval would only re-store identical
/// points and shrink the retained window. 1440 points × ≥30 min spans the same
/// ~30 days of readable movement as the Sports feed.
const TENNIS_HISTORY_CAP: usize = 1440;
const TENNIS_TELEMETRY_HEARTBEAT_SECS: i64 = 1800;

/// Background task — every `TELEMETRY_SAMPLE_SECS`, snapshot the current Raptor
/// signal values into the per-asset ring buffer. Spawned once by
/// `run_api_server`; runs for the life of the process.
async fn run_telemetry_sampler(
    raptor_health_rx: watch::Receiver<HashMap<String, AssetRaptorHealth>>,
    history: TelemetryHistory,
) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(TELEMETRY_SAMPLE_SECS));
    loop {
        ticker.tick().await;
        let now = chrono::Utc::now().timestamp_millis();
        let snapshot = raptor_health_rx.borrow().clone();
        if snapshot.is_empty() { continue; }
        let mut hist = match history.lock() {
            Ok(h) => h,
            Err(poisoned) => poisoned.into_inner(),
        };
        for (asset, h) in snapshot.iter() {
            let buf = hist.entry(asset.clone()).or_default();

            // De-duplicate the slow Sports feed: it polls every ~2h, so storing it at
            // the 2s crypto cadence would fill the buffer with identical points and
            // render a flat line. Keep a point only when a signal actually changes, or
            // once per heartbeat so the series still advances in time.
            if asset == "sports" {
                let changed = match buf.back() {
                    Some(last) => {
                        last.sports_consensus_prob  != h.sports_consensus_prob
                            || last.sports_line_drift      != h.sports_line_drift
                            || last.sports_book_dispersion != h.sports_book_dispersion
                            || last.sports_num_books       != h.sports_num_books
                            || last.sports_event           != h.sports_event
                            || last.sports_connected       != h.sports_connected
                    }
                    None => true,
                };
                let heartbeat_due = buf.back()
                    .map(|last| now - last.t >= SPORTS_TELEMETRY_HEARTBEAT_SECS * 1000)
                    .unwrap_or(true);
                if !changed && !heartbeat_due {
                    continue;
                }
            }

            // Same treatment for the slow Tennis feed: keep a point only when the
            // event state actually changes, or once per heartbeat.
            if asset == "tennis" {
                let changed = match buf.back() {
                    Some(last) => {
                        last.tennis_sets_p1      != h.tennis_sets_p1
                            || last.tennis_sets_p2       != h.tennis_sets_p2
                            || last.tennis_games_p1      != h.tennis_games_p1
                            || last.tennis_games_p2      != h.tennis_games_p2
                            || last.tennis_points        != h.tennis_points
                            || last.tennis_server        != h.tennis_server
                            || last.tennis_break_point   != h.tennis_break_point
                            || last.tennis_num_live      != h.tennis_num_live
                            || last.tennis_match         != h.tennis_match
                            || last.tennis_connected     != h.tennis_connected
                    }
                    None => true,
                };
                let heartbeat_due = buf.back()
                    .map(|last| now - last.t >= TENNIS_TELEMETRY_HEARTBEAT_SECS * 1000)
                    .unwrap_or(true);
                if !changed && !heartbeat_due {
                    continue;
                }
            }

            buf.push_back(TelemetrySample {
                t: now,
                oracle_price: h.oracle_price,
                velocity_5s:  h.velocity_5s,
                velocity_1s:  h.velocity_1s,
                acceleration: h.acceleration,
                drift_60m:    h.drift_60m,
                drift_10m:    h.drift_10m,
                funding_rate: h.funding_rate,
                open_interest: h.open_interest,
                oi_delta_pct:  h.oi_delta_pct,
                cvd_ratio:     h.cvd_ratio,
                price_connected:   h.price_connected,
                funding_connected: h.funding_connected,
                deriv_connected:   h.deriv_connected,
                tide_connected:      h.tide_connected,
                tide_market_open:    h.tide_market_open,
                institutional_pulse: h.institutional_pulse,
                tide_coherence:      h.tide_coherence,
                ibit_premium_bps:    h.ibit_premium_bps,
                fbtc_premium_bps:    h.fbtc_premium_bps,
                arkb_premium_bps:    h.arkb_premium_bps,
                sports_connected:      h.sports_connected,
                sports_consensus_prob: h.sports_consensus_prob,
                sports_line_drift:     h.sports_line_drift,
                sports_book_dispersion: h.sports_book_dispersion,
                sports_num_books:      h.sports_num_books,
                sports_event:          h.sports_event.clone(),
                sports_reference:      h.sports_reference.clone(),
                sports_sport:          h.sports_sport.clone(),
                sports_commence:       h.sports_commence.clone(),
                sports_books:          h.sports_books.clone(),
                tennis_connected:     h.tennis_connected,
                tennis_num_live:      h.tennis_num_live,
                tennis_sets_p1:       h.tennis_sets_p1,
                tennis_sets_p2:       h.tennis_sets_p2,
                tennis_games_p1:      h.tennis_games_p1,
                tennis_games_p2:      h.tennis_games_p2,
                tennis_server:        h.tennis_server,
                tennis_break_point:   h.tennis_break_point,
                tennis_is_tiebreak:   h.tennis_is_tiebreak,
                tennis_feed_age_secs: h.tennis_feed_age_secs,
                tennis_match:         h.tennis_match.clone(),
                tennis_tournament:    h.tennis_tournament.clone(),
                tennis_tour:          h.tennis_tour.clone(),
                tennis_points:        h.tennis_points.clone(),
                tennis_score_at:      h.tennis_score_at.clone(),
                horizon_connected:   h.horizon_connected,
                horizon_market_open: h.tradfi_velocity != Decimal::ZERO || h.vix_proxy != Decimal::ZERO,
                tradfi_velocity:     h.tradfi_velocity,
                macro_coherence:     h.macro_coherence,
                vix_proxy:           h.vix_proxy,
                vix_velocity:        h.vix_velocity,
            });
            let len = buf.len();
            let cap = match asset.as_str() {
                "sports" => SPORTS_HISTORY_CAP,
                "tennis" => TENNIS_HISTORY_CAP,
                _ => TELEMETRY_HISTORY_CAP,
            };
            if len > cap {
                buf.drain(0..len - cap);
            }
        }
    }
}

// ─── Shared state ────────────────────────────────────────────────────────────

/// Cloneable handle passed to every axum handler via `State<ApiState>`.
#[derive(Clone)]
pub struct ApiState {
    /// Broadcast sender — PATCH handler calls `.send()` to hot-reload strategies.
    pub config_tx: Arc<watch::Sender<Arc<DynamicConfig>>>,
    /// Receiver — GET handler reads the latest snapshot without blocking.
    pub config_rx: watch::Receiver<Arc<DynamicConfig>>,
    /// Receiver — maps strategy key ("time_decay", "momentum", …) to current market name.
    pub markets_rx: watch::Receiver<HashMap<String, String>>,
    /// Receiver — maps asset symbol (e.g. "btc") to its pair of Raptor health flags.
    /// Updated by the Price and Funding Raptors in real-time.
    pub raptor_health_rx: watch::Receiver<HashMap<String, AssetRaptorHealth>>,
    /// Optional API key read from `DRADIS_API_KEY` env var at startup.
    /// When `Some`, every request must include `X-API-Key: <value>`.
    /// When `None`, no authentication is required (default for local dev).
    pub api_key: Option<String>,
    /// When true (`DRADIS_READ_ONLY=true`), every mutating request (any method
    /// other than GET/HEAD) is rejected with 403. Powers the public read-only
    /// demo at demo.dradis.live — the live raptor telemetry streams, but no visitor
    /// can patch config, toggle vipers, or exit positions.
    pub read_only: bool,
    /// Gnosis Safe wallet address — used by POST /api/positions/sync to fetch live
    /// on-chain holdings and purge stale open_positions rows without a restart.
    /// Intl-only: the US custodial venue has no self-custody wallet address.
    #[cfg(feature = "intl_clob")]
    pub safe_address: Address,
    /// CAG (Carrier Air Group) — squadron registry.
    /// Phase 3d: exposes GET /api/squadrons and GET /api/squadrons/{id}.
    /// Phase 3f: will also handle POST/DELETE once patrol() is fully wired.
    pub cag: Cag,
    /// Server-side ring buffer of Raptor signal samples (per asset).
    /// Populated by the telemetry sampler task; served by
    /// GET /api/telemetry/history so the UI survives reloads and can scrub.
    pub telemetry_history: TelemetryHistory,
}

// ─── API-key middleware ──────────────────────────────────────────────────────

/// Optional `X-API-Key` authentication gate.
///
/// When `DRADIS_API_KEY` is set in the environment, every request must carry a
/// matching `X-API-Key` header — including requests from OpenClaw or any other
/// external tool.  When the env var is absent the middleware is a no-op, keeping
/// local-dev workflow unchanged.
///
/// CORS pre-flight (`OPTIONS`) requests bypass this check because they are handled
/// by `CorsLayer` (the outer layer) before this middleware is reached.
async fn require_api_key(
    State(s): State<ApiState>,
    req: Request,
    next: Next,
) -> Response {
    if let Some(ref expected) = s.api_key {
        let provided = req
            .headers()
            .get("x-api-key")
            .and_then(|v| v.to_str().ok());
        if provided != Some(expected.as_str()) {
            warn!(" API key rejected — invalid or missing X-API-Key header");
            return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
        }
    }
    next.run(req).await
}

/// Read-only demo gate.
///
/// When `DRADIS_READ_ONLY=true`, any state-mutating request (any HTTP method
/// other than the safe `GET`/`HEAD`) is rejected with `403 Forbidden` and a
/// small JSON body. This is the single server-side chokepoint that makes the
/// public demo safe: even if a visitor bypasses the UI and hits the API
/// directly, no write (config patch, viper toggle, position exit, chain sync)
/// can land. A no-op when the env var is unset/false, so normal deployments are
/// unchanged.
///
/// CORS pre-flight (`OPTIONS`) is handled by the outer `CorsLayer` and never
/// reaches this middleware.
async fn enforce_read_only(
    State(s): State<ApiState>,
    req: Request,
    next: Next,
) -> Response {
    if s.read_only && !matches!(*req.method(), Method::GET | Method::HEAD) {
        warn!(
            " Read-only demo: rejected {} {}",
            req.method(),
            req.uri().path()
        );
        return (
            StatusCode::FORBIDDEN,
            [(header::CONTENT_TYPE, "application/json")],
            r#"{"error":"read-only demo — deploy your own at github.com/mbordash/DRADIS"}"#,
        )
            .into_response();
    }
    next.run(req).await
}


/// Query params for asset-scoped endpoints.
#[derive(Deserialize)]
struct AssetQuery {
    asset: Option<String>,
    limit: Option<i64>,
    /// `?fresh=1` bypasses the quote cache for this request.
    ///
    /// Only the intl-gated quotes endpoint reads it, so it is dead on the other
    /// venue builds by construction rather than by oversight.
    #[cfg_attr(not(feature = "intl_clob"), allow(dead_code))]
    ///
    /// Only `/api/positions/quotes` reads it. The operator-facing case is the
    /// Trade Log's manual refresh control: someone about to close a position by
    /// hand wants the book as it is now, not as it was up to
    /// `position_quote_ttl_secs` ago. Every other endpoint deserializing this
    /// struct simply ignores the field.
    fresh: Option<u8>,
}

/// Request body for manual "Return to Base" exit.
///
/// POST /api/positions/manual-exit
///
/// Executes an immediate FAK (Fill-And-Kill) market sell order for the
/// specified position, records the trade with actual exit price and P&L,
/// and closes the position in the database.
#[derive(Deserialize)]
#[cfg(feature = "intl_clob")]
struct ManualExitRequest {
    /// Token ID (decimal U256 string)
    token_id: String,
    /// Asset symbol (e.g. "btc") for DB pool selection
    asset: String,
    /// Strategy name for position lookup
    strategy: String,
    /// Market name for trade recording
    market: String,
    /// Side (YES/NO) for trade recording
    side: String,
    /// Current bid supplied by the client. IGNORED server-side — the live best bid
    /// is fetched from the CLOB (the client value was a hardcoded "0.5" placeholder).
    /// Retained for wire compatibility with existing Control Tower builds.
    #[allow(dead_code)]
    current_bid: String,
    /// Verifying contract address supplied by the client. IGNORED server-side —
    /// the exchange is resolved from the market's neg-risk status (see handler).
    /// Retained for wire compatibility with existing Control Tower builds.
    #[allow(dead_code)]
    verifying_contract: String,
}

// ─── Handlers ────────────────────────────────────────────────────────────────

/// GET /api/health
async fn health() -> &'static str {
    debug!("Received GET /api/health request");
    "ok"
}

/// GET /api/assets
///
/// Returns the list of asset symbols for which a SQLite pool has been
/// initialized, sorted alphabetically.  The Control Tower uses this to
/// populate the asset selector tabs.
///
/// Response: `["btc", "eth", "sol"]`
async fn get_assets() -> Response {
    debug!("Received GET /api/assets request");
    Json(db::available_assets()).into_response()
}

/// GET /api/config
///
/// Returns the full DynamicConfig as a flat JSON object.
/// Field names match the struct fields (snake_case).
async fn get_config(State(s): State<ApiState>) -> Response {
    debug!("Received GET /api/config request");
    let cfg = s.config_rx.borrow().clone();
    match serde_json::to_value(cfg.as_ref()) {
        Ok(val) => {
            debug!("Successfully processed GET /api/config");
            (StatusCode::OK, Json(val)).into_response()
        },
        Err(e)  => {
            error!("Error processing GET /api/config: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        },
    }
}

/// PATCH /api/config
///
/// Body: a partial JSON object with only the fields to change, e.g.
///   `{"ghost_mode": false, "basis_stop_loss_pct": "0.08"}`
///
/// Uses JSON merge-patch semantics: unknown keys are ignored.
/// On success, broadcasts the new config on the watch channel so all
/// in-flight strategy tick contexts pick it up within 50 ms.
/// Does a global PATCH also reach the squadrons that are already deployed?
///
/// Global config seeds a squadron at deploy time and is read by nothing
/// afterwards: a deployed squadron reads its OWN `squadron_configs` row via
/// `load_or_init_for_squadron`, and nothing in the CAG subscribes to the global
/// broadcast. So a global-only write is invisible to every running patrol loop.
///
/// That cost real trust on 2026-08-29. An operator used the Control Tower's
/// GHOST/LIVE header button to go live on the production Marketplace instance.
/// The button calls this endpoint, the global row genuinely flipped, the API
/// returned 200 and every surface rendered LIVE — while all three deployed
/// squadrons kept their own `ghost_mode: true` and went on simulating fills for
/// a day. The operator believed they were trading real money and were not.
///
/// The same defect had already been diagnosed on 2026-08-11 and fixed for the
/// profile-apply endpoint alone (`setup.rs`, `ProfileScope`), leaving the other
/// callers of this endpoint broken. So the default here matches that one: reach
/// the deployed squadrons unless explicitly told not to.
///
/// A merge patch only touches the fields it names, so fanning out sets exactly
/// the fields the operator edited and leaves every other per-squadron value
/// alone. `?scope=global_only` restores the old seed-only behavior for a caller
/// that genuinely wants to change future deployments without disturbing what is
/// running.
#[derive(Deserialize, Default, PartialEq, Eq, Clone, Copy, Debug)]
#[serde(rename_all = "snake_case")]
enum PatchScope {
    /// Global row only. Seeds squadrons deployed LATER; changes nothing running.
    GlobalOnly,
    /// Global row plus every currently-deployed squadron (default).
    #[default]
    GlobalAndDeployed,
}

#[derive(Deserialize, Default)]
struct PatchConfigQuery {
    #[serde(default)]
    scope: PatchScope,
}

async fn patch_config(
    State(s): State<ApiState>,
    Query(q): Query<PatchConfigQuery>,
    body: String,
) -> Response {
    info!("📥 Received PATCH /api/config (global) with body: {}", body);
    let current = s.config_rx.borrow().clone();
    match DynamicConfig::apply_patch(&current, &body).await {
        Ok(new_cfg) => {
            // Broadcast to all strategy tick loops.
            let _ = s.config_tx.send(new_cfg.clone());

            // Fan out to what is actually running. See `PatchScope`.
            if q.scope == PatchScope::GlobalAndDeployed {
                // Target set = registered live handles UNION persisted squadron
                // rows.
                //
                // The handle registry alone is not enough, and relying on it
                // silently broke two of the three venues. Only the intl paths
                // call `register_squadron_config_handle` (cag/run.rs and
                // cag/adama.rs); the Kalshi and Polymarket US traders never do,
                // so the registry is permanently EMPTY on those builds. That is
                // why the 2026-08-11 profile fan-out has never worked there
                // either: it reported success over zero squadrons while the
                // Setup dialog told the operator "no squadrons are currently
                // deployed" with PATROLLING squadrons listed beside it.
                //
                // Those traders hold a plain `Arc<DynamicConfig>` that they
                // reload from the DB roughly every 30s, so writing the row is
                // what reaches them. `apply_squadron_patch_as` writes the row and
                // additionally pushes into the live handle when one is
                // registered, so intl still applies on the next tick.
                //
                // Rows for stood-down squadrons are included deliberately: they
                // are not running, so the write is harmless, and it means a
                // squadron redeployed later starts from the operator's current
                // settings rather than resurrecting stale ones.
                let mut ids = crate::helpers::dynamic_config::registered_squadron_ids();
                if let Some(pool) = crate::helpers::db::pool() {
                    for id in crate::helpers::db::squadron_config_list(pool).await {
                        if !ids.contains(&id) {
                            ids.push(id);
                        }
                    }
                }
                let mut reached = 0usize;
                for id in &ids {
                    match DynamicConfig::apply_squadron_patch_as(id, &body, "operator").await {
                        Ok(_) => reached += 1,
                        // Reported, never fatal: the global write already
                        // succeeded, and failing the request would leave the
                        // caller unsure whether ANY of it landed.
                        Err(e) => warn!("⚙️  Global patch did not reach squadron {id}: {e}"),
                    }
                }
                if !ids.is_empty() {
                    info!("⚙️  Global patch applied to {reached}/{} deployed squadron(s)", ids.len());
                }
            }

            match serde_json::to_value(new_cfg.as_ref()) {
                Ok(val) => {
                    debug!("Successfully processed PATCH /api/config");
                    (StatusCode::OK, Json(val)).into_response()
                },
                Err(e)  => {
                    error!("Error serializing new config after PATCH /api/config: {}", e);
                    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                },
            }
        }
        Err(e) => {
            error!("Error applying patch for PATCH /api/config: {}", e);
            (StatusCode::BAD_REQUEST, e.to_string()).into_response()
        },
    }
}

/// GET /api/logs?tail=500
///
/// Recent engine log lines (oldest first) from the in-memory ring buffer —
/// the Control Tower Console view. No Docker socket or file access involved;
/// see helpers::logbuf.
async fn get_logs(Query(q): Query<LogsQuery>) -> Response {
    let tail = q.tail.unwrap_or(500).clamp(1, 2000);
    let lines = crate::helpers::logbuf::tail(tail);
    Json(serde_json::json!({ "count": lines.len(), "lines": lines })).into_response()
}

#[derive(Deserialize)]
struct LogsQuery {
    tail: Option<usize>,
}

/// GET /api/latency
///
/// Rolling round-trip latency from the engine host to the trading venue
/// (CLOB on intl builds, US venue API on US builds) — the Control Tower
/// footer meter. Measured server-side; see helpers::latency.
async fn get_latency() -> Response {
    Json(crate::helpers::latency::snapshot()).into_response()
}

/// GET /api/gboost/veto-scores?asset=btc
///
/// Per-gate scoreboard for GBoost's entry stack, scored against SETTLED market
/// outcomes rather than the model's own probability. Each row answers the only
/// question that matters for gate calibration: of the signals this gate blocked,
/// how many would actually have won, and what was the realised edge per share?
///
/// `total - scored` is the still-unresolved backlog — read `avg_pnl_per_share`
/// only once `scored` is large enough to mean something.
async fn get_gboost_veto_scores(Query(q): Query<AssetQuery>) -> Response {
    let Some(pool) = db::pool_for_opt_retry(q.asset.as_deref()).await else {
        log_pool_unavailable("GET /api/gboost/veto-scores", q.asset.as_deref());
        return (StatusCode::SERVICE_UNAVAILABLE, "database not ready").into_response();
    };
    Json(db::gboost_veto_scoreboard(&pool).await).into_response()
}

/// GET /api/vipers/status?asset=btc
///
/// Per-viper "why aren't we trading?" registry, keyed by (squadron asset,
/// strategy): last evaluation time/outcome plus the most recent named veto
/// reason from instrumented vipers. Omit `asset` for all squadrons (CAG
/// rollup); pass it for one squadron's detail view.
async fn get_vipers_status(Query(q): Query<AssetQuery>) -> Response {
    Json(crate::helpers::viper_status::snapshot(q.asset.as_deref())).into_response()
}

/// GET /api/trades/export?asset=btc
///
/// Full tradelog as a CSV download (oldest first) for tax reporting or
/// offline review. Same per-asset DB selection as /api/trades.
async fn export_trades(Query(q): Query<AssetQuery>) -> Response {
    let Some(pool) = db::pool_for_opt_retry(q.asset.as_deref()).await else {
        log_pool_unavailable("GET /api/trades/export", q.asset.as_deref());
        return (StatusCode::SERVICE_UNAVAILABLE, "database not ready").into_response();
    };
    let trades = db::get_all_trades(&pool).await;

    // Minimal CSV escaping: quote any field containing comma/quote/newline.
    fn esc(s: &str) -> String {
        if s.contains([',', '"', '\n', '\r']) {
            format!("\"{}\"", s.replace('"', "\"\""))
        } else {
            s.to_string()
        }
    }

    let mut csv = String::with_capacity(trades.len() * 96 + 128);
    csv.push_str("timestamp,strategy,market,side,entry_price,exit_price,shares,pnl_usdc,exit_reason\n");
    for t in &trades {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{},{}\n",
            esc(&t.ts), esc(&t.strategy), esc(&t.market), esc(&t.side),
            esc(&t.entry_price), esc(&t.exit_price), esc(&t.shares),
            esc(&t.pnl), esc(&t.reason),
        ));
    }

    let filename = format!(
        "dradis-tradelog-{}{}.csv",
        q.asset.as_deref().map(|a| format!("{a}-")).unwrap_or_default(),
        chrono::Utc::now().format("%Y%m%d"),
    );
    info!("📄 Tradelog export: {} trades → {}", trades.len(), filename);
    (
        [
            ("content-type", "text/csv; charset=utf-8".to_string()),
            ("content-disposition", format!("attachment; filename=\"{filename}\"")),
        ],
        csv,
    ).into_response()
}

/// Snapshot closest in time to `target_time`, within `window_secs`.
///
/// Split out of [`get_pnl_history`] so the timestamp join can be regression
/// tested without a database or an axum router. Note that callers pass
/// newest-first slices, so any "first match" strategy here silently biases
/// toward the future — see the comment at the call site.
fn nearest_snapshot(
    snaps: &[db::PnlSnapshotRow],
    target_time: i64,
    window_secs: i64,
) -> Option<&db::PnlSnapshotRow> {
    snaps
        .iter()
        .filter_map(|s| {
            chrono::DateTime::parse_from_rfc3339(&s.ts)
                .ok()
                .map(|dt| ((dt.timestamp() - target_time).abs(), s))
        })
        .filter(|(delta, _)| *delta <= window_secs)
        .min_by_key(|(delta, _)| *delta)
        .map(|(_, s)| s)
}

/// GET /api/pnl/history?limit=200&asset=btc
///
/// Returns up to `limit` P&L snapshots, newest first.
/// Each row: { ts, session_pnl, collateral, total_value }
///
/// When `asset` query param is omitted, returns aggregated global P&L history
/// (collateral + sum of all assets' positions_value per timestamp).
async fn get_pnl_history(Query(q): Query<AssetQuery>) -> Response {
    debug!("Received GET /api/pnl/history request with limit: {:?}, asset: {:?}", q.limit, q.asset);
    // 2000, not 1000: the dashboard asks for 1440 and was silently cut to 1000.
    // The number is now a POINT COUNT spread across 24 hours rather than a row
    // count off the end of the table, so honouring the request costs nothing —
    // the server downsamples to fit (see db::get_pnl_history).
    let limit = q.limit.unwrap_or(200).clamp(1, 2000);

    // If asset is specified, return single-asset history (legacy behavior)
    if let Some(asset_name) = q.asset.as_deref() {
        match db::pool_for_opt_retry(Some(asset_name)).await {
            Some(pool) => {
                let history = db::get_pnl_history(&pool, limit).await;
                debug!("Successfully retrieved PNL history for asset: {}", asset_name);
                return Json(history).into_response();
            },
            None => {
                log_pool_unavailable(&format!("asset: {asset_name}"), Some(asset_name));
                return Json(Vec::<db::PnlSnapshotRow>::new()).into_response();
            },
        }
    }

    // No asset specified → return aggregated global P&L history
    use rust_decimal::Decimal;
    use std::str::FromStr;

    let assets = db::available_assets();
    if assets.is_empty() {
        warn!("GET /api/pnl/history (global): no assets available");
        return Json(Vec::<db::PnlSnapshotRow>::new()).into_response();
    }

    // Fetch snapshots from all assets
    let mut all_snapshots: Vec<(String, Vec<db::PnlSnapshotRow>)> = vec![];
    for asset in &assets {
        if let Some(pool) = db::pool_for(asset) {
            let snaps = db::get_pnl_history(&pool, limit).await;
            all_snapshots.push((asset.clone(), snaps));
        }
    }

    // Base the timeline on the asset with the freshest snapshot — NOT simply
    // assets[0]. On the US build both "btc" (primary DB, never written) and
    // "us" pools exist; "btc" sorts first with zero rows in the 24h window,
    // which returned an empty global history while the us pool had data
    // (2026-08-08: empty balance card next to a live $120 portfolio value).
    all_snapshots.sort_by(|a, b| {
        let latest = |s: &Vec<db::PnlSnapshotRow>| s.first().map(|r| r.ts.clone());
        latest(&b.1).cmp(&latest(&a.1))
    });

    if all_snapshots.is_empty() {
        warn!("GET /api/pnl/history (global): no snapshots from any asset");
        return Json(Vec::<db::PnlSnapshotRow>::new()).into_response();
    }

    // Use primary asset's timestamps as the base timeline
    let (primary_asset, primary_snaps) = &all_snapshots[0];

    // For each primary timestamp, aggregate positions_value from all assets
    let aggregated: Vec<db::PnlSnapshotRow> = primary_snaps.iter().map(|primary| {
        let ts = &primary.ts;
        let collateral = &primary.collateral;

        // Match each asset to this timestamp by NEAREST snapshot, not by the
        // first one inside the window.
        //
        // `get_pnl_history` returns rows newest-first, so an `Iterator::find`
        // over a ±120s window yielded the *newest* row in that window rather
        // than the closest one. Every point then took its collateral from its
        // own row but its positions_value from a row up to two minutes in the
        // future, putting a phantom spike in front of every entry and a phantom
        // dip in front of every exit. Observed 2026-08-14: the 19:36 buy marked
        // the 19:34 and 19:35 points at $66.78 — pre-trade cash $64.06 plus a
        // position that did not exist yet — against a true $64.06, then dropped
        // the 19:41 point to $61.71 by pairing mid-trade cash with post-exit
        // positions. The snapshots themselves were correct throughout
        // (19:36:15 recorded 61.71 + 2.73 = 64.43); only this join was wrong.
        // Nearest-match keeps cash and positions on the same clock.
        let window_secs: i64 = 120;
        let primary_time = chrono::DateTime::parse_from_rfc3339(ts)
            .map(|dt| dt.timestamp())
            .unwrap_or(0);

        let mut total_positions_value = Decimal::ZERO;
        let mut total_session_pnl = Decimal::ZERO;

        for (asset, snaps) in &all_snapshots {
            // The primary asset contributes the very row that defines this
            // point, so there is nothing to search for — and searching would
            // reintroduce the skew whenever two snapshots share a timestamp.
            let nearest = if asset == primary_asset {
                Some(primary)
            } else {
                nearest_snapshot(snaps, primary_time, window_secs)
            };
            if let Some(snap) = nearest {
                // Extract positions_value = total_value - collateral
                if let (Some(tv_str), Ok(coll)) = (
                    snap.total_value.as_ref(),
                    Decimal::from_str(&snap.collateral),
                ) {
                    if let Ok(tv) = Decimal::from_str(tv_str) {
                        let pos_val = (tv - coll).max(Decimal::ZERO);
                        total_positions_value += pos_val;
                        debug!("[{}] @ {} positions_value={}", asset, ts, pos_val);
                    }
                }

                // Sum session P&L from each asset
                if let Ok(pnl) = Decimal::from_str(&snap.session_pnl) {
                    total_session_pnl += pnl;
                }
            }
        }

        // Global total = collateral + sum(positions across all assets)
        let coll_dec = Decimal::from_str(collateral).unwrap_or(Decimal::ZERO);
        let global_total = coll_dec + total_positions_value;

        db::PnlSnapshotRow {
            ts: ts.clone(),
            session_pnl: total_session_pnl.to_string(),
            collateral: collateral.clone(),
            total_value: Some(global_total.to_string()),
        }
    }).collect();

    debug!("Successfully retrieved global aggregated PNL history ({} points)", aggregated.len());
    Json(aggregated).into_response()
}

/// GET /api/status
///
/// Returns the current market name each strategy is attached to, the session
/// start timestamp (RFC-3339), and per-asset Raptor connection health.
///
/// Response:
/// ```json
/// {
///   "strategy_markets": { "time_decay": "Will BTC…", … },
///   "session_started_at": "2026-06-02T14:32:01Z",
///   "raptors": {
///     "btc": { "price_connected": true, "funding_connected": true }
///   }
/// }
/// ```
#[derive(Serialize)]
struct StatusResponse {
    strategy_markets: HashMap<String, String>,
    /// RFC-3339 timestamp of the current session start (= process startup).
    session_started_at: String,
    /// Per-asset Binance Raptor connection health.
    raptors: HashMap<String, AssetRaptorHealth>,
    /// Markets whose order-book feed has gone dark, with seconds since the last
    /// real book. Empty is the healthy case.
    ///
    /// Reported because nothing else does. The Raptor health above covers the
    /// SIGNAL feeds; when the venue's own book stopped arriving, health stayed
    /// 200, the squadron stayed PATROLLING and the Maker went on logging
    /// "✅ Maker quoting" — every gate correctly declined to trade an empty
    /// book, and declining quietly is indistinguishable from a quiet market.
    dark_market_feeds: Vec<DarkFeed>,
}


/// Report a missing database pool at a severity that matches the situation.
///
/// Before the operator finishes Setup the engine is parked and has no pool, but
/// the Control Tower dashboard polls regardless — a fresh AMI logged 32 ERROR
/// lines in its first minute for a state that is expected, self-resolving, and
/// the direct consequence of parking on purpose. That undercuts the calm the
/// park fix was meant to create: a buyer watching the log during setup sees a
/// wall of red.
///
/// Once the trading loop has started, a missing pool IS an error and still says
/// so.
fn log_pool_unavailable(what: &str, asset: Option<&str>) {
    if crate::helpers::watchdog::is_parked_for_setup() {
        debug!("Database pool not available for {what} — engine parked awaiting setup");
        return;
    }
    // An asset this instance does not trade has no pool BY DESIGN, so a request
    // for one is an expected condition rather than a fault. The Control Tower
    // asks per-asset regardless of what is configured, so on 2026-08-28 a
    // BTC-only instance logged six ERROR lines the moment the dashboard was
    // opened: three endpoints times eth and sol. A customer who has done nothing
    // wrong should not see red on first load.
    //
    // An EMPTY pool set is different, and stays an error: that means nothing is
    // initialized at all, which is a real fault rather than an unconfigured asset.
    if let Some(a) = asset {
        let known = db::available_assets();
        if !known.is_empty() && !known.iter().any(|k| k.eq_ignore_ascii_case(a)) {
            debug!(
                "Database pool not available for {what} — asset '{a}' is not traded \
                 on this instance (configured: {})",
                known.join(", "),
            );
            return;
        }
    }
    error!("Database pool not available for {what}");
}

async fn get_status(State(s): State<ApiState>) -> Response {
    debug!("Received GET /api/status request");
    let markets = s.markets_rx.borrow().clone();
    let raptors = s.raptor_health_rx.borrow().clone();
    let session_started_at = db::current_session_id().to_string();
    let dark_market_feeds: Vec<DarkFeed> = crate::state::price_state::book_feed::dark_markets()
        .into_iter()
        .map(|(market, market_name, secs)| DarkFeed { market, market_name, dark_for_secs: secs })
        .collect();
    debug!("Successfully retrieved status");
    Json(StatusResponse { strategy_markets: markets, session_started_at, raptors, dark_market_feeds }).into_response()
}

/// GET /api/telemetry
///
/// Returns the live signal snapshot for every asset's Raptors — the same
/// `AssetRaptorHealth` map exposed under `/api/status.raptors`, but on a
/// dedicated lightweight endpoint the Control Tower Telemetry view can poll at
/// a high cadence (every ~2 s) to build rolling signal graphs.
///
/// Response (keyed by asset symbol):
/// ```json
/// {
///   "btc": {
///     "price_connected": true, "funding_connected": true,
///     "oracle_price": 64210.5, "velocity_5s": 12.3, "velocity_1s": 4.1,
///     "acceleration": 1.2, "drift_60m": 305.0, "drift_10m": 88.0,
///     "funding_rate": 0.00012
///   }
/// }
/// ```
async fn get_telemetry(State(s): State<ApiState>) -> Response {
    debug!("Received GET /api/telemetry request");
    let raptors = s.raptor_health_rx.borrow().clone();
    Json(raptors).into_response()
}

/// Query params for telemetry history.
#[derive(Deserialize)]
struct TelemetryHistoryQuery {
    asset: Option<String>,
    limit: Option<usize>,
}

/// GET /api/telemetry/history?asset=btc&limit=900
///
/// Returns up to `limit` most-recent telemetry samples (oldest→newest) for the
/// given asset from the server-side ring buffer. Defaults to the primary asset
/// and the full retained window (1 hour). This durable history lets the Control
/// Tower Telemetry view survive reloads and scrub back over past signal windows.
async fn get_telemetry_history(
    State(s): State<ApiState>,
    Query(q): Query<TelemetryHistoryQuery>,
) -> Response {
    debug!("Received GET /api/telemetry/history request");
    let limit = q.limit.unwrap_or(TELEMETRY_HISTORY_CAP).clamp(1, TELEMETRY_HISTORY_CAP);
    let hist = match s.telemetry_history.lock() {
        Ok(h) => h,
        Err(poisoned) => poisoned.into_inner(),
    };
    // Resolve asset: explicit param (lowercased), else the first (primary) key.
    let key = q.asset
        .map(|a| a.to_lowercase())
        .or_else(|| hist.keys().next().cloned());
    let samples: Vec<TelemetrySample> = match key.as_deref().and_then(|k| hist.get(k)) {
        Some(buf) => {
            let start = buf.len().saturating_sub(limit);
            buf.iter().skip(start).cloned().collect()
        }
        None => Vec::new(),
    };
    Json(samples).into_response()
}

/// GET /api/telemetry/assets
///
/// Returns the list of asset keys that have raptor telemetry data (i.e. keys
/// present in the telemetry history ring buffer). This is the set of crypto
/// underlyings actively monitored by raptors — distinct from the full DB
/// pool list returned by `/api/assets`, which also includes venue-only
/// databases (e.g. "kalshi") that have no raptor signal data.
async fn get_telemetry_assets(State(s): State<ApiState>) -> Response {
    let hist = match s.telemetry_history.lock() {
        Ok(h) => h,
        Err(poisoned) => poisoned.into_inner(),
    };
    let mut assets: Vec<String> = hist.keys()
        .filter(|k| !k.is_empty())
        .cloned()
        .collect();
    assets.sort();
    Json(assets).into_response()
}

/// GET /api/trades?limit=100&asset=btc
///
/// Returns up to `limit` completed trades, newest first.
/// Each row: { ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason }
async fn get_trades(Query(q): Query<AssetQuery>) -> Response {
    debug!("Received GET /api/trades request with limit: {:?}", q.limit);
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    // Unscoped reads span EVERY shard — see db::pools_for_opt. A venue that
    // shards by wing (Polymarket US) writes nothing to its primary pool, so the
    // single-pool read the dashboard issues returned an empty log while trades
    // were landing.
    if q.asset.is_none() {
        let mut all: Vec<db::TradeRow> = Vec::new();
        for pool in db::pools_for_opt(None) {
            all.extend(db::get_recent_trades(&pool, limit).await);
        }
        all.sort_by(|a, b| b.ts.cmp(&a.ts));
        all.truncate(limit as usize);
        return Json(all).into_response();
    }
    match db::pool_for_opt_retry(q.asset.as_deref()).await {
        Some(pool) => {
            let trades = db::get_recent_trades(&pool, limit).await;
            debug!("Successfully retrieved trades");
            Json(trades).into_response()
        },
        None       => {
            log_pool_unavailable(&format!("GET /api/trades (asset={:?})", q.asset), q.asset.as_deref());
            Json(Vec::<db::TradeRow>::new()).into_response()
        },
    }
}

/// GET /api/trades/stats?asset=btc
///
/// Lifetime aggregates over the shard's entire trade history — count, wins,
/// losses, realized P&L, fees. Separate from `/api/trades` on purpose: that
/// endpoint is a bounded recent window for the trade *list*, and summing it
/// client-side silently truncated every "total" card on the dashboard.
async fn get_trade_stats(Query(q): Query<AssetQuery>) -> Response {
    // Summed across every shard when unscoped. These are the dashboard's "total"
    // cards, so reading one shard on a wing-sharded venue reported zero trades
    // and zero P&L while both were accumulating elsewhere.
    if q.asset.is_none() {
        let mut agg = db::TradeStatsRow {
            count: 0, wins: 0, losses: 0, realized_pnl: 0.0, fees: 0.0,
            first_ts: None, last_ts: None,
        };
        for pool in db::pools_for_opt(None) {
            let s = db::get_trade_stats(&pool).await;
            agg.count += s.count;
            agg.wins += s.wins;
            agg.losses += s.losses;
            agg.realized_pnl += s.realized_pnl;
            agg.fees += s.fees;
            // Earliest first_ts and latest last_ts across the whole venue.
            agg.first_ts = match (agg.first_ts.take(), s.first_ts) {
                (Some(a), Some(b)) => Some(if a <= b { a } else { b }),
                (a, b) => a.or(b),
            };
            agg.last_ts = match (agg.last_ts.take(), s.last_ts) {
                (Some(a), Some(b)) => Some(if a >= b { a } else { b }),
                (a, b) => a.or(b),
            };
        }
        return Json(agg).into_response();
    }
    match db::pool_for_opt_retry(q.asset.as_deref()).await {
        Some(pool) => Json(db::get_trade_stats(&pool).await).into_response(),
        None => {
            log_pool_unavailable(&format!("GET /api/trades/stats (asset={:?})", q.asset), q.asset.as_deref());
            Json(db::TradeStatsRow {
                count: 0, wins: 0, losses: 0, realized_pnl: 0.0, fees: 0.0,
                first_ts: None, last_ts: None,
            }).into_response()
        }
    }
}

/// GET /api/positions?asset=btc
///
/// Returns all currently open positions for this session (inserted on entry, removed on exit).
/// Covers all strategies and both ghost/live modes so the UI always has a complete picture
/// of in-flight positions even before they appear as completed trades.
/// GET /api/positions/quotes?asset=btc
///
/// Live bid/ask/mid for every open position, straight from the venue.
///
/// The Trade Log's mark price is refreshed by a 300s chain-sync sweep reading
/// the indexer-backed Data API. That is fine for a dashboard glance and useless
/// for the thing an operator actually does with it: deciding whether to close a
/// position by hand right now. On 2026-08-30 an operator watched $0.82 on a
/// position the book had at $0.98, sixteen cents adrift on a binary minutes from
/// resolution.
///
/// The BID is the number that matters here and it is returned first. A manual
/// exit sells into the bid, so the mid flatters the decision on a wide book; the
/// mid is returned too because that is what a portfolio is conventionally marked
/// at, but the two are labeled rather than silently interchanged.
///
/// Results are cached for `position_quote_ttl_secs`. That bounds repeat asks
/// within the window; it is NOT single-flight, so concurrent viewers who both
/// miss will both fetch. At the default TTL the dashboard's poll deliberately
/// outruns the window — freshness is the whole point of this endpoint.
///
/// `?fresh=1` skips the cache read and refetches every open position from the
/// venue. It is the Trade Log's manual refresh control: the automatic poll is
/// paced for a dashboard left open all day, and an operator deciding whether to
/// close a position by hand needs the book as of now. The refetch still writes
/// through to the cache, so the press benefits every other viewer too.
#[cfg(feature = "intl_clob")]
async fn get_position_quotes(State(s): State<ApiState>, Query(q): Query<AssetQuery>) -> Response {
    use std::collections::HashMap as StdHashMap;
    use std::sync::{Mutex as StdMutex, OnceLock};
    use std::time::Instant;

    #[derive(Clone, Serialize)]
    struct Quote {
        token_id: String,
        /// What a sale would execute against right now. `null` when the venue
        /// has no bid, which is itself the answer: the position cannot be sold.
        bid: Option<String>,
        ask: Option<String>,
        mid: Option<String>,
        /// Age of this quote in seconds. Zero means it was just fetched.
        age_secs: u64,
    }

    static CACHE: OnceLock<StdMutex<StdHashMap<String, (Quote, Instant)>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| StdMutex::new(StdHashMap::new()));

    let Some(asset) = q.asset.clone() else {
        return (StatusCode::BAD_REQUEST, "asset query parameter is required").into_response();
    };
    let Some(session) = s.cag.session_for_asset(&asset) else {
        return (StatusCode::BAD_REQUEST, "Asset not found").into_response();
    };
    let Some(pool) = db::pool_for_opt_retry(Some(&asset)).await else {
        return (StatusCode::SERVICE_UNAVAILABLE, "database not ready").into_response();
    };

    let ttl = std::time::Duration::from_secs(
        s.config_rx.borrow().position_quote_ttl_secs.max(1),
    );

    // An explicit refresh skips the cache READ but still writes through below,
    // so one operator pressing refresh also re-primes the entry every polling
    // viewer is served from. Bypassing the write instead would leave the stale
    // entry in place and make the button a no-op for everyone else.
    //
    // Two limits, because this parameter is an amplifier: each bypassed position
    // costs two sequential venue REST calls, made with the SAME credentials the
    // engine trades on, so an unthrottled caller does not merely slow the
    // dashboard — it can get the trading path rate-limited.
    //
    //  * Read-only deployments (the public demo) never honor it. Nobody there is
    //    about to close a position by hand, which is the only thing it is for.
    //  * Otherwise a floor of `FRESH_BYPASS_MIN_INTERVAL` between bypasses, which
    //    is far below a human refresh cadence and far above a loop's. The button
    //    disables itself while in flight; this bounds every other caller,
    //    including one with no API key on an exposed :9000.
    const FRESH_BYPASS_MIN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);
    static LAST_BYPASS: OnceLock<StdMutex<Option<Instant>>> = OnceLock::new();
    let bypass_cache = q.fresh.unwrap_or(0) != 0
        && !s.read_only
        && {
            let cell = LAST_BYPASS.get_or_init(|| StdMutex::new(None));
            match cell.lock() {
                Ok(mut last) => {
                    let ok = last.map_or(true, |t| t.elapsed() >= FRESH_BYPASS_MIN_INTERVAL);
                    if ok { *last = Some(Instant::now()); }
                    ok
                }
                // A poisoned lock must not become a way to bypass the throttle.
                Err(_) => false,
            }
        };

    let mut out: Vec<Quote> = Vec::new();
    for pos in db::get_open_positions(&pool).await {
        // Serve from cache when it is young enough. The lock is a std Mutex held
        // only for the lookup — never across the awaits below.
        if !bypass_cache {
            if let Ok(guard) = cache.lock() {
                if let Some((q, at)) = guard.get(&pos.token_id) {
                    if at.elapsed() < ttl {
                        let mut q = q.clone();
                        q.age_secs = at.elapsed().as_secs();
                        out.push(q);
                        continue;
                    }
                }
            }
        }

        let Ok(token) = U256::from_str(&pos.token_id) else { continue };
        let bid = fetch_side_price(&session, token, Side::Buy).await;
        let ask = fetch_side_price(&session, token, Side::Sell).await;
        let mid = match (bid, ask) {
            (Some(b), Some(a)) => Some((b + a) / Decimal::TWO),
            _ => None,
        };

        // Write the mid through so every DB-derived consumer inherits the
        // freshness instead of waiting for the next 300s sweep.
        //
        // `refresh_position_mark`, NOT `update_position_current_price`: the
        // latter also confirms the row, and a book existing is not evidence a
        // fill happened. See that function's doc for the fabricated-trade chain
        // that caused. Skipped entirely in read-only mode, where a GET must not
        // write.
        if let Some(m) = mid {
            if !s.read_only {
                db::refresh_position_mark(&pool, &pos.token_id, m).await;
            }
        }

        let quote = Quote {
            token_id: pos.token_id.clone(),
            bid: bid.map(|d| d.to_string()),
            ask: ask.map(|d| d.to_string()),
            mid: mid.map(|d| d.to_string()),
            age_secs: 0,
        };
        if let Ok(mut guard) = cache.lock() {
            guard.insert(pos.token_id.clone(), (quote.clone(), Instant::now()));
            // Drop tokens that are no longer open so a long session's rotations
            // cannot grow this without bound.
            guard.retain(|_, (_, at)| at.elapsed().as_secs() < 3600);
        }
        out.push(quote);
    }

    Json(out).into_response()
}

/// One side's best price, or `None` when the venue has no quote or is slow.
///
/// A missing price is reported as missing rather than substituted, because the
/// caller is an operator deciding whether to sell: an invented number is worse
/// than a blank.
#[cfg(feature = "intl_clob")]
async fn fetch_side_price(
    session: &crate::cag::session::SessionState,
    token: U256,
    side: Side,
) -> Option<Decimal> {
    let req = PriceRequest::builder().token_id(token).side(side).build();
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        session.venue.trading_client().price(&req),
    ).await {
        // A zero is not a price. The venue returns it rather than an error when
        // a side is empty — manual_exit already guards on `current_bid <= 0` for
        // this reason. Passing it through would paint a $0.0000 row wearing the
        // green "bid" badge, with unrealized P&L of the entire position.
        Ok(Ok(r)) if r.price > Decimal::ZERO => Some(r.price),
        Ok(Ok(_)) => { debug!("quote: venue returned a non-positive price for {token}"); None }
        Ok(Err(e)) => { debug!("quote: price fetch failed for {token}: {e}"); None }
        Err(_)     => { debug!("quote: price fetch timed out for {token}"); None }
    }
}

async fn get_open_positions(Query(q): Query<AssetQuery>) -> Response {
    debug!("Received GET /api/positions request");
    if q.asset.is_none() {
        let mut all: Vec<db::OpenPositionRow> = Vec::new();
        for pool in db::pools_for_opt(None) {
            all.extend(db::get_open_positions(&pool).await);
        }
        return Json(all).into_response();
    }
    match db::pool_for_opt_retry(q.asset.as_deref()).await {
        Some(pool) => {
            let positions = db::get_open_positions(&pool).await;
            Json(positions).into_response()
        },
        None => {
            log_pool_unavailable(&format!("GET /api/positions (asset={:?})", q.asset), q.asset.as_deref());
            Json(Vec::<db::OpenPositionRow>::new()).into_response()
        },
    }
}

/// GET /api/positions/pending?asset=btc
///
/// Returns only pending positions (Viper Launches) - orders placed but not yet confirmed on-chain.
async fn get_pending_positions(Query(q): Query<AssetQuery>) -> Response {
    debug!("Received GET /api/positions/pending request");
    match db::pool_for_opt_retry(q.asset.as_deref()).await {
        Some(pool) => {
            let positions = db::get_pending_positions(&pool).await;
            Json(positions).into_response()
        },
        None => {
            log_pool_unavailable(&format!("GET /api/positions/pending (asset={:?})", q.asset), q.asset.as_deref());
            Json(Vec::<db::OpenPositionRow>::new()).into_response()
        },
    }
}

/// GET /api/positions/confirmed?asset=btc
///
/// Returns only confirmed positions (Viper Missions In-Flight) - verified on-chain.
async fn get_confirmed_positions(Query(q): Query<AssetQuery>) -> Response {
    debug!("Received GET /api/positions/confirmed request");
    match db::pool_for_opt_retry(q.asset.as_deref()).await {
        Some(pool) => {
            let positions = db::get_confirmed_positions(&pool).await;
            Json(positions).into_response()
        },
        None => {
            log_pool_unavailable(&format!("GET /api/positions/confirmed (asset={:?})", q.asset), q.asset.as_deref());
            Json(Vec::<db::OpenPositionRow>::new()).into_response()
        },
    }
}

/// DELETE /api/positions/{token_id}?asset=btc
///
/// Purges a specific row from `open_positions` by token_id (decimal U256 string).
async fn delete_open_position(Path(token_id): Path<String>, Query(q): Query<AssetQuery>) -> Response {
    debug!("Received DELETE /api/positions/{}", token_id);
    let pool = match db::pool_for_opt_retry(q.asset.as_deref()).await {
        Some(p) => p,
        None => {
            log_pool_unavailable(&format!("DELETE /api/positions (asset={:?})", q.asset), q.asset.as_deref());
            return (StatusCode::SERVICE_UNAVAILABLE, "DB unavailable").into_response();
        }
    };
    match sqlx::query("DELETE FROM open_positions WHERE token_id = ?")
        .bind(&token_id)
        .execute(&pool)
        .await
    {
        Ok(r) if r.rows_affected() > 0 => {
            info!("️ Purged stale open_position row for token {}", &token_id[..token_id.len().min(20)]);
            (StatusCode::OK, format!("Deleted {} row(s)", r.rows_affected())).into_response()
        }
        Ok(_) => {
            warn!("DELETE /api/positions/{}: token_id not found in open_positions", token_id);
            (StatusCode::NOT_FOUND, "token_id not found").into_response()
        }
        Err(e) => {
            error!("DELETE /api/positions/{} DB error: {}", token_id, e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// POST /api/positions/sync
///
/// Triggers an immediate two-way reconciliation of the `open_positions` DB table
/// against the wallet's live on-chain holdings via the Polymarket Data API:
///
///   PURGE  — removes rows for tokens no longer held on-chain (settled, expired,
///            redeemed, or sold externally on the Polymarket UI).
///   ADOPT  — re-inserts on-chain positions that have no DB row.
///
/// Normally runs automatically at startup and every 300 s.  Call this endpoint
/// after manually "clearing" settled losses in the Polymarket UI to immediately
/// reflect the cleared state in DRADIS without waiting for a bot restart.
///
/// Returns: `{ "message": "Chain sync complete" }`
#[cfg(feature = "intl_clob")]
async fn sync_positions(State(s): State<ApiState>) -> Response {
    info!(" Manual chain-sync triggered via POST /api/positions/sync");
    sync_open_positions_with_chain(s.safe_address).await;
    (StatusCode::OK, Json(serde_json::json!({ "message": "Chain sync complete" }))).into_response()
}

/// POST /api/positions/manual-exit
///
/// Execute a manual "Return to Base" exit for a specific position.
///
/// Flow:
///  1. Lookup position in DB to get entry price and shares
///  2. Place FAK (Fill-And-Kill) market sell order at current bid
///  3. Wait up to 10s for order to fill
///  4. Record trade with actual exit price and P&L
///  5. Close position in DB
///
/// Returns 200 with trade details on success, 4xx/5xx on failure.
#[cfg(feature = "intl_clob")]
async fn manual_exit(
    State(s): State<ApiState>,
    Json(req): Json<ManualExitRequest>,
) -> Response {
    info!(" RTB: Manual exit request for token {} [{}]", &req.token_id[..req.token_id.len().min(20)], req.strategy);

    // ── Step 1: Get session for this asset ────────────────────────────────────
    let session = match s.cag.session_for_asset(&req.asset) {
        Some(sess) => sess,
        None => {
            warn!("RTB: Asset '{}' not found in CAG sessions", req.asset);
            return (StatusCode::BAD_REQUEST, "Asset not found").into_response();
        }
    };

    // ── Step 2: Lookup position in DB to get entry price and shares ───────────
    let pool = match db::pool_for(&req.asset) {
        Some(p) => p,
        None => {
            log_pool_unavailable(&format!("RTB: asset {}", req.asset), Some(req.asset.as_str()));
            return (StatusCode::SERVICE_UNAVAILABLE, "DB unavailable").into_response();
        }
    };

    #[derive(sqlx::FromRow)]
    struct PositionRow {
        entry_price: String,
        shares: String,
        /// Was this position simulated? RTB had no ghost gate at all, so pressing
        /// it on a ghost row fired a REAL sell for shares the wallet does not
        /// hold. The order normally bounces, but the handler booked a completed
        /// exit either way.
        ghost_mode: i64,
        /// Dollars already paid to open this position. The automated exit books
        /// `entry_fee + exit_fee`; the manual one booked neither, so a manual
        /// close overstated P&L by the entry fee and then deleted the row that
        /// carried it, losing the record entirely.
        entry_fee: Option<String>,
    }

    let pos_result = sqlx::query_as::<_, PositionRow>(
        "SELECT entry_price, shares, ghost_mode, entry_fee FROM open_positions WHERE token_id = ? AND strategy = ?"
    )
    .bind(&req.token_id)
    .bind(&req.strategy)
    .fetch_one(&pool)
    .await;

    let (entry_price, shares, position_is_ghost, entry_fee) = match pos_result {
        Ok(row) => {
            let entry = match Decimal::from_str(&row.entry_price) {
                Ok(p) => p,
                Err(e) => {
                    error!("RTB: Invalid entry_price in DB: {}", e);
                    return (StatusCode::INTERNAL_SERVER_ERROR, "Invalid entry price").into_response();
                }
            };
            let shares = match Decimal::from_str(&row.shares) {
                Ok(s) => s,
                Err(e) => {
                    error!("RTB: Invalid shares in DB: {}", e);
                    return (StatusCode::INTERNAL_SERVER_ERROR, "Invalid shares").into_response();
                }
            };
            let entry_fee = row.entry_fee.as_deref()
                .and_then(|f| Decimal::from_str(f).ok())
                .unwrap_or(Decimal::ZERO);
            (entry, shares, row.ghost_mode != 0, entry_fee)
        }
        Err(sqlx::Error::RowNotFound) => {
            warn!("RTB: Position not found in DB (token={}, strategy={})", req.token_id, req.strategy);
            return (StatusCode::NOT_FOUND, "Position not found").into_response();
        }
        Err(e) => {
            error!("RTB: Database error: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, "Database error").into_response();
        }
    };

    // ── Step 3: Parse inputs ───────────────────────────────────────────────────
    let token_id = match U256::from_str(&req.token_id) {
        Ok(t) => t,
        Err(e) => {
            error!("RTB: Invalid token_id: {}", e);
            return (StatusCode::BAD_REQUEST, "Invalid token_id").into_response();
        }
    };

    // ── Fetch the LIVE best bid SERVER-SIDE ────────────────────────────────────
    // The client's `req.current_bid` is a hardcoded placeholder ("0.5") and is
    // deliberately IGNORED. Using it would price the FAK sell at 0.5 regardless of
    // the real market, so any position whose true bid is below 0.5 would never fill
    // (RTB silently leaves underwater positions open). Query the CLOB for the
    // current best bid. NOTE: the CLOB /price endpoint returns the best resting
    // order ON the requested side of the book — side=Buy → best bid, side=Sell →
    // best ask (verified empirically 2026-07-24: RTB used Side::Sell, got the ask,
    // priced the FAK sell 1¢ under the ask but above the real bid → "no orders
    // found to match with FAK order").
    let current_bid = {
        let price_req = PriceRequest::builder().token_id(token_id).side(Side::Buy).build();
        match tokio::time::timeout(
            std::time::Duration::from_secs(10),
            session.venue.trading_client().price(&price_req),
        ).await {
            Ok(Ok(r)) => r.price,
            Ok(Err(e)) => {
                error!("RTB: failed to fetch best bid from CLOB: {}", e);
                return (StatusCode::BAD_GATEWAY, format!("Could not fetch current bid: {}", e)).into_response();
            }
            Err(_) => {
                error!("RTB: best-bid lookup timed out (10s)");
                return (StatusCode::GATEWAY_TIMEOUT, "Bid lookup timed out").into_response();
            }
        }
    };
    if current_bid <= Decimal::ZERO {
        warn!("RTB: no live bid for token (bid={}) — cannot place sell", current_bid);
        return (StatusCode::CONFLICT, "No live bid available to sell into").into_response();
    }
    info!("RTB: live best bid = ${:.4}", current_bid);

    // ── Resolve the EIP-712 verifying contract SERVER-SIDE ─────────────────────
    // The client-supplied `req.verifying_contract` is deliberately IGNORED: the
    // Control Tower sends a stale/hardcoded CTF Exchange address, which yields the
    // wrong EIP-712 domain and a "invalid POLY_GNOSIS_SAFE signature" rejection.
    // Derive neg-risk status from the CLOB (same lookup used at market discovery)
    // and pick the matching exchange — exactly as every automated order path does.
    let is_neg_risk = match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        session.venue.trading_client().neg_risk(token_id),
    ).await {
        Ok(Ok(r)) => r.neg_risk,
        Ok(Err(e)) => {
            error!("RTB: neg_risk lookup failed for token: {}", e);
            return (StatusCode::BAD_GATEWAY, format!("neg_risk lookup failed: {}", e)).into_response();
        }
        Err(_) => {
            error!("RTB: neg_risk lookup timed out (10s)");
            return (StatusCode::GATEWAY_TIMEOUT, "neg_risk lookup timed out").into_response();
        }
    };
    let verifying_contract = crate::venues::intl::exchange_verifying_contract(is_neg_risk);
    info!("RTB: resolved exchange {} (neg_risk={})", verifying_contract, is_neg_risk);

    // ── Step 4: Place FAK market sell order ────────────────────────────────────
    // Shave SELL_PRICE_OFFSET below the live bid (floored at MIN_SELL_LIMIT_PRICE)
    // so the FAK limit is marketable and clears against the resting bid — the same
    // convention every automated exit path uses.
    let sell_price = round_to_tick_size(
        (current_bid - crate::config::SELL_PRICE_OFFSET).max(crate::config::MIN_SELL_LIMIT_PRICE)
    );

    // ── Ghost gate ────────────────────────────────────────────────────────────
    // A simulated position holds nothing on-chain. Selling it for real is both
    // wrong and, when the venue happens to accept it, a real trade the operator
    // never asked for. Book the simulated exit at the live bid and stop here.
    if position_is_ghost {
        // Ghost mode exists so an operator can trust the numbers before risking
        // money, so a simulated exit must be booked as completely as a real one.
        // The automated ghost path books a simulated EXIT fee and credits the
        // session; this one did neither on its first draft. Ghost entries carry
        // no entry fee today (patrol inserts them with `entry_fee: ZERO`), so
        // the two terms agree at zero — but subtract the reported `fees` rather
        // than the exit fee alone, so the P&L and the fees column cannot drift
        // apart if ghost entries ever gain a simulated entry fee.
        let fee_rate = crate::venues::intl::live_taker_fee_rate();
        let sim_exit_fee = crate::venues::intl::taker_fee(fee_rate, current_bid, shares);
        let fees = entry_fee + sim_exit_fee;
        let pnl = (current_bid - entry_price) * shares - fees;
        info!("👻 RTB (ghost): {} shares | entry ${:.4} → bid ${:.4} | simulated P&L ${:.4} (net of ${:.4} fees)",
              shares, entry_price, current_bid, pnl, fees);

        let mut scope = crate::state::TradeScope::shard_only(&req.asset);
        scope.ghost = true;
        metrics::record_trade(
            &scope, fees, req.strategy.clone(), req.market.clone(), req.side.clone(),
            entry_price, current_bid, shares, pnl,
            "Manual RTB (ghost)".to_string(),
        ).await;
        *session.total_pnl.lock().await += pnl;

        db::close_open_position(&pool, &req.strategy, &req.token_id).await;
        let market = crate::venues::intl::market_id_from_u256(token_id);
        {
            let mut pos_map = session.positions.lock().await;
            pos_map.retain(|k, _| !(k.strategy == req.strategy && k.market == market));
        }
        // Ghost entries claim token ownership exactly like real ones, and the
        // automated exit releases the claim for both. Omitting it here left the
        // token unenterable for the rest of the session — the same leak the real
        // close path below documents, in the branch a new customer hits first,
        // since ghost is the default first-run mode.
        session.token_ownership.lock().await.remove(&market);

        return Json(serde_json::json!({
            "status": "closed", "ghost": true,
            "shares": shares.to_string(), "exit_price": current_bid.to_string(),
            "pnl": pnl.to_string(), "fees": fees.to_string(),
        })).into_response();
    }

    info!(" RTB: Placing FAK sell order — {} shares @ ${:.4} (live bid ${:.4})", shares, sell_price, current_bid);

    let order_result = place_limit_order_filled(
        session.venue.trading_client(),
        session.venue.nonce_manager(),
        session.venue.signer(),
        s.safe_address,
        session.venue.eoa_address(), // signer EOA — must match the API key's address
        verifying_contract,
        &crate::venues::intl::market_id_from_u256(token_id),
        Side::Sell,
        shares,
        sell_price,
        0, // fee_rate_bps (unused in V2)
        crate::venues::core::TimeInForce::Fak,
        false, // not post-only
        0, // expiration_secs (FAK doesn't need expiration)
        session.venue.shared_http(),
    ).await;

    // ── Step 5: Read the ACTUAL fill from the FAK response ────────────────────
    //
    // A FAK fills or dies inside the POST, so `place_limit_order_filled` returns
    // the matched amounts synchronously and there is nothing to wait for. This
    // used to call the wrapper that DISCARDS those amounts, sleep 10 seconds,
    // and then book the full share count at the shaved limit price with zero
    // fees — the same one-tick understatement the automated path fixed on
    // 2026-08-11, plus a killed or partial FAK booked as a completed exit.
    let (order_id, filled_shares, fill_price) = match order_result {
        Ok((id, making, taking)) => {
            // SELL orientation: making = shares given up, taking = USDC received.
            let px = if making > dec!(0) && taking > dec!(0) {
                let p = taking / making;
                if p > dec!(0) && p <= dec!(1) { p } else { sell_price }
            } else {
                sell_price
            };
            info!("✅ RTB: FAK responded (order_id={}) — filled {} of {} @ ${:.4}",
                  id, making, shares, px);
            (id, making, px)
        }
        Err(e) => {
            error!("RTB: Order placement failed: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("Order failed: {}", e)).into_response();
        }
    };

    // Nothing matched. The position is untouched, so touch nothing: booking here
    // is what left a real position closed in the ledger and unmanaged in memory.
    if filled_shares <= dec!(0) {
        warn!("RTB: FAK missed — no fill. Position left intact.");
        return (
            StatusCode::CONFLICT,
            "FAK missed the book — nothing filled, position unchanged. Try again.",
        ).into_response();
    }

    // ── Step 6: Book the real fill, net of fees ───────────────────────────────
    // A remainder below the venue's minimum order size cannot be traded, so it is
    // not a partial fill — it is a full close with dust. Using a tighter
    // threshold than `MIN_ORDER_SHARES` would manufacture sub-minimum positions
    // that no exit path can ever close normally, which is the one failure mode
    // this handler must not create.
    let remaining_raw = (shares - filled_shares).max(dec!(0));
    let partial = remaining_raw >= crate::config::MIN_ORDER_SHARES;
    let remaining = if partial { remaining_raw } else { dec!(0) };
    let booked_shares = if partial { filled_shares } else { shares };

    // Both ends of the round trip. The entry fee is prorated to the portion being
    // booked; a partial leaves the rest on the row for whatever closes it later.
    let fee_rate = crate::venues::intl::live_taker_fee_rate();
    let exit_fee = crate::venues::intl::taker_fee(fee_rate, fill_price, filled_shares);
    let entry_fee_booked = if partial && shares > dec!(0) {
        entry_fee * (booked_shares / shares)
    } else {
        entry_fee
    };
    let fees = entry_fee_booked + exit_fee;
    let pnl = (fill_price - entry_price) * booked_shares - fees;

    info!(" RTB: {} — {} of {} shares | entry ${:.4} → exit ${:.4} | P&L ${:.4} (net of ${:.4} fees)",
          if partial { "PARTIAL" } else { "closed" },
          filled_shares, shares, entry_price, fill_price, pnl, fees);

    // A manual RTB knows which book to write to but nothing about the market's
    // taxonomy — file it under the shard's venue and leave the rest NULL.
    metrics::record_trade(
        &crate::state::TradeScope::shard_only(&req.asset),
        fees,
        req.strategy.clone(),
        req.market.clone(),
        req.side.clone(),
        entry_price,
        fill_price,
        booked_shares,
        pnl,
        if partial {
            "Manual RTB (partial fill)".to_string()
        } else {
            "Manual RTB (Return to Base)".to_string()
        },
    ).await;

    // Credit the session. Every automated exit does this; the manual one never
    // did, so a session's P&L silently omitted whatever the operator closed by
    // hand and the drawdown guard was computed from an incomplete figure.
    *session.total_pnl.lock().await += pnl;

    // ── Step 7: Close, or reduce on a partial fill ────────────────────────────
    let market = crate::venues::intl::market_id_from_u256(token_id);
    if partial {
        // The rest is still on the book and still ours. Booking a full close
        // here is what left real shares with no strategy managing them.
        // Prorate the surviving entry fee with the surviving shares, the same way
        // the chain-adoption path rewrites it. Leaving the full fee on a reduced
        // row would double-count it against whatever closes the remainder.
        let remaining_entry_fee = if shares > dec!(0) {
            entry_fee * (remaining / shares)
        } else {
            Decimal::ZERO
        };
        if let Err(e) = sqlx::query(
            "UPDATE open_positions SET shares = ?, entry_fee = ? WHERE token_id = ? AND strategy = ?"
        )
        .bind(remaining.to_string())
        .bind(remaining_entry_fee.to_string())
        .bind(&req.token_id)
        .bind(&req.strategy)
        .execute(&pool)
        .await {
            error!("RTB: could not reduce position after partial fill: {e}");
        }
        let mut pos_map = session.positions.lock().await;
        for (k, p) in pos_map.iter_mut() {
            if k.strategy == req.strategy && k.market == market {
                // Scale the in-memory entry fee with the shares, exactly as the
                // DB row above. The automated exit computes fees from the
                // in-memory Position, so leaving the full original fee here
                // would charge it a second time when the strategy closes the
                // remainder.
                if p.shares > dec!(0) {
                    p.entry_fee = p.entry_fee * (remaining / p.shares);
                }
                p.shares = remaining;
            }
        }
        warn!("RTB: partial fill — {} shares remain open and still managed", remaining);
    } else {
        db::close_open_position(&pool, &req.strategy, &req.token_id).await;

        // ── Step 8: Remove from in-memory positions map ────────────────────────
        {
            let mut pos_map = session.positions.lock().await;
            // The operator names a strategy and a token, not a squadron, so remove
            // every squadron's entry for that pair. On this venue there is one
            // squadron per asset, which makes it exactly the previous behavior;
            // once several squadrons can hold the same token, a manual RTB from
            // this endpoint should carry the squadron so it targets just one.
            let before = pos_map.len();
            pos_map.retain(|k, _| !(k.strategy == req.strategy && k.market == market));
            if before - pos_map.len() > 1 {
                warn!("RTB: removed {} squadrons' entries for {}/{}", before - pos_map.len(), req.strategy, market);
            }
        }

        // Release the token-ownership claim. Without this the token stayed
        // claimed for the rest of the session and no strategy could re-enter it,
        // which is invisible until an operator wonders why nothing trades a
        // market they exited hours ago.
        session.token_ownership.lock().await.remove(&market);
    }

    info!("✅ RTB: Manual exit complete — order_id={}", order_id);

    /// What actually happened, not what was requested.
    ///
    /// The old response echoed the requested share count and the shaved LIMIT
    /// price, so a partial fill and a full one were indistinguishable to the UI
    /// and the operator was shown a price they did not get.
    #[derive(Serialize)]
    struct ExitResponse {
        success: bool,
        /// "closed" or "partial".
        status: &'static str,
        order_id: String,
        /// Real average fill price from the FAK response.
        exit_price: String,
        /// Shares actually sold.
        filled_shares: String,
        /// Shares still open and still managed. "0" on a full close.
        remaining_shares: String,
        /// Net of the exit taker fee.
        pnl: String,
        fees: String,
    }

    Json(ExitResponse {
        success: true,
        status: if partial { "partial" } else { "closed" },
        order_id,
        exit_price: fill_price.to_string(),
        filled_shares: filled_shares.to_string(),
        remaining_shares: remaining.to_string(),
        pnl: pnl.to_string(),
        fees: fees.to_string(),
    }).into_response()
}

/// GET /api/llm/recommendations?limit=10&asset=btc
///
/// Returns up to `limit` LLM Advisor analyzes, newest first.
/// Each row: { id, ts, model, trade_count, session_pnl, analysis }
async fn get_llm_recommendations(Query(q): Query<AssetQuery>) -> Response {    debug!("Received GET /api/llm/recommendations request with limit: {:?}", q.limit);
    let limit = q.limit.unwrap_or(10).clamp(1, 50);
    match db::pool_for_opt_retry(q.asset.as_deref()).await {
        Some(pool) => {
            let recs = db::get_recent_llm_recommendations(&pool, limit).await;
            debug!("Successfully retrieved {} LLM recommendations", recs.len());
            Json(recs).into_response()
        },
        None => {
            log_pool_unavailable("GET /api/llm/recommendations", q.asset.as_deref());
            Json(Vec::<db::LlmRecommendationRow>::new()).into_response()
        },
    }
}

// ─── LLM autonomy: action queue + approval flow (Epic S4) ────────────────────

/// GET /api/llm/actions?limit=100
///
/// The AI action audit trail, newest first (proposed/applied/rejected/
/// expired/reverted/failed). Sweeps TTL-expired proposals first so the
/// approval queue never shows stale rows.
async fn get_llm_actions(Query(q): Query<AssetQuery>) -> Response {
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    match db::pool() {
        Some(pool) => {
            db::expire_stale_llm_actions(&pool).await;
            Json(db::fetch_llm_actions(&pool, limit).await).into_response()
        }
        None => Json(Vec::<db::LlmActionRow>::new()).into_response(),
    }
}

/// POST /api/llm/actions/{id}/approve
///
/// Human approval for a `proposed` change (tier 1, or a tier-2/3 hold).
/// The stored value is REVALIDATED against the *current* config and schema —
/// market conditions and config may have moved since the LLM proposed it —
/// then applied via the same hot-patch path as PATCH /api/config.
async fn approve_llm_action(
    State(s): State<ApiState>,
    AxumPath(id): AxumPath<i64>,
) -> Response {
    use crate::helpers::llm_patch::{self, RawProposal};
    info!("📥 Received POST /api/llm/actions/{id}/approve");
    let Some(pool) = db::pool() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "database unavailable").into_response();
    };
    db::expire_stale_llm_actions(&pool).await;
    let Some(row) = db::fetch_llm_action_by_id(&pool, id).await else {
        return (StatusCode::NOT_FOUND, "no such action").into_response();
    };
    if row.status != "proposed" {
        return (
            StatusCode::CONFLICT,
            format!("action is '{}', only 'proposed' actions can be approved", row.status),
        ).into_response();
    }

    // Approve against the SQUADRON the proposal was reasoned about.
    //
    // This used to revalidate against, and apply to, the global config — which no
    // patrol loop reads. The endpoint returned 200 and the row was stamped
    // `applied` while no live parameter moved: the operator was told their
    // approval had taken effect when it had not.
    //
    // A row with no squadron_id predates the squadron-scoped advisor. It cannot
    // be applied anywhere meaningful, so say so rather than repeating the lie.
    let Some(squadron_id) = row.squadron_id.clone().filter(|v| !v.is_empty()) else {
        let detail = "this proposal predates squadron-scoped advice and targets a config no strategy reads - reject it and let the advisor re-propose".to_string();
        db::update_llm_action_status(&pool, id, "rejected", Some(&detail), None).await;
        return (StatusCode::CONFLICT, detail).into_response();
    };

    // Re-validate against the squadron's live config (apply-time revalidation).
    let current = DynamicConfig::load_for_squadron(&squadron_id).await;
    let to: serde_json::Value = serde_json::from_str(&row.to_value)
        .unwrap_or(serde_json::Value::String(row.to_value.clone()));
    let raw = RawProposal { field: row.field.clone(), to, reason: row.reason.clone() };
    let batch = llm_patch::validate_proposals(vec![raw], &current);
    let Some(change) = batch.accepted.first() else {
        let why = batch.rejected.first()
            .map(|r| r.why.clone())
            .unwrap_or_else(|| "no longer valid against current config".into());
        let detail = format!("approval failed revalidation: {why}");
        db::update_llm_action_status(&pool, id, "rejected", Some(&detail), None).await;
        return (StatusCode::CONFLICT, detail).into_response();
    };

    let patch = serde_json::json!({ change.key.clone(): change.to.clone() }).to_string();
    // apply_squadron_patch_as persists, records the diff for revert, and pushes
    // into the running squadron's live handle so the next patrol tick sees it.
    match DynamicConfig::apply_squadron_patch_as(&squadron_id, &patch, "llm_approved").await {
        Ok(_) => {
            let inverse = serde_json::json!({ change.key.clone(): change.from.clone() }).to_string();
            let pnl = db::get_pnl_history(&pool, 1).await.first()
                .and_then(|p| p.session_pnl.parse::<f64>().ok())
                .unwrap_or(0.0);
            let detail = format!(
                "approved by operator{}",
                if change.clamped { " (re-clamped to schema bounds)" } else { "" },
            );
            db::mark_llm_action_applied(&pool, id, &detail, &inverse, pnl).await;
            info!("✅ LLM action {id} approved & applied to {squadron_id}: {} -> {}", change.key, change.to);
            match db::fetch_llm_action_by_id(&pool, id).await {
                Some(updated) => Json(updated).into_response(),
                None => StatusCode::OK.into_response(),
            }
        }
        Err(e) => {
            let detail = format!("apply error: {e}");
            db::update_llm_action_status(&pool, id, "failed", Some(&detail), None).await;
            error!("❌ LLM action {id} approve failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, detail).into_response()
        }
    }
}

/// POST /api/llm/actions/{id}/reject
///
/// Human rejection of a `proposed` change. Rejected rows feed the few-shot
/// retraining corpus (S7) as negative examples.
async fn reject_llm_action(AxumPath(id): AxumPath<i64>) -> Response {
    info!("📥 Received POST /api/llm/actions/{id}/reject");
    let Some(pool) = db::pool() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "database unavailable").into_response();
    };
    let Some(row) = db::fetch_llm_action_by_id(&pool, id).await else {
        return (StatusCode::NOT_FOUND, "no such action").into_response();
    };
    if row.status != "proposed" {
        return (
            StatusCode::CONFLICT,
            format!("action is '{}', only 'proposed' actions can be rejected", row.status),
        ).into_response();
    }
    db::update_llm_action_status(&pool, id, "rejected", Some("rejected by operator"), None).await;
    match db::fetch_llm_action_by_id(&pool, id).await {
        Some(updated) => Json(updated).into_response(),
        None => StatusCode::OK.into_response(),
    }
}

/// GET /api/portfolio
///
/// Returns aggregated portfolio value across all assets:
/// - collateral: total pUSD cash
/// - positions_value: sum of (shares × current_mid_price) for all open positions
/// - total_value: collateral + positions_value
/// - unrealized_pnl: sum of (shares × (current_mid - entry_price))
/// - position_count: total number of open positions
/// - prices_live: true if CLOB prices are fresh
///
/// This endpoint aggregates data from all asset pools (BTC, ETH, SOL, etc.)
#[derive(Serialize)]
struct PortfolioValue {
    collateral: String,
    positions_value: String,
    total_value: String,
    unrealized_pnl: String,
    position_count: usize,
    prices_live: bool,
}

async fn get_portfolio_value(State(s): State<ApiState>) -> Response {
    debug!("Received GET /api/portfolio request");
    // `s` is only consulted for the intl on-chain balance probe below.
    #[cfg(not(feature = "intl_clob"))]
    let _ = &s;

    use rust_decimal::Decimal;
    use std::str::FromStr;
    use chrono::{Utc, Duration};

    let assets = db::available_assets();

    // Fetch live wallet collateral as ground truth (10s timeout)
    // US custodial venue exposes no on-chain wallet balance here yet (Step 3b);
    // fall back to the DB-tracked collateral snapshot below. (Same for Kalshi
    // until its Execution::collateral is threaded into this endpoint.)
    #[cfg(not(feature = "intl_clob"))]
    let live_collateral: Option<Decimal> = None;
    #[cfg(feature = "intl_clob")]
    let live_collateral = {
        use polymarket_client_sdk_v2::clob::types::request::BalanceAllowanceRequest;
        use polymarket_client_sdk_v2::clob::types::AssetType;

        // Get first available session to access trading_client
        let session = assets.iter().find_map(|a| s.cag.session_for_asset(a));

        if let Some(sess) = session {
            let mut req = BalanceAllowanceRequest::default();
            req.asset_type = AssetType::Collateral;

            match tokio::time::timeout(
                std::time::Duration::from_secs(10),
                sess.venue.trading_client().balance_allowance(req),
            ).await {
                Ok(Ok(resp)) => {
                    let balance = Decimal::from_str(&resp.balance.to_string())
                        .unwrap_or(Decimal::ZERO) / Decimal::from_str("1000000").unwrap();
                    debug!(" Live wallet collateral from CLOB: ${:.4}", balance);
                    Some(balance)
                }
                Ok(Err(e)) => {
                    warn!("⚠️ CLOB balance fetch failed in /api/portfolio: {}", e);
                    None
                }
                Err(_) => {
                    warn!("⚠️ CLOB balance fetch timed out (10s) in /api/portfolio");
                    None
                }
            }
        } else {
            None
        }
    };

    let mut latest_collateral: Option<(String, Decimal)> = None;
    let mut total_positions_value = Decimal::ZERO;
    let mut total_unrealized_pnl = Decimal::ZERO;
    let mut total_position_count = 0;
    let mut all_prices_live = true;

    // Freshness threshold: snapshots older than this are considered stale
    let freshness_threshold = Utc::now() - Duration::minutes(5);

    // Aggregate across all asset pools
    for asset in &assets {
        let pool = match db::pool_for(asset) {
            Some(p) => p,
            None => continue,
        };

        // Collateral is wallet-global, not asset-scoped. Each asset DB stores the
        // same wallet cash snapshot, so we keep only the freshest one (but we'll
        // prefer live_collateral from CLOB if available).
        let pnl_snapshots = db::get_pnl_history(&pool, 1).await;
        if let Some(snap) = pnl_snapshots.first() {
            if let Ok(collateral) = Decimal::from_str(&snap.collateral) {
                match &latest_collateral {
                    Some((ts, _)) if ts >= &snap.ts => {}
                    _ => latest_collateral = Some((snap.ts.clone(), collateral)),
                }
            }
        }

        // Build a deduped view of positions by token_id for count/fallback valuation.
        // The on-chain wallet holds ONE balance per token, so rows that share a
        // token_id (e.g. an Arbitrage leg and a TrendCapture leg on the same outcome)
        // must be valued ONCE — never summed (double-count) nor arbitrarily dropped.
        //
        // Prefer the CHAIN-ADOPTED row: chain-sync stamps it to the wallet's real
        // on-chain size (and purges it when the token is no longer held), so it is the
        // authoritative reflection of holdings. A non-adopted strategy row may be a
        // phantom that never settled on-chain (e.g. a same-token leg that overlaps an
        // existing position). Among rows with equal adoption status, prefer larger
        // shares. The genuinely-additive case self-heals on the next chain-sync, which
        // stamps every row for the token to the full on-chain size.
        let mut deduped_positions: std::collections::HashMap<String, db::OpenPositionRow> =
            std::collections::HashMap::new();
        for pos in db::get_open_positions(&pool).await {
            // Skip UNCONFIRMED phantoms: still `status='pending'` AND not chain-adopted
            // means the order was placed but never confirmed on-chain (never filled or
            // rejected). Valuing it inflates the portfolio with non-existent profit until
            // the 60-min purge grace elapses.
            //
            // This endpoint deliberately does NOT read the chain itself: the dashboard
            // polls it continuously, and a balance call per open position per poll would
            // hammer the venue. It relies instead on the 60s status task, which reconciles
            // every open position against on-chain holdings and writes the correction back
            // to this very row (`calculate_positions_value` in patrol_tasks.rs). That is
            // what keeps the banner and the snapshots one source of truth — and it is why
            // the row's `shares` and `chain_adopted` can be trusted here.
            //
            // The consequence worth knowing: this endpoint can be up to one status-task
            // cycle behind the chain. It cannot DIVERGE from the snapshot any more, but it
            // can lag it.
            if pos.status == "pending" && !pos.chain_adopted {
                continue;
            }
            match deduped_positions.get(&pos.token_id) {
                None => {
                    deduped_positions.insert(pos.token_id.clone(), pos);
                }
                Some(existing) => {
                    let existing_shares = Decimal::from_str(&existing.shares).unwrap_or(Decimal::ZERO);
                    let candidate_shares = Decimal::from_str(&pos.shares).unwrap_or(Decimal::ZERO);
                    let replace = (!existing.chain_adopted && pos.chain_adopted)
                        || (existing.chain_adopted == pos.chain_adopted && candidate_shares > existing_shares);
                    if replace {
                        deduped_positions.insert(pos.token_id.clone(), pos);
                    }
                }
            }
        }
        total_position_count += deduped_positions.len();

        // Check snapshot freshness before trusting mark-to-market valuation
        let snapshot_is_fresh = pnl_snapshots.first().and_then(|snap| {
            chrono::DateTime::parse_from_rfc3339(&snap.ts)
                .ok()
                .map(|dt| dt.with_timezone(&Utc) > freshness_threshold)
        }).unwrap_or(false);

        // Compute positions value and unrealized P&L from deduped positions.
        // Prefer current_price (live mark-to-market from chain-sync) when available.
        // Fall back to fresh pnl_snapshot total_value - collateral, then to cost basis.
        let mut asset_positions_value = Decimal::ZERO;
        let mut asset_unrealized_pnl = Decimal::ZERO;
        let mut has_live_prices = false;

        for (_, pos) in &deduped_positions {
            if let (Ok(shares), Ok(entry_price)) = (
                Decimal::from_str(&pos.shares),
                Decimal::from_str(&pos.entry_price),
            ) {
                let cost_basis = shares * entry_price;
                // A SIMULATED position contributes only what it has MADE.
                //
                // Opening a real position debits collateral by roughly its cost, so
                // crediting the full mark leaves the total steady on entry and then
                // tracking the position. A paper entry debits nothing, so crediting
                // the full mark adds the whole notional the moment it opens and
                // takes it back on exit: the banner jumps $8 for an $8 paper
                // position that has made nothing, and every entry reads as a gain on
                // the very chart an operator uses to judge the system.
                //
                // Kept in step with `calculate_positions_value`, which books the
                // same `shares × (mark − entry)` into pnl_snapshots. The two are
                // meant to be one source of truth and briefly were not.
                let ghost = pos.ghost_mode;
                if let Some(ref cp_str) = pos.current_price {
                    if let Ok(cur_price) = Decimal::from_str(cp_str) {
                        if cur_price > Decimal::ZERO {
                            let market_value = shares * cur_price;
                            asset_positions_value += if ghost {
                                market_value - cost_basis
                            } else {
                                market_value
                            };
                            asset_unrealized_pnl += market_value - cost_basis;
                            has_live_prices = true;
                            debug!(" [{}] token {} {} shares × cur=${:.4} = ${:.4} (entry=${:.4} pnl={:+.4})",
                                   asset.to_uppercase(), &pos.token_id[..pos.token_id.len().min(12)],
                                   shares, cur_price, market_value, entry_price,
                                   market_value - cost_basis);
                            continue;
                        }
                    }
                }
                // No current_price — fall through to snapshot or cost basis below
                // (tracked separately so we can mix per-position accuracy).
                // A simulated position with no mark has made nothing yet, so it
                // contributes nothing rather than its cost.
                if !ghost {
                    asset_positions_value += cost_basis;
                }
            }
        }

        if !has_live_prices && deduped_positions.is_empty() {
            // No positions — nothing to value
        } else if !has_live_prices {
            // No current_price on any position — try snapshot, then cost basis
            if snapshot_is_fresh {
                if let Some(snap) = pnl_snapshots.first() {
                    if let (Some(tv), Ok(collateral)) = (
                        snap.total_value.as_ref().and_then(|v| Decimal::from_str(v).ok()),
                        Decimal::from_str(&snap.collateral),
                    ) {
                        asset_positions_value = (tv - collateral).max(Decimal::ZERO);
                        debug!("✅ [{}] Fresh snapshot (no cur_price): positions_value = ${:.4}",
                               asset.to_uppercase(), asset_positions_value);
                    }
                }
            } else {
                all_prices_live = false;
                warn!("⚠️ [{}] No current_price and stale/missing snapshot — using cost basis",
                      asset.to_uppercase());
            }
        }

        if !has_live_prices {
            all_prices_live = false;
        }

        total_positions_value += asset_positions_value;
        total_unrealized_pnl  += asset_unrealized_pnl;
        debug!(" [{}] positions=${:.4} unrealized_pnl={:+.4}",
               asset.to_uppercase(), asset_positions_value, asset_unrealized_pnl);
    }

    // Use live CLOB collateral if available, otherwise fall back to latest snapshot
    let total_collateral = if let Some(live_bal) = live_collateral {
        live_bal
    } else {
        all_prices_live = false;
        latest_collateral
            .map(|(_, c)| c)
            .unwrap_or(Decimal::ZERO)
    };

    let total_value = total_collateral + total_positions_value;

    debug!(" Portfolio summary: collateral=${:.4} positions=${:.4} total=${:.4} count={} live={}",
           total_collateral, total_positions_value, total_value, total_position_count, all_prices_live);

    Json(PortfolioValue {
        collateral: total_collateral.to_string(),
        positions_value: total_positions_value.to_string(),
        total_value: total_value.to_string(),
        unrealized_pnl: total_unrealized_pnl.to_string(),
        position_count: total_position_count,
        prices_live: all_prices_live,
    }).into_response()
}

// ─── Squadron handlers (Phase 3d) ────────────────────────────────────────────

/// GET /api/squadrons
///
/// Returns a JSON array of all currently registered squadrons, sorted by
/// deployment time (oldest first).  Each entry is a `SquadronSummary`:
///
/// ```json
/// [
///   {
///     "id":          "btc-hourly-2026-05-29T14:00:00Z",
///     "asset":       "BTC",
///     "name":        "Full Wing — Will BTC …",
///     "state":       "PATROLLING",
///     "market_name": "Will BTC exceed $70,000 at 3 PM ET?",
///     "deployed_at": "2026-05-29T14:00:01Z"
///   }
/// ]
/// ```
async fn get_squadrons(State(s): State<ApiState>) -> Response {
    debug!("Received GET /api/squadrons");
    let mut list = s.cag.list_squadrons();
    for summary in &mut list {
        enrich_taxonomy(summary).await;
    }
    Json(list).into_response()
}

/// GET /api/squadrons/{id}
///
/// Returns the `SquadronSummary` for a single squadron, or 404 if unknown.
async fn get_squadron_by_id(
    State(s): State<ApiState>,
    Path(id): Path<String>,
) -> Response {
    debug!("Received GET /api/squadrons/{}", id);
    match s.cag.get_squadron(&id) {
        Some(mut summary) => {
            enrich_taxonomy(&mut summary).await;
            Json(summary).into_response()
        }
        None => {
            warn!("GET /api/squadrons/{}: not found", id);
            (StatusCode::NOT_FOUND, format!("squadron '{}' not found", id)).into_response()
        }
    }
}

/// Response for POST /api/squadrons/{id}/stand-down.
#[derive(Serialize)]
struct StandDownResponse {
    success: bool,
    squadron_id: String,
    /// Set when the class's auto-deploy switch was turned off as part of this.
    auto_deploy_disabled: Option<String>,
    message: String,
}

/// POST /api/squadrons/{id}/stand-down
///
/// Stop a squadron. The engine keeps running; this squadron's trade loop exits,
/// its resting orders are cancelled, and any position it holds is flattened or
/// left to settle by the venue's normal stand-down path.
///
/// There was no way to do this at all before — `Cag::stand_down` existed but no
/// route reached it, so the deploy endpoint's own error message ("stand it down
/// before deploying another") named a control the product did not have.
///
/// If the squadron belongs to a class DRADIS auto-deploys, the corresponding
/// switch is turned off in the same operation. Otherwise the seeder would notice
/// the class was empty and start a replacement within seconds, and the operator
/// would be left fighting a loop they cannot see. Turning the switch back on is
/// how they resume it.
async fn stand_down_squadron(
    State(s): State<ApiState>,
    Path(id): Path<String>,
) -> Response {
    info!("📥 POST /api/squadrons/{}/stand-down", id);

    let Some(mut summary) = s.cag.get_squadron(&id) else {
        warn!("stand-down: squadron '{}' not found", id);
        return (StatusCode::NOT_FOUND, format!("squadron '{}' not found", id)).into_response();
    };
    // A registry copy carries an EMPTY market_class — it is resolved from the
    // taxonomy at request time, which is why every other handler that reads the
    // field calls this first. Without it the match below fell through to `None`,
    // no auto-deploy switch was turned off, and the seeder replaced the squadron
    // roughly ten seconds after the operator stood it down.
    enrich_taxonomy(&mut summary).await;

    // Turn the switch off BEFORE cancelling, so the seeder cannot win the race
    // between the squadron leaving the registry and the config being written.
    let class = summary.market_class.to_lowercase();
    let mut disabled = None;
    let patch = match class.as_str() {
        "politics" => Some(("auto_deploy_politics", serde_json::json!(false))),
        "sports"   => Some(("auto_deploy_sports",   serde_json::json!(false))),
        _ => None,
    };
    if let Some((field, value)) = patch {
        let current = s.config_rx.borrow().clone();
        let still_on = match field {
            "auto_deploy_politics" => current.auto_deploy_politics,
            _ => current.auto_deploy_sports,
        };
        if still_on {
            let body = serde_json::json!({ field: value }).to_string();
            match DynamicConfig::apply_patch_as(&current, &body, "operator (stand-down)").await {
                Ok(updated) => {
                    let _ = s.config_tx.send(updated);
                    disabled = Some(field.to_string());
                    info!("🛬 stand-down: {field} switched off so the squadron is not re-seeded");
                }
                Err(e) => warn!("🛬 stand-down: could not switch off {field}: {e}"),
            }
        }
    }

    if !s.cag.stand_down(&id) {
        return (StatusCode::NOT_FOUND, format!("squadron '{}' not found", id)).into_response();
    }
    s.cag.update_state(&id, crate::squadron::SquadronState::StoodDown);

    let message = match &disabled {
        Some(f) => format!("Squadron {id} standing down. {f} switched off so it is not redeployed automatically."),
        None => format!("Squadron {id} standing down."),
    };
    Json(StandDownResponse {
        success: true,
        squadron_id: id,
        auto_deploy_disabled: disabled,
        message,
    }).into_response()
}

/// Populate a squadron summary's market taxonomy (`market_class` + the
/// `raptors`/`vipers` meaningful for it) from the DB at request time.
///
/// The class is resolved once at registration by `Squadron::classify_and_link`
/// and persisted on the `squadron_configs` row; here we read it back and expand
/// it through the join tables so the UI can render data-driven cards instead of
/// a hardcoded set.
///
/// In read-only demo mode (or when the DB row doesn't exist), infers the class
/// from the asset symbol instead of relying on the missing squadron_configs row.
async fn enrich_taxonomy(summary: &mut crate::cag::SquadronSummary) {
    let Some(pool) = db::pool() else { return };

    // Try to read the persisted market_class first.
    let class = match db::get_squadron_market_class(pool, &summary.id).await {
        Some(c) if !c.is_empty() && c != "unknown" => c,
        _ => {
            // No DB row (read-only mode) or empty/unknown class — infer from asset.
            // BTC/ETH/SOL are always crypto; custom assets fall back to classification.
            let asset_lower = summary.asset.to_ascii_lowercase();
            if matches!(asset_lower.as_str(), "btc" | "eth" | "sol") {
                "crypto".to_string()
            } else {
                // For custom assets, attempt classification from the market name.
                let symbols: [&str; 0] = [];
                db::classify_market(pool, "", &symbols, &summary.market_name).await
            }
        }
    };

    summary.raptors = db::raptors_for_class(pool, &class).await;
    summary.vipers = db::vipers_for_class(pool, &class).await;
    summary.market_class = class;
}

/// GET /api/config/schema
///
/// Returns the editable-config field schema — the single source of truth describing
/// every `DynamicConfig` field (group, label, type, unit, min/max, advanced flag).
/// The Control Tower renders Basic panels + the Advanced modal from this, so new
/// Rust config fields surface automatically without a hand-maintained frontend list.
async fn get_config_schema() -> Response {
    debug!("Received GET /api/config/schema");
    Json(crate::api::config_schema::config_schema()).into_response()
}

/// GET /api/squadrons/{id}/config
///
/// Returns the squadron's DynamicConfig as JSON.
/// In read-only demo mode (or if no DB row exists yet), returns compile-time defaults.
async fn get_squadron_config(
    Path(id): Path<String>,
) -> Response {
    debug!("Received GET /api/squadrons/{}/config", id);

    // In read-only demo mode, squadron configs are never persisted to DB, so
    // we return compile-time defaults directly rather than 404.
    if crate::helpers::dynamic_config::read_only_mode() {
        debug!("READ-ONLY mode: returning compile-time defaults for squadron {}", id);
        match serde_json::to_value(&DynamicConfig::default()) {
            Ok(val) => return (StatusCode::OK, Json(val)).into_response(),
            Err(e) => {
                error!("Error serializing default config for {}: {}", id, e);
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        }
    }

    // Retry-aware: this endpoint is polled by the dashboard immediately at boot,
    // before the primary pool may have initialized (roadmap bug #7).
    match db::pool_for_opt_retry(None).await {
        Some(pool) => {
            if db::squadron_config_get(&pool, &id).await.is_some() {
                // Served through `load_for_squadron`, not raw from the row.
                //
                // That function applies the build caps and reconciles the
                // instance-level fields, so the raw row can differ from what the
                // engine actually uses — a `time_decay_max_entry_price` above its
                // build cap, or a stale `ghost_mode`. Returning the raw row made
                // the squadron page display values already overridden underneath
                // it, which is the exact "surface shows state it does not govern"
                // pattern behind today's incidents, on the page whose entire job
                // is showing operative config.
                let cfg = DynamicConfig::load_for_squadron(&id).await;
                match Ok::<_, serde_json::Error>(cfg) {
                    Ok(cfg) => match serde_json::to_value(cfg.as_ref()) {
                        Ok(val) => {
                            debug!("Successfully retrieved squadron config for {}", id);
                            (StatusCode::OK, Json(val)).into_response()
                        },
                        Err(e) => {
                            error!("Error serializing squadron config for {}: {}", id, e);
                            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                        },
                    },
                    Err(e) => {
                        error!("Error parsing squadron config for {}: {}", id, e);
                        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                    },
                }
            } else {
                warn!("GET /api/squadrons/{}/config: not found", id);
                (StatusCode::NOT_FOUND, format!("squadron '{}' config not found", id)).into_response()
            }
        },
        None => {
            log_pool_unavailable(&format!("GET /api/squadrons/{id}/config"), None);
            (StatusCode::INTERNAL_SERVER_ERROR, "database unavailable").into_response()
        },
    }
}

/// PATCH /api/squadrons/{id}/config
///
/// Body: a partial JSON object with only the fields to change, e.g.
///   `{"time_decay_position_size_usdc": "8.0"}`
///
/// Applies squadron-specific config changes.
async fn patch_squadron_config(
    Path(id): Path<String>,
    body: String,
) -> Response {
    info!("📥 Received PATCH /api/squadrons/{}/config with body: {}", id, body);
    match DynamicConfig::apply_squadron_patch(&id, &body).await {
        Ok(new_cfg) => {
            match serde_json::to_value(new_cfg.as_ref()) {
                Ok(val) => {
                    debug!("Successfully patched squadron config for {}", id);
                    (StatusCode::OK, Json(val)).into_response()
                },
                Err(e) => {
                    error!("Error serializing new squadron config for {}: {}", id, e);
                    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                },
            }
        },
        Err(e) => {
            error!("Error applying patch for squadron {}: {}", id, e);
            (StatusCode::BAD_REQUEST, e.to_string()).into_response()
        },
    }
}

#[cfg(test)]
mod patch_scope_tests {
    use super::*;

    /// The default is the whole point. If `PatchScope` ever defaults back to
    /// `GlobalOnly`, the GHOST/LIVE button silently stops reaching the running
    /// squadrons again and every surface goes on claiming it worked — which is
    /// exactly what happened on the production Marketplace instance on
    /// 2026-08-29, for a day, with the operator believing they were live.
    #[test]
    fn a_patch_reaches_deployed_squadrons_by_default() {
        assert_eq!(PatchScope::default(), PatchScope::GlobalAndDeployed);
    }

    /// An absent `scope` must mean the default, not a deserialization failure.
    #[test]
    fn an_omitted_scope_defaults_to_reaching_deployed_squadrons() {
        let q: PatchConfigQuery = serde_json::from_str("{}").expect("empty query deserializes");
        assert_eq!(q.scope, PatchScope::GlobalAndDeployed);
    }

    /// The escape hatch still exists for a caller that means "seed only".
    #[test]
    fn global_only_can_be_requested_explicitly() {
        let q: PatchConfigQuery =
            serde_json::from_str(r#"{"scope":"global_only"}"#).expect("deserializes");
        assert_eq!(q.scope, PatchScope::GlobalOnly);
    }

    /// The wire spelling the Control Tower would send, kept in step with the
    /// profile endpoint's `ProfileScope` so the two cannot drift apart.
    #[test]
    fn scope_spellings_match_the_profile_endpoint() {
        let q: PatchConfigQuery =
            serde_json::from_str(r#"{"scope":"global_and_deployed"}"#).expect("deserializes");
        assert_eq!(q.scope, PatchScope::GlobalAndDeployed);
    }
}

// ─── Deployment Region & Taxonomy Endpoints ──────────────────────────────────

/// Response for GET /api/deployment/region.
#[derive(Serialize)]
struct DeploymentRegionResponse {
    region: String,
    available_types: Vec<String>,
}

/// GET /api/deployment/region
///
/// Returns the deployment region and available market types based on feature flags.
/// US deployment: politics, sports only (crypto wing is auto-managed)
/// Kalshi deployment: crypto only
/// INTL deployment (intl_clob feature): politics, sports, crypto
async fn get_deployment_region() -> Response {
    debug!("Received GET /api/deployment/region");
    
    #[cfg(feature = "intl_clob")]
    let (region, types) = ("intl", vec!["politics", "sports", "crypto"]);

    // Kalshi carries far more politics and sports than crypto — 4,626 and 815
    // open markets respectively against a handful of crypto series. Listing only
    // crypto hid the venue's entire brand from its own customers, and DRADIS has
    // supported two vipers on those classes all along.
    #[cfg(all(not(feature = "intl_clob"), feature = "kalshi"))]
    let (region, types) = ("kalshi", vec!["politics", "sports", "crypto"]);

    // Crypto belongs here as much as on the other venues: the crypto wing is
    // live and trading (`us-crypto-open`), and "the wing manages it" is no more
    // a reason to hide the class than Kalshi's rotation loop is for its crypto.
    // The list predates that wing existing.
    #[cfg(all(not(feature = "intl_clob"), not(feature = "kalshi")))]
    let (region, types) = ("us", vec!["politics", "sports", "crypto"]);
    
    Json(DeploymentRegionResponse {
        region: region.to_string(),
        available_types: types.into_iter().map(String::from).collect(),
    }).into_response()
}

/// Raptor kind info for taxonomy endpoints.
#[derive(Serialize)]
struct RaptorKindResponse {
    id: String,
    display: String,
    implemented: bool,
}

/// Viper kind info for taxonomy endpoints.
#[derive(Serialize)]
struct ViperKindResponse {
    id: String,
    display: String,
    venue_agnostic: bool,
}

/// Query params for taxonomy endpoints.
#[derive(Deserialize)]
struct TaxonomyQuery {
    market_class: String,
}

/// GET /api/taxonomy/raptors?market_class=crypto
///
/// Returns the raptor kinds available for a given market class.
async fn get_taxonomy_raptors(Query(q): Query<TaxonomyQuery>) -> Response {
    debug!("Received GET /api/taxonomy/raptors for class {}", q.market_class);
    
    let Some(pool) = db::pool() else {
        return Json(Vec::<RaptorKindResponse>::new()).into_response();
    };
    
    let raptors = db::raptors_for_class_full(pool, &q.market_class).await;
    Json(raptors.into_iter().map(|(id, display, implemented)| RaptorKindResponse {
        id,
        display,
        implemented,
    }).collect::<Vec<_>>()).into_response()
}

/// GET /api/taxonomy/vipers?market_class=crypto
///
/// Returns the viper kinds available for a given market class.
async fn get_taxonomy_vipers(Query(q): Query<TaxonomyQuery>) -> Response {
    debug!("Received GET /api/taxonomy/vipers for class {}", q.market_class);
    
    let Some(pool) = db::pool() else {
        return Json(Vec::<ViperKindResponse>::new()).into_response();
    };
    
    let vipers = db::vipers_for_class_full(pool, &q.market_class).await;
    Json(vipers.into_iter().map(|(id, display, venue_agnostic)| ViperKindResponse {
        id,
        display,
        venue_agnostic,
    }).collect::<Vec<_>>()).into_response()
}

// ─── Available Markets Endpoint ──────────────────────────────────────────────

/// Query params for GET /api/markets/available.
#[derive(Deserialize)]
struct AvailableMarketsQuery {
    market_type: String,           // "crypto" | "sports" | "politics"
    expiry_window: Option<String>, // "1h" | "4h" | "24h" | "7d"
    min_liquidity: Option<f64>,
}

/// A market available for squadron deployment.
#[derive(Serialize)]
pub(crate) struct AvailableMarket {
    pub(crate) condition_id: String,
    pub(crate) question: String,
    pub(crate) market_class: String,
    pub(crate) end_date: Option<String>,
    pub(crate) liquidity: f64,
    pub(crate) tokens: AvailableMarketTokens,
}

#[derive(Serialize)]
pub(crate) struct AvailableMarketTokens {
    pub(crate) yes_id: String,
    pub(crate) no_id: String,
}

#[derive(Serialize)]
struct AvailableMarketsResponse {
    markets: Vec<AvailableMarket>,
}

/// Does this build run a deployment-queue consumer?
///
/// Every venue now drains the queue with the one consumer,
/// `venues::deployment::run_deployment_processor`, through its own
/// `DeploymentRunner`: `KalshiDeploymentRunner`, `UsDeploymentRunner` and
/// `cag::adama::IntlDeploymentRunner`. Intl had a second consumer of its own
/// (`run_adama_processor`) until it was folded in — while it existed, intl
/// silently lacked restart requeue, auto-deploy seeding and any cancellation
/// path, because those were fixed once in the shared consumer and never
/// back-ported. Kept rather than deleted because the coupling is
/// worth being able to grep from both ends: if a future venue ships without a
/// consumer, this is where it says so, and the deploy endpoint refuses cleanly
/// instead of writing a row nothing will ever collect.
const DEPLOY_QUEUE_HAS_CONSUMER: bool = true;


/// A lazily-connected venue handle for market discovery.
///
/// Caches a SUCCESSFUL connection for the process lifetime and a FAILED one for
/// [`Self::RETRY_AFTER`]. Both halves matter and pull in opposite directions:
///
/// * Caching the failure permanently — which `OnceCell<Option<_>>` does — let a
///   single transient error disable market discovery for the whole process. The
///   operator saw an empty market list from then on, with the cause a lone
///   warning that had scrolled away.
/// * Not caching it at all makes every browse re-attempt the connect. Against a
///   venue that rate-limits (Cloudflare 1015, which several restarts in quick
///   succession will trigger), that keeps the limiter tripped and turns a brief
///   outage into a standing one.
///
/// So: retry, but not on every keystroke.
// The intl CLOB discovers through the Gamma API rather than a venue handle, so
// this is unused on that build.
#[cfg_attr(feature = "intl_clob", allow(dead_code))]
struct VenueSlot<V> {
    connected: Option<std::sync::Arc<V>>,
    last_failure: Option<std::time::Instant>,
}

#[cfg_attr(feature = "intl_clob", allow(dead_code))]
impl<V> VenueSlot<V> {
    /// How long to wait before re-attempting after a failed connect.
    const RETRY_AFTER: std::time::Duration = std::time::Duration::from_secs(30);

    const fn new() -> Self {
        Self { connected: None, last_failure: None }
    }

    /// The cached handle, connecting if there is none and the cooldown has passed.
    async fn get_or_connect<F, Fut, E>(&mut self, connect: F) -> Option<std::sync::Arc<V>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<V, E>>,
        E: std::fmt::Display,
    {
        if let Some(v) = self.connected.as_ref() {
            return Some(std::sync::Arc::clone(v));
        }
        if let Some(at) = self.last_failure {
            if at.elapsed() < Self::RETRY_AFTER {
                debug!("venue connect on cooldown, {}s remaining",
                       (Self::RETRY_AFTER - at.elapsed()).as_secs());
                return None;
            }
        }
        match connect().await {
            Ok(v) => {
                let v = std::sync::Arc::new(v);
                self.connected = Some(std::sync::Arc::clone(&v));
                self.last_failure = None;
                Some(v)
            }
            Err(e) => {
                warn!("Venue connect failed for market discovery: {e}");
                self.last_failure = Some(std::time::Instant::now());
                None
            }
        }
    }
}

/// Default max-time-to-close for a market class, in seconds.
///
/// The sensible value depends on the VENUE as much as the class, because the
/// venues structure the same category completely differently.
///
/// Polymarket sports are game-day markets, so 24h is right there. Kalshi sports
/// are season futures — measured 2026-08-22, its 5,715 open sports markets had a
/// median 6,896 hours to close and NOT ONE closed within seven days ("Will
/// Philadelphia win the 2027 Pro Basketball Final" closes in 25,748h). Applying
/// the Polymarket default to Kalshi filtered the entire category to nothing,
/// which reads as "no markets available" rather than as a filter doing its job.
///
/// Shared by the browse list and Quick deploy: when Quick had its own hardcoded
/// 7-day window, it found nothing for classes the browse list happily showed.
fn default_expiry_secs(market_type: &str) -> i64 {
    #[cfg(all(not(feature = "intl_clob"), feature = "kalshi"))]
    match market_type.to_lowercase().as_str() {
        "sports" => 63_072_000,   // 2y — championship futures
        "politics" => 63_072_000, // 2y — election cycles run long
        "crypto" => 2_592_000,    // 30d
        _ => 604_800,
    }
    // Polymarket US structures crypto as year-scale price targets ("Will Bitcoin
    // be above $200,000 in 2026?"), not the 15-minute and daily strikes the
    // other venues list. A 30-day horizon excluded every crypto market it has —
    // including the one its own crypto wing was trading — so browsing crypto
    // returned nothing on a venue that demonstrably had crypto markets.
    #[cfg(all(not(feature = "intl_clob"), not(feature = "kalshi")))]
    match market_type.to_lowercase().as_str() {
        "sports" => 86_400,      // 24h — game-day markets
        "crypto" => 63_072_000,  // 2y — year-scale price targets
        "politics" => 31_536_000,// 1y — election cycles
        _ => 604_800,            // 7d fallback
    }
    #[cfg(feature = "intl_clob")]
    match market_type.to_lowercase().as_str() {
        "sports" => 86_400,     // 24h — game-day markets
        "crypto" => 2_592_000,  // 30d — price targets have longer horizons
        "politics" => 7_776_000,// 90d — longer horizons
        _ => 604_800,           // 7d fallback
    }
}

#[cfg(test)]
mod deploy_horizon_tests {
    use super::default_expiry_secs;

    /// Quick deploy bounds its choice by min(discovery window, horizon). The two
    /// answer different questions: discovery reaches years out because Kalshi
    /// structures politics and sports as multi-year futures, while selection has
    /// to hand the vipers something they can actually trade.
    fn selectable_secs(class: &str, horizon_days: i64) -> i64 {
        default_expiry_secs(class).min(horizon_days * 86_400)
    }

    /// The concrete case from QA: Quick deploy chose KXCITRINI-28JUL01, closing
    /// in July 2028, for a squadron whose only vipers are Arbitrage and Maker.
    #[test]
    fn a_multi_year_market_is_out_of_reach_at_the_default_horizon() {
        let two_years = 730 * 86_400;
        for class in ["politics", "sports"] {
            let limit = selectable_secs(class, crate::config::DEPLOY_MAX_DAYS_TO_CLOSE as i64);
            assert!(
                limit < two_years,
                "{class}: a 2028 market is still selectable ({limit}s)",
            );
        }
    }

    /// The horizon must bound selection, never widen it beyond what discovery
    /// would return — otherwise raising the knob silently changes the class
    /// filter as well.
    #[test]
    fn the_horizon_can_only_narrow_the_window() {
        for class in ["politics", "sports", "crypto", "weather"] {
            for days in [1_i64, 30, 90, 100_000] {
                assert!(
                    selectable_secs(class, days) <= default_expiry_secs(class),
                    "{class} at {days}d exceeded the discovery window",
                );
            }
        }
    }

    /// A tight horizon must still leave something selectable for crypto, which
    /// is where the venue's own rotation operates on 15-minute and daily markets.
    #[test]
    fn a_tight_horizon_still_admits_short_dated_crypto() {
        assert!(selectable_secs("crypto", 1) >= 86_400, "a one-day horizon excluded daily crypto");
    }
}

#[cfg(test)]
mod discovery_window_tests {
    use super::default_expiry_secs;

    /// Quick deploy used a hardcoded 7-day window while the browse list used
    /// this function. On Kalshi that combination reported "No politics markets
    /// available for deployment" for a category the browse list could list,
    /// because no Kalshi politics or sports market closes within a week.
    #[test]
    fn every_class_admits_more_than_a_week_where_the_venue_needs_it() {
        const WEEK: i64 = 7 * 24 * 3600;
        for class in ["politics", "sports", "crypto"] {
            let secs = default_expiry_secs(class);
            assert!(secs > 0, "{class} has no window");
            #[cfg(all(not(feature = "intl_clob"), feature = "kalshi"))]
            assert!(
                secs > WEEK,
                "{class}: Kalshi structures this as a long-dated future; {secs}s hides the whole category",
            );
            #[cfg(not(all(not(feature = "intl_clob"), feature = "kalshi")))]
            let _ = WEEK;
        }
    }

    /// Case must not decide whether a category is visible.
    #[test]
    fn the_window_is_case_insensitive() {
        for class in ["politics", "sports", "crypto"] {
            assert_eq!(
                default_expiry_secs(class),
                default_expiry_secs(&class.to_uppercase()),
                "{class} changed window with case",
            );
        }
    }

    /// An unknown class must still be usable rather than resolving to zero.
    #[test]
    fn an_unknown_class_falls_back_to_a_week() {
        assert_eq!(default_expiry_secs("weather"), 604_800);
    }
}

/// Liquidity floor for market discovery. Quick deploy sorts by liquidity and
/// takes the best, so it uses the same floor as the browse list rather than a
/// stricter one — otherwise Quick refuses to deploy a market the operator can
/// see listed, with an error that says none exist.
const DISCOVERY_MIN_LIQUIDITY: f64 = 500.0;

/// GET /api/markets/available?market_type=crypto&expiry_window=4h&min_liquidity=1000
///
/// Returns available markets for squadron deployment, filtered by type.
/// Uses the Gamma API (INTL) or retail venue (US) depending on build features.
async fn get_available_markets(Query(q): Query<AvailableMarketsQuery>) -> Response {
    debug!("Received GET /api/markets/available for type {}", q.market_type);
    
    let market_type = q.market_type.to_lowercase();
    let min_liquidity = q.min_liquidity.unwrap_or(DISCOVERY_MIN_LIQUIDITY);
    
    // Parse expiry window to seconds. The sensible default depends on the VENUE
    // as much as the class, because the venues structure the same category
    // completely differently.
    //
    // Polymarket sports are game-day markets, so 24h is right there. Kalshi
    // sports are season futures — measured 2026-08-22, its 5,715 open sports
    // markets had a median 6,896 hours to close and NOT ONE closed within seven
    // days ("Will Philadelphia win the 2027 Pro Basketball Final" closes in
    // 25,748h). Applying the Polymarket default to Kalshi filtered the entire
    // category to nothing, which read as "no markets available" rather than as
    // a filter doing its job.
    let default_expiry = default_expiry_secs(&q.market_type);
    
    let max_expiry_secs: i64 = match q.expiry_window.as_deref() {
        Some("1h") => 3600,
        Some("4h") => 14400,
        Some("24h") => 86400,
        Some("7d") => 604800,
        Some("30d") => 2592000,
        Some("90d") => 7776000,
        // Kalshi season futures and election cycles sit years out; without this
        // the longest selectable window still hides them.
        Some("1y") => 31_536_000,
        Some("2y") => 63_072_000,
        _ => default_expiry,
    };
    
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap_or_default();
    
    let markets = fetch_markets_by_type(&http, &market_type, max_expiry_secs, min_liquidity).await;
    
    Json(AvailableMarketsResponse { markets }).into_response()
}

/// Fetch markets from Gamma API filtered by market type.
/// Uses existing helpers: fetch_simplified_crypto_candidates for crypto,
/// tag-based filtering for sports, regex for politics.
#[cfg(feature = "intl_clob")]
pub(crate) async fn fetch_markets_by_type(
    http: &reqwest::Client,
    market_type: &str,
    _max_expiry_secs: i64,
    min_liquidity: f64,
) -> Vec<AvailableMarket> {
    // For sports, use tag-based filtering from /sports endpoint
    if market_type == "sports" {
        return fetch_sports_markets_by_tags(http, _max_expiry_secs, min_liquidity).await;
    }
    
    // For crypto, use the existing market.rs helper that already handles
    // slug-based filtering, window markets, daily markets, etc.
    if market_type == "crypto" {
        let candidates = crate::helpers::market::fetch_simplified_crypto_candidates(http, "all").await;
        let mut out: Vec<AvailableMarket> = candidates
            .into_iter()
            .filter(|(_, _, _, vol, _, _, _, _)| *vol >= min_liquidity)
            .map(|(tokens, question, _slug, liquidity, _priority, end_date, _desc, condition_id)| {
                AvailableMarket {
                    condition_id,
                    question,
                    market_class: "crypto".to_string(),
                    end_date: end_date.map(|dt| dt.to_rfc3339()),
                    liquidity,
                    tokens: AvailableMarketTokens {
                        yes_id: crate::venues::intl::market_id_from_u256(tokens[0]).to_string(),
                        no_id: crate::venues::intl::market_id_from_u256(tokens[1]).to_string(),
                    },
                }
            })
            .collect();
        
        out.sort_by(|a, b| b.liquidity.partial_cmp(&a.liquidity).unwrap_or(std::cmp::Ordering::Equal));
        out.truncate(50);
        info!("📊 fetch_markets_by_type: found {} crypto markets", out.len());
        return out;
    }
    
    // For politics, use regex filtering (no clean umbrella tags exist)
    let mut out = Vec::new();
    let now = chrono::Utc::now();
    
    let filter_patterns: Vec<regex::Regex> = vec![
        regex::Regex::new(r"(?i)\belection\b").ok(),
        regex::Regex::new(r"(?i)\bpresident\b").ok(),
        regex::Regex::new(r"(?i)\bsenate\b").ok(),
        regex::Regex::new(r"(?i)\bcongress\b").ok(),
        regex::Regex::new(r"(?i)\bvote\b").ok(),
        regex::Regex::new(r"(?i)\bprime minister\b").ok(),
        regex::Regex::new(r"(?i)\bgovernment\b").ok(),
        regex::Regex::new(r"(?i)\btrump\b").ok(),
        regex::Regex::new(r"(?i)\bbiden\b").ok(),
    ].into_iter().flatten().collect();
    
    // Fetch top markets by volume, then filter locally
    let url = "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=200&order=volume24hrClob&ascending=false";
    
    let resp = match http.get(url).send().await {
        Ok(r) => r,
        Err(e) => {
            warn!("Market fetch failed: {}", e);
            return out;
        }
    };
    
    let data: serde_json::Value = match resp.json().await {
        Ok(d) => d,
        Err(_) => return out,
    };
    
    let markets_arr = data.as_array()
        .or_else(|| data.get("data").and_then(|v| v.as_array()));
    
    if let Some(arr) = markets_arr {
        for m in arr {
            let question = m.get("question")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            
            // Filter by market type using regex patterns
            let matches_type = filter_patterns.iter().any(|re| re.is_match(&question));
            if !matches_type {
                continue;
            }
            
            // Skip if already seen (dedup by condition_id)
            let condition_id = m.get("conditionId")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            if condition_id.is_empty() || out.iter().any(|e: &AvailableMarket| e.condition_id == condition_id) {
                continue;
            }
            
            // Check liquidity
            let volume = m.get("volume24hrClob")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            if volume < min_liquidity {
                continue;
            }
            
            // Check expiry
            let end_date_str = m.get("endDate")
                .or_else(|| m.get("event").and_then(|e| e.get("endDate")))
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            
            let close_time = chrono::DateTime::parse_from_rfc3339(end_date_str)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc));
            
            if let Some(ct) = close_time {
                let secs_left = (ct - now).num_seconds();
                if secs_left < 300 || secs_left > _max_expiry_secs {
                    continue; // Too close to expiry or too far out
                }
            }
            
            // Extract token IDs
            let tokens = crate::helpers::json::extract_token_ids_u256(m);
            if tokens.len() < 2 {
                continue;
            }
            
            out.push(AvailableMarket {
                condition_id,
                question,
                market_class: "politics".to_string(),
                end_date: close_time.map(|ct| ct.to_rfc3339()),
                liquidity: volume,
                tokens: AvailableMarketTokens {
                    yes_id: crate::venues::intl::market_id_from_u256(tokens[0]).to_string(),
                    no_id: crate::venues::intl::market_id_from_u256(tokens[1]).to_string(),
                },
            });
        }
    }
    
    info!("📊 fetch_markets_by_type: found {} politics markets", out.len());
    
    // Sort by liquidity descending
    out.sort_by(|a, b| b.liquidity.partial_cmp(&a.liquidity).unwrap_or(std::cmp::Ordering::Equal));
    
    // Limit to top 50
    out.truncate(50);
    out
}

/// Fetch sports markets using tag IDs from the /sports endpoint.
/// This is the official Polymarket approach per their API docs.
#[cfg(feature = "intl_clob")]
async fn fetch_sports_markets_by_tags(
    http: &reqwest::Client,
    max_expiry_secs: i64,
    min_liquidity: f64,
) -> Vec<AvailableMarket> {
    let mut out = Vec::new();
    let now = chrono::Utc::now();
    
    // Step 1: Fetch all sports and collect their tag IDs
    let sports_url = "https://gamma-api.polymarket.com/sports";
    let sports_resp = match http.get(sports_url).send().await {
        Ok(r) => r,
        Err(e) => {
            warn!("Sports endpoint fetch failed: {}", e);
            return out;
        }
    };
    
    let sports_data: serde_json::Value = match sports_resp.json().await {
        Ok(d) => d,
        Err(_) => return out,
    };
    
    // Collect unique tag IDs from all sports
    let mut tag_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    if let Some(arr) = sports_data.as_array() {
        for sport in arr {
            if let Some(tags_str) = sport.get("tags").and_then(|v| v.as_str()) {
                for tag in tags_str.split(',') {
                    let tag = tag.trim();
                    if !tag.is_empty() {
                        tag_ids.insert(tag.to_string());
                    }
                }
            }
        }
    }
    
    // Step 2: Fetch markets for each tag (parallelize with first few high-volume tags)
    // Use tag_id=1 which appears in all sports as an umbrella
    let primary_tags: Vec<&str> = vec!["1", "450", "100381"]; // Sports umbrella, NFL, MLB
    
    for tag_id in primary_tags.iter().take(3) {
        let url = format!(
            "https://gamma-api.polymarket.com/markets?tag_id={}&active=true&closed=false&limit=100&order=volume24hrClob&ascending=false",
            tag_id
        );
        
        let resp = match http.get(&url).send().await {
            Ok(r) => r,
            Err(_) => continue,
        };
        
        let data: serde_json::Value = match resp.json().await {
            Ok(d) => d,
            Err(_) => continue,
        };
        
        let markets_arr = data.as_array()
            .or_else(|| data.get("data").and_then(|v| v.as_array()));
        
        if let Some(arr) = markets_arr {
            for m in arr {
                let question = m.get("question")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                
                let condition_id = m.get("conditionId")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                
                // Skip duplicates
                if condition_id.is_empty() || out.iter().any(|e: &AvailableMarket| e.condition_id == condition_id) {
                    continue;
                }
                
                // Check liquidity
                let volume = m.get("volume24hrClob")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                if volume < min_liquidity {
                    continue;
                }
                
                // Check expiry
                let end_date_str = m.get("endDate")
                    .or_else(|| m.get("event").and_then(|e| e.get("endDate")))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                
                let close_time = chrono::DateTime::parse_from_rfc3339(end_date_str)
                    .ok()
                    .map(|dt| dt.with_timezone(&chrono::Utc));
                
                if let Some(ct) = close_time {
                    let secs_left = (ct - now).num_seconds();
                    if secs_left < 300 || secs_left > max_expiry_secs {
                        continue;
                    }
                }
                
                // Extract token IDs
                let tokens = crate::helpers::json::extract_token_ids_u256(m);
                if tokens.len() < 2 {
                    continue;
                }
                
                out.push(AvailableMarket {
                    condition_id,
                    question,
                    market_class: "sports".to_string(),
                    end_date: close_time.map(|ct| ct.to_rfc3339()),
                    liquidity: volume,
                    tokens: AvailableMarketTokens {
                        yes_id: crate::venues::intl::market_id_from_u256(tokens[0]).to_string(),
                        no_id: crate::venues::intl::market_id_from_u256(tokens[1]).to_string(),
                    },
                });
            }
        }
    }
    
    // Sort by liquidity and limit
    out.sort_by(|a, b| b.liquidity.partial_cmp(&a.liquidity).unwrap_or(std::cmp::Ordering::Equal));
    out.truncate(50);
    out
}

/// Fetch markets for the US retail venue via `UsRetailVenue::discover_binary_markets`.
///
/// The venue handle is connected on first use and cached, but only on SUCCESS —
/// a failed connect is retried on the next request rather than remembered.
///
/// `OnceCell<Option<_>>` cached the failure too, so one transient error disabled
/// market discovery for the rest of the process: the operator saw an empty
/// market list from then on, with the cause a single warning that had scrolled
/// away minutes earlier. Restarts are the common trigger — several in quick
/// succession trip the venue's rate limiter (Cloudflare 1015) on the auth probe,
/// and by the time the limit clears the empty cache is what the browser sees.
///
/// Markets missing a close time degrade to "always open" (venue convention) and
/// pass the expiry filter.
#[cfg(all(not(feature = "intl_clob"), feature = "us_retail"))]
pub(crate) async fn fetch_markets_by_type(
    http: &reqwest::Client,
    market_type: &str,
    max_expiry_secs: i64,
    min_liquidity: f64,
) -> Vec<AvailableMarket> {
    use crate::venues::us::UsRetailVenue;
    static US_VENUE: tokio::sync::Mutex<VenueSlot<UsRetailVenue>> =
        tokio::sync::Mutex::const_new(VenueSlot::new());

    let venue = {
        let mut slot = US_VENUE.lock().await;
        match slot.get_or_connect(|| {
            let http = std::sync::Arc::new(http.clone());
            async move { UsRetailVenue::connect(http).await }
        }).await {
            Some(v) => v,
            None => return Vec::new(),
        }
    };

    // Every class discovers differently on this venue — `/v1/markets` is
    // sports-dominated, so politics and crypto go through `/v1/search` instead.
    // Browsing used `/v1/markets` for all three, which is why politics and
    // crypto came back empty while both of those wings were trading. Routing
    // through the same helper the wings use means the browser can only show
    // markets the deploy runner will also find.
    let pairs = match crate::venues::us::trader::discover_for_class(&venue, market_type).await {
        Ok(p) => p,
        Err(e) => {
            warn!("US venue {market_type} discovery failed: {e:#}");
            return Vec::new();
        }
    };

    let now = chrono::Utc::now();
    let mut out: Vec<AvailableMarket> = Vec::new();
    for p in pairs {
        // Class membership comes from the venue's own wing definition, so a
        // market listed here is one the deploy runner will actually find. The
        // browser used to apply no class filter and stamp every market with the
        // requested class, which listed football under politics and produced
        // deploys that failed at run time.
        if !crate::venues::us::trader::pair_matches_class(&p, market_type).await {
            continue;
        }
        // The Polymarket US gateway returns no volume field, so every pair
        // reports 0 and a liquidity floor removes the entire venue. Treat 0 as
        // "not reported" rather than "no interest": filtering on a number the
        // venue never sends is how this list came back empty for all three
        // classes while three wings were trading happily.
        if p.volume > 0.0 && p.volume < min_liquidity {
            continue;
        }
        if let Some(ct) = p.close_time {
            let secs_left = (ct - now).num_seconds();
            if secs_left < 300 || secs_left > max_expiry_secs {
                continue;
            }
        }
        out.push(AvailableMarket {
            condition_id: p.slug,
            question: p.question,
            market_class: market_type.to_string(),
            end_date: p.close_time.map(|ct| ct.to_rfc3339()),
            liquidity: p.volume,
            tokens: AvailableMarketTokens {
                yes_id: p.long.to_string(),
                no_id: p.short.to_string(),
            },
        });
    }

    // Soonest close first. Ranking by volume is meaningless here — every market
    // reports 0 — so it produced an arbitrary order that merely looked ranked.
    out.sort_by_key(|m| m.end_date.clone().unwrap_or_else(|| "9999".to_string()));
    out.truncate(50);
    info!("📊 fetch_markets_by_type: found {} US markets for type '{}'", out.len(), market_type);
    out
}

/// Fetch markets for the Kalshi venue across its configured crypto series.
///
/// Public endpoints (no credentials needed for discovery). Only crypto-class
/// markets exist in the hunted series, so non-crypto requests return empty.
#[cfg(all(not(feature = "intl_clob"), not(feature = "us_retail"), feature = "kalshi"))]
pub(crate) async fn fetch_markets_by_type(
    _http: &reqwest::Client,
    market_type: &str,
    max_expiry_secs: i64,
    min_liquidity: f64,
) -> Vec<AvailableMarket> {
    use crate::venues::kalshi::KalshiVenue;
    // Cached on success only — a failed init is retried next request rather
    // than remembered. See the note on the US venue handle: caching the failure
    // disabled market discovery for the whole process from one transient error.
    static KALSHI_VENUE: tokio::sync::Mutex<VenueSlot<KalshiVenue>> =
        tokio::sync::Mutex::const_new(VenueSlot::new());
    let venue = {
        let mut slot = KALSHI_VENUE.lock().await;
        match slot.get_or_connect(|| async { KalshiVenue::from_env() }).await {
            Some(v) => v,
            None => return Vec::new(),
        }
    };

    let now = chrono::Utc::now();
    let mut out: Vec<AvailableMarket> = Vec::new();

    // Politics and sports come from Kalshi's own category taxonomy rather than
    // from series tickers: there are thousands of series (2,226 under Politics
    // alone), so `/events?with_nested_markets=true` is the only viable sweep.
    if market_type == "politics" || market_type == "sports" {
        let cats_raw = if market_type == "politics" {
            crate::config::KALSHI_POLITICS_CATEGORIES
        } else {
            crate::config::KALSHI_SPORTS_CATEGORIES
        };
        let cats: Vec<&str> = cats_raw.split(',').map(str::trim).filter(|c| !c.is_empty()).collect();
        match venue.open_markets_for_categories(&cats).await {
            Ok(found) => {
                for (_cat, m) in found {
                    let close = m.close_time_utc();
                    let volume = crate::venues::kalshi::types::fp(&m.volume_fp)
                        .and_then(|d| f64::try_from(d).ok())
                        .unwrap_or(0.0);
                    if volume < min_liquidity { continue; }
                    if let Some(ct) = close {
                        let secs_left = (ct - now).num_seconds();
                        if secs_left < 300 || secs_left > max_expiry_secs { continue; }
                    }
                    let question = if m.yes_sub_title.is_empty() {
                        m.title.clone()
                    } else {
                        format!("{} — {}", m.title, m.yes_sub_title)
                    };
                    out.push(AvailableMarket {
                        condition_id: m.ticker.clone(),
                        question,
                        market_class: market_type.to_string(),
                        end_date: close.map(|ct| ct.to_rfc3339()),
                        liquidity: volume,
                        tokens: AvailableMarketTokens {
                            yes_id: crate::venues::kalshi::leg_id(&m.ticker, true),
                            no_id: crate::venues::kalshi::leg_id(&m.ticker, false),
                        },
                    });
                }
            }
            Err(e) => warn!("Kalshi {market_type} discovery failed: {e:#}"),
        }
        out.sort_by(|a, b| b.liquidity.partial_cmp(&a.liquidity).unwrap_or(std::cmp::Ordering::Equal));
        out.truncate(50);
        info!("📊 fetch_markets_by_type: found {} Kalshi markets for type '{}'", out.len(), market_type);
        return out;
    }

    for series in ["KXBTC15M", "KXBTCD", "KXETH15M", "KXETHD"] {
        let markets = match venue.markets_for_series(series).await {
            Ok(m) => m,
            Err(e) => {
                warn!("Kalshi market discovery failed for {series}: {e:#}");
                continue;
            }
        };
        for m in markets {
            let close = m.close_time_utc();
            let volume = crate::venues::kalshi::types::fp(&m.volume_fp)
                .and_then(|d| f64::try_from(d).ok())
                .unwrap_or(0.0);
            if volume < min_liquidity {
                continue;
            }
            if let Some(ct) = close {
                let secs_left = (ct - now).num_seconds();
                if secs_left < 300 || secs_left > max_expiry_secs {
                    continue;
                }
            }
            let question = if m.yes_sub_title.is_empty() {
                m.title.clone()
            } else {
                format!("{} — {}", m.title, m.yes_sub_title)
            };
            out.push(AvailableMarket {
                condition_id: m.ticker.clone(),
                question,
                market_class: market_type.to_string(),
                end_date: close.map(|ct| ct.to_rfc3339()),
                liquidity: volume,
                tokens: AvailableMarketTokens {
                    yes_id: crate::venues::kalshi::leg_id(&m.ticker, true),
                    no_id: crate::venues::kalshi::leg_id(&m.ticker, false),
                },
            });
        }
    }
    out.sort_by(|a, b| b.liquidity.partial_cmp(&a.liquidity).unwrap_or(std::cmp::Ordering::Equal));
    out.truncate(50);
    info!("📊 fetch_markets_by_type: found {} Kalshi markets for type '{}'", out.len(), market_type);
    out
}

/// No venue features compiled in — market discovery unavailable.
#[cfg(not(any(feature = "intl_clob", feature = "us_retail", feature = "kalshi")))]
pub(crate) async fn fetch_markets_by_type(
    _http: &reqwest::Client,
    market_type: &str,
    _max_expiry_secs: i64,
    _min_liquidity: f64,
) -> Vec<AvailableMarket> {
    warn!("Market discovery unavailable for type '{}' — no venue feature compiled in", market_type);
    Vec::new()
}

// ─── Squadron Deployment Endpoints ───────────────────────────────────────────

/// Request body for POST /api/squadrons/deploy.
#[derive(Debug, Deserialize)]
struct DeploySquadronRequest {
    mode: String,         // "quick" or "manual"
    market_type: String,  // "crypto", "sports", "politics"
    #[serde(default)]
    #[allow(dead_code)]
    auto_config: bool,
    market_id: Option<String>,
    #[serde(default)]
    raptors: Vec<String>,
    #[serde(default)]
    vipers: Vec<String>,
    /// Per-viper capital budgets: viper kind id → max-exposure USDC.
    /// Applied to the squadron's `DynamicConfig` `*_max_exposure_usdc` fields at spawn.
    #[serde(default)]
    viper_budgets: std::collections::HashMap<String, f64>,
    /// Operator-chosen name, used to tell two squadrons of the same class apart
    /// in the Control Tower and to give each its own config and positions.
    ///
    /// Squadron ids are deliberately stable across restarts and market
    /// rotations — they are the persistence key for a squadron's config, and a
    /// generated id would orphan an operator's tuning on every restart. A name
    /// is stable in the same way while still being unique, which a derived
    /// `{asset}-{cadence}` id is not.
    #[serde(default)]
    name: Option<String>,
}

/// Response for POST /api/squadrons/deploy.
#[derive(Serialize)]
struct DeploySquadronResponse {
    success: bool,
    squadron_id: Option<String>,
    error: Option<String>,
}

/// POST /api/squadrons/deploy
///
/// Deploy a new squadron to a market.
/// - Quick mode: DRADIS auto-selects the best market for the given type
/// - Manual mode: User specifies market_id, raptors, and vipers
async fn deploy_squadron(
    State(_s): State<ApiState>,
    Json(req): Json<DeploySquadronRequest>,
) -> Response {
    info!("📥 POST /api/squadrons/deploy: mode={}, type={}", req.mode, req.market_type);

    // Refuse rather than queue a request nothing will ever pick up.
    //
    // A deploy request is written to the deployment_queue table and consumed by
    // run_adama_processor, which is generic over the on-chain wallet Provider
    // and therefore spawned only on the intl CLOB build. On Kalshi and US the
    // row was written, the API answered "queued", and the squadron never
    // appeared — no error anywhere, in the log or the UI. That is the worst
    // possible failure: the operator is told it worked.
    //
    // These venues run their own market selection, so nothing is lost today by
    // saying so plainly. Operator-chosen squadrons here need a venue-side queue
    // consumer, which is real work: the Kalshi loop trades one market at a time
    // and derives its Raptor stack from a crypto underlying that a politics or
    // sports market does not have.
    if !DEPLOY_QUEUE_HAS_CONSUMER {
        warn!(
            "📋 Deployment refused ({} / {}): no queue processor on this venue build",
            req.mode, req.market_type,
        );
        return Json(DeploySquadronResponse {
            success: false,
            squadron_id: None,
            error: Some(
                "This venue selects and manages its own markets automatically — \
                 operator-deployed squadrons are not available on this build yet. \
                 The squadron it is trading appears on the Main view; use the viper \
                 controls on its squadron page to tune it."
                    .to_string(),
            ),
        }).into_response();
    }
    
    // Validate market type against deployment region.
    // Every venue now accepts all three classes: politics and sports are the
    // bulk of Kalshi and Polymarket US, and the two venue-agnostic vipers
    // (Arbitrage, Maker) have always been mapped to those classes. Crypto is
    // deployable everywhere too — a venue's own rotation loop or wing owning a
    // class is not a reason the operator cannot add a squadron to it.
    #[cfg(all(not(feature = "intl_clob"), feature = "kalshi"))]
    if !matches!(req.market_type.as_str(), "crypto" | "politics" | "sports") {
        return Json(DeploySquadronResponse {
            success: false,
            squadron_id: None,
            error: Some(format!(
                "Unknown market type '{}' — Kalshi supports crypto, politics and sports",
                req.market_type
            )),
        }).into_response();
    }

    
    // ── Duplicate-squadron guard ─────────────────────────────────────────────
    // The engine's identity model is one squadron per asset/market-class:
    // session state, SQLite pools, dynamic config, and the viper status
    // registry are all keyed by asset (crypto_filter). A second squadron for
    // the same class would silently interleave state with the first. Reject
    // until squadron identity is first-class (see roadmap: A/B experiments).
    {
        let mut summaries = _s.cag.list_squadrons();
        for sq in &mut summaries {
            // Registry copies carry an empty market_class; resolve it so a
            // "crypto" deploy also matches boot-time BTC/ETH squadrons.
            enrich_taxonomy(sq).await;
        }
        // A name is what tells two squadrons of one class apart, so a second one
        // must carry a distinct one rather than silently colliding.
        if let Some(name) = req.name.as_deref().map(str::trim).filter(|n| !n.is_empty()) {
            if summaries.iter().any(|sq| sq.name.eq_ignore_ascii_case(name) && sq.state != "STOOD_DOWN") {
                return Json(DeploySquadronResponse {
                    success: false,
                    squadron_id: None,
                    error: Some(format!("A squadron named \"{name}\" is already running — choose another name.")),
                }).into_response();
            }
        }
    }

    // For Quick mode, auto-select a market
    let market_id = if req.mode == "quick" {
        // Fetch available markets and pick the best one (highest liquidity)
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        
        // Quick deploy picks a market to trade, so it is bounded by the
        // deployment horizon rather than by the discovery window. Those are
        // different questions: discovery reaches years out because that is how
        // Kalshi structures politics and sports, but Arbitrage locks collateral
        // until resolution and Maker quotes on a session scale, so pointing
        // either at a 2028 market is a poor use of both. The browse list is
        // unaffected — a longer-dated market can still be deployed by hand.
        let horizon_days = _s.config_rx.borrow().deploy_max_days_to_close as i64;
        let max_secs = default_expiry_secs(&req.market_type).min(horizon_days * 86_400);
        let markets = fetch_markets_by_type(
            &http,
            &req.market_type,
            max_secs,
            DISCOVERY_MIN_LIQUIDITY,
        ).await;
        
        if markets.is_empty() {
            return Json(DeploySquadronResponse {
                success: false,
                squadron_id: None,
                error: Some(format!("No {} markets available for deployment", req.market_type)),
            }).into_response();
        }
        
        // Return the best market (first one, already sorted by liquidity)
        markets[0].condition_id.clone()
    } else {
        // Manual mode: user must provide market_id
        match req.market_id {
            Some(id) => id,
            None => {
                return Json(DeploySquadronResponse {
                    success: false,
                    squadron_id: None,
                    error: Some("Manual mode requires market_id".to_string()),
                }).into_response();
            }
        }
    };

    // ── One squadron per MARKET ──────────────────────────────────────────────
    // Two squadrons of a class are now safe — positions, config and budgets are
    // all keyed by squadron — so the only thing left to prevent is two squadrons
    // quoting the same book against each other, which is self-competition rather
    // than diversification.
    //
    // Checked HERE, after the market is resolved, rather than up with the class
    // checks: in quick mode `req.market_id` is None until the selection above
    // runs, so a check placed earlier saw an empty string and passed every time.
    // The registry cannot answer this either — a squadron records its market's
    // question, never its id — so the queue is the source of truth.
    if let Some(pool) = crate::helpers::db::pool() {
        if crate::helpers::db::deployment_markets_in_flight(pool).await
            .iter()
            .any(|m| m == &market_id)
        {
            warn!("📋 Deployment refused: a squadron is already on market {market_id}");
            return Json(DeploySquadronResponse {
                success: false,
                squadron_id: None,
                error: Some(
                    "A squadron is already running on that market — two squadrons quoting the \
                     same book compete with each other rather than diversifying. Pick a different \
                     market, or stand the existing squadron down first."
                        .to_string(),
                ),
            }).into_response();
        }
    }

    // Validate raptors and vipers (if manual mode)
    let raptors = if req.mode == "manual" && !req.raptors.is_empty() {
        req.raptors.clone()
    } else {
        // Auto-select default raptors for this market class
        default_raptors_for_class(&req.market_type).await
    };
    
    let vipers = if req.mode == "manual" && !req.vipers.is_empty() {
        req.vipers.clone()
    } else {
        // Auto-select default vipers for this market class
        default_vipers_for_class(&req.market_type).await
    };
    
    // Queue the deployment request for the CAG to process
    // NOTE: Full Admiral Adama extension will spawn actual squadron tasks.
    // For now, we record the intent and return success.
    let squadron_name = req.name.as_deref().map(str::trim).unwrap_or("");
    let deployment_id = format!("deploy-{}-{}", req.market_type, chrono::Utc::now().timestamp());
    
    info!(
        deployment_id = %deployment_id,
        market_id = %market_id,
        raptors = ?raptors,
        vipers = ?vipers,
        "🚀 Squadron deployment queued"
    );
    
    // Store deployment request in the database for CAG to pick up
    if let Err(e) = crate::helpers::db::queue_deployment(&deployment_id, &market_id, &req.market_type, squadron_name, &raptors, &vipers, &req.viper_budgets).await {
        error!("Failed to queue deployment: {}", e);
        return Json(DeploySquadronResponse {
            success: false,
            squadron_id: None,
            error: Some(format!("Failed to queue deployment: {}", e)),
        }).into_response();
    }
    
    Json(DeploySquadronResponse {
        success: true,
        squadron_id: Some(deployment_id),
        error: None,
    }).into_response()
}

/// The raptors a market class actually links to, from the taxonomy.
///
/// This used to return a hardcoded list naming things that do not exist —
/// "market_maker" and "reversal" are not raptor kinds — so a deploy request was
/// recorded, and reported back to the operator, advertising a stack the engine
/// would never run. The engine reads `market_class_raptor`; so does this now,
/// and the two cannot drift.
async fn default_raptors_for_class(market_class: &str) -> Vec<String> {
    match crate::helpers::db::pool() {
        Some(p) => crate::helpers::db::raptors_for_class(p, market_class).await,
        None => Vec::new(),
    }
}

/// The vipers a market class actually links to, from the taxonomy.
///
/// Same defect as the raptors above: "trailing_stop" is not a viper kind. A
/// politics deployment reported `["trailing_stop", "time_decay"]` while the
/// engine ran Arbitrage and Maker — the operator was shown neither what they
/// were getting nor anything that exists.
async fn default_vipers_for_class(market_class: &str) -> Vec<String> {
    match crate::helpers::db::pool() {
        Some(p) => crate::helpers::db::vipers_for_class(p, market_class).await,
        None => Vec::new(),
    }
}

/// Response for the deployment-row actions.
#[derive(Serialize)]
struct DeploymentActionResponse {
    success: bool,
    deployment_id: String,
    status: String,
    message: String,
}

/// POST /api/deployments/{id}/dismiss
///
/// Acknowledge a failed deployment so it stops being shown.
///
/// A failed row has no squadron behind it — the deployment never produced one —
/// so "stand down" is the wrong verb and there was nothing for the operator to
/// act on at all: the row simply sat there until a ten-minute timer retired it.
/// Dismissing marks it terminal rather than deleting it, so the failure and its
/// reason stay in the queue for anyone looking later.
async fn dismiss_deployment(Path(id): Path<String>) -> Response {
    info!("📥 POST /api/deployments/{}/dismiss", id);
    match crate::helpers::db::update_deployment_status(&id, "dismissed", None, None).await {
        Ok(()) => Json(DeploymentActionResponse {
            success: true,
            deployment_id: id,
            status: "dismissed".to_string(),
            message: "Deployment dismissed.".to_string(),
        }).into_response(),
        Err(e) => {
            warn!("dismiss {id} failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("could not dismiss: {e}")).into_response()
        }
    }
}

/// POST /api/deployments/{id}/retry
///
/// Put a failed deployment back in the queue.
///
/// Worth having because failures are not all alike. "Market is no longer listed"
/// will fail again and should be dismissed; a venue rate-limit or a transient
/// connect error is exactly the case where the same request succeeds moments
/// later, and re-entering it by hand means re-picking the market from a list
/// that has since moved on.
async fn retry_deployment(Path(id): Path<String>) -> Response {
    info!("📥 POST /api/deployments/{}/retry", id);
    // Back to 'pending' with the error cleared — the processor's own poll picks
    // it up, so this needs no venue-specific knowledge.
    match crate::helpers::db::update_deployment_status(&id, "pending", None, None).await {
        Ok(()) => Json(DeploymentActionResponse {
            success: true,
            deployment_id: id,
            status: "pending".to_string(),
            message: "Deployment re-queued — the engine collects it within a few seconds.".to_string(),
        }).into_response(),
        Err(e) => {
            warn!("retry {id} failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("could not retry: {e}")).into_response()
        }
    }
}

/// Response for GET /api/deployments.
#[derive(Serialize)]
struct DeploymentStatusResponse {
    id: String,
    market_id: String,
    market_type: String,
    raptors: Vec<String>,
    vipers: Vec<String>,
    status: String,
    squadron_id: Option<String>,
    error: Option<String>,
    created_at: String,
}

/// GET /api/deployments
///
/// Returns all deployment requests from the queue with their status.
async fn get_deployments() -> Response {
    debug!("Received GET /api/deployments");
    
    let deployments = crate::helpers::db::fetch_all_deployments().await;
    
    let response: Vec<DeploymentStatusResponse> = deployments.into_iter().map(|d| {
        DeploymentStatusResponse {
            id: d.0,
            market_id: d.1,
            market_type: d.2,
            raptors: d.3,
            vipers: d.4,
            status: d.5,
            squadron_id: d.6,
            error: d.7,
            created_at: d.8,
        }
    }).collect();
    
    Json(response).into_response()
}

// ─── Server startup ──────────────────────────────────────────────────────────

/// Spawn the Control Tower axum server.
///
/// Call once from `main()` via `tokio::spawn(run_api_server(...))`.
/// The function runs forever; errors are logged but do not crash the process.
pub async fn run_api_server(
    config_tx: Arc<watch::Sender<Arc<DynamicConfig>>>,
    config_rx: watch::Receiver<Arc<DynamicConfig>>,
    markets_rx: watch::Receiver<HashMap<String, String>>,
    raptor_health_rx: watch::Receiver<HashMap<String, AssetRaptorHealth>>,
    #[cfg(feature = "intl_clob")] safe_address: Address,
    cag: Cag,
) {
    let port = std::env::var("API_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(9000);

    // Expose the config broadcast to routes outside ApiState (Setup profile picker).
    //
    // Idempotent: `main` registers the same sender at the moment it creates the
    // channel, so anything reading the global config during startup sees it. This
    // call remains for callers that start an API server without going through
    // `main`, and re-registering the same Arc is a no-op.
    crate::helpers::dynamic_config::register_global_config_tx(Arc::clone(&config_tx));

    let api_key = std::env::var("DRADIS_API_KEY").ok();
    if api_key.is_some() {
        tracing::info!(" API key authentication enabled (DRADIS_API_KEY is set)");
    } else {
        tracing::info!(" API key authentication disabled (set DRADIS_API_KEY to enable)");
    }

    let read_only = std::env::var("DRADIS_READ_ONLY")
        .ok()
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    if read_only {
        tracing::info!(" READ-ONLY demo mode ENABLED — all mutating requests will be rejected (403)");
    }

    // Server-side ring buffer for Raptor signal telemetry (durable across reloads).
    let telemetry_history: TelemetryHistory = Arc::new(Mutex::new(HashMap::new()));

    let state = ApiState { config_tx, config_rx, markets_rx, raptor_health_rx: raptor_health_rx.clone(), api_key, read_only, #[cfg(feature = "intl_clob")] safe_address, cag, telemetry_history: telemetry_history.clone() };

    // Spawn the telemetry sampler — snapshots live Raptor signals into the ring
    // buffer every TELEMETRY_SAMPLE_SECS so the Control Tower has durable,
    // scrubable history that survives browser reloads.
    tokio::spawn(run_telemetry_sampler(raptor_health_rx, telemetry_history));

    // Venue latency probe — feeds GET /api/latency (Control Tower footer meter).
    tokio::spawn(crate::helpers::latency::run_latency_probe());

    // /api/health is intentionally public — no API key required.
    // Docker HEALTHCHECK, load balancers, and uptime monitors all probe this
    // endpoint without credentials; gating it would mark every container unhealthy.
    // /api/assets is also public — it contains no sensitive data and is queried
    // by the Control Tower before authentication is established.
    let public_routes = Router::new()
        .route("/api/health", get(health))
        .route("/api/assets", get(get_assets));

    // All other routes require X-API-Key when DRADIS_API_KEY is set.
    let protected_routes = Router::new()
        .route("/api/config",                get(get_config).patch(patch_config))
        .route("/api/config/schema",         get(get_config_schema))
        .route("/api/pnl/history",           get(get_pnl_history))
        .route("/api/trades",                get(get_trades))
        .route("/api/trades/stats",          get(get_trade_stats))
        .route("/api/trades/export",         get(export_trades))
        .route("/api/logs",                  get(get_logs))
        .route("/api/latency",               get(get_latency))
        .route("/api/vipers/status",         get(get_vipers_status))
        .route("/api/gboost/veto-scores",    get(get_gboost_veto_scores))
        .route("/api/positions",             get(get_open_positions))
        .route("/api/positions/pending",     get(get_pending_positions))
        .route("/api/positions/confirmed",   get(get_confirmed_positions))
        .route("/api/positions/{token_id}",  delete(delete_open_position))
        .route("/api/status",                get(get_status))
        .route("/api/telemetry",             get(get_telemetry))
        .route("/api/telemetry/history",     get(get_telemetry_history))
        .route("/api/telemetry/assets",      get(get_telemetry_assets))
        .route("/api/portfolio",             get(get_portfolio_value))
        .route("/api/llm/recommendations",   get(get_llm_recommendations))
        .route("/api/llm/actions",           get(get_llm_actions))
        .route("/api/llm/actions/{id}/approve", axum::routing::post(approve_llm_action))
        .route("/api/llm/actions/{id}/reject",  axum::routing::post(reject_llm_action))
        // ── Phase 3d: Squadron registry endpoints ──────────────────────────
        .route("/api/squadrons",             get(get_squadrons))
        .route("/api/squadrons/{id}",        get(get_squadron_by_id))
        .route("/api/squadrons/{id}/config", get(get_squadron_config).patch(patch_squadron_config))
        .route("/api/squadrons/deploy",      axum::routing::post(deploy_squadron))
        .route("/api/squadrons/{id}/stand-down", axum::routing::post(stand_down_squadron))
        // ── Squadron Deployment & Taxonomy (Admiral Adama extension) ───────
        .route("/api/deployment/region",     get(get_deployment_region))
        .route("/api/deployments",           get(get_deployments))
        .route("/api/deployments/{id}/dismiss", axum::routing::post(dismiss_deployment))
        .route("/api/deployments/{id}/retry",   axum::routing::post(retry_deployment))
        .route("/api/taxonomy/raptors",      get(get_taxonomy_raptors))
        .route("/api/taxonomy/vipers",       get(get_taxonomy_vipers))
        .route("/api/markets/available",     get(get_available_markets));

    // Intl-only endpoints: self-custody chain-sync + manual on-chain FAK exit.
    // The US custodial venue performs settlement/exit differently (Step 3b).
    #[cfg(feature = "intl_clob")]
    let protected_routes = protected_routes
        .route("/api/positions/sync",        axum::routing::post(sync_positions))
        .route("/api/positions/manual-exit", axum::routing::post(manual_exit))
        // Live bid/ask/mid for open positions — the surface a manual exit is
        // decided from, and intl-only for the same reason manual-exit is.
        .route("/api/positions/quotes",      get(get_position_quotes));

    let protected_routes = protected_routes
        // API-key check applied to all matched routes (inner layer — runs after CORS).
        // No-op when DRADIS_API_KEY is unset so local-dev workflow is unchanged.
        .layer(axum::middleware::from_fn_with_state(state.clone(), require_api_key))
        // Read-only demo gate — rejects all mutating methods when DRADIS_READ_ONLY=true.
        // No-op otherwise. All mutating endpoints live in protected_routes.
        .layer(axum::middleware::from_fn_with_state(state.clone(), enforce_read_only))
        .with_state(state.clone());

    // Setup & credentials management (prosumer onboarding). Admin-token gate
    // lives inside setup::admin_routes(); the X-API-Key and read-only layers
    // stack on top so existing deployment gates keep applying.
    let setup_routes = crate::api::setup::admin_routes()
        .merge(crate::api::setup::public_routes())
        .layer(axum::middleware::from_fn_with_state(state.clone(), require_api_key))
        .layer(axum::middleware::from_fn_with_state(state.clone(), enforce_read_only));

    let app = public_routes
        .merge(protected_routes)
        .merge(setup_routes)
        // Permissive CORS (outer layer — runs first, handles OPTIONS pre-flight
        // before the API-key middleware is reached).
        .layer(CorsLayer::permissive());

    // Admiral Adama deployment processor runs in main.rs where it has
    // access to full trading infrastructure (wallet_provider, etc.)

    let addr = format!("0.0.0.0:{}", port);
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l)  => l,
        Err(e) => {
            tracing::error!(" Control Tower API: failed to bind on {}: {}", addr, e);
            return;
        }
    };

    tracing::info!(" Control Tower API listening on port {}", port);

    if let Err(e) = axum::serve(listener, app.into_make_service()).await {
        tracing::error!(" Control Tower API error: {}", e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(ts: &str, collateral: &str, total: &str) -> db::PnlSnapshotRow {
        db::PnlSnapshotRow {
            ts: ts.to_string(),
            session_pnl: "0".to_string(),
            collateral: collateral.to_string(),
            total_value: Some(total.to_string()),
        }
    }

    fn secs(ts: &str) -> i64 {
        chrono::DateTime::parse_from_rfc3339(ts).unwrap().timestamp()
    }

    /// The exact production series that produced the phantom spike.
    ///
    /// On 2026-08-14 a FairValue buy filled at 19:36 and closed at 19:41 for
    /// +$0.44. The portfolio chart showed $64.06 → $66.78 → $64.50. The
    /// snapshots were correct all along; the join was picking each point's
    /// positions_value out of the future.
    fn production_series() -> Vec<db::PnlSnapshotRow> {
        // Newest first, exactly as `get_pnl_history` returns them.
        vec![
            snap("2026-08-14T19:42:15+00:00", "64.50", "64.50"),
            snap("2026-08-14T19:41:15+00:00", "61.71", "63.40"),
            snap("2026-08-14T19:37:15+00:00", "61.71", "63.40"),
            snap("2026-08-14T19:36:15+00:00", "61.71", "64.43"),
            snap("2026-08-14T19:35:15+00:00", "64.06", "64.06"),
            snap("2026-08-14T19:34:15+00:00", "64.06", "64.06"),
        ]
    }

    /// A newest-first slice must not resolve to a later row just because it
    /// came first. This is the whole bug: `find` over a ±120s window returned
    /// the newest match, so 19:34 paired with 19:36 and inherited a position
    /// that had not been bought yet.
    #[test]
    fn nearest_snapshot_does_not_drift_into_the_future() {
        let snaps = production_series();
        let hit = nearest_snapshot(&snaps, secs("2026-08-14T19:34:15+00:00"), 120).unwrap();
        assert_eq!(hit.ts, "2026-08-14T19:34:15+00:00", "must match itself, not 19:36");

        let hit = nearest_snapshot(&snaps, secs("2026-08-14T19:35:15+00:00"), 120).unwrap();
        assert_eq!(hit.ts, "2026-08-14T19:35:15+00:00");
    }

    /// The same skew ran the other way on the exit: 19:41 (mid-trade cash) was
    /// pairing with 19:42 (post-exit, no positions) and dropping the position
    /// from the chart entirely.
    #[test]
    fn nearest_snapshot_does_not_drop_a_live_position_at_the_exit() {
        let snaps = production_series();
        let hit = nearest_snapshot(&snaps, secs("2026-08-14T19:41:15+00:00"), 120).unwrap();
        assert_eq!(hit.ts, "2026-08-14T19:41:15+00:00");
        assert_eq!(hit.total_value.as_deref(), Some("63.40"), "position must still be counted");
    }

    /// Reconstruct the plotted series the way the handler does for a
    /// single-asset deployment and check it against the recorded truth.
    #[test]
    fn plotted_series_matches_the_recorded_snapshots() {
        let snaps = production_series();
        for primary in &snaps {
            let t = secs(&primary.ts);
            let collateral: f64 = primary.collateral.parse().unwrap();
            // The primary asset contributes its own row; every other asset is
            // matched by nearest. With one asset the two must agree.
            let matched = nearest_snapshot(&snaps, t, 120).unwrap();
            let pos = matched.total_value.as_ref().unwrap().parse::<f64>().unwrap()
                - matched.collateral.parse::<f64>().unwrap();
            let plotted = collateral + pos.max(0.0);
            let truth: f64 = primary.total_value.as_ref().unwrap().parse().unwrap();
            assert!(
                (plotted - truth).abs() < 0.005,
                "at {} plotted {plotted:.2} but the snapshot recorded {truth:.2}",
                primary.ts
            );
        }
    }

    /// Beyond the window there is no match at all, rather than a far-away row.
    #[test]
    fn nearest_snapshot_respects_the_window() {
        let snaps = production_series();
        assert!(nearest_snapshot(&snaps, secs("2026-08-14T18:00:00+00:00"), 120).is_none());
        // 19:37 is 240s from 19:41 — outside a 120s window, inside a 300s one.
        assert!(nearest_snapshot(&snaps[2..3], secs("2026-08-14T19:41:15+00:00"), 120).is_none());
        assert!(nearest_snapshot(&snaps[2..3], secs("2026-08-14T19:41:15+00:00"), 300).is_some());
    }
}

#[cfg(test)]
mod pool_logging_tests {
    /// Every `log_pool_unavailable` call must pass an asset when one is in scope,
    /// or the severity check below it can never fire.
    ///
    /// The failure this guards against is silent: a call site that passes `None`
    /// while a `q.asset` sits right there still compiles, still logs, and still
    /// produces the ERROR wall for an unconfigured asset. Only the endpoints with
    /// genuinely no asset in scope are exempt, and they are named here so adding
    /// one is a deliberate act.
    #[test]
    fn pool_unavailable_call_sites_pass_their_asset() {
        let src = include_str!("server.rs");
        let prod = src.split("#[cfg(test)]").next().unwrap_or(src);
        const EXEMPT: &[&str] = &["squadrons"];
        let offenders: Vec<String> = prod
            .lines()
            .enumerate()
            .filter(|(_, l)| l.contains("log_pool_unavailable(") && l.trim().ends_with("None);"))
            .filter(|(_, l)| !EXEMPT.iter().any(|e| l.contains(e)))
            .map(|(i, l)| format!("{}: {}", i + 1, l.trim()))
            .collect();
        assert!(
            offenders.is_empty(),
            "these call sites pass None but are not exempt, so an unconfigured asset \
             would log at ERROR:\n{}",
            offenders.join("\n"),
        );
    }

    /// A missing DB pool must not be logged at ERROR unconditionally.
    ///
    /// Before Setup completes the engine parks on purpose and has no pool, while
    /// the dashboard polls anyway. A fresh AMI logged 32 ERROR lines in its first
    /// minute for that expected state — a wall of red for a buyer who has done
    /// nothing wrong, right after the watchdog fix stopped the engine
    /// crash-looping in the same window.
    ///
    /// `log_pool_unavailable` picks the severity from
    /// `watchdog::is_parked_for_setup()`. This asserts against the source because
    /// a new endpoint copying the old `error!` line is the likely regression, and
    /// nothing at the call site hints that severity is conditional.
    #[test]
    fn pool_unavailable_goes_through_the_severity_aware_helper() {
        let src = include_str!("server.rs");
        let prod = src.split("#[cfg(test)]").next().unwrap_or(src);
        let offenders: Vec<String> = prod
            .lines()
            .enumerate()
            .filter(|(_, l)| {
                let l = l.trim();
                l.starts_with("error!(") && l.contains("Database pool not available")
            })
            .map(|(i, l)| format!("{}: {}", i + 1, l.trim()))
            .collect();

        // Exactly one is expected: the helper's own else-branch.
        assert_eq!(
            offenders.len(), 1,
            "endpoints must call log_pool_unavailable() rather than error! directly, \
             so a parked engine does not report expected idleness as failure: {offenders:#?}",
        );
    }
}
