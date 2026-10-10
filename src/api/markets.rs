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

//! The Markets page's API: live markets on the instance's venue, each with the
//! venue's own picture (facts, quotes, depth, prints, history) and the engine's
//! view of it (the squadron flying it and every viper's verdict).
//!
//! Venue-neutral by construction. Every venue read goes through the session's
//! `Execution` handle, exactly as the Helm API's `book_facts` does, so this file
//! carries no `#[cfg]` and never names a venue. A venue that publishes no depth,
//! prints or history answers `Ok(None)` and the page says "not published by
//! this venue"; a failed read is a 503, which is a different fact.
//!
//! Everything that touches the venue sits behind a short cache keyed by market,
//! so a dashboard polling every few seconds costs the venue one read per key
//! per TTL, and one shared HTTP client serves the list endpoint (the older
//! `/api/markets/available` builds a client per call; that is not copied here).
//!
//! Honest-state rule ([B43]): a figure we do not have arrives as `null`, never
//! as 0. A closed market renders as closed with no quotes rather than as a book
//! of zeros.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tracing::debug;

use crate::api::server::ApiState;
use crate::helpers::db;
use crate::venues::core::{Execution, MarketFacts, MarketId, TokenResolution};

const LIST_TTL: Duration = Duration::from_secs(20);
const DETAIL_TTL: Duration = Duration::from_secs(5);
const FACTS_TTL: Duration = Duration::from_secs(60);
const BOOK_TTL: Duration = Duration::from_secs(5);
const PRINTS_TTL: Duration = Duration::from_secs(15);
const HISTORY_TTL: Duration = Duration::from_secs(30);
const CACHE_MAX_KEYS: usize = 500;
const DEFAULT_PRINTS: usize = 50;
const MAX_PRINTS: usize = 200;
const DEFAULT_HISTORY_HOURS: i64 = 2;
const MAX_HISTORY_HOURS: i64 = 24;
const NOT_PUBLISHED: &str = "not published by this venue";

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/markets/live", get(live_markets))
        .route("/api/markets/{id}", get(market_detail))
        .route("/api/markets/{id}/book", get(market_book))
        .route("/api/markets/{id}/prints", get(market_prints))
        .route("/api/markets/{id}/history", get(market_history))
}

fn err(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, Json(serde_json::json!({ "error": msg.into() }))).into_response()
}

// ── Caches ──────────────────────────────────────────────────────────────────

type ValueCache = Mutex<HashMap<String, (Instant, Arc<serde_json::Value>)>>;

fn value_cache() -> &'static ValueCache {
    static C: OnceLock<ValueCache> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cache_get(key: &str, ttl: Duration) -> Option<Arc<serde_json::Value>> {
    let map = value_cache().lock().ok()?;
    map.get(key).filter(|(at, _)| at.elapsed() < ttl).map(|(_, v)| Arc::clone(v))
}

fn cache_put(key: String, v: Arc<serde_json::Value>) {
    if let Ok(mut map) = value_cache().lock() {
        if map.len() >= CACHE_MAX_KEYS {
            // Drop the oldest half rather than the whole map, so a burst of
            // distinct markets does not evict the ones being watched.
            let mut by_age: Vec<(String, Instant)> = map.iter().map(|(k, (t, _))| (k.clone(), *t)).collect();
            by_age.sort_by_key(|(_, t)| *t);
            for (k, _) in by_age.into_iter().take(CACHE_MAX_KEYS / 2) {
                map.remove(&k);
            }
        }
        map.insert(key, (Instant::now(), v));
    }
}

/// Serve `key` from the cache inside `ttl`, else compute, store and serve.
async fn cached<F>(key: String, ttl: Duration, fill: F) -> Result<Arc<serde_json::Value>, Response>
where
    F: std::future::Future<Output = Result<serde_json::Value, Response>>,
{
    if let Some(v) = cache_get(&key, ttl) {
        debug!("markets cache hit: {key}");
        return Ok(v);
    }
    let v = Arc::new(fill.await?);
    cache_put(key, Arc::clone(&v));
    Ok(v)
}

type FactsCache = Mutex<HashMap<String, (Instant, Arc<MarketFacts>)>>;

fn facts_cache() -> &'static FactsCache {
    static C: OnceLock<FactsCache> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

fn shared_http() -> &'static reqwest::Client {
    static H: OnceLock<reqwest::Client> = OnceLock::new();
    H.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default()
    })
}

// ── The venue session ───────────────────────────────────────────────────────

fn session(s: &ApiState) -> Result<crate::cag::session::SessionState, Response> {
    s.cag.session().ok_or_else(|| err(
        StatusCode::SERVICE_UNAVAILABLE,
        "no venue session is registered yet; the engine is still starting, retry in a few seconds",
    ))
}

/// The venue's facts for a market, cached for a minute: they change at
/// resolution and nowhere else, and every endpoint below needs the token ids.
async fn facts_for(venue: &dyn Execution, id: &str) -> Result<Arc<MarketFacts>, Response> {
    if let Ok(map) = facts_cache().lock() {
        if let Some((at, f)) = map.get(id) {
            if at.elapsed() < FACTS_TTL {
                return Ok(Arc::clone(f));
            }
        }
    }
    let market = MarketId::new(id);
    let facts = match venue.market_facts(&market).await {
        Ok(Some(f)) => f,
        Ok(None) => return Err(err(StatusCode::NOT_FOUND, format!("the venue knows no market {id}"))),
        Err(e) => return Err(err(StatusCode::SERVICE_UNAVAILABLE, format!("reading market {id} from the venue failed: {e}"))),
    };
    if facts.market_id != market {
        return Err(err(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("the venue answered for {} when asked about {id}", facts.market_id),
        ));
    }
    let facts = Arc::new(facts);
    if let Ok(mut map) = facts_cache().lock() {
        map.insert(id.to_string(), (Instant::now(), Arc::clone(&facts)));
    }
    Ok(facts)
}

// ── Pure shaping ────────────────────────────────────────────────────────────

/// Best bid and ask for one leg, with the derived mid and spread only when
/// both sides rest. Decimals serialize as strings; a missing side is `null`.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct SideQuote {
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
    pub mid: Option<Decimal>,
    /// `ask - bid`. Negative on a crossed book, reported as such rather than
    /// clamped: a crossed book is a fact about the venue worth seeing.
    pub spread: Option<Decimal>,
}

pub fn side_quote(bid: Option<Decimal>, ask: Option<Decimal>) -> SideQuote {
    let (mid, spread) = match (bid, ask) {
        (Some(b), Some(a)) => (Some((a + b) / Decimal::TWO), Some(a - b)),
        _ => (None, None),
    };
    SideQuote { bid, ask, mid, spread }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct BookSummary {
    pub yes: SideQuote,
    pub no: SideQuote,
    /// `yes.ask + no.ask`: above 1.00 is the taker's round-trip cost of a
    /// hedged pair, below 1.00 is the arbitrage. `null` unless both asks rest.
    pub ask_sum: Option<Decimal>,
}

pub fn book_summary(yes: (Option<Decimal>, Option<Decimal>), no: (Option<Decimal>, Option<Decimal>)) -> BookSummary {
    let yes = side_quote(yes.0, yes.1);
    let no = side_quote(no.0, no.1);
    let ask_sum = match (yes.ask, no.ask) {
        (Some(a), Some(b)) => Some(a + b),
        _ => None,
    };
    BookSummary { yes, no, ask_sum }
}

/// `("live" | "closed", seconds to close)`.
///
/// The venue's own word wins when it gives one. A sports market's listed close
/// is kick-off and the book trades through the game, so with the venue still
/// accepting orders it is live with `secs_to_close` 0 (past its listed close,
/// still trading); a market the venue has closed is closed however far off its
/// listed close. Without a word from the venue the clock decides, and a market
/// with no close time at all is live with an unknown horizon.
pub fn market_state(close: Option<DateTime<Utc>>, accepting_orders: Option<bool>, now: DateTime<Utc>) -> (&'static str, Option<i64>) {
    let countdown = close.map(|c| (c - now).num_seconds().max(0));
    match accepting_orders {
        Some(false) => ("closed", Some(0)),
        Some(true) => ("live", countdown),
        None => match close {
            Some(c) if c <= now => ("closed", Some(0)),
            Some(_) => ("live", countdown),
            None => ("live", None),
        },
    }
}

/// Every market the engine is flying, as `(squadron_id, market_id, class)`.
///
/// Two sources, because squadrons arrive two ways. Helm and the auto-deploy
/// seeder go through `deployment_queue`, whose row carries the class. The
/// hourly crypto squadrons never touch the queue: the CAG rotates them onto
/// each hour's market itself, so their only record is the registry summary,
/// whose `market_id` is the `MarketConfig` condition id. Reading the queue
/// alone left the BTC hourly unflagged on the list and its detail without a
/// squadron or verdicts, while the engine was flying it.
///
/// A stood-down or returning squadron is not flying anything.
async fn flown_markets(s: &ApiState) -> Vec<(String, String, String)> {
    let mut out: Vec<(String, String, String)> = match db::pool() {
        Some(pool) => db::active_deployments(pool).await,
        None => Vec::new(),
    };
    for q in s.cag.list_squadrons() {
        let Some(market_id) = q.market_id.clone().filter(|m| !m.is_empty()) else { continue };
        if matches!(q.state.as_str(), "RTB" | "STOOD_DOWN") || out.iter().any(|(sq, _, _)| *sq == q.id) {
            continue;
        }
        let class = if !q.market_class.is_empty() {
            q.market_class.to_lowercase()
        } else if matches!(q.asset.to_ascii_lowercase().as_str(), "btc" | "eth" | "sol") {
            "crypto".to_string()
        } else {
            continue;
        };
        out.push((q.id.clone(), market_id, class));
    }
    out
}

fn published<T: Serialize>(v: Option<T>) -> serde_json::Value {
    match v {
        Some(data) => serde_json::json!({ "published": true, "data": data }),
        None => serde_json::json!({ "published": false, "reason": NOT_PUBLISHED }),
    }
}

// ── GET /api/markets/live ───────────────────────────────────────────────────

#[derive(Deserialize)]
struct LiveQuery {
    market_type: String,
    expiry_window: Option<String>,
    min_liquidity: Option<f64>,
}

#[derive(Serialize)]
struct LiveMarketRow {
    market_id: String,
    question: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    criteria: String,
    market_class: String,
    end_date: Option<String>,
    /// The venue's word on whether the book is open, known only for a flown
    /// market read from its facts; a sports market's `end_date` is kick-off, so
    /// `true` past it means "in play, still trading", not closed.
    #[serde(skip_serializing_if = "Option::is_none")]
    accepting_orders: Option<bool>,
    /// The venue's liquidity or volume figure; `null` for a market the venue
    /// list omitted and the engine is flying anyway.
    liquidity: Option<f64>,
    /// `null` only when the venue did not answer for a flown market (see `note`).
    tokens: Option<LiveTokens>,
    squadron_id: Option<String>,
    squadron_asset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    note: Option<String>,
}

#[derive(Serialize)]
struct LiveTokens {
    yes_id: String,
    no_id: String,
}

/// The venue's expiry window for a browse, in seconds.
pub(crate) fn expiry_window_secs(window: Option<&str>, default: i64) -> i64 {
    match window {
        Some("1h") => 3600,
        Some("4h") => 14400,
        Some("24h") => 86400,
        Some("7d") => 604800,
        Some("30d") => 2592000,
        Some("90d") => 7776000,
        Some("1y") => 31_536_000,
        Some("2y") => 63_072_000,
        _ => default,
    }
}

async fn live_markets(State(s): State<ApiState>, Query(q): Query<LiveQuery>) -> Response {
    let market_type = q.market_type.trim().to_lowercase();
    if !matches!(market_type.as_str(), "crypto" | "sports" | "politics") {
        return err(StatusCode::BAD_REQUEST, "market_type must be crypto, sports or politics");
    }
    let window = q.expiry_window.clone().unwrap_or_default();
    let min_liq = q.min_liquidity.unwrap_or(crate::api::server::DISCOVERY_MIN_LIQUIDITY);
    let key = format!("live:{market_type}:{window}:{min_liq}");
    match cached(key, LIST_TTL, async {
        let max_expiry = expiry_window_secs(q.expiry_window.as_deref(), crate::api::server::default_expiry_secs(&market_type));
        let listed = crate::api::server::fetch_markets_by_type(shared_http(), &market_type, max_expiry, min_liq).await;

        // Which of these the engine is flying, and which flown markets the
        // venue list left out.
        let deployments: Vec<(String, String, String)> =
            flown_markets(&s).await.into_iter().filter(|(_, _, t)| *t == market_type).collect();
        let squadron_for = |market_id: &str| -> Option<(String, Option<String>)> {
            deployments.iter().find(|(_, m, _)| m == market_id).map(|(sq, _, _)| {
                let asset = s.cag.get_squadron(sq).map(|q| q.asset.to_lowercase());
                (sq.clone(), asset)
            })
        };
        let mut rows: Vec<LiveMarketRow> = listed.into_iter().map(|m| {
            let (squadron_id, squadron_asset) = squadron_for(&m.condition_id).map(|(a, b)| (Some(a), b)).unwrap_or((None, None));
            LiveMarketRow {
                market_id: m.condition_id, question: m.question, criteria: m.criteria,
                market_class: m.market_class, end_date: m.end_date, accepting_orders: None, liquidity: Some(m.liquidity),
                tokens: Some(LiveTokens { yes_id: m.tokens.yes_id, no_id: m.tokens.no_id }),
                squadron_id, squadron_asset, note: None,
            }
        }).collect();
        let listed_ids: std::collections::HashSet<String> = rows.iter().map(|r| r.market_id.clone()).collect();
        let missing: Vec<&(String, String, String)> = deployments.iter().filter(|(_, m, _)| !listed_ids.contains(m)).collect();
        if !missing.is_empty() {
            let session = session(&s)?;
            for (sq, market_id, class) in missing {
                let summary = s.cag.get_squadron(sq);
                let asset = summary.as_ref().map(|q| q.asset.to_lowercase());
                let row = match facts_for(session.venue.as_ref(), market_id).await {
                    Ok(facts) => LiveMarketRow {
                        market_id: market_id.clone(), question: facts.question.clone(), criteria: facts.criteria.clone(),
                        market_class: class.clone(), end_date: facts.close_time.map(|t| t.to_rfc3339()),
                        accepting_orders: facts.accepting_orders, liquidity: None,
                        tokens: Some(LiveTokens { yes_id: facts.yes_token.as_str().to_string(), no_id: facts.no_token.as_str().to_string() }),
                        squadron_id: Some(sq.clone()), squadron_asset: asset, note: None,
                    },
                    // The venue did not answer for it this time; the squadron
                    // still flies it, so it stays on the list under its own name.
                    Err(_) => LiveMarketRow {
                        market_id: market_id.clone(),
                        question: summary.as_ref().map(|q| q.market_name.clone()).unwrap_or_else(|| market_id.clone()),
                        criteria: String::new(), market_class: class.clone(), end_date: None, accepting_orders: None, liquidity: None, tokens: None,
                        squadron_id: Some(sq.clone()), squadron_asset: asset,
                        note: Some("the venue did not answer for this market; shown from the squadron's record".to_string()),
                    },
                };
                rows.insert(0, row);
            }
        }
        Ok(serde_json::json!({ "markets": rows, "as_of": Utc::now().to_rfc3339() }))
    }).await {
        Ok(v) => Json((*v).clone()).into_response(),
        Err(r) => r,
    }
}

// ── GET /api/markets/{id} ───────────────────────────────────────────────────

#[derive(Serialize)]
struct EngineSquadron {
    id: String,
    asset: String,
    name: String,
    state: String,
    vipers: Vec<String>,
}

/// The engine's own view of a market.
///
/// Verdicts are the squadron's, keyed by its asset, which is the closest thing
/// the registry has to a per-market answer; `verdict_scope` says so. The model
/// reading is FairValue's own arithmetic on this market with the GLOBAL knobs,
/// computed on demand and labeled as a reading, not a decision. The sports
/// line is the bookmaker consensus the sports ledger holds for these tokens.
/// Each is `null` with a sentence in its `_unavailable` field when it cannot
/// be produced, and `null` with no sentence when it does not apply.
#[derive(Serialize)]
struct EngineView {
    squadron: Option<EngineSquadron>,
    verdicts: Vec<crate::helpers::viper_status::ViperStatusView>,
    verdict_scope: &'static str,
    model: Option<crate::vipers::fairvalue_impl::ModelReading>,
    model_unavailable: Option<String>,
    sports: Option<SportsLineView>,
    sports_unavailable: Option<String>,
}

#[derive(Serialize)]
struct SportsSideLine {
    outcome_label: String,
    consensus: f64,
    num_books: i64,
    dispersion: Option<f64>,
    drift: Option<f64>,
    drift_secs: Option<i64>,
    odds_age_secs: i64,
}

#[derive(Serialize)]
struct SportsLineView {
    yes: Option<SportsSideLine>,
    no: Option<SportsSideLine>,
    commence: Option<String>,
}

fn sports_side(line: &crate::raptors::sports_ledger::SportsLine, now: DateTime<Utc>) -> SportsSideLine {
    SportsSideLine {
        outcome_label: line.outcome_label.clone(),
        consensus: line.consensus,
        num_books: line.num_books,
        dispersion: line.dispersion,
        drift: line.drift,
        drift_secs: line.drift_secs,
        odds_age_secs: (now - line.odds_at).num_seconds().max(0),
    }
}

/// The strike of an "Up or Down" market: the underlying's one-minute open at
/// the window start, cached for a day once the window has opened (it never
/// changes after that). `Err` carries the sentence for `model_unavailable`.
async fn up_down_strike(asset: &str, market_id: &str, question: &str, criteria: &str, close: DateTime<Utc>, now: DateTime<Utc>) -> Result<Decimal, String> {
    let window = crate::helpers::time::up_down_window_secs(question);
    let reference = if window == 3600 {
        close - chrono::Duration::hours(1)
    } else {
        crate::helpers::time::daily_window_reference_time(close).ok_or_else(|| "the close time has no Eastern equivalent".to_string())?
    };
    if reference > now {
        let et = reference.with_timezone(&chrono_tz::US::Eastern);
        return Err(format!("no strike until the window opens at {} ET", et.format("%-I:%M %p")));
    }
    let key = format!("strike:{asset}:{market_id}:{}", reference.timestamp());
    if let Some(v) = cache_get(&key, Duration::from_secs(86_400)) {
        if let Some(d) = v.as_str().and_then(|x| x.parse::<Decimal>().ok()) {
            return Ok(d);
        }
    }
    // The engine's own resolver first: a daily market's description names the
    // candle it compares, and the viper's strike comes from exactly that text.
    // The window math is the fallback for a description that names no time.
    let from_criteria = if window == 3600 || criteria.is_empty() {
        None
    } else {
        crate::helpers::time::fetch_historical_strike_price(shared_http(), asset, criteria).await
    };
    let strike = match from_criteria {
        Some(d) => Some(d),
        None => crate::helpers::time::fetch_window_open(shared_http(), asset, reference).await,
    };
    match strike {
        Some(d) => {
            cache_put(key, Arc::new(serde_json::Value::String(d.to_string())));
            Ok(d)
        }
        None => Err("the window open could not be read from Binance; retry in a moment".to_string()),
    }
}

/// Does FairValue price this kind of market at all? It trades "Up or Down"
/// windows and above/below strikes, both terminal digitals on one price. A
/// "reach $150,000 by December" market is a touch, which the same formula
/// misprices by about half, so it gets no reading rather than a wrong one.
fn fairvalue_prices_this(question_lc: &str, quoted_strike: bool) -> bool {
    if question_lc.contains("up or down") {
        return true;
    }
    if !quoted_strike {
        return false;
    }
    let touch = ["reach", "hit", "touch", "dip to", "fall to", "rise to"].iter().any(|w| question_lc.contains(w));
    let digital = ["above", "below", "higher than", "lower than", "at or above", "at or below"].iter().any(|w| question_lc.contains(w));
    digital && !touch
}

/// FairValue's reading of a crypto market, or the sentence saying why not.
async fn model_reading(
    s: &ApiState,
    facts: &MarketFacts,
    quotes: Option<&BookSummary>,
    now: DateTime<Utc>,
) -> (Option<crate::vipers::fairvalue_impl::ModelReading>, Option<String>) {
    let Some(asset) = crate::helpers::market_title::infer_asset_from_title(&facts.question) else {
        return (None, None);
    };
    let question_lc = facts.question.to_lowercase();
    let quoted_strike = crate::helpers::time::extract_strike_price(&facts.question);
    if !fairvalue_prices_this(&question_lc, quoted_strike.is_some()) {
        return (None, Some("FairValue prices Up or Down windows and above/below strikes only; this market is neither".to_string()));
    }
    let Some(close) = facts.close_time else {
        return (None, Some("the venue reports no close time".to_string()));
    };
    let spot = s.raptor_health_rx.borrow().get(asset).map(|h| h.oracle_price).filter(|p| *p > Decimal::ZERO);
    let Some(spot) = spot.and_then(|p| p.to_f64()) else {
        return (None, Some(format!("no oracle price: no squadron feeds the {asset} price raptor")));
    };
    let strike = match quoted_strike {
        Some(k) => k,
        None => match up_down_strike(asset, facts.market_id.as_str(), &facts.question, &facts.criteria, close, now).await {
            Ok(k) => k,
            Err(why) => return (None, Some(why)),
        },
    };
    let Some(strike) = strike.to_f64() else {
        return (None, Some("the strike cannot be priced".to_string()));
    };
    let dc = s.config_rx.borrow().clone();
    let (yes_ask, no_ask) = quotes.map(|q| (q.yes.ask, q.no.ask)).unwrap_or((None, None));
    match crate::vipers::fairvalue_impl::FairValueStrategyImpl::model_reading(asset, &dc, spot, strike, now, close, yes_ask, no_ask) {
        Ok(r) => (Some(r), None),
        Err(e) => (None, Some(e.to_string())),
    }
}

async fn engine_view(s: &ApiState, market_id: &str, facts: &MarketFacts, quotes: Option<&BookSummary>, now: DateTime<Utc>) -> EngineView {
    let deployment = flown_markets(s).await.into_iter().find(|(_, m, _)| m == market_id);
    let squadron = deployment.as_ref()
        .and_then(|(sq, _, _)| s.cag.get_squadron(sq))
        .map(|q| EngineSquadron { id: q.id.clone(), asset: q.asset.to_lowercase(), name: q.name.clone(), state: q.state.clone(), vipers: q.vipers.clone() });
    let verdicts = match &squadron {
        Some(q) => crate::helpers::viper_status::snapshot(Some(&q.asset)),
        None => Vec::new(),
    };
    let (model, model_unavailable) = model_reading(s, facts, quotes, now).await;

    let flown_as_sports = deployment.as_ref().is_some_and(|(_, _, class)| class == "sports");
    let (sports, sports_unavailable) = match crate::raptors::sports_ledger::line_for(facts.yes_token.as_str(), facts.no_token.as_str()) {
        Some(line) => (
            Some(SportsLineView {
                commence: line.yes.as_ref().or(line.no.as_ref()).map(|l| l.commence.to_rfc3339()),
                yes: line.yes.as_ref().map(|l| sports_side(l, now)),
                no: line.no.as_ref().map(|l| sports_side(l, now)),
            }),
            None,
        ),
        None if flown_as_sports && !crate::raptors::sports_ledger::board_ready() => (None, Some("the sports board has not spoken yet".to_string())),
        None if flown_as_sports => (None, Some("no bookmaker line on the board for these tokens".to_string())),
        None => (None, None),
    };

    EngineView { squadron, verdicts, verdict_scope: "squadron", model, model_unavailable, sports, sports_unavailable }
}

async fn ours(market_name: &str, yes: &str, no: &str) -> db::MarketActivity {
    let mut all = db::MarketActivity::default();
    for pool in db::all_pools() {
        let part = db::market_activity(&pool, market_name, &[yes, no]).await;
        all.entries.extend(part.entries);
        all.trades.extend(part.trades);
        all.open_positions.extend(part.open_positions);
    }
    all.entries.sort_by(|a, b| b.ts.cmp(&a.ts));
    all.trades.sort_by(|a, b| b.ts.cmp(&a.ts));
    all
}

async fn market_detail(State(s): State<ApiState>, Path(id): Path<String>) -> Response {
    match cached(format!("detail:{id}"), DETAIL_TTL, async {
        let session = session(&s)?;
        let venue = session.venue.as_ref();
        let facts = facts_for(venue, &id).await?;
        let now = Utc::now();
        let (state, secs_to_close) = market_state(facts.close_time, facts.accepting_orders, now);
        let past_listed_close = state == "live" && facts.close_time.is_some_and(|c| c <= now);

        // A failed quote costs the quotes, not the page: facts, our history and
        // the engine's view are all still true without the venue's book.
        let mut quotes_unavailable: Option<String> = None;
        let (quotes, resolution) = if state == "live" {
            let (y, n) = tokio::join!(venue.quote_both(&facts.yes_token), venue.quote_both(&facts.no_token));
            match (y, n) {
                (Ok(y), Ok(n)) => (Some(book_summary(y, n)), None),
                (Err(e), _) | (_, Err(e)) => {
                    quotes_unavailable = Some(format!("the venue did not answer for the book: {e}"));
                    (None, None)
                }
            }
        } else {
            // A venue reports a leg resolved only at a decisive price (Gamma's
            // reader calls 0.995 final; see `resolution_for_token`), so the
            // same threshold decides the word here.
            let res = match venue.resolution(&facts.yes_token).await {
                Ok(TokenResolution::Resolved(p)) if p >= Decimal::new(99, 2) => Some("yes"),
                Ok(TokenResolution::Resolved(p)) if p <= Decimal::new(1, 2) => Some("no"),
                _ => None,
            };
            (None, res)
        };

        let activity = ours(&facts.question, facts.yes_token.as_str(), facts.no_token.as_str()).await;
        let engine = engine_view(&s, &id, &facts, quotes.as_ref(), now).await;

        Ok(serde_json::json!({
            "market_id": id,
            "question": facts.question,
            "criteria": facts.criteria,
            "leg_labels": facts.leg_labels,
            "yes_token": facts.yes_token.as_str(),
            "no_token": facts.no_token.as_str(),
            "close_time": facts.close_time.map(|t| t.to_rfc3339()),
            "secs_to_close": secs_to_close,
            "state": state,
            "past_listed_close": past_listed_close,
            "resolution": resolution,
            "quotes": quotes,
            "quotes_unavailable": quotes_unavailable,
            "ours": activity,
            "engine": engine,
            "as_of": now.to_rfc3339(),
        }))
    }).await {
        Ok(v) => Json((*v).clone()).into_response(),
        Err(r) => r,
    }
}

// ── GET /api/markets/{id}/book | prints | history ──────────────────────────

async fn market_book(State(s): State<ApiState>, Path(id): Path<String>) -> Response {
    match cached(format!("book:{id}"), BOOK_TTL, async {
        let session = session(&s)?;
        let venue = session.venue.as_ref();
        let facts = facts_for(venue, &id).await?;
        let (y, n) = tokio::join!(venue.order_book(&facts.yes_token), venue.order_book(&facts.no_token));
        let y = y.map_err(|e| err(StatusCode::SERVICE_UNAVAILABLE, format!("reading the YES book failed: {e}")))?;
        let n = n.map_err(|e| err(StatusCode::SERVICE_UNAVAILABLE, format!("reading the NO book failed: {e}")))?;
        Ok(published(match (y, n) {
            (None, None) => None,
            (y, n) => Some(serde_json::json!({ "yes": y, "no": n, "as_of": Utc::now().to_rfc3339() })),
        }))
    }).await {
        Ok(v) => Json((*v).clone()).into_response(),
        Err(r) => r,
    }
}

#[derive(Deserialize)]
struct PrintsQuery {
    limit: Option<usize>,
}

async fn market_prints(State(s): State<ApiState>, Path(id): Path<String>, Query(q): Query<PrintsQuery>) -> Response {
    let limit = q.limit.unwrap_or(DEFAULT_PRINTS).clamp(1, MAX_PRINTS);
    match cached(format!("prints:{id}:{limit}"), PRINTS_TTL, async {
        let session = session(&s)?;
        let venue = session.venue.as_ref();
        let prints = venue.recent_prints(&MarketId::new(&id), limit).await
            .map_err(|e| err(StatusCode::SERVICE_UNAVAILABLE, format!("reading prints failed: {e}")))?;
        Ok(published(prints.map(|p| serde_json::json!({ "prints": p, "as_of": Utc::now().to_rfc3339() }))))
    }).await {
        Ok(v) => Json((*v).clone()).into_response(),
        Err(r) => r,
    }
}

#[derive(Deserialize)]
struct HistoryQuery {
    hours: Option<i64>,
}

async fn market_history(State(s): State<ApiState>, Path(id): Path<String>, Query(q): Query<HistoryQuery>) -> Response {
    let hours = q.hours.unwrap_or(DEFAULT_HISTORY_HOURS).clamp(1, MAX_HISTORY_HOURS);
    match cached(format!("history:{id}:{hours}"), HISTORY_TTL, async {
        let session = session(&s)?;
        let venue = session.venue.as_ref();
        let facts = facts_for(venue, &id).await?;
        let since = Utc::now() - chrono::Duration::hours(hours);
        let (y, n) = tokio::join!(venue.price_history(&facts.yes_token, since), venue.price_history(&facts.no_token, since));
        let y = y.map_err(|e| err(StatusCode::SERVICE_UNAVAILABLE, format!("reading YES history failed: {e}")))?;
        let n = n.map_err(|e| err(StatusCode::SERVICE_UNAVAILABLE, format!("reading NO history failed: {e}")))?;
        Ok(published(match (y, n) {
            (None, None) => None,
            (y, n) => Some(serde_json::json!({ "yes": y, "no": n, "hours": hours, "as_of": Utc::now().to_rfc3339() })),
        }))
    }).await {
        Ok(v) => Json((*v).clone()).into_response(),
        Err(r) => r,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// A one-sided book has a bid or an ask, not a mid of half of something.
    #[test]
    fn mid_and_spread_are_null_when_one_side_is_missing() {
        let q = side_quote(Some(dec!(0.55)), None);
        assert_eq!(q.bid, Some(dec!(0.55)));
        assert!(q.ask.is_none() && q.mid.is_none() && q.spread.is_none());
        let q = side_quote(Some(dec!(0.55)), Some(dec!(0.57)));
        assert_eq!(q.mid, Some(dec!(0.56)));
        assert_eq!(q.spread, Some(dec!(0.02)));
    }

    #[test]
    fn ask_sum_is_null_unless_both_asks_exist() {
        let b = book_summary((Some(dec!(0.55)), Some(dec!(0.57))), (Some(dec!(0.42)), None));
        assert!(b.ask_sum.is_none());
        let b = book_summary((Some(dec!(0.55)), Some(dec!(0.57))), (Some(dec!(0.42)), Some(dec!(0.45))));
        assert_eq!(b.ask_sum, Some(dec!(1.02)));
    }

    /// A crossed book is a fact about the venue; clamping it to zero would hide it.
    #[test]
    fn a_crossed_book_reports_a_negative_spread_rather_than_zero() {
        let q = side_quote(Some(dec!(0.60)), Some(dec!(0.58)));
        assert_eq!(q.spread, Some(dec!(-0.02)));
    }

    #[test]
    fn a_market_past_its_close_is_closed_and_one_before_it_counts_down() {
        let now = Utc::now();
        assert_eq!(market_state(Some(now - chrono::Duration::seconds(1)), None, now), ("closed", Some(0)));
        let (st, secs) = market_state(Some(now + chrono::Duration::seconds(600)), None, now);
        assert_eq!(st, "live");
        assert!(secs.is_some_and(|s| (598..=600).contains(&s)));
        assert_eq!(market_state(None, None, now), ("live", None));
    }

    /// A sports market's listed close is kick-off. The venue still accepting
    /// orders makes it live past that close, and the venue closing it makes it
    /// closed however far off the listed close is.
    #[test]
    fn the_venues_word_on_accepting_orders_outranks_the_listed_close() {
        let now = Utc::now();
        assert_eq!(market_state(Some(now - chrono::Duration::hours(1)), Some(true), now), ("live", Some(0)));
        assert_eq!(market_state(Some(now + chrono::Duration::hours(1)), Some(false), now), ("closed", Some(0)));
        assert_eq!(market_state(None, Some(true), now), ("live", None));
    }

    /// The venue's silence and our failure are different facts; only the first
    /// becomes "not published".
    #[test]
    fn published_none_serializes_as_not_published() {
        let v = published::<Vec<u8>>(None);
        assert_eq!(v["published"], serde_json::json!(false));
        assert_eq!(v["reason"], serde_json::json!(NOT_PUBLISHED));
        let v = published(Some(vec![1u8]));
        assert_eq!(v["published"], serde_json::json!(true));
        assert_eq!(v["data"], serde_json::json!([1]));
    }

    /// A touch market is not a terminal digital; the formula would say half
    /// the truth with full confidence.
    #[test]
    fn fairvalue_reads_up_or_down_and_above_below_but_not_touch_markets() {
        assert!(fairvalue_prices_this("bitcoin up or down - october 8, 12pm et", false));
        assert!(fairvalue_prices_this("will bitcoin be above $85,000 on october 10?", true));
        assert!(!fairvalue_prices_this("will bitcoin reach $150,000 by december 31?", true));
        assert!(!fairvalue_prices_this("will bitcoin dip to $70,000 in october?", true));
        assert!(!fairvalue_prices_this("who will win the 2026 world series?", false));
    }

    #[test]
    fn the_expiry_window_falls_back_to_the_class_default() {
        assert_eq!(expiry_window_secs(Some("4h"), 99), 14400);
        assert_eq!(expiry_window_secs(Some("nonsense"), 99), 99);
        assert_eq!(expiry_window_secs(None, 99), 99);
    }
}
