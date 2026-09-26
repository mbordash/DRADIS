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

//! Sports line ledger: Phase 0 of the sports spike (`docs/spikes/sports-viper-spike.md`).
//!
//! Records the sportsbook consensus against Polymarket's own prices for every
//! Polymarket International sports moneyline it can match to a Odds API event,
//! plus each market's eventual resolution. It places no orders and feeds no
//! viper. Its only job is to build the history the spike's go/no-go statistics
//! need (book lead/lag, paper maker P&L, idleness) before anything trades.
//!
//! ── Budget ──────────────────────────────────────────────────────────────────
//! Built for The Odds API free tier (500 credits a month). Only one call costs
//! anything: `/sports/{sport}/odds`, one credit per region, and a single call
//! returns every game of that sport. Discovery (`/events`) and every Polymarket
//! read are free, and `/events` still reports the remaining quota in its
//! headers, so the ledger always knows its budget without spending on it.
//!
//! A sport is snapshotted when any matched game reaches one of the configured
//! offsets from its start (default two hours and ten minutes before). Spend is
//! spread evenly over the days left in the quota cycle and never dips into the
//! configured reserve. On 2026-09-09's MLB probe the sportsbook consensus did
//! not move a full point in 71 pre-game game-hours of five-minute polls, which
//! is why a handful of pre-game snapshots per game loses little.
//!
//! This IS the Sports Raptor as of [E63]. The polling raptor it replaced picked
//! one nearest-commencing event across all sports and published that same global
//! snapshot to every squadron, which no viper could use and nothing recorded; it
//! was deleted rather than left spending the same Odds API quota.
//!
//! Scope: discovery and prices are Polymarket International (Gamma + CLOB), so
//! the board is keyed by Gamma token ids and `StrategyContext.sports` is `None`
//! on Kalshi and Polymarket US. Extending the recorder to those venues is open
//! work; until then this telemetry is honest but venue-specific.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDate, TimeZone, Utc};

use rust_decimal::Decimal;
use serde_json::Value;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::config;
use crate::helpers::db;
use crate::helpers::dynamic_config::DynamicConfig;

/// Fixed health-map key the sports signal publishes its telemetry under,
/// alongside the per-asset crypto entries ("btc"/"eth"/…). Venue-neutral: one
/// entry, not one per asset.
///
/// Moved here from the retired polling Sports Raptor ([E63]): the ledger is the
/// Sports Raptor now, and this is the key operators already know it by.
pub const SPORTS_HEALTH_KEY: &str = "sports";

/// Blank out any `apiKey=...` value in a string destined for a log.
///
/// The Odds API takes its credential in the query string, so every URL this
/// module builds carries the operator's key. Anything that renders such a URL —
/// most notably `reqwest::Error`'s `Display`, which names the request it failed
/// on — would otherwise write that key to disk in plain text.
///
/// Deliberately operates on the rendered STRING rather than the URL, because the
/// leak arrives inside an error message with surrounding prose, not as a bare
/// URL that could be parsed and rebuilt.
pub fn redact_url_secrets(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find("apiKey=") {
        let (head, tail) = rest.split_at(i + "apiKey=".len());
        out.push_str(head);
        out.push_str("<redacted>");
        // The value runs to the next query separator or whitespace; whatever
        // follows (`&markets=...`, a closing paren, trailing prose) is kept.
        let end = tail
            .find(|c: char| c == '&' || c == '#' || c.is_whitespace() || c == ')')
            .unwrap_or(tail.len());
        rest = &tail[end..];
    }
    out.push_str(rest);
    out
}

const GAMMA: &str = "https://gamma-api.polymarket.com";
const ODDS: &str = "https://api.the-odds-api.com/v4";
const CLOB_BOOK: &str = "https://clob.polymarket.com/book";

/// Scheduler cadence. Every Odds API spend is gated separately, so this only
/// decides how promptly a due snapshot is taken.
const TICK_SECS: u64 = 60;
/// How often the free discovery pass (Polymarket games, Odds API events) runs.
const CATALOG_REFRESH_SECS: i64 = 30 * 60;
/// How often closed markets are checked for their resolution.
const RESULTS_REFRESH_SECS: i64 = 30 * 60;
/// Largest start-time difference at which a Polymarket game and an Odds API
/// event can be the same game. Sampled 2026-09-11: identical for Ligue 1, one
/// minute apart for MLB.
const MATCH_TOLERANCE_SECS: i64 = 15 * 60;
/// A snapshot target stays due for this long, so a slow tick or a restart
/// does not skip it.
const SNAPSHOT_WINDOW_SECS: i64 = 20 * 60;
/// A snapshot taken this long before a target already covers it. Pre-game lines
/// barely move (not a full point in 71 game-hours on the 2026-09-09 probe), so
/// a second paid call minutes after the first buys nothing. Must stay well
/// under the gap between configured offsets.
const SNAPSHOT_COALESCE_SECS: i64 = 30 * 60;
/// Games are tracked from this long before their start...
const LOOKAHEAD_SECS: i64 = 36 * 3600;
/// ...until this long after it, so in-play snapshots still cover them.
const LOOKBACK_SECS: i64 = 6 * 3600;
/// A market is checked for its resolution once its game started this long ago...
const RESULT_AFTER_START_SECS: i64 = 3 * 3600;
/// ...and given up on once it started this long ago, so a market Gamma never
/// settles cannot occupy the per-pass budget forever.
const RESULT_GIVE_UP_SECS: i64 = 14 * 86_400;
/// A board line older than this is not the current state of its game, so the
/// telemetry panel reports the feed as not live.
const BOARD_FRESH_SECS: i64 = 30 * 60;
/// A line is kept on the board until this long after kick-off: long enough to cover
/// a game and its overtime, short enough that the board is the current slate.
const BOARD_RETAIN_SECS: i64 = 6 * 3600;
/// Most markets checked for a resolution per pass.
const RESULTS_PER_PASS: usize = 40;
/// Two team names are the same team when this share of the shorter name's
/// distinctive tokens match.
const MIN_TEAM_SIMILARITY: f64 = 0.5;
/// Two tokens match when they share a prefix at least this long:
/// "Stade Rennais" (Polymarket) is "Rennes" (The Odds API).
const TOKEN_PREFIX_MATCH: usize = 4;
const HTTP_TIMEOUT_SECS: u64 = 15;
/// Gamma's page size for `/events`, and how many pages a league may take.
const GAMMA_PAGE: usize = 100;
const GAMMA_MAX_PAGES: usize = 5;

/// Club-name filler that says nothing about which team it is.
const NAME_NOISE: &[&str] = &[
    "fc", "afc", "cf", "sc", "ac", "as", "rc", "cd", "ud", "sd", "ssc", "sv", "vfb", "vfl", "tsg",
    "club", "de", "del", "la", "le", "les", "the", "of", "and", "calcio", "alsace",
];

// ── Parsing ──────────────────────────────────────────────────────────────────

/// `"mlb=baseball_mlb, fl1=soccer_france_ligue_one"` → `[("mlb", "baseball_mlb"), ...]`.
/// The left side is Polymarket's league code from Gamma `/sports`, the right
/// side The Odds API sport key. Malformed entries are dropped.
pub fn parse_leagues(s: &str) -> Vec<(String, String)> {
    s.split(',')
        .filter_map(|pair| {
            let (code, key) = pair.split_once('=')?;
            let (code, key) = (code.trim().to_ascii_lowercase(), key.trim().to_string());
            (!code.is_empty() && !key.is_empty()).then_some((code, key))
        })
        .collect()
}

/// `"-120,-10"` → `[-120, -10]` minutes relative to the game's start.
pub fn parse_offsets_mins(s: &str) -> Vec<i64> {
    s.split(',').filter_map(|v| v.trim().parse::<i64>().ok()).collect()
}

fn json_string_list(v: Option<&Value>) -> Vec<String> {
    match v {
        // Gamma serializes these as JSON-encoded strings, not arrays.
        Some(Value::String(s)) => serde_json::from_str::<Vec<String>>(s).unwrap_or_default(),
        Some(Value::Array(a)) => a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect(),
        _ => Vec::new(),
    }
}

fn parse_time(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|t| t.with_timezone(&Utc))
        .ok()
        // Gamma's `gameStartTime` form: "2026-09-11 18:45:00+00".
        .or_else(|| DateTime::parse_from_str(&format!("{s}00"), "%Y-%m-%d %H:%M:%S%z").ok().map(|t| t.with_timezone(&Utc)))
}

/// Polymarket International's catalog: Gamma for the games and the resolutions,
/// the CLOB for the touch.
///
/// This is the implementation the ledger has always had, behind the seam rather
/// than inlined. It stays in this module rather than moving to `src/venues/intl/`
/// only because the Gamma paging, the league-series lookup and the matcher are
/// deeply interleaved here; the other venues' adapters have no such history and
/// belong under `src/venues/`.
pub struct IntlSportsCatalog;

#[async_trait::async_trait]
impl SportsCatalog for IntlSportsCatalog {
    async fn games(
        &self,
        http: &reqwest::Client,
        api_key: &str,
        leagues: &[(String, String)],
        now: DateTime<Utc>,
    ) -> (Vec<MatchedGame>, Option<i64>) {
        refresh_catalog(http, api_key, leagues, now).await
    }

    async fn best_levels(
        &self,
        http: &reqwest::Client,
        leg_id: &str,
    ) -> (Option<f64>, Option<f64>, Option<f64>, Option<f64>) {
        match get_json(http, CLOB_BOOK, None, &[("token_id", leg_id)]).await {
            Ok((book, _, _)) => best_levels(&book),
            Err(_) => (None, None, None, None),
        }
    }

    async fn settled_prices(
        &self,
        http: &reqwest::Client,
        market_key: &str,
        legs: &[String],
    ) -> HashMap<String, f64> {
        settled_prices_for_market(http, market_key, legs).await
    }
}

/// The three things the ledger needs from a venue, and the only things about it
/// that are venue-specific.
///
/// Everything else in this module — the Odds API polling, the de-vig, the game
/// matching, the credit budget, the board fold, the telemetry — is already
/// venue-neutral and stays that way. What differs per venue is which markets exist
/// (Polymarket International reads Gamma, Kalshi reads its game series, Polymarket
/// US reads its signed catalog), where the touch comes from, and who answers the
/// question of how a game resolved.
///
/// One board, keyed by the venue's own leg identifier, and one implementation live
/// per process: the engine builds a separate binary per venue feature, so a running
/// ledger only ever holds one venue's identifiers. That is why there is no venue
/// field on `SportsLine` and no partitioning of the board — the ambiguity a
/// multi-venue board would create cannot arise.
///
/// Implementations belong under `src/venues/`, which is where the architecture puts
/// everything that knows a venue's shape.
#[async_trait::async_trait]
pub trait SportsCatalog: Send + Sync {
    /// The venue's game markets for these leagues, matched to bookmaker events.
    /// The second element is the Odds API credits remaining, when the call reports it.
    async fn games(
        &self,
        http: &reqwest::Client,
        api_key: &str,
        leagues: &[(String, String)],
        now: DateTime<Utc>,
    ) -> (Vec<MatchedGame>, Option<i64>);

    /// Best bid, bid size, ask, ask size for one leg, as the venue reports them.
    async fn best_levels(
        &self,
        http: &reqwest::Client,
        leg_id: &str,
    ) -> (Option<f64>, Option<f64>, Option<f64>, Option<f64>);

    /// How the venue says these legs resolved, for legs it has settled.
    async fn settled_prices(
        &self,
        http: &reqwest::Client,
        market_key: &str,
        legs: &[String],
    ) -> HashMap<String, f64>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct PmOutcome {
    /// Team name, or "Draw".
    pub label: String,
    pub condition_id: String,
    /// The token that pays out if this outcome happens.
    pub token_id: String,
}

#[derive(Debug, Clone)]
pub struct PmGame {
    pub league: String,
    pub slug: String,
    pub title: String,
    pub start: DateTime<Utc>,
    pub outcomes: Vec<PmOutcome>,
}

/// Polymarket games with a moneyline in a Gamma `/events` response, starting
/// within the tracked window.
///
/// Two shapes are handled, both sampled from Gamma on 2026-09-11:
/// - one market whose two outcomes are the teams (MLB: "Pittsburgh Pirates" /
///   "Chicago Cubs"), each with its own token;
/// - one Yes/No market per outcome (soccer: "Will Stade Rennais FC 1901 win",
///   "... end in a draw?", "Will Olympique de Marseille win"), where the Yes
///   token pays on that outcome and `groupItemTitle` names it.
///
/// Games that do not resolve to exactly two named teams are skipped.
pub fn parse_pm_games(events: &Value, league: &str, now: DateTime<Utc>) -> Vec<PmGame> {
    let mut out = Vec::new();
    for ev in events.as_array().map(Vec::as_slice).unwrap_or(&[]) {
        let Some(start) = ev.get("startTime").and_then(Value::as_str).and_then(parse_time) else { continue };
        if start < now - ChronoDuration::seconds(LOOKBACK_SECS) || start > now + ChronoDuration::seconds(LOOKAHEAD_SECS) {
            continue;
        }
        let mut outcomes = Vec::new();
        for m in ev.get("markets").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]) {
            if m.get("sportsMarketType").and_then(Value::as_str) != Some("moneyline") { continue; }
            if m.get("closed").and_then(Value::as_bool) == Some(true) { continue; }
            let Some(cid) = m.get("conditionId").and_then(Value::as_str) else { continue };
            let names = json_string_list(m.get("outcomes"));
            let tokens = json_string_list(m.get("clobTokenIds"));
            if names.len() != 2 || tokens.len() != 2 { continue; }
            if names[0].eq_ignore_ascii_case("yes") {
                let title = m.get("groupItemTitle").and_then(Value::as_str).unwrap_or("").trim();
                let label = if title.to_ascii_lowercase().starts_with("draw") { "Draw".to_string() } else { title.to_string() };
                if label.is_empty() { continue; }
                outcomes.push(PmOutcome { label, condition_id: cid.to_string(), token_id: tokens[0].clone() });
            } else {
                for i in 0..2 {
                    outcomes.push(PmOutcome { label: names[i].clone(), condition_id: cid.to_string(), token_id: tokens[i].clone() });
                }
            }
        }
        if outcomes.iter().filter(|o| o.label != "Draw").count() != 2 { continue; }
        out.push(PmGame {
            league: league.to_string(),
            slug: ev.get("slug").and_then(Value::as_str).unwrap_or("").to_string(),
            title: ev.get("title").and_then(Value::as_str).unwrap_or("").to_string(),
            start,
            outcomes,
        });
    }
    out
}

#[derive(Debug, Clone)]
pub struct OddsEvent {
    pub id: String,
    pub home: String,
    pub away: String,
    pub commence: DateTime<Utc>,
}

pub fn parse_odds_events(events: &Value) -> Vec<OddsEvent> {
    events.as_array().map(Vec::as_slice).unwrap_or(&[]).iter().filter_map(|e| {
        Some(OddsEvent {
            id: e.get("id")?.as_str()?.to_string(),
            home: e.get("home_team")?.as_str()?.to_string(),
            away: e.get("away_team")?.as_str()?.to_string(),
            commence: parse_time(e.get("commence_time")?.as_str()?)?,
        })
    }).collect()
}

// ── Matching ─────────────────────────────────────────────────────────────────

/// The distinctive, lower-cased tokens of a team name.
pub fn name_tokens(name: &str) -> HashSet<String> {
    name.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .filter(|t| t.len() > 1 && !t.chars().all(|c| c.is_ascii_digit()) && !NAME_NOISE.contains(t))
        .map(str::to_string)
        .collect()
}

fn tokens_match(a: &str, b: &str) -> bool {
    if a == b { return true; }
    let common = a.chars().zip(b.chars()).take_while(|(x, y)| x == y).count();
    common >= TOKEN_PREFIX_MATCH
}

/// Share of the shorter name's distinctive tokens that match the other name, in [0, 1].
pub fn name_similarity(a: &str, b: &str) -> f64 {
    let (ta, tb) = (name_tokens(a), name_tokens(b));
    if ta.is_empty() || tb.is_empty() { return 0.0; }
    let (short, long) = if ta.len() <= tb.len() { (&ta, &tb) } else { (&tb, &ta) };
    let hits = short.iter().filter(|s| long.iter().any(|l| tokens_match(s, l))).count();
    hits as f64 / short.len() as f64
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchedOutcome {
    pub pm: PmOutcome,
    /// The outcome's name in The Odds API's h2h market.
    pub odds_name: String,
}

#[derive(Debug, Clone)]
pub struct MatchedGame {
    pub league: String,
    pub sport_key: String,
    pub odds_event_id: String,
    pub pm_slug: String,
    pub commence: DateTime<Utc>,
    pub outcomes: Vec<MatchedOutcome>,
}

/// One outcome token's line: the bookmakers' consensus for the outcome that token
/// pays on, with the confidence and freshness a consumer needs to judge it ([E63]).
///
/// Keyed by the venue's own token id, so a squadron looks up `market.yes_token` and
/// `market.no_token` directly and never matches team names in the patrol. The
/// ledger already resolved the game and the polarity when it matched the market.
#[derive(Debug, Clone, PartialEq)]
pub struct SportsLine {
    pub league: String,
    pub sport_key: String,
    pub odds_event_id: String,
    /// Kick-off, the only start time in the system: a sports market's close time is
    /// a week after the game on MLB and kick-off itself on football and soccer.
    pub commence: DateTime<Utc>,
    /// The outcome this token pays on ("Chelsea FC", "Draw"), as the venue names it.
    pub outcome_label: String,
    /// Vig-free consensus probability across the books that quoted this outcome.
    pub consensus: f64,
    pub num_books: i64,
    /// Highest minus lowest book probability: a wide line is a soft line.
    pub dispersion: Option<f64>,
    /// Age of the oldest book quote behind `consensus`, at `odds_at`.
    pub max_book_age_secs: Option<i64>,
    /// When the odds were read. A consumer judges staleness from this, not from
    /// when it happened to look at the board.
    pub odds_at: DateTime<Utc>,
    /// Change in `consensus` since this token's previous line, when there was one:
    /// the line movement that [E32] wanted, per outcome rather than per feed.
    pub drift: Option<f64>,
    /// Seconds the `drift` above spans, so a consumer can turn it into a velocity.
    ///
    /// A bare difference between two readings is not a rate: the same 4-point
    /// move means something very different over two minutes than over an hour,
    /// and the interval between readings is set by the poll cadence rather than
    /// by the market. Without this, any knob scaled on `drift` silently means
    /// something different on every instance and whenever the cadence changes.
    pub drift_secs: Option<i64>,
}

impl SportsLine {
    /// How old the consensus is now, in seconds.
    pub fn age_secs(&self, now: DateTime<Utc>) -> i64 { (now - self.odds_at).num_seconds().max(0) }
    /// Seconds until kick-off; negative once the game is under way.
    pub fn secs_to_start(&self, now: DateTime<Utc>) -> i64 { (self.commence - now).num_seconds() }
}

/// Both sides of one market, already mapped to the venue's YES and NO tokens.
#[derive(Debug, Clone, PartialEq)]
pub struct SportsMarketLine {
    pub yes: Option<SportsLine>,
    pub no: Option<SportsLine>,
}

impl SportsMarketLine {
    /// The line for one side (0 = YES, 1 = NO).
    pub fn side(&self, side: usize) -> Option<&SportsLine> {
        if side == 0 { self.yes.as_ref() } else { self.no.as_ref() }
    }
    /// Kick-off, from whichever side the board holds.
    pub fn commence(&self) -> Option<DateTime<Utc>> {
        self.yes.as_ref().or(self.no.as_ref()).map(|l| l.commence)
    }
}

/// Token id -> its line. Published by the ledger after every odds snapshot.
pub type SportsBoard = HashMap<String, SportsLine>;

fn board_tx() -> &'static watch::Sender<Arc<SportsBoard>> {
    static TX: std::sync::OnceLock<watch::Sender<Arc<SportsBoard>>> = std::sync::OnceLock::new();
    TX.get_or_init(|| watch::channel(Arc::new(SportsBoard::new())).0)
}

/// The current board. Cheap: an `Arc` clone of the last published map.
pub fn board() -> Arc<SportsBoard> { board_tx().borrow().clone() }

/// The lines for one market's two tokens, or `None` when the board knows neither
/// (any non-sports market, and a sports market the ledger has not matched).
pub fn line_for(yes_token: &str, no_token: &str) -> Option<SportsMarketLine> {
    let b = board();
    let (yes, no) = (b.get(yes_token).cloned(), b.get(no_token).cloned());
    (yes.is_some() || no.is_some()).then_some(SportsMarketLine { yes, no })
}

/// Fold one snapshot's rows into the board and publish it.
///
/// Lines for games that kicked off more than `BOARD_RETAIN_SECS` ago are dropped, so
/// the board stays the size of the current slate rather than the season.
pub fn publish_rows(rows: &[db::SportsLedgerRow], now: DateTime<Utc>) {
    // `send_replace`, not `send`: the board has no long-lived receiver (readers call
    // `board()`), and `send` fails when every receiver has been dropped, which would
    // leave the board permanently empty.
    board_tx().send_replace(Arc::new(fold_rows(&board(), rows, now)));
}

/// `prev` with this snapshot's lines folded in and finished games dropped.
pub fn fold_rows(prev: &SportsBoard, rows: &[db::SportsLedgerRow], now: DateTime<Utc>) -> SportsBoard {
    let mut next: SportsBoard = prev.clone();
    next.retain(|_, l| (now - l.commence).num_seconds() < BOARD_RETAIN_SECS);
    for r in rows {
        // The real odds-arrival time when the row has one, falling back to the
        // pass timestamp for rows written before [E63] separated them. A pass
        // runs for minutes, so on later sports `ts` understates the age of the
        // board's readings and orders drift by when we started rather than by
        // when the books actually moved.
        let odds_at = r.odds_at.as_deref().and_then(parse_time).or_else(|| parse_time(&r.ts));
        let (Some(commence), Some(odds_at)) = (parse_time(&r.commence), odds_at) else { continue };
        // Every snapshot covers every matched game of its sport, so a row with no
        // consensus is the books saying they quote nothing on this outcome right
        // now (11% of rows, 25% on college football, typically suspended in play).
        // Dropping the line says that; keeping the old one would serve a pre-game
        // number as an in-play reading.
        let Some(consensus) = r.consensus else {
            next.remove(&r.token_id);
            continue;
        };
        let prev_line = prev.get(&r.token_id).filter(|p| p.odds_at < odds_at);
        let drift = prev_line.map(|p| consensus - p.consensus);
        let drift_secs = prev_line.map(|p| (odds_at - p.odds_at).num_seconds().max(0));
        next.insert(r.token_id.clone(), SportsLine {
            league: r.league.clone(),
            sport_key: r.sport_key.clone(),
            odds_event_id: r.odds_event_id.clone(),
            commence,
            outcome_label: r.outcome_label.clone(),
            consensus,
            num_books: r.num_books,
            dispersion: r.dispersion,
            max_book_age_secs: r.max_book_age_secs,
            odds_at,
            drift,
            drift_secs,
        });
    }
    next
}

/// Telemetry for the sports feed, from the board rather than from a tracked event.
///
/// The Control Tower's sports panel was fed by the Sports Raptor's nearest-event
/// singleton, which is paused while the ledger owns the Odds API budget, so the
/// panel read "disconnected" while the ledger was in fact reading odds every few
/// minutes. These fields are a display summary of the board, and the game shown is
/// simply the next one to kick off. They are not a trading signal: a consumer reads
/// its own market's line from `line_for`, with that line's own freshness.
pub fn report_board_health(
    tx: &watch::Sender<HashMap<String, crate::api::server::AssetRaptorHealth>>,
    now: DateTime<Utc>,
    enabled: bool,
    has_key: bool,
) {
    let board = board();
    // Liveness is about the FEED, not about one game: the newest odds on the board.
    // A feed that stopped answering publishes nothing, so this ages and goes false.
    let freshest = board.values().map(|l| l.odds_at).max();
    let live = freshest.is_some_and(|t| (now - t).num_seconds() <= BOARD_FRESH_SECS);
    // The game shown is the next to kick off, tie-broken by label so the panel does
    // not flip between the two sides of the same game from one publish to the next.
    let next = board.values()
        .min_by(|a, b| (a.commence < now, (a.commence - now).num_seconds().abs(), &a.outcome_label)
            .cmp(&(b.commence < now, (b.commence - now).num_seconds().abs(), &b.outcome_label)))
        .cloned();
    let games: HashSet<&str> = board.values().map(|l| l.odds_event_id.as_str()).collect();
    let n_games = games.len();
    tx.send_modify(|map| {
        let h = map.entry(SPORTS_HEALTH_KEY.to_string()).or_default();
        h.sports_connected = live;
        h.sports_enabled = enabled;
        h.sports_has_key = has_key;
        match &next {
            Some(l) => {
                h.sports_line_drift = l.drift.and_then(Decimal::from_f64_retain).unwrap_or_default().round_dp(6);
                h.sports_consensus_prob = Decimal::from_f64_retain(l.consensus).unwrap_or_default().round_dp(6);
                h.sports_book_dispersion = l.dispersion.and_then(Decimal::from_f64_retain).unwrap_or_default().round_dp(6);
                h.sports_num_books = Decimal::from(l.num_books);
                h.sports_event = format!("{} ({} game(s) on the board)", l.outcome_label, n_games);
                h.sports_reference = l.outcome_label.clone();
                h.sports_sport = l.league.to_uppercase();
                h.sports_commence = l.commence.to_rfc3339();
                h.sports_books = format!("{} book(s), oldest quote {}s", l.num_books, l.max_book_age_secs.unwrap_or(-1));
            }
            None => {
                h.sports_consensus_prob = Decimal::ZERO;
                h.sports_line_drift = Decimal::ZERO;
                h.sports_book_dispersion = Decimal::ZERO;
                h.sports_num_books = Decimal::ZERO;
                h.sports_event = String::new();
                h.sports_reference = String::new();
                h.sports_sport = String::new();
                h.sports_commence = String::new();
                h.sports_books = String::new();
            }
        }
    });
}

/// Pair Polymarket games with Odds API events: same league, start times within
/// `MATCH_TOLERANCE_SECS`, and both teams recognizably the same, in either
/// home/away order. Each Odds API event is used at most once, best score first.
pub fn match_games(pm: &[PmGame], odds: &[OddsEvent], sport_key: &str) -> Vec<MatchedGame> {
    let mut candidates: Vec<(f64, usize, usize, bool)> = Vec::new();
    for (pi, g) in pm.iter().enumerate() {
        let teams: Vec<&PmOutcome> = g.outcomes.iter().filter(|o| o.label != "Draw").collect();
        if teams.len() != 2 { continue; }
        for (oi, e) in odds.iter().enumerate() {
            if (e.commence - g.start).num_seconds().abs() > MATCH_TOLERANCE_SECS { continue; }
            let straight = (name_similarity(&teams[0].label, &e.home), name_similarity(&teams[1].label, &e.away));
            let swapped = (name_similarity(&teams[0].label, &e.away), name_similarity(&teams[1].label, &e.home));
            for (pair, is_swapped) in [(straight, false), (swapped, true)] {
                if pair.0 >= MIN_TEAM_SIMILARITY && pair.1 >= MIN_TEAM_SIMILARITY {
                    candidates.push((pair.0 + pair.1, pi, oi, is_swapped));
                }
            }
        }
    }
    candidates.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let (mut used_pm, mut used_odds) = (HashSet::new(), HashSet::new());
    let mut out = Vec::new();
    for (_, pi, oi, swapped) in candidates {
        if used_pm.contains(&pi) || used_odds.contains(&oi) { continue; }
        used_pm.insert(pi);
        used_odds.insert(oi);
        let (g, e) = (&pm[pi], &odds[oi]);
        let mut team_idx = 0;
        let outcomes = g.outcomes.iter().map(|o| {
            let odds_name = if o.label == "Draw" {
                "Draw".to_string()
            } else {
                let first = team_idx == 0;
                team_idx += 1;
                match (first, swapped) {
                    (true, false) | (false, true) => e.home.clone(),
                    _ => e.away.clone(),
                }
            };
            MatchedOutcome { pm: o.clone(), odds_name }
        }).collect();
        out.push(MatchedGame {
            league: g.league.clone(),
            sport_key: sport_key.to_string(),
            odds_event_id: e.id.clone(),
            pm_slug: g.slug.clone(),
            commence: g.start,
            outcomes,
        });
    }
    out
}

// ── Scheduling and budget ────────────────────────────────────────────────────

/// Sport keys with a snapshot due now: some matched game has reached one of the
/// offsets from its start within the last `SNAPSHOT_WINDOW_SECS`, and that
/// sport has not been snapshotted since `SNAPSHOT_COALESCE_SECS` before the
/// target. One call covers every game of a sport, so a due sport is returned
/// once, and games starting close together share a snapshot.
pub fn due_sport_keys(
    games: &[MatchedGame],
    offsets_mins: &[i64],
    last_snapshot: &HashMap<String, DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Vec<String> {
    let mut due: Vec<String> = Vec::new();
    for g in games {
        for off in offsets_mins {
            let target = g.commence + ChronoDuration::minutes(*off);
            let open = target <= now && now < target + ChronoDuration::seconds(SNAPSHOT_WINDOW_SECS);
            let taken = last_snapshot.get(&g.sport_key)
                .is_some_and(|t| *t >= target - ChronoDuration::seconds(SNAPSHOT_COALESCE_SECS));
            if open && !taken && !due.contains(&g.sport_key) {
                due.push(g.sport_key.clone());
            }
        }
    }
    due.sort();
    due
}

/// Whole days until the quota next resets on `reset_day` of the month
/// (00:00 UTC), counting today. Never less than 1. Days past the 28th are
/// clamped so every month has one.
pub fn days_until_reset(now: DateTime<Utc>, reset_day: u32) -> i64 {
    let day = reset_day.clamp(1, 28);
    let this_month = Utc.with_ymd_and_hms(now.year(), now.month(), day, 0, 0, 0).single();
    let next = match this_month {
        Some(t) if t > now => t,
        _ => {
            let (y, m) = if now.month() == 12 { (now.year() + 1, 1) } else { (now.year(), now.month() + 1) };
            Utc.with_ymd_and_hms(y, m, day, 0, 0, 0).single().unwrap_or(now + ChronoDuration::days(30))
        }
    };
    let secs = (next - now).num_seconds().max(0);
    ((secs + 86_399) / 86_400).max(1)
}

/// Credits that may be spent today: what was left at the start of the day,
/// minus the reserve, spread over the days left in the cycle.
pub fn allowance_today(day_start_remaining: i64, reserve: i64, days_left: i64) -> i64 {
    (day_start_remaining - reserve).max(0) / days_left.max(1)
}

/// May a call costing `cost` credits be made?
pub fn may_spend(remaining: i64, reserve: i64, spent_today: i64, allowance: i64, cost: i64) -> bool {
    remaining - cost >= reserve && spent_today + cost <= allowance
}

/// One bookmaker's raw h2h quote for `odds_name`: its decimal odds, the implied
/// probability before the vig is removed, and that book's overround (the sum of
/// its raw implied probabilities across the market's outcomes).
///
/// The vig-free probability is `raw_implied / overround`, which is the
/// proportional de-vig the consensus has always used. Returning the parts rather
/// than only the quotient is what lets a different method be applied later to
/// rows already recorded.
pub fn book_quote_for(book: &Value, odds_name: &str) -> Option<(f64, f64, f64)> {
    let outcomes = book.get("markets").and_then(Value::as_array)?
        .iter().find(|m| m.get("key").and_then(Value::as_str) == Some("h2h"))?
        .get("outcomes").and_then(Value::as_array)?;
    let mut raw: Vec<(&str, f64, f64)> = Vec::new();
    for o in outcomes {
        let name = o.get("name").and_then(Value::as_str)?;
        let odds = o.get("price").and_then(Value::as_f64).filter(|p| *p > 1.0)?;
        raw.push((name, odds, 1.0 / odds));
    }
    // A one-sided market has no vig to remove and no meaningful overround.
    if raw.len() < 2 { return None; }
    let overround: f64 = raw.iter().map(|(_, _, p)| *p).sum();
    if overround <= 0.0 { return None; }
    raw.iter().find(|(name, _, _)| *name == odds_name)
        .map(|(_, odds, implied)| (*odds, *implied, overround))
}

/// What one snapshot knows about one outcome's book consensus.
#[derive(Debug, Clone, Default)]
pub struct BookConsensus {
    /// Vig-free consensus probability (proportional de-vig, averaged over books).
    pub consensus: Option<f64>,
    pub num_books: i64,
    pub dispersion: Option<f64>,
    pub max_book_age_secs: Option<i64>,
    /// Mean per-book overround, and the mean raw implied probability of this
    /// outcome before that overround is divided out.
    pub overround: Option<f64>,
    pub raw_consensus: Option<f64>,
    /// Every contributing book's raw quote: (book_key, decimal_odds,
    /// raw_implied, overround, last_update).
    pub quotes: Vec<(String, f64, f64, f64, Option<String>)>,
    /// Books that contributed to the consensus but carry no identifier. Their
    /// raw quotes are NOT recorded: the per-book table is keyed by book, so
    /// storing them all under one placeholder would silently keep just the
    /// first and look like a complete set.
    pub unkeyed_books: i64,
}

/// Vig-free consensus for one outcome across books, with book count,
/// max-minus-min dispersion, the oldest book quote's age in seconds, and the
/// raw per-book quotes the consensus was reduced from.
///
/// `now` should be when the odds response returned, not when the pass began:
/// the quote ages are about the feed, and a pass takes minutes.
pub fn consensus_for(books: Option<&Vec<Value>>, odds_name: &str, now: DateTime<Utc>) -> BookConsensus {
    let mut out = BookConsensus::default();
    let mut probs: Vec<f64> = Vec::new();
    let mut raws: Vec<f64> = Vec::new();
    let mut overrounds: Vec<f64> = Vec::new();
    for book in books.map(Vec::as_slice).unwrap_or(&[]) {
        let Some((odds, raw_implied, overround)) = book_quote_for(book, odds_name) else { continue };
        probs.push(raw_implied / overround);
        raws.push(raw_implied);
        overrounds.push(overround);
        let last_update = book.get("last_update").and_then(Value::as_str).map(str::to_string);
        if let Some(t) = last_update.as_deref().and_then(parse_time) {
            let age = (now - t).num_seconds().max(0);
            out.max_book_age_secs = Some(out.max_book_age_secs.map_or(age, |o| o.max(age)));
        }
        match book.get("key").and_then(Value::as_str)
            .or_else(|| book.get("title").and_then(Value::as_str))
        {
            Some(key) => out.quotes.push((key.to_string(), odds, raw_implied, overround, last_update)),
            None => out.unkeyed_books += 1,
        }
    }
    if probs.is_empty() { return out; }
    let n = probs.len();
    out.consensus = Some(probs.iter().sum::<f64>() / n as f64);
    out.raw_consensus = Some(raws.iter().sum::<f64>() / n as f64);
    out.overround = Some(overrounds.iter().sum::<f64>() / n as f64);
    out.dispersion = Some(
        probs.iter().cloned().fold(f64::MIN, f64::max) - probs.iter().cloned().fold(f64::MAX, f64::min)
    );
    out.num_books = n as i64;
    out
}

/// Best bid and ask, with size at each, from a CLOB `/book` response.
pub fn best_levels(book: &Value) -> (Option<f64>, Option<f64>, Option<f64>, Option<f64>) {
    let levels = |side: &str| -> Vec<(f64, f64)> {
        book.get(side).and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]).iter().filter_map(|l| {
            let p = l.get("price")?.as_str()?.parse::<f64>().ok()?;
            let s = l.get("size")?.as_str()?.parse::<f64>().ok()?;
            (s > 0.0).then_some((p, s))
        }).collect()
    };
    let bid = levels("bids").into_iter().fold(None, |acc: Option<(f64, f64)>, l| match acc { Some(a) if a.0 >= l.0 => Some(a), _ => Some(l) });
    let ask = levels("asks").into_iter().fold(None, |acc: Option<(f64, f64)>, l| match acc { Some(a) if a.0 <= l.0 => Some(a), _ => Some(l) });
    (bid.map(|b| b.0), bid.map(|b| b.1), ask.map(|a| a.0), ask.map(|a| a.1))
}

// ── Network ──────────────────────────────────────────────────────────────────

async fn get_json(http: &reqwest::Client, url: &str, api_key: Option<&str>, extra: &[(&str, &str)]) -> Result<(Value, Option<i64>, Option<i64>), String> {
    let mut req = http.get(url).header("User-Agent", "dradis-sports-ledger");
    if let Some(k) = api_key { req = req.query(&[("apiKey", k)]); }
    if !extra.is_empty() { req = req.query(extra); }
    let resp = tokio::time::timeout(std::time::Duration::from_secs(HTTP_TIMEOUT_SECS), req.send())
        .await
        .map_err(|_| format!("timed out after {HTTP_TIMEOUT_SECS}s"))?
        .map_err(|e| redact_url_secrets(&e.to_string()))?;
    let header = |name: &str| resp.headers().get(name).and_then(|v| v.to_str().ok()).and_then(|s| s.trim().parse::<f64>().ok()).map(|f| f as i64);
    let (remaining, last) = (header("x-requests-remaining"), header("x-requests-last"));
    let status = resp.status();
    let body = resp.text().await.map_err(|e| redact_url_secrets(&e.to_string()))?;
    if !status.is_success() {
        return Err(format!("HTTP {status}: {}", body.chars().take(200).collect::<String>()));
    }
    let json = serde_json::from_str(&body).map_err(|e| format!("invalid JSON: {e}"))?;
    Ok((json, remaining, last))
}

/// The Odds API splits a league's preseason into its own sport key, and carries
/// no preseason games under the regular-season key at all. Polymarket does not:
/// it prices preseason games in the same league series as the rest. So a league
/// configured as `nhl=icehockey_nhl` silently matched nothing through the whole
/// of preseason, while Polymarket priced a full slate every night.
///
/// Only the `_preseason` sibling is taken, and only while the API reports it
/// active. The other siblings a league accretes are not games of that league:
/// `_championship_winner`, `_super_bowl_winner` and `_world_series_winner` are
/// season-long futures, and `_women`, `_summer_league`, `_all_stars`, `_fcs` and
/// `_qualification` are different competitions. Matching a league's moneylines
/// against any of those would pair a game with something that is not that game.
const PRESEASON_SUFFIX: &str = "_preseason";

/// Sport keys the Odds API currently reports as active. Free: `/sports` does not
/// count against the quota. An empty set on failure means the caller falls back
/// to the configured key alone, which is the behavior before this existed.
async fn active_odds_sports(http: &reqwest::Client, api_key: &str) -> HashSet<String> {
    match get_json(http, &format!("{ODDS}/sports"), Some(api_key), &[]).await {
        Ok((v, _, _)) => v.as_array().map(Vec::as_slice).unwrap_or(&[]).iter()
            .filter(|s| s.get("active").and_then(Value::as_bool) == Some(true))
            .filter_map(|s| s.get("key").and_then(Value::as_str).map(str::to_string))
            .collect(),
        Err(e) => {
            warn!("🏈 Sports ledger: Odds API /sports failed ({e}) — preseason keys not considered this pass");
            HashSet::new()
        }
    }
}

/// The sport keys to look for one league's games under: the configured key, plus
/// its preseason key while that is active.
fn sport_keys_for(sport_key: &str, active: &HashSet<String>) -> Vec<String> {
    let mut keys = vec![sport_key.to_string()];
    let pre = format!("{sport_key}{PRESEASON_SUFFIX}");
    if active.contains(&pre) { keys.push(pre); }
    keys
}

async fn refresh_catalog(
    http: &reqwest::Client,
    api_key: &str,
    leagues: &[(String, String)],
    now: DateTime<Utc>,
) -> (Vec<MatchedGame>, Option<i64>) {
    let series: HashMap<String, String> = match get_json(http, &format!("{GAMMA}/sports"), None, &[]).await {
        Ok((v, _, _)) => v.as_array().map(Vec::as_slice).unwrap_or(&[]).iter().filter_map(|s| {
            let code = s.get("sport")?.as_str()?.to_ascii_lowercase();
            let sid = match s.get("series")? { Value::String(x) => x.clone(), Value::Number(n) => n.to_string(), _ => return None };
            Some((code, sid))
        }).collect(),
        Err(e) => {
            warn!("🏈 Sports ledger: Gamma /sports failed ({e}) — no discovery this pass");
            return (Vec::new(), None);
        }
    };
    let mut games = Vec::new();
    let mut remaining = None;
    let active = active_odds_sports(http, api_key).await;
    // Per-league coverage, logged at info once per refresh. A league that matches
    // nothing looks identical to a quiet league in the totals, so the operator
    // needs to see each one: "nfl 16/16" against "mlb 0/12" is the difference
    // between no games today and a matcher that has stopped working.
    let mut coverage: Vec<String> = Vec::new();
    for (code, sport_key) in leagues {
        let Some(sid) = series.get(code) else {
            coverage.push(format!("{code} n/a (not a Polymarket league)"));
            continue;
        };
        // Gamma pages at 100 events; a league lists past, live and future games.
        let mut pm = Vec::new();
        for page in 0..GAMMA_MAX_PAGES {
            let offset = (page * GAMMA_PAGE).to_string();
            let limit = GAMMA_PAGE.to_string();
            match get_json(http, &format!("{GAMMA}/events"), None,
                &[("series_id", sid.as_str()), ("closed", "false"), ("active", "true"),
                  ("limit", limit.as_str()), ("offset", offset.as_str())]).await {
                Ok((v, _, _)) => {
                    let n = v.as_array().map_or(0, Vec::len);
                    pm.extend(parse_pm_games(&v, code, now));
                    if n < GAMMA_PAGE { break; }
                }
                Err(e) => { debug!("🏈 Sports ledger: Gamma events for '{code}' failed: {e}"); break; }
            }
        }
        if pm.is_empty() {
            coverage.push(format!("{code} 0/0"));
            continue;
        }
        // A league's games may sit under more than one Odds API sport key (the
        // regular-season key and, in its window, the preseason one). Each key is
        // tried against the games still unmatched, so a game is claimed once and
        // carries the key it was actually found under: that is the key the paid
        // snapshot call will use for it.
        let mut unmatched: Vec<PmGame> = pm.clone();
        let mut matched_here = 0usize;
        let mut failed_keys = 0usize;
        let mut found_under: Vec<String> = Vec::new();
        let keys = sport_keys_for(sport_key, &active);
        let multi = keys.len() > 1;
        for key in &keys {
            if unmatched.is_empty() { break; }
            // Free: /events does not count against the quota but still reports it.
            let odds = match get_json(http, &format!("{ODDS}/sports/{key}/events"), Some(api_key), &[]).await {
                Ok((v, r, _)) => { remaining = r.or(remaining); parse_odds_events(&v) }
                Err(e) => {
                    failed_keys += 1;
                    debug!("🏈 Sports ledger: Odds API events for '{key}' failed: {e}");
                    continue;
                }
            };
            let matched = match_games(&unmatched, &odds, key);
            debug!("🏈 Sports ledger: {code} → {key}: {} Polymarket games, {} Odds API events, {} matched",
                   unmatched.len(), odds.len(), matched.len());
            if !matched.is_empty() {
                // Name the key whenever it is not the league's configured one.
                // "nhl 6/14" alone does not say the games were found under the
                // preseason feed, which is the whole point of having looked.
                if multi && key != sport_key { found_under.push(format!("{}×{key}", matched.len())); }
                let claimed: HashSet<&str> = matched.iter().map(|m| m.pm_slug.as_str()).collect();
                unmatched.retain(|g| !claimed.contains(g.slug.as_str()));
                matched_here += matched.len();
                games.extend(matched);
            }
        }
        let note = if failed_keys == keys.len() {
            " (odds events failed)".to_string()
        } else if !found_under.is_empty() {
            format!(" [{}]", found_under.join(" + "))
        } else {
            String::new()
        };
        coverage.push(format!("{code} {matched_here}/{}{note}", pm.len()));
    }
    if !coverage.is_empty() {
        info!("🏈 Sports ledger coverage (matched/Polymarket games): {}", coverage.join(" · "));
    }
    (games, remaining)
}

/// The snapshot's single paid call. Kept separate from the row building below
/// so the caller can record the spend the moment the credits are gone, rather
/// than after the minutes of free reads that follow.
///
/// Returns the odds payload, when it arrived, the quota left and what this call
/// actually cost.
async fn fetch_odds(
    http: &reqwest::Client,
    api_key: &str,
    sport_key: &str,
    regions: &str,
) -> Result<(Value, DateTime<Utc>, Option<i64>, Option<i64>), String> {
    let (odds, remaining, last) = get_json(
        http, &format!("{ODDS}/sports/{sport_key}/odds"), Some(api_key),
        &[("regions", regions), ("markets", "h2h"), ("oddsFormat", "decimal")],
    ).await?;
    // Everything after this is free reads that take their own time, so the odds
    // are as of here and not as of the pass start.
    Ok((odds, Utc::now(), remaining, last))
}

async fn build_snapshot_rows(
    http: &reqwest::Client,
    odds: &Value,
    odds_time: DateTime<Utc>,
    sport_key: &str,
    games: &[MatchedGame],
    now: DateTime<Utc>,
    remaining: Option<i64>,
) -> (Vec<db::SportsLedgerRow>, Vec<db::SportsLedgerBookRow>) {
    let odds_at = odds_time.to_rfc3339();
    let by_id: HashMap<&str, &Value> = odds.as_array().map(Vec::as_slice).unwrap_or(&[]).iter()
        .filter_map(|e| Some((e.get("id")?.as_str()?, e)))
        .collect();
    let ts = now.to_rfc3339();
    let mut rows = Vec::new();
    let mut book_rows = Vec::new();
    let mut unkeyed = 0i64;
    for g in games.iter().filter(|g| g.sport_key == sport_key) {
        let books = by_id.get(g.odds_event_id.as_str()).and_then(|e| e.get("bookmakers")).and_then(Value::as_array);
        for o in &g.outcomes {
            let c = consensus_for(books, &o.odds_name, odds_time);
            let (pm_bid, pm_bid_size, pm_ask, pm_ask_size) = match get_json(http, CLOB_BOOK, None, &[("token_id", o.pm.token_id.as_str())]).await {
                Ok((b, _, _)) => best_levels(&b),
                Err(_) => (None, None, None, None),
            };
            // Per outcome, because the book read above is one of many and the
            // last one of a pass can be minutes after the first.
            let pm_at = Utc::now().to_rfc3339();
            unkeyed += c.unkeyed_books;
            for (book_key, decimal_odds, raw_implied, overround, book_last_update) in &c.quotes {
                book_rows.push(db::SportsLedgerBookRow {
                    ts: ts.clone(),
                    odds_at: Some(odds_at.clone()),
                    league: g.league.clone(),
                    sport_key: sport_key.to_string(),
                    odds_event_id: g.odds_event_id.clone(),
                    condition_id: o.pm.condition_id.clone(),
                    token_id: o.pm.token_id.clone(),
                    outcome_label: o.pm.label.clone(),
                    book_key: book_key.clone(),
                    decimal_odds: *decimal_odds,
                    raw_implied: *raw_implied,
                    overround: *overround,
                    book_last_update: book_last_update.clone(),
                });
            }
            rows.push(db::SportsLedgerRow {
                ts: ts.clone(),
                league: g.league.clone(),
                sport_key: sport_key.to_string(),
                odds_event_id: g.odds_event_id.clone(),
                pm_slug: g.pm_slug.clone(),
                condition_id: o.pm.condition_id.clone(),
                token_id: o.pm.token_id.clone(),
                outcome_label: o.pm.label.clone(),
                odds_outcome: o.odds_name.clone(),
                commence: g.commence.to_rfc3339(),
                secs_to_start: (g.commence - now).num_seconds(),
                consensus: c.consensus,
                num_books: c.num_books,
                dispersion: c.dispersion,
                max_book_age_secs: c.max_book_age_secs,
                pm_bid, pm_ask, pm_bid_size, pm_ask_size,
                credits_remaining: remaining,
                odds_at: Some(odds_at.clone()),
                pm_at: Some(pm_at),
                overround: c.overround,
                raw_consensus: c.raw_consensus,
            });
        }
    }
    if unkeyed > 0 {
        warn!("🏈 Sports ledger [{sport_key}]: {unkeyed} book quote(s) had no bookmaker key and were not recorded —                their consensus contribution stands but they are missing from sports_line_books");
    }
    (rows, book_rows)
}

/// What `token` paid when its Gamma market resolved, or `None` while it is open
/// or unsettled.
///
/// A win or loss settles at 1 or 0. A tie, or a game canceled with no makeup,
/// settles both outcomes at 0.5 under Polymarket's sports rules; that is only
/// taken as a result when Gamma also reports `umaResolutionStatus: resolved`,
/// so a closed market still mid-resolution at 0.5 is not mistaken for one.
/// Without this a canceled game never records a result and stays pending forever.
pub fn resolved_price(market: &Value, token: &str) -> Option<f64> {
    if market.get("closed").and_then(Value::as_bool) != Some(true) { return None; }
    let i = json_string_list(market.get("clobTokenIds")).iter().position(|t| t == token)?;
    let price = json_string_list(market.get("outcomePrices")).get(i)?.parse::<f64>().ok()?;
    if (price - 1.0).abs() < 1e-6 || price.abs() < 1e-6 {
        return Some(price.round());
    }
    let resolved = market.get("umaResolutionStatus").and_then(Value::as_str) == Some("resolved");
    ((price - 0.5).abs() < 1e-6 && resolved).then_some(0.5)
}

/// Settled prices for the given tokens of one market, by the same rule
/// `capture_results` records results with: 0 or 1 outright, and 0.5 only when
/// Gamma also reports `umaResolutionStatus: resolved`.
///
/// Exposed for the ghost-settlement path, which needs exactly this question and
/// had been asking a worse one. Its own helper accepted "closed, past endDate,
/// both outcomes 0.50", which on a survey of closed sports markets matched 472
/// markets of which only 352 were resolved — the other 120 were closed,
/// past-end placeholders at 0.5/0.5 that Gamma never resolved, and every one
/// would have been booked as a tie. `endDate` is no help on a game market: it
/// is kick-off, so every finished game is past it.
///
/// One request prices both sides, which is also one fewer round trip per
/// undecided token than probing them separately.
pub async fn settled_prices_for_market(
    http: &reqwest::Client,
    condition_id: &str,
    tokens: &[String],
) -> HashMap<String, f64> {
    let mut out = HashMap::new();
    let Ok((v, _, _)) = get_json(
        http, &format!("{GAMMA}/markets"), None,
        &[("condition_ids", condition_id), ("closed", "true")],
    ).await else { return out };
    let Some(market) = v.as_array().and_then(|a| a.first()) else { return out };
    for token in tokens {
        if let Some(px) = resolved_price(market, token) {
            out.insert(token.clone(), px);
        }
    }
    out
}

/// Record the resolution of every ledger market whose game started at least
/// `RESULT_AFTER_START_SECS` ago and is now closed with a 0/1 price.
async fn capture_results(http: &reqwest::Client, pool: &sqlx::SqlitePool, now: DateTime<Utc>) {
    let started_before = (now - ChronoDuration::seconds(RESULT_AFTER_START_SECS)).to_rfc3339();
    let started_after = (now - ChronoDuration::seconds(RESULT_GIVE_UP_SECS)).to_rfc3339();
    // Oldest game first, so every pending market is reached within a few passes.
    let pending = db::sports_ledger_pending_results(pool, &started_after, &started_before).await;
    let mut order: Vec<String> = Vec::new();
    let mut by_cid: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for (cid, token, label) in pending {
        if !by_cid.contains_key(&cid) { order.push(cid.clone()); }
        by_cid.entry(cid).or_default().push((token, label));
    }
    let mut recorded = 0;
    for cid in order.into_iter().take(RESULTS_PER_PASS) {
        let tokens = by_cid.remove(&cid).unwrap_or_default();
        // Gamma hides closed markets unless asked: without `closed=true` a
        // finished game's market comes back as an empty list, so no result was
        // ever recorded (0 from 919 ledger rows by 2026-09-12). Only a closed
        // market can carry a result, so asking for closed ones alone loses nothing.
        let Ok((v, _, _)) = get_json(http, &format!("{GAMMA}/markets"), None, &[("condition_ids", cid.as_str()), ("closed", "true")]).await else { continue };
        let Some(m) = v.as_array().and_then(|a| a.first()) else { continue };
        for (token, label) in tokens {
            if let Some(price) = resolved_price(m, &token) {
                db::record_sports_line_result(pool, &cid, &token, &label, price).await;
                recorded += 1;
            }
        }
    }
    if recorded > 0 {
        info!("🏈 Sports ledger: recorded {recorded} market resolution(s)");
    }
}

// ── Loop ─────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct LedgerState {
    games: Vec<MatchedGame>,
    catalog_at: Option<DateTime<Utc>>,
    results_at: Option<DateTime<Utc>>,
    last_snapshot: HashMap<String, DateTime<Utc>>,
    remaining: Option<i64>,
    day: Option<NaiveDate>,
    day_start_remaining: Option<i64>,
    spent_today: i64,
    warned_no_key: bool,
    last_budget_log: Option<DateTime<Utc>>,
    /// Whether `last_snapshot` has been seeded from the ledger's own rows.
    seeded: bool,
}

pub async fn run_sports_ledger(
    http: Arc<reqwest::Client>,
    mut config_rx: watch::Receiver<Arc<DynamicConfig>>,
    raptor_health_tx: Arc<watch::Sender<HashMap<String, crate::api::server::AssetRaptorHealth>>>,
) {
    let mut st = LedgerState::default();
    let mut was_enabled = false;
    loop {
        let cfg = config_rx.borrow().clone();
        if cfg.sports_ledger_enabled != was_enabled {
            info!("🏈 Sports ledger {}", if cfg.sports_ledger_enabled {
                "enabled — recording cross-book consensus against Polymarket International for matched sports moneylines (no trading)"
            } else {
                "disabled"
            });
            was_enabled = cfg.sports_ledger_enabled;
            if !cfg.sports_ledger_enabled {
                // Switched off means no lines, not lines that quietly age out over
                // the next six hours: `StrategyContext.sports` must be None at once.
                board_tx().send_replace(Arc::new(SportsBoard::new()));
            }
        }
        if cfg.sports_ledger_enabled {
            match std::env::var(config::SPORTS_ODDS_KEY_ENV).ok().filter(|k| !k.is_empty()) {
                Some(key) => tick(&http, &key, &cfg, &mut st, &raptor_health_tx).await,
                None if !st.warned_no_key => {
                    warn!("🏈 Sports ledger enabled but {} is not set — nothing to record", config::SPORTS_ODDS_KEY_ENV);
                    st.warned_no_key = true;
                }
                None => {
                    board_tx().send_replace(Arc::new(SportsBoard::new()));
                }
            }
        }
        // Every tick, not only after a paid snapshot: a feed that has stopped
        // answering publishes nothing, and a panel fed only by publishes would keep
        // reporting the last success forever ([B43]).
        report_board_health(
            &raptor_health_tx,
            Utc::now(),
            cfg.sports_ledger_enabled,
            std::env::var(config::SPORTS_ODDS_KEY_ENV).ok().is_some_and(|k| !k.is_empty()),
        );
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(TICK_SECS)) => {}
            _ = config_rx.changed() => {}
        }
    }
}

async fn tick(
    http: &reqwest::Client,
    api_key: &str,
    cfg: &DynamicConfig,
    st: &mut LedgerState,
    raptor_health_tx: &watch::Sender<HashMap<String, crate::api::server::AssetRaptorHealth>>,
) {
    let now = Utc::now();
    // After a restart the in-memory snapshot times are gone, and a target taken
    // minutes before the restart would read as due again and be paid for twice.
    // The ledger's own rows say when each sport was last snapshotted.
    if !st.seeded {
        if let Some(pool) = db::pool() {
            for (sport_key, ts) in db::sports_ledger_last_snapshots(pool).await {
                if let Some(t) = parse_time(&ts) {
                    st.last_snapshot.entry(sport_key).and_modify(|v| if t > *v { *v = t }).or_insert(t);
                }
            }
        }
        // The slate this instance already paid for: without it the board is empty
        // until the next paid snapshot of each sport, which on a free-tier key can
        // be the whole game (default offsets are two hours and ten minutes out).
        if let Some(pool) = db::pool() {
            let from = (now - ChronoDuration::seconds(BOARD_RETAIN_SECS)).to_rfc3339();
            let to = (now + ChronoDuration::seconds(LOOKAHEAD_SECS)).to_rfc3339();
            let rows = db::sports_ledger_board_rows(pool, &from, &to).await;
            if !rows.is_empty() {
                publish_rows(&rows, now);
                info!("🏈 Sports ledger: board seeded with {} line(s) from the recorded rows", board().len());
            }
        }
        st.seeded = true;
    }
    if st.catalog_at.map_or(true, |t| (now - t).num_seconds() >= CATALOG_REFRESH_SECS) {
        let leagues = parse_leagues(&cfg.sports_ledger_leagues);
        let (games, remaining) = refresh_catalog(http, api_key, &leagues, now).await;
        info!("🏈 Sports ledger: {} matched game(s) across {} league(s) | Odds API credits remaining {}",
              games.len(), leagues.len(), remaining.map_or("?".to_string(), |r| r.to_string()));
        st.games = games;
        if remaining.is_some() { st.remaining = remaining; }
        st.catalog_at = Some(now);
    }

    let today = now.date_naive();
    if st.day != Some(today) {
        st.day = Some(today);
        st.spent_today = 0;
        st.day_start_remaining = st.remaining;
        // A restart hands a fresh in-memory state a spend of zero, which would
        // grant the day's allowance a second time. The recorded spend for this
        // day is the truth; on a genuine rollover there is no row and the reset
        // above stands.
        if let Some(pool) = db::pool() {
            if let Some((recorded_start, spent)) = db::sports_ledger_budget(pool, &today.to_string()).await {
                st.spent_today = spent;
                st.day_start_remaining = recorded_start.or(st.day_start_remaining);
                if spent > 0 {
                    info!("🏈 Sports ledger: resuming today's budget — {spent} credit(s) already spent");
                }
            }
        }
    }
    if st.day_start_remaining.is_none() { st.day_start_remaining = st.remaining; }
    // A quota that GREW mid-day (a plan upgrade, or a key swapped for a larger
    // one) must raise the day's opening reading. The allowance is computed from
    // it, so leaving it at the old low reading pins the ledger at zero spend
    // until 00:00 UTC with nothing the operator can do from the engine.
    if let (Some(remaining), Some(day_start)) = (st.remaining, st.day_start_remaining) {
        if remaining > day_start { st.day_start_remaining = Some(remaining); }
    }

    let offsets = parse_offsets_mins(&cfg.sports_ledger_snapshot_offsets_mins);
    let regions = cfg.sports_odds_regions.clone();
    let cost = regions.split(',').filter(|r| !r.trim().is_empty()).count().max(1) as i64;
    let reserve = cfg.sports_ledger_credit_reserve;
    for sport_key in due_sport_keys(&st.games, &offsets, &st.last_snapshot, now) {
        let (Some(remaining), Some(day_start)) = (st.remaining, st.day_start_remaining) else {
            debug!("🏈 Sports ledger: quota not known yet — deferring {sport_key}");
            break;
        };
        let allowance = allowance_today(day_start, reserve, days_until_reset(now, cfg.sports_ledger_quota_reset_day));
        if !may_spend(remaining, reserve, st.spent_today, allowance, cost) {
            if st.last_budget_log.map_or(true, |t| (now - t).num_minutes() >= 30) {
                info!("🏈 Sports ledger: skipping {sport_key} — {} of {allowance} credit(s) spent today, {remaining} remaining (reserve {reserve})",
                      st.spent_today);
                st.last_budget_log = Some(now);
            }
            st.last_snapshot.insert(sport_key, now);
            continue;
        }
        match fetch_odds(http, api_key, &sport_key, &regions).await {
            Ok((odds, odds_time, remaining_after, last)) => {
                st.spent_today += last.unwrap_or(cost);
                if remaining_after.is_some() { st.remaining = remaining_after; }
                // The credits are spent the moment this response returns, and the
                // free reads below take minutes. Record the spend first: stopping
                // anywhere in that window (a deploy, the watchdog, an OOM) would
                // otherwise lose it and hand the next start the day's budget again.
                if let Some(pool) = db::pool() {
                    db::record_sports_ledger_budget(
                        pool, &today.to_string(), st.day_start_remaining, st.spent_today, &now.to_rfc3339(),
                    ).await;
                }
                let (rows, book_rows) =
                    build_snapshot_rows(http, &odds, odds_time, &sport_key, &st.games, now, st.remaining).await;
                let n_games = rows.iter().map(|r| r.odds_event_id.as_str()).collect::<HashSet<_>>().len();
                // The board is what squadrons read; the rows are what the research
                // reads. Both come from this one paid call.
                publish_rows(&rows, now);
                let (mut wrote_rows, mut wrote_books) = (0, 0);
                if let Some(pool) = db::pool() {
                    wrote_rows = db::record_sports_ledger_rows(pool, &rows).await;
                    wrote_books = db::record_sports_ledger_book_rows(pool, &book_rows).await;
                }
                // Written, not merely built: a rolled-back snapshot must not read
                // in the log as a recorded one.
                info!("🏈 Sports ledger [{sport_key}]: recorded {wrote_rows}/{} row(s) across {n_games} game(s), {wrote_books}/{} book quote(s) | credits remaining {} (spent today {} of {allowance})",
                      rows.len(), book_rows.len(), st.remaining.map_or("?".to_string(), |r| r.to_string()), st.spent_today);
            }
            Err(e) => warn!("🏈 Sports ledger [{sport_key}]: snapshot failed: {e}"),
        }
        // Taken, failed or not: this target is done, so a failure cannot retry
        // a paid call every minute for the rest of the window.
        st.last_snapshot.insert(sport_key, now);
    }

    if st.results_at.map_or(true, |t| (now - t).num_seconds() >= RESULTS_REFRESH_SECS) {
        st.results_at = Some(now);
        if let Some(pool) = db::pool() {
            capture_results(http, pool, now).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> DateTime<Utc> { parse_time(s).unwrap() }

    fn row(token: &str, label: &str, consensus: Option<f64>, commence: &str, ts: &str) -> db::SportsLedgerRow {
        db::SportsLedgerRow {
            ts: ts.into(), league: "mlb".into(), sport_key: "baseball_mlb".into(),
            odds_event_id: "e1".into(), pm_slug: "mlb-min-laa-2026-09-19".into(),
            condition_id: "0xabc".into(), token_id: token.into(), outcome_label: label.into(),
            odds_outcome: label.into(), commence: commence.into(), secs_to_start: 600,
            consensus, num_books: 7, dispersion: Some(0.0052), max_book_age_secs: Some(83),
            pm_bid: Some(0.49), pm_ask: Some(0.50), pm_bid_size: Some(100.0), pm_ask_size: Some(200.0),
            credits_remaining: Some(99_000),
            odds_at: Some(ts.into()), pm_at: Some(ts.into()), overround: Some(1.04), raw_consensus: consensus.map(|c| c * 1.04),
        }
    }

    /// A snapshot becomes one line per outcome token, and a squadron finds its own
    /// market by the tokens it already holds: no team-name matching in the patrol.
    #[test]
    fn a_snapshot_becomes_a_line_per_token() {
        let now = t("2026-09-19T23:00:00Z");
        let b = fold_rows(&SportsBoard::new(), &[
            row("tok-yes", "Minnesota Twins", Some(0.4981), "2026-09-20T01:38:00Z", "2026-09-19T23:00:00Z"),
            row("tok-no", "Los Angeles Angels", Some(0.5018), "2026-09-20T01:38:00Z", "2026-09-19T23:00:00Z"),
            // No consensus (every book pulled the game): no line, rather than a zero.
            row("tok-none", "Draw", None, "2026-09-20T01:38:00Z", "2026-09-19T23:00:00Z"),
        ], now);
        assert_eq!(b.len(), 2, "the outcome with no consensus carries no line");
        let yes = b.get("tok-yes").expect("a line for the YES token");
        assert_eq!(yes.outcome_label, "Minnesota Twins");
        assert_eq!(yes.secs_to_start(now), 2 * 3600 + 38 * 60, "kick-off is 2h38m away");
        assert_eq!(yes.age_secs(t("2026-09-19T23:02:05Z")), 125);
        assert_eq!(yes.num_books, 7);
        let line = SportsMarketLine { yes: b.get("tok-yes").cloned(), no: b.get("tok-no").cloned() };
        assert!((line.side(1).unwrap().consensus - 0.5018).abs() < 1e-9, "NO is the other token's line");
        assert_eq!(line.commence(), Some(t("2026-09-20T01:38:00Z")));
    }

    /// The board is the current slate, not the season: a game that started long ago
    /// is dropped when the next snapshot folds in.
    #[test]
    fn finished_games_leave_the_board() {
        let old = row("old-tok", "Chelsea FC", Some(0.6), "2026-09-19T12:00:00Z", "2026-09-19T11:00:00Z");
        let new = row("new-tok", "Arsenal", Some(0.55), "2026-09-19T20:00:00Z", "2026-09-19T19:00:00Z");
        let b = fold_rows(&SportsBoard::new(), &[old], t("2026-09-19T11:00:00Z"));
        assert!(b.contains_key("old-tok"));
        let b = fold_rows(&b, &[new.clone()], t("2026-09-19T17:00:00Z"));
        assert!(b.contains_key("old-tok"), "five hours after kick-off it is still within the window");
        let b = fold_rows(&b, &[new], t("2026-09-19T19:00:00Z"));
        assert!(!b.contains_key("old-tok"), "seven hours after kick-off it is gone");
        assert!(b.contains_key("new-tok"));
    }

    /// Books withdraw a line in play (11% of production rows, 25% on college
    /// football): the token leaves the board, so a consumer sees no line rather
    /// than the pre-game number served as the current one.
    #[test]
    fn a_withdrawn_line_leaves_the_board() {
        let pre = row("tok", "Arkansas", Some(0.42), "2026-09-19T16:00:00Z", "2026-09-19T14:00:00Z");
        let b = fold_rows(&SportsBoard::new(), &[pre], t("2026-09-19T14:00:00Z"));
        assert!(b.contains_key("tok"));
        let withdrawn = row("tok", "Arkansas", None, "2026-09-19T16:00:00Z", "2026-09-19T17:15:00Z");
        let b = fold_rows(&b, &[withdrawn], t("2026-09-19T17:15:00Z"));
        assert!(!b.contains_key("tok"), "the books quote nothing, so the board holds nothing");
    }

    /// A token's line movement is its own consensus change between snapshots, which
    /// is the per-outcome drift [E32] asked for.
    #[test]
    fn a_line_carries_its_own_drift() {
        let first = row("tok", "Arsenal", Some(0.50), "2026-09-19T20:00:00Z", "2026-09-19T18:00:00Z");
        let b = fold_rows(&SportsBoard::new(), &[first], t("2026-09-19T18:00:00Z"));
        assert_eq!(b["tok"].drift, None, "nothing to compare the first reading with");
        let second = row("tok", "Arsenal", Some(0.56), "2026-09-19T20:00:00Z", "2026-09-19T19:00:00Z");
        let b = fold_rows(&b, &[second], t("2026-09-19T19:00:00Z"));
        assert!((b["tok"].drift.unwrap() - 0.06).abs() < 1e-9, "drift {:?}", b["tok"].drift);
    }

    /// The published board is what `line_for` reads, and a market the ledger never
    /// matched has no line at all.
    #[test]
    fn the_published_board_answers_by_token() {
        publish_rows(&[row("pub-yes", "Arsenal", Some(0.61), "2026-09-20T01:38:00Z", "2026-09-19T23:00:00Z")], t("2026-09-19T23:00:00Z"));
        let line = line_for("pub-yes", "pub-no").expect("the YES token is on the board");
        assert!((line.yes.unwrap().consensus - 0.61).abs() < 1e-9);
        assert!(line.no.is_none(), "the other token was not matched");
        assert!(line_for("a-btc-token", "another").is_none(), "no line for a market the ledger never matched");
    }

    /// Names as each side published them on 2026-09-11.
    #[test]
    fn team_names_match_across_the_two_sources() {
        for (pm, odds) in [
            ("Stade Rennais FC 1901", "Rennes"),
            ("Olympique de Marseille", "Marseille"),
            ("RC Strasbourg Alsace", "Strasbourg"),
            ("AS Monaco FC", "AS Monaco"),
            ("Athletics", "Oakland Athletics"),
            ("Pittsburgh Pirates", "Pittsburgh Pirates"),
        ] {
            assert!(name_similarity(pm, odds) >= MIN_TEAM_SIMILARITY, "{pm} vs {odds}: {}", name_similarity(pm, odds));
        }
        assert!(name_similarity("Stade Rennais FC 1901", "Marseille") < MIN_TEAM_SIMILARITY);
    }

    /// Same-city teams share name tokens ("Chicago", and "Angels"/"Angeles" share
    /// a prefix), so a single name can look like the wrong team. The protection
    /// is that a match needs BOTH teams and the start time: simultaneous games in
    /// one city still pair correctly because their opponents differ.
    #[test]
    fn simultaneous_same_city_games_pair_with_the_right_events() {
        let now = t("2026-09-11T12:00:00Z");
        let mk = |slug: &str, a: &str, b: &str, cid: &str| serde_json::json!({
            "title": format!("{a} vs. {b}"), "slug": slug, "startTime": "2026-09-11T23:05:00Z",
            "markets": [{"sportsMarketType": "moneyline", "conditionId": cid,
                "outcomes": format!("[\"{a}\", \"{b}\"]"), "clobTokenIds": format!("[\"{cid}-a\", \"{cid}-b\"]")}]
        });
        let pm = parse_pm_games(&serde_json::json!([
            mk("cubs", "Pittsburgh Pirates", "Chicago Cubs", "c1"),
            mk("sox", "Detroit Tigers", "Chicago White Sox", "c2"),
            mk("angels", "Los Angeles Angels", "Seattle Mariners", "c3"),
            mk("dodgers", "Los Angeles Dodgers", "San Diego Padres", "c4"),
        ]), "mlb", now);
        assert_eq!(pm.len(), 4);
        let odds = parse_odds_events(&serde_json::json!([
            {"id": "e-sox", "home_team": "Chicago White Sox", "away_team": "Detroit Tigers", "commence_time": "2026-09-11T23:06:00Z"},
            {"id": "e-cubs", "home_team": "Chicago Cubs", "away_team": "Pittsburgh Pirates", "commence_time": "2026-09-11T23:05:00Z"},
            {"id": "e-dodgers", "home_team": "Los Angeles Dodgers", "away_team": "San Diego Padres", "commence_time": "2026-09-11T23:05:00Z"},
            {"id": "e-angels", "home_team": "Seattle Mariners", "away_team": "Los Angeles Angels", "commence_time": "2026-09-11T23:05:00Z"}
        ]));
        let matched = match_games(&pm, &odds, "baseball_mlb");
        let pairs: HashMap<&str, &str> = matched.iter().map(|m| (m.pm_slug.as_str(), m.odds_event_id.as_str())).collect();
        assert_eq!(pairs.len(), 4, "{pairs:?}");
        assert_eq!(pairs["cubs"], "e-cubs");
        assert_eq!(pairs["sox"], "e-sox");
        assert_eq!(pairs["angels"], "e-angels");
        assert_eq!(pairs["dodgers"], "e-dodgers");
        let angels = matched.iter().find(|m| m.pm_slug == "angels").unwrap();
        assert_eq!(angels.outcomes[0].odds_name, "Los Angeles Angels", "Polymarket's first team maps to the Odds API away side here");
    }

    fn soccer_event() -> Value {
        serde_json::json!([{
            "title": "Stade Rennais FC 1901 vs. Olympique de Marseille", "slug": "fl1-ren-olm-2026-09-11",
            "startTime": "2026-09-11T18:45:00Z",
            "markets": [
                {"sportsMarketType": "moneyline", "conditionId": "c-ren", "groupItemTitle": "Stade Rennais FC 1901",
                 "outcomes": "[\"Yes\", \"No\"]", "clobTokenIds": "[\"ren-yes\", \"ren-no\"]"},
                {"sportsMarketType": "moneyline", "conditionId": "c-draw", "groupItemTitle": "Draw (Stade Rennais FC 1901 vs. Olympique de Marseille)",
                 "outcomes": "[\"Yes\", \"No\"]", "clobTokenIds": "[\"draw-yes\", \"draw-no\"]"},
                {"sportsMarketType": "moneyline", "conditionId": "c-olm", "groupItemTitle": "Olympique de Marseille",
                 "outcomes": "[\"Yes\", \"No\"]", "clobTokenIds": "[\"olm-yes\", \"olm-no\"]"},
                {"sportsMarketType": "totals", "conditionId": "c-tot", "outcomes": "[\"Over\", \"Under\"]", "clobTokenIds": "[\"o\", \"u\"]"}
            ]
        }])
    }

    fn mlb_event() -> Value {
        serde_json::json!([{
            "title": "Pittsburgh Pirates vs. Chicago Cubs", "slug": "mlb-pit-chc-2026-09-11",
            "startTime": "2026-09-11T18:20:00Z",
            "markets": [{"sportsMarketType": "moneyline", "conditionId": "c-mlb",
                "outcomes": "[\"Pittsburgh Pirates\", \"Chicago Cubs\"]", "clobTokenIds": "[\"pit\", \"chc\"]"}]
        }])
    }

    #[test]
    fn both_polymarket_moneyline_shapes_parse() {
        let now = t("2026-09-11T12:00:00Z");
        let soccer = parse_pm_games(&soccer_event(), "fl1", now);
        assert_eq!(soccer.len(), 1);
        let labels: Vec<&str> = soccer[0].outcomes.iter().map(|o| o.label.as_str()).collect();
        assert_eq!(labels, ["Stade Rennais FC 1901", "Draw", "Olympique de Marseille"]);
        assert_eq!(soccer[0].outcomes[1].token_id, "draw-yes", "the Yes token pays on the outcome");

        let mlb = parse_pm_games(&mlb_event(), "mlb", now);
        assert_eq!(mlb[0].outcomes.iter().map(|o| (o.label.as_str(), o.token_id.as_str())).collect::<Vec<_>>(),
                   [("Pittsburgh Pirates", "pit"), ("Chicago Cubs", "chc")]);

        assert!(parse_pm_games(&mlb_event(), "mlb", t("2026-09-14T12:00:00Z")).is_empty(), "outside the tracked window");
    }

    #[test]
    fn games_match_on_time_and_names_in_either_order() {
        let now = t("2026-09-11T12:00:00Z");
        let odds = parse_odds_events(&serde_json::json!([
            {"id": "e-ren", "home_team": "Rennes", "away_team": "Marseille", "commence_time": "2026-09-11T18:45:00Z"},
            {"id": "e-str", "home_team": "Strasbourg", "away_team": "AS Monaco", "commence_time": "2026-09-12T15:15:00Z"},
            // Odds API lists the Cubs at home; Polymarket lists the Pirates first. One minute apart.
            {"id": "e-mlb", "home_team": "Chicago Cubs", "away_team": "Pittsburgh Pirates", "commence_time": "2026-09-11T18:21:00Z"}
        ]));
        let soccer = match_games(&parse_pm_games(&soccer_event(), "fl1", now), &odds, "soccer_france_ligue_one");
        assert_eq!(soccer.len(), 1);
        assert_eq!(soccer[0].odds_event_id, "e-ren");
        let names: Vec<(&str, &str)> = soccer[0].outcomes.iter().map(|o| (o.pm.label.as_str(), o.odds_name.as_str())).collect();
        assert_eq!(names, [("Stade Rennais FC 1901", "Rennes"), ("Draw", "Draw"), ("Olympique de Marseille", "Marseille")]);

        let mlb = match_games(&parse_pm_games(&mlb_event(), "mlb", now), &odds, "baseball_mlb");
        assert_eq!(mlb.len(), 1);
        let names: Vec<(&str, &str)> = mlb[0].outcomes.iter().map(|o| (o.pm.label.as_str(), o.odds_name.as_str())).collect();
        assert_eq!(names, [("Pittsburgh Pirates", "Pittsburgh Pirates"), ("Chicago Cubs", "Chicago Cubs")]);

        let late = parse_odds_events(&serde_json::json!([
            {"id": "x", "home_team": "Rennes", "away_team": "Marseille", "commence_time": "2026-09-11T19:30:00Z"}
        ]));
        assert!(match_games(&parse_pm_games(&soccer_event(), "fl1", now), &late, "k").is_empty(), "45 minutes apart is a different game");
    }

    /// [E63] The NHL preseason gap, as production reported it on 2026-09-20:
    /// `nhl 0/15`, fifteen Polymarket games matching nothing, for weeks.
    ///
    /// The Odds API carries no preseason game under `icehockey_nhl`; its earliest
    /// event was nine days out, while every one of those fifteen games was live
    /// on `icehockey_nhl_preseason` that night.
    #[test]
    fn a_league_finds_its_games_under_the_preseason_key_too() {
        let active: HashSet<String> = ["icehockey_nhl", "icehockey_nhl_preseason", "icehockey_nhl_championship_winner"]
            .iter().map(|s| s.to_string()).collect();

        let keys = sport_keys_for("icehockey_nhl", &active);
        assert_eq!(keys, vec!["icehockey_nhl", "icehockey_nhl_preseason"],
                   "the preseason key is taken; the season-long futures key is not");

        // Inactive preseason (the rest of the year) costs nothing and adds nothing.
        let season_only: HashSet<String> = ["icehockey_nhl"].iter().map(|s| s.to_string()).collect();
        assert_eq!(sport_keys_for("icehockey_nhl", &season_only), vec!["icehockey_nhl"]);

        // Real shapes from that night. Polymarket says 17:00:00 and names the
        // teams short and away-first; the Odds API says 17:08:11 and names them
        // in full, home-first.
        let now = t("2026-09-20T15:00:00Z");
        let pm = parse_pm_games(&serde_json::json!([{
            "slug": "nhl-nyi-njd-2026-09-20", "title": "Islanders vs. Devils",
            "startTime": "2026-09-20T17:00:00Z",
            "markets": [{
                "sportsMarketType": "moneyline", "closed": false, "conditionId": "0xnhl",
                "outcomes": "[\"Islanders\", \"Devils\"]",
                "clobTokenIds": "[\"tok-nyi\", \"tok-njd\"]"
            }]
        }]), "nhl", now);
        assert_eq!(pm.len(), 1, "Polymarket prices the preseason game");

        let season = parse_odds_events(&serde_json::json!([
            {"id": "e-reg", "home_team": "Carolina Hurricanes", "away_team": "Florida Panthers",
             "commence_time": "2026-09-29T21:00:47Z"}
        ]));
        assert!(match_games(&pm, &season, "icehockey_nhl").is_empty(),
                "this is the gap: the regular-season feed has nothing for tonight");

        let preseason = parse_odds_events(&serde_json::json!([
            {"id": "e-pre", "home_team": "New Jersey Devils", "away_team": "New York Islanders",
             "commence_time": "2026-09-20T17:08:11Z"}
        ]));
        let matched = match_games(&pm, &preseason, "icehockey_nhl_preseason");
        assert_eq!(matched.len(), 1, "and this is the fix");
        // The game carries the key it was found under, which is the key its paid
        // snapshot call must use.
        assert_eq!(matched[0].sport_key, "icehockey_nhl_preseason");
        let names: Vec<(&str, &str)> = matched[0].outcomes.iter()
            .map(|o| (o.pm.label.as_str(), o.odds_name.as_str())).collect();
        assert_eq!(names, [("Islanders", "New York Islanders"), ("Devils", "New Jersey Devils")],
                   "the sides are paired across the two naming conventions, not by position");
    }

    fn game(sport: &str, start: &str) -> MatchedGame {
        MatchedGame { league: "x".into(), sport_key: sport.into(), odds_event_id: "e".into(), pm_slug: "s".into(), commence: t(start), outcomes: vec![] }
    }

    #[test]
    fn a_sport_is_due_once_per_target_and_one_call_covers_its_games() {
        let games = [game("mlb", "2026-09-11T22:40:00Z"), game("mlb", "2026-09-11T22:45:00Z"), game("nfl", "2026-09-12T17:00:00Z")];
        let offsets = [-120, -10];
        let mut last = HashMap::new();
        assert!(due_sport_keys(&games, &offsets, &last, t("2026-09-11T20:30:00Z")).is_empty(), "before the first target");
        let at = t("2026-09-11T20:41:00Z");
        assert_eq!(due_sport_keys(&games, &offsets, &last, at), ["mlb"]);
        last.insert("mlb".to_string(), at);
        assert!(due_sport_keys(&games, &offsets, &last, t("2026-09-11T20:46:00Z")).is_empty(),
                "the second game's target at 20:45 is covered by the 20:41 call, within the coalescing window");
        assert_eq!(due_sport_keys(&games, &offsets, &last, t("2026-09-11T22:31:00Z")), ["mlb"], "the ten-minute target");
        assert!(due_sport_keys(&games, &offsets, &HashMap::new(), t("2026-09-11T21:05:00Z")).is_empty(), "a target past its window is skipped");
    }

    #[test]
    fn spend_is_spread_over_the_cycle_and_never_touches_the_reserve() {
        assert_eq!(days_until_reset(t("2026-09-11T15:00:00Z"), 1), 20);
        assert_eq!(days_until_reset(t("2026-09-30T23:00:00Z"), 1), 1);
        assert_eq!(days_until_reset(t("2026-12-15T00:00:00Z"), 10), 26);
        assert_eq!(days_until_reset(t("2026-09-11T15:00:00Z"), 31), 17, "day clamped to the 28th");

        // 265 left with 20 days to go and a 25-credit reserve: 12 a day.
        let allowance = allowance_today(265, 25, 20);
        assert_eq!(allowance, 12);
        assert!(may_spend(265, 25, 11, allowance, 1));
        assert!(!may_spend(265, 25, 12, allowance, 1), "the day's allowance is spent");
        assert!(!may_spend(25, 25, 0, allowance, 1), "never below the reserve");
    }

    #[test]
    fn consensus_removes_the_vig_per_book_and_reads_the_three_way_draw() {
        let now = t("2026-09-11T18:00:00Z");
        let books: Vec<Value> = serde_json::from_value(serde_json::json!([
            {"key": "draftkings", "last_update": "2026-09-11T17:59:30Z", "markets": [{"key": "h2h", "outcomes": [
                {"name": "Rennes", "price": 2.5}, {"name": "Draw", "price": 3.4}, {"name": "Marseille", "price": 2.9}]}]},
            {"key": "fanduel", "last_update": "2026-09-11T17:58:00Z", "markets": [{"key": "h2h", "outcomes": [
                {"name": "Rennes", "price": 2.4}, {"name": "Draw", "price": 3.5}, {"name": "Marseille", "price": 3.0}]}]}
        ])).unwrap();
        let c = consensus_for(Some(&books), "Draw", now);
        assert_eq!(c.num_books, 2);
        let p = c.consensus.unwrap();
        assert!((0.25..0.30).contains(&p), "draw consensus {p}");
        assert!(c.dispersion.unwrap() < 0.02);
        assert_eq!(c.max_book_age_secs, Some(120), "oldest quote is two minutes old");
        assert_eq!(consensus_for(Some(&books), "Lyon", now).num_books, 0);

        // [E63] The raw parts must reconstruct the de-vigged consensus: both
        // books price a three-way market at an overround above 1, and the stored
        // raw figures are what a different de-vig would start from.
        assert!(c.overround.unwrap() > 1.0, "three-way book overround {:?}", c.overround);
        assert!(c.raw_consensus.unwrap() > p, "raw prob must exceed the de-vigged one");
        assert_eq!(c.quotes.len(), 2, "one raw quote per contributing book");
        for (_, odds, raw_implied, overround, _) in &c.quotes {
            assert!((raw_implied - 1.0 / odds).abs() < 1e-12, "raw implied is 1/decimal odds");
            assert!(*overround > 1.0);
        }
        assert_eq!(c.unkeyed_books, 0);

        // A book with no identifier still counts toward the consensus, but its
        // raw quote is not recorded: the per-book table is keyed by book, so a
        // shared placeholder would keep only the first and read as a full set.
        let anonymous: Vec<Value> = serde_json::from_value(serde_json::json!([
            {"last_update": "2026-09-11T17:59:30Z", "markets": [{"key": "h2h", "outcomes": [
                {"name": "Rennes", "price": 2.5}, {"name": "Draw", "price": 3.4}, {"name": "Marseille", "price": 2.9}]}]}
        ])).unwrap();
        let a = consensus_for(Some(&anonymous), "Draw", now);
        assert_eq!(a.num_books, 1, "it still contributes to the consensus");
        assert!(a.consensus.is_some());
        assert!(a.quotes.is_empty(), "but no per-book row is written for it");
        assert_eq!(a.unkeyed_books, 1, "and it is counted so the gap is reportable");
    }

    /// Both closed-market shapes as Gamma served them on 2026-09-11.
    #[test]
    fn resolutions_read_per_token_from_closed_markets() {
        let mlb = serde_json::json!({
            "closed": true, "outcomes": "[\"Pittsburgh Pirates\", \"Chicago White Sox\"]",
            "outcomePrices": "[\"1\", \"0\"]", "clobTokenIds": "[\"pirates\", \"whitesox\"]"
        });
        assert_eq!(resolved_price(&mlb, "pirates"), Some(1.0));
        assert_eq!(resolved_price(&mlb, "whitesox"), Some(0.0));
        assert_eq!(resolved_price(&mlb, "someone-else"), None);

        let rennes_win = serde_json::json!({
            "closed": true, "groupItemTitle": "Stade Rennais FC 1901", "outcomes": "[\"Yes\", \"No\"]",
            "outcomePrices": "[\"1\", \"0\"]", "clobTokenIds": "[\"ren-yes\", \"ren-no\"]"
        });
        assert_eq!(resolved_price(&rennes_win, "ren-yes"), Some(1.0), "the ledger records the Yes token");

        let open = serde_json::json!({"closed": false, "outcomePrices": "[\"0.62\", \"0.38\"]", "clobTokenIds": "[\"a\", \"b\"]"});
        assert_eq!(resolved_price(&open, "a"), None);
        let closed_unsettled = serde_json::json!({"closed": true, "outcomePrices": "[\"0.5\", \"0.5\"]", "clobTokenIds": "[\"a\", \"b\"]"});
        assert_eq!(resolved_price(&closed_unsettled, "a"), None, "0.5 on a closed market is not a result until Gamma says it resolved");
        let canceled = serde_json::json!({"closed": true, "umaResolutionStatus": "resolved",
            "outcomePrices": "[\"0.5\", \"0.5\"]", "clobTokenIds": "[\"a\", \"b\"]"});
        assert_eq!(resolved_price(&canceled, "b"), Some(0.5), "a tie or cancellation settles both outcomes at 0.5");
    }

    #[test]
    fn best_levels_read_the_touch_regardless_of_ordering() {
        let book = serde_json::json!({
            "bids": [{"price": "0.47", "size": "100"}, {"price": "0.48", "size": "25"}, {"price": "0.49", "size": "0"}],
            "asks": [{"price": "0.52", "size": "10"}, {"price": "0.50", "size": "40"}]
        });
        assert_eq!(best_levels(&book), (Some(0.48), Some(25.0), Some(0.50), Some(40.0)));
    }
}

#[cfg(test)]
mod live_gamma_tests {
    use super::*;

    /// End-to-end against the live free endpoints, which cost no credits: does
    /// the real NHL slate actually match now? Run it with
    /// `ODDS_API_KEY=... cargo test nhl_preseason_matches_live -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore = "live Gamma and Odds API: network"]
    async fn nhl_preseason_matches_live() {
        let Ok(key) = std::env::var("ODDS_API_KEY") else { return };
        let http = reqwest::Client::new();
        let now = Utc::now();

        let (pm_json, _, _) = get_json(&http, &format!("{GAMMA}/events"), None,
            &[("series_id", "10346"), ("closed", "false"), ("active", "true"), ("limit", "100")])
            .await.expect("gamma events");
        let pm = parse_pm_games(&pm_json, "nhl", now);
        println!("Polymarket NHL games in window: {}", pm.len());

        let active = active_odds_sports(&http, &key).await;
        let keys = sport_keys_for("icehockey_nhl", &active);
        println!("sport keys tried: {keys:?}");

        let mut unmatched = pm.clone();
        let mut total = 0usize;
        for k in &keys {
            let (v, _, _) = get_json(&http, &format!("{ODDS}/sports/{k}/events"), Some(&key), &[])
                .await.expect("odds events");
            let odds = parse_odds_events(&v);
            let matched = match_games(&unmatched, &odds, k);
            println!("  {k}: {} events, {} matched", odds.len(), matched.len());
            let claimed: HashSet<&str> = matched.iter().map(|m| m.pm_slug.as_str()).collect();
            unmatched.retain(|g| !claimed.contains(g.slug.as_str()));
            total += matched.len();
        }
        println!("matched {total}/{} ; unmatched slugs: {:?}",
                 pm.len(), unmatched.iter().map(|g| &g.slug).collect::<Vec<_>>());
        if !pm.is_empty() {
            assert!(total > 0, "no NHL game matched under any key — the gap is still open");
        }
    }

    /// The 2026-09-11 Colorado Rockies at Detroit Tigers game, as the production
    /// ledger recorded it. Gamma hides a closed market unless the query asks for
    /// closed markets, so before `closed=true` this pass got an empty list and
    /// recorded nothing.
    #[tokio::test]
    #[ignore = "live Gamma: network"]
    async fn results_capture_records_a_finished_game_from_live_gamma() {
        let pool = db::memory_pool_for_tests().await;
        let cid = "0x5fb42ea94462e80ef345081e6886c77435130b5555db31dc15ca8c63d7e9deea";
        let row = |token: &str, label: &str| db::SportsLedgerRow {
            ts: "2026-09-11T22:10:18+00:00".into(), league: "mlb".into(), sport_key: "baseball_mlb".into(),
            odds_event_id: "ef265df05ca10a02712319b8f7e74641".into(), pm_slug: "mlb-col-det-2026-09-11".into(),
            condition_id: cid.into(), token_id: token.into(), outcome_label: label.into(), odds_outcome: label.into(),
            commence: "2026-09-11T22:40:00+00:00".into(), secs_to_start: 1781, consensus: None, num_books: 9,
            dispersion: None, max_book_age_secs: None, pm_bid: None, pm_ask: None, pm_bid_size: None,
            pm_ask_size: None, credits_remaining: None,
            odds_at: None, pm_at: None, overround: None, raw_consensus: None,
        };
        db::record_sports_ledger_rows(&pool, &[
            row("114532502496137932693089688451118145455360814432114926570975463361514518351293", "Colorado Rockies"),
            row("29514720079430880907153855961681340086780539591746523903213318866605387663992", "Detroit Tigers"),
        ]).await;
        let now = parse_time("2026-09-12T13:00:00Z").unwrap();
        capture_results(&reqwest::Client::new(), &pool, now).await;
        let prices: Vec<f64> = sqlx::query_scalar("SELECT resolved_price FROM sports_line_results ORDER BY outcome_label")
            .fetch_all(&pool).await.unwrap();
        assert_eq!(prices.len(), 2, "both outcomes of a finished game resolve");
        assert_eq!(prices.iter().sum::<f64>(), 1.0);
    }
}

#[cfg(test)]
mod redaction_tests {
    use super::redact_url_secrets;

    /// The shape the leak actually arrives in: a `reqwest::Error` names the URL
    /// it failed on, and this provider's URL carries the credential.
    #[test]
    fn a_transport_error_does_not_carry_the_key() {
        let leak = "error sending request for url \
                    (https://api.the-odds-api.com/v4/sports/soccer_epl/odds?regions=uk\
&markets=h2h&oddsFormat=decimal&apiKey=abcd1234deadbeef)";
        let safe = redact_url_secrets(leak);
        assert!(!safe.contains("abcd1234deadbeef"), "key survived redaction: {safe}");
        assert!(safe.contains("apiKey=<redacted>"), "{safe}");
        // Everything around the secret must survive, or the log stops being useful.
        assert!(safe.contains("error sending request"));
        assert!(safe.contains("soccer_epl"));
        assert!(safe.ends_with(')'), "trailing context lost: {safe}");
    }

    /// The key is not always last in the query string.
    #[test]
    fn a_key_followed_by_more_parameters_is_redacted() {
        let safe = redact_url_secrets("...?apiKey=SECRET&markets=h2h&oddsFormat=decimal");
        assert!(!safe.contains("SECRET"));
        assert_eq!(safe, "...?apiKey=<redacted>&markets=h2h&oddsFormat=decimal");
    }

    /// More than one occurrence, and text with none at all.
    #[test]
    fn every_occurrence_goes_and_clean_text_is_untouched() {
        let safe = redact_url_secrets("a apiKey=one b apiKey=two c");
        assert!(!safe.contains("one") && !safe.contains("two"), "{safe}");
        assert_eq!(redact_url_secrets("HTTP 401: quota reached"), "HTTP 401: quota reached");
        assert_eq!(redact_url_secrets(""), "");
    }
}
