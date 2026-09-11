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
//! When the ledger is enabled it owns the Odds API budget, and the Sports Raptor
//! stops polling: its single global consensus has no consumer and would spend
//! the same quota.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDate, TimeZone, Utc};
use rust_decimal::prelude::ToPrimitive;
use serde_json::Value;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::config;
use crate::helpers::db;
use crate::helpers::dynamic_config::DynamicConfig;

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

/// Vig-free consensus for one outcome across books, with book count,
/// max-minus-min dispersion and the oldest book quote's age in seconds.
pub fn consensus_for(books: Option<&Vec<Value>>, odds_name: &str, now: DateTime<Utc>) -> (Option<f64>, i64, Option<f64>, Option<i64>) {
    let mut probs: Vec<f64> = Vec::new();
    let mut oldest: Option<i64> = None;
    for book in books.map(Vec::as_slice).unwrap_or(&[]) {
        let Some(p) = crate::raptors::sports::vig_free_prob_for(book, odds_name).and_then(|d| d.to_f64()) else { continue };
        probs.push(p);
        if let Some(t) = book.get("last_update").and_then(Value::as_str).and_then(parse_time) {
            let age = (now - t).num_seconds().max(0);
            oldest = Some(oldest.map_or(age, |o| o.max(age)));
        }
    }
    if probs.is_empty() { return (None, 0, None, oldest); }
    let n = probs.len();
    let mean = probs.iter().sum::<f64>() / n as f64;
    let disp = probs.iter().cloned().fold(f64::MIN, f64::max) - probs.iter().cloned().fold(f64::MAX, f64::min);
    (Some(mean), n as i64, Some(disp), oldest)
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
        .map_err(|e| crate::raptors::sports::redact_url_secrets(&e.to_string()))?;
    let header = |name: &str| resp.headers().get(name).and_then(|v| v.to_str().ok()).and_then(|s| s.trim().parse::<f64>().ok()).map(|f| f as i64);
    let (remaining, last) = (header("x-requests-remaining"), header("x-requests-last"));
    let status = resp.status();
    let body = resp.text().await.map_err(|e| crate::raptors::sports::redact_url_secrets(&e.to_string()))?;
    if !status.is_success() {
        return Err(format!("HTTP {status}: {}", body.chars().take(200).collect::<String>()));
    }
    let json = serde_json::from_str(&body).map_err(|e| format!("invalid JSON: {e}"))?;
    Ok((json, remaining, last))
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
    for (code, sport_key) in leagues {
        let Some(sid) = series.get(code) else {
            debug!("🏈 Sports ledger: Polymarket has no league '{code}'");
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
        if pm.is_empty() { continue; }
        // Free: /events does not count against the quota but still reports it.
        let odds = match get_json(http, &format!("{ODDS}/sports/{sport_key}/events"), Some(api_key), &[]).await {
            Ok((v, r, _)) => { remaining = r.or(remaining); parse_odds_events(&v) }
            Err(e) => { debug!("🏈 Sports ledger: Odds API events for '{sport_key}' failed: {e}"); continue; }
        };
        let matched = match_games(&pm, &odds, sport_key);
        debug!("🏈 Sports ledger: {code} → {sport_key}: {} Polymarket games, {} Odds API events, {} matched", pm.len(), odds.len(), matched.len());
        games.extend(matched);
    }
    (games, remaining)
}

async fn snapshot(
    http: &reqwest::Client,
    api_key: &str,
    sport_key: &str,
    regions: &str,
    games: &[MatchedGame],
    now: DateTime<Utc>,
) -> Result<(Vec<db::SportsLedgerRow>, Option<i64>, Option<i64>), String> {
    let (odds, remaining, last) = get_json(
        http, &format!("{ODDS}/sports/{sport_key}/odds"), Some(api_key),
        &[("regions", regions), ("markets", "h2h"), ("oddsFormat", "decimal")],
    ).await?;
    let by_id: HashMap<&str, &Value> = odds.as_array().map(Vec::as_slice).unwrap_or(&[]).iter()
        .filter_map(|e| Some((e.get("id")?.as_str()?, e)))
        .collect();
    let ts = now.to_rfc3339();
    let mut rows = Vec::new();
    for g in games.iter().filter(|g| g.sport_key == sport_key) {
        let books = by_id.get(g.odds_event_id.as_str()).and_then(|e| e.get("bookmakers")).and_then(Value::as_array);
        for o in &g.outcomes {
            let (consensus, num_books, dispersion, max_book_age_secs) = consensus_for(books, &o.odds_name, now);
            let (pm_bid, pm_bid_size, pm_ask, pm_ask_size) = match get_json(http, CLOB_BOOK, None, &[("token_id", o.pm.token_id.as_str())]).await {
                Ok((b, _, _)) => best_levels(&b),
                Err(_) => (None, None, None, None),
            };
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
                consensus, num_books, dispersion, max_book_age_secs,
                pm_bid, pm_ask, pm_bid_size, pm_ask_size,
                credits_remaining: remaining,
            });
        }
    }
    Ok((rows, remaining, last))
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
        let Ok((v, _, _)) = get_json(http, &format!("{GAMMA}/markets"), None, &[("condition_ids", cid.as_str())]).await else { continue };
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
) {
    let mut st = LedgerState::default();
    let mut was_enabled = false;
    loop {
        let cfg = config_rx.borrow().clone();
        if cfg.sports_ledger_enabled != was_enabled {
            info!("🏈 Sports ledger {}", if cfg.sports_ledger_enabled {
                "enabled — recording book consensus against Polymarket for matched sports moneylines (no trading); the Sports Raptor stops polling"
            } else {
                "disabled"
            });
            was_enabled = cfg.sports_ledger_enabled;
        }
        if cfg.sports_ledger_enabled {
            match std::env::var(config::SPORTS_ODDS_KEY_ENV).ok().filter(|k| !k.is_empty()) {
                Some(key) => tick(&http, &key, &cfg, &mut st).await,
                None if !st.warned_no_key => {
                    warn!("🏈 Sports ledger enabled but {} is not set — nothing to record", config::SPORTS_ODDS_KEY_ENV);
                    st.warned_no_key = true;
                }
                None => {}
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(TICK_SECS)) => {}
            _ = config_rx.changed() => {}
        }
    }
}

async fn tick(http: &reqwest::Client, api_key: &str, cfg: &DynamicConfig, st: &mut LedgerState) {
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
    }
    if st.day_start_remaining.is_none() { st.day_start_remaining = st.remaining; }

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
        match snapshot(http, api_key, &sport_key, &regions, &st.games, now).await {
            Ok((rows, remaining_after, last)) => {
                st.spent_today += last.unwrap_or(cost);
                if remaining_after.is_some() { st.remaining = remaining_after; }
                let n_games = rows.iter().map(|r| r.odds_event_id.as_str()).collect::<HashSet<_>>().len();
                if let Some(pool) = db::pool() {
                    db::record_sports_ledger_rows(pool, &rows).await;
                }
                info!("🏈 Sports ledger [{sport_key}]: {} row(s) across {n_games} game(s) | credits remaining {} (spent today {} of {allowance})",
                      rows.len(), st.remaining.map_or("?".to_string(), |r| r.to_string()), st.spent_today);
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
            {"last_update": "2026-09-11T17:59:30Z", "markets": [{"key": "h2h", "outcomes": [
                {"name": "Rennes", "price": 2.5}, {"name": "Draw", "price": 3.4}, {"name": "Marseille", "price": 2.9}]}]},
            {"last_update": "2026-09-11T17:58:00Z", "markets": [{"key": "h2h", "outcomes": [
                {"name": "Rennes", "price": 2.4}, {"name": "Draw", "price": 3.5}, {"name": "Marseille", "price": 3.0}]}]}
        ])).unwrap();
        let (p, n, disp, age) = consensus_for(Some(&books), "Draw", now);
        assert_eq!(n, 2);
        let p = p.unwrap();
        assert!((0.25..0.30).contains(&p), "draw consensus {p}");
        assert!(disp.unwrap() < 0.02);
        assert_eq!(age, Some(120), "oldest quote is two minutes old");
        assert_eq!(consensus_for(Some(&books), "Lyon", now).1, 0);
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
