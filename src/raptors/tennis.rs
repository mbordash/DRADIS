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

/// Tennis Raptor — live tennis event-state signal (score, server, break point).
///
/// A *macro*, venue-neutral Raptor beside the Sports Raptor. Where the Sports
/// Raptor reads the public betting market's slow line drift across all sports,
/// the Tennis Raptor reads the **event itself**: the live score of one tracked
/// tennis match — sets, games, in-game points, who is serving, and whether the
/// receiver holds a break point. Both venues list tennis match markets; this is
/// the event-specific state a 300s generic line sample cannot carry.
///
/// ── Source ──────────────────────────────────────────────────────────────────
/// The Live Tennis API (livetennisapi.com) REST surface, keyed on env
/// `LIVETENNIS_API_KEY` (`config::TENNIS_API_KEY_ENV`). Each poll fetches
/// `GET /matches?status=live` (FREE tier), picks a **tracked match** — sticky on
/// the previously tracked id while it stays live, otherwise the match with the
/// freshest score timestamp — and derives:
///
/// │ Field          │ Derivation                                                │
/// │────────────────│───────────────────────────────────────────────────────────│
/// │ num_live       │ live matches in the sample (0 = nothing on court)          │
/// │ sets/games     │ tracked match's sets won + games in the current set        │
/// │ server         │ serving side (1/2; 0 = unknown)                            │
/// │ break_point    │ receiver at AD, or receiver at 40 vs server <40; never in  │
/// │                │ tiebreaks                                                  │
/// │ feed_age_secs  │ now − tracked score's `timestamp` (staleness measure)      │
///
/// ── Budget (honest numbers) ─────────────────────────────────────────────────
/// The free tier allows 30 req/min and 100 req/day. The default
/// `TENNIS_POLL_SECS = 900` fits all-day polling inside the free day cap
/// (≤96 req/day); dropping to ~60s gives near point-level tracking but burns
/// the free day cap in ~100 minutes — fine for develop-and-test or following a
/// few matches, while sustained all-day fast polling needs a paid tier. The
/// top tier's push WebSocket and model win-probability fields are NOT used
/// here — v1 is free-tier REST only (a WS lane can arrive later, config-gated).
///
/// ── Feed health / staleness ─────────────────────────────────────────────────
/// Every score carries the API's own `timestamp` (last score change, UTC). When
/// the tracked match's score is older than `TENNIS_SCORE_STALENESS_SECS` the
/// raptor reports `tennis_connected = false` while still publishing the last
/// values plus `feed_age_secs`, so a consumer treats a stale feed exactly like
/// a missing one — widen or pull, never hold on it. Tennis pauses legitimately
/// (changeovers ~90s, set breaks ~120s+), so the threshold sits well above
/// those. Zero live matches is a *healthy* state (`tennis_connected = true`,
/// `num_live = 0`), not a failure — tennis has quiet hours every day.
///
/// ── Observe-only status ─────────────────────────────────────────────────────
/// Wired **observe-only** exactly like the Tide and Sports Raptors: it
/// publishes to telemetry but is NOT consumed by any Viper sizing. Without
/// `LIVETENNIS_API_KEY` it degrades silently to its `Default` (all-zero,
/// `tennis_connected = false`); consumers treat a zero snapshot as neutral.
/// Telemetry is published under the fixed `"tennis"` health-map key.
use std::collections::HashMap;
use std::sync::Arc;

use rust_decimal::Decimal;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::api::server::AssetRaptorHealth;
use crate::config;

/// Fixed health-map key under which the (venue-neutral) Tennis Raptor publishes
/// its telemetry, alongside "sports" and the per-asset crypto entries.
pub const TENNIS_HEALTH_KEY: &str = "tennis";

/// Normalised tennis event-state snapshot broadcast to every consuming Squadron.
///
/// `Copy` so the `watch` channel hands out cheap value clones, and `Default`
/// (all-zero, `num_live = 0` meaning "no data") so the channel can be seeded
/// before the first successful poll and off-source reads are unambiguous.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TennisSnapshot {
    /// Live matches in the sample. `0` = no data OR nothing on court — either
    /// way there is no event state to act on (neutral).
    pub num_live: Decimal,
    /// Sets won by player 1 / player 2 in the tracked match.
    pub sets_p1: Decimal,
    pub sets_p2: Decimal,
    /// Games won in the tracked match's current set.
    pub games_p1: Decimal,
    pub games_p2: Decimal,
    /// Serving side of the tracked match: `1` or `2`; `0` = unknown.
    pub server: Decimal,
    /// True when the receiver holds a break point: receiver at AD, or receiver
    /// at 40 while the server is below 40. Never true in a tiebreak.
    pub break_point: bool,
    /// True while the tracked match's current game is a tiebreak.
    pub is_tiebreak: bool,
    /// Age (seconds) of the tracked score's API timestamp at poll time.
    /// `-1` when unknown (no tracked match, or no timestamp on the score).
    pub feed_age_secs: Decimal,
}

pub async fn run_tennis_raptor(
    http: Arc<reqwest::Client>,
    tennis_tx: watch::Sender<TennisSnapshot>,
    raptor_health_tx: Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
) {
    let api_key = std::env::var(config::TENNIS_API_KEY_ENV).ok().filter(|k| !k.is_empty());
    let Some(api_key) = api_key else {
        info!(
            "🎾 Tennis Raptor idle — {} not set (observe-only; no event-state signal)",
            config::TENNIS_API_KEY_ENV
        );
        // Seed a neutral snapshot + offline telemetry, then park. Receivers stay
        // valid; consumers read a zero snapshot as neutral.
        let _ = tennis_tx.send(TennisSnapshot::default());
        raptor_health_tx.send_modify(|map| {
            map.entry(TENNIS_HEALTH_KEY.to_string()).or_default().tennis_connected = false;
        });
        std::future::pending::<()>().await;
        return;
    };

    let url = live_matches_url(config::TENNIS_TOUR);

    // Track the previously tracked match id so the raptor stays on the SAME
    // match while it remains live and rotates cleanly when it finishes.
    let mut tracked: Option<i64> = None;
    let mut consecutive_failures: u32 = 0;

    loop {
        match try_fetch_live_state(&http, &url, &api_key, tracked).await {
            Ok(sample) => {
                consecutive_failures = 0;
                tracked = (sample.match_id != 0).then_some(sample.match_id);

                let snap = TennisSnapshot {
                    num_live:      Decimal::from(sample.num_live),
                    sets_p1:       Decimal::from(sample.sets_p1),
                    sets_p2:       Decimal::from(sample.sets_p2),
                    games_p1:      Decimal::from(sample.games_p1),
                    games_p2:      Decimal::from(sample.games_p2),
                    server:        Decimal::from(sample.server),
                    break_point:   sample.break_point,
                    is_tiebreak:   sample.is_tiebreak,
                    feed_age_secs: Decimal::from(sample.feed_age_secs),
                };
                let _ = tennis_tx.send(snap);
                // Stage-4 semantics: a stale feed reads as NOT connected, so any
                // consumer widens/pulls exactly as if the feed were missing.
                let connected = !sample.stale;
                raptor_health_tx.send_modify(|map| {
                    let h = map.entry(TENNIS_HEALTH_KEY.to_string()).or_default();
                    h.tennis_connected     = connected;
                    h.tennis_num_live      = snap.num_live;
                    h.tennis_sets_p1       = snap.sets_p1;
                    h.tennis_sets_p2       = snap.sets_p2;
                    h.tennis_games_p1      = snap.games_p1;
                    h.tennis_games_p2      = snap.games_p2;
                    h.tennis_server        = snap.server;
                    h.tennis_break_point   = snap.break_point;
                    h.tennis_is_tiebreak   = snap.is_tiebreak;
                    h.tennis_feed_age_secs = snap.feed_age_secs;
                    h.tennis_match         = sample.match_label.clone();
                    h.tennis_tournament    = sample.tournament.clone();
                    h.tennis_tour          = sample.tour.clone();
                    h.tennis_points        = sample.points_label.clone();
                    h.tennis_score_at      = sample.score_at.clone();
                });
                if sample.stale {
                    warn!(
                        "🎾 Tennis Raptor [{}]: score stale ({}s > {}s limit) — reporting disconnected \
                         (consumers widen/pull, never hold)",
                        sample.match_label, sample.feed_age_secs, config::TENNIS_SCORE_STALENESS_SECS,
                    );
                } else if sample.match_id != 0 {
                    info!(
                        "🎾 Tennis Raptor [{}]: sets {}-{} games {}-{} pts {} server={}{}{} live={}",
                        sample.match_label, sample.sets_p1, sample.sets_p2,
                        sample.games_p1, sample.games_p2, sample.points_label, sample.server,
                        if sample.break_point { " BREAK-POINT" } else { "" },
                        if sample.is_tiebreak { " TIEBREAK" } else { "" },
                        sample.num_live,
                    );
                } else {
                    debug!("🎾 Tennis Raptor: no live matches (feed healthy; snapshot neutral)");
                }
            }
            Err(reason) => {
                consecutive_failures += 1;
                raptor_health_tx.send_modify(|map| {
                    map.entry(TENNIS_HEALTH_KEY.to_string()).or_default().tennis_connected = false;
                });
                if consecutive_failures == 1 {
                    warn!("⚠️ Tennis Raptor poll failed: {reason} (will retry silently; signal treated as neutral)");
                } else {
                    debug!("🎾 Tennis Raptor unavailable (attempt {}): {reason}", consecutive_failures);
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(config::TENNIS_POLL_SECS)).await;
    }
}

/// Build the live-matches poll URL, with the optional tour filter
/// (`atp` / `wta` / `challenger` / `itf` / `juniors`; empty = all tours).
/// The API key travels in the `Authorization: Bearer` header, never the URL.
fn live_matches_url(tour: &str) -> String {
    let base = "https://api.livetennisapi.com/api/public/v1/matches?status=live";
    if tour.is_empty() {
        base.to_string()
    } else {
        format!("{base}&tour={tour}")
    }
}

/// Parsed event state for the tracked live match in a poll response.
#[derive(Debug)]
struct TennisSample {
    /// Tracked match id (`0` = none tracked, e.g. nothing live).
    match_id: i64,
    /// Human label, e.g. "C. Alcaraz vs J. Sinner".
    match_label: String,
    /// Tournament name from the feed.
    tournament: String,
    /// Tour ("atp"/"wta"/…), empty when the feed never stated one.
    tour: String,
    /// In-game points as the feed's tennis strings, e.g. "30–40" or "AD–40".
    points_label: String,
    /// ISO-8601 UTC timestamp of the tracked score (empty when absent).
    score_at: String,
    num_live: u32,
    sets_p1: u32,
    sets_p2: u32,
    games_p1: u32,
    games_p2: u32,
    /// Serving side (1/2; 0 = unknown).
    server: u32,
    break_point: bool,
    is_tiebreak: bool,
    /// Age of the score timestamp in seconds (`-1` = unknown).
    feed_age_secs: i64,
    /// True when the tracked score is older than `TENNIS_SCORE_STALENESS_SECS`.
    stale: bool,
}

/// Fetch the live-match list and reduce it to the tracked match's event state.
///
/// Returns `Err(reason)` on any failure so the caller can log *why* the poll
/// produced no signal (bad key, quota exhausted, error payload, bad JSON, …).
async fn try_fetch_live_state(
    http: &reqwest::Client,
    url: &str,
    api_key: &str,
    tracked: Option<i64>,
) -> Result<TennisSample, String> {
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(8),
        http.get(url).bearer_auth(api_key).send(),
    )
    .await
    .map_err(|_| "request timed out after 8s".to_string())?
    .map_err(|e| format!("transport error: {e}"))?;

    let status = resp.status();
    // The API documents X-RateLimit-* headers on every response (the daily
    // quota itself lives on GET /usage, which is quota-exempt). Read the
    // remaining count opportunistically so budget draw-down is visible in the
    // logs and the free-tier cap never arrives as a surprise 429.
    let remaining = resp
        .headers()
        .get("x-ratelimit-remaining")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<i64>().ok());
    match remaining {
        Some(r) if r <= config::TENNIS_LOW_BUDGET_WARN => warn!(
            "⚠️ Tennis Raptor: Live Tennis API budget low — {r} requests remaining in the current window. \
             Consider raising TENNIS_POLL_SECS (free tier: 30 req/min, 100 req/day)."
        ),
        Some(r) => debug!("🎾 Tennis Raptor budget: {r} requests remaining in the current window"),
        None => {}
    }
    let body = resp.text().await.map_err(|e| format!("failed reading body: {e}"))?;
    if !status.is_success() {
        // Errors arrive as JSON `{ "error": code, "detail": ... }` — surface a
        // truncated snippet so 401 (bad key) / 403 (upgrade_required) / 429
        // (rate_limited / daily quota) are obvious in the logs.
        let snippet: String = body.chars().take(200).collect();
        return Err(format!("HTTP {status}: {snippet}"));
    }

    reduce_live_matches(&body, chrono::Utc::now(), tracked)
}

/// Reduce a `GET /matches?status=live` response body to the tracked match's
/// event state. Pure (no I/O) so the parse/derivation logic is unit-testable.
fn reduce_live_matches(
    body: &str,
    now: chrono::DateTime<chrono::Utc>,
    tracked: Option<i64>,
) -> Result<TennisSample, String> {
    let parsed: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("invalid JSON: {e}"))?;
    // List endpoints return `{data, meta}`; an error object carries `error`.
    if let Some(code) = parsed.get("error").and_then(|e| e.as_str()) {
        let detail = parsed.get("detail").and_then(|d| d.as_str()).unwrap_or("");
        return Err(format!("API error '{code}': {detail}"));
    }
    let matches = parsed
        .get("data")
        .and_then(|d| d.as_array())
        .ok_or_else(|| {
            let snippet: String = body.chars().take(200).collect();
            format!("expected {{data: [...]}} match list, got: {snippet}")
        })?;

    let num_live = matches.len() as u32;

    // Neutral (but healthy) sample when nothing is on court — tennis has quiet
    // hours every day, so an empty live list is a real state, not a failure.
    let neutral = |num_live: u32| TennisSample {
        match_id: 0,
        match_label: String::new(),
        tournament: String::new(),
        tour: String::new(),
        points_label: String::new(),
        score_at: String::new(),
        num_live,
        sets_p1: 0, sets_p2: 0, games_p1: 0, games_p2: 0,
        server: 0,
        break_point: false,
        is_tiebreak: false,
        feed_age_secs: -1,
        stale: false,
    };
    if matches.is_empty() {
        return Ok(neutral(0));
    }

    // Pick the tracked match: sticky on the previously tracked id while it is
    // still in the live list (so state reads as one continuous match), else the
    // match with the freshest score timestamp (ties broken by lowest id —
    // timestamps are ISO-8601 UTC "Z", so lexical order == chronological order).
    // Matches whose `score` is null carry no event state and are not selectable.
    let still_tracked = tracked.and_then(|id| {
        matches
            .iter()
            .find(|m| match_id_of(m) == Some(id) && m.get("score").is_some_and(|s| s.is_object()))
    });
    let event = still_tracked.or_else(|| {
        matches
            .iter()
            .filter(|m| m.get("score").is_some_and(|s| s.is_object()) && match_id_of(m).is_some())
            .max_by_key(|m| {
                let ts = score_timestamp(m).unwrap_or_default().to_string();
                (ts, std::cmp::Reverse(match_id_of(m).unwrap_or(i64::MAX)))
            })
    });
    let Some(event) = event else {
        // Live matches listed but none carries a score object yet.
        return Ok(neutral(num_live));
    };

    let match_id = match_id_of(event).ok_or_else(|| "tracked match missing id".to_string())?;
    let p1 = player_name(event, "p1");
    let p2 = player_name(event, "p2");
    let match_label = format!("{p1} vs {p2}");
    let tournament = event
        .get("tournament")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();
    let tour = event.get("tour").and_then(|t| t.as_str()).unwrap_or("").to_string();

    let score = event
        .get("score")
        .ok_or_else(|| format!("match '{match_label}' has no score object"))?;

    // Sets won: `score.sets` is `[sets_p1, sets_p2]`.
    let (sets_p1, sets_p2) = int_pair(score.get("sets"));
    // Games: `score.games` is `[[per-set games p1], [per-set games p2]]` — the
    // current set is the LAST entry of each list. Completed matches have been
    // observed live with empty games arrays; read those as 0, never panic.
    let games_p1 = last_game_count(score.get("games"), 0);
    let games_p2 = last_game_count(score.get("games"), 1);

    // In-game points arrive as tennis strings ("0", "15", "30", "40", "AD");
    // entries can be NULL per the API spec, so decode defensively.
    let pts_p1 = point_str(score.get("points"), 0);
    let pts_p2 = point_str(score.get("points"), 1);
    let server = score
        .get("server")
        .and_then(|s| s.as_u64())
        .filter(|s| *s == 1 || *s == 2)
        .unwrap_or(0) as u32;
    let is_tiebreak = score.get("is_tiebreak").and_then(|t| t.as_bool()).unwrap_or(false);
    let break_point = is_break_point(server, pts_p1.as_deref(), pts_p2.as_deref(), is_tiebreak);
    let points_label = match (&pts_p1, &pts_p2) {
        (Some(a), Some(b)) => format!("{a}–{b}"),
        _ => String::new(),
    };

    // Staleness: the score's `timestamp` is the API's own last-change instant.
    let score_at = score
        .get("timestamp")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();
    let feed_age_secs = chrono::DateTime::parse_from_rfc3339(&score_at)
        .map(|t| (now - t.with_timezone(&chrono::Utc)).num_seconds().max(0))
        .unwrap_or(-1);
    let stale = feed_age_secs > config::TENNIS_SCORE_STALENESS_SECS as i64;

    Ok(TennisSample {
        match_id,
        match_label,
        tournament,
        tour,
        points_label,
        score_at,
        num_live,
        sets_p1,
        sets_p2,
        games_p1,
        games_p2,
        server,
        break_point,
        is_tiebreak,
        feed_age_secs,
        stale,
    })
}

/// Break-point derivation from the feed's tennis point strings.
///
/// The receiver holds a break point when they are one point from winning the
/// server's service game: receiver at `AD`, or receiver at `40` while the
/// server is below 40 (`0`/`15`/`30`). Deuce (40–40) and server-advantage are
/// NOT break points, and a tiebreak never is — there is no service game to
/// break (mini-breaks are a different concept, deliberately not derived here).
/// Unknown server or missing point strings read as `false`, never a guess.
fn is_break_point(server: u32, pts_p1: Option<&str>, pts_p2: Option<&str>, is_tiebreak: bool) -> bool {
    if is_tiebreak {
        return false;
    }
    let (Some(p1), Some(p2)) = (pts_p1, pts_p2) else { return false };
    let (server_pts, receiver_pts) = match server {
        1 => (p1, p2),
        2 => (p2, p1),
        _ => return false,
    };
    match (server_pts, receiver_pts) {
        (_, "AD") => true,
        (s, "40") => matches!(s, "0" | "15" | "30"),
        _ => false,
    }
}

/// The match's integer `id`, when present.
fn match_id_of(m: &serde_json::Value) -> Option<i64> {
    m.get("id").and_then(|i| i.as_i64())
}

/// The score's ISO-8601 `timestamp` string, when present.
fn score_timestamp(m: &serde_json::Value) -> Option<&str> {
    m.get("score")?.get("timestamp")?.as_str()
}

/// A player's display name from `players.p1` / `players.p2`.
fn player_name(m: &serde_json::Value, side: &str) -> String {
    m.get("players")
        .and_then(|p| p.get(side))
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())
        .unwrap_or("?")
        .to_string()
}

/// Decode a `[a, b]` integer pair (e.g. `score.sets`), defaulting to 0.
fn int_pair(v: Option<&serde_json::Value>) -> (u32, u32) {
    let get = |i: usize| {
        v.and_then(|a| a.as_array())
            .and_then(|a| a.get(i))
            .and_then(|n| n.as_u64())
            .unwrap_or(0) as u32
    };
    (get(0), get(1))
}

/// Games won in the current set for one side: the LAST entry of that side's
/// per-set list in `score.games`. Empty/missing lists read as 0.
fn last_game_count(games: Option<&serde_json::Value>, side: usize) -> u32 {
    games
        .and_then(|g| g.as_array())
        .and_then(|g| g.get(side))
        .and_then(|s| s.as_array())
        .and_then(|s| s.last())
        .and_then(|n| n.as_u64())
        .unwrap_or(0) as u32
}

/// One side's in-game point string from `score.points`; entries can be NULL.
fn point_str(points: Option<&serde_json::Value>, side: usize) -> Option<String> {
    points
        .and_then(|p| p.as_array())
        .and_then(|p| p.get(side))
        .and_then(|s| s.as_str())
        .map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    /// A live `/matches?status=live` body with `n` matches. Match ids, score
    /// timestamps and point states are taken by the callers below.
    fn body_with(matches: &[serde_json::Value]) -> String {
        serde_json::json!({
            "data": matches,
            "meta": { "limit": 50, "offset": 0, "count": matches.len(), "total": matches.len(), "has_more": false }
        })
        .to_string()
    }

    fn live_match(
        id: i64,
        ts: &str,
        server: serde_json::Value,
        points: serde_json::Value,
        is_tiebreak: bool,
    ) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "tournament": "Cincinnati Open",
            "tour": "atp",
            "status": "live",
            "is_doubles": false,
            "players": {
                "p1": { "id": 10, "name": "C. Alcaraz" },
                "p2": { "id": 11, "name": "J. Sinner" }
            },
            "score": {
                "sets": [1, 0],
                "games": [[6, 3], [4, 5]],
                "points": points,
                "server": server,
                "is_tiebreak": is_tiebreak,
                "timestamp": ts
            }
        })
    }

    fn now() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 16, 14, 0, 30).unwrap()
    }

    #[test]
    fn parses_live_match_state() {
        let body = body_with(&[live_match(
            42,
            "2026-08-16T14:00:10Z",
            serde_json::json!(1),
            serde_json::json!(["30", "40"]),
            false,
        )]);
        let s = reduce_live_matches(&body, now(), None).unwrap();
        assert_eq!(s.match_id, 42);
        assert_eq!(s.match_label, "C. Alcaraz vs J. Sinner");
        assert_eq!(s.tournament, "Cincinnati Open");
        assert_eq!(s.tour, "atp");
        assert_eq!(s.num_live, 1);
        assert_eq!((s.sets_p1, s.sets_p2), (1, 0));
        // Current set = LAST entry of each per-set games list.
        assert_eq!((s.games_p1, s.games_p2), (3, 5));
        assert_eq!(s.server, 1);
        assert_eq!(s.points_label, "30–40");
        // Server 1 at 30, receiver (p2) at 40 → break point.
        assert!(s.break_point);
        assert!(!s.is_tiebreak);
        assert_eq!(s.feed_age_secs, 20);
        assert!(!s.stale);
    }

    /// One break-point derivation case: (server, p1 pts, p2 pts, tiebreak, expected).
    type BpCase = (u32, Option<&'static str>, Option<&'static str>, bool, bool);

    #[test]
    fn break_point_matrix() {
        let cases: &[BpCase] = &[
            // Receiver at 40 vs server below 40 → BP.
            (1, Some("0"),  Some("40"), false, true),
            (1, Some("15"), Some("40"), false, true),
            (1, Some("30"), Some("40"), false, true),
            (2, Some("40"), Some("15"), false, true),
            // Deuce is NOT a break point.
            (1, Some("40"), Some("40"), false, false),
            // Receiver advantage → BP; server advantage → not.
            (1, Some("40"), Some("AD"), false, true),
            (1, Some("AD"), Some("40"), false, false),
            (2, Some("AD"), Some("40"), false, true),
            // Server ahead or level below 40 → not.
            (1, Some("40"), Some("30"), false, false),
            (1, Some("15"), Some("15"), false, false),
            // Never in a tiebreak, even with BP-shaped strings.
            (1, Some("30"), Some("40"), true,  false),
            // Unknown server / missing points → never a guess.
            (0, Some("30"), Some("40"), false, false),
            (1, None,       Some("40"), false, false),
            (1, Some("30"), None,       false, false),
        ];
        for &(server, p1, p2, tb, expected) in cases {
            assert_eq!(
                is_break_point(server, p1, p2, tb),
                expected,
                "server={server} p1={p1:?} p2={p2:?} tiebreak={tb}"
            );
        }
    }

    #[test]
    fn tiebreak_state_is_reported_but_never_a_break_point() {
        let body = body_with(&[live_match(
            7,
            "2026-08-16T14:00:25Z",
            serde_json::json!(2),
            serde_json::json!(["6", "5"]),
            true,
        )]);
        let s = reduce_live_matches(&body, now(), None).unwrap();
        assert!(s.is_tiebreak);
        assert!(!s.break_point);
        assert_eq!(s.points_label, "6–5");
    }

    #[test]
    fn null_points_and_empty_games_do_not_panic() {
        // Observed live on completed matches: null point entries + empty games.
        let mut m = live_match(9, "2026-08-16T13:00:00Z", serde_json::Value::Null, serde_json::json!([null, null]), false);
        m["score"]["games"] = serde_json::json!([[], []]);
        let s = reduce_live_matches(&body_with(&[m]), now(), None).unwrap();
        assert_eq!((s.games_p1, s.games_p2), (0, 0));
        assert_eq!(s.server, 0);
        assert!(!s.break_point);
        assert_eq!(s.points_label, "");
    }

    #[test]
    fn tracks_freshest_score_then_sticks_to_it() {
        let older = live_match(1, "2026-08-16T13:59:00Z", serde_json::json!(1), serde_json::json!(["0", "0"]), false);
        let fresher = live_match(2, "2026-08-16T14:00:20Z", serde_json::json!(2), serde_json::json!(["15", "30"]), false);
        let body = body_with(&[older.clone(), fresher.clone()]);

        // No prior tracked match → the freshest score wins.
        let s = reduce_live_matches(&body, now(), None).unwrap();
        assert_eq!(s.match_id, 2);
        assert_eq!(s.num_live, 2);

        // A previously tracked match stays tracked even when another is fresher.
        let s = reduce_live_matches(&body, now(), Some(1)).unwrap();
        assert_eq!(s.match_id, 1);

        // Tracked match gone from the live list → rotate to the freshest.
        let body_rotated = body_with(&[fresher]);
        let s = reduce_live_matches(&body_rotated, now(), Some(1)).unwrap();
        assert_eq!(s.match_id, 2);
    }

    #[test]
    fn stale_score_is_flagged_not_hidden() {
        // Score last moved 20 minutes ago — beyond TENNIS_SCORE_STALENESS_SECS.
        let body = body_with(&[live_match(
            3,
            "2026-08-16T13:40:30Z",
            serde_json::json!(1),
            serde_json::json!(["15", "0"]),
            false,
        )]);
        let s = reduce_live_matches(&body, now(), None).unwrap();
        assert_eq!(s.feed_age_secs, 1200);
        assert!(s.stale);
        // The state itself is still published alongside the stale flag.
        assert_eq!(s.match_id, 3);
    }

    #[test]
    fn missing_timestamp_reads_as_unknown_age_not_stale() {
        let mut m = live_match(4, "", serde_json::json!(1), serde_json::json!(["0", "0"]), false);
        m["score"]["timestamp"] = serde_json::Value::Null;
        let s = reduce_live_matches(&body_with(&[m]), now(), None).unwrap();
        assert_eq!(s.feed_age_secs, -1);
        assert!(!s.stale);
    }

    #[test]
    fn empty_live_list_is_healthy_neutral() {
        let s = reduce_live_matches(&body_with(&[]), now(), None).unwrap();
        assert_eq!(s.num_live, 0);
        assert_eq!(s.match_id, 0);
        assert!(!s.stale);
        assert_eq!(s.feed_age_secs, -1);
    }

    #[test]
    fn api_error_object_is_surfaced() {
        let body = r#"{"error":"rate_limited","detail":"Daily quota exhausted"}"#;
        let err = reduce_live_matches(body, now(), None).unwrap_err();
        assert!(err.contains("rate_limited"), "got: {err}");
        assert!(err.contains("Daily quota exhausted"), "got: {err}");
    }

    #[test]
    fn malformed_bodies_are_errors_not_panics() {
        assert!(reduce_live_matches("not json", now(), None).is_err());
        assert!(reduce_live_matches(r#"{"unexpected": true}"#, now(), None).is_err());
        assert!(reduce_live_matches(r#"[1,2,3]"#, now(), None).is_err());
    }

    #[test]
    fn live_url_carries_optional_tour_filter() {
        assert_eq!(
            live_matches_url(""),
            "https://api.livetennisapi.com/api/public/v1/matches?status=live"
        );
        assert_eq!(
            live_matches_url("wta"),
            "https://api.livetennisapi.com/api/public/v1/matches?status=live&tour=wta"
        );
    }
}
