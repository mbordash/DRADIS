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

/// Sports Raptor — betting line-movement & consensus-probability signal.
///
/// A *macro*, venue-neutral Raptor. Where the crypto Raptors read Binance
/// microstructure, the Sports Raptor reads the **public betting market**: how the
/// consensus moneyline for the nearest upcoming event is drifting across
/// sportsbooks, and how much the books disagree (a soft-line proxy). It is the
/// first non-crypto Raptor and is shared identically by the US and intl squadron
/// pipelines — nothing in it is asset- or venue-specific.
///
/// ── Source ──────────────────────────────────────────────────────────────────
/// The Odds API (the-odds-api.com) free tier, keyed on env `ODDS_API_KEY`. Each
/// poll fetches head-to-head (moneyline) odds for the configured sport across US
/// bookmakers, picks the **nearest-commencing event**, and derives:
///
/// │ Field            │ Derivation                                              │
/// │──────────────────│─────────────────────────────────────────────────────────│
/// │ consensus_prob   │ vig-free implied prob of the reference (first) outcome   │
/// │ line_drift       │ Δ consensus_prob vs the previous poll (same event id)    │
/// │ book_dispersion  │ max−min of per-book implied probs (0..1); book disagreement│
/// │ num_books        │ bookmakers contributing to the sample (0 = no data)     │
///
/// Per book the two-way vig is removed by normalizing the raw implied probs
/// (`1/decimal_odds`) so they sum to 1; the reference-outcome prob is then
/// averaged across books for the consensus.
///
/// ── Observe-only status ─────────────────────────────────────────────────────
/// Wired in **observe-only** mode exactly like the Tide Raptor: it publishes to
/// telemetry but is NOT consumed by any Viper sizing yet. Without `ODDS_API_KEY`
/// (or when the API is unreachable) it degrades silently to its `Default`
/// (all-zero, `sports_connected = false`); consumers treat a zero snapshot as
/// neutral. Telemetry is published under the fixed `"sports"` health-map key.
use std::collections::HashMap;
use std::sync::Arc;

use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal_macros::dec;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::api::server::AssetRaptorHealth;
use crate::config;

/// Fixed health-map key under which the (venue-neutral) Sports Raptor publishes
/// its telemetry, alongside the per-asset crypto entries ("btc"/"eth"/…).
pub const SPORTS_HEALTH_KEY: &str = "sports";

/// Normalized sports-market snapshot broadcast to every consuming Squadron.
///
/// `Copy` so the `watch` channel hands out cheap value clones, and `Default`
/// (all-zero, `num_books = 0` meaning "no data") so the channel can be seeded
/// before the first successful poll and off-source reads are unambiguous.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct SportsSnapshot {
    /// Vig-free consensus implied probability of the tracked event's reference
    /// (first-listed) outcome, in `[0, 1]`. `0` when no data.
    pub consensus_prob: Decimal,
    /// Change in `consensus_prob` since the previous poll for the **same** event
    /// (matched by id). Signed: `>0` the line is firming toward the reference
    /// outcome, `<0` drifting away. `0` on the first poll or on event rotation.
    pub line_drift: Decimal,
    /// Spread of per-book implied probabilities for the reference outcome
    /// (`max − min`), in `[0, 1]`. High = soft/disagreeing line. `0` when <2 books.
    pub book_dispersion: Decimal,
    /// Number of bookmakers contributing to this sample. `0` = no data (treated
    /// as neutral by any future consumer).
    pub num_books: Decimal,
}

pub async fn run_sports_raptor(
    http: Arc<reqwest::Client>,
    sports_tx: watch::Sender<SportsSnapshot>,
    raptor_health_tx: Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
    mut config_rx: watch::Receiver<Arc<crate::helpers::dynamic_config::DynamicConfig>>,
) {
    let api_key = std::env::var(config::SPORTS_ODDS_KEY_ENV).ok().filter(|k| !k.is_empty());
    let Some(api_key) = api_key else {
        info!(
            "🏈 Sports Raptor idle — {} not set (observe-only; no line-movement signal)",
            config::SPORTS_ODDS_KEY_ENV
        );
        // Seed a neutral snapshot + offline telemetry, then park. Receivers stay
        // valid; consumers read a zero snapshot as neutral.
        let _ = sports_tx.send(SportsSnapshot::default());
        raptor_health_tx.send_modify(|map| {
            map.entry(SPORTS_HEALTH_KEY.to_string()).or_default().sports_connected = false;
        });
        std::future::pending::<()>().await;
        return;
    };

    // The sport/region selectors are live config, so the URL is rebuilt each
    // cycle rather than once here — changing the sport must not require a
    // restart, and a stale URL would keep polling the previous feed.
    let odds_url = |sport: &str, regions: &str| format!(
        "https://api.the-odds-api.com/v4/sports/{sport}/odds?regions={regions}\
&markets=h2h&oddsFormat=decimal&apiKey={api_key}",
    );

    // Track the last consensus per event id so `line_drift` measures movement on
    // the SAME event and resets cleanly when the nearest event rotates.
    let mut prev_event: Option<(String, Decimal)> = None;
    let mut consecutive_failures: u32 = 0;

    loop {
        // Re-read each cycle so a budget-warning change applies without a restart.
        let (low_budget_warn, sport, regions) = {
            let c = config_rx.borrow();
            (c.sports_low_budget_warn, c.sports_odds_sport.clone(), c.sports_odds_regions.clone())
        };
        let url = odds_url(&sport, &regions);
        match try_fetch_nearest_event(&http, &url, low_budget_warn).await {
            Ok(sample) => {
                consecutive_failures = 0;
                let line_drift = match &prev_event {
                    Some((id, prev_prob)) if *id == sample.event_id => {
                        sample.consensus_prob - *prev_prob
                    }
                    _ => dec!(0),
                };
                prev_event = Some((sample.event_id.clone(), sample.consensus_prob));

                let snap = SportsSnapshot {
                    consensus_prob: sample.consensus_prob,
                    line_drift,
                    book_dispersion: sample.book_dispersion,
                    num_books: Decimal::from(sample.num_books),
                };
                let _ = sports_tx.send(snap);
                raptor_health_tx.send_modify(|map| {
                    let h = map.entry(SPORTS_HEALTH_KEY.to_string()).or_default();
                    h.sports_connected      = true;
                    h.sports_consensus_prob = snap.consensus_prob;
                    h.sports_line_drift     = snap.line_drift;
                    h.sports_book_dispersion = snap.book_dispersion;
                    h.sports_num_books      = snap.num_books;
                    h.sports_event          = sample.event_label.clone();
                    h.sports_reference      = sample.ref_outcome.clone();
                    h.sports_sport          = sample.sport_title.clone();
                    h.sports_commence       = sample.commence_time.clone();
                    h.sports_books          = sample.book_names.join(", ");
                });
                info!(
                    "🏈 Sports Raptor [{}]: consensus={:.3} drift={:+.3} dispersion={:.3} books={}",
                    sample.event_label, snap.consensus_prob, snap.line_drift,
                    snap.book_dispersion, sample.num_books,
                );
            }
            Err(reason) => {
                consecutive_failures += 1;
                raptor_health_tx.send_modify(|map| {
                    map.entry(SPORTS_HEALTH_KEY.to_string()).or_default().sports_connected = false;
                });
                if consecutive_failures == 1 {
                    warn!("⚠️ Sports Raptor poll failed: {reason} (will retry silently; signal treated as neutral)");
                } else {
                    debug!("🏈 Sports Raptor unavailable (attempt {}): {reason}", consecutive_failures);
                }
            }
        }
        // Sleep the CURRENT configured interval, waking early if it changes —
        // otherwise a new value waits out the remainder of the old sleep, which
        // on this feed's multi-minute cadence reads as a setting that did nothing.
        let poll_secs = config_rx.borrow().sports_poll_secs.max(1);
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(poll_secs)) => {}
            _ = config_rx.changed() => {
                let next = config_rx.borrow().sports_poll_secs.max(1);
                if next != poll_secs {
                    info!("🏈 Sports Raptor poll interval {poll_secs}s → {next}s (applies now)");
                }
            }
        }
    }
}

/// Parsed consensus for the nearest-commencing event in a poll response.
struct EventSample {
    event_id: String,
    event_label: String,
    /// The outcome the consensus/drift track (first-listed h2h outcome).
    ref_outcome: String,
    /// Human sport title from the feed (e.g. "MLB", "NFL"); "upcoming" mixes sports.
    sport_title: String,
    /// ISO-8601 UTC kickoff time of the tracked event.
    commence_time: String,
    /// Bookmaker titles contributing to the consensus (e.g. "DraftKings, FanDuel").
    book_names: Vec<String>,
    consensus_prob: Decimal,
    book_dispersion: Decimal,
    num_books: u32,
}

/// Fetch the configured sport's h2h odds and reduce the nearest-commencing event
/// to a vig-free consensus for its reference (first-listed) outcome.
///
/// Returns `Err(reason)` on any failure so the caller can log *why* the poll
/// produced no signal (bad key, quota exhausted, error payload, no events, …).
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
fn redact_url_secrets(s: &str) -> String {
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

async fn try_fetch_nearest_event(http: &reqwest::Client, url: &str, low_budget_warn: i64) -> Result<EventSample, String> {
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(8),
        http.get(url).send(),
    )
    .await
    .map_err(|_| "request timed out after 8s".to_string())?
    // `reqwest::Error` renders the URL it was fetching, and this provider takes
    // its credential as `?apiKey=...` — so the raw Display leaks the operator's
    // key into `dradis.log`, which is the file that gets pulled to laptops and
    // pasted into support threads. Redact before the message can be logged.
    .map_err(|e| format!("transport error: {}", redact_url_secrets(&e.to_string())))?;

    let status = resp.status();
    // The Odds API returns the remaining monthly quota on every response. Capture
    // it before consuming the body so we can log budget draw-down and warn loudly
    // before the free-tier cap is hit.
    let remaining = resp
        .headers()
        .get("x-requests-remaining")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<f64>().ok())
        .map(|f| f as i64);
    let used = resp
        .headers()
        .get("x-requests-used")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<f64>().ok())
        .map(|f| f as i64);
    match remaining {
        Some(r) if r <= low_budget_warn => warn!(
            "⚠️ Sports Raptor: The Odds API budget low — {r} requests remaining (used {}). Raise the Sports poll interval in Setup → Raptor Signal Sources.",
            used.map(|u| u.to_string()).unwrap_or_else(|| "?".into()),
        ),
        Some(r) => debug!(
            "🏈 Sports Raptor budget: {r} requests remaining (used {})",
            used.map(|u| u.to_string()).unwrap_or_else(|| "?".into()),
        ),
        None => {}
    }
    let body = resp.text().await.map_err(|e| format!("failed reading body: {e}"))?;
    if !status.is_success() {
        // The Odds API returns a JSON `{ "message": ... }` on error — surface it,
        // truncated, so 401 (bad key) / 429 (quota) are obvious in the logs.
        let snippet: String = body.chars().take(200).collect();
        return Err(format!("HTTP {status}: {snippet}"));
    }

    let events: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| format!("invalid JSON: {e}"))?;
    let events = events
        .as_array()
        .ok_or_else(|| {
            let snippet: String = body.chars().take(200).collect();
            format!("expected a JSON array of events, got: {snippet}")
        })?;
    if events.is_empty() {
        return Err("no upcoming events returned for the configured sport".to_string());
    }

    // Pick the nearest-commencing event that actually has priced h2h odds. The
    // Odds API commonly returns the very nearest events with an empty
    // `bookmakers: []` (books pulled the line at/near start), so selecting purely
    // by time and then requiring odds would fail spuriously. Filter to events
    // that carry ≥1 bookmaker with an h2h market first, then take the soonest.
    // Times are ISO-8601 UTC ("Z") so lexical order == chronological order.
    let event = events
        .iter()
        .filter(|e| e.get("commence_time").and_then(|t| t.as_str()).is_some())
        .filter(|e| {
            e.get("bookmakers")
                .and_then(|b| b.as_array())
                .map(|books| books.iter().any(|b| h2h_outcomes(b).is_some()))
                .unwrap_or(false)
        })
        .min_by(|a, b| {
            let ta = a.get("commence_time").and_then(|t| t.as_str()).unwrap_or("");
            let tb = b.get("commence_time").and_then(|t| t.as_str()).unwrap_or("");
            ta.cmp(tb)
        })
        .ok_or_else(|| {
            format!("{} events returned but none have priced h2h odds yet", events.len())
        })?;

    let event_id = event
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "nearest event missing id".to_string())?
        .to_string();
    let home = event.get("home_team").and_then(|v| v.as_str()).unwrap_or("home");
    let away = event.get("away_team").and_then(|v| v.as_str()).unwrap_or("away");
    let event_label = format!("{} vs {}", home, away);
    let sport_title = event
        .get("sport_title")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let commence_time = event
        .get("commence_time")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // The reference outcome is the first outcome listed in a book's h2h market.
    // Determine its name from the first book, then read the SAME name across all
    // books for a like-for-like consensus.
    let books = event
        .get("bookmakers")
        .and_then(|v| v.as_array())
        .filter(|b| !b.is_empty())
        .ok_or_else(|| format!("event '{event_label}' has no bookmakers (h2h not yet priced)"))?;
    let ref_name = first_h2h_outcome_name(books)
        .ok_or_else(|| format!("event '{event_label}' has no h2h market outcomes"))?;

    let mut probs: Vec<Decimal> = Vec::new();
    let mut book_names: Vec<String> = Vec::new();
    for book in books {
        if let Some(p) = vig_free_prob_for(book, &ref_name) {
            probs.push(p);
            if let Some(name) = book.get("title").and_then(|v| v.as_str()) {
                book_names.push(name.to_string());
            }
        }
    }
    if probs.is_empty() {
        return Err(format!("event '{event_label}': no usable h2h odds across {} books", books.len()));
    }

    let num_books = probs.len() as u32;
    let sum: Decimal = probs.iter().copied().sum();
    let consensus_prob = sum / Decimal::from(num_books);
    let max = probs.iter().copied().max().unwrap_or(dec!(0));
    let min = probs.iter().copied().min().unwrap_or(dec!(0));
    let book_dispersion = max - min;

    Ok(EventSample {
        event_id,
        event_label,
        ref_outcome: ref_name,
        sport_title,
        commence_time,
        book_names,
        consensus_prob,
        book_dispersion,
        num_books,
    })
}

/// Name of the first outcome in the first book's h2h market — the reference
/// outcome we track for consensus and drift.
fn first_h2h_outcome_name(books: &[serde_json::Value]) -> Option<String> {
    for book in books {
        if let Some(outcomes) = h2h_outcomes(book) {
            if let Some(name) = outcomes.first().and_then(|o| o.get("name")).and_then(|n| n.as_str()) {
                return Some(name.to_string());
            }
        }
    }
    None
}

/// Vig-free implied probability of `ref_name` within a single book's two-way
/// h2h market. Removes the overround by normalizing both raw `1/odds` implied
/// probs to sum to 1. Returns `None` if the book lacks the outcome or has bad odds.
fn vig_free_prob_for(book: &serde_json::Value, ref_name: &str) -> Option<Decimal> {
    let outcomes = h2h_outcomes(book)?;
    let mut raw: Vec<(String, Decimal)> = Vec::new();
    for o in outcomes {
        let name = o.get("name").and_then(|n| n.as_str())?;
        let odds = o.get("price").and_then(|p| p.as_f64()).filter(|p| *p > 1.0)?;
        let implied = Decimal::from_f64(1.0 / odds)?;
        raw.push((name.to_string(), implied));
    }
    if raw.len() < 2 {
        return None;
    }
    let total: Decimal = raw.iter().map(|(_, p)| *p).sum();
    if total <= dec!(0) {
        return None;
    }
    raw.iter()
        .find(|(name, _)| name == ref_name)
        .map(|(_, p)| *p / total)
}

/// Extract a book's `h2h` market outcomes array, if present.
fn h2h_outcomes(book: &serde_json::Value) -> Option<&Vec<serde_json::Value>> {
    book.get("markets")
        .and_then(|m| m.as_array())?
        .iter()
        .find(|m| m.get("key").and_then(|k| k.as_str()) == Some("h2h"))?
        .get("outcomes")
        .and_then(|o| o.as_array())
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
