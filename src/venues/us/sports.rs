// DRADIS — Polymarket US sports catalog
//
// This file is part of DRADIS and is licensed under the terms in LICENSE.

//! Polymarket US's implementation of [`SportsCatalog`].
//!
//! ## The venue that suits this design best
//!
//! Of the three venues, this is where a resting bid has the most going for it:
//! Polymarket US pays the maker a **rebate at trade** (−0.0125 against a 0.06 taker
//! coefficient), so Bookline's break-even sits around −0.81 points against −0.69 on
//! Polymarket International and only −0.56 on a two-cent Kalshi book. The maker
//! leg is not merely free here, it earns.
//!
//! ## Why the market records make this easier than Kalshi
//!
//! Kalshi forced two awkward inferences: kick-off had to be parsed out of the
//! ticker (and NFL tickers carry no time at all, which silently dropped the whole
//! league until it was found), and team names arrive truncated to things like
//! `"Chicago C"`, which prefix-match equally well against Cubs and White Sox.
//!
//! Polymarket US sends the facts outright. Captured live from
//! `aec-nfl-lac-ten-2025-11-02`, each `marketSides` entry carries:
//!
//! ```text
//! long: true   description "Chargers"  team.name "Los Angeles Chargers"  ordering "away"
//! long: false  description "Titans"    team.name "Tennessee Titans"      ordering "home"
//! ```
//!
//! So three things are read rather than inferred. **Which side pays on which team**
//! comes from the side's own `team` object, not from slug order or a polarity
//! convention — get that wrong and every line on the venue is inverted, with the
//! board pricing the favorite's consensus against the underdog's book.
//! **`team.name` is the full name**, which is what The Odds API uses, so matching
//! is more reliable here than on Kalshi rather than less. And **`gameStartTime` is
//! a real timestamp**, so there is no ticker to parse and no expiry-minus-three-hours
//! estimate.
//!
//! `team.league` also arrives as the shared lowercase code (`"nfl"`), so unlike
//! Kalshi's `KXNCAAFGAME` there is no alias table to keep in step.
//!
//! ## What is signed
//!
//! Every read on this venue is signed — discovery and settlement both. There is no
//! public endpoint to fall back on, which is why this catalog holds the
//! authenticated venue rather than a bare HTTP client.

use super::types::UsMarket;
use super::UsRetailVenue;
use crate::raptors::sports_ledger::{
    match_games, MatchedGame, OddsEvent, PmGame, PmOutcome, SportsCatalog,
};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;

/// One side of a moneyline, as the gateway describes it.
#[derive(Debug, Clone, PartialEq)]
pub struct MoneylineSide {
    /// The venue's leg symbol, `{slug}#long` or `{slug}#short`.
    pub leg_id: String,
    /// Full team name, e.g. "Los Angeles Chargers" — what The Odds API uses.
    pub team: String,
    /// `"home"` or `"away"` when the gateway says; carried for diagnostics only.
    pub ordering: Option<String>,
}

/// Read both sides of a moneyline out of a raw gateway market record.
///
/// Pure, so the one fact the adapter cannot afford to get wrong is testable against
/// the captured record. Returns `None` unless exactly two tradable instrument sides
/// carry a team — a three-way market, a prop, or a record missing team objects is
/// not a moneyline this viper can price, and inventing a side would invert the line.
/// The two board keys for a market slug, in `(yes, no)` order.
///
/// Shared rather than formatted at each call site: the auto-deploy selector has to
/// look a game up under exactly the key the ledger wrote it under, and a format
/// derived twice is one that drifts silently -- the lookup simply misses and the
/// venue reads as having no board coverage at all.
pub fn board_leg_ids(slug: &str) -> (String, String) {
    (format!("{slug}#long"), format!("{slug}#short"))
}

pub fn moneyline_sides(market: &UsMarket) -> Option<[MoneylineSide; 2]> {
    let slug = market.slug.as_str();
    if slug.is_empty() { return None; }
    let sides = &market.market_sides;
    let mut out: Vec<MoneylineSide> = Vec::new();
    for s in sides {
        // Instrument sides only: the gateway also sends non-instrument rows.
        if let Some(t) = s.get("marketSideType").and_then(|v| v.as_str()) {
            if t != "MARKET_SIDE_TYPE_INSTRUMENT" { continue; }
        }
        let team = s.get("team").and_then(|t| t.get("name")).and_then(|v| v.as_str())
            // `description` is the short form ("Chargers") and the fallback when a
            // record carries no structured team. Never the slug: two teams share it.
            .or_else(|| s.get("description").and_then(|v| v.as_str()))?;
        let long = s.get("long").and_then(|v| v.as_bool())?;
        out.push(MoneylineSide {
            leg_id: {
                let (yes, no) = board_leg_ids(slug);
                if long { yes } else { no }
            },
            team: team.to_string(),
            ordering: s.get("team").and_then(|t| t.get("ordering"))
                .and_then(|v| v.as_str()).map(str::to_string),
        });
    }
    // Exactly two, and on opposite legs. Two sides both claiming `long` is a record
    // shape this code does not understand, and guessing which is which is the error
    // that inverts a venue.
    if out.len() != 2 { return None; }
    let longs = out.iter().filter(|s| s.leg_id.ends_with("#long")).count();
    if longs != 1 { return None; }
    let [a, b]: [MoneylineSide; 2] = out.try_into().ok()?;
    Some([a, b])
}

/// The ledger's league code for a market record.
///
/// Straight from `team.league`, which already arrives as the shared lowercase code
/// (`"nfl"`). No alias table: unlike Kalshi, the venue and the knob agree.
pub fn league_of(market: &UsMarket) -> Option<String> {
    market.market_sides.iter()
        .find_map(|s| s.get("team")?.get("league")?.as_str())
        .map(|l| l.to_ascii_lowercase())
}

/// Kick-off, straight from `gameStartTime`.
///
/// A real timestamp, so unlike Kalshi there is nothing to parse out of an
/// identifier and no game-duration estimate. `None` when the gateway omits it, and
/// the game is then dropped rather than dated from `endDate` — that field is the
/// settlement date, days after the game, and using it would put every game outside
/// the matcher's tolerance.
pub fn kickoff_of(market: &UsMarket) -> Option<DateTime<Utc>> {
    let raw = market.game_start_time.as_deref()?;
    DateTime::parse_from_rfc3339(raw).ok().map(|d| d.with_timezone(&Utc))
}

/// Polymarket US's catalog: its signed market listing, its BBO, its settlement record.
///
/// Connects on FIRST USE rather than at construction. The ledger is spawned before
/// the trading venue exists, and `UsRetailVenue::connect` is async while the venue
/// selector is not — so rather than restructure startup, the catalog holds the
/// client and dials in the first time it is asked a question. A failure to connect
/// leaves the catalog answering nothing, which reads as zero coverage in the
/// telemetry rather than as a crash at boot.
pub struct UsSportsCatalog {
    pub http: Arc<reqwest::Client>,
    pub venue: tokio::sync::OnceCell<Arc<UsRetailVenue>>,
}

impl UsSportsCatalog {
    pub fn new(http: Arc<reqwest::Client>) -> Self {
        Self { http, venue: tokio::sync::OnceCell::new() }
    }

    /// The connected venue, or `None` when the gateway cannot be reached or the
    /// credentials are absent.
    async fn venue(&self) -> Option<&Arc<UsRetailVenue>> {
        self.venue.get_or_try_init(|| async {
            UsRetailVenue::connect(Arc::clone(&self.http)).await.map(Arc::new)
        }).await.ok()
    }

    /// This venue's open moneyline records, through the trading wing's own listing.
    ///
    /// Deliberately NOT a `/v1/markets` query of its own. The first version of this
    /// method was exactly that -- `?categories=sports&limit=200&page=1&closed=false`
    /// -- and it found nothing: without the date window and `orderBy` the shared
    /// listing applies, page one of this venue's sports category is dominated by
    /// already-settled events, so the ledger reported "no Polymarket US moneylines"
    /// for all fourteen leagues at the same moment the trading wing was admitting 46
    /// of them. It also read the body without checking the status, so an auth failure
    /// would have looked identical to an empty slate.
    ///
    /// `min_volume` is `None`, meaning the venue's default floor -- the same thing the
    /// sports trading wing passes, and not the `Some(0.0)` the crypto wing needs.
    /// Dropping the floor here is actively harmful: this venue lists far more props,
    /// spreads and totals than moneylines (a census of one page came back 125 spreads,
    /// 61 props, 9 totals and not one moneyline), they all sit near zero volume, and
    /// with `orderBy=closed` they fill the early pages and push the game slate out of
    /// reach. The floor is what makes moneylines visible at all.
    async fn moneyline_records(&self) -> Vec<UsMarket> {
        let Some(venue) = self.venue().await else {
            tracing::warn!("🏈 Polymarket US sports: no venue connection — recording nothing");
            return Vec::new();
        };
        match venue.list_sports_category_markets(&["sports"], None).await {
            Ok(markets) => markets.into_iter()
                .filter(|m| !m.closed && m.market_type.eq_ignore_ascii_case("moneyline"))
                .collect(),
            Err(e) => {
                // Loud, because silence here is indistinguishable from an empty slate.
                tracing::warn!("🏈 Polymarket US sports listing failed: {e}");
                Vec::new()
            }
        }
    }
}

#[async_trait::async_trait]
impl SportsCatalog for UsSportsCatalog {
    async fn games(
        &self,
        http: &reqwest::Client,
        api_key: &str,
        leagues: &[(String, String)],
        _now: DateTime<Utc>,
    ) -> (Vec<MatchedGame>, Option<i64>) {
        let records = self.moneyline_records().await;
        let mut by_league: HashMap<String, Vec<PmGame>> = HashMap::new();
        for m in &records {
            let (Some(sides), Some(league), Some(commence)) =
                (moneyline_sides(m), league_of(m), kickoff_of(m)) else { continue };
            let slug = m.slug.clone();
            by_league.entry(league.clone()).or_default().push(PmGame {
                league,
                slug: slug.clone(),
                title: m.question.clone(),
                start: commence,
                outcomes: sides.iter().map(|s| PmOutcome {
                    label: s.team.clone(),
                    condition_id: slug.clone(),
                    token_id: s.leg_id.clone(),
                }).collect(),
            });
        }

        let mut out = Vec::new();
        let mut remaining = None;
        // Per-league coverage, for the reason both other catalogs log it: a league
        // that MATCHES nothing is indistinguishable from a league with no games once
        // summed into a total, and on Kalshi that hid two bugs for a day.
        let mut coverage: Vec<String> = Vec::new();
        for (code, sport_key) in leagues {
            let Some(games) = by_league.get(code) else {
                coverage.push(format!("{code} n/a (no Polymarket US moneylines)"));
                continue;
            };
            let (events, rem) =
                crate::raptors::sports_ledger::odds_events_for_sport(http, api_key, sport_key).await;
            if rem.is_some() { remaining = rem; }
            let events: Vec<OddsEvent> = events;
            let matched = match_games(games, &events, sport_key);
            coverage.push(format!("{code} {}/{}", matched.len(), games.len()));
            out.extend(matched);
        }
        if !coverage.is_empty() {
            tracing::info!(
                "🏈 Sports ledger coverage (matched/Polymarket US games): {}",
                coverage.join(" · "),
            );
        }
        (out, remaining)
    }

    async fn best_levels(
        &self,
        _http: &reqwest::Client,
        leg_id: &str,
    ) -> (Option<f64>, Option<f64>, Option<f64>, Option<f64>) {
        let Some(venue) = self.venue().await else { return (None, None, None, None) };

        // The gateway's BBO route addresses a MARKET by slug, and this receives
        // DRADIS's internal leg id, `{slug}#long` or `{slug}#short`. Passing the
        // leg id through is what broke this: a `#` opens a URL fragment, so the
        // signed path and the requested path differed and every call returned
        // 401. Strip the suffix.
        let (slug, side) = match leg_id.split_once('#') {
            Some((slug, side)) => (slug, side),
            // No suffix: treat the whole thing as a slug.
            None => (leg_id, "long"),
        };

        // One book per market, so only the long side can be read directly.
        //
        // The short leg's quote is NOT derived here. On the one market observed
        // after the 401s were fixed, `bestBid` 0.2300 and `bestAsk` 0.7700 summed
        // to exactly $1.00, which is either a genuine 54-cent spread on an
        // illiquid tennis book or the gateway quoting the two SIDES rather than
        // the two ends of one side's spread. Those imply opposite formulas for
        // the short leg, and picking the wrong one would misprice every US
        // sports quote while looking plausible. So the short leg reports no book
        // until the convention is established from real data — which is now
        // possible, because the call finally works and its failures are logged.
        if !side.eq_ignore_ascii_case("long") {
            return (None, None, None, None);
        }

        match venue.bbo_for_sports(slug).await {
            Some((bid, bid_sz, ask, ask_sz)) => (bid, bid_sz, ask, ask_sz),
            None => (None, None, None, None),
        }
    }

    async fn settled_prices(
        &self,
        _http: &reqwest::Client,
        _market_key: &str,
        legs: &[String],
    ) -> HashMap<String, f64> {
        use crate::venues::core::TokenResolution as R;
        let mut out = HashMap::new();
        let Some(venue) = self.venue().await else { return out };
        for leg in legs {
            // Only a decisive venue answer books. The gateway pins a resolved
            // market's side price to exactly "1" or "0"; anything else is no answer.
            if let R::Resolved(px) = venue.settlement_for_sports(leg).await {
                if let Ok(v) = px.to_string().parse::<f64>() { out.insert(leg.clone(), v); }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The captured live record, verbatim in the fields that matter.
    ///
    /// From `aec-nfl-lac-ten-2025-11-02`, read off the gateway on 2026-09-27. This
    /// is the record the adapter was written against rather than guessed at.
    /// The captured record as the live path sees it: deserialized into `UsMarket`.
    ///
    /// Going through serde rather than hand-building the struct is the point. The
    /// parsers read a typed record now, so a rename or a missing `#[serde(rename)]`
    /// on `marketSides` or `gameStartTime` would silently empty the field -- and this
    /// is the test that would catch it, because the JSON below is verbatim what
    /// gateway.polymarket.us returned.
    fn captured_market() -> UsMarket {
        serde_json::from_value(captured_moneyline()).expect("the captured record deserializes")
    }

    /// Same record with `f` applied to the raw JSON first, for the malformed cases.
    fn captured_market_with(f: impl FnOnce(&mut serde_json::Value)) -> UsMarket {
        let mut v = captured_moneyline();
        f(&mut v);
        serde_json::from_value(v).expect("the mutated record still deserializes")
    }

    fn captured_moneyline() -> serde_json::Value {
        serde_json::json!({
            "slug": "aec-nfl-lac-ten-2025-11-02",
            "question": "Chargers vs. Titans",
            "marketType": "moneyline",
            "gameStartTime": "2025-11-02T18:00:00Z",
            "endDate": "2025-11-03T18:00:00Z",
            "category": "sports",
            "marketSides": [
                {
                    "description": "Chargers", "long": true, "id": "1",
                    "marketSideType": "MARKET_SIDE_TYPE_INSTRUMENT",
                    "team": { "name": "Los Angeles Chargers", "alias": "Chargers",
                              "abbreviation": "lac", "league": "nfl", "ordering": "away" }
                },
                {
                    "description": "Titans", "long": false, "id": "2",
                    "marketSideType": "MARKET_SIDE_TYPE_INSTRUMENT",
                    "team": { "name": "Tennessee Titans", "alias": "Titans",
                              "abbreviation": "ten", "league": "nfl", "ordering": "home" }
                }
            ]
        })
    }

    /// The one fact that inverts a venue if wrong: which leg pays on which team.
    ///
    /// Read off each side's own `team` object rather than inferred from slug order.
    /// The slug here is `lac-ten` and `long` pays on the Chargers (lac, away) — so a
    /// "first team in the slug is long" convention would happen to work on this
    /// record and is exactly the kind of coincidence that hides an inversion.
    #[test]
    fn each_leg_is_keyed_to_its_own_team_not_to_slug_order() {
        let sides = moneyline_sides(&captured_market()).expect("a moneyline");
        let find = |leg: &str| sides.iter().find(|s| s.leg_id == leg).map(|s| s.team.clone());

        assert_eq!(find("aec-nfl-lac-ten-2025-11-02#long").as_deref(), Some("Los Angeles Chargers"));
        assert_eq!(find("aec-nfl-lac-ten-2025-11-02#short").as_deref(), Some("Tennessee Titans"));

        // Full names, because that is what The Odds API matches against — the short
        // `description` ("Chargers") is only the fallback.
        assert!(sides.iter().all(|s| s.team.contains(' ')), "full team names: {sides:?}");
        // Ordering is carried but never load-bearing.
        assert_eq!(sides[0].ordering.as_deref(), Some("away"));
    }

    /// A record this code does not understand is refused, never half-read.
    #[test]
    fn a_record_that_is_not_a_two_sided_moneyline_is_refused() {
        // Three sides: a drawable market is not a two-way moneyline.
        let three = captured_market_with(|v| {
            let extra = serde_json::json!({
                "description": "Draw", "long": false,
                "marketSideType": "MARKET_SIDE_TYPE_INSTRUMENT",
                "team": { "name": "Draw", "league": "nfl" }
            });
            v["marketSides"].as_array_mut().unwrap().push(extra);
        });
        assert!(moneyline_sides(&three).is_none(), "three outcomes is not this instrument");

        // Both sides claiming `long` is a shape we do not understand; guessing which
        // is which is the error that inverts a venue.
        let both_long = captured_market_with(|v| v["marketSides"][1]["long"] = serde_json::json!(true));
        assert!(moneyline_sides(&both_long).is_none());

        // No team and no description: nothing to match on.
        let nameless = captured_market_with(|v| {
            v["marketSides"][0]["team"] = serde_json::json!({});
            v["marketSides"][0].as_object_mut().unwrap().remove("description");
        });
        assert!(moneyline_sides(&nameless).is_none());

        // A record with no sides at all, and one with no slug to key them to.
        let sideless = captured_market_with(|v| v["marketSides"] = serde_json::json!([]));
        assert!(moneyline_sides(&sideless).is_none());
        let unslugged = captured_market_with(|v| v["slug"] = serde_json::json!(""));
        assert!(moneyline_sides(&unslugged).is_none(), "no slug means no board key");
    }

    /// Kick-off is a real timestamp here, and `endDate` must never stand in for it.
    #[test]
    fn kickoff_comes_from_game_start_time_only() {
        assert_eq!(kickoff_of(&captured_market()).map(|t| t.to_rfc3339()).as_deref(),
                   Some("2025-11-02T18:00:00+00:00"));

        // `endDate` is the settlement date a day later. Falling back to it would put
        // every game outside the matcher's 15-minute tolerance, matching nothing.
        let no_start = captured_market_with(|v| { v.as_object_mut().unwrap().remove("gameStartTime"); });
        assert_eq!(kickoff_of(&no_start), None, "dropped, not dated from endDate");
    }

    /// The league code arrives already normalized, unlike Kalshi's.
    /// The auto-deploy selector looks a game up on the bookmaker board by slug,
    /// while the ledger writes it under whatever `moneyline_sides` produced. If those
    /// two ever disagree the lookup just misses, `board_coverage` reports the venue
    /// as having no covered games, and the sports slot goes quietly idle forever --
    /// a failure that looks exactly like an out-of-season slate. So they are one
    /// function, and this is the test that keeps them one.
    #[test]
    fn the_board_key_the_selector_builds_is_the_key_the_ledger_writes() {
        let m = captured_market();
        let sides = moneyline_sides(&m).expect("a two-sided moneyline");

        let (yes, no) = board_leg_ids(&m.slug);
        let published: Vec<&str> = sides.iter().map(|s| s.leg_id.as_str()).collect();

        assert!(published.contains(&yes.as_str()), "published {published:?} is missing {yes}");
        assert!(published.contains(&no.as_str()), "published {published:?} is missing {no}");
        assert_ne!(yes, no, "both legs cannot share one board key");
    }

    #[test]
    fn the_league_code_is_the_shared_one() {
        assert_eq!(league_of(&captured_market()).as_deref(), Some("nfl"));
        assert_eq!(
            league_of(&captured_market_with(|v| v["marketSides"] = serde_json::json!([]))),
            None,
        );
    }
}

/// Live checks against gateway.polymarket.us. Ignored by default: they need
/// credentials and the network.
#[cfg(test)]
mod live_catalog_checks {
    use super::*;

    /// Where the pre-game slate is lost, if it is lost.
    ///
    /// The ledger reports coverage per league, so "no Polymarket US moneylines" is the
    /// same line whether the listing came back empty, the type filter rejected
    /// everything, or every record was dropped for want of a league code. This walks
    /// the catalog's own path and names the stage that loses them.
    #[tokio::test]
    #[ignore = "live venue: needs credentials and network"]
    async fn the_catalog_keeps_the_pre_game_slate() {
        let http = Arc::new(reqwest::Client::new());
        let venue = UsRetailVenue::connect(http).await.expect("venue connect");
        let all = venue.list_sports_category_markets(&["sports"], None).await
            .expect("listing");

        let open: Vec<&UsMarket> = all.iter().filter(|m| !m.closed).collect();
        let money: Vec<&UsMarket> = open.iter().copied()
            .filter(|m| m.market_type.eq_ignore_ascii_case("moneyline"))
            .collect();

        let mut types: HashMap<&str, usize> = HashMap::new();
        for m in &open { *types.entry(m.market_type.as_str()).or_default() += 1; }
        eprintln!("listing: {} records, {} open; marketType census: {types:?}",
                  all.len(), open.len());

        let mut no_league = Vec::new();
        let mut no_sides = Vec::new();
        let mut no_kickoff = Vec::new();
        let mut kept = 0usize;
        for m in &money {
            let l = league_of(m);
            let s = moneyline_sides(m);
            let k = kickoff_of(m);
            if l.is_none() { no_league.push(m.slug.as_str()); }
            if s.is_none() { no_sides.push(m.slug.as_str()); }
            if k.is_none() { no_kickoff.push(m.slug.as_str()); }
            if l.is_some() && s.is_some() && k.is_some() { kept += 1; }
        }
        eprintln!(
            "moneylines: {} | kept {} | dropped: no league {}, no sides {}, no kickoff {}",
            money.len(), kept, no_league.len(), no_sides.len(), no_kickoff.len(),
        );
        for (label, v) in [("no league", &no_league), ("no sides", &no_sides),
                           ("no kickoff", &no_kickoff)] {
            for slug in v.iter().take(4) { eprintln!("  {label}: {slug}"); }
        }
        // The leagues actually present, which is what the knob has to name.
        let mut leagues: HashMap<String, usize> = HashMap::new();
        for m in &money { *leagues.entry(league_of(m).unwrap_or_else(|| "(none)".into())).or_default() += 1; }
        eprintln!("leagues seen: {leagues:?}");

        assert!(!money.is_empty(), "the venue listed no open moneylines at all");
        assert!(kept > 0, "every open moneyline was dropped by the catalog's parsers");
    }
}
