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

/// Background task: market switch monitor.
///
/// Polls the Gamma API every 90 seconds and broadcasts a new market tuple
/// via the watch channel whenever the active hourly or maker market changes.
/// `main.rs` breaks its inner trading loop when it sees the channel updated.
use std::sync::Arc;

use alloy::primitives::U256;
use chrono::Utc;
use rust_decimal::Decimal;
use tokio::sync::watch;
use tracing::info;

use crate::config;
use crate::helpers::market::{get_market_pair, MarketCandidate};
use crate::helpers::time::{fetch_historical_strike_price, fetch_strike_price_from_close_time, hourly_window_reference_time};
use crate::venues::core::MarketId;
use crate::venues::intl::market_id_from_u256;

/// Hard cap on how long a single `get_market_pair` scan may run.
///
/// `fetch_simplified_crypto_candidates` pages through GAMMA_API_MARKET_SCAN_PAGES (currently 30)
/// Gamma API pages sequentially, each with a 20-second reqwest timeout.  In the worst
/// case (all 30 pages stall until timeout) the scan takes 600 s.  Without an outer cap
/// the market_monitor task can go silent for up to 10 minutes, missing market switches.
///
/// 90 s = 1 full 90-second monitor interval.  If the scan hasn't finished in 90 s
/// something is badly wrong with the Gamma API; log a warning and retry next tick.
const MARKET_SCAN_TIMEOUT_SECS: u64 = 90;

/// The shared market state tuple broadcast on the watch channel.
pub type MarketState = (
    MarketId,                    // yes_token (venue-neutral)
    MarketId,                    // no_token  (venue-neutral)
    String,                      // market_name
    Option<chrono::DateTime<Utc>>, // market_close_time
    Option<Decimal>,             // strike_price
    String,                      // description
    Option<MarketCandidate>,     // maker_market_candidate
    String,                      // condition_id (NEW)
);

/// Whether the held hourly market is owed a strike it did not have at rotation.
///
/// An "Up or Down" market's strike is the open of the Binance candle that
/// starts its window, and the squadron rotates onto the next hour's market
/// before that window opens — so the strike legitimately does not exist at
/// rotation time. It exists from the window open onward, and it has to be
/// fetched then: the monitor is the only task that resolves strikes, and its
/// switch path runs once. Due while the market is live, its strike is still
/// unknown, and its window has opened. Never due for a closed market.
fn strike_refresh_due(
    strike: Option<Decimal>,
    close_time: Option<chrono::DateTime<Utc>>,
    now: chrono::DateTime<Utc>,
) -> bool {
    strike.is_none()
        && close_time.is_some_and(|ct| ct > now && hourly_window_reference_time(ct, now).is_some())
}

/// Whether a held market should be released when no replacement candidate exists.
///
/// Only *proven* expiry releases. A missing close time means unknown, not dead:
/// releasing on unknown would drop live markets whose close time simply never got
/// populated, which is a worse failure than holding one a little too long.
fn should_release_held_market(
    holding_a_market: bool,
    close_time: Option<chrono::DateTime<Utc>>,
    now: chrono::DateTime<Utc>,
) -> bool {
    holding_a_market && close_time.is_some_and(|ct| (ct - now).num_seconds() <= 0)
}

/// Resolve the maker/daily candidate's strike price before broadcasting it.
///
/// The CAG only resolves the maker strike ONCE at startup; every market-switch
/// broadcast used to send the maker candidate with `strike_price: None`, which
/// silently disabled strike-dependent vipers (FairValue, Basis) on the daily
/// venue after the first hourly rotation. Reuse the previous candidate's strike
/// when the maker market itself hasn't changed (avoids redundant API calls).
async fn resolve_maker_strike(
    http: &reqwest::Client,
    crypto_filter: &str,
    prev: Option<&MarketCandidate>,
    mk: &mut MarketCandidate,
) {
    if mk.strike_price.is_some() {
        return;
    }
    if let Some(s) = prev
        .filter(|p| p.yes_token == mk.yes_token)
        .and_then(|p| p.strike_price)
    {
        mk.strike_price = Some(s);
        return;
    }
    let mut strike = crate::helpers::market::extract_strike_price(&mk.name);
    if strike.is_none() {
        strike = fetch_historical_strike_price(http, crypto_filter, &mk.description).await;
    }
    if strike.is_none() {
        strike = fetch_historical_strike_price(http, crypto_filter, &mk.name).await;
    }
    mk.strike_price = strike;
    match strike {
        Some(s) => info!("✅ Maker market strike price resolved (monitor): ${}", s),
        None => tracing::warn!("⚠️ Maker market strike price unresolved for \"{}\"", mk.name),
    }
}

pub async fn run_market_monitor(
    http: Arc<reqwest::Client>,
    crypto_filter: String,
    market_tx: watch::Sender<MarketState>,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(90));
    // A widened (180 s) scan overruns the 90 s tick; don't burst-fire the missed
    // ticks afterwards — just resume the normal cadence.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Escalating backoff for Gamma API stalls (roadmap bug #8).
    // 1st timeout → retry next tick at the normal 90 s cap; 2nd → widen the cap
    // to 180 s (slow-but-alive API); 3rd+ → circuit-break: log ONE warn and skip
    // the next few ticks entirely so a struggling API isn't hammered and the log
    // isn't spammed every 90 s. Any successful scan resets the breaker.
    let mut consecutive_timeouts: u32 = 0;
    let mut skip_ticks: u32 = 0;
    loop {
        interval.tick().await;
        if skip_ticks > 0 {
            skip_ticks -= 1;
            continue;
        }
        // Hard cap on market scan — see MARKET_SCAN_TIMEOUT_SECS comment above.
        // Widened to 2× once the API has already shown one consecutive stall.
        let scan_cap_secs = if consecutive_timeouts >= 1 {
            MARKET_SCAN_TIMEOUT_SECS * 2
        } else {
            MARKET_SCAN_TIMEOUT_SECS
        };
        let scan_result = tokio::time::timeout(
            std::time::Duration::from_secs(scan_cap_secs),
            get_market_pair(&http, &crypto_filter),
        ).await;
        let (candidate, mut maker_candidate) = match scan_result {
            Ok(pair) => {
                if consecutive_timeouts > 0 {
                    info!("✅ Market monitor: Gamma API recovered after {} timed-out scan(s)", consecutive_timeouts);
                }
                consecutive_timeouts = 0;
                pair
            }
            Err(_) => {
                consecutive_timeouts += 1;
                if consecutive_timeouts >= 3 {
                    // Circuit-break: stand down for 3 ticks (~4.5 min) before probing again.
                    skip_ticks = 3;
                    tracing::warn!(
                        "🚨 Market monitor circuit-break: get_market_pair timed out {} consecutive times (cap {}s) — pausing scans for {} ticks",
                        consecutive_timeouts, scan_cap_secs, skip_ticks
                    );
                } else {
                    tracing::warn!(
                        "⚠️ Market monitor: get_market_pair timed out after {}s ({} consecutive) — retrying next poll cycle",
                        scan_cap_secs, consecutive_timeouts
                    );
                }
                continue;
            }
        };
        // No tradeable market this scan. Before skipping, check whether the market we
        // are ALREADY holding has expired — releasing it is the whole point of this arm.
        //
        // This guard used to be a bare `continue`, which sat ABOVE the expiry logic below
        // and therefore made `cur_secs_left <= 0` — the branch whose entire job is
        // releasing a dead market — unreachable whenever no replacement existed. A
        // squadron that could not find a successor clung to its expired market forever.
        //
        // Ireland, 2026-08-27: a squadron sat on a market that had closed 28 minutes
        // earlier, reporting `secs_to_expiry -1684s` and counting down by 30s a tick,
        // with every viper gate correctly refusing to quote it. Nothing was at risk, but
        // nothing could recover either — only a process restart would clear it.
        //
        // Releasing publishes the ZERO sentinel, which is the same state the channel is
        // seeded with at startup before any market is found, so every consumer already
        // handles it. The maker candidate is carried through untouched: the hourly market
        // expiring says nothing about the maker market, which has its own lifecycle.
        if candidate.yes_token == market_id_from_u256(U256::ZERO) {
            let (cur_yes, _, cur_name, cur_close_time, _, _, cur_maker, _) =
                market_tx.borrow().clone();
            let holding_a_market = cur_yes != market_id_from_u256(U256::ZERO);
            if should_release_held_market(holding_a_market, cur_close_time, Utc::now()) {
                info!(
                    "🛑 Releasing expired market \"{}\" — no replacement available; \
                     squadron waits rather than holding a market that closed",
                    cur_name,
                );
                let _ = market_tx.send((
                    market_id_from_u256(U256::ZERO),
                    market_id_from_u256(U256::ZERO),
                    String::new(),
                    None,
                    None,
                    String::new(),
                    cur_maker,
                    String::new(),
                ));
            }
            continue;
        }

        let (cur_yes, _, cur_name, cur_close_time, _, _, _, _cur_cid) = market_tx.borrow().clone();

        if candidate.yes_token == cur_yes {
            // ── Strike arriving after rotation ───────────────────────────────
            // Re-broadcast the SAME market with its strike filled in. The patrol
            // loop treats an unchanged condition id as a no-op rather than a
            // rotation (no order cancel, no redeploy) and reads the strike live
            // from this channel on every tick — see `live_hourly_strike`.
            let (cur_strike, cur_close) = {
                let ms = market_tx.borrow();
                (ms.4, ms.3)
            };
            if strike_refresh_due(cur_strike, cur_close, Utc::now()) {
                match fetch_strike_price_from_close_time(&http, &crypto_filter, cur_close).await {
                    Some(strike) => {
                        info!(
                            "✅ Hourly strike resolved at window open: ${} for \"{}\"",
                            strike, cur_name,
                        );
                        let (y, n, nm, ct, _, ds, mk, cid) = market_tx.borrow().clone();
                        let _ = market_tx.send((y, n, nm, ct, Some(strike), ds, mk, cid));
                    }
                    None => tracing::warn!(
                        "⚠️ Hourly strike still unresolved for \"{}\" after its window opened — retrying next scan",
                        cur_name,
                    ),
                }
            }
            // Hourly market unchanged — still check if maker market changed
            let cur_maker_yes = market_tx.borrow().6.as_ref().map(|m| m.yes_token.clone());
            let new_maker_yes = maker_candidate.as_ref().map(|m| m.yes_token.clone());
            if cur_maker_yes != new_maker_yes {
                if let Some(ref mk) = maker_candidate {
                    info!("🏦 Maker market updated: \"{}\"", mk.name);
                }
                let (y, n, nm, ct, sp, ds, prev_mk, cid) = market_tx.borrow().clone();
                if let Some(ref mut mk) = maker_candidate {
                    resolve_maker_strike(&http, &crypto_filter, prev_mk.as_ref(), mk).await;
                }
                let _ = market_tx.send((y, n, nm, ct, sp, ds, maker_candidate, cid));
            }
            continue;
        }

        let now_ts = Utc::now();
        let cur_secs_left = cur_close_time.map_or(9999i64, |ct| (ct - now_ts).num_seconds());
        let new_secs_left = candidate.close_time.map_or(9999i64, |ct| (ct - now_ts).num_seconds());

        let candidate_is_binary = candidate.name.to_lowercase().contains("up or down");
        let current_is_binary = cur_name.to_lowercase().contains("up or down");
        let candidate_is_range = config::is_range_market(&candidate.name);

        let time_based_upgrade = new_secs_left > cur_secs_left + 1800
            && !(current_is_binary && !candidate_is_binary);

        // Detect the daily-as-substitute case: we're running on a long-lived daily/window market
        // (used as a fallback during bootstrap when no hourly was published yet) and a real hourly
        // market has now appeared.  Force an upgrade so MomentumStrategy and other hourly-venue
        // strategies can participate.  Without this, the time_based_upgrade check would NEVER fire
        // because the daily's secs_left >> hourly's secs_left, and the bot would stay on the daily
        // for the entire hour even after the 12PM-ET (or any other) hourly market is listed.
        let current_is_daily_sub = config::is_daily_market(&cur_name) || config::is_window_market(&cur_name);
        let candidate_is_hourly = !config::is_daily_market(&candidate.name)
            && !config::is_window_market(&candidate.name)
            && !config::is_ultra_short_window_market(&candidate.name);
        let daily_to_hourly_upgrade = current_is_daily_sub && candidate_is_hourly && new_secs_left > 600;

        let should_switch = cur_secs_left < config::FINAL_EXPIRY_WINDOW_SECS
            || cur_secs_left <= 0
            || time_based_upgrade
            || daily_to_hourly_upgrade
            || (candidate_is_binary && !current_is_binary && !candidate_is_range
                && new_secs_left > 600 && cur_secs_left > 300);

        if !should_switch { continue; }

        info!("🔄 Market Switch Detected: {} -> {}", cur_name, candidate.name);
        let mut strike = crate::helpers::market::extract_strike_price(&candidate.name);
        if strike.is_none() {
            strike = fetch_historical_strike_price(&http, &crypto_filter, &candidate.description).await;
        }
        if strike.is_none() {
            strike = fetch_historical_strike_price(&http, &crypto_filter, &candidate.name).await;
        }
        if strike.is_none() {
            strike = fetch_strike_price_from_close_time(&http, &crypto_filter, candidate.close_time).await;
        }
        match (strike, candidate.close_time) {
            (Some(s), _) => info!("✅ Hourly strike resolved: ${} for \"{}\"", s, candidate.name),
            // The normal state for a fresh "Up or Down" market: it is picked
            // up before its window opens, and no strike exists until then.
            // Strike-dependent vipers idle on it; the refresh above fills it in.
            (None, Some(ct)) if hourly_window_reference_time(ct, Utc::now()).is_none() => info!(
                "⏳ \"{}\" opens at {} — no strike until then; strike-dependent vipers idle on it until the open",
                candidate.name,
                (ct - chrono::Duration::hours(1)).with_timezone(&chrono_tz::US::Eastern).format("%H:%M ET"),
            ),
            (None, _) => tracing::warn!("⚠️ Hourly strike unresolved for \"{}\"", candidate.name),
        }
        if let Some(ref mut mk) = maker_candidate {
            let prev_mk = market_tx.borrow().6.clone();
            resolve_maker_strike(&http, &crypto_filter, prev_mk.as_ref(), mk).await;
        }
        let _ = market_tx.send((
            candidate.yes_token, candidate.no_token,
            candidate.name.clone(), candidate.close_time,
            strike, candidate.description.clone(),
            maker_candidate,
            candidate.condition_id.clone(),
        ));
    }
}

#[cfg(test)]
mod strike_refresh_tests {
    use super::strike_refresh_due;
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal_macros::dec;

    fn utc(s: &str) -> chrono::DateTime<Utc> {
        Utc.datetime_from_str(s, "%Y-%m-%dT%H:%M:%SZ").unwrap()
    }

    /// The 2026-09-03 6AM ET market (10:00-11:00 UTC), held from before its
    /// open: not due until the window opens, due from then until close, never
    /// due once it has closed or once it has a strike.
    #[test]
    fn a_strike_is_owed_only_while_the_window_is_open_and_the_strike_unknown() {
        let close = Some(utc("2026-09-03T11:00:00Z"));
        assert!(!strike_refresh_due(None, close, utc("2026-09-03T09:55:00Z")), "pre-open: nothing to fetch yet");
        assert!(strike_refresh_due(None, close, utc("2026-09-03T10:00:30Z")), "just opened: owed");
        assert!(strike_refresh_due(None, close, utc("2026-09-03T10:48:00Z")), "mid-hour: still owed");
        assert!(!strike_refresh_due(None, close, utc("2026-09-03T11:00:01Z")), "closed: nothing to price");
        assert!(!strike_refresh_due(Some(dec!(77600)), close, utc("2026-09-03T10:30:00Z")), "already known");
    }

    /// The ZERO sentinel (no market held) and a market with no close time can
    /// never owe a strike; the fetch would have nothing to reference.
    #[test]
    fn no_close_time_means_nothing_is_owed() {
        let now = Utc::now();
        assert!(!strike_refresh_due(None, None, now));
        assert!(!strike_refresh_due(None, Some(now + Duration::minutes(30)), now - Duration::hours(2)));
    }
}

#[cfg(test)]
mod release_expired_market_tests {
    use super::should_release_held_market;
    use chrono::{Duration, Utc};

    /// The Ireland case: the held market closed 28 minutes ago and the scan found no
    /// replacement. Before this, the bare `continue` above the expiry logic meant the
    /// squadron held it forever, reporting `secs_to_expiry -1684s` and counting down.
    #[test]
    fn an_expired_market_is_released_when_nothing_replaces_it() {
        let now = Utc::now();
        assert!(should_release_held_market(true, Some(now - Duration::minutes(28)), now));
    }

    /// A live market is kept — this arm must not evict a market that is still trading
    /// just because one scan came back empty.
    #[test]
    fn a_live_market_is_kept() {
        let now = Utc::now();
        assert!(!should_release_held_market(true, Some(now + Duration::minutes(20)), now));
    }

    /// Unknown close time is not evidence of death. Releasing here would drop live
    /// markets whose close time never got populated.
    #[test]
    fn an_unknown_close_time_is_not_treated_as_expired() {
        assert!(!should_release_held_market(true, None, Utc::now()));
    }

    /// Already holding nothing — releasing again would republish the sentinel every
    /// scan and wake every consumer of the channel for no reason.
    #[test]
    fn holding_nothing_releases_nothing() {
        let now = Utc::now();
        assert!(!should_release_held_market(false, Some(now - Duration::hours(1)), now));
    }

    /// Exactly at the close boundary counts as expired, matching `cur_secs_left <= 0`
    /// in the switch logic below so the two paths agree.
    #[test]
    fn the_close_boundary_counts_as_expired() {
        let now = Utc::now();
        assert!(should_release_held_market(true, Some(now), now));
    }
}
