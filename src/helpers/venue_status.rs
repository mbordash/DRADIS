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

//! [E109] Whether the venue is taking orders, for every venue that says.
//!
//! A venue that is closed looks, from inside the engine, like a venue whose
//! books have died: resting orders leave the public feed, nothing new opens,
//! every gate declines an empty book, and the Control Tower raised the
//! dark-feed warning for a closure Kalshi had published (Thursday 2026-11-26,
//! 03:00 to 05:00 ET). This module reads the venue's own flag through
//! `crate::venues::fetch_trading_status`, keeps the current pause, and tells
//! the executor to hold entries and the status API to say why.
//!
//! Venue-neutral by construction: the probe starts only where the venue
//! publishes the flag (`publishes_trading_status`), and a build without one
//! reads as always open. Exits are never held here: an exit order during a
//! pause is refused by the venue and retried by the viper, which is the
//! posture the operator chose, and holding it would not be.

use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tracing::{debug, info, warn};

/// Seconds between reads. The route is public and unauthenticated.
const PROBE_INTERVAL: Duration = Duration::from_secs(30);
/// A pause older than this with no successful read since is no longer
/// believed: the venue may well be open and only the probe is failing, and
/// an engine that cannot read the flag must not hold entries on a guess.
const BELIEVED_FOR_SECS: i64 = 600;

/// The idle reason a viper shows while entries are held.
pub const PAUSED_REASON: &str = "venue trading paused";

/// The pause as the API reports it. `None` is the open case.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct Pause {
    /// The venue, as the operator reads it.
    pub venue: &'static str,
    /// When the engine first read the pause.
    pub since: DateTime<Utc>,
    pub paused_for_secs: u64,
    /// Why, in the venue's terms.
    pub detail: String,
    /// When trading is expected back, when the venue or its schedule says.
    pub resumes_at: Option<DateTime<Utc>>,
    pub resumes_in_secs: Option<u64>,
}

#[derive(Default)]
struct State {
    /// The current pause: first read, detail, expected end.
    pause: Option<(DateTime<Utc>, String, Option<DateTime<Utc>>)>,
    /// The last successful read, pause or not.
    read_at: Option<DateTime<Utc>>,
    /// Whether the stale-pause warning has been given for this pause.
    stale_warned: bool,
}

static STATE: OnceLock<Mutex<State>> = OnceLock::new();

fn state() -> &'static Mutex<State> {
    STATE.get_or_init(|| Mutex::new(State::default()))
}

fn lock() -> std::sync::MutexGuard<'static, State> {
    match state().lock() {
        Ok(s) => s,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn et(t: DateTime<Utc>) -> String {
    t.with_timezone(&chrono_tz::US::Eastern).format("%a %H:%M ET").to_string()
}

/// Record one read of the venue's flag at `now`.
pub fn note(active: bool, detail: &str, resumes_at: Option<DateTime<Utc>>, now: DateTime<Utc>) {
    let venue = crate::venues::venue_display_name();
    let mut st = lock();
    st.read_at = Some(now);
    match (&mut st.pause, active) {
        (Some((since, _, _)), true) => {
            let mins = (now - *since).num_minutes();
            info!("▶️ {venue} trading resumed after {mins} min; entries evaluate again");
            st.pause = None;
            st.stale_warned = false;
        }
        (None, false) => {
            match resumes_at {
                Some(t) => info!("⏸️ {venue} trading paused ({detail}); entries held, exits still evaluated; expected back {}", et(t)),
                None => info!("⏸️ {venue} trading paused ({detail}); entries held, exits still evaluated; no end announced"),
            }
            st.pause = Some((now, detail.to_string(), resumes_at));
        }
        (Some((_, d, r)), false) => {
            // The same pause, re-read: keep its start, take the venue's latest word.
            *d = detail.to_string();
            *r = resumes_at;
            st.stale_warned = false;
        }
        (None, true) => {}
    }
}

/// The current pause as of `now`, if one is believed.
pub fn snapshot_at(now: DateTime<Utc>) -> Option<Pause> {
    let mut st = lock();
    let (since, detail, resumes_at) = st.pause.clone()?;
    let read_at = st.read_at.unwrap_or(since);
    if (now - read_at).num_seconds() > BELIEVED_FOR_SECS {
        if !st.stale_warned {
            st.stale_warned = true;
            warn!(
                "⏸️ {} trading status unread for {} min; the pause read at {} is no longer believed and entries evaluate again",
                crate::venues::venue_display_name(), (now - read_at).num_minutes(), et(read_at),
            );
        }
        return None;
    }
    Some(Pause {
        venue: crate::venues::venue_display_name(),
        since,
        paused_for_secs: (now - since).num_seconds().max(0) as u64,
        detail,
        resumes_at,
        resumes_in_secs: resumes_at.map(|t| (t - now).num_seconds().max(0) as u64),
    })
}

/// The current pause, if one is believed.
pub fn snapshot() -> Option<Pause> {
    snapshot_at(Utc::now())
}

/// Entries are held: the venue says it is not taking orders.
pub fn paused() -> bool {
    snapshot().is_some()
}

/// Background loop: read the venue's flag every [`PROBE_INTERVAL`]. Returns at
/// once on a venue that publishes none.
pub async fn run_venue_status_probe() {
    if !crate::venues::publishes_trading_status() {
        debug!("Venue trading status probe not started: {} publishes no trading flag", crate::venues::venue_display_name());
        return;
    }
    info!("⏯️ Venue trading status probe started ({})", crate::venues::venue_display_name());
    loop {
        let now = Utc::now();
        match crate::venues::fetch_trading_status(now).await {
            Ok(t) => note(
                t.active,
                &t.detail,
                t.resumes_at.and_then(|s| DateTime::from_timestamp(s, 0)),
                now,
            ),
            // A failed read is not a pause, and not a resume either: the last
            // good read stands until it is too old to believe.
            Err(e) => debug!("Venue trading status read failed: {e}"),
        }
        tokio::time::sleep(PROBE_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One process-wide state; the tests take turns.
    static TEST_GUARD: Mutex<()> = Mutex::new(());

    fn reset() {
        *lock() = State::default();
    }

    /// Test instants sit years in the past on purpose: the state is one
    /// process-wide static, and a pause noted here is read by every other test
    /// in the binary through `paused()`. Stamped in 2020 it is "unread" for far
    /// longer than `BELIEVED_FOR_SECS` at the real clock, so it holds nothing.
    fn t(s: &str) -> DateTime<Utc> {
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap().and_utc()
    }

    #[test]
    fn a_pause_is_reported_from_its_first_read_until_the_venue_reopens() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        reset();
        assert!(snapshot_at(t("2020-11-26 08:02:00")).is_none());
        note(false, "the exchange's scheduled closing hours", Some(t("2020-11-26 10:00:00")), t("2020-11-26 08:02:00"));
        note(false, "the exchange's scheduled closing hours", Some(t("2020-11-26 10:00:00")), t("2020-11-26 08:02:30"));
        let p = snapshot_at(t("2020-11-26 08:03:00")).expect("paused");
        assert_eq!(p.since, t("2020-11-26 08:02:00"), "the first read is the start");
        assert_eq!(p.paused_for_secs, 60);
        assert_eq!(p.resumes_in_secs, Some(117 * 60));
        assert_eq!(p.detail, "the exchange's scheduled closing hours");
        note(true, "", None, t("2020-11-26 10:00:15"));
        assert!(snapshot_at(t("2020-11-26 10:00:20")).is_none());
        reset();
    }

    #[test]
    fn a_pause_the_probe_cannot_refresh_is_dropped_after_ten_minutes() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        reset();
        note(false, "paused by the exchange", None, t("2020-11-26 12:00:00"));
        assert!(snapshot_at(t("2020-11-26 12:09:59")).is_some());
        assert!(snapshot_at(t("2020-11-26 12:10:01")).is_none(), "unread for ten minutes: not believed");
        // A fresh read of the same pause brings it back.
        note(false, "paused by the exchange", None, t("2020-11-26 12:11:00"));
        let p = snapshot_at(t("2020-11-26 12:11:30")).expect("believed again");
        assert_eq!(p.since, t("2020-11-26 12:00:00"));
        reset();
    }

    #[test]
    fn an_open_venue_reports_nothing() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        reset();
        note(true, "", None, t("2020-11-26 12:00:00"));
        assert!(snapshot_at(t("2020-11-26 12:00:10")).is_none());
        reset();
    }
}
