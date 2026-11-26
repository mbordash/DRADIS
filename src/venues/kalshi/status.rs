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

//! [E109] Kalshi's trading state: `GET /exchange/status` says whether trading
//! is permitted right now and `GET /exchange/schedule` says when it next is.
//!
//! Kalshi closes every Thursday from 03:00 to 05:00 ET and publishes those
//! hours; it can also pause trading at any time. While a pause holds, resting
//! orders leave the public books and no new hourly or 15-minute crypto
//! markets open, which the engine could only read as a dead book feed
//! (production, Thursday 2026-11-26 03:02 ET: "Market data has stopped for 2
//! markets", on a venue that was closed on schedule). The latency probe has
//! always hit the status route; it only timed the answer.
//!
//! The top-level `trading_active` is read, not the per-shard list: the docs
//! warn that a shard whose read failed is reported with every flag false,
//! and whether the top-level flag spans all shards is not documented. Crypto
//! trades on shard 2; a halt of that shard alone would read as open here,
//! which is no worse than before this module.
//!
//! Both routes are public and unauthenticated, so the trading client's rate
//! bucket is left alone. Kalshi's docs say to read the flags only from a 200,
//! and `get_json` fails on anything else, so a failed read never reads as a
//! pause.

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, TimeZone, Utc, Weekday};
use chrono_tz::America::New_York;
use serde_json::Value;

use crate::venues::core::VenueTrading;

/// Days ahead the schedule is searched for the next open. The exchange's week
/// has no closure longer than a few hours, so one week is generous.
const LOOKAHEAD_DAYS: i64 = 8;

/// The venue's trading state as of `now`.
pub async fn fetch(now: DateTime<Utc>) -> Result<VenueTrading, String> {
    let base = super::base_url();
    let status = super::history::get_json(&format!("{base}/exchange/status")).await?;
    let active = status
        .get("trading_active")
        .and_then(Value::as_bool)
        .ok_or_else(|| "exchange status without trading_active".to_string())?;
    if active {
        return Ok(VenueTrading::open());
    }
    let exchange_active = status.get("exchange_active").and_then(Value::as_bool).unwrap_or(true);
    // The exchange's own estimate wins when it gives one that is still ahead.
    let estimate = status
        .get("exchange_estimated_resume_time")
        .and_then(Value::as_str)
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&Utc))
        .filter(|t| *t > now);
    let halt = if exchange_active { "paused by the exchange" } else { "exchange maintenance" };
    let (resumes_at, detail) = match estimate {
        Some(t) => (Some(t), halt.to_string()),
        None => {
            // A failed schedule read leaves the resume time unknown; the pause
            // itself was read from a 200 and stands.
            let schedule = super::history::get_json(&format!("{base}/exchange/schedule")).await.ok();
            match schedule.as_ref().and_then(|s| next_open(s, now)) {
                Some(t) => (Some(t), "the exchange's scheduled closing hours".to_string()),
                None => (None, halt.to_string()),
            }
        }
    };
    Ok(VenueTrading { active: false, detail, resumes_at: resumes_at.map(|t| t.timestamp()) })
}

/// When the schedule next has trading open, as of `now`: the first session
/// open after `now`, pushed past any maintenance window that covers it. `None`
/// when the schedule says trading should be open now and no maintenance window
/// covers it, so the pause is not the schedule's doing and its end is unknown.
///
/// `schedule` is the `GET /exchange/schedule` body (with or without its
/// `schedule` wrapper): `standard_hours` is a list of weekly schedules, each
/// effective between `start_time` and `end_time`, with per-weekday lists of
/// `{open_time, close_time}` in ET, `HH:MM`, where a close at or before its open
/// means the session runs into the next day (`00:00` to `00:00` is all day).
/// `maintenance_windows` are `{start_datetime, end_datetime}` instants.
pub fn next_open(schedule: &Value, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let sched = schedule.get("schedule").unwrap_or(schedule);
    let weeks = sched.get("standard_hours")?.as_array()?;
    // No week covering `now` means no schedule to read; "no end announced"
    // is more honest than an expired week's hours.
    let week = weeks.iter().find(|w| effective(w, now))?;
    let windows: Vec<(DateTime<Utc>, DateTime<Utc>)> = sched
        .get("maintenance_windows")
        .and_then(Value::as_array)
        .map(|ws| {
            ws.iter()
                .filter_map(|w| Some((instant(w.get("start_datetime")?)?, instant(w.get("end_datetime")?)?)))
                .collect()
        })
        .unwrap_or_default();

    let mut t = now;
    // Each pass either returns or moves `t` strictly forward; the bound only
    // caps a pathological schedule.
    for _ in 0..8 {
        if let Some(end) = windows.iter().filter(|(s, e)| *s <= t && t < *e).map(|(_, e)| *e).max() {
            t = end;
            continue;
        }
        if in_session(week, t) {
            return if t > now { Some(t) } else { None };
        }
        t = next_session_open(week, t)?;
    }
    Some(t)
}

fn effective(week: &Value, now: DateTime<Utc>) -> bool {
    let start = week.get("start_time").and_then(instant);
    let end = week.get("end_time").and_then(instant);
    start.is_none_or(|s| s <= now) && end.is_none_or(|e| now < e)
}

fn instant(v: &Value) -> Option<DateTime<Utc>> {
    v.as_str()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&Utc))
}

fn day_key(day: Weekday) -> &'static str {
    match day {
        Weekday::Mon => "monday",
        Weekday::Tue => "tuesday",
        Weekday::Wed => "wednesday",
        Weekday::Thu => "thursday",
        Weekday::Fri => "friday",
        Weekday::Sat => "saturday",
        Weekday::Sun => "sunday",
    }
}

fn hhmm(v: Option<&Value>) -> Option<NaiveTime> {
    let s = v?.as_str()?.trim();
    NaiveTime::parse_from_str(s, "%H:%M")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S"))
        .ok()
}

/// The ET wall-clock instant `date` at `time`, in UTC. Across a spring-forward
/// gap the first valid instant after it; across a fall-back fold the earlier.
fn et(date: NaiveDate, time: NaiveTime) -> Option<DateTime<Utc>> {
    let naive = date.and_time(time);
    New_York
        .from_local_datetime(&naive)
        .earliest()
        .or_else(|| New_York.from_local_datetime(&(naive + Duration::hours(1))).earliest())
        .map(|d| d.with_timezone(&Utc))
}

/// The sessions whose open falls on `date` (ET), as UTC instants.
fn sessions_on(week: &Value, date: NaiveDate) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
    let Some(list) = week.get(day_key(date.weekday())).and_then(Value::as_array) else { return vec![] };
    list.iter()
        .filter_map(|s| {
            let open_t = hhmm(s.get("open_time"))?;
            let close_t = hhmm(s.get("close_time"))?;
            let open = et(date, open_t)?;
            // A close at or before its open runs into the next day.
            let close_date = if close_t <= open_t { date.succ_opt()? } else { date };
            let close = et(close_date, close_t)?;
            Some((open, close))
        })
        .collect()
}

fn in_session(week: &Value, t: DateTime<Utc>) -> bool {
    let today = t.with_timezone(&New_York).date_naive();
    // Yesterday's sessions can run past midnight into today.
    let days = [today.pred_opt(), Some(today)];
    days.into_iter()
        .flatten()
        .flat_map(|d| sessions_on(week, d))
        .any(|(open, close)| open <= t && t < close)
}

fn next_session_open(week: &Value, t: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let today = t.with_timezone(&New_York).date_naive();
    (0..=LOOKAHEAD_DAYS)
        .filter_map(|d| today.checked_add_signed(Duration::days(d)))
        .flat_map(|d| sessions_on(week, d))
        .map(|(open, _)| open)
        .filter(|open| *open > t)
        .min()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Kalshi's published week as of 2026-11-26: open around the clock except
    /// Thursday 03:00 to 05:00 ET.
    fn week() -> Value {
        let all_day = json!([{"open_time": "00:00", "close_time": "00:00"}]);
        json!({"schedule": {"standard_hours": [{
            "start_time": "2024-12-01T00:00:00Z", "end_time": "2200-12-01T00:00:00Z",
            "monday": all_day, "tuesday": all_day, "wednesday": all_day,
            "thursday": [{"open_time": "00:00", "close_time": "03:00"}, {"open_time": "05:00", "close_time": "00:00"}],
            "friday": all_day, "saturday": all_day, "sunday": all_day,
        }], "maintenance_windows": []}})
    }

    fn utc(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn inside_the_thursday_closure_the_next_open_is_five_et() {
        // Thursday 2026-11-26 03:30 ET is 08:30 UTC (EST).
        assert_eq!(next_open(&week(), utc("2026-11-26T08:30:00Z")), Some(utc("2026-11-26T10:00:00Z")));
        // At the close itself.
        assert_eq!(next_open(&week(), utc("2026-11-26T08:00:00Z")), Some(utc("2026-11-26T10:00:00Z")));
    }

    #[test]
    fn a_pause_during_scheduled_hours_has_no_known_end() {
        // Thursday 02:00 ET, inside the first session.
        assert_eq!(next_open(&week(), utc("2026-11-26T07:00:00Z")), None);
        // Wednesday evening, an all-day session; and Thursday 05:00 ET itself, the reopen.
        assert_eq!(next_open(&week(), utc("2026-11-25T23:30:00Z")), None);
        assert_eq!(next_open(&week(), utc("2026-11-26T10:00:00Z")), None);
    }

    #[test]
    fn a_maintenance_window_pushes_the_open_past_its_end() {
        let mut s = week();
        s["schedule"]["maintenance_windows"] = json!([
            {"start_datetime": "2026-11-25T19:00:00Z", "end_datetime": "2026-11-25T21:00:00Z"}
        ]);
        assert_eq!(next_open(&s, utc("2026-11-25T20:00:00Z")), Some(utc("2026-11-25T21:00:00Z")));
        // A window ending inside the Thursday closure defers to the schedule.
        s["schedule"]["maintenance_windows"] = json!([
            {"start_datetime": "2026-11-26T07:30:00Z", "end_datetime": "2026-11-26T08:30:00Z"}
        ]);
        assert_eq!(next_open(&s, utc("2026-11-26T07:45:00Z")), Some(utc("2026-11-26T10:00:00Z")));
    }

    #[test]
    fn the_week_in_force_is_the_one_whose_dates_cover_now() {
        let mut s = week();
        // An expired week that closed Wednesdays entirely must not apply.
        let mut old = s["schedule"]["standard_hours"][0].clone();
        old["end_time"] = json!("2025-01-01T00:00:00Z");
        old["wednesday"] = json!([]);
        s["schedule"]["standard_hours"] = json!([old, s["schedule"]["standard_hours"][0].clone()]);
        assert_eq!(next_open(&s, utc("2026-11-25T23:30:00Z")), None);
    }

    #[test]
    fn only_an_expired_week_gives_nothing() {
        let mut s = week();
        s["schedule"]["standard_hours"][0]["end_time"] = json!("2025-01-01T00:00:00Z");
        assert_eq!(next_open(&s, utc("2026-11-26T08:30:00Z")), None);
    }

    #[test]
    fn seconds_in_the_hours_are_tolerated() {
        let mut s = week();
        s["schedule"]["standard_hours"][0]["thursday"] = json!([
            {"open_time": "00:00:00", "close_time": "03:00:00"}, {"open_time": "05:00:00", "close_time": "00:00:00"}
        ]);
        assert_eq!(next_open(&s, utc("2026-11-26T08:30:00Z")), Some(utc("2026-11-26T10:00:00Z")));
    }

    #[test]
    fn a_body_without_hours_gives_nothing() {
        assert_eq!(next_open(&json!({"schedule": {}}), utc("2026-11-26T08:30:00Z")), None);
        assert_eq!(next_open(&json!({"schedule": {"standard_hours": [{"thursday": [{"open_time": "x", "close_time": "y"}]}]}}), utc("2026-11-26T08:30:00Z")), None);
    }
}
