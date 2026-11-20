// Copyright (C) 2026 Michael Bordash
//
// This file is part of DRADIS.
//
// DRADIS is free software: you can redistribute it and/or modify it under the
// terms of the GNU Affero General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option)
// any later version.
//
// DRADIS is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for
// more details.
//
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

//! [E106] A scripted viper's trading schedule: the days and Eastern-time
//! windows in which it may open a position. Set on the viper's Settings tab,
//! instance-wide like its Live switch, and enforced by the adapter on entries
//! only: a position opened inside a window keeps its exits after the window
//! closes, exactly as the documented `et_hour` pattern in a script does.
//!
//! An empty schedule means always. Times are `HH:MM` in US/Eastern with
//! daylight saving applied, the clock every log line and `ctx.et_hour` use.
//! A window whose end is at or before its start runs overnight: `22:00` to
//! `06:00` on Monday opens Monday 22:00 and closes Tuesday 06:00. `24:00` is
//! accepted as an end, meaning midnight at the end of the day, and `00:00` to
//! `00:00` is the whole of each listed day (the one case where start and end
//! may be equal, since a time picker cannot type `24:00`).

use chrono::{DateTime, Datelike, Timelike, Utc};
use serde::{Deserialize, Serialize};

/// One window: the days it starts on (1 = Monday .. 7 = Sunday) and its
/// Eastern-time bounds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleWindow {
    pub days: Vec<u8>,
    pub start: String,
    pub end: String,
}

const DAY_NAMES: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const MINUTES_PER_DAY: u32 = 24 * 60;

/// `HH:MM` to minutes after midnight. `24:00` is allowed (end of day); nothing
/// past it is.
pub fn parse_hhmm(s: &str) -> Option<u32> {
    let (h, m) = s.trim().split_once(':')?;
    if h.len() != 2 || m.len() != 2 { return None; }
    let h: u32 = h.parse().ok()?;
    let m: u32 = m.parse().ok()?;
    if m > 59 || h > 24 || (h == 24 && m != 0) { return None; }
    Some(h * 60 + m)
}

/// Why a schedule cannot be saved, or `Ok` when every window is well formed.
/// Checked when the patch arrives, so a bad window is refused with its reason
/// rather than quietly read as "always" or "never".
pub fn validate(windows: &[ScheduleWindow]) -> Result<(), String> {
    for (i, w) in windows.iter().enumerate() {
        let n = i + 1;
        if w.days.is_empty() {
            return Err(format!("schedule window {n} names no days"));
        }
        if let Some(d) = w.days.iter().find(|d| !(1..=7).contains(*d)) {
            return Err(format!("schedule window {n}: day {d} is not 1 (Monday) to 7 (Sunday)"));
        }
        let start = parse_hhmm(&w.start).ok_or_else(|| format!("schedule window {n}: start '{}' is not HH:MM", w.start))?;
        let end = parse_hhmm(&w.end).ok_or_else(|| format!("schedule window {n}: end '{}' is not HH:MM", w.end))?;
        if start == MINUTES_PER_DAY {
            return Err(format!("schedule window {n}: a window cannot start at 24:00"));
        }
        if start == end && start != 0 {
            return Err(format!("schedule window {n}: start and end are both {}; a window needs some length (00:00 to 00:00 is a whole day)", w.start));
        }
    }
    Ok(())
}

/// Whether the schedule is open at `now`. Empty means always open.
pub fn is_open(windows: &[ScheduleWindow], now: DateTime<Utc>) -> bool {
    if windows.is_empty() { return true; }
    let et = now.with_timezone(&chrono_tz::US::Eastern);
    let today = et.weekday().number_from_monday() as u8;
    let yesterday = if today == 1 { 7 } else { today - 1 };
    let minute = et.hour() * 60 + et.minute();
    windows.iter().any(|w| {
        let (Some(start), Some(end)) = (parse_hhmm(&w.start), parse_hhmm(&w.end)) else { return false };
        if start < end {
            // Same-day window: open on a listed day between the bounds.
            w.days.contains(&today) && minute >= start && minute < end
        } else {
            // Overnight: the evening part on a listed day, the morning part on
            // the day after a listed day.
            (w.days.contains(&today) && minute >= start) || (w.days.contains(&yesterday) && minute < end)
        }
    })
}

/// The schedule in words, for the viper card: "Mon-Fri 09:30-16:00, Sat 10:00-12:00 ET".
pub fn describe(windows: &[ScheduleWindow]) -> String {
    if windows.is_empty() { return "always".into(); }
    let parts: Vec<String> = windows.iter().map(|w| {
        let span = if parse_hhmm(&w.start) == Some(0) && matches!(parse_hhmm(&w.end), Some(0) | Some(MINUTES_PER_DAY)) {
            "all day".to_string()
        } else {
            format!("{}-{}", w.start, w.end)
        };
        format!("{} {}", day_range(&w.days), span)
    }).collect();
    format!("{} ET", parts.join(", "))
}

/// The reason an entry is refused outside the schedule.
pub fn closed_reason(windows: &[ScheduleWindow]) -> String {
    format!("outside the viper's schedule ({})", describe(windows))
}

/// "Mon-Fri" for a run of consecutive days, "Mon, Wed, Fri" otherwise, "every day" for all seven.
fn day_range(days: &[u8]) -> String {
    let mut d: Vec<u8> = days.iter().copied().filter(|d| (1..=7).contains(d)).collect();
    d.sort_unstable();
    d.dedup();
    if d.len() == 7 { return "every day".into(); }
    let consecutive = d.len() >= 3 && d.windows(2).all(|p| p[1] == p[0] + 1);
    let name = |x: u8| DAY_NAMES[(x - 1) as usize];
    if consecutive {
        format!("{}-{}", name(d[0]), name(d[d.len() - 1]))
    } else {
        d.iter().map(|&x| name(x)).collect::<Vec<_>>().join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn w(days: &[u8], start: &str, end: &str) -> ScheduleWindow {
        ScheduleWindow { days: days.to_vec(), start: start.into(), end: end.into() }
    }
    /// A UTC instant from an Eastern wall-clock time, so the tests read in the
    /// zone the operator types.
    fn et(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        chrono_tz::US::Eastern.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn an_empty_schedule_is_always_open() {
        assert!(is_open(&[], Utc::now()));
        assert_eq!(describe(&[]), "always");
    }

    #[test]
    fn a_weekday_window_opens_at_its_start_and_closes_at_its_end() {
        let s = [w(&[1, 2, 3, 4, 5], "09:30", "16:00")];
        // Wednesday 2026-11-18.
        assert!(!is_open(&s, et(2026, 11, 18, 9, 29)));
        assert!(is_open(&s, et(2026, 11, 18, 9, 30)), "the start minute is inside");
        assert!(is_open(&s, et(2026, 11, 18, 15, 59)));
        assert!(!is_open(&s, et(2026, 11, 18, 16, 0)), "the end minute is outside");
        // Saturday 2026-11-21 at noon: a weekday-only window is shut.
        assert!(!is_open(&s, et(2026, 11, 21, 12, 0)));
        assert_eq!(describe(&s), "Mon-Fri 09:30-16:00 ET");
    }

    #[test]
    fn an_overnight_window_spans_midnight_into_the_next_day() {
        // Friday evening into Saturday morning only.
        let s = [w(&[5], "22:00", "06:00")];
        assert!(is_open(&s, et(2026, 11, 20, 22, 0)), "Friday 22:00 opens it");
        assert!(is_open(&s, et(2026, 11, 20, 23, 59)));
        assert!(is_open(&s, et(2026, 11, 21, 3, 0)), "Saturday 03:00 is the same window");
        assert!(!is_open(&s, et(2026, 11, 21, 6, 0)), "Saturday 06:00 closes it");
        assert!(!is_open(&s, et(2026, 11, 21, 22, 30)), "Saturday evening is not listed");
        assert!(!is_open(&s, et(2026, 11, 20, 3, 0)), "Friday 03:00 belongs to Thursday's night, which is not listed");
        // Sunday night wraps to Monday morning.
        let sun = [w(&[7], "23:00", "01:00")];
        assert!(is_open(&sun, et(2026, 11, 23, 0, 30)), "Monday 00:30 is Sunday's window");
    }

    #[test]
    fn several_windows_are_a_union_and_24_00_is_end_of_day() {
        let s = [w(&[1, 2, 3, 4, 5], "09:00", "11:00"), w(&[1, 2, 3, 4, 5], "14:00", "24:00"), w(&[6], "10:00", "12:00")];
        assert!(is_open(&s, et(2026, 11, 18, 10, 0)));
        assert!(!is_open(&s, et(2026, 11, 18, 12, 0)), "between the two weekday windows");
        assert!(is_open(&s, et(2026, 11, 18, 23, 59)), "24:00 keeps the window open to the last minute");
        assert!(!is_open(&s, et(2026, 11, 19, 0, 0)), "and closed at midnight");
        assert!(is_open(&s, et(2026, 11, 21, 11, 0)), "Saturday's own window");
        assert_eq!(describe(&s), "Mon-Fri 09:00-11:00, Mon-Fri 14:00-24:00, Sat 10:00-12:00 ET");
    }

    #[test]
    fn midnight_to_midnight_is_the_whole_of_each_listed_day() {
        let s = [w(&[6, 7], "00:00", "00:00")];
        assert!(is_open(&s, et(2026, 11, 21, 0, 0)), "Saturday 00:00");
        assert!(is_open(&s, et(2026, 11, 21, 12, 0)));
        assert!(is_open(&s, et(2026, 11, 22, 23, 59)), "Sunday's last minute");
        assert!(!is_open(&s, et(2026, 11, 23, 0, 0)), "Monday 00:00 is closed: Sunday's window ends at midnight");
        assert!(!is_open(&s, et(2026, 11, 20, 12, 0)), "Friday is not listed");
        assert_eq!(describe(&s), "Sat, Sun all day ET");
        assert_eq!(describe(&[w(&[1], "00:00", "24:00")]), "Mon all day ET");
    }

    #[test]
    fn the_clock_is_eastern_with_daylight_saving() {
        // 2026-07-15 13:30 UTC is 09:30 EDT; 2026-12-15 13:30 UTC is 08:30 EST.
        let s = [w(&[1, 2, 3, 4, 5], "09:00", "10:00")];
        assert!(is_open(&s, Utc.with_ymd_and_hms(2026, 7, 15, 13, 30, 0).unwrap()));
        assert!(!is_open(&s, Utc.with_ymd_and_hms(2026, 12, 15, 13, 30, 0).unwrap()));
    }

    #[test]
    fn validation_names_the_window_and_the_fault() {
        assert!(validate(&[]).is_ok());
        assert!(validate(&[w(&[1, 7], "09:00", "17:00")]).is_ok());
        assert!(validate(&[w(&[5], "22:00", "06:00")]).is_ok(), "overnight is allowed");
        assert_eq!(validate(&[w(&[], "09:00", "17:00")]).unwrap_err(), "schedule window 1 names no days");
        assert!(validate(&[w(&[0], "09:00", "17:00")]).unwrap_err().contains("day 0"));
        assert!(validate(&[w(&[8], "09:00", "17:00")]).unwrap_err().contains("day 8"));
        assert!(validate(&[w(&[1], "9:00", "17:00")]).unwrap_err().contains("not HH:MM"));
        assert!(validate(&[w(&[1], "09:60", "17:00")]).unwrap_err().contains("not HH:MM"));
        assert!(validate(&[w(&[1], "09:00", "24:30")]).unwrap_err().contains("not HH:MM"));
        assert!(validate(&[w(&[1], "24:00", "06:00")]).unwrap_err().contains("start at 24:00"));
        assert!(validate(&[w(&[1], "09:00", "17:00"), w(&[2], "10:00", "10:00")]).unwrap_err().starts_with("schedule window 2"));
        assert!(validate(&[w(&[6, 7], "00:00", "00:00")]).is_ok(), "00:00 to 00:00 is a whole day");
        assert_eq!(parse_hhmm("24:00"), Some(1440));
        assert_eq!(parse_hhmm(" 07:05 "), Some(425));
    }

    #[test]
    fn day_ranges_read_naturally() {
        assert_eq!(day_range(&[1, 2, 3, 4, 5, 6, 7]), "every day");
        assert_eq!(day_range(&[1, 3, 5]), "Mon, Wed, Fri");
        assert_eq!(day_range(&[6, 7]), "Sat, Sun");
        assert_eq!(day_range(&[3, 2, 1]), "Mon-Wed");
        assert_eq!(closed_reason(&[w(&[6, 7], "10:00", "12:00")]), "outside the viper's schedule (Sat, Sun 10:00-12:00 ET)");
    }
}
