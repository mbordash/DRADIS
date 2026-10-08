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

//! Hour-of-day volatility profile for the lognormal fair-value model.
//!
//! FairValue estimates σ from the trailing hour of prices. That estimate is
//! wrong in a direction set by the clock: BTC's 1-minute volatility rises into
//! the US pre-open and open (8 to 10 ET), falls through the afternoon (11 to
//! 16 ET), and rises again at 20 ET, which is 00:00 UTC, the Binance daily close
//! and funding settlement. Measured over every weekday hour from 2026-05-15 to
//! 2026-10-07 (3,501 hour boundaries), the volatility of the coming hour over the
//! trailing hour's ran a median 1.33x at 9 ET, 1.22x at 8 ET, 1.24x at 20 ET and
//! 0.83x to 0.89x from 11 to 16 ET, with every figure holding in both halves of
//! the sample. The ratio also depends on the minute: at 9:30 ET, with the open
//! inside the remaining half hour, it reaches 1.77x.
//!
//! A trailing σ scaled by this profile was the only estimator tested that beat
//! the plain trailing window at forecasting the coming hour's volatility (RMSE of
//! the log ratio 0.369 against 0.383, and at 9 ET 0.399 against 0.475 with the
//! 27% under-estimate removed). Same-hour-yesterday (0.630) and a 24-hour window
//! (0.441) were worse. One-hour volatility is noisy whatever the estimator; the
//! value here is removing a bias that is systematic by the clock.
//!
//! Applied to FairValue's 34 real trades with model inputs, the profile would
//! have refused 11 (8 losers), turning −$8.95 into −$3.76; the deployed 9:30
//! point event alone recovers +$3.58 of that. On 34 trades the dollar figure is
//! suggestive (p ≈ 0.17 against random refusals); the volatility structure it
//! rests on is not in doubt. With perfect knowledge of realized σ the kept trades
//! still lose $0.31, so this corrects the viper's aim and does not by itself make
//! it profitable: the market is calibrated once σ is right.
//!
//! The profile is a table of medians by (ET hour, 10-minute bucket), built from
//! the 1-minute closes the GBoost pipeline already keeps under
//! `logs/gboost_planb/<asset>/klines`, weekdays only, from data strictly before
//! the time it is applied to. It is data, not a knob; the knobs are whether it is
//! used, the smallest cell it may be read from, and how far back it is fitted.

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::America::New_York as Et;
use std::collections::HashMap;

/// Width of a minute bucket. Finer than this and the cells thin out; coarser and
/// the 9:30 open blurs into the quiet twenty minutes before it.
pub const BUCKET_MINS: u32 = 10;
/// A forward window shorter than this is too few returns to estimate σ from, so
/// the last bucket of each hour (:50) has no cell and the caller falls back.
const MIN_FORWARD_MINS: i64 = 15;

/// One cell of the profile: the median forward-over-trailing σ ratio and how
/// many weekday observations it rests on.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cell {
    pub factor: f64,
    pub n: usize,
}

/// The fitted table.
#[derive(Debug, Clone)]
pub struct SigmaProfile {
    cells: HashMap<(u32, u32), Cell>,
    pub built_at: DateTime<Utc>,
    /// How many 1-minute closes the fit saw, for the operator's log line.
    pub bars: usize,
}

/// The bucket a minute belongs to: 0, 10, 20, 30, 40 or 50.
pub fn minute_bucket(minute: u32) -> u32 {
    (minute / BUCKET_MINS) * BUCKET_MINS
}

/// Population standard deviation of the `mins` one-minute log returns starting
/// at `from`, per root second: the same estimator the engine prices with. `None`
/// when any bar is missing, so a gap in the store never reads as calm.
fn sigma_per_sqrt_sec(closes: &HashMap<i64, f64>, from: i64, mins: i64) -> Option<f64> {
    if mins < 3 {
        return None;
    }
    let mut rets = Vec::with_capacity(mins as usize);
    for i in 0..mins {
        let a = *closes.get(&(from + 60 * i))?;
        let b = *closes.get(&(from + 60 * (i + 1)))?;
        if a <= 0.0 || b <= 0.0 {
            return None;
        }
        rets.push((b / a).ln());
    }
    let n = rets.len() as f64;
    let mean = rets.iter().sum::<f64>() / n;
    let var = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
    Some(var.sqrt() / 60f64.sqrt())
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = v.len();
    if n % 2 == 1 { v[n / 2] } else { (v[n / 2 - 1] + v[n / 2]) / 2.0 }
}

impl SigmaProfile {
    /// Fit the table from 1-minute closes keyed by bar open time (UTC seconds).
    ///
    /// For every weekday in the last `lookback_days` (in Eastern time, so the
    /// clock the structure lives on), every hour and every bucket: the σ of the
    /// REMAINING minutes of that hour over the σ of the 60 minutes before it.
    /// That is exactly the quantity FairValue needs for an hourly market, whose
    /// horizon ends at the top of the hour. Only windows that have fully closed
    /// before `now` are used, so the fit never looks past the moment it serves.
    pub fn build(closes: &HashMap<i64, f64>, now: DateTime<Utc>, lookback_days: i64) -> Self {
        let mut samples: HashMap<(u32, u32), Vec<f64>> = HashMap::new();
        let end_day = now.with_timezone(&Et).date_naive();
        let mut day = end_day - Duration::days(lookback_days.max(1));
        let now_s = now.timestamp();
        while day <= end_day {
            if !matches!(day.weekday(), Weekday::Sat | Weekday::Sun) {
                for h in 0..24u32 {
                    let mut mb = 0u32;
                    while (60 - mb as i64) >= MIN_FORWARD_MINS {
                        let rem = 60 - mb as i64;
                        let at = day
                            .and_hms_opt(h, mb, 0)
                            .and_then(|naive| Et.from_local_datetime(&naive).earliest())
                            .map(|t| t.timestamp());
                        if let Some(t) = at {
                            if t + rem * 60 <= now_s {
                                if let (Some(fwd), Some(trail)) = (
                                    sigma_per_sqrt_sec(closes, t, rem),
                                    sigma_per_sqrt_sec(closes, t - 3600, 60),
                                ) {
                                    if trail > 0.0 {
                                        samples.entry((h, mb)).or_default().push(fwd / trail);
                                    }
                                }
                            }
                        }
                        mb += BUCKET_MINS;
                    }
                }
            }
            match day.succ_opt() {
                Some(next) => day = next,
                None => break,
            }
        }
        let cells = samples
            .into_iter()
            .map(|(k, mut v)| { let n = v.len(); (k, Cell { factor: median(&mut v), n }) })
            .collect();
        Self { cells, built_at: now, bars: closes.len() }
    }

    /// The factor for a decision taken at `at`, or `None` when the profile has
    /// nothing trustworthy to say: a weekend (the structure is a weekday one),
    /// the last bucket of the hour, or a cell with fewer than `min_cell_n`
    /// observations. `None` means fall back, never "assume 1.0".
    pub fn factor(&self, at: DateTime<Utc>, min_cell_n: usize) -> Option<f64> {
        let local = at.with_timezone(&Et);
        if matches!(local.weekday(), Weekday::Sat | Weekday::Sun) {
            return None;
        }
        let cell = self.cells.get(&(local.hour(), minute_bucket(local.minute())))?;
        (cell.n >= min_cell_n).then_some(cell.factor)
    }

    pub fn cell(&self, hour_et: u32, minute_bucket: u32) -> Option<Cell> {
        self.cells.get(&(hour_et, minute_bucket)).copied()
    }

    pub fn len(&self) -> usize {
        self.cells.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Synthetic 1-minute closes over `days` Eastern days, with returns that
    /// alternate ±amp so σ is exact: `amp` normally, `hot_amp` from 9:30 to
    /// 11:00 ET on weekdays, and ten times that on weekends (which the fit must
    /// ignore). Deterministic, so the cells are exact ratios.
    fn synthetic(first: chrono::NaiveDate, days: i64, amp: f64, hot_amp: f64) -> (HashMap<i64, f64>, DateTime<Utc>) {
        let mut closes = HashMap::new();
        let mut px = 85_000.0f64;
        let mut last = 0i64;
        for d in 0..days {
            let day = first + Duration::days(d);
            let midnight = Et.from_local_datetime(&day.and_hms_opt(0, 0, 0).unwrap()).earliest().unwrap().timestamp();
            let weekend = matches!(day.weekday(), Weekday::Sat | Weekday::Sun);
            for m in 0..1440i64 {
                let t = midnight + 60 * m;
                let (h, mi) = ((m / 60) as u32, (m % 60) as u32);
                let hot = (h == 9 && mi >= 30) || h == 10;
                let a = if weekend { hot_amp * 10.0 } else if hot { hot_amp } else { amp };
                px *= if m % 2 == 0 { a.exp() } else { (-a).exp() };
                closes.insert(t, px);
                last = t;
            }
        }
        (closes, Utc.timestamp_opt(last + 60, 0).unwrap())
    }

    const S: f64 = 1e-4;

    /// Forward-over-trailing σ for a window of `rem` returns holding `hot_f`
    /// hot ones, over a trailing hour holding `hot_t`. Windows are close to
    /// close, so the window starting at 9:00 holds the returns ending 9:01
    /// through 10:00: one more hot return than the regime's clock boundary
    /// suggests, which is why these are counted rather than assumed.
    /// Compared at 1e-3, not exactly: the alternating signs leave a window
    /// with an odd ± split a mean of ±amp/60, which moves the population σ by
    /// about 1e-4 relative. A miscounted return would move the ratio by ~0.02.
    const TOL: f64 = 1e-3;

    fn expected(rem: f64, hot_f: f64, hot_t: f64, hot: f64) -> f64 {
        let fwd = ((rem - hot_f + hot_f * hot * hot) / rem).sqrt();
        let trail = ((60.0 - hot_t + hot_t * hot * hot) / 60.0).sqrt();
        fwd / trail
    }

    /// The ratio the table stores is exactly forward-over-trailing σ.
    #[test]
    fn cells_are_the_forward_over_trailing_ratio() {
        let first = chrono::NaiveDate::from_ymd_opt(2026, 9, 1).unwrap(); // a Tuesday
        let (closes, now) = synthetic(first, 30, S, 2.0 * S);
        let p = SigmaProfile::build(&closes, now, 60);
        let f = |h, mb| p.cell(h, mb).unwrap().factor;
        // 9:00: returns 9:01..10:00, hot from 9:30: 31 hot of 60, over a calm hour.
        assert!((f(9, 0) - expected(60.0, 31.0, 0.0, 2.0)).abs() < TOL, "{}", f(9, 0));
        // 9:30: returns 9:31..10:00 all hot; trailing 8:31..9:30 has one hot return.
        assert!((f(9, 30) - expected(30.0, 30.0, 1.0, 2.0)).abs() < TOL, "{}", f(9, 30));
        // 11:00: returns 11:01..12:00 calm; trailing 10:01..11:00 has 59 hot.
        assert!((f(11, 0) - expected(60.0, 0.0, 59.0, 2.0)).abs() < TOL, "{}", f(11, 0));
        // 14:00: calm over calm.
        assert!((f(14, 0) - 1.0).abs() < 1e-6, "{}", f(14, 0));
    }

    /// Weekends are left out of the fit and out of the lookup.
    #[test]
    fn weekends_are_excluded_from_fit_and_lookup() {
        let first = chrono::NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let (closes, now) = synthetic(first, 30, S, 2.0 * S);
        let p = SigmaProfile::build(&closes, now, 60);
        // Weekend bars were ten times hotter; a weekday cell is untouched by them.
        assert!((p.cell(14, 0).unwrap().factor - 1.0).abs() < 1e-6);
        // Saturday 2026-09-05 at 14:00 ET.
        let sat = Et.with_ymd_and_hms(2026, 9, 5, 14, 0, 0).unwrap().with_timezone(&Utc);
        assert_eq!(p.factor(sat, 1), None);
    }

    /// A thin cell is refused rather than trusted.
    #[test]
    fn a_cell_below_the_minimum_count_yields_none() {
        let first = chrono::NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let (closes, now) = synthetic(first, 14, S, 2.0 * S); // 10 weekdays
        let p = SigmaProfile::build(&closes, now, 60);
        let tue = Et.with_ymd_and_hms(2026, 9, 8, 14, 0, 0).unwrap().with_timezone(&Utc);
        assert_eq!(p.cell(14, 0).unwrap().n, 10);
        assert!(p.factor(tue, 10).is_some());
        assert_eq!(p.factor(tue, 11), None);
    }

    /// The last bucket of the hour has too short a forward window for a cell.
    #[test]
    fn the_fifty_minute_bucket_has_no_cell() {
        let first = chrono::NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let (closes, now) = synthetic(first, 20, S, 2.0 * S);
        let p = SigmaProfile::build(&closes, now, 60);
        assert!(p.cell(14, 40).is_some());
        assert_eq!(p.cell(14, 50), None);
        let t = Et.with_ymd_and_hms(2026, 9, 8, 14, 55, 0).unwrap().with_timezone(&Utc);
        assert_eq!(p.factor(t, 1), None, "the caller falls back in the last ten minutes");
    }

    /// A window that has not closed yet is not in the fit.
    #[test]
    fn an_unfinished_window_is_not_counted() {
        let first = chrono::NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let (closes, _) = synthetic(first, 21, S, 2.0 * S); // Tue Sep 1 .. Mon Sep 21
        // "Now" is 14:20 ET on the last day: the (14,0) window for that day is open.
        let now = Et.with_ymd_and_hms(2026, 9, 21, 14, 20, 0).unwrap().with_timezone(&Utc);
        let p = SigmaProfile::build(&closes, now, 60);
        let n_1400 = p.cell(14, 0).unwrap().n;
        let n_1300 = p.cell(13, 0).unwrap().n;
        assert_eq!(n_1300, n_1400 + 1, "13:00 closed before now on the last day; 14:00 did not");
    }

    /// The table is indexed in Eastern time, so 9:30 ET is read at 14:30 UTC in
    /// December and 13:30 UTC in September.
    #[test]
    fn the_lookup_follows_eastern_time_across_daylight_saving() {
        let first = chrono::NaiveDate::from_ymd_opt(2026, 11, 2).unwrap(); // after the Nov 1 change
        let (closes, now) = synthetic(first, 20, S, 2.0 * S);
        let p = SigmaProfile::build(&closes, now, 60);
        let dec_1430utc = Utc.with_ymd_and_hms(2026, 12, 8, 14, 30, 0).unwrap();
        assert!((p.factor(dec_1430utc, 1).unwrap() - expected(30.0, 30.0, 1.0, 2.0)).abs() < TOL, "14:30 UTC is 9:30 EST");
        let dec_1330utc = Utc.with_ymd_and_hms(2026, 12, 8, 13, 30, 0).unwrap();
        assert!((p.factor(dec_1330utc, 1).unwrap() - 1.0).abs() < 1e-6, "13:30 UTC is 8:30 EST, calm");
    }

    #[test]
    fn median_handles_even_and_odd_counts() {
        assert_eq!(median(&mut [3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&mut [4.0, 1.0, 3.0, 2.0]), 2.5);
    }
}
