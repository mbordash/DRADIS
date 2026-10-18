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

//! The hourly market recorder: GBoost plan B's training store, written live.
//!
//! On Polymarket International the plan-B pipeline backfills its store from the
//! venue's public history (`gboost_planb_train::catch_up`). Kalshi publishes no
//! price history, so a Kalshi instance has to write its own: while the venue loop
//! flies a one-hour market, this samples both legs' mids once a minute from the
//! book it is already streaming, and at the close fetches the hour's prints and
//! writes one [`MarketRecord`] in exactly the shape the fetcher writes, into the
//! same directory, under the same window key. The pipeline then fills the
//! outcome from the venue's settlement (`venues::core::resolve_leg`) and trains
//! from the records once there are enough of them.
//!
//! Venue-neutral by construction: it reads a `MarketSnapshot`'s mids and the
//! `Execution::recent_prints` tape, so any venue loop can own one.

use std::collections::HashMap;

use crate::venues::core::{Side, TapePrint};
use crate::vipers::gboost_planb_train::{DataDir, HistPoint, MarketRecord, Print};

/// RFC 3339 for a unix second, as the fetcher writes `window_start`.
fn rfc3339(t: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(t, 0).map(|d| d.to_rfc3339()).unwrap_or_default()
}

/// The `outcomePrices` string Gamma publishes for a resolved market, which
/// `MarketRecord::up_won` reads: `["1","0"]` when the YES/Up leg won.
pub fn outcome_prices(up_won: bool) -> String {
    if up_won { "[\"1\",\"0\"]".to_string() } else { "[\"0\",\"1\"]".to_string() }
}

/// A venue-neutral print in the store's shape: `o` is the leg (0 YES/Up, 1 NO/Down),
/// `side` the taker's.
pub fn print_from_tape(p: &TapePrint) -> Print {
    use rust_decimal::prelude::ToPrimitive;
    Print {
        ts: p.at.timestamp(),
        side: match p.taker_side { Side::Buy => "BUY".to_string(), Side::Sell => "SELL".to_string() },
        o: if p.leg_is_yes { 0 } else { 1 },
        p: p.price.to_f64().unwrap_or(0.0),
        s: p.size.to_f64().unwrap_or(0.0),
    }
}

/// One hourly market being recorded.
#[derive(Debug)]
pub struct HourlyRecorder {
    w: i64,
    ticker: String,
    tokens: [String; 2],
    strike: Option<f64>,
    hist: [Vec<HistPoint>; 2],
    last_minute: Option<i64>,
}

impl HourlyRecorder {
    /// A recorder for the market closing at `close_s` (unix seconds); its window
    /// starts an hour earlier, as the store keys it.
    pub fn new(ticker: &str, yes_token: &str, no_token: &str, close_s: i64, strike: Option<f64>) -> Self {
        Self {
            w: close_s - 3600,
            ticker: ticker.to_string(),
            tokens: [yes_token.to_string(), no_token.to_string()],
            strike,
            hist: [Vec::with_capacity(61), Vec::with_capacity(61)],
            last_minute: None,
        }
    }

    pub fn window_start(&self) -> i64 { self.w }
    pub fn ticker(&self) -> &str { &self.ticker }
    /// Minutes sampled so far.
    pub fn samples(&self) -> usize { self.hist[0].len() }

    /// Record both legs' mids for the minute containing `now_s`, once per minute:
    /// the first tick of each minute is the sample, as a one-minute price history
    /// would publish it. A leg with no two-sided quote contributes nothing that
    /// minute (never a zero), and the minute is left open for a later tick.
    pub fn sample(&mut self, now_s: i64, mid_yes: Option<f64>, mid_no: Option<f64>) {
        let minute = now_s.div_euclid(60) * 60;
        if self.last_minute == Some(minute) { return; }
        let (Some(y), Some(n)) = (mid_yes, mid_no) else { return };
        if !(y > 0.0 && y < 1.0 && n > 0.0 && n < 1.0) { return; }
        self.hist[0].push(HistPoint { t: now_s, p: y });
        self.hist[1].push(HistPoint { t: now_s, p: n });
        self.last_minute = Some(minute);
    }

    /// The record at the close. The outcome is unknown until the venue settles;
    /// `outcome_prices` stays `None` and the pipeline fills it.
    pub fn finish(self, prints: &[TapePrint]) -> MarketRecord {
        let mut tape: Vec<Print> = prints.iter().map(print_from_tape).collect();
        tape.sort_by_key(|p| p.ts);
        let mut hist = HashMap::new();
        hist.insert("0".to_string(), self.hist[0].clone());
        hist.insert("1".to_string(), self.hist[1].clone());
        MarketRecord {
            slug: self.ticker,
            missing: false,
            window_start: rfc3339(self.w),
            condition_id: None,
            tokens: self.tokens.to_vec(),
            outcome_prices: None,
            closed: Some(true),
            end_date: Some(rfc3339(self.w + 3600)),
            tape,
            hist,
            strike: self.strike,
        }
    }
}

/// Write `rec` under its window key in `dir`, atomically (temp file, then rename).
/// Returns the path written.
pub fn write_record(dir: &DataDir, w: i64, rec: &MarketRecord) -> std::io::Result<std::path::PathBuf> {
    std::fs::create_dir_all(dir.markets())?;
    let path = dir.market(w);
    let tmp = path.with_extension("json.tmp");
    let json = serde_json::to_string(rec).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    std::fs::write(&tmp, json)?;
    std::fs::rename(&tmp, &path)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vipers::gboost_planb::Bar;
    use crate::vipers::gboost_planb_train::{build_market_rows, Plan};
    use crate::helpers::dynamic_config::DynamicConfig;
    use rust_decimal_macros::dec;

    fn tape_print(at: i64, yes: bool, buy: bool, price: &str) -> TapePrint {
        TapePrint {
            at: chrono::DateTime::<chrono::Utc>::from_timestamp(at, 0).unwrap(),
            taker_side: if buy { Side::Buy } else { Side::Sell },
            leg_is_yes: yes,
            price: price.parse().unwrap(),
            size: dec!(10),
        }
    }

    /// One sample per minute, from the first tick that has both mids; a one-sided
    /// book leaves the minute open rather than recording a zero.
    #[test]
    fn samples_once_a_minute_and_never_a_zero() {
        let mut r = HourlyRecorder::new("KXBTCD-26OCT1719-T91299.99", "k#yes", "k#no", 7200, Some(91_299.99));
        assert_eq!(r.window_start(), 3600);
        r.sample(3600, None, Some(0.55));          // one-sided: nothing yet
        r.sample(3601, Some(0.45), Some(0.55));    // the minute's sample
        r.sample(3630, Some(0.46), Some(0.54));    // same minute: ignored
        r.sample(3660, Some(0.0), Some(0.54));     // a zero mid is not a price
        r.sample(3661, Some(0.47), Some(0.53));
        assert_eq!(r.samples(), 2);
        let rec = r.finish(&[]);
        assert_eq!(rec.hist["0"].iter().map(|p| p.p).collect::<Vec<_>>(), vec![0.45, 0.47]);
        assert_eq!(rec.hist["1"][0].t, 3601);
    }

    /// The record is the fetcher's shape: window key, tokens, a sorted tape with the
    /// store's leg and side encoding, the market's strike, and no outcome yet.
    #[test]
    fn the_record_is_the_fetchers_shape_with_the_outcome_left_open() {
        let r = HourlyRecorder::new("KXBTCD-T", "k#yes", "k#no", 7200, Some(81_500.0));
        let rec = r.finish(&[tape_print(5000, false, false, "0.60"), tape_print(4000, true, true, "0.42")]);
        assert_eq!(rec.slug, "KXBTCD-T");
        assert_eq!(rec.window_start, rfc3339(3600));
        assert_eq!(rec.tokens, vec!["k#yes", "k#no"]);
        assert_eq!(rec.strike, Some(81_500.0));
        assert_eq!(rec.outcome_prices, None, "the venue has not settled yet");
        assert!(rec.up_won().is_none());
        assert_eq!(rec.tape.iter().map(|p| p.ts).collect::<Vec<_>>(), vec![4000, 5000], "sorted by time");
        assert_eq!((rec.tape[0].o, rec.tape[0].side.as_str(), rec.tape[0].p), (0, "BUY", 0.42));
        assert_eq!((rec.tape[1].o, rec.tape[1].side.as_str()), (1, "SELL"));
        assert_eq!(outcome_prices(true), "[\"1\",\"0\"]");
        let mut up = rec.clone(); up.outcome_prices = Some(outcome_prices(true));
        assert_eq!(up.up_won(), Some(true));
        let mut dn = rec.clone(); dn.outcome_prices = Some(outcome_prices(false));
        assert_eq!(dn.up_won(), Some(false));
    }

    /// A full hour recorded live, settled, trains: the row builder accepts the
    /// record exactly as it accepts a fetched one, and uses the market's strike.
    #[test]
    fn a_recorded_hour_builds_training_rows() {
        let w = 1_792_000_800i64; // on the hour
        let mut r = HourlyRecorder::new("KXBTCD-T", "k#yes", "k#no", w + 3600, Some(100.0));
        let mut prints = Vec::new();
        for m in 0..60 {
            let t = w + 60 * m;
            r.sample(t + 1, Some(0.50 + m as f64 * 0.002), Some(0.50 - m as f64 * 0.002));
            prints.push(tape_print(t + 20, true, true, "0.52"));
            prints.push(tape_print(t + 40, false, true, "0.49"));
        }
        let mut rec = r.finish(&prints);
        rec.outcome_prices = Some(outcome_prices(true));
        // Bars from an hour before the window through its end, flat at 100 with a drift.
        let bars: Vec<Bar> = ((w - 62 * 60)..(w + 3600)).step_by(60)
            .map(|o| { let px = 100.0 + ((o - w) as f64) / 36_000.0; Bar { open_s: o, open: px, high: px + 0.1, low: px - 0.1, close: px + 0.01 } })
            .collect();
        let plan = Plan::from_config(&DynamicConfig::default(), 0.07);
        let rows = build_market_rows(w, &rec, &bars, &[], (&[], &[]), &plan);
        assert!(!rows.is_empty(), "a recorded hour must train like a fetched one");
        assert!(rows.iter().all(|row| row.w == w));
    }

    /// The record lands under the store's window key and survives a round trip.
    #[test]
    fn the_record_is_written_under_the_window_key() {
        let dir = DataDir::at(std::env::temp_dir().join(format!("dradis-record-{}", std::process::id())));
        let _ = std::fs::remove_dir_all(&dir.root);
        let rec = HourlyRecorder::new("KXBTCD-T", "k#yes", "k#no", 7200, None).finish(&[]);
        let path = write_record(&dir, 3600, &rec).unwrap();
        assert_eq!(path, dir.market(3600));
        let back: MarketRecord = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(back.slug, "KXBTCD-T");
        let _ = std::fs::remove_dir_all(&dir.root);
    }
}
