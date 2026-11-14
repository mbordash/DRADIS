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

//! What a script sees: a read-only map built from the `StrategyContext` once
//! per evaluation.
//!
//! Prices are `Decimal`, times are whole seconds, and there are no venue
//! identifiers or `DateTime` values. A signal whose Raptor is not running on
//! this squadron is `()` rather than 0, so a script on a sports squadron cannot
//! read a missing oracle as a price of zero. This map is the script API's
//! contract: renaming a field breaks operators' scripts.

use std::collections::BTreeMap;

use chrono::{DateTime, Datelike, Timelike, Utc};
use rhai::{Dynamic, Map};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;

use crate::orchestrator::strategy::StrategyContext;
use super::manifest::ParamValue;

fn d(x: Decimal) -> Dynamic { Dynamic::from_decimal(x) }
fn i(x: i64) -> Dynamic { Dynamic::from_int(x) }
fn f(x: f64) -> Dynamic { Decimal::from_f64(x).map(|v| d(v.round_dp(10))).unwrap_or(Dynamic::UNIT) }
fn opt<T>(x: Option<T>, g: impl FnOnce(T) -> Dynamic) -> Dynamic { x.map(g).unwrap_or(Dynamic::UNIT) }

/// The tick's wall clock in US/Eastern (the engine's time zone), as
/// `(hour 0-23, minute 0-59, weekday 1 = Monday .. 7 = Sunday)`. Daylight
/// saving is applied here so a script that flies only certain hours does not
/// have to work it out from `now_secs`.
fn et_clock(now: DateTime<Utc>) -> (i64, i64, i64) {
    let et = now.with_timezone(&chrono_tz::US::Eastern);
    (et.hour() as i64, et.minute() as i64, et.weekday().number_from_monday() as i64)
}

/// The account and budget figures the adapter computes for this viper.
pub struct Account {
    pub ghost: bool,
    pub exposure: Decimal,
    pub max_exposure: Decimal,
    pub day_pnl: Decimal,
}

pub fn build(ctx: &StrategyContext, params: &BTreeMap<String, ParamValue>, acct: &Account) -> Map {
    let now = Utc::now();
    let s = &ctx.snapshot;
    let m = &ctx.market;
    let whole_book = ctx.dynamic_config.obi_use_whole_book;
    let mut c = Map::new();

    // ── Market ──
    c.insert("market_name".into(), m.market_name.clone().into());
    c.insert("market_class".into(), opt(ctx.market_class.clone(), Dynamic::from));
    c.insert("venue".into(), crate::venues::venue_name().into());
    c.insert("strike".into(), opt(m.strike_price, d));
    c.insert("secs_to_close".into(), opt(m.market_close_time.map(|t| (t - now).num_seconds()), i));
    c.insert("market_age_secs".into(), i((now - ctx.market_started_at).num_seconds().max(0)));
    c.insert("window_secs".into(), opt(m.window_secs, i));
    c.insert("now_secs".into(), i(now.timestamp()));
    let (et_hour, et_minute, et_weekday) = et_clock(now);
    c.insert("et_hour".into(), i(et_hour));
    c.insert("et_minute".into(), i(et_minute));
    c.insert("et_weekday".into(), i(et_weekday));
    c.insert("book_age_secs".into(), i((now - s.timestamp).num_seconds().max(0)));

    // ── Book ──
    let side = |bid, ask, bd, ad, bdt, adt, obi| {
        let mut x = Map::new();
        x.insert("bid".into(), d(bid));
        x.insert("ask".into(), d(ask));
        x.insert("bid_depth".into(), d(bd));
        x.insert("ask_depth".into(), d(ad));
        x.insert("bid_depth_total".into(), d(bdt));
        x.insert("ask_depth_total".into(), d(adt));
        x.insert("obi".into(), d(obi));
        Dynamic::from_map(x)
    };
    c.insert("yes".into(), side(s.yes_bid, s.yes_ask, s.yes_bid_depth, s.yes_ask_depth,
        s.yes_bid_depth_total, s.yes_ask_depth_total, s.yes_obi(whole_book)));
    c.insert("no".into(), side(s.no_bid, s.no_ask, s.no_bid_depth, s.no_ask_depth,
        s.no_bid_depth_total, s.no_ask_depth_total, s.no_obi(whole_book)));

    // ── Signals: `()` without a live price feed behind them ──
    // [B95] A squadron with a feed attached still reads an oracle of 0 until
    // the first print after a start, and again while the feed is down (the
    // price raptor publishes 0 for "not connected"). Every rate is 0 with it.
    // A script must see "no price" there, never a price of zero: on production
    // 2026-11-07 a script bought YES one second after a restart because a $0
    // level sat $75,800 below the strike and its model read that as certain.
    let feed = ctx.has_price_feed && s.oracle_price > Decimal::ZERO;
    let sig = |x: Decimal| if feed { d(x) } else { Dynamic::UNIT };
    c.insert("has_price_feed".into(), feed.into());
    for (k, v) in [
        ("oracle", s.oracle_price), ("velocity", s.velocity), ("velocity_1s", s.velocity_1s),
        ("acceleration", s.acceleration), ("drift_10m", s.oracle_drift_10m),
        ("drift_60m", s.oracle_drift_60m), ("hist_vol", s.hist_vol), ("funding_rate", s.funding_rate),
        ("institutional_pulse", s.institutional_pulse), ("cvd_ratio", s.cvd_ratio),
        ("oi_delta_pct", s.oi_delta_pct), ("tide_coherence", s.tide_coherence),
        ("tradfi_velocity", s.tradfi_velocity), ("vix_proxy", s.vix_proxy),
    ] {
        c.insert(k.into(), sig(v));
    }

    // ── Sports consensus, when the ledger has matched this market ──
    c.insert("sports".into(), opt(ctx.sports.as_ref(), |line| {
        let one = |l: &crate::raptors::sports_ledger::SportsLine| {
            let mut x = Map::new();
            x.insert("consensus".into(), f(l.consensus));
            x.insert("num_books".into(), i(l.num_books));
            x.insert("dispersion".into(), opt(l.dispersion, f));
            x.insert("odds_age_secs".into(), i((now - l.odds_at).num_seconds().max(0)));
            x.insert("secs_to_start".into(), i((l.commence - now).num_seconds()));
            x.insert("league".into(), l.league.clone().into());
            Dynamic::from_map(x)
        };
        let mut x = Map::new();
        x.insert("yes".into(), opt(line.yes.as_ref(), one));
        x.insert("no".into(), opt(line.no.as_ref(), one));
        Dynamic::from_map(x)
    }));

    // ── Account ──
    c.insert("collateral".into(), d(ctx.available_collateral));
    c.insert("day_pnl".into(), d(acct.day_pnl));
    c.insert("ghost".into(), acct.ghost.into());
    c.insert("exposure".into(), d(acct.exposure));
    c.insert("max_exposure".into(), d(acct.max_exposure));
    c.insert("min_shares".into(), d(crate::venues::min_order_shares()));

    // ── Operator parameters ──
    c.insert("params".into(), params_value(params));
    c
}

/// `ctx.params` for a set of resolved parameter values.
pub fn params_value(params: &BTreeMap<String, ParamValue>) -> Dynamic {
    let mut p = Map::new();
    for (k, v) in params {
        p.insert(k.as_str().into(), match v {
            ParamValue::Bool(b) => (*b).into(),
            ParamValue::Int(n) => i(*n),
            ParamValue::Decimal(x) => d(*x),
        });
    }
    Dynamic::from_map(p)
}

/// One position as `exit(ctx, pos)` sees it.
pub struct PosView {
    pub side: &'static str,
    pub shares: Decimal,
    pub avg_entry: Decimal,
    pub entry_fee: Decimal,
    pub held_secs: i64,
    pub take_profit: Option<Decimal>,
    pub market_name: String,
}

pub fn position(p: &PosView) -> Map {
    let mut x = Map::new();
    x.insert("side".into(), p.side.into());
    x.insert("shares".into(), d(p.shares));
    x.insert("avg_entry".into(), d(p.avg_entry));
    x.insert("entry_fee".into(), d(p.entry_fee));
    x.insert("held_secs".into(), i(p.held_secs));
    x.insert("take_profit".into(), opt(p.take_profit, d));
    x.insert("market_name".into(), p.market_name.clone().into());
    x
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn et_clock_applies_daylight_saving() {
        // 2026-07-15 (Wednesday) 13:05 UTC is 09:05 EDT (UTC-4).
        assert_eq!(et_clock(Utc.with_ymd_and_hms(2026, 7, 15, 13, 5, 0).unwrap()), (9, 5, 3));
        // 2026-12-15 (Tuesday) 13:05 UTC is 08:05 EST (UTC-5).
        assert_eq!(et_clock(Utc.with_ymd_and_hms(2026, 12, 15, 13, 5, 0).unwrap()), (8, 5, 2));
    }

    #[test]
    fn et_clock_weekday_follows_eastern_date() {
        // Monday 2026-11-16 02:30 UTC is still Sunday 21:30 EST.
        assert_eq!(et_clock(Utc.with_ymd_and_hms(2026, 11, 16, 2, 30, 0).unwrap()), (21, 30, 7));
    }
}
