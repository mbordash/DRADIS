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

//! Parsers for Polymarket's public market-data JSON: the CLOB book and price
//! history, and the Data API trade tape.
//!
//! Venue-neutral on purpose. The GBoost trainer (`vipers::gboost_planb_train`)
//! has read these shapes since September and compiles on every venue build, so
//! the parsers cannot live under the feature-gated `venues::intl` module. The
//! Markets page's Polymarket International reads (`Execution::order_book`,
//! `recent_prints`, `price_history`) parse through the same functions, so the
//! trainer and the operator's view of a market can never disagree on a field.
//!
//! Every parser skips a malformed row rather than zeroing it: a print with no
//! price is not a print at $0.

use rust_decimal::Decimal;
use serde_json::Value;

/// A JSON number that Polymarket sometimes sends as a string ("0.57").
pub fn num(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// A venue price or size that arrived as an f64 (the trainer's parsers keep
/// f64 for parity). Polymarket quotes to four decimals and sizes to two, so
/// six places then `normalize` recovers the number the venue sent without the
/// binary tail `from_f64_retain` would keep (0.57 as 0.56999999999999995).
pub fn dec_from_f64(x: f64) -> Option<Decimal> {
    Decimal::from_f64_retain(x).map(|d| d.round_dp(6).normalize())
}

fn dec(v: &Value) -> Option<Decimal> {
    match v {
        Value::String(s) => s.trim().parse().ok(),
        Value::Number(n) => n.to_string().parse().ok(),
        _ => None,
    }
}

/// One Data API trade, as `/trades?market=<condition_id>` returns it.
#[derive(Clone, Debug, PartialEq)]
pub struct RawPrint {
    pub ts: i64,
    /// "BUY" or "SELL", the taker's side.
    pub side: String,
    /// 0 is the first outcome (YES or Up), 1 the second.
    pub outcome_index: u8,
    pub price: f64,
    pub size: f64,
}

/// Trade rows from a `/trades` page. Rows missing any field are skipped.
pub fn parse_trades(v: &Value) -> Vec<RawPrint> {
    let rows = v.as_array().cloned().unwrap_or_default();
    rows.iter()
        .filter_map(|x| {
            let ts = num(&x["timestamp"])? as i64;
            let side = x["side"].as_str()?.to_string();
            let outcome_index = num(&x["outcomeIndex"])? as u8;
            let price = num(&x["price"])?;
            let size = num(&x["size"])?;
            Some(RawPrint { ts, side, outcome_index, price, size })
        })
        .collect()
}

/// `(t, p)` points of a CLOB `prices-history` response, sorted by time.
pub fn parse_price_history(v: &Value) -> Vec<(i64, f64)> {
    let mut pts: Vec<(i64, f64)> = v["history"]
        .as_array()
        .map(|a| a.iter().filter_map(|p| Some((num(&p["t"])? as i64, num(&p["p"])?))).collect())
        .unwrap_or_default();
    pts.sort_by_key(|x| x.0);
    pts
}

/// `(bids, asks)` of a CLOB `/book?token_id=` response as `(price, size)`
/// pairs, in the venue's order. Levels with an unparseable price or size are
/// skipped.
pub fn parse_book(v: &Value) -> (Vec<(Decimal, Decimal)>, Vec<(Decimal, Decimal)>) {
    let levels = |key: &str| -> Vec<(Decimal, Decimal)> {
        v[key]
            .as_array()
            .map(|a| a.iter().filter_map(|l| Some((dec(&l["price"])?, dec(&l["size"])?))).collect())
            .unwrap_or_default()
    };
    (levels("bids"), levels("asks"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    /// The CLOB returns history in time order already, but nothing promises it,
    /// and the trainer indexes the series by time.
    #[test]
    fn price_history_points_come_back_sorted_by_time() {
        let v = json!({"history": [{"t": 30, "p": "0.52"}, {"t": 10, "p": 0.5}, {"t": 20, "p": 0.51}]});
        assert_eq!(parse_price_history(&v), vec![(10, 0.5), (20, 0.51), (30, 0.52)]);
        assert!(parse_price_history(&json!({})).is_empty());
    }

    /// A row with no price is not a print at zero; it is no print.
    #[test]
    fn trades_with_missing_fields_are_skipped_not_zeroed() {
        let v = json!([
            {"timestamp": 1790000000, "side": "BUY", "outcomeIndex": 0, "price": "0.57", "size": 12.5},
            {"timestamp": 1790000001, "side": "SELL", "outcomeIndex": 1, "size": 3},
            {"timestamp": "1790000002", "side": "SELL", "outcomeIndex": "1", "price": 0.44, "size": "7"}
        ]);
        let got = parse_trades(&v);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], RawPrint { ts: 1790000000, side: "BUY".into(), outcome_index: 0, price: 0.57, size: 12.5 });
        assert_eq!(got[1].outcome_index, 1);
        assert_eq!(got[1].price, 0.44);
    }

    /// 0.57 is 0.57, not the sixteen digits a binary double carries.
    #[test]
    fn f64_prices_become_the_decimal_the_venue_sent() {
        assert_eq!(dec_from_f64(0.57).unwrap().to_string(), "0.57");
        assert_eq!(dec_from_f64(12.5).unwrap().to_string(), "12.5");
        assert_eq!(dec_from_f64(0.1 + 0.2).unwrap().to_string(), "0.3");
    }

    #[test]
    fn book_levels_parse_prices_and_sizes_as_strings_or_numbers() {
        let v = json!({"bids": [{"price": "0.55", "size": "100"}, {"price": "bad", "size": "1"}], "asks": [{"price": 0.57, "size": 40}]});
        let (bids, asks) = parse_book(&v);
        assert_eq!(bids, vec![(dec!(0.55), dec!(100))]);
        assert_eq!(asks, vec![(dec!(0.57), dec!(40))]);
    }
}
