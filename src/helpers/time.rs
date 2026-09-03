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

use rust_decimal::Decimal;
use chrono::{DateTime, Utc, TimeZone, Datelike, Timelike};
use chrono_tz::US::Eastern;
use regex::Regex;
use std::str::FromStr as _;
use tracing::debug;

/// Extract a strike price from a market's question text (e.g. "$115,000" /
/// "above 115000" / "[BTC 115,000]"). Venue-neutral: shared by the intl
/// discovery pipeline and the US crypto wing. Requires the value to exceed 100
/// so share prices / percentages never false-match.
pub fn extract_strike_price(market_name: &str) -> Option<Decimal> {
    let lower_name = market_name.to_lowercase();
    let re1 = Regex::new(r"(?:\$|above\s|below\s|at\s)(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d{3,}(?:\.\d+)?)").unwrap();
    if let Some(cap) = re1.captures(&lower_name) {
        if let Some(num_str) = cap.get(1) {
            let cleaned = num_str.as_str().replace(",", "");
            if let Ok(price) = Decimal::from_str(&cleaned) {
                if price > Decimal::from(100) { return Some(price); }
            }
        }
    }
    let re2 = Regex::new(r"\[(?:BTC|ETH|SOL)?\s*(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d{3,}(?:\.\d+)?)\]").unwrap();
    if let Some(cap) = re2.captures(&lower_name) {
        if let Some(num_str) = cap.get(1) {
            let cleaned = num_str.as_str().replace(",", "");
            if let Ok(price) = Decimal::from_str(&cleaned) {
                if price > Decimal::from(100) { return Some(price); }
            }
        }
    }
    let re3 = Regex::new(r"\bat\s+(\d+(?:\.\d+)?)(?:\s|$)").unwrap();
    if let Some(cap) = re3.captures(&lower_name) {
        if let Some(num_str) = cap.get(1) {
            if let Ok(price) = Decimal::from_str(num_str.as_str()) {
                if price > Decimal::from(100) { return Some(price); }
            }
        }
    }
    None
}

/// The Binance 1m kline whose OPEN is a market's reference price, for a market
/// whose reference is the start of a fixed window ending at `close_time`.
///
/// `None` while the window has not opened: the reference price does not exist
/// yet, and nothing should stand in for it. Until 2026-09-03 this fell back to
/// "the latest completed minute" for a not-yet-open window, so a squadron that
/// rotated onto the next hour's "Up or Down" market minutes before the hour
/// carried the price at rotation time as that market's strike for the whole
/// hour. Three real-money FairValue losses on 2026-09-02/03 came from that
/// single defect: on the 6AM market the model's strike sat ~$280 (1.2 sigma)
/// above the actual window open, so it priced a coin flip at 0.87 and bought
/// the side that settled at zero.
pub fn hourly_window_reference_time(
    close_time: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let window_start = close_time - chrono::Duration::hours(1);
    (window_start <= now).then_some(window_start)
}

/// The OPEN of a Binance kline row (`[open_time, open, high, low, close, …]`).
///
/// Open, not close. Polymarket's "Up or Down" markets resolve on the open and
/// close of the 1H (or 1D) Binance candle that begins at the named time, and a
/// candle's open is its first trade, so the 1m candle that starts at the same
/// instant opens at exactly the same price. Its CLOSE is one minute of drift
/// later, and that minute was silently folded into every strike this read.
pub fn kline_open_price(kline: &serde_json::Value) -> Option<Decimal> {
    let row = kline.as_array()?;
    let open = row.get(1)?.as_str()?;
    Decimal::from_str(open).ok()
}

async fn fetch_kline_open(
    http: &reqwest::Client,
    filter: &str,
    at: DateTime<Utc>,
) -> Option<Decimal> {
    let binance_symbol = match filter {
        "eth" => "ETHUSDT",
        "sol" => "SOLUSDT",
        _ => "BTCUSDT",
    };
    let url = format!(
        "https://api.binance.com/api/v3/klines?symbol={}&interval=1m&startTime={}&limit=1",
        binance_symbol, at.timestamp_millis(),
    );
    let resp = http.get(&url).send().await.ok()?;
    let json = resp.json::<serde_json::Value>().await.ok()?;
    let candle = json.as_array().and_then(|a| a.first())?;
    // Binance answers a request for a minute that has not started with an
    // EMPTY array, so a future `at` falls out here as None rather than as some
    // other candle. The callers guard the time anyway; this is the backstop.
    kline_open_price(candle)
}

/// Strike for a fixed one-hour window market from its close time: the open of
/// the Binance 1m candle at the window start. `None` before the window opens.
pub async fn fetch_strike_price_from_close_time(
    http: &reqwest::Client,
    filter: &str,
    close_time: Option<DateTime<Utc>>,
) -> Option<Decimal> {
    let close_time = close_time?;
    let Some(reference_time) = hourly_window_reference_time(close_time, Utc::now()) else {
        debug!(
            "Window closing {} has not opened yet — no strike exists for it",
            close_time.with_timezone(&Eastern).format("%H:%M ET"),
        );
        return None;
    };
    let price = fetch_kline_open(http, filter, reference_time).await?;
    debug!("✅ Fetched strike price from Binance at window open: ${}", price);
    Some(price)
}

/// Fetch historical strike price by parsing market description for date/time
pub async fn fetch_historical_strike_price(
    http: &reqwest::Client,
    filter: &str,
    text_to_scan: &str,
) -> Option<Decimal> {
    let lower_text = text_to_scan.to_lowercase();

    let re1 = Regex::new(r"([a-z]{3})\s+(\d{1,2})\s+'(\d{2})\s+(\d{1,2}):(\d{2})").unwrap();
    let re2 = Regex::new(r"([a-z]+)\s+(\d{1,2}),\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)").unwrap();

    let (year, month, day, hour, min) = if let Some(cap) = re1.captures(&lower_text) {
        let month_str = cap.get(1).map(|m| m.as_str())?;
        let day: u32 = cap.get(2).map(|m| m.as_str().parse().ok()).flatten()?;
        let year: i32 = 2000 + cap.get(3).map(|m| m.as_str().parse::<i32>().ok()).flatten()?;
        let hour: u32 = cap.get(4).map(|m| m.as_str().parse().ok()).flatten()?;
        let min: u32 = cap.get(5).map(|m| m.as_str().parse().ok()).flatten()?;

        let month = match month_str {
            "jan" => 1, "feb" => 2, "mar" => 3, "apr" => 4, "may" => 5, "jun" => 6,
            "jul" => 7, "aug" => 8, "sep" => 9, "oct" => 10, "nov" => 11, "dec" => 12,
            _ => return None,
        };
        (year, month, day, hour, min)
    } else if let Some(cap) = re2.captures(&lower_text) {
        let month_str = cap.get(1).map(|m| m.as_str())?;
        let day: u32 = cap.get(2).map(|m| m.as_str().parse().ok()).flatten()?;
        let mut hour: u32 = cap.get(3).map(|m| m.as_str().parse().ok()).flatten()?;
        let min: u32 = cap.get(4).map(|m| m.as_str().parse().unwrap_or(0)).unwrap_or(0);
        let ampm = cap.get(5).map(|m| m.as_str())?;

        if ampm == "pm" && hour < 12 { hour += 12; }
        if ampm == "am" && hour == 12 { hour = 0; }

        let month = match &month_str[..3] {
            "jan" => 1, "feb" => 2, "mar" => 3, "apr" => 4, "may" => 5, "jun" => 6,
            "jul" => 7, "aug" => 8, "sep" => 9, "oct" => 10, "nov" => 11, "dec" => 12,
            _ => return None,
        };
        let year = Utc::now().year();
        (year, month, day, hour, min)
    } else {
        return None;
    };

    let et_time = match Eastern.with_ymd_and_hms(year, month, day, hour, min, 0).single() {
        Some(t) => t,
        None => return None,
    };
    let reference_time = et_time.with_timezone(&Utc);
    if reference_time > Utc::now() {
        // The named candle has not opened. There is no reference price yet,
        // and the caller must not be handed a stand-in for one.
        debug!(
            "Reference time {} is in the future — no strike exists for it yet",
            et_time.format("%b %-d %H:%M ET"),
        );
        return None;
    }
    fetch_kline_open(http, filter, reference_time).await
}

/// Generate candidate market names for hourly crypto events
/// Returns possible name patterns to search for
pub fn generate_hourly_market_names(crypto_filter: &str, current_time_utc: DateTime<Utc>) -> Vec<String> {
    let mut names = Vec::new();
    let eastern_time = current_time_utc.with_timezone(&Eastern);

    let crypto_name_long = match crypto_filter {
        "btc" => "Bitcoin",
        "eth" => "Ethereum",
        "sol" => "Solana",
        _ => "Crypto",
    };
    let crypto_name_short = crypto_filter.to_uppercase();

    // Generate names for current hour and next hour
    for i in 0..=1 {
        let target_time = eastern_time.clone() + chrono::Duration::hours(i);
        let hour = target_time.hour();
        let ampm = if hour >= 12 { "PM" } else { "AM" };
        let display_hour = if hour == 0 { 12 } else if hour > 12 { hour - 12 } else { hour };
        let next_hour = if display_hour == 12 { 1 } else { display_hour + 1 };

        let month_name = target_time.format("%B").to_string();
        let day = target_time.day();

        // Standard: "Bitcoin Up or Down - April 3, 5PM ET"
        names.push(format!("{} Up or Down - {} {}, {}{} ET", crypto_name_long, month_name, day, display_hour, ampm));
        // Range: "Bitcoin Up or Down - April 3, 5-6PM ET"
        names.push(format!("{} Up or Down - {} {}, {}-{}{} ET", crypto_name_long, month_name, day, display_hour, next_hour, ampm));
        // Short name versions
        names.push(format!("{} Up or Down - {} {}, {}{} ET", crypto_name_short, month_name, day, display_hour, ampm));
        names.push(format!("{} Up or Down - {} {}, {}-{}{} ET", crypto_name_short, month_name, day, display_hour, next_hour, ampm));
    }
    names
}

/// Generate Polymarket event slugs for the daily "Up or Down on [date]?" event.
///
/// Polymarket's slug format is: `{crypto}-up-or-down-on-{month}-{day}-{year}`
/// e.g. `bitcoin-up-or-down-on-april-29-2026`
///
/// Generates today and tomorrow (ET) so overnight sessions crossing midnight still find the market.
pub fn generate_daily_event_slugs(crypto_filter: &str, current_time_utc: DateTime<Utc>) -> Vec<String> {
    let eastern_time = current_time_utc.with_timezone(&Eastern);

    let crypto_slug = match crypto_filter {
        "btc" => "bitcoin",
        "eth" => "ethereum",
        "sol" => "solana",
        _ => "bitcoin",
    };

    let mut slugs = Vec::new();
    for day_offset in 0..=1i64 {
        let target = eastern_time + chrono::Duration::days(day_offset);
        // month name in lowercase, no leading-zero day
        let month = target.format("%B").to_string().to_lowercase();
        let day = target.day();
        let year = target.year();
        slugs.push(format!("{}-up-or-down-on-{}-{}-{}", crypto_slug, month, day, year));
    }
    slugs
}

/// Generate candidate market names for daily "Up or Down on [date]?" markets.
/// These are the preferred window/daily venue for non-momentum strategies.
/// Checks today and tomorrow (in ET) to handle overnight sessions crossing midnight.
pub fn generate_daily_market_names(crypto_filter: &str, current_time_utc: DateTime<Utc>) -> Vec<String> {
    let mut names = Vec::new();
    let eastern_time = current_time_utc.with_timezone(&Eastern);

    let crypto_name_long = match crypto_filter {
        "btc" => "Bitcoin",
        "eth" => "Ethereum",
        "sol" => "Solana",
        _ => "Crypto",
    };
    let crypto_name_short = crypto_filter.to_uppercase();

    // Today and tomorrow in ET so overnight sessions always find the right market
    for day_offset in 0..=1i64 {
        let target = eastern_time + chrono::Duration::days(day_offset);
        let month_name = target.format("%B").to_string();
        let day = target.day();

        // Polymarket canonical pattern: "Bitcoin Up or Down on April 28?"
        names.push(format!("{} Up or Down on {} {}?", crypto_name_long, month_name, day));
        names.push(format!("{} Up or Down on {} {}?", crypto_name_short, month_name, day));
        // Without the question mark (some listings omit it)
        names.push(format!("{} Up or Down on {} {}", crypto_name_long, month_name, day));
        names.push(format!("{} Up or Down on {} {}", crypto_name_short, month_name, day));
    }
    names
}



#[cfg(test)]
mod strike_reference_tests {
    use super::*;
    use chrono::TimeZone;

    fn utc(s: &str) -> DateTime<Utc> {
        Utc.datetime_from_str(s, "%Y-%m-%dT%H:%M:%SZ").unwrap()
    }

    /// The 2026-09-03 6AM ET incident, replayed against the clock.
    ///
    /// The squadron rotated onto "Bitcoin Up or Down - September 3, 6AM ET"
    /// (window 10:00-11:00 UTC) well before 10:00 UTC. The old fallback answered
    /// "the latest completed minute" for a window that had not opened, and that
    /// price became the strike for the whole hour. Before the window opens there
    /// is no reference price, and the function has to say so.
    #[test]
    fn a_window_that_has_not_opened_has_no_reference_price() {
        let close = utc("2026-09-03T11:00:00Z");
        for now in ["2026-09-03T09:20:00Z", "2026-09-03T09:50:00Z", "2026-09-03T09:59:59Z"] {
            assert_eq!(
                hourly_window_reference_time(close, utc(now)),
                None,
                "at {now} the 10:00 window has not opened; no strike exists",
            );
        }
    }

    /// From the first second of the window onward the reference is the window
    /// open — including after the market has closed, when the same reference
    /// is what the settlement was judged against.
    #[test]
    fn an_open_window_references_its_own_start() {
        let close = utc("2026-09-03T11:00:00Z");
        let open = utc("2026-09-03T10:00:00Z");
        for now in ["2026-09-03T10:00:00Z", "2026-09-03T10:00:05Z", "2026-09-03T10:48:00Z", "2026-09-03T11:30:00Z"] {
            assert_eq!(hourly_window_reference_time(close, utc(now)), Some(open), "at {now}");
        }
    }

    /// Binance kline row: `[open_time, open, high, low, close, volume, ...]`.
    /// The strike is the OPEN. Reading index 4 (the close) folded a minute of
    /// drift into every strike; on a 1.4-sigma-per-minute BTC tape that is not
    /// noise.
    #[test]
    fn strike_is_the_candle_open_not_its_close() {
        let row = serde_json::json!([
            1788091200000_i64, "77600.02000000", "77640.00000000", "77571.00000000",
            "77630.10000000", "12.5", 1788091259999_i64, "0", 100, "0", "0", "0"
        ]);
        assert_eq!(kline_open_price(&row), Some(Decimal::from_str("77600.02").unwrap()));
        // An empty answer (Binance's reply for a minute that has not started)
        // must not be mistaken for a price.
        assert_eq!(kline_open_price(&serde_json::json!([])), None);
        assert_eq!(kline_open_price(&serde_json::json!(null)), None);
    }
}
