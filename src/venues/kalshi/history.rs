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

//! Kalshi's public history of its hourly crypto markets, for GBoost plan B's
//! training store.
//!
//! The recorder (`vipers::gboost_planb_record`) was written on the premise that
//! Kalshi publishes no price history, so an instance could only train on the
//! hours it had flown itself. That premise was wrong: the public API serves,
//! without a key, every settled market with its `result`, and per-market
//! one-minute candlesticks carrying the YES bid and ask at each minute's close
//! (`GET /series/{series}/markets/{ticker}/candlesticks?period_interval=1`).
//! Checked 2026-11-25 on KXBTCD-26NOV2011-T59299.99: 54 candles over the hour,
//! 43 of them two-sided; settled hourly markets listed back to 2026-09-25, 24
//! close hours a day.
//!
//! One hour has a ladder of strikes (188 on 2026-11-20 16:00Z). The squadron
//! records the market it flies, the at-the-money strike ([B114]), so the history
//! takes the hour's most-traded strike as the market the squadron would have
//! held. Daily markets share the series and are told apart by their span: an
//! hourly market opens exactly an hour before it closes.
//!
//! Everything venue-specific stays here; the trainer sees
//! [`crate::venues::core::HourlyHistory`].

use crate::venues::core::{HistoryFetch, HistoryQuote, HourlyHistory};

/// Pause after every public call, and the first backoff after a 429 or 5xx.
const REQUEST_PAUSE_MS: u64 = 300;
const BACKOFF_BASE_SECS: u64 = 2;
const HTTP_ATTEMPTS: u32 = 5;
const HTTP_TIMEOUT_SECS: u64 = 30;
/// A market listed without a result this long after its close is given up as
/// unsettled (an incident day's settlements can run hours late, so this is
/// generous); the hour is then recorded as having no market.
const UNSETTLED_GIVE_UP_SECS: i64 = 48 * 3600;
const USER_AGENT: &str = "Mozilla/5.0 (compatible; dradis-engine/1.0)";

/// The hourly series for an underlying, where Kalshi lists one.
pub fn hourly_series(underlying: &str) -> Option<&'static str> {
    match underlying.to_ascii_lowercase().as_str() {
        "btc" => Some("KXBTCD"),
        "eth" => Some("KXETHD"),
        "sol" => Some("KXSOLD"),
        _ => None,
    }
}

fn client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(HTTP_TIMEOUT_SECS))
            .user_agent(USER_AGENT)
            .build()
            .unwrap_or_default()
    })
}

/// GET with pacing and exponential backoff on 429 and 5xx. No credentials: these
/// are public routes, and the authenticated client's rate bucket is left alone.
pub(super) async fn get_json(url: &str) -> Result<serde_json::Value, String> {
    let mut last = String::new();
    for attempt in 0..HTTP_ATTEMPTS {
        match client().get(url).header("Accept", "application/json").send().await {
            Ok(r) if r.status().is_success() => {
                tokio::time::sleep(std::time::Duration::from_millis(REQUEST_PAUSE_MS)).await;
                return r.json::<serde_json::Value>().await.map_err(|e| e.to_string());
            }
            Ok(r) if r.status().as_u16() == 429 || r.status().is_server_error() => last = format!("HTTP {}", r.status()),
            Ok(r) => return Err(format!("HTTP {}", r.status())),
            Err(e) => last = e.to_string(),
        }
        tokio::time::sleep(std::time::Duration::from_secs(BACKOFF_BASE_SECS << attempt)).await;
    }
    Err(format!("{url}: {last}"))
}

fn num(v: &serde_json::Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
}

fn ts(v: &serde_json::Value) -> Option<i64> {
    v.as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|d| d.timestamp())
}

/// The markets of `series` closing at `close_s`, whatever their status, every
/// page. A page without a `markets` array is an error (a maintenance body, a
/// schema change), never an empty ladder: an empty ladder is written down as an
/// hour with no market and is not asked for again.
async fn markets_closing_at(base: &str, series: &str, close_s: i64) -> Result<Vec<serde_json::Value>, String> {
    let mut out = Vec::new();
    let mut cursor = String::new();
    loop {
        let url = format!(
            "{base}/markets?series_ticker={series}&limit=1000&min_close_ts={}&max_close_ts={}{}",
            close_s - 60, close_s + 60,
            if cursor.is_empty() { String::new() } else { format!("&cursor={cursor}") },
        );
        let page = get_json(&url).await?;
        let ms = page["markets"].as_array().ok_or_else(|| format!("{url}: no markets array in the response"))?;
        out.extend(ms.iter().cloned());
        cursor = page["cursor"].as_str().unwrap_or("").to_string();
        if cursor.is_empty() { break; }
    }
    Ok(out)
}

/// The hour's market, chosen from its ladder of strikes: an hourly span (open an
/// hour before `close_s`) closing at `close_s`; among those the strike nearest
/// `spot_at_open` when the caller knows the price at the open (the at-the-money
/// market the squadron flies, [B114]), otherwise the most traded. `None` when
/// the ladder has no hourly market.
pub fn pick_hourly_market(markets: &[serde_json::Value], close_s: i64, spot_at_open: Option<f64>) -> Option<&serde_json::Value> {
    let hourly = markets.iter()
        .filter(|m| ts(&m["close_time"]) == Some(close_s) && ts(&m["open_time"]) == Some(close_s - 3600));
    let strike = |m: &serde_json::Value| num(&m["floor_strike"]).or_else(|| num(&m["cap_strike"]));
    if let Some(spot) = spot_at_open {
        let nearest = hourly.clone()
            .filter(|m| strike(m).is_some())
            .min_by(|a, b| (strike(a).unwrap() - spot).abs().total_cmp(&(strike(b).unwrap() - spot).abs()));
        if nearest.is_some() { return nearest; }
    }
    hourly.max_by(|a, b| {
        let va = num(&a["volume_fp"]).or_else(|| num(&a["volume"])).unwrap_or(0.0);
        let vb = num(&b["volume_fp"]).or_else(|| num(&b["volume"])).unwrap_or(0.0);
        va.total_cmp(&vb)
    })
}

/// The market's settlement as the YES leg's answer: `Some(true)` when YES won,
/// `None` while the venue has published no result.
pub fn up_won_of(market: &serde_json::Value) -> Option<bool> {
    match market["result"].as_str() {
        Some("yes") => Some(true),
        Some("no") => Some(false),
        _ => None,
    }
}

/// One-minute candlesticks as quotes: the minute's closing YES bid and ask where
/// both sides were quoted. A candle with no bid, or an ask at $1, is a one-sided
/// book and contributes nothing, as the live recorder skips such a minute.
pub fn quotes_of(candles: &[serde_json::Value]) -> Vec<HistoryQuote> {
    let mut out: Vec<HistoryQuote> = candles.iter().filter_map(|c| {
        let t = c["end_period_ts"].as_i64()?;
        let bid = num(&c["yes_bid"]["close_dollars"])?;
        let ask = num(&c["yes_ask"]["close_dollars"])?;
        (bid > 0.0 && ask < 1.0 && ask > bid - 1e-9).then_some(HistoryQuote { t, yes_bid: bid, yes_ask: ask })
    }).collect();
    out.sort_by_key(|q| q.t);
    out.dedup_by_key(|q| q.t);
    out
}

/// Assemble the history record from the venue's market object and its candles.
pub fn history_of(market: &serde_json::Value, candles: &[serde_json::Value]) -> Option<HourlyHistory> {
    let ticker = market["ticker"].as_str()?.to_string();
    Some(HourlyHistory {
        yes_token: super::leg_id(&ticker, true),
        no_token: super::leg_id(&ticker, false),
        strike: num(&market["floor_strike"]).or_else(|| num(&market["cap_strike"])),
        up_won: up_won_of(market),
        quotes: quotes_of(candles),
        ticker,
    })
}

/// One market's one-minute candlesticks over the hour closing at `close_s`. A
/// response without a `candlesticks` array is an error, retried next pass, not
/// an hour with no quotes.
async fn candles_of(base: &str, series: &str, ticker: &str, close_s: i64) -> Result<Vec<serde_json::Value>, String> {
    let url = format!(
        "{base}/series/{series}/markets/{ticker}/candlesticks?start_ts={}&end_ts={close_s}&period_interval=1",
        close_s - 3600,
    );
    let page = get_json(&url).await?;
    page["candlesticks"].as_array().cloned().ok_or_else(|| format!("{url}: no candlesticks array in the response"))
}

/// The hour starting at `window_start` for `underlying`, from the public API.
pub async fn fetch_hourly_history(underlying: &str, window_start: i64, now: i64, spot_at_open: Option<f64>) -> Result<HistoryFetch, String> {
    let Some(series) = hourly_series(underlying) else { return Ok(HistoryFetch::NoMarket) };
    let base = super::base_url();
    let close_s = window_start + 3600;
    let markets = markets_closing_at(&base, series, close_s).await?;
    // Markets are listed before they open, so an hour with none in its ladder
    // after the close is an hour the venue never ran.
    let Some(m) = pick_hourly_market(&markets, close_s, spot_at_open) else { return Ok(HistoryFetch::NoMarket) };
    if up_won_of(m).is_none() {
        // Listed, closed, no result yet: asked again until the venue settles it,
        // or until it has clearly never going to.
        return Ok(if now < close_s + UNSETTLED_GIVE_UP_SECS { HistoryFetch::NotResolved } else { HistoryFetch::NoMarket });
    }
    let ticker = m["ticker"].as_str().ok_or("market without a ticker")?;
    let candles = candles_of(&base, series, ticker, close_s).await?;
    let h = history_of(m, &candles).ok_or("market without a ticker")?;
    Ok(HistoryFetch::Market(h))
}

/// The per-minute quotes of a known market over its hour, for a record written
/// live before the recorder kept asks.
pub async fn fetch_quotes(underlying: &str, ticker: &str, window_start: i64) -> Result<Vec<HistoryQuote>, String> {
    let Some(series) = hourly_series(underlying) else { return Ok(Vec::new()) };
    let candles = candles_of(&super::base_url(), series, ticker, window_start + 3600).await?;
    Ok(quotes_of(&candles))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn market(ticker: &str, open: &str, close: &str, vol: &str, result: &str, strike: f64) -> serde_json::Value {
        json!({"ticker": ticker, "open_time": open, "close_time": close, "volume_fp": vol, "result": result, "floor_strike": strike})
    }

    /// The hour's market is the most traded strike with an hourly span closing at
    /// the hour; a daily market in the same series is not it.
    #[test]
    fn the_most_traded_hourly_strike_is_the_hours_market() {
        let close = 1795190400; // 2026-11-20T16:00:00Z
        let ladder = vec![
            market("KXBTCD-26NOV2011-T59299.99", "2026-11-20T15:00:00Z", "2026-11-20T16:00:00Z", "107525.96", "yes", 59299.99),
            market("KXBTCD-26NOV2011-T59399.99", "2026-11-20T15:00:00Z", "2026-11-20T16:00:00Z", "215988.73", "no", 59399.99),
            market("KXBTCD-26NOV2011-T72799.99", "2026-11-20T15:00:00Z", "2026-11-20T16:00:00Z", "3234.25", "no", 72799.99),
            market("KXBTCD-26NOV2017-T60000.00", "2026-11-19T16:00:00Z", "2026-11-20T16:00:00Z", "999999", "no", 60000.0),
            market("KXBTCD-26NOV2012-T59399.99", "2026-11-20T16:00:00Z", "2026-11-20T17:00:00Z", "999999", "no", 59399.99),
        ];
        let m = pick_hourly_market(&ladder, close, None).unwrap();
        assert_eq!(m["ticker"], "KXBTCD-26NOV2011-T59399.99", "the daily and the next hour are not this hour");
        assert_eq!(up_won_of(m), Some(false));
        assert_eq!(up_won_of(&json!({"result": ""})), None, "no result yet");
        assert!(pick_hourly_market(&ladder, close + 7200, None).is_none());
        // With the price at the open known, the at-the-money strike is the market,
        // as the squadron would have flown it, whatever traded most.
        let m = pick_hourly_market(&ladder, close, Some(72_650.0)).unwrap();
        assert_eq!(m["ticker"], "KXBTCD-26NOV2011-T72799.99", "nearest strike to the open");
        let m = pick_hourly_market(&ladder, close, Some(59_310.0)).unwrap();
        assert_eq!(m["ticker"], "KXBTCD-26NOV2011-T59299.99");
    }

    /// Candles become quotes only where both sides are quoted, sorted and
    /// deduplicated by minute; the record carries the venue's leg ids and strike.
    #[test]
    fn candles_become_two_sided_quotes_in_the_records_shape() {
        let c = |end: i64, bid: &str, ask: &str| json!({"end_period_ts": end, "yes_bid": {"close_dollars": bid}, "yes_ask": {"close_dollars": ask}, "price": {}, "volume_fp": "0.00"});
        let candles = vec![
            c(1795186860, "0.0000", "0.0100"),   // no bid: one-sided
            c(1795186920, "0.9900", "1.0000"),   // ask at $1: one-sided
            c(1795187040, "0.9300", "0.9400"),
            c(1795186980, "0.9400", "0.9500"),
            c(1795187040, "0.9300", "0.9400"),   // the same minute twice
        ];
        let m = market("KXBTCD-26NOV2011-T59299.99", "2026-11-20T15:00:00Z", "2026-11-20T16:00:00Z", "1", "yes", 59299.99);
        let h = history_of(&m, &candles).unwrap();
        assert_eq!(h.quotes.iter().map(|q| q.t).collect::<Vec<_>>(), vec![1795186980, 1795187040]);
        assert_eq!((h.quotes[0].yes_bid, h.quotes[0].yes_ask), (0.94, 0.95));
        assert_eq!((h.yes_token.as_str(), h.no_token.as_str()), ("KXBTCD-26NOV2011-T59299.99#yes", "KXBTCD-26NOV2011-T59299.99#no"));
        assert_eq!((h.strike, h.up_won), (Some(59299.99), Some(true)));
        assert_eq!(hourly_series("ETH"), Some("KXETHD"));
        assert_eq!(hourly_series("doge"), None);
    }
}
