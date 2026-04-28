use rust_decimal::Decimal;
use chrono::{DateTime, Utc, TimeZone, Datelike, Timelike};
use chrono_tz::US::Eastern;
use regex::Regex;
use std::str::FromStr as _;
use tracing::debug;

/// Fetch strike price from Binance using market close time as reference
pub async fn fetch_strike_price_from_close_time(
    http: &reqwest::Client,
    filter: &str,
    close_time: Option<DateTime<Utc>>,
) -> Option<Decimal> {
    let close_time = close_time?;
    let now = Utc::now();

    // If close_time is in the future, the market hasn't closed yet.
    // Use close_time - 1 hour as the reference (approximates the market start/reference time
    // for standard hourly markets like "Bitcoin Up or Down - April 7, 9AM ET").
    // If even that is in the future (very short window), fall back to the latest completed minute.
    let reference_time = if close_time > now {
        let one_hour_before = close_time - chrono::Duration::hours(1);
        if one_hour_before < now - chrono::Duration::minutes(1) {
            one_hour_before
        } else {
            // Market window is shorter than 1 hour; use the latest available candle
            now - chrono::Duration::minutes(1)
        }
    } else {
        close_time
    };

    let utc_millis = reference_time.timestamp_millis();

    let binance_symbol = match filter {
        "eth" => "ETHUSDT",
        "sol" => "SOLUSDT",
        _ => "BTCUSDT",
    };

    let url = format!("https://api.binance.com/api/v3/klines?symbol={}&interval=1m&startTime={}&limit=1", binance_symbol, utc_millis);

    if let Ok(resp) = http.get(&url).send().await {
        if let Ok(json) = resp.json::<serde_json::Value>().await {
            if let Some(candle) = json.as_array().and_then(|a| a.first()) {
                if let Some(close_str) = candle.as_array().and_then(|a| a.get(4)).and_then(|v| v.as_str()) {
                    if let Ok(price) = Decimal::from_str(close_str) {
                        debug!("✅ Fetched strike price from Binance at market close time: ${}", price);
                        return Some(price);
                    }
                }
            }
        }
    }
    None
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
    let utc_millis = et_time.with_timezone(&Utc).timestamp_millis();

    let binance_symbol = match filter {
        "eth" => "ETHUSDT",
        "sol" => "SOLUSDT",
        _ => "BTCUSDT",
    };

    let url = format!("https://api.binance.com/api/v3/klines?symbol={}&interval=1m&startTime={}&limit=1", binance_symbol, utc_millis);

    if let Ok(resp) = http.get(&url).send().await {
        if let Ok(json) = resp.json::<serde_json::Value>().await {
            if let Some(candle) = json.as_array().and_then(|a| a.first()) {
                if let Some(close_str) = candle.as_array().and_then(|a| a.get(4)).and_then(|v| v.as_str()) {
                    return Decimal::from_str(close_str).ok();
                }
            }
        }
    }
    None
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


