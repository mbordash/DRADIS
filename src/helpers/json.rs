use alloy::primitives::U256;
use serde_json;
use std::str::FromStr as _;

/// Extract U256 value from serde_json::Value
/// Handles string, u64, i64, and numeric types
pub fn value_to_u256(v: &serde_json::Value) -> Option<U256> {
    if let Some(s) = v.as_str() {
        U256::from_str(s).ok()
    } else if let Some(n) = v.as_u64() {
        Some(U256::from(n))
    } else if let Some(n) = v.as_i64().filter(|&n| n >= 0) {
        Some(U256::from(n as u64))
    } else {
        None
    }
}

/// Check if a market has orderbook enabled
/// Handles both enableOrderBook and enable_order_book field names
pub fn get_enable_orderbook(market: &serde_json::Value) -> bool {
    market
        .get("enableOrderBook")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
        || market
            .get("enable_order_book")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
}

/// Extract close/end time from market or event object
/// Tries multiple field names for compatibility
pub fn extract_close_time(
    event: &serde_json::Value,
    market: &serde_json::Value,
) -> Option<chrono::DateTime<chrono::Utc>> {
    parse_dt(market.get("endDate"))
        .or_else(|| parse_dt(market.get("end_date")))
        .or_else(|| parse_dt(market.get("closeTime")))
        .or_else(|| parse_dt(market.get("close_time")))
        .or_else(|| parse_dt(event.get("endDate")))
        .or_else(|| parse_dt(event.get("end_date")))
        .or_else(|| parse_dt(event.get("closeTime")))
        .or_else(|| parse_dt(event.get("close_time")))
}

/// Extract start time from market or event object
/// Tries multiple field names for compatibility
pub fn extract_start_time(
    event: &serde_json::Value,
    market: &serde_json::Value,
) -> Option<chrono::DateTime<chrono::Utc>> {
    parse_dt(market.get("startDate"))
        .or_else(|| parse_dt(market.get("start_date")))
        .or_else(|| parse_dt(event.get("startDate")))
        .or_else(|| parse_dt(event.get("start_date")))
}

/// Extract token IDs from market object
/// Handles multiple formats: array, JSON string, CSV string, or single value
pub fn extract_token_ids_u256(market: &serde_json::Value) -> Vec<U256> {
    let v = market
        .get("clobTokenIds")
        .or_else(|| market.get("clob_token_ids"))
        .unwrap_or(&serde_json::Value::Null);

    let mut out = vec![];

    if let Some(arr) = v.as_array() {
        for item in arr {
            if let Some(t) = value_to_u256(item) {
                if t != U256::ZERO {
                    out.push(t);
                }
            }
        }
        if out.len() >= 2 {
            return out;
        }
    }

    if let Some(s) = v.as_str() {
        if let Ok(parsed) = serde_json::from_str::<Vec<serde_json::Value>>(s) {
            for item in parsed {
                if let Some(t) = value_to_u256(&item) {
                    if t != U256::ZERO {
                        out.push(t);
                    }
                }
            }
        } else if let Ok(parsed) = serde_json::from_str::<Vec<String>>(s) {
            for item_str in parsed {
                if let Ok(t) = U256::from_str(&item_str) {
                    if t != U256::ZERO {
                        out.push(t);
                    }
                }
            }
        }
    }

    if let Some(t) = value_to_u256(v) {
        if t != U256::ZERO {
            out.push(t);
        }
    }

    out
}

/// Parse RFC3339 datetime string
fn parse_dt(v: Option<&serde_json::Value>) -> Option<chrono::DateTime<chrono::Utc>> {
    let s = v.and_then(|x| x.as_str())?;
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

