/// Market Validation Module
/// Provides comprehensive market filtering and validation to prevent
/// trading on expired, illiquid, or non-parseable markets.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use regex::Regex;
use std::str::FromStr;

/// Validation result for a market
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketValidationStatus {
    /// Market is valid and ready to trade
    Valid,
    /// Market has no token IDs (unpaired)
    NoTokenIds,
    /// Market lacks orderbook data
    NoOrderbook,
    /// Market has already expired
    Expired,
    /// Market expires within the safety buffer (too risky)
    ExpiringSoon,
    /// Market hasn't started yet
    NotStarted,
    /// Market is outside the acceptable time window
    OutsideTimeWindow,
    /// Market doesn't match the crypto filter
    WrongCrypto,
    /// Market has no valid strike price
    NoStrike,
    /// Market doesn't have sufficient liquidity
    InsufficientLiquidity,
    /// Market is blocked by keyword filter
    Blocked,
}

impl std::fmt::Display for MarketValidationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarketValidationStatus::Valid => write!(f, "Valid"),
            MarketValidationStatus::NoTokenIds => write!(f, "NoTokenIds"),
            MarketValidationStatus::NoOrderbook => write!(f, "NoOrderbook"),
            MarketValidationStatus::Expired => write!(f, "Expired"),
            MarketValidationStatus::ExpiringSoon => write!(f, "ExpiringSoon"),
            MarketValidationStatus::NotStarted => write!(f, "NotStarted"),
            MarketValidationStatus::OutsideTimeWindow => write!(f, "OutsideTimeWindow"),
            MarketValidationStatus::WrongCrypto => write!(f, "WrongCrypto"),
            MarketValidationStatus::NoStrike => write!(f, "NoStrike"),
            MarketValidationStatus::InsufficientLiquidity => write!(f, "InsufficientLiquidity"),
            MarketValidationStatus::Blocked => write!(f, "Blocked"),
        }
    }
}

/// Market validation context
#[derive(Debug, Clone)]
pub struct ValidationContext {
    pub now: DateTime<Utc>,
    pub crypto_filter: String,
    pub min_seconds_to_expiry: i64,
    pub max_seconds_to_expiry: i64,
    pub safety_buffer_secs: i64,
    pub min_volume: f64,
}

impl Default for ValidationContext {
    fn default() -> Self {
        Self {
            now: Utc::now(),
            crypto_filter: "btc".to_string(),
            min_seconds_to_expiry: 300,
            max_seconds_to_expiry: 14400,
            safety_buffer_secs: 180,
            min_volume: 0.0,
        }
    }
}

/// Attempts to extract a valid strike price from market name.
/// Searches for patterns like "$50000", "above $50000", "at 68,384.49", etc.
pub fn extract_strike_price(market_name: &str) -> Option<Decimal> {
    let lower_name = market_name.to_lowercase();

    // Pattern 1: Explicit price with currency or directional markers
    // Matches: "$50000", "above $68,384.49", "below $2,100", "at $100.50"
    // Match both: comma-separated numbers (like $68,384.49) OR any 3+ digit number with optional decimal
    let re1 = Regex::new(r"(?:\$|above\s|below\s|at\s)(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d{3,}(?:\.\d+)?)").unwrap();

    if let Some(cap) = re1.captures(&lower_name) {
        if let Some(num_str) = cap.get(1) {
            let cleaned = num_str.as_str().replace(",", "");
            if let Ok(price) = Decimal::from_str(&cleaned) {
                // Sanity check: reject prices that are clearly wrong
                // (BTC should be > 1000, ETH > 100, SOL > 1)
                if price > Decimal::from(100) {
                    return Some(price);
                }
            }
        }
    }

    // Pattern 2: Strike price in bracket notation
    // Matches: "[50000]", "[BTC 68384]", etc.
    let re2 = Regex::new(r"\[(?:BTC|ETH|SOL)?\s*(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d{3,}(?:\.\d+)?)\]").unwrap();
    if let Some(cap) = re2.captures(&lower_name) {
        if let Some(num_str) = cap.get(1) {
            let cleaned = num_str.as_str().replace(",", "");
            if let Ok(price) = Decimal::from_str(&cleaned) {
                if price > Decimal::from(100) {
                    return Some(price);
                }
            }
        }
    }

    // Pattern 3: "at X" where X is a number
    // Matches: "at 50000", "at 2100.50", "at 200.50"
    // Word boundary ensures we match the full number, not partial
    let re3 = Regex::new(r"\bat\s+(\d+(?:\.\d+)?)(?:\s|$)").unwrap();
    if let Some(cap) = re3.captures(&lower_name) {
        if let Some(num_str) = cap.get(1) {
            if let Ok(price) = Decimal::from_str(num_str.as_str()) {
                if price > Decimal::from(100) {
                    return Some(price);
                }
            }
        }
    }

    None
}

/// Checks if a market has an explicit strike price or is a valid binary market.
/// Binary markets (Up/Down with no strike) are implicitly valid.
pub fn has_valid_strike_or_binary(market_name: &str) -> bool {
    // Check for explicit strike price
    if extract_strike_price(market_name).is_some() {
        return true;
    }

    // Check if it's a binary "Up or Down" market (implicitly valid)
    let lower = market_name.to_lowercase();
    if lower.contains("up or down") {
        return true;
    }

    false
}

/// Validates market expiry status.
/// Returns the validation status and seconds remaining.
pub fn validate_expiry(
    close_time: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    min_seconds_to_expiry: i64,
    max_seconds_to_expiry: i64,
    safety_buffer_secs: i64,
) -> (MarketValidationStatus, i64) {
    match close_time {
        None => (MarketValidationStatus::Valid, 0),
        Some(ct) => {
            let seconds_left = (ct - now).num_seconds();

            if seconds_left < 0 {
                (MarketValidationStatus::Expired, 0)
            } else if seconds_left < safety_buffer_secs {
                (MarketValidationStatus::ExpiringSoon, seconds_left)
            } else if seconds_left < min_seconds_to_expiry {
                (MarketValidationStatus::OutsideTimeWindow, seconds_left)
            } else if seconds_left > max_seconds_to_expiry {
                (MarketValidationStatus::OutsideTimeWindow, seconds_left)
            } else {
                (MarketValidationStatus::Valid, seconds_left)
            }
        }
    }
}

/// Validates market's time window based on name (hourly, specific time ranges, etc.)
pub fn validate_time_window(market_name: &str) -> bool {
    let lower = market_name.to_lowercase();

    // Check for time markers (ET timezone)
    let has_et = lower.contains(" et");
    let has_time_marker = lower.contains(":") && (lower.contains("am") || lower.contains("pm"));

    if has_et && has_time_marker {
        // Reject midnight windows (too risky)
        if lower.contains("12:00pm") || lower.contains("am-12:") || lower.contains("pm-12:") {
            return false;
        }
        return true;
    }

    // Also allow simple "hour" or general "et" references
    lower.contains("hour") || lower.contains("et")
}

/// Checks if market has required token pairs
pub fn validate_token_ids(token_ids: &[impl core::fmt::Debug], _market_name: &str) -> bool {
    if token_ids.len() < 2 {
        return false;
    }
    true
}

/// Checks crypto filter match
pub fn validate_crypto_filter(market_name: &str, crypto_filter: &str) -> bool {
    let lower = market_name.to_lowercase();
    match crypto_filter {
        "btc" | "bitcoin" => lower.contains("bitcoin") || lower.contains("btc"),
        "eth" | "ethereum" => lower.contains("ethereum") || lower.contains("eth"),
        "sol" | "solana" => lower.contains("solana") || lower.contains("sol"),
        "all" => true,
        _ => true,
    }
}

/// Validates market volume is sufficient
pub fn validate_volume(volume: f64, min_volume: f64) -> bool {
    volume >= min_volume
}

/// Comprehensive market validation function
/// Returns (is_valid, status, message)
pub fn validate_market(
    market_name: &str,
    event_title: &str,
    token_ids: &[impl core::fmt::Debug],
    close_time: Option<DateTime<Utc>>,
    volume: f64,
    ctx: &ValidationContext,
    blocked_keywords: &[&str],
) -> (bool, MarketValidationStatus, String) {
    // 1. Check if market is blocked by keyword
    let combined_text = format!("{} {}", market_name, event_title).to_lowercase();
    for keyword in blocked_keywords {
        if combined_text.contains(&keyword.to_lowercase()) {
            return (
                false,
                MarketValidationStatus::Blocked,
                format!("Blocked keyword: '{}'", keyword),
            );
        }
    }

    // 2. Check crypto filter match
    if !validate_crypto_filter(market_name, &ctx.crypto_filter) {
        return (
            false,
            MarketValidationStatus::WrongCrypto,
            format!(
                "Does not match crypto filter: {}",
                ctx.crypto_filter
            ),
        );
    }

    // 3. Check token IDs
    if !validate_token_ids(token_ids, market_name) {
        return (
            false,
            MarketValidationStatus::NoTokenIds,
            "Missing token pair (need YES + NO)".to_string(),
        );
    }

    // 4. Check time window
    if !validate_time_window(market_name) {
        return (
            false,
            MarketValidationStatus::OutsideTimeWindow,
            "Market name does not indicate short-term window".to_string(),
        );
    }

    // 5. Check expiry window
    let (expiry_status, seconds_left) = validate_expiry(
        close_time,
        ctx.now,
        ctx.min_seconds_to_expiry,
        ctx.max_seconds_to_expiry,
        ctx.safety_buffer_secs,
    );
    if expiry_status != MarketValidationStatus::Valid {
        let msg = match expiry_status {
            MarketValidationStatus::Expired => "Market has expired".to_string(),
            MarketValidationStatus::ExpiringSoon => {
                format!(
                    "Market expires too soon ({} seconds left)",
                    seconds_left
                )
            }
            MarketValidationStatus::OutsideTimeWindow => {
                format!(
                    "Market outside time window ({} seconds to expiry)",
                    seconds_left
                )
            }
            _ => "Time window validation failed".to_string(),
        };
        return (false, expiry_status, msg);
    }

    // 6. Check volume (if required)
    if !validate_volume(volume, ctx.min_volume) {
        return (
            false,
            MarketValidationStatus::InsufficientLiquidity,
            format!(
                "Volume {} below minimum {}",
                volume, ctx.min_volume
            ),
        );
    }

    // 7. Check strike price (binary markets are OK without explicit strike)
    if !has_valid_strike_or_binary(market_name) {
        return (
            false,
            MarketValidationStatus::NoStrike,
            "No valid strike price found in market name".to_string(),
        );
    }

    (
        true,
        MarketValidationStatus::Valid,
        "Market is valid".to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_strike_price() {
        assert_eq!(
            extract_strike_price("Bitcoin Up or Down - $68,384.49"),
            Some(Decimal::from_str("68384.49").unwrap())
        );
        assert_eq!(
            extract_strike_price("Ethereum above $2100"),
            Some(Decimal::from_str("2100").unwrap())
        );
        assert_eq!(
            extract_strike_price("Solana at 200.50"),
            Some(Decimal::from_str("200.50").unwrap())
        );
        assert_eq!(extract_strike_price("Bitcoin Up or Down"), None);
    }

    #[test]
    fn test_has_valid_strike_or_binary() {
        assert!(has_valid_strike_or_binary("Bitcoin Up or Down"));
        assert!(has_valid_strike_or_binary("Bitcoin at $68,384"));
        assert!(!has_valid_strike_or_binary("Bitcoin will Moon"));
    }

    #[test]
    fn test_validate_crypto_filter() {
        assert!(validate_crypto_filter("Bitcoin Up or Down", "btc"));
        assert!(validate_crypto_filter("Ethereum Up or Down", "eth"));
        assert!(!validate_crypto_filter("Bitcoin Up or Down", "eth"));
        assert!(validate_crypto_filter("Bitcoin Up or Down", "all"));
    }

    #[test]
    fn test_validate_time_window() {
        assert!(validate_time_window("Bitcoin Up or Down - April 7, 5PM ET"));
        assert!(validate_time_window("Ethereum Up or Down - hourly"));
        assert!(!validate_time_window("Bitcoin winner - 2026"));
    }
}



