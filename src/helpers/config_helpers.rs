use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::time::Duration as StdDuration;

// ============================================================================
// ORACLE-RELATIVE THRESHOLD HELPER
// ============================================================================

/// Scale a percentage-based threshold to an absolute dollar amount using the
/// current oracle price.
///
/// All per-asset dollar constants (previously expressed as separate `_BTC`, `_ETH`,
/// `_SOL` variants) are now stored as a single fraction of spot price.  This keeps
/// thresholds proportionally correct across assets and auto-adjusts when BTC/ETH/SOL
/// prices change significantly over time.
///
/// # Examples
/// ```
/// // BTC at $100,000, 0.2% threshold → $200
/// let thr = oracle_threshold(dec!(0.002), dec!(100_000));  // → 200
///
/// // ETH at $3,500, same 0.2% → $7
/// let thr = oracle_threshold(dec!(0.002), dec!(3_500));    // → 7
/// ```
#[inline]
pub fn oracle_threshold(pct: Decimal, oracle_price: Decimal) -> Decimal {
    pct * oracle_price
}

// ============================================================================
// INTERVAL TIMINGS (Duration for Market Scanning and Monitoring)
// ============================================================================

/// Market switch evaluation interval (checks for better markets to trade)
pub fn market_switch_interval() -> StdDuration {
    StdDuration::from_secs(90)
}

/// Periodic on-chain balance sync interval (syncs positions with blockchain)
pub fn periodic_sync_interval() -> StdDuration {
    StdDuration::from_secs(300)
}

/// Main ticker interval for trade execution checks (milliseconds)
/// Reduced from 100ms to 50ms for faster momentum signal response.
/// At 50ms, worst-case polling jitter is halved, and 2-tick confirmation
/// resolves in ~100ms instead of ~200ms.
pub fn main_ticker_interval() -> StdDuration {
    StdDuration::from_millis(50)
}


// ============================================================================
// MONITORING AND LOGGING INTERVALS
// ============================================================================

/// Status log interval (shows open positions, P&L summary)
pub fn status_log_interval() -> StdDuration {
    StdDuration::from_secs(60)
}


// ============================================================================
// WEBSOCKET AND CONNECTION TIMEOUTS
// ============================================================================

/// HTTP request timeout for API calls
pub fn http_timeout() -> StdDuration {
    StdDuration::from_secs(20)
}

/// TCP keepalive duration for connection persistence
pub fn tcp_keepalive() -> StdDuration {
    StdDuration::from_secs(30)
}

// ============================================================================
// RISK MANAGEMENT THRESHOLDS
// ============================================================================

/// Session drawdown limit: 4% of collateral with $10 minimum.
/// Raised from 1%/$5: the old limit locked out the bot after 1-2 bad trades on a
/// $100 account, preventing any recovery.  4%/$10 gives ~2 full stop-loss chains
/// before lockout while still protecting against runaway losses.
pub fn max_session_drawdown(collateral: Decimal) -> Decimal {
    (collateral * dec!(0.04)).max(dec!(10.00))
}


// ============================================================================
// MARKET FILTERING CRITERIA
// ============================================================================

/// Blocked market name keywords (politics, long-term events, etc.)
pub fn is_bad_market(name: &str) -> bool {
    let n = name.to_lowercase();
    n.contains("presidential") || n.contains("nomination") || n.contains("election") ||
        n.contains("democratic") || n.contains("republican") ||
        n.contains("masters") || n.contains("tournament") || n.contains("spieth") || n.contains("jordan") ||
        n.contains("5-minute") || n.contains("5 minute") || n.contains("5m")
}

/// Long-term 2026 markets (typically too illiquid for short-term trading)
pub fn is_long_term_2026(name: &str) -> bool {
    let n = name.to_lowercase();
    n.contains("2026") && (n.contains("win the") || n.contains("finals") || n.contains("cup") || n.contains("stanley"))
}

/// Detect price-range / price-band markets (e.g. "Will BTC be between $72,000 and $74,000 on April 12?")
/// These are NegRisk markets that are often already decided and unsuitable for directional strategies.
pub fn is_range_market(name: &str) -> bool {
    let n = name.to_lowercase();
    // "between $X and $Y" pattern
    (n.contains("between") && n.contains("and $")) ||
    // "price of X be between" pattern
    (n.contains("price of") && n.contains("between")) ||
    // "will ... be above/below $X" single-sided range
    (n.contains("will") && (n.contains("above $") || n.contains("below $")) && !n.contains("up or down"))
}

/// Hourly crypto markets (high-priority short-term session)
pub fn is_hourly_crypto_market(name: &str) -> bool {
    let n = name.to_lowercase();
    is_crypto_market(&n) && (
        n.contains("up or down") ||
            n.contains("hour") ||
            n.contains("et") ||
            n.contains("pm et") ||
            n.contains("am et")
    )
}

pub fn is_ultra_short_window_market(name: &str) -> bool {
    let n = name.to_lowercase();

    if n.contains("15 minutes") || n.contains("15 minute") {
        return true;
    }

    if let Some(window_mins) = parse_window_duration_minutes(name) {
        return window_mins <= 30;
    }

    false
}

/// High-priority market text patterns (very short time windows)
pub fn is_high_priority_text(s: &str) -> bool {
    let n = s.to_lowercase();
    n.contains("up or down") ||
        n.contains("5 minutes") ||
        n.contains("5m") ||
        n.contains("updown") ||
        n.contains("next 5") ||
        n.contains("next hour")
}

/// Crypto market detection by coin name
pub fn is_crypto_market(name: &str) -> bool {
    let n = name.to_lowercase();
    n.contains("btc") || n.contains("bitcoin") ||
        n.contains("eth") || n.contains("ethereum") ||
        n.contains("sol") || n.contains("solana")
}

/// Ultra-short time window market detection (15-minute windows only).
/// 4-hour windows (e.g. "4:00PM-8:00PM ET") are explicitly NOT ultra-short —
/// they are valid "window markets" suitable for the Maker strategy.
pub fn is_window_market(name: &str) -> bool {
    if let Some(mins) = parse_window_duration_minutes(name) {
        mins > 30  // at least a 1-hour window
    } else {
        false
    }
}

pub fn parse_window_duration_minutes(name: &str) -> Option<u32> {
    let n = name.to_lowercase();
    if !n.contains(" et") { return None; }

    let et_pos = n.find(" et")?;
    let segment = &n[..et_pos];

    let dash_pos = segment.rfind('-')?;
    let end_part = segment[dash_pos + 1..].trim();
    let start_part = {
        let before_dash = &segment[..dash_pos];
        let space_pos = before_dash.rfind(' ').unwrap_or(0);
        before_dash[space_pos..].trim()
    };

    fn parse_minutes(s: &str) -> Option<u32> {
        let s = s.trim();
        let is_pm = s.ends_with("pm");
        let is_am = s.ends_with("am");
        if !is_pm && !is_am { return None; }
        let time_part = &s[..s.len() - 2];
        let colon = time_part.find(':')?;
        let h: u32 = time_part[..colon].parse().ok()?;
        let m: u32 = time_part[colon + 1..].parse().ok()?;
        let mut total = h * 60 + m;
        if is_pm && h != 12 { total += 720; }
        if is_am && h == 12 { total = m; }
        Some(total)
    }

    let start_mins = parse_minutes(start_part)?;
    let end_mins = parse_minutes(end_part)?;
    let duration = if end_mins >= start_mins { end_mins - start_mins } else { end_mins + 1440 - start_mins };
    Some(duration)
}


/// Daily market: resolves on a specific calendar date (e.g. "Bitcoin Up or Down on April 21?").
/// These offer the longest price discovery window — ideal for Maker as a fallback
/// when no window market is available.
pub fn is_daily_market(name: &str) -> bool {
    let n = name.to_lowercase();
    // Pattern: "up or down on [Month] [Day]?" — no "ET" time component
    n.contains("up or down") && n.contains(" on ") && !n.contains(" et")
}

// ============================================================================
// SLEEP AND RETRY DURATIONS
// ============================================================================

/// Sleep duration before retrying failed API calls
pub fn retry_sleep_duration() -> StdDuration {
    StdDuration::from_secs(5)
}

/// Initial sleep on application startup (allows connectors to initialize)
pub fn startup_delay() -> StdDuration {
    StdDuration::from_secs(10)
}

/// Connector initialization delay (allows connection to establish)
pub fn connector_init_delay() -> StdDuration {
    StdDuration::from_secs(10)
}

/// Order execution delay (prevents rate limiting)
pub fn order_execution_delay() -> StdDuration {
    StdDuration::from_millis(300)
}
