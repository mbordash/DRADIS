use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::time::Duration as StdDuration;

// ============================================================================
// NOTIFICATION SETTINGS (Telegram)
// ============================================================================

pub const ENABLE_TELEGRAM: bool = true;


// ============================================================================
// ARBITRAGE STRATEGY PARAMETERS
// ============================================================================

/// Margin threshold to trigger entry.
/// Lowered to 0.040 (4 cents) temporarily to test execution speed and latency.
pub const ARBITRAGE_PROFIT_THRESHOLD: Decimal = dec!(0.040);

pub const MAX_SUM_PRICE_FOR_ENTRY: Decimal = dec!(0.975);

/// Minimum number of shares for a single order ( exchange requirement )
pub const MIN_ORDER_SHARES: Decimal = dec!(5.0);

/// Minimum USDC value for a single order ( exchange requirement )
pub const MIN_ORDER_USDC: Decimal = dec!(1.05);

/// Price offset added to the ask price when placing buy orders to ensure aggressive fills.
pub const BUY_PRICE_OFFSET: Decimal = dec!(0.02);

/// Maximum price for a single share when placing a buy order.
pub const MAX_BUY_LIMIT_PRICE: Decimal = dec!(0.99);

/// Price offset subtracted from the bid price when placing sell orders to ensure aggressive fills.
pub const SELL_PRICE_OFFSET: Decimal = dec!(0.01);
/// Minimum price for a single share when placing a sell order.
pub const MIN_SELL_LIMIT_PRICE: Decimal = dec!(0.01);

/// Combined bid threshold (YES_bid + NO_bid) to trigger an early exit from a hedged position.
pub const EARLY_EXIT_COMBINED_BID_THRESHOLD: Decimal = dec!(0.995);


// ============================================================================
// EMERGENCY CIRCUIT BREAKERS
// ============================================================================

/// Maximum number of consecutive failed trade attempts before the bot kills itself.
pub const MAX_CONSECUTIVE_FAILURES: u32 = 3;

/// Cooldown period after a failed trade attempt (seconds).
pub const FAILURE_COOLDOWN_SECS: i64 = 60;


// ============================================================================
// TRADING PARAMETERS - Price Thresholds and Position Sizing
// ============================================================================

/// Price scale conversion factor for on-chain operations (shares to base units)
pub const SHARE_SCALE: Decimal = dec!(1_000_000);


// ============================================================================
// POSITION DURATION AND TIMING (Seconds)
// ============================================================================

/// Cooldown period after a successful trade before entering a new one.
pub const TRADE_COOLDOWN_SECS: i64 = 8;

/// Cooldown period after a partial fill before attempting a new trade.
pub const PARTIAL_FILL_COOLDOWN_SECS: i64 = 30;

/// Minimum time until market expiry to still allow new position entry (seconds)
pub const MIN_SECONDS_TO_EXPIRY_FOR_ENTRY: i64 = 900;

/// Maximum time until market expiry to consider a market (seconds, 2 hours)
pub const MAX_SECONDS_TO_EXPIRY_FOR_ENTRY: i64 = 7200;

/// Final expiry window: stops all trading this close to market close (seconds, 10 minutes)
pub const FINAL_EXPIRY_WINDOW_SECS: i64 = 600;


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
pub fn main_ticker_interval() -> StdDuration {
    StdDuration::from_millis(100)
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

/// Minimum volume (USDC) required to trade a market.
/// Increased from 500 to 5000 to avoid illiquid "ghost town" markets.
pub const MIN_MARKET_VOLUME: f64 = 5000.0;

/// Maximum exposure allowed per individual token (in USDC)
pub const MAX_EXPOSURE_PER_TOKEN_USDC: Decimal = dec!(25);

/// Session drawdown limit: 1% of collateral with $5 minimum
pub fn max_session_drawdown(collateral: Decimal) -> Decimal {
    (collateral * dec!(0.01)).max(dec!(5.00))
}


// ============================================================================
// API ENDPOINTS AND URLS
// ============================================================================

pub const CLOB_API_BASE: &str = "https://clob.polymarket.com";


// ============================================================================
// MARKET FILTERING CRITERIA
// ============================================================================

/// Number of pages to scan for markets in the Gamma API.
pub const GAMMA_API_MARKET_SCAN_PAGES: usize = 50;

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

/// Ultra-short time window market detection (15-minute or specific time ranges)
pub fn is_ultra_short_window_market(name: &str) -> bool {
    let n = name.to_lowercase();

    if n.contains("15 minutes") || n.contains("15 minute") {
        return true;
    }

    let has_et = n.contains(" et");
    let has_range = n.contains('-');
    let has_time_marker = n.contains("am") || n.contains("pm");
    let has_minutes = n.contains(':');

    has_et && has_range && has_time_marker && has_minutes
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