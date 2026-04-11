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

/// If true, the bot will log trades but NOT actually execute them on-chain.
pub const GHOST_MODE: bool = false;

/// Margin threshold to trigger entry.
/// IMPORTANT: This should be higher than (2 * BUY_PRICE_OFFSET) to be profitable.
pub const ARBITRAGE_PROFIT_THRESHOLD: Decimal = dec!(0.05);

pub const MAX_SUM_PRICE_FOR_ENTRY: Decimal = dec!(0.98);

/// Minimum number of shares for a single order (exchange requirement)
pub const MIN_ORDER_SHARES: Decimal = dec!(5.0);

/// Minimum USDC value for a single order (exchange requirement)
pub const MIN_ORDER_USDC: Decimal = dec!(1.05);

/// Minimum ratio of target shares that MUST be available at the top of the book
/// before firing an order. Prevents massive partial fills on thin books.
pub const MIN_LIQUIDITY_FILL_RATIO: Decimal = dec!(0.80);

/// Price offset added to the ask price when placing buy orders to ensure aggressive fills.
pub const BUY_PRICE_OFFSET: Decimal = dec!(0.01);

/// Maximum price for a single share when placing a buy order.
pub const MAX_BUY_LIMIT_PRICE: Decimal = dec!(0.99);

/// Price offset subtracted from the bid price when placing sell orders to ensure aggressive fills.
pub const SELL_PRICE_OFFSET: Decimal = dec!(0.01);
/// Minimum price for a single share when placing a sell order.
pub const MIN_SELL_LIMIT_PRICE: Decimal = dec!(0.01);

/// Combined bid threshold (YES_bid + NO_bid) to trigger an early exit from a hedged position.
pub const EARLY_EXIT_COMBINED_BID_THRESHOLD: Decimal = dec!(0.995);


// ============================================================================
// MOMENTUM & ORACLE SETTINGS (Predictive Arbitrage)
// ============================================================================

/// Allow one-sided momentum trades (riskier, non-hedged entries based on Binance oracle)
pub const ENABLE_MOMENTUM_TRADING: bool = true;

/// Number of consecutive signal ticks required before firing a trade.
/// Set to 2 to filter out single-tick outliers and "fakeouts".
pub const MOMENTUM_CONFIRMATION_TICKS: u32 = 2;

/// Dynamic Take Profit: Exit if we gain this % over our entry price.
/// Lowered to 3% to capture quick moves in volatile markets.
pub const MOMENTUM_TARGET_PROFIT_PERCENT: Decimal = dec!(0.05);

/// Stop Loss: Exit if we lose this % from our entry price.
pub const MOMENTUM_STOP_LOSS_PERCENT: Decimal = dec!(0.10);

/// Reversal Exit: Exit if momentum velocity drops below this % of the entry threshold.
pub const MOMENTUM_REVERSAL_RATIO: Decimal = dec!(0.20);

/// Static Take Profit Ceiling: Exit if bid hits this price regardless of entry.
pub const MOMENTUM_TAKE_PROFIT_CEILING: Decimal = dec!(0.90);

/// Minimum distance from strike price (in USD) to trigger a momentum trade.
pub const BTC_STRIKE_BUFFER: Decimal = dec!(50.0);
pub const ETH_STRIKE_BUFFER: Decimal = dec!(5.0);
pub const SOL_STRIKE_BUFFER: Decimal = dec!(0.2);

/// Maximum token price allowed for a momentum entry.
/// Increased from 0.65 to 0.85 to allow entries when markets are in normal range (e.g., $0.51 to $0.49)
pub const MAX_MOMENTUM_ENTRY_PRICE: Decimal = dec!(0.85);

/// The time window (in seconds) used to calculate price velocity from Binance.
pub const MOMENTUM_WINDOW_SECS: u64 = 10;

/// Price change threshold (absolute USD) within the window to trigger a signal.
/// Increased BTC from 80 to 100 to filter out noise and catch more mature momentum moves.
pub const BTC_MOMENTUM_THRESHOLD: Decimal = dec!(100.0);
pub const ETH_MOMENTUM_THRESHOLD: Decimal = dec!(5.0);
pub const SOL_MOMENTUM_THRESHOLD: Decimal = dec!(0.5);


// ============================================================================
// TIME DECAY (THETA) STRATEGY PARAMETERS
// ============================================================================

/// Allow time decay trading (exploits YES+NO convergence to $1.00)
pub const ENABLE_TIME_DECAY_TRADING: bool = true;

/// Minimum net profit per share (after fees) for settlement-mode entry.
/// At 0 bps fees: requires combined_ask < $0.998.  At 100 bps: < ~$0.988.
pub const MIN_TIME_DECAY_NET_PROFIT: Decimal = dec!(0.002);

/// Maximum combined ask price for convergence-mode entry.
/// Allows entries slightly above $1.00 — profit comes from bid convergence near expiry.
pub const MAX_TIME_DECAY_COMBINED_ASK: Decimal = dec!(1.008);

/// Convergence mode only activates when market is within this many seconds of expiry.
/// Tighter window = higher convergence confidence.
pub const TIME_DECAY_CONVERGENCE_WINDOW_SECS: i64 = 1200;  // 20 minutes

/// Exit convergence-mode positions when combined bid reaches this level.
pub const TIME_DECAY_CONVERGENCE_EXIT_BID: Decimal = dec!(0.998);

/// Minimum seconds to expiry for time decay entry (must exceed MARKET_EXPIRY_SAFETY_BUFFER_SECS)
pub const TIME_DECAY_MIN_SECS_TO_EXPIRY: i64 = 240;  // 4 minutes

/// Maximum seconds to expiry for time decay entry
pub const TIME_DECAY_MAX_SECS_TO_EXPIRY: i64 = 1800;  // 30 minutes

/// Base position size for time decay (per side: YES and NO)
pub const TIME_DECAY_POSITION_SIZE_USDC: Decimal = dec!(10);

/// Auto-exit when profit reaches this percentage
pub const TIME_DECAY_TARGET_PROFIT_PERCENT: Decimal = dec!(0.015);  // 1.5% profit target

/// Maximum combined exposure in time decay positions
pub const TIME_DECAY_MAX_TOTAL_EXPOSURE_USDC: Decimal = dec!(50);

/// Maximum number of simultaneous time decay positions
pub const TIME_DECAY_MAX_POSITIONS: usize = 5;

/// Exit if spread widens more than this (lose this much)
pub const TIME_DECAY_STOP_LOSS_PERCENT: Decimal = dec!(0.01);  // Stop at 1% loss


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
/// Increased from 60 to 300 to ensure markets don't expire while trading executes
pub const MIN_SECONDS_TO_EXPIRY_FOR_ENTRY: i64 = 300;

/// Maximum time until market expiry to consider a market (seconds, 4 hours)
pub const MAX_SECONDS_TO_EXPIRY_FOR_ENTRY: i64 = 14400;

/// Final expiry window: stops all trading this close to market close (seconds, 10 minutes)
pub const FINAL_EXPIRY_WINDOW_SECS: i64 = 600;

/// Safety buffer to re-check market expiry before each trade (seconds)
/// Prevents selecting markets that expire during order execution
pub const MARKET_EXPIRY_SAFETY_BUFFER_SECS: i64 = 180;


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
/// Lowered to 0 to catch new hourly sessions as they launch.
pub const MIN_MARKET_VOLUME: f64 = 0.0;

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
/// 30 pages = 3000 markets (sorted by 24hr volume descending).
/// Crypto hourly markets have enough volume to appear within the first 3000.
pub const GAMMA_API_MARKET_SCAN_PAGES: usize = 30;

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