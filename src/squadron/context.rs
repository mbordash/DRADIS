/// PatrolContext — everything `Squadron::patrol()` needs to drive the inner loop.
///
/// Owned by `main.rs` and passed as `&mut PatrolContext<P>` to `patrol()` on
/// each market rotation so that:
///   • Cooldown maps (`last_trade_time`, etc.) survive market rotations without
///     Arc/Mutex overhead — they are plain `HashMap<String, Instant>` mutated
///     exclusively by the single running patrol() call.
///   • Per-market feeds (`feeds`, `maker_market_config`, `market_started_at`)
///     can be updated in-place before each patrol() invocation.
///   • Generic over `P` (the alloy wallet provider) so the settlement ticker
///     can call `auto_settle_closed_positions(ctx.wallet_provider.clone(), …)`
///     without dynamic dispatch.
///
/// ┌──────────────────────────────────────────────────────────────────────────┐
/// │  Phase 3f-3 (current)                                                    │
/// │  Introduced so Squadron::patrol() drives the full inner tick loop.       │
/// │  main.rs constructs one PatrolContext outside 'market_loop and updates   │
/// │  per-market fields before each patrol() call.                            │
/// │                                                                          │
/// │  Phase 3f-5                                                              │
/// │  CAG constructs PatrolContext from its own registry; main.rs is reduced  │
/// │  to a thin bootstrapper that calls Cag::run().                           │
/// └──────────────────────────────────────────────────────────────────────────┘

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use alloy::primitives::Address;
use alloy::signers::local::LocalSigner;
use chrono::{DateTime, Utc};
use tokio::sync::{watch, Mutex};
use tokio::time::Instant;

use polymarket_client_sdk_v2::clob::Client as ClobClient;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;

use crate::cag::{Cag, SessionState};
use crate::helpers::dynamic_config::DynamicConfig;
use crate::state::MarketConfig;
use crate::tasks::market_monitor::MarketState;
use super::MarketPriceFeeds;

// ─── PatrolContext ────────────────────────────────────────────────────────────

/// All infrastructure `Squadron::patrol()` needs to drive the inner tick loop.
///
/// Generic over `P` (the alloy wallet provider type) so the settlement arm can
/// call `auto_settle_closed_positions(ctx.wallet_provider.clone(), …)` without
/// dynamic dispatch or `Arc<dyn Provider>` boxing.
pub struct PatrolContext<P> {
    // ── Session-scoped shared state ──────────────────────────────────────────
    /// All Arc-wrapped session mutable state (positions, PnL, collateral, etc.).
    pub session: SessionState,

    // ── Trading infrastructure ───────────────────────────────────────────────
    pub trading_client: Arc<ClobClient<Authenticated<Normal>>>,
    pub nonce_manager:  Arc<AtomicU64>,
    /// Wallet signing key — cheaply cloned per order placement.
    pub signer: LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    pub safe_address: Address,
    pub eoa_address:  Address,
    pub shared_http:  Arc<reqwest::Client>,
    /// Alloy wallet provider — consumed only by `auto_settle_closed_positions`.
    pub wallet_provider: P,

    // ── Market rotation ──────────────────────────────────────────────────────
    /// Market-state channel — patrol() watches this to detect rotation.
    pub market_rx: watch::Receiver<MarketState>,

    // ── Configuration ────────────────────────────────────────────────────────
    /// Dynamic runtime config (strategy parameters tunable without restart).
    pub config_rx:  watch::Receiver<Arc<DynamicConfig>>,
    /// Broadcasts the strategy→market mapping to the Control Tower status feed.
    pub markets_tx: Arc<watch::Sender<HashMap<String, String>>>,
    /// Upper-case crypto symbol (e.g. `"BTC"`) for oracle price filtering.
    pub crypto_filter: String,

    // ── Notification credentials ─────────────────────────────────────────────
    pub tg_token:               String,
    pub tg_chat_id:             String,
    pub tw_api_key:             String,
    pub tw_api_secret:          String,
    pub tw_access_token:        String,
    pub tw_access_token_secret: String,

    // ── Watchdog heartbeats ──────────────────────────────────────────────────
    /// UNIX epoch seconds of the last process heartbeat — read by the OS-thread
    /// watchdog to detect a frozen tokio runtime.
    pub process_heartbeat_secs: Arc<AtomicU64>,
    /// Updated by the strategy ticker and status ticker on every live tick.
    /// The inner-loop watchdog arm reads `elapsed()` to detect stalls.
    pub last_heartbeat_at: Arc<Mutex<Instant>>,

    // ── Per-market data (updated before each patrol() call) ─────────────────
    /// Live WS orderbook price receivers for the current rotation.
    pub feeds: MarketPriceFeeds,
    /// Config for the window/daily maker venue, or `None` if unavailable.
    pub maker_market_config: Option<MarketConfig>,
    /// Wall-clock start of the current market rotation.
    pub market_started_at: DateTime<Utc>,

    /// CAG registry handle — used by patrol() to update squadron state on
    /// market rotation and stand-down.
    pub cag: Cag,

    // ── Session-scoped cooldown maps (survive market rotation) ───────────────
    //
    // Stored as plain `HashMap` (not Arc<Mutex>) because patrol() runs
    // exclusively — no concurrent access.  Being in PatrolContext (owned by
    // main.rs outside 'market_loop) means they persist across rotations,
    // preserving the stop-loss and expiry-exit cooldown state through hourly
    // market switches.
    //
    /// Last successful trade time per strategy — gates `TRADE_COOLDOWN_SECS`.
    pub last_trade_time: HashMap<String, Instant>,
    /// Last stop-loss exit time per strategy — gates `STOP_LOSS_COOLDOWN_SECS`.
    pub last_stop_loss_time: HashMap<String, Instant>,
    /// Last expiry-exit time per strategy — gates the 5-minute re-entry block.
    pub last_expiry_exit_time: HashMap<String, Instant>,
    /// Last FAK-miss exit attempt per strategy — throttles retry log floods.
    pub last_exit_attempt_time: HashMap<String, Instant>,
}

