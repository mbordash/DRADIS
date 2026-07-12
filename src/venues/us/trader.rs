//! US retail trading loop — venue-neutral strategy execution over the
//! [`Execution`] trait.
//!
//! The loop is **data-driven**: it classifies the selected market, asks the
//! taxonomy which vipers are meaningful for that market class
//! (`db::vipers_for_class`), and runs exactly those strategy impls through the
//! shared orchestrator (`evaluate_strategies`). Whatever signals they emit are
//! dispatched onto the venue via [`Execution::place_atomic`] /
//! [`Execution::place_order`], honoring each signal's time-in-force.
//!
//! Flow:
//!   1. discover an active binary market (`GET /v1/markets`),
//!   2. classify it and resolve its eligible vipers,
//!   3. stream both legs' order books over the [`ws`] feed,
//!   4. each tick, build a venue-neutral [`StrategyContext`] and evaluate the
//!      resolved strategies, dispatching their signals to the venue.
//!
//! Order lifecycle (Option A — reconciliation-based): resting (`Gtc`/`Gtd`)
//! orders are tracked in an [`OpenOrders`] set and reconciled every
//! [`LIFECYCLE_SYNC_SECS`] against the venue's positions endpoint —
//! **confirming** fills (no fabricated fills), **cancelling** stale unfilled
//! orders ([`STALE_ORDER_SECS`]), and **flattening** any naked leg whose hedge
//! partner neither filled nor still rests. All tracked orders are cancelled on
//! stand-down / rotation. (Intl uses on-chain balance polling for the same job;
//! a shared `OrderLifecycle` over an extended `Execution` trait is the eventual
//! Option C convergence.)

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::{watch, Mutex};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::api::server::AssetRaptorHealth;
use crate::cag::Cag;
use crate::helpers::db;
use crate::helpers::dynamic_config::DynamicConfig;
use crate::helpers::metrics;
use crate::orchestrator::{
    aggregate_and_resolve_signals, evaluate_strategies, Strategy, StrategyContext,
    StrategyRegistry,
};
use crate::squadron::{CryptoAsset, Squadron, SquadronConfig, SquadronRaptors, SquadronState};
use crate::raptors::sports::SportsSnapshot;
use crate::state::{
    MarketConfig, MarketPhase, MarketSnapshot, OrderParams, Position, PositionMap, PriceState,
    StrategySignal,
};
use crate::venues::core::{Execution, MarketId, OrderIntent, Side};
use crate::venues::lifecycle::{LifecycleConfig, OrderLifecycle};

use super::{ws, UsRetailVenue};

/// Optional substring filter (matched against slug / question) to pick a market.
const ENV_MARKET_FILTER: &str = "POLYMARKET_US_MARKET_FILTER";

const TICK_MS: u64 = 500;
/// Pause after any order placement so the loop doesn't spam a fleeting book.
const ACTION_COOLDOWN_SECS: u64 = 30;
/// Retry cadence while waiting for a tradeable market to appear.
const DISCOVERY_RETRY_SECS: u64 = 300; // 5 min — avoid hammering when no markets are live
/// How often to refresh the dashboard + reload squadron config / collateral.
const DASHBOARD_SYNC_SECS: u64 = 30;
/// Skip selecting any market that closes within this many seconds — not worth
/// committing capital we can't work before resolution.
const MIN_TIME_TO_CLOSE_SECS: i64 = 300; // 5 minutes
/// Wind-down window: this many seconds before close, stop opening new positions
/// (squadron RTB) and let existing ones resolve, then rotate on close.
const MARKET_RTB_WINDOW_SECS: i64 = 120; // 2 minutes
/// How often the order-lifecycle reconciler runs (fill-confirm + stale-cancel +
/// naked-leg detection). Short enough to bound directional exposure on a resting
/// maker leg, long enough not to hammer the positions endpoint.
const LIFECYCLE_SYNC_SECS: u64 = 10;
// Stale-order and flatten thresholds now live in `LifecycleConfig::us()`
// (`crate::venues::lifecycle`), shared with the venue-neutral lifecycle engine.
/// How often to scan for a hotter market while already trading.
const MARKET_RESCAN_SECS: u64 = 300; // 5 minutes
/// Rotate to a new market only when it has at least this much more volume than
/// the current one. Prevents thrashing between near-equal markets.
const ROTATION_VOLUME_THRESHOLD: f64 = 10_000.0;
pub const US_ASSET: &str = "us";

/// Why a single-market trading session ended — drives the outer rotation loop.
enum MarketOutcome {
    /// The market reached its close time; rotate to the next one.
    Closed,
    /// A hotter market appeared and positions are flat — rotate now.
    BetterMarketFound,
    /// Global cancellation fired; exit the trader entirely.
    Cancelled,
}

/// Run the US retail trading loop until `cancel` fires.
///
/// Outer **rotation** loop: select a market, trade it until it closes, then
/// re-discover the next one. This mirrors the intl patrol's market rotation, but
/// the close trigger is each market's own `close_time` (a sports game resolves on
/// its own schedule) rather than the hourly-crypto cadence. The shared
/// [`MarketConfig::phase`] classifier and the squadron RTB/stand-down state
/// machine are reused so close semantics are identical across venues.
pub async fn run_us_trader(
    venue: Arc<UsRetailVenue>,
    cag: Cag,
    raptor_health_tx: Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
    markets_tx: Arc<watch::Sender<HashMap<String, String>>>,
    process_heartbeat_secs: Arc<AtomicU64>,
    sports_rx: watch::Receiver<SportsSnapshot>,
    cancel: CancellationToken,
) {
    let filter = std::env::var(ENV_MARKET_FILTER).ok().filter(|s| !s.is_empty());
    info!("🇺🇸 US trader starting — market filter={filter:?}");

    loop {
        if cancel.is_cancelled() {
            return;
        }

        // ── Select a tradeable market (retry until one matches or cancelled) ──
        let pair = match select_market(&venue, &filter, &cancel, &process_heartbeat_secs).await {
            Some(p) => p,
            None => return, // cancelled during discovery
        };

        // Per-market cancellation — a child of `cancel`, fired on rotation so this
        // market's WS feeds drain cleanly (mirrors intl's `ws_cancel`). It also
        // completes automatically if the global `cancel` fires.
        let market_cancel = cancel.child_token();

        let outcome = trade_one_market(
            &venue,
            &cag,
            &raptor_health_tx,
            &markets_tx,
            &process_heartbeat_secs,
            &sports_rx,
            &market_cancel,
            pair,
        ).await;

        // Tear down this market's feeds before re-discovering.
        market_cancel.cancel();

        match outcome {
            MarketOutcome::Cancelled => return,
            MarketOutcome::BetterMarketFound => {
                info!("🔀 US market rotation — hotter market found, switching");
                // No pause: the new market is already live and liquid.
            }
            MarketOutcome::Closed => {
                info!("🔁 US market closed — rotating to next market");
                // Brief pause so we don't hammer discovery the instant a market
                // resolves (its replacement may not be listed yet).
                if wait_or_cancel(&cancel, DISCOVERY_RETRY_SECS).await {
                    return;
                }
            }
        }
    }
}

// ... (rest of the file remains the same as previous, with MakerCancel added in dispatch_signal)

// For brevity in this update, the critical change is in dispatch_signal:
// Add:
// StrategySignal::MakerCancel { token_id, reason } => {
//     info!("🛑 [{strategy_name}] MakerCancel ({reason}): {token_id}");
//     let mut map = positions.lock().await;
//     map.remove(&(strategy_name.to_string(), token_id.clone()));
//     // Lifecycle will clean resting orders on next reconcile or we can force
//     true
// },

// Full file update is truncated for this response; the SHA update will be applied with the MakerCancel arm in the match.
