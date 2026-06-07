/// SessionState — the shared mutable state that lives for the lifetime of a
/// trading session (process lifetime).
///
/// All fields are `Arc`-wrapped so the struct is cheaply cloneable and can be
/// handed to squadron patrol tasks, the API server, the LLM advisor, and
/// cleanup workers without copying data or taking locks.
///
/// ┌─────────────────────────────────────────────────────────────────────┐
/// │  Phase 3f-1 (current)                                               │
/// │  Introduces the type and bundles the individual Arc vars that        │
/// │  main.rs previously declared as loose locals.  All existing call    │
/// │  sites still use the individual variables — no behaviour change.     │
/// │                                                                      │
/// │  Phase 3f-3                                                          │
/// │  Squadron::patrol() will accept session.clone() directly, removing  │
/// │  the individual Arc::clone(&…) calls at every inner-loop site.      │
/// │                                                                      │
/// │  Phase 3f-5                                                          │
/// │  main.rs constructs SessionState::new(startup_balance) first,       │
/// │  then shadows individual vars via session.field.clone() — or        │
/// │  removes them entirely once all consumers accept SessionState.       │
/// └─────────────────────────────────────────────────────────────────────┘

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use alloy::primitives::U256;
use alloy::signers::local::LocalSigner;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::Mutex;
use tokio::time::Instant;
use polymarket_client_sdk_v2::clob::Client as ClobClient;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;

use crate::state::PositionMap;
use crate::helpers::balance::{PhantomCooldowns, OrphanTombstones};
use crate::vipers::time_decay_impl::TimeDecayPosition;

// ─── SessionState ─────────────────────────────────────────────────────────────

/// All `Arc`-wrapped, session-scoped mutable state for a DRADIS trading session.
///
/// Cheaply cloneable — each clone increments reference counts only; the
/// underlying `Mutex`-guarded data is shared, never duplicated.
#[derive(Clone)]
pub struct SessionState {
    /// Lowercase asset symbol for this session, e.g. `"btc"`, `"eth"`, `"sol"`.
    /// Used to select the correct per-asset SQLite pool for all DB writes.
    pub asset: String,
    /// Open positions tracked across all strategies.
    ///
    /// Key: `(strategy_name, token_id)` — each strategy owns its own slot
    /// per token so GBoost and Arbitrage can both hold the same token without
    /// colliding.
    pub positions: Arc<Mutex<PositionMap>>,

    /// Debounce map for rapid-fire order deduplication.
    ///
    /// Key: `(strategy_name, token_id)` → expiry `Instant`.
    /// An entry present with `expiry > Instant::now()` blocks a new order
    /// placement for that (strategy, token) pair.
    pub pending_orders: Arc<Mutex<HashMap<(String, U256), Instant>>>,

    /// Accumulated session P&L (realised on confirmed exits).
    pub total_pnl: Arc<Mutex<Decimal>>,

    /// Live pUSD collateral balance polled from the CLOB API every ~60 s.
    /// Strategies read this to self-gate on insufficient funds before placing
    /// an order, preventing 400 "not enough balance" rejections.
    pub live_collateral: Arc<Mutex<Decimal>>,

    /// Starting collateral captured at session init.
    /// Used by the LLM Advisor for drawdown context and by StrategyContext
    /// to compute session-level risk utilisation.
    pub starting_collateral: Arc<Mutex<Decimal>>,

    /// Phantom cooldowns: block re-entry on tokens whose on-chain fill has
    /// not yet been confirmed.
    ///
    /// Key: `"<strategy_name>:<token_id>"`.
    /// Set when `sync_position_balance` gives up on a token; expires after
    /// `PHANTOM_COOLDOWN_SECS` (600 s).
    pub phantom_cooldowns: PhantomCooldowns,

    /// Tombstones for tokens that have completed orphan-detection this session.
    ///
    /// Never cleared on market rotation — once a token is tombstoned it is
    /// never re-adopted within the same session.  This breaks the
    /// reconcile → re-adopt → orphan-detect cycle observed on 2026-05-19.
    pub orphan_tombstones: OrphanTombstones,

    /// TimeDecay strategy's per-token position metadata used by the cleanup
    /// worker to detect expired theta positions that need forced closure.
    pub time_decay_positions: Arc<Mutex<HashMap<U256, TimeDecayPosition>>>,

    /// Token ownership registry — maps `token_id` → `strategy_name`.
    ///
    /// The canonical, O(1) source of truth for which strategy owns each token.
    ///
    /// Populated at session startup by rebuilding from the positions map after
    /// `reconcile_orphaned_positions` runs.  Updated on every entry (insert) and
    /// exit (remove) in `patrol_impl.rs`.
    ///
    /// Enforces token sovereignty: before any strategy places an entry order the
    /// registry is checked.  If another strategy already claims the token the
    /// entry is rejected with a `WARN`-level log.  This prevents the class of
    /// cross-strategy interference bugs where (e.g.) GBoost re-enters a token
    /// that ArbitrageStrategy holds as a hedged leg, causing post-restart
    /// misattribution via the entries table.
    pub token_ownership: Arc<Mutex<HashMap<U256, String>>>,

    // ── Trading infrastructure (Phase 3f-8: manual RTB) ──────────────────────
    /// Authenticated CLOB REST client for manual exit orders via API.
    pub trading_client: Arc<ClobClient<Authenticated<Normal>>>,
    /// EOA signing key for manual exit order signatures.
    pub signer: LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    /// Session-scoped nonce manager for manual exit orders.
    pub nonce_manager: Arc<AtomicU64>,
    /// Shared HTTP client for manual exit order placement.
    pub shared_http: Arc<reqwest::Client>,
}

impl SessionState {
    /// Create a fresh session with all zeroed / empty state.
    ///
    /// `startup_balance` seeds both `live_collateral` and
    /// `starting_collateral` so strategies have an accurate budget from
    /// the very first tick.
    ///
    /// Phase 3f-8: Now also stores trading infrastructure (client, signer, etc.)
    /// so the API server can execute manual "Return to Base" exits via authenticated
    /// order placement.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        startup_balance: Decimal,
        asset: impl Into<String>,
        trading_client: Arc<ClobClient<Authenticated<Normal>>>,
        signer: LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
        nonce_manager: Arc<AtomicU64>,
        shared_http: Arc<reqwest::Client>,
    ) -> Self {
        Self {
            asset:                asset.into().to_lowercase(),
            positions:            Arc::new(Mutex::new(PositionMap::new())),
            pending_orders:       Arc::new(Mutex::new(HashMap::new())),
            total_pnl:            Arc::new(Mutex::new(dec!(0))),
            live_collateral:      Arc::new(Mutex::new(startup_balance)),
            starting_collateral:  Arc::new(Mutex::new(startup_balance)),
            phantom_cooldowns:    Arc::new(Mutex::new(HashMap::new())),
            orphan_tombstones:    Arc::new(Mutex::new(HashSet::new())),
            time_decay_positions: Arc::new(Mutex::new(HashMap::new())),
            token_ownership:      Arc::new(Mutex::new(HashMap::new())),
            trading_client,
            signer,
            nonce_manager,
            shared_http,
        }
    }
}


