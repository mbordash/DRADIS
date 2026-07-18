/// Admiral Adama — Squadron spawning infrastructure for user-deployed markets.
///
/// The Admiral Adama extension allows users to deploy squadrons to arbitrary
/// markets via the Control Tower UI. This module provides the infrastructure
/// needed to actually spawn and run those squadrons (trading client, signer,
/// orderbook feeds, etc.).
///
/// ## Architecture
///
/// `AdamaInfrastructure` bundles all the trading handles needed to spawn a
/// squadron. `main.rs` constructs these during startup and stores them in the
/// CAG via `cag.set_adama_infrastructure()`. The Admiral Adama processor in
/// `api/server.rs` then calls `cag.spawn_adama_squadron()` to create real
/// trading squadrons.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::collections::HashMap;

use alloy::primitives::Address;
use alloy::signers::local::LocalSigner;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use polymarket_client_sdk_v2::clob::Client as ClobClient;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;

use crate::cag::SessionState;
use crate::helpers::dynamic_config::DynamicConfig;
use crate::squadron::{Squadron, SquadronConfig, SquadronRaptors, CryptoAsset, MarketPriceFeeds};
use crate::squadron::raptors::SportsRaptorHandle;
use crate::state::MarketConfig;
use crate::venues::core::MarketId;
use crate::venues::intl::u256_from_market_id;

/// Trading infrastructure needed by Admiral Adama to spawn real squadrons.
///
/// Stored in the CAG after `main.rs` finishes initialisation. Contains all
/// the handles that a squadron needs to actually trade.
pub struct AdamaInfrastructure {
    /// Authenticated CLOB REST client — shared across all Adama squadrons.
    pub trading_client: Arc<ClobClient<Authenticated<Normal>>>,
    /// EOA signing key — cloned per order placement.
    pub signer: LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    /// Shared nonce manager for the Safe/Maker wallet.
    pub nonce_manager: Arc<AtomicU64>,
    /// Polymarket Safe (maker) address.
    pub safe_address: Address,
    /// EOA address.
    pub eoa_address: Address,
    /// Shared HTTP client.
    pub shared_http: Arc<reqwest::Client>,
    /// Sports Raptor signal receiver (for sports markets).
    pub sports_raptor: Option<SportsRaptorHandle>,
    /// Default session state for new squadrons.
    pub default_session: SessionState,
    /// Broadcasts strategy→market mapping to Control Tower.
    pub markets_tx: Arc<watch::Sender<HashMap<String, String>>>,
}

impl AdamaInfrastructure {
    /// Spawn a squadron for a specific market.
    ///
    /// Returns the squadron ID and a JoinHandle for the patrol task.
    pub async fn spawn_squadron(
        &self,
        squadron_id: String,
        market_id: &str,
        market_type: &str,
        market_question: &str,
        yes_token: &str,
        no_token: &str,
        _raptors: &[String],
        _vipers: &[String],
    ) -> Result<tokio::task::JoinHandle<()>, String> {
        info!(
            squadron_id = %squadron_id,
            market_id = %market_id,
            market_type = %market_type,
            "🚀 Admiral Adama: spawning squadron"
        );

        // Build MarketId wrappers for token IDs
        let yes_market_id = MarketId::new(yes_token);
        let no_market_id = MarketId::new(no_token);

        // Build MarketConfig for this market
        let market_config = MarketConfig {
            yes_token: yes_market_id.clone(),
            no_token: no_market_id.clone(),
            market_name: market_question.to_string(),
            market_close_time: None, // Event markets don't have hourly close
            strike_price: None,
            is_neg_risk: false,
            condition_id: market_id.to_string(),
            yes_fee_bps: 0,
            no_fee_bps: 0,
        };

        // Create asset from market type
        let asset = CryptoAsset::Custom(market_type.to_uppercase());

        // Build squadron config
        let squadron_config = SquadronConfig::full_wing(
            format!("{} Squadron — {}", market_type.to_uppercase(), &market_question[..market_question.len().min(40)])
        );

        // Build raptors based on market type
        let squadron_raptors = self.build_raptors_for_type(market_type);

        // Create the squadron
        let mut squadron = Squadron::new(
            asset,
            squadron_config,
            market_config,
            squadron_raptors,
        );
        // Override the auto-generated ID with our deployment ID
        squadron.id = squadron_id.clone();
        squadron.start_patrol();

        // Load/init squadron-scoped config
        let squadron_config = DynamicConfig::load_or_init_for_squadron(&squadron.id).await;
        let dynamic_config = Arc::new(std::sync::RwLock::new((*squadron_config).clone()));

        // Register the config handle
        crate::helpers::dynamic_config::register_squadron_config_handle(
            &squadron.id,
            Arc::clone(&dynamic_config),
        );

        // Subscribe to orderbook feeds
        let yes_u256 = u256_from_market_id(&yes_market_id).map_err(|e| e.to_string())?;
        let no_u256 = u256_from_market_id(&no_market_id).map_err(|e| e.to_string())?;
        let feeds = squadron.subscribe_markets(yes_u256, no_u256, None);

        // Classify and link to taxonomy
        squadron.classify_and_link().await;

        // Clone handles for the patrol task
        let trading_client = Arc::clone(&self.trading_client);
        let signer = self.signer.clone();
        let nonce_manager = Arc::clone(&self.nonce_manager);
        let safe_address = self.safe_address;
        let eoa_address = self.eoa_address;
        let shared_http = Arc::clone(&self.shared_http);
        let session = self.default_session.clone();
        let markets_tx = Arc::clone(&self.markets_tx);
        let cancel_token = CancellationToken::new();

        // Spawn the patrol task
        let handle = tokio::spawn(async move {
            info!(squadron_id = %squadron.id, "🛫 Admiral Adama squadron patrol started");
            
            // Run a simplified patrol loop for event markets
            run_adama_patrol(
                squadron,
                feeds,
                trading_client,
                signer,
                nonce_manager,
                safe_address,
                eoa_address,
                shared_http,
                session,
                dynamic_config,
                markets_tx,
                cancel_token,
            ).await;
            
            info!("🛬 Admiral Adama squadron patrol ended");
        });

        Ok(handle)
    }

    /// Build raptor signals based on market type.
    fn build_raptors_for_type(&self, market_type: &str) -> SquadronRaptors {
        match market_type {
            "sports" => {
                // Sports markets use the Sports Raptor
                if let Some(ref sports) = self.sports_raptor {
                    SquadronRaptors::sports_only(sports.clone())
                } else {
                    warn!("Sports raptor not available, using empty raptors");
                    SquadronRaptors::empty()
                }
            }
            "crypto" => {
                // Crypto markets would need price/funding raptors
                // For now, use empty - they should go through the normal market loop
                warn!("Crypto markets should use normal market loop, not Adama");
                SquadronRaptors::empty()
            }
            _ => {
                // Politics and other types - no raptors yet
                SquadronRaptors::empty()
            }
        }
    }
}

/// Simplified patrol loop for Admiral Adama squadrons.
///
/// Unlike the full `run_market_loop` which handles market rotation, this
/// loop runs until the market closes or is cancelled.
async fn run_adama_patrol(
    mut squadron: Squadron,
    feeds: MarketPriceFeeds,
    _trading_client: Arc<ClobClient<Authenticated<Normal>>>,
    _signer: LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    _nonce_manager: Arc<AtomicU64>,
    _safe_address: Address,
    _eoa_address: Address,
    _shared_http: Arc<reqwest::Client>,
    _session: SessionState,
    _dynamic_config: Arc<std::sync::RwLock<DynamicConfig>>,
    _markets_tx: Arc<watch::Sender<HashMap<String, String>>>,
    cancel: CancellationToken,
) {
    use tokio::time::{interval, Duration};
    
    let mut ticker = interval(Duration::from_secs(10));
    let squadron_id = squadron.id.clone();
    
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!(squadron_id = %squadron_id, "Admiral Adama squadron received cancel signal");
                break;
            }
            _ = ticker.tick() => {
                // Read current prices (tuple: bid, bid_depth, ask, ask_depth, timestamp)
                let yes_price = feeds.hourly_yes.borrow().clone();
                let no_price = feeds.hourly_no.borrow().clone();
                
                // Log heartbeat
                info!(
                    squadron_id = %squadron_id,
                    yes_bid = %yes_price.0,
                    yes_ask = %yes_price.2,
                    no_bid = %no_price.0,
                    no_ask = %no_price.2,
                    "💓 Admiral Adama squadron heartbeat"
                );
                
                // TODO: Run viper strategies here
                // For now, just monitor the market
            }
        }
    }
    
    squadron.stand_down();
    info!(squadron_id = %squadron_id, "Admiral Adama squadron stood down");
}
