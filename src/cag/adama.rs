/// Admiral Adama — Squadron spawning infrastructure for user-deployed markets.
///
/// Uses the SAME `Squadron::patrol()` infrastructure as the original crypto
/// pipeline. The only difference is how squadrons are instantiated:
///   - Original: `CRYPTO_FILTER` env var at startup
///   - Adama: User deploys via Control Tower UI
///
/// Event markets (sports/politics) don't rotate hourly, so Adama creates a
/// dummy `market_rx` that never fires.

use std::collections::HashMap;
use std::sync::{Arc, atomic::AtomicU64, RwLock};

use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::signers::local::LocalSigner;
use chrono::Utc;
use tokio::sync::{watch, Mutex};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use polymarket_client_sdk_v2::clob::Client as ClobClient;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;

use crate::cag::{Cag, SessionState};
use crate::helpers::dynamic_config::DynamicConfig;
use crate::squadron::{Squadron, SquadronConfig, SquadronRaptors, CryptoAsset, PatrolContext};
use crate::squadron::raptors::SportsRaptorHandle;
use crate::state::MarketConfig;
use crate::tasks::market_monitor::MarketState;
use crate::venues::core::MarketId;
use crate::venues::intl::u256_from_market_id;

/// Trading infrastructure needed by Admiral Adama to spawn real squadrons.
///
/// Generic over `P` (wallet provider) to match `PatrolContext<P>`.
pub struct AdamaInfrastructure<P> {
    // ── Trading infrastructure ───────────────────────────────────────────────
    pub trading_client: Arc<ClobClient<Authenticated<Normal>>>,
    pub signer: LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    pub nonce_manager: Arc<AtomicU64>,
    pub safe_address: Address,
    pub eoa_address: Address,
    pub shared_http: Arc<reqwest::Client>,
    pub wallet_provider: P,

    // ── CAG and session ──────────────────────────────────────────────────────
    pub cag: Cag,
    pub default_session: SessionState,
    pub markets_tx: Arc<watch::Sender<HashMap<String, String>>>,

    // ── Raptor handles ───────────────────────────────────────────────────────
    pub sports_raptor: Option<SportsRaptorHandle>,

    // ── Notification credentials ─────────────────────────────────────────────
    pub tg_token: String,
    pub tg_chat_id: String,
    pub tw_api_key: String,
    pub tw_api_secret: String,
    pub tw_access_token: String,
    pub tw_access_token_secret: String,

    // ── Watchdog handles ─────────────────────────────────────────────────────
    pub process_heartbeat_secs: Arc<AtomicU64>,
}

impl<P> AdamaInfrastructure<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    /// Spawn a real trading squadron using the SAME patrol infrastructure as crypto.
    ///
    /// Creates a Squadron, builds PatrolContext, and calls squadron.patrol().
    /// Event markets don't rotate, so we use a dummy market_rx that never fires.
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
            "🚀 Admiral Adama: spawning squadron (using real patrol infrastructure)"
        );

        // Build MarketId wrappers
        let yes_market_id = MarketId::new(yes_token);
        let no_market_id = MarketId::new(no_token);

        // Build MarketConfig
        let market_config = MarketConfig {
            yes_token: yes_market_id.clone(),
            no_token: no_market_id.clone(),
            market_name: market_question.to_string(),
            market_close_time: None, // Event markets resolve at event end
            strike_price: None,
            is_neg_risk: false,
            condition_id: market_id.to_string(),
            yes_fee_bps: 0,
            no_fee_bps: 0,
        };

        // Create Squadron
        let asset = CryptoAsset::Custom(market_type.to_uppercase());
        let squadron_config = SquadronConfig::full_wing(
            format!("{} Squadron — {}", market_type.to_uppercase(), &market_question[..market_question.len().min(40)])
        );
        let squadron_raptors = self.build_raptors_for_type(market_type);

        let mut squadron = Squadron::new(asset, squadron_config, market_config, squadron_raptors);
        squadron.id = squadron_id.clone();
        squadron.start_patrol();

        // Subscribe to orderbook WS feeds
        let yes_u256 = u256_from_market_id(&yes_market_id).map_err(|e| e.to_string())?;
        let no_u256 = u256_from_market_id(&no_market_id).map_err(|e| e.to_string())?;
        let feeds = squadron.subscribe_markets(yes_u256, no_u256, None);

        // Classify and link
        squadron.classify_and_link().await;

        // Load squadron-scoped dynamic config
        let squadron_cfg = DynamicConfig::load_or_init_for_squadron(&squadron.id).await;
        let dynamic_config = Arc::new(RwLock::new((*squadron_cfg).clone()));
        crate::helpers::dynamic_config::register_squadron_config_handle(
            &squadron.id,
            Arc::clone(&dynamic_config),
        );

        // Create dummy market_rx — event markets don't rotate
        // MarketState is a tuple: (yes_token, no_token, name, close_time, strike, desc, maker, condition_id)
        let dummy_market_state: MarketState = (
            yes_market_id.clone(),
            no_market_id.clone(),
            market_question.to_string(),
            None, // no close time
            None, // no strike
            String::new(),
            None, // no maker
            market_id.to_string(),
        );
        let (_market_tx, market_rx) = watch::channel(dummy_market_state);

        // Build PatrolContext — same as run_market_loop does
        let mut patrol_ctx = PatrolContext {
            session: self.default_session.clone(),
            trading_client: Arc::clone(&self.trading_client),
            nonce_manager: Arc::clone(&self.nonce_manager),
            signer: self.signer.clone(),
            safe_address: self.safe_address,
            eoa_address: self.eoa_address,
            shared_http: Arc::clone(&self.shared_http),
            wallet_provider: self.wallet_provider.clone(),
            market_rx,
            dynamic_config,
            markets_tx: Arc::clone(&self.markets_tx),
            crypto_filter: market_type.to_uppercase(),
            tg_token: self.tg_token.clone(),
            tg_chat_id: self.tg_chat_id.clone(),
            tw_api_key: self.tw_api_key.clone(),
            tw_api_secret: self.tw_api_secret.clone(),
            tw_access_token: self.tw_access_token.clone(),
            tw_access_token_secret: self.tw_access_token_secret.clone(),
            process_heartbeat_secs: Arc::clone(&self.process_heartbeat_secs),
            last_heartbeat_at: Arc::new(Mutex::new(Instant::now())),
            feeds,
            maker_market_config: None,
            market_started_at: Utc::now(),
            cag: self.cag.clone(),
            last_trade_time: HashMap::new(),
            last_stop_loss_time: HashMap::new(),
            last_expiry_exit_time: HashMap::new(),
            last_exit_attempt_time: HashMap::new(),
        };

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        // Spawn the REAL patrol task
        let handle = tokio::spawn(async move {
            info!(squadron_id = %squadron.id, "🛫 Admiral Adama squadron patrol started (REAL infrastructure)");
            squadron.patrol(cancel_clone, &mut patrol_ctx).await;
            info!(squadron_id = %squadron.id, "🛬 Admiral Adama squadron patrol ended");
        });

        Ok(handle)
    }

    fn build_raptors_for_type(&self, market_type: &str) -> SquadronRaptors {
        match market_type {
            "sports" => {
                if let Some(ref sports) = self.sports_raptor {
                    SquadronRaptors::sports_only(sports.clone())
                } else {
                    warn!("Sports raptor not available, using empty raptors");
                    SquadronRaptors::empty()
                }
            }
            "crypto" => {
                warn!("Crypto markets should use run_market_loop, not Adama");
                SquadronRaptors::empty()
            }
            _ => SquadronRaptors::empty(),
        }
    }
}

/// Market info needed for squadron spawning.
pub struct MarketInfo {
    pub question: String,
    pub yes_token: String,
    pub no_token: String,
}

/// Fetch full market details from Gamma API by condition_id.
pub async fn fetch_market_info(http: &reqwest::Client, condition_id: &str) -> Option<MarketInfo> {
    let url = format!(
        "https://gamma-api.polymarket.com/markets?condition_id={}",
        condition_id
    );
    
    let resp = http.get(&url).send().await.ok()?;
    let markets: Vec<serde_json::Value> = resp.json().await.ok()?;
    
    let market = markets.first()?;
    
    let question = market.get("question")
        .and_then(|q| q.as_str())
        .map(String::from)?;
    
    // Token IDs are in the clobTokenIds array: [yes_token, no_token]
    let clob_tokens = market.get("clobTokenIds")
        .and_then(|t| t.as_array())?;
    
    let yes_token = clob_tokens.first()
        .and_then(|t| t.as_str())
        .map(String::from)?;
    
    let no_token = clob_tokens.get(1)
        .and_then(|t| t.as_str())
        .map(String::from)?;
    
    Some(MarketInfo { question, yes_token, no_token })
}

/// Run the Admiral Adama deployment processor.
///
/// Polls the deployment_queue table and spawns real squadrons.
/// MUST run in main.rs where we have access to the wallet_provider.
pub async fn run_adama_processor<P>(infra: Arc<AdamaInfrastructure<P>>)
where
    P: Provider + Clone + Send + Sync + 'static,
{
    use tokio::time::{interval, Duration};
    use tracing::error;
    
    let mut ticker = interval(Duration::from_secs(5));
    info!("📋 Admiral Adama processor started (real patrol infrastructure)");
    
    loop {
        ticker.tick().await;
        
        // Fetch pending deployments (returns Vec directly)
        let pending = crate::helpers::db::fetch_pending_deployments().await;
        
        if pending.is_empty() {
            continue;
        }
        
        info!("📋 Admiral Adama: {} pending deployment(s) found", pending.len());
        
        for (deployment_id, market_id, market_type, raptors, vipers) in pending {
            // Mark as processing
            if let Err(e) = crate::helpers::db::update_deployment_status(
                &deployment_id, "processing", None, None
            ).await {
                warn!("Failed to update deployment status: {}", e);
                continue;
            }
            
            info!(
                deployment_id = %deployment_id,
                market_id = %market_id,
                market_type = %market_type,
                raptors = ?raptors,
                vipers = ?vipers,
                "🛫 Admiral Adama: processing deployment"
            );
            
            // Fetch full market details from Gamma API
            let market_info = match fetch_market_info(&infra.shared_http, &market_id).await {
                Some(info) => info,
                None => {
                    warn!("Failed to fetch market details for {}", market_id);
                    if let Err(e) = crate::helpers::db::update_deployment_status(
                        &deployment_id, "failed", None, Some("Failed to fetch market details")
                    ).await {
                        warn!("Failed to update deployment status: {}", e);
                    }
                    continue;
                }
            };
            
            // Spawn a real trading squadron
            let squadron_id = format!("{}-sq", deployment_id);
            match infra.spawn_squadron(
                squadron_id.clone(),
                &market_id,
                &market_type,
                &market_info.question,
                &market_info.yes_token,
                &market_info.no_token,
                &raptors,
                &vipers,
            ).await {
                Ok(handle) => {
                    // Register in CAG with the JoinHandle
                    infra.cag.register_adama_squadron(
                        &squadron_id,
                        &market_id,
                        &market_type,
                        &market_info.question,
                        &raptors,
                        &vipers,
                        handle,
                    );
                    
                    // Mark as deployed in the queue
                    if let Err(e) = crate::helpers::db::update_deployment_status(
                        &deployment_id, "deployed", Some(&squadron_id), None
                    ).await {
                        warn!("Failed to mark deployment as deployed: {}", e);
                    }
                    
                    info!(
                        deployment_id = %deployment_id,
                        squadron_id = %squadron_id,
                        market_question = %market_info.question,
                        "✅ Admiral Adama: {} squadron DEPLOYED and PATROLLING",
                        market_type.to_uppercase()
                    );
                }
                Err(e) => {
                    error!(
                        deployment_id = %deployment_id,
                        error = %e,
                        "❌ Admiral Adama: failed to spawn squadron"
                    );
                    if let Err(e) = crate::helpers::db::update_deployment_status(
                        &deployment_id, "failed", None, Some(&format!("Spawn failed: {}", e))
                    ).await {
                        warn!("Failed to update deployment status: {}", e);
                    }
                }
            }
        }
    }
}
