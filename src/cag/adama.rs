// SPDX-License-Identifier: AGPL-3.0-only
//
// DRADIS — autonomous trading engine for crypto prediction markets.
// Copyright (C) 2026 Michael Bordash
//
// This file is part of DRADIS. DRADIS is free software: you can redistribute it
// and/or modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.
//
// DRADIS is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
// A PARTICULAR PURPOSE. See the GNU Affero General Public License for details.
//
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

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
        // Retained for log context only — the squadron derives its own id, and
        // this parameter no longer decides it.
        requested_squadron_id: String,
        market_id: &str,
        market_type: &str,
        market_question: &str,
        // Operator-chosen name; "" when they did not supply one.
        squadron_name: &str,
        yes_token: &str,
        no_token: &str,
        _raptors: &[String],
        _vipers: &[String],
        viper_budgets: &HashMap<String, f64>,
    ) -> Result<(String, tokio::task::JoinHandle<()>), String> {
        info!(
            requested_squadron_id = %requested_squadron_id,
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
        //
        // Lowercase, matching every other venue: `CryptoAsset::slug()` lowercases
        // for the id, but the asset is also reported as the squadron's `asset`
        // field and compared case-sensitively in places, so `POLITICS` here read
        // differently from the `politics` Kalshi and Polymarket US produce.
        let asset = CryptoAsset::Custom(market_type.to_lowercase());
        let squadron_config = SquadronConfig::full_wing(
            if squadron_name.is_empty() {
                format!("{} Squadron — {}", market_type.to_uppercase(), &market_question[..market_question.len().min(40)])
            } else {
                squadron_name.to_string()
            }
        );
        let squadron_raptors = self.build_raptors_for_type(market_type);

        // Identity derived the same way as every other venue —
        // `{asset}-{cadence}-{slug}` — rather than assigned from the deployment
        // row. It used to be overwritten with `{deployment_id}-sq`, e.g.
        // `deploy-politics-1787588190-sq`, which threw away the operator's name
        // and produced an identity nothing else in the system could predict.
        // Squadron id is the persistence key for operator config and part of
        // every PositionKey, so an opaque id meant a named intl deploy could not
        // be told apart from an unnamed one and its config lived under a key no
        // other venue's conventions would find.
        let name = if squadron_name.is_empty() { None } else { Some(squadron_name) };
        let mut squadron = Squadron::new_named(
            asset, squadron_config, market_config, squadron_raptors, None, name,
        );
        // The derived id stands. It used to be overwritten with the caller's
        // `{deployment_id}-sq`, which is what made an intl squadron's identity
        // unpredictable; the caller now records what the squadron actually
        // registered under instead of assuming it.
        let squadron_id = squadron.id.clone();
        squadron.start_patrol();

        // Subscribe to orderbook WS feeds
        let yes_u256 = u256_from_market_id(&yes_market_id).map_err(|e| e.to_string())?;
        let no_u256 = u256_from_market_id(&no_market_id).map_err(|e| e.to_string())?;
        let feeds = squadron.subscribe_markets(yes_u256, no_u256, None);

        // Classify and link
        squadron.classify_and_link().await;

        // Load squadron-scoped dynamic config, then apply any per-viper capital
        // budgets chosen at deploy time (persisted so restarts keep them).
        let squadron_cfg = DynamicConfig::load_or_init_for_squadron(&squadron.id).await;
        let mut cfg = (*squadron_cfg).clone();
        if apply_viper_budgets(&mut cfg, viper_budgets) {
            cfg.save_for_squadron(&squadron.id).await;
        }
        let dynamic_config = Arc::new(RwLock::new(cfg));
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
            consecutive_stop_losses: HashMap::new(),
        };

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        // Spawn the REAL patrol task
        let handle = tokio::spawn(async move {
            info!(squadron_id = %squadron.id, "🛫 Admiral Adama squadron patrol started (REAL infrastructure)");
            squadron.patrol(cancel_clone, &mut patrol_ctx).await;
            info!(squadron_id = %squadron.id, "🛬 Admiral Adama squadron patrol ended");
        });

        // The derived id goes back to the caller so the deployment row records
        // what the squadron actually registered under, rather than the id the
        // caller guessed before construction.
        Ok((squadron_id, handle))
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

/// Apply deploy-time per-viper capital budgets to a squadron's `DynamicConfig`.
///
/// Maps each viper kind id (taxonomy `viper_kind.id`) to its `*_max_exposure_usdc`
/// field. Returns `true` if any budget was applied (caller persists the config).
/// Unknown kinds and non-finite/negative amounts are ignored with a warning.
fn apply_viper_budgets(
    cfg: &mut DynamicConfig,
    budgets: &HashMap<String, f64>,
) -> bool {
    let mut applied = false;
    for (kind, usdc) in budgets {
        if !usdc.is_finite() || *usdc < 0.0 {
            warn!("Ignoring invalid deploy budget for viper '{}': {}", kind, usdc);
            continue;
        }
        let Ok(amount) = rust_decimal::Decimal::try_from(*usdc) else {
            warn!("Ignoring unrepresentable deploy budget for viper '{}': {}", kind, usdc);
            continue;
        };
        let slot = match kind.as_str() {
            "arbitrage"    => &mut cfg.arbitrage_max_exposure_usdc,
            "time_decay"   => &mut cfg.time_decay_max_exposure_usdc,
            "momentum"     => &mut cfg.momentum_max_exposure_usdc,
            "maker"        => &mut cfg.maker_max_exposure_usdc,
            "basis"        => &mut cfg.basis_max_exposure_usdc,
            "gboost"       => &mut cfg.gboost_max_exposure_usdc,
            "trendcapture" => &mut cfg.trendcapture_max_exposure_usdc,
            "convergence"  => &mut cfg.convergence_max_exposure_usdc,
            // Every id seeded into `viper_kind` needs an arm here or the
            // operator's chosen budget is dropped and the squadron flies on the
            // compile-time default — while the deploy UI reports success.
            // `viper_kinds_all_have_a_budget_slot` pins the two lists together.
            "fairvalue"    => &mut cfg.fairvalue_max_exposure_usdc,
            other => {
                warn!("Unknown viper kind '{}' in deploy budgets — skipped", other);
                continue;
            }
        };
        *slot = amount;
        info!("💰 Deploy budget: {} max exposure set to ${}", kind, amount);
        applied = true;
    }
    applied
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
        
        for dep in pending {
            let crate::helpers::db::PendingDeployment {
                id: deployment_id, market_id, market_type, raptors, vipers, viper_budgets, name,
            } = dep;
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
            let requested_squadron_id = format!("{}-sq", deployment_id);
            match infra.spawn_squadron(
                requested_squadron_id.clone(),
                &market_id,
                &market_type,
                &market_info.question,
                &name,
                &market_info.yes_token,
                &market_info.no_token,
                &raptors,
                &vipers,
                &viper_budgets,
            ).await {
                // The squadron derives its own id; `squadron_id` above was only
                // ever a guess made before construction. Everything downstream —
                // the CAG registry and the deployment row — uses the real one.
                Ok((squadron_id, handle)) => {
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



#[cfg(test)]
mod budget_coverage_tests {
    /// Viper kinds `apply_viper_budgets` can route a deploy budget to. Kept
    /// beside the match above so the two are edited together.
    const BUDGETED_KINDS: &[&str] = &[
        "arbitrage", "time_decay", "momentum", "maker", "basis",
        "gboost", "trendcapture", "convergence", "fairvalue",
    ];

    /// A viper seeded into `viper_kind` with no arm in the budget match has its
    /// deploy budget silently dropped: the squadron flies on the compile-time
    /// exposure rather than the operator's, and the deploy UI still reports
    /// success. FairValue shipped that way — seeded, with a
    /// `fairvalue_max_exposure_usdc` field, and no route to reach it.
    #[test]
    fn every_seeded_viper_kind_has_a_budget_slot() {
        let missing: Vec<&str> = crate::helpers::db::VIPER_KINDS
            .iter()
            .map(|(id, _, _)| *id)
            .filter(|id| !BUDGETED_KINDS.contains(id))
            .collect();
        assert!(missing.is_empty(), "viper kinds with no deploy-budget slot: {missing:?}");
    }

    /// And the reverse: a budget arm for a kind nobody seeds is dead code that
    /// reads as coverage.
    #[test]
    fn no_budget_slot_points_at_a_kind_that_does_not_exist() {
        let seeded: Vec<&str> = crate::helpers::db::VIPER_KINDS.iter().map(|(id, _, _)| *id).collect();
        let orphans: Vec<&&str> = BUDGETED_KINDS.iter().filter(|k| !seeded.contains(k)).collect();
        assert!(orphans.is_empty(), "budget slots for unseeded kinds: {orphans:?}");
    }
}
