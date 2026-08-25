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
        // The token the patrol task selects on. Supplied by the caller rather
        // than minted here: a token created inside this function and dropped at
        // its end is one nothing can ever fire, which is exactly how deployed
        // intl squadrons became unkillable.
        cancel: CancellationToken,
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

/// Fetch full market details from Gamma API by condition id.
///
/// The filter parameter is `condition_ids`, plural. Gamma does not reject an
/// unknown query parameter — it ignores it and returns the unfiltered first
/// page — so the singular `condition_id` this used to send matched nothing and
/// silently handed back whichever market happened to top the list. That market
/// carries a perfectly valid `clobTokenIds`, so the deploy did not fail: it
/// resolved to a market nobody chose and traded it. The identity check below is
/// the real defence, since it holds whatever Gamma does with the parameter.
pub async fn fetch_market_info(http: &reqwest::Client, condition_id: &str) -> Option<MarketInfo> {
    let url = format!(
        "https://gamma-api.polymarket.com/markets?condition_ids={}",
        condition_id
    );
    
    // Each failure says which one it was. This used to be a chain of `.ok()?`,
    // so every cause — transport error, unexpected shape, empty result —
    // collapsed into the same bare None and the caller could only report
    // "could not load market details", which is true of all of them and
    // actionable for none.
    let resp = match http.get(&url).send().await {
        Ok(r) => r,
        Err(e) => { warn!(%url, "Gamma request failed: {e}"); return None; }
    };
    let status = resp.status();
    let body = match resp.text().await {
        Ok(b) => b,
        Err(e) => { warn!(%status, "Gamma response unreadable: {e}"); return None; }
    };
    let markets: Vec<serde_json::Value> = match serde_json::from_str(&body) {
        Ok(m) => m,
        Err(e) => {
            warn!(%status, body = %body.chars().take(200).collect::<String>(),
                  "Gamma response was not a market list: {e}");
            return None;
        }
    };

    let Some(market) = markets.first() else {
        warn!(%condition_id, %status, "Gamma knows no market with this condition id");
        return None;
    };

    // Refuse anything that is not the market that was asked for. Deploying the
    // wrong market is worse than not deploying at all: the squadron trades real
    // size on a question the operator never selected.
    let returned = market.get("conditionId").and_then(|c| c.as_str()).unwrap_or_default();
    if !returned.eq_ignore_ascii_case(condition_id) {
        warn!(
            requested = %condition_id,
            returned = %returned,
            "Gamma returned a different market than requested — refusing to deploy it"
        );
        return None;
    }
    
    let Some(question) = market.get("question").and_then(|q| q.as_str()).map(String::from) else {
        warn!(%condition_id, "Gamma market has no question field");
        return None;
    };

    // Token ids come back as a JSON-encoded STRING — `"[\"123\", \"456\"]"` —
    // not as a JSON array, the same way `outcomes` does. `as_array()` therefore
    // returned None on every market Gamma has ever served, and the `?` swallowed
    // it: intl deployments failed here, silently, from the first one. The array
    // form is still accepted in case Gamma ever normalises the field.
    let tokens: Vec<String> = match market.get("clobTokenIds") {
        Some(serde_json::Value::Array(a)) =>
            a.iter().filter_map(|t| t.as_str().map(String::from)).collect(),
        Some(serde_json::Value::String(s)) =>
            serde_json::from_str::<Vec<String>>(s).unwrap_or_default(),
        _ => Vec::new(),
    };

    let (Some(yes_token), Some(no_token)) = (tokens.first().cloned(), tokens.get(1).cloned()) else {
        warn!(
            %condition_id, count = tokens.len(),
            "Gamma market does not expose a YES/NO token pair — cannot trade it"
        );
        return None;
    };

    Some(MarketInfo { question, yes_token, no_token })
}

/// Run the Admiral Adama deployment processor.
///
/// Polls the deployment_queue table and spawns real squadrons.
/// MUST run in main.rs where we have access to the wallet_provider.
/// Polymarket International's adapter onto the shared deployment queue.
///
/// Intl used to drain the queue with `run_adama_processor`, a second consumer
/// written before `venues::deployment` existed. Keeping two consumers meant
/// every fix landed twice or not at all, and intl quietly missed three that
/// Kalshi and Polymarket US had: it never requeued deployments interrupted by a
/// restart, so an operator lost every deployed squadron each time the Control
/// Tower restarted the engine for a config change; it never seeded the
/// auto-deploy classes, so politics and sports came up empty on intl while the
/// other venues populated them; and it had no cancellation path at all.
pub struct IntlDeploymentRunner<P> {
    pub infra: Arc<AdamaInfrastructure<P>>,
}

#[async_trait::async_trait]
impl<P> crate::venues::deployment::DeploymentRunner for IntlDeploymentRunner<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    fn venue_label(&self) -> &'static str { "Polymarket International" }

    async fn run_pinned(
        &self,
        dep: &crate::helpers::db::PendingDeployment,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let info = fetch_market_info(&self.infra.shared_http, &dep.market_id).await
            .ok_or_else(|| anyhow::anyhow!("could not load market details for {}", dep.market_id))?;

        let (squadron_id, handle) = self.infra.spawn_squadron(
            dep.id.clone(),
            &dep.market_id,
            &dep.market_type,
            &info.question,
            &dep.name,
            &info.yes_token,
            &info.no_token,
            &dep.raptors,
            &dep.vipers,
            &dep.viper_budgets,
            cancel.clone(),
        ).await.map_err(|e| anyhow::anyhow!("{e}"))?;

        // Registered with the SAME token the patrol task selects on, so a
        // stand-down from the Control Tower actually reaches it.
        self.infra.cag.register_adama_squadron(
            &squadron_id,
            &dep.market_id,
            &dep.market_type,
            &info.question,
            &dep.raptors,
            &dep.vipers,
            cancel,
        );

        // A patrol that ends because its market closed is a success, and so is
        // one an operator stood down. Only a panic is worth showing as failed.
        match handle.await {
            Ok(()) => Ok(()),
            Err(e) if e.is_cancelled() => Ok(()),
            Err(e) => Err(anyhow::anyhow!("squadron {squadron_id} patrol panicked: {e}")),
        }
    }

    async fn select_market(&self, class: &str, max_days_to_close: u32) -> Option<String> {
        // The same discovery the Control Tower's deploy browser uses, so a
        // seeded squadron lands on the market an operator would have picked.
        let horizon = max_days_to_close as i64 * 86_400;
        let found = crate::api::server::fetch_markets_by_type(
            &self.infra.shared_http, class, horizon, 0.0,
        ).await;
        found.into_iter()
            .max_by(|a, b| a.liquidity.total_cmp(&b.liquidity))
            .map(|m| m.condition_id)
    }
}

#[cfg(test)]
mod gamma_shape_tests {
    /// Extract the YES/NO pair exactly as `fetch_market_info` does.
    fn tokens_of(market: &serde_json::Value) -> Vec<String> {
        match market.get("clobTokenIds") {
            Some(serde_json::Value::Array(a)) =>
                a.iter().filter_map(|t| t.as_str().map(String::from)).collect(),
            Some(serde_json::Value::String(s)) =>
                serde_json::from_str::<Vec<String>>(s).unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    /// Gamma serialises `clobTokenIds` as a JSON-encoded string, not an array.
    ///
    /// The original code called `.as_array()` on it and let `?` swallow the
    /// None, so every intl deployment failed at this line reporting only
    /// "could not load market details". This payload is copied from a live
    /// Gamma response.
    #[test]
    fn token_ids_arrive_as_a_json_encoded_string() {
        let market: serde_json::Value = serde_json::json!({
            "question": "Will Renan Santos win the 2026 Brazilian presidential election?",
            "clobTokenIds": "[\"93998891488819623915454849994768171534113749478841216025646247933473925258016\", \"7565921021555775006041943394390068423142281108\"]",
        });
        assert!(market["clobTokenIds"].as_array().is_none(), "the field is a string, not an array");
        let tokens = tokens_of(&market);
        assert_eq!(tokens.len(), 2, "both legs must be recovered from the encoded string");
        assert!(tokens[0].starts_with("9399889"));
    }

    /// Still accepted if Gamma ever normalises the field to a real array.
    #[test]
    fn a_real_array_is_still_accepted() {
        let market = serde_json::json!({ "clobTokenIds": ["111", "222"] });
        assert_eq!(tokens_of(&market), vec!["111".to_string(), "222".to_string()]);
    }

    /// A market with one leg (or none) must not deploy half a position.
    #[test]
    fn an_incomplete_pair_yields_no_market() {
        let market = serde_json::json!({ "clobTokenIds": "[\"only-one\"]" });
        assert!(tokens_of(&market).get(1).is_none(), "a single leg must not pass as a pair");
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
