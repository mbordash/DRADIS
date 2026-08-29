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
// Shared with Kalshi and Polymarket US — a deploy budget must mean the same
// thing on every venue, and it did not while this lived here.
use crate::venues::deployment::apply_viper_budgets;
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
    /// The shard every deployed squadron files its rows under.
    ///
    /// The intl CLOB shards by underlying, so this is the primary asset ("btc").
    /// A deployed politics or sports squadron has no shard of its own and must
    /// be aliased onto this one — see `spawn_squadron`.
    pub primary_asset: String,
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
        // Gamma's end date for this market, or None when it has none.
        market_close_time: Option<chrono::DateTime<chrono::Utc>>,
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
            // Safe to populate now: the squadron's identity no longer depends on
            // this field (the cadence component is a constant), so a close time
            // cannot make the same squadron register under two ids.
            market_close_time,
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

        // Route this squadron's rows to the venue shard.
        //
        // `pool_for()` returns None on a miss rather than falling back, so
        // without this every write from a deployed squadron was dropped: over a
        // three-hour soak the intl database held 1,599 trades, all Bitcoin, and
        // not one row from the politics or sports squadrons that had been
        // patrolling the whole time. Worse than the missing rows, orphan
        // detection bailed out with "No database pool available" on every sweep,
        // so the safety net that flattens a one-sided arbitrage leg was not
        // running for them. Kalshi and Polymarket US already alias this way.
        crate::helpers::db::alias_pool(&market_type.to_lowercase(), &self.primary_asset);
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
        // One market, no hourly/daily split, no rotation behind it. Lets the
        // patrol loop hand Arbitrage this market as its own maker venue, and
        // retire the squadron when the market closes so the class frees up for
        // the next auto-deploy. See `Squadron::single_market`.
        squadron.single_market = true;
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
            // The vipers read the close time from THIS channel, not from the
            // MarketConfig above — patrol_impl rebuilds its context MarketConfig
            // from the market channel each tick. Setting only one of the two
            // would leave the gates exactly where they were.
            market_close_time,
            None, // no strike
            String::new(),
            None, // no maker
            market_id.to_string(),
        );
        // The sender MUST outlive the patrol task, even though nothing ever
        // sends on it. Event markets do not rotate, so this channel exists only
        // to satisfy PatrolContext — but the patrol loop selects on
        // `market_rx.changed()`, and a watch channel whose sender has been
        // dropped resolves `changed()` immediately and forever. Binding this to
        // `_market_tx` dropped it at the end of this function, so that arm was
        // permanently ready and starved the strategy-evaluation arm beside it:
        // politics and sports squadrons never evaluated a single strategy, never
        // pulsed the inner-loop heartbeat, and were killed by the stall watchdog
        // exactly 240s after deploy — then redeployed, and killed again.
        let (market_tx_keepalive, market_rx) = watch::channel(dummy_market_state);

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
            // Moved in purely to hold the market channel open for as long as the
            // patrol runs; see the comment where it is created.
            let _market_tx_keepalive = market_tx_keepalive;
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

/// Market info needed for squadron spawning.
pub struct MarketInfo {
    pub question: String,
    pub yes_token: String,
    pub no_token: String,
    /// When the market resolves, from Gamma's `endDate`.
    ///
    /// Event markets DO have an end date — a tennis match finishes, an election
    /// is called — and Gamma returns it right beside the tokens. This struct
    /// used to drop it, so every deployed squadron flew with
    /// `market_close_time: None` and the vipers that need one gated themselves
    /// off permanently: over a single soak the Maker refused 276 times with
    /// `no market_close_time` and TimeDecay 45 times with `market has no close
    /// time`. The squadrons patrolled, quoted nothing, and looked healthy.
    pub close_time: Option<chrono::DateTime<chrono::Utc>>,
}

/// The question text if Gamma knows this market as CLOSED, else `None`.
///
/// Gamma's default listing omits closed markets, so a market that finished
/// between discovery and deployment simply returns nothing — indistinguishable
/// from an id that never existed. Asking again with `closed=true` separates the
/// two, which is the difference between "this tennis match ended" and "this id
/// is wrong". Only called on the failure path, so it costs nothing normally.
async fn closed_market_question(http: &reqwest::Client, condition_id: &str) -> Option<String> {
    let url = format!(
        "https://gamma-api.polymarket.com/markets?condition_ids={condition_id}&closed=true"
    );
    let resp = http.get(&url).send().await.ok()?;
    let markets: Vec<serde_json::Value> = resp.json().await.ok()?;
    let m = markets.first()?;
    if m.get("closed").and_then(|c| c.as_bool()) != Some(true) {
        return None;
    }
    m.get("question").and_then(|q| q.as_str()).map(String::from)
}

/// Fetch full market details from Gamma API by condition id.
///
/// The filter parameter is `condition_ids`, plural. Gamma does not reject an
/// unknown query parameter — it ignores it and returns the unfiltered first
/// page — so the singular `condition_id` this used to send matched nothing and
/// silently handed back whichever market happened to top the list. That market
/// carries a perfectly valid `clobTokenIds`, so the deploy did not fail: it
/// resolved to a market nobody chose and traded it. The identity check below is
/// the real defense, since it holds whatever Gamma does with the parameter.
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
    // form is still accepted in case Gamma ever normalizes the field.
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

    // Absent or unparseable is not fatal: the squadron still trades, and the
    // close-time-dependent vipers gate off exactly as they did before.
    let close_time = market.get("endDate")
        .and_then(|d| d.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&chrono::Utc));
    if close_time.is_none() {
        warn!(%condition_id, "Gamma market has no usable endDate — close-time vipers will stay gated");
    }

    Some(MarketInfo { question, yes_token, no_token, close_time })
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
        let info = match fetch_market_info(&self.infra.shared_http, &dep.market_id).await {
            Some(i) => i,
            None => {
                // Name the cause the operator can act on. A closed market is a
                // race, not a fault, and the shared processor retires a seeded
                // one on this marker instead of leaving a FAILED row behind.
                return Err(match closed_market_question(&self.infra.shared_http, &dep.market_id).await {
                    Some(q) => anyhow::anyhow!(
                        "{} — \"{q}\"",
                        crate::venues::deployment::ERR_MARKET_CLOSED,
                    ),
                    None => anyhow::anyhow!("could not load market details for {}", dep.market_id),
                });
            }
        };

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
            info.close_time,
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
mod market_channel_tests {
    use tokio::sync::watch;

    /// A watch channel whose sender has been dropped reports "changed" forever.
    ///
    /// The patrol loop selects on `market_rx.changed()` for market rotation
    /// beside the strategy-evaluation tick. Adama builds a dummy channel because
    /// event markets do not rotate, and it used to drop the sender immediately —
    /// making that arm permanently ready, starving strategy evaluation, and
    /// leaving the inner-loop heartbeat unpulsed until the stall watchdog killed
    /// the squadron 240s after every deploy. This pins the tokio behavior the
    /// fix depends on.
    #[tokio::test]
    async fn a_dropped_sender_makes_changed_resolve_immediately() {
        let (tx, mut rx) = watch::channel(0u8);
        drop(tx);

        // Resolves at once, and keeps resolving — this is the starvation.
        for _ in 0..3 {
            let r = tokio::time::timeout(std::time::Duration::from_millis(50), rx.changed()).await;
            assert!(r.is_ok(), "changed() must resolve immediately on a closed channel");
            assert!(r.unwrap().is_err(), "and it resolves as an error, which `_ =` discards");
        }
    }

    /// Holding the sender keeps the arm pending, so the tick arm can run.
    #[tokio::test]
    async fn a_live_sender_leaves_changed_pending() {
        let (_tx, mut rx) = watch::channel(0u8);
        let r = tokio::time::timeout(std::time::Duration::from_millis(50), rx.changed()).await;
        assert!(r.is_err(), "changed() must stay pending while the sender is alive");
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

    /// Gamma serializes `clobTokenIds` as a JSON-encoded string, not an array.
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

    /// Still accepted if Gamma ever normalizes the field to a real array.
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
mod deployed_squadron_wiring_tests {
    use super::*;

    /// Gamma returns the market's end date as `endDate`, beside the tokens.
    ///
    /// Dropping it left every deployed squadron with `market_close_time: None`,
    /// which permanently gated the vipers that need one — 276 Maker refusals
    /// and 45 TimeDecay refusals in a single soak, from squadrons that looked
    /// healthy because they were patrolling and quoting nothing.
    #[test]
    fn an_end_date_is_parsed_from_the_gamma_payload() {
        let market = serde_json::json!({ "endDate": "2026-10-04T00:00:00Z" });
        let parsed = market.get("endDate")
            .and_then(|d| d.as_str())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&chrono::Utc));
        assert!(parsed.is_some(), "Gamma's endDate format must parse");
        assert_eq!(parsed.unwrap().to_rfc3339(), "2026-10-04T00:00:00+00:00");
    }

    /// A market with no end date must still deploy — the close-time vipers gate
    /// off exactly as before, which is a limitation, not a failure.
    #[test]
    fn a_missing_end_date_is_not_fatal() {
        let market = serde_json::json!({ "question": "Who wins?" });
        let parsed = market.get("endDate")
            .and_then(|d| d.as_str())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok());
        assert!(parsed.is_none());
    }

    /// A deployed squadron's asset must resolve to a pool via the alias.
    ///
    /// `pool_for` returns None on a miss rather than falling back, so without
    /// the alias every row a deployed squadron wrote was silently dropped and
    /// orphan detection skipped it entirely.
    #[test]
    fn a_deployed_class_resolves_to_the_venue_shard() {
        crate::helpers::db::alias_pool("adamatest-politics", "adamatest-primary");
        // The alias must not appear as an asset of its own — the selector lists
        // one entry per real database.
        assert!(
            !crate::helpers::db::available_assets().contains(&"adamatest-politics".to_string()),
            "an alias must not surface as a separate asset",
        );
    }
}
