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

//! Venue-neutral deployment queue consumer.
//!
//! The Control Tower's deploy endpoint writes a row to `deployment_queue`; a
//! consumer picks it up and runs the market. That consumer was written for
//! Kalshi and lived in its trader, so Polymarket US — which had no consumer at
//! all — refused every deploy while still offering a Deploy button, and the
//! intl CLOB used a third, unrelated path.
//!
//! Almost none of the machinery was ever venue-specific. Requeueing interrupted
//! rows, claiming one so a second tick cannot start it twice, the status
//! transitions, the auto-deploy seeder and its dedupe are all queue mechanics.
//! Exactly two things differ per venue: turning a market id into something the
//! venue can trade, and choosing a market for a class. Those are the two methods
//! of [`DeploymentRunner`]; everything else lives here once.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::helpers::db;
use crate::helpers::dynamic_config::DynamicConfig;
use crate::cag::Cag;

/// How often the deployment queue is polled. Operational plumbing rather than a
/// trading parameter, so it stays a compile-time constant alongside the trade
/// loops' other cadences rather than becoming a Control Tower knob.
pub const DEPLOY_POLL_SECS: u64 = 5;

/// Market classes DRADIS keeps a squadron running for on its own, subject to
/// the `auto_deploy_*` switches. Crypto is absent because every venue's own
/// rotation loop or wing already owns it.
pub const AUTO_DEPLOY_CLASSES: [&str; 2] = ["politics", "sports"];

/// What a venue must supply for the shared queue consumer to drive it.
///
/// Deliberately narrow. Anything expressible in terms of the queue belongs in
/// [`run_deployment_processor`], not here — a venue that has to reimplement
/// claiming or dedupe is how the two consumers drifted apart in the first place.
#[async_trait::async_trait]
pub trait DeploymentRunner: Send + Sync + 'static {
    /// Human label for log lines ("Kalshi", "Polymarket US").
    fn venue_label(&self) -> &'static str;

    /// Resolve `market_id` and trade it until `cancel` fires.
    ///
    /// Returns an error only when the market cannot be resolved or started; a
    /// market that trades and then closes is a success. The error text is
    /// recorded against the deployment row and shown to the operator, so it
    /// should say what went wrong in their terms.
    /// The whole queue row is passed, not just the market id, because the
    /// operator chose more than a market: `viper_budgets` carries the per-viper
    /// exposure they set in the deploy dialog. Narrowing this to
    /// `(market_id, class, name)` is what let Kalshi and Polymarket US collect
    /// those budgets in the UI, store them, and then silently fly the
    /// compile-time exposure instead.
    async fn run_pinned(
        &self,
        dep: &db::PendingDeployment,
        cancel: CancellationToken,
    ) -> anyhow::Result<()>;

    /// Highest-volume open market in `class` within `max_days_to_close`, or
    /// `None` when the venue has nothing suitable open right now.
    ///
    /// `None` is not an error: an out-of-season sport or a quiet politics
    /// calendar is ordinary, and the seeder simply tries again next tick.
    async fn select_market(&self, class: &str, max_days_to_close: u32) -> Option<String>;
}

/// Drain the deployment queue until `cancel` fires, seeding auto-deploy classes
/// along the way.
pub async fn run_deployment_processor<R: DeploymentRunner>(
    runner: Arc<R>,
    cag: Cag,
    cancel: CancellationToken,
) {
    let label = runner.venue_label();
    let mut ticker = tokio::time::interval(Duration::from_secs(DEPLOY_POLL_SECS));
    info!("📋 {label} deployment processor started — operator-deployed squadrons enabled");

    // Nothing can be mid-flight at startup, so any row still marked active or
    // processing belongs to a previous process. Return it to the queue rather
    // than stranding it: the Control Tower restarts the engine for ordinary
    // config changes, and an operator should not silently lose every squadron
    // they deployed each time they adjust a setting.
    match db::requeue_interrupted_deployments().await {
        0 => {}
        n => info!("📋 Requeued {n} deployment(s) interrupted by the last restart"),
    }

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("📋 {label} deployment processor stopping");
                return;
            }
            _ = ticker.tick() => {}
        }

        // Seed the classes DRADIS is configured to keep running. Ordered after
        // the requeue above so a squadron restored from the last process is
        // already visible and is not duplicated.
        seed_auto_deployments(runner.as_ref(), &cag).await;

        for dep in db::fetch_pending_deployments().await {
            // Claim it first. fetch_pending_deployments only returns rows still
            // marked 'pending', so this is what stops the next tick — five
            // seconds away — from starting the same market a second time.
            if let Err(e) = db::update_deployment_status(&dep.id, "processing", None, None).await {
                warn!("📋 Could not claim deployment {}: {e}", dep.id);
                continue;
            }
            if let Err(e) = db::update_deployment_status(&dep.id, "active", None, None).await {
                warn!("📋 Could not mark deployment {} active: {e}", dep.id);
            }

            let runner_t = Arc::clone(&runner);
            // A child token so standing the venue down stops deployed markets too.
            let cancel_t = cancel.child_token();
            let dep_id = dep.id.clone();
            let class = dep.market_type.clone();
            let market_id = dep.market_id.clone();

            tokio::spawn(async move {
                info!("📋 Deploying {class} squadron on [{market_id}]");
                match runner_t.run_pinned(&dep, cancel_t).await {
                    Ok(()) => {
                        info!("📋 Deployed {class} squadron finished");
                        let _ = db::update_deployment_status(&dep_id, "completed", None, None).await;
                    }
                    Err(e) => {
                        // Recorded against the row so the Control Tower can show
                        // it. A deployment that fails silently is indistinguishable
                        // from one still starting, forever.
                        let msg = e.to_string();
                        warn!("📋 Deployment {dep_id} failed — {msg}");
                        let _ = db::update_deployment_status(&dep_id, "failed", None, Some(&msg)).await;
                    }
                }
            });
        }
    }
}

/// Keep a squadron running for every market class DRADIS is configured to
/// deploy on its own.
///
/// The seed goes through the ordinary deployment queue rather than spawning a
/// second lifecycle beside it: the resulting squadron is indistinguishable from
/// one an operator deployed, shows up in the Control Tower with the same status
/// transitions, and is stood down the same way.
///
/// Idempotent by construction: a class is seeded only when it has no live
/// squadron and no row already waiting, so this is safe on every tick. When a
/// seeded market closes its squadron ends and the next tick picks a fresh one —
/// that, rather than an internal rotation, is what keeps the class populated.
async fn seed_auto_deployments<R: DeploymentRunner + ?Sized>(runner: &R, cag: &Cag) {
    let Some(pool) = db::pool() else {
        warn!("📋 Auto-deploy: DB unavailable, skipping this pass");
        return;
    };
    let live: Vec<String> = cag
        .list_squadrons()
        .into_iter()
        .filter(|sq| sq.state != "STOOD_DOWN")
        .map(|sq| sq.asset.to_ascii_lowercase())
        .collect();
    // Every class with a deployment still in flight — including one being
    // claimed right now, which is briefly in neither the pending queue nor the
    // squadron list.
    let queued = db::deployment_classes_in_flight(pool).await;

    // Read the operator's switches fresh each pass so turning one off takes
    // effect without a restart. Only reached when a class actually needs
    // seeding, so the cost is a read on an idle tick at most.
    let mut cfg: Option<Arc<DynamicConfig>> = None;

    for class in AUTO_DEPLOY_CLASSES {
        if live.iter().any(|a| a == class) || queued.iter().any(|q| q == class) {
            continue;
        }
        let cfg = match &cfg {
            Some(c) => Arc::clone(c),
            None => {
                let c = DynamicConfig::load_or_default().await;
                cfg = Some(Arc::clone(&c));
                c
            }
        };
        let enabled = match class {
            "politics" => cfg.auto_deploy_politics,
            "sports" => cfg.auto_deploy_sports,
            _ => false,
        };
        if !enabled {
            continue;
        }

        let Some(market_id) = runner.select_market(class, cfg.deploy_max_days_to_close).await else {
            // Nothing suitable open right now (out-of-season sports, a quiet
            // politics calendar). Not an error — the next tick tries again.
            debug!("📋 Auto-deploy: no {class} market available yet");
            continue;
        };

        let raptors = db::raptors_for_class(pool, class).await;
        let vipers  = db::vipers_for_class(pool, class).await;

        let id = format!("autodeploy-{class}-{}", chrono::Utc::now().timestamp());
        match db::queue_deployment(&id, &market_id, class, "", &raptors, &vipers, &Default::default()).await {
            Ok(()) => info!("📋 Auto-deploying {class} squadron on [{market_id}]"),
            Err(e) => warn!("📋 Auto-deploy of {class} failed to queue: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AUTO_DEPLOY_CLASSES;

    /// Crypto is deliberately absent. Every venue's own rotation loop or wing
    /// already keeps a crypto squadron running, so seeding one would produce a
    /// second squadron on a class that is never empty — and the seeder's dedupe
    /// would then be the only thing preventing a duplicate on every tick.
    #[test]
    fn crypto_is_not_auto_seeded() {
        assert!(!AUTO_DEPLOY_CLASSES.contains(&"crypto"));
    }

    /// These strings are matched against `market_type` in the queue and against
    /// squadron assets, both of which are lowercase. A capitalised entry here
    /// would silently never match and the class would never seed.
    #[test]
    fn seeded_classes_are_lowercase() {
        for c in AUTO_DEPLOY_CLASSES {
            assert_eq!(*c, c.to_ascii_lowercase(), "{c} would never match");
        }
    }
}
