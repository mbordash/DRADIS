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

//! [E102] Which crypto underlyings the venue's rotation flies, and whether each
//! rotation loop is alive. Venue-neutral: Polymarket International runs one
//! patrol loop per asset, Kalshi one rotation loop per underlying, and both
//! read the same setting (`TRADE_COINS`, e.g. `btc,eth,sol`).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// The underlyings a rotation may fly: the rows of [`crate::helpers::coins`].
/// Anything else is refused at startup with a warning: an unsupported one
/// would register as a BTC squadron (`register_kalshi_squadron`) or open a
/// feed for a symbol with no market.
pub fn supported_underlyings() -> Vec<&'static str> {
    crate::helpers::coins::slugs()
}

/// The environment variable naming the underlyings, in order; the first is the
/// instance's primary one (see `dynamic_config::starts_held`).
pub const ENV_TRADE_COINS: &str = "TRADE_COINS";

/// Parse a comma-separated list: lower-cased, de-duplicated, order kept.
/// Returns (accepted, refused).
pub fn parse_underlyings(raw: &str) -> (Vec<String>, Vec<String>) {
    let mut ok: Vec<String> = Vec::new();
    let mut refused = Vec::new();
    for u in raw.split(',').map(|s| s.trim().to_ascii_lowercase()).filter(|s| !s.is_empty()) {
        if !crate::helpers::coins::is_slug(&u) {
            refused.push(u);
        } else if !ok.contains(&u) {
            ok.push(u);
        }
    }
    (ok, refused)
}

/// The configured underlyings, if the operator set any. `None` keeps each
/// venue's pre-E102 behavior (one underlying from its own settings).
pub fn configured_underlyings() -> Option<Vec<String>> {
    let raw = std::env::var(ENV_TRADE_COINS).ok()?;
    let (ok, refused) = parse_underlyings(&raw);
    if !refused.is_empty() {
        tracing::warn!(
            "⚠️ {ENV_TRADE_COINS}: ignoring {refused:?}; supported underlyings are {:?}",
            supported_underlyings()
        );
    }
    (!ok.is_empty()).then_some(ok)
}

// ─── Per-loop liveness ───────────────────────────────────────────────────────

fn loops() -> &'static std::sync::Mutex<BTreeMap<String, Arc<AtomicU64>>> {
    static L: std::sync::OnceLock<std::sync::Mutex<BTreeMap<String, Arc<AtomicU64>>>> = std::sync::OnceLock::new();
    L.get_or_init(Default::default)
}

fn now_secs() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// A heartbeat for one rotation loop, registered under `name` (e.g.
/// `kalshi:eth`). The loop touches it exactly where it touched the process
/// heartbeat; [`spawn_heartbeat_forwarder`] carries the newest one to the
/// process watchdog, so any loop's tick still counts for the watchdog (as
/// before) while `/api/status` shows each loop's own age.
pub fn register_loop(name: &str) -> Arc<AtomicU64> {
    let hb = Arc::new(AtomicU64::new(now_secs()));
    let mut m = loops().lock().unwrap_or_else(|p| p.into_inner());
    m.insert(name.to_string(), Arc::clone(&hb));
    hb
}

/// Stop tracking a loop that ended on purpose (stand-down, cancellation), so
/// it is not reported as stuck.
pub fn retire_loop(name: &str) {
    let mut m = loops().lock().unwrap_or_else(|p| p.into_inner());
    m.remove(name);
}

/// Mark a heartbeat as ticked now (for loops idling on purpose, e.g. a
/// retired instance's 30 s sleeps).
pub fn touch(hb: &AtomicU64) {
    hb.store(now_secs(), Ordering::Relaxed);
}

/// Seconds since each registered loop last ticked.
pub fn loop_ages() -> BTreeMap<String, u64> {
    let now = now_secs();
    let m = loops().lock().unwrap_or_else(|p| p.into_inner());
    m.iter().map(|(k, hb)| (k.clone(), now.saturating_sub(hb.load(Ordering::Relaxed)))).collect()
}

/// A loop silent this long is reported in the log and on the dashboard.
pub const LOOP_STALL_WARN_SECS: u64 = 180;
/// A loop silent this long ends the process so the supervisor restarts it,
/// as the process watchdog does for a silent process: with several loops the
/// others keep the process heartbeat fresh, so one stuck real-money loop would
/// otherwise go unmanaged indefinitely.
pub const LOOP_STALL_EXIT_SECS: u64 = 300;

/// Pure rule behind the forwarder's stall exit.
pub fn stalled_loops(ages: &BTreeMap<String, u64>, limit: u64) -> Vec<String> {
    ages.iter().filter(|(_, a)| **a > limit).map(|(n, _)| n.clone()).collect()
}

/// Copy the newest loop heartbeat to the process heartbeat every second, and
/// warn once a minute about any loop silent past [`LOOP_STALL_WARN_SECS`].
pub fn spawn_heartbeat_forwarder(process: Arc<AtomicU64>, cancel: tokio_util::sync::CancellationToken) {
    tokio::spawn(async move {
        let mut tick = 0u64;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            }
            let newest = {
                let m = loops().lock().unwrap_or_else(|p| p.into_inner());
                m.values().map(|hb| hb.load(Ordering::Relaxed)).max()
            };
            if let Some(n) = newest { process.fetch_max(n, Ordering::Relaxed); }
            tick += 1;
            let ages = loop_ages();
            let parked = crate::helpers::watchdog::is_parked_for_setup()
                || crate::helpers::watchdog::is_parked_for_backup()
                || crate::helpers::migration::is_retired();
            let dead = stalled_loops(&ages, LOOP_STALL_EXIT_SECS);
            if !parked && !dead.is_empty() {
                tracing::error!("❌ Rotation loop(s) {dead:?} silent past {LOOP_STALL_EXIT_SECS}s while others run — exiting so the supervisor restarts the engine");
                crate::helpers::watchdog::dump_thread_states();
                std::process::exit(1);
            }
            if tick % 60 == 0 {
                for name in stalled_loops(&ages, LOOP_STALL_WARN_SECS) {
                    tracing::warn!("⚠️ Rotation loop {name} has not ticked for {}s", ages[&name]);
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_validates_and_keeps_order() {
        let (ok, refused) = parse_underlyings(" BTC, eth ,xrp,sol,eth,, doge, ada");
        assert_eq!(ok, vec!["btc", "eth", "xrp", "sol", "doge"]);
        assert_eq!(refused, vec!["ada"]);
        assert_eq!(parse_underlyings("").0, Vec::<String>::new());
    }

    #[test]
    fn stalled_loops_are_named_past_the_limit() {
        let ages: BTreeMap<String, u64> = [("kalshi:btc".to_string(), 5), ("kalshi:eth".to_string(), 301)].into_iter().collect();
        assert_eq!(stalled_loops(&ages, 300), vec!["kalshi:eth".to_string()]);
        assert!(stalled_loops(&ages, 400).is_empty());
        let hb = register_loop("test:rotation-retired");
        retire_loop("test:rotation-retired");
        assert!(!loop_ages().contains_key("test:rotation-retired"));
        drop(hb);
    }

    #[test]
    fn loop_ages_report_each_loop() {
        let hb = register_loop("test:rotation-a");
        hb.store(now_secs() - 50, Ordering::Relaxed);
        let ages = loop_ages();
        assert!(ages["test:rotation-a"] >= 50);
        loops().lock().unwrap().remove("test:rotation-a");
    }
}
