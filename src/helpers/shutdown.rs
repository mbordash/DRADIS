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

//! Graceful shutdown — cancel resting orders before the process goes away.
//!
//! DRADIS rests real GTC orders on the book: Maker quotes, TimeDecay bids, and
//! the Maker resting-exit asks. Every way the process ended left them there,
//! live and fillable, with nothing running to manage a fill:
//!
//! * `docker stop` / `systemctl restart` send SIGTERM, and there was no handler
//!   at all, so tokio simply died mid-tick.
//! * The Control Tower's Setup → Restart button called `process::exit(0)`
//!   directly.
//!
//! The only cancel-all ran at the NEXT boot, after auth and balance retries, so
//! anything that filled in between became a position nobody entered, adopted
//! later at a stale mark by chain reconciliation.
//!
//! The hook is registered by `main` once the venue client exists, because
//! `Execution` exposes only `cancel(id)` and not a cancel-all — the bulk cancel
//! is a venue-SDK call, so each venue registers what it can do rather than the
//! trait pretending to offer it.
//!
//! Deliberately NOT wired to the watchdog's `process::exit(1)`. That fires
//! because the runtime has stalled for five minutes, so awaiting an async cancel
//! there could hang forever and defeat the restart the watchdog exists to force.
//! A stall exit accepts leaving orders resting; a clean exit must not.

use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;

use tracing::{info, warn};

type Hook = Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

static HOOK: OnceLock<Hook> = OnceLock::new();

/// Register the cancel-resting-orders action. First registration wins; calling
/// twice is a no-op rather than an error so a venue re-init cannot clobber it.
pub fn register(hook: Hook) {
    if HOOK.set(hook).is_err() {
        warn!("🛑 Shutdown hook already registered — keeping the first");
    }
}

/// Has a hook been registered? Used by tests and by the restart endpoint to
/// report honestly rather than implying a cancel happened.
pub fn is_registered() -> bool {
    HOOK.get().is_some()
}

/// Run the shutdown hook, if one is registered. Safe to call when none is.
pub async fn run() {
    match HOOK.get() {
        Some(hook) => {
            info!("🛑 Graceful shutdown: cancelling resting orders…");
            hook().await;
            info!("🛑 Graceful shutdown: done");
        }
        None => warn!("🛑 Graceful shutdown requested but no hook is registered — resting orders may remain open"),
    }
}

/// Watch for SIGTERM and Ctrl-C, run the hook, then exit 0.
///
/// SIGTERM is the one that matters in production: it is what `docker stop` and
/// systemd send, so it is the normal way a customer's engine stops.
pub fn spawn_signal_handler() {
    tokio::spawn(async {
        let signal = wait_for_terminate().await;
        warn!("🛑 {signal} received — cancelling resting orders before exit");
        run().await;
        std::process::exit(0);
    });
}

#[cfg(unix)]
async fn wait_for_terminate() -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            warn!("🛑 Could not install SIGTERM handler: {e} — falling back to Ctrl-C only");
            let _ = tokio::signal::ctrl_c().await;
            return "Ctrl-C";
        }
    };
    tokio::select! {
        _ = term.recv()               => "SIGTERM",
        _ = tokio::signal::ctrl_c()   => "Ctrl-C",
    }
}

#[cfg(not(unix))]
async fn wait_for_terminate() -> &'static str {
    let _ = tokio::signal::ctrl_c().await;
    "Ctrl-C"
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `run()` must not panic when nothing is registered — the API restart path
    /// calls it unconditionally, including on a box whose venue never connected.
    #[tokio::test]
    async fn run_without_a_hook_is_a_noop() {
        if !is_registered() {
            run().await;
        }
    }
}
