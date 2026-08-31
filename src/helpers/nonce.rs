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

use alloy::primitives::Address;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{info, warn, error};

/// Fetch nonce for an address from the CLOB API
pub async fn fetch_next_nonce(http: &reqwest::Client, address: Address) -> Option<u64> {
    // Built with the query API rather than `format!` so the address can never be
    // read as part of the URL structure.
    //
    // Not a live vulnerability — `CLOB_API_BASE` is a compile-time constant and
    // `Address` is 20 bytes that Display as hex, so neither the host nor the path
    // is reachable from any input. But a string-formatted URL is what static
    // analysis flags (CodeQL "server-side request forgery", 2026-08), and this
    // form is both unflaggable and correct by construction.
    let url = format!("{}/nonce", crate::config::CLOB_API_BASE);
    match http.get(&url).query(&[("address", address.to_string())]).send().await {
        Ok(resp) => {
            let status = resp.status();
            if status == reqwest::StatusCode::NOT_FOUND {
                return Some(0);
            }
            let body = resp.text().await.unwrap_or_default();
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                if let Some(n) = json.get("next_nonce").and_then(|n| n.as_u64()) {
                    return Some(n);
                }
                warn!("⚠️ Nonce API response missing next_nonce (Status {}): {}", status, body);
            } else {
                warn!("⚠️ Nonce API returned non-JSON response (Status {}). Account might not be initialized or API is down.", status);
            }
        }
        Err(e) => error!("⚠️ Failed to connect to Nonce API: {:?}", e),
    }
    None
}

/// Synchronize nonce manager with the latest nonce from the API.
/// Uses AtomicU64 for lock-free access — safe to call from concurrent contexts.
pub async fn sync_nonce_manager(
    nonce_manager: &Arc<AtomicU64>,
    http: &reqwest::Client,
    address: Address,
) {
    if let Some(new_nonce) = fetch_next_nonce(http, address).await {
        nonce_manager.store(new_nonce, Ordering::SeqCst);
        info!("🔄 Nonce manager synchronized to: {} for address {}", new_nonce, address);
    }
}

/// Log nonce state for debugging nonce-related failures
pub async fn log_nonce_state(
    nonce_manager: &Arc<AtomicU64>,
    http: &reqwest::Client,
    address: Address,
    context: &str,
) {
    let local_nonce = nonce_manager.load(Ordering::SeqCst);
    if let Some(chain_nonce) = fetch_next_nonce(http, address).await {
        if local_nonce != chain_nonce {
            warn!("📊 [NONCE MISMATCH] {}: Local={}, Chain={}, Diff={}", context, local_nonce, chain_nonce, (chain_nonce as i64) - (local_nonce as i64));
        }
    }
}
