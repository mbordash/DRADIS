use alloy::primitives::Address;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn, error};

/// Fetch nonce for an address from the CLOB API
pub async fn fetch_next_nonce(http: &reqwest::Client, address: Address) -> Option<u64> {
    let url = format!("{}/nonce?address={}", crate::config::CLOB_API_BASE, address);
    match http.get(&url).send().await {
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

/// Synchronize nonce manager with the latest nonce from the API
pub async fn sync_nonce_manager(
    nonce_manager: &Arc<Mutex<u64>>,
    http: &reqwest::Client,
    address: Address,
) {
    if let Some(new_nonce) = fetch_next_nonce(http, address).await {
        let mut guard = nonce_manager.lock().await;
        *guard = new_nonce;
        info!("🔄 Nonce manager synchronized to: {} for address {}", new_nonce, address);
    }
}

/// Log nonce state for debugging nonce-related failures
pub async fn log_nonce_state(
    nonce_manager: &Arc<Mutex<u64>>,
    http: &reqwest::Client,
    address: Address,
    context: &str,
) {
    let local_nonce = {
        let guard = nonce_manager.lock().await;
        *guard
    };
    if let Some(chain_nonce) = fetch_next_nonce(http, address).await {
        if local_nonce != chain_nonce {
            warn!("📊 [NONCE MISMATCH] {}: Local={}, Chain={}, Diff={}", context, local_nonce, chain_nonce, (chain_nonce as i64) - (local_nonce as i64));
        }
    }
}

