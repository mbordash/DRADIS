//! Operator funding actions on venues that split collateral into pools.
//!
//! Kalshi keeps a separate balance per exchange shard and its API never moves
//! money between them, so an operator whose deposit sits on the default shard
//! has every crypto order rejected. These endpoints let the Control Tower move
//! collateral between pools and set the venue's automatic rebalancing target,
//! through the venue-neutral [`Execution`] surface; a single-wallet venue
//! answers that it has nothing to move.
//!
//! Admin-gated like Setup: moving money is the operator's decision, taken
//! behind the admin session with a confirmation in the UI. The X-API-Key and
//! read-only layers in `server.rs` wrap these routes as they wrap Setup's.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::api::server::ApiState;
use crate::venues::core::{AllocationTarget, Execution};

pub fn admin_routes() -> Router<ApiState> {
    Router::new()
        .route("/api/venue/collateral-pools/transfer",   post(transfer))
        .route("/api/venue/collateral-pools/allocation", get(get_allocation).put(put_allocation))
        .layer(axum::middleware::from_fn(crate::api::setup::require_admin))
}

fn err(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, Json(serde_json::json!({ "error": msg.into() }))).into_response()
}

#[derive(Debug, Deserialize)]
pub struct TransferRequest {
    pub from: String,
    pub to: String,
    /// Dollars, as a decimal string.
    pub amount: String,
}

#[derive(Debug, Serialize)]
pub struct TransferResponse {
    pub transfer_id: String,
    pub from: String,
    pub to: String,
    pub amount: String,
}

/// Validate a transfer request into (from, to, amount). Pure, so the rules a
/// request must meet are tested without a venue.
pub fn validate_transfer(req: &TransferRequest) -> Result<(String, String, Decimal), String> {
    let from = req.from.trim();
    let to = req.to.trim();
    if from.is_empty() || to.is_empty() {
        return Err("name both the source and the destination pool".into());
    }
    if from == to {
        return Err("source and destination are the same pool".into());
    }
    let amount: Decimal = req.amount.trim().parse().map_err(|_| format!("amount {:?} is not a number", req.amount))?;
    if amount <= Decimal::ZERO {
        return Err("amount must be positive".into());
    }
    if amount.scale() > 4 {
        return Err("amount has more than four decimal places".into());
    }
    Ok((from.to_string(), to.to_string(), amount))
}

/// Validate an allocation: percentages total 100, each pool once. Empty is
/// valid and means "turn automatic rebalancing off".
pub fn validate_allocation(targets: &[AllocationTarget]) -> Result<(), String> {
    if targets.is_empty() {
        return Ok(());
    }
    let mut seen = std::collections::HashSet::new();
    for t in targets {
        if t.pool_id.trim().is_empty() {
            return Err("every target names a pool".into());
        }
        if t.percent > 100 {
            return Err(format!("pool {} has {}%, above 100", t.pool_id, t.percent));
        }
        if !seen.insert(t.pool_id.trim()) {
            return Err(format!("pool {} is listed twice", t.pool_id));
        }
    }
    let total: u32 = targets.iter().map(|t| t.percent as u32).sum();
    if total != 100 {
        return Err(format!("percentages total {total}, not 100"));
    }
    Ok(())
}

/// POST /api/venue/collateral-pools/transfer
async fn transfer(State(s): State<ApiState>, Json(req): Json<TransferRequest>) -> Response {
    let (from, to, amount) = match validate_transfer(&req) {
        Ok(v) => v,
        Err(e) => return err(StatusCode::BAD_REQUEST, e),
    };
    let Some(session) = s.cag.session() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "no venue session is registered yet; the engine is still starting");
    };
    let venue = session.venue.as_ref();
    // The venue refuses a repeat of an accepted key, so a retried request
    // cannot move the money twice.
    let key = idempotency_key();
    match tokio::time::timeout(std::time::Duration::from_secs(20), Execution::move_collateral(venue, &from, &to, amount, &key)).await {
        Ok(Ok(transfer_id)) => {
            tracing::info!("💰 operator moved ${amount:.2} of collateral from pool {from} to pool {to} (transfer {transfer_id})");
            Json(TransferResponse { transfer_id, from, to, amount: amount.to_string() }).into_response()
        }
        Ok(Err(e)) => err(StatusCode::BAD_GATEWAY, format!("the venue refused the transfer: {e}")),
        Err(_) => err(StatusCode::GATEWAY_TIMEOUT, "the venue did not answer within 20s; check the balances before retrying"),
    }
}

/// A random version-4 UUID from the OS, the shape venues expect for a client
/// transfer id. Built on the same entropy source the admin session keys use.
fn idempotency_key() -> String {
    use argon2::password_hash::rand_core::RngCore;
    let mut b = [0u8; 16];
    argon2::password_hash::rand_core::OsRng.fill_bytes(&mut b);
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // RFC 4122 variant
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
    )
}

#[derive(Debug, Serialize)]
pub struct AllocationView {
    /// False when the venue has no automatic rebalancing to set.
    pub supported: bool,
    /// The standing target; empty when rebalancing is off. Null when unsupported.
    pub targets: Option<Vec<AllocationTarget>>,
}

/// GET /api/venue/collateral-pools/allocation
async fn get_allocation(State(s): State<ApiState>) -> Response {
    let Some(session) = s.cag.session() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "no venue session is registered yet; the engine is still starting");
    };
    match tokio::time::timeout(std::time::Duration::from_secs(10), Execution::collateral_allocation(session.venue.as_ref())).await {
        Ok(Ok(Some(targets))) => Json(AllocationView { supported: true, targets: Some(targets) }).into_response(),
        Ok(Ok(None)) => Json(AllocationView { supported: false, targets: None }).into_response(),
        Ok(Err(e)) => err(StatusCode::BAD_GATEWAY, format!("the venue could not report its allocation: {e}")),
        Err(_) => err(StatusCode::GATEWAY_TIMEOUT, "the venue did not answer within 10s"),
    }
}

#[derive(Debug, Deserialize)]
pub struct AllocationRequest {
    pub targets: Vec<AllocationTarget>,
}

/// PUT /api/venue/collateral-pools/allocation
async fn put_allocation(State(s): State<ApiState>, Json(req): Json<AllocationRequest>) -> Response {
    if let Err(e) = validate_allocation(&req.targets) {
        return err(StatusCode::BAD_REQUEST, e);
    }
    let Some(session) = s.cag.session() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "no venue session is registered yet; the engine is still starting");
    };
    match tokio::time::timeout(std::time::Duration::from_secs(20), Execution::set_collateral_allocation(session.venue.as_ref(), &req.targets)).await {
        Ok(Ok(())) => {
            tracing::info!("💰 operator set the collateral allocation: {:?}", req.targets.iter().map(|t| format!("{}:{}%", t.pool_id, t.percent)).collect::<Vec<_>>());
            Json(AllocationView { supported: true, targets: Some(req.targets) }).into_response()
        }
        Ok(Err(e)) => err(StatusCode::BAD_GATEWAY, format!("the venue refused the allocation: {e}")),
        Err(_) => err(StatusCode::GATEWAY_TIMEOUT, "the venue did not answer within 20s; check the allocation before retrying"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(from: &str, to: &str, amount: &str) -> TransferRequest {
        TransferRequest { from: from.into(), to: to.into(), amount: amount.into() }
    }

    /// A transfer names two different pools and a positive dollar amount with
    /// at most the venue's four decimal places; anything else is refused before
    /// the venue sees it.
    #[test]
    fn a_transfer_needs_two_pools_and_a_positive_amount() {
        assert_eq!(validate_transfer(&req("0", "2", "30")).unwrap().2, Decimal::from(30));
        assert!(validate_transfer(&req("0", "0", "30")).is_err());
        assert!(validate_transfer(&req("0", "2", "0")).is_err());
        assert!(validate_transfer(&req("0", "2", "-5")).is_err());
        assert!(validate_transfer(&req("0", "2", "abc")).is_err());
        assert!(validate_transfer(&req("0", "2", "1.00001")).is_err());
        assert!(validate_transfer(&req("", "2", "1")).is_err());
    }

    /// The transfer key has the UUID v4 shape venues validate client ids against.
    #[test]
    fn the_idempotency_key_is_a_version_four_uuid() {
        let k = idempotency_key();
        assert_eq!(k.len(), 36);
        assert_eq!(&k[14..15], "4");
        assert!(matches!(&k[19..20], "8" | "9" | "a" | "b"));
        assert_ne!(k, idempotency_key());
    }

    /// An allocation totals 100 with each pool once; an empty one is the way to
    /// turn rebalancing off and is accepted.
    #[test]
    fn an_allocation_totals_one_hundred_or_is_empty() {
        let t = |id: &str, p: u8| AllocationTarget { pool_id: id.into(), percent: p };
        assert!(validate_allocation(&[t("0", 20), t("2", 60), t("3", 20)]).is_ok());
        assert!(validate_allocation(&[]).is_ok());
        assert!(validate_allocation(&[t("0", 50), t("2", 40)]).is_err());
        assert!(validate_allocation(&[t("0", 50), t("0", 50)]).is_err());
        assert!(validate_allocation(&[t("0", 101)]).is_err());
    }
}
