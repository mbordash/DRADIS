// SPDX-License-Identifier: AGPL-3.0-only
//! Verify the exchange protocol before a token can reach the V2 signer.

use alloy::primitives::{B256, U256};
use anyhow::{ensure, Result};
use serde_json::Value;
use std::{collections::HashSet, str::FromStr, time::Duration};
use tokio::sync::RwLock;

pub struct CtfTokenRegistry {
    http: reqwest::Client,
    host: String,
    verified: RwLock<HashSet<U256>>,
}

impl Default for CtfTokenRegistry {
    fn default() -> Self {
        Self::new(crate::config::CLOB_API_BASE)
    }
}

impl CtfTokenRegistry {
    fn new(host: &str) -> Self {
        Self {
            http: reqwest::Client::new(),
            host: host.trim_end_matches('/').into(),
            verified: RwLock::new(HashSet::new()),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        let mut registry = Self::new("http://127.0.0.1:1");
        registry.verified = RwLock::new([U256::from(1), U256::from(7)].into_iter().collect());
        registry
    }

    pub async fn verify(&self, token: U256) -> Result<()> {
        if self.verified.read().await.contains(&token) {
            return Ok(());
        }
        let response = self
            .http
            .get(format!("{}/book", self.host))
            .query(&[("token_id", token.to_string())])
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("CLOB protocol metadata read failed"))?;
        ensure!(
            response.status().is_success(),
            "CLOB protocol metadata request was rejected"
        );
        let book: Value = response
            .json()
            .await
            .map_err(|_| anyhow::anyhow!("CLOB protocol metadata is malformed"))?;
        validate_ctf_book(&book, token)?;
        self.verified.write().await.insert(token);
        Ok(())
    }
}

fn validate_ctf_book(book: &Value, token: U256) -> Result<()> {
    // Current Protocol V2 books declare version="v2" and require Exchange V3.
    // SDK 0.7's CTF V2 payload cannot sign that domain. Missing metadata fails
    // closed; a versioned book must never fall through to the CTF signer.
    ensure!(
        book.get("version").is_none_or(Value::is_null),
        "This market requires an unsupported exchange protocol"
    );
    let asset = book
        .get("asset_id")
        .and_then(Value::as_str)
        .and_then(|value| U256::from_str(value).ok());
    ensure!(
        asset == Some(token),
        "CLOB book does not identify the requested token"
    );
    let condition = book
        .get("market")
        .and_then(Value::as_str)
        .and_then(|value| B256::from_str(value).ok());
    ensure!(
        condition.is_some_and(|value| !value.is_zero()),
        "CLOB book has no supported CTF condition"
    );
    ensure!(
        book.get("bids").is_some_and(Value::is_array)
            && book.get("asks").is_some_and(Value::is_array),
        "CLOB book metadata is incomplete"
    );
    Ok(())
}

pub fn supported_gamma_ctf_market(market: &Value) -> bool {
    market.get("version").is_none_or(Value::is_null)
        && market
            .get("conditionId")
            .and_then(Value::as_str)
            .and_then(|value| B256::from_str(value).ok())
            .is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Json, Router};
    use serde_json::json;

    fn book() -> Value {
        json!({"asset_id":"42", "market":format!("0x{}", "11".repeat(32)), "bids":[], "asks":[]})
    }

    #[tokio::test]
    async fn unsupported_or_unidentified_books_cannot_enter_the_ctf_signer() {
        for (value, expected) in [
            (book(), true),
            (
                {
                    let mut value = book();
                    value["version"] = json!("v2");
                    value
                },
                false,
            ),
            (
                {
                    let mut value = book();
                    value["asset_id"] = json!("43");
                    value
                },
                false,
            ),
            (json!({"error":"not a book"}), false),
        ] {
            let app = Router::new().route(
                "/book",
                get(move || {
                    let value = value.clone();
                    async move { Json(value) }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let registry =
                CtfTokenRegistry::new(&format!("http://{}", listener.local_addr().unwrap()));
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            assert_eq!(registry.verify(U256::from(42)).await.is_ok(), expected);
            server.abort();
            if expected {
                assert!(registry.verify(U256::from(42)).await.is_ok());
            } else {
                assert!(registry.verify(U256::from(42)).await.is_err());
            }
        }
    }

    #[test]
    fn gamma_protocol_v2_and_unknown_metadata_are_not_ctf_candidates() {
        let mut value = json!({"conditionId": format!("0x{}", "11".repeat(32))});
        assert!(supported_gamma_ctf_market(&value));
        value["conditionId"] = json!(format!("0x{}", "11".repeat(31)));
        assert!(!supported_gamma_ctf_market(&value));
        value["conditionId"] = json!(format!("0x{}", "11".repeat(32)));
        value["version"] = json!("v3");
        assert!(!supported_gamma_ctf_market(&value));
    }
}
