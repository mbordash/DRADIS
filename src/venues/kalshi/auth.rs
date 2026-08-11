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

//! Kalshi API authentication — RSA-PSS request signing.
//!
//! Every REST request and the WebSocket handshake carry three headers:
//!
//! | Header                     | Value                                        |
//! |----------------------------|----------------------------------------------|
//! | `KALSHI-ACCESS-KEY`        | API key id (UUID from account settings)      |
//! | `KALSHI-ACCESS-TIMESTAMP`  | request timestamp in **milliseconds**        |
//! | `KALSHI-ACCESS-SIGNATURE`  | base64( RSA-PSS-SHA256( ts + METHOD + path ))|
//!
//! The signed message is the concatenation of the millisecond timestamp, the
//! uppercase HTTP method, and the request **path without query string** (e.g.
//! `1700000000000GET/trade-api/v2/portfolio/balance`). PSS salt length equals
//! the digest length (32 bytes for SHA-256).
//!
//! Spec: https://docs.kalshi.com/getting_started/api_keys

use anyhow::{Context, Result};
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::SigningKey;
use rsa::sha2::Sha256;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::RsaPrivateKey;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;

/// Signs Kalshi API requests. Cheap to clone via `Arc` upstream; the signing
/// key itself is kept private to this type.
pub struct KalshiAuth {
    key_id: String,
    signing_key: SigningKey<Sha256>,
}

impl std::fmt::Debug for KalshiAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KalshiAuth")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

impl KalshiAuth {
    /// Build from the key id and a PEM-encoded RSA private key.
    ///
    /// Accepts both PKCS#8 (`-----BEGIN PRIVATE KEY-----`, what Kalshi's key
    /// generator downloads) and PKCS#1 (`-----BEGIN RSA PRIVATE KEY-----`).
    pub fn new(key_id: String, private_key_pem: &str) -> Result<Self> {
        let key = RsaPrivateKey::from_pkcs8_pem(private_key_pem)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(private_key_pem))
            .context("Kalshi private key: not valid PKCS#8 or PKCS#1 PEM")?;
        Ok(Self {
            key_id,
            // Salt length = digest length (32) — matches Kalshi's reference
            // implementations (`PSS.DIGEST_LENGTH` / `RSA_PSS_SALTLEN_DIGEST`).
            signing_key: SigningKey::<Sha256>::new(key),
        })
    }

    /// Build from environment: `KALSHI_API_KEY_ID` + one of
    /// `KALSHI_PRIVATE_KEY_PATH` (PEM file) or `KALSHI_PRIVATE_KEY` (inline PEM,
    /// `\n` escapes tolerated).
    pub fn from_env() -> Result<Self> {
        let key_id = std::env::var("KALSHI_API_KEY_ID")
            .context("KALSHI_API_KEY_ID not set")?;
        let pem = if let Ok(path) = std::env::var("KALSHI_PRIVATE_KEY_PATH") {
            std::fs::read_to_string(&path)
                .with_context(|| format!("reading KALSHI_PRIVATE_KEY_PATH={path}"))?
        } else {
            std::env::var("KALSHI_PRIVATE_KEY")
                .context("set KALSHI_PRIVATE_KEY_PATH or KALSHI_PRIVATE_KEY")?
                .replace("\\n", "\n")
        };
        Self::new(key_id, &pem)
    }

    /// Produce the three signed headers for `method` + `path`.
    ///
    /// `path` must start at the API root (e.g. `/trade-api/v2/markets`); any
    /// query string is stripped before signing per spec.
    pub fn signed_headers(&self, method: &str, path: &str) -> [(String, String); 3] {
        let ts_ms = chrono::Utc::now().timestamp_millis();
        self.signed_headers_at(method, path, ts_ms)
    }

    /// Deterministic-timestamp variant (used by tests and the WS handshake,
    /// which needs to know the timestamp it signed).
    pub fn signed_headers_at(
        &self,
        method: &str,
        path: &str,
        ts_ms: i64,
    ) -> [(String, String); 3] {
        let path_no_query = path.split('?').next().unwrap_or(path);
        let msg = format!("{ts_ms}{}{path_no_query}", method.to_uppercase());
        let sig = self
            .signing_key
            .sign_with_rng(&mut rsa::rand_core::OsRng, msg.as_bytes());
        [
            ("KALSHI-ACCESS-KEY".to_string(), self.key_id.clone()),
            ("KALSHI-ACCESS-TIMESTAMP".to_string(), ts_ms.to_string()),
            ("KALSHI-ACCESS-SIGNATURE".to_string(), B64.encode(sig.to_bytes())),
        ]
    }

    /// Key id accessor (used in logs — never log the private key).
    pub fn key_id(&self) -> &str {
        &self.key_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs8::EncodePrivateKey;
    use rsa::pss::VerifyingKey;
    use rsa::signature::Verifier;

    fn test_auth() -> (KalshiAuth, RsaPrivateKey) {
        // 2048-bit keygen is slow in debug builds but fine for a unit test.
        let key = RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).unwrap();
        let pem = key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).unwrap();
        (KalshiAuth::new("test-key-id".into(), &pem).unwrap(), key)
    }

    #[test]
    fn signs_and_verifies_pss() {
        let (auth, key) = test_auth();
        let ts = 1_700_000_000_000i64;
        let headers = auth.signed_headers_at("GET", "/trade-api/v2/portfolio/balance", ts);

        assert_eq!(headers[0], ("KALSHI-ACCESS-KEY".to_string(), "test-key-id".to_string()));
        assert_eq!(headers[1].1, ts.to_string());

        // Verify the signature over the expected message with the public half.
        let msg = format!("{ts}GET/trade-api/v2/portfolio/balance");
        let sig_bytes = B64.decode(&headers[2].1).unwrap();
        let sig = rsa::pss::Signature::try_from(sig_bytes.as_slice()).unwrap();
        let verifier = VerifyingKey::<Sha256>::new(key.to_public_key());
        verifier.verify(msg.as_bytes(), &sig).expect("PSS signature must verify");
    }

    #[test]
    fn strips_query_string_before_signing() {
        let (auth, key) = test_auth();
        let ts = 1_700_000_000_000i64;
        let headers = auth.signed_headers_at(
            "GET",
            "/trade-api/v2/portfolio/orders?limit=5&cursor=abc",
            ts,
        );
        // Must verify against the path WITHOUT the query string.
        let msg = format!("{ts}GET/trade-api/v2/portfolio/orders");
        let sig_bytes = B64.decode(&headers[2].1).unwrap();
        let sig = rsa::pss::Signature::try_from(sig_bytes.as_slice()).unwrap();
        let verifier = VerifyingKey::<Sha256>::new(key.to_public_key());
        verifier.verify(msg.as_bytes(), &sig).expect("query string must be stripped");
    }

    #[test]
    fn uppercases_method() {
        let (auth, key) = test_auth();
        let ts = 42i64;
        let headers = auth.signed_headers_at("post", "/trade-api/v2/portfolio/events/orders", ts);
        let msg = format!("{ts}POST/trade-api/v2/portfolio/events/orders");
        let sig_bytes = B64.decode(&headers[2].1).unwrap();
        let sig = rsa::pss::Signature::try_from(sig_bytes.as_slice()).unwrap();
        let verifier = VerifyingKey::<Sha256>::new(key.to_public_key());
        verifier.verify(msg.as_bytes(), &sig).expect("method must be uppercased");
    }
}
