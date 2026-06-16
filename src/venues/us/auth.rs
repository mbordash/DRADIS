//! Custodial Web2 authentication for the Polymarket US retail venue.
//!
//! The US gateway authenticates **every request** with an Ed25519 signature
//! (there is no JWT/mint exchange). A developer-portal API key issues two values:
//!
//!   * **Key ID** — a UUID identifying the key (`X-PM-Access-Key`).
//!   * **Secret Key** — Base64 of the 64-byte Ed25519 keypair (`seed ‖ public`),
//!     shown once at creation.
//!
//! Each request carries three headers:
//!
//! ```http
//! X-PM-Access-Key: <KEY_ID>
//! X-PM-Timestamp:  <unix_millis>
//! X-PM-Signature:  Base64( Ed25519_sign(secret, <signing-payload>) )
//! ```
//!
//! The gateway checks the timestamp is within its replay window and verifies the
//! signature against the public key on file. Signing is stateless and local, so
//! there is no token to cache or refresh — call sites just call
//! [`UsAuth::signed_headers`] per request.

use anyhow::{anyhow, Context, Result};
use base64::Engine as _;
use ed25519_dalek::{Signer, SigningKey};

/// Environment variable holding the portal **Key ID** (UUID).
pub const ENV_KEY_ID: &str = "POLYMARKET_US_KEY_ID";
/// Environment variable holding the Base64 **Secret Key** (64-byte keypair).
pub const ENV_SECRET_KEY: &str = "POLYMARKET_US_SECRET_KEY";

/// Request header names (per developer-portal usage notes).
pub const HEADER_ACCESS_KEY: &str = "X-PM-Access-Key";
pub const HEADER_TIMESTAMP: &str = "X-PM-Timestamp";
pub const HEADER_SIGNATURE: &str = "X-PM-Signature";

/// Auth state for one US retail session — a key id + the Ed25519 signer.
///
/// Holds no network handle and no mutable token, since each request is signed
/// independently.
pub struct UsAuth {
    key_id: String,
    signing_key: SigningKey,
}

impl UsAuth {
    /// Build auth state from environment credentials.
    pub fn from_env() -> Result<Self> {
        let key_id = std::env::var(ENV_KEY_ID)
            .with_context(|| format!("{ENV_KEY_ID} not set"))?;
        let secret_b64 = std::env::var(ENV_SECRET_KEY)
            .with_context(|| format!("{ENV_SECRET_KEY} not set"))?;
        Self::from_parts(key_id, &secret_b64)
    }

    /// Build auth state from explicit credentials (used by tests).
    pub fn from_parts(key_id: String, secret_b64: &str) -> Result<Self> {
        let secret = base64::engine::general_purpose::STANDARD
            .decode(secret_b64.trim())
            .context("POLYMARKET_US_SECRET_KEY is not valid Base64")?;

        // The portal secret is either:
        // - 64 bytes (full Ed25519 keypair: seed ‖ public), or
        // - 32 bytes (just the seed)
        // Per the official TypeScript SDK, we use the FIRST 32 BYTES (seed) for signing.
        let signing_key = match secret.len() {
            64 => {
                let seed: [u8; 32] = secret[..32].try_into().expect("first 32 bytes");
                SigningKey::from_bytes(&seed)
            }
            32 => {
                let seed: [u8; 32] = secret.as_slice().try_into().expect("len checked == 32");
                SigningKey::from_bytes(&seed)
            }
            n => {
                return Err(anyhow!(
                    "POLYMARKET_US_SECRET_KEY must decode to 64 bytes (keypair) or 32 bytes (seed), got {n}"
                ))
            }
        };

        Ok(Self { key_id, signing_key })
    }

    /// The portal Key ID (`X-PM-Access-Key` header value).
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// Build the canonical payload that gets Ed25519-signed for a request.
    ///
    /// Common signature schemes:
    /// - FTX-style: `timestamp + method.upper() + path` (+ body for POST/PUT)
    /// - AWS SigV4: canonical request hash
    /// - Simple: just `timestamp`
    ///
    /// **Trying FTX-like pattern first**: `timestamp + method + path`
    /// (e.g., `"1750191234567GETv1/account/balance"`). If this fails, we'll need
    /// to check the portal docs or contact support for the exact format.
    fn signing_payload(timestamp_ms: i64, method: &str, path: &str) -> String {
        format!("{}{}{}", timestamp_ms, method.to_uppercase(), path)
    }

    /// Sign a request, returning `(timestamp_ms, base64_signature)`.
    pub fn sign(&self, method: &str, path: &str) -> (i64, String) {
        let ts = chrono::Utc::now().timestamp_millis();
        let payload = Self::signing_payload(ts, method, path);
        let sig_bytes = self.signing_key.sign(payload.as_bytes()).to_bytes();
        let signature = base64::engine::general_purpose::STANDARD.encode(sig_bytes);
        tracing::debug!(
            "🔐 Ed25519 sign: method={} path={} ts={} → payload={:?} → sig={}...",
            method, path, ts, payload, &signature[..16.min(signature.len())]
        );
        (ts, signature)
    }

    /// The three auth headers `(name, value)` for a request.
    pub fn signed_headers(&self, method: &str, path: &str) -> [(&'static str, String); 3] {
        let (ts, sig) = self.sign(method, path);
        [
            (HEADER_ACCESS_KEY, self.key_id.clone()),
            (HEADER_TIMESTAMP, ts.to_string()),
            (HEADER_SIGNATURE, sig),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Verifier, VerifyingKey};

    const SAMPLE_SECRET: &str =
        "lxcsopNhvp+FyZMtVPnHPeHAGihFMPEZcUg6TrJX6kCfwSEXu8v8vmyi3wJbMFUs3a9Fe7mkyRIwfZZkd/5kPg==";

    /// The real portal sample is a 64-byte keypair → must load and sign.
    #[test]
    fn loads_64_byte_keypair_and_signs_verifiably() {
        let auth = UsAuth::from_parts("483074f3-key".into(), SAMPLE_SECRET).unwrap();

        let (ts, sig_b64) = auth.sign("GET", "/v1/account/balance");
        let sig_bytes = base64::engine::general_purpose::STANDARD
            .decode(sig_b64)
            .unwrap();
        let sig = ed25519_dalek::Signature::from_slice(&sig_bytes).unwrap();

        // The signature must verify against the public half of the keypair.
        let raw = base64::engine::general_purpose::STANDARD
            .decode(SAMPLE_SECRET)
            .unwrap();
        let pub_bytes: [u8; 32] = raw[32..64].try_into().unwrap();
        let vk = VerifyingKey::from_bytes(&pub_bytes).unwrap();
        let payload = format!("{}GET/v1/account/balance", ts);
        assert!(vk.verify(payload.as_bytes(), &sig).is_ok());
    }

    #[test]
    fn signed_headers_carry_key_id_and_timestamp() {
        let auth = UsAuth::from_parts("my-key-id".into(), SAMPLE_SECRET).unwrap();
        let headers = auth.signed_headers("POST", "/v1/trading/orders");
        assert_eq!(headers[0].0, HEADER_ACCESS_KEY);
        assert_eq!(headers[0].1, "my-key-id");
        assert_eq!(headers[1].0, HEADER_TIMESTAMP);
        assert!(headers[1].1.parse::<i64>().unwrap() > 0);
        assert_eq!(headers[2].0, HEADER_SIGNATURE);
        assert!(!headers[2].1.is_empty());
    }

    #[test]
    fn rejects_wrong_length_secret() {
        let bad = base64::engine::general_purpose::STANDARD.encode([0u8; 10]);
        assert!(UsAuth::from_parts("k".into(), &bad).is_err());
    }
}

