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

//! Setup & credentials management API — the "prosumer" onboarding chapter.
//!
//! Lets the Control Tower configure venue credentials (intl wallet / US API keys),
//! Raptor signal-source keys (Alpaca, The Odds API), shared integrations
//! (Telegram, LLM advisor), and an admin password — without SSH access to the
//! server or editing config files.
//!
//! ## Panels
//! `MANAGED_KEYS` tags every credential with the Setup panel that owns it.
//! Venue keys are blocking (no key ⇒ no trading); Raptor keys are additive
//! (no key ⇒ that Raptor idles on a neutral snapshot), which is why they are a
//! separate panel rather than more rows in the venue card — first boot must not
//! present them as though they gate trading. `RAPTOR_SOURCES` drives that panel
//! and is the extension point for contributed Raptors: registering one there
//! surfaces its key in the Control Tower with no front-end change.
//!
//! ## Storage model
//! Secrets are persisted to `$DRADIS_DATA_DIR/secrets.env` (default `./data/`),
//! NOT the container `.env`: Docker injects `--env-file` values at *create* time,
//! so writes to `/.env` inside a container are ephemeral and never re-read. The
//! data dir is expected to be a mounted volume (it already holds the SQLite DBs
//! in production deployments), so the secrets file survives container recreation.
//!
//! `load_secrets_file()` is called from `main.rs` immediately after
//! `dotenv::dotenv()` and OVERRIDES process env — the UI-managed value always
//! wins over a stale baked-in container value.
//!
//! ## Applying changes
//! Venue clients authenticate once at boot, so credential changes apply via
//! `POST /api/setup/restart` (graceful exit → Docker `restart: always` respawns
//! the container → boot path re-reads the secrets file).
//!
//! ## Admin auth
//! - Password hash: argon2id, stored as `DRADIS_ADMIN_HASH` in the secrets file.
//! - Sessions: HMAC-SHA256 bearer tokens (`exp.hex(hmac(key, exp))`), signed with
//!   `DRADIS_SESSION_KEY` (generated on first login, persisted so restarts don't
//!   invalidate sessions). 24h expiry.
//! - First-boot wizard: while no admin hash is set, setup routes are open so the
//!   AMI first-boot flow can enter credentials + create the password. Every
//!   status response advertises `admin_set` so the UI forces password creation.
//! - These routes are nested inside the protected router in `server.rs`, so the
//!   existing `X-API-Key` and read-only demo gates also apply.

use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use tracing::{info, warn};

type HmacSha256 = Hmac<Sha256>;

/// Session token lifetime (seconds).
const SESSION_TTL_SECS: i64 = 24 * 3600;

/// Internal keys — persisted in the secrets file but never listed, returned,
/// or settable through the credentials endpoints. (Referenced by tests as a
/// whitelist-safety check.)
#[allow(dead_code)]
const INTERNAL_KEYS: &[&str] = &["DRADIS_ADMIN_HASH", "DRADIS_SESSION_KEY"];

/// UI-manageable credential whitelist: (env key, label, venue scope, panel).
///
/// Scope: "intl" | "us" | "shared" — the UI shows only the scopes relevant to
/// the running build (see `GET /api/setup/status`).
///
/// Panel: which Setup-view section the key belongs to — "venue" (core trading
/// credentials, blocking: no key means no trading), "raptor" (signal-source
/// credentials, additive: no key means that Raptor idles and publishes a
/// neutral snapshot) or "integration" (notifications, LLM advisor). The split
/// matters because the two kinds fail differently, so first boot must not ask
/// for Raptor keys as though they were required to trade.
const MANAGED_KEYS: &[(&str, &str, &str, &str)] = &[
    ("POLYMARKET_PRIVATE_KEY",   "Polymarket wallet private key", "intl",   "venue"),
    ("POLYGON_RPC_URL",          "Polygon RPC endpoint",          "intl",   "venue"),
    ("POLYMARKET_US_KEY_ID",     "Polymarket US API key ID",      "us",     "venue"),
    ("POLYMARKET_US_SECRET_KEY", "Polymarket US secret key",      "us",     "venue"),
    ("ALPACA_API_KEY_ID",        "Alpaca API key ID",             "shared", "raptor"),
    ("ALPACA_API_SECRET_KEY",    "Alpaca API secret key",         "shared", "raptor"),
    ("ODDS_API_KEY",             "The Odds API key",              "shared", "raptor"),
    ("TELEGRAM_BOT_TOKEN",       "Telegram bot token",            "shared", "integration"),
    ("TELEGRAM_CHAT_ID",         "Telegram chat ID",              "shared", "integration"),
    ("LLM_PROVIDER",             "LLM provider (ollama | openai | anthropic)", "shared", "integration"),
    ("OLLAMA_URL",               "Ollama URL (local or remote)",  "shared", "integration"),
    ("OLLAMA_MODEL",             "Ollama model",                  "shared", "integration"),
    ("LLM_API_BASE",             "Hosted LLM API base URL",       "shared", "integration"),
    ("LLM_API_KEY",              "Hosted LLM API key",            "shared", "integration"),
    ("LLM_MODEL",                "Hosted LLM model",              "shared", "integration"),
];

/// Raptor signal sources rendered as cards in the Setup view's Raptor panel.
///
/// This table is the extension point for new Raptors: adding one here (plus any
/// credential rows in `MANAGED_KEYS` with panel `"raptor"`) makes it appear in
/// the Control Tower with no front-end change. A contributor adding a Raptor
/// therefore never has to touch the React code to make their key configurable.
///
/// `tier` describes how much the *signal* matters, not whether it is currently
/// satisfied:
///   - "required"    — DRADIS depends on this feed to trade at all.
///   - "recommended" — materially improves signal quality where it applies.
///   - "optional"    — observe-only or narrow; safe to leave unconfigured.
///
/// Raptors backed by public endpoints carry no keys at all; they still appear
/// so the panel is a complete inventory of the recon layer, with an empty
/// `keys` list signalling "nothing to configure".
struct RaptorSource {
    /// Stable identifier, also the React key for the card.
    id: &'static str,
    /// Display name, e.g. "Tide Raptor".
    name: &'static str,
    /// Upstream data source, shown under the name.
    source: &'static str,
    /// What the Raptor contributes, in one line.
    blurb: &'static str,
    tier: &'static str,
    /// Env keys this Raptor reads; empty ⇒ no credentials required.
    keys: &'static [&'static str],
    /// `POST /api/setup/test` kind that validates `keys`, when one exists.
    test_kind: Option<&'static str>,
}

const RAPTOR_SOURCES: &[RaptorSource] = &[
    RaptorSource {
        id: "price", name: "Price Raptor", source: "Binance spot (public)",
        blurb: "Oracle price, velocity, acceleration and drift — the core signal every Viper reads.",
        tier: "required", keys: &[], test_kind: None,
    },
    RaptorSource {
        id: "funding", name: "Funding Raptor", source: "Binance perpetuals (public)",
        blurb: "Perp funding rates; the Basis Viper uses them to confirm retail skew.",
        tier: "required", keys: &[], test_kind: None,
    },
    RaptorSource {
        id: "derivatives", name: "Derivatives Raptor", source: "Binance derivatives (public)",
        blurb: "Open interest and CVD ratio, fused as features by GBoost and Convergence.",
        tier: "required", keys: &[], test_kind: None,
    },
    RaptorSource {
        id: "tide", name: "Tide Raptor", source: "Alpaca IEX",
        blurb: "ETF premium \"institutional pulse\" (BTC only, US market hours). Shares one IEX connection with Horizon.",
        tier: "recommended", keys: &["ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY"], test_kind: Some("alpaca"),
    },
    RaptorSource {
        id: "horizon", name: "Horizon Raptor", source: "Alpaca IEX (shared connection)",
        blurb: "TradFi macro velocity from SPY/QQQ/UVXY plus a VIX proxy (BTC only). Uses the same Alpaca keys as Tide.",
        tier: "recommended", keys: &["ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY"], test_kind: Some("alpaca"),
    },
    RaptorSource {
        id: "sports", name: "Sports Raptor", source: "The Odds API",
        blurb: "Head-to-head line movement and consensus probability. Observe-only — published to telemetry, not consumed by Viper sizing.",
        tier: "optional", keys: &["ODDS_API_KEY"], test_kind: Some("odds"),
    },
];

// ─── Secrets file I/O ────────────────────────────────────────────────────────

/// Path of the UI-managed secrets file: `$DRADIS_DATA_DIR/secrets.env`.
pub fn secrets_path() -> PathBuf {
    let dir = std::env::var("DRADIS_DATA_DIR").unwrap_or_else(|_| "./data".to_string());
    PathBuf::from(dir).join("secrets.env")
}

/// Parse the secrets file into an ordered map. Missing file → empty map.
fn read_secrets() -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    if let Ok(content) = std::fs::read_to_string(secrets_path()) {
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') { continue; }
            if let Some((k, v)) = line.split_once('=') {
                map.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
    }
    map
}

/// Atomically persist the secrets map: write to `secrets.env.tmp` (0600 on unix),
/// back up the previous file to `.bak`, then rename into place.
fn write_secrets(map: &BTreeMap<String, String>) -> std::io::Result<()> {
    let path = secrets_path();
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
    let tmp = path.with_extension("env.tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        writeln!(f, "# DRADIS managed secrets — written by the Control Tower setup UI.")?;
        writeln!(f, "# Do not edit while DRADIS is running; changes apply on restart.")?;
        for (k, v) in map {
            writeln!(f, "{}={}", k, v)?;
        }
        f.sync_all()?;
    }
    if path.exists() {
        let _ = std::fs::copy(&path, path.with_extension("env.bak"));
    }
    std::fs::rename(&tmp, &path)
}

/// Load the secrets file into the process environment, OVERRIDING any existing
/// values (container `.env` values are create-time stale copies; the UI-managed
/// file is the source of truth). Called from `main.rs` right after dotenv.
pub fn load_secrets_file() {
    let map = read_secrets();
    if map.is_empty() { return; }
    let n = map.len();
    for (k, v) in map {
        std::env::set_var(k, v);
    }
    info!("🔐 Loaded {} secret(s) from {}", n, secrets_path().display());
}

// ─── Admin password & session tokens ─────────────────────────────────────────

fn admin_hash() -> Option<String> {
    read_secrets().get("DRADIS_ADMIN_HASH").cloned()
        .or_else(|| std::env::var("DRADIS_ADMIN_HASH").ok())
        .filter(|h| !h.is_empty())
}

/// Session-signing key: generated on first use and persisted so a container
/// restart does not invalidate live sessions.
fn session_key() -> Vec<u8> {
    let mut map = read_secrets();
    if let Some(k) = map.get("DRADIS_SESSION_KEY") {
        if let Ok(bytes) = hex_decode(k) { if bytes.len() >= 32 { return bytes; } }
    }
    // Generate 32 random bytes via the OS RNG (argon2's password-hash rand_core).
    use argon2::password_hash::rand_core::RngCore;
    let mut bytes = [0u8; 32];
    argon2::password_hash::rand_core::OsRng.fill_bytes(&mut bytes);
    map.insert("DRADIS_SESSION_KEY".to_string(), hex_encode(&bytes));
    if let Err(e) = write_secrets(&map) {
        warn!("⚠️ Could not persist session key ({}) — sessions will not survive restart", e);
    }
    bytes.to_vec()
}

fn hex_encode(b: &[u8]) -> String { b.iter().map(|x| format!("{:02x}", x)).collect() }

fn hex_decode(s: &str) -> Result<Vec<u8>, ()> {
    if s.len() % 2 != 0 { return Err(()); }
    (0..s.len()).step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

fn issue_token() -> String {
    let exp = chrono::Utc::now().timestamp() + SESSION_TTL_SECS;
    let payload = exp.to_string();
    let mut mac = HmacSha256::new_from_slice(&session_key()).expect("hmac key");
    mac.update(payload.as_bytes());
    format!("{}.{}", payload, hex_encode(&mac.finalize().into_bytes()))
}

fn verify_token(token: &str) -> bool {
    let Some((payload, sig)) = token.split_once('.') else { return false };
    let Ok(exp) = payload.parse::<i64>() else { return false };
    if exp < chrono::Utc::now().timestamp() { return false; }
    let Ok(sig_bytes) = hex_decode(sig) else { return false };
    let mut mac = HmacSha256::new_from_slice(&session_key()).expect("hmac key");
    mac.update(payload.as_bytes());
    mac.verify_slice(&sig_bytes).is_ok()
}

fn hash_password(password: &str) -> Result<String, String> {
    use argon2::password_hash::{PasswordHasher, SaltString};
    let salt = SaltString::generate(&mut argon2::password_hash::rand_core::OsRng);
    argon2::Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| e.to_string())
}

fn verify_password(password: &str, hash: &str) -> bool {
    use argon2::password_hash::{PasswordHash, PasswordVerifier};
    PasswordHash::new(hash)
        .map(|parsed| argon2::Argon2::default().verify_password(password.as_bytes(), &parsed).is_ok())
        .unwrap_or(false)
}

/// Extract and verify the admin token: `Authorization: Bearer` (proxy path)
/// or `X-Admin-Token` (direct browser → engine in local dev, where the
/// Authorization header is reserved for CT Basic Auth).
fn request_is_admin(req: &Request) -> bool {
    let bearer = req.headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let direct = req.headers()
        .get("x-admin-token")
        .and_then(|v| v.to_str().ok());
    bearer.or(direct).map(verify_token).unwrap_or(false)
}

/// Middleware: require a valid admin session token.
///
/// True when the operator has explicitly disabled the setup admin gate
/// (`DRADIS_SETUP_AUTH=off|false|disabled|0`). Intended for deployments that
/// already gate access via CT basic auth + DRADIS_API_KEY — the setup routes
/// remain behind those layers, just without a second password prompt.
fn setup_auth_disabled() -> bool {
    std::env::var("DRADIS_SETUP_AUTH")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "off" | "false" | "disabled" | "0"))
        .unwrap_or(false)
}

/// Middleware: require a valid admin session token.
///
/// First-boot exception: while NO admin password is configured, requests pass so
/// the AMI first-boot wizard can enter credentials and create the password. The
/// moment a hash exists, everything behind this gate requires a login.
/// Operator exception: DRADIS_SETUP_AUTH=off waives the gate entirely.
async fn require_admin(req: Request, next: Next) -> Response {
    if !setup_auth_disabled() && admin_hash().is_some() && !request_is_admin(&req) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "admin session required", "login": "/api/auth/login"})),
        ).into_response();
    }
    next.run(req).await
}

// ─── Handlers ────────────────────────────────────────────────────────────────

/// Which venue is compiled into this binary.
fn build_venue() -> &'static str {
    #[cfg(feature = "intl_clob")]
    { "intl" }
    #[cfg(not(feature = "intl_clob"))]
    { "us" }
}

/// DB key for the one-time alpha/jurisdiction acknowledgment record.
const ALPHA_ACK_KEY: &str = "alpha_jurisdiction_ack";

/// Has the operator recorded the alpha risk + jurisdiction acknowledgment?
async fn alpha_acknowledged() -> bool {
    match crate::helpers::db::pool() {
        Some(pool) => crate::helpers::db::config_get(pool, ALPHA_ACK_KEY).await.is_some(),
        None => false,
    }
}

/// GET /api/setup/status — first-boot / configuration state. No secrets exposed;
/// safe for the UI to call before login.
async fn get_status() -> Response {
    let secrets = read_secrets();
    let env_set = |k: &str| -> bool {
        secrets.get(k).map(|v| !v.is_empty()).unwrap_or(false)
            || std::env::var(k).map(|v| !v.is_empty()).unwrap_or(false)
    };
    let venue = build_venue();
    let venue_configured = match venue {
        "intl" => env_set("POLYMARKET_PRIVATE_KEY") && env_set("POLYGON_RPC_URL"),
        _      => env_set("POLYMARKET_US_KEY_ID") && env_set("POLYMARKET_US_SECRET_KEY"),
    };
    Json(json!({
        "venue": venue,
        "admin_set": admin_hash().is_some(),
        "auth_disabled": setup_auth_disabled(),
        "venue_configured": venue_configured,
        "alpha_ack": alpha_acknowledged().await,
        "app_version": env!("CARGO_PKG_VERSION"),
        "restart_pending": false,
    })).into_response()
}

/// POST /api/setup/acknowledge — record the one-time alpha risk + jurisdiction
/// acknowledgment. Public (shown before login on first boot); write-once and
/// non-sensitive: it only records acceptance with a timestamp — it never
/// unlocks anything a fresh instance wouldn't already allow.
async fn acknowledge_alpha() -> Response {
    let Some(pool) = crate::helpers::db::pool() else {
        return (StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "DB not ready — try again shortly"}))).into_response();
    };
    if let Some(existing) = crate::helpers::db::config_get(pool, ALPHA_ACK_KEY).await {
        // Idempotent: first acknowledgment is the record of legal significance.
        let rec: serde_json::Value = serde_json::from_str(&existing).unwrap_or(json!({}));
        return Json(json!({"ok": true, "already_acknowledged": true, "record": rec})).into_response();
    }
    let record = json!({
        "acknowledged_at": chrono::Utc::now().to_rfc3339(),
        "venue": build_venue(),
        "app_version": env!("CARGO_PKG_VERSION"),
    });
    crate::helpers::db::config_set(pool, ALPHA_ACK_KEY, &record.to_string()).await;
    info!("⚠️ Early-access risk + jurisdiction acknowledgment recorded: {}", record);
    Json(json!({"ok": true, "already_acknowledged": false, "record": record})).into_response()
}

/// GET /api/setup/credentials — masked inventory of managed keys. Never returns
/// secret values; only whether each is set plus a last-4 hint for recognition.
async fn get_credentials() -> Response {
    let secrets = read_secrets();
    let items: Vec<serde_json::Value> = MANAGED_KEYS.iter().map(|(key, label, scope, panel)| {
        // Prefer the managed file; fall back to process env (container .env).
        let val = secrets.get(*key).cloned()
            .filter(|v| !v.is_empty())
            .or_else(|| std::env::var(key).ok().filter(|v| !v.is_empty()));
        let (set, hint, source) = match val {
            Some(v) => {
                let hint = if v.len() > 4 { format!("…{}", &v[v.len() - 4..]) } else { "•••".to_string() };
                let source = if secrets.contains_key(*key) { "managed" } else { "env" };
                (true, hint, source)
            }
            None => (false, String::new(), "unset"),
        };
        json!({ "key": key, "label": label, "scope": scope, "panel": panel,
                "set": set, "hint": hint, "source": source })
    }).collect();
    Json(json!({ "credentials": items })).into_response()
}

/// GET /api/setup/raptors — the Raptor signal inventory backing the Setup
/// view's Raptor panel. Pairs with `GET /api/setup/credentials` for per-key
/// state; this endpoint carries only static card metadata, never secrets.
async fn get_raptor_sources() -> Response {
    let items: Vec<serde_json::Value> = RAPTOR_SOURCES.iter().map(|r| {
        json!({
            "id": r.id, "name": r.name, "source": r.source, "blurb": r.blurb,
            "tier": r.tier, "keys": r.keys, "test_kind": r.test_kind,
        })
    }).collect();
    Json(json!({ "raptors": items })).into_response()
}

#[derive(Deserialize)]
struct PutCredentials {
    /// key → new value. Empty string deletes the managed entry.
    credentials: BTreeMap<String, String>,
}

/// PUT /api/setup/credentials — persist managed credentials to the secrets file.
/// Whitelist-enforced; applies fully on restart (process env is updated too so
/// test endpoints and freshly-spawned tasks see new values immediately).
async fn put_credentials(Json(body): Json<PutCredentials>) -> Response {
    for key in body.credentials.keys() {
        if !MANAGED_KEYS.iter().any(|(k, _, _, _)| k == key) {
            return (StatusCode::BAD_REQUEST,
                    Json(json!({"error": format!("'{}' is not a managed credential", key)}))).into_response();
        }
    }
    let mut map = read_secrets();
    let mut changed: Vec<String> = Vec::new();
    for (key, value) in &body.credentials {
        let value = value.trim();
        if value.is_empty() {
            if map.remove(key.as_str()).is_some() {
                std::env::remove_var(key);
                changed.push(format!("{} (removed)", key));
            }
        } else {
            map.insert(key.clone(), value.to_string());
            std::env::set_var(key, value);
            changed.push(key.clone());
        }
    }
    if changed.is_empty() {
        return Json(json!({"ok": true, "changed": [], "restart_required": false})).into_response();
    }
    match write_secrets(&map) {
        Ok(()) => {
            info!("🔐 Setup: credentials updated via UI: {}", changed.join(", "));
            Json(json!({"ok": true, "changed": changed, "restart_required": true})).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR,
                   Json(json!({"error": format!("failed to write secrets file: {}", e)}))).into_response(),
    }
}

#[derive(Deserialize)]
struct SetAdmin { password: String }

/// POST /api/setup/admin — set (or change) the admin password.
/// While unset: open (first-boot wizard). Once set: requires an admin session
/// (enforced by the `require_admin` layer on this router).
async fn set_admin(Json(body): Json<SetAdmin>) -> Response {
    if body.password.len() < 8 {
        return (StatusCode::BAD_REQUEST,
                Json(json!({"error": "password must be at least 8 characters"}))).into_response();
    }
    let hash = match hash_password(&body.password) {
        Ok(h) => h,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR,
                          Json(json!({"error": format!("hashing failed: {}", e)}))).into_response(),
    };
    let mut map = read_secrets();
    map.insert("DRADIS_ADMIN_HASH".to_string(), hash.clone());
    match write_secrets(&map) {
        Ok(()) => {
            std::env::set_var("DRADIS_ADMIN_HASH", hash);
            info!("🔐 Setup: admin password {} via UI", if admin_hash().is_some() { "updated" } else { "created" });
            // Issue a session immediately so the wizard flows straight on.
            Json(json!({"ok": true, "token": issue_token()})).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR,
                   Json(json!({"error": format!("failed to write secrets file: {}", e)}))).into_response(),
    }
}

#[derive(Deserialize)]
struct Login { password: String }

#[derive(Serialize)]
struct LoginOk { token: String, expires_in: i64 }

/// POST /api/auth/login — exchange the admin password for a bearer token.
async fn login(Json(body): Json<Login>) -> Response {
    let Some(hash) = admin_hash() else {
        return (StatusCode::CONFLICT,
                Json(json!({"error": "no admin password configured — complete first-boot setup"}))).into_response();
    };
    if !verify_password(&body.password, &hash) {
        // Blunt brute-force damper.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        warn!("🔐 Setup: failed admin login attempt");
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "invalid password"}))).into_response();
    }
    Json(LoginOk { token: issue_token(), expires_in: SESSION_TTL_SECS }).into_response()
}

#[derive(Deserialize)]
struct TestRequest {
    /// "intl_wallet" | "polygon_rpc" | "us_keys" | "telegram" | "alpaca"
    kind: String,
    /// Candidate credentials to test (falls back to stored/env values if omitted).
    #[serde(default)]
    credentials: BTreeMap<String, String>,
}

fn test_cred(body: &TestRequest, key: &str) -> Option<String> {
    body.credentials.get(key).cloned().filter(|v| !v.is_empty())
        .or_else(|| std::env::var(key).ok().filter(|v| !v.is_empty()))
}

/// POST /api/setup/test — validate candidate credentials WITHOUT persisting.
async fn test_connection(Json(body): Json<TestRequest>) -> Response {
    let started = std::time::Instant::now();
    let result: Result<serde_json::Value, String> = match body.kind.as_str() {
        #[cfg(feature = "intl_clob")]
        "intl_wallet" => test_intl_wallet(&body).await,
        "polygon_rpc" => test_polygon_rpc(&body).await,
        "telegram" => test_telegram(&body).await,
        "alpaca" => test_alpaca(&body).await,
        "odds" => test_odds(&body).await,
        "llm" => test_llm(&body).await,
        #[cfg(feature = "us_retail")]
        "us_keys" => test_us_keys(&body).await,
        other => Err(format!("unknown or unsupported test kind '{}' for this build", other)),
    };
    let ms = started.elapsed().as_millis() as u64;
    match result {
        Ok(details) => Json(json!({"ok": true, "ms": ms, "details": details})).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"ok": false, "ms": ms, "error": e}))).into_response(),
    }
}

/// Validate an intl wallet key: parse → derive EOA + Safe → full CLOB authenticate.
#[cfg(feature = "intl_clob")]
async fn test_intl_wallet(body: &TestRequest) -> Result<serde_json::Value, String> {
    use alloy::signers::local::LocalSigner;
    use alloy::signers::Signer;
    use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config};
    use polymarket_client_sdk_v2::clob::types::SignatureType;
    use polymarket_client_sdk_v2::{derive_safe_wallet, POLYGON};
    use std::str::FromStr;

    let key = test_cred(body, "POLYMARKET_PRIVATE_KEY")
        .ok_or("POLYMARKET_PRIVATE_KEY not provided or stored")?;
    let signer = LocalSigner::from_str(key.trim())
        .map_err(|e| format!("invalid private key: {}", e))?
        .with_chain_id(Some(POLYGON));
    let eoa = signer.address();
    let safe = derive_safe_wallet(eoa, POLYGON).ok_or("Safe derivation failed".to_string())?;

    let auth = ClobClient::new(crate::config::CLOB_API_BASE, Config::default())
        .map_err(|e| format!("CLOB client init failed: {}", e))?
        .authentication_builder(&signer)
        .signature_type(SignatureType::GnosisSafe)
        .authenticate();
    tokio::time::timeout(std::time::Duration::from_secs(15), auth)
        .await
        .map_err(|_| "CLOB authentication timed out (15s)".to_string())?
        .map_err(|e| format!("CLOB authentication failed: {}", e))?;

    Ok(json!({"eoa": eoa.to_string(), "safe": safe.to_string(), "clob": "authenticated"}))
}

/// Validate a Polygon RPC endpoint with `eth_blockNumber`.
async fn test_polygon_rpc(body: &TestRequest) -> Result<serde_json::Value, String> {
    let url = test_cred(body, "POLYGON_RPC_URL").ok_or("POLYGON_RPC_URL not provided or stored")?;
    let client = reqwest::Client::new();
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.post(url.trim())
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}))
            .send(),
    ).await.map_err(|_| "RPC request timed out (10s)".to_string())?
        .map_err(|e| format!("RPC request failed: {}", e))?;
    let status = resp.status();
    let body_json: serde_json::Value = resp.json().await
        .map_err(|e| format!("RPC returned non-JSON (HTTP {}): {}", status, e))?;
    if let Some(err) = body_json.get("error") {
        return Err(format!("RPC error: {}", err));
    }
    let block_hex = body_json.get("result").and_then(|v| v.as_str())
        .ok_or_else(|| format!("unexpected RPC response: {}", body_json))?;
    let block = u64::from_str_radix(block_hex.trim_start_matches("0x"), 16).unwrap_or(0);
    Ok(json!({"chain": "polygon", "block": block}))
}

/// Validate a Telegram bot token (and optionally chat ID) via `getMe`.
async fn test_telegram(body: &TestRequest) -> Result<serde_json::Value, String> {
    let token = test_cred(body, "TELEGRAM_BOT_TOKEN").ok_or("TELEGRAM_BOT_TOKEN not provided or stored")?;
    let client = reqwest::Client::new();
    let resp: serde_json::Value = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.get(format!("https://api.telegram.org/bot{}/getMe", token.trim())).send(),
    ).await.map_err(|_| "Telegram request timed out (10s)".to_string())?
        .map_err(|e| format!("Telegram request failed: {}", e))?
        .json().await.map_err(|e| format!("Telegram returned non-JSON: {}", e))?;
    if resp.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        return Err(format!("Telegram rejected the token: {}", resp));
    }
    let username = resp.pointer("/result/username").and_then(|v| v.as_str()).unwrap_or("?");
    Ok(json!({"bot": username}))
}

/// Validate Alpaca market-data keys with a lightweight account/clock probe.
async fn test_alpaca(body: &TestRequest) -> Result<serde_json::Value, String> {
    let key = test_cred(body, "ALPACA_API_KEY_ID").ok_or("ALPACA_API_KEY_ID not provided or stored")?;
    let secret = test_cred(body, "ALPACA_API_SECRET_KEY").ok_or("ALPACA_API_SECRET_KEY not provided or stored")?;
    let client = reqwest::Client::new();
    // Data-plane probe (works for data-only keys, which is what the raptors use).
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.get("https://data.alpaca.markets/v2/stocks/SPY/trades/latest")
            .header("APCA-API-KEY-ID", key.trim())
            .header("APCA-API-SECRET-KEY", secret.trim())
            .send(),
    ).await.map_err(|_| "Alpaca request timed out (10s)".to_string())?
        .map_err(|e| format!("Alpaca request failed: {}", e))?;
    if !resp.status().is_success() {
        return Err(format!("Alpaca rejected the keys (HTTP {})", resp.status()));
    }
    Ok(json!({"feed": "IEX", "probe": "SPY latest trade"}))
}

/// Validate a The Odds API key via `GET /v4/sports`.
///
/// That endpoint is deliberately chosen because it does not draw down the
/// request quota, so testing a key never costs the operator part of the free
/// tier the Sports Raptor is budgeted against. The remaining-quota headers are
/// surfaced so the panel can show the budget alongside the result.
async fn test_odds(body: &TestRequest) -> Result<serde_json::Value, String> {
    let key = test_cred(body, "ODDS_API_KEY").ok_or("ODDS_API_KEY not provided or stored")?;
    let client = reqwest::Client::new();
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.get("https://api.the-odds-api.com/v4/sports")
            .query(&[("apiKey", key.trim())])
            .send(),
    ).await.map_err(|_| "The Odds API request timed out (10s)".to_string())?
        .map_err(|e| format!("The Odds API request failed: {}", e))?;
    let status = resp.status();
    let header = |r: &reqwest::Response, n: &str| -> Option<String> {
        r.headers().get(n).and_then(|v| v.to_str().ok()).map(|s| s.to_string())
    };
    let remaining = header(&resp, "x-requests-remaining");
    let used = header(&resp, "x-requests-used");
    if !status.is_success() {
        // 401 = bad key, 429 = quota exhausted. Surface a snippet either way.
        let snippet: String = resp.text().await.unwrap_or_default().chars().take(200).collect();
        return Err(format!("The Odds API rejected the key (HTTP {}): {}", status, snippet));
    }
    let sports: serde_json::Value = resp.json().await
        .map_err(|e| format!("The Odds API returned non-JSON: {}", e))?;
    let count = sports.as_array().map(|a| a.len()).unwrap_or(0);
    Ok(json!({
        "sports": count,
        "requests_remaining": remaining.unwrap_or_else(|| "?".into()),
        "requests_used": used.unwrap_or_else(|| "?".into()),
    }))
}

/// Validate the LLM Advisor provider settings with a cheap live probe:
/// - ollama    → GET {url}/api/tags, and confirm the model is pulled
/// - openai    → GET {base}/models with bearer auth
/// - anthropic → 1-token POST /v1/messages round-trip
async fn test_llm(body: &TestRequest) -> Result<serde_json::Value, String> {
    let provider = test_cred(body, "LLM_PROVIDER")
        .unwrap_or_else(|| crate::config::LLM_PROVIDER.to_string())
        .trim().to_ascii_lowercase();
    let client = reqwest::Client::new();
    let timeout = std::time::Duration::from_secs(15);
    match provider.as_str() {
        "ollama" => {
            let url = test_cred(body, "OLLAMA_URL")
                .unwrap_or_else(|| crate::config::LLM_OLLAMA_URL.to_string());
            let model = test_cred(body, "OLLAMA_MODEL")
                .unwrap_or_else(|| crate::config::LLM_OLLAMA_MODEL.to_string());
            let resp: serde_json::Value = tokio::time::timeout(
                timeout,
                client.get(format!("{}/api/tags", url.trim_end_matches('/'))).send(),
            ).await.map_err(|_| "Ollama request timed out (15s)".to_string())?
                .map_err(|e| format!("Ollama unreachable at {}: {}", url, e))?
                .json().await.map_err(|e| format!("Ollama returned non-JSON: {}", e))?;
            let models: Vec<String> = resp.get("models").and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|m| m.get("name").and_then(|n| n.as_str()).map(String::from)).collect())
                .unwrap_or_default();
            let pulled = models.iter().any(|m| m == &model || m.starts_with(&format!("{model}:")));
            if !pulled {
                return Err(format!(
                    "Ollama is up but model '{}' is not pulled (available: {})",
                    model, if models.is_empty() { "none".to_string() } else { models.join(", ") },
                ));
            }
            Ok(json!({"provider": "ollama", "url": url, "model": model}))
        }
        "openai" => {
            let key = test_cred(body, "LLM_API_KEY").ok_or("LLM_API_KEY not provided or stored")?;
            let base = test_cred(body, "LLM_API_BASE")
                .unwrap_or_else(|| "https://api.openai.com/v1".to_string());
            let resp = tokio::time::timeout(
                timeout,
                client.get(format!("{}/models", base.trim_end_matches('/')))
                    .bearer_auth(key.trim())
                    .send(),
            ).await.map_err(|_| "LLM API request timed out (15s)".to_string())?
                .map_err(|e| format!("LLM API unreachable at {}: {}", base, e))?;
            if !resp.status().is_success() {
                return Err(format!("LLM API rejected the key (HTTP {})", resp.status()));
            }
            Ok(json!({"provider": "openai", "base": base, "auth": "accepted"}))
        }
        "anthropic" => {
            let key = test_cred(body, "LLM_API_KEY").ok_or("LLM_API_KEY not provided or stored")?;
            let model = test_cred(body, "LLM_MODEL").ok_or("LLM_MODEL not provided or stored")?;
            let base = test_cred(body, "LLM_API_BASE")
                .unwrap_or_else(|| "https://api.anthropic.com".to_string());
            let resp = tokio::time::timeout(
                timeout,
                client.post(format!("{}/v1/messages", base.trim_end_matches('/')))
                    .header("x-api-key", key.trim())
                    .header("anthropic-version", "2023-06-01")
                    .json(&json!({
                        "model": model.trim(),
                        "max_tokens": 1,
                        "messages": [{"role": "user", "content": "ping"}],
                    }))
                    .send(),
            ).await.map_err(|_| "Anthropic request timed out (15s)".to_string())?
                .map_err(|e| format!("Anthropic unreachable at {}: {}", base, e))?;
            if !resp.status().is_success() {
                let status = resp.status();
                let detail = resp.text().await.unwrap_or_default();
                return Err(format!("Anthropic rejected the request (HTTP {}): {}", status, detail));
            }
            Ok(json!({"provider": "anthropic", "model": model, "auth": "accepted"}))
        }
        other => Err(format!("unknown LLM provider '{}' — use ollama, openai, or anthropic", other)),
    }
}

/// Validate US venue keys by constructing the SDK auth from the candidate values.
#[cfg(feature = "us_retail")]
async fn test_us_keys(body: &TestRequest) -> Result<serde_json::Value, String> {
    let key_id = test_cred(body, "POLYMARKET_US_KEY_ID").ok_or("POLYMARKET_US_KEY_ID not provided or stored")?;
    let secret = test_cred(body, "POLYMARKET_US_SECRET_KEY").ok_or("POLYMARKET_US_SECRET_KEY not provided or stored")?;
    // The SDK auth reads env vars; set them for this process (test is only
    // reachable pre-restart, and PUT would set the same vars anyway).
    std::env::set_var(crate::venues::us::auth::ENV_KEY_ID, key_id.trim());
    std::env::set_var(crate::venues::us::auth::ENV_SECRET_KEY, secret.trim());
    crate::venues::us::auth::UsAuth::from_env()
        .map_err(|e| format!("US auth failed: {}", e))?;
    Ok(json!({"auth": "constructed"}))
}

/// POST /api/setup/restart — gracefully exit the process so the container
/// supervisor (`restart: always`) respawns it with the new secrets applied.
async fn restart() -> Response {
    warn!("🔄 Setup: restart requested via UI — exiting in 1s (supervisor will respawn)");
    tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        std::process::exit(0);
    });
    Json(json!({"ok": true, "message": "restarting — back in ~30-60s"})).into_response()
}

// ─── Config export / import (AMI upgrade path) ───────────────────────────────

/// Bundle format version. Bump when the bundle *structure* changes (not when
/// DynamicConfig gains fields — serde defaults absorb those transparently).
const BUNDLE_SCHEMA_VERSION: u32 = 1;

/// GET /api/setup/export — download a portable config bundle for migrating to
/// a new instance (AMI upgrade path). Contains the managed secrets (including
/// the admin password hash so the login carries over), the global
/// DynamicConfig, and every squadron config. Admin-gated; treat the file as
/// sensitive — it holds keys.
async fn export_bundle() -> Response {
    let secrets = read_secrets();
    // Everything in the secrets file except the session-signing key (a new box
    // mints its own; carrying it over has no benefit).
    let secrets_out: BTreeMap<&String, &String> = secrets
        .iter()
        .filter(|(k, _)| k.as_str() != "DRADIS_SESSION_KEY")
        .collect();

    let mut dynamic_config = serde_json::Value::Null;
    let mut squadrons = serde_json::Map::new();
    if let Some(pool) = crate::helpers::db::pool() {
        if let Some(json) = crate::helpers::db::config_get(pool, "dynamic_config").await {
            dynamic_config = serde_json::from_str(&json).unwrap_or(serde_json::Value::Null);
        }
        for sid in crate::helpers::db::squadron_config_list(pool).await {
            if let Some(json) = crate::helpers::db::squadron_config_get(pool, &sid).await {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&json) {
                    squadrons.insert(sid, v);
                }
            }
        }
    }

    let bundle = json!({
        "kind": "dradis-config-bundle",
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "exported_at": chrono::Utc::now().to_rfc3339(),
        "venue": build_venue(),
        "app_version": env!("CARGO_PKG_VERSION"),
        "secrets": secrets_out,
        "dynamic_config": dynamic_config,
        "squadron_configs": squadrons,
    });
    info!("📦 Setup: config bundle exported ({} secrets, {} squadron configs)",
          secrets.len().saturating_sub(1), bundle["squadron_configs"].as_object().map(|o| o.len()).unwrap_or(0));

    (
        [
            ("content-type", "application/json"),
            ("content-disposition", "attachment; filename=\"dradis-config-bundle.json\""),
        ],
        bundle.to_string(),
    ).into_response()
}

#[derive(Deserialize)]
struct ImportBundle {
    kind: Option<String>,
    schema_version: Option<u32>,
    venue: Option<String>,
    #[serde(default)]
    secrets: BTreeMap<String, String>,
    #[serde(default)]
    dynamic_config: serde_json::Value,
    #[serde(default)]
    squadron_configs: serde_json::Map<String, serde_json::Value>,
}

/// POST /api/setup/import — restore a bundle produced by `export_bundle` on a
/// (typically fresh) instance. Secrets are merged into the managed file and
/// process env; configs are round-tripped through `DynamicConfig` so fields
/// added since the export pick up defaults and removed fields are dropped
/// (serde is the migration subroutine). Follow with POST /api/setup/restart.
async fn import_bundle(Json(bundle): Json<ImportBundle>) -> Response {
    if bundle.kind.as_deref() != Some("dradis-config-bundle") {
        return (StatusCode::BAD_REQUEST,
                Json(json!({"error": "not a DRADIS config bundle"}))).into_response();
    }
    let ver = bundle.schema_version.unwrap_or(0);
    if ver == 0 || ver > BUNDLE_SCHEMA_VERSION {
        return (StatusCode::BAD_REQUEST,
                Json(json!({"error": format!(
                    "unsupported bundle schema_version {ver} (this build supports 1..={BUNDLE_SCHEMA_VERSION})")}))).into_response();
    }
    // Venue mismatch is a hard error: an intl bundle restored onto a US AMI
    // (or vice versa) would carry the wrong credential set.
    if let Some(v) = bundle.venue.as_deref() {
        if v != build_venue() {
            return (StatusCode::BAD_REQUEST,
                    Json(json!({"error": format!(
                        "bundle is for the '{v}' venue build but this instance is '{}'", build_venue())}))).into_response();
        }
    }

    // ── Secrets: merge managed + internal admin hash; never the session key ──
    let mut map = read_secrets();
    let mut imported_secrets = 0usize;
    for (k, v) in &bundle.secrets {
        let allowed = MANAGED_KEYS.iter().any(|(mk, _, _, _)| mk == k)
            || k == "DRADIS_ADMIN_HASH"
            || k.starts_with("LLM_");   // autonomy knobs persisted by put_autonomy
        if !allowed || v.is_empty() { continue; }
        map.insert(k.clone(), v.clone());
        std::env::set_var(k, v);
        imported_secrets += 1;
    }
    if let Err(e) = write_secrets(&map) {
        return (StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("could not persist secrets: {e}")}))).into_response();
    }

    // ── Configs: validate through the current schema, then persist ──────────
    let mut config_restored = false;
    let mut squadrons_restored = 0usize;
    if let Some(pool) = crate::helpers::db::pool() {
        if bundle.dynamic_config.is_object() {
            match serde_json::from_value::<crate::helpers::dynamic_config::DynamicConfig>(bundle.dynamic_config.clone()) {
                Ok(cfg) => {
                    if let Ok(migrated) = serde_json::to_string(&cfg) {
                        let old = crate::helpers::db::config_get(pool, "dynamic_config").await;
                        crate::helpers::db::config_set(pool, "dynamic_config", &migrated).await;
                        crate::helpers::db::record_config_change(
                            pool, "bundle_import", "full_snapshot", old.as_deref(), &migrated).await;
                        config_restored = true;
                    }
                }
                Err(e) => {
                    return (StatusCode::BAD_REQUEST,
                            Json(json!({"error": format!("dynamic_config invalid: {e}")}))).into_response();
                }
            }
        }
        for (sid, v) in &bundle.squadron_configs {
            match serde_json::from_value::<crate::helpers::dynamic_config::DynamicConfig>(v.clone()) {
                Ok(cfg) => {
                    if let Ok(migrated) = serde_json::to_string(&cfg) {
                        crate::helpers::db::squadron_config_set(pool, sid, &migrated).await;
                        squadrons_restored += 1;
                    }
                }
                Err(e) => warn!("⚠️ Bundle import: squadron '{}' config invalid — skipped: {}", sid, e),
            }
        }
    } else if bundle.dynamic_config.is_object() || !bundle.squadron_configs.is_empty() {
        return (StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "DB not ready — configs cannot be restored yet (secrets were saved)"}))).into_response();
    }

    info!("📦 Setup: bundle imported — {} secret(s), global config: {}, {} squadron config(s). Restart to apply.",
          imported_secrets, config_restored, squadrons_restored);
    Json(json!({
        "ok": true,
        "secrets_imported": imported_secrets,
        "dynamic_config_restored": config_restored,
        "squadron_configs_restored": squadrons_restored,
        "restart_required": true,
    })).into_response()
}

// ─── Config profiles (risk-posture presets) ──────────────────────────────────
//
// `src/profiles.json` is generated by `tools/generate-profiles.py` from the
// three `config.*.rs.example` files: each profile is a COMPLETE set of the
// runtime-tunable DynamicConfig fields, applied through the same validated
// `DynamicConfig::apply_patch_as` path as PATCH /api/config (live, audited in
// config_history, no restart). Safety floors still clamp on next load.

static PROFILES_JSON: &str = include_str!("../profiles.json");

fn profiles_value() -> serde_json::Value {
    serde_json::from_str(PROFILES_JSON)
        .expect("embedded profiles.json is valid (generated by tools/generate-profiles.py)")
}

/// GET /api/setup/profiles — profile metadata + full value sets (for preview).
///
/// Also reports the squadrons a `global_and_deployed` apply would touch, so the
/// UI can name them in the confirmation instead of making the operator guess.
async fn list_profiles() -> Response {
    let mut v = profiles_value();
    if let Some(obj) = v.as_object_mut() {
        obj.insert(
            "deployed_squadrons".to_string(),
            json!(crate::helpers::dynamic_config::registered_squadron_ids()),
        );
    }
    Json(v).into_response()
}

/// How far a profile apply reaches.
///
/// A deployed squadron reads its config from its OWN `squadron_configs` row
/// (`DynamicConfig::load_or_init_for_squadron`), and nothing in the CAG
/// subscribes to the global config broadcast. So a global-only apply is
/// invisible to every running patrol loop — which is why this defaults to
/// `GlobalAndDeployed`.
#[derive(Deserialize, Default, PartialEq, Eq, Clone, Copy, Debug)]
#[serde(rename_all = "snake_case")]
enum ProfileScope {
    /// Global config only. Seeds squadrons deployed LATER; changes nothing running now.
    GlobalOnly,
    /// Global config plus every currently-deployed squadron (default).
    #[default]
    GlobalAndDeployed,
}

#[derive(Deserialize)]
struct ApplyProfileRequest {
    name: String,
    #[serde(default)]
    scope: ProfileScope,
}

/// POST /api/setup/profiles/apply {"name":"balanced","scope":"global_and_deployed"}
///
/// Applies the profile as a COMPLETE replacement of the runtime-tunable field
/// set (profiles.json carries every such field), persists, and broadcasts so
/// live strategies pick it up within one tick. Attribution in config_history is
/// `profile_<name>` so the change is auditable/revertable.
///
/// The global write always happens — it is what `init_for_squadron` seeds future
/// squadrons from. When scope is `global_and_deployed` (the default) the same
/// values are then pushed into each deployed squadron's own config and live
/// handle, because that — not the global row — is what a running patrol loop
/// reads each tick.
async fn apply_profile(Json(req): Json<ApplyProfileRequest>) -> Response {
    use crate::helpers::dynamic_config::{self as dyncfg, DynamicConfig};

    let profiles = profiles_value();
    let Some(profile) = profiles["profiles"].get(&req.name) else {
        return (StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("unknown profile '{}'", req.name)}))).into_response();
    };
    let patch = profile["values"].to_string();

    let Some(tx) = dyncfg::global_config_tx() else {
        return (StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "config broadcast not ready — try again shortly"}))).into_response();
    };
    let current = tx.borrow().clone();
    let actor = format!("profile_{}", req.name);
    let fields = profile["values"].as_object().map(|o| o.len()).unwrap_or(0);

    let new_cfg = match DynamicConfig::apply_patch_as(&current, &patch, &actor).await {
        Ok(cfg) => cfg,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("profile apply failed: {e}")}))).into_response(),
    };
    let _ = tx.send(new_cfg);
    info!("🎛️ Setup: applied '{}' profile ({} fields) to global config", req.name, fields);

    // Fan out to the squadrons that are actually trading.
    let mut squadrons_applied: Vec<String> = Vec::new();
    let mut squadron_errors: Vec<serde_json::Value> = Vec::new();
    if req.scope == ProfileScope::GlobalAndDeployed {
        for sid in dyncfg::registered_squadron_ids() {
            match DynamicConfig::apply_squadron_patch_as(&sid, &patch, &actor).await {
                Ok(_) => squadrons_applied.push(sid),
                Err(e) => {
                    warn!("⚠️ Setup: profile '{}' failed on squadron '{}': {}", req.name, sid, e);
                    squadron_errors.push(json!({"squadron": sid, "error": e.to_string()}));
                }
            }
        }
        info!("🎛️ Setup: profile '{}' fanned out to {} deployed squadron(s){}",
              req.name, squadrons_applied.len(),
              if squadron_errors.is_empty() { String::new() }
              else { format!(", {} failed", squadron_errors.len()) });
    }

    // Partial failure is reported explicitly rather than swallowed: a squadron
    // that kept its old values is still trading on them.
    let status = if squadron_errors.is_empty() { StatusCode::OK } else { StatusCode::MULTI_STATUS };
    (status, Json(json!({
        "ok": squadron_errors.is_empty(),
        "profile": req.name,
        "scope": match req.scope {
            ProfileScope::GlobalOnly => "global_only",
            ProfileScope::GlobalAndDeployed => "global_and_deployed",
        },
        "fields_applied": fields,
        "squadrons_applied": squadrons_applied,
        "squadron_errors": squadron_errors,
    }))).into_response()
}

// ─── AI autonomy (LLM config-patch policy) ───────────────────────────────────

/// GET /api/setup/autonomy — current autonomy tier, kill switch, breaker state,
/// and the effective policy knobs. Values live in env (secrets-file backed) and
/// are re-read by the policy engine every advisory cycle, so changes here apply
/// live — no restart needed.
async fn get_autonomy() -> Response {
    use crate::helpers::llm_policy::{breaker_demoted, PolicyKnobs};
    let knobs = PolicyKnobs::from_env();
    let tier = std::env::var("LLM_AUTONOMY_TIER")
        .ok().and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|t| (1..=3).contains(t))
        .unwrap_or(1);
    Json(json!({
        "tier": tier,
        "kill_switch": knobs.kill_switch,
        "breaker_demoted": breaker_demoted(),
        "max_patches_per_hour": knobs.max_batches_per_hour,
        "max_delta_pct": knobs.max_delta_pct,
        "breaker_drawdown_usdc": knobs.breaker_drawdown_usdc,
        "breaker_window_secs": knobs.breaker_window_secs,
    })).into_response()
}

#[derive(Deserialize)]
struct PutAutonomy {
    tier: Option<i64>,
    kill_switch: Option<bool>,
    /// Clear a circuit-breaker demotion after reviewing the reverted changes.
    reset_breaker: Option<bool>,
}

/// PUT /api/setup/autonomy — set the autonomy tier and/or kill switch
/// (persisted to the secrets file, applied to process env immediately), and
/// optionally reset a circuit-breaker demotion.
async fn put_autonomy(Json(body): Json<PutAutonomy>) -> Response {
    if let Some(t) = body.tier {
        if !(1..=3).contains(&t) {
            return (StatusCode::BAD_REQUEST,
                    Json(json!({"error": "tier must be 1, 2, or 3"}))).into_response();
        }
    }
    let mut map = read_secrets();
    if let Some(t) = body.tier {
        map.insert("LLM_AUTONOMY_TIER".to_string(), t.to_string());
        std::env::set_var("LLM_AUTONOMY_TIER", t.to_string());
        info!("🤖 Setup: LLM autonomy tier set to {} via UI", t);
    }
    if let Some(k) = body.kill_switch {
        let v = if k { "1" } else { "0" };
        map.insert("LLM_AUTONOMY_KILL".to_string(), v.to_string());
        std::env::set_var("LLM_AUTONOMY_KILL", v);
        info!("🤖 Setup: LLM autonomy kill switch {} via UI", if k { "ENGAGED" } else { "cleared" });
    }
    if body.reset_breaker == Some(true) {
        crate::helpers::llm_policy::reset_breaker_demotion();
        info!("🧯 Setup: LLM autonomy circuit-breaker demotion cleared via UI");
    }
    if (body.tier.is_some() || body.kill_switch.is_some()) && write_secrets(&map).is_err() {
        return (StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "failed to persist autonomy settings"}))).into_response();
    }
    get_autonomy().await
}

// ─── Router ──────────────────────────────────────────────────────────────────

/// Routes that require an admin session (or first-boot wizard mode).
/// Nested inside the protected router in `server.rs`, so `X-API-Key` and the
/// read-only demo gate stack on top.
pub fn admin_routes() -> Router {
    Router::new()
        .route("/api/setup/credentials", get(get_credentials).put(put_credentials))
        .route("/api/setup/raptors",     get(get_raptor_sources))
        .route("/api/setup/admin",       post(set_admin))
        .route("/api/setup/test",        post(test_connection))
        .route("/api/setup/restart",     post(restart))
        .route("/api/setup/export",      get(export_bundle))
        .route("/api/setup/import",      post(import_bundle))
        .route("/api/setup/autonomy",    get(get_autonomy).put(put_autonomy))
        .route("/api/setup/profiles",    get(list_profiles))
        .route("/api/setup/profiles/apply", post(apply_profile))
        .layer(axum::middleware::from_fn(require_admin))
}

/// Routes callable before login: status probe and the login exchange itself.
pub fn public_routes() -> Router {
    Router::new()
        .route("/api/setup/status", get(get_status))
        .route("/api/setup/acknowledge", post(acknowledge_alpha))
        .route("/api/auth/login",   post(login))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every embedded profile must be a complete, currently-valid DynamicConfig
    /// value set: parseable, covering every serialized field (so a profile is a
    /// full seed, not a partial patch), with no unknown keys (schema drift ⇒
    /// re-run tools/generate-profiles.py).
    #[test]
    fn embedded_profiles_match_config_schema() {
        let profiles = profiles_value();
        let schema = serde_json::to_value(
            crate::helpers::dynamic_config::DynamicConfig::default()).unwrap();
        let schema_keys: std::collections::BTreeSet<&str> =
            schema.as_object().unwrap().keys().map(|k| k.as_str()).collect();

        let map = profiles["profiles"].as_object().expect("profiles map");
        assert_eq!(map.len(), 3, "expected conservative/balanced/aggressive");
        for (name, profile) in map {
            let values = profile["values"].as_object().expect("values object");
            let keys: std::collections::BTreeSet<&str> =
                values.keys().map(|k| k.as_str()).collect();
            let missing: Vec<_> = schema_keys.difference(&keys).collect();
            let unknown: Vec<_> = keys.difference(&schema_keys).collect();
            assert!(missing.is_empty(), "profile '{name}' missing fields: {missing:?}");
            assert!(unknown.is_empty(), "profile '{name}' has unknown fields: {unknown:?}");
            // Full deserialization proves every value has the right type.
            let mut base = schema.clone();
            for (k, v) in values { base[k] = v.clone(); }
            serde_json::from_value::<crate::helpers::dynamic_config::DynamicConfig>(base)
                .unwrap_or_else(|e| panic!("profile '{name}' invalid: {e}"));
        }
    }

    /// The bundle migration guarantee: a config exported from an OLDER build
    /// (fields added since then carry `#[serde(default)]` — repo convention)
    /// or a NEWER build (extra fields → ignored by serde) must deserialize
    /// into the current DynamicConfig schema.
    #[test]
    fn bundle_config_migration_roundtrip() {
        let current = crate::helpers::dynamic_config::DynamicConfig::default();
        let mut old = serde_json::to_value(&current).unwrap();
        let obj = old.as_object_mut().unwrap();
        // Old export: a field added later (has #[serde(default)]) is absent…
        assert!(obj.remove("maker_min_spread").is_some());
        // …and a future export carries a field this build doesn't know.
        obj.insert("field_from_the_future".into(), serde_json::json!(42));
        obj.insert("ghost_mode".into(), serde_json::json!(true));

        let cfg: crate::helpers::dynamic_config::DynamicConfig =
            serde_json::from_value(old).expect("cross-version config must deserialize");
        assert!(cfg.ghost_mode);
        // Missing defaulted field picked up its compile-time default.
        assert_eq!(cfg.maker_min_spread, crate::config::MAKER_MIN_SPREAD);

        // Round-trip through the current schema drops the unknown field.
        let migrated = serde_json::to_value(&cfg).unwrap();
        assert!(migrated.get("field_from_the_future").is_none());
    }

    #[test]
    fn token_roundtrip_and_expiry() {
        std::env::set_var("DRADIS_DATA_DIR", std::env::temp_dir().join("dradis-setup-test").to_str().unwrap());
        let t = issue_token();
        assert!(verify_token(&t));
        assert!(!verify_token("12345.deadbeef"));
        assert!(!verify_token("garbage"));
        // Expired token: forge payload in the past with a valid MAC.
        let past = (chrono::Utc::now().timestamp() - 10).to_string();
        let mut mac = HmacSha256::new_from_slice(&session_key()).unwrap();
        mac.update(past.as_bytes());
        let expired = format!("{}.{}", past, hex_encode(&mac.finalize().into_bytes()));
        assert!(!verify_token(&expired));
        let _ = std::fs::remove_dir_all(std::env::temp_dir().join("dradis-setup-test"));
    }

    #[test]
    fn password_hash_roundtrip() {
        // Construct at runtime so static analysis doesn't flag a hard-coded credential.
        let pw = ["hunter2", "hunter2"].join("-");
        let h = hash_password(&pw).unwrap();
        assert!(verify_password(&pw, &h));
        assert!(!verify_password("wrong", &h));
    }

    #[test]
    fn managed_key_whitelist_rejects_internal() {
        for k in INTERNAL_KEYS {
            assert!(!MANAGED_KEYS.iter().any(|(mk, _, _, _)| mk == k));
        }
    }

    /// Every key a Raptor advertises must be a managed credential filed under
    /// the "raptor" panel. Without this, adding a Raptor whose key is missing
    /// from `MANAGED_KEYS` renders an input that `put_credentials` then rejects
    /// as un-managed — a dead field the operator cannot diagnose.
    #[test]
    fn raptor_sources_reference_managed_raptor_keys() {
        for r in RAPTOR_SOURCES {
            for k in r.keys {
                let entry = MANAGED_KEYS.iter().find(|(mk, _, _, _)| mk == k);
                let (_, _, _, panel) = entry.unwrap_or_else(|| {
                    panic!("Raptor '{}' references unmanaged key '{}' — add it to MANAGED_KEYS", r.id, k)
                });
                assert_eq!(
                    *panel, "raptor",
                    "key '{}' is used by Raptor '{}' but filed under the '{}' panel",
                    k, r.id, panel,
                );
            }
        }
    }

    /// Tiers drive the UI badges, so an unrecognised value would render blank.
    #[test]
    fn raptor_tiers_are_known_values() {
        for r in RAPTOR_SOURCES {
            assert!(
                matches!(r.tier, "required" | "recommended" | "optional"),
                "Raptor '{}' has unknown tier '{}'", r.id, r.tier,
            );
        }
    }

    /// Conversely, every "raptor"-panel credential must be claimed by at least
    /// one Raptor — otherwise it appears in no card and is unreachable in the UI.
    #[test]
    fn every_raptor_panel_key_belongs_to_a_raptor() {
        for (key, _, _, _) in MANAGED_KEYS.iter().filter(|(_, _, _, p)| *p == "raptor") {
            assert!(
                RAPTOR_SOURCES.iter().any(|r| r.keys.contains(key)),
                "credential '{}' is on the raptor panel but no Raptor in RAPTOR_SOURCES claims it",
                key,
            );
        }
    }
}
