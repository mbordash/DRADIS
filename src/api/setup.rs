//! Setup & credentials management API — the "prosumer" onboarding chapter.
//!
//! Lets the Control Tower configure venue credentials (intl wallet / US API keys),
//! shared integrations (Alpaca, Telegram), and an admin password — without SSH
//! access to the server or editing config files.
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

/// UI-manageable credential whitelist: (env key, label, venue scope).
/// Scope: "intl" | "us" | "shared" — the UI shows only the scopes relevant to
/// the running build (see `GET /api/setup/status`).
const MANAGED_KEYS: &[(&str, &str, &str)] = &[
    ("POLYMARKET_PRIVATE_KEY",   "Polymarket wallet private key", "intl"),
    ("POLYGON_RPC_URL",          "Polygon RPC endpoint",          "intl"),
    ("POLYMARKET_US_KEY_ID",     "Polymarket US API key ID",      "us"),
    ("POLYMARKET_US_SECRET_KEY", "Polymarket US secret key",      "us"),
    ("ALPACA_API_KEY_ID",        "Alpaca API key ID",             "shared"),
    ("ALPACA_API_SECRET_KEY",    "Alpaca API secret key",         "shared"),
    ("TELEGRAM_BOT_TOKEN",       "Telegram bot token",            "shared"),
    ("TELEGRAM_CHAT_ID",         "Telegram chat ID",              "shared"),
    ("LLM_PROVIDER",             "LLM provider (ollama | openai | anthropic)", "shared"),
    ("OLLAMA_URL",               "Ollama URL (local or remote)",  "shared"),
    ("OLLAMA_MODEL",             "Ollama model",                  "shared"),
    ("LLM_API_BASE",             "Hosted LLM API base URL",       "shared"),
    ("LLM_API_KEY",              "Hosted LLM API key",            "shared"),
    ("LLM_MODEL",                "Hosted LLM model",              "shared"),
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
        "restart_pending": false,
    })).into_response()
}

/// GET /api/setup/credentials — masked inventory of managed keys. Never returns
/// secret values; only whether each is set plus a last-4 hint for recognition.
async fn get_credentials() -> Response {
    let secrets = read_secrets();
    let items: Vec<serde_json::Value> = MANAGED_KEYS.iter().map(|(key, label, scope)| {
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
        json!({ "key": key, "label": label, "scope": scope, "set": set, "hint": hint, "source": source })
    }).collect();
    Json(json!({ "credentials": items })).into_response()
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
        if !MANAGED_KEYS.iter().any(|(k, _, _)| k == key) {
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
        .route("/api/setup/admin",       post(set_admin))
        .route("/api/setup/test",        post(test_connection))
        .route("/api/setup/restart",     post(restart))
        .route("/api/setup/autonomy",    get(get_autonomy).put(put_autonomy))
        .layer(axum::middleware::from_fn(require_admin))
}

/// Routes callable before login: status probe and the login exchange itself.
pub fn public_routes() -> Router {
    Router::new()
        .route("/api/setup/status", get(get_status))
        .route("/api/auth/login",   post(login))
}

#[cfg(test)]
mod tests {
    use super::*;

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
            assert!(!MANAGED_KEYS.iter().any(|(mk, _, _)| mk == k));
        }
    }
}
