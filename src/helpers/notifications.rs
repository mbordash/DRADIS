use anyhow::Result;
use reqwest;
use serde::Serialize;
use tracing::{error, info};
use crate::config;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use hmac::{Hmac, Mac};
use sha1::Sha1;

type HmacSha1 = Hmac<Sha1>;

// ── Telegram ────────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct TelegramMessage {
    chat_id: String,
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parse_mode: Option<String>,
}

pub async fn send_notification(token: &str, chat_id: &str, message: &str) -> Result<()> {
    if !config::ENABLE_TELEGRAM || token.is_empty() || chat_id.is_empty() {
        return Ok(());
    }

    let url = format!("https://api.telegram.org/bot{}/sendMessage", token);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap_or_default();

    let payload = TelegramMessage {
        chat_id: chat_id.to_string(),
        text: message.to_string(),
        parse_mode: None,
    };

    let resp = client.post(&url)
        .json(&payload)
        .send()
        .await?;

    let status = resp.status();
    if status.is_success() {
        info!("📱 Telegram notification sent successfully");
        Ok(())
    } else {
        let err_body = resp.text().await.unwrap_or_default();
        error!("❌ Failed to send Telegram notification: HTTP {} - {}", status, err_body);
        Err(anyhow::anyhow!("Failed to send notification, status: {}", status))
    }
}

// ── Twitter / X ─────────────────────────────────────────────────────────────

/// Percent-encode a string per RFC 3986 (required by OAuth 1.0a).
fn pct(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

/// Build an `Authorization: OAuth …` header using HMAC-SHA1 (OAuth 1.0a).
/// The Twitter v2 REST endpoint uses JSON bodies, so only oauth params are
/// included in the signature base string (no body params).
fn oauth1_header(
    method: &str,
    url: &str,
    api_key: &str,
    api_secret: &str,
    access_token: &str,
    access_token_secret: &str,
) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Nonce: nanos is distinct enough for low-frequency tweet volume.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let nonce = format!("{}{:09}", ts, nanos);
    let ts_str = ts.to_string();

    let params: Vec<(&str, String)> = vec![
        ("oauth_consumer_key",     api_key.to_string()),
        ("oauth_nonce",            nonce.clone()),
        ("oauth_signature_method","HMAC-SHA1".to_string()),
        ("oauth_timestamp",        ts_str.clone()),
        ("oauth_token",            access_token.to_string()),
        ("oauth_version",          "1.0".to_string()),
    ];

    let mut sorted = params.clone();
    sorted.sort_by(|a, b| a.0.cmp(b.0));

    let param_str = sorted.iter()
        .map(|(k, v)| format!("{}={}", pct(k), pct(v)))
        .collect::<Vec<_>>()
        .join("&");

    let base = format!("{}&{}&{}", pct(method), pct(url), pct(&param_str));
    let signing_key = format!("{}&{}", pct(api_secret), pct(access_token_secret));

    let mut mac = HmacSha1::new_from_slice(signing_key.as_bytes())
        .expect("HMAC can take any key length");
    mac.update(base.as_bytes());
    let signature = BASE64_STANDARD.encode(mac.finalize().into_bytes());

    // Build the full Authorization header including the signature.
    let mut header_params = params;
    header_params.push(("oauth_signature", signature.clone()));
    header_params.sort_by(|a, b| a.0.cmp(b.0));

    let header_value = header_params.iter()
        .map(|(k, v)| format!("{}=\"{}\"", k, pct(v)))
        .collect::<Vec<_>>()
        .join(", ");

    format!("OAuth {}", header_value)
}

/// Truncate a string to `max` chars (Unicode-aware), appending "…" if cut.
fn truncate(s: &str, max: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max {
        s.to_string()
    } else {
        let cut: String = chars[..max - 1].iter().collect();
        format!("{}…", cut)
    }
}

/// Post a tweet to Twitter/X using the v2 API with OAuth 1.0a User Context.
///
/// Credentials come from env vars at startup:
///   `X_API_KEY`, `X_API_SECRET`,
///   `X_ACCESS_TOKEN`, `X_ACCESS_TOKEN_SECRET`
pub async fn post_tweet(
    api_key: &str,
    api_secret: &str,
    access_token: &str,
    access_token_secret: &str,
    text: &str,
) -> Result<()> {
    if !config::ENABLE_X
        || api_key.is_empty()
        || api_secret.is_empty()
        || access_token.is_empty()
        || access_token_secret.is_empty()
    {
        return Ok(());
    }

    // Twitter caps tweets at 280 chars; hard-truncate to be safe.
    let tweet_text = truncate(text, 280);

    let endpoint = "https://api.twitter.com/2/tweets";
    let auth = oauth1_header("POST", endpoint, api_key, api_secret, access_token, access_token_secret);

    let body = serde_json::json!({ "text": tweet_text });

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    let resp = client
        .post(endpoint)
        .header("Authorization", auth)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;

    let status = resp.status();
    if status.is_success() {
        info!("🐦 Tweet posted successfully");
        Ok(())
    } else {
        let err_body = resp.text().await.unwrap_or_default();
        error!("❌ Failed to post tweet: HTTP {} - {}", status, err_body);
        Err(anyhow::anyhow!("Tweet failed, status: {}", status))
    }
}

/// Compose and fire an ENTRY tweet (detached — never blocks the trading loop).
pub fn tweet_entry(
    tw_key: String, tw_secret: String, tw_token: String, tw_token_secret: String,
    slug: String, market_name: String, price: rust_decimal::Decimal, shares: rust_decimal::Decimal,
) {
    if !config::ENABLE_X { return; }
    tokio::spawn(async move {
        let name  = truncate(&market_name, 50);
        let slug  = truncate(&slug, 28);
        let text  = format!(
            "🟢 ENTRY | {slug}\n{name}\n${price:.2} × {shares:.1} shares | #polymarket #DRADIStrading",
        );
        let _ = post_tweet(&tw_key, &tw_secret, &tw_token, &tw_token_secret, &text).await;
    });
}

/// Compose and fire an EXIT tweet (detached — never blocks the trading loop).
pub fn tweet_exit(
    tw_key: String, tw_secret: String, tw_token: String, tw_token_secret: String,
    slug: String, market_name: String, exit_price: rust_decimal::Decimal,
    reason: String, trade_pnl: rust_decimal::Decimal, session_pnl: rust_decimal::Decimal,
) {
    if !config::ENABLE_X { return; }
    tokio::spawn(async move {
        let name      = truncate(&market_name, 50);
        let slug      = truncate(&slug, 28);
        let pnl_sign  = if trade_pnl   >= rust_decimal::Decimal::ZERO { "+" } else { "" };
        let sess_sign = if session_pnl >= rust_decimal::Decimal::ZERO { "+" } else { "" };
        let text = format!(
            "🔴 EXIT | {slug}\n{name}\nbid=${exit_price:.2} | {reason}\nTrade P&L: {pnl_sign}{trade_pnl:.2} | Session: {sess_sign}{session_pnl:.2}| #polymarket #DRADIStrading",
        );
        let _ = post_tweet(&tw_key, &tw_secret, &tw_token, &tw_token_secret, &text).await;
    });
}
