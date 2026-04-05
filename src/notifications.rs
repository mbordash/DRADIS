use anyhow::Result;
use reqwest;
use serde::Serialize;
use tracing::{error, info};
use crate::config;

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
    let client = reqwest::Client::new();
    
    // We avoid Markdown to prevent 400 Bad Request errors when the message contains
    // special characters from API error responses (like [ or _).
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
