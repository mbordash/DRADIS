use anyhow::Result;
use reqwest;
use serde::Serialize;
use tracing::{error, info};
use crate::config;

#[derive(Serialize)]
struct TelegramMessage {
    chat_id: String,
    text: String,
    parse_mode: String,
}

pub async fn send_notification(token: &str, chat_id: &str, message: &str) -> Result<()> {
    if !config::ENABLE_TELEGRAM || token.is_empty() || chat_id.is_empty() {
        return Ok(());
    }

    let url = format!("https://api.telegram.org/bot{}/sendMessage", token);
    let client = reqwest::Client::new();
    
    let payload = TelegramMessage {
        chat_id: chat_id.to_string(),
        text: message.to_string(),
        parse_mode: "Markdown".to_string(),
    };

    let resp = client.post(&url)
        .json(&payload)
        .send()
        .await?;

    if resp.status().is_success() {
        info!("📱 Telegram notification sent successfully");
        Ok(())
    } else {
        error!("❌ Failed to send Telegram notification: HTTP {}", resp.status());
        Err(anyhow::anyhow!("Failed to send notification, status: {}", resp.status()))
    }
}
