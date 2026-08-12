use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

use super::{Channel, InboundMessage, OutboundMessage};

pub struct TelegramChannel {
    bot_token: String,
    bot_name: String,
    dm_policy: String,
    allowed_users: Vec<i64>,
    client: reqwest::Client,
    stop_flag: Arc<AtomicBool>,
}

impl TelegramChannel {
    pub fn new(
        bot_token: String,
        bot_name: String,
        dm_policy: String,
        allowed_users: Vec<i64>,
    ) -> Self {
        Self {
            bot_token,
            bot_name,
            dm_policy,
            allowed_users,
            client: reqwest::Client::new(),
            stop_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn get_me(&self) -> Result<serde_json::Value> {
        let url = format!("https://api.telegram.org/bot{}/getMe", self.bot_token);
        let resp = self.client.get(&url).send().await?.json().await?;
        Ok(resp)
    }

    async fn get_updates(&self, offset: Option<i64>) -> Result<Vec<serde_json::Value>> {
        let mut url = format!(
            "https://api.telegram.org/bot{}/getUpdates?timeout=30",
            self.bot_token
        );
        if let Some(offset) = offset {
            url = format!("{}&offset={}", url, offset);
        }

        let resp: serde_json::Value = self.client.get(&url).send().await?.json().await?;

        if let Some(ok) = resp.get("ok").and_then(|v| v.as_bool()) {
            if ok {
                if let Some(result) = resp.get("result").and_then(|v| v.as_array()) {
                    return Ok(result.clone());
                }
            }
        }

        Ok(Vec::new())
    }

    async fn send_message(&self, chat_id: &str, text: &str, reply_to: Option<i64>) -> Result<serde_json::Value> {
        let mut body = json!({
            "chat_id": chat_id,
            "text": text,
        });

        if let Some(reply_to) = reply_to {
            body["reply_to_message_id"] = json!(reply_to);
        }

        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.bot_token);
        let resp = self.client.post(&url).json(&body).send().await?.json().await?;
        Ok(resp)
    }

    async fn send_typing(&self, chat_id: &str) -> Result<()> {
        let body = json!({
            "chat_id": chat_id,
            "action": "typing",
        });

        let url = format!("https://api.telegram.org/bot{}/sendChatAction", self.bot_token);
        let _ = self.client.post(&url).json(&body).send().await;
        Ok(())
    }

    fn is_user_allowed(&self, user_id: i64) -> bool {
        if self.allowed_users.is_empty() {
            return true; // No allowlist = everyone allowed
        }
        self.allowed_users.contains(&user_id)
    }

    fn extract_text(message: &serde_json::Value) -> Option<String> {
        if let Some(text) = message.get("text").and_then(|v| v.as_str()) {
            return Some(text.to_string());
        }

        if let Some(caption) = message.get("caption").and_then(|v| v.as_str()) {
            return Some(caption.to_string());
        }

        None
    }

    fn extract_user(message: &serde_json::Value) -> Option<(i64, String)> {
        let user = message.get("from")?;
        let id = user.get("id")?.as_i64()?;
        let name = user.get("first_name")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string();
        Some((id, name))
    }

    fn extract_chat(message: &serde_json::Value) -> Option<(i64, String)> {
        let chat = message.get("chat")?;
        let id = chat.get("id")?.as_i64()?;
        let kind = chat.get("type").and_then(|v| v.as_str()).unwrap_or("private");
        Some((id, kind.to_string()))
    }
}

#[async_trait]
impl Channel for TelegramChannel {
    fn id(&self) -> &str {
        "telegram"
    }

    async fn start(&self, inbound: mpsc::Sender<InboundMessage>) -> Result<()> {
        tracing::info!("Starting Telegram channel");

        // Test bot connection
        let me = self.get_me().await
            .context("Failed to connect to Telegram Bot API")?;

        let bot_username = me.get("result")
            .and_then(|r| r.get("username"))
            .and_then(|u| u.as_str())
            .unwrap_or("unknown");

        tracing::info!(bot = bot_username, "Telegram bot connected");

        self.stop_flag.store(false, Ordering::SeqCst);
        let mut offset: Option<i64> = None;
        let stop = self.stop_flag.clone();

        loop {
            if stop.load(Ordering::SeqCst) {
                break;
            }

            match self.get_updates(offset).await {
                Ok(updates) => {
                    for update in &updates {
                        if let Some(update_id) = update.get("update_id").and_then(|v| v.as_i64()) {
                            offset = Some(update_id + 1);
                        }

                        if let Some(message) = update.get("message") {
                            let text = Self::extract_text(message);
                            let user = Self::extract_user(message);
                            let chat = Self::extract_chat(message);
                            let msg_id = message.get("message_id").and_then(|v| v.as_i64());

                            if let (Some((user_id, user_name)), Some((chat_id, chat_type))) = (user, chat) {
                                // Check allowlist
                                if !self.is_user_allowed(user_id) {
                                    tracing::debug!(user_id = user_id, "Unauthorized user, ignoring");
                                    continue;
                                }

                                let peer_id = if chat_type == "private" {
                                    format!("user:{}", user_id)
                                } else {
                                    format!("group:{}", chat_id)
                                };

                                let inbound_msg = InboundMessage {
                                    id: format!("tg_{}", msg_id.unwrap_or(0)),
                                    channel: "telegram".to_string(),
                                    account_id: Some("default".to_string()),
                                    peer_id,
                                    conversation_id: Some(chat_id.to_string()),
                                    text,
                                    attachments: Vec::new(),
                                    reply_to: message.get("reply_to_message")
                                        .and_then(|r| r.get("message_id"))
                                        .and_then(|v| v.as_i64())
                                        .map(|id| id.to_string()),
                                    timestamp: chrono::Utc::now(),
                                    metadata: json!({
                                        "user_name": user_name,
                                        "chat_type": chat_type,
                                        "chat_id": chat_id,
                                    }),
                                };

                                if let Err(e) = inbound.send(inbound_msg).await {
                                    tracing::error!(error = %e, "Failed to send inbound message");
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to get updates, retrying in 5s");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }

        Ok(())
    }

    async fn send(&self, message: OutboundMessage) -> Result<()> {
        self.send_typing(&message.target).await?;

        let reply_id = message.reply_to.and_then(|r| r.parse::<i64>().ok());
        self.send_message(&message.target, &message.text, reply_id).await?;

        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.stop_flag.store(true, Ordering::SeqCst);
        Ok(())
    }
}
