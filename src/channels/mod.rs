pub mod telegram;
pub mod email;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct InboundMessage {
    pub id: String,
    pub channel: String,
    pub account_id: Option<String>,
    pub peer_id: String,
    pub conversation_id: Option<String>,
    pub text: Option<String>,
    pub attachments: Vec<crate::agent::Attachment>,
    pub reply_to: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct OutboundMessage {
    pub channel: String,
    pub target: String,
    pub reply_to: Option<String>,
    pub text: String,
    pub attachments: Vec<crate::agent::Attachment>,
    pub metadata: serde_json::Value,
}

#[async_trait]
pub trait Channel: Send + Sync {
    fn id(&self) -> &str;

    async fn start(&self, inbound: mpsc::Sender<InboundMessage>) -> Result<()>;

    async fn send(&self, message: OutboundMessage) -> Result<()>;

    async fn stop(&self) -> Result<()>;
}
