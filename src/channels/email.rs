use anyhow::Result;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

use super::{Channel, InboundMessage, OutboundMessage};

pub struct EmailChannel {
    imap_host: String,
    imap_port: u16,
    smtp_host: String,
    smtp_port: u16,
    username: String,
    password: String,
    stop_flag: std::sync::Arc<AtomicBool>,
}

impl EmailChannel {
    pub fn new(
        imap_host: String,
        imap_port: u16,
        smtp_host: String,
        smtp_port: u16,
        username: String,
        password: String,
    ) -> Self {
        Self {
            imap_host,
            imap_port,
            smtp_host,
            smtp_port,
            username,
            password,
            stop_flag: std::sync::Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl Channel for EmailChannel {
    fn id(&self) -> &str {
        "email"
    }

    async fn start(&self, _inbound: mpsc::Sender<InboundMessage>) -> Result<()> {
        tracing::info!(
            imap_host = %self.imap_host,
            smtp_host = %self.smtp_host,
            username = %self.username,
            "Email channel starting (IMAP IDLE not yet implemented)"
        );

        // TODO: Implement IMAP IDLE for real-time email monitoring
        // For now, log that it's configured but not active

        self.stop_flag.store(false, Ordering::SeqCst);

        loop {
            if self.stop_flag.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }

        Ok(())
    }

    async fn send(&self, message: OutboundMessage) -> Result<()> {
        tracing::info!(
            target = %message.target,
            "Email send requested (SMTP not yet implemented)"
        );

        // TODO: Implement SMTP sending
        // For now, just log
        tracing::info!(text = %message.text, "Would send email");

        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.stop_flag.store(true, Ordering::SeqCst);
        Ok(())
    }
}
