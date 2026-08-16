use anyhow::{Context, Result};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, broadcast};

use crate::agent::{AgentConfig, AgentRuntime, AgentRequest, AgentResponse};
use crate::channels::{Channel, InboundMessage, OutboundMessage};
use crate::database::Database;
use crate::database::sessions::SessionStore;
use crate::database::jobs::JobStore;
use crate::database::approvals::ApprovalStore;
use crate::database::memory::MemoryStore;
use crate::database::provider_usage::ProviderUsageStore;
use crate::llm::LlmProvider;
use crate::models::ModelRouter;
use crate::tools::create_default_registry;
use crate::config::Config;
use crate::permissions::{PermissionManager, PermissionConfig};
use crate::scheduler::Scheduler;

pub struct Gateway {
    agents: DashMap<String, Arc<AgentRuntime>>,
    channels: Vec<Arc<dyn Channel>>,
    scheduler: Arc<Scheduler>,
    permission_manager: Arc<PermissionManager>,
    outbound_rx: mpsc::Receiver<OutboundMessage>,
    outbound_tx: mpsc::Sender<OutboundMessage>,
    inbound_tx: mpsc::Sender<InboundMessage>,
    inbound_rx: mpsc::Receiver<InboundMessage>,
}

impl Gateway {
    pub async fn new(config: &Config) -> Result<Self> {
        // Initialize database
        let db = Database::new(&config.database.path).await?;
        let pool = db.pool.clone();

        // Initialize stores
        let session_store = Arc::new(SessionStore::new(pool.clone()));
        let job_store = Arc::new(JobStore::new(pool.clone()));
        let approval_store = Arc::new(ApprovalStore::new(pool.clone()));
        let _memory_store = Arc::new(MemoryStore::new(pool.clone()));
        let _provider_usage = Arc::new(ProviderUsageStore::new(pool.clone()));

        // Initialize model router
        let model_router = Arc::new(ModelRouter::new());

        // Register model profiles from config
        if let Some(fast) = &config.models.fast {
            model_router.register_profile("fast".to_string(), fast.clone());
        }
        if let Some(smart) = &config.models.smart {
            model_router.register_profile("smart".to_string(), smart.clone());
        }
        if let Some(reasoning) = &config.models.reasoning {
            model_router.register_profile("reasoning".to_string(), reasoning.clone());
        }

        // Initialize tool registry with default tools
        let tools = Arc::new(create_default_registry());

        // Initialize LLM provider from default model profile
        let default_model = config.models.fast.as_ref()
            .or(config.models.smart.as_ref())
            .or(config.models.reasoning.as_ref())
            .context("No model profile configured")?;

        let llm_provider = Arc::new(LlmProvider::from_config(default_model)?);

        // Initialize permission manager
        let permission_manager = Arc::new(PermissionManager::new(
            approval_store.clone(),
            PermissionConfig::default(),
        ));

        // Create channels
        let mut channels: Vec<Arc<dyn Channel>> = Vec::new();

        if let Some(telegram_config) = &config.channels.telegram {
            if telegram_config.enabled {
                let token = telegram_config.bot_token.clone()
                    .or_else(|| telegram_config.bot_token_env.as_ref()
                        .and_then(|env_var| std::env::var(env_var).ok()))
                    .context("Telegram bot token not configured")?;

                let channel = crate::channels::telegram::TelegramChannel::new(
                    token,
                    "clawrig".to_string(),
                    telegram_config.dm_policy.clone(),
                    Vec::new(),
                );
                channels.push(Arc::new(channel));
            }
        }

        if let Some(email_config) = &config.channels.email {
            if email_config.enabled {
                let username = email_config.username.clone()
                    .or_else(|| email_config.username_env.as_ref()
                        .and_then(|env_var| std::env::var(env_var).ok()))
                    .context("Email username not configured")?;

                let password = email_config.password.clone()
                    .or_else(|| email_config.password_env.as_ref()
                        .and_then(|env_var| std::env::var(env_var).ok()))
                    .context("Email password not configured")?;

                let channel = crate::channels::email::EmailChannel::new(
                    email_config.imap_host.clone(),
                    email_config.imap_port.unwrap_or(993),
                    email_config.smtp_host.clone(),
                    email_config.smtp_port.unwrap_or(587),
                    username,
                    password,
                );
                channels.push(Arc::new(channel));
            }
        }

        // Create channels for message passing
        let (outbound_tx, outbound_rx) = mpsc::channel::<OutboundMessage>(100);
        let (inbound_tx, inbound_rx) = mpsc::channel::<InboundMessage>(100);

        // Create SSE broadcast channel
        let (event_tx, _) = broadcast::channel::<String>(256);

        // Create agent runtime
        let agent_config = AgentConfig {
            name: "main".to_string(),
            system_prompt: "You are ClawRig, a helpful AI assistant.".to_string(),
            max_tool_rounds: 10,
            model_override: None,
            yolo_mode: config.agents.defaults.yolo,
        };
        let agent_runtime = Arc::new(AgentRuntime::new(
            agent_config,
            session_store.clone(),
            tools.clone(),
            model_router.clone(),
            llm_provider.clone(),
            permission_manager.clone(),
            event_tx,
        ));

        // Initialize scheduler
        let scheduler = Arc::new(Scheduler::new(
            job_store.clone(),
            agent_runtime.clone(),
            outbound_tx.clone(),
        ));

        let agents = DashMap::new();
        agents.insert("main".to_string(), agent_runtime.clone());

        Ok(Self {
            agents,
            channels,
            scheduler,
            permission_manager,
            outbound_rx,
            outbound_tx,
            inbound_tx,
            inbound_rx,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        tracing::info!("Gateway starting");

        // Start channels
        let inbound_tx = self.inbound_tx.clone();
        for channel in &self.channels {
            let channel = channel.clone();
            let tx = inbound_tx.clone();
            tokio::spawn(async move {
                if let Err(e) = channel.start(tx).await {
                    tracing::error!(channel = channel.id(), error = %e, "Channel failed");
                }
            });
        }

        // Start scheduler
        let scheduler = self.scheduler.clone();
        tokio::spawn(async move {
            if let Err(e) = scheduler.start().await {
                tracing::error!(error = %e, "Scheduler failed");
            }
        });

        tracing::info!("Gateway started, processing messages");

        // Main message loop
        while let Some(message) = self.inbound_rx.recv().await {
            self.handle_inbound(message).await;
        }

        Ok(())
    }

    async fn handle_inbound(&self, message: InboundMessage) {
        let agent_id = self.resolve_agent(&message);

        if let Some(agent) = self.agents.get(&agent_id) {
            let request = AgentRequest {
                agent_id: agent_id.clone(),
                session_id: format!("{}:{}:{}", message.channel, message.peer_id, message.conversation_id.as_deref().unwrap_or("")),
                channel: message.channel.clone(),
                peer_id: message.peer_id.clone(),
                content: message.text.unwrap_or_default(),
                attachments: message.attachments,
                run_id: None,
                model_profile: None,
                cancel_token: None,
            };

            match agent.run(request).await {
                Ok(response) => {
                    if let Some(chat_id) = message.conversation_id {
                        let outbound = OutboundMessage {
                            channel: message.channel.clone(),
                            target: chat_id,
                            reply_to: Some(message.id),
                            text: response.content,
                            attachments: Vec::new(),
                            metadata: serde_json::json!({
                                "agent": agent_id,
                                "session": response.session_id,
                            }),
                        };

                        if let Err(e) = self.outbound_tx.send(outbound).await {
                            tracing::error!(error = %e, "Failed to send outbound message");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(agent = %agent_id, error = %e, "Agent run failed");
                }
            }
        } else {
            tracing::warn!(agent = %agent_id, "Agent not found");
        }
    }

    fn resolve_agent(&self, _message: &InboundMessage) -> String {
        // Simple resolution: default to "main"
        // TODO: Implement proper binding resolution
        "main".to_string()
    }

    pub async fn run_agent_direct(&self, agent_id: &str, prompt: &str) -> Result<AgentResponse> {
        let agent = self.agents.get(agent_id)
            .ok_or_else(|| anyhow::anyhow!("Agent '{}' not found", agent_id))?;

        let request = AgentRequest {
            agent_id: agent_id.to_string(),
            session_id: format!("cli:{}", agent_id),
            channel: "cli".to_string(),
            peer_id: "operator".to_string(),
            content: prompt.to_string(),
            attachments: Vec::new(),
            run_id: None,
            model_profile: None,
            cancel_token: None,
        };

        agent.run(request).await
    }
}
