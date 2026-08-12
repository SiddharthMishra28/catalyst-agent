use anyhow::Result;
use dashmap::DashMap;
use std::sync::Arc;

use crate::agent::{AgentConfig, AgentRuntime};
use crate::config::Config;
use crate::database::approvals::ApprovalStore;
use crate::database::sessions::SessionStore;
use crate::llm::LlmProvider;
use crate::models::ModelRouter;
use crate::permissions::{PermissionConfig, PermissionManager};
use crate::tools::create_default_registry;

pub struct AgentManager {
    agents: DashMap<String, Arc<AgentRuntime>>,
}

impl AgentManager {
    pub fn new() -> Self {
        Self {
            agents: DashMap::new(),
        }
    }

    pub fn from_config(
        config: &Config,
        sessions: Arc<SessionStore>,
        model_router: Arc<ModelRouter>,
        llm_provider: Arc<LlmProvider>,
        approval_store: Arc<ApprovalStore>,
        event_tx: tokio::sync::broadcast::Sender<String>,
    ) -> Result<Self> {
        let manager = Self::new();
        let tools = Arc::new(create_default_registry());
        let permissions = Arc::new(PermissionManager::new(
            approval_store,
            PermissionConfig::default(),
        ));

        // Create default "main" agent
        let main_config = AgentConfig {
            name: "main".to_string(),
            system_prompt: "You are ClawRig, a helpful AI assistant with access to tools. Be concise and helpful.".to_string(),
            max_tool_rounds: 10,
            model_override: None,
            yolo_mode: config.agents.defaults.yolo,
        };
        let main_agent = Arc::new(AgentRuntime::new(
            main_config,
            sessions.clone(),
            tools.clone(),
            model_router.clone(),
            llm_provider.clone(),
            permissions.clone(),
            event_tx.clone(),
        ));
        manager.agents.insert("main".to_string(), main_agent);

        // Create agents from config
        for agent_def in &config.agents.agents {
            let agent_config = AgentConfig {
                name: agent_def.name.clone(),
                system_prompt: format!(
                    "You are {}, a specialized AI assistant.",
                    agent_def.name
                ),
                max_tool_rounds: 10,
                model_override: agent_def.model.clone(),
                yolo_mode: config.agents.defaults.yolo,
            };

            let agent = Arc::new(AgentRuntime::new(
                agent_config,
                sessions.clone(),
                tools.clone(),
                model_router.clone(),
                llm_provider.clone(),
                permissions.clone(),
                event_tx.clone(),
            ));

            manager.agents.insert(agent_def.name.clone(), agent);
        }

        Ok(manager)
    }

    pub fn get(&self, name: &str) -> Option<Arc<AgentRuntime>> {
        self.agents.get(name).map(|r| r.value().clone())
    }

    pub fn list(&self) -> Vec<String> {
        self.agents.iter().map(|r| r.key().clone()).collect()
    }

    pub fn count(&self) -> usize {
        self.agents.len()
    }
}
