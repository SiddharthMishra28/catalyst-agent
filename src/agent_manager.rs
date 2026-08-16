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

/// Rules appended to every agent's system prompt: the user watches work in a
/// code editor panel, so code must live in files - never in chat text.
const IDE_WORKSPACE_RULES: &str = "\n\n## IDE workspace rules
The user is watching you work in a code editor panel (a file explorer + CodeMirror). Chat is for conversation only - code lives in files.

- ALWAYS put code, configs, scripts, and any file content into files using the tools: `write_file` to create or overwrite a file, `edit_file` for targeted find-and-replace edits, `delete_file` to remove files, `read_file` / `list_dir` to inspect, `git_status` / `git_diff` to review changes.
- NEVER paste code, file contents, configs, or diffs into your chat reply. Chat replies must be concise conversational updates: 1-4 short sentences saying what you did, what you plan next, or asking the user a question. No code blocks, no file dumps.
- Every file you write or edit appears automatically in the user's editor and is shown with a highlight, so you can just tell the user the file paths you touched instead of repeating their contents.
- When a task needs several files (e.g. a script plus its config), create them all with tools in the same run rather than describing them in chat.
- If the user asks to see code, write or open the relevant file with a tool and reference its path - do not paste the code in chat.
- Use `shell_exec` to run builds, tests, or the created files when verifying your work.

## Agentic work loop (open-code style)
Approach every task as an autonomous engineering agent, like opencode's build agent:

1. **Discover before acting.** Explore the workspace with `glob` and `grep` (e.g. `glob **/*.rs`, `grep pattern path=.`) and `read_file` / `list_dir` to understand existing code before writing anything. Never guess paths or duplicate logic that already exists.
2. **Plan, then track.** For multi-step tasks, first call `todo_list` with op=add for each step (or op=list to check state), then mark steps op=update status=in_progress / completed as you go. The user sees this list.
3. **Reason with context.** If you need external or current information (docs, APIs, versions), use `web_search` for results and `web_fetch` for pages - do not rely on possibly-outdated knowledge.
4. **Edit precisely.** Prefer `edit_file` with exact old_string/new_string for small targeted changes (after reading the file); use `write_file` for new or heavily changed files. Verify old_string uniqueness like opencode does - if ambiguous, read more context first.
5. **Verify your work.** After changing code, run builds/tests/scripts with `shell_exec` (e.g. `cargo check`, `npm test`, `python script.py`) and fix failures until it works. Check `git_status` / `git_diff` before and after to confirm only intended changes.
6. **Stay in scope.** Respect the permission gate: build/plan/ask behaviors are normal; destructive or long-running commands may require user approval - proceed when allowed, explain briefly when blocked.
7. **Keep chat concise.** These rules still apply while you work: chat stays conversational (1-4 short sentences); all code and file content goes through tools.";

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
            system_prompt: format!(
                "You are ClawRig, a coding agent that works inside a web IDE.{IDE_WORKSPACE_RULES}",
            ),
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
                    "You are {}, a specialized coding agent that works inside a web IDE.{IDE_WORKSPACE_RULES}",
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
