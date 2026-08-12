use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;

use crate::database::approvals::{Approval, ApprovalStore};
use crate::tools::{PermissionMode, ToolPermission};

#[derive(Debug, Clone)]
pub struct PermissionConfig {
    pub agent_permissions: HashMap<String, HashMap<String, ToolPermission>>,
}

impl Default for PermissionConfig {
    fn default() -> Self {
        let mut agent_permissions = HashMap::new();

        // Default agent permissions
        let mut main_perms = HashMap::new();
        main_perms.insert("shell_exec".to_string(), ToolPermission {
            mode: PermissionMode::Ask,
            scopes: vec![],
        });
        main_perms.insert("fs_write".to_string(), ToolPermission {
            mode: PermissionMode::Ask,
            scopes: vec!["~/workspace/**".to_string()],
        });

        agent_permissions.insert("main".to_string(), main_perms);

        Self { agent_permissions }
    }
}

pub struct PermissionManager {
    approval_store: Arc<ApprovalStore>,
    config: PermissionConfig,
}

impl PermissionManager {
    pub fn new(approval_store: Arc<ApprovalStore>, config: PermissionConfig) -> Self {
        Self { approval_store, config }
    }

    pub fn check_permission(
        &self,
        agent_id: &str,
        tool_name: &str,
    ) -> PermissionMode {
        if let Some(agents) = self.config.agent_permissions.get(agent_id) {
            if let Some(perm) = agents.get(tool_name) {
                return perm.mode.clone();
            }
        }

        // Default: allow read-only, ask for write/exec/delete
        match tool_name {
            "shell_exec" | "fs_write" | "delete_file" => PermissionMode::Ask,
            _ => PermissionMode::Allow,
        }
    }

    pub async fn request_approval(
        &self,
        agent_id: &str,
        session_id: &str,
        tool_name: &str,
        arguments: &str,
    ) -> Result<Approval> {
        // Simple hash for dedup
        let args_hash = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            arguments.hash(&mut hasher);
            format!("{:x}", hasher.finish())
        };

        self.approval_store.request(
            agent_id,
            session_id,
            tool_name,
            arguments,
            &args_hash,
            300, // 5 minute expiry
        ).await
    }

    pub async fn wait_for_approval(
        &self,
        approval_id: &str,
    ) -> Result<PermissionMode> {
        let mut checks = 0;
        let max_checks = 60; // 5 minutes at 5s intervals

        loop {
            if checks >= max_checks {
                return Ok(PermissionMode::Deny);
            }

            if let Some(approval) = self.approval_store.get(approval_id).await? {
                match approval.status.as_str() {
                    "approved" => return Ok(PermissionMode::Allow),
                    "denied" | "expired" => return Ok(PermissionMode::Deny),
                    _ => {}
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            checks += 1;
        }
    }

    pub async fn approve(&self, id: &str) -> Result<bool> {
        self.approval_store.approve(id).await
    }

    pub async fn deny(&self, id: &str) -> Result<bool> {
        self.approval_store.deny(id).await
    }
}
