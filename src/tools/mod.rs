pub mod code;
pub mod filesystem;
pub mod search;
pub mod shell;
pub mod todos;
pub mod web;

use anyhow::Result;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ToolContext {
    pub agent_id: String,
    pub session_id: String,
    pub channel: String,
    pub peer_id: String,
    /// Scratch workspace for this session: the temp folder the agent's
    /// generated code is written into. Relative paths resolve against it.
    pub workspace_dir: std::path::PathBuf,
}

#[derive(Debug, Clone)]
pub struct ToolResult {
    pub success: bool,
    pub content: String,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct ToolSchema {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PermissionMode {
    Deny,
    Allow,
    Ask,
    Auto,
}

#[derive(Debug, Clone)]
pub struct ToolPermission {
    pub mode: PermissionMode,
    pub scopes: Vec<String>,
}

#[derive(Clone)]
pub struct ToolEntry {
    pub schema: ToolSchema,
    pub permission: ToolPermission,
    pub handler: Arc<dyn ToolHandler>,
}

#[async_trait::async_trait]
pub trait ToolHandler: Send + Sync {
    async fn execute(&self, ctx: &ToolContext, input: Value) -> Result<ToolResult>;
}

pub struct ToolRegistry {
    tools: dashmap::DashMap<String, ToolEntry>,
    categories: dashmap::DashMap<String, Vec<String>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: dashmap::DashMap::new(),
            categories: dashmap::DashMap::new(),
        }
    }

    pub fn register(
        &self,
        schema: ToolSchema,
        permission: ToolPermission,
        handler: Arc<dyn ToolHandler>,
        category: &str,
    ) {
        let name = schema.name.clone();
        self.tools.insert(name.clone(), ToolEntry {
            schema,
            permission,
            handler,
        });

        self.categories
            .entry(category.to_string())
            .or_default()
            .push(name);
    }

    pub fn get(&self, name: &str) -> Option<ToolEntry> {
        self.tools.get(name).map(|e| e.value().clone())
    }

    pub fn get_schemas(&self) -> Vec<ToolSchema> {
        self.tools.iter().map(|e| e.value().schema.clone()).collect()
    }

    pub fn get_schemas_for_task(&self, categories: &[&str]) -> Vec<ToolSchema> {
        let mut names = Vec::new();
        for cat in categories {
            if let Some(tools) = self.categories.get(*cat) {
                names.extend(tools.iter().cloned());
            }
        }

        names.iter()
            .filter_map(|name| self.tools.get(name).map(|e| e.value().schema.clone()))
            .collect()
    }

    pub fn search(&self, query: &str) -> Vec<ToolSchema> {
        let query_lower = query.to_lowercase();
        self.tools.iter()
            .filter(|e| {
                let schema = &e.value().schema;
                schema.name.to_lowercase().contains(&query_lower)
                    || schema.description.to_lowercase().contains(&query_lower)
            })
            .map(|e| e.value().schema.clone())
            .collect()
    }

    pub fn list_categories(&self) -> Vec<String> {
        self.categories.iter().map(|e| e.key().clone()).collect()
    }

    pub fn check_permission(&self, tool_name: &str, agent_permissions: &HashMap<String, ToolPermission>) -> PermissionMode {
        // Check agent-specific permission first
        if let Some(perm) = agent_permissions.get(tool_name) {
            return perm.mode.clone();
        }

        // Check tool default
        if let Some(entry) = self.tools.get(tool_name) {
            return entry.permission.mode.clone();
        }

        PermissionMode::Deny
    }

    pub fn count(&self) -> usize {
        self.tools.len()
    }
}

/// Create a ToolRegistry with all default built-in tools
pub fn create_default_registry() -> ToolRegistry {
    let registry = ToolRegistry::new();

    let allow_permission = ToolPermission {
        mode: PermissionMode::Allow,
        scopes: vec![],
    };

    // Filesystem tools
    registry.register(
        ToolSchema {
            name: "read_file".to_string(),
            description: "Read the contents of a file".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path to the file to read"
                    }
                },
                "required": ["path"]
            }),
        },
        allow_permission.clone(),
        Arc::new(filesystem::ReadFileTool),
        "filesystem",
    );

    registry.register(
        ToolSchema {
            name: "write_file".to_string(),
            description: "Write content to a file (creates parent directories if needed)".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path to the file to write"
                    },
                    "content": {
                        "type": "string",
                        "description": "Content to write to the file"
                    }
                },
                "required": ["path", "content"]
            }),
        },
        allow_permission.clone(),
        Arc::new(filesystem::WriteFileTool),
        "filesystem",
    );

    registry.register(
        ToolSchema {
            name: "list_dir".to_string(),
            description: "List the contents of a directory".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path to the directory (defaults to current directory)"
                    }
                }
            }),
        },
        allow_permission.clone(),
        Arc::new(filesystem::ListDirTool),
        "filesystem",
    );

    registry.register(
        ToolSchema {
            name: "delete_file".to_string(),
            description: "Delete a file or directory".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path to the file or directory to delete"
                    }
                },
                "required": ["path"]
            }),
        },
        allow_permission.clone(),
        Arc::new(filesystem::DeleteFileTool),
        "filesystem",
    );

    // Shell tool
    registry.register(
        ToolSchema {
            name: "shell_exec".to_string(),
            description: "Execute a shell command and return its output. On Windows the command runs via cmd.exe (cmd /C) using Windows syntax (dir, type, echo %VAR%); on Unix it runs via sh -c".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The shell command to execute"
                    },
                    "working_dir": {
                        "type": "string",
                        "description": "Working directory for the command (optional)"
                    },
                    "timeout": {
                        "type": "integer",
                        "description": "Timeout in seconds (default: 30, max: 300)"
                    }
                },
                "required": ["command"]
            }),
        },
        ToolPermission {
            mode: PermissionMode::Ask,
            scopes: vec![],
        },
        Arc::new(shell::ShellExecTool),
        "shell",
    );

    // Web tool
    registry.register(
        ToolSchema {
            name: "web_fetch".to_string(),
            description: "Fetch content from a URL".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "URL to fetch"
                    },
                    "max_length": {
                        "type": "integer",
                        "description": "Maximum content length to return (default: 50000)"
                    }
                },
                "required": ["url"]
            }),
        },
        allow_permission.clone(),
        Arc::new(web::WebFetchTool),
        "web",
    );

    registry.register(
        ToolSchema {
            name: "web_search".to_string(),
            description: "Search the web with a text query (DuckDuckGo, no API key needed). Returns up to 20 result titles, URLs and snippets. Use this when you need current, external information the user did not provide. Follow up with web_fetch to read a full page.".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query"
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum results to return (default: 8, max: 20)"
                    }
                },
                "required": ["query"]
            }),
        },
        allow_permission.clone(),
        Arc::new(web::WebSearchTool),
        "web",
    );

    // Search tools
    registry.register(
        ToolSchema {
            name: "glob".to_string(),
            description: "Find files by glob pattern, e.g. `**/*.rs` or `src/**` or `*.toml`. `*` matches within one path segment, `**` matches across directories, `?` matches a single char. Patterns without a `/` match against file names at any depth. Use before reading or editing to discover exact paths.".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Glob pattern to match files"
                    },
                    "path": {
                        "type": "string",
                        "description": "Root directory to search (defaults to current directory)"
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum results (default: 200)"
                    }
                },
                "required": ["pattern"]
            }),
        },
        allow_permission.clone(),
        Arc::new(search::GlobTool),
        "search",
    );

    registry.register(
        ToolSchema {
            name: "grep".to_string(),
            description: "Search file contents with a regex pattern. Returns `path:line: content` matches. Skips binary files and common dependency directories (.git, node_modules, target, etc.). Use include to limit by filename glob (e.g. `*.rs`). Use before editing to find the exact locations to change.".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Regex pattern to search for"
                    },
                    "path": {
                        "type": "string",
                        "description": "Root directory to search (defaults to current directory)"
                    },
                    "include": {
                        "type": "string",
                        "description": "Only search files matching this glob, e.g. `*.rs`"
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum matches (default: 100)"
                    }
                },
                "required": ["pattern"]
            }),
        },
        allow_permission.clone(),
        Arc::new(search::GrepTool),
        "search",
    );

    // Todo tool
    registry.register(
        ToolSchema {
            name: "todo_list".to_string(),
            description: "Manage a per-session todo list for multi-step tasks. Operations: add (with content), list, update (set status, e.g. in_progress/completed, by id), remove (by id), clear. Create a todo list whenever a task has several steps so the user can follow your plan and progress.".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "op": {
                        "type": "string",
                        "enum": ["list", "add", "update", "remove", "clear"],
                        "description": "Operation to perform"
                    },
                    "content": {
                        "type": "string",
                        "description": "Todo item text (required for op=add)"
                    },
                    "id": {
                        "type": "string",
                        "description": "Todo item id (required for op=update and op=remove)"
                    },
                    "status": {
                        "type": "string",
                        "enum": ["pending", "in_progress", "completed"],
                        "description": "New status for op=update"
                    }
                },
                "required": ["op"]
            }),
        },
        allow_permission.clone(),
        Arc::new(todos::TodoListTool),
        "todo",
    );

    // Coding tools
    registry.register(
        ToolSchema {
            name: "edit_file".to_string(),
            description: "Apply a targeted find-and-replace edit to a file. Provide old_string (exact text to replace) and new_string (replacement). If old_string is empty, new_string is appended to the end of the file. Use read_file first to get exact contents. Fails if old_string is not found or is ambiguous.".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path to the file to edit"
                    },
                    "old_string": {
                        "type": "string",
                        "description": "Exact text to find and replace (empty to append)"
                    },
                    "new_string": {
                        "type": "string",
                        "description": "Replacement text"
                    }
                },
                "required": ["path", "new_string"]
            }),
        },
        allow_permission.clone(),
        Arc::new(code::EditFileTool),
        "coding",
    );

    registry.register(
        ToolSchema {
            name: "git_status".to_string(),
            description: "Show the git working tree status (short format with branch) for a repository. Use before and after edits to see what changed.".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path to the git repository or a file inside it (defaults to current directory)"
                    }
                }
            }),
        },
        allow_permission.clone(),
        Arc::new(code::GitStatusTool),
        "coding",
    );

    registry.register(
        ToolSchema {
            name: "git_diff".to_string(),
            description: "Show the current git diff (unstaged by default; set staged=true for the staged diff)".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path to the git repository or a file inside it (defaults to current directory)"
                    },
                    "staged": {
                        "type": "boolean",
                        "description": "Show the staged diff instead (default: false)"
                    }
                }
            }),
        },
        allow_permission.clone(),
        Arc::new(code::GitDiffTool),
        "coding",
    );

    registry.register(
        ToolSchema {
            name: "git_commit".to_string(),
            description: "Stage all changes (git add -A) and create a commit with the given message".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path to the git repository (defaults to current directory)"
                    },
                    "message": {
                        "type": "string",
                        "description": "Commit message"
                    }
                },
                "required": ["message"]
            }),
        },
        ToolPermission {
            mode: PermissionMode::Ask,
            scopes: vec![],
        },
        Arc::new(code::GitCommitTool),
        "coding",
    );

    registry
}
