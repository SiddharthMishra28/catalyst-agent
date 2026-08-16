use anyhow::{Context, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use serde_json::{json, Value};
use std::sync::OnceLock;

use super::{ToolContext, ToolHandler, ToolResult};

/// Per-session todo list state (like the `todowrite` tool in opencode).
/// State is in-memory and scoped to a chat session.
static TODOS: OnceLock<DashMap<String, Vec<Value>>> = OnceLock::new();

fn todos() -> &'static DashMap<String, Vec<Value>> {
    TODOS.get_or_init(DashMap::new)
}

fn list_todos(agent_id: &str) -> Vec<Value> {
    todos()
        .get(agent_id)
        .map(|v| v.value().clone())
        .unwrap_or_default()
}

fn save_todos(agent_id: &str, items: Vec<Value>) {
    todos().insert(agent_id.to_string(), items);
}

fn next_id(items: &[Value]) -> String {
    let max = items
        .iter()
        .filter_map(|i| i["id"].as_str().and_then(|s| s.parse::<u64>().ok()))
        .max()
        .unwrap_or(0);
    (max + 1).to_string()
}

/// Manage a per-session todo list: add, list, update status, remove, clear.
/// Use for multi-step tasks so the plan and progress are tracked.
pub struct TodoListTool;

#[async_trait]
impl ToolHandler for TodoListTool {
    async fn execute(&self, ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let operation = input["op"]
            .as_str()
            .unwrap_or("list")
            .to_lowercase();

        let mut items = list_todos(&ctx.agent_id);

        match operation.as_str() {
            "add" => {
                let content = input["content"]
                    .as_str()
                    .context("Missing 'content' parameter for add")?;
                items.push(json!({
                    "id": next_id(&items),
                    "content": content,
                    "status": input["status"].as_str().unwrap_or("pending"),
                }));
                save_todos(&ctx.agent_id, items.clone());
                let entry = items.last().unwrap().clone();
                Ok(ToolResult {
                    success: true,
                    content: format!(
                        "Added todo #{}: {} [{}]",
                        entry["id"].as_str().unwrap_or(""),
                        entry["content"].as_str().unwrap_or(""),
                        entry["status"].as_str().unwrap_or("")
                    ),
                    metadata: Some(json!({
                        "op": "add",
                        "item": entry,
                        "count": items.len(),
                    })),
                })
            }
            "update" => {
                let id = input["id"].as_str().context("Missing 'id' parameter for update")?;
                let Some(item) = items.iter_mut().find(|i| i["id"].as_str() == Some(id)) else {
                    return Ok(ToolResult {
                        success: false,
                        content: format!("Todo #{} not found", id),
                        metadata: None,
                    });
                };
                if let Some(status) = input["status"].as_str() {
                    item["status"] = Value::String(status.to_string());
                }
                if let Some(content) = input["content"].as_str() {
                    item["content"] = Value::String(content.to_string());
                }
                let item_snapshot = item.clone();
                let item_id = item["id"].as_str().unwrap_or("").to_string();
                let item_content = item["content"].as_str().unwrap_or("").to_string();
                let item_status = item["status"].as_str().unwrap_or("").to_string();
                save_todos(&ctx.agent_id, items.clone());
                Ok(ToolResult {
                    success: true,
                    content: format!("Todo #{}: {} [{}]", item_id, item_content, item_status),
                    metadata: Some(json!({ "op": "update", "item": item_snapshot })),
                })
            }
            "remove" => {
                let id = input["id"].as_str().context("Missing 'id' parameter for remove")?;
                let before = items.len();
                items.retain(|i| i["id"].as_str() != Some(id));
                save_todos(&ctx.agent_id, items.clone());
                Ok(ToolResult {
                    success: before != items.len(),
                    content: if before != items.len() {
                        format!("Removed todo #{}", id)
                    } else {
                        format!("Todo #{} not found", id)
                    },
                    metadata: Some(json!({ "op": "remove", "count": items.len() })),
                })
            }
            "clear" => {
                save_todos(&ctx.agent_id, Vec::new());
                Ok(ToolResult {
                    success: true,
                    content: "Cleared all todos".to_string(),
                    metadata: Some(json!({ "op": "clear", "count": 0 })),
                })
            }
            "list" => {
                if items.is_empty() {
                    return Ok(ToolResult {
                        success: true,
                        content: "No todos yet. Use op=add to create one.".to_string(),
                        metadata: Some(json!({ "op": "list", "count": 0 })),
                    });
                }
                let lines = items
                    .iter()
                    .map(|i| {
                        format!(
                            "- [{}] #{}: {}",
                            i["status"].as_str().unwrap_or("pending"),
                            i["id"].as_str().unwrap_or(""),
                            i["content"].as_str().unwrap_or("")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                Ok(ToolResult {
                    success: true,
                    content: lines,
                    metadata: Some(json!({ "op": "list", "count": items.len() })),
                })
            }
            other => Ok(ToolResult {
                success: false,
                content: format!(
                    "Unknown operation '{}'. Valid ops: list, add, update, remove, clear",
                    other
                ),
                metadata: None,
            }),
        }
    }
}