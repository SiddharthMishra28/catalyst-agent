use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};

use super::{ToolContext, ToolHandler, ToolResult};

/// Resolve a tool-supplied path: relative paths land inside the session's
/// workspace folder, absolute paths are honored as-is.
fn resolve_path(ctx: &ToolContext, path_arg: &str) -> std::path::PathBuf {
    let p = std::path::PathBuf::from(path_arg);
    if p.is_absolute() {
        p
    } else {
        ctx.workspace_dir.join(p)
    }
}

/// Path shown to the user/LLM: relative to the session workspace when inside it.
fn display_path(ctx: &ToolContext, p: &std::path::Path) -> String {
    p.strip_prefix(&ctx.workspace_dir)
        .unwrap_or(p)
        .to_string_lossy()
        .replace('\\', "/")
}

/// Read file contents
pub struct ReadFileTool;

#[async_trait]
impl ToolHandler for ReadFileTool {
    async fn execute(&self, ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let path = input["path"]
            .as_str()
            .context("Missing 'path' parameter")?;

        let path = resolve_path(ctx, path);
        if !path.exists() {
            return Ok(ToolResult {
                success: false,
                content: format!("File not found: {}", display_path(ctx, &path)),
                metadata: None,
            });
        }

        let content = tokio::fs::read_to_string(&path)
            .await
            .context(format!("Failed to read file: {}", path.display()))?;

        let content_len = content.len();
        Ok(ToolResult {
            success: true,
            content,
            metadata: Some(json!({
                "path": display_path(ctx, &path),
                "size": content_len,
            })),
        })
    }
}

/// Write content to a file
pub struct WriteFileTool;

#[async_trait]
impl ToolHandler for WriteFileTool {
    async fn execute(&self, ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let path = input["path"]
            .as_str()
            .context("Missing 'path' parameter")?;

        let content = input["content"]
            .as_str()
            .context("Missing 'content' parameter")?;

        let path = resolve_path(ctx, path);

        // Create parent directories if they don't exist
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .context(format!("Failed to create directories for: {}", path.display()))?;
            }
        }

        tokio::fs::write(&path, content)
            .await
            .context(format!("Failed to write file: {}", path.display()))?;

        Ok(ToolResult {
            success: true,
            content: format!("File written: {}", display_path(ctx, &path)),
            metadata: Some(json!({
                "path": display_path(ctx, &path),
                "bytes_written": content.len(),
            })),
        })
    }
}

/// List directory contents
pub struct ListDirTool;

#[async_trait]
impl ToolHandler for ListDirTool {
    async fn execute(&self, ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let path = input["path"]
            .as_str()
            .unwrap_or(".");

        let path = resolve_path(ctx, path);
        if !path.exists() {
            return Ok(ToolResult {
                success: false,
                content: format!("Directory not found: {}", path.display()),
                metadata: None,
            });
        }

        let mut entries = Vec::new();
        let mut read_dir = tokio::fs::read_dir(&path)
            .await
            .context(format!("Failed to read directory: {}", path.display()))?;

        while let Some(entry) = read_dir
            .next_entry()
            .await
            .context("Failed to read directory entry")?
        {
            let file_type = entry.file_type().await.ok();
            let name = entry.file_name().to_string_lossy().to_string();
            let is_dir = file_type.map(|ft| ft.is_dir()).unwrap_or(false);

            entries.push(json!({
                "name": name,
                "is_dir": is_dir,
                "path": entry.path().display().to_string(),
            }));
        }

        // Sort: directories first, then alphabetically
        entries.sort_by(|a, b| {
            let a_dir = a["is_dir"].as_bool().unwrap_or(false);
            let b_dir = b["is_dir"].as_bool().unwrap_or(false);
            b_dir.cmp(&a_dir).then_with(|| {
                a["name"]
                    .as_str()
                    .unwrap_or("")
                    .cmp(b["name"].as_str().unwrap_or(""))
            })
        });

        let listing = entries
            .iter()
            .map(|e| {
                let name = e["name"].as_str().unwrap_or("");
                if e["is_dir"].as_bool().unwrap_or(false) {
                    format!("{}/", name)
                } else {
                    name.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n");

        Ok(ToolResult {
            success: true,
            content: if listing.is_empty() {
                "(empty directory)".to_string()
            } else {
                listing
            },
            metadata: Some(json!({
                "path": display_path(ctx, &path),
                "count": entries.len(),
            })),
        })
    }
}

/// Delete a file
pub struct DeleteFileTool;

#[async_trait]
impl ToolHandler for DeleteFileTool {
    async fn execute(&self, ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let path = input["path"]
            .as_str()
            .context("Missing 'path' parameter")?;

        let path = resolve_path(ctx, path);
        if !path.exists() {
            return Ok(ToolResult {
                success: false,
                content: format!("File not found: {}", display_path(ctx, &path)),
                metadata: None,
            });
        }

        let was_dir = path.is_dir();
        if was_dir {
            tokio::fs::remove_dir_all(&path)
                .await
                .context(format!("Failed to delete directory: {}", path.display()))?;
        } else {
            tokio::fs::remove_file(&path)
                .await
                .context(format!("Failed to delete file: {}", path.display()))?;
        }

        Ok(ToolResult {
            success: true,
            content: format!("Deleted: {}", display_path(ctx, &path)),
            metadata: Some(json!({
                "path": display_path(ctx, &path),
                "was_dir": was_dir,
            })),
        })
    }
}
