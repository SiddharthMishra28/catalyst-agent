use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::path::PathBuf;

use super::{ToolContext, ToolHandler, ToolResult};

/// Read file contents
pub struct ReadFileTool;

#[async_trait]
impl ToolHandler for ReadFileTool {
    async fn execute(&self, _ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let path = input["path"]
            .as_str()
            .context("Missing 'path' parameter")?;

        let path = PathBuf::from(path);
        if !path.exists() {
            return Ok(ToolResult {
                success: false,
                content: format!("File not found: {}", path.display()),
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
                "path": path.display().to_string(),
                "size": content_len,
            })),
        })
    }
}

/// Write content to a file
pub struct WriteFileTool;

#[async_trait]
impl ToolHandler for WriteFileTool {
    async fn execute(&self, _ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let path = input["path"]
            .as_str()
            .context("Missing 'path' parameter")?;

        let content = input["content"]
            .as_str()
            .context("Missing 'content' parameter")?;

        let path = PathBuf::from(path);

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
            content: format!("File written: {}", path.display()),
            metadata: Some(json!({
                "path": path.display().to_string(),
                "bytes_written": content.len(),
            })),
        })
    }
}

/// List directory contents
pub struct ListDirTool;

#[async_trait]
impl ToolHandler for ListDirTool {
    async fn execute(&self, _ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let path = input["path"]
            .as_str()
            .unwrap_or(".");

        let path = PathBuf::from(path);
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
                "path": path.display().to_string(),
                "count": entries.len(),
            })),
        })
    }
}

/// Delete a file
pub struct DeleteFileTool;

#[async_trait]
impl ToolHandler for DeleteFileTool {
    async fn execute(&self, _ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let path = input["path"]
            .as_str()
            .context("Missing 'path' parameter")?;

        let path = PathBuf::from(path);
        if !path.exists() {
            return Ok(ToolResult {
                success: false,
                content: format!("File not found: {}", path.display()),
                metadata: None,
            });
        }

        if path.is_dir() {
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
            content: format!("Deleted: {}", path.display()),
            metadata: Some(json!({
                "path": path.display().to_string(),
                "was_dir": path.is_dir(),
            })),
        })
    }
}
