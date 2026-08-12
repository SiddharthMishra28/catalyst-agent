use anyhow::{Context, Result};
use serde_json::Value;
use tokio::process::Command;

use super::{ToolContext, ToolHandler, ToolResult};

/// Apply a targeted find-and-replace edit to a file.
/// If `old_string` is empty, `new_string` is appended to the file.
pub struct EditFileTool;

#[async_trait::async_trait]
impl ToolHandler for EditFileTool {
    async fn execute(&self, _ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let path = input
            .get("path")
            .and_then(Value::as_str)
            .context("Missing required parameter: path")?;
        let new_string = input
            .get("new_string")
            .and_then(Value::as_str)
            .context("Missing required parameter: new_string")?;
        let old_string = input.get("old_string").and_then(Value::as_str).unwrap_or("");

        let content = tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("Failed to read file: {}", path))?;

        let (updated, changed) = if old_string.is_empty() {
            (format!("{}{}", content, new_string), true)
        } else {
            let matches: Vec<_> = content.match_indices(old_string).collect();
            if matches.is_empty() {
                return Ok(ToolResult {
                    success: false,
                    content: format!(
                        "old_string not found in {} ({} chars searched). Use read_file first to see exact contents.",
                        path,
                        content.chars().count()
                    ),
                    metadata: None,
                });
            }
            if matches.len() > 1 {
                return Ok(ToolResult {
                    success: false,
                    content: format!(
                        "old_string found {} times in {}. Include more surrounding context to make it unique.",
                        matches.len(),
                        path
                    ),
                    metadata: None,
                });
            }
            let start = matches[0].0;
            let end = start + old_string.len();
            let mut updated = content.clone();
            updated.replace_range(start..end, new_string);
            (updated, true)
        };

        if !changed {
            return Ok(ToolResult {
                success: false,
                content: "No change was made".to_string(),
                metadata: None,
            });
        }

        tokio::fs::write(path, updated)
            .await
            .with_context(|| format!("Failed to write file: {}", path))?;

        Ok(ToolResult {
            success: true,
            content: format!(
                "Edited {}: replaced {} chars with {} chars ({} line total).",
                path,
                old_string.chars().count(),
                new_string.chars().count(),
                content.lines().count()
            ),
            metadata: Some(serde_json::json!({ "path": path })),
        })
    }
}

/// Show git working tree status (short format with branch).
pub struct GitStatusTool;

#[async_trait::async_trait]
impl ToolHandler for GitStatusTool {
    async fn execute(&self, _ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let path = repo_dir(input.get("path").and_then(Value::as_str)).await?;
        let output = run_git(&path, &["status", "--short", "--branch"]).await?;
        Ok(ToolResult {
            success: true,
            content: if output.is_empty() {
                format!("Git repository at {} is clean.", path.display())
            } else {
                format!("Git status for {}:\n{}", path.display(), output)
            },
            metadata: None,
        })
    }
}

/// Show the current git diff (unstaged unless `staged` is true).
pub struct GitDiffTool;

#[async_trait::async_trait]
impl ToolHandler for GitDiffTool {
    async fn execute(&self, _ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let path = repo_dir(input.get("path").and_then(Value::as_str)).await?;
        let staged = input.get("staged").and_then(Value::as_bool).unwrap_or(false);

        let mut args = vec!["diff"];
        if staged {
            args.push("--staged");
        }
        args.push("--stat");

        let stat = run_git(&path, &args).await?;
        let full_args = if staged {
            vec!["diff", "--staged"]
        } else {
            vec!["diff"]
        };
        let diff = run_git(&path, &full_args).await?;

        let max_diff = 30000;
        let mut content = format!("Diff stat:\n{}\n\n", stat);
        if diff.is_empty() {
            content.push_str("(no changes)");
        } else {
            let truncated = diff.chars().count() > max_diff;
            content.push_str(&diff.chars().take(max_diff).collect::<String>());
            if truncated {
                content.push_str("\n... [diff truncated]");
            }
        }
        Ok(ToolResult {
            success: true,
            content,
            metadata: None,
        })
    }
}

/// Stage all changes and commit them with the given message.
pub struct GitCommitTool;

#[async_trait::async_trait]
impl ToolHandler for GitCommitTool {
    async fn execute(&self, _ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let path = repo_dir(input.get("path").and_then(Value::as_str)).await?;
        let message = input
            .get("message")
            .and_then(Value::as_str)
            .context("Missing required parameter: message")?;

        let add_out = run_git(&path, &["add", "-A"]).await?;
        let commit_out = run_git(&path, &["commit", "-m", message]).await?;

        let short = run_git(&path, &["log", "-1", "--oneline"]).await?;

        Ok(ToolResult {
            success: true,
            content: format!(
                "Staged: {}\nCommitted: {}\n{}",
                if add_out.is_empty() { "(nothing to stage)" } else { add_out.trim() },
                commit_out.trim(),
                short.trim()
            ),
            metadata: None,
        })
    }
}

/// Determine the git repository root: explicit path arg, or the agent's working directory.
async fn repo_dir(path_arg: Option<&str>) -> Result<std::path::PathBuf> {
    if let Some(p) = path_arg {
        let dir = std::path::PathBuf::from(p);
        if dir.is_file() {
            return dir
                .parent()
                .map(|d| d.to_path_buf())
                .context("Could not determine directory from path");
        }
        return Ok(dir);
    }
    std::env::current_dir().context("Failed to resolve current directory")
}

async fn run_git(dir: &std::path::Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .await
        .with_context(|| format!("Failed to run git {:?} in {}", args, dir.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        return Err(anyhow::anyhow!(
            "git {:?} failed (exit {:?}): {}",
            args,
            output.status.code(),
            stderr.trim()
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    Ok(stdout)
}
