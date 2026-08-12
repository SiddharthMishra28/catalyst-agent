use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{ToolContext, ToolHandler, ToolResult};

/// Execute a shell command
pub struct ShellExecTool;

fn rand_id() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    (nanos as u64) ^ ((std::process::id() as u64) << 32)
}

/// Unwrap a `powershell -Command "..."` wrapper if present, returning the inner expression
fn unwrap_powershell_wrapper(command: &str) -> Option<String> {
    let trimmed = command.trim();
    let re = regex::Regex::new(
        r#"(?i)^(?:pwsh|powershell)(?:\.exe)?\s+(?:-\w+\s+)*-Command\s+["']?(.*?)["']?\s*$"#,
    )
    .ok()?;
    if let Some(caps) = re.captures(trimmed) {
        if let Some(inner) = caps.get(1) {
            let inner = inner.as_str().trim();
            if !inner.is_empty() {
                return Some(inner.to_string());
            }
        }
    }
    None
}

/// Detect if a command is PowerShell syntax (and should be routed via a .ps1 file)
fn is_powershell_syntax(command: &str) -> bool {
    let re = regex::Regex::new(
        r#"(?i)\[System\.|Get-Date|ConvertTime|\.ToString\(|\$env:|\bGet-\w+\(|\bSet-\w+\(|\bWrite-\w+\(|\bNew-\w+\(|\bConvert-\w+\(|\bSelect-Object|\bMeasure-Object"#,
    )
    .ok();
    match re {
        Some(re) => re.is_match(command),
        None => false,
    }
}

#[async_trait]
impl ToolHandler for ShellExecTool {
    async fn execute(&self, _ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let command = input["command"]
            .as_str()
            .context("Missing 'command' parameter")?;

        let timeout_secs = input["timeout"]
            .as_u64()
            .unwrap_or(30)
            .min(300); // Max 5 minutes

        let working_dir = input["working_dir"]
            .as_str()
            .map(|s| std::path::PathBuf::from(s));

        // On Windows, route PowerShell syntax through a temp .ps1 file to avoid
        // cmd.exe nested-quoting issues (cmd /C mangles "powershell -Command \"...\"").
        let mut ps_cleanup_path: Option<std::path::PathBuf> = None;
        let mut cmd = if cfg!(target_os = "windows") {
            let inner = unwrap_powershell_wrapper(command)
                .filter(|c| is_powershell_syntax(c))
                .or_else(|| {
                    if is_powershell_syntax(command) {
                        Some(command.to_string())
                    } else {
                        None
                    }
                });

            if let Some(ps_cmd) = inner {
                let ps_path = std::env::temp_dir().join(format!("clawrig_{:x}.ps1", rand_id()));
                std::fs::write(&ps_path, ps_cmd)
                    .with_context(|| format!("Failed to write PS script: {}", ps_path.display()))?;
                ps_cleanup_path = Some(ps_path.clone());
                let mut c = std::process::Command::new("powershell");
                c.arg("-NoProfile")
                    .arg("-ExecutionPolicy")
                    .arg("Bypass")
                    .arg("-File")
                    .arg(&ps_path);
                if let Some(dir) = &working_dir {
                    c.current_dir(dir);
                }
                c
            } else {
                let mut c = std::process::Command::new("cmd");
                c.arg("/C").arg(command);
                if let Some(dir) = &working_dir {
                    c.current_dir(dir);
                }
                c
            }
        } else {
            let mut c = std::process::Command::new("sh");
            c.arg("-c").arg(command);
            if let Some(dir) = &working_dir {
                c.current_dir(dir);
            }
            c
        };

        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let child = cmd
            .spawn()
            .context(format!("Failed to spawn command: {}", command))?;

        let output = tokio::time::timeout(
            Duration::from_secs(timeout_secs),
            tokio::task::spawn_blocking(move || child.wait_with_output()),
        )
        .await
        .context(format!("Command timed out after {} seconds", timeout_secs))?
        .context("Failed to wait for command output")?
        .context("Command execution failed")?;

        if let Some(path) = ps_cleanup_path {
            let _ = std::fs::remove_file(path);
        }

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let exit_code = output.status.code().unwrap_or(-1);

        let success = output.status.success();
        let stdout_len = stdout.len();
        let stderr_len = stderr.len();
        let content = if stderr.is_empty() {
            stdout
        } else if stdout.is_empty() {
            stderr
        } else {
            format!("STDOUT:\n{}\n\nSTDERR:\n{}", stdout, stderr)
        };

        Ok(ToolResult {
            success,
            content,
            metadata: Some(json!({
                "command": command,
                "exit_code": exit_code,
                "stdout_len": stdout_len,
                "stderr_len": stderr_len,
            })),
        })
    }
}
