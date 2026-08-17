use anyhow::{Context, Result};
use async_trait::async_trait;
use regex::Regex;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

use super::{ToolContext, ToolHandler, ToolResult};

const SKIP_DIRS: &[&str] = &[
    ".git", "node_modules", "target", "dist", "build", ".venv", "venv",
    "__pycache__", ".next", ".cache", ".idea", ".vscode",
];

const MAX_DEPTH: usize = 10;

/// Convert a glob-style pattern (e.g. `**/*.rs`, `*.json`, `src/**`) into a regex.
/// `*` matches any chars except `/`, `**` matches across `/`, `?` matches one char.
fn glob_to_regex(pattern: &str) -> Result<Regex> {
    let mut out = String::from("^");
    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '*' => {
                if i + 1 < chars.len() && chars[i + 1] == '*' {
                    out.push_str(".*");
                    i += 2;
                    // allow trailing slashes after ** (so `a/**` also matches `a/`)
                    if i < chars.len() && chars[i] == '/' {
                        out.push_str("/?");
                        i += 1;
                    }
                    continue;
                } else {
                    out.push_str("[^/]*");
                    i += 1;
                }
            }
            '?' => {
                out.push_str("[^/]");
                i += 1;
            }
            '.' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' | '\\' => {
                out.push('\\');
                out.push(c);
                i += 1;
            }
            _ => {
                out.push(c);
                i += 1;
            }
        }
    }
    out.push('$');
    Regex::new(&out).context("Invalid glob pattern")
}

fn walk_files(root: &Path, max_depth: usize, skip: &[&str]) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack: Vec<(PathBuf, usize)> = vec![(root.to_path_buf(), 0)];

    while let Some((dir, depth)) = stack.pop() {
        if depth >= max_depth {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&dir) else { continue };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(ft) = entry.file_type() else { continue };
            if ft.is_dir() {
                let name = entry.file_name().to_string_lossy().to_string();
                if skip.contains(&name.as_str()) {
                    continue;
                }
                stack.push((path, depth + 1));
            } else if ft.is_file() {
                files.push(path);
            }
        }
    }

    files.sort();
    files
}

/// Find files matching a glob pattern (like the `glob` tool in opencode)
pub struct GlobTool;

#[async_trait]
impl ToolHandler for GlobTool {
    async fn execute(&self, ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let pattern = input["pattern"]
            .as_str()
            .context("Missing 'pattern' parameter")?;

        let root = match input["path"].as_str() {
            Some(p) => PathBuf::from(p),
            None => ctx.workspace_dir.clone(),
        };
        if !root.exists() {
            return Ok(ToolResult {
                success: false,
                content: format!("Directory not found: {}", root.display()),
                metadata: None,
            });
        }

        let regex = glob_to_regex(pattern)?;
        let max_results = input["max_results"].as_u64().unwrap_or(200) as usize;

        let files = walk_files(&root, MAX_DEPTH, SKIP_DIRS);
        let mut matches: Vec<String> = Vec::new();

        for file in files {
            let rel = file
                .strip_prefix(&root)
                .unwrap_or(&file)
                .to_string_lossy()
                .replace('\\', "/");
            let name = file
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            let haystack = if pattern.contains('/') { rel.clone() } else { name };
            if regex.is_match(&haystack) {
                matches.push(rel);
                if matches.len() >= max_results {
                    break;
                }
            }
        }

        let total = matches.len();
        let truncated = matches.len() >= max_results;
        let content = if matches.is_empty() {
            "No files matched pattern".to_string()
        } else {
            matches.join("\n")
        };

        Ok(ToolResult {
            success: !matches.is_empty(),
            content,
            metadata: Some(json!({
                "pattern": pattern,
                "root": root.display().to_string(),
                "count": total,
                "truncated": truncated,
            })),
        })
    }
}

/// Search file contents with a regex (like the `grep` tool in opencode)
pub struct GrepTool;

#[async_trait]
impl ToolHandler for GrepTool {
    async fn execute(&self, ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let pattern = input["pattern"]
            .as_str()
            .context("Missing 'pattern' parameter")?;
        let regex = Regex::new(pattern).context("Invalid regex pattern")?;

        let root = match input["path"].as_str() {
            Some(p) => PathBuf::from(p),
            None => ctx.workspace_dir.clone(),
        };
        if !root.exists() {
            return Ok(ToolResult {
                success: false,
                content: format!("Directory not found: {}", root.display()),
                metadata: None,
            });
        }

        let include_filter = input["include"].as_str();
        let include_regex = match include_filter {
            Some(inc) => Some(glob_to_regex(inc)?),
            None => None,
        };

        let max_results = input["max_results"].as_u64().unwrap_or(100) as usize;
        let show_line_numbers = input["line_numbers"].as_bool().unwrap_or(true);

        let files = walk_files(&root, MAX_DEPTH, SKIP_DIRS);
        let mut hits: Vec<String> = Vec::new();
        let mut hit_files: Vec<String> = Vec::new();

        'outer: for file in files {
            let rel = file
                .strip_prefix(&root)
                .unwrap_or(&file)
                .to_string_lossy()
                .replace('\\', "/");
            let name = file
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            if let Some(re) = &include_regex {
                let hay = if include_filter.map_or(false, |i| i.contains('/')) { rel.clone() } else { name.clone() };
                if !re.is_match(&hay) {
                    continue;
                }
            }

            let Ok(bytes) = std::fs::read(&file) else { continue };
            if bytes.contains(&0u8) {
                continue;
            }
            let text = String::from_utf8_lossy(&bytes);

            for (idx, line) in text.lines().enumerate() {
                if regex.is_match(line) {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    hits.push(if show_line_numbers {
                        format!("{}:{}: {}", rel, idx + 1, trimmed)
                    } else {
                        format!("{}: {}", rel, trimmed)
                    });
                    if !hit_files.contains(&rel) {
                        hit_files.push(rel.clone());
                    }
                    if hits.len() >= max_results {
                        break 'outer;
                    }
                }
            }
        }

        hits.truncate(max_results);

        let content = if hits.is_empty() {
            "No matches found".to_string()
        } else {
            hits.join("\n")
        };

        Ok(ToolResult {
            success: !hits.is_empty(),
            content,
            metadata: Some(json!({
                "pattern": pattern,
                "root": root.display().to_string(),
                "matches": hits.len(),
                "files": hit_files.len(),
            })),
        })
    }
}