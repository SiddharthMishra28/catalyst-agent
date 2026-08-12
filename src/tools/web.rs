use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};

use super::{ToolContext, ToolHandler, ToolResult};

/// Fetch content from a URL
pub struct WebFetchTool;

#[async_trait]
impl ToolHandler for WebFetchTool {
    async fn execute(&self, _ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let url = input["url"]
            .as_str()
            .context("Missing 'url' parameter")?;

        let max_length = input["max_length"]
            .as_u64()
            .unwrap_or(50000) as usize;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context("Failed to create HTTP client")?;

        let response = client
            .get(url)
            .header("User-Agent", "ClawRig/0.1")
            .send()
            .await
            .context(format!("Failed to fetch URL: {}", url))?;

        let status = response.status();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown")
            .to_string();

        let body = response
            .text()
            .await
            .context("Failed to read response body")?;

        let body_len = body.len();
        let truncated = body_len > max_length;
        let content = if truncated {
            format!("{}...[truncated at {} chars]", &body[..max_length], max_length)
        } else {
            body
        };

        Ok(ToolResult {
            success: status.is_success(),
            content,
            metadata: Some(json!({
                "url": url,
                "status": status.as_u16(),
                "content_type": content_type,
                "body_length": body_len,
                "truncated": truncated,
            })),
        })
    }
}
