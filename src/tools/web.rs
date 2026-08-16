use anyhow::{Context, Result};
use async_trait::async_trait;
use regex::Regex;
use serde_json::{json, Value};

use super::{ToolContext, ToolHandler, ToolResult};

fn decode_entities(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#x27;", "'")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
}

fn strip_tags(s: &str) -> String {
    let re = Regex::new(r"<[^>]+>").unwrap();
    decode_entities(re.replace_all(s, " ").trim())
}

fn unescape_url(s: &str) -> String {
    match url_decode(s) {
        Some(d) => d,
        None => s.to_string(),
    }
}

fn url_decode(s: &str) -> Option<String> {
    let mut out = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok()?;
            let v = u8::from_str_radix(hex, 16).ok()?;
            out.push(v);
            i += 3;
        } else if bytes[i] == b'+' {
            out.push(b' ');
            i += 1;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

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

/// Search the web via DuckDuckGo (keyless, like the `websearch` tool in opencode)
pub struct WebSearchTool;

#[async_trait]
impl ToolHandler for WebSearchTool {
    async fn execute(&self, _ctx: &ToolContext, input: Value) -> Result<ToolResult> {
        let query = input["query"]
            .as_str()
            .context("Missing 'query' parameter")?;

        let max_results = input["max_results"].as_u64().unwrap_or(8).min(20) as usize;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(25))
            .build()
            .context("Failed to create HTTP client")?;

        let url = format!(
            "https://html.duckduckgo.com/html/?q={}",
            url_encode(query)
        );

        let response = client
            .get(url)
            .header("User-Agent", "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0")
            .send()
            .await
            .context("Search request failed")?;

        let status = response.status();
        let body = response
            .text()
            .await
            .context("Failed to read search response")?;

        if !status.is_success() {
            return Ok(ToolResult {
                success: false,
                content: format!("Search backend returned HTTP {}", status.as_u16()),
                metadata: Some(json!({ "status": status.as_u16() })),
            });
        }

        let title_re = Regex::new(r#"<a[^>]*class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>"#).unwrap();
        let snippet_re = Regex::new(r#"<a[^>]*class="result__snippet"[^>]*>(.*?)</a>"#).unwrap();

        let titles: Vec<(String, String)> = title_re
            .captures_iter(&body)
            .take(max_results)
            .map(|c| {
                let href = c.get(1).map(|m| m.as_str()).unwrap_or("");
                let title = strip_tags(c.get(2).map(|m| m.as_str()).unwrap_or(""));
                (href.to_string(), title)
            })
            .collect();

        let snippets: Vec<String> = snippet_re
            .captures_iter(&body)
            .take(max_results)
            .map(|c| strip_tags(c.get(1).map(|m| m.as_str()).unwrap_or("")))
            .collect();

        if titles.is_empty() {
            return Ok(ToolResult {
                success: false,
                content: format!("No results for: {}", query),
                metadata: Some(json!({ "query": query, "count": 0 })),
            });
        }

        let mut lines = Vec::new();
        for (i, (href, title)) in titles.iter().enumerate() {
            let final_url = resolve_ddg_url(href);
            lines.push(format!("{}. {} - {}", i + 1, title, final_url));
            if let Some(snippet) = snippets.get(i) {
                if !snippet.is_empty() {
                    lines.push(format!("   {}", snippet));
                }
            }
        }

        Ok(ToolResult {
            success: true,
            content: lines.join("\n"),
            metadata: Some(json!({
                "query": query,
                "count": titles.len(),
            })),
        })
    }
}

fn url_encode(query: &str) -> String {
    // Encode query for use in a URL query string
    let mut out = String::new();
    for b in query.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn resolve_ddg_url(href: &str) -> String {
    // DDG result links look like //duckduckgo.com/l/?uddg=<urlencoded>
    let href = if let Some(rest) = href.strip_prefix("//") {
        format!("https://{}", rest)
    } else if href.starts_with('/') {
        format!("https://duckduckgo.com{}", href)
    } else {
        href.to_string()
    };

    if let Some(pos) = href.find("uddg=") {
        let enc = &href[pos + 5..];
        let enc = enc.split('&').next().unwrap_or(enc);
        return unescape_url(enc);
    }
    href
}
