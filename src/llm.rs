use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::ModelProfile;
use crate::tools::ToolSchema;

/// Response from LLM - either text or tool calls
#[derive(Debug, Clone)]
pub enum LlmResponse {
    Text(String),
    ToolCalls(Vec<ToolCallRequest>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallRequest {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

/// Rich message type that preserves tool_calls through the pipeline
#[derive(Debug, Clone)]
pub enum ChatMessage {
    User(String),
    Assistant { text: String, tool_calls: Option<Vec<ToolCallRequest>> },
    ToolResult { call_id: String, content: String },
}

/// A small OpenAI-compatible chat client (works with streaming + tool calls).
pub struct OpenAiClient {
    api_key: String,
    base_url: String,
    http: reqwest::Client,
}

impl Clone for OpenAiClient {
    fn clone(&self) -> Self {
        Self {
            api_key: self.api_key.clone(),
            base_url: self.base_url.clone(),
            http: self.http.clone(),
        }
    }
}

/// A wrapper around LLM providers for completions
#[derive(Clone)]
pub enum LlmProvider {
    OpenAi(OpenAiClient),
}

const DEFAULT_UA: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";

#[derive(Debug, Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessageResponse,
}

#[derive(Debug, Deserialize)]
struct ChatMessageResponse {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<ChatToolCall>,
}

#[derive(Debug, Deserialize)]
struct ChatToolCall {
    id: String,
    function: ChatToolFunction,
}

#[derive(Debug, Deserialize)]
struct ChatToolFunction {
    name: String,
    arguments: String,
}

impl LlmProvider {
    /// Create a new provider from config
    pub fn from_config(profile: &ModelProfile) -> Result<Self> {
        match profile.provider.as_str() {
            "openai" => {
                let api_key = if let Some(key) = &profile.api_key {
                    key.clone()
                } else if let Some(env) = &profile.api_key_env {
                    std::env::var(env).context(format!("Environment variable {} not set", env))?
                } else {
                    return Err(anyhow::anyhow!("No API key provided for OpenAI"));
                };

                let base_url = profile
                    .base_url
                    .clone()
                    .unwrap_or_else(|| "https://api.openai.com/v1".to_string());

                let http = reqwest::Client::builder()
                    .user_agent(DEFAULT_UA)
                    .build()
                    .context("Failed to build HTTP client")?;

                Ok(LlmProvider::OpenAi(OpenAiClient { api_key, base_url, http }))
            }
            _ => Err(anyhow::anyhow!("Unsupported provider: {}", profile.provider)),
        }
    }

    fn openai(&self) -> Result<&OpenAiClient> {
        match self {
            LlmProvider::OpenAi(client) => Ok(client),
        }
    }

    /// Generate a completion with optional tool support.
    pub async fn complete(
        &self,
        model: &str,
        system_prompt: &str,
        messages: Vec<ChatMessage>,
        tools: &[ToolSchema],
    ) -> Result<LlmResponse> {
        let client = self.openai()?;
        let body = build_request(model, system_prompt, &messages, tools, false);

        let response = client
            .http
            .post(format!("{}/chat/completions", client.base_url))
            .bearer_auth(&client.api_key)
            .json(&body)
            .send()
            .await
            .context("Failed to send completion request")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("Chat completion failed ({}): {}", status, text));
        }

        let parsed: ChatResponse = response
            .json()
            .await
            .context("Failed to parse chat completion response")?;

        parse_choice(parsed.choices.first().map(|c| &c.message))
    }

    /// Generate a completion with optional tool support, streaming text tokens to
    /// the `emit` closure as they arrive. Tool calls are accumulated internally
    /// (including ids) and returned at the end.
    pub async fn complete_streaming<F>(
        &self,
        model: &str,
        system_prompt: &str,
        messages: Vec<ChatMessage>,
        tools: &[ToolSchema],
        emit: F,
    ) -> Result<LlmResponse>
    where
        F: FnMut(String) + Send + 'static,
    {
        let client = self.openai()?;
        let body = build_request(model, system_prompt, &messages, tools, true);

        let response = client
            .http
            .post(format!("{}/chat/completions", client.base_url))
            .bearer_auth(&client.api_key)
            .json(&body)
            .send()
            .await
            .context("Failed to send streaming completion request")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("Chat completion failed ({}): {}", status, text));
        }

        parse_sse_stream(response.bytes_stream(), emit).await
    }
}

/// Build the OpenAI-compatible request body. Mirrors the wire format that has
/// been verified against the OpenCode Zen API (system message, string contents,
/// assistant tool_calls as {id, type, function{name, arguments-string}}).
fn build_request(
    model: &str,
    system_prompt: &str,
    messages: &[ChatMessage],
    tools: &[ToolSchema],
    stream: bool,
) -> serde_json::Value {
    use serde_json::{json, Value};

    let mut wire_messages: Vec<Value> = vec![json!({
        "role": "system",
        "content": system_prompt,
    })];

    for msg in messages {
        match msg {
            ChatMessage::User(content) => {
                wire_messages.push(json!({ "role": "user", "content": content }));
            }
            ChatMessage::Assistant { text, tool_calls } => {
                if let Some(calls) = tool_calls {
                    let calls: Vec<Value> = calls
                        .iter()
                        .map(|tc| {
                            json!({
                                "id": tc.id,
                                "type": "function",
                                "function": {
                                    "name": tc.name,
                                    "arguments": tc.arguments.to_string(),
                                }
                            })
                        })
                        .collect();
                    wire_messages.push(json!({
                        "role": "assistant",
                        "content": [],
                        "tool_calls": calls,
                    }));
                } else {
                    wire_messages.push(json!({ "role": "assistant", "content": text }));
                }
            }
            ChatMessage::ToolResult { call_id, content } => {
                wire_messages.push(json!({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": content,
                }));
            }
        }
    }

    let tool_defs: Vec<Value> = tools
        .iter()
        .map(|t| {
            json!({
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.parameters,
                }
            })
        })
        .collect();

    json!({
        "model": model,
        "messages": wire_messages,
        "tools": tool_defs,
        "tool_choice": "auto",
        "stream": stream,
    })
}

/// Parse a non-streaming choice into an LlmResponse.
fn parse_choice(message: Option<&ChatMessageResponse>) -> Result<LlmResponse> {
    let message = message.context("Empty response from chat completion")?;

    if !message.tool_calls.is_empty() {
        let calls = message
            .tool_calls
            .iter()
            .map(|tc| {
                let arguments = serde_json::from_str(&tc.function.arguments)
                    .unwrap_or(serde_json::Value::String(tc.function.arguments.clone()));
                ToolCallRequest {
                    id: tc.id.clone(),
                    name: tc.function.name.clone(),
                    arguments,
                }
            })
            .collect();
        Ok(LlmResponse::ToolCalls(calls))
    } else {
        Ok(LlmResponse::Text(message.content.clone().unwrap_or_default()))
    }
}

/// Streamed SSE delta. The `reasoning_content` field is deliberately ignored;
/// the upstream flakily requires it to be echoed back, so callers retry instead.
#[derive(Debug, Deserialize)]
struct StreamChunk {
    choices: Vec<StreamChoice>,
}

#[derive(Debug, Deserialize)]
struct StreamChoice {
    delta: StreamDelta,
}

#[derive(Debug, Deserialize)]
struct StreamDelta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<StreamToolCall>,
}

#[derive(Debug, Deserialize)]
struct StreamToolCall {
    #[serde(default)]
    index: usize,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    function: Option<StreamFunction>,
}

#[derive(Debug, Deserialize)]
struct StreamFunction {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: String,
}

async fn parse_sse_stream<S, F>(mut stream: S, mut emit: F) -> Result<LlmResponse>
where
    S: futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin,
    F: FnMut(String) + Send + 'static,
{
    use futures::StreamExt;

    let mut text_parts = String::new();
    // index -> (id, name, arguments)
    let mut calls: std::collections::HashMap<usize, (String, String, String)> =
        std::collections::HashMap::new();

    // Buffer raw bytes and only parse complete newline-terminated lines.
    // SSE events frequently span multiple HTTP chunks; parsing line fragments
    // would silently drop tokens and corrupt UTF-8 sequences.
    let mut buffer: Vec<u8> = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Stream read error")?;
        buffer.extend_from_slice(&chunk);

        while let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
            let line_bytes: Vec<u8> = buffer.drain(..=pos).collect();
            let line = String::from_utf8_lossy(&line_bytes);
            let line = line.trim_end_matches('\r').trim();
            if line.is_empty() || line.starts_with(':') {
                continue;
            }
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let data = data.trim();
            if data == "[DONE]" {
                return finish_stream(&text_parts, &calls);
            }
            let Ok(chunk) = serde_json::from_str::<StreamChunk>(data) else {
                continue;
            };
            let Some(choice) = chunk.choices.first() else {
                continue;
            };
            let delta = &choice.delta;

            if let Some(content) = &delta.content {
                emit(content.clone());
                text_parts.push_str(content);
            }

            for tc in &delta.tool_calls {
                match &tc.function {
                    Some(f) if f.name.is_some() => {
                        let id = tc.id.clone().unwrap_or_else(|| format!("call_{}", tc.index));
                        calls.insert(
                            tc.index,
                            (id, f.name.clone().unwrap_or_default(), f.arguments.clone()),
                        );
                    }
                    Some(f) => {
                        let entry = calls.entry(tc.index).or_insert_with(|| {
                            (
                                tc.id.clone().unwrap_or_else(|| format!("call_{}", tc.index)),
                                String::new(),
                                String::new(),
                            )
                        });
                        entry.2.push_str(&f.arguments);
                    }
                    None => {}
                }
            }
        }
    }

    finish_stream(&text_parts, &calls)
}

fn finish_stream(
    text_parts: &str,
    calls: &std::collections::HashMap<usize, (String, String, String)>,
) -> Result<LlmResponse> {
    if !calls.is_empty() {
        let mut tool_calls = Vec::new();
        let mut indices: Vec<usize> = calls.keys().cloned().collect();
        indices.sort_unstable();
        for idx in indices {
            let (id, name, arguments) = &calls[&idx];
            let arguments = serde_json::from_str(arguments)
                .unwrap_or(serde_json::Value::String(arguments.clone()));
            tool_calls.push(ToolCallRequest {
                id: id.clone(),
                name: name.clone(),
                arguments,
            });
        }
        Ok(LlmResponse::ToolCalls(tool_calls))
    } else {
        Ok(LlmResponse::Text(text_parts.to_string()))
    }
}
