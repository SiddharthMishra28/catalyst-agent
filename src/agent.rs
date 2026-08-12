use anyhow::{Context, Result};
use chrono::Utc;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::database::sessions::{Message, SessionStore};
use crate::llm::{LlmProvider, LlmResponse, ToolCallRequest, ChatMessage};
use crate::models::ModelRouter;
use crate::permissions::PermissionManager;
use crate::tools::{ToolContext, ToolRegistry, ToolResult, PermissionMode};

const COMPACT_THRESHOLD: i64 = 60;
const COMPACT_KEEP: i64 = 30;

#[derive(Debug, Clone, Serialize)]
pub struct AgentRequest {
    pub agent_id: String,
    pub session_id: String,
    pub channel: String,
    pub peer_id: String,
    pub content: String,
    pub attachments: Vec<Attachment>,
    pub run_id: Option<String>,
    #[serde(skip)]
    pub cancel_token: Option<Arc<AtomicBool>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Attachment {
    pub kind: String,
    pub filename: String,
    pub mime: String,
    pub size: u64,
    pub local_path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentResponse {
    pub session_id: String,
    pub content: String,
    pub tool_calls: Vec<ToolCallInfo>,
    pub tokens_used: Option<u32>,
    pub model_used: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ToolCallInfo {
    pub name: String,
    pub arguments: String,
    pub result: Option<String>,
}

pub struct AgentRuntime {
    pub config: AgentConfig,
    pub sessions: Arc<SessionStore>,
    pub tools: Arc<ToolRegistry>,
    pub model_router: Arc<ModelRouter>,
    pub llm_provider: Arc<LlmProvider>,
    pub permissions: Arc<PermissionManager>,
    pub event_tx: tokio::sync::broadcast::Sender<String>,
}

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub name: String,
    pub system_prompt: String,
    pub max_tool_rounds: u32,
    pub model_override: Option<String>,
    pub yolo_mode: bool,
}

impl AgentRuntime {
    pub fn new(
        config: AgentConfig,
        sessions: Arc<SessionStore>,
        tools: Arc<ToolRegistry>,
        model_router: Arc<ModelRouter>,
        llm_provider: Arc<LlmProvider>,
        permissions: Arc<PermissionManager>,
        event_tx: tokio::sync::broadcast::Sender<String>,
    ) -> Self {
        Self { config, sessions, tools, model_router, llm_provider, permissions, event_tx }
    }

    pub async fn run(self: &Arc<Self>, request: AgentRequest) -> Result<AgentResponse> {
        let session = self.sessions.get_or_create(
            &request.agent_id,
            &request.channel,
            &request.peer_id,
            None,
        ).await?;

        // Store user message
        let user_msg = Message {
            id: uuid::Uuid::new_v4().to_string(),
            session_id: session.id.clone(),
            role: "user".to_string(),
            content: request.content.clone(),
            attachments: None,
            tool_call_id: None,
            tool_calls: None,
            created_at: Utc::now().timestamp(),
            tokens: None,
        };
        self.sessions.add_message(&user_msg).await?;

        // Select model
        let (_profile_name, profile) = self.model_router.select(&crate::models::TaskClass::Chat)?;

        tracing::info!(
            agent = %request.agent_id,
            session = %session.id,
            model = %profile.model,
            provider = %profile.provider,
            "Agent run started"
        );

        // Build system prompt (no tool descriptions - tools sent via API)
        let system_prompt = self.build_system_prompt();

        // Inject prior-context summary if the session was compacted before
        let system_prompt = if let Some(summary) = &session.summary {
            if summary.trim().is_empty() {
                system_prompt
            } else {
                format!(
                    "{}\n\n## Prior conversation summary\n{}\n\nThe messages before this summary were archived to save context. Use this summary to recall earlier context.",
                    system_prompt, summary
                )
            }
        } else {
            system_prompt
        };

        // Get tool schemas for the API call
        let tool_schemas = self.tools.get_schemas();

        // Tool execution loop
        let mut all_tool_calls = Vec::new();
        let mut final_response = None;
        let mut response_retries = 0u32;
        let mut error_retries = 0u32;

        for round in 0..self.config.max_tool_rounds {
            // Check for user-requested cancellation
            if request.cancel_token.as_ref().is_some_and(|c| c.load(Ordering::Relaxed)) {
                tracing::info!(
                    agent = %request.agent_id,
                    session = %session.id,
                    run_id = request.run_id.as_deref().unwrap_or("-"),
                    "Agent run cancelled"
                );
                let _ = self.event_tx.send(serde_json::to_string(&serde_json::json!({
                    "type": "run_cancelled",
                    "run_id": request.run_id.as_deref().unwrap_or(""),
                    "session_id": session.id,
                })).unwrap_or_default());
                return Ok(AgentResponse {
                    session_id: session.id,
                    content: "Run cancelled by user.".to_string(),
                    tool_calls: all_tool_calls,
                    tokens_used: None,
                    model_used: Some(profile.model),
                });
            }

            // Build message history for LLM
            let messages = self.sessions.get_messages(&session.id, 100).await?;
            let llm_messages = self.build_llm_messages(&messages);

            // Generate LLM response with native tool calling (streamed)
            let emit_tx = self.event_tx.clone();
            let emit_run_id = request.run_id.clone();
            let emit_session_id = session.id.clone();
            let response = match self.llm_provider.complete_streaming(
                &profile.model,
                &system_prompt,
                llm_messages,
                &tool_schemas,
                move |token: String| {
                    if let Some(run_id) = &emit_run_id {
                        let _ = emit_tx.send(serde_json::to_string(&serde_json::json!({
                            "type": "session.token",
                            "run_id": run_id,
                            "session_id": emit_session_id,
                            "token": token,
                        })).unwrap_or_default());
                    }
                },
            ).await {
                Ok(resp) => resp,
                Err(e) => {
                    // The free models intermittently reject requests when the
                    // upstream expects reasoning_content to be echoed back.
                    // Retry - the error is not deterministic.
                    if e.to_string().contains("reasoning_content") && error_retries < 2 {
                        error_retries += 1;
                        tracing::warn!(
                            agent = %request.agent_id,
                            session = %session.id,
                            round = round,
                            retry = error_retries,
                            error = %e,
                            "reasoning_content rejection, retrying"
                        );
                        // Clear any partial tokens streamed before the failure
                        if let Some(run_id) = &request.run_id {
                            let _ = self.event_tx.send(serde_json::to_string(&serde_json::json!({
                                "type": "session.token_reset",
                                "run_id": run_id,
                                "session_id": session.id,
                            })).unwrap_or_default());
                        }
                        continue;
                    }
                    tracing::error!(
                        agent = %request.agent_id,
                        session = %session.id,
                        round = round,
                        error = %e,
                        "LLM completion failed"
                    );
                    final_response = Some(format!("I encountered an error processing your request: {}", e));
                    break;
                }
            };

            match response {
                LlmResponse::Text(text) => {
                    // Free models are flaky and sometimes return an empty or
                    // canned greeting instead of answering. Retry a few times.
                    let trimmed = text.trim();
                    let is_bad_response = trimmed.is_empty() || is_canned_greeting(trimmed);
                    if is_bad_response && response_retries < 2 {
                        response_retries += 1;
                        tracing::warn!(
                            agent = %request.agent_id,
                            session = %session.id,
                            round = round,
                            retry = response_retries,
                            response = trimmed.chars().take(80).collect::<String>(),
                            "Empty or canned response, retrying"
                        );
                        // Clear any partial tokens streamed before the retry
                        if let Some(run_id) = &request.run_id {
                            let _ = self.event_tx.send(serde_json::to_string(&serde_json::json!({
                                "type": "session.token_reset",
                                "run_id": run_id,
                                "session_id": session.id,
                            })).unwrap_or_default());
                        }
                        continue;
                    }

                    // No tool calls - this is the final response
                    final_response = Some(text.clone());

                    let assistant_msg = Message {
                        id: uuid::Uuid::new_v4().to_string(),
                        session_id: session.id.clone(),
                        role: "assistant".to_string(),
                        content: text,
                        attachments: None,
                        tool_call_id: None,
                        tool_calls: None,
                        created_at: Utc::now().timestamp(),
                        tokens: None,
                    };
                    self.sessions.add_message(&assistant_msg).await?;
                    break;
                }
                LlmResponse::ToolCalls(tool_calls) => {
                    tracing::info!(
                        agent = %request.agent_id,
                        session = %session.id,
                        round = round,
                        tool_count = tool_calls.len(),
                        "LLM requested tool calls"
                    );

                    // Serialize tool calls to JSON for storage
                    let tool_calls_json = serde_json::to_string(&tool_calls).unwrap_or_default();

                    // Store the assistant's tool call message with tool_calls field populated
                    let assistant_msg = Message {
                        id: uuid::Uuid::new_v4().to_string(),
                        session_id: session.id.clone(),
                        role: "assistant".to_string(),
                        content: String::new(), // Empty content - tool_calls carry the info
                        attachments: None,
                        tool_call_id: None,
                        tool_calls: Some(tool_calls_json),
                        created_at: Utc::now().timestamp(),
                        tokens: None,
                    };
                    self.sessions.add_message(&assistant_msg).await?;

                    // Execute each tool call
                    let tool_context = ToolContext {
                        agent_id: request.agent_id.clone(),
                        session_id: session.id.clone(),
                        channel: request.channel.clone(),
                        peer_id: request.peer_id.clone(),
                    };

                    for tc in &tool_calls {
                        // Broadcast tool call event to web UI
                        let _ = self.event_tx.send(serde_json::to_string(&serde_json::json!({
                            "type": "tool_call",
                            "name": tc.name,
                            "args": tc.arguments.to_string(),
                            "run_id": request.run_id.as_deref().unwrap_or(""),
                            "session_id": session.id,
                        })).unwrap_or_default());

                        let permission = if self.config.yolo_mode {
                            PermissionMode::Allow
                        } else {
                            self.permissions.check_permission(
                                &request.agent_id,
                                &tc.name,
                            )
                        };

                        let result_str = match permission {
                            PermissionMode::Allow | PermissionMode::Auto => {
                                tracing::info!(
                                    agent = %request.agent_id,
                                    tool = %tc.name,
                                    "Executing tool (allowed)"
                                );
                                match self.execute_tool(&tool_context, &tc.name, &tc.arguments.to_string()).await {
                                    Ok(r) => r.content,
                                    Err(e) => format!("Error: {}", e),
                                }
                            }
                            PermissionMode::Ask => {
                                tracing::info!(
                                    agent = %request.agent_id,
                                    tool = %tc.name,
                                    "Requesting approval for tool"
                                );
                                let approval = self.permissions.request_approval(
                                    &request.agent_id,
                                    &session.id,
                                    &tc.name,
                                    &tc.arguments.to_string(),
                                ).await?;

                                tracing::info!(
                                    approval_id = %approval.id,
                                    tool = %tc.name,
                                    "Waiting for human approval..."
                                );

                                // Broadcast approval needed event to web UI
                                let _ = self.event_tx.send(serde_json::to_string(&serde_json::json!({
                                    "type": "approval_needed",
                                    "approval_id": approval.id,
                                    "tool": tc.name,
                                    "arguments": tc.arguments,
                                    "agent": request.agent_id,
                                    "session_id": session.id,
                                })).unwrap_or_default());

                                match self.permissions.wait_for_approval(&approval.id).await? {
                                    PermissionMode::Allow => {
                                        tracing::info!(
                                            approval_id = %approval.id,
                                            tool = %tc.name,
                                            "Approved - executing"
                                        );
                                        match self.execute_tool(&tool_context, &tc.name, &tc.arguments.to_string()).await {
                                            Ok(r) => r.content,
                                            Err(e) => format!("Error: {}", e),
                                        }
                                    }
                                    _ => {
                                        tracing::warn!(
                                            approval_id = %approval.id,
                                            tool = %tc.name,
                                            "Denied or expired"
                                        );
                                        "Tool execution denied by human approval".to_string()
                                    }
                                }
                            }
                            PermissionMode::Deny => {
                                tracing::warn!(
                                    agent = %request.agent_id,
                                    tool = %tc.name,
                                    "Tool denied by policy"
                                );
                                "Tool execution denied by policy".to_string()
                            }
                        };

                        let result_truncated: String = result_str.chars().take(2000).collect();

                        // Broadcast tool result event to web UI
                        let _ = self.event_tx.send(serde_json::to_string(&serde_json::json!({
                            "type": "tool_result",
                            "name": tc.name,
                            "result": result_truncated,
                            "run_id": request.run_id.as_deref().unwrap_or(""),
                            "session_id": session.id,
                        })).unwrap_or_default());

                        all_tool_calls.push(ToolCallInfo {
                            name: tc.name.clone(),
                            arguments: tc.arguments.to_string(),
                            result: Some(result_str.clone()),
                        });

                        // Store tool result as user message with tool_result role
                        let result_msg = Message {
                            id: uuid::Uuid::new_v4().to_string(),
                            session_id: session.id.clone(),
                            role: "tool_result".to_string(),
                            content: format!("{}|{}", tc.id, result_str),
                            attachments: None,
                            tool_call_id: Some(tc.id.clone()),
                            tool_calls: None,
                            created_at: Utc::now().timestamp(),
                            tokens: None,
                        };
                        self.sessions.add_message(&result_msg).await?;
                    }
                    // Continue loop - LLM will see tool results and respond
                }
            }
        }

        // If we didn't get a final response (max rounds exceeded), use the last LLM output
        if final_response.is_none() {
            let messages = self.sessions.get_messages(&session.id, 2).await?;
            final_response = messages.last().map(|m| m.content.clone());
        }

        let response_content = final_response.unwrap_or_else(|| "No response generated.".to_string());

        // Background compaction: if the session grew large, summarize the oldest
        // messages and trim them so the context window stays manageable.
        if let Ok(message_count) = self.sessions.count_messages(&session.id).await {
            if message_count > COMPACT_THRESHOLD {
                let sessions = self.sessions.clone();
                let llm_provider = self.llm_provider.clone();
                let model = profile.model.clone();
                let session_id = session.id.clone();
                let event_tx = self.event_tx.clone();
                let run_id = request.run_id.clone();
                tokio::spawn(async move {
                    if let Err(e) = compact_session(
                        &sessions,
                        &llm_provider,
                        &model,
                        &session_id,
                        &event_tx,
                        run_id.as_deref(),
                    ).await {
                        tracing::warn!(session = %session_id, error = %e, "Session compaction failed");
                    }
                });
            }
        }

        tracing::info!(
            agent = %request.agent_id,
            session = %session.id,
            tool_calls_count = all_tool_calls.len(),
            "Agent run completed"
        );

        Ok(AgentResponse {
            session_id: session.id,
            content: response_content,
            tool_calls: all_tool_calls,
            tokens_used: None,
            model_used: Some(profile.model),
        })
    }

    pub fn build_system_prompt(&self) -> String {
        let mut prompt = self.config.system_prompt.clone();

        if cfg!(target_os = "windows") {
            prompt.push_str(
                "\n\nEnvironment: You are running on Windows. The shell_exec tool runs commands via cmd.exe (cmd /C). \
Use Windows command syntax (e.g. `dir`, `type file.txt`, `echo %VAR%`, `where python`, `date /t`, `time /t`). \
Do NOT use Unix syntax like `ls -la`, `cat`, `grep`, `env`, `TZ=... date`, or `export VAR=value`. \
Use PowerShell syntax only if explicitly needed via `powershell -Command \"...\"`.",
            );
        } else {
            prompt.push_str(
                "\n\nEnvironment: You are running on Linux/Unix. The shell_exec tool runs commands via sh -c.",
            );
        }

        prompt
    }

    /// Build LLM messages from DB messages, properly reconstructing tool_calls.
    /// tool_result messages whose preceding assistant message has no tool_calls
    /// are orphaned (e.g. from pre-tool_calls-format sessions) and must be
    /// skipped, otherwise the provider rejects `role: 'tool'` messages.
    fn build_llm_messages(&self, messages: &[Message]) -> Vec<ChatMessage> {
        let mut result = Vec::new();
        let mut last_assistant_had_tool_calls = false;
        for msg in messages {
            match msg.role.as_str() {
                "user" => {
                    result.push(ChatMessage::User(msg.content.clone()));
                    last_assistant_had_tool_calls = false;
                }
                "assistant" => {
                    let mut had_tool_calls = false;
                    // If tool_calls field is populated, reconstruct with both text and tool_calls
                    if let Some(tc_json) = &msg.tool_calls {
                        if let Ok(tool_calls) = serde_json::from_str::<Vec<ToolCallRequest>>(tc_json) {
                            had_tool_calls = !tool_calls.is_empty();
                            result.push(ChatMessage::Assistant {
                                text: msg.content.clone(),
                                tool_calls: Some(tool_calls),
                            });
                        } else {
                            result.push(ChatMessage::Assistant {
                                text: msg.content.clone(),
                                tool_calls: None,
                            });
                        }
                    } else if !msg.content.is_empty() {
                        result.push(ChatMessage::Assistant {
                            text: msg.content.clone(),
                            tool_calls: None,
                        });
                    }
                    last_assistant_had_tool_calls = had_tool_calls;
                }
                "tool_result" => {
                    // Only keep if the preceding assistant message actually declared this tool call
                    if !last_assistant_had_tool_calls {
                        continue;
                    }
                    // Parse "tool_call_id|result_content"
                    if let Some((call_id, result_content)) = msg.content.split_once('|') {
                        result.push(ChatMessage::ToolResult {
                            call_id: call_id.to_string(),
                            content: result_content.to_string(),
                        });
                    }
                }
                _ => {}
            }
        }
        result
    }

    async fn execute_tool(
        &self,
        ctx: &ToolContext,
        tool_name: &str,
        arguments: &str,
    ) -> Result<ToolResult> {
        let entry = self.tools.get(tool_name)
            .context(format!("Tool not found: {}", tool_name))?;

        let input: serde_json::Value = serde_json::from_str(arguments)
            .context("Failed to parse tool arguments as JSON")?;

        entry.handler.execute(ctx, input).await
    }
}

/// Summarize the oldest messages in a session and trim them so the context
/// window stays manageable. Runs in the background after a run completes.
async fn compact_session(
    sessions: &Arc<SessionStore>,
    llm_provider: &Arc<LlmProvider>,
    model: &str,
    session_id: &str,
    event_tx: &tokio::sync::broadcast::Sender<String>,
    run_id: Option<&str>,
) -> Result<()> {
    let messages = sessions.get_messages(session_id, 100).await?;
    if messages.len() <= COMPACT_KEEP as usize {
        return Ok(());
    }

    let keep = COMPACT_KEEP as usize;
    let old = &messages[..messages.len() - keep];

    // Build a plain-text transcript of the messages being archived
    let mut transcript = String::new();
    for msg in old {
        if msg.role == "tool_result" || msg.role == "tool" {
            continue;
        }
        let content: String = msg.content.chars().take(600).collect();
        transcript.push_str(&format!(
            "[{}] {}\n",
            if msg.role == "assistant" { "assistant" } else { "user" },
            content
        ));
    }
    let transcript: String = transcript.chars().take(9000).collect();
    if transcript.trim().is_empty() {
        return Ok(());
    }

    let summary_prompt = format!(
        "Summarize the conversation transcript below into a compact summary \
         (max 250 words) that preserves the key facts, decisions, tasks, user \
         preferences, and any context needed to continue the conversation. \
         Do not add anything not present in the transcript.\n\nTRANSCRIPT:\n{}",
        transcript
    );

    let summary = llm_provider
        .complete(
            model,
            "You are a conversation summarizer. Output only the summary.",
            vec![ChatMessage::User(summary_prompt)],
            &[],
        )
        .await?;

    let summary_text = match summary {
        LlmResponse::Text(text) => text,
        _ => return Ok(()),
    };
    let summary_text = summary_text.trim().to_string();
    if summary_text.is_empty() {
        return Ok(());
    }

    sessions.append_summary(session_id, &summary_text).await?;
    sessions.delete_oldest_messages(session_id, COMPACT_KEEP).await?;

    tracing::info!(session = session_id, archived = old.len(), "Session compacted");

    if let Some(run_id) = run_id {
        let _ = event_tx.send(serde_json::to_string(&serde_json::json!({
            "type": "session.compacted",
            "run_id": run_id,
            "session_id": session_id,
            "archived_messages": old.len(),
        })).unwrap_or_default());
    }

    Ok(())
}

/// Detect canned generic greetings that free models sometimes emit instead of
/// actually answering (e.g. "Hi there! How can I help you today?").
/// Only short texts are flagged - a real answer followed by a closing line is
/// long enough to pass.
fn is_canned_greeting(text: &str) -> bool {
    if text.chars().count() > 120 {
        return false;
    }
    let lower = text.to_lowercase();
    [
        "hi there!",
        "hi!",
        "hello!",
        "hey there",
        "hello there",
        "greetings",
        "how can i help you today",
        "how may i help you",
        "what can i help you with",
        "how can i assist you",
        "what can i do for you",
        "is there anything else i can help you with",
    ]
    .iter()
    .any(|pattern| lower.contains(pattern))
}
