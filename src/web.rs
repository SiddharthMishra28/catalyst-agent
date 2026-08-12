use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        Html,
    },
    routing::{get, post},
    Json, Router,
};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::agent::AgentRequest;
use crate::agent_manager::AgentManager;
use crate::database::approvals::ApprovalStore;
use crate::database::tasks::TaskStore;
use crate::permissions::PermissionManager;

#[derive(Clone)]
pub struct WebState {
    pub agent_manager: Arc<AgentManager>,
    pub event_tx: broadcast::Sender<String>,
    pub approval_store: Arc<ApprovalStore>,
    pub permissions: Arc<PermissionManager>,
    pub task_store: Arc<TaskStore>,
    pub cancel_tokens: Arc<dashmap::DashMap<String, Arc<AtomicBool>>>,
}

#[derive(Deserialize)]
pub struct ChatRequest {
    pub message: String,
    pub agent: Option<String>,
    pub peer_id: Option<String>,
}

#[derive(Serialize)]
pub struct ChatResponse {
    pub run_id: String,
    pub agent: String,
    pub session_id: String,
    pub status: String,
}

#[derive(Deserialize)]
pub struct ApprovalRequest {
    pub approval_id: String,
}

pub fn create_router(state: WebState) -> Router {
    let cors = tower_http::cors::CorsLayer::permissive();

    Router::new()
        .route("/", get(index_page))
        .route("/api/chat", post(handle_chat))
        .route("/api/agents", get(list_agents))
        .route("/api/events", get(sse_events))
        .route("/api/health", get(health_check))
        .route("/api/metrics", get(get_metrics))
        .route("/api/approve", post(handle_approve))
        .route("/api/deny", post(handle_deny))
        .route("/api/tasks", get(list_tasks))
        .route("/api/tasks/{run_id}", get(get_task))
        .route("/api/tasks/{run_id}/cancel", post(cancel_task))
        .with_state(state)
        .layer(cors)
}

async fn index_page() -> Html<&'static str> {
    Html(include_str!("../ui/index.html"))
}

async fn handle_chat(
    State(state): State<WebState>,
    Json(req): Json<ChatRequest>,
) -> Result<Json<ChatResponse>, StatusCode> {
    let agent_name = req.agent.as_deref().unwrap_or("main");

    let agent = state.agent_manager.get(agent_name)
        .ok_or(StatusCode::NOT_FOUND)?;

    let run_id = uuid::Uuid::new_v4().to_string();
    let agent_owned = agent_name.to_string();
    let run_id_clone = run_id.clone();
    let session_id = format!("web:{}:{}", agent_name, req.peer_id.as_deref().unwrap_or("anonymous"));

    // Register a cancel token for this run
    let cancel_token = Arc::new(AtomicBool::new(false));
    state.cancel_tokens.insert(run_id.clone(), cancel_token.clone());

    // Record the run in the task store
    state.task_store.create_run(&run_id, agent_name, &session_id).await
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to record task run");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Broadcast event: user message
    let _ = state.event_tx.send(serde_json::to_string(&serde_json::json!({
        "type": "user_message",
        "agent": agent_name,
        "run_id": &run_id,
        "content": &req.message,
    })).unwrap_or_default());

    let request = AgentRequest {
        agent_id: agent_owned.clone(),
        session_id: session_id.clone(),
        channel: "web".to_string(),
        peer_id: req.peer_id.unwrap_or_else(|| "anonymous".to_string()),
        content: req.message,
        attachments: Vec::new(),
        run_id: Some(run_id.clone()),
        cancel_token: Some(cancel_token),
    };

    // Run in the background so the request returns immediately with a run_id.
    // Streaming tokens, tool calls, and completion arrive over /api/events SSE.
    let state_clone = state.clone();
    tokio::spawn(async move {
        let result = agent.run(request).await;
        match result {
            Ok(response) => {
                let _ = state_clone.task_store.complete_run(&run_id_clone).await;
                // Broadcast event: agent response
                let _ = state_clone.event_tx.send(serde_json::to_string(&serde_json::json!({
                    "type": "agent_response",
                    "agent": agent_owned,
                    "run_id": &run_id_clone,
                    "content": &response.content,
                    "tool_calls": response.tool_calls.len(),
                })).unwrap_or_default());
            }
            Err(e) => {
                tracing::error!(agent = %agent_owned, run_id = %run_id_clone, error = %e, "Chat failed");
                let _ = state_clone.task_store.fail_run(&run_id_clone, &e.to_string()).await;
                let _ = state_clone.event_tx.send(serde_json::to_string(&serde_json::json!({
                    "type": "run_error",
                    "agent": agent_owned,
                    "run_id": &run_id_clone,
                    "error": e.to_string(),
                })).unwrap_or_default());
            }
        }
        state_clone.cancel_tokens.remove(&run_id_clone);
    });

    Ok(Json(ChatResponse {
        run_id,
        agent: agent_name.to_string(),
        session_id,
        status: "running".to_string(),
    }))
}

async fn list_tasks(
    State(state): State<WebState>,
) -> Json<Vec<crate::database::tasks::Task>> {
    let tasks = state.task_store.list_runs(50).await.unwrap_or_default();
    Json(tasks)
}

async fn get_task(
    State(state): State<WebState>,
    Path(run_id): Path<String>,
) -> Result<Json<crate::database::tasks::Task>, StatusCode> {
    state.task_store.get_run(&run_id).await
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to get task");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn cancel_task(
    State(state): State<WebState>,
    Path(run_id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if let Some(token) = state.cancel_tokens.get(&run_id) {
        token.value().store(true, Ordering::Relaxed);
        let _ = state.task_store.cancel_run(&run_id).await;
        let _ = state.event_tx.send(serde_json::to_string(&serde_json::json!({
            "type": "run_cancelled",
            "run_id": &run_id,
        })).unwrap_or_default());
        Ok(Json(serde_json::json!({"status": "cancelled"})))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn list_agents(
    State(state): State<WebState>,
) -> Json<Vec<String>> {
    Json(state.agent_manager.list())
}

async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

async fn get_metrics() -> Json<serde_json::Value> {
    let mem = get_memory_usage();
    Json(serde_json::json!({
        "memory_kb": mem,
        "pid": std::process::id(),
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

async fn sse_events(
    State(state): State<WebState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut rx = state.event_tx.subscribe();

    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(data) => {
                    yield Ok(Event::default().data(data));
                }
                Err(_) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn handle_approve(
    State(state): State<WebState>,
    Json(req): Json<ApprovalRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match state.permissions.approve(&req.approval_id).await {
        Ok(true) => {
            let _ = state.event_tx.send(serde_json::to_string(&serde_json::json!({
                "type": "approval_granted",
                "approval_id": &req.approval_id,
            })).unwrap_or_default());
            Ok(Json(serde_json::json!({"status": "approved"})))
        }
        Ok(false) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::error!(error = %e, "Approve failed");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn handle_deny(
    State(state): State<WebState>,
    Json(req): Json<ApprovalRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match state.permissions.deny(&req.approval_id).await {
        Ok(true) => {
            let _ = state.event_tx.send(serde_json::to_string(&serde_json::json!({
                "type": "approval_denied",
                "approval_id": &req.approval_id,
            })).unwrap_or_default());
            Ok(Json(serde_json::json!({"status": "denied"})))
        }
        Ok(false) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::error!(error = %e, "Deny failed");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

fn get_memory_usage() -> u64 {
    #[cfg(target_os = "windows")]
    {
        0
    }
    #[cfg(not(target_os = "windows"))]
    {
        use std::fs;
        if let Ok(status) = fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb) = line.split_whitespace().nth(1) {
                        return kb.parse().unwrap_or(0);
                    }
                }
            }
        }
        0
    }
}
