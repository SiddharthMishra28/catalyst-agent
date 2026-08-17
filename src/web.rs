use axum::{
    extract::{Path, Query, State},
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
    pub provider: Option<String>,
    pub session_id: Option<String>,
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

#[derive(Deserialize)]
pub struct FileRequest {
    pub path: String,
    pub session: Option<String>,
}

#[derive(Deserialize)]
pub struct FileWriteRequest {
    pub path: String,
    pub content: String,
    pub append: Option<bool>,
    pub session: Option<String>,
}

#[derive(Deserialize)]
pub struct ListFilesRequest {
    pub session: Option<String>,
}

#[derive(Deserialize)]
pub struct SessionRequest {
    pub agent: Option<String>,
    pub peer: Option<String>,
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
        .route("/api/file", get(read_file).post(write_file).delete(delete_file))
        .route("/api/files", get(list_files))
        .route("/api/project", get(project_info))
        .route("/api/session", get(session_info))
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

    // Session identity: the caller can resume an earlier run by passing back
    // the session id it received; otherwise every run gets its own fresh
    // session key (and therefore its own scratch workspace).
    let peer = req.peer_id.clone().unwrap_or_else(|| "anonymous".to_string());
    let session_key = req
        .session_id
        .clone()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| format!("web:{}:{}", agent_name, uuid::Uuid::new_v4()));

    let session = agent
        .sessions
        .get_or_create(agent_name, "web", &peer, Some(&session_key))
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to resolve chat session");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let session_id = session.id.clone();

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
        session_id: session_key.clone(),
        channel: "web".to_string(),
        peer_id: peer,
        content: req.message,
        attachments: Vec::new(),
        run_id: Some(run_id.clone()),
        model_profile: req.provider.as_deref().and_then(|p| match p {
            "nvidia" => Some("nvidia".to_string()),
            "groq" => Some("groq".to_string()),
            "opencode" => Some("fast".to_string()),
            _ => None,
        }),
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
        session_id: session_key,
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

/// Session-scoped workspace root: the temp folder the agent writes generated
/// code into for the given session. Missing session falls back to "default".
async fn workspace_root(session: Option<&str>) -> Result<std::path::PathBuf, StatusCode> {
    let id = session.filter(|s| !s.is_empty()).unwrap_or("default");
    let dir = crate::agent::session_workspace_dir(id);
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(dir)
}

/// Resolve a user-supplied path against a workspace root (a session temp
/// folder). The web file editor is intentionally scoped to that workspace so
/// the browser UI cannot be used to read or write arbitrary server files.
fn resolve_workspace_path(
    base: &std::path::Path,
    path_arg: &str,
) -> Result<std::path::PathBuf, StatusCode> {
    let base = base.canonicalize().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let raw = std::path::PathBuf::from(path_arg);
    let candidate = if raw.is_absolute() {
        raw.clone()
    } else {
        base.join(raw)
    };

    // Canonicalize the deepest existing ancestor of the candidate so we can
    // verify containment even when the target file (or its parent dirs) does
    // not exist yet (e.g. a brand-new file being written by the editor).
    let mut probe = candidate.clone();
    let mut suffix = Vec::new();
    let canonical_ancestor = loop {
        match probe.canonicalize() {
            Ok(canon) => break canon,
            Err(_) => match probe.file_name() {
                Some(name) => {
                    suffix.push(name.to_os_string());
                    match probe.parent() {
                        Some(parent) => probe = parent.to_path_buf(),
                        None => return Err(StatusCode::FORBIDDEN),
                    }
                }
                None => return Err(StatusCode::FORBIDDEN),
            },
        }
    };

    if !canonical_ancestor.starts_with(&base) {
        return Err(StatusCode::FORBIDDEN);
    }

    let mut resolved = canonical_ancestor;
    for component in suffix.iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

async fn read_file(
    State(_state): State<WebState>,
    Query(req): Query<FileRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let base = workspace_root(req.session.as_deref()).await?;
    let path = resolve_workspace_path(&base, &req.path)?;
    if !path.is_file() {
        return Err(StatusCode::NOT_FOUND);
    }

    let content = tokio::fs::read(&path)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let text = String::from_utf8_lossy(&content);

    let rel = path
        .strip_prefix(&base)
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| path.display().to_string());

    Ok(Json(serde_json::json!({
        "path": rel,
        "name": path.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default(),
        "content": text,
        "size": content.len(),
    })))
}

async fn write_file(
    State(_state): State<WebState>,
    Json(req): Json<FileWriteRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let base = workspace_root(req.session.as_deref()).await?;
    let path = resolve_workspace_path(&base, &req.path)?;

    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    if req.append.unwrap_or(false) {
        let current = if path.exists() {
            tokio::fs::read_to_string(&path).await.unwrap_or_default()
        } else {
            String::new()
        };
        let combined = format!("{}{}", current, req.content);
        tokio::fs::write(&path, combined)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    } else {
        tokio::fs::write(&path, &req.content)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    Ok(Json(serde_json::json!({
        "status": "ok",
        "path": req.path,
        "bytes": req.content.len(),
    })))
}

/// Directories that are skipped (with all contents) when listing the
/// workspace for the file explorer, so heavy build/dependency dirs never
/// flood the tree.
const SKIP_DIRS: &[&str] = &[
    ".git", "node_modules", "target", ".cargo", ".cache", ".next", ".nuxt",
    "dist", "build", "vendor", ".venv", "venv", "__pycache__", ".pytest_cache",
    ".mypy_cache", ".ruff_cache", ".idea", ".vscode", "coverage", "tmp",
    "bin", "boot", "dev", "etc", "home", "lib", "lib64", "opt", "proc",
    "root", "run", "sbin", "srv", "sys", "usr", "var",
];

const MAX_TREE_DEPTH: usize = 8;

/// Recursively collect a nested folder/file tree under `dir`, relative to
/// `base`, honoring the skip list and depth cap. Kept synchronous — the
/// workspace scan is bounded and this only runs on explicit user action.
fn collect_tree(base: &std::path::Path, dir: &std::path::Path, depth: usize) -> Vec<serde_json::Value> {
    if depth > MAX_TREE_DEPTH {
        return Vec::new();
    }
    let mut entries: Vec<_> = match std::fs::read_dir(dir) {
        Ok(rd) => rd.filter_map(|e| e.ok()).collect(),
        Err(_) => return Vec::new(),
    };
    entries.sort_by_key(|e| e.file_name());

    let mut nodes = Vec::new();
    for entry in entries {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        let Ok(ft) = entry.file_type() else { continue };
        let Ok(rel) = path.strip_prefix(base) else { continue };
        let rel_str = rel
            .components()
            .map(|c| c.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        if rel_str.is_empty() {
            continue;
        }
        if ft.is_dir() {
            if SKIP_DIRS.contains(&name.as_str()) {
                continue;
            }
            nodes.push(serde_json::json!({
                "name": name,
                "type": "dir",
                "path": rel_str,
                "children": collect_tree(base, &path, depth + 1),
            }));
        } else if ft.is_file() {
            let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            let mtime = std::fs::metadata(&path)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            nodes.push(serde_json::json!({
                "name": name,
                "type": "file",
                "path": rel_str,
                "size": size,
                "mtime": mtime,
            }));
        }
    }
    nodes
}

/// Flatten a nested tree back into the legacy flat file list.
fn flatten_tree(nodes: &[serde_json::Value], out: &mut Vec<serde_json::Value>) {
    for n in nodes {
        if n["type"] == "dir" {
            if let Some(children) = n["children"].as_array() {
                flatten_tree(children, out);
            }
        } else {
            out.push(serde_json::json!({
                "path": n["path"],
                "size": n["size"],
                "mtime": n["mtime"],
            }));
        }
    }
}

async fn list_files(
    State(_state): State<WebState>,
    Query(req): Query<ListFilesRequest>,
) -> Json<serde_json::Value> {
    let base = match workspace_root(req.session.as_deref()).await {
        Ok(b) => b,
        Err(_) => return Json(serde_json::json!({ "root": "", "tree": [], "files": [] })),
    };
    let base = base.canonicalize().unwrap_or(base);

    let tree = collect_tree(&base, &base, 0);
    let mut files = Vec::new();
    flatten_tree(&tree, &mut files);
    files.sort_by(|a, b| a["path"].as_str().unwrap_or("").cmp(b["path"].as_str().unwrap_or("")));

    Json(serde_json::json!({
        "root": base.display().to_string(),
        "tree": tree,
        "files": files,
    }))
}

fn project_json(
    p_type: &str,
    label: &str,
    icon: &str,
    name: String,
    version: String,
    manifest: &str,
    dir: &str,
    deps: Vec<serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({
        "type": p_type,
        "type_label": label,
        "icon": icon,
        "name": name,
        "version": version,
        "manifest": manifest,
        "dir": dir,
        "dependencies": deps,
    })
}

/// Split a spec like `requests>=2.0,<3` or `torch[vision]~=2.1` into
/// name + version.
fn dep_from_spec(spec: &str, kind: &str) -> serde_json::Value {
    let s = spec.trim();
    let name = s
        .split(|c: char| matches!(c, '<' | '>' | '=' | '~' | '!' | '[' | ';' | ' ' | '\t'))
        .next()
        .unwrap_or(s)
        .trim();
    let version = s.strip_prefix(name).unwrap_or("").trim_matches(|c: char| c == ' ' || c == ',' || c == ';' || c == '\t');
    serde_json::json!({ "name": name.to_string(), "version": version.to_string(), "kind": kind })
}

fn sorted_deps(mut deps: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
    deps.sort_by(|a, b| a["name"].as_str().unwrap_or("").cmp(b["name"].as_str().unwrap_or("")));
    deps
}

/// Detect the project manifest in `dir` and parse name/version/dependencies.
/// Precedence: Cargo > npm > pyproject > requirements > go.mod > composer >
/// Maven > Gradle > Gemfile > mix > pubspec > csproj.
fn detect_project(dir: &std::path::Path, base: &std::path::Path) -> Option<serde_json::Value> {
    let rel_str = dir
        .strip_prefix(base)
        .ok()
        .map(|r| r.components().map(|c| c.as_os_str().to_string_lossy()).collect::<Vec<_>>().join("/"))
        .unwrap_or_default();
    let dir_name = dir.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();

    // Rust / Cargo
    let cargo = dir.join("Cargo.toml");
    if cargo.exists() {
        if let Ok(text) = std::fs::read_to_string(&cargo) {
            if let Ok(v) = text.parse::<toml::Value>() {
                let pkg = v.get("package");
                let name = pkg.and_then(|p| p.get("name")).and_then(|n| n.as_str()).unwrap_or("").to_string();
                let version = pkg.and_then(|p| p.get("version")).and_then(|n| n.as_str()).unwrap_or("").to_string();
                let mut deps = Vec::new();
                for (kind, key) in [("deps", "dependencies"), ("dev", "dev-dependencies"), ("build", "build-dependencies")] {
                    if let Some(t) = v.get(key).and_then(|d| d.as_table()) {
                        for (dn, spec) in t {
                            let ver = match spec {
                                toml::Value::String(s) => s.clone(),
                                toml::Value::Table(tbl) => tbl.get("version").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                                _ => String::new(),
                            };
                            deps.push(serde_json::json!({ "name": dn, "version": ver, "kind": kind }));
                        }
                    }
                }
                return Some(project_json("rust", "Rust", "🦀", if name.is_empty() { dir_name } else { name }, version, "Cargo.toml", &rel_str, sorted_deps(deps)));
            }
        }
    }

    // Node.js / npm
    let pkg_json = dir.join("package.json");
    if pkg_json.exists() {
        if let Ok(text) = std::fs::read_to_string(&pkg_json) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                let mut deps = Vec::new();
                for (kind, key) in [("deps", "dependencies"), ("dev", "devDependencies")] {
                    if let Some(t) = v.get(key).and_then(|d| d.as_object()) {
                        for (dn, spec) in t {
                            deps.push(serde_json::json!({ "name": dn, "version": spec.as_str().unwrap_or(""), "kind": kind }));
                        }
                    }
                }
                let name = v.get("name").and_then(|n| n.as_str()).unwrap_or("").to_string();
                let version = v.get("version").and_then(|n| n.as_str()).unwrap_or("").to_string();
                return Some(project_json("node", "Node.js", "⬢", if name.is_empty() { dir_name } else { name }, version, "package.json", &rel_str, sorted_deps(deps)));
            }
        }
    }

    // Python: pyproject.toml
    let pyproject = dir.join("pyproject.toml");
    if pyproject.exists() {
        if let Ok(text) = std::fs::read_to_string(&pyproject) {
            if let Ok(v) = text.parse::<toml::Value>() {
                let proj = v.get("project");
                let name = proj.and_then(|p| p.get("name")).and_then(|n| n.as_str()).unwrap_or("").to_string();
                let version = proj.and_then(|p| p.get("version")).and_then(|n| n.as_str()).unwrap_or("").to_string();
                let mut deps = Vec::new();
                if let Some(arr) = proj.and_then(|p| p.get("dependencies")).and_then(|d| d.as_array()) {
                    for spec in arr {
                        if let Some(s) = spec.as_str() {
                            deps.push(dep_from_spec(s, "deps"));
                        }
                    }
                }
                if let Some(groups) = proj.and_then(|p| p.get("optional-dependencies")).and_then(|d| d.as_table()) {
                    for (g, arr) in groups {
                        if let Some(arr) = arr.as_array() {
                            for spec in arr {
                                if let Some(s) = spec.as_str() {
                                    deps.push(dep_from_spec(s, g));
                                }
                            }
                        }
                    }
                }
                if let Some(pd) = v.get("tool").and_then(|t| t.get("poetry")).and_then(|p| p.get("dependencies")).and_then(|d| d.as_table()) {
                    for (dn, spec) in pd {
                        let ver = match spec {
                            toml::Value::String(s) => s.clone(),
                            toml::Value::Table(tbl) => tbl.get("version").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                            _ => String::new(),
                        };
                        deps.push(serde_json::json!({ "name": dn, "version": ver, "kind": "poetry" }));
                    }
                }
                let name = if name.is_empty() { dir_name } else { name };
                return Some(project_json("python", "Python", "🐍", name, version, "pyproject.toml", &rel_str, sorted_deps(deps)));
            }
        }
    }

    // Python: requirements.txt (fallback)
    let req = dir.join("requirements.txt");
    if req.exists() {
        if let Ok(text) = std::fs::read_to_string(&req) {
            let mut deps = Vec::new();
            for line in text.lines() {
                let l = line.trim();
                if l.is_empty() || l.starts_with('#') || l.starts_with('-') || l.starts_with('.') {
                    continue;
                }
                deps.push(dep_from_spec(l, "deps"));
            }
            return Some(project_json("python", "Python", "🐍", dir_name, String::new(), "requirements.txt", &rel_str, sorted_deps(deps)));
        }
    }

    // Go
    let go_mod = dir.join("go.mod");
    if go_mod.exists() {
        if let Ok(text) = std::fs::read_to_string(&go_mod) {
            let mut module = String::new();
            let mut gover = String::new();
            let mut deps = Vec::new();
            let mut in_block = false;
            for line in text.lines() {
                let l = line.trim();
                if l.is_empty() || l.starts_with("//") {
                    continue;
                }
                if let Some(rest) = l.strip_prefix("module ") {
                    module = rest.trim().to_string();
                } else if let Some(rest) = l.strip_prefix("go ") {
                    gover = rest.trim().to_string();
                } else if l == "require (" {
                    in_block = true;
                } else if l == ")" {
                    in_block = false;
                } else if in_block || l.starts_with("require ") {
                    let spec = l.strip_prefix("require ").unwrap_or(l).trim();
                    let parts: Vec<&str> = spec.split_whitespace().collect();
                    if parts.len() >= 2 {
                        deps.push(serde_json::json!({
                            "name": parts[0],
                            "version": parts[1].trim_matches('"'),
                            "kind": "require",
                        }));
                    }
                }
            }
            let name = module.rsplit('/').next().unwrap_or(&module).to_string();
            return Some(project_json("go", "Go", "🐹", if name.is_empty() { dir_name } else { name }, gover, "go.mod", &rel_str, sorted_deps(deps)));
        }
    }

    // PHP / Composer
    let composer = dir.join("composer.json");
    if composer.exists() {
        if let Ok(text) = std::fs::read_to_string(&composer) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                let mut deps = Vec::new();
                for (kind, key) in [("require", "require"), ("dev", "require-dev")] {
                    if let Some(t) = v.get(key).and_then(|d| d.as_object()) {
                        for (dn, spec) in t {
                            deps.push(serde_json::json!({ "name": dn, "version": spec.as_str().unwrap_or(""), "kind": kind }));
                        }
                    }
                }
                let name = v.get("name").and_then(|n| n.as_str()).unwrap_or("").to_string();
                let version = v.get("version").and_then(|n| n.as_str()).unwrap_or("").to_string();
                return Some(project_json("php", "PHP", "🐘", if name.is_empty() { dir_name } else { name }, version, "composer.json", &rel_str, sorted_deps(deps)));
            }
        }
    }

    // Java / Maven
    let pom = dir.join("pom.xml");
    if pom.exists() {
        if let Ok(text) = std::fs::read_to_string(&pom) {
            let re = regex::Regex::new(r"(?s)<dependency>\s*<groupId>([^<]+)</groupId>\s*<artifactId>([^<]+)</artifactId>\s*(?:<version>([^<]+)</version>)?").unwrap();
            let re_art = regex::Regex::new(r"<artifactId>([^<]+)</artifactId>").unwrap();
            let re_ver = regex::Regex::new(r"<version>([^<]+)</version>").unwrap();
            let mut deps = Vec::new();
            for caps in re.captures_iter(&text) {
                deps.push(serde_json::json!({
                    "name": caps.get(2).map(|m| m.as_str()).unwrap_or("").to_string(),
                    "version": caps.get(3).map(|m| m.as_str()).unwrap_or("").to_string(),
                    "kind": "deps",
                }));
            }
            let name = re_art.captures(&text).map(|c| c[1].to_string()).unwrap_or_else(|| dir_name.clone());
            let version = re_ver.captures(&text).map(|c| c[1].to_string()).unwrap_or_default();
            return Some(project_json("java", "Java (Maven)", "☕", name, version, "pom.xml", &rel_str, sorted_deps(deps)));
        }
    }

    // Java / Gradle
    for g in ["build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts"] {
        if dir.join(g).exists() {
            return Some(project_json("java", "Java (Gradle)", "☕", dir_name, String::new(), g, &rel_str, Vec::new()));
        }
    }

    // Ruby / Gemfile
    let gemfile = dir.join("Gemfile");
    if gemfile.exists() {
        if let Ok(text) = std::fs::read_to_string(&gemfile) {
            let re = regex::Regex::new(r#"^\s*gem\s+['"]([^'"]+)['"]"#).unwrap();
            let mut deps = Vec::new();
            for caps in re.captures_iter(&text) {
                deps.push(serde_json::json!({ "name": caps[1].to_string(), "version": String::new(), "kind": "gem" }));
            }
            return Some(project_json("ruby", "Ruby", "💎", dir_name, String::new(), "Gemfile", &rel_str, sorted_deps(deps)));
        }
    }

    // Elixir / mix
    if dir.join("mix.exs").exists() {
        return Some(project_json("elixir", "Elixir", "💧", dir_name, String::new(), "mix.exs", &rel_str, Vec::new()));
    }

    // Dart / Flutter
    let pubspec = dir.join("pubspec.yaml");
    if pubspec.exists() {
        if let Ok(text) = std::fs::read_to_string(&pubspec) {
            let re = regex::Regex::new(r"(?m)^name:\s*([^\s#]+)").unwrap();
            let name = re.captures(&text).map(|c| c[1].to_string()).unwrap_or_else(|| dir_name.clone());
            return Some(project_json("dart", "Dart/Flutter", "🎯", name, String::new(), "pubspec.yaml", &rel_str, Vec::new()));
        }
    }

    // .NET
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.filter_map(|e| e.ok()) {
            if let Some(ext) = e.path().extension() {
                if ext == "csproj" || ext == "fsproj" {
                    let name = e.file_name().to_string_lossy().to_string();
                    return Some(project_json("dotnet", ".NET", "🔷", name.clone(), String::new(), &name, &rel_str, Vec::new()));
                }
            }
        }
    }

    None
}

/// Report detected projects at the workspace root and its immediate
/// subdirectories, with parsed dependencies per project type.
async fn project_info(
    State(_state): State<WebState>,
    Query(req): Query<ListFilesRequest>,
) -> Json<serde_json::Value> {
    let base = match workspace_root(req.session.as_deref()).await {
        Ok(b) => b,
        Err(_) => return Json(serde_json::json!({ "projects": [] })),
    };
    let base = base.canonicalize().unwrap_or(base);

    let mut dirs = vec![base.clone()];
    if let Ok(rd) = std::fs::read_dir(&base) {
        for e in rd.filter_map(|e| e.ok()) {
            let Ok(ft) = e.file_type() else { continue };
            if !ft.is_dir() {
                continue;
            }
            let name = e.file_name().to_string_lossy().to_string();
            if SKIP_DIRS.contains(&name.as_str()) {
                continue;
            }
            dirs.push(e.path());
        }
    }

    let mut projects = Vec::new();
    for d in dirs {
        if let Some(p) = detect_project(&d, &base) {
            projects.push(p);
        }
    }
    Json(serde_json::json!({ "projects": projects }))
}

/// Resolve the session workspace for the web editor: creates (or reuses) the
/// same session the agent runtime uses for this peer, so the browser sees the
/// temp folder the agent writes generated code into.
async fn session_info(
    State(state): State<WebState>,
    Query(req): Query<SessionRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let agent_name = req.agent.as_deref().unwrap_or("main");
    let peer = req.peer.unwrap_or_else(|| "web-user".to_string());

    let agent = state
        .agent_manager
        .get(agent_name)
        .ok_or(StatusCode::NOT_FOUND)?;

    let session = agent
        .sessions
        .get_or_create(agent_name, "web", &peer, None)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let workspace = crate::agent::session_workspace_dir(&session.id);
    tokio::fs::create_dir_all(&workspace)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(serde_json::json!({
        "agent": agent_name,
        "peer": peer,
        "session_id": session.id,
        "workspace": workspace.display().to_string(),
    })))
}

async fn delete_file(
    State(_state): State<WebState>,
    Query(req): Query<FileRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let base = workspace_root(req.session.as_deref()).await?;
    let path = resolve_workspace_path(&base, &req.path)?;
    if path.is_dir() {
        return Err(StatusCode::BAD_REQUEST);
    }
    tokio::fs::remove_file(&path)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "path": req.path,
    })))
}
