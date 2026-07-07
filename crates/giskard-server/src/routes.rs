use std::path::{Path, PathBuf};

use axum::{
    Router,
    extract::{
        Path as AxumPath, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    middleware,
    response::{IntoResponse, Json},
    routing::{get, post},
};
use chrono::Utc;
use serde::Deserialize;
use tracing::{debug, error, warn};

use futures::{SinkExt, StreamExt};
use giskard_core::ids::ProjectId;
use giskard_core::turn::{Mode, TurnOverrides};
use giskard_core::user_input::UserInput;
use giskard_persist::store::ThreadFile;
use giskard_proto::*;
use tokio::sync::mpsc;

use crate::AppState;
use crate::auth::{
    SESSION_COOKIE, auth_middleware, create_session_cookie, get_session_token_from_header,
    verify_session,
};

pub fn protected_routes(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/api/projects", get(list_projects).post(create_project))
        .route(
            "/api/projects/{id}",
            get(get_project).delete(delete_project),
        )
        .route(
            "/api/projects/{id}/threads",
            get(list_threads).post(open_thread),
        )
        .route("/api/projects/{id}/highlight", get(highlight_file))
        .route("/api/projects/{id}/raw", get(download_file))
        .route("/api/projects/{id}/linkify", post(linkify))
        .route("/api/browse", get(browse))
        .route("/api/models", get(list_models))
        .route("/api/models/refresh", post(refresh_models))
        .route("/api/tokens", get(global_tokens))
        .route("/api/projects/{id}/tokens", get(project_tokens))
        .route("/api/logout", post(logout))
        .route("/api/ws", get(ws_handler))
        .layer(middleware::from_fn_with_state(state, auth_middleware))
}

pub fn public_routes() -> Router<AppState> {
    Router::new()
        .route("/", get(index))
        .route("/api/login", post(login))
}

/// Serve the single-page desktop UI (§13). Self-contained HTML/CSS/JS (no npm); it authenticates
/// via `/api/login` and drives the app through the same REST + WS API as any client.
async fn index() -> axum::response::Html<&'static str> {
    axum::response::Html(include_str!("../static/index.html"))
}

async fn login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> Result<axum::response::Response, (axum::http::StatusCode, String)> {
    let config = state
        .store
        .load_config()
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let hash = if let Some(h) = config.auth.password_hash.as_deref() {
        Some(h.to_string())
    } else {
        std::env::var("GISKARD_PASSWORD_HASH").ok()
    };

    let ok = match hash.as_deref() {
        Some(h) => verify_password(&req.password, h),
        None => {
            warn!("no password hash configured, denying login");
            false
        }
    };

    if !ok {
        return Ok(Json(LoginResponse { ok: false }).into_response());
    }

    let expiry = (Utc::now().timestamp() as u64) + (config.auth.session_days as u64) * 86400;
    let cookie = create_session_cookie(expiry, &state.session_key, config.server.secure_cookies);

    let mut response = Json(LoginResponse { ok: true }).into_response();
    response.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        axum::http::HeaderValue::from_str(&cookie).unwrap(),
    );
    Ok(response)
}

async fn logout() -> impl IntoResponse {
    let expired = format!("{SESSION_COOKIE}=expired; HttpOnly; SameSite=Strict; Path=/; Max-Age=0");
    (
        [(axum::http::header::SET_COOKIE, expired)],
        Json(LoginResponse { ok: false }),
    )
}

fn verify_password(password: &str, hash: &str) -> bool {
    use argon2::{Argon2, PasswordHash, PasswordVerifier};
    match PasswordHash::new(hash) {
        Ok(parsed) => Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok(),
        Err(e) => {
            error!("invalid password hash: {e}");
            false
        }
    }
}

async fn list_projects(
    State(state): State<AppState>,
) -> Result<Json<ListProjectsResponse>, ApiError> {
    let index = state.store.load_project_index().await?;
    let projects = index
        .projects
        .iter()
        .map(|e| ProjectSummary {
            id: e.id,
            name: e.name.clone(),
            dir: e.dir.clone(),
            created_at: e.created_at,
        })
        .collect();
    Ok(Json(ListProjectsResponse { projects }))
}

async fn create_project(
    State(state): State<AppState>,
    Json(req): Json<CreateProjectRequest>,
) -> Result<Json<CreateProjectResponse>, ApiError> {
    let id = ProjectId::new();
    state
        .store
        .create_project(
            id,
            &req.name,
            &req.dir,
            req.default_model.clone(),
            req.approval_policy,
        )
        .await?;

    if let Some(ws_root) = &req.workspace_root {
        let mut config = state
            .store
            .load_project(id)
            .await?
            .ok_or(ApiError::NotFound)?;
        config.workspace_root = Some(ws_root.clone());
        state.store.save_project(&config).await?;
    }

    Ok(Json(CreateProjectResponse { id }))
}

async fn get_project(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<ProjectId>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let config = state
        .store
        .load_project(id)
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(serde_json::to_value(config).unwrap()))
}

async fn delete_project(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<ProjectId>,
) -> Result<axum::http::StatusCode, ApiError> {
    state.store.delete_project(id).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

async fn list_threads(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<ProjectId>,
) -> Result<Json<ListThreadsResponse>, ApiError> {
    let thread_ids = state.store.list_threads(project_id).await?;
    let mut threads = Vec::new();
    for tid in thread_ids {
        if let Ok(Some(tf)) = state.store.load_thread(project_id, tid).await {
            threads.push(ThreadSummary {
                id: tf.id,
                title: tf.title,
                mode: tf.mode,
                created_at: tf.created_at,
                updated_at: tf.updated_at,
            });
        }
    }
    threads.sort_by_key(|t| std::cmp::Reverse(t.updated_at));
    Ok(Json(ListThreadsResponse { threads }))
}

async fn open_thread(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<ProjectId>,
    Json(req): Json<OpenThreadRequest>,
) -> Result<Json<OpenThreadResponse>, ApiError> {
    let project_config = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;

    let ws_root = project_config
        .workspace_root
        .as_deref()
        .unwrap_or(&project_config.dir);

    let handle = state
        .registry
        .open_thread(
            &project_config,
            ws_root,
            req.resume,
            project_config.default_model.clone(),
        )
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    let now = Utc::now();
    let thread_file = ThreadFile {
        version: 1,
        id: handle.thread,
        project_id,
        title: "New thread".into(),
        harness_thread_id: handle.harness_thread_id.clone(),
        mode: Mode::Build,
        current_model: project_config.default_model.clone(),
        context_window: 128_000,
        approval_policy: None,
        model_efforts: std::collections::HashMap::new(),
        tokens: giskard_core::token::TokenLedger::default(),
        created_at: now,
        updated_at: now,
        turns: vec![],
    };
    state.store.save_thread(project_id, &thread_file).await?;

    Ok(Json(OpenThreadResponse {
        thread_id: handle.thread,
        harness_thread_id: handle.harness_thread_id,
    }))
}

#[derive(Deserialize)]
struct BrowseQuery {
    path: Option<String>,
}

async fn browse(
    State(state): State<AppState>,
    Query(q): Query<BrowseQuery>,
) -> Result<Json<BrowseResponse>, ApiError> {
    let config = state.store.load_config().await?;
    let raw_path = q.path.unwrap_or_else(|| "/".into());
    let path = PathBuf::from(&raw_path);
    let canonical = path
        .canonicalize()
        .map_err(|e| ApiError::BadRequest(format!("cannot canonicalize path: {e}")))?;

    if !config.browse.roots.is_empty() {
        let allowed = config.browse.roots.iter().any(|root| {
            let root = PathBuf::from(root);
            let root_canonical = root.canonicalize().unwrap_or(root);
            canonical.starts_with(&root_canonical)
        });
        if !allowed {
            return Err(ApiError::Forbidden("path outside allowed roots".into()));
        }
    }

    let mut entries = Vec::new();
    let mut reader = match tokio::fs::read_dir(&canonical).await {
        Ok(r) => r,
        Err(e) => return Err(ApiError::BadRequest(format!("cannot read directory: {e}"))),
    };

    while let Ok(Some(entry)) = reader.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        let metadata = match entry.metadata().await {
            Ok(m) => m,
            Err(_) => continue,
        };
        let mtime = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| {
                chrono::DateTime::<Utc>::from_timestamp(d.as_secs() as i64, 0).unwrap_or_default()
            })
            .unwrap_or_default();
        entries.push(DirEntry {
            name,
            is_dir: metadata.is_dir(),
            size: metadata.len(),
            mtime,
        });
    }

    entries.sort_by(|a, b| {
        b.is_dir
            .cmp(&a.is_dir)
            .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
    });

    Ok(Json(BrowseResponse {
        path: canonical.to_string_lossy().to_string(),
        entries,
    }))
}

/// Query parameters for the highlight endpoint.
#[derive(Deserialize)]
struct HighlightQuery {
    /// Relative or absolute path to the file (confined to workspace root).
    path: String,
    /// 1-based start line (inclusive) for pagination.
    start: Option<usize>,
    /// 1-based end line (inclusive) for pagination.
    end: Option<usize>,
}

/// `GET /api/projects/{id}/highlight` — syntax-highlighted file content (spec §11.2).
///
/// Returns highlighted HTML, detected language, binary flag, total line count,
/// and file size. The `start`/`end` query params enable line-range pagination
/// for large files. Path is confined to the project's workspace root.
async fn highlight_file(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<ProjectId>,
    Query(q): Query<HighlightQuery>,
) -> Result<Json<HighlightResponse>, ApiError> {
    let project = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;

    let workspace_root = PathBuf::from(project.workspace_root.as_deref().unwrap_or(&project.dir));
    let resolved = resolve_confined_path(&workspace_root, &q.path)
        .ok_or(ApiError::Forbidden("path escapes workspace root".into()))?;

    let result = state
        .highlighter
        .highlight_file(&resolved, q.start, q.end)
        .await
        .map_err(ApiError::BadRequest)?;

    Ok(Json(HighlightResponse {
        html: result.html,
        language: result.language,
        is_binary: result.is_binary,
        total_lines: result.total_lines,
        file_size: result.file_size,
    }))
}

/// Query parameters for the raw file download endpoint.
#[derive(Deserialize)]
struct RawQuery {
    /// Relative or absolute path to the file (confined to workspace root).
    path: String,
}

/// `GET /api/projects/{id}/raw` — download a raw file (spec §11.2).
///
/// Returns the file contents as `application/octet-stream` with a
/// `Content-Disposition: attachment` header. Path is confined to the
/// project's workspace root.
async fn download_file(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<ProjectId>,
    Query(q): Query<RawQuery>,
) -> Result<axum::response::Response, ApiError> {
    let project = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;

    let workspace_root = PathBuf::from(project.workspace_root.as_deref().unwrap_or(&project.dir));
    let resolved = resolve_confined_path(&workspace_root, &q.path)
        .ok_or(ApiError::Forbidden("path escapes workspace root".into()))?;

    let bytes = tokio::fs::read(&resolved)
        .await
        .map_err(|e| ApiError::BadRequest(format!("cannot read file: {e}")))?;

    let filename = resolved
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "file".into());

    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        [(
            axum::http::header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{filename}\""),
        )],
        bytes,
    )
        .into_response())
}

/// `POST /api/projects/{id}/linkify` — detect file paths in agent text (spec §11.2).
///
/// Scans the request body's `text` field for strings that look like file paths,
/// resolves them against the project's workspace root, and returns byte-offset
/// spans for each path that points to an existing file. The client uses these
/// spans to render clickable links in agent messages.
async fn linkify(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<ProjectId>,
    Json(req): Json<LinkifyRequest>,
) -> Result<Json<LinkifyResponse>, ApiError> {
    let project = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;

    let workspace_root = PathBuf::from(project.workspace_root.as_deref().unwrap_or(&project.dir));
    let root_canonical = workspace_root.canonicalize().unwrap_or(workspace_root);

    let spans = crate::linkify::linkify_text(&req.text, &root_canonical);
    let links = spans
        .into_iter()
        .map(|s| LinkSpanResponse {
            start: s.start,
            end: s.end,
            path: s.path,
        })
        .collect();

    Ok(Json(LinkifyResponse { links }))
}

/// Canonicalize a requested path and verify it stays within the workspace root.
///
/// Returns `None` if the path escapes the workspace root after symlink
/// resolution. Used by the highlight, raw download, and linkify endpoints to
/// prevent path traversal attacks.
fn resolve_confined_path(workspace_root: &Path, requested: &str) -> Option<PathBuf> {
    let root_canonical = workspace_root.canonicalize().ok()?;
    let candidate = if Path::new(requested).is_absolute() {
        PathBuf::from(requested)
    } else {
        root_canonical.join(requested)
    };
    let canonical = candidate.canonicalize().ok()?;
    if canonical.starts_with(&root_canonical) {
        Some(canonical)
    } else {
        None
    }
}

async fn list_models(State(state): State<AppState>) -> Result<Json<ListModelsResponse>, ApiError> {
    let config = state.store.load_config().await?;
    Ok(Json(ListModelsResponse {
        models: crate::models::list_descriptors(&config),
    }))
}

/// `POST /api/models/refresh` — merge each listing-enabled provider's `/v1/models` over the static
/// list (spec §8.3). Best-effort: always returns at least the static list.
async fn refresh_models(
    State(state): State<AppState>,
) -> Result<Json<ListModelsResponse>, ApiError> {
    let config = state.store.load_config().await?;
    Ok(Json(ListModelsResponse {
        models: crate::models::refresh_models(&config).await,
    }))
}

/// `GET /api/tokens` — the global token dashboard (day/week/month/total, §10.2).
async fn global_tokens(State(state): State<AppState>) -> Result<Json<TokenReport>, ApiError> {
    let config = state.store.load_config().await?;
    let ledger = state.store.load_global_tokens().await?.unwrap_or_default();
    Ok(Json(crate::tokens::build_report(&ledger, &config.tokens)))
}

/// `GET /api/projects/{id}/tokens` — a project's token dashboard (§10.2).
async fn project_tokens(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<ProjectId>,
) -> Result<Json<TokenReport>, ApiError> {
    let config = state.store.load_config().await?;
    let ledger = state
        .store
        .load_project_tokens(project_id)
        .await?
        .unwrap_or_default();
    Ok(Json(crate::tokens::build_report(&ledger, &config.tokens)))
}

async fn ws_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    let cookie_header = headers
        .get(axum::http::header::COOKIE)
        .and_then(|v| v.to_str().ok());

    let valid = cookie_header
        .and_then(get_session_token_from_header)
        .as_ref()
        .map(|t| verify_session(t, &state.session_key))
        .unwrap_or(false);

    if !valid {
        return Err(axum::http::StatusCode::UNAUTHORIZED);
    }

    Ok(ws.on_upgrade(move |socket| handle_ws(socket, state)))
}

async fn handle_ws(socket: WebSocket, state: AppState) {
    let (tx, mut rx) = mpsc::channel::<ServerMessage>(256);
    let client_id = state.hub.next_client_id();

    let (mut ws_sender, mut ws_receiver) = socket.split();

    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let json = serde_json::to_string(&msg).unwrap();
            if ws_sender.send(Message::Text(json.into())).await.is_err() {
                break;
            }
        }
    });

    let hub = state.hub.clone();

    loop {
        match ws_receiver.next().await {
            Some(Ok(Message::Text(text))) => {
                let msg: ClientMessage = match serde_json::from_str(&text) {
                    Ok(m) => m,
                    Err(e) => {
                        warn!("invalid WS message: {e}");
                        continue;
                    }
                };
                if let Err(e) = handle_client_msg(&state, client_id, &tx, msg).await {
                    error!("WS handler error: {e}");
                }
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                warn!("WS receive error: {e}");
                break;
            }
            None => break,
        }
    }

    hub.disconnect(client_id).await;
    send_task.abort();
}

async fn handle_client_msg(
    state: &AppState,
    client_id: usize,
    tx: &mpsc::Sender<ServerMessage>,
    msg: ClientMessage,
) -> Result<(), String> {
    match msg {
        ClientMessage::Subscribe { thread_id } => {
            state.hub.subscribe(thread_id, client_id, tx.clone()).await;

            if let Some(project_id) = state.registry.get_project_for_thread(thread_id).await {
                if let Ok(Some(tf)) = state.store.load_thread(project_id, thread_id).await {
                    let _ = tx
                        .send(ServerMessage::ThreadState(giskard_proto::ThreadState {
                            thread_id,
                            state: serde_json::to_value(&tf).unwrap(),
                        }))
                        .await;
                }
            }

            if let Some(snap) = state.live_buffers.snapshot(thread_id).await {
                let _ = tx.send(ServerMessage::LiveTurnSnapshot(snap)).await;
            }
        }
        ClientMessage::Unsubscribe { thread_id } => {
            state.hub.unsubscribe(thread_id, client_id).await;
        }
        ClientMessage::SendInput { thread_id, text } => {
            let project_id = project_for(state, thread_id).await?;
            let project = state
                .store
                .load_project(project_id)
                .await
                .map_err(|e| e.to_string())?
                .ok_or("project not found")?;

            // RMW under the per-thread lock: bump activity and read back the resolved state.
            let tf = state
                .store
                .update_thread(project_id, thread_id, |tf| {
                    tf.updated_at = chrono::Utc::now();
                })
                .await
                .map_err(|e| e.to_string())?
                .ok_or("thread not found")?;

            let effective_model = tf.current_model.clone();

            // Resolved snapshot the harness applies to `turn/start` (§7.5, §8.4/§8.5):
            //  - the thread's current model (carrying its reasoning effort), so a mid-thread
            //    model/effort change actually reaches the agent. Passing `None` here would leave
            //    Codex on whatever model was set at `thread/start`.
            //  - the effective approval policy: a per-thread override wins over the project's (§9).
            let overrides = TurnOverrides {
                model: Some(effective_model.clone()),
                mode: tf.mode,
                approval_policy: tf.approval_policy.unwrap_or(project.approval_policy),
            };

            state
                .registry
                .start_turn(thread_id, UserInput::text(text), overrides, effective_model)
                .await
                .map_err(|e| e.to_string())?;
        }
        ClientMessage::SwitchMode { thread_id, mode } => {
            let project_id = project_for(state, thread_id).await?;
            let tf = state
                .store
                .update_thread(project_id, thread_id, |tf| {
                    tf.mode = mode;
                    tf.updated_at = chrono::Utc::now();
                })
                .await
                .map_err(|e| e.to_string())?
                .ok_or("thread not found")?;
            broadcast_thread_state(state, thread_id, &tf).await;
        }
        ClientMessage::SelectModel {
            thread_id,
            model_ref,
        } => {
            let project_id = project_for(state, thread_id).await?;
            let config = state.store.load_config().await.map_err(|e| e.to_string())?;

            // All model/effort resolution happens inside the RMW closure so it sees the
            // authoritative current model under the per-thread lock (§5.4, C7 effort retention).
            let tf = state
                .store
                .update_thread(project_id, thread_id, move |tf| {
                    let old = crate::models::resolve_descriptor(&config, &tf.current_model);
                    if old.supports_reasoning_effort {
                        if let Some(effort) = tf.current_model.reasoning_effort {
                            tf.model_efforts.insert(tf.current_model.key(), effort);
                        }
                    }

                    let new_descriptor = crate::models::resolve_descriptor(&config, &model_ref);
                    let mut new_model = model_ref.clone();
                    if new_descriptor.supports_reasoning_effort {
                        if new_model.reasoning_effort.is_none() {
                            new_model.reasoning_effort =
                                tf.model_efforts.get(&new_model.key()).copied();
                        }
                    } else {
                        new_model.reasoning_effort = None;
                    }

                    tf.context_window = crate::models::context_window_for(&config, &new_model);
                    tf.current_model = new_model;
                    tf.updated_at = chrono::Utc::now();
                })
                .await
                .map_err(|e| e.to_string())?
                .ok_or("thread not found")?;
            broadcast_thread_state(state, thread_id, &tf).await;
        }
        ClientMessage::SetApprovalPolicy {
            thread_id,
            project_id,
            policy,
        } => {
            // Two targets (§9): a `thread_id` sets the per-thread override (consumed by SendInput
            // via `tf.approval_policy.unwrap_or(project)`); a `project_id` changes the project
            // default that applies to threads without an override. `thread_id` wins if both are set.
            if let Some(tid) = thread_id {
                let pid = project_for(state, tid).await?;
                let tf = state
                    .store
                    .update_thread(pid, tid, |tf| {
                        tf.approval_policy = Some(policy);
                        tf.updated_at = chrono::Utc::now();
                    })
                    .await
                    .map_err(|e| e.to_string())?
                    .ok_or("thread not found")?;
                broadcast_thread_state(state, tid, &tf).await;
            } else if let Some(pid) = project_id {
                let mut config = state
                    .store
                    .load_project(pid)
                    .await
                    .map_err(|e| e.to_string())?
                    .ok_or("project not found")?;
                config.approval_policy = policy;
                config.updated_at = chrono::Utc::now();
                state
                    .store
                    .save_project(&config)
                    .await
                    .map_err(|e| e.to_string())?;
            } else {
                return Err("SetApprovalPolicy requires thread_id or project_id".into());
            }
        }
        ClientMessage::ApprovalDecision {
            request_id,
            decision,
        } => {
            state
                .registry
                .respond_approval(giskard_core::ids::ApprovalId(request_id), decision)
                .await
                .map_err(|e| e.to_string())?;
        }
        ClientMessage::Interrupt { thread_id } => {
            state
                .registry
                .interrupt(thread_id)
                .await
                .map_err(|e| e.to_string())?;
        }
        ClientMessage::SavePlan { thread_id, path } => {
            match save_plan(state, thread_id, &path).await {
                Ok(written) => {
                    debug!(%thread_id, path = %written, "plan saved");
                }
                Err(e) => {
                    let _ = tx
                        .send(ServerMessage::Error {
                            message: format!("save plan failed: {e}"),
                        })
                        .await;
                }
            }
        }
        ClientMessage::Ping => {
            let _ = tx.send(ServerMessage::Pong).await;
        }
    }
    Ok(())
}

/// Resolve the project a currently-open thread belongs to (via the harness registry).
async fn project_for(
    state: &AppState,
    thread_id: giskard_core::ids::ThreadId,
) -> Result<ProjectId, String> {
    state
        .registry
        .get_project_for_thread(thread_id)
        .await
        .ok_or_else(|| "thread not open".to_string())
}

/// Load a thread file plus the project it belongs to (via the harness registry).
async fn load_thread(
    state: &AppState,
    thread_id: giskard_core::ids::ThreadId,
) -> Result<(ProjectId, ThreadFile), String> {
    let project_id = state
        .registry
        .get_project_for_thread(thread_id)
        .await
        .ok_or("thread not open")?;
    let tf = state
        .store
        .load_thread(project_id, thread_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("thread not found")?;
    Ok((project_id, tf))
}

/// Push the updated persisted thread snapshot to all subscribers (§13.6).
async fn broadcast_thread_state(
    state: &AppState,
    thread_id: giskard_core::ids::ThreadId,
    tf: &ThreadFile,
) {
    if let Ok(value) = serde_json::to_value(tf) {
        state
            .hub
            .broadcast(
                thread_id,
                ServerMessage::ThreadState(giskard_proto::ThreadState {
                    thread_id,
                    state: value,
                }),
            )
            .await;
    }
}

/// Write the current plan to a markdown file inside the workspace root (§7.4.1). Returns the
/// path actually written (workspace-relative when possible).
async fn save_plan(
    state: &AppState,
    thread_id: giskard_core::ids::ThreadId,
    requested_path: &str,
) -> Result<String, String> {
    let (project_id, tf) = load_thread(state, thread_id).await?;
    let project = state
        .store
        .load_project(project_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("project not found")?;
    let workspace_root = PathBuf::from(project.workspace_root.as_deref().unwrap_or(&project.dir));

    let markdown = crate::plan::extract_plan_markdown(&tf).ok_or("no plan-mode content to save")?;

    let config = state.store.load_config().await.map_err(|e| e.to_string())?;
    let path = if requested_path.trim().is_empty() {
        let ts = chrono::Utc::now().format("%Y%m%d-%H%M").to_string();
        crate::plan::default_plan_path(
            &config.plan.default_dir,
            &config.plan.filename_template,
            &tf.title,
            &ts,
        )
    } else {
        requested_path.to_string()
    };

    let target =
        crate::plan::safe_plan_path(&workspace_root, &path).ok_or("path escapes workspace root")?;

    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| e.to_string())?;
    }
    tokio::fs::write(&target, markdown)
        .await
        .map_err(|e| e.to_string())?;

    Ok(target
        .strip_prefix(&workspace_root)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| target.to_string_lossy().to_string()))
}

#[derive(Debug)]
pub enum ApiError {
    NotFound,
    BadRequest(String),
    Forbidden(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, msg) = match self {
            ApiError::NotFound => (axum::http::StatusCode::NOT_FOUND, "not found".into()),
            ApiError::BadRequest(msg) => (axum::http::StatusCode::BAD_REQUEST, msg),
            ApiError::Forbidden(msg) => (axum::http::StatusCode::FORBIDDEN, msg),
            ApiError::Internal(msg) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, msg),
        };
        (status, msg).into_response()
    }
}

impl From<giskard_core::PersistError> for ApiError {
    fn from(e: giskard_core::PersistError) -> Self {
        ApiError::Internal(e.to_string())
    }
}
