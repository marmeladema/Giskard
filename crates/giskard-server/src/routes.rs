use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::{
    Router,
    extract::{
        Path as AxumPath, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    middleware,
    response::{IntoResponse, Json},
    routing::{delete, get, patch, post},
};
use chrono::Utc;
use serde::Deserialize;
use tracing::{debug, error, warn};

use futures::{SinkExt, StreamExt};
use giskard_core::error::{HarnessError, PersistError};
use giskard_core::ids::{ProjectId, ThreadId};
use giskard_core::turn::{ApprovalPolicy, Mode, TurnOverrides};
use giskard_core::user_input::UserInput;
use giskard_persist::store::{ProjectConfig, ThreadFile};
use giskard_proto::*;
use tokio::sync::mpsc;

use crate::AppState;
use crate::auth::{
    SESSION_COOKIE, auth_middleware, create_session_cookie, get_session_token_from_header,
    sign_session, verify_session,
};

const HARNESS_CONTROL_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_THREAD_TITLE_CHARS: usize = 120;

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
        .route(
            "/api/projects/{id}/threads/{thread_id}",
            delete(delete_thread),
        )
        .route(
            "/api/projects/{id}/threads/{thread_id}/title",
            patch(rename_thread),
        )
        .route(
            "/api/projects/{id}/threads/{thread_id}/archive",
            post(archive_thread),
        )
        .route("/api/projects/{id}/highlight", get(highlight_file))
        .route("/api/projects/{id}/raw", get(download_file))
        .route("/api/projects/{id}/linkify", post(linkify))
        .route("/api/projects/{id}/render", post(render_markdown))
        .route("/api/browse", get(browse))
        .route("/api/browse/mkdir", post(browse_mkdir))
        .route("/api/models", get(list_models))
        .route("/api/models/refresh", post(refresh_models))
        .route("/api/projects/{id}/mcp", get(list_mcp_servers))
        .route("/api/projects/{id}/mcp/reload", post(reload_mcp_servers))
        .route(
            "/api/projects/{id}/mcp/oauth-login",
            post(start_mcp_oauth_login),
        )
        .route("/api/tokens", get(global_tokens))
        .route("/api/projects/{id}/tokens", get(project_tokens))
        .route("/api/logout", post(logout))
        .route("/api/ws-ticket", get(ws_ticket))
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
    let cookie = create_session_cookie(expiry, &state.session_key, config.server.secure_cookies)
        .map_err(|e| {
            error!("failed to sign session cookie: {e}");
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "failed to create session cookie".to_string(),
            )
        })?;

    let mut response = Json(LoginResponse { ok: true }).into_response();
    let cookie = axum::http::HeaderValue::from_str(&cookie).map_err(|e| {
        error!("failed to create session cookie header: {e}");
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create session cookie".to_string(),
        )
    })?;
    response
        .headers_mut()
        .insert(axum::http::header::SET_COOKIE, cookie);
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
    let config = state.store.load_config().await?;
    let default_model = crate::models::normalize_model_ref(&config, &req.default_model);
    state
        .store
        .create_project(id, &req.name, &req.dir, default_model)
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
    let value = serde_json::to_value(config)
        .map_err(|e| ApiError::Internal(format!("failed to serialize project: {e}")))?;
    Ok(Json(value))
}

async fn delete_project(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<ProjectId>,
) -> Result<axum::http::StatusCode, ApiError> {
    state.store.delete_project(id).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

async fn reject_thread_mutation_if_live(
    state: &AppState,
    thread_id: ThreadId,
) -> Result<(), ApiError> {
    if state.live_buffers.is_active(thread_id).await {
        return Err(ApiError::Conflict(
            "thread has an active turn; stop it before archiving or deleting".into(),
        ));
    }
    if !state.running_commands.snapshot(thread_id).await.is_empty() {
        return Err(ApiError::Conflict(
            "thread has running commands; stop them before archiving or deleting".into(),
        ));
    }
    Ok(())
}

async fn list_threads(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<ProjectId>,
) -> Result<Json<ListThreadsResponse>, ApiError> {
    let thread_ids = state.store.list_threads(project_id).await?;
    let mut threads = Vec::new();
    for tid in thread_ids {
        if let Ok(Some(tf)) = state.store.load_thread(project_id, tid).await {
            threads.push(thread_summary(&tf));
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
    let app_config = state.store.load_config().await?;
    let mut project_config = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let default_model =
        crate::models::normalize_model_ref(&app_config, &project_config.default_model);
    if default_model != project_config.default_model {
        project_config.default_model = default_model;
        project_config.updated_at = Utc::now();
        state.store.save_project(&project_config).await?;
    }

    let ws_root = project_config
        .workspace_root
        .as_deref()
        .unwrap_or(&project_config.dir);
    debug!(
        %project_id,
        thread_id = ?req.thread_id,
        resume = ?req.resume,
        action = "open_thread",
        "open thread request"
    );

    if let Some(thread_id) = req.thread_id {
        let thread_file = state
            .store
            .load_thread(project_id, thread_id)
            .await?
            .ok_or(ApiError::NotFound)?;
        let current_model =
            crate::models::normalize_model_ref(&app_config, &thread_file.current_model);
        let model_changed = current_model != thread_file.current_model;
        if model_changed {
            state
                .store
                .update_thread(project_id, thread_id, |tf| {
                    tf.current_model = current_model.clone();
                    tf.context_window =
                        crate::models::context_window_for(&app_config, &current_model);
                    tf.updated_at = Utc::now();
                })
                .await?;
        }
        if let Some(handle) = state.registry.get_thread_handle(thread_id).await {
            return Ok(Json(OpenThreadResponse {
                thread_id: handle.thread,
                harness_thread_id: handle.harness_thread_id,
                warning: None,
            }));
        }

        let handle = state
            .registry
            .open_thread(
                &project_config,
                ws_root,
                Some(thread_id),
                Some(thread_file.harness_thread_id.clone()),
                current_model.clone(),
            )
            .await
            .map_err(|e| ApiError::Internal(e.to_string()))?;

        if handle.thread != thread_id {
            return Err(ApiError::Internal(format!(
                "harness resumed wrong thread: expected {thread_id}, got {}",
                handle.thread
            )));
        }

        if handle.harness_thread_id != thread_file.harness_thread_id {
            state
                .store
                .update_thread(project_id, thread_id, |tf| {
                    tf.harness_thread_id = handle.harness_thread_id.clone();
                    tf.updated_at = Utc::now();
                })
                .await?;
        }

        let warning = handle.warning.as_ref().map(|warning| {
            warning_info(
                warning.code.clone(),
                warning.message.clone(),
                warning.detail.clone(),
                thread_id,
                "open_thread",
            )
        });

        return Ok(Json(OpenThreadResponse {
            thread_id,
            harness_thread_id: handle.harness_thread_id,
            warning,
        }));
    }

    let handle = state
        .registry
        .open_thread(
            &project_config,
            ws_root,
            None,
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
        context_window: crate::models::context_window_for(
            &app_config,
            &project_config.default_model,
        ),
        approval_policy: ApprovalPolicy::Ask,
        model_efforts: std::collections::HashMap::new(),
        tokens: giskard_core::token::TokenLedger::default(),
        created_at: now,
        updated_at: now,
        archived: false,
    };
    state.store.save_thread(project_id, &thread_file).await?;

    let warning = handle.warning.as_ref().map(|warning| {
        warning_info(
            warning.code.clone(),
            warning.message.clone(),
            warning.detail.clone(),
            handle.thread,
            "open_thread",
        )
    });

    Ok(Json(OpenThreadResponse {
        thread_id: handle.thread,
        harness_thread_id: handle.harness_thread_id,
        warning,
    }))
}

fn thread_summary(tf: &ThreadFile) -> ThreadSummary {
    ThreadSummary {
        id: tf.id,
        title: tf.title.clone(),
        mode: tf.mode,
        archived: tf.archived,
        created_at: tf.created_at,
        updated_at: tf.updated_at,
    }
}

fn normalize_thread_title(raw: &str) -> Result<String, ApiError> {
    let title = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    if title.is_empty() {
        return Err(ApiError::BadRequest("thread title cannot be empty".into()));
    }
    if title.chars().count() > MAX_THREAD_TITLE_CHARS {
        return Err(ApiError::BadRequest(format!(
            "thread title must be {MAX_THREAD_TITLE_CHARS} characters or fewer"
        )));
    }
    Ok(title)
}

async fn archive_thread(
    State(state): State<AppState>,
    AxumPath((project_id, thread_id)): AxumPath<(ProjectId, ThreadId)>,
    Json(req): Json<ArchiveThreadRequest>,
) -> Result<Json<ThreadSummary>, ApiError> {
    reject_thread_mutation_if_live(&state, thread_id).await?;
    let project_config = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let thread_file = state
        .store
        .load_thread(project_id, thread_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    state
        .registry
        .set_thread_archived(
            &project_config,
            thread_id,
            thread_file.harness_thread_id,
            req.archived,
        )
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    let tf = state
        .store
        .update_thread(project_id, thread_id, |tf| {
            tf.archived = req.archived;
            tf.updated_at = Utc::now();
        })
        .await?
        .ok_or(ApiError::NotFound)?;

    Ok(Json(thread_summary(&tf)))
}

async fn rename_thread(
    State(state): State<AppState>,
    AxumPath((project_id, thread_id)): AxumPath<(ProjectId, ThreadId)>,
    Json(req): Json<RenameThreadRequest>,
) -> Result<Json<ThreadSummary>, ApiError> {
    let title = normalize_thread_title(&req.title)?;
    let project_config = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let thread_file = state
        .store
        .load_thread(project_id, thread_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    if thread_file.title == title {
        return Ok(Json(thread_summary(&thread_file)));
    }

    state
        .registry
        .set_thread_name(
            &project_config,
            thread_id,
            thread_file.harness_thread_id,
            title.clone(),
        )
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    let tf = state
        .store
        .update_thread(project_id, thread_id, |tf| {
            tf.title = title;
            tf.updated_at = Utc::now();
        })
        .await?
        .ok_or(ApiError::NotFound)?;
    broadcast_thread_state(&state, thread_id, &tf).await;

    Ok(Json(thread_summary(&tf)))
}

async fn delete_thread(
    State(state): State<AppState>,
    AxumPath((project_id, thread_id)): AxumPath<(ProjectId, ThreadId)>,
) -> Result<axum::http::StatusCode, ApiError> {
    reject_thread_mutation_if_live(&state, thread_id).await?;
    let project_config = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let thread_file = state
        .store
        .load_thread(project_id, thread_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    state
        .registry
        .delete_thread(&project_config, thread_id, thread_file.harness_thread_id)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    state.store.delete_thread(project_id, thread_id).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
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

    if !within_browse_roots(&canonical, &config.browse.roots) {
        return Err(ApiError::Forbidden("path outside allowed roots".into()));
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

/// True when `path` is inside one of the configured browse roots, or when no roots are configured
/// (empty ⇒ the whole filesystem is browsable, spec Appendix C / `BrowseConfig`).
fn within_browse_roots(path: &Path, roots: &[String]) -> bool {
    if roots.is_empty() {
        return true;
    }
    roots.iter().any(|root| {
        let root = PathBuf::from(root);
        let root_canonical = root.canonicalize().unwrap_or(root);
        path.starts_with(&root_canonical)
    })
}

/// Create a single directory under `parent` for the filesystem picker's "New folder" action. The
/// name must be one path segment (no separators, not `.`/`..`), and `parent` must resolve inside
/// the configured browse roots — the same confinement the `browse` listing enforces.
async fn browse_mkdir(
    State(state): State<AppState>,
    Json(req): Json<MkdirRequest>,
) -> Result<Json<MkdirResponse>, ApiError> {
    let name = req.name.trim();
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        return Err(ApiError::BadRequest("invalid directory name".into()));
    }

    let config = state.store.load_config().await?;
    let parent = PathBuf::from(&req.parent)
        .canonicalize()
        .map_err(|e| ApiError::BadRequest(format!("cannot canonicalize parent: {e}")))?;
    if !within_browse_roots(&parent, &config.browse.roots) {
        return Err(ApiError::Forbidden("path outside allowed roots".into()));
    }

    let target = parent.join(name);
    tokio::fs::create_dir(&target)
        .await
        .map_err(|e| ApiError::BadRequest(format!("cannot create directory: {e}")))?;
    let canonical = target.canonicalize().unwrap_or(target);
    Ok(Json(MkdirResponse {
        path: canonical.to_string_lossy().to_string(),
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
            line: s.line,
        })
        .collect();

    Ok(Json(LinkifyResponse { links }))
}

/// `POST /api/projects/{id}/render` — render agent Markdown to sanitized HTML (spec §11.2).
///
/// Agents emit GitHub-flavored Markdown; this returns safe HTML the client injects directly.
/// Detected workspace paths are wrapped in `.path-link` buttons (the same affordance `/linkify`
/// feeds), so rendering and linkification are a single pass. See [`crate::markdown`] for the
/// sanitization guarantees.
async fn render_markdown(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<ProjectId>,
    Json(req): Json<RenderRequest>,
) -> Result<Json<RenderResponse>, ApiError> {
    let project = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;

    let workspace_root = PathBuf::from(project.workspace_root.as_deref().unwrap_or(&project.dir));
    let root_canonical = workspace_root.canonicalize().unwrap_or(workspace_root);

    let html = crate::markdown::render_markdown(&req.text, &root_canonical);
    Ok(Json(RenderResponse { html }))
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
        warnings: Vec::new(),
    }))
}

/// `POST /api/models/refresh` — merge each listing-enabled provider's `/v1/models` over the static
/// list (spec §8.3). Best-effort: always returns at least the static list, plus any per-provider
/// discovery failures (e.g. a 401) as `warnings` so the UI can surface them.
async fn refresh_models(
    State(state): State<AppState>,
) -> Result<Json<ListModelsResponse>, ApiError> {
    let config = state.store.load_config().await?;
    let (models, warnings) = crate::models::refresh_models(&config).await;
    Ok(Json(ListModelsResponse { models, warnings }))
}

async fn list_mcp_servers(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<ProjectId>,
) -> Result<Json<ListMcpServersResponse>, ApiError> {
    let project_config = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let capabilities = state
        .registry
        .capabilities(&project_config)
        .await
        .map_err(harness_api_error)?;
    let servers = if capabilities.mcp_status {
        state
            .registry
            .list_mcp_servers(&project_config)
            .await
            .map_err(harness_api_error)?
    } else {
        Vec::new()
    };
    Ok(Json(ListMcpServersResponse {
        servers,
        capabilities: McpCapabilitiesResponse {
            status: capabilities.mcp_status,
            reload: capabilities.mcp_reload,
            oauth_login: capabilities.mcp_oauth_login,
        },
    }))
}

async fn reload_mcp_servers(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<ProjectId>,
) -> Result<Json<ReloadMcpServersResponse>, ApiError> {
    let project_config = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let capabilities = state
        .registry
        .capabilities(&project_config)
        .await
        .map_err(harness_api_error)?;
    if !capabilities.mcp_reload {
        return Err(ApiError::BadRequest(
            "MCP server reload is not supported by this harness".into(),
        ));
    }
    state
        .registry
        .reload_mcp_servers(&project_config)
        .await
        .map_err(harness_api_error)?;
    Ok(Json(ReloadMcpServersResponse { ok: true }))
}

async fn start_mcp_oauth_login(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<ProjectId>,
    Json(req): Json<StartMcpOauthLoginRequest>,
) -> Result<Json<McpOauthStart>, ApiError> {
    let name = req.name.trim();
    if name.is_empty() {
        return Err(ApiError::BadRequest(
            "MCP server name cannot be empty".into(),
        ));
    }
    let project_config = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let capabilities = state
        .registry
        .capabilities(&project_config)
        .await
        .map_err(harness_api_error)?;
    if !capabilities.mcp_oauth_login {
        return Err(ApiError::BadRequest(
            "MCP OAuth login is not supported by this harness".into(),
        ));
    }
    let login = state
        .registry
        .start_mcp_oauth_login(&project_config, name)
        .await
        .map_err(harness_api_error)?;
    Ok(Json(login))
}

fn harness_api_error(error: HarnessError) -> ApiError {
    match error {
        HarnessError::Unsupported(message) => ApiError::BadRequest(message),
        HarnessError::ThreadBusy { .. } => {
            ApiError::Conflict("Thread already has an active turn.".into())
        }
        other => ApiError::Internal(other.to_string()),
    }
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

async fn ws_ticket(State(state): State<AppState>) -> Result<Json<WsTicketResponse>, ApiError> {
    let expiry = (Utc::now().timestamp() as u64) + 60;
    let ticket = sign_session(expiry, &state.session_key)
        .map_err(|e| ApiError::Internal(format!("failed to sign websocket ticket: {e}")))?;
    Ok(Json(WsTicketResponse { ticket }))
}

#[derive(Deserialize)]
struct WsQuery {
    ticket: Option<String>,
}

async fn ws_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Query(q): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    let cookie_header = headers
        .get(axum::http::header::COOKIE)
        .and_then(|v| v.to_str().ok());

    let valid = cookie_header
        .and_then(get_session_token_from_header)
        .as_ref()
        .map(|t| verify_session(t, &state.session_key))
        .unwrap_or(false)
        || q.ticket
            .as_deref()
            .map(|t| verify_session(t, &state.session_key))
            .unwrap_or(false);

    if !valid {
        return Err(axum::http::StatusCode::UNAUTHORIZED);
    }

    Ok(ws.on_upgrade(move |socket| handle_ws(socket, state)))
}

#[derive(Debug, Clone)]
struct WsError {
    info: ErrorInfo,
}

impl WsError {
    fn new(code: impl Into<String>, severity: ErrorSeverity, message: impl Into<String>) -> Self {
        Self {
            info: ErrorInfo {
                code: code.into(),
                severity,
                message: message.into(),
                detail: None,
                thread_id: None,
                action: None,
                process_id: None,
            },
        }
    }

    fn detail(mut self, detail: impl Into<String>) -> Self {
        self.info.detail = Some(detail.into());
        self
    }

    fn thread(mut self, thread_id: ThreadId) -> Self {
        self.info.thread_id = Some(thread_id);
        self
    }

    fn action(mut self, action: impl Into<String>) -> Self {
        self.info.action = Some(action.into());
        self
    }

    fn process_id(mut self, process_id: impl Into<String>) -> Self {
        self.info.process_id = Some(process_id.into());
        self
    }

    fn from_harness(error: HarnessError, action: &str, thread_id: Option<ThreadId>) -> Self {
        let (code, message) = match &error {
            HarnessError::Spawn(_) => ("harness_spawn_failed", "Codex CLI could not start."),
            HarnessError::NotInitialized => (
                "harness_not_initialized",
                "Codex is not ready for this request.",
            ),
            HarnessError::Unauthenticated => {
                ("harness_unauthenticated", "Codex is not authenticated.")
            }
            HarnessError::Transport(_) => ("harness_transport_error", "Codex transport failed."),
            HarnessError::Protocol(_) => ("harness_protocol_error", "Codex protocol error."),
            HarnessError::Overloaded => ("harness_overloaded", "Codex is overloaded."),
            HarnessError::Unsupported(_) => (
                "harness_unsupported",
                "The active harness does not support this action.",
            ),
            HarnessError::ThreadNotFound(_) => {
                ("thread_not_open", "Thread is not open in the harness.")
            }
            HarnessError::ThreadBusy { .. } => {
                ("thread_turn_active", "Thread already has an active turn.")
            }
            HarnessError::Timeout(_) => ("harness_timeout", "Codex operation timed out."),
        };
        let mut ws_error = Self::new(code, ErrorSeverity::Error, message)
            .detail(error.to_string())
            .action(action);
        if let Some(thread_id) = thread_id {
            ws_error = ws_error.thread(thread_id);
        }
        ws_error
    }

    fn from_persist(error: PersistError, action: &str, thread_id: Option<ThreadId>) -> Self {
        let mut ws_error = Self::new(
            "persistence_error",
            ErrorSeverity::Error,
            "Persistence failed.",
        )
        .detail(error.to_string())
        .action(action);
        if let Some(thread_id) = thread_id {
            ws_error = ws_error.thread(thread_id);
        }
        ws_error
    }

    fn into_server_message(self) -> ServerMessage {
        ServerMessage::Error { error: self.info }
    }
}

impl fmt::Display for WsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.info.code, self.info.message)?;
        if let Some(detail) = &self.info.detail {
            write!(f, " ({detail})")?;
        }
        Ok(())
    }
}

fn warning_info(
    code: impl Into<String>,
    message: impl Into<String>,
    detail: Option<String>,
    thread_id: ThreadId,
    action: &str,
) -> ErrorInfo {
    ErrorInfo {
        code: code.into(),
        severity: ErrorSeverity::Warning,
        message: message.into(),
        detail,
        thread_id: Some(thread_id),
        action: Some(action.to_string()),
        process_id: None,
    }
}

fn harness_error_means_no_active_command(error: &HarnessError) -> bool {
    matches!(error, HarnessError::Transport(message) if message
        .to_ascii_lowercase()
        .contains("no active command/exec for process id"))
}

async fn handle_ws(socket: WebSocket, state: AppState) {
    let (tx, mut rx) = mpsc::channel::<ServerMessage>(256);
    let client_id = state.hub.next_client_id();

    let (mut ws_sender, mut ws_receiver) = socket.split();

    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let json = match serde_json::to_string(&msg) {
                Ok(json) => json,
                Err(e) => {
                    error!("failed to serialize WS message: {e}");
                    continue;
                }
            };
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
                        let _ = tx
                            .send(
                                WsError::new(
                                    "invalid_ws_message",
                                    ErrorSeverity::Error,
                                    "Browser sent an invalid WebSocket message.",
                                )
                                .detail(e.to_string())
                                .action("parse_ws_message")
                                .into_server_message(),
                            )
                            .await;
                        continue;
                    }
                };
                if let Err(e) = handle_client_msg(&state, client_id, &tx, msg).await {
                    error!(
                        code = %e.info.code,
                        severity = ?e.info.severity,
                        thread_id = ?e.info.thread_id,
                        action = ?e.info.action,
                        detail = ?e.info.detail,
                        "WS handler error: {}",
                        e.info.message
                    );
                    let _ = tx.send(e.into_server_message()).await;
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
) -> Result<(), WsError> {
    match msg {
        ClientMessage::Subscribe { thread_id } => {
            let access = ensure_thread_open(state, thread_id, "subscribe").await?;
            state.hub.subscribe(thread_id, client_id, tx.clone()).await;

            if let Some(warning) = access.warning {
                let _ = tx.send(ServerMessage::Error { error: warning }).await;
            }

            let tf = state
                .store
                .recompute_aggregates(access.project_id, thread_id)
                .await
                .map_err(|e| WsError::from_persist(e, "subscribe", Some(thread_id)))?
                .ok_or_else(|| {
                    WsError::new(
                        "thread_not_found",
                        ErrorSeverity::Error,
                        "Thread not found.",
                    )
                    .thread(thread_id)
                    .action("subscribe")
                })?;
            let thread_state = serde_json::to_value(&tf).map_err(|e| {
                WsError::new(
                    "thread_state_serialize_failed",
                    ErrorSeverity::Error,
                    "Thread state could not be serialized.",
                )
                .detail(e.to_string())
                .thread(thread_id)
                .action("subscribe")
            })?;
            let _ = tx
                .send(ServerMessage::ThreadState(giskard_proto::ThreadState {
                    thread_id,
                    state: thread_state,
                }))
                .await;

            // H4/H6: send the most recent page of history (not the whole thread). Older pages are
            // fetched on demand via `LoadHistory`.
            let limit = state
                .store
                .load_config()
                .await
                .map(|c| c.history.initial)
                .unwrap_or(50);
            if let Ok((turns, has_more)) = state
                .store
                .load_history(access.project_id, thread_id, None, limit)
                .await
            {
                let _ = tx
                    .send(ServerMessage::HistoryPage {
                        thread_id,
                        turns: turns.into_iter().map(Into::into).collect(),
                        has_more,
                    })
                    .await;
            }

            // H5: the in-flight turn is not in the JSONL yet — reconstruct it from the live buffer.
            if let Some(snap) = state.live_buffers.snapshot(thread_id).await {
                let _ = tx.send(ServerMessage::LiveTurnSnapshot(snap)).await;
            }

            let tasks = state.running_commands.snapshot(thread_id).await;
            let _ = tx
                .send(ServerMessage::RunningTasks { thread_id, tasks })
                .await;
        }
        ClientMessage::LoadHistory {
            thread_id,
            before,
            limit,
        } => {
            let project_id = project_for(state, thread_id, "load_history").await?;
            let default_limit = state
                .store
                .load_config()
                .await
                .map(|c| c.history.page)
                .unwrap_or(50);
            let limit = limit.unwrap_or(default_limit);
            let (turns, has_more) = state
                .store
                .load_history(project_id, thread_id, before, limit)
                .await
                .map_err(|e| WsError::from_persist(e, "load_history", Some(thread_id)))?;
            let _ = tx
                .send(ServerMessage::HistoryPage {
                    thread_id,
                    turns: turns.into_iter().map(Into::into).collect(),
                    has_more,
                })
                .await;
        }
        ClientMessage::Unsubscribe { thread_id } => {
            state.hub.unsubscribe(thread_id, client_id).await;
        }
        ClientMessage::SendInput { thread_id, text } => {
            let project_id = project_for(state, thread_id, "send_input").await?;
            let app_config = state
                .store
                .load_config()
                .await
                .map_err(|e| WsError::from_persist(e, "send_input", Some(thread_id)))?;
            // RMW under the per-thread lock: bump activity and read back the resolved state.
            let tf = state
                .store
                .update_thread(project_id, thread_id, |tf| {
                    let normalized =
                        crate::models::normalize_model_ref(&app_config, &tf.current_model);
                    if normalized != tf.current_model {
                        tf.current_model = normalized;
                        tf.context_window =
                            crate::models::context_window_for(&app_config, &tf.current_model);
                    }
                    tf.updated_at = chrono::Utc::now();
                })
                .await
                .map_err(|e| WsError::from_persist(e, "send_input", Some(thread_id)))?
                .ok_or_else(|| {
                    WsError::new(
                        "thread_not_found",
                        ErrorSeverity::Error,
                        "Thread not found.",
                    )
                    .thread(thread_id)
                    .action("send_input")
                })?;

            let effective_model = tf.current_model.clone();

            // Resolved snapshot the harness applies to `turn/start` (§7.5, §8.4/§8.5):
            //  - the thread's current model (carrying its reasoning effort), so a mid-thread
            //    model/effort change actually reaches the agent. Passing `None` here would leave
            //    Codex on whatever model was set at `thread/start`.
            //  - the thread's persisted approval policy (§9).
            let overrides = TurnOverrides {
                model: Some(effective_model.clone()),
                mode: tf.mode,
                approval_policy: tf.approval_policy,
            };

            state
                .registry
                .start_turn(thread_id, UserInput::text(text), overrides, effective_model)
                .await
                .map_err(|e| WsError::from_harness(e, "send_input", Some(thread_id)))?;
        }
        ClientMessage::SwitchMode { thread_id, mode } => {
            let project_id = project_for(state, thread_id, "switch_mode").await?;
            let tf = state
                .store
                .update_thread(project_id, thread_id, |tf| {
                    tf.mode = mode;
                    tf.updated_at = chrono::Utc::now();
                })
                .await
                .map_err(|e| WsError::from_persist(e, "switch_mode", Some(thread_id)))?
                .ok_or_else(|| {
                    WsError::new(
                        "thread_not_found",
                        ErrorSeverity::Error,
                        "Thread not found.",
                    )
                    .thread(thread_id)
                    .action("switch_mode")
                })?;
            broadcast_thread_state(state, thread_id, &tf).await;
        }
        ClientMessage::SelectModel {
            thread_id,
            model_ref,
        } => {
            let project_id = project_for(state, thread_id, "select_model").await?;
            let config = state
                .store
                .load_config()
                .await
                .map_err(|e| WsError::from_persist(e, "select_model", Some(thread_id)))?;
            let model_ref = crate::models::normalize_model_ref(&config, &model_ref);

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
                    let same_model = tf.current_model.provider == new_model.provider
                        && tf.current_model.model == new_model.model;
                    if new_descriptor.supports_reasoning_effort {
                        if same_model && new_model.reasoning_effort.is_none() {
                            tf.model_efforts.remove(&new_model.key());
                        } else if new_model.reasoning_effort.is_none() {
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
                .map_err(|e| WsError::from_persist(e, "select_model", Some(thread_id)))?
                .ok_or_else(|| {
                    WsError::new(
                        "thread_not_found",
                        ErrorSeverity::Error,
                        "Thread not found.",
                    )
                    .thread(thread_id)
                    .action("select_model")
                })?;
            broadcast_thread_state(state, thread_id, &tf).await;
        }
        ClientMessage::SetApprovalPolicy { thread_id, policy } => {
            let project_id = project_for(state, thread_id, "set_approval_policy").await?;
            let tf = state
                .store
                .update_thread(project_id, thread_id, |tf| {
                    tf.approval_policy = policy;
                    tf.updated_at = chrono::Utc::now();
                })
                .await
                .map_err(|e| WsError::from_persist(e, "set_approval_policy", Some(thread_id)))?
                .ok_or_else(|| {
                    WsError::new(
                        "thread_not_found",
                        ErrorSeverity::Error,
                        "Thread not found.",
                    )
                    .thread(thread_id)
                    .action("set_approval_policy")
                })?;
            broadcast_thread_state(state, thread_id, &tf).await;
        }
        ClientMessage::ApprovalDecision {
            request_id,
            decision,
        } => {
            state
                .registry
                .respond_approval(giskard_core::ids::ApprovalId(request_id), decision)
                .await
                .map_err(|e| WsError::from_harness(e, "approval_decision", None))?;
        }
        ClientMessage::ServerRequestResponse {
            request_id,
            response,
        } => {
            let req_id = giskard_core::ids::ServerRequestId(request_id);
            tokio::time::timeout(
                HARNESS_CONTROL_TIMEOUT,
                state.registry.respond_server_request(req_id, response),
            )
            .await
            .map_err(|_| {
                WsError::from_harness(
                    HarnessError::Timeout(
                        "server request response timed out waiting for Codex".into(),
                    ),
                    "server_request_response",
                    None,
                )
            })?
            .map_err(|e| WsError::from_harness(e, "server_request_response", None))?;
        }
        ClientMessage::Interrupt { thread_id } => {
            tokio::time::timeout(HARNESS_CONTROL_TIMEOUT, state.registry.interrupt(thread_id))
                .await
                .map_err(|_| {
                    WsError::from_harness(
                        HarnessError::Timeout(
                            "interrupt request timed out waiting for Codex".into(),
                        ),
                        "interrupt",
                        Some(thread_id),
                    )
                })?
                .map_err(|e| WsError::from_harness(e, "interrupt", Some(thread_id)))?;
        }
        ClientMessage::CompactContext { thread_id } => {
            let project_id = project_for(state, thread_id, "compact_context").await?;
            let tf = state
                .store
                .load_thread(project_id, thread_id)
                .await
                .map_err(|e| WsError::from_persist(e, "compact_context", Some(thread_id)))?
                .ok_or_else(|| {
                    WsError::new(
                        "thread_not_found",
                        ErrorSeverity::Error,
                        "Thread not found.",
                    )
                    .thread(thread_id)
                    .action("compact_context")
                })?;
            tokio::time::timeout(
                HARNESS_CONTROL_TIMEOUT,
                state
                    .registry
                    .compact_thread(thread_id, tf.current_model.clone(), tf.mode),
            )
            .await
            .map_err(|_| {
                WsError::from_harness(
                    HarnessError::Timeout(
                        "context compaction request timed out waiting for Codex".into(),
                    ),
                    "compact_context",
                    Some(thread_id),
                )
            })?
            .map_err(|e| WsError::from_harness(e, "compact_context", Some(thread_id)))?;
        }
        ClientMessage::TerminateCommand {
            thread_id,
            process_id,
        } => {
            let process_id_for_state = process_id.clone();
            let existing_command = state
                .running_commands
                .get_by_process(thread_id, &process_id_for_state)
                .await;
            if state
                .running_commands
                .set_terminating_by_process(thread_id, &process_id_for_state, true)
                .await
            {
                broadcast_running_commands(state, thread_id).await;
            }
            let terminate_result = tokio::time::timeout(
                HARNESS_CONTROL_TIMEOUT,
                state.registry.terminate_command(thread_id, process_id),
            )
            .await
            .map_err(|_| {
                HarnessError::Timeout(
                    "terminate command request timed out waiting for Codex".into(),
                )
            });
            if let Err(error) = terminate_result.and_then(|result| result) {
                if harness_error_means_no_active_command(&error)
                    && existing_command
                        .as_ref()
                        .map(|cmd| cmd.after_turn)
                        .unwrap_or(false)
                {
                    if state
                        .running_commands
                        .remove_by_process(thread_id, &process_id_for_state)
                        .await
                    {
                        broadcast_running_commands(state, thread_id).await;
                    }
                    return Ok(());
                }

                if state
                    .running_commands
                    .set_terminating_by_process(thread_id, &process_id_for_state, false)
                    .await
                {
                    broadcast_running_commands(state, thread_id).await;
                }
                return Err(
                    WsError::from_harness(error, "terminate_command", Some(thread_id))
                        .process_id(process_id_for_state),
                );
            }
        }
        ClientMessage::SavePlan { thread_id, path } => {
            match save_plan(state, thread_id, &path).await {
                Ok(written) => {
                    debug!(%thread_id, path = %written, "plan saved");
                }
                Err(e) => {
                    let _ = tx
                        .send(
                            WsError::new(
                                "save_plan_failed",
                                ErrorSeverity::Error,
                                "Save plan failed.",
                            )
                            .detail(e)
                            .thread(thread_id)
                            .action("save_plan")
                            .into_server_message(),
                        )
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

struct ThreadAccess {
    project_id: ProjectId,
    warning: Option<ErrorInfo>,
}

async fn ensure_thread_open(
    state: &AppState,
    thread_id: ThreadId,
    action: &str,
) -> Result<ThreadAccess, WsError> {
    if let Some(project_id) = state.registry.get_project_for_thread(thread_id).await {
        return Ok(ThreadAccess {
            project_id,
            warning: None,
        });
    }

    let Some((project_config, thread_file)) =
        find_persisted_thread(state, thread_id, action).await?
    else {
        return Err(WsError::new(
            "thread_not_found",
            ErrorSeverity::Error,
            "Thread not found.",
        )
        .thread(thread_id)
        .action(action));
    };

    let ws_root = project_config
        .workspace_root
        .as_deref()
        .unwrap_or(&project_config.dir);
    let app_config = state
        .store
        .load_config()
        .await
        .map_err(|e| WsError::from_persist(e, action, Some(thread_id)))?;
    let current_model = crate::models::normalize_model_ref(&app_config, &thread_file.current_model);
    if current_model != thread_file.current_model {
        state
            .store
            .update_thread(project_config.id, thread_id, |tf| {
                tf.current_model = current_model.clone();
                tf.context_window = crate::models::context_window_for(&app_config, &current_model);
                tf.updated_at = Utc::now();
            })
            .await
            .map_err(|e| WsError::from_persist(e, action, Some(thread_id)))?;
    }
    debug!(
        project_id = %project_config.id,
        %thread_id,
        harness_thread_id = %thread_file.harness_thread_id,
        %action,
        "reopening persisted thread"
    );
    let handle = state
        .registry
        .open_thread(
            &project_config,
            ws_root,
            Some(thread_id),
            Some(thread_file.harness_thread_id.clone()),
            current_model,
        )
        .await
        .map_err(|e| WsError::from_harness(e, action, Some(thread_id)))?;

    if handle.thread != thread_id {
        return Err(WsError::new(
            "thread_resume_mismatch",
            ErrorSeverity::Error,
            "Harness resumed the wrong thread.",
        )
        .detail(format!("expected {thread_id}, got {}", handle.thread))
        .thread(thread_id)
        .action(action));
    }

    if handle.harness_thread_id != thread_file.harness_thread_id {
        let harness_thread_id = handle.harness_thread_id.clone();
        state
            .store
            .update_thread(project_config.id, thread_id, |tf| {
                tf.harness_thread_id = harness_thread_id;
                tf.updated_at = Utc::now();
            })
            .await
            .map_err(|e| WsError::from_persist(e, action, Some(thread_id)))?;
    }

    let warning = handle.warning.map(|warning| {
        warning_info(
            warning.code,
            warning.message,
            warning.detail,
            thread_id,
            action,
        )
    });

    if let Some(warning) = &warning {
        warn!(
            project_id = %project_config.id,
            %thread_id,
            code = %warning.code,
            %action,
            "thread reopened with warning: {}",
            warning.message
        );
        state
            .hub
            .broadcast(
                thread_id,
                ServerMessage::Error {
                    error: warning.clone(),
                },
            )
            .await;
    }

    Ok(ThreadAccess {
        project_id: project_config.id,
        warning,
    })
}

async fn find_persisted_thread(
    state: &AppState,
    thread_id: ThreadId,
    action: &str,
) -> Result<Option<(ProjectConfig, ThreadFile)>, WsError> {
    let index = state
        .store
        .load_project_index()
        .await
        .map_err(|e| WsError::from_persist(e, action, Some(thread_id)))?;

    for project in index.projects {
        match state.store.load_thread(project.id, thread_id).await {
            Ok(Some(thread_file)) => {
                let project_config = state
                    .store
                    .load_project(project.id)
                    .await
                    .map_err(|e| WsError::from_persist(e, action, Some(thread_id)))?
                    .ok_or_else(|| {
                        WsError::new(
                            "project_not_found",
                            ErrorSeverity::Error,
                            "Project not found for persisted thread.",
                        )
                        .thread(thread_id)
                        .action(action)
                    })?;
                return Ok(Some((project_config, thread_file)));
            }
            Ok(None) => {}
            Err(e) => return Err(WsError::from_persist(e, action, Some(thread_id))),
        }
    }

    Ok(None)
}

/// Resolve the project a thread belongs to, reopening it from persistence on first access.
async fn project_for(
    state: &AppState,
    thread_id: ThreadId,
    action: &str,
) -> Result<ProjectId, WsError> {
    ensure_thread_open(state, thread_id, action)
        .await
        .map(|access| access.project_id)
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
    let value = match serde_json::to_value(tf) {
        Ok(value) => value,
        Err(e) => {
            error!(%thread_id, "failed to serialize thread state: {e}");
            return;
        }
    };
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

async fn broadcast_running_commands(state: &AppState, thread_id: ThreadId) {
    let tasks = state.running_commands.snapshot(thread_id).await;
    state
        .hub
        .broadcast(thread_id, ServerMessage::RunningTasks { thread_id, tasks })
        .await;
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

    // Plan extraction reads the authoritative JSONL history (H1), not the metadata file.
    let turns = state
        .store
        .load_all_turns(project_id, thread_id)
        .await
        .map_err(|e| e.to_string())?;
    let markdown = crate::plan::extract_plan_markdown(&tf.title, &turns)
        .ok_or("no plan-mode content to save")?;

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
    Conflict(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, msg) = match self {
            ApiError::NotFound => (axum::http::StatusCode::NOT_FOUND, "not found".into()),
            ApiError::BadRequest(msg) => (axum::http::StatusCode::BAD_REQUEST, msg),
            ApiError::Forbidden(msg) => (axum::http::StatusCode::FORBIDDEN, msg),
            ApiError::Conflict(msg) => (axum::http::StatusCode::CONFLICT, msg),
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
