use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::LazyLock;
use std::time::Duration;

use axum::{
    Router,
    extract::{
        DefaultBodyLimit, Path as AxumPath, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    middleware,
    response::{IntoResponse, Json},
    routing::{delete, get, patch, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use chrono::Utc;
use serde::Deserialize;
use tracing::{debug, error, info, warn};

use futures::{SinkExt, StreamExt};
use giskard_core::error::{HarnessError, PersistError};
use giskard_core::ids::{ProjectId, ThreadId};
use giskard_core::model::{ModelDescriptor, ModelRef};
use giskard_core::text::trimmed_non_empty;
use giskard_core::thread::ThreadKind;
use giskard_core::turn::{Mode, PermissionPreset, TurnOverrides};
use giskard_core::user_input::UserInput;
use giskard_git_parser::{
    GitNumstatEntry, apply_numstat_counts, index_numstat, parse_git_numstat, parse_git_status,
};
use giskard_harness::HarnessProvider;
use giskard_persist::Config;
use giskard_persist::store::{
    ProjectConfig, ThreadFile, ThreadGitWorkspace, ThreadRecency, ThreadWorktree,
};
use giskard_proto::*;
use tokio::process::Command as TokioCommand;
use tokio::sync::{mpsc, oneshot};

use crate::AppState;
use crate::auth::{
    SESSION_COOKIE, TokenPurpose, auth_middleware, create_session_cookie,
    get_session_token_from_header, sign_token, verify_token,
};
use crate::thread_graph::{
    descendant_deletion_order, graph_issue, inherited_git_workspace, load_thread_graph,
    should_refresh_subagent_title,
};

const HARNESS_CONTROL_TIMEOUT: Duration = Duration::from_secs(2);
const PROJECT_LIFECYCLE_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_THREAD_TITLE_CHARS: usize = 120;
const GENERATED_THREAD_TITLE_CHARS: usize = 72;
const GENERATED_THREAD_TITLE_WORDS: usize = 8;
const MAX_ATTACHMENTS_PER_MESSAGE: usize = 8;
const MAX_ATTACHMENT_BYTES: usize = 25 * 1024 * 1024;
const MAX_TOTAL_ATTACHMENT_BYTES: usize = 25 * 1024 * 1024;
const MAX_ATTACHMENT_HTTP_BODY_BYTES: usize = 40 * 1024 * 1024;
const MAX_WS_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
const MAX_ATTACHMENT_NAME_BYTES: usize = 255;
const MAX_ATTACHMENT_MIME_BYTES: usize = 127;
const GIT_COMMAND_TIMEOUT: Duration = Duration::from_secs(3);

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
            "/api/projects/{id}/threads/start",
            post(start_thread_with_message)
                .layer(DefaultBodyLimit::max(MAX_ATTACHMENT_HTTP_BODY_BYTES)),
        )
        .route(
            "/api/projects/{id}/threads/{parent_thread_id}/subagent-links/{item_id}/open",
            post(open_subagent_link),
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
        .route(
            "/api/projects/{id}/threads/{thread_id}/deletion-impact",
            get(thread_deletion_impact),
        )
        // File reads name their thread in the path rather than leaving it implicit. The workspace a
        // read is answered from is the thread's, so the thread has to be part of the request; a
        // caller that could omit it would be answered from somewhere it never asked about.
        .route(
            "/api/projects/{id}/threads/{thread_id}/highlight",
            get(highlight_file),
        )
        .route(
            "/api/projects/{id}/threads/{thread_id}/raw",
            get(download_file),
        )
        .route(
            "/api/projects/{id}/threads/{thread_id}/image",
            get(image_file),
        )
        .route("/api/projects/{id}/git/status", get(git_status))
        .route("/api/projects/{id}/git/diff", get(git_diff))
        .route(
            "/api/projects/{id}/threads/{thread_id}/linkify",
            post(linkify),
        )
        .route(
            "/api/projects/{id}/threads/{thread_id}/render",
            post(render_markdown),
        )
        .route("/api/browse", get(browse))
        .route("/api/browse/mkdir", post(browse_mkdir))
        .route("/api/projects/{id}/models", get(project_list_models))
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
        .route("/favicon.svg", get(favicon_svg))
        // Assets are served under content-hashed paths (see build.rs) so they can be cached
        // immutably: the URL changes exactly when the bytes do, so an upgraded binary can never be
        // shadowed by a stale browser cache. `index.html` (served no-cache) points at the current
        // hashes.
        .route(APP_JS_PATH, get(app_js))
        .route(APP_CSS_PATH, get(app_css))
        // The service worker must live at a stable root path so it controls the whole origin and so
        // the browser's update check can re-fetch the same URL — hence not content-hashed.
        .route("/sw.js", get(service_worker))
        .route("/api/login", post(login))
}

/// Build/version identity, stamped in by `build.rs`: a human-readable git short hash (with a
/// `-dirty` suffix for uncommitted builds) shown in the Settings panel, and content hashes of the
/// two assets used as cache-busting path segments.
const GIT_HASH: &str = env!("GISKARD_GIT_HASH");
const APP_JS_PATH: &str = concat!("/app.", env!("GISKARD_JS_HASH"), ".js");
const APP_CSS_PATH: &str = concat!("/app.", env!("GISKARD_CSS_HASH"), ".css");

/// Immutable caching is safe because the asset URL carries a content hash.
const ASSET_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";

/// `index.html` with its placeholders resolved once at startup: the hashed asset URLs and the build
/// version (exposed to the browser as a `<meta>` tag, which the strict CSP allows where an inline
/// script would be blocked).
static INDEX_HTML: LazyLock<String> = LazyLock::new(|| {
    include_str!("../static/index.html")
        .replace("__APP_CSS__", APP_CSS_PATH)
        .replace("__APP_JS__", APP_JS_PATH)
        .replace("__GISKARD_VERSION__", GIT_HASH)
});

/// Serve the single-page desktop UI (§13). Self-contained HTML/CSS/JS (no npm); it authenticates
/// via `/api/login` and drives the app through the same REST + WS API as any client. The script
/// and stylesheet ship as separate same-origin assets (below) so the Content-Security-Policy can
/// enforce a strict `script-src 'self'` with no inline execution. Served `no-cache` so the browser
/// always revalidates the HTML and thus always picks up the current hashed asset URLs.
async fn index() -> impl IntoResponse {
    (
        [(axum::http::header::CACHE_CONTROL, "no-cache")],
        axum::response::Html(INDEX_HTML.as_str()),
    )
}

async fn app_js() -> impl IntoResponse {
    (
        [
            (
                axum::http::header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            ),
            (axum::http::header::CACHE_CONTROL, ASSET_CACHE_CONTROL),
        ],
        include_str!("../static/app.js"),
    )
}

async fn app_css() -> impl IntoResponse {
    (
        [
            (axum::http::header::CONTENT_TYPE, "text/css; charset=utf-8"),
            (axum::http::header::CACHE_CONTROL, ASSET_CACHE_CONTROL),
        ],
        include_str!("../static/app.css"),
    )
}

/// The notification service worker. Served no-cache from the root so it controls the whole origin
/// and the browser revalidates it on each update check (the service-worker lifecycle then rolls it
/// over). It carries no version-specific content, so unlike the app assets it isn't content-hashed.
async fn service_worker() -> impl IntoResponse {
    (
        [
            (
                axum::http::header::CONTENT_TYPE,
                "text/javascript; charset=utf-8",
            ),
            (axum::http::header::CACHE_CONTROL, "no-cache"),
        ],
        include_str!("../static/sw.js"),
    )
}

async fn favicon_svg() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "image/svg+xml")],
        include_str!("../static/favicon.svg"),
    )
}

/// Best-effort client identity for login audit logs. The socket peer is not threaded through the
/// router, so this reports `X-Forwarded-For` — meaningful exactly when Giskard sits behind a
/// trusted reverse proxy that sets it (the recommended deployment), and attacker-controlled
/// otherwise, so treat it as diagnostic only.
fn login_client(headers: &axum::http::HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string())
        .unwrap_or_else(|| "unknown".into())
}

async fn login(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<LoginRequest>,
) -> Result<axum::response::Response, (axum::http::StatusCode, String)> {
    // Brute-force lockout runs before the (memory-hard) Argon2 verification so a flood of wrong
    // passwords can neither guess the password nor burn server CPU/RAM.
    if let Err(remaining) = state.login_throttle.check() {
        let retry_after = remaining.as_secs().max(1);
        warn!(
            client = %login_client(&headers),
            retry_after_secs = retry_after,
            "login rejected: throttled after repeated failures"
        );
        let mut response = (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            format!("too many failed login attempts; retry in {retry_after}s"),
        )
            .into_response();
        if let Ok(value) = axum::http::HeaderValue::from_str(&retry_after.to_string()) {
            response
                .headers_mut()
                .insert(axum::http::header::RETRY_AFTER, value);
        }
        return Ok(response);
    }

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
        let (failures, lockout) = state.login_throttle.record_failure();
        // Stable, greppable line for external log watchers (e.g. fail2ban on the proxy host).
        warn!(
            client = %login_client(&headers),
            consecutive_failures = failures,
            lockout_secs = lockout.map(|d| d.as_secs()).unwrap_or(0),
            "login failed: invalid password"
        );
        return Ok(Json(LoginResponse { ok: false }).into_response());
    }

    state.login_throttle.record_success();
    info!(client = %login_client(&headers), "login succeeded");

    let lifetime_secs = (config.auth.session_days as u64) * 86400;
    let expiry = (Utc::now().timestamp() as u64) + lifetime_secs;
    let cookie = create_session_cookie(
        expiry,
        &state.session_key,
        config.server.secure_cookies,
        lifetime_secs,
    )
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
    // `browse.roots` must confine more than the folder picker: a project's directory becomes the
    // agent's workspace and the boundary for the raw/highlight file endpoints, so an arbitrary
    // `dir` in this API request would bypass the picker's confinement entirely.
    ensure_dir_within_browse_roots(&req.dir, &config.browse.roots)?;
    if let Some(ws_root) = &req.workspace_root {
        ensure_dir_within_browse_roots(ws_root, &config.browse.roots)?;
    }
    state.store.create_project(id, &req.name, &req.dir).await?;

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
    let _lifecycle_guard = state
        .registry
        .lock_project_lifecycle_with_timeout(id, PROJECT_LIFECYCLE_LOCK_TIMEOUT)
        .await
        .map_err(harness_api_error)?;
    state
        .store
        .load_project(id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let thread_ids = state.store.list_threads(id).await?;
    for thread_id in thread_ids {
        if state.live_buffers.is_active(thread_id).await
            || state.registry.thread_has_active_turn(thread_id).await
        {
            return Err(ApiError::Conflict(
                "project has a thread with an active turn; stop it before removing the project"
                    .into(),
            ));
        }
        if state
            .running_commands
            .has_running_for_thread(thread_id)
            .await
        {
            return Err(ApiError::Conflict(
                "project has a thread with running commands; stop them before removing the project"
                    .into(),
            ));
        }
    }
    state
        .registry
        .delete_project(id)
        .await
        .map_err(harness_api_error)?;
    state.model_catalogs.remove(id).await;
    // Forced: the user confirmed a project-scope deletion, and a thread whose worktree still holds
    // work must not be able to strand the whole project half-deleted.
    //
    // Placed between the two deletions on purpose. After the registry, because that is what shuts
    // the agents down, and a checkout must not be pulled out from under a session still running in
    // it — and because a registry failure returns early, which would otherwise leave every thread
    // file naming a checkout that is already gone. Before the thread files, because they are the
    // only record of where each checkout is.
    for thread_id in state.store.list_threads(id).await? {
        let Some(worktree) = state
            .store
            .load_thread(id, thread_id)
            .await?
            .and_then(|tf| tf.git_workspace)
            .and_then(|ws| ws.as_worktree().cloned())
        else {
            continue;
        };
        // Deliberately ignored, unlike the per-thread route: this confirmation is project-scoped and
        // already forced, so one thread whose checkout will not come down must not strand the whole
        // project half-deleted. The helper logs what it could not remove.
        let _ = remove_worktree_for_deleted_thread(
            &worktree,
            thread_id,
            "delete_project",
            /*force*/ true,
        )
        .await;
    }
    state.store.delete_project(id).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

async fn reject_thread_mutation_if_live(
    state: &AppState,
    thread_id: ThreadId,
) -> Result<(), ApiError> {
    if state.registry.thread_has_active_turn(thread_id).await
        || state.live_buffers.is_active(thread_id).await
    {
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
    let mut loaded = Vec::new();
    for tid in thread_ids {
        match state.store.load_thread(project_id, tid).await {
            Ok(Some(tf)) => loaded.push(tf),
            Ok(None) => {}
            Err(e) => {
                warn!(
                    %project_id,
                    thread_id = %tid,
                    error = %e,
                    "failed to load thread while listing project; omitting thread"
                );
            }
        }
    }
    let graph = loaded
        .iter()
        .cloned()
        .map(|thread| (thread.id, thread))
        .collect();
    for thread in &loaded {
        if let Some(issue) = graph_issue(&graph, thread) {
            error!(
                %project_id,
                thread_id = %thread.id,
                parent_thread_id = ?thread.parent_thread_id,
                kind = ?thread.kind,
                issue,
                action = "list_threads",
                "invalid persisted thread graph"
            );
        }
    }
    let project = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let mut threads = Vec::with_capacity(loaded.len());
    for thread in &loaded {
        let workspace_root = thread_workspace_root(&state, &project, thread).await?;
        threads.push(thread_summary(thread, workspace_root));
    }
    threads.sort_by_key(|t| std::cmp::Reverse(t.updated_at));
    Ok(Json(ListThreadsResponse { threads }))
}

async fn find_thread_by_harness_id(
    state: &AppState,
    project_id: ProjectId,
    harness_thread_id: &str,
) -> Result<Option<ThreadFile>, ApiError> {
    for tid in state.store.list_threads(project_id).await? {
        let Some(tf) = state.store.load_thread(project_id, tid).await? else {
            continue;
        };
        if tf.harness_thread_id == harness_thread_id {
            return Ok(Some(tf));
        }
    }
    Ok(None)
}

async fn open_thread(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<ProjectId>,
    Json(req): Json<OpenThreadRequest>,
) -> Result<Json<OpenThreadResponse>, ApiError> {
    let app_config = state.store.load_config().await?;
    let project_config = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let project_ws_root = project_config
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
        if state
            .store
            .load_thread(project_id, thread_id)
            .await?
            .is_none()
        {
            return Err(ApiError::NotFound);
        }
        let catalog = project_model_catalog(&state, &project_config, &app_config).await;
        let thread_file =
            normalize_persisted_thread_model(&state, project_id, thread_id, &app_config, &catalog)
                .await?
                .ok_or(ApiError::NotFound)?;
        let current_model = thread_file.current_model.clone();
        if let Some(handle) = state.registry.get_thread_handle(thread_id).await {
            return Ok(Json(OpenThreadResponse {
                thread_id: handle.thread,
                harness_thread_id: handle.harness_thread_id,
                warning: None,
            }));
        }

        // A thread with a worktree is opened against that worktree, not the project's checkout — and
        // a sub-agent against its parent's, which is where the harness ran it. Resolved after the
        // early return above: an already-attached thread is the common case for this endpoint, and
        // the lookup walks the ownership chain reading persisted parents to answer.
        let ws_root = thread_workspace_root(&state, &project_config, &thread_file).await?;

        // Opening an existing thread must not hard-fail when the harness can't attach — most
        // often because the thread's provider was removed from config. The browser opens a
        // thread through this endpoint *before* subscribing over the WebSocket, so a 500 here
        // would make the whole thread unviewable. Degrade to a read-only open instead: the
        // client proceeds to subscribe and the persisted history loads; only new turns fail.
        let open_result = if thread_file.kind == ThreadKind::Subagent {
            state
                .registry
                .open_linked_thread(
                    &project_config,
                    &ws_root,
                    Some(thread_id),
                    thread_file.harness_thread_id.clone(),
                    current_model.clone(),
                )
                .await
        } else {
            state
                .registry
                .open_thread(
                    &project_config,
                    &ws_root,
                    Some(thread_id),
                    Some(thread_file.harness_thread_id.clone()),
                    Some(current_model.clone()),
                )
                .await
        };
        let handle = match open_result {
            Ok(handle) => handle,
            Err(error) => {
                warn!(
                    %project_id,
                    %thread_id,
                    harness_thread_id = %thread_file.harness_thread_id,
                    %error,
                    action = "open_thread",
                    "harness attach failed; opening thread read-only"
                );
                let context = ReadOnlyProviderContext {
                    provider: current_model.provider.clone(),
                    configured: provider_is_known(
                        &state,
                        &project_config,
                        Some(&app_config),
                        &current_model.provider,
                    )
                    .await,
                };
                return Ok(Json(OpenThreadResponse {
                    thread_id,
                    harness_thread_id: thread_file.harness_thread_id,
                    warning: Some(read_only_info(
                        Some(&context),
                        Some(error.to_string()),
                        thread_id,
                        "open_thread",
                    )),
                }));
            }
        };

        if handle.thread != thread_id {
            return Err(ApiError::Internal(format!(
                "harness resumed wrong thread: expected {thread_id}, got {}",
                handle.thread
            )));
        }

        if handle.harness_thread_id != thread_file.harness_thread_id {
            state
                .thread_metadata
                .mutate(project_id, thread_id, |tf| {
                    tf.harness_thread_id = handle.harness_thread_id.clone();
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

    let Some(resume) = req.resume else {
        return Err(ApiError::BadRequest(
            "creating a new thread requires an initial message".into(),
        ));
    };
    let _lifecycle_guard = state
        .registry
        .lock_project_lifecycle_with_timeout(project_id, PROJECT_LIFECYCLE_LOCK_TIMEOUT)
        .await
        .map_err(harness_api_error)?;

    if let Some(existing) = find_thread_by_harness_id(&state, project_id, &resume).await? {
        let (handle, warning) =
            if let Some(handle) = state.registry.get_thread_handle(existing.id).await {
                (handle, None)
            } else {
                let ws_root = thread_workspace_root(&state, &project_config, &existing).await?;
                let handle = if existing.kind == ThreadKind::Subagent {
                    state
                        .registry
                        .open_linked_thread(
                            &project_config,
                            &ws_root,
                            Some(existing.id),
                            existing.harness_thread_id.clone(),
                            existing.current_model.clone(),
                        )
                        .await
                } else {
                    state
                        .registry
                        .open_thread(
                            &project_config,
                            &ws_root,
                            Some(existing.id),
                            Some(existing.harness_thread_id.clone()),
                            Some(existing.current_model.clone()),
                        )
                        .await
                }
                .map_err(harness_api_error)?;
                let warning = handle.warning.as_ref().map(|warning| {
                    warning_info(
                        warning.code.clone(),
                        warning.message.clone(),
                        warning.detail.clone(),
                        handle.thread,
                        "open_thread",
                    )
                });
                (handle, warning)
            };
        refresh_route_imported_subagent_title(
            &state,
            project_id,
            existing.id,
            handle.agent_name.as_deref(),
        )
        .await?;
        return Ok(Json(OpenThreadResponse {
            thread_id: handle.thread,
            harness_thread_id: handle.harness_thread_id,
            warning,
        }));
    }

    // Importing a native thread Giskard has no record of: the model is the thread's, and only the
    // harness knows it. Naming one here would not express a preference — Codex treats
    // `model`/`modelProvider` on resume as an override and stops applying the thread's own
    // persisted model — so it would silently move an existing conversation onto another model.
    let handle = state
        .registry
        .open_thread(&project_config, project_ws_root, None, Some(resume), None)
        .await
        // Not `Internal`: the usual reason is that this thread's provider is no longer in the
        // harness's config, which is a conflict with the current state of the world and fixable by
        // the user. Answering 500 would both blame the server and log at error, alerting on a
        // config edit.
        .map_err(|e| ApiError::Conflict(e.to_string()))?;

    let current_model = handle.resumed_model.clone().ok_or_else(|| {
        // Nothing to fall back to: this thread's model was never ours to choose, and starting it
        // on a guess would bind it to a provider it was not using (LT7).
        ApiError::Conflict(
            "the harness did not report which model this thread is using; it cannot be imported"
                .into(),
        )
    })?;
    let catalog = project_model_catalog(&state, &project_config, &app_config).await;
    let descriptor =
        crate::models::resolve_catalog_descriptor(&catalog, &app_config, &current_model);
    let context_window = descriptor.context_window;
    let now = Utc::now();
    let title = "New thread".to_owned();
    let thread_file = ThreadFile {
        revision: 0,
        version: 1,
        id: handle.thread,
        project_id,
        title,
        harness_thread_id: handle.harness_thread_id.clone(),
        parent_thread_id: None,
        spawned_by_turn_id: None,
        kind: ThreadKind::Primary,
        mode: Mode::Build,
        current_model: current_model.clone(),
        context_window,
        model_context_windows: std::collections::HashMap::new(),
        permission_preset: PermissionPreset::AskFirst,
        model_efforts: std::collections::HashMap::new(),
        tokens: giskard_core::token::TokenLedger::default(),
        created_at: now,
        updated_at: now,
        archived: false,
        git_workspace: None,
    };
    let thread_file = state
        .thread_metadata
        .create(project_id, thread_file)
        .await?;
    state
        .thread_metadata
        .publish_created(project_id, &thread_file)
        .await;

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

async fn open_subagent_link(
    State(state): State<AppState>,
    AxumPath((project_id, parent_thread_id, item_id)): AxumPath<(ProjectId, ThreadId, ItemId)>,
) -> Result<Json<OpenSubagentLinkResponse>, ApiError> {
    // Materialization is serialized with every other lifecycle mutation for the project and may
    // be queued behind a slow native resume. Bound the complete browser action so this endpoint
    // has the same availability guarantee as HTTP handlers waiting directly on the lifecycle lock.
    let thread_id = tokio::time::timeout(
        PROJECT_LIFECYCLE_LOCK_TIMEOUT,
        state
            .registry
            .open_subagent_link(project_id, parent_thread_id, item_id),
    )
    .await
    .map_err(|_| {
        ApiError::Unavailable(format!(
            "timed out opening sub-agent link from thread {parent_thread_id}"
        ))
    })?
    .map_err(harness_api_error)?
    .ok_or_else(|| {
        ApiError::Conflict(format!(
            "item {item_id} is not an openable sub-agent link from thread {parent_thread_id}"
        ))
    })?;
    let thread = state
        .store
        .load_thread(project_id, thread_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(OpenSubagentLinkResponse {
        thread_id,
        title: thread.title,
    }))
}

async fn start_thread_with_message(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<ProjectId>,
    Json(req): Json<StartThreadRequest>,
) -> Result<Json<StartThreadResponse>, ApiError> {
    let text = req.text.trim().to_string();
    if text.is_empty() && req.attachments.is_empty() {
        return Err(ApiError::BadRequest(
            "creating a new thread requires a message or attachment".into(),
        ));
    }
    validate_user_attachments(&req.attachments)?;

    let app_config = state.store.load_config().await?;
    let project_config = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let catalog = project_model_catalog(&state, &project_config, &app_config).await;
    let (model_ref, model_descriptor) =
        resolve_initial_thread_model(&app_config, &catalog, req.model_ref);
    let project_ws_root = project_config
        .workspace_root
        .as_deref()
        .unwrap_or(&project_config.dir);
    let thread_id = ThreadId::new();
    info!(
        %project_id,
        %thread_id,
        provider = %model_ref.provider,
        model = %model_ref.model,
        mode = ?req.mode,
        permission_preset = ?req.permission_preset,
        git_strategy = ?req.git_strategy,
        "starting new thread from initial user message"
    );

    // The worktree has to exist before the harness opens the thread, because it *is* the cwd the
    // harness is opened against. A failure here fails the whole request rather than falling back to
    // the project's checkout: a thread that silently runs unisolated still looks isolated in the UI,
    // which is the one outcome worse than an error the user can act on.
    let worktree = match req.git_strategy {
        GitStrategy::Shared => None,
        GitStrategy::Worktree => {
            let path = crate::worktree::worktree_path(
                state.store.data_dir(),
                &project_id.to_string(),
                thread_id,
            );
            let branch = crate::worktree::branch_name(thread_id);
            match crate::worktree::create(Path::new(project_ws_root), &path, &branch).await {
                Ok(worktree) => {
                    info!(%project_id, %thread_id, branch, path = %path.display(), "created thread worktree");
                    Some(worktree)
                }
                Err(error) => {
                    warn!(%project_id, %thread_id, branch, %error, "could not create thread worktree");
                    return Err(match error {
                        crate::worktree::WorktreeError::Unavailable(message) => {
                            ApiError::Unavailable(message)
                        }
                        other => ApiError::BadRequest(other.to_string()),
                    });
                }
            }
        }
    };
    let ws_root = worktree
        .as_ref()
        .map(|w| w.workspace_root())
        .unwrap_or(project_ws_root);

    let handle = match state
        .registry
        .open_thread(
            &project_config,
            ws_root,
            Some(thread_id),
            None,
            Some(model_ref.clone()),
        )
        .await
    {
        Ok(handle) => handle,
        Err(error) => {
            remove_worktree_after_start_failure(worktree.as_ref(), thread_id, "open_thread").await;
            return Err(harness_api_error(error));
        }
    };
    if handle.thread != thread_id {
        cleanup_new_thread_after_start_failure(
            &state,
            &project_config,
            handle.thread,
            handle.harness_thread_id.clone(),
            false,
            "open_thread_mismatch",
        )
        .await;
        remove_worktree_after_start_failure(worktree.as_ref(), thread_id, "open_thread_mismatch")
            .await;
        let detail = format!(
            "harness opened wrong thread: expected {thread_id}, got {}",
            handle.thread
        );
        warn!(%project_id, %thread_id, detail, "new thread open returned mismatched thread id");
        return Err(ApiError::Internal(detail));
    }

    let now = Utc::now();
    let title = if text.is_empty() {
        thread_title_from_attachments(&req.attachments)
    } else {
        thread_title_from_first_prompt(&text)
    };
    let thread_file = ThreadFile {
        revision: 0,
        version: 1,
        id: thread_id,
        project_id,
        title: title.clone(),
        harness_thread_id: handle.harness_thread_id.clone(),
        parent_thread_id: None,
        spawned_by_turn_id: None,
        kind: ThreadKind::Primary,
        mode: req.mode,
        current_model: model_ref.clone(),
        context_window: model_descriptor.context_window,
        model_context_windows: std::collections::HashMap::new(),
        permission_preset: req.permission_preset,
        model_efforts: std::collections::HashMap::new(),
        tokens: giskard_core::token::TokenLedger::default(),
        created_at: now,
        updated_at: now,
        archived: false,
        git_workspace: worktree.clone().map(ThreadGitWorkspace::Worktree),
    };

    let thread_file = match state.thread_metadata.create(project_id, thread_file).await {
        Ok(thread_file) => thread_file,
        Err(error) => {
            cleanup_new_thread_after_start_failure(
                &state,
                &project_config,
                thread_id,
                handle.harness_thread_id.clone(),
                false,
                "save_thread",
            )
            .await;
            remove_worktree_after_start_failure(worktree.as_ref(), thread_id, "save_thread").await;
            return Err(ApiError::Internal(error.to_string()));
        }
    };

    let overrides = TurnOverrides {
        model: Some(model_ref.clone()),
        mode: req.mode,
        permission_preset: req.permission_preset,
    };
    let turn_id = match state
        .registry
        .start_turn(
            thread_id,
            UserInput::text_with_attachments(text, req.attachments),
            overrides,
            model_ref.clone(),
        )
        .await
    {
        Ok(turn_id) => turn_id,
        Err(error) => {
            cleanup_new_thread_after_start_failure(
                &state,
                &project_config,
                thread_id,
                handle.harness_thread_id.clone(),
                true,
                "start_turn",
            )
            .await;
            remove_worktree_after_start_failure(worktree.as_ref(), thread_id, "start_turn").await;
            return Err(harness_api_error(error));
        }
    };

    let warning = handle.warning.as_ref().map(|warning| {
        warning_info(
            warning.code.clone(),
            warning.message.clone(),
            warning.detail.clone(),
            thread_id,
            "start_thread",
        )
    });

    state
        .thread_metadata
        .publish_created(project_id, &thread_file)
        .await;

    Ok(Json(StartThreadResponse {
        thread_id,
        title,
        harness_thread_id: handle.harness_thread_id,
        turn_id,
        warning,
    }))
}

fn resolve_initial_thread_model(
    config: &giskard_persist::Config,
    catalog: &[ModelDescriptor],
    model: ModelRef,
) -> (ModelRef, ModelDescriptor) {
    let mut model = crate::models::normalize_model_ref(config, catalog, &model);
    let descriptor = crate::models::resolve_catalog_descriptor(catalog, config, &model);
    if !descriptor.supports_reasoning_effort {
        model.reasoning_effort = None;
    }
    (model, descriptor)
}

fn thread_title_from_first_prompt(prompt: &str) -> String {
    let mut candidate = meaningful_prompt_lines(prompt)
        .into_iter()
        .next()
        .unwrap_or_else(|| collapse_whitespace(prompt));
    candidate = collapse_whitespace(&candidate);
    candidate = replace_ticket_urls(&candidate);
    candidate = strip_broad_prompt_boilerplate(&candidate).to_string();
    candidate = trim_at_title_boundary(&candidate);
    candidate = limit_title_words(&candidate, GENERATED_THREAD_TITLE_WORDS);
    candidate = candidate
        .trim_end_matches(|ch: char| ch.is_ascii_whitespace() || ".,;:!?-/".contains(ch))
        .to_string();
    candidate = truncate_title(&candidate, GENERATED_THREAD_TITLE_CHARS);
    if candidate.is_empty() {
        "New thread".into()
    } else {
        candidate
    }
}

fn thread_title_from_attachments(attachments: &[UserAttachment]) -> String {
    let Some(first) = attachments.first() else {
        return "New thread".into();
    };
    let name = collapse_whitespace(&first.name);
    let title = if attachments.len() == 1 {
        format!("Attached {name}")
    } else {
        format!("Attached {name} and {} more", attachments.len() - 1)
    };
    truncate_title(&title, GENERATED_THREAD_TITLE_CHARS)
}

fn validate_user_attachments(attachments: &[UserAttachment]) -> Result<(), ApiError> {
    if attachments.len() > MAX_ATTACHMENTS_PER_MESSAGE {
        return Err(ApiError::BadRequest(format!(
            "at most {MAX_ATTACHMENTS_PER_MESSAGE} attachments are allowed"
        )));
    }
    let mut total_bytes = 0_usize;
    for attachment in attachments {
        total_bytes = checked_attachment_total(total_bytes, validate_user_attachment(attachment)?)?;
    }
    Ok(())
}

fn checked_attachment_total(current: usize, added: usize) -> Result<usize, ApiError> {
    let total = current
        .checked_add(added)
        .ok_or_else(|| ApiError::BadRequest("attachment sizes overflowed".into()))?;
    if total > MAX_TOTAL_ATTACHMENT_BYTES {
        return Err(ApiError::BadRequest(format!(
            "attachments exceed the {MAX_TOTAL_ATTACHMENT_BYTES} byte total limit"
        )));
    }
    Ok(total)
}

fn validate_user_attachment(attachment: &UserAttachment) -> Result<usize, ApiError> {
    let name = attachment.name.trim();
    if name.is_empty() {
        return Err(ApiError::BadRequest("attachment name is required".into()));
    }
    if name.len() > MAX_ATTACHMENT_NAME_BYTES {
        return Err(ApiError::BadRequest(format!(
            "attachment name exceeds the {MAX_ATTACHMENT_NAME_BYTES} byte limit"
        )));
    }
    if name.chars().any(char::is_control) {
        return Err(ApiError::BadRequest(
            "attachment name contains control characters".into(),
        ));
    }
    let mime_type = attachment.mime_type.trim().to_ascii_lowercase();
    if mime_type.is_empty() {
        return Err(ApiError::BadRequest(
            "attachment MIME type is required".into(),
        ));
    }
    if mime_type.len() > MAX_ATTACHMENT_MIME_BYTES || !is_valid_mime_type(&mime_type) {
        return Err(ApiError::BadRequest(format!(
            "attachment {} has an invalid MIME type",
            attachment.name
        )));
    }
    if attachment.data_base64.is_empty() {
        return Err(ApiError::BadRequest("attachment data is required".into()));
    }
    let decoded = BASE64_STANDARD
        .decode(&attachment.data_base64)
        .map_err(|err| {
            ApiError::BadRequest(format!(
                "attachment {} is not valid base64: {err}",
                attachment.name
            ))
        })?;
    if decoded.is_empty() {
        return Err(ApiError::BadRequest(format!(
            "attachment {} is empty",
            attachment.name
        )));
    }
    if decoded.len() > MAX_ATTACHMENT_BYTES {
        return Err(ApiError::BadRequest(format!(
            "attachment {} exceeds the {} byte limit",
            attachment.name, MAX_ATTACHMENT_BYTES
        )));
    }
    if attachment.size != decoded.len() as u64 {
        return Err(ApiError::BadRequest(format!(
            "attachment {} size does not match its decoded data",
            attachment.name
        )));
    }
    if attachment.kind == AttachmentKind::Image {
        let detected_mime = detect_supported_image_mime(&decoded).ok_or_else(|| {
            ApiError::BadRequest(format!(
                "attachment {} does not contain a supported image",
                attachment.name
            ))
        })?;
        if mime_type != detected_mime {
            return Err(ApiError::BadRequest(format!(
                "attachment {} MIME type does not match its image data",
                attachment.name
            )));
        }
    }
    Ok(decoded.len())
}

fn is_valid_mime_type(mime_type: &str) -> bool {
    fn is_token(value: &str) -> bool {
        !value.is_empty()
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(
                        byte,
                        b'!' | b'#' | b'$' | b'&' | b'^' | b'_' | b'.' | b'+' | b'-'
                    )
            })
    }

    let mut parts = mime_type.split('/');
    matches!(
        (parts.next(), parts.next(), parts.next()),
        (Some(type_name), Some(subtype), None) if is_token(type_name) && is_token(subtype)
    )
}

fn detect_supported_image_mime(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(&[0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a]) {
        return Some("image/png");
    }
    if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        return Some("image/jpeg");
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return Some("image/gif");
    }
    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        return Some("image/webp");
    }
    None
}

fn ticket_key(text: &str) -> Option<String> {
    text.split(|ch: char| ch.is_ascii_whitespace() || matches!(ch, '/' | '?' | '#' | '&'))
        .find(|part| {
            let Some((prefix, number)) = part.split_once('-') else {
                return false;
            };
            prefix.len() >= 2
                && prefix.chars().all(|ch| ch.is_ascii_uppercase())
                && number.chars().all(|ch| ch.is_ascii_digit())
        })
        .map(|ticket| ticket.trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '-'))
        .filter(|ticket| !ticket.is_empty())
        .map(str::to_string)
}

fn replace_ticket_urls(text: &str) -> String {
    text.split_whitespace()
        .map(|word| {
            if word.starts_with("http://") || word.starts_with("https://") {
                ticket_key(word).unwrap_or_else(|| word.to_string())
            } else {
                word.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn strip_broad_prompt_boilerplate(text: &str) -> &str {
    let lower = text.to_ascii_lowercase();
    for prefix in [
        "i want to ",
        "i need to ",
        "we need to ",
        "can you ",
        "could you ",
        "please ",
    ] {
        if lower.starts_with(prefix) {
            return text[prefix.len()..].trim_start();
        }
    }
    text
}

fn trim_at_title_boundary(text: &str) -> String {
    let mut end = text.len();
    for delimiter in [": ", "; ", "? ", "! ", ", "] {
        if let Some(idx) = text.find(delimiter) {
            end = end.min(idx);
        }
    }
    if let Some(idx) = first_sentence_period_boundary(text) {
        end = end.min(idx);
    }
    text[..end].trim().to_string()
}

fn first_sentence_period_boundary(text: &str) -> Option<usize> {
    let mut previous = None;
    for (idx, ch) in text.char_indices() {
        if ch == '.' && !previous.is_some_and(|previous: char| previous.is_ascii_digit()) {
            let after_period = &text[idx + ch.len_utf8()..];
            if after_period.starts_with(char::is_whitespace) {
                return Some(idx);
            }
        }
        previous = Some(ch);
    }
    None
}

fn limit_title_words(text: &str, max_words: usize) -> String {
    let words = text.split_whitespace().take(max_words).collect::<Vec<_>>();
    let mut title = words.join(" ");
    while let Some(last) = title.split_whitespace().last() {
        let trimmed = last.trim_matches(|ch: char| !ch.is_ascii_alphanumeric());
        if !matches!(
            trimmed.to_ascii_lowercase().as_str(),
            "a" | "an" | "the" | "and" | "or" | "of" | "for" | "to" | "in" | "on" | "with"
        ) {
            break;
        }
        let new_len = title.len().saturating_sub(last.len());
        title.truncate(new_len);
        title = title.trim_end().to_string();
    }
    title
}

fn meaningful_prompt_lines(text: &str) -> Vec<String> {
    let mut lines = Vec::new();
    let mut in_fence = false;
    for raw in text.replace("\r\n", "\n").replace('\r', "\n").lines() {
        let stripped = raw.trim();
        if stripped.starts_with("```") || stripped.starts_with("~~~") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence {
            continue;
        }

        let line = strip_markdown_prefix(stripped);
        if line.is_empty() || is_generic_heading(&line) {
            continue;
        }
        lines.push(line);
    }
    lines
}

fn strip_markdown_prefix(line: &str) -> String {
    let mut line = line.trim();

    let hashes = line.chars().take_while(|ch| *ch == '#').count();
    if (1..=6).contains(&hashes) {
        let rest = &line[hashes..];
        if rest.starts_with(char::is_whitespace) {
            line = rest.trim_start();
        }
    }

    if let Some(rest) = line.strip_prefix('>') {
        line = rest.trim_start();
    }

    if let Some(first) = line.chars().next()
        && matches!(first, '-' | '*' | '+')
    {
        let rest = &line[first.len_utf8()..];
        if rest.starts_with(char::is_whitespace) {
            line = rest.trim_start();
            if let Some(rest) = strip_checkbox_prefix(line) {
                line = rest;
            }
        }
    }

    line = strip_numbered_prefix(line).unwrap_or(line);
    line.trim().to_string()
}

fn strip_checkbox_prefix(line: &str) -> Option<&str> {
    let rest = line.strip_prefix('[')?;
    let mut chars = rest.chars();
    let mark = chars.next()?;
    if !matches!(mark, ' ' | 'x' | 'X') {
        return None;
    }
    let after_mark = chars.as_str();
    let after_bracket = after_mark.strip_prefix(']')?;
    if !after_bracket.starts_with(char::is_whitespace) {
        return None;
    }
    Some(after_bracket.trim_start())
}

fn strip_numbered_prefix(line: &str) -> Option<&str> {
    let mut digits_end = 0;
    for (idx, ch) in line.char_indices() {
        if ch.is_ascii_digit() {
            digits_end = idx + ch.len_utf8();
            continue;
        }
        break;
    }
    if digits_end == 0 {
        return None;
    }
    let rest = &line[digits_end..];
    let delimiter = rest.chars().next()?;
    if !matches!(delimiter, '.' | ')') {
        return None;
    }
    let after_delimiter = &rest[delimiter.len_utf8()..];
    if !after_delimiter.starts_with(char::is_whitespace) {
        return None;
    }
    Some(after_delimiter.trim_start())
}

fn is_generic_heading(line: &str) -> bool {
    matches!(
        line.trim_end_matches(':')
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "ask" | "prompt" | "question" | "request" | "task" | "todo"
    )
}

fn collapse_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_title(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }

    let mut end = text.len();
    for (count, (idx, _)) in text.char_indices().enumerate() {
        if count == max_chars {
            end = idx;
            break;
        }
    }

    let clipped = text[..end].trim_end();
    let mut boundary = None;
    let mut boundary_chars = 0;
    for (count, (idx, ch)) in clipped.char_indices().enumerate() {
        if matches!(ch, ' ' | '-' | '/') {
            boundary = Some(idx);
            boundary_chars = count;
        }
    }
    let clipped = if boundary_chars >= max_chars / 2 {
        boundary
            .map(|idx| clipped[..idx].trim_end())
            .unwrap_or(clipped)
    } else {
        clipped
    };
    clipped
        .trim_end_matches(|ch: char| ch.is_ascii_whitespace() || " ,;:-".contains(ch))
        .to_string()
}

/// What removing a thread's worktree would destroy, in the two ways it can.
///
/// Committed work and working-tree work are lost differently: the branch holds commits, so deleting
/// it is what loses those; the worktree holds edits, so removing it is what loses those. A thread
/// can be at risk from either, both, or neither.
struct WorktreeDeletionImpact {
    branch: String,
    uncommitted_changes: usize,
    /// Commits on the thread's branch reachable from no other ref — what deleting it destroys.
    unreachable_commits: usize,
}

impl WorktreeDeletionImpact {
    fn destroys_work(&self) -> bool {
        self.uncommitted_changes > 0 || self.unreachable_commits > 0
    }

    /// A sentence the user can decide from, naming both kinds of loss when both apply.
    fn describe(&self) -> String {
        let mut parts = Vec::new();
        if self.uncommitted_changes > 0 {
            parts.push(format!(
                "{} uncommitted change{}",
                self.uncommitted_changes,
                if self.uncommitted_changes == 1 {
                    ""
                } else {
                    "s"
                }
            ));
        }
        if self.unreachable_commits > 0 {
            parts.push(format!(
                "{} commit{} on no other ref",
                self.unreachable_commits,
                if self.unreachable_commits == 1 {
                    ""
                } else {
                    "s"
                }
            ));
        }
        format!("{} in worktree {}", parts.join(" and "), self.branch)
    }
}

/// Fails rather than reporting an empty impact when Git cannot answer. "Nothing would be lost" is
/// the verdict that lets a deletion run unconfirmed, so it must never be what an error looks like.
async fn worktree_deletion_impact(
    worktree: &ThreadWorktree,
) -> Result<WorktreeDeletionImpact, crate::worktree::WorktreeError> {
    Ok(WorktreeDeletionImpact {
        branch: worktree.branch.clone(),
        uncommitted_changes: crate::worktree::uncommitted_changes(worktree).await?,
        unreachable_commits: crate::worktree::commits_reachable_from_nowhere_else(worktree).await?,
    })
}

/// Remove a deleted thread's worktree and the branch it started on.
///
/// Ordering is forced: Git refuses to delete a branch that is checked out in a worktree. Branches
/// the agent created during the thread are deliberately untouched — they live in the shared
/// repository and are the user's now.
///
/// Returns the failure rather than only logging it. The thread file is the only record of where this
/// checkout and branch are, so a caller that deleted it first and then discovered the cleanup had
/// failed would have stranded both with nothing left to find them by.
async fn remove_worktree_for_deleted_thread(
    worktree: &ThreadWorktree,
    thread_id: ThreadId,
    action: &str,
    force: bool,
) -> Result<(), crate::worktree::WorktreeError> {
    // `force` is the user's answer, not a constant. The impact probe ran earlier in the request, so
    // anything written to the checkout since — by an editor, a hook, a shell the user still has open
    // — is invisible to it; Git looks again here, and on an unforced deletion its refusal is the
    // last chance to ask. Attempted even when the directory is already gone, forced or not: removal
    // de-registers a vanished checkout (measured: exit 0), and until it is de-registered Git refuses
    // to delete the branch it holds.
    if let Err(error) = crate::worktree::remove(worktree, force).await {
        warn!(
            %thread_id,
            branch = %worktree.branch,
            path = %worktree.path,
            %action,
            %error,
            "could not remove the thread's worktree"
        );
        return Err(error);
    }
    // A worktree whose directory is gone stays *registered* with Git, and a registered worktree
    // holds its branch: without pruning, a checkout removed outside Giskard would leave a branch
    // that can never be deleted.
    match crate::worktree::delete_branch(worktree).await {
        // Reported so the reflog window is usable: the commit is recoverable for as long as Git
        // keeps it, and nothing else records where the branch pointed.
        Ok(sha) => info!(
            %thread_id,
            branch = %worktree.branch,
            was = sha.as_deref().unwrap_or("(unknown)"),
            %action,
            "deleted the thread's worktree and branch"
        ),
        Err(error) => {
            warn!(
                %thread_id,
                branch = %worktree.branch,
                %action,
                %error,
                "could not delete the thread's branch"
            );
            return Err(error);
        }
    }
    Ok(())
}

/// A workspace query for an endpoint whose URL names a project but whose subject may be a thread.
#[derive(Deserialize)]
struct WorkspaceQuery {
    /// The thread whose workspace to read. Omitted for the project's own workspace, which is what a
    /// draft has before any thread exists.
    thread_id: Option<ThreadId>,
}

/// Resolve the workspace an endpoint should read, given an optional thread.
///
/// An unknown thread is an error rather than a fallback: answering from the project's workspace
/// would hand back a different tree under the name of the one that was asked for, which is exactly
/// the confusion isolation exists to prevent. `load_thread` is scoped to the project, so a thread id
/// belonging to another project is unknown here too — otherwise this project's endpoints would read
/// files out of that project's workspace.
async fn workspace_for_query(
    state: &AppState,
    project: &ProjectConfig,
    query: &WorkspaceQuery,
) -> Result<PathBuf, ApiError> {
    let Some(thread_id) = query.thread_id else {
        return Ok(PathBuf::from(project_workspace_root(project)));
    };
    let thread = state
        .store
        .load_thread(project.id, thread_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(PathBuf::from(
        thread_workspace_root(state, project, &thread).await?,
    ))
}

/// The workspace a thread works in: the nearest Git worktree in its ownership chain, the project's
/// workspace otherwise.
///
/// Every path served for a thread — the harness cwd, file reads, Git status — has to agree on this,
/// or the browser shows one workspace while the agent edits another. The chain rather than the
/// thread's own record, because a sub-agent works in its parent's worktree and holds no record of
/// one itself (see [`inherited_git_workspace`]).
async fn thread_workspace_root(
    state: &AppState,
    project: &ProjectConfig,
    thread: &ThreadFile,
) -> Result<String, ApiError> {
    Ok(inherited_git_workspace(&state.store, project.id, thread)
        .await?
        .map(|workspace| workspace.workspace_root().to_string())
        .unwrap_or_else(|| project_workspace_root(project).to_string()))
}

/// The project's own workspace, which is what a draft reads before any thread exists.
fn project_workspace_root(project: &ProjectConfig) -> &str {
    project.workspace_root.as_deref().unwrap_or(&project.dir)
}

/// Unwind the worktree created for a thread whose startup then failed.
///
/// The worktree is created before the harness is opened, so every later failure in that handler
/// leaves one behind: an empty checkout and a branch belonging to a thread that does not exist.
/// Forced, because there is nothing in it worth confirming — no turn ever ran.
async fn remove_worktree_after_start_failure(
    worktree: Option<&ThreadWorktree>,
    thread_id: ThreadId,
    failed_action: &str,
) {
    let Some(worktree) = worktree else {
        return;
    };
    match crate::worktree::remove(worktree, /*force*/ true).await {
        Ok(()) => debug!(
            %thread_id,
            branch = %worktree.branch,
            %failed_action,
            "removed thread worktree after failed new-thread startup"
        ),
        Err(error) => warn!(
            %thread_id,
            branch = %worktree.branch,
            path = %worktree.path,
            %failed_action,
            %error,
            "could not remove thread worktree after failed new-thread startup"
        ),
    }
    // The branch outlives `worktree remove`, and a thread that never started has nothing on it.
    if let Err(error) = crate::worktree::delete_branch(worktree).await {
        warn!(
            %thread_id,
            branch = %worktree.branch,
            %failed_action,
            %error,
            "could not delete thread branch after failed new-thread startup"
        );
    }
}

async fn cleanup_new_thread_after_start_failure(
    state: &AppState,
    project_config: &ProjectConfig,
    thread_id: ThreadId,
    harness_thread_id: String,
    remove_local_thread: bool,
    failed_action: &str,
) {
    match state
        .registry
        .delete_thread(project_config, thread_id, harness_thread_id.clone())
        .await
    {
        Ok(()) => {
            debug!(
                project_id = %project_config.id,
                %thread_id,
                %harness_thread_id,
                %failed_action,
                "cleaned up native thread after failed new-thread startup"
            );
        }
        Err(error) => {
            warn!(
                project_id = %project_config.id,
                %thread_id,
                %harness_thread_id,
                %failed_action,
                error = %error,
                "failed to delete native thread after failed new-thread startup"
            );
            state.registry.forget_thread(thread_id).await;
        }
    }

    if remove_local_thread
        && let Err(error) = state
            .store
            .delete_thread(project_config.id, thread_id)
            .await
    {
        warn!(
            project_id = %project_config.id,
            %thread_id,
            %failed_action,
            error = %error,
            "failed to delete local thread after failed new-thread startup"
        );
    }
}

fn thread_summary(tf: &ThreadFile, workspace_root: String) -> ThreadSummary {
    ThreadSummary {
        id: tf.id,
        revision: tf.revision,
        title: tf.title.clone(),
        workspace_root,
        parent_thread_id: tf.parent_thread_id,
        spawned_by_turn_id: tf.spawned_by_turn_id,
        kind: tf.kind,
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

async fn refresh_route_imported_subagent_title(
    state: &AppState,
    project_id: ProjectId,
    thread_id: ThreadId,
    agent_name: Option<&str>,
) -> Result<(), ApiError> {
    let Some(agent_name) = agent_name.and_then(trimmed_non_empty) else {
        return Ok(());
    };
    let desired = normalize_thread_title(&format!("Sub-agent: {agent_name}"))?;
    state
        .thread_metadata
        .mutate(project_id, thread_id, |thread| {
            if thread.kind == ThreadKind::Subagent
                && should_refresh_subagent_title(&thread.title, &desired)
            {
                thread.title = desired.clone();
            }
        })
        .await?;
    Ok(())
}

async fn archive_thread(
    State(state): State<AppState>,
    AxumPath((project_id, thread_id)): AxumPath<(ProjectId, ThreadId)>,
    Json(req): Json<ArchiveThreadRequest>,
) -> Result<Json<ThreadSummary>, ApiError> {
    if state.registry.thread_has_passive_monitor(thread_id).await {
        return Err(ApiError::Conflict(
            "thread has delegated sub-agent work; wait for it to finish before archiving".into(),
        ));
    }
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
        .thread_metadata
        .mutate_with_recency(project_id, thread_id, ThreadRecency::TouchIfChanged, |tf| {
            tf.archived = req.archived
        })
        .await?
        .into_current()
        .ok_or(ApiError::NotFound)?;

    let workspace_root = thread_workspace_root(&state, &project_config, &tf).await?;
    Ok(Json(thread_summary(&tf, workspace_root)))
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
        let workspace_root = thread_workspace_root(&state, &project_config, &thread_file).await?;
        return Ok(Json(thread_summary(&thread_file, workspace_root)));
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
        .thread_metadata
        .mutate_with_recency(project_id, thread_id, ThreadRecency::TouchIfChanged, |tf| {
            tf.title = title
        })
        .await?
        .into_current()
        .ok_or(ApiError::NotFound)?;

    let workspace_root = thread_workspace_root(&state, &project_config, &tf).await?;
    Ok(Json(thread_summary(&tf, workspace_root)))
}

/// `GET /api/projects/{id}/threads/{thread_id}/deletion-impact` — what deleting this thread would
/// destroy, for the confirmation card to say before the user decides rather than after they try.
///
/// Covers the whole subtree, because deleting a thread cascades to its linked children and each may
/// have a worktree of its own.
async fn thread_deletion_impact(
    State(state): State<AppState>,
    AxumPath((project_id, thread_id)): AxumPath<(ProjectId, ThreadId)>,
) -> Result<Json<ThreadDeletionImpactResponse>, ApiError> {
    let graph = load_thread_graph(&state.store, project_id).await?;
    let deletion_order = descendant_deletion_order(&graph, thread_id);
    if deletion_order.is_empty() {
        return Err(ApiError::NotFound);
    }
    let mut worktrees = Vec::new();
    for candidate in &deletion_order {
        let Some(worktree) = graph
            .get(candidate)
            .and_then(|tf| tf.git_workspace.as_ref())
            .and_then(ThreadGitWorkspace::as_worktree)
        else {
            continue;
        };
        let impact = worktree_deletion_impact(worktree).await.map_err(|error| {
            warn!(
                %project_id,
                thread_id = %candidate,
                branch = %worktree.branch,
                %error,
                action = "thread_deletion_impact",
                "could not determine what deleting this thread would destroy"
            );
            ApiError::Unavailable(format!(
                "could not check what deleting this thread would destroy: {error}"
            ))
        })?;
        worktrees.push(WorktreeImpactResponse {
            thread_id: *candidate,
            branch: impact.branch.clone(),
            uncommitted_changes: impact.uncommitted_changes,
            unreachable_commits: impact.unreachable_commits,
            summary: impact.destroys_work().then(|| impact.describe()),
        });
    }
    Ok(Json(ThreadDeletionImpactResponse { worktrees }))
}

#[derive(Deserialize)]
struct DeleteThreadQuery {
    /// Delete even when a worktree holds work that exists nowhere else. The browser sets this only
    /// after showing what would be destroyed (`GET .../deletion-impact`).
    #[serde(default)]
    force: bool,
}

async fn delete_thread(
    State(state): State<AppState>,
    AxumPath((project_id, thread_id)): AxumPath<(ProjectId, ThreadId)>,
    Query(q): Query<DeleteThreadQuery>,
) -> Result<axum::http::StatusCode, ApiError> {
    let _lifecycle_guard = state
        .registry
        .lock_project_lifecycle_with_timeout(project_id, PROJECT_LIFECYCLE_LOCK_TIMEOUT)
        .await
        .map_err(harness_api_error)?;
    let graph = load_thread_graph(&state.store, project_id).await?;
    let deletion_order = descendant_deletion_order(&graph, thread_id);
    if deletion_order.is_empty() {
        return Err(ApiError::NotFound);
    }
    // Preflight the complete subtree before deleting any native or local record. A busy descendant
    // must reject the entire request instead of leaving a partially deleted ownership tree.
    for candidate in &deletion_order {
        reject_thread_mutation_if_live(&state, *candidate).await?;
    }
    // Idle pre-turn monitors are cancellable delegated subscriptions, not durable work. Stop the
    // complete subtree before deleting anything, then preflight again so a child turn that raced
    // the first check rejects the request without leaving a partially deleted ownership graph.
    for candidate in &deletion_order {
        state
            .registry
            .stop_passive_subagent_monitor(*candidate)
            .await
            .map_err(harness_api_error)?;
    }
    for candidate in &deletion_order {
        reject_thread_mutation_if_live(&state, *candidate).await?;
    }
    let project_config = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    // Worktrees are preflighted across the whole subtree before anything is deleted, for the same
    // reason liveness is: a refusal partway through would leave a half-deleted ownership tree.
    if !q.force {
        for candidate in &deletion_order {
            let Some(worktree) = graph
                .get(candidate)
                .and_then(|tf| tf.git_workspace.as_ref())
                .and_then(ThreadGitWorkspace::as_worktree)
            else {
                continue;
            };
            let impact = worktree_deletion_impact(worktree).await.map_err(|error| {
                warn!(
                    %project_id,
                    thread_id = %candidate,
                    branch = %worktree.branch,
                    %error,
                    action = "delete_thread",
                    "refusing to delete a thread whose worktree Git cannot report on"
                );
                // A conflict rather than a server error: the deletion is refused pending the user's
                // say-so, and the browser's 409 path already offers exactly that — confirm again to
                // go ahead anyway.
                ApiError::Conflict(format!(
                    "could not check whether deleting worktree {} would destroy work: {error}",
                    worktree.branch
                ))
            })?;
            if impact.destroys_work() {
                return Err(ApiError::Conflict(format!(
                    "deleting this thread would destroy {}",
                    impact.describe()
                )));
            }
        }
    }
    let descendant_count = deletion_order.len().saturating_sub(1);
    info!(
        %project_id,
        %thread_id,
        descendant_count,
        action = "delete_thread",
        "deleting thread subtree in leaf-first order"
    );
    for (deleted_count, candidate) in deletion_order.iter().enumerate() {
        let Some(thread_file) = graph.get(candidate) else {
            error!(
                %project_id,
                root_thread_id = %thread_id,
                thread_id = %candidate,
                deleted_count,
                action = "delete_thread",
                "thread disappeared from the loaded deletion graph"
            );
            return Err(ApiError::Internal(
                "thread deletion graph changed unexpectedly".into(),
            ));
        };
        // Before the thread file goes: it is the only record of this checkout's path, branch and
        // repository, so cleaning up afterwards means cleaning up blind if anything fails. Keeping
        // the thread on failure is what makes a retry possible, and a retry converges — the probes
        // read a missing checkout as no work, removal de-registers a vanished one, and deleting an
        // already-gone branch is not an error. `force` accepts whatever is left behind instead,
        // which is the only way to delete a thread whose repository has gone missing entirely.
        //
        // The message deliberately does not promise nothing was touched: cleanup is two Git
        // commands, and the second can fail after the first has already removed the checkout.
        if let Some(worktree) = thread_file
            .git_workspace
            .as_ref()
            .and_then(ThreadGitWorkspace::as_worktree)
            && let Err(error) =
                remove_worktree_for_deleted_thread(worktree, *candidate, "delete_thread", q.force)
                    .await
            && !q.force
        {
            return Err(ApiError::Conflict(format!(
                "could not finish removing worktree {} at {}: {error}. The thread was kept so this \
                 can be retried — some of the cleanup may already have run. Deleting it anyway \
                 leaves whatever remains of the checkout and branch behind.",
                worktree.branch, worktree.path
            )));
        }
        if let Err(error) = state
            .registry
            .delete_thread(
                &project_config,
                *candidate,
                thread_file.harness_thread_id.clone(),
            )
            .await
        {
            error!(
                %project_id,
                root_thread_id = %thread_id,
                thread_id = %candidate,
                harness_thread_id = %thread_file.harness_thread_id,
                deleted_count,
                total_count = deletion_order.len(),
                action = "delete_thread",
                %error,
                "failed to delete native thread while cascading thread deletion"
            );
            return Err(ApiError::Internal(format!(
                "failed to delete thread subtree after deleting {deleted_count} thread(s): {error}"
            )));
        }
        if let Err(error) = state.thread_metadata.delete(project_id, *candidate).await {
            error!(
                %project_id,
                root_thread_id = %thread_id,
                thread_id = %candidate,
                harness_thread_id = %thread_file.harness_thread_id,
                deleted_count,
                total_count = deletion_order.len(),
                action = "delete_thread",
                %error,
                "native thread was deleted but local thread removal failed"
            );
            return Err(error.into());
        }

        info!(
            %project_id,
            root_thread_id = %thread_id,
            thread_id = %candidate,
            harness_thread_id = %thread_file.harness_thread_id,
            deleted_count = deleted_count + 1,
            total_count = deletion_order.len(),
            action = "delete_thread",
            "deleted thread from cascading subtree"
        );
    }
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

    sort_browse_entries(&mut entries);

    Ok(Json(BrowseResponse {
        path: canonical.to_string_lossy().to_string(),
        entries,
    }))
}

fn sort_browse_entries(entries: &mut [DirEntry]) {
    entries.sort_by(|a, b| {
        b.is_dir
            .cmp(&a.is_dir)
            .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
            .then_with(|| a.name.cmp(&b.name))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a real repository with one linked worktree, and hands back the record the deletion
    /// path works from.
    async fn repo_with_worktree(tmp: &std::path::Path) -> ThreadWorktree {
        let repo = tmp.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        for args in [
            vec!["init", "-q", "-b", "main"],
            vec!["config", "user.email", "t@example.invalid"],
            vec!["config", "user.name", "T"],
            vec!["commit", "-q", "--allow-empty", "-m", "initial"],
        ] {
            let out = std::process::Command::new("git")
                .current_dir(&repo)
                .args(&args)
                .output()
                .expect("run git");
            assert!(out.status.success(), "git {args:?} failed");
        }
        crate::worktree::create(&repo, &tmp.join("wt"), "giskard/worktree-01race")
            .await
            .expect("create worktree")
    }

    /// The impact probe runs earlier in the request than the removal does, so anything written to
    /// the checkout in between is invisible to it — an editor saving, a hook, a shell the user left
    /// open. Passing the user's answer through to `git worktree remove` closes that window: Git
    /// looks again, and on an unforced deletion its refusal is what turns the race into a second
    /// confirmation instead of lost work.
    #[tokio::test]
    async fn an_unforced_removal_refuses_a_checkout_that_went_dirty_after_the_probe() {
        let tmp = tempfile::TempDir::new().unwrap();
        let worktree = repo_with_worktree(tmp.path()).await;
        let thread_id = ThreadId::new();

        // Exactly the race: clean when the impact was measured, dirty by the time removal runs.
        assert_eq!(
            crate::worktree::uncommitted_changes(&worktree)
                .await
                .unwrap(),
            0
        );
        std::fs::write(
            std::path::Path::new(&worktree.path).join("late.txt"),
            "written after the probe\n",
        )
        .unwrap();

        let refused =
            remove_worktree_for_deleted_thread(&worktree, thread_id, "test", /*force*/ false).await;
        assert!(
            refused.is_err(),
            "an unforced removal must not discard work that appeared after the probe"
        );
        assert!(
            std::path::Path::new(&worktree.path)
                .join("late.txt")
                .exists(),
            "the refusal must leave the work where it is"
        );

        // The second confirmation is what actually destroys it, which is the contract the card
        // describes.
        remove_worktree_for_deleted_thread(&worktree, thread_id, "test", /*force*/ true)
            .await
            .expect("a forced removal takes the checkout down");
        assert!(!std::path::Path::new(&worktree.path).exists());
    }

    /// Cleanup is two Git commands and the second can fail after the first has run, leaving the
    /// checkout gone and the branch behind. Nothing persists that half-state, so the retry has to
    /// converge on its own: removal de-registers a vanished checkout, and the branch deletion runs
    /// against the repository, which is still there.
    #[tokio::test]
    async fn cleanup_converges_when_retried_after_a_partial_failure() {
        let tmp = tempfile::TempDir::new().unwrap();
        let worktree = repo_with_worktree(tmp.path()).await;
        let thread_id = ThreadId::new();

        // The state a failed second command leaves: checkout gone, branch still there.
        crate::worktree::remove(&worktree, /*force*/ true)
            .await
            .expect("remove the checkout");
        assert!(!std::path::Path::new(&worktree.path).exists());
        let listed = std::process::Command::new("git")
            .current_dir(&worktree.repo_root)
            .args(["branch", "--list", &worktree.branch])
            .output()
            .expect("run git");
        assert!(
            !String::from_utf8_lossy(&listed.stdout).trim().is_empty(),
            "the fixture must leave the branch behind, or it tests nothing"
        );

        remove_worktree_for_deleted_thread(&worktree, thread_id, "test", /*force*/ false)
            .await
            .expect("a retry must finish the cleanup rather than fail on what is already done");

        let listed = std::process::Command::new("git")
            .current_dir(&worktree.repo_root)
            .args(["branch", "--list", &worktree.branch])
            .output()
            .expect("run git");
        assert!(
            String::from_utf8_lossy(&listed.stdout).trim().is_empty(),
            "the retry left the branch behind"
        );
    }

    fn dir_entry(name: &str, is_dir: bool) -> DirEntry {
        DirEntry {
            name: name.to_string(),
            is_dir,
            size: 0,
            mtime: Utc::now(),
        }
    }

    #[test]
    fn browse_entries_sort_directories_first_then_names() {
        let mut entries = vec![
            dir_entry("zeta.txt", false),
            dir_entry("beta", true),
            dir_entry("Alpha.txt", false),
            dir_entry("Alpha", true),
            dir_entry("alpha", true),
            dir_entry("beta.txt", false),
        ];

        sort_browse_entries(&mut entries);

        let sorted: Vec<_> = entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry.is_dir))
            .collect();
        assert_eq!(
            sorted,
            vec![
                ("Alpha", true),
                ("alpha", true),
                ("beta", true),
                ("Alpha.txt", false),
                ("beta.txt", false),
                ("zeta.txt", false),
            ]
        );
    }

    /// A workspace that isn't a repository is an expected answer, not a failure: `error` stays
    /// empty so clients don't report git's "fatal: not a git repository" as a broken status.
    #[tokio::test]
    async fn non_repository_workspace_reports_no_error() {
        let workspace = tempfile::tempdir().expect("temp workspace");

        let status = git_status_for_workspace(workspace.path()).await;

        assert!(!status.is_repository);
        assert_eq!(status.error, None);
        assert!(!status.dirty);
        assert!(status.files.is_empty());
    }

    /// A repository git refuses to read is degraded, not absent: the reason has to reach the client
    /// so it can say something rather than silently showing nothing.
    #[test]
    fn unreadable_repository_is_reported_as_an_error() {
        let workspace = Path::new("/tmp/example");

        let absent = git_status_failure(
            workspace,
            "fatal: not a git repository (or any of the parent directories): .git",
        );
        let unreadable = git_status_failure(
            workspace,
            "fatal: detected dubious ownership in repository at '/tmp/example'",
        );

        assert_eq!(absent.error, None);
        assert_eq!(
            unreadable.error.as_deref(),
            Some("fatal: detected dubious ownership in repository at '/tmp/example'")
        );
        assert!(!absent.is_repository && !unreadable.is_repository);
    }

    #[test]
    fn parses_the_diff_side_selector() {
        assert!(matches!(GitDiffSide::parse(None), Ok(GitDiffSide::Both)));
        assert!(matches!(
            GitDiffSide::parse(Some("")),
            Ok(GitDiffSide::Both)
        ));
        assert!(matches!(
            GitDiffSide::parse(Some("staged")),
            Ok(GitDiffSide::Staged)
        ));
        assert!(matches!(
            GitDiffSide::parse(Some("unstaged")),
            Ok(GitDiffSide::Unstaged)
        ));
        // An unrecognized side is a client bug, not a silent fallback to the combined diff.
        assert!(GitDiffSide::parse(Some("cached")).is_err());

        assert!(GitDiffSide::Staged.includes_staged() && !GitDiffSide::Staged.includes_unstaged());
        assert!(
            GitDiffSide::Unstaged.includes_unstaged() && !GitDiffSide::Unstaged.includes_staged()
        );
        assert!(GitDiffSide::Both.includes_staged() && GitDiffSide::Both.includes_unstaged());
    }

    #[test]
    fn git_diff_path_rejects_workspace_escape() {
        assert_eq!(
            safe_git_relative_path("src/lib.rs").as_deref(),
            Some("src/lib.rs")
        );
        assert_eq!(
            safe_git_relative_path("./src/lib.rs").as_deref(),
            Some("src/lib.rs")
        );
        assert!(safe_git_relative_path("../secrets").is_none());
        assert!(safe_git_relative_path("/tmp/file").is_none());
        assert!(safe_git_relative_path("").is_none());
    }

    #[test]
    fn thread_title_uses_first_meaningful_prompt_line() {
        let title = thread_title_from_first_prompt(
            r#"
Task:

```rust
fn main() {}
```

- [ ] Fix the OAuth callback regression. Then add tests.
"#,
        );

        assert_eq!(title, "Fix the OAuth callback regression");
    }

    #[test]
    fn thread_title_strips_markdown_prefixes_and_sentence_punctuation() {
        assert_eq!(
            thread_title_from_first_prompt("### Investigate thread title generation! More detail."),
            "Investigate thread title generation"
        );
        assert_eq!(
            thread_title_from_first_prompt("> 1. Revisit creation flow?"),
            "Revisit creation flow"
        );
    }

    #[test]
    fn attachment_title_uses_uploaded_file_name() {
        let title = thread_title_from_attachments(&[
            UserAttachment {
                name: "design.pdf".into(),
                mime_type: "application/pdf".into(),
                size: 4,
                kind: AttachmentKind::File,
                data_base64: "ZmlsZQ==".into(),
            },
            UserAttachment {
                name: "diagram.png".into(),
                mime_type: "image/png".into(),
                size: 5,
                kind: AttachmentKind::Image,
                data_base64: "aW1hZ2U=".into(),
            },
        ]);

        assert_eq!(title, "Attached design.pdf and 1 more");
    }

    #[test]
    fn validates_supported_image_attachment() {
        validate_user_attachments(&[UserAttachment {
            name: "diagram.png".into(),
            mime_type: "image/png".into(),
            size: 68,
            kind: AttachmentKind::Image,
            data_base64: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=".into(),
        }])
        .unwrap();
    }

    #[test]
    fn rejects_attachment_with_mismatched_size() {
        let err = validate_user_attachments(&[UserAttachment {
            name: "notes.pdf".into(),
            mime_type: "application/pdf".into(),
            size: 999,
            kind: AttachmentKind::File,
            data_base64: "ZmlsZQ==".into(),
        }])
        .unwrap_err();

        assert!(err.to_string().contains("size does not match"));
    }

    #[test]
    fn rejects_image_mime_that_does_not_match_the_bytes() {
        let err = validate_user_attachments(&[UserAttachment {
            name: "diagram.bmp".into(),
            mime_type: "image/bmp".into(),
            size: 68,
            kind: AttachmentKind::Image,
            data_base64: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=".into(),
        }])
        .unwrap_err();

        assert!(err.to_string().contains("MIME type does not match"));
    }

    #[test]
    fn rejects_malformed_image_bytes() {
        let err = validate_user_attachments(&[UserAttachment {
            name: "diagram.png".into(),
            mime_type: "image/png".into(),
            size: 5,
            kind: AttachmentKind::Image,
            data_base64: "aW1hZ2U=".into(),
        }])
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("does not contain a supported image")
        );
    }

    #[test]
    fn rejects_unbounded_or_control_character_metadata() {
        let long_name = "x".repeat(MAX_ATTACHMENT_NAME_BYTES + 1);
        let err = validate_user_attachments(&[UserAttachment {
            name: long_name,
            mime_type: "application/pdf".into(),
            size: 4,
            kind: AttachmentKind::File,
            data_base64: "ZmlsZQ==".into(),
        }])
        .unwrap_err();
        assert!(err.to_string().contains("name exceeds"));

        let err = validate_user_attachments(&[UserAttachment {
            name: "notes\ninstructions.pdf".into(),
            mime_type: "application/pdf".into(),
            size: 4,
            kind: AttachmentKind::File,
            data_base64: "ZmlsZQ==".into(),
        }])
        .unwrap_err();
        assert!(err.to_string().contains("control characters"));
    }

    #[test]
    fn rejects_attachments_over_total_size_limit() {
        let err = checked_attachment_total(MAX_TOTAL_ATTACHMENT_BYTES - 1, 2).unwrap_err();
        assert!(err.to_string().contains("total limit"));
    }

    #[test]
    fn thread_title_handles_anonymized_real_prompt_shapes() {
        let cases = [
            (
                "Do we have a policy-engine integration test for IP sets and compliance mode?",
                "Do we have a policy-engine integration test",
            ),
            (
                "Let's fix https://tracker.example/browse/BUG-12345",
                "Let's fix BUG-12345",
            ),
            (
                "Let's look at changes between 2026.7.16 and 2026.7.17 for anything which could lead to request bodies not being received by origin under some circumstances",
                "Let's look at changes between 2026.7.16 and 2026.7.17",
            ),
            (
                "In a new branch called user/policy-engine-0.357.0, we need to update policy-engine to that version. But in order to ease future updates, it would be great to produce a skill file in the new .skills directory",
                "In a new branch called user/policy-engine-0.357.0",
            ),
            (
                "I want to investigate a different approach to the FromCatalog / from_catalogs action variant. Right now we need to add a FromCatalog variant when we want a dynamic value based on a catalog item.",
                "investigate a different approach to the FromCatalog",
            ),
            (
                "I want to rework a little bit a few UI things:\n1. on the files changed activity, for files within the project, we should show relative path\n2. we don't seem to have TOML syntax highlighting",
                "rework a little bit a few UI things",
            ),
            (
                "On another Acme IDE instance, I have a case of a broken thread: on load the thread seems to not have an active turn, however sending a message is returning an error saying a turn is already active.",
                "On another Acme IDE instance",
            ),
            (
                "Can you review the last 2 commits of this branch. First one is already on main, its expected",
                "review the last 2 commits of this branch",
            ),
            ("Hello", "Hello"),
        ];

        for (prompt, expected) in cases {
            assert_eq!(
                thread_title_from_first_prompt(prompt),
                expected,
                "prompt: {prompt}"
            );
        }
    }

    #[test]
    fn thread_title_falls_back_for_empty_prompt() {
        assert_eq!(thread_title_from_first_prompt(" \n\t "), "New thread");
    }

    #[test]
    fn thread_title_truncates_at_readable_boundary() {
        let prompt = format!("{} final words", "a".repeat(MAX_THREAD_TITLE_CHARS - 10));
        let title = thread_title_from_first_prompt(&prompt);

        assert!(title.len() <= MAX_THREAD_TITLE_CHARS);
        assert!(!title.ends_with(' '));
        assert!(!title.ends_with('-'));
    }
}

/// Enforce `browse.roots` on a project directory supplied via the API. With roots configured, the
/// directory must exist and canonicalize (symlinks resolved) to a path inside one of them. With no
/// roots configured the whole filesystem is allowed, matching the `browse` endpoint's contract.
fn ensure_dir_within_browse_roots(dir: &str, roots: &[String]) -> Result<(), ApiError> {
    if roots.is_empty() {
        return Ok(());
    }
    let canonical = PathBuf::from(dir)
        .canonicalize()
        .map_err(|e| ApiError::BadRequest(format!("cannot canonicalize project dir: {e}")))?;
    if !within_browse_roots(&canonical, roots) {
        return Err(ApiError::Forbidden(
            "project directory outside allowed roots".into(),
        ));
    }
    Ok(())
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

/// The workspace a thread's file requests resolve against — read from by `highlight`, `raw` and
/// `image`, and probed for existence by `linkify` and `render`.
///
/// Resolved per thread rather than per project because that is what these endpoints promise: the
/// caller asks about a file *as this thread sees it*, and with isolation that is no longer the same
/// answer for every thread — a thread started in its own worktree holds a *different file under the
/// same path*. `load_thread` is scoped to the project, so an unknown thread, or one belonging to
/// another project, is a `404` rather than an answer from a workspace nobody asked about.
async fn thread_workspace(
    state: &AppState,
    project_id: ProjectId,
    thread_id: ThreadId,
) -> Result<PathBuf, ApiError> {
    let project = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let thread = state
        .store
        .load_thread(project_id, thread_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(PathBuf::from(
        thread_workspace_root(state, &project, &thread).await?,
    ))
}

/// `GET /api/projects/{id}/threads/{thread_id}/highlight` — syntax-highlighted file content (spec §11.2).
///
/// Returns highlighted HTML, detected language, binary flag, total line count,
/// and file size. The `start`/`end` query params enable line-range pagination
/// for large files. Path is confined to the named thread's workspace root.
async fn highlight_file(
    State(state): State<AppState>,
    AxumPath((project_id, thread_id)): AxumPath<(ProjectId, ThreadId)>,
    Query(q): Query<HighlightQuery>,
) -> Result<Json<HighlightResponse>, ApiError> {
    let workspace_root = thread_workspace(&state, project_id, thread_id).await?;
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

/// `GET /api/projects/{id}/threads/{thread_id}/raw` — download a raw file (spec §11.2).
///
/// Returns the file contents as `application/octet-stream` with a
/// `Content-Disposition: attachment` header. Path is confined to the
/// project's workspace root.
async fn download_file(
    State(state): State<AppState>,
    AxumPath((project_id, thread_id)): AxumPath<(ProjectId, ThreadId)>,
    Query(q): Query<RawQuery>,
) -> Result<axum::response::Response, ApiError> {
    let workspace_root = thread_workspace(&state, project_id, thread_id).await?;
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

/// `GET /api/projects/{id}/threads/{thread_id}/image` — inline raster image preview.
///
/// This deliberately serves a narrower surface than `/raw`: only common raster formats are
/// returned with image MIME types for browser `<img>` rendering. SVG remains a normal file link.
async fn image_file(
    State(state): State<AppState>,
    AxumPath((project_id, thread_id)): AxumPath<(ProjectId, ThreadId)>,
    Query(q): Query<RawQuery>,
) -> Result<axum::response::Response, ApiError> {
    let workspace_root = thread_workspace(&state, project_id, thread_id).await?;
    let resolved = resolve_confined_path(&workspace_root, &q.path)
        .ok_or(ApiError::Forbidden("path escapes workspace root".into()))?;
    let content_type = raster_image_content_type(&resolved)
        .ok_or(ApiError::BadRequest("unsupported image type".into()))?;

    let bytes = tokio::fs::read(&resolved)
        .await
        .map_err(|e| ApiError::BadRequest(format!("cannot read image: {e}")))?;

    Ok(([(axum::http::header::CONTENT_TYPE, content_type)], bytes).into_response())
}

fn raster_image_content_type(path: &Path) -> Option<&'static str> {
    let ext = path.extension()?.to_string_lossy().to_ascii_lowercase();
    match ext.as_str() {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        "bmp" => Some("image/bmp"),
        "avif" => Some("image/avif"),
        _ => None,
    }
}

/// `POST /api/projects/{id}/threads/{thread_id}/linkify` — detect file paths in agent text (spec §11.2).
///
/// Scans the request body's `text` field for strings that look like file paths,
/// resolves them against the project's workspace root, and returns byte-offset
/// spans for each path that points to an existing file. The client uses these
/// spans to render clickable links in agent messages.
async fn linkify(
    State(state): State<AppState>,
    AxumPath((project_id, thread_id)): AxumPath<(ProjectId, ThreadId)>,
    Json(req): Json<LinkifyRequest>,
) -> Result<Json<LinkifyResponse>, ApiError> {
    let workspace_root = thread_workspace(&state, project_id, thread_id).await?;
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

/// `POST /api/projects/{id}/threads/{thread_id}/render` — render agent Markdown to sanitized HTML (spec §11.2).
///
/// Agents emit GitHub-flavored Markdown; this returns safe HTML the client injects directly.
/// Detected workspace paths are wrapped in `.path-link` buttons (the same affordance `/linkify`
/// feeds), so rendering and linkification are a single pass. See [`crate::markdown`] for the
/// sanitization guarantees.
async fn render_markdown(
    State(state): State<AppState>,
    AxumPath((project_id, thread_id)): AxumPath<(ProjectId, ThreadId)>,
    Json(req): Json<RenderRequest>,
) -> Result<Json<RenderResponse>, ApiError> {
    let workspace_root = thread_workspace(&state, project_id, thread_id).await?;
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

async fn git_status(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<ProjectId>,
    Query(q): Query<WorkspaceQuery>,
) -> Result<Json<GitStatusResponse>, ApiError> {
    let project = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let workspace_root = workspace_for_query(&state, &project, &q).await?;
    Ok(Json(git_status_for_workspace(&workspace_root).await))
}

#[derive(Deserialize)]
struct GitDiffQuery {
    /// The thread whose workspace to diff, matching the status the row came from. A diff read from
    /// a different tree than the row that opened it would describe someone else's changes.
    thread_id: Option<ThreadId>,
    /// Omitted for the whole working tree; present to diff a single workspace-relative path.
    path: Option<String>,
    /// Which side to diff: `staged` (index against HEAD) or `unstaged` (worktree against index).
    /// Omitted for both concatenated, which is what the whole-working-tree review uses.
    ///
    /// A path staged and then modified again is genuinely two diffs, and the UI lists it once per
    /// side; without this the row would open a combined diff that does not match the row's own
    /// line counts.
    side: Option<String>,
}

#[derive(Clone, Copy, PartialEq)]
enum GitDiffSide {
    Staged,
    Unstaged,
    Both,
}

impl GitDiffSide {
    fn parse(raw: Option<&str>) -> Result<Self, ApiError> {
        match raw.map(str::trim).filter(|side| !side.is_empty()) {
            None => Ok(Self::Both),
            Some("staged") => Ok(Self::Staged),
            Some("unstaged") => Ok(Self::Unstaged),
            Some(other) => Err(ApiError::BadRequest(format!(
                "unknown diff side {other:?}; expected \"staged\" or \"unstaged\""
            ))),
        }
    }

    fn includes_staged(self) -> bool {
        matches!(self, Self::Staged | Self::Both)
    }

    fn includes_unstaged(self) -> bool {
        matches!(self, Self::Unstaged | Self::Both)
    }
}

async fn git_diff(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<ProjectId>,
    Query(q): Query<GitDiffQuery>,
) -> Result<Json<GitDiffResponse>, ApiError> {
    let project = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let workspace_root = workspace_for_query(
        &state,
        &project,
        &WorkspaceQuery {
            thread_id: q.thread_id,
        },
    )
    .await?;
    let relative_path = match q.path.as_deref().map(str::trim).filter(|p| !p.is_empty()) {
        Some(requested) => Some(
            safe_git_relative_path(requested)
                .ok_or_else(|| ApiError::Forbidden("path escapes workspace root".into()))?,
        ),
        None => None,
    };
    let side = GitDiffSide::parse(q.side.as_deref())?;
    // With a path we pass it as an explicit pathspec after `--`; without one we diff the whole
    // working tree, so there is no pathspec and no `--` separator to add.
    let pathspec: &[&str] = match relative_path.as_deref() {
        Some(path) => &[path],
        None => &[],
    };

    let staged_args: &[&str] = if relative_path.is_some() {
        &["diff", "--cached", "--no-ext-diff", "--"]
    } else {
        &["diff", "--cached", "--no-ext-diff"]
    };
    let unstaged_args: &[&str] = if relative_path.is_some() {
        &["diff", "--no-ext-diff", "--"]
    } else {
        &["diff", "--no-ext-diff"]
    };
    // The two sides are independent and each carries the full git timeout, so the combined diff
    // runs them together rather than paying both in series on a large repository.
    let (staged, unstaged) = tokio::join!(
        async {
            match side.includes_staged() {
                true => Some(run_git(&workspace_root, staged_args, pathspec).await),
                false => None,
            }
        },
        async {
            match side.includes_unstaged() {
                true => Some(run_git(&workspace_root, unstaged_args, pathspec).await),
                false => None,
            }
        },
    );

    // Staged first, so the combined diff reads index-then-worktree in the order git applies them.
    let mut outputs = Vec::new();
    for output in [staged, unstaged].into_iter().flatten() {
        outputs.push(output?);
    }

    if outputs.iter().any(|output| !output.status.success()) {
        let borrowed: Vec<&std::process::Output> = outputs.iter().collect();
        let stderr = first_non_empty_stderr(&borrowed).unwrap_or_else(|| "git diff failed".into());
        warn!(
            %project_id,
            workspace = %workspace_root.display(),
            action = "git_diff",
            path = relative_path.as_deref().unwrap_or("<working tree>"),
            stderr = %stderr,
            "git diff failed"
        );
        return Err(ApiError::BadRequest(stderr));
    }

    let mut diff = String::new();
    for output in &outputs {
        let text = String::from_utf8_lossy(&output.stdout);
        if text.trim().is_empty() {
            continue;
        }
        diff.push_str(&text);
        if !diff.ends_with('\n') {
            diff.push('\n');
        }
    }
    let is_empty = diff.trim().is_empty();
    Ok(Json(GitDiffResponse {
        path: relative_path,
        diff,
        is_empty,
    }))
}

/// Merge `git diff --numstat` line counts (index and worktree) into an already-parsed status.
///
/// Numstat failures are non-fatal: the status is still useful without line counts, so a failed or
/// timed-out call just leaves every `added`/`deleted` as `None` and the totals at zero.
async fn apply_git_numstat(workspace_root: &Path, status: &mut GitStatusResponse) {
    // The index and worktree numstats are independent, and each carries the full git timeout, so
    // they run concurrently rather than making a large repository pay both in series.
    let (staged, unstaged) = tokio::join!(
        run_git_numstat(
            workspace_root,
            &["diff", "--cached", "--numstat", "--no-ext-diff", "-z"]
        ),
        run_git_numstat(
            workspace_root,
            &["diff", "--numstat", "--no-ext-diff", "-z"]
        ),
    );
    apply_numstat_counts(status, &index_numstat(staged), &index_numstat(unstaged));
}

/// Run one `git diff --numstat` and parse it, treating any failure as "no counts available": the
/// status is still worth returning without line counts.
async fn run_git_numstat(workspace_root: &Path, args: &[&str]) -> Vec<GitNumstatEntry> {
    match run_git(workspace_root, args, &[]).await {
        Ok(output) if output.status.success() => parse_git_numstat(&output.stdout),
        Ok(output) => {
            warn!(
                workspace = %workspace_root.display(),
                stderr = %String::from_utf8_lossy(&output.stderr).trim(),
                "git numstat failed; line counts omitted"
            );
            Vec::new()
        }
        Err(error) => {
            warn!(
                workspace = %workspace_root.display(),
                %error,
                "could not run git numstat; line counts omitted"
            );
            Vec::new()
        }
    }
}

async fn git_status_for_workspace(workspace_root: &Path) -> GitStatusResponse {
    let status = match run_git(
        workspace_root,
        &[
            "status",
            "--porcelain=v2",
            "-z",
            "-b",
            // `normal`, not `all`: an untracked directory collapses to one entry, the way git
            // reports it to a person. Expanding it means an unignored `node_modules` or `target`
            // returns thousands of rows nobody asked for and the line counts a number that says
            // nothing about the change.
            "--untracked-files=normal",
            "--",
        ],
        &["."],
    )
    .await
    {
        Ok(output) => output,
        Err(error) => {
            warn!(
                workspace = %workspace_root.display(),
                %error,
                "could not run git status for workspace"
            );
            return git_status_unavailable(Some(error.to_string()));
        }
    };

    if !status.status.success() {
        let message = String::from_utf8_lossy(&status.stderr).trim().to_string();
        return git_status_failure(workspace_root, &message);
    }

    let mut parsed = parse_git_status(&status.stdout);
    if parsed.dirty {
        apply_git_numstat(workspace_root, &mut parsed).await;
    }
    parsed
}

/// Classify a non-zero `git status` exit.
///
/// A workspace that simply isn't a repository is the expected case and reports
/// `is_repository: false` with no `error` — git's own "not a git repository" would otherwise be
/// surfaced by clients as a failure for every non-Git project. Anything else means the workspace
/// may well be a repository that git refused to read (dubious ownership, a locked or corrupt index,
/// an unreadable `.git`), which is degraded rather than absent: it is logged at `warn` and the
/// reason is passed to the client so it can say so instead of silently showing nothing.
///
/// `run_git` pins `LC_ALL=C`, so the message being matched is not localized.
fn git_status_failure(workspace_root: &Path, stderr: &str) -> GitStatusResponse {
    if stderr.contains("not a git repository") {
        debug!(
            workspace = %workspace_root.display(),
            action = "git_status",
            stderr = %stderr,
            "workspace is not a git repository"
        );
        return git_status_unavailable(None);
    }
    warn!(
        workspace = %workspace_root.display(),
        action = "git_status",
        stderr = %stderr,
        "git status failed for workspace"
    );
    git_status_unavailable(Some(if stderr.is_empty() {
        "git status failed".into()
    } else {
        stderr.to_string()
    }))
}

fn git_status_unavailable(error: Option<String>) -> GitStatusResponse {
    GitStatusResponse {
        is_repository: false,
        branch: None,
        head: None,
        detached: false,
        ahead: 0,
        behind: 0,
        dirty: false,
        staged_count: 0,
        unstaged_count: 0,
        untracked_count: 0,
        conflicted_count: 0,
        added_total: 0,
        deleted_total: 0,
        files: Vec::new(),
        error,
    }
}

async fn run_git(
    workspace_root: &Path,
    args: &[&str],
    paths: &[&str],
) -> Result<std::process::Output, ApiError> {
    let mut cmd = TokioCommand::new("git");
    cmd.current_dir(workspace_root)
        .args(args)
        .args(paths)
        // Tokio leaves a child running when the future that owns it is dropped, so without this a
        // timed-out git would outlive its request and accumulate across polls.
        .kill_on_drop(true)
        // Porcelain output is locale-independent, but the `fatal:` messages are not, and
        // `git_status_failure` has to tell "not a repository" apart from a repository it could not
        // read.
        .env("LC_ALL", "C")
        // Status can refresh the on-disk index, which takes the index lock. These calls are polls
        // against a workspace an agent may be working in, so they stay strictly read-only.
        .env("GIT_OPTIONAL_LOCKS", "0");
    tokio::time::timeout(GIT_COMMAND_TIMEOUT, cmd.output())
        .await
        .map_err(|_| ApiError::Unavailable("git command timed out".into()))?
        .map_err(|e| ApiError::Unavailable(format!("failed to run git: {e}")))
}

fn first_non_empty_stderr(outputs: &[&std::process::Output]) -> Option<String> {
    outputs.iter().find_map(|output| {
        let text = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if text.is_empty() { None } else { Some(text) }
    })
}

fn safe_git_relative_path(path: &str) -> Option<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() || Path::new(trimmed).is_absolute() {
        return None;
    }
    let mut parts = Vec::new();
    for component in Path::new(trimmed).components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().to_string()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("/"))
    }
}

/// `GET /api/projects/{id}/models` — the model picker list for a project, and the only model
/// listing there is: every configured model merged with each `model_listing` provider's
/// `/v1/models` discovery, and the project harness's friendly `display_name` overlaid by model id
/// where the config left one unset (spec §8.3). For a Codex project this surfaces Codex's
/// `model/list` names instead of raw ids.
///
/// `warnings` carries everything that degraded the list rather than failing it: a configured
/// provider id the harness does not know (§8.2), per-provider discovery failures, and a harness
/// that could not answer at all. Each is best-effort — the usable part of the list is always
/// returned.
async fn project_list_models(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<ProjectId>,
) -> Result<Json<ListModelsResponse>, ApiError> {
    let project_config = state
        .store
        .load_project(project_id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let config = state.store.load_config().await?;
    let (models, warnings) = refresh_project_model_catalog(&state, &project_config, &config).await;
    Ok(Json(ListModelsResponse { models, warnings }))
}

/// Return the last catalog fetched for this project, refreshing it on demand when a client starts
/// or mutates a thread before the browser has loaded the picker. This keeps model mutation aligned
/// with the descriptors that drive the UI without repeating provider and harness discovery for
/// every turn.
async fn project_model_catalog(
    state: &AppState,
    project_config: &ProjectConfig,
    config: &Config,
) -> Vec<ModelDescriptor> {
    if let Some(models) = state.model_catalogs.get(project_config.id).await {
        return models;
    }
    refresh_project_model_catalog(state, project_config, config)
        .await
        .0
}

/// Normalize a persisted thread's selected model and repair its context-window cache while holding
/// the store's per-thread lock. The returned snapshot is the only model state callers should pass
/// to a harness; deriving it before `update_thread` could overwrite a concurrent model selection.
async fn normalize_persisted_thread_model(
    state: &AppState,
    project_id: ProjectId,
    thread_id: ThreadId,
    config: &Config,
    catalog: &[ModelDescriptor],
) -> Result<Option<ThreadFile>, PersistError> {
    state
        .thread_metadata
        .mutate(project_id, thread_id, |thread| {
            let current_model =
                crate::models::normalize_model_ref(config, catalog, &thread.current_model);
            let descriptor =
                crate::models::resolve_catalog_descriptor(catalog, config, &current_model);
            let context_window = crate::models::context_window_with_runtime(
                &current_model,
                &descriptor,
                &thread.model_context_windows,
            );
            if current_model != thread.current_model || context_window != thread.context_window {
                thread.current_model = current_model;
                thread.context_window = context_window;
            }
        })
        .await
        .map(|mutation| mutation.into_current())
}

async fn refresh_project_model_catalog(
    state: &AppState,
    project_config: &ProjectConfig,
    config: &Config,
) -> (Vec<ModelDescriptor>, Vec<ModelListingWarning>) {
    let (harness_providers, mut warnings) = harness_provider_table(state, project_config).await;
    // Only an answered table can convict a configured id of being unknown. When the harness cannot
    // say, every id is unverified rather than wrong, and discovery simply does not run.
    if let Some(table) = &harness_providers {
        warnings.extend(crate::models::validate_provider_ids(
            config,
            table,
            &project_config.harness,
        ));
    }
    // Discovery needs endpoints only the harness can name. Without a table it cannot run at all,
    // and a provider configured for listing would otherwise come back short with no explanation.
    // Only an explicit `model_listing = true` earns this. Listing is on by default now, and a
    // harness that cannot introspect its providers is a capability gap rather than a failure —
    // announcing it on every refresh would be noise for everyone who never asked.
    if harness_providers.is_none()
        && config
            .providers
            .values()
            .any(|p| p.model_listing == Some(true))
    {
        warn!(
            project_id = %project_config.id,
            harness = %project_config.harness,
            action = "refresh_project_model_catalog",
            "no harness provider table; /v1/models discovery cannot run"
        );
        warnings.push(ModelListingWarning {
            source: format!("harness:{}", project_config.harness),
            message: "model discovery needs the provider endpoints this harness cannot report; \
                      only declared models are offered"
                .into(),
        });
    }
    // Only worth asking when something will actually be fetched. Keyed off the providers the
    // harness reports rather than off config: with listing on by default, config is usually empty,
    // and testing it would have quietly stopped sending `client_version` — costing the richer
    // catalog on exactly the setups it was built for.
    let client_version = if crate::models::will_query_any_provider(
        config,
        harness_providers.as_deref().unwrap_or(&[]),
    ) {
        state.registry.client_version(project_config).await
    } else {
        None
    };
    let discovery = crate::models::discover_models(
        config,
        harness_providers.as_deref().unwrap_or(&[]),
        client_version.as_deref(),
    )
    .await;
    warnings.extend(discovery.warnings);
    let (composed, harness_warning) = overlay_harness_metadata(
        state,
        project_config,
        config,
        discovery.models,
        &discovery.efforts_from_discovery,
    )
    .await;
    // Ordered once, after every source has contributed: config's declared models, discovery, and
    // the harness catalog each land at a different point (§8.3).
    let models = crate::models::order_for_picker(composed, config);
    if let Some(warning) = harness_warning {
        warnings.push(warning);
    }
    // An empty picker is never a normal outcome, and it used to arrive in silence — no models, no
    // explanation, nothing to act on. Only when nothing else spoke up, though: a provider that
    // already reported its own 401 has named the cause, and following it with a vaguer message
    // about the harness would point away from the answer the user already has.
    if models.is_empty() && warnings.is_empty() {
        warn!(
            project_id = %project_config.id,
            harness = %project_config.harness,
            action = "refresh_project_model_catalog",
            "composed model catalog is empty"
        );
        warnings.push(ModelListingWarning {
            source: format!("harness:{}", project_config.harness),
            message: "no models are available for this project: the harness reported no catalog \
                      and no provider offered one. Check that the harness starts, and that its \
                      providers are reachable."
                .into(),
        });
    }
    state
        .model_catalogs
        .replace(project_config.id, models.clone())
        .await;
    (models, warnings)
}

/// Read the providers the project's harness is configured to route to (§8.2).
///
/// Best-effort in the same way as the metadata overlay: a harness that cannot introspect its own
/// configuration, or fails to answer, yields an empty table. An empty table disables both discovery
/// and id validation rather than reporting every configured provider as unknown — absence of
/// evidence is not evidence that the ids are wrong.
async fn harness_provider_table(
    state: &AppState,
    project_config: &ProjectConfig,
) -> (Option<Vec<HarnessProvider>>, Vec<ModelListingWarning>) {
    match state.registry.capabilities(project_config).await {
        Ok(caps) if !caps.provider_listing => return (None, Vec::new()),
        Ok(_) => {}
        Err(e) => {
            warn!(
                project_id = %project_config.id,
                harness = %project_config.harness,
                error = %e,
                "cannot read harness capabilities; serving models without provider resolution"
            );
            return (
                None,
                vec![ModelListingWarning {
                    source: format!("harness:{}", project_config.harness),
                    message: format!("could not read provider-listing capabilities: {e}"),
                }],
            );
        }
    }
    match state.registry.list_providers(project_config).await {
        Ok(providers) => (Some(providers), Vec::new()),
        Err(e) => {
            warn!(
                project_id = %project_config.id,
                harness = %project_config.harness,
                error = %e,
                "harness provider listing failed; serving models without provider resolution"
            );
            (
                None,
                vec![ModelListingWarning {
                    source: format!("harness:{}", project_config.harness),
                    message: format!("provider listing failed: {e}"),
                }],
            )
        }
    }
}

/// Overlay the project harness's model metadata (friendly names + advertised reasoning efforts) onto
/// `base` when the harness supports model listing. Best-effort: capability or listing failures are
/// logged and leave `base` unchanged, so a non-Codex harness (or a Codex process that can't answer)
/// just yields config/discovered metadata.
async fn overlay_harness_metadata(
    state: &AppState,
    project_config: &ProjectConfig,
    config: &Config,
    base: Vec<ModelDescriptor>,
    efforts_from_discovery: &std::collections::HashSet<(String, String)>,
) -> (Vec<ModelDescriptor>, Option<ModelListingWarning>) {
    match state.registry.capabilities(project_config).await {
        Ok(caps) if !caps.model_listing => return (base, None),
        Ok(_) => {}
        Err(e) => {
            warn!(
                project_id = %project_config.id,
                harness = %project_config.harness,
                error = %e,
                "cannot read harness capabilities; serving models without harness metadata"
            );
            return (
                base,
                Some(ModelListingWarning {
                    source: format!("harness:{}", project_config.harness),
                    message: format!("could not read model-listing capabilities: {e}"),
                }),
            );
        }
    }
    match state.registry.list_models(project_config).await {
        Ok(harness_models) => (
            crate::models::apply_harness_metadata(
                base,
                &harness_models,
                config,
                efforts_from_discovery,
            ),
            None,
        ),
        Err(e) => {
            warn!(
                project_id = %project_config.id,
                harness = %project_config.harness,
                error = %e,
                "harness model listing failed; serving models without harness metadata"
            );
            (
                base,
                Some(ModelListingWarning {
                    source: format!("harness:{}", project_config.harness),
                    message: format!("model listing failed: {e}"),
                }),
            )
        }
    }
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
    if !capabilities.mcp_status {
        warn!(
            %project_id,
            harness = %project_config.harness,
            "MCP status requested but harness reports it unsupported"
        );
    }
    let servers = if capabilities.mcp_status {
        state
            .registry
            .list_mcp_servers(&project_config)
            .await
            .map_err(harness_api_error)?
    } else {
        Vec::new()
    };
    info!(
        %project_id,
        harness = %project_config.harness,
        mcp_status_supported = capabilities.mcp_status,
        mcp_reload_supported = capabilities.mcp_reload,
        mcp_oauth_login_supported = capabilities.mcp_oauth_login,
        server_count = servers.len(),
        "MCP server status loaded"
    );
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
        warn!(
            %project_id,
            harness = %project_config.harness,
            "MCP reload requested but harness reports it unsupported"
        );
        return Err(ApiError::BadRequest(
            "MCP server reload is not supported by this harness".into(),
        ));
    }
    state
        .registry
        .reload_mcp_servers(&project_config)
        .await
        .map_err(harness_api_error)?;
    info!(
        %project_id,
        harness = %project_config.harness,
        "MCP server reload completed"
    );
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
        warn!(
            %project_id,
            harness = %project_config.harness,
            server = name,
            "MCP OAuth login requested but harness reports it unsupported"
        );
        return Err(ApiError::BadRequest(
            "MCP OAuth login is not supported by this harness".into(),
        ));
    }
    let login = state
        .registry
        .start_mcp_oauth_login(&project_config, name)
        .await
        .map_err(harness_api_error)?;
    info!(
        %project_id,
        harness = %project_config.harness,
        server = name,
        authorization_url_returned = !login.authorization_url.is_empty(),
        "MCP OAuth login started"
    );
    Ok(Json(login))
}

fn harness_api_error(error: HarnessError) -> ApiError {
    match error {
        HarnessError::Unsupported(message) => ApiError::BadRequest(message),
        HarnessError::ThreadBusy { .. } => {
            ApiError::Conflict("Thread already has an active turn.".into())
        }
        HarnessError::Timeout(message) => ApiError::Unavailable(message),
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

/// Mint a short-lived WebSocket ticket. Tickets travel in the `/api/ws` query string (which can
/// land in reverse-proxy access logs), so they are domain-separated from session cookies: a
/// leaked ticket is only good for a WebSocket upgrade, and only for 60 seconds.
async fn ws_ticket(State(state): State<AppState>) -> Result<Json<WsTicketResponse>, ApiError> {
    let expiry = (Utc::now().timestamp() as u64) + 60;
    let ticket = sign_token(TokenPurpose::WsTicket, expiry, &state.session_key)
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
        .map(|t| verify_token(TokenPurpose::Session, t, &state.session_key))
        .unwrap_or(false)
        || q.ticket
            .as_deref()
            .map(|t| verify_token(TokenPurpose::WsTicket, t, &state.session_key))
            .unwrap_or(false);

    if !valid {
        return Err(axum::http::StatusCode::UNAUTHORIZED);
    }

    Ok(ws
        .max_message_size(MAX_WS_MESSAGE_BYTES)
        .max_frame_size(MAX_WS_MESSAGE_BYTES)
        .on_upgrade(move |socket| handle_ws(socket, state)))
}

#[derive(Debug, Clone)]
struct WsError {
    info: Box<ErrorInfo>,
}

impl WsError {
    fn new(code: impl Into<String>, severity: ErrorSeverity, message: impl Into<String>) -> Self {
        Self {
            info: Box::new(ErrorInfo {
                code: code.into(),
                severity,
                message: message.into(),
                detail: None,
                thread_id: None,
                action: None,
                request_id: None,
                process_id: None,
            }),
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
        ServerMessage::Error { error: *self.info }
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
        request_id: None,
        process_id: None,
    }
}

async fn history_limit_or_default(
    state: &AppState,
    thread_id: ThreadId,
    action: &'static str,
    select: impl FnOnce(&giskard_persist::Config) -> usize,
    fallback: usize,
) -> usize {
    match state.store.load_config().await {
        Ok(config) => select(&config),
        Err(error) => {
            warn!(
                %thread_id,
                action = %action,
                %error,
                fallback,
                "failed to load history config; using fallback page size"
            );
            fallback
        }
    }
}

fn harness_error_means_command_unmanaged(error: &HarnessError) -> bool {
    let HarnessError::Transport(message) = error else {
        return false;
    };
    let message = message.to_ascii_lowercase();
    message.contains("no active command/exec for process id")
        || message.contains("no active turn to interrupt")
}

/// Tell a just-connected client which threads are already waiting on it.
///
/// Cross-thread activity is broadcast live and never replayed, so a browser that was closed or
/// disconnected when a sub-agent raised an approval has no way to learn the thread is blocked — the
/// child has no sidebar row of its own, and the approval only reaches the transcript once that
/// thread is opened. Sent to this client alone, before it subscribes to anything.
async fn send_activity_bootstrap(
    state: &AppState,
    client_id: usize,
    tx: &mpsc::Sender<ServerMessage>,
) {
    let pending = state.live_buffers.pending_attention().await;
    if pending.is_empty() {
        return;
    }
    let mut activities = Vec::new();
    for entry in &pending {
        for approval in &entry.approvals {
            activities.push(ThreadActivity {
                thread_id: entry.thread_id,
                kind: ThreadActivityKind::ApprovalRequested {
                    approval_id: approval.id.to_string(),
                },
                active_turn: true,
                summary: Some("Approval requested".into()),
            });
        }
        for request in &entry.server_requests {
            activities.push(ThreadActivity {
                thread_id: entry.thread_id,
                kind: ThreadActivityKind::ServerRequestReceived {
                    server_request_id: request.id.to_string(),
                },
                active_turn: true,
                summary: Some("Waiting for your input".to_string()),
            });
        }
    }
    if activities.is_empty() {
        return;
    }
    debug!(
        %client_id,
        threads = pending.len(),
        activities = activities.len(),
        "replaying pending cross-thread activity to a connecting client"
    );
    let _ = tx
        .send(ServerMessage::ThreadActivityBootstrap { activities })
        .await;
}

async fn handle_ws(socket: WebSocket, state: AppState) {
    let (tx, mut rx) = mpsc::channel::<ServerMessage>(256);
    let client_id = state.hub.next_client_id();

    let (mut ws_sender, mut ws_receiver) = socket.split();

    let replacements = state.hub.register_client(client_id, tx.clone()).await;
    let (writer_done_tx, mut writer_done_rx) = oneshot::channel();

    // Order matters, and is load-bearing in both directions:
    //
    // 1. Start draining before the awaited bootstrap send below. Registration itself does not
    //    await client capacity, so the short registration-to-spawn interval cannot park a domain
    //    producer or this connection.
    // 2. Register before computing the bootstrap, not after. Anything that changes between the
    //    snapshot and its delivery then also reaches this client as a live event, so a badge cannot
    //    be left showing state that resolved in the gap.
    //
    // A live broadcast can therefore land ahead of the bootstrap. That is harmless: the live event
    // claims the notification dedup key, so the replay for the same approval is suppressed rather
    // than alerting twice.
    let send_task = tokio::spawn(async move {
        loop {
            // Replacement state bypasses the ordered FIFO. It stays latest-by-key until the
            // socket writer actually selects it, so obsolete metadata cannot consume event
            // capacity or make a domain producer wait on a slow peer.
            let msg = tokio::select! {
                ordered = rx.recv() => {
                    let Some(message) = ordered else { break; };
                    message
                }
                replacement = replacements.recv() => replacement,
            };
            let json = match serde_json::to_string(&msg) {
                Ok(json) => json,
                Err(e) => {
                    error!(
                        %client_id,
                        action = "serialize_ws_message",
                        error = %e,
                        "failed to serialize WebSocket message"
                    );
                    continue;
                }
            };
            if let Err(e) = ws_sender.send(Message::Text(json.into())).await {
                debug!(
                    %client_id,
                    action = "write_ws_message",
                    error = %e,
                    "WebSocket writer stopped"
                );
                break;
            }
        }
        let _ = writer_done_tx.send(());
    });

    send_activity_bootstrap(&state, client_id, &tx).await;

    let hub = state.hub.clone();

    loop {
        let incoming = tokio::select! {
            incoming = ws_receiver.next() => incoming,
            _ = &mut writer_done_rx => {
                debug!(
                    %client_id,
                    action = "stop_ws_receiver",
                    "WebSocket writer ended; stopping receive loop"
                );
                break;
            }
        };
        match incoming {
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
                let metadata_request_id = match &msg {
                    ClientMessage::SwitchMode { request_id, .. }
                    | ClientMessage::SelectModel { request_id, .. }
                    | ClientMessage::SetPermissionPreset { request_id, .. } => {
                        Some(request_id.clone())
                    }
                    _ => None,
                };
                if let Err(mut e) = handle_client_msg(&state, client_id, &tx, msg).await {
                    e.info.request_id = metadata_request_id;
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
    if !send_task.is_finished() {
        send_task.abort();
    }
}

async fn handle_client_msg(
    state: &AppState,
    client_id: usize,
    tx: &mpsc::Sender<ServerMessage>,
    msg: ClientMessage,
) -> Result<(), WsError> {
    match msg {
        ClientMessage::Subscribe { thread_id, since } => {
            // Attaching the harness is best-effort. If it fails — most often because the thread's
            // provider was removed from config — degrade to a read-only view: the persisted
            // history is still served and the attach failure is surfaced as a non-fatal warning,
            // so an orphaned thread stays viewable even though it can never run a new turn. Only a
            // genuinely missing thread remains a hard error.
            let (project_id, notice) = match ensure_thread_open(state, thread_id, "subscribe").await
            {
                Ok(access) => (access.project_id, access.warning),
                Err(attach_error) => {
                    let project_id = project_for_readonly(state, thread_id, "subscribe").await?;
                    warn!(
                        %thread_id,
                        code = %attach_error.info.code,
                        detail = ?attach_error.info.detail,
                        "thread harness attach failed; serving read-only history"
                    );
                    (
                        project_id,
                        Some(read_only_warning(state, project_id, &attach_error, thread_id).await),
                    )
                }
            };
            // Registering before the snapshot below is built is what makes the snapshot's
            // `active_turn` safe to act on: a turn that ends after this line broadcasts its
            // `TurnCompleted` to this client, and one that ended before it is already out of the
            // turn gate. Build the snapshot first and a turn ending in between would be reported
            // live by a client that then never hears it finish.
            if !state.hub.subscribe(thread_id, client_id).await {
                return Err(WsError::new(
                    "ws_client_not_registered",
                    ErrorSeverity::Error,
                    "WebSocket client registration was lost; reconnect to continue.",
                )
                .thread(thread_id)
                .action("subscribe"));
            }

            if let Some(warning) = notice {
                let _ = tx.send(ServerMessage::Error { error: warning }).await;
            }

            let tf = state
                .thread_metadata
                .recompute_aggregates(project_id, thread_id)
                .await
                .map_err(|e| WsError::from_persist(e, "subscribe_history", Some(thread_id)))?
                .into_current()
                .ok_or_else(|| {
                    WsError::new(
                        "thread_not_found",
                        ErrorSeverity::Error,
                        "Thread not found.",
                    )
                    .thread(thread_id)
                    .action("subscribe")
                })?;
            let _ = tx
                .send(ServerMessage::ThreadState(
                    crate::thread_metadata::ThreadMetadataService::thread_state(
                        &tf,
                        Some(state.registry.thread_has_active_turn(thread_id).await),
                    ),
                ))
                .await;

            // Two snapshot shapes, distinguished by whether the client sent a resync cursor:
            //
            // * Resync (`since` present): history-first ordering. Send the persisted history — a
            //   `HistoryDelta` of the turns after the cursor when we can resolve it, or a full
            //   `HistoryPage` when we can't (stale cursor) — *before* the live turn and tasks. The
            //   client reconciles or rebuilds the transcript while it still owns it, then the live
            //   turn appends on top. The browser may keep a stale live DOM block visible until the
            //   replacement snapshot arrives, so delta rows still need to be inserted before that
            //   retained live block on the UI side.
            // * Fresh (`since` absent): live-first ordering. The in-flight turn (H5) isn't in the
            //   JSONL yet, so reconstruct it from the live buffer and send it — with its tasks —
            //   before the history page, for the fastest first paint. The browser prepends older
            //   history above it.
            let resync_delta = match since {
                Some(cursor) => state
                    .store
                    .load_turns_after(project_id, thread_id, cursor)
                    .await
                    .map_err(|e| WsError::from_persist(e, "subscribe_resync", Some(thread_id)))?,
                None => None,
            };

            // The persisted-history message: a delta after a resolvable cursor, otherwise a full
            // initial page (fresh open, or a stale cursor that fell back to a full rebuild).
            let history_message = if let Some(turns) = resync_delta {
                ServerMessage::HistoryDelta {
                    thread_id,
                    turns: turns.into_iter().map(Into::into).collect(),
                }
            } else {
                // H4/H6: the most recent page of persisted history (not the whole thread). The
                // initial page is deliberately small (see `HistoryConfig::initial`); the browser
                // tops it up to fill the viewport. Older pages are fetched via `LoadHistory`.
                let limit = history_limit_or_default(
                    state,
                    thread_id,
                    "subscribe_history",
                    |config| config.history.initial,
                    5,
                )
                .await;
                let (turns, has_more) = state
                    .store
                    .load_history(project_id, thread_id, None, limit)
                    .await
                    .map_err(|e| WsError::from_persist(e, "subscribe_history", Some(thread_id)))?;
                ServerMessage::HistoryPage {
                    thread_id,
                    turns: turns.into_iter().map(Into::into).collect(),
                    has_more,
                }
            };

            // The live turn (H5) isn't in the JSONL yet — reconstruct it from the live buffer — and
            // its running tasks. On a fresh open (`since` absent) these go first, for the fastest
            // first paint. On a resync (`since` present, delta or stale-cursor rebuild) the history
            // goes first so the client can reconcile or rebuild before handling the live turn. The
            // browser may still insert delta rows before a retained stale live block to avoid a
            // visible gap before the replacement live snapshot arrives.
            let live_snapshot = state.live_buffers.snapshot(thread_id).await;
            let tasks = state.running_commands.snapshot(thread_id).await;
            let running_tasks = ServerMessage::RunningTasks { thread_id, tasks };
            if since.is_some() {
                let _ = tx.send(history_message).await;
                if let Some(snap) = live_snapshot {
                    let _ = tx.send(ServerMessage::LiveTurnSnapshot(snap)).await;
                }
                let _ = tx.send(running_tasks).await;
            } else {
                if let Some(snap) = live_snapshot {
                    let _ = tx.send(ServerMessage::LiveTurnSnapshot(snap)).await;
                }
                let _ = tx.send(running_tasks).await;
                let _ = tx.send(history_message).await;
            }
        }
        ClientMessage::LoadHistory {
            thread_id,
            before,
            limit,
        } => {
            let project_id = project_for_readonly(state, thread_id, "load_history").await?;
            let default_limit = history_limit_or_default(
                state,
                thread_id,
                "load_history",
                |config| config.history.page,
                50,
            )
            .await;
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
        ClientMessage::SendInput {
            thread_id,
            text,
            attachments,
        } => {
            let text = text.trim().to_string();
            if text.is_empty() && attachments.is_empty() {
                return Err(WsError::new(
                    "empty_input",
                    ErrorSeverity::Error,
                    "Send a message or attach a file.",
                )
                .thread(thread_id)
                .action("send_input"));
            }
            validate_user_attachments(&attachments).map_err(|error| {
                WsError::new(
                    "invalid_attachment",
                    ErrorSeverity::Error,
                    "One or more attachments could not be sent.",
                )
                .detail(error.to_string())
                .thread(thread_id)
                .action("send_input")
            })?;
            let project_id = project_for(state, thread_id, "send_input").await?;
            let app_config = state
                .store
                .load_config()
                .await
                .map_err(|e| WsError::from_persist(e, "send_input", Some(thread_id)))?;
            let project_config = state
                .store
                .load_project(project_id)
                .await
                .map_err(|e| WsError::from_persist(e, "send_input", Some(thread_id)))?
                .ok_or_else(|| {
                    WsError::new(
                        "project_not_found",
                        ErrorSeverity::Error,
                        "Project not found.",
                    )
                    .thread(thread_id)
                    .action("send_input")
                })?;
            let catalog = project_model_catalog(state, &project_config, &app_config).await;
            // RMW under the per-thread lock: bump activity and read back the resolved state.
            let tf = state
                .thread_metadata
                .mutate(project_id, thread_id, |tf| {
                    let normalized = crate::models::normalize_model_ref(
                        &app_config,
                        &catalog,
                        &tf.current_model,
                    );
                    let descriptor = crate::models::resolve_catalog_descriptor(
                        &catalog,
                        &app_config,
                        &normalized,
                    );
                    tf.context_window = crate::models::context_window_with_runtime(
                        &normalized,
                        &descriptor,
                        &tf.model_context_windows,
                    );
                    tf.current_model = normalized;
                })
                .await
                .map_err(|e| WsError::from_persist(e, "send_input", Some(thread_id)))?
                .into_current()
                .ok_or_else(|| {
                    WsError::new(
                        "thread_not_found",
                        ErrorSeverity::Error,
                        "Thread not found.",
                    )
                    .thread(thread_id)
                    .action("send_input")
                })?;

            let tf = ensure_send_harness_provider_current(state, project_id, thread_id, tf).await?;
            let effective_model = tf.current_model.clone();

            // Resolved snapshot the harness applies to `turn/start` (§7.5, §8.4/§8.5):
            //  - the thread's current model (carrying its reasoning effort), so a mid-thread
            //    model/effort change actually reaches the agent. Passing `None` here would leave
            //    Codex on whatever model was set at `thread/start`.
            //  - the thread's persisted permission preset (§9).
            let overrides = TurnOverrides {
                model: Some(effective_model.clone()),
                mode: tf.mode,
                permission_preset: tf.permission_preset,
            };

            state
                .registry
                .start_turn(
                    thread_id,
                    UserInput::text_with_attachments(text, attachments),
                    overrides,
                    effective_model,
                )
                .await
                .map_err(|e| WsError::from_harness(e, "send_input", Some(thread_id)))?;
        }
        ClientMessage::SwitchMode {
            thread_id,
            request_id,
            mode,
        } => {
            let project_id = project_for(state, thread_id, "switch_mode").await?;
            let tf = state
                .thread_metadata
                .mutate_with_recency(project_id, thread_id, ThreadRecency::TouchIfChanged, |tf| {
                    tf.mode = mode
                })
                .await
                .map_err(|e| WsError::from_persist(e, "switch_mode", Some(thread_id)))?
                .into_current()
                .ok_or_else(|| {
                    WsError::new(
                        "thread_not_found",
                        ErrorSeverity::Error,
                        "Thread not found.",
                    )
                    .thread(thread_id)
                    .action("switch_mode")
                })?;
            let _ = tx
                .send(ServerMessage::ThreadMetadataResult {
                    request_id,
                    metadata: crate::thread_metadata::ThreadMetadataService::metadata(&tf),
                })
                .await;
        }
        ClientMessage::SelectModel {
            thread_id,
            request_id,
            model_ref,
        } => {
            // Resolve the project without forcing a harness attach: model selection must work on
            // a *cold* thread too — that is exactly how an orphaned thread (provider removed from
            // config) gets rescued.
            let project_id = project_for_readonly(state, thread_id, "select_model").await?;
            let config = state
                .store
                .load_config()
                .await
                .map_err(|e| WsError::from_persist(e, "select_model", Some(thread_id)))?;
            let project_config = state
                .store
                .load_project(project_id)
                .await
                .map_err(|e| WsError::from_persist(e, "select_model", Some(thread_id)))?
                .ok_or_else(|| {
                    WsError::new(
                        "project_not_found",
                        ErrorSeverity::Error,
                        "Project not found.",
                    )
                    .thread(thread_id)
                    .action("select_model")
                })?;
            let catalog = project_model_catalog(state, &project_config, &config).await;
            let model_ref = crate::models::normalize_model_ref(&config, &catalog, &model_ref);

            if state
                .registry
                .get_thread_native_model(thread_id)
                .await
                .is_some()
            {
                // Warm thread: the provider is bound to the loaded Codex session (PB2) —
                // cross-provider changes stay rejected because a loaded thread can silently
                // ignore resume overrides.
                ensure_provider_change_allowed(
                    state,
                    project_id,
                    thread_id,
                    &model_ref,
                    "select_model",
                )
                .await?;
            } else {
                // Cold thread (not loaded in this server process): a cross-provider switch is
                // reliable via a *verified* cold re-resume — Codex applies `thread/resume`
                // model/provider overrides when the thread is not loaded, and we confirm the
                // response before persisting anything (spec PS1). Same-provider selections need
                // no attach: persisting is enough, the next open resumes with the new model.
                let stored_provider = state
                    .store
                    .load_thread(project_id, thread_id)
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
                    })?
                    .current_model
                    .provider;
                if stored_provider != model_ref.provider
                    && let Some(warning) =
                        switch_provider_cold(state, project_id, thread_id, &model_ref).await?
                {
                    let _ = tx.send(ServerMessage::Error { error: warning }).await;
                }
            }

            // All model/effort resolution happens inside the RMW closure so it sees the
            // authoritative current model under the per-thread lock (§5.4, C7 effort retention).
            let tf = state
                .thread_metadata
                .mutate_with_recency(
                    project_id,
                    thread_id,
                    ThreadRecency::TouchIfChanged,
                    move |tf| {
                        let old = crate::models::resolve_catalog_descriptor(
                            &catalog,
                            &config,
                            &tf.current_model,
                        );
                        if old.supports_reasoning_effort
                            && let Some(effort) = tf.current_model.reasoning_effort.clone()
                        {
                            tf.model_efforts.insert(tf.current_model.key(), effort);
                        }

                        let new_descriptor = crate::models::resolve_catalog_descriptor(
                            &catalog, &config, &model_ref,
                        );
                        let mut new_model = model_ref.clone();
                        let same_model = tf.current_model.provider == new_model.provider
                            && tf.current_model.model == new_model.model;
                        if new_descriptor.supports_reasoning_effort {
                            if same_model && new_model.reasoning_effort.is_none() {
                                tf.model_efforts.remove(&new_model.key());
                            } else if new_model.reasoning_effort.is_none() {
                                new_model.reasoning_effort =
                                    tf.model_efforts.get(&new_model.key()).cloned();
                            }
                        } else {
                            new_model.reasoning_effort = None;
                        }

                        tf.context_window = crate::models::context_window_with_runtime(
                            &new_model,
                            &new_descriptor,
                            &tf.model_context_windows,
                        );
                        tf.current_model = new_model;
                    },
                )
                .await
                .map_err(|e| WsError::from_persist(e, "select_model", Some(thread_id)))?
                .into_current()
                .ok_or_else(|| {
                    WsError::new(
                        "thread_not_found",
                        ErrorSeverity::Error,
                        "Thread not found.",
                    )
                    .thread(thread_id)
                    .action("select_model")
                })?;
            let _ = tx
                .send(ServerMessage::ThreadMetadataResult {
                    request_id,
                    metadata: crate::thread_metadata::ThreadMetadataService::metadata(&tf),
                })
                .await;
        }
        ClientMessage::SetPermissionPreset {
            thread_id,
            request_id,
            preset,
        } => {
            let project_id = project_for(state, thread_id, "set_permission_preset").await?;
            let tf = state
                .thread_metadata
                .mutate_with_recency(project_id, thread_id, ThreadRecency::TouchIfChanged, |tf| {
                    tf.permission_preset = preset
                })
                .await
                .map_err(|e| WsError::from_persist(e, "set_permission_preset", Some(thread_id)))?
                .into_current()
                .ok_or_else(|| {
                    WsError::new(
                        "thread_not_found",
                        ErrorSeverity::Error,
                        "Thread not found.",
                    )
                    .thread(thread_id)
                    .action("set_permission_preset")
                })?;
            let _ = tx
                .send(ServerMessage::ThreadMetadataResult {
                    request_id,
                    metadata: crate::thread_metadata::ThreadMetadataService::metadata(&tf),
                })
                .await;
        }
        ClientMessage::ApprovalDecision {
            request_id,
            decision,
        } => {
            let request_id_for_broadcast = request_id.clone();
            let thread_id = state
                .registry
                .respond_approval(giskard_core::ids::ApprovalId(request_id), decision.clone())
                .await
                .map_err(|e| WsError::from_harness(e, "approval_decision", None))?;
            // Record the resolution against the in-flight turn so a browser reload replays this
            // approval as answered rather than re-prompting the user (spec §13.6).
            state
                .live_buffers
                .resolve_approval(
                    thread_id,
                    giskard_core::ids::ApprovalId(request_id_for_broadcast.clone()),
                    decision.clone(),
                )
                .await;
            debug!(
                %thread_id,
                request_id = %request_id_for_broadcast,
                ?decision,
                "recorded approval resolution in live buffer for reconnect"
            );
            state
                .hub
                .broadcast(
                    thread_id,
                    ServerMessage::ApprovalResolved {
                        thread_id,
                        request_id: request_id_for_broadcast,
                        decision,
                    },
                )
                .await;
        }
        ClientMessage::ServerRequestResponse {
            request_id,
            response,
        } => {
            let request_id_for_log = request_id.clone();
            let req_id = giskard_core::ids::ServerRequestId(request_id);
            let thread_id = tokio::time::timeout(
                HARNESS_CONTROL_TIMEOUT,
                state
                    .registry
                    .respond_server_request(req_id.clone(), response),
            )
            .await
            .map_err(|_| {
                error!(
                    request_id = %request_id_for_log,
                    timeout_ms = HARNESS_CONTROL_TIMEOUT.as_millis(),
                    "server request response timed out waiting for Codex"
                );
                WsError::from_harness(
                    HarnessError::Timeout(
                        "server request response timed out waiting for Codex".into(),
                    ),
                    "server_request_response",
                    None,
                )
            })?
            .map_err(|e| WsError::from_harness(e, "server_request_response", None))?;
            // Record the answer against the in-flight turn. The harness emits its own resolved
            // event, but on its own schedule and not guaranteed at all; until then the request
            // still reads as outstanding in the replayed events, so a reload in that window would
            // re-prompt and re-answering routes a stale id to the harness (spec §13.6).
            state
                .live_buffers
                .resolve_server_request(thread_id, req_id)
                .await;
            debug!(
                %thread_id,
                request_id = %request_id_for_log,
                "recorded server request resolution in live buffer for reconnect"
            );
        }
        ClientMessage::Interrupt { thread_id } => {
            tokio::time::timeout(HARNESS_CONTROL_TIMEOUT, state.registry.interrupt(thread_id))
                .await
                .map_err(|_| {
                    error!(
                        %thread_id,
                        timeout_ms = HARNESS_CONTROL_TIMEOUT.as_millis(),
                        "interrupt request timed out waiting for Codex"
                    );
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
                error!(
                    %thread_id,
                    timeout_ms = HARNESS_CONTROL_TIMEOUT.as_millis(),
                    "context compaction request timed out waiting for Codex"
                );
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
                error!(
                    %thread_id,
                    process_id = %process_id_for_state,
                    timeout_ms = HARNESS_CONTROL_TIMEOUT.as_millis(),
                    had_running_task = existing_command.is_some(),
                    running_task_after_turn = existing_command
                        .as_ref()
                        .map(|cmd| cmd.after_turn)
                        .unwrap_or(false),
                    "terminate command request timed out waiting for Codex"
                );
                HarnessError::Timeout(
                    "terminate command request timed out waiting for Codex".into(),
                )
            });
            if let Err(error) = terminate_result.and_then(|result| result) {
                if harness_error_means_command_unmanaged(&error)
                    && existing_command
                        .as_ref()
                        .map(|cmd| cmd.after_turn)
                        .unwrap_or(false)
                {
                    let removed = state
                        .running_commands
                        .remove_by_process(thread_id, &process_id_for_state)
                        .await;
                    if removed {
                        broadcast_running_commands(state, thread_id).await;
                    }
                    warn!(
                        %thread_id,
                        process_id = %process_id_for_state,
                        running_task_removed = removed,
                        error = %error,
                        "harness no longer manages after-turn command; cleared stale running-task state"
                    );
                    let _ = tx
                        .send(ServerMessage::Error {
                            error: ErrorInfo {
                                code: "harness_command_unmanaged".into(),
                                severity: ErrorSeverity::Warning,
                                message: "The harness no longer manages this command.".into(),
                                detail: Some(format!(
                                    "{error}. The command may still be running in the harness environment."
                                )),
                                thread_id: Some(thread_id),
                                action: Some("terminate_command".into()),
                                request_id: None,
                                process_id: Some(process_id_for_state),
                            },
                        })
                        .await;
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
            let written = save_plan(state, thread_id, &path).await.map_err(|e| {
                WsError::new(
                    "save_plan_failed",
                    ErrorSeverity::Error,
                    "Save plan failed.",
                )
                .detail(e)
                .thread(thread_id)
                .action("save_plan")
            })?;
            debug!(%thread_id, path = %written, "plan saved");
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

    let Some((project_config, persisted_thread)) =
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

    // Resume must land in the worktree the thread works in — its own, or its parent's for a
    // sub-agent. Falling back to the project's checkout here would silently un-isolate the thread
    // after a restart, with nothing in the UI to say so.
    let ws_root = thread_workspace_root(state, &project_config, &persisted_thread)
        .await
        .map_err(|e| {
            WsError::new("workspace_unavailable", ErrorSeverity::Error, e.to_string())
                .thread(thread_id)
                .action(action)
        })?;
    let app_config = state
        .store
        .load_config()
        .await
        .map_err(|e| WsError::from_persist(e, action, Some(thread_id)))?;
    let catalog = project_model_catalog(state, &project_config, &app_config).await;
    let thread_file = normalize_persisted_thread_model(
        state,
        project_config.id,
        thread_id,
        &app_config,
        &catalog,
    )
    .await
    .map_err(|e| WsError::from_persist(e, action, Some(thread_id)))?
    .ok_or_else(|| {
        WsError::new(
            "thread_not_found",
            ErrorSeverity::Error,
            "Thread not found.",
        )
        .thread(thread_id)
        .action(action)
    })?;
    let current_model = thread_file.current_model.clone();
    debug!(
        project_id = %project_config.id,
        %thread_id,
        harness_thread_id = %thread_file.harness_thread_id,
        %action,
        "reopening persisted thread"
    );
    let handle = if thread_file.kind == ThreadKind::Subagent {
        state
            .registry
            .open_linked_thread(
                &project_config,
                &ws_root,
                Some(thread_id),
                thread_file.harness_thread_id.clone(),
                current_model,
            )
            .await
    } else {
        state
            .registry
            .open_thread(
                &project_config,
                &ws_root,
                Some(thread_id),
                Some(thread_file.harness_thread_id.clone()),
                Some(current_model),
            )
            .await
    }
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
            .thread_metadata
            .mutate(project_config.id, thread_id, |tf| {
                tf.harness_thread_id = harness_thread_id;
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

/// Resolve a thread's project using only persistence — **no harness attach**. The read-only paths
/// (subscribe history, load_history) use this so a thread whose provider was removed from config
/// stays viewable even though its harness can never re-attach. Prefers an already-open thread's
/// registered project, then falls back to scanning persistence.
async fn project_for_readonly(
    state: &AppState,
    thread_id: ThreadId,
    action: &str,
) -> Result<ProjectId, WsError> {
    if let Some(project_id) = state.registry.get_project_for_thread(thread_id).await {
        return Ok(project_id);
    }
    match find_persisted_thread(state, thread_id, action).await? {
        Some((project_config, _)) => Ok(project_config.id),
        None => Err(WsError::new(
            "thread_not_found",
            ErrorSeverity::Error,
            "Thread not found.",
        )
        .thread(thread_id)
        .action(action)),
    }
}

/// The thread's provider id plus whether it is still declared in config — the context that lets
/// the read-only message name the culprit precisely instead of hedging.
struct ReadOnlyProviderContext {
    provider: String,
    configured: bool,
}

/// Best-effort provider context for a read-only warning; `None` when persistence can't be read
/// (the warning then falls back to generic wording rather than failing the whole degrade path).
async fn read_only_provider_context(
    state: &AppState,
    project_id: ProjectId,
    thread_id: ThreadId,
) -> Option<ReadOnlyProviderContext> {
    let provider = state
        .store
        .load_thread(project_id, thread_id)
        .await
        .ok()??
        .current_model
        .provider;
    let config = state.store.load_config().await.ok();
    // Without the project we cannot ask its harness which providers exist, so config is all there
    // is — and config alone can no longer convict a provider.
    let configured = match state.store.load_project(project_id).await.ok().flatten() {
        Some(project_config) => {
            provider_is_known(state, &project_config, config.as_ref(), &provider).await
        }
        None => true,
    };
    Some(ReadOnlyProviderContext {
        provider,
        configured,
    })
}

/// Whether a provider is one this project can actually route to, for the read-only warning.
///
/// Absence from `[providers.*]` used to prove a provider was gone. It no longer does: model
/// listing is on by default and most configs name no providers at all, so testing config alone
/// would tell nearly every user their provider "is no longer configured" whenever a harness failed
/// to attach for some unrelated reason.
///
/// The harness table is the authority (§8.2), but this runs on a path where the harness has just
/// failed, so it often cannot answer. Only a table that *does* answer, and does not list the
/// provider, convicts it; anything else is treated as known, leaving the generic attach-failure
/// wording to explain what actually happened.
async fn provider_is_known(
    state: &AppState,
    project_config: &ProjectConfig,
    config: Option<&Config>,
    provider: &str,
) -> bool {
    if config.is_some_and(|config| config.providers.contains_key(provider)) {
        return true;
    }
    // Bounded, because asking the harness costs more than the answer is worth here. Both registry
    // calls go through `get_or_create_harness`, which will *spawn* one — on a path that exists
    // precisely because a harness just failed, where a spawn that hangs would hold up opening the
    // thread. All this buys is a more precise warning, so it gets a short deadline and gives up.
    const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
    match tokio::time::timeout(
        PROBE_TIMEOUT,
        harness_knows_provider(state, project_config, provider),
    )
    .await
    {
        Ok(known) => known,
        Err(_) => {
            warn!(
                project_id = %project_config.id,
                harness = %project_config.harness,
                %provider,
                action = "provider_is_known",
                timeout_ms = PROBE_TIMEOUT.as_millis() as u64,
                "harness did not answer in time; treating the provider as known"
            );
            true
        }
    }
}

/// The harness half of [`provider_is_known`]: does its provider table list this id?
///
/// Every failure answers "known". This runs to decide whether a warning may accuse the provider,
/// and an accusation made on a failed lookup is worse than a vague message — but the failure is
/// logged, because this is exactly when an operator needs to tell a capabilities failure from a
/// listing failure from a table that answered and omitted the provider.
async fn harness_knows_provider(
    state: &AppState,
    project_config: &ProjectConfig,
    provider: &str,
) -> bool {
    // The capability gate matters as much as the call: a harness that does not support provider
    // listing may still answer with an empty table, and taking that as gospel would convict every
    // provider — the very accusation this function exists to stop making.
    match state.registry.capabilities(project_config).await {
        Ok(caps) if !caps.provider_listing => return true,
        Err(error) => {
            warn!(
                project_id = %project_config.id,
                harness = %project_config.harness,
                %provider,
                %error,
                action = "provider_is_known",
                "cannot read harness capabilities; treating the provider as known"
            );
            return true;
        }
        Ok(_) => {}
    }
    match state.registry.list_providers(project_config).await {
        Ok(table) => table.iter().any(|known| known.id == provider),
        Err(error) => {
            warn!(
                project_id = %project_config.id,
                harness = %project_config.harness,
                %provider,
                %error,
                action = "provider_is_known",
                "harness provider listing failed; treating the provider as known"
            );
            true
        }
    }
}

/// Build the non-fatal warning shown when a thread loads read-only because its harness could not
/// attach: history stays viewable and the model picker is unlocked so the user can reactivate the
/// thread under a configured provider (PS1). Shared by the HTTP `open_thread` handler and the
/// WebSocket subscribe path so both surface the same `thread_read_only` code. The wording is
/// action-first and only claims the provider is missing when config verifiably lacks it — attach
/// failures can also be auth or spawn problems.
fn read_only_info(
    context: Option<&ReadOnlyProviderContext>,
    detail: Option<String>,
    thread_id: ThreadId,
    action: &str,
) -> ErrorInfo {
    let message = match context {
        Some(ctx) if !ctx.configured => format!(
            "Read-only: provider \"{}\" is no longer configured, so this thread's agent could \
             not be started. Pick a model from a configured provider to reactivate the thread.",
            ctx.provider
        ),
        Some(ctx) => format!(
            "Read-only: this thread's agent (provider \"{}\") could not be started. Pick a model \
             from a configured provider to reactivate the thread.",
            ctx.provider
        ),
        None => "Read-only: this thread's agent could not be started. Pick a model from a \
                 configured provider to reactivate the thread."
            .into(),
    };
    ErrorInfo {
        code: "thread_read_only".into(),
        severity: ErrorSeverity::Warning,
        message,
        detail,
        thread_id: Some(thread_id),
        action: Some(action.to_string()),
        request_id: None,
        process_id: None,
    }
}

async fn read_only_warning(
    state: &AppState,
    project_id: ProjectId,
    attach_error: &WsError,
    thread_id: ThreadId,
) -> ErrorInfo {
    let context = read_only_provider_context(state, project_id, thread_id).await;
    let detail = attach_error
        .info
        .detail
        .clone()
        .or_else(|| Some(attach_error.info.message.clone()));
    read_only_info(context.as_ref(), detail, thread_id, "subscribe")
}

/// Switch a **cold** thread to a different provider via a verified native re-resume (spec PS1).
///
/// Calls `thread/resume` with the requested model/provider and requires the harness to confirm
/// them as effective before the caller persists anything: Codex answers JSON-RPC success even
/// when it ignores resume overrides (loaded-thread rejoin), so success alone proves nothing. On
/// an unconfirmed switch the fresh registry binding is dropped again and a structured
/// `thread_provider_switch_ignored` error is returned, leaving persisted state untouched.
///
/// Returns the harness's non-fatal open warning (e.g. `codex_resume_failed` when Codex lost the
/// native context and started a fresh session under the new provider) for the caller to forward.
async fn switch_provider_cold(
    state: &AppState,
    project_id: ProjectId,
    thread_id: ThreadId,
    requested: &ModelRef,
) -> Result<Option<ErrorInfo>, WsError> {
    let project_config = state
        .store
        .load_project(project_id)
        .await
        .map_err(|e| WsError::from_persist(e, "select_model", Some(thread_id)))?
        .ok_or_else(|| {
            WsError::new(
                "project_not_found",
                ErrorSeverity::Error,
                "Project not found.",
            )
            .thread(thread_id)
            .action("select_model")
        })?;
    let thread_file = state
        .store
        .load_thread(project_id, thread_id)
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
    let ws_root = thread_workspace_root(state, &project_config, &thread_file)
        .await
        .map_err(|e| {
            WsError::new("workspace_unavailable", ErrorSeverity::Error, e.to_string())
                .thread(thread_id)
                .action("select_model")
        })?;

    info!(
        %project_id,
        %thread_id,
        harness_thread_id = %thread_file.harness_thread_id,
        from_provider = %thread_file.current_model.provider,
        to_provider = %requested.provider,
        to_model = %requested.model,
        "attempting verified cold-resume provider switch"
    );

    let handle = if thread_file.kind == ThreadKind::Subagent {
        state
            .registry
            .open_linked_thread(
                &project_config,
                &ws_root,
                Some(thread_id),
                thread_file.harness_thread_id.clone(),
                requested.clone(),
            )
            .await
    } else {
        state
            .registry
            .open_thread(
                &project_config,
                &ws_root,
                Some(thread_id),
                Some(thread_file.harness_thread_id.clone()),
                Some(requested.clone()),
            )
            .await
    }
    .map_err(|e| WsError::from_harness(e, "select_model", Some(thread_id)))?;

    let confirmed = handle.resumed_model.as_ref().is_some_and(|effective| {
        effective.provider == requested.provider && effective.model == requested.model
    });
    if !confirmed {
        // Unwind: drop the just-created binding so the thread returns to cold instead of staying
        // bound under an unverified model.
        state.registry.forget_thread(thread_id).await;
        let effective = handle
            .resumed_model
            .as_ref()
            .map(|m| format!("{}/{}", m.provider, m.model))
            .unwrap_or_else(|| "unreported".into());
        warn!(
            %project_id,
            %thread_id,
            requested = %format!("{}/{}", requested.provider, requested.model),
            %effective,
            "harness did not confirm provider switch; keeping old binding"
        );
        return Err(WsError::new(
            "thread_provider_switch_ignored",
            ErrorSeverity::Error,
            "The agent did not apply the provider switch. Retry after restarting the server, or \
             create a new thread with the selected provider.",
        )
        .detail(format!(
            "requested {}/{}, harness reported {effective}",
            requested.provider, requested.model
        ))
        .thread(thread_id)
        .action("select_model"));
    }

    // The C5 fallback (native context lost ⇒ fresh Codex session) yields a new native id; keep
    // the persisted mapping in sync exactly like the normal open path does.
    if handle.harness_thread_id != thread_file.harness_thread_id {
        state
            .thread_metadata
            .mutate(project_id, thread_id, |tf| {
                tf.harness_thread_id = handle.harness_thread_id.clone();
            })
            .await
            .map_err(|e| WsError::from_persist(e, "select_model", Some(thread_id)))?;
    }

    Ok(handle.warning.as_ref().map(|warning| {
        warning_info(
            warning.code.clone(),
            warning.message.clone(),
            warning.detail.clone(),
            thread_id,
            "select_model",
        )
    }))
}

async fn ensure_provider_change_allowed(
    state: &AppState,
    project_id: ProjectId,
    thread_id: ThreadId,
    selected_model: &ModelRef,
    action: &str,
) -> Result<(), WsError> {
    let Some(native_model) = state.registry.get_thread_native_model(thread_id).await else {
        return Ok(());
    };
    if native_model.provider == selected_model.provider {
        return Ok(());
    }

    let turns = state
        .store
        .load_all_turns(project_id, thread_id)
        .await
        .map_err(|e| WsError::from_persist(e, action, Some(thread_id)))?;
    let active = state.registry.thread_has_active_turn(thread_id).await;
    warn!(
        %project_id,
        %thread_id,
        native_provider = %native_model.provider,
        selected_provider = %selected_model.provider,
        active,
        turn_count = turns.len(),
        %action,
        "rejecting provider change on provider-bound Codex thread"
    );
    Err(provider_locked_error(
        thread_id,
        action,
        &native_model.provider,
        &selected_model.provider,
    ))
}

async fn ensure_send_harness_provider_current(
    state: &AppState,
    project_id: ProjectId,
    thread_id: ThreadId,
    tf: ThreadFile,
) -> Result<ThreadFile, WsError> {
    let Some(native_model) = state.registry.get_thread_native_model(thread_id).await else {
        return Ok(tf);
    };
    if native_model.provider == tf.current_model.provider {
        return Ok(tf);
    }

    let turns = state
        .store
        .load_all_turns(project_id, thread_id)
        .await
        .map_err(|e| WsError::from_persist(e, "send_input", Some(thread_id)))?;
    let active = state.registry.thread_has_active_turn(thread_id).await;
    warn!(
        %project_id,
        %thread_id,
        native_provider = %native_model.provider,
        selected_provider = %tf.current_model.provider,
        selected_model = %tf.current_model.model,
        active,
        turn_count = turns.len(),
        "rejecting persisted provider mismatch on provider-bound Codex thread"
    );
    Err(provider_locked_error(
        thread_id,
        "send_input",
        &native_model.provider,
        &tf.current_model.provider,
    ))
}

fn provider_locked_error(
    thread_id: ThreadId,
    action: &str,
    native_provider: &str,
    selected_provider: &str,
) -> WsError {
    WsError::new(
        "thread_provider_locked",
        ErrorSeverity::Error,
        "This Codex thread is bound to a different provider. Create a new thread to use the selected provider.",
    )
    .detail(format!(
        "native provider: {native_provider}; selected provider: {selected_provider}"
    ))
    .thread(thread_id)
    .action(action)
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
    // Through the same resolver the file endpoints use, rather than straight from the project. A
    // plan is written into the workspace the thread works in, and this was the last place that held
    // a thread and then asked the project where to put its file.
    let workspace_root = thread_workspace(state, project_id, thread_id)
        .await
        .map_err(|e| format!("could not resolve the thread's workspace: {e}"))?;

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
    Unavailable(String),
    Internal(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::NotFound => write!(f, "not found"),
            ApiError::BadRequest(msg)
            | ApiError::Forbidden(msg)
            | ApiError::Conflict(msg)
            | ApiError::Unavailable(msg)
            | ApiError::Internal(msg) => f.write_str(msg),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, msg, level) = match self {
            ApiError::NotFound => (
                axum::http::StatusCode::NOT_FOUND,
                "not found".into(),
                ApiErrorLogLevel::Debug,
            ),
            ApiError::BadRequest(msg) => (
                axum::http::StatusCode::BAD_REQUEST,
                msg,
                ApiErrorLogLevel::Debug,
            ),
            ApiError::Forbidden(msg) => (
                axum::http::StatusCode::FORBIDDEN,
                msg,
                ApiErrorLogLevel::Debug,
            ),
            ApiError::Conflict(msg) => (
                axum::http::StatusCode::CONFLICT,
                msg,
                ApiErrorLogLevel::Warn,
            ),
            ApiError::Unavailable(msg) => (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                msg,
                ApiErrorLogLevel::Warn,
            ),
            ApiError::Internal(msg) => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                msg,
                ApiErrorLogLevel::Error,
            ),
        };
        log_api_error(status, &msg, level);
        (status, msg).into_response()
    }
}

#[derive(Clone, Copy)]
enum ApiErrorLogLevel {
    Debug,
    Warn,
    Error,
}

fn log_api_error(status: axum::http::StatusCode, message: &str, level: ApiErrorLogLevel) {
    match level {
        ApiErrorLogLevel::Debug => {
            debug!(status = %status.as_u16(), message, "HTTP handler returned client error");
        }
        ApiErrorLogLevel::Warn => {
            warn!(status = %status.as_u16(), message, "HTTP handler returned conflict");
        }
        ApiErrorLogLevel::Error => {
            error!(status = %status.as_u16(), message, "HTTP handler returned internal error");
        }
    }
}

impl From<giskard_core::PersistError> for ApiError {
    fn from(e: giskard_core::PersistError) -> Self {
        ApiError::Internal(e.to_string())
    }
}
