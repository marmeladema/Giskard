use std::fmt;
use std::time::{Duration, Instant};

use axum::{
    extract::{
        Query, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    },
    response::Json,
};
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use giskard_core::error::{HarnessError, PersistError};
use giskard_core::ids::{ProjectId, ThreadId};
use giskard_core::model::ModelRef;
use giskard_core::thread::ThreadKind;
use giskard_core::turn::{TurnMode, TurnModel, TurnOverrides};
use giskard_core::user_input::UserInput;
use giskard_persist::store::{ProjectConfig, ThreadFile, ThreadRecency};
use giskard_proto::*;
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use crate::AppState;
use crate::auth::{TokenPurpose, get_session_token_from_header, sign_token, verify_token};
use crate::hub::Outbound;
use crate::log_fields::display_opt;
use crate::routes::{
    ApiError, ReadOnlyProviderContext, UI_VERSION, history_limit_or_default, load_thread,
    normalize_persisted_thread_model, project_model_catalog, provider_is_known, read_only_info,
    thread_workspace, validate_user_attachments, warning_info,
};
use crate::thread_graph::effective_thread_workspace_root as effective_workspace_root;

const HARNESS_CONTROL_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_WS_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Mint a short-lived WebSocket ticket. Tickets travel in the `/api/ws` query string (which can
/// land in reverse-proxy access logs), so they are domain-separated from session cookies: a
/// leaked ticket is only good for a WebSocket upgrade, and only for 60 seconds.
pub(crate) async fn ws_ticket(
    State(state): State<AppState>,
) -> Result<Json<WsTicketResponse>, ApiError> {
    let expiry = (Utc::now().timestamp() as u64) + 60;
    let ticket = sign_token(TokenPurpose::WsTicket, expiry, &state.session_key)
        .map_err(|e| ApiError::Internal(format!("failed to sign websocket ticket: {e}")))?;
    Ok(Json(WsTicketResponse {
        ticket,
        ui_version: UI_VERSION.to_owned(),
    }))
}

#[derive(Deserialize)]
pub(crate) struct WsQuery {
    ticket: Option<String>,
}

pub(crate) async fn ws_handler(
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
            HarnessError::ThreadReadOnly { .. } => {
                ("thread_read_only", "Agent-owned threads are read-only.")
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
    let overview = state.registry.runtime_overview();
    debug!(
        %client_id,
        revision = overview.revision,
        threads = overview.threads.len(),
        "sending authoritative runtime overview to connecting client"
    );
    let _ = tx
        .send(ServerMessage::ThreadRuntimeOverview(overview))
        .await;
}

async fn handle_ws(socket: WebSocket, state: AppState) {
    const SHUTDOWN_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
    const SHUTDOWN_WRITER_GRACE: Duration = Duration::from_millis(100);

    let (tx, mut rx) = mpsc::channel::<ServerMessage>(256);
    let client_id = state.hub.next_client_id();

    let (mut ws_sender, mut ws_receiver) = socket.split();

    let replacements = state.hub.register_client(client_id, tx.clone()).await;
    let (writer_done_tx, mut writer_done_rx) = oneshot::channel();
    let writer_shutdown = state.shutdown.clone();

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
    let mut send_task = tokio::spawn(async move {
        loop {
            // Replacement state bypasses the ordered FIFO. It stays latest-by-key until the
            // socket writer actually selects it, so obsolete metadata cannot consume event
            // capacity or make a domain producer wait on a slow peer.
            let msg = tokio::select! {
                () = writer_shutdown.wait() => {
                    let close = Message::Close(Some(CloseFrame {
                        code: close_code::AWAY,
                        reason: "server shutting down".into(),
                    }));
                    match tokio::time::timeout(SHUTDOWN_CLOSE_TIMEOUT, ws_sender.send(close)).await {
                        Ok(Ok(())) => debug!(
                            %client_id,
                            action = "close_ws_for_shutdown",
                            "sent WebSocket shutdown close frame"
                        ),
                        Ok(Err(error)) => debug!(
                            %client_id,
                            action = "close_ws_for_shutdown",
                            %error,
                            "could not send WebSocket shutdown close frame"
                        ),
                        Err(_) => warn!(
                            %client_id,
                            action = "close_ws_for_shutdown",
                            timeout_ms = SHUTDOWN_CLOSE_TIMEOUT.as_millis(),
                            "timed out sending WebSocket shutdown close frame"
                        ),
                    }
                    break;
                }
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
    let receiver_shutdown = state.shutdown.clone();
    let mut shutting_down = false;

    loop {
        let incoming = tokio::select! {
            () = receiver_shutdown.wait() => {
                shutting_down = true;
                debug!(
                    %client_id,
                    action = "stop_ws_receiver_for_shutdown",
                    "stopping WebSocket receive loop for server shutdown"
                );
                break;
            }
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
                        warn!(
                            %client_id,
                            action = "parse_ws_message",
                            error = %e,
                            "invalid WebSocket message"
                        );
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
                        %client_id,
                        code = %e.info.code,
                        severity = ?e.info.severity,
                        thread_id = display_opt(e.info.thread_id),
                        request_id = display_opt(e.info.request_id.as_deref()),
                        action = display_opt(e.info.action.as_deref()),
                        detail = display_opt(e.info.detail.as_deref()),
                        "WS handler error: {}",
                        e.info.message
                    );
                    let _ = tx.send(e.into_server_message()).await;
                }
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                warn!(
                    %client_id,
                    action = "read_ws_message",
                    error = %e,
                    "WebSocket receive error"
                );
                break;
            }
            None => break,
        }
    }

    hub.disconnect(client_id).await;
    if shutting_down {
        let writer_wait = SHUTDOWN_CLOSE_TIMEOUT + SHUTDOWN_WRITER_GRACE;
        let _ = tokio::time::timeout(writer_wait, &mut send_task).await;
    }
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
                        detail = display_opt(attach_error.info.detail.as_deref()),
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

            // Initial/reconnect history remains a temporary bootstrap-only delta. Only older-page
            // pagination is fetched over HTTP and kept out of the ordered socket lane.
            //
            // * Resync (`since` present): history-first ordering. Send the persisted history — a
            //   `HistoryDelta` of the turns after the cursor when we can resolve it, or a bounded
            //   reset delta when we can't (stale cursor) — *before* the live turn and tasks. The
            //   client reconciles or rebuilds the transcript while it still owns it, then the live
            //   turn appends on top. The browser may keep a stale live DOM block visible until the
            //   replacement snapshot arrives, so delta rows still need to be inserted before that
            //   retained live block on the UI side.
            let history_started_at = Instant::now();
            let resync_delta = match since {
                Some(cursor) => state
                    .store
                    .load_turns_after(project_id, thread_id, cursor)
                    .await
                    .map_err(|e| WsError::from_persist(e, "subscribe_resync", Some(thread_id)))?,
                None => None,
            };

            let history_message = if let Some(turns) = resync_delta {
                Some(ServerMessage::HistoryDelta {
                    thread_id,
                    turns: turns.into_iter().map(Into::into).collect(),
                    reset: false,
                    has_more: None,
                })
            } else {
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
                Some(ServerMessage::HistoryDelta {
                    thread_id,
                    turns: turns.into_iter().map(Into::into).collect(),
                    reset: true,
                    has_more: Some(has_more),
                })
            };

            // The live turn (H5) isn't in the JSONL yet — reconstruct it from the live buffer — and
            // its running tasks. Bootstrap history goes first so a reset rebuilds completed rows
            // before the live snapshot appends its in-flight rows.
            debug!(
                %project_id,
                %thread_id,
                action = "subscribe_history",
                incremental = since.is_some(),
                elapsed_ms = history_started_at.elapsed().as_millis(),
                "loaded subscription history"
            );

            let live_snapshot_started_at = Instant::now();
            let runtime = state.registry.thread_runtime(thread_id).await;
            let live_snapshot = runtime.as_ref().and_then(|runtime| runtime.live_snapshot());
            debug!(
                %project_id,
                %thread_id,
                action = "build_live_snapshot",
                accumulated_events = live_snapshot
                    .as_ref()
                    .map_or(0, |snapshot| snapshot.accumulated.len()),
                elapsed_ms = live_snapshot_started_at.elapsed().as_millis(),
                "built subscription live snapshot"
            );
            let (revision, tasks) = runtime
                .as_ref()
                .map_or((0, Vec::new()), |runtime| runtime.tasks_snapshot());
            let running_tasks = ServerMessage::RunningTasks {
                thread_id,
                revision,
                tasks,
            };
            if let Some(history_message) = history_message {
                let _ = tx.send(history_message).await;
            }
            if let Some(snap) = live_snapshot {
                let _ = tx.send(ServerMessage::LiveTurnSnapshot(snap)).await;
            }
            let _ = tx.send(running_tasks).await;
            for request in runtime
                .as_ref()
                .map_or_else(Vec::new, |runtime| runtime.request_states())
            {
                let _ = tx.send(ServerMessage::RequestState(request)).await;
            }
        }
        ClientMessage::Unsubscribe { thread_id } => {
            state.hub.unsubscribe(thread_id, client_id).await;
        }
        ClientMessage::SendInput {
            thread_id,
            text,
            attachments,
        } => {
            let project_id = project_for_readonly(state, thread_id, "send_input").await?;
            state
                .registry
                .ensure_thread_writable(project_id, thread_id)
                .await
                .map_err(|error| WsError::from_harness(error, "send_input", Some(thread_id)))?;
            // Only a writable primary may cause a cold harness attach.
            project_for(state, thread_id, "send_input").await?;
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
                    let Some(current_model) = tf.current_model.as_known() else {
                        return;
                    };
                    let normalized =
                        crate::models::normalize_model_ref(&app_config, &catalog, current_model);
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
                    tf.current_model = TurnModel::Known(normalized);
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
            let effective_model = tf.current_model.as_known().cloned().ok_or_else(|| {
                WsError::new(
                    "thread_metadata_invalid",
                    ErrorSeverity::Error,
                    "This primary thread has no authoritative model.",
                )
                .thread(thread_id)
                .action("send_input")
            })?;
            let effective_mode = tf.mode.as_known().ok_or_else(|| {
                WsError::new(
                    "thread_metadata_invalid",
                    ErrorSeverity::Error,
                    "This primary thread has no authoritative mode.",
                )
                .thread(thread_id)
                .action("send_input")
            })?;

            // Resolved snapshot the harness applies to `turn/start` (§7.5, §8.4/§8.5):
            //  - the thread's current model (carrying its reasoning effort), so a mid-thread
            //    model/effort change actually reaches the agent. Passing `None` here would leave
            //    Codex on whatever model was set at `thread/start`.
            //  - the thread's persisted permission preset (§9).
            let overrides = TurnOverrides {
                model: Some(effective_model.clone()),
                mode: effective_mode,
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
            let project_id = project_for_readonly(state, thread_id, "switch_mode").await?;
            state
                .registry
                .ensure_thread_writable(project_id, thread_id)
                .await
                .map_err(|error| WsError::from_harness(error, "switch_mode", Some(thread_id)))?;
            let tf = state
                .thread_metadata
                .mutate_with_recency(project_id, thread_id, ThreadRecency::TouchIfChanged, |tf| {
                    tf.mode = TurnMode::Known(mode)
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
            state
                .registry
                .ensure_thread_writable(project_id, thread_id)
                .await
                .map_err(|error| WsError::from_harness(error, "select_model", Some(thread_id)))?;
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

            let native_model = state
                .registry
                .loaded_thread_binding(thread_id)
                .await
                .and_then(|binding| binding.native_model().cloned());
            if let Some(native_model) = native_model.as_ref() {
                // Warm thread: the provider is bound to the loaded Codex session (PB2) —
                // cross-provider changes stay rejected because a loaded thread can silently
                // ignore resume overrides.
                ensure_provider_change_allowed(
                    state,
                    project_id,
                    thread_id,
                    native_model,
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
                let stored_model = state
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
                    .current_model;
                let stored_provider = stored_model.as_known().ok_or_else(|| {
                    WsError::new(
                        "thread_metadata_invalid",
                        ErrorSeverity::Error,
                        "This primary thread has no authoritative model.",
                    )
                    .thread(thread_id)
                    .action("select_model")
                })?;
                if stored_provider.provider != model_ref.provider
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
                        let current_model = tf.current_model.as_known().cloned();
                        if let Some(current_model) = current_model.as_ref() {
                            let old = crate::models::resolve_catalog_descriptor(
                                &catalog,
                                &config,
                                current_model,
                            );
                            if old.supports_reasoning_effort
                                && let Some(effort) = current_model.reasoning_effort.clone()
                            {
                                tf.model_efforts.insert(current_model.key(), effort);
                            }
                        }

                        let new_descriptor = crate::models::resolve_catalog_descriptor(
                            &catalog, &config, &model_ref,
                        );
                        let mut new_model = model_ref.clone();
                        let same_model = current_model.as_ref().is_some_and(|current| {
                            current.provider == new_model.provider
                                && current.model == new_model.model
                        });
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
                        tf.current_model = TurnModel::Known(new_model);
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
            let project_id =
                project_for_readonly(state, thread_id, "set_permission_preset").await?;
            state
                .registry
                .ensure_thread_writable(project_id, thread_id)
                .await
                .map_err(|error| {
                    WsError::from_harness(error, "set_permission_preset", Some(thread_id))
                })?;
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
            thread_id,
            request_id,
            decision,
        } => {
            let request_id_for_broadcast = request_id.clone();
            let approval_id = giskard_core::ids::ApprovalId(request_id);
            let response_started = Instant::now();
            let response_result = tokio::time::timeout(
                HARNESS_CONTROL_TIMEOUT,
                state
                    .registry
                    .respond_approval(thread_id, approval_id.clone(), decision.clone()),
            )
            .await;
            match response_result {
                Ok(result) => {
                    result.map_err(|e| {
                        WsError::from_harness(e, "approval_decision", Some(thread_id))
                    })?;
                }
                Err(_) => {
                    error!(
                        %thread_id,
                        request_id = %request_id_for_broadcast,
                        timeout_ms = HARNESS_CONTROL_TIMEOUT.as_millis(),
                        elapsed_ms = response_started.elapsed().as_millis(),
                        "approval decision timed out waiting for Codex"
                    );
                    // Cancelling the registry future drops its claim and rolls Responding back to
                    // Pending. Republish the rollback so every tab becomes actionable again.
                    state
                        .registry
                        .republish_approval_request_state(thread_id, approval_id)
                        .await;
                    return Err(WsError::from_harness(
                        HarnessError::Timeout(
                            "approval decision timed out waiting for Codex".into(),
                        ),
                        "approval_decision",
                        Some(thread_id),
                    ));
                }
            };
            // `respond_approval` owns both halves of the resolution: it records the answer against
            // the in-flight turn for reconnect and publishes the revisioned `RequestState`. A
            // second broadcast here would give the browser two authorities for one request, only
            // one of which a client can gate on.
        }
        ClientMessage::ServerRequestResponse {
            thread_id,
            request_id,
            response,
        } => {
            let request_id_for_log = request_id.clone();
            let req_id = giskard_core::ids::ServerRequestId(request_id);
            let response_started = Instant::now();
            let response_result = tokio::time::timeout(
                HARNESS_CONTROL_TIMEOUT,
                state
                    .registry
                    .respond_server_request(thread_id, req_id.clone(), response),
            )
            .await;
            match response_result {
                Ok(result) => {
                    result.map_err(|e| {
                        WsError::from_harness(e, "server_request_response", Some(thread_id))
                    })?;
                }
                Err(_) => {
                    error!(
                        %thread_id,
                        request_id = %request_id_for_log,
                        timeout_ms = HARNESS_CONTROL_TIMEOUT.as_millis(),
                        elapsed_ms = response_started.elapsed().as_millis(),
                        "server request response timed out waiting for Codex"
                    );
                    // Cancelling the registry future drops its claim and rolls Responding back to
                    // Pending. Publish that authoritative rollback so peer tabs do not remain
                    // disabled after the claimant receives its timeout error.
                    state
                        .registry
                        .republish_server_request_state(thread_id, req_id.clone())
                        .await;
                    return Err(WsError::from_harness(
                        HarnessError::Timeout(
                            "server request response timed out waiting for Codex".into(),
                        ),
                        "server_request_response",
                        Some(thread_id),
                    ));
                }
            };
            // `respond_server_request` records the answer against the in-flight turn before it
            // publishes the resolution, so there is nothing left to do here.
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
            let project_id = project_for_readonly(state, thread_id, "compact_context").await?;
            state
                .registry
                .ensure_thread_writable(project_id, thread_id)
                .await
                .map_err(|error| {
                    WsError::from_harness(error, "compact_context", Some(thread_id))
                })?;
            // Only a writable primary may cause a cold harness attach.
            project_for(state, thread_id, "compact_context").await?;
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
                state.registry.compact_thread(
                    thread_id,
                    tf.current_model.as_known().cloned().ok_or_else(|| {
                        WsError::new(
                            "thread_metadata_invalid",
                            ErrorSeverity::Error,
                            "This primary thread has no authoritative model.",
                        )
                        .thread(thread_id)
                        .action("compact_context")
                    })?,
                    tf.mode.as_known().ok_or_else(|| {
                        WsError::new(
                            "thread_metadata_invalid",
                            ErrorSeverity::Error,
                            "This primary thread has no authoritative mode.",
                        )
                        .thread(thread_id)
                        .action("compact_context")
                    })?,
                ),
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
            let runtime = state
                .registry
                .thread_runtime(thread_id)
                .await
                .ok_or_else(|| {
                    WsError::from_harness(
                        HarnessError::ThreadNotFound(thread_id),
                        "terminate_command",
                        Some(thread_id),
                    )
                })?;
            let existing_command = runtime.task_by_process(&process_id_for_state);
            if runtime.set_task_terminating(&process_id_for_state, true) {
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
                    let removed = runtime.remove_task_by_process(&process_id_for_state);
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

                if runtime.set_task_terminating(&process_id_for_state, false) {
                    broadcast_running_commands(state, thread_id).await;
                }
                return Err(
                    WsError::from_harness(error, "terminate_command", Some(thread_id))
                        .process_id(process_id_for_state),
                );
            }
        }
        ClientMessage::SavePlan { thread_id, path } => {
            let project_id = project_for_readonly(state, thread_id, "save_plan").await?;
            state
                .registry
                .ensure_thread_writable(project_id, thread_id)
                .await
                .map_err(|error| WsError::from_harness(error, "save_plan", Some(thread_id)))?;
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
    if let Some(binding) = state.registry.loaded_thread_binding(thread_id).await {
        return Ok(ThreadAccess {
            project_id: binding.project_id(),
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
    let ws_root = effective_workspace_root(&state.store, &project_config, &persisted_thread)
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
    if thread_file.kind == ThreadKind::Orphan {
        return Err(WsError::new(
            "thread_not_found",
            ErrorSeverity::Error,
            "Thread not found.",
        )
        .thread(thread_id)
        .action(action));
    }
    let current_model = thread_file.current_model.as_known().cloned();
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
            .attach_subagent_thread(&project_config, &thread_file)
            .await
    } else {
        let current_model = current_model.ok_or_else(|| {
            WsError::new(
                "thread_metadata_invalid",
                ErrorSeverity::Error,
                "Primary thread has no authoritative model.",
            )
            .thread(thread_id)
            .action(action)
        })?;
        state
            .registry
            .open_thread(
                &project_config,
                &ws_root,
                thread_id,
                Some(thread_file.harness_thread_id.clone()),
                current_model,
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
            .publish(thread_id, Outbound::Error(warning.clone()))
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
/// Subscription reads use this so a thread whose provider was removed from config
/// stays viewable even though its harness can never re-attach. Prefers an already-open thread's
/// registered project, then falls back to scanning persistence.
async fn project_for_readonly(
    state: &AppState,
    thread_id: ThreadId,
    action: &str,
) -> Result<ProjectId, WsError> {
    if let Some(binding) = state.registry.loaded_thread_binding(thread_id).await {
        let project_id = binding.project_id();
        let visible = state
            .store
            .load_thread(project_id, thread_id)
            .await
            .map_err(|e| WsError::from_persist(e, action, Some(thread_id)))?
            .is_some_and(|thread| thread.kind != ThreadKind::Orphan);
        if visible {
            return Ok(project_id);
        }
        return Err(WsError::new(
            "thread_not_found",
            ErrorSeverity::Error,
            "Thread not found.",
        )
        .thread(thread_id)
        .action(action));
    }
    match find_persisted_thread(state, thread_id, action).await? {
        Some((_, thread_file)) if thread_file.kind == ThreadKind::Orphan => Err(WsError::new(
            "thread_not_found",
            ErrorSeverity::Error,
            "Thread not found.",
        )
        .thread(thread_id)
        .action(action)),
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
        .into_known()?
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
    let ws_root = effective_workspace_root(&state.store, &project_config, &thread_file)
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
        from_model = ?thread_file.current_model,
        to_provider = %requested.provider,
        to_model = %requested.model,
        "attempting verified cold-resume provider switch"
    );

    let handle = state
        .registry
        .open_thread(
            &project_config,
            &ws_root,
            thread_id,
            Some(thread_file.harness_thread_id.clone()),
            requested.clone(),
        )
        .await
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
    native_model: &ModelRef,
    selected_model: &ModelRef,
    action: &str,
) -> Result<(), WsError> {
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
    let Some(binding) = state.registry.loaded_thread_binding(thread_id).await else {
        return Ok(tf);
    };
    let Some(native_model) = binding.native_model() else {
        return Ok(tf);
    };
    let selected_model = tf.current_model.as_known().ok_or_else(|| {
        WsError::new(
            "thread_metadata_invalid",
            ErrorSeverity::Error,
            "This primary thread has no authoritative model.",
        )
        .thread(thread_id)
        .action("send_input")
    })?;
    if native_model.provider == selected_model.provider {
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
        selected_provider = %selected_model.provider,
        selected_model = %selected_model.model,
        active,
        turn_count = turns.len(),
        "rejecting persisted provider mismatch on provider-bound Codex thread"
    );
    Err(provider_locked_error(
        thread_id,
        "send_input",
        &native_model.provider,
        &selected_model.provider,
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
async fn broadcast_running_commands(state: &AppState, thread_id: ThreadId) {
    let Some(runtime) = state.registry.thread_runtime(thread_id).await else {
        return;
    };
    let (revision, tasks) = runtime.tasks_snapshot();
    state
        .hub
        .publish(thread_id, Outbound::RunningTasks { revision, tasks })
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
