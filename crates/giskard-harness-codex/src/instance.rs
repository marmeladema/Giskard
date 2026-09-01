use super::*;
use crate::discovery::{DiscoveryAdmission, Submission};
use crate::native_routes::{
    ActiveRoute, CodexRouteAuthority, FreshRouteConflict, ReplaceRouteFailure,
};

/// One task-owned runtime for one Codex app-server process.
///
/// Exactly one instance is created for each spawned transport and moved into exactly one Tokio
/// task. Its mapper, active turns, pending compactions, and pending context restores never leave
/// that task; helper futures may borrow this state only through `&mut self`. No independent worker
/// may mutate protocol state.
///
/// This runtime serves every native thread on the process and is unrelated to a primary-thread or
/// sub-agent hierarchy.
pub(super) struct CodexInstance<C> {
    client: C,
    receivers: WorkerReceivers,
    routes: CodexRouteAuthority,
    discovery: DiscoveryAdmission,
    worker_queue: Arc<WorkerQueueWatchdog>,
    workspace_root: PathBuf,
    writable_roots: Vec<PathBuf>,
    mapper: CodexMapper,
    active_turns: ActiveTurns,
    pending_compactions: HashMap<ThreadId, PendingCompaction>,
    pending_context_restores: HashMap<NativeThreadId, PendingContextRestore>,
}

impl<C> CodexInstance<C> {
    pub(super) fn new(
        client: C,
        receivers: WorkerReceivers,
        routes: CodexRouteAuthority,
        discovery: DiscoveryChannels,
        worker_queue: Arc<WorkerQueueWatchdog>,
        workspace: (PathBuf, Vec<PathBuf>),
        bootstrap: HarnessBootstrap,
    ) -> Result<Self, HarnessError> {
        let mapper = CodexMapper::new(workspace.0.clone());
        let instance = Self {
            client,
            receivers,
            routes,
            discovery: DiscoveryAdmission::new(discovery.submissions),
            worker_queue,
            workspace_root: workspace.0,
            writable_roots: workspace.1,
            mapper,
            active_turns: HashMap::new(),
            pending_compactions: HashMap::new(),
            pending_context_restores: HashMap::new(),
        };
        for binding in bootstrap.known_threads {
            let route = instance
                .routes
                .bootstrap(binding.harness_thread_id, binding.thread_id)?;
            if route.thread_id() != binding.thread_id {
                return Err(HarnessError::Protocol(format!(
                    "bootstrap native route resolved to {} instead of {}",
                    route.thread_id(),
                    binding.thread_id
                )));
            }
        }
        Ok(instance)
    }

    fn tombstone_event_route(&mut self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        self.routes
            .tombstone(&thread.harness_thread_id, thread.thread)
    }

    fn ensure_discovered_route(&mut self, harness_thread_id: &str) -> Option<ActiveRoute> {
        let harness_thread_id = harness_thread_id.trim();
        if harness_thread_id.is_empty() {
            return None;
        }
        if !self.discovery.is_open() {
            return self
                .routes
                .resolve(
                    harness_thread_id,
                    fallback_thread(&self.mapper, &self.active_turns),
                )
                .ok();
        }
        let discovery = match self
            .routes
            .discover(harness_thread_id.to_owned(), ThreadId::new())
        {
            Ok(discovery) => discovery,
            Err(error) => {
                warn!(
                    native_thread_id = harness_thread_id,
                    error = %error,
                    "failed to establish a route for discovered Codex traffic"
                );
                return None;
            }
        };
        if let Some(ticket) = discovery.ticket {
            match self.discovery.submit(ticket) {
                Submission::Queued => {}
                Submission::Deferred(ticket) => {
                    if let Err(error) = ticket.defer() {
                        warn!(native_thread_id = harness_thread_id, %error, "failed to defer Codex route discovery");
                    }
                }
                Submission::Closed(ticket) => {
                    self.discovery_consumer_closed(ticket);
                    return None;
                }
            }
        }
        Some(discovery.route)
    }

    fn discovery_consumer_closed(&mut self, ticket: DiscoveryTicket) {
        warn!(
            thread_id = %ticket.thread_id(),
            native_thread_id = ticket.harness_thread_id(),
            "Codex thread discovery receiver closed; closing the route authority"
        );
        self.discovery.close_failed();
        self.routes.close();
        drop(ticket);
    }

    fn idle_discovery_consumer_closed(&mut self) {
        warn!("Codex thread discovery receiver closed while idle; closing the route authority");
        self.discovery.close_failed();
        self.routes.close();
    }
}

impl<C> CodexInstance<C>
where
    C: CodexTransport,
{
    pub(super) async fn run(mut self) {
        let mut first_event_warn_tick = tokio::time::interval(Duration::from_secs(1));

        loop {
            if self.discovery.failed() {
                break;
            }
            if let Err(error) = self.discovery.stage_pending(&self.routes) {
                error!(
                    error = %error,
                    "Codex route authority failed while staging discovery; stopping harness"
                );
                self.discovery.close_failed();
                break;
            }
            let discovery_tx = self.discovery.sender();
            let discovery_closure = self.discovery.closure_signal();
            let has_pending_discovery = self.discovery.has_pending();
            tokio::select! {
                biased;
                _ = wait_for_shutdown_request(&mut self.receivers.shutdown) => {
                    cleanup_all_active_turn_uploads(&mut self.client, &mut self.active_turns).await;
                    self.routes.close();
                    shutdown_codex_transport(self.client, &self.workspace_root).await;
                    self.worker_queue.close();
                    self.receivers.done.send_replace(true);
                    return;
                }
                permit = discovery_tx.reserve_owned(), if has_pending_discovery => {
                    match permit {
                        Ok(permit) => self.discovery.send_pending(permit),
                        Err(_) => {
                            if let Some(ticket) = self.discovery.take_pending() {
                                self.discovery_consumer_closed(ticket);
                            }
                        }
                    }
                }
                _ = discovery_closure.closed(), if self.discovery.is_open() => {
                    self.idle_discovery_consumer_closed();
                }
                msg = self.client.next_message(), if should_poll_codex_messages(&self.mapper, &self.active_turns, &self.pending_compactions) || !self.pending_context_restores.is_empty() => {
                    match msg {
                        Ok(Some(msg)) => {
                            observe_pending_context_restore(&mut self.pending_context_restores, &msg);
                            match self.handle_server_message(msg).await {
                                MessageOutcome::Handled => {}
                                MessageOutcome::CompactionCompleted { thread, elapsed_ms } => {
                                    info!(
                                        %thread,
                                        elapsed_ms,
                                        pending_compactions = self.pending_compactions.len(),
                                        "Codex context compaction completion observed"
                                    );
                                }
                            }
                        }
                        Ok(None) => {
                            cleanup_all_active_turn_uploads(&mut self.client, &mut self.active_turns).await;
                            emit_incomplete_active_turns(
                                &self.routes,
                                &mut self.mapper,
                                &mut self.active_turns,
                                "Codex stream ended before turn completion",
                            )
                            .await;
                            if !self.pending_compactions.is_empty() {
                                warn!(
                                    action = "read_codex_stream",
                                    workspace_root = %self.workspace_root.display(),
                                    pending_compactions = self.pending_compactions.len(),
                                    pending_compaction_states = ?pending_compaction_states(&self.pending_compactions),
                                    "Codex message stream ended with pending context compactions"
                                );
                            }
                            break;
                        }
                        Err(CodexStreamError::NonJsonStdout {
                            parse_error,
                            raw_preview,
                            raw_bytes,
                        }) => {
                            warn!(
                                active_turns = self.active_turns.len(),
                                pending_compactions = self.pending_compactions.len(),
                                pending_compaction_states = ?pending_compaction_states(&self.pending_compactions),
                                workspace_root = %self.workspace_root.display(),
                                error = %parse_error,
                                raw_bytes,
                                raw_preview = ?raw_preview,
                                "Ignoring non-JSON line from Codex app-server stdout"
                            );
                        }
                        Err(CodexStreamError::Fatal(e)) => {
                            let message = e.to_string();
                            if self.active_turns.is_empty() {
                                warn!(
                                    action = "read_codex_stream",
                                    error = %message,
                                    pending_compactions = self.pending_compactions.len(),
                                    pending_compaction_states = ?pending_compaction_states(&self.pending_compactions),
                                    workspace_root = %self.workspace_root.display(),
                                    "Codex idle stream failed while background work was running"
                                );
                            } else {
                                warn!(
                                    action = "read_codex_stream",
                                    error = %message,
                                    active_turns = self.active_turns.len(),
                                    active_turn_states = ?active_turn_states(&self.active_turns),
                                    pending_compactions = self.pending_compactions.len(),
                                    pending_compaction_states = ?pending_compaction_states(&self.pending_compactions),
                                    workspace_root = %self.workspace_root.display(),
                                    "Codex stream failed before all active turns completed"
                                );
                                cleanup_all_active_turn_uploads(&mut self.client, &mut self.active_turns).await;
                                emit_incomplete_active_turns(
                                    &self.routes,
                                    &mut self.mapper,
                                    &mut self.active_turns,
                                    format!("Codex stream failed before turn completion: {message}"),
                                )
                                .await;
                            }
                            break;
                        }
                    }
                }
                queued = self.receivers.commands.recv() => {
                    let queued = match queued {
                        Some(queued) => queued,
                        None => {
                            cleanup_all_active_turn_uploads(&mut self.client, &mut self.active_turns).await;
                            break;
                        }
                    };
                    self.worker_queue.mark_started(queued.token);
                    let token = queued.token;
                    self.handle_harness_command(queued).await;
                    self.worker_queue.mark_finished(token);
                }
                queued = self.receivers.controls.recv() => {
                    let Some(queued) = queued else {
                        cleanup_all_active_turn_uploads(&mut self.client, &mut self.active_turns).await;
                        break;
                    };
                    self.worker_queue.mark_started(queued.token);
                    let token = queued.token;
                    self.handle_control_command(queued.command).await;
                    self.worker_queue.mark_finished(token);
                }
                _ = first_event_warn_tick.tick(), if !self.active_turns.is_empty() => {
                    warn_slow_first_events(&mut self.active_turns);
                }
            }
        }
        self.routes.close();
        self.worker_queue.close();
        self.receivers.done.send_replace(true);
    }

    async fn handle_harness_command(&mut self, queued: QueuedHarnessCommand) {
        match queued.command {
            HarnessCommand::OpenThread { opts, response } => {
                let result = self.handle_open_thread(&opts).await;
                match result {
                    Ok(outcome) => {
                        let attachment = outcome.attachment;
                        let handle = attachment.handle();
                        if let Some(model) = outcome.resume_replay_model {
                            let replaced = self.pending_context_restores.insert(
                                NativeThreadId::new(handle.harness_thread_id.clone()),
                                PendingContextRestore {
                                    thread: handle.thread,
                                    model,
                                    sink: opts.updates.clone(),
                                },
                            );
                            if let Some(replaced) = replaced {
                                warn!(
                                    thread_id = %handle.thread,
                                    replaced_thread_id = %replaced.thread,
                                    harness_thread_id = %handle.harness_thread_id,
                                    "replaced an overlapping pending context restore"
                                );
                            }
                        }
                        let _ = response.send(Ok(attachment));
                    }
                    Err(error) => {
                        let _ = response.send(Err(error));
                    }
                }
            }
            HarnessCommand::StartTurn {
                thread,
                input,
                overrides,
                response,
            } => {
                match handle_start_turn(
                    &mut self.client,
                    &mut self.mapper,
                    &thread,
                    &input,
                    &overrides,
                    &self.writable_roots,
                )
                .await
                {
                    Ok(started) => {
                        let _ = response.send(Ok(started.turn));
                        self.active_turns.insert(
                            thread.thread,
                            ActiveTurn::new(*thread, started.turn)
                                .with_upload_dir(started.upload_dir),
                        );
                    }
                    Err(error) => {
                        let _ = response.send(Err(error));
                    }
                }
            }
        }
    }

    async fn handle_open_thread(
        &mut self,
        opts: &OpenThreadOptions,
    ) -> Result<OpenThreadOutcome, HarnessError> {
        let cwd = opts.workspace_root.to_string_lossy().to_string();
        let thread_id = opts.thread;

        // Track whether resume-by-id failed and we fell back to a fresh native thread (C5), so we
        // can warn the caller that agent context was lost while keeping the Giskard-side history.
        let mut resume_warning = None;

        let opened = if let Some(ref resume_id) = opts.resume {
            let context = CodexOperationContext::for_project("thread_resume", opts.project)
                .with_thread_id(thread_id)
                .with_harness_thread_id(resume_id);
            match resume_thread(
                &mut self.client,
                context,
                resume_id,
                &cwd,
                &opts.initial_model,
            )
            .await
            {
                Ok(opened) => opened,
                Err(e) => {
                    // C5: Codex thread store purged/rotated. Start fresh instead of hard-failing.
                    resume_warning = Some(HarnessNotice {
                        code: "codex_resume_failed".into(),
                        message: "Agent context was lost; started a fresh Codex session. History is intact."
                            .into(),
                        detail: Some(e.to_string()),
                    });
                    let context = CodexOperationContext::for_project(
                        "thread_start_after_resume_failed",
                        opts.project,
                    )
                    .with_thread_id(thread_id);
                    start_thread(&mut self.client, context, &cwd, &opts.initial_model).await?
                }
            }
        } else {
            let context = CodexOperationContext::for_project("thread_start", opts.project)
                .with_thread_id(thread_id);
            start_thread(&mut self.client, context, &cwd, &opts.initial_model).await?
        };

        let native = opened.harness_thread_id.clone();
        let warning_for_handle = resume_warning.clone();
        let model_for_handle = opened.model.clone();
        let agent_name = opened.agent_name.clone();
        let parent_native = opened.parent_harness_thread_id.clone();
        let workspace = opts.workspace_root.clone();
        let make_handle = move |authoritative_thread| ThreadHandle {
            warning: warning_for_handle,
            resumed_model: model_for_handle,
            agent_name,
            parent_harness_thread_id: parent_native,
            ..ThreadHandle::opened(authoritative_thread, native, workspace)
        };
        // A failed resume atomically replaces the exact old activation. Successful explicit
        // resume may reactivate its exact tombstone; a fresh open establishes a new route.
        let attachment = if let (Some(expected_harness_thread_id), Some(_)) =
            (&opts.resume, &resume_warning)
        {
            match self.routes.replace_fresh(
                expected_harness_thread_id.clone(),
                opened.harness_thread_id.clone(),
                thread_id,
                make_handle,
            ) {
                Ok(attachment) => attachment,
                Err(ReplaceRouteFailure::AuthoritativeNative {
                    thread: authoritative,
                }) => {
                    return Err(HarnessError::Protocol(format!(
                        "resume fallback returned native thread {} already owned by {authoritative}",
                        opened.harness_thread_id
                    )));
                }
                Err(ReplaceRouteFailure::NewProviderRoute(error)) => {
                    let detached =
                        ThreadHandle::detached(thread_id, opened.harness_thread_id.clone());
                    if let Err(cleanup_error) =
                        handle_delete_thread(&mut self.client, &detached).await
                    {
                        warn!(
                            thread_id = %thread_id,
                            native_thread_id = opened.harness_thread_id,
                            error = %cleanup_error,
                            "failed to delete newly created native thread after resume fallback replacement failed"
                        );
                    }
                    return Err(error);
                }
            }
        } else if opts.resume.is_some() {
            self.routes
                .resume(opened.harness_thread_id.clone(), thread_id, make_handle)?
        } else {
            match self.routes.claim_fresh(
                opened.harness_thread_id.clone(),
                thread_id,
                make_handle,
            )? {
                Ok(attachment) => attachment,
                Err(conflict @ FreshRouteConflict::AuthoritativeNative { .. }) => {
                    return Err(conflict.as_error(&opened.harness_thread_id, thread_id));
                }
                Err(conflict @ FreshRouteConflict::NewNativeForBoundThread { .. }) => {
                    let collision = conflict.as_error(&opened.harness_thread_id, thread_id);
                    let detached =
                        ThreadHandle::detached(thread_id, opened.harness_thread_id.clone());
                    if let Err(cleanup_error) =
                        handle_delete_thread(&mut self.client, &detached).await
                    {
                        warn!(
                            thread_id = %thread_id,
                            native_thread_id = opened.harness_thread_id,
                            error = %cleanup_error,
                            "failed to delete newly created native thread after route collision"
                        );
                    }
                    return Err(collision);
                }
            }
        };
        if attachment.handle().thread != thread_id {
            return Err(HarnessError::Protocol(format!(
                "opened native thread {} is already bound to {}, not {thread_id}",
                opened.harness_thread_id,
                attachment.handle().thread
            )));
        }
        let route = self.routes.active_for_thread(thread_id)?;
        self.routes.deliver(
            &route,
            AgentEvent::ThreadOpened {
                thread: thread_id,
                harness_thread_id: opened.harness_thread_id.clone(),
            },
        )?;

        if let Some(warning) = &resume_warning {
            let message = warning.message.clone();
            self.routes.deliver(
                &route,
                AgentEvent::Error {
                    thread: thread_id,
                    turn: None,
                    error: HarnessError::Transport(message),
                },
            )?;
        }

        let resume_replay_model = (opts.resume.is_some() && resume_warning.is_none())
            .then(|| opened.model.clone())
            .flatten();
        Ok(OpenThreadOutcome {
            attachment,
            resume_replay_model,
        })
    }
}

impl<C> CodexInstance<C>
where
    C: CodexTransport,
{
    async fn handle_server_message(
        &mut self,
        message: codex_codes::ServerMessage,
    ) -> MessageOutcome {
        let fallback_thread = fallback_thread(&self.mapper, &self.active_turns);
        match message {
            codex_codes::ServerMessage::Notification(notif) => {
                let eligible_native_thread_id = eligible_notification_native_id(&notif);
                let route = if let Some(native_thread_id) = eligible_native_thread_id
                    .as_ref()
                    .map(EligibleNotificationNativeId::as_ref)
                {
                    let Some(route) = self.ensure_discovered_route(native_thread_id) else {
                        warn!(
                            native_thread_id,
                            method = notif.method(),
                            "dropping Codex notification without an active route"
                        );
                        return MessageOutcome::Handled;
                    };
                    Some(route)
                } else {
                    self.routes.active_for_thread(fallback_thread).ok()
                };
                let Some(route) = route else {
                    return MessageOutcome::Handled;
                };
                let route_thread = route.thread_id();
                let mapped = match self.mapper.try_map_notification(&notif, route_thread) {
                    Ok(mapped) => mapped,
                    Err(error) => {
                        warn!(method = notif.method(), %error, "failed to map routed Codex notification");
                        None
                    }
                };
                if let Some(event) = mapped {
                    let thread = event_thread(&event);
                    if let Some(active) = self.active_turns.get_mut(&thread) {
                        active.mark_server_message();
                        if let AgentEvent::TurnStarted { turn, .. } = &event
                            && *turn == active.acknowledged_turn
                        {
                            active.active_turn = Some(*turn);
                        }
                    }
                    let completed_compaction =
                        observe_pending_compaction(&mut self.pending_compactions, thread, &event);
                    let completed_active_turn =
                        completed_current_active_turn(&self.active_turns, &event)
                            .map(|(_, turn)| turn);
                    if self.active_turns.contains_key(&thread)
                        && matches!(&event, AgentEvent::TurnCompleted { .. })
                        && completed_active_turn.is_none()
                    {
                        debug!(
                            %thread,
                            acknowledged_turn = display_opt(self.active_turns.get(&thread).map(|active| active.acknowledged_turn)),
                            event_turn = display_opt(agent_event_turn(&event)),
                            "ignoring Codex turn completion for a non-current turn"
                        );
                    }
                    let fatal_completion = self.active_turns.get(&thread).and_then(|active| {
                        active
                            .event_is_current_turn(&event)
                            .then(|| {
                                mapping::fatal_turn_error(&notif)
                                    .map(|message| (active.active_turn, message))
                            })
                            .flatten()
                    });
                    if let Err(error) = self.routes.deliver(&route, event) {
                        warn!(%thread, method = notif.method(), %error, "failed to deliver mapped Codex notification");
                    }
                    if let Some(turn) = completed_active_turn {
                        cleanup_active_turn_upload(
                            &mut self.client,
                            &mut self.active_turns,
                            thread,
                        )
                        .await;
                        self.active_turns.remove(&thread);
                        self.mapper.clear_active_turn(thread);
                        debug!(
                            %thread,
                            %turn,
                            remaining_active_turns = self.active_turns.len(),
                            "Codex turn completion observed"
                        );
                    } else if let Some((turn, message)) = fatal_completion
                        && emit_fatal_turn_completion(&self.routes, thread, turn, message).await
                    {
                        cleanup_active_turn_upload(
                            &mut self.client,
                            &mut self.active_turns,
                            thread,
                        )
                        .await;
                        self.active_turns.remove(&thread);
                        self.mapper.clear_active_turn(thread);
                    }
                    if let Some(elapsed_ms) = completed_compaction {
                        return MessageOutcome::CompactionCompleted { thread, elapsed_ms };
                    }
                } else if let Some(message) = mapping::fatal_turn_error(&notif) {
                    let (harness_thread_id, native_turn_id) = match &notif {
                        codex_codes::messages::Notification::Error(error) => {
                            (Some(error.thread_id.as_str()), Some(error.turn_id.as_str()))
                        }
                        _ => (None, notif.turn_id()),
                    };
                    warn!(
                        action = "map_fatal_notification",
                        method = notif.method(),
                        harness_thread_id,
                        native_turn_id,
                        fallback_thread = %fallback_thread,
                        error = %message,
                        "dropping fatal Codex error notification that could not be mapped to a known thread"
                    );
                }
                MessageOutcome::Handled
            }
            codex_codes::ServerMessage::Request { id, request } => {
                // Unknown and genuinely threadless server requests have no active route
                // capability. Do not attribute them to whichever thread happens to be the
                // fallback: that would create browser-response correlation for an unrelated
                // route.
                if matches!(
                    request,
                    codex_codes::messages::ServerRequest::Unknown { .. }
                ) {
                    respond_unroutable_server_request(&mut self.client, &id, &request).await;
                    return MessageOutcome::Handled;
                }
                let eligible_native_thread_id = eligible_server_request_native_id(&request);
                let route = match eligible_native_thread_id.as_deref() {
                    Some(native_thread_id) => self.ensure_discovered_route(native_thread_id),
                    None => None,
                };
                let Some(route) = route else {
                    respond_unroutable_server_request(&mut self.client, &id, &request).await;
                    return MessageOutcome::Handled;
                };
                let prepared = match self.mapper.prepare_server_request(
                    &id,
                    &request,
                    route.thread_id(),
                ) {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        warn!(request_id = %id, method = request.method(), %error, "failed to prepare routed Codex server request");
                        respond_unroutable_server_request(&mut self.client, &id, &request).await;
                        return MessageOutcome::Handled;
                    }
                };
                let Some(event) = prepared.event().cloned() else {
                    return MessageOutcome::Handled;
                };
                let thread = event_thread(&event);
                if let Some(active) = self.active_turns.get_mut(&thread) {
                    active.mark_server_message();
                }
                if let Err(error) = self.routes.deliver(&route, event) {
                    warn!(%thread, request_id = %id, method = request.method(), %error, "failed to deliver Codex server request");
                    respond_unroutable_server_request(&mut self.client, &id, &request).await;
                    return MessageOutcome::Handled;
                }
                self.mapper.commit_server_request(prepared);
                MessageOutcome::Handled
            }
        }
    }
}

pub(super) fn eligible_server_request_native_id(
    request: &codex_codes::messages::ServerRequest,
) -> Option<String> {
    if matches!(
        request,
        codex_codes::messages::ServerRequest::Unknown { .. }
    ) {
        return None;
    }
    server_request_native_scope(request)
        .0
        .and_then(|native_thread_id| trimmed_non_empty(&native_thread_id).map(str::to_owned))
}

enum EligibleNotificationNativeId<'a> {
    Direct(std::borrow::Cow<'a, str>),
}

impl EligibleNotificationNativeId<'_> {
    fn as_ref(&self) -> &str {
        match self {
            Self::Direct(value) => value.as_ref(),
        }
    }
}

fn eligible_notification_native_id(
    notification: &codex_codes::messages::Notification,
) -> Option<EligibleNotificationNativeId<'_>> {
    use codex_codes::ThreadStatus;
    use codex_codes::messages::Notification;

    match notification {
        Notification::ThreadStatusChanged(changed)
            if !matches!(changed.status, ThreadStatus::NotLoaded) =>
        {
            direct_native_id(&changed.thread_id)
        }
        Notification::ThreadStatusChanged(_)
        | Notification::ThreadArchived(_)
        | Notification::ThreadClosed(_)
        | Notification::ThreadDeleted(_)
        | Notification::ThreadUnarchived(_)
        | Notification::Unknown { .. } => None,
        Notification::ThreadStarted(started) => direct_native_id(&started.thread.id),
        Notification::ThreadTokenUsageUpdated(n) => direct_native_id(&n.thread_id),
        Notification::TurnStarted(n) => direct_native_id(&n.thread_id),
        Notification::TurnCompleted(n) => direct_native_id(&n.thread_id),
        Notification::ItemStarted(n) => direct_native_id(&n.thread_id),
        Notification::ItemCompleted(n) => direct_native_id(&n.thread_id),
        Notification::AgentMessageDelta(n) => direct_native_id(&n.thread_id),
        Notification::CmdOutputDelta(n) => direct_native_id(&n.thread_id),
        Notification::FileChangeOutputDelta(n) => direct_native_id(&n.thread_id),
        Notification::FileChangePatchUpdated(n) => direct_native_id(&n.thread_id),
        Notification::ReasoningDelta(n) => direct_native_id(&n.thread_id),
        Notification::ReasoningTextDelta(n) => direct_native_id(&n.thread_id),
        Notification::PlanDelta(n) => direct_native_id(&n.thread_id),
        Notification::McpToolCallProgress(n) => direct_native_id(&n.thread_id),
        Notification::ReasoningSummaryPartAdded(n) => direct_native_id(&n.thread_id),
        Notification::TurnDiffUpdated(n) => direct_native_id(&n.thread_id),
        Notification::Error(n) => direct_native_id(&n.thread_id),
        Notification::TurnPlanUpdated(n) => direct_native_id(&n.thread_id),
        Notification::ModelRerouted(n) => direct_native_id(&n.thread_id),
        Notification::ServerRequestResolved(n) => direct_native_id(&n.thread_id),
        Notification::ContextCompacted(n) => direct_native_id(&n.thread_id),
        Notification::GuardianWarning(n) => direct_native_id(&n.thread_id),
        Notification::Warning(n) => optional_native_id(n.thread_id.as_deref()),
        Notification::ThreadGoalCleared(n) => direct_native_id(&n.thread_id),
        Notification::ThreadNameUpdated(n) => direct_native_id(&n.thread_id),
        Notification::HookCompleted(n) => direct_native_id(&n.thread_id),
        Notification::HookStarted(n) => direct_native_id(&n.thread_id),
        Notification::ItemGuardianApprovalReviewCompleted(n) => direct_native_id(&n.thread_id),
        Notification::ItemGuardianApprovalReviewStarted(n) => direct_native_id(&n.thread_id),
        Notification::TerminalInteraction(n) => direct_native_id(&n.thread_id),
        Notification::ModelVerification(n) => direct_native_id(&n.thread_id),
        Notification::ThreadGoalUpdated(n) => direct_native_id(&n.thread_id),
        Notification::ThreadRealtimeClosed(n) => direct_native_id(&n.thread_id),
        Notification::ThreadRealtimeError(n) => direct_native_id(&n.thread_id),
        Notification::ThreadRealtimeItemAdded(n) => direct_native_id(&n.thread_id),
        Notification::ThreadRealtimeOutputAudioDelta(n) => direct_native_id(&n.thread_id),
        Notification::ThreadRealtimeSdp(n) => direct_native_id(&n.thread_id),
        Notification::ThreadRealtimeStarted(n) => direct_native_id(&n.thread_id),
        Notification::ThreadRealtimeTranscriptDelta(n) => direct_native_id(&n.thread_id),
        Notification::ThreadRealtimeTranscriptDone(n) => direct_native_id(&n.thread_id),
        Notification::ThreadSettingsUpdated(n) => direct_native_id(&n.thread_id),
        Notification::TurnModerationMetadata(n) => direct_native_id(&n.thread_id),
        Notification::ModelSafetyBufferingUpdated(n) => direct_native_id(&n.thread_id),
        Notification::ThreadEnvironmentConnected(n)
        | Notification::ThreadEnvironmentDisconnected(n) => direct_native_id(&n.thread_id),
        Notification::StrictReviewRequired(n) => direct_native_id(&n.thread_id),
        Notification::ThreadProjectUpdated(n) => direct_native_id(&n.thread_id),
        Notification::ThreadQueueChanged(n) => direct_native_id(&n.thread_id),
        Notification::ThreadReverted(n) => direct_native_id(&n.thread_id),
        Notification::ThreadRealtimeItemStarted(n) => direct_native_id(&n.thread_id),
        Notification::ThreadRealtimeItemCompleted(n) => direct_native_id(&n.thread_id),
        Notification::ThreadRealtimeItemTranscriptDelta(n) => direct_native_id(&n.thread_id),
        Notification::ModelProviderAuthRecoveryStarted(n)
        | Notification::ModelProviderAuthRecoveryCompleted(n) => direct_native_id(&n.thread_id),
        Notification::McpServerStartupStatusUpdated(n) => {
            optional_native_id(n.thread_id.as_deref())
        }
        Notification::McpServerOauthLoginCompleted(n) => optional_native_id(n.thread_id.as_deref()),
        Notification::AccountRateLimitsUpdated(_)
        | Notification::RemoteControlStatusChanged(_)
        | Notification::AccountLoginCompleted(_)
        | Notification::DeprecationNotice(_)
        | Notification::SkillsChanged(_)
        | Notification::FsChanged(_)
        | Notification::ConfigWarning(_)
        | Notification::AccountUpdated(_)
        | Notification::AppListUpdated(_)
        | Notification::CommandExecOutputDelta(_)
        | Notification::ExternalAgentConfigImportCompleted(_)
        | Notification::FuzzyFileSearchSessionCompleted(_)
        | Notification::FuzzyFileSearchSessionUpdated(_)
        | Notification::ProcessExited(_)
        | Notification::ProcessOutputDelta(_)
        | Notification::WindowsWorldWritableWarning(_)
        | Notification::WindowsSandboxSetupCompleted(_)
        | Notification::ExternalAgentConfigImportProgress(_)
        | Notification::ProjectChanged(_)
        | Notification::McpServerEventStream(_) => None,
    }
}

fn direct_native_id(value: &str) -> Option<EligibleNotificationNativeId<'_>> {
    trimmed_non_empty(value)
        .map(|value| EligibleNotificationNativeId::Direct(std::borrow::Cow::Borrowed(value)))
}

fn optional_native_id(value: Option<&str>) -> Option<EligibleNotificationNativeId<'_>> {
    value.and_then(direct_native_id)
}

impl<C> CodexInstance<C>
where
    C: CodexTransport,
{
    async fn handle_control_command(&mut self, control: ControlCommand) {
        match control {
            ControlCommand::ClaimNativeThread {
                thread,
                harness_thread_id,
                workspace_root,
                reactivate_tombstone,
                response,
            } => {
                let parent_harness_thread_id = self.mapper.native_parent(&harness_thread_id);
                let native_for_handle = harness_thread_id.clone();
                let make_handle = move |authoritative_thread| ThreadHandle {
                    parent_harness_thread_id,
                    ..ThreadHandle::opened(authoritative_thread, native_for_handle, workspace_root)
                };
                let result = if reactivate_tombstone {
                    self.routes.reattach(harness_thread_id, thread, make_handle)
                } else {
                    self.routes
                        .claim_parent(harness_thread_id, thread, make_handle)
                };
                let _ = response.send(result);
            }
            ControlCommand::RespondApproval {
                id,
                decision,
                response,
            } => {
                let result =
                    handle_respond_approval(&mut self.client, &mut self.mapper, &id, &decision)
                        .await;
                let _ = response.send(result);
            }
            ControlCommand::RespondServerRequest {
                id,
                response_payload,
                response,
            } => {
                let result = handle_respond_server_request(
                    &mut self.client,
                    &mut self.mapper,
                    &self.routes,
                    &id,
                    response_payload,
                )
                .await;
                let _ = response.send(result);
            }
            ControlCommand::Interrupt { thread, response } => {
                let native_turn_id = self
                    .mapper
                    .active_native_turn_for_thread(thread.thread)
                    .map(str::to_owned);
                let result = timeout_codex_control(
                    "interrupt",
                    Some(&thread),
                    None,
                    native_turn_id.as_deref(),
                    handle_interrupt(&mut self.client, &self.mapper, &thread),
                )
                .await;
                if result.is_ok() {
                    reject_pending_requests_for_interrupted_thread(
                        &mut self.client,
                        &mut self.mapper,
                        &self.routes,
                        thread.thread,
                    )
                    .await;
                }
                let _ = response.send(result);
            }
            ControlCommand::TerminateCommand {
                thread,
                process_id,
                response,
            } => {
                let native_turn_id = self
                    .mapper
                    .native_turn_for_process(thread.thread, &process_id)
                    .or_else(|| self.mapper.active_native_turn_for_thread(thread.thread))
                    .map(str::to_owned);
                let result = timeout_codex_control(
                    "terminate_command",
                    Some(&thread),
                    Some(&process_id),
                    native_turn_id.as_deref(),
                    handle_terminate_command(&mut self.client, &thread, &process_id),
                )
                .await;
                let _ = response.send(result);
            }
            ControlCommand::CompactThread { thread, response } => {
                if self.active_turns.contains_key(&thread.thread) {
                    let _ = response.send(Err(HarnessError::Unsupported(
                        "context compaction is not available during an active turn".into(),
                    )));
                    return;
                }
                let started = Instant::now();
                info!(
                    thread = %thread.thread,
                    harness_thread_id = %thread.harness_thread_id,
                    pending_compactions = self.pending_compactions.len(),
                    "requesting Codex context compaction"
                );
                let result = handle_compact_thread(&mut self.client, &thread).await;
                match &result {
                    Ok(()) => {
                        self.pending_compactions
                            .insert(thread.thread, PendingCompaction::new(started));
                        info!(
                            thread = %thread.thread,
                            harness_thread_id = %thread.harness_thread_id,
                            ack_elapsed_ms = started.elapsed().as_millis(),
                            pending_compactions = self.pending_compactions.len(),
                            "Codex accepted context compaction request"
                        );
                    }
                    Err(error) => {
                        warn!(
                            action = "compact_thread",
                            thread_id = %thread.thread,
                            harness_thread_id = %thread.harness_thread_id,
                            error = %error,
                            elapsed_ms = started.elapsed().as_millis(),
                            "Codex context compaction request failed"
                        );
                    }
                }
                let _ = response.send(result);
            }
            ControlCommand::SetThreadName {
                thread,
                name,
                response,
            } => {
                let result = handle_set_thread_name(&mut self.client, &thread, &name).await;
                let _ = response.send(result);
            }
            ControlCommand::SetThreadArchived {
                thread,
                archived,
                response,
            } => {
                let result = if self.active_turns.contains_key(&thread.thread) {
                    Err(HarnessError::Unsupported(
                        "thread archiving is not available during an active turn".into(),
                    ))
                } else {
                    handle_set_thread_archived(&mut self.client, &thread, archived).await
                };
                let _ = response.send(result);
            }
            ControlCommand::DeleteThread {
                thread,
                retired,
                response,
            } => {
                if self.active_turns.contains_key(&thread.thread) {
                    let error = HarnessError::Unsupported(
                        "thread deletion is not available during an active turn".into(),
                    );
                    let _ = retired.send(Err(HarnessError::Unsupported(error.to_string())));
                    let _ = response.send(Err(error));
                } else {
                    match self.tombstone_event_route(&thread) {
                        Ok(()) => {
                            // Tombstoning is the committed local cutover. Purge every operation
                            // owned by that activation before provider I/O, regardless of whether
                            // the subsequent best-effort delete succeeds.
                            retire_tombstoned_state(
                                &mut self.mapper,
                                &mut self.pending_context_restores,
                                &mut self.pending_compactions,
                                &thread,
                            );
                            let _ = retired.send(Ok(()));
                            let result = match handle_delete_thread(&mut self.client, &thread).await
                            {
                                Ok(()) => Ok(ThreadDeletion::Retired),
                                Err(error) => Ok(ThreadDeletion::RetiredWithProviderError(error)),
                            };
                            let _ = response.send(result);
                        }
                        Err(error) => {
                            let retirement_error = HarnessError::Protocol(error.to_string());
                            let _ = retired.send(Err(retirement_error));
                            let _ = response.send(Err(error));
                        }
                    }
                }
            }
            ControlCommand::ListMcpServers { response } => {
                let result = timeout_codex_control(
                    "list_mcp_servers",
                    None,
                    None,
                    None,
                    handle_list_mcp_servers(&mut self.client),
                )
                .await;
                let _ = response.send(result);
            }
            ControlCommand::ReloadMcpServers { response } => {
                let result = timeout_codex_control(
                    "reload_mcp_servers",
                    None,
                    None,
                    None,
                    handle_reload_mcp_servers(&mut self.client),
                )
                .await;
                let _ = response.send(result);
            }
            ControlCommand::StartMcpOauthLogin { name, response } => {
                let result = timeout_codex_control(
                    "start_mcp_oauth_login",
                    None,
                    Some(&name),
                    None,
                    handle_start_mcp_oauth_login(&mut self.client, &name),
                )
                .await;
                let _ = response.send(result);
            }
            ControlCommand::ListProviders { cwd, response } => {
                let result = timeout_codex_control(
                    "list_providers",
                    None,
                    None,
                    None,
                    handle_list_providers(&mut self.client, cwd),
                )
                .await;
                let _ = response.send(result);
            }
            ControlCommand::ListModels { cwd, response } => {
                let result = timeout_codex_control(
                    "list_models",
                    None,
                    None,
                    None,
                    handle_list_models(&mut self.client, cwd),
                )
                .await;
                let _ = response.send(result);
            }
        }
    }
}

fn retire_tombstoned_state(
    mapper: &mut CodexMapper,
    pending_context_restores: &mut HashMap<NativeThreadId, PendingContextRestore>,
    pending_compactions: &mut HashMap<ThreadId, PendingCompaction>,
    thread: &ThreadHandle,
) {
    mapper.retire_thread(thread.thread);
    pending_context_restores.remove(thread.harness_thread_id.as_str());
    pending_compactions.remove(&thread.thread);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tombstone_cutover_clears_restore_and_compaction_before_provider_cleanup() {
        let thread = ThreadHandle::detached(ThreadId::new(), "native-retired".into());
        let mut mapper = CodexMapper::new(PathBuf::from("/tmp"));
        let (updates, _) = giskard_harness::thread_update_channel();
        let mut restores = HashMap::from([(
            NativeThreadId::new(thread.harness_thread_id.clone()),
            PendingContextRestore {
                thread: thread.thread,
                model: ModelRef {
                    provider: "openai".into(),
                    model: "test".into(),
                    reasoning_effort: None,
                },
                sink: updates,
            },
        )]);
        let mut compactions =
            HashMap::from([(thread.thread, PendingCompaction::new(Instant::now()))]);

        retire_tombstoned_state(&mut mapper, &mut restores, &mut compactions, &thread);

        assert!(restores.is_empty());
        assert!(compactions.is_empty());
    }
}
