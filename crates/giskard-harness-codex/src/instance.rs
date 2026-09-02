use super::*;
use crate::native_routes::UnknownNativeThread;

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
    senders: SenderMap,
    discoveries: Arc<EventLog<ThreadDiscovered>>,
    worker_queue: Arc<WorkerQueueWatchdog>,
    workspace_root: PathBuf,
    writable_roots: Vec<PathBuf>,
    mapper: CodexMapper,
    active_turns: ActiveTurns,
    pending_compactions: HashMap<ThreadId, PendingCompaction>,
    pending_context_restores: HashMap<NativeThreadId, PendingContextRestore>,
}

impl<C> CodexInstance<C> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        client: C,
        receivers: WorkerReceivers,
        senders: SenderMap,
        discoveries: Arc<EventLog<ThreadDiscovered>>,
        worker_queue: Arc<WorkerQueueWatchdog>,
        workspace_root: PathBuf,
        writable_roots: Vec<PathBuf>,
        bootstrap: HarnessBootstrap,
    ) -> Result<Self, HarnessError> {
        let mapper = CodexMapper::new(workspace_root.clone());
        let mut instance = Self {
            client,
            receivers,
            senders,
            discoveries,
            worker_queue,
            workspace_root,
            writable_roots,
            mapper,
            active_turns: HashMap::new(),
            pending_compactions: HashMap::new(),
            pending_context_restores: HashMap::new(),
        };
        for binding in bootstrap.known_threads {
            instance.claim_thread_route(binding.harness_thread_id, binding.thread_id)?;
        }
        Ok(instance)
    }

    fn claim_thread_route(
        &mut self,
        harness_thread_id: String,
        thread_id: ThreadId,
    ) -> Result<(), HarnessError> {
        self.mapper.claim_thread(harness_thread_id, thread_id)?;
        self.ensure_thread_route_sender(thread_id);
        Ok(())
    }

    fn claim_or_adopt_thread_route(
        &mut self,
        harness_thread_id: String,
        proposed_thread_id: ThreadId,
    ) -> Result<ThreadId, HarnessError> {
        let thread_id = self
            .mapper
            .claim_or_adopt_thread(harness_thread_id, proposed_thread_id)?;
        self.ensure_thread_route_sender(thread_id);
        Ok(thread_id)
    }

    fn replace_thread_route(
        &mut self,
        expected_harness_thread_id: String,
        new_harness_thread_id: String,
        thread_id: ThreadId,
    ) -> Result<(), HarnessError> {
        self.mapper.replace_thread_route(
            expected_harness_thread_id,
            new_harness_thread_id,
            thread_id,
        )?;
        self.ensure_thread_route_sender(thread_id);
        Ok(())
    }

    fn ensure_thread_route_sender(&self, thread_id: ThreadId) {
        lock_senders(&self.senders)
            .entry(thread_id)
            .or_insert_with(|| Arc::new(EventLog::new()));
    }
}

impl<C> CodexInstance<C>
where
    C: CodexTransport,
{
    pub(super) async fn run(mut self) {
        let mut first_event_warn_tick = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                biased;
                _ = wait_for_shutdown_request(&mut self.receivers.shutdown) => {
                    cleanup_all_active_turn_uploads(&mut self.client, &mut self.active_turns).await;
                    shutdown_codex_transport(self.client, &self.workspace_root).await;
                    self.worker_queue.close();
                    self.discoveries.close();
                    self.receivers.done.send_replace(true);
                    return;
                }
                msg = self.client.next_message() => {
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
                                &self.senders,
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
                                    &self.senders,
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
        self.worker_queue.close();
        self.discoveries.close();
        self.receivers.done.send_replace(true);
    }

    async fn handle_harness_command(&mut self, queued: QueuedHarnessCommand) {
        match queued.command {
            HarnessCommand::OpenThread { opts, response } => {
                let result = self.handle_open_thread(&opts).await;
                match result {
                    Ok(outcome) => {
                        let handle = outcome.handle;
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
                        let _ = response.send(Ok(handle));
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

        // B4: bind the (possibly re-established) native id to the durable ThreadId. A failed
        // resume replaces the exact bootstrapped native route with the fresh provider identity.
        if let (Some(expected_harness_thread_id), Some(_)) = (&opts.resume, &resume_warning) {
            self.replace_thread_route(
                expected_harness_thread_id.clone(),
                opened.harness_thread_id.clone(),
                thread_id,
            )?;
        } else {
            self.claim_thread_route(opened.harness_thread_id.clone(), thread_id)?;
        }

        let _ = broadcast_event(&self.senders, thread_id, || AgentEvent::ThreadOpened {
            thread: thread_id,
            harness_thread_id: opened.harness_thread_id.clone(),
        })
        .await;

        if let Some(warning) = &resume_warning {
            let message = warning.message.clone();
            let _ = broadcast_event(&self.senders, thread_id, || AgentEvent::Error {
                thread: thread_id,
                turn: None,
                error: HarnessError::Transport(message),
            })
            .await;
        }

        let resume_replay_model = (opts.resume.is_some() && resume_warning.is_none())
            .then(|| opened.model.clone())
            .flatten();
        Ok(OpenThreadOutcome {
            handle: ThreadHandle {
                warning: resume_warning,
                resumed_model: opened.model,
                agent_name: opened.agent_name,
                parent_harness_thread_id: opened.parent_harness_thread_id,
                ..ThreadHandle::opened(
                    thread_id,
                    opened.harness_thread_id,
                    opts.workspace_root.clone(),
                )
            },
            resume_replay_model,
        })
    }
}

impl<C> CodexInstance<C>
where
    C: CodexTransport,
{
    fn map_or_discover<T>(
        &mut self,
        map: impl Fn(&mut CodexMapper) -> Result<T, UnknownNativeThread>,
        frame: &'static str,
    ) -> Option<T> {
        match map(&mut self.mapper) {
            Ok(mapped) => Some(mapped),
            Err(UnknownNativeThread { native_thread_id }) => {
                let thread = ThreadId::new();
                if let Err(error) = self.claim_thread_route(native_thread_id.to_string(), thread) {
                    warn!(
                        native_thread_id = %native_thread_id,
                        %thread,
                        %error,
                        frame,
                        "could not bind a native thread discovered from traffic; dropping frame"
                    );
                    return None;
                }
                let parent = self.mapper.native_parent(native_thread_id.as_str());
                info!(
                    native_thread_id = %native_thread_id,
                    %thread,
                    parent_harness_thread_id = parent.as_deref(),
                    frame,
                    "bound a native thread discovered from traffic"
                );
                self.discoveries.append(ThreadDiscovered {
                    thread,
                    harness_thread_id: native_thread_id.to_string(),
                    parent_harness_thread_id: parent,
                });
                match map(&mut self.mapper) {
                    Ok(mapped) => Some(mapped),
                    Err(error) => {
                        error!(
                            native_thread_id = %error.native_thread_id,
                            frame,
                            "native thread still unknown after binding; dropping frame"
                        );
                        None
                    }
                }
            }
        }
    }

    async fn handle_server_message(
        &mut self,
        message: codex_codes::ServerMessage,
    ) -> MessageOutcome {
        let fallback_thread = fallback_thread(&self.mapper, &self.active_turns);
        match message {
            codex_codes::ServerMessage::Notification(notif) => {
                if let Some(event) = self
                    .map_or_discover(
                        |mapper| mapper.try_map_notification(&notif, fallback_thread),
                        "notification",
                    )
                    .flatten()
                {
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
                    let _ = broadcast_event(&self.senders, thread, || event).await;
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
                        && emit_fatal_turn_completion(&self.senders, thread, turn, message).await
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
                let Some(event) = self
                    .map_or_discover(
                        |mapper| mapper.try_map_server_request(&id, &request, fallback_thread),
                        "server_request",
                    )
                    .flatten()
                else {
                    respond_unroutable_server_request(&mut self.client, &id, &request).await;
                    return MessageOutcome::Handled;
                };
                let thread = event_thread(&event);
                if let Some(active) = self.active_turns.get_mut(&thread) {
                    active.mark_server_message();
                }
                let _ = broadcast_event(&self.senders, thread, || event).await;
                MessageOutcome::Handled
            }
        }
    }
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
                response,
            } => {
                let result = self
                    .claim_or_adopt_thread_route(harness_thread_id.clone(), thread)
                    .map(|accepted_thread| {
                        // A claim answers with the identity facts this harness lifetime already
                        // attested through its own events. It must not resume the thread to learn
                        // more: the native model stays unreported until an event names it.
                        let parent_harness_thread_id =
                            self.mapper.native_parent(&harness_thread_id);
                        ThreadHandle {
                            parent_harness_thread_id,
                            ..ThreadHandle::opened(
                                accepted_thread,
                                harness_thread_id,
                                workspace_root,
                            )
                        }
                    });
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
                    &self.senders,
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
                        &self.senders,
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
            ControlCommand::DeleteThread { thread, response } => {
                let result = if self.active_turns.contains_key(&thread.thread) {
                    Err(HarnessError::Unsupported(
                        "thread deletion is not available during an active turn".into(),
                    ))
                } else {
                    handle_delete_thread(&mut self.client, &thread).await
                };
                if result.is_ok() {
                    if let Some(log) = lock_senders(&self.senders).remove(&thread.thread) {
                        log.close();
                    }
                    self.pending_context_restores
                        .remove(thread.harness_thread_id.as_str());
                }
                let _ = response.send(result);
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
