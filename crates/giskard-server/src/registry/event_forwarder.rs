use super::*;

fn is_context_compaction_item(item: &Item) -> bool {
    matches!(
        &item.payload,
        ItemPayload::Activity { title, .. } if title == "Context compacted"
    )
}

fn should_skip_duplicate_notice(
    event: &AgentEvent,
    seen_notices: &mut HashSet<(Option<TurnId>, String)>,
) -> bool {
    let AgentEvent::Notice { turn, message, .. } = event else {
        return false;
    };
    !seen_notices.insert((*turn, message.clone()))
}

pub(super) fn event_turn_id(event: &AgentEvent) -> Option<TurnId> {
    match event {
        AgentEvent::TurnStarted { turn, .. }
        | AgentEvent::TurnUsageUpdated { turn, .. }
        | AgentEvent::ItemStarted { turn, .. }
        | AgentEvent::ItemDelta { turn, .. }
        | AgentEvent::ItemCompleted { turn, .. }
        | AgentEvent::DiffUpdated { turn, .. }
        | AgentEvent::ApprovalRequested { turn, .. }
        | AgentEvent::TurnCompleted { turn, .. } => Some(*turn),
        AgentEvent::ServerRequestReceived { turn, .. }
        | AgentEvent::ServerRequestResolved { turn, .. } => *turn,
        AgentEvent::ThreadOpened { .. }
        | AgentEvent::Error { turn: None, .. }
        | AgentEvent::Notice { turn: None, .. } => None,
        AgentEvent::Error {
            turn: Some(turn), ..
        }
        | AgentEvent::Notice {
            turn: Some(turn), ..
        } => Some(*turn),
    }
}

fn event_item_identity(event: &AgentEvent) -> Option<(TurnId, &str, ItemId)> {
    match event {
        AgentEvent::ItemStarted { turn, item, .. } if !item.harness_item_id.is_empty() => {
            Some((*turn, item.harness_item_id.as_str(), item.id))
        }
        AgentEvent::ItemCompleted { turn, item, .. } if !item.harness_item_id.is_empty() => {
            Some((*turn, item.harness_item_id.as_str(), item.id))
        }
        _ => None,
    }
}

/// Harness-neutral native item identity retained by one thread's event forwarder.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct HarnessItemId(String);

impl HarnessItemId {
    fn new(value: String) -> Self {
        Self(value)
    }
}

/// Scoped harness item identity; native item IDs need only be unique within a turn.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct HarnessItemKey {
    turn_id: TurnId,
    harness_item_id: HarnessItemId,
}

impl HarnessItemKey {
    fn new(turn_id: TurnId, harness_item_id: HarnessItemId) -> Self {
        Self {
            turn_id,
            harness_item_id,
        }
    }
}

fn track_item_identity(
    item_ids_by_harness: &mut HashMap<HarnessItemKey, ItemId>,
    event: &AgentEvent,
) -> Option<(TurnId, String, ItemId, ItemId)> {
    let (turn, harness_item_id, item_id) = event_item_identity(event)?;
    let identity_key = HarnessItemKey::new(turn, HarnessItemId::new(harness_item_id.to_owned()));
    match item_ids_by_harness.get(&identity_key) {
        Some(existing_item_id) if *existing_item_id != item_id => {
            Some((turn, harness_item_id.to_owned(), *existing_item_id, item_id))
        }
        Some(_) => None,
        None => {
            item_ids_by_harness.insert(identity_key, item_id);
            None
        }
    }
}

pub(super) fn event_kind(event: &AgentEvent) -> &'static str {
    match event {
        AgentEvent::ThreadOpened { .. } => "thread_opened",
        AgentEvent::TurnStarted { .. } => "turn_started",
        AgentEvent::TurnUsageUpdated { .. } => "turn_usage_updated",
        AgentEvent::ItemStarted { .. } => "item_started",
        AgentEvent::ItemDelta { .. } => "item_delta",
        AgentEvent::ItemCompleted { .. } => "item_completed",
        AgentEvent::DiffUpdated { .. } => "diff_updated",
        AgentEvent::ApprovalRequested { .. } => "approval_requested",
        AgentEvent::ServerRequestReceived { .. } => "server_request_received",
        AgentEvent::ServerRequestResolved { .. } => "server_request_resolved",
        AgentEvent::TurnCompleted { .. } => "turn_completed",
        AgentEvent::Error { .. } => "error",
        AgentEvent::Notice { .. } => "notice",
    }
}

pub(super) fn event_item_id(event: &AgentEvent) -> Option<ItemId> {
    match event {
        AgentEvent::ItemStarted { item, .. } => Some(item.id),
        AgentEvent::ItemDelta { item_id, .. } => Some(*item_id),
        AgentEvent::ItemCompleted { item, .. } => Some(item.id),
        _ => None,
    }
}

fn event_item_delta_kind(event: &AgentEvent) -> Option<&'static str> {
    match event {
        AgentEvent::ItemDelta {
            delta: ItemDelta::Text { .. },
            ..
        } => Some("text"),
        AgentEvent::ItemDelta {
            delta: ItemDelta::CommandOutput { .. },
            ..
        } => Some("command_output"),
        _ => None,
    }
}

struct CompletedItemDiagnostics {
    turn_id: TurnId,
    item_id: ItemId,
    harness_item_id: String,
    payload_kind: &'static str,
}

fn completed_item_diagnostics(event: &AgentEvent) -> Option<CompletedItemDiagnostics> {
    let AgentEvent::ItemCompleted { turn, item, .. } = event else {
        return None;
    };
    Some(CompletedItemDiagnostics {
        turn_id: *turn,
        item_id: item.id,
        harness_item_id: item.harness_item_id.clone(),
        payload_kind: match item.payload {
            ItemPayload::CommandExecution { .. } => "command_execution",
            ItemPayload::ToolCall { .. } => "tool_call",
            _ => "other",
        },
    })
}

fn log_foreign_thread_event_drop(
    project_id: ProjectId,
    thread_id: ThreadId,
    event_thread_id: ThreadId,
    event: &AgentEvent,
) {
    error!(
        %project_id,
        %thread_id,
        %event_thread_id,
        event_kind = event_kind(event),
        event_turn_id = display_opt(event_turn_id(event)),
        event_item_id = display_opt(event_item_id(event)),
        item_delta_kind = event_item_delta_kind(event),
        "dropping harness event for a different thread"
    );
}

pub(super) fn log_metadata_only_event_rejection(
    project_id: ProjectId,
    thread_id: ThreadId,
    event_kind: &'static str,
    event_turn_id: Option<TurnId>,
    event_item_id: Option<ItemId>,
) {
    warn!(
        %project_id,
        %thread_id,
        event_kind,
        event_turn_id = display_opt(event_turn_id),
        event_item_id = display_opt(event_item_id),
        "refusing to broadcast a metadata-only event on the transcript stream"
    );
}

fn log_cross_turn_event_drop(
    project_id: ProjectId,
    thread_id: ThreadId,
    owned_turn: TurnId,
    event_turn: TurnId,
    event: &AgentEvent,
    elapsed_ms: u128,
) {
    warn!(
        %project_id,
        %thread_id,
        %owned_turn,
        %event_turn,
        event_kind = event_kind(event),
        event_item_id = display_opt(event_item_id(event)),
        item_delta_kind = event_item_delta_kind(event),
        elapsed_ms,
        "dropping harness event for a different turn on the same thread"
    );
}

fn log_ignored_seen_turn_running_task_start(project_id: ProjectId, event: &AgentEvent) {
    let AgentEvent::ItemStarted { thread, turn, item } = event else {
        return;
    };
    let Some(command) = &item.command else {
        return;
    };
    let status = command.status.as_deref().unwrap_or("in_progress");
    if !command_status_is_running(status) {
        return;
    }
    warn!(
        %project_id,
        thread_id = %thread,
        turn_id = %turn,
        item_id = %item.id,
        harness_item_id = %item.harness_item_id,
        process_id = display_opt(command.process_id.as_deref()),
        command = %command.command,
        status,
        "ignoring running command start for already-persisted turn"
    );
}

async fn terminating_command_before_terminal_completion(
    runtime: &ThreadRuntimeSupport,
    authority: &Arc<ThreadAuthority>,
    event: &AgentEvent,
) -> Option<RunningTask> {
    let AgentEvent::ItemCompleted { turn, item, .. } = event else {
        return None;
    };
    let ItemPayload::CommandExecution { status, .. } = &item.payload else {
        return None;
    };
    if status
        .as_deref()
        .map(command_status_is_running)
        .unwrap_or(false)
    {
        return None;
    }

    let command = runtime.task_by_item(authority, *turn, item.id)?;
    command.terminating.then_some(command)
}

fn log_command_completion_after_terminate(
    project_id: ProjectId,
    command: Option<&RunningTask>,
    event: &AgentEvent,
) {
    let Some(command) = command else {
        return;
    };
    let AgentEvent::ItemCompleted { thread, turn, item } = event else {
        return;
    };
    let ItemPayload::CommandExecution {
        status,
        exit_code,
        duration_ms,
        ..
    } = &item.payload
    else {
        return;
    };
    let Some(status) = status else {
        return;
    };
    if !command_completion_is_normal_success(status, *exit_code) {
        return;
    }

    warn!(
        %project_id,
        thread_id = %thread,
        turn_id = %turn,
        item_id = %item.id,
        harness_item_id = %item.harness_item_id,
        process_id = display_opt(command.process_id.as_deref()),
        command = %command.command,
        status = %status,
        exit_code = display_opt(exit_code.as_ref()),
        duration_ms = display_opt(duration_ms.as_ref()),
        "command completed normally after stop request; Codex did not terminate the process"
    );
}

async fn publish_applied_runtime_effects(
    hub: &Hub,
    thread_id: ThreadId,
    applied: AppliedRuntimeEvent,
) {
    if let Some(request) = applied.request_state {
        hub.broadcast(thread_id, ServerMessage::RequestState(request))
            .await;
    }
    if let Some(tasks) = applied.running_tasks_if_changed {
        hub.broadcast(
            thread_id,
            ServerMessage::RunningTasks {
                thread_id,
                revision: tasks.revision,
                tasks: tasks.tasks,
            },
        )
        .await;
    }
    if let Some(overview) = applied.overview_if_changed {
        hub.publish_runtime_overview(overview).await;
    }
}

fn is_terminal_command_completion(event: &AgentEvent) -> bool {
    let AgentEvent::ItemCompleted { item, .. } = event else {
        return false;
    };
    let ItemPayload::CommandExecution { status, .. } = &item.payload else {
        return false;
    };
    !status
        .as_deref()
        .map(command_status_is_running)
        .unwrap_or(false)
}

fn completed_tool_has_terminal_output(item: &Item) -> bool {
    let ItemPayload::ToolCall { output, status, .. } = &item.payload else {
        return false;
    };
    output.is_some() && !status.as_deref().is_some_and(tool_status_is_running)
}

fn late_command_completion_message(
    thread_id: ThreadId,
    event: AgentEvent,
) -> Option<ServerMessage> {
    let AgentEvent::ItemCompleted { thread, turn, item } = event else {
        return None;
    };
    let descriptor = match &item.payload {
        ItemPayload::CommandExecution {
            output,
            output_truncated,
            output_original_bytes,
            output_original_lines,
            ..
        } => {
            let (original_bytes, original_lines) = giskard_core::resolve_command_output_counts(
                output,
                *output_truncated,
                *output_original_bytes,
                *output_original_lines,
            );
            Some(giskard_core::CommandOutputDescriptor::from_durable(
                output,
                *output_truncated,
                original_bytes,
                original_lines,
                false,
            ))
        }
        _ => None,
    };
    Some(ServerMessage::Event {
        thread_id,
        agent_event: Box::new(WireAgentEvent::ItemCompleted {
            thread,
            turn,
            item: WireItem::from_item_with_command_output(item, descriptor),
        }),
    })
}

fn command_completion_is_normal_success(status: &str, exit_code: Option<i32>) -> bool {
    matches!(
        normalized_command_status(status).as_str(),
        "completed" | "succeeded" | "success"
    ) && exit_code == Some(0)
}

/// Owns the current turn's completed items and their authoritative `ItemId` index.
/// Keeping both in one type ensures draining a completed turn cannot leave indexes pointing into
/// the previous vector. Native item ids are validated separately and are never used to re-key an
/// item whose Giskard identity is already known.
#[derive(Default)]
struct CurrentTurnItems {
    items: Vec<Item>,
    indexes: HashMap<ItemId, usize>,
}

impl CurrentTurnItems {
    fn len(&self) -> usize {
        self.items.len()
    }

    fn iter(&self) -> impl Iterator<Item = &Item> {
        self.items.iter()
    }

    fn rebuild_indexes(&mut self) {
        self.indexes.clear();
        for (idx, item) in self.items.iter().enumerate() {
            self.indexes.insert(item.id, idx);
        }
    }

    /// Returns true when an inconsistent stale index was detected and repaired.
    fn upsert(&mut self, item: &Item) -> bool {
        if let Some(&idx) = self.indexes.get(&item.id) {
            if let Some(existing) = self
                .items
                .get_mut(idx)
                .filter(|existing| existing.id == item.id)
            {
                *existing = item.clone();
                return false;
            }
            self.rebuild_indexes();
            if let Some(&repaired_idx) = self.indexes.get(&item.id) {
                self.items[repaired_idx] = item.clone();
                return true;
            }
            self.append_indexed(item);
            return true;
        }
        self.append_indexed(item);
        false
    }

    fn append_indexed(&mut self, item: &Item) {
        let idx = self.items.len();
        self.items.push(item.clone());
        self.indexes.insert(item.id, idx);
    }

    fn take(&mut self) -> Vec<Item> {
        self.indexes.clear();
        std::mem::take(&mut self.items)
    }
}

async fn persisted_turn_ids(
    store: &PersistStore,
    project_id: ProjectId,
    thread_id: ThreadId,
) -> HashSet<TurnId> {
    match store.load_all_turns(project_id, thread_id).await {
        Ok(turns) => turns.into_iter().map(|turn| turn.id).collect(),
        Err(error) => {
            warn!(
                %project_id,
                %thread_id,
                %error,
                "failed to load persisted turn ids; duplicate-turn detection starts empty"
            );
            HashSet::new()
        }
    }
}

/// Persist an effective context window reported by the harness for a turn's model.
async fn persist_model_context_window(
    thread_metadata: &ThreadMetadataService,
    project_id: ProjectId,
    thread_id: ThreadId,
    turn_id: TurnId,
    model: &ModelRef,
    context_window: u32,
) {
    let provider = model.provider.clone();
    let model_id = model.model.clone();
    let stored_model = model.clone();
    match thread_metadata
        .mutate(project_id, thread_id, move |tf| {
            tf.record_model_context_window(&stored_model, context_window);
        })
        .await
    {
        Ok(ThreadMutation::Changed { after, .. }) => info!(
            %project_id,
            %thread_id,
            %turn_id,
            metadata_revision = after.revision,
            provider = %provider,
            model = %model_id,
            context_window,
            "persisted harness-reported model context window"
        ),
        Ok(ThreadMutation::Unchanged { current }) => debug!(
            %project_id,
            %thread_id,
            %turn_id,
            metadata_revision = current.revision,
            provider = %provider,
            model = %model_id,
            context_window,
            "harness-reported model context window was already current"
        ),
        Ok(ThreadMutation::Missing) => warn!(
            %project_id,
            %thread_id,
            %turn_id,
            provider = %provider,
            model = %model_id,
            context_window,
            "thread file missing while persisting model context window"
        ),
        Err(error) => error!(
            %project_id,
            %thread_id,
            %turn_id,
            provider = %provider,
            model = %model_id,
            context_window,
            %error,
            "failed to persist harness-reported model context window"
        ),
    }
}

/// Append a completed `Turn` to the thread file, fold its usage into the thread ledger, persist
/// atomically (§7.1), and hand the usage delta to the global + project ledger actor (§10.2).
/// Best-effort: logs on failure.
#[derive(Clone, Debug, Default)]
struct PersistTurnOutcome {
    history_appended: bool,
    metadata_updated: bool,
    history_error: Option<String>,
}

async fn persist_turn(
    thread_metadata: &ThreadMetadataService,
    ledger: &LedgerHandle,
    project_id: ProjectId,
    thread_id: ThreadId,
    turn: &Turn,
    captured_diffs: &[giskard_core::CapturedDiffRecord],
) -> PersistTurnOutcome {
    // Only completed/interrupted turns carry real usage; capture the bits we need before `turn`
    // moves into the closure.
    let should_record = matches!(
        turn.status.kind,
        TurnStatusKind::Completed | TurnStatusKind::Interrupted
    );
    let model = turn.model.clone();
    let usage = turn.usage;
    let turn_id = turn.id;
    let item_count = turn.items.len();
    let diff_count = turn.diffs.len();
    let status_kind = turn.status.kind;
    let started_at = turn.started_at;
    let completed_at = turn.completed_at;

    // H3 ordering: append the turn to the authoritative JSONL history FIRST, then update the
    // metadata aggregates. A crash between the two leaves the turn in history but not yet in the
    // aggregates cache — recoverable via `recompute_aggregates`.
    let commit = match thread_metadata
        .append_turn_with_diffs(project_id, thread_id, turn, captured_diffs)
        .await
    {
        Ok(commit) => commit,
        Err(e) => {
            warn!(
                %project_id,
                %thread_id,
                turn = %turn_id,
                status = ?status_kind,
                item_count,
                diff_count,
                %e,
                "failed to append turn to history; skipping metadata update"
            );
            return PersistTurnOutcome {
                history_error: Some(e.to_string()),
                ..PersistTurnOutcome::default()
            };
        }
    };
    info!(
        %project_id,
        %thread_id,
        turn = %turn_id,
        status = ?status_kind,
        item_count,
        diff_count,
        started_at = %rfc3339(&started_at),
        completed_at = rfc3339_opt(completed_at.as_ref()),
        "appended completed turn to history"
    );

    match commit {
        TurnCommitOutcome::MetadataMutation(
            ThreadMutation::Changed { .. } | ThreadMutation::Unchanged { .. },
        ) => {}
        TurnCommitOutcome::MetadataMutation(ThreadMutation::Missing) => {
            warn!(
                %project_id,
                %thread_id,
                turn = %turn_id,
                "thread file missing on turn completion after history append"
            );
            return PersistTurnOutcome {
                history_appended: true,
                metadata_updated: false,
                history_error: None,
            };
        }
        TurnCommitOutcome::MetadataFailed(e) => {
            warn!(
                %project_id,
                %thread_id,
                turn = %turn_id,
                %e,
                "failed to persist thread metadata on turn completion after history append"
            );
            return PersistTurnOutcome {
                history_appended: true,
                metadata_updated: false,
                history_error: None,
            };
        }
    }
    info!(
        %project_id,
        %thread_id,
        turn = %turn_id,
        status = ?status_kind,
        should_record_usage = should_record,
        "updated thread metadata for completed turn"
    );

    // Fold the same usage into the project + global ledgers via the single-writer actor (§10.2).
    if should_record {
        let date = Utc::now().format("%Y-%m-%d").to_string();
        match model.into_known() {
            Some(model) => {
                ledger
                    .record(project_id, date, model.provider, model.model, usage)
                    .await;
            }
            None => ledger.record_unattributed(project_id, date, usage).await,
        }
    }

    PersistTurnOutcome {
        history_appended: true,
        metadata_updated: true,
        history_error: None,
    }
}

fn external_turn_defaults(
    binding: &LoadedThreadBinding,
    persisted: Option<&ThreadFile>,
) -> ExternalTurnDefaults {
    ExternalTurnDefaults {
        model: persisted
            .map(|thread| thread.current_model.clone())
            .or_else(|| binding.native_model.clone().map(TurnModel::Known))
            .unwrap_or(TurnModel::Unknown),
        mode: persisted
            .map(|thread| thread.mode)
            .unwrap_or(TurnMode::Unknown),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ForwarderExitReason {
    StreamEndedRecovered,
    StreamEndedWithoutTurn,
    PersistenceBlocked,
    EventPreparationFailed,
    RuntimeAuthorityReplaced,
}

enum ForwarderControl {
    Continue,
    Exit(ForwarderExitReason),
}

pub(super) fn forwarder_exit_reason_label(reason: ForwarderExitReason) -> &'static str {
    match reason {
        ForwarderExitReason::StreamEndedRecovered => "stream_ended_recovered",
        ForwarderExitReason::StreamEndedWithoutTurn => "stream_ended_without_turn",
        ForwarderExitReason::PersistenceBlocked => "persistence_blocked",
        ForwarderExitReason::EventPreparationFailed => "event_preparation_failed",
        ForwarderExitReason::RuntimeAuthorityReplaced => "runtime_authority_replaced",
    }
}

struct ForwardedTurnState {
    context: TurnContext,
    lease: Option<ThreadTurnLease>,
    observed_turn: Option<TurnId>,
    owned_turn: Option<TurnId>,
    started_at: chrono::DateTime<Utc>,
    items: CurrentTurnItems,
    diffs: Vec<giskard_core::FileDiff>,
    seen_notices: HashSet<(Option<TurnId>, String)>,
    item_ids_by_harness: HashMap<HarnessItemKey, ItemId>,
    saw_context_compaction_marker: bool,
    live_usage: Option<giskard_core::token::TokenUsage>,
    // Reserved for the documented additive `Turn.context_window` persistence extension.
    live_context_window: Option<u32>,
    persisted_context_window: Option<u32>,
}

impl ForwardedTurnState {
    fn new(context: TurnContext) -> Self {
        Self {
            context,
            lease: None,
            observed_turn: None,
            owned_turn: None,
            started_at: Utc::now(),
            items: CurrentTurnItems::default(),
            diffs: Vec::new(),
            seen_notices: HashSet::new(),
            item_ids_by_harness: HashMap::new(),
            saw_context_compaction_marker: false,
            live_usage: None,
            live_context_window: None,
            persisted_context_window: None,
        }
    }

    fn reset(&mut self, idle_context: &TurnContext) {
        self.context = idle_context.clone();
        self.lease = None;
        self.observed_turn = None;
        self.owned_turn = None;
        self.started_at = Utc::now();
        self.items = CurrentTurnItems::default();
        self.diffs.clear();
        self.seen_notices.clear();
        self.item_ids_by_harness.clear();
        self.saw_context_compaction_marker = false;
        self.live_usage = None;
        self.live_context_window = None;
        self.persisted_context_window = None;
    }
}

struct AdmittedIntent {
    context: TurnContext,
    lease: ThreadTurnLease,
}

enum IntentReply {
    Turn(oneshot::Sender<Result<TurnId, HarnessError>>),
    Unit(oneshot::Sender<Result<(), HarnessError>>),
}

struct InflightRequest {
    request: futures::future::BoxFuture<'static, Result<Option<TurnId>, HarnessError>>,
    reply: IntentReply,
    context: TurnContext,
    started: Instant,
}

/// Owns event reduction for one installed coordinator without owning its lifecycle.
pub(super) struct ThreadEventForwarder {
    shared: Arc<RegistryShared>,
    authority: Arc<ThreadAuthority>,
    coordinator: Arc<ThreadCoordinator>,
    harness: Weak<dyn AgentHarness>,
    binding: LoadedThreadBinding,
    stream: giskard_harness::AgentEventStream,
    cancel: watch::Receiver<bool>,
    intents: mpsc::Receiver<TurnIntent>,
    driver: DriverHandle,
    intents_closed: bool,
    admitted: Option<AdmittedIntent>,
    inflight: Option<InflightRequest>,
    idle_context: TurnContext,
    turn: ForwardedTurnState,
    seen_turn_ids: HashSet<TurnId>,
    forwarder_started: Instant,
    stream_error: Option<String>,
}

impl ThreadEventForwarder {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn new(
        shared: Arc<RegistryShared>,
        authority: Arc<ThreadAuthority>,
        coordinator: Arc<ThreadCoordinator>,
        harness: Weak<dyn AgentHarness>,
        stream: giskard_harness::AgentEventStream,
        cancel: watch::Receiver<bool>,
        intents: mpsc::Receiver<TurnIntent>,
        driver: DriverHandle,
    ) -> Self {
        let binding = coordinator.binding().await;
        let thread_id = binding.handle.thread;
        let project_id = binding.project_id;
        let persisted = shared
            .store
            .load_thread(project_id, thread_id)
            .await
            .ok()
            .flatten();
        let idle_defaults = external_turn_defaults(&binding, persisted.as_ref());
        let idle_context = TurnContext {
            user_input: UserInput::text(""),
            model: idle_defaults.model,
            mode: idle_defaults.mode,
            kind: TurnContextKind::User,
        };
        let runtime = shared.runtime.clone();
        // Establish the authority once. Per-event permits must only observe this entry, never recreate
        // it after retirement.
        drop(runtime.restoration_permit(&authority));
        let seen_turn_ids = persisted_turn_ids(&shared.store, project_id, thread_id).await;
        let forwarder_started = Instant::now();
        let turn = ForwardedTurnState::new(idle_context.clone());
        debug!(
            %project_id,
            %thread_id,
            context_kind = turn_context_kind_label(turn.context.kind),
            mode = ?turn.context.mode,
            model = ?turn.context.model,
            turn_gate_held = turn.lease.as_ref().is_some_and(|lease| !lease.is_released()),
            persisted_turn_count = seen_turn_ids.len(),
            "event forwarder started"
        );
        Self {
            shared,
            authority,
            coordinator,
            harness,
            binding,
            stream,
            cancel,
            intents,
            driver,
            intents_closed: false,
            admitted: None,
            inflight: None,
            idle_context,
            turn,
            seen_turn_ids,
            forwarder_started,
            stream_error: None,
        }
    }

    pub(super) fn thread_id(&self) -> ThreadId {
        self.binding.handle.thread
    }

    pub(super) async fn run(mut self) -> ForwarderExitReason {
        let exit_reason = loop {
            enum Step {
                Cancelled(Result<(), tokio::sync::watch::error::RecvError>),
                Intent(Option<TurnIntent>),
                Answered(Result<Option<TurnId>, HarnessError>),
                Event(Result<AgentEvent, EventStreamError>),
            }
            // Retained events intentionally precede harness answers: a browser reply may wait for
            // the bounded event backlog, preserving native event order before admitting new work.
            let step = tokio::select! {
                biased;
                changed = self.cancel.changed() => {
                    Step::Cancelled(changed)
                }
                result = self.stream.recv() => Step::Event(result),
                outcome = async {
                    match self.inflight.as_mut() {
                        Some(request) => request.request.as_mut().await,
                        None => std::future::pending().await,
                    }
                }, if self.inflight.is_some() => Step::Answered(outcome),
                intent = self.intents.recv(), if !self.intents_closed => Step::Intent(intent),
            };
            match step {
                Step::Cancelled(changed) => {
                    if changed.is_err() || *self.cancel.borrow() {
                        break ForwarderExitReason::StreamEndedWithoutTurn;
                    }
                }
                Step::Intent(Some(intent)) => self.admit_intent(intent).await,
                Step::Intent(None) => self.intents_closed = true,
                Step::Answered(outcome) => self.handle_answer(outcome).await,
                Step::Event(Ok(event)) => match self.handle_event(event).await {
                    ForwarderControl::Continue => continue,
                    ForwarderControl::Exit(reason) => break reason,
                },
                Step::Event(Err(e)) => match self.handle_stream_error(e).await {
                    ForwarderControl::Continue => continue,
                    ForwarderControl::Exit(reason) => break reason,
                },
            }
        };
        self.finish(exit_reason).await
    }
}

impl ThreadEventForwarder {
    async fn admit_intent(&mut self, intent: TurnIntent) {
        let thread_id = self.thread_id();
        let project_id = self.binding.project_id;
        let reject = |intent: TurnIntent, error| match intent {
            TurnIntent::StartTurn { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            TurnIntent::Compact { reply, .. } => {
                let _ = reply.send(Err(error));
            }
        };
        if *self.cancel.borrow() {
            reject(
                intent,
                HarnessError::Protocol(format!("thread {thread_id} has no live event owner")),
            );
            return;
        }
        if self.coordinator.classification().await != ClassificationPhase::Primary {
            reject(intent, HarnessError::ThreadReadOnly { thread: thread_id });
            return;
        }
        if self.inflight.is_some() || self.admitted.is_some() || self.turn.owned_turn.is_some() {
            reject(intent, HarnessError::ThreadBusy { thread: thread_id });
            return;
        }
        let context = match &intent {
            TurnIntent::StartTurn { context, .. } | TurnIntent::Compact { context, .. } => {
                context.clone()
            }
        };
        let lease = match self.shared.runtime.reserve_turn(
            &self.authority,
            turn_reservation(project_id, &self.binding.handle, &context),
        ) {
            Ok(lease) => lease,
            Err(error) => {
                reject(intent, error);
                return;
            }
        };
        publish_runtime_overview(&self.shared).await;
        let Some(harness) = self.harness.upgrade() else {
            let mut lease = lease;
            if let Some(overview) = lease.release() {
                self.shared.hub.publish_runtime_overview(overview).await;
            }
            reject(
                intent,
                HarnessError::Protocol("project harness is gone".into()),
            );
            return;
        };
        self.admitted = Some(AdmittedIntent {
            context: context.clone(),
            lease,
        });
        let handle = self.binding.handle.clone();
        let started = Instant::now();
        self.inflight = Some(match intent {
            TurnIntent::StartTurn {
                input,
                overrides,
                reply,
                ..
            } => {
                info!(%project_id, %thread_id, harness_thread_id = %handle.harness_thread_id,
                    mode = ?overrides.mode, model = ?context.model, "starting harness turn");
                InflightRequest {
                    request: Box::pin(async move {
                        harness
                            .start_turn(&handle, input, overrides)
                            .await
                            .map(Some)
                    }),
                    reply: IntentReply::Turn(reply),
                    context,
                    started,
                }
            }
            TurnIntent::Compact { reply, .. } => {
                info!(%project_id, %thread_id, harness_thread_id = %handle.harness_thread_id,
                    mode = ?context.mode, model = ?context.model, "starting context compaction");
                InflightRequest {
                    request: Box::pin(async move {
                        harness.compact_thread(&handle).await.map(|()| None)
                    }),
                    reply: IntentReply::Unit(reply),
                    context,
                    started,
                }
            }
        });
    }

    async fn handle_answer(&mut self, outcome: Result<Option<TurnId>, HarnessError>) {
        let Some(request) = self.inflight.take() else {
            return;
        };
        match outcome {
            Ok(turn_id) => {
                match &request.reply {
                    IntentReply::Turn(_) => info!(
                        project_id = %self.binding.project_id,
                        thread_id = %self.thread_id(),
                        harness_thread_id = %self.binding.handle.harness_thread_id,
                        turn_id = display_opt(turn_id),
                        mode = ?request.context.mode,
                        model = ?request.context.model,
                        ack_elapsed_ms = request.started.elapsed().as_millis(),
                        "harness accepted turn start request"
                    ),
                    IntentReply::Unit(_) => info!(
                        project_id = %self.binding.project_id,
                        thread_id = %self.thread_id(),
                        harness_thread_id = %self.binding.handle.harness_thread_id,
                        mode = ?request.context.mode,
                        model = ?request.context.model,
                        ack_elapsed_ms = request.started.elapsed().as_millis(),
                        "harness accepted context compaction request"
                    ),
                }
                if let Some(admitted) = self.admitted.as_mut()
                    && let Some(id) = turn_id
                    && let Some(overview) = admitted.lease.acknowledge_turn(id)
                {
                    self.shared.hub.publish_runtime_overview(overview).await;
                }
                if let (Some(owned), Some(id)) = (self.turn.owned_turn, turn_id)
                    && owned != id
                {
                    warn!(thread_id = %self.thread_id(), %owned, harness_turn = %id,
                    elapsed_ms = request.started.elapsed().as_millis(),
                    "harness named a different turn than the one already attached");
                }
                match request.reply {
                    IntentReply::Turn(reply) => {
                        let result = turn_id.ok_or_else(|| {
                            HarnessError::Protocol("turn start returned no turn id".into())
                        });
                        let _ = reply.send(result);
                    }
                    IntentReply::Unit(reply) => {
                        let _ = reply.send(Ok(()));
                    }
                }
            }
            Err(error) => {
                match &request.reply {
                    IntentReply::Turn(_) => warn!(
                        project_id = %self.binding.project_id,
                        thread_id = %self.thread_id(),
                        harness_thread_id = %self.binding.handle.harness_thread_id,
                        mode = ?request.context.mode,
                        model = ?request.context.model,
                        %error,
                        ack_elapsed_ms = request.started.elapsed().as_millis(),
                        "harness rejected turn start request"
                    ),
                    IntentReply::Unit(_) => warn!(
                        project_id = %self.binding.project_id,
                        thread_id = %self.thread_id(),
                        harness_thread_id = %self.binding.handle.harness_thread_id,
                        mode = ?request.context.mode,
                        model = ?request.context.model,
                        %error,
                        ack_elapsed_ms = request.started.elapsed().as_millis(),
                        "harness rejected context compaction request"
                    ),
                }
                if let Some(mut admitted) = self.admitted.take()
                    && let Some(overview) = admitted.lease.release()
                {
                    self.shared.hub.publish_runtime_overview(overview).await;
                }
                match request.reply {
                    IntentReply::Turn(reply) => {
                        let _ = reply.send(Err(error));
                    }
                    IntentReply::Unit(reply) => {
                        let _ = reply.send(Err(error));
                    }
                }
            }
        }
    }

    async fn finish(&mut self, exit_reason: ForwarderExitReason) -> ForwarderExitReason {
        let thread_id = self.thread_id();
        let project_id = self.binding.project_id;
        // Closing first makes the cancellation fence cover every sender clone: intents already
        // queued are rejected below, while blocked and future sends fail at the sender boundary.
        self.intents.close();
        while let Ok(intent) = self.intents.try_recv() {
            let error =
                HarnessError::Protocol(format!("thread {thread_id} has no live event owner"));
            match intent {
                TurnIntent::StartTurn { reply, .. } => {
                    let _ = reply.send(Err(error));
                }
                TurnIntent::Compact { reply, .. } => {
                    let _ = reply.send(Err(error));
                }
            }
        }
        if let Some(mut admitted) = self.admitted.take()
            && let Some(overview) = admitted.lease.release()
        {
            self.shared.hub.publish_runtime_overview(overview).await;
        }
        if let Some(request) = self.inflight.take() {
            let error =
                HarnessError::Protocol("event owner exited before the harness answered".into());
            match request.reply {
                IntentReply::Turn(reply) => {
                    let _ = reply.send(Err(error));
                }
                IntentReply::Unit(reply) => {
                    let _ = reply.send(Err(error));
                }
            }
        }
        let turn_gate_held = self
            .turn
            .lease
            .as_ref()
            .is_some_and(|lease| !lease.is_released());
        if turn_gate_held {
            warn!(
                %project_id,
                %thread_id,
                context_kind = turn_context_kind_label(self.turn.context.kind),
                mode = ?self.turn.context.mode,
                model = ?self.turn.context.model,
                owned_turn = display_opt(self.turn.owned_turn),
                turn_id = display_opt(self.turn.observed_turn),
                exit_reason = forwarder_exit_reason_label(exit_reason),
                stream_error = display_opt(self.stream_error.as_deref()),
                items_buffered = self.turn.items.len(),
                diffs_buffered = self.turn.diffs.len(),
                saw_context_compaction_marker = self.turn.saw_context_compaction_marker,
                elapsed_ms = self.forwarder_started.elapsed().as_millis(),
                "event forwarder exited without turn completion; releasing active-turn ownership"
            );
        } else {
            debug!(
                %project_id,
                %thread_id,
                context_kind = turn_context_kind_label(self.turn.context.kind),
                owned_turn = display_opt(self.turn.owned_turn),
                turn_id = display_opt(self.turn.observed_turn),
                turn_gate_held,
                exit_reason = forwarder_exit_reason_label(exit_reason),
                stream_error = display_opt(self.stream_error.as_deref()),
                elapsed_ms = self.forwarder_started.elapsed().as_millis(),
                "event forwarder exited"
            );
        }
        if let Some(turn_gate) = self.turn.lease.as_mut()
            && let Some(overview) = turn_gate.release()
        {
            self.shared.hub.publish_runtime_overview(overview).await;
        }
        exit_reason
    }

    async fn handle_stream_error(&mut self, e: EventStreamError) -> ForwarderControl {
        let thread_id = self.thread_id();
        let project_id = self.binding.project_id;
        let hub = self.shared.hub.clone();
        let runtime = self.shared.runtime.clone();
        let gap = matches!(&e, EventStreamError::Gap { .. });
        self.stream_error = Some(e.to_string());
        if self.turn.context.kind == TurnContextKind::ManualCompaction {
            let live_buffer_active = runtime.live_is_active(&self.authority);
            warn!(
                %project_id,
                %thread_id,
                ?e,
                owned_turn = display_opt(self.turn.owned_turn),
                turn_id = display_opt(self.turn.observed_turn),
                saw_context_compaction_marker = self.turn.saw_context_compaction_marker,
                items_buffered = self.turn.items.len(),
                live_buffer_active,
                turn_gate_held = self.turn.lease.is_some(),
                elapsed_ms = self.forwarder_started.elapsed().as_millis(),
                "context compaction event stream ended before completion"
            );
        } else {
            debug!(%thread_id, ?e, "event stream ended");
        }
        if let Some(incomplete_turn) = self.turn.observed_turn.or(self.turn.owned_turn) {
            let live_buffer_active = runtime.live_is_active(&self.authority);
            let turn_gate_held = self
                .turn
                .lease
                .as_ref()
                .is_some_and(|lease| !lease.is_released());
            let status = TurnStatus {
                kind: TurnStatusKind::Interrupted,
                message: Some(if gap {
                    "Harness event log overflowed before turn completion".into()
                } else {
                    "Harness event stream ended before turn completion".into()
                }),
            };
            warn!(
                %project_id,
                %thread_id,
                turn = %incomplete_turn,
                context_kind = turn_context_kind_label(self.turn.context.kind),
                mode = ?self.turn.context.mode,
                model = ?self.turn.context.model,
                owned_turn = display_opt(self.turn.owned_turn),
                turn_id = display_opt(self.turn.observed_turn),
                stream_error = display_opt(self.stream_error.as_deref()),
                items_buffered = self.turn.items.len(),
                diffs_buffered = self.turn.diffs.len(),
                live_buffer_active,
                turn_gate_held,
                elapsed_ms = self.forwarder_started.elapsed().as_millis(),
                "persisting incomplete turn after event stream ended"
            );
            let completion_event = AgentEvent::TurnCompleted {
                thread: thread_id,
                turn: incomplete_turn,
                usage: self.turn.live_usage.unwrap_or_default(),
                status: status.clone(),
            };
            let Some(_) = self
                .complete_forwarded_turn(
                    incomplete_turn,
                    self.turn.live_usage.unwrap_or_default(),
                    status,
                )
                .await
            else {
                return ForwarderControl::Exit(ForwarderExitReason::PersistenceBlocked);
            };
            hub.broadcast_event(thread_id, completion_event).await;
            if gap {
                self.turn.reset(&self.idle_context);
                self.stream_error = None;
                return ForwarderControl::Continue;
            }
            ForwarderControl::Exit(ForwarderExitReason::StreamEndedRecovered)
        } else if let EventStreamError::Gap { dropped } = e {
            error!(
                %project_id,
                %thread_id,
                dropped,
                "event log overflowed while idle; N events dropped"
            );
            self.stream_error = None;
            ForwarderControl::Continue
        } else {
            ForwarderControl::Exit(ForwarderExitReason::StreamEndedWithoutTurn)
        }
    }

    async fn handle_event(&mut self, event: AgentEvent) -> ForwarderControl {
        let thread_id = self.thread_id();
        let project_id = self.binding.project_id;
        let hub = self.shared.hub.clone();
        let runtime = self.shared.runtime.clone();
        let event_thread = event.thread_id();
        if event_thread != thread_id {
            log_foreign_thread_event_drop(project_id, thread_id, event_thread, &event);
            return ForwarderControl::Continue;
        }

        if should_skip_duplicate_notice(&event, &mut self.turn.seen_notices) {
            debug!(
                %project_id,
                %thread_id,
                event_turn_id = display_opt(event_turn_id(&event)),
                "skipping duplicate harness notice"
            );
            return ForwarderControl::Continue;
        }

        if let Some((event_turn, harness_item_id, existing_item_id, conflicting_item_id)) =
            track_item_identity(&mut self.turn.item_ids_by_harness, &event)
        {
            error!(
                %project_id,
                %thread_id,
                turn_id = %event_turn,
                event_kind = event_kind(&event),
                harness_item_id,
                existing_item_id = %existing_item_id,
                conflicting_item_id = %conflicting_item_id,
                "dropping harness event because a native item id remapped to a different Giskard item id"
            );
            return ForwarderControl::Continue;
        }

        let event_turn = event_turn_id(&event);
        if let Some(owned) = self.turn.owned_turn {
            if let Some(turn) = event_turn {
                // A command may outlive its persisted turn. Its terminal replacement must
                // still reach the late-event path while a newer turn is active; it updates
                // runtime task state only and cannot enter the newer turn's transcript.
                // Events for any other non-owned, non-persisted turn remain a protocol
                // violation and are dropped before they mutate runtime or persistence.
                if turn != owned && !self.seen_turn_ids.contains(&turn) {
                    log_cross_turn_event_drop(
                        project_id,
                        thread_id,
                        owned,
                        turn,
                        &event,
                        self.forwarder_started.elapsed().as_millis(),
                    );
                    return ForwarderControl::Continue;
                }
            }
        } else if let Some(turn) = event_turn
            && !self.seen_turn_ids.contains(&turn)
        {
            let (context, mut lease) = if let Some(admitted) = self.admitted.take() {
                (admitted.context, admitted.lease)
            } else {
                let persisted = self
                    .shared
                    .store
                    .load_thread(project_id, thread_id)
                    .await
                    .ok()
                    .flatten();
                let defaults = external_turn_defaults(&self.binding, persisted.as_ref());
                let classification = self.coordinator.classification().await;
                let context = TurnContext {
                    user_input: external_turn_input_label(classification),
                    model: defaults.model,
                    mode: defaults.mode,
                    kind: match classification {
                        ClassificationPhase::Primary => TurnContextKind::User,
                        ClassificationPhase::Subagent => TurnContextKind::ExternalSubagent,
                        ClassificationPhase::Orphan => TurnContextKind::ExternalOrphan,
                    },
                };
                let lease = match runtime.reserve_turn(
                    &self.authority,
                    turn_reservation(project_id, &self.binding.handle, &context),
                ) {
                    Ok(lease) => lease,
                    Err(error) => {
                        error!(%project_id, %thread_id, %turn, %error,
                            "event owner could not reserve an external native turn");
                        return ForwarderControl::Exit(
                            ForwarderExitReason::RuntimeAuthorityReplaced,
                        );
                    }
                };
                (context, lease)
            };
            if let Some(overview) = lease.acknowledge_turn(turn) {
                self.shared.hub.publish_runtime_overview(overview).await;
            }
            self.turn.context = context;
            self.turn.lease = Some(lease);
            self.turn.owned_turn = Some(turn);
            if !matches!(event, AgentEvent::TurnStarted { .. }) {
                debug!(
                    %thread_id,
                    %turn,
                    "event forwarder attached to turn before seeing turn start"
                );
            }
        }

        // Normalize every admitted completed-item payload once before runtime state, wire
        // projection, current-turn assembly, or persistence can observe it. Command
        // terminality is handled separately: providers may send a nonterminal
        // ItemCompleted followed by a later terminal replacement.
        let is_completed_addressable_output = matches!(
            &event,
            AgentEvent::ItemCompleted { item, .. }
                if matches!(&item.payload, ItemPayload::CommandExecution { .. } | ItemPayload::ToolCall { .. })
        );
        let (event, prepared_item_output, preparation_permit) = if is_completed_addressable_output {
            let Some(permit) = runtime.event_application_permit(&self.authority) else {
                return ForwarderControl::Exit(ForwarderExitReason::RuntimeAuthorityReplaced);
            };
            let preparation_diagnostics = completed_item_diagnostics(&event);
            let preparation_runtime = runtime.clone();
            match tokio::task::spawn_blocking(move || {
                preparation_runtime.prepare_item_output(event)
            })
            .await
            {
                Ok((event, prepared)) => (event, prepared, Some(permit)),
                Err(error) => {
                    tracing::error!(
                        %project_id,
                        %thread_id,
                        self.turn.observed_turn = preparation_diagnostics.as_ref().map(|value| tracing::field::display(value.turn_id)),
                        item_id = preparation_diagnostics.as_ref().map(|value| tracing::field::display(value.item_id)),
                        harness_item_id = preparation_diagnostics.as_ref().map(|value| value.harness_item_id.as_str()),
                        item_payload_kind = preparation_diagnostics.as_ref().map(|value| value.payload_kind),
                        error = %error,
                        "addressable item-output event preparation task failed"
                    );
                    return ForwarderControl::Exit(ForwarderExitReason::EventPreparationFailed);
                }
            }
        } else {
            (event, None, None)
        };

        if let Some(turn) = event_turn
            && self.seen_turn_ids.contains(&turn)
        {
            if matches!(event, AgentEvent::TurnUsageUpdated { .. }) {
                debug!(
                    %project_id,
                    %thread_id,
                    %turn,
                    "ignoring usage update for an already-persisted turn"
                );
                return ForwarderControl::Continue;
            }
            let command_state_changed = if is_terminal_command_completion(&event) {
                let before = terminating_command_before_terminal_completion(
                    &runtime,
                    &self.authority,
                    &event,
                )
                .await;
                let applied = match preparation_permit.as_ref() {
                    Some(permit) => match self.shared.runtime.apply_prepared_event_if_current(
                        permit,
                        &event,
                        false,
                        prepared_item_output,
                    ) {
                        Some(applied) => applied,
                        None => {
                            return ForwarderControl::Exit(
                                ForwarderExitReason::RuntimeAuthorityReplaced,
                            );
                        }
                    },
                    None => self.shared.runtime.apply_prepared_event(
                        &self.authority,
                        &event,
                        false,
                        prepared_item_output,
                    ),
                };
                if let AgentEvent::ItemCompleted { turn, item, .. } = &event {
                    self.shared
                        .runtime
                        .remove_command_output(&self.authority, *turn, item.id);
                    warn!(
                        %project_id,
                        %thread_id,
                        %turn,
                        item_id = %item.id,
                        harness_item_id = %item.harness_item_id,
                        "deferred durable command-output update for already-persisted turn"
                    );
                }
                log_command_completion_after_terminate(project_id, before.as_ref(), &event);
                debug!(
                    %thread_id,
                    event_sequence = display_opt(applied.sequence),
                    event_kind = event_kind(&event),
                    "applied late terminal event to thread runtime"
                );
                let changed = applied.tasks_changed;
                publish_applied_runtime_effects(&hub, thread_id, applied).await;
                changed
            } else {
                log_ignored_seen_turn_running_task_start(project_id, &event);
                false
            };
            if is_terminal_command_completion(&event) {
                if !command_state_changed
                    && let AgentEvent::ItemCompleted { turn, item, .. } = &event
                {
                    warn!(
                        %project_id,
                        %thread_id,
                        %turn,
                        item_id = %item.id,
                        harness_item_id = %item.harness_item_id,
                        "broadcasting terminal command completion for a persisted turn without matching running-task state"
                    );
                }
                if let Some(message) = late_command_completion_message(thread_id, event.clone()) {
                    hub.broadcast(thread_id, message).await;
                }
            }
            if let AgentEvent::ItemCompleted { turn, item, .. } = &event
                && let ItemPayload::ToolCall { name, server, .. } = &item.payload
            {
                self.shared
                    .runtime
                    .remove_tool_output(&self.authority, *turn, item.id);
                if completed_tool_has_terminal_output(item) {
                    warn!(
                        %project_id,
                        %thread_id,
                        %turn,
                        item_id = %item.id,
                        harness_item_id = %item.harness_item_id,
                        tool_name = %name,
                        tool_server = server.as_deref(),
                        "ignoring completed tool output for an already-persisted turn"
                    );
                }
            }
            return ForwarderControl::Continue;
        }

        if self.turn.owned_turn.is_none() && event_turn.is_none() {
            let applied = self
                .shared
                .runtime
                .apply_event(&self.authority, &event, false);
            debug!(
                %thread_id,
                event_sequence = display_opt(applied.sequence),
                event_kind = event_kind(&event),
                "applied turnless agent event to thread runtime"
            );
            publish_applied_runtime_effects(&hub, thread_id, applied).await;
            match &event {
                AgentEvent::Error { error, .. } => {
                    warn!(
                        %project_id,
                        %thread_id,
                        error = %error,
                        turn_gate_held = self.turn.lease
                            .as_ref()
                            .is_some_and(|lease| !lease.is_released()),
                        elapsed_ms = self.forwarder_started.elapsed().as_millis(),
                        "turnless harness error received before turn ownership"
                    );
                    hub.broadcast_event(thread_id, event.clone()).await;
                }
                AgentEvent::Notice { message, .. } => {
                    debug!(
                        %project_id,
                        %thread_id,
                        message,
                        turn_gate_held = self.turn.lease
                            .as_ref()
                            .is_some_and(|lease| !lease.is_released()),
                        elapsed_ms = self.forwarder_started.elapsed().as_millis(),
                        "turnless harness notice received before turn ownership"
                    );
                    hub.broadcast_event(thread_id, event.clone()).await;
                }
                AgentEvent::ServerRequestReceived { request, .. } => {
                    warn!(
                        %project_id,
                        %thread_id,
                        request_id = %request.id,
                        method = %request.method,
                        turn_gate_held = self.turn.lease
                            .as_ref()
                            .is_some_and(|lease| !lease.is_released()),
                        elapsed_ms = self.forwarder_started.elapsed().as_millis(),
                        "turnless server request received before turn ownership"
                    );
                    hub.broadcast_event(thread_id, event.clone()).await;
                }
                _ => {}
            }
            return ForwarderControl::Continue;
        }

        // Only admitted events may mutate lazy diff storage. Extract bodies after the
        // wrong-turn and already-persisted-turn exits, but before reconnect state,
        // persistence assembly, or browser projection can observe the event.
        let event = runtime.capture_event_diffs(&self.authority, event);

        if let AgentEvent::TurnUsageUpdated {
            turn,
            usage,
            model,
            context_window,
            ..
        } = &event
        {
            self.turn.live_usage = Some(*usage);
            if let Some(window) = context_window {
                self.turn.live_context_window = Some(*window);
            }
            if let (Some(model), Some(window)) = (model, context_window) {
                if self.turn.context.model.as_known().is_some_and(|expected| {
                    model.provider != expected.provider || model.model != expected.model
                }) {
                    error!(
                        %project_id,
                        %thread_id,
                        turn = %turn,
                        expected_model = ?self.turn.context.model,
                        event_provider = %model.provider,
                        event_model = %model.model,
                        "skipping model context-window persistence for the wrong turn model"
                    );
                } else {
                    if self.turn.context.model.as_known().is_none() {
                        self.turn.context.model = TurnModel::Known(model.clone());
                    }
                    if self.turn.persisted_context_window != Some(*window) {
                        persist_model_context_window(
                            &self.shared.thread_metadata,
                            project_id,
                            thread_id,
                            *turn,
                            model,
                            *window,
                        )
                        .await;
                        self.turn.persisted_context_window = Some(*window);
                    }
                }
            }
        }

        match &event {
            AgentEvent::TurnStarted { turn, .. } => {
                self.turn.observed_turn = Some(*turn);
                self.turn.started_at = Utc::now();
                self.turn.items.rebuild_indexes();
                if self.turn.context.kind == TurnContextKind::ManualCompaction {
                    info!(
                        %project_id,
                        %thread_id,
                        %turn,
                        elapsed_ms = self.forwarder_started.elapsed().as_millis(),
                        "context compaction turn started"
                    );
                }
            }
            AgentEvent::ItemStarted { item, turn, .. } => {
                if let Some(info) = subagent_start_info(item)
                    && let Err(error) = self
                        .driver
                        .link(Link {
                            parent_thread_id: thread_id,
                            spawned_by_turn_id: *turn,
                            item_id: item.id,
                            origin: "item_started",
                            info,
                            reply: None,
                        })
                        .await
                {
                    warn!(%project_id, parent_thread_id = %thread_id, turn_id = %turn,
                        item_id = %item.id, %error,
                        "failed to send linked native identity to the project event driver");
                }
            }
            AgentEvent::ItemCompleted { item, turn, .. } => {
                if let Some(info) = subagent_activity_info(item)
                    && let Err(error) = self
                        .driver
                        .link(Link {
                            parent_thread_id: thread_id,
                            spawned_by_turn_id: *turn,
                            item_id: item.id,
                            origin: "item_completed",
                            info,
                            reply: None,
                        })
                        .await
                {
                    warn!(%project_id, parent_thread_id = %thread_id, turn_id = %turn,
                        item_id = %item.id, %error,
                        "failed to send linked native identity to the project event driver");
                }
                if self.turn.context.kind == TurnContextKind::ManualCompaction
                    && is_context_compaction_item(item)
                {
                    self.turn.saw_context_compaction_marker = true;
                    info!(
                        %project_id,
                        %thread_id,
                        %turn,
                        turn_started_seen = self.turn.observed_turn.is_some(),
                        will_synthesize_completion = self.turn.observed_turn.is_none(),
                        items_buffered_after = self.turn.items.len() + 1,
                        elapsed_ms = self.forwarder_started.elapsed().as_millis(),
                        "context compaction marker received"
                    );
                }
                if self.turn.items.upsert(item) {
                    error!(
                        %project_id,
                        %thread_id,
                        %turn,
                        item_id = %item.id,
                        harness_item_id = %item.harness_item_id,
                        "recovered stale current-turn item index"
                    );
                }
            }
            AgentEvent::DiffUpdated { diff, .. } => {
                let existing = self.turn.diffs.iter_mut().find(|d| d.path == diff.path);
                if let Some(existing) = existing {
                    *existing = diff.clone();
                } else {
                    self.turn.diffs.push(diff.clone());
                }
            }
            _ => {}
        }

        let completed = if let AgentEvent::TurnCompleted {
            turn,
            usage,
            status,
            ..
        } = &event
        {
            Some((*turn, *usage, status.clone()))
        } else {
            None
        };

        // A harness may deliver an item for an unseen turn before TurnStarted. Start the
        // reconnect buffer from the first turn-scoped event and reuse it when the delayed
        // start arrives, otherwise a reload in that window loses the already-visible item.
        let mut append_to_live_buffer = true;
        if let Some(buffer_turn) = event_turn
            && let Err(existing_turn) = runtime.ensure_live_turn(
                &self.authority,
                buffer_turn,
                live_turn_user_input(&self.turn.context),
            )
        {
            if matches!(event, AgentEvent::TurnStarted { .. }) {
                warn!(
                    %project_id,
                    %thread_id,
                    %buffer_turn,
                    %existing_turn,
                    "replacing a stale live buffer when a new turn started"
                );
                runtime.replace_live_turn(
                    &self.authority,
                    buffer_turn,
                    live_turn_user_input(&self.turn.context),
                );
            } else {
                error!(
                    %project_id,
                    %thread_id,
                    %buffer_turn,
                    %existing_turn,
                    event_kind = event_kind(&event),
                    "not buffering an event for a different turn; live delivery and persistence continue"
                );
                append_to_live_buffer = false;
            }
        }
        if completed.is_none() {
            let applied = match preparation_permit.as_ref() {
                Some(permit) => {
                    match self.shared.runtime.apply_prepared_event_if_current(
                        permit,
                        &event,
                        append_to_live_buffer,
                        prepared_item_output,
                    ) {
                        Some(applied) => applied,
                        None => {
                            return ForwarderControl::Exit(
                                ForwarderExitReason::RuntimeAuthorityReplaced,
                            );
                        }
                    }
                }
                None => self.shared.runtime.apply_prepared_event(
                    &self.authority,
                    &event,
                    append_to_live_buffer,
                    prepared_item_output,
                ),
            };
            debug!(
                %thread_id,
                event_sequence = display_opt(applied.sequence),
                event_kind = event_kind(&event),
                "applied agent event to thread runtime"
            );
            publish_applied_runtime_effects(&hub, thread_id, applied).await;
        }

        if let Some((completed_turn, usage, status)) = completed {
            info!(
                %project_id,
                %thread_id,
                turn = %completed_turn,
                started_turn = display_opt(self.turn.observed_turn),
                status = ?status.kind,
                context_kind = turn_context_kind_label(self.turn.context.kind),
                items_buffered = self.turn.items.len(),
                diffs_buffered = self.turn.diffs.len(),
                elapsed_ms = self.forwarder_started.elapsed().as_millis(),
                "turn completion event received"
            );
            if self.turn.context.kind == TurnContextKind::ManualCompaction {
                info!(
                    %project_id,
                    %thread_id,
                    turn = %completed_turn,
                    status = ?status.kind,
                    items_buffered = self.turn.items.len(),
                    saw_context_compaction_marker = self.turn.saw_context_compaction_marker,
                    elapsed_ms = self.forwarder_started.elapsed().as_millis(),
                    "context compaction turn completed"
                );
            }
            let Some(tid) = self
                .complete_forwarded_turn(completed_turn, usage, status.clone())
                .await
            else {
                return ForwarderControl::Exit(ForwarderExitReason::PersistenceBlocked);
            };
            hub.broadcast_event(thread_id, event).await;
            if runtime.has_running_for_turn(&self.authority, tid) {
                info!(
                    %project_id,
                    %thread_id,
                    turn = %tid,
                    elapsed_ms = self.forwarder_started.elapsed().as_millis(),
                    "event forwarder monitoring after-turn running commands"
                );
            }
            self.turn.reset(&self.idle_context);
            return ForwarderControl::Continue;
        }

        broadcast_event_with_context(&hub, project_id, thread_id, event, &self.turn.context).await;
        ForwarderControl::Continue
    }

    async fn complete_forwarded_turn(
        &mut self,
        completed_turn: TurnId,
        usage: giskard_core::token::TokenUsage,
        status: TurnStatus,
    ) -> Option<TurnId> {
        let thread_id = self.thread_id();
        let project_id = self.binding.project_id;
        let ctx = &self.turn.context;
        let turn_id = self.turn.observed_turn;
        let tid = turn_id.unwrap_or(completed_turn);
        let item_count = self.turn.items.len();
        let diff_count = self.turn.diffs.len();
        let has_context_compaction_marker = self.turn.items.iter().any(is_context_compaction_item);
        if ctx.kind == TurnContextKind::ManualCompaction {
            info!(
                %project_id,
                %thread_id,
                turn = %tid,
                completed_turn = %completed_turn,
                started_turn = display_opt(turn_id),
                item_count,
                has_context_compaction_marker,
                status = ?status.kind,
                "persisting context compaction turn"
            );
        }
        let turn = Turn {
            id: tid,
            user_input: ctx.user_input.clone(),
            items: self.turn.items.take(),
            model: ctx.model.clone(),
            mode: ctx.mode,
            status: status.clone(),
            usage,
            diffs: std::mem::take(&mut self.turn.diffs),
            started_at: self.turn.started_at,
            completed_at: Some(Utc::now()),
        };
        let captured_diffs = self
            .shared
            .runtime
            .captured_diff_records(&self.authority, tid);
        let persist_outcome = persist_turn(
            &self.shared.thread_metadata,
            &self.shared.ledger,
            project_id,
            thread_id,
            &turn,
            &captured_diffs,
        )
        .await;
        if ctx.kind == TurnContextKind::ManualCompaction {
            info!(
                %project_id,
                %thread_id,
                turn = %tid,
                item_count,
                has_context_compaction_marker,
                history_appended = persist_outcome.history_appended,
                metadata_updated = persist_outcome.metadata_updated,
                "context compaction persistence path finished"
            );
        }
        let completion_event = AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: completed_turn,
            usage,
            status: status.clone(),
        };
        if persist_outcome.history_appended {
            self.seen_turn_ids.insert(tid);
            let applied = match self.turn.lease.as_mut() {
                Some(turn_gate) => turn_gate.commit_after_persistence(&completion_event),
                None => self.shared.runtime.settle_completed_turn(
                    &self.authority,
                    &completion_event,
                    None,
                ),
            };
            publish_applied_runtime_effects(&self.shared.hub, thread_id, applied).await;
        } else {
            let error = persist_outcome
                .history_error
                .clone()
                .unwrap_or_else(|| "turn history append failed".into());
            let applied = match self.turn.lease.as_mut() {
                Some(turn_gate) => {
                    turn_gate.retain_after_persistence_failure(&completion_event, turn, error)
                }
                None => self.shared.runtime.settle_completed_turn(
                    &self.authority,
                    &completion_event,
                    Some((turn, error)),
                ),
            };
            publish_applied_runtime_effects(&self.shared.hub, thread_id, applied).await;
            self.shared
            .hub
            .broadcast(
                thread_id,
                ServerMessage::Error {
                    error: giskard_proto::ErrorInfo {
                        code: "turn_persistence_blocked".into(),
                        severity: giskard_proto::ErrorSeverity::Error,
                        message:
                            "The completed turn could not be saved; this thread remains blocked."
                                .into(),
                        detail: persist_outcome.history_error,
                        thread_id: Some(thread_id),
                        action: Some("persist_turn".into()),
                        request_id: None,
                        process_id: None,
                    },
                },
            )
            .await;
            return None;
        }
        info!(
            %project_id,
            %thread_id,
            turn = %tid,
            completed_turn = %completed_turn,
            status = ?status.kind,
            context_kind = turn_context_kind_label(ctx.kind),
            item_count,
            diff_count,
            history_appended = persist_outcome.history_appended,
            metadata_updated = persist_outcome.metadata_updated,
            "completed turn cleanup finished"
        );
        Some(tid)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use chrono::Utc;
    use giskard_core::approval::{ApprovalKind, ApprovalRequest};
    use giskard_core::item::{CommandExecutionStart, ItemDelta, ItemKind, ItemStart};
    use giskard_core::model::ModelRef;
    use giskard_core::server_request::{ServerRequest, ServerRequestResponse};
    use giskard_core::token::{TokenLedger, TokenUsage};
    use giskard_core::turn::{Mode, PermissionPreset, TurnMode, TurnModel, TurnStatusKind};
    use giskard_core::user_input::UserInput;
    use giskard_harness::{
        AgentEventStream, EventLog, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
    };
    use giskard_persist::PersistStore;
    use giskard_persist::store::ThreadFile;
    use giskard_proto::{RequestStatus as WireRequestStatus, ServerMessage, WireAgentEvent};
    use tokio::sync::{Notify, mpsc};
    use tokio::task::JoinHandle;

    use super::*;
    use crate::hub::Hub;
    use crate::ledger;
    use crate::test_logs::CapturedLogWriter;
    use crate::thread_runtime::{RequestResolution, RuntimeRequestId, ThreadRuntimeSupport};

    struct TestIntentHarness {
        start_result: Result<TurnId, HarnessError>,
        compact_result: Result<(), HarnessError>,
        start_gate: Option<Arc<Notify>>,
        compact_gate: Option<Arc<Notify>>,
        start_calls: AtomicUsize,
        compact_calls: AtomicUsize,
    }

    impl TestIntentHarness {
        fn accepting(turn: TurnId) -> Self {
            Self {
                start_result: Ok(turn),
                compact_result: Ok(()),
                start_gate: None,
                compact_gate: None,
                start_calls: AtomicUsize::new(0),
                compact_calls: AtomicUsize::new(0),
            }
        }

        fn gated(turn: TurnId, gate: Arc<Notify>) -> Self {
            Self {
                start_gate: Some(gate),
                ..Self::accepting(turn)
            }
        }
    }

    #[async_trait]
    impl AgentHarness for TestIntentHarness {
        fn capabilities(&self) -> HarnessCapabilities {
            HarnessCapabilities::default()
        }

        async fn list_models(
            &self,
        ) -> Result<Vec<giskard_core::model::ModelDescriptor>, HarnessError> {
            Ok(Vec::new())
        }

        async fn open_thread(
            &self,
            _opts: OpenThreadOptions,
        ) -> Result<ThreadHandle, HarnessError> {
            Err(HarnessError::Unsupported("unused".into()))
        }

        async fn start_turn(
            &self,
            _thread: &ThreadHandle,
            _input: UserInput,
            _overrides: giskard_core::turn::TurnOverrides,
        ) -> Result<TurnId, HarnessError> {
            self.start_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(gate) = &self.start_gate {
                gate.notified().await;
            }
            self.start_result.clone()
        }

        async fn compact_thread(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
            self.compact_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(gate) = &self.compact_gate {
                gate.notified().await;
            }
            self.compact_result.clone()
        }

        fn subscribe(&self, _thread: &ThreadHandle) -> AgentEventStream {
            let log = Arc::new(EventLog::new());
            log.close();
            AgentEventStream::new(log.reader())
        }

        async fn respond_approval(
            &self,
            _req: giskard_core::ids::ApprovalId,
            _decision: giskard_core::approval::ApprovalDecision,
        ) -> Result<(), HarnessError> {
            Ok(())
        }

        async fn respond_server_request(
            &self,
            _req: giskard_core::ids::ServerRequestId,
            _response: giskard_core::server_request::ServerRequestResponse,
        ) -> Result<(), HarnessError> {
            Ok(())
        }

        async fn interrupt(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
            Ok(())
        }

        async fn shutdown(&self) -> Result<(), HarnessError> {
            Ok(())
        }
    }

    fn start_intent(
        context: TurnContext,
    ) -> (TurnIntent, oneshot::Receiver<Result<TurnId, HarnessError>>) {
        let (reply, response) = oneshot::channel();
        (
            TurnIntent::StartTurn {
                input: context.user_input.clone(),
                overrides: giskard_core::turn::TurnOverrides {
                    model: None,
                    mode: Mode::Build,
                    permission_preset: PermissionPreset::AskFirst,
                },
                context,
                reply,
            },
            response,
        )
    }

    async fn intent_forwarder(
        classification: ClassificationPhase,
        harness: Arc<TestIntentHarness>,
    ) -> (
        ThreadEventForwarder,
        mpsc::Sender<TurnIntent>,
        Arc<EventLog>,
        Arc<RegistryShared>,
        Arc<ThreadAuthority>,
        Arc<ThreadCoordinator>,
        Arc<dyn AgentHarness>,
        ProjectId,
        ThreadId,
        tempfile::TempDir,
    ) {
        let temp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(temp.path().to_path_buf()));
        let shared = Arc::new(RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let authority = Arc::new(ThreadAuthority::new_for_test(thread_id, project_id));
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (intent_tx, intent_rx) = mpsc::channel(crate::registry::thread::TURN_INTENT_CAPACITY);
        let coordinator = Arc::new(ThreadCoordinator::new_live(
            LoadedThreadBinding {
                project_id,
                handle: ThreadHandle::detached(thread_id, format!("native-{thread_id}")),
                native_model: None,
            },
            classification,
            cancel_tx,
            intent_tx.clone(),
        ));
        let log = Arc::new(EventLog::new());
        let trait_harness: Arc<dyn AgentHarness> = harness;
        let forwarder = ThreadEventForwarder::new(
            shared.clone(),
            authority.clone(),
            coordinator.clone(),
            Arc::downgrade(&trait_harness),
            AgentEventStream::new(log.reader()),
            cancel_rx,
            intent_rx,
            DriverHandle::disconnected(),
        )
        .await;
        (
            forwarder,
            intent_tx,
            log,
            shared,
            authority,
            coordinator,
            trait_harness,
            project_id,
            thread_id,
            temp,
        )
    }

    async fn running_intent_forwarder(
        classification: ClassificationPhase,
        harness: Arc<TestIntentHarness>,
    ) -> (
        JoinHandle<ForwarderExitReason>,
        mpsc::Sender<TurnIntent>,
        Arc<EventLog>,
        Arc<RegistryShared>,
        Arc<ThreadAuthority>,
        ProjectId,
        ThreadId,
        tempfile::TempDir,
    ) {
        let (
            forwarder,
            intent_tx,
            log,
            shared,
            authority,
            _coordinator,
            trait_harness,
            project_id,
            thread_id,
            temp,
        ) = intent_forwarder(classification, harness).await;
        let handle = tokio::spawn(async move {
            let result = forwarder.run().await;
            drop(trait_harness);
            result
        });
        (
            handle, intent_tx, log, shared, authority, project_id, thread_id, temp,
        )
    }

    async fn wait_for_call_count(counter: &AtomicUsize, expected: usize) {
        tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
            while counter.load(Ordering::SeqCst) != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("harness call count should reach the expected value");
    }

    #[tokio::test]
    async fn a_second_intent_while_one_is_admitted_is_thread_busy() {
        let gate = Arc::new(Notify::new());
        let harness = Arc::new(TestIntentHarness::gated(TurnId::new(), gate));
        let (handle, intents, log, _shared, _authority, _project_id, _thread_id, _temp) =
            running_intent_forwarder(ClassificationPhase::Primary, harness.clone()).await;
        let (first, _first_response) = start_intent(crate::registry::tests::test_turn_context());
        intents.send(first).await.unwrap();
        wait_for_call_count(&harness.start_calls, 1).await;

        let (second, second_response) = start_intent(crate::registry::tests::test_turn_context());
        intents.send(second).await.unwrap();
        assert!(matches!(
            second_response.await.unwrap(),
            Err(HarnessError::ThreadBusy { .. })
        ));
        assert_eq!(harness.start_calls.load(Ordering::SeqCst), 1);
        log.close();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn a_subagent_owner_rejects_intents_as_read_only() {
        let harness = Arc::new(TestIntentHarness::accepting(TurnId::new()));
        let (handle, intents, log, shared, authority, _project_id, _thread_id, _temp) =
            running_intent_forwarder(ClassificationPhase::Subagent, harness.clone()).await;
        let (intent, response) = start_intent(crate::registry::tests::test_turn_context());
        intents.send(intent).await.unwrap();
        assert!(matches!(
            response.await.unwrap(),
            Err(HarnessError::ThreadReadOnly { .. })
        ));
        assert_eq!(harness.start_calls.load(Ordering::SeqCst), 0);
        assert!(!shared.runtime.has_active_turn(&authority));
        log.close();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn a_retained_event_is_processed_before_a_queued_intent() {
        let harness = Arc::new(TestIntentHarness::accepting(TurnId::new()));
        let (
            forwarder,
            intents,
            log,
            shared,
            _authority,
            _coordinator,
            trait_harness,
            project_id,
            thread_id,
            _temp,
        ) = intent_forwarder(ClassificationPhase::Primary, harness.clone()).await;
        let external_turn = TurnId::new();
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: external_turn,
        }));
        let (intent, response) = start_intent(crate::registry::tests::test_turn_context());
        intents.send(intent).await.unwrap();

        let handle = tokio::spawn(async move {
            let result = forwarder.run().await;
            drop(trait_harness);
            result
        });
        assert!(matches!(
            response.await.unwrap(),
            Err(HarnessError::ThreadBusy { thread }) if thread == thread_id
        ));
        assert_eq!(harness.start_calls.load(Ordering::SeqCst), 0);

        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: external_turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));
        wait_for_turn_count(&shared.store, project_id, thread_id, 1).await;
        let saved = shared
            .store
            .load_all_turns(project_id, thread_id)
            .await
            .unwrap();
        assert_eq!(saved[0].user_input, UserInput::text(""));
        log.close();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn an_intent_delivered_after_detach_began_is_refused() {
        let harness = Arc::new(TestIntentHarness::accepting(TurnId::new()));
        let (
            forwarder,
            intents,
            _log,
            _shared,
            _authority,
            coordinator,
            _trait_harness,
            _project_id,
            thread_id,
            _temp,
        ) = intent_forwarder(ClassificationPhase::Primary, harness.clone()).await;
        let (detach_reply, _detach_response) = oneshot::channel();
        let _ = coordinator.request_detach(detach_reply).await;
        let (intent, response) = start_intent(crate::registry::tests::test_turn_context());
        intents.send(intent).await.unwrap();
        let handle = tokio::spawn(async move { forwarder.run().await });

        assert!(matches!(
            response.await.unwrap(),
            Err(HarnessError::Protocol(message))
                if message == format!("thread {thread_id} has no live event owner")
        ));
        assert_eq!(harness.start_calls.load(Ordering::SeqCst), 0);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn an_intent_reserves_the_runtime_and_the_first_native_turn_adopts_it() {
        let turn_id = TurnId::new();
        let harness = Arc::new(TestIntentHarness::accepting(turn_id));
        let (handle, intents, log, shared, authority, project_id, thread_id, _temp) =
            running_intent_forwarder(ClassificationPhase::Primary, harness).await;
        let context = crate::registry::tests::test_turn_context();
        let expected_input = context.user_input.clone();
        let expected_model = context.model.clone();
        let expected_mode = context.mode;
        let (intent, response) = start_intent(context);
        intents.send(intent).await.unwrap();
        assert_eq!(response.await.unwrap().unwrap(), turn_id);
        assert!(matches!(
            shared.runtime
                .current_overview()
                .threads
                .iter()
                .find(|summary| summary.thread_id == thread_id)
                .map(|summary| &summary.turn_state),
            Some(giskard_proto::RuntimeTurnState::Active {
                turn_id: Some(id)
            }) if *id == turn_id
        ));
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: turn_id
        }));
        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: turn_id,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None
            },
        }));
        wait_for_turn_count(&shared.store, project_id, thread_id, 1).await;
        let saved = shared
            .store
            .load_all_turns(project_id, thread_id)
            .await
            .unwrap();
        assert_eq!(saved[0].user_input, expected_input);
        assert_eq!(saved[0].model, expected_model);
        assert_eq!(saved[0].mode, expected_mode);
        assert!(!shared.runtime.has_active_turn(&authority));
        log.close();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn native_turn_start_before_the_harness_reply_still_adopts_the_intent() {
        let turn_id = TurnId::new();
        let gate = Arc::new(Notify::new());
        let harness = Arc::new(TestIntentHarness::gated(turn_id, gate.clone()));
        let (handle, intents, log, shared, _authority, project_id, thread_id, _temp) =
            running_intent_forwarder(ClassificationPhase::Primary, harness.clone()).await;
        let context = crate::registry::tests::test_turn_context();
        let expected_input = context.user_input.clone();
        let (intent, response) = start_intent(context);
        intents.send(intent).await.unwrap();
        wait_for_call_count(&harness.start_calls, 1).await;
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: turn_id
        }));
        tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
            while !matches!(
                shared
                    .runtime
                    .current_overview()
                    .threads
                    .iter()
                    .find(|summary| summary.thread_id == thread_id)
                    .map(|summary| &summary.turn_state),
                Some(giskard_proto::RuntimeTurnState::Active {
                    turn_id: Some(id)
                }) if *id == turn_id
            ) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the native start should attach before the harness reply");
        gate.notify_one();
        assert_eq!(response.await.unwrap().unwrap(), turn_id);
        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: turn_id,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None
            },
        }));
        wait_for_turn_count(&shared.store, project_id, thread_id, 1).await;
        let saved = shared
            .store
            .load_all_turns(project_id, thread_id)
            .await
            .unwrap();
        assert_eq!(saved[0].user_input, expected_input);
        log.close();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn a_short_native_turn_can_complete_before_the_harness_reply() {
        let turn_id = TurnId::new();
        let gate = Arc::new(Notify::new());
        let harness = Arc::new(TestIntentHarness::gated(turn_id, gate.clone()));
        let (handle, intents, log, shared, _authority, project_id, thread_id, _temp) =
            running_intent_forwarder(ClassificationPhase::Primary, harness.clone()).await;
        let context = crate::registry::tests::test_turn_context();
        let expected_input = context.user_input.clone();
        let (intent, response) = start_intent(context);
        intents.send(intent).await.unwrap();
        wait_for_call_count(&harness.start_calls, 1).await;
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: turn_id
        }));
        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: turn_id,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None
            },
        }));
        wait_for_turn_count(&shared.store, project_id, thread_id, 1).await;
        gate.notify_one();
        assert_eq!(response.await.unwrap().unwrap(), turn_id);
        let saved = shared
            .store
            .load_all_turns(project_id, thread_id)
            .await
            .unwrap();
        assert_eq!(saved[0].user_input, expected_input);
        log.close();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn a_harness_rejection_releases_the_admitted_lease() {
        let mut configured = TestIntentHarness::accepting(TurnId::new());
        configured.start_result = Err(HarnessError::Protocol("rejected".into()));
        let harness = Arc::new(configured);
        let (handle, intents, log, shared, authority, _project_id, _thread_id, _temp) =
            running_intent_forwarder(ClassificationPhase::Primary, harness.clone()).await;
        let (intent, response) = start_intent(crate::registry::tests::test_turn_context());
        intents.send(intent).await.unwrap();
        assert!(matches!(
            response.await.unwrap(),
            Err(HarnessError::Protocol(_))
        ));
        assert!(!shared.runtime.has_active_turn(&authority));

        let (second, second_response) = start_intent(crate::registry::tests::test_turn_context());
        intents.send(second).await.unwrap();
        assert!(matches!(
            second_response.await.unwrap(),
            Err(HarnessError::Protocol(_))
        ));
        assert_eq!(harness.start_calls.load(Ordering::SeqCst), 2);
        log.close();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn a_compaction_intent_labels_the_native_turn_as_manual_compaction() {
        let harness = Arc::new(TestIntentHarness::accepting(TurnId::new()));
        let (handle, intents, log, shared, _authority, project_id, thread_id, _temp) =
            running_intent_forwarder(ClassificationPhase::Primary, harness.clone()).await;
        let mut context = crate::registry::tests::test_turn_context();
        context.user_input = UserInput::text("/compact");
        context.kind = TurnContextKind::ManualCompaction;
        let (reply, response) = oneshot::channel();
        intents
            .send(TurnIntent::Compact { context, reply })
            .await
            .unwrap();
        assert!(response.await.unwrap().is_ok());
        assert_eq!(harness.compact_calls.load(Ordering::SeqCst), 1);

        let turn_id = TurnId::new();
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: turn_id
        }));
        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: turn_id,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None
            },
        }));
        wait_for_turn_count(&shared.store, project_id, thread_id, 1).await;
        let saved = shared
            .store
            .load_all_turns(project_id, thread_id)
            .await
            .unwrap();
        assert_eq!(saved[0].user_input, UserInput::text("/compact"));
        log.close();
        handle.await.unwrap();
    }

    #[test]
    fn command_completion_success_requires_success_status_and_zero_exit() {
        assert!(command_completion_is_normal_success("completed", Some(0)));
        assert!(command_completion_is_normal_success("succeeded", Some(0)));
        assert!(command_completion_is_normal_success("success", Some(0)));

        assert!(!command_completion_is_normal_success(
            "completed",
            Some(143)
        ));
        assert!(!command_completion_is_normal_success("failed", Some(0)));
        assert!(!command_completion_is_normal_success("interrupted", None));
    }

    fn capture_logs(log: impl FnOnce()) -> String {
        let output = Arc::new(StdMutex::new(Vec::new()));
        let writer_output = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(move || CapturedLogWriter(writer_output.clone()))
            .finish();
        tracing::subscriber::with_default(subscriber, log);
        String::from_utf8(output.lock().unwrap().clone()).unwrap()
    }

    fn capture_cross_turn_warning(event: &AgentEvent) -> String {
        capture_logs(|| {
            log_cross_turn_event_drop(
                ProjectId::new(),
                ThreadId::new(),
                TurnId::new(),
                event_turn_id(event).unwrap(),
                event,
                42,
            );
        })
    }

    #[test]
    fn cross_turn_item_delta_warning_reports_bare_identity_without_content() {
        let item_id = ItemId::new();
        let event = AgentEvent::ItemDelta {
            thread: ThreadId::new(),
            turn: TurnId::new(),
            item_id,
            delta: ItemDelta::CommandOutput {
                chunk: "sensitive output".into(),
            },
        };

        assert_eq!(event_item_id(&event), Some(item_id));
        assert_eq!(event_item_delta_kind(&event), Some("command_output"));

        let output = capture_cross_turn_warning(&event);
        assert!(
            output.contains(&format!("event_item_id={item_id}")),
            "{output}"
        );
        assert!(
            output.contains("item_delta_kind=\"command_output\""),
            "{output}"
        );
        assert!(output.contains("elapsed_ms=42"), "{output}");
        assert!(!output.contains("Some("), "{output}");
        assert!(!output.contains("sensitive output"), "{output}");

        let text_event = AgentEvent::ItemDelta {
            thread: ThreadId::new(),
            turn: TurnId::new(),
            item_id: ItemId::new(),
            delta: ItemDelta::Text {
                text: "sensitive text".into(),
            },
        };
        assert_eq!(event_item_delta_kind(&text_event), Some("text"));
        let output = capture_cross_turn_warning(&text_event);
        assert!(output.contains("item_delta_kind=\"text\""), "{output}");
        assert!(!output.contains("sensitive text"), "{output}");

        let turn_event = AgentEvent::TurnStarted {
            thread: ThreadId::new(),
            turn: TurnId::new(),
        };
        let output = capture_cross_turn_warning(&turn_event);
        assert!(!output.contains("event_item_id"), "{output}");
        assert!(!output.contains("item_delta_kind"), "{output}");
    }

    #[test]
    fn dropped_and_rejected_event_logs_include_bare_identity_without_content() {
        let project_id = ProjectId::new();
        let expected_thread_id = ThreadId::new();
        let event_thread_id = ThreadId::new();
        let turn_id = TurnId::new();
        let item_id = ItemId::new();
        let event = AgentEvent::ItemDelta {
            thread: event_thread_id,
            turn: turn_id,
            item_id,
            delta: ItemDelta::Text {
                text: "foreign sensitive text".into(),
            },
        };

        let output = capture_logs(|| {
            log_foreign_thread_event_drop(project_id, expected_thread_id, event_thread_id, &event);
        });
        for expected in [
            format!("project_id={project_id}"),
            format!("thread_id={expected_thread_id}"),
            format!("event_thread_id={event_thread_id}"),
            format!("event_turn_id={turn_id}"),
            format!("event_item_id={item_id}"),
            "event_kind=\"item_delta\"".into(),
            "item_delta_kind=\"text\"".into(),
        ] {
            assert!(output.contains(&expected), "missing {expected}: {output}");
        }
        assert!(!output.contains("foreign sensitive text"), "{output}");
        assert!(!output.contains("Some("), "{output}");

        let output = capture_logs(|| {
            log_metadata_only_event_rejection(
                project_id,
                expected_thread_id,
                "diff_updated",
                Some(turn_id),
                None,
            );
        });
        assert!(
            output.contains(&format!("project_id={project_id}")),
            "{output}"
        );
        assert!(
            output.contains(&format!("event_turn_id={turn_id}")),
            "{output}"
        );
        assert!(output.contains("event_kind=\"diff_updated\""), "{output}");
        assert!(!output.contains("event_item_id"), "{output}");
        assert!(!output.contains("Some("), "{output}");
    }

    #[test]
    fn late_tool_warning_requires_terminal_output() {
        let item = |status: Option<&str>, output: Option<serde_json::Value>| Item {
            id: ItemId::new(),
            harness_item_id: "tool-1".into(),
            payload: ItemPayload::ToolCall {
                name: "lookup".into(),
                input: serde_json::Value::Null,
                output,
                server: None,
                status: status.map(str::to_owned),
                metadata: None,
                subagent: None,
                error: None,
            },
            created_at: Utc::now(),
        };
        assert!(!completed_tool_has_terminal_output(&item(
            Some("completed"),
            None,
        )));
        assert!(!completed_tool_has_terminal_output(&item(
            Some("running"),
            Some(serde_json::json!({"partial": true})),
        )));
        assert!(completed_tool_has_terminal_output(&item(
            Some("completed"),
            Some(serde_json::Value::Null),
        )));
    }

    #[test]
    fn late_untruncated_command_completion_ignores_original_counts() {
        let thread_id = ThreadId::new();
        let message = late_command_completion_message(
            thread_id,
            AgentEvent::ItemCompleted {
                thread: thread_id,
                turn: TurnId::new(),
                item: Item {
                    id: ItemId::new(),
                    harness_item_id: "command-1".into(),
                    payload: ItemPayload::CommandExecution {
                        command: "printf ok".into(),
                        cwd: std::path::PathBuf::from("/tmp/project"),
                        output: "ok\n".into(),
                        output_truncated: false,
                        output_original_bytes: Some(999),
                        output_original_lines: Some(88),
                        exit_code: Some(0),
                        status: Some("completed".into()),
                        process_id: None,
                        duration_ms: None,
                    },
                    created_at: Utc::now(),
                },
            },
        )
        .unwrap();
        let ServerMessage::Event { agent_event, .. } = message else {
            panic!("expected event message");
        };
        let WireAgentEvent::ItemCompleted { item, .. } = *agent_event else {
            panic!("expected completed item");
        };
        let giskard_proto::WireItemPayload::CommandExecution { output, .. } = item.payload else {
            panic!("expected command execution payload");
        };
        assert_eq!(output.original_bytes, 3);
        assert_eq!(output.original_lines, 1);
    }

    #[test]
    fn current_turn_items_take_clears_indexes_for_reused_item_id() {
        let item_id = ItemId::new();
        let mut buffer = CurrentTurnItems::default();
        let first = Item {
            id: item_id,
            harness_item_id: "native_first".into(),
            payload: ItemPayload::AgentMessage {
                text: "first".into(),
            },
            created_at: Utc::now(),
        };
        assert!(!buffer.upsert(&first));
        assert_eq!(buffer.take(), vec![first]);
        assert!(buffer.indexes.is_empty());

        let second = Item {
            id: item_id,
            harness_item_id: "native_second".into(),
            payload: ItemPayload::AgentMessage {
                text: "second".into(),
            },
            created_at: Utc::now(),
        };
        assert!(!buffer.upsert(&second));
        assert_eq!(buffer.take(), vec![second]);
    }

    #[test]
    fn current_turn_items_repairs_stale_index_without_panicking() {
        let mut buffer = CurrentTurnItems::default();
        let item_id = ItemId::new();
        buffer.indexes.insert(item_id, 7);
        let item = Item {
            id: item_id,
            harness_item_id: "stale_item".into(),
            payload: ItemPayload::AgentMessage {
                text: "recovered".into(),
            },
            created_at: Utc::now(),
        };

        assert!(buffer.upsert(&item));
        assert_eq!(buffer.items, vec![item]);
        assert_eq!(buffer.indexes.get(&item_id), Some(&0));
    }

    #[test]
    fn current_turn_items_repairs_in_range_stale_index() {
        let first_id = ItemId::new();
        let second_id = ItemId::new();
        let first = Item {
            id: first_id,
            harness_item_id: "first".into(),
            payload: ItemPayload::AgentMessage {
                text: "first".into(),
            },
            created_at: Utc::now(),
        };
        let second = Item {
            id: second_id,
            harness_item_id: "second".into(),
            payload: ItemPayload::AgentMessage {
                text: "second".into(),
            },
            created_at: Utc::now(),
        };
        let replacement = Item {
            payload: ItemPayload::AgentMessage {
                text: "updated second".into(),
            },
            ..second.clone()
        };
        let mut buffer = CurrentTurnItems::default();
        assert!(!buffer.upsert(&first));
        assert!(!buffer.upsert(&second));
        buffer.indexes.insert(second_id, 0);

        assert!(buffer.upsert(&replacement));
        assert_eq!(buffer.items, vec![first, replacement]);
        assert_eq!(buffer.indexes.get(&first_id), Some(&0));
        assert_eq!(buffer.indexes.get(&second_id), Some(&1));
    }

    #[test]
    fn current_turn_items_upserts_empty_native_id_by_item_id() {
        let item_id = ItemId::new();
        let mut buffer = CurrentTurnItems::default();
        let first = Item {
            id: item_id,
            harness_item_id: String::new(),
            payload: ItemPayload::AgentMessage {
                text: "partial".into(),
            },
            created_at: Utc::now(),
        };
        let completed = Item {
            payload: ItemPayload::AgentMessage {
                text: "complete".into(),
            },
            ..first.clone()
        };

        assert!(!buffer.upsert(&first));
        assert!(!buffer.upsert(&completed));
        assert_eq!(buffer.items, vec![completed]);
    }

    #[test]
    fn command_status_running_accepts_codex_variants() {
        assert!(command_status_is_running("in_progress"));
        assert!(command_status_is_running("in-progress"));
        assert!(command_status_is_running("running"));

        assert!(!command_status_is_running("completed"));
        assert!(!command_status_is_running("interrupted"));
    }

    #[test]
    fn item_identity_tracking_rejects_native_id_remapping_within_a_turn() {
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let original_item = ItemId::new();
        let conflicting_item = ItemId::new();
        let mut identities = Default::default();

        let started = AgentEvent::ItemStarted {
            thread,
            turn,
            item: ItemStart {
                id: original_item,
                harness_item_id: "cmd_1".into(),
                kind: ItemKind::CommandExecution,
                command: None,
                tool: None,
            },
        };
        assert!(track_item_identity(&mut identities, &started).is_none());

        let repeated = AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: original_item,
                harness_item_id: "cmd_1".into(),
                payload: ItemPayload::AgentMessage {
                    text: "same identity".into(),
                },
                created_at: Utc::now(),
            },
        };
        assert!(track_item_identity(&mut identities, &repeated).is_none());

        let conflicting = AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: conflicting_item,
                harness_item_id: "cmd_1".into(),
                payload: ItemPayload::AgentMessage {
                    text: "different identity".into(),
                },
                created_at: Utc::now(),
            },
        };
        assert_eq!(
            track_item_identity(&mut identities, &conflicting),
            Some((turn, "cmd_1".into(), original_item, conflicting_item))
        );
    }

    #[tokio::test]
    async fn resumed_context_window_uses_resumed_model_and_metadata_service() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "historical-model".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "native-thread".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();
        let hub = Arc::new(Hub::new());
        let shared = Arc::new(super::RegistryShared::new(
            hub,
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let (sink, stream, permit) = prepare_thread_updates(&shared, project_id, thread_id).await;
        let forwarder =
            spawn_thread_update_forwarder(shared.clone(), project_id, thread_id, stream, permit)
                .unwrap();
        sink.send(ThreadUpdate::ContextWindowRestored {
            model: model.clone(),
            context_window: 258_400,
        })
        .unwrap();
        forwarder.await.unwrap();
        assert_eq!(
            store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .unwrap()
                .context_window,
            258_400
        );

        for invalidate_with_turn in [true, false] {
            let stale_thread_id = ThreadId::new();
            let mut stale_thread = store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .unwrap();
            stale_thread.id = stale_thread_id;
            stale_thread.revision = 0;
            stale_thread.context_window = 128_000;
            stale_thread.model_context_windows.clear();
            store.save_thread(project_id, &stale_thread).await.unwrap();
            let (sink, stream, permit) =
                prepare_thread_updates(&shared, project_id, stale_thread_id).await;
            let stale_authority = shared.thread_authority(stale_thread_id).await.unwrap();
            if invalidate_with_turn {
                let handle = ThreadHandle::detached(stale_thread_id, "native-stale".into());
                let ctx = TurnContext {
                    user_input: UserInput::text("newer turn"),
                    model: TurnModel::Known(model.clone()),
                    mode: TurnMode::Known(Mode::Build),
                    kind: TurnContextKind::User,
                };
                let _lease = shared
                    .runtime
                    .reserve_turn(
                        &stale_authority,
                        turn_reservation(project_id, &handle, &ctx),
                    )
                    .unwrap();
            } else {
                shared.runtime.forget_threads(&[stale_authority]);
            }
            let forwarder = spawn_thread_update_forwarder(
                shared.clone(),
                project_id,
                stale_thread_id,
                stream,
                permit,
            )
            .unwrap();
            sink.send(ThreadUpdate::ContextWindowRestored {
                model: model.clone(),
                context_window: 400_000,
            })
            .unwrap();
            forwarder.await.unwrap();
            assert_eq!(
                store
                    .load_thread(project_id, stale_thread_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .context_window,
                128_000
            );
        }
    }

    #[tokio::test]
    async fn forwarder_skips_persistence_for_mismatched_turn_model() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let turn_id = TurnId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.6-sol".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let replacements = hub.register_client(1, client_tx.clone()).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());
        let handle = spawn_forwarder_handle(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub,
            store.clone(),
            ledger,
            model.clone(),
            "context window mismatch",
        );

        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: turn_id,
        }));
        assert!(log.append(AgentEvent::TurnUsageUpdated {
            thread: thread_id,
            turn: turn_id,
            usage: TokenUsage::default(),
            model: Some(ModelRef {
                provider: model.provider.clone(),
                model: "gpt-5.6-pro".into(),
                reasoning_effort: None,
            }),
            context_window: Some(400_000),
        }));
        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: turn_id,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));
        log.close();

        tokio::time::timeout(tokio::time::Duration::from_secs(2), handle)
            .await
            .expect("forwarder should exit after turn completion")
            .unwrap();

        let persisted = store
            .load_thread(project_id, thread_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.context_window, 128_000);
        assert!(
            persisted.model_context_windows.is_empty(),
            "a mismatched turn model must not be persisted"
        );
        assert!(
            std::iter::from_fn(|| client_rx.try_recv().ok()).any(|message| matches!(
                message,
                ServerMessage::Event { agent_event, .. }
                    if matches!(*agent_event, WireAgentEvent::TurnUsageUpdated { .. })
            ))
        );
        while let Some(message) = replacements.try_recv() {
            if let ServerMessage::ThreadState(state) = message {
                assert_ne!(state.metadata.context_window, 400_000);
            }
        }
    }

    #[tokio::test]
    async fn unchanged_windows_do_not_bump_revision_and_usage_is_broadcast() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let turn_id = TurnId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.6-sol".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let replacements = hub.register_client(1, client_tx.clone()).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());
        let handle = spawn_forwarder_handle(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub,
            store.clone(),
            ledger,
            model.clone(),
            "context window match",
        );

        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: turn_id,
        }));
        assert!(log.append(AgentEvent::TurnUsageUpdated {
            thread: thread_id,
            turn: turn_id,
            usage: TokenUsage {
                input: 10,
                output: 1,
                total: 11,
            },
            model: Some(model.clone()),
            context_window: Some(258_400),
        }));
        for input in [20, 30] {
            assert!(log.append(AgentEvent::TurnUsageUpdated {
                thread: thread_id,
                turn: turn_id,
                usage: TokenUsage {
                    input,
                    output: 1,
                    total: input + 1,
                },
                model: Some(model.clone()),
                context_window: Some(258_400),
            }));
        }
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);
        loop {
            let current = store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .unwrap();
            if current.context_window == 258_400 {
                assert_eq!(
                    current.revision, 1,
                    "unchanged windows must produce one metadata revision bump"
                );
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "window was not persisted"
            );
            tokio::task::yield_now().await;
        }
        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: turn_id,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));
        log.close();

        tokio::time::timeout(tokio::time::Duration::from_secs(2), handle)
            .await
            .expect("forwarder should exit after turn completion")
            .unwrap();

        let persisted = store
            .load_thread(project_id, thread_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.context_window, 258_400);
        assert_eq!(
            persisted
                .model_context_windows
                .get("openai")
                .and_then(|models| models.get("gpt-5.6-sol")),
            Some(&258_400)
        );

        let mut matching_states = 0;
        while let Some(message) = replacements.try_recv() {
            if let ServerMessage::ThreadState(state) = message {
                assert_eq!(state.metadata.thread_id, thread_id);
                assert!(state.metadata.revision <= persisted.revision);
                assert_eq!(state.active_turn, None);
                if state.metadata.context_window == 258_400 {
                    matching_states += 1;
                    assert_eq!(state.metadata.revision, persisted.revision);
                    assert_eq!(
                        state.metadata.current_model,
                        TurnModel::Known(model.clone())
                    );
                }
            }
        }
        assert_eq!(
            matching_states, 1,
            "matching update must survive coalescing into the latest committed thread state"
        );
        assert!(
            std::iter::from_fn(|| client_rx.try_recv().ok()).any(|message| matches!(
                message,
                ServerMessage::Event { agent_event, .. }
                    if matches!(*agent_event, WireAgentEvent::TurnUsageUpdated { .. })
            ))
        );
    }

    #[tokio::test]
    async fn forwarder_broadcasts_live_usage_for_an_unknown_model_turn_and_persists_nothing() {
        let (_tmp, store, project_id, thread_id, model) = usage_forwarder_fixture().await;
        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());
        let (handle, runtime, _coordinator, authority) = spawn_forwarder_handle_with_runtime(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub,
            store.clone(),
            ledger,
            model,
            "unknown model usage",
            None,
        );
        let turn = TurnId::new();
        let usage = TokenUsage {
            input: 42,
            output: 3,
            total: 45,
        };
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
        }));
        assert!(log.append(AgentEvent::TurnUsageUpdated {
            thread: thread_id,
            turn,
            usage,
            context_window: Some(258_400),
            model: None,
        }));

        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);
        loop {
            if runtime.live_snapshot(&authority).is_some_and(|snapshot| {
                snapshot.accumulated.iter().any(|event| {
                    matches!(event, WireAgentEvent::TurnUsageUpdated { usage: got, .. } if *got == usage)
                })
            }) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "usage was not live-buffered"
            );
            tokio::task::yield_now().await;
        }
        let persisted = store
            .load_thread(project_id, thread_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.context_window, 128_000);
        assert!(persisted.model_context_windows.is_empty());
        while let Some(message) = replacements.try_recv() {
            if let ServerMessage::ThreadState(state) = message {
                assert_ne!(state.metadata.context_window, 258_400);
            }
        }
        assert!(std::iter::from_fn(|| client_rx.try_recv().ok()).any(|message| matches!(
            message,
            ServerMessage::Event { agent_event, .. }
                if matches!(*agent_event, WireAgentEvent::TurnUsageUpdated { usage: got, .. } if got == usage)
        )));

        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn,
            usage,
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));
        log.close();
        tokio::time::timeout(tokio::time::Duration::from_secs(2), handle)
            .await
            .unwrap()
            .unwrap();
        let turns = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(turns[0].usage, usage);
    }

    #[tokio::test]
    async fn forwarder_ignores_usage_for_an_already_persisted_turn() {
        let (_tmp, store, project_id, thread_id, model) = usage_forwarder_fixture().await;
        let old_turn = TurnId::new();
        store
            .append_turn(
                project_id,
                thread_id,
                &Turn {
                    id: old_turn,
                    user_input: UserInput::text("old"),
                    items: Vec::new(),
                    diffs: Vec::new(),
                    model: TurnModel::Known(model.clone()),
                    mode: TurnMode::Known(Mode::Build),
                    usage: TokenUsage::default(),
                    status: TurnStatus {
                        kind: TurnStatusKind::Completed,
                        message: None,
                    },
                    started_at: Utc::now(),
                    completed_at: Some(Utc::now()),
                },
            )
            .await
            .unwrap();
        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());
        let (handle, _runtime, _coordinator, _authority) = spawn_forwarder_handle_with_runtime(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub,
            store.clone(),
            ledger,
            model,
            "late usage",
            None,
        );
        let late_usage = AgentEvent::TurnUsageUpdated {
            thread: thread_id,
            turn: old_turn,
            usage: TokenUsage {
                input: 999,
                output: 1,
                total: 1_000,
            },
            context_window: Some(400_000),
            model: None,
        };
        assert!(log.append(late_usage.clone()));
        let new_turn = TurnId::new();
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: new_turn,
        }));
        assert!(log.append(late_usage));
        let new_usage = TokenUsage {
            input: 12,
            output: 1,
            total: 13,
        };
        assert!(log.append(AgentEvent::TurnUsageUpdated {
            thread: thread_id,
            turn: new_turn,
            usage: new_usage,
            context_window: Some(258_400),
            model: None,
        }));
        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: new_turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));
        log.close();
        tokio::time::timeout(tokio::time::Duration::from_secs(2), handle)
            .await
            .unwrap()
            .unwrap();
        let messages = std::iter::from_fn(|| client_rx.try_recv().ok()).collect::<Vec<_>>();
        assert!(!messages.iter().any(|message| matches!(
            message,
            ServerMessage::Event { agent_event, .. }
                if matches!(agent_event.as_ref(), WireAgentEvent::TurnUsageUpdated { turn, .. } if *turn == old_turn)
        )));
        assert!(messages.iter().any(|message| matches!(
            message,
            ServerMessage::Event { agent_event, .. }
                if matches!(agent_event.as_ref(), WireAgentEvent::TurnUsageUpdated { turn, usage, .. } if *turn == new_turn && *usage == new_usage)
        )));
        let persisted = store
            .load_thread(project_id, thread_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.context_window, 128_000);
        assert!(persisted.model_context_windows.is_empty());
        assert_eq!(
            store
                .load_all_turns(project_id, thread_id)
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn one_long_lived_forwarder_persists_successive_native_turns() {
        let tmp = tempfile::TempDir::new().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let ledger = ledger::spawn(store.clone());

        let (runtime, authority) = spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub.clone(),
            store.clone(),
            ledger.clone(),
            model.clone(),
            "first",
        );
        tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
            while !runtime.has_active_turn(&authority) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first intent should be admitted before native events arrive");
        let first_turn = TurnId::new();
        let second_turn = TurnId::new();
        let mut first_events = turn_events(
            thread_id,
            first_turn,
            "first",
            "one",
            TokenUsage::new(10, 1),
        );
        assert!(log.append(first_events.remove(0)));
        let rejected_diff = giskard_core::FileDiff {
            path: "src/rejected.rs".into(),
            change: giskard_core::FileChangeKind::Modified,
            old_text: Some("old".into()),
            new_text: Some("foreign".into()),
            hunks: Vec::new(),
            binary: false,
            captured: None,
        };
        let rejected_id = giskard_core::capture_structured_diff(rejected_diff.clone())
            .1
            .id;
        assert!(log.append(AgentEvent::DiffUpdated {
            thread: thread_id,
            turn: second_turn,
            diff: rejected_diff,
        }));
        for event in first_events {
            assert!(log.append(event));
        }
        wait_for_turn_count(&store, project_id, thread_id, 1).await;
        assert!(matches!(
            runtime.captured_diff(&authority, second_turn, &rejected_id),
            crate::thread_runtime::RuntimeDiffLookup::Missing
        ));

        for event in turn_events(
            thread_id,
            second_turn,
            "second",
            "two",
            TokenUsage::new(20, 2),
        ) {
            assert!(log.append(event));
        }
        wait_for_turn_count(&store, project_id, thread_id, 2).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // The bounded index carries one record per turn, after its one-line header.
        let raw_history = tokio::fs::read_to_string(
            data_dir
                .join("projects")
                .join(project_id.to_string())
                .join("threads")
                .join(thread_id.to_string())
                .join("history.jsonl"),
        )
        .await
        .unwrap();
        assert_eq!(
            raw_history
                .lines()
                .filter(|line| line.contains(r#""kind":"turn""#))
                .count(),
            2
        );

        let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(saved.len(), 2);
        assert_eq!(saved[0].id, first_turn);
        assert_eq!(saved[0].user_input, UserInput::text("first"));
        assert_eq!(saved[1].id, second_turn);
        assert_eq!(saved[1].user_input, UserInput::text(""));
    }

    #[tokio::test]
    async fn long_lived_forwarder_uses_current_external_context_for_each_turn() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let parent_thread_id = ThreadId::new();
        let initial_model = ModelRef {
            provider: "openai".into(),
            model: "initial".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "orphan".into(),
                    harness_thread_id: "native-orphan".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Orphan,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(initial_model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::new());
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let (intent_tx, intent_rx) = mpsc::channel(crate::registry::thread::TURN_INTENT_CAPACITY);
        let coordinator = Arc::new(super::ThreadCoordinator::new_live(
            super::LoadedThreadBinding {
                project_id,
                handle: ThreadHandle::detached(thread_id, "native-orphan".into()),
                native_model: Some(initial_model),
            },
            super::ClassificationPhase::Orphan,
            cancel_tx,
            intent_tx,
        ));
        let authority = Arc::new(super::ThreadAuthority::new_for_test(thread_id, project_id));
        let harness: Arc<dyn AgentHarness> = Arc::new(TestIntentHarness::accepting(TurnId::new()));
        let forwarder = tokio::spawn(
            ThreadEventForwarder::new(
                shared.clone(),
                authority,
                coordinator.clone(),
                Arc::downgrade(&harness),
                AgentEventStream::new(log.reader()),
                cancel_rx,
                intent_rx,
                DriverHandle::disconnected(),
            )
            .await
            .run(),
        );
        assert_eq!(log.reader_count(), 1);

        let first_turn = TurnId::new();
        for event in turn_events(
            thread_id,
            first_turn,
            "ignored",
            "first",
            TokenUsage::new(1, 1),
        ) {
            assert!(log.append(event));
        }
        wait_for_turn_count(&store, project_id, thread_id, 1).await;

        let orphan = store
            .load_thread(project_id, thread_id)
            .await
            .unwrap()
            .unwrap();
        shared
            .thread_metadata
            .classify_orphan(
                project_id,
                thread_id,
                orphan.revision,
                crate::thread_metadata::OrphanClassification {
                    parent_thread_id,
                    spawned_by_turn_id: first_turn,
                    title: "sub-agent".into(),
                    mode: TurnMode::Known(Mode::Plan),
                    permission_preset: PermissionPreset::AskFirst,
                },
            )
            .await
            .unwrap();
        coordinator.classify_orphan_as_subagent().await.unwrap();

        let second_turn = TurnId::new();
        for event in turn_events(
            thread_id,
            second_turn,
            "ignored",
            "second",
            TokenUsage::new(2, 2),
        ) {
            assert!(log.append(event));
        }
        wait_for_turn_count(&store, project_id, thread_id, 2).await;
        assert_eq!(log.reader_count(), 1);
        log.close();
        forwarder.await.unwrap();

        let turns = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].id, first_turn);
        assert_eq!(
            turns[0].user_input,
            UserInput::text("Unclassified native turn")
        );
        assert_eq!(turns[0].mode, TurnMode::Known(Mode::Build));
        assert_eq!(turns[1].id, second_turn);
        assert_eq!(turns[1].user_input, UserInput::text("Sub-agent turn"));
        assert_eq!(turns[1].mode, TurnMode::Known(Mode::Plan));
    }

    #[tokio::test]
    async fn completed_turn_forwarder_drains_processless_command_completion() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let ledger = ledger::spawn(store.clone());
        let (handle, runtime, _coordinator, authority) = spawn_forwarder_handle_with_runtime(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub,
            store.clone(),
            ledger,
            model,
            "first",
            None,
        );

        let turn = TurnId::new();
        let command_item = ItemId::new();
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
        }));
        assert!(log.append(AgentEvent::ItemStarted {
            thread: thread_id,
            turn,
            item: ItemStart {
                id: command_item,
                harness_item_id: "long_running_command".into(),
                kind: ItemKind::CommandExecution,
                command: Some(CommandExecutionStart {
                    command: "sleep 600".into(),
                    cwd: "/tmp/test".into(),
                    status: Some("running".into()),
                    process_id: None,
                    started_at_ms: Some(1),
                }),
                tool: None,
            },
        }));
        assert!(log.append(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn,
            item: Item {
                id: command_item,
                harness_item_id: "long_running_command".into(),
                payload: ItemPayload::CommandExecution {
                    command: "sleep 600".into(),
                    cwd: "/tmp/test".into(),
                    output: "still running".into(),
                    output_truncated: false,
                    output_original_bytes: None,
                    output_original_lines: None,
                    exit_code: None,
                    status: Some("running".into()),
                    process_id: None,
                    duration_ms: None,
                },
                created_at: Utc::now(),
            },
        }));
        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));

        wait_for_turn_count(&store, project_id, thread_id, 1).await;
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);
        let tasks = loop {
            let tasks = runtime.tasks_snapshot(&authority).1;
            if tasks.first().is_some_and(|task| task.after_turn) {
                break tasks;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "persisted command was not marked after-turn"
            );
            tokio::task::yield_now().await;
        };
        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].after_turn);
        assert!(tasks[0].process_id.is_none());

        // A newer turn may begin while a process from the persisted turn is still running. The
        // old process's terminal replacement must update running-task state without being mistaken
        // for an event belonging to the new turn.
        let next_turn = TurnId::new();
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: next_turn,
        }));

        assert!(log.append(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn,
            item: Item {
                id: command_item,
                harness_item_id: "long_running_command".into(),
                payload: ItemPayload::CommandExecution {
                    command: "sleep 600".into(),
                    cwd: "/tmp/test".into(),
                    output: "done".into(),
                    output_truncated: false,
                    output_original_bytes: None,
                    output_original_lines: None,
                    exit_code: Some(0),
                    status: Some("completed".into()),
                    process_id: None,
                    duration_ms: Some(60_000),
                },
                created_at: Utc::now(),
            },
        }));

        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);
        while !runtime.tasks_snapshot(&authority).1.is_empty() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "old command completion was not applied while the next turn was active"
            );
            tokio::task::yield_now().await;
        }
        assert!(runtime.has_active_turn(&authority));

        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: next_turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));
        wait_for_turn_count(&store, project_id, thread_id, 2).await;
        let turns = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].id, turn);
        assert_eq!(turns[1].id, next_turn);
        assert!(turns[1].items.is_empty());
        log.close();

        tokio::time::timeout(tokio::time::Duration::from_secs(2), handle)
            .await
            .expect("forwarder should exit after the event stream closes")
            .unwrap();

        assert!(runtime.tasks_snapshot(&authority).1.is_empty());
        assert!(!runtime.has_active_turn(&authority));
    }

    #[tokio::test]
    async fn synthesized_interrupted_completion_carries_live_usage() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());
        let (handle, runtime, _coordinator, authority) = spawn_forwarder_handle_with_runtime(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub,
            store.clone(),
            ledger,
            model,
            "incomplete",
            None,
        );

        let turn = TurnId::new();
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
        }));
        let live_usage = TokenUsage {
            input: 120,
            output: 30,
            total: 150,
        };
        assert!(log.append(AgentEvent::TurnUsageUpdated {
            thread: thread_id,
            turn,
            usage: live_usage,
            context_window: Some(258_400),
            model: None,
        }));
        assert!(log.append(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn,
            item: Item {
                id: ItemId::new(),
                harness_item_id: "agent_partial".into(),
                payload: ItemPayload::AgentMessage {
                    text: "partial answer".into(),
                },
                created_at: Utc::now(),
            },
        }));
        assert!(log.append(AgentEvent::ItemStarted {
            thread: thread_id,
            turn,
            item: ItemStart {
                id: ItemId::new(),
                harness_item_id: "partial_command".into(),
                kind: ItemKind::CommandExecution,
                command: Some(CommandExecutionStart {
                    command: "sleep 600".into(),
                    cwd: "/tmp/test".into(),
                    status: Some("running".into()),
                    process_id: Some("proc_partial".into()),
                    started_at_ms: Some(1),
                }),
                tool: None,
            },
        }));
        log.close();

        tokio::time::timeout(tokio::time::Duration::from_secs(2), handle)
            .await
            .expect("forwarder should exit when stream closes")
            .unwrap();

        let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].id, turn);
        assert!(matches!(saved[0].status.kind, TurnStatusKind::Interrupted));
        assert_eq!(saved[0].usage, live_usage);
        assert!(
            std::iter::from_fn(|| client_rx.try_recv().ok()).any(|message| matches!(
                message,
                ServerMessage::Event { agent_event, .. }
                    if matches!(
                        *agent_event,
                        WireAgentEvent::TurnCompleted { usage, .. } if usage == live_usage
                    )
            ))
        );
        assert_eq!(saved[0].items.len(), 1);
        assert!(
            runtime.live_snapshot(&authority).is_none(),
            "synthetic completion should clear live state"
        );

        let tasks = runtime.tasks_snapshot(&authority).1;
        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].after_turn);
    }

    #[tokio::test]
    async fn stream_end_before_native_turn_releases_admitted_intent() {
        let gate = Arc::new(Notify::new());
        let harness = Arc::new(TestIntentHarness::gated(TurnId::new(), gate));
        let (handle, intents, log, shared, authority, _project_id, _thread_id, _temp) =
            running_intent_forwarder(ClassificationPhase::Primary, harness.clone()).await;
        let (intent, response) = start_intent(crate::registry::tests::test_turn_context());
        intents.send(intent).await.unwrap();
        wait_for_call_count(&harness.start_calls, 1).await;
        log.close();

        let reason = tokio::time::timeout(tokio::time::Duration::from_secs(2), handle)
            .await
            .expect("forwarder should exit after the event stream closes")
            .unwrap();
        assert!(matches!(
            reason,
            ForwarderExitReason::StreamEndedWithoutTurn
        ));
        assert!(!shared.runtime.has_active_turn(&authority));
        assert!(matches!(
            response.await.unwrap(),
            Err(HarnessError::Protocol(message))
                if message == "event owner exited before the harness answered"
        ));
    }

    #[tokio::test]
    async fn persisted_turn_command_starts_do_not_recreate_running_tasks() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "target".into(),
                    harness_thread_id: "th_target".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let turn = TurnId::new();
        let item_id = ItemId::new();
        let harness_item_id = "cmd_1".to_string();
        store
            .append_turn(
                project_id,
                thread_id,
                &Turn {
                    id: turn,
                    user_input: UserInput::text("already done"),
                    items: vec![Item {
                        id: item_id,
                        harness_item_id: harness_item_id.clone(),
                        payload: ItemPayload::CommandExecution {
                            command: "sleep 1".into(),
                            cwd: "/tmp/test".into(),
                            output: "done".into(),
                            output_truncated: false,
                            output_original_bytes: None,
                            output_original_lines: None,
                            exit_code: Some(0),
                            status: Some("completed".into()),
                            process_id: Some("proc_1".into()),
                            duration_ms: Some(1_000),
                        },
                        created_at: now,
                    }],
                    model: TurnModel::Known(model.clone()),
                    mode: TurnMode::Known(Mode::Build),
                    status: TurnStatus {
                        kind: TurnStatusKind::Completed,
                        message: None,
                    },
                    usage: TokenUsage::default(),
                    diffs: Vec::new(),
                    started_at: now,
                    completed_at: Some(now),
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let ledger = ledger::spawn(store.clone());

        let (runtime, authority) = spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub,
            store,
            ledger,
            model,
            "next",
        );

        assert!(log.append(AgentEvent::ItemStarted {
            thread: thread_id,
            turn,
            item: ItemStart {
                id: item_id,
                harness_item_id,
                kind: ItemKind::CommandExecution,
                command: Some(CommandExecutionStart {
                    command: "sleep 1".into(),
                    cwd: "/tmp/test".into(),
                    status: Some("in_progress".into()),
                    process_id: Some("proc_1".into()),
                    started_at_ms: Some(1),
                }),
                tool: None,
            },
        }));

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(
            runtime.tasks_snapshot(&authority).1.is_empty(),
            "historical starts for already-persisted turns must not create stale running tasks"
        );
    }

    #[tokio::test]
    async fn persisted_turn_terminal_command_completion_is_broadcast_without_running_task() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "target".into(),
                    harness_thread_id: "th_target".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let turn = TurnId::new();
        let item_id = ItemId::new();
        let harness_item_id = "cmd_late".to_string();
        store
            .append_turn(
                project_id,
                thread_id,
                &Turn {
                    id: turn,
                    user_input: UserInput::text("already persisted"),
                    items: vec![Item {
                        id: item_id,
                        harness_item_id: harness_item_id.clone(),
                        payload: ItemPayload::CommandExecution {
                            command: "<command included NUL byte>".into(),
                            cwd: "/tmp/test".into(),
                            output: String::new(),
                            output_truncated: false,
                            output_original_bytes: None,
                            output_original_lines: None,
                            exit_code: None,
                            status: Some("in_progress".into()),
                            process_id: None,
                            duration_ms: None,
                        },
                        created_at: now,
                    }],
                    model: TurnModel::Known(model.clone()),
                    mode: TurnMode::Known(Mode::Build),
                    status: TurnStatus {
                        kind: TurnStatusKind::Completed,
                        message: None,
                    },
                    usage: TokenUsage::default(),
                    diffs: Vec::new(),
                    started_at: now,
                    completed_at: Some(now),
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());

        let (runtime, authority) = spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub,
            store,
            ledger,
            model,
            "next",
        );

        assert!(runtime.tasks_snapshot(&authority).1.is_empty());
        assert!(log.append(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn,
            item: Item {
                id: item_id,
                harness_item_id,
                payload: ItemPayload::CommandExecution {
                    command: "<command included NUL byte>".into(),
                    cwd: "/tmp/test".into(),
                    output: "failed before spawn".into(),
                    output_truncated: false,
                    output_original_bytes: None,
                    output_original_lines: None,
                    exit_code: Some(1),
                    status: Some("failed".into()),
                    process_id: None,
                    duration_ms: Some(10),
                },
                created_at: now,
            },
        }));

        let message = tokio::time::timeout(tokio::time::Duration::from_secs(1), async {
            loop {
                if let Some(message) = client_rx.recv().await
                    && matches!(
                        &message,
                        ServerMessage::Event {
                            agent_event,
                            ..
                        } if matches!(**agent_event, WireAgentEvent::ItemCompleted { .. })
                    )
                {
                    break message;
                }
            }
        })
        .await
        .expect("late terminal command completion should be broadcast");
        let ServerMessage::Event { agent_event, .. } = message else {
            panic!("expected event");
        };
        let WireAgentEvent::ItemCompleted { item, .. } = *agent_event else {
            panic!("expected item completion");
        };
        assert_eq!(item.id, item_id);
    }

    #[tokio::test]
    async fn live_turn_forwarder_ignores_events_for_other_threads() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let other_thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "target".into(),
                    harness_thread_id: "th_target".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let ledger = ledger::spawn(store.clone());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);

        let (runtime, authority) = spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub,
            store.clone(),
            ledger,
            model,
            "target",
        );
        let foreign_turn = TurnId::new();
        for event in turn_events(
            other_thread_id,
            foreign_turn,
            "foreign",
            "wrong",
            TokenUsage::new(99, 1),
        ) {
            assert!(log.append(event));
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert!(
            saved.is_empty(),
            "events for another thread must not be persisted into the target thread"
        );
        assert!(
            runtime.live_snapshot(&authority).is_none(),
            "events for another thread must not create a live snapshot"
        );
        assert!(
            matches!(
                client_rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "events for another thread must not be broadcast to target-thread subscribers"
        );
    }

    #[tokio::test]
    async fn live_turn_forwarder_rejects_foreign_side_effect_events() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let other_thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "target".into(),
                    harness_thread_id: "th_target".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let ledger = ledger::spawn(store.clone());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);

        let (runtime, authority) = spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub,
            store.clone(),
            ledger,
            model,
            "target",
        );

        let foreign_turn = TurnId::new();
        let foreign_item = ItemId::new();
        let approval_id = ApprovalId("foreign_approval".into());
        let server_request_id = ServerRequestId("foreign_request".into());
        let foreign_events = vec![
            AgentEvent::Notice {
                thread: other_thread_id,
                turn: None,
                message: "wrong thread notice".into(),
            },
            AgentEvent::Error {
                thread: other_thread_id,
                turn: None,
                error: HarnessError::Protocol("wrong thread error".into()),
            },
            AgentEvent::ApprovalRequested {
                thread: other_thread_id,
                turn: foreign_turn,
                request: ApprovalRequest {
                    id: approval_id.clone(),
                    kind: ApprovalKind::CommandExecution {
                        command: "sleep 60".into(),
                        cwd: "/tmp/test".into(),
                    },
                    reason: Some("wrong thread approval".into()),
                    metadata: Vec::new(),
                    available: vec![ApprovalDecision::Accept, ApprovalDecision::Cancel],
                },
            },
            AgentEvent::ServerRequestReceived {
                thread: other_thread_id,
                turn: Some(foreign_turn),
                request: ServerRequest {
                    id: server_request_id.clone(),
                    method: "tool/request_user_input".into(),
                    params: serde_json::json!({"message": "wrong thread request"}),
                    received_at: Utc::now(),
                },
            },
            AgentEvent::ItemStarted {
                thread: other_thread_id,
                turn: foreign_turn,
                item: ItemStart {
                    id: foreign_item,
                    harness_item_id: "foreign_command".into(),
                    kind: ItemKind::CommandExecution,
                    command: Some(CommandExecutionStart {
                        command: "sleep 60".into(),
                        cwd: "/tmp/test".into(),
                        status: Some("running".into()),
                        process_id: Some("foreign_process".into()),
                        started_at_ms: Some(1),
                    }),
                    tool: None,
                },
            },
        ];

        for event in foreign_events {
            assert!(log.append(event));
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(
            store
                .load_all_turns(project_id, thread_id)
                .await
                .unwrap()
                .is_empty(),
            "foreign events must not be persisted into the target thread"
        );
        assert!(
            runtime.live_snapshot(&authority).is_none(),
            "foreign events must not create target-thread live state"
        );
        assert!(
            runtime.tasks_snapshot(&authority).1.is_empty(),
            "foreign running commands must not appear in the target-thread task list"
        );
        assert!(
            matches!(
                client_rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "foreign notices/errors must not be broadcast to target-thread subscribers"
        );
    }

    #[tokio::test]
    async fn forwarder_broadcasts_turnless_server_request_before_turn_start() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "target".into(),
                    harness_thread_id: "th_target".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let ledger = ledger::spawn(store.clone());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);

        let (runtime, authority) = spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub,
            store.clone(),
            ledger,
            model,
            "target",
        );

        let request_id = ServerRequestId("turnless_request".into());
        assert!(log.append(AgentEvent::ServerRequestReceived {
            thread: thread_id,
            turn: None,
            request: ServerRequest {
                id: request_id.clone(),
                method: "mcpServer/elicitation/request".into(),
                params: serde_json::json!({
                    "message": "Allow cf-mcp to run tool \"wiki_search\"?"
                }),
                received_at: Utc::now(),
            },
        }));

        tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
            loop {
                match client_rx
                    .recv()
                    .await
                    .expect("subscriber should remain connected")
                {
                    ServerMessage::Event { agent_event, .. } => match *agent_event {
                        WireAgentEvent::ServerRequestReceived { turn, request, .. } => {
                            assert!(turn.is_none());
                            assert_eq!(request.id, request_id);
                            assert_eq!(request.method, "mcpServer/elicitation/request");
                            break;
                        }
                        other => panic!("expected turnless server request event, got {other:?}"),
                    },
                    ServerMessage::RequestState(_) => {}
                    other => panic!("expected turnless server request event, got {other:?}"),
                }
            }
        })
        .await
        .expect("normal forwarder should broadcast the turnless request");

        assert!(
            store
                .load_all_turns(project_id, thread_id)
                .await
                .unwrap()
                .is_empty(),
            "turnless request alone must not persist a turn"
        );
        assert!(
            runtime.live_snapshot(&authority).is_none(),
            "turnless request alone must not create target-thread live turn state"
        );
    }

    #[tokio::test]
    async fn forwarder_applied_harness_resolution_does_not_break_a_pending_claim() {
        let (_tmp, store, project_id, thread_id, model) = usage_forwarder_fixture().await;
        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());
        let (runtime, authority) = spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub.clone(),
            store,
            ledger,
            model,
            "request claim",
        );
        let turn = TurnId::new();
        let request_id = ServerRequestId("q".into());
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
        }));
        assert!(log.append(AgentEvent::ServerRequestReceived {
            thread: thread_id,
            turn: Some(turn),
            request: ServerRequest {
                id: request_id.clone(),
                method: "tool/request_user_input".into(),
                params: serde_json::json!({"message": "Choose"}),
                received_at: Utc::now(),
            },
        }));

        let pending = tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
            loop {
                if let Some(ServerMessage::RequestState(state)) = client_rx.recv().await
                    && state.request_id == request_id.0
                {
                    break state;
                }
            }
        })
        .await
        .expect("forwarder should publish the pending request");
        assert_eq!(pending.revision, 1);
        assert!(matches!(pending.status, WireRequestStatus::Pending));

        let (claim, responding) = runtime
            .claim_request(&authority, RuntimeRequestId::Server(request_id.clone()))
            .unwrap();
        assert_eq!(responding.request_state.revision, 2);
        assert!(matches!(
            responding.request_state.status,
            WireRequestStatus::Responding
        ));
        hub.broadcast(
            thread_id,
            ServerMessage::RequestState(responding.request_state),
        )
        .await;

        assert!(log.append(AgentEvent::ServerRequestResolved {
            thread: thread_id,
            turn: Some(turn),
            request_id: request_id.clone(),
        }));
        assert!(log.append(AgentEvent::Notice {
            thread: thread_id,
            turn: Some(turn),
            message: "fence-13".into(),
        }));

        let request_states = tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
            let mut request_states = Vec::new();
            loop {
                match client_rx
                    .recv()
                    .await
                    .expect("subscriber should remain connected")
                {
                    ServerMessage::RequestState(state) => request_states.push(state),
                    ServerMessage::Event { agent_event, .. }
                        if matches!(
                            *agent_event,
                            WireAgentEvent::Notice { ref message, .. }
                                if message == "fence-13"
                        ) =>
                    {
                        break request_states;
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("forwarder should apply the resolution before the notice fence");
        assert_eq!(request_states.len(), 1);
        assert_eq!(request_states[0].revision, 2);
        assert!(matches!(
            request_states[0].status,
            WireRequestStatus::Responding
        ));

        let committed = claim
            .commit(RequestResolution::Server(ServerRequestResponse::result(
                serde_json::json!({"answer": 1}),
            )))
            .unwrap();
        assert_eq!(committed.request_state.revision, 3);
        assert!(matches!(
            committed.request_state.status,
            WireRequestStatus::Resolved { .. }
        ));
        assert!(matches!(
            client_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn forwarder_publishes_harness_resolution_for_an_unclaimed_request() {
        let (_tmp, store, project_id, thread_id, model) = usage_forwarder_fixture().await;
        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());
        let (_runtime, _authority) = spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub,
            store,
            ledger,
            model,
            "unclaimed request",
        );
        let turn = TurnId::new();
        let request_id = ServerRequestId("q".into());
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
        }));
        assert!(log.append(AgentEvent::ServerRequestReceived {
            thread: thread_id,
            turn: Some(turn),
            request: ServerRequest {
                id: request_id.clone(),
                method: "tool/request_user_input".into(),
                params: serde_json::json!({"message": "Choose"}),
                received_at: Utc::now(),
            },
        }));
        assert!(log.append(AgentEvent::ServerRequestResolved {
            thread: thread_id,
            turn: Some(turn),
            request_id: request_id.clone(),
        }));

        let states = tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
            let mut states = Vec::new();
            loop {
                if let Some(ServerMessage::RequestState(state)) = client_rx.recv().await
                    && state.request_id == request_id.0
                {
                    let resolved = matches!(state.status, WireRequestStatus::Resolved { .. });
                    states.push(state);
                    if resolved {
                        break states;
                    }
                }
            }
        })
        .await
        .expect("forwarder should publish the harness resolution");
        assert_eq!(states.len(), 2);
        assert_eq!(states[0].revision, 1);
        assert!(matches!(states[0].status, WireRequestStatus::Pending));
        assert_eq!(states[1].revision, 2);
        assert!(matches!(
            states[1].status,
            WireRequestStatus::Resolved { .. }
        ));
    }

    #[tokio::test]
    async fn forwarder_deduplicates_notices_per_turn_and_resets_between_turns() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());

        spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub,
            store,
            ledger,
            model,
            "compact",
        );

        let turn = TurnId::new();
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
        }));
        for _ in 0..2 {
            assert!(log.append(AgentEvent::Notice {
                thread: thread_id,
                turn: Some(turn),
                message: "Heads up: Long threads and multiple compactions can cause drift.".into(),
            }));
        }
        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));

        let mut notice_count = 0;
        let mut completed = false;
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline && !completed {
            match tokio::time::timeout(tokio::time::Duration::from_secs(1), client_rx.recv()).await
            {
                Ok(Some(ServerMessage::Event { agent_event, .. })) => match *agent_event {
                    WireAgentEvent::Notice { .. } => notice_count += 1,
                    WireAgentEvent::TurnCompleted { .. } => completed = true,
                    _ => {}
                },
                Ok(Some(_)) => {}
                _ => {}
            }
        }

        assert!(completed, "turn should complete");
        assert_eq!(notice_count, 1);

        let next_turn = TurnId::new();
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: next_turn,
        }));
        assert!(log.append(AgentEvent::Notice {
            thread: thread_id,
            turn: Some(next_turn),
            message: "Heads up: Long threads and multiple compactions can cause drift.".into(),
        }));
        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: next_turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));

        let mut second_notice_count = 0;
        loop {
            if let ServerMessage::Event { agent_event, .. } =
                tokio::time::timeout(tokio::time::Duration::from_secs(5), client_rx.recv())
                    .await
                    .expect("second turn events should arrive")
                    .expect("subscriber should remain connected")
            {
                match *agent_event {
                    WireAgentEvent::Notice { .. } => second_notice_count += 1,
                    WireAgentEvent::TurnCompleted { turn, .. } if turn == next_turn => break,
                    _ => {}
                }
            }
        }
        assert_eq!(second_notice_count, 1);
    }

    #[tokio::test]
    async fn forwarder_gap_recovers_but_truncates_the_interrupted_native_turn() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "lag test".into(),
                    harness_thread_id: "th-lag".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::with_limit(2));
        let stream = AgentEventStream::new(log.reader());
        for sequence in 0..3 {
            assert!(log.append(AgentEvent::Notice {
                thread: thread_id,
                turn: None,
                message: format!("queued notice {sequence}"),
            }));
        }
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());
        let (forwarder, runtime, _coordinator, _authority) = spawn_forwarder_handle_with_runtime(
            thread_id,
            project_id,
            stream,
            hub,
            store.clone(),
            ledger,
            model,
            "lag test",
            None,
        );
        drop(forwarder);

        tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                if matches!(
                    client_rx.recv().await,
                    Some(ServerMessage::Event { agent_event, .. })
                        if matches!(*agent_event, WireAgentEvent::Notice { .. })
                ) {
                    break;
                }
            }
        })
        .await
        .expect("the forwarder should continue after reporting lag");

        let turn = TurnId::new();
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
        }));
        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));

        tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                if store
                    .load_all_turns(project_id, thread_id)
                    .await
                    .unwrap()
                    .iter()
                    .any(|saved| saved.id == turn)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a turn after lag should be persisted normally");

        let lagged_turn = TurnId::new();
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: lagged_turn,
        }));
        tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                if runtime.current_overview().threads.iter().any(|summary| {
                    summary.thread_id == thread_id
                        && matches!(summary.turn_state, giskard_proto::RuntimeTurnState::Active { turn_id: Some(id) } if id == lagged_turn)
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the native turn should become active before forcing lag");

        for sequence in 0..3 {
            assert!(log.append(AgentEvent::Notice {
                thread: thread_id,
                turn: Some(lagged_turn),
                message: format!("lagging active turn {sequence}"),
            }));
        }
        tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                if store
                    .load_all_turns(project_id, thread_id)
                    .await
                    .unwrap()
                    .iter()
                    .any(|saved| {
                        saved.id == lagged_turn && saved.status.kind == TurnStatusKind::Interrupted
                    })
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("lag should persist the active native turn as interrupted");

        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: lagged_turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));
        assert!(log.append(AgentEvent::Notice {
            thread: thread_id,
            turn: None,
            message: "completion fence after lag".into(),
        }));
        tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                if matches!(
                    client_rx.recv().await,
                    Some(ServerMessage::Event { agent_event, .. })
                        if matches!(&*agent_event, WireAgentEvent::Notice { message, .. } if message == "completion fence after lag")
                ) {
                    break;
                }
            }
        })
        .await
        .expect("the owner should consume the real completion after lag");
        let following_turn = TurnId::new();
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: following_turn,
        }));
        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: following_turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));
        tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                let turns = store.load_all_turns(project_id, thread_id).await.unwrap();
                if turns.iter().any(|saved| saved.id == following_turn) {
                    let lagged = turns.iter().find(|saved| saved.id == lagged_turn).unwrap();
                    assert_eq!(lagged.status.kind, TurnStatusKind::Interrupted);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the real completion is ignored and later native turns still proceed");
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_forwarder(
        thread_id: ThreadId,
        project_id: ProjectId,
        stream: AgentEventStream,
        hub: Arc<Hub>,
        store: Arc<PersistStore>,
        ledger: ledger::LedgerHandle,
        model: ModelRef,
        user_input: &str,
    ) -> (Arc<ThreadRuntimeSupport>, Arc<super::ThreadAuthority>) {
        let (handle, runtime, _coordinator, authority) = spawn_forwarder_handle_with_runtime(
            thread_id, project_id, stream, hub, store, ledger, model, user_input, None,
        );
        std::mem::drop(handle);
        (runtime, authority)
    }

    async fn usage_forwarder_fixture() -> (
        tempfile::TempDir,
        Arc<PersistStore>,
        ProjectId,
        ThreadId,
        ModelRef,
    ) {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "usage".into(),
                    harness_thread_id: "usage-native".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();
        (tmp, store, project_id, thread_id, model)
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_forwarder_handle(
        thread_id: ThreadId,
        project_id: ProjectId,
        stream: AgentEventStream,
        hub: Arc<Hub>,
        store: Arc<PersistStore>,
        ledger: ledger::LedgerHandle,
        model: ModelRef,
        user_input: &str,
    ) -> JoinHandle<()> {
        spawn_forwarder_handle_with_runtime(
            thread_id, project_id, stream, hub, store, ledger, model, user_input, None,
        )
        .0
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_forwarder_handle_with_runtime(
        thread_id: ThreadId,
        project_id: ProjectId,
        stream: AgentEventStream,
        hub: Arc<Hub>,
        store: Arc<PersistStore>,
        ledger: ledger::LedgerHandle,
        model: ModelRef,
        user_input: &str,
        harness_turn_id: Option<TurnId>,
    ) -> (
        JoinHandle<()>,
        Arc<ThreadRuntimeSupport>,
        Arc<super::ThreadCoordinator>,
        Arc<super::ThreadAuthority>,
    ) {
        let ctx = TurnContext {
            user_input: UserInput::text(user_input),
            model: TurnModel::Known(model.clone()),
            mode: TurnMode::Known(Mode::Build),
            kind: TurnContextKind::User,
        };
        let shared = super::RegistryShared::new(hub, store, ledger);
        let shared = Arc::new(shared);
        let runtime = shared.runtime.clone();
        let native_handle = ThreadHandle::detached(thread_id, format!("native-{thread_id}"));
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let (intent_tx, intent_rx) = mpsc::channel(crate::registry::thread::TURN_INTENT_CAPACITY);
        let coordinator = Arc::new(super::ThreadCoordinator::new_live(
            super::LoadedThreadBinding {
                project_id,
                handle: native_handle.clone(),
                native_model: Some(model),
            },
            super::ClassificationPhase::Primary,
            cancel_tx,
            intent_tx.clone(),
        ));
        let authority = Arc::new(super::ThreadAuthority::new_for_test(thread_id, project_id));
        let coordinator_for_task = coordinator.clone();
        let task_authority = authority.clone();
        let harness: Arc<dyn AgentHarness> = Arc::new(TestIntentHarness::accepting(
            harness_turn_id.unwrap_or_default(),
        ));
        let weak_harness = Arc::downgrade(&harness);
        let handle = tokio::spawn(async move {
            let (reply, _response) = oneshot::channel();
            intent_tx
                .send(TurnIntent::StartTurn {
                    input: ctx.user_input.clone(),
                    overrides: giskard_core::turn::TurnOverrides {
                        model: None,
                        mode: Mode::Build,
                        permission_preset: PermissionPreset::AskFirst,
                    },
                    context: ctx.clone(),
                    reply,
                })
                .await
                .unwrap();
            ThreadEventForwarder::new(
                shared,
                task_authority,
                coordinator_for_task,
                weak_harness,
                stream,
                cancel_rx,
                intent_rx,
                DriverHandle::disconnected(),
            )
            .await
            .run()
            .await;
            drop(harness);
        });
        (handle, runtime, coordinator, authority)
    }

    /// Drive an owner over a promptless externally started turn, the way a provider-owned thread
    /// arrives: no admitted intent, so the turn is labelled from the coordinator's classification
    /// alone.
    async fn persist_external_turn_input(classification: super::ClassificationPhase) -> UserInput {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let turn = TurnId::new();
        let ledger = ledger::spawn(store.clone());
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger,
        ));
        let native_handle = ThreadHandle::detached(thread_id, format!("native-{thread_id}"));
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let (intent_tx, intent_rx) = mpsc::channel(crate::registry::thread::TURN_INTENT_CAPACITY);
        let coordinator = Arc::new(super::ThreadCoordinator::new_live(
            super::LoadedThreadBinding {
                project_id,
                handle: native_handle,
                native_model: Some(ModelRef {
                    provider: "openai".into(),
                    model: "test".into(),
                    reasoning_effort: None,
                }),
            },
            classification,
            cancel_tx,
            intent_tx,
        ));
        let authority = Arc::new(super::ThreadAuthority::new_for_test(thread_id, project_id));
        let log = Arc::new(EventLog::new());
        let harness: Arc<dyn AgentHarness> = Arc::new(TestIntentHarness::accepting(TurnId::new()));
        let weak_harness = Arc::downgrade(&harness);
        let forwarder = tokio::spawn(
            ThreadEventForwarder::new(
                shared,
                authority,
                coordinator,
                weak_harness,
                AgentEventStream::new(log.reader()),
                cancel_rx,
                intent_rx,
                DriverHandle::disconnected(),
            )
            .await
            .run(),
        );
        for event in turn_events(
            thread_id,
            turn,
            "ignored",
            "external output",
            TokenUsage::new(1, 1),
        ) {
            assert!(log.append(event));
        }
        log.close();
        forwarder.await.unwrap();
        drop(harness);

        let turns = store.load_all_turns(project_id, thread_id).await.unwrap();
        turns
            .into_iter()
            .find(|saved| saved.id == turn)
            .expect("the external turn should be persisted")
            .user_input
    }

    #[tokio::test]
    async fn an_unclassified_native_turn_does_not_claim_to_be_a_sub_agent() {
        let subagent = persist_external_turn_input(super::ClassificationPhase::Subagent).await;
        assert_eq!(subagent.as_text().unwrap(), "Sub-agent turn");
        let orphan = persist_external_turn_input(super::ClassificationPhase::Orphan).await;
        assert_eq!(orphan.as_text().unwrap(), "Unclassified native turn");
    }

    fn turn_events(
        thread: ThreadId,
        turn: TurnId,
        input: &str,
        output: &str,
        usage: TokenUsage,
    ) -> Vec<AgentEvent> {
        let now = Utc::now();
        vec![
            AgentEvent::TurnStarted { thread, turn },
            AgentEvent::ItemCompleted {
                thread,
                turn,
                item: Item {
                    id: ItemId::new(),
                    harness_item_id: format!("user_{input}"),
                    payload: ItemPayload::UserMessage { text: input.into() },
                    created_at: now,
                },
            },
            AgentEvent::ItemCompleted {
                thread,
                turn,
                item: Item {
                    id: ItemId::new(),
                    harness_item_id: format!("agent_{output}"),
                    payload: ItemPayload::AgentMessage {
                        text: output.into(),
                    },
                    created_at: now,
                },
            },
            AgentEvent::TurnCompleted {
                thread,
                turn,
                usage,
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
            },
        ]
    }

    #[tokio::test]
    async fn forwarder_upserts_items_and_drops_conflicting_native_identity() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let first_turn = TurnId::new();
        let reused_harness = "agent_reply".to_string();
        let first_item_id = ItemId::new();
        let second_item_id = ItemId::new();
        let conflicting_item_id = ItemId::new();

        store
            .append_turn(
                project_id,
                thread_id,
                &Turn {
                    id: first_turn,
                    user_input: UserInput::text("first"),
                    items: vec![Item {
                        id: first_item_id,
                        harness_item_id: reused_harness.clone(),
                        payload: ItemPayload::AgentMessage {
                            text: "first answer".into(),
                        },
                        created_at: now,
                    }],
                    model: TurnModel::Known(model.clone()),
                    mode: TurnMode::Known(Mode::Build),
                    status: TurnStatus {
                        kind: TurnStatusKind::Completed,
                        message: None,
                    },
                    usage: TokenUsage::new(1, 1),
                    diffs: vec![],
                    started_at: now,
                    completed_at: Some(now),
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(64);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());

        spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub,
            store.clone(),
            ledger,
            model,
            "second",
        );

        let second_turn = TurnId::new();
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: second_turn,
        }));
        // Two ItemCompleted events for the same harness id within the new turn: this should
        // upsert to a single persisted item carrying the latest payload, while the earlier
        // persisted turn keeps its own distinct item.
        assert!(log.append(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn: second_turn,
            item: Item {
                id: second_item_id,
                harness_item_id: reused_harness.clone(),
                payload: ItemPayload::AgentMessage {
                    text: "first version in second turn".into(),
                },
                created_at: now,
            },
        }));
        assert!(log.append(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn: second_turn,
            item: Item {
                id: second_item_id,
                harness_item_id: reused_harness.clone(),
                payload: ItemPayload::AgentMessage {
                    text: "second version in second turn".into(),
                },
                created_at: now,
            },
        }));
        assert!(log.append(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn: second_turn,
            item: Item {
                id: conflicting_item_id,
                harness_item_id: reused_harness.clone(),
                payload: ItemPayload::AgentMessage {
                    text: "conflicting identity".into(),
                },
                created_at: now,
            },
        }));
        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: second_turn,
            usage: TokenUsage::new(2, 2),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));

        wait_for_turn_count(&store, project_id, thread_id, 2).await;
        let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(saved.len(), 2);
        assert_eq!(saved[0].id, first_turn);
        assert_eq!(saved[1].id, second_turn);
        assert_eq!(saved[0].items.len(), 1);
        assert_eq!(
            saved[1].items.len(),
            1,
            "repeated harness id in same turn should upsert to one item"
        );
        assert_eq!(saved[1].items[0].id, second_item_id);
        assert!(
            matches!(
                &saved[1].items[0].payload,
                ItemPayload::AgentMessage { text } if text == "second version in second turn"
            ),
            "upsert should keep the latest occurrence within the turn"
        );
        assert!(
            saved[0].items[0].id == first_item_id,
            "earlier turn item must remain untouched"
        );
        while let Ok(message) = client_rx.try_recv() {
            if let ServerMessage::Event { agent_event, .. } = message
                && let WireAgentEvent::ItemCompleted { item, .. } = *agent_event
            {
                assert_ne!(
                    item.id, conflicting_item_id,
                    "conflicting native identity must not be broadcast"
                );
            }
        }
    }

    #[tokio::test]
    async fn forwarder_forwards_item_started_and_delta_for_harness_id_reused_across_turns() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let first_turn = TurnId::new();
        let reused_harness = "agent_stream".to_string();
        let first_item_id = ItemId::new();

        store
            .append_turn(
                project_id,
                thread_id,
                &Turn {
                    id: first_turn,
                    user_input: UserInput::text("first"),
                    items: vec![Item {
                        id: first_item_id,
                        harness_item_id: reused_harness.clone(),
                        payload: ItemPayload::AgentMessage {
                            text: "first answer".into(),
                        },
                        created_at: now,
                    }],
                    model: TurnModel::Known(model.clone()),
                    mode: TurnMode::Known(Mode::Build),
                    status: TurnStatus {
                        kind: TurnStatusKind::Completed,
                        message: None,
                    },
                    usage: TokenUsage::new(1, 1),
                    diffs: vec![],
                    started_at: now,
                    completed_at: Some(now),
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(64);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());

        spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub,
            store.clone(),
            ledger,
            model,
            "second",
        );

        let second_turn = TurnId::new();
        let second_item_id = ItemId::new();
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: second_turn,
        }));
        assert!(log.append(AgentEvent::ItemStarted {
            thread: thread_id,
            turn: second_turn,
            item: ItemStart {
                id: second_item_id,
                harness_item_id: reused_harness.clone(),
                kind: ItemKind::AgentMessage,
                command: None,
                tool: None,
            },
        }));
        assert!(log.append(AgentEvent::ItemDelta {
            thread: thread_id,
            turn: second_turn,
            item_id: second_item_id,
            delta: giskard_core::item::ItemDelta::Text {
                text: "streaming".into(),
            },
        }));
        assert!(log.append(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn: second_turn,
            item: Item {
                id: second_item_id,
                harness_item_id: reused_harness.clone(),
                payload: ItemPayload::AgentMessage {
                    text: "second answer".into(),
                },
                created_at: now,
            },
        }));
        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: second_turn,
            usage: TokenUsage::new(2, 2),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));

        wait_for_turn_count(&store, project_id, thread_id, 2).await;

        // Collect broadcast events for the new turn and ensure the reused harness id did not
        // cause ItemStarted/ItemDelta/ItemCompleted to be suppressed.
        let mut saw_started = false;
        let mut saw_delta = false;
        let mut saw_completed = false;
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            if let Ok(Some(ServerMessage::Event { agent_event, .. })) =
                tokio::time::timeout(tokio::time::Duration::from_millis(100), client_rx.recv())
                    .await
            {
                match *agent_event {
                    WireAgentEvent::ItemStarted { item, .. }
                        if item.harness_item_id == reused_harness =>
                    {
                        saw_started = true;
                    }
                    WireAgentEvent::ItemDelta { item_id, .. } if item_id == second_item_id => {
                        saw_delta = true;
                    }
                    WireAgentEvent::ItemCompleted { item, .. }
                        if item.harness_item_id == reused_harness =>
                    {
                        saw_completed = true;
                    }
                    _ => {}
                }
            }
        }

        assert!(
            saw_started,
            "ItemStarted for reused harness id must be forwarded"
        );
        assert!(
            saw_delta,
            "ItemDelta for reused harness id must be forwarded"
        );
        assert!(
            saw_completed,
            "ItemCompleted for reused harness id must be forwarded"
        );

        let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(saved[1].items.len(), 1);
        assert_eq!(saved[1].items[0].id, second_item_id);
        assert!(
            saved[0].items[0].id == first_item_id,
            "earlier turn item must remain untouched"
        );
    }

    #[tokio::test]
    async fn forwarder_upserts_item_deltas_for_repeated_item_id_within_turn() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(64);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());

        spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(log.reader()),
            hub,
            store.clone(),
            ledger,
            model,
            "delta-upsert",
        );

        let turn = TurnId::new();
        let item_id = ItemId::new();
        let harness = "agent_text";
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
        }));
        assert!(log.append(AgentEvent::ItemStarted {
            thread: thread_id,
            turn,
            item: ItemStart {
                id: item_id,
                harness_item_id: harness.into(),
                kind: ItemKind::AgentMessage,
                command: None,
                tool: None,
            },
        }));
        assert!(log.append(AgentEvent::ItemDelta {
            thread: thread_id,
            turn,
            item_id,
            delta: giskard_core::item::ItemDelta::Text {
                text: "first".into(),
            },
        }));
        assert!(log.append(AgentEvent::ItemDelta {
            thread: thread_id,
            turn,
            item_id,
            delta: giskard_core::item::ItemDelta::Text {
                text: " second".into(),
            },
        }));
        assert!(log.append(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn,
            item: Item {
                id: item_id,
                harness_item_id: harness.into(),
                payload: ItemPayload::AgentMessage {
                    text: "final".into(),
                },
                created_at: now,
            },
        }));
        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn,
            usage: TokenUsage::new(3, 3),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));

        wait_for_turn_count(&store, project_id, thread_id, 1).await;

        // Collect broadcast events before querying persistence; the live buffer may already have
        // been cleared by the time the persisted turn is visible.
        let mut delta_texts = Vec::new();
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            if let Ok(Some(ServerMessage::Event { agent_event, .. })) =
                tokio::time::timeout(tokio::time::Duration::from_millis(100), client_rx.recv())
                    .await
                && let WireAgentEvent::ItemDelta {
                    delta: giskard_proto::ItemDelta::Text { text },
                    ..
                } = *agent_event
            {
                delta_texts.push(text);
            }
        }
        assert_eq!(
            delta_texts.len(),
            2,
            "both deltas for the same item id must be forwarded"
        );
        assert_eq!(delta_texts.concat(), "first second");

        let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].items.len(), 1);
        assert_eq!(saved[0].items[0].id, item_id);
    }

    #[tokio::test]
    async fn replacement_forwarder_persists_events_sent_while_no_forwarder_ran() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "gap test".into(),
                    harness_thread_id: "th-gap".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let log = Arc::new(EventLog::new());
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(64);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());
        let turn = TurnId::new();
        let (first_forwarder, _runtime, coordinator, _authority) =
            spawn_forwarder_handle_with_runtime(
                thread_id,
                project_id,
                AgentEventStream::new(log.reader()),
                hub.clone(),
                store.clone(),
                ledger.clone(),
                model.clone(),
                "first",
                Some(turn),
            );

        let completed_item = |harness_item_id: &str| AgentEvent::ItemCompleted {
            thread: thread_id,
            turn,
            item: Item {
                id: ItemId::new(),
                harness_item_id: harness_item_id.to_owned(),
                payload: ItemPayload::AgentMessage {
                    text: harness_item_id.to_owned(),
                },
                created_at: Utc::now(),
            },
        };
        assert!(log.append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
        }));
        assert!(log.append(completed_item("before")));
        tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                if matches!(
                    client_rx.recv().await,
                    Some(ServerMessage::Event { agent_event, .. })
                        if matches!(&*agent_event, WireAgentEvent::ItemCompleted { item, .. }
                            if item.harness_item_id == "before")
                ) {
                    break;
                }
            }
        })
        .await
        .expect("the first forwarder should consume the item before the gap");

        let (reply, response) = tokio::sync::oneshot::channel();
        let _ = coordinator.request_detach(reply).await;
        first_forwarder.await.unwrap();
        let outcome = coordinator
            .owner_exited(ForwarderExitReason::StreamEndedWithoutTurn)
            .await;
        if let super::thread::OwnerExitOutcome::Detached(waiters) = outcome {
            for waiter in waiters {
                let _ = waiter.send(());
            }
        }
        response.await.unwrap();

        assert!(log.append(completed_item("during-gap")));

        let (_second_forwarder, _runtime, _coordinator, _authority) =
            spawn_forwarder_handle_with_runtime(
                thread_id,
                project_id,
                AgentEventStream::new(log.reader()),
                hub,
                store.clone(),
                ledger,
                model,
                "second",
                Some(turn),
            );
        assert!(log.append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));

        wait_for_turn_count(&store, project_id, thread_id, 1).await;
        let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
        let item_ids: Vec<&str> = saved[0]
            .items
            .iter()
            .map(|item| item.harness_item_id.as_str())
            .collect();
        assert!(
            item_ids.contains(&"during-gap"),
            "an event sent while no forwarder ran must reach the replacement; persisted items: {item_ids:?}"
        );
    }

    async fn wait_for_turn_count(
        store: &PersistStore,
        project_id: ProjectId,
        thread_id: ThreadId,
        count: usize,
    ) {
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        loop {
            let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
            if saved.len() >= count {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timed out waiting for {count} persisted turns");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }
}
