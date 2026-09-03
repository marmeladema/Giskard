//! Process-local authority for a thread while the server is running.
//!
//! M2 moves turn ownership, reconnect state, tasks, and requests behind this object. Callers use
//! narrow projections and transitions rather than coordinating the underlying state independently.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use giskard_core::approval::{ApprovalDecision, ApprovalRequest};
use giskard_core::diff::{CapturedDiffDescriptor, CapturedDiffRecord};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ProjectId, ServerRequestId, ThreadId, TurnId};
use giskard_core::ids::{DiffId, ItemId};
use giskard_core::item::{Item, ItemPayload, command_status_is_running};
use giskard_core::server_request::{ServerRequest, ServerRequestResponse};
use giskard_core::turn::{Turn, TurnMode, TurnModel};
use giskard_core::user_input::UserInput;
use sha2::{Digest, Sha256};
use tracing::{debug, warn};

use crate::log_fields::display_opt;
use crate::registry::ThreadAuthority;
use crate::runtime_live::LiveTurnState;
use crate::runtime_tasks::RunningTaskState;
use giskard_proto::{LiveTurnSnapshot, RunningTask};
use giskard_proto::{
    OutstandingRequest, RequestKind, RequestPayload as WireRequestPayload,
    RequestResolution as WireRequestResolution, RequestState as WireRequestState,
    RequestStatus as WireRequestStatus, RuntimeTurnState, ThreadRuntimeOverview,
    ThreadRuntimeSummary, WireApprovalRequest,
};

pub(crate) struct ThreadRuntimeSupport {
    // Cross-thread derived projection; entity-local runtime state lives on ThreadAuthority.
    overview: Arc<Mutex<OverviewState>>,
    max_command_output_bytes: usize,
}

#[derive(Default)]
pub(crate) struct ThreadRuntimeEntry {
    active_turn: Option<ActiveTurnOwner>,
    lifecycle_revision: u64,
    requests: HashMap<RuntimeRequestId, RequestRecord>,
    event_sequence: u64,
    task_revision: u64,
    live: LiveTurnState,
    tasks: RunningTaskState,
    captured_diffs: HashMap<TurnId, ActiveCapturedDiffs>,
    command_outputs: HashMap<(TurnId, ItemId), RuntimeCommandOutput>,
    tool_outputs: HashMap<(TurnId, ItemId), RuntimeToolOutput>,
    persisted_command_output_versions: HashMap<(TurnId, ItemId), String>,
}

/// Optional runtime-entry storage owned by a thread authority.
pub(crate) struct ThreadRuntimeSlot {
    current: Mutex<Option<Arc<Mutex<ThreadRuntimeEntry>>>>,
}

impl ThreadRuntimeSlot {
    /// Creates an empty runtime slot without allocating an entry.
    pub(crate) fn new() -> Self {
        Self {
            current: Mutex::new(None),
        }
    }

    /// Clones the current entry without creating one.
    pub(crate) fn current(&self) -> Option<Arc<Mutex<ThreadRuntimeEntry>>> {
        lock_unpoison(&self.current, "thread runtime slot").clone()
    }

    /// Returns the current entry or installs one while holding the slot lock.
    pub(crate) fn get_or_create(&self) -> Arc<Mutex<ThreadRuntimeEntry>> {
        let mut slot = lock_unpoison(&self.current, "thread runtime slot");
        slot.get_or_insert_with(Default::default).clone()
    }

    /// Removes and returns the current entry.
    pub(crate) fn take(&self) -> Option<Arc<Mutex<ThreadRuntimeEntry>>> {
        lock_unpoison(&self.current, "thread runtime slot").take()
    }

    /// Invokes `callback` with both non-reentrant slot and entry locks held. The callback must not
    /// re-enter this slot through the authority's runtime-entry operations.
    pub(crate) fn with_exact_current<R>(
        &self,
        expected: &Arc<Mutex<ThreadRuntimeEntry>>,
        callback: impl FnOnce(&mut ThreadRuntimeEntry) -> R,
    ) -> Option<R> {
        let slot = lock_unpoison(&self.current, "thread runtime slot");
        let current = slot.as_ref()?;
        if !Arc::ptr_eq(current, expected) {
            return None;
        }
        let mut entry = lock_unpoison(current, "thread runtime entry");
        Some(callback(&mut entry))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeCommandOutput {
    pub output: String,
    pub output_truncated: bool,
    pub original_bytes: u64,
    pub original_lines: u64,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeToolOutput {
    pub bytes: Vec<u8>,
    pub descriptor: giskard_proto::WireToolOutput,
}

pub(crate) struct PreparedItemOutput {
    turn_id: TurnId,
    item_id: ItemId,
    command_runtime: Option<RuntimeCommandOutput>,
    command_descriptor: Option<giskard_core::CommandOutputDescriptor>,
    tool_runtime: Option<RuntimeToolOutput>,
    tool_descriptor: Option<giskard_proto::WireToolOutput>,
    command_item: bool,
    live_event: Option<AgentEvent>,
}

pub(crate) enum RuntimeCommandOutputLookup {
    Found(RuntimeCommandOutput),
    Missing,
}

pub(crate) enum RuntimeToolOutputLookup {
    Found(RuntimeToolOutput),
    Missing,
}

pub(crate) struct PersistedCommandOutputVersionPermit {
    entry: std::sync::Weak<Mutex<ThreadRuntimeEntry>>,
}

impl PersistedCommandOutputVersionPermit {
    pub(crate) fn version(&self, turn_id: TurnId, item_id: ItemId) -> Option<String> {
        let entry = self.entry.upgrade()?;
        lock_unpoison(&entry, "thread runtime entry")
            .persisted_command_output_versions
            .get(&(turn_id, item_id))
            .cloned()
    }

    pub(crate) fn cache(
        &self,
        turn_id: TurnId,
        item_id: ItemId,
        version: String,
    ) -> Option<String> {
        let entry = self.entry.upgrade()?;
        Some(
            lock_unpoison(&entry, "thread runtime entry")
                .persisted_command_output_versions
                .entry((turn_id, item_id))
                .or_insert(version)
                .clone(),
        )
    }
}

#[derive(Default)]
struct ActiveCapturedDiffs {
    // Current authority is a set of logical slots, not a path map: turn-level paths and each
    // occurrence of a path inside an item evolve independently. ItemCompleted replaces the
    // complete slot set for that item. A matched replacement keeps one conflict redirect; an
    // omitted slot becomes missing. `contents` contains exactly bodies still referenced by at
    // least one current slot, with identical content identities shared across slots.
    contents: HashMap<DiffId, CapturedDiffRecord>,
    current_by_slot: HashMap<CapturedDiffSlot, CapturedDiffDescriptor>,
    superseded: HashMap<DiffId, SupersededCapturedDiff>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CapturedDiffSlot {
    Item {
        item_id: ItemId,
        path: std::path::PathBuf,
        occurrence: usize,
    },
    Turn(std::path::PathBuf),
}

struct SupersededCapturedDiff {
    slot: CapturedDiffSlot,
    current: CapturedDiffDescriptor,
}

pub(crate) enum RuntimeDiffLookup {
    Found(CapturedDiffRecord),
    Superseded(CapturedDiffDescriptor),
    Missing,
}

#[derive(Default)]
struct OverviewState {
    revision: u64,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Hold the cross-thread runtime summary sent to browser connections.
    // Source of truth: Runtime entries remain authoritative; this is a derived projection.
    // Structural reason: One revisioned replacement snapshot necessarily spans many threads.
    // Synchronization: The enclosing overview mutex protects revision and summaries together.
    // Invalidation/removal: Runtime summary transitions replace or remove the matching projection.
    summaries: HashMap<ThreadId, ThreadRuntimeSummary>,
}

pub(crate) struct AppliedRuntimeEvent {
    pub sequence: Option<u64>,
    pub tasks_changed: bool,
    pub running_tasks_if_changed: Option<RunningTasksProjection>,
    pub request_state: Option<WireRequestState>,
    pub overview_if_changed: Option<ThreadRuntimeOverview>,
    overview_refresh_needed: bool,
}

pub(crate) struct RunningTasksProjection {
    pub revision: u64,
    pub tasks: Vec<RunningTask>,
}

#[derive(Debug)]
pub(crate) struct RequestTransition {
    pub request_state: WireRequestState,
    pub overview_if_changed: Option<ThreadRuntimeOverview>,
}

#[derive(Debug)]
pub(crate) struct RequestCommitError {
    pub error: HarnessError,
    pub rollback: Option<RequestTransition>,
}

#[derive(Clone)]
struct ActiveTurnOwner {
    reservation: TurnReservation,
    acknowledged_turn: Option<TurnId>,
    reserved_at: Instant,
    persistence_blocked: Option<(Turn, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RuntimeRequestId {
    Approval(ApprovalId),
    Server(ServerRequestId),
}

#[derive(Clone, PartialEq)]
enum RequestPayload {
    Approval(ApprovalRequest),
    Server(ServerRequest),
}

#[derive(Clone, Debug, PartialEq)]
enum RequestStatus {
    Pending,
    Responding { claim: u64, harness_resolved: bool },
    Resolved(RequestResolution),
}

#[derive(Clone)]
struct RequestRecord {
    turn_id: Option<TurnId>,
    payload: RequestPayload,
    status: RequestStatus,
    revision: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RequestResolution {
    Approval(ApprovalDecision),
    Server(ServerRequestResponse),
}

pub(crate) struct RequestClaim {
    authority: Arc<ThreadAuthority>,
    overview: Arc<Mutex<OverviewState>>,
    max_command_output_bytes: usize,
    request_id: RuntimeRequestId,
    thread_id: ThreadId,
    claim_id: u64,
    settled: bool,
}

#[derive(Clone)]
pub(crate) struct TurnReservation {
    pub project_id: ProjectId,
    pub harness_thread_id: String,
    pub mode: TurnMode,
    pub model: TurnModel,
    pub context_kind: &'static str,
}

pub(crate) struct ThreadTurnLease {
    authority: Arc<ThreadAuthority>,
    overview: Arc<Mutex<OverviewState>>,
    max_command_output_bytes: usize,
    detached: bool,
}

/// Opaque proof that no newer lifecycle has superseded a delayed thread restore.
pub(crate) struct RestorePermit {
    thread_id: ThreadId,
    authority: std::sync::Weak<ThreadAuthority>,
    entry: std::sync::Weak<Mutex<ThreadRuntimeEntry>>,
    lifecycle_revision: u64,
}

/// Opaque route-facing runtime access bound to one stable thread authority.
pub struct ResolvedThreadRuntime {
    support: Arc<ThreadRuntimeSupport>,
    authority: Arc<ThreadAuthority>,
}

impl ResolvedThreadRuntime {
    /// Binds support to an already resolved authority without creating runtime state.
    pub(crate) fn new(support: Arc<ThreadRuntimeSupport>, authority: Arc<ThreadAuthority>) -> Self {
        Self { support, authority }
    }

    /// Reports whether an admitted turn currently owns this thread.
    pub(crate) fn has_active_turn(&self) -> bool {
        self.support.has_active_turn(&self.authority)
    }

    /// Reports whether the reconnect buffer contains an active turn.
    pub fn live_is_active(&self) -> bool {
        self.support.live_is_active(&self.authority)
    }

    /// Returns reconnect state without creating a runtime entry.
    pub fn live_snapshot(&self) -> Option<LiveTurnSnapshot> {
        self.support.live_snapshot(&self.authority)
    }

    /// Reports whether the current runtime entry contains running tasks.
    pub(crate) fn has_running_tasks(&self) -> bool {
        self.support.has_running_for_thread(&self.authority)
    }

    /// Returns the current revisioned running-task projection.
    pub fn tasks_snapshot(&self) -> (u64, Vec<RunningTask>) {
        self.support.tasks_snapshot(&self.authority)
    }

    /// Returns the current request projections from the bound authority.
    pub(crate) fn request_states(&self) -> Vec<WireRequestState> {
        self.support.request_states(&self.authority)
    }

    /// Resolves captured diff content from the current runtime entry.
    pub(crate) fn captured_diff(&self, turn_id: TurnId, diff_id: &DiffId) -> RuntimeDiffLookup {
        self.support
            .captured_diff(&self.authority, turn_id, diff_id)
    }

    /// Resolves command output from the current runtime entry.
    pub(crate) fn command_output(
        &self,
        turn_id: TurnId,
        item_id: ItemId,
    ) -> RuntimeCommandOutputLookup {
        self.support
            .command_output(&self.authority, turn_id, item_id)
    }

    /// Resolves tool output from the current runtime entry.
    pub(crate) fn tool_output(&self, turn_id: TurnId, item_id: ItemId) -> RuntimeToolOutputLookup {
        self.support.tool_output(&self.authority, turn_id, item_id)
    }

    /// Captures an exact-entry permit for caching a persisted output version.
    pub(crate) fn persisted_command_output_version_permit(
        &self,
    ) -> PersistedCommandOutputVersionPermit {
        self.support
            .persisted_command_output_version_permit(&self.authority)
    }

    /// Finds a running task by its provider process identity.
    pub(crate) fn task_by_process(&self, process_id: &str) -> Option<RunningTask> {
        self.support.task_by_process(&self.authority, process_id)
    }

    /// Updates the terminating projection for a matching running task.
    pub(crate) fn set_task_terminating(&self, process_id: &str, terminating: bool) -> bool {
        self.support
            .set_task_terminating(&self.authority, process_id, terminating)
    }

    /// Removes a matching running task after the provider reports it unmanaged.
    pub(crate) fn remove_task_by_process(&self, process_id: &str) -> bool {
        self.support
            .remove_task_by_process(&self.authority, process_id)
    }

    /// Integration-test setup that bypasses normal turn orchestration to seed reconnect state.
    #[doc(hidden)]
    pub fn replace_live_turn_for_test(&self, turn_id: TurnId, user_input: Option<UserInput>) {
        self.support
            .replace_live_turn(&self.authority, turn_id, user_input);
    }

    /// Integration-test setup that bypasses normal event orchestration to seed runtime state.
    #[doc(hidden)]
    pub fn apply_event_for_test(&self, event: &AgentEvent, append_live: bool) {
        self.support
            .apply_event(&self.authority, event, append_live);
    }

    #[cfg(test)]
    pub(crate) fn reserve_turn_for_test(&self, reservation: TurnReservation) -> ThreadTurnLease {
        self.support
            .reserve_turn(&self.authority, reservation)
            .expect("test turn reservation must succeed")
    }

    #[cfg(test)]
    pub(crate) fn normalize_command_output_for_test(&self, event: AgentEvent) -> AgentEvent {
        self.support.normalize_command_output(event)
    }
}

impl ThreadRuntimeSupport {
    /// Normalize a completed-item event before runtime, wire, or persistence can observe it.
    pub(crate) fn normalize_command_output(&self, mut event: AgentEvent) -> AgentEvent {
        let AgentEvent::ItemCompleted { item, .. } = &mut event else {
            return event;
        };
        let ItemPayload::CommandExecution {
            output,
            output_truncated,
            output_original_bytes,
            output_original_lines,
            ..
        } = &mut item.payload
        else {
            return event;
        };
        let normalized = giskard_persist::normalize_command_output(
            std::mem::take(output),
            self.max_command_output_bytes,
        );
        *output = normalized.output;
        *output_truncated = normalized.output_truncated;
        *output_original_bytes = normalized.output_original_bytes;
        *output_original_lines = normalized.output_original_lines;
        event
    }

    /// Normalize and fully project command output before the event enters locked runtime state.
    pub(crate) fn prepare_item_output(
        &self,
        event: AgentEvent,
    ) -> (AgentEvent, Option<PreparedItemOutput>) {
        let event = self.normalize_command_output(event);
        let prepared = prepare_item_output(&event);
        (event, prepared)
    }

    fn prepare_existing_item_output(&self, event: &AgentEvent) -> Option<PreparedItemOutput> {
        prepare_item_output(event)
    }

    /// Extract full diff bodies before an event reaches reconnect state or browser projection.
    pub(crate) fn capture_event_diffs(
        &self,
        authority: &Arc<ThreadAuthority>,
        mut event: AgentEvent,
    ) -> AgentEvent {
        let thread_id = authority.thread_id();
        let entry = self.entry_or_create(authority);
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        match &mut event {
            AgentEvent::ItemCompleted { turn, item, .. } => {
                let state = entry.captured_diffs.entry(*turn).or_default();
                let mut captures = Vec::new();
                if let ItemPayload::FileChange { changes, .. } = &mut item.payload {
                    let mut occurrences = HashMap::new();
                    for change in changes {
                        let occurrence = occurrences.entry(change.path.clone()).or_insert(0);
                        let slot = CapturedDiffSlot::Item {
                            item_id: item.id,
                            path: change.path.clone(),
                            occurrence: *occurrence,
                        };
                        *occurrence += 1;
                        let Some(text) = change.diff.take() else {
                            continue;
                        };
                        let (descriptor, record) = giskard_core::capture_unified_diff(
                            change.path.clone(),
                            change.change,
                            Some(item.id),
                            text,
                        );
                        captures.push((slot, descriptor.clone(), record));
                        change.captured_diff = Some(descriptor);
                    }
                }
                // ItemCompleted is an upsert of the complete item payload. An empty file-change
                // set or a replacement payload of another kind therefore retires every old slot.
                reconcile_item_captured_diffs(state, thread_id, *turn, item.id, captures);
            }
            AgentEvent::DiffUpdated { turn, diff, .. } => {
                let (projected, record) = giskard_core::capture_structured_diff(diff.clone());
                if let Some(descriptor) = projected.captured.clone() {
                    let state = entry.captured_diffs.entry(*turn).or_default();
                    install_captured_diff(
                        state,
                        thread_id,
                        *turn,
                        CapturedDiffSlot::Turn(descriptor.path.clone()),
                        descriptor,
                        record,
                    );
                }
                *diff = projected;
            }
            _ => {}
        }
        event
    }

    pub(crate) fn captured_diff_records(
        &self,
        authority: &Arc<ThreadAuthority>,
        turn_id: TurnId,
    ) -> Vec<CapturedDiffRecord> {
        let Some(entry) = self.existing_entry(authority) else {
            return Vec::new();
        };
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry
            .captured_diffs
            .get(&turn_id)
            .map_or_else(Vec::new, |state| state.contents.values().cloned().collect())
    }

    pub(crate) fn command_output(
        &self,
        authority: &Arc<ThreadAuthority>,
        turn_id: TurnId,
        item_id: ItemId,
    ) -> RuntimeCommandOutputLookup {
        let Some(entry) = self.existing_entry(authority) else {
            return RuntimeCommandOutputLookup::Missing;
        };
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry
            .command_outputs
            .get(&(turn_id, item_id))
            .cloned()
            .map_or(
                RuntimeCommandOutputLookup::Missing,
                RuntimeCommandOutputLookup::Found,
            )
    }

    pub(crate) fn tool_output(
        &self,
        authority: &Arc<ThreadAuthority>,
        turn_id: TurnId,
        item_id: ItemId,
    ) -> RuntimeToolOutputLookup {
        let Some(entry) = self.existing_entry(authority) else {
            return RuntimeToolOutputLookup::Missing;
        };
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry.tool_outputs.get(&(turn_id, item_id)).cloned().map_or(
            RuntimeToolOutputLookup::Missing,
            RuntimeToolOutputLookup::Found,
        )
    }

    pub(crate) fn remove_tool_output(
        &self,
        authority: &Arc<ThreadAuthority>,
        turn_id: TurnId,
        item_id: ItemId,
    ) {
        let Some(entry) = self.existing_entry(authority) else {
            return;
        };
        lock_unpoison(&entry, "thread runtime entry")
            .tool_outputs
            .remove(&(turn_id, item_id));
    }

    pub(crate) fn remove_command_output(
        &self,
        authority: &Arc<ThreadAuthority>,
        turn_id: TurnId,
        item_id: ItemId,
    ) {
        let Some(entry) = self.existing_entry(authority) else {
            return;
        };
        lock_unpoison(&entry, "thread runtime entry")
            .command_outputs
            .remove(&(turn_id, item_id));
    }

    pub(crate) fn persisted_command_output_version_permit(
        &self,
        authority: &Arc<ThreadAuthority>,
    ) -> PersistedCommandOutputVersionPermit {
        let entry = self.entry_or_create(authority);
        PersistedCommandOutputVersionPermit {
            entry: Arc::downgrade(&entry),
        }
    }

    pub(crate) fn captured_diff(
        &self,
        authority: &Arc<ThreadAuthority>,
        turn_id: TurnId,
        diff_id: &DiffId,
    ) -> RuntimeDiffLookup {
        let Some(entry) = self.existing_entry(authority) else {
            return RuntimeDiffLookup::Missing;
        };
        let entry = lock_unpoison(&entry, "thread runtime entry");
        let Some(state) = entry.captured_diffs.get(&turn_id) else {
            return RuntimeDiffLookup::Missing;
        };
        if let Some(record) = state.contents.get(diff_id) {
            return RuntimeDiffLookup::Found(record.clone());
        }
        state
            .superseded
            .get(diff_id)
            .map(|superseded| superseded.current.clone())
            .map_or(RuntimeDiffLookup::Missing, RuntimeDiffLookup::Superseded)
    }
    pub(crate) fn new() -> Self {
        Self::with_max_command_output_bytes(
            giskard_persist::config::RetentionConfig::DEFAULT_MAX_COMMAND_OUTPUT_BYTES,
        )
    }

    pub(crate) fn with_max_command_output_bytes(max_command_output_bytes: usize) -> Self {
        Self {
            overview: Arc::new(Mutex::new(OverviewState::default())),
            max_command_output_bytes,
        }
    }

    pub(crate) fn restoration_permit(&self, authority: &Arc<ThreadAuthority>) -> RestorePermit {
        let entry = self.entry_or_create(authority);
        self.permit_for_entry(authority, entry)
    }

    pub(crate) fn event_application_permit(
        &self,
        authority: &Arc<ThreadAuthority>,
    ) -> Option<RestorePermit> {
        let entry = self.existing_entry(authority)?;
        Some(self.permit_for_entry(authority, entry))
    }

    fn permit_for_entry(
        &self,
        authority: &Arc<ThreadAuthority>,
        entry: Arc<Mutex<ThreadRuntimeEntry>>,
    ) -> RestorePermit {
        let revision = lock_unpoison(&entry, "thread runtime entry").lifecycle_revision;
        RestorePermit {
            thread_id: authority.thread_id(),
            authority: Arc::downgrade(authority),
            entry: Arc::downgrade(&entry),
            lifecycle_revision: revision,
        }
    }

    pub(crate) fn restoration_is_current(&self, permit: &RestorePermit) -> bool {
        let Some(expected) = permit.entry.upgrade() else {
            return false;
        };
        let Some(authority) = permit.authority.upgrade() else {
            return false;
        };
        authority
            .with_exact_runtime_entry(&expected, |entry| entry.lifecycle_revision)
            .is_some_and(|revision| revision == permit.lifecycle_revision)
    }

    pub(crate) fn live_is_active(&self, authority: &Arc<ThreadAuthority>) -> bool {
        let thread_id = authority.thread_id();
        let Some(entry) = self.existing_entry(authority) else {
            return false;
        };
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry.live.is_active(thread_id)
    }

    pub(crate) fn live_snapshot(
        &self,
        authority: &Arc<ThreadAuthority>,
    ) -> Option<LiveTurnSnapshot> {
        let thread_id = authority.thread_id();
        let entry = self.existing_entry(authority)?;
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry.live.snapshot(thread_id)
    }

    pub(crate) fn live_item_events(
        &self,
        authority: &Arc<ThreadAuthority>,
        item_id: ItemId,
    ) -> Vec<AgentEvent> {
        let thread_id = authority.thread_id();
        let Some(entry) = self.existing_entry(authority) else {
            return Vec::new();
        };
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry.live.item_events(thread_id, item_id)
    }

    pub(crate) fn ensure_live_turn(
        &self,
        authority: &Arc<ThreadAuthority>,
        turn_id: TurnId,
        user_input: Option<UserInput>,
    ) -> Result<(), TurnId> {
        let thread_id = authority.thread_id();
        let entry = self.entry_or_create(authority);
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        entry
            .live
            .ensure_turn_with_user_input(thread_id, turn_id, user_input)
    }

    pub(crate) fn replace_live_turn(
        &self,
        authority: &Arc<ThreadAuthority>,
        turn_id: TurnId,
        user_input: Option<UserInput>,
    ) {
        let thread_id = authority.thread_id();
        let entry = self.entry_or_create(authority);
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        entry
            .live
            .replace_turn_with_user_input(thread_id, turn_id, user_input);
    }

    pub(crate) fn resolve_live_approval(
        &self,
        authority: &Arc<ThreadAuthority>,
        approval_id: ApprovalId,
        decision: ApprovalDecision,
    ) {
        let thread_id = authority.thread_id();
        let Some(entry) = self.existing_entry(authority) else {
            return;
        };
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        entry
            .live
            .resolve_approval(thread_id, approval_id, decision);
    }

    pub(crate) fn resolve_live_server_request(
        &self,
        authority: &Arc<ThreadAuthority>,
        request_id: ServerRequestId,
    ) {
        let thread_id = authority.thread_id();
        let Some(entry) = self.existing_entry(authority) else {
            return;
        };
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        entry.live.resolve_server_request(thread_id, request_id);
    }

    pub(crate) fn tasks_snapshot(
        &self,
        authority: &Arc<ThreadAuthority>,
    ) -> (u64, Vec<RunningTask>) {
        let thread_id = authority.thread_id();
        let Some(entry) = self.existing_entry(authority) else {
            return (0, Vec::new());
        };
        let entry = lock_unpoison(&entry, "thread runtime entry");
        let tasks = entry.tasks.snapshot(thread_id);
        (entry.task_revision, tasks)
    }

    pub(crate) fn has_running_for_turn(
        &self,
        authority: &Arc<ThreadAuthority>,
        turn_id: TurnId,
    ) -> bool {
        let thread_id = authority.thread_id();
        let Some(entry) = self.existing_entry(authority) else {
            return false;
        };
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry.tasks.has_running_for_turn(thread_id, turn_id)
    }

    pub(crate) fn has_running_for_thread(&self, authority: &Arc<ThreadAuthority>) -> bool {
        let thread_id = authority.thread_id();
        let Some(entry) = self.existing_entry(authority) else {
            return false;
        };
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry.tasks.has_running_for_thread(thread_id)
    }

    pub(crate) fn task_by_process(
        &self,
        authority: &Arc<ThreadAuthority>,
        process_id: &str,
    ) -> Option<RunningTask> {
        let thread_id = authority.thread_id();
        let entry = self.existing_entry(authority)?;
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry.tasks.get_by_process(thread_id, process_id)
    }

    pub(crate) fn task_by_item(
        &self,
        authority: &Arc<ThreadAuthority>,
        turn_id: TurnId,
        item_id: ItemId,
    ) -> Option<RunningTask> {
        let thread_id = authority.thread_id();
        let entry = self.existing_entry(authority)?;
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry.tasks.get_by_item(thread_id, turn_id, item_id)
    }

    pub(crate) fn set_task_terminating(
        &self,
        authority: &Arc<ThreadAuthority>,
        process_id: &str,
        terminating: bool,
    ) -> bool {
        let thread_id = authority.thread_id();
        let Some(entry) = self.existing_entry(authority) else {
            return false;
        };
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        let changed = entry
            .tasks
            .set_terminating_by_process(thread_id, process_id, terminating);
        if changed {
            entry.task_revision = entry.task_revision.saturating_add(1);
        }
        changed
    }

    pub(crate) fn remove_task_by_process(
        &self,
        authority: &Arc<ThreadAuthority>,
        process_id: &str,
    ) -> bool {
        let thread_id = authority.thread_id();
        let Some(entry) = self.existing_entry(authority) else {
            return false;
        };
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        let changed = entry.tasks.remove_by_process(thread_id, process_id);
        if changed {
            entry.task_revision = entry.task_revision.saturating_add(1);
        }
        changed
    }

    pub(crate) fn apply_event(
        &self,
        authority: &Arc<ThreadAuthority>,
        event: &AgentEvent,
        append_live: bool,
    ) -> AppliedRuntimeEvent {
        let thread_id = authority.thread_id();
        let event_thread_id = event.thread_id();
        if event_thread_id != thread_id {
            warn!(
                %thread_id,
                %event_thread_id,
                "refusing to apply a foreign-thread event to runtime state"
            );
            return AppliedRuntimeEvent::unchanged();
        }
        let prepared_output = self.prepare_existing_item_output(event);
        self.apply_prepared_event(authority, event, append_live, prepared_output)
    }

    pub(crate) fn apply_prepared_event(
        &self,
        authority: &Arc<ThreadAuthority>,
        event: &AgentEvent,
        append_live: bool,
        prepared_output: Option<PreparedItemOutput>,
    ) -> AppliedRuntimeEvent {
        let thread_id = authority.thread_id();
        if event.thread_id() != thread_id {
            return self.apply_event(authority, event, append_live);
        }
        let entry = self.entry_or_create(authority);
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        self.apply_prepared_event_to_entry(
            thread_id,
            event,
            append_live,
            prepared_output,
            &mut entry,
        )
    }

    pub(crate) fn apply_prepared_event_if_current(
        &self,
        permit: &RestorePermit,
        event: &AgentEvent,
        append_live: bool,
        prepared_output: Option<PreparedItemOutput>,
    ) -> Option<AppliedRuntimeEvent> {
        if event.thread_id() != permit.thread_id {
            return None;
        }
        let expected = permit.entry.upgrade()?;
        let authority = permit.authority.upgrade()?;
        // Keep the runtime slot locked until application completes. Retirement therefore either wins
        // before this check, or removes the fully-applied entry afterward; it cannot be followed by
        // stale prepared work recreating authority.
        authority.with_exact_runtime_entry(&expected, |entry| {
            if entry.lifecycle_revision != permit.lifecycle_revision {
                return None;
            }
            Some(self.apply_prepared_event_to_entry(
                permit.thread_id,
                event,
                append_live,
                prepared_output,
                entry,
            ))
        })?
    }

    fn apply_prepared_event_to_entry(
        &self,
        thread_id: ThreadId,
        event: &AgentEvent,
        append_live: bool,
        mut prepared_output: Option<PreparedItemOutput>,
        entry: &mut ThreadRuntimeEntry,
    ) -> AppliedRuntimeEvent {
        let live_command_descriptor = prepared_output
            .as_ref()
            .and_then(|prepared| prepared.command_descriptor.clone());
        let live_tool_descriptor = prepared_output
            .as_ref()
            .and_then(|prepared| prepared.tool_descriptor.clone());
        let live_event = prepared_output
            .as_mut()
            .and_then(|prepared| prepared.live_event.take());
        let mut applied = self.apply_event_locked(thread_id, event, false, prepared_output, entry);
        if append_live && entry.live.is_active(thread_id) {
            entry.live.append_with_outputs(
                thread_id,
                live_event.unwrap_or_else(|| event.clone()),
                live_command_descriptor,
                live_tool_descriptor,
            );
        }
        if applied.overview_refresh_needed {
            applied.overview_if_changed = self.refresh_overview(thread_id, entry);
        }
        applied
    }

    fn apply_event_locked(
        &self,
        thread_id: ThreadId,
        event: &AgentEvent,
        append_live: bool,
        prepared_output: Option<PreparedItemOutput>,
        entry: &mut ThreadRuntimeEntry,
    ) -> AppliedRuntimeEvent {
        let sequence = (!matches!(
            event,
            AgentEvent::ThreadOpened { .. } | AgentEvent::DiffUpdated { .. }
        ))
        .then(|| {
            entry.event_sequence = entry.event_sequence.saturating_add(1);
            entry.event_sequence
        });
        let (request_id, request_changed) = match event {
            AgentEvent::ApprovalRequested { turn, request, .. } => {
                let changed = register_request(
                    entry,
                    RuntimeRequestId::Approval(request.id.clone()),
                    Some(*turn),
                    RequestPayload::Approval(request.clone()),
                );
                (
                    Some(RuntimeRequestId::Approval(request.id.clone())),
                    changed,
                )
            }
            AgentEvent::ServerRequestReceived { turn, request, .. } => {
                let changed = register_request(
                    entry,
                    RuntimeRequestId::Server(request.id.clone()),
                    *turn,
                    RequestPayload::Server(request.clone()),
                );
                (Some(RuntimeRequestId::Server(request.id.clone())), changed)
            }
            AgentEvent::ServerRequestResolved { request_id, .. } => {
                let changed = resolve_server_request_from_harness(entry, thread_id, request_id);
                (Some(RuntimeRequestId::Server(request_id.clone())), changed)
            }
            _ => (None, false),
        };
        if let AgentEvent::ItemCompleted { turn, item, .. } = event {
            if let Some(prepared) = prepared_output {
                update_prepared_item_output_authority(entry, prepared);
            } else {
                update_command_output_authority(entry, *turn, item);
                update_tool_output_authority(entry, thread_id, *turn, item);
            }
        }
        let tasks_changed = entry.tasks.apply_event(event);
        if tasks_changed {
            entry.task_revision = entry.task_revision.saturating_add(1);
        }
        if append_live && entry.live.is_active(thread_id) {
            entry.live.append(thread_id, event.clone());
        }
        // An event that left the record untouched has no new state to publish: re-sending the
        // record under its existing revision is indistinguishable from a real update to a client
        // that gates on revision.
        let request_state = if request_changed {
            request_id
                .as_ref()
                .and_then(|id| entry.requests.get(id))
                .map(|record| wire_request_state(thread_id, record))
        } else {
            None
        };
        AppliedRuntimeEvent {
            sequence,
            tasks_changed,
            running_tasks_if_changed: tasks_changed.then(|| RunningTasksProjection {
                revision: entry.task_revision,
                tasks: entry.tasks.snapshot(thread_id),
            }),
            request_state,
            overview_if_changed: None,
            overview_refresh_needed: request_changed,
        }
    }

    pub(crate) fn settle_completed_turn(
        &self,
        authority: &Arc<ThreadAuthority>,
        event: &AgentEvent,
        persisted_turn: Option<(Turn, String)>,
    ) -> AppliedRuntimeEvent {
        let thread_id = authority.thread_id();
        let entry = self.entry_or_create(authority);
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        let mut applied = self.apply_event_locked(thread_id, event, true, None, &mut entry);
        match persisted_turn {
            None => {
                entry.live.clear_turn(thread_id);
                let completed_turn = match event {
                    AgentEvent::TurnCompleted { turn, .. } => Some(*turn),
                    _ => None,
                };
                if let Some(completed_turn) = completed_turn {
                    entry.captured_diffs.remove(&completed_turn);
                    entry
                        .command_outputs
                        .retain(|(turn_id, _), _| *turn_id != completed_turn);
                    entry
                        .tool_outputs
                        .retain(|(turn_id, _), _| *turn_id != completed_turn);
                }
                entry.requests.retain(|_, record| {
                    !(matches!(record.status, RequestStatus::Resolved(_))
                        && record.turn_id == completed_turn)
                });
                if let Some(owner) = entry.active_turn.take() {
                    debug!(
                        %thread_id,
                        project_id = %owner.reservation.project_id,
                        turn_id = display_opt(owner.acknowledged_turn),
                        elapsed_ms = owner.reserved_at.elapsed().as_millis(),
                        "committed persisted turn and released thread runtime"
                    );
                }
            }
            Some((turn, error)) => {
                if let Some(owner) = entry.active_turn.as_mut() {
                    owner.acknowledged_turn = Some(turn.id);
                    owner.persistence_blocked = Some((turn, error));
                } else {
                    warn!(%thread_id, "cannot retain failed turn without an active owner");
                }
            }
        }
        applied.overview_if_changed = self.refresh_overview(thread_id, &entry);
        applied
    }

    pub(crate) fn reserve_turn(
        &self,
        authority: &Arc<ThreadAuthority>,
        reservation: TurnReservation,
    ) -> Result<ThreadTurnLease, HarnessError> {
        let thread_id = authority.thread_id();
        let entry = self.entry_or_create(authority);
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        if let Some(existing) = &entry.active_turn {
            warn!(
                %thread_id,
                owner_project_id = %existing.reservation.project_id,
                owner_turn_id = display_opt(existing.acknowledged_turn),
                owner_harness_thread_id = %existing.reservation.harness_thread_id,
                owner_context_kind = existing.reservation.context_kind,
                owner_mode = ?existing.reservation.mode,
                owner_model = ?existing.reservation.model,
                owner_elapsed_ms = existing.reserved_at.elapsed().as_millis(),
                "rejecting turn start because thread runtime is already active"
            );
            return Err(HarnessError::ThreadBusy { thread: thread_id });
        }
        entry.active_turn = Some(ActiveTurnOwner {
            reservation,
            acknowledged_turn: None,
            reserved_at: Instant::now(),
            persistence_blocked: None,
        });
        entry.lifecycle_revision = entry.lifecycle_revision.saturating_add(1);
        self.refresh_overview(thread_id, &entry);
        Ok(ThreadTurnLease {
            authority: authority.clone(),
            overview: self.overview.clone(),
            max_command_output_bytes: self.max_command_output_bytes,
            detached: false,
        })
    }

    pub(crate) fn has_active_turn(&self, authority: &Arc<ThreadAuthority>) -> bool {
        let Some(entry) = self.existing_entry(authority) else {
            return false;
        };
        lock_unpoison(&entry, "thread runtime entry")
            .active_turn
            .is_some()
    }

    fn acknowledge_turn(
        &self,
        authority: &Arc<ThreadAuthority>,
        turn_id: TurnId,
    ) -> Option<ThreadRuntimeOverview> {
        let thread_id = authority.thread_id();
        let entry = self.entry_or_create(authority);
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        let Some(owner) = entry.active_turn.as_mut() else {
            warn!(%thread_id, %turn_id, "turn acknowledgement has no runtime owner");
            return None;
        };
        owner.acknowledged_turn = Some(turn_id);
        self.refresh_overview(thread_id, &entry)
    }

    fn release_turn(&self, authority: &Arc<ThreadAuthority>) -> Option<ThreadRuntimeOverview> {
        let thread_id = authority.thread_id();
        let entry = self.existing_entry(authority)?;
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        if let Some(owner) = entry.active_turn.take() {
            debug!(
                %thread_id,
                project_id = %owner.reservation.project_id,
                turn_id = display_opt(owner.acknowledged_turn),
                elapsed_ms = owner.reserved_at.elapsed().as_millis(),
                "released active thread runtime"
            );
        }
        self.refresh_overview(thread_id, &entry)
    }

    #[cfg(test)]
    pub(crate) fn register_approval(
        &self,
        authority: &Arc<ThreadAuthority>,
        request: ApprovalRequest,
    ) {
        let thread_id = authority.thread_id();
        let entry = self.entry_or_create(authority);
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        register_request(
            &mut entry,
            RuntimeRequestId::Approval(request.id.clone()),
            None,
            RequestPayload::Approval(request),
        );
        self.refresh_overview(thread_id, &entry);
    }

    pub(crate) fn claim_request(
        &self,
        authority: &Arc<ThreadAuthority>,
        request_id: RuntimeRequestId,
    ) -> Result<(RequestClaim, RequestTransition), HarnessError> {
        let thread_id = authority.thread_id();
        let entry = self.entry_or_create(authority);
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        let record = entry.requests.get_mut(&request_id).ok_or_else(|| {
            HarnessError::Protocol(format!("no pending request for id {}", request_id.as_str()))
        })?;
        if record.status != RequestStatus::Pending {
            return Err(HarnessError::Protocol(format!(
                "request {} is not pending",
                request_id.as_str()
            )));
        }
        let claim_id = next_claim_id();
        record.status = RequestStatus::Responding {
            claim: claim_id,
            harness_resolved: false,
        };
        record.revision = record.revision.saturating_add(1);
        let transition = RequestTransition {
            request_state: wire_request_state(thread_id, record),
            overview_if_changed: self.refresh_overview(thread_id, &entry),
        };
        Ok((
            RequestClaim {
                authority: authority.clone(),
                overview: self.overview.clone(),
                max_command_output_bytes: self.max_command_output_bytes,
                request_id,
                thread_id,
                claim_id,
                settled: false,
            },
            transition,
        ))
    }

    pub(crate) fn forget_threads(&self, authorities: &[Arc<ThreadAuthority>]) {
        let thread_ids = authorities
            .iter()
            .map(|authority| authority.thread_id())
            .collect::<std::collections::HashSet<_>>();
        for authority in authorities {
            authority.take_runtime_entry();
        }
        let mut overview = lock_unpoison(&self.overview, "runtime overview");
        let before = overview.summaries.len();
        overview.summaries.retain(|id, _| !thread_ids.contains(id));
        if overview.summaries.len() != before {
            overview.revision = overview.revision.saturating_add(1);
        }
    }

    pub(crate) fn request_state(
        &self,
        authority: &Arc<ThreadAuthority>,
        request_id: &RuntimeRequestId,
    ) -> Option<WireRequestState> {
        let thread_id = authority.thread_id();
        let entry = self.existing_entry(authority)?;
        lock_unpoison(&entry, "thread runtime entry")
            .requests
            .get(request_id)
            .map(|record| wire_request_state(thread_id, record))
    }

    pub(crate) fn request_states(&self, authority: &Arc<ThreadAuthority>) -> Vec<WireRequestState> {
        let thread_id = authority.thread_id();
        let Some(entry) = self.existing_entry(authority) else {
            return Vec::new();
        };
        lock_unpoison(&entry, "thread runtime entry")
            .requests
            .values()
            .map(|record| wire_request_state(thread_id, record))
            .collect()
    }

    #[cfg(test)]
    fn resolution_for_test(
        &self,
        authority: &Arc<ThreadAuthority>,
        request_id: &RuntimeRequestId,
    ) -> Option<RequestResolution> {
        let entry = self.existing_entry(authority)?;
        let entry = lock_unpoison(&entry, "thread runtime entry");
        match &entry.requests.get(request_id)?.status {
            RequestStatus::Resolved(resolution) => Some(resolution.clone()),
            RequestStatus::Pending | RequestStatus::Responding { .. } => None,
        }
    }

    pub(crate) fn current_overview(&self) -> ThreadRuntimeOverview {
        let overview = lock_unpoison(&self.overview, "runtime overview");
        let mut threads = overview.summaries.values().cloned().collect::<Vec<_>>();
        threads.sort_by_key(|entry| entry.thread_id.to_string());
        ThreadRuntimeOverview {
            revision: overview.revision,
            threads,
        }
    }

    fn refresh_overview(
        &self,
        thread_id: ThreadId,
        entry: &ThreadRuntimeEntry,
    ) -> Option<ThreadRuntimeOverview> {
        let summary = runtime_summary(thread_id, entry);
        let mut overview = lock_unpoison(&self.overview, "runtime overview");
        let changed = match summary {
            Some(summary) if overview.summaries.get(&thread_id) != Some(&summary) => {
                overview.summaries.insert(thread_id, summary);
                true
            }
            None => overview.summaries.remove(&thread_id).is_some(),
            _ => false,
        };
        if !changed {
            return None;
        }
        overview.revision = overview.revision.saturating_add(1);
        let mut threads = overview.summaries.values().cloned().collect::<Vec<_>>();
        threads.sort_by_key(|summary| summary.thread_id.to_string());
        Some(ThreadRuntimeOverview {
            revision: overview.revision,
            threads,
        })
    }

    fn entry_or_create(&self, authority: &Arc<ThreadAuthority>) -> Arc<Mutex<ThreadRuntimeEntry>> {
        authority.runtime_entry_or_create()
    }

    fn existing_entry(
        &self,
        authority: &Arc<ThreadAuthority>,
    ) -> Option<Arc<Mutex<ThreadRuntimeEntry>>> {
        authority.runtime_entry()
    }
}

fn install_captured_diff(
    state: &mut ActiveCapturedDiffs,
    thread_id: ThreadId,
    turn_id: TurnId,
    slot: CapturedDiffSlot,
    descriptor: CapturedDiffDescriptor,
    record: CapturedDiffRecord,
) {
    if let Some(previous) = state
        .current_by_slot
        .insert(slot.clone(), descriptor.clone())
        && previous.id != descriptor.id
    {
        if !state
            .current_by_slot
            .values()
            .any(|current| current.id == previous.id)
        {
            state.contents.remove(&previous.id);
            debug!(
                %thread_id,
                %turn_id,
                ?slot,
                superseded_diff_id = %previous.id,
                current_diff_id = %descriptor.id,
                "dropped superseded captured diff body"
            );
        }
        // Keep only the immediately superseded identity for each logical diff slot. Item-owned
        // and turn-level diffs for the same path are independent authorities.
        state
            .superseded
            .retain(|_, superseded| superseded.slot != slot);
        state.superseded.insert(
            previous.id,
            SupersededCapturedDiff {
                slot,
                current: descriptor.clone(),
            },
        );
    }
    state.contents.insert(record.id.clone(), record);
}

fn reconcile_item_captured_diffs(
    state: &mut ActiveCapturedDiffs,
    thread_id: ThreadId,
    turn_id: TurnId,
    item_id: ItemId,
    captures: Vec<(CapturedDiffSlot, CapturedDiffDescriptor, CapturedDiffRecord)>,
) {
    let new_slots: std::collections::HashSet<_> =
        captures.iter().map(|(slot, _, _)| slot.clone()).collect();
    let omitted: Vec<_> = state
        .current_by_slot
        .keys()
        .filter(|slot| {
            matches!(slot, CapturedDiffSlot::Item { item_id: owner, .. } if *owner == item_id)
                && !new_slots.contains(*slot)
        })
        .cloned()
        .collect();
    for slot in omitted {
        if let Some(previous) = state.current_by_slot.remove(&slot)
            && !state
                .current_by_slot
                .values()
                .any(|current| current.id == previous.id)
        {
            state.contents.remove(&previous.id);
            debug!(
                %thread_id,
                %turn_id,
                ?slot,
                removed_diff_id = %previous.id,
                "dropped captured diff body omitted by replacement item"
            );
        }
        state
            .superseded
            .retain(|_, superseded| superseded.slot != slot);
    }
    for (slot, descriptor, record) in captures {
        install_captured_diff(state, thread_id, turn_id, slot, descriptor, record);
    }
}

impl AppliedRuntimeEvent {
    fn unchanged() -> Self {
        Self {
            sequence: None,
            tasks_changed: false,
            running_tasks_if_changed: None,
            request_state: None,
            overview_if_changed: None,
            overview_refresh_needed: false,
        }
    }
}

impl Clone for ThreadRuntimeSupport {
    fn clone(&self) -> Self {
        Self {
            overview: self.overview.clone(),
            max_command_output_bytes: self.max_command_output_bytes,
        }
    }
}

fn update_command_output_authority(entry: &mut ThreadRuntimeEntry, turn_id: TurnId, item: &Item) {
    let ItemPayload::CommandExecution {
        output,
        output_truncated,
        output_original_bytes,
        output_original_lines,
        status,
        ..
    } = &item.payload
    else {
        entry.command_outputs.remove(&(turn_id, item.id));
        return;
    };
    if status.as_deref().is_some_and(command_status_is_running) {
        entry.command_outputs.remove(&(turn_id, item.id));
        return;
    }
    let Ok(descriptor) = giskard_persist::command_output_descriptor(
        output,
        *output_truncated,
        *output_original_bytes,
        *output_original_lines,
        true,
    ) else {
        tracing::error!(
            %turn_id,
            item_id = %item.id,
            "completed command output has inconsistent truncation metadata"
        );
        entry.command_outputs.remove(&(turn_id, item.id));
        return;
    };
    entry.command_outputs.insert(
        (turn_id, item.id),
        RuntimeCommandOutput {
            output: output.clone(),
            output_truncated: *output_truncated,
            original_bytes: descriptor.original_bytes,
            original_lines: descriptor.original_lines,
            version: command_output_version(output),
        },
    );
}

fn prepare_item_output(event: &AgentEvent) -> Option<PreparedItemOutput> {
    let AgentEvent::ItemCompleted { turn, item, .. } = event else {
        return None;
    };
    if let ItemPayload::ToolCall { output, status, .. } = &item.payload {
        let terminal = !status
            .as_deref()
            .is_some_and(giskard_core::item::tool_status_is_running);
        let prepared = output
            .as_ref()
            .filter(|_| terminal)
            .and_then(|output| giskard_core::item::serialize_tool_output(output).ok());
        let (tool_runtime, tool_descriptor) =
            prepared.map_or((None, None), |(bytes, descriptor)| {
                (
                    Some(RuntimeToolOutput {
                        bytes,
                        descriptor: descriptor.clone(),
                    }),
                    Some(descriptor),
                )
            });
        let mut live_event = event.clone();
        if let AgentEvent::ItemCompleted { item, .. } = &mut live_event
            && let ItemPayload::ToolCall { output, .. } = &mut item.payload
        {
            *output = None;
        }
        return Some(PreparedItemOutput {
            turn_id: *turn,
            item_id: item.id,
            command_runtime: None,
            command_descriptor: None,
            tool_runtime,
            tool_descriptor,
            command_item: false,
            live_event: Some(live_event),
        });
    }
    let ItemPayload::CommandExecution {
        output,
        output_truncated,
        output_original_bytes,
        output_original_lines,
        status,
        ..
    } = &item.payload
    else {
        return None;
    };
    let descriptor = giskard_persist::command_output_descriptor(
        output,
        *output_truncated,
        *output_original_bytes,
        *output_original_lines,
        true,
    );
    let (runtime, descriptor) = match descriptor {
        Ok(descriptor) => {
            let runtime = (!status.as_deref().is_some_and(command_status_is_running)).then(|| {
                RuntimeCommandOutput {
                    output: output.clone(),
                    output_truncated: *output_truncated,
                    original_bytes: descriptor.original_bytes,
                    original_lines: descriptor.original_lines,
                    version: command_output_version(output),
                }
            });
            (runtime, Some(descriptor))
        }
        Err(_) => (None, None),
    };
    let mut live_event = event.clone();
    if let (Some(descriptor), AgentEvent::ItemCompleted { item, .. }) =
        (&descriptor, &mut live_event)
        && let ItemPayload::CommandExecution { output, .. } = &mut item.payload
    {
        *output = descriptor.preview.clone();
    }
    Some(PreparedItemOutput {
        turn_id: *turn,
        item_id: item.id,
        command_runtime: runtime,
        command_descriptor: descriptor,
        tool_runtime: None,
        tool_descriptor: None,
        command_item: true,
        live_event: Some(live_event),
    })
}

fn update_prepared_item_output_authority(
    entry: &mut ThreadRuntimeEntry,
    prepared: PreparedItemOutput,
) {
    let key = (prepared.turn_id, prepared.item_id);
    match prepared.command_runtime {
        Some(runtime) => {
            entry.command_outputs.insert(key, runtime);
        }
        None => {
            if prepared.command_item && prepared.command_descriptor.is_none() {
                tracing::error!(
                    turn_id = %prepared.turn_id,
                    item_id = %prepared.item_id,
                    "completed command output has inconsistent truncation metadata"
                );
            }
            entry.command_outputs.remove(&key);
        }
    }
    match prepared.tool_runtime {
        Some(runtime) => {
            entry.tool_outputs.insert(key, runtime);
        }
        None => {
            entry.tool_outputs.remove(&key);
        }
    }
}

fn update_tool_output_authority(
    entry: &mut ThreadRuntimeEntry,
    thread_id: ThreadId,
    turn_id: TurnId,
    item: &Item,
) {
    let key = (turn_id, item.id);
    let ItemPayload::ToolCall { output, status, .. } = &item.payload else {
        entry.tool_outputs.remove(&key);
        return;
    };
    if status
        .as_deref()
        .is_some_and(giskard_core::item::tool_status_is_running)
    {
        entry.tool_outputs.remove(&key);
        return;
    }
    let Some(output) = output else {
        entry.tool_outputs.remove(&key);
        return;
    };
    match giskard_core::item::serialize_tool_output(output) {
        Ok((bytes, descriptor)) => {
            entry
                .tool_outputs
                .insert(key, RuntimeToolOutput { bytes, descriptor });
        }
        Err(error) => {
            let project_id = entry
                .active_turn
                .as_ref()
                .map(|owner| tracing::field::display(owner.reservation.project_id));
            tracing::error!(
                project_id,
                %thread_id,
                %turn_id,
                item_id = %item.id,
                action = "serialize_completed_tool_output",
                %error,
                "could not serialize completed tool output"
            );
            entry.tool_outputs.remove(&key);
        }
    }
}

pub(crate) fn command_output_version(output: &str) -> String {
    format!("\"sha256_{:x}\"", Sha256::digest(output.as_bytes()))
}

impl ThreadTurnLease {
    fn support(&self) -> ThreadRuntimeSupport {
        ThreadRuntimeSupport {
            overview: self.overview.clone(),
            max_command_output_bytes: self.max_command_output_bytes,
        }
    }

    /// Adopt the harness's turn id. The overview it returns is a changed replacement projection:
    /// the caller must publish it, or connected clients keep an overview this transition
    /// superseded.
    #[must_use = "an acknowledged turn changes the runtime overview; publish it"]
    pub(crate) fn acknowledge_turn(&mut self, turn_id: TurnId) -> Option<ThreadRuntimeOverview> {
        if self.detached {
            return None;
        }
        self.support().acknowledge_turn(&self.authority, turn_id)
    }

    pub(crate) fn release(&mut self) -> Option<ThreadRuntimeOverview> {
        if self.detached {
            return None;
        }
        let overview = self.support().release_turn(&self.authority);
        self.detached = true;
        overview
    }

    pub(crate) fn is_released(&self) -> bool {
        self.detached
    }

    pub(crate) fn commit_after_persistence(&mut self, event: &AgentEvent) -> AppliedRuntimeEvent {
        let applied = self
            .support()
            .settle_completed_turn(&self.authority, event, None);
        self.detached = true;
        applied
    }

    pub(crate) fn retain_after_persistence_failure(
        &mut self,
        event: &AgentEvent,
        turn: Turn,
        error: String,
    ) -> AppliedRuntimeEvent {
        let applied =
            self.support()
                .settle_completed_turn(&self.authority, event, Some((turn, error)));
        self.detached = true;
        applied
    }
}

impl Drop for ThreadTurnLease {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

impl RuntimeRequestId {
    fn as_str(&self) -> &str {
        match self {
            Self::Approval(id) => &id.0,
            Self::Server(id) => &id.0,
        }
    }
}

fn lock_unpoison<'a, T>(mutex: &'a Mutex<T>, state_kind: &'static str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!(
                state_kind,
                "thread runtime lock was poisoned; recovering state"
            );
            poisoned.into_inner()
        }
    }
}

fn register_request(
    entry: &mut ThreadRuntimeEntry,
    request_id: RuntimeRequestId,
    turn_id: Option<TurnId>,
    payload: RequestPayload,
) -> bool {
    use std::collections::hash_map::Entry;

    match entry.requests.entry(request_id) {
        Entry::Vacant(record) => {
            record.insert(RequestRecord {
                turn_id,
                payload,
                status: RequestStatus::Pending,
                revision: 1,
            });
            true
        }
        Entry::Occupied(mut record) => {
            // Duplicate provider delivery may refresh bounded request metadata, but it must not
            // resurrect a responding or resolved request. An identical redelivery is not a new
            // revision, and a changed one must take a new revision rather than republish different
            // content under the revision a client already accepted.
            let record = record.get_mut();
            if record.payload == payload {
                return false;
            }
            record.payload = payload;
            record.revision = record.revision.saturating_add(1);
            true
        }
    }
}

fn resolve_server_request_from_harness(
    entry: &mut ThreadRuntimeEntry,
    thread_id: ThreadId,
    request_id: &ServerRequestId,
) -> bool {
    let Some(record) = entry
        .requests
        .get_mut(&RuntimeRequestId::Server(request_id.clone()))
    else {
        warn!(
            %thread_id,
            request_id = %request_id.0,
            "harness resolved a server request with no runtime record"
        );
        return false;
    };
    match &mut record.status {
        RequestStatus::Responding {
            harness_resolved, ..
        } => {
            if !*harness_resolved {
                debug!(
                    %thread_id,
                    request_id = %request_id.0,
                    "harness resolved a server request while a claim is in flight; deferring to the claimant"
                );
            }
            *harness_resolved = true;
            return false;
        }
        RequestStatus::Resolved(_) => return false,
        RequestStatus::Pending => {}
    }
    debug!(
        %thread_id,
        request_id = %request_id.0,
        "synthesizing runtime resolution from a harness-resolved server request"
    );
    record.status = RequestStatus::Resolved(RequestResolution::Server(
        ServerRequestResponse::result(serde_json::Value::Null),
    ));
    record.revision = record.revision.saturating_add(1);
    true
}

fn runtime_summary(
    thread_id: ThreadId,
    entry: &ThreadRuntimeEntry,
) -> Option<ThreadRuntimeSummary> {
    let turn_state = entry
        .active_turn
        .as_ref()
        .map_or(RuntimeTurnState::Idle, |owner| {
            if let Some((turn, error)) = &owner.persistence_blocked {
                RuntimeTurnState::PersistenceBlocked {
                    turn_id: turn.id,
                    error: error.clone(),
                }
            } else {
                RuntimeTurnState::Active {
                    turn_id: owner.acknowledged_turn,
                }
            }
        });
    let mut outstanding_requests = entry
        .requests
        .iter()
        .filter_map(|(id, record)| {
            let responding = matches!(record.status, RequestStatus::Responding { .. });
            matches!(
                record.status,
                RequestStatus::Pending | RequestStatus::Responding { .. }
            )
            .then(|| OutstandingRequest {
                request_id: id.as_str().to_string(),
                kind: match id {
                    RuntimeRequestId::Approval(_) => RequestKind::Approval,
                    RuntimeRequestId::Server(_) => RequestKind::Server,
                },
                responding,
            })
        })
        .collect::<Vec<_>>();
    outstanding_requests.sort_by(|left, right| left.request_id.cmp(&right.request_id));
    if matches!(turn_state, RuntimeTurnState::Idle) && outstanding_requests.is_empty() {
        return None;
    }
    Some(ThreadRuntimeSummary {
        thread_id,
        turn_state,
        outstanding_requests,
    })
}

fn wire_request_state(thread_id: ThreadId, record: &RequestRecord) -> WireRequestState {
    let (request_id, payload) = match &record.payload {
        RequestPayload::Approval(request) => (
            request.id.0.clone(),
            WireRequestPayload::Approval {
                request: WireApprovalRequest::from(request.clone()),
            },
        ),
        RequestPayload::Server(request) => (
            request.id.0.clone(),
            WireRequestPayload::Server {
                request: request.clone(),
            },
        ),
    };
    let status = match &record.status {
        RequestStatus::Pending => WireRequestStatus::Pending,
        RequestStatus::Responding { .. } => WireRequestStatus::Responding,
        RequestStatus::Resolved(RequestResolution::Approval(decision)) => {
            WireRequestStatus::Resolved {
                resolution: WireRequestResolution::Approval {
                    decision: decision.clone(),
                },
            }
        }
        RequestStatus::Resolved(RequestResolution::Server(_)) => WireRequestStatus::Resolved {
            resolution: WireRequestResolution::Server,
        },
    };
    WireRequestState {
        thread_id,
        request_id,
        revision: record.revision,
        payload,
        status,
    }
}

impl RequestClaim {
    fn support(&self) -> ThreadRuntimeSupport {
        ThreadRuntimeSupport {
            overview: self.overview.clone(),
            max_command_output_bytes: self.max_command_output_bytes,
        }
    }

    pub(crate) fn commit(
        mut self,
        resolution: RequestResolution,
    ) -> Result<RequestTransition, Box<RequestCommitError>> {
        let support = self.support();
        let Some(entry) = support.existing_entry(&self.authority) else {
            self.settled = true;
            return Err(Box::new(RequestCommitError {
                error: HarnessError::Protocol(format!(
                    "runtime state for request {} disappeared",
                    self.request_id.as_str()
                )),
                rollback: None,
            }));
        };
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        let Some(record) = entry.requests.get_mut(&self.request_id) else {
            self.settled = true;
            return Err(Box::new(RequestCommitError {
                error: HarnessError::Protocol(format!(
                    "request {} disappeared",
                    self.request_id.as_str()
                )),
                rollback: None,
            }));
        };
        if !matches!(
            record.status,
            RequestStatus::Responding { claim, .. } if claim == self.claim_id
        ) {
            let error = HarnessError::Protocol(format!(
                "stale claim for request {}",
                self.request_id.as_str()
            ));
            drop(entry);
            let rollback = self.rollback_inner();
            return Err(Box::new(RequestCommitError { error, rollback }));
        }
        match (&record.payload, &resolution) {
            (RequestPayload::Approval(_), RequestResolution::Approval(_))
            | (RequestPayload::Server(_), RequestResolution::Server(_)) => {}
            _ => {
                let error = HarnessError::Protocol(format!(
                    "response kind does not match request {}",
                    self.request_id.as_str()
                ));
                drop(entry);
                let rollback = self.rollback_inner();
                return Err(Box::new(RequestCommitError { error, rollback }));
            }
        }
        record.status = RequestStatus::Resolved(resolution);
        record.revision = record.revision.saturating_add(1);
        let transition = RequestTransition {
            request_state: wire_request_state(self.thread_id, record),
            overview_if_changed: support.refresh_overview(self.thread_id, &entry),
        };
        self.settled = true;
        Ok(transition)
    }

    pub(crate) fn rollback(mut self) -> Option<RequestTransition> {
        self.rollback_inner()
    }

    fn rollback_inner(&mut self) -> Option<RequestTransition> {
        if self.settled {
            return None;
        }
        let support = self.support();
        let Some(entry) = support.existing_entry(&self.authority) else {
            self.settled = true;
            return None;
        };
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        if let Some(record) = entry.requests.get_mut(&self.request_id)
            && let RequestStatus::Responding {
                claim,
                harness_resolved,
            } = record.status
            && claim == self.claim_id
        {
            record.status = if harness_resolved {
                RequestStatus::Resolved(RequestResolution::Server(ServerRequestResponse::result(
                    serde_json::Value::Null,
                )))
            } else {
                RequestStatus::Pending
            };
            record.revision = record.revision.saturating_add(1);
            let transition = RequestTransition {
                request_state: wire_request_state(self.thread_id, record),
                overview_if_changed: support.refresh_overview(self.thread_id, &entry),
            };
            self.settled = true;
            return Some(transition);
        }
        self.settled = true;
        None
    }
}

impl Drop for RequestClaim {
    fn drop(&mut self) {
        if let Some(transition) = self.rollback_inner() {
            let outcome = match transition.request_state.status {
                WireRequestStatus::Resolved { .. } => "resolved",
                WireRequestStatus::Pending => "pending",
                WireRequestStatus::Responding => "responding",
            };
            warn!(
                thread_id = %self.thread_id,
                request_id = self.request_id.as_str(),
                revision = transition.request_state.revision,
                outcome,
                "request claim dropped without settlement"
            );
        }
    }
}

fn next_claim_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed).max(1)
}

impl Default for ThreadRuntimeSupport {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use giskard_core::approval::ApprovalKind;
    use giskard_core::model::ModelRef;
    use giskard_core::turn::{Mode, TurnMode, TurnModel, TurnStatus, TurnStatusKind};

    fn test_authority(thread_id: ThreadId) -> Arc<ThreadAuthority> {
        Arc::new(ThreadAuthority::new_for_test(thread_id, ProjectId::new()))
    }

    fn approval(id: &str) -> ApprovalRequest {
        ApprovalRequest {
            id: ApprovalId(id.into()),
            kind: ApprovalKind::Permission {
                detail: "test".into(),
            },
            reason: Some("test".into()),
            metadata: Vec::new(),
            available: vec![ApprovalDecision::Accept],
        }
    }

    fn server_request(id: &str) -> ServerRequest {
        ServerRequest {
            id: ServerRequestId(id.into()),
            method: "item/tool/requestUserInput".into(),
            params: serde_json::json!({"question": "continue?"}),
            received_at: Utc::now(),
        }
    }

    fn register_server_request(
        runtime: &ThreadRuntimeSupport,
        authority: &Arc<ThreadAuthority>,
        request: ServerRequest,
    ) -> AppliedRuntimeEvent {
        runtime.apply_event(
            authority,
            &AgentEvent::ServerRequestReceived {
                thread: authority.thread_id(),
                turn: Some(TurnId::new()),
                request,
            },
            false,
        )
    }

    fn resolve_server_request(
        runtime: &ThreadRuntimeSupport,
        authority: &Arc<ThreadAuthority>,
        id: &str,
    ) -> AppliedRuntimeEvent {
        runtime.apply_event(
            authority,
            &AgentEvent::ServerRequestResolved {
                thread: authority.thread_id(),
                turn: Some(TurnId::new()),
                request_id: ServerRequestId(id.into()),
            },
            false,
        )
    }

    #[test]
    fn replacement_runtime_preserves_exact_and_reentry_semantics() {
        let runtime = ThreadRuntimeSupport::new();
        let thread_id = ThreadId::new();
        let authority = test_authority(thread_id);
        let restore = runtime.restoration_permit(&authority);
        let output = runtime.persisted_command_output_version_permit(&authority);
        runtime.register_approval(&authority, approval("request"));
        let (claim, _) = runtime
            .claim_request(
                &authority,
                RuntimeRequestId::Approval(ApprovalId("request".into())),
            )
            .unwrap();
        let mut old_lease = runtime
            .reserve_turn(
                &authority,
                TurnReservation {
                    project_id: ProjectId::new(),
                    harness_thread_id: "old".into(),
                    mode: TurnMode::Unknown,
                    model: TurnModel::Unknown,
                    context_kind: "test",
                },
            )
            .unwrap();

        runtime.forget_threads(std::slice::from_ref(&authority));
        let _new_lease = runtime
            .reserve_turn(
                &authority,
                TurnReservation {
                    project_id: ProjectId::new(),
                    harness_thread_id: "new".into(),
                    mode: TurnMode::Unknown,
                    model: TurnModel::Unknown,
                    context_kind: "test",
                },
            )
            .unwrap();
        runtime.register_approval(&authority, approval("request"));

        assert!(!runtime.restoration_is_current(&restore));
        assert!(
            output
                .cache(TurnId::new(), ItemId::new(), "version".into())
                .is_none()
        );
        assert!(claim.rollback().is_none());
        assert!(old_lease.release().is_some());
        assert!(!runtime.has_active_turn(&authority));
    }

    #[test]
    fn support_keeps_the_configured_command_output_limit() {
        let runtime = ThreadRuntimeSupport::with_max_command_output_bytes(4);
        let expected = giskard_persist::normalize_command_output("abcdefgh".into(), 4);
        let event = runtime.normalize_command_output(AgentEvent::ItemCompleted {
            thread: ThreadId::new(),
            turn: TurnId::new(),
            item: Item {
                id: ItemId::new(),
                harness_item_id: "command".into(),
                payload: ItemPayload::CommandExecution {
                    command: "printf output".into(),
                    cwd: "/tmp".into(),
                    output: "abcdefgh".into(),
                    output_truncated: false,
                    output_original_bytes: None,
                    output_original_lines: None,
                    exit_code: Some(0),
                    status: Some("completed".into()),
                    process_id: None,
                    duration_ms: None,
                },
                created_at: Utc::now(),
            },
        });
        let AgentEvent::ItemCompleted { item, .. } = event else {
            panic!("expected completed item");
        };
        let ItemPayload::CommandExecution { output, .. } = item.payload else {
            panic!("expected command output");
        };
        assert_eq!(output, expected.output);
    }

    #[test]
    fn authority_runtime_removal_advances_and_clears_overview() {
        let runtime = ThreadRuntimeSupport::new();
        let authority = test_authority(ThreadId::new());
        runtime.register_approval(&authority, approval("overview"));
        let before = runtime.current_overview();

        runtime.forget_threads(&[authority]);

        let after = runtime.current_overview();
        assert_eq!(after.revision, before.revision + 1);
        assert!(after.threads.is_empty());
    }

    fn reserve_test_turn(
        runtime: &ThreadRuntimeSupport,
        thread_id: ThreadId,
    ) -> (ProjectId, Arc<ThreadAuthority>, ThreadTurnLease) {
        let project_id = ProjectId::new();
        let authority = test_authority(thread_id);
        let lease = runtime
            .reserve_turn(
                &authority,
                TurnReservation {
                    project_id,
                    harness_thread_id: "native".into(),
                    mode: TurnMode::Known(Mode::Build),
                    model: TurnModel::Known(ModelRef {
                        provider: "provider".into(),
                        model: "model".into(),
                        reasoning_effort: None,
                    }),
                    context_kind: "test",
                },
            )
            .unwrap();
        (project_id, authority, lease)
    }

    #[test]
    fn replacing_active_diff_returns_conflict_for_immediately_previous_identity() {
        let runtime = ThreadRuntimeSupport::new();
        let thread = ThreadId::new();
        let authority = test_authority(thread);
        let turn = TurnId::new();
        let make_event = |text: &str| AgentEvent::DiffUpdated {
            thread,
            turn,
            diff: giskard_core::FileDiff {
                path: "src/main.rs".into(),
                change: giskard_core::FileChangeKind::Modified,
                old_text: None,
                new_text: Some(text.into()),
                hunks: Vec::new(),
                binary: false,
                captured: None,
            },
        };

        let first = runtime.capture_event_diffs(&authority, make_event("first"));
        let first_id = match first {
            AgentEvent::DiffUpdated { diff, .. } => diff.captured.unwrap().id,
            _ => panic!("expected diff event"),
        };
        let second = runtime.capture_event_diffs(&authority, make_event("second"));
        let second_id = match second {
            AgentEvent::DiffUpdated { diff, .. } => diff.captured.unwrap().id,
            _ => panic!("expected diff event"),
        };

        assert!(matches!(
            runtime.captured_diff(&authority, turn, &first_id),
            RuntimeDiffLookup::Superseded(current) if current.id == second_id
        ));
        assert!(matches!(
            runtime.captured_diff(&authority, turn, &second_id),
            RuntimeDiffLookup::Found(_)
        ));
        let repeated = runtime.capture_event_diffs(&authority, make_event("second"));
        let repeated_id = match repeated {
            AgentEvent::DiffUpdated { diff, .. } => diff.captured.unwrap().id,
            _ => panic!("expected diff event"),
        };
        assert_eq!(
            second_id, repeated_id,
            "identical content reuses its hash id"
        );
        let entry = runtime.existing_entry(&authority).unwrap();
        let entry = lock_unpoison(&entry, "thread runtime entry");
        assert_eq!(entry.captured_diffs[&turn].contents.len(), 1);
    }

    #[test]
    fn identical_unified_text_on_different_paths_has_independent_identity() {
        let mut state = ActiveCapturedDiffs::default();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let (first, first_record) = giskard_core::capture_unified_diff(
            "src/first.rs".into(),
            giskard_core::FileChangeKind::Modified,
            None,
            "@@ -1 +1 @@\n-old\n+same".into(),
        );
        let (second, second_record) = giskard_core::capture_unified_diff(
            "src/second.rs".into(),
            giskard_core::FileChangeKind::Modified,
            None,
            "@@ -1 +1 @@\n-old\n+same".into(),
        );
        assert_ne!(first.id, second.id);
        install_captured_diff(
            &mut state,
            thread,
            turn,
            CapturedDiffSlot::Turn(first.path.clone()),
            first.clone(),
            first_record,
        );
        install_captured_diff(
            &mut state,
            thread,
            turn,
            CapturedDiffSlot::Turn(second.path.clone()),
            second.clone(),
            second_record,
        );

        let (replacement, replacement_record) = giskard_core::capture_unified_diff(
            "src/first.rs".into(),
            giskard_core::FileChangeKind::Modified,
            None,
            "@@ -1 +1 @@\n-old\n+changed".into(),
        );
        install_captured_diff(
            &mut state,
            thread,
            turn,
            CapturedDiffSlot::Turn(replacement.path.clone()),
            replacement,
            replacement_record,
        );

        assert!(state.contents.contains_key(&second.id));
        assert!(!state.superseded.contains_key(&second.id));
        assert_eq!(
            state.current_by_slot[&CapturedDiffSlot::Turn(second.path.clone())].id,
            second.id
        );
    }

    #[test]
    fn item_and_turn_diffs_for_the_same_path_have_independent_authority() {
        let mut state = ActiveCapturedDiffs::default();
        let thread = ThreadId::new();
        let turn_id = TurnId::new();
        let path = std::path::PathBuf::from("src/main.rs");
        let item_id = ItemId::new();
        let (item, item_record) = giskard_core::capture_unified_diff(
            path.clone(),
            giskard_core::FileChangeKind::Modified,
            Some(item_id),
            "item body".into(),
        );
        let structured = giskard_core::FileDiff {
            path: path.clone(),
            change: giskard_core::FileChangeKind::Modified,
            old_text: Some("old".into()),
            new_text: Some("turn body".into()),
            hunks: Vec::new(),
            binary: false,
            captured: None,
        };
        let (turn, turn_record) = giskard_core::capture_structured_diff(structured);
        let turn = turn.captured.unwrap();

        install_captured_diff(
            &mut state,
            thread,
            turn_id,
            CapturedDiffSlot::Item {
                item_id,
                path: path.clone(),
                occurrence: 0,
            },
            item.clone(),
            item_record,
        );
        install_captured_diff(
            &mut state,
            thread,
            turn_id,
            CapturedDiffSlot::Turn(path.clone()),
            turn.clone(),
            turn_record,
        );

        assert!(state.contents.contains_key(&item.id));
        assert!(state.contents.contains_key(&turn.id));
        assert!(state.superseded.is_empty());
        assert_eq!(
            state.current_by_slot[&CapturedDiffSlot::Item {
                item_id,
                path: path.clone(),
                occurrence: 0,
            }]
                .id,
            item.id
        );
        assert_eq!(
            state.current_by_slot[&CapturedDiffSlot::Turn(path)].id,
            turn.id
        );
    }

    #[test]
    fn empty_inline_diff_is_captured_instead_of_dropped() {
        let runtime = ThreadRuntimeSupport::new();
        let thread = ThreadId::new();
        let authority = test_authority(thread);
        let turn = TurnId::new();
        let item_id = ItemId::new();
        let event = AgentEvent::ItemCompleted {
            thread,
            turn,
            item: giskard_core::Item {
                id: item_id,
                harness_item_id: "empty-diff-item".into(),
                payload: ItemPayload::FileChange {
                    path: "src/empty.rs".into(),
                    change: giskard_core::FileChangeKind::Modified,
                    changes: vec![giskard_core::item::FileChangeEntry {
                        path: "src/empty.rs".into(),
                        change: giskard_core::FileChangeKind::Modified,
                        diff: Some(String::new()),
                        captured_diff: None,
                    }],
                    status: None,
                },
                created_at: chrono::Utc::now(),
            },
        };

        let captured = runtime.capture_event_diffs(&authority, event);
        let descriptor = match captured {
            AgentEvent::ItemCompleted { item, .. } => match item.payload {
                ItemPayload::FileChange { changes, .. } => {
                    let change = &changes[0];
                    assert!(change.diff.is_none());
                    change.captured_diff.clone().unwrap()
                }
                _ => panic!("expected file-change payload"),
            },
            _ => panic!("expected completed item"),
        };
        assert_eq!(descriptor.byte_size, 0);
        assert!(matches!(
            runtime.captured_diff(&authority, turn, &descriptor.id),
            RuntimeDiffLookup::Found(CapturedDiffRecord {
                content: giskard_core::CapturedDiffContent::Unified { text },
                ..
            }) if text.is_empty()
        ));
    }

    #[test]
    fn item_diff_reconciliation_treats_each_completion_as_a_complete_set() {
        let runtime = ThreadRuntimeSupport::new();
        let thread = ThreadId::new();
        let authority = test_authority(thread);
        let turn = TurnId::new();
        let item_id = ItemId::new();
        let capture = |changes: Vec<(&str, &str)>| {
            let event = AgentEvent::ItemCompleted {
                thread,
                turn,
                item: giskard_core::Item {
                    id: item_id,
                    harness_item_id: "replacement-item".into(),
                    payload: ItemPayload::FileChange {
                        path: changes[0].0.into(),
                        change: giskard_core::FileChangeKind::Modified,
                        changes: changes
                            .into_iter()
                            .map(|(path, text)| giskard_core::item::FileChangeEntry {
                                path: path.into(),
                                change: giskard_core::FileChangeKind::Modified,
                                diff: Some(text.into()),
                                captured_diff: None,
                            })
                            .collect(),
                        status: None,
                    },
                    created_at: chrono::Utc::now(),
                },
            };
            match runtime.capture_event_diffs(&authority, event) {
                AgentEvent::ItemCompleted { item, .. } => match item.payload {
                    ItemPayload::FileChange { changes, .. } => changes
                        .into_iter()
                        .map(|change| change.captured_diff.unwrap())
                        .collect::<Vec<_>>(),
                    _ => panic!("expected file-change payload"),
                },
                _ => panic!("expected completed item"),
            }
        };

        // Duplicate paths receive occurrence-scoped slots, so every descriptor retained by the
        // item has a corresponding content record for persistence.
        let first = capture(vec![
            ("src/a.rs", "a0"),
            ("src/a.rs", "a1"),
            ("src/b.rs", "b0"),
        ]);
        assert_eq!(runtime.captured_diff_records(&authority, turn).len(), 3);
        for descriptor in &first {
            assert!(matches!(
                runtime.captured_diff(&authority, turn, &descriptor.id),
                RuntimeDiffLookup::Found(_)
            ));
        }

        // Reordering distinct paths does not change their slots; duplicate occurrences replace
        // only their corresponding occurrence.
        let second = capture(vec![
            ("src/b.rs", "b1"),
            ("src/a.rs", "a0-next"),
            ("src/a.rs", "a1-next"),
        ]);
        assert_eq!(runtime.captured_diff_records(&authority, turn).len(), 3);
        for descriptor in &second {
            assert!(matches!(
                runtime.captured_diff(&authority, turn, &descriptor.id),
                RuntimeDiffLookup::Found(_)
            ));
        }

        // Omitting one duplicate and renaming another path retires those slots instead of keeping
        // obsolete bodies alive. Repeating an unchanged body preserves its stable identity.
        let third = capture(vec![("src/a.rs", "a0-next"), ("src/c.rs", "c0")]);
        assert_eq!(third[0].id, second[1].id);
        assert_eq!(runtime.captured_diff_records(&authority, turn).len(), 2);
        assert!(matches!(
            runtime.captured_diff(&authority, turn, &second[0].id),
            RuntimeDiffLookup::Missing
        ));
        assert!(matches!(
            runtime.captured_diff(&authority, turn, &second[2].id),
            RuntimeDiffLookup::Missing
        ));
        let entry = runtime.existing_entry(&authority).unwrap();
        let entry = lock_unpoison(&entry, "thread runtime entry");
        let state = &entry.captured_diffs[&turn];
        assert_eq!(state.current_by_slot.len(), 2);
        assert_eq!(state.contents.len(), 2);
        drop(entry);

        runtime.capture_event_diffs(
            &authority,
            AgentEvent::ItemCompleted {
                thread,
                turn,
                item: giskard_core::Item {
                    id: item_id,
                    harness_item_id: "replacement-item".into(),
                    payload: ItemPayload::AgentMessage {
                        text: "the item no longer represents a file change".into(),
                    },
                    created_at: chrono::Utc::now(),
                },
            },
        );
        assert!(runtime.captured_diff_records(&authority, turn).is_empty());
    }

    #[test]
    fn read_only_queries_do_not_create_runtime_entries() {
        let runtime = ThreadRuntimeSupport::new();
        let thread_id = ThreadId::new();
        let authority = test_authority(thread_id);

        assert!(!runtime.live_is_active(&authority));
        assert!(runtime.live_snapshot(&authority).is_none());
        assert!(
            runtime
                .live_item_events(&authority, ItemId::new())
                .is_empty()
        );
        assert_eq!(runtime.tasks_snapshot(&authority), (0, Vec::new()));
        assert!(!runtime.has_running_for_thread(&authority));
        assert!(runtime.request_states(&authority).is_empty());
        assert!(authority.runtime_entry().is_none());
    }

    #[test]
    fn foreign_thread_event_is_rejected_before_mutation() {
        let runtime = ThreadRuntimeSupport::new();
        let target_thread = ThreadId::new();
        let authority = test_authority(target_thread);
        let event_thread = ThreadId::new();
        let event_authority = test_authority(event_thread);
        let request = approval("foreign");

        let applied = runtime.apply_event(
            &authority,
            &AgentEvent::ApprovalRequested {
                thread: event_thread,
                turn: TurnId::new(),
                request: request.clone(),
            },
            true,
        );

        assert!(applied.sequence.is_none());
        assert!(!applied.tasks_changed);
        assert!(applied.request_state.is_none());
        assert!(runtime.request_states(&authority).is_empty());
        assert!(runtime.request_states(&event_authority).is_empty());
        assert!(authority.runtime_entry().is_none());
        assert!(event_authority.runtime_entry().is_none());
    }

    #[test]
    fn requests_are_claimed_independently_and_failed_claims_roll_back() {
        let runtime = ThreadRuntimeSupport::new();
        let thread_id = ThreadId::new();
        let authority = test_authority(thread_id);
        runtime.register_approval(&authority, approval("a"));
        runtime.register_approval(&authority, approval("b"));
        assert_eq!(runtime.request_states(&authority).len(), 2);

        let (claim_a, _) = runtime
            .claim_request(
                &authority,
                RuntimeRequestId::Approval(ApprovalId("a".into())),
            )
            .unwrap();
        assert_eq!(
            runtime
                .request_state(
                    &authority,
                    &RuntimeRequestId::Approval(ApprovalId("a".into()))
                )
                .unwrap()
                .revision,
            2
        );
        let (claim_b, _) = runtime
            .claim_request(
                &authority,
                RuntimeRequestId::Approval(ApprovalId("b".into())),
            )
            .unwrap();
        drop(claim_a);
        claim_b
            .commit(RequestResolution::Approval(ApprovalDecision::Accept))
            .unwrap();
        assert_eq!(
            runtime
                .request_state(
                    &authority,
                    &RuntimeRequestId::Approval(ApprovalId("a".into()))
                )
                .unwrap()
                .revision,
            3,
            "rolling a failed claim back is an ordered state transition"
        );

        let states = runtime.request_states(&authority);
        assert_eq!(
            states.len(),
            2,
            "resolving one request must not erase its peer"
        );
        assert!(
            states.iter().any(|state| state.request_id == "a"
                && matches!(state.status, WireRequestStatus::Pending))
        );
        assert!(states.iter().any(|state| state.request_id == "b"
            && matches!(state.status, WireRequestStatus::Resolved { .. })));

        assert!(
            runtime
                .claim_request(
                    &authority,
                    RuntimeRequestId::Approval(ApprovalId("a".into()))
                )
                .is_ok()
        );
        assert!(
            runtime
                .claim_request(
                    &authority,
                    RuntimeRequestId::Approval(ApprovalId("b".into()))
                )
                .is_err()
        );
    }

    #[test]
    fn harness_resolution_during_claim_does_not_preempt_commit() {
        let runtime = ThreadRuntimeSupport::new();
        let authority = test_authority(ThreadId::new());
        register_server_request(&runtime, &authority, server_request("srv"));
        let request_id = RuntimeRequestId::Server(ServerRequestId("srv".into()));
        let (claim, transition) = runtime
            .claim_request(&authority, request_id.clone())
            .unwrap();
        assert_eq!(transition.request_state.revision, 2);
        assert!(matches!(
            transition.request_state.status,
            WireRequestStatus::Responding
        ));

        let applied = resolve_server_request(&runtime, &authority, "srv");
        assert!(applied.request_state.is_none());
        let state = runtime.request_state(&authority, &request_id).unwrap();
        assert_eq!(state.revision, 2);
        assert!(matches!(state.status, WireRequestStatus::Responding));

        let answer = ServerRequestResponse::result(serde_json::json!({"answer": 1}));
        let committed = claim
            .commit(RequestResolution::Server(answer.clone()))
            .unwrap();
        assert_eq!(committed.request_state.revision, 3);
        assert!(matches!(
            committed.request_state.status,
            WireRequestStatus::Resolved { .. }
        ));
        assert_eq!(
            runtime.resolution_for_test(&authority, &request_id),
            Some(RequestResolution::Server(answer))
        );
    }

    #[test]
    fn harness_resolution_during_claim_resolves_on_rollback() {
        let runtime = ThreadRuntimeSupport::new();
        let authority = test_authority(ThreadId::new());
        register_server_request(&runtime, &authority, server_request("srv"));
        let request_id = RuntimeRequestId::Server(ServerRequestId("srv".into()));
        let (claim, _) = runtime
            .claim_request(&authority, request_id.clone())
            .unwrap();
        resolve_server_request(&runtime, &authority, "srv");

        let transition = claim.rollback().unwrap();
        assert_eq!(transition.request_state.revision, 3);
        assert!(matches!(
            transition.request_state.status,
            WireRequestStatus::Resolved {
                resolution: WireRequestResolution::Server
            }
        ));
        assert!(
            runtime
                .claim_request(&authority, request_id)
                .err()
                .unwrap()
                .to_string()
                .contains("is not pending")
        );
    }

    #[test]
    fn harness_resolution_during_claim_resolves_on_drop() {
        let runtime = ThreadRuntimeSupport::new();
        let authority = test_authority(ThreadId::new());
        register_server_request(&runtime, &authority, server_request("srv"));
        let request_id = RuntimeRequestId::Server(ServerRequestId("srv".into()));
        let (claim, _) = runtime
            .claim_request(&authority, request_id.clone())
            .unwrap();
        resolve_server_request(&runtime, &authority, "srv");
        drop(claim);

        let state = runtime.request_state(&authority, &request_id).unwrap();
        assert_eq!(state.revision, 3);
        assert!(matches!(state.status, WireRequestStatus::Resolved { .. }));
    }

    #[test]
    fn rollback_without_harness_resolution_still_returns_to_pending() {
        let runtime = ThreadRuntimeSupport::new();
        let authority = test_authority(ThreadId::new());
        register_server_request(&runtime, &authority, server_request("srv"));
        let request_id = RuntimeRequestId::Server(ServerRequestId("srv".into()));
        let (claim, _) = runtime
            .claim_request(&authority, request_id.clone())
            .unwrap();

        let transition = claim.rollback().unwrap();
        assert_eq!(transition.request_state.revision, 3);
        assert!(matches!(
            transition.request_state.status,
            WireRequestStatus::Pending
        ));
        assert!(runtime.claim_request(&authority, request_id).is_ok());
    }

    #[test]
    fn harness_resolution_before_any_claim_still_resolves_and_publishes() {
        let runtime = ThreadRuntimeSupport::new();
        let authority = test_authority(ThreadId::new());
        register_server_request(&runtime, &authority, server_request("srv"));
        let request_id = RuntimeRequestId::Server(ServerRequestId("srv".into()));

        let applied = resolve_server_request(&runtime, &authority, "srv");
        let state = applied.request_state.unwrap();
        assert_eq!(state.revision, 2);
        assert!(matches!(state.status, WireRequestStatus::Resolved { .. }));
        assert!(runtime.claim_request(&authority, request_id).is_err());
    }

    #[test]
    fn harness_resolution_after_commit_is_a_no_op() {
        let runtime = ThreadRuntimeSupport::new();
        let authority = test_authority(ThreadId::new());
        register_server_request(&runtime, &authority, server_request("srv"));
        let request_id = RuntimeRequestId::Server(ServerRequestId("srv".into()));
        let answer = ServerRequestResponse::result(serde_json::json!({"answer": 1}));
        runtime
            .claim_request(&authority, request_id.clone())
            .unwrap()
            .0
            .commit(RequestResolution::Server(answer.clone()))
            .unwrap();

        let applied = resolve_server_request(&runtime, &authority, "srv");
        assert!(applied.request_state.is_none());
        assert_eq!(
            runtime
                .request_state(&authority, &request_id)
                .unwrap()
                .revision,
            3
        );
        assert_eq!(
            runtime.resolution_for_test(&authority, &request_id),
            Some(RequestResolution::Server(answer))
        );
    }

    #[test]
    fn repeated_harness_resolutions_during_claim_are_idempotent() {
        let runtime = ThreadRuntimeSupport::new();
        let authority = test_authority(ThreadId::new());
        register_server_request(&runtime, &authority, server_request("srv"));
        let request_id = RuntimeRequestId::Server(ServerRequestId("srv".into()));
        let (claim, _) = runtime
            .claim_request(&authority, request_id.clone())
            .unwrap();

        assert!(
            resolve_server_request(&runtime, &authority, "srv")
                .request_state
                .is_none()
        );
        assert!(
            resolve_server_request(&runtime, &authority, "srv")
                .request_state
                .is_none()
        );
        assert_eq!(
            runtime
                .request_state(&authority, &request_id)
                .unwrap()
                .revision,
            2
        );
        assert!(
            claim
                .commit(RequestResolution::Server(ServerRequestResponse::result(
                    serde_json::Value::Null,
                )))
                .is_ok()
        );
    }

    #[test]
    fn only_the_current_claim_can_settle_the_record() {
        let runtime = ThreadRuntimeSupport::new();
        let authority = test_authority(ThreadId::new());
        register_server_request(&runtime, &authority, server_request("first"));
        let first_id = RuntimeRequestId::Server(ServerRequestId("first".into()));
        let (claim_a, _) = runtime.claim_request(&authority, first_id.clone()).unwrap();
        claim_a.rollback();
        let (claim_b, _) = runtime.claim_request(&authority, first_id.clone()).unwrap();
        resolve_server_request(&runtime, &authority, "first");
        assert!(
            claim_b
                .commit(RequestResolution::Server(ServerRequestResponse::result(
                    serde_json::Value::Null,
                )))
                .is_ok()
        );

        register_server_request(&runtime, &authority, server_request("second"));
        let second_id = RuntimeRequestId::Server(ServerRequestId("second".into()));
        let (claim, _) = runtime
            .claim_request(&authority, second_id.clone())
            .unwrap();
        assert!(
            runtime
                .claim_request(&authority, second_id.clone())
                .err()
                .unwrap()
                .to_string()
                .contains("is not pending")
        );
        resolve_server_request(&runtime, &authority, "second");
        drop(claim);
        assert!(runtime.claim_request(&authority, second_id).is_err());
    }

    #[test]
    fn duplicate_request_delivery_during_a_claim_keeps_the_claim() {
        let runtime = ThreadRuntimeSupport::new();
        let authority = test_authority(ThreadId::new());
        let request = server_request("srv");
        register_server_request(&runtime, &authority, request.clone());
        let request_id = RuntimeRequestId::Server(request.id.clone());
        let (claim, _) = runtime
            .claim_request(&authority, request_id.clone())
            .unwrap();
        assert!(
            register_server_request(&runtime, &authority, request.clone())
                .request_state
                .is_none()
        );
        resolve_server_request(&runtime, &authority, "srv");
        let mut refreshed = request;
        refreshed.params = serde_json::json!({"question": "updated?"});
        let changed = register_server_request(&runtime, &authority, refreshed);
        assert_eq!(changed.request_state.unwrap().revision, 3);
        assert!(
            claim
                .commit(RequestResolution::Server(ServerRequestResponse::result(
                    serde_json::Value::Null,
                )))
                .is_ok()
        );
        assert_eq!(
            runtime
                .request_state(&authority, &request_id)
                .unwrap()
                .revision,
            4
        );

        let rollback_request = server_request("rollback");
        register_server_request(&runtime, &authority, rollback_request.clone());
        let rollback_id = RuntimeRequestId::Server(rollback_request.id.clone());
        let (claim, _) = runtime
            .claim_request(&authority, rollback_id.clone())
            .unwrap();
        resolve_server_request(&runtime, &authority, "rollback");
        let mut refreshed = rollback_request;
        refreshed.params = serde_json::json!({"question": "updated before rollback?"});
        register_server_request(&runtime, &authority, refreshed);

        let rollback = claim.rollback().unwrap();
        assert!(matches!(
            rollback.request_state.status,
            WireRequestStatus::Resolved { .. }
        ));
        assert!(runtime.claim_request(&authority, rollback_id).is_err());
    }

    #[test]
    fn responding_record_with_harness_resolution_is_still_outstanding_in_the_overview() {
        let runtime = ThreadRuntimeSupport::new();
        let authority = test_authority(ThreadId::new());
        register_server_request(&runtime, &authority, server_request("srv"));
        let request_id = RuntimeRequestId::Server(ServerRequestId("srv".into()));
        let (claim, _) = runtime.claim_request(&authority, request_id).unwrap();
        resolve_server_request(&runtime, &authority, "srv");
        let overview = runtime.current_overview();
        assert_eq!(overview.threads[0].outstanding_requests.len(), 1);
        assert!(overview.threads[0].outstanding_requests[0].responding);

        claim
            .commit(RequestResolution::Server(ServerRequestResponse::result(
                serde_json::Value::Null,
            )))
            .unwrap();
        assert!(runtime.current_overview().threads.is_empty());
    }

    #[test]
    fn approval_claims_are_untouched_by_server_resolutions() {
        let runtime = ThreadRuntimeSupport::new();
        let authority = test_authority(ThreadId::new());
        runtime.register_approval(&authority, approval("same"));
        register_server_request(&runtime, &authority, server_request("same"));
        let approval_id = RuntimeRequestId::Approval(ApprovalId("same".into()));
        let server_id = RuntimeRequestId::Server(ServerRequestId("same".into()));
        let (claim, _) = runtime
            .claim_request(&authority, approval_id.clone())
            .unwrap();

        resolve_server_request(&runtime, &authority, "same");
        assert!(matches!(
            runtime
                .request_state(&authority, &approval_id)
                .unwrap()
                .status,
            WireRequestStatus::Responding
        ));
        assert!(matches!(
            runtime
                .request_state(&authority, &server_id)
                .unwrap()
                .status,
            WireRequestStatus::Resolved { .. }
        ));
        assert!(
            claim
                .commit(RequestResolution::Approval(ApprovalDecision::Accept))
                .is_ok()
        );
    }

    #[test]
    fn commit_with_mismatched_resolution_kind_after_harness_resolution_resolves_on_rollback() {
        let runtime = ThreadRuntimeSupport::new();
        let authority = test_authority(ThreadId::new());
        register_server_request(&runtime, &authority, server_request("srv"));
        let request_id = RuntimeRequestId::Server(ServerRequestId("srv".into()));
        let (claim, _) = runtime.claim_request(&authority, request_id).unwrap();
        resolve_server_request(&runtime, &authority, "srv");

        let failure = claim
            .commit(RequestResolution::Approval(ApprovalDecision::Accept))
            .unwrap_err();
        let rollback = failure.rollback.unwrap();
        assert!(matches!(
            rollback.request_state.status,
            WireRequestStatus::Resolved { .. }
        ));
        assert!(matches!(failure.error, HarnessError::Protocol(_)));
    }

    #[test]
    fn failed_commit_returns_the_authoritative_rollback_transition() {
        let runtime = ThreadRuntimeSupport::new();
        let thread_id = ThreadId::new();
        let authority = test_authority(thread_id);
        runtime.register_approval(&authority, approval("mismatched"));
        let (claim, _) = runtime
            .claim_request(
                &authority,
                RuntimeRequestId::Approval(ApprovalId("mismatched".into())),
            )
            .unwrap();

        let failure = claim
            .commit(RequestResolution::Server(ServerRequestResponse::result(
                serde_json::Value::Null,
            )))
            .unwrap_err();

        let rollback = failure
            .rollback
            .expect("failed commit must expose rollback");
        assert_eq!(rollback.request_state.revision, 3);
        assert!(matches!(
            rollback.request_state.status,
            WireRequestStatus::Pending
        ));
        assert!(matches!(failure.error, HarnessError::Protocol(_)));
    }

    #[tokio::test]
    async fn duplicate_request_event_does_not_resurrect_a_resolved_request() {
        let runtime = ThreadRuntimeSupport::new();
        let thread_id = ThreadId::new();
        let authority = test_authority(thread_id);
        let turn_id = TurnId::new();
        let request = approval("duplicate");
        runtime.apply_event(
            &authority,
            &AgentEvent::ApprovalRequested {
                thread: thread_id,
                turn: turn_id,
                request: request.clone(),
            },
            false,
        );
        runtime
            .claim_request(&authority, RuntimeRequestId::Approval(request.id.clone()))
            .unwrap()
            .0
            .commit(RequestResolution::Approval(ApprovalDecision::Accept))
            .unwrap();

        let duplicate = runtime.apply_event(
            &authority,
            &AgentEvent::ApprovalRequested {
                thread: thread_id,
                turn: turn_id,
                request: request.clone(),
            },
            false,
        );
        // An identical redelivery changed nothing, so there is nothing to publish. Re-sending the
        // record under revision 3 would be indistinguishable from a real update to a client that
        // gates on revision.
        assert!(duplicate.request_state.is_none());
        assert!(duplicate.overview_if_changed.is_none());
        let state = runtime
            .request_state(&authority, &RuntimeRequestId::Approval(request.id))
            .expect("the resolved record survives a duplicate delivery");
        assert_eq!(state.revision, 3);
        assert!(matches!(state.status, WireRequestStatus::Resolved { .. }));
    }

    #[tokio::test]
    async fn duplicate_request_event_with_new_metadata_takes_a_new_revision() {
        let runtime = ThreadRuntimeSupport::new();
        let thread_id = ThreadId::new();
        let authority = test_authority(thread_id);
        let turn_id = TurnId::new();
        let request = approval("refreshed");
        runtime.apply_event(
            &authority,
            &AgentEvent::ApprovalRequested {
                thread: thread_id,
                turn: turn_id,
                request: request.clone(),
            },
            false,
        );

        let mut refreshed = request.clone();
        refreshed.reason = Some("the provider filled in a reason".into());
        let duplicate = runtime.apply_event(
            &authority,
            &AgentEvent::ApprovalRequested {
                thread: thread_id,
                turn: turn_id,
                request: refreshed,
            },
            false,
        );

        // Changed content must arrive under a revision the client has not already accepted.
        let state = duplicate
            .request_state
            .expect("refreshed metadata is a publishable change");
        assert_eq!(state.revision, 2);
        assert!(matches!(state.status, WireRequestStatus::Pending));
    }

    #[tokio::test]
    async fn event_application_refreshes_the_overview_only_for_summary_changes() {
        let runtime = ThreadRuntimeSupport::new();
        let thread_id = ThreadId::new();
        let authority = test_authority(thread_id);
        let turn_id = TurnId::new();
        let mut lease = runtime
            .reserve_turn(
                &authority,
                TurnReservation {
                    project_id: ProjectId::new(),
                    harness_thread_id: "native".into(),
                    mode: TurnMode::Known(Mode::Build),
                    model: TurnModel::Known(ModelRef {
                        provider: "provider".into(),
                        model: "model".into(),
                        reasoning_effort: None,
                    }),
                    context_kind: "user",
                },
            )
            .unwrap();
        let initial_revision = runtime.current_overview().revision;

        let notice = runtime.apply_event(
            &authority,
            &AgentEvent::Notice {
                thread: thread_id,
                turn: Some(turn_id),
                message: "stream progress".into(),
            },
            false,
        );
        assert!(!notice.overview_refresh_needed);
        assert!(notice.overview_if_changed.is_none());
        assert_eq!(runtime.current_overview().revision, initial_revision);

        let request = approval("overview-change");
        let registered = runtime.apply_event(
            &authority,
            &AgentEvent::ApprovalRequested {
                thread: thread_id,
                turn: turn_id,
                request: request.clone(),
            },
            false,
        );
        assert!(registered.overview_refresh_needed);
        assert_eq!(
            registered
                .overview_if_changed
                .as_ref()
                .map(|view| view.revision),
            Some(initial_revision + 1)
        );

        let duplicate = runtime.apply_event(
            &authority,
            &AgentEvent::ApprovalRequested {
                thread: thread_id,
                turn: turn_id,
                request,
            },
            false,
        );
        assert!(!duplicate.overview_refresh_needed);
        assert!(duplicate.overview_if_changed.is_none());
        assert_eq!(runtime.current_overview().revision, initial_revision + 1);

        lease.release();
    }

    #[tokio::test]
    async fn restore_permit_is_invalidated_by_a_new_turn_lifecycle() {
        let runtime = ThreadRuntimeSupport::new();
        let thread = ThreadId::new();
        let authority = test_authority(thread);
        let permit = runtime.restoration_permit(&authority);
        assert!(runtime.restoration_is_current(&permit));
        let _lease = runtime
            .reserve_turn(
                &authority,
                TurnReservation {
                    project_id: ProjectId::new(),
                    harness_thread_id: "native".into(),
                    mode: TurnMode::Known(Mode::Build),
                    model: TurnModel::Known(ModelRef {
                        provider: "provider".into(),
                        model: "model".into(),
                        reasoning_effort: None,
                    }),
                    context_kind: "test",
                },
            )
            .unwrap();
        assert!(!runtime.restoration_is_current(&permit));
    }

    #[tokio::test]
    async fn restore_permit_does_not_survive_forget_and_recreate() {
        let runtime = ThreadRuntimeSupport::new();
        let thread = ThreadId::new();
        let authority = test_authority(thread);
        let permit = runtime.restoration_permit(&authority);
        runtime.forget_threads(std::slice::from_ref(&authority));
        let replacement = runtime.restoration_permit(&authority);
        assert!(!runtime.restoration_is_current(&permit));
        assert!(runtime.restoration_is_current(&replacement));
    }

    #[tokio::test]
    async fn persisted_completion_releases_lease_and_prunes_resolved_turn_requests() {
        let runtime = ThreadRuntimeSupport::new();
        let thread_id = ThreadId::new();
        let authority = test_authority(thread_id);
        let turn_id = TurnId::new();
        let mut lease = runtime
            .reserve_turn(
                &authority,
                TurnReservation {
                    project_id: ProjectId::new(),
                    harness_thread_id: "native".into(),
                    mode: TurnMode::Known(Mode::Build),
                    model: TurnModel::Known(ModelRef {
                        provider: "provider".into(),
                        model: "model".into(),
                        reasoning_effort: None,
                    }),
                    context_kind: "user",
                },
            )
            .unwrap();
        assert!(
            lease.acknowledge_turn(turn_id).is_some(),
            "adopting the harness turn id changes the overview projection"
        );
        let request = approval("settled");
        runtime.apply_event(
            &authority,
            &AgentEvent::ApprovalRequested {
                thread: thread_id,
                turn: turn_id,
                request: request.clone(),
            },
            false,
        );
        runtime
            .claim_request(&authority, RuntimeRequestId::Approval(request.id))
            .unwrap()
            .0
            .commit(RequestResolution::Approval(ApprovalDecision::Accept))
            .unwrap();

        let completion = AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: turn_id,
            usage: Default::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        };
        let applied = lease.commit_after_persistence(&completion);
        assert_eq!(applied.sequence, Some(2));
        assert!(lease.is_released());
        assert!(!runtime.has_active_turn(&authority));
        assert!(runtime.request_states(&authority).is_empty());
        assert!(runtime.current_overview().threads.is_empty());
    }

    #[test]
    fn claim_validates_the_thread_identity() {
        let runtime = ThreadRuntimeSupport::new();
        let owner = ThreadId::new();
        let owner_authority = test_authority(owner);
        let other_authority = test_authority(ThreadId::new());
        runtime.register_approval(&owner_authority, approval("a"));
        let result = runtime.claim_request(
            &other_authority,
            RuntimeRequestId::Approval(ApprovalId("a".into())),
        );
        assert!(matches!(result, Err(error) if error.to_string().contains("no pending request")));
    }

    #[tokio::test]
    async fn empty_overview_replaces_the_last_active_summary() {
        let runtime = ThreadRuntimeSupport::new();
        let thread_id = ThreadId::new();
        let authority = test_authority(thread_id);
        runtime.register_approval(&authority, approval("a"));
        assert_eq!(runtime.current_overview().threads.len(), 1);
        runtime.forget_threads(std::slice::from_ref(&authority));
        assert!(runtime.current_overview().threads.is_empty());
    }

    #[tokio::test]
    async fn explicit_lease_release_returns_the_empty_overview_effect() {
        let runtime = ThreadRuntimeSupport::new();
        let thread_id = ThreadId::new();
        let authority = test_authority(thread_id);
        let mut lease = runtime
            .reserve_turn(
                &authority,
                TurnReservation {
                    project_id: ProjectId::new(),
                    harness_thread_id: "native".into(),
                    mode: TurnMode::Known(Mode::Build),
                    model: TurnModel::Known(ModelRef {
                        provider: "provider".into(),
                        model: "model".into(),
                        reasoning_effort: None,
                    }),
                    context_kind: "user",
                },
            )
            .unwrap();

        let overview = lease.release().expect("release changes the overview");
        assert!(overview.threads.is_empty());
        assert!(!runtime.has_active_turn(&authority));
        assert!(lease.release().is_none());
    }

    #[tokio::test]
    async fn persistence_failure_keeps_the_complete_turn_and_lease() {
        let runtime = ThreadRuntimeSupport::new();
        let thread_id = ThreadId::new();
        let authority = test_authority(thread_id);
        let reservation = TurnReservation {
            project_id: ProjectId::new(),
            harness_thread_id: "native".into(),
            mode: TurnMode::Known(Mode::Build),
            model: TurnModel::Known(ModelRef {
                provider: "provider".into(),
                model: "model".into(),
                reasoning_effort: None,
            }),
            context_kind: "user",
        };
        let mut lease = runtime
            .reserve_turn(&authority, reservation.clone())
            .unwrap();
        let command_item_id = ItemId::new();
        let tool_item_id = ItemId::new();
        let turn = Turn {
            id: TurnId::new(),
            user_input: UserInput::text("keep me"),
            items: Vec::new(),
            model: ModelRef {
                provider: "provider".into(),
                model: "model".into(),
                reasoning_effort: None,
            }
            .into(),
            mode: TurnMode::Known(Mode::Build),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
            usage: Default::default(),
            diffs: Vec::new(),
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
        };
        runtime.apply_event(
            &authority,
            &AgentEvent::ItemCompleted {
                thread: thread_id,
                turn: turn.id,
                item: Item {
                    id: command_item_id,
                    harness_item_id: "command".into(),
                    payload: ItemPayload::CommandExecution {
                        command: "printf retained".into(),
                        cwd: "/tmp".into(),
                        output: "retained command".into(),
                        output_truncated: false,
                        output_original_bytes: None,
                        output_original_lines: None,
                        exit_code: Some(0),
                        status: Some("completed".into()),
                        process_id: None,
                        duration_ms: None,
                    },
                    created_at: Utc::now(),
                },
            },
            true,
        );
        runtime.apply_event(
            &authority,
            &AgentEvent::ItemCompleted {
                thread: thread_id,
                turn: turn.id,
                item: Item {
                    id: tool_item_id,
                    harness_item_id: "tool".into(),
                    payload: ItemPayload::ToolCall {
                        name: "lookup".into(),
                        input: serde_json::json!({}),
                        output: Some(serde_json::json!({"retained": true})),
                        server: Some("mcp".into()),
                        status: Some("completed".into()),
                        metadata: None,
                        subagent: None,
                        error: None,
                    },
                    created_at: Utc::now(),
                },
            },
            true,
        );
        let completion = AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: turn.id,
            usage: turn.usage,
            status: turn.status.clone(),
        };
        lease.retain_after_persistence_failure(&completion, turn.clone(), "disk full".into());

        assert!(matches!(
            runtime.current_overview().threads[0].turn_state,
            RuntimeTurnState::PersistenceBlocked { turn_id, .. } if turn_id == turn.id
        ));
        assert!(
            runtime
                .reserve_turn(&authority, reservation.clone())
                .is_err()
        );
        assert!(matches!(
            runtime.command_output(&authority, turn.id, command_item_id),
            RuntimeCommandOutputLookup::Found(_)
        ));
        assert!(matches!(
            runtime.tool_output(&authority, turn.id, tool_item_id),
            RuntimeToolOutputLookup::Found(_)
        ));
        let entry = runtime.existing_entry(&authority).unwrap();
        let entry = lock_unpoison(&entry, "thread runtime entry");
        assert_eq!(
            entry
                .active_turn
                .as_ref()
                .unwrap()
                .persistence_blocked
                .as_ref()
                .unwrap()
                .0,
            turn
        );
    }

    #[test]
    fn terminal_command_output_is_normalized_and_addressable_until_cleanup() {
        let runtime = ThreadRuntimeSupport::with_max_command_output_bytes(32 * 1024);
        let thread = ThreadId::new();
        let (_project, authority, _lease) = reserve_test_turn(&runtime, thread);
        let turn = TurnId::new();
        let item_id = ItemId::new();
        let event = runtime.normalize_command_output(AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: item_id,
                harness_item_id: "command".into(),
                payload: ItemPayload::CommandExecution {
                    command: "produce-output".into(),
                    cwd: "/tmp".into(),
                    output: format!("head\n{}\ntail", "🙂".repeat(20_000)),
                    output_truncated: false,
                    output_original_bytes: None,
                    output_original_lines: None,
                    exit_code: Some(0),
                    status: Some("completed".into()),
                    process_id: None,
                    duration_ms: None,
                },
                created_at: Utc::now(),
            },
        });
        runtime.apply_event(&authority, &event, true);
        let RuntimeCommandOutputLookup::Found(output) =
            runtime.command_output(&authority, turn, item_id)
        else {
            panic!("terminal output was not installed");
        };
        assert!(output.output.len() <= 32 * 1024);
        assert!(output.output_truncated);
        assert!(output.original_bytes > output.output.len() as u64);
        assert_eq!(output.version, command_output_version(&output.output));

        runtime.settle_completed_turn(
            &authority,
            &AgentEvent::TurnCompleted {
                thread,
                turn,
                usage: Default::default(),
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
            },
            None,
        );
        assert!(matches!(
            runtime.command_output(&authority, turn, item_id),
            RuntimeCommandOutputLookup::Missing
        ));
    }

    #[test]
    fn tool_output_authority_tracks_terminal_replacements() {
        let runtime = ThreadRuntimeSupport::new();
        let thread = ThreadId::new();
        let (_project, authority, _lease) = reserve_test_turn(&runtime, thread);
        let turn = TurnId::new();
        let item_id = ItemId::new();
        let event =
            |status: Option<&str>, output: Option<serde_json::Value>| AgentEvent::ItemCompleted {
                thread,
                turn,
                item: Item {
                    id: item_id,
                    harness_item_id: "tool".into(),
                    payload: ItemPayload::ToolCall {
                        name: "lookup".into(),
                        input: serde_json::json!({}),
                        output,
                        server: Some("mcp".into()),
                        status: status.map(str::to_owned),
                        metadata: None,
                        subagent: None,
                        error: None,
                    },
                    created_at: Utc::now(),
                },
            };

        runtime.apply_event(
            &authority,
            &event(Some("completed"), Some(serde_json::Value::Null)),
            true,
        );
        let RuntimeToolOutputLookup::Found(first) = runtime.tool_output(&authority, turn, item_id)
        else {
            panic!("terminal tool output was not installed");
        };
        assert_eq!(first.bytes, b"null");

        runtime.apply_event(
            &authority,
            &event(
                Some("IN-PROGRESS"),
                Some(serde_json::json!({"stale": true})),
            ),
            true,
        );
        assert!(matches!(
            runtime.tool_output(&authority, turn, item_id),
            RuntimeToolOutputLookup::Missing
        ));

        runtime.apply_event(
            &authority,
            &event(None, Some(serde_json::json!({"new": true}))),
            true,
        );
        let RuntimeToolOutputLookup::Found(replacement) =
            runtime.tool_output(&authority, turn, item_id)
        else {
            panic!("replacement tool output was not installed");
        };
        assert_ne!(replacement.descriptor.version, first.descriptor.version);
        assert_eq!(replacement.bytes, br#"{"new":true}"#);
    }

    #[test]
    fn inconsistent_truncated_command_metadata_is_not_installed() {
        let runtime = ThreadRuntimeSupport::new();
        let thread = ThreadId::new();
        let (_project, authority, _lease) = reserve_test_turn(&runtime, thread);
        let turn = TurnId::new();
        let item_id = ItemId::new();
        let event = AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: item_id,
                harness_item_id: "bad-command".into(),
                payload: ItemPayload::CommandExecution {
                    command: "bad-output".into(),
                    cwd: "/tmp".into(),
                    output: "retained".into(),
                    output_truncated: true,
                    output_original_bytes: Some(100),
                    output_original_lines: None,
                    exit_code: Some(0),
                    status: Some("completed".into()),
                    process_id: None,
                    duration_ms: None,
                },
                created_at: Utc::now(),
            },
        };

        let mut valid_event = event.clone();
        let AgentEvent::ItemCompleted { item, .. } = &mut valid_event else {
            panic!("expected completed item");
        };
        let ItemPayload::CommandExecution {
            output_truncated,
            output_original_bytes,
            output_original_lines,
            ..
        } = &mut item.payload
        else {
            panic!("expected command payload");
        };
        *output_truncated = false;
        *output_original_bytes = None;
        *output_original_lines = None;
        runtime.apply_event(&authority, &valid_event, true);
        assert!(matches!(
            runtime.command_output(&authority, turn, item_id),
            RuntimeCommandOutputLookup::Found(_)
        ));

        runtime.apply_event(&authority, &event, true);

        assert!(matches!(
            runtime.command_output(&authority, turn, item_id),
            RuntimeCommandOutputLookup::Missing
        ));
    }

    #[test]
    fn stale_prepared_command_cannot_recreate_forgotten_authority() {
        let runtime = ThreadRuntimeSupport::new();
        let thread = ThreadId::new();
        let authority = test_authority(thread);
        let turn = TurnId::new();
        let item_id = ItemId::new();
        let permit = runtime.restoration_permit(&authority);
        let event = AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: item_id,
                harness_item_id: "late-command".into(),
                payload: ItemPayload::CommandExecution {
                    command: "printf late".into(),
                    cwd: "/tmp".into(),
                    output: "late output".into(),
                    output_truncated: false,
                    output_original_bytes: None,
                    output_original_lines: None,
                    exit_code: Some(0),
                    status: Some("completed".into()),
                    process_id: None,
                    duration_ms: None,
                },
                created_at: Utc::now(),
            },
        };
        let (event, prepared) = runtime.prepare_item_output(event);

        runtime.forget_threads(std::slice::from_ref(&authority));

        assert!(
            runtime
                .apply_prepared_event_if_current(&permit, &event, true, prepared)
                .is_none()
        );
        assert!(runtime.existing_entry(&authority).is_none());
        assert!(matches!(
            runtime.command_output(&authority, turn, item_id),
            RuntimeCommandOutputLookup::Missing
        ));
    }

    #[test]
    fn persisted_output_version_cache_is_authority_scoped_and_separate_from_runtime() {
        let runtime = ThreadRuntimeSupport::new();
        let thread = ThreadId::new();
        let (_project, authority, _lease) = reserve_test_turn(&runtime, thread);
        let turn = TurnId::new();
        let item = ItemId::new();
        let permit = runtime.persisted_command_output_version_permit(&authority);
        let persisted = command_output_version("persisted");
        assert_eq!(
            permit.cache(turn, item, persisted.clone()),
            Some(persisted.clone())
        );
        assert_eq!(permit.version(turn, item), Some(persisted));

        let event = runtime.normalize_command_output(AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: item,
                harness_item_id: "runtime-command".into(),
                payload: ItemPayload::CommandExecution {
                    command: "printf runtime".into(),
                    cwd: "/tmp".into(),
                    output: "runtime".into(),
                    output_truncated: false,
                    output_original_bytes: None,
                    output_original_lines: None,
                    exit_code: Some(0),
                    status: Some("completed".into()),
                    process_id: None,
                    duration_ms: None,
                },
                created_at: Utc::now(),
            },
        });
        runtime.apply_event(&authority, &event, true);
        let RuntimeCommandOutputLookup::Found(output) =
            runtime.command_output(&authority, turn, item)
        else {
            panic!("runtime output was not installed");
        };
        assert_eq!(output.version, command_output_version("runtime"));
        assert_eq!(
            permit.version(turn, item),
            Some(command_output_version("persisted"))
        );

        runtime.forget_threads(std::slice::from_ref(&authority));
        assert_eq!(permit.cache(turn, item, "stale".into()), None);
        assert!(runtime.existing_entry(&authority).is_none());
    }
}
