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
use giskard_core::turn::{Mode, Turn};
use giskard_core::user_input::UserInput;
use sha2::{Digest, Sha256};
use tracing::{debug, warn};

use crate::runtime_live::LiveTurnState;
use crate::runtime_tasks::RunningTaskState;
use giskard_proto::{LiveTurnSnapshot, RunningTask};
use giskard_proto::{
    OutstandingRequest, RequestKind, RequestPayload as WireRequestPayload,
    RequestResolution as WireRequestResolution, RequestState as WireRequestState,
    RequestStatus as WireRequestStatus, RuntimeTurnState, ThreadRuntimeOverview,
    ThreadRuntimeSummary, WireApprovalRequest,
};

pub struct ThreadRuntimeRegistry {
    entries: Arc<Mutex<HashMap<ThreadId, Arc<Mutex<ThreadRuntimeEntry>>>>>,
    overview: Arc<Mutex<OverviewState>>,
    max_command_output_bytes: usize,
}

#[derive(Default)]
struct ThreadRuntimeEntry {
    active_turn: Option<ActiveTurnOwner>,
    lifecycle_revision: u64,
    requests: HashMap<RuntimeRequestId, RequestRecord>,
    event_sequence: u64,
    task_revision: u64,
    live: LiveTurnState,
    tasks: RunningTaskState,
    captured_diffs: HashMap<TurnId, ActiveCapturedDiffs>,
    command_outputs: HashMap<(TurnId, ItemId), RuntimeCommandOutput>,
    persisted_command_output_versions: HashMap<(TurnId, ItemId), String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeCommandOutput {
    pub output: String,
    pub output_truncated: bool,
    pub original_bytes: u64,
    pub original_lines: u64,
    pub version: String,
}

pub(crate) enum RuntimeCommandOutputLookup {
    Found(RuntimeCommandOutput),
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
    summaries: HashMap<ThreadId, ThreadRuntimeSummary>,
}

pub struct AppliedRuntimeEvent {
    pub sequence: Option<u64>,
    pub tasks_changed: bool,
    pub running_tasks_if_changed: Option<RunningTasksProjection>,
    pub request_state: Option<WireRequestState>,
    pub overview_if_changed: Option<ThreadRuntimeOverview>,
    overview_refresh_needed: bool,
}

pub struct RunningTasksProjection {
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
    Responding(u64),
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
    registry: ThreadRuntimeRegistry,
    request_id: RuntimeRequestId,
    thread_id: ThreadId,
    claim_id: u64,
    settled: bool,
}

#[derive(Clone)]
pub(crate) struct TurnReservation {
    pub project_id: ProjectId,
    pub harness_thread_id: String,
    pub mode: Mode,
    pub provider: String,
    pub model: String,
    pub context_kind: &'static str,
}

pub(crate) struct ThreadTurnLease {
    registry: ThreadRuntimeRegistry,
    thread_id: ThreadId,
    detached: bool,
}

/// Opaque proof that no newer lifecycle has superseded a delayed thread restore.
pub(crate) struct RestorePermit {
    thread_id: ThreadId,
    entry: std::sync::Weak<Mutex<ThreadRuntimeEntry>>,
    lifecycle_revision: u64,
}

impl ThreadRuntimeRegistry {
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

    /// Extract full diff bodies before an event reaches reconnect state or browser projection.
    pub(crate) fn capture_event_diffs(
        &self,
        thread_id: ThreadId,
        mut event: AgentEvent,
    ) -> AgentEvent {
        let entry = self.entry_or_create(thread_id);
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
        thread_id: ThreadId,
        turn_id: TurnId,
    ) -> Vec<CapturedDiffRecord> {
        let Some(entry) = self.existing_entry(thread_id) else {
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
        thread_id: ThreadId,
        turn_id: TurnId,
        item_id: ItemId,
    ) -> RuntimeCommandOutputLookup {
        let Some(entry) = self.existing_entry(thread_id) else {
            return RuntimeCommandOutputLookup::Missing;
        };
        lock_unpoison(&entry, "thread runtime entry")
            .command_outputs
            .get(&(turn_id, item_id))
            .cloned()
            .map_or(
                RuntimeCommandOutputLookup::Missing,
                RuntimeCommandOutputLookup::Found,
            )
    }

    pub(crate) fn remove_command_output(
        &self,
        thread_id: ThreadId,
        turn_id: TurnId,
        item_id: ItemId,
    ) {
        let Some(entry) = self.existing_entry(thread_id) else {
            return;
        };
        lock_unpoison(&entry, "thread runtime entry")
            .command_outputs
            .remove(&(turn_id, item_id));
    }

    pub(crate) fn persisted_command_output_version_permit(
        &self,
        thread_id: ThreadId,
    ) -> PersistedCommandOutputVersionPermit {
        let entry = self.entry_or_create(thread_id);
        PersistedCommandOutputVersionPermit {
            entry: Arc::downgrade(&entry),
        }
    }

    pub(crate) fn captured_diff(
        &self,
        thread_id: ThreadId,
        turn_id: TurnId,
        diff_id: &DiffId,
    ) -> RuntimeDiffLookup {
        let Some(entry) = self.existing_entry(thread_id) else {
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
    pub fn new() -> Self {
        Self::with_max_command_output_bytes(
            giskard_persist::config::RetentionConfig::DEFAULT_MAX_COMMAND_OUTPUT_BYTES,
        )
    }

    pub fn with_max_command_output_bytes(max_command_output_bytes: usize) -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
            overview: Arc::new(Mutex::new(OverviewState::default())),
            max_command_output_bytes,
        }
    }

    pub(crate) fn restoration_permit(&self, thread_id: ThreadId) -> RestorePermit {
        let entry = self.entry_or_create(thread_id);
        let revision = lock_unpoison(&entry, "thread runtime entry").lifecycle_revision;
        RestorePermit {
            thread_id,
            entry: Arc::downgrade(&entry),
            lifecycle_revision: revision,
        }
    }

    pub(crate) fn restoration_is_current(&self, permit: &RestorePermit) -> bool {
        let Some(expected) = permit.entry.upgrade() else {
            return false;
        };
        let Some(current) = self.existing_entry(permit.thread_id) else {
            return false;
        };
        if !Arc::ptr_eq(&expected, &current) {
            return false;
        }
        let current_revision = lock_unpoison(&current, "thread runtime entry").lifecycle_revision;
        current_revision == permit.lifecycle_revision
    }

    pub fn live_is_active(&self, thread_id: ThreadId) -> bool {
        let Some(entry) = self.existing_entry(thread_id) else {
            return false;
        };
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry.live.is_active(thread_id)
    }

    pub fn live_snapshot(&self, thread_id: ThreadId) -> Option<LiveTurnSnapshot> {
        let entry = self.existing_entry(thread_id)?;
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry.live.snapshot(thread_id)
    }

    pub(crate) fn live_item_events(&self, thread_id: ThreadId, item_id: ItemId) -> Vec<AgentEvent> {
        let Some(entry) = self.existing_entry(thread_id) else {
            return Vec::new();
        };
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry.live.item_events(thread_id, item_id)
    }

    pub(crate) fn ensure_live_turn(
        &self,
        thread_id: ThreadId,
        turn_id: TurnId,
        user_input: Option<UserInput>,
    ) -> Result<(), TurnId> {
        let entry = self.entry_or_create(thread_id);
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        entry
            .live
            .ensure_turn_with_user_input(thread_id, turn_id, user_input)
    }

    pub fn replace_live_turn(
        &self,
        thread_id: ThreadId,
        turn_id: TurnId,
        user_input: Option<UserInput>,
    ) {
        let entry = self.entry_or_create(thread_id);
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        entry
            .live
            .replace_turn_with_user_input(thread_id, turn_id, user_input);
    }

    pub(crate) fn resolve_live_approval(
        &self,
        thread_id: ThreadId,
        approval_id: ApprovalId,
        decision: ApprovalDecision,
    ) {
        let Some(entry) = self.existing_entry(thread_id) else {
            return;
        };
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        entry
            .live
            .resolve_approval(thread_id, approval_id, decision);
    }

    pub(crate) fn resolve_live_server_request(
        &self,
        thread_id: ThreadId,
        request_id: ServerRequestId,
    ) {
        let Some(entry) = self.existing_entry(thread_id) else {
            return;
        };
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        entry.live.resolve_server_request(thread_id, request_id);
    }

    pub fn tasks_snapshot(&self, thread_id: ThreadId) -> (u64, Vec<RunningTask>) {
        let Some(entry) = self.existing_entry(thread_id) else {
            return (0, Vec::new());
        };
        let entry = lock_unpoison(&entry, "thread runtime entry");
        let tasks = entry.tasks.snapshot(thread_id);
        (entry.task_revision, tasks)
    }

    pub(crate) fn has_running_for_turn(&self, thread_id: ThreadId, turn_id: TurnId) -> bool {
        let Some(entry) = self.existing_entry(thread_id) else {
            return false;
        };
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry.tasks.has_running_for_turn(thread_id, turn_id)
    }

    pub(crate) fn has_running_for_thread(&self, thread_id: ThreadId) -> bool {
        let Some(entry) = self.existing_entry(thread_id) else {
            return false;
        };
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry.tasks.has_running_for_thread(thread_id)
    }

    pub(crate) fn task_by_process(
        &self,
        thread_id: ThreadId,
        process_id: &str,
    ) -> Option<RunningTask> {
        let entry = self.existing_entry(thread_id)?;
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry.tasks.get_by_process(thread_id, process_id)
    }

    pub(crate) fn task_by_item(
        &self,
        thread_id: ThreadId,
        turn_id: TurnId,
        item_id: ItemId,
    ) -> Option<RunningTask> {
        let entry = self.existing_entry(thread_id)?;
        let entry = lock_unpoison(&entry, "thread runtime entry");
        entry.tasks.get_by_item(thread_id, turn_id, item_id)
    }

    pub(crate) fn set_task_terminating(
        &self,
        thread_id: ThreadId,
        process_id: &str,
        terminating: bool,
    ) -> bool {
        let Some(entry) = self.existing_entry(thread_id) else {
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

    pub(crate) fn remove_task_by_process(&self, thread_id: ThreadId, process_id: &str) -> bool {
        let Some(entry) = self.existing_entry(thread_id) else {
            return false;
        };
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        let changed = entry.tasks.remove_by_process(thread_id, process_id);
        if changed {
            entry.task_revision = entry.task_revision.saturating_add(1);
        }
        changed
    }

    pub fn apply_event(
        &self,
        thread_id: ThreadId,
        event: &AgentEvent,
        append_live: bool,
    ) -> AppliedRuntimeEvent {
        let event_thread_id = event.thread_id();
        if event_thread_id != thread_id {
            warn!(
                %thread_id,
                %event_thread_id,
                "refusing to apply a foreign-thread event to runtime state"
            );
            return AppliedRuntimeEvent::unchanged();
        }
        let entry = self.entry_or_create(thread_id);
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        let mut applied = self.apply_event_locked(thread_id, event, append_live, &mut entry);
        if applied.overview_refresh_needed {
            applied.overview_if_changed = self.refresh_overview(thread_id, &entry);
        }
        applied
    }

    fn apply_event_locked(
        &self,
        thread_id: ThreadId,
        event: &AgentEvent,
        append_live: bool,
        entry: &mut ThreadRuntimeEntry,
    ) -> AppliedRuntimeEvent {
        let sequence = (!matches!(
            event,
            AgentEvent::ThreadOpened { .. }
                | AgentEvent::DiffUpdated { .. }
                | AgentEvent::ContextWindowUpdated { .. }
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
            update_command_output_authority(entry, *turn, item);
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
        thread_id: ThreadId,
        event: &AgentEvent,
        persisted_turn: Option<(Turn, String)>,
    ) -> AppliedRuntimeEvent {
        let entry = self.entry_or_create(thread_id);
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        let mut applied = self.apply_event_locked(thread_id, event, true, &mut entry);
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
                }
                entry.requests.retain(|_, record| {
                    !(matches!(record.status, RequestStatus::Resolved(_))
                        && record.turn_id == completed_turn)
                });
                if let Some(owner) = entry.active_turn.take() {
                    debug!(
                        %thread_id,
                        project_id = %owner.reservation.project_id,
                        turn_id = ?owner.acknowledged_turn,
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
        thread_id: ThreadId,
        reservation: TurnReservation,
    ) -> Result<ThreadTurnLease, HarnessError> {
        let entry = self.entry_or_create(thread_id);
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        if let Some(existing) = &entry.active_turn {
            warn!(
                %thread_id,
                owner_project_id = %existing.reservation.project_id,
                owner_turn_id = ?existing.acknowledged_turn,
                owner_harness_thread_id = %existing.reservation.harness_thread_id,
                owner_context_kind = existing.reservation.context_kind,
                owner_mode = ?existing.reservation.mode,
                owner_provider = %existing.reservation.provider,
                owner_model = %existing.reservation.model,
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
            registry: self.clone(),
            thread_id,
            detached: false,
        })
    }

    pub(crate) fn has_active_turn(&self, thread_id: ThreadId) -> bool {
        let Some(entry) = self.existing_entry(thread_id) else {
            return false;
        };
        lock_unpoison(&entry, "thread runtime entry")
            .active_turn
            .is_some()
    }

    fn acknowledge_turn(
        &self,
        thread_id: ThreadId,
        turn_id: TurnId,
    ) -> Option<ThreadRuntimeOverview> {
        let entry = self.entry_or_create(thread_id);
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        let Some(owner) = entry.active_turn.as_mut() else {
            warn!(%thread_id, %turn_id, "turn acknowledgement has no runtime owner");
            return None;
        };
        owner.acknowledged_turn = Some(turn_id);
        self.refresh_overview(thread_id, &entry)
    }

    fn release_turn(&self, thread_id: ThreadId) -> Option<ThreadRuntimeOverview> {
        let entry = self.existing_entry(thread_id)?;
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        if let Some(owner) = entry.active_turn.take() {
            debug!(
                %thread_id,
                project_id = %owner.reservation.project_id,
                turn_id = ?owner.acknowledged_turn,
                elapsed_ms = owner.reserved_at.elapsed().as_millis(),
                "released active thread runtime"
            );
        }
        self.refresh_overview(thread_id, &entry)
    }

    #[cfg(test)]
    pub(crate) fn register_approval(&self, thread_id: ThreadId, request: ApprovalRequest) {
        let entry = self.entry_or_create(thread_id);
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
        thread_id: ThreadId,
        request_id: RuntimeRequestId,
    ) -> Result<(RequestClaim, RequestTransition), HarnessError> {
        let entry = self.entry_or_create(thread_id);
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
        record.status = RequestStatus::Responding(claim_id);
        record.revision = record.revision.saturating_add(1);
        let transition = RequestTransition {
            request_state: wire_request_state(thread_id, record),
            overview_if_changed: self.refresh_overview(thread_id, &entry),
        };
        Ok((
            RequestClaim {
                registry: self.clone(),
                request_id,
                thread_id,
                claim_id,
                settled: false,
            },
            transition,
        ))
    }

    pub(crate) fn forget_threads(&self, thread_ids: &std::collections::HashSet<ThreadId>) {
        for thread_id in thread_ids {
            let mut entries = lock_unpoison(&self.entries, "thread runtime entry registry");
            entries.remove(thread_id);
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
        thread_id: ThreadId,
        request_id: &RuntimeRequestId,
    ) -> Option<WireRequestState> {
        let entry = self.existing_entry(thread_id)?;
        lock_unpoison(&entry, "thread runtime entry")
            .requests
            .get(request_id)
            .map(|record| wire_request_state(thread_id, record))
    }

    pub(crate) fn request_states(&self, thread_id: ThreadId) -> Vec<WireRequestState> {
        let Some(entry) = self.existing_entry(thread_id) else {
            return Vec::new();
        };
        lock_unpoison(&entry, "thread runtime entry")
            .requests
            .values()
            .map(|record| wire_request_state(thread_id, record))
            .collect()
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

    fn entry_or_create(&self, thread_id: ThreadId) -> Arc<Mutex<ThreadRuntimeEntry>> {
        let mut entries = lock_unpoison(&self.entries, "thread runtime entry registry");
        entries
            .entry(thread_id)
            .or_insert_with(|| Arc::new(Mutex::new(ThreadRuntimeEntry::default())))
            .clone()
    }

    fn existing_entry(&self, thread_id: ThreadId) -> Option<Arc<Mutex<ThreadRuntimeEntry>>> {
        lock_unpoison(&self.entries, "thread runtime entry registry")
            .get(&thread_id)
            .cloned()
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

impl Clone for ThreadRuntimeRegistry {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
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

pub(crate) fn command_output_version(output: &str) -> String {
    format!("\"sha256_{:x}\"", Sha256::digest(output.as_bytes()))
}

impl ThreadTurnLease {
    /// Adopt the harness's turn id. The overview it returns is a changed replacement projection:
    /// the caller must publish it, or connected clients keep an overview this transition
    /// superseded.
    #[must_use = "an acknowledged turn changes the runtime overview; publish it"]
    pub(crate) fn acknowledge_turn(&mut self, turn_id: TurnId) -> Option<ThreadRuntimeOverview> {
        if self.detached {
            return None;
        }
        self.registry.acknowledge_turn(self.thread_id, turn_id)
    }

    pub(crate) fn release(&mut self) -> Option<ThreadRuntimeOverview> {
        if self.detached {
            return None;
        }
        let overview = self.registry.release_turn(self.thread_id);
        self.detached = true;
        overview
    }

    pub(crate) fn is_released(&self) -> bool {
        self.detached
    }

    pub(crate) fn commit_after_persistence(&mut self, event: &AgentEvent) -> AppliedRuntimeEvent {
        let applied = self
            .registry
            .settle_completed_turn(self.thread_id, event, None);
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
            self.registry
                .settle_completed_turn(self.thread_id, event, Some((turn, error)));
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
    if matches!(record.status, RequestStatus::Resolved(_)) {
        return false;
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
            let responding = matches!(record.status, RequestStatus::Responding(_));
            matches!(
                record.status,
                RequestStatus::Pending | RequestStatus::Responding(_)
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
        RequestStatus::Responding(_) => WireRequestStatus::Responding,
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
    pub(crate) fn commit(
        mut self,
        resolution: RequestResolution,
    ) -> Result<RequestTransition, Box<RequestCommitError>> {
        let Some(entry) = self.registry.existing_entry(self.thread_id) else {
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
        if record.status != RequestStatus::Responding(self.claim_id) {
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
            overview_if_changed: self.registry.refresh_overview(self.thread_id, &entry),
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
        let Some(entry) = self.registry.existing_entry(self.thread_id) else {
            self.settled = true;
            return None;
        };
        let mut entry = lock_unpoison(&entry, "thread runtime entry");
        if let Some(record) = entry.requests.get_mut(&self.request_id)
            && record.status == RequestStatus::Responding(self.claim_id)
        {
            record.status = RequestStatus::Pending;
            record.revision = record.revision.saturating_add(1);
            let transition = RequestTransition {
                request_state: wire_request_state(self.thread_id, record),
                overview_if_changed: self.registry.refresh_overview(self.thread_id, &entry),
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
            warn!(
                thread_id = %self.thread_id,
                request_id = self.request_id.as_str(),
                revision = transition.request_state.revision,
                "request claim dropped without settlement; rolled request back to pending"
            );
        }
    }
}

fn next_claim_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed).max(1)
}

impl Default for ThreadRuntimeRegistry {
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
    use giskard_core::turn::{TurnStatus, TurnStatusKind};

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

    #[test]
    fn replacing_active_diff_returns_conflict_for_immediately_previous_identity() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
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

        let first = runtime.capture_event_diffs(thread, make_event("first"));
        let first_id = match first {
            AgentEvent::DiffUpdated { diff, .. } => diff.captured.unwrap().id,
            _ => panic!("expected diff event"),
        };
        let second = runtime.capture_event_diffs(thread, make_event("second"));
        let second_id = match second {
            AgentEvent::DiffUpdated { diff, .. } => diff.captured.unwrap().id,
            _ => panic!("expected diff event"),
        };

        assert!(matches!(
            runtime.captured_diff(thread, turn, &first_id),
            RuntimeDiffLookup::Superseded(current) if current.id == second_id
        ));
        assert!(matches!(
            runtime.captured_diff(thread, turn, &second_id),
            RuntimeDiffLookup::Found(_)
        ));
        let repeated = runtime.capture_event_diffs(thread, make_event("second"));
        let repeated_id = match repeated {
            AgentEvent::DiffUpdated { diff, .. } => diff.captured.unwrap().id,
            _ => panic!("expected diff event"),
        };
        assert_eq!(
            second_id, repeated_id,
            "identical content reuses its hash id"
        );
        let entry = runtime.existing_entry(thread).unwrap();
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
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
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

        let captured = runtime.capture_event_diffs(thread, event);
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
            runtime.captured_diff(thread, turn, &descriptor.id),
            RuntimeDiffLookup::Found(CapturedDiffRecord {
                content: giskard_core::CapturedDiffContent::Unified { text },
                ..
            }) if text.is_empty()
        ));
    }

    #[test]
    fn item_diff_reconciliation_treats_each_completion_as_a_complete_set() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
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
            match runtime.capture_event_diffs(thread, event) {
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
        assert_eq!(runtime.captured_diff_records(thread, turn).len(), 3);
        for descriptor in &first {
            assert!(matches!(
                runtime.captured_diff(thread, turn, &descriptor.id),
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
        assert_eq!(runtime.captured_diff_records(thread, turn).len(), 3);
        for descriptor in &second {
            assert!(matches!(
                runtime.captured_diff(thread, turn, &descriptor.id),
                RuntimeDiffLookup::Found(_)
            ));
        }

        // Omitting one duplicate and renaming another path retires those slots instead of keeping
        // obsolete bodies alive. Repeating an unchanged body preserves its stable identity.
        let third = capture(vec![("src/a.rs", "a0-next"), ("src/c.rs", "c0")]);
        assert_eq!(third[0].id, second[1].id);
        assert_eq!(runtime.captured_diff_records(thread, turn).len(), 2);
        assert!(matches!(
            runtime.captured_diff(thread, turn, &second[0].id),
            RuntimeDiffLookup::Missing
        ));
        assert!(matches!(
            runtime.captured_diff(thread, turn, &second[2].id),
            RuntimeDiffLookup::Missing
        ));
        let entry = runtime.existing_entry(thread).unwrap();
        let entry = lock_unpoison(&entry, "thread runtime entry");
        let state = &entry.captured_diffs[&turn];
        assert_eq!(state.current_by_slot.len(), 2);
        assert_eq!(state.contents.len(), 2);
        drop(entry);

        runtime.capture_event_diffs(
            thread,
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
        assert!(runtime.captured_diff_records(thread, turn).is_empty());
    }

    #[test]
    fn read_only_queries_do_not_create_runtime_entries() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread_id = ThreadId::new();

        assert!(!runtime.live_is_active(thread_id));
        assert!(runtime.live_snapshot(thread_id).is_none());
        assert!(
            runtime
                .live_item_events(thread_id, ItemId::new())
                .is_empty()
        );
        assert_eq!(runtime.tasks_snapshot(thread_id), (0, Vec::new()));
        assert!(!runtime.has_running_for_thread(thread_id));
        assert!(runtime.request_states(thread_id).is_empty());
        assert!(
            lock_unpoison(&runtime.entries, "thread runtime entry registry").is_empty(),
            "querying absent state must not allocate an entry"
        );
    }

    #[test]
    fn foreign_thread_event_is_rejected_before_mutation() {
        let runtime = ThreadRuntimeRegistry::new();
        let target_thread = ThreadId::new();
        let event_thread = ThreadId::new();
        let request = approval("foreign");

        let applied = runtime.apply_event(
            target_thread,
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
        assert!(runtime.request_states(target_thread).is_empty());
        assert!(runtime.request_states(event_thread).is_empty());
        assert!(
            lock_unpoison(&runtime.entries, "thread runtime entry registry").is_empty(),
            "a rejected event must not allocate either thread entry"
        );
    }

    #[test]
    fn requests_are_claimed_independently_and_failed_claims_roll_back() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread_id = ThreadId::new();
        runtime.register_approval(thread_id, approval("a"));
        runtime.register_approval(thread_id, approval("b"));
        assert_eq!(runtime.request_states(thread_id).len(), 2);

        let (claim_a, _) = runtime
            .claim_request(
                thread_id,
                RuntimeRequestId::Approval(ApprovalId("a".into())),
            )
            .unwrap();
        assert_eq!(
            runtime
                .request_state(
                    thread_id,
                    &RuntimeRequestId::Approval(ApprovalId("a".into()))
                )
                .unwrap()
                .revision,
            2
        );
        let (claim_b, _) = runtime
            .claim_request(
                thread_id,
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
                    thread_id,
                    &RuntimeRequestId::Approval(ApprovalId("a".into()))
                )
                .unwrap()
                .revision,
            3,
            "rolling a failed claim back is an ordered state transition"
        );

        let states = runtime.request_states(thread_id);
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
                    thread_id,
                    RuntimeRequestId::Approval(ApprovalId("a".into()))
                )
                .is_ok()
        );
        assert!(
            runtime
                .claim_request(
                    thread_id,
                    RuntimeRequestId::Approval(ApprovalId("b".into()))
                )
                .is_err()
        );
    }

    #[test]
    fn failed_commit_returns_the_authoritative_rollback_transition() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread_id = ThreadId::new();
        runtime.register_approval(thread_id, approval("mismatched"));
        let (claim, _) = runtime
            .claim_request(
                thread_id,
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
        let runtime = ThreadRuntimeRegistry::new();
        let thread_id = ThreadId::new();
        let turn_id = TurnId::new();
        let request = approval("duplicate");
        runtime.apply_event(
            thread_id,
            &AgentEvent::ApprovalRequested {
                thread: thread_id,
                turn: turn_id,
                request: request.clone(),
            },
            false,
        );
        runtime
            .claim_request(thread_id, RuntimeRequestId::Approval(request.id.clone()))
            .unwrap()
            .0
            .commit(RequestResolution::Approval(ApprovalDecision::Accept))
            .unwrap();

        let duplicate = runtime.apply_event(
            thread_id,
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
            .request_state(thread_id, &RuntimeRequestId::Approval(request.id))
            .expect("the resolved record survives a duplicate delivery");
        assert_eq!(state.revision, 3);
        assert!(matches!(state.status, WireRequestStatus::Resolved { .. }));
    }

    #[tokio::test]
    async fn duplicate_request_event_with_new_metadata_takes_a_new_revision() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread_id = ThreadId::new();
        let turn_id = TurnId::new();
        let request = approval("refreshed");
        runtime.apply_event(
            thread_id,
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
            thread_id,
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
        let runtime = ThreadRuntimeRegistry::new();
        let thread_id = ThreadId::new();
        let turn_id = TurnId::new();
        let mut lease = runtime
            .reserve_turn(
                thread_id,
                TurnReservation {
                    project_id: ProjectId::new(),
                    harness_thread_id: "native".into(),
                    mode: Mode::Build,
                    provider: "provider".into(),
                    model: "model".into(),
                    context_kind: "user",
                },
            )
            .unwrap();
        let initial_revision = runtime.current_overview().revision;

        let notice = runtime.apply_event(
            thread_id,
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
            thread_id,
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
            thread_id,
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
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let permit = runtime.restoration_permit(thread);
        assert!(runtime.restoration_is_current(&permit));
        let _lease = runtime
            .reserve_turn(
                thread,
                TurnReservation {
                    project_id: ProjectId::new(),
                    harness_thread_id: "native".into(),
                    mode: Mode::Build,
                    provider: "provider".into(),
                    model: "model".into(),
                    context_kind: "test",
                },
            )
            .unwrap();
        assert!(!runtime.restoration_is_current(&permit));
    }

    #[tokio::test]
    async fn restore_permit_does_not_survive_forget_and_recreate() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let permit = runtime.restoration_permit(thread);
        runtime.forget_threads(&std::collections::HashSet::from([thread]));
        let replacement = runtime.restoration_permit(thread);
        assert!(!runtime.restoration_is_current(&permit));
        assert!(runtime.restoration_is_current(&replacement));
    }

    #[tokio::test]
    async fn persisted_completion_releases_lease_and_prunes_resolved_turn_requests() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread_id = ThreadId::new();
        let turn_id = TurnId::new();
        let mut lease = runtime
            .reserve_turn(
                thread_id,
                TurnReservation {
                    project_id: ProjectId::new(),
                    harness_thread_id: "native".into(),
                    mode: Mode::Build,
                    provider: "provider".into(),
                    model: "model".into(),
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
            thread_id,
            &AgentEvent::ApprovalRequested {
                thread: thread_id,
                turn: turn_id,
                request: request.clone(),
            },
            false,
        );
        runtime
            .claim_request(thread_id, RuntimeRequestId::Approval(request.id))
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
        assert!(!runtime.has_active_turn(thread_id));
        assert!(runtime.request_states(thread_id).is_empty());
        assert!(runtime.current_overview().threads.is_empty());
    }

    #[test]
    fn claim_validates_the_thread_identity() {
        let runtime = ThreadRuntimeRegistry::new();
        let owner = ThreadId::new();
        runtime.register_approval(owner, approval("a"));
        let result = runtime.claim_request(
            ThreadId::new(),
            RuntimeRequestId::Approval(ApprovalId("a".into())),
        );
        assert!(matches!(result, Err(error) if error.to_string().contains("no pending request")));
    }

    #[tokio::test]
    async fn empty_overview_replaces_the_last_active_summary() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread_id = ThreadId::new();
        runtime.register_approval(thread_id, approval("a"));
        assert_eq!(runtime.current_overview().threads.len(), 1);
        runtime.forget_threads(&std::collections::HashSet::from([thread_id]));
        assert!(runtime.current_overview().threads.is_empty());
    }

    #[tokio::test]
    async fn explicit_lease_release_returns_the_empty_overview_effect() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread_id = ThreadId::new();
        let mut lease = runtime
            .reserve_turn(
                thread_id,
                TurnReservation {
                    project_id: ProjectId::new(),
                    harness_thread_id: "native".into(),
                    mode: Mode::Build,
                    provider: "provider".into(),
                    model: "model".into(),
                    context_kind: "user",
                },
            )
            .unwrap();

        let overview = lease.release().expect("release changes the overview");
        assert!(overview.threads.is_empty());
        assert!(!runtime.has_active_turn(thread_id));
        assert!(lease.release().is_none());
    }

    #[tokio::test]
    async fn persistence_failure_keeps_the_complete_turn_and_lease() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread_id = ThreadId::new();
        let reservation = TurnReservation {
            project_id: ProjectId::new(),
            harness_thread_id: "native".into(),
            mode: Mode::Build,
            provider: "provider".into(),
            model: "model".into(),
            context_kind: "user",
        };
        let mut lease = runtime
            .reserve_turn(thread_id, reservation.clone())
            .unwrap();
        let turn = Turn {
            id: TurnId::new(),
            user_input: UserInput::text("keep me"),
            items: Vec::new(),
            model: ModelRef {
                provider: "provider".into(),
                model: "model".into(),
                reasoning_effort: None,
            },
            mode: Mode::Build,
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
            usage: Default::default(),
            diffs: Vec::new(),
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
        };
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
        assert!(runtime.reserve_turn(thread_id, reservation).is_err());
        let entry = runtime.existing_entry(thread_id).unwrap();
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
        let runtime = ThreadRuntimeRegistry::with_max_command_output_bytes(32 * 1024);
        let thread = ThreadId::new();
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
        runtime.apply_event(thread, &event, true);
        let RuntimeCommandOutputLookup::Found(output) =
            runtime.command_output(thread, turn, item_id)
        else {
            panic!("terminal output was not installed");
        };
        assert!(output.output.len() <= 32 * 1024);
        assert!(output.output_truncated);
        assert!(output.original_bytes > output.output.len() as u64);
        assert_eq!(output.version, command_output_version(&output.output));

        runtime.settle_completed_turn(
            thread,
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
            runtime.command_output(thread, turn, item_id),
            RuntimeCommandOutputLookup::Missing
        ));
    }

    #[test]
    fn persisted_output_version_cache_is_authority_scoped_and_separate_from_runtime() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item = ItemId::new();
        let permit = runtime.persisted_command_output_version_permit(thread);
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
        runtime.apply_event(thread, &event, true);
        let RuntimeCommandOutputLookup::Found(output) = runtime.command_output(thread, turn, item)
        else {
            panic!("runtime output was not installed");
        };
        assert_eq!(output.version, command_output_version("runtime"));
        assert_eq!(
            permit.version(turn, item),
            Some(command_output_version("persisted"))
        );

        runtime.forget_threads(&std::collections::HashSet::from([thread]));
        assert_eq!(permit.cache(turn, item, "stale".into()), None);
        assert!(runtime.existing_entry(thread).is_none());
    }
}
