//! Process-local authority for a thread while the server is running.
//!
//! M2 moves turn ownership, reconnect state, tasks, and requests behind this object. Callers use
//! narrow projections and transitions rather than coordinating the underlying state independently.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use giskard_core::approval::{ApprovalDecision, ApprovalRequest};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::ItemId;
use giskard_core::ids::{ApprovalId, ProjectId, ServerRequestId, ThreadId, TurnId};
use giskard_core::server_request::{ServerRequest, ServerRequestResponse};
use giskard_core::turn::{Mode, Turn};
use giskard_core::user_input::UserInput;
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
}

#[derive(Default)]
struct ThreadRuntimeEntry {
    active_turn: Option<ActiveTurnOwner>,
    requests: HashMap<RuntimeRequestId, RequestRecord>,
    event_sequence: u64,
    task_revision: u64,
    live: LiveTurnState,
    tasks: RunningTaskState,
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

impl ThreadRuntimeRegistry {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
            overview: Arc::new(Mutex::new(OverviewState::default())),
        }
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
        let mut entries = lock_unpoison(&self.entries, "thread runtime entry registry");
        for thread_id in thread_ids {
            let entry = entries.get(thread_id).cloned();
            let _entry = entry
                .as_ref()
                .map(|entry| lock_unpoison(entry, "thread runtime entry"));
            entries.remove(thread_id);
        }
        drop(entries);
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
        }
    }
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

    #[test]
    fn event_application_refreshes_the_overview_only_for_summary_changes() {
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

    #[test]
    fn empty_overview_replaces_the_last_active_summary() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread_id = ThreadId::new();
        runtime.register_approval(thread_id, approval("a"));
        assert_eq!(runtime.current_overview().threads.len(), 1);
        runtime.forget_threads(&std::collections::HashSet::from([thread_id]));
        assert!(runtime.current_overview().threads.is_empty());
    }

    #[test]
    fn explicit_lease_release_returns_the_empty_overview_effect() {
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

    #[test]
    fn persistence_failure_keeps_the_complete_turn_and_lease() {
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
}
