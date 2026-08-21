//! Authoritative process-local state for one thread's active runtime.
//!
//! This module deliberately keeps turn ownership, reconnect state, tasks, requests, and the
//! ordered event journal behind one lock. An agent event is applied once and produces immutable
//! effects which callers may publish after the lock is released.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Instant;

use chrono::Utc;
use indexmap::IndexMap;
use tokio::sync::watch;
use tracing::{debug, error, warn};

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ProjectId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{ItemDelta, ItemPayload, command_status_is_running};
use giskard_core::model::ModelRef;
use giskard_core::server_request::{ServerRequest, ServerRequestResponse};
use giskard_core::turn::{Mode, Turn};
use giskard_core::user_input::UserInput;
use giskard_proto::{
    LiveTurnProjection, OutstandingRequest, RequestKind, RequestPayload, RequestResolution,
    RequestState, RequestStatus, RunningTask, RuntimeTurnState, SequencedThreadEvent, TaskKind,
    ThreadEventPayload, ThreadFinalRuntime, ThreadRuntimeOverview, ThreadRuntimeSummary,
    ThreadTaskSnapshot, WireAgentEvent, WireApprovalRequest,
};

const DEFAULT_JOURNAL_ENTRIES: usize = 4_096;
const DEFAULT_JOURNAL_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_PIN_SUFFIX_ENTRIES: usize = 2_048;
const DEFAULT_PIN_SUFFIX_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_PIN_RESERVATION_ENTRIES: usize = 8_192;
const DEFAULT_PIN_RESERVATION_BYTES: usize = 16 * 1024 * 1024;
const MAX_LIVE_COMMAND_OUTPUT: usize = 16 * 1024;
const LIVE_COMMAND_OUTPUT_EDGE: usize = 8 * 1024;
const MAX_LIVE_TEXT: usize = 64 * 1024;
const LIVE_TEXT_EDGE: usize = 32 * 1024;
const MAX_TASK_OUTPUT_TAIL: usize = 8_000;
const LIVE_COMMAND_OUTPUT_TRUNCATED: &str = "\n\n[... command output omitted from reconnect state; full output remains authoritative on completion ...]\n\n";
const LIVE_TEXT_TRUNCATED: &str = "\n\n[... streamed text omitted from reconnect state; completed output remains authoritative ...]\n\n";

type TaskKey = (TurnId, ItemId);

#[derive(Clone, Copy, Debug)]
pub struct RuntimeLimits {
    pub journal_entries: usize,
    pub journal_bytes: usize,
    pub pin_suffix_entries: usize,
    pub pin_suffix_bytes: usize,
    pub total_pin_reservation_entries: usize,
    pub total_pin_reservation_bytes: usize,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            journal_entries: DEFAULT_JOURNAL_ENTRIES,
            journal_bytes: DEFAULT_JOURNAL_BYTES,
            pin_suffix_entries: DEFAULT_PIN_SUFFIX_ENTRIES,
            pin_suffix_bytes: DEFAULT_PIN_SUFFIX_BYTES,
            total_pin_reservation_entries: DEFAULT_PIN_RESERVATION_ENTRIES,
            total_pin_reservation_bytes: DEFAULT_PIN_RESERVATION_BYTES,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TurnOwner {
    pub project_id: ProjectId,
    pub harness_thread_id: String,
    pub mode: Mode,
    pub model: ModelRef,
    pub context_kind: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistenceBlockedState {
    pub turn: Turn,
    pub attempts: u32,
    pub error: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RuntimeRequestKind {
    Approval,
    Server,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RuntimeRequestId {
    Approval(ApprovalId),
    Server(ServerRequestId),
}

impl RuntimeRequestId {
    pub fn kind(&self) -> RuntimeRequestKind {
        match self {
            Self::Approval(_) => RuntimeRequestKind::Approval,
            Self::Server(_) => RuntimeRequestKind::Server,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Approval(id) => &id.0,
            Self::Server(id) => &id.0,
        }
    }
}

#[derive(Clone, Debug)]
pub enum RuntimeRequestPayload {
    Approval(WireApprovalRequest),
    Server(ServerRequest),
}

#[derive(Clone, Debug, PartialEq)]
pub enum RuntimeRequestResolution {
    Approval(ApprovalDecision),
    Server(ServerRequestResponse),
    Harness,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RuntimeRequestStatus {
    Pending,
    Responding,
    Resolved(RuntimeRequestResolution),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JournalCoverage {
    Turn(TurnId),
}

#[derive(Clone, Debug)]
pub struct SequencedAgentEvent {
    pub event: SequencedThreadEvent,
    pub coverage: Option<JournalCoverage>,
    pub encoded_bytes: usize,
}

impl SequencedAgentEvent {
    pub fn seq(&self) -> u64 {
        self.event.seq
    }
}

#[derive(Clone, Debug)]
pub struct AppliedRuntimeEvent {
    pub stream_event: Option<SequencedAgentEvent>,
    pub running_tasks_if_changed: Option<ThreadTaskSnapshot>,
    pub requests_changed: bool,
}

#[derive(Clone, Debug)]
pub struct BootstrapRuntimeCut {
    pub through_seq: u64,
    pub suffix: Vec<SequencedAgentEvent>,
    pub final_runtime: ThreadFinalRuntime,
}

pub struct LiveRuntimeCut {
    pub live: Option<LiveTurnProjection>,
    pub represented_through: u64,
    pub pin: JournalPin,
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeApplyError {
    #[error("event belongs to thread {actual}, not {expected}")]
    ForeignThread {
        expected: ThreadId,
        actual: ThreadId,
    },
    #[error("runtime event sequence is exhausted for thread {0}")]
    SequenceExhausted(ThreadId),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum JournalPinError {
    #[error("journal pin reservation capacity is exhausted")]
    ReservationCapacity,
    #[error("journal suffix already exceeds the requested pin budget")]
    SuffixTooLarge,
    #[error("journal suffix is no longer available at the requested watermark")]
    SuffixUnavailable,
    #[error("journal pin is no longer active")]
    Released,
    #[error("journal pin suffix exceeded its reserved budget")]
    Exhausted,
    #[error("bootstrap subscription was cancelled before the runtime cut committed")]
    PreparationRejected,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RequestClaimError {
    #[error("request was not found")]
    Missing,
    #[error("request is already being answered")]
    Responding,
    #[error("request is already resolved")]
    Resolved,
    #[error("request claim is stale")]
    StaleClaim,
    #[error("request event sequence is exhausted")]
    SequenceExhausted,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PersistenceTransitionError {
    #[error("thread has no active turn owner")]
    MissingOwner,
    #[error("thread has no retained persistence failure")]
    MissingBlocked,
    #[error("turn lease is stale")]
    StaleLease,
    #[error("thread already has a retained persistence failure")]
    AlreadyBlocked,
    #[error("retained turn does not match the active turn owner")]
    TurnMismatch,
}

#[derive(Clone)]
pub struct ThreadRuntimeRegistry {
    inner: Arc<RuntimeInner>,
}

struct RuntimeInner {
    state: Mutex<RegistryState>,
    overview_tx: watch::Sender<ThreadRuntimeOverview>,
    limits: RuntimeLimits,
}

struct RegistryState {
    threads: HashMap<ThreadId, ThreadRuntime>,
    next_lease_id: u64,
    next_claim_id: u64,
    overview_revision: u64,
    overview_threads: Vec<OverviewKey>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OverviewKey {
    thread_id: ThreadId,
    blocked: Option<BlockedOverview>,
    active: bool,
    turn_id: Option<TurnId>,
    requests: Vec<(RuntimeRequestKind, String, bool)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BlockedOverview {
    turn_id: TurnId,
    attempts: u32,
    error: String,
}

struct ThreadRuntime {
    owner: Option<TurnLeaseState>,
    blocked: Option<PersistenceBlockedState>,
    live: Option<LiveTurn>,
    tasks: HashMap<TaskKey, RunningTask>,
    task_revision: u64,
    requests: IndexMap<RuntimeRequestId, RequestRecord>,
    resolved_requests: HashMap<RuntimeRequestId, RuntimeRequestResolution>,
    journal: EventJournal,
}

struct TurnLeaseState {
    lease_id: u64,
    owner: TurnOwner,
    acknowledged_turn: Option<TurnId>,
    reserved_at: Instant,
}

struct LiveTurn {
    turn_id: TurnId,
    user_input: Option<UserInput>,
    events: Vec<AgentEvent>,
    represented_through: u64,
}

struct RequestRecord {
    payload: RuntimeRequestPayload,
    status: RuntimeRequestStatus,
    claim_id: Option<u64>,
}

struct EventJournal {
    entries: VecDeque<SequencedAgentEvent>,
    bytes: usize,
    next_seq: u64,
    next_pin_id: u64,
    pins: HashMap<u64, PinState>,
}

struct PinState {
    after_seq: u64,
    max_entries: usize,
    max_bytes: usize,
    retained_entries: usize,
    retained_bytes: usize,
    exhausted: bool,
}

pub(crate) struct ThreadTurnLease {
    registry: Weak<RuntimeInner>,
    thread_id: ThreadId,
    lease_id: u64,
    released: bool,
}

pub struct RequestClaim {
    registry: Weak<RuntimeInner>,
    pub thread_id: ThreadId,
    pub request_id: RuntimeRequestId,
    claim_id: u64,
    transition: SequencedAgentEvent,
    settled: bool,
}

pub struct JournalPin {
    registry: Weak<RuntimeInner>,
    thread_id: ThreadId,
    pin_id: u64,
    released: bool,
}

impl ThreadRuntimeRegistry {
    pub fn new() -> Self {
        Self::with_limits(RuntimeLimits::default())
    }

    pub fn with_limits(limits: RuntimeLimits) -> Self {
        let overview = ThreadRuntimeOverview {
            revision: 0,
            threads: Vec::new(),
        };
        let (overview_tx, _) = watch::channel(overview);
        Self {
            inner: Arc::new(RuntimeInner {
                state: Mutex::new(RegistryState {
                    threads: HashMap::new(),
                    next_lease_id: 1,
                    next_claim_id: 1,
                    overview_revision: 0,
                    overview_threads: Vec::new(),
                }),
                overview_tx,
                limits,
            }),
        }
    }

    /// Subscribe one delivery forwarder to authoritative cross-thread replacements.
    ///
    /// This receiver is intentionally global rather than per connection. The integration layer
    /// must keep one task draining it and publish each changed value through the replacement lane;
    /// `watch` coalescing is safe because every value is a complete snapshot.
    pub fn subscribe_overview(&self) -> watch::Receiver<ThreadRuntimeOverview> {
        self.inner.overview_tx.subscribe()
    }

    pub fn current_overview(&self) -> ThreadRuntimeOverview {
        self.inner.overview_tx.borrow().clone()
    }

    pub(crate) fn reserve(
        &self,
        thread_id: ThreadId,
        owner: TurnOwner,
    ) -> Result<ThreadTurnLease, HarnessError> {
        let mut state = self.inner.lock_state();
        if let Some(existing) = state
            .threads
            .get(&thread_id)
            .and_then(|runtime| runtime.owner.as_ref())
        {
            warn!(
                %thread_id,
                owner_project_id = %existing.owner.project_id,
                owner_turn_id = ?existing.acknowledged_turn,
                owner_harness_thread_id = %existing.owner.harness_thread_id,
                owner_context_kind = existing.owner.context_kind,
                owner_mode = ?existing.owner.mode,
                owner_provider = %existing.owner.model.provider,
                owner_model = %existing.owner.model.model,
                owner_elapsed_ms = existing.reserved_at.elapsed().as_millis(),
                rejected_project_id = %owner.project_id,
                rejected_context_kind = owner.context_kind,
                "rejecting turn start because thread runtime is already active"
            );
            return Err(HarnessError::ThreadBusy { thread: thread_id });
        }
        let lease_id = state.next_lease_id;
        state.next_lease_id = next_nonzero(state.next_lease_id);
        let runtime = state
            .threads
            .entry(thread_id)
            .or_insert_with(|| ThreadRuntime::new(self.inner.limits));
        runtime.blocked = None;
        runtime.owner = Some(TurnLeaseState {
            lease_id,
            owner: owner.clone(),
            acknowledged_turn: None,
            reserved_at: Instant::now(),
        });
        self.inner.refresh_overview(&mut state);
        debug!(
            %thread_id,
            project_id = %owner.project_id,
            harness_thread_id = %owner.harness_thread_id,
            context_kind = owner.context_kind,
            "reserved active thread runtime"
        );
        Ok(ThreadTurnLease {
            registry: Arc::downgrade(&self.inner),
            thread_id,
            lease_id,
            released: false,
        })
    }

    pub fn is_active(&self, thread_id: ThreadId) -> bool {
        self.inner
            .lock_state()
            .threads
            .get(&thread_id)
            .is_some_and(|runtime| runtime.owner.is_some())
    }

    pub fn ensure_turn_with_user_input(
        &self,
        thread_id: ThreadId,
        turn_id: TurnId,
        user_input: Option<UserInput>,
    ) -> Result<(), TurnId> {
        let mut state = self.inner.lock_state();
        let runtime = state
            .threads
            .entry(thread_id)
            .or_insert_with(|| ThreadRuntime::new(self.inner.limits));
        match runtime.live.as_mut() {
            None => {
                runtime.live = Some(LiveTurn::new(turn_id, user_input));
                Ok(())
            }
            Some(live) if live.turn_id == turn_id => {
                if live.user_input.is_none() && user_input.is_some() {
                    live.user_input = user_input;
                }
                Ok(())
            }
            Some(live) => Err(live.turn_id),
        }
    }

    pub fn replace_turn_with_user_input(
        &self,
        thread_id: ThreadId,
        turn_id: TurnId,
        user_input: Option<UserInput>,
    ) {
        let mut state = self.inner.lock_state();
        let runtime = state
            .threads
            .entry(thread_id)
            .or_insert_with(|| ThreadRuntime::new(self.inner.limits));
        runtime.live = Some(LiveTurn::new(turn_id, user_input));
    }

    pub fn apply_event(
        &self,
        thread_id: ThreadId,
        event: AgentEvent,
        user_input: Option<UserInput>,
    ) -> Result<AppliedRuntimeEvent, RuntimeApplyError> {
        self.apply_event_with_coverage(thread_id, event, user_input, None)
    }

    pub fn apply_event_with_coverage(
        &self,
        thread_id: ThreadId,
        event: AgentEvent,
        user_input: Option<UserInput>,
        coverage: Option<JournalCoverage>,
    ) -> Result<AppliedRuntimeEvent, RuntimeApplyError> {
        let actual = event_thread_id(&event);
        if actual != thread_id {
            return Err(RuntimeApplyError::ForeignThread {
                expected: thread_id,
                actual,
            });
        }
        let agent_payload = browser_wire_event(event.clone(), user_input).map(|agent_event| {
            ThreadEventPayload::Agent {
                agent_event: Box::new(agent_event),
            }
        });
        let request_bearing = is_request_event(&event);
        if agent_payload.is_none() && !request_bearing {
            return Ok(AppliedRuntimeEvent {
                stream_event: None,
                running_tasks_if_changed: None,
                requests_changed: false,
            });
        }
        let mut state = self.inner.lock_state();
        let runtime = state
            .threads
            .entry(thread_id)
            .or_insert_with(|| ThreadRuntime::new(self.inner.limits));

        runtime.journal.ensure_sequence_available(thread_id)?;
        let task_changed = apply_task_event(runtime, &event);
        let request_state = apply_request_event(runtime, thread_id, &event);
        let requests_changed = request_state.is_some();
        let payload = if let Some(request) = request_state {
            ThreadEventPayload::Request {
                request: Box::new(request),
            }
        } else if request_bearing {
            warn!(
                %thread_id,
                action = "apply_request_event",
                "request lifecycle event did not match authoritative request state"
            );
            return Ok(AppliedRuntimeEvent {
                stream_event: None,
                running_tasks_if_changed: task_changed.then(|| runtime.task_snapshot(thread_id)),
                requests_changed: false,
            });
        } else {
            let Some(payload) = agent_payload else {
                return Ok(AppliedRuntimeEvent {
                    stream_event: None,
                    running_tasks_if_changed: task_changed
                        .then(|| runtime.task_snapshot(thread_id)),
                    requests_changed: false,
                });
            };
            payload
        };
        let seq = runtime.journal.allocate_seq(thread_id)?;
        let encoded_bytes = encoded_event_bytes(seq, &payload, self.inner.limits.journal_bytes);
        if let Some(live) = runtime.live.as_mut()
            && agent_event_turn_id(&event).is_none_or(|turn_id| turn_id == live.turn_id)
        {
            append_live_event(live, event.clone());
            live.represented_through = seq;
        }
        let sequenced = SequencedAgentEvent {
            event: SequencedThreadEvent {
                seq,
                event: payload,
            },
            coverage,
            encoded_bytes,
        };
        runtime.journal.append(sequenced.clone(), self.inner.limits);
        let running_tasks_if_changed = task_changed.then(|| runtime.task_snapshot(thread_id));
        self.inner.refresh_overview(&mut state);
        Ok(AppliedRuntimeEvent {
            stream_event: Some(sequenced),
            running_tasks_if_changed,
            requests_changed,
        })
    }

    /// Mark only events already present at persistence time as durably covered by this turn.
    /// Events for the same turn which arrive later retain `None` coverage.
    pub fn mark_turn_persisted(&self, thread_id: ThreadId, turn_id: TurnId) {
        let mut state = self.inner.lock_state();
        let Some(runtime) = state.threads.get_mut(&thread_id) else {
            return;
        };
        for entry in &mut runtime.journal.entries {
            if thread_event_turn_id(&entry.event.event) == Some(turn_id) {
                entry.coverage = Some(JournalCoverage::Turn(turn_id));
            }
        }
    }

    pub fn clear_live_turn(&self, thread_id: ThreadId) {
        let mut state = self.inner.lock_state();
        let Some(runtime) = state.threads.get_mut(&thread_id) else {
            return;
        };
        runtime.live = None;
        runtime
            .requests
            .retain(|_, request| !matches!(request.status, RuntimeRequestStatus::Resolved(_)));
        self.inner.refresh_overview(&mut state);
    }

    pub fn item_events(&self, thread_id: ThreadId, item_id: ItemId) -> Vec<AgentEvent> {
        self.inner
            .lock_state()
            .threads
            .get(&thread_id)
            .and_then(|runtime| runtime.live.as_ref())
            .into_iter()
            .flat_map(|live| live.events.iter())
            .filter(|event| match event {
                AgentEvent::ItemStarted { item, .. } => item.id == item_id,
                AgentEvent::ItemCompleted { item, .. } => item.id == item_id,
                _ => false,
            })
            .cloned()
            .collect()
    }

    pub fn capture_live_cut(&self, thread_id: ThreadId) -> Result<LiveRuntimeCut, JournalPinError> {
        let mut state = self.inner.lock_state();
        let runtime = state
            .threads
            .entry(thread_id)
            .or_insert_with(|| ThreadRuntime::new(self.inner.limits));
        let live = runtime.live_snapshot(thread_id);
        let represented_through = live
            .as_ref()
            .map(|snapshot| snapshot.represented_through)
            .unwrap_or_else(|| runtime.journal.latest_seq());
        let pin_id = runtime.journal.install_pin(
            represented_through,
            self.inner.limits.pin_suffix_entries,
            self.inner.limits.pin_suffix_bytes,
            self.inner.limits,
        )?;
        Ok(LiveRuntimeCut {
            live,
            represented_through,
            pin: JournalPin {
                registry: Arc::downgrade(&self.inner),
                thread_id,
                pin_id,
                released: false,
            },
        })
    }

    pub fn finalize_bootstrap_cut(
        &self,
        pin: &JournalPin,
    ) -> Result<BootstrapRuntimeCut, JournalPinError> {
        self.finalize_bootstrap_cut_with(pin, |_| true)
    }

    /// Capture the final runtime/suffix cut and prepare the matching subscription barrier under
    /// the required `runtime -> subscription` lock order.
    pub fn finalize_bootstrap_cut_with(
        &self,
        pin: &JournalPin,
        prepare: impl FnOnce(u64) -> bool,
    ) -> Result<BootstrapRuntimeCut, JournalPinError> {
        let state = self.inner.lock_state();
        let runtime = state
            .threads
            .get(&pin.thread_id)
            .ok_or(JournalPinError::Released)?;
        let pin_state = runtime
            .journal
            .pins
            .get(&pin.pin_id)
            .ok_or(JournalPinError::Released)?;
        if pin_state.exhausted {
            return Err(JournalPinError::Exhausted);
        }
        let through_seq = runtime.journal.latest_seq();
        if !prepare(through_seq) {
            return Err(JournalPinError::PreparationRejected);
        }
        let suffix = runtime
            .journal
            .entries
            .iter()
            .filter(|entry| entry.seq() > pin_state.after_seq && entry.seq() <= through_seq)
            .cloned()
            .collect();
        Ok(BootstrapRuntimeCut {
            through_seq,
            suffix,
            final_runtime: runtime.final_snapshot(pin.thread_id),
        })
    }

    pub fn task_snapshot(&self, thread_id: ThreadId) -> ThreadTaskSnapshot {
        self.inner
            .lock_state()
            .threads
            .get(&thread_id)
            .map(|runtime| runtime.task_snapshot(thread_id))
            .unwrap_or(ThreadTaskSnapshot {
                thread_id,
                revision: 0,
                tasks: Vec::new(),
            })
    }

    pub fn task_by_process(&self, thread_id: ThreadId, process_id: &str) -> Option<RunningTask> {
        self.inner
            .lock_state()
            .threads
            .get(&thread_id)
            .and_then(|runtime| {
                runtime
                    .tasks
                    .values()
                    .find(|task| task.process_id.as_deref() == Some(process_id))
                    .cloned()
            })
    }

    pub fn task_by_item(
        &self,
        thread_id: ThreadId,
        turn_id: TurnId,
        item_id: ItemId,
    ) -> Option<RunningTask> {
        self.inner
            .lock_state()
            .threads
            .get(&thread_id)
            .and_then(|runtime| runtime.tasks.get(&(turn_id, item_id)).cloned())
    }

    pub fn has_running_for_turn(&self, thread_id: ThreadId, turn_id: TurnId) -> bool {
        self.inner
            .lock_state()
            .threads
            .get(&thread_id)
            .is_some_and(|runtime| {
                runtime
                    .tasks
                    .values()
                    .any(|task| task.turn_id == turn_id && command_status_is_running(&task.status))
            })
    }

    pub fn has_running_for_thread(&self, thread_id: ThreadId) -> bool {
        self.inner
            .lock_state()
            .threads
            .get(&thread_id)
            .is_some_and(|runtime| {
                runtime
                    .tasks
                    .values()
                    .any(|task| command_status_is_running(&task.status))
            })
    }

    pub fn set_task_terminating(
        &self,
        thread_id: ThreadId,
        process_id: &str,
        terminating: bool,
    ) -> Option<ThreadTaskSnapshot> {
        let mut state = self.inner.lock_state();
        let runtime = state.threads.get_mut(&thread_id)?;
        let mut changed = false;
        for task in runtime.tasks.values_mut() {
            if task.process_id.as_deref() == Some(process_id) && task.terminating != terminating {
                task.terminating = terminating;
                changed = true;
            }
        }
        if !changed {
            return None;
        }
        runtime.bump_task_revision();
        Some(runtime.task_snapshot(thread_id))
    }

    pub fn remove_task_by_process(
        &self,
        thread_id: ThreadId,
        process_id: &str,
    ) -> Option<ThreadTaskSnapshot> {
        let mut state = self.inner.lock_state();
        let runtime = state.threads.get_mut(&thread_id)?;
        let before = runtime.tasks.len();
        runtime
            .tasks
            .retain(|_, task| task.process_id.as_deref() != Some(process_id));
        if runtime.tasks.len() == before {
            return None;
        }
        runtime.bump_task_revision();
        Some(runtime.task_snapshot(thread_id))
    }

    pub fn claim_request(
        &self,
        thread_id: ThreadId,
        request_id: RuntimeRequestId,
    ) -> Result<RequestClaim, RequestClaimError> {
        let mut state = self.inner.lock_state();
        let claim_id = state.next_claim_id;
        state.next_claim_id = next_nonzero(state.next_claim_id);
        let runtime = state
            .threads
            .get_mut(&thread_id)
            .ok_or(RequestClaimError::Missing)?;
        let transition_state = {
            let request = runtime
                .requests
                .get_mut(&request_id)
                .ok_or(RequestClaimError::Missing)?;
            match request.status {
                RuntimeRequestStatus::Pending => {}
                RuntimeRequestStatus::Responding => return Err(RequestClaimError::Responding),
                RuntimeRequestStatus::Resolved(_) => return Err(RequestClaimError::Resolved),
            }
            request.status = RuntimeRequestStatus::Responding;
            request.claim_id = Some(claim_id);
            request_state(thread_id, &request_id, request)
        };
        let transition =
            match runtime.append_request_transition(thread_id, transition_state, self.inner.limits)
            {
                Ok(transition) => transition,
                Err(_) => {
                    if let Some(request) = runtime.requests.get_mut(&request_id) {
                        request.status = RuntimeRequestStatus::Pending;
                        request.claim_id = None;
                    }
                    return Err(RequestClaimError::SequenceExhausted);
                }
            };
        self.inner.refresh_overview(&mut state);
        Ok(RequestClaim {
            registry: Arc::downgrade(&self.inner),
            thread_id,
            request_id,
            claim_id,
            transition,
            settled: false,
        })
    }

    pub fn final_snapshot(&self, thread_id: ThreadId) -> ThreadFinalRuntime {
        self.inner
            .lock_state()
            .threads
            .get(&thread_id)
            .map(|runtime| runtime.final_snapshot(thread_id))
            .unwrap_or_else(|| idle_final_snapshot(thread_id))
    }

    pub fn persistence_blocked(&self, thread_id: ThreadId) -> Option<PersistenceBlockedState> {
        self.inner
            .lock_state()
            .threads
            .get(&thread_id)
            .and_then(|runtime| runtime.blocked.clone())
    }

    /// Return the already-sequenced terminal event and current task replacement retained for a
    /// turn whose history append is being recovered. Recovery must publish this event rather than
    /// applying a second synthetic completion and allocating another sequence number.
    pub(crate) fn persistence_recovery_effects(
        &self,
        thread_id: ThreadId,
        turn_id: TurnId,
    ) -> Option<AppliedRuntimeEvent> {
        let state = self.inner.lock_state();
        let runtime = state.threads.get(&thread_id)?;
        let stream_event = runtime
            .journal
            .entries
            .iter()
            .rev()
            .find(|entry| {
                matches!(
                    &entry.event.event,
                    ThreadEventPayload::Agent { agent_event }
                        if matches!(
                            agent_event.as_ref(),
                            WireAgentEvent::TurnCompleted { turn, .. } if *turn == turn_id
                        )
                )
            })
            .cloned()?;
        Some(AppliedRuntimeEvent {
            stream_event: Some(stream_event),
            running_tasks_if_changed: Some(runtime.task_snapshot(thread_id)),
            requests_changed: true,
        })
    }

    /// Settle a successfully persisted blocked turn and return the retained durable record.
    pub fn settle_persistence_blocked(
        &self,
        thread_id: ThreadId,
        expected_turn_id: TurnId,
    ) -> Result<PersistenceBlockedState, PersistenceTransitionError> {
        let mut state = self.inner.lock_state();
        let blocked = take_persistence_blocked(&mut state, thread_id, expected_turn_id)?;
        self.inner.refresh_overview(&mut state);
        Ok(blocked)
    }

    /// Explicitly discard the only complete representation of a turn after user confirmation.
    pub fn discard_persistence_blocked(
        &self,
        thread_id: ThreadId,
        expected_turn_id: TurnId,
    ) -> Result<PersistenceBlockedState, PersistenceTransitionError> {
        let mut state = self.inner.lock_state();
        let blocked = take_persistence_blocked(&mut state, thread_id, expected_turn_id)?;
        error!(
            %thread_id,
            turn_id = %blocked.turn.id,
            attempts = blocked.attempts,
            action = "discard_unpersisted_turn",
            "discarded an unpersisted turn after explicit confirmation"
        );
        self.inner.refresh_overview(&mut state);
        Ok(blocked)
    }

    pub fn update_persistence_blocked_failure(
        &self,
        thread_id: ThreadId,
        expected_turn_id: TurnId,
        attempts: u32,
        error: String,
    ) -> Result<(), PersistenceTransitionError> {
        let mut state = self.inner.lock_state();
        let Some(blocked) = state
            .threads
            .get_mut(&thread_id)
            .and_then(|runtime| runtime.blocked.as_mut())
        else {
            return Err(PersistenceTransitionError::MissingBlocked);
        };
        if blocked.turn.id != expected_turn_id {
            return Err(PersistenceTransitionError::TurnMismatch);
        }
        blocked.attempts = attempts;
        blocked.error = error;
        self.inner.refresh_overview(&mut state);
        Ok(())
    }

    pub fn forget(&self, thread_id: ThreadId) {
        let mut state = self.inner.lock_state();
        state.threads.remove(&thread_id);
        self.inner.refresh_overview(&mut state);
    }

    /// Retire process-local clocks only when no domain state or bootstrap pin still needs them.
    /// The caller must separately establish that the Hub has no subscriber carrying this event
    /// sequence as its current baseline.
    pub fn retire_if_idle(&self, thread_id: ThreadId) -> bool {
        let mut state = self.inner.lock_state();
        let retire = state.threads.get(&thread_id).is_some_and(|runtime| {
            runtime.owner.is_none()
                && runtime.blocked.is_none()
                && runtime.live.is_none()
                && runtime.tasks.is_empty()
                && runtime.journal.pins.is_empty()
                && runtime
                    .requests
                    .values()
                    .all(|request| matches!(request.status, RuntimeRequestStatus::Resolved(_)))
        });
        if !retire {
            return false;
        }
        state.threads.remove(&thread_id);
        self.inner.refresh_overview(&mut state);
        debug!(%thread_id, action = "retire_thread_runtime", "retired idle thread runtime");
        true
    }
}

impl Default for ThreadRuntimeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn take_persistence_blocked(
    state: &mut RegistryState,
    thread_id: ThreadId,
    expected_turn_id: TurnId,
) -> Result<PersistenceBlockedState, PersistenceTransitionError> {
    let runtime = state
        .threads
        .get_mut(&thread_id)
        .ok_or(PersistenceTransitionError::MissingBlocked)?;
    let blocked = runtime
        .blocked
        .as_ref()
        .ok_or(PersistenceTransitionError::MissingBlocked)?;
    if blocked.turn.id != expected_turn_id {
        return Err(PersistenceTransitionError::TurnMismatch);
    }
    let blocked = runtime
        .blocked
        .take()
        .ok_or(PersistenceTransitionError::MissingBlocked)?;
    runtime.owner = None;
    runtime.live = None;
    runtime
        .requests
        .retain(|_, request| !matches!(request.status, RuntimeRequestStatus::Resolved(_)));
    Ok(blocked)
}

impl RuntimeInner {
    fn lock_state(&self) -> MutexGuard<'_, RegistryState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                error!(
                    action = "lock_thread_runtime_registry",
                    "thread runtime lock was poisoned; recovering state"
                );
                poisoned.into_inner()
            }
        }
    }

    fn refresh_overview(&self, state: &mut RegistryState) {
        let mut keys: Vec<_> = state
            .threads
            .iter()
            .filter_map(|(thread_id, runtime)| runtime.overview(*thread_id))
            .collect();
        keys.sort_by_key(|entry| entry.thread_id.to_string());
        if keys == state.overview_threads {
            return;
        }
        state.overview_threads = keys.clone();
        state.overview_revision = state.overview_revision.saturating_add(1);
        self.overview_tx.send_replace(ThreadRuntimeOverview {
            revision: state.overview_revision,
            threads: keys.into_iter().map(OverviewKey::into_proto).collect(),
        });
    }

    fn acknowledge(&self, thread_id: ThreadId, lease_id: u64, turn_id: TurnId) {
        let mut state = self.lock_state();
        let Some(owner) = state
            .threads
            .get_mut(&thread_id)
            .and_then(|runtime| runtime.owner.as_mut())
        else {
            warn!(%thread_id, %turn_id, "turn acknowledgement has no runtime owner");
            return;
        };
        if owner.lease_id != lease_id {
            warn!(%thread_id, %turn_id, "ignoring acknowledgement from a stale turn lease");
            return;
        }
        owner.acknowledged_turn = Some(turn_id);
        self.refresh_overview(&mut state);
    }

    fn release_lease(&self, thread_id: ThreadId, lease_id: u64) {
        let mut state = self.lock_state();
        let Some(runtime) = state.threads.get_mut(&thread_id) else {
            return;
        };
        if runtime.owner.as_ref().map(|owner| owner.lease_id) != Some(lease_id) {
            warn!(%thread_id, "ignoring release from a stale turn lease");
            return;
        }
        runtime.owner = None;
        self.refresh_overview(&mut state);
    }

    fn transfer_to_persistence_blocked(
        &self,
        thread_id: ThreadId,
        lease_id: u64,
        blocked: PersistenceBlockedState,
    ) -> Result<(), PersistenceTransitionError> {
        let mut state = self.lock_state();
        let runtime = state
            .threads
            .get_mut(&thread_id)
            .ok_or(PersistenceTransitionError::MissingOwner)?;
        let owner = runtime
            .owner
            .as_ref()
            .ok_or(PersistenceTransitionError::MissingOwner)?;
        if owner.lease_id != lease_id {
            return Err(PersistenceTransitionError::StaleLease);
        }
        if owner.acknowledged_turn != Some(blocked.turn.id) {
            return Err(PersistenceTransitionError::TurnMismatch);
        }
        if runtime.blocked.is_some() {
            return Err(PersistenceTransitionError::AlreadyBlocked);
        }
        runtime.blocked = Some(blocked);
        self.refresh_overview(&mut state);
        Ok(())
    }

    fn settle_request(
        &self,
        thread_id: ThreadId,
        request_id: &RuntimeRequestId,
        claim_id: u64,
        resolution: Option<RuntimeRequestResolution>,
    ) -> Result<SequencedAgentEvent, RequestClaimError> {
        let mut state = self.lock_state();
        let runtime = state
            .threads
            .get_mut(&thread_id)
            .ok_or(RequestClaimError::Missing)?;
        let (transition_state, settled_resolution) = {
            let request = runtime
                .requests
                .get_mut(request_id)
                .ok_or(RequestClaimError::Missing)?;
            let harness_resolved = matches!(
                request.status,
                RuntimeRequestStatus::Resolved(RuntimeRequestResolution::Harness)
            );
            if request.claim_id != Some(claim_id)
                || (!matches!(request.status, RuntimeRequestStatus::Responding)
                    && !harness_resolved)
            {
                return Err(RequestClaimError::StaleClaim);
            }
            request.claim_id = None;
            request.status = match resolution {
                Some(resolution) => RuntimeRequestStatus::Resolved(resolution),
                None if harness_resolved => {
                    RuntimeRequestStatus::Resolved(RuntimeRequestResolution::Harness)
                }
                None => RuntimeRequestStatus::Pending,
            };
            let settled_resolution = match &request.status {
                RuntimeRequestStatus::Resolved(resolution) => Some(resolution.clone()),
                RuntimeRequestStatus::Pending | RuntimeRequestStatus::Responding => None,
            };
            (
                request_state(thread_id, request_id, request),
                settled_resolution,
            )
        };
        let transition =
            match runtime.append_request_transition(thread_id, transition_state, self.limits) {
                Ok(transition) => transition,
                Err(_) => {
                    if let Some(request) = runtime.requests.get_mut(request_id) {
                        request.status = RuntimeRequestStatus::Responding;
                        request.claim_id = Some(claim_id);
                    }
                    return Err(RequestClaimError::SequenceExhausted);
                }
            };
        if let Some(resolution) = settled_resolution {
            runtime
                .resolved_requests
                .insert(request_id.clone(), resolution);
        }
        self.refresh_overview(&mut state);
        Ok(transition)
    }

    fn release_pin(&self, thread_id: ThreadId, pin_id: u64) {
        let mut state = self.lock_state();
        let Some(runtime) = state.threads.get_mut(&thread_id) else {
            return;
        };
        runtime.journal.pins.remove(&pin_id);
        runtime.journal.evict(self.limits);
    }
}

impl ThreadRuntime {
    fn new(limits: RuntimeLimits) -> Self {
        Self {
            owner: None,
            blocked: None,
            live: None,
            tasks: HashMap::new(),
            task_revision: 0,
            requests: IndexMap::new(),
            resolved_requests: HashMap::new(),
            journal: EventJournal::new(limits),
        }
    }

    fn live_snapshot(&self, thread_id: ThreadId) -> Option<LiveTurnProjection> {
        self.live.as_ref().map(|live| {
            let mut seen_requests = HashSet::new();
            let mut events = Vec::new();
            for event in &live.events {
                if matches!(event, AgentEvent::ServerRequestResolved { .. }) {
                    continue;
                }
                if let Some(request_id) = request_event_id(event) {
                    if !seen_requests.insert(request_id.clone()) {
                        continue;
                    }
                    if let Some(request) = self.requests.get(&request_id) {
                        events.push(ThreadEventPayload::Request {
                            request: Box::new(request_state(thread_id, &request_id, request)),
                        });
                    }
                    continue;
                }
                if let Some(agent_event) = browser_wire_event(event.clone(), None) {
                    events.push(ThreadEventPayload::Agent {
                        agent_event: Box::new(agent_event),
                    });
                }
            }
            LiveTurnProjection {
                turn_id: live.turn_id,
                user_input: live.user_input.clone(),
                events,
                represented_through: live.represented_through,
            }
        })
    }

    fn append_request_transition(
        &mut self,
        thread_id: ThreadId,
        request: RequestState,
        limits: RuntimeLimits,
    ) -> Result<SequencedAgentEvent, RuntimeApplyError> {
        let seq = self.journal.allocate_seq(thread_id)?;
        let event = SequencedThreadEvent {
            seq,
            event: ThreadEventPayload::Request {
                request: Box::new(request),
            },
        };
        let encoded_bytes = match serde_json::to_vec(&event) {
            Ok(encoded) => encoded.len(),
            Err(error) => {
                error!(
                    %thread_id,
                    event_seq = seq,
                    action = "measure_request_transition",
                    %error,
                    "failed to measure request transition; charging the journal byte limit"
                );
                limits.journal_bytes
            }
        };
        let entry = SequencedAgentEvent {
            event,
            coverage: None,
            encoded_bytes,
        };
        self.journal.append(entry.clone(), limits);
        if let Some(live) = self.live.as_mut() {
            live.represented_through = seq;
        }
        Ok(entry)
    }

    fn bump_task_revision(&mut self) {
        self.task_revision = self.task_revision.saturating_add(1);
    }

    fn task_snapshot(&self, thread_id: ThreadId) -> ThreadTaskSnapshot {
        let mut tasks: Vec<_> = self.tasks.values().cloned().collect();
        tasks.sort_by_key(|task| (task.turn_id.to_string(), task.item_id.to_string()));
        ThreadTaskSnapshot {
            thread_id,
            revision: self.task_revision,
            tasks,
        }
    }

    fn final_snapshot(&self, thread_id: ThreadId) -> ThreadFinalRuntime {
        let turn_state = if let Some(blocked) = &self.blocked {
            RuntimeTurnState::PersistenceBlocked {
                turn_id: blocked.turn.id,
                attempts: blocked.attempts,
                error: blocked.error.clone(),
            }
        } else if let Some(owner) = &self.owner {
            RuntimeTurnState::Active {
                turn_id: owner.acknowledged_turn,
            }
        } else {
            RuntimeTurnState::Idle
        };
        ThreadFinalRuntime {
            through_seq: self.journal.latest_seq(),
            turn_state,
            tasks: self.task_snapshot(thread_id),
            requests: self
                .requests
                .iter()
                .map(|(request_id, request)| request_state(thread_id, request_id, request))
                .collect(),
        }
    }

    fn overview(&self, thread_id: ThreadId) -> Option<OverviewKey> {
        let blocked = self.blocked.as_ref().map(|blocked| BlockedOverview {
            turn_id: blocked.turn.id,
            attempts: blocked.attempts,
            error: blocked.error.clone(),
        });
        let turn_id = blocked.as_ref().map(|blocked| blocked.turn_id).or_else(|| {
            self.owner
                .as_ref()
                .and_then(|owner| owner.acknowledged_turn)
        });
        let requests: Vec<_> = self
            .requests
            .iter()
            .filter(|(_, request)| {
                matches!(
                    request.status,
                    RuntimeRequestStatus::Pending | RuntimeRequestStatus::Responding
                )
            })
            .map(|(request_id, request)| {
                (
                    request_id.kind(),
                    request_id.as_str().to_string(),
                    matches!(request.status, RuntimeRequestStatus::Responding),
                )
            })
            .collect();
        if self.owner.is_none() && blocked.is_none() && requests.is_empty() {
            return None;
        }
        Some(OverviewKey {
            thread_id,
            blocked,
            active: self.owner.is_some(),
            turn_id,
            requests,
        })
    }
}

impl OverviewKey {
    fn into_proto(self) -> ThreadRuntimeSummary {
        let turn_state = if let Some(blocked) = self.blocked {
            RuntimeTurnState::PersistenceBlocked {
                turn_id: blocked.turn_id,
                attempts: blocked.attempts,
                error: blocked.error,
            }
        } else if self.active {
            RuntimeTurnState::Active {
                turn_id: self.turn_id,
            }
        } else {
            RuntimeTurnState::Idle
        };
        ThreadRuntimeSummary {
            thread_id: self.thread_id,
            turn_state,
            outstanding_requests: self
                .requests
                .into_iter()
                .map(|(kind, request_id, responding)| OutstandingRequest {
                    request_id,
                    kind: request_kind(kind),
                    responding,
                })
                .collect(),
        }
    }
}

impl LiveTurn {
    fn new(turn_id: TurnId, user_input: Option<UserInput>) -> Self {
        Self {
            turn_id,
            user_input,
            events: Vec::new(),
            represented_through: 0,
        }
    }
}

impl EventJournal {
    fn new(limits: RuntimeLimits) -> Self {
        Self {
            entries: VecDeque::with_capacity(limits.journal_entries.min(256)),
            bytes: 0,
            next_seq: 1,
            next_pin_id: 1,
            pins: HashMap::new(),
        }
    }

    fn allocate_seq(&mut self, thread_id: ThreadId) -> Result<u64, RuntimeApplyError> {
        self.ensure_sequence_available(thread_id)?;
        let seq = self.next_seq;
        self.next_seq += 1;
        Ok(seq)
    }

    fn ensure_sequence_available(&self, thread_id: ThreadId) -> Result<(), RuntimeApplyError> {
        if self.next_seq == u64::MAX {
            return Err(RuntimeApplyError::SequenceExhausted(thread_id));
        }
        Ok(())
    }

    fn latest_seq(&self) -> u64 {
        self.next_seq.saturating_sub(1)
    }

    fn append(&mut self, entry: SequencedAgentEvent, limits: RuntimeLimits) {
        for pin in self.pins.values_mut() {
            if pin.exhausted || entry.seq() <= pin.after_seq {
                continue;
            }
            pin.retained_entries = pin.retained_entries.saturating_add(1);
            pin.retained_bytes = pin.retained_bytes.saturating_add(entry.encoded_bytes);
            if pin.retained_entries > pin.max_entries || pin.retained_bytes > pin.max_bytes {
                pin.exhausted = true;
            }
        }
        self.bytes = self.bytes.saturating_add(entry.encoded_bytes);
        self.entries.push_back(entry);
        self.evict(limits);
    }

    fn install_pin(
        &mut self,
        after_seq: u64,
        max_entries: usize,
        max_bytes: usize,
        limits: RuntimeLimits,
    ) -> Result<u64, JournalPinError> {
        if self.latest_seq() > after_seq {
            let expected_first = after_seq.saturating_add(1);
            let first_retained = self
                .entries
                .iter()
                .find(|entry| entry.seq() > after_seq)
                .map(SequencedAgentEvent::seq);
            if first_retained != Some(expected_first) {
                return Err(JournalPinError::SuffixUnavailable);
            }
        }
        let reserved_entries: usize = self.pins.values().map(|pin| pin.max_entries).sum();
        let reserved_bytes: usize = self.pins.values().map(|pin| pin.max_bytes).sum();
        if reserved_entries.saturating_add(max_entries) > limits.total_pin_reservation_entries
            || reserved_bytes.saturating_add(max_bytes) > limits.total_pin_reservation_bytes
        {
            return Err(JournalPinError::ReservationCapacity);
        }
        let retained_entries = self
            .entries
            .iter()
            .filter(|entry| entry.seq() > after_seq)
            .count();
        let retained_bytes = self
            .entries
            .iter()
            .filter(|entry| entry.seq() > after_seq)
            .map(|entry| entry.encoded_bytes)
            .sum();
        if retained_entries > max_entries || retained_bytes > max_bytes {
            return Err(JournalPinError::SuffixTooLarge);
        }
        let pin_id = self.next_pin_id;
        self.next_pin_id = next_nonzero(self.next_pin_id);
        self.pins.insert(
            pin_id,
            PinState {
                after_seq,
                max_entries,
                max_bytes,
                retained_entries,
                retained_bytes,
                exhausted: false,
            },
        );
        Ok(pin_id)
    }

    fn evict(&mut self, limits: RuntimeLimits) {
        while self.entries.len() > limits.journal_entries || self.bytes > limits.journal_bytes {
            let protected_after = self
                .pins
                .values()
                .filter(|pin| !pin.exhausted)
                .map(|pin| pin.after_seq)
                .min();
            let Some(front) = self.entries.front() else {
                break;
            };
            if protected_after.is_some_and(|after| front.seq() > after) {
                break;
            }
            let Some(removed) = self.entries.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(removed.encoded_bytes);
        }
    }
}

impl ThreadTurnLease {
    pub(crate) fn acknowledge_turn(&mut self, turn_id: TurnId) {
        if self.released {
            warn!(thread_id = %self.thread_id, %turn_id, "ignoring acknowledgement after release");
            return;
        }
        if let Some(registry) = self.registry.upgrade() {
            registry.acknowledge(self.thread_id, self.lease_id, turn_id);
        }
    }

    pub(crate) fn release(&mut self) {
        if self.released {
            return;
        }
        if let Some(registry) = self.registry.upgrade() {
            registry.release_lease(self.thread_id, self.lease_id);
        }
        self.released = true;
    }

    pub(crate) fn transfer_to_persistence_blocked(
        &mut self,
        turn: Turn,
        attempts: u32,
        error: String,
    ) -> Result<(), PersistenceTransitionError> {
        let registry = self
            .registry
            .upgrade()
            .ok_or(PersistenceTransitionError::MissingOwner)?;
        registry.transfer_to_persistence_blocked(
            self.thread_id,
            self.lease_id,
            PersistenceBlockedState {
                turn,
                attempts,
                error,
            },
        )?;
        // Ownership moved into the runtime's recoverable terminal state. Do not let Drop release
        // the owner which keeps this thread blocked until retry succeeds or discard is confirmed.
        self.released = true;
        Ok(())
    }

    pub(crate) fn is_released(&self) -> bool {
        self.released
    }
}

impl Drop for ThreadTurnLease {
    fn drop(&mut self) {
        self.release();
    }
}

impl RequestClaim {
    pub fn transition(&self) -> &SequencedAgentEvent {
        &self.transition
    }

    pub fn commit(
        mut self,
        resolution: RuntimeRequestResolution,
    ) -> Result<SequencedAgentEvent, RequestClaimError> {
        let registry = self
            .registry
            .upgrade()
            .ok_or(RequestClaimError::StaleClaim)?;
        let result = registry.settle_request(
            self.thread_id,
            &self.request_id,
            self.claim_id,
            Some(resolution),
        );
        if result.is_ok() {
            self.settled = true;
        }
        result
    }

    pub fn rollback(mut self) -> Result<SequencedAgentEvent, RequestClaimError> {
        let registry = self
            .registry
            .upgrade()
            .ok_or(RequestClaimError::StaleClaim)?;
        let result = registry.settle_request(self.thread_id, &self.request_id, self.claim_id, None);
        if result.is_ok() {
            self.settled = true;
        }
        result
    }
}

impl std::fmt::Debug for RequestClaim {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RequestClaim")
            .field("thread_id", &self.thread_id)
            .field("request_id", &self.request_id)
            .field("claim_id", &self.claim_id)
            .field("transition", &self.transition)
            .field("settled", &self.settled)
            .finish_non_exhaustive()
    }
}

impl Drop for RequestClaim {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        if let Some(registry) = self.registry.upgrade() {
            match registry.settle_request(self.thread_id, &self.request_id, self.claim_id, None) {
                Ok(_) => self.settled = true,
                Err(error) => warn!(
                    thread_id = %self.thread_id,
                    request_id = self.request_id.as_str(),
                    action = "rollback_dropped_request_claim",
                    %error,
                    "failed to return a dropped request claim to pending state"
                ),
            }
        }
    }
}

impl JournalPin {
    pub fn release(&mut self) {
        if self.released {
            return;
        }
        if let Some(registry) = self.registry.upgrade() {
            registry.release_pin(self.thread_id, self.pin_id);
        }
        self.released = true;
    }
}

impl Drop for JournalPin {
    fn drop(&mut self) {
        self.release();
    }
}

fn apply_request_event(
    runtime: &mut ThreadRuntime,
    thread_id: ThreadId,
    event: &AgentEvent,
) -> Option<RequestState> {
    let request_id = match event {
        AgentEvent::ApprovalRequested { request, .. } => {
            let id = RuntimeRequestId::Approval(request.id.clone());
            if request_id_conflicts(runtime, &id) {
                error!(
                    %thread_id,
                    request_id = id.as_str(),
                    action = "register_runtime_request",
                    "approval id collides with a different request kind"
                );
                return None;
            }
            let payload = RuntimeRequestPayload::Approval(request.clone().into());
            match runtime.requests.get_mut(&id) {
                Some(record) => {
                    record.payload = payload;
                    match record.status {
                        RuntimeRequestStatus::Pending => {}
                        RuntimeRequestStatus::Responding | RuntimeRequestStatus::Resolved(_) => {}
                    }
                }
                None => {
                    let status = runtime
                        .resolved_requests
                        .get(&id)
                        .cloned()
                        .map(RuntimeRequestStatus::Resolved)
                        .unwrap_or(RuntimeRequestStatus::Pending);
                    runtime.requests.insert(
                        id.clone(),
                        RequestRecord {
                            payload,
                            status,
                            claim_id: None,
                        },
                    );
                }
            }
            id
        }
        AgentEvent::ServerRequestReceived { request, .. } => {
            let id = RuntimeRequestId::Server(request.id.clone());
            if request_id_conflicts(runtime, &id) {
                error!(
                    %thread_id,
                    request_id = id.as_str(),
                    action = "register_runtime_request",
                    "server request id collides with a different request kind"
                );
                return None;
            }
            let existing = runtime.requests.shift_remove(&id);
            let (status, claim_id) = match existing {
                Some(record) if matches!(record.status, RuntimeRequestStatus::Responding) => {
                    (record.status, record.claim_id)
                }
                Some(record) if matches!(record.status, RuntimeRequestStatus::Resolved(_)) => {
                    (record.status, None)
                }
                _ => (
                    runtime
                        .resolved_requests
                        .get(&id)
                        .cloned()
                        .map(RuntimeRequestStatus::Resolved)
                        .unwrap_or(RuntimeRequestStatus::Pending),
                    None,
                ),
            };
            runtime.requests.insert(
                id.clone(),
                RequestRecord {
                    payload: RuntimeRequestPayload::Server(request.clone()),
                    status,
                    claim_id,
                },
            );
            id
        }
        AgentEvent::ServerRequestResolved { request_id, .. } => {
            let id = RuntimeRequestId::Server(request_id.clone());
            runtime
                .resolved_requests
                .insert(id.clone(), RuntimeRequestResolution::Harness);
            let record = runtime.requests.get_mut(&id)?;
            // Retain a live claim token when resolution races its commit. `settle_request` then
            // turns the harness resolution into the browser's authoritative result. Records which
            // were not being answered already have no token.
            record.status = RuntimeRequestStatus::Resolved(RuntimeRequestResolution::Harness);
            id
        }
        _ => return None,
    };
    runtime
        .requests
        .get(&request_id)
        .map(|request| request_state(thread_id, &request_id, request))
}

fn request_id_conflicts(runtime: &ThreadRuntime, candidate: &RuntimeRequestId) -> bool {
    runtime
        .requests
        .keys()
        .chain(runtime.resolved_requests.keys())
        .any(|existing| existing != candidate && existing.as_str() == candidate.as_str())
}

fn apply_task_event(runtime: &mut ThreadRuntime, event: &AgentEvent) -> bool {
    let changed = match event {
        AgentEvent::ItemStarted { thread, turn, item } => {
            let task = if let Some(command) = &item.command {
                let status = command
                    .status
                    .clone()
                    .unwrap_or_else(|| "in_progress".into());
                if !command_status_is_running(&status) {
                    return false;
                }
                RunningTask {
                    kind: TaskKind::Command,
                    thread_id: *thread,
                    turn_id: *turn,
                    item_id: item.id,
                    harness_item_id: item.harness_item_id.clone(),
                    command: command.command.clone(),
                    cwd: command.cwd.clone(),
                    server: None,
                    status,
                    process_id: command.process_id.clone(),
                    started_at_ms: command.started_at_ms.unwrap_or_else(now_ms),
                    output: String::new(),
                    after_turn: false,
                    terminating: false,
                }
            } else if let Some(tool) = &item.tool {
                let status = tool.status.clone().unwrap_or_else(|| "in_progress".into());
                if !command_status_is_running(&status) {
                    return false;
                }
                RunningTask {
                    kind: TaskKind::Tool,
                    thread_id: *thread,
                    turn_id: *turn,
                    item_id: item.id,
                    harness_item_id: item.harness_item_id.clone(),
                    command: tool.name.clone(),
                    cwd: String::new(),
                    server: tool.server.clone(),
                    status,
                    process_id: None,
                    started_at_ms: tool.started_at_ms.unwrap_or_else(now_ms),
                    output: String::new(),
                    after_turn: false,
                    terminating: false,
                }
            } else {
                return false;
            };
            runtime.tasks.insert((*turn, item.id), task);
            true
        }
        AgentEvent::ItemDelta {
            turn,
            item_id,
            delta: ItemDelta::CommandOutput { chunk } | ItemDelta::Text { text: chunk },
            ..
        } => {
            let Some(task) = runtime.tasks.get_mut(&(*turn, *item_id)) else {
                return false;
            };
            task.output.push_str(chunk);
            truncate_task_output(&mut task.output);
            true
        }
        AgentEvent::ItemCompleted { turn, item, .. } => {
            let completed = match &item.payload {
                ItemPayload::CommandExecution {
                    command,
                    cwd,
                    output,
                    status,
                    process_id,
                    ..
                } => CompletedTask {
                    kind: TaskKind::Command,
                    command: command.clone(),
                    cwd: path_to_display(cwd),
                    server: None,
                    output: output.clone(),
                    status: status.clone(),
                    process_id: process_id.clone(),
                },
                ItemPayload::ToolCall {
                    name,
                    output,
                    server,
                    status,
                    error,
                    ..
                } => CompletedTask {
                    kind: TaskKind::Tool,
                    command: name.clone(),
                    cwd: String::new(),
                    server: server.clone(),
                    output: tool_output_string(output.as_ref(), error.as_deref()),
                    status: status.clone(),
                    process_id: None,
                },
                _ => return false,
            };
            let key = (*turn, item.id);
            match completed.status.as_deref() {
                None => runtime.tasks.remove(&key).is_some(),
                Some(status) if !command_status_is_running(status) => {
                    runtime.tasks.remove(&key).is_some()
                }
                Some(status) => {
                    let mut output = completed.output;
                    truncate_task_output(&mut output);
                    let existing = runtime.tasks.get(&key);
                    let started_at_ms = existing
                        .map(|task| task.started_at_ms)
                        .unwrap_or_else(now_ms);
                    let after_turn = existing.is_some_and(|task| task.after_turn);
                    let terminating = existing.is_some_and(|task| task.terminating);
                    runtime.tasks.insert(
                        key,
                        RunningTask {
                            kind: completed.kind,
                            thread_id: event_thread_id(event),
                            turn_id: *turn,
                            item_id: item.id,
                            harness_item_id: item.harness_item_id.clone(),
                            command: completed.command,
                            cwd: completed.cwd,
                            server: completed.server,
                            status: status.to_string(),
                            process_id: completed.process_id,
                            started_at_ms,
                            output,
                            after_turn,
                            terminating,
                        },
                    );
                    true
                }
            }
        }
        AgentEvent::TurnCompleted { turn, .. } => {
            let mut changed = false;
            runtime.tasks.retain(|_, task| {
                let drop_task = task.turn_id == *turn
                    && task.kind == TaskKind::Tool
                    && command_status_is_running(&task.status);
                changed |= drop_task;
                !drop_task
            });
            for task in runtime.tasks.values_mut() {
                if task.turn_id == *turn
                    && task.kind == TaskKind::Command
                    && command_status_is_running(&task.status)
                    && !task.after_turn
                {
                    task.after_turn = true;
                    changed = true;
                }
            }
            changed
        }
        _ => false,
    };
    if changed {
        runtime.bump_task_revision();
    }
    changed
}

struct CompletedTask {
    kind: TaskKind,
    command: String,
    cwd: String,
    server: Option<String>,
    output: String,
    status: Option<String>,
    process_id: Option<String>,
}

fn append_live_event(live: &mut LiveTurn, event: AgentEvent) {
    if let AgentEvent::TurnStarted { turn, .. } = &event {
        live.turn_id = *turn;
    }
    if let Some(item_id) = completed_command_item_id(&event) {
        remove_deltas(&mut live.events, item_id, DeltaKind::Command);
    }
    let delta = delta_item(&event);
    let event = compact_completed_command_output(event);
    live.events.push(event);
    if let Some((item_id, kind)) = delta {
        compact_deltas(&mut live.events, item_id, kind);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DeltaKind {
    Command,
    Text,
}

fn delta_item(event: &AgentEvent) -> Option<(ItemId, DeltaKind)> {
    match event {
        AgentEvent::ItemDelta {
            item_id,
            delta: ItemDelta::CommandOutput { .. },
            ..
        } => Some((*item_id, DeltaKind::Command)),
        AgentEvent::ItemDelta {
            item_id,
            delta: ItemDelta::Text { .. },
            ..
        } => Some((*item_id, DeltaKind::Text)),
        _ => None,
    }
}

fn remove_deltas(events: &mut Vec<AgentEvent>, item_id: ItemId, kind: DeltaKind) {
    events.retain(|event| delta_item(event) != Some((item_id, kind)));
}

fn compact_deltas(events: &mut Vec<AgentEvent>, item_id: ItemId, kind: DeltaKind) {
    let mut combined = String::new();
    for event in events.iter() {
        if delta_item(event) != Some((item_id, kind)) {
            continue;
        }
        match event {
            AgentEvent::ItemDelta {
                delta: ItemDelta::CommandOutput { chunk },
                ..
            }
            | AgentEvent::ItemDelta {
                delta: ItemDelta::Text { text: chunk },
                ..
            } => combined.push_str(chunk),
            _ => {}
        }
    }
    let max = match kind {
        DeltaKind::Command => MAX_LIVE_COMMAND_OUTPUT,
        DeltaKind::Text => MAX_LIVE_TEXT,
    };
    let Some(index) = events
        .iter()
        .position(|event| delta_item(event) == Some((item_id, kind)))
    else {
        return;
    };
    let (edge, marker) = match kind {
        DeltaKind::Command => (LIVE_COMMAND_OUTPUT_EDGE, LIVE_COMMAND_OUTPUT_TRUNCATED),
        DeltaKind::Text => (LIVE_TEXT_EDGE, LIVE_TEXT_TRUNCATED),
    };
    let compacted = if combined.len() > max {
        compact_head_tail(&combined, edge, marker)
    } else {
        combined
    };
    match events.get_mut(index) {
        Some(AgentEvent::ItemDelta {
            delta: ItemDelta::CommandOutput { chunk },
            ..
        })
        | Some(AgentEvent::ItemDelta {
            delta: ItemDelta::Text { text: chunk },
            ..
        }) => *chunk = compacted,
        _ => return,
    }
    let mut original_index = 0;
    events.retain(|event| {
        let keep = delta_item(event) != Some((item_id, kind)) || original_index == index;
        original_index += 1;
        keep
    });
}

fn completed_command_item_id(event: &AgentEvent) -> Option<ItemId> {
    let AgentEvent::ItemCompleted { item, .. } = event else {
        return None;
    };
    matches!(item.payload, ItemPayload::CommandExecution { .. }).then_some(item.id)
}

fn compact_completed_command_output(mut event: AgentEvent) -> AgentEvent {
    if let AgentEvent::ItemCompleted { item, .. } = &mut event
        && let ItemPayload::CommandExecution { output, .. } = &mut item.payload
    {
        *output = compact_head_tail(
            output,
            LIVE_COMMAND_OUTPUT_EDGE,
            LIVE_COMMAND_OUTPUT_TRUNCATED,
        );
    }
    event
}

fn compact_head_tail(value: &str, edge: usize, marker: &str) -> String {
    if value.len() <= edge.saturating_mul(2) {
        return value.to_string();
    }
    let head_end = floor_char_boundary(value, edge);
    let tail_start = ceil_char_boundary(value, value.len().saturating_sub(edge));
    format!("{}{}{}", &value[..head_end], marker, &value[tail_start..])
}

fn idle_final_snapshot(thread_id: ThreadId) -> ThreadFinalRuntime {
    ThreadFinalRuntime {
        through_seq: 0,
        turn_state: RuntimeTurnState::Idle,
        tasks: ThreadTaskSnapshot {
            thread_id,
            revision: 0,
            tasks: Vec::new(),
        },
        requests: Vec::new(),
    }
}

fn request_kind(kind: RuntimeRequestKind) -> RequestKind {
    match kind {
        RuntimeRequestKind::Approval => RequestKind::Approval,
        RuntimeRequestKind::Server => RequestKind::Server,
    }
}

fn request_state(
    thread_id: ThreadId,
    request_id: &RuntimeRequestId,
    request: &RequestRecord,
) -> RequestState {
    let payload = match &request.payload {
        RuntimeRequestPayload::Approval(request) => RequestPayload::Approval {
            request: request.clone(),
        },
        RuntimeRequestPayload::Server(request) => RequestPayload::Server {
            request: request.clone(),
        },
    };
    let status = match &request.status {
        RuntimeRequestStatus::Pending => RequestStatus::Pending,
        RuntimeRequestStatus::Responding => RequestStatus::Responding,
        RuntimeRequestStatus::Resolved(RuntimeRequestResolution::Approval(decision)) => {
            RequestStatus::Resolved {
                resolution: RequestResolution::Approval {
                    decision: decision.clone(),
                },
            }
        }
        RuntimeRequestStatus::Resolved(
            RuntimeRequestResolution::Server(_) | RuntimeRequestResolution::Harness,
        ) => RequestStatus::Resolved {
            resolution: RequestResolution::Server,
        },
    };
    RequestState {
        thread_id,
        request_id: request_id.as_str().to_string(),
        payload,
        status,
    }
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while index > 0 && !value.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn ceil_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while index < value.len() && !value.is_char_boundary(index) {
        index += 1;
    }
    index
}

fn browser_wire_event(event: AgentEvent, user_input: Option<UserInput>) -> Option<WireAgentEvent> {
    match event {
        AgentEvent::ThreadOpened { .. }
        | AgentEvent::ContextWindowUpdated { .. }
        | AgentEvent::DiffUpdated { .. } => None,
        AgentEvent::TurnStarted { thread, turn } => Some(WireAgentEvent::TurnStarted {
            thread,
            turn,
            user_input,
        }),
        event => WireAgentEvent::from_agent_event(event),
    }
}

fn is_request_event(event: &AgentEvent) -> bool {
    matches!(
        event,
        AgentEvent::ApprovalRequested { .. }
            | AgentEvent::ServerRequestReceived { .. }
            | AgentEvent::ServerRequestResolved { .. }
    )
}

fn request_event_id(event: &AgentEvent) -> Option<RuntimeRequestId> {
    match event {
        AgentEvent::ApprovalRequested { request, .. } => {
            Some(RuntimeRequestId::Approval(request.id.clone()))
        }
        AgentEvent::ServerRequestReceived { request, .. } => {
            Some(RuntimeRequestId::Server(request.id.clone()))
        }
        AgentEvent::ServerRequestResolved { request_id, .. } => {
            Some(RuntimeRequestId::Server(request_id.clone()))
        }
        _ => None,
    }
}

fn encoded_event_bytes(seq: u64, payload: &ThreadEventPayload, fallback: usize) -> usize {
    match serde_json::to_vec(&SequencedThreadEvent {
        seq,
        event: payload.clone(),
    }) {
        Ok(encoded) => encoded.len(),
        Err(error) => {
            error!(
                event_seq = seq,
                action = "measure_runtime_event",
                %error,
                "failed to measure runtime event; charging the journal byte limit"
            );
            fallback
        }
    }
}

fn event_thread_id(event: &AgentEvent) -> ThreadId {
    match event {
        AgentEvent::ThreadOpened { thread, .. }
        | AgentEvent::TurnStarted { thread, .. }
        | AgentEvent::ContextWindowUpdated { thread, .. }
        | AgentEvent::ItemStarted { thread, .. }
        | AgentEvent::ItemDelta { thread, .. }
        | AgentEvent::ItemCompleted { thread, .. }
        | AgentEvent::DiffUpdated { thread, .. }
        | AgentEvent::ApprovalRequested { thread, .. }
        | AgentEvent::ServerRequestReceived { thread, .. }
        | AgentEvent::ServerRequestResolved { thread, .. }
        | AgentEvent::TurnCompleted { thread, .. }
        | AgentEvent::Error { thread, .. }
        | AgentEvent::Notice { thread, .. } => *thread,
    }
}

fn agent_event_turn_id(event: &AgentEvent) -> Option<TurnId> {
    match event {
        AgentEvent::TurnStarted { turn, .. }
        | AgentEvent::ContextWindowUpdated { turn, .. }
        | AgentEvent::ItemStarted { turn, .. }
        | AgentEvent::ItemDelta { turn, .. }
        | AgentEvent::ItemCompleted { turn, .. }
        | AgentEvent::DiffUpdated { turn, .. }
        | AgentEvent::ApprovalRequested { turn, .. }
        | AgentEvent::TurnCompleted { turn, .. } => Some(*turn),
        AgentEvent::ServerRequestReceived { turn, .. }
        | AgentEvent::ServerRequestResolved { turn, .. }
        | AgentEvent::Error { turn, .. }
        | AgentEvent::Notice { turn, .. } => *turn,
        AgentEvent::ThreadOpened { .. } => None,
    }
}

fn thread_event_turn_id(event: &ThreadEventPayload) -> Option<TurnId> {
    let ThreadEventPayload::Agent { agent_event } = event else {
        return None;
    };
    match agent_event.as_ref() {
        WireAgentEvent::TurnStarted { turn, .. }
        | WireAgentEvent::ItemStarted { turn, .. }
        | WireAgentEvent::ItemDelta { turn, .. }
        | WireAgentEvent::ItemCompleted { turn, .. }
        | WireAgentEvent::ApprovalRequested { turn, .. }
        | WireAgentEvent::TurnCompleted { turn, .. } => Some(*turn),
        WireAgentEvent::ServerRequestReceived { turn, .. }
        | WireAgentEvent::ServerRequestResolved { turn, .. }
        | WireAgentEvent::Error { turn, .. }
        | WireAgentEvent::Notice { turn, .. } => *turn,
        WireAgentEvent::ThreadOpened { .. } | WireAgentEvent::DiffUpdated { .. } => None,
    }
}

fn next_nonzero(value: u64) -> u64 {
    value.checked_add(1).filter(|next| *next != 0).unwrap_or(1)
}

fn path_to_display(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn truncate_task_output(output: &mut String) {
    if output.len() <= MAX_TASK_OUTPUT_TAIL {
        return;
    }
    let cutoff = ceil_char_boundary(output, output.len() - MAX_TASK_OUTPUT_TAIL);
    output.drain(..cutoff);
}

fn tool_output_string(output: Option<&serde_json::Value>, error: Option<&str>) -> String {
    if let Some(value) = output {
        match value {
            serde_json::Value::String(value) => value.clone(),
            other => other.to_string(),
        }
    } else {
        error.unwrap_or_default().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use giskard_core::approval::{ApprovalKind, ApprovalRequest};
    use giskard_core::ids::ApprovalId;
    use giskard_core::item::{CommandExecutionStart, ItemKind, ItemStart};
    use giskard_core::token::TokenUsage;
    use giskard_core::turn::{TurnStatus, TurnStatusKind};

    fn owner() -> TurnOwner {
        TurnOwner {
            project_id: ProjectId::new(),
            harness_thread_id: "native".into(),
            mode: Mode::Build,
            model: ModelRef {
                provider: "test".into(),
                model: "test".into(),
                reasoning_effort: None,
            },
            context_kind: "test",
        }
    }

    fn completed_turn(turn_id: TurnId) -> Turn {
        Turn {
            id: turn_id,
            user_input: UserInput::text("persist me"),
            items: Vec::new(),
            model: owner().model,
            mode: Mode::Build,
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
            usage: TokenUsage::default(),
            diffs: Vec::new(),
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
        }
    }

    #[test]
    fn turn_lease_is_exclusive_and_drop_releases_it() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let lease = runtime.reserve(thread, owner()).unwrap();
        assert!(runtime.is_active(thread));
        assert_eq!(runtime.current_overview().threads.len(), 1);
        assert!(matches!(
            runtime.reserve(thread, owner()),
            Err(HarnessError::ThreadBusy { .. })
        ));
        drop(lease);
        assert!(!runtime.is_active(thread));
        assert!(runtime.current_overview().threads.is_empty());
        assert!(runtime.reserve(thread, owner()).is_ok());
    }

    #[test]
    fn persistence_blocked_transfer_retains_turn_until_retry_settles() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let turn_id = TurnId::new();
        let mut lease = runtime.reserve(thread, owner()).unwrap();
        lease.acknowledge_turn(turn_id);
        lease
            .transfer_to_persistence_blocked(completed_turn(turn_id), 3, "disk full".into())
            .unwrap();

        assert!(runtime.is_active(thread));
        let retained = runtime.persistence_blocked(thread).unwrap();
        assert_eq!(retained.turn.id, turn_id);
        assert_eq!(retained.attempts, 3);
        assert!(matches!(
            runtime.final_snapshot(thread).turn_state,
            RuntimeTurnState::PersistenceBlocked { turn_id: id, .. } if id == turn_id
        ));
        assert!(matches!(
            runtime.reserve(thread, owner()),
            Err(HarnessError::ThreadBusy { .. })
        ));

        let settled = runtime.settle_persistence_blocked(thread, turn_id).unwrap();
        assert_eq!(settled.turn.id, turn_id);
        assert!(!runtime.is_active(thread));
        assert!(matches!(
            runtime.final_snapshot(thread).turn_state,
            RuntimeTurnState::Idle
        ));
    }

    #[test]
    fn persistence_blocked_transfer_rejects_a_different_turn() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let acknowledged_turn = TurnId::new();
        let mut lease = runtime.reserve(thread, owner()).unwrap();
        lease.acknowledge_turn(acknowledged_turn);

        assert_eq!(
            lease
                .transfer_to_persistence_blocked(
                    completed_turn(TurnId::new()),
                    1,
                    "disk full".into(),
                )
                .unwrap_err(),
            PersistenceTransitionError::TurnMismatch
        );
        assert!(runtime.is_active(thread));
        assert!(runtime.persistence_blocked(thread).is_none());
        drop(lease);
        assert!(!runtime.is_active(thread));
    }

    #[test]
    fn persistence_recovery_cannot_settle_a_different_blocked_turn() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let turn_id = TurnId::new();
        let mut lease = runtime.reserve(thread, owner()).unwrap();
        lease.acknowledge_turn(turn_id);
        lease
            .transfer_to_persistence_blocked(completed_turn(turn_id), 1, "disk full".into())
            .unwrap();

        let stale_turn = TurnId::new();
        assert_eq!(
            runtime
                .update_persistence_blocked_failure(thread, stale_turn, 2, "still full".into(),)
                .unwrap_err(),
            PersistenceTransitionError::TurnMismatch
        );
        assert_eq!(
            runtime
                .settle_persistence_blocked(thread, stale_turn)
                .unwrap_err(),
            PersistenceTransitionError::TurnMismatch
        );
        assert_eq!(runtime.persistence_blocked(thread).unwrap().attempts, 1);

        let discarded = runtime
            .discard_persistence_blocked(thread, turn_id)
            .unwrap();
        assert_eq!(discarded.turn.id, turn_id);
        assert!(!runtime.is_active(thread));
        assert!(runtime.persistence_blocked(thread).is_none());
    }

    #[test]
    fn metadata_only_event_does_not_allocate_a_sequence() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let result = runtime
            .apply_event(
                thread,
                AgentEvent::ContextWindowUpdated {
                    thread,
                    turn,
                    model: owner().model,
                    context_window: 128_000,
                },
                None,
            )
            .unwrap();
        assert!(result.stream_event.is_none());
        assert!(!runtime.inner.lock_state().threads.contains_key(&thread));
        let result = runtime
            .apply_event(
                thread,
                AgentEvent::Notice {
                    thread,
                    turn: Some(turn),
                    message: "visible".into(),
                },
                None,
            )
            .unwrap();
        assert_eq!(result.stream_event.map(|entry| entry.seq()), Some(1));
    }

    #[test]
    fn live_cut_and_suffix_partition_events_exactly_once() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        runtime
            .ensure_turn_with_user_input(thread, turn, Some(UserInput::text("hi")))
            .unwrap();
        runtime
            .apply_event(
                thread,
                AgentEvent::TurnStarted { thread, turn },
                Some(UserInput::text("hi")),
            )
            .unwrap();
        let cut = runtime.capture_live_cut(thread).unwrap();
        assert_eq!(cut.represented_through, 1);
        runtime
            .apply_event(
                thread,
                AgentEvent::Notice {
                    thread,
                    turn: Some(turn),
                    message: "after".into(),
                },
                None,
            )
            .unwrap();
        let final_cut = runtime.finalize_bootstrap_cut(&cut.pin).unwrap();
        assert_eq!(final_cut.through_seq, 2);
        assert_eq!(final_cut.suffix.len(), 1);
        assert_eq!(final_cut.suffix[0].seq(), 2);
    }

    #[test]
    fn late_old_turn_completion_stays_outside_the_new_live_projection() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let current_turn = TurnId::new();
        let old_turn = TurnId::new();
        runtime
            .ensure_turn_with_user_input(thread, current_turn, Some(UserInput::text("new")))
            .unwrap();
        runtime
            .apply_event(
                thread,
                AgentEvent::TurnStarted {
                    thread,
                    turn: current_turn,
                },
                Some(UserInput::text("new")),
            )
            .unwrap();
        runtime
            .apply_event(
                thread,
                AgentEvent::ItemCompleted {
                    thread,
                    turn: old_turn,
                    item: giskard_core::item::Item {
                        id: ItemId::new(),
                        harness_item_id: "late-command".into(),
                        payload: ItemPayload::CommandExecution {
                            command: "cargo test".into(),
                            cwd: Path::new("/tmp").into(),
                            output: "done".into(),
                            exit_code: Some(0),
                            status: Some("completed".into()),
                            process_id: Some("42".into()),
                            duration_ms: Some(10),
                        },
                        created_at: Utc::now(),
                    },
                },
                None,
            )
            .unwrap();

        let cut = runtime.capture_live_cut(thread).unwrap();
        assert_eq!(cut.represented_through, 1);
        assert_eq!(cut.live.as_ref().unwrap().events.len(), 1);
        let final_cut = runtime.finalize_bootstrap_cut(&cut.pin).unwrap();
        assert_eq!(final_cut.suffix.len(), 1);
        assert_eq!(final_cut.suffix[0].seq(), 2);
        assert!(final_cut.suffix[0].coverage.is_none());
    }

    #[test]
    fn final_cut_rejection_does_not_cross_the_subscription_handoff() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let cut = runtime.capture_live_cut(thread).unwrap();
        let result = runtime.finalize_bootstrap_cut_with(&cut.pin, |_| false);
        assert_eq!(result.unwrap_err(), JournalPinError::PreparationRejected);
    }

    #[test]
    fn a_pin_exhausts_without_allowing_the_journal_to_grow_unbounded() {
        let runtime = ThreadRuntimeRegistry::with_limits(RuntimeLimits {
            journal_entries: 2,
            journal_bytes: usize::MAX,
            pin_suffix_entries: 1,
            pin_suffix_bytes: usize::MAX,
            total_pin_reservation_entries: 1,
            total_pin_reservation_bytes: usize::MAX,
        });
        let thread = ThreadId::new();
        let cut = runtime.capture_live_cut(thread).unwrap();
        for message in ["one", "two", "three"] {
            runtime
                .apply_event(
                    thread,
                    AgentEvent::Notice {
                        thread,
                        turn: None,
                        message: message.into(),
                    },
                    None,
                )
                .unwrap();
        }
        assert_eq!(
            runtime.finalize_bootstrap_cut(&cut.pin).unwrap_err(),
            JournalPinError::Exhausted
        );
        let state = runtime.inner.lock_state();
        assert!(state.threads[&thread].journal.entries.len() <= 2);
    }

    #[test]
    fn pin_rejects_a_missing_suffix_even_when_the_journal_is_empty() {
        let limits = RuntimeLimits {
            journal_entries: 0,
            journal_bytes: usize::MAX,
            ..RuntimeLimits::default()
        };
        let runtime = ThreadRuntimeRegistry::with_limits(limits);
        let thread = ThreadId::new();
        runtime
            .apply_event(
                thread,
                AgentEvent::Notice {
                    thread,
                    turn: None,
                    message: "evicted".into(),
                },
                None,
            )
            .unwrap();

        let mut state = runtime.inner.lock_state();
        let journal = &mut state.threads.get_mut(&thread).unwrap().journal;
        assert!(journal.entries.is_empty());
        assert_eq!(
            journal.install_pin(0, usize::MAX, usize::MAX, limits),
            Err(JournalPinError::SuffixUnavailable)
        );
    }

    #[test]
    fn request_claim_commit_and_rollback_are_authoritative() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let approval_id = ApprovalId("approval".into());
        let received = runtime
            .apply_event(
                thread,
                AgentEvent::ApprovalRequested {
                    thread,
                    turn,
                    request: ApprovalRequest {
                        id: approval_id.clone(),
                        kind: ApprovalKind::Permission {
                            detail: "test".into(),
                        },
                        reason: None,
                        metadata: Vec::new(),
                        available: vec![ApprovalDecision::Accept],
                    },
                },
                None,
            )
            .unwrap();
        let Some(received) = received.stream_event else {
            panic!("request should produce one ordered event");
        };
        assert!(matches!(
            received.event.event,
            ThreadEventPayload::Request { .. }
        ));
        assert_eq!(runtime.final_snapshot(thread).requests.len(), 1);
        let id = RuntimeRequestId::Approval(approval_id);
        let claim = runtime.claim_request(thread, id.clone()).unwrap();
        assert_eq!(
            runtime.claim_request(thread, id.clone()).unwrap_err(),
            RequestClaimError::Responding
        );
        assert_eq!(claim.transition().seq(), 2);
        assert_eq!(claim.rollback().unwrap().seq(), 3);
        let claim = runtime.claim_request(thread, id.clone()).unwrap();
        assert_eq!(claim.transition().seq(), 4);
        let resolved = claim
            .commit(RuntimeRequestResolution::Approval(ApprovalDecision::Accept))
            .unwrap();
        assert_eq!(resolved.seq(), 5);
        assert_eq!(
            runtime.claim_request(thread, id).unwrap_err(),
            RequestClaimError::Resolved
        );
        assert!(runtime.current_overview().threads.is_empty());
    }

    #[test]
    fn server_request_receive_and_resolution_each_emit_one_typed_event() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let request_id = ServerRequestId("request".into());
        runtime
            .ensure_turn_with_user_input(thread, turn, None)
            .unwrap();

        let received = runtime
            .apply_event(
                thread,
                AgentEvent::ServerRequestReceived {
                    thread,
                    turn: Some(turn),
                    request: ServerRequest {
                        id: request_id.clone(),
                        method: "tool/request_user_input".into(),
                        params: serde_json::json!({"message": "Continue?"}),
                        received_at: Utc::now(),
                    },
                },
                None,
            )
            .unwrap()
            .stream_event
            .unwrap();
        let overview = runtime.current_overview();
        assert_eq!(overview.threads.len(), 1);
        assert!(matches!(
            overview.threads[0].turn_state,
            RuntimeTurnState::Idle
        ));
        let resolved = runtime
            .apply_event(
                thread,
                AgentEvent::ServerRequestResolved {
                    thread,
                    turn: Some(turn),
                    request_id,
                },
                None,
            )
            .unwrap()
            .stream_event
            .unwrap();

        assert!(matches!(
            received.event.event,
            ThreadEventPayload::Request { .. }
        ));
        assert!(matches!(
            resolved.event.event,
            ThreadEventPayload::Request { .. }
        ));
        assert_eq!((received.seq(), resolved.seq()), (1, 2));
        let state = runtime.inner.lock_state();
        let journal = &state.threads[&thread].journal.entries;
        assert_eq!(journal.len(), 2);
        assert!(
            journal
                .iter()
                .all(|entry| matches!(entry.event.event, ThreadEventPayload::Request { .. }))
        );
        let live = state.threads[&thread].live_snapshot(thread).unwrap();
        assert_eq!(live.events.len(), 1);
        assert!(matches!(live.events[0], ThreadEventPayload::Request { .. }));
    }

    #[test]
    fn server_request_resolution_before_payload_cannot_resurrect_pending_state() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let request_id = ServerRequestId("resolved-before-payload".into());

        let early_resolution = runtime
            .apply_event(
                thread,
                AgentEvent::ServerRequestResolved {
                    thread,
                    turn: Some(turn),
                    request_id: request_id.clone(),
                },
                None,
            )
            .unwrap();
        assert!(early_resolution.stream_event.is_none());

        let received = runtime
            .apply_event(
                thread,
                AgentEvent::ServerRequestReceived {
                    thread,
                    turn: Some(turn),
                    request: ServerRequest {
                        id: request_id.clone(),
                        method: "tool/request_user_input".into(),
                        params: serde_json::json!({"message": "Continue?"}),
                        received_at: Utc::now(),
                    },
                },
                None,
            )
            .unwrap()
            .stream_event
            .unwrap();
        assert!(matches!(
            received.event.event,
            ThreadEventPayload::Request { request }
                if matches!(request.status, RequestStatus::Resolved { .. })
        ));
        assert_eq!(
            runtime
                .claim_request(thread, RuntimeRequestId::Server(request_id))
                .unwrap_err(),
            RequestClaimError::Resolved
        );
    }

    #[test]
    fn resolving_one_request_preserves_another_pending_request() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let first = ApprovalId("first".into());
        let second = ApprovalId("second".into());
        for id in [&first, &second] {
            runtime
                .apply_event(
                    thread,
                    AgentEvent::ApprovalRequested {
                        thread,
                        turn,
                        request: ApprovalRequest {
                            id: id.clone(),
                            kind: ApprovalKind::Permission {
                                detail: "test".into(),
                            },
                            reason: None,
                            metadata: Vec::new(),
                            available: vec![ApprovalDecision::Accept],
                        },
                    },
                    None,
                )
                .unwrap();
        }

        runtime
            .claim_request(thread, RuntimeRequestId::Approval(first))
            .unwrap()
            .commit(RuntimeRequestResolution::Approval(ApprovalDecision::Accept))
            .unwrap();

        let snapshot = runtime.final_snapshot(thread);
        assert_eq!(snapshot.requests.len(), 2);
        assert_eq!(
            runtime.current_overview().threads[0]
                .outstanding_requests
                .len(),
            1
        );
        assert_eq!(
            runtime
                .claim_request(thread, RuntimeRequestId::Approval(second))
                .unwrap()
                .transition()
                .seq(),
            5
        );
    }

    #[test]
    fn task_changes_advance_one_revision_per_changed_event() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();
        let started = runtime
            .apply_event(
                thread,
                AgentEvent::ItemStarted {
                    thread,
                    turn,
                    item: ItemStart {
                        id: item_id,
                        harness_item_id: "command".into(),
                        kind: ItemKind::CommandExecution,
                        command: Some(CommandExecutionStart {
                            command: "cargo test".into(),
                            cwd: "/tmp".into(),
                            status: Some("in_progress".into()),
                            process_id: Some("process".into()),
                            started_at_ms: Some(1),
                        }),
                        tool: None,
                    },
                },
                None,
            )
            .unwrap();
        assert_eq!(
            started.running_tasks_if_changed.map(|tasks| tasks.revision),
            Some(1)
        );
        let output = runtime
            .apply_event(
                thread,
                AgentEvent::ItemDelta {
                    thread,
                    turn,
                    item_id,
                    delta: ItemDelta::CommandOutput {
                        chunk: "building".into(),
                    },
                },
                None,
            )
            .unwrap();
        let Some(tasks) = output.running_tasks_if_changed else {
            panic!("task output should change the task snapshot");
        };
        assert_eq!(tasks.revision, 2);
        assert_eq!(tasks.tasks[0].output, "building");
    }

    #[test]
    fn native_item_events_remain_available_for_link_resolution() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();
        runtime
            .ensure_turn_with_user_input(thread, turn, None)
            .unwrap();
        runtime
            .apply_event(
                thread,
                AgentEvent::ItemStarted {
                    thread,
                    turn,
                    item: ItemStart {
                        id: item_id,
                        harness_item_id: "native-item".into(),
                        kind: ItemKind::AgentMessage,
                        command: None,
                        tool: None,
                    },
                },
                None,
            )
            .unwrap();
        let events = runtime.item_events(thread, item_id);
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            AgentEvent::ItemStarted { item, .. } if item.harness_item_id == "native-item"
        ));
    }

    #[test]
    fn coverage_is_applied_only_to_events_present_at_persist_time() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        runtime
            .apply_event(
                thread,
                AgentEvent::Notice {
                    thread,
                    turn: Some(turn),
                    message: "before".into(),
                },
                None,
            )
            .unwrap();
        runtime.mark_turn_persisted(thread, turn);
        runtime
            .apply_event(
                thread,
                AgentEvent::Notice {
                    thread,
                    turn: Some(turn),
                    message: "late".into(),
                },
                None,
            )
            .unwrap();
        let state = runtime.inner.lock_state();
        let entries = &state.threads[&thread].journal.entries;
        assert_eq!(entries[0].coverage, Some(JournalCoverage::Turn(turn)));
        assert_eq!(entries[1].coverage, None);
    }

    #[test]
    fn idle_runtime_is_not_retired_while_a_bootstrap_pin_exists() {
        let runtime = ThreadRuntimeRegistry::new();
        let thread = ThreadId::new();
        let cut = runtime.capture_live_cut(thread).unwrap();

        assert!(!runtime.retire_if_idle(thread));
        drop(cut);
        assert!(runtime.retire_if_idle(thread));
        assert_eq!(runtime.final_snapshot(thread).through_seq, 0);
    }
}
