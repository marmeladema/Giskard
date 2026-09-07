use super::*;

#[cfg(not(test))]
const WORKER_QUEUE_WARN_AFTER: Duration = Duration::from_secs(10);
#[cfg(test)]
const WORKER_QUEUE_WARN_AFTER: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerQueueKind {
    Command,
    Control,
}

impl WorkerQueueKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Command => "command",
            Self::Control => "control",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct WorkerQueueToken {
    id: u64,
    kind: WorkerQueueKind,
    action: &'static str,
    project_id: Option<ProjectId>,
    thread_id: Option<ThreadId>,
    enqueued_at: Instant,
}

#[derive(Debug, Clone)]
struct WorkerQueueEntrySnapshot {
    id: u64,
    kind: WorkerQueueKind,
    action: &'static str,
    project_id: Option<ProjectId>,
    thread_id: Option<ThreadId>,
    elapsed_ms: u128,
}

#[derive(Debug, Clone)]
struct WorkerQueueSnapshot {
    active: Option<WorkerQueueEntrySnapshot>,
    oldest_pending: Option<WorkerQueueEntrySnapshot>,
    command_pending: usize,
    control_pending: usize,
}

#[derive(Debug)]
struct WorkerQueueState {
    next_id: u64,
    pending: HashMap<u64, WorkerQueueToken>,
    active: Option<WorkerQueueToken>,
    closed: bool,
}

#[derive(Debug)]
pub(crate) struct WorkerQueueWatchdog {
    state: StdMutex<WorkerQueueState>,
}

impl WorkerQueueWatchdog {
    pub(crate) fn new() -> Self {
        Self {
            state: StdMutex::new(WorkerQueueState {
                next_id: 1,
                pending: HashMap::new(),
                active: None,
                closed: false,
            }),
        }
    }

    pub(crate) fn enqueue(
        &self,
        kind: WorkerQueueKind,
        action: &'static str,
        project_id: Option<ProjectId>,
        thread_id: Option<ThreadId>,
    ) -> WorkerQueueToken {
        let mut state = self.lock_state();
        let token = WorkerQueueToken {
            id: state.next_id,
            kind,
            action,
            project_id,
            thread_id,
            enqueued_at: Instant::now(),
        };
        state.next_id = state.next_id.saturating_add(1);
        state.pending.insert(token.id, token);
        token
    }

    pub(crate) fn cancel(&self, token: WorkerQueueToken) {
        self.lock_state().pending.remove(&token.id);
    }

    pub(crate) fn mark_started(&self, token: WorkerQueueToken) {
        let mut state = self.lock_state();
        state.pending.remove(&token.id);
        state.active = Some(token);
    }

    pub(crate) fn mark_finished(&self, token: WorkerQueueToken) {
        let mut state = self.lock_state();
        if state.active.is_some_and(|active| active.id == token.id) {
            state.active = None;
        }
    }

    pub(crate) fn close(&self) {
        self.lock_state().closed = true;
    }

    fn snapshot(&self) -> WorkerQueueSnapshot {
        let state = self.lock_state();
        let now = Instant::now();
        let mut command_pending = 0;
        let mut control_pending = 0;
        let mut oldest_pending: Option<WorkerQueueToken> = None;
        for token in state.pending.values().copied() {
            match token.kind {
                WorkerQueueKind::Command => command_pending += 1,
                WorkerQueueKind::Control => control_pending += 1,
            }
            if oldest_pending.is_none_or(|oldest| token.enqueued_at < oldest.enqueued_at) {
                oldest_pending = Some(token);
            }
        }

        WorkerQueueSnapshot {
            active: state.active.map(|token| snapshot_queue_token(token, now)),
            oldest_pending: oldest_pending.map(|token| snapshot_queue_token(token, now)),
            command_pending,
            control_pending,
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.lock_state().closed
    }

    fn lock_state(&self) -> StdMutexGuard<'_, WorkerQueueState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("Codex worker queue watchdog lock was poisoned; recovering state");
                poisoned.into_inner()
            }
        }
    }
}

fn snapshot_queue_token(token: WorkerQueueToken, now: Instant) -> WorkerQueueEntrySnapshot {
    WorkerQueueEntrySnapshot {
        id: token.id,
        kind: token.kind,
        action: token.action,
        project_id: token.project_id,
        thread_id: token.thread_id,
        elapsed_ms: now.duration_since(token.enqueued_at).as_millis(),
    }
}

pub(crate) async fn run_worker_queue_watchdog(watchdog: Weak<WorkerQueueWatchdog>) {
    let mut tick = tokio::time::interval(WORKER_QUEUE_WARN_AFTER);
    loop {
        tick.tick().await;
        let Some(watchdog) = watchdog.upgrade() else {
            break;
        };
        if watchdog.is_closed() {
            break;
        }
        let snapshot = watchdog.snapshot();
        let active_is_slow = snapshot
            .active
            .as_ref()
            .is_some_and(|active| active.elapsed_ms >= WORKER_QUEUE_WARN_AFTER.as_millis());
        let pending_is_slow = snapshot
            .oldest_pending
            .as_ref()
            .is_some_and(|pending| pending.elapsed_ms >= WORKER_QUEUE_WARN_AFTER.as_millis());
        if active_is_slow || pending_is_slow {
            warn!(
                active_id = display_opt(snapshot.active.as_ref().map(|entry| entry.id)),
                active_kind =
                    display_opt(snapshot.active.as_ref().map(|entry| entry.kind.as_str())),
                active_action = display_opt(snapshot.active.as_ref().map(|entry| entry.action)),
                active_project_id =
                    display_opt(snapshot.active.as_ref().and_then(|entry| entry.project_id)),
                active_thread_id =
                    display_opt(snapshot.active.as_ref().and_then(|entry| entry.thread_id)),
                active_elapsed_ms =
                    display_opt(snapshot.active.as_ref().map(|entry| entry.elapsed_ms)),
                oldest_pending_id =
                    display_opt(snapshot.oldest_pending.as_ref().map(|entry| entry.id)),
                oldest_pending_kind = display_opt(
                    snapshot
                        .oldest_pending
                        .as_ref()
                        .map(|entry| entry.kind.as_str())
                ),
                oldest_pending_action =
                    display_opt(snapshot.oldest_pending.as_ref().map(|entry| entry.action)),
                oldest_pending_project_id = display_opt(
                    snapshot
                        .oldest_pending
                        .as_ref()
                        .and_then(|entry| entry.project_id)
                ),
                oldest_pending_thread_id = display_opt(
                    snapshot
                        .oldest_pending
                        .as_ref()
                        .and_then(|entry| entry.thread_id)
                ),
                oldest_pending_elapsed_ms = display_opt(
                    snapshot
                        .oldest_pending
                        .as_ref()
                        .map(|entry| entry.elapsed_ms)
                ),
                command_pending = snapshot.command_pending,
                control_pending = snapshot.control_pending,
                "Codex worker queue has slow active or pending work"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_queue_snapshot_preserves_operation_identity() {
        let watchdog = WorkerQueueWatchdog::new();
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let token = watchdog.enqueue(
            WorkerQueueKind::Command,
            "open_thread",
            Some(project_id),
            Some(thread_id),
        );
        watchdog.mark_started(token);

        let active = watchdog.snapshot().active.expect("active queue entry");
        assert_eq!(active.project_id, Some(project_id));
        assert_eq!(active.thread_id, Some(thread_id));
        assert_eq!(active.action, "open_thread");
    }
}
