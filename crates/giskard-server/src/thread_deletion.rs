//! Background thread deletion.
//!
//! Deleting a thread cascades to every sub-agent under it, and a long-lived thread can own
//! more than a thousand. Each one costs a native delete, a directory removal and a couple of
//! broadcasts, so the cascade is not something an HTTP request should wait on: the browser
//! would have to guess a timeout, and aborting the request would abort the handler with it,
//! half way through. Instead `DELETE` runs the preflight and answers `202 Accepted`; the cascade
//! runs here, owned by the process, and reports its count over the WebSocket.
//!
//! Three properties make the partial states harmless:
//!
//! - The subtree is deleted leaf-first and the cascade stops at the first failure, so between
//!   any two threads the root and every ancestor of an undeleted thread still exist. Nothing is
//!   ever orphaned, and the same request retried deletes what is left.
//! - Every step is bounded. Git and Codex calls carry their own timeouts; the store removal and
//!   the harness stop get one here. A stuck disk cannot finish the job, but it also cannot hold
//!   the project's lifecycle lock forever: the job fails, names the thread it stuck on, and lets
//!   go.
//! - The user can cancel between threads, which is always a safe point by the first property.
//!
//! The record of a deletion lives only in this process. It is claimed by a [`DeletionReservation`]
//! before the subtree is retired and released by the cascade's terminal state; both are guards,
//! so no early return, `?`, or panic can leave a root fenced with nothing working on it.
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use giskard_core::ids::{ProjectId, ThreadId};
use giskard_persist::PersistStore;
use giskard_persist::store::{ProjectConfig, ThreadFile, ThreadGitWorkspace};
use giskard_proto::{ThreadDeletionProgress, ThreadDeletionState};
use tokio::sync::OwnedMutexGuard;
use tracing::{Instrument, error, info, warn};

use crate::hub::Hub;
use crate::registry::HarnessRegistry;

/// How long one thread's harness delete or store removal may take before the cascade gives up.
/// Generous for a healthy disk and a healthy harness, since both finish in milliseconds; it only
/// ever fires on a hung filesystem or a child that will not stop.
pub const DEFAULT_STEP_TIMEOUT: Duration = Duration::from_secs(30);

/// A catalog invalidation makes every connected tab refetch the project's whole thread list, so
/// the cascade batches them: at most one per `CATALOG_INVALIDATION_EVERY` deleted threads or per
/// `CATALOG_INVALIDATION_INTERVAL`, whichever comes first, plus one when it stops.
const CATALOG_INVALIDATION_EVERY: usize = 50;
const CATALOG_INVALIDATION_INTERVAL: Duration = Duration::from_millis(500);

/// Everything the preflight established, handed to the background task so it never has to
/// re-derive a decision the request already made.
pub(crate) struct DeletionPlan {
    pub project: ProjectConfig,
    pub root: ThreadId,
    /// The subtree's thread files, leaf-first; the root is last. Only these are kept alive for
    /// the cascade, not the project graph they were cut from.
    pub files: Vec<ThreadFile>,
    pub force: bool,
    /// Held for the whole cascade so no other lifecycle operation on the project interleaves
    /// with it. Dropped when the task ends, however it ends.
    pub lifecycle_guard: OwnedMutexGuard<()>,
}

struct DeletionJob {
    progress: ThreadDeletionProgress,
    cancel: Arc<AtomicBool>,
}

#[derive(Default)]
struct Jobs {
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Track the one deletion in flight, or the last one that failed, per root thread.
    // Source of truth: The progress is produced by the task below and nowhere else.
    // Structural reason: A deletion outlives the request that started it and spans a subtree;
    //   it is owned by the process, not by the thread it is removing.
    // Synchronization: The service mutex protects both maps together; no await is held under it.
    // Invalidation/removal: A finished deletion removes its entry (the thread is gone). A failed
    //   or cancelled one keeps its record until the next attempt on the same root replaces it,
    //   the user dismisses it, or the project is deleted. A reservation or a task that ends
    //   without reaching a terminal state is released by its guard's `Drop`.
    by_root: HashMap<ThreadId, DeletionJob>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Answer "is this thread inside a running deletion" for every thread in the subtree.
    // Source of truth: The plan's leaf-first order.
    // Structural reason: Mutations and opens must be refused for descendants too, and a
    //   descendant does not know its root.
    // Synchronization: The service mutex, together with `by_root`.
    // Invalidation/removal: Cleared when the deletion reaches a terminal state or its guard drops.
    members: HashMap<ThreadId, ThreadId>,
}

pub struct ThreadDeletionService {
    /// A `std` mutex on purpose: every critical section is a few map operations with no await,
    /// and the guards below need to release their claim from `Drop`.
    jobs: Mutex<Jobs>,
    /// Milliseconds; a test knob, read once per cascade.
    step_timeout_ms: AtomicU64,
}

impl Default for ThreadDeletionService {
    fn default() -> Self {
        Self::new()
    }
}

/// What [`ThreadDeletionService::cancel`] found under the root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The cascade will stop after the thread it is on.
    Requested,
    /// The record is already terminal; cancelling it would only hide why it stopped.
    NotRunning,
    NotFound,
}

/// What [`ThreadDeletionService::dismiss`] found under the root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DismissOutcome {
    Dismissed,
    /// A running deletion is cancelled, not dismissed.
    StillRunning,
    NotFound,
}

/// The claim on a subtree between the preflight and the cascade. Dropping it without
/// [`Self::consume`] releases the claim, so a refused or failed preflight can never leave the
/// root fenced with nothing working on it.
pub(crate) struct DeletionReservation {
    service: Arc<ThreadDeletionService>,
    root: ThreadId,
    consumed: bool,
}

impl DeletionReservation {
    fn consume(mut self) {
        self.consumed = true;
    }
}

impl Drop for DeletionReservation {
    fn drop(&mut self) {
        if self.consumed {
            return;
        }
        let mut jobs = self.service.lock_jobs();
        jobs.members.retain(|_, job_root| *job_root != self.root);
        jobs.by_root.remove(&self.root);
    }
}

/// Held by the cascade task. If the task ends without recording a terminal state — a panic, or
/// the runtime tearing it down — the record is marked `Failed` and the fence released, so the
/// project and the subtree are not held by a job that no longer exists.
struct RunGuard {
    service: Arc<ThreadDeletionService>,
    root: ThreadId,
    finished: bool,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut jobs = self.service.lock_jobs();
        jobs.members.retain(|_, job_root| *job_root != self.root);
        if let Some(job) = jobs.by_root.get_mut(&self.root) {
            error!(
                project_id = %job.progress.project_id,
                thread_id = %self.root,
                deleted = job.progress.deleted,
                total = job.progress.total,
                action = "delete_thread",
                "thread deletion task ended without a terminal state; recording it as failed"
            );
            job.progress.state = ThreadDeletionState::Failed;
            job.progress.error = Some(format!(
                "the deletion task ended unexpectedly after deleting {} of {} thread(s)",
                job.progress.deleted, job.progress.total
            ));
        }
    }
}

impl ThreadDeletionService {
    pub fn new() -> Self {
        Self {
            jobs: Mutex::new(Jobs::default()),
            step_timeout_ms: AtomicU64::new(DEFAULT_STEP_TIMEOUT.as_millis() as u64),
        }
    }

    /// Bound for one thread's harness delete and store removal. Tests shorten it to exercise the
    /// stuck-step failure without waiting out the production value.
    pub fn set_step_timeout(&self, timeout: Duration) {
        self.step_timeout_ms
            .store(timeout.as_millis() as u64, Ordering::Relaxed);
    }

    fn step_timeout(&self) -> Duration {
        Duration::from_millis(self.step_timeout_ms.load(Ordering::Relaxed))
    }

    fn lock_jobs(&self) -> MutexGuard<'_, Jobs> {
        match self.jobs.lock() {
            Ok(jobs) => jobs,
            Err(poisoned) => {
                error!(
                    action = "lock_thread_deletions",
                    "thread deletion registry lock was poisoned; recovering its state"
                );
                poisoned.into_inner()
            }
        }
    }

    /// The root of the running deletion this thread belongs to, if any.
    pub fn deleting_root(&self, thread_id: ThreadId) -> Option<ThreadId> {
        self.lock_jobs().members.get(&thread_id).copied()
    }

    /// Whether any deletion is running in this project. The cascade holds the project's lifecycle
    /// lock, so a second one could only wait on the first; refusing up front says why.
    pub fn project_has_running(&self, project_id: ProjectId) -> bool {
        self.lock_jobs().by_root.values().any(|job| {
            job.progress.project_id == project_id
                && job.progress.state == ThreadDeletionState::Running
        })
    }

    pub fn progress(&self, root: ThreadId) -> Option<ThreadDeletionProgress> {
        self.lock_jobs()
            .by_root
            .get(&root)
            .map(|job| job.progress.clone())
    }

    /// Every running or failed deletion in the project, for the thread list to carry.
    pub fn project_progress(
        &self,
        project_id: ProjectId,
    ) -> HashMap<ThreadId, ThreadDeletionProgress> {
        self.lock_jobs()
            .by_root
            .values()
            .filter(|job| job.progress.project_id == project_id)
            .map(|job| (job.progress.thread_id, job.progress.clone()))
            .collect()
    }

    /// Ask a running deletion to stop at its next safe point. A terminal record is left alone:
    /// cancelling it would discard the reason it stopped, which is what `dismiss` is for.
    pub fn cancel(&self, root: ThreadId) -> CancelOutcome {
        let jobs = self.lock_jobs();
        let Some(job) = jobs.by_root.get(&root) else {
            return CancelOutcome::NotFound;
        };
        if job.progress.state != ThreadDeletionState::Running {
            return CancelOutcome::NotRunning;
        }
        info!(
            project_id = %job.progress.project_id,
            thread_id = %root,
            deleted = job.progress.deleted,
            total = job.progress.total,
            action = "cancel_thread_deletion",
            "cancellation requested; the cascade stops after the current thread"
        );
        job.cancel.store(true, Ordering::SeqCst);
        CancelOutcome::Requested
    }

    /// Forget a failed or cancelled deletion's record, so its root reads as an ordinary thread.
    pub fn dismiss(&self, root: ThreadId) -> DismissOutcome {
        let mut jobs = self.lock_jobs();
        match jobs.by_root.get(&root) {
            None => DismissOutcome::NotFound,
            Some(job) if job.progress.state == ThreadDeletionState::Running => {
                DismissOutcome::StillRunning
            }
            Some(_) => {
                jobs.by_root.remove(&root);
                DismissOutcome::Dismissed
            }
        }
    }

    /// Drop every record for a project that no longer exists.
    pub fn forget_project(&self, project_id: ProjectId) {
        let mut jobs = self.lock_jobs();
        let Jobs { by_root, members } = &mut *jobs;
        by_root.retain(|_, job| job.progress.project_id != project_id);
        members.retain(|_, root| by_root.contains_key(root));
    }

    /// Claim the subtree before anything is retired, so no open or subscribe can slip in between
    /// retirement and the cascade. Refuses when another deletion is running in the project: the
    /// caller holds the lifecycle lock, so this is the authoritative check, not the fast path.
    /// The record is `Running` with nothing deleted until [`Self::start`] consumes the
    /// reservation; dropping it instead releases the claim.
    pub(crate) fn reserve(
        self: &Arc<Self>,
        project_id: ProjectId,
        root: ThreadId,
        members: &[ThreadId],
    ) -> Result<DeletionReservation, ThreadDeletionProgress> {
        let mut jobs = self.lock_jobs();
        if let Some(running) = jobs.by_root.values().find(|job| {
            job.progress.project_id == project_id
                && job.progress.state == ThreadDeletionState::Running
        }) {
            return Err(running.progress.clone());
        }
        // A retry replaces the previous attempt's failure record outright.
        jobs.by_root.insert(
            root,
            DeletionJob {
                progress: ThreadDeletionProgress {
                    project_id,
                    thread_id: root,
                    state: ThreadDeletionState::Running,
                    deleted: 0,
                    total: members.len(),
                    error: None,
                },
                cancel: Arc::new(AtomicBool::new(false)),
            },
        );
        for thread_id in members {
            jobs.members.insert(*thread_id, root);
        }
        Ok(DeletionReservation {
            service: Arc::clone(self),
            root,
            consumed: false,
        })
    }

    /// Spawn the cascade for a reserved root. The caller has retired every thread in
    /// `plan.files`; nothing here can refuse. The plan's subtree may be larger than the
    /// reservation's if a sub-agent was admitted in between, so membership is topped up.
    ///
    /// The task is registered with the registry's background tasks so shutdown waits for it to
    /// reach a safe point; when the registry is already shutting down, the cascade is recorded
    /// as cancelled before it deletes anything.
    pub(crate) async fn start(
        self: &Arc<Self>,
        reservation: DeletionReservation,
        plan: DeletionPlan,
        store: Arc<PersistStore>,
        hub: Arc<Hub>,
        registry: Arc<HarnessRegistry>,
    ) -> ThreadDeletionProgress {
        debug_assert_eq!(reservation.root, plan.root);
        let (progress, cancel) = {
            let mut jobs = self.lock_jobs();
            for file in &plan.files {
                jobs.members.insert(file.id, plan.root);
            }
            let job = jobs
                .by_root
                .entry(plan.root)
                .or_insert_with(|| DeletionJob {
                    progress: ThreadDeletionProgress {
                        project_id: plan.project.id,
                        thread_id: plan.root,
                        state: ThreadDeletionState::Running,
                        deleted: 0,
                        total: 0,
                        error: None,
                    },
                    cancel: Arc::new(AtomicBool::new(false)),
                });
            job.progress.total = plan.files.len();
            (job.progress.clone(), job.cancel.clone())
        };
        reservation.consume();
        let guard = RunGuard {
            service: Arc::clone(self),
            root: plan.root,
            finished: false,
        };
        let Some(permit) = registry.register_background_task() else {
            warn!(
                project_id = %plan.project.id,
                thread_id = %plan.root,
                total = plan.files.len(),
                action = "start_thread_deletion",
                reason = "registry_shutting_down",
                "not starting thread deletion"
            );
            let message = "the server is shutting down; nothing was deleted".to_string();
            let recorded = self
                .finish(
                    guard,
                    ThreadDeletionState::Cancelled,
                    0,
                    Some(message.clone()),
                    &hub,
                    &registry,
                )
                .await;
            drop(plan.lifecycle_guard);
            // The record is always there this early; the fallback only keeps the response
            // truthful if it is not.
            return recorded.unwrap_or(ThreadDeletionProgress {
                state: ThreadDeletionState::Cancelled,
                error: Some(message),
                ..progress
            });
        };
        info!(
            project_id = %plan.project.id,
            thread_id = %plan.root,
            total = plan.files.len(),
            action = "start_thread_deletion",
            "deleting thread subtree in the background, leaf-first"
        );
        hub.publish_thread_deletion_progress(progress.clone()).await;
        let service = Arc::clone(self);
        tokio::spawn(
            async move {
                let _permit = permit;
                service.run(guard, plan, cancel, store, hub, registry).await;
            }
            .in_current_span(),
        );
        progress
    }

    async fn run(
        self: Arc<Self>,
        guard: RunGuard,
        plan: DeletionPlan,
        cancel: Arc<AtomicBool>,
        store: Arc<PersistStore>,
        hub: Arc<Hub>,
        registry: Arc<HarnessRegistry>,
    ) {
        let DeletionPlan {
            project,
            root,
            files,
            force,
            lifecycle_guard,
        } = plan;
        let project_id = project.id;
        let total = files.len();
        let step_timeout = self.step_timeout();
        let started = Instant::now();
        let mut deleted = 0usize;
        let mut last_invalidation = Instant::now();
        let mut invalidation_backlog = 0usize;
        let mut root_removal_still_running = false;
        let mut outcome: Result<(), String> = Ok(());

        for thread_file in &files {
            if cancel.load(Ordering::SeqCst) {
                self.finish(
                    guard,
                    ThreadDeletionState::Cancelled,
                    deleted,
                    Some(format!(
                        "cancelled after deleting {deleted} of {total} thread(s)"
                    )),
                    &hub,
                    &registry,
                )
                .await;
                hub.invalidate_thread_catalog(project_id).await;
                drop(lifecycle_guard);
                return;
            }
            if registry.is_shutting_down().await {
                info!(
                    %project_id,
                    thread_id = %root,
                    deleted,
                    total,
                    action = "delete_thread",
                    "server shutting down; stopping the thread deletion between threads"
                );
                self.finish(
                    guard,
                    ThreadDeletionState::Cancelled,
                    deleted,
                    Some(format!(
                        "the server is shutting down; {deleted} of {total} thread(s) deleted"
                    )),
                    &hub,
                    &registry,
                )
                .await;
                drop(lifecycle_guard);
                return;
            }
            match delete_one(
                &store,
                &registry,
                &project,
                thread_file,
                root,
                deleted,
                total,
                force,
                step_timeout,
                &hub,
            )
            .await
            {
                Ok(StepOutcome::Deleted) => {}
                // The thread is past the point of no return; its files are still being removed
                // by a task this cascade no longer waits on. It counts as deleted, but nothing
                // more is started on a disk that slow — unless it was the last one, in which
                // case the cascade has nothing left to do.
                Ok(StepOutcome::RemovalStillRunning) if deleted + 1 == total => {
                    root_removal_still_running = true;
                }
                Ok(StepOutcome::RemovalStillRunning) => {
                    deleted += 1;
                    outcome = Err(format!(
                        "the files of thread {} are still being removed after {} seconds; \
                         {deleted} of {total} thread(s) deleted. Retry once the disk catches up.",
                        thread_file.id,
                        step_timeout.as_secs()
                    ));
                    break;
                }
                Err(message) => {
                    outcome = Err(message);
                    break;
                }
            }
            deleted += 1;
            invalidation_backlog += 1;
            self.publish_running(root, deleted, &hub).await;
            if invalidation_backlog >= CATALOG_INVALIDATION_EVERY
                || last_invalidation.elapsed() >= CATALOG_INVALIDATION_INTERVAL
            {
                hub.invalidate_thread_catalog(project_id).await;
                invalidation_backlog = 0;
                last_invalidation = Instant::now();
            }
        }

        match outcome {
            Ok(()) => {
                info!(
                    %project_id,
                    thread_id = %root,
                    deleted,
                    total,
                    elapsed_ms = started.elapsed().as_millis(),
                    root_removal_still_running,
                    action = "delete_thread",
                    "deleted thread subtree"
                );
                self.finish(
                    guard,
                    ThreadDeletionState::Done,
                    deleted,
                    None,
                    &hub,
                    &registry,
                )
                .await;
            }
            Err(message) => {
                // Shutdown stops harnesses before this task is drained, so a step caught by it
                // fails with a harness error; name the real cause rather than the symptom.
                let (state, message) = if registry.is_shutting_down().await {
                    info!(
                        %project_id,
                        thread_id = %root,
                        deleted,
                        total,
                        action = "delete_thread",
                        "thread deletion step failed during server shutdown; recording it as \
                         cancelled"
                    );
                    (
                        ThreadDeletionState::Cancelled,
                        format!(
                            "the server is shutting down; {deleted} of {total} thread(s) deleted"
                        ),
                    )
                } else {
                    (ThreadDeletionState::Failed, message)
                };
                self.finish(guard, state, deleted, Some(message), &hub, &registry)
                    .await;
            }
        }
        hub.invalidate_thread_catalog(project_id).await;
        drop(lifecycle_guard);
    }

    async fn publish_running(&self, root: ThreadId, deleted: usize, hub: &Hub) {
        let progress = {
            let mut jobs = self.lock_jobs();
            let Some(job) = jobs.by_root.get_mut(&root) else {
                return;
            };
            job.progress.deleted = deleted;
            job.progress.clone()
        };
        hub.publish_thread_deletion_progress(progress).await;
    }

    /// Record the terminal state, release the subtree for other operations, and broadcast once.
    /// `None` when the record is already gone (the project was forgotten under the cascade);
    /// nothing is published then, since there is no project to publish it for.
    async fn finish(
        &self,
        mut guard: RunGuard,
        state: ThreadDeletionState,
        deleted: usize,
        error: Option<String>,
        hub: &Hub,
        registry: &HarnessRegistry,
    ) -> Option<ThreadDeletionProgress> {
        let root = guard.root;
        guard.finished = true;
        let progress = {
            let mut jobs = self.lock_jobs();
            jobs.members.retain(|_, job_root| *job_root != root);
            let Some(job) = jobs.by_root.get_mut(&root) else {
                error!(
                    thread_id = %root,
                    ?state,
                    deleted,
                    action = "delete_thread",
                    "thread deletion finished but its record was already gone; nothing published"
                );
                return None;
            };
            job.progress.state = state;
            job.progress.deleted = deleted;
            job.progress.error = error;
            let progress = job.progress.clone();
            if state == ThreadDeletionState::Done {
                jobs.by_root.remove(&root);
            }
            progress
        };
        // Retiring happened before the cascade started and again per deleted thread; the overview
        // is published once here rather than once per thread.
        registry.publish_runtime_overview().await;
        hub.publish_thread_deletion_progress(progress.clone()).await;
        Some(progress)
    }
}

enum StepOutcome {
    Deleted,
    /// The store removal outran its bound but keeps running to completion on its own task; the
    /// thread is, or is about to be, out of enumeration and cannot be "kept".
    RemovalStillRunning,
}

/// Delete one thread: its worktree, its native thread, then its files. A failure before the
/// files are touched keeps the thread so the deletion can be retried, and says so.
#[allow(clippy::too_many_arguments)]
async fn delete_one(
    store: &Arc<PersistStore>,
    registry: &HarnessRegistry,
    project: &ProjectConfig,
    thread_file: &ThreadFile,
    root: ThreadId,
    deleted: usize,
    total: usize,
    force: bool,
    step_timeout: Duration,
    hub: &Arc<Hub>,
) -> Result<StepOutcome, String> {
    let project_id = project.id;
    let candidate = thread_file.id;
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
        && let Err(error) = crate::routes::remove_worktree_for_deleted_thread(
            worktree,
            candidate,
            "delete_thread",
            force,
        )
        .await
        && !force
    {
        return Err(format!(
            "could not finish removing worktree {} at {}: {error}. The thread was kept so this \
             can be retried — some of the cleanup may already have run. Deleting it anyway \
             leaves whatever remains of the checkout and branch behind.",
            worktree.branch, worktree.path
        ));
    }
    // The bound covers the whole registry call, not only the harness's own delete: for a thread
    // whose harness is not loaded, that includes starting the harness process. Dropping the
    // future at any point in it leaves the thread file untouched, so the thread really is kept,
    // and both harnesses' deletes are idempotent on the retry.
    let native = tokio::time::timeout(
        step_timeout,
        registry.delete_thread_quietly(
            project,
            candidate,
            &thread_file.harness,
            thread_file.harness_thread_id.clone(),
        ),
    )
    .await;
    match native {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            error!(
                %project_id,
                root_thread_id = %root,
                thread_id = %candidate,
                harness_thread_id = %thread_file.harness_thread_id,
                deleted,
                total_count = total,
                action = "delete_thread",
                %error,
                "failed to delete native thread while cascading thread deletion"
            );
            return Err(format!(
                "failed to delete thread subtree after deleting {deleted} thread(s): {error}"
            ));
        }
        Err(_) => {
            return Err(stuck(
                project_id,
                root,
                candidate,
                deleted,
                total,
                "the harness did not finish deleting",
                step_timeout,
            ));
        }
    }
    // The store renames the directory out of enumeration and then removes it; abandoning that
    // half way would leave a thread that is listed nowhere and cleaned up by nobody until the
    // next start. So the removal runs on its own task, which the cascade waits on with the bound
    // but never cancels. The cascade's own batching covers the catalog in the ordinary case; only
    // a removal that lands after the cascade stopped waiting invalidates the catalog itself.
    let mut removal = {
        let store = Arc::clone(store);
        tokio::spawn(
            async move { store.delete_thread(project_id, candidate).await }.in_current_span(),
        )
    };
    match tokio::time::timeout(step_timeout, &mut removal).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(error))) => {
            error!(
                %project_id,
                root_thread_id = %root,
                thread_id = %candidate,
                harness_thread_id = %thread_file.harness_thread_id,
                deleted,
                total_count = total,
                action = "delete_thread",
                %error,
                "native thread was deleted but local thread removal failed"
            );
            return Err(format!(
                "failed to delete thread subtree after deleting {deleted} thread(s): {error}"
            ));
        }
        Ok(Err(join_error)) => {
            error!(
                %project_id,
                root_thread_id = %root,
                thread_id = %candidate,
                deleted,
                total_count = total,
                action = "delete_thread",
                error = %join_error,
                "local thread removal task failed"
            );
            return Err(format!(
                "failed to delete thread subtree after deleting {deleted} thread(s): {join_error}"
            ));
        }
        Err(_) => {
            warn!(
                %project_id,
                root_thread_id = %root,
                thread_id = %candidate,
                deleted,
                total_count = total,
                timeout_ms = step_timeout.as_millis(),
                action = "delete_thread",
                "local thread removal outran its bound; it keeps running and the cascade stops"
            );
            let hub = Arc::clone(hub);
            tokio::spawn(
                async move {
                    match removal.await {
                        Ok(Ok(())) => info!(
                            %project_id,
                            thread_id = %candidate,
                            action = "delete_thread",
                            "late local thread removal finished"
                        ),
                        Ok(Err(error)) => error!(
                            %project_id,
                            thread_id = %candidate,
                            action = "delete_thread",
                            %error,
                            "late local thread removal failed"
                        ),
                        Err(error) => error!(
                            %project_id,
                            thread_id = %candidate,
                            action = "delete_thread",
                            %error,
                            "late local thread removal task failed"
                        ),
                    }
                    hub.invalidate_thread_catalog(project_id).await;
                }
                .in_current_span(),
            );
            return Ok(StepOutcome::RemovalStillRunning);
        }
    }
    info!(
        %project_id,
        root_thread_id = %root,
        thread_id = %candidate,
        harness_thread_id = %thread_file.harness_thread_id,
        deleted_count = deleted + 1,
        total_count = total,
        action = "delete_thread",
        "deleted thread from cascading subtree"
    );
    Ok(StepOutcome::Deleted)
}

/// The harness step outran its bound. Dropping it leaves the thread file intact, so the thread
/// really is kept; the native delete is idempotent on the retry.
fn stuck(
    project_id: ProjectId,
    root: ThreadId,
    candidate: ThreadId,
    deleted: usize,
    total: usize,
    what: &str,
    step_timeout: Duration,
) -> String {
    warn!(
        %project_id,
        root_thread_id = %root,
        thread_id = %candidate,
        deleted,
        total_count = total,
        timeout_ms = step_timeout.as_millis(),
        action = "delete_thread",
        "thread deletion step timed out; stopping the cascade so the project is not held"
    );
    format!(
        "stuck on thread {candidate} after deleting {deleted} of {total} thread(s): {what} within \
         {} seconds. The thread was kept so this can be retried.",
        step_timeout.as_secs()
    )
}

/// Remove `<id>.deleting/` directories a previous run left behind.
///
/// `delete_thread` renames a thread's directory out of enumeration before removing it, so a
/// crash or a timed-out removal between the two leaves a directory nothing lists and nothing
/// will ever delete: the thread it belonged to is no longer there to be deleted again. Run once
/// at startup, when no deletion can be in flight.
pub async fn sweep_abandoned_deletions(store: &PersistStore) {
    let index = match store.load_project_index().await {
        Ok(index) => index,
        Err(error) => {
            warn!(
                %error,
                action = "sweep_abandoned_deletions",
                "could not list projects; leftover thread deletions are not swept"
            );
            return;
        }
    };
    for project in index.projects {
        match store.sweep_abandoned_thread_deletions(project.id).await {
            Ok(swept) if swept.is_empty() => {}
            Ok(swept) => info!(
                project_id = %project.id,
                swept = swept.len(),
                action = "sweep_abandoned_deletions",
                "removed thread directories left behind by interrupted deletions"
            ),
            Err(error) => warn!(
                project_id = %project.id,
                %error,
                action = "sweep_abandoned_deletions",
                "could not sweep leftover thread deletions"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress(project_id: ProjectId, root: ThreadId) -> ThreadDeletionProgress {
        ThreadDeletionProgress {
            project_id,
            thread_id: root,
            state: ThreadDeletionState::Running,
            deleted: 0,
            total: 3,
            error: None,
        }
    }

    fn register(
        service: &ThreadDeletionService,
        project_id: ProjectId,
        root: ThreadId,
        members: &[ThreadId],
    ) -> Arc<AtomicBool> {
        let cancel = Arc::new(AtomicBool::new(false));
        let mut jobs = service.lock_jobs();
        jobs.by_root.insert(
            root,
            DeletionJob {
                progress: progress(project_id, root),
                cancel: cancel.clone(),
            },
        );
        for member in members {
            jobs.members.insert(*member, root);
        }
        cancel
    }

    fn set_state(service: &ThreadDeletionService, root: ThreadId, state: ThreadDeletionState) {
        let mut jobs = service.lock_jobs();
        jobs.members.retain(|_, job_root| *job_root != root);
        let job = jobs.by_root.get_mut(&root).unwrap();
        job.progress.state = state;
        job.progress.error = Some("stuck".into());
    }

    #[test]
    fn members_answer_for_descendants_until_the_deletion_ends() {
        let service = ThreadDeletionService::new();
        let project_id = ProjectId::new();
        let (root, child) = (ThreadId::new(), ThreadId::new());
        register(&service, project_id, root, &[child, root]);

        assert_eq!(service.deleting_root(child), Some(root));
        assert_eq!(service.deleting_root(root), Some(root));
        assert!(service.project_has_running(project_id));
        assert!(!service.project_has_running(ProjectId::new()));

        set_state(&service, root, ThreadDeletionState::Failed);
        assert_eq!(service.deleting_root(child), None);
        assert!(!service.project_has_running(project_id));
        // The failure record stays for the list to show until it is dismissed.
        let shown = service.progress(root).unwrap();
        assert_eq!(shown.state, ThreadDeletionState::Failed);
        assert_eq!(service.dismiss(root), DismissOutcome::Dismissed);
        assert_eq!(service.progress(root), None);
        assert_eq!(service.dismiss(root), DismissOutcome::NotFound);
    }

    #[test]
    fn cancel_flags_a_running_deletion_and_leaves_a_terminal_record_alone() {
        let service = ThreadDeletionService::new();
        let project_id = ProjectId::new();
        let root = ThreadId::new();
        let cancel = register(&service, project_id, root, &[root]);

        assert_eq!(service.cancel(root), CancelOutcome::Requested);
        assert!(cancel.load(Ordering::SeqCst));
        assert_eq!(
            service.progress(root).map(|p| p.state),
            Some(ThreadDeletionState::Running)
        );
        assert_eq!(
            service.dismiss(root),
            DismissOutcome::StillRunning,
            "a running deletion is cancelled, not dismissed"
        );

        // A cancel that lands after the cascade stopped must not erase why it stopped.
        set_state(&service, root, ThreadDeletionState::Failed);
        assert_eq!(service.cancel(root), CancelOutcome::NotRunning);
        assert_eq!(
            service.progress(root).map(|p| p.state),
            Some(ThreadDeletionState::Failed)
        );
        assert_eq!(service.cancel(ThreadId::new()), CancelOutcome::NotFound);
    }

    #[test]
    fn a_reservation_fences_the_subtree_and_is_released_when_dropped_unconsumed() {
        let service = Arc::new(ThreadDeletionService::new());
        let project_id = ProjectId::new();
        let (root, child, other_root) = (ThreadId::new(), ThreadId::new(), ThreadId::new());

        let reservation = service
            .reserve(project_id, root, &[child, root])
            .expect("first reservation in the project");
        assert_eq!(service.deleting_root(child), Some(root));
        assert!(service.project_has_running(project_id));
        let refused = service
            .reserve(project_id, other_root, &[other_root])
            .err()
            .expect("one deletion per project at a time");
        assert_eq!(refused.thread_id, root);
        assert!(
            service
                .reserve(ProjectId::new(), other_root, &[other_root])
                .is_ok(),
            "another project is unaffected"
        );

        // The preflight refused after reserving: the claim goes with the guard, as it would on
        // an early `?` return.
        drop(reservation);
        assert_eq!(service.deleting_root(child), None);
        assert!(service.progress(root).is_none());
        assert!(!service.project_has_running(project_id));
        assert!(
            service.reserve(project_id, root, &[child, root]).is_ok(),
            "the project accepts a new deletion"
        );
    }

    #[test]
    fn a_task_that_ends_without_a_terminal_state_is_recorded_as_failed_and_unfenced() {
        let service = Arc::new(ThreadDeletionService::new());
        let project_id = ProjectId::new();
        let (root, child) = (ThreadId::new(), ThreadId::new());
        register(&service, project_id, root, &[child, root]);
        {
            let mut jobs = service.lock_jobs();
            jobs.by_root.get_mut(&root).unwrap().progress.deleted = 1;
        }

        drop(RunGuard {
            service: Arc::clone(&service),
            root,
            finished: false,
        });

        let record = service
            .progress(root)
            .expect("the record is kept to explain");
        assert_eq!(record.state, ThreadDeletionState::Failed);
        assert_eq!(record.deleted, 1);
        assert!(
            record
                .error
                .as_deref()
                .is_some_and(|e| e.contains("ended unexpectedly")),
            "{record:?}"
        );
        assert_eq!(service.deleting_root(child), None);
        assert!(!service.project_has_running(project_id));
    }

    #[test]
    fn forgetting_a_project_drops_its_records_and_members_only() {
        let service = ThreadDeletionService::new();
        let (kept_project, dropped_project) = (ProjectId::new(), ProjectId::new());
        let (kept_root, dropped_root) = (ThreadId::new(), ThreadId::new());
        register(&service, kept_project, kept_root, &[kept_root]);
        register(&service, dropped_project, dropped_root, &[dropped_root]);

        service.forget_project(dropped_project);

        assert_eq!(service.deleting_root(kept_root), Some(kept_root));
        assert_eq!(service.deleting_root(dropped_root), None);
        assert!(service.progress(dropped_root).is_none());
        assert!(service.progress(kept_root).is_some());
        assert_eq!(
            service.project_progress(kept_project).len(),
            1,
            "the other project's record is untouched"
        );
    }

    #[test]
    fn stuck_message_names_the_thread_and_the_bound() {
        let candidate = ThreadId::new();
        let message = stuck(
            ProjectId::new(),
            ThreadId::new(),
            candidate,
            7,
            12,
            "removing the thread's files did not finish",
            Duration::from_secs(30),
        );
        assert!(message.contains(&candidate.to_string()));
        assert!(message.contains("7 of 12"));
        assert!(message.contains("30 seconds"));
        assert!(message.contains("retried"));
    }
}
