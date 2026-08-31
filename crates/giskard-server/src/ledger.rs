//! Single-writer token-ledger actor (spec §5.4 / §10.2).
//!
//! `tokens-global.json` is a cross-project hot file: every completed turn in any project updates
//! it. To avoid multi-writer races without a global lock, exactly one Tokio task owns the
//! in-memory global ledger (and each project's ledger) and serializes all writes. Producers send
//! [`LedgerMsg::Record`] deltas over an mpsc channel; the actor coalesces bursts (drains everything
//! pending) and then writes each dirtied file once.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use giskard_core::ids::ProjectId;
use giskard_core::token::{DailyTokenLedger, TokenUsage};
use giskard_persist::PersistStore;

/// Log-only stand-in for a delta with no authoritative model. It never reaches a ledger key: the
/// unattributed bucket is a separate field, not a provider/model pair.
const UNATTRIBUTED_MODEL_FIELD: &str = "<unattributed>";

/// A usage delta to fold into the project + global ledgers.
struct Record {
    project: ProjectId,
    date: String,
    model: Option<(String, String)>,
    usage: TokenUsage,
}

enum LedgerMsg {
    Record(Record),
    Shutdown { completed: oneshot::Sender<()> },
}

/// Cloneable handle used by producers (the turn forwarder) to record usage.
#[derive(Clone)]
pub struct LedgerHandle {
    tx: mpsc::Sender<LedgerMsg>,
}

impl LedgerHandle {
    /// Record a turn's usage against a project's ledger and the global ledger (§10.2).
    /// Best-effort and non-blocking-ish: if the actor's queue is full the delta is dropped with a
    /// warning rather than stalling turn completion (token counts are a metric, not correctness).
    pub async fn record(
        &self,
        project: ProjectId,
        date: String,
        provider: String,
        model: String,
        usage: TokenUsage,
    ) {
        let rec = Record {
            project,
            date,
            model: Some((provider, model)),
            usage,
        };
        self.enqueue(rec).await;
    }

    /// Record usage the provider reported without authoritative model metadata (`TurnModel::
    /// Unknown`). It still counts toward project and global totals, but into an explicit
    /// unattributed bucket rather than under an invented provider/model key.
    pub async fn record_unattributed(&self, project: ProjectId, date: String, usage: TokenUsage) {
        let rec = Record {
            project,
            date,
            model: None,
            usage,
        };
        self.enqueue(rec).await;
    }

    /// Queue one delta, naming the record precisely if the queue cannot take it. A dropped delta
    /// is silent in the ledger files, so the log line is the only trace it ever existed.
    async fn enqueue(&self, rec: Record) {
        if let Err(error) = self.tx.try_send(LedgerMsg::Record(rec)) {
            let Some((reason, record)) = dropped_record_context(error) else {
                tracing::error!(
                    action = "record_token_usage",
                    "token ledger record send returned a shutdown message"
                );
                return;
            };
            let (provider, model) = match record.model.as_ref() {
                Some((provider, model)) => (provider.as_str(), model.as_str()),
                None => (UNATTRIBUTED_MODEL_FIELD, UNATTRIBUTED_MODEL_FIELD),
            };
            warn!(
                project_id = %record.project,
                date = %record.date,
                provider = %provider,
                model = %model,
                reason,
                action = "record_token_usage",
                "token ledger queue unavailable; dropping a usage delta"
            );
        }
    }

    /// Flush every previously queued usage delta and stop the ledger actor.
    pub async fn shutdown(&self) {
        let (completed, wait) = oneshot::channel();
        if self
            .tx
            .send(LedgerMsg::Shutdown { completed })
            .await
            .is_ok()
        {
            let _ = wait.await;
        }
    }
}

fn dropped_record_context(
    error: mpsc::error::TrySendError<LedgerMsg>,
) -> Option<(&'static str, Record)> {
    match error {
        mpsc::error::TrySendError::Full(LedgerMsg::Record(record)) => Some(("full", record)),
        mpsc::error::TrySendError::Closed(LedgerMsg::Record(record)) => Some(("closed", record)),
        mpsc::error::TrySendError::Full(LedgerMsg::Shutdown { .. })
        | mpsc::error::TrySendError::Closed(LedgerMsg::Shutdown { .. }) => None,
    }
}

/// Spawn the ledger actor, returning a handle. Loads the existing global ledger at startup so
/// counts survive restarts (§5.1).
pub fn spawn(store: Arc<PersistStore>) -> LedgerHandle {
    let (tx, rx) = mpsc::channel(1024);
    tokio::spawn(actor(store, rx));
    LedgerHandle { tx }
}

async fn actor(store: Arc<PersistStore>, mut rx: mpsc::Receiver<LedgerMsg>) {
    let mut global = match store.load_global_tokens().await {
        Ok(Some(ledger)) => ledger,
        Ok(None) => DailyTokenLedger::default(),
        Err(error) => {
            warn!(
                %error,
                "failed to load global token ledger; starting with an empty in-memory ledger"
            );
            DailyTokenLedger::default()
        }
    };
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Aggregate token usage per project for durable ledger flushes.
    // Source of truth: Persisted ledger files are durable; this actor-confined state folds updates.
    // Structural reason: A single persistence actor batches cross-project accounting writes.
    // Synchronization: The actor owns the map and processes messages serially without a lock.
    // Invalidation/removal: Shutdown flushes pending state and then drops the actor-owned map.
    let mut projects: HashMap<ProjectId, DailyTokenLedger> = HashMap::new();

    while let Some(message) = rx.recv().await {
        let first = match message {
            LedgerMsg::Record(record) => record,
            LedgerMsg::Shutdown { completed } => {
                let _ = completed.send(());
                return;
            }
        };
        // Coalesce: apply this delta and every other one already queued, then flush once per file.
        let mut dirty: HashSet<ProjectId> = HashSet::new();
        apply(&store, &mut global, &mut projects, &mut dirty, first).await;
        let mut shutdown = None;
        while let Ok(message) = rx.try_recv() {
            match message {
                LedgerMsg::Record(record) => {
                    apply(&store, &mut global, &mut projects, &mut dirty, record).await;
                }
                LedgerMsg::Shutdown { completed } => {
                    shutdown = Some(completed);
                    break;
                }
            }
        }

        if let Err(e) = store.save_global_tokens(&global).await {
            warn!(%e, "failed to persist global token ledger");
        }
        for pid in dirty {
            if let Some(ledger) = projects.get(&pid)
                && let Err(e) = store.save_project_tokens(pid, ledger).await
            {
                warn!(%pid, %e, "failed to persist project token ledger");
            }
        }
        if let Some(completed) = shutdown {
            let _ = completed.send(());
            return;
        }
    }
}

async fn apply(
    store: &PersistStore,
    global: &mut DailyTokenLedger,
    projects: &mut HashMap<ProjectId, DailyTokenLedger>,
    dirty: &mut HashSet<ProjectId>,
    rec: Record,
) {
    match &rec.model {
        Some((provider, model)) => global.record(&rec.date, provider, model, &rec.usage),
        None => global.record_unattributed(&rec.date, &rec.usage),
    }

    let ledger = match projects.entry(rec.project) {
        std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
        std::collections::hash_map::Entry::Vacant(e) => {
            // Hydrate the project ledger from disk on first touch so restarts accumulate.
            let existing = match store.load_project_tokens(rec.project).await {
                Ok(Some(ledger)) => ledger,
                Ok(None) => DailyTokenLedger::default(),
                Err(error) => {
                    warn!(
                        project_id = %rec.project,
                        %error,
                        "failed to load project token ledger; starting with an empty in-memory ledger"
                    );
                    DailyTokenLedger::default()
                }
            };
            e.insert(existing)
        }
    };
    match &rec.model {
        Some((provider, model)) => ledger.record(&rec.date, provider, model, &rec.usage),
        None => ledger.record_unattributed(&rec.date, &rec.usage),
    }
    dirty.insert(rec.project);
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use super::*;
    use crate::test_logs::CapturedLogWriter;

    fn record(project: ProjectId) -> Record {
        Record {
            project,
            date: "2026-08-26".into(),
            model: Some(("openai".into(), "gpt-5".into())),
            usage: TokenUsage::new(12, 3),
        }
    }

    #[test]
    fn dropped_record_context_distinguishes_full_and_closed_queues() {
        let full_project = ProjectId::new();
        let (reason, full) = dropped_record_context(mpsc::error::TrySendError::Full(
            LedgerMsg::Record(record(full_project)),
        ))
        .unwrap();
        assert_eq!(reason, "full");
        assert_eq!(full.project, full_project);
        assert_eq!(full.model, Some(("openai".into(), "gpt-5".into())));

        let closed_project = ProjectId::new();
        let (reason, closed) = dropped_record_context(mpsc::error::TrySendError::Closed(
            LedgerMsg::Record(record(closed_project)),
        ))
        .unwrap();
        assert_eq!(reason, "closed");
        assert_eq!(closed.project, closed_project);
    }

    #[tokio::test]
    async fn dropped_usage_warning_identifies_the_record_and_queue_failure() {
        let output = Arc::new(StdMutex::new(Vec::new()));
        let writer_output = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(move || CapturedLogWriter(writer_output.clone()))
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let handle = LedgerHandle { tx };
        let project = ProjectId::new();

        handle
            .record(
                project,
                "2026-08-26".into(),
                "openai".into(),
                "gpt-5".into(),
                TokenUsage::new(12, 3),
            )
            .await;

        let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        for expected in [
            format!("project_id={project}"),
            "date=2026-08-26".into(),
            "provider=openai".into(),
            "model=gpt-5".into(),
            "reason=\"closed\"".into(),
            "action=\"record_token_usage\"".into(),
        ] {
            assert!(output.contains(&expected), "missing {expected}: {output}");
        }
    }

    #[tokio::test]
    async fn shutdown_flushes_queued_usage_before_returning() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project = ProjectId::new();
        let handle = spawn(store.clone());

        handle
            .record(
                project,
                "2026-08-26".into(),
                "openai".into(),
                "gpt-5".into(),
                TokenUsage::new(12, 3),
            )
            .await;
        handle.shutdown().await;

        let global = store.load_global_tokens().await.unwrap().unwrap();
        let project_ledger = store.load_project_tokens(project).await.unwrap().unwrap();
        assert_eq!(global.total.input, 12);
        assert_eq!(global.total.output, 3);
        assert_eq!(project_ledger.total, global.total);
    }
}
