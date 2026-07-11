//! Single-writer token-ledger actor (spec §5.4 / §10.2).
//!
//! `tokens-global.json` is a cross-project hot file: every completed turn in any project updates
//! it. To avoid multi-writer races without a global lock, exactly one Tokio task owns the
//! in-memory global ledger (and each project's ledger) and serializes all writes. Producers send
//! [`LedgerMsg::Record`] deltas over an mpsc channel; the actor coalesces bursts (drains everything
//! pending) and then writes each dirtied file once.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::warn;

use giskard_core::ids::ProjectId;
use giskard_core::token::{DailyTokenLedger, TokenUsage};
use giskard_persist::PersistStore;

/// A usage delta to fold into the project + global ledgers.
struct Record {
    project: ProjectId,
    date: String,
    provider: String,
    model: String,
    usage: TokenUsage,
}

/// Cloneable handle used by producers (the turn forwarder) to record usage.
#[derive(Clone)]
pub struct LedgerHandle {
    tx: mpsc::Sender<Record>,
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
            provider,
            model,
            usage,
        };
        if self.tx.try_send(rec).is_err() {
            warn!("token ledger queue full or closed; dropping a usage delta");
        }
    }
}

/// Spawn the ledger actor, returning a handle. Loads the existing global ledger at startup so
/// counts survive restarts (§5.1).
pub fn spawn(store: Arc<PersistStore>) -> LedgerHandle {
    let (tx, rx) = mpsc::channel(1024);
    tokio::spawn(actor(store, rx));
    LedgerHandle { tx }
}

async fn actor(store: Arc<PersistStore>, mut rx: mpsc::Receiver<Record>) {
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
    let mut projects: HashMap<ProjectId, DailyTokenLedger> = HashMap::new();

    while let Some(first) = rx.recv().await {
        // Coalesce: apply this delta and every other one already queued, then flush once per file.
        let mut dirty: HashSet<ProjectId> = HashSet::new();
        apply(&store, &mut global, &mut projects, &mut dirty, first).await;
        while let Ok(rec) = rx.try_recv() {
            apply(&store, &mut global, &mut projects, &mut dirty, rec).await;
        }

        if let Err(e) = store.save_global_tokens(&global).await {
            warn!(%e, "failed to persist global token ledger");
        }
        for pid in dirty {
            if let Some(ledger) = projects.get(&pid) {
                if let Err(e) = store.save_project_tokens(pid, ledger).await {
                    warn!(%pid, %e, "failed to persist project token ledger");
                }
            }
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
    global.record(&rec.date, &rec.provider, &rec.model, &rec.usage);

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
    ledger.record(&rec.date, &rec.provider, &rec.model, &rec.usage);
    dirty.insert(rec.project);
}
