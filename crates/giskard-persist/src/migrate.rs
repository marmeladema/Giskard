//! Format 1 → format 2 migration: one thread at a time, on open, idempotently.
//!
//! This is the first change in the storage sequence that can touch transcript history, the part of
//! a user's data nothing else can reconstruct. So migration never deletes: it stages a rebuild,
//! verifies it before committing, commits with one rename, and *relocates* the originals under
//! `legacy/`. Removing them is a separate, explicit `giskard-admin` step.

use std::path::Path;

use chrono::{DateTime, Utc};
use tokio::fs;

use giskard_core::PersistError;
use giskard_core::ids::ThreadId;
use giskard_core::turn::Turn;

use crate::atomic::atomic_write;
use crate::history::{HistoryHeader, TurnRecord, parse_history_index, payload_file_bytes};
use crate::layout::{ThreadLayout, ThreadPaths};
use crate::store::parse_turn_history;

/// What a migration did, for operator-facing output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationOutcome {
    /// Nothing on disk for this thread.
    Absent,
    /// Already format 2, with no format 1 originals left beside it.
    AlreadyCurrent,
    /// A previous run committed the directory but had not yet relocated the originals.
    FinishedLegacyMove,
    /// A format 1 thread was rebuilt as a format 2 directory.
    Migrated,
}

/// What [`migrate_thread`] will do, decided from the files on disk alone.
///
/// Shared with it rather than mirrored, so a dry run and the real thing cannot disagree about what
/// a thread needs. The one thing a plan cannot know is whether the work will *succeed*:
/// `Migrated` here means "will be attempted", and a migration that aborts on an unparseable format
/// 1 history still plans as `Migrated`. Deciding otherwise would mean doing the whole staged
/// rebuild and throwing it away, which is not what a dry run is for.
pub async fn planned_outcome(threads_dir: &Path, thread: ThreadId) -> MigrationOutcome {
    let paths = ThreadPaths::new(threads_dir.to_path_buf(), thread, ThreadLayout::Directory);
    let has_originals = exists(&paths.flat_metadata()).await || exists(&paths.flat_history()).await;
    match (is_dir(&paths.dir()).await, has_originals) {
        // Both shapes on disk: a crash between the commit rename and the relocation. The rebuild
        // is already committed, so what remains is finishing the move.
        (true, true) => MigrationOutcome::FinishedLegacyMove,
        (true, false) => MigrationOutcome::AlreadyCurrent,
        (false, true) => MigrationOutcome::Migrated,
        (false, false) => MigrationOutcome::Absent,
    }
}

async fn exists(path: &Path) -> bool {
    fs::metadata(path).await.is_ok()
}

async fn is_dir(path: &Path) -> bool {
    matches!(fs::metadata(path).await, Ok(meta) if meta.is_dir())
}

/// fsync a directory so its entries survive a power loss, not just its files' contents.
async fn sync_dir(path: &Path) -> Result<(), PersistError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || std::fs::File::open(&path)?.sync_all())
        .await
        .map_err(|e| PersistError::Io(e.to_string()))?
        .map_err(|e| PersistError::Io(e.to_string()))
}

/// Migrate one thread if it needs it. Safe to call on every open.
///
/// Every step is re-runnable: a crash before the commit rename leaves a `.migrating` directory the
/// next run discards and rebuilds, and a crash between the rename and the legacy move leaves both a
/// top-level `<id>.json` and an `<id>/` directory — readers prefer the directory and this call
/// finishes the move.
pub async fn migrate_thread(
    threads_dir: &Path,
    thread: ThreadId,
) -> Result<MigrationOutcome, PersistError> {
    let paths = ThreadPaths::new(threads_dir.to_path_buf(), thread, ThreadLayout::Directory);
    let flat_metadata = paths.flat_metadata();
    let flat_history = paths.flat_history();

    match planned_outcome(threads_dir, thread).await {
        MigrationOutcome::AlreadyCurrent => return Ok(MigrationOutcome::AlreadyCurrent),
        MigrationOutcome::Absent => return Ok(MigrationOutcome::Absent),
        MigrationOutcome::FinishedLegacyMove => {
            move_originals_to_legacy(&paths).await?;
            return Ok(MigrationOutcome::FinishedLegacyMove);
        }
        // The one case with work to do; everything below is that work.
        MigrationOutcome::Migrated => {}
    }

    // 1. Read the format 1 originals. A history that will not parse aborts the migration: the
    //    thread stays format 1 and keeps behaving exactly as it does today, which is strictly
    //    better than rebuilding it minus the turns that were lost.
    let source_turns = read_flat_history(&flat_history).await?;
    let created_at = read_flat_created_at(&flat_metadata).await;

    // 2. Stage the rebuild beside the original, never over it.
    let staging = paths.migrating_dir();
    if exists(&staging).await {
        fs::remove_dir_all(&staging)
            .await
            .map_err(|e| PersistError::Io(e.to_string()))?;
    }
    // The staged tree is built at `<id>.migrating/`, so its paths are built by hand: `paths` names
    // the committed location the rename will produce.
    let staged_turns = staging.join("turns");
    fs::create_dir_all(&staged_turns)
        .await
        .map_err(|e| PersistError::Io(e.to_string()))?;

    if exists(&flat_metadata).await {
        // Copied byte-for-byte rather than reserialized: an unreadable or older-shaped metadata
        // record must migrate to exactly what it was, and be quarantined by the ordinary read path
        // if it is bad — not silently rewritten by the migration.
        let metadata = fs::read(&flat_metadata)
            .await
            .map_err(|e| PersistError::Io(e.to_string()))?;
        atomic_write(&staging.join("thread.json"), &metadata).await?;
    }

    // Every staged file is written through `atomic_write`, which fsyncs *contents* before its
    // rename. A plain write would leave the commit rename below publishing a directory of files
    // whose bytes are still only in the page cache — and the verification step would read those
    // same cached bytes and happily pass. The directory fsyncs that follow make the entries
    // durable; these make what the entries point at durable.
    let mut index = HistoryHeader::new(thread, created_at).line()?;
    for turn in &source_turns {
        atomic_write(
            &staged_turns.join(format!("{}.jsonl", turn.id)),
            &payload_file_bytes(turn)?,
        )
        .await?;
        index.push_str(&TurnRecord::from_turn(turn).line()?);
    }
    let staged_index = staging.join("history.jsonl");
    atomic_write(&staged_index, index.as_bytes()).await?;

    // 3. Verify the rebuild by reading it back, and abort without committing on any mismatch.
    verify_staged(&staging, &staged_index, &source_turns).await?;

    // 4. Commit: fsync the staged tree, then one rename.
    sync_dir(&staged_turns).await?;
    sync_dir(&staging).await?;
    fs::rename(&staging, paths.dir())
        .await
        .map_err(|e| PersistError::Io(e.to_string()))?;
    sync_dir(threads_dir).await?;

    // 5. Relocate — never delete — the originals.
    move_originals_to_legacy(&paths).await?;
    Ok(MigrationOutcome::Migrated)
}

/// Parse the format 1 history, treating an absent file as an empty one — a thread can have
/// metadata and no turns. A file that will not parse is an error, which aborts the migration.
async fn read_flat_history(flat_history: &Path) -> Result<Vec<Turn>, PersistError> {
    match fs::read_to_string(flat_history).await {
        Ok(data) => parse_turn_history(flat_history, &data),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(vec![]),
        Err(e) => Err(PersistError::Io(e.to_string())),
    }
}

/// The thread's creation time, for the history header. Falls back to now for a metadata record that
/// is missing or unreadable — the header timestamp is descriptive, and refusing to migrate over it
/// would strand the thread on a format whose corruption path this change exists to close.
async fn read_flat_created_at(flat_metadata: &Path) -> DateTime<Utc> {
    let Ok(data) = fs::read(flat_metadata).await else {
        return Utc::now();
    };
    serde_json::from_slice::<serde_json::Value>(&data)
        .ok()
        .and_then(|value| {
            value
                .get("created_at")?
                .as_str()?
                .parse::<DateTime<Utc>>()
                .ok()
        })
        .unwrap_or_else(Utc::now)
}

/// Re-read the staged directory and compare turn ids, order, and count against the source.
async fn verify_staged(
    staging: &Path,
    staged_index: &Path,
    source_turns: &[Turn],
) -> Result<(), PersistError> {
    let data = fs::read_to_string(staged_index)
        .await
        .map_err(|e| PersistError::Io(e.to_string()))?;
    let records = parse_history_index(staged_index, &data)?;
    let staged_ids: Vec<_> = records.iter().map(|record| record.turn_id).collect();
    let source_ids: Vec<_> = source_turns.iter().map(|turn| turn.id).collect();
    if staged_ids != source_ids {
        return Err(PersistError::Invalid(format!(
            "{}: staged history index does not match the source ({} of {} turns matched in order)",
            staging.display(),
            staged_ids
                .iter()
                .zip(&source_ids)
                .take_while(|(a, b)| a == b)
                .count(),
            source_ids.len()
        )));
    }
    for record in &records {
        let payload_path = staging
            .join("turns")
            .join(format!("{}.jsonl", record.turn_id));
        let data = fs::read_to_string(&payload_path)
            .await
            .map_err(|e| PersistError::Io(e.to_string()))?;
        crate::history::parse_turn_payload(&payload_path, &data)?;
    }
    Ok(())
}

/// Move the format 1 originals under `legacy/`. Idempotent, and the step a crash may leave undone.
async fn move_originals_to_legacy(paths: &ThreadPaths) -> Result<(), PersistError> {
    let flat_metadata = paths.flat_metadata();
    let flat_history = paths.flat_history();
    if !exists(&flat_metadata).await && !exists(&flat_history).await {
        return Ok(());
    }
    let legacy = paths.legacy_dir();
    fs::create_dir_all(&legacy)
        .await
        .map_err(|e| PersistError::Io(e.to_string()))?;
    for (from, to) in [
        (flat_history, legacy.join("history.jsonl")),
        (flat_metadata, legacy.join("thread.json")),
    ] {
        if exists(&from).await {
            fs::rename(&from, &to)
                .await
                .map_err(|e| PersistError::Io(e.to_string()))?;
        }
    }
    Ok(())
}
