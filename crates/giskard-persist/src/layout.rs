//! Where a thread's files live, and which on-disk format they are in.
//!
//! Two layouts coexist while a store still holds unmigrated threads, and *every* thread file
//! operation resolves differently between them — metadata, history, deletion, enumeration. Path
//! resolution therefore lives here rather than being branched at each call site, so a new caller
//! cannot forget one of the two shapes.

use std::path::{Path, PathBuf};

use giskard_core::ids::{ThreadId, TurnId};

/// The directory layout and index schema this build writes (`history.jsonl` header `format`).
///
/// Governs the *layout*, not the metadata record (`ThreadFile.version`) and not a turn's payload
/// (`turns/<id>.jsonl` header `format`). The three evolve independently and each marker says only
/// what it governs.
pub const HISTORY_FORMAT: u32 = 3;

/// The per-turn payload schema this build writes (`turns/<id>.jsonl` header `format`).
///
/// Per-file rather than per-directory so a turn can be migrated on its own: a directory holding a
/// mix of old and new payload files is legal, which is what makes lazy per-turn migration possible.
pub const TURN_PAYLOAD_FORMAT: u32 = 1;

/// Suffix of a staged, not-yet-committed migration. Never enumerated as a thread.
pub const MIGRATING_SUFFIX: &str = ".migrating";

/// Suffix of a thread directory renamed out of enumeration by `delete_thread`, pending removal.
pub const DELETING_SUFFIX: &str = ".deleting";

/// The on-disk shape of one thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadLayout {
    /// format 1: `threads/<id>.json` beside `threads/<id>.jsonl`.
    Flat,
    /// format 2: the `threads/<id>/` directory.
    Directory,
}

impl ThreadLayout {
    /// The `format` a `history.jsonl` header claims for this layout. Format 1 has no header; the
    /// number exists so operator-facing output can name both layouts in one vocabulary.
    pub fn history_format(self) -> u32 {
        match self {
            Self::Flat => 1,
            Self::Directory => HISTORY_FORMAT,
        }
    }
}

/// Resolved paths for one thread, in whichever layout that thread is actually stored.
#[derive(Debug, Clone)]
pub struct ThreadPaths {
    threads_dir: PathBuf,
    thread: ThreadId,
    layout: ThreadLayout,
}

impl ThreadPaths {
    pub fn new(threads_dir: PathBuf, thread: ThreadId, layout: ThreadLayout) -> Self {
        Self {
            threads_dir,
            thread,
            layout,
        }
    }

    pub fn layout(&self) -> ThreadLayout {
        self.layout
    }

    pub fn threads_dir(&self) -> &Path {
        &self.threads_dir
    }

    /// `threads/<id>/` — the format 2 thread directory.
    pub fn dir(&self) -> PathBuf {
        self.threads_dir.join(self.thread.to_string())
    }

    /// The staging directory a migration builds before committing it with one rename.
    pub fn migrating_dir(&self) -> PathBuf {
        self.threads_dir
            .join(format!("{}{MIGRATING_SUFFIX}", self.thread))
    }

    /// Where `delete_thread` renames the directory to drop it out of enumeration atomically.
    pub fn deleting_dir(&self) -> PathBuf {
        self.threads_dir
            .join(format!("{}{DELETING_SUFFIX}", self.thread))
    }

    /// Pre-migration originals, retained rather than deleted.
    pub fn legacy_dir(&self) -> PathBuf {
        self.dir().join("legacy")
    }

    /// The thread metadata record (`ThreadFile`).
    pub fn metadata(&self) -> PathBuf {
        match self.layout {
            ThreadLayout::Flat => self.flat_metadata(),
            ThreadLayout::Directory => self.dir().join("thread.json"),
        }
    }

    /// The turn history index. Format 1 stores whole turns here; format 2 stores bounded records.
    pub fn history(&self) -> PathBuf {
        match self.layout {
            ThreadLayout::Flat => self.flat_history(),
            ThreadLayout::Directory => self.dir().join("history.jsonl"),
        }
    }

    pub fn turns_dir(&self) -> PathBuf {
        self.dir().join("turns")
    }

    /// One turn's payload — everything whose size depends on what the agent did.
    pub fn turn_payload(&self, turn: TurnId) -> PathBuf {
        self.turns_dir().join(format!("{turn}.jsonl"))
    }

    /// `threads/<id>.json`, wherever the thread currently is: migration reads it, deletion removes
    /// it, and both have to name it while a directory already exists beside it.
    pub fn flat_metadata(&self) -> PathBuf {
        self.threads_dir.join(format!("{}.json", self.thread))
    }

    /// `threads/<id>.jsonl`, same reasoning as [`Self::flat_metadata`].
    pub fn flat_history(&self) -> PathBuf {
        self.threads_dir.join(format!("{}.jsonl", self.thread))
    }
}
