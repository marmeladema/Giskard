//! Advisory exclusion over a whole data directory.
//!
//! The store's per-thread locks are in-process `Mutex`es, so they order writers *within* one
//! binary and provide nothing between two. `giskard-admin` is a separate binary from
//! `giskard-server`: a `prune-legacy` or `migrate-storage` run against a live server's data
//! directory had no protection at all, and the orphan sweep had only a wall-clock guess about
//! another process's progress. A second `giskard-server` on the same directory is the same hazard.
//!
//! One `flock` on one file per data directory replaces all of that with actual mutual exclusion.
//!
//! **Limits, which are real and worth knowing before relying on this:**
//!
//! - **Advisory only.** It constrains Giskard's own binaries. Nothing here stops `rm -rf`, a backup
//!   tool, or an editor writing into the directory.
//! - **Unreliable over NFS.** Acceptable for a local-first, self-hosted tool (§1.2), but it is not
//!   a guarantee on a network filesystem and must not be described as one.
//! - **Platform behaviour is `std`'s**, which documents the lock as advisory or mandatory depending
//!   on the platform. Nothing here relies on more than the advisory guarantee.
//! - **Scope is the data directory**, not a thread or a project. Two Giskard instances on two
//!   separate data directories never contend.

use std::fs::{File, TryLockError};
use std::path::{Path, PathBuf};

use giskard_core::PersistError;

/// The lock file's name inside the data directory.
///
/// Its *contents* are irrelevant — the lock is the lock, not the bytes — so nothing is ever written
/// to it and nothing reads it.
pub const LOCK_FILE_NAME: &str = ".giskard.lock";

/// An exclusive advisory lock on one data directory, held for as long as this value lives.
///
/// The lock belongs to the open file, so dropping this releases it. A holder that needs the lock
/// for its whole process lifetime has to keep the value alive for that long — binding it to `_`
/// drops it immediately and locks nothing.
///
/// There is deliberately no unlock-on-shutdown path and no cleanup of the file: the kernel releases
/// the lock when the holding process dies, including abnormally, so a crash leaves nothing stale.
/// That is the whole reason to prefer this over a pidfile, which would have needed liveness checks
/// and PID-reuse handling to answer the same question.
#[derive(Debug)]
pub struct DataDirLock {
    _file: File,
    path: PathBuf,
}

impl DataDirLock {
    /// Take the lock, or report that someone else holds it.
    ///
    /// `Ok(None)` means another Giskard process has this data directory. `try_lock` rather than
    /// `lock` is deliberate: an operator who ran a command by mistake wants to be told, not to
    /// watch a process that appears to hang.
    pub fn try_acquire(data_dir: &Path) -> Result<Option<Self>, PersistError> {
        let path = Self::path(data_dir);
        std::fs::create_dir_all(data_dir).map_err(|e| {
            PersistError::Io(format!(
                "cannot create data dir {}: {e}",
                data_dir.display()
            ))
        })?;
        // Created if absent and never deleted. Unlinking it on shutdown would race whoever takes it
        // next and buys nothing — an unlocked lock file is already the "nobody holds this" state.
        let file = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .map_err(|e| PersistError::Io(format!("cannot open {}: {e}", path.display())))?;

        match file.try_lock() {
            Ok(()) => Ok(Some(Self { _file: file, path })),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(e) => Err(PersistError::Io(format!(
                "cannot lock {}: {e}",
                path.display()
            ))),
        }
    }

    /// Whether another process currently holds this data directory.
    ///
    /// A momentary probe: it takes the lock and releases it, so the answer is already stale when it
    /// is returned. Only for read-only paths that want to *warn* that their listing may be racy —
    /// never as a check before doing something destructive, which must hold the lock instead.
    pub fn is_held(data_dir: &Path) -> bool {
        matches!(Self::try_acquire(data_dir), Ok(None))
    }

    pub fn path(data_dir: &Path) -> PathBuf {
        data_dir.join(LOCK_FILE_NAME)
    }

    /// Where this lock is held, for messages that need to name it.
    pub fn lock_path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Two acquisitions from **one process** must conflict.
    ///
    /// This test is only meaningful because `std` uses `flock`, whose locks belong to the open file
    /// description: two opens of the same path are two descriptions, so they contend. The obvious
    /// alternative primitive — POSIX `fcntl` record locks — is per-*process*, and under it the
    /// second acquisition would succeed and this test would pass while guarding nothing.
    #[test]
    fn a_second_acquisition_is_refused_while_the_first_is_held() {
        let tmp = TempDir::new().unwrap();
        let first = DataDirLock::try_acquire(tmp.path()).unwrap();
        assert!(
            first.is_some(),
            "an unlocked data directory must be takeable"
        );
        assert!(
            DataDirLock::try_acquire(tmp.path()).unwrap().is_none(),
            "a second holder must be refused, not blocked and not admitted"
        );
        assert!(DataDirLock::is_held(tmp.path()));

        drop(first);
        assert!(
            DataDirLock::try_acquire(tmp.path()).unwrap().is_some(),
            "releasing must make the directory takeable again"
        );
        assert!(!DataDirLock::is_held(tmp.path()));
    }

    #[test]
    fn the_lock_file_is_created_and_left_behind() {
        let tmp = TempDir::new().unwrap();
        let path = DataDirLock::path(tmp.path());
        assert!(!path.exists());

        let lock = DataDirLock::try_acquire(tmp.path()).unwrap().unwrap();
        assert_eq!(lock.lock_path(), path);
        assert!(path.exists());

        drop(lock);
        assert!(
            path.exists(),
            "the file outlives the lock: deleting it would race the next holder"
        );
    }

    /// Taking the lock must never disturb a directory it is only guarding.
    #[test]
    fn acquiring_does_not_touch_anything_else_in_the_data_directory() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join("config.toml"), b"[server]\n").unwrap();
        let lock = DataDirLock::try_acquire(tmp.path()).unwrap().unwrap();
        assert_eq!(
            std::fs::read(tmp.path().join("config.toml")).unwrap(),
            b"[server]\n"
        );
        drop(lock);
    }
}
