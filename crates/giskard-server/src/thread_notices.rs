//! Process-local authority for retained background notices.
//!
//! Direct action errors stay connection-scoped. Background degradation which must survive a
//! reconnect is keyed by stable notice kind here and published as one revisioned replacement.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, MutexGuard};

use tracing::error;

use giskard_core::ids::ThreadId;
use giskard_proto::{ErrorInfo, ThreadNotice, ThreadNoticeSnapshot};

#[derive(Default)]
pub struct ThreadNoticeStore {
    threads: Mutex<HashMap<ThreadId, NoticeState>>,
}

#[derive(Default)]
struct NoticeState {
    revision: u64,
    notices: BTreeMap<String, ThreadNotice>,
}

#[derive(Debug, thiserror::Error)]
pub enum NoticeStoreError {
    #[error("notice revision exhausted for thread {0}")]
    RevisionExhausted(ThreadId),
}

impl ThreadNoticeStore {
    pub fn snapshot(&self, thread_id: ThreadId) -> ThreadNoticeSnapshot {
        self.lock_threads()
            .get(&thread_id)
            .map(|state| state.snapshot(thread_id))
            .unwrap_or(ThreadNoticeSnapshot {
                thread_id,
                revision: 0,
                notices: Vec::new(),
            })
    }

    pub fn upsert_error(
        &self,
        thread_id: ThreadId,
        warning: &ErrorInfo,
    ) -> Result<ThreadNoticeSnapshot, NoticeStoreError> {
        let mut threads = self.lock_threads();
        let state = threads.entry(thread_id).or_default();
        let revision = state
            .revision
            .checked_add(1)
            .ok_or(NoticeStoreError::RevisionExhausted(thread_id))?;
        state.revision = revision;
        state.notices.insert(
            warning.code.clone(),
            ThreadNotice {
                kind: warning.code.clone(),
                revision,
                severity: warning.severity,
                message: warning.message.clone(),
                detail: warning.detail.clone(),
            },
        );
        Ok(state.snapshot(thread_id))
    }

    pub fn clear_kind(
        &self,
        thread_id: ThreadId,
        kind: &str,
    ) -> Result<Option<ThreadNoticeSnapshot>, NoticeStoreError> {
        let mut threads = self.lock_threads();
        let Some(state) = threads.get_mut(&thread_id) else {
            return Ok(None);
        };
        if state.notices.remove(kind).is_none() {
            return Ok(None);
        }
        state.revision = state
            .revision
            .checked_add(1)
            .ok_or(NoticeStoreError::RevisionExhausted(thread_id))?;
        let snapshot = state.snapshot(thread_id);
        Ok(Some(snapshot))
    }

    pub fn forget(&self, thread_id: ThreadId) {
        self.lock_threads().remove(&thread_id);
    }

    fn lock_threads(&self) -> MutexGuard<'_, HashMap<ThreadId, NoticeState>> {
        match self.threads.lock() {
            Ok(threads) => threads,
            Err(poisoned) => {
                error!(
                    action = "lock_thread_notice_store",
                    "thread notice lock was poisoned; recovering state"
                );
                poisoned.into_inner()
            }
        }
    }
}

impl NoticeState {
    fn snapshot(&self, thread_id: ThreadId) -> ThreadNoticeSnapshot {
        ThreadNoticeSnapshot {
            thread_id,
            revision: self.revision,
            notices: self.notices.values().cloned().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use giskard_proto::ErrorSeverity;

    use super::*;

    fn warning(code: &str, message: &str) -> ErrorInfo {
        ErrorInfo {
            code: code.into(),
            severity: ErrorSeverity::Warning,
            message: message.into(),
            detail: None,
            request_id: None,
            process_id: None,
            thread_id: None,
            action: None,
        }
    }

    #[test]
    fn notice_kind_is_replaced_and_clear_publishes_empty_snapshot() {
        let store = ThreadNoticeStore::default();
        let thread_id = ThreadId::new();
        let first = store
            .upsert_error(thread_id, &warning("restore", "first"))
            .unwrap();
        let second = store
            .upsert_error(thread_id, &warning("restore", "second"))
            .unwrap();
        assert_eq!(first.revision, 1);
        assert_eq!(second.revision, 2);
        assert_eq!(second.notices.len(), 1);
        assert_eq!(second.notices[0].message, "second");

        let cleared = store.clear_kind(thread_id, "restore").unwrap().unwrap();
        assert_eq!(cleared.revision, 3);
        assert!(cleared.notices.is_empty());
        assert_eq!(store.snapshot(thread_id).revision, 3);
    }
}
