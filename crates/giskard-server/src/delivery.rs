use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::{Notify, mpsc};
use tracing::{debug, error};

use giskard_core::ids::ThreadId;
use giskard_proto::{ServerMessage, ThreadState};

use crate::hub::ClientId;

/// Connection-owned delivery handle.
///
/// Ordered and direct messages retain the WebSocket's bounded FIFO. Authoritative replacement
/// state waits outside that FIFO, keyed by authority, so a slow socket can make the latest value
/// wait without making the producer wait or losing the update.
pub(crate) struct ClientDelivery {
    ordered: mpsc::Sender<ServerMessage>,
    replacements: Arc<ReplacementMailbox>,
}

/// Socket-writer half of the replacement lane.
///
/// Keeping this separate from [`ClientDelivery`] means the writer does not retain an ordered
/// sender merely to receive replacement state. Once the hub drops the connection's senders, the
/// ordinary receiver can close and terminate the writer normally.
pub(crate) struct ReplacementReceiver {
    replacements: Arc<ReplacementMailbox>,
}

pub(crate) enum DeliverySendError {
    Full,
    Closed,
}

impl ClientDelivery {
    pub(crate) fn new(
        client_id: ClientId,
        ordered: mpsc::Sender<ServerMessage>,
    ) -> (Arc<Self>, ReplacementReceiver) {
        let replacements = Arc::new(ReplacementMailbox::new(client_id));
        let delivery = Arc::new(Self {
            ordered,
            replacements: replacements.clone(),
        });
        (delivery, ReplacementReceiver { replacements })
    }

    pub(crate) fn try_send(&self, message: ServerMessage) -> Result<(), DeliverySendError> {
        self.ordered.try_send(message).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => DeliverySendError::Full,
            mpsc::error::TrySendError::Closed(_) => DeliverySendError::Closed,
        })
    }

    pub(crate) fn publish_thread_state(&self, state: ThreadState) -> bool {
        if self.ordered.is_closed() {
            return false;
        }
        self.replacements.insert_thread_state(state);
        true
    }

    pub(crate) fn invalidate_thread_catalog(&self) -> bool {
        if self.ordered.is_closed() {
            return false;
        }
        self.replacements.invalidate_thread_catalog();
        true
    }
}

impl ReplacementReceiver {
    /// Return the next latest-by-key replacement without passing through the ordered FIFO.
    pub(crate) async fn recv(&self) -> ServerMessage {
        self.replacements.recv().await
    }

    #[cfg(test)]
    pub(crate) fn try_recv(&self) -> Option<ServerMessage> {
        self.replacements.pop_front()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ReplacementKey {
    Thread(ThreadId),
    ThreadCatalog,
}

#[derive(Default)]
struct PendingReplacements {
    order: VecDeque<ReplacementKey>,
    messages: HashMap<ReplacementKey, ServerMessage>,
}

struct ReplacementMailbox {
    client_id: ClientId,
    pending: Mutex<PendingReplacements>,
    changed: Notify,
}

impl ReplacementMailbox {
    fn new(client_id: ClientId) -> Self {
        Self {
            client_id,
            pending: Mutex::new(PendingReplacements::default()),
            changed: Notify::new(),
        }
    }

    fn insert_thread_state(&self, state: ThreadState) {
        let key = ReplacementKey::Thread(state.metadata.thread_id);
        let revision = state.metadata.revision;
        let mut pending = self.lock_pending();

        if let Some(ServerMessage::ThreadState(current)) = pending.messages.get(&key) {
            if current.metadata.revision > revision {
                debug!(
                    client_id = self.client_id,
                    thread_id = %state.metadata.thread_id,
                    pending_revision = current.metadata.revision,
                    ignored_revision = revision,
                    action = "coalesce_thread_metadata",
                    "ignoring an older pending thread metadata replacement"
                );
                return;
            }
            if current.metadata.revision == revision && current.metadata != state.metadata {
                error!(
                    client_id = self.client_id,
                    thread_id = %state.metadata.thread_id,
                    metadata_revision = revision,
                    action = "coalesce_thread_metadata",
                    "same thread metadata revision has conflicting payloads; retaining the first"
                );
                return;
            }
            debug!(
                client_id = self.client_id,
                thread_id = %state.metadata.thread_id,
                replaced_revision = current.metadata.revision,
                retained_revision = revision,
                action = "coalesce_thread_metadata",
                "coalescing pending thread metadata replacement"
            );
        }

        if !pending.messages.contains_key(&key) {
            pending.order.push_back(key);
        }
        pending
            .messages
            .insert(key, ServerMessage::ThreadState(state));
        drop(pending);
        self.changed.notify_one();
    }

    fn invalidate_thread_catalog(&self) {
        let key = ReplacementKey::ThreadCatalog;
        let mut pending = self.lock_pending();
        if !pending.messages.contains_key(&key) {
            pending.order.push_back(key);
            pending
                .messages
                .insert(key, ServerMessage::ThreadCatalogChanged);
        } else {
            debug!(
                client_id = self.client_id,
                action = "coalesce_thread_catalog_invalidation",
                "coalescing pending thread-catalog invalidation"
            );
        }
        drop(pending);
        self.changed.notify_one();
    }

    fn pop_front(&self) -> Option<ServerMessage> {
        let mut pending = self.lock_pending();
        while let Some(key) = pending.order.pop_front() {
            if let Some(message) = pending.messages.remove(&key) {
                return Some(message);
            }
        }
        None
    }

    async fn recv(&self) -> ServerMessage {
        loop {
            let changed = self.changed.notified();
            if let Some(message) = self.pop_front() {
                return message;
            }
            changed.await;
        }
    }

    fn lock_pending(&self) -> MutexGuard<'_, PendingReplacements> {
        match self.pending.lock() {
            Ok(pending) => pending,
            Err(poisoned) => {
                error!(
                    client_id = self.client_id,
                    action = "lock_replacement_mailbox",
                    "replacement mailbox lock was poisoned; recovering its pending state"
                );
                poisoned.into_inner()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use giskard_core::model::ModelRef;
    use giskard_core::token::TokenLedger;
    use giskard_core::turn::{Mode, PermissionPreset};
    use giskard_proto::ThreadMetadata;
    use tokio::time::{Duration, timeout};

    use super::*;

    fn thread_state(thread_id: ThreadId, revision: u64) -> ThreadState {
        ThreadState {
            metadata: ThreadMetadata {
                thread_id,
                revision,
                title: format!("revision {revision}"),
                mode: Mode::Build,
                current_model: ModelRef {
                    provider: "test".into(),
                    model: "test".into(),
                    reasoning_effort: None,
                },
                context_window: 128_000,
                permission_preset: PermissionPreset::AskFirst,
                tokens: TokenLedger::default(),
            },
            active_turn: None,
        }
    }

    #[tokio::test]
    async fn replacement_lane_coalesces_thread_state_to_latest_revision() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(ServerMessage::Pong).await.unwrap();
        let (delivery, replacements) = ClientDelivery::new(1, tx);
        let thread_id = ThreadId::new();

        assert!(delivery.publish_thread_state(thread_state(thread_id, 2)));
        assert!(delivery.publish_thread_state(thread_state(thread_id, 3)));
        assert!(delivery.publish_thread_state(thread_state(thread_id, 4)));

        let message = timeout(Duration::from_secs(1), replacements.recv())
            .await
            .unwrap();
        match message {
            ServerMessage::ThreadState(state) => {
                assert_eq!(state.metadata.thread_id, thread_id);
                assert_eq!(state.metadata.revision, 4);
            }
            other => panic!("expected thread state, got {other:?}"),
        }
        assert!(matches!(rx.recv().await, Some(ServerMessage::Pong)));
    }

    #[tokio::test]
    async fn older_thread_state_cannot_replace_a_newer_pending_revision() {
        let (tx, _rx) = mpsc::channel(1);
        tx.send(ServerMessage::Pong).await.unwrap();
        let (delivery, replacements) = ClientDelivery::new(2, tx);
        let thread_id = ThreadId::new();

        assert!(delivery.publish_thread_state(thread_state(thread_id, 8)));
        assert!(delivery.publish_thread_state(thread_state(thread_id, 7)));

        match timeout(Duration::from_secs(1), replacements.recv())
            .await
            .unwrap()
        {
            ServerMessage::ThreadState(state) => assert_eq!(state.metadata.revision, 8),
            other => panic!("expected thread state, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn distinct_thread_replacements_remain_independent() {
        let (tx, _rx) = mpsc::channel(1);
        tx.send(ServerMessage::Pong).await.unwrap();
        let (delivery, replacements) = ClientDelivery::new(3, tx);
        let first = ThreadId::new();
        let second = ThreadId::new();

        assert!(delivery.publish_thread_state(thread_state(first, 2)));
        assert!(delivery.publish_thread_state(thread_state(second, 5)));

        let mut revisions = HashMap::new();
        for _ in 0..2 {
            match timeout(Duration::from_secs(1), replacements.recv())
                .await
                .unwrap()
            {
                ServerMessage::ThreadState(state) => {
                    revisions.insert(state.metadata.thread_id, state.metadata.revision);
                }
                other => panic!("expected thread state, got {other:?}"),
            }
        }
        assert_eq!(revisions.get(&first), Some(&2));
        assert_eq!(revisions.get(&second), Some(&5));
    }

    #[tokio::test]
    async fn repeated_catalog_invalidations_coalesce_globally() {
        let (tx, _rx) = mpsc::channel(1);
        tx.send(ServerMessage::Pong).await.unwrap();
        let (delivery, replacements) = ClientDelivery::new(4, tx);
        for _ in 0..3 {
            assert!(delivery.invalidate_thread_catalog());
        }

        assert!(matches!(
            timeout(Duration::from_secs(1), replacements.recv())
                .await
                .unwrap(),
            ServerMessage::ThreadCatalogChanged
        ));
    }

    #[tokio::test]
    async fn publishing_replacements_does_not_wait_for_fifo_capacity() {
        let (tx, _rx) = mpsc::channel(1);
        tx.send(ServerMessage::Pong).await.unwrap();
        let (delivery, _replacements) = ClientDelivery::new(5, tx);
        let thread_id = ThreadId::new();
        timeout(Duration::from_millis(20), async {
            for revision in 1..=1_000 {
                assert!(delivery.publish_thread_state(thread_state(thread_id, revision)));
                assert!(delivery.invalidate_thread_catalog());
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn replacement_state_does_not_consume_ordered_capacity() {
        let (tx, mut rx) = mpsc::channel(2);
        let thread_id = ThreadId::new();
        let mut bootstrap = thread_state(thread_id, 1);
        bootstrap.active_turn = Some(false);
        tx.send(ServerMessage::ThreadState(bootstrap))
            .await
            .unwrap();
        tx.send(ServerMessage::Pong).await.unwrap();
        let (delivery, replacements) = ClientDelivery::new(6, tx);

        assert!(delivery.publish_thread_state(thread_state(thread_id, 2)));

        match timeout(Duration::from_secs(1), replacements.recv())
            .await
            .unwrap()
        {
            ServerMessage::ThreadState(state) => {
                assert_eq!(state.metadata.revision, 2);
                assert_eq!(state.active_turn, None);
            }
            other => panic!("expected metadata replacement, got {other:?}"),
        }

        match rx.recv().await {
            Some(ServerMessage::ThreadState(state)) => {
                assert_eq!(state.metadata.revision, 1);
                assert_eq!(state.active_turn, Some(false));
            }
            other => panic!("expected direct bootstrap, got {other:?}"),
        }
        assert!(matches!(rx.recv().await, Some(ServerMessage::Pong)));
    }
}
