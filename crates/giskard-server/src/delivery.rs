use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;
use tracing::{debug, error};

use giskard_core::ids::ThreadId;
use giskard_proto::{
    ServerMessage, ThreadMetadata, ThreadNoticeSnapshot, ThreadRuntimeOverview, ThreadTaskSnapshot,
};

use crate::hub::ClientId;

const DATA_CAPACITY_ENTRIES: usize = 256;
const DATA_CAPACITY_BYTES: usize = 4 * 1024 * 1024;
const CONTROL_CAPACITY_ENTRIES: usize = 32;
const CONTROL_CAPACITY_BYTES: usize = 512 * 1024;

/// A connection-owned, bounded and ordered outbox.
///
/// All classes share one dequeue order, so replacement or control traffic cannot overtake an
/// already admitted bootstrap transaction. Admission is class-aware: authoritative replacement
/// state coalesces by key, ordinary ordered data reports pressure to the subscription state
/// machine, and control messages use capacity reserved from ordinary data.
pub(crate) struct ClientDelivery {
    client_id: ClientId,
    outbox: Arc<Outbox>,
}

pub(crate) struct DeliveryReceiver {
    outbox: Arc<Outbox>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeliverySendError {
    Full,
    Closed,
    Oversized,
    Serialize,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ReplacementKey {
    ThreadMetadata(ThreadId),
    ThreadCatalog,
    RuntimeOverview,
    ThreadTasks(ThreadId),
    ThreadNotices(ThreadId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionClass {
    Data,
    Control,
}

struct QueuedMessage {
    message: ServerMessage,
    encoded_bytes: usize,
    class: AdmissionClass,
    replacement: Option<ReplacementKey>,
}

#[derive(Default)]
struct OutboxState {
    queue: VecDeque<QueuedMessage>,
    replacement_indexes: HashMap<ReplacementKey, usize>,
    data_entries: usize,
    data_bytes: usize,
    control_entries: usize,
    control_bytes: usize,
    closed: bool,
}

struct Outbox {
    client_id: ClientId,
    state: Mutex<OutboxState>,
    readable: Notify,
    writable: Notify,
}

impl ClientDelivery {
    pub(crate) fn new(client_id: ClientId) -> (Arc<Self>, DeliveryReceiver) {
        let outbox = Arc::new(Outbox::new(client_id));
        (
            Arc::new(Self {
                client_id,
                outbox: outbox.clone(),
            }),
            DeliveryReceiver { outbox },
        )
    }

    pub(crate) fn try_send_ordered(&self, message: ServerMessage) -> Result<(), DeliverySendError> {
        self.outbox.try_insert(message, AdmissionClass::Data, None)
    }

    pub(crate) fn try_send_control(&self, message: ServerMessage) -> Result<(), DeliverySendError> {
        self.outbox
            .try_insert(message, AdmissionClass::Control, None)
    }

    /// Admit a bootstrap frame without blocking a domain producer. Only a connection-owned
    /// bootstrap encoder calls this method; cancellation is enforced by the generation checks
    /// before and after each awaited admission.
    pub(crate) async fn send_bootstrap(
        &self,
        message: ServerMessage,
    ) -> Result<(), DeliverySendError> {
        let encoded_bytes = encoded_len(&message)?;
        if encoded_bytes > DATA_CAPACITY_BYTES {
            return Err(DeliverySendError::Oversized);
        }
        loop {
            let writable = self.outbox.writable.notified();
            match self.outbox.try_insert_sized(
                message.clone(),
                encoded_bytes,
                AdmissionClass::Data,
                None,
            ) {
                Ok(()) => return Ok(()),
                Err(DeliverySendError::Full) => writable.await,
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) fn publish_thread_metadata(&self, metadata: ThreadMetadata) -> bool {
        let thread_id = metadata.thread_id;
        self.outbox
            .try_insert(
                ServerMessage::ThreadMetadata(metadata),
                AdmissionClass::Data,
                Some(ReplacementKey::ThreadMetadata(thread_id)),
            )
            .is_ok()
    }

    pub(crate) fn invalidate_thread_catalog(&self) -> bool {
        self.outbox
            .try_insert(
                ServerMessage::ThreadCatalogChanged,
                AdmissionClass::Data,
                Some(ReplacementKey::ThreadCatalog),
            )
            .is_ok()
    }

    pub(crate) fn publish_runtime_overview(&self, overview: ThreadRuntimeOverview) -> bool {
        self.outbox
            .try_insert(
                ServerMessage::ThreadRuntimeOverview(overview),
                AdmissionClass::Data,
                Some(ReplacementKey::RuntimeOverview),
            )
            .is_ok()
    }

    pub(crate) fn publish_thread_tasks(&self, tasks: ThreadTaskSnapshot) -> bool {
        let thread_id = tasks.thread_id;
        self.outbox
            .try_insert(
                ServerMessage::ThreadTasks(tasks),
                AdmissionClass::Data,
                Some(ReplacementKey::ThreadTasks(thread_id)),
            )
            .is_ok()
    }

    pub(crate) fn publish_thread_notices(&self, notices: ThreadNoticeSnapshot) -> bool {
        let thread_id = notices.thread_id;
        self.outbox
            .try_insert(
                ServerMessage::ThreadNotices(notices),
                AdmissionClass::Data,
                Some(ReplacementKey::ThreadNotices(thread_id)),
            )
            .is_ok()
    }

    pub(crate) fn close(&self) {
        debug!(
            client_id = self.client_id,
            action = "close_delivery",
            "closing client outbox"
        );
        self.outbox.close();
    }
}

impl DeliveryReceiver {
    pub(crate) async fn recv(&self) -> Option<ServerMessage> {
        self.outbox.recv().await
    }

    #[cfg(test)]
    pub(crate) fn try_recv(&self) -> Option<ServerMessage> {
        self.outbox.pop_front()
    }
}

impl Outbox {
    fn new(client_id: ClientId) -> Self {
        Self {
            client_id,
            state: Mutex::new(OutboxState::default()),
            readable: Notify::new(),
            writable: Notify::new(),
        }
    }

    fn try_insert(
        &self,
        message: ServerMessage,
        class: AdmissionClass,
        replacement: Option<ReplacementKey>,
    ) -> Result<(), DeliverySendError> {
        let encoded_bytes = encoded_len(&message)?;
        self.try_insert_sized(message, encoded_bytes, class, replacement)
    }

    fn try_insert_sized(
        &self,
        message: ServerMessage,
        encoded_bytes: usize,
        class: AdmissionClass,
        replacement: Option<ReplacementKey>,
    ) -> Result<(), DeliverySendError> {
        let mut state = self.lock_state();
        if state.closed {
            return Err(DeliverySendError::Closed);
        }

        if let Some(key) = replacement
            && let Some(index) = state.replacement_indexes.get(&key).copied()
            && let Some(queued) = state.queue.get(index)
        {
            if !replacement_may_replace(self.client_id, key, &queued.message, &message) {
                return Ok(());
            }
            let old_bytes = queued.encoded_bytes;
            let queued_class = queued.class;
            let replacement_fits = match queued.class {
                AdmissionClass::Data => {
                    state
                        .data_bytes
                        .saturating_sub(old_bytes)
                        .saturating_add(encoded_bytes)
                        <= DATA_CAPACITY_BYTES
                }
                AdmissionClass::Control => {
                    state
                        .control_bytes
                        .saturating_sub(old_bytes)
                        .saturating_add(encoded_bytes)
                        <= CONTROL_CAPACITY_BYTES
                }
            };
            if !replacement_fits {
                return Err(DeliverySendError::Full);
            }
            match queued_class {
                AdmissionClass::Data => {
                    state.data_bytes = state
                        .data_bytes
                        .saturating_sub(old_bytes)
                        .saturating_add(encoded_bytes);
                }
                AdmissionClass::Control => {
                    state.control_bytes = state
                        .control_bytes
                        .saturating_sub(old_bytes)
                        .saturating_add(encoded_bytes);
                }
            }
            if let Some(queued) = state.queue.get_mut(index) {
                queued.message = message;
                queued.encoded_bytes = encoded_bytes;
            }
            debug!(
                client_id = self.client_id,
                ?key,
                action = "coalesce_replacement",
                "coalesced authoritative replacement in client outbox"
            );
            return Ok(());
        }

        if !state.can_admit(class, encoded_bytes) {
            return if encoded_bytes > state.byte_capacity(class) {
                Err(DeliverySendError::Oversized)
            } else {
                Err(DeliverySendError::Full)
            };
        }

        state.increment(class, encoded_bytes);
        let index = state.queue.len();
        if let Some(key) = replacement {
            state.replacement_indexes.insert(key, index);
        }
        state.queue.push_back(QueuedMessage {
            message,
            encoded_bytes,
            class,
            replacement,
        });
        drop(state);
        self.readable.notify_one();
        Ok(())
    }

    fn pop_front(&self) -> Option<ServerMessage> {
        let mut state = self.lock_state();
        let queued = state.queue.pop_front()?;
        state.decrement(queued.class, queued.encoded_bytes);
        if let Some(key) = queued.replacement {
            state.replacement_indexes.remove(&key);
        }
        for index in state.replacement_indexes.values_mut() {
            *index = index.saturating_sub(1);
        }
        drop(state);
        self.writable.notify_waiters();
        Some(queued.message)
    }

    async fn recv(&self) -> Option<ServerMessage> {
        loop {
            let readable = self.readable.notified();
            if let Some(message) = self.pop_front() {
                return Some(message);
            }
            if self.lock_state().closed {
                return None;
            }
            readable.await;
        }
    }

    fn close(&self) {
        let mut state = self.lock_state();
        state.closed = true;
        drop(state);
        self.readable.notify_waiters();
        self.writable.notify_waiters();
    }

    fn lock_state(&self) -> MutexGuard<'_, OutboxState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                error!(
                    client_id = self.client_id,
                    action = "lock_delivery_outbox",
                    "client outbox lock was poisoned; recovering queued state"
                );
                poisoned.into_inner()
            }
        }
    }
}

impl OutboxState {
    fn can_admit(&self, class: AdmissionClass, bytes: usize) -> bool {
        match class {
            AdmissionClass::Data => {
                self.data_entries < DATA_CAPACITY_ENTRIES
                    && self.data_bytes.saturating_add(bytes) <= DATA_CAPACITY_BYTES
            }
            AdmissionClass::Control => {
                self.control_entries < CONTROL_CAPACITY_ENTRIES
                    && self.control_bytes.saturating_add(bytes) <= CONTROL_CAPACITY_BYTES
            }
        }
    }

    fn byte_capacity(&self, class: AdmissionClass) -> usize {
        match class {
            AdmissionClass::Data => DATA_CAPACITY_BYTES,
            AdmissionClass::Control => CONTROL_CAPACITY_BYTES,
        }
    }

    fn increment(&mut self, class: AdmissionClass, bytes: usize) {
        match class {
            AdmissionClass::Data => {
                self.data_entries += 1;
                self.data_bytes += bytes;
            }
            AdmissionClass::Control => {
                self.control_entries += 1;
                self.control_bytes += bytes;
            }
        }
    }

    fn decrement(&mut self, class: AdmissionClass, bytes: usize) {
        match class {
            AdmissionClass::Data => {
                self.data_entries = self.data_entries.saturating_sub(1);
                self.data_bytes = self.data_bytes.saturating_sub(bytes);
            }
            AdmissionClass::Control => {
                self.control_entries = self.control_entries.saturating_sub(1);
                self.control_bytes = self.control_bytes.saturating_sub(bytes);
            }
        }
    }
}

fn encoded_len(message: &ServerMessage) -> Result<usize, DeliverySendError> {
    serde_json::to_vec(message)
        .map(|encoded| encoded.len())
        .map_err(|error| {
            error!(%error, action = "size_delivery_message", "failed to serialize server message");
            DeliverySendError::Serialize
        })
}

fn replacement_may_replace(
    client_id: ClientId,
    key: ReplacementKey,
    current: &ServerMessage,
    candidate: &ServerMessage,
) -> bool {
    let revisions = match (current, candidate) {
        (ServerMessage::ThreadMetadata(current), ServerMessage::ThreadMetadata(candidate)) => {
            Some((current.revision, candidate.revision))
        }
        (
            ServerMessage::ThreadRuntimeOverview(current),
            ServerMessage::ThreadRuntimeOverview(candidate),
        ) => Some((current.revision, candidate.revision)),
        (ServerMessage::ThreadTasks(current), ServerMessage::ThreadTasks(candidate)) => {
            Some((current.revision, candidate.revision))
        }
        (ServerMessage::ThreadNotices(current), ServerMessage::ThreadNotices(candidate)) => {
            Some((current.revision, candidate.revision))
        }
        (ServerMessage::ThreadCatalogChanged, ServerMessage::ThreadCatalogChanged) => None,
        _ => {
            error!(
                %client_id,
                ?key,
                action = "coalesce_replacement",
                "replacement key was reused for incompatible message kinds"
            );
            return false;
        }
    };
    let Some((current_revision, candidate_revision)) = revisions else {
        return true;
    };
    if candidate_revision < current_revision {
        debug!(
            %client_id,
            ?key,
            current_revision,
            candidate_revision,
            action = "coalesce_replacement",
            "ignoring an older authoritative replacement"
        );
        return false;
    }
    if candidate_revision == current_revision {
        let same_payload = serde_json::to_vec(current).ok() == serde_json::to_vec(candidate).ok();
        if !same_payload {
            error!(
                %client_id,
                ?key,
                current_revision,
                action = "coalesce_replacement",
                "same replacement revision has conflicting payloads; retaining the first"
            );
        }
        return same_payload;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use giskard_core::model::ModelRef;
    use giskard_core::token::TokenLedger;
    use giskard_core::turn::{Mode, PermissionPreset};

    fn thread_metadata(thread_id: ThreadId, revision: u64) -> ThreadMetadata {
        ThreadMetadata {
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
        }
    }

    #[test]
    fn replacement_stays_in_original_order_and_keeps_latest_value() {
        let (delivery, receiver) = ClientDelivery::new(1);
        let thread_id = ThreadId::new();
        assert!(delivery.publish_thread_metadata(thread_metadata(thread_id, 1)));
        delivery.try_send_control(ServerMessage::Pong).unwrap();
        assert!(delivery.publish_thread_metadata(thread_metadata(thread_id, 2)));

        match receiver.try_recv() {
            Some(ServerMessage::ThreadMetadata(metadata)) => assert_eq!(metadata.revision, 2),
            other => panic!("expected coalesced thread state, got {other:?}"),
        }
        assert!(matches!(receiver.try_recv(), Some(ServerMessage::Pong)));
    }

    #[test]
    fn control_reserve_is_independent_from_data_capacity() {
        let (delivery, receiver) = ClientDelivery::new(2);
        for _ in 0..DATA_CAPACITY_ENTRIES {
            delivery.try_send_ordered(ServerMessage::Pong).unwrap();
        }
        assert_eq!(
            delivery.try_send_ordered(ServerMessage::Pong),
            Err(DeliverySendError::Full)
        );
        assert!(delivery.try_send_control(ServerMessage::Pong).is_ok());
        drop(receiver);
    }

    #[tokio::test]
    async fn close_wakes_a_waiting_receiver() {
        let (delivery, receiver) = ClientDelivery::new(3);
        delivery.close();
        assert!(receiver.recv().await.is_none());
    }
}
