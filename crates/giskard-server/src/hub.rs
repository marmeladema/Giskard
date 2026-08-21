use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard};
use std::time::Duration;

use tokio::sync::Mutex;
use tracing::{debug, error, warn};

use giskard_core::ids::{ProjectId, ThreadId};
use giskard_proto::{
    ResyncReason, SequencedThreadEvent, ServerMessage, ThreadBootstrapFrame, ThreadMetadata,
    ThreadNoticeSnapshot, ThreadRuntimeOverview, ThreadTaskSnapshot,
};

use crate::delivery::{ClientDelivery, DeliveryReceiver, DeliverySendError};

pub type ClientId = usize;
pub type SubscriptionGeneration = u64;

const COMMIT_PENDING_EVENT_ENTRIES: usize = 512;
const COMMIT_PENDING_EVENT_BYTES: usize = 2 * 1024 * 1024;
const BOOTSTRAP_FRAME_ADMISSION_TIMEOUT: Duration = Duration::from_secs(5);

pub struct Hub {
    clients: Mutex<HashMap<ClientId, Arc<ClientDelivery>>>,
    subscriptions: StdMutex<HashMap<ThreadId, HashMap<ClientId, Subscription>>>,
    next_client_id: AtomicUsize,
    next_generation: AtomicU64,
}

struct Subscription {
    delivery: Arc<ClientDelivery>,
    generation: SubscriptionGeneration,
    phase: SubscriptionPhase,
    pending_metadata: Option<ThreadMetadata>,
    pending_tasks: Option<ThreadTaskSnapshot>,
    pending_notices: Option<ThreadNoticeSnapshot>,
}

enum SubscriptionPhase {
    Bootstrapping,
    Committing {
        represented_through: u64,
        pending_events: VecDeque<SequencedThreadEvent>,
        pending_bytes: usize,
    },
    Live {
        next_seq: u64,
    },
    NeedsResync,
}

impl Hub {
    pub fn new() -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            subscriptions: StdMutex::new(HashMap::new()),
            next_client_id: AtomicUsize::new(1),
            next_generation: AtomicU64::new(1),
        }
    }

    pub fn next_client_id(&self) -> ClientId {
        self.next_client_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) async fn register_client(&self, client_id: ClientId) -> DeliveryReceiver {
        let (delivery, receiver) = ClientDelivery::new(client_id);
        if let Some(previous) = self.clients.lock().await.insert(client_id, delivery) {
            previous.close();
        }
        debug!(%client_id, action = "register_client", "client registered");
        receiver
    }

    pub async fn begin_subscribe(
        &self,
        thread_id: ThreadId,
        client_id: ClientId,
    ) -> Option<SubscriptionGeneration> {
        let delivery = self.clients.lock().await.get(&client_id).cloned()?;
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let subscription = Subscription {
            delivery,
            generation,
            phase: SubscriptionPhase::Bootstrapping,
            pending_metadata: None,
            pending_tasks: None,
            pending_notices: None,
        };
        self.lock_subscriptions()
            .entry(thread_id)
            .or_default()
            .insert(client_id, subscription);
        debug!(
            %thread_id,
            %client_id,
            subscription_generation = generation,
            action = "begin_subscribe",
            "registered bootstrapping thread subscription"
        );
        Some(generation)
    }

    pub fn prepare_bootstrap_commit(
        &self,
        thread_id: ThreadId,
        client_id: ClientId,
        generation: SubscriptionGeneration,
        represented_through: u64,
    ) -> bool {
        let mut subscriptions = self.lock_subscriptions();
        let Some(subscription) = subscriptions
            .get_mut(&thread_id)
            .and_then(|clients| clients.get_mut(&client_id))
            .filter(|subscription| subscription.generation == generation)
        else {
            return false;
        };
        if !matches!(subscription.phase, SubscriptionPhase::Bootstrapping) {
            return false;
        }
        subscription.phase = SubscriptionPhase::Committing {
            represented_through,
            pending_events: VecDeque::new(),
            pending_bytes: 0,
        };
        true
    }

    pub(crate) async fn send_bootstrap_frame(
        &self,
        thread_id: ThreadId,
        client_id: ClientId,
        generation: SubscriptionGeneration,
        frame: ThreadBootstrapFrame,
    ) -> Result<(), DeliverySendError> {
        let delivery = {
            let subscriptions = self.lock_subscriptions();
            subscriptions
                .get(&thread_id)
                .and_then(|clients| clients.get(&client_id))
                .filter(|subscription| {
                    subscription.generation == generation
                        && matches!(subscription.phase, SubscriptionPhase::Committing { .. })
                })
                .map(|subscription| subscription.delivery.clone())
        }
        .ok_or(DeliverySendError::Closed)?;

        let send = delivery.send_bootstrap(ServerMessage::ThreadBootstrap {
            thread_id,
            subscription_generation: generation,
            frame,
        });
        match tokio::time::timeout(BOOTSTRAP_FRAME_ADMISSION_TIMEOUT, send).await {
            Ok(Ok(())) if self.bootstrap_is_committing(thread_id, client_id, generation) => Ok(()),
            Ok(Ok(())) => Err(DeliverySendError::Closed),
            Ok(Err(error)) => Err(error),
            Err(_) => {
                warn!(
                    %thread_id,
                    %client_id,
                    subscription_generation = generation,
                    timeout_ms = BOOTSTRAP_FRAME_ADMISSION_TIMEOUT.as_millis(),
                    action = "send_bootstrap_frame",
                    "bootstrap frame admission timed out for a slow client"
                );
                Err(DeliverySendError::Full)
            }
        }
    }

    pub(crate) fn bootstrap_is_committing(
        &self,
        thread_id: ThreadId,
        client_id: ClientId,
        generation: SubscriptionGeneration,
    ) -> bool {
        self.lock_subscriptions()
            .get(&thread_id)
            .and_then(|clients| clients.get(&client_id))
            .is_some_and(|subscription| {
                subscription.generation == generation
                    && matches!(subscription.phase, SubscriptionPhase::Committing { .. })
            })
    }

    pub async fn finish_bootstrap(
        &self,
        thread_id: ThreadId,
        client_id: ClientId,
        generation: SubscriptionGeneration,
    ) -> bool {
        let mut resync = None;
        let mut subscriptions = self.lock_subscriptions();
        let Some(subscription) = subscriptions
            .get_mut(&thread_id)
            .and_then(|clients| clients.get_mut(&client_id))
            .filter(|subscription| subscription.generation == generation)
        else {
            return false;
        };
        let SubscriptionPhase::Committing {
            represented_through,
            mut pending_events,
            ..
        } = std::mem::replace(&mut subscription.phase, SubscriptionPhase::NeedsResync)
        else {
            return false;
        };

        let mut next_seq = represented_through.saturating_add(1);
        while let Some(event) = pending_events.pop_front() {
            if event.seq < next_seq {
                continue;
            }
            if event.seq != next_seq {
                resync = Some(ResyncReason::SequenceGap);
                break;
            }
            let message = ServerMessage::ThreadEvent {
                thread_id,
                subscription_generation: generation,
                event,
            };
            if let Err(error) = subscription.delivery.try_send_ordered(message) {
                resync = Some(match error {
                    DeliverySendError::Full | DeliverySendError::Oversized => {
                        ResyncReason::OrderedOverflow
                    }
                    DeliverySendError::Closed | DeliverySendError::Serialize => {
                        ResyncReason::SequenceGap
                    }
                });
                break;
            }
            next_seq = next_seq.saturating_add(1);
        }

        if let Some(reason) = resync {
            subscription.phase = SubscriptionPhase::NeedsResync;
            enqueue_resync(thread_id, client_id, subscription, reason);
        } else if !flush_pending_replacements(subscription) {
            subscription.phase = SubscriptionPhase::NeedsResync;
            enqueue_resync(
                thread_id,
                client_id,
                subscription,
                ResyncReason::ReplacementOverflow,
            );
        } else {
            subscription.phase = SubscriptionPhase::Live { next_seq };
            debug!(
                %thread_id,
                %client_id,
                subscription_generation = generation,
                represented_through,
                next_seq,
                action = "finish_bootstrap",
                "thread bootstrap committed"
            );
        }
        true
    }

    pub async fn fail_bootstrap(
        &self,
        thread_id: ThreadId,
        client_id: ClientId,
        generation: SubscriptionGeneration,
        reason: ResyncReason,
        retry_after_ms: Option<u64>,
    ) {
        let mut subscriptions = self.lock_subscriptions();
        let Some(subscription) = subscriptions
            .get_mut(&thread_id)
            .and_then(|clients| clients.get_mut(&client_id))
            .filter(|subscription| subscription.generation == generation)
        else {
            return;
        };
        subscription.phase = SubscriptionPhase::NeedsResync;
        enqueue_resync_after(thread_id, client_id, subscription, reason, retry_after_ms);
    }

    /// Invalidate every current subscription for one thread without disturbing other threads on
    /// the same sockets. Used when durable history advanced but its ordered runtime publication
    /// could not be committed locally.
    pub async fn require_thread_resync(&self, thread_id: ThreadId, reason: ResyncReason) {
        let mut subscriptions = self.lock_subscriptions();
        let Some(clients) = subscriptions.get_mut(&thread_id) else {
            return;
        };
        for (client_id, subscription) in clients {
            if matches!(subscription.phase, SubscriptionPhase::NeedsResync) {
                continue;
            }
            subscription.phase = SubscriptionPhase::NeedsResync;
            enqueue_resync(thread_id, *client_id, subscription, reason);
        }
    }

    pub async fn unsubscribe(&self, thread_id: ThreadId, client_id: ClientId) {
        let mut subscriptions = self.lock_subscriptions();
        if let Some(clients) = subscriptions.get_mut(&thread_id) {
            clients.remove(&client_id);
            if clients.is_empty() {
                subscriptions.remove(&thread_id);
            }
        }
        debug!(%thread_id, %client_id, action = "unsubscribe", "client unsubscribed");
    }

    /// Remove only the failed bootstrap generation. An older task must never tear down the newer
    /// subscription which replaced it on the same socket.
    pub(crate) fn unsubscribe_generation(
        &self,
        thread_id: ThreadId,
        client_id: ClientId,
        generation: SubscriptionGeneration,
    ) -> bool {
        let mut subscriptions = self.lock_subscriptions();
        let matches = subscriptions
            .get(&thread_id)
            .and_then(|clients| clients.get(&client_id))
            .is_some_and(|subscription| subscription.generation == generation);
        if !matches {
            return false;
        }
        if let Some(clients) = subscriptions.get_mut(&thread_id) {
            clients.remove(&client_id);
            if clients.is_empty() {
                subscriptions.remove(&thread_id);
            }
        }
        true
    }

    pub async fn disconnect(&self, client_id: ClientId) -> Vec<ThreadId> {
        if let Some(delivery) = self.clients.lock().await.remove(&client_id) {
            delivery.close();
        }
        let mut subscriptions = self.lock_subscriptions();
        let mut removed_threads = Vec::new();
        subscriptions.retain(|thread_id, clients| {
            if clients.remove(&client_id).is_some() {
                removed_threads.push(*thread_id);
            }
            !clients.is_empty()
        });
        debug!(%client_id, action = "disconnect", "client disconnected from all threads");
        removed_threads
    }

    pub fn has_subscribers(&self, thread_id: ThreadId) -> bool {
        self.lock_subscriptions()
            .get(&thread_id)
            .is_some_and(|clients| !clients.is_empty())
    }

    pub async fn publish_thread_event(&self, thread_id: ThreadId, event: SequencedThreadEvent) {
        let event_bytes = match serde_json::to_vec(&event) {
            Ok(encoded) => encoded.len(),
            Err(error) => {
                error!(
                    %thread_id,
                    event_seq_start = event.seq,
                    event_seq_end = event.seq,
                    %error,
                    action = "size_thread_event",
                    "failed to serialize ordered thread event"
                );
                return;
            }
        };
        let mut subscriptions = self.lock_subscriptions();
        let Some(clients) = subscriptions.get_mut(&thread_id) else {
            return;
        };
        for (client_id, subscription) in clients {
            match &mut subscription.phase {
                SubscriptionPhase::Bootstrapping => {}
                SubscriptionPhase::Committing {
                    represented_through,
                    pending_events,
                    pending_bytes,
                } => {
                    if event.seq <= *represented_through {
                        continue;
                    }
                    if pending_events.len() >= COMMIT_PENDING_EVENT_ENTRIES
                        || pending_bytes.saturating_add(event_bytes) > COMMIT_PENDING_EVENT_BYTES
                    {
                        subscription.phase = SubscriptionPhase::NeedsResync;
                        enqueue_resync(
                            thread_id,
                            *client_id,
                            subscription,
                            ResyncReason::OrderedOverflow,
                        );
                        continue;
                    }
                    pending_events.push_back(event.clone());
                    *pending_bytes += event_bytes;
                }
                SubscriptionPhase::Live { next_seq } => {
                    if event.seq < *next_seq {
                        continue;
                    }
                    if event.seq > *next_seq {
                        warn!(
                            %thread_id,
                            client_id = %client_id,
                            subscription_generation = subscription.generation,
                            expected_event_seq = *next_seq,
                            observed_event_seq = event.seq,
                            action = "publish_thread_event",
                            "ordered thread event sequence gap requires resync"
                        );
                        subscription.phase = SubscriptionPhase::NeedsResync;
                        enqueue_resync(
                            thread_id,
                            *client_id,
                            subscription,
                            ResyncReason::SequenceGap,
                        );
                        continue;
                    }
                    let message = ServerMessage::ThreadEvent {
                        thread_id,
                        subscription_generation: subscription.generation,
                        event: event.clone(),
                    };
                    match subscription.delivery.try_send_ordered(message) {
                        Ok(()) => *next_seq = next_seq.saturating_add(1),
                        Err(DeliverySendError::Full | DeliverySendError::Oversized) => {
                            subscription.phase = SubscriptionPhase::NeedsResync;
                            enqueue_resync(
                                thread_id,
                                *client_id,
                                subscription,
                                ResyncReason::OrderedOverflow,
                            );
                        }
                        Err(DeliverySendError::Closed | DeliverySendError::Serialize) => {
                            subscription.phase = SubscriptionPhase::NeedsResync;
                        }
                    }
                }
                SubscriptionPhase::NeedsResync => {}
            }
        }
    }

    pub async fn publish_thread_metadata(&self, thread_id: ThreadId, metadata: ThreadMetadata) {
        if metadata.thread_id != thread_id {
            warn!(
                %thread_id,
                message_thread_id = %metadata.thread_id,
                metadata_revision = metadata.revision,
                action = "publish_thread_metadata",
                "refusing metadata under a different subscription id"
            );
            return;
        }
        let mut subscriptions = self.lock_subscriptions();
        let Some(clients) = subscriptions.get_mut(&thread_id) else {
            return;
        };
        for (client_id, subscription) in clients {
            match subscription.phase {
                SubscriptionPhase::Bootstrapping | SubscriptionPhase::Committing { .. } => {
                    replace_newer_metadata(&mut subscription.pending_metadata, metadata.clone());
                }
                SubscriptionPhase::Live { .. } => {
                    if !subscription
                        .delivery
                        .publish_thread_metadata(metadata.clone())
                    {
                        subscription.phase = SubscriptionPhase::NeedsResync;
                        enqueue_resync(
                            thread_id,
                            *client_id,
                            subscription,
                            ResyncReason::ReplacementOverflow,
                        );
                    }
                }
                SubscriptionPhase::NeedsResync => {}
            }
        }
    }

    pub async fn publish_thread_tasks(&self, tasks: ThreadTaskSnapshot) {
        let mut subscriptions = self.lock_subscriptions();
        let Some(clients) = subscriptions.get_mut(&tasks.thread_id) else {
            return;
        };
        for (client_id, subscription) in clients {
            match subscription.phase {
                SubscriptionPhase::Bootstrapping | SubscriptionPhase::Committing { .. } => {
                    replace_newer_tasks(&mut subscription.pending_tasks, tasks.clone());
                }
                SubscriptionPhase::Live { .. } => {
                    if !subscription.delivery.publish_thread_tasks(tasks.clone()) {
                        subscription.phase = SubscriptionPhase::NeedsResync;
                        enqueue_resync(
                            tasks.thread_id,
                            *client_id,
                            subscription,
                            ResyncReason::ReplacementOverflow,
                        );
                    }
                }
                SubscriptionPhase::NeedsResync => {}
            }
        }
    }

    pub async fn publish_thread_notices(&self, notices: ThreadNoticeSnapshot) {
        let mut subscriptions = self.lock_subscriptions();
        let Some(clients) = subscriptions.get_mut(&notices.thread_id) else {
            return;
        };
        for (client_id, subscription) in clients {
            match subscription.phase {
                SubscriptionPhase::Bootstrapping | SubscriptionPhase::Committing { .. } => {
                    replace_newer_notices(&mut subscription.pending_notices, notices.clone());
                }
                SubscriptionPhase::Live { .. } => {
                    if !subscription
                        .delivery
                        .publish_thread_notices(notices.clone())
                    {
                        subscription.phase = SubscriptionPhase::NeedsResync;
                        enqueue_resync(
                            notices.thread_id,
                            *client_id,
                            subscription,
                            ResyncReason::ReplacementOverflow,
                        );
                    }
                }
                SubscriptionPhase::NeedsResync => {}
            }
        }
    }

    pub async fn publish_runtime_overview(&self, overview: ThreadRuntimeOverview) {
        let mut clients = self.clients.lock().await;
        clients.retain(|client_id, delivery| {
            let retained = delivery.publish_runtime_overview(overview.clone());
            if !retained {
                warn!(
                    %client_id,
                    overview_revision = overview.revision,
                    action = "publish_runtime_overview",
                    "client outbox rejected runtime overview"
                );
                delivery.close();
            }
            retained
        });
    }

    pub async fn invalidate_thread_catalog(&self, project_id: ProjectId) {
        let mut clients = self.clients.lock().await;
        clients.retain(|client_id, delivery| {
            let retained = delivery.invalidate_thread_catalog();
            if !retained {
                warn!(
                    %project_id,
                    %client_id,
                    action = "invalidate_thread_catalog",
                    "client outbox rejected catalog invalidation"
                );
                delivery.close();
            }
            retained
        });
    }

    pub async fn send_control(&self, client_id: ClientId, message: ServerMessage) -> bool {
        let delivery = self.clients.lock().await.get(&client_id).cloned();
        let Some(delivery) = delivery else {
            return false;
        };
        match delivery.try_send_control(message) {
            Ok(()) => true,
            Err(error) => {
                error!(
                    %client_id,
                    ?error,
                    action = "send_control",
                    "control reserve exhausted; closing unhealthy client"
                );
                delivery.close();
                false
            }
        }
    }

    fn lock_subscriptions(
        &self,
    ) -> MutexGuard<'_, HashMap<ThreadId, HashMap<ClientId, Subscription>>> {
        match self.subscriptions.lock() {
            Ok(subscriptions) => subscriptions,
            Err(poisoned) => {
                error!(
                    action = "lock_subscriptions",
                    "subscription lock was poisoned; recovering connection state"
                );
                poisoned.into_inner()
            }
        }
    }
}

impl Default for Hub {
    fn default() -> Self {
        Self::new()
    }
}

fn enqueue_resync(
    thread_id: ThreadId,
    client_id: ClientId,
    subscription: &Subscription,
    reason: ResyncReason,
) {
    enqueue_resync_after(thread_id, client_id, subscription, reason, None);
}

fn enqueue_resync_after(
    thread_id: ThreadId,
    client_id: ClientId,
    subscription: &Subscription,
    reason: ResyncReason,
    retry_after_ms: Option<u64>,
) {
    warn!(
        %thread_id,
        %client_id,
        subscription_generation = subscription.generation,
        ?reason,
        action = "require_thread_resync",
        "thread subscription requires same-socket resync"
    );
    let result = subscription
        .delivery
        .try_send_control(ServerMessage::ResyncRequired {
            thread_id,
            subscription_generation: subscription.generation,
            reason,
            retry_after_ms,
        });
    if let Err(error) = result {
        error!(
            %thread_id,
            %client_id,
            subscription_generation = subscription.generation,
            ?error,
            action = "enqueue_resync_control",
            "resync control could not use reserved capacity; closing connection"
        );
        subscription.delivery.close();
    }
}

fn flush_pending_replacements(subscription: &mut Subscription) -> bool {
    let mut admitted = true;
    if let Some(metadata) = subscription.pending_metadata.take() {
        admitted &= subscription.delivery.publish_thread_metadata(metadata);
    }
    if let Some(tasks) = subscription.pending_tasks.take() {
        admitted &= subscription.delivery.publish_thread_tasks(tasks);
    }
    if let Some(notices) = subscription.pending_notices.take() {
        admitted &= subscription.delivery.publish_thread_notices(notices);
    }
    admitted
}

fn replace_newer_metadata(slot: &mut Option<ThreadMetadata>, candidate: ThreadMetadata) {
    if slot
        .as_ref()
        .is_none_or(|current| candidate.revision >= current.revision)
    {
        *slot = Some(candidate);
    }
}

fn replace_newer_tasks(slot: &mut Option<ThreadTaskSnapshot>, candidate: ThreadTaskSnapshot) {
    if slot
        .as_ref()
        .is_none_or(|current| candidate.revision >= current.revision)
    {
        *slot = Some(candidate);
    }
}

fn replace_newer_notices(slot: &mut Option<ThreadNoticeSnapshot>, candidate: ThreadNoticeSnapshot) {
    if slot
        .as_ref()
        .is_none_or(|current| candidate.revision >= current.revision)
    {
        *slot = Some(candidate);
    }
}

#[cfg(test)]
mod tests {
    use giskard_core::model::ModelRef;
    use giskard_core::token::TokenLedger;
    use giskard_core::turn::{Mode, PermissionPreset};
    use giskard_proto::{
        ResyncReason, ThreadEventPayload, ThreadNoticeSnapshot, ThreadTaskSnapshot,
    };

    use super::*;

    #[tokio::test]
    async fn bootstrap_barrier_flushes_replacements_after_commit() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let client_id = hub.next_client_id();
        let receiver = hub.register_client(client_id).await;
        let generation = hub.begin_subscribe(thread_id, client_id).await.unwrap();

        hub.publish_thread_metadata(thread_id, thread_metadata(thread_id, 2))
            .await;
        assert!(hub.prepare_bootstrap_commit(thread_id, client_id, generation, 0));
        hub.send_bootstrap_frame(
            thread_id,
            client_id,
            generation,
            ThreadBootstrapFrame::Start {
                sections: Vec::new(),
            },
        )
        .await
        .unwrap();
        hub.publish_thread_tasks(ThreadTaskSnapshot {
            thread_id,
            revision: 3,
            tasks: Vec::new(),
        })
        .await;
        hub.publish_thread_notices(ThreadNoticeSnapshot {
            thread_id,
            revision: 4,
            notices: Vec::new(),
        })
        .await;
        hub.send_bootstrap_frame(
            thread_id,
            client_id,
            generation,
            ThreadBootstrapFrame::Commit,
        )
        .await
        .unwrap();

        assert!(matches!(
            receiver.try_recv(),
            Some(ServerMessage::ThreadBootstrap {
                frame: ThreadBootstrapFrame::Start { .. },
                ..
            })
        ));
        assert!(matches!(
            receiver.try_recv(),
            Some(ServerMessage::ThreadBootstrap {
                frame: ThreadBootstrapFrame::Commit,
                ..
            })
        ));
        assert!(receiver.try_recv().is_none());

        assert!(hub.finish_bootstrap(thread_id, client_id, generation).await);
        assert!(matches!(
            receiver.try_recv(),
            Some(ServerMessage::ThreadMetadata(ThreadMetadata {
                revision: 2,
                ..
            }))
        ));
        assert!(matches!(
            receiver.try_recv(),
            Some(ServerMessage::ThreadTasks(ThreadTaskSnapshot {
                revision: 3,
                ..
            }))
        ));
        assert!(matches!(
            receiver.try_recv(),
            Some(ServerMessage::ThreadNotices(ThreadNoticeSnapshot {
                revision: 4,
                ..
            }))
        ));
        assert!(receiver.try_recv().is_none());
    }

    #[tokio::test]
    async fn bootstrap_failure_preserves_subscription_for_delayed_same_socket_resync() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let client_id = hub.next_client_id();
        let receiver = hub.register_client(client_id).await;
        let generation = hub.begin_subscribe(thread_id, client_id).await.unwrap();

        hub.fail_bootstrap(
            thread_id,
            client_id,
            generation,
            ResyncReason::BootstrapCapacity,
            Some(500),
        )
        .await;

        assert_needs_resync(&hub, thread_id, client_id);
        assert!(matches!(
            receiver.try_recv(),
            Some(ServerMessage::ResyncRequired {
                thread_id: observed_thread,
                subscription_generation,
                reason: ResyncReason::BootstrapCapacity,
                retry_after_ms: Some(500),
            }) if observed_thread == thread_id && subscription_generation == generation
        ));
        assert_eq!(
            hub.send_bootstrap_frame(
                thread_id,
                client_id,
                generation,
                ThreadBootstrapFrame::Commit,
            )
            .await,
            Err(DeliverySendError::Closed)
        );
    }

    #[tokio::test]
    async fn failed_old_bootstrap_cannot_remove_replacement_generation() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let client_id = hub.next_client_id();
        let _receiver = hub.register_client(client_id).await;
        let old_generation = hub.begin_subscribe(thread_id, client_id).await.unwrap();
        let replacement_generation = hub.begin_subscribe(thread_id, client_id).await.unwrap();

        assert!(!hub.unsubscribe_generation(thread_id, client_id, old_generation));
        assert!(hub.has_subscribers(thread_id));
        assert!(hub.unsubscribe_generation(thread_id, client_id, replacement_generation));
        assert!(!hub.has_subscribers(thread_id));
    }

    #[tokio::test]
    async fn post_cut_event_is_flushed_exactly_once() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let client_id = hub.next_client_id();
        let receiver = hub.register_client(client_id).await;
        let generation = hub.begin_subscribe(thread_id, client_id).await.unwrap();
        assert!(hub.prepare_bootstrap_commit(thread_id, client_id, generation, 5));

        hub.publish_thread_event(thread_id, test_event(thread_id, 5))
            .await;
        hub.publish_thread_event(thread_id, test_event(thread_id, 6))
            .await;
        hub.publish_thread_event(thread_id, test_event(thread_id, 6))
            .await;
        assert!(receiver.try_recv().is_none());

        assert!(hub.finish_bootstrap(thread_id, client_id, generation).await);
        assert!(matches!(
            receiver.try_recv(),
            Some(ServerMessage::ThreadEvent {
                event: SequencedThreadEvent { seq: 6, .. },
                ..
            })
        ));
        assert!(receiver.try_recv().is_none());
    }

    #[tokio::test]
    async fn ordered_pressure_resyncs_only_slow_subscription_once() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let slow_client = hub.next_client_id();
        let fast_client = hub.next_client_id();
        let slow_receiver = hub.register_client(slow_client).await;
        let fast_receiver = hub.register_client(fast_client).await;
        let slow_generation = hub.begin_subscribe(thread_id, slow_client).await.unwrap();
        let fast_generation = hub.begin_subscribe(thread_id, fast_client).await.unwrap();
        assert!(hub.prepare_bootstrap_commit(thread_id, slow_client, slow_generation, 0));
        assert!(hub.prepare_bootstrap_commit(thread_id, fast_client, fast_generation, 0));
        assert!(
            hub.finish_bootstrap(thread_id, slow_client, slow_generation)
                .await
        );
        assert!(
            hub.finish_bootstrap(thread_id, fast_client, fast_generation)
                .await
        );

        for seq in 1..=300 {
            hub.publish_thread_event(thread_id, test_event(thread_id, seq))
                .await;
            assert!(matches!(
                fast_receiver.try_recv(),
                Some(ServerMessage::ThreadEvent {
                    event: SequencedThreadEvent { seq: observed, .. },
                    ..
                }) if observed == seq
            ));
        }

        {
            let subscriptions = hub.lock_subscriptions();
            assert!(matches!(
                subscriptions[&thread_id][&slow_client].phase,
                SubscriptionPhase::NeedsResync
            ));
            assert!(matches!(
                subscriptions[&thread_id][&fast_client].phase,
                SubscriptionPhase::Live { next_seq: 301 }
            ));
        }

        let mut resyncs = 0;
        while let Some(message) = slow_receiver.try_recv() {
            if matches!(
                message,
                ServerMessage::ResyncRequired {
                    thread_id: observed_thread,
                    subscription_generation,
                    reason: ResyncReason::OrderedOverflow,
                    ..
                } if observed_thread == thread_id && subscription_generation == slow_generation
            ) {
                resyncs += 1;
            }
        }
        assert_eq!(resyncs, 1);

        hub.publish_thread_event(thread_id, test_event(thread_id, 301))
            .await;
        assert!(slow_receiver.try_recv().is_none());
        assert!(matches!(
            fast_receiver.try_recv(),
            Some(ServerMessage::ThreadEvent {
                event: SequencedThreadEvent { seq: 301, .. },
                ..
            })
        ));
    }

    #[tokio::test]
    async fn same_socket_resubscribe_is_scoped_to_one_thread() {
        let hub = Hub::new();
        let first_thread = ThreadId::new();
        let second_thread = ThreadId::new();
        let client_id = hub.next_client_id();
        let receiver = hub.register_client(client_id).await;
        let first_generation = hub.begin_subscribe(first_thread, client_id).await.unwrap();
        let second_generation = hub.begin_subscribe(second_thread, client_id).await.unwrap();
        assert!(hub.prepare_bootstrap_commit(first_thread, client_id, first_generation, 0));
        assert!(hub.prepare_bootstrap_commit(second_thread, client_id, second_generation, 0));
        assert!(
            hub.finish_bootstrap(first_thread, client_id, first_generation)
                .await
        );
        assert!(
            hub.finish_bootstrap(second_thread, client_id, second_generation)
                .await
        );

        let replacement_generation = hub.begin_subscribe(first_thread, client_id).await.unwrap();
        assert_ne!(replacement_generation, first_generation);
        hub.publish_thread_event(second_thread, test_event(second_thread, 1))
            .await;
        hub.publish_thread_event(first_thread, test_event(first_thread, 1))
            .await;
        assert!(matches!(
            receiver.try_recv(),
            Some(ServerMessage::ThreadEvent {
                thread_id,
                subscription_generation,
                event: SequencedThreadEvent { seq: 1, .. },
            }) if thread_id == second_thread && subscription_generation == second_generation
        ));
        assert!(receiver.try_recv().is_none());

        assert!(hub.prepare_bootstrap_commit(first_thread, client_id, replacement_generation, 1,));
        assert!(
            hub.finish_bootstrap(first_thread, client_id, replacement_generation)
                .await
        );
        hub.publish_thread_event(first_thread, test_event(first_thread, 2))
            .await;
        hub.publish_thread_event(second_thread, test_event(second_thread, 2))
            .await;
        assert!(matches!(
            receiver.try_recv(),
            Some(ServerMessage::ThreadEvent {
                thread_id,
                subscription_generation,
                event: SequencedThreadEvent { seq: 2, .. },
            }) if thread_id == first_thread && subscription_generation == replacement_generation
        ));
        assert!(matches!(
            receiver.try_recv(),
            Some(ServerMessage::ThreadEvent {
                thread_id,
                subscription_generation,
                event: SequencedThreadEvent { seq: 2, .. },
            }) if thread_id == second_thread && subscription_generation == second_generation
        ));
        assert!(receiver.try_recv().is_none());
    }

    #[tokio::test]
    async fn subscriber_lifecycle_is_thread_scoped() {
        let hub = Hub::new();
        let first_thread = ThreadId::new();
        let second_thread = ThreadId::new();
        let client_id = hub.next_client_id();
        let _receiver = hub.register_client(client_id).await;
        hub.begin_subscribe(first_thread, client_id).await.unwrap();
        hub.begin_subscribe(second_thread, client_id).await.unwrap();
        assert!(hub.has_subscribers(first_thread));
        assert!(hub.has_subscribers(second_thread));

        hub.unsubscribe(first_thread, client_id).await;
        assert!(!hub.has_subscribers(first_thread));
        assert!(hub.has_subscribers(second_thread));

        assert_eq!(hub.disconnect(client_id).await, vec![second_thread]);
        assert!(!hub.has_subscribers(second_thread));
    }

    #[tokio::test]
    async fn live_replacement_pressure_requires_thread_resync() {
        let (hub, thread_id, client_id, generation, receiver, delivery) = live_subscription().await;
        fill_data_outbox(&delivery);
        hub.publish_thread_metadata(thread_id, thread_metadata(thread_id, 1))
            .await;
        assert_needs_resync(&hub, thread_id, client_id);
        assert_one_overflow_resync(&receiver, thread_id, generation);

        let (hub, thread_id, client_id, generation, receiver, delivery) = live_subscription().await;
        fill_data_outbox(&delivery);
        hub.publish_thread_tasks(ThreadTaskSnapshot {
            thread_id,
            revision: 1,
            tasks: Vec::new(),
        })
        .await;
        assert_needs_resync(&hub, thread_id, client_id);
        assert_one_overflow_resync(&receiver, thread_id, generation);

        let (hub, thread_id, client_id, generation, receiver, delivery) = live_subscription().await;
        fill_data_outbox(&delivery);
        hub.publish_thread_notices(ThreadNoticeSnapshot {
            thread_id,
            revision: 1,
            notices: Vec::new(),
        })
        .await;
        assert_needs_resync(&hub, thread_id, client_id);
        assert_one_overflow_resync(&receiver, thread_id, generation);
    }

    #[tokio::test]
    async fn bootstrap_replacement_flush_pressure_requires_thread_resync() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let client_id = hub.next_client_id();
        let receiver = hub.register_client(client_id).await;
        let generation = hub.begin_subscribe(thread_id, client_id).await.unwrap();
        hub.publish_thread_metadata(thread_id, thread_metadata(thread_id, 1))
            .await;
        assert!(hub.prepare_bootstrap_commit(thread_id, client_id, generation, 0));
        let delivery = delivery_for(&hub, thread_id, client_id);
        fill_data_outbox(&delivery);

        assert!(hub.finish_bootstrap(thread_id, client_id, generation).await);
        assert_needs_resync(&hub, thread_id, client_id);
        assert_one_overflow_resync(&receiver, thread_id, generation);
    }

    #[tokio::test]
    async fn rejected_global_replacements_close_detached_delivery() {
        let hub = Hub::new();
        let client_id = hub.next_client_id();
        let _receiver = hub.register_client(client_id).await;
        let delivery = hub.clients.lock().await[&client_id].clone();
        fill_data_outbox(&delivery);
        hub.publish_runtime_overview(ThreadRuntimeOverview {
            revision: 1,
            threads: Vec::new(),
        })
        .await;
        assert_eq!(
            delivery.try_send_control(ServerMessage::Pong),
            Err(DeliverySendError::Closed)
        );

        let hub = Hub::new();
        let client_id = hub.next_client_id();
        let _receiver = hub.register_client(client_id).await;
        let delivery = hub.clients.lock().await[&client_id].clone();
        fill_data_outbox(&delivery);
        hub.invalidate_thread_catalog(ProjectId::new()).await;
        assert_eq!(
            delivery.try_send_control(ServerMessage::Pong),
            Err(DeliverySendError::Closed)
        );
    }

    async fn live_subscription() -> (
        Hub,
        ThreadId,
        ClientId,
        SubscriptionGeneration,
        DeliveryReceiver,
        Arc<ClientDelivery>,
    ) {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let client_id = hub.next_client_id();
        let receiver = hub.register_client(client_id).await;
        let generation = hub.begin_subscribe(thread_id, client_id).await.unwrap();
        assert!(hub.prepare_bootstrap_commit(thread_id, client_id, generation, 0));
        assert!(hub.finish_bootstrap(thread_id, client_id, generation).await);
        let delivery = delivery_for(&hub, thread_id, client_id);
        (hub, thread_id, client_id, generation, receiver, delivery)
    }

    fn delivery_for(hub: &Hub, thread_id: ThreadId, client_id: ClientId) -> Arc<ClientDelivery> {
        hub.lock_subscriptions()[&thread_id][&client_id]
            .delivery
            .clone()
    }

    fn fill_data_outbox(delivery: &ClientDelivery) {
        loop {
            match delivery.try_send_ordered(ServerMessage::Pong) {
                Ok(()) => {}
                Err(DeliverySendError::Full) => return,
                Err(error) => panic!("unexpected delivery error while filling outbox: {error:?}"),
            }
        }
    }

    fn assert_needs_resync(hub: &Hub, thread_id: ThreadId, client_id: ClientId) {
        assert!(matches!(
            hub.lock_subscriptions()[&thread_id][&client_id].phase,
            SubscriptionPhase::NeedsResync
        ));
    }

    fn assert_one_overflow_resync(
        receiver: &DeliveryReceiver,
        thread_id: ThreadId,
        generation: SubscriptionGeneration,
    ) {
        let mut resyncs = 0;
        while let Some(message) = receiver.try_recv() {
            if matches!(
                message,
                ServerMessage::ResyncRequired {
                    thread_id: observed_thread,
                    subscription_generation,
                    reason: ResyncReason::ReplacementOverflow,
                    ..
                } if observed_thread == thread_id && subscription_generation == generation
            ) {
                resyncs += 1;
            }
        }
        assert_eq!(resyncs, 1);
    }

    fn test_event(thread_id: ThreadId, seq: u64) -> SequencedThreadEvent {
        SequencedThreadEvent {
            seq,
            event: ThreadEventPayload::Request {
                request: Box::new(test_request(thread_id, seq)),
            },
            history_amendment: None,
        }
    }

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

    fn test_request(thread_id: ThreadId, seq: u64) -> giskard_proto::RequestState {
        use chrono::Utc;
        use giskard_core::ids::ServerRequestId;
        use giskard_core::server_request::ServerRequest;
        use giskard_proto::{RequestPayload, RequestState, RequestStatus};

        RequestState {
            thread_id,
            request_id: seq.to_string(),
            payload: RequestPayload::Server {
                request: ServerRequest {
                    id: ServerRequestId(seq.to_string()),
                    method: "test".into(),
                    params: serde_json::json!({}),
                    received_at: Utc::now(),
                },
            },
            status: RequestStatus::Pending,
        }
    }
}
