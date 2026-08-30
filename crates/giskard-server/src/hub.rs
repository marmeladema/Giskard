use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Mutex, mpsc};
use tracing::{debug, warn};

use giskard_core::event::AgentEvent;
use giskard_core::ids::{ProjectId, ThreadId};
use giskard_proto::{ServerMessage, ThreadRuntimeOverview, ThreadState, WireAgentEvent};

use crate::delivery::{ClientDelivery, DeliverySendError, ReplacementReceiver};

pub type ClientId = usize;

type SubList = Vec<(ClientId, Arc<ClientDelivery>)>;

pub struct Hub {
    clients: Mutex<HashMap<ClientId, Arc<ClientDelivery>>>,
    subs: Mutex<HashMap<ThreadId, SubList>>,
    next_id: AtomicUsize,
}

impl Hub {
    pub fn new() -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            subs: Mutex::new(HashMap::new()),
            next_id: AtomicUsize::new(1),
        }
    }

    pub fn next_client_id(&self) -> ClientId {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) async fn register_client(
        &self,
        client_id: ClientId,
        tx: mpsc::Sender<ServerMessage>,
    ) -> ReplacementReceiver {
        let (delivery, replacements) = ClientDelivery::new(client_id, tx);
        self.clients.lock().await.insert(client_id, delivery);
        debug!(%client_id, "client registered");
        replacements
    }

    pub async fn subscribe(&self, thread_id: ThreadId, client_id: ClientId) -> bool {
        let Some(delivery) = self.clients.lock().await.get(&client_id).cloned() else {
            warn!(
                %thread_id,
                %client_id,
                action = "subscribe_thread",
                "refusing a thread subscription for an unregistered client"
            );
            return false;
        };
        let mut subs = self.subs.lock().await;
        let list = subs.entry(thread_id).or_default();
        if list.iter().any(|(id, _)| *id == client_id) {
            debug!(%thread_id, %client_id, "client already subscribed");
            return true;
        }
        list.push((client_id, delivery));
        debug!(%thread_id, %client_id, "client subscribed");
        true
    }

    pub async fn unsubscribe(&self, thread_id: ThreadId, client_id: ClientId) {
        let mut subs = self.subs.lock().await;
        if let Some(list) = subs.get_mut(&thread_id) {
            list.retain(|(id, _)| *id != client_id);
            if list.is_empty() {
                subs.remove(&thread_id);
            }
        }
    }

    pub async fn disconnect(&self, client_id: ClientId) {
        self.clients.lock().await.remove(&client_id);
        let mut subs = self.subs.lock().await;
        let mut empty = Vec::new();
        for (thread_id, list) in subs.iter_mut() {
            list.retain(|(id, _)| *id != client_id);
            if list.is_empty() {
                empty.push(*thread_id);
            }
        }
        for tid in empty {
            subs.remove(&tid);
        }
        debug!(%client_id, "client disconnected from all threads");
    }

    pub async fn broadcast_all(&self, msg: ServerMessage) {
        let mut clients = self.clients.lock().await;
        let message_kind = server_message_kind(&msg);
        clients.retain(|client_id, delivery| match delivery.try_send(msg.clone()) {
            Ok(()) => true,
            Err(DeliverySendError::Full) => {
                warn!(
                    %client_id,
                    message_kind = %message_kind,
                    "client outbound queue full; dropping global message for this client"
                );
                true
            }
            Err(DeliverySendError::Closed) => {
                warn!(
                    %client_id,
                    message_kind = %message_kind,
                    "client outbound queue closed; removing global client"
                );
                false
            }
        });
    }

    pub async fn broadcast(&self, thread_id: ThreadId, msg: ServerMessage) {
        let mut subs = self.subs.lock().await;
        if let Some(list) = subs.get_mut(&thread_id) {
            let message_kind = server_message_kind(&msg);
            list.retain(
                |(client_id, delivery)| match delivery.try_send(msg.clone()) {
                    Ok(()) => true,
                    Err(DeliverySendError::Full) => {
                        warn!(
                            %thread_id,
                            %client_id,
                            message_kind = %message_kind,
                            "client outbound queue full; dropping message for this client"
                        );
                        true
                    }
                    Err(DeliverySendError::Closed) => {
                        warn!(
                            %thread_id,
                            %client_id,
                            message_kind = %message_kind,
                            "client outbound queue closed; removing subscription"
                        );
                        false
                    }
                },
            );
        }
    }

    /// Publish committed persisted metadata without waiting for a client's socket queue.
    ///
    /// Only metadata-only `ThreadState` values belong on this lane. Subscribe snapshots carry the
    /// separately-clocked `active_turn` value and remain direct FIFO messages.
    pub async fn publish_thread_metadata(&self, thread_id: ThreadId, state: ThreadState) {
        if state.metadata.thread_id != thread_id {
            warn!(
                %thread_id,
                message_thread_id = %state.metadata.thread_id,
                metadata_revision = state.metadata.revision,
                action = "publish_thread_metadata",
                "refusing to publish thread metadata under a different subscription id"
            );
            return;
        }
        if state.active_turn.is_some() {
            warn!(
                %thread_id,
                metadata_revision = state.metadata.revision,
                action = "publish_thread_metadata",
                "refusing to coalesce separately-clocked active-turn state with thread metadata"
            );
            return;
        }

        let mut subs = self.subs.lock().await;
        if let Some(list) = subs.get_mut(&thread_id) {
            list.retain(|(client_id, delivery)| {
                let retained = delivery.publish_thread_state(state.clone());
                if !retained {
                    warn!(
                        %thread_id,
                        %client_id,
                        metadata_revision = state.metadata.revision,
                        action = "publish_thread_metadata",
                        "client outbound queue closed; removing metadata subscription"
                    );
                }
                retained
            });
        }
    }

    /// Mark a project's authoritative HTTP thread catalog dirty for every connected client.
    pub async fn invalidate_thread_catalog(&self, project_id: ProjectId) {
        let mut clients = self.clients.lock().await;
        clients.retain(|client_id, delivery| {
            let retained = delivery.invalidate_thread_catalog();
            if !retained {
                warn!(
                    %project_id,
                    %client_id,
                    action = "invalidate_thread_catalog",
                    "client outbound queue closed; removing global client during catalog invalidation"
                );
            }
            retained
        });
    }

    /// Publish the complete process-local runtime projection as latest-wins replacement state.
    pub async fn publish_runtime_overview(&self, overview: ThreadRuntimeOverview) {
        let mut clients = self.clients.lock().await;
        clients.retain(|client_id, delivery| {
            let retained = delivery.publish_runtime_overview(overview.clone());
            if !retained {
                warn!(
                    %client_id,
                    runtime_revision = overview.revision,
                    action = "publish_runtime_overview",
                    "client outbound queue closed; removing global client"
                );
            }
            retained
        });
    }

    pub async fn broadcast_event(&self, thread_id: ThreadId, event: AgentEvent) {
        if matches!(
            event,
            AgentEvent::ThreadOpened { .. }
                | AgentEvent::DiffUpdated { .. }
                | AgentEvent::ContextWindowUpdated { .. }
        ) {
            debug!(
                %thread_id,
                event_kind = event_kind(&event),
                "keeping internal-only harness event off the browser stream"
            );
            return;
        }
        // C1/§3.5: narrow core → wire (lossy `PathBuf → String`) at the outbound edge.
        let Some(agent_event) = WireAgentEvent::from_agent_event(event) else {
            warn!(
                %thread_id,
                "refusing to broadcast a metadata-only event on the transcript stream"
            );
            return;
        };
        self.broadcast(
            thread_id,
            ServerMessage::Event {
                thread_id,
                agent_event: Box::new(agent_event),
            },
        )
        .await;
    }
}

fn event_kind(event: &AgentEvent) -> &'static str {
    match event {
        AgentEvent::ThreadOpened { .. } => "thread_opened",
        AgentEvent::TurnStarted { .. } => "turn_started",
        AgentEvent::ContextWindowUpdated { .. } => "context_window_updated",
        AgentEvent::ItemStarted { .. } => "item_started",
        AgentEvent::ItemDelta { .. } => "item_delta",
        AgentEvent::ItemCompleted { .. } => "item_completed",
        AgentEvent::DiffUpdated { .. } => "diff_updated",
        AgentEvent::ApprovalRequested { .. } => "approval_requested",
        AgentEvent::ServerRequestReceived { .. } => "server_request_received",
        AgentEvent::ServerRequestResolved { .. } => "server_request_resolved",
        AgentEvent::TurnCompleted { .. } => "turn_completed",
        AgentEvent::Error { .. } => "error",
        AgentEvent::Notice { .. } => "notice",
    }
}

impl Default for Hub {
    fn default() -> Self {
        Self::new()
    }
}

fn server_message_kind(msg: &ServerMessage) -> &'static str {
    match msg {
        ServerMessage::Event { .. } => "event",
        ServerMessage::RequestState(_) => "request_state",
        ServerMessage::ThreadRuntimeOverview(_) => "thread_runtime_overview",
        ServerMessage::ThreadState(_) => "thread_state",
        ServerMessage::ThreadMetadataResult { .. } => "thread_metadata_result",
        ServerMessage::ThreadCatalogChanged => "thread_catalog_changed",
        ServerMessage::HistoryDelta { .. } => "history_delta",
        ServerMessage::LiveTurnSnapshot(_) => "live_turn_snapshot",
        ServerMessage::RunningTasks { .. } => "running_tasks",
        ServerMessage::Error { .. } => "error",
        ServerMessage::Pong => "pong",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unregistered_client_cannot_create_a_receiverless_subscription() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();

        assert!(!hub.subscribe(thread_id, 99).await);
        assert!(hub.subs.lock().await.get(&thread_id).is_none());
    }

    #[tokio::test]
    async fn repeated_subscribe_is_idempotent() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let (tx, _rx) = mpsc::channel(1);

        let _replacements = hub.register_client(5, tx).await;
        assert!(hub.subscribe(thread_id, 5).await);
        assert!(hub.subscribe(thread_id, 5).await);

        assert_eq!(hub.subs.lock().await.get(&thread_id).map(Vec::len), Some(1));
    }

    #[tokio::test]
    async fn full_client_queue_does_not_unsubscribe_client() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let (tx, mut rx) = mpsc::channel(1);

        let _replacements = hub.register_client(7, tx.clone()).await;
        assert!(hub.subscribe(thread_id, 7).await);
        tx.try_send(ServerMessage::Pong).unwrap();

        hub.broadcast(thread_id, ServerMessage::Pong).await;

        let subs = hub.subs.lock().await;
        assert_eq!(subs.get(&thread_id).map(Vec::len), Some(1));
        drop(subs);
        assert!(matches!(rx.try_recv(), Ok(ServerMessage::Pong)));
    }

    #[tokio::test]
    async fn closed_client_queue_removes_subscription() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let (tx, rx) = mpsc::channel(1);
        let _replacements = hub.register_client(9, tx).await;
        drop(rx);

        assert!(hub.subscribe(thread_id, 9).await);
        hub.broadcast(thread_id, ServerMessage::Pong).await;

        let subs = hub.subs.lock().await;
        assert!(subs.get(&thread_id).is_none_or(Vec::is_empty));
    }

    #[tokio::test]
    async fn metadata_publication_survives_a_full_client_queue() {
        use giskard_core::model::ModelRef;
        use giskard_core::token::TokenLedger;
        use giskard_core::turn::{Mode, PermissionPreset};
        use giskard_proto::{ThreadMetadata, ThreadState};
        use tokio::time::{Duration, timeout};

        let hub = Hub::new();
        let client_id = hub.next_client_id();
        let thread_id = ThreadId::new();
        let (tx, mut rx) = mpsc::channel(1);
        let replacements = hub.register_client(client_id, tx.clone()).await;
        assert!(hub.subscribe(thread_id, client_id).await);
        tx.send(ServerMessage::Pong).await.unwrap();

        let state = |revision| ThreadState {
            metadata: ThreadMetadata {
                thread_id,
                revision,
                title: format!("revision {revision}"),
                mode: giskard_core::turn::TurnMode::Known(Mode::Build),
                current_model: giskard_core::turn::TurnModel::Known(ModelRef {
                    provider: "test".into(),
                    model: "test".into(),
                    reasoning_effort: None,
                }),
                context_window: 128_000,
                permission_preset: PermissionPreset::AskFirst,
                tokens: TokenLedger::default(),
            },
            active_turn: None,
        };

        timeout(
            Duration::from_millis(20),
            hub.publish_thread_metadata(thread_id, state(2)),
        )
        .await
        .unwrap();
        hub.publish_thread_metadata(thread_id, state(3)).await;

        assert!(matches!(rx.recv().await, Some(ServerMessage::Pong)));
        match timeout(Duration::from_secs(1), replacements.recv())
            .await
            .unwrap()
        {
            ServerMessage::ThreadState(state) => assert_eq!(state.metadata.revision, 3),
            other => panic!("expected thread state, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn project_invalidation_survives_a_full_client_queue() {
        use tokio::time::{Duration, timeout};

        let hub = Hub::new();
        let client_id = hub.next_client_id();
        let project_id = giskard_core::ids::ProjectId::new();
        let (tx, mut rx) = mpsc::channel(1);
        let replacements = hub.register_client(client_id, tx.clone()).await;
        tx.send(ServerMessage::Pong).await.unwrap();

        timeout(
            Duration::from_millis(20),
            hub.invalidate_thread_catalog(project_id),
        )
        .await
        .unwrap();
        hub.invalidate_thread_catalog(project_id).await;

        assert!(matches!(rx.recv().await, Some(ServerMessage::Pong)));
        assert!(matches!(
            timeout(Duration::from_secs(1), replacements.recv())
                .await
                .unwrap(),
            ServerMessage::ThreadCatalogChanged
        ));
    }

    #[tokio::test]
    async fn metadata_replacement_lane_rejects_active_turn_state() {
        use giskard_core::model::ModelRef;
        use giskard_core::token::TokenLedger;
        use giskard_core::turn::{Mode, PermissionPreset};
        use giskard_proto::{ThreadMetadata, ThreadState};
        use tokio::time::{Duration, timeout};

        let hub = Hub::new();
        let client_id = hub.next_client_id();
        let thread_id = ThreadId::new();
        let (tx, _rx) = mpsc::channel(1);
        let replacements = hub.register_client(client_id, tx.clone()).await;
        assert!(hub.subscribe(thread_id, client_id).await);

        hub.publish_thread_metadata(
            thread_id,
            ThreadState {
                metadata: ThreadMetadata {
                    thread_id,
                    revision: 2,
                    title: "Thread".into(),
                    mode: giskard_core::turn::TurnMode::Known(Mode::Build),
                    current_model: giskard_core::turn::TurnModel::Known(ModelRef {
                        provider: "test".into(),
                        model: "test".into(),
                        reasoning_effort: None,
                    }),
                    context_window: 128_000,
                    permission_preset: PermissionPreset::AskFirst,
                    tokens: TokenLedger::default(),
                },
                active_turn: Some(true),
            },
        )
        .await;

        assert!(
            timeout(Duration::from_millis(20), replacements.recv())
                .await
                .is_err()
        );
    }
}
