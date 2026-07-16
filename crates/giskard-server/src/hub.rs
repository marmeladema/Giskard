use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Mutex, mpsc};
use tracing::{debug, warn};

use giskard_core::event::AgentEvent;
use giskard_core::ids::ThreadId;
use giskard_proto::ServerMessage;

pub type ClientId = usize;

type SubList = Vec<(ClientId, mpsc::Sender<ServerMessage>)>;

pub struct Hub {
    clients: Mutex<HashMap<ClientId, mpsc::Sender<ServerMessage>>>,
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

    pub async fn register_client(&self, client_id: ClientId, tx: mpsc::Sender<ServerMessage>) {
        self.clients.lock().await.insert(client_id, tx);
        debug!(%client_id, "client registered");
    }

    pub async fn subscribe(
        &self,
        thread_id: ThreadId,
        client_id: ClientId,
        tx: mpsc::Sender<ServerMessage>,
    ) {
        let mut subs = self.subs.lock().await;
        subs.entry(thread_id).or_default().push((client_id, tx));
        debug!(%thread_id, %client_id, "client subscribed");
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
        clients.retain(|client_id, tx| match tx.try_send(msg.clone()) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(
                    %client_id,
                    message_kind = %message_kind,
                    "client outbound queue full; dropping global message for this client"
                );
                true
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
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
            list.retain(|(client_id, tx)| match tx.try_send(msg.clone()) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!(
                        %thread_id,
                        %client_id,
                        message_kind = %message_kind,
                        "client outbound queue full; dropping message for this client"
                    );
                    true
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    warn!(
                        %thread_id,
                        %client_id,
                        message_kind = %message_kind,
                        "client outbound queue closed; removing subscription"
                    );
                    false
                }
            });
        }
    }

    pub async fn broadcast_event(&self, thread_id: ThreadId, event: AgentEvent) {
        // C1/§3.5: narrow core → wire (lossy `PathBuf → String`) at the outbound edge.
        self.broadcast(
            thread_id,
            ServerMessage::Event {
                thread_id,
                agent_event: event.into(),
            },
        )
        .await;
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
        ServerMessage::ThreadActivity(_) => "thread_activity",
        ServerMessage::ThreadState(_) => "thread_state",
        ServerMessage::HistoryPage { .. } => "history_page",
        ServerMessage::LiveTurnSnapshot(_) => "live_turn_snapshot",
        ServerMessage::RunningTasks { .. } => "running_tasks",
        ServerMessage::TokenUpdate { .. } => "token_update",
        ServerMessage::ApprovalRequest { .. } => "approval_request",
        ServerMessage::ApprovalResolved { .. } => "approval_resolved",
        ServerMessage::Error { .. } => "error",
        ServerMessage::Pong => "pong",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn full_client_queue_does_not_unsubscribe_client() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let (tx, mut rx) = mpsc::channel(1);

        hub.subscribe(thread_id, 7, tx.clone()).await;
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
        drop(rx);

        hub.subscribe(thread_id, 9, tx).await;
        hub.broadcast(thread_id, ServerMessage::Pong).await;

        let subs = hub.subs.lock().await;
        assert!(subs.get(&thread_id).is_none_or(Vec::is_empty));
    }

    #[tokio::test]
    async fn global_broadcast_reaches_unsubscribed_client() {
        let hub = Hub::new();
        let client_id = hub.next_client_id();
        let (tx, mut rx) = mpsc::channel(1);
        let thread_id = ThreadId::new();

        hub.register_client(client_id, tx).await;
        hub.broadcast_all(ServerMessage::ThreadActivity(
            giskard_proto::ThreadActivity {
                thread_id,
                kind: giskard_proto::ThreadActivityKind::ApprovalRequested {
                    approval_id: "approval-1".into(),
                },
                active_turn: true,
                summary: Some("Approval requested".into()),
            },
        ))
        .await;

        match rx.try_recv() {
            Ok(ServerMessage::ThreadActivity(activity)) => {
                assert_eq!(activity.thread_id, thread_id);
                match activity.kind {
                    giskard_proto::ThreadActivityKind::ApprovalRequested { approval_id } => {
                        assert_eq!(approval_id, "approval-1");
                    }
                    other => panic!("expected approval activity, got {other:?}"),
                }
            }
            other => panic!("expected thread activity, got {other:?}"),
        }
    }
}
