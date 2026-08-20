use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Mutex, mpsc};
use tracing::{debug, warn};

use giskard_core::event::AgentEvent;
use giskard_core::ids::ThreadId;
use giskard_proto::ServerMessage;

pub type ClientId = usize;

struct Subscription {
    tx: mpsc::Sender<ServerMessage>,
    buffered: Option<VecDeque<ServerMessage>>,
    retained_warning_buffered: bool,
}

type SubList = HashMap<ClientId, Subscription>;

pub struct Hub {
    clients: Mutex<HashMap<ClientId, mpsc::Sender<ServerMessage>>>,
    subs: Mutex<HashMap<ThreadId, SubList>>,
    pending_thread_warnings: Mutex<HashMap<ThreadId, ServerMessage>>,
    next_id: AtomicUsize,
}

impl Hub {
    pub fn new() -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            subs: Mutex::new(HashMap::new()),
            pending_thread_warnings: Mutex::new(HashMap::new()),
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
        let subscription = subs.entry(thread_id).or_default().entry(client_id);
        let subscription = subscription.insert_entry(Subscription {
            tx,
            buffered: None,
            retained_warning_buffered: false,
        });
        let mut warnings = self.pending_thread_warnings.lock().await;
        if let Some(warning) = warnings.get(&thread_id).cloned()
            && subscription.get().tx.try_send(warning).is_ok()
        {
            warnings.remove(&thread_id);
        }
        debug!(%thread_id, %client_id, "client subscribed");
    }

    /// Register before reading bootstrap snapshots, retaining every intervening live message so
    /// snapshots are always delivered first. The buffer has no independent message limit: losing
    /// transcript events or repeatedly reconnecting is worse than the memory cost. The buffer
    /// remains until snapshots are built and every captured message drains through the bounded
    /// outbound queue at socket-writer throughput.
    pub async fn subscribe_buffered(
        &self,
        thread_id: ThreadId,
        client_id: ClientId,
        tx: mpsc::Sender<ServerMessage>,
    ) {
        let mut subs = self.subs.lock().await;
        let mut subscription = subs
            .entry(thread_id)
            .or_default()
            .entry(client_id)
            .insert_entry(Subscription {
                tx,
                buffered: Some(VecDeque::new()),
                retained_warning_buffered: false,
            });
        let warnings = self.pending_thread_warnings.lock().await;
        if let Some(warning) = warnings.get(&thread_id).cloned()
            && let Some(buffered) = &mut subscription.get_mut().buffered
        {
            buffered.push_back(warning);
            subscription.get_mut().retained_warning_buffered = true;
        }
        debug!(%thread_id, %client_id, "client subscribed with live messages buffered");
    }

    /// Flush messages captured during subscribe bootstrap and make the subscriber live. It stays
    /// buffered while awaiting outbound capacity, so concurrent broadcasts append behind the
    /// messages already captured and preserve their order.
    pub async fn finish_subscribe(&self, thread_id: ThreadId, client_id: ClientId) -> bool {
        loop {
            let next = {
                let mut subs = self.subs.lock().await;
                let Some(subscription) = subs
                    .get_mut(&thread_id)
                    .and_then(|list| list.get_mut(&client_id))
                else {
                    return false;
                };
                let Some(buffered) = &mut subscription.buffered else {
                    return true;
                };
                if let Some(message) = buffered.pop_front() {
                    Some((subscription.tx.clone(), message))
                } else {
                    subscription.buffered = None;
                    if subscription.retained_warning_buffered {
                        self.pending_thread_warnings.lock().await.remove(&thread_id);
                        subscription.retained_warning_buffered = false;
                    }
                    debug!(%thread_id, %client_id, "client subscribe bootstrap completed");
                    None
                }
            };
            let Some((tx, message)) = next else {
                return true;
            };
            if let Err(error) = tx.send(message).await {
                warn!(
                    %thread_id,
                    %client_id,
                    error = %error,
                    "client outbound queue closed while flushing subscribe messages"
                );
                self.unsubscribe(thread_id, client_id).await;
                return false;
            }
        }
    }

    pub async fn unsubscribe(&self, thread_id: ThreadId, client_id: ClientId) {
        let mut subs = self.subs.lock().await;
        if let Some(list) = subs.get_mut(&thread_id) {
            list.remove(&client_id);
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
            list.remove(&client_id);
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
            list.retain(|client_id, subscription| {
                if let Some(buffered) = &mut subscription.buffered {
                    buffered.push_back(msg.clone());
                    return true;
                }
                match subscription.tx.try_send(msg.clone()) {
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
                }
            });
        }
    }

    /// Deliver sparse authoritative state without dropping it when a live client's bounded queue
    /// is temporarily full. Buffered subscribers retain normal bootstrap ordering; live sends wait
    /// concurrently without holding the subscription lock.
    pub async fn broadcast_reliably(&self, thread_id: ThreadId, msg: ServerMessage) {
        let live = {
            let mut subs = self.subs.lock().await;
            let Some(list) = subs.get_mut(&thread_id) else {
                return;
            };
            let mut live = Vec::new();
            for (client_id, subscription) in list {
                if let Some(buffered) = &mut subscription.buffered {
                    buffered.push_back(msg.clone());
                } else {
                    live.push((*client_id, subscription.tx.clone()));
                }
            }
            live
        };

        let results = futures::future::join_all(live.into_iter().map(|(client_id, tx)| {
            let message = msg.clone();
            async move {
                let result = tx.send(message).await;
                (client_id, tx, result)
            }
        }))
        .await;
        let closed = results
            .into_iter()
            .filter_map(|(client_id, tx, result)| result.err().map(|error| (client_id, tx, error)))
            .collect::<Vec<_>>();
        if closed.is_empty() {
            return;
        }

        let message_kind = server_message_kind(&msg);
        let mut subs = self.subs.lock().await;
        let Some(list) = subs.get_mut(&thread_id) else {
            return;
        };
        for (client_id, tx, error) in closed {
            warn!(
                %thread_id,
                %client_id,
                message_kind = %message_kind,
                %error,
                "client outbound queue closed during reliable broadcast"
            );
            if list
                .get(&client_id)
                .is_some_and(|subscription| subscription.tx.same_channel(&tx))
            {
                list.remove(&client_id);
            }
        }
        if list.is_empty() {
            subs.remove(&thread_id);
        }
    }

    /// Deliver a warning to current subscribers, or retain the latest one until the next
    /// subscription when the degraded operation happened before a browser attached.
    pub async fn broadcast_or_retain_warning(&self, thread_id: ThreadId, msg: ServerMessage) {
        let mut subs = self.subs.lock().await;
        let Some(list) = subs.get_mut(&thread_id).filter(|list| !list.is_empty()) else {
            self.pending_thread_warnings
                .lock()
                .await
                .insert(thread_id, msg);
            return;
        };
        let mut delivered = false;
        list.retain(|_client_id, subscription| {
            if let Some(buffered) = &mut subscription.buffered {
                buffered.push_back(msg.clone());
                subscription.retained_warning_buffered = true;
                return true;
            }
            match subscription.tx.try_send(msg.clone()) {
                Ok(()) => {
                    delivered = true;
                    true
                }
                Err(mpsc::error::TrySendError::Full(_)) => true,
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        });
        let mut warnings = self.pending_thread_warnings.lock().await;
        if delivered {
            warnings.remove(&thread_id);
        } else {
            warnings.insert(thread_id, msg);
        }
    }

    pub async fn clear_thread(&self, thread_id: ThreadId) {
        self.pending_thread_warnings.lock().await.remove(&thread_id);
    }

    pub async fn broadcast_event(&self, thread_id: ThreadId, event: AgentEvent) {
        // C1/§3.5: narrow core → wire (lossy `PathBuf → String`) at the outbound edge.
        self.broadcast(
            thread_id,
            ServerMessage::Event {
                thread_id,
                agent_event: Box::new(event.into()),
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
        ServerMessage::ThreadActivityBootstrap { .. } => "thread_activity_bootstrap",
        ServerMessage::ThreadState(_) => "thread_state",
        ServerMessage::ThreadContextWindowUpdated { .. } => "thread_context_window_updated",
        ServerMessage::HistoryPage { .. } => "history_page",
        ServerMessage::HistoryDelta { .. } => "history_delta",
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
        assert_eq!(subs.get(&thread_id).map(HashMap::len), Some(1));
        drop(subs);
        assert!(matches!(rx.try_recv(), Ok(ServerMessage::Pong)));
    }

    #[tokio::test]
    async fn reliable_broadcast_waits_for_live_queue_capacity() {
        let hub = std::sync::Arc::new(Hub::new());
        let thread_id = ThreadId::new();
        let (tx, mut rx) = mpsc::channel(1);
        hub.subscribe(thread_id, 7, tx.clone()).await;
        tx.send(ServerMessage::Pong).await.unwrap();

        let broadcasting = {
            let hub = hub.clone();
            tokio::spawn(async move {
                hub.broadcast_reliably(thread_id, ServerMessage::Pong).await;
            })
        };
        tokio::task::yield_now().await;
        assert!(!broadcasting.is_finished());
        assert!(matches!(rx.recv().await, Some(ServerMessage::Pong)));
        broadcasting.await.unwrap();
        assert!(matches!(rx.recv().await, Some(ServerMessage::Pong)));
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
        assert!(subs.get(&thread_id).is_none_or(HashMap::is_empty));
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

    #[tokio::test]
    async fn buffered_subscription_delivers_snapshot_before_live_messages() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let (tx, mut rx) = mpsc::channel(4);

        hub.subscribe_buffered(thread_id, 11, tx.clone()).await;
        hub.broadcast(thread_id, ServerMessage::Pong).await;
        tx.send(ServerMessage::ThreadState(giskard_proto::ThreadState {
            thread_id,
            state: serde_json::json!({"revision": 1}),
            active_turn: false,
        }))
        .await
        .unwrap();
        assert!(hub.finish_subscribe(thread_id, 11).await);

        assert!(matches!(
            rx.recv().await,
            Some(ServerMessage::ThreadState(_))
        ));
        assert!(matches!(rx.recv().await, Some(ServerMessage::Pong)));
    }

    #[tokio::test]
    async fn bootstrap_buffer_retains_bursts_larger_than_the_outbound_queue() {
        let hub = std::sync::Arc::new(Hub::new());
        let thread_id = ThreadId::new();
        let (tx, mut rx) = mpsc::channel(1);

        hub.subscribe_buffered(thread_id, 11, tx).await;
        for _ in 0..1_024 {
            hub.broadcast(thread_id, ServerMessage::Pong).await;
        }
        let buffered_len = hub
            .subs
            .lock()
            .await
            .get(&thread_id)
            .and_then(|subscriptions| subscriptions.get(&11))
            .and_then(|subscription| subscription.buffered.as_ref())
            .map(VecDeque::len);
        assert_eq!(buffered_len, Some(1_024));
        let finishing = {
            let hub = hub.clone();
            tokio::spawn(async move { hub.finish_subscribe(thread_id, 11).await })
        };
        for _ in 0..1_024 {
            assert!(matches!(rx.recv().await, Some(ServerMessage::Pong)));
        }
        assert!(finishing.await.unwrap());
    }

    #[tokio::test]
    async fn repeated_subscribe_replaces_the_existing_subscription() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let (old_tx, mut old_rx) = mpsc::channel(2);
        let (new_tx, mut new_rx) = mpsc::channel(2);

        hub.subscribe(thread_id, 11, old_tx).await;
        hub.subscribe(thread_id, 11, new_tx).await;
        hub.broadcast(thread_id, ServerMessage::Pong).await;

        assert!(old_rx.try_recv().is_err());
        assert!(matches!(new_rx.recv().await, Some(ServerMessage::Pong)));
        assert_eq!(
            hub.subs.lock().await.get(&thread_id).map(HashMap::len),
            Some(1)
        );
    }

    #[tokio::test]
    async fn warning_without_subscribers_is_delivered_to_the_next_subscriber() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let warning = ServerMessage::Error {
            error: giskard_proto::ErrorInfo {
                code: "restore_failed".into(),
                severity: giskard_proto::ErrorSeverity::Warning,
                message: "restore failed".into(),
                detail: None,
                thread_id: Some(thread_id),
                action: Some("restore_context_window".into()),
                process_id: None,
            },
        };
        hub.broadcast_or_retain_warning(thread_id, warning).await;

        let (tx, mut rx) = mpsc::channel(1);
        hub.subscribe(thread_id, 12, tx).await;

        assert!(matches!(
            rx.recv().await,
            Some(ServerMessage::Error { error }) if error.code == "restore_failed"
        ));
    }

    #[tokio::test]
    async fn buffered_warning_survives_a_disconnect_before_flush() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let warning = ServerMessage::Error {
            error: giskard_proto::ErrorInfo {
                code: "restore_failed".into(),
                severity: giskard_proto::ErrorSeverity::Warning,
                message: "restore failed".into(),
                detail: None,
                thread_id: Some(thread_id),
                action: Some("restore_context_window".into()),
                process_id: None,
            },
        };
        hub.broadcast_or_retain_warning(thread_id, warning).await;

        let (closed_tx, closed_rx) = mpsc::channel(1);
        drop(closed_rx);
        hub.subscribe_buffered(thread_id, 12, closed_tx).await;
        assert!(!hub.finish_subscribe(thread_id, 12).await);
        assert!(
            hub.pending_thread_warnings
                .lock()
                .await
                .contains_key(&thread_id)
        );

        let (next_tx, mut next_rx) = mpsc::channel(1);
        hub.subscribe(thread_id, 13, next_tx).await;
        assert!(matches!(
            next_rx.recv().await,
            Some(ServerMessage::Error { error }) if error.code == "restore_failed"
        ));
    }

    #[tokio::test]
    async fn warning_arriving_during_buffering_survives_a_failed_flush() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let (closed_tx, closed_rx) = mpsc::channel(1);
        hub.subscribe_buffered(thread_id, 12, closed_tx).await;

        let warning = ServerMessage::Error {
            error: giskard_proto::ErrorInfo {
                code: "restore_failed_during_bootstrap".into(),
                severity: giskard_proto::ErrorSeverity::Warning,
                message: "restore failed during bootstrap".into(),
                detail: None,
                thread_id: Some(thread_id),
                action: Some("restore_context_window".into()),
                process_id: None,
            },
        };
        hub.broadcast_or_retain_warning(thread_id, warning).await;
        hub.broadcast_or_retain_warning(
            thread_id,
            ServerMessage::Error {
                error: giskard_proto::ErrorInfo {
                    code: "latest_restore_failed_during_bootstrap".into(),
                    severity: giskard_proto::ErrorSeverity::Warning,
                    message: "latest restore failure during bootstrap".into(),
                    detail: None,
                    thread_id: Some(thread_id),
                    action: Some("restore_context_window".into()),
                    process_id: None,
                },
            },
        )
        .await;
        drop(closed_rx);

        assert!(!hub.finish_subscribe(thread_id, 12).await);
        assert!(
            hub.pending_thread_warnings
                .lock()
                .await
                .contains_key(&thread_id)
        );

        let (next_tx, mut next_rx) = mpsc::channel(1);
        hub.subscribe(thread_id, 13, next_tx).await;
        assert!(matches!(
            next_rx.recv().await,
            Some(ServerMessage::Error { error })
                if error.code == "latest_restore_failed_during_bootstrap"
        ));
    }

    #[tokio::test]
    async fn warning_is_retained_when_every_subscriber_queue_is_full() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let warning = ServerMessage::Error {
            error: giskard_proto::ErrorInfo {
                code: "restore_failed".into(),
                severity: giskard_proto::ErrorSeverity::Warning,
                message: "restore failed".into(),
                detail: None,
                thread_id: Some(thread_id),
                action: Some("restore_context_window".into()),
                process_id: None,
            },
        };
        let (full_tx, _full_rx) = mpsc::channel(1);
        full_tx.try_send(ServerMessage::Pong).unwrap();
        hub.subscribe(thread_id, 12, full_tx).await;

        hub.broadcast_or_retain_warning(thread_id, warning).await;
        assert!(
            hub.pending_thread_warnings
                .lock()
                .await
                .contains_key(&thread_id)
        );

        let (next_tx, mut next_rx) = mpsc::channel(1);
        hub.subscribe(thread_id, 13, next_tx).await;
        assert!(matches!(
            next_rx.recv().await,
            Some(ServerMessage::Error { error }) if error.code == "restore_failed"
        ));
        assert!(
            !hub.pending_thread_warnings
                .lock()
                .await
                .contains_key(&thread_id)
        );
    }
}
