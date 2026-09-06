use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Mutex, mpsc};
use tracing::{debug, warn};

use giskard_core::event::AgentEvent;
use giskard_core::ids::{ProjectId, ThreadId};
use giskard_core::user_input::UserInput;
use giskard_proto::{
    ErrorInfo, RunningTask, ServerMessage, ThreadRuntimeOverview, ThreadState, WireAgentEvent,
    WireCommandOutput, WireItem,
};

use crate::delivery::{ClientDelivery, DeliverySendError, ReplacementReceiver};
use crate::thread_runtime::{AppliedRuntimeEvent, is_internal_event};

/// Everything the server publishes to the browsers subscribed to one thread. Each variant is
/// one row of spec §13.6.1: the hub, not the caller, chooses the lane and does the core→wire
/// narrowing (§3.5).
pub enum Outbound {
    /// An agent event for the ordered transcript FIFO. Internal-only kinds are dropped here.
    Transcript {
        /// Boxed: an `AgentEvent` dwarfs every other variant's payload.
        event: Box<AgentEvent>,
        /// Attached to `TurnStarted` only: the prompt text for externally started turns.
        user_input: Option<UserInput>,
        /// Attached to `ItemCompleted` only: the durable command output of a late completion.
        command_output: Option<WireCommandOutput>,
    },
    /// The running-tasks projection as a revisioned snapshot. On the ordered lane today; the
    /// spec table lists it as revisioned replacement, a lane change this step does not make.
    RunningTasks {
        revision: u64,
        tasks: Vec<RunningTask>,
    },
    /// What one applied event changed: request state, running tasks, overview, in that order.
    RuntimeEffects(AppliedRuntimeEvent),
    /// Committed persisted metadata, on the per-connection replacement lane.
    Metadata(ThreadState),
    /// A thread-scoped error for the ordered lane.
    Error(ErrorInfo),
}

pub type ClientId = usize;

type SubList = Vec<(ClientId, Arc<ClientDelivery>)>;

pub struct Hub {
    clients: Mutex<HashMap<ClientId, Arc<ClientDelivery>>>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Route thread-scoped server messages to subscribed browser connections.
    // Source of truth: Client subscription actions define this delivery membership only.
    // Structural reason: Delivery routing spans authorities and is not entity-local state.
    // Synchronization: The subs mutex protects subscription lookup, addition, and removal.
    // Invalidation/removal: Unsubscribe and client removal delete the corresponding routes.
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

    async fn send_ordered(&self, thread_id: ThreadId, msg: ServerMessage) {
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
    async fn publish_metadata(&self, thread_id: ThreadId, state: ThreadState) {
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

    /// The one entry point for everything published to the browsers subscribed to a thread.
    ///
    /// The lane and the core→wire narrowing are chosen here so no caller has to.
    pub async fn publish(&self, thread_id: ThreadId, outbound: Outbound) {
        match outbound {
            Outbound::Transcript {
                event,
                user_input,
                command_output,
            } => {
                if is_internal_event(&event) {
                    debug!(
                        %thread_id,
                        event_kind = event.kind(),
                        "keeping internal-only harness event off the browser stream"
                    );
                    return;
                }
                let agent_event = match (*event, command_output) {
                    // A late command completion is the one event whose wire form needs the durable
                    // output descriptor, and `from_item_with_command_output` takes the core `Item`,
                    // so it is applied before `from_agent_event` would consume the event.
                    (AgentEvent::ItemCompleted { thread, turn, item }, Some(command_output)) => {
                        WireAgentEvent::ItemCompleted {
                            thread,
                            turn,
                            item: WireItem::from_item_with_command_output(
                                item,
                                Some(command_output),
                            ),
                        }
                    }
                    (event, _) => {
                        // C1/§3.5: narrow core → wire (lossy `PathBuf → String`) at the outbound
                        // edge.
                        let Some(mut agent_event) = WireAgentEvent::from_agent_event(event) else {
                            warn!(
                                %thread_id,
                                "refusing to broadcast a metadata-only event on the transcript stream"
                            );
                            return;
                        };
                        // `from_agent_event` always writes `user_input: None`; the prompt of an
                        // externally started turn is filled in here rather than by a second mapping.
                        if let WireAgentEvent::TurnStarted {
                            user_input: slot, ..
                        } = &mut agent_event
                        {
                            *slot = user_input;
                        }
                        agent_event
                    }
                };
                self.send_ordered(
                    thread_id,
                    ServerMessage::Event {
                        thread_id,
                        agent_event: Box::new(agent_event),
                    },
                )
                .await;
            }
            Outbound::RunningTasks { revision, tasks } => {
                self.send_running_tasks(thread_id, revision, tasks).await;
            }
            Outbound::RuntimeEffects(applied) => {
                if let Some(request) = applied.request_state {
                    self.send_ordered(thread_id, ServerMessage::RequestState(request))
                        .await;
                }
                if let Some(tasks) = applied.running_tasks_if_changed {
                    self.send_running_tasks(thread_id, tasks.revision, tasks.tasks)
                        .await;
                }
                if let Some(overview) = applied.overview_if_changed {
                    self.publish_runtime_overview(overview).await;
                }
            }
            Outbound::Metadata(state) => {
                self.publish_metadata(thread_id, state).await;
            }
            Outbound::Error(error) => {
                self.send_ordered(thread_id, ServerMessage::Error { error })
                    .await;
            }
        }
    }

    async fn send_running_tasks(
        &self,
        thread_id: ThreadId,
        revision: u64,
        tasks: Vec<RunningTask>,
    ) {
        self.send_ordered(
            thread_id,
            ServerMessage::RunningTasks {
                thread_id,
                revision,
                tasks,
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
    async fn live_usage_is_broadcast_while_diff_stays_internal() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let turn = giskard_core::ids::TurnId::new();
        let (tx, mut rx) = mpsc::channel(2);
        let _replacements = hub.register_client(1, tx).await;
        assert!(hub.subscribe(thread_id, 1).await);

        hub.publish(
            thread_id,
            Outbound::Transcript {
                event: Box::new(AgentEvent::TurnUsageUpdated {
                    thread: thread_id,
                    turn,
                    usage: giskard_core::token::TokenUsage {
                        input: 10,
                        output: 1,
                        total: 11,
                    },
                    context_window: Some(258_400),
                    model: None,
                }),
                user_input: None,
                command_output: None,
            },
        )
        .await;
        assert!(matches!(
            rx.try_recv(),
            Ok(ServerMessage::Event { agent_event, .. })
                if matches!(*agent_event, WireAgentEvent::TurnUsageUpdated { .. })
        ));

        hub.publish(
            thread_id,
            Outbound::Transcript {
                event: Box::new(AgentEvent::DiffUpdated {
                    thread: thread_id,
                    turn,
                    diff: giskard_core::diff::FileDiff {
                        path: "src/lib.rs".into(),
                        change: giskard_core::item::FileChangeKind::Modified,
                        old_text: None,
                        new_text: None,
                        hunks: Vec::new(),
                        binary: false,
                        captured: None,
                    },
                }),
                user_input: None,
                command_output: None,
            },
        )
        .await;
        assert!(rx.try_recv().is_err());

        hub.publish(
            thread_id,
            Outbound::Transcript {
                event: Box::new(AgentEvent::ThreadOpened {
                    thread: thread_id,
                    harness_thread_id: "native-1".into(),
                }),
                user_input: None,
                command_output: None,
            },
        )
        .await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn transcript_attaches_user_input_to_turn_started() {
        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let turn = giskard_core::ids::TurnId::new();
        let (tx, mut rx) = mpsc::channel(2);
        let _replacements = hub.register_client(1, tx).await;
        assert!(hub.subscribe(thread_id, 1).await);

        hub.publish(
            thread_id,
            Outbound::Transcript {
                event: Box::new(AgentEvent::TurnStarted {
                    thread: thread_id,
                    turn,
                }),
                user_input: Some(UserInput::text("prompt")),
                command_output: None,
            },
        )
        .await;
        match rx.try_recv() {
            Ok(ServerMessage::Event { agent_event, .. }) => match *agent_event {
                WireAgentEvent::TurnStarted { user_input, .. } => {
                    assert_eq!(
                        user_input.as_ref().and_then(UserInput::as_text),
                        Some("prompt")
                    );
                }
                other => panic!("expected turn started, got {other:?}"),
            },
            other => panic!("expected event message, got {other:?}"),
        }

        // The prompt rides along on every event of an externally started turn, but only
        // `TurnStarted` carries it on the wire.
        hub.publish(
            thread_id,
            Outbound::Transcript {
                event: Box::new(AgentEvent::ItemDelta {
                    thread: thread_id,
                    turn,
                    item_id: giskard_core::ids::ItemId::new(),
                    delta: giskard_core::item::ItemDelta::Text {
                        text: "hello".into(),
                    },
                }),
                user_input: Some(UserInput::text("prompt")),
                command_output: None,
            },
        )
        .await;
        match rx.try_recv() {
            Ok(ServerMessage::Event { agent_event, .. }) => match *agent_event {
                WireAgentEvent::ItemDelta { delta, .. } => {
                    assert!(matches!(
                        delta,
                        giskard_core::item::ItemDelta::Text { text } if text == "hello"
                    ));
                }
                other => panic!("expected item delta, got {other:?}"),
            },
            other => panic!("expected event message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn transcript_applies_late_command_output() {
        use giskard_core::item::{Item, ItemPayload};

        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let turn = giskard_core::ids::TurnId::new();
        let (tx, mut rx) = mpsc::channel(2);
        let _replacements = hub.register_client(1, tx).await;
        assert!(hub.subscribe(thread_id, 1).await);

        let item = || Item {
            id: giskard_core::ids::ItemId::new(),
            harness_item_id: "command-1".into(),
            payload: ItemPayload::CommandExecution {
                command: "printf ok".into(),
                cwd: std::path::PathBuf::from("/tmp/project"),
                output: "ok\n".into(),
                output_truncated: false,
                output_original_bytes: Some(999),
                output_original_lines: Some(88),
                exit_code: Some(0),
                status: Some("completed".into()),
                process_id: None,
                duration_ms: None,
            },
            created_at: chrono::Utc::now(),
        };
        let descriptor =
            giskard_core::CommandOutputDescriptor::from_durable("ok\n", false, 3, 1, false);

        let received_output = |message| match message {
            Ok(ServerMessage::Event { agent_event, .. }) => match *agent_event {
                WireAgentEvent::ItemCompleted { item, .. } => match item.payload {
                    giskard_proto::WireItemPayload::CommandExecution { output, .. } => output,
                    other => panic!("expected command execution payload, got {other:?}"),
                },
                other => panic!("expected completed item, got {other:?}"),
            },
            other => panic!("expected event message, got {other:?}"),
        };

        hub.publish(
            thread_id,
            Outbound::Transcript {
                event: Box::new(AgentEvent::ItemCompleted {
                    thread: thread_id,
                    turn,
                    item: item(),
                }),
                user_input: None,
                command_output: Some(descriptor.clone()),
            },
        )
        .await;
        assert_eq!(received_output(rx.try_recv()), descriptor);

        hub.publish(
            thread_id,
            Outbound::Transcript {
                event: Box::new(AgentEvent::ItemCompleted {
                    thread: thread_id,
                    turn,
                    item: item(),
                }),
                user_input: None,
                command_output: None,
            },
        )
        .await;
        let plain: WireItem = item().into();
        let giskard_proto::WireItemPayload::CommandExecution {
            output: expected, ..
        } = plain.payload
        else {
            panic!("expected command execution payload");
        };
        assert_eq!(received_output(rx.try_recv()), expected);
    }

    #[tokio::test]
    async fn runtime_effects_keep_request_tasks_overview_order() {
        use crate::registry::ThreadAuthority;
        use crate::thread_runtime::ThreadRuntimeSupport;
        use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
        use giskard_core::ids::ApprovalId;
        use giskard_core::item::{CommandExecutionStart, ItemKind, ItemStart};
        use tokio::time::{Duration, timeout};

        let hub = Hub::new();
        let thread_id = ThreadId::new();
        let turn = giskard_core::ids::TurnId::new();
        let (tx, mut rx) = mpsc::channel(4);
        let replacements = hub.register_client(1, tx).await;
        assert!(hub.subscribe(thread_id, 1).await);

        // `AppliedRuntimeEvent` keeps a private field, so the effects come from a real runtime:
        // one event that moves the running tasks, one that registers a request.
        let runtime = ThreadRuntimeSupport::default();
        let authority = Arc::new(ThreadAuthority::new_for_test(
            thread_id,
            giskard_core::ids::ProjectId::new(),
        ));
        let mut applied = runtime.apply_event(
            &authority,
            &AgentEvent::ItemStarted {
                thread: thread_id,
                turn,
                item: ItemStart {
                    id: giskard_core::ids::ItemId::new(),
                    harness_item_id: "command-1".into(),
                    kind: ItemKind::CommandExecution,
                    command: Some(CommandExecutionStart {
                        command: "sleep 1".into(),
                        cwd: "/tmp/project".into(),
                        status: Some("in_progress".into()),
                        process_id: None,
                        started_at_ms: None,
                    }),
                    tool: None,
                },
            },
            false,
        );
        let requested = runtime.apply_event(
            &authority,
            &AgentEvent::ApprovalRequested {
                thread: thread_id,
                turn,
                request: ApprovalRequest {
                    id: ApprovalId("approval-1".into()),
                    kind: ApprovalKind::Permission {
                        detail: "test".into(),
                    },
                    reason: None,
                    metadata: Vec::new(),
                    available: vec![ApprovalDecision::Accept],
                },
            },
            false,
        );
        applied.request_state = requested.request_state;
        applied.overview_if_changed = Some(runtime.current_overview());
        assert!(applied.request_state.is_some());
        assert!(applied.running_tasks_if_changed.is_some());

        hub.publish(thread_id, Outbound::RuntimeEffects(applied))
            .await;

        assert!(matches!(rx.try_recv(), Ok(ServerMessage::RequestState(_))));
        assert!(matches!(
            rx.try_recv(),
            Ok(ServerMessage::RunningTasks { thread_id: tid, .. }) if tid == thread_id
        ));
        assert!(rx.try_recv().is_err());
        assert!(matches!(
            timeout(Duration::from_secs(1), replacements.recv())
                .await
                .unwrap(),
            ServerMessage::ThreadRuntimeOverview(_)
        ));
    }

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

        hub.send_ordered(thread_id, ServerMessage::Pong).await;

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
        hub.send_ordered(thread_id, ServerMessage::Pong).await;

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
            hub.publish(thread_id, Outbound::Metadata(state(2))),
        )
        .await
        .unwrap();
        hub.publish(thread_id, Outbound::Metadata(state(3))).await;

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

        hub.publish(
            thread_id,
            Outbound::Metadata(ThreadState {
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
            }),
        )
        .await;

        assert!(
            timeout(Duration::from_millis(20), replacements.recv())
                .await
                .is_err()
        );
    }
}
