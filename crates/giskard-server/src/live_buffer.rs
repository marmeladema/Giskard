use std::collections::HashMap;

use tokio::sync::Mutex;

use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{ItemDelta, ItemPayload};
use giskard_core::server_request::ServerRequest;
use giskard_core::user_input::UserInput;
use giskard_proto::{LiveTurnSnapshot, WireAgentEvent, WireApprovalRequest};

const MAX_LIVE_COMMAND_OUTPUT: usize = 16 * 1024;
const LIVE_COMMAND_OUTPUT_EDGE: usize = 8 * 1024;
const LIVE_COMMAND_OUTPUT_TRUNCATED: &str = "\n\n[... command output truncated in the live reconnect snapshot; full output is preserved on command completion ...]\n\n";

struct LiveTurn {
    turn_id: TurnId,
    user_input: Option<UserInput>,
    events: Vec<AgentEvent>,
}

pub struct LiveBufferStore {
    buffers: Mutex<HashMap<ThreadId, LiveTurn>>,
}

impl LiveBufferStore {
    pub fn new() -> Self {
        Self {
            buffers: Mutex::new(HashMap::new()),
        }
    }

    pub async fn start_turn(&self, thread_id: ThreadId) {
        self.start_turn_with_user_input(thread_id, None).await;
    }

    pub async fn start_turn_with_user_input(
        &self,
        thread_id: ThreadId,
        user_input: Option<UserInput>,
    ) {
        self.replace_turn_with_user_input(thread_id, TurnId::new(), user_input)
            .await;
    }

    pub async fn replace_turn_with_user_input(
        &self,
        thread_id: ThreadId,
        turn_id: TurnId,
        user_input: Option<UserInput>,
    ) {
        let mut buffers = self.buffers.lock().await;
        buffers.insert(
            thread_id,
            LiveTurn {
                turn_id,
                user_input,
                events: Vec::new(),
            },
        );
    }

    /// Ensure an exact turn has a reconnect buffer without replacing events already observed for
    /// it. Harnesses may publish a turn-scoped item before their delayed `TurnStarted` event; that
    /// item is still live state and must survive a browser reload.
    pub async fn ensure_turn_with_user_input(
        &self,
        thread_id: ThreadId,
        turn_id: TurnId,
        user_input: Option<UserInput>,
    ) -> Result<(), TurnId> {
        use std::collections::hash_map::Entry;

        let mut buffers = self.buffers.lock().await;
        match buffers.entry(thread_id) {
            Entry::Vacant(entry) => {
                entry.insert(LiveTurn {
                    turn_id,
                    user_input,
                    events: Vec::new(),
                });
                Ok(())
            }
            Entry::Occupied(mut entry) if entry.get().turn_id == turn_id => {
                if entry.get().user_input.is_none() && user_input.is_some() {
                    entry.get_mut().user_input = user_input;
                }
                Ok(())
            }
            Entry::Occupied(entry) => Err(entry.get().turn_id),
        }
    }

    pub async fn append(&self, thread_id: ThreadId, event: AgentEvent) {
        let mut buffers = self.buffers.lock().await;
        if let Some(turn) = buffers.get_mut(&thread_id) {
            if let AgentEvent::TurnStarted { turn: tid, .. } = &event {
                turn.turn_id = *tid;
            }
            if let Some(item_id) = completed_command_item_id(&event) {
                remove_command_output_deltas(&mut turn.events, item_id);
            }
            let command_delta_item = command_output_item_id(&event);
            let event = compact_completed_command_output(event);
            turn.events.push(event);
            if let Some(item_id) = command_delta_item {
                compact_command_output_deltas(&mut turn.events, item_id);
            }
        }
    }

    pub async fn clear_turn(&self, thread_id: ThreadId) {
        let mut buffers = self.buffers.lock().await;
        buffers.remove(&thread_id);
    }

    pub async fn is_active(&self, thread_id: ThreadId) -> bool {
        let buffers = self.buffers.lock().await;
        buffers.contains_key(&thread_id)
    }

    /// Return raw server-side lifecycle events for one Giskard item. This is intentionally not a
    /// wire snapshot: linked-thread opening needs the native routing id that wire conversion
    /// redacts before data reaches the browser.
    pub async fn item_events(&self, thread_id: ThreadId, item_id: ItemId) -> Vec<AgentEvent> {
        let buffers = self.buffers.lock().await;
        buffers
            .get(&thread_id)
            .into_iter()
            .flat_map(|turn| turn.events.iter())
            .filter(|event| match event {
                AgentEvent::ItemStarted { item, .. } => item.id == item_id,
                AgentEvent::ItemCompleted { item, .. } => item.id == item_id,
                _ => false,
            })
            .cloned()
            .collect()
    }

    pub async fn snapshot(&self, thread_id: ThreadId) -> Option<LiveTurnSnapshot> {
        let buffers = self.buffers.lock().await;
        buffers.get(&thread_id).map(|turn| {
            // C1/§3.5: the snapshot crosses the wire, so narrow core → wire here too.
            let pending_approval: Option<WireApprovalRequest> =
                turn.events.iter().rev().find_map(|e| {
                    if let AgentEvent::ApprovalRequested { request, .. } = e {
                        Some(request.clone().into())
                    } else {
                        None
                    }
                });
            let accumulated: Vec<WireAgentEvent> =
                turn.events.iter().cloned().map(Into::into).collect();
            let pending_server_requests = pending_server_requests(&turn.events);
            LiveTurnSnapshot {
                thread_id,
                turn_id: turn.turn_id,
                user_input: turn.user_input.clone(),
                accumulated,
                pending_approval,
                pending_server_requests,
            }
        })
    }
}

fn pending_server_requests(events: &[AgentEvent]) -> Vec<ServerRequest> {
    let mut requests: HashMap<ServerRequestId, ServerRequest> = HashMap::new();
    for event in events {
        match event {
            AgentEvent::ServerRequestReceived { request, .. } => {
                requests.insert(request.id.clone(), request.clone());
            }
            AgentEvent::ServerRequestResolved { request_id, .. } => {
                requests.remove(request_id);
            }
            _ => {}
        }
    }
    requests.into_values().collect()
}

fn completed_command_item_id(event: &AgentEvent) -> Option<ItemId> {
    let AgentEvent::ItemCompleted { item, .. } = event else {
        return None;
    };
    matches!(item.payload, ItemPayload::CommandExecution { .. }).then_some(item.id)
}

fn command_output_item_id(event: &AgentEvent) -> Option<ItemId> {
    let AgentEvent::ItemDelta {
        item_id,
        delta: ItemDelta::CommandOutput { .. },
        ..
    } = event
    else {
        return None;
    };
    Some(*item_id)
}

fn compact_completed_command_output(mut event: AgentEvent) -> AgentEvent {
    if let AgentEvent::ItemCompleted { item, .. } = &mut event
        && let ItemPayload::CommandExecution { output, .. } = &mut item.payload
    {
        *output = compact_command_output(output);
    }
    event
}

fn remove_command_output_deltas(events: &mut Vec<AgentEvent>, item_id: ItemId) {
    events.retain(|event| command_output_item_id(event) != Some(item_id));
}

fn compact_command_output_deltas(events: &mut Vec<AgentEvent>, item_id: ItemId) {
    let mut combined = String::new();
    for event in events.iter() {
        if command_output_item_id(event) != Some(item_id) {
            continue;
        }
        let AgentEvent::ItemDelta {
            delta: ItemDelta::CommandOutput { chunk },
            ..
        } = event
        else {
            continue;
        };
        combined.push_str(chunk);
    }

    if combined.len() <= MAX_LIVE_COMMAND_OUTPUT {
        return;
    }

    let compacted = compact_command_output(&combined);
    let mut inserted = false;
    let mut compacted_events = Vec::with_capacity(events.len());
    for mut event in events.drain(..) {
        if command_output_item_id(&event) == Some(item_id) {
            if !inserted {
                if let AgentEvent::ItemDelta {
                    delta: ItemDelta::CommandOutput { chunk },
                    ..
                } = &mut event
                {
                    *chunk = compacted.clone();
                }
                compacted_events.push(event);
                inserted = true;
            }
        } else {
            compacted_events.push(event);
        }
    }
    *events = compacted_events;
}

fn compact_command_output(output: &str) -> String {
    if output.len() <= MAX_LIVE_COMMAND_OUTPUT {
        return output.to_owned();
    }

    let head_end = floor_char_boundary(output, LIVE_COMMAND_OUTPUT_EDGE.min(output.len()));
    let tail_start = ceil_char_boundary(
        output,
        output.len().saturating_sub(LIVE_COMMAND_OUTPUT_EDGE),
    );
    format!(
        "{}{}{}",
        &output[..head_end],
        LIVE_COMMAND_OUTPUT_TRUNCATED,
        &output[tail_start..]
    )
}

fn floor_char_boundary(s: &str, mut idx: usize) -> usize {
    idx = idx.min(s.len());
    while idx > 0 && !s.is_char_boundary(idx) {
        idx -= 1;
    }
    idx
}

fn ceil_char_boundary(s: &str, mut idx: usize) -> usize {
    idx = idx.min(s.len());
    while idx < s.len() && !s.is_char_boundary(idx) {
        idx += 1;
    }
    idx
}

impl Default for LiveBufferStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use giskard_core::approval::{
        ApprovalDecision, ApprovalKind, ApprovalMetadata, ApprovalRequest,
    };
    use giskard_core::ids::ApprovalId;
    use giskard_core::ids::{ItemId, ServerRequestId, ThreadId, TurnId};
    use giskard_core::item::{CommandExecutionStart, Item, ItemKind, ItemStart};
    use giskard_core::server_request::ServerRequest;
    use giskard_proto::{WireApprovalMetadata, WireItemPayload};

    use super::*;

    fn command_start(item_id: ItemId) -> ItemStart {
        ItemStart {
            id: item_id,
            harness_item_id: "cmd_1".into(),
            kind: ItemKind::CommandExecution,
            command: Some(CommandExecutionStart {
                command: "yes".into(),
                cwd: "/tmp/project".into(),
                status: Some("in_progress".into()),
                process_id: Some("proc_1".into()),
                started_at_ms: Some(1_700_000_000_000),
            }),
            tool: None,
        }
    }

    #[tokio::test]
    async fn turn_started_does_not_discard_an_earlier_turn_item() {
        let store = LiveBufferStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let input = UserInput::text("Sub-agent turn");
        let item = AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: ItemId::new(),
                harness_item_id: "subagent-started".into(),
                payload: ItemPayload::Activity {
                    title: "Sub-agent started".into(),
                    detail: None,
                    metadata: None,
                    subagent: None,
                },
                created_at: Utc::now(),
            },
        };

        store
            .ensure_turn_with_user_input(thread, turn, Some(input.clone()))
            .await
            .expect("first event starts the exact turn buffer");
        store.append(thread, item).await;
        store
            .ensure_turn_with_user_input(thread, turn, Some(input.clone()))
            .await
            .expect("late turn start reuses the buffer");
        store
            .append(thread, AgentEvent::TurnStarted { thread, turn })
            .await;

        let snapshot = store.snapshot(thread).await.expect("snapshot");
        assert_eq!(snapshot.turn_id, turn);
        assert_eq!(snapshot.user_input, Some(input));
        assert_eq!(snapshot.accumulated.len(), 2);
        assert!(matches!(
            snapshot.accumulated[0],
            WireAgentEvent::ItemCompleted { .. }
        ));
        assert!(matches!(
            snapshot.accumulated[1],
            WireAgentEvent::TurnStarted { .. }
        ));
    }

    #[tokio::test]
    async fn command_output_deltas_are_compacted_for_live_snapshot() {
        let store = LiveBufferStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item = ItemId::new();
        store.start_turn(thread).await;
        store
            .append(thread, AgentEvent::TurnStarted { thread, turn })
            .await;
        store
            .append(
                thread,
                AgentEvent::ItemStarted {
                    thread,
                    turn,
                    item: command_start(item),
                },
            )
            .await;

        store
            .append(
                thread,
                AgentEvent::ItemDelta {
                    thread,
                    turn,
                    item_id: item,
                    delta: ItemDelta::CommandOutput {
                        chunk: format!("head\n{}", "a".repeat(MAX_LIVE_COMMAND_OUTPUT)),
                    },
                },
            )
            .await;
        store
            .append(
                thread,
                AgentEvent::ItemDelta {
                    thread,
                    turn,
                    item_id: item,
                    delta: ItemDelta::CommandOutput {
                        chunk: format!("{}\ntail", "b".repeat(MAX_LIVE_COMMAND_OUTPUT)),
                    },
                },
            )
            .await;

        let snapshot = store.snapshot(thread).await.expect("snapshot");
        let outputs = snapshot
            .accumulated
            .iter()
            .filter_map(|event| {
                let WireAgentEvent::ItemDelta { delta, .. } = event else {
                    return None;
                };
                let ItemDelta::CommandOutput { chunk } = delta else {
                    return None;
                };
                Some(chunk)
            })
            .collect::<Vec<_>>();

        assert_eq!(outputs.len(), 1);
        assert!(outputs[0].starts_with("head\n"));
        assert!(outputs[0].contains(LIVE_COMMAND_OUTPUT_TRUNCATED.trim()));
        assert!(outputs[0].ends_with("\ntail"));
    }

    #[tokio::test]
    async fn completed_command_output_is_compacted_in_live_snapshot() {
        let store = LiveBufferStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();
        let output = format!(
            "head\n{}{}\ntail",
            "a".repeat(MAX_LIVE_COMMAND_OUTPUT),
            "b".repeat(MAX_LIVE_COMMAND_OUTPUT)
        );

        store.start_turn(thread).await;
        store
            .append(
                thread,
                AgentEvent::ItemCompleted {
                    thread,
                    turn,
                    item: Item {
                        id: item_id,
                        harness_item_id: "cmd_1".into(),
                        payload: ItemPayload::CommandExecution {
                            command: "yes".into(),
                            cwd: "/tmp/project".into(),
                            output,
                            exit_code: Some(0),
                            status: Some("completed".into()),
                            process_id: Some("proc_1".into()),
                            duration_ms: Some(500),
                        },
                        created_at: Utc::now(),
                    },
                },
            )
            .await;

        let snapshot = store.snapshot(thread).await.expect("snapshot");
        let completed = snapshot.accumulated.iter().find_map(|event| {
            let WireAgentEvent::ItemCompleted { item, .. } = event else {
                return None;
            };
            let WireItemPayload::CommandExecution { output, .. } = &item.payload else {
                return None;
            };
            Some(output)
        });

        let output = completed.expect("completed command output");
        assert!(output.starts_with("head\n"));
        assert!(output.contains(LIVE_COMMAND_OUTPUT_TRUNCATED.trim()));
        assert!(output.ends_with("\ntail"));
    }

    #[tokio::test]
    async fn pending_server_requests_are_reconstructed_for_live_snapshot() {
        let store = LiveBufferStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let pending = ServerRequestId("pending".into());
        let resolved = ServerRequestId("resolved".into());

        store.start_turn(thread).await;
        store
            .append(thread, AgentEvent::TurnStarted { thread, turn })
            .await;
        for id in [pending.clone(), resolved.clone()] {
            store
                .append(
                    thread,
                    AgentEvent::ServerRequestReceived {
                        thread,
                        turn: Some(turn),
                        request: ServerRequest {
                            id,
                            method: "item/tool/call".into(),
                            params: serde_json::json!({ "tool": "example" }),
                            received_at: Utc::now(),
                        },
                    },
                )
                .await;
        }
        store
            .append(
                thread,
                AgentEvent::ServerRequestResolved {
                    thread,
                    turn: Some(turn),
                    request_id: resolved,
                },
            )
            .await;

        let snapshot = store.snapshot(thread).await.expect("snapshot");
        assert_eq!(snapshot.pending_server_requests.len(), 1);
        assert_eq!(snapshot.pending_server_requests[0].id, pending);
    }

    #[tokio::test]
    async fn pending_approval_metadata_is_reconstructed_for_live_snapshot() {
        let store = LiveBufferStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();

        store.start_turn(thread).await;
        store
            .append(
                thread,
                AgentEvent::ApprovalRequested {
                    thread,
                    turn,
                    request: ApprovalRequest {
                        id: ApprovalId("ap_1".into()),
                        kind: ApprovalKind::Permission {
                            detail: "network".into(),
                        },
                        reason: None,
                        metadata: vec![
                            ApprovalMetadata::Host {
                                label: "Network host".into(),
                                host: "api.example.com".into(),
                                protocol: Some("https".into()),
                                port: Some(443),
                                target: None,
                            },
                            ApprovalMetadata::Path {
                                label: "Grant root".into(),
                                path: "/tmp/project".into(),
                                source_link: false,
                            },
                        ],
                        available: vec![ApprovalDecision::Accept, ApprovalDecision::Decline],
                    },
                },
            )
            .await;

        let snapshot = store.snapshot(thread).await.expect("snapshot");
        let pending = snapshot.pending_approval.expect("pending approval");
        assert!(pending.metadata.iter().any(|metadata| {
            matches!(
                metadata,
                WireApprovalMetadata::Host {
                    label,
                    host,
                    protocol,
                    port,
                    ..
                } if label == "Network host"
                    && host == "api.example.com"
                    && protocol.as_deref() == Some("https")
                    && *port == Some(443)
            )
        }));
        assert!(pending.metadata.iter().any(|metadata| {
            matches!(
                metadata,
                WireApprovalMetadata::Path {
                    label,
                    path,
                    source_link,
                } if label == "Grant root" && path == "/tmp/project" && !*source_link
            )
        }));
    }
}
