use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};

use tokio::sync::Mutex;

use giskard_core::approval::{ApprovalDecision, ApprovalRequest};
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{ItemDelta, ItemPayload};
use giskard_core::server_request::ServerRequest;
use giskard_core::user_input::UserInput;
use giskard_proto::{AnsweredApproval, LiveTurnSnapshot, WireAgentEvent, WireApprovalRequest};

const MAX_LIVE_COMMAND_OUTPUT: usize = 16 * 1024;
const LIVE_COMMAND_OUTPUT_EDGE: usize = 8 * 1024;
const LIVE_COMMAND_OUTPUT_TRUNCATED: &str = "\n\n[... command output truncated in the live reconnect snapshot; full output is preserved on command completion ...]\n\n";

struct LiveTurn {
    turn_id: TurnId,
    user_input: Option<UserInput>,
    events: Vec<AgentEvent>,
    /// Approvals the user answered during this turn, and the decision they made. Resolution is not
    /// otherwise recorded in `events` (there is no harness-emitted approval-resolved event), so
    /// without this the reconnect snapshot would replay every answered approval as still pending.
    resolved_approvals: HashMap<ApprovalId, ApprovalDecision>,
    /// Server requests the user answered during this turn.
    ///
    /// Unlike approvals these *do* have a harness-emitted resolved event, but it arrives on the
    /// harness's own schedule — and a harness may never send one. Until it lands the request is
    /// still "received but not resolved" as far as `events` is concerned, so a reload in that window
    /// would replay it as actionable and re-answering routes a stale id to the harness. The answer
    /// is recorded here the moment it is routed, which closes that window.
    resolved_server_requests: HashSet<ServerRequestId>,
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
                resolved_approvals: HashMap::new(),
                resolved_server_requests: HashSet::new(),
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
                    resolved_approvals: HashMap::new(),
                    resolved_server_requests: HashSet::new(),
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

    /// Record that the user answered an approval in the thread's in-flight turn, so the reconnect
    /// snapshot renders it resolved instead of re-prompting (spec §13.6). No-op if the thread has no
    /// live turn (e.g. the turn already completed and its buffer was cleared).
    pub async fn resolve_approval(
        &self,
        thread_id: ThreadId,
        approval_id: ApprovalId,
        decision: ApprovalDecision,
    ) {
        let mut buffers = self.buffers.lock().await;
        if let Some(turn) = buffers.get_mut(&thread_id) {
            turn.resolved_approvals.insert(approval_id, decision);
        }
    }

    /// Record that the user answered a server request in the thread's in-flight turn. Mirrors
    /// [`Self::resolve_approval`]: the harness's own resolved event may be late or may never come,
    /// and until then a reconnect would replay the request as actionable.
    pub async fn resolve_server_request(&self, thread_id: ThreadId, request_id: ServerRequestId) {
        let mut buffers = self.buffers.lock().await;
        if let Some(turn) = buffers.get_mut(&thread_id) {
            turn.resolved_server_requests.insert(request_id);
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
            // Answered approvals still ride along in `accumulated`; the client renders them resolved
            // from this list rather than re-prompting.
            let answered_approvals: Vec<AnsweredApproval> = turn
                .events
                .iter()
                .filter_map(|e| match e {
                    AgentEvent::ApprovalRequested { request, .. } => turn
                        .resolved_approvals
                        .get(&request.id)
                        .map(|decision| AnsweredApproval {
                            request_id: request.id.clone(),
                            decision: decision.clone(),
                        }),
                    _ => None,
                })
                .collect();
            let accumulated: Vec<WireAgentEvent> = turn
                .events
                .iter()
                .cloned()
                .filter_map(WireAgentEvent::from_agent_event)
                .collect();
            // Answered requests still ride along in `accumulated` as `ServerRequestReceived`, and
            // replaying that renders an actionable card. Naming them lets the client render those
            // resolved instead, exactly as `answered_approvals` does.
            let answered_server_requests: Vec<ServerRequestId> = turn
                .events
                .iter()
                .filter_map(|e| match e {
                    AgentEvent::ServerRequestReceived { request, .. }
                        if turn.resolved_server_requests.contains(&request.id) =>
                    {
                        Some(request.id.clone())
                    }
                    _ => None,
                })
                .collect();
            LiveTurnSnapshot {
                thread_id,
                turn_id: turn.turn_id,
                user_input: turn.user_input.clone(),
                accumulated,
                answered_approvals,
                answered_server_requests,
            }
        })
    }

    /// Every thread currently waiting on the user, across all in-flight turns.
    ///
    /// `ThreadActivity` is broadcast the instant an approval is raised and is never replayed, so a
    /// browser that was not connected at that moment learns nothing about it — no sidebar badge, no
    /// notification — until it happens to open that thread. This is the state a connecting client
    /// needs to catch up. A buffer exists exactly while a turn is in flight, so "has a live buffer
    /// with an unanswered approval" is the same question as "is blocked right now". Answered
    /// approvals are filtered out for the same reason `snapshot` filters them.
    pub async fn pending_attention(&self) -> Vec<PendingAttention> {
        let buffers = self.buffers.lock().await;
        buffers
            .iter()
            .filter_map(|(thread_id, turn)| {
                let approvals = pending_approvals(&turn.events, &turn.resolved_approvals);
                let server_requests =
                    pending_server_requests(&turn.events, &turn.resolved_server_requests);
                if approvals.is_empty() && server_requests.is_empty() {
                    return None;
                }
                Some(PendingAttention {
                    thread_id: *thread_id,
                    approvals,
                    server_requests,
                })
            })
            .collect()
    }
}

/// One thread's outstanding user-facing requests, as needed to rebuild a connecting client's
/// cross-thread activity view.
#[derive(Debug)]
pub struct PendingAttention {
    pub thread_id: ThreadId,
    pub approvals: Vec<WireApprovalRequest>,
    pub server_requests: Vec<ServerRequest>,
}

/// Every approval the turn is still waiting on the user for, oldest first.
///
/// A turn can be blocked on several approvals at once (three commands proposed together, say), so
/// this is a list, not a single value. A re-sent id is the same approval with a fresher payload, so
/// the latest occurrence wins and the order is the first occurrence of each id. Approvals have no
/// harness-emitted resolved event, so "still waiting" is exactly "not in `resolved_approvals`".
fn pending_approvals(
    events: &[AgentEvent],
    resolved_approvals: &HashMap<ApprovalId, ApprovalDecision>,
) -> Vec<WireApprovalRequest> {
    let mut order: Vec<ApprovalId> = Vec::new();
    let mut latest: HashMap<ApprovalId, &ApprovalRequest> = HashMap::new();
    for event in events {
        if let AgentEvent::ApprovalRequested { request, .. } = event
            && !resolved_approvals.contains_key(&request.id)
        {
            if !latest.contains_key(&request.id) {
                order.push(request.id.clone());
            }
            latest.insert(request.id.clone(), request);
        }
    }
    order
        .into_iter()
        .map(|id| latest.remove(&id).expect("seen").clone().into())
        .collect()
}

fn pending_server_requests(
    events: &[AgentEvent],
    resolved_server_requests: &HashSet<ServerRequestId>,
) -> Vec<ServerRequest> {
    // An `IndexMap` keeps arrival order deterministically (unlike `HashMap::into_values`, which
    // iterated by hash bucket) so the bootstrap names the same request in the sidebar every reload.
    // `insert` on receive updates the payload in place when the id is already present, or appends
    // it when it is new; `shift_remove` on resolve drops it. A re-sent id after a resolution
    // re-inserts at the end, so a reopen moves to the back rather than keeping its first-seen
    // position — the reopened request is the newest thing demanding attention. Mirrors
    // `outstandingServerRequests` in the browser.
    let mut pending: IndexMap<ServerRequestId, &ServerRequest> = IndexMap::new();
    for event in events {
        match event {
            AgentEvent::ServerRequestReceived { request, .. } => {
                pending.insert(request.id.clone(), request);
            }
            AgentEvent::ServerRequestResolved { request_id, .. } => {
                pending.shift_remove(request_id);
            }
            _ => {}
        }
    }
    pending
        .into_iter()
        .filter(|(id, _)| !resolved_server_requests.contains(id))
        .map(|(_, request)| request.clone())
        .collect()
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
    use std::collections::HashSet;

    use chrono::Utc;
    use giskard_core::approval::{ApprovalKind, ApprovalMetadata};
    use giskard_core::ids::ApprovalId;
    use giskard_core::ids::{ItemId, ServerRequestId, ThreadId, TurnId};
    use giskard_core::item::{CommandExecutionStart, Item, ItemKind, ItemStart};
    use giskard_core::server_request::ServerRequest;
    use giskard_proto::{WireApprovalMetadata, WireItemPayload};

    use super::*;

    /// The approvals a reconnecting client would still treat as actionable, in arrival order.
    /// Mirrors `outstandingApprovals` in the browser so a server-side test can assert what the
    /// client will derive from the snapshot.
    fn outstanding_approvals(snapshot: &LiveTurnSnapshot) -> Vec<ApprovalId> {
        let answered: HashSet<ApprovalId> = snapshot
            .answered_approvals
            .iter()
            .map(|a| a.request_id.clone())
            .collect();
        let mut order: Vec<ApprovalId> = Vec::new();
        let mut seen: HashSet<ApprovalId> = HashSet::new();
        for event in &snapshot.accumulated {
            if let WireAgentEvent::ApprovalRequested { request, .. } = event
                && !answered.contains(&request.id)
                && seen.insert(request.id.clone())
            {
                order.push(request.id.clone());
            }
        }
        order
    }

    /// The server requests a reconnecting client would still treat as actionable, in arrival
    /// order. Mirrors `outstandingServerRequests` in the browser so a server-side test can assert
    /// what the client will derive from the snapshot.
    fn outstanding_server_requests(snapshot: &LiveTurnSnapshot) -> Vec<ServerRequestId> {
        let answered: HashSet<ServerRequestId> =
            snapshot.answered_server_requests.iter().cloned().collect();
        // An `IndexSet` keeps arrival order deterministically. `insert` on receive updates in place
        // when the id is already present, or appends it when it is new; `shift_remove` on resolve
        // drops it. A re-sent id after a resolution re-inserts at the end, so a reopen moves to the
        // back rather than keeping its first-seen position. Mirrors `outstandingServerRequests` in
        // the browser and `pending_server_requests` on the server.
        let mut pending: indexmap::IndexSet<ServerRequestId> = indexmap::IndexSet::new();
        for event in &snapshot.accumulated {
            match event {
                WireAgentEvent::ServerRequestReceived { request, .. } => {
                    pending.insert(request.id.clone());
                }
                WireAgentEvent::ServerRequestResolved { request_id, .. } => {
                    pending.shift_remove(request_id);
                }
                _ => {}
            }
        }
        pending
            .into_iter()
            .filter(|id| !answered.contains(id))
            .collect()
    }

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
        // The harness resolved one with its own event, the other is still open. The outstanding
        // server requests are derived from `accumulated` plus `answered_server_requests`, so only
        // the still-open one is reported as pending.
        assert_eq!(
            outstanding_server_requests(&snapshot),
            vec![pending.clone()]
        );
    }

    // A harness may resolve a server request and then re-raise the same id with a fresher payload
    // (a re-send). The outstanding derivation must reopen it, not keep it hidden behind the earlier
    // resolution — a reconnected user has to be able to answer it.
    #[tokio::test]
    async fn a_server_request_reopened_after_resolution_is_outstanding_again() {
        let store = LiveBufferStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let id = ServerRequestId("srv".into());

        store.start_turn(thread).await;
        store
            .append(thread, AgentEvent::TurnStarted { thread, turn })
            .await;
        store
            .append(thread, server_request_received(thread, turn, &id))
            .await;
        store
            .append(
                thread,
                AgentEvent::ServerRequestResolved {
                    thread,
                    turn: Some(turn),
                    request_id: id.clone(),
                },
            )
            .await;
        // The harness re-raises the same id after resolving it.
        store
            .append(thread, server_request_received(thread, turn, &id))
            .await;

        let snapshot = store.snapshot(thread).await.expect("snapshot");
        assert_eq!(
            outstanding_server_requests(&snapshot),
            vec![id],
            "a re-sent id after resolution must reopen the request"
        );
    }

    // A reopen moves the request to the end of the arrival order, not back to its first-seen
    // position. The sequence received(A), received(B), resolved(A), received(A) yields [B, A]:
    // the second receive(A) re-inserts A at the back, so the reopened request is the newest thing
    // demanding attention. Mirrors `pending_server_requests` on the server and
    // `outstandingServerRequests` in the browser.
    #[tokio::test]
    async fn a_reopened_server_request_moves_to_the_end_of_arrival_order() {
        let store = LiveBufferStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let a = ServerRequestId("a".into());
        let b = ServerRequestId("b".into());

        store.start_turn(thread).await;
        store
            .append(thread, AgentEvent::TurnStarted { thread, turn })
            .await;
        store
            .append(thread, server_request_received(thread, turn, &a))
            .await;
        store
            .append(thread, server_request_received(thread, turn, &b))
            .await;
        store
            .append(
                thread,
                AgentEvent::ServerRequestResolved {
                    thread,
                    turn: Some(turn),
                    request_id: a.clone(),
                },
            )
            .await;
        // A re-opens after being resolved.
        store
            .append(thread, server_request_received(thread, turn, &a))
            .await;

        let snapshot = store.snapshot(thread).await.expect("snapshot");
        assert_eq!(
            outstanding_server_requests(&snapshot),
            vec![b.clone(), a.clone()],
            "a reopen moves the request to the end, it does not restore its first-seen position"
        );
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
        let pending = snapshot
            .accumulated
            .iter()
            .find_map(|e| match e {
                WireAgentEvent::ApprovalRequested { request, .. } => Some(request.clone()),
                _ => None,
            })
            .expect("the approval request should be present in the accumulated events");
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

    fn approval_requested(thread: ThreadId, turn: TurnId, id: &str) -> AgentEvent {
        AgentEvent::ApprovalRequested {
            thread,
            turn,
            request: ApprovalRequest {
                id: ApprovalId(id.into()),
                kind: ApprovalKind::CommandExecution {
                    command: "rm -rf /tmp/x".into(),
                    cwd: "/tmp/project".into(),
                },
                reason: None,
                metadata: vec![],
                available: vec![ApprovalDecision::Accept, ApprovalDecision::Decline],
            },
        }
    }

    fn server_request_received(thread: ThreadId, turn: TurnId, id: &ServerRequestId) -> AgentEvent {
        AgentEvent::ServerRequestReceived {
            thread,
            turn: Some(turn),
            request: ServerRequest {
                id: id.clone(),
                method: "item/tool/call".into(),
                params: serde_json::json!({ "tool": "example" }),
                received_at: Utc::now(),
            },
        }
    }

    #[tokio::test]
    async fn answered_approval_is_not_resurfaced_as_pending() {
        let store = LiveBufferStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();

        store.start_turn(thread).await;
        store
            .append(thread, AgentEvent::TurnStarted { thread, turn })
            .await;
        store
            .append(thread, approval_requested(thread, turn, "ap_answered"))
            .await;
        store
            .resolve_approval(
                thread,
                ApprovalId("ap_answered".into()),
                ApprovalDecision::Accept,
            )
            .await;

        let snapshot = store.snapshot(thread).await.expect("snapshot");
        // The answered approval must not come back as actionable — re-answering a stale id errors.
        assert!(
            outstanding_approvals(&snapshot).is_empty(),
            "answered approval should not be pending after a reload"
        );
        // But it is reported as answered so the reconnecting client renders it in its resolved state.
        assert_eq!(snapshot.answered_approvals.len(), 1);
        assert_eq!(
            snapshot.answered_approvals[0].request_id,
            ApprovalId("ap_answered".into())
        );
        assert_eq!(
            snapshot.answered_approvals[0].decision,
            ApprovalDecision::Accept
        );
    }

    #[tokio::test]
    async fn unanswered_approval_stays_pending_when_another_is_answered() {
        let store = LiveBufferStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();

        store.start_turn(thread).await;
        store
            .append(thread, AgentEvent::TurnStarted { thread, turn })
            .await;
        store
            .append(thread, approval_requested(thread, turn, "ap_answered"))
            .await;
        store
            .append(thread, approval_requested(thread, turn, "ap_open"))
            .await;
        store
            .resolve_approval(
                thread,
                ApprovalId("ap_answered".into()),
                ApprovalDecision::Decline,
            )
            .await;

        let snapshot = store.snapshot(thread).await.expect("snapshot");
        // Only the still-open approval is actionable; the answered one is not, even though both
        // ride along in `accumulated`.
        assert_eq!(
            outstanding_approvals(&snapshot),
            vec![ApprovalId("ap_open".into())]
        );
        assert_eq!(snapshot.answered_approvals.len(), 1);
        assert_eq!(
            snapshot.answered_approvals[0].request_id,
            ApprovalId("ap_answered".into())
        );
    }

    // A turn really can be blocked on several approvals at once — three commands proposed together,
    // for instance. The snapshot used to carry a single `pending_approval`, so it named only the
    // most recently raised one and quietly dropped the rest. The outstanding set is now derived
    // from `accumulated`, so every unanswered approval is reported.
    #[tokio::test]
    async fn every_unanswered_approval_is_reported_as_pending() {
        let store = LiveBufferStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();

        store.start_turn(thread).await;
        store
            .append(thread, AgentEvent::TurnStarted { thread, turn })
            .await;
        for id in ["ap_first", "ap_second", "ap_third"] {
            store
                .append(thread, approval_requested(thread, turn, id))
                .await;
        }

        let snapshot = store.snapshot(thread).await.expect("snapshot");
        assert_eq!(
            outstanding_approvals(&snapshot),
            vec![
                ApprovalId("ap_first".into()),
                ApprovalId("ap_second".into()),
                ApprovalId("ap_third".into()),
            ]
        );

        // The SB5 connect bootstrap reports all of them too, not just one.
        let [attention] = store
            .pending_attention()
            .await
            .into_iter()
            .filter(|entry| entry.thread_id == thread)
            .collect::<Vec<_>>()
            .try_into()
            .expect("one pending-attention entry for the thread");
        assert_eq!(
            attention
                .approvals
                .iter()
                .map(|approval| approval.id.clone())
                .collect::<Vec<_>>(),
            vec![
                ApprovalId("ap_first".into()),
                ApprovalId("ap_second".into()),
                ApprovalId("ap_third".into()),
            ]
        );
    }

    // A turn blocked on several server requests must report all of them in first-seen order, both
    // in the reconnect snapshot and the SB5 connect bootstrap. The bootstrap feeds the sidebar's
    // "what is this thread waiting on" and notification-click focus, so its order must be
    // deterministic, not decided by hash iteration.
    #[tokio::test]
    async fn every_unanswered_server_request_is_reported_in_arrival_order() {
        let store = LiveBufferStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let ids: Vec<ServerRequestId> = ["sr_first", "sr_second", "sr_third"]
            .into_iter()
            .map(|s| ServerRequestId(s.into()))
            .collect();

        store.start_turn(thread).await;
        store
            .append(thread, AgentEvent::TurnStarted { thread, turn })
            .await;
        for id in &ids {
            store
                .append(thread, server_request_received(thread, turn, id))
                .await;
        }

        let snapshot = store.snapshot(thread).await.expect("snapshot");
        assert_eq!(
            outstanding_server_requests(&snapshot),
            ids,
            "the snapshot derivation is first-seen ordered"
        );

        // The SB5 connect bootstrap (the production `pending_attention`) is first-seen ordered too.
        let [attention] = store
            .pending_attention()
            .await
            .into_iter()
            .filter(|entry| entry.thread_id == thread)
            .collect::<Vec<_>>()
            .try_into()
            .expect("one pending-attention entry for the thread");
        assert_eq!(
            attention
                .server_requests
                .iter()
                .map(|request| request.id.clone())
                .collect::<Vec<_>>(),
            ids,
            "the connect bootstrap must preserve first-seen order, not hash iteration order"
        );
    }

    #[tokio::test]
    async fn resolve_approval_is_a_no_op_without_a_live_turn() {
        let store = LiveBufferStore::new();
        let thread = ThreadId::new();
        // No panic and no snapshot materializes when the turn buffer was already cleared.
        store
            .resolve_approval(thread, ApprovalId("ap_1".into()), ApprovalDecision::Accept)
            .await;
        assert!(store.snapshot(thread).await.is_none());
    }
}
