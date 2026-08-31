#[cfg(test)]
use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};

use giskard_core::approval::ApprovalDecision;
#[cfg(test)]
use giskard_core::approval::ApprovalRequest;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{CommandOutputDescriptor, ItemDelta, ItemPayload};
#[cfg(test)]
use giskard_core::server_request::ServerRequest;
use giskard_core::user_input::UserInput;
#[cfg(test)]
use giskard_proto::WireApprovalRequest;
use giskard_proto::{AnsweredApproval, LiveTurnSnapshot, WireAgentEvent, WireToolOutput};

const MAX_LIVE_COMMAND_OUTPUT: usize = 16 * 1024;
const LIVE_COMMAND_OUTPUT_EDGE: usize = 8 * 1024;
const LIVE_COMMAND_OUTPUT_TRUNCATED: &str = "\n\n[... command output truncated in the live reconnect snapshot; full output is preserved on command completion ...]\n\n";

struct LiveTurn {
    turn_id: TurnId,
    user_input: Option<UserInput>,
    events: Vec<AgentEvent>,
    command_output_descriptors: HashMap<ItemId, CommandOutputDescriptor>,
    tool_output_descriptors: HashMap<ItemId, WireToolOutput>,
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

#[derive(Default)]
pub struct LiveTurnState {
    thread_id: Option<ThreadId>,
    turn: Option<LiveTurn>,
}

impl LiveTurnState {
    #[cfg(test)]
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub fn start_turn(&mut self, thread_id: ThreadId) {
        self.start_turn_with_user_input(thread_id, None);
    }

    #[cfg(test)]
    pub fn start_turn_with_user_input(
        &mut self,
        thread_id: ThreadId,
        user_input: Option<UserInput>,
    ) {
        self.replace_turn_with_user_input(thread_id, TurnId::new(), user_input);
    }

    pub fn replace_turn_with_user_input(
        &mut self,
        thread_id: ThreadId,
        turn_id: TurnId,
        user_input: Option<UserInput>,
    ) {
        self.thread_id = Some(thread_id);
        self.turn = Some(LiveTurn {
            turn_id,
            user_input,
            events: Vec::new(),
            command_output_descriptors: HashMap::new(),
            tool_output_descriptors: HashMap::new(),
            resolved_approvals: HashMap::new(),
            resolved_server_requests: HashSet::new(),
        });
    }

    /// Ensure an exact turn has a reconnect buffer without replacing events already observed for
    /// it. Harnesses may publish a turn-scoped item before their delayed `TurnStarted` event; that
    /// item is still live state and must survive a browser reload.
    pub fn ensure_turn_with_user_input(
        &mut self,
        thread_id: ThreadId,
        turn_id: TurnId,
        user_input: Option<UserInput>,
    ) -> Result<(), TurnId> {
        match self.turn.as_mut() {
            None => {
                self.thread_id = Some(thread_id);
                self.turn = Some(LiveTurn {
                    turn_id,
                    user_input,
                    events: Vec::new(),
                    command_output_descriptors: HashMap::new(),
                    tool_output_descriptors: HashMap::new(),
                    resolved_approvals: HashMap::new(),
                    resolved_server_requests: HashSet::new(),
                });
                Ok(())
            }
            Some(turn) if turn.turn_id == turn_id => {
                if turn.user_input.is_none() && user_input.is_some() {
                    turn.user_input = user_input;
                }
                Ok(())
            }
            Some(turn) => Err(turn.turn_id),
        }
    }

    pub fn append(&mut self, thread_id: ThreadId, event: AgentEvent) {
        self.append_with_outputs(thread_id, event, None, None);
    }

    #[cfg(test)]
    pub fn append_with_command_output(
        &mut self,
        thread_id: ThreadId,
        event: AgentEvent,
        command_output: Option<CommandOutputDescriptor>,
    ) {
        self.append_with_outputs(thread_id, event, command_output, None);
    }

    pub fn append_with_outputs(
        &mut self,
        thread_id: ThreadId,
        mut event: AgentEvent,
        command_output: Option<CommandOutputDescriptor>,
        tool_output: Option<WireToolOutput>,
    ) {
        if self.thread_id == Some(thread_id)
            && let Some(turn) = self.turn.as_mut()
        {
            if let AgentEvent::TurnStarted { turn: tid, .. } = &event {
                turn.turn_id = *tid;
            }
            if let Some(item_id) = completed_command_item_id(&event) {
                remove_command_output_deltas(&mut turn.events, item_id);
                remove_completed_command_events(&mut turn.events, item_id);
                turn.command_output_descriptors.remove(&item_id);
                if let Some(descriptor) = command_output {
                    turn.command_output_descriptors
                        .insert(item_id, descriptor.clone());
                    if let AgentEvent::ItemCompleted { item, .. } = &mut event
                        && let ItemPayload::CommandExecution { output, .. } = &mut item.payload
                    {
                        *output = descriptor.preview;
                    }
                } else if let AgentEvent::ItemCompleted { item, .. } = &mut event
                    && let ItemPayload::CommandExecution {
                        output,
                        output_truncated,
                        output_original_bytes,
                        output_original_lines,
                        ..
                    } = &mut item.payload
                {
                    // Invalid truncation metadata makes the completed output unusable. Do not keep
                    // its potentially large payload in reconnect state or let a descriptor from an
                    // earlier completion of the same item leak into this replacement.
                    output.clear();
                    *output_truncated = false;
                    *output_original_bytes = None;
                    *output_original_lines = None;
                    turn.command_output_descriptors.insert(
                        item_id,
                        CommandOutputDescriptor::from_durable("", false, 0, 0, false),
                    );
                }
            }
            if let Some(item_id) = completed_tool_item_id(&event) {
                remove_completed_tool_events(&mut turn.events, item_id);
                turn.tool_output_descriptors.remove(&item_id);
                if let Some(descriptor) = tool_output {
                    turn.tool_output_descriptors.insert(item_id, descriptor);
                }
                if let AgentEvent::ItemCompleted { item, .. } = &mut event
                    && let ItemPayload::ToolCall { output, .. } = &mut item.payload
                {
                    *output = None;
                }
            }
            let command_delta_item = command_output_item_id(&event);
            turn.events.push(event);
            if let Some(item_id) = command_delta_item {
                compact_command_output_deltas(&mut turn.events, item_id);
            }
        }
    }

    /// Record that the user answered an approval in the thread's in-flight turn, so the reconnect
    /// snapshot renders it resolved instead of re-prompting (spec §13.6). No-op if the thread has no
    /// live turn (e.g. the turn already completed and its buffer was cleared).
    pub fn resolve_approval(
        &mut self,
        thread_id: ThreadId,
        approval_id: ApprovalId,
        decision: ApprovalDecision,
    ) {
        if self.thread_id == Some(thread_id)
            && let Some(turn) = self.turn.as_mut()
        {
            turn.resolved_approvals.insert(approval_id, decision);
        }
    }

    /// Record that the user answered a server request in the thread's in-flight turn. Mirrors
    /// [`Self::resolve_approval`]: the harness's own resolved event may be late or may never come,
    /// and until then a reconnect would replay the request as actionable.
    pub fn resolve_server_request(&mut self, thread_id: ThreadId, request_id: ServerRequestId) {
        if self.thread_id == Some(thread_id)
            && let Some(turn) = self.turn.as_mut()
        {
            turn.resolved_server_requests.insert(request_id);
        }
    }

    pub fn clear_turn(&mut self, thread_id: ThreadId) {
        if self.thread_id == Some(thread_id) {
            self.thread_id = None;
            self.turn = None;
        }
    }

    pub fn is_active(&self, thread_id: ThreadId) -> bool {
        self.thread_id == Some(thread_id) && self.turn.is_some()
    }

    /// Return raw server-side lifecycle events for one Giskard item. This is intentionally not a
    /// wire snapshot: linked-thread opening needs the native routing id that wire conversion
    /// redacts before data reaches the browser.
    pub fn item_events(&self, thread_id: ThreadId, item_id: ItemId) -> Vec<AgentEvent> {
        (self.thread_id == Some(thread_id))
            .then_some(self.turn.as_ref())
            .flatten()
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

    pub fn snapshot(&self, thread_id: ThreadId) -> Option<LiveTurnSnapshot> {
        (self.thread_id == Some(thread_id))
            .then_some(self.turn.as_ref())
            .flatten()
            .map(|turn| {
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
                    .filter_map(|event| match event {
                        AgentEvent::ItemCompleted {
                            thread,
                            turn: turn_id,
                            item,
                        } => {
                            let command_descriptor =
                                turn.command_output_descriptors.get(&item.id).cloned();
                            let tool_descriptor =
                                turn.tool_output_descriptors.get(&item.id).cloned();
                            Some(WireAgentEvent::ItemCompleted {
                                thread,
                                turn: turn_id,
                                item: giskard_proto::WireItem::from_item_with_outputs(
                                    item,
                                    command_descriptor,
                                    tool_descriptor,
                                ),
                            })
                        }
                        event => WireAgentEvent::from_agent_event(event),
                    })
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
    /// Legacy test projection retained to verify that every unanswered request is reconstructed.
    /// Production cross-thread state comes from the root runtime overview projection.
    #[cfg(test)]
    pub fn pending_attention(&self) -> Vec<PendingAttention> {
        self.thread_id
            .zip(self.turn.as_ref())
            .into_iter()
            .filter_map(|(thread_id, turn)| {
                let approvals = pending_approvals(&turn.events, &turn.resolved_approvals);
                let server_requests =
                    pending_server_requests(&turn.events, &turn.resolved_server_requests);
                if approvals.is_empty() && server_requests.is_empty() {
                    return None;
                }
                Some(PendingAttention {
                    thread_id,
                    approvals,
                    server_requests,
                })
            })
            .collect()
    }
}

fn completed_tool_item_id(event: &AgentEvent) -> Option<ItemId> {
    let AgentEvent::ItemCompleted { item, .. } = event else {
        return None;
    };
    matches!(item.payload, ItemPayload::ToolCall { .. }).then_some(item.id)
}

fn remove_completed_tool_events(events: &mut Vec<AgentEvent>, item_id: ItemId) {
    events.retain(|event| completed_tool_item_id(event) != Some(item_id));
}

/// One thread's outstanding user-facing requests, as needed to rebuild a connecting client's
/// cross-thread activity view.
#[derive(Debug)]
#[cfg(test)]
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
#[cfg(test)]
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

#[cfg(test)]
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

fn remove_command_output_deltas(events: &mut Vec<AgentEvent>, item_id: ItemId) {
    events.retain(|event| command_output_item_id(event) != Some(item_id));
}

fn remove_completed_command_events(events: &mut Vec<AgentEvent>, item_id: ItemId) {
    events.retain(|event| completed_command_item_id(event) != Some(item_id));
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
        let mut store = LiveTurnState::new();
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
            .expect("first event starts the exact turn buffer");
        store.append(thread, item);
        store
            .ensure_turn_with_user_input(thread, turn, Some(input.clone()))
            .expect("late turn start reuses the buffer");
        store.append(thread, AgentEvent::TurnStarted { thread, turn });

        let snapshot = store.snapshot(thread).expect("snapshot");
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
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item = ItemId::new();
        store.start_turn(thread);
        store.append(thread, AgentEvent::TurnStarted { thread, turn });
        store.append(
            thread,
            AgentEvent::ItemStarted {
                thread,
                turn,
                item: command_start(item),
            },
        );

        store.append(
            thread,
            AgentEvent::ItemDelta {
                thread,
                turn,
                item_id: item,
                delta: ItemDelta::CommandOutput {
                    chunk: format!("head\n{}", "a".repeat(MAX_LIVE_COMMAND_OUTPUT)),
                },
            },
        );
        store.append(
            thread,
            AgentEvent::ItemDelta {
                thread,
                turn,
                item_id: item,
                delta: ItemDelta::CommandOutput {
                    chunk: format!("{}\ntail", "b".repeat(MAX_LIVE_COMMAND_OUTPUT)),
                },
            },
        );

        let snapshot = store.snapshot(thread).expect("snapshot");
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
    async fn completed_command_output_uses_bounded_wire_descriptor_in_live_snapshot() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();
        let output = format!(
            "head\n{}{}\ntail",
            "a".repeat(MAX_LIVE_COMMAND_OUTPUT),
            "b".repeat(MAX_LIVE_COMMAND_OUTPUT)
        );

        store.start_turn(thread);
        let descriptor = CommandOutputDescriptor::from_durable(
            &output,
            false,
            output.len() as u64,
            giskard_core::command_output_logical_lines(&output),
            true,
        );
        let original_bytes = output.len() as u64;
        store.append_with_command_output(
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
                        output_truncated: false,
                        output_original_bytes: None,
                        output_original_lines: None,
                        exit_code: Some(0),
                        status: Some("completed".into()),
                        process_id: Some("proc_1".into()),
                        duration_ms: Some(500),
                    },
                    created_at: Utc::now(),
                },
            },
            Some(descriptor),
        );

        let retained_output = store
            .turn
            .as_ref()
            .and_then(|turn| turn.events.last())
            .and_then(|event| match event {
                AgentEvent::ItemCompleted { item, .. } => match &item.payload {
                    ItemPayload::CommandExecution { output, .. } => Some(output),
                    _ => None,
                },
                _ => None,
            })
            .expect("bounded completed output");
        assert!(retained_output.len() <= CommandOutputDescriptor::PREVIEW_MAX_BYTES);

        let snapshot = store.snapshot(thread).expect("snapshot");
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
        assert!(output.preview_truncated);
        assert!(!output.preview.contains("head\n"));
        assert!(output.preview.len() <= giskard_persist::COMMAND_OUTPUT_PREVIEW_MAX_BYTES);
        assert!(output.preview.ends_with("\ntail"));
        assert_eq!(output.original_bytes, original_bytes);
        assert_eq!(output.durable_bytes, original_bytes);
    }

    #[tokio::test]
    async fn rejected_command_output_replaces_stale_descriptor_with_unavailable_snapshot() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();
        store.start_turn(thread);

        let completed =
            |output: String, output_truncated, output_original_lines| AgentEvent::ItemCompleted {
                thread,
                turn,
                item: Item {
                    id: item_id,
                    harness_item_id: "cmd_1".into(),
                    payload: ItemPayload::CommandExecution {
                        command: "yes".into(),
                        cwd: "/tmp/project".into(),
                        output,
                        output_truncated,
                        output_original_bytes: output_truncated.then_some(100_000),
                        output_original_lines,
                        exit_code: Some(0),
                        status: Some("completed".into()),
                        process_id: Some("proc_1".into()),
                        duration_ms: Some(500),
                    },
                    created_at: Utc::now(),
                },
            };

        let first_output = "first output".to_owned();
        let first_descriptor = CommandOutputDescriptor::from_durable(
            &first_output,
            false,
            first_output.len() as u64,
            1,
            true,
        );
        store.append_with_command_output(
            thread,
            completed(first_output, false, None),
            Some(first_descriptor),
        );
        store.append_with_command_output(
            thread,
            completed("x".repeat(MAX_LIVE_COMMAND_OUTPUT * 4), true, None),
            None,
        );

        let turn_state = store.turn.as_ref().expect("live turn");
        let retained = turn_state
            .events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::ItemCompleted { item, .. } if item.id == item_id => {
                    let ItemPayload::CommandExecution { output, .. } = &item.payload else {
                        return None;
                    };
                    Some(output)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(retained, vec![""]);

        let snapshot = store.snapshot(thread).expect("snapshot");
        let outputs = snapshot
            .accumulated
            .iter()
            .filter_map(|event| {
                let WireAgentEvent::ItemCompleted { item, .. } = event else {
                    return None;
                };
                let WireItemPayload::CommandExecution { output, .. } = &item.payload else {
                    return None;
                };
                Some(output)
            })
            .collect::<Vec<_>>();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].preview, "");
        assert_eq!(outputs[0].durable_bytes, 0);
        assert_eq!(outputs[0].original_bytes, 0);
        assert!(!outputs[0].output_available);
    }

    #[tokio::test]
    async fn pending_server_requests_are_reconstructed_for_live_snapshot() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let pending = ServerRequestId("pending".into());
        let resolved = ServerRequestId("resolved".into());

        store.start_turn(thread);
        store.append(thread, AgentEvent::TurnStarted { thread, turn });
        for id in [pending.clone(), resolved.clone()] {
            store.append(
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
            );
        }
        store.append(
            thread,
            AgentEvent::ServerRequestResolved {
                thread,
                turn: Some(turn),
                request_id: resolved,
            },
        );

        let snapshot = store.snapshot(thread).expect("snapshot");
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
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let id = ServerRequestId("srv".into());

        store.start_turn(thread);
        store.append(thread, AgentEvent::TurnStarted { thread, turn });
        store.append(thread, server_request_received(thread, turn, &id));
        store.append(
            thread,
            AgentEvent::ServerRequestResolved {
                thread,
                turn: Some(turn),
                request_id: id.clone(),
            },
        );
        // The harness re-raises the same id after resolving it.
        store.append(thread, server_request_received(thread, turn, &id));

        let snapshot = store.snapshot(thread).expect("snapshot");
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
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let a = ServerRequestId("a".into());
        let b = ServerRequestId("b".into());

        store.start_turn(thread);
        store.append(thread, AgentEvent::TurnStarted { thread, turn });
        store.append(thread, server_request_received(thread, turn, &a));
        store.append(thread, server_request_received(thread, turn, &b));
        store.append(
            thread,
            AgentEvent::ServerRequestResolved {
                thread,
                turn: Some(turn),
                request_id: a.clone(),
            },
        );
        // A re-opens after being resolved.
        store.append(thread, server_request_received(thread, turn, &a));

        let snapshot = store.snapshot(thread).expect("snapshot");
        assert_eq!(
            outstanding_server_requests(&snapshot),
            vec![b.clone(), a.clone()],
            "a reopen moves the request to the end, it does not restore its first-seen position"
        );
    }

    #[tokio::test]
    async fn pending_approval_metadata_is_reconstructed_for_live_snapshot() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();

        store.start_turn(thread);
        store.append(
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
        );

        let snapshot = store.snapshot(thread).expect("snapshot");
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
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();

        store.start_turn(thread);
        store.append(thread, AgentEvent::TurnStarted { thread, turn });
        store.append(thread, approval_requested(thread, turn, "ap_answered"));
        store.resolve_approval(
            thread,
            ApprovalId("ap_answered".into()),
            ApprovalDecision::Accept,
        );

        let snapshot = store.snapshot(thread).expect("snapshot");
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
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();

        store.start_turn(thread);
        store.append(thread, AgentEvent::TurnStarted { thread, turn });
        store.append(thread, approval_requested(thread, turn, "ap_answered"));
        store.append(thread, approval_requested(thread, turn, "ap_open"));
        store.resolve_approval(
            thread,
            ApprovalId("ap_answered".into()),
            ApprovalDecision::Decline,
        );

        let snapshot = store.snapshot(thread).expect("snapshot");
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
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();

        store.start_turn(thread);
        store.append(thread, AgentEvent::TurnStarted { thread, turn });
        for id in ["ap_first", "ap_second", "ap_third"] {
            store.append(thread, approval_requested(thread, turn, id));
        }

        let snapshot = store.snapshot(thread).expect("snapshot");
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
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let ids: Vec<ServerRequestId> = ["sr_first", "sr_second", "sr_third"]
            .into_iter()
            .map(|s| ServerRequestId(s.into()))
            .collect();

        store.start_turn(thread);
        store.append(thread, AgentEvent::TurnStarted { thread, turn });
        for id in &ids {
            store.append(thread, server_request_received(thread, turn, id));
        }

        let snapshot = store.snapshot(thread).expect("snapshot");
        assert_eq!(
            outstanding_server_requests(&snapshot),
            ids,
            "the snapshot derivation is first-seen ordered"
        );

        // The SB5 connect bootstrap (the production `pending_attention`) is first-seen ordered too.
        let [attention] = store
            .pending_attention()
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
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        // No panic and no snapshot materializes when the turn buffer was already cleared.
        store.resolve_approval(thread, ApprovalId("ap_1".into()), ApprovalDecision::Accept);
        assert!(store.snapshot(thread).is_none());
    }
}
