use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};

use giskard_core::approval::ApprovalDecision;
#[cfg(test)]
use giskard_core::approval::ApprovalRequest;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{
    CommandOutputDescriptor, Item, ItemDelta, ItemKind, ItemPayload, ItemStart,
};
#[cfg(test)]
use giskard_core::server_request::ServerRequest;
use giskard_core::user_input::UserInput;
#[cfg(test)]
use giskard_proto::WireApprovalRequest;
use giskard_proto::{AnsweredApproval, LiveTurnSnapshot, WireAgentEvent, WireToolOutput};

const MAX_LIVE_COMMAND_OUTPUT: usize = 16 * 1024;
const LIVE_COMMAND_OUTPUT_EDGE: usize = 8 * 1024;
const LIVE_COMMAND_OUTPUT_TRUNCATED: &str = "\n\n[... command output truncated in the live reconnect snapshot; full output is preserved on command completion ...]\n\n";

/// The reconnect buffer of one in-flight turn, keyed by item (spec §13.6, LB1).
///
/// Each item is one entry holding its start, its latest completion and the deltas received since
/// that completion, so a completion replaces in place and a delta appends to one string: the cost
/// of an event does not grow with the turn, and the snapshot carries one or two events per item
/// instead of every chunk ever streamed.
struct LiveTurn {
    turn_id: TurnId,
    user_input: Option<UserInput>,
    /// Expanded first, wherever it arrived. A harness may publish an item before its delayed
    /// `TurnStarted`, and the browser's `turn_started` handler forgets every item row it knows, so
    /// replaying it after an item would orphan that item's row.
    turn_started: Option<AgentEvent>,
    /// Arrival order. An item sits where the browser draws its row: at its start for a start that
    /// draws one (see [`start_draws_row`]), otherwise at its first delta or completion. Every other
    /// event sits where it arrived, so a request card replays where the live transcript drew it.
    /// Entries are only ever pushed, never removed or moved.
    entries: Vec<LiveEntry>,
    /// The index in `entries` of each item's entry, written only by [`LiveTurn::entry_for`].
    slots: HashMap<ItemId, usize>,
    /// Starts that draw no row and whose item has no entry yet, with the thread and turn of their
    /// event, in arrival order. Giving such a start a slot would replay the item's row above a
    /// request card that arrived between the start and the item's first delta, where the live
    /// transcript drew it below. The first delta or completion moves the start into the new entry;
    /// a start still waiting here replays after the entries, where it only registers a kind.
    pending_starts: IndexMap<ItemId, (ThreadId, TurnId, ItemStart)>,
    /// Only the latest usage is kept, and it is expanded last: the browser's handler only moves the
    /// gauge, so its position is invisible.
    latest_usage: Option<AgentEvent>,
    command_output_descriptors: HashMap<ItemId, CommandOutputDescriptor>,
    tool_output_descriptors: HashMap<ItemId, WireToolOutput>,
    /// Approvals the user answered during this turn, and the decision they made. Resolution is not
    /// otherwise recorded in `entries` (there is no harness-emitted approval-resolved event), so
    /// without this the reconnect snapshot would replay every answered approval as still pending.
    resolved_approvals: HashMap<ApprovalId, ApprovalDecision>,
    /// Server requests the user answered during this turn.
    ///
    /// Unlike approvals these *do* have a harness-emitted resolved event, but it arrives on the
    /// harness's own schedule — and a harness may never send one. Until it lands the request is
    /// still "received but not resolved" as far as `entries` is concerned, so a reload in that
    /// window would replay it as actionable and re-answering routes a stale id to the harness. The
    /// answer is recorded here the moment it is routed, which closes that window.
    resolved_server_requests: HashSet<ServerRequestId>,
}

#[expect(
    clippy::large_enum_variant,
    reason = "nearly every entry is an item, so boxing items would add an allocation per item and save nothing"
)]
enum LiveEntry {
    Item(LiveItemEntry),
    /// Anything that is not an item lifecycle event, `TurnStarted` or `TurnUsageUpdated`:
    /// approval and server-request events, `Error`, `Notice`, `DiffUpdated`, and the
    /// `TurnCompleted` a persistence-blocked turn keeps.
    Event(AgentEvent),
}

struct LiveItemEntry {
    id: ItemId,
    /// The thread and turn of the event that first named this item, stamped on every event the
    /// entry expands to.
    thread: ThreadId,
    turn: TurnId,
    start: Option<ItemStart>,
    /// The latest completion, with command output already reduced to its preview and tool output
    /// stripped.
    completed: Option<Item>,
    /// Text deltas received since the latest completion (or since the start, if none), joined the
    /// way the browser joins them (see [`LiveItemEntry::push_text`]).
    text: String,
    /// `CommandOutput` deltas received since the latest completion, head/tail-compacted past
    /// `MAX_LIVE_COMMAND_OUTPUT`.
    output: String,
}

impl LiveTurn {
    fn new(turn_id: TurnId, user_input: Option<UserInput>) -> Self {
        Self {
            turn_id,
            user_input,
            turn_started: None,
            entries: Vec::new(),
            slots: HashMap::new(),
            pending_starts: IndexMap::new(),
            latest_usage: None,
            command_output_descriptors: HashMap::new(),
            tool_output_descriptors: HashMap::new(),
            resolved_approvals: HashMap::new(),
            resolved_server_requests: HashSet::new(),
        }
    }

    /// The item's entry, pushed at the tail (taking over a pending start) if it has none. A slot
    /// that does not name an item entry is replaced by a fresh one rather than trusted, so the
    /// `None` arm is never taken.
    fn entry_for(
        &mut self,
        thread: ThreadId,
        turn: TurnId,
        item_id: ItemId,
    ) -> Option<&mut LiveItemEntry> {
        let slot = match self.slots.get(&item_id) {
            Some(&slot) if matches!(self.entries.get(slot), Some(LiveEntry::Item(_))) => slot,
            _ => {
                let (thread, turn, start) = match self.pending_starts.shift_remove(&item_id) {
                    Some((thread, turn, start)) => (thread, turn, Some(start)),
                    None => (thread, turn, None),
                };
                self.entries.push(LiveEntry::Item(LiveItemEntry {
                    id: item_id,
                    thread,
                    turn,
                    start,
                    completed: None,
                    text: String::new(),
                    output: String::new(),
                }));
                self.slots.insert(item_id, self.entries.len() - 1);
                self.entries.len() - 1
            }
        };
        match self.entries.get_mut(slot) {
            Some(LiveEntry::Item(entry)) => Some(entry),
            _ => None,
        }
    }

    fn item(&self, item_id: ItemId) -> Option<&LiveItemEntry> {
        match self.entries.get(*self.slots.get(&item_id)?)? {
            LiveEntry::Item(entry) => Some(entry),
            LiveEntry::Event(_) => None,
        }
    }

    fn events(&self) -> impl Iterator<Item = &AgentEvent> {
        self.entries.iter().filter_map(|entry| match entry {
            LiveEntry::Event(event) => Some(event),
            LiveEntry::Item(_) => None,
        })
    }

    fn start_item(&mut self, thread: ThreadId, turn: TurnId, start: ItemStart) {
        if !self.slots.contains_key(&start.id) && !start_draws_row(&start) {
            self.pending_starts.insert(start.id, (thread, turn, start));
            return;
        }
        let Some(entry) = self.entry_for(thread, turn, start.id) else {
            return;
        };
        if entry.completed.is_some() {
            // Adapters emit one lifecycle per item (spec §4.4), so this is a protocol violation.
            // The completion stays authoritative for `live_item` and the replay.
            tracing::debug!(
                thread_id = %thread,
                turn_id = %turn,
                item_id = %start.id,
                "live buffer received ItemStarted for an already completed item; keeping the completion"
            );
        }
        entry.start = Some(start);
    }

    fn complete_item(
        &mut self,
        thread: ThreadId,
        turn: TurnId,
        mut item: Item,
        command_output: Option<CommandOutputDescriptor>,
        tool_output: Option<WireToolOutput>,
    ) {
        match &mut item.payload {
            ItemPayload::CommandExecution {
                output,
                output_truncated,
                output_original_bytes,
                output_original_lines,
                ..
            } => {
                if let Some(descriptor) = command_output {
                    *output = descriptor.preview.clone();
                    self.command_output_descriptors.insert(item.id, descriptor);
                } else {
                    // Invalid truncation metadata makes the completed output unusable. Do not keep
                    // its potentially large payload in reconnect state or let a descriptor from an
                    // earlier completion of the same item leak into this replacement.
                    output.clear();
                    *output_truncated = false;
                    *output_original_bytes = None;
                    *output_original_lines = None;
                    self.command_output_descriptors.insert(
                        item.id,
                        CommandOutputDescriptor::from_durable("", false, 0, 0, false),
                    );
                }
            }
            ItemPayload::ToolCall { output, .. } => {
                *output = None;
                match tool_output {
                    Some(descriptor) => self.tool_output_descriptors.insert(item.id, descriptor),
                    None => self.tool_output_descriptors.remove(&item.id),
                };
            }
            _ => {}
        }
        // The completion re-renders the row over everything streamed before it, so those deltas
        // replay nothing the item does not already carry.
        let Some(entry) = self.entry_for(thread, turn, item.id) else {
            return;
        };
        entry.completed = Some(item);
        entry.text.clear();
        entry.output.clear();
    }
}

/// Whether the browser draws the item's row at `item_started` (`startRunningCommand` /
/// `startToolCall`) rather than at its first delta or completion.
fn start_draws_row(start: &ItemStart) -> bool {
    match start.kind {
        ItemKind::CommandExecution => start.command.is_some(),
        ItemKind::ToolCall => start.tool.is_some(),
        _ => false,
    }
}

fn started_event(thread: ThreadId, turn: TurnId, item: ItemStart) -> AgentEvent {
    AgentEvent::ItemStarted { thread, turn, item }
}

impl LiveItemEntry {
    /// Join a text delta the way the browser does: a tool call's progress chunks are separated by
    /// a newline (`appendToolProgress`), everything else concatenates (`appendStream`). The browser
    /// routes a delta by the kind `item_started` announced, so only `start` decides.
    fn push_text(&mut self, chunk: &str) {
        let is_tool = self
            .start
            .as_ref()
            .is_some_and(|start| start.kind == ItemKind::ToolCall);
        if is_tool && !self.text.is_empty() {
            self.text.push('\n');
        }
        self.text.push_str(chunk);
    }

    fn push_output(&mut self, chunk: &str) {
        self.output.push_str(chunk);
        if self.output.len() > MAX_LIVE_COMMAND_OUTPUT {
            self.output = compact_command_output(&self.output);
        }
    }

    fn started_event(&self) -> Option<AgentEvent> {
        self.start
            .clone()
            .map(|item| started_event(self.thread, self.turn, item))
    }

    fn completed_event(&self) -> Option<AgentEvent> {
        self.completed
            .clone()
            .map(|item| AgentEvent::ItemCompleted {
                thread: self.thread,
                turn: self.turn,
                item,
            })
    }

    /// Expand into wire events: start, completion, remaining text, remaining output (LB2).
    fn expand(&self, turn: &LiveTurn, out: &mut Vec<WireAgentEvent>) {
        out.extend(
            self.started_event()
                .and_then(WireAgentEvent::from_agent_event),
        );
        if let Some(item) = &self.completed {
            out.push(WireAgentEvent::ItemCompleted {
                thread: self.thread,
                turn: self.turn,
                item: giskard_proto::WireItem::from_item_with_outputs(
                    item.clone(),
                    turn.command_output_descriptors.get(&self.id).cloned(),
                    turn.tool_output_descriptors.get(&self.id).cloned(),
                ),
            });
        }
        let text = (!self.text.is_empty()).then(|| ItemDelta::Text {
            text: self.text.clone(),
        });
        let output = (!self.output.is_empty()).then(|| ItemDelta::CommandOutput {
            chunk: self.output.clone(),
        });
        for delta in text.into_iter().chain(output) {
            out.extend(WireAgentEvent::from_agent_event(AgentEvent::ItemDelta {
                thread: self.thread,
                turn: self.turn,
                item_id: self.id,
                delta,
            }));
        }
    }
}

pub(crate) enum LiveItem {
    Started(ItemStart),
    Completed {
        item: Item,
        command_output: Option<CommandOutputDescriptor>,
        tool_output: Option<WireToolOutput>,
    },
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
        self.turn = Some(LiveTurn::new(turn_id, user_input));
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
                self.turn = Some(LiveTurn::new(turn_id, user_input));
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
        event: AgentEvent,
        command_output: Option<CommandOutputDescriptor>,
        tool_output: Option<WireToolOutput>,
    ) {
        if self.thread_id != Some(thread_id) {
            return;
        }
        let Some(turn) = self.turn.as_mut() else {
            return;
        };
        match event {
            AgentEvent::TurnStarted { thread, turn: tid } => {
                turn.turn_id = tid;
                turn.turn_started = Some(AgentEvent::TurnStarted { thread, turn: tid });
            }
            event @ AgentEvent::TurnUsageUpdated { .. } => turn.latest_usage = Some(event),
            AgentEvent::ItemStarted {
                thread,
                turn: tid,
                item,
            } => turn.start_item(thread, tid, item),
            AgentEvent::ItemDelta {
                thread,
                turn: tid,
                item_id,
                delta,
            } => match (turn.entry_for(thread, tid, item_id), delta) {
                (Some(entry), ItemDelta::Text { text }) => entry.push_text(&text),
                (Some(entry), ItemDelta::CommandOutput { chunk }) => entry.push_output(&chunk),
                (None, _) => {}
            },
            AgentEvent::ItemCompleted {
                thread,
                turn: tid,
                item,
            } => turn.complete_item(thread, tid, item, command_output, tool_output),
            event => turn.entries.push(LiveEntry::Event(event)),
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

    /// Return raw server-side lifecycle events for one Giskard item, start before completion. This
    /// is intentionally not a wire snapshot: linked-thread opening needs the native routing id that
    /// wire conversion redacts before data reaches the browser.
    pub fn item_events(&self, thread_id: ThreadId, item_id: ItemId) -> Vec<AgentEvent> {
        let Some(turn) = (self.thread_id == Some(thread_id))
            .then_some(self.turn.as_ref())
            .flatten()
        else {
            return Vec::new();
        };
        if let Some(entry) = turn.item(item_id) {
            return entry
                .started_event()
                .into_iter()
                .chain(entry.completed_event())
                .collect();
        }
        turn.pending_starts
            .get(&item_id)
            .map(|(thread, turn_id, start)| started_event(*thread, *turn_id, start.clone()))
            .into_iter()
            .collect()
    }

    pub fn live_item(
        &self,
        thread_id: ThreadId,
        turn_id: TurnId,
        item_id: ItemId,
    ) -> Option<LiveItem> {
        if self.thread_id != Some(thread_id) {
            return None;
        }
        let turn = self.turn.as_ref().filter(|turn| turn.turn_id == turn_id)?;
        let Some(entry) = turn.item(item_id) else {
            return turn
                .pending_starts
                .get(&item_id)
                .map(|(_, _, start)| LiveItem::Started(start.clone()));
        };
        if let Some(item) = &entry.completed {
            return Some(LiveItem::Completed {
                item: item.clone(),
                command_output: turn.command_output_descriptors.get(&item_id).cloned(),
                tool_output: turn.tool_output_descriptors.get(&item_id).cloned(),
            });
        }
        entry.start.clone().map(LiveItem::Started)
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
                    .events()
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
                // LB2: `TurnStarted` first, each entry in arrival order, the starts still waiting
                // for a row, the latest usage last.
                let mut accumulated: Vec<WireAgentEvent> = Vec::with_capacity(
                    turn.entries.len() + 2, // usually one or two events per entry
                );
                accumulated.extend(
                    turn.turn_started
                        .clone()
                        .and_then(WireAgentEvent::from_agent_event),
                );
                for entry in &turn.entries {
                    match entry {
                        LiveEntry::Item(item) => item.expand(turn, &mut accumulated),
                        LiveEntry::Event(event) => {
                            accumulated.extend(WireAgentEvent::from_agent_event(event.clone()))
                        }
                    }
                }
                accumulated.extend(turn.pending_starts.values().filter_map(
                    |(thread, turn_id, start)| {
                        WireAgentEvent::from_agent_event(started_event(
                            *thread,
                            *turn_id,
                            start.clone(),
                        ))
                    },
                ));
                accumulated.extend(
                    turn.latest_usage
                        .clone()
                        .and_then(WireAgentEvent::from_agent_event),
                );
                // Answered requests still ride along in `accumulated` as `ServerRequestReceived`, and
                // replaying that renders an actionable card. Naming them lets the client render those
                // resolved instead, exactly as `answered_approvals` does.
                let answered_server_requests: Vec<ServerRequestId> = turn
                    .events()
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
                let approvals = pending_approvals(turn.events(), &turn.resolved_approvals);
                let server_requests =
                    pending_server_requests(turn.events(), &turn.resolved_server_requests);
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
fn pending_approvals<'a>(
    events: impl Iterator<Item = &'a AgentEvent>,
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
fn pending_server_requests<'a>(
    events: impl Iterator<Item = &'a AgentEvent>,
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
    use tracing_test::traced_test;

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

    #[test]
    fn live_item_tracks_the_latest_lifecycle_event_and_exact_turn() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();
        store.replace_turn_with_user_input(thread, turn, None);
        store.append(
            thread,
            AgentEvent::ItemStarted {
                thread,
                turn,
                item: command_start(item_id),
            },
        );
        assert!(matches!(
            store.live_item(thread, turn, item_id),
            Some(LiveItem::Started(item)) if item.id == item_id
        ));
        assert!(store.live_item(thread, TurnId::new(), item_id).is_none());
        assert!(store.live_item(thread, turn, ItemId::new()).is_none());

        let descriptor = CommandOutputDescriptor::from_durable("done", false, 4, 1, true);
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
                        output: "done".into(),
                        output_truncated: false,
                        output_original_bytes: None,
                        output_original_lines: None,
                        exit_code: Some(0),
                        status: Some("completed".into()),
                        process_id: None,
                        duration_ms: None,
                    },
                    created_at: Utc::now(),
                },
            },
            Some(descriptor.clone()),
        );
        assert!(matches!(
            store.live_item(thread, turn, item_id),
            Some(LiveItem::Completed { command_output: Some(found), .. }) if found == descriptor
        ));
        store.clear_turn(thread);
        assert!(store.live_item(thread, turn, item_id).is_none());
    }

    #[test]
    fn live_tool_item_carries_descriptor_and_no_inline_output() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();
        let descriptor = WireToolOutput {
            serialized_bytes: 4,
            version: "\"sha256_test\"".into(),
        };
        store.replace_turn_with_user_input(thread, turn, None);
        store.append_with_outputs(
            thread,
            AgentEvent::ItemCompleted {
                thread,
                turn,
                item: Item {
                    id: item_id,
                    harness_item_id: "tool_1".into(),
                    payload: ItemPayload::ToolCall {
                        name: "lookup".into(),
                        input: serde_json::Value::Null,
                        output: Some(serde_json::json!({"secret": true})),
                        server: None,
                        status: Some("completed".into()),
                        metadata: None,
                        subagent: None,
                        error: None,
                    },
                    created_at: Utc::now(),
                },
            },
            None,
            Some(descriptor.clone()),
        );
        let Some(LiveItem::Completed {
            item,
            tool_output: Some(found),
            ..
        }) = store.live_item(thread, turn, item_id)
        else {
            panic!("completed tool item");
        };
        assert_eq!(found, descriptor);
        assert!(matches!(
            item.payload,
            ItemPayload::ToolCall { output: None, .. }
        ));
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
        // The delayed `TurnStarted` replays first (D3): replayed after the item, the browser's
        // handler would forget the item's row.
        assert_eq!(snapshot.accumulated.len(), 2);
        assert!(matches!(
            snapshot.accumulated[0],
            WireAgentEvent::TurnStarted { .. }
        ));
        assert!(matches!(
            snapshot.accumulated[1],
            WireAgentEvent::ItemCompleted { .. }
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

    #[test]
    fn live_snapshot_keeps_only_the_latest_usage_event_at_the_end() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item = ItemId::new();
        store.start_turn(thread);
        store.append(thread, AgentEvent::TurnStarted { thread, turn });
        for input in [10, 20] {
            if input == 20 {
                store.append(
                    thread,
                    AgentEvent::ItemStarted {
                        thread,
                        turn,
                        item: command_start(item),
                    },
                );
            }
            store.append(
                thread,
                AgentEvent::TurnUsageUpdated {
                    thread,
                    turn,
                    usage: giskard_core::token::TokenUsage {
                        input,
                        output: 1,
                        total: input + 1,
                    },
                    context_window: Some(258_400),
                    model: None,
                },
            );
        }

        let snapshot = store.snapshot(thread).expect("snapshot");
        assert_eq!(snapshot.accumulated.len(), 3);
        assert!(matches!(
            snapshot.accumulated[1],
            WireAgentEvent::ItemStarted { .. }
        ));
        assert!(matches!(
            snapshot.accumulated[2],
            WireAgentEvent::TurnUsageUpdated {
                usage: giskard_core::token::TokenUsage { input: 20, .. },
                ..
            }
        ));
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

        store.replace_turn_with_user_input(thread, turn, None);
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

        let Some(LiveItem::Completed {
            item:
                Item {
                    payload:
                        ItemPayload::CommandExecution {
                            output: retained_output,
                            ..
                        },
                    ..
                },
            ..
        }) = store.live_item(thread, turn, item_id)
        else {
            panic!("bounded completed output");
        };
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
        store.replace_turn_with_user_input(thread, turn, None);

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

        let Some(LiveItem::Completed {
            item:
                Item {
                    payload:
                        ItemPayload::CommandExecution {
                            output: retained, ..
                        },
                    ..
                },
            ..
        }) = store.live_item(thread, turn, item_id)
        else {
            panic!("completed command item");
        };
        assert_eq!(retained, "");

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

    // ---- Item-centric buffer (L1) ----

    fn item_start(id: ItemId, kind: ItemKind) -> ItemStart {
        ItemStart {
            id,
            harness_item_id: format!("h_{id}"),
            kind,
            command: None,
            tool: None,
        }
    }

    fn started(thread: ThreadId, turn: TurnId, id: ItemId, kind: ItemKind) -> AgentEvent {
        AgentEvent::ItemStarted {
            thread,
            turn,
            item: item_start(id, kind),
        }
    }

    fn text_delta(thread: ThreadId, turn: TurnId, id: ItemId, text: &str) -> AgentEvent {
        AgentEvent::ItemDelta {
            thread,
            turn,
            item_id: id,
            delta: ItemDelta::Text { text: text.into() },
        }
    }

    fn output_delta(thread: ThreadId, turn: TurnId, id: ItemId, chunk: &str) -> AgentEvent {
        AgentEvent::ItemDelta {
            thread,
            turn,
            item_id: id,
            delta: ItemDelta::CommandOutput {
                chunk: chunk.into(),
            },
        }
    }

    fn completed_message(thread: ThreadId, turn: TurnId, id: ItemId, text: &str) -> AgentEvent {
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id,
                harness_item_id: format!("h_{id}"),
                payload: ItemPayload::AgentMessage { text: text.into() },
                created_at: Utc::now(),
            },
        }
    }

    fn completed_command(thread: ThreadId, turn: TurnId, id: ItemId, status: &str) -> AgentEvent {
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id,
                harness_item_id: "cmd_1".into(),
                payload: ItemPayload::CommandExecution {
                    command: "yes".into(),
                    cwd: "/tmp/project".into(),
                    output: "done".into(),
                    output_truncated: false,
                    output_original_bytes: None,
                    output_original_lines: None,
                    exit_code: None,
                    status: Some(status.into()),
                    process_id: Some("proc_1".into()),
                    duration_ms: None,
                },
                created_at: Utc::now(),
            },
        }
    }

    fn usage(thread: ThreadId, turn: TurnId, input: u64) -> AgentEvent {
        AgentEvent::TurnUsageUpdated {
            thread,
            turn,
            usage: giskard_core::token::TokenUsage {
                input,
                output: 1,
                total: input + 1,
            },
            context_window: None,
            model: None,
        }
    }

    /// One readable label per replayed event, so an ordering test states the whole sequence.
    fn shape(snapshot: &LiveTurnSnapshot) -> Vec<String> {
        snapshot
            .accumulated
            .iter()
            .map(|event| match event {
                WireAgentEvent::TurnStarted { .. } => "turn_started".to_owned(),
                WireAgentEvent::TurnUsageUpdated { .. } => "usage".to_owned(),
                WireAgentEvent::ItemStarted { item, .. } => format!("start:{}", item.id),
                WireAgentEvent::ItemCompleted { item, .. } => format!("completed:{}", item.id),
                WireAgentEvent::ItemDelta {
                    item_id,
                    delta: ItemDelta::Text { .. },
                    ..
                } => format!("text:{item_id}"),
                WireAgentEvent::ItemDelta {
                    item_id,
                    delta: ItemDelta::CommandOutput { .. },
                    ..
                } => format!("output:{item_id}"),
                WireAgentEvent::ApprovalRequested { request, .. } => {
                    format!("approval:{}", request.id.0)
                }
                WireAgentEvent::ServerRequestReceived { request, .. } => {
                    format!("server_request:{}", request.id.0)
                }
                WireAgentEvent::TurnCompleted { .. } => "turn_completed".to_owned(),
                other => format!("{other:?}"),
            })
            .collect()
    }

    fn texts(snapshot: &LiveTurnSnapshot) -> Vec<&str> {
        snapshot
            .accumulated
            .iter()
            .filter_map(|event| match event {
                WireAgentEvent::ItemDelta {
                    delta: ItemDelta::Text { text },
                    ..
                } => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }

    fn outputs(snapshot: &LiveTurnSnapshot) -> Vec<&str> {
        snapshot
            .accumulated
            .iter()
            .filter_map(|event| match event {
                WireAgentEvent::ItemDelta {
                    delta: ItemDelta::CommandOutput { chunk },
                    ..
                } => Some(chunk.as_str()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_completed_message_replays_without_its_chunks_and_matches_one_chunk() {
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let id = ItemId::new();
        let words: Vec<String> = (0..50).map(|i| format!("w{i} ")).collect();
        let full = words.concat();
        let completion = completed_message(thread, turn, id, &full);

        let replay = |chunks: &[String]| {
            let mut store = LiveTurnState::new();
            store.replace_turn_with_user_input(thread, turn, None);
            store.append(thread, AgentEvent::TurnStarted { thread, turn });
            store.append(thread, started(thread, turn, id, ItemKind::AgentMessage));
            for chunk in chunks {
                store.append(thread, text_delta(thread, turn, id, chunk));
            }
            store.append(thread, completion.clone());
            store.snapshot(thread).expect("snapshot")
        };
        let chunked = replay(&words);
        let whole = replay(std::slice::from_ref(&full));

        assert_eq!(
            shape(&chunked),
            vec![
                "turn_started".to_owned(),
                format!("start:{id}"),
                format!("completed:{id}")
            ]
        );
        assert!(matches!(
            &chunked.accumulated[2],
            WireAgentEvent::ItemCompleted { item, .. }
                if matches!(&item.payload, WireItemPayload::AgentMessage { text } if *text == full)
        ));
        assert_eq!(
            serde_json::to_value(&chunked.accumulated).expect("serialize"),
            serde_json::to_value(&whole.accumulated).expect("serialize")
        );
    }

    #[test]
    fn an_open_item_replays_its_start_and_one_coalesced_delta() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let id = ItemId::new();
        store.replace_turn_with_user_input(thread, turn, None);
        store.append(thread, started(thread, turn, id, ItemKind::Reasoning));
        for chunk in ["Weighing ", "the ", "options"] {
            store.append(thread, text_delta(thread, turn, id, chunk));
        }

        let snapshot = store.snapshot(thread).expect("snapshot");
        assert_eq!(
            shape(&snapshot),
            vec![format!("start:{id}"), format!("text:{id}")]
        );
        assert_eq!(texts(&snapshot), vec!["Weighing the options"]);
    }

    // The browser joins a tool call's progress chunks with a newline (`appendToolProgress`) and
    // concatenates everything else (`appendStream`), keyed by the kind `item_started` announced.
    // Replaying the coalesced text as one chunk must render what the chunks rendered.
    #[test]
    fn tool_progress_joins_with_newlines_and_unstarted_text_concatenates() {
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let coalesce = |start: Option<ItemKind>, chunks: &[&str]| {
            let mut store = LiveTurnState::new();
            let id = ItemId::new();
            store.replace_turn_with_user_input(thread, turn, None);
            if let Some(kind) = start {
                store.append(thread, started(thread, turn, id, kind));
            }
            for chunk in chunks {
                store.append(thread, text_delta(thread, turn, id, chunk));
            }
            let snapshot = store.snapshot(thread).expect("snapshot");
            texts(&snapshot).concat()
        };

        // `current ? current + "\n" + chunk : chunk`: an empty chunk after text still adds its
        // separator, and an empty chunk at the start adds nothing.
        assert_eq!(
            coalesce(Some(ItemKind::ToolCall), &["a", "", "b"]),
            "a\n\nb"
        );
        assert_eq!(coalesce(Some(ItemKind::ToolCall), &["", "a", "b"]), "a\nb");
        assert_eq!(coalesce(None, &["a", "", "b"]), "ab");
        assert_eq!(coalesce(Some(ItemKind::AgentMessage), &["a", "b"]), "ab");
    }

    #[test]
    fn an_item_keeps_the_slot_where_it_was_first_seen() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let (a, b) = (ItemId::new(), ItemId::new());
        store.replace_turn_with_user_input(thread, turn, None);
        store.append(thread, started(thread, turn, a, ItemKind::AgentMessage));
        store.append(thread, started(thread, turn, b, ItemKind::AgentMessage));
        store.append(thread, completed_message(thread, turn, a, "A"));
        store.append(thread, completed_message(thread, turn, b, "B"));
        store.append(thread, text_delta(thread, turn, a, "late"));

        let snapshot = store.snapshot(thread).expect("snapshot");
        assert_eq!(
            shape(&snapshot),
            vec![
                format!("start:{a}"),
                format!("completed:{a}"),
                format!("text:{a}"),
                format!("start:{b}"),
                format!("completed:{b}"),
            ]
        );
    }

    #[test]
    fn requests_keep_their_arrival_position_between_items() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let a = ItemId::new();
        let request = ServerRequestId("sr_1".into());
        store.replace_turn_with_user_input(thread, turn, None);
        // A command start with its metadata draws the row, so the item's slot is its start.
        store.append(
            thread,
            AgentEvent::ItemStarted {
                thread,
                turn,
                item: command_start(a),
            },
        );
        store.append(thread, approval_requested(thread, turn, "ap_1"));
        store.append(thread, completed_command(thread, turn, a, "completed"));
        store.append(thread, server_request_received(thread, turn, &request));

        let snapshot = store.snapshot(thread).expect("snapshot");
        assert_eq!(
            shape(&snapshot),
            vec![
                format!("start:{a}"),
                format!("completed:{a}"),
                "approval:ap_1".to_owned(),
                "server_request:sr_1".to_owned(),
            ]
        );
        assert_eq!(
            outstanding_approvals(&snapshot),
            vec![ApprovalId("ap_1".into())]
        );
        assert_eq!(outstanding_server_requests(&snapshot), vec![request]);
    }

    // An agent message's row appears at its first delta, so live the card is drawn above it. Its
    // start must not claim a slot before the card, or the replayed completion would draw the row
    // above the card instead.
    #[test]
    fn a_request_between_a_start_and_the_first_delta_replays_above_the_row() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let (done, open) = (ItemId::new(), ItemId::new());
        store.replace_turn_with_user_input(thread, turn, None);
        store.append(thread, started(thread, turn, done, ItemKind::AgentMessage));
        store.append(thread, approval_requested(thread, turn, "ap_1"));
        store.append(thread, text_delta(thread, turn, done, "A"));
        store.append(thread, completed_message(thread, turn, done, "A"));
        store.append(thread, started(thread, turn, open, ItemKind::Reasoning));
        store.append(thread, approval_requested(thread, turn, "ap_2"));
        store.append(thread, text_delta(thread, turn, open, "thinking"));

        let snapshot = store.snapshot(thread).expect("snapshot");
        assert_eq!(
            shape(&snapshot),
            vec![
                "approval:ap_1".to_owned(),
                format!("start:{done}"),
                format!("completed:{done}"),
                "approval:ap_2".to_owned(),
                format!("start:{open}"),
                format!("text:{open}"),
            ]
        );
    }

    // A start that draws no row and has had no delta yet registers only a kind in the browser, so
    // it replays after the entries, before the usage; the lookups still see it.
    #[test]
    fn a_start_still_waiting_for_its_row_replays_after_the_entries() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let (note, command) = (ItemId::new(), ItemId::new());
        store.replace_turn_with_user_input(thread, turn, None);
        store.append(thread, started(thread, turn, note, ItemKind::Reasoning));
        store.append(
            thread,
            AgentEvent::ItemStarted {
                thread,
                turn,
                item: command_start(command),
            },
        );
        store.append(thread, usage(thread, turn, 10));

        let snapshot = store.snapshot(thread).expect("snapshot");
        assert_eq!(
            shape(&snapshot),
            vec![
                format!("start:{command}"),
                format!("start:{note}"),
                "usage".to_owned()
            ]
        );
        assert!(matches!(
            store.live_item(thread, turn, note),
            Some(LiveItem::Started(item)) if item.id == note
        ));
        let events = store.item_events(thread, note);
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], AgentEvent::ItemStarted { item, .. } if item.id == note));
    }

    #[test]
    fn a_delayed_turn_started_replays_first_and_restamps_the_turn() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let provisional = TurnId::new();
        let turn = TurnId::new();
        let a = ItemId::new();
        store.replace_turn_with_user_input(thread, provisional, None);
        store.append(thread, started(thread, turn, a, ItemKind::AgentMessage));
        store.append(thread, AgentEvent::TurnStarted { thread, turn });

        let snapshot = store.snapshot(thread).expect("snapshot");
        assert_eq!(snapshot.turn_id, turn);
        assert_eq!(
            shape(&snapshot),
            vec!["turn_started".to_owned(), format!("start:{a}")]
        );
    }

    #[test]
    fn usage_replays_last_even_when_items_arrive_after_it() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let a = ItemId::new();
        store.replace_turn_with_user_input(thread, turn, None);
        store.append(thread, AgentEvent::TurnStarted { thread, turn });
        store.append(thread, usage(thread, turn, 10));
        store.append(thread, started(thread, turn, a, ItemKind::AgentMessage));

        let snapshot = store.snapshot(thread).expect("snapshot");
        assert_eq!(
            shape(&snapshot),
            vec![
                "turn_started".to_owned(),
                format!("start:{a}"),
                "usage".to_owned()
            ]
        );
    }

    #[test]
    fn a_command_completion_drops_streamed_output_and_later_output_follows_it() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let (done, running) = (ItemId::new(), ItemId::new());
        store.replace_turn_with_user_input(thread, turn, None);

        store.append(
            thread,
            started(thread, turn, done, ItemKind::CommandExecution),
        );
        store.append(thread, output_delta(thread, turn, done, "streamed"));
        store.append(thread, completed_command(thread, turn, done, "completed"));

        store.append(
            thread,
            started(thread, turn, running, ItemKind::CommandExecution),
        );
        store.append(thread, output_delta(thread, turn, running, "before"));
        store.append(
            thread,
            completed_command(thread, turn, running, "in_progress"),
        );
        store.append(thread, output_delta(thread, turn, running, "after"));

        let snapshot = store.snapshot(thread).expect("snapshot");
        assert_eq!(
            shape(&snapshot),
            vec![
                format!("start:{done}"),
                format!("completed:{done}"),
                format!("start:{running}"),
                format!("completed:{running}"),
                format!("output:{running}"),
            ]
        );
        assert_eq!(outputs(&snapshot), vec!["after"]);
    }

    #[test]
    fn lookups_read_the_item_entry() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let (open, done, delta_only) = (ItemId::new(), ItemId::new(), ItemId::new());
        store.replace_turn_with_user_input(thread, turn, None);
        store.append(thread, started(thread, turn, open, ItemKind::AgentMessage));
        store.append(thread, started(thread, turn, done, ItemKind::AgentMessage));
        store.append(thread, completed_message(thread, turn, done, "done"));
        store.append(thread, text_delta(thread, turn, delta_only, "orphan"));

        assert!(matches!(
            store.live_item(thread, turn, open),
            Some(LiveItem::Started(item)) if item.id == open
        ));
        assert!(matches!(
            store.live_item(thread, turn, done),
            Some(LiveItem::Completed { item, .. }) if item.id == done
        ));
        assert!(store.live_item(thread, turn, delta_only).is_none());
        assert!(store.live_item(thread, TurnId::new(), done).is_none());

        let events = store.item_events(thread, done);
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], AgentEvent::ItemStarted { item, .. } if item.id == done));
        assert!(matches!(&events[1], AgentEvent::ItemCompleted { item, .. } if item.id == done));
        let events = store.item_events(thread, open);
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], AgentEvent::ItemStarted { item, .. } if item.id == open));
        assert!(store.item_events(thread, delta_only).is_empty());
        assert!(store.item_events(ThreadId::new(), done).is_empty());

        // A completion comes back with the descriptors recorded for it, and only those.
        let (command, tool) = (ItemId::new(), ItemId::new());
        let command_descriptor = CommandOutputDescriptor::from_durable("done", false, 4, 1, true);
        let tool_descriptor = WireToolOutput {
            serialized_bytes: 4,
            version: "\"sha256_lookup\"".into(),
        };
        store.append_with_command_output(
            thread,
            completed_command(thread, turn, command, "completed"),
            Some(command_descriptor.clone()),
        );
        store.append_with_outputs(
            thread,
            AgentEvent::ItemCompleted {
                thread,
                turn,
                item: Item {
                    id: tool,
                    harness_item_id: "tool_lookup".into(),
                    payload: ItemPayload::ToolCall {
                        name: "lookup".into(),
                        input: serde_json::Value::Null,
                        output: Some(serde_json::json!({"found": true})),
                        server: None,
                        status: Some("completed".into()),
                        metadata: None,
                        subagent: None,
                        error: None,
                    },
                    created_at: Utc::now(),
                },
            },
            None,
            Some(tool_descriptor.clone()),
        );
        assert!(matches!(
            store.live_item(thread, turn, command),
            Some(LiveItem::Completed { command_output: Some(found), tool_output: None, .. })
                if found == command_descriptor
        ));
        assert!(matches!(
            store.live_item(thread, turn, tool),
            Some(LiveItem::Completed { command_output: None, tool_output: Some(found), .. })
                if found == tool_descriptor
        ));
    }

    #[test]
    #[traced_test]
    fn a_start_after_a_completion_keeps_the_item_completed() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let id = ItemId::new();
        store.replace_turn_with_user_input(thread, turn, None);
        store.append(thread, completed_message(thread, turn, id, "done"));
        store.append(thread, started(thread, turn, id, ItemKind::AgentMessage));

        assert!(matches!(
            store.live_item(thread, turn, id),
            Some(LiveItem::Completed { .. })
        ));
        assert!(logs_contain(
            "live buffer received ItemStarted for an already completed item"
        ));
    }

    // A turn whose persistence failed keeps its buffer, so the snapshot a `PersistenceBlocked`
    // thread sends must still end with the `TurnCompleted` that unlocks the composer.
    #[test]
    fn a_persistence_blocked_turn_replays_its_completion_last() {
        let mut store = LiveTurnState::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let a = ItemId::new();
        store.replace_turn_with_user_input(thread, turn, None);
        store.append(thread, AgentEvent::TurnStarted { thread, turn });
        store.append(thread, started(thread, turn, a, ItemKind::AgentMessage));
        store.append(thread, completed_message(thread, turn, a, "A"));
        store.append(
            thread,
            AgentEvent::TurnCompleted {
                thread,
                turn,
                usage: giskard_core::token::TokenUsage::default(),
                status: giskard_core::turn::TurnStatus {
                    kind: giskard_core::turn::TurnStatusKind::Completed,
                    message: None,
                },
            },
        );

        let snapshot = store.snapshot(thread).expect("snapshot");
        assert_eq!(
            shape(&snapshot).last().map(String::as_str),
            Some("turn_completed")
        );
    }
}
