use std::collections::HashMap;

use tokio::sync::Mutex;

use giskard_core::event::AgentEvent;
use giskard_core::ids::{ThreadId, TurnId};
use giskard_proto::{LiveTurnSnapshot, WireAgentEvent, WireApprovalRequest};

struct LiveTurn {
    turn_id: TurnId,
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
        let mut buffers = self.buffers.lock().await;
        buffers.insert(
            thread_id,
            LiveTurn {
                turn_id: TurnId::new(),
                events: Vec::new(),
            },
        );
    }

    pub async fn append(&self, thread_id: ThreadId, event: AgentEvent) {
        let mut buffers = self.buffers.lock().await;
        if let Some(turn) = buffers.get_mut(&thread_id) {
            if let AgentEvent::TurnStarted { turn: tid, .. } = &event {
                turn.turn_id = *tid;
            }
            turn.events.push(event);
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
            LiveTurnSnapshot {
                thread_id,
                turn_id: turn.turn_id,
                accumulated,
                pending_approval,
            }
        })
    }
}

impl Default for LiveBufferStore {
    fn default() -> Self {
        Self::new()
    }
}
