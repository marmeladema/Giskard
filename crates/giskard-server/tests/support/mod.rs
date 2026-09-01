use std::sync::{Arc, Mutex};

use giskard_core::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_harness::{AgentEventStream, ThreadAttachment, ThreadHandle};
use tokio::sync::broadcast;

/// Minimal single-receiver route authority for integration-test harnesses.
#[derive(Clone)]
pub struct TestEventRoute {
    sender: Arc<Mutex<Option<broadcast::Sender<AgentEvent>>>>,
    state: Arc<Mutex<RouteState>>,
}

struct RouteState {
    receiver: Option<AgentEventStream>,
    phase: RoutePhase,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RoutePhase {
    Idle,
    Attaching,
    Owned,
    Closed,
}

impl TestEventRoute {
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = broadcast::channel(capacity);
        Self {
            sender: Arc::new(Mutex::new(Some(sender))),
            state: Arc::new(Mutex::new(RouteState {
                receiver: Some(AgentEventStream::new(receiver)),
                phase: RoutePhase::Idle,
            })),
        }
    }

    #[allow(dead_code)]
    pub fn send(&self, event: AgentEvent) -> Result<usize, ()> {
        let sender = self.sender.lock().ok().and_then(|sender| sender.clone());
        match sender {
            Some(sender) => sender.send(event).map_err(|_| ()),
            None => Err(()),
        }
    }

    #[allow(dead_code)]
    pub fn receiver_count(&self) -> usize {
        self.sender
            .lock()
            .ok()
            .and_then(|sender| sender.as_ref().map(broadcast::Sender::receiver_count))
            .unwrap_or_default()
    }

    pub fn attach(&self, handle: ThreadHandle) -> Result<ThreadAttachment, HarnessError> {
        let stream = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| HarnessError::Protocol("test event route lock poisoned".into()))?;
            if state.phase != RoutePhase::Idle {
                return Err(HarnessError::Protocol(
                    "test event route already has an attachment or owner".into(),
                ));
            }
            let stream = state.receiver.take().ok_or_else(|| {
                HarnessError::Protocol("test event route lost its receiver".into())
            })?;
            state.phase = RoutePhase::Attaching;
            stream
        };
        let commit_state = self.state.clone();
        let attachment_drop_state = self.state.clone();
        Ok(ThreadAttachment::from_route(
            handle,
            stream,
            move || {
                let mut state = match commit_state.lock() {
                    Ok(state) => state,
                    Err(_) => {
                        return Err(HarnessError::Protocol(
                            "test event route lock poisoned".into(),
                        ));
                    }
                };
                if state.phase != RoutePhase::Attaching {
                    return Err(HarnessError::Protocol(
                        "test event attachment is stale".into(),
                    ));
                }
                state.phase = RoutePhase::Owned;
                drop(state);
                let owner_drop_state = commit_state.clone();
                Ok(Box::new(move |stream| {
                    let Ok(mut state) = owner_drop_state.lock() else {
                        return;
                    };
                    if state.phase == RoutePhase::Owned {
                        state.receiver = Some(stream);
                        state.phase = RoutePhase::Idle;
                    }
                })
                    as Box<dyn FnOnce(AgentEventStream) + Send>)
            },
            move |stream| {
                let Ok(mut state) = attachment_drop_state.lock() else {
                    return;
                };
                if state.phase == RoutePhase::Attaching {
                    state.receiver = Some(stream);
                    state.phase = RoutePhase::Idle;
                }
            },
        ))
    }

    #[allow(dead_code)]
    pub fn close(&self) {
        if let Ok(mut sender) = self.sender.lock() {
            *sender = None;
        }
        if let Ok(mut state) = self.state.lock() {
            state.receiver = None;
            state.phase = RoutePhase::Closed;
        }
    }
}
