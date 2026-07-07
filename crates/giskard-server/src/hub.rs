use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Mutex, mpsc};
use tracing::debug;

use giskard_core::event::AgentEvent;
use giskard_core::ids::ThreadId;
use giskard_proto::ServerMessage;

pub type ClientId = usize;

type SubList = Vec<(ClientId, mpsc::Sender<ServerMessage>)>;

pub struct Hub {
    subs: Mutex<HashMap<ThreadId, SubList>>,
    next_id: AtomicUsize,
}

impl Hub {
    pub fn new() -> Self {
        Self {
            subs: Mutex::new(HashMap::new()),
            next_id: AtomicUsize::new(1),
        }
    }

    pub fn next_client_id(&self) -> ClientId {
        self.next_id.fetch_add(1, Ordering::Relaxed)
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

    pub async fn broadcast(&self, thread_id: ThreadId, msg: ServerMessage) {
        let mut subs = self.subs.lock().await;
        if let Some(list) = subs.get_mut(&thread_id) {
            list.retain(|(_, tx)| tx.try_send(msg.clone()).is_ok());
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
