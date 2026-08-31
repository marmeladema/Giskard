//! Bounded harness channels and strict route contracts shared by server integration tests.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::ThreadId;
use giskard_core::model::ModelRef;
use giskard_harness::{
    AgentEventStream, ClaimedNativeRoute, HarnessSignal, HarnessSignalStream,
    ThreadActivationCause, thread_activation,
};
use tokio::sync::mpsc;

#[derive(Default)]
struct TestRoutes {
    by_native: HashMap<String, ClaimedNativeRoute>,
    native_by_thread: HashMap<ThreadId, String>,
    next_epoch: u64,
}

/// Minimal strict M3 routing contract for server integration-test harnesses.
#[derive(Clone)]
pub(crate) struct TestRouteContract {
    routes: Arc<Mutex<TestRoutes>>,
    signal_receiver: Arc<Mutex<Option<mpsc::Receiver<HarnessSignal>>>>,
    // Retaining the bounded sender keeps the signal stream alive for the harness lifetime.
    #[allow(dead_code)] // Some test harnesses can only fail open, so they never activate.
    signal_sender: mpsc::Sender<HarnessSignal>,
}

impl TestRouteContract {
    pub(crate) fn new() -> Self {
        let (signal_sender, signal_receiver) = mpsc::channel(16);
        Self {
            routes: Arc::new(Mutex::new(TestRoutes::default())),
            signal_receiver: Arc::new(Mutex::new(Some(signal_receiver))),
            signal_sender,
        }
    }

    pub(crate) fn claim_native_route(
        &self,
        harness_thread_id: String,
        suggested_thread_id: ThreadId,
    ) -> Result<ClaimedNativeRoute, HarnessError> {
        let harness_thread_id = harness_thread_id.trim();
        if harness_thread_id.is_empty() {
            return Err(HarnessError::Protocol(
                "cannot claim an empty test native thread id".into(),
            ));
        }

        let mut routes = self
            .routes
            .lock()
            .expect("test route-contract route lock poisoned");
        if let Some(route) = routes.by_native.get(harness_thread_id) {
            return Ok(route.clone());
        }
        if let Some(native) = routes.native_by_thread.get(&suggested_thread_id) {
            return Err(HarnessError::Protocol(format!(
                "test thread {suggested_thread_id} is already bound to native route {native}"
            )));
        }

        routes.next_epoch = routes
            .next_epoch
            .checked_add(1)
            .ok_or_else(|| HarnessError::Protocol("test route epoch space exhausted".into()))?;
        let route = ClaimedNativeRoute {
            thread_id: suggested_thread_id,
            harness_thread_id: harness_thread_id.to_owned(),
            route_epoch: routes.next_epoch,
        };
        routes
            .native_by_thread
            .insert(route.thread_id, route.harness_thread_id.clone());
        routes
            .by_native
            .insert(route.harness_thread_id.clone(), route.clone());
        Ok(route)
    }

    pub(crate) fn take_harness_signals(&self) -> Result<HarnessSignalStream, HarnessError> {
        self.signal_receiver
            .lock()
            .expect("test route-contract signal receiver lock poisoned")
            .take()
            .map(HarnessSignalStream::new)
            .ok_or_else(|| {
                HarnessError::Protocol("test harness signal stream already taken".into())
            })
    }

    #[allow(dead_code)] // This shared module is compiled by error-only harness tests too.
    pub(crate) async fn activate_primary(
        &self,
        harness_thread_id: String,
        suggested_thread_id: ThreadId,
        identity_generation: Option<u64>,
        reported_model: Option<ModelRef>,
    ) -> Result<ClaimedNativeRoute, HarnessError> {
        let route = self.claim_native_route(harness_thread_id, suggested_thread_id)?;
        if let Some(generation) = identity_generation {
            let (activation, readiness) = thread_activation(
                route.clone(),
                ThreadActivationCause::IdentityResponse {
                    method: "test/open".into(),
                    generation,
                    reported_model,
                },
            );
            self.signal_sender
                .send(HarnessSignal::Activate(activation))
                .await
                .map_err(|_| {
                    HarnessError::Transport("test harness signal receiver closed".into())
                })?;
            readiness.await.map_err(|_| {
                HarnessError::Transport("test Primary activation acknowledgement dropped".into())
            })??;
        }
        Ok(route)
    }

    #[allow(dead_code)] // Only harnesses that model notification-first activation use this path.
    pub(crate) async fn activate_notification(
        &self,
        route: ClaimedNativeRoute,
        method: &str,
    ) -> Result<(), HarnessError> {
        let (activation, readiness) = thread_activation(
            route,
            ThreadActivationCause::Notification {
                method: method.to_owned(),
            },
        );
        self.signal_sender
            .send(HarnessSignal::Activate(activation))
            .await
            .map_err(|_| HarnessError::Transport("test harness signal receiver closed".into()))?;
        readiness.await.map_err(|_| {
            HarnessError::Transport("test notification activation acknowledgement dropped".into())
        })??;
        Ok(())
    }
}

impl Default for TestRouteContract {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub(crate) struct BoundedEventRoute {
    sender: mpsc::Sender<AgentEvent>,
    receiver: Arc<Mutex<Option<mpsc::Receiver<AgentEvent>>>>,
}

#[allow(dead_code)] // This shared module is also compiled by tests that only need route claims.
impl BoundedEventRoute {
    pub(crate) fn new(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);
        Self {
            sender,
            receiver: Arc::new(Mutex::new(Some(receiver))),
        }
    }

    pub(crate) async fn send(&self, event: AgentEvent) -> Result<(), HarnessError> {
        self.sender
            .send(event)
            .await
            .map_err(|_| HarnessError::Transport("test event receiver closed".into()))
    }

    pub(crate) fn take_stream(&self) -> Option<AgentEventStream> {
        self.receiver
            .lock()
            .expect("test event-route receiver lock poisoned")
            .take()
            .map(AgentEventStream::new)
    }

    #[allow(dead_code)] // This shared module is compiled independently by each integration test.
    pub(crate) fn receiver_count(&self) -> usize {
        usize::from(
            self.receiver
                .lock()
                .expect("test event-route receiver lock poisoned")
                .is_none()
                && !self.sender.is_closed(),
        )
    }
}

#[allow(dead_code)] // This shared module is compiled independently by each integration test.
pub(crate) fn closed_event_stream() -> AgentEventStream {
    let (_, receiver) = mpsc::channel(1);
    AgentEventStream::new(receiver)
}
