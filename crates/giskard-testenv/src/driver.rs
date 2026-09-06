use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use giskard_core::ids::{ProjectId, ThreadId};
use giskard_server::{DriverEvent, DriverEventSink};
use tokio::sync::mpsc;

struct ProbeSink {
    tx: mpsc::UnboundedSender<(ProjectId, DriverEvent)>,
}

impl DriverEventSink for ProbeSink {
    fn observe(&self, project_id: ProjectId, event: &DriverEvent) {
        event.log(project_id);
        let _ = self.tx.send((project_id, event.clone()));
    }
}

pub struct DriverProbe(mpsc::UnboundedReceiver<(ProjectId, DriverEvent)>);

pub fn probe() -> (Arc<dyn DriverEventSink>, DriverProbe) {
    let (tx, rx) = mpsc::unbounded_channel();
    (Arc::new(ProbeSink { tx }), DriverProbe(rx))
}

impl DriverProbe {
    /// The next admission of `harness_thread_id` that installed a thread.
    pub async fn expect_admitted(&mut self, harness_thread_id: &str) -> ThreadId {
        let (_, event) = self
            .expect(|event| match event {
                DriverEvent::DiscoveryFinished {
                    native_thread_id, ..
                }
                | DriverEvent::LinkFinished {
                    native_thread_id, ..
                } => native_thread_id == harness_thread_id,
                _ => false,
            })
            .await;
        match event {
            DriverEvent::DiscoveryFinished {
                outcome: Ok(Some(thread)),
                ..
            }
            | DriverEvent::LinkFinished {
                outcome: Ok(Some(thread)),
                ..
            } => thread,
            DriverEvent::DiscoveryFinished { outcome, .. }
            | DriverEvent::LinkFinished { outcome, .. } => {
                panic!("admission for {harness_thread_id:?} did not install a thread: {outcome:?}")
            }
            _ => unreachable!("expect predicate only accepts admission events"),
        }
    }

    /// The next link admission under `parent` that installed a child.
    pub async fn expect_child_of(&mut self, parent: ThreadId) -> ThreadId {
        let (_, event) = self
            .expect(|event| {
                matches!(event, DriverEvent::LinkFinished { parent_thread_id, .. } if *parent_thread_id == parent)
            })
            .await;
        match event {
            DriverEvent::LinkFinished {
                outcome: Ok(Some(thread)),
                ..
            } => thread,
            DriverEvent::LinkFinished {
                native_thread_id,
                outcome,
                ..
            } => {
                panic!(
                    "link admission for {native_thread_id:?} under {parent} did not install a thread: {outcome:?}"
                )
            }
            _ => unreachable!("expect predicate only accepts link events"),
        }
    }

    pub async fn expect(
        &mut self,
        pred: impl Fn(&DriverEvent) -> bool,
    ) -> (ProjectId, DriverEvent) {
        let seen = Mutex::new(Vec::new());
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some((project_id, event)) = self.0.recv().await {
                if pred(&event) {
                    return (project_id, event);
                }
                seen.lock().unwrap().push(event);
            }
            panic!("driver event stream closed")
        })
        .await;
        match result {
            Ok(event) => event,
            Err(_) => panic!(
                "driver event was not observed; discarded: {:?}",
                seen.into_inner().unwrap()
            ),
        }
    }

    pub fn drain(&mut self) -> Vec<(ProjectId, DriverEvent)> {
        let mut events = Vec::new();
        while let Ok(event) = self.0.try_recv() {
            events.push(event);
        }
        events
    }
}
