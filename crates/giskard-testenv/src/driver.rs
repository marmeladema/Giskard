use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use giskard_core::ids::ProjectId;
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
