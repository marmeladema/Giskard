use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::debug;

use giskard_core::error::HarnessError;
use giskard_core::ids::{ProjectId, ThreadId, TurnId};
use giskard_core::turn::TurnOverrides;
use giskard_core::user_input::UserInput;
use giskard_harness::{AgentHarness, OpenThreadOptions, ThreadHandle};
use giskard_persist::store::ProjectConfig;

use crate::hub::Hub;
use crate::live_buffer::LiveBufferStore;

pub trait HarnessFactory: Send + Sync {
    fn create(&self, config: &ProjectConfig) -> Result<Arc<dyn AgentHarness>, HarnessError>;
}

pub struct HarnessRegistry {
    harnesses: Mutex<HashMap<ProjectId, Arc<dyn AgentHarness>>>,
    threads: Mutex<HashMap<ThreadId, (ProjectId, ThreadHandle)>>,
    factory: Arc<dyn HarnessFactory>,
    hub: Arc<Hub>,
    live_buffers: Arc<LiveBufferStore>,
}

impl HarnessRegistry {
    pub fn new(
        factory: Arc<dyn HarnessFactory>,
        hub: Arc<Hub>,
        live_buffers: Arc<LiveBufferStore>,
    ) -> Self {
        Self {
            harnesses: Mutex::new(HashMap::new()),
            threads: Mutex::new(HashMap::new()),
            factory,
            hub,
            live_buffers,
        }
    }

    async fn get_or_create_harness(
        &self,
        project: ProjectId,
        config: &ProjectConfig,
    ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
        let mut harnesses = self.harnesses.lock().await;
        if let Some(h) = harnesses.get(&project) {
            return Ok(h.clone());
        }
        let h = self.factory.create(config)?;
        harnesses.insert(project, h.clone());
        Ok(h)
    }

    pub async fn open_thread(
        &self,
        config: &ProjectConfig,
        workspace_root: &str,
        resume: Option<String>,
        initial_model: giskard_core::model::ModelRef,
    ) -> Result<ThreadHandle, HarnessError> {
        let harness = self.get_or_create_harness(config.id, config).await?;

        let handle = harness
            .open_thread(OpenThreadOptions {
                project: config.id,
                workspace_root: workspace_root.into(),
                resume,
                initial_model: initial_model.clone(),
            })
            .await?;

        let mut threads = self.threads.lock().await;
        threads.insert(handle.thread, (config.id, handle.clone()));

        Ok(handle)
    }

    pub async fn start_turn(
        &self,
        thread_id: ThreadId,
        input: UserInput,
        overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        let threads = self.threads.lock().await;
        let (project_id, handle) = threads
            .get(&thread_id)
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let project_id = *project_id;
        let handle = handle.clone();
        drop(threads);

        let harnesses = self.harnesses.lock().await;
        let harness = harnesses
            .get(&project_id)
            .ok_or(HarnessError::ThreadNotFound(thread_id))?
            .clone();
        drop(harnesses);

        let hub = self.hub.clone();
        let live_buffers = self.live_buffers.clone();
        let tid = thread_id;

        let stream = harness.subscribe(&handle);
        let turn_id = harness.start_turn(&handle, input, overrides).await?;

        tokio::spawn(async move {
            forward_events(tid, stream, hub, live_buffers).await;
        });

        Ok(turn_id)
    }

    pub async fn get_thread_handle(&self, thread_id: ThreadId) -> Option<ThreadHandle> {
        let threads = self.threads.lock().await;
        threads.get(&thread_id).map(|(_, h)| h.clone())
    }

    pub async fn get_project_for_thread(&self, thread_id: ThreadId) -> Option<ProjectId> {
        let threads = self.threads.lock().await;
        threads.get(&thread_id).map(|(p, _)| *p)
    }
}

async fn forward_events(
    thread_id: ThreadId,
    mut stream: giskard_harness::AgentEventStream,
    hub: Arc<Hub>,
    live_buffers: Arc<LiveBufferStore>,
) {
    use giskard_core::event::AgentEvent;

    loop {
        match stream.recv().await {
            Ok(event) => {
                let is_turn_start = matches!(event, AgentEvent::TurnStarted { .. });
                let is_turn_end = matches!(event, AgentEvent::TurnCompleted { .. });

                if is_turn_start {
                    live_buffers.start_turn(thread_id).await;
                }

                if live_buffers.is_active(thread_id).await {
                    live_buffers.append(thread_id, event.clone()).await;
                }

                hub.broadcast_event(thread_id, event).await;

                if is_turn_end {
                    live_buffers.clear_turn(thread_id).await;
                }
            }
            Err(e) => {
                debug!(%thread_id, ?e, "event stream ended");
                break;
            }
        }
    }
}
