use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use giskard_core::error::HarnessError;
use giskard_core::thread::ThreadKind;
use giskard_core::turn::{PermissionPreset, TurnMode, TurnModel};
use giskard_harness::{AgentHarness, ThreadDiscovered, ThreadHandle};
use giskard_persist::store::ThreadFile;
use tracing::{debug, warn};

use super::driver::Link;
use super::{
    ClassificationPhase, ExistingLinkDisposition, LoadedThreadBinding, RegistryShared,
    classify_existing_link, display_opt, effective_thread_workspace_root, load_thread_graph,
    parent_chain_is_valid, should_refresh_subagent_title, subagent_info_with_agent_name,
    subagent_thread_title,
};

pub(super) enum Admission {
    Discovered(ThreadDiscovered),
    Link(Box<Link>),
}

pub(super) struct Admitted {
    pub(super) binding: LoadedThreadBinding,
    pub(super) classification: ClassificationPhase,
    pub(super) thread_id: giskard_core::ids::ThreadId,
}

fn admitted(
    project_id: giskard_core::ids::ProjectId,
    handle: ThreadHandle,
    file: &ThreadFile,
) -> Admitted {
    Admitted {
        binding: LoadedThreadBinding {
            project_id,
            native_model: handle
                .resumed_model
                .clone()
                .or_else(|| file.current_model.as_known().cloned()),
            handle,
        },
        classification: ClassificationPhase::from(file.kind),
        thread_id: file.id,
    }
}

fn protocol(error: impl std::fmt::Display) -> HarnessError {
    HarnessError::Protocol(error.to_string())
}

fn orphan_file(
    project_id: giskard_core::ids::ProjectId,
    thread_id: giskard_core::ids::ThreadId,
    harness_thread_id: String,
    current_model: TurnModel,
) -> ThreadFile {
    let now = Utc::now();
    ThreadFile {
        revision: 0,
        version: giskard_persist::store::THREAD_METADATA_VERSION,
        id: thread_id,
        project_id,
        title: "Unclassified native thread".into(),
        harness_thread_id,
        parent_thread_id: None,
        spawned_by_turn_id: None,
        kind: ThreadKind::Orphan,
        mode: TurnMode::Unknown,
        current_model,
        context_window: 0,
        model_context_windows: HashMap::new(),
        permission_preset: PermissionPreset::AskFirst,
        model_efforts: HashMap::new(),
        tokens: Default::default(),
        created_at: now,
        updated_at: now,
        archived: false,
        git_workspace: None,
    }
}

pub(super) async fn admit(
    shared: Arc<RegistryShared>,
    harness: Arc<dyn AgentHarness>,
    project_id: giskard_core::ids::ProjectId,
    source: Admission,
) -> Result<Option<Admitted>, HarnessError> {
    let project = shared
        .store
        .load_project(project_id)
        .await
        .map_err(protocol)?
        .ok_or_else(|| match &source {
            Admission::Discovered(_) => HarnessError::Protocol(
                "project disappeared before a discovered native thread could be admitted".into(),
            ),
            Admission::Link(_) => HarnessError::Protocol(format!(
                "project {project_id} disappeared while importing sub-agent"
            )),
        })?;

    let (handle, link) = match source {
        Admission::Discovered(record) => {
            let provisional = orphan_file(
                project_id,
                record.thread,
                record.harness_thread_id.clone(),
                TurnModel::Unknown,
            );
            let root = effective_thread_workspace_root(&shared.store, &project, &provisional)
                .await
                .map_err(protocol)?;
            let handle = ThreadHandle {
                parent_harness_thread_id: record.parent_harness_thread_id,
                ..ThreadHandle::opened(record.thread, record.harness_thread_id, root.into())
            };
            (handle, None)
        }
        Admission::Link(link) => {
            let parent = shared
                .store
                .load_thread(project_id, link.parent_thread_id)
                .await
                .map_err(protocol)?
                .ok_or(HarnessError::ThreadNotFound(link.parent_thread_id))?;
            let root = effective_thread_workspace_root(&shared.store, &project, &parent)
                .await
                .map_err(protocol)?;
            let handle = harness
                .claim_native_thread(
                    giskard_core::ids::ThreadId::new(),
                    link.info.native_thread_id.clone(),
                    root.into(),
                )
                .await?;
            if handle.harness_thread_id != link.info.native_thread_id {
                return Err(HarnessError::Protocol(format!(
                    "linked-thread claim returned native thread {} instead of {}",
                    handle.harness_thread_id, link.info.native_thread_id
                )));
            }
            (handle, Some((link, parent)))
        }
    };

    let mut file = match shared
        .store
        .load_thread(project_id, handle.thread)
        .await
        .map_err(protocol)?
    {
        Some(file) => file,
        None => {
            let current_model = handle
                .resumed_model
                .clone()
                .map(TurnModel::Known)
                .unwrap_or(TurnModel::Unknown);
            let mut file = orphan_file(
                project_id,
                handle.thread,
                handle.harness_thread_id.clone(),
                current_model,
            );
            if let Some((link, parent)) = link.as_ref() {
                let graph = load_thread_graph(&shared.store, project_id)
                    .await
                    .map_err(protocol)?;
                if !parent_chain_is_valid(&graph, parent.id) {
                    warn!(%project_id, parent_thread_id = %parent.id,
                        linked_harness_thread_id = %handle.harness_thread_id,
                        "refusing to materialize a sub-agent under an invalid parent chain");
                } else if let Some(native_parent) = handle.parent_harness_thread_id.as_deref()
                    && native_parent != parent.harness_thread_id
                {
                    warn!(%project_id, parent_thread_id = %parent.id,
                        proposed_parent_harness_thread_id = %parent.harness_thread_id,
                        reported_parent_harness_thread_id = %native_parent,
                        linked_harness_thread_id = %handle.harness_thread_id,
                        "refusing to materialize a native thread under a mismatched parent");
                } else {
                    file.title = subagent_thread_title(&subagent_info_with_agent_name(
                        link.info.clone(),
                        handle.agent_name.clone(),
                    ));
                    file.parent_thread_id = Some(parent.id);
                    file.spawned_by_turn_id = Some(link.spawned_by_turn_id);
                    file.kind = ThreadKind::Subagent;
                    file.mode = parent.mode;
                    file.permission_preset = parent.permission_preset;
                }
            }
            let file = shared
                .thread_metadata
                .create(project_id, file)
                .await
                .map_err(protocol)?;
            if file.kind == ThreadKind::Subagent {
                shared
                    .thread_metadata
                    .publish_created(project_id, &file)
                    .await;
            }
            return Ok(Some(admitted(project_id, handle, &file)));
        }
    };

    if let Some((link, parent)) = link {
        let needs_graph = parent.parent_thread_id == Some(file.id)
            || (file.kind != ThreadKind::Primary
                && (file.kind == ThreadKind::Orphan || file.parent_thread_id != Some(parent.id)));
        let graph = if needs_graph {
            Some(
                load_thread_graph(&shared.store, project_id)
                    .await
                    .map_err(protocol)?,
            )
        } else {
            None
        };
        let disposition = match graph.as_ref() {
            Some(graph) => classify_existing_link(graph, parent.id, &file),
            None if file.kind == ThreadKind::Primary => ExistingLinkDisposition::PrimaryThread,
            None => ExistingLinkDisposition::OwnedChild,
        };
        if disposition == ExistingLinkDisposition::Parent {
            debug!(%project_id, source_thread_id = %parent.id, parent_thread_id = %file.id,
                linked_harness_thread_id = %handle.harness_thread_id,
                "recognized reverse sub-agent activity targeting the existing parent");
            return Ok(None);
        }
        if disposition != ExistingLinkDisposition::OwnedChild {
            warn!(%project_id, parent_thread_id = %parent.id, existing_thread_id = %file.id,
                existing_kind = ?file.kind,
                existing_parent_thread_id = display_opt(file.parent_thread_id),
                linked_harness_thread_id = %handle.harness_thread_id,
                disposition = ?disposition, reason = disposition.reason(),
                "ignoring sub-agent materialization for an existing thread with incompatible ownership");
            return Ok(None);
        }

        if file.kind == ThreadKind::Orphan {
            let graph = graph.as_ref().ok_or_else(|| {
                HarnessError::Protocol("orphan classification requires a thread graph".into())
            })?;
            if !parent_chain_is_valid(graph, parent.id) {
                warn!(%project_id, parent_thread_id = %parent.id,
                    linked_harness_thread_id = %handle.harness_thread_id,
                    "refusing to materialize a sub-agent under an invalid parent chain");
                return Ok(Some(admitted(project_id, handle, &file)));
            }
            if let Some(native_parent) = handle.parent_harness_thread_id.as_deref()
                && native_parent != parent.harness_thread_id
            {
                warn!(%project_id, parent_thread_id = %parent.id,
                    proposed_parent_harness_thread_id = %parent.harness_thread_id,
                    reported_parent_harness_thread_id = %native_parent,
                    linked_harness_thread_id = %handle.harness_thread_id,
                    "refusing to materialize a native thread under a mismatched parent");
                return Ok(Some(admitted(project_id, handle, &file)));
            }
            let desired_title = subagent_thread_title(&subagent_info_with_agent_name(
                link.info.clone(),
                handle.agent_name.clone(),
            ));
            let mutation = shared
                .thread_metadata
                .classify_orphan(
                    project_id,
                    file.id,
                    file.revision,
                    crate::thread_metadata::OrphanClassification {
                        parent_thread_id: parent.id,
                        spawned_by_turn_id: link.spawned_by_turn_id,
                        title: desired_title,
                        mode: parent.mode,
                        permission_preset: parent.permission_preset,
                    },
                )
                .await
                .map_err(protocol)?;
            file = mutation.into_current().ok_or_else(|| {
                HarnessError::Protocol(format!(
                    "orphan thread {} disappeared during classification",
                    file.id
                ))
            })?;
            if file.kind != ThreadKind::Subagent
                || file.parent_thread_id != Some(parent.id)
                || file.spawned_by_turn_id != Some(link.spawned_by_turn_id)
            {
                return Err(HarnessError::Protocol(format!(
                    "orphan thread {} was classified concurrently with conflicting ownership",
                    file.id
                )));
            }
            if let Some(coordinator) = shared.coordinator(file.id).await {
                coordinator.classify_orphan_as_subagent().await?;
            }
            shared
                .thread_metadata
                .publish_created(project_id, &file)
                .await;
        }

        let desired_title = subagent_thread_title(&subagent_info_with_agent_name(
            link.info,
            handle.agent_name.clone(),
        ));
        if should_refresh_subagent_title(&file.title, &desired_title) {
            shared
                .thread_metadata
                .mutate(project_id, file.id, |current| {
                    if should_refresh_subagent_title(&current.title, &desired_title) {
                        current.title = desired_title.clone();
                    }
                })
                .await
                .map_err(protocol)?;
        }
    } else if file.kind == ThreadKind::Primary {
        warn!(%project_id, thread_id = %file.id, harness_thread_id = %file.harness_thread_id,
            "ignoring traffic discovery for an already persisted primary thread");
        return Ok(None);
    }

    Ok(Some(admitted(project_id, handle, &file)))
}
