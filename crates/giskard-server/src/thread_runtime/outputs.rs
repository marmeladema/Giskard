//! The live outputs of the completed items of a thread's in-flight turns.
//!
//! Command and tool output are addressable while their turn runs and are dropped when it settles.
//! Projecting an output is expensive enough to be worth doing off the lock, so an item may arrive
//! either as a raw `Item` or as a `PreparedItemOutput` computed ahead of time.

use std::collections::HashMap;

use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemPayload, command_status_is_running};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeCommandOutput {
    pub output: String,
    pub output_truncated: bool,
    pub original_bytes: u64,
    pub original_lines: u64,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeToolOutput {
    pub bytes: Vec<u8>,
    pub descriptor: giskard_proto::WireToolOutput,
}

/// The live, authoritative outputs of one completed item while its turn is in flight.
#[derive(Default)]
struct ItemOutputs {
    command: Option<RuntimeCommandOutput>,
    tool: Option<RuntimeToolOutput>,
}

impl ItemOutputs {
    fn is_empty(&self) -> bool {
        self.command.is_none() && self.tool.is_none()
    }
}

pub struct PreparedItemOutput {
    turn_id: TurnId,
    item_id: ItemId,
    command_runtime: Option<RuntimeCommandOutput>,
    pub(crate) command_descriptor: Option<giskard_core::CommandOutputDescriptor>,
    tool_runtime: Option<RuntimeToolOutput>,
    pub(crate) tool_descriptor: Option<giskard_proto::WireToolOutput>,
    command_item: bool,
    pub(crate) live_event: Option<AgentEvent>,
}

pub enum RuntimeCommandOutputLookup {
    Found(RuntimeCommandOutput),
    Missing,
}

pub enum RuntimeToolOutputLookup {
    Found(RuntimeToolOutput),
    Missing,
}

#[derive(Default)]
pub struct ItemOutputState {
    items: HashMap<(TurnId, ItemId), ItemOutputs>,
}

impl ItemOutputState {
    pub fn set_command(&mut self, key: (TurnId, ItemId), output: Option<RuntimeCommandOutput>) {
        let outputs = self.items.entry(key).or_default();
        outputs.command = output;
        if outputs.is_empty() {
            self.items.remove(&key);
        }
    }

    pub fn set_tool(&mut self, key: (TurnId, ItemId), output: Option<RuntimeToolOutput>) {
        let outputs = self.items.entry(key).or_default();
        outputs.tool = output;
        if outputs.is_empty() {
            self.items.remove(&key);
        }
    }

    pub fn command_output(&self, turn_id: TurnId, item_id: ItemId) -> RuntimeCommandOutputLookup {
        self.items
            .get(&(turn_id, item_id))
            .and_then(|outputs| outputs.command.clone())
            .map_or(
                RuntimeCommandOutputLookup::Missing,
                RuntimeCommandOutputLookup::Found,
            )
    }

    pub fn tool_output(&self, turn_id: TurnId, item_id: ItemId) -> RuntimeToolOutputLookup {
        self.items
            .get(&(turn_id, item_id))
            .and_then(|outputs| outputs.tool.clone())
            .map_or(
                RuntimeToolOutputLookup::Missing,
                RuntimeToolOutputLookup::Found,
            )
    }

    /// Applies output projected outside the lock by `prepare_item_output`.
    pub fn apply_prepared(&mut self, prepared: PreparedItemOutput) {
        let key = (prepared.turn_id, prepared.item_id);
        if prepared.command_runtime.is_none()
            && prepared.command_item
            && prepared.command_descriptor.is_none()
        {
            tracing::error!(
                turn_id = %prepared.turn_id,
                item_id = %prepared.item_id,
                "completed command output has inconsistent truncation metadata"
            );
        }
        self.set_command(key, prepared.command_runtime);
        self.set_tool(key, prepared.tool_runtime);
    }

    /// Applies a completed item that was not prepared ahead of the lock. `project_id` is the
    /// turn owner's project, read by the caller for the serialization-failure log.
    pub fn apply_completed_item(
        &mut self,
        thread_id: ThreadId,
        project_id: Option<ProjectId>,
        turn_id: TurnId,
        item: &Item,
    ) {
        self.update_command_output_authority(turn_id, item);
        self.update_tool_output_authority(thread_id, project_id, turn_id, item);
    }

    fn update_command_output_authority(&mut self, turn_id: TurnId, item: &Item) {
        let ItemPayload::CommandExecution {
            output,
            output_truncated,
            output_original_bytes,
            output_original_lines,
            status,
            ..
        } = &item.payload
        else {
            self.set_command((turn_id, item.id), None);
            return;
        };
        if status.as_deref().is_some_and(command_status_is_running) {
            self.set_command((turn_id, item.id), None);
            return;
        }
        let Ok(descriptor) = giskard_persist::command_output_descriptor(
            output,
            *output_truncated,
            *output_original_bytes,
            *output_original_lines,
            true,
        ) else {
            tracing::error!(
                %turn_id,
                item_id = %item.id,
                "completed command output has inconsistent truncation metadata"
            );
            self.set_command((turn_id, item.id), None);
            return;
        };
        self.set_command(
            (turn_id, item.id),
            Some(RuntimeCommandOutput {
                output: output.clone(),
                output_truncated: *output_truncated,
                original_bytes: descriptor.original_bytes,
                original_lines: descriptor.original_lines,
                version: command_output_version(output),
            }),
        );
    }

    fn update_tool_output_authority(
        &mut self,
        thread_id: ThreadId,
        project_id: Option<ProjectId>,
        turn_id: TurnId,
        item: &Item,
    ) {
        let key = (turn_id, item.id);
        let ItemPayload::ToolCall { output, status, .. } = &item.payload else {
            self.set_tool(key, None);
            return;
        };
        if status
            .as_deref()
            .is_some_and(giskard_core::item::tool_status_is_running)
        {
            self.set_tool(key, None);
            return;
        }
        let Some(output) = output else {
            self.set_tool(key, None);
            return;
        };
        match giskard_core::item::serialize_tool_output(output) {
            Ok((bytes, descriptor)) => {
                self.set_tool(key, Some(RuntimeToolOutput { bytes, descriptor }));
            }
            Err(error) => {
                let project_id = project_id.map(tracing::field::display);
                tracing::error!(
                    project_id,
                    %thread_id,
                    %turn_id,
                    item_id = %item.id,
                    action = "serialize_completed_tool_output",
                    %error,
                    "could not serialize completed tool output"
                );
                self.set_tool(key, None);
            }
        }
    }

    /// Drops every output held for a settled turn.
    pub fn clear_turn(&mut self, turn_id: TurnId) {
        self.items.retain(|(turn, _), _| *turn != turn_id);
    }

    #[cfg(test)]
    pub fn contains(&self, turn_id: TurnId, item_id: ItemId) -> bool {
        self.items.contains_key(&(turn_id, item_id))
    }
}

pub fn prepare_item_output(event: &AgentEvent) -> Option<PreparedItemOutput> {
    let AgentEvent::ItemCompleted { turn, item, .. } = event else {
        return None;
    };
    if let ItemPayload::ToolCall { output, status, .. } = &item.payload {
        let terminal = !status
            .as_deref()
            .is_some_and(giskard_core::item::tool_status_is_running);
        let prepared = output
            .as_ref()
            .filter(|_| terminal)
            .and_then(|output| giskard_core::item::serialize_tool_output(output).ok());
        let (tool_runtime, tool_descriptor) =
            prepared.map_or((None, None), |(bytes, descriptor)| {
                (
                    Some(RuntimeToolOutput {
                        bytes,
                        descriptor: descriptor.clone(),
                    }),
                    Some(descriptor),
                )
            });
        let mut live_event = event.clone();
        if let AgentEvent::ItemCompleted { item, .. } = &mut live_event
            && let ItemPayload::ToolCall { output, .. } = &mut item.payload
        {
            *output = None;
        }
        return Some(PreparedItemOutput {
            turn_id: *turn,
            item_id: item.id,
            command_runtime: None,
            command_descriptor: None,
            tool_runtime,
            tool_descriptor,
            command_item: false,
            live_event: Some(live_event),
        });
    }
    let ItemPayload::CommandExecution {
        output,
        output_truncated,
        output_original_bytes,
        output_original_lines,
        status,
        ..
    } = &item.payload
    else {
        return None;
    };
    let descriptor = giskard_persist::command_output_descriptor(
        output,
        *output_truncated,
        *output_original_bytes,
        *output_original_lines,
        true,
    );
    let (runtime, descriptor) = match descriptor {
        Ok(descriptor) => {
            let runtime = (!status.as_deref().is_some_and(command_status_is_running)).then(|| {
                RuntimeCommandOutput {
                    output: output.clone(),
                    output_truncated: *output_truncated,
                    original_bytes: descriptor.original_bytes,
                    original_lines: descriptor.original_lines,
                    version: command_output_version(output),
                }
            });
            (runtime, Some(descriptor))
        }
        Err(_) => (None, None),
    };
    let mut live_event = event.clone();
    if let (Some(descriptor), AgentEvent::ItemCompleted { item, .. }) =
        (&descriptor, &mut live_event)
        && let ItemPayload::CommandExecution { output, .. } = &mut item.payload
    {
        *output = descriptor.preview.clone();
    }
    Some(PreparedItemOutput {
        turn_id: *turn,
        item_id: item.id,
        command_runtime: runtime,
        command_descriptor: descriptor,
        tool_runtime: None,
        tool_descriptor: None,
        command_item: true,
        live_event: Some(live_event),
    })
}

pub fn command_output_version(output: &str) -> String {
    format!("\"sha256_{:x}\"", Sha256::digest(output.as_bytes()))
}
