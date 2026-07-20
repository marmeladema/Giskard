use std::collections::HashMap;
use std::path::Path;

use chrono::Utc;
use tokio::sync::Mutex;

use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ThreadId, TurnId};
use giskard_core::item::{ItemDelta, ItemPayload, command_status_is_running};
use giskard_proto::{RunningTask, TaskKind};

const MAX_OUTPUT_TAIL: usize = 8_000;

type TaskKey = (TurnId, ItemId);

#[derive(Default)]
pub struct RunningTaskStore {
    tasks: Mutex<HashMap<ThreadId, HashMap<TaskKey, RunningTask>>>,
}
impl RunningTaskStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn apply_event(&self, event: &AgentEvent) -> bool {
        match event {
            AgentEvent::ItemStarted { thread, turn, item } => {
                let task = if let Some(command) = &item.command {
                    let status = command
                        .status
                        .clone()
                        .unwrap_or_else(|| "in_progress".into());
                    if !command_status_is_running(&status) {
                        return false;
                    }
                    RunningTask {
                        kind: TaskKind::Command,
                        thread_id: *thread,
                        turn_id: *turn,
                        item_id: item.id,
                        harness_item_id: item.harness_item_id.clone(),
                        command: command.command.clone(),
                        cwd: command.cwd.clone(),
                        server: None,
                        status,
                        process_id: command.process_id.clone(),
                        started_at_ms: command.started_at_ms.unwrap_or_else(now_ms),
                        output: String::new(),
                        after_turn: false,
                        terminating: false,
                    }
                } else if let Some(tool) = &item.tool {
                    let status = tool.status.clone().unwrap_or_else(|| "in_progress".into());
                    if !command_status_is_running(&status) {
                        return false;
                    }
                    RunningTask {
                        kind: TaskKind::Tool,
                        thread_id: *thread,
                        turn_id: *turn,
                        item_id: item.id,
                        harness_item_id: item.harness_item_id.clone(),
                        command: tool.name.clone(),
                        cwd: String::new(),
                        server: tool.server.clone(),
                        status,
                        // Tool calls have no OS process; a stop request interrupts the owning turn.
                        process_id: None,
                        started_at_ms: tool.started_at_ms.unwrap_or_else(now_ms),
                        output: String::new(),
                        after_turn: false,
                        terminating: false,
                    }
                } else {
                    return false;
                };
                let mut tasks = self.tasks.lock().await;
                tasks
                    .entry(*thread)
                    .or_default()
                    .insert((*turn, item.id), task);
                true
            }
            // Command output arrives as `CommandOutput`, tool progress as `Text`. Either appends to
            // the tracked task for its item id (untracked item ids — e.g. agent text — are ignored).
            AgentEvent::ItemDelta {
                thread,
                turn,
                item_id,
                delta: ItemDelta::CommandOutput { chunk } | ItemDelta::Text { text: chunk },
                ..
            } => {
                let mut tasks = self.tasks.lock().await;
                let Some(task) = tasks
                    .get_mut(thread)
                    .and_then(|thread_tasks| thread_tasks.get_mut(&(*turn, *item_id)))
                else {
                    return false;
                };
                task.output.push_str(chunk);
                truncate_output_tail(&mut task.output);
                true
            }
            AgentEvent::ItemCompleted { thread, turn, item } => {
                let completed = match &item.payload {
                    ItemPayload::CommandExecution {
                        command,
                        cwd,
                        output,
                        status,
                        process_id,
                        ..
                    } => CompletedTask {
                        kind: TaskKind::Command,
                        command: command.clone(),
                        cwd: path_to_display(cwd),
                        server: None,
                        output: output.clone(),
                        status: status.clone(),
                        process_id: process_id.clone(),
                    },
                    ItemPayload::ToolCall {
                        name,
                        output,
                        server,
                        status,
                        error,
                        ..
                    } => CompletedTask {
                        kind: TaskKind::Tool,
                        command: name.clone(),
                        cwd: String::new(),
                        server: server.clone(),
                        output: tool_output_string(output.as_ref(), error.as_deref()),
                        status: status.clone(),
                        process_id: None,
                    },
                    _ => return false,
                };

                let mut tasks = self.tasks.lock().await;
                let thread_tasks = tasks.entry(*thread).or_default();
                let key = (*turn, item.id);
                let Some(status) = completed.status.as_deref() else {
                    return thread_tasks.remove(&key).is_some();
                };

                if !command_status_is_running(status) {
                    return thread_tasks.remove(&key).is_some();
                }

                let mut output = completed.output;
                truncate_output_tail(&mut output);
                let after_turn = thread_tasks
                    .get(&key)
                    .map(|task| task.after_turn)
                    .unwrap_or(false);
                let started_at_ms = thread_tasks
                    .get(&key)
                    .map(|task| task.started_at_ms)
                    .unwrap_or_else(now_ms);
                let terminating = thread_tasks
                    .get(&key)
                    .map(|task| task.terminating)
                    .unwrap_or(false);
                thread_tasks.insert(
                    key,
                    RunningTask {
                        kind: completed.kind,
                        thread_id: *thread,
                        turn_id: *turn,
                        item_id: item.id,
                        harness_item_id: item.harness_item_id.clone(),
                        command: completed.command,
                        cwd: completed.cwd,
                        server: completed.server,
                        status: status.to_string(),
                        process_id: completed.process_id,
                        started_at_ms,
                        output,
                        after_turn,
                        terminating,
                    },
                );
                true
            }
            AgentEvent::TurnCompleted { thread, turn, .. } => {
                let mut tasks = self.tasks.lock().await;
                let Some(thread_tasks) = tasks.get_mut(thread) else {
                    return false;
                };
                let mut changed = false;
                // Tool calls do not outlive their turn (they are synchronous requests): drop any
                // still-running tool when the turn ends, e.g. an interrupted turn.
                thread_tasks.retain(|_, task| {
                    let drop = task.turn_id == *turn
                        && task.kind == TaskKind::Tool
                        && command_status_is_running(&task.status);
                    if drop {
                        changed = true;
                    }
                    !drop
                });
                // Commands can outlive an interrupted turn; keep them, marked `after_turn`.
                for task in thread_tasks.values_mut() {
                    if task.turn_id == *turn
                        && task.kind == TaskKind::Command
                        && command_status_is_running(&task.status)
                        && !task.after_turn
                    {
                        task.after_turn = true;
                        changed = true;
                    }
                }
                changed
            }
            _ => false,
        }
    }

    pub async fn set_terminating_by_process(
        &self,
        thread_id: ThreadId,
        process_id: &str,
        terminating: bool,
    ) -> bool {
        let mut commands = self.tasks.lock().await;
        let Some(thread_commands) = commands.get_mut(&thread_id) else {
            return false;
        };
        let mut changed = false;
        for cmd in thread_commands.values_mut() {
            if cmd.process_id.as_deref() == Some(process_id) && cmd.terminating != terminating {
                cmd.terminating = terminating;
                changed = true;
            }
        }
        changed
    }

    pub async fn get_by_process(
        &self,
        thread_id: ThreadId,
        process_id: &str,
    ) -> Option<RunningTask> {
        let commands = self.tasks.lock().await;
        commands.get(&thread_id).and_then(|thread_commands| {
            thread_commands
                .values()
                .find(|cmd| cmd.process_id.as_deref() == Some(process_id))
                .cloned()
        })
    }

    pub async fn get_by_item(
        &self,
        thread_id: ThreadId,
        turn_id: TurnId,
        item_id: ItemId,
    ) -> Option<RunningTask> {
        let commands = self.tasks.lock().await;
        commands
            .get(&thread_id)
            .and_then(|thread_commands| thread_commands.get(&(turn_id, item_id)))
            .cloned()
    }

    pub async fn remove_by_process(&self, thread_id: ThreadId, process_id: &str) -> bool {
        let mut commands = self.tasks.lock().await;
        let Some(thread_commands) = commands.get_mut(&thread_id) else {
            return false;
        };
        let before = thread_commands.len();
        thread_commands.retain(|_, cmd| cmd.process_id.as_deref() != Some(process_id));
        let changed = thread_commands.len() != before;
        if thread_commands.is_empty() {
            commands.remove(&thread_id);
        }
        changed
    }

    pub async fn snapshot(&self, thread_id: ThreadId) -> Vec<RunningTask> {
        let commands = self.tasks.lock().await;
        let mut snapshot = commands
            .get(&thread_id)
            .map(|thread_commands| thread_commands.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        snapshot.sort_by_key(|cmd| (cmd.turn_id.to_string(), cmd.item_id.to_string()));
        snapshot
    }

    pub async fn has_running_for_turn(
        &self,
        thread_id: ThreadId,
        turn_id: giskard_core::ids::TurnId,
    ) -> bool {
        let commands = self.tasks.lock().await;
        commands
            .get(&thread_id)
            .map(|thread_commands| {
                thread_commands
                    .values()
                    .any(|cmd| cmd.turn_id == turn_id && command_status_is_running(&cmd.status))
            })
            .unwrap_or(false)
    }

    pub async fn has_running_for_thread(&self, thread_id: ThreadId) -> bool {
        let commands = self.tasks.lock().await;
        commands
            .get(&thread_id)
            .map(|thread_commands| {
                thread_commands
                    .values()
                    .any(|cmd| command_status_is_running(&cmd.status))
            })
            .unwrap_or(false)
    }
}

/// Normalized fields extracted from a completed command or tool item before the shared
/// running-vs-terminal bookkeeping.
struct CompletedTask {
    kind: TaskKind,
    command: String,
    cwd: String,
    server: Option<String>,
    output: String,
    status: Option<String>,
    process_id: Option<String>,
}

/// Render a tool call's completion output for the right-panel tail: prefer the structured result,
/// fall back to the error string.
fn tool_output_string(output: Option<&serde_json::Value>, error: Option<&str>) -> String {
    if let Some(value) = output {
        match value {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        }
    } else if let Some(error) = error {
        error.to_string()
    } else {
        String::new()
    }
}

fn path_to_display(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn truncate_output_tail(output: &mut String) {
    if output.len() <= MAX_OUTPUT_TAIL {
        return;
    }
    let mut cutoff = output.len() - MAX_OUTPUT_TAIL;
    while !output.is_char_boundary(cutoff) {
        cutoff += 1;
    }
    output.drain(..cutoff);
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use giskard_core::ids::{ItemId, ThreadId, TurnId};
    use giskard_core::item::{CommandExecutionStart, Item, ItemKind, ItemStart};
    use giskard_core::token::TokenUsage;
    use giskard_core::turn::{TurnStatus, TurnStatusKind};

    use super::*;

    fn command_start(
        thread: ThreadId,
        turn: TurnId,
        item_id: ItemId,
        process_id: &str,
        status: &str,
    ) -> AgentEvent {
        AgentEvent::ItemStarted {
            thread,
            turn,
            item: ItemStart {
                id: item_id,
                harness_item_id: format!("harness_{process_id}"),
                kind: ItemKind::CommandExecution,
                command: Some(CommandExecutionStart {
                    command: format!("sleep {process_id}"),
                    cwd: "/tmp/project".into(),
                    status: Some(status.into()),
                    process_id: Some(process_id.into()),
                    started_at_ms: Some(1_785_000_000_000),
                }),
                tool: None,
            },
        }
    }

    fn command_completed(
        thread: ThreadId,
        turn: TurnId,
        item_id: ItemId,
        process_id: &str,
        status: Option<&str>,
    ) -> AgentEvent {
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: item_id,
                harness_item_id: format!("harness_{process_id}"),
                payload: ItemPayload::CommandExecution {
                    command: format!("sleep {process_id}"),
                    cwd: "/tmp/project".into(),
                    output: "finished".into(),
                    exit_code: Some(1),
                    status: status.map(Into::into),
                    process_id: Some(process_id.into()),
                    duration_ms: Some(1_000),
                },
                created_at: Utc::now(),
            },
        }
    }

    fn tool_start(thread: ThreadId, turn: TurnId, item_id: ItemId, name: &str) -> AgentEvent {
        use giskard_core::item::ToolCallStart;
        AgentEvent::ItemStarted {
            thread,
            turn,
            item: ItemStart {
                id: item_id,
                harness_item_id: format!("tool_{name}"),
                kind: ItemKind::ToolCall,
                command: None,
                tool: Some(ToolCallStart {
                    name: name.into(),
                    input: serde_json::json!({ "q": name }),
                    server: Some("wiki".into()),
                    status: Some("in_progress".into()),
                    started_at_ms: Some(1_785_000_000_000),
                }),
            },
        }
    }

    fn tool_completed(
        thread: ThreadId,
        turn: TurnId,
        item_id: ItemId,
        name: &str,
        status: Option<&str>,
    ) -> AgentEvent {
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: item_id,
                harness_item_id: format!("tool_{name}"),
                payload: ItemPayload::ToolCall {
                    name: name.into(),
                    input: serde_json::json!({ "q": name }),
                    output: Some(serde_json::json!("a big result")),
                    server: Some("wiki".into()),
                    status: status.map(Into::into),
                    error: None,
                },
                created_at: Utc::now(),
            },
        }
    }

    #[tokio::test]
    async fn tool_call_is_tracked_like_a_command_and_removed_on_completion() {
        let store = RunningTaskStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();

        assert!(
            store
                .apply_event(&tool_start(thread, turn, item_id, "search"))
                .await
        );
        let snapshot = store.snapshot(thread).await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].kind, TaskKind::Tool);
        assert_eq!(snapshot[0].command, "search");
        assert_eq!(snapshot[0].server.as_deref(), Some("wiki"));
        assert_eq!(snapshot[0].process_id, None);
        assert_eq!(snapshot[0].started_at_ms, 1_785_000_000_000);

        // Tool progress arrives as a Text delta.
        assert!(
            store
                .apply_event(&AgentEvent::ItemDelta {
                    thread,
                    turn,
                    item_id,
                    delta: ItemDelta::Text {
                        text: "searching…".into(),
                    },
                })
                .await
        );
        assert_eq!(store.snapshot(thread).await[0].output, "searching…");

        // A terminal completion removes it from the running set.
        assert!(
            store
                .apply_event(&tool_completed(
                    thread,
                    turn,
                    item_id,
                    "search",
                    Some("completed")
                ))
                .await
        );
        assert!(store.snapshot(thread).await.is_empty());
    }

    #[tokio::test]
    async fn running_tool_is_dropped_when_its_turn_ends() {
        let store = RunningTaskStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let tool_item = ItemId::new();
        let cmd_item = ItemId::new();

        store
            .apply_event(&tool_start(thread, turn, tool_item, "search"))
            .await;
        store
            .apply_event(&command_start(
                thread,
                turn,
                cmd_item,
                "proc_1",
                "in_progress",
            ))
            .await;

        // Interrupting the turn: the tool is abandoned (dropped), the command outlives it.
        assert!(
            store
                .apply_event(&AgentEvent::TurnCompleted {
                    thread,
                    turn,
                    usage: TokenUsage::default(),
                    status: TurnStatus {
                        kind: TurnStatusKind::Interrupted,
                        message: None,
                    },
                })
                .await
        );
        let snapshot = store.snapshot(thread).await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].kind, TaskKind::Command);
        assert!(snapshot[0].after_turn);
    }

    #[tokio::test]
    async fn running_command_snapshot_tracks_output_and_after_turn() {
        let store = RunningTaskStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();

        assert!(
            store
                .apply_event(&AgentEvent::ItemStarted {
                    thread,
                    turn,
                    item: ItemStart {
                        id: item_id,
                        harness_item_id: "cmd1".into(),
                        kind: ItemKind::CommandExecution,
                        command: Some(CommandExecutionStart {
                            command: "sleep 60".into(),
                            cwd: "/tmp/project".into(),
                            status: Some("in_progress".into()),
                            process_id: Some("proc_1".into()),
                            started_at_ms: Some(1_785_000_000_000),
                        }),
                        tool: None,
                    },
                })
                .await
        );

        assert!(
            store
                .apply_event(&AgentEvent::ItemDelta {
                    thread,
                    turn,
                    item_id,
                    delta: ItemDelta::CommandOutput {
                        chunk: "started".into(),
                    },
                })
                .await
        );
        assert!(
            store
                .apply_event(&AgentEvent::TurnCompleted {
                    thread,
                    turn,
                    usage: TokenUsage::default(),
                    status: TurnStatus {
                        kind: TurnStatusKind::Interrupted,
                        message: Some("interrupted".into()),
                    },
                })
                .await
        );

        let snapshot = store.snapshot(thread).await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].command, "sleep 60");
        assert_eq!(snapshot[0].output, "started");
        assert_eq!(snapshot[0].started_at_ms, 1_785_000_000_000);
        assert!(snapshot[0].after_turn);
    }

    #[tokio::test]
    async fn unknown_command_output_is_ignored() {
        let store = RunningTaskStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();

        assert!(
            !store
                .apply_event(&AgentEvent::ItemDelta {
                    thread,
                    turn,
                    item_id,
                    delta: ItemDelta::CommandOutput {
                        chunk: "orphan output".into(),
                    },
                })
                .await
        );
        assert!(store.snapshot(thread).await.is_empty());
    }

    #[tokio::test]
    async fn terminal_item_start_is_not_tracked() {
        let store = RunningTaskStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();

        assert!(
            !store
                .apply_event(&command_start(
                    thread,
                    turn,
                    ItemId::new(),
                    "proc_1",
                    "completed"
                ))
                .await
        );
        assert!(store.snapshot(thread).await.is_empty());
    }

    #[tokio::test]
    async fn in_progress_completed_item_stays_visible() {
        let store = RunningTaskStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();

        assert!(
            store
                .apply_event(&AgentEvent::ItemCompleted {
                    thread,
                    turn,
                    item: Item {
                        id: item_id,
                        harness_item_id: "cmd1".into(),
                        payload: ItemPayload::CommandExecution {
                            command: "sleep 60".into(),
                            cwd: "/tmp/project".into(),
                            output: "still sleeping".into(),
                            exit_code: None,
                            status: Some("in_progress".into()),
                            process_id: Some("proc_1".into()),
                            duration_ms: None,
                        },
                        created_at: Utc::now(),
                    },
                })
                .await
        );

        let snapshot = store.snapshot(thread).await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].output, "still sleeping");
        assert_eq!(snapshot[0].process_id.as_deref(), Some("proc_1"));
    }

    #[tokio::test]
    async fn terminal_completed_item_removes_command() {
        let store = RunningTaskStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();

        store
            .apply_event(&AgentEvent::ItemStarted {
                thread,
                turn,
                item: ItemStart {
                    id: item_id,
                    harness_item_id: "cmd1".into(),
                    kind: ItemKind::CommandExecution,
                    command: Some(CommandExecutionStart {
                        command: "cargo test".into(),
                        cwd: "/tmp/project".into(),
                        status: Some("in_progress".into()),
                        process_id: Some("proc_1".into()),
                        started_at_ms: Some(1_785_000_000_123),
                    }),
                    tool: None,
                },
            })
            .await;

        assert!(
            store
                .apply_event(&AgentEvent::ItemCompleted {
                    thread,
                    turn,
                    item: Item {
                        id: item_id,
                        harness_item_id: "cmd1".into(),
                        payload: ItemPayload::CommandExecution {
                            command: "cargo test".into(),
                            cwd: "/tmp/project".into(),
                            output: "ok".into(),
                            exit_code: Some(0),
                            status: Some("completed".into()),
                            process_id: Some("proc_1".into()),
                            duration_ms: Some(1_250),
                        },
                        created_at: Utc::now(),
                    },
                })
                .await
        );

        assert!(store.snapshot(thread).await.is_empty());
    }

    #[tokio::test]
    async fn running_tasks_with_same_item_id_in_different_turns_are_tracked_separately() {
        let store = RunningTaskStore::new();
        let thread = ThreadId::new();
        let first_turn = TurnId::new();
        let second_turn = TurnId::new();
        let shared_item_id = ItemId::new();

        // First turn starts a long-running command with the shared item id.
        store
            .apply_event(&AgentEvent::ItemStarted {
                thread,
                turn: first_turn,
                item: ItemStart {
                    id: shared_item_id,
                    harness_item_id: "cmd".into(),
                    kind: ItemKind::CommandExecution,
                    command: Some(CommandExecutionStart {
                        command: "sleep 1".into(),
                        cwd: "/tmp".into(),
                        status: Some("in_progress".into()),
                        process_id: Some("proc_1".into()),
                        started_at_ms: Some(1),
                    }),
                    tool: None,
                },
            })
            .await;

        // Second turn starts another command with the same item id.
        store
            .apply_event(&AgentEvent::ItemStarted {
                thread,
                turn: second_turn,
                item: ItemStart {
                    id: shared_item_id,
                    harness_item_id: "cmd".into(),
                    kind: ItemKind::CommandExecution,
                    command: Some(CommandExecutionStart {
                        command: "sleep 2".into(),
                        cwd: "/tmp".into(),
                        status: Some("in_progress".into()),
                        process_id: Some("proc_2".into()),
                        started_at_ms: Some(2),
                    }),
                    tool: None,
                },
            })
            .await;

        let tasks = store.snapshot(thread).await;
        assert_eq!(
            tasks.len(),
            2,
            "same item id in different turns must create separate running tasks"
        );
        assert!(
            tasks
                .iter()
                .any(|t| t.turn_id == first_turn && t.process_id.as_deref() == Some("proc_1"))
        );
        assert!(
            tasks
                .iter()
                .any(|t| t.turn_id == second_turn && t.process_id.as_deref() == Some("proc_2"))
        );
    }

    #[tokio::test]
    async fn terminal_failed_and_declined_items_remove_commands() {
        for status in ["failed", "declined"] {
            let store = RunningTaskStore::new();
            let thread = ThreadId::new();
            let turn = TurnId::new();
            let item_id = ItemId::new();

            assert!(
                store
                    .apply_event(&command_start(
                        thread,
                        turn,
                        item_id,
                        "proc_1",
                        "in_progress"
                    ))
                    .await
            );
            assert!(
                store
                    .apply_event(&command_completed(
                        thread,
                        turn,
                        item_id,
                        "proc_1",
                        Some(status)
                    ))
                    .await
            );
            assert!(
                store.snapshot(thread).await.is_empty(),
                "{status} should remove the command"
            );
        }
    }

    #[tokio::test]
    async fn remove_by_process_clears_terminated_command() {
        let store = RunningTaskStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();

        store
            .apply_event(&AgentEvent::ItemStarted {
                thread,
                turn,
                item: ItemStart {
                    id: item_id,
                    harness_item_id: "cmd1".into(),
                    kind: ItemKind::CommandExecution,
                    command: Some(CommandExecutionStart {
                        command: "sleep 60".into(),
                        cwd: "/tmp/project".into(),
                        status: Some("in_progress".into()),
                        process_id: Some("proc_1".into()),
                        started_at_ms: Some(1_785_000_000_456),
                    }),
                    tool: None,
                },
            })
            .await;

        assert!(store.remove_by_process(thread, "proc_1").await);
        assert!(store.snapshot(thread).await.is_empty());
        assert!(!store.remove_by_process(thread, "proc_1").await);
    }

    #[tokio::test]
    async fn remove_by_process_preserves_unrelated_commands() {
        let store = RunningTaskStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let first = ItemId::new();
        let second = ItemId::new();

        store
            .apply_event(&command_start(thread, turn, first, "proc_1", "in_progress"))
            .await;
        store
            .apply_event(&command_start(
                thread,
                turn,
                second,
                "proc_2",
                "in_progress",
            ))
            .await;

        assert!(store.remove_by_process(thread, "proc_1").await);
        let snapshot = store.snapshot(thread).await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].process_id.as_deref(), Some("proc_2"));
    }

    #[tokio::test]
    async fn set_terminating_by_process_updates_matching_command() {
        let store = RunningTaskStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();

        store
            .apply_event(&command_start(
                thread,
                turn,
                item_id,
                "proc_1",
                "in_progress",
            ))
            .await;

        assert!(
            store
                .set_terminating_by_process(thread, "proc_1", true)
                .await
        );
        let snapshot = store.snapshot(thread).await;
        assert_eq!(snapshot.len(), 1);
        assert!(snapshot[0].terminating);

        assert!(
            !store
                .set_terminating_by_process(thread, "proc_1", true)
                .await
        );
        assert!(
            store
                .set_terminating_by_process(thread, "proc_1", false)
                .await
        );
        let snapshot = store.snapshot(thread).await;
        assert_eq!(snapshot.len(), 1);
        assert!(!snapshot[0].terminating);
    }

    #[tokio::test]
    async fn get_by_item_returns_matching_command() {
        let store = RunningTaskStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();

        store
            .apply_event(&command_start(
                thread,
                turn,
                item_id,
                "proc_1",
                "in_progress",
            ))
            .await;

        let command = store
            .get_by_item(thread, turn, item_id)
            .await
            .expect("running command should be indexed by item id");
        assert_eq!(command.process_id.as_deref(), Some("proc_1"));
        assert!(
            store
                .get_by_item(thread, TurnId::new(), ItemId::new())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn has_running_for_turn_only_matches_active_commands_for_that_turn() {
        let store = RunningTaskStore::new();
        let thread = ThreadId::new();
        let first_turn = TurnId::new();
        let second_turn = TurnId::new();
        let first = ItemId::new();
        let second = ItemId::new();

        store
            .apply_event(&command_start(
                thread,
                first_turn,
                first,
                "proc_1",
                "in_progress",
            ))
            .await;
        store
            .apply_event(&command_start(
                thread,
                second_turn,
                second,
                "proc_2",
                "in_progress",
            ))
            .await;

        assert!(store.has_running_for_turn(thread, first_turn).await);
        assert!(store.has_running_for_turn(thread, second_turn).await);

        store
            .apply_event(&command_completed(
                thread,
                first_turn,
                first,
                "proc_1",
                Some("completed"),
            ))
            .await;

        assert!(!store.has_running_for_turn(thread, first_turn).await);
        assert!(store.has_running_for_turn(thread, second_turn).await);
        assert!(
            !store
                .has_running_for_turn(ThreadId::new(), second_turn)
                .await
        );
    }

    #[tokio::test]
    async fn running_status_completion_preserves_terminating_flag() {
        let store = RunningTaskStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();

        store
            .apply_event(&command_start(
                thread,
                turn,
                item_id,
                "proc_1",
                "in_progress",
            ))
            .await;
        assert!(
            store
                .set_terminating_by_process(thread, "proc_1", true)
                .await
        );
        assert!(
            store
                .apply_event(&command_completed(
                    thread,
                    turn,
                    item_id,
                    "proc_1",
                    Some("in_progress"),
                ))
                .await
        );

        let snapshot = store.snapshot(thread).await;
        assert_eq!(snapshot.len(), 1);
        assert!(snapshot[0].terminating);
        assert_eq!(snapshot[0].output, "finished");
    }

    #[tokio::test]
    async fn command_output_tail_truncates_on_utf8_boundary() {
        let store = RunningTaskStore::new();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();
        let tail = "a".repeat(MAX_OUTPUT_TAIL - 1);

        store
            .apply_event(&command_start(
                thread,
                turn,
                item_id,
                "proc_1",
                "in_progress",
            ))
            .await;
        assert!(
            store
                .apply_event(&AgentEvent::ItemDelta {
                    thread,
                    turn,
                    item_id,
                    delta: ItemDelta::CommandOutput {
                        chunk: format!("é{tail}"),
                    },
                })
                .await
        );

        let snapshot = store.snapshot(thread).await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].output, tail);
        assert!(snapshot[0].output.len() <= MAX_OUTPUT_TAIL);
    }
}
