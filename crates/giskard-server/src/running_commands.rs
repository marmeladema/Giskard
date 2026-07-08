use std::collections::HashMap;
use std::path::Path;

use chrono::Utc;
use tokio::sync::Mutex;

use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ThreadId};
use giskard_core::item::{ItemDelta, ItemPayload};
use giskard_proto::RunningCommand;

const MAX_OUTPUT_TAIL: usize = 8_000;

#[derive(Default)]
pub struct RunningCommandStore {
    commands: Mutex<HashMap<ThreadId, HashMap<ItemId, RunningCommand>>>,
}

impl RunningCommandStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn apply_event(&self, event: &AgentEvent) -> bool {
        match event {
            AgentEvent::ItemStarted { thread, turn, item } => {
                let Some(command) = &item.command else {
                    return false;
                };
                let status = command
                    .status
                    .clone()
                    .unwrap_or_else(|| "in_progress".into());
                if !command_status_is_running(&status) {
                    return false;
                }
                let cmd = RunningCommand {
                    thread_id: *thread,
                    turn_id: *turn,
                    item_id: item.id,
                    harness_item_id: item.harness_item_id.clone(),
                    command: command.command.clone(),
                    cwd: command.cwd.clone(),
                    status,
                    process_id: command.process_id.clone(),
                    started_at_ms: command.started_at_ms.unwrap_or_else(now_ms),
                    output: String::new(),
                    after_turn: false,
                    terminating: false,
                };
                let mut commands = self.commands.lock().await;
                commands.entry(*thread).or_default().insert(item.id, cmd);
                true
            }
            AgentEvent::ItemDelta {
                thread,
                item_id,
                delta: ItemDelta::CommandOutput { chunk },
                ..
            } => {
                let mut commands = self.commands.lock().await;
                let Some(cmd) = commands
                    .get_mut(thread)
                    .and_then(|thread_commands| thread_commands.get_mut(item_id))
                else {
                    return false;
                };
                cmd.output.push_str(chunk);
                truncate_output_tail(&mut cmd.output);
                true
            }
            AgentEvent::ItemCompleted { thread, turn, item } => {
                let ItemPayload::CommandExecution {
                    command,
                    cwd,
                    output,
                    status,
                    process_id,
                    ..
                } = &item.payload
                else {
                    return false;
                };

                let mut commands = self.commands.lock().await;
                let thread_commands = commands.entry(*thread).or_default();
                let Some(status) = status else {
                    return thread_commands.remove(&item.id).is_some();
                };

                if !command_status_is_running(status) {
                    return thread_commands.remove(&item.id).is_some();
                }

                let mut output = output.clone();
                truncate_output_tail(&mut output);
                let after_turn = thread_commands
                    .get(&item.id)
                    .map(|cmd| cmd.after_turn)
                    .unwrap_or(false);
                let started_at_ms = thread_commands
                    .get(&item.id)
                    .map(|cmd| cmd.started_at_ms)
                    .unwrap_or_else(now_ms);
                let terminating = thread_commands
                    .get(&item.id)
                    .map(|cmd| cmd.terminating)
                    .unwrap_or(false);
                thread_commands.insert(
                    item.id,
                    RunningCommand {
                        thread_id: *thread,
                        turn_id: *turn,
                        item_id: item.id,
                        harness_item_id: item.harness_item_id.clone(),
                        command: command.clone(),
                        cwd: path_to_display(cwd),
                        status: status.clone(),
                        process_id: process_id.clone(),
                        started_at_ms,
                        output,
                        after_turn,
                        terminating,
                    },
                );
                true
            }
            AgentEvent::TurnCompleted { thread, turn, .. } => {
                let mut commands = self.commands.lock().await;
                let Some(thread_commands) = commands.get_mut(thread) else {
                    return false;
                };
                let mut changed = false;
                for cmd in thread_commands.values_mut() {
                    if cmd.turn_id == *turn
                        && command_status_is_running(&cmd.status)
                        && !cmd.after_turn
                    {
                        cmd.after_turn = true;
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
        let mut commands = self.commands.lock().await;
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
    ) -> Option<RunningCommand> {
        let commands = self.commands.lock().await;
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
        item_id: ItemId,
    ) -> Option<RunningCommand> {
        let commands = self.commands.lock().await;
        commands
            .get(&thread_id)
            .and_then(|thread_commands| thread_commands.get(&item_id))
            .cloned()
    }

    pub async fn remove_by_process(&self, thread_id: ThreadId, process_id: &str) -> bool {
        let mut commands = self.commands.lock().await;
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

    pub async fn snapshot(&self, thread_id: ThreadId) -> Vec<RunningCommand> {
        let commands = self.commands.lock().await;
        let mut snapshot = commands
            .get(&thread_id)
            .map(|thread_commands| thread_commands.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        snapshot.sort_by_key(|cmd| (cmd.turn_id.to_string(), cmd.item_id.to_string()));
        snapshot
    }
}

fn command_status_is_running(status: &str) -> bool {
    matches!(
        status.to_ascii_lowercase().replace('-', "_").as_str(),
        "in_progress" | "inprogress" | "running"
    )
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

    #[tokio::test]
    async fn running_command_snapshot_tracks_output_and_after_turn() {
        let store = RunningCommandStore::new();
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
        let store = RunningCommandStore::new();
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
        let store = RunningCommandStore::new();
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
        let store = RunningCommandStore::new();
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
        let store = RunningCommandStore::new();
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
    async fn terminal_failed_and_declined_items_remove_commands() {
        for status in ["failed", "declined"] {
            let store = RunningCommandStore::new();
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
        let store = RunningCommandStore::new();
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
                },
            })
            .await;

        assert!(store.remove_by_process(thread, "proc_1").await);
        assert!(store.snapshot(thread).await.is_empty());
        assert!(!store.remove_by_process(thread, "proc_1").await);
    }

    #[tokio::test]
    async fn remove_by_process_preserves_unrelated_commands() {
        let store = RunningCommandStore::new();
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
        let store = RunningCommandStore::new();
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
        let store = RunningCommandStore::new();
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
            .get_by_item(thread, item_id)
            .await
            .expect("running command should be indexed by item id");
        assert_eq!(command.process_id.as_deref(), Some("proc_1"));
        assert!(store.get_by_item(thread, ItemId::new()).await.is_none());
    }

    #[tokio::test]
    async fn running_status_completion_preserves_terminating_flag() {
        let store = RunningCommandStore::new();
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
        let store = RunningCommandStore::new();
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
