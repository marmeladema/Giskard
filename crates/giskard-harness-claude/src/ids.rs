//! Native identifiers the mapper keys its state by.

/// Prefix of the native id Giskard gives a Claude Code sub-agent thread: `task:<tool_use_id>`,
/// where the tool-use id is that of the parent's `Agent` call. A session UUID never carries it.
pub const TASK_ID_PREFIX: &str = "task:";

/// Whether a native thread id names a sub-agent route rather than a Claude Code session.
pub fn is_task_native_id(native_id: &str) -> bool {
    native_id.starts_with(TASK_ID_PREFIX)
}

/// The native identity of one item within a turn.
///
/// A `tool_use` block has its own id, which `can_use_tool.tool_use_id` and
/// `tool_result.tool_use_id` reference. Text and thinking blocks have none, so they are keyed by
/// the API message that carries them and their index within it: the CLI emits an `assistant`
/// frame per block, each repeating the same `message.id`, and the stream's
/// `content_block_*.index` numbers the same blocks.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NativeItemKey {
    ToolUse(String),
    Block { message_id: String, index: u32 },
}

impl NativeItemKey {
    /// The `harness_item_id` an item with this key carries, and its `native_item_id` log field.
    pub fn harness_item_id(&self) -> String {
        match self {
            Self::ToolUse(id) => id.clone(),
            Self::Block { message_id, index } => format!("{message_id}:{index}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_native_ids_are_recognised_by_their_prefix() {
        assert!(is_task_native_id("task:toolu_01DSgcYLdZqTSvfAwdnE2njN"));
        assert!(!is_task_native_id("f18693ff-2d11-4f87-9556-2b527e19e081"));
    }

    #[test]
    fn harness_item_ids_name_the_tool_use_or_the_message_block() {
        assert_eq!(
            NativeItemKey::ToolUse("toolu_1".into()).harness_item_id(),
            "toolu_1"
        );
        assert_eq!(
            NativeItemKey::Block {
                message_id: "msg_1".into(),
                index: 2
            }
            .harness_item_id(),
            "msg_1:2"
        );
    }
}
