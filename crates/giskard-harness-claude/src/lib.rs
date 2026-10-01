//! Claude Code CLI adapter (`claude -p --output-format stream-json`).
//!
//! Milestone 2 of `specs/claude-code-harness-plan.md`: the pure mapper from Claude Code's
//! stream-json output to Giskard's [`AgentEvent`](giskard_core::event::AgentEvent)s, plus
//! [`ClaudeHarness`], an `AgentHarness` that supervises one `claude` child per primary thread.
//! Approvals, server requests and per-turn settings arrive in milestone 3; see this crate's README
//! for which milestone supplies what is missing.
//!
//! Every Claude Code-specific type stays inside this crate.

mod attachments;
mod catalog;
mod frame;
mod harness;
mod ids;
mod log_fields;
mod mapper;
mod process;
mod session;

pub use catalog::ANTHROPIC_PROVIDER_ID;
pub use frame::{BlockStart, Delta, Frame, FrameError, StreamEvent, StreamEventKind};
pub use harness::ClaudeHarness;
pub use mapper::{ClaudeMapper, MapperOutput, Route, TurnKind};
pub use process::ClaudeLaunchOptions;

use giskard_harness::HarnessCapabilities;

/// What the Claude Code adapter advertises (plan §4).
///
/// `live_approvals`, `plan_build_modes`, `per_turn_model` and `reasoning_effort` stay false until
/// milestone 3 builds the paths behind them.
pub fn capabilities() -> HarnessCapabilities {
    HarnessCapabilities {
        live_approvals: false,
        plan_build_modes: false,
        per_turn_model: false,
        reasoning_effort: false,
        resumable_threads: true,
        model_listing: true,
        provider_listing: true,
        token_usage: true,
        // Milestone 4 wires `list_mcp_servers` (the `mcp_status` control request) beside
        // registration; until then the trait default answers `Unsupported`.
        mcp_status: false,
        // Milestone 3 implements `compact_thread` (`/compact`); until then the trait default
        // answers `Unsupported`.
        context_compaction: false,
        // `structured_diffs`, `mcp_reload`, `mcp_oauth_login` and `turn_steering` are false per
        // plan §4: Claude Code offers no path behind them.
        ..HarnessCapabilities::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_advertise_only_what_milestone_two_can_back() {
        let capabilities = capabilities();
        assert!(!capabilities.live_approvals);
        assert!(!capabilities.plan_build_modes);
        assert!(!capabilities.per_turn_model);
        assert!(!capabilities.reasoning_effort);
        assert!(capabilities.resumable_threads);
        assert!(capabilities.token_usage);
        assert!(capabilities.model_listing);
        assert!(capabilities.provider_listing);
        assert!(!capabilities.mcp_status);
        assert!(!capabilities.context_compaction);
        assert!(!capabilities.structured_diffs);
        assert!(!capabilities.mcp_reload);
        assert!(!capabilities.mcp_oauth_login);
        assert!(!capabilities.turn_steering);
    }
}
