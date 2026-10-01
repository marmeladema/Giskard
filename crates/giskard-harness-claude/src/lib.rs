//! Claude Code CLI adapter (`claude -p --output-format stream-json`).
//!
//! Milestone 1 of `specs/claude-code-harness-plan.md`: the pure mapper from Claude Code's
//! stream-json output to Giskard's [`AgentEvent`](giskard_core::event::AgentEvent)s, driven by the
//! recorded fixtures under `tests/fixtures/`. Nothing here spawns a process or implements
//! `AgentHarness` yet; see this crate's README for which milestone supplies what is missing.
//!
//! Every Claude Code-specific type stays inside this crate.

mod frame;
mod ids;
mod log_fields;
mod mapper;

pub use frame::{BlockStart, Delta, Frame, FrameError, StreamEvent, StreamEventKind};
pub use mapper::{ClaudeMapper, MapperOutput, Route, TurnKind};

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
        mcp_status: true,
        context_compaction: true,
        // `structured_diffs`, `mcp_reload`, `mcp_oauth_login` and `turn_steering` are false per
        // plan §4: Claude Code offers no path behind them.
        ..HarnessCapabilities::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_advertise_only_what_milestone_one_can_back() {
        let capabilities = capabilities();
        assert!(!capabilities.live_approvals);
        assert!(!capabilities.plan_build_modes);
        assert!(!capabilities.per_turn_model);
        assert!(!capabilities.reasoning_effort);
        assert!(capabilities.resumable_threads);
        assert!(capabilities.token_usage);
        assert!(capabilities.context_compaction);
        assert!(!capabilities.structured_diffs);
        assert!(!capabilities.mcp_reload);
        assert!(!capabilities.mcp_oauth_login);
        assert!(!capabilities.turn_steering);
    }
}
