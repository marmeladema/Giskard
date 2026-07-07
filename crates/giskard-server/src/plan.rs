//! Plan-dump-to-markdown helpers (spec §7.4.1).
//!
//! Pure logic only: extracting the plan text from a thread's history and resolving the target
//! path safely inside the workspace root. The actual filesystem write lives in `routes`.

use std::path::{Component, Path, PathBuf};

use giskard_core::item::ItemPayload;
use giskard_core::turn::Mode;
use giskard_persist::store::ThreadFile;

/// Extract "the current plan" as markdown: the concatenation of the agent-message items of the
/// **most recent Plan-mode turn** in the thread (spec §7.4.1). Returns `None` if there is no
/// Plan-mode turn with agent text yet.
pub fn extract_plan_markdown(thread: &ThreadFile) -> Option<String> {
    let turn = thread.turns.iter().rev().find(|t| t.mode == Mode::Plan)?;

    let body = turn
        .items
        .iter()
        .filter_map(|item| match &item.payload {
            ItemPayload::AgentMessage { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n\n");

    if body.trim().is_empty() {
        return None;
    }

    let title = thread.title.trim();
    if title.is_empty() {
        Some(body)
    } else {
        Some(format!("# {title}\n\n{body}\n"))
    }
}

/// Resolve a user-supplied plan path against the workspace root, rejecting any path that would
/// escape the root (spec §7.4.1: the write respects the workspace-root boundary).
///
/// Works purely lexically (the file need not exist yet): a leading `/` is treated as
/// workspace-relative, and `..` components may not climb above the root.
pub fn safe_plan_path(workspace_root: &Path, requested: &str) -> Option<PathBuf> {
    let rel = Path::new(requested.trim_start_matches('/'));
    let mut resolved = PathBuf::new();
    for comp in rel.components() {
        match comp {
            Component::Normal(c) => resolved.push(c),
            Component::CurDir => {}
            Component::ParentDir => {
                // Refuse to climb above the workspace root.
                if !resolved.pop() {
                    return None;
                }
            }
            // Reject absolute prefixes / root / Windows prefixes.
            Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    if resolved.as_os_str().is_empty() {
        return None;
    }
    Some(workspace_root.join(resolved))
}

/// Build the default plan path from config template + a title slug + timestamp (spec §7.4.1).
pub fn default_plan_path(default_dir: &str, template: &str, title: &str, ts: &str) -> String {
    let slug = slugify(title);
    let filename = template.replace("{slug}", &slug).replace("{ts}", ts);
    if default_dir.is_empty() {
        filename
    } else {
        format!("{}/{}", default_dir.trim_end_matches('/'), filename)
    }
}

fn slugify(s: &str) -> String {
    let mut out = String::new();
    let mut prev_dash = false;
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() {
            out.extend(ch.to_lowercase());
            prev_dash = false;
        } else if !prev_dash && !out.is_empty() {
            out.push('-');
            prev_dash = true;
        }
    }
    let trimmed = out.trim_end_matches('-').to_string();
    if trimmed.is_empty() {
        "plan".to_string()
    } else {
        trimmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use giskard_core::ids::TurnId;
    use giskard_core::item::{Item, ItemPayload};
    use giskard_core::model::ModelRef;
    use giskard_core::token::{TokenLedger, TokenUsage};
    use giskard_core::turn::{Mode, Turn, TurnStatus, TurnStatusKind};
    use giskard_core::user_input::UserInput;
    use giskard_persist::store::ThreadFile;

    fn model() -> ModelRef {
        ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        }
    }

    fn agent_item(text: &str) -> Item {
        Item {
            id: giskard_core::ids::ItemId::new(),
            harness_item_id: String::new(),
            payload: ItemPayload::AgentMessage { text: text.into() },
            created_at: Utc::now(),
        }
    }

    fn turn(mode: Mode, items: Vec<Item>) -> Turn {
        Turn {
            id: TurnId::new(),
            user_input: UserInput::text("plan it"),
            items,
            model: model(),
            mode,
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
            usage: TokenUsage::default(),
            diffs: Vec::new(),
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
        }
    }

    fn thread_with(turns: Vec<Turn>) -> ThreadFile {
        ThreadFile {
            version: 1,
            id: giskard_core::ids::ThreadId::new(),
            project_id: giskard_core::ids::ProjectId::new(),
            title: "Fix auth".into(),
            harness_thread_id: "th".into(),
            mode: Mode::Plan,
            current_model: model(),
            context_window: 0,
            approval_policy: None,
            model_efforts: std::collections::HashMap::new(),
            tokens: TokenLedger::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            turns,
        }
    }

    #[test]
    fn extracts_latest_plan_turn() {
        let thread = thread_with(vec![
            turn(Mode::Plan, vec![agent_item("old plan")]),
            turn(Mode::Build, vec![agent_item("building")]),
            turn(Mode::Plan, vec![agent_item("step 1"), agent_item("step 2")]),
        ]);
        let md = extract_plan_markdown(&thread).unwrap();
        assert!(md.contains("step 1"));
        assert!(md.contains("step 2"));
        assert!(!md.contains("old plan"));
        assert!(md.starts_with("# Fix auth"));
    }

    #[test]
    fn no_plan_turn_returns_none() {
        let thread = thread_with(vec![turn(Mode::Build, vec![agent_item("building")])]);
        assert!(extract_plan_markdown(&thread).is_none());
    }

    #[test]
    fn safe_path_confines_to_root() {
        let root = Path::new("/home/elie/dev/proj");
        assert_eq!(
            safe_plan_path(root, "docs/plan.md"),
            Some(PathBuf::from("/home/elie/dev/proj/docs/plan.md"))
        );
        // absolute-looking is treated as workspace-relative
        assert_eq!(
            safe_plan_path(root, "/docs/plan.md"),
            Some(PathBuf::from("/home/elie/dev/proj/docs/plan.md"))
        );
        // escapes are rejected
        assert!(safe_plan_path(root, "../evil.md").is_none());
        assert!(safe_plan_path(root, "docs/../../evil.md").is_none());
    }

    #[test]
    fn default_path_template() {
        let p = default_plan_path(
            "docs",
            "plan-{slug}-{ts}.md",
            "Fix Qobuz OAuth!",
            "20260706-1030",
        );
        assert_eq!(p, "docs/plan-fix-qobuz-oauth-20260706-1030.md");
    }
}
