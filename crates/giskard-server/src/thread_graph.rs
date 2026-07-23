use std::collections::{HashMap, HashSet};

use giskard_core::ids::{ProjectId, ThreadId};
use giskard_core::thread::ThreadKind;
use giskard_persist::PersistStore;
use giskard_persist::store::ThreadFile;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExistingLinkDisposition {
    OwnedChild,
    SelfLink,
    PrimaryThread,
    DifferentParent,
    WouldCycle,
}

impl ExistingLinkDisposition {
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::OwnedChild => "existing sub-agent already belongs to this parent",
            Self::SelfLink => "thread cannot be its own child",
            Self::PrimaryThread => "existing primary thread cannot be reclassified as a sub-agent",
            Self::DifferentParent => "existing sub-agent belongs to a different parent",
            Self::WouldCycle => "sub-agent relationship would create a thread cycle",
        }
    }
}

pub(crate) async fn load_thread_graph(
    store: &PersistStore,
    project_id: ProjectId,
) -> Result<HashMap<ThreadId, ThreadFile>, giskard_core::error::PersistError> {
    let mut graph = HashMap::new();
    for thread_id in store.list_threads(project_id).await? {
        if let Some(thread) = store.load_thread(project_id, thread_id).await? {
            graph.insert(thread_id, thread);
        }
    }
    Ok(graph)
}

pub(crate) fn classify_existing_link(
    graph: &HashMap<ThreadId, ThreadFile>,
    proposed_parent: ThreadId,
    existing: &ThreadFile,
) -> ExistingLinkDisposition {
    if existing.id == proposed_parent {
        return ExistingLinkDisposition::SelfLink;
    }
    if existing.kind == ThreadKind::Primary || existing.parent_thread_id.is_none() {
        return ExistingLinkDisposition::PrimaryThread;
    }
    if existing.parent_thread_id != Some(proposed_parent) {
        return ExistingLinkDisposition::DifferentParent;
    }
    if parent_chain_reaches(graph, proposed_parent, existing.id) {
        return ExistingLinkDisposition::WouldCycle;
    }
    ExistingLinkDisposition::OwnedChild
}

pub(crate) fn parent_chain_is_valid(
    graph: &HashMap<ThreadId, ThreadFile>,
    start: ThreadId,
) -> bool {
    let mut current = start;
    let mut seen = HashSet::new();
    loop {
        if !seen.insert(current) {
            return false;
        }
        let Some(thread) = graph.get(&current) else {
            return false;
        };
        match (thread.kind, thread.parent_thread_id) {
            (ThreadKind::Subagent, Some(parent)) => current = parent,
            (ThreadKind::Primary, None) => return true,
            _ => return false,
        }
    }
}

pub(crate) fn graph_issue(
    graph: &HashMap<ThreadId, ThreadFile>,
    thread: &ThreadFile,
) -> Option<&'static str> {
    match (thread.kind, thread.parent_thread_id) {
        (ThreadKind::Primary, Some(_)) => Some("primary thread has a parent"),
        (ThreadKind::Subagent, None) => Some("sub-agent thread has no parent"),
        (ThreadKind::Subagent, Some(_)) if !parent_chain_is_valid(graph, thread.id) => {
            Some("sub-agent parent chain is missing or cyclic")
        }
        _ => None,
    }
}

pub(crate) fn should_refresh_subagent_title(current: &str, desired: &str) -> bool {
    current != desired
        && (current.starts_with("Sub-agent")
            || current
                .chars()
                .all(|ch| ch.is_ascii_hexdigit() || ch == '-'))
}

/// Return a deterministic leaf-first deletion order for `root` and every thread that names it,
/// directly or transitively, as its parent. The visited set also makes malformed persisted cycles
/// finite; deleting either node of a two-node cycle includes both nodes exactly once.
pub(crate) fn descendant_deletion_order(
    graph: &HashMap<ThreadId, ThreadFile>,
    root: ThreadId,
) -> Vec<ThreadId> {
    fn visit(
        graph: &HashMap<ThreadId, ThreadFile>,
        current: ThreadId,
        seen: &mut HashSet<ThreadId>,
        order: &mut Vec<ThreadId>,
    ) {
        if !seen.insert(current) {
            return;
        }
        let mut children = graph
            .values()
            .filter(|thread| thread.parent_thread_id == Some(current))
            .map(|thread| thread.id)
            .collect::<Vec<_>>();
        children.sort_by_key(ToString::to_string);
        for child in children {
            visit(graph, child, seen, order);
        }
        order.push(current);
    }

    if !graph.contains_key(&root) {
        return Vec::new();
    }
    let mut seen = HashSet::new();
    let mut order = Vec::new();
    visit(graph, root, &mut seen, &mut order);
    order
}

fn parent_chain_reaches(
    graph: &HashMap<ThreadId, ThreadFile>,
    start: ThreadId,
    target: ThreadId,
) -> bool {
    let mut current = Some(start);
    let mut seen = HashSet::new();
    while let Some(thread_id) = current {
        if thread_id == target {
            return true;
        }
        if !seen.insert(thread_id) {
            return true;
        }
        current = graph
            .get(&thread_id)
            .and_then(|thread| thread.parent_thread_id);
    }
    false
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use giskard_core::model::ModelRef;
    use giskard_core::token::TokenLedger;
    use giskard_core::turn::{ApprovalPolicy, Mode};

    use super::*;

    fn thread(id: ThreadId, kind: ThreadKind, parent: Option<ThreadId>) -> ThreadFile {
        ThreadFile {
            version: 1,
            id,
            project_id: ProjectId::new(),
            title: id.to_string(),
            harness_thread_id: format!("native-{id}"),
            parent_thread_id: parent,
            spawned_by_turn_id: None,
            kind,
            mode: Mode::Build,
            current_model: ModelRef {
                provider: "test".into(),
                model: "test".into(),
                reasoning_effort: None,
            },
            context_window: 1,
            model_context_windows: HashMap::new(),
            approval_policy: ApprovalPolicy::Ask,
            model_efforts: HashMap::new(),
            tokens: TokenLedger::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            archived: false,
        }
    }

    #[test]
    fn classifies_existing_links_without_mutating_ownership() {
        let root = ThreadId::new();
        let child = ThreadId::new();
        let other_root = ThreadId::new();
        let other_child = ThreadId::new();
        let mut graph = HashMap::from([
            (root, thread(root, ThreadKind::Primary, None)),
            (child, thread(child, ThreadKind::Subagent, Some(root))),
            (other_root, thread(other_root, ThreadKind::Primary, None)),
            (
                other_child,
                thread(other_child, ThreadKind::Subagent, Some(other_root)),
            ),
        ]);

        assert_eq!(
            classify_existing_link(&graph, root, graph.get(&root).unwrap()),
            ExistingLinkDisposition::SelfLink
        );
        assert_eq!(
            classify_existing_link(&graph, child, graph.get(&root).unwrap()),
            ExistingLinkDisposition::PrimaryThread
        );
        assert_eq!(
            classify_existing_link(&graph, root, graph.get(&other_child).unwrap()),
            ExistingLinkDisposition::DifferentParent
        );
        assert_eq!(
            classify_existing_link(&graph, root, graph.get(&child).unwrap()),
            ExistingLinkDisposition::OwnedChild
        );

        graph.get_mut(&root).unwrap().kind = ThreadKind::Subagent;
        graph.get_mut(&root).unwrap().parent_thread_id = Some(child);
        assert_eq!(
            classify_existing_link(&graph, child, graph.get(&root).unwrap()),
            ExistingLinkDisposition::WouldCycle
        );
        assert_eq!(
            graph_issue(&graph, graph.get(&root).unwrap()),
            Some("sub-agent parent chain is missing or cyclic")
        );
    }

    #[test]
    fn validates_complete_parent_chains_and_reports_dangling_ones() {
        let root = ThreadId::new();
        let child = ThreadId::new();
        let grandchild = ThreadId::new();
        let dangling = ThreadId::new();
        let malformed_parent = ThreadId::new();
        let malformed_child = ThreadId::new();
        let missing = ThreadId::new();
        let graph = HashMap::from([
            (root, thread(root, ThreadKind::Primary, None)),
            (child, thread(child, ThreadKind::Subagent, Some(root))),
            (
                grandchild,
                thread(grandchild, ThreadKind::Subagent, Some(child)),
            ),
            (
                dangling,
                thread(dangling, ThreadKind::Subagent, Some(missing)),
            ),
            (
                malformed_parent,
                thread(malformed_parent, ThreadKind::Primary, Some(root)),
            ),
            (
                malformed_child,
                thread(
                    malformed_child,
                    ThreadKind::Subagent,
                    Some(malformed_parent),
                ),
            ),
        ]);

        assert!(parent_chain_is_valid(&graph, root));
        assert!(parent_chain_is_valid(&graph, grandchild));
        assert!(!parent_chain_is_valid(&graph, dangling));
        assert!(!parent_chain_is_valid(&graph, malformed_parent));
        assert!(!parent_chain_is_valid(&graph, malformed_child));
        assert_eq!(
            graph_issue(&graph, graph.get(&dangling).unwrap()),
            Some("sub-agent parent chain is missing or cyclic")
        );
        assert_eq!(
            graph_issue(&graph, graph.get(&malformed_parent).unwrap()),
            Some("primary thread has a parent")
        );
        assert_eq!(
            graph_issue(&graph, graph.get(&malformed_child).unwrap()),
            Some("sub-agent parent chain is missing or cyclic")
        );
    }

    #[test]
    fn orders_descendants_before_their_parent_and_handles_cycles() {
        let root = ThreadId::new();
        let child = ThreadId::new();
        let grandchild = ThreadId::new();
        let sibling = ThreadId::new();
        let mut graph = HashMap::from([
            (root, thread(root, ThreadKind::Primary, None)),
            (child, thread(child, ThreadKind::Subagent, Some(root))),
            (
                grandchild,
                thread(grandchild, ThreadKind::Subagent, Some(child)),
            ),
            (sibling, thread(sibling, ThreadKind::Subagent, Some(root))),
        ]);

        let order = descendant_deletion_order(&graph, root);
        assert_eq!(order.last(), Some(&root));
        assert!(
            order.iter().position(|id| *id == grandchild)
                < order.iter().position(|id| *id == child)
        );
        assert!(order.iter().position(|id| *id == child) < order.iter().position(|id| *id == root));
        assert!(
            order.iter().position(|id| *id == sibling) < order.iter().position(|id| *id == root)
        );

        graph.get_mut(&root).unwrap().kind = ThreadKind::Subagent;
        graph.get_mut(&root).unwrap().parent_thread_id = Some(child);
        let cycle_order = descendant_deletion_order(&graph, root);
        assert_eq!(cycle_order.len(), graph.len());
        assert_eq!(cycle_order.last(), Some(&root));
        assert_eq!(cycle_order.iter().filter(|id| **id == child).count(), 1);
    }
}
