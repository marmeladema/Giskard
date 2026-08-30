use std::collections::{HashMap, HashSet};

use giskard_core::error::PersistError;
use giskard_core::ids::{ProjectId, ThreadId};
use giskard_core::thread::ThreadKind;
use giskard_persist::PersistStore;
use giskard_persist::store::{ThreadFile, ThreadGitWorkspace};
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExistingLinkDisposition {
    OwnedChild,
    Parent,
    SelfLink,
    PrimaryThread,
    DifferentParent,
    WouldCycle,
}

impl ExistingLinkDisposition {
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::OwnedChild => "existing sub-agent already belongs to this parent",
            Self::Parent => "linked thread is the source thread's parent",
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

/// The Git workspace a thread's work belongs in: its own when it has one, otherwise the nearest
/// one above it in the parent chain. Strategy-neutral — a sub-agent inherits whatever its owner was
/// created with.
///
/// Only a thread started with isolation carries a workspace record; a sub-agent never does. The
/// harness spawns a sub-agent inside its parent's turn, so it inherits that cwd and its work lands
/// in the parent's worktree — but a lookup that read the child's own record alone would answer with
/// the project's checkout, and it would be wrong in the way that is hardest to notice. The harness
/// ignores a cwd override while the child is still live, so the mismatch would surface only on the
/// next cold turn: a direct follow-up, or anything after a restart, running somewhere other than
/// where that same thread's earlier work is.
///
/// The chain is read, never copied down it. The worktree stays owned by the thread that created it,
/// so deleting a sub-agent cannot take its parent's checkout and branch with it.
pub(crate) async fn inherited_git_workspace(
    store: &PersistStore,
    project_id: ProjectId,
    thread: &ThreadFile,
) -> Result<Option<ThreadGitWorkspace>, PersistError> {
    if thread.git_workspace.is_some() {
        return Ok(thread.git_workspace.clone());
    }
    let mut seen = HashSet::from([thread.id]);
    let mut next = thread.parent_thread_id;
    while let Some(parent_id) = next {
        // A cyclic chain is malformed rather than impossible — `graph_issue` reports one instead of
        // repairing it — and this walk has to terminate on it either way.
        if !seen.insert(parent_id) {
            warn!(
                %project_id,
                thread_id = %thread.id,
                "stopping the Git workspace lookup on a cyclic parent chain"
            );
            return Ok(None);
        }
        // A parent that is not persisted breaks the chain, and the caller's fallback is the
        // project's checkout — so a sub-agent silently runs in the user's tree instead of the
        // worktree its owner works in. That is the failure this function's callers are least able
        // to notice, so it is said out loud.
        let Some(parent) = store.load_thread(project_id, parent_id).await? else {
            warn!(
                %project_id,
                thread_id = %thread.id,
                missing_parent_thread_id = %parent_id,
                "stopping the worktree lookup on a parent that is not persisted"
            );
            return Ok(None);
        };
        if parent.git_workspace.is_some() {
            return Ok(parent.git_workspace);
        }
        next = parent.parent_thread_id;
    }
    Ok(None)
}

pub(crate) fn classify_existing_link(
    graph: &HashMap<ThreadId, ThreadFile>,
    proposed_parent: ThreadId,
    existing: &ThreadFile,
) -> ExistingLinkDisposition {
    if existing.id == proposed_parent {
        return ExistingLinkDisposition::SelfLink;
    }
    if graph
        .get(&proposed_parent)
        .is_some_and(|source| source.parent_thread_id == Some(existing.id))
        && parent_chain_is_valid(graph, proposed_parent)
    {
        return ExistingLinkDisposition::Parent;
    }
    if existing.kind == ThreadKind::Primary {
        return ExistingLinkDisposition::PrimaryThread;
    }
    if existing.kind == ThreadKind::Orphan {
        if parent_chain_reaches(graph, proposed_parent, existing.id) {
            return ExistingLinkDisposition::WouldCycle;
        }
        return ExistingLinkDisposition::OwnedChild;
    }
    if existing.parent_thread_id.is_none() {
        return ExistingLinkDisposition::DifferentParent;
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
            (ThreadKind::Orphan, _) => return false,
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
        (ThreadKind::Orphan, Some(_)) => Some("orphan thread unexpectedly has a parent"),
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
    use giskard_core::turn::{Mode, PermissionPreset};

    use super::*;

    fn thread(id: ThreadId, kind: ThreadKind, parent: Option<ThreadId>) -> ThreadFile {
        ThreadFile {
            revision: 0,
            version: 1,
            id,
            project_id: ProjectId::new(),
            title: id.to_string(),
            harness_thread_id: format!("native-{id}"),
            parent_thread_id: parent,
            spawned_by_turn_id: None,
            kind,
            mode: giskard_core::turn::TurnMode::Known(Mode::Build),
            current_model: giskard_core::turn::TurnModel::Known(ModelRef {
                provider: "test".into(),
                model: "test".into(),
                reasoning_effort: None,
            }),
            context_window: 1,
            model_context_windows: HashMap::new(),
            permission_preset: PermissionPreset::AskFirst,
            model_efforts: HashMap::new(),
            tokens: TokenLedger::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            archived: false,
            git_workspace: None,
        }
    }

    fn worktree(path: &str) -> ThreadGitWorkspace {
        ThreadGitWorkspace::Worktree(giskard_persist::store::ThreadWorktree {
            path: path.into(),
            workspace: None,
            branch: "giskard/worktree-01test".into(),
            base_commit: None,
            repo_root: "/repo".into(),
            common_dir: "/repo/.git".into(),
            git_dir: "/repo/.git/worktrees/t".into(),
        })
    }

    /// A sub-agent works in its parent's worktree and never records one, so the lookup has to read
    /// the chain — at any depth, and without hanging on a chain that is malformed.
    #[tokio::test]
    async fn worktree_is_inherited_from_the_nearest_owner_in_the_chain() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = PersistStore::new(dir.path().to_path_buf());
        let project_id = ProjectId::new();
        store
            .create_project(project_id, "inheritance", "/repo")
            .await
            .unwrap();

        let root = ThreadId::new();
        let child = ThreadId::new();
        let grandchild = ThreadId::new();
        let orphan = ThreadId::new();
        let mut files = vec![
            thread(root, ThreadKind::Primary, None),
            thread(child, ThreadKind::Subagent, Some(root)),
            thread(grandchild, ThreadKind::Subagent, Some(child)),
            thread(orphan, ThreadKind::Subagent, Some(ThreadId::new())),
        ];
        files[0].git_workspace = Some(worktree("/worktrees/root"));
        for file in &files {
            store.save_thread(project_id, file).await.unwrap();
        }

        // Its own record wins, and a descendant at any depth inherits it.
        for (thread, expected) in [
            (&files[0], Some("/worktrees/root")),
            (&files[1], Some("/worktrees/root")),
            (&files[2], Some("/worktrees/root")),
            // A parent that is not persisted answers "none", not the nearest thing to hand.
            (&files[3], None),
        ] {
            assert_eq!(
                inherited_git_workspace(&store, project_id, thread)
                    .await
                    .unwrap()
                    .map(|workspace| workspace.workspace_root().to_string()),
                expected.map(str::to_string),
                "wrong Git workspace for {}",
                thread.id
            );
        }

        // A child of an owner-less chain inherits nothing rather than the project's own worktree.
        files[0].git_workspace = None;
        store.save_thread(project_id, &files[0]).await.unwrap();
        assert!(
            inherited_git_workspace(&store, project_id, &files[2])
                .await
                .unwrap()
                .is_none()
        );

        // A cycle is malformed but persistable — `graph_issue` reports one rather than repairing
        // it — so the walk has to terminate instead of following it forever.
        //
        // The cycle-closing thread owns a workspace *on disk*, so this asserts on a value rather
        // than only on termination: a walk that followed the cycle would come back around, load it,
        // and answer `Some`. Without that the assertion could only ever fail by hanging.
        //
        // Saved from a clone so the in-memory `files[2]` the walk starts from still owns nothing —
        // otherwise the lookup short-circuits on its own record and never walks at all.
        let mut cycle_owner = files[2].clone();
        cycle_owner.git_workspace = Some(worktree("/worktrees/grandchild"));
        store.save_thread(project_id, &cycle_owner).await.unwrap();
        files[0].kind = ThreadKind::Subagent;
        files[0].parent_thread_id = Some(grandchild);
        store.save_thread(project_id, &files[0]).await.unwrap();
        assert!(
            inherited_git_workspace(&store, project_id, &files[2])
                .await
                .unwrap()
                .is_none()
        );
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
            ExistingLinkDisposition::Parent
        );
        assert_eq!(
            classify_existing_link(&graph, child, graph.get(&other_root).unwrap()),
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
