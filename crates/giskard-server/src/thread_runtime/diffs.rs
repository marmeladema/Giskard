//! The captured diff bodies of a thread's in-flight turns.
//!
//! Full diff text never reaches reconnect state or a browser projection: an event carrying one is
//! stripped here and the body kept behind its content identity until the turn settles. Current
//! authority is a set of logical slots, so turn-level paths and each occurrence of a path inside
//! an item evolve independently.

use std::collections::HashMap;

use giskard_core::diff::{CapturedDiffDescriptor, CapturedDiffRecord};
use giskard_core::event::AgentEvent;
use giskard_core::ids::{DiffId, ItemId, ThreadId, TurnId};
use giskard_core::item::ItemPayload;
use tracing::debug;

#[derive(Default)]
struct ActiveCapturedDiffs {
    // Current authority is a set of logical slots, not a path map: turn-level paths and each
    // occurrence of a path inside an item evolve independently. ItemCompleted replaces the
    // complete slot set for that item. A matched replacement keeps one conflict redirect; an
    // omitted slot becomes missing. `contents` contains exactly bodies still referenced by at
    // least one current slot, with identical content identities shared across slots.
    contents: HashMap<DiffId, CapturedDiffRecord>,
    current_by_slot: HashMap<CapturedDiffSlot, CapturedDiffDescriptor>,
    superseded: HashMap<DiffId, SupersededCapturedDiff>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CapturedDiffSlot {
    Item {
        item_id: ItemId,
        path: std::path::PathBuf,
        occurrence: usize,
    },
    Turn(std::path::PathBuf),
}

struct SupersededCapturedDiff {
    slot: CapturedDiffSlot,
    current: CapturedDiffDescriptor,
}

pub enum RuntimeDiffLookup {
    Found(CapturedDiffRecord),
    Superseded(CapturedDiffDescriptor),
    Missing,
}

#[derive(Default)]
pub struct CapturedDiffState {
    turns: HashMap<TurnId, ActiveCapturedDiffs>,
}

impl CapturedDiffState {
    /// Extracts full diff bodies from `event`, replacing them in place with their descriptors.
    pub fn capture(&mut self, thread_id: ThreadId, event: &mut AgentEvent) {
        match &mut *event {
            AgentEvent::ItemCompleted { turn, item, .. } => {
                let state = self.turns.entry(*turn).or_default();
                let mut captures = Vec::new();
                if let ItemPayload::FileChange { changes, .. } = &mut item.payload {
                    let mut occurrences = HashMap::new();
                    for change in changes {
                        let occurrence = occurrences.entry(change.path.clone()).or_insert(0);
                        let slot = CapturedDiffSlot::Item {
                            item_id: item.id,
                            path: change.path.clone(),
                            occurrence: *occurrence,
                        };
                        *occurrence += 1;
                        let Some(text) = change.diff.take() else {
                            continue;
                        };
                        let (descriptor, record) = giskard_core::capture_unified_diff(
                            change.path.clone(),
                            change.change,
                            Some(item.id),
                            text,
                        );
                        captures.push((slot, descriptor.clone(), record));
                        change.captured_diff = Some(descriptor);
                    }
                }
                // ItemCompleted is an upsert of the complete item payload. An empty file-change
                // set or a replacement payload of another kind therefore retires every old slot.
                reconcile_item_captured_diffs(state, thread_id, *turn, item.id, captures);
            }
            AgentEvent::DiffUpdated { turn, diff, .. } => {
                let (projected, record) = giskard_core::capture_structured_diff(diff.clone());
                if let Some(descriptor) = projected.captured.clone() {
                    let state = self.turns.entry(*turn).or_default();
                    install_captured_diff(
                        state,
                        thread_id,
                        *turn,
                        CapturedDiffSlot::Turn(descriptor.path.clone()),
                        descriptor,
                        record,
                    );
                }
                *diff = projected;
            }
            _ => {}
        }
    }

    /// Every captured body still current for a turn.
    pub fn records(&self, turn_id: TurnId) -> Vec<CapturedDiffRecord> {
        self.turns
            .get(&turn_id)
            .map_or_else(Vec::new, |state| state.contents.values().cloned().collect())
    }

    /// Resolves one diff identity to its body, to the descriptor that superseded it, or to
    /// nothing.
    pub fn lookup(&self, turn_id: TurnId, diff_id: &DiffId) -> RuntimeDiffLookup {
        let Some(state) = self.turns.get(&turn_id) else {
            return RuntimeDiffLookup::Missing;
        };
        if let Some(record) = state.contents.get(diff_id) {
            return RuntimeDiffLookup::Found(record.clone());
        }
        state
            .superseded
            .get(diff_id)
            .map(|superseded| superseded.current.clone())
            .map_or(RuntimeDiffLookup::Missing, RuntimeDiffLookup::Superseded)
    }

    /// Drops every body captured for a settled turn.
    pub fn clear_turn(&mut self, turn_id: TurnId) {
        self.turns.remove(&turn_id);
    }

    #[cfg(test)]
    pub fn slot_count(&self, turn_id: TurnId) -> usize {
        self.turns
            .get(&turn_id)
            .map_or(0, |state| state.current_by_slot.len())
    }
}

fn install_captured_diff(
    state: &mut ActiveCapturedDiffs,
    thread_id: ThreadId,
    turn_id: TurnId,
    slot: CapturedDiffSlot,
    descriptor: CapturedDiffDescriptor,
    record: CapturedDiffRecord,
) {
    if let Some(previous) = state
        .current_by_slot
        .insert(slot.clone(), descriptor.clone())
        && previous.id != descriptor.id
    {
        if !state
            .current_by_slot
            .values()
            .any(|current| current.id == previous.id)
        {
            state.contents.remove(&previous.id);
            debug!(
                %thread_id,
                %turn_id,
                ?slot,
                superseded_diff_id = %previous.id,
                current_diff_id = %descriptor.id,
                "dropped superseded captured diff body"
            );
        }
        // Keep only the immediately superseded identity for each logical diff slot. Item-owned
        // and turn-level diffs for the same path are independent authorities.
        state
            .superseded
            .retain(|_, superseded| superseded.slot != slot);
        state.superseded.insert(
            previous.id,
            SupersededCapturedDiff {
                slot,
                current: descriptor.clone(),
            },
        );
    }
    state.contents.insert(record.id.clone(), record);
}

fn reconcile_item_captured_diffs(
    state: &mut ActiveCapturedDiffs,
    thread_id: ThreadId,
    turn_id: TurnId,
    item_id: ItemId,
    captures: Vec<(CapturedDiffSlot, CapturedDiffDescriptor, CapturedDiffRecord)>,
) {
    let new_slots: std::collections::HashSet<_> =
        captures.iter().map(|(slot, _, _)| slot.clone()).collect();
    let omitted: Vec<_> = state
        .current_by_slot
        .keys()
        .filter(|slot| {
            matches!(slot, CapturedDiffSlot::Item { item_id: owner, .. } if *owner == item_id)
                && !new_slots.contains(*slot)
        })
        .cloned()
        .collect();
    for slot in omitted {
        if let Some(previous) = state.current_by_slot.remove(&slot)
            && !state
                .current_by_slot
                .values()
                .any(|current| current.id == previous.id)
        {
            state.contents.remove(&previous.id);
            debug!(
                %thread_id,
                %turn_id,
                ?slot,
                removed_diff_id = %previous.id,
                "dropped captured diff body omitted by replacement item"
            );
        }
        state
            .superseded
            .retain(|_, superseded| superseded.slot != slot);
    }
    for (slot, descriptor, record) in captures {
        install_captured_diff(state, thread_id, turn_id, slot, descriptor, record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_unified_text_on_different_paths_has_independent_identity() {
        let mut state = ActiveCapturedDiffs::default();
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let (first, first_record) = giskard_core::capture_unified_diff(
            "src/first.rs".into(),
            giskard_core::FileChangeKind::Modified,
            None,
            "@@ -1 +1 @@\n-old\n+same".into(),
        );
        let (second, second_record) = giskard_core::capture_unified_diff(
            "src/second.rs".into(),
            giskard_core::FileChangeKind::Modified,
            None,
            "@@ -1 +1 @@\n-old\n+same".into(),
        );
        assert_ne!(first.id, second.id);
        install_captured_diff(
            &mut state,
            thread,
            turn,
            CapturedDiffSlot::Turn(first.path.clone()),
            first.clone(),
            first_record,
        );
        install_captured_diff(
            &mut state,
            thread,
            turn,
            CapturedDiffSlot::Turn(second.path.clone()),
            second.clone(),
            second_record,
        );

        let (replacement, replacement_record) = giskard_core::capture_unified_diff(
            "src/first.rs".into(),
            giskard_core::FileChangeKind::Modified,
            None,
            "@@ -1 +1 @@\n-old\n+changed".into(),
        );
        install_captured_diff(
            &mut state,
            thread,
            turn,
            CapturedDiffSlot::Turn(replacement.path.clone()),
            replacement,
            replacement_record,
        );

        assert!(state.contents.contains_key(&second.id));
        assert!(!state.superseded.contains_key(&second.id));
        assert_eq!(
            state.current_by_slot[&CapturedDiffSlot::Turn(second.path.clone())].id,
            second.id
        );
    }

    #[test]
    fn item_and_turn_diffs_for_the_same_path_have_independent_authority() {
        let mut state = ActiveCapturedDiffs::default();
        let thread = ThreadId::new();
        let turn_id = TurnId::new();
        let path = std::path::PathBuf::from("src/main.rs");
        let item_id = ItemId::new();
        let (item, item_record) = giskard_core::capture_unified_diff(
            path.clone(),
            giskard_core::FileChangeKind::Modified,
            Some(item_id),
            "item body".into(),
        );
        let structured = giskard_core::FileDiff {
            path: path.clone(),
            change: giskard_core::FileChangeKind::Modified,
            old_text: Some("old".into()),
            new_text: Some("turn body".into()),
            hunks: Vec::new(),
            binary: false,
            captured: None,
        };
        let (turn, turn_record) = giskard_core::capture_structured_diff(structured);
        let turn = turn.captured.unwrap();

        install_captured_diff(
            &mut state,
            thread,
            turn_id,
            CapturedDiffSlot::Item {
                item_id,
                path: path.clone(),
                occurrence: 0,
            },
            item.clone(),
            item_record,
        );
        install_captured_diff(
            &mut state,
            thread,
            turn_id,
            CapturedDiffSlot::Turn(path.clone()),
            turn.clone(),
            turn_record,
        );

        assert!(state.contents.contains_key(&item.id));
        assert!(state.contents.contains_key(&turn.id));
        assert!(state.superseded.is_empty());
        assert_eq!(
            state.current_by_slot[&CapturedDiffSlot::Item {
                item_id,
                path: path.clone(),
                occurrence: 0,
            }]
                .id,
            item.id
        );
        assert_eq!(
            state.current_by_slot[&CapturedDiffSlot::Turn(path)].id,
            turn.id
        );
    }
}
