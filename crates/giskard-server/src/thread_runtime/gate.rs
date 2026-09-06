//! The per-thread turn gate: who owns the thread's in-flight turn, and the lifecycle clock that
//! numbers each ownership.
//!
//! The gate knows nothing about locks, authorities, permits, or the cross-thread overview. It
//! answers whether a turn may start, records the owner's acknowledgement and persistence
//! outcome, and projects the turn half of the runtime summary.

use std::time::Instant;

use giskard_core::error::HarnessError;
use giskard_core::ids::{ProjectId, ThreadId, TurnId};
use giskard_core::turn::{Turn, TurnMode, TurnModel};
use giskard_proto::RuntimeTurnState;
use tracing::warn;

use crate::log_fields::display_opt;

#[derive(Clone)]
pub struct ActiveTurnOwner {
    pub(crate) reservation: TurnReservation,
    pub(crate) acknowledged_turn: Option<TurnId>,
    pub(crate) reserved_at: Instant,
    pub(crate) persistence_blocked: Option<(Turn, String)>,
}

#[derive(Clone)]
pub struct TurnReservation {
    pub project_id: ProjectId,
    pub harness_thread_id: String,
    pub mode: TurnMode,
    pub model: TurnModel,
    pub context_kind: &'static str,
}

#[derive(Default)]
pub struct TurnGate {
    active: Option<ActiveTurnOwner>,
    lifecycle_revision: u64,
}

impl TurnGate {
    /// Installs `reservation` as the thread's turn owner, or refuses because one already owns it.
    /// A successful reservation advances the lifecycle clock.
    pub fn reserve(
        &mut self,
        thread_id: ThreadId,
        reservation: TurnReservation,
    ) -> Result<(), HarnessError> {
        if let Some(existing) = &self.active {
            warn!(
                %thread_id,
                owner_project_id = %existing.reservation.project_id,
                owner_turn_id = display_opt(existing.acknowledged_turn),
                owner_harness_thread_id = %existing.reservation.harness_thread_id,
                owner_context_kind = existing.reservation.context_kind,
                owner_mode = ?existing.reservation.mode,
                owner_model = ?existing.reservation.model,
                owner_elapsed_ms = existing.reserved_at.elapsed().as_millis(),
                "rejecting turn start because thread runtime is already active"
            );
            return Err(HarnessError::ThreadBusy { thread: thread_id });
        }
        self.active = Some(ActiveTurnOwner {
            reservation,
            acknowledged_turn: None,
            reserved_at: Instant::now(),
            persistence_blocked: None,
        });
        self.lifecycle_revision = self.lifecycle_revision.saturating_add(1);
        Ok(())
    }

    /// Reports whether an admitted turn currently owns the thread.
    pub fn is_active(&self) -> bool {
        self.active.is_some()
    }

    /// The clock a restore permit compares against to prove no newer lifecycle superseded it.
    pub fn lifecycle_revision(&self) -> u64 {
        self.lifecycle_revision
    }

    /// Adopts the harness's turn id. `false` when there is no owner; the caller warns.
    pub fn acknowledge(&mut self, turn_id: TurnId) -> bool {
        let Some(owner) = self.active.as_mut() else {
            return false;
        };
        owner.acknowledged_turn = Some(turn_id);
        true
    }

    /// Releases the owner and hands it back. The two release sites log different lines, so the
    /// gate reports the owner instead of logging.
    pub fn release(&mut self) -> Option<ActiveTurnOwner> {
        self.active.take()
    }

    /// Retains a completed turn whose persistence failed. `false` when there is no owner; the
    /// caller warns.
    pub fn block_on_persistence(&mut self, turn: Turn, error: String) -> bool {
        let Some(owner) = self.active.as_mut() else {
            return false;
        };
        owner.acknowledged_turn = Some(turn.id);
        owner.persistence_blocked = Some((turn, error));
        true
    }

    /// The owning project, for logs emitted while the turn is in flight.
    pub fn project_id(&self) -> Option<ProjectId> {
        self.active
            .as_ref()
            .map(|owner| owner.reservation.project_id)
    }

    /// The turn half of the thread's runtime summary.
    pub fn turn_state(&self) -> RuntimeTurnState {
        self.active
            .as_ref()
            .map_or(RuntimeTurnState::Idle, |owner| {
                if let Some((turn, error)) = &owner.persistence_blocked {
                    RuntimeTurnState::PersistenceBlocked {
                        turn_id: turn.id,
                        error: error.clone(),
                    }
                } else {
                    RuntimeTurnState::Active {
                        turn_id: owner.acknowledged_turn,
                    }
                }
            })
    }

    #[cfg(test)]
    pub fn blocked_turn(&self) -> Option<TurnId> {
        self.active
            .as_ref()?
            .persistence_blocked
            .as_ref()
            .map(|(turn, _)| turn.id)
    }
}
