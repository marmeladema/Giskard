use giskard_core::error::HarnessError;
use giskard_harness::DiscoveryTicket;
use tokio::sync::mpsc;

use crate::native_routes::CodexRouteAuthority;

/// Task-owned bounded admission for traffic-driven route discovery.
///
/// Route identity and ticket validity remain owned by `CodexRouteAuthority`; this helper owns only
/// the channel-facing state needed to preserve one pending ticket without blocking stdout.
pub(super) struct DiscoveryAdmission {
    submissions: mpsc::Sender<DiscoveryTicket>,
    pending: Option<DiscoveryTicket>,
    open: bool,
    failed: bool,
}

pub(super) enum Submission {
    Queued,
    Deferred(DiscoveryTicket),
    Closed(DiscoveryTicket),
}

impl DiscoveryAdmission {
    pub(super) fn new(submissions: mpsc::Sender<DiscoveryTicket>) -> Self {
        Self {
            submissions,
            pending: None,
            open: true,
            failed: false,
        }
    }

    pub(super) fn is_open(&self) -> bool {
        self.open
    }

    pub(super) fn failed(&self) -> bool {
        self.failed
    }

    pub(super) fn submit(&self, ticket: DiscoveryTicket) -> Submission {
        match self.submissions.try_send(ticket) {
            Ok(()) => Submission::Queued,
            Err(mpsc::error::TrySendError::Full(ticket)) => Submission::Deferred(ticket),
            Err(mpsc::error::TrySendError::Closed(ticket)) => Submission::Closed(ticket),
        }
    }

    pub(super) fn stage_pending(
        &mut self,
        routes: &CodexRouteAuthority,
    ) -> Result<(), HarnessError> {
        if self.pending.is_none() && self.open {
            self.pending = routes.take_pending_discovery()?;
        }
        Ok(())
    }

    pub(super) fn has_pending(&self) -> bool {
        self.open && self.pending.is_some()
    }

    pub(super) fn sender(&self) -> mpsc::Sender<DiscoveryTicket> {
        self.submissions.clone()
    }

    /// Return an owned sender whose `closed()` future observes an already-closed receiver.
    ///
    /// The actor keeps this clone outside its `select!` borrow so closure remains independently
    /// observable even while no ticket is pending and stdout polling is disabled.
    pub(super) fn closure_signal(&self) -> mpsc::Sender<DiscoveryTicket> {
        self.submissions.clone()
    }

    pub(super) fn send_pending(&mut self, permit: mpsc::OwnedPermit<DiscoveryTicket>) {
        if let Some(ticket) = self.pending.take() {
            permit.send(ticket);
        }
    }

    pub(super) fn take_pending(&mut self) -> Option<DiscoveryTicket> {
        self.pending.take()
    }

    pub(super) fn close_failed(&mut self) {
        self.open = false;
        self.failed = true;
        self.pending = None;
    }
}

#[cfg(test)]
mod tests {
    use giskard_core::ids::ThreadId;

    use super::*;

    #[tokio::test]
    async fn full_channel_defers_exact_ticket_until_capacity_is_reserved() {
        let routes = CodexRouteAuthority::default();
        let (tx, mut rx) = mpsc::channel(1);
        let mut admission = DiscoveryAdmission::new(tx);
        let first = routes
            .discover("native-a".into(), ThreadId::new())
            .unwrap()
            .ticket
            .unwrap();
        assert!(matches!(admission.submit(first), Submission::Queued));

        let second = routes
            .discover("native-b".into(), ThreadId::new())
            .unwrap()
            .ticket
            .unwrap();
        let Submission::Deferred(second) = admission.submit(second) else {
            panic!("a saturated bounded channel must return the exact ticket");
        };
        second.defer().unwrap();
        admission.stage_pending(&routes).unwrap();
        assert!(admission.has_pending());

        drop(rx.recv().await);
        let permit = admission.sender().reserve_owned().await.unwrap();
        admission.send_pending(permit);
        assert_eq!(rx.recv().await.unwrap().harness_thread_id(), "native-b");
        assert!(!admission.has_pending());
    }

    #[test]
    fn failed_admission_closes_and_discards_pending_state() {
        let routes = CodexRouteAuthority::default();
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let mut admission = DiscoveryAdmission::new(tx);
        let ticket = routes
            .discover("native-a".into(), ThreadId::new())
            .unwrap()
            .ticket
            .unwrap();
        assert!(matches!(admission.submit(ticket), Submission::Closed(_)));
        admission.close_failed();
        assert!(admission.failed());
        assert!(!admission.is_open());
        assert!(!admission.has_pending());
    }
}
