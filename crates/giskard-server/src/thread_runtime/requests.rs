//! The per-thread request ledger: the approval and server requests a thread has outstanding, and
//! the revisioned record each one publishes to browsers.
//!
//! The ledger knows nothing about locks, authorities, claims-in-flight, or the cross-thread
//! overview. It records transitions and reports the wire state they produce; the protocol error
//! text for a refused transition belongs to the dispatch layer, so refusals come back as reasons.

use std::collections::HashMap;

use giskard_core::approval::{ApprovalDecision, ApprovalRequest};
use giskard_core::ids::{ApprovalId, ServerRequestId, ThreadId, TurnId};
use giskard_core::server_request::{ServerRequest, ServerRequestResponse};
use giskard_proto::{
    OutstandingRequest, RequestKind, RequestPayload as WireRequestPayload,
    RequestResolution as WireRequestResolution, RequestState as WireRequestState,
    RequestStatus as WireRequestStatus, WireApprovalRequest,
};
use tracing::{debug, warn};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RuntimeRequestId {
    Approval(ApprovalId),
    Server(ServerRequestId),
}

impl RuntimeRequestId {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Approval(id) => &id.0,
            Self::Server(id) => &id.0,
        }
    }
}

#[derive(Clone, PartialEq)]
pub enum RequestPayload {
    Approval(ApprovalRequest),
    Server(ServerRequest),
}

#[derive(Clone, Debug, PartialEq)]
enum RequestStatus {
    Pending,
    Responding { claim: u64, harness_resolved: bool },
    Resolved(RequestResolution),
}

#[derive(Clone)]
struct RequestRecord {
    turn_id: Option<TurnId>,
    payload: RequestPayload,
    status: RequestStatus,
    revision: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RequestResolution {
    Approval(ApprovalDecision),
    Server(ServerRequestResponse),
}

/// Why the ledger refused to hand out a claim.
pub enum ClaimRejection {
    Missing,
    NotPending,
}

/// Why the ledger refused to resolve a claimed request.
pub enum CommitRejection {
    Missing,
    StaleClaim,
    KindMismatch,
}

#[derive(Default)]
pub struct RequestLedger {
    records: HashMap<RuntimeRequestId, RequestRecord>,
}

impl RequestLedger {
    /// Records a request the provider delivered. `true` when the record changed and its new
    /// revision must be published.
    pub fn register(
        &mut self,
        request_id: RuntimeRequestId,
        turn_id: Option<TurnId>,
        payload: RequestPayload,
    ) -> bool {
        use std::collections::hash_map::Entry;

        match self.records.entry(request_id) {
            Entry::Vacant(record) => {
                record.insert(RequestRecord {
                    turn_id,
                    payload,
                    status: RequestStatus::Pending,
                    revision: 1,
                });
                true
            }
            Entry::Occupied(mut record) => {
                // Duplicate provider delivery may refresh bounded request metadata, but it must not
                // resurrect a responding or resolved request. An identical redelivery is not a new
                // revision, and a changed one must take a new revision rather than republish different
                // content under the revision a client already accepted.
                let record = record.get_mut();
                if record.payload == payload {
                    return false;
                }
                record.payload = payload;
                record.revision = record.revision.saturating_add(1);
                true
            }
        }
    }

    /// Applies a harness-side resolution of a server request. `true` when the record changed.
    pub fn resolve_from_harness(
        &mut self,
        thread_id: ThreadId,
        request_id: &ServerRequestId,
    ) -> bool {
        let Some(record) = self
            .records
            .get_mut(&RuntimeRequestId::Server(request_id.clone()))
        else {
            warn!(
                %thread_id,
                request_id = %request_id.0,
                "harness resolved a server request with no runtime record"
            );
            return false;
        };
        match &mut record.status {
            RequestStatus::Responding {
                harness_resolved, ..
            } => {
                if !*harness_resolved {
                    debug!(
                        %thread_id,
                        request_id = %request_id.0,
                        "harness resolved a server request while a claim is in flight; deferring to the claimant"
                    );
                }
                *harness_resolved = true;
                return false;
            }
            RequestStatus::Resolved(_) => return false,
            RequestStatus::Pending => {}
        }
        debug!(
            %thread_id,
            request_id = %request_id.0,
            "synthesizing runtime resolution from a harness-resolved server request"
        );
        record.status = RequestStatus::Resolved(RequestResolution::Server(
            ServerRequestResponse::result(serde_json::Value::Null),
        ));
        record.revision = record.revision.saturating_add(1);
        true
    }

    /// The replacement state of one record.
    pub fn state(
        &self,
        thread_id: ThreadId,
        request_id: &RuntimeRequestId,
    ) -> Option<WireRequestState> {
        self.records
            .get(request_id)
            .map(|record| wire_request_state(thread_id, record))
    }

    /// The replacement state of every record.
    pub fn states(&self, thread_id: ThreadId) -> Vec<WireRequestState> {
        self.records
            .values()
            .map(|record| wire_request_state(thread_id, record))
            .collect()
    }

    /// The summary projection: every request still awaiting a resolution, ordered by id.
    pub fn outstanding(&self) -> Vec<OutstandingRequest> {
        let mut outstanding_requests = self
            .records
            .iter()
            .filter_map(|(id, record)| {
                let responding = matches!(record.status, RequestStatus::Responding { .. });
                matches!(
                    record.status,
                    RequestStatus::Pending | RequestStatus::Responding { .. }
                )
                .then(|| OutstandingRequest {
                    request_id: id.as_str().to_string(),
                    kind: match id {
                        RuntimeRequestId::Approval(_) => RequestKind::Approval,
                        RuntimeRequestId::Server(_) => RequestKind::Server,
                    },
                    responding,
                })
            })
            .collect::<Vec<_>>();
        outstanding_requests.sort_by(|left, right| left.request_id.cmp(&right.request_id));
        outstanding_requests
    }

    /// Drops the resolved records belonging to a settled turn. `turn_id` is compared as recorded,
    /// so a settle carrying no completed turn prunes the turn-less resolved records.
    pub fn prune_resolved(&mut self, turn_id: Option<TurnId>) {
        self.records.retain(|_, record| {
            !(matches!(record.status, RequestStatus::Resolved(_)) && record.turn_id == turn_id)
        });
    }

    /// Moves a pending record to responding under a fresh claim id.
    pub fn claim(
        &mut self,
        thread_id: ThreadId,
        request_id: &RuntimeRequestId,
    ) -> Result<(u64, WireRequestState), ClaimRejection> {
        let record = self
            .records
            .get_mut(request_id)
            .ok_or(ClaimRejection::Missing)?;
        if record.status != RequestStatus::Pending {
            return Err(ClaimRejection::NotPending);
        }
        let claim_id = next_claim_id();
        record.status = RequestStatus::Responding {
            claim: claim_id,
            harness_resolved: false,
        };
        record.revision = record.revision.saturating_add(1);
        Ok((claim_id, wire_request_state(thread_id, record)))
    }

    /// Resolves a record held by `claim_id`. The record is untouched unless both the claim and the
    /// resolution kind match.
    pub fn commit(
        &mut self,
        thread_id: ThreadId,
        request_id: &RuntimeRequestId,
        claim_id: u64,
        resolution: RequestResolution,
    ) -> Result<WireRequestState, CommitRejection> {
        let record = self
            .records
            .get_mut(request_id)
            .ok_or(CommitRejection::Missing)?;
        if !matches!(
            record.status,
            RequestStatus::Responding { claim, .. } if claim == claim_id
        ) {
            return Err(CommitRejection::StaleClaim);
        }
        match (&record.payload, &resolution) {
            (RequestPayload::Approval(_), RequestResolution::Approval(_))
            | (RequestPayload::Server(_), RequestResolution::Server(_)) => {}
            _ => return Err(CommitRejection::KindMismatch),
        }
        record.status = RequestStatus::Resolved(resolution);
        record.revision = record.revision.saturating_add(1);
        Ok(wire_request_state(thread_id, record))
    }

    /// Returns a record held by `claim_id` to pending, or to the resolution the harness reported
    /// while the claim was in flight. `None` when no such record is held by this claim.
    pub fn rollback(
        &mut self,
        thread_id: ThreadId,
        request_id: &RuntimeRequestId,
        claim_id: u64,
    ) -> Option<WireRequestState> {
        let record = self.records.get_mut(request_id)?;
        let RequestStatus::Responding {
            claim,
            harness_resolved,
        } = record.status
        else {
            return None;
        };
        if claim != claim_id {
            return None;
        }
        record.status = if harness_resolved {
            RequestStatus::Resolved(RequestResolution::Server(ServerRequestResponse::result(
                serde_json::Value::Null,
            )))
        } else {
            RequestStatus::Pending
        };
        record.revision = record.revision.saturating_add(1);
        Some(wire_request_state(thread_id, record))
    }

    #[cfg(test)]
    pub fn resolution(&self, request_id: &RuntimeRequestId) -> Option<RequestResolution> {
        match &self.records.get(request_id)?.status {
            RequestStatus::Resolved(resolution) => Some(resolution.clone()),
            RequestStatus::Pending | RequestStatus::Responding { .. } => None,
        }
    }
}

fn wire_request_state(thread_id: ThreadId, record: &RequestRecord) -> WireRequestState {
    let (request_id, payload) = match &record.payload {
        RequestPayload::Approval(request) => (
            request.id.0.clone(),
            WireRequestPayload::Approval {
                request: WireApprovalRequest::from(request.clone()),
            },
        ),
        RequestPayload::Server(request) => (
            request.id.0.clone(),
            WireRequestPayload::Server {
                request: request.clone(),
            },
        ),
    };
    let status = match &record.status {
        RequestStatus::Pending => WireRequestStatus::Pending,
        RequestStatus::Responding { .. } => WireRequestStatus::Responding,
        RequestStatus::Resolved(RequestResolution::Approval(decision)) => {
            WireRequestStatus::Resolved {
                resolution: WireRequestResolution::Approval {
                    decision: decision.clone(),
                },
            }
        }
        RequestStatus::Resolved(RequestResolution::Server(_)) => WireRequestStatus::Resolved {
            resolution: WireRequestResolution::Server,
        },
    };
    WireRequestState {
        thread_id,
        request_id,
        revision: record.revision,
        payload,
        status,
    }
}

fn next_claim_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed).max(1)
}
