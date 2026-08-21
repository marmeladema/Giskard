use std::fmt;
use std::time::Instant;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Serialize;
use tracing::debug;

use giskard_persist::{HistoryCursor, HistorySnapshotKind};
use giskard_proto::{
    BootstrapHistory, BootstrapSection, BootstrapSectionDescriptor, ResyncReason,
    ThreadBootstrapFrame, ThreadBootstrapPayload, ThreadHistoryCursor,
};

use giskard_core::ids::{ProjectId, ThreadId, TurnId};

use crate::AppState;
use crate::hub::{ClientId, SubscriptionGeneration};
use crate::thread_runtime::{JournalCoverage, JournalPinError};

/// Keep deterministic headroom for base64 expansion and the JSON frame envelope. A raw chunk is
/// half the physical target: 32 KiB becomes 43,692 base64 bytes before the bounded envelope.
const BOOTSTRAP_FRAME_TARGET_BYTES: usize = 64 * 1024;
const BOOTSTRAP_RAW_CHUNK_BYTES: usize = BOOTSTRAP_FRAME_TARGET_BYTES / 2;
const BOOTSTRAP_RETRY_AFTER_MS: u64 = 500;

#[derive(Debug)]
pub(crate) enum BootstrapBuildError {
    Retryable {
        reason: ResyncReason,
        retry_after_ms: u64,
        detail: String,
    },
    Cancelled(String),
    Failed(String),
}

impl BootstrapBuildError {
    fn from_pin(error: JournalPinError, action: &str) -> Self {
        let detail = format!("{action}: {error}");
        match error {
            JournalPinError::PreparationRejected => Self::Cancelled(detail),
            JournalPinError::ReservationCapacity
            | JournalPinError::SuffixTooLarge
            | JournalPinError::SuffixUnavailable
            | JournalPinError::Released
            | JournalPinError::Exhausted => Self::Retryable {
                reason: ResyncReason::BootstrapCapacity,
                retry_after_ms: BOOTSTRAP_RETRY_AFTER_MS,
                detail,
            },
        }
    }
}

impl From<String> for BootstrapBuildError {
    fn from(detail: String) -> Self {
        Self::Failed(detail)
    }
}

#[derive(Debug)]
pub(crate) enum BootstrapEncodeError {
    Serialize {
        section: BootstrapSection,
        source: serde_json::Error,
    },
    TooManyChunks {
        section: BootstrapSection,
        chunks: usize,
    },
}

impl fmt::Display for BootstrapEncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialize { section, source } => {
                write!(
                    formatter,
                    "failed to encode bootstrap section {section:?}: {source}"
                )
            }
            Self::TooManyChunks { section, chunks } => write!(
                formatter,
                "bootstrap section {section:?} requires {chunks} chunks, exceeding the protocol limit"
            ),
        }
    }
}

impl std::error::Error for BootstrapEncodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Serialize { source, .. } => Some(source),
            Self::TooManyChunks { .. } => None,
        }
    }
}

struct EncodedSection {
    section: BootstrapSection,
    bytes: Vec<u8>,
}

struct EncodedBootstrap {
    sections: Vec<EncodedSection>,
}

/// Encode one logical bootstrap through the only supported physical transaction path.
///
/// Sections are independently JSON encoded and split, allowing history records larger than one
/// socket frame without making the browser maintain a separate large-bootstrap apply path.
fn encode_bootstrap(
    payload: &ThreadBootstrapPayload,
) -> Result<EncodedBootstrap, BootstrapEncodeError> {
    let sections = vec![
        encode_section(BootstrapSection::Metadata, &payload.metadata)?,
        encode_section(BootstrapSection::History, &payload.history)?,
        encode_section(BootstrapSection::LiveTurn, &payload.live_turn)?,
        encode_section(BootstrapSection::OrderedSuffix, &payload.ordered_suffix)?,
        encode_section(BootstrapSection::FinalRuntime, &payload.final_runtime)?,
        encode_section(BootstrapSection::Notices, &payload.notices)?,
    ];

    let encoded = EncodedBootstrap { sections };
    // Validate every count before any frame is admitted. Chunk indices can then use the matching
    // bounded `usize -> u32` conversion while streaming without another fallible pass.
    encoded.descriptors()?;
    Ok(encoded)
}

impl EncodedBootstrap {
    fn descriptors(&self) -> Result<Vec<BootstrapSectionDescriptor>, BootstrapEncodeError> {
        self.sections
            .iter()
            .map(|encoded| {
                let chunks = encoded.bytes.len().div_ceil(BOOTSTRAP_RAW_CHUNK_BYTES);
                let chunk_count =
                    u32::try_from(chunks).map_err(|_| BootstrapEncodeError::TooManyChunks {
                        section: encoded.section,
                        chunks,
                    })?;
                Ok(BootstrapSectionDescriptor {
                    section: encoded.section,
                    encoded_bytes: encoded.bytes.len() as u64,
                    chunk_count,
                })
            })
            .collect()
    }

    fn frame_count(&self) -> usize {
        self.sections
            .iter()
            .map(|section| section.bytes.len().div_ceil(BOOTSTRAP_RAW_CHUNK_BYTES))
            .sum::<usize>()
            .saturating_add(2)
    }

    fn encoded_section_bytes(&self) -> u64 {
        self.sections
            .iter()
            .map(|section| section.bytes.len() as u64)
            .sum()
    }
}

pub(crate) async fn build_and_send(
    state: &AppState,
    client_id: ClientId,
    project_id: ProjectId,
    thread_id: ThreadId,
    generation: SubscriptionGeneration,
    since: Option<ThreadHistoryCursor>,
    history_limit: usize,
) -> Result<(), BootstrapBuildError> {
    let started = Instant::now();
    debug!(
        %project_id,
        %thread_id,
        %client_id,
        subscription_generation = generation,
        action = "build_thread_bootstrap",
        "building staged thread bootstrap"
    );
    let live_cut = state
        .runtime
        .capture_live_cut(thread_id)
        .map_err(|error| BootstrapBuildError::from_pin(error, "failed to pin runtime journal"))?;

    let (history, history_turn_ids, history_amendment_sequence) = load_bootstrap_history(
        state,
        project_id,
        thread_id,
        since,
        live_cut.live.as_ref().map(|live| live.turn_id),
        history_limit,
    )
    .await?;

    let metadata = state
        .thread_metadata
        .recompute_aggregates(project_id, thread_id)
        .await
        .map_err(|error| format!("failed to repair thread aggregates: {error}"))?
        .into_current()
        .map(|thread| crate::thread_metadata::ThreadMetadataService::metadata(&thread))
        .ok_or_else(|| "thread disappeared while its bootstrap was being built".to_string())?;

    // The final journal cut and the Hub's commit barrier are one atomic handoff under the
    // documented runtime -> subscription lock order. An event cannot land after the cut while the
    // subscription is still in `Bootstrapping`, where it would otherwise be absent from both the
    // suffix and the post-cut queue.
    let final_cut = state
        .runtime
        .finalize_bootstrap_cut_with(&live_cut.pin, |through_seq| {
            state
                .hub
                .prepare_bootstrap_commit(thread_id, client_id, generation, through_seq)
        })
        .map_err(|error| {
            BootstrapBuildError::from_pin(error, "failed to materialize pinned runtime suffix")
        })?;
    // The suffix is now materialized and the Hub commit barrier owns all later events. Keeping the
    // journal pin while frames wait for socket admission would retain history with no correctness
    // benefit and delay runtime retirement for a cancelled bootstrap.
    let represented_through = live_cut.represented_through;
    let live_at_cut = live_cut.live;
    drop(live_cut.pin);
    debug!(
        %project_id,
        %thread_id,
        %client_id,
        subscription_generation = generation,
        event_seq_start = represented_through.saturating_add(1),
        event_seq_end = final_cut.through_seq,
        suffix_entries = final_cut.suffix.len(),
        action = "cut_thread_bootstrap",
        "captured final staged bootstrap cut"
    );

    let live_turn = live_at_cut.filter(|live| !history_turn_ids.contains(&live.turn_id));
    let ordered_suffix = final_cut
        .suffix
        .into_iter()
        .filter(|entry| match entry.coverage {
            Some(JournalCoverage::Turn(turn_id)) => !history_turn_ids.contains(&turn_id),
            Some(JournalCoverage::Amendment(sequence)) => sequence > history_amendment_sequence,
            None => true,
        })
        .map(|entry| entry.event)
        .collect();
    let notices = state.notices.snapshot(thread_id);
    let payload = ThreadBootstrapPayload {
        metadata,
        history,
        live_turn,
        ordered_suffix,
        final_runtime: final_cut.final_runtime,
        notices,
    };
    let encoded = encode_bootstrap(&payload)
        .map_err(|error| BootstrapBuildError::Failed(error.to_string()))?;
    let frame_count = encoded.frame_count();
    let encoded_section_bytes = encoded.encoded_section_bytes();
    send_frame(
        state,
        thread_id,
        client_id,
        generation,
        ThreadBootstrapFrame::Start {
            sections: encoded
                .descriptors()
                .map_err(|error| BootstrapBuildError::Failed(error.to_string()))?,
        },
    )
    .await?;
    for section in &encoded.sections {
        for (index, chunk) in section.bytes.chunks(BOOTSTRAP_RAW_CHUNK_BYTES).enumerate() {
            // `encode_bootstrap` validated the total chunk count against `u32` before sending the
            // transaction, so every index in this loop is representable.
            let index = index as u32;
            send_frame(
                state,
                thread_id,
                client_id,
                generation,
                ThreadBootstrapFrame::Chunk {
                    section: section.section,
                    index,
                    payload_base64: BASE64.encode(chunk),
                },
            )
            .await?;
        }
    }
    send_frame(
        state,
        thread_id,
        client_id,
        generation,
        ThreadBootstrapFrame::Commit,
    )
    .await?;
    if !state
        .hub
        .finish_bootstrap(thread_id, client_id, generation)
        .await
    {
        return Err(BootstrapBuildError::Cancelled(
            "subscription was superseded while bootstrap frames were sent".into(),
        ));
    }
    debug!(
        %project_id,
        %thread_id,
        %client_id,
        subscription_generation = generation,
        metadata_revision = payload.metadata.revision,
        event_seq_end = payload.final_runtime.through_seq,
        frame_count,
        encoded_section_bytes,
        elapsed_ms = started.elapsed().as_millis(),
        action = "commit_thread_bootstrap",
        "committed staged thread bootstrap"
    );
    Ok(())
}

async fn send_frame(
    state: &AppState,
    thread_id: ThreadId,
    client_id: ClientId,
    generation: SubscriptionGeneration,
    frame: ThreadBootstrapFrame,
) -> Result<(), BootstrapBuildError> {
    if let Err(error) = state
        .hub
        .send_bootstrap_frame(thread_id, client_id, generation, frame)
        .await
    {
        if !state
            .hub
            .bootstrap_is_committing(thread_id, client_id, generation)
        {
            return Err(BootstrapBuildError::Cancelled(format!(
                "subscription was superseded while a bootstrap frame awaited admission: {error:?}"
            )));
        }
        return Err(BootstrapBuildError::Retryable {
            reason: ResyncReason::BootstrapCapacity,
            retry_after_ms: BOOTSTRAP_RETRY_AFTER_MS,
            detail: format!("failed to enqueue bootstrap frame: {error:?}"),
        });
    }
    Ok(())
}

async fn load_bootstrap_history(
    state: &AppState,
    project_id: ProjectId,
    thread_id: ThreadId,
    since: Option<ThreadHistoryCursor>,
    required_turn: Option<TurnId>,
    history_limit: usize,
) -> Result<(BootstrapHistory, std::collections::HashSet<TurnId>, u64), String> {
    let requested = since.clone().map(persist_history_cursor);
    let snapshot = state
        .store
        .load_history_snapshot(
            project_id,
            thread_id,
            requested,
            required_turn,
            history_limit,
        )
        .await
        .map_err(|error| format!("failed to load coherent bootstrap history: {error}"))?;
    let cursor = wire_history_cursor(snapshot.cursor.clone());
    let amendment_sequence = cursor.amendment_sequence;
    let ids = snapshot.turns.iter().map(|turn| turn.id).collect();
    let turns = snapshot.turns.into_iter().map(Into::into).collect();
    let history = match snapshot.kind {
        HistorySnapshotKind::FullPage => BootstrapHistory::FullPage {
            cursor,
            turns,
            has_more: snapshot.has_more,
        },
        HistorySnapshotKind::Delta { .. } => {
            let after = since.ok_or_else(|| {
                "history snapshot selected a delta without a requested cursor".to_string()
            })?;
            BootstrapHistory::Delta {
                after,
                cursor,
                turns,
            }
        }
        HistorySnapshotKind::CursorReset { .. } => {
            let requested_after = since.ok_or_else(|| {
                "history snapshot selected a reset without a requested cursor".to_string()
            })?;
            BootstrapHistory::CursorReset {
                requested_after,
                cursor,
                turns,
                has_more: snapshot.has_more,
            }
        }
    };
    Ok((history, ids, amendment_sequence))
}

fn persist_history_cursor(cursor: ThreadHistoryCursor) -> HistoryCursor {
    HistoryCursor {
        newest_turn_id: cursor.newest_turn_id,
        server_epoch: cursor.server_epoch,
        amendment_sequence: cursor.amendment_sequence,
    }
}

fn wire_history_cursor(cursor: HistoryCursor) -> ThreadHistoryCursor {
    ThreadHistoryCursor {
        newest_turn_id: cursor.newest_turn_id,
        server_epoch: cursor.server_epoch,
        amendment_sequence: cursor.amendment_sequence,
    }
}

fn encode_section<T: Serialize>(
    section: BootstrapSection,
    value: &T,
) -> Result<EncodedSection, BootstrapEncodeError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|source| BootstrapEncodeError::Serialize { section, source })?;
    Ok(EncodedSection { section, bytes })
}

#[cfg(test)]
mod tests {
    use giskard_core::ids::ThreadId;
    use giskard_core::model::ModelRef;
    use giskard_core::token::TokenLedger;
    use giskard_core::turn::{Mode, PermissionPreset};
    use giskard_proto::{
        BootstrapHistory, RuntimeTurnState, ThreadFinalRuntime, ThreadMetadata,
        ThreadNoticeSnapshot, ThreadTaskSnapshot,
    };

    use super::*;

    fn collect_frames(encoded: &EncodedBootstrap) -> Vec<ThreadBootstrapFrame> {
        let mut frames = vec![ThreadBootstrapFrame::Start {
            sections: encoded.descriptors().unwrap(),
        }];
        for section in &encoded.sections {
            for (index, chunk) in section.bytes.chunks(BOOTSTRAP_RAW_CHUNK_BYTES).enumerate() {
                frames.push(ThreadBootstrapFrame::Chunk {
                    section: section.section,
                    index: index as u32,
                    payload_base64: BASE64.encode(chunk),
                });
            }
        }
        frames.push(ThreadBootstrapFrame::Commit);
        frames
    }

    fn payload(title: String) -> ThreadBootstrapPayload {
        let thread_id = ThreadId::new();
        ThreadBootstrapPayload {
            metadata: ThreadMetadata {
                thread_id,
                revision: 1,
                title,
                mode: Mode::Build,
                current_model: ModelRef {
                    provider: "test".into(),
                    model: "test".into(),
                    reasoning_effort: None,
                },
                context_window: 128_000,
                permission_preset: PermissionPreset::AskFirst,
                tokens: TokenLedger::default(),
            },
            history: BootstrapHistory::FullPage {
                cursor: ThreadHistoryCursor {
                    newest_turn_id: None,
                    server_epoch: "test-epoch".into(),
                    amendment_sequence: 0,
                },
                turns: Vec::new(),
                has_more: false,
            },
            live_turn: None,
            ordered_suffix: Vec::new(),
            final_runtime: ThreadFinalRuntime {
                through_seq: 0,
                turn_state: RuntimeTurnState::Idle,
                history_recovery: None,
                tasks: ThreadTaskSnapshot {
                    thread_id,
                    revision: 0,
                    tasks: Vec::new(),
                },
                requests: Vec::new(),
            },
            notices: ThreadNoticeSnapshot {
                thread_id,
                revision: 0,
                notices: Vec::new(),
            },
        }
    }

    #[test]
    fn one_chunk_sections_use_the_same_transaction_path() {
        let encoded = encode_bootstrap(&payload("small".into())).unwrap();
        let frames = collect_frames(&encoded);
        assert!(matches!(
            frames.first(),
            Some(ThreadBootstrapFrame::Start { .. })
        ));
        assert!(matches!(frames.last(), Some(ThreadBootstrapFrame::Commit)));
        assert_eq!(
            frames
                .iter()
                .filter(|frame| matches!(frame, ThreadBootstrapFrame::Chunk { .. }))
                .count(),
            6
        );
    }

    #[test]
    fn large_section_is_chunked_before_commit() {
        let encoded =
            encode_bootstrap(&payload("x".repeat(BOOTSTRAP_RAW_CHUNK_BYTES * 2))).unwrap();
        let frames = collect_frames(&encoded);
        let metadata_chunks = frames
            .iter()
            .filter(|frame| {
                matches!(
                    frame,
                    ThreadBootstrapFrame::Chunk {
                        section: BootstrapSection::Metadata,
                        ..
                    }
                )
            })
            .count();
        assert!(metadata_chunks >= 3);
        assert!(matches!(frames.last(), Some(ThreadBootstrapFrame::Commit)));
    }

    #[test]
    fn maximum_chunk_stays_below_the_physical_frame_target() {
        let thread_id = ThreadId::new();
        let message = giskard_proto::ServerMessage::ThreadBootstrap {
            thread_id,
            subscription_generation: u64::MAX,
            frame: ThreadBootstrapFrame::Chunk {
                section: BootstrapSection::OrderedSuffix,
                index: u32::MAX,
                payload_base64: BASE64.encode(vec![b'x'; BOOTSTRAP_RAW_CHUNK_BYTES]),
            },
        };
        let encoded = serde_json::to_vec(&message).unwrap();
        assert!(
            encoded.len() < BOOTSTRAP_FRAME_TARGET_BYTES,
            "maximum bootstrap frame encoded to {} bytes",
            encoded.len()
        );
    }

    #[test]
    fn journal_capacity_failures_are_retryable_bootstrap_failures() {
        for error in [
            JournalPinError::ReservationCapacity,
            JournalPinError::SuffixTooLarge,
            JournalPinError::SuffixUnavailable,
            JournalPinError::Released,
            JournalPinError::Exhausted,
        ] {
            assert!(matches!(
                BootstrapBuildError::from_pin(error, "test"),
                BootstrapBuildError::Retryable {
                    reason: ResyncReason::BootstrapCapacity,
                    retry_after_ms: BOOTSTRAP_RETRY_AFTER_MS,
                    ..
                }
            ));
        }
    }

    #[test]
    fn superseded_bootstrap_is_cancelled_instead_of_retried() {
        assert!(matches!(
            BootstrapBuildError::from_pin(JournalPinError::PreparationRejected, "test"),
            BootstrapBuildError::Cancelled(_)
        ));
    }
}
