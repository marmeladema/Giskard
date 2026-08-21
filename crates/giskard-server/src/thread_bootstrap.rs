use std::fmt;
use std::time::Instant;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Serialize;
use tracing::debug;

use giskard_proto::{
    BootstrapHistory, BootstrapSection, BootstrapSectionDescriptor, ResyncReason,
    ThreadBootstrapFrame, ThreadBootstrapPayload,
};

use giskard_core::ids::{ProjectId, ThreadId, TurnId};

use crate::AppState;
use crate::hub::{ClientId, SubscriptionGeneration};
use crate::thread_runtime::{JournalCoverage, JournalPinError};

/// Raw JSON bytes per physical chunk. Base64 expansion and the small frame envelope keep each
/// encoded WebSocket message comfortably below 64 KiB.
const BOOTSTRAP_RAW_CHUNK_BYTES: usize = 48 * 1024;
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
    encoded_bytes: usize,
    chunks: Vec<String>,
}

/// Encode one logical bootstrap through the only supported physical transaction path.
///
/// Sections are independently JSON encoded and split, allowing history records larger than one
/// socket frame without making the browser maintain a separate large-bootstrap apply path.
pub(crate) fn encode_bootstrap(
    payload: &ThreadBootstrapPayload,
) -> Result<Vec<ThreadBootstrapFrame>, BootstrapEncodeError> {
    let sections = vec![
        encode_section(BootstrapSection::Metadata, &payload.metadata)?,
        encode_section(BootstrapSection::History, &payload.history)?,
        encode_section(BootstrapSection::LiveTurn, &payload.live_turn)?,
        encode_section(BootstrapSection::OrderedSuffix, &payload.ordered_suffix)?,
        encode_section(BootstrapSection::FinalRuntime, &payload.final_runtime)?,
        encode_section(BootstrapSection::Notices, &payload.notices)?,
    ];

    let mut descriptors = Vec::with_capacity(sections.len());
    for encoded in &sections {
        let chunk_count = u32::try_from(encoded.chunks.len()).map_err(|_| {
            BootstrapEncodeError::TooManyChunks {
                section: encoded.section,
                chunks: encoded.chunks.len(),
            }
        })?;
        descriptors.push(BootstrapSectionDescriptor {
            section: encoded.section,
            encoded_bytes: encoded.encoded_bytes as u64,
            chunk_count,
        });
    }

    let total_chunks: usize = sections.iter().map(|section| section.chunks.len()).sum();
    let mut frames = Vec::with_capacity(total_chunks + 2);
    frames.push(ThreadBootstrapFrame::Start {
        sections: descriptors,
    });
    for encoded in sections {
        for (index, payload_base64) in encoded.chunks.into_iter().enumerate() {
            let index = u32::try_from(index).map_err(|_| BootstrapEncodeError::TooManyChunks {
                section: encoded.section,
                chunks: index,
            })?;
            frames.push(ThreadBootstrapFrame::Chunk {
                section: encoded.section,
                index,
                payload_base64,
            });
        }
    }
    frames.push(ThreadBootstrapFrame::Commit);
    Ok(frames)
}

pub(crate) async fn build_and_send(
    state: &AppState,
    client_id: ClientId,
    project_id: ProjectId,
    thread_id: ThreadId,
    generation: SubscriptionGeneration,
    since: Option<TurnId>,
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

    let (mut history, mut history_turn_ids) =
        load_bootstrap_history(state, project_id, thread_id, since, history_limit).await?;

    if let Some(live) = &live_cut.live
        && !history_turn_ids.contains(&live.turn_id)
    {
        let all_turns = state
            .store
            .load_all_turns(project_id, thread_id)
            .await
            .map_err(|error| format!("failed to verify the live turn against history: {error}"))?;
        if let Some(index) = all_turns.iter().position(|turn| turn.id == live.turn_id) {
            let has_more = index > 0;
            let turns: Vec<_> = all_turns[index..].iter().cloned().map(Into::into).collect();
            history_turn_ids = all_turns[index..].iter().map(|turn| turn.id).collect();
            history = match since {
                Some(requested_after) => BootstrapHistory::CursorReset {
                    requested_after,
                    turns,
                    has_more,
                },
                None => BootstrapHistory::FullPage { turns, has_more },
            };
        }
    }

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
    debug!(
        %project_id,
        %thread_id,
        %client_id,
        subscription_generation = generation,
        event_seq_start = live_cut.represented_through.saturating_add(1),
        event_seq_end = final_cut.through_seq,
        suffix_entries = final_cut.suffix.len(),
        action = "cut_thread_bootstrap",
        "captured final staged bootstrap cut"
    );

    let live_turn = live_cut
        .live
        .filter(|live| !history_turn_ids.contains(&live.turn_id));
    let ordered_suffix = final_cut
        .suffix
        .into_iter()
        .filter(|entry| match entry.coverage {
            Some(JournalCoverage::Turn(turn_id)) => !history_turn_ids.contains(&turn_id),
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
    let frames = encode_bootstrap(&payload)
        .map_err(|error| BootstrapBuildError::Failed(error.to_string()))?;
    let frame_count = frames.len();
    let encoded_section_bytes = match frames.first() {
        Some(ThreadBootstrapFrame::Start { sections }) => sections
            .iter()
            .map(|section| section.encoded_bytes)
            .sum::<u64>(),
        _ => 0,
    };
    for frame in frames {
        if let Err(error) = state
            .hub
            .send_bootstrap_frame(thread_id, client_id, generation, frame)
            .await
        {
            return Err(BootstrapBuildError::Retryable {
                reason: ResyncReason::BootstrapCapacity,
                retry_after_ms: BOOTSTRAP_RETRY_AFTER_MS,
                detail: format!("failed to enqueue bootstrap frame: {error:?}"),
            });
        }
    }
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

async fn load_bootstrap_history(
    state: &AppState,
    project_id: ProjectId,
    thread_id: ThreadId,
    since: Option<TurnId>,
    history_limit: usize,
) -> Result<(BootstrapHistory, std::collections::HashSet<TurnId>), String> {
    if let Some(after) = since {
        if let Some(turns) = state
            .store
            .load_turns_after(project_id, thread_id, after)
            .await
            .map_err(|error| format!("failed to load incremental history: {error}"))?
        {
            let ids = turns.iter().map(|turn| turn.id).collect();
            return Ok((
                BootstrapHistory::Delta {
                    after,
                    turns: turns.into_iter().map(Into::into).collect(),
                },
                ids,
            ));
        }
        let (turns, has_more) = state
            .store
            .load_history(project_id, thread_id, None, history_limit)
            .await
            .map_err(|error| format!("failed to reset the history cursor: {error}"))?;
        let ids = turns.iter().map(|turn| turn.id).collect();
        return Ok((
            BootstrapHistory::CursorReset {
                requested_after: after,
                turns: turns.into_iter().map(Into::into).collect(),
                has_more,
            },
            ids,
        ));
    }
    let (turns, has_more) = state
        .store
        .load_history(project_id, thread_id, None, history_limit)
        .await
        .map_err(|error| format!("failed to load initial history: {error}"))?;
    let ids = turns.iter().map(|turn| turn.id).collect();
    Ok((
        BootstrapHistory::FullPage {
            turns: turns.into_iter().map(Into::into).collect(),
            has_more,
        },
        ids,
    ))
}

fn encode_section<T: Serialize>(
    section: BootstrapSection,
    value: &T,
) -> Result<EncodedSection, BootstrapEncodeError> {
    let encoded = serde_json::to_vec(value)
        .map_err(|source| BootstrapEncodeError::Serialize { section, source })?;
    let chunks = encoded
        .chunks(BOOTSTRAP_RAW_CHUNK_BYTES)
        .map(|chunk| BASE64.encode(chunk))
        .collect();
    Ok(EncodedSection {
        section,
        encoded_bytes: encoded.len(),
        chunks,
    })
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
                turns: Vec::new(),
                has_more: false,
            },
            live_turn: None,
            ordered_suffix: Vec::new(),
            final_runtime: ThreadFinalRuntime {
                through_seq: 0,
                turn_state: RuntimeTurnState::Idle,
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
        let frames = encode_bootstrap(&payload("small".into())).unwrap();
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
        let frames = encode_bootstrap(&payload("x".repeat(BOOTSTRAP_RAW_CHUNK_BYTES * 2))).unwrap();
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
