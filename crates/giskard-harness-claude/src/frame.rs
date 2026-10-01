//! One stdout line of `claude -p --output-format stream-json`, classified.
//!
//! The `claude-codes` types are used **per frame, after peeking** at `type` (and `subtype` for
//! `system` and `control_request`), never by deserializing a whole line into
//! `claude_codes::ClaudeOutput`. That enum has no fallback variant, so a top-level `type` the crate
//! does not know (`autocompact_state`, `active_goal`), a control request subtype it does not type,
//! or a missing required field would fail the line, and a stream that dies on its first unknown
//! frame is as wrong as one that drops it silently. Here a frame the crate cannot type becomes
//! [`Frame::Unknown`] or a [`FrameError::Untyped`] that the mapper logs and skips.

use claude_codes::io::{AssistantMessage, ResultMessage, UserMessage};
use claude_codes::{
    ApiRetryMessage, CompactBoundaryMessage, InitMessage, PermissionDeniedMessage, RateLimitEvent,
    SessionTitleChangedMessage, StatusMessage, StreamEventMessage, TaskNotificationMessage,
    TaskStartedMessage, TaskUpdatedMessage, ToolPermissionRequest, UsageInfo,
};
use serde::de::DeserializeOwned;
use serde_json::Value;

/// One stdout line, classified. Typed where the crate types it, raw where it does not.
#[derive(Debug)]
pub enum Frame {
    /// `system/init`. The raw value is kept beside the typed struct because
    /// `system/init.capabilities` is read from it later (milestone 8).
    Init(Box<InitMessage>, Value),
    Status(StatusMessage),
    TaskStarted(TaskStartedMessage),
    TaskUpdated(TaskUpdatedMessage),
    TaskNotification(TaskNotificationMessage),
    CompactBoundary(Box<CompactBoundaryMessage>),
    ApiRetry(ApiRetryMessage),
    PermissionDenied(PermissionDeniedMessage),
    SessionTitleChanged(SessionTitleChangedMessage),
    /// `system` subtypes this milestone reads but does not act on, kept for the debug log.
    SystemIgnored {
        subtype: String,
    },
    Assistant(Box<AssistantMessage>),
    User(Box<UserMessage>),
    Stream(StreamEvent),
    Result(Box<ResultMessage>),
    RateLimit(RateLimitEvent),
    /// The top-level `{type: "autocompact_state", value: {...}}` frame `claude-codes` cannot type.
    AutocompactState {
        effective_window: u64,
        threshold: Option<u64>,
    },
    /// A `control_request` with subtype `can_use_tool`. `raw` is the request object as the CLI
    /// sent it: `ToolPermissionRequest` has no `agent_id`, `display_name` or `description`, and its
    /// typed `PermissionSuggestion` drops keys such as `directories` that `AcceptForSession` must
    /// echo back verbatim.
    CanUseTool {
        request_id: String,
        request: Box<ToolPermissionRequest>,
        agent_id: Option<String>,
        raw: Value,
    },
    /// Every other inbound control request subtype (`request_user_dialog`, `rename_session`,
    /// `hook_callback`, `mcp_message`, …); `raw` is the request object, not deserialized.
    ControlRequest {
        request_id: String,
        subtype: String,
        raw: Value,
    },
    /// The CLI's answer to a control request the adapter sent; `raw` is the whole frame.
    ControlResponse {
        request_id: String,
        raw: Value,
    },
    /// The top-level `{type: "control_cancel_request", request_id}` frame: the CLI withdrew one of
    /// its own asks (it does so for every pending ask when the turn is interrupted).
    ControlCancelRequest {
        request_id: String,
    },
    Unknown {
        r#type: String,
        subtype: Option<String>,
    },
}

/// Why a line did not become a [`Frame`].
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    /// Not a JSON object line. Callers log the line's byte length, never its content.
    #[error("stdout line is not JSON ({bytes} bytes): {error}")]
    NotJson {
        bytes: usize,
        #[source]
        error: serde_json::Error,
    },
    /// A JSON value with no string `type`.
    #[error("stdout frame has no `type`")]
    NoType,
    /// A frame whose kind this crate types but whose payload did not convert.
    #[error("{} frame did not match its type: {}", frame_kind(r#type, subtype.as_deref()), redact_serde_error(error))]
    Untyped {
        r#type: String,
        subtype: Option<String>,
        error: serde_json::Error,
    },
}

/// `system` subtypes kept for the debug log only in this milestone.
const IGNORED_SYSTEM_SUBTYPES: &[&str] = &[
    "background_tasks_changed",
    "task_progress",
    "thinking_tokens",
    "post_turn_summary",
];

impl Frame {
    /// Classify one stdout line.
    pub fn parse(line: &str) -> Result<Frame, FrameError> {
        let value: Value = serde_json::from_str(line).map_err(|error| FrameError::NotJson {
            bytes: line.len(),
            error,
        })?;
        let Some(frame_type) = value.get("type").and_then(Value::as_str).map(str::to_owned) else {
            return Err(FrameError::NoType);
        };
        match frame_type.as_str() {
            "system" => Self::parse_system(value),
            "assistant" => typed(value, "assistant", None).map(|m| Frame::Assistant(Box::new(m))),
            "user" => typed(value, "user", None).map(|m| Frame::User(Box::new(m))),
            "result" => typed(value, "result", None).map(|m| Frame::Result(Box::new(m))),
            "rate_limit_event" => typed(value, "rate_limit_event", None).map(Frame::RateLimit),
            "stream_event" => StreamEvent::parse(value).map(Frame::Stream),
            "autocompact_state" => Self::parse_autocompact_state(&value),
            "control_request" => Self::parse_control_request(value),
            "control_response" => Self::parse_control_response(value),
            "control_cancel_request" => {
                typed::<CancelEnvelope>(value, "control_cancel_request", None).map(|envelope| {
                    Frame::ControlCancelRequest {
                        request_id: envelope.request_id,
                    }
                })
            }
            _ => Ok(Frame::Unknown {
                subtype: value
                    .get("subtype")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                r#type: frame_type,
            }),
        }
    }

    /// The frame's `type` and, where it has one, `subtype`, for logs.
    pub fn kind(&self) -> (&str, Option<&str>) {
        match self {
            Frame::Init(..) => ("system", Some("init")),
            Frame::Status(_) => ("system", Some("status")),
            Frame::TaskStarted(_) => ("system", Some("task_started")),
            Frame::TaskUpdated(_) => ("system", Some("task_updated")),
            Frame::TaskNotification(_) => ("system", Some("task_notification")),
            Frame::CompactBoundary(_) => ("system", Some("compact_boundary")),
            Frame::ApiRetry(_) => ("system", Some("api_retry")),
            Frame::PermissionDenied(_) => ("system", Some("permission_denied")),
            Frame::SessionTitleChanged(_) => ("system", Some("session_title_changed")),
            Frame::SystemIgnored { subtype } => ("system", Some(subtype)),
            Frame::Assistant(_) => ("assistant", None),
            Frame::User(_) => ("user", None),
            Frame::Stream(_) => ("stream_event", None),
            Frame::Result(_) => ("result", None),
            Frame::RateLimit(_) => ("rate_limit_event", None),
            Frame::AutocompactState { .. } => ("autocompact_state", None),
            Frame::CanUseTool { .. } => ("control_request", Some("can_use_tool")),
            Frame::ControlRequest { subtype, .. } => ("control_request", Some(subtype)),
            Frame::ControlResponse { .. } => ("control_response", None),
            Frame::ControlCancelRequest { .. } => ("control_cancel_request", None),
            Frame::Unknown { r#type, subtype } => (r#type, subtype.as_deref()),
        }
    }

    fn parse_system(value: Value) -> Result<Frame, FrameError> {
        let Some(subtype) = value
            .get("subtype")
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            return Ok(Frame::Unknown {
                r#type: "system".into(),
                subtype: None,
            });
        };
        // Deserialize the subtype's struct straight from the frame rather than through
        // `SystemMessage`'s `as_*` accessors: those return `None` on a failure without saying why,
        // and the error must reach the log.
        let sub = Some(subtype.as_str());
        match subtype.as_str() {
            "init" => {
                let init = typed(value.clone(), "system", sub)?;
                Ok(Frame::Init(Box::new(init), value))
            }
            "status" => typed(value, "system", sub).map(Frame::Status),
            "task_started" => typed(value, "system", sub).map(Frame::TaskStarted),
            "task_updated" => typed(value, "system", sub).map(Frame::TaskUpdated),
            "task_notification" => typed(value, "system", sub).map(Frame::TaskNotification),
            "compact_boundary" => {
                typed(value, "system", sub).map(|m| Frame::CompactBoundary(Box::new(m)))
            }
            "api_retry" => typed(value, "system", sub).map(Frame::ApiRetry),
            "permission_denied" => typed(value, "system", sub).map(Frame::PermissionDenied),
            "session_title_changed" => typed(value, "system", sub).map(Frame::SessionTitleChanged),
            ignored if IGNORED_SYSTEM_SUBTYPES.contains(&ignored) => {
                Ok(Frame::SystemIgnored { subtype })
            }
            _ => Ok(Frame::Unknown {
                r#type: "system".into(),
                subtype: Some(subtype),
            }),
        }
    }

    fn parse_autocompact_state(value: &Value) -> Result<Frame, FrameError> {
        #[derive(serde::Deserialize)]
        struct AutocompactValue {
            effective_window: u64,
            #[serde(default)]
            threshold: Option<u64>,
        }
        let state: AutocompactValue = typed(
            value.get("value").cloned().unwrap_or(Value::Null),
            "autocompact_state",
            None,
        )?;
        Ok(Frame::AutocompactState {
            effective_window: state.effective_window,
            threshold: state.threshold,
        })
    }

    fn parse_control_request(mut value: Value) -> Result<Frame, FrameError> {
        #[derive(serde::Deserialize)]
        struct Envelope {
            request_id: String,
            request: Value,
        }
        let subtype = value
            .pointer("/request/subtype")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let envelope: Envelope = typed(value.take(), "control_request", subtype.as_deref())?;
        let Some(subtype) = subtype else {
            return Ok(Frame::Unknown {
                r#type: "control_request".into(),
                subtype: None,
            });
        };
        if subtype != "can_use_tool" {
            return Ok(Frame::ControlRequest {
                request_id: envelope.request_id,
                subtype,
                raw: envelope.request,
            });
        }
        // The CLI sends `permission_suggestions: null` when it has none (an `ExitPlanMode` ask),
        // which `ToolPermissionRequest`'s `#[serde(default)]` Vec rejects. Absent and null mean the
        // same here, so drop the null from the copy being typed; `raw` keeps the frame as sent.
        let mut typed_request = envelope.request.clone();
        if let Some(object) = typed_request.as_object_mut()
            && object
                .get("permission_suggestions")
                .is_some_and(Value::is_null)
        {
            object.remove("permission_suggestions");
        }
        let request: ToolPermissionRequest =
            typed(typed_request, "control_request", Some("can_use_tool"))?;
        let agent_id = envelope
            .request
            .get("agent_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        Ok(Frame::CanUseTool {
            request_id: envelope.request_id,
            request: Box::new(request),
            agent_id,
            raw: envelope.request,
        })
    }

    fn parse_control_response(value: Value) -> Result<Frame, FrameError> {
        #[derive(serde::Deserialize)]
        struct Envelope {
            response: Response,
        }
        #[derive(serde::Deserialize)]
        struct Response {
            request_id: String,
        }
        let envelope: Envelope = typed(value.clone(), "control_response", None)?;
        Ok(Frame::ControlResponse {
            request_id: envelope.response.request_id,
            raw: value,
        })
    }
}

/// The `control_cancel_request` frame's one field.
#[derive(serde::Deserialize)]
struct CancelEnvelope {
    request_id: String,
}

/// A `stream_event` frame. `claude-codes` leaves `event` an untyped `Value`, so the event is this
/// crate's own small enum, carrying the envelope's `parent_tool_use_id`.
#[derive(Debug, Clone)]
pub struct StreamEvent {
    pub parent_tool_use_id: Option<String>,
    pub event: StreamEventKind,
}

#[derive(Debug, Clone)]
pub enum StreamEventKind {
    MessageStart {
        message_id: String,
    },
    ContentBlockStart {
        index: u32,
        block: BlockStart,
    },
    ContentBlockDelta {
        index: u32,
        delta: Delta,
    },
    ContentBlockStop {
        index: u32,
    },
    MessageDelta {
        stop_reason: Option<String>,
        usage: Option<UsageInfo>,
    },
    MessageStop,
    /// An event `type` this crate does not act on.
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockStart {
    Text,
    Thinking,
    ToolUse { id: String, name: String },
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delta {
    Text(String),
    Thinking(String),
    InputJson(String),
    Signature,
    Other(String),
}

impl StreamEvent {
    fn parse(value: Value) -> Result<StreamEvent, FrameError> {
        let envelope: StreamEventMessage = typed(value, "stream_event", None)?;
        let event_type = envelope
            .event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let untyped = |error| FrameError::Untyped {
            r#type: "stream_event".into(),
            subtype: Some(event_type.clone()),
            error,
        };
        let event = &envelope.event;
        let event = match event_type.as_str() {
            "message_start" => StreamEventKind::MessageStart {
                message_id: field::<String>(event, "/message/id").map_err(untyped)?,
            },
            "content_block_start" => {
                let index = field::<u32>(event, "/index").map_err(untyped)?;
                let block_type = field::<String>(event, "/content_block/type").map_err(untyped)?;
                let block = match block_type.as_str() {
                    "text" => BlockStart::Text,
                    "thinking" => BlockStart::Thinking,
                    "tool_use" => BlockStart::ToolUse {
                        id: field(event, "/content_block/id").map_err(untyped)?,
                        name: field(event, "/content_block/name").map_err(untyped)?,
                    },
                    _ => BlockStart::Other(block_type),
                };
                StreamEventKind::ContentBlockStart { index, block }
            }
            "content_block_delta" => {
                let index = field::<u32>(event, "/index").map_err(untyped)?;
                let delta_type = field::<String>(event, "/delta/type").map_err(untyped)?;
                let delta = match delta_type.as_str() {
                    "text_delta" => Delta::Text(field(event, "/delta/text").map_err(untyped)?),
                    "thinking_delta" => {
                        Delta::Thinking(field(event, "/delta/thinking").map_err(untyped)?)
                    }
                    "input_json_delta" => {
                        Delta::InputJson(field(event, "/delta/partial_json").map_err(untyped)?)
                    }
                    "signature_delta" => Delta::Signature,
                    _ => Delta::Other(delta_type),
                };
                StreamEventKind::ContentBlockDelta { index, delta }
            }
            "content_block_stop" => StreamEventKind::ContentBlockStop {
                index: field(event, "/index").map_err(untyped)?,
            },
            "message_delta" => StreamEventKind::MessageDelta {
                stop_reason: field::<Option<String>>(event, "/delta/stop_reason")
                    .map_err(untyped)?,
                usage: field::<Option<UsageInfo>>(event, "/usage").map_err(untyped)?,
            },
            "message_stop" => StreamEventKind::MessageStop,
            _ => StreamEventKind::Other(event_type),
        };
        Ok(StreamEvent {
            parent_tool_use_id: envelope.parent_tool_use_id,
            event,
        })
    }
}

fn typed<T: DeserializeOwned>(
    value: Value,
    frame_type: &str,
    subtype: Option<&str>,
) -> Result<T, FrameError> {
    serde_json::from_value(value).map_err(|error| FrameError::Untyped {
        r#type: frame_type.to_owned(),
        subtype: subtype.map(str::to_owned),
        error,
    })
}

/// One field of an untyped event by JSON pointer; an absent field reads as `null`, so an `Option`
/// target accepts it and any other target reports it.
fn field<T: DeserializeOwned>(value: &Value, pointer: &str) -> Result<T, serde_json::Error> {
    serde_json::from_value(value.pointer(pointer).cloned().unwrap_or(Value::Null))
}

fn frame_kind(frame_type: &str, subtype: Option<&str>) -> String {
    match subtype {
        Some(subtype) => format!("{frame_type}/{subtype}"),
        None => frame_type.to_owned(),
    }
}

/// A serde error with every double-quoted span replaced, for logs.
///
/// `serde_json` quotes the offending *value* in messages such as `invalid type: string "touch
/// probe.txt", expected u32`, which would put frame content in a log line. Field and variant names
/// are backtick-quoted and stay, since they are what makes the error diagnosable.
pub(crate) fn redact_serde_error(error: &serde_json::Error) -> String {
    let message = error.to_string();
    let mut redacted = String::with_capacity(message.len());
    let mut in_quotes = false;
    for character in message.chars() {
        match (character, in_quotes) {
            ('"', false) => {
                in_quotes = true;
                redacted.push_str("\"…");
            }
            ('"', true) => {
                in_quotes = false;
                redacted.push('"');
            }
            (_, true) => {}
            (character, false) => redacted.push(character),
        }
    }
    redacted
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURES: &[&str] = &[
        "accept-for-session",
        "autocompact-state",
        "background-bash",
        "cancel",
        "compact",
        "delegation",
        "delegation-interrupted",
        "initialize",
        "plan-exit-denied",
        "resume-missing",
        "text-turn",
        "tool-allowed",
        "tool-denied",
    ];

    fn fixture_lines(name: &str) -> Vec<String> {
        let path = format!(
            "{}/tests/fixtures/{name}.out.jsonl",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {path}: {error}"))
            .lines()
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn every_fixture_line_parses_to_a_typed_frame() {
        for name in FIXTURES {
            for (number, line) in fixture_lines(name).iter().enumerate() {
                let frame =
                    Frame::parse(line).unwrap_or_else(|error| panic!("{name}:{number}: {error}"));
                match (&frame, *name) {
                    (Frame::Unknown { r#type, .. }, "autocompact-state") => {
                        assert_eq!(r#type, "active_goal", "{name}:{number}");
                    }
                    (Frame::Unknown { .. }, _) => panic!("{name}:{number}: {frame:?}"),
                    _ => {}
                }
            }
        }
    }

    #[test]
    fn the_autocompact_state_frame_reads_its_effective_window() {
        let lines = fixture_lines("autocompact-state");
        assert!(matches!(
            Frame::parse(&lines[0]),
            Ok(Frame::Unknown { ref r#type, subtype: None }) if r#type == "active_goal"
        ));
        assert!(matches!(
            Frame::parse(&lines[1]),
            Ok(Frame::AutocompactState {
                effective_window: 180_000,
                threshold: Some(144_000)
            })
        ));
    }

    #[test]
    fn ignored_system_subtypes_are_kept_for_the_debug_log() {
        let frame = Frame::parse(r#"{"type":"system","subtype":"task_progress","task_id":"t"}"#);
        assert!(
            matches!(frame, Ok(Frame::SystemIgnored { ref subtype }) if subtype == "task_progress")
        );
    }

    #[test]
    fn the_text_turn_holds_every_stream_event_shape() {
        let events: Vec<StreamEventKind> = fixture_lines("text-turn")
            .iter()
            .filter_map(|line| match Frame::parse(line) {
                Ok(Frame::Stream(stream)) => Some(stream.event),
                _ => None,
            })
            .collect();
        let has = |wanted: fn(&StreamEventKind) -> bool| events.iter().any(wanted);
        assert!(has(|event| matches!(
            event,
            StreamEventKind::MessageStart { message_id } if message_id == "msg_011CfaU4sfyRtL8m7Sg3g87p"
        )));
        assert!(has(|event| matches!(
            event,
            StreamEventKind::ContentBlockStart {
                index: 0,
                block: BlockStart::Thinking
            }
        )));
        assert!(has(|event| matches!(
            event,
            StreamEventKind::ContentBlockStart {
                index: 1,
                block: BlockStart::Text
            }
        )));
        assert!(has(|event| matches!(
            event,
            StreamEventKind::ContentBlockDelta { index: 0, delta: Delta::Thinking(text) } if text.is_empty()
        )));
        assert!(has(|event| matches!(
            event,
            StreamEventKind::ContentBlockDelta {
                index: 0,
                delta: Delta::Signature
            }
        )));
        assert!(has(|event| matches!(
            event,
            StreamEventKind::ContentBlockDelta { index: 1, delta: Delta::Text(text) } if text == "pong"
        )));
        assert!(has(|event| matches!(
            event,
            StreamEventKind::ContentBlockStop { index: 1 }
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEventKind::MessageDelta { stop_reason: Some(reason), usage: Some(usage) }
                if reason == "end_turn" && usage.output_tokens == 53
        )));
        assert!(has(|event| matches!(event, StreamEventKind::MessageStop)));
    }

    #[test]
    fn a_can_use_tool_ask_keeps_the_raw_request_and_its_agent_id() {
        let line = r#"{"type":"control_request","request_id":"r1","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"ls"},"agent_id":"a1","permission_suggestions":null}}"#;
        let Ok(Frame::CanUseTool {
            request_id,
            request,
            agent_id,
            raw,
        }) = Frame::parse(line)
        else {
            panic!("not a can_use_tool frame");
        };
        assert_eq!(request_id, "r1");
        assert_eq!(request.tool_name, "Bash");
        assert!(request.permission_suggestions.is_empty());
        assert_eq!(agent_id.as_deref(), Some("a1"));
        assert!(raw["permission_suggestions"].is_null());
    }

    #[test]
    fn other_control_requests_are_not_deserialized() {
        let line = r#"{"type":"control_request","request_id":"r2","request":{"subtype":"rename_session","title":"x"}}"#;
        assert!(matches!(
            Frame::parse(line),
            Ok(Frame::ControlRequest { ref request_id, ref subtype, ref raw })
                if request_id == "r2" && subtype == "rename_session" && raw["title"] == "x"
        ));
    }

    #[test]
    fn a_control_cancel_request_names_the_withdrawn_ask() {
        let frame =
            Frame::parse(r#"{"type":"control_cancel_request","request_id":"a949f115"}"#).unwrap();
        assert!(
            matches!(&frame, Frame::ControlCancelRequest { request_id } if request_id == "a949f115")
        );
        assert_eq!(frame.kind(), ("control_cancel_request", None));
        assert!(matches!(
            Frame::parse(r#"{"type":"control_cancel_request"}"#),
            Err(FrameError::Untyped { r#type, .. }) if r#type == "control_cancel_request"
        ));
    }

    #[test]
    fn a_non_json_line_is_not_json() {
        assert!(matches!(
            Frame::parse("Error: something broke"),
            Err(FrameError::NotJson { bytes: 22, .. })
        ));
    }

    #[test]
    fn a_frame_without_a_type_is_reported() {
        assert!(matches!(
            Frame::parse(r#"{"subtype":"init"}"#),
            Err(FrameError::NoType)
        ));
    }

    #[test]
    fn a_system_frame_with_an_unknown_subtype_names_both() {
        assert!(matches!(
            Frame::parse(r#"{"type":"system","subtype":"brand_new","session_id":"s"}"#),
            Ok(Frame::Unknown { ref r#type, subtype: Some(ref subtype) })
                if r#type == "system" && subtype == "brand_new"
        ));
    }

    #[test]
    fn an_assistant_frame_missing_its_model_is_untyped_naming_the_type() {
        let line = r#"{"type":"assistant","session_id":"s","message":{"id":"msg_1","role":"assistant","content":[]}}"#;
        let Err(FrameError::Untyped {
            r#type, subtype, ..
        }) = Frame::parse(line)
        else {
            panic!("expected an untyped frame");
        };
        assert_eq!(r#type, "assistant");
        assert_eq!(subtype, None);
    }

    #[test]
    fn untyped_errors_redact_quoted_values() {
        let line = r#"{"type":"system","subtype":"task_started","session_id":"s","task_id":"t","description":"d","uuid":"u","spawn_depth":"touch probe.txt"}"#;
        let Err(error) = Frame::parse(line) else {
            panic!("expected an untyped frame");
        };
        let rendered = error.to_string();
        assert!(
            rendered.starts_with("system/task_started frame"),
            "{rendered}"
        );
        assert!(!rendered.contains("touch probe.txt"), "{rendered}");
    }
}
