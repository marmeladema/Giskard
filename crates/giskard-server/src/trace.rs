//! On-demand tracing: capture, buffer, and export `tracing` spans as Chrome trace-event JSON.
//!
//! A custom `tracing_subscriber::Layer` records completed spans into a bounded in-memory buffer
//! keyed by W3C `trace_id`. When armed, `#[instrument]` spans across the backend, networking,
//! and (via `POST /api/traces/ui`) the browser flow into one combined trace. `GET /admin/trace`
//! flushes the buffer and returns Chrome trace-event JSON consumable by `chrome://tracing` and
//! perfetto.dev, so a single waterfall answers "where is the time spent for this request/flow".
//!
//! Capture is **off by default** and must be armed (config or `POST /admin/trace/arm`). The
//! buffer is per-process and reset on restart; no on-disk persistence (local-first v1).

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::field::Visit;
use tracing::{
    Subscriber,
    span::{Attributes, Id},
};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// Maximum number of spans allowed in a single `POST /api/traces/ui` batch.
pub const UI_SPAN_BATCH_MAX: usize = 256;

/// Bounds on a single UI span's string content, to keep the in-memory buffer bounded against an
/// authenticated, armed client that could otherwise bloat it. Offending spans are dropped with a
/// `debug` log (consistent with the existing malformed-span handling). Single-user v1 sizes.
pub const UI_SPAN_NAME_MAX: usize = 256;
/// Maximum number of labels per UI span.
pub const UI_SPAN_LABEL_COUNT_MAX: usize = 64;
/// Maximum length of any single label key or value.
pub const UI_SPAN_LABEL_FIELD_MAX: usize = 1024;

/// A recorded span, in a representation close to Chrome trace-event format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordedSpan {
    pub name: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    /// Microseconds since the Unix epoch.
    pub start_us: i64,
    /// Microseconds since the Unix epoch.
    pub end_us: i64,
    pub labels: HashMap<String, String>,
}

/// A complete trace: one `trace_id` and all its spans.
#[derive(Debug, Clone, Default)]
pub struct Trace {
    pub trace_id: String,
    pub spans: Vec<RecordedSpan>,
}

/// Bounded in-memory store of recent traces, keyed by `trace_id`. Oldest evicted on overflow.
#[derive(Debug)]
pub struct TraceBuffer {
    max_traces: std::sync::atomic::AtomicUsize,
    traces: RwLock<Vec<Trace>>,
    /// Index of `trace_id -> position in traces`, kept in lockstep with `traces` under the same
    /// write lock so `record` is O(1) per trace lookup instead of O(traces). Rebuilt from
    /// `traces` when invalidated by eviction (which shifts all positions).
    index: RwLock<HashMap<String, usize>>,
}

impl TraceBuffer {
    pub fn new(max_traces: usize) -> Self {
        let cap = max_traces.max(1);
        Self {
            max_traces: std::sync::atomic::AtomicUsize::new(cap),
            traces: RwLock::new(Vec::with_capacity(cap)),
            index: RwLock::new(HashMap::with_capacity(cap)),
        }
    }

    /// Update the capacity. If shrinking below the current length, oldest traces are evicted.
    pub fn set_max_traces(&self, max_traces: usize) {
        let cap = max_traces.max(1);
        let mut traces = self.traces.write();
        let mut index = self.index.write();
        while traces.len() > cap {
            traces.remove(0);
        }
        // Eviction shifted positions; rebuild the index from the surviving traces.
        index.clear();
        index.extend(
            traces
                .iter()
                .enumerate()
                .map(|(i, t)| (t.trace_id.clone(), i)),
        );
        self.max_traces
            .store(cap, std::sync::atomic::Ordering::Relaxed);
    }

    /// Merge a batch of spans (server-side or browser-emitted) into the buffer, grouping by
    /// `trace_id`. Spans for an unknown trace start a new trace entry.
    pub fn record(&self, spans: Vec<RecordedSpan>) {
        if spans.is_empty() {
            return;
        }
        let mut by_trace: HashMap<String, Vec<RecordedSpan>> = HashMap::new();
        for span in spans {
            by_trace
                .entry(span.trace_id.clone())
                .or_default()
                .push(span);
        }
        let mut traces = self.traces.write();
        let mut index = self.index.write();
        for (trace_id, mut new_spans) in by_trace {
            if let Some(&pos) = index.get(&trace_id) {
                if let Some(existing) = traces.get_mut(pos) {
                    existing.spans.append(&mut new_spans);
                    continue;
                }
            }
            // Not found: insert, evicting the oldest if at capacity.
            if traces.len() >= self.max_traces.load(std::sync::atomic::Ordering::Relaxed) {
                traces.remove(0);
                // Eviction shifts every position down by one; rebuild the index from scratch
                // rather than decrementing each entry (simpler and robust to drift).
                index.clear();
                index.extend(
                    traces
                        .iter()
                        .enumerate()
                        .map(|(i, t)| (t.trace_id.clone(), i)),
                );
            }
            let pos = traces.len();
            traces.push(Trace {
                trace_id,
                spans: new_spans,
            });
            index.insert(traces[pos].trace_id.clone(), pos);
        }
    }

    /// Snapshot and clear the buffer, returning all traces. Optionally filter to one `trace_id`.
    pub fn flush(&self, trace_id: Option<&str>) -> Vec<Trace> {
        let mut traces = self.traces.write();
        self.index.write().clear();
        let all = std::mem::take(&mut *traces);
        match trace_id {
            Some(id) => all.into_iter().filter(|t| t.trace_id == id).collect(),
            None => all,
        }
    }

    /// Snapshot without clearing (for tests / inspection).
    pub fn snapshot(&self) -> Vec<Trace> {
        self.traces.read().clone()
    }

    /// Non-draining count of `(trace_count, span_count)` currently in the buffer, so the UI can
    /// show "N spans" beside the status dot without consuming the capture.
    pub fn counts(&self) -> (usize, usize) {
        let traces = self.traces.read();
        let span_count = traces.iter().map(|t| t.spans.len()).sum();
        (traces.len(), span_count)
    }
}

/// Handle held by `AppState` to arm/disarm capture and flush/export traces.
#[derive(Clone)]
pub struct TraceHandle {
    pub armed: Arc<std::sync::atomic::AtomicBool>,
    pub buffer: Arc<TraceBuffer>,
}

impl TraceHandle {
    pub fn new(max_traces: usize, armed: bool) -> Self {
        Self {
            armed: Arc::new(std::sync::atomic::AtomicBool::new(armed)),
            buffer: Arc::new(TraceBuffer::new(max_traces)),
        }
    }

    pub fn is_armed(&self) -> bool {
        self.armed.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_armed(&self, armed: bool) {
        self.armed
            .store(armed, std::sync::atomic::Ordering::Relaxed);
    }

    /// Update the in-memory buffer capacity (spec §17.5 `[tracing].buffer_max_traces`).
    pub fn set_buffer_max_traces(&self, max_traces: usize) {
        self.buffer.set_max_traces(max_traces);
    }

    pub fn record(&self, spans: Vec<RecordedSpan>) {
        self.buffer.record(spans);
    }

    pub fn flush(&self, trace_id: Option<&str>) -> Vec<Trace> {
        self.buffer.flush(trace_id)
    }

    /// Non-draining `(trace_count, span_count)` for the UI's "N spans" indicator.
    pub fn counts(&self) -> (usize, usize) {
        self.buffer.counts()
    }

    /// Render the flushed traces as Chrome trace-event JSON (array of events). Each span becomes
    /// emitted as an **async** pair: a `{"ph":"b"}` begin event at `ts=start_us` and a
    /// `{"ph":"e"}` end event at `ts=end_us`, both sharing `id = span_id`. Async events do not
    /// require the strict timestamp-nesting that `X` (complete) events impose on a single tid,
    /// so concurrent/overlapping spans in this buffer (parallel `forward_events`, overlapping
    /// hub/ledger children, merged browser spans) render as a correct waterfall in
    /// `chrome://tracing` and Perfetto instead of mis-nesting or triggering a nesting-violation
    /// warning. The `args` object (labels, trace_id, parent_span_id) is attached to both the
    /// begin and end events; `id` is repeated so the viewer joins the pair.
    pub fn render_perfetto_json(&self, trace_id: Option<&str>) -> String {
        let traces = self.flush(trace_id);
        let mut events = Vec::new();
        for trace in &traces {
            for span in &trace.spans {
                let mut args = serde_json::Map::new();
                args.insert(
                    "trace_id".into(),
                    serde_json::Value::String(span.trace_id.clone()),
                );
                args.insert(
                    "parent_span_id".into(),
                    span.parent_span_id
                        .clone()
                        .map(serde_json::Value::String)
                        .unwrap_or(serde_json::Value::Null),
                );
                for (k, v) in &span.labels {
                    args.insert(k.clone(), serde_json::Value::String(v.clone()));
                }
                let begin = serde_json::json!({
                    "name": span.name,
                    "cat": "giskard",
                    "ph": "b",
                    "ts": span.start_us,
                    "pid": 1,
                    "tid": 0,
                    "id": span.span_id,
                    "args": serde_json::Value::Object(args),
                });
                let mut end = serde_json::json!({
                    "name": span.name,
                    "cat": "giskard",
                    "ph": "e",
                    "ts": span.end_us,
                    "pid": 1,
                    "tid": 0,
                    "id": span.span_id,
                });
                // Attach the same args to the end event so tools that only inspect close events
                // still see the labels; begin carries the canonical copy.
                if let serde_json::Value::Object(a) = begin["args"].clone() {
                    end["args"] = serde_json::Value::Object(a);
                }
                events.push(begin);
                events.push(end);
            }
        }
        serde_json::to_string(&events).unwrap_or_else(|_| "[]".to_string())
    }
}

/// A `tracing::Subscriber` field visitor that collects stringified span fields into a labels map.
struct FieldCollector(HashMap<String, String>);

impl Visit for FieldCollector {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        // Fallback for fields without a typed recorder: stringify via Debug.
        // Display values (`field = %x`) route here through `tracing::field::Display`,
        // whose `Debug` impl delegates to `Display`, so they store unquoted.
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

/// The capture layer. When the shared `armed` flag is false, it is a no-op (no allocation).
pub struct TraceCaptureLayer {
    handle: TraceHandle,
}

impl TraceCaptureLayer {
    pub fn new(handle: TraceHandle) -> Self {
        Self { handle }
    }
}

impl<S> Layer<S> for TraceCaptureLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if !self.handle.is_armed() {
            return;
        }
        let mut collector = FieldCollector(HashMap::new());
        attrs.values().record(&mut collector);
        let labels = collector.0;

        let span = ctx.span(id);
        if let Some(span) = span {
            let mut ext = span.extensions_mut();
            if ext.get_mut::<SpanState>().is_none() {
                ext.insert(SpanState {
                    name: span.name().to_string(),
                    labels,
                    start_us: now_us(),
                });
            }
        }
    }

    fn on_record(&self, id: &Id, values: &tracing::span::Record<'_>, ctx: Context<'_, S>) {
        // Fields declared `field::Empty` at span creation are filled in later via
        // `span.record(...)` / `Span::current().record(...)`. Without this hook those values
        // never reach the export (on_new_span only snapshots creation-time values). Merge them
        // into the existing SpanState.labels so the exported trace carries the full annotation
        // set (e.g. `cache_hit`, `bytes`, `turns`, `attempts`, HTTP `status`).
        if !self.handle.is_armed() {
            return;
        }
        let Some(span) = ctx.span(id) else { return };
        let mut ext = span.extensions_mut();
        let Some(state) = ext.get_mut::<SpanState>() else {
            // No SpanState means the span was created before arming; nothing to merge into.
            return;
        };
        let mut collector = FieldCollector(std::mem::take(&mut state.labels));
        values.record(&mut collector);
        state.labels = collector.0;
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        if !self.handle.is_armed() {
            return;
        }
        let span = ctx.span(&id);
        let Some(span) = span else { return };
        let mut ext = span.extensions_mut();
        let Some(state) = ext.remove::<SpanState>() else {
            return;
        };
        let end_us = now_us();

        let (trace_id, span_id, parent_span_id) = span_context(&span, &state);
        self.handle.record(vec![RecordedSpan {
            name: state.name,
            trace_id,
            span_id,
            parent_span_id,
            start_us: state.start_us,
            end_us,
            labels: state.labels,
        }]);
    }
}

/// Per-span state stored in the registry extensions.
struct SpanState {
    name: String,
    labels: HashMap<String, String>,
    start_us: i64,
}

/// Derive (trace_id, span_id, parent_span_id) for a closed span. `tracing` itself does not carry
/// W3C ids, so we synthesize stable ids from the span's `Id` plus any parent. For server-initiated
/// flows these are self-consistent within the buffer; for browser→server flows the `traceparent`
/// is propagated via span fields (`trace_id`, `parent_span_id`) recorded by the request
/// middleware, and we prefer those when present.
fn span_context<S>(
    span: &tracing_subscriber::registry::SpanRef<'_, S>,
    state: &SpanState,
) -> (String, String, Option<String>)
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let span_id = format!("{:016x}", id_to_u64(span.id()));
    let parent_span_id = span.parent().map(|p| format!("{:016x}", id_to_u64(p.id())));

    // If the span recorded a `trace_id` field (from `traceparent` propagation), prefer it so
    // server-side spans join the browser's trace.
    if let Some(tid) = state.labels.get("trace_id").filter(|t| !t.is_empty()) {
        return (
            tid.clone(),
            span_id,
            parent_span_id.or_else(|| {
                state
                    .labels
                    .get("parent_span_id")
                    .filter(|p| !p.is_empty())
                    .cloned()
            }),
        );
    }
    // No propagated trace context: synthesize a trace id grouping this span and its ancestors.
    // Walk to the root and use its span_id as the trace id, so a server-only tree stays grouped.
    // `SpanRef` is not `Clone`, so walk by following `parent()` into owned values and track the
    // deepest id we reach.
    let root_id = {
        let mut current = span.id();
        let mut cursor = span.parent();
        while let Some(parent) = cursor {
            current = parent.id();
            cursor = parent.parent();
        }
        current
    };
    let trace_id = format!("{:016x}", id_to_u64(root_id));
    (trace_id, span_id, parent_span_id)
}

fn id_to_u64(id: Id) -> u64 {
    id.into_u64()
}

fn now_us() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// Arm/disarm request body for `POST /admin/trace/arm`.
#[derive(Debug, Deserialize)]
pub struct ArmRequest {
    pub armed: bool,
}

/// Config response body for `GET /api/traces/config` (browser learns whether to emit UI spans).
#[derive(Debug, Serialize)]
pub struct TraceConfigResponse {
    pub armed: bool,
    pub ui_ingest_enabled: bool,
    /// Traces currently in the buffer (non-draining; for the UI "N spans" indicator).
    pub trace_count: usize,
    /// Total spans across those traces.
    pub span_count: usize,
}

/// Parse a W3C `traceparent` header value: `00-<trace_id>-<parent_span_id>-<flags>`.
/// Returns `(trace_id, parent_span_id)` on success.
pub fn parse_traceparent(header: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = header.split('-').collect();
    if parts.len() != 4 {
        return None;
    }
    let trace_id = parts[1];
    let parent_span_id = parts[2];
    if trace_id.len() != 32 || parent_span_id.len() != 16 {
        return None;
    }
    if !trace_id.chars().all(|c| c.is_ascii_hexdigit())
        || !parent_span_id.chars().all(|c| c.is_ascii_hexdigit())
    {
        return None;
    }
    Some((trace_id.to_string(), parent_span_id.to_string()))
}

/// Build a `traceparent` header value for outbound propagation.
pub fn build_traceparent(trace_id: &str, span_id: &str) -> String {
    format!("00-{trace_id}-{span_id}-01")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::instrument;
    use tracing_subscriber::prelude::*;

    fn setup_armed() -> TraceHandle {
        TraceHandle::new(16, true)
    }

    #[instrument(skip(handle), fields(op = "test_op"))]
    fn do_work(handle: TraceHandle) {
        let _ = handle;
    }

    /// Regression for dynamically-recorded fields (F1): fields declared `field::Empty` at
    /// creation and filled in later via `Span::current().record(...)` must reach the export.
    /// Before `on_record`, these were silently dropped.
    #[instrument(skip(handle), fields(cache_hit = tracing::field::Empty, bytes = tracing::field::Empty))]
    fn do_work_with_late_fields(handle: TraceHandle, hit: bool, bytes: u64) {
        let _ = handle;
        tracing::Span::current().record("cache_hit", hit);
        tracing::Span::current().record("bytes", bytes);
    }

    fn mk_span(trace_id: &str, span_id: &str) -> RecordedSpan {
        RecordedSpan {
            name: "a".into(),
            trace_id: trace_id.into(),
            span_id: span_id.into(),
            parent_span_id: None,
            start_us: 0,
            end_us: 1,
            labels: HashMap::new(),
        }
    }

    #[test]
    fn buffer_evicts_oldest_when_full() {
        let buf = TraceBuffer::new(2);
        buf.record(vec![mk_span("t1", "s1")]);
        buf.record(vec![mk_span("t2", "s2")]);
        buf.record(vec![mk_span("t3", "s3")]);
        let snap = buf.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].trace_id, "t2");
        assert_eq!(snap[1].trace_id, "t3");
    }

    #[test]
    fn flush_clears_buffer() {
        let buf = TraceBuffer::new(4);
        buf.record(vec![mk_span("t1", "s1")]);
        let flushed = buf.flush(None);
        assert_eq!(flushed.len(), 1);
        assert!(buf.snapshot().is_empty());
    }

    #[test]
    fn flush_filters_by_trace_id() {
        let buf = TraceBuffer::new(4);
        buf.record(vec![mk_span("t1", "s1")]);
        buf.record(vec![mk_span("t2", "s2")]);
        let flushed = buf.flush(Some("t1"));
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].trace_id, "t1");
    }

    #[test]
    fn merge_appends_to_existing_trace() {
        let buf = TraceBuffer::new(4);
        buf.record(vec![mk_span("t1", "s1")]);
        buf.record(vec![mk_span("t1", "s2")]);
        let snap = buf.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].spans.len(), 2);
    }

    #[test]
    fn render_perfetto_json_emits_async_begin_end_pair() {
        let handle = TraceHandle::new(4, true);
        handle.record(vec![RecordedSpan {
            name: "load_history".into(),
            trace_id: "t1".into(),
            span_id: "s1".into(),
            parent_span_id: Some("s0".into()),
            start_us: 1000,
            end_us: 2500,
            labels: HashMap::from([("turns".into(), "50".into())]),
        }]);
        let json = handle.render_perfetto_json(None);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(v.is_array());
        // Each span is an async pair: ph:b at start_us, ph:e at end_us, shared id.
        assert_eq!(v.as_array().unwrap().len(), 2, "begin + end events");
        assert_eq!(v[0]["name"], "load_history");
        assert_eq!(v[0]["ph"], "b");
        assert_eq!(v[0]["ts"], 1000);
        assert_eq!(v[0]["id"], "s1");
        assert_eq!(v[0]["args"]["turns"], "50");
        assert_eq!(v[0]["args"]["parent_span_id"], "s0");
        assert_eq!(v[1]["name"], "load_history");
        assert_eq!(v[1]["ph"], "e");
        assert_eq!(v[1]["ts"], 2500);
        assert_eq!(v[1]["id"], "s1");
        assert_eq!(v[1]["args"]["turns"], "50", "end event repeats the labels");
    }

    #[test]
    fn render_perfetto_json_overlapping_spans_get_separate_async_pairs() {
        // F2: two spans that overlap in time must not require strict nesting. With async events
        // each gets its own begin/end pair keyed by id, so overlapping spans render correctly.
        let handle = TraceHandle::new(4, true);
        handle.record(vec![
            RecordedSpan {
                name: "forward_events".into(),
                trace_id: "t1".into(),
                span_id: "s1".into(),
                parent_span_id: None,
                start_us: 1000,
                end_us: 3000,
                labels: HashMap::new(),
            },
            RecordedSpan {
                name: "broadcast_event".into(),
                trace_id: "t1".into(),
                span_id: "s2".into(),
                parent_span_id: Some("s1".into()),
                // Overlaps the first span: starts after, ends before the first ends.
                start_us: 1500,
                end_us: 2500,
                labels: HashMap::new(),
            },
        ]);
        let v: serde_json::Value =
            serde_json::from_str(&handle.render_perfetto_json(None)).unwrap();
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 4, "two spans -> two begin/end pairs");
        // Pairs keyed by id; ordering is begin/begin/end/end by emission, but the contract is
        // just that each id has one b and one e with matching timestamps.
        let mut by_id: std::collections::HashMap<&str, Vec<&serde_json::Value>> =
            std::collections::HashMap::new();
        for ev in arr {
            by_id
                .entry(ev["id"].as_str().unwrap())
                .or_default()
                .push(ev);
        }
        let s1 = &by_id["s1"];
        let s2 = &by_id["s2"];
        assert_eq!(s1.len(), 2);
        assert_eq!(s2.len(), 2);
        let (b1, e1) = (
            s1.iter().find(|e| e["ph"] == "b").unwrap(),
            s1.iter().find(|e| e["ph"] == "e").unwrap(),
        );
        let (b2, e2) = (
            s2.iter().find(|e| e["ph"] == "b").unwrap(),
            s2.iter().find(|e| e["ph"] == "e").unwrap(),
        );
        assert_eq!(b1["ts"], 1000);
        assert_eq!(e1["ts"], 3000);
        assert_eq!(b2["ts"], 1500);
        assert_eq!(e2["ts"], 2500);
        // Overlap: s2 starts before s1 ends — legal for async events, illegal for X-on-one-tid.
        assert!(b2["ts"].as_i64().unwrap() < e1["ts"].as_i64().unwrap());
    }

    #[test]
    fn parse_traceparent_valid() {
        let (tid, sid) =
            parse_traceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01").unwrap();
        assert_eq!(tid, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(sid, "b7ad6b7169203331");
    }

    #[test]
    fn parse_traceparent_rejects_malformed() {
        assert!(parse_traceparent("garbage").is_none());
        assert!(parse_traceparent("00-short-short-01").is_none());
    }

    #[test]
    fn build_traceparent_roundtrip() {
        let tp = build_traceparent("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331");
        let (tid, sid) = parse_traceparent(&tp).unwrap();
        assert_eq!(tid, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(sid, "b7ad6b7169203331");
    }

    #[test]
    fn capture_layer_records_instrumented_span_when_armed() {
        let handle = setup_armed();
        let layer = TraceCaptureLayer::new(handle.clone());
        let sub = tracing_subscriber::registry::Registry::default().with(layer);
        let dispatch = tracing::dispatcher::Dispatch::new(sub);
        tracing::dispatcher::with_default(&dispatch, || {
            do_work(handle.clone());
        });
        let traces = handle.flush(None);
        assert_eq!(traces.len(), 1, "expected one trace");
        let spans = &traces[0].spans;
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "do_work");
        assert_eq!(
            spans[0].labels.get("op").map(|s| s.as_str()),
            Some("test_op")
        );
        assert!(spans[0].end_us >= spans[0].start_us);
    }

    #[test]
    fn capture_layer_records_late_recorded_fields() {
        // Regression for F1: fields declared `field::Empty` at creation and filled via
        // `Span::current().record(...)` mid-body must appear in the exported span labels.
        // Without `on_record`, only creation-time literals (op) survive and cache_hit/bytes
        // are silently dropped.
        let handle = setup_armed();
        let layer = TraceCaptureLayer::new(handle.clone());
        let sub = tracing_subscriber::registry::Registry::default().with(layer);
        let dispatch = tracing::dispatcher::Dispatch::new(sub);
        tracing::dispatcher::with_default(&dispatch, || {
            do_work_with_late_fields(handle.clone(), true, 4096);
        });
        let traces = handle.flush(None);
        assert_eq!(traces.len(), 1, "expected one trace");
        let spans = &traces[0].spans;
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "do_work_with_late_fields");
        assert_eq!(
            spans[0].labels.get("cache_hit").map(|s| s.as_str()),
            Some("true"),
            "late-recorded bool field must reach the export"
        );
        assert_eq!(
            spans[0].labels.get("bytes").map(|s| s.as_str()),
            Some("4096"),
            "late-recorded u64 field must reach the export"
        );
    }

    /// The browser↔server join (spec §17.4): a server span that records `trace_id` +
    /// `parent_span_id` fields (as handle_subscribe does when the WS Subscribe carries trace
    /// context) must group under the *browser's* trace_id with the browser span as parent, not
    /// under a synthesized server-only trace_id. This is the mechanism that makes the
    /// click→paint waterfall show the client↔server delta.
    #[instrument(
        skip(handle, tid, parent),
        fields(
            op = "joined",
            trace_id = tracing::field::Empty,
            parent_span_id = tracing::field::Empty,
        )
    )]
    fn do_joined_work(handle: TraceHandle, tid: String, parent: String) {
        let _ = handle;
        // Simulate handle_subscribe recording the browser-supplied trace context.
        tracing::Span::current().record("trace_id", tid);
        tracing::Span::current().record("parent_span_id", parent);
    }

    #[test]
    fn server_span_with_propagated_trace_context_joins_browser_trace() {
        let handle = setup_armed();
        let layer = TraceCaptureLayer::new(handle.clone());
        let sub = tracing_subscriber::registry::Registry::default().with(layer);
        let dispatch = tracing::dispatcher::Dispatch::new(sub);
        const BROWSER_TRACE_ID: &str = "0af7651916cd43dd8448eb211c80319c";
        const BROWSER_PARENT_SPAN_ID: &str = "b7ad6b7169203331";
        tracing::dispatcher::with_default(&dispatch, || {
            do_joined_work(
                handle.clone(),
                BROWSER_TRACE_ID.into(),
                BROWSER_PARENT_SPAN_ID.into(),
            );
        });
        // Also drop a browser-style span straight into the buffer under the same trace_id, the
        // way POST /api/traces/ui would, so we can assert both spans land under one trace.
        handle.record(vec![RecordedSpan {
            name: "ui.open_thread".into(),
            trace_id: BROWSER_TRACE_ID.into(),
            span_id: BROWSER_PARENT_SPAN_ID.into(),
            parent_span_id: None,
            start_us: 0,
            end_us: 1,
            labels: HashMap::new(),
        }]);
        let traces = handle.flush(None);
        assert_eq!(
            traces.len(),
            1,
            "browser + server spans group under ONE trace_id"
        );
        assert_eq!(traces[0].trace_id, BROWSER_TRACE_ID);
        let by_name: HashMap<&str, &RecordedSpan> = traces[0]
            .spans
            .iter()
            .map(|s| (s.name.as_str(), s))
            .collect();
        assert!(by_name.contains_key("ui.open_thread"));
        let srv = by_name.get("do_joined_work").expect("server span present");
        assert_eq!(
            srv.trace_id, BROWSER_TRACE_ID,
            "server span joined the browser trace_id"
        );
        assert_eq!(
            srv.parent_span_id.as_deref(),
            Some(BROWSER_PARENT_SPAN_ID),
            "server span parented to the browser's ui.open_thread span"
        );
    }

    #[test]
    fn capture_layer_noop_when_disarmed() {
        let handle = TraceHandle::new(16, false);
        let layer = TraceCaptureLayer::new(handle.clone());
        let sub = tracing_subscriber::registry::Registry::default().with(layer);
        let dispatch = tracing::dispatcher::Dispatch::new(sub);
        tracing::dispatcher::with_default(&dispatch, || {
            do_work(handle.clone());
        });
        let traces = handle.flush(None);
        assert!(traces.is_empty(), "disarmed layer must not record");
    }
}
