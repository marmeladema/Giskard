//! A retained, cursor-based event log: the transport between a harness and the server.
//!
//! An appended event is kept until every reader has consumed it. With no reader, everything is
//! kept, so a reader created later starts at the oldest event nobody consumed. This is what makes
//! "subscribe after the first event" and "replace a reader mid-turn" lossless. The only way to
//! lose an event is the retention cap, and that loss is reported to the reader as an explicit
//! [`EventStreamError::Gap`] instead of happening silently.
//! An eviction that happened while no reader existed is reported to the next reader created.
//!
//! Pull model: readers wait on a [`Notify`]; there is no channel and no pump task. Reader creation
//! is synchronous, so a synchronous `AgentHarness::subscribe` stays possible.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;
use tracing::{error, warn};

/// Events retained per log beyond which the oldest unconsumed entry is evicted.
///
/// Reached only when no reader exists or a reader falls far behind; in normal operation the
/// server's forwarder consumes each event as it arrives. Eviction is reported as a `Gap`.
pub const EVENT_LOG_RETAIN_LIMIT: usize = 16_384;

/// Why a reader could not produce an event.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EventStreamError {
    /// The log was closed and every retained event has been consumed.
    #[error("event log closed")]
    Closed,
    /// `dropped` events were evicted by the retention cap before this reader consumed them. The
    /// reader continues from the oldest retained event on its next call.
    #[error("event log overflowed; {dropped} events dropped")]
    Gap { dropped: u64 },
}

struct LogState<T> {
    /// Sequence number of `entries[0]`.
    base: u64,
    entries: VecDeque<T>,
    /// Next sequence each live reader will consume.
    cursors: HashMap<u64, Cursor>,
    next_reader: u64,
    /// Evictions no reader could observe, reported to the next reader created.
    unreported_evictions: u64,
    closed: bool,
    /// Set once the cap has evicted at least one entry, so the error is logged once per log.
    overflowed: bool,
}

struct Cursor {
    next: u64,
    pending_gap: u64,
}

/// One thread's retained event log.
pub struct EventLog<T: Clone + Send + 'static = giskard_core::event::AgentEvent> {
    state: Mutex<LogState<T>>,
    notify: Notify,
    limit: usize,
}

impl<T> Default for EventLog<T>
where
    T: Clone + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> EventLog<T>
where
    T: Clone + Send + 'static,
{
    pub fn new() -> Self {
        Self::with_limit(EVENT_LOG_RETAIN_LIMIT)
    }

    /// A log with a custom retention cap; tests use a tiny cap to exercise `Gap`.
    pub fn with_limit(limit: usize) -> Self {
        Self {
            state: Mutex::new(LogState {
                base: 0,
                entries: VecDeque::new(),
                cursors: HashMap::new(),
                next_reader: 0,
                unreported_evictions: 0,
                closed: false,
                overflowed: false,
            }),
            notify: Notify::new(),
            limit: limit.max(1),
        }
    }

    fn lock(&self) -> MutexGuard<'_, LogState<T>> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("event log lock was poisoned; recovering log state");
                poisoned.into_inner()
            }
        }
    }

    /// Append one event. Returns `false` if the log is closed and the event was discarded.
    pub fn append(&self, event: T) -> bool {
        {
            let mut state = self.lock();
            if state.closed {
                return false;
            }
            state.entries.push_back(event);
            if state.entries.len() > self.limit {
                state.entries.pop_front();
                state.base += 1;
                if state.cursors.is_empty() {
                    state.unreported_evictions += 1;
                }
                if !state.overflowed {
                    state.overflowed = true;
                    error!(
                        limit = self.limit,
                        readers = state.cursors.len(),
                        "event log exceeded its retention cap; dropping the oldest unconsumed event"
                    );
                }
            }
        }
        self.notify.notify_waiters();
        true
    }

    /// Close the log. Readers drain what is retained, then receive `Closed`.
    pub fn close(&self) {
        self.lock().closed = true;
        self.notify.notify_waiters();
    }

    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// Live readers. Intended for tests that previously counted broadcast receivers.
    pub fn reader_count(&self) -> usize {
        self.lock().cursors.len()
    }

    /// Retained, not yet trimmed entries. Intended for diagnostics and tests.
    pub fn retained_len(&self) -> usize {
        self.lock().entries.len()
    }

    /// A reader positioned at the oldest event no reader has consumed.
    pub fn reader(self: &Arc<Self>) -> EventLogReader<T> {
        let mut state = self.lock();
        let id = state.next_reader;
        state.next_reader += 1;
        let base = state.base;
        let pending_gap = std::mem::take(&mut state.unreported_evictions);
        state.cursors.insert(
            id,
            Cursor {
                next: base,
                pending_gap,
            },
        );
        EventLogReader {
            log: Arc::clone(self),
            id,
        }
    }

    /// A reader over a closed, empty log: `recv` returns `Closed` immediately.
    pub fn closed_reader() -> EventLogReader<T> {
        let log = Arc::new(Self::new());
        log.close();
        log.reader()
    }

    fn trim(state: &mut LogState<T>) {
        let Some(min_cursor) = state.cursors.values().map(|cursor| cursor.next).min() else {
            return;
        };
        while state.base < min_cursor && !state.entries.is_empty() {
            state.entries.pop_front();
            state.base += 1;
        }
    }

    /// Advance one reader. `None` means "nothing available yet, wait".
    fn poll_reader(state: &mut LogState<T>, id: u64) -> Option<Result<T, EventStreamError>> {
        let cursor = state.cursors.get_mut(&id)?;
        if cursor.pending_gap > 0 {
            let dropped = std::mem::take(&mut cursor.pending_gap);
            return Some(Err(EventStreamError::Gap { dropped }));
        }
        let cursor = cursor.next;
        if cursor < state.base {
            let dropped = state.base - cursor;
            state.cursors.get_mut(&id)?.next = state.base;
            return Some(Err(EventStreamError::Gap { dropped }));
        }
        let offset = usize::try_from(cursor - state.base).ok()?;
        if let Some(event) = state.entries.get(offset) {
            let event = event.clone();
            state.cursors.get_mut(&id)?.next = cursor + 1;
            Self::trim(state);
            return Some(Ok(event));
        }
        if state.closed {
            return Some(Err(EventStreamError::Closed));
        }
        None
    }
}

/// A cursor over an [`EventLog`]. Dropping it releases the events only it was holding.
pub struct EventLogReader<T: Clone + Send + 'static = giskard_core::event::AgentEvent> {
    log: Arc<EventLog<T>>,
    id: u64,
}

impl<T> EventLogReader<T>
where
    T: Clone + Send + 'static,
{
    /// The next event, waiting if none is retained yet. Cancel-safe: a cancelled call consumes
    /// nothing.
    pub async fn recv(&mut self) -> Result<T, EventStreamError> {
        loop {
            let notified = self.log.notify.notified();
            tokio::pin!(notified);
            {
                let mut state = self.log.lock();
                if let Some(result) = EventLog::poll_reader(&mut state, self.id) {
                    return result;
                }
                // Register before releasing the lock so an append that lands in between is not
                // missed: `enable` makes `notify_waiters` count for this future.
                notified.as_mut().enable();
            }
            notified.await;
        }
    }
}

impl<T> Drop for EventLogReader<T>
where
    T: Clone + Send + 'static,
{
    fn drop(&mut self) {
        let mut state = self.log.lock();
        state.cursors.remove(&self.id);
        EventLog::trim(&mut state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use giskard_core::event::AgentEvent;
    use giskard_core::ids::{ThreadId, TurnId};
    use std::time::Duration;
    use tokio::time::timeout;

    fn notice(thread: ThreadId, message: &str) -> AgentEvent {
        AgentEvent::Notice {
            thread,
            turn: None,
            message: message.to_owned(),
        }
    }

    fn message_of(event: &AgentEvent) -> &str {
        match event {
            AgentEvent::Notice { message, .. } => message,
            other => panic!("expected notice, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn events_appended_before_the_first_reader_are_delivered() {
        let thread = ThreadId::new();
        let log = Arc::new(EventLog::new());
        assert!(log.append(notice(thread, "a")));
        assert!(log.append(notice(thread, "b")));
        let mut reader = log.reader();
        assert_eq!(message_of(&reader.recv().await.unwrap()), "a");
        assert_eq!(message_of(&reader.recv().await.unwrap()), "b");
        assert_eq!(log.retained_len(), 0, "consumed entries are trimmed");
    }

    #[tokio::test]
    async fn a_slow_reader_never_lags_below_the_cap() {
        let thread = ThreadId::new();
        let log = Arc::new(EventLog::new());
        let mut reader = log.reader();
        for i in 0..300 {
            log.append(notice(thread, &i.to_string()));
        }
        for i in 0..300 {
            assert_eq!(message_of(&reader.recv().await.unwrap()), i.to_string());
        }
    }

    #[tokio::test]
    async fn a_replacement_reader_starts_at_the_oldest_unconsumed_event() {
        let thread = ThreadId::new();
        let log = Arc::new(EventLog::new());
        let mut first = log.reader();
        log.append(notice(thread, "consumed"));
        assert_eq!(message_of(&first.recv().await.unwrap()), "consumed");
        drop(first);
        log.append(notice(thread, "after-drop"));
        let mut second = log.reader();
        assert_eq!(
            message_of(&second.recv().await.unwrap()),
            "after-drop",
            "the consumed event must not be replayed"
        );
    }

    #[tokio::test]
    async fn a_reader_waits_for_an_append_and_wakes() {
        let thread = ThreadId::new();
        let log = Arc::new(EventLog::new());
        let mut reader = log.reader();
        let producer = log.clone();
        let task = tokio::spawn(async move {
            tokio::task::yield_now().await;
            producer.append(notice(thread, "late"));
        });
        let event = timeout(Duration::from_secs(1), reader.recv())
            .await
            .expect("reader must wake on append")
            .unwrap();
        assert_eq!(message_of(&event), "late");
        task.await.unwrap();
    }

    #[tokio::test]
    async fn a_cancelled_recv_consumes_nothing() {
        let thread = ThreadId::new();
        let log = Arc::new(EventLog::new());
        let mut reader = log.reader();
        assert!(
            timeout(Duration::from_millis(20), reader.recv())
                .await
                .is_err(),
            "nothing to read yet"
        );
        log.append(notice(thread, "x"));
        assert_eq!(message_of(&reader.recv().await.unwrap()), "x");
    }

    #[tokio::test]
    async fn close_drains_then_reports_closed() {
        let thread = ThreadId::new();
        let log = Arc::new(EventLog::new());
        let mut reader = log.reader();
        log.append(notice(thread, "last"));
        log.close();
        assert!(!log.append(notice(thread, "ignored")));
        assert_eq!(message_of(&reader.recv().await.unwrap()), "last");
        assert_eq!(reader.recv().await.unwrap_err(), EventStreamError::Closed);
        let mut closed: EventLogReader = EventLog::closed_reader();
        assert_eq!(closed.recv().await.unwrap_err(), EventStreamError::Closed);
    }

    #[tokio::test]
    async fn the_cap_evicts_the_oldest_and_reports_a_gap_once() {
        let thread = ThreadId::new();
        let log = Arc::new(EventLog::with_limit(2));
        let mut reader = log.reader();
        for i in 0..5 {
            log.append(notice(thread, &i.to_string()));
        }
        assert_eq!(
            reader.recv().await.unwrap_err(),
            EventStreamError::Gap { dropped: 3 }
        );
        assert_eq!(message_of(&reader.recv().await.unwrap()), "3");
        assert_eq!(message_of(&reader.recv().await.unwrap()), "4");
    }

    #[tokio::test]
    async fn evictions_before_the_first_reader_are_reported_as_a_gap() {
        let thread = ThreadId::new();
        let log = Arc::new(EventLog::with_limit(2));
        for i in 0..5 {
            log.append(notice(thread, &i.to_string()));
        }
        let mut reader = log.reader();
        assert_eq!(
            reader.recv().await.unwrap_err(),
            EventStreamError::Gap { dropped: 3 }
        );
        assert_eq!(message_of(&reader.recv().await.unwrap()), "3");
        assert_eq!(message_of(&reader.recv().await.unwrap()), "4");
    }

    #[tokio::test]
    async fn evictions_between_readers_are_reported_to_the_next_reader() {
        let thread = ThreadId::new();
        let log = Arc::new(EventLog::with_limit(2));
        let mut first = log.reader();
        log.append(notice(thread, "consumed"));
        assert_eq!(message_of(&first.recv().await.unwrap()), "consumed");
        drop(first);

        for i in 0..5 {
            log.append(notice(thread, &i.to_string()));
        }
        let mut second = log.reader();
        assert_eq!(
            second.recv().await.unwrap_err(),
            EventStreamError::Gap { dropped: 3 }
        );
        assert_eq!(message_of(&second.recv().await.unwrap()), "3");
        assert_eq!(message_of(&second.recv().await.unwrap()), "4");
    }

    #[tokio::test]
    async fn a_second_reader_created_without_an_intervening_append_gets_no_gap() {
        let thread = ThreadId::new();
        let log = Arc::new(EventLog::with_limit(2));
        for i in 0..5 {
            log.append(notice(thread, &i.to_string()));
        }
        let mut first = log.reader();
        let mut second = log.reader();
        assert_eq!(
            first.recv().await.unwrap_err(),
            EventStreamError::Gap { dropped: 3 }
        );
        assert_eq!(message_of(&second.recv().await.unwrap()), "3");
    }

    #[tokio::test]
    async fn two_readers_each_see_every_event() {
        let thread = ThreadId::new();
        let log = Arc::new(EventLog::new());
        let mut a = log.reader();
        let mut b = log.reader();
        log.append(AgentEvent::TurnStarted {
            thread,
            turn: TurnId::new(),
        });
        assert!(matches!(
            a.recv().await.unwrap(),
            AgentEvent::TurnStarted { .. }
        ));
        assert_eq!(log.retained_len(), 1, "b has not consumed it yet");
        assert!(matches!(
            b.recv().await.unwrap(),
            AgentEvent::TurnStarted { .. }
        ));
        assert_eq!(log.retained_len(), 0);
        assert_eq!(log.reader_count(), 2);
    }
}
