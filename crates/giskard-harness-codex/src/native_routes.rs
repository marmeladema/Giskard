use std::collections::HashMap;

use giskard_core::error::HarnessError;
use giskard_core::ids::ThreadId;

use crate::native_ids::{NativeRouteEpoch, NativeThreadId};

/// One authoritative native-to-Giskard thread route for this harness lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct NativeRoute {
    pub(super) thread_id: ThreadId,
    pub(super) epoch: NativeRouteEpoch,
}

/// Owns the bijective native-thread routes established for one Codex worker.
#[derive(Default)]
pub(super) struct NativeThreadRoutes {
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Own native Codex thread routes and their allocation epochs.
    // Source of truth: Successful provider-protocol route claims establish each record.
    // Structural reason: The lower harness adapter must route native protocol identities.
    // Synchronization: The single Codex background task owns and mutates this component.
    // Invalidation/removal: Routes live for one harness process and drop on shutdown.
    by_native: HashMap<NativeThreadId, NativeRoute>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Enforce the reverse Giskard-to-native half of each bijective route claim.
    // Source of truth: The same successful claim that publishes `by_native` publishes this entry.
    // Structural reason: The adapter cannot depend on server authority types.
    // Synchronization: The single Codex background task owns and mutates this component.
    // Invalidation/removal: Routes live for one harness process and drop on shutdown.
    native_by_thread: HashMap<ThreadId, NativeThreadId>,
    /// Monotonic route epoch allocator scoped to one harness process.
    next_epoch: NativeRouteEpoch,
}

/// A non-empty native thread ID that has no established route.
#[derive(Debug)]
pub(super) struct UnknownNativeThread {
    pub(super) native_thread_id: NativeThreadId,
}

impl NativeThreadRoutes {
    /// Claims one normalized native/Giskard pair or returns its existing route.
    pub(super) fn claim(
        &mut self,
        native_thread_id: String,
        thread_id: ThreadId,
    ) -> Result<NativeRoute, HarnessError> {
        let native_thread_id = NativeThreadId::new(native_thread_id.trim().to_owned());
        if native_thread_id.as_str().is_empty() {
            return Err(HarnessError::Protocol(
                "cannot claim an empty native thread id".into(),
            ));
        }
        if let Some(existing) = self.by_native.get(&native_thread_id) {
            if existing.thread_id != thread_id {
                return Err(HarnessError::Protocol(format!(
                    "native thread {native_thread_id} is already bound to {}, not {thread_id}",
                    existing.thread_id
                )));
            }
            return Ok(*existing);
        }
        if let Some(existing) = self.native_by_thread.get(&thread_id) {
            return Err(HarnessError::Protocol(format!(
                "thread {thread_id} is already bound to native thread {existing}, not {native_thread_id}"
            )));
        }
        self.next_epoch = self
            .next_epoch
            .checked_next()
            .ok_or_else(|| HarnessError::Protocol("native route epoch space exhausted".into()))?;
        let route = NativeRoute {
            thread_id,
            epoch: self.next_epoch,
        };
        self.by_native.insert(native_thread_id.clone(), route);
        self.native_by_thread.insert(thread_id, native_thread_id);
        Ok(route)
    }

    /// Atomically replaces one exact native/Giskard route with a new native identity.
    pub(super) fn replace(
        &mut self,
        expected_native_thread_id: String,
        new_native_thread_id: String,
        thread_id: ThreadId,
    ) -> Result<NativeRoute, HarnessError> {
        let expected_native_thread_id =
            NativeThreadId::new(expected_native_thread_id.trim().to_owned());
        let new_native_thread_id = NativeThreadId::new(new_native_thread_id.trim().to_owned());
        if expected_native_thread_id.as_str().is_empty() || new_native_thread_id.as_str().is_empty()
        {
            return Err(HarnessError::Protocol(
                "cannot replace a route with an empty native thread id".into(),
            ));
        }
        let Some(expected_route) = self.by_native.get(&expected_native_thread_id) else {
            return Err(HarnessError::Protocol(format!(
                "native thread {expected_native_thread_id} is not bound to thread {thread_id}"
            )));
        };
        if expected_route.thread_id != thread_id {
            return Err(HarnessError::Protocol(format!(
                "native thread {expected_native_thread_id} is already bound to {}, not {thread_id}",
                expected_route.thread_id
            )));
        }
        if self.native_by_thread.get(&thread_id) != Some(&expected_native_thread_id) {
            return Err(HarnessError::Protocol(format!(
                "thread {thread_id} is not bound to expected native thread {expected_native_thread_id}"
            )));
        }
        if new_native_thread_id != expected_native_thread_id
            && let Some(existing) = self.by_native.get(&new_native_thread_id)
        {
            return Err(HarnessError::Protocol(format!(
                "native thread {new_native_thread_id} is already bound to {}, not {thread_id}",
                existing.thread_id
            )));
        }

        let next_epoch = self
            .next_epoch
            .checked_next()
            .ok_or_else(|| HarnessError::Protocol("native route epoch space exhausted".into()))?;
        let route = NativeRoute {
            thread_id,
            epoch: next_epoch,
        };
        self.by_native.remove(&expected_native_thread_id);
        self.by_native.insert(new_native_thread_id.clone(), route);
        self.native_by_thread
            .insert(thread_id, new_native_thread_id);
        self.next_epoch = next_epoch;
        Ok(route)
    }

    /// Returns the route for a normalized native ID when one is established.
    pub(super) fn route_for_native(&self, native_thread_id: &str) -> Option<NativeRoute> {
        self.by_native.get(native_thread_id.trim()).copied()
    }

    /// Resolves provider identity while preserving the mapper's scoped fallback rules.
    pub(super) fn resolve(
        &self,
        native_thread_id: &str,
        fallback: ThreadId,
    ) -> Result<ThreadId, UnknownNativeThread> {
        let native_thread_id = native_thread_id.trim();
        if native_thread_id.is_empty() {
            return Ok(fallback);
        }
        if let Some(route) = self.route_for_native(native_thread_id) {
            return Ok(route.thread_id);
        }
        if self.by_native.is_empty() {
            return Ok(fallback);
        }
        Err(UnknownNativeThread {
            native_thread_id: NativeThreadId::new(native_thread_id.to_owned()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_route_claims_are_idempotent_and_bijective() {
        let mut routes = NativeThreadRoutes::default();
        let first = ThreadId::new();
        let second = ThreadId::new();

        let route = routes.claim(" native-a ".into(), first).unwrap();
        assert_eq!(route.thread_id, first);
        assert_eq!(routes.route_for_native("native-a"), Some(route));
        assert_eq!(routes.claim("native-a".into(), first).unwrap(), route);
        assert!(routes.claim("native-a".into(), second).is_err());
        assert!(routes.claim("native-b".into(), first).is_err());
        assert!(routes.claim("   ".into(), second).is_err());

        let second_route = routes.claim("native-b".into(), second).unwrap();
        assert!(second_route.epoch > route.epoch);
    }

    #[test]
    fn native_route_replacement_is_exact_atomic_and_advances_epoch() {
        let mut routes = NativeThreadRoutes::default();
        let thread = ThreadId::new();
        let other = ThreadId::new();
        let original = routes.claim("native-old".into(), thread).unwrap();
        routes.claim("native-other".into(), other).unwrap();

        assert!(
            routes
                .replace("native-wrong".into(), "native-new".into(), thread)
                .is_err()
        );
        assert!(
            routes
                .replace("native-old".into(), "native-other".into(), thread)
                .is_err()
        );
        assert_eq!(routes.route_for_native("native-old"), Some(original));
        assert!(routes.route_for_native("native-new").is_none());

        let replacement = routes
            .replace("native-old".into(), "native-new".into(), thread)
            .unwrap();
        assert!(replacement.epoch > original.epoch);
        assert!(routes.route_for_native("native-old").is_none());
        assert_eq!(routes.route_for_native("native-new"), Some(replacement));
        assert_eq!(
            routes.claim("native-new".into(), thread).unwrap(),
            replacement
        );
    }
}
