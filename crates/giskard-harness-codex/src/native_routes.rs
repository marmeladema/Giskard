use std::collections::HashMap;

use giskard_core::error::HarnessError;
use giskard_core::ids::ThreadId;

/// One authoritative native-to-Giskard thread route for this harness lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct NativeRoute {
    pub(super) thread_id: ThreadId,
    pub(super) epoch: u64,
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
    by_native: HashMap<String, NativeRoute>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Enforce the reverse Giskard-to-native half of each bijective route claim.
    // Source of truth: The same successful claim that publishes `by_native` publishes this entry.
    // Structural reason: The adapter cannot depend on server authority types.
    // Synchronization: The single Codex background task owns and mutates this component.
    // Invalidation/removal: Routes live for one harness process and drop on shutdown.
    native_by_thread: HashMap<ThreadId, String>,
    /// Monotonic route epoch allocator scoped to one harness process.
    next_epoch: u64,
}

/// A non-empty native thread ID that has no established route.
#[derive(Debug)]
pub(super) struct UnknownNativeThread {
    pub(super) native_thread_id: String,
}

impl NativeThreadRoutes {
    /// Claims one normalized native/Giskard pair or returns its existing route.
    pub(super) fn claim(
        &mut self,
        native_thread_id: String,
        thread_id: ThreadId,
    ) -> Result<NativeRoute, HarnessError> {
        let native_thread_id = native_thread_id.trim().to_owned();
        if native_thread_id.is_empty() {
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
            .checked_add(1)
            .ok_or_else(|| HarnessError::Protocol("native route epoch space exhausted".into()))?;
        let route = NativeRoute {
            thread_id,
            epoch: self.next_epoch,
        };
        self.by_native.insert(native_thread_id.clone(), route);
        self.native_by_thread.insert(thread_id, native_thread_id);
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
            native_thread_id: native_thread_id.to_owned(),
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
}
