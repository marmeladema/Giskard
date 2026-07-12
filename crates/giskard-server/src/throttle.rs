//! Brute-force throttling for the single shared app password.
//!
//! The throttle is global rather than per-IP: Giskard is a single-user app that typically sits
//! behind a reverse proxy, where the peer address is the proxy and `X-Forwarded-For` is only as
//! trustworthy as the proxy configuration. A global lockout cannot be sidestepped by rotating
//! source addresses, and the legitimate (single) user is the only party inconvenienced by it.
//!
//! Policy: the first [`FREE_FAILURES`] consecutive failures are accepted immediately (typos).
//! Every consecutive failure after that locks the login endpoint for
//! `BASE_LOCKOUT_SECS * 2^(n - FREE_FAILURES)` seconds, capped at [`MAX_LOCKOUT_SECS`].
//! A successful login resets the counter. Lockout is checked *before* the Argon2 verification
//! runs, so a flood of wrong passwords cannot be used to burn server CPU/RAM either.
//!
//! State is in-memory only: a server restart forgives the counter, which is an acceptable
//! trade-off for a self-hosted tool (an attacker cannot restart the server, the operator can).

use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Consecutive failures tolerated before lockouts begin.
const FREE_FAILURES: u32 = 5;
/// Lockout applied by the first throttled failure; doubles per subsequent failure.
const BASE_LOCKOUT_SECS: u64 = 30;
/// Upper bound for a single lockout window (15 minutes).
const MAX_LOCKOUT_SECS: u64 = 900;

#[derive(Debug, Default)]
struct ThrottleState {
    consecutive_failures: u32,
    locked_until: Option<Instant>,
}

/// Global login throttle shared by all `/api/login` requests.
#[derive(Debug, Default)]
pub struct LoginThrottle {
    state: Mutex<ThrottleState>,
}

impl LoginThrottle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `Err(remaining)` while the endpoint is locked out.
    pub fn check(&self) -> Result<(), Duration> {
        let mut state = self.lock();
        if let Some(until) = state.locked_until {
            let now = Instant::now();
            if now < until {
                return Err(until - now);
            }
            state.locked_until = None;
        }
        Ok(())
    }

    /// Record a failed password attempt. Returns the consecutive-failure count and the lockout
    /// applied by this failure, if any.
    pub fn record_failure(&self) -> (u32, Option<Duration>) {
        let mut state = self.lock();
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        let failures = state.consecutive_failures;
        let lockout = lockout_for(failures);
        if let Some(duration) = lockout {
            state.locked_until = Some(Instant::now() + duration);
        }
        (failures, lockout)
    }

    /// Record a successful login: clears the failure counter and any pending lockout.
    pub fn record_success(&self) {
        let mut state = self.lock();
        state.consecutive_failures = 0;
        state.locked_until = None;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ThrottleState> {
        // A poisoned mutex means another login request panicked mid-update; the counter state is
        // still structurally valid (two integers), so recover rather than take the server down.
        self.state.lock().unwrap_or_else(|poisoned| {
            tracing::error!("login throttle mutex poisoned; recovering state");
            poisoned.into_inner()
        })
    }
}

fn lockout_for(consecutive_failures: u32) -> Option<Duration> {
    let over = consecutive_failures.checked_sub(FREE_FAILURES)?;
    let secs = BASE_LOCKOUT_SECS
        .saturating_mul(1u64.checked_shl(over).unwrap_or(u64::MAX))
        .min(MAX_LOCKOUT_SECS);
    Some(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_failures_do_not_lock() {
        let throttle = LoginThrottle::new();
        for _ in 0..FREE_FAILURES - 1 {
            let (_, lockout) = throttle.record_failure();
            assert!(lockout.is_none());
            assert!(throttle.check().is_ok());
        }
    }

    #[test]
    fn lockout_escalates_and_caps() {
        assert_eq!(lockout_for(FREE_FAILURES - 1), None);
        assert_eq!(
            lockout_for(FREE_FAILURES),
            Some(Duration::from_secs(BASE_LOCKOUT_SECS))
        );
        assert_eq!(
            lockout_for(FREE_FAILURES + 1),
            Some(Duration::from_secs(BASE_LOCKOUT_SECS * 2))
        );
        assert_eq!(
            lockout_for(FREE_FAILURES + 40),
            Some(Duration::from_secs(MAX_LOCKOUT_SECS))
        );
        // Shift overflow (n - FREE >= 64) must saturate at the cap, not panic.
        assert_eq!(
            lockout_for(FREE_FAILURES + 100),
            Some(Duration::from_secs(MAX_LOCKOUT_SECS))
        );
    }

    #[test]
    fn nth_failure_locks_and_success_resets() {
        let throttle = LoginThrottle::new();
        for _ in 0..FREE_FAILURES {
            throttle.record_failure();
        }
        assert!(throttle.check().is_err());
        throttle.record_success();
        assert!(throttle.check().is_ok());
        let (failures, _) = throttle.record_failure();
        assert_eq!(failures, 1);
    }
}
