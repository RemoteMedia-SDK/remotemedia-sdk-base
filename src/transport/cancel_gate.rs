//! Per-(session, node) cancellation primitive with barge-suppression.
//!
//! Wraps a `tokio::sync::Notify` with a protection counter so a node
//! can declare itself "uninterruptible" for a window — typically while
//! a tool call the LLM picked is mid-flight and we don't want a stray
//! VAD blip to cancel the in-flight HTTP stream and lose the tool
//! dispatch.
//!
//! # Mechanics
//!
//! - The router holds one `Arc<CancelGate>` per (session, node) and
//!   wires it into both the barge-filter task (publisher of cancels)
//!   and the dispatch `tokio::select!` (consumer of cancels via
//!   [`CancelGate::notified`]).
//! - The router's barge-filter calls [`CancelGate::try_cancel`] when a
//!   `barge_in` envelope arrives. If the protection counter is
//!   non-zero the call is a no-op and returns `false` — the in-flight
//!   future keeps running. Otherwise it wakes every waiter on the
//!   inner `Notify` (cascading-drop the dispatch future, which kills
//!   the HTTP stream / IPC send / etc).
//! - Nodes (typically the chat backend) take a [`ProtectGuard`] when
//!   they enter a non-cancelable region. The guard increments the
//!   counter on take and decrements on drop, so accidental panics or
//!   early returns can't leave the gate stuck open.
//!
//! Counter semantics are AcqRel — a barge-publisher and a guard-taker
//! racing each other will see consistent ordering: if `protect()`
//! happens-before `try_cancel()`, the cancel is suppressed; if
//! `try_cancel()` happens-before `protect()`, the cancel fires and the
//! protection arrives too late (acceptable — the node hadn't entered
//! its critical section yet).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Cancellation primitive with a protection counter.
///
/// Construct with [`CancelGate::new`] (returns a fresh, unprotected
/// gate), share via `Arc`. Cheap to clone; size is one `Notify` plus
/// one `AtomicUsize`.
#[derive(Debug)]
pub struct CancelGate {
    notify: Notify,
    protected: AtomicUsize,
}

impl CancelGate {
    /// Construct a new, unprotected gate with no waiters.
    pub fn new() -> Self {
        Self {
            notify: Notify::new(),
            protected: AtomicUsize::new(0),
        }
    }

    /// Take a protection guard. The returned [`ProtectGuard`] holds an
    /// `Arc<Self>`, so the gate is kept alive at least until the guard
    /// is dropped. While **any** guard is live, [`try_cancel`] is a
    /// no-op.
    ///
    /// Re-entrant: stacking guards is supported. The counter
    /// decrements one-for-one on drop.
    pub fn protect(self: &Arc<Self>) -> ProtectGuard {
        self.protected.fetch_add(1, Ordering::AcqRel);
        ProtectGuard {
            gate: Arc::clone(self),
        }
    }

    /// Returns `true` while at least one [`ProtectGuard`] is live.
    pub fn is_protected(&self) -> bool {
        self.protected.load(Ordering::Acquire) > 0
    }

    /// Wake every current waiter on the inner `Notify` unless the
    /// gate is currently protected.
    ///
    /// Returns `true` if the cancel was delivered, `false` if it was
    /// suppressed by an active [`ProtectGuard`]. Callers typically log
    /// the suppression so it's visible in traces.
    pub fn try_cancel(&self) -> bool {
        if self.is_protected() {
            return false;
        }
        self.notify.notify_waiters();
        true
    }

    /// Force a cancel through, ignoring protection.
    ///
    /// Reserved for shutdown paths (`terminate_session`, drop) where
    /// even a non-cancelable tool must yield. Normal barge handling
    /// uses [`try_cancel`].
    pub fn cancel_force(&self) {
        self.notify.notify_waiters();
    }

    /// Wait for the next cancel notification.
    ///
    /// Returns a future suitable for `tokio::select!`. Mirror of
    /// `Notify::notified`. The future is fused — if the gate is
    /// notified before this future is awaited, the wait may still
    /// block (same `Notify` semantics as upstream).
    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }
}

impl Default for CancelGate {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII handle returned by [`CancelGate::protect`].
///
/// Decrements the protection counter on drop. Holds an `Arc<CancelGate>`
/// so the gate outlives the guard even if the original owner is gone
/// — important when a guard is moved into a spawned task or held
/// across an `.await`.
#[must_use = "ProtectGuard must be held to keep barge suppression active"]
pub struct ProtectGuard {
    gate: Arc<CancelGate>,
}

impl Drop for ProtectGuard {
    fn drop(&mut self) {
        self.gate.protected.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_gate_is_not_protected() {
        let gate = CancelGate::new();
        assert!(!gate.is_protected());
    }

    #[test]
    fn try_cancel_succeeds_when_unprotected() {
        let gate = CancelGate::new();
        assert!(gate.try_cancel());
    }

    #[test]
    fn try_cancel_suppressed_while_guard_held() {
        let gate = Arc::new(CancelGate::new());
        let _g = gate.protect();
        assert!(gate.is_protected());
        assert!(!gate.try_cancel());
    }

    #[test]
    fn guard_drop_restores_cancellability() {
        let gate = Arc::new(CancelGate::new());
        {
            let _g = gate.protect();
            assert!(gate.is_protected());
        }
        assert!(!gate.is_protected());
        assert!(gate.try_cancel());
    }

    #[test]
    fn nested_guards_decrement_independently() {
        let gate = Arc::new(CancelGate::new());
        let g1 = gate.protect();
        let g2 = gate.protect();
        assert!(gate.is_protected());
        drop(g1);
        // Still protected — g2 is live.
        assert!(gate.is_protected());
        assert!(!gate.try_cancel());
        drop(g2);
        assert!(!gate.is_protected());
        assert!(gate.try_cancel());
    }

    #[test]
    fn cancel_force_bypasses_protection() {
        let gate = Arc::new(CancelGate::new());
        let _g = gate.protect();
        // Should not panic / hang. We don't observe the wake (no
        // waiters in this synchronous test) but we exercise the path.
        gate.cancel_force();
    }

    #[tokio::test]
    async fn notified_wakes_on_cancel() {
        let gate = Arc::new(CancelGate::new());
        let waiter_gate = Arc::clone(&gate);
        let waiter = tokio::spawn(async move {
            waiter_gate.notified().await;
        });
        // Yield once so the waiter registers.
        tokio::task::yield_now().await;
        assert!(gate.try_cancel());
        waiter.await.unwrap();
    }
}
