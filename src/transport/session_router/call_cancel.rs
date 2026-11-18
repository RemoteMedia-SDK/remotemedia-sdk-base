//! A node instance's call cancellation in the router (openspec:
//! add-generator-cancellation).
//!
//! The filter task decides a cancel — its reason and the explicit choices of
//! the canceller and the manifest — and records it here before waking the
//! main task. The main task fires the in-flight call's token with it, after
//! re-resolving it with the node's own default for this cancel, spawns the
//! node's `on_cancel`, and only then drops the call's future. Token callbacks
//! (a plugin's `cancel_call`, a worker's cancel frame) therefore always see
//! the real cancellation before the drop.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use remotemedia_traits::{
    CancelContext, CancelReason, CancelRequest, CancelSource, Cancellation, DefaultDispositions,
    DispositionOverride, Retention,
};

use crate::nodes::{CallCancel, StreamingNode};

/// What the router knows about a node's cancellation before any call.
#[derive(Debug, Clone)]
pub(crate) struct CancelProfile {
    /// The node declares a generator role. Until it does, a barge keeps
    /// today's behavior (queued inputs are dropped).
    pub(crate) declared: bool,
    /// The node's default per reason, for decisions that cannot wait for a
    /// per-cancel default (the router's own queue).
    pub(crate) defaults: DefaultDispositions,
    /// The manifest node's `on_cancel`.
    pub(crate) manifest: Option<DispositionOverride>,
}

impl CancelProfile {
    pub(crate) fn of(node: &dyn StreamingNode, manifest: Option<DispositionOverride>) -> Self {
        CancelProfile {
            declared: node.generator_declaration().is_generator(),
            defaults: remotemedia_traits::default_dispositions(|ctx| node.default_disposition(ctx)),
            manifest,
        }
    }

    /// The cancel the filter records: the explicit choices (canceller over
    /// manifest) on top of the node's declared default.
    pub(crate) fn request(
        &self,
        reason: CancelReason,
        canceller: Option<DispositionOverride>,
    ) -> CancelRequest {
        let explicit = canceller
            .unwrap_or_default()
            .or(self.manifest.unwrap_or_default());
        let node_default = self
            .defaults
            .get(&reason)
            .copied()
            .unwrap_or_else(|| reason.default_disposition());
        CancelRequest {
            cancellation: Cancellation::with_disposition(reason, node_default.with(explicit)),
            explicit,
            elapsed_ms: None,
        }
    }

    /// Whether the router drops the inputs queued for this node.
    pub(crate) fn discard_pending(&self, request: &CancelRequest) -> bool {
        if !self.declared && request.explicit.pending_input.is_none() {
            // Not migrated: keep today's behavior. A barge drops the queue; a
            // newer input never did.
            return request.cancellation.reason != CancelReason::Superseded;
        }
        request.cancellation.disposition.pending_input == Retention::Discard
    }
}

struct InFlight {
    source: CancelSource,
    request: Arc<OnceLock<CancelRequest>>,
    started: Instant,
}

/// The in-flight call of one node instance, and the cancel decided for it.
#[derive(Default)]
pub(crate) struct CallCancelSlot {
    current: Mutex<Option<InFlight>>,
    pending: Mutex<Option<CancelRequest>>,
}

impl CallCancelSlot {
    fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        m.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A call starts: its context's cancellation. Clears a cancel recorded
    /// while nothing was in flight.
    pub(crate) fn begin(&self) -> CallCancel {
        let (source, cancel) = CallCancel::new();
        *Self::lock(&self.pending) = None;
        *Self::lock(&self.current) = Some(InFlight {
            source,
            request: cancel.request.clone(),
            started: Instant::now(),
        });
        cancel
    }

    /// The call ended (finished or dropped).
    pub(crate) fn end(&self) {
        *Self::lock(&self.current) = None;
    }

    /// The filter decided a cancel; the main task fires it.
    pub(crate) fn record(&self, request: CancelRequest) {
        *Self::lock(&self.pending) = Some(request);
    }

    /// The cancel was suppressed (`ProtectGuard`): forget it.
    pub(crate) fn forget(&self) {
        *Self::lock(&self.pending) = None;
    }

    /// Fire the in-flight call's token: with the recorded cancel, or with
    /// `fallback` when none was recorded (shutdown). The node re-resolves
    /// the explicit choices with its default for this cancel. Returns the
    /// cancellation fired, if a call was in flight and not already cancelled.
    pub(crate) fn fire(
        &self,
        node: &dyn StreamingNode,
        fallback: CancelReason,
        profile: &CancelProfile,
    ) -> Option<Cancellation> {
        let recorded = Self::lock(&self.pending).take();
        let current = Self::lock(&self.current);
        let call = current.as_ref()?;
        let mut request = recorded.unwrap_or_else(|| profile.request(fallback, None));
        let elapsed = call.started.elapsed();
        request.elapsed_ms = Some(elapsed.as_millis() as u64);
        let cancellation = request.resolve_with(node.default_disposition(&CancelContext {
            reason: request.cancellation.reason,
            elapsed: Some(elapsed),
        }));
        request.cancellation = cancellation;
        // Adapters forward the request (with the explicit choices) across a
        // boundary; set it before the token's callbacks run.
        let _ = call.request.set(request);
        let source = call.source.clone();
        drop(current);
        source.cancel(cancellation).then_some(cancellation)
    }
}

/// Run a generator's `on_cancel` without holding up the drop of its call.
pub(crate) fn spawn_on_cancel(
    node: &Arc<dyn StreamingNode>,
    session_id: &str,
    node_id: &str,
    cancellation: Cancellation,
) {
    if node.generator().is_none() {
        return;
    }
    let node = node.clone();
    let call = remotemedia_traits::rpc::CallContext {
        session_id: Some(session_id.to_string()),
        node_id: node_id.to_string(),
    };
    tokio::spawn(async move {
        if let Some(role) = node.generator() {
            role.cancellable().on_cancel(&call, &cancellation).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use remotemedia_traits::Disposition;

    fn profile(declared: bool, defaults: Disposition) -> CancelProfile {
        CancelProfile {
            declared,
            defaults: remotemedia_traits::default_dispositions(|_| defaults),
            manifest: None,
        }
    }

    #[test]
    fn undeclared_nodes_keep_todays_queue_behavior() {
        let p = profile(false, Disposition::INTERRUPT);
        assert!(p.discard_pending(&p.request(CancelReason::BargeIn, None)));
        assert!(!p.discard_pending(&p.request(CancelReason::Superseded, None)));
        // An explicit choice applies to any node.
        let keep = DispositionOverride {
            pending_input: Some(Retention::Retain),
            ..Default::default()
        };
        assert!(!p.discard_pending(&p.request(CancelReason::BargeIn, Some(keep))));
    }

    #[test]
    fn a_generators_interrupt_keeps_its_queue() {
        let p = profile(true, Disposition::INTERRUPT);
        assert!(!p.discard_pending(&p.request(CancelReason::BargeIn, None)));
        let tts = profile(
            true,
            Disposition {
                pending_input: Retention::Discard,
                ..Disposition::INTERRUPT
            },
        );
        assert!(tts.discard_pending(&tts.request(CancelReason::BargeIn, None)));
    }

    #[test]
    fn canceller_beats_manifest_beats_node() {
        let mut p = profile(true, Disposition::INTERRUPT);
        p.manifest = Some(DispositionOverride {
            partial_output: Some(Retention::Discard),
            pending_input: Some(Retention::Discard),
        });
        let req = p.request(
            CancelReason::BargeIn,
            Some(DispositionOverride {
                pending_input: Some(Retention::Retain),
                ..Default::default()
            }),
        );
        assert_eq!(
            req.cancellation.disposition.partial_output,
            Retention::Discard
        );
        assert_eq!(
            req.cancellation.disposition.pending_input,
            Retention::Retain
        );
        assert!(!p.discard_pending(&req));
    }
}
