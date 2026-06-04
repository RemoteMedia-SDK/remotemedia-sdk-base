//! Transport-abstracted control-bus client surface.
//!
//! `SessionControl` (the per-session pub/sub/intercept hub) is reached today
//! from one of two places: an in-process `Arc<SessionControl>` lookup (the
//! Rust API), or the `PipelineControl.Attach` gRPC bidi stream. This module
//! lets a single client surface (the Python `AttachedSession`) span both.
//!
//! Implementations:
//! - [`InProcControlTransport`] — direct `Arc<SessionControl>` access via
//!   [`session_control::global_bus`].
//! - (Python-side `_GrpcTransport` lives in
//!   `clients/python/remotemedia/control/transport.py` — gRPC is a Python
//!   client; no Rust transport wrapper is needed.)

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc};

use crate::data::RuntimeData;
use crate::transport::session_control::{
    self, ControlAddress, ControlEvent, InterceptDecision, NodeState, SessionControl,
};
use crate::Error;

// ─── Intercept types ─────────────────────────────────────────────────────────

/// One pending intercept request forwarded from the bus to the transport
/// caller. The caller must call [`InterceptReplyHandle::complete`] with the
/// `correlation_id` to unblock the data path.
pub struct InterceptRequest {
    /// Opaque identifier — pass back verbatim to [`InterceptReplyHandle::complete`].
    pub correlation_id: u64,
    /// The data frame the node emitted; inspect or replace it.
    pub data: RuntimeData,
}

/// Returned by [`ControlTransport::intercept`]. Contains the incoming request
/// stream and the handle to send decisions back into the bus.
///
/// Dropping this value automatically removes the intercept slot from the bus,
/// so callers do not need to call [`ControlTransport::remove_intercept`]
/// explicitly on early-return paths.
pub struct InterceptSession {
    /// Incoming intercept requests from the bus.
    pub requests: mpsc::Receiver<InterceptRequest>,
    /// Clone this to share with async reply handlers.
    pub reply: InterceptReplyHandle,
    // Private cleanup state — held so Drop can remove the bus slot.
    control: Arc<SessionControl>,
    addr: ControlAddress,
}

impl Drop for InterceptSession {
    /// Tear down the bus intercept slot if the caller dropped the session
    /// without calling `ControlTransport::remove_intercept` explicitly.
    /// Idempotent — `SessionControl::remove_intercept` no-ops if the slot
    /// is already gone.
    fn drop(&mut self) {
        self.control.remove_intercept(&self.addr);
    }
}

/// Cloneable handle for completing intercept decisions. Wraps the underlying
/// `Arc<SessionControl>` so callers don't need to hold the transport open.
#[derive(Clone)]
pub struct InterceptReplyHandle {
    inner: Arc<dyn InterceptReplyBackend>,
}

impl InterceptReplyHandle {
    /// Resolve a pending intercept. The `correlation_id` must match one
    /// previously received from [`InterceptSession::requests`].
    pub async fn complete(&self, correlation_id: u64, decision: InterceptDecision) {
        self.inner.complete(correlation_id, decision).await;
    }
}

#[async_trait]
trait InterceptReplyBackend: Send + Sync {
    async fn complete(&self, correlation_id: u64, decision: InterceptDecision);
}

struct InProcReplyBackend {
    control: Arc<SessionControl>,
}

#[async_trait]
impl InterceptReplyBackend for InProcReplyBackend {
    async fn complete(&self, correlation_id: u64, decision: InterceptDecision) {
        self.control.complete_intercept(correlation_id, decision);
    }
}

/// Async surface every control transport implements.
///
/// Lossy on backpressure (broadcast lag drops oldest) — matches the
/// gRPC tap path's semantics. Errors are coarse-grained on purpose;
/// callers translate to language-native exceptions.
#[async_trait]
pub trait ControlTransport: Send + Sync {
    async fn publish(&self, addr: &ControlAddress, data: RuntimeData) -> Result<(), Error>;

    /// Publish data to a ``.out`` address — bypasses the direction check of
    /// :method:`publish`.
    ///
    /// Used for control-plane communication where an external process must
    /// write to a ``.out`` address that a subscriber reads from.
    async fn publish_out(&self, addr: &ControlAddress, data: RuntimeData) -> Result<(), Error> {
        // Default: delegate to publish (works for transports that don't
        // enforce direction). Override in InProcControlTransport.
        self.publish(addr, data).await
    }

    /// Returns a `broadcast::Receiver` whose stream the caller bridges
    /// into its language-native iterator. `Closed`/`Lagged` semantics
    /// match `tokio::sync::broadcast`.
    async fn subscribe(
        &self,
        addr: &ControlAddress,
    ) -> Result<broadcast::Receiver<RuntimeData>, Error>;

    /// Subscribe to a ``.in`` address — bypasses the direction check of
    /// :method:`subscribe`.
    ///
    /// Used for control-plane communication where an external process must
    /// read from a ``.in`` address that the publisher writes to.
    async fn subscribe_in(
        &self,
        addr: &ControlAddress,
    ) -> Result<broadcast::Receiver<RuntimeData>, Error> {
        // Default: delegate to subscribe (works for transports that don't
        // enforce direction). Override in InProcControlTransport.
        self.subscribe(addr).await
    }

    async fn set_node_state(&self, node_id: &str, state: NodeState) -> Result<(), Error>;

    async fn clear_node_state(&self, node_id: &str) -> Result<(), Error>;

    /// Install an intercept on `addr`. Returns an [`InterceptSession`] whose
    /// `requests` channel yields one [`InterceptRequest`] per data frame that
    /// passes through the intercept point. The caller must call
    /// [`InterceptReplyHandle::complete`] within `deadline` or the frame is
    /// passed through unchanged.
    ///
    /// Only one intercept per address is supported; installing a second one
    /// replaces the first (the old request stream is closed).
    async fn intercept(
        &self,
        addr: &ControlAddress,
        deadline: Duration,
    ) -> Result<InterceptSession, Error>;

    /// Remove the intercept installed at `addr`. No-op if none is installed.
    async fn remove_intercept(&self, addr: &ControlAddress) -> Result<(), Error>;
}

/// In-process transport — holds an `Arc<SessionControl>` resolved at open
/// time via `session_control::global_bus().get(session_id)`.
pub struct InProcControlTransport {
    control: Arc<SessionControl>,
}

impl InProcControlTransport {
    /// Open against a session already registered with the process-global
    /// [`SessionControlBus`](session_control::SessionControlBus). Returns
    /// `None` if no such session exists — caller maps to a language-level
    /// `SessionNotFoundError`.
    pub fn open(session_id: &str) -> Option<Self> {
        let bus = session_control::global_bus()?;
        let control = bus.get(session_id)?;
        Some(Self { control })
    }

    /// Session id this transport is bound to.
    pub fn session_id(&self) -> &str {
        self.control.session_id()
    }
}

#[async_trait]
impl ControlTransport for InProcControlTransport {
    async fn publish(&self, addr: &ControlAddress, data: RuntimeData) -> Result<(), Error> {
        self.control
            .publish(addr, data)
            .await
            .map_err(|e| Error::Execution(format!("publish failed: {e}")))
    }

    async fn publish_out(&self, addr: &ControlAddress, data: RuntimeData) -> Result<(), Error> {
        self.control
            .publish_out(addr, data)
            .await
            .map_err(|e| Error::Execution(format!("publish failed: {e}")))
    }

    async fn subscribe(
        &self,
        addr: &ControlAddress,
    ) -> Result<broadcast::Receiver<RuntimeData>, Error> {
        if addr.node_id == "__system__" {
            // Route through subscribe_system() for retained-buffer replay.
            // SystemEventReceiver yields retained events first (in publication
            // order), then transitions to the live broadcast stream.
            let mut sys_rx = self.control.subscribe_system();
            let (tx, rx) = broadcast::channel(64);
            tokio::spawn(async move {
                while let Some(rd) = sys_rx.recv().await {
                    // RuntimeData is returned directly from SystemEventReceiver::recv.
                    if tx.send(rd).is_err() {
                        break;
                    }
                }
            });
            return Ok(rx);
        }
        self.control
            .subscribe(addr)
            .map_err(|e| Error::Execution(format!("subscribe failed: {e}")))
    }

    async fn subscribe_in(
        &self,
        addr: &ControlAddress,
    ) -> Result<broadcast::Receiver<RuntimeData>, Error> {
        self.control
            .subscribe_in(addr)
            .map_err(|e| Error::Execution(format!("subscribe failed: {e}")))
    }

    async fn set_node_state(&self, node_id: &str, state: NodeState) -> Result<(), Error> {
        self.control.set_node_state(node_id, state);
        Ok(())
    }

    async fn clear_node_state(&self, node_id: &str) -> Result<(), Error> {
        self.control.clear_node_state(node_id);
        Ok(())
    }

    async fn intercept(
        &self,
        addr: &ControlAddress,
        deadline: Duration,
    ) -> Result<InterceptSession, Error> {
        // Ask the bus for an mpsc::Receiver<ControlEvent> carrying
        // InterceptRequest variants. Pass deadline as Some so the bus slot
        // uses our deadline rather than its own default.
        let mut bus_rx = self
            .control
            .intercept(addr, Some(deadline))
            .map_err(|e| Error::Execution(format!("intercept failed: {e}")))?;

        // Bounded forwarding channel — same capacity as the bus tap.
        let (fwd_tx, fwd_rx) = mpsc::channel::<InterceptRequest>(1024);

        // Spawn a task that drains bus events into the forwarding channel.
        // When either end closes the task exits naturally; no handle is
        // stored because remove_intercept drops the slot on the bus side,
        // causing bus_rx to return None and the task to exit.
        //
        // Anonymous spawn — name with tokio::task::Builder when the workspace
        // enables tokio's `tracing` feature for console diagnostics.
        tokio::spawn(async move {
            while let Some(event) = bus_rx.recv().await {
                if let ControlEvent::InterceptRequest {
                    correlation_id,
                    data,
                    ..
                } = event
                {
                    if fwd_tx
                        .send(InterceptRequest {
                            correlation_id,
                            data,
                        })
                        .await
                        .is_err()
                    {
                        // Caller dropped the receiver — stop forwarding.
                        break;
                    }
                }
            }
        });

        let reply = InterceptReplyHandle {
            inner: Arc::new(InProcReplyBackend {
                control: Arc::clone(&self.control),
            }),
        };

        Ok(InterceptSession {
            requests: fwd_rx,
            reply,
            control: Arc::clone(&self.control),
            addr: addr.clone(),
        })
    }

    async fn remove_intercept(&self, addr: &ControlAddress) -> Result<(), Error> {
        self.control.remove_intercept(addr);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::session_control::{SessionControl, SessionControlBus};

    #[tokio::test]
    async fn open_returns_none_for_unknown_session() {
        // Don't install a global bus — open should fail cleanly.
        assert!(InProcControlTransport::open("nonexistent-session-id").is_none());
    }

    /// Verify that `subscribe` wires up to the bus tap so that events
    /// published directly via `control.publish_tap` are delivered.
    /// Uses `publish_tap` rather than `transport.publish` so the subscribe
    /// path is tested independently of the inject-input path.
    #[tokio::test]
    async fn subscribe_receives_tap_broadcast() {
        // install_global is first-writer-wins; get whichever bus is active.
        let fresh = SessionControlBus::new();
        SessionControlBus::install_global(fresh);
        let bus = session_control::global_bus().unwrap();
        let control = SessionControl::new("ct-test-session-1");
        bus.register(control.clone());

        let transport = InProcControlTransport::open("ct-test-session-1").unwrap();
        let out_addr = ControlAddress::node_out("echo");
        let mut sub = transport.subscribe(&out_addr).await.unwrap();

        // Inject directly on the bus to model "node emitted this".
        control.publish_tap("echo", None, RuntimeData::Text("hello".into()));

        let received = tokio::time::timeout(std::time::Duration::from_millis(500), sub.recv())
            .await
            .expect("subscribe timed out")
            .unwrap();

        match received {
            RuntimeData::Text(s) => assert_eq!(s, "hello"),
            _ => panic!("unexpected variant: {received:?}"),
        }
    }

    /// Verify that `transport.publish` returns a structured error when no
    /// input sender is attached (i.e. the router has not been wired up).
    /// This is the expected contract for a bare `SessionControl::new` in tests.
    #[tokio::test]
    async fn publish_without_router_returns_execution_error() {
        // install_global is first-writer-wins; get whichever bus is active.
        let fresh = SessionControlBus::new();
        SessionControlBus::install_global(fresh);
        let bus = session_control::global_bus().unwrap();
        let control = SessionControl::new("ct-test-session-2");
        bus.register(control.clone());

        let transport = InProcControlTransport::open("ct-test-session-2").unwrap();
        let in_addr = ControlAddress::node_in("echo");

        // No input sender attached (no router) — publish must return a structured
        // Execution error, not panic. Assert the specific variant and message.
        let err = transport
            .publish(&in_addr, RuntimeData::Text("payload".into()))
            .await
            .unwrap_err();

        // String fingerprint sourced from session_control.rs's "session control not
        // yet attached to router" error — update this test if the bus refactors it.
        match err {
            Error::Execution(msg) => assert!(
                msg.contains("not yet attached to router"),
                "unexpected error message: {msg}"
            ),
            other => panic!("expected Error::Execution, got {other:?}"),
        }
    }

    /// Verify that an installed intercept can replace a frame before the
    /// router propagates it downstream.
    ///
    /// Flow:
    ///   1. `transport.intercept()` installs the intercept slot on the bus.
    ///   2. A background task drives `control.on_node_output` — this is the
    ///      real path that the session router calls, which goes through the
    ///      intercept slot.
    ///   3. The intercept handler reads each `InterceptRequest` and calls
    ///      `reply.complete(id, Replace(...))`.
    ///   4. `on_node_output` unblocks and returns the replacement data.
    ///
    /// Note: `publish_tap` is a direct tap broadcast that intentionally
    /// bypasses the intercept path. The intercept gates what the router
    /// propagates (`on_node_output` return value), not what tap subscribers
    /// observe.
    #[tokio::test]
    async fn intercept_replace_mutates_tap() {
        let fresh = SessionControlBus::new();
        SessionControlBus::install_global(fresh);
        let bus = session_control::global_bus().unwrap();
        let control = SessionControl::new("ct-test-intercept");
        bus.register(control.clone());

        let transport = InProcControlTransport::open("ct-test-intercept").unwrap();
        let addr = ControlAddress::node_out("echo");

        let mut session = transport
            .intercept(&addr, Duration::from_millis(500))
            .await
            .unwrap();

        // Drive on_node_output in a background task — it will block until
        // the intercept is resolved.
        let ctrl_dp = control.clone();
        let data_task = tokio::spawn(async move {
            ctrl_dp
                .on_node_output("echo", None, RuntimeData::Text("original".into()))
                .await
        });

        // Receive the intercept request via the transport layer and reply Replace.
        let reply = session.reply.clone();
        let req = tokio::time::timeout(Duration::from_millis(500), session.requests.recv())
            .await
            .expect("intercept request timed out")
            .expect("intercept channel closed");

        reply
            .complete(
                req.correlation_id,
                InterceptDecision::Replace(RuntimeData::Text("rewritten".into())),
            )
            .await;

        // on_node_output should return the replacement.
        let result = tokio::time::timeout(Duration::from_millis(500), data_task)
            .await
            .expect("data task timed out")
            .expect("task panicked")
            .expect("on_node_output returned None");

        match result {
            RuntimeData::Text(s) => assert_eq!(s, "rewritten"),
            _ => panic!("unexpected variant: {result:?}"),
        }
    }

    /// Verify that `subscribe` wires up to the bus tap for `__perf__` so that
    /// events published via `control.publish_tap` are delivered. This proves
    /// the ordinary subscribe path (without any special-casing) works for
    /// virtual nodes like `__perf__`.
    #[tokio::test]
    async fn subscribe_to_perf_topic_via_ordinary_subscribe() {
        use crate::data::RuntimeData;
        use crate::transport::session_control::{SessionControl, SessionControlBus};

        let fresh = SessionControlBus::new();
        SessionControlBus::install_global(fresh);
        let bus = session_control::global_bus().unwrap();
        let control = SessionControl::new("ct-test-session-perf");
        bus.register(control.clone());

        let transport = InProcControlTransport::open("ct-test-session-perf").unwrap();
        let mut sub = transport
            .subscribe(&ControlAddress::node_out("__perf__"))
            .await
            .unwrap();

        control.publish_tap("__perf__", None, RuntimeData::Text(r#"{"cpu":0.4}"#.into()));

        let observed = tokio::time::timeout(std::time::Duration::from_millis(500), sub.recv())
            .await
            .expect("perf tap timed out")
            .unwrap();

        match observed {
            RuntimeData::Text(s) => assert!(s.contains("cpu")),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    /// Verify that subscribing to `__system__.out` replays events that were
    /// published BEFORE the subscription was installed (replay-buffer semantics).
    #[tokio::test]
    async fn system_subscribe_replays_retained_events() {
        use crate::data::RuntimeData;
        use crate::transport::session_control::{SessionControl, SessionControlBus};

        let fresh = SessionControlBus::new();
        SessionControlBus::install_global(fresh);
        let bus = session_control::global_bus().unwrap();
        let control = SessionControl::new("ct-test-system-replay");
        bus.register(control.clone());

        // Publish BEFORE subscribing — these should still be visible via replay.
        control.publish_tap("__system__", None, RuntimeData::Text("loading_a".into()));
        control.publish_tap("__system__", None, RuntimeData::Text("loading_b".into()));

        let transport = InProcControlTransport::open("ct-test-system-replay").unwrap();
        let mut sub = transport
            .subscribe(&ControlAddress::node_out("__system__"))
            .await
            .unwrap();

        let first = tokio::time::timeout(std::time::Duration::from_millis(500), sub.recv())
            .await
            .expect("system replay timed out")
            .unwrap();
        let second = sub.recv().await.unwrap();

        match (first, second) {
            (RuntimeData::Text(a), RuntimeData::Text(b)) => {
                assert_eq!(a, "loading_a");
                assert_eq!(b, "loading_b");
            }
            other => panic!("unexpected events: {other:?}"),
        }
    }

    /// Verify that dropping an `InterceptSession` removes the bus slot so
    /// subsequent `on_node_output` calls are not blocked by a stale intercept.
    ///
    /// Installing a second intercept on the same address replaces (not rejects)
    /// the first, so we verify cleanup via observable behavior: after the
    /// session is dropped, `on_node_output` completes immediately (no blocking
    /// intercept waiting for a reply that will never arrive).
    #[tokio::test]
    async fn intercept_session_drop_removes_bus_slot() {
        let fresh = SessionControlBus::new();
        SessionControlBus::install_global(fresh);
        let bus = session_control::global_bus().unwrap();
        let control = SessionControl::new("ct-test-intercept-drop");
        bus.register(control.clone());

        let transport = InProcControlTransport::open("ct-test-intercept-drop").unwrap();
        let addr = ControlAddress::node_out("echo");

        {
            let _session = transport
                .intercept(&addr, std::time::Duration::from_millis(100))
                .await
                .unwrap();
            // Bus slot is installed. Drop happens at end of block.
        }

        // After drop, the intercept slot must be gone. If it were still
        // installed, on_node_output would block until the deadline (100ms)
        // expires. A live slot with no replying handler would cause this to
        // time out. We give it a tight budget — if it passes, the slot was
        // released by Drop.
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            control.on_node_output("echo", None, RuntimeData::Text("after-drop".into())),
        )
        .await
        .expect("on_node_output blocked — intercept slot was not removed by Drop");

        match result {
            Some(RuntimeData::Text(s)) => assert_eq!(s, "after-drop"),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// Two `PipelineExecutor`s constructed back-to-back must both see the same
    /// global bus — regression test for the SHARED_EXECUTOR fix.
    #[tokio::test]
    async fn two_executors_share_global_bus() {
        use crate::transport::executor::PipelineExecutor;

        let e1 = PipelineExecutor::new().expect("executor 1");
        let bus_a = session_control::global_bus().expect("global bus after e1");
        let e2 = PipelineExecutor::new().expect("executor 2");
        let bus_b = session_control::global_bus().expect("global bus after e2");
        assert!(
            Arc::ptr_eq(&bus_a, &bus_b),
            "global bus must be the same instance after constructing two executors"
        );
        assert!(
            Arc::ptr_eq(&e1.control_bus(), &e2.control_bus()),
            "PipelineExecutor.control_bus() must return the same Arc for both executors"
        );
    }
}
