//! NAPI surface for the in-proc control bus.
//!
//! Mirrors [`crate::control_session`] (the PyO3 surface). Exposes
//! [`NapiAttachedSession`] + [`NapiSubscription`] + [`NapiInterceptSession`]
//! to Node.js so the JS client surface in `crates/transports/ffi/nodejs/control/`
//! can run the full subscribe/publish/intercept/set-node-state surface against
//! a `NapiStreamingSession` without going through gRPC.
//!
//! Errors:
//! - Reasons prefixed `SessionNotFoundError: ...` are translated by the JS
//!   adapter to a `SessionNotFoundError` instance.
//! - Reasons prefixed `ControlAddressError: ...` translate similarly. Both
//!   prefixes mirror the Python exception class names so cross-language docs
//!   stay synchronized.

use std::sync::Arc;
use std::time::Duration;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::{broadcast, Mutex};

use remotemedia_core::data::RuntimeData;
use remotemedia_core::transport::control_transport::{
    ControlTransport, InProcControlTransport, InterceptReplyHandle, InterceptSession,
};
use remotemedia_core::transport::session_control::{
    ControlAddress, Direction, InterceptDecision, NodeState,
};

use super::pipeline::NapiRuntimeData;

// ─── Error helpers ───────────────────────────────────────────────────────────

/// Reason prefix the JS adapter looks for to rethrow as `SessionNotFoundError`.
pub const SESSION_NOT_FOUND_PREFIX: &str = "SessionNotFoundError";
/// Reason prefix the JS adapter looks for to rethrow as `ControlAddressError`.
pub const CONTROL_ADDRESS_ERROR_PREFIX: &str = "ControlAddressError";

fn session_not_found(msg: impl Into<String>) -> Error {
    Error::from_reason(format!("{}: {}", SESSION_NOT_FOUND_PREFIX, msg.into()))
}

fn control_address_error(msg: impl Into<String>) -> Error {
    Error::from_reason(format!("{}: {}", CONTROL_ADDRESS_ERROR_PREFIX, msg.into()))
}

// ─── Address parsing ─────────────────────────────────────────────────────────

/// Parse `"node.in[.port]"` / `"node.out[.port]"` into a [`ControlAddress`].
///
/// Identical semantics to `parse_address` in [`crate::control_session`]; kept
/// duplicated to avoid pulling the PyO3 surface into NAPI builds (the two
/// modules are gated by different cargo features).
fn parse_address(spec: &str) -> Result<ControlAddress> {
    let (node_part, rest) = spec.split_once('.').ok_or_else(|| {
        control_address_error(format!(
            "invalid control address {spec:?}; expected 'node.in[.port]' or 'node.out[.port]'"
        ))
    })?;
    if node_part.is_empty() {
        return Err(control_address_error(format!(
            "invalid control address {spec:?}: empty node_id"
        )));
    }
    let (dir_str, port) = match rest.split_once('.') {
        Some((d, p)) => (d, Some(p.to_string())),
        None => (rest, None),
    };
    let direction = match dir_str {
        "in" => Direction::In,
        "out" => Direction::Out,
        other => {
            return Err(control_address_error(format!(
                "invalid direction {other:?} in address {spec:?}; expected 'in' or 'out'"
            )));
        }
    };
    let mut addr = match direction {
        Direction::In => ControlAddress::node_in(node_part),
        Direction::Out => ControlAddress::node_out(node_part),
    };
    if let Some(p) = port {
        if !p.is_empty() {
            addr = addr.with_port(p);
        }
    }
    Ok(addr)
}

fn node_state_from_u8(v: u8) -> Result<NodeState> {
    match v {
        0 | 1 => Ok(NodeState::Enabled),
        2 => Ok(NodeState::Bypass),
        3 => Ok(NodeState::Disabled),
        other => Err(Error::from_reason(format!(
            "unknown NodeState value {other}; expected 0-3"
        ))),
    }
}

// ─── NapiAttachedSession ─────────────────────────────────────────────────────

/// In-proc control attach handle. Mirror of `PyAttachedSession`.
///
/// Wraps an [`InProcControlTransport`] resolved against the process-global
/// `SessionControlBus`. Obtain via `NapiStreamingSession.control()`.
#[napi]
pub struct NapiAttachedSession {
    transport: Arc<InProcControlTransport>,
}

#[napi]
impl NapiAttachedSession {
    /// Session id this attach is bound to.
    #[napi(getter)]
    pub fn session_id(&self) -> String {
        self.transport.session_id().to_string()
    }

    /// Subscribe to outputs from a node port.
    ///
    /// `address` format: `"node_id.out[.port]"`.
    #[napi]
    pub async fn subscribe(&self, address: String) -> Result<NapiSubscription> {
        let addr = parse_address(&address)?;
        if addr.direction != Direction::Out {
            return Err(control_address_error(
                "subscribe requires an '.out' address",
            ));
        }
        let rx = self
            .transport
            .subscribe(&addr)
            .await
            .map_err(|e| Error::from_reason(format!("subscribe failed: {e}")))?;
        Ok(NapiSubscription {
            rx: Arc::new(Mutex::new(rx)),
        })
    }

    /// Inject `data` at a node's input.
    ///
    /// `address` format: `"node_id.in[.port]"`.
    #[napi]
    pub async fn publish(&self, address: String, data: &NapiRuntimeData) -> Result<()> {
        let addr = parse_address(&address)?;
        if addr.direction != Direction::In {
            return Err(control_address_error("publish requires an '.in' address"));
        }
        self.transport
            .publish(&addr, data.get_inner().clone())
            .await
            .map_err(|e| Error::from_reason(format!("publish failed: {e}")))?;
        Ok(())
    }

    /// Set a node's execution state.
    ///
    /// `state` is the numeric `NodeState` value: 0/1 = Enabled, 2 = Bypass,
    /// 3 = Disabled. Matches the Python enum encoding.
    #[napi]
    pub async fn set_node_state(&self, node_id: String, state: u8) -> Result<()> {
        let rust_state = node_state_from_u8(state)?;
        self.transport
            .set_node_state(&node_id, rust_state)
            .await
            .map_err(|e| Error::from_reason(format!("setNodeState failed: {e}")))?;
        Ok(())
    }

    /// Clear any runtime state override for a node (returns it to `Enabled`).
    #[napi]
    pub async fn clear_node_state(&self, node_id: String) -> Result<()> {
        self.transport
            .clear_node_state(&node_id)
            .await
            .map_err(|e| Error::from_reason(format!("clearNodeState failed: {e}")))?;
        Ok(())
    }

    /// Open an intercept slot on `address`. The caller drives the session via
    /// `recv` (next request) and `complete(cid, kind, replacement)` (reply).
    ///
    /// `address` format: `"node_id.out[.port]"`.
    /// `deadlineMs`: bus default if the caller doesn't reply in time.
    #[napi]
    pub async fn intercept(
        &self,
        address: String,
        deadline_ms: u32,
    ) -> Result<NapiInterceptSession> {
        let addr = parse_address(&address)?;
        let sess: InterceptSession = self
            .transport
            .intercept(&addr, Duration::from_millis(deadline_ms as u64))
            .await
            .map_err(|e| Error::from_reason(format!("intercept failed: {e}")))?;
        let reply = sess.reply.clone();
        Ok(NapiInterceptSession {
            inner: Arc::new(Mutex::new(sess)),
            reply,
        })
    }
}

// ─── NapiSubscription ────────────────────────────────────────────────────────

/// Async handle to a node-output tap. Created by `NapiAttachedSession.subscribe`.
///
/// `recv()` semantics mirror the Python `PySubscription.recv`:
/// - Returns `Some(NapiRuntimeData)` on a delivered frame.
/// - Returns `None` on broadcast lag (the stream is still live; keep calling).
/// - Throws when the upstream channel closes (session ended).
#[napi]
pub struct NapiSubscription {
    rx: Arc<Mutex<broadcast::Receiver<RuntimeData>>>,
}

#[napi]
impl NapiSubscription {
    /// Await the next tap item.
    #[napi]
    pub async fn recv(&self) -> Result<Option<NapiRuntimeData>> {
        use tokio::sync::broadcast::error::RecvError;
        let result = {
            let mut guard = self.rx.lock().await;
            guard.recv().await
        };
        match result {
            Ok(rd) => Ok(Some(NapiRuntimeData { inner: rd })),
            Err(RecvError::Lagged(_)) => Ok(None),
            Err(RecvError::Closed) => Err(Error::from_reason("tap channel closed")),
        }
    }
}

// ─── NapiInterceptSession ────────────────────────────────────────────────────

/// One pending intercept request as it appears on the JS side.
///
/// Access fields via `.correlationId` and `.data` getters from JavaScript.
#[napi]
pub struct InterceptItem {
    correlation_id: u64,
    data: NapiRuntimeData,
}

#[napi]
impl InterceptItem {
    /// The opaque identifier — echo back via `complete(correlationId, ...)`
    /// to resolve the frame. Exposed as `BigInt` because the bus uses `u64`.
    #[napi(getter)]
    pub fn correlation_id(&self) -> BigInt {
        BigInt {
            sign_bit: false,
            words: vec![self.correlation_id],
        }
    }

    /// The data frame the node emitted; inspect or replace it.
    #[napi(getter)]
    pub fn data(&self) -> NapiRuntimeData {
        NapiRuntimeData {
            inner: self.data.get_inner().clone(),
        }
    }
}

/// Live intercept handle. Created by `NapiAttachedSession.intercept`.
///
/// Drive the loop:
/// ```javascript
/// for (;;) {
///   const item = await sess.recv();
///   if (item == null) break;   // bus closed the channel
///   await sess.complete(item.correlationId, 0, null);   // pass through
/// }
/// ```
///
/// `complete(cid, decisionKind, replacement)`:
///   - `0 = pass_through` (replacement ignored)
///   - `1 = replace` (replacement must be a NapiRuntimeData)
///   - `2 = drop` (replacement ignored)
///
/// Dropping this object removes the intercept slot from the bus (via the
/// inner `InterceptSession`'s `Drop` impl).
#[napi]
pub struct NapiInterceptSession {
    inner: Arc<Mutex<InterceptSession>>,
    reply: InterceptReplyHandle,
}

#[napi]
impl NapiInterceptSession {
    /// Await the next intercept request, or `null` if the channel is closed.
    #[napi]
    pub async fn recv(&self) -> Result<Option<InterceptItem>> {
        let result = {
            let mut guard = self.inner.lock().await;
            guard.requests.recv().await
        };
        Ok(result.map(|req| InterceptItem {
            correlation_id: req.correlation_id,
            data: NapiRuntimeData { inner: req.data },
        }))
    }

    /// Resolve one intercept request.
    #[napi]
    pub async fn complete(
        &self,
        correlation_id: BigInt,
        decision_kind: u8,
        replacement: Option<&NapiRuntimeData>,
    ) -> Result<()> {
        let cid: u64 = bigint_to_u64(&correlation_id)?;
        let decision = match decision_kind {
            0 => InterceptDecision::Pass,
            1 => {
                let r = replacement.ok_or_else(|| {
                    Error::from_reason("decisionKind=1 (replace) requires a replacement object")
                })?;
                InterceptDecision::Replace(r.get_inner().clone())
            }
            2 => InterceptDecision::Drop,
            other => {
                return Err(Error::from_reason(format!(
                    "invalid intercept decisionKind {other}; expected 0=pass, 1=replace, 2=drop"
                )));
            }
        };
        self.reply.complete(cid, decision).await;
        Ok(())
    }
}

fn bigint_to_u64(bi: &BigInt) -> Result<u64> {
    if bi.sign_bit {
        return Err(Error::from_reason("correlationId must be non-negative"));
    }
    if bi.words.is_empty() {
        return Ok(0);
    }
    if bi.words.len() > 1 {
        return Err(Error::from_reason("correlationId exceeds u64::MAX"));
    }
    Ok(bi.words[0])
}

// ─── Module-level open helper ────────────────────────────────────────────────

/// Open an in-proc control attach for `session_id`.
///
/// Looks up the session in the process-global `SessionControlBus`. Returns
/// a `SessionNotFoundError`-prefixed error if the session is not registered
/// (e.g. it was created via the legacy `createStreamSession`, or after the
/// session has been closed).
pub(crate) fn open(session_id: &str) -> Result<NapiAttachedSession> {
    let transport = InProcControlTransport::open(session_id).ok_or_else(|| {
        session_not_found(format!(
            "session {session_id:?} not found in process-global SessionControlBus; \
             was the session created via createStreamingSession()?"
        ))
    })?;
    Ok(NapiAttachedSession {
        transport: Arc::new(transport),
    })
}
