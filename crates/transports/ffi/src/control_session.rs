//! PyO3 surface for the in-proc control bus.
//!
//! Exposes [`PyAttachedSession`] and [`PySubscription`] to Python, allowing
//! a `PyStreamingSession` to open a direct control-bus attach without gRPC.
//!
//! Python usage:
//! ```python
//! async with attach_inproc(session) as ctrl:
//!     tap = await ctrl.subscribe("echo.out")
//!     await ctrl.publish("echo.in", Data.from_text("hello"))
//!     item = await tap.recv()
//! ```

use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::{broadcast, Mutex};

use remotemedia_core::transport::control_transport::{
    ControlTransport, InProcControlTransport, InterceptReplyHandle, InterceptSession,
};
use remotemedia_core::transport::session_control::{
    ControlAddress, Direction, InterceptDecision, NodeState,
};

use crate::marshal::runtime_data_to_python;

// ─── Custom exceptions ───────────────────────────────────────────────────────

pyo3::create_exception!(
    remotemedia.runtime,
    SessionNotFoundError,
    pyo3::exceptions::PyRuntimeError
);
pyo3::create_exception!(
    remotemedia.runtime,
    ControlAddressError,
    pyo3::exceptions::PyValueError
);

// ─── Address parsing ─────────────────────────────────────────────────────────

/// Parse `"node.in[.port]"` / `"node.out[.port]"` into a [`ControlAddress`].
///
/// Returns `Err(ControlAddressError)` on malformed input.
fn parse_address(spec: &str) -> PyResult<ControlAddress> {
    let (node_part, rest) = spec.split_once('.').ok_or_else(|| {
        ControlAddressError::new_err(format!(
            "invalid control address {spec:?}; expected 'node.in[.port]' or 'node.out[.port]'"
        ))
    })?;

    if node_part.is_empty() {
        return Err(ControlAddressError::new_err(format!(
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
            return Err(ControlAddressError::new_err(format!(
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

/// Map a u8 from Python NodeState int to the Rust enum.
///
/// Python `NodeState` enum values mirror the protobuf constants:
///   0 = UNSPECIFIED → Enabled (default)
///   1 = ENABLED     → Enabled
///   2 = BYPASS      → Bypass
///   3 = DISABLED    → Disabled
fn node_state_from_u8(v: u8) -> PyResult<NodeState> {
    match v {
        0 | 1 => Ok(NodeState::Enabled),
        2 => Ok(NodeState::Bypass),
        3 => Ok(NodeState::Disabled),
        other => Err(PyValueError::new_err(format!(
            "unknown NodeState value {other}; expected 0-3"
        ))),
    }
}

// ─── PyAttachedSession ───────────────────────────────────────────────────────

/// In-proc control-bus handle. Wraps [`InProcControlTransport`] and exposes
/// coroutine-returning methods that the Python `_InProcTransport` adapter
/// calls directly (no gRPC round-trip).
///
/// Obtain via `PyStreamingSession.control()`.
#[pyclass]
pub struct PyAttachedSession {
    transport: Arc<InProcControlTransport>,
}

#[pymethods]
impl PyAttachedSession {
    /// Session id this handle is bound to.
    #[getter]
    fn session_id(&self) -> String {
        self.transport.session_id().to_string()
    }

    /// Subscribe to outputs from a node port. Returns a coroutine that
    /// resolves to a [`PySubscription`]. The caller must await the coroutine.
    ///
    /// `address` format: `"node_id.out[.port]"`.
    fn subscribe<'py>(&self, py: Python<'py>, address: String) -> PyResult<Bound<'py, PyAny>> {
        let addr = parse_address(&address)?;
        if addr.direction != Direction::Out {
            return Err(ControlAddressError::new_err(
                "subscribe requires an '.out' address",
            ));
        }
        let transport = Arc::clone(&self.transport);
        future_into_py(py, async move {
            let rx = transport
                .subscribe(&addr)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("subscribe failed: {e}")))?;
            Python::attach(|py| {
                Py::new(
                    py,
                    PySubscription {
                        rx: Arc::new(Mutex::new(rx)),
                    },
                )
                .map(|obj| obj.into_any())
            })
        })
    }

    /// Subscribe to a `.in` address — bypasses the direction check of
    /// :meth:`subscribe`.
    ///
    /// Used for control-plane communication where an external process must
    /// read from a ``.in`` address that the publisher writes to.
    ///
    /// `address` format: `"node_id.in[.port]"`.
    fn subscribe_in<'py>(&self, py: Python<'py>, address: String) -> PyResult<Bound<'py, PyAny>> {
        let addr = parse_address(&address)?;
        if addr.direction != Direction::In {
            return Err(ControlAddressError::new_err(
                "subscribe_in requires an '.in' address (use subscribe for '.out')",
            ));
        }
        let transport = Arc::clone(&self.transport);
        future_into_py(py, async move {
            let rx = transport
                .subscribe_in(&addr)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("subscribe failed: {e}")))?;
            Python::attach(|py| {
                Py::new(
                    py,
                    PySubscription {
                        rx: Arc::new(Mutex::new(rx)),
                    },
                )
                .map(|obj| obj.into_any())
            })
        })
    }

    /// Inject `data` at a node's input. `data` is a Python object in the
    /// same shapes that `python_to_runtime_data` accepts (dict, str, bytes).
    ///
    /// `address` format: `"node_id.in[.port]"`.
    ///
    /// Design choice: accept a Python `PyAny` and marshal it here via
    /// `python_to_runtime_data`. The Python side passes a `Data` object's
    /// representation. We call `data.as_buffer()` on the Python side and
    /// pass the serialized bytes, OR we accept the raw `PyAny` and marshal
    /// it. We chose the latter to keep the Python adapter thin.
    fn publish<'py>(
        &self,
        py: Python<'py>,
        address: String,
        data: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let addr = parse_address(&address)?;
        if addr.direction != Direction::In {
            return Err(ControlAddressError::new_err(
                "publish requires an '.in' address",
            ));
        }
        let rd = crate::marshal::python_to_runtime_data(py, &data)?;
        let transport = Arc::clone(&self.transport);
        future_into_py(py, async move {
            transport
                .publish(&addr, rd)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("publish failed: {e}")))?;
            Python::attach(|py| Ok(py.None()))
        })
    }

    /// Publish data to a ``.out`` address — bypasses the direction check of
    /// :meth:`publish`.
    ///
    /// Used for control-plane communication where an external process must
    /// write to a ``.out`` address that a subscriber reads from.
    ///
    /// `address` format: `"node_id.out[.port]"`.
    fn publish_out<'py>(
        &self,
        py: Python<'py>,
        address: String,
        data: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let addr = parse_address(&address)?;
        if addr.direction != Direction::Out {
            return Err(ControlAddressError::new_err(
                "publish_out requires an '.out' address (use publish for '.in')",
            ));
        }
        let rd = crate::marshal::python_to_runtime_data(py, &data)?;
        let transport = Arc::clone(&self.transport);
        future_into_py(py, async move {
            transport
                .publish_out(&addr, rd)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("publish failed: {e}")))?;
            Python::attach(|py| Ok(py.None()))
        })
    }

    /// Set the execution state of a node. `state` is the integer value of the
    /// Python `NodeState` enum (0/1=Enabled, 2=Bypass, 3=Disabled).
    fn set_node_state<'py>(
        &self,
        py: Python<'py>,
        node_id: String,
        state: u8,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_state = node_state_from_u8(state)?;
        let transport = Arc::clone(&self.transport);
        future_into_py(py, async move {
            transport
                .set_node_state(&node_id, rust_state)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("set_node_state failed: {e}")))?;
            Python::attach(|py| Ok(py.None()))
        })
    }

    /// Clear any runtime state override for a node (returns it to `Enabled`).
    fn clear_node_state<'py>(
        &self,
        py: Python<'py>,
        node_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let transport = Arc::clone(&self.transport);
        future_into_py(py, async move {
            transport
                .clear_node_state(&node_id)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("clear_node_state failed: {e}")))?;
            Python::attach(|py| Ok(py.None()))
        })
    }

    /// Open an intercept slot on `addr`. Returns a coroutine that resolves to
    /// a [`PyInterceptSession`]. The caller drives the session by calling
    /// `await sess.recv()` to obtain `(correlation_id, data)` tuples and
    /// `await sess.complete(cid, kind, replacement)` to reply.
    ///
    /// `address` format: `"node_id.out[.port]"`.
    /// `deadline_ms`: how long (in ms) the bus waits for a reply before
    /// passing the frame through unconditionally.
    fn intercept<'py>(
        &self,
        py: Python<'py>,
        address: String,
        deadline_ms: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let addr = parse_address(&address)?;
        let transport = Arc::clone(&self.transport);
        future_into_py(py, async move {
            let sess: InterceptSession = transport
                .intercept(&addr, std::time::Duration::from_millis(deadline_ms))
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("intercept failed: {e}")))?;
            // Clone the reply handle before moving sess into the Arc<Mutex<>>.
            let reply = sess.reply.clone();
            Python::attach(|py| {
                Py::new(
                    py,
                    PyInterceptSession {
                        inner: Arc::new(Mutex::new(sess)),
                        reply,
                    },
                )
                .map(|obj| obj.into_any())
            })
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "PyAttachedSession(session_id='{}')",
            self.transport.session_id()
        )
    }
}

// ─── PyInterceptSession ──────────────────────────────────────────────────────

/// Live intercept handle. Created by `PyAttachedSession.intercept`.
///
/// Drive the intercept loop with:
/// ```python
/// while True:
///     next_item = await sess.recv()
///     if next_item is None:
///         break          # bus closed the channel
///     correlation_id, data = next_item
///     await sess.complete(correlation_id, 0, None)  # pass through
/// ```
///
/// `complete(cid, kind, replacement)`:
///   kind 0 = pass_through (replacement ignored)
///   kind 1 = replace (replacement must be a Python data object)
///   kind 2 = drop (replacement ignored)
///
/// Dropping this object removes the intercept slot from the bus (via the
/// inner `InterceptSession`'s `Drop` impl which calls `remove_intercept`).
#[pyclass]
pub struct PyInterceptSession {
    /// Full session wrapped in a Mutex so we can mutably borrow `requests`
    /// across await points. The `Drop` impl on `InterceptSession` fires when
    /// this `Arc` is dropped, which removes the bus slot.
    inner: Arc<Mutex<InterceptSession>>,
    /// Cloned eagerly so `complete` doesn't need to lock `inner`.
    reply: InterceptReplyHandle,
}

#[pymethods]
impl PyInterceptSession {
    /// Await the next intercept request.
    ///
    /// Returns `(correlation_id: int, data: Any)` on success.
    /// Returns `None` when the bus closes the upstream channel (session ended).
    fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let result = {
                let mut guard = inner.lock().await;
                guard.requests.recv().await
            };
            match result {
                Some(req) => Python::attach(|py| {
                    let py_data = crate::marshal::runtime_data_to_python(py, &req.data)?;
                    let cid_py = req.correlation_id.into_pyobject(py)?;
                    let tuple =
                        pyo3::types::PyTuple::new(py, [cid_py.into_any(), py_data.into_bound(py)])?;
                    Ok(tuple.into_any().unbind())
                }),
                None => Python::attach(|py| Ok(py.None())),
            }
        })
    }

    /// Resolve one intercept request.
    ///
    /// `decision_kind`:
    ///   0 = pass_through (replacement ignored)
    ///   1 = replace (replacement must be a Python data object)
    ///   2 = drop (replacement ignored)
    fn complete<'py>(
        &self,
        py: Python<'py>,
        correlation_id: u64,
        decision_kind: u8,
        replacement: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let decision = match decision_kind {
            0 => InterceptDecision::Pass,
            1 => {
                let r = replacement.ok_or_else(|| {
                    PyValueError::new_err("decision_kind=1 (replace) requires a replacement object")
                })?;
                let rd = crate::marshal::python_to_runtime_data(py, &r)?;
                InterceptDecision::Replace(rd)
            }
            2 => InterceptDecision::Drop,
            other => {
                return Err(PyValueError::new_err(format!(
                    "invalid intercept decision_kind {other}; expected 0=pass, 1=replace, 2=drop"
                )));
            }
        };
        let reply = self.reply.clone();
        future_into_py(py, async move {
            reply.complete(correlation_id, decision).await;
            Python::attach(|py| Ok(py.None()))
        })
    }

    fn __repr__(&self) -> String {
        "PyInterceptSession(active)".to_string()
    }
}

// ─── PySubscription ──────────────────────────────────────────────────────────

/// Async handle to a node-output tap. Created by `PyAttachedSession.subscribe`.
///
/// Call `await sub.recv()` in a loop to receive items:
/// - Returns the Python representation of a `RuntimeData` frame on success.
/// - Returns `None` on broadcast lag (the stream is still live — keep calling).
/// - Raises `StopAsyncIteration` when the tap channel is closed (session ended).
#[pyclass]
pub struct PySubscription {
    rx: Arc<Mutex<broadcast::Receiver<remotemedia_core::data::RuntimeData>>>,
}

#[pymethods]
impl PySubscription {
    /// Await the next tap item.
    ///
    /// Return semantics:
    /// - Ok(data) — a marshalled `RuntimeData` dict/primitive
    /// - Ok(None) — lagged (broadcast buffer was full); keep calling
    /// - Err(StopAsyncIteration) — channel closed, session is gone
    fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rx = Arc::clone(&self.rx);
        future_into_py(py, async move {
            use tokio::sync::broadcast::error::RecvError;
            let result = {
                let mut guard = rx.lock().await;
                guard.recv().await
            };
            match result {
                Ok(rd) => Python::attach(|py| {
                    // Return the marshalled Python object. The Python adapter
                    // wraps non-dict primitives in a canonical dict envelope
                    // before passing to Data._from_runtime_dict.
                    Ok(runtime_data_to_python(py, &rd)?)
                }),
                Err(RecvError::Lagged(_)) => {
                    // The broadcast buffer was full; signal that a frame was
                    // dropped so the caller can handle lag (retry is fine).
                    Python::attach(|py| Ok(py.None()))
                }
                Err(RecvError::Closed) => Err(PyStopAsyncIteration::new_err("tap channel closed")),
            }
        })
    }

    fn __repr__(&self) -> String {
        "PySubscription(active)".to_string()
    }
}

// ─── Module-level open function ──────────────────────────────────────────────

/// Open an in-proc control attach for `session_id`.
///
/// Looks up the session in the process-global [`SessionControlBus`].
/// Returns `SessionNotFoundError` if the session is not registered.
///
/// Called from `PyStreamingSession.control()`.
pub(crate) fn open(session_id: &str) -> PyResult<PyAttachedSession> {
    let transport = InProcControlTransport::open(session_id).ok_or_else(|| {
        SessionNotFoundError::new_err(format!(
            "session {session_id:?} not found in process-global SessionControlBus; \
             was the session created via create_streaming_session()?"
        ))
    })?;
    Ok(PyAttachedSession {
        transport: Arc::new(transport),
    })
}
