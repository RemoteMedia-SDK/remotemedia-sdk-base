//! Streaming-session Python FFI.
//!
//! Exposes a long-lived `SessionHandle` to Python: push inputs continuously,
//! drain per-kind outputs (audio / video / data) on demand. Maps directly onto
//! [`remotemedia_core::transport::SessionHandle`] — the same API other
//! transports use. Lets Python drive manifests with VAD +
//! Accumulator + LFM2-style streaming nodes that produce many outputs per
//! input.
//!
//! API shape:
//! ```python
//! import remotemedia.runtime as rt
//! sess = await rt.create_streaming_session(manifest_json)
//! await sess.send_input({"type":"audio","samples":[...],"sample_rate":48000,"channels":1})
//! out = await sess.recv_audio()         # next audio frame, or None when closed
//! sess.signal_input_complete()
//! await sess.close()
//! ```

use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::{mpsc, Mutex};

use remotemedia_core::data::RuntimeData;
use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::{
    Participant, ParticipantSessionHandle, PipelineExecutor, SessionHandle, SessionInputSender,
    SharedPipelineOutputReceivers, TransportData,
};

use crate::marshal::{python_to_runtime_data, runtime_data_to_python};

type InputSlot = Arc<Mutex<Option<InputSender>>>;
type RxSlot = Arc<Mutex<Option<mpsc::Receiver<RuntimeData>>>>;
type HandleSlot = Arc<Mutex<Option<SessionHandle>>>;

enum InputSender {
    Raw(SessionInputSender),
    Participant(ParticipantSessionHandle),
}

impl InputSender {
    async fn send(&self, data: TransportData) -> remotemedia_core::Result<()> {
        match self {
            Self::Raw(sender) => sender.send(data).await,
            Self::Participant(sender) => sender.send(data).await,
        }
    }
}

enum Outcome {
    Got(&'static str, RuntimeData),
    Closed(&'static str),
}

/// Long-lived streaming session handle, callable from Python.
///
/// Cheap to clone: every shared cell sits behind its own `Arc<Mutex<...>>`
/// so multiple Python tasks can share the session — one driving input, one
/// draining each output kind.
#[pyclass]
#[derive(Clone)]
pub struct PyStreamingSession {
    session_id: String,
    input_sender: InputSlot,
    audio_rx: RxSlot,
    video_rx: RxSlot,
    data_rx: RxSlot,
    handle: HandleSlot,
    /// Keep the executor alive for the session's lifetime. The scheduler
    /// task spawned at session creation holds `Arc` clones internally, but
    /// holding the executor here is a defensive cheap guarantee.
    _executor: Arc<PipelineExecutor>,
}

#[pymethods]
impl PyStreamingSession {
    /// Session id assigned by the executor.
    #[getter]
    fn session_id(&self) -> String {
        self.session_id.clone()
    }

    /// Push one input frame. `data` accepts the same shapes
    /// `execute_pipeline_with_input` does: numpy arrays, `{"type":"audio", ...}`
    /// dicts, strings, bytes, or anything `python_to_runtime_data` recognises.
    ///
    /// Returns a coroutine resolving to `None`. Raises `RuntimeError` if the
    /// input channel is closed.
    fn send_input<'py>(
        &self,
        py: Python<'py>,
        data: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rd = python_to_runtime_data(py, &data)?;
        let input_sender = Arc::clone(&self.input_sender);
        future_into_py(py, async move {
            let guard = input_sender.lock().await;
            let sender = guard.as_ref().ok_or_else(|| {
                PyRuntimeError::new_err(
                    "Input channel closed — send_input after signal_input_complete/close",
                )
            })?;
            sender
                .send(TransportData::new(rd))
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("send_input failed: {}", e)))?;
            Python::attach(|py| Ok(py.None()))
        })
    }

    /// Await the next audio-kind output. Resolves to a Python dict (same
    /// shape `runtime_data_to_python` produces) or `None` when the channel
    /// closes.
    fn recv_audio<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        recv_kind_async(py, Arc::clone(&self.audio_rx))
    }

    /// Await the next data-kind output (text / json / numpy / tensor / etc.).
    fn recv_data<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        recv_kind_async(py, Arc::clone(&self.data_rx))
    }

    /// Await the next video-kind output.
    fn recv_video<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        recv_kind_async(py, Arc::clone(&self.video_rx))
    }

    /// Await the next output across all kinds. Resolves to `(kind, data)` or
    /// `None` once every kind channel is closed. `kind` is one of
    /// `"audio" | "video" | "data"`.
    fn recv_output<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let audio = Arc::clone(&self.audio_rx);
        let video = Arc::clone(&self.video_rx);
        let data = Arc::clone(&self.data_rx);
        future_into_py(py, async move {
            loop {
                let mut a_guard = audio.lock().await;
                let mut v_guard = video.lock().await;
                let mut d_guard = data.lock().await;

                // Move receivers out for the select; put back after.
                let mut a_taken = a_guard.take();
                let mut v_taken = v_guard.take();
                let mut d_taken = d_guard.take();

                let a_active = a_taken.is_some();
                let v_active = v_taken.is_some();
                let d_active = d_taken.is_some();
                if !a_active && !v_active && !d_active {
                    return Python::attach(|py| Ok(py.None()));
                }

                let outcome: Outcome = {
                    let a_fut = async {
                        match a_taken.as_mut() {
                            Some(rx) => rx.recv().await,
                            None => std::future::pending().await,
                        }
                    };
                    let v_fut = async {
                        match v_taken.as_mut() {
                            Some(rx) => rx.recv().await,
                            None => std::future::pending().await,
                        }
                    };
                    let d_fut = async {
                        match d_taken.as_mut() {
                            Some(rx) => rx.recv().await,
                            None => std::future::pending().await,
                        }
                    };
                    tokio::pin!(a_fut, v_fut, d_fut);
                    tokio::select! {
                        biased;
                        v = &mut a_fut, if a_active => match v {
                            Some(rd) => Outcome::Got("audio", rd),
                            None => Outcome::Closed("audio"),
                        },
                        v = &mut v_fut, if v_active => match v {
                            Some(rd) => Outcome::Got("video", rd),
                            None => Outcome::Closed("video"),
                        },
                        v = &mut d_fut, if d_active => match v {
                            Some(rd) => Outcome::Got("data", rd),
                            None => Outcome::Closed("data"),
                        },
                    }
                };

                // Restore receivers. Drop any that closed.
                match outcome {
                    Outcome::Got(kind, rd) => {
                        *a_guard = a_taken;
                        *v_guard = v_taken;
                        *d_guard = d_taken;
                        return Python::attach(|py| pack_kind(py, kind, &rd));
                    }
                    Outcome::Closed("audio") => {
                        *a_guard = None;
                        *v_guard = v_taken;
                        *d_guard = d_taken;
                    }
                    Outcome::Closed("video") => {
                        *a_guard = a_taken;
                        *v_guard = None;
                        *d_guard = d_taken;
                    }
                    Outcome::Closed("data") => {
                        *a_guard = a_taken;
                        *v_guard = v_taken;
                        *d_guard = None;
                    }
                    Outcome::Closed(_) => unreachable!(),
                }
            }
        })
    }

    /// Non-blocking peek on audio. Returns the next frame, or `None` if
    /// the channel is empty / closed / locked.
    fn try_recv_audio(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        try_recv_kind_sync(py, &self.audio_rx)
    }

    fn try_recv_data(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        try_recv_kind_sync(py, &self.data_rx)
    }

    fn try_recv_video(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        try_recv_kind_sync(py, &self.video_rx)
    }

    /// Signal end-of-input. After this, `send_input` raises. Outputs continue
    /// to drain until each kind channel closes naturally.
    fn signal_input_complete<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let input_sender = Arc::clone(&self.input_sender);
        future_into_py(py, async move {
            *input_sender.lock().await = None;
            Python::attach(|py| Ok(py.None()))
        })
    }

    /// Gracefully tear down the session. Idempotent.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let handle = Arc::clone(&self.handle);
        let input_sender = Arc::clone(&self.input_sender);
        let audio = Arc::clone(&self.audio_rx);
        let video = Arc::clone(&self.video_rx);
        let data = Arc::clone(&self.data_rx);
        future_into_py(py, async move {
            *input_sender.lock().await = None;
            *audio.lock().await = None;
            *video.lock().await = None;
            *data.lock().await = None;
            if let Some(mut h) = handle.lock().await.take() {
                let _ = h.close().await;
            }
            Python::attach(|py| Ok(py.None()))
        })
    }

    /// Open an in-proc control attach against this session.
    ///
    /// Returns a `PyAttachedSession` resolved through the process-global
    /// `SessionControlBus`. Raises `SessionNotFoundError` if the session is
    /// not yet registered (e.g. called before `create_streaming_session`
    /// completes, or after `close`).
    fn control(&self, py: Python<'_>) -> PyResult<Py<crate::control_session::PyAttachedSession>> {
        let session = crate::control_session::open(&self.session_id)?;
        Py::new(py, session)
    }

    fn __repr__(&self) -> String {
        format!("PyStreamingSession(session_id='{}')", self.session_id)
    }
}

fn recv_kind_async(py: Python<'_>, rx_arc: RxSlot) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let mut guard = rx_arc.lock().await;
        let result = match guard.as_mut() {
            Some(rx) => rx.recv().await,
            None => None,
        };
        if result.is_none() {
            *guard = None;
        }
        drop(guard);
        Python::attach(|py| match result {
            Some(rd) => Ok(runtime_data_to_python(py, &rd)?),
            None => Ok(py.None()),
        })
    })
}

fn try_recv_kind_sync(py: Python<'_>, rx: &RxSlot) -> PyResult<Py<PyAny>> {
    let mut guard = match rx.try_lock() {
        Ok(g) => g,
        Err(_) => return Ok(py.None()),
    };
    use tokio::sync::mpsc::error::TryRecvError;
    let outcome = match guard.as_mut() {
        Some(rx) => rx.try_recv(),
        None => return Ok(py.None()),
    };
    match outcome {
        Ok(rd) => Ok(runtime_data_to_python(py, &rd)?),
        Err(TryRecvError::Empty) => Ok(py.None()),
        Err(TryRecvError::Disconnected) => {
            *guard = None;
            Ok(py.None())
        }
    }
}

fn pack_kind(py: Python<'_>, kind: &str, rd: &RuntimeData) -> PyResult<Py<PyAny>> {
    let data_py = runtime_data_to_python(py, rd)?;
    let kind_py = kind.into_py_any(py)?;
    let tuple = pyo3::types::PyTuple::new(py, &[kind_py, data_py])?;
    Ok(tuple.into_any().unbind())
}

/// Create a long-lived streaming session for the given manifest.
///
/// Returns a Python coroutine resolving to a [`PyStreamingSession`]. Each call
/// builds a fresh `PipelineExecutor`; this is intentional — sessions are
/// independent and prewarm is opt-in (not relevant for the typical Python
/// streaming use case yet).
///
/// # Example
/// ```python
/// import asyncio, json
/// import remotemedia.runtime as rt
///
/// async def main():
///     manifest = json.dumps({...})
///     sess = await rt.create_streaming_session(manifest)
///     await sess.send_input({"type": "audio", "samples": [...], "sample_rate": 48000, "channels": 1})
///     out = await sess.recv_audio()
///     await sess.close()
///
/// asyncio.run(main())
/// ```
#[pyfunction]
pub fn create_streaming_session(
    py: Python<'_>,
    manifest_json: String,
) -> PyResult<Bound<'_, PyAny>> {
    // Capture asyncio task-locals for inline-async dispatch — mirrors the
    // setup execute_pipeline does so inline Python nodes inside the manifest
    // can resolve their event loop.
    super::inline_python_node::set_task_locals(pyo3_async_runtimes::tokio::get_current_locals(py)?);

    future_into_py(py, async move {
        let executor = PipelineExecutor::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create executor: {}", e)))?;
        // Register every plugin loaded so far via `rt.load_plugin(path)`
        // BEFORE the manifest is parsed — otherwise unknown-node-type
        // validation would fire even though the user already loaded the
        // .so. Clones are cheap (Arc) and the underlying bundles stay
        // pinned in `plugins::LOADED_BUNDLES`.
        for factory in crate::plugins::collect_registered_factories() {
            executor.register_factory(factory).await;
        }
        let executor = Arc::new(executor);
        let sess = build_session(executor, manifest_json).await?;
        Python::attach(|py| Ok(sess.into_py_any(py)?))
    })
}

/// Create or join a shared streaming session for a participant.
///
/// `shared_session_key` is the logical room/conversation key. Calls with the
/// same key and executor share one core pipeline session; this FFI constructor
/// creates an executor scoped to the returned Python object, so share the
/// returned session object in-process when multiple Python tasks should attach
/// to the same runtime instance.
#[pyfunction]
#[pyo3(signature = (
    manifest_json,
    shared_session_key,
    participant_id,
    participant_role,
    track_id=None,
    modality=None,
    display_name=None
))]
pub fn create_shared_streaming_session(
    py: Python<'_>,
    manifest_json: String,
    shared_session_key: String,
    participant_id: String,
    participant_role: String,
    track_id: Option<String>,
    modality: Option<String>,
    display_name: Option<String>,
) -> PyResult<Bound<'_, PyAny>> {
    super::inline_python_node::set_task_locals(pyo3_async_runtimes::tokio::get_current_locals(py)?);

    future_into_py(py, async move {
        let executor = PipelineExecutor::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create executor: {}", e)))?;
        for factory in crate::plugins::collect_registered_factories() {
            executor.register_factory(factory).await;
        }
        let executor = Arc::new(executor);
        let sess = build_shared_session(
            executor,
            manifest_json,
            shared_session_key,
            participant_id,
            participant_role,
            track_id,
            modality,
            display_name,
        )
        .await?;
        Python::attach(|py| Ok(sess.into_py_any(py)?))
    })
}

#[doc(hidden)]
pub(crate) async fn build_session(
    executor: Arc<PipelineExecutor>,
    manifest_json: String,
) -> PyResult<PyStreamingSession> {
    let manifest: Manifest = serde_json::from_str(&manifest_json)
        .map_err(|e| PyValueError::new_err(format!("Failed to parse manifest: {}", e)))?;
    let manifest = Arc::new(manifest);

    let mut handle = executor
        .create_session(manifest)
        .await
        .map_err(|e| PyRuntimeError::new_err(format!("create_session failed: {}", e)))?;
    let session_id = handle.session_id.clone();
    let input_sender = handle
        .input_sender()
        .ok_or_else(|| PyRuntimeError::new_err("session has no input sender (already closed?)"))?;
    let receivers = handle
        .take_output_receivers()
        .ok_or_else(|| PyRuntimeError::new_err("session output receivers already taken"))?;

    Ok(PyStreamingSession {
        session_id,
        input_sender: Arc::new(Mutex::new(Some(InputSender::Raw(input_sender)))),
        audio_rx: Arc::new(Mutex::new(Some(receivers.audio_rx))),
        video_rx: Arc::new(Mutex::new(Some(receivers.video_rx))),
        data_rx: Arc::new(Mutex::new(Some(receivers.data_rx))),
        handle: Arc::new(Mutex::new(Some(handle))),
        _executor: executor,
    })
}

#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_shared_session(
    executor: Arc<PipelineExecutor>,
    manifest_json: String,
    shared_session_key: String,
    participant_id: String,
    participant_role: String,
    track_id: Option<String>,
    modality: Option<String>,
    display_name: Option<String>,
) -> PyResult<PyStreamingSession> {
    let manifest: Manifest = serde_json::from_str(&manifest_json)
        .map_err(|e| PyValueError::new_err(format!("Failed to parse manifest: {}", e)))?;
    let manifest = Arc::new(manifest);

    let shared = executor
        .get_or_create_shared_session(shared_session_key, manifest)
        .await
        .map_err(|e| PyRuntimeError::new_err(format!("create shared session failed: {}", e)))?;
    let mut participant = Participant::new(participant_id, participant_role);
    if let Some(track_id) = track_id {
        participant = participant.with_track_id(track_id);
    }
    if let Some(modality) = modality {
        participant = participant.with_modality(modality);
    }
    if let Some(display_name) = display_name {
        participant = participant.with_display_name(display_name);
    }
    let participant = shared.participant_handle(participant);
    let receivers = bridge_shared_outputs(shared.subscribe_outputs());

    Ok(PyStreamingSession {
        session_id: shared.session_id().to_string(),
        input_sender: Arc::new(Mutex::new(Some(InputSender::Participant(participant)))),
        audio_rx: Arc::new(Mutex::new(Some(receivers.0))),
        video_rx: Arc::new(Mutex::new(Some(receivers.1))),
        data_rx: Arc::new(Mutex::new(Some(receivers.2))),
        handle: Arc::new(Mutex::new(None)),
        _executor: executor,
    })
}

fn bridge_shared_outputs(
    mut outputs: SharedPipelineOutputReceivers,
) -> (
    mpsc::Receiver<RuntimeData>,
    mpsc::Receiver<RuntimeData>,
    mpsc::Receiver<RuntimeData>,
) {
    let (audio_tx, audio_rx) = mpsc::channel(128);
    let (video_tx, video_rx) = mpsc::channel(128);
    let (data_tx, data_rx) = mpsc::channel(128);

    tokio::spawn(async move {
        loop {
            let Ok(Some(output)) = outputs.recv_output().await else {
                break;
            };
            let runtime = output.data;
            let result = match &runtime {
                RuntimeData::Audio { .. } => audio_tx.send(runtime).await,
                RuntimeData::Video { .. } => video_tx.send(runtime).await,
                _ => data_tx.send(runtime).await,
            };
            if matches!(result, Err(mpsc::error::SendError(_))) {
                break;
            }
        }
    });

    (audio_rx, video_rx, data_rx)
}
