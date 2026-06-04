//! Executor-backed streaming-session NAPI binding.
//!
//! This is the Node.js mirror of [`crate::streaming_session::PyStreamingSession`].
//! Each session is constructed via `PipelineExecutor::create_session(manifest)` —
//! which means it lands on the process-global `SessionControlBus` and is reachable
//! through [`crate::napi::control_session::open`].
//!
//! Sibling type [`super::pipeline::NapiStreamSession`] (note: `Stream`, not
//! `Streaming`) stays in place for backward compatibility. That older API runs
//! its own manifest loop and does NOT register on the bus — it is not reachable
//! from `attachInproc`.
//!
//! JS usage:
//! ```javascript
//! const { createStreamingSession } = require('@remotemedia/runtime');
//! const { attachInproc, Data } = require('@remotemedia/runtime/control');
//!
//! const sess = await createStreamingSession(manifestJson);
//! const ctrl = await attachInproc(sess);
//! const tap = await ctrl.subscribe('echo.out');
//! await ctrl.publish('echo.in', Data.fromText('hello'));
//! const frame = await tap.recv();   // Data { kind: 'text', textValue: 'hello' }
//! await sess.close();
//! ```
use std::sync::{Arc, OnceLock};

/// Initialize tracing on first NAPI call. Otherwise tracing! calls from
/// the executor / plugin-sdk subprocess capture path silently no-op,
/// which makes diagnosing plugin spawn failures painful.
///
/// Honor `RUST_LOG` if set, else default to `info`.
fn ensure_tracing_initialized() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .try_init();
    });
}

use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::{mpsc, Mutex};

use remotemedia_core::data::RuntimeData;
use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::{
    PipelineExecutor, SessionHandle, SessionInputSender, TransportData,
};

use super::control_session::NapiAttachedSession;
use super::pipeline::NapiRuntimeData;

type InputSlot = Arc<Mutex<Option<SessionInputSender>>>;
type RxSlot = Arc<Mutex<Option<mpsc::Receiver<RuntimeData>>>>;
type HandleSlot = Arc<Mutex<Option<SessionHandle>>>;

/// A `{kind, data}` pair returned from `recvOutput()`.
///
/// `kind` is `"audio"`, `"video"`, or `"data"` — matching the per-receiver
/// split the underlying `SessionHandle` exposes. Access via `.kind` and
/// `.data` getters from JavaScript.
#[napi]
pub struct PipelineEvent {
    kind: String,
    data: NapiRuntimeData,
}

#[napi]
impl PipelineEvent {
    #[napi(getter)]
    pub fn kind(&self) -> String {
        self.kind.clone()
    }

    #[napi(getter)]
    pub fn data(&self) -> NapiRuntimeData {
        NapiRuntimeData {
            inner: self.data.get_inner().clone(),
        }
    }
}

/// Long-lived streaming session handle for Node.js.
///
/// Symmetric with [`crate::streaming_session::PyStreamingSession`]: holds a
/// `SessionHandle` from `PipelineExecutor::create_session` plus per-kind
/// output receivers. The `_executor` field keeps the executor alive for the
/// session's lifetime.
#[napi]
pub struct NapiStreamingSession {
    session_id: String,
    input_sender: InputSlot,
    audio_rx: RxSlot,
    video_rx: RxSlot,
    data_rx: RxSlot,
    handle: HandleSlot,
    _executor: Arc<PipelineExecutor>,
}

#[napi]
impl NapiStreamingSession {
    /// Session id assigned by the executor.
    #[napi(getter)]
    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    /// Push one input frame into the pipeline.
    ///
    /// Returns a Promise resolving to `null`. Throws if the input channel is
    /// closed (i.e. `signalInputComplete` or `close` was already called).
    #[napi]
    pub async fn send_input(&self, data: &NapiRuntimeData) -> Result<()> {
        let rd = data.get_inner().clone();
        let guard = self.input_sender.lock().await;
        let sender = guard.as_ref().ok_or_else(|| {
            Error::from_reason("Input channel closed — sendInput after signalInputComplete/close")
        })?;
        sender
            .send(TransportData::new(rd))
            .await
            .map_err(|e| Error::from_reason(format!("sendInput failed: {}", e)))?;
        Ok(())
    }

    /// Await the next audio-kind output. Resolves to a `NapiRuntimeData` or
    /// `null` once the channel is closed.
    #[napi]
    pub async fn recv_audio(&self) -> Result<Option<NapiRuntimeData>> {
        recv_kind(&self.audio_rx).await
    }

    /// Await the next video-kind output.
    #[napi]
    pub async fn recv_video(&self) -> Result<Option<NapiRuntimeData>> {
        recv_kind(&self.video_rx).await
    }

    /// Await the next data-kind output (text / json / tensor / etc.).
    #[napi]
    pub async fn recv_data(&self) -> Result<Option<NapiRuntimeData>> {
        recv_kind(&self.data_rx).await
    }

    /// Await the next output across all kinds. Resolves to a
    /// `{kind, data}` object or `null` once every channel has closed.
    ///
    /// Polling priority: audio → video → data.
    #[napi]
    pub async fn recv_output(&self) -> Result<Option<PipelineEvent>> {
        let audio = Arc::clone(&self.audio_rx);
        let video = Arc::clone(&self.video_rx);
        let data = Arc::clone(&self.data_rx);

        loop {
            let mut a_guard = audio.lock().await;
            let mut v_guard = video.lock().await;
            let mut d_guard = data.lock().await;

            let mut a_taken = a_guard.take();
            let mut v_taken = v_guard.take();
            let mut d_taken = d_guard.take();

            let a_active = a_taken.is_some();
            let v_active = v_taken.is_some();
            let d_active = d_taken.is_some();

            if !a_active && !v_active && !d_active {
                return Ok(None);
            }

            enum Outcome {
                Got(&'static str, RuntimeData),
                Closed(&'static str),
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

            match outcome {
                Outcome::Got(kind, rd) => {
                    *a_guard = a_taken;
                    *v_guard = v_taken;
                    *d_guard = d_taken;
                    return Ok(Some(PipelineEvent {
                        kind: kind.to_string(),
                        data: NapiRuntimeData { inner: rd },
                    }));
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
    }

    /// Signal end-of-input. After this, `sendInput` throws. Outputs continue
    /// to drain until each kind channel closes naturally.
    #[napi]
    pub async fn signal_input_complete(&self) -> Result<()> {
        *self.input_sender.lock().await = None;
        Ok(())
    }

    /// Gracefully tear down the session. Idempotent.
    #[napi]
    pub async fn close(&self) -> Result<()> {
        *self.input_sender.lock().await = None;
        *self.audio_rx.lock().await = None;
        *self.video_rx.lock().await = None;
        *self.data_rx.lock().await = None;
        if let Some(mut h) = self.handle.lock().await.take() {
            let _ = h.close().await;
        }
        Ok(())
    }

    /// Open an in-proc control attach against this session.
    ///
    /// Resolves through the process-global `SessionControlBus`. Throws
    /// `SessionNotFoundError` if the session is not registered (e.g. after
    /// `close` returns).
    #[napi]
    pub fn control(&self) -> Result<NapiAttachedSession> {
        super::control_session::open(&self.session_id)
    }
}

async fn recv_kind(rx_arc: &RxSlot) -> Result<Option<NapiRuntimeData>> {
    let mut guard = rx_arc.lock().await;
    let result = match guard.as_mut() {
        Some(rx) => rx.recv().await,
        None => None,
    };
    if result.is_none() {
        *guard = None;
    }
    Ok(result.map(|rd| NapiRuntimeData { inner: rd }))
}

/// Create a long-lived streaming session for the given manifest.
///
/// Each call builds a fresh `Arc<PipelineExecutor>` and calls
/// `executor.create_session(manifest)`. The executor reuses the process-global
/// `SessionControlBus` (per the bus-reuse fix in PR #9), so multiple concurrent
/// sessions all land on the same bus and `attachInproc` works against each.
///
/// # JS Example
/// ```javascript
/// const { createStreamingSession } = require('@remotemedia/runtime');
/// const sess = await createStreamingSession(JSON.stringify(manifest));
/// await sess.sendInput(NapiRuntimeData.text('hello'));
/// const out = await sess.recvData();
/// await sess.close();
/// ```
#[napi]
pub async fn create_streaming_session(manifest_json: String) -> Result<NapiStreamingSession> {
    ensure_tracing_initialized();
    let executor = PipelineExecutor::new()
        .map_err(|e| Error::from_reason(format!("Failed to create executor: {}", e)))?;
    // Register every loaded plugin so the manifest can reference dynamic node
    // types. Mirrors `create_streaming_session` in the Python FFI. The
    // `plugins` module is python-feature-gated today (it sits under
    // `crate::plugins`); for NAPI-only builds, plugin loading is a no-op
    // until a Node-side loader lands. Existing inventory-registered factories
    // are still picked up by `PipelineExecutor::new`.
    #[cfg(feature = "python")]
    for factory in crate::plugins::collect_registered_factories() {
        executor.register_factory(factory).await;
    }
    let executor = Arc::new(executor);
    build_session(executor, manifest_json).await
}

#[doc(hidden)]
pub(crate) async fn build_session(
    executor: Arc<PipelineExecutor>,
    manifest_json: String,
) -> Result<NapiStreamingSession> {
    let manifest: Manifest = serde_json::from_str(&manifest_json)
        .map_err(|e| Error::from_reason(format!("Failed to parse manifest: {}", e)))?;
    let manifest = Arc::new(manifest);

    let mut handle = executor
        .create_session(manifest)
        .await
        .map_err(|e| Error::from_reason(format!("createSession failed: {}", e)))?;
    let session_id = handle.session_id.clone();
    let input_sender = handle
        .input_sender()
        .ok_or_else(|| Error::from_reason("session has no input sender (already closed?)"))?;
    let receivers = handle
        .take_output_receivers()
        .ok_or_else(|| Error::from_reason("session output receivers already taken"))?;

    Ok(NapiStreamingSession {
        session_id,
        input_sender: Arc::new(Mutex::new(Some(input_sender))),
        audio_rx: Arc::new(Mutex::new(Some(receivers.audio_rx))),
        video_rx: Arc::new(Mutex::new(Some(receivers.video_rx))),
        data_rx: Arc::new(Mutex::new(Some(receivers.data_rx))),
        handle: Arc::new(Mutex::new(Some(handle))),
        _executor: executor,
    })
}
