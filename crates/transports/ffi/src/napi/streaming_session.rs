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
    Participant, ParticipantSessionHandle, PipelineExecutor, SessionHandle, SessionInputSender,
    TransportData,
};

use super::buffer_config::PipelineBufferConfig;
use super::control_session::NapiAttachedSession;
use super::output_loans::NapiOutputAllocator;
use super::pipeline::NapiRuntimeData;

type InputSlot = Arc<Mutex<Option<InputSender>>>;
type MembershipSlot = Arc<Mutex<Option<crate::shared_sessions::SharedMembership>>>;

/// Where a session's input goes: straight in, or in as a participant of a
/// shared session.
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

/// Who a shared session's handle speaks for (`createSharedStreamingSession`).
#[napi(object)]
pub struct NapiParticipant {
    pub id: String,
    /// Defaults to `"user"`.
    pub role: Option<String>,
    pub track_id: Option<String>,
    pub modality: Option<String>,
    pub display_name: Option<String>,
}
/// Session outputs; metadata carries the producing participant.
type RxSlot = Arc<Mutex<Option<mpsc::Receiver<TransportData>>>>;
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
    participant: Option<std::collections::HashMap<String, String>>,
}

#[napi]
impl PipelineEvent {
    #[napi(getter)]
    pub fn kind(&self) -> String {
        self.kind.clone()
    }

    #[napi(getter)]
    pub fn data(&self) -> NapiRuntimeData {
        self.data.clone()
    }

    /// The participant whose input produced this output (`{id, role,
    /// trackId?, modality?, displayName?}`), or `null` when the output is not
    /// attributed (openspec: add-multi-participant-sessions).
    #[napi(getter, ts_return_type = "Record<string, string> | null")]
    pub fn participant(&self) -> Option<std::collections::HashMap<String, String>> {
        self.participant.clone()
    }
}

/// `participant.*` output metadata as the JS `participant` object.
fn participant_object(
    metadata: &std::collections::HashMap<String, String>,
) -> Option<std::collections::HashMap<String, String>> {
    let fields = crate::shared_sessions::participant_fields(metadata)?;
    Some(
        fields
            .into_iter()
            .map(|(field, value)| {
                let key = match field {
                    "track_id" => "trackId",
                    "display_name" => "displayName",
                    other => other,
                };
                (key.to_string(), value)
            })
            .collect(),
    )
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
    copies: remotemedia_types::CopyContext,
    input_sender: InputSlot,
    audio_rx: RxSlot,
    video_rx: RxSlot,
    data_rx: RxSlot,
    handle: HandleSlot,
    /// Membership of a shared session; `None` for a private session.
    membership: MembershipSlot,
    _executor: Arc<PipelineExecutor>,
    output_allocator: Arc<dyn remotemedia_types::OutputAllocator>,
}

#[napi]
impl NapiStreamingSession {
    /// Session id assigned by the executor.
    #[napi(getter)]
    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    /// Logical copies observed by the core session, including snapshots of its
    /// bound owners. Standalone imports/getters use nodejsBufferCopyReport.
    #[napi]
    pub fn copy_report(&self) -> Result<String> {
        serde_json::to_string(&self.copies.report()).map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Produce directly in the session ingress provider without escaped JS aliases.
    #[napi]
    pub fn output_allocator(&self) -> NapiOutputAllocator {
        NapiOutputAllocator {
            inner: self.output_allocator.clone(),
            copies: self.copies.clone(),
        }
    }

    /// Push one input frame into the pipeline.
    ///
    /// Returns a Promise resolving to `null`. Throws if the input channel is
    /// closed (i.e. `signalInputComplete` or `close` was already called).
    #[napi(ts_return_type = "Promise<void>")]
    pub fn send_input(&self, env: Env, data: &NapiRuntimeData) -> Result<napi::JsObject> {
        // Take the owner on the JS thread: a borrowed handle must not be read
        // inside the future, which runs after this call returns.
        let rd = data.get_inner().clone();
        let slot = self.input_sender.clone();
        env.spawn_future(async move {
            let guard = slot.lock().await;
            let sender = guard.as_ref().ok_or_else(|| {
                Error::from_reason(
                    "Input channel closed — sendInput after signalInputComplete/close",
                )
            })?;
            sender
                .send(TransportData::new(rd))
                .await
                .map_err(|e| Error::from_reason(format!("sendInput failed: {}", e)))?;
            Ok(())
        })
    }

    /// Await the next audio-kind output. Resolves to a `NapiRuntimeData` or
    /// `null` once the channel is closed.
    #[napi]
    pub async fn recv_audio(&self) -> Result<Option<NapiRuntimeData>> {
        recv_kind(&self.audio_rx, &self.copies).await
    }

    /// Await the next video-kind output.
    #[napi]
    pub async fn recv_video(&self) -> Result<Option<NapiRuntimeData>> {
        recv_kind(&self.video_rx, &self.copies).await
    }

    /// Await the next data-kind output (text / json / tensor / etc.).
    #[napi]
    pub async fn recv_data(&self) -> Result<Option<NapiRuntimeData>> {
        recv_kind(&self.data_rx, &self.copies).await
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
                Got(&'static str, TransportData),
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
                Outcome::Got(kind, out) => {
                    *a_guard = a_taken;
                    *v_guard = v_taken;
                    *d_guard = d_taken;
                    return Ok(Some(PipelineEvent {
                        kind: kind.to_string(),
                        participant: participant_object(&out.metadata),
                        data: NapiRuntimeData::from_runtime_with_context(out.data, &self.copies),
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
        // A participant leaves its shared session: its per-participant node
        // instances drain and stop.
        if let Some(InputSender::Participant(participant)) = self.input_sender.lock().await.take() {
            let _ = participant.leave().await;
        }
        *self.audio_rx.lock().await = None;
        *self.video_rx.lock().await = None;
        *self.data_rx.lock().await = None;
        if let Some(mut h) = self.handle.lock().await.take() {
            let _ = h.close().await;
        }
        if let Some(membership) = self.membership.lock().await.take() {
            membership.leave().await;
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

async fn recv_kind(
    rx_arc: &RxSlot,
    copies: &remotemedia_types::CopyContext,
) -> Result<Option<NapiRuntimeData>> {
    let mut guard = rx_arc.lock().await;
    let result = match guard.as_mut() {
        Some(rx) => rx.recv().await.map(|out| out.data),
        None => None,
    };
    if result.is_none() {
        *guard = None;
    }
    Ok(result.map(|rd| NapiRuntimeData::from_runtime_with_context(rd, copies)))
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
pub async fn create_streaming_session(
    manifest_json: String,
    pipeline_buffers: Option<PipelineBufferConfig>,
) -> Result<NapiStreamingSession> {
    ensure_tracing_initialized();
    let executor = plugin_executor(pipeline_buffers.as_ref(), None).await?;
    build_session(executor, manifest_json).await
}

/// An executor for `pipeline_buffers` (default policy when absent) with the
/// loaded plugin factories registered. `session_prefix` names its sessions,
/// which namespaces their IPC channels.
pub(crate) async fn plugin_executor(
    pipeline_buffers: Option<&PipelineBufferConfig>,
    session_prefix: Option<String>,
) -> Result<Arc<PipelineExecutor>> {
    let mut config = pipeline_buffers
        .map(PipelineBufferConfig::to_executor_config)
        .transpose()?
        .unwrap_or_default();
    if let Some(prefix) = session_prefix {
        config.session_id_prefix = prefix;
    }
    let executor = PipelineExecutor::with_config(config)
        .map_err(|e| Error::from_reason(format!("Failed to create executor: {}", e)))?;
    register_loaded_plugins(&executor).await;
    Ok(Arc::new(executor))
}

/// Register every plugin loaded so far, so manifests can use its node types.
async fn register_loaded_plugins(executor: &PipelineExecutor) {
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
        output_allocator: handle.output_allocator(),
        copies: handle.transport_copy_context(),
        session_id,
        input_sender: Arc::new(Mutex::new(Some(InputSender::Raw(input_sender)))),
        audio_rx: Arc::new(Mutex::new(Some(receivers.audio_rx))),
        video_rx: Arc::new(Mutex::new(Some(receivers.video_rx))),
        data_rx: Arc::new(Mutex::new(Some(receivers.data_rx))),
        handle: Arc::new(Mutex::new(Some(handle))),
        membership: Arc::new(Mutex::new(None)),
        _executor: executor,
    })
}

/// Create or join the shared streaming session `sharedSessionKey` as
/// `participant` (openspec: add-multi-participant-sessions).
///
/// Calls in one process with the same key (and the same `pipelineBuffers`)
/// join one pipeline session; each handle's input enters as its participant
/// and `recvOutput()` events carry the producing `participant`. `close()`
/// leaves the session; the last handle to close shuts it down.
///
/// # JS Example
/// ```javascript
/// const alice = await createSharedStreamingSession(manifest, 'room1', { id: 'alice' });
/// const bob = await createSharedStreamingSession(manifest, 'room1', { id: 'bob' });
/// await alice.sendInput(NapiRuntimeData.text('hi'));
/// const ev = await bob.recvOutput();   // ev.participant.id === 'alice'
/// ```
#[napi]
pub async fn create_shared_streaming_session(
    manifest_json: String,
    shared_session_key: String,
    participant: NapiParticipant,
    pipeline_buffers: Option<PipelineBufferConfig>,
) -> Result<NapiStreamingSession> {
    ensure_tracing_initialized();
    if participant.id.is_empty() {
        return Err(Error::from_reason("participant.id is required"));
    }
    let config = pipeline_buffers
        .as_ref()
        .map(PipelineBufferConfig::to_executor_config)
        .transpose()?
        .unwrap_or_default();
    let executor = crate::shared_sessions::shared_executor(config)
        .map_err(|e| Error::from_reason(format!("Failed to create executor: {}", e)))?;
    register_loaded_plugins(&executor).await;
    let manifest: Manifest = serde_json::from_str(&manifest_json)
        .map_err(|e| Error::from_reason(format!("Failed to parse manifest: {}", e)))?;
    let shared = executor
        .get_or_create_shared_session(shared_session_key, Arc::new(manifest))
        .await
        .map_err(|e| Error::from_reason(format!("create shared session failed: {}", e)))?;
    let mut who = Participant::new(participant.id, participant.role.unwrap_or_else(|| "user".to_string()));
    if let Some(track_id) = participant.track_id {
        who = who.with_track_id(track_id);
    }
    if let Some(modality) = participant.modality {
        who = who.with_modality(modality);
    }
    if let Some(display_name) = participant.display_name {
        who = who.with_display_name(display_name);
    }
    let input = shared.participant_handle(who);
    let (audio_rx, video_rx, data_rx) = bridge_outputs(shared.subscribe_outputs());
    let membership = crate::shared_sessions::SharedMembership::join(Arc::clone(&shared));
    Ok(NapiStreamingSession {
        output_allocator: shared.output_allocator().await,
        copies: shared.transport_copy_context().await,
        session_id: shared.session_id().to_string(),
        input_sender: Arc::new(Mutex::new(Some(InputSender::Participant(input)))),
        audio_rx: Arc::new(Mutex::new(Some(audio_rx))),
        video_rx: Arc::new(Mutex::new(Some(video_rx))),
        data_rx: Arc::new(Mutex::new(Some(data_rx))),
        handle: Arc::new(Mutex::new(None)),
        membership: Arc::new(Mutex::new(Some(membership))),
        _executor: executor,
    })
}

/// Per-kind receivers fed from a shared session's output broadcast; outputs
/// keep their metadata (the producing participant).
fn bridge_outputs(
    mut outputs: remotemedia_core::transport::SharedPipelineOutputReceivers,
) -> (
    mpsc::Receiver<TransportData>,
    mpsc::Receiver<TransportData>,
    mpsc::Receiver<TransportData>,
) {
    let (audio_tx, audio_rx) = mpsc::channel(128);
    let (video_tx, video_rx) = mpsc::channel(128);
    let (data_tx, data_rx) = mpsc::channel(128);
    tokio::spawn(async move {
        while let Ok(Some(output)) = outputs.recv_output().await {
            let sent = match &output.data {
                RuntimeData::Audio { .. } => audio_tx.send(output).await,
                RuntimeData::Video { .. } => video_tx.send(output).await,
                _ => data_tx.send(output).await,
            };
            if sent.is_err() {
                break;
            }
        }
    });
    (audio_rx, video_rx, data_rx)
}
