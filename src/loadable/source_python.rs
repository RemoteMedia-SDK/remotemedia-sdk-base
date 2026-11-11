//! Source-load factory for Python plugins.
//!
//! Companion to [`crate::loadable::factory::LoadableNodeBundle`] — that
//! one wraps an already-built cdylib + dlopen, this one wraps Python
//! source code extracted by the resolver from a GitHub tarball.
//!
//! Plugin author publishes a repo with:
//! - `plugin.toml` at the root declaring `language = "python"` +
//!   `entry_module` + `node_types` + `requires`.
//! - One or more `.py` files for the node implementation.
//!
//! No Rust, no Cargo, no cdylib needed. The resolver fetches the
//! tarball, extracts it, then [`SourcePythonFactory`] takes over:
//! provisions a uv-managed venv from the plugin.toml `requires`,
//! spawns `python -m remotemedia.core.multiprocessing.runner` against
//! that venv, and exposes the registered node types as a
//! [`StreamingNodeFactory`] alongside built-in nodes.
//!
//! Gated behind the `python-source-plugin` cargo feature so the core
//! crate's default build stays narrow (no iceoryx2, no include_dir).

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use remotemedia_plugin_sdk::python_ipc::{
    BodyState, EmittedFrame, IpcCommand, TurnFrame, WireDataType, WireRuntimeData,
};
use remotemedia_plugin_sdk::python_subprocess::{
    spawn_python_subprocess_with_handshake, PluginProvisioning,
};
use tokio::sync::{mpsc, oneshot};

use std::collections::HashMap;

use crate::data::RuntimeData;
use crate::error::Error;
use crate::nodes::{NodeRuntimeContextRead, StreamingNode, StreamingNodeFactory};
use crate::python::env_manager::{PythonEnvConfig, PythonEnvManager};

/// Configuration for a source-loaded Python plugin instance.
///
/// Built by the resolver after parsing `plugin.toml` + extracting the
/// repo tarball. Carries everything `SourcePythonFactory::create` needs
/// to provision a venv + spawn the runner.
#[derive(Debug, Clone)]
pub struct SourcePythonPlugin {
    /// Node type this factory registers — matches the `node_type`
    /// referenced from `nodes[].node_type` in pipeline manifests.
    pub node_type: String,
    /// Python module to import via the runner's `--register-module`.
    /// Resolved against `module_root` on Python's `sys.path`.
    pub entry_module: String,
    /// Directory containing `entry_module.py` after extraction.
    /// Passed as `--module-root` to the runner.
    pub module_root: PathBuf,
    /// PEP 723 deps parsed from `plugin.toml`. Used to provision a
    /// managed uv venv via `PythonEnvManager::ensure_env`.
    pub requires: Vec<String>,
    /// Content hash (typically the source tarball's SHA256) used as the
    /// venv-cache key — same deps + same source → same venv.
    pub hash: String,
}

/// Factory that produces [`StreamingNode`] instances backed by Python
/// subprocesses spawned from on-disk source.
///
/// One factory per `(plugin, node_type)`. The resolver registers each
/// `node_types` entry from `plugin.toml` as its own
/// [`StreamingNodeFactory`] so the executor's registry doesn't need
/// to know anything new about plugins.
pub struct SourcePythonFactory {
    plugin: Arc<SourcePythonPlugin>,
    /// SHA-256 over the plugin's source files. Identifies the code a worker
    /// runs, so two versions of a plugin never share a worker.
    digest: String,
    /// `json_input = "text"` in `plugin.toml`.
    json_as_text: bool,
}

/// Environment switch that keeps the previous plugin-SDK runner for source
/// plugins (`REMOTEMEDIA_SOURCE_PLUGIN_RUNTIME=legacy`). Rollback only.
const RUNTIME_SWITCH: &str = "REMOTEMEDIA_SOURCE_PLUGIN_RUNTIME";

/// SHA-256 (hex) over the Python sources and `plugin.toml` under `root`:
/// each file's path relative to `root`, then its bytes, in path order.
/// Hidden directories and `__pycache__` are skipped.
pub fn content_digest(root: &std::path::Path) -> String {
    use sha2::{Digest, Sha256};

    fn collect(dir: &std::path::Path, files: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if path.is_dir() {
                if !name.starts_with('.') && name != "__pycache__" {
                    collect(&path, files);
                }
            } else if name.ends_with(".py") || name == "plugin.toml" {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    collect(root, &mut files);
    files.sort();
    let mut hasher = Sha256::new();
    for file in files {
        let relative = file.strip_prefix(root).unwrap_or(&file);
        hasher.update(relative.to_string_lossy().replace('\\', "/").as_bytes());
        hasher.update([0u8]);
        hasher.update(std::fs::read(&file).unwrap_or_default());
        hasher.update([0u8]);
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

impl SourcePythonFactory {
    /// Wrap a resolved plugin spec into a factory.
    pub fn new(plugin: SourcePythonPlugin) -> Self {
        let digest = content_digest(&plugin.module_root);
        Self {
            plugin: Arc::new(plugin),
            digest,
            json_as_text: false,
        }
    }

    /// Deliver JSON inputs to this plugin's nodes as text holding the JSON
    /// (`json_input = "text"`).
    pub fn json_as_text(mut self, on: bool) -> Self {
        self.json_as_text = on;
        self
    }

    /// SHA-256 (hex) of the plugin's source files.
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// A node on the in-tree multiprocess executor, the runtime regular
    /// Python nodes use: the plugin's module root on the worker's import
    /// path, its entry module imported first, its `requires` as base
    /// dependencies. (openspec: unify-python-source-plugin-runtime)
    fn create_on_executor(
        &self,
        node_id: String,
        params: &serde_json::Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::python_streaming::{PythonSpawnSettings, PythonStreamingNode};

        let plugin = &self.plugin;
        let mut params = params.clone();
        if !params.is_object() {
            params = serde_json::json!({});
        }
        if let Some(object) = params.as_object_mut() {
            object.insert(
                "__python_plugin_deps__".into(),
                serde_json::json!(plugin.requires),
            );
            // An environment belongs to one plugin: two plugins with the same
            // dependency list do not share one. The session's own scope
            // setting, when present, still applies on top.
            let scope = match object
                .get("__python_env_scope_context__")
                .and_then(|value| value.as_str())
                .filter(|value| !value.trim().is_empty())
            {
                Some(session_scope) => format!("{session_scope};plugin:{}", plugin.hash),
                None => format!("plugin:{}", plugin.hash),
            };
            object.insert("__python_env_scope_context__".into(), scope.into());
        }
        let settings = PythonSpawnSettings {
            python_path: vec![plugin.module_root.clone()],
            register_modules: vec![plugin.entry_module.clone()],
            env: vec![("REMOTEMEDIA_PLUGIN_DIGEST".into(), self.digest.clone())],
            json_as_text: self.json_as_text,
        };
        let node = match session_id {
            Some(session) if !session.is_empty() => {
                PythonStreamingNode::with_session(node_id, &plugin.node_type, &params, session)?
            }
            _ => PythonStreamingNode::new(node_id, &plugin.node_type, &params)?,
        }
        .with_spawn_settings(settings);
        Ok(Box::new(crate::nodes::AsyncNodeWrapper(Arc::new(node))))
    }

    /// Access the resolved plugin metadata.
    pub fn plugin(&self) -> &SourcePythonPlugin {
        &self.plugin
    }
}

impl StreamingNodeFactory for SourcePythonFactory {
    fn node_type(&self) -> &str {
        &self.plugin.node_type
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }

    fn create(
        &self,
        node_id: String,
        params: &serde_json::Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        if std::env::var(RUNTIME_SWITCH).as_deref() != Ok("legacy") {
            return self.create_on_executor(node_id, params, session_id);
        }
        let plugin = self.plugin.clone();
        let params_json = serde_json::to_string(params).unwrap_or_else(|_| "{}".to_string());

        // Provision the venv synchronously in a one-shot tokio runtime
        // — matches the cdylib path's `provision_plugin_env_blocking`
        // for ordering reasons (we want READY observed before
        // create_node returns).
        let provisioning_handle = std::thread::spawn({
            let plugin = plugin.clone();
            let mut env_config = PythonEnvConfig::from_env();
            if let Some(python_version) = params
                .get("__python_version__")
                .and_then(|value| value.as_str())
                .filter(|value| !value.trim().is_empty())
            {
                env_config.python_version = python_version.to_string();
            }
            let scope_context = params
                .get("__python_env_scope_context__")
                .and_then(|value| value.as_str())
                .filter(|value| !value.trim().is_empty())
                .map(|value| format!("{value};plugin:{}", plugin.hash));
            move || -> Result<PluginProvisioning, String> {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("build provisioning rt: {e}"))?;
                rt.block_on(provision_source_plugin_env(
                    plugin.module_root.clone(),
                    plugin.requires.clone(),
                    plugin.hash.clone(),
                    env_config,
                    scope_context,
                ))
            }
        });
        let provisioning = provisioning_handle
            .join()
            .map_err(|_| {
                Error::Execution("source-python provisioning thread panicked".to_string())
            })?
            .map_err(|e| {
                Error::Execution(format!(
                    "source-python plugin '{}' provisioning failed: {e}",
                    plugin.node_type
                ))
            })?;

        // Prefer the host's session_id (so iceoryx2 channel names line
        // up with `SessionControlBus::get(session_id)` and the
        // plugin-sdk control hook can route runtime PROGRESS:/PUBLISH:
        // payloads back into the bus). Fall back to a synthetic id
        // only when invoked outside of a streaming session (tests,
        // standalone harnesses) — same anti-collision shape as before
        // so existing test fixtures don't break.
        let session_id = match session_id {
            Some(s) if !s.is_empty() => s,
            _ => format!(
                "rmsrc-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros())
                    .unwrap_or(0),
            ),
        };

        let (cmd_tx, child, handshake) = spawn_python_subprocess_with_handshake(
            &provisioning,
            &plugin.entry_module,
            &plugin.node_type,
            &node_id,
            &session_id,
            &params_json,
        )
        .map_err(|e| {
            Error::Execution(format!(
                "source-python plugin '{}' subprocess spawn failed: {e}",
                plugin.node_type
            ))
        })?;

        Ok(Box::new(SourcePythonNode {
            node_type: plugin.node_type.clone(),
            session_id,
            node_id,
            cmd_tx,
            declared_instance_scope: handshake.instance_scope,
            declared_self_driven: handshake.self_driven,
            declared_background: handshake.background,
            body: Default::default(),
            _child: child,
            _env_lease: provisioning.venv.clone(),
        }))
    }
}

async fn provision_source_plugin_env(
    module_root: PathBuf,
    deps: Vec<String>,
    hash: String,
    env_config: PythonEnvConfig,
    scope_context: Option<String>,
) -> Result<PluginProvisioning, String> {
    let env_mgr =
        PythonEnvManager::new(env_config).map_err(|e| format!("PythonEnvManager::new: {e}"))?;
    let venv = env_mgr
        .ensure_env_scoped_leased(&deps, scope_context.as_deref())
        .await
        .map_err(|e| format!("PythonEnvManager::ensure_env({deps:?}): {e}"))?;

    Ok(PluginProvisioning {
        hash,
        extracted_dir: module_root,
        deps,
        venv,
    })
}

/// `StreamingNode` driving a Python subprocess via plugin-sdk's IPC
/// machinery. Counterpart to plugin-sdk's `PythonSubprocessNode` (which
/// implements `FfiNode` for the cdylib path) — same subprocess + IPC,
/// different trait surface.
struct SourcePythonNode {
    node_type: String,
    session_id: String,
    cmd_tx: mpsc::Sender<IpcCommand>,
    /// The node class's `@instance_scope(...)`, as the worker reported it
    /// before READY.
    declared_instance_scope: Option<crate::nodes::InstanceScope>,
    /// Whether the node class is self-driven, as the worker reported it.
    declared_self_driven: bool,
    /// The worker's node has a `background()` that waits for the start
    /// signal: this node's body starts it and reports how it ended.
    declared_background: bool,
    /// How the worker's `background()` stands.
    body: tokio::sync::watch::Sender<BodyState>,
    /// The id the runner was started with (it names the runner's stop event).
    node_id: String,
    /// Held so the subprocess is killed on drop.
    _child: Arc<std::sync::Mutex<std::process::Child>>,
    /// Environment cache lease. Fields drop after `Drop::drop`, so this is
    /// released only after the runner has been terminated.
    _env_lease: remotemedia_py_env::LeasedVenvInfo,
}

impl Drop for SourcePythonNode {
    fn drop(&mut self) {
        remotemedia_plugin_sdk::python_ipc::terminate_runner(&self._child);
    }
}

/// Deliver one frame the Python node emitted on its own. `runtime_owns_body`:
/// the node's body is run by the router, which finishes the emitter when the
/// body returns.
async fn deliver_emitted(
    mut frame: EmittedFrame,
    emitter: &crate::nodes::Emitter,
    body: &tokio::sync::watch::Sender<BodyState>,
    runtime_owns_body: bool,
    node_type: &str,
) {
    if let Some(error) = frame.failed.take() {
        if !runtime_owns_body {
            tracing::error!(node_type = %node_type, "the Python runner exited: {error}");
        }
        body.send_replace(BodyState::Failed(error));
        return;
    }
    if frame.finished {
        if runtime_owns_body {
            body.send_replace(BodyState::Finished);
        } else {
            emitter.body_finished();
        }
        return;
    }
    let origin = match frame.origin_mode {
        0 => crate::nodes::EmitOrigin::Default,
        1 => crate::nodes::EmitOrigin::Unattributed,
        _ => crate::nodes::EmitOrigin::Participant(frame.participant),
    };
    let result = match WireRuntimeData::from_bytes(&frame.frame)
        .map_err(Error::Execution)
        .and_then(decode_wire)
    {
        Ok(data) => emitter.emit_as(data, origin).await,
        Err(error) => Err(error),
    };
    if let Err(error) = result {
        tracing::error!(node_type = %node_type, "emitted frame was not delivered: {error}");
    }
}

impl SourcePythonNode {
    fn wire_copies(
        &self,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<remotemedia_types::CopyContext, Error> {
        wire_copies(&self.node_type, ctx)
    }
}

/// Source-loaded Python exchanges the copying legacy wire. Charge its bulk
/// encode/decode to the caller's explicit compatibility ledger; strict or
/// unattributed calls reject before any copy.
fn wire_copies(
    node_type: &str,
    ctx: &dyn NodeRuntimeContextRead,
) -> Result<remotemedia_types::CopyContext, Error> {
    match ctx.copy_context() {
        Some(copies) if copies.policy() == remotemedia_types::CopyPolicy::AllowCopies => {
            Ok(copies)
        }
        _ => Err(Error::Execution(format!(
            "source-python plugin '{node_type}' uses the copying legacy wire and requires an explicit allow_copies context; declare [python.mapped] for strict buffer policy"
        ))),
    }
}

#[async_trait]
impl StreamingNode for SourcePythonNode {
    fn node_type(&self) -> &str {
        &self.node_type
    }

    fn instance_scope(&self) -> Option<crate::nodes::InstanceScope> {
        self.declared_instance_scope
    }

    fn self_driven(&self) -> Option<bool> {
        self.declared_self_driven.then_some(true)
    }

    fn is_multi_input(&self) -> bool {
        false
    }

    /// Frames the Python node emits on its own go to the node instance's
    /// emitter, in the order the node sent them (openspec:
    /// add-node-initiated-output).
    async fn initialize(&self, ctx: &dyn crate::nodes::InitializeContextRead) -> Result<(), Error> {
        if ctx.emitter_any().is_none() {
            return Ok(());
        }
        let emitter = crate::nodes::Emitter::of(ctx);
        let (frames, mut emitted) =
            mpsc::channel::<remotemedia_plugin_sdk::python_ipc::EmittedFrame>(64);
        self.cmd_tx
            .send(IpcCommand::EmitSink { frames })
            .await
            .map_err(|e| Error::Execution(format!("ipc cmd send: {e}")))?;
        let node_type = self.node_type.clone();
        let (body, runtime_owns_body) = (self.body.clone(), self.declared_background);
        tokio::spawn(async move {
            while let Some(frame) = emitted.recv().await {
                deliver_emitted(frame, &emitter, &body, runtime_owns_body, &node_type).await;
            }
        });
        Ok(())
    }

    fn has_background(&self) -> bool {
        self.declared_background
    }

    /// The Python node's `background()`: the router calls this when the
    /// instance starts. It tells the worker to start the body and resolves
    /// when the body returned, or fails when the worker exited (a failing
    /// `background()` ends the worker).
    async fn background(&self, _ctx: &dyn NodeRuntimeContextRead) -> Result<(), Error> {
        remotemedia_plugin_sdk::python_ipc::run_body(&self.cmd_tx, &self.session_id, &self.body)
            .await
            .map_err(|error| Error::Execution(format!("node type '{}': {error}", self.node_type)))
    }

    /// The runtime's `shutdown` notice: ask the worker to stop, so it
    /// cancels `background()` and runs `cleanup()`, before this node is
    /// dropped and whatever is left of the runner is ended.
    async fn process_control_message(
        &self,
        message: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<bool, Error> {
        if crate::transport::session_control::aux_port_of(&message).as_deref() == Some("shutdown") {
            let (child, session_id, node_id) =
                (self._child.clone(), self.session_id.clone(), self.node_id.clone());
            let _ = tokio::task::spawn_blocking(move || {
                remotemedia_plugin_sdk::python_ipc::stop_runner(
                    &child,
                    &session_id,
                    &node_id,
                    remotemedia_plugin_sdk::python_ipc::STOP_GRACE,
                )
            })
            .await;
        }
        Ok(false)
    }

    /// Only the runtime-delivered `__call_reply__` uses the native lane
    /// (call-reply-only lane); other `@rpc` methods keep reaching the
    /// runner's dispatcher through the input path.
    fn rpc_methods(&self) -> Vec<remotemedia_traits::rpc::RpcMethod> {
        vec![remotemedia_traits::call::call_reply_method()]
    }

    /// Forward `__call_reply__` to the runner out of band — published even
    /// while a `process()` round is in flight, with no output expected.
    async fn call_rpc(
        &self,
        request: remotemedia_traits::rpc::RpcRequest,
        _ctx: &dyn NodeRuntimeContextRead,
        _replies: &mut remotemedia_traits::rpc::RpcResponder,
    ) -> Result<(), Error> {
        if request.method != remotemedia_traits::call::CALL_REPLY_METHOD {
            return Err(Error::Execution(format!(
                "unknown RPC method: {}",
                request.method
            )));
        }
        let frame = remotemedia_plugin_sdk::python_subprocess::call_reply_frame(&request);
        let wire = WireRuntimeData::now_text(&frame, &self.session_id);
        self.cmd_tx
            .send(IpcCommand::Send {
                req_bytes: wire.to_bytes(),
            })
            .await
            .map_err(|e| Error::Execution(format!("ipc cmd send: {e}")))
    }

    async fn process_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        let copies = self.wire_copies(ctx)?;
        let req_bytes = encode_input(&data, &self.session_id, &copies)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(IpcCommand::Round {
                req_bytes,
                reply: reply_tx,
            })
            .await
            .map_err(|e| Error::Execution(format!("ipc cmd send: {e}")))?;
        let resp_bytes = reply_rx
            .await
            .map_err(|e| Error::Execution(format!("ipc reply recv: {e}")))?
            .map_err(|e| Error::Execution(format!("ipc reply: {e}")))?;
        decode_output(&resp_bytes, &copies)
    }

    async fn process_multi_async(
        &self,
        _inputs: HashMap<String, RuntimeData>,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        // Source-load Python plugins don't currently support
        // multi-input (the Python runner's IPC layer is single-input).
        // Surface a precise error rather than the generic "not
        // implemented" default trait method emits.
        Err(Error::Execution(format!(
            "source-python plugin '{}' does not support multi-input nodes",
            self.node_type
        )))
    }

    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<usize, Error> {
        let copies = self.wire_copies(ctx)?;
        let req_bytes = encode_input(&data, &self.session_id, &copies)?;
        // Per-frame streaming: the IPC thread forwards each yield onto
        // `frame_rx` as it arrives, instead of accumulating into a Vec
        // and returning at EndOfInput. Capacity 32 is plenty for any
        // real generator — the consumer (router) drains in the same
        // event loop tick and `blocking_send` on the IPC thread will
        // park if the consumer ever falls behind.
        //
        // One turn: the input carries the participant it came from, and the
        // input's replies and the frames the node emits meanwhile arrive on
        // one channel, in the order the node produced them.
        let (frame_tx, mut frame_rx) = mpsc::channel::<TurnFrame>(32);
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(IpcCommand::Turn {
                req_bytes,
                origin: ctx.origin().map(|origin| origin.to_json()),
                frames: frame_tx,
                reply: reply_tx,
            })
            .await
            .map_err(|e| Error::Execution(format!("ipc cmd send: {e}")))?;

        let emitter = crate::nodes::Emitter::for_call(ctx);
        let mut count = 0usize;
        while let Some(frame) = frame_rx.recv().await {
            match frame {
                TurnFrame::Reply(frame) => {
                    callback(decode_output(&frame, &copies)?)?;
                    count += 1;
                }
                TurnFrame::Emitted(frame) => {
                    deliver_emitted(
                        frame,
                        &emitter,
                        &self.body,
                        self.declared_background,
                        &self.node_type,
                    )
                    .await
                }
            }
        }
        // `frame_rx` returns `None` only after the IPC thread drops its
        // sender, which happens immediately before it sends on `reply`.
        // The await below is therefore a quick handshake, not a wait
        // for generation.
        reply_rx
            .await
            .map_err(|e| Error::Execution(format!("ipc reply recv: {e}")))?
            .map_err(|e| Error::Execution(format!("ipc reply: {e}")))?;
        Ok(count)
    }
}

/// Encode a public RuntimeData → wire bytes the Python runner expects.
///
/// Supports the same variants plugin-sdk's cdylib path does (Text,
/// Audio, Tensor, Video, ControlMessage, plus Json → Text for aux
/// ports). Other variants surface a precise error so misroutes fail
/// loud at the boundary.
fn encode_input(
    data: &RuntimeData,
    session_id: &str,
    copies: &remotemedia_types::CopyContext,
) -> Result<Vec<u8>, Error> {
    copies
        .materialize_result(
            remotemedia_types::CopyReason::PortableSerialization,
            data.bulk_payload_bytes() as u64,
            || encode_wire(data, session_id),
        )
        .map_err(|rejected| Error::Execution(rejected.to_string()))?
}

fn encode_wire(data: &RuntimeData, session_id: &str) -> Result<Vec<u8>, Error> {
    let wire = match data {
        RuntimeData::Text(t) => WireRuntimeData::now_text(t, session_id),
        RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            ..
        } => {
            let ch = u16::try_from(*channels).map_err(|_| {
                Error::Execution(format!(
                    "audio channels {channels} doesn't fit u16 (wire format limit)"
                ))
            })?;
            WireRuntimeData::now_audio(samples, *sample_rate, ch, session_id)
        }
        RuntimeData::Tensor {
            data,
            shape,
            dtype,
            metadata,
        } => {
            let shape_u32: Vec<u32> = shape
                .iter()
                .map(|d| {
                    u32::try_from(*d)
                        .map_err(|_| Error::Execution(format!("negative tensor dim: {d}")))
                })
                .collect::<Result<_, _>>()?;
            let dtype_u8 = u8::try_from(*dtype)
                .map_err(|_| Error::Execution(format!("tensor dtype {dtype} doesn't fit u8")))?;
            WireRuntimeData::now_tensor(data, &shape_u32, dtype_u8, metadata.as_ref(), session_id)
        }
        RuntimeData::ControlMessage {
            message_type,
            segment_id,
            timestamp_ms,
            metadata,
        } => {
            let payload = serde_json::json!({
                "message_type": message_type,
                "segment_id":   segment_id,
                "timestamp_ms": timestamp_ms,
                "metadata":     metadata,
            });
            WireRuntimeData::now_control_message(&payload, session_id)
        }
        RuntimeData::Json(value) => {
            // Aux-port envelope path — JSON serialized as text.
            let text = serde_json::to_string(value)
                .map_err(|e| Error::Execution(format!("encode Json input: {e}")))?;
            WireRuntimeData::now_text(&text, session_id)
        }
        other => {
            return Err(Error::Execution(format!(
                "SourcePythonNode encodes only Text/Audio/Tensor/ControlMessage/Json input \
                 (got variant {:?})",
                std::mem::discriminant(other)
            )));
        }
    };
    Ok(wire.to_bytes())
}

/// Decode wire bytes from the Python runner → public RuntimeData.
fn decode_output(
    bytes: &[u8],
    copies: &remotemedia_types::CopyContext,
) -> Result<RuntimeData, Error> {
    let wire = WireRuntimeData::from_bytes(bytes)
        .map_err(|e| Error::Execution(format!("wire decode: {e}")))?;
    let output = decode_wire(wire)?;
    // Callers bind an AllowCopies ledger (`wire_copies`), so this records the
    // exact reconstructed element bytes, excluding wire headers and metadata.
    copies
        .materialize(
            remotemedia_types::CopyReason::PortableDeserialization,
            output.bulk_payload_bytes() as u64,
            || (),
        )
        .map_err(|rejected| Error::Execution(rejected.to_string()))?;
    Ok(output)
}

fn decode_wire(wire: WireRuntimeData) -> Result<RuntimeData, Error> {
    match wire.data_type {
        WireDataType::Text => {
            let text = String::from_utf8_lossy(&wire.payload).into_owned();
            Ok(RuntimeData::Text(text))
        }
        WireDataType::Json => serde_json::from_slice(&wire.payload)
            .map(RuntimeData::Json)
            .map_err(|e| Error::Execution(format!("decode json: {e}"))),
        WireDataType::Audio => {
            let (samples, sample_rate, channels) = wire
                .decode_audio()
                .map_err(|e| Error::Execution(format!("decode audio: {e}")))?;
            Ok(RuntimeData::Audio {
                samples: crate::data::AudioSamples::from(samples),
                sample_rate,
                channels: channels as u32,
                stream_id: None,
                timestamp_us: Some(wire.timestamp_us),
                arrival_ts_us: None,
                metadata: None,
            })
        }
        WireDataType::Tensor => {
            let (data, shape, dtype, extras) = wire
                .decode_tensor()
                .map_err(|e| Error::Execution(format!("decode tensor: {e}")))?;
            let metadata = match extras {
                serde_json::Value::Null => None,
                other => Some(other),
            };
            Ok(RuntimeData::Tensor {
                data: data.into(),
                shape: shape.into_iter().map(|d| d as i32).collect(),
                dtype: dtype as i32,
                metadata,
            })
        }
        other => Err(Error::Execution(format!(
            "SourcePythonNode decodes only Text/Json/Audio/Tensor output (got {other:?})"
        ))),
    }
}

#[cfg(test)]
mod wire_copy_tests {
    use super::*;
    use crate::nodes::NodeRuntimeContext;
    use remotemedia_types::{CopyContext, CopyPolicy, CopyReason};

    fn counts(copies: &CopyContext, reason: CopyReason) -> remotemedia_types::CopyCounts {
        copies
            .report()
            .entries
            .into_iter()
            .find(|(entry, _)| *entry == reason)
            .map(|(_, counts)| counts)
            .unwrap_or_default()
    }

    #[test]
    fn strict_and_unattributed_contexts_reject_before_any_copy() {
        let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "source-python-strict");
        let ctx = NodeRuntimeContext::for_test("s", "n").with_copy_context(strict.clone());
        let error = wire_copies("Echo", &ctx).unwrap_err().to_string();
        assert!(error.contains("[python.mapped]"), "{error}");
        assert_eq!(
            counts(&strict, CopyReason::PortableSerialization).rejected,
            0
        );
        let allowed = CopyContext::new(CopyPolicy::AllowCopies, "source-python-allowed");
        let ctx = NodeRuntimeContext::for_test("s", "n").with_copy_context(allowed.clone());
        assert!(wire_copies("Echo", &ctx).unwrap().same_authority(&allowed));
    }

    #[test]
    fn json_output_from_the_runner_decodes_as_json() {
        // `MultiprocessNode._send_output` writes RuntimeData.json(...) as type 9.
        let payload = br#"{"kind":"turn_end","complete":true}"#;
        let mut bytes = vec![9u8];
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.push(b's');
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(payload);
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "source-python-json");
        let RuntimeData::Json(value) = decode_output(&bytes, &copies).unwrap() else {
            panic!("expected json")
        };
        assert_eq!(value, serde_json::json!({"kind": "turn_end", "complete": true}));
    }

    #[test]
    fn legacy_wire_round_trip_charges_bulk_bytes_to_the_call_ledger() {
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "source-python-call");
        let audio = RuntimeData::Audio {
            samples: vec![0.25_f32; 10].into(),
            sample_rate: 16_000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };
        let bytes = encode_input(&audio, "s", &copies).unwrap();
        let encoded = counts(&copies, CopyReason::PortableSerialization);
        assert_eq!((encoded.completed, encoded.copied_bytes), (1, 40));
        let decoded = decode_output(&bytes, &copies).unwrap();
        let RuntimeData::Audio { samples, .. } = &decoded else {
            panic!("expected audio")
        };
        assert_eq!(samples.len(), 10);
        assert!(samples.iter().all(|value| *value == 0.25));
        let decoded = counts(&copies, CopyReason::PortableDeserialization);
        assert_eq!((decoded.completed, decoded.copied_bytes), (1, 40));
        // Metadata-only payloads stay unattributed: no bulk bytes move.
        let text = encode_input(&RuntimeData::Text("hi".into()), "s", &copies).unwrap();
        decode_output(&text, &copies).unwrap();
        assert_eq!(
            counts(&copies, CopyReason::PortableSerialization).completed,
            1
        );
        assert_eq!(
            counts(&copies, CopyReason::PortableDeserialization).completed,
            1
        );
    }
}
