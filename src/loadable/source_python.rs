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
use remotemedia_plugin_sdk::python_ipc::{IpcCommand, WireDataType, WireRuntimeData};
use remotemedia_plugin_sdk::python_subprocess::{spawn_python_subprocess, PluginProvisioning};
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
}

impl SourcePythonFactory {
    /// Wrap a resolved plugin spec into a factory.
    pub fn new(plugin: SourcePythonPlugin) -> Self {
        Self {
            plugin: Arc::new(plugin),
        }
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

    fn create(
        &self,
        node_id: String,
        params: &serde_json::Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
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

        let (cmd_tx, child) = spawn_python_subprocess(
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
            cmd_tx,
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

    fn is_multi_input(&self) -> bool {
        false
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
        let (frame_tx, mut frame_rx) = mpsc::channel::<Vec<u8>>(32);
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(IpcCommand::RoundStreaming {
                req_bytes,
                frame_tx,
                reply: reply_tx,
            })
            .await
            .map_err(|e| Error::Execution(format!("ipc cmd send: {e}")))?;

        let mut count = 0usize;
        while let Some(frame) = frame_rx.recv().await {
            let runtime_data = decode_output(&frame, &copies)?;
            callback(runtime_data)?;
            count += 1;
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
            "SourcePythonNode decodes only Text/Audio/Tensor output (got {other:?})"
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
