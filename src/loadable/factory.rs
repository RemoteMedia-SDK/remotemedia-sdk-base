//! `StreamingNodeFactory` wrapping an in-process loaded plugin.
//!
//! Usage:
//!
//! ```no_run
//! use std::path::Path;
//! use remotemedia_core::loadable::factory::LoadableNodeBundle;
//! use remotemedia_core::nodes::StreamingNodeRegistry;
//!
//! let bundle = LoadableNodeBundle::load(Path::new("./my_plugin.so")).unwrap();
//! let mut registry = StreamingNodeRegistry::new();
//! bundle.register_into(&mut registry);
//! // From here every factory the plugin exposed is reachable via
//! // `registry.create_node("MyPluginNode", ...)`.
//! ```
//!
//! # Wire format
//!
//! The FFI payload is the public `crate::data::RuntimeData` enum
//! serialized with `rmp-serde` (MessagePack). That gives **full
//! variant coverage automatically** — Audio (with `AudioSamples`),
//! Video, Image, Tensor, Numpy, Json, Text, Binary, ControlMessage,
//! File — because the public type derives `Serialize` / `Deserialize`
//! and so do all its sub-types.
//!
//! MessagePack rather than bincode 1.x: bincode 1 doesn't support
//! `deserialize_any`, which `serde_json::Value` (used inside Json,
//! ControlMessage metadata, Audio metadata) requires. MessagePack is
//! self-describing and handles it natively.
//!
//! Plugin and host must agree on the rmp-serde version. Both link
//! `remotemedia-core` and use it transitively, so they always agree.
//!
//! For the multiprocess Python path we still use the hand-crafted
//! `multiprocess::data_transfer::RuntimeData` binary format — it
//! has a stable wire layout the Python decoder expects, predates
//! this loadable path, and supports a smaller variant set. The two
//! formats coexist because they target different runtimes.
//!
//! # ABI safety
//!
//! `loadable_node_abi` uses `#[sabi_trait]` and `#[sabi(kind(Prefix(...)))]`
//! so the FFI surface (trait vtables, struct layouts, root-module
//! prefix) is checked at load time by abi_stable. A plugin built
//! against a different abi_stable version refuses to load with a
//! typed `LibraryError` — not UB.

use std::path::Path;
use std::sync::Arc;

use abi_stable::library::RootModule;
use abi_stable::sabi_trait::TD_Opaque;
use abi_stable::std_types::{RErr, ROk, RResult, RString, RVec};
use async_trait::async_trait;
use loadable_node_abi::{
    FfiNodeBox, FfiNodeFactory, FfiNodeFactoryBox, FfiNodeFactory_TO, NodePluginRef, OutputSink,
    OutputSinkBox, OutputSink_TO,
};
use serde_json::Value;

use crate::data::RuntimeData;
use crate::nodes::{
    AsyncNodeWrapper, AsyncStreamingNode, InitializeContextRead, NodeRuntimeContextRead,
    StreamingNode, StreamingNodeFactory, StreamingNodeRegistry,
};
use crate::Error;

/// Host-side `OutputSink` implementation: forwards each plugin
/// emission onto a tokio mpsc channel the host drains in real time.
///
/// `push` is sync (it's the FFI contract) — it `try_send`s first, and
/// on a full channel parks via `blocking_send` so backpressure
/// propagates back into the plugin's generator. Since `push` runs on
/// the plugin's multi-thread runtime (`plugin_runtime()` in
/// `plugin-sdk`), `blocking_send` is safe — one worker parks, the
/// other keeps spinning until the host catches up.
struct ChannelSink {
    tx: tokio::sync::mpsc::Sender<RVec<u8>>,
}

impl OutputSink for ChannelSink {
    fn push(&self, bytes: RVec<u8>) -> RResult<(), RString> {
        match self.tx.try_send(bytes) {
            Ok(()) => ROk(()),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                RErr(RString::from("sink receiver closed"))
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(bytes)) => {
                // Channel is at capacity. Block the calling worker
                // until a slot frees — propagates backpressure to the
                // plugin's generator. Mirrors the dispatch in
                // `session_router.rs::cb_fan_tx.try_send` (line ~1543).
                let send_res = match tokio::runtime::Handle::try_current() {
                    Err(_) => self.tx.blocking_send(bytes),
                    Ok(handle) => match handle.runtime_flavor() {
                        tokio::runtime::RuntimeFlavor::MultiThread => {
                            tokio::task::block_in_place(|| self.tx.blocking_send(bytes))
                        }
                        _ => {
                            // Single-thread runtime: can't block
                            // safely. Drop with a typed error so the
                            // plugin can unwind cleanly.
                            return RErr(RString::from(
                                "sink full on non-multi-thread runtime; dropping frame",
                            ));
                        }
                    },
                };
                match send_res {
                    Ok(()) => ROk(()),
                    Err(_) => RErr(RString::from("sink receiver closed during blocking send")),
                }
            }
        }
    }
}

/// One loaded plugin, plus the factory adapters it exposed. Hold
/// this for as long as you want the registered factories to be
/// callable. `abi_stable` keeps the underlying library mapped for
/// the lifetime of the process once `load_from_file` succeeds, so
/// dropping the bundle does *not* unload the library — but it does
/// drop the `Arc<dyn StreamingNodeFactory>` references the host
/// holds.
pub struct LoadableNodeBundle {
    plugin: NodePluginRef,
    factories: Vec<Arc<dyn StreamingNodeFactory>>,
}

impl LoadableNodeBundle {
    /// Load a plugin from disk. Validates abi_stable layout and
    /// version on the way in — mismatches return a typed error.
    pub fn load(path: &Path) -> Result<Self, Error> {
        let plugin = NodePluginRef::load_from_file(path).map_err(|e| {
            Error::Execution(format!("loadable plugin load {}: {e:?}", path.display()))
        })?;

        let raw_factories = plugin.list_factories()();
        let factories: Vec<Arc<dyn StreamingNodeFactory>> = raw_factories
            .into_iter()
            .map(|f| {
                let node_type = f.node_type().into_string();
                Arc::new(LoadableFactoryAdapter {
                    inner: f,
                    node_type,
                }) as Arc<dyn StreamingNodeFactory>
            })
            .collect();

        Ok(Self { plugin, factories })
    }

    /// abi_stable handle to the loaded plugin's root module.
    pub fn plugin(&self) -> NodePluginRef {
        self.plugin
    }

    /// Adapters for every node type the plugin advertised.
    pub fn factories(&self) -> &[Arc<dyn StreamingNodeFactory>] {
        &self.factories
    }

    /// Register every factory into the given registry.
    pub fn register_into(&self, registry: &mut StreamingNodeRegistry) {
        for f in &self.factories {
            registry.register(Arc::clone(f));
        }
    }
}

/// Wrap a plugin factory (no dlopen) so it can be registered into a
/// `StreamingNodeRegistry`.
///
/// For in-process linked plugins: the plugin crate is linked as an
/// rlib (`crate-type = ["cdylib", "rlib"]`), the consumer constructs
/// the factory concrete type and hands it to this function. The
/// abi_stable trait-object wrap (`FfiNodeFactory_TO::from_value(..,
/// TD_Opaque)`) happens internally, so callers never need to import
/// `abi_stable` or `loadable_node_abi`. The resulting
/// `Arc<dyn StreamingNodeFactory>` behaves identically to one produced
/// by `LoadableNodeBundle::load` but skips the dynamic-library load
/// step entirely.
///
/// ```ignore
/// use remotemedia_core::loadable::factory::wrap_ffi_factory;
/// use silero_vad_loadable_plugin::SileroVADNodeFactory;
///
/// executor
///     .register_factory(wrap_ffi_factory(SileroVADNodeFactory::default()))
///     .await;
/// ```
pub fn wrap_ffi_factory<F>(factory: F) -> Arc<dyn StreamingNodeFactory>
where
    F: FfiNodeFactory + 'static,
{
    let boxed: FfiNodeFactoryBox = FfiNodeFactory_TO::from_value(factory, TD_Opaque);
    let node_type = boxed.node_type().into_string();
    Arc::new(LoadableFactoryAdapter {
        inner: boxed,
        node_type,
    })
}

struct LoadableFactoryAdapter {
    inner: FfiNodeFactoryBox,
    node_type: String,
}

impl StreamingNodeFactory for LoadableFactoryAdapter {
    fn node_type(&self) -> &str {
        &self.node_type
    }

    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let params_json = serde_json::to_string(params).unwrap_or_else(|_| "{}".to_string());
        let ffi_node = match self.inner.create(RString::from(params_json)) {
            RResult::ROk(n) => n,
            RResult::RErr(e) => {
                return Err(Error::Execution(format!(
                    "loadable factory create '{}' (id={node_id}): {}",
                    self.node_type,
                    e.into_string()
                )));
            }
        };

        Ok(Box::new(AsyncNodeWrapper(Arc::new(LoadableNodeAdapter {
            inner: ffi_node,
            node_type: self.node_type.clone(),
            node_id,
        }))))
    }
}

struct LoadableNodeAdapter {
    inner: FfiNodeBox,
    node_type: String,
    node_id: String,
}

#[async_trait]
impl AsyncStreamingNode for LoadableNodeAdapter {
    fn node_type(&self) -> &str {
        &self.node_type
    }

    /// Forward initialization across the FFI boundary so lazy-load
    /// plugins (e.g. llama-cpp spawning a worker that loads the GGUF)
    /// actually run their `initialize()` before the first `process()`.
    /// Plugins that do all work eagerly inside the factory's `create()`
    /// see a no-op (FFI default impl returns `Ok(())`).
    async fn initialize(&self, ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        match self
            .inner
            .initialize(
                RString::from(ctx.session_id().to_string()),
                RString::from(self.node_id.clone()),
            )
            .await
        {
            RResult::ROk(()) => Ok(()),
            RResult::RErr(e) => Err(Error::from_plugin(
                e.into_string(),
                &self.node_id,
                &self.node_type,
            )),
        }
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        // Encode any RuntimeData variant via MessagePack. No
        // per-variant mapping needed — the public type derives
        // Serialize and so does every sub-type (AudioSamples,
        // ImageFormat, ControlMessageType, serde_json::Value, …).
        // Use named (map) encoding to remain compatible with
        // `#[serde(skip_serializing_if = "Option::is_none")]` fields
        // on `RuntimeData` variants. The compact (positional) encoder
        // would shift fields when partial optionals are skipped.
        let req_bytes = rmp_serde::to_vec_named(&data).map_err(|e| {
            Error::Execution(format!(
                "rmp encode for plugin '{}' (id={}): {e}",
                self.node_type, self.node_id
            ))
        })?;

        match self.inner.process(RVec::from(req_bytes)).await {
            RResult::ROk(out) => {
                rmp_serde::from_slice::<RuntimeData>(out.as_slice()).map_err(|e| {
                    Error::Execution(format!(
                        "rmp decode from plugin '{}' (id={}): {e}",
                        self.node_type, self.node_id
                    ))
                })
            }
            RResult::RErr(e) => Err(Error::from_plugin(
                e.into_string(),
                &self.node_id,
                &self.node_type,
            )),
        }
    }

    /// Streaming entry point — forwards through the per-frame FFI
    /// path (`FfiNode::process_streaming`) so multi-emission nodes
    /// (SileroVAD's `Json(event) + audio passthrough`, Whisper's
    /// per-segment fragments, TTS audio chunks, …) keep their full
    /// output fan-out across the dlopen boundary AND retain real-time
    /// streaming — previously this called `process_multi`, which
    /// collected every emission into an `RVec` before returning,
    /// delaying a 13-second TTS turn until generation completed.
    ///
    /// Bridge: we hand the plugin a `ChannelSink` backed by a tokio
    /// mpsc, then drain that channel in this function and invoke the
    /// user's `callback` per frame. When the FFI future completes it
    /// drops the sink (and thus the sender), so `frame_rx.recv()`
    /// observes `None` and the loop exits.
    ///
    /// Plugins not yet rebuilt against the streaming ABI fall through
    /// to the default `process_streaming` impl in `loadable-node-abi`,
    /// which delegates to `process_multi` and preserves correctness
    /// (just without real-time streaming) — same behaviour as before.
    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        _session_id: Option<String>,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let req_bytes = rmp_serde::to_vec_named(&data).map_err(|e| {
            Error::Execution(format!(
                "rmp encode for plugin '{}' (id={}): {e}",
                self.node_type, self.node_id
            ))
        })?;

        // Capacity 32 is a comfortable margin for any real plugin —
        // the drain loop below runs on the host runtime and pulls
        // frames as fast as the user callback consumes them. If the
        // callback is slow the sink's `push` parks via blocking_send
        // (multi-threaded plugin runtime → safe), propagating real
        // backpressure to the plugin's generator.
        let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel::<RVec<u8>>(32);
        let sink: OutputSinkBox =
            OutputSink_TO::from_value(ChannelSink { tx: frame_tx }, TD_Opaque);

        // Build the FFI future before we await anything else so the
        // plugin starts generating in parallel with our drain loop.
        // The future captures `sink` by value (FfiFuture is `'static`),
        // so there's no borrow on `self`.
        let ffi_fut = self.inner.process_streaming(RVec::from(req_bytes), sink);
        tokio::pin!(ffi_fut);

        let mut count = 0usize;
        let mut ffi_result: Option<RResult<usize, RString>> = None;
        loop {
            tokio::select! {
                biased;
                maybe_frame = frame_rx.recv() => {
                    match maybe_frame {
                        Some(bytes) => {
                            let out_data: RuntimeData =
                                rmp_serde::from_slice(bytes.as_slice()).map_err(|e| {
                                    Error::Execution(format!(
                                        "rmp decode from plugin '{}' (id={}): {e}",
                                        self.node_type, self.node_id
                                    ))
                                })?;
                            callback(out_data)?;
                            count += 1;
                        }
                        None => {
                            // Sink dropped → FFI future already returned
                            // (or panicked). Drain the FfiFuture once
                            // more so we propagate its terminal status.
                            break;
                        }
                    }
                }
                res = &mut ffi_fut, if ffi_result.is_none() => {
                    ffi_result = Some(res);
                    // Don't break yet — the FFI future may have just
                    // returned with frames still queued in the channel
                    // (the sink's tx was dropped at FFI return; recv
                    // will return None once we drain them).
                }
            }
        }

        // We exited because the channel closed. Make sure the FFI
        // future is collected (it may have completed before the
        // channel drained).
        if ffi_result.is_none() {
            ffi_result = Some(ffi_fut.await);
        }
        match ffi_result.expect("ffi_result set above") {
            RResult::ROk(_ffi_count) => Ok(count),
            RResult::RErr(e) => Err(Error::from_plugin(
                e.into_string(),
                &self.node_id,
                &self.node_type,
            )),
        }
    }

    /// Ctx-aware streaming entry point — same per-frame FFI path,
    /// just derives `session_id` from the runtime context (matches
    /// the trait's default delegation contract).
    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let session_id = if ctx.session_id().is_empty() {
            None
        } else {
            Some(ctx.session_id().to_string())
        };
        self.process_streaming(data, session_id, callback).await
    }
}
