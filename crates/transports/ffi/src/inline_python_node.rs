//! In-process Python streaming node.
//!
//! Supports custom Python `Node` subclasses defined inline (REPL, agent-
//! generated scripts, `__main__` modules) by holding a `Py<PyAny>` class
//! reference and instantiating + dispatching in-process via PyO3.
//!
//! Contrast with [`crate::python::...PythonStreamingNode`] which spawns
//! a subprocess and uses `importlib.import_module(dotted_path)` — the
//! subprocess can't see classes that aren't importable from a module on
//! disk. Inline nodes share the calling Python interpreter, so any class
//! object the user can reference can be registered.
//!
//! **GIL trade-off:** every call holds the GIL. Inline nodes serialise
//! against each other and against any other PyO3 callbacks on the
//! tokio runtime. Use for orchestration / light glue; route heavy
//! compute through the multiprocess path.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use remotemedia_core::data::RuntimeData;
use remotemedia_core::nodes::provider::NodeProvider;
use remotemedia_core::nodes::schema::{NodeSchema, RuntimeDataType};
use remotemedia_core::nodes::streaming_node::{
    AsyncNodeWrapper, AsyncStreamingNode, StreamingNode, StreamingNodeFactory,
    StreamingNodeRegistry,
};
use remotemedia_core::nodes::InitializeContextRead;
use remotemedia_core::Error;
use remotemedia_traits::buffer_contract::{
    BufferContract, BufferProduction, BufferProtocol, BUFFER_CONTRACT_VERSION,
    INPROCESS_PYTHON_BUFFER_VERSION,
};
use remotemedia_traits::runtime_context::NodeRuntimeContextRead;
use remotemedia_types::CopyContext;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, OnceLock, RwLock};
use tokio::sync::Mutex;
use tracing::debug;

/// One inline registration: the class object plus optional metadata.
pub struct InlineNodeConfig {
    pub node_type: String,
    pub cls: Py<PyAny>,
    pub multi_output: bool,
    pub description: Option<String>,
    pub category: Option<String>,
    pub accepts: Vec<String>,
    pub produces: Vec<String>,
    pub buffer_contract: Option<BufferContract>,
}

impl InlineNodeConfig {
    /// Deep-clone the config (requires GIL for `Py<PyAny>::clone_ref`).
    fn clone_with_gil(&self, py: Python<'_>) -> Self {
        Self {
            node_type: self.node_type.clone(),
            cls: self.cls.clone_ref(py),
            multi_output: self.multi_output,
            description: self.description.clone(),
            category: self.category.clone(),
            accepts: self.accepts.clone(),
            produces: self.produces.clone(),
            buffer_contract: self.buffer_contract.clone(),
        }
    }
}

/// A declaration is a node-author promise, not proof about arbitrary Python code.
/// Inline dispatch implements default ports and version 1 owning Python buffers.
pub fn parse_buffer_contract(cls: &Bound<'_, PyAny>, json: &str) -> PyResult<BufferContract> {
    use pyo3::exceptions::PyValueError;
    let contract: BufferContract = serde_json::from_str(json)
        .map_err(|e| PyValueError::new_err(format!("Invalid buffer contract: {e}")))?;
    if contract.version != BUFFER_CONTRACT_VERSION
        || contract.protocol
            != (BufferProtocol::InProcessPython {
                buffers: INPROCESS_PYTHON_BUFFER_VERSION,
            })
    {
        return Err(PyValueError::new_err("Inline nodes require buffer contract version 1 and in_process_python buffers version 1"));
    }
    for (name, port) in contract.inputs.iter().chain(&contract.outputs) {
        if !name.is_empty() {
            return Err(PyValueError::new_err(
                "Inline buffer contracts support only the default port",
            ));
        }
        port.validate_definition().map_err(PyValueError::new_err)?;
    }
    if matches!(
        contract.production,
        BufferProduction::OutputAllocator | BufferProduction::ForwardOrOutputAllocator
    ) && !cls
        .getattr("process_shared")
        .is_ok_and(|method| method.is_callable())
    {
        return Err(PyValueError::new_err(
            "Allocator production requires process_shared(data, ctx)",
        ));
    }
    Ok(contract)
}

/// Process-global registry of inline-registered Python node classes.
///
/// Keyed by `node_type` string. Populated by the PyO3 binding
/// `register_inline_node_class`; consumed on every
/// `PipelineExecutor::new()` by [`InlinePythonNodesProvider`].
static INLINE_REGISTRY: LazyLock<RwLock<HashMap<String, InlineNodeConfig>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Captured `TaskLocals` (asyncio loop + context) used by inline async
/// dispatch. Set by [`set_task_locals`] from the FFI entry points so
/// that nested `pyo3_async_runtimes::tokio::into_future_with_locals`
/// calls (driven from session-router tokio tasks that don't inherit
/// the outer task-locals) can find the user's running asyncio loop.
static INLINE_TASK_LOCALS: std::sync::OnceLock<
    std::sync::Mutex<Option<pyo3_async_runtimes::TaskLocals>>,
> = std::sync::OnceLock::new();

/// Store `TaskLocals` for inline async dispatch. Called from the FFI
/// entry points (`execute_pipeline*`) under the GIL with the user's
/// running asyncio loop available. Idempotent (always overwrites — the
/// most recent FFI call's loop is used by subsequent inline dispatch).
pub fn set_task_locals(locals: pyo3_async_runtimes::TaskLocals) {
    let slot = INLINE_TASK_LOCALS.get_or_init(|| std::sync::Mutex::new(None));
    *slot.lock().expect("INLINE_TASK_LOCALS poisoned") = Some(locals);
}

/// Snapshot the currently-stored task locals, if any. Used by
/// [`InlinePythonStreamingNode`] when dispatching coroutines /
/// async generators.
fn task_locals_snapshot(py: Python<'_>) -> Option<pyo3_async_runtimes::TaskLocals> {
    let slot = INLINE_TASK_LOCALS.get()?;
    slot.lock()
        .ok()
        .and_then(|g| g.as_ref().map(|l| l.clone_ref(py)))
}

/// Insert or replace an inline registration.
pub fn register_inline_node(config: InlineNodeConfig) {
    let mut map = INLINE_REGISTRY.write().expect("INLINE_REGISTRY poisoned");
    debug!(
        node_type = %config.node_type,
        "Registering inline Python node class"
    );
    map.insert(config.node_type.clone(), config);
}

/// Remove an inline registration. Returns true if the entry existed.
pub fn unregister_inline_node(node_type: &str) -> bool {
    let mut map = INLINE_REGISTRY.write().expect("INLINE_REGISTRY poisoned");
    map.remove(node_type).is_some()
}

/// Snapshot all currently registered inline configs (deep-clones the
/// stored `Py<PyAny>` references under the GIL).
pub fn list_inline_nodes() -> Vec<InlineNodeConfig> {
    let map = INLINE_REGISTRY.read().expect("INLINE_REGISTRY poisoned");
    Python::attach(|py| map.values().map(|c| c.clone_with_gil(py)).collect())
}

/// An `AsyncStreamingNode` that delegates to a Python class instance held
/// in this process.
pub struct InlinePythonStreamingNode {
    node_id: String,
    node_type: String,
    cls: Py<PyAny>,
    params: Value,
    /// Constructed once on `initialize`. `Mutex<Option<>>` to satisfy
    /// `Send + Sync` and serialize initialization.
    instance: Mutex<Option<Py<PyAny>>>,
    copies: OnceLock<CopyContext>,
}

impl InlinePythonStreamingNode {
    pub fn new(node_id: String, node_type: String, cls: Py<PyAny>, params: Value) -> Self {
        Self {
            node_id,
            node_type,
            cls,
            params,
            instance: Mutex::new(None),
            copies: OnceLock::new(),
        }
    }

    /// Build the Python instance from the class object and `params`.
    /// Called lazily on first `initialize()`.
    fn instantiate(&self, py: Python<'_>, copies: &CopyContext) -> Result<Py<PyAny>, Error> {
        let kwargs = PyDict::new(py);

        // params is a JSON Value — convert to a Python dict of keyword args.
        if let Value::Object(map) = &self.params {
            for (k, v) in map {
                let py_val = super::marshal::json_to_python_with_context(py, v, None, copies)
                    .map_err(|e| {
                        Error::Execution(format!(
                            "Inline node '{}' param '{}' conversion failed: {}",
                            self.node_id, k, e
                        ))
                    })?;
                kwargs.set_item(k, py_val).map_err(py_err_to_exec)?;
            }
        }

        // Always pass `name` so Node.__init__ has a sensible default.
        if kwargs.contains("name").map_err(py_err_to_exec)? == false {
            kwargs
                .set_item("name", &self.node_id)
                .map_err(py_err_to_exec)?;
        }

        let args = PyTuple::empty(py);
        let instance = self.cls.bind(py).call(args, Some(&kwargs)).map_err(|e| {
            Error::Execution(format!(
                "Inline node '{}' constructor raised: {}",
                self.node_id, e
            ))
        })?;

        Ok(instance.unbind())
    }

    /// Drain a single Python call result into a callback. Handles:
    /// - `None`             → no output
    /// - sync return value  → 1 output
    /// - coroutine          → bridged via `pyo3-async-runtimes::tokio::into_future`,
    ///                        result recursively dispatched
    /// - async generator    → iterated via `__anext__` until `StopAsyncIteration`,
    ///                        callback per yielded value
    ///
    /// All async paths rely on pyo3-async-runtimes' bridge to the
    /// asyncio loop that was active when the surrounding
    /// `future_into_py(...)` call set up the runtime context. As long
    /// as the user is awaiting our coroutine from an `asyncio.run()`,
    /// the bridge has a loop to schedule on.
    fn dispatch_result(
        result: Py<PyAny>,
        callback: &mut (dyn FnMut(RuntimeData) -> Result<(), Error> + Send),
        copies: CopyContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<usize, Error>> + Send + '_>>
    {
        Box::pin(async move {
            let (is_generator, is_awaitable) = Python::attach(|py| -> PyResult<_> {
                let bound = result.bind(py);
                Ok((
                    bound.hasattr("__aiter__")? && bound.hasattr("__anext__")?,
                    bound.hasattr("__await__")?,
                ))
            })
            .map_err(py_err_to_exec)?;
            if is_generator {
                return Self::drain_generator(result, callback, copies).await;
            }
            if is_awaitable {
                let value = await_python(result).await.map_err(py_err_to_exec)?;
                return Self::dispatch_result(value, callback, copies).await;
            }
            Self::emit_result(result, callback, &copies)
        })
    }

    fn emit_result(
        result: Py<PyAny>,
        callback: &mut (dyn FnMut(RuntimeData) -> Result<(), Error> + Send),
        copies: &CopyContext,
    ) -> Result<usize, Error> {
        let value = Python::attach(|py| -> PyResult<_> {
            if result.bind(py).is_none() {
                return Ok(None);
            }
            super::marshal::python_to_runtime_data_with_context(py, result.bind(py), copies)
                .map(Some)
        })
        .map_err(py_err_to_exec)?;
        match value {
            Some(value) => {
                callback(value)?;
                Ok(1)
            }
            None => Ok(0),
        }
    }

    async fn drain_generator(
        generator: Py<PyAny>,
        callback: &mut (dyn FnMut(RuntimeData) -> Result<(), Error> + Send),
        copies: CopyContext,
    ) -> Result<usize, Error> {
        let result = async {
            let mut count = 0;
            loop {
                let next = Python::attach(|py| {
                    generator
                        .bind(py)
                        .call_method0("__anext__")
                        .map(Bound::unbind)
                });
                let next = match next {
                    Ok(next) => await_python(next).await,
                    Err(error) => Err(error),
                };
                match next {
                    Ok(value) => count += Self::emit_result(value, callback, &copies)?,
                    Err(error)
                        if Python::attach(|py| {
                            error.is_instance_of::<pyo3::exceptions::PyStopAsyncIteration>(py)
                        }) =>
                    {
                        break
                    }
                    Err(error) => return Err(py_err_to_exec(error)),
                }
            }
            Ok(count)
        }
        .await;
        // Run finally blocks even when conversion or downstream delivery fails.
        // Preserve the original operation error if closing also fails.
        let closed: Result<(), Error> = async {
            let close = Python::attach(|py| -> PyResult<_> {
                let bound = generator.bind(py);
                if bound.hasattr("aclose")? {
                    Ok(Some(bound.call_method0("aclose")?.unbind()))
                } else {
                    Ok(None)
                }
            })
            .map_err(py_err_to_exec)?;
            if let Some(close) = close {
                await_python(close).await.map_err(py_err_to_exec)?;
            }
            Ok(())
        }
        .await;
        let count = result?;
        closed?;
        Ok(count)
    }

    fn bound_copies(&self) -> Result<CopyContext, Error> {
        self.copies.get().cloned().ok_or_else(|| {
            Error::Execution(
                "Inline Python node requires initialization with node copy authority".into(),
            )
        })
    }

    async fn process_one(
        &self,
        data: RuntimeData,
        ctx: Option<&dyn NodeRuntimeContextRead>,
    ) -> Result<RuntimeData, Error> {
        let mut output = None;
        self.process_streaming_inner(data, ctx, &mut |value| {
            if output.is_none() {
                output = Some(value);
            }
            Ok(())
        })
        .await?;
        output.ok_or_else(|| {
            Error::Execution(format!("Inline node '{}' produced no output", self.node_id))
        })
    }

    async fn process_streaming_inner(
        &self,
        data: RuntimeData,
        ctx: Option<&dyn NodeRuntimeContextRead>,
        callback: &mut (dyn FnMut(RuntimeData) -> Result<(), Error> + Send),
    ) -> Result<usize, Error> {
        let copies = match ctx {
            Some(ctx) => ctx.copy_context().ok_or_else(|| {
                Error::Execution("Inline Python call requires node copy authority".into())
            })?,
            None => self.bound_copies()?,
        };
        let instance = {
            let guard = self.instance.lock().await;
            let instance = guard
                .as_ref()
                .ok_or_else(|| Error::Execution("Inline Python node was not initialized".into()))?;
            Python::attach(|py| instance.clone_ref(py))
        };
        let result = Python::attach(|py| -> Result<Py<PyAny>, Error> {
            let py_data = super::marshal::runtime_data_to_python_with_context(
                py,
                &data.into_shared(),
                &copies,
            )
            .map_err(py_err_to_exec)?;
            let bound = instance.bind(py);
            let output = if bound.hasattr("process_shared").map_err(py_err_to_exec)? {
                let ctx = ctx.ok_or_else(|| {
                    Error::Execution("process_shared requires a runtime allocator context".into())
                })?;
                let allocator = ctx.output_allocator().ok_or_else(|| {
                    Error::Execution("process_shared requires the node output allocator".into())
                })?;
                let allocator = _remotemedia_buffers::OutputAllocator::new(allocator)
                    .with_identity(ctx.session_id(), ctx.node_id());
                let allocator = Py::new(py, allocator).map_err(py_err_to_exec)?;
                bound.call_method1("process_shared", (py_data, allocator))
            } else {
                bound.call_method1("process", (py_data,))
            }
            .map_err(py_err_to_exec)?;
            Ok(output.unbind())
        })?;
        Self::dispatch_result(result, callback, copies).await
    }
}

async fn await_python(value: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let future = Python::attach(|py| {
        let locals = task_locals_snapshot(py).ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "Inline async dispatch requires a captured asyncio loop",
            )
        })?;
        pyo3_async_runtimes::into_future_with_locals(&locals, value.into_bound(py))
    })?;
    future.await
}

fn py_err_to_exec(e: PyErr) -> Error {
    Error::Execution(format!("Python error: {}", e))
}

#[async_trait::async_trait]
impl AsyncStreamingNode for InlinePythonStreamingNode {
    fn node_type(&self) -> &str {
        &self.node_type
    }

    async fn initialize(&self, ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        let mut guard = self.instance.lock().await;
        if guard.is_some() {
            return Ok(());
        }
        let copies = ctx.copy_context().ok_or_else(|| {
            Error::Execution("Inline initialization requires node copy authority".into())
        })?;
        let instance = Python::attach(|py| self.instantiate(py, &copies))?;
        let initialized = Python::attach(|py| -> PyResult<_> {
            let bound = instance.bind(py);
            if !bound.hasattr("initialize")? {
                return Ok(None);
            }
            let result = bound.call_method0("initialize")?;
            Ok(result.hasattr("__await__")?.then(|| result.unbind()))
        })
        .map_err(py_err_to_exec)?;
        if let Some(initialized) = initialized {
            await_python(initialized).await.map_err(py_err_to_exec)?;
        }
        self.copies
            .set(copies)
            .map_err(|_| Error::Execution("Inline copy authority already bound".into()))?;
        *guard = Some(instance);
        Ok(())
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        self.process_one(data, None).await
    }

    async fn process_with_ctx(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        self.process_one(data, Some(ctx)).await
    }

    async fn process_multi_with_ctx(
        &self,
        inputs: HashMap<String, RuntimeData>,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        if inputs.len() != 1 {
            return Err(Error::Execution(
                "Inline Python nodes require exactly one input".into(),
            ));
        }
        self.process_one(inputs.into_values().next().unwrap(), Some(ctx))
            .await
    }

    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        _session_id: Option<String>,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        self.process_streaming_inner(data, None, &mut callback)
            .await
    }

    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        self.process_streaming_inner(data, Some(ctx), &mut callback)
            .await
    }
}

/// Factory that builds [`InlinePythonStreamingNode`] from a registered class.
struct InlinePythonNodeFactory {
    config: InlineNodeConfig,
}

impl StreamingNodeFactory for InlinePythonNodeFactory {
    fn buffer_contract(&self, _params: &Value) -> Option<BufferContract> {
        self.config.buffer_contract.clone()
    }

    fn supports_buffer_protocol(&self, protocol: &BufferProtocol) -> bool {
        matches!(
            protocol,
            BufferProtocol::InProcessPython {
                buffers: INPROCESS_PYTHON_BUFFER_VERSION
            }
        )
    }

    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let cls = Python::attach(|py| self.config.cls.clone_ref(py));
        let node = InlinePythonStreamingNode::new(
            node_id,
            self.config.node_type.clone(),
            cls,
            params.clone(),
        );
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        &self.config.node_type
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        self.config.multi_output
    }

    fn schema(&self) -> Option<NodeSchema> {
        let mut schema = NodeSchema::new(&self.config.node_type);
        if let Some(d) = &self.config.description {
            schema = schema.description(d);
        }
        if let Some(c) = &self.config.category {
            schema = schema.category(c);
        }
        let accepts = string_types_to_runtime(&self.config.accepts);
        let produces = string_types_to_runtime(&self.config.produces);
        if !accepts.is_empty() {
            schema = schema.accepts(accepts);
        }
        if !produces.is_empty() {
            schema = schema.produces(produces);
        }
        Some(schema)
    }
}

fn string_types_to_runtime(types: &[String]) -> Vec<RuntimeDataType> {
    types
        .iter()
        .filter_map(|t| match t.as_str() {
            "audio" => Some(RuntimeDataType::Audio),
            "text" => Some(RuntimeDataType::Text),
            "json" => Some(RuntimeDataType::Json),
            "video" => Some(RuntimeDataType::Video),
            "binary" | "bytes" => Some(RuntimeDataType::Binary),
            "tensor" => Some(RuntimeDataType::Tensor),
            "numpy" => Some(RuntimeDataType::Numpy),
            "control_message" => Some(RuntimeDataType::ControlMessage),
            _ => None,
        })
        .collect()
}

/// Provider that registers an `InlinePythonNodeFactory` for every entry in
/// [`INLINE_REGISTRY`]. Registered via `inventory::submit!` so it runs on
/// every `PipelineExecutor::new()`. Late additions to the registry are
/// picked up on the next executor construction.
pub struct InlinePythonNodesProvider;

impl NodeProvider for InlinePythonNodesProvider {
    fn register(&self, registry: &mut StreamingNodeRegistry) {
        for config in list_inline_nodes() {
            let node_type = config.node_type.clone();
            registry.register(Arc::new(InlinePythonNodeFactory { config }));
            debug!(node_type = %node_type, "Registered inline Python node factory");
        }
    }

    fn provider_name(&self) -> &'static str {
        "inline-python-nodes"
    }

    fn node_count(&self) -> usize {
        INLINE_REGISTRY.read().map(|m| m.len()).unwrap_or(0)
    }

    fn priority(&self) -> i32 {
        // Between built-in Python (500) and user-defined static (100).
        // Higher than 500 so an inline registration can OVERRIDE a built-in
        // name during dev/testing.
        600
    }
}

inventory::submit! {
    &InlinePythonNodesProvider as &'static dyn NodeProvider
}
