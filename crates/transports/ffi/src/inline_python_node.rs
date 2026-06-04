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
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};
use tokio::sync::Mutex;
use tracing::{debug, error};

/// One inline registration: the class object plus optional metadata.
pub struct InlineNodeConfig {
    pub node_type: String,
    pub cls: Py<PyAny>,
    pub multi_output: bool,
    pub description: Option<String>,
    pub category: Option<String>,
    pub accepts: Vec<String>,
    pub produces: Vec<String>,
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
        }
    }
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
    /// `Send + Sync` and allow lazy init.
    instance: Mutex<Option<Py<PyAny>>>,
}

impl InlinePythonStreamingNode {
    pub fn new(node_id: String, node_type: String, cls: Py<PyAny>, params: Value) -> Self {
        Self {
            node_id,
            node_type,
            cls,
            params,
            instance: Mutex::new(None),
        }
    }

    /// Build the Python instance from the class object and `params`.
    /// Called lazily on first `initialize()`.
    fn instantiate(&self, py: Python<'_>) -> Result<Py<PyAny>, Error> {
        let kwargs = PyDict::new(py);

        // params is a JSON Value — convert to a Python dict of keyword args.
        if let Value::Object(map) = &self.params {
            for (k, v) in map {
                let py_val = super::marshal::json_to_python(py, v).map_err(|e| {
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
        node_id: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<usize, Error>> + Send + '_>>
    {
        Box::pin(async move {
            enum Shape {
                None,
                Sync(RuntimeData),
                Awaitable(Py<PyAny>),
                AsyncGen(Py<PyAny>),
            }

            let shape = Python::attach(|py| -> Result<Shape, Error> {
                let bound = result.bind(py);
                if bound.is_none() {
                    return Ok(Shape::None);
                }
                // Async generator: has both __aiter__ and __anext__.
                let is_async_gen = bound.hasattr("__aiter__").unwrap_or(false)
                    && bound.hasattr("__anext__").unwrap_or(false);
                if is_async_gen {
                    return Ok(Shape::AsyncGen(bound.clone().unbind()));
                }
                // Awaitable: has __await__ (covers coroutines & most awaitables).
                let is_awaitable = bound.hasattr("__await__").unwrap_or(false);
                if is_awaitable {
                    return Ok(Shape::Awaitable(bound.clone().unbind()));
                }
                let rd =
                    super::marshal::python_to_runtime_data(py, bound).map_err(py_err_to_exec)?;
                Ok(Shape::Sync(rd))
            })?;

            match shape {
                Shape::None => Ok(0),
                Shape::Sync(rd) => {
                    callback(rd)?;
                    Ok(1)
                }
                Shape::Awaitable(coro) => {
                    // Bridge the Python coroutine into a Rust future.
                    // We use `into_future_with_locals` because the
                    // surrounding tokio task (a node task spawned by
                    // SessionRouter) does NOT inherit the asyncio-loop
                    // task-locals that the FFI entry registered.
                    // Snapshot them from our global instead.
                    let fut = Python::attach(|py| -> PyResult<_> {
                        let locals = task_locals_snapshot(py).ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                                "Inline async dispatch requires a captured \
                                 asyncio loop, but none was registered. Call \
                                 from inside `await execute_pipeline*(...)`.",
                            )
                        })?;
                        let bound = coro.bind(py).clone();
                        pyo3_async_runtimes::into_future_with_locals(&locals, bound)
                    })
                    .map_err(py_err_to_exec)?;
                    let py_result = fut.await.map_err(py_err_to_exec)?;
                    // The coroutine might itself have returned a
                    // generator / coroutine; recurse.
                    Self::dispatch_result(py_result, callback, node_id).await
                }
                Shape::AsyncGen(gen) => {
                    let mut count = 0usize;
                    loop {
                        // Call __anext__() to get the next awaitable.
                        // StopAsyncIteration can fire either at the
                        // call site (synchronous raise) or when the
                        // awaitable is driven — handle both.
                        let next_fut = Python::attach(|py| -> PyResult<_> {
                            let locals = task_locals_snapshot(py).ok_or_else(|| {
                                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                                    "Inline async-gen dispatch requires a \
                                     captured asyncio loop, but none was \
                                     registered.",
                                )
                            })?;
                            let aw = gen.bind(py).call_method0("__anext__")?;
                            pyo3_async_runtimes::into_future_with_locals(&locals, aw)
                        });
                        let next_fut = match next_fut {
                            Ok(f) => f,
                            Err(e) => {
                                if Python::attach(|py| {
                                    e.is_instance_of::<pyo3::exceptions::PyStopAsyncIteration>(py)
                                }) {
                                    break;
                                }
                                return Err(py_err_to_exec(e));
                            }
                        };
                        let item = match next_fut.await {
                            Ok(v) => v,
                            Err(e) => {
                                if Python::attach(|py| {
                                    e.is_instance_of::<pyo3::exceptions::PyStopAsyncIteration>(py)
                                }) {
                                    break;
                                }
                                return Err(py_err_to_exec(e));
                            }
                        };
                        let maybe_rd =
                            Python::attach(|py| -> Result<Option<RuntimeData>, Error> {
                                let bound = item.bind(py);
                                if bound.is_none() {
                                    return Ok(None);
                                }
                                Ok(Some(
                                    super::marshal::python_to_runtime_data(py, bound)
                                        .map_err(py_err_to_exec)?,
                                ))
                            })?;
                        if let Some(rd) = maybe_rd {
                            callback(rd)?;
                            count += 1;
                        }
                    }
                    let _ = node_id; // silence unused on this branch
                    Ok(count)
                }
            }
        })
    }
}

fn py_err_to_exec(e: PyErr) -> Error {
    Error::Execution(format!("Python error: {}", e))
}

#[async_trait::async_trait]
impl AsyncStreamingNode for InlinePythonStreamingNode {
    fn node_type(&self) -> &str {
        &self.node_type
    }

    async fn initialize(&self, _ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        let mut guard = self.instance.lock().await;
        if guard.is_some() {
            return Ok(());
        }
        let instance = Python::attach(|py| self.instantiate(py))?;
        // If the instance exposes an `initialize()` method, call it.
        Python::attach(|py| -> Result<(), Error> {
            let bound = instance.bind(py);
            if bound.hasattr("initialize").map_err(py_err_to_exec)? {
                if let Err(e) = bound.call_method0("initialize") {
                    // Coroutines returned by sync initialize() in async subclasses:
                    // ignore — we'll resolve them lazily.
                    error!("Inline node '{}' initialize raised: {}", self.node_id, e);
                    return Err(py_err_to_exec(e));
                }
            }
            Ok(())
        })?;
        *guard = Some(instance);
        Ok(())
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        // Collect a single output via the streaming path.
        let mut out: Option<RuntimeData> = None;
        let mut cb = |rd: RuntimeData| -> Result<(), Error> {
            if out.is_none() {
                out = Some(rd);
            }
            Ok(())
        };
        let n = self.process_streaming_inner(data, &mut cb).await?;
        if n == 0 {
            return Err(Error::Execution(format!(
                "Inline node '{}' produced no output",
                self.node_id
            )));
        }
        Ok(out.unwrap())
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
        self.process_streaming_inner(data, &mut callback).await
    }
}

impl InlinePythonStreamingNode {
    async fn process_streaming_inner(
        &self,
        data: RuntimeData,
        callback: &mut (dyn FnMut(RuntimeData) -> Result<(), Error> + Send),
    ) -> Result<usize, Error> {
        // Lazy-instantiate if initialize() wasn't called.
        let instance = {
            let mut guard = self.instance.lock().await;
            if guard.is_none() {
                let i = Python::attach(|py| self.instantiate(py))?;
                *guard = Some(i);
            }
            Python::attach(|py| guard.as_ref().unwrap().clone_ref(py))
        };

        // Call instance.process(data) under GIL.
        let result = Python::attach(|py| -> Result<Py<PyAny>, Error> {
            let py_data =
                super::marshal::runtime_data_to_python(py, &data).map_err(py_err_to_exec)?;
            let bound = instance.bind(py);
            let py_result = bound
                .call_method1("process", (py_data,))
                .map_err(py_err_to_exec)?;
            Ok(py_result.unbind())
        })?;

        Self::dispatch_result(result, callback, self.node_id.clone()).await
    }
}

/// Factory that builds [`InlinePythonStreamingNode`] from a registered class.
struct InlinePythonNodeFactory {
    config: InlineNodeConfig,
}

impl StreamingNodeFactory for InlinePythonNodeFactory {
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
