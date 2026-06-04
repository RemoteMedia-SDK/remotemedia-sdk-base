//! Loadable-plugin discovery hook for the Python FFI runtime.
//!
//! Without this module, a manifest referencing a node type that lives in a
//! third-party `.so`/`.dll`/`.dylib` fails at session creation because
//! `PipelineExecutor::new()` only knows about the built-in nodes baked into
//! `remotemedia-core`. With it, the Python caller explicitly loads a plugin
//! via [`load_plugin`] before [`crate::streaming_session::create_streaming_session`],
//! and the plugin's factories become resolvable in subsequent manifests.
//!
//! ## Usage
//!
//! ```python
//! import remotemedia.runtime as rt
//!
//! # Explicit, project-local load. The .so can live anywhere on disk —
//! # next to the manifest, in ./plugins/, wherever. No global config dir
//! # is required.
//! exposed = rt.load_plugin("./plugins/libecho_python_loadable_plugin.so")
//! print(exposed)  # ["EchoPythonNode"]
//!
//! # Now the manifest can reference EchoPythonNode by node_type as if
//! # it were built into the runtime.
//! sess = await rt.create_streaming_session(manifest_json)
//! ```
//!
//! ## Lifetime
//!
//! Bundles are stashed in a process-global list (`LOADED_BUNDLES`) so the
//! underlying `dlopen`'d library stays mapped for the lifetime of the
//! process. Dropping a bundle would release the `Arc<dyn StreamingNodeFactory>`
//! references but leave the library mapped (`abi_stable` keeps it pinned),
//! so the leak is bounded. We never unload — there's no `unload_plugin`
//! API today, by design: removing a node type while sessions hold factory
//! references would be a use-after-free trap.

use std::path::PathBuf;
use std::sync::Mutex;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use remotemedia_core::loadable::factory::LoadableNodeBundle;
use remotemedia_core::nodes::StreamingNodeFactory;
use std::sync::Arc;

/// Holds every plugin loaded via [`load_plugin`]. Each entry is `(path,
/// bundle)`. `create_streaming_session` walks this list and registers
/// every factory into the freshly-built executor's registry before any
/// session is created.
///
/// Mutex-wrapped because Python can call `load_plugin` from any thread
/// and the list is read once per `create_streaming_session` call —
/// contention is negligible.
static LOADED_BUNDLES: Mutex<Vec<(PathBuf, LoadableNodeBundle)>> = Mutex::new(Vec::new());

/// Load a `.so` / `.dll` / `.dylib` plugin and register its node types
/// for use in subsequent [`crate::streaming_session::create_streaming_session`]
/// calls.
///
/// Returns the list of node types the plugin exposed.
///
/// Idempotent on identical paths: calling twice with the same path is a
/// no-op (returns the already-known node-type list) so notebooks / tests
/// can re-run the cell without leaking factories.
///
/// # Errors
///
/// - `RuntimeError` if the file can't be loaded (missing / not a cdylib /
///   ABI-mismatched / corrupt).
#[pyfunction]
pub fn load_plugin(path: String) -> PyResult<Vec<String>> {
    let path_buf = PathBuf::from(&path);

    // Idempotency: if we've already loaded this exact path, return the
    // cached factory list. Reload-on-overwrite would require unloading
    // (which we don't support — see module doc comment).
    {
        let bundles = LOADED_BUNDLES.lock().expect("LOADED_BUNDLES mutex");
        if let Some((_, existing)) = bundles.iter().find(|(p, _)| p == &path_buf) {
            return Ok(existing
                .factories()
                .iter()
                .map(|f| f.node_type().to_string())
                .collect());
        }
    }

    let bundle = LoadableNodeBundle::load(&path_buf)
        .map_err(|e| PyRuntimeError::new_err(format!("load_plugin {path}: {e}")))?;
    let exposed: Vec<String> = bundle
        .factories()
        .iter()
        .map(|f| f.node_type().to_string())
        .collect();

    LOADED_BUNDLES
        .lock()
        .expect("LOADED_BUNDLES mutex")
        .push((path_buf, bundle));

    Ok(exposed)
}

/// Returns the list of `(plugin_path, exposed_node_types)` pairs for
/// every plugin loaded via [`load_plugin`] so far this process.
///
/// Useful for debugging "why doesn't the manifest find my node type" —
/// the answer is almost always "you forgot to call `load_plugin` first".
#[pyfunction]
pub fn list_loaded_plugins() -> Vec<(String, Vec<String>)> {
    LOADED_BUNDLES
        .lock()
        .expect("LOADED_BUNDLES mutex")
        .iter()
        .map(|(p, b)| {
            (
                p.display().to_string(),
                b.factories()
                    .iter()
                    .map(|f| f.node_type().to_string())
                    .collect(),
            )
        })
        .collect()
}

/// Collect `Arc<dyn StreamingNodeFactory>` clones for every plugin loaded
/// so far. Called by `create_streaming_session` immediately after building
/// the `PipelineExecutor` so manifest validation sees the plugin's node
/// types as if they were built in.
///
/// Cloning the `Arc` is cheap; the bundle itself stays in `LOADED_BUNDLES`
/// keeping the `.so` mapped.
pub(crate) fn collect_registered_factories() -> Vec<Arc<dyn StreamingNodeFactory>> {
    LOADED_BUNDLES
        .lock()
        .expect("LOADED_BUNDLES mutex")
        .iter()
        .flat_map(|(_, b)| b.factories().iter().cloned())
        .collect()
}
