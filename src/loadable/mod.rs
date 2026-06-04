//! In-process loadable native plugins.
//!
//! Loads `.so` / `.dll` / `.dylib` files that export the
//! `loadable_node_abi::NodePlugin` root module, adapts them into
//! `StreamingNodeFactory` via `factory::LoadableNodeBundle`.
//!
//! Plugins run in the host's address space — no spawn, no IPC. The
//! FFI surface is `abi_stable` + `async_ffi` (so the boundary
//! survives rustc/feature drift), and the payload bytes are the
//! same `crate::multiprocess::data_transfer::RuntimeData` binary
//! format used by the multiprocess path.
//!
//! That gives a clean trade-off matrix at the registry level:
//!
//! | Path                 | Address space | Wire format              |
//! |----------------------|---------------|--------------------------|
//! | `loadable::factory`  | in-process    | `RuntimeData::to_bytes`  |
//! | `multiprocess::factory` | own process | `RuntimeData::to_bytes`  |
//! | `python::multiprocess` | own process | `RuntimeData::to_bytes`  |
//!
//! Same wire format means a `RuntimeData` payload is transcribed
//! once on the producer side and decoded once on the consumer side
//! regardless of the runtime host the plugin happens to be using.

pub mod factory;
pub mod plugin_toml;
pub mod resolver;

#[cfg(feature = "python-source-plugin")]
pub mod source_python;
