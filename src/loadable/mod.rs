//! Plugin resolution and factory registration.
//!
//! `factory::LoadableNodeBundle` loads native DLL/SO/dylib plugins into the
//! host process using the versioned ABI, including retained bulk buffer owners.
//! Explicit mapped Python and native package declarations select subprocess
//! factories with descriptor transport and per-session mapped pools. They require
//! separate host authorization; plugin metadata alone never grants mapping trust.
//!
//! Legacy source Python and multiprocess adapters retain their serialized data
//! paths. Selecting a mapped declaration never silently falls back to those paths.

pub mod factory;
pub mod plugin_toml;
pub mod resolver;

#[cfg(all(feature = "native-worker", any(windows, target_os = "linux")))]
pub mod mapped_native;

#[cfg(all(feature = "mapped-python", any(windows, target_os = "linux")))]
pub mod mapped_python;

#[cfg(feature = "python-source-plugin")]
pub mod source_python;
