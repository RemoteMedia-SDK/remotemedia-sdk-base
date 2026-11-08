//! Multiprocess and in-process loadable execution: shared wire format
//! plus runtime-specific adapters.
//!
//! `data_transfer` defines the copying binary `RuntimeData` compatibility
//! format. It has no iceoryx2 dependency and is always compiled. The mapped
//! `native_worker` runner exchanges metadata and retained buffer references
//! instead; shared loadable hooks also bypass this compatibility codec.
//!
//! The iceoryx2-using pieces (`ipc_channel`, `factory`, `runner`)
//! are gated behind the `multiprocess` feature so a build without
//! iceoryx2 still has access to the wire format helpers.
//!
//! Per-language adapters live elsewhere:
//! - `crate::python::multiprocess` — Python child-process executor,
//!   docker support, env management. Re-exports `data_transfer` and
//!   `ipc_channel` from here for backwards compatibility.
//! - `crate::multiprocess::runner` — library helpers for native Rust
//!   plugins using the legacy copying multiprocess protocol.
//! - `crate::multiprocess::native_worker` — typed mapped runner for the
//!   existing node traits, behind the independent `native-worker` feature.
//! - `crate::multiprocess::factory` — `StreamingNodeFactory` that
//!   spawns a native binary plugin and bridges via iceoryx2.
//! - `crate::loadable::factory` — `StreamingNodeFactory` that loads
//!   an in-process `.so/.dll/.dylib` plugin and bridges via abi_stable
//!   (uses the same `data_transfer` codec).
//!
//! # Channel naming (multiprocess)
//!
//! `{session_id}_{node_id}_input`  — host → plugin
//! `{session_id}_{node_id}_output` — plugin → host
//!
//! Both sides may call `service_builder(...).open_or_create()` —
//! whichever attaches first creates the service.

pub mod data_transfer;
#[cfg(feature = "multiprocess")]
pub(crate) mod control_frames;
#[cfg(feature = "multiprocess")]
mod native_copy_reporting;
mod python_wire;

/// Typed mapped runner for existing node traits and direct output allocation.
/// The legacy `runner::Plugin` remains a copying compatibility protocol.
#[cfg(all(feature = "native-worker", any(windows, target_os = "linux")))]
pub use remotemedia_plugin_sdk::native_worker;

/// Managed Python factories using the same mapped pool and owning views.
#[cfg(all(feature = "mapped-python", any(windows, target_os = "linux")))]
pub use remotemedia_plugin_sdk::mapped_python;

#[cfg(feature = "multiprocess")]
pub mod factory;
#[cfg(feature = "multiprocess")]
pub mod ipc_channel;
#[cfg(feature = "multiprocess")]
pub mod runner;
