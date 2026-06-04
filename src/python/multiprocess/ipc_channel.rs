//! Re-export shim — the iceoryx2 channel registry moved to
//! `crate::multiprocess::ipc_channel` because the IPC layer is
//! language-neutral. Kept here so existing call sites that import
//! `crate::python::multiprocess::ipc_channel::*` keep compiling.
//!
//! New code should import from `crate::multiprocess::ipc_channel`.

pub use crate::multiprocess::ipc_channel::*;
