//! Re-export of [`remotemedia_py_env`] for back-compat.
//!
//! The implementation moved out of `remotemedia-core` into its own crate
//! so that loadable-node plugins (dlopen'd cdylibs) can call
//! `ensure_env(&deps)` without pulling all of `remotemedia-core` through
//! the ABI boundary. Existing internal call sites that import
//! `remotemedia_core::python::env_manager::*` keep compiling unchanged.

pub use remotemedia_py_env::*;
