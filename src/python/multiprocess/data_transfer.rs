//! Re-export shim — the actual definitions moved to
//! `crate::multiprocess::data_transfer` because the wire format is
//! language-neutral. Kept here so existing call sites that import
//! `crate::python::multiprocess::data_transfer::*` keep compiling.
//!
//! New code should import from `crate::multiprocess::data_transfer`.

pub use crate::multiprocess::data_transfer::*;
