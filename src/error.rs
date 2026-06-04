//! Error types for RemoteMedia Runtime Core
//!
//! Re-exported from `remotemedia-types` (the wire-format / trait-surface
//! crate) so plugin code and host code share the same `Error` enum.
//!
//! Heavy upstream conversions (`anyhow::Error`, optional `ort::Error`)
//! used to live here as `From` impls, but with `Error` itself relocated
//! to `remotemedia-types` the orphan rule prevents host-side `From`
//! impls on the upstream pair. Neither conversion has live callers in
//! tree (verified at the time of the move), so the impls are dropped.
//! If a future caller needs them, expose `Error::from_anyhow` /
//! `Error::from_ort` helpers in this module.

pub use remotemedia_types::{Error, Result};
