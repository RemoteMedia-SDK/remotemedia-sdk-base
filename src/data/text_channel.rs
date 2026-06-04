//! Routing-channel helpers for `RuntimeData::Text`.
//!
//! Re-exports the wire-shape helpers from `remotemedia-types` so
//! existing `use remotemedia_core::data::{split_text_str, …}` call
//! sites keep compiling. See `remotemedia_types::text_channel`-
//! adjacent items for the canonical definitions.
//!
//! Byte-level equivalents live in `crate::multiprocess::data_transfer`
//! when the `multiprocess` feature is enabled; both agree on the
//! wire format.

pub use remotemedia_types::{split_text_str, tag_text_str, TEXT_CHANNEL_DEFAULT};
