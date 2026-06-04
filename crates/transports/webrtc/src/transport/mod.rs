//! Main WebRTC transport implementation
//!
//! Implements the PipelineTransport trait for RemoteMedia integration.

#[allow(clippy::module_inception)]
pub mod transport;

pub use transport::WebRtcTransport;
