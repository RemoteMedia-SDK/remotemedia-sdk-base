//! Node.js owning pipeline handles and native shared-memory IPC.
//!
//! Publishers keep exclusive native loans on a dedicated thread. Receivers use
//! the thread-safe IPC service and retain immutable samples through SharedBytes.
//! Pull, asynchronous and callback delivery share one bounded native borrow budget.
//! Callback wakeups carry no payload pointers; abort and environment teardown
//! reclaim the separate owning queue before joining its polling thread.
//!
//! Mutable JS Buffer imports/exports and portable MessagePack codecs are explicit
//! copying compatibility operations under the immutable IPC-session CopyContext.
//! Owning pipeline handoff and retained slices do not materialize payloads.
//! Legacy public threadsafe helpers remain separate from the native receive path.

pub mod buffer_config;
mod buffers;
mod output_loans;
pub mod channel;
pub mod control_session;
pub mod error;
pub mod node;
pub mod pipeline;
pub mod publisher;
pub mod runtime_data;
pub mod sample;
pub mod schema;
pub mod session;
pub mod streaming_session;
pub mod subscriber;
pub mod threadsafe;
mod unary;

// WebRTC bindings (only compiled with `napi-webrtc` feature)
#[cfg(feature = "napi-webrtc")]
pub mod webrtc;

// Re-export main types for convenient access
pub use channel::*;
pub use control_session::*;
pub use error::*;
pub use node::*;
pub use pipeline::*;
pub use publisher::*;
pub use sample::*;
pub use schema::*;
pub use session::*;
pub use streaming_session::*;
pub use subscriber::*;
