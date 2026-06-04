//! Node.js FFI bindings for RemoteMedia zero-copy IPC
//!
//! This module provides napi-rs bindings for iceoryx2 shared memory IPC,
//! enabling Node.js applications to participate in zero-copy data pipelines
//! with Python and Rust nodes.
//!
//! # Architecture
//!
//! - **node.rs**: iceoryx2 Node wrapper for IPC management
//! - **channel.rs**: Channel creation and configuration
//! - **subscriber.rs**: Zero-copy sample receiving
//! - **publisher.rs**: Zero-copy sample publishing
//! - **sample.rs**: Sample lifecycle (LoanedSample, ReceivedSample)
//! - **session.rs**: Session-scoped channel isolation
//! - **threadsafe.rs**: ThreadsafeFunction helpers for callbacks
//! - **error.rs**: Error types for napi
//! - **runtime_data.rs**: RuntimeData serialization/deserialization
//!
//! # Threading Model
//!
//! iceoryx2 `Publisher`/`Subscriber` are `!Send` and cannot cross thread boundaries.
//! This module uses dedicated OS threads for IPC operations and communicates
//! with the JavaScript main thread via `ThreadsafeFunction` callbacks.
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │  Dedicated OS Thread (iceoryx2 lives here)      │
//! │    ├─ iceoryx2::Subscriber (!Send)              │
//! │    ├─ Polling loop with yield_now()             │
//! │    └─ ThreadsafeFunction.call() when data ready │
//! ├─────────────────────────────────────────────────┤
//! │  JavaScript Main Thread                         │
//! │    └─ Callback receives zero-copy buffer        │
//! └─────────────────────────────────────────────────┘
//! ```

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
