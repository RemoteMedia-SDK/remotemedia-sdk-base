//! Shared WebRTC infrastructure for FFI bindings
//!
//! This module provides the core WebRTC types and infrastructure that are
//! shared between Node.js (napi) and Python (PyO3) bindings. It wraps the
//! `remotemedia-webrtc` crate's signaling service and peer management.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │  FFI Layer (this module)                                    │
//! │    ├─ config.rs    - WebRtcServerConfig (language-agnostic) │
//! │    ├─ events.rs    - Event types for callbacks              │
//! │    ├─ error.rs     - WebRtcError with error codes           │
//! │    └─ core.rs      - WebRtcServerCore (wraps signaling)     │
//! ├─────────────────────────────────────────────────────────────┤
//! │  napi/webrtc/      - Node.js ThreadsafeFunction bindings    │
//! │  python/webrtc/    - Python Py<PyAny> callback bindings     │
//! ├─────────────────────────────────────────────────────────────┤
//! │  remotemedia-webrtc crate                                   │
//! │    ├─ WebRtcSignalingService (gRPC server)                  │
//! │    ├─ ServerPeer (per-client WebRTC connection)             │
//! │    └─ PeerConnection (WebRTC internals)                     │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Event Flow
//!
//! Events from the WebRTC layer flow through mpsc channels to FFI callbacks:
//!
//! 1. `WebRtcSignalingService` receives client connection
//! 2. `ServerPeer` is created for the client
//! 3. Events are sent via `WebRtcServerCore::event_tx`
//! 4. FFI layer polls `event_rx` and invokes callbacks
//!
//! # Threading
//!
//! Unlike iceoryx2 types, WebRTC types from `webrtc-rs` are `Send + Sync`
//! and can be used directly in tokio async tasks without dedicated OS threads.

pub mod config;
pub mod core;
pub mod error;
pub mod events;

// Re-export main types
pub use config::*;
pub use core::*;
pub use error::*;
pub use events::*;
