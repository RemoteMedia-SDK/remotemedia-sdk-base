//! Python WebRTC bindings for RemoteMedia
//!
//! This module provides PyO3 bindings for WebRTC transport,
//! enabling Python applications to create and manage WebRTC media servers
//! with asyncio integration and decorator-based callbacks.
//!
//! # Architecture
//!
//! - **config.rs**: WebRtcServerConfig dataclass
//! - **events.rs**: Event types (PeerConnectedEvent, PipelineOutputEvent, etc.)
//! - **server.rs**: WebRtcServer PyClass with callback registration
//!
//! # Callback Pattern
//!
//! Uses decorator-based callback registration for Pythonic API:
//!
//! ```python
//! server = await WebRtcServer.create(config)
//!
//! @server.on_peer_connected
//! async def handle_peer(event: PeerConnectedEvent):
//!     print(f"Peer {event.peer_id} connected")
//!
//! @server.on_pipeline_output
//! async def handle_output(event: PipelineOutputEvent):
//!     print(f"Output for {event.peer_id}: {event.data}")
//!
//! await server.start()
//! ```
//!
//! # Async Integration
//!
//! Uses `future_into_py()` for proper asyncio integration. Callbacks are
//! stored as `Py<PyAny>` (which is `Send`) and invoked via `Python::attach()`
//! from the event loop.

pub mod config;
pub mod events;
pub mod server;
pub mod session;

// Re-export main types for convenience
pub use config::*;
pub use events::*;
pub use server::*;
pub use session::*;
