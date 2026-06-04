//! WebRTC data channel management
//!
//! Handles reliable/unreliable data channel creation and message passing.
//!
//! # Overview
//!
//! Data channels provide a way to send arbitrary data between peers
//! alongside media streams. This module provides:
//!
//! - [`DataChannel`] - High-level wrapper for WebRTC data channels
//! - [`DataChannelMessage`] - Message types (JSON, binary, text, runtime-data, chunk)
//! - [`ChunkInfo`] - Chunk metadata for fragmented payloads
//! - [`DataChannelSplitter`] - Splits large payloads into bounded chunks
//! - [`DataChannelReassembler`] - Reassembles chunks into complete payloads
//! - [`crate::config::DataChannelMode`] - Reliable vs unreliable delivery
//! - [`ControlMessage`] - Pre-defined control message types
//!
//! # RTP vs Data Channel
//!
//! - `RuntimeData::Audio` / `RuntimeData::Video` → RTP media tracks
//! - All other `RuntimeData` variants → WebRTC Data Channel
//!
//! # Chunking
//!
//! Payloads exceeding [`messages::MAX_CHUNK_SIZE`] (128 KB) are
//! automatically split into numbered chunks. The [`DataChannelReassembler`]
//! buffers and reassembles before surfacing to the application layer.
//!
//! # Example
//!
//! ```ignore
//! use remotemedia_webrtc::channels::{DataChannel, DataChannelMessage};
//! use remotemedia_webrtc::DataChannelMode;
//!
//! // Create a reliable data channel
//! let channel = DataChannel::new(&peer_connection, "control", DataChannelMode::Reliable).await?;
//!
//! // Send a JSON message
//! let msg = DataChannelMessage::json(&serde_json::json!({
//!     "action": "configure",
//!     "params": { "quality": "high" }
//! }))?;
//! channel.send_message(&msg).await?;
//!
//! // Set up message handler
//! channel.on_message(|msg| async move {
//!     println!("Received: {:?}", msg);
//! });
//! ```

mod chunking;
mod data_channel;
mod messages;

pub use chunking::{DataChannelReassembler, DataChannelSplitter};
pub use data_channel::{DataChannel, DataChannelState, DataChannelStats};
pub use messages::{
    ChunkInfo, ControlMessage, DataChannelMessage, MAX_CHUNK_SIZE, MAX_MESSAGE_SIZE,
};
