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
//! # Buffer ownership
//!
//! [`DataChannel::send_binary_owned`] accepts an owning `bytes::Bytes` range;
//! `Bytes::from_owner(shared_bytes)` preserves a runtime buffer and its budget.
//! [`DataChannel::on_binary_message_owned`] forwards the incoming WebRTC owner,
//! whose slices can outlive the callback and channel. Neither wrapper stages the
//! payload in another buffer.
//!
//! Borrowed sends and mutable `Vec` callbacks remain available. Their
//! `*_with_context` variants attribute the required snapshot to the caller's
//! copy authority; context-free calls use the explicitly unattributed legacy
//! ledger. A receive snapshot rejected by policy is delivered as an error.
//! Transport byte/message statistics are separate from these copy counters.
//!
//! This is wrapper ownership, not zero-copy network delivery. WebRTC receive
//! buffering, SCTP/DTLS, protobuf envelopes, chunking and network/device transfers
//! remain separate boundaries. Structured decoding still allocates its decoded
//! payload, although dispatch no longer copies the incoming wire buffer first.
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
