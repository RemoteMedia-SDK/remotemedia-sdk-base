//! Data structures for low-latency streaming pipeline
//!
//! This module contains core data structures for the low-latency streaming
//! optimization feature, including:
//! - SpeculativeSegment: Audio segments forwarded before VAD confirmation
//! - ControlMessage: Standardized control flow messages (cancellation, hints)
//! - BufferingPolicy: Configuration for auto-batching wrapper
//! - RingBuffer: Lock-free circular buffer for speculative audio segments

pub mod buffering_policy;
pub mod control_message;
pub mod ring_buffer;
pub mod speculative_segment;
pub mod text_channel;

pub use buffering_policy::{BufferingPolicy, MergeStrategy};
pub use control_message::{ControlMessage, ControlMessageType};
pub use ring_buffer::RingBuffer;
pub use speculative_segment::{SegmentStatus, SpeculativeSegment};
pub use text_channel::{split_text_str, tag_text_str, TEXT_CHANNEL_DEFAULT};
