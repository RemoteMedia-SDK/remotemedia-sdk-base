//! Audio/Video synchronization for multi-peer scenarios
//!
//! Manages jitter buffers, clock drift estimation, and RTP timestamp tracking.
//!
//! # Overview
//!
//! This module provides the synchronization infrastructure for WebRTC multi-peer
//! audio/video conferencing:
//!
//! - **RTP Timestamp utilities**: Handle 32-bit wraparound, NTP correlation
//! - **Clock Drift Estimation**: Detect and correct clock differences between peers
//! - **Jitter Buffers**: Smooth network jitter for audio and video playback
//! - **SyncManager**: Per-peer synchronization coordinator
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                    Per-Peer SyncManager                  │
//! │  ┌─────────────────┐    ┌─────────────────┐             │
//! │  │  Audio Jitter   │    │  Video Jitter   │             │
//! │  │     Buffer      │    │     Buffer      │             │
//! │  └────────┬────────┘    └────────┬────────┘             │
//! │           │                      │                       │
//! │           ▼                      ▼                       │
//! │  ┌─────────────────────────────────────────┐            │
//! │  │         Clock Drift Estimator            │            │
//! │  │  (RTCP SR → linear regression → PPM)    │            │
//! │  └─────────────────────────────────────────┘            │
//! │           │                                              │
//! │           ▼                                              │
//! │  ┌─────────────────────────────────────────┐            │
//! │  │      RTP Timestamp Correlation           │            │
//! │  │   (NTP/RTP mapping → wall-clock time)   │            │
//! │  └─────────────────────────────────────────┘            │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use remotemedia_webrtc::sync::{SyncManager, SyncConfig, AudioFrame};
//!
//! // Create sync manager for a peer
//! let config = SyncConfig::default();
//! let mut sync = SyncManager::new("peer1".to_string(), config)?;
//!
//! // Process incoming RTCP Sender Report
//! sync.update_rtcp_sender_report(sr, is_audio);
//!
//! // Process incoming audio frames
//! sync.process_audio_frame(audio_frame)?;
//!
//! // Pop synchronized frames for playback
//! if let Some(synced_frame) = sync.pop_next_audio_frame() {
//!     // Play synced_frame.samples at synced_frame.sample_rate
//! }
//! ```

mod clock_drift;
mod jitter_buffer;
mod manager;
mod timestamp;

// Re-export main types for public API - used by external consumers of this crate
#[allow(unused_imports)]
pub use clock_drift::{ClockDriftEstimate, ClockDriftEstimator, ClockObservation, DriftAction};
#[allow(unused_imports)]
pub use jitter_buffer::{BufferStats, JitterBuffer, JitterBufferFrame};
#[allow(unused_imports)]
pub use manager::TimestampOffset;
pub use manager::{
    AudioFrame, RtcpSenderReport, SyncConfig, SyncManager, SyncState, SyncedAudioFrame,
    SyncedVideoFrame, VideoFrame,
};
#[allow(unused_imports)]
pub use timestamp::{NtpRtpMapping, RtpHeader, RtpTimestamp};
