//! IPC Channel abstraction for Node.js
//!
//! Provides a high-level interface for creating publishers and subscribers
//! on iceoryx2 channels.

use super::error::IpcError;
use super::publisher::NapiPublisher;
use super::subscriber::NapiSubscriber;
use napi_derive::napi;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Channel configuration
#[napi(object)]
#[derive(Debug, Clone, Default)]
pub struct ChannelConfig {
    /// Maximum number of samples that can be in flight (default: 64)
    pub capacity: Option<u32>,
    /// Maximum payload size in bytes (default: 1MB)
    pub max_payload_size: Option<u32>,
    /// Enable backpressure - block on full (default: false, drops oldest)
    pub backpressure: Option<bool>,
    /// Number of historical samples for new subscribers (default: 0)
    pub history_size: Option<u32>,
}

impl ChannelConfig {
    pub fn capacity(&self) -> usize {
        self.capacity.unwrap_or(64) as usize
    }

    pub fn max_payload_size(&self) -> usize {
        self.max_payload_size.unwrap_or(1_048_576) as usize
    }

    pub fn backpressure(&self) -> bool {
        self.backpressure.unwrap_or(false)
    }

    pub fn history_size(&self) -> usize {
        self.history_size.unwrap_or(0) as usize
    }
}

/// IPC Channel for pub/sub communication
#[napi]
pub struct NapiChannel {
    /// Full channel name including session prefix
    name: String,
    /// Channel configuration
    config: ChannelConfig,
    /// Whether the channel is still open (shared across all handles)
    is_open: Arc<AtomicBool>,
}

#[napi]
impl NapiChannel {
    /// Create a new channel (internal use - use Session.channel() instead)
    pub(crate) fn new(name: String, config: ChannelConfig) -> Self {
        Self {
            name,
            config,
            is_open: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Create a new channel handle sharing state with an existing channel
    pub(crate) fn with_shared_state(
        name: String,
        config: ChannelConfig,
        is_open: Arc<AtomicBool>,
    ) -> Self {
        Self {
            name,
            config,
            is_open,
        }
    }

    /// Get the shared is_open state for creating handles
    pub(crate) fn get_is_open(&self) -> Arc<AtomicBool> {
        self.is_open.clone()
    }

    /// Get the full channel name
    #[napi(getter)]
    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Get the channel configuration
    #[napi(getter)]
    pub fn config(&self) -> ChannelConfig {
        self.config.clone()
    }

    /// Check if the channel is still open
    #[napi(getter)]
    pub fn is_open(&self) -> bool {
        self.is_open.load(Ordering::SeqCst)
    }

    /// Create a publisher for this channel
    #[napi]
    pub fn create_publisher(&self) -> napi::Result<NapiPublisher> {
        if !self.is_open() {
            return Err(IpcError::ChannelError("Channel is closed".to_string()).into());
        }

        NapiPublisher::new(
            self.name.clone(),
            self.config.max_payload_size(),
            self.config.capacity(),
        )
    }

    /// Create a subscriber for this channel
    #[napi]
    pub fn create_subscriber(&self, buffer_size: Option<u32>) -> napi::Result<NapiSubscriber> {
        if !self.is_open() {
            return Err(IpcError::ChannelError("Channel is closed".to_string()).into());
        }

        let buffer = buffer_size
            .map(|s| s as usize)
            .unwrap_or(self.config.capacity());

        NapiSubscriber::new(self.name.clone(), buffer)
    }

    /// Close the channel and release resources
    #[napi]
    pub fn close(&self) {
        self.is_open.store(false, Ordering::SeqCst);
        // Note: Actual cleanup of publishers/subscribers happens in their Drop impls
    }
}

/// Helper to construct channel names with session prefix
pub fn make_channel_name(session_id: &str, channel_name: &str) -> String {
    format!("{}_{}", session_id, channel_name)
}

/// Helper to construct input channel name (for receiving from Rust/Python)
pub fn make_input_channel_name(session_id: &str, node_id: &str) -> String {
    format!("{}_{}_input", session_id, node_id)
}

/// Helper to construct output channel name (for sending to Rust/Python)
pub fn make_output_channel_name(session_id: &str, node_id: &str) -> String {
    format!("{}_{}_output", session_id, node_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_name_helpers() {
        assert_eq!(
            make_channel_name("session_123", "audio"),
            "session_123_audio"
        );
        assert_eq!(
            make_input_channel_name("session_123", "node_456"),
            "session_123_node_456_input"
        );
        assert_eq!(
            make_output_channel_name("session_123", "node_456"),
            "session_123_node_456_output"
        );
    }

    #[test]
    fn test_channel_config_defaults() {
        let config = ChannelConfig::default();
        assert_eq!(config.capacity(), 64);
        assert_eq!(config.max_payload_size(), 1_048_576);
        assert!(!config.backpressure());
        assert_eq!(config.history_size(), 0);
    }
}
