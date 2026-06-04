//! WebRTC Data Channel implementation (T160-T164)
//!
//! Provides reliable and unreliable data channel communication
//! over WebRTC peer connections.

// Public API types - fields and methods used by library consumers, not internally
#![allow(dead_code)]

use super::messages::{DataChannelMessage, MAX_MESSAGE_SIZE};
use crate::config::DataChannelMode;
use crate::{Error, Result};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, warn};
use webrtc::data_channel::RTCDataChannel;
use webrtc::peer_connection::RTCPeerConnection;

/// Data channel state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataChannelState {
    /// Channel is being created
    Connecting,
    /// Channel is open and ready for messages
    Open,
    /// Channel is closing
    Closing,
    /// Channel is closed
    Closed,
}

/// Callback type for incoming messages
pub type MessageHandler = Box<
    dyn Fn(DataChannelMessage) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

/// WebRTC Data Channel wrapper (T160)
///
/// Provides a high-level interface for sending and receiving
/// structured messages over a WebRTC data channel.
pub struct DataChannel {
    /// Channel label/name
    label: String,
    /// The underlying RTCDataChannel
    rtc_channel: Arc<RTCDataChannel>,
    /// Channel mode (reliable/unreliable)
    mode: DataChannelMode,
    /// Current channel state
    state: Arc<RwLock<DataChannelState>>,
    /// Total bytes sent
    bytes_sent: Arc<RwLock<u64>>,
    /// Total bytes received
    bytes_received: Arc<RwLock<u64>>,
    /// Messages sent count
    messages_sent: Arc<RwLock<u64>>,
    /// Messages received count
    messages_received: Arc<RwLock<u64>>,
}

impl DataChannel {
    /// Default channel label for control messages
    pub const CONTROL_CHANNEL_LABEL: &'static str = "control";

    /// Create a new data channel on an existing peer connection (T161)
    ///
    /// # Arguments
    /// * `peer_connection` - The RTCPeerConnection to create the channel on
    /// * `label` - Channel label/name
    /// * `mode` - Delivery mode (reliable or unreliable)
    ///
    /// # Returns
    /// Result with the created DataChannel
    pub async fn new(
        peer_connection: &RTCPeerConnection,
        label: &str,
        mode: DataChannelMode,
    ) -> Result<Self> {
        use webrtc::data_channel::data_channel_init::RTCDataChannelInit;

        // Configure channel based on mode
        let init = RTCDataChannelInit {
            ordered: Some(mode.ordered()),
            max_retransmits: mode.max_retransmits(),
            ..Default::default()
        };

        let rtc_channel = peer_connection
            .create_data_channel(label, Some(init))
            .await
            .map_err(|e| {
                Error::DataChannelError(format!("Failed to create data channel: {}", e))
            })?;

        let state = Arc::new(RwLock::new(DataChannelState::Connecting));
        let bytes_sent = Arc::new(RwLock::new(0u64));
        let bytes_received = Arc::new(RwLock::new(0u64));
        let messages_sent = Arc::new(RwLock::new(0u64));
        let messages_received = Arc::new(RwLock::new(0u64));

        let channel = Self {
            label: label.to_string(),
            rtc_channel, // Already Arc<RTCDataChannel> from create_data_channel
            mode,
            state,
            bytes_sent,
            bytes_received,
            messages_sent,
            messages_received,
        };

        // Set up state change handler
        channel.setup_state_handler().await;

        Ok(channel)
    }

    /// Wrap an existing RTCDataChannel (for incoming channels)
    ///
    /// # Arguments
    /// * `rtc_channel` - The RTCDataChannel received from remote peer
    /// * `mode` - Assumed delivery mode
    pub fn from_rtc_channel(rtc_channel: Arc<RTCDataChannel>, mode: DataChannelMode) -> Self {
        let label = rtc_channel.label().to_string();

        Self {
            label,
            rtc_channel,
            mode,
            state: Arc::new(RwLock::new(DataChannelState::Connecting)),
            bytes_sent: Arc::new(RwLock::new(0u64)),
            bytes_received: Arc::new(RwLock::new(0u64)),
            messages_sent: Arc::new(RwLock::new(0u64)),
            messages_received: Arc::new(RwLock::new(0u64)),
        }
    }

    /// Set up state change handler
    async fn setup_state_handler(&self) {
        let state = Arc::clone(&self.state);
        let label = self.label.clone();

        self.rtc_channel.on_open(Box::new(move || {
            let state = Arc::clone(&state);
            let label = label.clone();
            Box::pin(async move {
                debug!("Data channel '{}' opened", label);
                *state.write().await = DataChannelState::Open;
            })
        }));

        let state = Arc::clone(&self.state);
        let label = self.label.clone();

        self.rtc_channel.on_close(Box::new(move || {
            let state = Arc::clone(&state);
            let label = label.clone();
            Box::pin(async move {
                debug!("Data channel '{}' closed", label);
                *state.write().await = DataChannelState::Closed;
            })
        }));

        let label = self.label.clone();
        self.rtc_channel.on_error(Box::new(move |err| {
            let label = label.clone();
            Box::pin(async move {
                error!("Data channel '{}' error: {}", label, err);
            })
        }));
    }

    /// Send a message over the data channel (T162)
    ///
    /// # Arguments
    /// * `msg` - Message to send
    ///
    /// # Returns
    /// Result indicating success or failure
    pub async fn send_message(&self, msg: &DataChannelMessage) -> Result<()> {
        // T164: Validate message size
        if msg.exceeds_max_size() {
            return Err(Error::DataChannelError(format!(
                "Message size {} exceeds maximum {} bytes",
                msg.size(),
                MAX_MESSAGE_SIZE
            )));
        }

        // Check channel state
        let state = *self.state.read().await;
        if state != DataChannelState::Open {
            return Err(Error::DataChannelError(format!(
                "Data channel is not open (state: {:?})",
                state
            )));
        }

        // Serialize message
        let bytes = msg
            .encode()
            .map_err(|e| Error::DataChannelError(format!("Failed to serialize message: {}", e)))?;

        let bytes_len = bytes.len();

        // Send via data channel
        self.rtc_channel
            .send(&bytes.into())
            .await
            .map_err(|e| Error::DataChannelError(format!("Failed to send message: {}", e)))?;

        // Update statistics
        *self.bytes_sent.write().await += bytes_len as u64;
        *self.messages_sent.write().await += 1;

        debug!("Sent {} bytes on data channel '{}'", bytes_len, self.label);

        Ok(())
    }

    /// Send raw binary data (for efficiency when type is known)
    ///
    /// # Arguments
    /// * `data` - Raw bytes to send
    pub async fn send_binary(&self, data: &[u8]) -> Result<()> {
        if data.len() > MAX_MESSAGE_SIZE {
            return Err(Error::DataChannelError(format!(
                "Data size {} exceeds maximum {} bytes",
                data.len(),
                MAX_MESSAGE_SIZE
            )));
        }

        let state = *self.state.read().await;
        if state != DataChannelState::Open {
            return Err(Error::DataChannelError(format!(
                "Data channel is not open (state: {:?})",
                state
            )));
        }

        self.rtc_channel
            .send(&data.to_vec().into())
            .await
            .map_err(|e| Error::DataChannelError(format!("Failed to send binary: {}", e)))?;

        *self.bytes_sent.write().await += data.len() as u64;
        *self.messages_sent.write().await += 1;

        Ok(())
    }

    /// Set message handler callback (T163)
    ///
    /// # Arguments
    /// * `handler` - Async callback function for incoming messages
    pub fn on_message<F, Fut>(&self, handler: F)
    where
        F: Fn(DataChannelMessage) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let bytes_received = Arc::clone(&self.bytes_received);
        let messages_received = Arc::clone(&self.messages_received);
        let label = self.label.clone();
        let handler = Arc::new(handler);

        self.rtc_channel.on_message(Box::new(move |msg| {
            let bytes_received = Arc::clone(&bytes_received);
            let messages_received = Arc::clone(&messages_received);
            let label = label.clone();
            let handler = Arc::clone(&handler);

            // Parse the message
            let data = msg.data.to_vec();
            let data_len = data.len();

            Box::pin(async move {
                // Update statistics
                *bytes_received.write().await += data_len as u64;
                *messages_received.write().await += 1;

                // Parse message
                match DataChannelMessage::decode(&data) {
                    Ok(parsed) => {
                        debug!("Received {} bytes on data channel '{}'", data_len, label);
                        handler(parsed).await;
                    }
                    Err(e) => {
                        warn!("Failed to parse message on channel '{}': {}", label, e);
                    }
                }
            })
        }));
    }

    /// Set raw binary message handler (for efficiency)
    ///
    /// # Arguments
    /// * `handler` - Callback for raw binary messages
    pub fn on_binary_message<F, Fut>(&self, handler: F)
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let bytes_received = Arc::clone(&self.bytes_received);
        let messages_received = Arc::clone(&self.messages_received);
        let handler = Arc::new(handler);

        self.rtc_channel.on_message(Box::new(move |msg| {
            let bytes_received = Arc::clone(&bytes_received);
            let messages_received = Arc::clone(&messages_received);
            let handler = Arc::clone(&handler);
            let data = msg.data.to_vec();
            let data_len = data.len();

            Box::pin(async move {
                *bytes_received.write().await += data_len as u64;
                *messages_received.write().await += 1;
                handler(data).await;
            })
        }));
    }

    /// Get the channel label
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Get the channel mode
    pub fn mode(&self) -> DataChannelMode {
        self.mode
    }

    /// Get current state
    pub async fn state(&self) -> DataChannelState {
        *self.state.read().await
    }

    /// Check if channel is open
    pub async fn is_open(&self) -> bool {
        *self.state.read().await == DataChannelState::Open
    }

    /// Get total bytes sent
    pub async fn bytes_sent(&self) -> u64 {
        *self.bytes_sent.read().await
    }

    /// Get total bytes received
    pub async fn bytes_received(&self) -> u64 {
        *self.bytes_received.read().await
    }

    /// Get messages sent count
    pub async fn messages_sent(&self) -> u64 {
        *self.messages_sent.read().await
    }

    /// Get messages received count
    pub async fn messages_received(&self) -> u64 {
        *self.messages_received.read().await
    }

    /// Get the underlying RTCDataChannel
    pub fn rtc_channel(&self) -> &Arc<RTCDataChannel> {
        &self.rtc_channel
    }

    /// Close the data channel
    pub async fn close(&self) -> Result<()> {
        *self.state.write().await = DataChannelState::Closing;

        self.rtc_channel
            .close()
            .await
            .map_err(|e| Error::DataChannelError(format!("Failed to close channel: {}", e)))?;

        *self.state.write().await = DataChannelState::Closed;

        debug!("Data channel '{}' closed", self.label);
        Ok(())
    }
}

/// Channel statistics
#[derive(Debug, Clone, Default)]
pub struct DataChannelStats {
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Total bytes received
    pub bytes_received: u64,
    /// Messages sent count
    pub messages_sent: u64,
    /// Messages received count
    pub messages_received: u64,
}

impl DataChannel {
    /// Get channel statistics
    pub async fn stats(&self) -> DataChannelStats {
        DataChannelStats {
            bytes_sent: *self.bytes_sent.read().await,
            bytes_received: *self.bytes_received.read().await,
            messages_sent: *self.messages_sent.read().await,
            messages_received: *self.messages_received.read().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_channel_mode_reliable() {
        let mode = DataChannelMode::Reliable;
        assert!(mode.ordered());
        assert_eq!(mode.max_retransmits(), None);
    }

    #[test]
    fn test_data_channel_mode_unreliable() {
        let mode = DataChannelMode::Unreliable;
        assert!(!mode.ordered());
        assert_eq!(mode.max_retransmits(), Some(0));
    }

    #[test]
    fn test_data_channel_state() {
        assert_ne!(DataChannelState::Open, DataChannelState::Closed);
        assert_eq!(DataChannelState::Connecting, DataChannelState::Connecting);
    }
}
