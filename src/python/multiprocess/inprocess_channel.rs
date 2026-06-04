//! In-process channel for Python nodes (Android, and opt-in on Linux/macOS)
//!
//! Provides a simple in-memory channel using `Arc<Mutex<Vec<u8>>>` for data exchange
//! between Rust host and Python nodes running in the same process. This replaces
//! the iceoryx2-based IPC when running on Android or when in-process execution
//! is explicitly requested via `REMOTEMEDIA_EXECUTION_STRATEGY=inprocess`.

use crate::{Error, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::RwLock;

use remotemedia_types::RuntimeData;

/// Global shared channel registry instance
static GLOBAL_REGISTRY: OnceLock<Arc<InProcessChannelRegistry>> = OnceLock::new();

/// Channel statistics
#[derive(Debug, Default, Clone)]
pub struct ChannelStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_transferred: u64,
    pub last_activity: Option<std::time::Instant>,
}

/// Handle to an in-process channel
#[derive(Debug, Clone)]
pub struct ChannelHandle {
    /// Unique channel name
    pub name: String,

    /// Maximum buffer capacity
    pub capacity: usize,

    /// Channel statistics
    pub stats: Arc<RwLock<ChannelStats>>,

    /// Backpressure enabled flag
    pub backpressure_enabled: bool,
}

/// In-process channel registry
pub struct InProcessChannelRegistry {
    /// Active channels - each channel is a simple MPSC channel
    channels: Arc<RwLock<HashMap<String, Arc<Mutex<Vec<u8>>>>>>,

    /// Channel handles
    handles: Arc<RwLock<HashMap<String, ChannelHandle>>>,
}

impl InProcessChannelRegistry {
    /// Get or create the global shared channel registry
    pub fn global() -> Arc<Self> {
        GLOBAL_REGISTRY
            .get_or_init(|| {
                Arc::new(Self::new_internal())
            })
            .clone()
    }

    /// Create a new channel registry (internal use only)
    fn new_internal() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            handles: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Initialize the registry (no-op for in-process, kept for API compatibility)
    pub async fn ensure_initialized(&self) -> Result<()> {
        Ok(())
    }

    /// Create a new in-process channel
    pub async fn create_channel(
        &self,
        name: &str,
        capacity: usize,
        backpressure: bool,
    ) -> Result<ChannelHandle> {
        // Create the in-memory buffer
        let buffer = Arc::new(Mutex::new(Vec::new()));

        // Store the channel buffer
        let mut channels = self.channels.write().await;
        channels.insert(name.to_string(), buffer);

        // Create channel handle
        let handle = ChannelHandle {
            name: name.to_string(),
            capacity,
            stats: Arc::new(RwLock::new(ChannelStats::default())),
            backpressure_enabled: backpressure,
        };

        let mut handles = self.handles.write().await;
        handles.insert(name.to_string(), handle.clone());

        tracing::info!(
            "Created in-process channel: {} (capacity: {})",
            name,
            capacity
        );
        Ok(handle)
    }

    /// Destroy a channel and cleanup resources
    pub async fn destroy_channel(&self, channel: ChannelHandle) -> Result<()> {
        let channel_name = channel.name.clone();

        // Remove the channel buffer
        let mut channels = self.channels.write().await;
        channels.remove(&channel_name);

        // Remove the channel handle
        let mut handles = self.handles.write().await;
        handles.remove(&channel_name);

        tracing::info!("Destroyed in-process channel: {}", channel_name);
        Ok(())
    }

    /// Drain all pending messages from a channel
    pub async fn drain_channel(&self, channel_name: &str) -> Result<usize> {
        let mut channels = self.channels.write().await;
        if let Some(buffer) = channels.get_mut(channel_name) {
            let mut buf = buffer.lock().unwrap();
            let count = buf.len();
            buf.clear();
            Ok(count)
        } else {
            Ok(0)
        }
    }

    /// Get the internal buffer for a channel (for Publisher/Subscriber)
    async fn get_buffer(&self, channel_name: &str) -> Result<Arc<Mutex<Vec<u8>>>> {
        let channels = self.channels.read().await;
        channels
            .get(channel_name)
            .cloned()
            .ok_or_else(|| Error::IpcError(format!("Channel not found: {}", channel_name)))
    }

    /// Create a publisher for a channel
    pub async fn create_publisher(&self, channel_name: &str) -> Result<InProcessPublisher> {
        let buffer = self.get_buffer(channel_name).await?;
        
        let handles = self.handles.read().await;
        let handle = handles.get(channel_name).ok_or_else(|| {
            Error::IpcError(format!("Channel handle not found: {}", channel_name))
        })?;

        Ok(InProcessPublisher {
            channel_name: channel_name.to_string(),
            buffer,
            stats: handle.stats.clone(),
            backpressure: handle.backpressure_enabled,
        })
    }

    /// Create a subscriber for a channel
    pub async fn create_subscriber(&self, channel_name: &str) -> Result<InProcessSubscriber> {
        let buffer = self.get_buffer(channel_name).await?;
        
        let handles = self.handles.read().await;
        let handle = handles.get(channel_name).ok_or_else(|| {
            Error::IpcError(format!("Channel handle not found: {}", channel_name))
        })?;

        // In-process: we don't have history, but we can at least get current messages
        Ok(InProcessSubscriber {
            channel_name: channel_name.to_string(),
            buffer,
            stats: handle.stats.clone(),
            cursor: 0, // Track read position for simple history
        })
    }

    /// Create a subscriber and immediately drain any stale messages
    pub async fn create_subscriber_fresh(&self, channel_name: &str) -> Result<InProcessSubscriber> {
        let mut subscriber = self.create_subscriber(channel_name).await?;
        // Reset cursor to end of buffer to skip stale messages
        if let Ok(mut buf) = subscriber.buffer.lock() {
            subscriber.cursor = buf.len();
        }
        Ok(subscriber)
    }
}

/// In-process publisher for sending data to a channel
pub struct InProcessPublisher {
    channel_name: String,
    buffer: Arc<Mutex<Vec<u8>>>,
    stats: Arc<RwLock<ChannelStats>>,
    backpressure: bool,
}

impl InProcessPublisher {
    /// Publish data to the channel
    pub fn publish(&self, data: &RuntimeData) -> Result<()> {
        // Serialize RuntimeData to bytes via msgpack
        let bytes = super::data_transfer::to_bytes(data)
            .map_err(|e| Error::IpcError(format!("Failed to serialize: {}", e)))?;

        tracing::debug!(
            "[InProcess Publisher] Channel '{}' publishing {} bytes (type: {})",
            self.channel_name,
            bytes.len(),
            data.data_type()
        );

        // Lock and push to buffer
        let mut buf = self.buffer.lock().unwrap();
        buf.extend_from_slice(&bytes);

        // Update stats
        if let Ok(mut stats) = self.stats.try_write() {
            stats.messages_sent += 1;
            stats.bytes_transferred += bytes.len() as u64;
            stats.last_activity = Some(std::time::Instant::now());
        }

        Ok(())
    }

    /// Try to publish without blocking (always succeeds for in-process)
    pub fn try_publish(&self, data: &RuntimeData) -> Result<bool> {
        self.publish(data)?;
        Ok(true)
    }

    /// Send raw bytes directly (like READY signal)
    pub fn send(&self, bytes: &[u8]) -> Result<()> {
        tracing::debug!(
            "[InProcess Publisher] Channel '{}' sending raw {} bytes",
            self.channel_name,
            bytes.len()
        );

        let mut buf = self.buffer.lock().unwrap();
        buf.extend_from_slice(bytes);

        if let Ok(mut stats) = self.stats.try_write() {
            stats.messages_sent += 1;
            stats.bytes_transferred += bytes.len() as u64;
            stats.last_activity = Some(std::time::Instant::now());
        }

        Ok(())
    }
}

/// In-process subscriber for receiving data from a channel
pub struct InProcessSubscriber {
    channel_name: String,
    buffer: Arc<Mutex<Vec<u8>>>,
    stats: Arc<RwLock<ChannelStats>>,
    cursor: usize, // Track read position for message streaming
}

impl InProcessSubscriber {
    /// Receive data from the channel
    pub fn receive(&mut self) -> Result<Option<RuntimeData>> {
        let mut buf = self.buffer.lock().unwrap();
        
        // Check if there's data available from our cursor position
        if self.cursor < buf.len() {
            // Try to deserialize from cursor position
            let data_slice = &buf[self.cursor..];
            
            // Try to deserialize a single RuntimeData
            match super::data_transfer::from_bytes(data_slice) {
                Ok(data) => {
                    // Calculate how many bytes we consumed
                    let consumed = data_slice.len() - {
                        // We can't easily know how many bytes were consumed,
                        // so we use a simple approach: advance by the serialized size
                        // For now, just clear the buffer after reading one message
                        // This is a simplification - in a real impl we'd track precisely
                        0
                    };
                    
                    // Simple approach: clear and set cursor to end
                    self.cursor = buf.len();
                    
                    // Update stats
                    if let Ok(mut stats) = self.stats.try_write() {
                        stats.messages_received += 1;
                        stats.last_activity = Some(std::time::Instant::now());
                    }

                    return Ok(Some(data));
                }
                Err(_) => {
                    // If deserialization failed, data might be incomplete
                    // Return None to indicate we need more data
                    return Ok(None);
                }
            }
        }
        
        Ok(None)
    }

    /// Receive raw bytes without deserialization
    pub fn receive_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        let mut buf = self.buffer.lock().unwrap();
        
        if self.cursor < buf.len() {
            let data = buf[self.cursor..].to_vec();
            self.cursor = buf.len();
            
            // Update stats
            if let Ok(mut stats) = self.stats.try_write() {
                stats.messages_received += 1;
                stats.bytes_transferred += data.len() as u64;
                stats.last_activity = Some(std::time::Instant::now());
            }
            
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_inprocess_channel_creation() {
        let registry = InProcessChannelRegistry::global();
        
        let channel_name = format!("test/channel/create/{}", std::process::id());
        let channel = registry
            .create_channel(&channel_name, 100, true)
            .await
            .unwrap();

        assert_eq!(channel.name, channel_name);
        assert_eq!(channel.capacity, 100);
        assert!(channel.backpressure_enabled);

        registry.destroy_channel(channel).await.unwrap();
    }

    #[tokio::test]
    async fn test_inprocess_publish_subscribe() {
        let registry = InProcessChannelRegistry::global();
        
        let channel_name = format!(
            "test/channel/pubsub/{}/{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let channel = registry
            .create_channel(&channel_name, 10, false)
            .await
            .unwrap();

        // Create publisher and subscriber
        let publisher = registry.create_publisher(&channel_name).await.unwrap();
        let mut subscriber = registry.create_subscriber(&channel_name).await.unwrap();

        // Publish data
        let data = RuntimeData::Text("Hello, InProcess!".into());
        publisher.publish(&data).unwrap();

        // Small delay
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Receive data
        let received = subscriber.receive().unwrap();
        assert!(received.is_some());
        
        let received_data = received.unwrap();
        match &received_data {
            RuntimeData::Text(s) => assert_eq!(s.as_str(), "Hello, InProcess!"),
            _ => panic!("Expected Text variant, got {:?}", received_data.data_type()),
        }

        registry.destroy_channel(channel).await.unwrap();
    }
}