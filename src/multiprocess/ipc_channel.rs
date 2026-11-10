//! IPC channel management using iceoryx2
//!
//! Legacy compatibility channels over iceoryx2 shared memory. Serialized wire
//! transfers still copy into/out of loans; mapped descriptor workers provide
//! strict zero-copy bulk forwarding.

use crate::{Error, Result};
#[cfg(feature = "multiprocess")]
use iceoryx2::prelude::*;
#[cfg(feature = "multiprocess")]
use iceoryx2::service::port_factory::publish_subscribe::PortFactory;
#[cfg(feature = "multiprocess")]
use iceoryx2_log::set_log_level;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;

use remotemedia_types::RuntimeData;
#[cfg(feature = "multiprocess")]
use remotemedia_types::{CopyContext, CopyReason, SharedBytes};

/// Global shared channel registry instance
/// All MultiprocessExecutor instances will use this single registry to ensure
/// they all share the same iceoryx2 Node and can communicate with each other
static GLOBAL_REGISTRY: OnceLock<Arc<ChannelRegistry>> = OnceLock::new();

/// Maximum payload size for IPC transfers (1 MB)
/// This is the maximum size of a single RuntimeData message that can be transferred
/// Matches the global iceoryx2 config: publish_subscribe.max_slice_len = 1048576
const MAX_SLICE_LEN: usize = 1 * 1024 * 1024;

/// Initial slice size of a channel's publisher; see `create_publisher`.
#[cfg(feature = "multiprocess")]
const INITIAL_SLICE_LEN: usize = 16 * 1024;

#[cfg(feature = "multiprocess")]
type WakeEventFactory = iceoryx2::service::port_factory::event::PortFactory<ipc::Service>;

/// Port that signals a channel's reader that a message was published.
#[cfg(feature = "multiprocess")]
pub type WakeNotifier = iceoryx2::port::notifier::Notifier<ipc::Service>;

/// Port a channel's reader blocks on until the writer signals.
#[cfg(feature = "multiprocess")]
pub type WakeListener = iceoryx2::port::listener::Listener<ipc::Service>;

/// Name of the event service that accompanies a channel.
pub fn wake_event_name(channel_name: &str) -> String {
    format!("{channel_name}_event")
}

/// Subscriber ports a channel accepts. See `ChannelRegistry::create_channel`.
#[cfg(feature = "multiprocess")]
const CHANNEL_MAX_SUBSCRIBERS: usize = 2;

/// Channel statistics
#[derive(Debug, Default, Clone)]
pub struct ChannelStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_transferred: u64,
    pub last_activity: Option<std::time::Instant>,
}

/// Handle to a shared memory channel
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

/// Channel registry for managing IPC channels
pub struct ChannelRegistry {
    #[cfg(feature = "multiprocess")]
    /// iceoryx2 node instance
    node: Option<Node<ipc::Service>>,

    /// Active channels
    channels: Arc<RwLock<HashMap<String, ChannelHandle>>>,

    #[cfg(feature = "multiprocess")]
    /// Active services (must be kept alive or they get dropped/deleted)
    services: Arc<RwLock<HashMap<String, PortFactory<ipc::Service, [u8], ()>>>>,

    #[cfg(feature = "multiprocess")]
    /// Wake-event services, keyed by the channel they belong to. See
    /// [`ChannelRegistry::create_wake_event`].
    wake_events: Arc<RwLock<HashMap<String, WakeEventFactory>>>,
}

impl ChannelRegistry {
    /// Get or create the global shared channel registry
    /// This ensures all nodes/executors use the same iceoryx2 Node instance
    pub fn global() -> Arc<Self> {
        GLOBAL_REGISTRY
            .get_or_init(|| {
                #[cfg(feature = "multiprocess")]
                {
                    // Set log level first
                    set_log_level(LogLevel::Info);

                    // Create iceoryx2 node directly here, not via a mutable method
                    match NodeBuilder::new().create::<ipc::Service>() {
                        Ok(node) => {
                            tracing::info!("Global iceoryx2 node created successfully");
                            Arc::new(Self {
                                node: Some(node),
                                channels: Arc::new(RwLock::new(HashMap::new())),
                                services: Arc::new(RwLock::new(HashMap::new())),
                                wake_events: Arc::new(RwLock::new(HashMap::new())),
                            })
                        }
                        Err(e) => {
                            tracing::error!("Failed to create global iceoryx2 node: {:?}. IPC operations will fail.", e);
                            // Return registry without node - operations will fail with clear error
                            Arc::new(Self {
                                node: None,
                                channels: Arc::new(RwLock::new(HashMap::new())),
                                services: Arc::new(RwLock::new(HashMap::new())),
                                wake_events: Arc::new(RwLock::new(HashMap::new())),
                            })
                        }
                    }
                }

                #[cfg(not(feature = "multiprocess"))]
                {
                    Arc::new(Self::new_internal())
                }
            })
            .clone()
    }

    /// Create a new channel registry (internal use only)
    /// External code should use `global()` to get the shared instance
    fn new_internal() -> Self {
        Self {
            #[cfg(feature = "multiprocess")]
            node: None,
            channels: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(feature = "multiprocess")]
            services: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(feature = "multiprocess")]
            wake_events: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Clean up stale iceoryx2 shared memory state left behind by crashed
    /// processes or previous test runs.
    ///
    /// This removes orphaned node monitor files in `/tmp/iceoryx2/nodes/`
    /// that prevent new IPC nodes from being created (causing `InternalError` /
    /// `NodeCreationFailure`).
    ///
    /// A node monitor is considered orphaned when its corresponding numeric node
    /// directory does not exist (the process that created it has exited).
    ///
    /// **IMPORTANT**: This method MUST NOT remove numeric node directories or
    /// shared memory segments — the current process's GLOBAL_REGISTRY node lives
    /// in a numeric directory, and removing it would corrupt all services that
    /// node created, causing `ServiceInCorruptedState` / `DoesNotExist` in child
    /// processes trying to connect.
    ///
    /// **Safety model**: Only deletes files whose corresponding numeric node
    /// directory is absent. The node directory is created when a process starts
    /// and removed on clean exit, so its absence means the process has died.
    ///
    /// **Why not use flock?** iceoryx2 does NOT hold `flock()`-style locks on
    /// these files — they are coordination markers managed by iceoryx2 internally.
    /// A `flock` check would always succeed (no one holds the lock), making it
    /// useless as a safety mechanism. The directory-existence check is the
    /// correct guard.
    ///
    /// Not available on Android — iceoryx2 is Linux-only and not used on Android.
    #[cfg(all(feature = "multiprocess", not(target_os = "android")))]
    pub fn cleanup_stale_iceoryx2_state() {
        use std::fs;

        // Every platform (the file sweep below only knows Linux's
        // /tmp/iceoryx2): iceoryx2's own cleanup of nodes whose process died,
        // with their ports and shared memory, so a crashed run's samples are
        // never delivered to the next run's identically named
        // `session_0_*` ports.
        let (cleanups, failed_cleanups) =
            super::runner::cleanup_dead_nodes(iceoryx2::config::Config::global_config());
        if cleanups + failed_cleanups > 0 {
            tracing::info!(
                "iceoryx2: removed {} dead node(s), {} could not be removed",
                cleanups,
                failed_cleanups
            );
        }

        // Remove orphaned node_monitor files — these are the source of
        // InternalError when a new node can't reconcile stale monitor state.
        // Only remove monitors where the corresponding numeric node directory
        // doesn't exist (process has died).
        if let Ok(entries) = fs::read_dir("/tmp/iceoryx2/nodes") {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file()
                    && path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .map_or(false, |n| n.ends_with(".node_monitor_owner_lock"))
                {
                    // Extract the node ID from the lock file name
                    // e.g., "iox2_7109616707674936231170695157.node_monitor_owner_lock"
                    if let Some(name) = path.file_stem().and_then(|n| n.to_str()) {
                        // The node ID is the part after "iox2_"
                        let node_id = name.strip_prefix("iox2_").unwrap_or("");
                        let node_dir = fs::canonicalize("/tmp/iceoryx2/nodes")
                            .ok()
                            .map(|d| d.join(node_id));
                        if node_dir.as_ref().map_or(true, |d| !d.is_dir()) {
                            // Numeric node dir doesn't exist — this lock is orphaned
                            if let Err(e) = fs::remove_file(&path) {
                                tracing::debug!(
                                    "Failed to remove orphaned iceoryx2 lock {:?}: {}",
                                    path,
                                    e
                                );
                            } else {
                                tracing::debug!("Removed orphaned iceoryx2 lock: {:?}", path);
                            }
                        }
                    }
                }
                if path.is_file()
                    && path.extension().and_then(|s| s.to_str()) == Some("node_monitor")
                {
                    // Extract the node ID from the file name
                    // e.g., "iox2_7109616707674936231170695157.node_monitor"
                    if let Some(name) = path.file_stem().and_then(|n| n.to_str()) {
                        let node_id = name.strip_prefix("iox2_").unwrap_or("");
                        let node_dir = fs::canonicalize("/tmp/iceoryx2/nodes")
                            .ok()
                            .map(|d| d.join(node_id));
                        if node_dir.as_ref().map_or(true, |d| !d.is_dir()) {
                            // Numeric node dir doesn't exist — this monitor is orphaned
                            if let Err(e) = fs::remove_file(&path) {
                                tracing::debug!(
                                    "Failed to remove orphaned node_monitor {:?}: {}",
                                    path,
                                    e
                                );
                            } else {
                                tracing::debug!("Removed orphaned node_monitor: {:?}", path);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Create a new channel registry
    ///
    /// **DEPRECATED**: Use `ChannelRegistry::global()` instead to ensure all nodes
    /// share the same iceoryx2 Node instance for proper IPC communication.
    pub fn new() -> Self {
        Self::new_internal()
    }

    /// Initialize iceoryx2 runtime
    #[cfg(feature = "multiprocess")]
    pub fn initialize(&mut self) -> Result<()> {
        // Skip if already initialized
        if self.node.is_some() {
            tracing::debug!("iceoryx2 node already initialized");
            return Ok(());
        }

        // Set log level
        set_log_level(LogLevel::Info);

        // Create iceoryx2 node
        let node = NodeBuilder::new()
            .create::<ipc::Service>()
            .map_err(|e| Error::IpcError(format!("Failed to create iceoryx2 node: {:?}", e)))?;

        self.node = Some(node);

        tracing::info!("iceoryx2 runtime initialized");
        Ok(())
    }

    /// Ensure the registry is initialized (for use in spawned threads)
    #[cfg(feature = "multiprocess")]
    pub async fn ensure_initialized(&self) -> Result<()> {
        if self.node.is_none() {
            return Err(Error::IpcError(
                "iceoryx2 node not initialized. Call ChannelRegistry::global() first on main thread.".to_string()
            ));
        }
        Ok(())
    }

    /// Create a new shared memory channel
    #[cfg(feature = "multiprocess")]
    pub async fn create_channel(
        &self,
        name: &str,
        capacity: usize,
        backpressure: bool,
    ) -> Result<ChannelHandle> {
        let node = self
            .node
            .as_ref()
            .ok_or_else(|| Error::IpcError("Node not initialized".to_string()))?;

        // Create service name from channel name
        let service_name = ServiceName::new(name)
            .map_err(|e| Error::IpcError(format!("Invalid service name: {:?}", e)))?;

        // Create publish-subscribe service for byte slices
        let service = node
            .service_builder(&service_name)
            .publish_subscribe::<[u8]>()
            .max_publishers(10)
            // One peer reads each channel; the second slot covers a restarted
            // peer while its predecessor's port is still registered.
            // iceoryx2 sizes every publisher's data segment for
            // `max_subscribers * (buffer + 2) + history` samples, and Windows
            // commits that shared memory up front: with 10 subscribers, a
            // capacity of 1000 and 512 KiB slices one channel reserved 5.4 GB.
            .max_subscribers(CHANNEL_MAX_SUBSCRIBERS)
            .history_size(capacity)
            .subscriber_max_buffer_size(capacity)
            .open_or_create()
            .map_err(|e| Error::IpcError(format!("Failed to create service: {:?}", e)))?;

        // Store the service to keep it alive
        let mut services = self.services.write().await;
        services.insert(name.to_string(), service);

        // Create channel handle
        let handle = ChannelHandle {
            name: name.to_string(),
            capacity,
            stats: Arc::new(RwLock::new(ChannelStats::default())),
            backpressure_enabled: backpressure,
        };

        let mut channels = self.channels.write().await;
        channels.insert(name.to_string(), handle.clone());

        tracing::info!(
            "Created iceoryx2 channel: {} (capacity: {})",
            name,
            capacity
        );
        Ok(handle)
    }

    /// Create the wake event of an existing channel.
    ///
    /// A channel's writer notifies it after each publish and the reader
    /// blocks on it, so neither side polls. Notifications latch: one that
    /// arrives before the reader starts waiting is delivered when it does.
    #[cfg(feature = "multiprocess")]
    pub async fn create_wake_event(&self, channel_name: &str) -> Result<()> {
        let node = self
            .node
            .as_ref()
            .ok_or_else(|| Error::IpcError("Node not initialized".to_string()))?;
        let name = wake_event_name(channel_name);
        let service_name = ServiceName::new(&name)
            .map_err(|e| Error::IpcError(format!("Invalid event service name: {:?}", e)))?;
        let service = node
            .service_builder(&service_name)
            .event()
            // The writer, plus a restarted peer whose old port is still registered.
            .max_notifiers(4)
            .max_listeners(2)
            .open_or_create()
            .map_err(|e| Error::IpcError(format!("Failed to create wake event: {:?}", e)))?;
        self.wake_events
            .write()
            .await
            .insert(channel_name.to_string(), service);
        Ok(())
    }

    /// Create the port that wakes `channel_name`'s reader. The port is not
    /// `Send`: create it on the thread that publishes.
    #[cfg(feature = "multiprocess")]
    pub async fn create_wake_notifier(&self, channel_name: &str) -> Result<WakeNotifier> {
        let events = self.wake_events.read().await;
        let service = events.get(channel_name).ok_or_else(|| {
            Error::IpcError(format!("Wake event not found for channel: {}", channel_name))
        })?;
        service
            .notifier_builder()
            .create()
            .map_err(|e| Error::IpcError(format!("Failed to create wake notifier: {:?}", e)))
    }

    /// Create the port `channel_name`'s reader blocks on. The port is not
    /// `Send`: create it on the thread that waits.
    #[cfg(feature = "multiprocess")]
    pub async fn create_wake_listener(&self, channel_name: &str) -> Result<WakeListener> {
        let events = self.wake_events.read().await;
        let service = events.get(channel_name).ok_or_else(|| {
            Error::IpcError(format!("Wake event not found for channel: {}", channel_name))
        })?;
        service
            .listener_builder()
            .create()
            .map_err(|e| Error::IpcError(format!("Failed to create wake listener: {:?}", e)))
    }

    /// Destroy a channel and cleanup resources
    ///
    /// This method:
    /// 1. Removes the service from the registry (drops all publishers/subscribers)
    /// 2. Removes the channel handle
    /// 3. When the service is dropped, iceoryx2 cleans up the shared memory
    #[cfg(feature = "multiprocess")]
    pub async fn destroy_channel(&self, channel: ChannelHandle) -> Result<()> {
        let channel_name = channel.name.clone();

        // CRITICAL: Remove the service FIRST to drop publishers/subscribers
        // When the service is dropped, iceoryx2 cleans up its shared memory segments
        {
            let mut services = self.services.write().await;
            if let Some(_service) = services.remove(&channel_name) {
                tracing::debug!("Removed iceoryx2 service for channel: {}", channel_name);
                // Service is dropped here, which triggers iceoryx2 cleanup
            }
        }
        self.wake_events.write().await.remove(&channel_name);

        // Then remove the channel handle
        let mut channels = self.channels.write().await;
        channels.remove(&channel_name);

        tracing::info!(
            "Destroyed channel and cleaned up iceoryx2 service: {}",
            channel_name
        );
        Ok(())
    }

    /// Discard a new subscriber's pending samples without decoding payloads.
    /// This releases this subscriber's borrows, not the publisher's history or
    /// another subscriber's queue. Producers must be quiesced for a final drain.
    #[cfg(feature = "multiprocess")]
    pub async fn drain_channel(&self, channel_name: &str) -> Result<usize> {
        let subscriber = self.create_subscriber(channel_name).await?;
        let count = subscriber.discard_pending()?;

        if count > 0 {
            tracing::info!(
                "Drained {} stale messages from channel: {}",
                count,
                channel_name
            );
        }

        Ok(count)
    }

    /// Create a publisher for a channel
    #[cfg(feature = "multiprocess")]
    pub async fn create_publisher(&self, channel_name: &str) -> Result<Publisher<'_>> {
        let services = self.services.read().await;
        let service = services
            .get(channel_name)
            .ok_or_else(|| Error::IpcError(format!("Channel not found: {}", channel_name)))?;

        // Start small and let the publisher grow its data segment, by powers
        // of two, when a larger message is actually sent. The segment holds
        // the channel's full sample count at the largest slice size, so a
        // large initial size is paid by every channel, including text-only ones.
        let publisher = service
            .publisher_builder()
            .initial_max_slice_len(INITIAL_SLICE_LEN)
            .allocation_strategy(AllocationStrategy::PowerOfTwo)
            .create()
            .map_err(|e| Error::IpcError(format!("Failed to create publisher: {:?}", e)))?;

        let channels = self.channels.read().await;
        let handle = channels.get(channel_name).ok_or_else(|| {
            Error::IpcError(format!("Channel handle not found: {}", channel_name))
        })?;

        Ok(Publisher {
            channel_name: channel_name.to_string(),
            inner: publisher,
            stats: handle.stats.clone(),
            backpressure: handle.backpressure_enabled,
            _lifetime: std::marker::PhantomData,
        })
    }

    /// Create a subscriber for a channel
    ///
    /// Note: By default, this creates a subscriber that can receive historical messages.
    /// Use `create_subscriber_fresh` if you want to skip any stale messages.
    #[cfg(feature = "multiprocess")]
    pub async fn create_subscriber(&self, channel_name: &str) -> Result<Subscriber> {
        let services = self.services.read().await;
        let service = services
            .get(channel_name)
            .ok_or_else(|| Error::IpcError(format!("Channel not found: {}", channel_name)))?;

        let channels = self.channels.read().await;
        let handle = channels.get(channel_name).ok_or_else(|| {
            Error::IpcError(format!("Channel handle not found: {}", channel_name))
        })?;

        // Create subscriber with buffer size to receive historical messages
        let subscriber = service
            .subscriber_builder()
            .buffer_size(handle.capacity) // Request history
            .create()
            .map_err(|e| Error::IpcError(format!("Failed to create subscriber: {:?}", e)))?;

        tracing::info!(
            "Created subscriber for '{}' with buffer_size={} to receive historical messages",
            channel_name,
            handle.capacity
        );

        Ok(Subscriber {
            channel_name: channel_name.to_string(),
            inner: subscriber,
            stats: handle.stats.clone(),
        })
    }

    /// Create a subscriber and immediately drain any stale messages
    ///
    /// Skips samples already available without parsing their wire format.
    /// Publisher history delivered on a later connection refresh is not skipped.
    /// Other subscriber queues and publisher history are unaffected. Producers
    /// must be quiesced if the caller needs a stable boundary for fresh traffic.
    #[cfg(feature = "multiprocess")]
    pub async fn create_subscriber_fresh(&self, channel_name: &str) -> Result<Subscriber> {
        let subscriber = self.create_subscriber(channel_name).await?;

        let stale_count = subscriber.discard_pending()?;

        if stale_count > 0 {
            tracing::warn!(
                "Drained {} stale messages from channel '{}' - previous session may not have been cleaned up properly",
                stale_count,
                channel_name
            );
        }

        Ok(subscriber)
    }

    /// Create a raw subscriber for control channels (bypasses RuntimeData deserialization)
    /// Used for simple control messages like READY signals
    #[cfg(feature = "multiprocess")]
    pub async fn create_raw_subscriber(
        &self,
        channel_name: &str,
    ) -> Result<iceoryx2::port::subscriber::Subscriber<ipc::Service, [u8], ()>> {
        let services = self.services.read().await;
        let service = services
            .get(channel_name)
            .ok_or_else(|| Error::IpcError(format!("Channel not found: {}", channel_name)))?;

        let channels = self.channels.read().await;
        let handle = channels.get(channel_name).ok_or_else(|| {
            Error::IpcError(format!("Channel handle not found: {}", channel_name))
        })?;

        // Create subscriber with buffer size to receive historical messages
        let subscriber = service
            .subscriber_builder()
            .buffer_size(handle.capacity) // Request history
            .create()
            .map_err(|e| Error::IpcError(format!("Failed to create raw subscriber: {:?}", e)))?;

        tracing::info!(
            "Created raw subscriber for '{}' with buffer_size={} to receive historical messages",
            channel_name,
            handle.capacity
        );

        Ok(subscriber)
    }
}

/// Publisher for sending data to a channel
#[cfg(feature = "multiprocess")]
pub struct Publisher<'a> {
    channel_name: String,
    inner: iceoryx2::port::publisher::Publisher<ipc::Service, [u8], ()>,
    stats: Arc<RwLock<ChannelStats>>,
    #[allow(dead_code)] // Reserved for future backpressure handling
    backpressure: bool,
    _lifetime: std::marker::PhantomData<&'a ()>,
}

#[cfg(feature = "multiprocess")]
impl<'a> Publisher<'a> {
    /// Publish data to the channel (synchronous - iceoryx2 is lock-free)
    pub fn publish(&self, data: &RuntimeData) -> Result<()> {
        self.publish_with_copy_context(data, CopyContext::legacy())
    }

    /// Authorize codec materialization and the final native wire transfer.
    pub fn publish_with_copy_context(
        &self,
        data: &RuntimeData,
        copies: &CopyContext,
    ) -> Result<()> {
        let bytes = super::data_transfer::to_bytes_with_copy_context(data, copies)
            .map_err(|e| Error::IpcError(format!("Failed to serialize: {e}")))?;
        self.send_with_copy_context(&bytes, copies)
    }

    /// Try to publish without blocking (returns false if would block)
    pub fn try_publish(&self, data: &RuntimeData) -> Result<bool> {
        // For now, just call publish - backpressure handling can be added later
        self.publish(data)?;
        Ok(true)
    }

    /// Send raw bytes directly (like READY signal) - bypasses RuntimeData serialization
    pub fn send(&self, bytes: &[u8]) -> Result<()> {
        self.send_with_copy_context(bytes, CopyContext::legacy())
    }

    /// Count the complete borrowed wire slice copied into the native loan.
    /// Includes framing/metadata, unlike logical portable-codec counters.
    /// A later send failure does not undo a transfer already performed.
    pub fn send_with_copy_context(&self, bytes: &[u8], copies: &CopyContext) -> Result<()> {
        tracing::debug!(
            "[IPC Publisher] Channel '{}' sending raw {} bytes",
            self.channel_name,
            bytes.len()
        );

        if bytes.len() > MAX_SLICE_LEN {
            return Err(Error::IpcError(format!(
                "Message too large: {} bytes (max: {})",
                bytes.len(),
                MAX_SLICE_LEN
            )));
        }

        let transfer = copies
            .begin(CopyReason::BorrowedInput, bytes.len() as u64)
            .map_err(|e| Error::IpcError(e.to_string()))?;

        // Loan uninitialized memory
        let sample = self
            .inner
            .loan_slice_uninit(bytes.len())
            .map_err(|e| Error::IpcError(format!("Failed to loan memory: {:?}", e)))?;

        // Write payload and send
        let sample = sample.write_from_slice(bytes);
        transfer.complete();
        sample
            .send()
            .map_err(|e| Error::IpcError(format!("Failed to send sample: {:?}", e)))?;

        tracing::debug!(
            "[IPC Publisher] Channel '{}' successfully sent raw {} bytes",
            self.channel_name,
            bytes.len()
        );

        // Update stats (use try_write to avoid blocking in async contexts)
        if let Ok(mut stats) = self.stats.try_write() {
            stats.messages_sent += 1;
            stats.bytes_transferred += bytes.len() as u64;
            stats.last_activity = Some(std::time::Instant::now());
        }
        // If lock is contended, skip stats update (non-critical)

        Ok(())
    }
}

/// Subscriber for receiving data from a channel
#[cfg(feature = "multiprocess")]
pub struct Subscriber {
    channel_name: String,
    inner: iceoryx2::port::subscriber::Subscriber<ipc::Service, [u8], ()>,
    stats: Arc<RwLock<ChannelStats>>,
}

#[cfg(feature = "multiprocess")]
impl Subscriber {
    /// Release currently available native samples without reading their payloads.
    /// Publisher history and other subscribers are unaffected. A temporarily
    /// empty queue is not EOF: publishers may deliver history on a later refresh.
    pub fn discard_pending(&self) -> Result<usize> {
        let mut count = 0;
        while self
            .inner
            .receive()
            .map_err(|e| Error::IpcError(format!("Failed to discard pending samples: {e:?}")))?
            .is_some()
        {
            count += 1;
        }
        Ok(count)
    }

    /// Receive data from the channel (synchronous - iceoryx2 is lock-free)
    pub fn receive(&self) -> Result<Option<RuntimeData>> {
        self.receive_with_copy_context(CopyContext::legacy())
    }

    /// Decode native MessagePack directly from the loan under caller authority.
    pub fn receive_with_copy_context(&self, copies: &CopyContext) -> Result<Option<RuntimeData>> {
        // Try to receive a sample
        match self.inner.receive() {
            Ok(Some(sample)) => {
                let bytes = sample.payload();

                tracing::debug!(
                    "[IPC Subscriber] Channel '{}' received {} bytes",
                    self.channel_name,
                    bytes.len()
                );

                // Deserialize RuntimeData from bytes via msgpack
                let data = super::data_transfer::from_bytes_with_copy_context(bytes, copies)
                    .map_err(|e| Error::IpcError(format!("Failed to deserialize: {}", e)))?;

                // Update stats (use try_write to avoid blocking in async contexts)
                if let Ok(mut stats) = self.stats.try_write() {
                    stats.messages_received += 1;
                    stats.bytes_transferred += bytes.len() as u64;
                    stats.last_activity = Some(std::time::Instant::now());
                }

                Ok(Some(data))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(Error::IpcError(format!("Failed to receive: {:?}", e))),
        }
    }

    /// Receive raw bytes without deserialization.
    /// Used by the Python IPC path where the wire format differs from the native msgpack encoding.
    pub fn receive_bytes(&self) -> Result<Option<Vec<u8>>> {
        self.receive_bytes_with_copy_context(CopyContext::legacy())
    }

    /// Snapshot a native loan once under explicit authority. Strict rejection
    /// consumes/releases the sample, but allocates no wire snapshot.
    /// Counts include the entire wire slice, including metadata and framing.
    pub fn receive_bytes_with_copy_context(&self, copies: &CopyContext) -> Result<Option<Vec<u8>>> {
        match self.inner.receive() {
            Ok(Some(sample)) => {
                let transfer = copies
                    .begin(CopyReason::PrivateIpcIngress, sample.payload().len() as u64)
                    .map_err(|e| Error::IpcError(e.to_string()))?;
                let bytes = sample.payload().to_vec();
                transfer.complete();

                tracing::trace!(
                    "[IPC Subscriber] Channel '{}' received {} raw bytes",
                    self.channel_name,
                    bytes.len()
                );

                // Update stats
                if let Ok(mut stats) = self.stats.try_write() {
                    stats.messages_received += 1;
                    stats.bytes_transferred += bytes.len() as u64;
                    stats.last_activity = Some(std::time::Instant::now());
                }

                Ok(Some(bytes))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(Error::IpcError(format!("Failed to receive: {:?}", e))),
        }
    }
    /// Move the one snapshot into a shareable owner; clones retain its payload.
    pub fn receive_shared_bytes_with_copy_context(
        &self,
        copies: &CopyContext,
    ) -> Result<Option<SharedBytes>> {
        self.receive_bytes_with_copy_context(copies)
            .map(|value| value.map(SharedBytes::from))
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[cfg(feature = "multiprocess")]
    async fn test_channel_creation() {
        let mut registry = ChannelRegistry::new();
        registry.initialize().unwrap();

        // Use unique name to avoid conflicts with other tests
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
    #[cfg(feature = "multiprocess")]
    async fn test_publish_subscribe() {
        let mut registry = ChannelRegistry::new();
        registry.initialize().unwrap();

        // Use unique name with timestamp to avoid conflicts
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
        let subscriber = registry.create_subscriber(&channel_name).await.unwrap();

        // Publish data
        let data = RuntimeData::Text("Hello, IPC!".into());
        publisher.publish(&data).unwrap();

        // Small delay to ensure message is received
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Receive data
        let received = subscriber.receive().unwrap();
        assert!(received.is_some());

        let received_data = received.unwrap();
        match &received_data {
            RuntimeData::Text(s) => assert_eq!(s.as_str(), "Hello, IPC!"),
            _ => panic!("Expected Text variant, got {:?}", received_data.data_type()),
        }

        registry.destroy_channel(channel).await.unwrap();
    }
}
