//! Zero-copy IPC publisher for Node.js
//!
//! Provides publishing to iceoryx2 channels with loan/send pattern.
//!
//! # Architecture
//!
//! This module integrates with the core ChannelRegistry to ensure
//! Node.js can communicate with Python over the same iceoryx2 services.
//!
//! iceoryx2 `Publisher` types are `!Send` because they contain `Rc<>` internals.
//! This means they cannot be moved across thread boundaries. To work around this:
//!
//! 1. We spawn a dedicated OS thread for each publisher
//! 2. The iceoryx2 publisher is created ON that thread via ChannelRegistry
//! 3. We use channels to send publish commands to this thread
//! 4. The thread processes publish requests and sends data via iceoryx2

use super::error::IpcError;
use super::pipeline::NapiRuntimeData;
use super::sample::LoanedSample;
use iceoryx2::prelude::*;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use remotemedia_core::python::multiprocess::ipc_channel::ChannelRegistry;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::Arc;

//

/// Commands sent to the IPC publisher thread
enum PublishCommand {
    /// Publish data bytes
    Publish(Vec<u8>),
    /// Shutdown the publisher thread
    Shutdown,
}

/// Handle to the publisher thread
struct PublisherThreadHandle {
    /// Channel to send commands to the thread
    command_tx: Sender<PublishCommand>,
    /// Thread handle
    thread_handle: Option<std::thread::JoinHandle<()>>,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
}

impl PublisherThreadHandle {
    fn new(
        command_tx: Sender<PublishCommand>,
        thread_handle: std::thread::JoinHandle<()>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            command_tx,
            thread_handle: Some(thread_handle),
            shutdown,
        }
    }

    fn stop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.command_tx.send(PublishCommand::Shutdown);
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for PublisherThreadHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// IPC Publisher for zero-copy sample publishing
#[napi]
pub struct NapiPublisher {
    /// Channel name this publisher is attached to
    channel_name: String,
    /// Maximum payload size
    max_payload_size: usize,
    /// Maximum number of loans
    max_loans: usize,
    /// Whether the publisher is still valid
    is_valid: Arc<AtomicBool>,
    /// Current number of loaned samples
    loaned_count: Arc<AtomicUsize>,
    /// Handle to the publisher thread (created on first publish)
    thread_handle: Option<PublisherThreadHandle>,
}

#[napi]
impl NapiPublisher {
    /// Create a new publisher (internal use - use Channel.createPublisher())
    pub(crate) fn new(
        channel_name: String,
        max_payload_size: usize,
        max_loans: usize,
    ) -> napi::Result<Self> {
        Ok(Self {
            channel_name,
            max_payload_size,
            max_loans,
            is_valid: Arc::new(AtomicBool::new(true)),
            loaned_count: Arc::new(AtomicUsize::new(0)),
            thread_handle: None,
        })
    }

    /// Get the channel name this publisher is attached to
    #[napi(getter)]
    pub fn channel_name(&self) -> String {
        self.channel_name.clone()
    }

    /// Check if the publisher is still valid
    #[napi(getter)]
    pub fn is_valid(&self) -> bool {
        self.is_valid.load(Ordering::SeqCst)
    }

    /// Get the number of currently loaned samples
    #[napi(getter)]
    pub fn loaned_count(&self) -> u32 {
        self.loaned_count.load(Ordering::SeqCst) as u32
    }

    /// Get the maximum number of loans allowed
    #[napi(getter)]
    pub fn max_loans(&self) -> u32 {
        self.max_loans as u32
    }

    /// Ensure the publisher thread is running
    ///
    /// This creates a dedicated OS thread for iceoryx2 operations because
    /// Publisher types are !Send. We use ChannelRegistry::global() to ensure
    /// Node.js uses the same iceoryx2 services as Python.
    fn ensure_thread(&mut self) -> napi::Result<&Sender<PublishCommand>> {
        if self.thread_handle.is_none() {
            let (tx, rx) = mpsc::channel::<PublishCommand>();
            let shutdown = Arc::new(AtomicBool::new(false));
            let shutdown_clone = shutdown.clone();
            let channel_name = self.channel_name.clone();
            let max_payload_size = self.max_payload_size;

            // Get the global ChannelRegistry (ensures we share services with Python)
            let registry = ChannelRegistry::global();

            let thread_handle = std::thread::spawn(move || {
                // Create a tokio runtime for this thread to call async ChannelRegistry methods
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        tracing::error!(
                            "Failed to create tokio runtime for publisher thread '{}': {:?}",
                            channel_name,
                            e
                        );
                        return;
                    }
                };

                // Create channel via ChannelRegistry (ensures compatible with Python)
                // Use reasonable defaults: capacity=64, backpressure=false
                let channel_result =
                    rt.block_on(async { registry.create_channel(&channel_name, 64, false).await });

                if let Err(e) = channel_result {
                    tracing::warn!(
                        "Channel '{}' may already exist (from Python?): {:?}. Attempting to open...",
                        channel_name,
                        e
                    );
                }

                // Create publisher via ChannelRegistry
                let publisher = match rt
                    .block_on(async { registry.create_publisher(&channel_name).await })
                {
                    Ok(pub_instance) => pub_instance,
                    Err(e) => {
                        // If channel doesn't exist, create it with default config and retry
                        tracing::info!(
                            "Publisher creation failed, creating channel '{}' first: {:?}",
                            channel_name,
                            e
                        );

                        // Try to create the channel with iceoryx2 directly as fallback
                        let node = match NodeBuilder::new().create::<ipc::Service>() {
                            Ok(node) => node,
                            Err(e) => {
                                tracing::error!(
                                    "Failed to create iceoryx2 node for publisher {}: {:?}",
                                    channel_name,
                                    e
                                );
                                return;
                            }
                        };

                        let service_name = match ServiceName::new(&channel_name) {
                            Ok(name) => name,
                            Err(e) => {
                                tracing::error!("Invalid service name '{}': {:?}", channel_name, e);
                                return;
                            }
                        };

                        // Match ChannelRegistry's service configuration for compatibility
                        let service = match node
                            .service_builder(&service_name)
                            .publish_subscribe::<[u8]>()
                            .max_publishers(10)
                            .max_subscribers(10)
                            .history_size(64)
                            .subscriber_max_buffer_size(64)
                            .open_or_create()
                        {
                            Ok(service) => service,
                            Err(e) => {
                                tracing::error!(
                                    "Failed to create/open service '{}': {:?}",
                                    channel_name,
                                    e
                                );
                                return;
                            }
                        };

                        // Create publisher with matching config
                        match service
                            .publisher_builder()
                            .initial_max_slice_len(max_payload_size)
                            .allocation_strategy(AllocationStrategy::PowerOfTwo)
                            .create()
                        {
                            Ok(pub_instance) => {
                                tracing::info!(
                                    "iceoryx2 publisher created for channel '{}' (direct fallback)",
                                    channel_name
                                );
                                // Store in thread-local and process commands
                                process_publish_commands(
                                    pub_instance,
                                    &channel_name,
                                    rx,
                                    shutdown_clone,
                                );
                                return;
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to create publisher for '{}': {:?}",
                                    channel_name,
                                    e
                                );
                                return;
                            }
                        }
                    }
                };

                tracing::info!(
                    "iceoryx2 publisher created for channel '{}' via ChannelRegistry with max_payload_size={}",
                    channel_name,
                    max_payload_size
                );

                // Process commands until shutdown using the ChannelRegistry publisher
                while !shutdown_clone.load(Ordering::SeqCst) {
                    match rx.recv() {
                        Ok(PublishCommand::Publish(data)) => {
                            // Use ChannelRegistry publisher's send method for raw bytes
                            if let Err(e) = publisher.send(&data) {
                                tracing::error!(
                                    "Failed to send sample on channel '{}': {:?}",
                                    channel_name,
                                    e
                                );
                            }
                        }
                        Ok(PublishCommand::Shutdown) => {
                            break;
                        }
                        Err(_) => {
                            // Channel closed
                            break;
                        }
                    }
                }

                tracing::debug!("IPC publisher thread for '{}' shutting down", channel_name);
            });

            self.thread_handle = Some(PublisherThreadHandle::new(tx, thread_handle, shutdown));
        }

        Ok(&self.thread_handle.as_ref().unwrap().command_tx)
    }

    /// Loan a sample buffer for zero-copy publishing
    ///
    /// The returned buffer is uninitialized - you must write the full payload.
    ///
    /// # Arguments
    ///
    /// * `size` - Required payload size in bytes
    #[napi]
    pub fn loan(&mut self, size: u32) -> napi::Result<LoanedSample> {
        if !self.is_valid() {
            return Err(IpcError::PublisherError("Publisher is closed".to_string()).into());
        }

        let size = size as usize;

        if size > self.max_payload_size {
            return Err(IpcError::PublisherError(format!(
                "Requested size {} exceeds max payload size {}",
                size, self.max_payload_size
            ))
            .into());
        }

        let current = self.loaned_count.load(Ordering::SeqCst);
        if current >= self.max_loans {
            return Err(IpcError::ResourceExhausted(format!(
                "Loan pool exhausted: {} of {} slots in use",
                current, self.max_loans
            ))
            .into());
        }

        self.loaned_count.fetch_add(1, Ordering::SeqCst);

        // Ensure thread is running (for when send() is called)
        self.ensure_thread()?;

        // Create a local buffer - actual publishing happens when send() is called
        Ok(LoanedSample::new(self.channel_name.clone(), size))
    }

    /// Try to loan a sample buffer, returning null if no slots available
    ///
    /// Non-blocking alternative to loan().
    #[napi]
    pub fn try_loan(&mut self, size: u32) -> napi::Result<Option<LoanedSample>> {
        if !self.is_valid() {
            return Err(IpcError::PublisherError("Publisher is closed".to_string()).into());
        }

        let size = size as usize;

        if size > self.max_payload_size {
            return Err(IpcError::PublisherError(format!(
                "Requested size {} exceeds max payload size {}",
                size, self.max_payload_size
            ))
            .into());
        }

        let current = self.loaned_count.load(Ordering::SeqCst);
        if current >= self.max_loans {
            return Ok(None);
        }

        self.loaned_count.fetch_add(1, Ordering::SeqCst);

        // Ensure thread is running
        self.ensure_thread()?;

        Ok(Some(LoanedSample::new(self.channel_name.clone(), size)))
    }

    /// Publish RuntimeData to the channel
    ///
    /// This method handles serialization internally, converting the RuntimeData
    /// to IPC format for zero-copy transfer.
    #[napi]
    pub fn publish(&mut self, data: &NapiRuntimeData) -> napi::Result<()> {
        if !self.is_valid() {
            return Err(IpcError::PublisherError("Publisher is closed".to_string()).into());
        }

        // Convert RuntimeData to IPC format and serialize
        let data_bytes = remotemedia_core::multiprocess::data_transfer::to_bytes(data.get_inner())
            .map_err(|e| napi::Error::from_reason(format!("Serialization error: {}", e)))?;

        if data_bytes.len() > self.max_payload_size {
            return Err(IpcError::PublisherError(format!(
                "Data size {} exceeds max payload size {}",
                data_bytes.len(),
                self.max_payload_size
            ))
            .into());
        }

        // Ensure thread is running and get command channel
        let tx = self.ensure_thread()?;

        // Send publish command to the dedicated thread
        tx.send(PublishCommand::Publish(data_bytes)).map_err(|e| {
            IpcError::PublisherError(format!("Failed to send publish command: {}", e))
        })?;

        Ok(())
    }

    /// Publish raw bytes to the channel (for advanced use cases)
    #[napi]
    pub fn publish_raw(&mut self, data: Buffer) -> napi::Result<()> {
        if !self.is_valid() {
            return Err(IpcError::PublisherError("Publisher is closed".to_string()).into());
        }

        let data_bytes = data.as_ref();
        if data_bytes.len() > self.max_payload_size {
            return Err(IpcError::PublisherError(format!(
                "Data size {} exceeds max payload size {}",
                data_bytes.len(),
                self.max_payload_size
            ))
            .into());
        }

        // Ensure thread is running and get command channel
        let tx = self.ensure_thread()?;

        // Send publish command to the dedicated thread
        tx.send(PublishCommand::Publish(data_bytes.to_vec()))
            .map_err(|e| {
                IpcError::PublisherError(format!("Failed to send publish command: {}", e))
            })?;

        Ok(())
    }

    /// Close the publisher and release resources
    ///
    /// Any loaned samples become invalid.
    #[napi]
    pub fn close(&mut self) {
        self.is_valid.store(false, Ordering::SeqCst);
        if let Some(mut handle) = self.thread_handle.take() {
            handle.stop();
        }
    }
}

impl Drop for NapiPublisher {
    fn drop(&mut self) {
        self.close();
    }
}

/// Helper function to process publish commands with a direct iceoryx2 publisher
/// Used in fallback path when ChannelRegistry publisher creation fails
fn process_publish_commands(
    publisher: iceoryx2::port::publisher::Publisher<ipc::Service, [u8], ()>,
    channel_name: &str,
    rx: mpsc::Receiver<PublishCommand>,
    shutdown: Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::SeqCst) {
        match rx.recv() {
            Ok(PublishCommand::Publish(data)) => {
                // Loan, write, and send
                match publisher.loan_slice_uninit(data.len()) {
                    Ok(sample) => {
                        let sample = sample.write_from_slice(&data);
                        if let Err(e) = sample.send() {
                            tracing::error!(
                                "Failed to send sample on channel '{}': {:?}",
                                channel_name,
                                e
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to loan memory on channel '{}': {:?}",
                            channel_name,
                            e
                        );
                    }
                }
            }
            Ok(PublishCommand::Shutdown) => {
                break;
            }
            Err(_) => {
                // Channel closed
                break;
            }
        }
    }

    tracing::debug!(
        "IPC publisher thread for '{}' shutting down (direct)",
        channel_name
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_publisher_creation() {
        let pub_instance = NapiPublisher::new("test_channel".to_string(), 1_048_576, 64).unwrap();
        assert_eq!(pub_instance.channel_name(), "test_channel");
        assert_eq!(pub_instance.max_loans(), 64);
        assert!(pub_instance.is_valid());
    }

    #[test]
    fn test_publisher_loan() {
        let mut pub_instance = NapiPublisher::new("test_channel".to_string(), 1024, 2).unwrap();

        let _sample1 = pub_instance.loan(100).unwrap();
        assert_eq!(pub_instance.loaned_count(), 1);

        let _sample2 = pub_instance.loan(100).unwrap();
        assert_eq!(pub_instance.loaned_count(), 2);

        // Third loan should fail
        let result = pub_instance.loan(100);
        assert!(result.is_err());
    }

    #[test]
    fn test_publisher_try_loan() {
        let mut pub_instance = NapiPublisher::new("test_channel".to_string(), 1024, 1).unwrap();

        let sample1 = pub_instance.try_loan(100).unwrap();
        assert!(sample1.is_some());

        let sample2 = pub_instance.try_loan(100).unwrap();
        assert!(sample2.is_none());
    }

    #[test]
    fn test_publisher_size_limit() {
        let mut pub_instance = NapiPublisher::new("test_channel".to_string(), 100, 64).unwrap();

        let result = pub_instance.loan(200);
        assert!(result.is_err());
    }
}
