//! Zero-copy IPC subscriber for Node.js
//!
//! Provides subscription to iceoryx2 channels with callback support.
//!
//! # Architecture
//!
//! This module integrates with the core ChannelRegistry to ensure
//! Node.js can communicate with Python over the same iceoryx2 services.
//!
//! iceoryx2 `Subscriber` types are `!Send` because they contain `Rc<>` internals.
//! This means they cannot be moved across thread boundaries. To work around this:
//!
//! 1. We spawn a dedicated OS thread for each subscriber
//! 2. The iceoryx2 subscriber is created ON that thread via ChannelRegistry
//! 3. We use `ThreadsafeFunction` to call JavaScript callbacks from this thread
//! 4. The thread polls for messages and forwards them to JavaScript

use super::error::IpcError;
use super::sample::ReceivedSample;
use super::threadsafe::{CallbackHandle, PollingConfig, SampleCallbackData};
use iceoryx2::prelude::*;
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use remotemedia_core::python::multiprocess::ipc_channel::ChannelRegistry;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// IPC Subscriber for receiving zero-copy samples
#[napi]
pub struct NapiSubscriber {
    /// Channel name this subscriber is attached to
    channel_name: String,
    /// Buffer size for the subscriber
    buffer_size: usize,
    /// Whether the subscriber is still valid
    is_valid: Arc<AtomicBool>,
    /// Count of pending samples
    pending_count: Arc<AtomicUsize>,
    /// Active callback handle (if any)
    callback_handle: Option<CallbackHandle>,
}

#[napi]
impl NapiSubscriber {
    /// Create a new subscriber (internal use - use Channel.createSubscriber())
    pub(crate) fn new(channel_name: String, buffer_size: usize) -> napi::Result<Self> {
        Ok(Self {
            channel_name,
            buffer_size,
            is_valid: Arc::new(AtomicBool::new(true)),
            pending_count: Arc::new(AtomicUsize::new(0)),
            callback_handle: None,
        })
    }

    /// Get the channel name this subscriber is attached to
    #[napi(getter)]
    pub fn channel_name(&self) -> String {
        self.channel_name.clone()
    }

    /// Check if the subscriber is still valid
    #[napi(getter)]
    pub fn is_valid(&self) -> bool {
        self.is_valid.load(Ordering::SeqCst)
    }

    /// Get the number of samples waiting in the receive queue
    #[napi(getter)]
    pub fn pending_count(&self) -> u32 {
        self.pending_count.load(Ordering::SeqCst) as u32
    }

    /// Get the size of the receive buffer
    #[napi(getter)]
    pub fn buffer_size(&self) -> u32 {
        self.buffer_size as u32
    }

    /// Try to receive a sample (non-blocking)
    ///
    /// Returns null if no samples are available.
    #[napi]
    pub fn receive(&self) -> napi::Result<Option<ReceivedSample>> {
        if !self.is_valid() {
            return Err(IpcError::SubscriberError("Subscriber is closed".to_string()).into());
        }

        // TODO: Actually receive from iceoryx2 via dedicated IPC thread
        // For now, return None (no samples available)
        Ok(None)
    }

    /// Receive a sample with timeout
    ///
    /// Blocks until a sample is available or timeout expires.
    #[napi]
    pub async fn receive_timeout(&self, timeout_ms: u32) -> napi::Result<Option<ReceivedSample>> {
        if !self.is_valid() {
            return Err(IpcError::SubscriberError("Subscriber is closed".to_string()).into());
        }

        // TODO: Implement with tokio timeout and channel
        // For now, just wait and return None
        tokio::time::sleep(tokio::time::Duration::from_millis(timeout_ms as u64)).await;
        Ok(None)
    }

    /// Receive a sample (blocking until available)
    ///
    /// This is an async operation that doesn't block the event loop.
    #[napi]
    pub async fn receive_async(&self) -> napi::Result<ReceivedSample> {
        if !self.is_valid() {
            return Err(IpcError::SubscriberError("Subscriber is closed".to_string()).into());
        }

        // TODO: Implement with channel receiver
        // For now, return an error
        Err(IpcError::SubscriberError("No samples available".to_string()).into())
    }

    /// Register a callback for incoming samples
    ///
    /// The callback is invoked on the main thread for each received sample.
    /// This is the recommended pattern for streaming data.
    ///
    /// Returns an unsubscribe function that stops the callback when called.
    #[napi(ts_return_type = "() => void")]
    pub fn on_data(
        &mut self,
        env: Env,
        #[napi(ts_arg_type = "(sample: ReceivedSample) => void")] callback: JsFunction,
    ) -> napi::Result<JsFunction> {
        if !self.is_valid() {
            return Err(IpcError::SubscriberError("Subscriber is closed".to_string()).into());
        }

        // Create threadsafe function for cross-thread callback
        let tsfn: ThreadsafeFunction<
            SampleCallbackData,
            napi::threadsafe_function::ErrorStrategy::Fatal,
        > = callback.create_threadsafe_function(
            0,
            |ctx: napi::threadsafe_function::ThreadSafeCallContext<SampleCallbackData>| {
                // Convert SampleCallbackData to ReceivedSample
                let sample = ReceivedSample::new(ctx.value.buffer, ctx.value.timestamp_ns);

                // Pass to JavaScript callback
                Ok(vec![sample])
            },
        )?;

        // Set up the IPC polling thread
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        let channel_name = self.channel_name.clone();
        let buffer_size = self.buffer_size;
        let pending_count = self.pending_count.clone();

        // Get the global ChannelRegistry (ensures we share services with Python)
        let registry = ChannelRegistry::global();

        let thread_handle = std::thread::spawn(move || {
            let config = PollingConfig::default();

            // Create a tokio runtime for this thread to call async ChannelRegistry methods
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::error!(
                        "Failed to create tokio runtime for subscriber thread '{}': {:?}",
                        channel_name,
                        e
                    );
                    return;
                }
            };

            // Try to create subscriber via ChannelRegistry first
            let subscriber_result =
                rt.block_on(async { registry.create_subscriber(&channel_name).await });

            match subscriber_result {
                Ok(subscriber) => {
                    tracing::info!(
                        "iceoryx2 subscriber created for channel '{}' via ChannelRegistry with buffer_size={}",
                        channel_name,
                        buffer_size
                    );

                    // Polling loop using ChannelRegistry subscriber
                    poll_with_registry_subscriber(
                        subscriber,
                        &channel_name,
                        &tsfn,
                        &pending_count,
                        &shutdown_clone,
                        &config,
                    );
                }
                Err(e) => {
                    // Fallback to direct iceoryx2 if channel doesn't exist in registry
                    tracing::info!(
                        "ChannelRegistry subscriber creation failed for '{}': {:?}. Falling back to direct iceoryx2...",
                        channel_name,
                        e
                    );

                    // Create iceoryx2 node ON this thread (required because Node is !Send)
                    let node = match NodeBuilder::new().create::<ipc::Service>() {
                        Ok(node) => node,
                        Err(e) => {
                            tracing::error!(
                                "Failed to create iceoryx2 node for subscriber {}: {:?}",
                                channel_name,
                                e
                            );
                            return;
                        }
                    };

                    // Create service name from channel name
                    let service_name = match ServiceName::new(&channel_name) {
                        Ok(name) => name,
                        Err(e) => {
                            tracing::error!("Invalid service name '{}': {:?}", channel_name, e);
                            return;
                        }
                    };

                    // Open the existing service (created by Python/Rust publisher)
                    // Use open_or_create with matching config for compatibility with ChannelRegistry
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
                                "Failed to open/create service '{}': {:?}. Make sure a publisher exists.",
                                channel_name,
                                e
                            );
                            return;
                        }
                    };

                    // Create subscriber ON this thread (required because Subscriber is !Send)
                    let subscriber = match service
                        .subscriber_builder()
                        .buffer_size(buffer_size)
                        .create()
                    {
                        Ok(sub) => sub,
                        Err(e) => {
                            tracing::error!(
                                "Failed to create subscriber for '{}': {:?}",
                                channel_name,
                                e
                            );
                            return;
                        }
                    };

                    tracing::info!(
                        "iceoryx2 subscriber created for channel '{}' (direct fallback) with buffer_size={}",
                        channel_name,
                        buffer_size
                    );

                    // Polling loop - receive samples and forward to JavaScript
                    poll_with_direct_subscriber(
                        subscriber,
                        &channel_name,
                        &tsfn,
                        &pending_count,
                        &shutdown_clone,
                        &config,
                    );
                }
            }

            tracing::debug!("IPC polling thread for '{}' shutting down", channel_name);
        });

        // Store the shutdown signal so we can trigger it from the unsubscribe function
        let shutdown_for_unsubscribe = shutdown.clone();

        self.callback_handle = Some(CallbackHandle::new(shutdown, thread_handle));

        // Create unsubscribe function that sets the shutdown flag
        let unsubscribe_fn = env.create_function_from_closure("unsubscribe", move |_ctx| {
            shutdown_for_unsubscribe.store(true, Ordering::SeqCst);
            Ok(())
        })?;

        Ok(unsubscribe_fn)
    }

    /// Unsubscribe from data callbacks
    #[napi]
    pub fn unsubscribe(&mut self) {
        if let Some(mut handle) = self.callback_handle.take() {
            handle.stop();
        }
    }

    /// Close the subscriber and release resources
    #[napi]
    pub fn close(&mut self) {
        self.is_valid.store(false, Ordering::SeqCst);

        // Stop the callback thread if active
        if let Some(mut handle) = self.callback_handle.take() {
            handle.stop();
        }
    }
}

impl Drop for NapiSubscriber {
    fn drop(&mut self) {
        self.close();
    }
}

/// Helper function to poll using ChannelRegistry subscriber
fn poll_with_registry_subscriber(
    subscriber: remotemedia_core::python::multiprocess::ipc_channel::Subscriber,
    channel_name: &str,
    tsfn: &ThreadsafeFunction<SampleCallbackData, napi::threadsafe_function::ErrorStrategy::Fatal>,
    pending_count: &Arc<AtomicUsize>,
    shutdown: &Arc<AtomicBool>,
    config: &PollingConfig,
) {
    while !shutdown.load(Ordering::SeqCst) {
        // Try to receive a sample using ChannelRegistry subscriber
        match subscriber.receive_bytes() {
            Ok(Some(buffer)) => {
                let timestamp_ns = Instant::now().elapsed().as_nanos() as u64;
                let size = buffer.len();

                pending_count.fetch_add(1, Ordering::SeqCst);

                let callback_data = SampleCallbackData {
                    buffer,
                    size,
                    timestamp_ns,
                };

                // Call JavaScript callback (non-blocking)
                let status = tsfn.call(callback_data, ThreadsafeFunctionCallMode::NonBlocking);
                if status != napi::Status::Ok {
                    tracing::warn!(
                        "Failed to call JavaScript callback for channel '{}': {:?}",
                        channel_name,
                        status
                    );
                }

                pending_count.fetch_sub(1, Ordering::SeqCst);
            }
            Ok(None) => {
                // No samples available - yield to avoid busy-spinning
                super::threadsafe::poll_once(config);
            }
            Err(e) => {
                tracing::error!("Error receiving from channel '{}': {:?}", channel_name, e);
                // Small backoff on error
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }
}

/// Helper function to poll using direct iceoryx2 subscriber (fallback)
fn poll_with_direct_subscriber(
    subscriber: iceoryx2::port::subscriber::Subscriber<ipc::Service, [u8], ()>,
    channel_name: &str,
    tsfn: &ThreadsafeFunction<SampleCallbackData, napi::threadsafe_function::ErrorStrategy::Fatal>,
    pending_count: &Arc<AtomicUsize>,
    shutdown: &Arc<AtomicBool>,
    config: &PollingConfig,
) {
    while !shutdown.load(Ordering::SeqCst) {
        // Try to receive a sample
        match subscriber.receive() {
            Ok(Some(sample)) => {
                let payload = sample.payload();
                let timestamp_ns = Instant::now().elapsed().as_nanos() as u64;

                // Copy payload to owned Vec for cross-thread transfer
                // (iceoryx2 samples are borrowed from shared memory)
                let buffer = payload.to_vec();
                let size = buffer.len();

                pending_count.fetch_add(1, Ordering::SeqCst);

                let callback_data = SampleCallbackData {
                    buffer,
                    size,
                    timestamp_ns,
                };

                // Call JavaScript callback (non-blocking)
                let status = tsfn.call(callback_data, ThreadsafeFunctionCallMode::NonBlocking);
                if status != napi::Status::Ok {
                    tracing::warn!(
                        "Failed to call JavaScript callback for channel '{}': {:?}",
                        channel_name,
                        status
                    );
                }

                pending_count.fetch_sub(1, Ordering::SeqCst);
            }
            Ok(None) => {
                // No samples available - yield to avoid busy-spinning
                super::threadsafe::poll_once(config);
            }
            Err(e) => {
                tracing::error!("Error receiving from channel '{}': {:?}", channel_name, e);
                // Small backoff on error
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscriber_creation() {
        let sub = NapiSubscriber::new("test_channel".to_string(), 64).unwrap();
        assert_eq!(sub.channel_name(), "test_channel");
        assert_eq!(sub.buffer_size(), 64);
        assert!(sub.is_valid());
    }

    #[test]
    fn test_subscriber_close() {
        let mut sub = NapiSubscriber::new("test_channel".to_string(), 64).unwrap();
        sub.close();
        assert!(!sub.is_valid());
    }
}
