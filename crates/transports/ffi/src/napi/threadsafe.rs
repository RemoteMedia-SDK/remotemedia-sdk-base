//! ThreadsafeFunction helpers for cross-thread JavaScript callbacks
//!
//! This module provides utilities for safely calling JavaScript callbacks
//! from dedicated IPC threads where iceoryx2 publishers/subscribers live.
//!
//! # Background
//!
//! iceoryx2's `Publisher` and `Subscriber` types are `!Send` because they
//! contain `Rc<>` internals. This means they cannot be moved across thread
//! boundaries. To work around this, we:
//!
//! 1. Create dedicated OS threads for each subscriber/publisher
//! 2. Keep the iceoryx2 types on that thread for their entire lifetime
//! 3. Use `ThreadsafeFunction` to call JavaScript callbacks from these threads
//!
//! # Usage
//!
//! ```rust,ignore
//! use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
//!
//! // Create a threadsafe function from a JavaScript callback
//! let tsfn = create_data_callback(callback)?;
//!
//! // On the IPC thread, call the JavaScript callback
//! tsfn.call(sample_data, ThreadsafeFunctionCallMode::NonBlocking);
//! ```

use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Data passed to JavaScript callback for received samples
#[derive(Clone)]
pub struct SampleCallbackData {
    /// Raw bytes from iceoryx2 shared memory (copied to Vec for ownership transfer)
    pub buffer: Vec<u8>,
    /// Size of the payload
    #[allow(dead_code)]
    pub size: usize,
    /// Timestamp when sample was published (nanoseconds)
    pub timestamp_ns: u64,
}

/// Control handle for managing subscriber callbacks
pub struct CallbackHandle {
    /// Flag to signal the IPC thread to stop
    shutdown: Arc<AtomicBool>,
    /// Handle to the IPC thread
    thread_handle: Option<std::thread::JoinHandle<()>>,
}

impl CallbackHandle {
    /// Create a new callback handle
    pub fn new(shutdown: Arc<AtomicBool>, thread_handle: std::thread::JoinHandle<()>) -> Self {
        Self {
            shutdown,
            thread_handle: Some(thread_handle),
        }
    }

    /// Signal the IPC thread to stop and wait for it to finish
    pub fn stop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }

    /// Check if the callback is still active
    pub fn is_active(&self) -> bool {
        !self.shutdown.load(Ordering::SeqCst) && self.thread_handle.is_some()
    }
}

impl Drop for CallbackHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Type alias for data callback threadsafe function
#[allow(dead_code)]
pub type DataCallback = ThreadsafeFunction<SampleCallbackData, ErrorStrategy::Fatal>;

/// IPC polling configuration
pub struct PollingConfig {
    /// Whether to use yield_now() (low latency) or sleep (lower CPU)
    pub low_latency: bool,
    /// Sleep duration when not in low latency mode (microseconds)
    pub sleep_us: u64,
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            low_latency: true,
            sleep_us: 100,
        }
    }
}

/// Polling loop helper for IPC threads
///
/// This function implements the optimal polling strategy based on configuration.
/// For low-latency applications, use `yield_now()`. For lower CPU usage,
/// use a small sleep interval.
pub fn poll_once(config: &PollingConfig) {
    if config.low_latency {
        std::thread::yield_now();
    } else {
        std::thread::sleep(std::time::Duration::from_micros(config.sleep_us));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_polling_config_default() {
        let config = PollingConfig::default();
        assert!(config.low_latency);
        assert_eq!(config.sleep_us, 100);
    }

    #[test]
    fn test_callback_handle_shutdown() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        let handle = std::thread::spawn(move || {
            while !shutdown_clone.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
        });

        let mut callback_handle = CallbackHandle::new(shutdown, handle);
        assert!(callback_handle.is_active());

        callback_handle.stop();
        assert!(!callback_handle.is_active());
    }
}
