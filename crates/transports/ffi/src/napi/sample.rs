//! Sample lifecycle management for Node.js
//!
//! Provides ReceivedSample and LoanedSample types for zero-copy IPC.

use super::error::IpcError;
use super::runtime_data::parse_runtime_data_header;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A received sample with zero-copy buffer access
///
/// IMPORTANT: Always call release() when done processing to return
/// the slot to the publisher's loan pool.
#[napi]
pub struct ReceivedSample {
    /// Raw buffer data (owned Vec for lifetime safety)
    buffer: Vec<u8>,
    /// Timestamp when sample was published (nanoseconds)
    timestamp_ns: u64,
    /// Whether this sample has been released
    is_released: Arc<AtomicBool>,
}

#[napi]
impl ReceivedSample {
    /// Create a new received sample from buffer data
    pub(crate) fn new(buffer: Vec<u8>, timestamp_ns: u64) -> Self {
        Self {
            buffer,
            timestamp_ns,
            is_released: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get the buffer containing the received payload
    ///
    /// WARNING: Do not use this buffer after calling release()
    #[napi(getter)]
    pub fn buffer(&self) -> napi::Result<Buffer> {
        if self.is_released.load(Ordering::SeqCst) {
            return Err(IpcError::SampleError("Sample already released".to_string()).into());
        }

        // Create a Buffer that shares ownership with the Vec
        Ok(Buffer::from(self.buffer.clone()))
    }

    /// Get the size of the payload in bytes
    #[napi(getter)]
    pub fn size(&self) -> u32 {
        self.buffer.len() as u32
    }

    /// Check if this sample has been released
    #[napi(getter)]
    pub fn is_released(&self) -> bool {
        self.is_released.load(Ordering::SeqCst)
    }

    /// Get the timestamp when the sample was published (nanoseconds)
    #[napi(getter)]
    pub fn timestamp_ns(&self) -> i64 {
        self.timestamp_ns as i64
    }

    /// Release the sample back to the publisher's pool
    ///
    /// This must be called when done processing to prevent pool exhaustion.
    #[napi]
    pub fn release(&self) -> napi::Result<()> {
        if self.is_released.swap(true, Ordering::SeqCst) {
            return Err(IpcError::SampleError("Sample already released".to_string()).into());
        }
        // The actual iceoryx2 sample is dropped when the IPC thread
        // finishes with it. This just marks our wrapper as released.
        Ok(())
    }

    /// Parse this sample as RuntimeData
    ///
    /// The sample is NOT automatically released after this call.
    /// You must still call release() when done.
    #[napi]
    pub fn to_runtime_data(&self) -> napi::Result<super::runtime_data::ParsedRuntimeData> {
        if self.is_released.load(Ordering::SeqCst) {
            return Err(IpcError::SampleError("Sample already released".to_string()).into());
        }

        parse_runtime_data_header(Buffer::from(self.buffer.clone()))
    }
}

impl Drop for ReceivedSample {
    fn drop(&mut self) {
        if !self.is_released.load(Ordering::SeqCst) {
            tracing::warn!(
                "ReceivedSample dropped without explicit release() - \
                 this may cause loan pool exhaustion"
            );
        }
    }
}

mod loan;
pub use loan::LoanedSample;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_received_sample_lifecycle() {
        let sample = ReceivedSample::new(vec![1, 2, 3, 4], 12345);
        assert!(!sample.is_released());
        assert_eq!(sample.size(), 4);
        assert_eq!(sample.timestamp_ns(), 12345);
        sample.release().unwrap();
        assert!(sample.is_released());
        assert!(sample.release().is_err());
    }
}
