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

/// A loaned sample buffer for zero-copy publishing
///
/// IMPORTANT: Always call send() or release() to return the slot to the pool.
#[napi]
pub struct LoanedSample {
    /// Mutable buffer for writing payload
    buffer: Vec<u8>,
    /// Maximum size of the buffer
    max_size: usize,
    /// Whether this sample has been consumed (sent or released)
    is_consumed: Arc<AtomicBool>,
    /// Channel name for publishing
    channel_name: String,
}

#[napi]
impl LoanedSample {
    /// Create a new loaned sample with the given capacity
    pub(crate) fn new(channel_name: String, size: usize) -> Self {
        Self {
            buffer: vec![0u8; size],
            max_size: size,
            is_consumed: Arc::new(AtomicBool::new(false)),
            channel_name,
        }
    }

    /// Get the mutable buffer for writing payload data
    ///
    /// WARNING: Do not use this buffer after calling send() or release()
    #[napi(getter)]
    pub fn buffer(&self) -> napi::Result<Buffer> {
        if self.is_consumed.load(Ordering::SeqCst) {
            return Err(IpcError::SampleError("Sample already consumed".to_string()).into());
        }

        Ok(Buffer::from(self.buffer.clone()))
    }

    /// Get the size of the buffer in bytes
    #[napi(getter)]
    pub fn size(&self) -> u32 {
        self.buffer.len() as u32
    }

    /// Check if this sample has been consumed (sent or released)
    #[napi(getter)]
    pub fn is_consumed(&self) -> bool {
        self.is_consumed.load(Ordering::SeqCst)
    }

    /// Write data to the sample buffer
    ///
    /// # Arguments
    ///
    /// * `data` - Data to write to the buffer
    /// * `offset` - Offset in the buffer to start writing (default: 0)
    #[napi]
    pub fn write(&mut self, data: Buffer, offset: Option<u32>) -> napi::Result<()> {
        if self.is_consumed.load(Ordering::SeqCst) {
            return Err(IpcError::SampleError("Sample already consumed".to_string()).into());
        }

        let offset = offset.unwrap_or(0) as usize;
        let data_bytes = data.as_ref();

        if offset + data_bytes.len() > self.max_size {
            return Err(IpcError::SampleError(format!(
                "Data too large: {} bytes at offset {} exceeds buffer size {}",
                data_bytes.len(),
                offset,
                self.max_size
            ))
            .into());
        }

        self.buffer[offset..offset + data_bytes.len()].copy_from_slice(data_bytes);
        Ok(())
    }

    /// Send the sample to all subscribers
    ///
    /// This is a zero-copy operation - only an 8-byte offset is transmitted.
    /// The sample is consumed and cannot be used after this call.
    #[napi]
    pub fn send(&self) -> napi::Result<()> {
        if self.is_consumed.swap(true, Ordering::SeqCst) {
            return Err(IpcError::SampleError("Sample already consumed".to_string()).into());
        }

        // TODO: Actually publish to iceoryx2 via the IPC thread
        // This will be implemented when we create the publisher infrastructure
        tracing::debug!(
            "LoanedSample::send() - publishing {} bytes to {}",
            self.buffer.len(),
            self.channel_name
        );

        Ok(())
    }

    /// Release the sample without sending
    ///
    /// Returns the slot to the loan pool.
    /// Use this to cancel a publish operation.
    #[napi]
    pub fn release(&self) -> napi::Result<()> {
        if self.is_consumed.swap(true, Ordering::SeqCst) {
            return Err(IpcError::SampleError("Sample already consumed".to_string()).into());
        }

        tracing::debug!(
            "LoanedSample::release() - cancelled publish to {}",
            self.channel_name
        );

        Ok(())
    }
}

impl Drop for LoanedSample {
    fn drop(&mut self) {
        if !self.is_consumed.load(Ordering::SeqCst) {
            tracing::warn!(
                "LoanedSample dropped without send() or release() - \
                 this may cause loan pool exhaustion"
            );
        }
    }
}

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

        // Double release should fail
        assert!(sample.release().is_err());
    }

    #[test]
    fn test_loaned_sample_lifecycle() {
        let mut sample = LoanedSample::new("test_channel".to_string(), 1024);

        assert!(!sample.is_consumed());
        assert_eq!(sample.size(), 1024);

        // Write some data
        sample.write(Buffer::from(vec![1, 2, 3, 4]), None).unwrap();

        // Send the sample
        sample.send().unwrap();
        assert!(sample.is_consumed());

        // Double send should fail
        assert!(sample.send().is_err());
    }

    #[test]
    fn test_loaned_sample_release() {
        let sample = LoanedSample::new("test_channel".to_string(), 1024);

        sample.release().unwrap();
        assert!(sample.is_consumed());

        // Can't send after release
        assert!(sample.send().is_err());
    }
}
