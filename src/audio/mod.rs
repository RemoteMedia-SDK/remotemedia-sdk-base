//! Audio data structures and utilities for zero-copy processing
//!
//! This module provides efficient audio data handling with:
//! - Zero-copy data transfer via Arc<Vec<T>>
//! - Multiple audio format support (F32, I16, I32)
//! - Sample rate and channel configuration
//! - Safe format conversions

pub mod buffer;
pub mod format;

pub use buffer::{AudioBuffer as AudioBufferNew, AudioData};
use std::sync::Arc;

/// Audio sample format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioFormat {
    /// 32-bit floating point samples (range: -1.0 to 1.0)
    F32,
    /// 16-bit signed integer samples (range: -32768 to 32767)
    I16,
    /// 32-bit signed integer samples (range: -2147483648 to 2147483647)
    I32,
}

/// Audio buffer with zero-copy semantics
///
/// Uses Arc<Vec<T>> for efficient sharing without copying.
/// Samples are stored in interleaved format (e.g., L, R, L, R for stereo).
#[derive(Debug, Clone)]
pub struct AudioBuffer {
    /// Audio sample data (interleaved channels)
    data: Arc<Vec<f32>>,

    /// Sample rate in Hz
    sample_rate: u32,

    /// Number of channels (1 = mono, 2 = stereo, etc.)
    channels: u16,

    /// Audio format
    format: AudioFormat,
}

impl AudioBuffer {
    /// Create a new audio buffer
    ///
    /// # Arguments
    /// * `data` - Audio samples (interleaved if multi-channel)
    /// * `sample_rate` - Sample rate in Hz
    /// * `channels` - Number of audio channels
    /// * `format` - Audio sample format
    ///
    /// # Example
    /// ```
    /// use remotemedia_core::audio::{AudioBuffer, AudioFormat};
    /// use std::sync::Arc;
    ///
    /// let samples = vec![0.0, 0.5, 1.0, 0.5];
    /// let buffer = AudioBuffer::new(Arc::new(samples), 48000, 1, AudioFormat::F32);
    /// assert_eq!(buffer.len_samples(), 4);
    /// ```
    pub fn new(data: Arc<Vec<f32>>, sample_rate: u32, channels: u16, format: AudioFormat) -> Self {
        Self {
            data,
            sample_rate,
            channels,
            format,
        }
    }

    /// Get the total number of samples (including all channels)
    pub fn len_samples(&self) -> usize {
        self.data.len()
    }

    /// Get the number of frames (samples per channel)
    ///
    /// For stereo audio with 1000 samples, this returns 500 frames.
    pub fn len_frames(&self) -> usize {
        self.data.len() / self.channels as usize
    }

    /// Get the duration in seconds
    pub fn duration_secs(&self) -> f64 {
        self.len_frames() as f64 / self.sample_rate as f64
    }

    /// Get a slice of the audio data (zero-copy)
    pub fn as_slice(&self) -> &[f32] {
        &self.data
    }

    /// Get mutable access to the data (clones if shared)
    ///
    /// This uses Arc::make_mut() which will clone the data only if
    /// there are multiple references to it (copy-on-write).
    pub fn make_mut(&mut self) -> &mut Vec<f32> {
        Arc::make_mut(&mut self.data)
    }

    /// Get the sample rate in Hz
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    /// Get the number of channels
    pub fn channels(&self) -> u16 {
        self.channels
    }

    /// Get the audio format
    pub fn format(&self) -> AudioFormat {
        self.format
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get a clone of the underlying Arc (zero-copy)
    ///
    /// This is useful for sharing the buffer without copying data.
    pub fn data_arc(&self) -> Arc<Vec<f32>> {
        Arc::clone(&self.data)
    }

    /// Create a buffer from a raw Vec (moves data into Arc)
    pub fn from_vec(data: Vec<f32>, sample_rate: u32, channels: u16, format: AudioFormat) -> Self {
        Self::new(Arc::new(data), sample_rate, channels, format)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audio_buffer_creation() {
        let samples = vec![0.0, 0.5, 1.0, 0.5];
        let buffer = AudioBuffer::from_vec(samples, 48000, 1, AudioFormat::F32);

        assert_eq!(buffer.len_samples(), 4);
        assert_eq!(buffer.len_frames(), 4);
        assert_eq!(buffer.sample_rate(), 48000);
        assert_eq!(buffer.channels(), 1);
        assert_eq!(buffer.format(), AudioFormat::F32);
    }

    #[test]
    fn test_audio_buffer_stereo() {
        // Stereo audio: L, R, L, R, L, R, L, R
        let samples = vec![0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7];
        let buffer = AudioBuffer::from_vec(samples, 44100, 2, AudioFormat::F32);

        assert_eq!(buffer.len_samples(), 8);
        assert_eq!(buffer.len_frames(), 4); // 4 frames with 2 channels each
        assert_eq!(buffer.channels(), 2);
    }

    #[test]
    fn test_audio_buffer_duration() {
        let samples = vec![0.0; 48000]; // 1 second at 48kHz
        let buffer = AudioBuffer::from_vec(samples, 48000, 1, AudioFormat::F32);

        assert_eq!(buffer.duration_secs(), 1.0);
    }

    #[test]
    fn test_audio_buffer_as_slice() {
        let samples = vec![0.1, 0.2, 0.3];
        let buffer = AudioBuffer::from_vec(samples, 48000, 1, AudioFormat::F32);

        let slice = buffer.as_slice();
        assert_eq!(slice.len(), 3);
        assert_eq!(slice[0], 0.1);
        assert_eq!(slice[1], 0.2);
        assert_eq!(slice[2], 0.3);
    }

    #[test]
    fn test_audio_buffer_zero_copy_clone() {
        let samples = vec![1.0, 2.0, 3.0];
        let buffer1 = AudioBuffer::from_vec(samples, 48000, 1, AudioFormat::F32);
        let buffer2 = buffer1.clone();

        // Both buffers should share the same Arc
        assert!(Arc::ptr_eq(&buffer1.data_arc(), &buffer2.data_arc()));
    }

    #[test]
    fn test_audio_buffer_make_mut() {
        let samples = vec![1.0, 2.0, 3.0];
        let mut buffer1 = AudioBuffer::from_vec(samples, 48000, 1, AudioFormat::F32);
        let buffer2 = buffer1.clone();

        // Before mutation, they share data
        assert!(Arc::ptr_eq(&buffer1.data_arc(), &buffer2.data_arc()));

        // After mutation, buffer1 gets its own copy
        let data_mut = buffer1.make_mut();
        data_mut[0] = 99.0;

        // Now they have different data
        assert!(!Arc::ptr_eq(&buffer1.data_arc(), &buffer2.data_arc()));
        assert_eq!(buffer1.as_slice()[0], 99.0);
        assert_eq!(buffer2.as_slice()[0], 1.0);
    }
}
