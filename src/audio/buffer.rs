//! High-performance audio buffer types for zero-copy processing

use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Audio sample formats
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AudioFormat {
    /// 32-bit floating point samples (range: -1.0 to 1.0)
    F32,
    /// 16-bit signed integer samples (range: -32768 to 32767)
    I16,
    /// 32-bit signed integer samples
    I32,
}

/// High-performance audio buffer with zero-copy sharing
#[derive(Clone)]
pub enum AudioBuffer {
    /// 32-bit float buffer
    F32(Arc<Vec<f32>>),
    /// 16-bit integer buffer
    I16(Arc<Vec<i16>>),
    /// 32-bit integer buffer
    I32(Arc<Vec<i32>>),
}

impl AudioBuffer {
    /// Create a new F32 buffer (takes ownership)
    pub fn new_f32(data: Vec<f32>) -> Self {
        AudioBuffer::F32(Arc::new(data))
    }

    /// Create a new I16 buffer (takes ownership)
    pub fn new_i16(data: Vec<i16>) -> Self {
        AudioBuffer::I16(Arc::new(data))
    }

    /// Create a new I32 buffer (takes ownership)
    pub fn new_i32(data: Vec<i32>) -> Self {
        AudioBuffer::I32(Arc::new(data))
    }

    /// Create from Arc (zero-copy)
    pub fn from_arc_f32(data: Arc<Vec<f32>>) -> Self {
        AudioBuffer::F32(data)
    }

    /// Create from Arc (zero-copy)
    pub fn from_arc_i16(data: Arc<Vec<i16>>) -> Self {
        AudioBuffer::I16(data)
    }

    /// Create from Arc (zero-copy)
    pub fn from_arc_i32(data: Arc<Vec<i32>>) -> Self {
        AudioBuffer::I32(data)
    }

    /// Get format of this buffer
    pub fn format(&self) -> AudioFormat {
        match self {
            AudioBuffer::F32(_) => AudioFormat::F32,
            AudioBuffer::I16(_) => AudioFormat::I16,
            AudioBuffer::I32(_) => AudioFormat::I32,
        }
    }

    /// Get number of samples (total across all channels)
    pub fn len(&self) -> usize {
        match self {
            AudioBuffer::F32(data) => data.len(),
            AudioBuffer::I16(data) => data.len(),
            AudioBuffer::I32(data) => data.len(),
        }
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get as f32 slice if format matches
    pub fn as_f32(&self) -> Option<&[f32]> {
        match self {
            AudioBuffer::F32(data) => Some(data.as_ref()),
            _ => None,
        }
    }

    /// Get as i16 slice if format matches
    pub fn as_i16(&self) -> Option<&[i16]> {
        match self {
            AudioBuffer::I16(data) => Some(data.as_ref()),
            _ => None,
        }
    }

    /// Get as i32 slice if format matches
    pub fn as_i32(&self) -> Option<&[i32]> {
        match self {
            AudioBuffer::I32(data) => Some(data.as_ref()),
            _ => None,
        }
    }

    /// Clone the underlying data (not zero-copy)
    pub fn to_vec_f32(&self) -> Option<Vec<f32>> {
        self.as_f32().map(|s| s.to_vec())
    }

    /// Clone the underlying data (not zero-copy)
    pub fn to_vec_i16(&self) -> Option<Vec<i16>> {
        self.as_i16().map(|s| s.to_vec())
    }

    /// Clone the underlying data (not zero-copy)
    pub fn to_vec_i32(&self) -> Option<Vec<i32>> {
        self.as_i32().map(|s| s.to_vec())
    }
}

impl std::fmt::Debug for AudioBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AudioBuffer::F32(data) => write!(f, "AudioBuffer::F32({} samples)", data.len()),
            AudioBuffer::I16(data) => write!(f, "AudioBuffer::I16({} samples)", data.len()),
            AudioBuffer::I32(data) => write!(f, "AudioBuffer::I32({} samples)", data.len()),
        }
    }
}

/// Audio data with metadata
#[derive(Debug, Clone)]
pub struct AudioData {
    /// Audio buffer (zero-copy shareable)
    pub buffer: AudioBuffer,
    /// Sample rate in Hz
    pub sample_rate: u32,
    /// Number of channels
    pub channels: usize,
}

impl AudioData {
    /// Create new audio data
    pub fn new(buffer: AudioBuffer, sample_rate: u32, channels: usize) -> Self {
        AudioData {
            buffer,
            sample_rate,
            channels,
        }
    }

    /// Get number of samples per channel
    pub fn samples_per_channel(&self) -> usize {
        self.buffer.len() / self.channels
    }

    /// Get duration in seconds
    pub fn duration_secs(&self) -> f64 {
        self.samples_per_channel() as f64 / self.sample_rate as f64
    }
}
