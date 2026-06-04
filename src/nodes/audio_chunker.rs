//! Audio Chunker Node
//!
//! This node splits incoming audio into fixed-size chunks. It's useful for:
//! - Feeding audio to models that require specific chunk sizes (e.g., Silero VAD needs 512 samples)
//! - Rate limiting or batching audio processing
//! - Buffering partial chunks until enough samples are available
//!
//! Features:
//! - Configurable chunk size
//! - Maintains state per session to handle partial chunks
//! - Streaming output - emits chunks as they become available

use crate::data::AudioBuffer as ProtoAudioBuffer;
use crate::data::RuntimeData;
use crate::error::{Error, Result};
use crate::nodes::SyncStreamingNode;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Audio Chunker Node configuration
///
/// Configuration for the audio chunker streaming node. Uses `#[serde(default)]` to allow
/// partial config, and `#[serde(alias)]` to accept both snake_case and camelCase.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct AudioChunkerConfig {
    /// Target chunk size in samples (e.g., 512 for Silero VAD)
    #[serde(alias = "chunkSize")]
    #[schemars(range(min = 1, max = 65536))]
    pub chunk_size: usize,
}

impl Default for AudioChunkerConfig {
    fn default() -> Self {
        Self { chunk_size: 512 }
    }
}

/// Chunker state for a session
#[derive(Debug, Clone)]
struct ChunkerState {
    /// Buffered samples waiting to form a complete chunk
    buffer: Vec<f32>,
    /// Sample rate of buffered audio
    sample_rate: u32,
    /// Number of channels in buffered audio
    channels: u32,
    /// Audio format (1 = F32, per protobuf AudioFormat enum)
    format: i32,
}

impl Default for ChunkerState {
    fn default() -> Self {
        Self {
            buffer: Vec::new(),
            sample_rate: 16000,
            channels: 1,
            format: 1, // AUDIO_FORMAT_F32
        }
    }
}

/// Audio Chunker Node
pub struct AudioChunkerNode {
    /// Target chunk size in samples
    chunk_size: usize,
    /// State per session (buffers partial chunks)
    states: Arc<Mutex<std::collections::HashMap<String, ChunkerState>>>,
}

impl AudioChunkerNode {
    /// Create a new AudioChunkerNode with the given configuration
    pub fn with_config(config: AudioChunkerConfig) -> Self {
        Self {
            chunk_size: config.chunk_size,
            states: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Create a new AudioChunkerNode with optional chunk size (legacy API)
    pub fn new(chunk_size: Option<usize>) -> Self {
        Self::with_config(AudioChunkerConfig {
            chunk_size: chunk_size.unwrap_or(512),
        })
    }

    /// Convert ProtoAudioBuffer samples to f32 vector
    #[allow(dead_code)] // Reserved for audio format conversion utilities
    fn samples_to_f32(&self, audio_buf: &ProtoAudioBuffer) -> Result<Vec<f32>> {
        match audio_buf.format {
            1 => {
                // AUDIO_FORMAT_F32
                let sample_count = audio_buf.samples.len() / 4;
                Ok((0..sample_count)
                    .map(|i| {
                        let offset = i * 4;
                        f32::from_le_bytes([
                            audio_buf.samples[offset],
                            audio_buf.samples[offset + 1],
                            audio_buf.samples[offset + 2],
                            audio_buf.samples[offset + 3],
                        ])
                    })
                    .collect())
            }
            _ => Err(Error::Execution(format!(
                "AudioChunkerNode only supports F32 audio format (received format {})",
                audio_buf.format
            ))),
        }
    }

    /// Convert f32 samples back to ProtoAudioBuffer
    #[allow(dead_code)] // Reserved for audio format conversion utilities
    fn f32_to_audio_buffer(&self, samples: &[f32], state: &ChunkerState) -> ProtoAudioBuffer {
        let mut sample_bytes = Vec::with_capacity(samples.len() * 4);
        for &sample in samples {
            sample_bytes.extend_from_slice(&sample.to_le_bytes());
        }

        ProtoAudioBuffer {
            samples: sample_bytes,
            sample_rate: state.sample_rate,
            channels: state.channels,
            format: state.format,
            num_samples: samples.len() as u64,
        }
    }
}

// Phase A-Wave 2: migrated to `SyncStreamingNode`. The body was already
// sync (parking_lot::Mutex, no `.await`); only the trait wrapping
// changes. Multi-output emission preserved via the new
// `SyncStreamingNode::process_streaming` hook.
impl SyncStreamingNode for AudioChunkerNode {
    fn node_type(&self) -> &str {
        "AudioChunkerNode"
    }

    fn process(&self, _data: RuntimeData) -> std::result::Result<RuntimeData, Error> {
        Err(Error::Execution(
            "AudioChunkerNode requires streaming mode - \
             callers must use process_streaming() (the router does this \
             automatically when the factory declares is_multi_output_streaming=true)"
                .into(),
        ))
    }

    fn process_streaming(
        &self,
        data: RuntimeData,
        session_id: Option<&str>,
        callback: &mut dyn FnMut(RuntimeData) -> std::result::Result<(), Error>,
    ) -> std::result::Result<usize, Error> {
        let (input_samples, input_sample_rate, input_channels) = match data {
            RuntimeData::Audio {
                ref samples,
                sample_rate,
                channels,
                ..
            } => (samples.clone(), sample_rate, channels),
            other => {
                callback(other)?;
                return Ok(1);
            }
        };

        // Collect ready chunks under the lock, release, then fire
        // callbacks. `parking_lot::Mutex` — safe because no `.await`.
        let session_key = session_id.unwrap_or("default").to_string();
        let (chunks, buffered_len) = {
            let mut states = self.states.lock();
            let state = states
                .entry(session_key)
                .or_insert_with(ChunkerState::default);
            state.sample_rate = input_sample_rate;
            state.channels = input_channels;
            state.format = 1; // F32
            state.buffer.extend_from_slice(&input_samples);

            let mut chunks: Vec<RuntimeData> = Vec::new();
            while state.buffer.len() >= self.chunk_size {
                let chunk: Vec<f32> = state.buffer.drain(..self.chunk_size).collect();
                chunks.push(RuntimeData::Audio {
                    samples: chunk.into(),
                    sample_rate: state.sample_rate,
                    channels: state.channels,
                    stream_id: None,
                    timestamp_us: None,
                    arrival_ts_us: None,
                    metadata: None,
                });
            }
            (chunks, state.buffer.len())
        };

        let output_count = chunks.len();
        for chunk in chunks {
            callback(chunk)?;
        }

        tracing::debug!(
            "AudioChunker: processed {} samples, emitted {} chunks, {} samples buffered",
            input_samples.len(),
            output_count,
            buffered_len
        );

        Ok(output_count)
    }
}
