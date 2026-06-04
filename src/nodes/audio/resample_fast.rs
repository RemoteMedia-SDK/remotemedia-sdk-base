use crate::audio::buffer::{AudioBuffer, AudioData};
use crate::error::{Error, Result};
use crate::nodes::audio::fast::FastAudioNode;
use rubato::{
    Resampler as RubatoResampler, SincFixedOut, SincInterpolationParameters, SincInterpolationType,
    WindowFunction,
};
use tracing::info;

/// Quality settings for audio resampling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResampleQuality {
    Low,
    Medium,
    High,
}

impl ResampleQuality {
    fn chunk_size(&self) -> usize {
        match self {
            ResampleQuality::Low => 512,
            ResampleQuality::Medium => 1024,
            ResampleQuality::High => 2048,
        }
    }

    fn sinc_len(&self) -> usize {
        match self {
            ResampleQuality::Low => 64,
            ResampleQuality::Medium => 128,
            ResampleQuality::High => 256,
        }
    }
}

/// Fast path resampling node - no JSON overhead
pub struct FastResampleNode {
    resampler: SincFixedOut<f32>,
    target_rate: u32,
    #[allow(dead_code)] // Used for initialization, runtime channels derived from input data
    channels: usize,
    // Ring buffer to accumulate input samples across calls (per channel)
    // Using Vec for simplicity - drain from front as we consume
    input_buffer: Vec<Vec<f32>>,
    // Pre-allocated buffer for chunk extraction
    chunk_buffer: Vec<Vec<f32>>,
}

impl FastResampleNode {
    pub fn new(
        source_rate: u32,
        target_rate: u32,
        quality: ResampleQuality,
        channels: usize,
    ) -> Result<Self> {
        let chunk_size = quality.chunk_size();
        let sinc_len = quality.sinc_len();
        let resample_ratio = target_rate as f64 / source_rate as f64;

        info!(
            "Creating FastResampleNode: {} Hz -> {} Hz, ratio: {:.3}, quality: {:?}, channels: {}, chunk_size: {}, sinc_len: {}",
            source_rate, target_rate, resample_ratio, quality, channels, chunk_size, sinc_len
        );

        let parameters = SincInterpolationParameters {
            sinc_len,
            f_cutoff: 0.95,
            interpolation: SincInterpolationType::Linear,
            oversampling_factor: 256,
            window: WindowFunction::BlackmanHarris2,
        };

        let resampler = SincFixedOut::new(
            resample_ratio,
            2.0, // max_resample_ratio_relative - allows 2x adjustment range
            parameters,
            chunk_size,
            channels,
        )
        .map_err(|e| Error::Execution(format!("Failed to create resampler: {}", e)))?;

        Ok(Self {
            resampler,
            target_rate,
            channels,
            input_buffer: vec![Vec::with_capacity(chunk_size * 4); channels],
            chunk_buffer: vec![Vec::with_capacity(chunk_size * 2); channels],
        })
    }
}

impl FastAudioNode for FastResampleNode {
    fn node_type(&self) -> &str {
        "FastResampleNode"
    }

    fn process_audio(&mut self, input: AudioData) -> Result<AudioData> {
        // Get F32 buffer
        let samples = input
            .buffer
            .as_f32()
            .ok_or_else(|| Error::Execution("Resample requires F32 format".into()))?;

        let channels = input.channels;
        let input_frames = samples.len() / channels;

        tracing::debug!(
            "Resampler input: {} frames, {} channels, {} samples, rate: {} Hz, first 3 samples: [{:.6}, {:.6}, {:.6}]",
            input_frames,
            channels,
            samples.len(),
            input.sample_rate,
            samples.get(0).copied().unwrap_or(0.0),
            samples.get(1).copied().unwrap_or(0.0),
            samples.get(2).copied().unwrap_or(0.0)
        );

        // Deinterleave and append to buffer (zero-copy when possible)
        for (i, &sample) in samples.iter().enumerate() {
            self.input_buffer[i % channels].push(sample);
        }

        tracing::debug!(
            "After deinterleave: channel 0 buffer has {} samples, first 3: [{:.6}, {:.6}, {:.6}]",
            self.input_buffer[0].len(),
            self.input_buffer[0]
                .get(self.input_buffer[0].len().saturating_sub(input_frames))
                .copied()
                .unwrap_or(0.0),
            self.input_buffer[0]
                .get(self.input_buffer[0].len().saturating_sub(input_frames) + 1)
                .copied()
                .unwrap_or(0.0),
            self.input_buffer[0]
                .get(self.input_buffer[0].len().saturating_sub(input_frames) + 2)
                .copied()
                .unwrap_or(0.0)
        );

        let mut all_output_samples = Vec::new();
        let mut chunks_processed = 0;

        // Process as many chunks as we have data for
        loop {
            let frames_needed = self.resampler.input_frames_next();
            let frames_available = self.input_buffer[0].len();

            if frames_available < frames_needed {
                // Not enough buffered data yet - this is normal for streaming
                tracing::debug!(
                    "Buffering: need {} frames, have {} frames (buffered {} more)",
                    frames_needed,
                    frames_available,
                    input_frames
                );
                break;
            }

            // Clear chunk buffer and extract from input buffer
            for channel in &mut self.chunk_buffer {
                channel.clear();
            }

            for (channel_idx, channel_buf) in self.input_buffer.iter_mut().enumerate() {
                // Drain frames_needed samples from the front
                self.chunk_buffer[channel_idx].extend(channel_buf.drain(..frames_needed));
            }

            // Process chunk
            let output_frames = self
                .resampler
                .process(&self.chunk_buffer, None)
                .map_err(|e| Error::Execution(format!("Resampling failed: {}", e)))?;

            // Interleave and append output
            let frames_out = output_frames[0].len();
            tracing::debug!(
                "Chunk {}: consumed {} input frames → produced {} output frames, first 3 output: [{:.6}, {:.6}, {:.6}]",
                chunks_processed,
                frames_needed,
                frames_out,
                output_frames[0].get(0).copied().unwrap_or(0.0),
                output_frames[0].get(1).copied().unwrap_or(0.0),
                output_frames[0].get(2).copied().unwrap_or(0.0)
            );

            for frame_idx in 0..frames_out {
                for channel_idx in 0..channels {
                    all_output_samples.push(output_frames[channel_idx][frame_idx]);
                }
            }

            chunks_processed += 1;
        }

        tracing::debug!(
            "Resampler output: {} frames, {} samples, rate: {} Hz (processed {} chunks, {} frames buffered)",
            all_output_samples.len() / channels,
            all_output_samples.len(),
            self.target_rate,
            chunks_processed,
            self.input_buffer[0].len()
        );

        Ok(AudioData::new(
            AudioBuffer::new_f32(all_output_samples),
            self.target_rate,
            channels,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_resample_creation() {
        let node = FastResampleNode::new(44100, 16000, ResampleQuality::Medium, 2);
        assert!(node.is_ok());
    }

    #[test]
    fn test_fast_resample_process() {
        let mut node = FastResampleNode::new(44100, 16000, ResampleQuality::Low, 2).unwrap();

        // Generate 1 second of test audio (44100 samples per channel, 2 channels)
        let samples: Vec<f32> = (0..88200)
            .map(|i| {
                let t = (i / 2) as f32 / 44100.0;
                (2.0 * std::f32::consts::PI * 440.0 * t).sin() * 0.5
            })
            .collect();

        let input = AudioData::new(AudioBuffer::new_f32(samples), 44100, 2);

        let result = node.process_audio(input);
        assert!(result.is_ok());

        let output = result.unwrap();
        assert_eq!(output.sample_rate, 16000);
        assert_eq!(output.channels, 2);

        // Output should have approximately 32000 samples (16000 per channel * 2)
        let expected_samples = (16000 * 2) as f32;
        let actual_samples = output.buffer.len() as f32;
        let ratio = actual_samples / expected_samples;
        assert!(
            (ratio - 1.0).abs() < 0.1,
            "Expected ~{} samples, got {}",
            expected_samples,
            actual_samples
        );
    }

    #[test]
    fn test_fast_resample_small_buffers() {
        // Test with 480-frame buffers (10ms at 48kHz) - common WebRTC size
        // This simulates real-time streaming where buffers accumulate
        let mut node = FastResampleNode::new(48000, 16000, ResampleQuality::Low, 1).unwrap();

        let mut total_output_samples = 0;
        let num_chunks = 10; // Send 10 chunks (100ms total)

        for chunk_idx in 0..num_chunks {
            // Generate 480 frames (10ms) of mono audio
            let samples: Vec<f32> = (0..480)
                .map(|i| {
                    let t = (chunk_idx * 480 + i) as f32 / 48000.0;
                    (2.0 * std::f32::consts::PI * 440.0 * t).sin() * 0.5
                })
                .collect();

            let input = AudioData::new(AudioBuffer::new_f32(samples), 48000, 1);
            let result = node.process_audio(input);
            assert!(result.is_ok());

            let output = result.unwrap();
            total_output_samples += output.buffer.len();
        }

        // After 10 chunks of 480 frames (4800 total input), expect ~1600 output samples
        // (4800 * 16000/48000 = 1600)
        // Note: actual may be slightly less due to buffering in final chunk
        let expected_samples = 1600.0;
        let actual_samples = total_output_samples as f32;
        let ratio = actual_samples / expected_samples;
        assert!(
            (ratio - 1.0).abs() < 0.3,
            "Expected ~{} samples, got {} (ratio: {:.2})",
            expected_samples,
            actual_samples,
            ratio
        );
    }
}
