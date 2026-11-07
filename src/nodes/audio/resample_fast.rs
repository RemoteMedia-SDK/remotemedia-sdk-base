use crate::audio::buffer::{AudioBuffer, AudioData};
use crate::error::{Error, Result};
use crate::nodes::audio::fast::FastAudioNode;
use audioadapter_buffers::direct::InterleavedSlice;
use remotemedia_types::{AudioSamples, HeapOutputAllocator, OutputAllocator, SharedBuffer};
use rubato::{
    Async, FixedAsync, Indexing, Resampler, SincInterpolationParameters, SincInterpolationType,
    WindowFunction,
};
mod input;
use input::PendingInput;

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

/// Stateful sinc resampler with retained input and caller-selected output storage.
/// Rubato keeps its own filter history; framework input/output staging is avoided.
pub struct FastResampleNode {
    resampler: Async<f32>,
    target_rate: u32,
    source_rate: u32,
    received_frames: u64,
    emitted_frames: u64,
    delay_remaining: usize,
    channels: usize,
    pending: PendingInput,
}

impl FastResampleNode {
    pub fn new(
        source_rate: u32,
        target_rate: u32,
        quality: ResampleQuality,
        channels: usize,
    ) -> Result<Self> {
        if source_rate == 0 || target_rate == 0 || channels == 0 {
            return Err(Error::Execution(
                "resampler rates and channels must be positive".into(),
            ));
        }
        let parameters = SincInterpolationParameters {
            sinc_len: quality.sinc_len(),
            f_cutoff: 0.95,
            interpolation: SincInterpolationType::Linear,
            oversampling_factor: 256,
            window: WindowFunction::BlackmanHarris2,
        };
        let resampler = Async::new_sinc(
            target_rate as f64 / source_rate as f64,
            2.0,
            &parameters,
            quality.chunk_size(),
            channels,
            FixedAsync::Output,
        )
        .map_err(|e| Error::Execution(format!("Failed to create resampler: {e}")))?;
        let delay_remaining = resampler.output_delay();
        Ok(Self {
            resampler,
            target_rate,
            source_rate,
            received_frames: 0,
            emitted_frames: 0,
            delay_remaining,
            channels,
            pending: PendingInput::new(channels),
        })
    }

    pub(crate) fn buffered_duration_us(&self) -> u64 {
        let input = self.received_frames as u128 * 1_000_000 / self.source_rate as u128;
        let output = self.emitted_frames as u128 * 1_000_000 / self.target_rate as u128;
        input.saturating_sub(output).min(u64::MAX as u128) as u64
    }

    pub(crate) fn can_reconfigure(&self) -> bool {
        self.received_frames == 0 || self.source_rate == self.target_rate
    }

    /// Borrow retained input chunks and compute interleaved output in the selected
    /// provider. Allocation failure happens before accepting this input or advancing
    /// filter state. The published view trims delay/padding without moving samples.
    pub fn process_shared(
        &mut self,
        samples: AudioSamples,
        sample_rate: u32,
        channels: usize,
        allocator: &dyn OutputAllocator,
    ) -> Result<SharedBuffer<f32>> {
        if channels != self.channels
            || sample_rate != self.source_rate
            || samples.len() % self.channels != 0
        {
            return Err(Error::Execution("resampler input format changed".into()));
        }
        let frames = samples.len() / channels;
        let received = self
            .received_frames
            .checked_add(frames as u64)
            .ok_or_else(size_error)?;
        if self.source_rate == self.target_rate {
            self.received_frames = received;
            self.emitted_frames = received;
            return Ok(samples.into_shared());
        }
        let available = self
            .pending
            .frames()
            .checked_add(frames)
            .ok_or_else(size_error)?;
        let capacity = if available < self.resampler.input_frames_next() {
            0
        } else {
            // Same bound as Rubato's process_all_needed_output_len, with checked
            // integer arithmetic. Fixed-output processing may leave unused capacity.
            let expected =
                (available as u128 * self.target_rate as u128).div_ceil(self.source_rate as u128);
            usize::try_from(expected)
                .ok()
                .and_then(|n| n.checked_add(self.resampler.output_delay()))
                .and_then(|n| n.checked_add(self.resampler.output_frames_max()))
                .ok_or_else(size_error)?
        };
        let mut output = allocator
            .allocate_audio(capacity.checked_mul(channels).ok_or_else(size_error)?)
            .map_err(|e| Error::Execution(format!("resampler output allocation: {e}")))?;
        self.pending.push(samples);
        self.received_frames = received;
        let written = self.render(output.as_mut_slice(), false, usize::MAX)?;
        self.publish(output.freeze(), written, None)
    }

    /// Drain only the real utterance duration; padding remains internal to Rubato.
    pub(crate) fn flush_shared(
        &mut self,
        allocator: &dyn OutputAllocator,
    ) -> Result<SharedBuffer<f32>> {
        let expected =
            self.received_frames as u128 * self.target_rate as u128 / self.source_rate as u128;
        let remaining = usize::try_from(expected.saturating_sub(self.emitted_frames as u128))
            .map_err(|_| size_error())?;
        if remaining == 0 {
            let output = allocator
                .allocate_audio(0)
                .map_err(|e| Error::Execution(format!("resampler output allocation: {e}")))?;
            self.reset();
            return Ok(output.freeze());
        }
        let block = self.resampler.output_frames_next();
        let raw = remaining
            .checked_add(self.delay_remaining)
            .ok_or_else(size_error)?;
        let capacity = raw
            .div_ceil(block)
            .checked_mul(block)
            .and_then(|n| n.checked_mul(self.channels))
            .ok_or_else(size_error)?;
        let mut output = allocator
            .allocate_audio(capacity)
            .map_err(|e| Error::Execution(format!("resampler output allocation: {e}")))?;
        let written = self.render(output.as_mut_slice(), true, raw)?;
        let result = self.publish(output.freeze(), written, Some(remaining))?;
        self.reset();
        Ok(result)
    }

    fn render(&mut self, output: &mut [f32], flush: bool, limit: usize) -> Result<usize> {
        let capacity = output.len() / self.channels;
        let mut target = InterleavedSlice::new_mut(output, self.channels, capacity)
            .map_err(|e| Error::Execution(format!("resampler output layout: {e}")))?;
        let mut written = 0;
        while written < limit {
            let needed = self.resampler.input_frames_next();
            if !flush && self.pending.frames() < needed {
                break;
            }
            let next = self.resampler.output_frames_next();
            if next > capacity.saturating_sub(written) {
                return Err(Error::Execution(
                    "resampler output capacity bound exceeded".into(),
                ));
            }
            let consumed = needed.min(self.pending.frames());
            let indexing = Indexing {
                input_offset: 0,
                output_offset: written,
                partial_len: flush.then_some(consumed),
                active_channels_mask: None,
            };
            let (_, produced) = self
                .resampler
                .process_into_buffer(&self.pending, &mut target, Some(&indexing))
                .map_err(|e| Error::Execution(format!("Resampling failed: {e}")))?;
            self.pending.consume(consumed);
            written += produced;
        }
        Ok(written)
    }

    fn publish(
        &mut self,
        output: SharedBuffer<f32>,
        written: usize,
        keep: Option<usize>,
    ) -> Result<SharedBuffer<f32>> {
        let skip = self.delay_remaining.min(written);
        let count = keep.unwrap_or(written - skip);
        if count > written - skip {
            return Err(size_error());
        }
        self.delay_remaining -= skip;
        self.emitted_frames = self
            .emitted_frames
            .checked_add(count as u64)
            .ok_or_else(size_error)?;
        output
            .slice(skip * self.channels..(skip + count) * self.channels)
            .map_err(|e| Error::Execution(e.to_string()))
    }

    pub(crate) fn reset(&mut self) {
        self.resampler.reset();
        self.pending.clear();
        self.received_frames = 0;
        self.emitted_frames = 0;
        self.delay_remaining = self.resampler.output_delay();
    }
}
fn size_error() -> Error {
    Error::Execution("resampler frame count overflow".into())
}

// Preserve the legacy owned fast-node API. Its explicit contiguous Vec recovery
// may materialize a trimmed output view; context-aware pipeline calls use loans.
struct OwnedF32(AudioBuffer);
impl AsRef<[f32]> for OwnedF32 {
    fn as_ref(&self) -> &[f32] {
        self.0.as_f32().expect("validated F32 buffer")
    }
}
impl FastAudioNode for FastResampleNode {
    fn node_type(&self) -> &str {
        "FastResampleNode"
    }
    fn process_audio(&mut self, input: AudioData) -> Result<AudioData> {
        if input.buffer.as_f32().is_none() {
            return Err(Error::Execution("Resample requires F32 format".into()));
        }
        let samples = SharedBuffer::from_owner(OwnedF32(input.buffer));
        let output = self.process_shared(
            samples.into(),
            input.sample_rate,
            input.channels,
            &HeapOutputAllocator,
        )?;
        Ok(AudioData::new(
            AudioBuffer::new_f32(output.into_vec()),
            self.target_rate,
            self.channels,
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
