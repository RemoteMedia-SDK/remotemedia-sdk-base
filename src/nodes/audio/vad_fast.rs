use crate::audio::buffer::AudioData;
use crate::error::{Error, Result};
use crate::nodes::audio::fast::FastAudioNode;

/// Fast path VAD node - simple energy-based detection without FFT overhead
pub struct FastVADNode {
    sample_rate: u32,
    frame_duration_ms: u32,
    energy_threshold: f32,
}

impl FastVADNode {
    pub fn new(sample_rate: u32, frame_duration_ms: u32, energy_threshold: f32) -> Self {
        Self {
            sample_rate,
            frame_duration_ms,
            energy_threshold,
        }
    }

    fn frame_size(&self) -> usize {
        ((self.sample_rate as f32 * self.frame_duration_ms as f32) / 1000.0) as usize
    }

    /// Compute RMS energy for a frame (simpler than FFT, matches Python numpy approach)
    #[inline]
    fn compute_frame_energy(&self, frame: &[f32]) -> f32 {
        if frame.is_empty() {
            return 0.0;
        }

        // RMS energy: sqrt(mean(samples^2))
        let sum_squares: f32 = frame.iter().map(|&s| s * s).sum();
        (sum_squares / frame.len() as f32).sqrt()
    }
}

impl FastAudioNode for FastVADNode {
    fn node_type(&self) -> &str {
        "FastVADNode"
    }

    fn process_audio(&mut self, input: AudioData) -> Result<AudioData> {
        // Get F32 buffer
        let samples = input
            .buffer
            .as_f32()
            .ok_or_else(|| Error::Execution("VAD requires F32 format".into()))?;

        // Convert to mono if stereo
        let mono_samples: Vec<f32> = if input.channels > 1 {
            // Average channels
            samples
                .chunks(input.channels)
                .map(|chunk| chunk.iter().sum::<f32>() / input.channels as f32)
                .collect()
        } else {
            samples.to_vec()
        };

        let frame_size = self.frame_size();
        let num_frames = mono_samples.len() / frame_size;

        // Process frames - check if any frame has speech
        let mut speech_frames = 0;

        for frame_idx in 0..num_frames {
            let start = frame_idx * frame_size;
            let end = (start + frame_size).min(mono_samples.len());

            let frame = &mono_samples[start..end];
            let energy = self.compute_frame_energy(frame);

            if energy > self.energy_threshold {
                speech_frames += 1;
            }
        }

        let _is_speech = speech_frames > 0; // TODO: Attach as metadata to output

        // Pass through original audio with metadata
        // Store VAD results in a way that can be retrieved (we'll use a simple approach)
        // For now, we'll just pass through the audio
        // In a real implementation, you'd want metadata handling

        Ok(input) // Pass through unchanged - VAD is typically a side-effect node
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_vad_creation() {
        let node = FastVADNode::new(16000, 30, 0.01);
        assert_eq!(node.sample_rate, 16000);
        assert_eq!(node.frame_duration_ms, 30);
    }

    #[test]
    fn test_fast_vad_frame_size() {
        let node = FastVADNode::new(16000, 30, 0.01);
        assert_eq!(node.frame_size(), 480);
    }

    #[test]
    fn test_fast_vad_energy_computation() {
        let node = FastVADNode::new(16000, 30, 0.01);

        // Silent frame
        let silent = vec![0.0f32; 480];
        let silent_energy = node.compute_frame_energy(&silent);
        assert!(silent_energy < 0.0001);

        // Loud frame
        let loud: Vec<f32> = (0..480)
            .map(|i| (2.0 * std::f32::consts::PI * 440.0 * i as f32 / 16000.0).sin() * 0.5)
            .collect();
        let loud_energy = node.compute_frame_energy(&loud);
        assert!(loud_energy > 0.3); // RMS of 0.5 amplitude sine is ~0.35
    }

    #[test]
    fn test_fast_vad_process_mono() {
        let mut node = FastVADNode::new(16000, 30, 0.01);

        // Generate 1 second of 440Hz tone
        let samples: Vec<f32> = (0..16000)
            .map(|i| (2.0 * std::f32::consts::PI * 440.0 * i as f32 / 16000.0).sin() * 0.5)
            .collect();

        let input = AudioData::new(
            crate::audio::buffer::AudioBuffer::new_f32(samples),
            16000,
            1,
        );

        let result = node.process_audio(input);
        assert!(result.is_ok());
    }

    #[test]
    fn test_fast_vad_process_stereo() {
        let mut node = FastVADNode::new(16000, 30, 0.01);

        // Generate 1 second of stereo audio
        let samples: Vec<f32> = (0..32000)
            .map(|i| {
                let t = (i / 2) as f32 / 16000.0;
                (2.0 * std::f32::consts::PI * 440.0 * t).sin() * 0.5
            })
            .collect();

        let input = AudioData::new(
            crate::audio::buffer::AudioBuffer::new_f32(samples),
            16000,
            2,
        );

        let result = node.process_audio(input);
        assert!(result.is_ok());
    }
}
