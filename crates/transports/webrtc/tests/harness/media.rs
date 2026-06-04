//! Synthetic media generation for WebRTC E2E testing
//!
//! Provides utilities for generating test audio and video signals.

use std::f32::consts::PI;

/// Media generator for creating synthetic test signals
pub struct MediaGenerator;

impl MediaGenerator {
    /// Create a new media generator
    pub fn new() -> Self {
        Self
    }

    // ========================================================================
    // Audio Generation
    // ========================================================================

    /// Generate a sine wave audio signal
    ///
    /// # Arguments
    ///
    /// * `frequency` - Frequency in Hz (e.g., 440.0 for A4)
    /// * `duration_secs` - Duration in seconds
    /// * `sample_rate` - Sample rate in Hz (e.g., 48000)
    ///
    /// # Returns
    ///
    /// Vec of f32 samples in range [-1.0, 1.0]
    pub fn generate_sine_wave(
        &self,
        frequency: f32,
        duration_secs: f32,
        sample_rate: u32,
    ) -> Vec<f32> {
        let num_samples = (duration_secs * sample_rate as f32) as usize;
        let angular_frequency = 2.0 * PI * frequency / sample_rate as f32;

        (0..num_samples)
            .map(|i| (angular_frequency * i as f32).sin())
            .collect()
    }

    /// Generate a sine wave with amplitude
    pub fn generate_sine_wave_with_amplitude(
        &self,
        frequency: f32,
        duration_secs: f32,
        sample_rate: u32,
        amplitude: f32,
    ) -> Vec<f32> {
        self.generate_sine_wave(frequency, duration_secs, sample_rate)
            .into_iter()
            .map(|s| s * amplitude)
            .collect()
    }

    /// Generate silence (all zeros)
    ///
    /// # Arguments
    ///
    /// * `duration_secs` - Duration in seconds
    /// * `sample_rate` - Sample rate in Hz
    pub fn generate_silence(&self, duration_secs: f32, sample_rate: u32) -> Vec<f32> {
        let num_samples = (duration_secs * sample_rate as f32) as usize;
        vec![0.0; num_samples]
    }

    /// Generate white noise
    pub fn generate_white_noise(&self, duration_secs: f32, sample_rate: u32) -> Vec<f32> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let num_samples = (duration_secs * sample_rate as f32) as usize;
        let mut samples = Vec::with_capacity(num_samples);

        // Simple pseudo-random number generator for reproducibility
        let mut hasher = DefaultHasher::new();
        for i in 0..num_samples {
            i.hash(&mut hasher);
            let hash = hasher.finish();
            // Convert to float in range [-1.0, 1.0]
            let sample = (hash as f32 / u64::MAX as f32) * 2.0 - 1.0;
            samples.push(sample * 0.5); // Reduce amplitude to 0.5
        }

        samples
    }

    /// Generate a chirp (frequency sweep)
    pub fn generate_chirp(
        &self,
        start_freq: f32,
        end_freq: f32,
        duration_secs: f32,
        sample_rate: u32,
    ) -> Vec<f32> {
        let num_samples = (duration_secs * sample_rate as f32) as usize;
        let freq_step = (end_freq - start_freq) / num_samples as f32;

        (0..num_samples)
            .map(|i| {
                let freq = start_freq + freq_step * i as f32;
                let phase = 2.0 * PI * freq * i as f32 / sample_rate as f32;
                phase.sin()
            })
            .collect()
    }

    /// Generate a DTMF tone (dual-tone multi-frequency)
    pub fn generate_dtmf(&self, digit: char, duration_secs: f32, sample_rate: u32) -> Vec<f32> {
        let (low_freq, high_freq) = match digit {
            '1' => (697.0, 1209.0),
            '2' => (697.0, 1336.0),
            '3' => (697.0, 1477.0),
            '4' => (770.0, 1209.0),
            '5' => (770.0, 1336.0),
            '6' => (770.0, 1477.0),
            '7' => (852.0, 1209.0),
            '8' => (852.0, 1336.0),
            '9' => (852.0, 1477.0),
            '*' => (941.0, 1209.0),
            '0' => (941.0, 1336.0),
            '#' => (941.0, 1477.0),
            'A' => (697.0, 1633.0),
            'B' => (770.0, 1633.0),
            'C' => (852.0, 1633.0),
            'D' => (941.0, 1633.0),
            _ => (0.0, 0.0), // Silent for unknown
        };

        let low = self.generate_sine_wave(low_freq, duration_secs, sample_rate);
        let high = self.generate_sine_wave(high_freq, duration_secs, sample_rate);

        // Mix the two tones at 50% each
        low.iter()
            .zip(high.iter())
            .map(|(l, h)| (l + h) * 0.5)
            .collect()
    }

    /// Generate audio with speech-like characteristics (for VAD testing)
    ///
    /// Generates bursts of tone with gaps to simulate speech patterns
    pub fn generate_speech_like(&self, duration_secs: f32, sample_rate: u32) -> Vec<f32> {
        let num_samples = (duration_secs * sample_rate as f32) as usize;
        let mut samples = vec![0.0; num_samples];

        // Create speech-like bursts (200ms tone, 100ms silence, repeat)
        let burst_samples = (0.2 * sample_rate as f32) as usize;
        let gap_samples = (0.1 * sample_rate as f32) as usize;
        let cycle_samples = burst_samples + gap_samples;

        for i in 0..num_samples {
            let pos_in_cycle = i % cycle_samples;
            if pos_in_cycle < burst_samples {
                // Generate tone during burst
                let freq = 200.0 + (i as f32 * 0.01).sin() * 100.0; // Varying frequency
                let phase = 2.0 * PI * freq * i as f32 / sample_rate as f32;
                samples[i] = phase.sin() * 0.8;
            }
            // Gap is already 0.0
        }

        samples
    }

    // ========================================================================
    // Video Generation
    // ========================================================================

    /// Generate a solid color video frame in YUV420P format
    ///
    /// # Arguments
    ///
    /// * `width` - Frame width in pixels
    /// * `height` - Frame height in pixels
    /// * `y` - Y (luma) value (0-255)
    /// * `u` - U (Cb) value (0-255, 128 = neutral)
    /// * `v` - V (Cr) value (0-255, 128 = neutral)
    ///
    /// # Returns
    ///
    /// Raw YUV420P frame data
    pub fn generate_solid_frame(&self, width: u32, height: u32, y: u8, u: u8, v: u8) -> Vec<u8> {
        let y_size = (width * height) as usize;
        let uv_size = y_size / 4; // 4:2:0 subsampling

        let mut frame = Vec::with_capacity(y_size + uv_size * 2);

        // Y plane
        frame.extend(std::iter::repeat(y).take(y_size));

        // U plane
        frame.extend(std::iter::repeat(u).take(uv_size));

        // V plane
        frame.extend(std::iter::repeat(v).take(uv_size));

        frame
    }

    /// Generate common color frames
    pub fn generate_black_frame(&self, width: u32, height: u32) -> Vec<u8> {
        self.generate_solid_frame(width, height, 16, 128, 128) // Black in YUV
    }

    pub fn generate_white_frame(&self, width: u32, height: u32) -> Vec<u8> {
        self.generate_solid_frame(width, height, 235, 128, 128) // White in YUV
    }

    pub fn generate_red_frame(&self, width: u32, height: u32) -> Vec<u8> {
        self.generate_solid_frame(width, height, 81, 90, 240) // Red in YUV
    }

    pub fn generate_green_frame(&self, width: u32, height: u32) -> Vec<u8> {
        self.generate_solid_frame(width, height, 145, 54, 34) // Green in YUV
    }

    pub fn generate_blue_frame(&self, width: u32, height: u32) -> Vec<u8> {
        self.generate_solid_frame(width, height, 41, 240, 110) // Blue in YUV
    }

    /// Generate a test pattern (color bars) in YUV420P format
    ///
    /// # Arguments
    ///
    /// * `width` - Frame width in pixels
    /// * `height` - Frame height in pixels
    ///
    /// # Returns
    ///
    /// Raw YUV420P frame data with vertical color bars
    pub fn generate_test_pattern(&self, width: u32, height: u32) -> Vec<u8> {
        let y_size = (width * height) as usize;
        let uv_width = width / 2;
        let uv_height = height / 2;
        let uv_size = (uv_width * uv_height) as usize;

        let mut y_plane = vec![0u8; y_size];
        let mut u_plane = vec![128u8; uv_size];
        let mut v_plane = vec![128u8; uv_size];

        // Color bar definitions (Y, U, V)
        let bars: [(u8, u8, u8); 8] = [
            (235, 128, 128), // White
            (210, 16, 146),  // Yellow
            (170, 166, 16),  // Cyan
            (145, 54, 34),   // Green
            (106, 202, 222), // Magenta
            (81, 90, 240),   // Red
            (41, 240, 110),  // Blue
            (16, 128, 128),  // Black
        ];

        let bar_width = width / 8;

        // Fill Y plane
        for y in 0..height {
            for x in 0..width {
                let bar_idx = (x / bar_width) as usize % 8;
                let (y_val, _, _) = bars[bar_idx];
                y_plane[(y * width + x) as usize] = y_val;
            }
        }

        // Fill U and V planes (2x2 subsampling)
        for y in 0..uv_height {
            for x in 0..uv_width {
                let bar_idx = ((x * 2) / bar_width) as usize % 8;
                let (_, u_val, v_val) = bars[bar_idx];
                let idx = (y * uv_width + x) as usize;
                u_plane[idx] = u_val;
                v_plane[idx] = v_val;
            }
        }

        let mut frame = Vec::with_capacity(y_size + uv_size * 2);
        frame.extend(y_plane);
        frame.extend(u_plane);
        frame.extend(v_plane);

        frame
    }

    /// Generate a gradient frame (horizontal luma gradient)
    pub fn generate_gradient_frame(&self, width: u32, height: u32) -> Vec<u8> {
        let y_size = (width * height) as usize;
        let uv_size = y_size / 4;

        let mut y_plane = Vec::with_capacity(y_size);

        // Horizontal gradient from black to white
        for _y in 0..height {
            for x in 0..width {
                let luma = (x as f32 / width as f32 * 219.0 + 16.0) as u8;
                y_plane.push(luma);
            }
        }

        let mut frame = Vec::with_capacity(y_size + uv_size * 2);
        frame.extend(y_plane);
        frame.extend(std::iter::repeat(128u8).take(uv_size)); // Neutral U
        frame.extend(std::iter::repeat(128u8).take(uv_size)); // Neutral V

        frame
    }

    /// Generate a frame sequence (for testing video pipelines)
    ///
    /// Creates a sequence of frames with incrementing pattern
    pub fn generate_frame_sequence(
        &self,
        width: u32,
        height: u32,
        num_frames: usize,
    ) -> Vec<Vec<u8>> {
        (0..num_frames)
            .map(|i| {
                // Each frame gets progressively brighter
                let luma = ((i as f32 / num_frames as f32) * 219.0 + 16.0) as u8;
                self.generate_solid_frame(width, height, luma, 128, 128)
            })
            .collect()
    }
}

impl Default for MediaGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sine_wave_generation() {
        let gen = MediaGenerator::new();
        let samples = gen.generate_sine_wave(440.0, 0.1, 48000);

        // 0.1 seconds at 48kHz = 4800 samples
        assert_eq!(samples.len(), 4800);

        // All samples should be in range [-1.0, 1.0]
        assert!(samples.iter().all(|&s| s >= -1.0 && s <= 1.0));

        // Should have some variation (not silence)
        let max = samples.iter().fold(0.0f32, |a, &b| a.max(b.abs()));
        assert!(max > 0.9);
    }

    #[test]
    fn test_silence_generation() {
        let gen = MediaGenerator::new();
        let samples = gen.generate_silence(0.05, 48000);

        assert_eq!(samples.len(), 2400);
        assert!(samples.iter().all(|&s| s == 0.0));
    }

    #[test]
    fn test_solid_frame_generation() {
        let gen = MediaGenerator::new();
        let frame = gen.generate_solid_frame(640, 480, 128, 64, 192);

        // YUV420P: Y = W*H, U = W*H/4, V = W*H/4
        let expected_size = 640 * 480 + (640 * 480 / 4) * 2;
        assert_eq!(frame.len(), expected_size);

        // Check Y plane (first 640*480 bytes should all be 128)
        let y_plane = &frame[0..(640 * 480)];
        assert!(y_plane.iter().all(|&b| b == 128));
    }

    #[test]
    fn test_test_pattern_generation() {
        let gen = MediaGenerator::new();
        let frame = gen.generate_test_pattern(640, 480);

        let expected_size = 640 * 480 + (640 * 480 / 4) * 2;
        assert_eq!(frame.len(), expected_size);

        // First bar (white) should have Y = 235
        assert_eq!(frame[0], 235);

        // Last bar (black) should have Y = 16
        assert_eq!(frame[639], 16);
    }

    #[test]
    fn test_dtmf_generation() {
        let gen = MediaGenerator::new();
        let samples = gen.generate_dtmf('5', 0.1, 48000);

        assert_eq!(samples.len(), 4800);
        // DTMF should have significant amplitude
        let max = samples.iter().fold(0.0f32, |a, &b| a.max(b.abs()));
        assert!(max > 0.4);
    }
}
