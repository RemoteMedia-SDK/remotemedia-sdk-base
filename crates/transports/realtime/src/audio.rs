//! PCM16 on the socket, `f32` in the pipeline, and the resampling between
//! the session's formats and the pipeline's rates.

use base64::Engine as _;

/// Base64 PCM16 (little-endian, mono) to `f32` samples in `[-1, 1)`.
pub fn decode_pcm16_base64(audio: &str) -> Result<Vec<f32>, String> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(audio.trim())
        .map_err(|error| format!("audio is not base64: {error}"))?;
    Ok(bytes
        .chunks_exact(2)
        .map(|pair| f32::from(i16::from_le_bytes([pair[0], pair[1]])) / 32768.0)
        .collect())
}

/// PCM16 samples to base64 (little-endian).
pub fn encode_pcm16_base64(samples: &[i16]) -> String {
    let mut bytes = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        bytes.extend_from_slice(&sample.to_le_bytes());
    }
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

pub fn to_pcm16(samples: &[f32]) -> Vec<i16> {
    samples
        .iter()
        .map(|sample| (sample.clamp(-1.0, 1.0) * 32767.0).round() as i16)
        .collect()
}

/// Interleaved audio to mono, by averaging the channels of each frame.
pub fn to_mono(samples: &[f32], channels: u32) -> Vec<f32> {
    let channels = channels.max(1) as usize;
    if channels == 1 {
        return samples.to_vec();
    }
    samples
        .chunks_exact(channels)
        .map(|frame| frame.iter().sum::<f32>() / channels as f32)
        .collect()
}

/// A streaming mono resampler (windowed sinc, band-limited to the lower of
/// the two rates). Feeding the audio in pieces gives the same samples as
/// feeding it at once. The output lags the input by [`Self::latency`] input
/// samples (under 2 ms for speech rates).
#[derive(Debug)]
pub struct Resampler {
    from: u32,
    to: u32,
    /// Input samples per output sample.
    step: f64,
    /// Cut-off as a fraction of the input's Nyquist frequency.
    cutoff: f64,
    /// Taps on each side of the output position.
    half: usize,
    /// Input not yet fully consumed, preceded by `half` samples of history.
    buffer: Vec<f32>,
    /// Position of the next output sample, in `buffer` coordinates.
    position: f64,
}

impl Resampler {
    pub fn new(from: u32, to: u32) -> Self {
        let from = from.max(1);
        let to = to.max(1);
        let cutoff = (f64::from(to) / f64::from(from)).min(1.0);
        // Eight zero crossings on each side at the cut-off.
        let half = ((8.0 / cutoff).ceil() as usize).clamp(8, 64);
        Self {
            from,
            to,
            step: f64::from(from) / f64::from(to),
            cutoff,
            half,
            buffer: vec![0.0; half],
            position: half as f64,
        }
    }

    pub fn from_rate(&self) -> u32 {
        self.from
    }

    pub fn to_rate(&self) -> u32 {
        self.to
    }

    /// Input samples the output is behind by.
    pub fn latency(&self) -> usize {
        if self.from == self.to {
            0
        } else {
            self.half
        }
    }

    pub fn process(&mut self, input: &[f32]) -> Vec<f32> {
        if self.from == self.to {
            return input.to_vec();
        }
        self.buffer.extend_from_slice(input);
        let half = self.half as isize;
        let mut output =
            Vec::with_capacity((input.len() as f64 / self.step) as usize + 1);
        // An output sample needs `half` input samples after its position.
        while (self.position.floor() as isize) + half < self.buffer.len() as isize {
            let center = self.position.floor() as isize;
            let mut sum = 0.0f64;
            let mut weight = 0.0f64;
            for index in (center - half + 1)..=(center + half) {
                let distance = self.position - index as f64;
                let tap = kernel(distance, self.cutoff, self.half as f64);
                sum += f64::from(self.buffer[index as usize]) * tap;
                weight += tap;
            }
            output.push(if weight.abs() > 1e-9 { (sum / weight) as f32 } else { 0.0 });
            self.position += self.step;
        }
        // Keep `half` samples of history before the next output position.
        let keep_from = (self.position.floor() as isize - half).max(0) as usize;
        if keep_from > 0 {
            self.buffer.drain(..keep_from.min(self.buffer.len()));
            self.position -= keep_from as f64;
        }
        output
    }
}

/// Hann-windowed sinc, `distance` in input samples.
fn kernel(distance: f64, cutoff: f64, half: f64) -> f64 {
    let window_position = distance / half;
    if window_position.abs() >= 1.0 {
        return 0.0;
    }
    let window = 0.5 * (1.0 + (std::f64::consts::PI * window_position).cos());
    let x = std::f64::consts::PI * cutoff * distance;
    let sinc = if x.abs() < 1e-9 { 1.0 } else { x.sin() / x };
    cutoff * sinc * window
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sine(rate: u32, hz: f32, samples: usize) -> Vec<f32> {
        (0..samples)
            .map(|n| (2.0 * std::f32::consts::PI * hz * n as f32 / rate as f32).sin() * 0.5)
            .collect()
    }

    #[test]
    fn pcm16_round_trips_through_base64() {
        let samples = vec![0i16, 1, -1, 12345, -32768, 32767];
        let decoded = decode_pcm16_base64(&encode_pcm16_base64(&samples)).unwrap();
        assert_eq!(to_pcm16(&decoded)[..4], samples[..4]);
        assert_eq!(decoded.len(), samples.len());
        assert!(decode_pcm16_base64("not base64!").is_err());
    }

    #[test]
    fn the_same_rate_is_passed_through() {
        let mut resampler = Resampler::new(24_000, 24_000);
        let input = sine(24_000, 440.0, 480);
        assert_eq!(resampler.process(&input), input);
    }

    #[test]
    fn output_length_follows_the_rate_ratio() {
        for (from, to) in [(24_000u32, 16_000u32), (24_000, 48_000), (16_000, 24_000), (48_000, 8_000)] {
            let mut resampler = Resampler::new(from, to);
            let produced = resampler.process(&sine(from, 300.0, from as usize)).len() as i64;
            let expected = i64::from(to);
            let lag = (resampler.latency() as f64 * f64::from(to) / f64::from(from)).ceil() as i64 + 1;
            assert!(
                (expected - produced) >= 0 && (expected - produced) <= lag,
                "{from}->{to}: {produced} of {expected} samples"
            );
        }
    }

    #[test]
    fn pieces_give_the_same_samples_as_the_whole() {
        let input = sine(24_000, 440.0, 4_800);
        let whole = Resampler::new(24_000, 16_000).process(&input);
        let mut pieces = Resampler::new(24_000, 16_000);
        let mut joined = Vec::new();
        for piece in input.chunks(333) {
            joined.extend(pieces.process(piece));
        }
        assert_eq!(joined.len(), whole.len());
        for (a, b) in joined.iter().zip(&whole) {
            assert!((a - b).abs() < 1e-5);
        }
    }

    #[test]
    fn a_tone_keeps_its_level_and_frequency() {
        // 440 Hz at 16 kHz to 24 kHz: same amplitude, same zero crossings.
        let output = Resampler::new(16_000, 24_000).process(&sine(16_000, 440.0, 16_000));
        let steady = &output[2_400..21_600];
        let peak = steady.iter().fold(0.0f32, |peak, sample| peak.max(sample.abs()));
        assert!((peak - 0.5).abs() < 0.02, "peak {peak}");
        let crossings = steady
            .windows(2)
            .filter(|pair| pair[0] <= 0.0 && pair[1] > 0.0)
            .count();
        // 0.8 s of a 440 Hz tone.
        assert!((351..=353).contains(&crossings), "{crossings} cycles");
    }
}
