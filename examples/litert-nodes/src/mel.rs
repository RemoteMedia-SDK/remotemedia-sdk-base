//! Whisper's log-mel spectrogram, as `whisper.audio.log_mel_spectrogram`
//! computes it: 16 kHz audio padded with zeros to 30 s, a 400-point STFT
//! (periodic Hann window, hop 160, reflect padding), power spectrum, 80 mel
//! filters, log10, clamped to 8 below the maximum, scaled `(x + 4) / 4`.

use std::sync::Arc;

use rustfft::{num_complex::Complex32, Fft, FftPlanner};

pub const SAMPLE_RATE: usize = 16_000;
pub const N_FFT: usize = 400;
pub const HOP: usize = 160;
pub const N_SAMPLES: usize = 30 * SAMPLE_RATE;
pub const N_FRAMES: usize = N_SAMPLES / HOP; // 3000
pub const N_MELS: usize = 80;
const N_BINS: usize = N_FFT / 2 + 1; // 201

/// The 80 x 201 filterbank OpenAI ships with Whisper (`mel_filters.npz`,
/// `mel_80`), little-endian f32.
static MEL_FILTERS_80: &[u8] = include_bytes!("../assets/mel_filters_80.bin");

pub struct LogMel {
    fft: Arc<dyn Fft<f32>>,
    window: Vec<f32>,
    filters: Vec<f32>,
}

impl Default for LogMel {
    fn default() -> Self {
        Self::new()
    }
}

impl LogMel {
    pub fn new() -> Self {
        let window = (0..N_FFT)
            .map(|n| 0.5 * (1.0 - (2.0 * std::f32::consts::PI * n as f32 / N_FFT as f32).cos()))
            .collect();
        let filters = MEL_FILTERS_80
            .chunks_exact(4)
            .map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
            .collect::<Vec<_>>();
        assert_eq!(filters.len(), N_MELS * N_BINS);
        Self {
            fft: FftPlanner::new().plan_fft_forward(N_FFT),
            window,
            filters,
        }
    }

    /// `[N_MELS][N_FRAMES]` row-major, written into `out`. Audio longer than
    /// 30 s is cut.
    pub fn compute(&self, audio: &[f32], out: &mut [f32]) {
        assert_eq!(out.len(), N_MELS * N_FRAMES);
        let len = audio.len().min(N_SAMPLES);
        let sample = |i: usize| if i < len { audio[i] } else { 0.0 };
        // Reflect padding by N_FFT/2 on the padded 30 s signal.
        let pad = N_FFT / 2;
        let at = |j: isize| -> f32 {
            let n = N_SAMPLES as isize;
            let idx = if j < 0 {
                -j
            } else if j >= n {
                2 * (n - 1) - j
            } else {
                j
            };
            sample(idx as usize)
        };

        let mut buf = vec![Complex32::new(0.0, 0.0); N_FFT];
        let mut scratch = vec![Complex32::new(0.0, 0.0); self.fft.get_inplace_scratch_len()];
        let mut power = vec![0f32; N_BINS];
        let mut max = f32::NEG_INFINITY;
        for t in 0..N_FRAMES {
            let start = (t * HOP) as isize - pad as isize;
            for (k, slot) in buf.iter_mut().enumerate() {
                *slot = Complex32::new(at(start + k as isize) * self.window[k], 0.0);
            }
            self.fft.process_with_scratch(&mut buf, &mut scratch);
            for (k, p) in power.iter_mut().enumerate() {
                *p = buf[k].norm_sqr();
            }
            for m in 0..N_MELS {
                let row = &self.filters[m * N_BINS..(m + 1) * N_BINS];
                let energy: f32 = row.iter().zip(&power).map(|(f, p)| f * p).sum();
                let value = energy.max(1e-10).log10();
                out[m * N_FRAMES + t] = value;
                max = max.max(value);
            }
        }
        let floor = max - 8.0;
        for value in out.iter_mut() {
            *value = (value.max(floor) + 4.0) / 4.0;
        }
    }
}

/// Mono 16 kHz from interleaved audio at any rate (linear interpolation).
pub fn to_mono_16k(samples: &[f32], sample_rate: u32, channels: u32) -> Vec<f32> {
    let channels = channels.max(1) as usize;
    let mono: Vec<f32> = if channels == 1 {
        samples.to_vec()
    } else {
        samples
            .chunks(channels)
            .map(|frame| frame.iter().sum::<f32>() / frame.len() as f32)
            .collect()
    };
    if sample_rate as usize == SAMPLE_RATE || mono.is_empty() {
        return mono;
    }
    let ratio = sample_rate as f64 / SAMPLE_RATE as f64;
    let out_len = ((mono.len() as f64) / ratio).floor() as usize;
    (0..out_len)
        .map(|i| {
            let pos = i as f64 * ratio;
            let i0 = pos.floor() as usize;
            let i1 = (i0 + 1).min(mono.len() - 1);
            let frac = (pos - i0 as f64) as f32;
            mono[i0] * (1.0 - frac) + mono[i1] * frac
        })
        .collect()
}
