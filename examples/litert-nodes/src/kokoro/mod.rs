//! Kokoro-82M text-to-speech from its `.litertlm` bundle
//! (`litert-community/Kokoro-82M`), English only:
//!
//! text → rule-table normalization (`en-textnorm`) → English G2P over the
//! bundled lexicon (`en-lexicon`) → phoneme ids → acoustic model
//! (`tf_lite_acoustic`: durations, pitch, energy; CPU, it takes int64 ids)
//! → vocoder (`tf_lite_vocoder`: magnitude and phase spectrograms; GPU) →
//! a 20-point inverse STFT → 24 kHz mono samples.
//!
//! The stages follow LiteRT-LM's `omni/text2speech/kokoro` C++ engine.

pub mod g2p;
pub mod normalizer;
pub mod patch;
pub mod tokens;

use std::time::Instant;

use crate::backend::Backend;
use crate::compiled::{accelerators, CompiledBackend, ModelSource};
use crate::litert::TfLiteBackend;
use crate::container::{Container, TFLITE_MODEL};
use tokens::Join;


const VOICE_TOKENS: usize = 510;
const VOICE_DIM: usize = 256;
const STYLE_DIM: usize = 128;
const FEATURE_DIM: usize = 512;
const FRAMES_PER_TOKEN_BUDGET: usize = 3;
const AUDIO_HOP: usize = 600;
const ISTFT_BINS: usize = 11;
const ISTFT_FRAME: usize = 20;
const ISTFT_HOP: usize = 5;
const HANN_OLA_SCALE: f32 = 1.5;
/// The vocoder is resized to the slice's frames rounded up to this, when
/// the backend can resize (its cost follows its input length).
const VOCODER_BUCKET: usize = 32;

const I64: u32 = 4;
/// Both models' only signature.
const SIGNATURE: &str = "serving_default";

#[derive(Clone, Debug)]
pub struct KokoroConfig {
    pub model_path: String,
    /// LiteRT library (see `CompiledBackend`); `None` tries the defaults.
    pub runtime_lib: Option<String>,
    pub voice: String,
    /// `gpu`: the vocoder on the GPU and the acoustic model on the CPU,
    /// both through LiteRT's CompiledModel API. `cpu`: both on the TFLite
    /// interpreter (XNNPACK), for libraries without CompiledModel (e.g.
    /// LiteRT-LM's Windows DLL); use the `kokoro_int8_fp16` bundle then.
    pub vocoder_accelerator: String,
    /// XNNPACK threads (`cpu`).
    pub threads: i32,
    /// LiteRT GPU options for the vocoder, as TOML (see `CompiledBackend`).
    pub gpu_options: Option<String>,
    /// A vocoder `.tflite` to use instead of the bundle's.
    pub vocoder_path: Option<String>,
    /// `en-us` or `en-gb`.
    pub language: String,
}

impl KokoroConfig {
    pub fn new(model_path: impl Into<String>) -> Self {
        Self {
            model_path: model_path.into(),
            runtime_lib: None,
            voice: "af_heart".into(),
            vocoder_accelerator: "gpu".into(),
            threads: 4,
            gpu_options: None,
            vocoder_path: None,
            language: "en-us".into(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Timing {
    pub phonemize_ms: f64,
    pub acoustic_ms: f64,
    pub vocoder_ms: f64,
    pub slices: usize,
    pub tokens: usize,
}

pub struct Kokoro {
    acoustic: Box<dyn Backend + Send>,
    vocoder: Box<dyn Backend + Send>,
    acoustic_sig: String,
    vocoder_sig: String,
    g2p: g2p::EnglishG2p,
    normalizer: Option<normalizer::TextNormalizer>,
    voice: Vec<f32>,
    british: bool,
    /// Phoneme slots in the acoustic model's input.
    capacity: usize,
    /// Acoustic frames the vocoder takes.
    frames: usize,
    vocoder_length_i64: bool,
    /// Frames the vocoder is currently shaped for, when it can be resized.
    vocoder_frames: Option<usize>,
    pub runtime: String,
}

fn ms(since: Instant) -> f64 {
    since.elapsed().as_secs_f64() * 1000.0
}

fn f32s(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

fn bytes_of<T: Copy>(data: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, std::mem::size_of_val(data)) }
}

impl Kokoro {
    pub fn load(config: &KokoroConfig) -> Result<Self, String> {
        let container = Container::open(&config.model_path)?;
        let map = container.mapping();
        let lib = config.runtime_lib.as_deref();
        let section = |kind: &str| -> Result<&[u8], String> {
            container
                .find(TFLITE_MODEL, "model_type", kind)
                .map(|s| container.bytes(s))
                .ok_or_else(|| format!("{}: no {kind} model", config.model_path))
        };
        let borrowed = |bytes: &[u8]| ModelSource::Buffer {
            owner: map.clone(),
            ptr: bytes.as_ptr(),
            len: bytes.len(),
        };

        // The acoustic model (int64 ids, the BiLSTM custom op) runs on the
        // TFLite interpreter with XNNPACK, which is faster for it on a phone
        // CPU than CompiledModel's CPU path.
        let acoustic: Box<dyn Backend + Send> = Box::new(TfLiteBackend::load_source(
            lib,
            borrowed(section("tf_lite_acoustic")?),
            config.threads,
            &[SIGNATURE],
        )?);

        let vocoder: Box<dyn Backend + Send> = if config.vocoder_accelerator == "cpu" {
            let source = match &config.vocoder_path {
                Some(path) => ModelSource::File(path.clone()),
                None => borrowed(section("tf_lite_vocoder")?),
            };
            Box::new(TfLiteBackend::load_source(lib, source, config.threads, &[SIGNATURE])?)
        } else {
            let source = match &config.vocoder_path {
                Some(path) => ModelSource::File(path.clone()),
                None => {
                    // Patched so LiteRT 2.3.2's GPU delegate takes it whole.
                    let (patched, _) = patch::keep_fc_dims(section("tf_lite_vocoder")?)?;
                    let patched = std::sync::Arc::new(patched);
                    ModelSource::Buffer {
                        ptr: patched.as_ptr(),
                        len: patched.len(),
                        owner: patched,
                    }
                }
            };
            Box::new(CompiledBackend::load_source(
                lib,
                source,
                accelerators(&config.vocoder_accelerator)?,
                None,
                config.gpu_options.as_deref(),
            )?)
        };
        let acoustic_sig = SIGNATURE.to_string();
        let vocoder_sig = SIGNATURE.to_string();

        let (ids_bytes, ids_type) = acoustic.tensor_info(&acoustic_sig, "phoneme_ids", false)?;
        if ids_type != I64 {
            return Err("the acoustic model's phoneme_ids are not int64".into());
        }
        let capacity = ids_bytes / 8;
        let (features, _) = vocoder.tensor_info(&vocoder_sig, "acoustic_features", false)?;
        let frames = features / (FEATURE_DIM * 4);
        let (length_bytes, _) = vocoder.tensor_info(&vocoder_sig, "speech_frame_length", false)?;

        let voice_bytes = container
            .named(&config.voice)
            .ok_or_else(|| format!("{}: no voice '{}'", config.model_path, config.voice))?;
        if voice_bytes.len() < VOICE_TOKENS * VOICE_DIM * 4 {
            return Err(format!("voice '{}' is truncated", config.voice));
        }
        let voice = f32s(&voice_bytes[..VOICE_TOKENS * VOICE_DIM * 4]);

        let lexicon = container
            .named("en-lexicon")
            .ok_or("the bundle has no en-lexicon")?;
        let g2p = g2p::EnglishG2p::new(lexicon, map.clone())?;
        let normalizer = container
            .named("en-textnorm")
            .map(|t| normalizer::TextNormalizer::parse(&String::from_utf8_lossy(t)))
            .transpose()?;

        let runtime = format!(
            "acoustic: {}; vocoder: {}",
            acoustic.describe(),
            vocoder.describe()
        );
        Ok(Self {
            acoustic,
            vocoder,
            acoustic_sig,
            vocoder_sig,
            g2p,
            normalizer,
            voice,
            british: config.language.eq_ignore_ascii_case("en-gb"),
            capacity,
            frames,
            vocoder_length_i64: length_bytes == 8,
            vocoder_frames: Some(frames),
            runtime,
        })
    }

    /// Kokoro phonemes for `text`.
    pub fn phonemize(&self, text: &str) -> Result<String, String> {
        let text = match &self.normalizer {
            Some(n) => n.normalize(text),
            None => text.to_string(),
        };
        self.g2p.text_to_ipa(&text, self.british)
    }

    /// Speech for `text`, one call of `sink` per slice (24 kHz mono).
    pub fn synthesize(
        &mut self,
        text: &str,
        mut sink: impl FnMut(Vec<f32>),
    ) -> Result<Timing, String> {
        let mut timing = Timing::default();
        let start = Instant::now();
        let ipa = self.phonemize(text)?;
        let (ids, _dropped) = tokens::phoneme_ids(&ipa);
        timing.phonemize_ms = ms(start);
        if ids.len() <= 2 {
            return Ok(timing);
        }
        timing.tokens = ids.len();
        let capacity = (self.capacity.saturating_sub(4))
            .min(self.frames / FRAMES_PER_TOKEN_BUDGET)
            .max(1);
        for slice in tokens::slice(&ids, capacity) {
            let pcm = self.run_slice(&slice.ids, slice.before, slice.after, &mut timing)?;
            timing.slices += 1;
            sink(pcm);
        }
        Ok(timing)
    }

    fn run_slice(
        &mut self,
        ids: &[i64],
        before: Join,
        after: Join,
        timing: &mut Timing,
    ) -> Result<Vec<f32>, String> {
        let start = Instant::now();
        let n = ids.len().min(self.capacity);
        let style = &self.voice[(n - 1).min(VOICE_TOKENS - 1) * VOICE_DIM..][..VOICE_DIM];
        let (decoder_style, prosody_style) = style.split_at(STYLE_DIM);
        let sig = self.acoustic_sig.clone();
        let a = &mut self.acoustic;
        a.write_bytes(&sig, "phoneme_ids", bytes_of(&ids[..n]))?;
        a.write_bytes(&sig, "speaker_style", bytes_of(prosody_style))?;
        a.write_bytes(&sig, "phoneme_length", bytes_of(&[n as i64]))?;
        a.invoke(&sig)?;
        let sig = sig.as_str();
        let a = &mut self.acoustic;
        let mut raw = Vec::new();
        a.read_bytes(sig, "speech_frame_length", &mut raw)?;
        let predicted = i64::from_le_bytes(raw[..8].try_into().unwrap());
        let frames = (predicted.max(1) as usize).min(self.frames);
        a.read_bytes(sig, "acoustic_features", &mut raw)?;
        let features = raw.clone();
        a.read_bytes(sig, "pitch_contour", &mut raw)?;
        let pitch = raw.clone();
        a.read_bytes(sig, "energy_contour", &mut raw)?;
        let energy = raw.clone();
        timing.acoustic_ms += ms(start);

        let start = Instant::now();
        let sig = self.vocoder_sig.clone();
        let sig = sig.as_str();
        // Shape the vocoder to this slice (a bucket of frames) when it can
        // be; otherwise it renders its whole frame budget.
        let mut width = self.frames;
        if let Some(current) = self.vocoder_frames {
            let bucket = frames.div_ceil(VOCODER_BUCKET).max(1) * VOCODER_BUCKET;
            let bucket = bucket.min(self.frames);
            if bucket != current {
                let b = bucket as i32;
                let shapes: [(&str, &[i32]); 3] = [
                    ("acoustic_features", &[1, FEATURE_DIM as i32, b]),
                    ("pitch_contour", &[1, 2 * b]),
                    ("energy_contour", &[1, 2 * b]),
                ];
                match self.vocoder.resize_inputs(sig, &shapes) {
                    Ok(()) => self.vocoder_frames = Some(bucket),
                    Err(e) => {
                        eprintln!("[kokoro] vocoder stays at {} frames: {e}", self.frames);
                        self.vocoder_frames = None;
                        if current != self.frames {
                            let full = self.frames as i32;
                            let shapes: [(&str, &[i32]); 3] = [
                                ("acoustic_features", &[1, FEATURE_DIM as i32, full]),
                                ("pitch_contour", &[1, 2 * full]),
                                ("energy_contour", &[1, 2 * full]),
                            ];
                            self.vocoder.resize_inputs(sig, &shapes)?;
                        }
                    }
                }
            }
            width = self.vocoder_frames.unwrap_or(self.frames);
        }
        let (features, pitch, energy) = if width == self.frames {
            (features, pitch, energy)
        } else {
            // [512, frames] rows cropped to [512, width]; contours to 2 * width.
            let row = self.frames * 4;
            let mut cropped = Vec::with_capacity(FEATURE_DIM * width * 4);
            for c in 0..FEATURE_DIM {
                cropped.extend_from_slice(&features[c * row..c * row + width * 4]);
            }
            (cropped, pitch[..2 * width * 4].to_vec(), energy[..2 * width * 4].to_vec())
        };
        let v = &mut self.vocoder;
        v.write_bytes(sig, "acoustic_features", &features)?;
        v.write_bytes(sig, "pitch_contour", &pitch)?;
        v.write_bytes(sig, "energy_contour", &energy)?;
        v.write_bytes(sig, "speaker_style", bytes_of(decoder_style))?;
        if self.vocoder_length_i64 {
            v.write_bytes(sig, "speech_frame_length", bytes_of(&[frames as i64]))?;
        } else {
            v.write_bytes(sig, "speech_frame_length", bytes_of(&[frames as i32]))?;
        }
        v.invoke(sig)?;
        v.read_bytes(sig, "magnitude_spectrogram", &mut raw)?;
        let magnitude = f32s(&raw);
        v.read_bytes(sig, "phase_spectrogram", &mut raw)?;
        let phase = f32s(&raw);
        let mut pcm = istft(&magnitude, &phase, frames * (AUDIO_HOP / ISTFT_HOP) + 1);
        pcm.resize(frames.min(width) * AUDIO_HOP, 0.0);
        tokens::trim_join_silence(&mut pcm, before, after);
        timing.vocoder_ms += ms(start);
        Ok(pcm)
    }
}

/// Inverse STFT: 11 bins, 20-sample periodic Hann frames, hop 5, centered
/// (the first half frame trimmed). `magnitude` and `phase` are
/// `[bins, subframes]`.
fn istft(magnitude: &[f32], phase: &[f32], active: usize) -> Vec<f32> {
    let total = magnitude.len() / ISTFT_BINS;
    let active = active.min(total);
    if active == 0 {
        return Vec::new();
    }
    let n = ISTFT_FRAME;
    let scale = 1.0 / (n as f32 * HANN_OLA_SCALE);
    let window: Vec<f32> = (0..n)
        .map(|j| 0.5 * (1.0 - (2.0 * std::f32::consts::PI * j as f32 / n as f32).cos()) * scale)
        .collect();
    // cos/sin table for e^{i 2π k j / N}.
    let table: Vec<(f32, f32)> = (0..n)
        .map(|m| {
            let a = 2.0 * std::f32::consts::PI * m as f32 / n as f32;
            (a.cos(), a.sin())
        })
        .collect();
    let mut acc = vec![0f32; active * ISTFT_HOP + n];
    let clean = |x: f32| if x.is_finite() { x } else { 0.0 };
    let mut re = [0f32; ISTFT_BINS];
    let mut im = [0f32; ISTFT_BINS];
    for m in 0..active {
        for k in 0..ISTFT_BINS {
            let mag = clean(magnitude[k * total + m]);
            let ph = clean(phase[k * total + m]);
            re[k] = mag * ph.cos();
            im[k] = mag * ph.sin();
        }
        let out = &mut acc[m * ISTFT_HOP..m * ISTFT_HOP + n];
        for (j, o) in out.iter_mut().enumerate() {
            // Real inverse DFT of a Hermitian spectrum (DC and Nyquist real).
            let mut x = re[0] + re[n / 2] * if j % 2 == 0 { 1.0 } else { -1.0 };
            for k in 1..n / 2 {
                let (c, s) = table[(k * j) % n];
                x += 2.0 * (re[k] * c - im[k] * s);
            }
            let sample = x * window[j];
            if sample.is_finite() {
                *o += sample;
            }
        }
    }
    let pad = n / 2;
    let len = (active - 1) * ISTFT_HOP;
    acc[pad..pad + len].to_vec()
}

/// 24 kHz → 48 kHz by linear interpolation (for 48 kHz-only consumers).
pub fn upsample_2x(pcm: &[f32]) -> Vec<f32> {
    let mut out = Vec::with_capacity(pcm.len() * 2);
    for (i, &s) in pcm.iter().enumerate() {
        let next = pcm.get(i + 1).copied().unwrap_or(s);
        out.push(s);
        out.push(0.5 * (s + next));
    }
    out
}

