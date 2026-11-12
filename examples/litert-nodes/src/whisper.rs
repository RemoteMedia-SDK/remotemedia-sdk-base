//! Whisper on LiteRT: a TFLite model with an `encode` signature (log-mel
//! `[1, 80, 3000]` → `[1, 1500, d_model]`) and a `decode` signature (encoder
//! output, `[1, T]` token ids, `[1, 1, T, T]` attention mask → `[1, T, vocab]`
//! logits), decoded greedily the way Whisper does with `without_timestamps`.

use std::time::Instant;

use crate::backend::Backend;
use crate::compiled::{accelerators, CompiledBackend};
use crate::litert::TfLiteBackend;
use crate::mel::{LogMel, N_FRAMES, N_MELS, SAMPLE_RATE};
use crate::tokenizer::Tokenizer;

/// `suppress_tokens` from openai/whisper's generation config (multilingual
/// and `.en` models share it).
const SUPPRESS: &[u32] = &[
    1, 2, 7, 8, 9, 10, 14, 25, 26, 27, 28, 29, 31, 58, 59, 60, 61, 62, 63, 90, 91, 92, 93, 359,
    503, 522, 542, 873, 893, 902, 918, 922, 931, 1350, 1853, 1982, 2460, 2627, 3246, 3253, 3268,
    3536, 3846, 3961, 4183, 4667, 6585, 6647, 7273, 9061, 9383, 10428, 10929, 11938, 12033, 12331,
    12562, 13793, 14157, 14635, 15265, 15618, 16553, 16604, 18362, 18956, 20075, 21675, 22520,
    26130, 26161, 26435, 28279, 29464, 31650, 32302, 32470, 36865, 42863, 47425, 49870, 50254,
];

/// How the decoder's attention mask is filled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaskMode {
    /// 0 where a position may attend, a large negative number elsewhere.
    Additive,
    /// 1 where a position may attend, 0 elsewhere.
    Binary,
}

#[derive(Clone, Debug)]
pub struct WhisperConfig {
    pub model_path: String,
    pub tokenizer_path: String,
    /// LiteRT library to load; `None` tries the platform's usual names.
    pub runtime_lib: Option<String>,
    /// `gpu`, `npu`, `npu,gpu` (LiteRT CompiledModel, CPU as fallback), or
    /// `cpu` (the TFLite interpreter on XNNPACK).
    pub accelerator: String,
    /// Where the NPU dispatch and compiler plugin libraries are.
    pub accelerator_dir: Option<String>,
    pub threads: i32,
    /// ISO 639-1 code; used when the tokenizer has `<|xx|>`.
    pub language: String,
    pub mask: MaskMode,
    pub encode_signature: String,
    pub decode_signature: String,
}

impl WhisperConfig {
    pub fn new(model_path: impl Into<String>) -> Self {
        let model_path = model_path.into();
        let tokenizer_path = std::path::Path::new(&model_path)
            .with_file_name("tokenizer.json")
            .to_string_lossy()
            .into_owned();
        Self {
            model_path,
            tokenizer_path,
            runtime_lib: None,
            accelerator: "gpu".into(),
            accelerator_dir: None,
            threads: 4,
            language: "en".into(),
            mask: MaskMode::Additive,
            encode_signature: "encode".into(),
            decode_signature: "decode".into(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Transcript {
    pub text: String,
    pub tokens: usize,
    pub mel_ms: f64,
    pub encode_ms: f64,
    pub decode_ms: f64,
}

pub struct Whisper {
    backend: Box<dyn Backend>,
    pub runtime: String,
    tokenizer: Tokenizer,
    mel: LogMel,
    mel_buf: Vec<f32>,
    hidden_buf: Vec<f32>,
    logits: Vec<f32>,
    ids: Vec<i32>,
    prompt: Vec<u32>,
    eot: u32,
    window: usize,
    vocab: usize,
    hidden: usize,
    enc: String,
    dec: String,
    suppress: Vec<bool>,
}

fn ms(since: Instant) -> f64 {
    since.elapsed().as_secs_f64() * 1000.0
}

impl Whisper {
    pub fn load(config: &WhisperConfig) -> Result<Self, String> {
        let tokenizer = Tokenizer::from_file(&config.tokenizer_path)?;
        let (enc, dec) = (
            config.encode_signature.clone(),
            config.decode_signature.clone(),
        );
        let mut backend: Box<dyn Backend> = if config.accelerator == "cpu" {
            Box::new(TfLiteBackend::load(
                config.runtime_lib.as_deref(),
                &config.model_path,
                config.threads,
                &[&enc, &dec],
            )?)
        } else {
            Box::new(CompiledBackend::load(
                config.runtime_lib.as_deref(),
                &config.model_path,
                accelerators(&config.accelerator)?,
                config.accelerator_dir.as_deref(),
            )?)
        };
        let runtime = backend.describe();

        if backend.input_len(&enc, "args_0")? != N_MELS * N_FRAMES {
            return Err("the encoder does not take an 80 x 3000 log-mel".into());
        }
        let hidden = backend.output_len(&enc, "output_0")?;
        let window = backend.input_len(&dec, "args_1")?;
        if window <= 8 || backend.input_len(&dec, "args_2")? != window * window {
            return Err(format!("unexpected decoder inputs (token window {window})"));
        }
        if backend.input_len(&dec, "args_0")? != hidden {
            return Err("the decoder's encoder input does not match the encoder output".into());
        }
        let vocab = backend.output_len(&dec, "output_0")? / window;

        let id = |content: &str| {
            tokenizer
                .special_id(content)
                .ok_or_else(|| format!("tokenizer has no {content}"))
        };
        let eot = id("<|endoftext|>")?;
        let mut prompt = vec![id("<|startoftranscript|>")?];
        if let Some(lang) = tokenizer.special_id(&format!("<|{}|>", config.language)) {
            prompt.push(lang);
            prompt.push(id("<|transcribe|>")?);
        }
        prompt.push(id("<|notimestamps|>")?);

        let mut suppress = vec![false; vocab];
        for &t in SUPPRESS {
            if (t as usize) < vocab {
                suppress[t as usize] = true;
            }
        }
        // No special tokens in the text except the end.
        for (t, s) in suppress.iter_mut().enumerate() {
            if t as u32 != eot && tokenizer.is_special(t as u32) {
                *s = true;
            }
        }

        // The causal mask is the same for every step.
        let (allow, block) = match config.mask {
            MaskMode::Additive => (0.0f32, -1e9f32),
            MaskMode::Binary => (1.0, 0.0),
        };
        let mask: Vec<f32> = (0..window * window)
            .map(|k| {
                if k % window <= k / window {
                    allow
                } else {
                    block
                }
            })
            .collect();
        backend.write_f32(&dec, "args_2", &mask)?;

        Ok(Self {
            backend,
            runtime,
            tokenizer,
            mel: LogMel::new(),
            mel_buf: vec![0.0; N_MELS * N_FRAMES],
            hidden_buf: Vec::with_capacity(hidden),
            logits: Vec::with_capacity(vocab),
            ids: vec![0; window],
            prompt,
            eot,
            window,
            vocab,
            hidden,
            enc,
            dec,
            suppress,
        })
    }

    /// Transcribe 16 kHz mono audio (up to 30 s).
    pub fn transcribe(&mut self, audio: &[f32]) -> Result<Transcript, String> {
        let mut out = Transcript::default();
        let start = Instant::now();
        self.mel.compute(audio, &mut self.mel_buf);
        out.mel_ms = ms(start);

        let start = Instant::now();
        self.backend.write_f32(&self.enc, "args_0", &self.mel_buf)?;
        self.backend.invoke(&self.enc)?;
        self.backend
            .read_f32(&self.enc, "output_0", 0..self.hidden, &mut self.hidden_buf)?;
        out.encode_ms = ms(start);

        let start = Instant::now();
        self.backend
            .write_f32(&self.dec, "args_0", &self.hidden_buf)?;
        // Room for the transcript: Whisper speaks ~3 tokens a second; cap well
        // above that, and at the window.
        let seconds = audio.len() as f32 / SAMPLE_RATE as f32;
        let budget = ((seconds * 8.0) as usize + 16).min(self.window - self.prompt.len());
        let mut tokens = self.prompt.clone();
        let mut generated = Vec::new();
        for step in 0..budget {
            self.ids.fill(self.eot as i32);
            for (slot, &t) in self.ids.iter_mut().zip(&tokens) {
                *slot = t as i32;
            }
            self.backend.write_i32(&self.dec, "args_1", &self.ids)?;
            self.backend.invoke(&self.dec)?;
            let pos = tokens.len() - 1;
            self.backend.read_f32(
                &self.dec,
                "output_0",
                pos * self.vocab..(pos + 1) * self.vocab,
                &mut self.logits,
            )?;
            let mut best = (self.eot, f32::NEG_INFINITY);
            for (t, &score) in self.logits.iter().enumerate() {
                if self.suppress[t] || (step == 0 && (t == 220 || t as u32 == self.eot)) {
                    continue;
                }
                if score > best.1 {
                    best = (t as u32, score);
                }
            }
            if best.0 == self.eot {
                break;
            }
            tokens.push(best.0);
            generated.push(best.0);
        }
        out.decode_ms = ms(start);
        out.tokens = generated.len();
        out.text = self.tokenizer.decode(&generated).trim().to_string();
        Ok(out)
    }
}
