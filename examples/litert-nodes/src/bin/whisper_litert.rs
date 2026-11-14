//! Transcribe WAV files with the LiteRT Whisper engine and print timings.
//!
//! whisper-litert <model.tflite> <audio.wav>... [--accel gpu|npu|npu,gpu|cpu]
//!                [--accel-dir DIR] [--threads N] [--lib PATH]
//!                [--mask additive|binary] [--repeat N] [--language xx]

use remotemedia_litert_nodes::mel::to_mono_16k;
use remotemedia_litert_nodes::whisper::MaskMode;
use remotemedia_litert_nodes::{Whisper, WhisperConfig};

fn read_wav(path: &str) -> Result<Vec<f32>, String> {
    let mut reader = hound::WavReader::open(path).map_err(|e| format!("{path}: {e}"))?;
    let spec = reader.spec();
    let samples: Vec<f32> = match spec.sample_format {
        hound::SampleFormat::Float => reader.samples::<f32>().map(|s| s.unwrap_or(0.0)).collect(),
        hound::SampleFormat::Int => {
            let scale = (1i64 << (spec.bits_per_sample - 1)) as f32;
            reader
                .samples::<i32>()
                .map(|s| s.unwrap_or(0) as f32 / scale)
                .collect()
        }
    };
    Ok(to_mono_16k(
        &samples,
        spec.sample_rate,
        spec.channels as u32,
    ))
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut positional = Vec::new();
    let mut config: Option<WhisperConfig> = None;
    let (mut threads, mut lib, mut mask, mut repeat, mut language) =
        (4, None, MaskMode::Additive, 1, None);
    let (mut accel, mut accel_dir) = ("gpu".to_string(), None);
    let mut i = 0;
    while i < args.len() {
        let value = || args.get(i + 1).cloned().expect("flag needs a value");
        match args[i].as_str() {
            "--threads" => threads = value().parse().unwrap(),
            "--lib" => lib = Some(value()),
            "--repeat" => repeat = value().parse().unwrap(),
            "--language" => language = Some(value()),
            "--accel" => accel = value(),
            "--accel-dir" => accel_dir = Some(value()),
            "--mask" => {
                mask = match value().as_str() {
                    "binary" => MaskMode::Binary,
                    _ => MaskMode::Additive,
                }
            }
            other => {
                positional.push(other.to_string());
                i += 1;
                continue;
            }
        }
        i += 2;
    }
    let (model, wavs) = positional
        .split_first()
        .expect("usage: whisper-litert <model.tflite> <audio.wav>...");
    let mut c = WhisperConfig::new(model.clone());
    c.threads = threads;
    c.runtime_lib = lib;
    c.mask = mask;
    c.accelerator = accel;
    c.accelerator_dir = accel_dir;
    if let Some(l) = language {
        c.language = l;
    }
    config.replace(c);
    let config = config.unwrap();

    let start = std::time::Instant::now();
    let mut whisper = Whisper::load(&config).unwrap_or_else(|e| panic!("load: {e}"));
    println!(
        "loaded {} in {:.0} ms\n  runtime: {}",
        config.model_path,
        start.elapsed().as_secs_f64() * 1000.0,
        whisper.runtime
    );
    for wav in wavs {
        let audio = read_wav(wav).unwrap_or_else(|e| panic!("{e}"));
        for _ in 0..repeat {
            let start = std::time::Instant::now();
            let t = whisper
                .transcribe(&audio)
                .unwrap_or_else(|e| panic!("transcribe: {e}"));
            println!(
                "{wav}: {:.2} s audio, {:.0} ms total (mel {:.0}, encode {:.0}, decode {:.0}, {} tokens)\n  {:?}",
                audio.len() as f64 / 16000.0,
                start.elapsed().as_secs_f64() * 1000.0,
                t.mel_ms,
                t.encode_ms,
                t.decode_ms,
                t.tokens,
                t.text
            );
        }
    }
}
