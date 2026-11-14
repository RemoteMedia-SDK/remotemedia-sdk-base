//! `kokoro-litert <model.litertlm> <text> [--out speech.wav] [--voice af_heart]
//! [--accel gpu|cpu] [--lib <LiteRT library>] [--repeat N] [--phonemes]`
//!
//! Speaks `text` with Kokoro-82M from its `.litertlm` bundle and writes a
//! 24 kHz WAV; prints the phonemes and the time each stage took.

use std::time::Instant;

use remotemedia_litert_nodes::kokoro::{Kokoro, KokoroConfig};

fn main() {
    let mut args = std::env::args().skip(1);
    let mut positional = Vec::new();
    let mut out = "speech.wav".to_string();
    let mut repeat = 1;
    let mut config = KokoroConfig::new(String::new());
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out" => out = args.next().expect("--out <path>"),
            "--voice" => config.voice = args.next().expect("--voice <name>"),
            "--accel" => config.vocoder_accelerator = args.next().expect("--accel gpu|cpu"),
            "--lib" => config.runtime_lib = Some(args.next().expect("--lib <path>")),
            "--vocoder" => config.vocoder_path = Some(args.next().expect("--vocoder <tflite>")),
            "--gpu-options" => config.gpu_options = Some(args.next().expect("--gpu-options <toml>")),
            "--language" => config.language = args.next().expect("--language en-us|en-gb"),
            "--repeat" => repeat = args.next().and_then(|n| n.parse().ok()).expect("--repeat N"),
            _ => positional.push(arg),
        }
    }
    if positional.len() != 2 {
        eprintln!("usage: kokoro-litert <model.litertlm> <text> [--out speech.wav] [--voice af_heart] [--accel gpu|cpu] [--lib <LiteRT library>] [--repeat N]");
        std::process::exit(2);
    }
    config.model_path = positional[0].clone();
    let text = &positional[1];

    let start = Instant::now();
    let mut kokoro = Kokoro::load(&config).unwrap_or_else(|e| {
        eprintln!("load: {e}");
        std::process::exit(1)
    });
    println!("loaded in {:.0} ms: {}", start.elapsed().as_secs_f64() * 1000.0, kokoro.runtime);
    match kokoro.phonemize(text) {
        Ok(ipa) => println!("phonemes: {ipa}"),
        Err(e) => println!("phonemes: error {e}"),
    }

    let mut audio = Vec::new();
    for round in 0..repeat {
        audio.clear();
        let start = Instant::now();
        let mut first = None;
        let timing = kokoro
            .synthesize(text, |pcm| {
                first.get_or_insert_with(|| start.elapsed().as_secs_f64() * 1000.0);
                audio.extend(pcm)
            })
            .unwrap_or_else(|e| {
                eprintln!("synthesize: {e}");
                std::process::exit(1)
            });
        let total = start.elapsed().as_secs_f64() * 1000.0;
        let seconds = audio.len() as f64 / 24_000.0;
        println!(
            "round {round}: {seconds:.2} s of speech in {total:.0} ms (first audio {:.0} ms; phonemize {:.1}, acoustic {:.0}, vocoder {:.0}; {} tokens, {} slices; {:.1}x real time)",
            first.unwrap_or(0.0),
            timing.phonemize_ms,
            timing.acoustic_ms,
            timing.vocoder_ms,
            timing.tokens,
            timing.slices,
            seconds * 1000.0 / total
        );
    }
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: 24_000,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut writer = hound::WavWriter::create(&out, spec).expect("create wav");
    for s in &audio {
        writer
            .write_sample((s.clamp(-1.0, 1.0) * 32767.0) as i16)
            .unwrap();
    }
    writer.finalize().unwrap();
    println!("wrote {out}");
}
