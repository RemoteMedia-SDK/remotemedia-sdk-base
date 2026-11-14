//! `gemma-litert <model.litertlm> [prompt ...] [--backend gpu|cpu]
//! [--lib <LiteRT-LM C API library>] [--cache <dir>] [--threads N]`
//!
//! Loads a LiteRT-LM model and streams a reply to each prompt in one
//! conversation, printing time to the first token and to each sentence.

use std::time::Instant;

use remotemedia_litert_nodes::lm::{Engine, LmConfig, Piece, Sentences};

fn main() {
    let mut args = std::env::args().skip(1);
    let mut positional = Vec::new();
    let mut config = LmConfig::new(String::new());
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--backend" => config.backend = args.next().expect("--backend gpu|cpu"),
            "--lib" => config.runtime_lib = Some(args.next().expect("--lib <path>")),
            "--cache" => config.cache_dir = Some(args.next().expect("--cache <dir>")),
            "--threads" => {
                config.threads = args.next().and_then(|n| n.parse().ok()).expect("--threads N")
            }
            _ => positional.push(arg),
        }
    }
    if positional.is_empty() {
        eprintln!("usage: gemma-litert <model.litertlm> [prompt ...] [--backend gpu|cpu] [--lib <path>] [--cache <dir>] [--threads N]");
        std::process::exit(2);
    }
    config.model_path = positional.remove(0);
    if positional.is_empty() {
        positional.push("In two sentences, what is a pixel?".into());
        positional.push("And roughly how big is one on a phone screen?".into());
    }
    let engine = Engine::load(&config).unwrap_or_else(|e| {
        eprintln!("load: {e}");
        std::process::exit(1)
    });
    println!("loaded: {}", engine.runtime);
    let mut conversation = engine
        .conversation("You are a voice assistant in a group chat. Answer briefly, in plain spoken sentences.")
        .unwrap();
    for prompt in positional {
        println!("> {prompt}");
        let start = Instant::now();
        let rx = conversation.send(&prompt, 256).unwrap();
        let mut sentences = Sentences::default();
        let mut first = None;
        let mut pieces = 0;
        loop {
            match rx.recv() {
                Ok(Piece::Text(t)) => {
                    pieces += 1;
                    first.get_or_insert_with(|| start.elapsed().as_secs_f64() * 1000.0);
                    for s in sentences.push(&t) {
                        println!("  [{:>5.0} ms] {s}", start.elapsed().as_secs_f64() * 1000.0);
                    }
                }
                Ok(Piece::Error(e)) => {
                    println!("  error: {e}");
                    break;
                }
                Ok(Piece::Done) | Err(_) => break,
            }
        }
        if let Some(rest) = sentences.finish() {
            println!("  [{:>5.0} ms] {rest}", start.elapsed().as_secs_f64() * 1000.0);
        }
        let total = start.elapsed().as_secs_f64();
        println!(
            "  first token {:.0} ms, {pieces} pieces in {total:.2} s ({:.1}/s), {} tokens in context",
            first.unwrap_or(0.0),
            pieces as f64 / total,
            conversation.tokens()
        );
    }
}
