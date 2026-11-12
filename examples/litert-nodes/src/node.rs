//! `WhisperLiteRtNode`: audio in, transcript (Text) out; any other input is
//! ignored, so it can sit behind a node that also emits events.
//!
//! Params (JSON):
//!
//! | Key | Default | |
//! |-----|---------|---|
//! | `model_path` | required | Whisper `.tflite` with `encode` / `decode` signatures |
//! | `tokenizer_path` | `tokenizer.json` beside the model | Hugging Face tokenizer |
//! | `runtime_lib` | platform default | LiteRT library (also `REMOTEMEDIA_LITERT_LIB`) |
//! | `accelerator` | `gpu` | `gpu`, `npu`, `npu,gpu` (CompiledModel) or `cpu` (interpreter) |
//! | `accelerator_dir` | none | where the NPU dispatch / compiler plugin libraries are |
//! | `threads` | 4 | XNNPACK threads (`cpu`) |
//! | `language` | `en` | |
//! | `mask` | `additive` | decoder mask encoding (`additive` or `binary`) |
//!
//! A model path may name an environment variable as `${NAME}`, so one
//! manifest can point at a different location on each runner.

use std::sync::mpsc;
use std::time::Instant;

use async_trait::async_trait;
use remotemedia_plugin_sdk::{
    abi_stable::{
        sabi_trait::TD_Opaque,
        std_types::{RErr, ROk, RResult, RString},
    },
    adapter::StreamingNodeFfiAdapter,
    traits::AsyncStreamingNode,
    types::{Error, RuntimeData},
    FfiNodeBox, FfiNodeFactory, FfiNode_TO,
};
use serde_json::Value;

use crate::mel::to_mono_16k;
use crate::whisper::{MaskMode, Transcript, Whisper, WhisperConfig};

pub const NODE_TYPE: &str = "WhisperLiteRtNode";

struct Job {
    audio: Vec<f32>,
    reply: futures_channel::oneshot::Sender<Result<Transcript, String>>,
}

/// The model lives on its own thread (the interpreter is neither `Send` nor
/// reentrant); turns queue for it.
pub struct WhisperLiteRtNode {
    jobs: std::sync::Mutex<mpsc::Sender<Job>>,
}

/// `${NAME}` → the variable's value.
fn expand(value: &str) -> String {
    let mut out = String::new();
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        match rest[start + 2..].find('}') {
            Some(end) => {
                let name = &rest[start + 2..start + 2 + end];
                out.push_str(&std::env::var(name).unwrap_or_default());
                rest = &rest[start + 3 + end..];
            }
            None => {
                out.push_str(&rest[start..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

pub fn config_from_params(params: &Value) -> Result<WhisperConfig, String> {
    let model_path = params["model_path"]
        .as_str()
        .map(expand)
        .filter(|p| !p.is_empty())
        .ok_or("WhisperLiteRtNode needs params.model_path")?;
    let mut config = WhisperConfig::new(model_path);
    if let Some(p) = params["tokenizer_path"].as_str() {
        config.tokenizer_path = expand(p);
    }
    if let Some(p) = params["runtime_lib"]
        .as_str()
        .map(expand)
        .filter(|p| !p.is_empty())
    {
        config.runtime_lib = Some(p);
    }
    if let Some(a) = params["accelerator"]
        .as_str()
        .map(expand)
        .filter(|a| !a.is_empty())
    {
        config.accelerator = a;
    }
    if let Some(d) = params["accelerator_dir"]
        .as_str()
        .map(expand)
        .filter(|d| !d.is_empty())
    {
        config.accelerator_dir = Some(d);
    }
    if let Some(t) = params["threads"].as_i64() {
        config.threads = t as i32;
    }
    if let Some(l) = params["language"].as_str() {
        config.language = l.to_string();
    }
    config.mask = match params["mask"].as_str() {
        None | Some("additive") => MaskMode::Additive,
        Some("binary") => MaskMode::Binary,
        Some(other) => return Err(format!("unknown mask '{other}'")),
    };
    Ok(config)
}

impl WhisperLiteRtNode {
    pub fn start(config: WhisperConfig) -> Result<Self, String> {
        let (jobs_tx, jobs_rx) = mpsc::channel::<Job>();
        let (ready_tx, ready_rx) = mpsc::channel::<Result<String, String>>();
        std::thread::Builder::new()
            .name("whisper-litert".into())
            .spawn(move || {
                let mut whisper = match Whisper::load(&config) {
                    Ok(w) => {
                        let _ = ready_tx.send(Ok(w.runtime.clone()));
                        w
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };
                while let Ok(job) = jobs_rx.recv() {
                    let _ = job.reply.send(whisper.transcribe(&job.audio));
                }
            })
            .map_err(|e| e.to_string())?;
        let runtime = ready_rx
            .recv()
            .map_err(|_| "the model thread exited while loading".to_string())??;
        eprintln!("[{NODE_TYPE}] loaded with {runtime}");
        Ok(Self {
            jobs: std::sync::Mutex::new(jobs_tx),
        })
    }

    async fn transcribe(&self, audio: Vec<f32>) -> Result<Transcript, Error> {
        let (reply, answer) = futures_channel::oneshot::channel();
        self.jobs
            .lock()
            .unwrap()
            .send(Job { audio, reply })
            .map_err(|_| Error::Execution("whisper model thread is gone".into()))?;
        answer
            .await
            .map_err(|_| Error::Execution("whisper model thread is gone".into()))?
            .map_err(Error::Execution)
    }
}

#[async_trait]
impl AsyncStreamingNode for WhisperLiteRtNode {
    fn node_type(&self) -> &str {
        NODE_TYPE
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        match data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                ..
            } => {
                let audio = to_mono_16k(&samples, sample_rate, channels);
                Ok(RuntimeData::Text(self.transcribe(audio).await?.text))
            }
            other => Err(Error::InvalidData(format!(
                "{NODE_TYPE} takes audio, not {}",
                other.data_type()
            ))),
        }
    }

    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        _session_id: Option<String>,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            ..
        } = data
        else {
            return Ok(0);
        };
        let audio = to_mono_16k(&samples, sample_rate, channels);
        let seconds = audio.len() as f64 / 16_000.0;
        let start = Instant::now();
        let transcript = self.transcribe(audio).await?;
        eprintln!(
            "[{NODE_TYPE}] {seconds:.2} s of audio in {:.0} ms (mel {:.0}, encode {:.0}, decode {:.0}, {} tokens): {:?}",
            start.elapsed().as_secs_f64() * 1000.0,
            transcript.mel_ms,
            transcript.encode_ms,
            transcript.decode_ms,
            transcript.tokens,
            transcript.text
        );
        if transcript.text.is_empty() {
            return Ok(0);
        }
        callback(RuntimeData::Text(transcript.text))?;
        Ok(1)
    }
}

#[derive(Default)]
pub struct WhisperLiteRtFactory;

impl FfiNodeFactory for WhisperLiteRtFactory {
    fn node_type(&self) -> RString {
        NODE_TYPE.into()
    }

    fn create(&self, params: RString) -> RResult<FfiNodeBox, RString> {
        let params: Value = serde_json::from_str(params.as_str()).unwrap_or(Value::Null);
        match config_from_params(&params).and_then(WhisperLiteRtNode::start) {
            Ok(node) => ROk(FfiNode_TO::from_value(
                StreamingNodeFfiAdapter::new(node),
                TD_Opaque,
            )),
            Err(e) => RErr(format!("{NODE_TYPE}: {e}").into()),
        }
    }
}
