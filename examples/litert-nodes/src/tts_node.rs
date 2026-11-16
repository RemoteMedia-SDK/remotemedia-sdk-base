//! `KokoroTtsNode`: text in, Kokoro-82M speech out (24 kHz mono), streamed
//! in chunks as each slice is synthesized.
//!
//! Speaks Text, and JSON with a `say` field (what `GemmaAssistantNode`
//! emits per sentence); other input is ignored.
//!
//! Params (JSON):
//!
//! | Key | Default | |
//! |-----|---------|---|
//! | `model_path` | required | Kokoro `.litertlm` bundle (`${VAR}` expands) |
//! | `runtime_lib` | platform default | LiteRT library (also `REMOTEMEDIA_LITERT_LIB`) |
//! | `accelerator` | `gpu` | `gpu` (vocoder on the GPU) or `cpu` (TFLite interpreter) |
//! | `voice` | `af_heart` | one of the bundle's 54 voices |
//! | `language` | `en-us` | `en-us` or `en-gb` |
//! | `threads` | 4 | XNNPACK threads |
//! | `gpu_options` | none | LiteRT GPU options (TOML) |
//! | `chunk_ms` | 500 | audio per output frame |

use std::sync::{mpsc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use futures_util::StreamExt;
use remotemedia_plugin_sdk::{
    abi_stable::{
        sabi_trait::TD_Opaque,
        std_types::{RErr, ROk, RResult, RString},
    },
    adapter::StreamingNodeFfiAdapter,
    traits::{
        AsyncStreamingNode, AudioGenerator, CancelContext, CancelStrategy, CancelToken,
        Cancellable, Disposition, GeneratorRole, NodeRuntimeContextRead, Retention,
    },
    types::{Error, RuntimeData},
    FfiNodeBox, FfiNodeFactory, FfiNode_TO,
};
use serde_json::Value;

use crate::kokoro::tokens::SAMPLE_RATE;
use crate::kokoro::{Kokoro, KokoroConfig};
use crate::node::expand;

pub const NODE_TYPE: &str = "KokoroTtsNode";

enum Out {
    Audio(Vec<f32>),
    Done(Result<String, String>),
}

struct Job {
    text: String,
    out: futures_channel::mpsc::UnboundedSender<Out>,
    /// The call's cancellation: synthesis stops between model invocations.
    cancel: CancelToken,
}

pub struct KokoroTtsNode {
    jobs: Mutex<mpsc::Sender<Job>>,
    chunk: usize,
    /// One utterance at a time, in order.
    turn: futures_util::lock::Mutex<()>,
}

pub fn config_from_params(params: &Value) -> Result<(KokoroConfig, usize), String> {
    let s = |key: &str| params[key].as_str().map(expand).filter(|v| !v.is_empty());
    let mut config =
        KokoroConfig::new(s("model_path").ok_or("KokoroTtsNode needs params.model_path")?);
    config.runtime_lib = s("runtime_lib");
    if let Some(a) = s("accelerator") {
        config.vocoder_accelerator = a;
    }
    if let Some(v) = s("voice") {
        config.voice = v;
    }
    if let Some(l) = s("language") {
        config.language = l;
    }
    config.gpu_options = s("gpu_options");
    if let Some(t) = params["threads"].as_i64() {
        config.threads = t as i32;
    }
    let chunk_ms = params["chunk_ms"].as_u64().unwrap_or(500).max(20) as usize;
    Ok((config, SAMPLE_RATE as usize * chunk_ms / 1000))
}

impl KokoroTtsNode {
    pub fn start(config: KokoroConfig, chunk: usize) -> Result<Self, String> {
        let (jobs_tx, jobs_rx) = mpsc::channel::<Job>();
        let (ready_tx, ready_rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("kokoro".into())
            .spawn(move || {
                let mut kokoro = match Kokoro::load(&config) {
                    Ok(k) => {
                        let _ = ready_tx.send(Ok(k.runtime.clone()));
                        k
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };
                while let Ok(job) = jobs_rx.recv() {
                    // Cancelled while it waited its turn: nothing to say.
                    if job.cancel.is_cancelled() {
                        let _ = job.out.unbounded_send(Out::Done(Ok("cancelled before it started".into())));
                        continue;
                    }
                    let out = job.out.clone();
                    let cancel = job.cancel.clone();
                    let result = kokoro
                        .synthesize_until(
                            &job.text,
                            |pcm| {
                                let _ = out.unbounded_send(Out::Audio(pcm));
                            },
                            // Stop when the call is cancelled, or nobody
                            // reads the audio any more.
                            || cancel.is_cancelled() || out.is_closed(),
                        )
                        .map(|t| {
                            format!(
                                "phonemes {:.0} ms, acoustic {:.0} ms, vocoder {:.0} ms, {} slices{}",
                                t.phonemize_ms,
                                t.acoustic_ms,
                                t.vocoder_ms,
                                t.slices,
                                if t.stopped { ", stopped" } else { "" }
                            )
                        });
                    let _ = job.out.unbounded_send(Out::Done(result));
                }
            })
            .map_err(|e| e.to_string())?;
        let runtime = ready_rx
            .recv()
            .map_err(|_| "the model thread exited while loading".to_string())??;
        eprintln!("[{NODE_TYPE}] loaded: {runtime}");
        Ok(Self {
            jobs: Mutex::new(jobs_tx),
            chunk,
            turn: futures_util::lock::Mutex::new(()),
        })
    }
}

fn text_of(data: &RuntimeData) -> Option<String> {
    match data {
        RuntimeData::Text(t) => Some(t.clone()),
        RuntimeData::Json(v) => v["say"].as_str().map(str::to_string),
        _ => None,
    }
    .map(|t| t.trim().to_string())
    .filter(|t| !t.is_empty())
}

/// An audio generator (openspec: add-generator-cancellation). Synthesis runs
/// on its own thread, which stops between model invocations once the call's token
/// fires. Its queued text belongs to the reply that was cancelled, so by
/// default an interrupt discards it.
#[async_trait]
impl Cancellable for KokoroTtsNode {
    fn cancel_strategy(&self) -> CancelStrategy {
        CancelStrategy::ObservesToken
    }

    fn default_disposition(&self, cancel: &CancelContext) -> Disposition {
        Disposition {
            pending_input: Retention::Discard,
            ..cancel.reason.default_disposition()
        }
    }
}
impl AudioGenerator for KokoroTtsNode {}

#[async_trait]
impl AsyncStreamingNode for KokoroTtsNode {
    fn node_type(&self) -> &str {
        NODE_TYPE
    }

    fn generator(&self) -> Option<GeneratorRole<'_>> {
        GeneratorRole::audio(self)
    }

    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData, Error> {
        Err(Error::InvalidData(format!("{NODE_TYPE} streams its audio")))
    }

    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        _session_id: Option<String>,
        callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        self.speak(data, CancelToken::never(), callback).await
    }

    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        self.speak(data, ctx.cancel_token(), callback).await
    }
}

impl KokoroTtsNode {
    async fn speak<F>(
        &self,
        data: RuntimeData,
        cancel: CancelToken,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let Some(text) = text_of(&data) else {
            return Ok(0);
        };
        let reply = match &data {
            RuntimeData::Json(value) => value["reply"].as_str().unwrap_or(""),
            _ => "",
        };
        let trace = crate::stage_trace::Stage::new(
            "tts",
            reply,
            serde_json::json!({"characters":text.chars().count()}),
        );
        let _turn = self.turn.lock().await;
        trace.mark("admitted", serde_json::json!({}));
        let (tx, mut rx) = futures_channel::mpsc::unbounded();
        self.jobs
            .lock()
            .unwrap()
            .send(Job {
                text: text.clone(),
                out: tx,
                cancel: cancel.clone(),
            })
            .map_err(|_| Error::Execution("the Kokoro thread is gone".into()))?;
        let start = Instant::now();
        let mut first = None;
        let mut samples = 0usize;
        let mut frames = 0;
        while let Some(out) = rx.next().await {
            if cancel.is_cancelled() {
                trace.finish(serde_json::json!({"outcome":"cancelled","samples":samples}));
                return Ok(frames);
            }
            match out {
                Out::Audio(pcm) => {
                    if first.is_none() {
                        trace.mark(
                            "first_audio",
                            serde_json::json!({"sample_rate":SAMPLE_RATE}),
                        );
                    }
                    first.get_or_insert_with(|| start.elapsed().as_secs_f64() * 1000.0);
                    for chunk in pcm.chunks(self.chunk) {
                        if cancel.is_cancelled() {
                            trace.finish(serde_json::json!({"outcome":"cancelled","samples":samples}));
                            return Ok(frames);
                        }
                        callback(RuntimeData::Audio {
                            samples: chunk.to_vec().into(),
                            sample_rate: SAMPLE_RATE,
                            channels: 1,
                            stream_id: None,
                            timestamp_us: None,
                            arrival_ts_us: None,
                            metadata: None,
                        })?;
                        samples += chunk.len();
                        frames += 1;
                    }
                }
                Out::Done(Ok(timing)) => {
                    trace.finish(serde_json::json!({"outcome":"complete","samples":samples,
                        "sample_rate":SAMPLE_RATE,"generation_first_audio_ms":first,
                        "generation_total_ms":start.elapsed().as_secs_f64() * 1000.0}));
                    eprintln!(
                        "[{NODE_TYPE}] {:.2} s of speech, first audio {:.0} ms, all {:.0} ms ({timing}): {text:?}",
                        samples as f64 / SAMPLE_RATE as f64,
                        first.unwrap_or(0.0),
                        start.elapsed().as_secs_f64() * 1000.0
                    );
                    break;
                }
                Out::Done(Err(e)) => return Err(Error::Execution(format!("{NODE_TYPE}: {e}"))),
            }
        }
        Ok(frames)
    }
}

#[derive(Default)]
pub struct KokoroTtsFactory;

#[cfg(test)]
mod tests {
    use super::*;
    use remotemedia_plugin_sdk::traits::{CancelReason, CancelSource, Cancellation};

    #[tokio::test]
    async fn cancellation_discards_remaining_pcm_chunks_without_affecting_next_call() {
        // Explicit PCM worker fixture: tests publication, not model cancellation.
        let (jobs, incoming) = mpsc::channel::<Job>();
        let worker = std::thread::spawn(move || {
            for job in incoming.into_iter().take(2) {
                let _ = job.out.unbounded_send(Out::Audio(vec![0.25; 12]));
                let _ = job.out.unbounded_send(Out::Done(Ok("fixture".into())));
            }
        });
        let node = KokoroTtsNode {
            jobs: Mutex::new(jobs),
            chunk: 4,
            turn: futures_util::lock::Mutex::new(()),
        };
        let cancel = CancelSource::new();
        let mut published = 0;
        let first = node
            .speak(RuntimeData::Text("first".into()), cancel.token(), |_| {
                published += 1;
                cancel.cancel(Cancellation::new(CancelReason::BargeIn));
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!((first, published), (1, 1));
        let next = node
            .speak(RuntimeData::Text("next".into()), CancelToken::never(), |_| {
                published += 1;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!((next, published), (3, 4));
        worker.join().unwrap();
    }
}

impl FfiNodeFactory for KokoroTtsFactory {
    fn node_type(&self) -> RString {
        NODE_TYPE.into()
    }

    fn create(&self, params: RString) -> RResult<FfiNodeBox, RString> {
        let params: Value = serde_json::from_str(params.as_str()).unwrap_or(Value::Null);
        match config_from_params(&params).and_then(|(c, chunk)| KokoroTtsNode::start(c, chunk)) {
            Ok(node) => ROk(FfiNode_TO::from_value(
                StreamingNodeFfiAdapter::new(node),
                TD_Opaque,
            )),
            Err(e) => RErr(format!("{NODE_TYPE}: {e}").into()),
        }
    }
}
