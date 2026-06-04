//! Session trace recorder — capture every node output to JSONL for
//! offline debugging / replay.
//!
//! The recorder is an observer: it does not alter the data path.
//! For each node in the manifest it calls
//! [`SessionControl::subscribe`] against `<node_id>.out`, gets a
//! `broadcast::Receiver<RuntimeData>`, and relays each frame into a
//! single writer task that appends to `<dir>/<session_id>.jsonl`.
//!
//! Enable by setting `REMOTEMEDIA_RECORD_DIR` on the process running
//! the executor; disable by unsetting it. The recorder only attaches
//! at session creation, so enabling/disabling requires a session
//! restart.
//!
//! Audio frames are summarised to `{samples, sample_rate, channels,
//! duration_ms}` — recording raw f32 PCM would quickly balloon trace
//! files into gigabytes. Text and Json payloads are captured in full.
//!
//! The output format is newline-delimited JSON (JSONL). Each line:
//!
//! ```json
//! {
//!   "ts_ms": 1712345678900,
//!   "session_id": "s1712…",
//!   "source": "llm.out",
//!   "kind": "text",
//!   "payload": "Hello world."
//! }
//! ```
//!
//! A sibling CLI (`session-replay`) pretty-prints these timelines.

use crate::data::RuntimeData;
use crate::manifest::Manifest;
use crate::transport::session_control::{ControlAddress, SessionControl};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::{broadcast, mpsc};

/// One captured frame on a tap.
#[derive(Debug, Clone, Serialize)]
pub struct TraceEvent {
    /// Wall-clock time in milliseconds since UNIX epoch.
    pub ts_ms: u64,
    /// Session identifier.
    pub session_id: String,
    /// Tap address, formatted as `<node_id>.out[.port]`.
    pub source: String,
    /// `"text" | "audio" | "json" | "binary" | …`
    pub kind: String,
    /// Payload summary. Text / Json are captured verbatim; audio is
    /// summarised to metadata so trace files don't balloon.
    pub payload: serde_json::Value,
}

impl TraceEvent {
    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn from_data(session_id: &str, source: &str, data: RuntimeData) -> Self {
        let (kind, payload) = summarize(&data);
        Self {
            ts_ms: Self::now_ms(),
            session_id: session_id.to_string(),
            source: source.to_string(),
            kind,
            payload,
        }
    }
}

/// Produce a `(kind, payload)` pair for a `RuntimeData`. Audio frames
/// are reduced to metadata so trace files stay tractable.
fn summarize(data: &RuntimeData) -> (String, serde_json::Value) {
    match data {
        RuntimeData::Text(s) => ("text".to_string(), serde_json::Value::String(s.clone())),
        RuntimeData::Json(v) => ("json".to_string(), v.clone()),
        RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            stream_id,
            timestamp_us,
            ..
        } => {
            let ch = (*channels).max(1) as usize;
            let frames = samples.len() / ch;
            let duration_ms = if *sample_rate > 0 {
                (frames as f64 / *sample_rate as f64 * 1000.0) as u64
            } else {
                0
            };
            (
                "audio".to_string(),
                serde_json::json!({
                    "samples": samples.len(),
                    "sample_rate": sample_rate,
                    "channels": channels,
                    "duration_ms": duration_ms,
                    "stream_id": stream_id,
                    "timestamp_us": timestamp_us,
                }),
            )
        }
        RuntimeData::Binary(b) => (
            "binary".to_string(),
            serde_json::json!({ "bytes": b.len() }),
        ),
        other => (
            format!("{:?}", other.data_type()).to_ascii_lowercase(),
            serde_json::Value::Null,
        ),
    }
}

/// Handle for a running recorder. Dropping this signals shutdown — the
/// writer task flushes and exits when the last sender drops.
pub struct SessionRecorder {
    _writer_tx: mpsc::Sender<TraceEvent>,
    _tap_handles: Vec<tokio::task::JoinHandle<()>>,
    _writer_handle: tokio::task::JoinHandle<()>,
    path: PathBuf,
}

impl SessionRecorder {
    /// Attach a recorder to `control` for every node declared in
    /// `manifest`. Returns `Ok(None)` if `dir` does not exist or is
    /// unusable — callers treat that as "recording disabled".
    pub async fn attach(
        dir: &Path,
        session_id: String,
        control: Arc<SessionControl>,
        manifest: &Manifest,
    ) -> std::io::Result<Self> {
        tokio::fs::create_dir_all(dir).await?;
        let path = dir.join(format!("{}.jsonl", session_id));
        let file = tokio::fs::File::create(&path).await?;
        let mut writer = tokio::io::BufWriter::new(file);

        // Separate the writer task from the tap tasks so one slow
        // subscriber can't block another. Capacity is generous —
        // bursty LLM streaming can easily emit hundreds of frames in
        // a few hundred ms.
        let (writer_tx, mut writer_rx) = mpsc::channel::<TraceEvent>(4096);
        let writer_path = path.clone();
        let writer_handle = tokio::spawn(async move {
            while let Some(ev) = writer_rx.recv().await {
                match serde_json::to_vec(&ev) {
                    Ok(mut buf) => {
                        buf.push(b'\n');
                        if let Err(e) = writer.write_all(&buf).await {
                            tracing::warn!("[recorder] write failed for {:?}: {}", writer_path, e);
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!("[recorder] serialize failed: {}", e);
                    }
                }
            }
            if let Err(e) = writer.flush().await {
                tracing::warn!("[recorder] final flush failed: {}", e);
            }
        });

        // Subscribe to every node's main output. Capture the
        // coordinator's tap side channel here too — its `turn_state`
        // envelopes land on `coordinator.out`.
        let mut tap_handles = Vec::new();
        for node in &manifest.nodes {
            let addr = ControlAddress::node_out(&node.id);
            match control.subscribe(&addr) {
                Ok(rx) => {
                    let source = format!("{}.out", node.id);
                    let handle = spawn_tap_relay(session_id.clone(), source, rx, writer_tx.clone());
                    tap_handles.push(handle);
                }
                Err(e) => {
                    tracing::warn!("[recorder] could not subscribe to {}.out: {}", node.id, e);
                }
            }
        }

        tracing::info!(
            "[recorder] capturing session {} to {:?} ({} taps)",
            session_id,
            path,
            tap_handles.len()
        );

        Ok(Self {
            _writer_tx: writer_tx,
            _tap_handles: tap_handles,
            _writer_handle: writer_handle,
            path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Helper that looks up `REMOTEMEDIA_RECORD_DIR` and attaches a
    /// recorder if it's set. Any failure (missing dir permission,
    /// subscribe error) downgrades to a warning and a `None` return
    /// so a bad trace config never takes out a session.
    pub async fn maybe_attach_from_env(
        session_id: String,
        control: Arc<SessionControl>,
        manifest: &Manifest,
    ) -> Option<Self> {
        let dir = std::env::var("REMOTEMEDIA_RECORD_DIR").ok()?;
        let dir = PathBuf::from(dir);
        match Self::attach(&dir, session_id, control, manifest).await {
            Ok(rec) => Some(rec),
            Err(e) => {
                tracing::warn!("[recorder] attach failed: {}", e);
                None
            }
        }
    }
}

fn spawn_tap_relay(
    session_id: String,
    source: String,
    mut rx: broadcast::Receiver<RuntimeData>,
    writer_tx: mpsc::Sender<TraceEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(data) => {
                    let ev = TraceEvent::from_data(&session_id, &source, data);
                    if writer_tx.send(ev).await.is_err() {
                        break; // writer dropped
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("[recorder] tap {} lagged, dropped {} frames", source, n);
                    let ev = TraceEvent {
                        ts_ms: TraceEvent::now_ms(),
                        session_id: session_id.clone(),
                        source: source.clone(),
                        kind: "lag".into(),
                        payload: serde_json::json!({ "dropped": n }),
                    };
                    let _ = writer_tx.send(ev).await;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}
