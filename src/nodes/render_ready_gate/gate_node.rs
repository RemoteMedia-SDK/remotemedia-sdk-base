//! See `mod.rs` for design rationale. This file holds the actual
//! `StreamingNode` implementation.

use crate::data::RuntimeData;
use crate::error::{Error, Result};
use crate::nodes::AsyncStreamingNode;
use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Emit a structured timing event for an outgoing gate envelope.
/// Phase is one of: `"drain"` (post-open buffer flush), `"live"` (gated
/// passthrough after open), `"passthrough"` (non-gated kind, never
/// buffered).
fn log_gate_emit(data: &RuntimeData, phase: &'static str) {
    let (kind_str, pts_ms) = match data {
        RuntimeData::Audio {
            metadata,
            timestamp_us,
            ..
        } => {
            let pts = metadata
                .as_ref()
                .and_then(|m| m.get("pts_us"))
                .and_then(|v| v.as_u64())
                .or(*timestamp_us)
                .map(|us| (us / 1000) as i64)
                .unwrap_or(-1);
            ("audio", pts)
        }
        RuntimeData::Json(v) => {
            let kind = v.get("kind").and_then(|k| k.as_str()).unwrap_or("json");
            let pts = v
                .get("pts_ms")
                .and_then(|p| p.as_u64())
                .map(|u| u as i64)
                .unwrap_or(-1);
            (kind, pts)
        }
        _ => ("other", -1),
    };
    tracing::info!(
        target: "timing",
        stage = "gate_emit",
        pts_ms = pts_ms,
        kind = kind_str,
        phase = phase,
    );
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct RenderReadyGateConfig {
    /// Json `kind` values that should be buffered while the gate is
    /// closed. Anything not in this set passes through immediately.
    /// Default: `["blendshapes", "skeletal_pose"]` (the renderer's
    /// pose-channel envelopes).
    pub gated_kinds: Vec<String>,
    /// Json `kind` value that opens the gate. Default `"render_ready"`.
    pub open_kind: String,
    /// When `true`, sleep between drained / passthrough envelopes so
    /// they reach downstream consumers at content-time pacing
    /// (`pts_ms` rate). Default `true`. Live pipelines with already-
    /// paced upstream emitters can safely set this to `false`.
    pub pace_realtime: bool,
    /// Maximum buffered envelopes while gate is closed. Older envelopes
    /// are dropped (FIFO) once the cap is hit, so we don't hold on to
    /// stale lip sync if the renderer takes minutes to come up.
    /// Default `4096` (~2 minutes at 30 Hz blendshapes).
    pub buffer_capacity: usize,
}

impl Default for RenderReadyGateConfig {
    fn default() -> Self {
        Self {
            gated_kinds: vec!["blendshapes".into(), "skeletal_pose".into()],
            open_kind: "render_ready".into(),
            pace_realtime: true,
            buffer_capacity: 4096,
        }
    }
}

pub struct RenderReadyGateNode {
    config: RenderReadyGateConfig,
    /// FIFO of envelopes received while the gate was closed.
    buffer: Arc<Mutex<Vec<RuntimeData>>>,
    /// `true` once the first `render_ready` envelope has been seen.
    open: Arc<AtomicBool>,
    /// Pacing anchor — `(wallclock_at_first_paced_emit, pts_ms_of_first_paced_emit)`.
    /// Set on the first paced emission AFTER the gate opens. Reset on
    /// `process_control_message` if we ever wire up barge-in.
    pace_anchor: Arc<Mutex<Option<(Instant, u64)>>>,
}

impl RenderReadyGateNode {
    pub fn new(config: RenderReadyGateConfig) -> Self {
        Self {
            config,
            buffer: Arc::new(Mutex::new(Vec::new())),
            open: Arc::new(AtomicBool::new(false)),
            pace_anchor: Arc::new(Mutex::new(None)),
        }
    }

    /// Inspect a `RuntimeData::Json` envelope's top-level `kind` field,
    /// returning `Some(&str)` if present.
    fn json_kind(data: &RuntimeData) -> Option<&str> {
        match data {
            RuntimeData::Json(v) => v.get("kind").and_then(|k| k.as_str()),
            _ => None,
        }
    }

    /// Extract the content-time pts (in ms) from an envelope, if any.
    /// Used to sort the drain buffer so envelopes from different kinds
    /// (skel + bs + audio) interleave by content time rather than by
    /// arrival order — without this, a stretch of skel-only arrivals
    /// between two bs envelopes forces the trailing bs to wait for all
    /// those skels to pace through, even when their content time has
    /// long since elapsed.
    fn pts_ms_of(data: &RuntimeData) -> Option<u64> {
        match data {
            RuntimeData::Json(v) => v.get("pts_ms").and_then(|p| p.as_u64()),
            RuntimeData::Audio {
                metadata,
                timestamp_us,
                ..
            } => metadata
                .as_ref()
                .and_then(|m| m.get("pts_us"))
                .and_then(|v| v.as_u64())
                .or(*timestamp_us)
                .map(|us| us / 1000),
            _ => None,
        }
    }

    /// Pace a single envelope by its `pts_ms` (Json) or `pts_us`
    /// metadata (Audio). Non-paceable envelopes return immediately.
    async fn pace_emit(&self, out: RuntimeData) -> Option<RuntimeData> {
        if !self.config.pace_realtime {
            return Some(out);
        }
        let pts_ms = match &out {
            RuntimeData::Json(v) => v.get("pts_ms").and_then(|p| p.as_u64()),
            RuntimeData::Audio {
                metadata,
                timestamp_us,
                ..
            } => {
                // Prefer upstream `pts_us` (content time stamped by
                // KokoroTTS / FastResample); fall back to `timestamp_us`.
                metadata
                    .as_ref()
                    .and_then(|m| m.get("pts_us"))
                    .and_then(|v| v.as_u64())
                    .or(*timestamp_us)
                    .map(|us| us / 1000)
            }
            _ => None,
        };
        let Some(pts_ms) = pts_ms else {
            return Some(out);
        };

        let now = Instant::now();
        let (anchor_wall, anchor_pts) = {
            let mut anchor = self.pace_anchor.lock();
            match *anchor {
                Some(a) => a,
                None => {
                    tracing::info!(
                        target: "render_ready_gate",
                        "pacing anchor set at pts_ms={}", pts_ms
                    );
                    *anchor = Some((now, pts_ms));
                    (now, pts_ms)
                }
            }
        };

        let target = anchor_wall + Duration::from_millis(pts_ms.saturating_sub(anchor_pts));
        if target > now {
            tokio::time::sleep(target - now).await;
        }
        Some(out)
    }

    /// `true` if `data` should be buffered/paced like the gated kinds.
    /// Returns true for both Json envelopes whose `kind` is in
    /// `gated_kinds` AND for `RuntimeData::Audio` (which is treated as
    /// a gated kind unconditionally so cc_render receives a single
    /// content-time-aligned stream of pose + audio updates).
    fn is_gated(&self, data: &RuntimeData) -> bool {
        match data {
            RuntimeData::Audio { .. } => true,
            RuntimeData::Json(v) => v
                .get("kind")
                .and_then(|k| k.as_str())
                .map(|k| self.config.gated_kinds.iter().any(|g| g == k))
                .unwrap_or(false),
            _ => false,
        }
    }
}

#[async_trait]
impl AsyncStreamingNode for RenderReadyGateNode {
    fn node_type(&self) -> &str {
        "RenderReadyGateNode"
    }

    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        _session_id: Option<String>,
        mut callback: F,
    ) -> Result<usize>
    where
        F: FnMut(RuntimeData) -> Result<()> + Send,
    {
        let kind = Self::json_kind(&data).map(|s| s.to_string());
        // Diagnostic: per-envelope ingress (kind + pts + open state).
        let dbg_kind = match &data {
            RuntimeData::Audio { .. } => "audio",
            RuntimeData::Json(_) => kind.as_deref().unwrap_or("json"),
            RuntimeData::Video { .. } => "video",
            _ => "other",
        };
        let dbg_pts: i64 = match &data {
            RuntimeData::Json(v) => v
                .get("pts_ms")
                .and_then(|p| p.as_u64())
                .map(|u| u as i64)
                .unwrap_or(-1),
            RuntimeData::Audio {
                metadata,
                timestamp_us,
                ..
            } => metadata
                .as_ref()
                .and_then(|m| m.get("pts_us"))
                .and_then(|v| v.as_u64())
                .or(*timestamp_us)
                .map(|us| (us / 1000) as i64)
                .unwrap_or(-1),
            _ => -1,
        };
        let was_open = self.open.load(Ordering::Acquire);
        tracing::info!(
            target: "timing",
            stage = "gate_recv",
            pts_ms = dbg_pts,
            kind = dbg_kind,
            open = was_open as u64,
        );

        // (1) Open trigger.
        if kind.as_deref() == Some(self.config.open_kind.as_str()) {
            // First open: drain the buffer paced by pts_ms. Subsequent
            // opens are no-ops (the gate is already open).
            if !self.open.swap(true, Ordering::AcqRel) {
                let mut buffered: Vec<RuntimeData> = std::mem::take(&mut *self.buffer.lock());
                let n = buffered.len();
                // Stable-sort by content-time pts_ms so envelopes from
                // different kinds interleave correctly during drain.
                // Without this, drain order = arrival order, which
                // skews when one stream arrives in bursts (e.g.
                // audio2face emits 30 bs in <1 ms, then waits 800 ms
                // for the next audio chunk; meanwhile motion_player
                // streams skel poses at content rate). Sorting by pts
                // means each pace_emit's `target = anchor + pts_ms`
                // sleep happens once per content tick rather than once
                // per envelope-of-mixed-kinds, eliminating multi-second
                // stalls when a stream falls out of phase. Envelopes
                // without a pts_ms (control envelopes) sort to the
                // front so they emit immediately.
                buffered.sort_by_key(|env| Self::pts_ms_of(env).unwrap_or(0));
                tracing::info!(
                    target: "render_ready_gate",
                    "open: draining {} buffered envelopes (pace_realtime={}, sorted_by_pts)",
                    n, self.config.pace_realtime
                );
                let mut emitted = 0;
                for env in buffered {
                    if let Some(out) = self.pace_emit(env).await {
                        log_gate_emit(&out, "drain");
                        callback(out)?;
                        emitted += 1;
                    }
                }
                return Ok(emitted);
            }
            // Already open — swallow the duplicate trigger.
            return Ok(0);
        }

        // (2) Gated kind while gate is closed → buffer.
        let gated = self.is_gated(&data);

        if gated && !self.open.load(Ordering::Acquire) {
            let mut buf = self.buffer.lock();
            if buf.len() >= self.config.buffer_capacity {
                buf.remove(0);
            }
            buf.push(data);
            return Ok(0);
        }

        // (3) Gate open OR input non-gated → emit.
        if gated {
            if let Some(out) = self.pace_emit(data).await {
                log_gate_emit(&out, "live");
                callback(out)?;
                return Ok(1);
            }
            return Ok(0);
        }
        log_gate_emit(&data, "passthrough");
        callback(data)?;
        Ok(1)
    }

    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData> {
        Err(Error::Execution(
            "RenderReadyGateNode requires streaming mode — use process_streaming()".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    fn make_blendshape(pts_ms: u64) -> RuntimeData {
        let arkit: Vec<f32> = vec![0.0; 52];
        RuntimeData::Json(serde_json::json!({
            "kind": "blendshapes",
            "pts_ms": pts_ms,
            "arkit_52": arkit,
        }))
    }

    fn make_open() -> RuntimeData {
        RuntimeData::Json(serde_json::json!({"kind": "render_ready"}))
    }

    /// Closed gate buffers; open trigger drains.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn buffers_then_drains_on_open() {
        let node = RenderReadyGateNode::new(RenderReadyGateConfig {
            pace_realtime: false, // tests don't sleep
            ..Default::default()
        });
        let received: Arc<StdMutex<Vec<RuntimeData>>> = Arc::default();
        let r = Arc::clone(&received);
        let cb = move |out: RuntimeData| -> Result<()> {
            r.lock().unwrap().push(out);
            Ok(())
        };

        // Three envelopes while gate closed → all buffered.
        node.process_streaming(make_blendshape(0), None, cb.clone())
            .await
            .unwrap();
        node.process_streaming(make_blendshape(33), None, cb.clone())
            .await
            .unwrap();
        node.process_streaming(make_blendshape(66), None, cb.clone())
            .await
            .unwrap();
        assert_eq!(
            received.lock().unwrap().len(),
            0,
            "nothing should leak before open"
        );

        // Open trigger drains all three.
        node.process_streaming(make_open(), None, cb.clone())
            .await
            .unwrap();
        assert_eq!(received.lock().unwrap().len(), 3);

        // Subsequent envelopes pass through.
        node.process_streaming(make_blendshape(99), None, cb.clone())
            .await
            .unwrap();
        assert_eq!(received.lock().unwrap().len(), 4);
    }

    /// Non-gated kinds pass through immediately even with closed gate.
    #[tokio::test]
    async fn non_gated_passthrough_when_closed() {
        let node = RenderReadyGateNode::new(RenderReadyGateConfig {
            pace_realtime: false,
            ..Default::default()
        });
        let received: Arc<StdMutex<Vec<RuntimeData>>> = Arc::default();
        let r = Arc::clone(&received);
        let cb = move |out: RuntimeData| -> Result<()> {
            r.lock().unwrap().push(out);
            Ok(())
        };

        let other = RuntimeData::Json(serde_json::json!({"kind": "audio_meta"}));
        node.process_streaming(other, None, cb).await.unwrap();
        assert_eq!(received.lock().unwrap().len(), 1);
    }

    /// Buffer cap evicts oldest (FIFO).
    #[tokio::test]
    async fn buffer_cap_drops_oldest() {
        let cfg = RenderReadyGateConfig {
            pace_realtime: false,
            buffer_capacity: 2,
            ..Default::default()
        };
        let node = RenderReadyGateNode::new(cfg);
        let received: Arc<StdMutex<Vec<RuntimeData>>> = Arc::default();
        let r = Arc::clone(&received);
        let cb = move |out: RuntimeData| -> Result<()> {
            r.lock().unwrap().push(out);
            Ok(())
        };

        for pts in [0u64, 33, 66, 99] {
            node.process_streaming(make_blendshape(pts), None, cb.clone())
                .await
                .unwrap();
        }
        // Buffer holds last 2 (66 and 99).
        node.process_streaming(make_open(), None, cb).await.unwrap();
        let r = received.lock().unwrap();
        assert_eq!(r.len(), 2);
        let pts_seen: Vec<u64> = r
            .iter()
            .filter_map(|d| match d {
                RuntimeData::Json(v) => v.get("pts_ms").and_then(|p| p.as_u64()),
                _ => None,
            })
            .collect();
        assert_eq!(pts_seen, vec![66, 99]);
    }

    /// Re-opening an already-open gate is a no-op.
    #[tokio::test]
    async fn duplicate_open_is_noop() {
        let node = RenderReadyGateNode::new(RenderReadyGateConfig {
            pace_realtime: false,
            ..Default::default()
        });
        let received: Arc<StdMutex<Vec<RuntimeData>>> = Arc::default();
        let r = Arc::clone(&received);
        let cb = move |out: RuntimeData| -> Result<()> {
            r.lock().unwrap().push(out);
            Ok(())
        };
        node.process_streaming(make_blendshape(0), None, cb.clone())
            .await
            .unwrap();
        node.process_streaming(make_open(), None, cb.clone())
            .await
            .unwrap();
        let after_first = received.lock().unwrap().len();
        node.process_streaming(make_open(), None, cb.clone())
            .await
            .unwrap();
        assert_eq!(received.lock().unwrap().len(), after_first);
    }
}
