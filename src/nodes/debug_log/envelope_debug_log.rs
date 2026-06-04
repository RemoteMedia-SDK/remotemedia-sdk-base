//! `EnvelopeDebugLogNode` — passthrough streaming node that logs the
//! `RuntimeData::Json` envelopes flowing through it. Built for
//! diagnosing avatar pipelines: tap on a connection, watch the
//! blendshapes / skeletal poses scroll by, then remove the tap.
//!
//! Behavior:
//! - Accepts any `RuntimeData`. `Json` envelopes are inspected;
//!   everything else passes through unchanged.
//! - Specialized formatting for `kind="blendshapes"` (top-K active
//!   ARKit weights by absolute value + active count) and
//!   `kind="skeletal_pose"` (joint count + root_pos summary).
//! - Generic fallback for unknown `kind` values.
//! - `every: u32` config controls log throttling (e.g. `every: 30`
//!   logs once per second at 30 Hz blendshapes).
//! - Always passes data through to its successor — sit it on a tap
//!   edge or inline; either works.

#[cfg(feature = "avatar-lipsync")]
use crate::nodes::lip_sync::ARKIT_BLENDSHAPE_NAMES;

use crate::data::RuntimeData;
use crate::error::Result;
use crate::nodes::AsyncStreamingNode;
use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvelopeDebugLogConfig {
    /// Tag prefixed to every log line so multiple instances can be
    /// distinguished in a busy log stream.
    pub label: String,
    /// Optional filter. When set, only envelopes whose top-level
    /// `kind` field matches are logged. Other envelopes pass through
    /// silently.
    #[serde(default)]
    pub kind: Option<String>,
    /// Log every Nth matching envelope (rate limit). Default 1 = log
    /// every envelope. Useful when blendshapes arrive at 30 Hz and
    /// per-frame logs flood the console — set `every: 30` to log
    /// roughly once per second.
    #[serde(default = "default_every")]
    pub every: u32,
    /// For `kind="blendshapes"`, how many of the most-activated ARKit
    /// channels to print per envelope. Default 5.
    #[serde(default = "default_top_k")]
    pub top_k: usize,
}

fn default_every() -> u32 {
    1
}

fn default_top_k() -> usize {
    5
}

pub struct EnvelopeDebugLogNode {
    config: EnvelopeDebugLogConfig,
    state: Arc<Mutex<DebugState>>,
}

struct DebugState {
    /// Total matching envelopes seen across the lifetime of the node.
    /// Used for the modulo throttle and as a sequence counter in logs.
    seen: u64,
}

impl EnvelopeDebugLogNode {
    pub fn new(config: EnvelopeDebugLogConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(DebugState { seen: 0 })),
        }
    }
}

impl std::fmt::Debug for EnvelopeDebugLogNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.state.lock();
        f.debug_struct("EnvelopeDebugLogNode")
            .field("label", &self.config.label)
            .field("kind", &self.config.kind)
            .field("every", &self.config.every)
            .field("top_k", &self.config.top_k)
            .field("seen", &s.seen)
            .finish()
    }
}

#[async_trait]
impl AsyncStreamingNode for EnvelopeDebugLogNode {
    fn node_type(&self) -> &str {
        "EnvelopeDebugLogNode"
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        if let RuntimeData::Json(v) = &data {
            self.log_if_matching(v);
        }
        Ok(data)
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
        if let RuntimeData::Json(v) = &data {
            self.log_if_matching(v);
        }
        callback(data)?;
        Ok(1)
    }
}

impl EnvelopeDebugLogNode {
    fn log_if_matching(&self, v: &serde_json::Value) {
        let kind = v.get("kind").and_then(|k| k.as_str()).unwrap_or("");

        if let Some(filter) = self.config.kind.as_deref() {
            if kind != filter {
                return;
            }
        }

        let n = {
            let mut s = self.state.lock();
            s.seen += 1;
            s.seen
        };

        let every = self.config.every.max(1) as u64;
        if n % every != 0 {
            return;
        }

        let pts_ms = v.get("pts_ms").and_then(|p| p.as_u64()).unwrap_or(0);

        match kind {
            "blendshapes" => {
                let formatted = format_blendshapes(v, self.config.top_k);
                tracing::info!(
                    target: "envelope_debug",
                    "[{}] #{} blendshapes pts={}ms {}",
                    self.config.label, n, pts_ms, formatted
                );
            }
            "skeletal_pose" => {
                let formatted = format_skeletal_pose(v);
                tracing::info!(
                    target: "envelope_debug",
                    "[{}] #{} skeletal_pose pts={}ms {}",
                    self.config.label, n, pts_ms, formatted
                );
            }
            other if !other.is_empty() => {
                tracing::info!(
                    target: "envelope_debug",
                    "[{}] #{} kind={} pts={}ms",
                    self.config.label, n, other, pts_ms
                );
            }
            _ => {
                tracing::info!(
                    target: "envelope_debug",
                    "[{}] #{} (no-kind json) pts={}ms",
                    self.config.label, n, pts_ms
                );
            }
        }
    }
}

fn format_blendshapes(v: &serde_json::Value, top_k: usize) -> String {
    let arr = match v.get("arkit_52").and_then(|a| a.as_array()) {
        Some(a) => a,
        None => return "(no arkit_52 array)".to_string(),
    };

    let mut indexed: Vec<(usize, f64)> = arr
        .iter()
        .enumerate()
        .map(|(i, x)| (i, x.as_f64().unwrap_or(0.0)))
        .collect();
    indexed.sort_by(|a, b| {
        b.1.abs()
            .partial_cmp(&a.1.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let active = arr
        .iter()
        .filter(|x| x.as_f64().unwrap_or(0.0).abs() > 1e-3)
        .count();

    let max_abs = indexed.first().map(|(_, w)| w.abs()).unwrap_or(0.0);

    let top: Vec<String> = indexed
        .iter()
        .take(top_k.max(1))
        .map(|(i, w)| {
            let name = arkit_name(*i);
            format!("{name}={w:.3}")
        })
        .collect();

    format!(
        "active={}/52 max_abs={:.3} top{}=[{}]",
        active,
        max_abs,
        top.len(),
        top.join(", ")
    )
}

fn format_skeletal_pose(v: &serde_json::Value) -> String {
    let joints = v
        .get("joint_quats_xyzw")
        .and_then(|a| a.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let root_str = v
        .get("root_pos")
        .and_then(|r| r.as_array())
        .and_then(|a| {
            if a.len() != 3 {
                None
            } else {
                Some(format!(
                    "[{:.3},{:.3},{:.3}]",
                    a[0].as_f64().unwrap_or(0.0),
                    a[1].as_f64().unwrap_or(0.0),
                    a[2].as_f64().unwrap_or(0.0),
                ))
            }
        })
        .unwrap_or_else(|| "?".into());
    let frame_idx = v.get("frame_idx").and_then(|p| p.as_u64());
    match frame_idx {
        Some(f) => format!("joints={joints} root={root_str} frame_idx={f}"),
        None => format!("joints={joints} root={root_str}"),
    }
}

#[cfg(feature = "avatar-lipsync")]
fn arkit_name(i: usize) -> &'static str {
    ARKIT_BLENDSHAPE_NAMES.get(i).copied().unwrap_or("?")
}

#[cfg(not(feature = "avatar-lipsync"))]
fn arkit_name(i: usize) -> String {
    format!("arkit[{i}]")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run_log(node: &EnvelopeDebugLogNode, env: serde_json::Value) {
        node.log_if_matching(&env);
    }

    #[test]
    fn kind_filter_skips_non_matching() {
        let node = EnvelopeDebugLogNode::new(EnvelopeDebugLogConfig {
            label: "t".into(),
            kind: Some("blendshapes".into()),
            every: 1,
            top_k: 3,
        });
        // Skipped (no log) — different kind.
        run_log(&node, json!({"kind": "audio_clock", "pts_ms": 100}));
        // The seen counter should NOT advance when the filter rejects.
        assert_eq!(node.state.lock().seen, 0);
        run_log(
            &node,
            json!({"kind": "blendshapes", "arkit_52": vec![0.0_f64; 52], "pts_ms": 200}),
        );
        assert_eq!(node.state.lock().seen, 1);
    }

    #[test]
    fn every_throttles_logs() {
        let node = EnvelopeDebugLogNode::new(EnvelopeDebugLogConfig {
            label: "t".into(),
            kind: None,
            every: 5,
            top_k: 3,
        });
        // 12 envelopes; only #5 and #10 should produce log lines —
        // but `seen` advances for every match (seen += 1 always).
        for i in 0..12 {
            run_log(
                &node,
                json!({"kind": "blendshapes", "arkit_52": vec![0.0_f64; 52], "pts_ms": i}),
            );
        }
        assert_eq!(node.state.lock().seen, 12);
    }

    #[test]
    fn format_blendshapes_picks_top_active() {
        let mut weights = vec![0.0_f64; 52];
        weights[17] = 0.8; // jawOpen          (avatar-lipsync builds)
        weights[0] = 0.4; // eyeBlinkLeft
        weights[51] = -0.2; // tongueOut (negative still counts via abs)
        let env = json!({
            "kind": "blendshapes",
            "arkit_52": weights,
            "pts_ms": 33,
        });
        let s = format_blendshapes(&env, 3);
        assert!(s.contains("active=3/52"), "got: {s}");
        assert!(s.contains("max_abs=0.800"), "got: {s}");
        // Names render symbolically when avatar-lipsync is enabled,
        // otherwise as `arkit[N]` indices. Either way, the values
        // should appear in top-3 sorted by |w|.
        assert!(s.contains("=0.800"), "got: {s}");
        assert!(s.contains("=0.400"), "got: {s}");
        assert!(s.contains("=-0.200"), "got: {s}");
        #[cfg(feature = "avatar-lipsync")]
        {
            assert!(s.contains("jawOpen=0.800"), "got: {s}");
            assert!(s.contains("eyeBlinkLeft=0.400"), "got: {s}");
        }
        #[cfg(not(feature = "avatar-lipsync"))]
        {
            assert!(s.contains("arkit[17]=0.800"), "got: {s}");
            assert!(s.contains("arkit[0]=0.400"), "got: {s}");
        }
    }

    #[test]
    fn format_skeletal_pose_summarizes_joints_and_root() {
        let env = json!({
            "kind": "skeletal_pose",
            "joint_quats_xyzw": (0..22).map(|_| [0.0, 0.0, 0.0, 1.0]).collect::<Vec<_>>(),
            "root_pos": [0.1, 1.5, -0.2],
            "frame_idx": 7,
            "pts_ms": 233,
        });
        let s = format_skeletal_pose(&env);
        assert!(s.contains("joints=22"));
        assert!(s.contains("root=[0.100,1.500,-0.200]"));
        assert!(s.contains("frame_idx=7"));
    }

    #[tokio::test]
    async fn passthrough_emits_data_unchanged() {
        let node = EnvelopeDebugLogNode::new(EnvelopeDebugLogConfig {
            label: "t".into(),
            kind: None,
            every: 1,
            top_k: 3,
        });
        let collected = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let cc = collected.clone();
        let env = RuntimeData::Json(
            json!({"kind": "blendshapes", "arkit_52": vec![0.0_f64; 52], "pts_ms": 1}),
        );
        let env_clone = env.clone();
        node.process_streaming(env, None, move |out| {
            cc.lock().unwrap().push(out);
            Ok(())
        })
        .await
        .expect("ok");
        let collected = collected.lock().unwrap();
        assert_eq!(collected.len(), 1);
        assert_eq!(format!("{:?}", collected[0]), format!("{:?}", env_clone));
    }
}
