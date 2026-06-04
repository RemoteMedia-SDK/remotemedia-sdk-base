//! See `mod.rs` for the rationale.

use crate::data::RuntimeData;
use crate::error::{Error, Result};
use crate::nodes::AsyncStreamingNode;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct EnvelopeFirstNotifierConfig {
    /// Json `kind` value to watch for. Default `"skeletal_pose"` — the
    /// stream Kimodo emits.
    pub target_kind: String,
    /// Optional label included in the emitted envelope (handy when
    /// multiple notifiers feed the same downstream).
    pub source_label: String,
}

impl Default for EnvelopeFirstNotifierConfig {
    fn default() -> Self {
        Self {
            target_kind: "skeletal_pose".into(),
            source_label: "kimodo_motion".into(),
        }
    }
}

pub struct EnvelopeFirstNotifierNode {
    config: EnvelopeFirstNotifierConfig,
    fired: Arc<AtomicBool>,
}

impl EnvelopeFirstNotifierNode {
    pub fn new(config: EnvelopeFirstNotifierConfig) -> Self {
        Self {
            config,
            fired: Arc::new(AtomicBool::new(false)),
        }
    }

    fn json_kind(data: &RuntimeData) -> Option<&str> {
        match data {
            RuntimeData::Json(v) => v.get("kind").and_then(|k| k.as_str()),
            _ => None,
        }
    }
}

#[async_trait]
impl AsyncStreamingNode for EnvelopeFirstNotifierNode {
    fn node_type(&self) -> &str {
        "EnvelopeFirstNotifierNode"
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
        let Some(kind) = Self::json_kind(&data) else {
            return Ok(0);
        };
        if kind != self.config.target_kind {
            return Ok(0);
        }
        // First-ever match wins; everything after is silently dropped.
        if self.fired.swap(true, Ordering::AcqRel) {
            return Ok(0);
        }
        tracing::info!(
            target: "envelope_first_notifier",
            "fired on first '{}' from {}",
            self.config.target_kind, self.config.source_label,
        );
        let envelope = RuntimeData::Json(serde_json::json!({
            "kind": "first_emit",
            "source": self.config.source_label,
            "target_kind": self.config.target_kind,
        }));
        callback(envelope)?;
        Ok(1)
    }

    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData> {
        Err(Error::Execution(
            "EnvelopeFirstNotifierNode requires streaming mode".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    fn make_pose(pts_ms: u64) -> RuntimeData {
        RuntimeData::Json(serde_json::json!({
            "kind": "skeletal_pose",
            "pts_ms": pts_ms,
        }))
    }

    #[tokio::test]
    async fn fires_once_on_first_match() {
        let node = EnvelopeFirstNotifierNode::new(EnvelopeFirstNotifierConfig::default());
        let received: Arc<StdMutex<Vec<RuntimeData>>> = Arc::default();
        let r = Arc::clone(&received);
        let cb = move |out: RuntimeData| -> Result<()> {
            r.lock().unwrap().push(out);
            Ok(())
        };

        node.process_streaming(make_pose(0), None, cb.clone())
            .await
            .unwrap();
        node.process_streaming(make_pose(33), None, cb.clone())
            .await
            .unwrap();
        node.process_streaming(make_pose(66), None, cb.clone())
            .await
            .unwrap();

        let r = received.lock().unwrap();
        assert_eq!(r.len(), 1, "should fire exactly once");
        match &r[0] {
            RuntimeData::Json(v) => {
                assert_eq!(v.get("kind").and_then(|k| k.as_str()), Some("first_emit"));
                assert_eq!(
                    v.get("target_kind").and_then(|k| k.as_str()),
                    Some("skeletal_pose")
                );
            }
            _ => panic!("expected Json envelope"),
        }
    }

    #[tokio::test]
    async fn ignores_non_matching_kinds() {
        let node = EnvelopeFirstNotifierNode::new(EnvelopeFirstNotifierConfig::default());
        let received: Arc<StdMutex<Vec<RuntimeData>>> = Arc::default();
        let r = Arc::clone(&received);
        let cb = move |out: RuntimeData| -> Result<()> {
            r.lock().unwrap().push(out);
            Ok(())
        };
        let other = RuntimeData::Json(serde_json::json!({"kind": "blendshapes"}));
        node.process_streaming(other, None, cb).await.unwrap();
        assert_eq!(received.lock().unwrap().len(), 0);
    }
}
