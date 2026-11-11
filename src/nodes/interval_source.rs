//! `IntervalSource`: a self-driven node that emits a JSON tick on an interval.
//!
//! The smallest node that produces without client input (openspec:
//! add-node-initiated-output): it declares itself self-driven, starts a task
//! in `initialize`, and puts frames on its own output edge through its
//! emitter. A session whose only source is an `IntervalSource` produces
//! output with nothing sent to it, and ends when the source has emitted
//! `count` ticks.
//!
//! ```json
//! { "id": "clock", "node_type": "IntervalSource",
//!   "params": { "interval_ms": 1000, "count": 10, "payload": { "source": "clock" } } }
//! ```
//!
//! | Param | Default | Meaning |
//! |---|---|---|
//! | `interval_ms` | 1000 | time between ticks |
//! | `count` | none | ticks to emit before the node finishes; unset runs until the session closes |
//! | `payload` | `{}` | JSON object merged into every tick |
//!
//! Each tick is `{"tick": n, "elapsed_ms": ms, ...payload}`. Input the node
//! receives from an upstream node is passed through unchanged.

use crate::data::RuntimeData;
use crate::nodes::{
    AsyncNodeWrapper, AsyncStreamingNode, Emitter, InitializeContextRead, StreamingNode,
    StreamingNodeFactory,
};
use crate::Error;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct IntervalSourceNode {
    interval: Duration,
    count: Option<u64>,
    payload: serde_json::Map<String, Value>,
}

impl IntervalSourceNode {
    pub fn new(params: &Value) -> Result<Self, Error> {
        let interval_ms = match params.get("interval_ms") {
            None | Some(Value::Null) => 1000,
            Some(value) => value.as_u64().filter(|ms| *ms > 0).ok_or_else(|| {
                Error::ConfigError("IntervalSource: interval_ms must be a positive integer".into())
            })?,
        };
        let count = match params.get("count") {
            None | Some(Value::Null) => None,
            Some(value) => Some(value.as_u64().ok_or_else(|| {
                Error::ConfigError("IntervalSource: count must be a non-negative integer".into())
            })?),
        };
        let payload = match params.get("payload") {
            None | Some(Value::Null) => serde_json::Map::new(),
            Some(Value::Object(fields)) => fields.clone(),
            Some(_) => {
                return Err(Error::ConfigError(
                    "IntervalSource: payload must be a JSON object".into(),
                ))
            }
        };
        Ok(Self {
            interval: Duration::from_millis(interval_ms),
            count,
            payload,
        })
    }
}

#[async_trait::async_trait]
impl AsyncStreamingNode for IntervalSourceNode {
    fn node_type(&self) -> &str {
        "IntervalSource"
    }

    fn self_driven(&self) -> Option<bool> {
        Some(true)
    }

    async fn initialize(&self, ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        let emitter = Emitter::of(ctx);
        let (interval, count, payload) = (self.interval, self.count, self.payload.clone());
        tokio::spawn(async move {
            // Other nodes may still be loading. Start counting when the
            // session's node tasks run, not while it is being set up.
            emitter.started().await;
            let started = Instant::now();
            let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut tick = 0u64;
            while count.map_or(true, |count| tick < count) {
                tokio::select! {
                    // The instance stopped: so does the task.
                    _ = emitter.closed() => return,
                    _ = ticker.tick() => {}
                }
                let mut frame = payload.clone();
                frame.insert("tick".into(), tick.into());
                frame.insert("elapsed_ms".into(), (started.elapsed().as_millis() as u64).into());
                if emitter.emit(RuntimeData::Json(Value::Object(frame))).await.is_err() {
                    return;
                }
                tick += 1;
            }
            // Every tick has been emitted: the node has finished producing.
            emitter.finish();
        });
        Ok(())
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        Ok(data)
    }
}

pub struct IntervalSourceNodeFactory;

impl StreamingNodeFactory for IntervalSourceNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        Ok(Box::new(AsyncNodeWrapper(Arc::new(IntervalSourceNode::new(params)?))))
    }

    fn node_type(&self) -> &str {
        "IntervalSource"
    }

    fn self_driven(&self) -> Option<bool> {
        Some(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{Connection, Manifest, ManifestMetadata, NodeManifest};
    use crate::transport::PipelineExecutor;

    fn manifest(params: Value) -> Arc<Manifest> {
        Arc::new(Manifest {
            version: "v1".into(),
            metadata: ManifestMetadata {
                name: "interval-source".into(),
                ..Default::default()
            },
            nodes: vec![
                NodeManifest {
                    id: "clock".into(),
                    node_type: "IntervalSource".into(),
                    params,
                    ..Default::default()
                },
                NodeManifest {
                    id: "out".into(),
                    node_type: "PassThrough".into(),
                    params: serde_json::json!({}),
                    ..Default::default()
                },
            ],
            connections: vec![Connection {
                from: "clock".into(),
                to: "out".into(),
                ..Default::default()
            }],
            python_env: None,
            plugins: Vec::new(),
        })
    }

    #[test]
    fn parameters_are_checked() {
        assert!(IntervalSourceNode::new(&serde_json::json!({})).is_ok());
        for bad in [
            serde_json::json!({"interval_ms": 0}),
            serde_json::json!({"interval_ms": "soon"}),
            serde_json::json!({"count": -1}),
            serde_json::json!({"payload": [1, 2]}),
        ] {
            assert!(IntervalSourceNode::new(&bad).is_err(), "{bad}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn feeds_a_pipeline_with_no_client_input_and_ends_the_session_when_done() {
        let executor = PipelineExecutor::new().unwrap();
        let mut session = executor
            .create_session(manifest(serde_json::json!({
                "interval_ms": 20, "count": 3, "payload": {"source": "clock"}
            })))
            .await
            .unwrap();
        // Nothing is sent to the session.
        for expected in 0..3u64 {
            let output = tokio::time::timeout(Duration::from_secs(5), session.recv_output())
                .await
                .expect("tick in time")
                .unwrap()
                .expect("session open");
            let RuntimeData::Json(tick) = &output.data else {
                panic!("expected json, got {:?}", output.data)
            };
            assert_eq!(tick["tick"], expected);
            assert_eq!(tick["source"], "clock");
        }
        // The source finished, so the session drains and ends by itself.
        let end = tokio::time::timeout(Duration::from_secs(5), session.recv_output()).await;
        assert!(matches!(end, Ok(Ok(None)) | Ok(Err(_))), "the session did not end");
    }
}
