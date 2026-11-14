//! A loadable plugin node with a background body, the origin of its inputs,
//! and its factory's declarations (openspec: add-source-node-lifecycle).
//! The node is built on the plugin SDK and reached through the plugin ABI on
//! both sides, inside a real session.
use super::*;
use crate::manifest::{Manifest, ManifestMetadata, NodeManifest};
use crate::transport::data::participant;
use crate::transport::{Participant, PipelineExecutor, SessionHandle, TransportData};
use remotemedia_plugin_sdk::adapter::StreamingNodeFfiAdapter;
use remotemedia_plugin_sdk::{Emitter as PluginEmitter, Origin as PluginOrigin};
use remotemedia_traits::NodeDescription;
use std::sync::Mutex;
use std::time::Duration;

/// `mode`: `return` (emit `frames`, return), `error` (emit, then fail),
/// `none` (no body; keeps the origin of `remember` and emits with it on
/// `later`).
struct PluginNode {
    mode: String,
    frames: u64,
    self_driven: bool,
    kept: Mutex<Option<Arc<PluginOrigin>>>,
}

#[async_trait]
impl AsyncStreamingNode for PluginNode {
    fn node_type(&self) -> &str {
        "PluginBody"
    }

    fn self_driven(&self) -> Option<bool> {
        self.self_driven.then_some(true)
    }

    fn has_background(&self) -> bool {
        self.mode != "none"
    }

    async fn background(&self, ctx: &dyn NodeRuntimeContextRead) -> Result<(), Error> {
        let emitter = PluginEmitter::for_call(ctx);
        for n in 0..self.frames {
            emitter.emit(RuntimeData::Text(format!("body-{n}"))).await?;
        }
        if self.mode == "error" {
            return Err(Error::Execution("plugin body sentinel".into()));
        }
        Ok(())
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        Ok(data)
    }

    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let RuntimeData::Text(command) = &data else {
            callback(data)?;
            return Ok(1);
        };
        match command.as_str() {
            "remember" => {
                *self.kept.lock().unwrap() = ctx.origin();
                let who = ctx.origin().map(|origin| origin.id().to_string());
                callback(RuntimeData::Text(format!("remembered {who:?}")))?;
            }
            "later" => {
                let kept = self.kept.lock().unwrap().clone();
                PluginEmitter::for_call(ctx)
                    .emit_as(RuntimeData::Text("for the one I remember".into()), kept)
                    .await?;
                callback(RuntimeData::Text("ack".into()))?;
            }
            _ => callback(data)?,
        }
        Ok(1)
    }
}

struct PluginFactory;

impl FfiNodeFactory for PluginFactory {
    fn node_type(&self) -> RString {
        "PluginBody".into()
    }

    fn create(&self, params: RString) -> RResult<FfiNodeBox, RString> {
        let params: Value = serde_json::from_str(params.as_str()).unwrap();
        ROk(loadable_node_abi::FfiNode_TO::from_value(
            StreamingNodeFfiAdapter::new(PluginNode {
                mode: params["mode"].as_str().unwrap_or("none").to_string(),
                frames: params["frames"].as_u64().unwrap_or(0),
                self_driven: params["self_driven"].as_bool().unwrap_or(false),
                kept: Mutex::new(None),
            }),
            TD_Opaque,
        ))
    }
}

/// Declares everything through `describe`, before a node exists.
struct DeclaringFactory;

impl FfiNodeFactory for DeclaringFactory {
    fn node_type(&self) -> RString {
        "PluginDeclared".into()
    }

    fn create(&self, _: RString) -> RResult<FfiNodeBox, RString> {
        ROk(loadable_node_abi::FfiNode_TO::from_value(
            StreamingNodeFfiAdapter::new(PluginNode {
                mode: "return".into(),
                frames: 2,
                // The instance declares nothing: the factory did.
                self_driven: false,
                kept: Mutex::new(None),
            }),
            TD_Opaque,
        ))
    }

    fn describe(&self, params: RString) -> RResult<RString, RString> {
        let params: Value = serde_json::from_str(params.as_str()).unwrap_or_default();
        let rate = params["sample_rate"].as_u64().unwrap_or(16_000) as u32;
        ROk(NodeDescription {
            self_driven: Some(true),
            instance_scope: Some("shared".into()),
            capability_behavior: Some(crate::capabilities::CapabilityBehavior::Configured),
            media_capabilities: Some(crate::capabilities::MediaCapabilities::with_output(
                crate::capabilities::MediaConstraints::Audio(crate::capabilities::AudioConstraints {
                    sample_rate: Some(crate::capabilities::ConstraintValue::Exact(rate)),
                    ..Default::default()
                }),
            )),
            ..Default::default()
        }
        .to_json()
        .into())
    }
}

async fn executor() -> PipelineExecutor {
    let executor = PipelineExecutor::new().unwrap();
    executor.register_factory(wrap_ffi_factory(PluginFactory)).await;
    executor.register_factory(wrap_ffi_factory(DeclaringFactory)).await;
    executor
}

fn manifest(node_type: &str, params: Value) -> Arc<Manifest> {
    Arc::new(Manifest {
        version: "v1".into(),
        metadata: ManifestMetadata {
            name: "plugin-body".into(),
            ..Default::default()
        },
        nodes: vec![NodeManifest {
            id: "plugin".into(),
            node_type: node_type.into(),
            params,
            ..Default::default()
        }],
        connections: Vec::new(),
        python_env: None,
        plugins: Vec::new(),
    })
}

async fn next(session: &mut SessionHandle) -> (String, Option<String>) {
    let output = tokio::time::timeout(Duration::from_secs(5), session.recv_output())
        .await
        .expect("output in time")
        .unwrap()
        .expect("session open");
    let RuntimeData::Text(body) = &output.data else {
        panic!("expected text, got {:?}", output.data)
    };
    (body.clone(), output.metadata.get(participant::ID).cloned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plugin_body_emits_and_its_return_finishes_a_self_driven_plugin_node() {
    let executor = executor().await;
    let mut session = executor
        .create_session(manifest(
            "PluginBody",
            serde_json::json!({"mode": "return", "frames": 3, "self_driven": true}),
        ))
        .await
        .unwrap();
    for n in 0..3 {
        assert_eq!(next(&mut session).await, (format!("body-{n}"), None));
    }
    let end = tokio::time::timeout(Duration::from_secs(5), session.recv_output()).await;
    assert!(matches!(end, Ok(Ok(None))), "the session did not end: {end:?}");
    assert!(session.wait().await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plugin_body_that_fails_is_a_failure_of_the_node() {
    let executor = executor().await;
    let session = executor
        .create_session(manifest(
            "PluginBody",
            serde_json::json!({"mode": "error", "frames": 1, "self_driven": true}),
        ))
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(5), session.wait())
        .await
        .expect("the session ended")
        .expect_err("the session failed")
        .to_string();
    assert!(error.contains("plugin") && error.contains("plugin body sentinel"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plugin_reads_the_origin_of_an_input_and_emits_with_it_later() {
    let executor = executor().await;
    let mut session = executor
        .create_session(manifest("PluginBody", serde_json::json!({})))
        .await
        .unwrap();
    let alice = session
        .join_participant(Participant::new("alice", participant::role::USER))
        .unwrap();
    alice.send(TransportData::new(RuntimeData::Text("remember".into()))).await.unwrap();
    assert_eq!(
        next(&mut session).await,
        ("remembered Some(\"alice\")".to_string(), Some("alice".to_string()))
    );
    // An input from nobody; the emitted frame still goes out as alice.
    session
        .send_input(TransportData::new(RuntimeData::Text("later".into())))
        .await
        .unwrap();
    let mut seen = vec![next(&mut session).await, next(&mut session).await];
    seen.sort();
    assert_eq!(
        seen,
        [
            ("ack".to_string(), None),
            ("for the one I remember".to_string(), Some("alice".to_string())),
        ]
    );
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plugin_factory_declares_its_node_before_creating_it() {
    let factory = wrap_ffi_factory(DeclaringFactory);
    assert_eq!(factory.self_driven(), Some(true));
    assert_eq!(
        factory.instance_scope(),
        Some(remotemedia_traits::streaming::InstanceScope::Shared)
    );
    assert_eq!(
        factory.capability_behavior(),
        crate::capabilities::CapabilityBehavior::Configured
    );
    let caps = factory
        .media_capabilities(&serde_json::json!({"sample_rate": 48_000}))
        .expect("declared capabilities");
    let crate::capabilities::MediaConstraints::Audio(audio) = caps.default_output().unwrap() else {
        panic!("expected audio")
    };
    assert_eq!(
        audio.sample_rate,
        Some(crate::capabilities::ConstraintValue::Exact(48_000))
    );
    // A plugin that declares nothing.
    let plain = wrap_ffi_factory(PluginFactory);
    assert_eq!(plain.self_driven(), None);
    assert!(plain.media_capabilities(&serde_json::json!({})).is_none());

    // The declaration is what the session uses: client input does not reach
    // the node, though the instance itself declares nothing.
    let executor = executor().await;
    let mut session = executor
        .create_session(manifest("PluginDeclared", serde_json::json!({})))
        .await
        .unwrap();
    assert_eq!(next(&mut session).await.0, "body-0");
    assert_eq!(next(&mut session).await.0, "body-1");
    let end = tokio::time::timeout(Duration::from_secs(5), session.recv_output()).await;
    assert!(matches!(end, Ok(Ok(None))), "the session did not end: {end:?}");
}
