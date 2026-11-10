use super::*;
use crate::HttpPipelineClient;
use remotemedia_core::{
    data::RuntimeData,
    nodes::{AsyncNodeWrapper, AsyncStreamingNode, StreamingNode, StreamingNodeFactory},
    transport::{ExecutorConfig, PipelineClient},
};
use remotemedia_types::{CopyContext, CopyPolicy, CopyReason};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::Duration,
};

struct Factory {
    calls: Arc<AtomicUsize>,
    blocked: Arc<tokio::sync::Notify>,
}
struct Node {
    calls: Arc<AtomicUsize>,
    blocked: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl AsyncStreamingNode for Node {
    fn node_type(&self) -> &str {
        "HttpFixture"
    }
    async fn process(&self, data: RuntimeData) -> remotemedia_core::Result<RuntimeData> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match &data {
            RuntimeData::Text(text) if text == "produce" => Ok(audio().data),
            RuntimeData::Text(text) if text == "fail" => Err(codec::error("node failure sentinel")),
            RuntimeData::Text(text) if text == "block" => {
                self.blocked.notify_one();
                std::future::pending().await
            }
            _ => Ok(data),
        }
    }
}
impl StreamingNodeFactory for Factory {
    fn node_type(&self) -> &str {
        "HttpFixture"
    }
    fn create(
        &self,
        _: String,
        _: &serde_json::Value,
        _: Option<String>,
    ) -> remotemedia_core::Result<Box<dyn StreamingNode>> {
        Ok(Box::new(AsyncNodeWrapper(Arc::new(Node {
            calls: self.calls.clone(),
            blocked: self.blocked.clone(),
        }))))
    }
}

struct Fixture {
    server: HttpServer,
    url: String,
    task: tokio::task::JoinHandle<()>,
    reports: Arc<Mutex<Vec<(String, CopyContext)>>>,
    calls: Arc<AtomicUsize>,
    blocked: Arc<tokio::sync::Notify>,
}
impl Fixture {
    async fn new(policy: CopyPolicy) -> Self {
        let executor = Arc::new(
            PipelineExecutor::with_config(ExecutorConfig {
                transport_materialization_policy: policy,
                ..Default::default()
            })
            .unwrap(),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let blocked = Arc::new(tokio::sync::Notify::new());
        executor
            .register_factory(Arc::new(Factory {
                calls: calls.clone(),
                blocked: blocked.clone(),
            }))
            .await;
        let reports = Arc::new(Mutex::new(Vec::new()));
        let retained = reports.clone();
        let server = HttpServer::new("127.0.0.1:0".into(), executor)
            .await
            .unwrap()
            .with_copy_observer(move |id, copies| {
                retained.lock().unwrap().push((id.to_owned(), copies))
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let router = server.build_router();
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            server,
            url,
            task,
            reports,
            calls,
            blocked,
        }
    }
    async fn client(&self) -> HttpPipelineClient {
        HttpPipelineClient::new(&self.url, None).await.unwrap()
    }
    async fn finish(&self) {
        let sessions: Vec<_> = self
            .server
            .state
            .sessions
            .write()
            .await
            .drain()
            .map(|(_, handle)| handle)
            .collect();
        for session in sessions {
            let _ = bounded(session.close()).await;
        }
        let bus = self.server.state.executor.control_bus();
        for (id, _) in self.reports.lock().unwrap().iter() {
            assert!(bus.get(id).is_none(), "session {id} still registered");
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("HTTP session timed out")
}
fn manifest() -> Arc<Manifest> {
    Arc::new(
        serde_json::from_value(serde_json::json!({"version":"v1", "metadata":{},
        "nodes":[{"id":"node","node_type":"HttpFixture"}], "connections":[]}))
        .unwrap(),
    )
}
fn audio() -> TransportData {
    TransportData::new(RuntimeData::Audio {
        samples: vec![0.25; 4].into(),
        sample_rate: 16000,
        channels: 1,
        timestamp_us: Some(123),
        arrival_ts_us: None,
        stream_id: None,
        metadata: None,
    })
}
fn bytes(context: &CopyContext, reason: CopyReason) -> u64 {
    context
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == reason)
        .unwrap()
        .1
        .copied_bytes
}
fn assert_audio(data: TransportData) {
    match data.data {
        RuntimeData::Audio {
            samples,
            timestamp_us,
            ..
        } => {
            assert_eq!(samples.as_slice(), &[0.25; 4]);
            assert_eq!(timestamp_us, Some(123));
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn unary_real_pipeline_retains_session_accounting_after_joined_cleanup() {
    let fixture = Fixture::new(CopyPolicy::AllowCopies).await;
    let client = fixture.client().await;
    assert_audio(
        bounded(client.execute_unary(manifest(), audio(), &[], None))
            .await
            .unwrap(),
    );
    let reports = fixture.reports.lock().unwrap().clone();
    assert_eq!(reports.len(), 1);
    let (id, context) = &reports[0];
    assert!(context.boundary().contains(id));
    assert_eq!(bytes(context, CopyReason::PortableSerialization), 16);
    assert_eq!(bytes(context, CopyReason::PortableDeserialization), 16);
    assert_eq!(bytes(context, CopyReason::VecMaterialization), 0);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    fixture.finish().await;
}

#[tokio::test]
async fn stream_inputs_return_while_outputs_drain_and_fanout_shares_one_wire_owner() {
    let fixture = Fixture::new(CopyPolicy::AllowCopies).await;
    let client = fixture.client().await;
    let mut stream = bounded(client.create_stream_session(manifest(), &[], None))
        .await
        .unwrap();
    let handle = find_session(&fixture.server.state, stream.session_id())
        .await
        .unwrap();
    let authority = fixture
        .server
        .session_copy_context(stream.session_id())
        .await
        .unwrap();
    assert!(authority.same_authority(&handle.copies));
    let mut first = handle.output.subscribe();
    let mut second = handle.output.subscribe();
    let mut retained = Vec::new();
    for _ in 0..2 {
        bounded(stream.send(audio())).await.unwrap();
        assert_audio(bounded(stream.receive()).await.unwrap().unwrap());
        let a = bounded(first.recv()).await.unwrap();
        let b = bounded(second.recv()).await.unwrap();
        assert_eq!(a.as_ptr(), b.as_ptr());
        retained.push(a);
    }
    assert_eq!(bytes(&authority, CopyReason::PortableSerialization), 32);
    assert_eq!(bytes(&authority, CopyReason::PortableDeserialization), 32);
    assert_eq!(bytes(&authority, CopyReason::LegacyAudioClone), 0);
    bounded(stream.close()).await.unwrap();
    drop(handle);
    fixture.finish().await;
    for wire in retained {
        assert_audio(codec::decode(&wire, CopyContext::legacy()).unwrap());
    }
}

#[tokio::test]
async fn strict_server_rejects_bulk_ingress_before_node_execution() {
    let fixture = Fixture::new(CopyPolicy::RequireZeroCopy).await;
    let client = fixture.client().await;
    assert!(
        bounded(client.execute_unary(manifest(), audio(), &[], None))
            .await
            .is_err()
    );
    let mut stream = bounded(client.create_stream_session(manifest(), &[], None))
        .await
        .unwrap();
    assert!(bounded(stream.send(audio())).await.is_err());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    bounded(stream.close()).await.unwrap();
    fixture.finish().await;
    for (_, context) in fixture.reports.lock().unwrap().iter() {
        assert_eq!(bytes(context, CopyReason::PortableDeserialization), 0);
        assert!(context
            .report()
            .entries
            .iter()
            .any(|(_, count)| count.rejected > 0));
    }
}

#[tokio::test]
async fn strict_output_and_core_errors_are_visible_to_sse_consumers() {
    for (policy, input, message) in [
        (CopyPolicy::RequireZeroCopy, "produce", "copy"),
        (CopyPolicy::AllowCopies, "fail", "node failure sentinel"),
    ] {
        let fixture = Fixture::new(policy).await;
        let mut stream = bounded(fixture.client().await.create_stream_session(
            manifest(),
            &[],
            None,
        ))
        .await
        .unwrap();
        bounded(stream.send(TransportData::new(RuntimeData::Text(input.into()))))
            .await
            .unwrap();
        let error = bounded(stream.receive()).await.unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
        assert!(bounded(stream.close()).await.is_err());
        fixture.finish().await;
        assert_eq!(
            bytes(
                &fixture.reports.lock().unwrap()[0].1,
                CopyReason::PortableSerialization
            ),
            0
        );
    }
}

#[tokio::test]
async fn abandoned_unary_handler_signals_core_cleanup() {
    let fixture = Fixture::new(CopyPolicy::AllowCopies).await;
    let body = serde_json::to_vec(&serde_json::json!({"manifest":manifest().as_ref(),
        "input":TransportData::new(RuntimeData::Text("block".into()))}))
    .unwrap();
    let request = Request::builder()
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body))
        .unwrap();
    let state = fixture.server.state.clone();
    let task = tokio::spawn(execute_handler(State(state), request));
    bounded(fixture.blocked.notified()).await;
    task.abort();
    let _ = task.await;
    let id = fixture.reports.lock().unwrap()[0].0.clone();
    bounded(async {
        while fixture
            .server
            .state
            .executor
            .control_bus()
            .get(&id)
            .is_some()
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    fixture.finish().await;
}

#[tokio::test]
async fn fragmented_request_body_skips_bulk_then_decodes_under_exact_authority() {
    let wire =
        serde_json::to_vec(&serde_json::json!({"manifest":manifest().as_ref(), "input":audio()}))
            .unwrap();
    let chunks: Vec<_> = wire
        .into_iter()
        .map(|byte| Ok::<_, Infallible>(bytes::Bytes::from(vec![byte])))
        .collect();
    let body = JsonBody::collect(futures::stream::iter(chunks), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body.manifest().unwrap().nodes[0].node_type, "HttpFixture");
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "fragmented-request");
    assert_audio(body.decode::<ExecuteRequest>(&copies).unwrap().input);
    assert_eq!(bytes(&copies, CopyReason::PortableDeserialization), 16);
    assert_eq!(bytes(&copies, CopyReason::VecMaterialization), 0);
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict-request");
    assert!(body.decode::<ExecuteRequest>(&strict).is_err());
    assert_eq!(bytes(&strict, CopyReason::PortableDeserialization), 0);
}

use std::convert::Infallible;

mod acceptance;
