//! Client-hosted endpoint conformance suite
//! (openspec `add-agent-harness-integration`, tasks 2.8 / 2.9).
//!
//! Every transport-sensitive case runs twice — against an in-process host
//! (`SessionControl::host_endpoint`) and a gRPC `PipelineControl.Attach`
//! host — and must produce the same results and error kinds. Callers are
//! real pipeline nodes on a real `SessionRouter`:
//!
//! * `EpEnvCaller` — in-tree node using the `__call__` aux-envelope
//!   convention, awaiting `__call_reply__` (delivered on its RPC lane) inside
//!   `process_streaming_async` — the shape loadable plugins use.
//! * `EpCtxCaller` — in-tree node using `NodeRuntimeContext::call_endpoint`.

extern crate remotemedia_sdk_base as remotemedia_core;

use async_trait::async_trait;
use remotemedia_core::data::RuntimeData;
use remotemedia_core::manifest::{Connection, Manifest, ManifestMetadata, NodeManifest};
use remotemedia_core::nodes::{
    NodeRuntimeContext, NodeRuntimeContextRead, StreamingNode, StreamingNodeFactory,
};
use remotemedia_core::transport::endpoint::{
    call_reply_method, CallErrorKind, CallOptions, CallReplyEnvelope, CallReplyEvent,
    CallRequestEnvelope, EndpointHost, EndpointSpec, HostEvent, HostKind, RpcMethod, RpcMode,
    RpcRequest, RpcResponder, CALL_REPLY_METHOD,
};
use remotemedia_core::transport::session_control::{ControlAddress, SessionControl};
use remotemedia_core::transport::{PipelineExecutor, SessionHandle, TransportData};
use remotemedia_core::Error;
use remotemedia_grpc::control::ControlServiceImpl;
use remotemedia_grpc::generated::{
    control_event::Event as PbEvent, control_frame::Op as PbOp,
    pipeline_control_client::PipelineControlClient, pipeline_control_server::PipelineControlServer,
    ControlEvent, ControlFrame, EndpointMethod as PbMethod, EndpointReply as PbReply, Hello,
    HostEndpoint as PbHost,
};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

// ─── Test nodes ─────────────────────────────────────────────────────────────

#[derive(Clone)]
struct CallerCfg {
    endpoint: String,
    method: String,
    mode: RpcMode,
    deadline_ms: u64,
    hold_ms: Option<u64>,
    fanout: usize,
}

impl CallerCfg {
    fn from(params: &Value) -> Self {
        Self {
            endpoint: params["endpoint"].as_str().unwrap_or("harness").into(),
            method: params["method"].as_str().unwrap_or("decide").into(),
            mode: match params["mode"].as_str() {
                Some("stream") => RpcMode::Stream,
                Some("ff") => RpcMode::FireAndForget,
                _ => RpcMode::Reply,
            },
            deadline_ms: params["deadline_ms"].as_u64().unwrap_or(2_000),
            hold_ms: params["hold_ms"].as_u64(),
            fanout: params["fanout"].as_u64().unwrap_or(1) as usize,
        }
    }
}

fn utterance(data: &RuntimeData) -> String {
    match data {
        RuntimeData::Text(t) => t.clone(),
        RuntimeData::Json(v) => v.to_string(),
        _ => String::new(),
    }
}

fn result_json(result: Result<Value, (CallErrorKind, String)>, started: Instant) -> RuntimeData {
    let elapsed_ms = started.elapsed().as_millis() as u64;
    RuntimeData::Json(match result {
        Ok(v) => json!({"ok": v, "elapsed_ms": elapsed_ms}),
        Err((kind, msg)) => json!({"err": kind.as_str(), "message": msg, "elapsed_ms": elapsed_ms}),
    })
}

/// Collect a call's chunks: reply ⇒ first value; stream ⇒ array of values.
async fn collect_chunks(
    rx: &mut mpsc::UnboundedReceiver<CallReplyEnvelope>,
    mode: RpcMode,
    limit: Duration,
) -> Result<Value, (CallErrorKind, String)> {
    let mut values = Vec::new();
    loop {
        let env = match tokio::time::timeout(limit, rx.recv()).await {
            Ok(Some(env)) => env,
            Ok(None) => return Err((CallErrorKind::Cancelled, "closed".into())),
            Err(_) => return Err((CallErrorKind::Timeout, "test-side timeout".into())),
        };
        match env.event {
            CallReplyEvent::Value { data } if mode != RpcMode::Stream => return Ok(data),
            CallReplyEvent::Value { data } => values.push(data),
            CallReplyEvent::End if mode == RpcMode::Stream => return Ok(Value::Array(values)),
            CallReplyEvent::End => return Ok(Value::Null),
            CallReplyEvent::Error { error, message } => return Err((error, message)),
        }
    }
}

/// Envelope-convention caller (the loadable-plugin shape).
struct EnvCaller {
    cfg: CallerCfg,
    pending: Arc<parking_lot::Mutex<HashMap<String, mpsc::UnboundedSender<CallReplyEnvelope>>>>,
    next: AtomicU64,
}

#[async_trait]
impl StreamingNode for EnvCaller {
    fn node_type(&self) -> &str {
        "EpEnvCaller"
    }
    async fn process_async(
        &self,
        d: RuntimeData,
        _: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Ok(d)
    }
    async fn process_multi_async(
        &self,
        _: HashMap<String, RuntimeData>,
        _: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Err(Error::Execution("single input".into()))
    }
    fn is_multi_input(&self) -> bool {
        false
    }
    fn rpc_methods(&self) -> Vec<RpcMethod> {
        vec![call_reply_method()]
    }
    async fn call_rpc(
        &self,
        req: RpcRequest,
        _: &dyn NodeRuntimeContextRead,
        _: &mut RpcResponder,
    ) -> Result<(), Error> {
        assert_eq!(req.method, CALL_REPLY_METHOD);
        let env: CallReplyEnvelope = serde_json::from_value(req.args[0].clone())
            .map_err(|e| Error::Execution(e.to_string()))?;
        if let Some(tx) = self.pending.lock().get(&env.corr_id) {
            let _ = tx.send(env);
        }
        Ok(())
    }
    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
        mut cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<usize, Error> {
        let started = Instant::now();
        let corr = format!("env-{}", self.next.fetch_add(1, Ordering::Relaxed));
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.pending.lock().insert(corr.clone(), tx);
        let mut req = CallRequestEnvelope::new(&self.cfg.endpoint, &self.cfg.method, corr.clone());
        req.mode = self.cfg.mode;
        req.kwargs
            .insert("utterance".into(), utterance(&data).into());
        req.deadline_ms = self.cfg.deadline_ms;
        req.hold_ms = self.cfg.hold_ms;
        cb(RuntimeData::Json(req.to_aux_json()))?;
        // Await the reply INSIDE processing: replies arrive on the RPC lane.
        let limit = Duration::from_millis(self.cfg.deadline_ms + 3_000);
        let result = collect_chunks(&mut rx, self.cfg.mode, limit).await;
        self.pending.lock().remove(&corr);
        cb(result_json(result, started))?;
        Ok(2)
    }
}

/// `NodeRuntimeContext::call_endpoint` caller; `fanout` concurrent calls.
struct CtxCaller {
    cfg: CallerCfg,
}

#[async_trait]
impl StreamingNode for CtxCaller {
    fn node_type(&self) -> &str {
        "EpCtxCaller"
    }
    async fn process_async(
        &self,
        d: RuntimeData,
        _: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Ok(d)
    }
    async fn process_multi_async(
        &self,
        _: HashMap<String, RuntimeData>,
        _: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Err(Error::Execution("single input".into()))
    }
    fn is_multi_input(&self) -> bool {
        false
    }
    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<usize, Error> {
        let ctx = ctx
            .as_any()
            .downcast_ref::<NodeRuntimeContext>()
            .expect("in-tree context");
        let started = Instant::now();
        let mut kwargs = Map::new();
        kwargs.insert("utterance".into(), utterance(&data).into());
        let mut opts = CallOptions::default()
            .mode(self.cfg.mode)
            .deadline_ms(self.cfg.deadline_ms);
        opts.hold_ms = self.cfg.hold_ms;
        let calls = (0..self.cfg.fanout).map(|_| {
            let stream = ctx.call_endpoint(
                &self.cfg.endpoint,
                &self.cfg.method,
                vec![],
                kwargs.clone(),
                opts.clone(),
            );
            let mode = self.cfg.mode;
            async move {
                let r = if mode == RpcMode::Stream {
                    stream.collect().await.map(Value::Array)
                } else {
                    stream.value().await
                };
                r.map_err(|e| (e.kind, e.message))
            }
        });
        let results = futures::future::join_all(calls).await;
        let n = results.len();
        for r in results {
            cb(result_json(r, started))?;
        }
        Ok(n)
    }
}

/// Has an RPC lane but does not declare `__call_reply__` ⇒ legacy semantics.
struct LegacyEmitter;

#[async_trait]
impl StreamingNode for LegacyEmitter {
    fn node_type(&self) -> &str {
        "EpLegacy"
    }
    async fn process_async(
        &self,
        d: RuntimeData,
        _: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Ok(d)
    }
    async fn process_multi_async(
        &self,
        _: HashMap<String, RuntimeData>,
        _: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Err(Error::Execution("single input".into()))
    }
    fn is_multi_input(&self) -> bool {
        false
    }
    fn rpc_methods(&self) -> Vec<RpcMethod> {
        vec![RpcMethod::new("ping", RpcMode::Reply)]
    }
    async fn process_streaming_async(
        &self,
        _data: RuntimeData,
        _: &dyn NodeRuntimeContextRead,
        mut cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<usize, Error> {
        cb(RuntimeData::Json(
            CallRequestEnvelope::new("harness", "decide", "legacy-1").to_aux_json(),
        ))?;
        Ok(1)
    }
}

/// No native RPC lane: replies come back as `__call_reply__` input frames.
struct LanelessCaller;

#[async_trait]
impl StreamingNode for LanelessCaller {
    fn node_type(&self) -> &str {
        "EpLaneless"
    }
    async fn process_async(
        &self,
        d: RuntimeData,
        _: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Ok(d)
    }
    async fn process_multi_async(
        &self,
        _: HashMap<String, RuntimeData>,
        _: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Err(Error::Execution("single input".into()))
    }
    fn is_multi_input(&self) -> bool {
        false
    }
    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        _: &dyn NodeRuntimeContextRead,
        mut cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<usize, Error> {
        if let RuntimeData::Json(v) = &data {
            if v["__aux_port__"] == CALL_REPLY_METHOD {
                cb(RuntimeData::Json(json!({"got_reply": v["payload"]})))?;
                return Ok(1);
            }
        }
        // Fire the call and return; the reply arrives as a later input.
        cb(RuntimeData::Json(
            CallRequestEnvelope::new("harness", "decide", "laneless-1").to_aux_json(),
        ))?;
        Ok(1)
    }
}

/// Declares only `__call_reply__`: other typed-RPC methods must keep the
/// input path (how Python nodes receive `@rpc` calls).
struct ReplyOnly;

#[async_trait]
impl StreamingNode for ReplyOnly {
    fn node_type(&self) -> &str {
        "EpReplyOnly"
    }
    async fn process_async(
        &self,
        d: RuntimeData,
        _: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Ok(d)
    }
    async fn process_multi_async(
        &self,
        _: HashMap<String, RuntimeData>,
        _: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Err(Error::Execution("single input".into()))
    }
    fn is_multi_input(&self) -> bool {
        false
    }
    fn rpc_methods(&self) -> Vec<RpcMethod> {
        vec![call_reply_method()]
    }
    async fn call_rpc(
        &self,
        _: RpcRequest,
        _: &dyn NodeRuntimeContextRead,
        _: &mut RpcResponder,
    ) -> Result<(), Error> {
        Ok(())
    }
    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        _: &dyn NodeRuntimeContextRead,
        mut cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<usize, Error> {
        cb(RuntimeData::Json(
            json!({"input": serde_json::to_value(&data).unwrap()}),
        ))?;
        Ok(1)
    }
}

/// Passthrough sink (`tts`).
struct Sink;

#[async_trait]
impl StreamingNode for Sink {
    fn node_type(&self) -> &str {
        "EpSink"
    }
    async fn process_async(
        &self,
        d: RuntimeData,
        _: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Ok(d)
    }
    async fn process_multi_async(
        &self,
        _: HashMap<String, RuntimeData>,
        _: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Err(Error::Execution("single input".into()))
    }
    fn is_multi_input(&self) -> bool {
        false
    }
}

struct Factory {
    node_type: &'static str,
    make: fn(&Value) -> Box<dyn StreamingNode>,
}

impl StreamingNodeFactory for Factory {
    fn create(
        &self,
        _id: String,
        params: &Value,
        _: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        Ok((self.make)(params))
    }
    fn node_type(&self) -> &str {
        self.node_type
    }
    fn is_multi_output_streaming(&self) -> bool {
        true
    }
}

async fn executor() -> Arc<PipelineExecutor> {
    let ex = Arc::new(PipelineExecutor::new().unwrap());
    let factories: Vec<Factory> = vec![
        Factory {
            node_type: "EpEnvCaller",
            make: |p| {
                Box::new(EnvCaller {
                    cfg: CallerCfg::from(p),
                    pending: Default::default(),
                    next: AtomicU64::new(1),
                })
            },
        },
        Factory {
            node_type: "EpCtxCaller",
            make: |p| {
                Box::new(CtxCaller {
                    cfg: CallerCfg::from(p),
                })
            },
        },
        Factory {
            node_type: "EpLegacy",
            make: |_| Box::new(LegacyEmitter),
        },
        Factory {
            node_type: "EpLaneless",
            make: |_| Box::new(LanelessCaller),
        },
        Factory {
            node_type: "EpReplyOnly",
            make: |_| Box::new(ReplyOnly),
        },
        Factory {
            node_type: "EpSink",
            make: |_| Box::new(Sink),
        },
    ];
    for f in factories {
        ex.register_factory(Arc::new(f)).await;
    }
    ex
}

fn manifest(nodes: &[(&str, &str, Value)], conns: &[(&str, &str)]) -> Manifest {
    Manifest {
        version: "v1".into(),
        metadata: ManifestMetadata {
            name: "endpoint-conformance".into(),
            ..Default::default()
        },
        nodes: nodes
            .iter()
            .map(|(id, ty, params)| NodeManifest {
                id: id.to_string(),
                node_type: ty.to_string(),
                params: params.clone(),
                ..Default::default()
            })
            .collect(),
        connections: conns
            .iter()
            .map(|(f, t)| Connection {
                from: f.to_string(),
                to: t.to_string(),
                from_port: None,
                to_port: None,
                edge_transport: None,
            })
            .collect(),
        ..Default::default()
    }
}

// ─── Session + host harness ─────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq)]
enum Via {
    InProc,
    Grpc,
}

struct Env {
    _executor: Arc<PipelineExecutor>,
    control: Arc<SessionControl>,
    session: SessionHandle,
    url: String,
    _server: tokio::task::JoinHandle<()>,
}

async fn env(nodes: &[(&str, &str, Value)], conns: &[(&str, &str)]) -> Env {
    let executor = executor().await;
    let session = executor
        .create_session(Arc::new(manifest(nodes, conns)))
        .await
        .expect("session");
    let control = executor
        .control_bus()
        .get(&session.session_id)
        .expect("control registered");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let svc = ControlServiceImpl::new(executor.control_bus());
    let server = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(PipelineControlServer::new(svc))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    Env {
        _executor: executor,
        control,
        session,
        url,
        _server: server,
    }
}

impl Env {
    async fn send(&self, text: &str) {
        self.session
            .send_input(TransportData::new(RuntimeData::Text(text.into())))
            .await
            .expect("send input");
    }

    async fn output(&mut self, within: Duration) -> Option<RuntimeData> {
        match tokio::time::timeout(within, self.session.recv_output()).await {
            Ok(Ok(Some(t))) => Some(t.data),
            _ => None,
        }
    }

    /// Next output that is a JSON object with `key`.
    async fn result(&mut self, key_any: &[&str], within: Duration) -> Value {
        let deadline = Instant::now() + within;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.output(left).await {
                Some(RuntimeData::Json(v)) if key_any.iter().any(|k| v.get(*k).is_some()) => {
                    return v
                }
                Some(other) => assert!(!is_call(&other), "__call__ leaked downstream: {other:?}"),
                None => panic!("no result with {key_any:?} within {within:?}"),
            }
        }
    }

    /// Drain outputs for `d`, asserting none is a `__call__` envelope.
    async fn assert_no_call_leak(&mut self, d: Duration) {
        let deadline = Instant::now() + d;
        while let Some(o) = self
            .output(deadline.saturating_duration_since(Instant::now()))
            .await
        {
            assert!(!is_call(&o), "__call__ leaked downstream: {o:?}");
        }
    }
}

fn is_call(d: &RuntimeData) -> bool {
    match d {
        RuntimeData::Json(v) => v["__aux_port__"] == "__call__",
        RuntimeData::Text(t) => t.contains("\"__call__\""),
        _ => false,
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Ev {
    Call {
        call_id: String,
        method: String,
        kwargs: Value,
    },
    Cancelled {
        call_id: String,
        reason: String,
    },
    Ended(String),
}

enum HostImpl {
    InProc(Option<EndpointHost>),
    Grpc {
        tx: Option<mpsc::Sender<ControlFrame>>,
        events: Option<tonic::Streaming<ControlEvent>>,
        buffered: VecDeque<ControlEvent>,
    },
}

struct TestHost {
    name: String,
    inner: HostImpl,
}

fn spec(name: &str) -> EndpointSpec {
    EndpointSpec::new(name, HostKind::Agent)
        .method("decide", RpcMode::Reply)
        .method("reply_text", RpcMode::Stream)
        .adapter("test-adapter")
}

async fn host(via: Via, env: &Env, spec: EndpointSpec, takeover: bool) -> Result<TestHost, String> {
    let name = spec.name.clone();
    match via {
        Via::InProc => env
            .control
            .host_endpoint(spec, takeover)
            .map(|h| TestHost {
                name,
                inner: HostImpl::InProc(Some(h)),
            })
            .map_err(|e| e.as_str().to_string()),
        Via::Grpc => {
            let channel = tonic::transport::Endpoint::from_shared(env.url.clone())
                .unwrap()
                .connect()
                .await
                .unwrap();
            let mut client = PipelineControlClient::new(channel);
            let (tx, rx) = mpsc::channel::<ControlFrame>(64);
            tx.send(ControlFrame {
                op: Some(PbOp::Hello(Hello {
                    session_id: env.session.session_id.clone(),
                    attach_id: "host".into(),
                    participant: None,
                })),
            })
            .await
            .unwrap();
            tx.send(ControlFrame {
                op: Some(PbOp::HostEndpoint(PbHost {
                    name: name.clone(),
                    methods: spec
                        .methods
                        .iter()
                        .map(|m| PbMethod {
                            name: m.name.clone(),
                            mode: match m.mode {
                                RpcMode::Reply => "reply",
                                RpcMode::Stream => "stream",
                                RpcMode::FireAndForget => "ff",
                            }
                            .into(),
                        })
                        .collect(),
                    host_kind: spec.host_kind.as_str().into(),
                    adapter: spec.adapter.clone().unwrap_or_default(),
                    queue_capacity: spec.queue_capacity as u32,
                    takeover,
                })),
            })
            .await
            .unwrap();
            let mut events = client
                .attach(ReceiverStream::new(rx))
                .await
                .unwrap()
                .into_inner();
            loop {
                let ev = tokio::time::timeout(Duration::from_secs(3), events.next())
                    .await
                    .expect("hosted result")
                    .expect("stream open")
                    .unwrap();
                match ev.event {
                    Some(PbEvent::Attached(_)) => continue,
                    Some(PbEvent::EndpointHosted(h)) if h.ok => {
                        return Ok(TestHost {
                            name,
                            inner: HostImpl::Grpc {
                                tx: Some(tx),
                                events: Some(events),
                                buffered: VecDeque::new(),
                            },
                        })
                    }
                    Some(PbEvent::EndpointHosted(h)) => return Err(h.error),
                    other => panic!("unexpected event before EndpointHosted: {other:?}"),
                }
            }
        }
    }
}

fn pb_to_ev(ev: ControlEvent) -> Option<Ev> {
    match ev.event? {
        PbEvent::EndpointCall(c) => Some(Ev::Call {
            call_id: c.call_id,
            method: c.method,
            kwargs: serde_json::from_str(&c.kwargs_json).unwrap(),
        }),
        PbEvent::EndpointCancelled(c) => Some(Ev::Cancelled {
            call_id: c.call_id,
            reason: c.reason,
        }),
        PbEvent::EndpointPresence(p) if !p.hosted => Some(Ev::Ended(p.reason)),
        _ => None,
    }
}

impl TestHost {
    async fn next(&mut self, within: Duration) -> Option<Ev> {
        match &mut self.inner {
            HostImpl::InProc(h) => {
                let h = h.as_mut()?;
                match tokio::time::timeout(within, h.next()).await {
                    Ok(Some(HostEvent::Call(c))) => Some(Ev::Call {
                        call_id: c.call_id,
                        method: c.method,
                        kwargs: Value::Object(c.kwargs),
                    }),
                    Ok(Some(HostEvent::Cancelled { call_id, reason })) => Some(Ev::Cancelled {
                        call_id,
                        reason: reason.as_str().into(),
                    }),
                    Ok(None) => Some(Ev::Ended(h.end_reason().unwrap_or("unhosted").into())),
                    Err(_) => None,
                }
            }
            HostImpl::Grpc {
                events, buffered, ..
            } => {
                while let Some(ev) = buffered.pop_front() {
                    if let Some(e) = pb_to_ev(ev) {
                        return Some(e);
                    }
                }
                let events = events.as_mut()?;
                let deadline = Instant::now() + within;
                loop {
                    let left = deadline.saturating_duration_since(Instant::now());
                    match tokio::time::timeout(left, events.next()).await {
                        Ok(Some(Ok(ev))) => {
                            if let Some(e) = pb_to_ev(ev) {
                                return Some(e);
                            }
                        }
                        _ => return None,
                    }
                }
            }
        }
    }

    async fn expect_call(&mut self) -> (String, String, Value) {
        match self.next(Duration::from_secs(3)).await {
            Some(Ev::Call {
                call_id,
                method,
                kwargs,
            }) => (call_id, method, kwargs),
            other => panic!("expected a call, got {other:?}"),
        }
    }

    /// Reply; `Err(reason)` when rejected (`late` / `cancelled` / ...).
    async fn reply(&mut self, call_id: &str, event: CallReplyEvent) -> Result<(), String> {
        match &mut self.inner {
            HostImpl::InProc(h) => h
                .as_ref()
                .expect("hosting")
                .replier()
                .send(call_id, event, Some(json!({"adapter": "test-adapter"})))
                .map_err(|k| match k {
                    CallErrorKind::Timeout => "late".to_string(),
                    other => other.as_str().to_string(),
                }),
            HostImpl::Grpc {
                tx,
                events,
                buffered,
            } => {
                let (kind, data_json, error, message) = match event {
                    CallReplyEvent::Value { data } => {
                        ("value", data.to_string(), String::new(), String::new())
                    }
                    CallReplyEvent::End => ("end", String::new(), String::new(), String::new()),
                    CallReplyEvent::Error { error, message } => {
                        ("error", String::new(), error.as_str().to_string(), message)
                    }
                };
                tx.as_ref()
                    .expect("hosting")
                    .send(ControlFrame {
                        op: Some(PbOp::EndpointReply(PbReply {
                            name: self.name.clone(),
                            call_id: call_id.into(),
                            kind: kind.into(),
                            data_json,
                            error,
                            message,
                            meta_json: json!({"adapter": "test-adapter"}).to_string(),
                        })),
                    })
                    .await
                    .unwrap();
                let events = events.as_mut().expect("hosting");
                loop {
                    let ev = tokio::time::timeout(Duration::from_secs(3), events.next())
                        .await
                        .expect("reply ack")
                        .expect("stream open")
                        .unwrap();
                    match &ev.event {
                        Some(PbEvent::EndpointReplyAck(a)) if a.call_id == call_id => {
                            return if a.accepted {
                                Ok(())
                            } else {
                                Err(a.reason.clone())
                            };
                        }
                        _ => buffered.push_back(ev),
                    }
                }
            }
        }
    }

    /// Lose the host (drop the registration / close the attach stream).
    fn drop_host(&mut self) {
        match &mut self.inner {
            HostImpl::InProc(h) => {
                h.take();
            }
            HostImpl::Grpc { tx, events, .. } => {
                tx.take();
                events.take();
            }
        }
    }
}

fn value(v: Value) -> CallReplyEvent {
    CallReplyEvent::Value { data: v }
}

fn caller_session(
    kind: &str,
    params: Value,
) -> (
    Vec<(&'static str, String, Value)>,
    Vec<(&'static str, &'static str)>,
) {
    (
        vec![
            ("brain", kind.to_string(), params),
            ("tts", "EpSink".into(), json!({})),
        ],
        vec![("brain", "tts")],
    )
}

async fn caller_env(kind: &str, params: Value) -> Env {
    let (nodes, conns) = caller_session(kind, params);
    let nodes: Vec<(&str, &str, Value)> = nodes
        .iter()
        .map(|(a, b, c)| (*a, b.as_str(), c.clone()))
        .collect();
    env(&nodes, &conns).await
}

const CALLERS: [&str; 2] = ["EpEnvCaller", "EpCtxCaller"];

macro_rules! both_transports {
    ($($name:ident),* $(,)?) => {$(
        mod $name {
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn inproc() {
                super::$name(super::Via::InProc).await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn grpc() {
                super::$name(super::Via::Grpc).await
            }
        }
    )*};
}

both_transports!(
    reply_round_trip_without_leak,
    stream_values_then_end,
    no_responder_fails_fast,
    hold_delivers_to_late_host,
    deadline_timeout_cancels_host,
    barge_in_cancels_and_counts_late_reply,
    host_loss_fails_pending_with_responder_gone,
    queue_overflow_is_overloaded,
    registration_rules,
    hermes_rpc_publish_becomes_call,
);

// ─── Cases ──────────────────────────────────────────────────────────────────

/// Reply round-trip from a node awaiting inside processing; the `__call__`
/// envelope reaches neither the downstream node nor tap observers.
async fn reply_round_trip_without_leak(via: Via) {
    for kind in CALLERS {
        let mut env = caller_env(kind, json!({})).await;
        let mut observer = env
            .control
            .subscribe(&ControlAddress::node_out("brain"))
            .unwrap();
        let mut in_observer = env
            .control
            .subscribe_in(&ControlAddress::node_in("harness").with_port("decide"))
            .unwrap();
        let mut h = host(via, &env, spec("harness"), false).await.unwrap();
        assert!(env.control.has_endpoint("harness"));
        assert!(env.control.endpoints().iter().any(|e| e.name == "harness"
            && e.methods
                .iter()
                .any(|m| m.name == "decide" && m.mode == RpcMode::Reply)));
        env.send("book a table").await;
        let (call_id, method, kwargs) = h.expect_call().await;
        assert_eq!(method, "decide");
        assert_eq!(kwargs["utterance"], "book a table");
        assert!(call_id.starts_with("brain/"), "{call_id}");
        h.reply(&call_id, value(json!({"tool": "reserve"})))
            .await
            .unwrap();
        let r = env.result(&["ok", "err"], Duration::from_secs(5)).await;
        assert_eq!(r["ok"], json!({"tool": "reserve"}), "{kind}: {r}");
        env.assert_no_call_leak(Duration::from_millis(200)).await;
        while let Ok(o) = observer.try_recv() {
            assert!(!is_call(&o), "{kind}: observer saw call payload {o:?}");
        }
        assert!(
            in_observer.try_recv().is_err(),
            "{kind}: .in tap carried the call"
        );
    }
}

async fn stream_values_then_end(via: Via) {
    for kind in CALLERS {
        let mut env = caller_env(kind, json!({"method": "reply_text", "mode": "stream"})).await;
        let mut h = host(via, &env, spec("harness"), false).await.unwrap();
        env.send("hi").await;
        let (call_id, method, _) = h.expect_call().await;
        assert_eq!(method, "reply_text");
        for part in ["a", "b", "c"] {
            h.reply(&call_id, value(json!(part))).await.unwrap();
        }
        h.reply(&call_id, CallReplyEvent::End).await.unwrap();
        let r = env.result(&["ok", "err"], Duration::from_secs(5)).await;
        assert_eq!(r["ok"], json!(["a", "b", "c"]), "{kind}: {r}");
    }
}

async fn no_responder_fails_fast(via: Via) {
    for kind in CALLERS {
        let mut env = caller_env(kind, json!({"deadline_ms": 5000})).await;
        // An attached client that does not host `harness`.
        let _other = host(via, &env, spec("other"), false).await.unwrap();
        env.send("x").await;
        let r = env.result(&["ok", "err"], Duration::from_secs(2)).await;
        assert_eq!(r["err"], "no_responder", "{kind}: {r}");
        assert!(r["elapsed_ms"].as_u64().unwrap() < 50, "{kind}: {r}");
    }
}

async fn hold_delivers_to_late_host(via: Via) {
    for kind in CALLERS {
        let mut env = caller_env(kind, json!({"hold_ms": 1000, "deadline_ms": 3000})).await;
        env.send("late host").await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let mut h = host(via, &env, spec("harness"), false).await.unwrap();
        let (call_id, _, _) = h.expect_call().await;
        h.reply(&call_id, value(json!("held"))).await.unwrap();
        let r = env.result(&["ok", "err"], Duration::from_secs(3)).await;
        assert_eq!(r["ok"], "held", "{kind}: {r}");
    }
}

async fn deadline_timeout_cancels_host(via: Via) {
    for kind in CALLERS {
        let mut env = caller_env(kind, json!({"deadline_ms": 600})).await;
        let mut h = host(via, &env, spec("harness"), false).await.unwrap();
        env.send("slow").await;
        let (call_id, _, _) = h.expect_call().await;
        let r = env.result(&["ok", "err"], Duration::from_secs(3)).await;
        assert_eq!(r["err"], "timeout", "{kind}: {r}");
        let ms = r["elapsed_ms"].as_u64().unwrap();
        assert!((550..1200).contains(&ms), "{kind}: elapsed {ms}");
        assert_eq!(
            h.next(Duration::from_secs(2)).await,
            Some(Ev::Cancelled {
                call_id: call_id.clone(),
                reason: "deadline".into()
            }),
            "{kind}"
        );
        assert_eq!(h.reply(&call_id, value(json!(1))).await, Err("late".into()));
        let tel = env.control.endpoint_telemetry(false);
        let m = tel
            .iter()
            .find(|t| t.endpoint == "harness" && t.method == "decide")
            .unwrap();
        assert_eq!(m.errors.get("timeout"), Some(&1));
        assert_eq!(m.late, 1);
    }
}

async fn barge_in_cancels_and_counts_late_reply(via: Via) {
    for kind in CALLERS {
        let env = caller_env(kind, json!({"deadline_ms": 10_000})).await;
        let mut h = host(via, &env, spec("harness"), false).await.unwrap();
        env.send("long question").await;
        let (call_id, _, _) = h.expect_call().await;
        env.control
            .publish(
                &ControlAddress::node_in("brain").with_port("barge_in"),
                RuntimeData::Json(json!({})),
            )
            .await
            .unwrap();
        assert_eq!(
            h.next(Duration::from_secs(2)).await,
            Some(Ev::Cancelled {
                call_id: call_id.clone(),
                reason: "caller".into()
            }),
            "{kind}"
        );
        let rejected = h.reply(&call_id, value(json!("too late"))).await;
        assert_eq!(rejected, Err("cancelled".into()), "{kind}");
        let tel = env.control.endpoint_telemetry(false);
        let m = tel
            .iter()
            .find(|t| t.endpoint == "harness" && t.method == "decide")
            .unwrap();
        assert_eq!(m.late, 1, "{kind}");
        assert_eq!(m.errors.get("cancelled"), Some(&1), "{kind}");
    }
}

async fn host_loss_fails_pending_with_responder_gone(via: Via) {
    let mut env = caller_env("EpCtxCaller", json!({"fanout": 3, "deadline_ms": 10_000})).await;
    let mut presence = env.control.watch_endpoints();
    let mut h = host(via, &env, spec("harness"), false).await.unwrap();
    env.send("three").await;
    for _ in 0..3 {
        h.expect_call().await;
    }
    let lost = Instant::now();
    h.drop_host();
    for _ in 0..3 {
        let r = env.result(&["ok", "err"], Duration::from_secs(2)).await;
        assert_eq!(r["err"], "responder_gone", "{r}");
    }
    let budget = if via == Via::InProc { 100 } else { 1_000 };
    assert!(
        lost.elapsed() < Duration::from_millis(budget),
        "{:?}",
        lost.elapsed()
    );
    assert!(!env.control.has_endpoint("harness"));
    let mut saw_unhosted = false;
    while let Ok(p) = presence.try_recv() {
        if p.name == "harness" && !p.hosted {
            saw_unhosted = true;
        }
    }
    assert!(saw_unhosted);
}

async fn queue_overflow_is_overloaded(via: Via) {
    let mut env = caller_env("EpCtxCaller", json!({"fanout": 2, "deadline_ms": 5_000})).await;
    let mut h = host(via, &env, spec("harness").queue_capacity(1), false)
        .await
        .unwrap();
    env.send("two").await;
    let (call_id, _, _) = h.expect_call().await;
    // The second call failed at once; the first is still pending.
    assert_eq!(h.next(Duration::from_millis(200)).await, None);
    h.reply(&call_id, value(json!("first"))).await.unwrap();
    let a = env.result(&["ok", "err"], Duration::from_secs(2)).await;
    let b = env.result(&["ok", "err"], Duration::from_secs(2)).await;
    let mut got = vec![a, b];
    got.sort_by_key(|v| v.get("ok").is_some());
    assert_eq!(got[0]["err"], "overloaded", "{got:?}");
    assert_eq!(got[1]["ok"], "first", "{got:?}");
}

async fn registration_rules(via: Via) {
    let mut env = caller_env("EpCtxCaller", json!({"deadline_ms": 5_000})).await;
    assert_eq!(
        host(via, &env, spec("brain"), false).await.err(),
        Some("name_conflict".into())
    );
    assert_eq!(
        host(via, &env, spec("__system__"), false).await.err(),
        Some("name_conflict".into())
    );
    let mut a = host(via, &env, spec("harness"), false).await.unwrap();
    assert_eq!(
        host(via, &env, spec("harness"), false).await.err(),
        Some("already_hosted".into())
    );
    // A remains the host.
    env.send("to a").await;
    let (call_id, _, _) = a.expect_call().await;
    // Takeover: A's pending call fails with responder_gone, A is told.
    let mut b = host(via, &env, spec("harness"), true).await.unwrap();
    let r = env.result(&["ok", "err"], Duration::from_secs(2)).await;
    assert_eq!(r["err"], "responder_gone", "{r}");
    assert_eq!(
        a.next(Duration::from_secs(2)).await,
        Some(Ev::Ended("taken_over".into()))
    );
    let _ = call_id;
    env.send("to b").await;
    let (call_id, _, _) = b.expect_call().await;
    b.reply(&call_id, value(json!("b"))).await.unwrap();
    let r = env.result(&["ok", "err"], Duration::from_secs(2)).await;
    assert_eq!(r["ok"], "b");
}

/// Task 2.9: an RPC-shaped publish (Hermes `tool_request` shape) to a
/// hosted `hermes` endpoint becomes a call; host loss yields an error reply
/// with kind `responder_gone` on the caller's `__reply__` tap instead of a
/// 30 s timeout. Unhosted, the publish keeps today's behavior.
async fn hermes_rpc_publish_becomes_call(via: Via) {
    let env = env(&[("llm", "EpSink", json!({}))], &[]).await;
    let mut reply_tap = env
        .control
        .subscribe(&ControlAddress::node_out("llm-node").with_port("__reply__"))
        .unwrap();
    let mut h = host(
        via,
        &env,
        EndpointSpec::new("hermes", HostKind::Agent),
        false,
    )
    .await
    .unwrap();
    let request = |corr: &str| {
        RuntimeData::Json(json!({
            "corr_id": corr,
            "tool_name": "web_search",
            "arguments": {"q": "weather"},
            "node_id": "llm-node",
            "pipeline_id": "p",
            "timeout_ms": 30_000,
        }))
    };
    let addr = ControlAddress::node_in("hermes").with_port("tool_request");
    // Success path.
    env.control.publish(&addr, request("t1")).await.unwrap();
    let (call_id, method, kwargs) = h.expect_call().await;
    assert_eq!(method, "tool_request");
    assert_eq!(call_id, "llm-node/t1");
    assert_eq!(kwargs["tool_name"], "web_search");
    h.reply(&call_id, value(json!({"result": "sunny"})))
        .await
        .unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(2), reply_tap.recv())
        .await
        .unwrap()
        .unwrap();
    let RuntimeData::Json(reply) = reply else {
        panic!("json reply")
    };
    assert_eq!(reply["corr_id"], "t1");
    assert_eq!(reply["kind"], "value");
    assert_eq!(reply["data"], json!({"result": "sunny"}));
    // Host loss while a request is pending.
    env.control.publish(&addr, request("t2")).await.unwrap();
    h.expect_call().await;
    let lost = Instant::now();
    h.drop_host();
    let reply = tokio::time::timeout(Duration::from_secs(2), reply_tap.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(lost.elapsed() < Duration::from_secs(1));
    let RuntimeData::Json(reply) = reply else {
        panic!("json reply")
    };
    assert_eq!(reply["corr_id"], "t2");
    assert_eq!(reply["kind"], "error");
    assert_eq!(reply["error"], "responder_gone");
    // Unhosted: legacy behavior (no endpoint call, no synthesized reply).
    let _ = env.control.publish(&addr, request("t3")).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), reply_tap.recv())
            .await
            .is_err()
    );
}

// ─── Transport-independent cases ────────────────────────────────────────────

/// Nodes that do not opt in keep today's semantics: a node with an RPC lane
/// but no `__call_reply__` forwards `__call__` envelopes downstream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opt_in_only_legacy_nodes_forward_call_envelopes() {
    let mut env = env(
        &[
            ("brain", "EpLegacy", json!({})),
            ("tts", "EpSink", json!({})),
        ],
        &[("brain", "tts")],
    )
    .await;
    let mut h = host(Via::InProc, &env, spec("harness"), false)
        .await
        .unwrap();
    env.send("x").await;
    let out = env
        .output(Duration::from_secs(2))
        .await
        .expect("forwarded envelope");
    assert!(is_call(&out), "{out:?}");
    assert_eq!(h.next(Duration::from_millis(200)).await, None);
}

/// Lane-less nodes are not opted in either: their `__call__` envelopes (and
/// upstream envelopes a passthrough node forwards) keep today's semantics.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn laneless_nodes_are_not_opted_in() {
    let mut env = env(
        &[
            ("brain", "EpLaneless", json!({})),
            ("tts", "EpSink", json!({})),
        ],
        &[("brain", "tts")],
    )
    .await;
    let mut h = host(Via::InProc, &env, spec("harness"), false)
        .await
        .unwrap();
    env.send("go").await;
    let out = env
        .output(Duration::from_secs(2))
        .await
        .expect("forwarded envelope");
    assert!(is_call(&out), "{out:?}");
    assert_eq!(h.next(Duration::from_millis(200)).await, None);
}
/// A call-reply-only lane must not capture other typed-RPC methods: they
/// keep the input path (how Python nodes receive `@rpc` calls), and the
/// reserved `__call_reply__` cannot be injected from outside.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn call_reply_only_lane_keeps_other_rpc_on_input_path() {
    let mut env = env(&[("py", "EpReplyOnly", json!({}))], &[]).await;
    env.control
        .publish(
            &ControlAddress::node_in("py").with_port("set_voice"),
            RuntimeData::Json(json!({"corr_id": 7, "args": ["alice"], "kwargs": {}})),
        )
        .await
        .unwrap();
    let r = env.result(&["input"], Duration::from_secs(2)).await;
    let input = &r["input"];
    let text = input.to_string();
    assert!(
        text.contains("set_voice") && text.contains("alice"),
        "{input}"
    );
    let spoof = env
        .control
        .publish(
            &ControlAddress::node_in("py").with_port(CALL_REPLY_METHOD),
            RuntimeData::Json(json!({"corr_id": "x", "args": [], "kwargs": {}})),
        )
        .await;
    assert!(spoof.is_err());
}

/// Endpoint telemetry per method + `agent_backed` (task 2.11).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn telemetry_counts_per_method_and_flags_agent_backed() {
    let mut env = caller_env("EpCtxCaller", json!({"deadline_ms": 300})).await;
    assert!(!env.control.summary().agent_backed);
    let mut h = host(Via::InProc, &env, spec("harness"), false)
        .await
        .unwrap();
    for i in 0..3 {
        env.send("q").await;
        let (call_id, _, _) = h.expect_call().await;
        if i < 2 {
            h.reply(&call_id, value(json!(i))).await.unwrap();
        }
        env.result(&["ok", "err"], Duration::from_secs(2)).await;
    }
    let _ = h.next(Duration::from_millis(100)).await; // deadline cancellation
    let tel = env.control.endpoint_telemetry(true);
    let m = tel
        .iter()
        .find(|t| t.endpoint == "harness" && t.method == "decide")
        .unwrap();
    assert_eq!((m.calls, m.ok), (3, 2));
    assert_eq!(m.errors.get("timeout"), Some(&1));
    assert!(m.latency_us.is_some());
    assert_eq!(m.host_kind, Some(HostKind::Agent));
    let adapter = m
        .adapters
        .iter()
        .find(|a| a.adapter == "test-adapter")
        .unwrap();
    assert_eq!((adapter.ok, adapter.errors.get("timeout")), (2, Some(&1)));
    assert!(env.control.summary().agent_backed);
    assert!(env.control.agent_backed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mark_agent_backed_sets_summary_flag() {
    let env = env(&[("llm", "EpSink", json!({}))], &[]).await;
    assert!(!env.control.summary().agent_backed);
    env.control.mark_agent_backed();
    assert!(env.control.summary().agent_backed);
}

/// Observe-only control servers refuse hosting with `forbidden`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observe_only_server_refuses_hosting() {
    let env = env(&[("llm", "EpSink", json!({}))], &[]).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let svc = ControlServiceImpl::new(env._executor.control_bus()).with_endpoint_hosting(false);
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(PipelineControlServer::new(svc))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    let observe_only = Env {
        _executor: env._executor.clone(),
        control: env.control.clone(),
        session: env.session,
        url,
        _server: tokio::spawn(async {}),
    };
    let err = host(Via::Grpc, &observe_only, spec("harness"), false)
        .await
        .err();
    assert_eq!(err, Some("forbidden".into()));
    assert!(!observe_only.control.has_endpoint("harness"));
}

// ─── Loadable-Rust caller (plugin-sdk helper over the FFI adapter) ──────────

/// A plugin node built with `remotemedia-plugin-sdk`'s `EndpointCaller`,
/// crossing the real FFI adapter (`wrap_ffi_factory` + `StreamingNodeFfiAdapter`,
/// the same vtable a dlopen'd cdylib uses). It awaits the reply inside its
/// streaming process call; the reply arrives through `__call_reply__` on the
/// loadable RPC lane while that call is still in flight.
#[cfg(all(feature = "loadable", feature = "loadable-export"))]
mod loadable_caller {
    use super::*;
    use remotemedia_core::loadable::factory::wrap_ffi_factory;
    use remotemedia_plugin_sdk::abi_stable::sabi_trait::TD_Opaque;
    use remotemedia_plugin_sdk::abi_stable::std_types::{RResult, RString};
    use remotemedia_plugin_sdk::adapter::StreamingNodeFfiAdapter;
    use remotemedia_plugin_sdk::endpoint_call::{
        CallOptions as PCallOptions, Emitter, EndpointCaller,
    };
    use remotemedia_plugin_sdk::traits::AsyncStreamingNode;
    use remotemedia_plugin_sdk::{FfiNodeBox, FfiNodeFactory, FfiNode_TO};

    struct PluginBrain {
        calls: EndpointCaller,
    }

    #[async_trait]
    impl AsyncStreamingNode for PluginBrain {
        fn node_type(&self) -> &str {
            "EpPluginCaller"
        }
        async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
            Ok(data)
        }
        async fn process_streaming_with_ctx<F>(
            &self,
            data: RuntimeData,
            _ctx: &dyn NodeRuntimeContextRead,
            callback: F,
        ) -> Result<usize, Error>
        where
            F: FnMut(RuntimeData) -> Result<(), Error> + Send,
        {
            let emit = Emitter::new(callback);
            let started = Instant::now();
            let mut kwargs = Map::new();
            kwargs.insert("utterance".into(), utterance(&data).into());
            let result = match self.calls.call(
                &emit,
                "harness",
                "decide",
                vec![],
                kwargs,
                PCallOptions::default().deadline_ms(2_000),
            ) {
                Ok(call) => call.value().await.map_err(|e| (e.kind, e.message)),
                Err(e) => Err((e.kind, e.message)),
            };
            emit.emit(result_json(result, started))?;
            Ok(2)
        }
        fn rpc_methods(&self) -> Vec<RpcMethod> {
            vec![call_reply_method()]
        }
        async fn call_rpc(
            &self,
            req: RpcRequest,
            _: &dyn NodeRuntimeContextRead,
            _: &mut RpcResponder,
        ) -> Result<(), Error> {
            self.calls
                .handle_rpc(&req)
                .unwrap_or_else(|| Err(Error::Execution("unknown method".into())))
        }
    }

    struct PluginFactory;

    impl FfiNodeFactory for PluginFactory {
        fn node_type(&self) -> RString {
            "EpPluginCaller".into()
        }
        fn create(&self, _params: RString) -> RResult<FfiNodeBox, RString> {
            RResult::ROk(FfiNode_TO::from_value(
                StreamingNodeFfiAdapter::new(PluginBrain {
                    calls: EndpointCaller::new(),
                }),
                TD_Opaque,
            ))
        }
    }

    async fn plugin_env() -> Env {
        let executor = executor().await;
        executor
            .register_factory(wrap_ffi_factory(PluginFactory))
            .await;
        let session = executor
            .create_session(Arc::new(manifest(
                &[
                    ("brain", "EpPluginCaller", json!({})),
                    ("tts", "EpSink", json!({})),
                ],
                &[("brain", "tts")],
            )))
            .await
            .expect("session");
        let control = executor.control_bus().get(&session.session_id).unwrap();
        Env {
            _executor: executor,
            control,
            session,
            url: String::new(),
            _server: tokio::spawn(async {}),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn loadable_plugin_awaits_reply_inside_processing() {
        let mut env = plugin_env().await;
        let mut h = host(Via::InProc, &env, spec("harness"), false)
            .await
            .unwrap();
        env.send("book a table").await;
        let (call_id, _, kwargs) = h.expect_call().await;
        assert_eq!(kwargs["utterance"], "book a table");
        h.reply(&call_id, value(json!({"tool": "reserve"})))
            .await
            .unwrap();
        let r = env.result(&["ok", "err"], Duration::from_secs(5)).await;
        assert_eq!(r["ok"], json!({"tool": "reserve"}), "{r}");
        env.assert_no_call_leak(Duration::from_millis(200)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn loadable_plugin_no_responder_fails_fast() {
        let mut env = plugin_env().await;
        env.send("x").await;
        let r = env.result(&["ok", "err"], Duration::from_secs(2)).await;
        assert_eq!(r["err"], "no_responder", "{r}");
        assert!(r["elapsed_ms"].as_u64().unwrap() < 50, "{r}");
    }
}
