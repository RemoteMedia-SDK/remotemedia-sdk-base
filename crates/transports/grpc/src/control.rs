//! gRPC handler for `PipelineControl` — the Session Control Bus over gRPC.
//!
//! Wire protocol is defined in `proto/control.proto`; the service is a
//! single bidirectional streaming RPC `Attach(stream ControlFrame)
//! returns (stream ControlEvent)`.
//!
//! Per-attach state machine:
//!
//! ```text
//!  client -> Hello { session_id }
//!  server -> Attached { session_id, attach_id } | Error(SESSION_NOT_FOUND) + end
//!  [ both sides loop: client ControlFrames / server ControlEvents ]
//!  ( session terminates on router side )
//!  server -> SessionClosed { reason } ; end response stream
//! ```
//!
//! An attach holds no strong reference to the session beyond an
//! `Arc<SessionControl>`; dropping the attach has no effect on session
//! lifetime. When the router closes, the server drains all pending
//! operations, emits `SessionClosed`, and ends the response stream.

use crate::generated::control_event::Event as PbEvent;
use crate::generated::control_frame::Op as PbOp;
use crate::generated::intercept_decision::Decision as PbDecision;
use crate::generated::pipeline_control_server::PipelineControl;
use crate::generated::session_lifecycle::Event as PbLifecycle;
use crate::generated::{
    Attached, CloseReasonCode, ControlAddress as PbAddress, ControlDirection, ControlErrorCode,
    ControlEvent, ControlFrame, DescribeSessionRequest, DescribeSessionResponse, Empty,
    EndpointCall as PbEndpointCall, EndpointCancelled as PbEndpointCancelled,
    EndpointHosted as PbEndpointHosted, EndpointPresenceEvent as PbEndpointPresence,
    EndpointReplyAck as PbEndpointReplyAck, ErrorEvent, InterceptRequest as PbInterceptRequest,
    ListSessionsRequest, ListSessionsResponse, NodeState as PbNodeState, SessionClosed,
    SessionClosedInfo, SessionLifecycle, SessionSummaryPb, TapEvent, WatchSessionsRequest,
};
use remotemedia_core::transport::endpoint::{EndpointReplier, EndpointSpec, HostEvent, HostKind};

use remotemedia_core::telemetry::{TelemetryLevel, SCHEMA_VERSION as OBSERVER_SCHEMA_VERSION};
use remotemedia_core::transport::session_control::{
    CloseReason, ControlAddress as CoreAddress, ControlEvent as CoreEvent, Direction,
    InterceptDecision as CoreDecision, NodeState, RpcCallScope, SessionControl, SessionControlBus,
    SessionLifecycleEvent, SessionPhase, SessionSummary, SYSTEM_PORT,
};

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

// Capacity of the outbound event channel. Large enough to absorb short
// bursts from a tapped node without clobbering the per-subscriber
// broadcast; small enough that a stuck client gets backpressured.
const OUTBOUND_EVENT_CAPACITY: usize = 256;

/// gRPC `PipelineControl` service implementation.
#[derive(Clone)]
pub struct ControlServiceImpl {
    bus: Arc<SessionControlBus>,
    /// Optional bearer token every request must carry
    /// (`authorization: Bearer <token>`). `None` = unauthenticated, which
    /// matches the historical behaviour of `Attach`.
    token: Option<Arc<str>>,
    /// Whether attaches may host endpoints (a control operation). When a
    /// bearer token is configured, every attach — and so every host —
    /// must carry it.
    allow_hosting: bool,
}

impl ControlServiceImpl {
    pub fn new(bus: Arc<SessionControlBus>) -> Self {
        Self {
            bus,
            token: None,
            allow_hosting: true,
        }
    }

    /// Allow or refuse `HostEndpoint` frames (refused ⇒ `forbidden`). Use
    /// `false` for observe-only control servers.
    pub fn with_endpoint_hosting(mut self, allow: bool) -> Self {
        self.allow_hosting = allow;
        self
    }

    /// Require `authorization: Bearer <token>` on every RPC.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        let token: String = token.into();
        self.token = (!token.is_empty()).then(|| Arc::from(token.as_str()));
        self
    }

    fn check_auth<T>(&self, req: &Request<T>) -> Result<(), Status> {
        let Some(expected) = &self.token else {
            return Ok(());
        };
        let got = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| {
                v.strip_prefix("Bearer ")
                    .or_else(|| v.strip_prefix("bearer "))
            });
        match got {
            Some(t) if t == expected.as_ref() => Ok(()),
            _ => Err(Status::unauthenticated("missing or invalid bearer token")),
        }
    }
}

type EventStream = Pin<Box<dyn Stream<Item = Result<ControlEvent, Status>> + Send + 'static>>;
type LifecycleStream =
    Pin<Box<dyn Stream<Item = Result<SessionLifecycle, Status>> + Send + 'static>>;

fn phase_str(p: SessionPhase) -> &'static str {
    match p {
        SessionPhase::Initializing => "initializing",
        SessionPhase::Warm => "warm",
        SessionPhase::Active => "active",
        SessionPhase::Closing => "closing",
    }
}

fn summary_to_pb(s: SessionSummary) -> SessionSummaryPb {
    SessionSummaryPb {
        session_id: s.session_id,
        pipeline_name: s.pipeline_name,
        created_at_ms: s.created_at_ms,
        transport: s.transport.unwrap_or_default(),
        phase: phase_str(s.phase).to_string(),
        node_count: s.node_count,
        origin: s.origin.unwrap_or_default(),
        has_telemetry: s.has_telemetry,
        agent_backed: s.agent_backed,
    }
}

fn list_response(sessions: Vec<SessionSummary>) -> ListSessionsResponse {
    ListSessionsResponse {
        sessions: sessions.into_iter().map(summary_to_pb).collect(),
        observer_schema_version: OBSERVER_SCHEMA_VERSION as u32,
        server_version: env!("CARGO_PKG_VERSION").to_string(),
    }
}

#[tonic::async_trait]
impl PipelineControl for ControlServiceImpl {
    type AttachStream = EventStream;
    type WatchSessionsStream = LifecycleStream;

    async fn list_sessions(
        &self,
        request: Request<ListSessionsRequest>,
    ) -> Result<Response<ListSessionsResponse>, Status> {
        self.check_auth(&request)?;
        Ok(Response::new(list_response(self.bus.list())))
    }

    async fn describe_session(
        &self,
        request: Request<DescribeSessionRequest>,
    ) -> Result<Response<DescribeSessionResponse>, Status> {
        self.check_auth(&request)?;
        let sid = request.into_inner().session_id;
        let ctrl = self
            .bus
            .get(&sid)
            .ok_or_else(|| Status::not_found(format!("session '{sid}' not found")))?;
        let descriptor = ctrl
            .descriptor()
            .ok_or_else(|| Status::not_found(format!("session '{sid}' has no descriptor")))?;
        let descriptor_json = serde_json::to_string(descriptor.as_ref())
            .map_err(|e| Status::internal(format!("descriptor serialization: {e}")))?;
        Ok(Response::new(DescribeSessionResponse {
            descriptor_json,
            schema_version: OBSERVER_SCHEMA_VERSION as u32,
        }))
    }

    async fn watch_sessions(
        &self,
        request: Request<WatchSessionsRequest>,
    ) -> Result<Response<Self::WatchSessionsStream>, Status> {
        self.check_auth(&request)?;
        let bus = self.bus.clone();
        let (tx, rx) = mpsc::channel::<Result<SessionLifecycle, Status>>(OUTBOUND_EVENT_CAPACITY);
        tokio::spawn(async move {
            let (initial, mut events) = bus.watch();
            let first = SessionLifecycle {
                event: Some(PbLifecycle::Initial(list_response(initial))),
            };
            if tx.send(Ok(first)).await.is_err() {
                return;
            }
            loop {
                let msg = tokio::select! {
                    _ = tx.closed() => return,
                    ev = events.recv() => ev,
                };
                let out = match msg {
                    Ok(SessionLifecycleEvent::Created { session }) => {
                        vec![PbLifecycle::Created(summary_to_pb(session))]
                    }
                    Ok(SessionLifecycleEvent::Updated { session }) => {
                        vec![PbLifecycle::Updated(summary_to_pb(session))]
                    }
                    Ok(SessionLifecycleEvent::Closed {
                        session_id,
                        reason,
                        ts_ms,
                    }) => vec![PbLifecycle::Closed(SessionClosedInfo {
                        session_id,
                        reason: reason.unwrap_or_default(),
                        ts_ms,
                    })],
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Fell behind: tell the client, then re-send a full
                        // list under a fresh watch.
                        let (initial, fresh) = bus.watch();
                        events = fresh;
                        vec![
                            PbLifecycle::Resync(Empty {}),
                            PbLifecycle::Initial(list_response(initial)),
                        ]
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                };
                for ev in out {
                    if tx
                        .send(Ok(SessionLifecycle { event: Some(ev) }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn attach(
        &self,
        request: Request<Streaming<ControlFrame>>,
    ) -> Result<Response<Self::AttachStream>, Status> {
        self.check_auth(&request)?;
        let mut in_stream = request.into_inner();
        let (out_tx, out_rx) =
            mpsc::channel::<Result<ControlEvent, Status>>(OUTBOUND_EVENT_CAPACITY);

        // Read the first frame — must be Hello.
        let first = in_stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("attach closed before Hello"))?
            .map_err(|e| Status::internal(format!("stream error before Hello: {e}")))?;

        let (session_id, attach_id) = match first.op {
            Some(PbOp::Hello(hello)) => (hello.session_id, hello.attach_id),
            _ => {
                return Err(Status::invalid_argument("first ControlFrame must be Hello"));
            }
        };

        let ctrl = match self.bus.get(&session_id) {
            Some(c) => c,
            None => {
                return Err(Status::not_found(format!(
                    "session '{session_id}' not found"
                )));
            }
        };

        // Hello accepted — send the Attached event and wire up background
        // forwarders for taps, intercepts, and close signals.
        send_event(
            &out_tx,
            ControlEvent {
                event: Some(PbEvent::Attached(Attached {
                    session_id: session_id.clone(),
                    attach_id: attach_id.clone(),
                    observer_schema_version: OBSERVER_SCHEMA_VERSION as u32,
                })),
            },
        )
        .await;

        // Spawn the inbound-frame dispatch + outbound-event forwarding loop.
        // Hosting requires an authorized attach: `check_auth` above already
        // enforced the bearer token when one is configured.
        let bus = self.bus.clone();
        let can_host = self.allow_hosting;
        tokio::spawn(async move {
            run_attach_loop(ctrl, bus, session_id, in_stream, out_tx, can_host).await;
        });

        let stream: EventStream = Box::pin(ReceiverStream::new(out_rx));
        Ok(Response::new(stream))
    }
}

// ─── The per-attach background loop ──────────────────────────────────────────

async fn run_attach_loop(
    ctrl: Arc<SessionControl>,
    _bus: Arc<SessionControlBus>,
    session_id: String,
    mut in_stream: Streaming<ControlFrame>,
    out_tx: mpsc::Sender<Result<ControlEvent, Status>>,
    can_host: bool,
) {
    // Watch for session-close. First fire ends this attach.
    let mut close_rx = ctrl.close_subscriber();
    let rpc_scope = RpcCallScope::default();
    // Endpoints hosted by this attach; dropped (⇒ unregistered, pending
    // calls fail with `responder_gone`) when the attach ends.
    let mut hosted = HostedEndpoints {
        can_host,
        map: HashMap::new(),
    };

    // Dedicated forwarder tasks for taps/intercepts. We hold their
    // JoinHandles so we can abort on shutdown / unsubscribe.
    let mut forwarders = Forwarders::default();

    loop {
        tokio::select! {
            _ = out_tx.closed() => break,
            // Client → server frame.
            frame = in_stream.next() => {
                let frame = match frame {
                    Some(Ok(f)) => f,
                    Some(Err(e)) => {
                        tracing::warn!(session=%session_id, "control stream error: {e}");
                        break;
                    }
                    None => {
                        tracing::debug!(session=%session_id, "client closed attach stream");
                        break;
                    }
                };

                if let Err(proto_err) = handle_inbound_frame(
                    &ctrl,
                    frame,
                    &out_tx,
                    &mut forwarders,
                    &rpc_scope,
                    &mut hosted,
                )
                .await
                {
                    // Protocol-level error — surface as ErrorEvent but keep
                    // the attach alive. Only fatal if the client disconnects.
                    send_event(
                        &out_tx,
                        ControlEvent {
                            event: Some(PbEvent::Error(proto_err)),
                        },
                    )
                    .await;
                }
            }

            // Session closed.
            reason = close_rx.recv() => {
                let (code, detail) = match reason {
                    Ok(CloseReason::Normal) => (CloseReasonCode::Normal as i32, String::new()),
                    Ok(CloseReason::Error(msg)) => (CloseReasonCode::Error as i32, msg),
                    Err(_lagged) => (CloseReasonCode::Unspecified as i32, "close channel lag".into()),
                };
                let _ = out_tx
                    .send(Ok(ControlEvent {
                        event: Some(PbEvent::SessionClosed(SessionClosed {
                            reason: code,
                            detail,
                        })),
                    }))
                    .await;
                break;
            }
        }
    }

    hosted.release_all();
    forwarders.abort_all();
    tracing::debug!(session=%session_id, "attach loop exited");
}

/// One endpoint hosted by an attach. The forwarder task owns the core
/// `EndpointHost`; aborting it drops the host, which unregisters it.
struct HostedEndpoint {
    replier: EndpointReplier,
    task: tokio::task::JoinHandle<()>,
}

struct HostedEndpoints {
    can_host: bool,
    map: HashMap<String, HostedEndpoint>,
}

impl HostedEndpoints {
    fn release(&mut self, name: &str) {
        if let Some(h) = self.map.remove(name) {
            h.task.abort();
        }
    }

    fn release_all(&mut self) {
        for (_, h) in self.map.drain() {
            h.task.abort();
        }
    }
}

fn mode_str(mode: remotemedia_core::transport::endpoint::RpcMode) -> &'static str {
    match mode {
        remotemedia_core::transport::endpoint::RpcMode::FireAndForget => "ff",
        remotemedia_core::transport::endpoint::RpcMode::Reply => "reply",
        remotemedia_core::transport::endpoint::RpcMode::Stream => "stream",
    }
}

fn parse_mode(s: &str) -> Option<remotemedia_core::transport::endpoint::RpcMode> {
    match s {
        "ff" | "fire_and_forget" => {
            Some(remotemedia_core::transport::endpoint::RpcMode::FireAndForget)
        }
        "reply" | "" => Some(remotemedia_core::transport::endpoint::RpcMode::Reply),
        "stream" => Some(remotemedia_core::transport::endpoint::RpcMode::Stream),
        _ => None,
    }
}

fn hosted_event(name: &str, result: Result<(), (&str, String)>) -> ControlEvent {
    let (ok, error, message) = match result {
        Ok(()) => (true, String::new(), String::new()),
        Err((kind, msg)) => (false, kind.to_string(), msg),
    };
    ControlEvent {
        event: Some(PbEvent::EndpointHosted(PbEndpointHosted {
            name: name.to_string(),
            ok,
            error,
            message,
        })),
    }
}

fn reply_ack(name: &str, call_id: &str, result: Result<(), &str>) -> ControlEvent {
    ControlEvent {
        event: Some(PbEvent::EndpointReplyAck(PbEndpointReplyAck {
            name: name.to_string(),
            call_id: call_id.to_string(),
            accepted: result.is_ok(),
            reason: result.err().unwrap_or_default().to_string(),
        })),
    }
}

async fn host_endpoint_frame(
    ctrl: &Arc<SessionControl>,
    req: crate::generated::HostEndpoint,
    out_tx: &mpsc::Sender<Result<ControlEvent, Status>>,
    hosted: &mut HostedEndpoints,
) {
    let name = req.name.clone();
    if !hosted.can_host {
        send_event(
            out_tx,
            hosted_event(
                &name,
                Err((
                    "forbidden",
                    "endpoint hosting is disabled on this control server".into(),
                )),
            ),
        )
        .await;
        return;
    }
    let Some(host_kind) = HostKind::parse(&req.host_kind) else {
        send_event(
            out_tx,
            hosted_event(
                &name,
                Err(("invalid", format!("unknown host_kind '{}'", req.host_kind))),
            ),
        )
        .await;
        return;
    };
    let mut methods = Vec::with_capacity(req.methods.len());
    for m in &req.methods {
        let Some(mode) = parse_mode(&m.mode) else {
            send_event(
                out_tx,
                hosted_event(
                    &name,
                    Err((
                        "invalid",
                        format!("unknown mode '{}' for method '{}'", m.mode, m.name),
                    )),
                ),
            )
            .await;
            return;
        };
        methods.push(remotemedia_core::transport::endpoint::RpcMethod::new(
            m.name.clone(),
            mode,
        ));
    }
    let spec = EndpointSpec {
        name: name.clone(),
        methods,
        host_kind,
        adapter: (!req.adapter.is_empty()).then(|| req.adapter.clone()),
        queue_capacity: req.queue_capacity as usize,
    };
    // Re-hosting a name this attach already hosts replaces our own slot.
    let own = hosted.map.contains_key(&name);
    if own {
        hosted.release(&name);
    }
    let takeover = req.takeover || own;
    let mut host = match ctrl.host_endpoint(spec, takeover) {
        Ok(h) => h,
        Err(e) => {
            send_event(
                out_tx,
                hosted_event(&name, Err((e.as_str(), e.to_string()))),
            )
            .await;
            return;
        }
    };
    let replier = host.replier();
    // Acknowledge before the forwarder can emit calls for this endpoint.
    send_event(out_tx, hosted_event(&name, Ok(()))).await;
    let tx = out_tx.clone();
    let ev_name = name.clone();
    let task = tokio::spawn(async move {
        while let Some(ev) = host.next().await {
            let event = match ev {
                HostEvent::Call(call) => PbEvent::EndpointCall(PbEndpointCall {
                    name: ev_name.clone(),
                    call_id: call.call_id,
                    caller_node: call.caller_node,
                    session_id: call.session_id,
                    method: call.method,
                    mode: mode_str(call.mode).to_string(),
                    args_json: serde_json::to_string(&call.args).unwrap_or_else(|_| "[]".into()),
                    kwargs_json: serde_json::to_string(&call.kwargs)
                        .unwrap_or_else(|_| "{}".into()),
                    deadline_ms: call.deadline_ms,
                    route_hint: call.route_hint.unwrap_or_default(),
                    adapter_hint: call.adapter_hint.unwrap_or_default(),
                }),
                HostEvent::Cancelled { call_id, reason } => {
                    PbEvent::EndpointCancelled(PbEndpointCancelled {
                        name: ev_name.clone(),
                        call_id,
                        reason: reason.as_str().to_string(),
                    })
                }
            };
            if tx
                .send(Ok(ControlEvent { event: Some(event) }))
                .await
                .is_err()
            {
                return;
            }
        }
        // Registration ended without this attach asking (takeover / close).
        let _ = tx
            .send(Ok(ControlEvent {
                event: Some(PbEvent::EndpointPresence(PbEndpointPresence {
                    name: ev_name.clone(),
                    hosted: false,
                    reason: host.end_reason().unwrap_or("unhosted").to_string(),
                })),
            }))
            .await;
    });
    hosted.map.insert(name, HostedEndpoint { replier, task });
}

async fn endpoint_reply_frame(
    reply: crate::generated::EndpointReply,
    out_tx: &mpsc::Sender<Result<ControlEvent, Status>>,
    hosted: &HostedEndpoints,
) {
    use remotemedia_core::transport::endpoint::{CallErrorKind, CallReplyEvent};
    let Some(h) = hosted.map.get(&reply.name) else {
        send_event(
            out_tx,
            reply_ack(&reply.name, &reply.call_id, Err("responder_gone")),
        )
        .await;
        return;
    };
    let parse_json = |s: &str| -> Result<serde_json::Value, ()> {
        if s.is_empty() {
            Ok(serde_json::Value::Null)
        } else {
            serde_json::from_str(s).map_err(|_| ())
        }
    };
    let event = match reply.kind.as_str() {
        "value" => match parse_json(&reply.data_json) {
            Ok(data) => CallReplyEvent::Value { data },
            Err(()) => {
                send_event(
                    out_tx,
                    reply_ack(&reply.name, &reply.call_id, Err("invalid")),
                )
                .await;
                return;
            }
        },
        "end" => CallReplyEvent::End,
        "error" => CallReplyEvent::Error {
            error: CallErrorKind::parse(&reply.error).unwrap_or(CallErrorKind::Error),
            message: reply.message.clone(),
        },
        _ => {
            send_event(
                out_tx,
                reply_ack(&reply.name, &reply.call_id, Err("invalid")),
            )
            .await;
            return;
        }
    };
    let meta = (!reply.meta_json.is_empty())
        .then(|| serde_json::from_str(&reply.meta_json).ok())
        .flatten();
    let result = h
        .replier
        .send(&reply.call_id, event, meta)
        .map_err(|k| match k {
            CallErrorKind::Timeout => "late",
            CallErrorKind::Cancelled => "cancelled",
            CallErrorKind::NotFound => "unknown_call",
            CallErrorKind::ResponderGone => "responder_gone",
            _ => "invalid",
        });
    send_event(out_tx, reply_ack(&reply.name, &reply.call_id, result)).await;
}

/// Forwarder tasks owned by one attach. Tap forwarders are keyed by
/// `(node_id, port)` so `Unsubscribe` can stop them.
#[derive(Default)]
struct Forwarders {
    taps: HashMap<(String, Option<String>), Vec<tokio::task::JoinHandle<()>>>,
    other: Vec<tokio::task::JoinHandle<()>>,
}

impl Forwarders {
    fn abort_all(&mut self) {
        for (_, ts) in self.taps.drain() {
            for t in ts {
                t.abort();
            }
        }
        for t in self.other.drain(..) {
            t.abort();
        }
    }
}

// Dispatch one inbound frame. Returns Err(ErrorEvent) for a protocol
// error worth surfacing to the client; Ok(()) otherwise.
async fn handle_inbound_frame(
    ctrl: &Arc<SessionControl>,
    frame: ControlFrame,
    out_tx: &mpsc::Sender<Result<ControlEvent, Status>>,
    forwarders: &mut Forwarders,
    rpc_scope: &RpcCallScope,
    hosted: &mut HostedEndpoints,
) -> Result<(), ErrorEvent> {
    match frame.op {
        Some(PbOp::HostEndpoint(req)) => {
            host_endpoint_frame(ctrl, req, out_tx, hosted).await;
            Ok(())
        }

        Some(PbOp::UnhostEndpoint(req)) => {
            hosted.release(&req.name);
            Ok(())
        }

        Some(PbOp::EndpointReply(reply)) => {
            endpoint_reply_frame(reply, out_tx, hosted).await;
            Ok(())
        }

        Some(PbOp::WatchEndpoints(_)) => {
            // Subscribe before listing so no change is missed.
            let mut rx = ctrl.watch_endpoints();
            let initial = ctrl.endpoints();
            let tx = out_tx.clone();
            let task = tokio::spawn(async move {
                for info in initial {
                    let ev = PbEvent::EndpointPresence(PbEndpointPresence {
                        name: info.name,
                        hosted: true,
                        reason: String::new(),
                    });
                    if tx.send(Ok(ControlEvent { event: Some(ev) })).await.is_err() {
                        return;
                    }
                }
                loop {
                    let p = match rx.recv().await {
                        Ok(p) => p,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    };
                    let ev = PbEvent::EndpointPresence(PbEndpointPresence {
                        name: p.name,
                        hosted: p.hosted,
                        reason: String::new(),
                    });
                    if tx.send(Ok(ControlEvent { event: Some(ev) })).await.is_err() {
                        return;
                    }
                }
            });
            forwarders.other.push(task);
            Ok(())
        }

        Some(PbOp::Hello(_)) => Err(error(
            ControlErrorCode::Protocol,
            "Hello may only appear as the first frame",
            None,
        )),

        Some(PbOp::Subscribe(sub)) => {
            let core = pb_addr_to_core(sub.addr, Direction::Out)?;
            let key = (core.node_id.clone(), core.port.clone());
            if core.node_id == SYSTEM_PORT && core.port.is_none() {
                // Replay the retained `__system__` ring first so late
                // observers see init history (matches WS / in-proc paths).
                let mut rx = ctrl.subscribe_system();
                let pb_addr = core_addr_to_pb(&core);
                let tx = out_tx.clone();
                let task = tokio::spawn(async move {
                    while let Some(data) = rx.recv().await {
                        let Some(pb) = runtime_data_to_pb(&data) else {
                            continue;
                        };
                        let event = ControlEvent {
                            event: Some(PbEvent::Tap(TapEvent {
                                addr: Some(pb_addr.clone()),
                                data: Some(pb),
                            })),
                        };
                        if tx.send(Ok(event)).await.is_err() {
                            break;
                        }
                    }
                });
                forwarders.taps.entry(key).or_default().push(task);
                return Ok(());
            }
            let mut rx = ctrl.subscribe(&core).map_err(|e| {
                error(
                    ControlErrorCode::Internal,
                    e.to_string(),
                    Some(core.clone()),
                )
            })?;

            let pb_addr = core_addr_to_pb(&core);
            let tx = out_tx.clone();
            let task = tokio::spawn(async move {
                loop {
                    let data = match rx.recv().await {
                        Ok(data) => data,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(count))
                            if core.port.as_deref() == Some("__reply__") =>
                        {
                            let event = ControlEvent {
                                event: Some(PbEvent::Error(error(
                                    ControlErrorCode::Internal,
                                    format!("RPC reply subscription lagged by {count} frames"),
                                    Some(core.clone()),
                                ))),
                            };
                            let _ = tx.send(Ok(event)).await;
                            break;
                        }
                        // Telemetry / output taps tolerate skipped frames:
                        // keep the subscription alive instead of silently
                        // ending it on the first lag.
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    };
                    let pb = match runtime_data_to_pb(&data) {
                        Some(b) => b,
                        None => continue,
                    };
                    let event = ControlEvent {
                        event: Some(PbEvent::Tap(TapEvent {
                            addr: Some(pb_addr.clone()),
                            data: Some(pb),
                        })),
                    };
                    if tx.send(Ok(event)).await.is_err() {
                        break;
                    }
                }
            });
            forwarders.taps.entry(key).or_default().push(task);
            Ok(())
        }

        Some(PbOp::Unsubscribe(unsub)) => {
            // Stop every forwarder this attach opened for the address.
            let core = pb_addr_to_core(unsub.addr, Direction::Out)?;
            if let Some(tasks) = forwarders.taps.remove(&(core.node_id, core.port)) {
                for t in tasks {
                    t.abort();
                }
            }
            Ok(())
        }

        Some(PbOp::Publish(p)) => {
            let core = pb_addr_to_core(p.addr, Direction::In)?;
            let data = p.data.ok_or_else(|| {
                error(
                    ControlErrorCode::InvalidAddress,
                    "publish missing data",
                    Some(core.clone()),
                )
            })?;
            let rt = pb_to_runtime_data(data)
                .map_err(|e| error(ControlErrorCode::Internal, e, Some(core.clone())))?;
            ctrl.publish_scoped(&core, rt, rpc_scope)
                .await
                .map_err(|e| {
                    error(
                        ControlErrorCode::Internal,
                        e.to_string(),
                        Some(core.clone()),
                    )
                })?;
            Ok(())
        }

        Some(PbOp::Intercept(intercept)) => {
            let core = pb_addr_to_core(intercept.addr, Direction::Out)?;
            let deadline = if intercept.deadline_ms > 0 {
                Some(Duration::from_millis(intercept.deadline_ms as u64))
            } else {
                None
            };
            let mut events = ctrl.intercept(&core, deadline).map_err(|e| {
                error(
                    ControlErrorCode::Internal,
                    e.to_string(),
                    Some(core.clone()),
                )
            })?;

            let pb_addr = core_addr_to_pb(&core);
            let tx = out_tx.clone();
            let task = tokio::spawn(async move {
                while let Some(event) = events.recv().await {
                    if let CoreEvent::InterceptRequest {
                        correlation_id,
                        data,
                        ..
                    } = event
                    {
                        let pb = match runtime_data_to_pb(&data) {
                            Some(b) => b,
                            None => continue,
                        };
                        let ev = ControlEvent {
                            event: Some(PbEvent::InterceptRequest(PbInterceptRequest {
                                addr: Some(pb_addr.clone()),
                                correlation_id,
                                data: Some(pb),
                            })),
                        };
                        if tx.send(Ok(ev)).await.is_err() {
                            break;
                        }
                    }
                }
            });
            forwarders.other.push(task);
            Ok(())
        }

        Some(PbOp::RemoveIntercept(rm)) => {
            let core = pb_addr_to_core(rm.addr, Direction::Out)?;
            ctrl.remove_intercept(&core);
            Ok(())
        }

        Some(PbOp::InterceptReply(reply)) => {
            let decision = pb_decision_to_core(reply.decision)?;
            ctrl.complete_intercept(reply.correlation_id, decision);
            Ok(())
        }

        Some(PbOp::SetNodeState(set)) => {
            let state = pb_node_state_to_core(set.state)
                .map_err(|msg| error(ControlErrorCode::Protocol, msg, None))?;
            ctrl.set_node_state(set.node_id, state);
            Ok(())
        }

        Some(PbOp::ClearNodeState(clear)) => {
            ctrl.clear_node_state(&clear.node_id);
            Ok(())
        }

        Some(PbOp::SetTelemetry(set)) => {
            let Some(tel) = ctrl.telemetry() else {
                return Err(error(
                    ControlErrorCode::Internal,
                    "session has no telemetry (not running on the core router)",
                    None,
                ));
            };
            if !set.level.is_empty() {
                let level = TelemetryLevel::parse(&set.level).ok_or_else(|| {
                    error(
                        ControlErrorCode::Protocol,
                        format!("unknown telemetry level '{}'", set.level),
                        None,
                    )
                })?;
                tel.set_level(level);
            }
            if set.interval_ms > 0 {
                tel.set_interval_ms(set.interval_ms);
            }
            Ok(())
        }

        None => Err(error(
            ControlErrorCode::Protocol,
            "empty ControlFrame (no op set)",
            None,
        )),
    }
}

// ─── Encoding helpers ────────────────────────────────────────────────────────

fn pb_addr_to_core(
    addr: Option<PbAddress>,
    required_dir: Direction,
) -> Result<CoreAddress, ErrorEvent> {
    let addr = addr.ok_or_else(|| {
        error(
            ControlErrorCode::InvalidAddress,
            "missing ControlAddress",
            None,
        )
    })?;
    let dir = match ControlDirection::try_from(addr.direction) {
        Ok(ControlDirection::In) => Direction::In,
        Ok(ControlDirection::Out) => Direction::Out,
        _ => {
            return Err(error(
                ControlErrorCode::InvalidAddress,
                "missing or invalid direction",
                None,
            ));
        }
    };
    if dir != required_dir {
        let err_addr = CoreAddress {
            node_id: addr.node_id.clone(),
            port: if addr.port.is_empty() {
                None
            } else {
                Some(addr.port.clone())
            },
            direction: dir,
        };
        return Err(error(
            ControlErrorCode::InvalidAddress,
            format!("operation requires direction {required_dir:?}, got {dir:?}"),
            Some(err_addr),
        ));
    }
    Ok(CoreAddress {
        node_id: addr.node_id,
        port: if addr.port.is_empty() {
            None
        } else {
            Some(addr.port)
        },
        direction: dir,
    })
}

fn core_addr_to_pb(addr: &CoreAddress) -> PbAddress {
    PbAddress {
        node_id: addr.node_id.clone(),
        port: addr.port.clone().unwrap_or_default(),
        direction: match addr.direction {
            Direction::In => ControlDirection::In as i32,
            Direction::Out => ControlDirection::Out as i32,
        },
    }
}

fn pb_decision_to_core(
    decision: Option<crate::generated::InterceptDecision>,
) -> Result<CoreDecision, ErrorEvent> {
    let decision = decision.ok_or_else(|| {
        error(
            ControlErrorCode::Protocol,
            "InterceptReply missing decision",
            None,
        )
    })?;
    match decision.decision {
        Some(PbDecision::Pass(_)) => Ok(CoreDecision::Pass),
        Some(PbDecision::Drop(_)) => Ok(CoreDecision::Drop),
        Some(PbDecision::Replace(data)) => {
            let rt =
                pb_to_runtime_data(data).map_err(|e| error(ControlErrorCode::Internal, e, None))?;
            Ok(CoreDecision::Replace(rt))
        }
        None => Err(error(
            ControlErrorCode::Protocol,
            "InterceptReply decision variant unset",
            None,
        )),
    }
}

fn pb_node_state_to_core(value: i32) -> Result<NodeState, String> {
    match PbNodeState::try_from(value) {
        Ok(PbNodeState::Enabled) => Ok(NodeState::Enabled),
        Ok(PbNodeState::Bypass) => Ok(NodeState::Bypass),
        Ok(PbNodeState::Disabled) => Ok(NodeState::Disabled),
        _ => Err(format!("unknown NodeState value {value}")),
    }
}

fn pb_to_runtime_data(
    data: crate::generated::DataBuffer,
) -> Result<remotemedia_core::data::RuntimeData, String> {
    crate::adapters::data_buffer_to_runtime_data(&data)
        .ok_or_else(|| "unsupported or unset DataBuffer variant".to_string())
}

fn runtime_data_to_pb(
    rt: &remotemedia_core::data::RuntimeData,
) -> Option<crate::generated::DataBuffer> {
    Some(crate::adapters::runtime_data_to_data_buffer(rt))
}

fn error(
    code: ControlErrorCode,
    message: impl Into<String>,
    addr: Option<CoreAddress>,
) -> ErrorEvent {
    ErrorEvent {
        code: code as i32,
        message: message.into(),
        addr: addr.as_ref().map(core_addr_to_pb),
    }
}

async fn send_event(tx: &mpsc::Sender<Result<ControlEvent, Status>>, event: ControlEvent) {
    let _ = tx.send(Ok(event)).await;
}

/// Serve **only** the `PipelineControl` service for `bus` on `addr` (with
/// gRPC-Web enabled). Lets processes that serve WebRTC / HTTP accept remote
/// observers (openspec: add-live-pipeline-observer). Returns when the server
/// stops; spawn it.
pub async fn serve_control(
    addr: std::net::SocketAddr,
    bus: Arc<SessionControlBus>,
    token: Option<String>,
) -> Result<(), tonic::transport::Error> {
    serve_control_with_incoming(
        tokio_stream::wrappers::TcpListenerStream::new(
            tokio::net::TcpListener::bind(addr)
                .await
                .expect("bind control address"),
        ),
        bus,
        token,
    )
    .await
}

/// Like [`serve_control`] but on a pre-bound listener stream (tests bind
/// port 0 and read the local address first).
pub async fn serve_control_with_incoming(
    incoming: tokio_stream::wrappers::TcpListenerStream,
    bus: Arc<SessionControlBus>,
    token: Option<String>,
) -> Result<(), tonic::transport::Error> {
    let mut svc = ControlServiceImpl::new(bus);
    if let Some(t) = token {
        svc = svc.with_token(t);
    }
    use tonic::service::LayerExt as _;
    let svc = tower::ServiceBuilder::new()
        .layer(tower_http::cors::CorsLayer::permissive())
        .layer(tonic_web::GrpcWebLayer::new())
        .into_inner()
        .named_layer(crate::generated::pipeline_control_server::PipelineControlServer::new(svc));
    tonic::transport::Server::builder()
        .accept_http1(true)
        .tcp_nodelay(true)
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .add_service(svc)
        .serve_with_incoming(incoming)
        .await
}
