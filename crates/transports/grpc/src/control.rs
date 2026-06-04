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
use crate::generated::{
    Attached, CloseReasonCode, ControlAddress as PbAddress, ControlDirection, ControlErrorCode,
    ControlEvent, ControlFrame, ErrorEvent, InterceptRequest as PbInterceptRequest,
    NodeState as PbNodeState, SessionClosed, TapEvent,
};

use remotemedia_core::transport::session_control::{
    CloseReason, ControlAddress as CoreAddress, ControlEvent as CoreEvent, Direction,
    InterceptDecision as CoreDecision, NodeState, SessionControl, SessionControlBus,
};

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
}

impl ControlServiceImpl {
    pub fn new(bus: Arc<SessionControlBus>) -> Self {
        Self { bus }
    }
}

type EventStream = Pin<Box<dyn Stream<Item = Result<ControlEvent, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl PipelineControl for ControlServiceImpl {
    type AttachStream = EventStream;

    async fn attach(
        &self,
        request: Request<Streaming<ControlFrame>>,
    ) -> Result<Response<Self::AttachStream>, Status> {
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
                })),
            },
        )
        .await;

        // Spawn the inbound-frame dispatch + outbound-event forwarding loop.
        let bus = self.bus.clone();
        tokio::spawn(async move {
            run_attach_loop(ctrl, bus, session_id, in_stream, out_tx).await;
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
) {
    // Watch for session-close. First fire ends this attach.
    let mut close_rx = ctrl.close_subscriber();

    // Dedicated forwarder tasks for taps/intercepts. We hold their
    // JoinHandles so we can abort on shutdown.
    let mut forwarders: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    loop {
        tokio::select! {
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

    for t in forwarders {
        t.abort();
    }
    tracing::debug!(session=%session_id, "attach loop exited");
}

// Dispatch one inbound frame. Returns Err(ErrorEvent) for a protocol
// error worth surfacing to the client; Ok(()) otherwise.
async fn handle_inbound_frame(
    ctrl: &Arc<SessionControl>,
    frame: ControlFrame,
    out_tx: &mpsc::Sender<Result<ControlEvent, Status>>,
    forwarders: &mut Vec<tokio::task::JoinHandle<()>>,
) -> Result<(), ErrorEvent> {
    match frame.op {
        Some(PbOp::Hello(_)) => Err(error(
            ControlErrorCode::Protocol,
            "Hello may only appear as the first frame",
            None,
        )),

        Some(PbOp::Subscribe(sub)) => {
            let core = pb_addr_to_core(sub.addr, Direction::Out)?;
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
                while let Ok(data) = rx.recv().await {
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
            forwarders.push(task);
            Ok(())
        }

        Some(PbOp::Unsubscribe(_)) => {
            // The broadcast::Receiver lives inside the forwarder task
            // above; the client can't cancel an individual sub today
            // without closing the whole attach. Acknowledge silently —
            // dropping the attach is the cancellation path.
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
            ctrl.publish(&core, rt).await.map_err(|e| {
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
            forwarders.push(task);
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
