//! Session Control Bus over a WebRTC data channel.
//!
//! Mirrors the gRPC `PipelineControl` handler [`remotemedia_grpc::control`]
//! but speaks over a single reliable+ordered data channel labeled
//! [`CONTROL_CHANNEL_LABEL`]. Wire encoding is the same `ControlFrame` /
//! `ControlEvent` protobuf defined in `proto/control.proto` — a WebRTC
//! peer and a gRPC client see identical bytes on the wire for the same
//! logical operation. That makes the Python client able to run over
//! either transport with the same encoder.
//!
//! Protocol (identical to gRPC):
//!
//!   1. Browser / native peer opens a data channel named
//!      `"remotemedia-control"`.
//!   2. First message is a `ControlFrame` whose `op` is `Hello { session_id }`.
//!   3. Server replies with `ControlEvent::Attached` (or `Error` + closes).
//!   4. Client and server exchange `ControlFrame` / `ControlEvent` until
//!      the session closes or the client detaches.
//!   5. On session close the server emits one `ControlEvent::SessionClosed`
//!      and closes the data channel.
//!
//! All messages are raw protobuf bytes over a **binary** data channel frame.

#[cfg(feature = "grpc-signaling")]
use crate::generated::control_event::Event as PbEvent;
#[cfg(feature = "grpc-signaling")]
use crate::generated::control_frame::Op as PbOp;
#[cfg(feature = "grpc-signaling")]
use crate::generated::intercept_decision::Decision as PbDecision;
#[cfg(feature = "grpc-signaling")]
use crate::generated::{
    Attached, CloseReasonCode, ControlAddress as PbAddress, ControlDirection, ControlErrorCode,
    ControlEvent, ControlFrame, ErrorEvent, InterceptRequest as PbInterceptRequest,
    NodeState as PbNodeState, SessionClosed, TapEvent,
};

#[cfg(feature = "grpc-signaling")]
use prost::Message;
use remotemedia_core::data::RuntimeData;
#[cfg(feature = "grpc-signaling")]
use remotemedia_core::transport::session_control::{
    CloseReason, ControlAddress as CoreAddress, ControlEvent as CoreEvent, Direction,
    InterceptDecision as CoreDecision, NodeState, SessionControl, SessionControlBus,
};

#[cfg(feature = "grpc-signaling")]
use std::sync::Arc;
#[cfg(feature = "grpc-signaling")]
use std::time::Duration;
#[cfg(feature = "grpc-signaling")]
use tokio::sync::{mpsc, Mutex};
#[cfg(feature = "grpc-signaling")]
use webrtc::data_channel::data_channel_message::DataChannelMessage;
#[cfg(feature = "grpc-signaling")]
use webrtc::data_channel::RTCDataChannel;

/// Label the browser / native peer opens to reach the control bus.
pub const CONTROL_CHANNEL_LABEL: &str = "remotemedia-control";

/// Capacity of the outbound event queue between the dispatch loop and
/// the send-to-data-channel serialization loop.
#[cfg(feature = "grpc-signaling")]
const OUTBOUND_EVENT_CAPACITY: usize = 256;

#[cfg(feature = "grpc-signaling")]
fn now_instant() -> std::time::Instant {
    std::time::Instant::now()
}

/// Attach a control-bus handler to a data channel.
///
/// The caller has already accepted a newly-opened incoming data channel
/// whose label matches [`CONTROL_CHANNEL_LABEL`]. This function installs
/// the `on_message` handler for the channel and spawns the forwarder
/// that sends outbound events back to the peer.
///
/// The handler lives for as long as the data channel is open. When the
/// session terminates on the server side, a `SessionClosed` event is
/// emitted and the channel is closed.
#[cfg(feature = "grpc-signaling")]
pub async fn attach_control_channel(
    data_channel: Arc<RTCDataChannel>,
    bus: Arc<SessionControlBus>,
) {
    // Queue for outbound events. Shared by:
    //   - the Hello/ErrorEvent sent before the dispatch loop starts
    //   - the forwarders installed by Subscribe/Intercept
    //   - the dispatch loop itself for error replies
    //   - the SessionClosed sender on shutdown
    let (out_tx, out_rx) = mpsc::channel::<ControlEvent>(OUTBOUND_EVENT_CAPACITY);

    // Spawn the outbound serializer: reads ControlEvents, encodes as
    // protobuf bytes, and writes to the data channel.
    let dc_for_sender = Arc::clone(&data_channel);
    tokio::spawn(async move {
        run_outbound_loop(dc_for_sender, out_rx).await;
    });

    // Gate for "Hello received yet?" — guards all non-Hello frames until
    // the attach is validated, and carries the SessionControl once it is.
    let state: Arc<Mutex<AttachState>> = Arc::new(Mutex::new(AttachState::AwaitingHello));

    // Shared forwarder join-handle registry so we can abort on close.
    let forwarders: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));

    // Install the message handler. Every inbound frame gets decoded and
    // dispatched; the dispatch path short-circuits until Hello.
    let state_cb = Arc::clone(&state);
    let forwarders_cb = Arc::clone(&forwarders);
    let out_tx_cb = out_tx.clone();
    let bus_cb = Arc::clone(&bus);
    let dc_for_msg = Arc::clone(&data_channel);

    data_channel.on_message(Box::new(move |msg: DataChannelMessage| {
        let state = Arc::clone(&state_cb);
        let forwarders = Arc::clone(&forwarders_cb);
        let out_tx = out_tx_cb.clone();
        let bus = Arc::clone(&bus_cb);
        let dc = Arc::clone(&dc_for_msg);
        Box::pin(async move {
            let frame = match ControlFrame::decode(&msg.data[..]) {
                Ok(f) => f,
                Err(e) => {
                    let _ = out_tx
                        .send(error_event(
                            ControlErrorCode::Protocol,
                            format!("invalid ControlFrame: {e}"),
                            None,
                        ))
                        .await;
                    return;
                }
            };

            handle_inbound(frame, &state, &bus, &out_tx, &forwarders, &dc).await;
        })
    }));

    // When the data channel closes, abort the close-watcher task and
    // all subscribe/intercept forwarders so they don't continue
    // sending into a closed channel.
    let state_close = Arc::clone(&state);
    let forwarders_close = Arc::clone(&forwarders);
    data_channel.on_close(Box::new(move || {
        let state_close = Arc::clone(&state_close);
        let forwarders_close = Arc::clone(&forwarders_close);
        Box::pin(async move {
            let mut guard = state_close.lock().await;
            if let AttachState::Attached { close_watcher, .. } = &*guard {
                close_watcher.abort();
            }
            *guard = AttachState::Closed;

            let fw = forwarders_close.lock().await;
            for h in fw.iter() {
                h.abort();
            }
        })
    }));
}

#[cfg(feature = "grpc-signaling")]
enum AttachState {
    AwaitingHello,
    Attached {
        ctrl: Arc<SessionControl>,
        // JoinHandle on the close-watcher task. Aborted when the data channel
        // closes so we don't leave a dangling task trying to send SessionClosed.
        close_watcher: tokio::task::JoinHandle<()>,
    },
    Closed,
}

#[cfg(feature = "grpc-signaling")]
async fn run_outbound_loop(dc: Arc<RTCDataChannel>, mut rx: mpsc::Receiver<ControlEvent>) {
    while let Some(ev) = rx.recv().await {
        let mut buf = Vec::with_capacity(128);
        if let Err(e) = ev.encode(&mut buf) {
            tracing::warn!("control: encode failed: {e}");
            continue;
        }
        if let Err(e) = dc.send(&bytes::Bytes::from(buf)).await {
            tracing::warn!("control: data-channel send failed: {e}");
            break;
        }
    }
    // Receiver closed — best-effort close the data channel.
    let _ = dc.close().await;
}

#[cfg(feature = "grpc-signaling")]
async fn handle_inbound(
    frame: ControlFrame,
    state: &Arc<Mutex<AttachState>>,
    bus: &Arc<SessionControlBus>,
    out_tx: &mpsc::Sender<ControlEvent>,
    forwarders: &Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    dc: &Arc<RTCDataChannel>,
) {
    // Short-circuit Hello vs. everything-else based on current state.
    let mut guard = state.lock().await;
    match &*guard {
        AttachState::Closed => {
            // Ignore stray frames after close.
            return;
        }
        AttachState::AwaitingHello => {
            let hello = match frame.op {
                Some(PbOp::Hello(h)) => h,
                _ => {
                    let _ = out_tx
                        .send(error_event(
                            ControlErrorCode::Protocol,
                            "first frame must be Hello",
                            None,
                        ))
                        .await;
                    return;
                }
            };
            let ctrl = match bus.get(&hello.session_id) {
                Some(c) => c,
                None => {
                    let _ = out_tx
                        .send(error_event(
                            ControlErrorCode::SessionNotFound,
                            format!("session '{}' not found", hello.session_id),
                            None,
                        ))
                        .await;
                    let _ = dc.close().await;
                    *guard = AttachState::Closed;
                    return;
                }
            };

            // Emit Attached and wire up the session-close watcher.
            let _ = out_tx
                .send(ControlEvent {
                    event: Some(PbEvent::Attached(Attached {
                        session_id: hello.session_id.clone(),
                        attach_id: hello.attach_id,
                    })),
                })
                .await;

            let close_tx = out_tx.clone();
            let mut close_rx = ctrl.close_subscriber();
            let dc_for_close = Arc::clone(dc);
            let close_watcher = tokio::spawn(async move {
                let reason = close_rx.recv().await;
                let (code, detail) = match reason {
                    Ok(CloseReason::Normal) => (CloseReasonCode::Normal as i32, String::new()),
                    Ok(CloseReason::Error(msg)) => (CloseReasonCode::Error as i32, msg),
                    Err(_) => (
                        CloseReasonCode::Unspecified as i32,
                        "close channel lag".into(),
                    ),
                };
                let _ = close_tx
                    .send(ControlEvent {
                        event: Some(PbEvent::SessionClosed(SessionClosed {
                            reason: code,
                            detail,
                        })),
                    })
                    .await;
                let _ = dc_for_close.close().await;
            });

            *guard = AttachState::Attached {
                ctrl,
                close_watcher,
            };
            return;
        }
        AttachState::Attached { .. } => {
            // fall through to dispatch
        }
    }

    // Borrow Ctrl out of the state to dispatch.
    let ctrl = match &*guard {
        AttachState::Attached { ctrl, .. } => Arc::clone(ctrl),
        _ => return,
    };
    drop(guard);

    // Reject a second Hello.
    if matches!(frame.op, Some(PbOp::Hello(_))) {
        let _ = out_tx
            .send(error_event(
                ControlErrorCode::Protocol,
                "Hello may only appear as the first frame",
                None,
            ))
            .await;
        return;
    }

    // Dispatch the frame.
    if let Err(err) = dispatch_frame(frame, &ctrl, out_tx, forwarders).await {
        let _ = out_tx
            .send(ControlEvent {
                event: Some(PbEvent::Error(err)),
            })
            .await;
    }

    // Touch `now_instant` so it isn't dead code in release builds.
    let _ = now_instant();
}

#[cfg(feature = "grpc-signaling")]
async fn dispatch_frame(
    frame: ControlFrame,
    ctrl: &Arc<SessionControl>,
    out_tx: &mpsc::Sender<ControlEvent>,
    forwarders: &Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
) -> Result<(), ErrorEvent> {
    match frame.op {
        Some(PbOp::Subscribe(sub)) => {
            let core = pb_addr_to_core(sub.addr, Direction::Out)?;
            let mut rx = ctrl.subscribe(&core).map_err(|e| {
                error_inner(
                    ControlErrorCode::Internal,
                    e.to_string(),
                    Some(core.clone()),
                )
            })?;
            let pb_addr = core_addr_to_pb(&core);
            let tx = out_tx.clone();
            let task = tokio::spawn(async move {
                while let Ok(data) = rx.recv().await {
                    let buf = crate::adapters::runtime_data_to_data_buffer(&data);
                    let _ = tx
                        .send(ControlEvent {
                            event: Some(PbEvent::Tap(TapEvent {
                                addr: Some(pb_addr.clone()),
                                data: Some(buf),
                            })),
                        })
                        .await;
                }
            });
            forwarders.lock().await.push(task);
            Ok(())
        }

        Some(PbOp::Unsubscribe(_)) => Ok(()), // drop the per-sub forwarder lazily

        Some(PbOp::Publish(p)) => {
            let core = pb_addr_to_core(p.addr, Direction::In)?;
            let data = p.data.ok_or_else(|| {
                error_inner(
                    ControlErrorCode::InvalidAddress,
                    "publish missing data",
                    Some(core.clone()),
                )
            })?;
            let rt = pb_to_runtime_data(data)
                .map_err(|e| error_inner(ControlErrorCode::Internal, e, Some(core.clone())))?;
            ctrl.publish(&core, rt).await.map_err(|e| {
                error_inner(
                    ControlErrorCode::Internal,
                    e.to_string(),
                    Some(core.clone()),
                )
            })?;
            Ok(())
        }

        Some(PbOp::Intercept(i)) => {
            let core = pb_addr_to_core(i.addr, Direction::Out)?;
            let deadline = if i.deadline_ms > 0 {
                Some(Duration::from_millis(i.deadline_ms as u64))
            } else {
                None
            };
            let mut events = ctrl.intercept(&core, deadline).map_err(|e| {
                error_inner(
                    ControlErrorCode::Internal,
                    e.to_string(),
                    Some(core.clone()),
                )
            })?;
            let pb_addr = core_addr_to_pb(&core);
            let tx = out_tx.clone();
            let task = tokio::spawn(async move {
                while let Some(ev) = events.recv().await {
                    if let CoreEvent::InterceptRequest {
                        correlation_id,
                        data,
                        ..
                    } = ev
                    {
                        let buf = crate::adapters::runtime_data_to_data_buffer(&data);
                        let _ = tx
                            .send(ControlEvent {
                                event: Some(PbEvent::InterceptRequest(PbInterceptRequest {
                                    addr: Some(pb_addr.clone()),
                                    correlation_id,
                                    data: Some(buf),
                                })),
                            })
                            .await;
                    }
                }
            });
            forwarders.lock().await.push(task);
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
                .map_err(|msg| error_inner(ControlErrorCode::Protocol, msg, None))?;
            ctrl.set_node_state(set.node_id, state);
            Ok(())
        }

        Some(PbOp::ClearNodeState(c)) => {
            ctrl.clear_node_state(&c.node_id);
            Ok(())
        }

        Some(PbOp::Hello(_)) | None => Err(error_inner(
            ControlErrorCode::Protocol,
            "unexpected frame",
            None,
        )),
    }
}

// ─── Encoding helpers (shared with gRPC, re-implemented here to keep
//     webrtc crate independent of remotemedia-grpc) ───────────────────────────

#[cfg(feature = "grpc-signaling")]
fn pb_addr_to_core(
    addr: Option<PbAddress>,
    required_dir: Direction,
) -> Result<CoreAddress, ErrorEvent> {
    let addr = addr.ok_or_else(|| {
        error_inner(
            ControlErrorCode::InvalidAddress,
            "missing ControlAddress",
            None,
        )
    })?;
    let dir = match ControlDirection::try_from(addr.direction) {
        Ok(ControlDirection::In) => Direction::In,
        Ok(ControlDirection::Out) => Direction::Out,
        _ => {
            return Err(error_inner(
                ControlErrorCode::InvalidAddress,
                "missing or invalid direction",
                None,
            ));
        }
    };
    if dir != required_dir {
        return Err(error_inner(
            ControlErrorCode::InvalidAddress,
            format!("operation requires direction {required_dir:?}, got {dir:?}"),
            Some(CoreAddress {
                node_id: addr.node_id.clone(),
                port: if addr.port.is_empty() {
                    None
                } else {
                    Some(addr.port.clone())
                },
                direction: dir,
            }),
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

#[cfg(feature = "grpc-signaling")]
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

#[cfg(feature = "grpc-signaling")]
fn pb_decision_to_core(
    decision: Option<crate::generated::InterceptDecision>,
) -> Result<CoreDecision, ErrorEvent> {
    let decision = decision.ok_or_else(|| {
        error_inner(
            ControlErrorCode::Protocol,
            "InterceptReply missing decision",
            None,
        )
    })?;
    match decision.decision {
        Some(PbDecision::Pass(_)) => Ok(CoreDecision::Pass),
        Some(PbDecision::Drop(_)) => Ok(CoreDecision::Drop),
        Some(PbDecision::Replace(data)) => {
            let rt = pb_to_runtime_data(data)
                .map_err(|e| error_inner(ControlErrorCode::Internal, e, None))?;
            Ok(CoreDecision::Replace(rt))
        }
        None => Err(error_inner(
            ControlErrorCode::Protocol,
            "InterceptReply decision unset",
            None,
        )),
    }
}

#[cfg(feature = "grpc-signaling")]
fn pb_node_state_to_core(value: i32) -> Result<NodeState, String> {
    match PbNodeState::try_from(value) {
        Ok(PbNodeState::Enabled) => Ok(NodeState::Enabled),
        Ok(PbNodeState::Bypass) => Ok(NodeState::Bypass),
        Ok(PbNodeState::Disabled) => Ok(NodeState::Disabled),
        _ => Err(format!("unknown NodeState value {value}")),
    }
}

#[cfg(feature = "grpc-signaling")]
fn pb_to_runtime_data(data: crate::generated::DataBuffer) -> Result<RuntimeData, String> {
    crate::adapters::data_buffer_to_runtime_data(&data)
        .ok_or_else(|| "unsupported or unset DataBuffer variant".to_string())
}

#[cfg(feature = "grpc-signaling")]
fn error_event(
    code: ControlErrorCode,
    message: impl Into<String>,
    addr: Option<CoreAddress>,
) -> ControlEvent {
    ControlEvent {
        event: Some(PbEvent::Error(error_inner(code, message, addr))),
    }
}

#[cfg(feature = "grpc-signaling")]
fn error_inner(
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
