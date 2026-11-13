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
    InterceptDecision as CoreDecision, NodeState, RpcCallScope, SessionControl, SessionControlBus,
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

pub mod access;
pub use access::{ControlAccess, ControlBinding, ControlChannelPolicy, ControlGrant, ControlOp, TapScope};

#[cfg(feature = "grpc-signaling")]
mod output;
#[cfg(feature = "grpc-signaling")]
use output::Outbound;

#[cfg(all(test, feature = "grpc-signaling"))]
#[path = "control/policy_tests.rs"]
mod policy_tests;

#[cfg(all(test, feature = "grpc-signaling"))]
#[path = "control/bound_tests.rs"]
mod bound_tests;

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
    attach(data_channel, Target::Bus(bus)).await;
}

/// Attach a control handler bound to one session and one member.
///
/// The channel reaches `ctrl` and nothing else: the session id in the
/// peer's `Hello` is ignored, so it cannot select another session. Every
/// frame is checked against `binding.access` and refused with an
/// `UNAUTHORIZED` error when not covered. What the member publishes is
/// attributed to `binding.member`, and a tap granted for the member's own
/// frames delivers only output produced from its input.
#[cfg(feature = "grpc-signaling")]
pub async fn attach_bound_control_channel(
    data_channel: Arc<RTCDataChannel>,
    ctrl: Arc<SessionControl>,
    binding: ControlBinding,
) {
    attach(data_channel, Target::Bound { ctrl, binding }).await;
}

/// Refuse a control channel: once it is open the peer receives one
/// `UNAUTHORIZED` error that says why, and the channel is closed.
#[cfg(feature = "grpc-signaling")]
pub async fn refuse_control_channel(data_channel: Arc<RTCDataChannel>, reason: &str) {
    let refusal = error_event(ControlErrorCode::Unauthorized, reason, None)
        .encode()
        .expect("metadata error event");
    let dc = Arc::clone(&data_channel);
    data_channel.on_open(Box::new(move || {
        Box::pin(async move {
            let _ = dc.send(&refusal).await;
            let _ = dc.close().await;
        })
    }));
}

/// What a control channel may reach.
#[cfg(feature = "grpc-signaling")]
enum Target {
    /// Any session on the bus, named by the peer's `Hello`.
    Bus(Arc<SessionControlBus>),
    /// One session, with the member's access.
    Bound {
        ctrl: Arc<SessionControl>,
        binding: ControlBinding,
    },
}

#[cfg(feature = "grpc-signaling")]
impl Target {
    fn binding(&self) -> Option<&ControlBinding> {
        match self {
            Target::Bus(_) => None,
            Target::Bound { binding, .. } => Some(binding),
        }
    }
}

#[cfg(feature = "grpc-signaling")]
async fn attach(data_channel: Arc<RTCDataChannel>, target: Target) {
    let target = Arc::new(target);
    // Queue for outbound events. Shared by:
    //   - the Hello/ErrorEvent sent before the dispatch loop starts
    //   - the forwarders installed by Subscribe/Intercept
    //   - the dispatch loop itself for error replies
    //   - the SessionClosed sender on shutdown
    let (out_tx, out_rx) = mpsc::channel::<Outbound>(OUTBOUND_EVENT_CAPACITY);

    // Serialize owning events directly into their final protobuf allocation,
    // then write it to the data channel.
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
    let target_cb = Arc::clone(&target);
    let dc_for_msg = Arc::clone(&data_channel);

    data_channel.on_message(Box::new(move |msg: DataChannelMessage| {
        let state = Arc::clone(&state_cb);
        let forwarders = Arc::clone(&forwarders_cb);
        let out_tx = out_tx_cb.clone();
        let target = Arc::clone(&target_cb);
        let dc = Arc::clone(&dc_for_msg);
        Box::pin(async move {
            let frame = match ControlFrame::decode(msg.data) {
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

            handle_inbound(frame, &state, &target, &out_tx, &forwarders, &dc).await;
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
            if let AttachState::Attached {
                close_watcher,
                rpc_scope,
                ..
            } = &*guard
            {
                rpc_scope.close();
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
        rpc_scope: Arc<RpcCallScope>,
        // JoinHandle on the close-watcher task. Aborted when the data channel
        // closes so we don't leave a dangling task trying to send SessionClosed.
        close_watcher: tokio::task::JoinHandle<()>,
    },
    Closed,
}

#[cfg(feature = "grpc-signaling")]
async fn run_outbound_loop(dc: Arc<RTCDataChannel>, mut rx: mpsc::Receiver<Outbound>) {
    while let Some(ev) = rx.recv().await {
        let buf = match ev.encode() {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::warn!("control: encode rejected: {error}");
                error_event(ControlErrorCode::Internal, error.to_string(), None)
                    .encode()
                    .expect("metadata error event")
            }
        };
        if let Err(e) = dc.send(&buf).await {
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
    target: &Target,
    out_tx: &mpsc::Sender<Outbound>,
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
            // A bound channel reaches its own session whatever the peer names.
            let found = match target {
                Target::Bus(bus) => bus.get(&hello.session_id),
                Target::Bound { ctrl, .. } => Some(Arc::clone(ctrl)),
            };
            let ctrl = match found {
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
                .send(Outbound::Metadata(ControlEvent {
                    event: Some(PbEvent::Attached(Attached {
                        session_id: match target {
                            Target::Bus(_) => hello.session_id.clone(),
                            Target::Bound { ctrl, .. } => ctrl.session_id().to_string(),
                        },
                        attach_id: hello.attach_id,
                        observer_schema_version:
                            remotemedia_core::telemetry::SCHEMA_VERSION as u32,
                    })),
                }))
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
                    .send(Outbound::Metadata(ControlEvent {
                        event: Some(PbEvent::SessionClosed(SessionClosed {
                            reason: code,
                            detail,
                        })),
                    }))
                    .await;
                let _ = dc_for_close.close().await;
            });

            *guard = AttachState::Attached {
                ctrl,
                rpc_scope: Arc::new(RpcCallScope::default()),
                close_watcher,
            };
            return;
        }
        AttachState::Attached { .. } => {
            // fall through to dispatch
        }
    }

    // Borrow Ctrl out of the state to dispatch.
    let (ctrl, rpc_scope) = match &*guard {
        AttachState::Attached {
            ctrl, rpc_scope, ..
        } => (Arc::clone(ctrl), Arc::clone(rpc_scope)),
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
    if let Err(err) =
        dispatch_frame(frame, &ctrl, out_tx, forwarders, &rpc_scope, target.binding()).await
    {
        let _ = out_tx
            .send(Outbound::Metadata(ControlEvent {
                event: Some(PbEvent::Error(err)),
            }))
            .await;
    }

    // Touch `now_instant` so it isn't dead code in release builds.
    let _ = now_instant();
}

#[cfg(feature = "grpc-signaling")]
async fn dispatch_frame(
    frame: ControlFrame,
    ctrl: &Arc<SessionControl>,
    out_tx: &mpsc::Sender<Outbound>,
    forwarders: &Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    rpc_scope: &RpcCallScope,
    binding: Option<&ControlBinding>,
) -> Result<(), ErrorEvent> {
    // A refusal names the operation and the node.
    let refuse = |message: String, addr: Option<CoreAddress>| {
        error_inner(ControlErrorCode::Unauthorized, message, addr)
    };
    match frame.op {
        Some(PbOp::Subscribe(sub)) => {
            let core = pb_addr_to_core(sub.addr, Direction::Out)?;
            if let Some(binding) = binding {
                let scope = binding
                    .access
                    .tap(&core.node_id, core.port.as_deref())
                    .map_err(|message| refuse(message, Some(core.clone())))?;
                if scope == TapScope::Own {
                    let rx = ctrl.subscribe_attributed(&core).map_err(|e| {
                        error_inner(ControlErrorCode::Internal, e.to_string(), Some(core.clone()))
                    })?;
                    let task = tokio::spawn(output::forward_own_taps(
                        rx,
                        out_tx.clone(),
                        core,
                        ctrl.transport_copy_context(),
                        binding.member.clone(),
                    ));
                    forwarders.lock().await.push(task);
                    return Ok(());
                }
            }
            let rx = ctrl.subscribe(&core).map_err(|e| {
                error_inner(
                    ControlErrorCode::Internal,
                    e.to_string(),
                    Some(core.clone()),
                )
            })?;
            let task = tokio::spawn(output::forward_taps(
                rx, out_tx.clone(), core, ctrl.transport_copy_context(),
            ));
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
            let rt = pb_to_runtime_data(data, &ctrl.transport_copy_context())
                .map_err(|e| error_inner(ControlErrorCode::Internal, e, Some(core.clone())))?;
            let published = match binding {
                Some(binding) => {
                    let rpc_shaped = core.port.is_some()
                        && matches!(&rt, RuntimeData::Json(value) if value.get("corr_id").is_some());
                    binding
                        .access
                        .publish(&core.node_id, core.port.as_deref(), rpc_shaped)
                        .map_err(|message| refuse(message, Some(core.clone())))?;
                    // Attributed to the admitted member, whatever the frame says.
                    let metadata = remotemedia_core::transport::Participant::new(
                        binding.member.clone(),
                        remotemedia_core::transport::data::participant::role::CLIENT,
                    )
                    .frame_metadata();
                    ctrl.publish_scoped_with_metadata(&core, rt, rpc_scope, metadata).await
                }
                None => ctrl.publish_scoped(&core, rt, rpc_scope).await,
            };
            published
                .map_err(|e| {
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
            if let Some(binding) = binding {
                binding
                    .access
                    .intercept(&core.node_id)
                    .map_err(|message| refuse(message, Some(core.clone())))?;
            }
            let deadline = if i.deadline_ms > 0 {
                Some(Duration::from_millis(i.deadline_ms as u64))
            } else {
                None
            };
            let events = ctrl.intercept(&core, deadline).map_err(|e| {
                error_inner(
                    ControlErrorCode::Internal,
                    e.to_string(),
                    Some(core.clone()),
                )
            })?;
            let task = tokio::spawn(output::forward_intercepts(
                events, out_tx.clone(), core_addr_to_pb(&core), ctrl.transport_copy_context(),
            ));
            forwarders.lock().await.push(task);
            Ok(())
        }

        Some(PbOp::RemoveIntercept(rm)) => {
            let core = pb_addr_to_core(rm.addr, Direction::Out)?;
            if let Some(binding) = binding {
                binding
                    .access
                    .intercept(&core.node_id)
                    .map_err(|message| refuse(message, Some(core.clone())))?;
            }
            ctrl.remove_intercept(&core);
            Ok(())
        }

        Some(PbOp::InterceptReply(reply)) => {
            if let Some(binding) = binding {
                binding
                    .access
                    .intercept_reply()
                    .map_err(|message| refuse(message, None))?;
            }
            let decision = pb_decision_to_core(reply.decision, &ctrl.transport_copy_context())?;
            ctrl.complete_intercept(reply.correlation_id, decision)
                .map_err(|e| error_inner(ControlErrorCode::Internal, e.to_string(), None))
        }

        Some(PbOp::SetNodeState(set)) => {
            if let Some(binding) = binding {
                binding
                    .access
                    .node_state(&set.node_id)
                    .map_err(|message| refuse(message, None))?;
            }
            let state = pb_node_state_to_core(set.state)
                .map_err(|msg| error_inner(ControlErrorCode::Protocol, msg, None))?;
            ctrl.set_node_state(set.node_id, state);
            Ok(())
        }

        Some(PbOp::ClearNodeState(c)) => {
            if let Some(binding) = binding {
                binding
                    .access
                    .node_state(&c.node_id)
                    .map_err(|message| refuse(message, None))?;
            }
            ctrl.clear_node_state(&c.node_id);
            Ok(())
        }

        Some(PbOp::SetTelemetry(set)) => {
            if let Some(binding) = binding {
                binding
                    .access
                    .session_wide("telemetry")
                    .map_err(|message| refuse(message, None))?;
            }
            let Some(tel) = ctrl.telemetry() else {
                return Err(error_inner(
                    ControlErrorCode::Internal,
                    "session has no telemetry".to_string(),
                    None,
                ));
            };
            if !set.level.is_empty() {
                let Some(level) = remotemedia_core::telemetry::TelemetryLevel::parse(&set.level)
                else {
                    return Err(error_inner(
                        ControlErrorCode::Protocol,
                        format!("unknown telemetry level '{}'", set.level),
                        None,
                    ));
                };
                tel.set_level(level);
            }
            if set.interval_ms > 0 {
                tel.set_interval_ms(set.interval_ms);
            }
            Ok(())
        }

        // Client-hosted endpoints (add-agent-harness-integration) are served by
        // gRPC `PipelineControl.Attach`; the WebRTC control channel doesn't carry
        // the hosting/presence streams. Refuse explicitly instead of dropping.
        Some(PbOp::HostEndpoint(_))
        | Some(PbOp::UnhostEndpoint(_))
        | Some(PbOp::EndpointReply(_))
        | Some(PbOp::WatchEndpoints(_)) => Err(error_inner(
            ControlErrorCode::Protocol,
            "endpoint hosting is not supported on the WebRTC control channel; \
             use gRPC PipelineControl.Attach",
            None,
        )),

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
    copies: &remotemedia_types::CopyContext,
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
            let rt = pb_to_runtime_data(data, copies)
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
fn pb_to_runtime_data(
    data: crate::generated::DataBuffer,
    copies: &remotemedia_types::CopyContext,
) -> Result<RuntimeData, String> {
    crate::adapters::data_buffer_into_runtime_data_with_context(data, copies)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "unsupported or unset DataBuffer variant".to_string())
}

#[cfg(all(test, feature = "grpc-signaling"))]
mod buffer_owner_tests {
    use super::*;

    #[test]
    fn control_decode_adopts_the_wire_buffers_owned_allocation() {
        let bytes = vec![9; 16];
        let pointer = bytes.as_ptr();
        let buffer = crate::generated::DataBuffer {
            data_type: Some(crate::generated::data_buffer::DataType::Binary(
                crate::generated::BinaryBuffer {
                    data: bytes.into(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let RuntimeData::Binary(owner) = pb_to_runtime_data(buffer, remotemedia_types::CopyContext::legacy()).unwrap() else {
            unreachable!()
        };
        assert_eq!(owner.as_ptr(), pointer);
        assert_eq!(&*owner, &[9; 16]);
    }
}

#[cfg(feature = "grpc-signaling")]
fn error_event(
    code: ControlErrorCode,
    message: impl Into<String>,
    addr: Option<CoreAddress>,
) -> Outbound {
    ControlEvent {
        event: Some(PbEvent::Error(error_inner(code, message, addr))),
    }
    .into()
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
