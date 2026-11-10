//! Router side of the node-initiated endpoint call convention
//! (openspec `add-agent-harness-integration`, design Decision 12).
//!
//! A node emits `{"__aux_port__":"__call__","payload":<CallRequestEnvelope>}`
//! on its normal output. For nodes that opted in, the session router turns
//! the envelope into an endpoint call **before** taps / intercepts / fanout
//! (so it never reaches downstream nodes or observers) and delivers each
//! reply chunk back to the node:
//!
//! | Node                                   | Mode     | Reply delivery |
//! |----------------------------------------|----------|----------------|
//! | declares `__call_reply__` (RPC lane)   | `Lane`   | `__call_reply__` invoked on the node's RPC lane (serial, independent of media input) |
//! | anything else                          | `Legacy` | none — envelope forwarded unchanged (today's behavior) |
//!
//! Lane-less nodes are deliberately *not* opted in: a passthrough node
//! (or a bypassed one) forwards upstream frames as its own outputs, so
//! converting envelopes on its output would hijack another node's legacy
//! traffic. Python multiprocess nodes and Python source plugins get a
//! call-reply-only lane from the runtime instead.

use super::session_control::{aux_port_of, SessionControl};
use crate::data::RuntimeData;
use remotemedia_traits::call::{
    CallErrorKind, CallReplyEnvelope, CallRequestEnvelope, CALL_AUX_PORT, CALL_REPLY_METHOD,
};
use remotemedia_traits::rpc::RpcMethod;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CallMode {
    /// `__call__` envelopes keep today's semantics.
    Legacy,
    /// Replies through the node's native RPC lane.
    Lane,
}

pub(super) fn call_mode(methods: &[RpcMethod], has_control: bool) -> CallMode {
    if has_control && methods.iter().any(|m| m.name == CALL_REPLY_METHOD) {
        CallMode::Lane
    } else {
        CallMode::Legacy
    }
}

/// A `__call__` envelope that did not parse: the caller's `corr_id` (when
/// recoverable) and the reason.
pub(super) struct BadCall {
    corr_id: Option<String>,
    reason: String,
}

/// `Some` when `data` is a `__call__` aux envelope (JSON, or JSON text as
/// emitted by Python runners, optionally channel-tagged).
pub(super) fn parse_call_envelope(
    data: &RuntimeData,
) -> Option<Result<CallRequestEnvelope, BadCall>> {
    if aux_port_of(data).as_deref() != Some(CALL_AUX_PORT) {
        return None;
    }
    let value = match data {
        RuntimeData::Json(v) => v.clone(),
        RuntimeData::Text(s) => {
            let body = crate::data::text_channel::split_text_str(s).1;
            serde_json::from_str(body.trim_start()).ok()?
        }
        _ => return None,
    };
    let parsed = CallRequestEnvelope::from_aux_json(&value)?;
    Some(parsed.map_err(|e| {
        BadCall {
            corr_id: value
                .get("payload")
                .and_then(|p| p.get("corr_id"))
                .and_then(|c| c.as_str())
                .map(str::to_owned),
            reason: e.to_string(),
        }
    }))
}

/// Envelope-originated calls in flight for one node (by caller `corr_id`).
pub(super) type EnvelopeCalls = Arc<parking_lot::Mutex<HashSet<String>>>;

/// Reply-forwarding tasks of one node. Dropping aborts them, which drops
/// their call streams and therefore cancels the calls at the host.
#[derive(Default)]
pub(super) struct CallTasks(Vec<tokio::task::AbortHandle>);

impl Drop for CallTasks {
    fn drop(&mut self) {
        for t in self.0.drain(..) {
            t.abort();
        }
    }
}

async fn deliver(
    ctrl: &SessionControl,
    node_id: &str,
    mode: CallMode,
    env: CallReplyEnvelope,
) -> bool {
    match mode {
        CallMode::Lane => ctrl.deliver_call_reply(node_id, env).await,
        CallMode::Legacy => false,
    }
}

/// Convert one `__call__` envelope emitted by `node_id` into an endpoint
/// call (or a cancellation) and forward its replies to the node.
pub(super) fn start_envelope_call(
    ctrl: &Arc<SessionControl>,
    node_id: &str,
    parsed: Result<CallRequestEnvelope, BadCall>,
    mode: CallMode,
    calls: &EnvelopeCalls,
    tasks: &mut CallTasks,
) {
    tasks.0.retain(|t| !t.is_finished());
    let req = match parsed {
        Ok(req) => req,
        Err(bad) => {
            tracing::warn!(node = node_id, reason = %bad.reason, "invalid __call__ envelope dropped");
            if let Some(corr) = bad.corr_id {
                let ctrl = ctrl.clone();
                let node = node_id.to_string();
                let env = CallReplyEnvelope::error(
                    corr,
                    CallErrorKind::Error,
                    format!("invalid __call__ envelope: {}", bad.reason),
                );
                tasks.0.push(
                    tokio::spawn(async move {
                        deliver(&ctrl, &node, mode, env).await;
                    })
                    .abort_handle(),
                );
            }
            return;
        }
    };
    if req.cancel {
        calls.lock().remove(&req.corr_id);
        ctrl.cancel_endpoint_call(node_id, &req.corr_id);
        return;
    }
    let corr = req.corr_id.clone();
    calls.lock().insert(corr.clone());
    let mut stream = ctrl.call_endpoint(node_id, req);
    let ctrl = ctrl.clone();
    let node = node_id.to_string();
    let calls = calls.clone();
    tasks.0.push(
        tokio::spawn(async move {
            while let Some(env) = stream.recv().await {
                let terminal = env.is_terminal();
                if !deliver(&ctrl, &node, mode, env).await {
                    tracing::debug!(node = %node, corr_id = %corr, "endpoint reply could not be delivered");
                }
                if terminal {
                    break;
                }
            }
            calls.lock().remove(&corr);
        })
        .abort_handle(),
    );
}

/// Barge-in / cancellation of the node's in-flight processing: cancel every
/// envelope-originated call it has in flight (hosts receive
/// `Cancelled { reason: caller }`; late replies are counted).
pub(super) fn cancel_envelope_calls(ctrl: &SessionControl, node_id: &str, calls: &EnvelopeCalls) {
    let corrs: Vec<String> = calls.lock().drain().collect();
    for corr in corrs {
        ctrl.cancel_endpoint_call(node_id, &corr);
    }
}
