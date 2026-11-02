//! Dedicated native RPC lane, independent of media ingress and fanout.

use super::session_control::{wrap_aux_port, RpcCaller, RpcInvocation, SessionControl};
use crate::{
    data::RuntimeData,
    nodes::{NodeRuntimeContext, StreamingNode},
    Error,
};
use futures::FutureExt;
use remotemedia_traits::rpc::{
    RpcId, RpcMethod, RpcMode, RpcReply, RpcReplySink, RpcRequest, RpcResponder,
};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};

const MAX_CALLS: usize = 32;

struct ActiveCall {
    method: String,
    caller_id: Option<u64>,
    abort: tokio::task::AbortHandle,
}

async fn caller_closed(caller: &mut Option<RpcCaller>) {
    if let Some(caller) = caller {
        if !*caller.closed.borrow() {
            let _ = caller.closed.changed().await;
        }
    } else {
        std::future::pending::<()>().await;
    }
}

struct BusReplies {
    control: Arc<SessionControl>,
    node_id: String,
}

#[async_trait::async_trait]
impl RpcReplySink for BusReplies {
    async fn send(&self, reply: RpcReply) -> Result<(), Error> {
        let data = serde_json::to_value(reply).map_err(|e| Error::Execution(e.to_string()))?;
        self.control.publish_rpc_reply(
            &self.node_id,
            wrap_aux_port("__reply__", RuntimeData::Json(data)),
        )
    }
}

/// Runs until input/session closure. Dropping this future aborts all child calls.
pub(super) async fn run(
    node: Arc<dyn StreamingNode>,
    ctx: NodeRuntimeContext,
    mut rx: mpsc::Receiver<RpcInvocation>,
    mut shutdown: watch::Receiver<bool>,
) {
    let methods = node.rpc_methods();
    let sink: Arc<dyn RpcReplySink> = Arc::new(BusReplies {
        control: ctx.control.clone(),
        node_id: ctx.node_id().to_string(),
    });
    let mut tasks = JoinSet::new();
    let mut active = HashMap::<RpcId, ActiveCall>::new();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {
                active.retain(|_, call| !call.abort.is_finished());
            },
            input = rx.recv() => {
                let Some(RpcInvocation { method, data, mut caller }) = input else { break };
                if caller.as_ref().is_some_and(|c| *c.closed.borrow()) { continue; }
                let caller_id = caller.as_ref().map(|c| c.id);
                active.retain(|_, call| !call.abort.is_finished());
                let RuntimeData::Json(mut payload) = data else { continue };
                let Some(object) = payload.as_object_mut() else { continue };
                let id = object.get("corr_id").cloned()
                    .and_then(|id| serde_json::from_value::<RpcId>(id).ok());
                let Some(id) = id else { continue };
                if let Some(method) = method.strip_suffix(".__cancel__") {
                    if active.get(&id).is_some_and(|call| call.method == method && call.caller_id == caller_id) {
                        if let Some(call) = active.remove(&id) { call.abort.abort(); }
                    }
                    continue;
                }
                object.insert("method".into(), method.clone().into());
                let mode = methods.iter().find(|m| m.name == method).map(|m| m.mode).unwrap_or(RpcMode::Reply);
                let mut replies = RpcResponder::new(sink.clone(), id.clone(), mode);
                let key = id;
                if active.contains_key(&key) {
                    // A second terminal reply would corrupt the original invocation.
                    tracing::warn!(method, "duplicate in-flight RPC correlation rejected");
                    continue;
                }
                if active.len() >= MAX_CALLS {
                    let _ = replies.finish(Err(Error::Execution("RPC concurrency limit reached".into()))).await;
                    continue;
                }
                let request = match serde_json::from_value::<RpcRequest>(payload) {
                    Ok(request) => request,
                    Err(error) => {
                        let _ = replies.finish(Err(Error::Execution(format!("invalid RPC request: {error}")))).await;
                        continue;
                    }
                };
                let node = node.clone();
                let ctx = ctx.clone();
                let methods = methods.clone();
                let handle = tasks.spawn(async move {
                    let result = tokio::select! {
                        biased;
                        _ = caller_closed(&mut caller) => return,
                        result = std::panic::AssertUnwindSafe(dispatch(&*node, &ctx, request, &methods, &mut replies)).catch_unwind() => {
                            result.unwrap_or_else(|_| Err(Error::Execution("RPC handler panicked".into())))
                        }
                    };
                    if let Err(error) = replies.finish(result).await {
                        tracing::debug!(%error, "RPC delivery/handler failed");
                    }
                });
                active.insert(key, ActiveCall { method, caller_id, abort: handle });
            }
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

async fn dispatch(
    node: &dyn StreamingNode,
    ctx: &NodeRuntimeContext,
    request: RpcRequest,
    methods: &[RpcMethod],
    replies: &mut RpcResponder,
) -> Result<(), Error> {
    if request.method == "__rpc_meta__" {
        let metadata: std::collections::BTreeMap<_, _> =
            methods.iter().map(|m| (&m.name, m.mode)).collect();
        return replies.value(metadata).await;
    }
    if !methods.iter().any(|m| m.name == request.method) {
        return Err(Error::Execution(format!(
            "unknown RPC method: {}",
            request.method
        )));
    }
    node.call_rpc(request, ctx, replies).await
}

/// Abort-on-drop prevents a cancelled main node task detaching its RPC lane.
pub(super) struct RpcTask(pub tokio::task::JoinHandle<()>);
impl Drop for RpcTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}
