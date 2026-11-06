//! Runtime layout/bound checks for explicitly declared node contracts.
//! Validation borrows buffers; forwarding moves/promotes owners without copying.
//! Allocation lineage, producer binding and live retention budgets remain separate.
use crate::nodes::{InitializeContextRead, NodeRuntimeContextRead, StreamingNode, Tick};
use crate::{data::RuntimeData, Error};
use remotemedia_traits::{
    buffer_contract::{BufferContract, BufferPortContract},
    capabilities::{CapabilityBehavior, MediaCapabilities},
    ports::SnapshotPort,
    rpc::{RpcMethod, RpcRequest, RpcResponder},
    streaming::{AnySessionState, NodeStatus, PacingNature},
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

type Callback = Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>;

struct Checks {
    node_id: String,
    contract: BufferContract,
}
impl Checks {
    fn check(&self, data: &RuntimeData, port: &str, output: bool) -> Result<(), Error> {
        let ports: &BTreeMap<String, BufferPortContract> = if output {
            &self.contract.outputs
        } else {
            &self.contract.inputs
        };
        let validation = match ports.get(port) {
            Some(contract) => contract.validate_payload(data),
            None if matches!(
                data,
                RuntimeData::Text(_)
                    | RuntimeData::Json(_)
                    | RuntimeData::File { .. }
                    | RuntimeData::ControlMessage { .. }
            ) =>
            {
                Ok(())
            }
            None => Err("bulk payload has no exact port contract".into()),
        };
        validation.map_err(|reason| {
            Error::Execution(format!(
                "buffer contract node '{}' {} port '{}': {reason}; no implicit conversion adapter",
                self.node_id,
                if output { "output" } else { "input" },
                if port.is_empty() { "<default>" } else { port },
            ))
        })
    }

    fn callback(self: &Arc<Self>, mut callback: Callback) -> (Callback, CallbackFailure) {
        let checks = self.clone();
        let failure = CallbackFailure::default();
        let captured = failure.clone();
        let guarded = Box::new(move |data: RuntimeData| {
            captured.result(Ok(()))?;
            let result = checks
                .check(&data, "", true)
                .and_then(|()| callback(data.into_shared()));
            if let Err(error) = &result {
                *captured.0.lock().unwrap() = Some(error.to_string());
            }
            result
        });
        (guarded, failure)
    }
}

/// A node may ignore a rejected output callback. Keep that failure for the call
/// result and reject later yields, rather than reporting successful completion.
#[derive(Clone, Default)]
struct CallbackFailure(Arc<Mutex<Option<String>>>);
impl CallbackFailure {
    fn result<T>(&self, result: Result<T, Error>) -> Result<T, Error> {
        if let Some(error) = &*self.0.lock().unwrap() {
            Err(Error::Execution(error.clone()))
        } else {
            result
        }
    }
}

/// Wrap a node after static contract checking. Input validation precedes every
/// media/control invocation; output validation precedes every return/yield.
/// This checks host layouts and sizes, not arbitrary internal copies. Snapshot
/// bindings fail explicitly until their fallible read path can enforce contracts.
pub struct BufferCheckedNode {
    inner: Box<dyn StreamingNode>,
    checks: Arc<Checks>,
}
impl BufferCheckedNode {
    pub fn new(node_id: String, contract: BufferContract, inner: Box<dyn StreamingNode>) -> Self {
        Self {
            inner,
            checks: Arc::new(Checks { node_id, contract }),
        }
    }

    fn output(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        self.checks.check(&data, "", true)?;
        Ok(data.into_shared())
    }
}

#[async_trait::async_trait]
impl StreamingNode for BufferCheckedNode {
    fn node_type(&self) -> &str {
        self.inner.node_type()
    }
    fn node_id(&self) -> &str {
        self.inner.node_id()
    }
    fn get_status(&self) -> NodeStatus {
        self.inner.get_status()
    }
    fn is_multi_input(&self) -> bool {
        self.inner.is_multi_input()
    }

    async fn initialize(&self, ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        self.inner.initialize(ctx).await?;
        if !self.inner.snapshot_outputs().is_empty() {
            return Err(Error::Execution(format!("buffer contract node '{}': snapshot output validation requires a fallible publication/read adapter", self.checks.node_id)));
        }
        Ok(())
    }

    async fn process_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        self.checks.check(&data, "", false)?;
        self.output(self.inner.process_async(data.into_shared(), ctx).await?)
    }

    async fn process_multi_async(
        &self,
        mut inputs: HashMap<String, RuntimeData>,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        for (port, data) in &inputs {
            self.checks.check(data, port, false)?;
        }
        // Move normalization preserves legacy Vec allocations before user fan-out.
        inputs = inputs
            .into_iter()
            .map(|(port, data)| (port, data.into_shared()))
            .collect();
        self.output(self.inner.process_multi_async(inputs, ctx).await?)
    }

    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        callback: Callback,
    ) -> Result<usize, Error> {
        self.checks.check(&data, "", false)?;
        let (callback, failure) = self.checks.callback(callback);
        failure.result(
            self.inner
                .process_streaming_async(data.into_shared(), ctx, callback)
                .await,
        )
    }

    async fn tick(
        &self,
        tick: Tick,
        ctx: &dyn NodeRuntimeContextRead,
        callback: Callback,
    ) -> Result<(), Error> {
        let (callback, failure) = self.checks.callback(callback);
        failure.result(self.inner.tick(tick, ctx, callback).await)
    }

    async fn process_control_message(
        &self,
        message: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<bool, Error> {
        self.checks.check(&message, "", false)?;
        self.inner
            .process_control_message(message.into_shared(), ctx)
            .await
    }

    fn validate_snapshot_input(&self, port: &str, _: &dyn SnapshotPort) -> Result<(), Error> {
        Err(Error::Execution(format!("buffer contract node '{}' input '{port}': snapshot validation requires a fallible read adapter", self.checks.node_id)))
    }
    fn snapshot_outputs(&self) -> HashMap<String, Arc<dyn SnapshotPort>> {
        self.inner.snapshot_outputs()
    }
    fn rpc_methods(&self) -> Vec<RpcMethod> {
        self.inner.rpc_methods()
    }
    async fn call_rpc(
        &self,
        request: RpcRequest,
        ctx: &dyn NodeRuntimeContextRead,
        replies: &mut RpcResponder,
    ) -> Result<(), Error> {
        self.inner.call_rpc(request, ctx, replies).await
    }
    fn pacing_nature(&self) -> PacingNature {
        self.inner.pacing_nature()
    }
    fn capability_subscriptions(&self) -> Vec<String> {
        self.inner.capability_subscriptions()
    }
    fn clocked_media_addr(&self) -> Option<(String, String)> {
        self.inner.clocked_media_addr()
    }
    fn fallback_rate_hz(&self) -> u32 {
        self.inner.fallback_rate_hz()
    }
    fn media_capabilities(&self) -> Option<MediaCapabilities> {
        self.inner.media_capabilities()
    }
    fn capability_behavior(&self) -> CapabilityBehavior {
        self.inner.capability_behavior()
    }
    fn potential_capabilities(&self) -> Option<MediaCapabilities> {
        self.inner.potential_capabilities()
    }
    fn actual_capabilities(&self) -> Option<MediaCapabilities> {
        self.inner.actual_capabilities()
    }
    fn configure_from_upstream(&self, caps: &MediaCapabilities) -> Result<(), Error> {
        self.inner.configure_from_upstream(caps)
    }
    fn make_session_state(&self, ctx: &dyn InitializeContextRead) -> Arc<dyn AnySessionState> {
        self.inner.make_session_state(ctx)
    }
    async fn on_capability_update(
        &self,
        addr: &str,
        caps: MediaCapabilities,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<(), Error> {
        self.inner.on_capability_update(addr, caps, ctx).await
    }
}
