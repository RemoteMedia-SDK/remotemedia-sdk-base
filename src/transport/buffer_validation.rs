//! Runtime layout/bound checks for explicitly declared node contracts.
//! Validation borrows buffers; forwarding moves/promotes owners without copying.
//! Host provenance is checked; live retention budgets remain separate.
use crate::nodes::{InitializeContextRead, NodeRuntimeContextRead, StreamingNode, Tick};
use crate::{data::RuntimeData, Error};
use remotemedia_traits::{
    buffer_contract::{BufferContract, BufferPortContract, BufferProduction},
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
    #[cfg(all(feature = "native-worker", any(windows, target_os = "linux")))]
    mapped_pool: Option<crate::multiprocess::native_worker::SharedPool>,
    node_id: String,
    contract: BufferContract,
    forwarding: remotemedia_types::ForwardingScope,
    allocator: Option<Arc<remotemedia_types::BoundOutputAllocator>>,
}
impl Checks {
    fn validate_domain(
        &self,
        contract: &BufferPortContract,
        data: &RuntimeData,
    ) -> Result<(), String> {
        #[cfg(all(feature = "native-worker", any(windows, target_os = "linux")))]
        if contract
            .domains
            .contains(&remotemedia_traits::buffer_contract::BufferDomain::SessionMapped)
            && self
                .mapped_pool
                .as_ref()
                .is_some_and(|pool| pool.owns_payload(data))
        {
            return contract.validate_payload_in_domain(
                data,
                &remotemedia_traits::buffer_contract::BufferDomain::SessionMapped,
            );
        }
        contract.validate_payload(data)
    }

    fn check(&self, data: &RuntimeData, port: &str, output: bool) -> Result<(), Error> {
        let ports: &BTreeMap<String, BufferPortContract> = if output {
            &self.contract.outputs
        } else {
            &self.contract.inputs
        };
        let validation = match ports.get(port) {
            Some(contract) => self.validate_domain(contract, data),
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
        let validation = validation.and_then(|()| {
            if output {
                self.check_production(data)
            } else {
                Ok(())
            }
        });
        validation.map_err(|reason| {
            Error::Execution(format!(
                "buffer contract node '{}' {} port '{}': {reason}; no implicit conversion adapter",
                self.node_id,
                if output { "output" } else { "input" },
                if port.is_empty() { "<default>" } else { port },
            ))
        })
    }

    fn check_production(&self, data: &RuntimeData) -> Result<(), String> {
        let allocated = || {
            self.allocator
                .as_ref()
                .is_some_and(|allocator| allocator.owns_payload(data))
        };
        let (valid, reason) = match self.contract.production {
            BufferProduction::ForwardInput => (
                self.forwarding.accepts_output(data),
                "output does not retain an input admitted to this node",
            ),
            BufferProduction::OutputAllocator => (
                allocated(),
                "output has no provenance from this node's bound allocator",
            ),
            BufferProduction::ForwardOrOutputAllocator => (
                self.forwarding.accepts_output(data) || allocated(),
                "output has neither admitted-input nor bound-allocator provenance",
            ),
            BufferProduction::PrivateStorage => return Ok(()),
        };
        if valid {
            Ok(())
        } else {
            Err(reason.into())
        }
    }

    fn admit(&self, data: RuntimeData) -> RuntimeData {
        if matches!(
            self.contract.production,
            BufferProduction::ForwardInput | BufferProduction::ForwardOrOutputAllocator
        ) {
            self.forwarding.accept(data)
        } else {
            data.into_shared()
        }
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
    pub fn new(
        node_id: String,
        contract: BufferContract,
        inner: Box<dyn StreamingNode>,
        allocator: Option<Arc<remotemedia_types::BoundOutputAllocator>>,
    ) -> Self {
        Self {
            inner,
            checks: Arc::new(Checks {
                #[cfg(all(feature = "native-worker", any(windows, target_os = "linux")))]
                mapped_pool: None,
                node_id,
                contract,
                allocator,
                forwarding: Default::default(),
            }),
        }
    }

    /// Bind the concrete session pool used to validate mapped-only ports.
    /// Private/foreign buffers fail; this never stages or imports storage.
    #[cfg(all(feature = "native-worker", any(windows, target_os = "linux")))]
    pub fn with_mapped_pool(
        mut self,
        pool: crate::multiprocess::native_worker::SharedPool,
    ) -> Self {
        Arc::get_mut(&mut self.checks)
            .expect("unshared node construction")
            .mapped_pool = Some(pool);
        self
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
        self.output(
            self.inner
                .process_async(self.checks.admit(data), ctx)
                .await?,
        )
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
            .map(|(port, data)| (port, self.checks.admit(data)))
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
                .process_streaming_async(self.checks.admit(data), ctx, callback)
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
            .process_control_message(self.checks.admit(message), ctx)
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
