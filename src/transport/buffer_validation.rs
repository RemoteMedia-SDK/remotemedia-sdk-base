//! Runtime layout/bound checks for explicitly declared node contracts.
//! Validation borrows buffers; forwarding moves/promotes owners without copying.
//! Host provenance and live admitted-input/bound-output retention are checked.
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

mod retention;
mod snapshots;

struct Checks {
    #[cfg(all(feature = "native-worker", any(windows, target_os = "linux")))]
    mapped_pool: Option<crate::multiprocess::native_worker::SharedPool>,
    node_id: String,
    contract: BufferContract,
    forwarding: remotemedia_types::ForwardingScope,
    allocator: Option<Arc<remotemedia_types::BoundOutputAllocator>>,
    snapshot_failure: CallbackFailure,
    retention: Option<remotemedia_types::BufferBudget>,
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
        self.snapshot_failure.result(Ok(()))?;
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

    fn admit(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        let data = match &self.retention {
            Some(budget) => budget.admit(data).map_err(|error| {
                Error::Execution(format!(
                    "buffer retention budget node '{}': {error}",
                    self.node_id,
                ))
            })?,
            None => data,
        };
        Ok(
            if matches!(
                self.contract.production,
                BufferProduction::ForwardInput | BufferProduction::ForwardOrOutputAllocator
            ) {
                self.forwarding.accept(data)
            } else {
                data.into_shared()
            },
        )
    }

    fn callback(self: &Arc<Self>, mut callback: Callback) -> (Callback, CallbackFailure) {
        let checks = self.clone();
        let failure = CallbackFailure::default();
        let captured = failure.clone();
        let guarded = Box::new(move |data: RuntimeData| {
            captured.result(Ok(()))?;
            let result = checks.output(data).and_then(&mut callback);
            if let Err(error) = &result {
                *captured.0.lock().unwrap() = Some(error.to_string());
            }
            result
        });
        (guarded, failure)
    }

    fn output(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        self.check(&data, "", true)?;
        // Private producers cannot reserve before their native allocation, but
        // every returned/published view must retain credit before it escapes.
        // Forwarded and allocator-owned outputs already carry their receipts.
        if self.contract.production == BufferProduction::PrivateStorage {
            self.admit(data)
        } else {
            Ok(data.into_shared())
        }
    }
}

/// A node may ignore a rejected output callback. Keep that failure for the call
/// result and reject later yields, rather than reporting successful completion.
#[derive(Clone, Default)]
struct CallbackFailure(Arc<Mutex<Option<String>>>);
impl CallbackFailure {
    fn capture<T>(&self, result: Result<T, Error>) -> Result<T, Error> {
        if let Err(error) = &result {
            self.0
                .lock()
                .unwrap()
                .get_or_insert_with(|| error.to_string());
        }
        result
    }

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
/// Snapshot readers validate both boundaries and retain owners. A failed snapshot
/// read is latched so legacy optional reads cannot hide a contract violation.
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
        let snapshot_failure = CallbackFailure::default();
        let retention = match retention::bind(&contract, allocator.as_deref()) {
            Ok(budget) => budget,
            Err(error) => {
                let _ = snapshot_failure.capture::<()>(Err(error));
                None
            }
        };
        Self {
            inner,
            checks: Arc::new(Checks {
                #[cfg(all(feature = "native-worker", any(windows, target_os = "linux")))]
                mapped_pool: None,
                node_id,
                contract,
                allocator,
                forwarding: Default::default(),
                snapshot_failure,
                retention,
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
        self.checks.output(data)
    }

    fn finish<T>(&self, result: Result<T, Error>) -> Result<T, Error> {
        let value = self.checks.snapshot_failure.result(result)?;
        // Validate currently visible publications even without a consumer. Any
        // later concurrent publication is checked again by the bound reader.
        for reader in self.snapshot_outputs().values() {
            reader.runtime_snapshot()?;
        }
        Ok(value)
    }
}

#[async_trait::async_trait]
impl StreamingNode for BufferCheckedNode {
    fn node_type(&self) -> &str {
        self.inner.node_type()
    }
    fn accepts_device(&self, device: remotemedia_types::Device) -> bool {
        self.inner.accepts_device(device)
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
        self.checks.snapshot_failure.result(Ok(()))?;
        self.finish(self.inner.initialize(ctx).await)
    }

    async fn process_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        self.checks.check(&data, "", false)?;
        self.finish(
            self.output(
                self.inner
                    .process_async(self.checks.admit(data)?, ctx)
                    .await?,
            ),
        )
    }

    async fn process_multi_async(
        &self,
        mut inputs: HashMap<String, RuntimeData>,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        self.checks.snapshot_failure.result(Ok(()))?;
        for (port, data) in &inputs {
            self.checks.check(data, port, false)?;
        }
        // Move normalization preserves legacy Vec allocations before user fan-out.
        inputs = inputs
            .into_iter()
            .map(|(port, data)| self.checks.admit(data).map(|data| (port, data)))
            .collect::<Result<_, _>>()?;
        self.finish(self.output(self.inner.process_multi_async(inputs, ctx).await?))
    }

    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        callback: Callback,
    ) -> Result<usize, Error> {
        self.checks.check(&data, "", false)?;
        let (callback, failure) = self.checks.callback(callback);
        self.finish(
            failure.result(
                self.inner
                    .process_streaming_async(self.checks.admit(data)?, ctx, callback)
                    .await,
            ),
        )
    }

    async fn tick(
        &self,
        tick: Tick,
        ctx: &dyn NodeRuntimeContextRead,
        callback: Callback,
    ) -> Result<(), Error> {
        self.checks.snapshot_failure.result(Ok(()))?;
        let (callback, failure) = self.checks.callback(callback);
        self.finish(failure.result(self.inner.tick(tick, ctx, callback).await))
    }

    async fn process_control_message(
        &self,
        message: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<bool, Error> {
        if super::session_control::aux_port_of(&message).as_deref() == Some("shutdown") {
            // Cleanup must run even after a latched contract failure. Once it
            // returns, remote readers may legitimately be closed; do not poll
            // them again. Preserve any earlier snapshot failure in the result.
            return self
                .checks
                .snapshot_failure
                .result(self.inner.process_control_message(message, ctx).await);
        }
        self.checks.check(&message, "", false)?;
        self.finish(
            self.inner
                .process_control_message(self.checks.admit(message)?, ctx)
                .await,
        )
    }

    fn validate_snapshot_input(&self, port: &str, reader: &dyn SnapshotPort) -> Result<(), Error> {
        snapshots::validate_binding(&self.checks, port, reader, false)?;
        self.inner.validate_snapshot_input(port, reader)
    }
    fn bind_snapshot_input(
        &self,
        port: &str,
        reader: Arc<dyn SnapshotPort>,
    ) -> Result<Arc<dyn SnapshotPort>, Error> {
        self.validate_snapshot_input(port, reader.as_ref())?;
        // Preserve any node-specific binding adapter before checking the reader
        // that will actually be exposed to execution contexts.
        let reader = self.inner.bind_snapshot_input(port, reader)?;
        snapshots::validate_binding(&self.checks, port, reader.as_ref(), false)?;
        Ok(snapshots::checked(
            self.checks.clone(),
            port.to_owned(),
            reader,
            false,
        ))
    }
    fn snapshot_outputs(&self) -> HashMap<String, Arc<dyn SnapshotPort>> {
        self.inner
            .snapshot_outputs()
            .into_iter()
            .map(|(port, reader)| {
                let checked = snapshots::checked(self.checks.clone(), port.clone(), reader, true);
                (port, checked)
            })
            .collect()
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
        self.checks.snapshot_failure.result(Ok(()))?;
        self.finish(self.inner.call_rpc(request, ctx, replies).await)
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
        self.checks.snapshot_failure.result(Ok(()))?;
        self.finish(self.inner.on_capability_update(addr, caps, ctx).await)
    }
}
