//! Budget-aware ingress. No raw sender is exposed after binding a session budget.
use super::DataPacket;
use crate::{Error, Result};
use remotemedia_types::{BufferAllocationError, BufferBudget};
use std::sync::{Arc, OnceLock};
use tokio::sync::mpsc;

pub(super) fn bind_allocator(
    router: &super::SessionRouter,
    provider: Arc<dyn remotemedia_types::OutputAllocator>,
    config: &super::ExecutorConfig,
) -> Result<Arc<dyn remotemedia_types::OutputAllocator>> {
    let limit = config.ingress_retention.or_else(|| {
        config
            .buffer_preflight
            .as_ref()
            .map(|preflight| preflight.node_retention_budget)
    });
    let Some(limit) = limit else {
        return Ok(provider);
    };
    let budget = BufferBudget::new(limit.owners, limit.bytes);
    router.set_ingress_budget(budget.clone())?;
    Ok(Arc::new(
        remotemedia_types::BoundOutputAllocator::new(provider).with_budget(budget),
    ))
}

/// Send-only packet channel shared by session, participant and control ingress.
/// Counts owning backing spans until the last admitted view is released, including
/// after dequeue or session close. Channel message-count backpressure is separate.
#[derive(Clone)]
pub struct SessionPacketSender {
    sender: mpsc::Sender<DataPacket>,
    budget: IngressAdmission,
}

/// Admission policy survives closing ingress, without keeping its queue open.
#[derive(Clone, Default)]
pub(crate) struct IngressAdmission(Arc<OnceLock<Option<BufferBudget>>>);

impl IngressAdmission {
    pub(crate) fn admit_payload(
        &self,
        data: &mut crate::data::RuntimeData,
    ) -> std::result::Result<(), BufferAllocationError> {
        match self.0.get_or_init(|| None) {
            Some(budget) => budget.admit_in_place(data),
            None => Ok(()),
        }
    }
}

impl From<mpsc::Sender<DataPacket>> for SessionPacketSender {
    /// Wrap a legacy channel. Without a binding it retains only channel capacity.
    fn from(sender: mpsc::Sender<DataPacket>) -> Self {
        Self {
            sender,
            budget: IngressAdmission::default(),
        }
    }
}

impl SessionPacketSender {
    pub(crate) fn admission(&self) -> IngressAdmission {
        self.budget.clone()
    }

    pub(crate) fn control_payload(
        &self,
        port: Option<&str>,
        data: crate::data::RuntimeData,
    ) -> Result<crate::data::RuntimeData> {
        let Some(port) = port else { return Ok(data) };
        use crate::data::RuntimeData::*;
        if self.budget.0.get_or_init(|| None).is_some()
            && matches!(
                &data,
                Audio { .. }
                    | Binary(_)
                    | Video { .. }
                    | Image { .. }
                    | Tensor { .. }
                    | Numpy { .. }
            )
        {
            return Err(Error::Execution("budgeted ingress cannot encode bulk payloads in auxiliary JSON envelopes; an explicit bulk-port adapter is required".into()));
        }
        Ok(super::session_control::wrap_aux_port(port, data))
    }

    /// Bind before the first send. Clones share the same binding. First use seals
    /// an unconfigured sender as legacy, so already-queued inputs cannot be missed
    /// by a later claim that a budget was active from creation.
    pub(crate) fn bind_budget(&self, budget: BufferBudget) -> Result<()> {
        self.budget.0.set(Some(budget)).map_err(|_| {
            Error::ConfigError("ingress budget must be bound once, before the first send".into())
        })
    }

    /// An owning handle for monitoring usage through retained views and shutdown.
    pub fn buffer_budget(&self) -> Option<BufferBudget> {
        self.budget.0.get().and_then(Clone::clone)
    }

    fn admit(&self, mut packet: DataPacket) -> std::result::Result<DataPacket, IngressSendError> {
        if let Err(error) = self.admit_payload(&mut packet.data) {
            return Err(IngressSendError::Budget { packet, error });
        }
        Ok(packet)
    }

    /// Control publications and intercept replacements are external ingress too,
    /// even though they do not travel through the router's packet queue.
    pub(crate) fn admit_payload(
        &self,
        data: &mut crate::data::RuntimeData,
    ) -> std::result::Result<(), BufferAllocationError> {
        self.budget.admit_payload(data)
    }

    /// Wait for channel space; fail immediately when owner/byte credit is exhausted.
    /// Cancelling this future releases its admitted owner. A returned error owns
    /// the packet and therefore any receipt already attached to it.
    pub async fn send(&self, packet: DataPacket) -> std::result::Result<(), IngressSendError> {
        let packet = self.admit(packet)?;
        self.sender
            .send(packet)
            .await
            .map_err(|error| IngressSendError::Closed(error.0))
    }

    /// Reserve channel space only. The permit's fallible send admits buffer
    /// ownership when the packet is supplied; it cannot bypass the live budget.
    pub async fn reserve(&self) -> Result<SessionPacketPermit<'_>> {
        let permit = self
            .sender
            .reserve()
            .await
            .map_err(|_| Error::Execution("session ingress channel closed".into()))?;
        Ok(SessionPacketPermit {
            permit,
            owner: self,
        })
    }

    pub fn try_send(&self, packet: DataPacket) -> std::result::Result<(), IngressSendError> {
        let packet = self.admit(packet)?;
        self.sender.try_send(packet).map_err(|error| match error {
            mpsc::error::TrySendError::Full(packet) => IngressSendError::Full(packet),
            mpsc::error::TrySendError::Closed(packet) => IngressSendError::Closed(packet),
        })
    }

    pub fn capacity(&self) -> usize {
        self.sender.capacity()
    }
    pub fn max_capacity(&self) -> usize {
        self.sender.max_capacity()
    }
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
    pub async fn closed(&self) {
        self.sender.closed().await;
    }
}

/// Reserved channel space. Payload admission still occurs at [`Self::send`].
pub struct SessionPacketPermit<'a> {
    permit: mpsc::Permit<'a, DataPacket>,
    owner: &'a SessionPacketSender,
}
impl SessionPacketPermit<'_> {
    pub fn send(self, packet: DataPacket) -> std::result::Result<(), IngressSendError> {
        self.permit.send(self.owner.admit(packet)?);
        Ok(())
    }
}

/// A rejected submission, retaining its packet for inspection or retry.
#[derive(Debug)]
pub enum IngressSendError {
    Full(DataPacket),
    Closed(DataPacket),
    Budget {
        packet: DataPacket,
        error: BufferAllocationError,
    },
}
impl IngressSendError {
    pub fn into_inner(self) -> DataPacket {
        match self {
            Self::Full(packet) | Self::Closed(packet) | Self::Budget { packet, .. } => packet,
        }
    }
}
impl std::fmt::Display for IngressSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full(_) => f.write_str("session ingress channel full"),
            Self::Closed(_) => f.write_str("session ingress channel closed"),
            Self::Budget { error, .. } => write!(f, "session ingress buffer budget: {error}"),
        }
    }
}
impl std::error::Error for IngressSendError {}

#[cfg(test)]
mod tests;
