//! Device tensors at host-only boundaries (task 6.4).
//!
//! A [`RuntimeData::DeviceTensor`] travels by owner through in-process routing,
//! fan-out and queues. Where it meets a consumer that has not declared it
//! accepts that device — a host node, a worker process, a plugin, or the
//! session's client output — the router transfers it to host first. That is a
//! counted `DeviceTransfer` under the consumer's copy context: strict policy
//! rejects it before any device access, compatibility waits on the producer's
//! readiness event and copies through the allocation's own primitive.

use crate::data::{CopyContext, Device, RuntimeData, TransferError};

/// Deliver `data` unchanged when it is host data or `accepts` its device;
/// otherwise transfer it to host under `copies`.
pub(crate) async fn host_unless_accepted(
    data: RuntimeData,
    accepts: impl FnOnce(Device) -> bool,
    copies: &CopyContext,
) -> Result<RuntimeData, TransferError> {
    match data.device() {
        Some(device) if !accepts(device) => {
            let copies = copies.clone();
            // The transfer waits on the device and copies; keep it off the
            // async workers.
            tokio::task::spawn_blocking(move || data.device_to_host(&copies))
                .await
                .map_err(|error| {
                    TransferError::Device(format!("device transfer task failed: {error}"))
                })?
        }
        _ => Ok(data),
    }
}
