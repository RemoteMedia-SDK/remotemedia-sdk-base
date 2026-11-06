//! Bounded ownership transfer from synchronous node callbacks to async fan-out.
use crate::{data::RuntimeData, Error, Result};
use tokio::sync::mpsc::{error::TrySendError, Sender};

/// Success means the queue owns the output. Never acknowledge a discarded frame.
/// Current-thread runtimes cannot block a synchronous callback waiting for their
/// own consumer; a full queue there is an explicit execution error.
pub(super) fn send_output(
    sender: &Sender<RuntimeData>,
    node: &str,
    output: RuntimeData,
) -> Result<()> {
    let output = match sender.try_send(output) {
        Ok(()) => return Ok(()),
        Err(TrySendError::Full(output)) => output,
        Err(TrySendError::Closed(_)) => {
            return Err(Error::Execution(format!(
                "node '{node}' output queue is closed"
            )));
        }
    };
    let result = match tokio::runtime::Handle::try_current() {
        Err(_) => sender.blocking_send(output),
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| sender.blocking_send(output))
        }
        Ok(_) => return Err(Error::Execution(format!(
            "node '{node}' output queue is full; synchronous backpressure requires a multi-thread runtime or an external producer thread"
        ))),
    };
    result.map_err(|_| {
        Error::Execution(format!(
            "node '{node}' output queue closed during backpressure"
        ))
    })
}
