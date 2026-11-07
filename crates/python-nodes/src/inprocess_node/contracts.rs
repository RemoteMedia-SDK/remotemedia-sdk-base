//! Check authored bounds against the concrete in-process Python adapter.
use crate::registry::{PythonExecutionMode, PythonNodeConfig};
use remotemedia_core::Error;
use remotemedia_traits::buffer_contract::*;

pub(super) fn protocol() -> BufferProtocol {
    BufferProtocol::InProcessPython {
        buffers: INPROCESS_PYTHON_BUFFER_VERSION,
    }
}

pub(super) fn inspect(config: &PythonNodeConfig) -> Result<Option<BufferContract>, Error> {
    let Some(contract) = &config.shared_buffer_contract else {
        return Ok(None);
    };
    if config.execution_mode != PythonExecutionMode::InProcessShared
        || contract.version != BUFFER_CONTRACT_VERSION
        || contract.protocol != protocol()
    {
        return Err(Error::ConfigError("in-process Python buffer declaration disagrees with its execution mode or protocol version".into()));
    }
    if contract.production == BufferProduction::PrivateStorage {
        return Err(Error::ConfigError(
            "shared Python cannot declare private output storage".into(),
        ));
    }
    for ports in [&contract.inputs, &contract.outputs] {
        if ports.len() != 1 || !ports.contains_key("") {
            return Err(Error::ConfigError("in-process Python requires exactly one default input and output; named/snapshot ports are unsupported".into()));
        }
        let port = &ports[""];
        port.validate_definition().map_err(Error::ConfigError)?;
        if port.max_payload_bytes > isize::MAX as u64
            || port
                .domains
                .iter()
                .any(|domain| matches!(domain, BufferDomain::Device(_)))
            || port.layouts.iter().any(|layout| {
                matches!(
                    layout,
                    BufferLayout::TensorPythonIpc | BufferLayout::TensorProtobuf
                )
            })
        {
            return Err(Error::ConfigError("in-process Python requires addressable host/session-mapped storage and runtime tensor encoding".into()));
        }
    }
    Ok(Some(contract.clone()))
}
