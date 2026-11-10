//! Opt-in bounds for built-in media nodes that retain or allocate owners.
//! Executor buffer preflight binds and enforces these declarations. The raw node
//! API is not a budget/validation boundary; omitting limits leaves support unknown.
use crate::{Error, Result};
use remotemedia_traits::buffer_contract::*;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MediaBufferLimits {
    domain: BufferDomain,
    max_input_bytes: u64,
    max_output_bytes: u64,
    retention: BufferRetention,
}

pub(super) fn from_params(params: &Value, layout: BufferLayout) -> Result<Option<BufferContract>> {
    let Some(value) = params.get("buffer_limits") else {
        return Ok(None);
    };
    let limits: MediaBufferLimits = serde_json::from_value(value.clone())
        .map_err(|error| Error::ConfigError(format!("buffer_limits: {error}")))?;
    if !matches!(
        limits.domain,
        BufferDomain::Host | BufferDomain::SessionMapped
    ) {
        return Err(Error::ConfigError(
            "buffer_limits supports only host or session_mapped storage".into(),
        ));
    }
    let maximum = limits.max_input_bytes.max(limits.max_output_bytes);
    if limits.max_input_bytes == 0
        || limits.max_output_bytes == 0
        || maximum > isize::MAX as u64
        || limits.retention.owners == 0
        || limits.retention.bytes < maximum
    {
        return Err(Error::ConfigError("buffer_limits needs addressable nonzero payload bounds and retention covering at least one maximum payload".into()));
    }
    let port = |max_payload_bytes| BufferPortContract {
        layouts: vec![layout.clone()],
        domains: vec![limits.domain.clone()],
        max_payload_bytes,
    };
    Ok(Some(BufferContract {
        version: BUFFER_CONTRACT_VERSION,
        protocol: BufferProtocol::RustOwners,
        inputs: BTreeMap::from([(String::new(), port(limits.max_input_bytes))]),
        outputs: BTreeMap::from([(String::new(), port(limits.max_output_bytes))]),
        production: BufferProduction::ForwardOrOutputAllocator,
        retention: Some(limits.retention),
    }))
}

pub(super) fn forwarding(params: &Value, layout: BufferLayout) -> Result<Option<BufferContract>> {
    let Some(mut contract) = from_params(params, layout)? else {
        return Ok(None);
    };
    if contract.outputs[""].max_payload_bytes < contract.inputs[""].max_payload_bytes {
        return Err(Error::ConfigError(
            "forwarding buffer_limits output bound must cover its input bound".into(),
        ));
    }
    contract.production = BufferProduction::ForwardInput;
    Ok(Some(contract))
}

pub(super) fn schema() -> Value {
    json!({
        "type":"object", "additionalProperties":false,
        "description":"Optional media buffer declaration enforced by executor buffer preflight. Retention counts backing storage, including conversion loans, output capacity and escaped views; omit to leave support undeclared.",
        "required":["domain","max_input_bytes","max_output_bytes","retention"],
        "properties":{
            "domain":{"type":"string","enum":["host","session_mapped"]},
            "max_input_bytes":{"type":"integer","minimum":1,"maximum":isize::MAX as u64},
            "max_output_bytes":{"type":"integer","minimum":1,"maximum":isize::MAX as u64},
            "retention":{
                "type":"object", "additionalProperties":false, "required":["owners","bytes"],
                "properties":{"owners":{"type":"integer","minimum":1},"bytes":{"type":"integer","minimum":1}}
            }
        }
    })
}

/// Metadata by default; callers can explicitly admit bounded bulk passthrough.
pub(super) fn metadata_or_forwarding(params: &Value) -> Result<BufferContract> {
    let Some(value) = params.get("passthrough_layouts") else {
        if params.get("buffer_limits").is_some() {
            return Err(Error::ConfigError(
                "buffer_limits requires explicit passthrough_layouts".into(),
            ));
        }
        return Ok(BufferContract::metadata_only(BufferProtocol::RustOwners));
    };
    let layouts: Vec<BufferLayout> = serde_json::from_value(value.clone())
        .map_err(|error| Error::ConfigError(format!("passthrough_layouts: {error}")))?;
    let first = layouts
        .first()
        .ok_or_else(|| Error::ConfigError("passthrough_layouts must not be empty".into()))?;
    let mut contract = forwarding(params, first.clone())?.ok_or_else(|| {
        Error::ConfigError("passthrough_layouts requires bounded buffer_limits".into())
    })?;
    for port in contract
        .inputs
        .values_mut()
        .chain(contract.outputs.values_mut())
    {
        port.layouts = layouts.clone();
        port.validate_definition().map_err(Error::ConfigError)?;
    }
    Ok(contract)
}

pub(super) fn passthrough_schema() -> Value {
    json!({"type":"array", "minItems":1,
    "description":"Optional bulk layouts forwarded unchanged. Requires buffer_limits. Tensor declarations must select exactly one dtype convention.",
    "items":{"type":"string","enum":[
        BufferLayout::AudioInterleavedF32, BufferLayout::VideoBytes, BufferLayout::ImageBytes,
        BufferLayout::TensorRuntime, BufferLayout::TensorPythonIpc, BufferLayout::TensorProtobuf,
        BufferLayout::NumpyCContiguous, BufferLayout::NumpyFContiguous, BufferLayout::Binary
    ]}})
}
