//! Opt-in bounds for built-in DSP whose contextual hooks retain or allocate owners.
//! Executor buffer preflight binds and enforces these declarations. The raw node
//! API is not a budget/validation boundary; omitting limits leaves support unknown.
use crate::{Error, Result};
use remotemedia_traits::buffer_contract::*;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AudioBufferLimits {
    domain: BufferDomain,
    max_input_bytes: u64,
    max_output_bytes: u64,
    retention: BufferRetention,
}

pub(super) fn from_params(params: &Value) -> Result<Option<BufferContract>> {
    let Some(value) = params.get("buffer_limits") else {
        return Ok(None);
    };
    let limits: AudioBufferLimits = serde_json::from_value(value.clone())
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
        layouts: vec![BufferLayout::AudioInterleavedF32],
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

pub(super) fn schema() -> Value {
    json!({
        "type":"object", "additionalProperties":false,
        "description":"Optional audio buffer declaration enforced by executor buffer preflight. Retention counts backing storage, including output capacity and escaped views; omit to leave support undeclared.",
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
