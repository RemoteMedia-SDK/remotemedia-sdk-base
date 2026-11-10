//! Shared Node.js buffer configuration, independent of WebRTC.
use crate::buffer_config as core_config;
use napi::bindgen_prelude::*;
use napi_derive::napi;

#[napi(object)]
#[derive(Debug, Clone)]
pub struct MappedSizeClass {
    pub bytes: f64,
    pub slots: f64,
}

#[napi(object)]
#[derive(Debug, Clone)]
pub struct MappedPoolConfig {
    pub classes: Vec<MappedSizeClass>,
    pub max_bytes: f64,
    pub max_leases: f64,
}

impl MappedPoolConfig {
    pub(crate) fn to_core_config(&self) -> Result<core_config::MappedPoolConfig> {
        let pool = core_config::MappedPoolConfig {
            classes: self
                .classes
                .iter()
                .map(|class| {
                    Ok(core_config::MappedSizeClass {
                        bytes: retention_budget(class.bytes, "mappedPool.classes.bytes")?,
                        slots: retention_budget(class.slots, "mappedPool.classes.slots")?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            max_bytes: retention_budget(self.max_bytes, "mappedPool.maxBytes")?,
            max_leases: retention_budget(self.max_leases, "mappedPool.maxLeases")?,
        };
        pool.validate()
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(pool)
    }
}

/// Shared pipeline policy and explicit retention limits for Node.js
#[napi(object)]
#[derive(Debug, Clone)]
pub struct PipelineBufferConfig {
    pub policy: String,
    /// Nonnegative safe integer, including zero for metadata-only graphs.
    pub node_retention_owners: f64,
    /// Nonnegative safe integer; never silently rounded or truncated.
    pub node_retention_bytes: f64,
    pub mapped_pool: Option<MappedPoolConfig>,
}

fn retention_budget(value: f64, field: &str) -> Result<u64> {
    if !value.is_finite() || value.fract() != 0.0 || !(0.0..=9007199254740991.0).contains(&value) {
        return Err(Error::from_reason(format!(
            "pipelineBuffers.{field} must be a nonnegative safe integer"
        )));
    }
    Ok(value as u64)
}

impl PipelineBufferConfig {
    pub(crate) fn to_core_config(&self) -> Result<core_config::PipelineBufferConfig> {
        Ok(core_config::PipelineBufferConfig {
            mapped_pool: self
                .mapped_pool
                .as_ref()
                .map(MappedPoolConfig::to_core_config)
                .transpose()?,
            policy: core_config::parse_copy_policy(&self.policy, "pipelineBuffers.policy")
                .map_err(Error::from_reason)?,
            node_retention_owners: retention_budget(
                self.node_retention_owners,
                "nodeRetentionOwners",
            )?,
            node_retention_bytes: retention_budget(
                self.node_retention_bytes,
                "nodeRetentionBytes",
            )?,
        })
    }
}

impl PipelineBufferConfig {
    pub(crate) fn to_executor_config(&self) -> Result<remotemedia_core::transport::ExecutorConfig> {
        self.to_core_config()?
            .to_executor_config()
            .map_err(|e| Error::from_reason(e.to_string()))
    }
}
