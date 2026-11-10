//! Python buffer configuration shared by sessions and WebRTC.
use pyo3::prelude::*;

pub use crate::buffer_config::{MappedPoolConfig, MappedSizeClass};

#[pymethods]
impl MappedSizeClass {
    #[new]
    fn new(bytes: u64, slots: u64) -> Self {
        Self { bytes, slots }
    }
}

#[pymethods]
impl MappedPoolConfig {
    #[new]
    fn new(classes: Vec<MappedSizeClass>, max_bytes: u64, max_leases: u64) -> PyResult<Self> {
        let pool = Self {
            classes,
            max_bytes,
            max_leases,
        };
        pool.validate()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(pool)
    }
}

/// Pipeline policy and explicit buffer budgets for Python
#[pyclass(get_all, frozen)]
#[derive(Clone, Debug)]
pub struct PipelineBufferConfig {
    pub policy: String,
    pub node_retention_owners: u64,
    pub node_retention_bytes: u64,
    pub mapped_pool: Option<MappedPoolConfig>,
}

#[pymethods]
impl PipelineBufferConfig {
    #[new]
    #[pyo3(signature = (policy, node_retention_owners, node_retention_bytes, *, mapped_pool=None))]
    fn new(
        policy: &str,
        node_retention_owners: u64,
        node_retention_bytes: u64,
        mapped_pool: Option<MappedPoolConfig>,
    ) -> PyResult<Self> {
        crate::buffer_config::parse_copy_policy(policy, "pipeline_buffers.policy")
            .map_err(pyo3::exceptions::PyValueError::new_err)?;
        Ok(Self {
            policy: policy.to_owned(),
            node_retention_owners,
            node_retention_bytes,
            mapped_pool,
        })
    }
}

impl PipelineBufferConfig {
    pub(crate) fn to_core_config(&self) -> PyResult<crate::buffer_config::PipelineBufferConfig> {
        Ok(crate::buffer_config::PipelineBufferConfig {
            policy: crate::buffer_config::parse_copy_policy(
                &self.policy,
                "pipeline_buffers.policy",
            )
            .map_err(pyo3::exceptions::PyValueError::new_err)?,
            node_retention_owners: self.node_retention_owners,
            node_retention_bytes: self.node_retention_bytes,
            mapped_pool: self.mapped_pool.clone(),
        })
    }
}

impl PipelineBufferConfig {
    pub(crate) fn to_executor_config(
        &self,
    ) -> PyResult<remotemedia_core::transport::ExecutorConfig> {
        self.to_core_config()?
            .to_executor_config()
            .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))
    }
}
