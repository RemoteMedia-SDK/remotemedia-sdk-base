//! Bounded, serializable pool selection; validation never creates mappings.
use super::ConfigValidationError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "python", pyo3::pyclass(get_all, frozen, from_py_object))]
pub struct MappedSizeClass {
    pub bytes: u64,
    pub slots: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "python", pyo3::pyclass(get_all, frozen, from_py_object))]
pub struct MappedPoolConfig {
    pub classes: Vec<MappedSizeClass>,
    pub max_bytes: u64,
    pub max_leases: u64,
}

impl MappedPoolConfig {
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        let invalid = |message: &str| ConfigValidationError::InvalidMappedPool(message.into());
        if self.classes.is_empty() || self.max_bytes == 0 || self.max_leases == 0 {
            return Err(invalid("classes, max_bytes and max_leases must be nonzero"));
        }
        if usize::try_from(self.max_bytes).is_err() || usize::try_from(self.max_leases).is_err() {
            return Err(invalid("budgets exceed this platform's address range"));
        }
        let mut total = 0u64;
        let mut slots = 0u64;
        for class in &self.classes {
            if class.bytes == 0 || class.slots == 0 || class.bytes > isize::MAX as u64 {
                return Err(invalid(
                    "class bytes and slots must be positive and addressable",
                ));
            }
            total = class
                .bytes
                .checked_mul(class.slots)
                .and_then(|bytes| total.checked_add(bytes))
                .ok_or_else(|| invalid("class byte totals overflow"))?;
            slots = slots
                .checked_add(class.slots)
                .ok_or_else(|| invalid("slot totals overflow"))?;
        }
        if total > self.max_bytes || slots > u32::MAX as u64 {
            return Err(invalid("classes exceed max_bytes or the slot index range"));
        }
        if !cfg!(all(
            feature = "mapped-buffers",
            any(windows, target_os = "linux")
        )) {
            return Err(invalid(
                "requires the mapped-buffers build feature on Windows or Linux",
            ));
        }
        Ok(())
    }

    #[cfg(all(feature = "mapped-buffers", any(windows, target_os = "linux")))]
    pub(super) fn to_pool_config(
        &self,
    ) -> Result<remotemedia_core::multiprocess::native_worker::PoolConfig, ConfigValidationError>
    {
        use remotemedia_core::multiprocess::native_worker::{PoolConfig, SizeClass};
        self.validate()?;
        Ok(PoolConfig {
            classes: self
                .classes
                .iter()
                .map(|class| SizeClass {
                    bytes: class.bytes as usize,
                    slots: class.slots as usize,
                })
                .collect(),
            max_bytes: self.max_bytes as usize,
            max_leases: self.max_leases as usize,
        })
    }
}
