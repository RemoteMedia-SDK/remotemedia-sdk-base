//! Shared buffer configuration for native sessions and transport frontends.
use remotemedia_types::CopyPolicy;
use serde::{Deserialize, Serialize};

/// One policy for preflight, node, transport and mapped-worker materialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineBufferConfig {
    pub policy: CopyPolicy,
    pub node_retention_owners: u64,
    pub node_retention_bytes: u64,
    /// Optional per-session mapped storage for producer output loans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mapped_pool: Option<MappedPoolConfig>,
}

mod mapped_pool_config;
pub use mapped_pool_config::{MappedPoolConfig, MappedSizeClass};

pub(crate) fn parse_copy_policy(value: &str, field: &str) -> Result<CopyPolicy, String> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|_| format!("{field} must be allow_copies or require_zero_copy"))
}

/// Configuration validation errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum ConfigValidationError {
    #[error("Invalid pipeline mapped pool: {0}")]
    InvalidMappedPool(String),
    #[error("Either port (embedded) or signaling_url (external) must be specified")]
    MissingSignalingConfig,

    #[error("Cannot specify both port and signaling_url - choose embedded or external signaling")]
    ConflictingSignalingConfig,

    #[error("Invalid port: {0} (must be non-zero)")]
    InvalidPort(u16),

    #[error("Invalid signaling URL: {0} (must start with grpc:// or grpcs://)")]
    InvalidSignalingUrl(String),

    #[error("At least one STUN server is required")]
    NoStunServers,

    #[error("Invalid STUN URL: {0} (must start with stun: or stuns:)")]
    InvalidStunUrl(String),

    #[error("Invalid TURN URL: {0} (must start with turn: or turns:)")]
    InvalidTurnUrl(String),

    #[error("TURN server username is required")]
    MissingTurnUsername,

    #[error("TURN server credential is required")]
    MissingTurnCredential,

    #[error("Invalid max_peers: {0} (must be 1-10)")]
    InvalidMaxPeers(u32),
}

impl PipelineBufferConfig {
    /// Configure one policy and explicit retention limits without creating a session.
    pub fn to_executor_config(
        &self,
    ) -> Result<remotemedia_core::transport::ExecutorConfig, ConfigValidationError> {
        if let Some(pool) = &self.mapped_pool {
            pool.validate()?;
        }
        Ok(remotemedia_core::transport::ExecutorConfig {
            pipeline_copy_policy: Some(self.policy),
            buffer_preflight: Some(
                remotemedia_core::transport::buffer_preflight::BufferPreflightConfig {
                    policy: self.policy,
                    node_retention_budget: remotemedia_traits::buffer_contract::BufferRetention {
                        owners: self.node_retention_owners,
                        bytes: self.node_retention_bytes,
                    },
                },
            ),
            #[cfg(all(feature = "mapped-buffers", any(windows, target_os = "linux")))]
            mapped_pool: self
                .mapped_pool
                .as_ref()
                .map(MappedPoolConfig::to_pool_config)
                .transpose()?,
            ..Default::default()
        })
    }
}
