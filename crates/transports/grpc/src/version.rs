//! Protocol version negotiation for gRPC service
//!
//! Implements version checking and compatibility matrix.
//! Ensures clients and server can communicate successfully.

use std::collections::HashSet;

/// Current protocol version
pub const PROTOCOL_VERSION: &str = "v1";

/// Runtime version (from Cargo.toml)
pub const RUNTIME_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Build timestamp (compile time)
pub const BUILD_TIMESTAMP: &str = "2025-10-28T00:00:00Z"; // TODO: Use build-time generation

/// Version compatibility manager
#[derive(Clone, Debug)]
pub struct VersionManager {
    /// Supported protocol versions
    supported_protocols: HashSet<String>,

    /// Registered node types
    supported_node_types: Vec<String>,
}

impl Default for VersionManager {
    fn default() -> Self {
        // Empty node types by default
        // Caller should use from_node_types() with actual registry
        Self::new(vec!["v1".to_string()], Vec::new())
    }
}

impl VersionManager {
    /// Create new version manager with custom supported versions
    pub fn new(supported_protocols: Vec<String>, supported_node_types: Vec<String>) -> Self {
        Self {
            supported_protocols: supported_protocols.into_iter().collect(),
            supported_node_types,
        }
    }

    /// Create version manager from node type list
    ///
    /// This populates supported node types from all registry tiers.
    pub fn from_node_types(node_types: Vec<String>) -> Self {
        Self {
            supported_protocols: vec!["v1".to_string()].into_iter().collect(),
            supported_node_types: node_types,
        }
    }

    /// Check if a protocol version is supported
    pub fn is_protocol_supported(&self, version: &str) -> bool {
        self.supported_protocols.contains(version)
    }

    /// Check if a node type is supported
    pub fn is_node_type_supported(&self, node_type: &str) -> bool {
        self.supported_node_types.iter().any(|t| t == node_type)
    }

    /// Validate protocol version, return error if unsupported
    pub fn validate_protocol(&self, version: &str) -> Result<(), VersionError> {
        if !self.is_protocol_supported(version) {
            return Err(VersionError::UnsupportedProtocol {
                requested: version.to_string(),
                supported: self.supported_protocols.iter().cloned().collect(),
            });
        }
        Ok(())
    }

    /// Convert to protobuf VersionInfo
    pub fn to_proto(&self) -> crate::VersionInfo {
        crate::VersionInfo {
            protocol_version: PROTOCOL_VERSION.to_string(),
            runtime_version: RUNTIME_VERSION.to_string(),
            supported_node_types: self.supported_node_types.clone(),
            supported_protocols: self.supported_protocols.iter().cloned().collect(),
            build_timestamp: BUILD_TIMESTAMP.to_string(),
        }
    }
}

/// Version-related errors
#[derive(Debug, thiserror::Error)]
pub enum VersionError {
    #[error("Unsupported protocol version: {requested}, supported: {supported:?}")]
    UnsupportedProtocol {
        requested: String,
        supported: Vec<String>,
    },

    #[error("Unsupported node type: {node_type}")]
    UnsupportedNodeType { node_type: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_version_manager() {
        let vm = VersionManager::from_node_types(vec![
            "AudioResample".to_string(),
            "PassThrough".to_string(),
        ]);
        assert!(vm.is_protocol_supported("v1"));
        assert!(!vm.is_protocol_supported("v2"));
        assert!(vm.is_node_type_supported("AudioResample"));
        assert!(!vm.is_node_type_supported("UnknownNode"));
    }

    #[test]
    fn test_validate_protocol_success() {
        let vm = VersionManager::default();
        assert!(vm.validate_protocol("v1").is_ok());
    }

    #[test]
    fn test_validate_protocol_failure() {
        let vm = VersionManager::default();
        let result = vm.validate_protocol("v99");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            VersionError::UnsupportedProtocol { .. }
        ));
    }

    #[test]
    fn test_version_info_proto() {
        let vm = VersionManager::from_node_types(vec!["AudioResample".to_string()]);
        let proto = vm.to_proto();

        assert_eq!(proto.protocol_version, "v1");
        assert_eq!(proto.runtime_version, RUNTIME_VERSION);
        assert!(proto
            .supported_node_types
            .contains(&"AudioResample".to_string()));
        assert!(proto.supported_protocols.contains(&"v1".to_string()));
    }

    #[test]
    fn test_custom_version_manager() {
        let vm = VersionManager::new(
            vec!["v1".to_string(), "v2".to_string()],
            vec!["CustomNode".to_string()],
        );

        assert!(vm.is_protocol_supported("v1"));
        assert!(vm.is_protocol_supported("v2"));
        assert!(vm.is_node_type_supported("CustomNode"));
        assert!(!vm.is_node_type_supported("AudioResample"));
    }
}
