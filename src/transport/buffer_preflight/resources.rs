use super::*;

/// Planned session allocation and transport policy. This is configuration for
/// pure preflight, not a capability to certify arbitrary storage. Runtime domain
/// checks use the actual pool and allocation/admission owners independently.
#[derive(Debug, Clone)]
pub struct BufferPreflightResources {
    pub session_mapped: bool,
    pub mapped_input_policy: CopyPolicy,
}
impl Default for BufferPreflightResources {
    fn default() -> Self {
        Self {
            session_mapped: false,
            mapped_input_policy: CopyPolicy::RequireZeroCopy,
        }
    }
}
impl BufferPreflightResources {
    pub(super) fn mapped_available(&self) -> bool {
        cfg!(all(
            feature = "native-worker",
            any(windows, target_os = "linux")
        )) && self.session_mapped
    }

    pub(super) fn mapped_worker_issue(
        &self,
        node: &NodeManifest,
        registry: &StreamingNodeRegistry,
    ) -> Option<&'static str> {
        if !self.mapped_available() {
            return Some("mapped buffer contract requires a supported session mapped pool");
        }
        if self.mapped_input_policy != CopyPolicy::RequireZeroCopy {
            return Some("mapped buffer contract requires RequireZeroCopy input policy; compatibility staging is enabled");
        }
        #[cfg(all(feature = "native-worker", any(windows, target_os = "linux")))]
        if registry.session_worker(&node.node_type).is_some() {
            return None;
        }
        let _ = (node, registry);
        Some("mapped buffer contract requires register_session_worker for exact session pool binding")
    }
}
