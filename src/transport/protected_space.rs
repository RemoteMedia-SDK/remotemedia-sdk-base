//! Host-side authorization and manifest validation for the protected Spaces runtime.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::manifest::{Manifest, NodeManifest, PluginSpec};

#[cfg(all(
    target_os = "linux",
    target_arch = "x86_64",
    feature = "loadable",
    feature = "python-source-plugin"
))]
mod preparation;
#[cfg(all(
    target_os = "linux",
    target_arch = "x86_64",
    feature = "loadable",
    feature = "python-source-plugin"
))]
pub use preparation::{ProtectedPreparationSettings, ProtectedSpacePreparation};

#[cfg(all(target_os = "linux", feature = "protected-space-runsc"))]
pub mod runsc;

/// The protected contract shared with the authenticated admission service.
pub const PROTECTED_SPACE_CONTRACT_VERSION: u32 = 1;
const MAX_MANIFEST_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtectedSpacePhase {
    Prepare,
    Serve { artifact_sha256: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtectedSpaceProfile {
    Reviewed,
    Isolated,
}

/// An approval supplied by the trusted host after checking the caller and the
/// worker policy. This type has no manifest or wire deserializer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtectedSpaceApproval {
    pub grant_id: String,
    pub owner: String,
    pub run_id: String,
    pub session_id: String,
    pub repository: String,
    pub commit: String,
    pub target: String,
    pub profile: ProtectedSpaceProfile,
    pub policy_revision: String,
    pub expires_unix: u64,
    pub phase: ProtectedSpacePhase,
    pub manifest_sha256: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ProtectedSpaceError {
    #[error("protected execution approval has expired")]
    Expired,
    #[error("worker clock is unavailable")]
    Clock,
    #[error("invalid protected execution approval")]
    Approval,
    #[error("protected Space manifest differs from its approval")]
    ManifestDigest,
    #[error("protected Space manifest does not match the trusted controller contract")]
    Controller,
    #[error("Verified protected execution runtime is unavailable")]
    RuntimeUnavailable,
}

/// Holds the exact parsed manifest and the out-of-band approval that applies
/// to it. Only the protected runtime can turn this into a running session.
#[derive(Debug)]
pub struct VerifiedSpaceExecution {
    approval: ProtectedSpaceApproval,
    controller_path: PathBuf,
    manifest: Manifest,
}

impl VerifiedSpaceExecution {
    pub(crate) fn validate(
        approval: ProtectedSpaceApproval,
        controller_path: &Path,
        manifest_bytes: &[u8],
    ) -> Result<Self, ProtectedSpaceError> {
        approval.check_current(SystemTime::now())?;
        if !approval.valid() || !controller_path.is_absolute() {
            return Err(ProtectedSpaceError::Approval);
        }
        if manifest_bytes.len() > MAX_MANIFEST_BYTES
            || format!("{:x}", Sha256::digest(manifest_bytes)) != approval.manifest_sha256
        {
            return Err(ProtectedSpaceError::ManifestDigest);
        }
        let manifest =
            serde_json::from_slice(manifest_bytes).map_err(|_| ProtectedSpaceError::Controller)?;
        validate_controller(&manifest, &approval, controller_path)?;
        let requested =
            serde_json::from_slice(manifest_bytes).map_err(|_| ProtectedSpaceError::Controller)?;
        let normalized =
            serde_json::to_value(&manifest).map_err(|_| ProtectedSpaceError::Controller)?;
        if !contains_manifest_fields(&normalized, &requested) {
            return Err(ProtectedSpaceError::Controller);
        }
        Ok(Self {
            approval,
            controller_path: controller_path.to_path_buf(),
            manifest,
        })
    }

    pub fn approval(&self) -> &ProtectedSpaceApproval {
        &self.approval
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// The worker-configured controller path for protected loading.
    pub fn controller_path(&self) -> &Path {
        &self.controller_path
    }

    pub fn check_current(&self) -> Result<(), ProtectedSpaceError> {
        self.approval.check_current(SystemTime::now())
    }
}

impl ProtectedSpaceApproval {
    fn check_current(&self, now: SystemTime) -> Result<(), ProtectedSpaceError> {
        let now = now
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ProtectedSpaceError::Clock)?;
        if now.as_secs() >= self.expires_unix {
            return Err(ProtectedSpaceError::Expired);
        }
        Ok(())
    }

    fn valid(&self) -> bool {
        [
            &self.grant_id,
            &self.owner,
            &self.run_id,
            &self.session_id,
            &self.target,
        ]
        .iter()
        .all(|value| identifier(value))
            && self
                .repository
                .split_once('/')
                .is_some_and(|(namespace, name)| {
                    [namespace, name].iter().all(|part| {
                        identifier(part) && !part.contains(':') && !matches!(*part, "." | "..")
                    })
                })
            && lower_hex(&self.commit, 40)
            && lower_hex(&self.policy_revision, 64)
            && lower_hex(&self.manifest_sha256, 64)
            && match &self.phase {
                ProtectedSpacePhase::Prepare => true,
                ProtectedSpacePhase::Serve { artifact_sha256 } => lower_hex(artifact_sha256, 64),
            }
    }
}

fn validate_controller(
    manifest: &Manifest,
    approval: &ProtectedSpaceApproval,
    controller_path: &Path,
) -> Result<(), ProtectedSpaceError> {
    let ([node], [PluginSpec::Explicit(plugin)]) =
        (manifest.nodes.as_slice(), manifest.plugins.as_slice())
    else {
        return Err(ProtectedSpaceError::Controller);
    };
    let mut params = json!({"placement": {"control_only": true, "state_class": "non_migratable"}});
    let node_type = match &approval.phase {
        ProtectedSpacePhase::Prepare => "PrepareSpaceNode",
        ProtectedSpacePhase::Serve { artifact_sha256 } => {
            params["approved_artifact_sha256"] = json!(artifact_sha256);
            params["descriptor"] =
                json!({"space_id": approval.repository, "revision": approval.commit});
            "HuggingFaceSpaceNode"
        }
    };
    let expected_node = NodeManifest {
        id: "space".into(),
        node_type: node_type.into(),
        params,
        is_streaming: true,
        ..Default::default()
    };
    let node_matches = serde_json::to_value(node)
        .and_then(|node| serde_json::to_value(expected_node).map(|expected| node == expected))
        .map_err(|_| ProtectedSpaceError::Controller)?;
    if manifest.version != "v1"
        || !manifest.connections.is_empty()
        || manifest.python_env.is_some()
        || manifest.metadata.default_python_runtime.is_some()
        || manifest.metadata.auto_negotiate
        || manifest.metadata.strict_capabilities
        || !node_matches
        || plugin.path.as_deref().map(Path::new) != Some(controller_path)
        || plugin.url.is_some()
        || plugin.name.is_some()
        || plugin.version.is_some()
        || plugin.sha256.is_some()
        || plugin.node_types.as_slice() != [node_type]
    {
        return Err(ProtectedSpaceError::Controller);
    }
    Ok(())
}

// Omitted defaults may be populated during decoding. A directive in the
// request must survive decoding so unknown or disabled fields cannot be ignored.
fn contains_manifest_fields(normalized: &Value, requested: &Value) -> bool {
    match (normalized, requested) {
        (Value::Object(normalized), Value::Object(requested)) => {
            requested.iter().all(|(key, value)| {
                normalized
                    .get(key)
                    .is_some_and(|field| contains_manifest_fields(field, value))
            })
        }
        (Value::Array(normalized), Value::Array(requested)) => {
            normalized.len() == requested.len()
                && normalized
                    .iter()
                    .zip(requested)
                    .all(|(field, value)| contains_manifest_fields(field, value))
        }
        _ => normalized == requested,
    }
}

fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::transport::{PipelineExecutor, WarmSessionPool};

    pub(super) fn fixture(phase: ProtectedSpacePhase) -> (ProtectedSpaceApproval, PathBuf, Value) {
        let commit = "a".repeat(40);
        let path = std::env::current_dir()
            .unwrap()
            .join("released-space-controller");
        let mut params =
            json!({"placement": {"control_only": true, "state_class": "non_migratable"}});
        let node_type = match &phase {
            ProtectedSpacePhase::Prepare => "PrepareSpaceNode",
            ProtectedSpacePhase::Serve { artifact_sha256 } => {
                params["approved_artifact_sha256"] = json!(artifact_sha256);
                params["descriptor"] = json!({"space_id": "org/demo", "revision": commit});
                "HuggingFaceSpaceNode"
            }
        };
        let manifest = json!({"version": "v1", "metadata": {"name": "space"}, "connections": [],
            "nodes": [{"id":"space", "node_type":node_type, "is_streaming":true, "params":params}],
            "plugins": [{"path":path, "node_types":[node_type]}]});
        let approval = ProtectedSpaceApproval {
            grant_id: "grant-1".into(),
            owner: "peer-1".into(),
            run_id: "run-1".into(),
            session_id: "session-1".into(),
            repository: "org/demo".into(),
            commit,
            target: "linux-amd64-cpu".into(),
            profile: ProtectedSpaceProfile::Isolated,
            policy_revision: "b".repeat(64),
            expires_unix: u64::MAX,
            phase,
            manifest_sha256: String::new(),
        };
        (approval, path, manifest)
    }

    pub(super) fn encode(approval: &mut ProtectedSpaceApproval, manifest: &Value) -> Vec<u8> {
        let bytes = serde_json::to_vec(manifest).unwrap();
        approval.manifest_sha256 = format!("{:x}", Sha256::digest(&bytes));
        bytes
    }

    #[test]
    fn protected_context_controls_both_phases_and_rejects_altered_or_expired_inputs() {
        let executor = PipelineExecutor::for_protected_spaces().unwrap();
        for phase in [
            ProtectedSpacePhase::Prepare,
            ProtectedSpacePhase::Serve {
                artifact_sha256: "c".repeat(64),
            },
        ] {
            let (mut approval, path, manifest) = fixture(phase);
            let mut bytes = encode(&mut approval, &manifest);
            let verified = executor
                .verify_protected_space(approval.clone(), &path, &bytes)
                .unwrap();
            assert_eq!(verified.approval().phase, approval.phase);
            assert_eq!(verified.approval().grant_id, "grant-1");
            assert_eq!(verified.approval().run_id, "run-1");
            assert_eq!(verified.controller_path(), path);
            assert_eq!(verified.manifest().nodes.len(), 1);
            let mut override_owner = manifest.clone();
            override_owner["nodes"][0]["params"]["preparation"] =
                json!({"isolated_owner": "other-peer"});
            let mut override_approval = approval.clone();
            let override_bytes = encode(&mut override_approval, &override_owner);
            assert!(matches!(
                executor.verify_protected_space(override_approval, &path, &override_bytes),
                Err(ProtectedSpaceError::Controller)
            ));
            bytes.push(b' ');
            assert!(matches!(
                executor.verify_protected_space(approval.clone(), &path, &bytes),
                Err(ProtectedSpaceError::ManifestDigest)
            ));
            approval.expires_unix = 0;
            assert!(matches!(
                executor.verify_protected_space(approval, &path, &bytes),
                Err(ProtectedSpaceError::Expired)
            ));
        }
    }

    #[test]
    fn protected_context_rejects_authorized_bytes_that_select_another_runtime_input() {
        let executor = PipelineExecutor::for_protected_spaces().unwrap();
        let (mut approval, path, manifest) = fixture(ProtectedSpacePhase::Serve {
            artifact_sha256: "c".repeat(64),
        });
        for (pointer, value) in [
            ("/authorization", json!({"owner":"other-peer"})),
            ("/python_env", json!({"extra_deps":["unapproved-package"]})),
            ("/metadata/default_python_runtime", json!("glibc")),
            ("/nodes/0/python_env", json!({"scope":"global"})),
            ("/nodes/0/host", json!("external")),
            ("/nodes/0/docker", json!({"image":"unapproved-image"})),
            ("/nodes/0/params/execution_mode", json!("reviewed")),
            ("/nodes/0/params/__python_version__", json!("3.11")),
            ("/nodes/0/params/owner", json!("other-peer")),
            (
                "/nodes/0/params/descriptor/checkout",
                json!("other-directory"),
            ),
            ("/nodes/0/params/descriptor/space_id", json!("org/other")),
            (
                "/nodes/0/params/approved_artifact_sha256",
                json!("d".repeat(64)),
            ),
            ("/plugins/0/path", json!("./alternate-plugin")),
        ] {
            let (parent, key) = pointer.rsplit_once('/').unwrap();
            let mut attempted = manifest.clone();
            attempted.pointer_mut(parent).unwrap()[key] = value;
            let bytes = encode(&mut approval, &attempted);
            assert!(
                matches!(
                    executor.verify_protected_space(approval.clone(), &path, &bytes),
                    Err(ProtectedSpaceError::Controller)
                ),
                "accepted {pointer}"
            );
        }
    }

    #[tokio::test]
    async fn protected_executor_denies_manifest_only_loading_and_warm_sessions() {
        let (mut approval, path, manifest) = fixture(ProtectedSpacePhase::Prepare);
        let bytes = encode(&mut approval, &manifest);
        let protected = Arc::new(PipelineExecutor::for_protected_spaces().unwrap());
        let verified = protected
            .verify_protected_space(approval.clone(), &path, &bytes)
            .unwrap();
        let manifest = Arc::new(verified.manifest().clone());
        let is_protected_denial = |error: crate::Error| {
            error
                .to_string()
                .contains("requires verified protected execution")
        };
        assert!(protected
            .ensure_plugins_loaded(&manifest)
            .await
            .is_err_and(is_protected_denial));
        assert!(protected
            .create_session(Arc::clone(&manifest))
            .await
            .is_err_and(is_protected_denial));
        let pool = WarmSessionPool::new(Arc::clone(&protected));
        assert!(pool.prewarm(manifest).await.is_err_and(is_protected_denial));
        let legacy = PipelineExecutor::new().unwrap();
        assert!(legacy
            .ensure_plugins_loaded(&Manifest::default())
            .await
            .is_ok());
        assert!(matches!(
            legacy.verify_protected_space(approval, &path, &bytes),
            Err(ProtectedSpaceError::RuntimeUnavailable)
        ));
    }
}
