//! Lossless gRPC manifest decoding and server-side plugin policy.

use crate::{generated::PipelineManifest as ProtoPipelineManifest, ServiceError};
use remotemedia_core::manifest::{self, Manifest, PluginSpec};
use std::path::{Path, PathBuf};

/// Policy applied before a submitted manifest reaches plugin resolution.
#[derive(Clone, Debug, Default)]
pub struct PluginPolicy {
    local_only: bool,
    allowed_roots: Vec<PathBuf>,
    manifest_base_dir: PathBuf,
}

impl PluginPolicy {
    /// Build policy from the gRPC server environment.
    ///
    /// `GRPC_PLUGIN_POLICY=local-only` rejects every non-local plugin spec.
    /// `GRPC_PLUGIN_ROOTS` is a colon-separated allowlist of canonical roots.
    pub fn from_env() -> Result<Self, ServiceError> {
        let local_only = match std::env::var("GRPC_PLUGIN_POLICY") {
            Ok(value) if value.eq_ignore_ascii_case("local-only") => true,
            Ok(value) if value.eq_ignore_ascii_case("permissive") => false,
            Ok(value) => {
                return Err(ServiceError::Validation(format!(
                    "unsupported GRPC_PLUGIN_POLICY '{value}' (expected permissive or local-only)"
                )))
            }
            Err(std::env::VarError::NotPresent) => false,
            Err(error) => {
                return Err(ServiceError::Validation(format!(
                    "GRPC_PLUGIN_POLICY is unusable: {error}"
                )))
            }
        };
        let manifest_base_dir = std::env::var_os("GRPC_MANIFEST_BASE_DIR")
            .map(PathBuf::from)
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from("."));
        let roots = std::env::var_os("GRPC_PLUGIN_ROOTS")
            .map(|value| std::env::split_paths(&value).collect::<Vec<_>>())
            .unwrap_or_default();

        let mut allowed_roots = Vec::with_capacity(roots.len());
        for root in roots {
            let canonical = root.canonicalize().map_err(|error| {
                ServiceError::Validation(format!(
                    "configured plugin root '{}' is unusable: {error}",
                    root.display()
                ))
            })?;
            allowed_roots.push(canonical);
        }
        if local_only && allowed_roots.is_empty() {
            return Err(ServiceError::Validation(
                "GRPC_PLUGIN_POLICY=local-only requires GRPC_PLUGIN_ROOTS".to_string(),
            ));
        }

        Ok(Self {
            local_only,
            allowed_roots,
            manifest_base_dir,
        })
    }

    /// Permissive SDK default. Resolution is still performed by core.
    pub fn permissive() -> Self {
        Self::default()
    }

    #[cfg(test)]
    fn local_only(allowed_roots: Vec<PathBuf>, manifest_base_dir: PathBuf) -> Self {
        Self {
            local_only: true,
            allowed_roots,
            manifest_base_dir,
        }
    }

    pub fn validate(&self, manifest: &Manifest) -> Result<(), ServiceError> {
        if !self.local_only {
            return Ok(());
        }

        for plugin in &manifest.plugins {
            let raw_path = local_path(plugin).ok_or_else(|| {
                ServiceError::Validation(
                    "server plugin policy permits only explicit local library paths".to_string(),
                )
            })?;
            let candidate = if raw_path.is_absolute() {
                raw_path
            } else {
                self.manifest_base_dir.join(raw_path)
            };
            let canonical = candidate.canonicalize().map_err(|error| {
                ServiceError::Validation(format!(
                    "plugin path '{}' is unusable: {error}",
                    candidate.display()
                ))
            })?;
            if !self
                .allowed_roots
                .iter()
                .any(|root| canonical.starts_with(root))
            {
                return Err(ServiceError::Validation(format!(
                    "plugin path '{}' is outside configured roots",
                    canonical.display()
                )));
            }
            if canonical.is_dir() {
                let metadata = canonical.join("plugin.toml");
                if !metadata.is_file() {
                    return Err(ServiceError::Validation(format!(
                        "source plugin directory '{}' does not contain plugin.toml",
                        canonical.display()
                    )));
                }
                let contents = std::fs::read_to_string(&metadata).map_err(|error| {
                    ServiceError::Validation(format!(
                        "source plugin metadata '{}' is unusable: {error}",
                        metadata.display()
                    ))
                })?;
                validate_source_plugin_toml(&contents).map_err(|error| {
                    ServiceError::Validation(format!(
                        "source plugin metadata '{}' is invalid: {error}",
                        metadata.display()
                    ))
                })?;
            } else if !canonical.is_file() {
                return Err(ServiceError::Validation(format!(
                    "plugin path '{}' is neither a file nor a source plugin directory",
                    canonical.display()
                )));
            }
        }
        Ok(())
    }
}

fn validate_source_plugin_toml(contents: &str) -> Result<(), String> {
    let value: toml::Value = toml::from_str(contents).map_err(|error| error.to_string())?;
    let plugin = value
        .get("plugin")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| "missing [plugin] table".to_string())?;
    if plugin.get("language").and_then(toml::Value::as_str) != Some("python") {
        return Err("[plugin].language must be python for a source directory".to_string());
    }
    let python = value
        .get("python")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| "missing [python] table".to_string())?;
    if python
        .get("entry_module")
        .and_then(toml::Value::as_str)
        .is_none_or(str::is_empty)
    {
        return Err("[python].entry_module must be non-empty".to_string());
    }
    if python
        .get("node_types")
        .and_then(toml::Value::as_array)
        .is_none_or(Vec::is_empty)
    {
        return Err("[python].node_types must be non-empty".to_string());
    }
    Ok(())
}

fn local_path(spec: &PluginSpec) -> Option<PathBuf> {
    match spec {
        PluginSpec::Explicit(explicit)
            if explicit.url.is_none() && explicit.name.is_none() && explicit.path.is_some() =>
        {
            explicit.path.as_ref().map(PathBuf::from)
        }
        PluginSpec::Shorthand(value) if looks_like_local_library(value) => {
            Some(PathBuf::from(value))
        }
        _ => None,
    }
}

fn looks_like_local_library(value: &str) -> bool {
    let path = Path::new(value);
    path.is_absolute()
        || value.starts_with("./")
        || value.starts_with("../")
        || matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("so" | "dylib" | "dll")
        )
}

/// Decode either the complete JSON field or the legacy protobuf projection.
pub fn decode_manifest(
    proto: &ProtoPipelineManifest,
    policy: &PluginPolicy,
) -> Result<Manifest, ServiceError> {
    let manifest = if proto.manifest_json.is_empty() {
        decode_legacy_manifest(proto)?
    } else {
        let json = std::str::from_utf8(&proto.manifest_json).map_err(|error| {
            ServiceError::Validation(format!("manifest_json is not UTF-8: {error}"))
        })?;
        let complete =
            manifest::parse(json).map_err(|error| ServiceError::Validation(error.to_string()))?;

        if legacy_fields_present(proto) {
            let legacy = decode_legacy_manifest(proto)?;
            let complete_value = serde_json::to_value(&complete)
                .map_err(|error| ServiceError::Validation(error.to_string()))?;
            let legacy_value = serde_json::to_value(&legacy)
                .map_err(|error| ServiceError::Validation(error.to_string()))?;
            if complete_value != legacy_value {
                return Err(ServiceError::Validation(
                    "manifest_json conflicts with structured manifest fields".to_string(),
                ));
            }
        }
        complete
    };

    manifest::validate(&manifest).map_err(|error| ServiceError::Validation(error.to_string()))?;
    policy.validate(&manifest)?;
    Ok(manifest)
}

fn legacy_fields_present(proto: &ProtoPipelineManifest) -> bool {
    !proto.version.is_empty()
        || proto.metadata.is_some()
        || !proto.nodes.is_empty()
        || !proto.connections.is_empty()
}

fn decode_legacy_manifest(proto: &ProtoPipelineManifest) -> Result<Manifest, ServiceError> {
    let json = serde_json::json!({
        "version": proto.version,
        "metadata": {
            "name": proto.metadata.as_ref().map(|m| m.name.clone()).unwrap_or_else(|| "test".to_string()),
            "description": proto.metadata.as_ref().map(|m| m.description.clone()),
            "created_at": proto.metadata.as_ref().map(|m| m.created_at.clone()),
        },
        "nodes": proto.nodes.iter().map(|node| serde_json::json!({
            "id": node.id,
            "node_type": node.node_type,
            "params": serde_json::from_str::<serde_json::Value>(&node.params)
                .unwrap_or_else(|_| serde_json::json!({})),
            "is_streaming": node.is_streaming,
            "capabilities": node.capabilities.as_ref().map(|capabilities| serde_json::json!({
                "gpu": capabilities.gpu.as_ref().map(|gpu| serde_json::json!({
                    "type": gpu.r#type,
                    "min_memory_gb": gpu.min_memory_gb,
                    "required": gpu.required,
                })),
                "cpu": capabilities.cpu.as_ref().map(|cpu| serde_json::json!({
                    "cores": cpu.cores,
                    "architecture": cpu.arch,
                })),
                "memory_gb": capabilities.memory_gb,
            })),
            "host": if node.host.is_empty() { None } else { Some(node.host.clone()) },
            "runtime_hint": match node.runtime_hint {
                1 => "rust_python",
                2 => "cpython",
                3 => "cpython_wasm",
                _ => "auto",
            },
        })).collect::<Vec<_>>(),
        "connections": proto.connections.iter().map(|connection| serde_json::json!({
            "from": connection.from,
            "to": connection.to,
        })).collect::<Vec<_>>(),
    });

    serde_json::from_value(json)
        .map_err(|error| ServiceError::Validation(format!("failed to parse manifest: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::PipelineManifest;
    use std::fs;

    fn complete_manifest(plugins: serde_json::Value) -> PipelineManifest {
        PipelineManifest {
            manifest_json: serde_json::to_vec(&serde_json::json!({
                "version": "v1",
                "metadata": {"name": "complete"},
                "nodes": [{
                    "id": "node",
                    "node_type": "WhisperCpp",
                    "params": {},
                    "is_streaming": true,
                    "is_output_node": true
                }],
                "connections": [],
                "plugins": plugins
            }))
            .unwrap(),
            ..Default::default()
        }
    }

    #[test]
    fn complete_manifest_preserves_runtime_fields() {
        let proto = complete_manifest(serde_json::json!([]));
        let manifest = decode_manifest(&proto, &PluginPolicy::permissive()).unwrap();
        assert!(manifest.nodes[0].is_output_node);
        assert!(manifest.nodes[0].is_streaming);
    }

    #[test]
    fn rejects_conflicting_representations() {
        let mut proto = complete_manifest(serde_json::json!([]));
        proto.version = "v1".to_string();
        let error = decode_manifest(&proto, &PluginPolicy::permissive()).unwrap_err();
        assert!(error.to_string().contains("conflicts"));
    }

    #[test]
    fn local_only_policy_rejects_remote_and_escape_paths() {
        let temp = tempfile::tempdir().unwrap();
        let allowed = temp.path().join("allowed");
        fs::create_dir(&allowed).unwrap();
        let plugin = allowed.join("plugin.so");
        fs::write(&plugin, b"fixture").unwrap();
        let outside = temp.path().join("outside.so");
        fs::write(&outside, b"fixture").unwrap();
        let policy = PluginPolicy::local_only(
            vec![allowed.canonicalize().unwrap()],
            temp.path().to_path_buf(),
        );

        assert!(decode_manifest(&complete_manifest(serde_json::json!([plugin])), &policy).is_ok());
        assert!(
            decode_manifest(&complete_manifest(serde_json::json!([outside])), &policy).is_err()
        );
        assert!(decode_manifest(
            &complete_manifest(serde_json::json!(["owner/repository@v1"])),
            &policy
        )
        .is_err());

        #[cfg(unix)]
        {
            let escaped_link = allowed.join("escaped.so");
            std::os::unix::fs::symlink(&outside, &escaped_link).unwrap();
            assert!(decode_manifest(
                &complete_manifest(serde_json::json!([escaped_link])),
                &policy
            )
            .is_err());
        }
    }

    #[test]
    fn local_only_policy_accepts_only_valid_allowlisted_source_directories() {
        let temp = tempfile::tempdir().unwrap();
        let allowed = temp.path().join("allowed");
        fs::create_dir(&allowed).unwrap();
        let source = allowed.join("lfm2-source");
        fs::create_dir(&source).unwrap();
        fs::write(
            source.join("plugin.toml"),
            r#"
[plugin]
name = "lfm2-source"
version = "1.0.0"
language = "python"

[python]
entry_module = "lfm2_audio_source"
node_types = ["LFM2AudioNode"]
module_root = "."
requires = []
"#,
        )
        .unwrap();
        let missing_metadata = allowed.join("missing-metadata");
        fs::create_dir(&missing_metadata).unwrap();
        let invalid_metadata = allowed.join("invalid-metadata");
        fs::create_dir(&invalid_metadata).unwrap();
        fs::write(invalid_metadata.join("plugin.toml"), "not = [valid").unwrap();
        let policy = PluginPolicy::local_only(
            vec![allowed.canonicalize().unwrap()],
            temp.path().to_path_buf(),
        );

        assert!(decode_manifest(&complete_manifest(serde_json::json!([source])), &policy).is_ok());
        assert!(decode_manifest(
            &complete_manifest(serde_json::json!([missing_metadata])),
            &policy
        )
        .unwrap_err()
        .to_string()
        .contains("does not contain plugin.toml"));
        assert!(decode_manifest(
            &complete_manifest(serde_json::json!([invalid_metadata])),
            &policy
        )
        .unwrap_err()
        .to_string()
        .contains("is invalid"));
    }
}
