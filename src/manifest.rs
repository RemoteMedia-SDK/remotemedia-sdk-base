//! Pipeline manifest parsing and validation
//!
//! This module handles JSON manifest parsing, schema validation,
//! and conversion to internal pipeline representations.
//!
//! Schema specification: ../schemas/manifest.v1.json

use crate::capabilities::MediaCapabilities;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Pipeline manifest structure (v1)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Schema version
    pub version: String,

    /// Pipeline metadata
    pub metadata: ManifestMetadata,

    /// List of nodes in the pipeline
    pub nodes: Vec<NodeManifest>,

    /// Connections between nodes
    pub connections: Vec<Connection>,

    /// Python environment configuration for managed venvs.
    /// Parsed regardless of feature flags; ignored at runtime when `bundled-uv` is not enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub python_env: Option<ManifestPythonEnv>,

    /// Loadable-node plugins this pipeline depends on.
    ///
    /// Each entry is resolved at session-creation time into a local file
    /// path (downloading from GitHub releases / cloning source as needed),
    /// then loaded via `LoadableNodeBundle::load` and registered into the
    /// executor's `StreamingNodeRegistry`. The registered node types
    /// become available for `nodes[].node_type` references in this
    /// manifest as if they were built into the runtime.
    ///
    /// Resolution requires the `plugin-resolver` feature on
    /// `remotemedia-core` (default-on for FFI / CLI builds, off for
    /// embedded / wasm). With the feature disabled, declaring `plugins`
    /// is a hard error at parse time so the misconfiguration fails loud.
    ///
    /// See `PluginSpec` for the supported shorthand and explicit forms.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plugins: Vec<PluginSpec>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            version: "v1".to_string(),
            metadata: ManifestMetadata::default(),
            nodes: Vec::new(),
            connections: Vec::new(),
            python_env: None,
            plugins: Vec::new(),
        }
    }
}

/// One entry in [`Manifest::plugins`].
///
/// JSON shorthand (the common case) is just a string:
/// ```json
/// "plugins": [
///   "echo-python-loadable",                              // canonical org shorthand
///   "moss-tts-realtime@v0.3",                            // ... at version
///   "github.com/someuser/somerepo",                      // full owner/repo
///   "github.com/someuser/somerepo@v1.0",                 // ... at tag
///   "./plugins/libfoo.so",                               // local relative path
///   "/abs/path/to/libfoo.so"                             // local absolute path
/// ]
/// ```
///
/// The explicit object form is for pinning a SHA256 or overriding
/// auto-discovered fields:
/// ```json
/// "plugins": [
///   {
///     "url": "https://github.com/owner/repo/releases/download/v1.0/libfoo-x86_64-linux.so",
///     "sha256": "abc123..."
///   },
///   { "name": "foo", "version": "v1.2.3" },
///   { "path": "./local/libfoo.so" }
/// ]
/// ```
///
/// Resolution rules — first match wins (parsed in order):
///
/// | Shorthand form                                | Resolves to                                                                 |
/// |-----------------------------------------------|------------------------------------------------------------------------------|
/// | `"./..." `, `"/..."`, `"foo.so"`              | Local file path (relative paths anchored to the manifest directory)         |
/// | `"github.com/owner/repo[@version]"`           | That repo's release at `version` (or `latest` if omitted)                   |
/// | `"owner/repo[@version]"`                      | Same as above (the `github.com/` prefix is implied)                         |
/// | `"name[@version]"` (no `/`)                   | Canonical-org shorthand: `github.com/RemoteMedia-SDK/name[@version]`        |
///
/// A bare name like `"echo-python-loadable"` is treated as "the SDK's
/// canonical-org plugin by that name". This is the easy default. The
/// URL form is the escape hatch for plugins published outside the org.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PluginSpec {
    /// Single-string shorthand. See the type-level docs for the resolution table.
    Shorthand(String),
    /// Explicit object form — required when pinning a SHA256 or splitting
    /// across multiple optional fields.
    Explicit(PluginSpecExplicit),
}

/// Object-form plugin spec. Exactly one of `url`, `name`, or `path`
/// MUST be set (validated at resolution time, not at parse — keeps the
/// schema permissive for tooling that builds manifests incrementally).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PluginSpecExplicit {
    /// Direct download URL (HTTP/HTTPS). The asset at this URL is the
    /// plugin binary (`.so`/`.dylib`/`.dll`) — no release-manifest
    /// lookup. Use this when distributing outside GitHub Releases.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    /// Canonical-org or `owner/repo` plugin name. Equivalent to the
    /// matching shorthand string. Use this when you want to combine with
    /// an explicit `version` or `sha256` rather than overloading the
    /// shorthand string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Local filesystem path (relative paths anchored to manifest dir).
    /// Use this when the binary lives in your project tree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// Version / git tag / branch. Defaults to `latest` when omitted
    /// (which means "fetch the most recent release").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,

    /// SHA256 (hex-encoded, lowercase) of the downloaded artifact.
    /// When set, the resolver refuses to use a downloaded file whose
    /// hash doesn't match — pin-by-hash for reproducibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// Python environment settings declared at the manifest level.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ManifestPythonEnv {
    /// Desired Python version (e.g. "3.11").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub python_version: Option<String>,

    /// Environment scope override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<crate::python::env_manager::EnvScope>,

    /// Extra dependencies added to all nodes in this pipeline.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_deps: Vec<String>,
}

/// Python environment settings declared on a single node.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodePythonEnv {
    /// Environment scope override for this node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<crate::python::env_manager::EnvScope>,
}

/// Pipeline metadata
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ManifestMetadata {
    /// Pipeline name
    #[serde(default)]
    pub name: String,

    /// Optional description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Creation timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,

    /// Enable automatic capability negotiation (spec 022, FR-014).
    ///
    /// When true, the system automatically inserts conversion nodes
    /// to resolve capability mismatches. When false, mismatches result
    /// in validation warnings (and an error when `strict_capabilities`
    /// is also true).
    #[serde(default)]
    pub auto_negotiate: bool,

    /// Treat unresolved capability mismatches as fatal at session creation.
    ///
    /// By default, mismatches are logged as warnings so existing manifests
    /// keep loading. When this is true (or the `REMOTEMEDIA_STRICT_CAPS`
    /// environment variable is set) the session refuses to start unless
    /// `auto_negotiate` is also true and successfully bridges every gap.
    #[serde(default)]
    pub strict_capabilities: bool,

    /// Default Python runtime to use if not overridden by node params ("bionic" or "glibc").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_python_runtime: Option<String>,
}

/// Node manifest entry
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeManifest {
    /// Unique node ID within pipeline
    pub id: String,

    /// Node type (e.g., "AudioSource", "HFPipelineNode")
    pub node_type: String,

    /// Node-specific parameters
    #[serde(default)]
    pub params: serde_json::Value,

    /// Whether this is a streaming node (async generator process method)
    #[serde(default)]
    pub is_streaming: bool,

    /// Whether this node should stream outputs to the client (spec 021, User Story 3)
    ///
    /// By default, only terminal nodes (sinks - nodes with no outputs) send data to
    /// the client. Setting `is_output_node: true` allows intermediate nodes to also
    /// stream their outputs to the client alongside terminal nodes.
    ///
    /// Use cases:
    /// - Debugging: see intermediate processing results
    /// - Monitoring: track VAD results while also getting final transcription
    /// - Branching: receive outputs from multiple stages of the pipeline
    #[serde(default)]
    pub is_output_node: bool,

    /// Optional capability requirements (GPU, CPU, memory)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<CapabilityRequirements>,

    /// Media format capabilities for input/output constraints (spec 022).
    ///
    /// Declares what media formats this node accepts as input and produces
    /// as output. Used for capability negotiation and pipeline validation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_capabilities: Option<MediaCapabilities>,

    /// Optional execution host
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,

    /// Optional runtime hint (Phase 1.10.5)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_hint: Option<RuntimeHint>,

    /// Execution placement (Phase 1.3.6 - capability-aware execution)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<ExecutionMetadata>,

    /// Docker configuration (integrated into multiprocess system)
    #[cfg(feature = "docker")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docker: Option<crate::python::multiprocess::docker_support::DockerNodeConfig>,

    /// Use low-latency fast path execution (spec 026)
    ///
    /// When enabled, the node uses `execute_streaming_node_fast()` which provides:
    /// - Lock-free circuit breaker check (atomic read only)
    /// - No timeout wrapper (avoids tokio timer overhead)
    /// - No HDR histogram metrics (just atomic sum/count/min/max)
    /// - try_acquire() for semaphore (non-blocking when permits available)
    ///
    /// Target overhead: <100ns vs ~250ns for full path.
    ///
    /// Recommended for:
    /// - Audio/video transforms that are CPU-bound and fast (<1ms)
    /// - High-frequency nodes (>100 calls/sec)
    /// - Nodes where timeout protection isn't critical
    ///
    /// NOT recommended for:
    /// - External API calls (need timeout protection)
    /// - Nodes that may hang (need circuit breaker full features)
    /// - Nodes requiring detailed latency percentiles (P50/P95/P99)
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub fast_path: bool,

    /// Python package dependencies for this node (override/extend node-declared deps).
    /// Used by the managed Python environment system to provision venvs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub python_deps: Option<Vec<String>>,

    /// Python environment policy for this node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub python_env: Option<NodePythonEnv>,
}

/// Runtime hint for Python node execution (Phase 1.10.5)
///
/// Specifies which Python runtime to use for executing the node.
/// This allows fine-grained control over runtime selection on a per-node basis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeHint {
    /// Use RustPython embedded interpreter (pure Rust, limited stdlib)
    RustPython,

    /// Use CPython via PyO3 in-process (full Python ecosystem, C-extensions)
    Cpython,

    /// Use CPython compiled to WASM (sandboxed, Phase 3)
    CpythonWasm,

    /// Automatically select runtime based on node requirements
    Auto,
}

/// Execution metadata for capability-aware placement (Phase 1.3.6)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionMetadata {
    /// Execution placement strategy
    #[serde(default)]
    pub placement: String, // "local", "remote", "prefer_local", "prefer_remote", "auto"

    /// Reason for execution placement (e.g., "requires_native_libs")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Fallback node if this one can't execute
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback: Option<String>,
}

/// Capability requirements for a node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityRequirements {
    /// GPU requirements
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpu: Option<GpuRequirement>,

    /// CPU requirements
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<CpuRequirement>,

    /// Memory requirements (GB)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_gb: Option<f64>,
}

/// GPU capability requirement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuRequirement {
    /// GPU type (cuda, rocm, metal)
    #[serde(rename = "type")]
    pub gpu_type: String,

    /// Minimum memory (GB)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_memory_gb: Option<f64>,

    /// Whether GPU is required or optional
    #[serde(default = "default_required")]
    pub required: bool,
}

/// CPU capability requirement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuRequirement {
    /// Minimum number of cores
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cores: Option<u32>,

    /// CPU architecture preference
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arch: Option<String>,
}

/// Connection between nodes
///
/// `from_port` / `to_port` are optional named-port selectors for nodes
/// that expose multiple inputs or outputs (snapshot ports, named taps,
/// fan-out by purpose). When omitted, the connection targets the node's
/// primary (anonymous) port and behaves like a streaming connection —
/// historical default. The session router uses these names plus the
/// target factory's `input_port_kinds()` to decide whether to wire an
/// mpsc channel (Stream) or a snapshot read handle (Snapshot).
///
/// `Default` is derived so existing call sites that built struct
/// literals can append `..Default::default()` without specifying the
/// new optional port-name fields.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Connection {
    /// Source node ID
    pub from: String,

    /// Target node ID
    pub to: String,

    /// Optional named output port on the source node. `None` means the
    /// node's primary (default/anonymous) output. Used for snapshot
    /// wiring: the router looks up `snapshot_outputs()[from_port]` on
    /// the producer to obtain the read handle.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub from_port: Option<String>,

    /// Optional named input port on the target node. `None` means the
    /// primary input. The router consults the target factory's
    /// `input_port_kinds()` map keyed by this name to determine whether
    /// the connection is a streaming mpsc or a snapshot read handle.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub to_port: Option<String>,
}

fn default_required() -> bool {
    true
}

/// Source kind for a downloadable model asset.
///
/// Serialized forms are stable for interchange:
/// - `huggingface`
/// - `http`
/// - `local`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceKind {
    #[serde(rename = "huggingface")]
    HuggingFace,
    #[serde(rename = "http")]
    Http,
    #[serde(rename = "local")]
    Local,
}

impl Default for SourceKind {
    fn default() -> Self {
        Self::HuggingFace
    }
}

impl From<SourceKind> for &'static str {
    fn from(value: SourceKind) -> Self {
        match value {
            SourceKind::HuggingFace => "huggingface",
            SourceKind::Http => "http",
            SourceKind::Local => "local",
        }
    }
}

/// One downloadable model asset declared in a node's `params["model_sources"]`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ModelSourceFile {
    /// Filesystem path expected at runtime.
    pub path: String,
    /// Retrieval source kind.
    #[serde(default)]
    pub source: SourceKind,
    /// File name used when fetching/caching.
    pub filename: String,
    /// Whether the file is required. Defaults to true.
    #[serde(default = "default_required")]
    pub required: bool,
    /// Optional SHA256 (hex, lowercase) for cache verification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// Optional direct download URL. When set, platforms should fetch from
    /// this location instead of deriving a URL from `source`/`filename`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Immutable upstream revision for externally cached deployment assets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    /// Exact byte size for an external asset. Embedded assets derive this on disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// Stable content-cache identity. Defaults to the SHA-256 digest when embedded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_key: Option<String>,
    /// Force embedding (`true`) or external caching (`false`) during bundle packing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embed: Option<bool>,
    /// Optional SPDX license identifier for SBOM generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    /// Whether the deployment target may supply credentials for an external source.
    #[serde(default)]
    pub target_may_supply_credentials: bool,
}

/// Per-node model sources declaration in `params["model_sources"]`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeModelSources {
    pub files: Vec<ModelSourceFile>,
}

/// Resolution outcome for one referenced model asset.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResolvedModelAsset {
    /// Node ID that referenced this file.
    pub node_id: String,
    /// Runtime path declared by the node metadata.
    pub path: String,
    /// File name used when fetching/caching.
    pub filename: String,
    /// Declared source kind.
    pub source: SourceKind,
    /// Whether node declared this file required.
    pub required: bool,
    /// Optional expected SHA256 (hex, lowercase).
    pub sha256: Option<String>,
    pub url: Option<String>,
    pub revision: Option<String>,
    pub size: Option<u64>,
    pub cache_key: Option<String>,
    pub embed: Option<bool>,
    pub license: Option<String>,
    pub target_may_supply_credentials: bool,
    /// Absolute resolved path checked on disk, if provided by base_dir.
    pub resolved_path: Option<PathBuf>,
    /// Whether the resolved path currently exists on disk.
    pub exists: bool,
}

/// Aggregated analysis output for a manifest.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResolvedModelAssets {
    /// All referenced assets, in declaration order.
    pub assets: Vec<ResolvedModelAsset>,
    /// Subset of assets that are absent and required.
    pub missing_required: Vec<ResolvedModelAsset>,
}

/// Per-node diagnostics suitable for logging/UI.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeModelDiagnostics {
    /// Node under inspection.
    pub node_id: String,
    /// Declared source files.
    pub files: Vec<ModelSourceFile>,
    /// How many of those files currently exist under the base dir.
    pub present: usize,
    /// How many required files are missing.
    pub missing_required: usize,
}

impl NodeModelSources {
    /// Convenience constructor for tests/builders.
    pub fn new(files: Vec<ModelSourceFile>) -> Self {
        Self { files }
    }
}

impl Manifest {
    /// Analyze downloadable model assets referenced by this manifest.

    /// Returns `ResolvedModelAssets` describing every file declared in
    /// `nodes[].params["model_sources"]` and whether it exists under `base_dir`.
    ///
    /// This helper never performs network I/O or downloads. Platform layers
    /// remain responsible for fetching/caching and for any auth or pin checks.
    pub fn analyze_model_assets(&self, base_dir: impl AsRef<Path>) -> Result<ResolvedModelAssets> {
        let base = base_dir.as_ref();
        let mut out = ResolvedModelAssets::default();
        for node in &self.nodes {
            let Some(node_sources) = node
                .params
                .get("model_sources")
                .and_then(|v| serde_json::from_value::<NodeModelSources>(v.clone()).ok())
            else {
                continue;
            };

            for file in &node_sources.files {
                let resolved = if file.path.is_empty() {
                    None
                } else {
                    let p = base.join(&file.path);
                    let exists = p.exists();
                    Some(p)
                };
                let exists = resolved.as_ref().map(|p| p.exists()).unwrap_or(false);

                let asset = ResolvedModelAsset {
                    node_id: node.id.clone(),
                    path: file.path.clone(),
                    filename: file.filename.clone(),
                    source: file.source,
                    required: file.required,
                    sha256: file.sha256.clone(),
                    url: file.url.clone(),
                    revision: file.revision.clone(),
                    size: file.size,
                    cache_key: file.cache_key.clone(),
                    embed: file.embed,
                    license: file.license.clone(),
                    target_may_supply_credentials: file.target_may_supply_credentials,
                    resolved_path: resolved,
                    exists,
                };
                out.assets.push(asset);
            }
        }

        out.missing_required = out
            .assets
            .iter()
            .filter(|a| a.required && !a.exists)
            .cloned()
            .collect();

        Ok(out)
    }

    /// Per-node diagnostics for logging/UI around model availability.
    pub fn node_model_diagnostics(
        &self,
        base_dir: impl AsRef<Path>,
    ) -> Result<Vec<NodeModelDiagnostics>> {
        let mut diags = Vec::new();
        for node in &self.nodes {
            let Some(node_sources) = node
                .params
                .get("model_sources")
                .and_then(|v| serde_json::from_value::<NodeModelSources>(v.clone()).ok())
            else {
                continue;
            };

            let base = base_dir.as_ref();
            let mut present = 0usize;
            let mut missing_required = 0usize;
            for file in &node_sources.files {
                let exists = if file.path.is_empty() {
                    false
                } else {
                    base.join(&file.path).exists()
                };
                if exists {
                    present += 1;
                } else if file.required {
                    missing_required += 1;
                }
            }

            diags.push(NodeModelDiagnostics {
                node_id: node.id.clone(),
                files: node_sources.files.clone(),
                present,
                missing_required,
            });
        }
        Ok(diags)
    }
}

/// Parse a JSON manifest string into a Manifest struct
pub fn parse(json: &str) -> Result<Manifest> {
    serde_json::from_str(json)
        .map_err(|e| Error::Manifest(format!("Failed to parse manifest: {}", e)))
}

/// Validate a manifest for correctness
pub fn validate(manifest: &Manifest) -> Result<()> {
    // Check version
    if manifest.version != "v1" {
        return Err(Error::Manifest(format!(
            "Unsupported manifest version: {}",
            manifest.version
        )));
    }

    // Check nodes are not empty
    if manifest.nodes.is_empty() {
        return Err(Error::Manifest(
            "Manifest must contain at least one node".to_string(),
        ));
    }

    // Validate node IDs are unique
    let mut seen_ids = std::collections::HashSet::new();
    for node in &manifest.nodes {
        if !seen_ids.insert(&node.id) {
            return Err(Error::Manifest(format!("Duplicate node ID: {}", node.id)));
        }
    }

    // Validate connections reference valid nodes
    let node_ids: std::collections::HashSet<_> = manifest.nodes.iter().map(|n| &n.id).collect();
    for conn in &manifest.connections {
        if !node_ids.contains(&conn.from) {
            return Err(Error::Manifest(format!(
                "Connection references unknown source node: {}",
                conn.from
            )));
        }
        if !node_ids.contains(&conn.to) {
            return Err(Error::Manifest(format!(
                "Connection references unknown target node: {}",
                conn.to
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_manifest() {
        let json = r#"{
            "version": "v1",
            "metadata": {
                "name": "test-pipeline"
            },
            "nodes": [
                {
                    "id": "node1",
                    "node_type": "AudioSource",
                    "params": {}
                }
            ],
            "connections": []
        }"#;

        let manifest = parse(json).unwrap();
        assert_eq!(manifest.version, "v1");
        assert_eq!(manifest.metadata.name, "test-pipeline");
        assert_eq!(manifest.nodes.len(), 1);
    }

    #[test]
    fn test_validate_empty_nodes() {
        let manifest = Manifest {
            version: "v1".to_string(),
            metadata: ManifestMetadata {
                name: "test".to_string(),
                ..Default::default()
            },
            nodes: vec![],
            connections: vec![],
            python_env: None,
            plugins: Vec::new(),
        };

        assert!(validate(&manifest).is_err());
    }

    /// Test is_output_node field parsing from JSON (spec 021 User Story 3)
    #[test]
    fn test_parse_is_output_node_field() {
        // Test with is_output_node explicitly set to true
        let json = r#"{
            "version": "v1",
            "metadata": { "name": "test-pipeline" },
            "nodes": [
                {
                    "id": "node1",
                    "node_type": "AudioSource",
                    "params": {},
                    "is_output_node": true
                },
                {
                    "id": "node2",
                    "node_type": "AudioSink",
                    "params": {},
                    "is_output_node": false
                }
            ],
            "connections": [{"from": "node1", "to": "node2"}]
        }"#;

        let manifest = parse(json).unwrap();
        assert_eq!(manifest.nodes.len(), 2);
        assert!(manifest.nodes[0].is_output_node); // Explicitly true
        assert!(!manifest.nodes[1].is_output_node); // Explicitly false
    }

    #[test]
    fn test_plugins_field_shorthand_strings() {
        let json = r#"{
            "version": "v1",
            "metadata": { "name": "test-pipeline" },
            "plugins": [
                "echo-python-loadable",
                "moss-tts-realtime@v0.3",
                "github.com/someone/foo@main",
                "./plugins/libfoo.so",
                "/abs/path/libbar.so"
            ],
            "nodes": [{ "id": "n", "node_type": "X", "params": {} }],
            "connections": []
        }"#;
        let manifest = parse(json).expect("parse");
        assert_eq!(manifest.plugins.len(), 5);
        for spec in &manifest.plugins {
            assert!(matches!(spec, PluginSpec::Shorthand(_)));
        }
    }

    #[test]
    fn test_plugins_field_explicit_object() {
        let json = r#"{
            "version": "v1",
            "metadata": { "name": "test-pipeline" },
            "plugins": [
                {
                    "url": "https://example.com/libfoo.so",
                    "sha256": "abc123"
                },
                {
                    "name": "echo-python-loadable",
                    "version": "v0.2"
                },
                { "path": "./local/libfoo.so" }
            ],
            "nodes": [{ "id": "n", "node_type": "X", "params": {} }],
            "connections": []
        }"#;
        let manifest = parse(json).expect("parse");
        assert_eq!(manifest.plugins.len(), 3);
        match &manifest.plugins[0] {
            PluginSpec::Explicit(e) => {
                assert_eq!(e.url.as_deref(), Some("https://example.com/libfoo.so"));
                assert_eq!(e.sha256.as_deref(), Some("abc123"));
            }
            other => panic!("expected Explicit, got {other:?}"),
        }
        match &manifest.plugins[1] {
            PluginSpec::Explicit(e) => {
                assert_eq!(e.name.as_deref(), Some("echo-python-loadable"));
                assert_eq!(e.version.as_deref(), Some("v0.2"));
            }
            other => panic!("expected Explicit, got {other:?}"),
        }
        match &manifest.plugins[2] {
            PluginSpec::Explicit(e) => {
                assert_eq!(e.path.as_deref(), Some("./local/libfoo.so"));
            }
            other => panic!("expected Explicit, got {other:?}"),
        }
    }

    #[test]
    fn test_plugins_field_defaults_empty() {
        let json = r#"{
            "version": "v1",
            "metadata": { "name": "test-pipeline" },
            "nodes": [{ "id": "n", "node_type": "X", "params": {} }],
            "connections": []
        }"#;
        let manifest = parse(json).expect("parse");
        assert!(manifest.plugins.is_empty());
    }

    #[test]
    fn test_node_python_env_scope_parses() {
        let json = r#"{
            "version": "v1",
            "metadata": { "name": "test-pipeline" },
            "python_env": { "scope": "global", "python_version": "3.12" },
            "nodes": [{
                "id": "lfm2_audio",
                "node_type": "LFM2AudioNode",
                "params": {},
                "python_env": { "scope": "per_node" }
            }],
            "connections": []
        }"#;
        let manifest = parse(json).expect("parse");
        assert_eq!(
            manifest.nodes[0]
                .python_env
                .as_ref()
                .and_then(|env| env.scope.as_ref()),
            Some(&crate::python::env_manager::EnvScope::PerNode)
        );
    }

    /// Test is_output_node defaults to false when not specified
    #[test]
    fn test_is_output_node_defaults_to_false() {
        let json = r#"{
            "version": "v1",
            "metadata": { "name": "test-pipeline" },
            "nodes": [
                {
                    "id": "node1",
                    "node_type": "AudioSource",
                    "params": {}
                }
            ],
            "connections": []
        }"#;

        let manifest = parse(json).unwrap();
        assert!(!manifest.nodes[0].is_output_node); // Defaults to false
    }

    #[test]
    fn test_model_sources_ignored_when_absent() {
        let json = r#"{
            "version": "v1",
            "metadata": { "name": "test-pipeline" },
            "nodes": [
                {
                    "id": "node1",
                    "node_type": "WhisperNode",
                    "params": {"language": "en"}
                }
            ],
            "connections": []
        }"#;

        let manifest = parse(json).expect("parse");
        let dir = std::env::temp_dir();
        let assets = manifest.analyze_model_assets(&dir).expect("analyze");
        assert!(assets.assets.is_empty());
        assert!(assets.missing_required.is_empty());
    }

    #[test]
    fn test_analyze_model_assets_with_present_file() {
        let dir = std::env::temp_dir().join("remotemedia-model-assets-test");
        let _ = std::fs::create_dir_all(&dir);
        let target = dir.join("whisper-tiny.tflite");
        let _ = std::fs::write(&target, b"x");

        let manifest = Manifest {
            version: "v1".to_string(),
            metadata: ManifestMetadata {
                name: "test".to_string(),
                ..Default::default()
            },
            nodes: vec![NodeManifest {
                id: "asr".to_string(),
                node_type: "WhisperNode".to_string(),
                params: serde_json::json!({
                    "model_path": target.to_string_lossy().to_string(),
                    "model_sources": {
                        "files": [{
                            "path": target.to_string_lossy().to_string(),
                            "source": "huggingface",
                            "filename": "whisper-tiny.tflite",
                            "required": true,
                            "sha256": "ffff"
                        }]
                    }
                }),
                ..Default::default()
            }],
            connections: vec![],
            python_env: None,
            plugins: Vec::new(),
        };

        let assets = manifest.analyze_model_assets(&dir).expect("analyze");
        assert_eq!(assets.assets.len(), 1);
        assert!(assets.assets[0].exists);
        assert!(assets.missing_required.is_empty());

        let _ = std::fs::remove_file(&target);
    }

    #[test]
    fn test_analyze_model_assets_with_missing_required_file() {
        let dir = std::env::temp_dir().join("remotemedia-model-assets-test-missing");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("tok.json");

        let manifest = Manifest {
            version: "v1".to_string(),
            metadata: ManifestMetadata {
                name: "test".to_string(),
                ..Default::default()
            },
            nodes: vec![NodeManifest {
                id: "asr".to_string(),
                node_type: "WhisperNode".to_string(),
                params: serde_json::json!({
                    "tokenizer_path": path.to_string_lossy().to_string(),
                    "model_sources": {
                        "files": [{
                            "path": path.to_string_lossy().to_string(),
                            "source": "huggingface",
                            "filename": "tok.json",
                            "required": true
                        }]
                    }
                }),
                ..Default::default()
            }],
            connections: vec![],
            python_env: None,
            plugins: Vec::new(),
        };

        let assets = manifest.analyze_model_assets(&dir).expect("analyze");
        assert_eq!(assets.assets.len(), 1);
        assert!(!assets.assets[0].exists);
        assert_eq!(assets.missing_required.len(), 1);

        let diags = manifest.node_model_diagnostics(&dir).expect("diag");
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].present, 0);
        assert_eq!(diags[0].missing_required, 1);
    }

    #[test]
    fn test_analyze_model_assets_with_optional_missing_file() {
        let dir = std::env::temp_dir().join("remotemedia-model-assets-test-opt");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("config.json");

        let manifest = Manifest {
            version: "v1".to_string(),
            metadata: ManifestMetadata {
                name: "test".to_string(),
                ..Default::default()
            },
            nodes: vec![NodeManifest {
                id: "asr".to_string(),
                node_type: "WhisperNode".to_string(),
                params: serde_json::json!({
                    "config_path": path.to_string_lossy().to_string(),
                    "model_sources": {
                        "files": [{
                            "path": path.to_string_lossy().to_string(),
                            "source": "huggingface",
                            "filename": "config.json",
                            "required": false
                        }]
                    }
                }),
                ..Default::default()
            }],
            connections: vec![],
            python_env: None,
            plugins: Vec::new(),
        };

        let assets = manifest.analyze_model_assets(&dir).expect("analyze");
        assert_eq!(assets.assets.len(), 1);
        assert!(!assets.assets[0].exists);
        assert!(assets.missing_required.is_empty());
    }

    #[test]
    fn test_model_sources_malformed_ignored() {
        let json = r#"{
            "version": "v1",
            "metadata": { "name": "test" },
            "nodes": [
                {
                    "id": "bad",
                    "node_type": "X",
                    "params": { "model_sources": "not-an-object" }
                }
            ],
            "connections": []
        }"#;

        let manifest = parse(json).expect("parse");
        let dir = std::env::temp_dir();
        let assets = manifest.analyze_model_assets(&dir).expect("analyze");
        assert!(assets.assets.is_empty());
    }
}
