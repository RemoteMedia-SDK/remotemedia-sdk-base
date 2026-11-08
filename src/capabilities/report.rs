//! Agent-facing manifest validation report (openspec
//! `add-agent-harness-integration`, requirement "Validation With Capability
//! Resolution").
//!
//! [`validate_manifest_report`] runs capability resolution
//! ([`CapabilityResolver`]) over a manifest against a node registry and
//! returns a serializable [`ManifestReport`] — the shape shared by
//! `remotemedia validate -o json`, the `rm_validate` tool, and the FFI
//! validators:
//!
//! ```json
//! {
//!   "valid": false,
//!   "errors":   [{"code": "capability_mismatch", "message": "...", "from": "mic", "to": "stt",
//!                 "suggestion": "Insert AudioResample node between 'mic' and 'stt' ..."}],
//!   "warnings": [],
//!   "connections": [{"from": "mic", "to": "stt", "ok": false,
//!                    "resolved": {"output": {"type": "audio", "sample_rate": 48000},
//!                                 "input":  {"type": "audio", "sample_rate": 16000}},
//!                    "mismatch": {"media_type": "audio", "constraint": "sample_rate",
//!                                 "source_value": "48000", "target_requirement": "16000",
//!                                 "message": "..."},
//!                    "suggestion": "Insert AudioResample node between ..."}],
//!   "unknown_node_types": []
//! }
//! ```
//!
//! Node types are recognized when the registry has a factory for them (this
//! includes plugins already loaded into it) or when the caller lists them in
//! `extra_node_types` (e.g. node types advertised by a manifest's declared
//! plugins that were not loaded). `PipelineExecutor::validate_manifest_report`
//! loads declared plugins first and then calls this function.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::constraints::{MediaCapabilities, MediaConstraints};
use super::manifest_negotiation::negotiate_manifest;
use super::resolver::CapabilityResolver;
use super::validation::{validate_connection, CapabilityMismatch};
use crate::manifest::Manifest;
use crate::nodes::StreamingNodeRegistry;

/// Machine-readable issue codes used in [`ReportIssue::code`].
pub mod codes {
    pub const UNKNOWN_NODE_TYPE: &str = "unknown_node_type";
    pub const DUPLICATE_NODE_ID: &str = "duplicate_node_id";
    pub const UNKNOWN_NODE: &str = "unknown_node";
    pub const RESOLUTION_FAILED: &str = "resolution_failed";
    pub const CAPABILITY_MISMATCH: &str = "capability_mismatch";
    /// A mismatch that `metadata.auto_negotiate` bridges by inserting a
    /// conversion node (reported as a warning).
    pub const AUTO_CONVERTED: &str = "auto_converted";
    pub const PLUGIN_LOAD_FAILED: &str = "plugin_load_failed";
}

/// Result of validating one manifest.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct ManifestReport {
    /// `true` when `errors` is empty.
    pub valid: bool,
    pub errors: Vec<ReportIssue>,
    pub warnings: Vec<ReportIssue>,
    /// One entry per manifest connection, in manifest order.
    pub connections: Vec<ConnectionReport>,
    /// Distinct node types with no registered factory, sorted.
    pub unknown_node_types: Vec<String>,
}

/// One error or warning.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct ReportIssue {
    /// See [`codes`].
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
}

impl ReportIssue {
    fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
            ..Default::default()
        }
    }
}

/// Capability status of one connection.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct ConnectionReport {
    pub from: String,
    pub to: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_port: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_port: Option<String>,
    /// No capability mismatch and both endpoints exist.
    pub ok: bool,
    /// Resolved constraints at both ends, when either side declares any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved: Option<ResolvedConnection>,
    /// First mismatch on this connection (all of them are in `errors`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mismatch: Option<MismatchReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
}

/// Constraints the resolver settled on for a connection's two ends.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct ResolvedConnection {
    /// What the source produces on the connected port.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<MediaConstraints>,
    /// What the target requires on the connected port.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<MediaConstraints>,
}

/// Serializable view of a [`CapabilityMismatch`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct MismatchReport {
    pub media_type: String,
    pub constraint: String,
    pub source_value: String,
    pub target_requirement: String,
    pub message: String,
}

impl From<&CapabilityMismatch> for MismatchReport {
    fn from(m: &CapabilityMismatch) -> Self {
        Self {
            media_type: m.media_type.clone(),
            constraint: m.constraint_name.clone(),
            source_value: m.source_value.clone(),
            target_requirement: m.target_requirement.clone(),
            message: m.display_message(),
        }
    }
}

impl ManifestReport {
    /// Record an error and keep `valid` in sync.
    pub fn push_error(&mut self, issue: ReportIssue) {
        self.errors.push(issue);
        self.valid = false;
    }
}

/// Validate `manifest` with capability resolution against `registry`.
///
/// `extra_node_types` are node types to treat as known even though
/// `registry` has no factory for them (their capabilities are then taken
/// only from the manifest's inline `media_capabilities`).
pub fn validate_manifest_report(
    manifest: &Manifest,
    registry: &StreamingNodeRegistry,
    extra_node_types: &[String],
) -> ManifestReport {
    let mut report = ManifestReport {
        valid: true,
        ..Default::default()
    };
    let extra: HashSet<&str> = extra_node_types.iter().map(String::as_str).collect();
    let is_known =
        |node_type: &str| registry.get_factory(node_type).is_some() || extra.contains(node_type);

    // ── Nodes: duplicates and unknown types ─────────────────────────────
    let mut seen: HashSet<&str> = HashSet::new();
    let mut unknown: Vec<String> = Vec::new();
    let mut nodes: Vec<(String, String)> = Vec::new();
    for node in &manifest.nodes {
        if !seen.insert(node.id.as_str()) {
            report.push_error(ReportIssue {
                node: Some(node.id.clone()),
                ..ReportIssue::new(
                    codes::DUPLICATE_NODE_ID,
                    format!("Duplicate node id '{}'", node.id),
                )
            });
            continue;
        }
        nodes.push((node.id.clone(), node.node_type.clone()));
        if !is_known(&node.node_type) {
            report.push_error(ReportIssue {
                node: Some(node.id.clone()),
                ..ReportIssue::new(
                    codes::UNKNOWN_NODE_TYPE,
                    format!(
                        "Node '{}' has unknown node type '{}'",
                        node.id, node.node_type
                    ),
                )
            });
            if !unknown.contains(&node.node_type) {
                unknown.push(node.node_type.clone());
            }
        }
    }
    unknown.sort();
    report.unknown_node_types = unknown;

    // ── Capability resolution ───────────────────────────────────────────
    let node_ids: HashSet<&str> = nodes.iter().map(|(id, _)| id.as_str()).collect();
    let edges: Vec<(String, String)> = manifest
        .connections
        .iter()
        .filter(|c| node_ids.contains(c.from.as_str()) && node_ids.contains(c.to.as_str()))
        .map(|c| (c.from.clone(), c.to.clone()))
        .collect();
    let params: HashMap<String, Value> = manifest
        .nodes
        .iter()
        .map(|n| (n.id.clone(), n.params.clone()))
        .collect();
    let resolved = match CapabilityResolver::new(registry).resolve(&nodes, &edges, &params) {
        Ok(ctx) => ctx.resolved,
        Err(e) => {
            report.push_error(ReportIssue::new(
                codes::RESOLUTION_FAILED,
                format!("Capability resolution failed: {e}"),
            ));
            HashMap::new()
        }
    };
    let caps_of = |id: &str| -> Option<MediaCapabilities> {
        resolved
            .get(id)
            .map(|r| r.capabilities.clone())
            .or_else(|| {
                manifest
                    .nodes
                    .iter()
                    .find(|n| n.id == id)
                    .and_then(|n| n.media_capabilities.clone())
            })
    };

    // Mismatches `auto_negotiate` cannot bridge (only consulted when on).
    let unbridged: Option<HashSet<(String, String)>> =
        manifest.metadata.auto_negotiate.then(|| {
            negotiate_manifest(Arc::new(manifest.clone()), registry)
                .warnings
                .into_iter()
                .map(|m| (m.source_node, m.target_node))
                .collect()
        });

    // ── Per-connection results ──────────────────────────────────────────
    for conn in &manifest.connections {
        let mut entry = ConnectionReport {
            from: conn.from.clone(),
            to: conn.to.clone(),
            from_port: conn.from_port.clone(),
            to_port: conn.to_port.clone(),
            ok: true,
            ..Default::default()
        };
        let mut missing = false;
        for end in [&conn.from, &conn.to] {
            if !node_ids.contains(end.as_str()) {
                missing = true;
                report.push_error(ReportIssue {
                    from: Some(conn.from.clone()),
                    to: Some(conn.to.clone()),
                    ..ReportIssue::new(
                        codes::UNKNOWN_NODE,
                        format!("Connection references unknown node '{end}'"),
                    )
                });
            }
        }
        if missing {
            entry.ok = false;
            report.connections.push(entry);
            continue;
        }

        let source_port = conn.from_port.as_deref().unwrap_or("default");
        let target_port = conn.to_port.as_deref().unwrap_or("default");
        let source_caps = caps_of(&conn.from).unwrap_or_default();
        let target_caps = caps_of(&conn.to).unwrap_or_default();
        let output = source_caps.outputs.get(source_port).cloned();
        let input = target_caps.inputs.get(target_port).cloned();
        if output.is_some() || input.is_some() {
            entry.resolved = Some(ResolvedConnection { output, input });
        }

        let mismatches = validate_connection(
            &conn.from,
            &source_caps,
            &conn.to,
            &target_caps,
            source_port,
            target_port,
        );
        let bridged = unbridged
            .as_ref()
            .is_some_and(|u| !u.contains(&(conn.from.clone(), conn.to.clone())));
        for m in &mismatches {
            let issue = ReportIssue {
                from: Some(conn.from.clone()),
                to: Some(conn.to.clone()),
                suggestion: m.suggestion.clone(),
                ..ReportIssue::new(
                    if bridged {
                        codes::AUTO_CONVERTED
                    } else {
                        codes::CAPABILITY_MISMATCH
                    },
                    format!(
                        "{} → {}: {} {} mismatch (source {}, target requires {})",
                        conn.from,
                        conn.to,
                        m.media_type,
                        m.constraint_name,
                        m.source_value,
                        m.target_requirement
                    ),
                )
            };
            if bridged {
                report.warnings.push(issue);
            } else {
                report.push_error(issue);
            }
        }
        if let Some(first) = mismatches.first() {
            entry.ok = false;
            entry.mismatch = Some(first.into());
            entry.suggestion = mismatches.iter().find_map(|m| m.suggestion.clone());
        }
        report.connections.push(entry);
    }

    report.valid = report.errors.is_empty();
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::{
        AudioConstraints, AudioSampleFormat, CapabilityBehavior, ConstraintValue,
    };
    use crate::manifest::{Connection, NodeManifest};
    use serde_json::json;

    fn audio(rate: u32) -> MediaConstraints {
        MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(rate)),
            channels: Some(ConstraintValue::Exact(1)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        })
    }

    /// Microphone whose output rate comes from params (`Configured`).
    struct MicFactory;
    /// Whisper-style node with a fixed 16 kHz mono input (`Static`).
    struct SttFactory;
    /// A node type with no declared capabilities.
    struct PlainFactory(&'static str);

    macro_rules! no_create {
        () => {
            fn create(
                &self,
                _node_id: String,
                _params: &Value,
                _session_id: Option<String>,
            ) -> Result<Box<dyn crate::nodes::StreamingNode>, crate::error::Error> {
                Err(crate::error::Error::Execution("test factory".into()))
            }
        };
    }

    impl crate::nodes::StreamingNodeFactory for MicFactory {
        no_create!();
        fn node_type(&self) -> &str {
            "TestMic"
        }
        fn capability_behavior(&self) -> CapabilityBehavior {
            CapabilityBehavior::Configured
        }
        fn media_capabilities(&self, params: &Value) -> Option<MediaCapabilities> {
            let rate = params["sample_rate"].as_u64().unwrap_or(48_000) as u32;
            Some(MediaCapabilities::with_output(audio(rate)))
        }
    }

    impl crate::nodes::StreamingNodeFactory for SttFactory {
        no_create!();
        fn node_type(&self) -> &str {
            "TestWhisper"
        }
        fn capability_behavior(&self) -> CapabilityBehavior {
            CapabilityBehavior::Static
        }
        fn media_capabilities(&self, _params: &Value) -> Option<MediaCapabilities> {
            Some(MediaCapabilities::with_input(audio(16_000)))
        }
    }

    impl crate::nodes::StreamingNodeFactory for PlainFactory {
        no_create!();
        fn node_type(&self) -> &str {
            self.0
        }
    }

    fn registry() -> StreamingNodeRegistry {
        let mut r = StreamingNodeRegistry::new();
        r.register(Arc::new(MicFactory));
        r.register(Arc::new(SttFactory));
        r
    }

    fn manifest(nodes: &[(&str, &str, Value)], conns: &[(&str, &str)]) -> Manifest {
        Manifest {
            nodes: nodes
                .iter()
                .map(|(id, ty, params)| NodeManifest {
                    id: id.to_string(),
                    node_type: ty.to_string(),
                    params: params.clone(),
                    ..Default::default()
                })
                .collect(),
            connections: conns
                .iter()
                .map(|(f, t)| Connection {
                    from: f.to_string(),
                    to: t.to_string(),
                    from_port: None,
                    to_port: None,
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn sample_rate_mismatch_suggests_resampler() {
        let m = manifest(
            &[
                ("mic", "TestMic", json!({"sample_rate": 48000})),
                ("stt", "TestWhisper", json!({})),
            ],
            &[("mic", "stt")],
        );
        let report = validate_manifest_report(&m, &registry(), &[]);
        assert!(!report.valid);
        assert!(report.unknown_node_types.is_empty());
        let conn = &report.connections[0];
        assert!(!conn.ok);
        let mismatch = conn.mismatch.as_ref().unwrap();
        assert_eq!(mismatch.media_type, "audio");
        assert_eq!(mismatch.constraint, "sample_rate");
        assert!(mismatch.source_value.contains("48000"));
        assert!(mismatch.target_requirement.contains("16000"));
        let suggestion = conn.suggestion.as_deref().unwrap();
        assert!(suggestion.contains("AudioResample"), "{suggestion}");
        assert_eq!(report.errors[0].code, codes::CAPABILITY_MISMATCH);
        assert!(report.errors[0].suggestion.is_some());

        let resolved = conn.resolved.as_ref().unwrap();
        assert_eq!(resolved.output, Some(audio(48_000)));
        assert_eq!(resolved.input, Some(audio(16_000)));

        // Wire shape matches the tool contract.
        let v = serde_json::to_value(&report).unwrap();
        for key in [
            "valid",
            "errors",
            "warnings",
            "connections",
            "unknown_node_types",
        ] {
            assert!(v.get(key).is_some(), "missing {key}");
        }
        assert_eq!(v["connections"][0]["from"], "mic");
        assert_eq!(v["connections"][0]["ok"], false);
        assert_eq!(v["connections"][0]["resolved"]["output"]["type"], "audio");
    }

    #[test]
    fn matching_rates_are_ok() {
        let m = manifest(
            &[
                ("mic", "TestMic", json!({"sample_rate": 16000})),
                ("stt", "TestWhisper", json!({})),
            ],
            &[("mic", "stt")],
        );
        let report = validate_manifest_report(&m, &registry(), &[]);
        assert!(report.valid, "{report:?}");
        assert!(report.connections[0].ok);
        assert!(report.connections[0].mismatch.is_none());
    }

    #[test]
    fn registry_and_plugin_types_are_not_unknown() {
        let mut reg = registry();
        // A type a plugin registered into the registry.
        reg.register(Arc::new(PlainFactory("Whisper")));
        let m = manifest(
            &[
                ("a", "Whisper", json!({})),
                ("b", "DeclaredByPlugin", json!({})),
                ("c", "Nope", json!({})),
                ("d", "Nope", json!({})),
            ],
            &[("a", "b"), ("b", "c"), ("c", "ghost")],
        );
        let report = validate_manifest_report(&m, &reg, &["DeclaredByPlugin".to_string()]);
        assert_eq!(report.unknown_node_types, vec!["Nope".to_string()]);
        assert!(!report.valid);
        assert_eq!(
            report
                .errors
                .iter()
                .filter(|e| e.code == codes::UNKNOWN_NODE_TYPE)
                .count(),
            2
        );
        assert!(report
            .errors
            .iter()
            .any(|e| e.code == codes::UNKNOWN_NODE && e.message.contains("ghost")));
        assert!(report.connections[0].ok && report.connections[1].ok);
        assert!(!report.connections[2].ok);
    }

    #[test]
    fn auto_negotiate_turns_bridgeable_mismatch_into_warning() {
        let mut m = manifest(
            &[
                ("mic", "TestMic", json!({"sample_rate": 48000})),
                ("stt", "TestWhisper", json!({})),
            ],
            &[("mic", "stt")],
        );
        m.metadata.auto_negotiate = true;
        let report = validate_manifest_report(&m, &registry(), &[]);
        // The connection itself still mismatches as authored...
        assert!(report.connections[0].mismatch.is_some());
        // ...but negotiation inserts a resampler, so it is not an error.
        assert!(report.valid, "{report:?}");
        let warning = &report.warnings[0];
        assert_eq!(warning.code, codes::AUTO_CONVERTED);
        assert!(warning
            .suggestion
            .as_deref()
            .unwrap()
            .contains("AudioResample"));
    }

    #[test]
    fn duplicate_ids_are_errors() {
        let m = manifest(
            &[("a", "TestMic", json!({})), ("a", "TestMic", json!({}))],
            &[],
        );
        let report = validate_manifest_report(&m, &registry(), &[]);
        assert!(!report.valid);
        assert_eq!(report.errors[0].code, codes::DUPLICATE_NODE_ID);
    }
}
