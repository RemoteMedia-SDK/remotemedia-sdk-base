//! `plugin.toml` schema — the standardized metadata file every plugin
//! repo MAY publish at its root.
//!
//! Required for Python source-load plugins (no cdylib to inspect, so
//! the resolver needs out-of-band metadata to know what node types are
//! registered + what Python deps to install). Optional for Rust
//! cdylib plugins (the cdylib is self-describing via abi_stable
//! factory enumeration) — when present, it just lets the resolver
//! validate / pre-display node types without dlopen'ing.
//!
//! ## Schema
//!
//! ```toml
//! [plugin]
//! name = "echo-python-source"
//! version = "0.2.0"
//! language = "python"          # "python" | "rust"
//!
//! # Required when language = "python".
//! [python]
//! entry_module = "echo_python"          # which .py to import
//! node_types = ["EchoPythonNode"]       # what types this plugin registers
//! requires = []                          # PEP 723-equivalent dep list
//! module_root = "."                      # repo-relative path containing entry_module
//!
//! # Required when language = "rust".
//! [rust]
//! node_types = ["EchoNode"]
//! asset_pattern = "lib{name}-{arch}-{os}.{ext}"  # {ext} = so|dylib|dll
//! ```
//!
//! The schema is permissive — unknown top-level tables / fields are
//! ignored so forward-compat additions don't break older readers.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Top-level `plugin.toml` document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginToml {
    /// `[plugin]` — universal metadata, always present.
    pub plugin: PluginMeta,

    /// `[python]` — present when `plugin.language = "python"`.
    /// Validated post-parse (`PluginToml::validate`) since serde
    /// can't express "this table required when discriminant X" cleanly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub python: Option<PluginPythonMeta>,

    /// `[rust]` — present when `plugin.language = "rust"`. Optional
    /// even then (the cdylib is self-describing via abi_stable, but
    /// the metadata helps the resolver pre-validate).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rust: Option<PluginRustMeta>,

    /// Unknown tables — captured into a map so author-added fields
    /// like `[ci]`, `[docs]`, `[license]` round-trip cleanly without
    /// breaking the parser.
    #[serde(flatten)]
    pub extra: BTreeMap<String, toml::Value>,
}

/// `[plugin]` table — name / version / language discriminant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginMeta {
    /// Plugin name — informational. The resolver's spec parser
    /// derives the GitHub repo name from the manifest spec, not from
    /// here, so this field is for display / debugging only.
    pub name: String,

    /// Plugin version — informational, matches the git tag.
    pub version: String,

    /// `"python"` or `"rust"`. Drives which sub-table is required.
    pub language: PluginLanguage,

    /// Optional human-readable description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Optional author string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,

    /// Optional license identifier (SPDX recommended).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
}

/// `plugin.language` discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PluginLanguage {
    Python,
    Rust,
}

/// `[python]` table — required when `plugin.language = "python"`.
///
/// The resolver downloads the plugin repo as a tarball, extracts it
/// into the project-local cache, then uses these fields to drive the
/// runner subprocess:
///
/// - `entry_module` is passed as `--register-module` (the runner's
///   `__init__` imports it before looking up `node_types`).
/// - `module_root` (default `"."`) is passed as `--module-root` — the
///   absolute path it expands to is added to `sys.path` so
///   `entry_module` resolves.
/// - `requires` is the PEP 723-equivalent dep list given to
///   `PythonEnvManager::ensure_env` to provision a managed venv.
/// - `node_types` is registered into the executor's
///   `StreamingNodeRegistry` so the manifest's `node_type` references
///   resolve.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginPythonMeta {
    /// Bare Python module name to import (e.g. `"echo_python"` for an
    /// `echo_python.py` file at `module_root`).
    pub entry_module: String,

    /// Node types this plugin registers. Must be non-empty.
    #[serde(default)]
    pub node_types: Vec<String>,

    /// PEP 723-equivalent dep list (e.g. `["torch>=2.1", "transformers>=5.0"]`).
    /// Empty means "no external deps — runner alone is enough".
    #[serde(default)]
    pub requires: Vec<String>,

    /// Repo-relative path containing `entry_module`. Defaults to repo
    /// root. Use this when your plugin's Python source lives in a
    /// subdir (e.g. `"src/"`).
    #[serde(default = "default_module_root")]
    pub module_root: String,
}

fn default_module_root() -> String {
    ".".to_string()
}

/// `[rust]` table — optional when `plugin.language = "rust"`.
///
/// Mostly informational today since Rust plugins are self-describing
/// (the cdylib's abi_stable factory enumeration is authoritative).
/// `asset_pattern` is reserved for future tooling that constructs
/// asset URLs without round-tripping through `release-manifest.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginRustMeta {
    /// Node types this plugin registers. Informational —
    /// authoritative source is the cdylib's factory list.
    #[serde(default)]
    pub node_types: Vec<String>,

    /// Optional filename pattern template for matrix-built assets.
    /// Tokens: `{name}` / `{version}` / `{arch}` / `{os}` / `{ext}`.
    /// Resolver doesn't use this in Phase 1B (it reads
    /// `release-manifest.json` instead); reserved for v2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asset_pattern: Option<String>,
}

/// Errors when parsing or validating a `plugin.toml`.
#[derive(Debug, thiserror::Error)]
pub enum PluginTomlError {
    #[error("plugin.toml is not valid TOML: {0}")]
    BadToml(#[from] toml::de::Error),

    #[error(
        "plugin.toml has `language = \"python\"` but no `[python]` table. \
         Add: \n\n[python]\nentry_module = \"...\"\nnode_types = [\"...\"]"
    )]
    MissingPythonTable,

    #[error("plugin.toml `[python].node_types` must be non-empty")]
    EmptyPythonNodeTypes,

    #[error(
        "plugin.toml has `language = \"rust\"` — Rust plugins ship as \
         cdylibs distributed via GitHub releases. Use the release-manifest.json \
         path instead (see resolver Phase 1B docs)."
    )]
    RustPluginWrongPath,
}

impl PluginToml {
    /// Parse a `plugin.toml` document. Validates that the discriminant +
    /// required sub-tables are coherent.
    pub fn parse(source: &str) -> Result<Self, PluginTomlError> {
        let doc: Self = toml::from_str(source)?;
        doc.validate()?;
        Ok(doc)
    }

    /// Validate the discriminant matches the present sub-tables and
    /// the required sub-tables are non-empty.
    pub fn validate(&self) -> Result<(), PluginTomlError> {
        match self.plugin.language {
            PluginLanguage::Python => {
                let py = self
                    .python
                    .as_ref()
                    .ok_or(PluginTomlError::MissingPythonTable)?;
                if py.node_types.is_empty() {
                    return Err(PluginTomlError::EmptyPythonNodeTypes);
                }
                Ok(())
            }
            PluginLanguage::Rust => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_python_plugin() {
        let src = r#"
            [plugin]
            name = "echo-python-source"
            version = "0.2.0"
            language = "python"

            [python]
            entry_module = "echo_python"
            node_types = ["EchoPythonNode"]
        "#;
        let doc = PluginToml::parse(src).unwrap();
        assert_eq!(doc.plugin.name, "echo-python-source");
        assert_eq!(doc.plugin.version, "0.2.0");
        assert_eq!(doc.plugin.language, PluginLanguage::Python);
        let py = doc.python.unwrap();
        assert_eq!(py.entry_module, "echo_python");
        assert_eq!(py.node_types, vec!["EchoPythonNode"]);
        assert!(py.requires.is_empty());
        assert_eq!(py.module_root, "."); // default
    }

    #[test]
    fn parse_python_plugin_with_full_metadata() {
        let src = r#"
            [plugin]
            name = "moss-tts-source"
            version = "0.3.0"
            language = "python"
            description = "MOSS TTS as a source-load Python plugin"
            author = "Jane Doe"
            license = "MIT"

            [python]
            entry_module = "moss_tts_realtime"
            node_types = ["MossTTSRealtimeNode"]
            requires = ["torch>=2.1", "transformers>=5.0", "accelerate>=0.33"]
            module_root = "src"
        "#;
        let doc = PluginToml::parse(src).unwrap();
        assert_eq!(doc.plugin.author.as_deref(), Some("Jane Doe"));
        assert_eq!(doc.plugin.license.as_deref(), Some("MIT"));
        let py = doc.python.unwrap();
        assert_eq!(py.requires.len(), 3);
        assert_eq!(py.module_root, "src");
    }

    #[test]
    fn parse_rust_plugin() {
        let src = r#"
            [plugin]
            name = "silero-vad-loadable"
            version = "1.0.0"
            language = "rust"

            [rust]
            node_types = ["SileroVADNode"]
            asset_pattern = "lib{name}-{arch}-{os}.{ext}"
        "#;
        let doc = PluginToml::parse(src).unwrap();
        assert_eq!(doc.plugin.language, PluginLanguage::Rust);
        assert!(doc.python.is_none());
        let rs = doc.rust.unwrap();
        assert_eq!(rs.node_types, vec!["SileroVADNode"]);
        assert_eq!(
            rs.asset_pattern.as_deref(),
            Some("lib{name}-{arch}-{os}.{ext}")
        );
    }

    #[test]
    fn parse_python_plugin_missing_python_table_rejected() {
        let src = r#"
            [plugin]
            name = "broken"
            version = "0.1"
            language = "python"
        "#;
        let err = PluginToml::parse(src).unwrap_err();
        assert!(matches!(err, PluginTomlError::MissingPythonTable));
    }

    #[test]
    fn parse_python_plugin_empty_node_types_rejected() {
        let src = r#"
            [plugin]
            name = "broken"
            version = "0.1"
            language = "python"

            [python]
            entry_module = "foo"
            node_types = []
        "#;
        let err = PluginToml::parse(src).unwrap_err();
        assert!(matches!(err, PluginTomlError::EmptyPythonNodeTypes));
    }

    #[test]
    fn parse_unknown_top_level_table_preserved() {
        // Authors may add [ci], [docs], etc. — must not break parsing.
        let src = r#"
            [plugin]
            name = "future-proof"
            version = "0.1"
            language = "python"

            [python]
            entry_module = "x"
            node_types = ["X"]

            [ci]
            github_actions = true

            [docs]
            url = "https://example.com"
        "#;
        let doc = PluginToml::parse(src).unwrap();
        assert!(doc.extra.contains_key("ci"));
        assert!(doc.extra.contains_key("docs"));
    }
}
