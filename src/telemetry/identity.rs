//! Node build identity for [`super::SessionDescriptor`] (openspec
//! `add-agent-harness-integration`, requirement "Node Build Identity").
//!
//! Lets an observer (or an agent) tell exactly which build of a node a
//! session ran: built-in vs. loadable plugin vs. Python, the plugin's name
//! and version, a content digest of the loaded library / Python source, and
//! a key-order-independent hash of the node's effective params.

use super::redact::redact_params;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::Path;

/// Where a node's implementation comes from.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ImplementationKind {
    /// Compiled into the host binary.
    #[default]
    Builtin,
    /// Loaded from a native plugin library (`.so` / `.dll` / `.dylib`).
    Loadable,
    /// Runs Python code (multiprocess node or Python source plugin).
    Python,
}

/// Implementation identity of one node type, as registered.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct NodeImplementation {
    pub kind: ImplementationKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plugin_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plugin_version: Option<String>,
    /// Content digest of the loaded library or Python source,
    /// `sha256:<hex>`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    /// Unix epoch milliseconds at which the implementation was loaded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub loaded_at_ms: Option<u64>,
}

impl NodeImplementation {
    pub fn builtin() -> Self {
        Self::default()
    }

    pub fn python() -> Self {
        Self {
            kind: ImplementationKind::Python,
            ..Self::default()
        }
    }

    /// Identity of a native plugin library at `path`, loaded now. Reads the
    /// file once to compute its digest. Name / version come from a
    /// `plugin.toml` beside the library when present, else the name is the
    /// library file stem (without a `lib` prefix).
    pub fn loadable_from_path(path: &Path) -> Self {
        let (toml_name, toml_version) = path
            .parent()
            .map(|dir| dir.join("plugin.toml"))
            .and_then(|p| std::fs::read_to_string(p).ok())
            .and_then(|src| src.parse::<toml::Table>().ok())
            .and_then(|t| t.get("plugin").and_then(|p| p.as_table()).cloned())
            .map(|p| {
                let field = |k: &str| p.get(k).and_then(|v| v.as_str()).map(str::to_string);
                (field("name"), field("version"))
            })
            .unwrap_or((None, None));
        let stem_name = path.file_stem().and_then(|s| s.to_str()).map(|s| {
            s.strip_prefix("lib")
                .filter(|rest| !rest.is_empty())
                .unwrap_or(s)
                .to_string()
        });
        Self {
            kind: ImplementationKind::Loadable,
            plugin_name: toml_name.or(stem_name),
            plugin_version: toml_version.filter(|v| !v.is_empty()),
            digest: file_digest(path).ok(),
            loaded_at_ms: Some(super::unix_ms()),
        }
    }
}

/// `sha256:<hex>` of the bytes of the file at `path`.
pub fn file_digest(path: &Path) -> std::io::Result<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
}

/// Stable hash of a node's effective params: `sha256:<hex>` over a canonical
/// JSON encoding (object keys sorted at every level), computed after
/// redaction so secrets contribute only the redaction marker.
pub fn params_hash(params: &Value) -> String {
    let mut canonical = String::new();
    write_canonical(&redact_params(params), &mut canonical);
    format!(
        "sha256:{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    )
}

fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(key.clone()).to_string());
                out.push(':');
                write_canonical(&map[key], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn params_hash_ignores_key_order() {
        let a: Value =
            serde_json::from_str(r#"{"model":"m","nested":{"x":1,"y":[1,{"b":2,"a":1}]},"t":0.5}"#)
                .unwrap();
        let b: Value =
            serde_json::from_str(r#"{"t":0.5,"nested":{"y":[1,{"a":1,"b":2}],"x":1},"model":"m"}"#)
                .unwrap();
        assert_eq!(params_hash(&a), params_hash(&b));
        assert!(params_hash(&a).starts_with("sha256:"));
        // Array order is significant.
        let c = json!({"model":"m","nested":{"x":1,"y":[{"b":2,"a":1},1]},"t":0.5});
        assert_ne!(params_hash(&a), params_hash(&c));
        assert_ne!(params_hash(&a), params_hash(&json!({"model":"other"})));
    }

    #[test]
    fn redacted_params_hash_by_marker_not_value() {
        let one = json!({"model":"m","api_key":"sk-one"});
        let two = json!({"model":"m","api_key":"sk-two"});
        assert_eq!(params_hash(&one), params_hash(&two));
        let marker = json!({"model":"m","api_key": super::super::redact::REDACTED});
        assert_eq!(params_hash(&one), params_hash(&marker));
        // A present secret still differs from an absent one.
        assert_ne!(params_hash(&one), params_hash(&json!({"model":"m"})));
    }

    #[test]
    fn digest_changes_when_file_bytes_change() {
        let dir = tempfile::tempdir().unwrap();
        let lib = dir.path().join("libdemo_plugin.so");
        std::fs::write(&lib, b"build one").unwrap();
        let first = NodeImplementation::loadable_from_path(&lib);
        assert_eq!(first.kind, ImplementationKind::Loadable);
        assert_eq!(first.plugin_name.as_deref(), Some("demo_plugin"));
        assert!(first.loaded_at_ms.is_some());
        let d1 = first.digest.clone().unwrap();
        assert_eq!(d1, file_digest(&lib).unwrap());

        std::fs::write(&lib, b"build two").unwrap();
        let d2 = NodeImplementation::loadable_from_path(&lib).digest.unwrap();
        assert_ne!(d1, d2);
    }

    #[test]
    fn plugin_toml_beside_library_names_the_plugin() {
        let dir = tempfile::tempdir().unwrap();
        let lib = dir.path().join("whatever.dll");
        std::fs::write(&lib, b"x").unwrap();
        std::fs::write(
            dir.path().join("plugin.toml"),
            "[plugin]\nname = \"demo\"\nversion = \"1.2.3\"\n",
        )
        .unwrap();
        let id = NodeImplementation::loadable_from_path(&lib);
        assert_eq!(id.plugin_name.as_deref(), Some("demo"));
        assert_eq!(id.plugin_version.as_deref(), Some("1.2.3"));
    }

    #[test]
    fn builtin_identity_serializes_compactly() {
        let v = serde_json::to_value(NodeImplementation::builtin()).unwrap();
        assert_eq!(v, json!({"kind":"builtin"}));
    }
}
