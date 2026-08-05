//! Inline ("embedded") native plugin blob handling.
//!
//! Portable pipeline bundles freeze native plugin `.so` blobs into the
//! bundle. When a client runs a bundle without a prior deploy, it ships those
//! blobs inline with the request. The server materializes each blob into a
//! content-addressed temp dir and rewrites the manifest's `embedded:<digest>`
//! plugin specs to point at the written files, so the executor can dlopen the
//! plugin via its normal `ensure_plugins_loaded` path.

use std::collections::HashMap;
use std::path::PathBuf;

use remotemedia_core::manifest::{Manifest, PluginSpec, PluginSpecExplicit};
use remotemedia_core::transport::client::EmbeddedPythonEnv;

use crate::generated::EmbeddedPluginBlob;

/// Removes a content-addressed temp dir when dropped (best effort). The
/// plugin is dlopened before this drops, so removing the file is safe. Hold
/// the guard for as long as the plugin must remain loaded.
pub struct CasGuard(PathBuf);

impl Drop for CasGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Materialize inline plugin blobs into a temp dir and rewrite any
/// `embedded:<digest>` plugin specs in `manifest` to point at the written
/// files. Returns a guard that cleans up the temp dir on drop (keep it alive
/// for the lifetime the plugin must stay loaded).
pub fn materialize_embedded_plugins(
    blobs: &[EmbeddedPluginBlob],
    manifest: &mut Manifest,
) -> Result<CasGuard, String> {
    let cas = std::env::temp_dir().join(format!(
        "rm-embedded-{}-{}",
        std::process::id(),
        uuid_suffix()
    ));
    std::fs::create_dir_all(&cas).map_err(|e| format!("create cas dir {cas:?}: {e}"))?;

    let mut digest_to_path: HashMap<String, PathBuf> = HashMap::new();
    for blob in blobs {
        let digest = blob.digest.trim().to_string();
        if digest.is_empty() {
            return Err("embedded plugin blob with empty digest".to_string());
        }
        let path = cas.join(format!("{digest}.so"));
        std::fs::write(&path, &blob.content).map_err(|e| format!("write plugin {digest}: {e}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
                .map_err(|e| format!("chmod plugin {digest}: {e}"))?;
        }
        digest_to_path.insert(digest, path);
    }

    for spec in &mut manifest.plugins {
        if let Some(digest) = embedded_digest_of(spec) {
            match digest_to_path.get(&digest) {
                Some(path) => {
                    *spec = PluginSpec::Explicit(PluginSpecExplicit {
                        path: Some(path.to_string_lossy().into_owned()),
                        ..Default::default()
                    });
                }
                None => {
                    return Err(format!(
                        "manifest references embedded plugin {digest} but no matching blob was supplied"
                    ));
                }
            }
        }
    }

    Ok(CasGuard(cas))
}

/// Extract the digest from a `embedded:<sha256>` plugin spec (shorthand or
/// explicit `name` form).
pub fn embedded_digest_of(spec: &PluginSpec) -> Option<String> {
    let token = match spec {
        PluginSpec::Shorthand(s) => s.clone(),
        PluginSpec::Explicit(e) => e.name.clone().unwrap_or_default(),
    };
    token
        .strip_prefix("embedded:")
        .map(|d| d.trim().to_string())
        .filter(|d| !d.is_empty())
}

/// Wires a shipped (frozen) wheelhouse into the managed uv environment system
/// for the lifetime of a request or stream session.
///
/// Materialization writes the wheels to a persistent, content-addressed cache
/// dir and prepends that dir to the process-wide `UV_FIND_LINKS`, which `uv`
/// consults natively when resolving dependencies. The guard restores the
/// previous `UV_FIND_LINKS` value on drop so the wiring stays request-scoped.
///
/// The guard deliberately does NOT delete the wheel cache dir: it is
/// content-addressed by `wheel_set_digest` and reused across requests.
pub struct PythonEnvGuard {
    wheels_dir: PathBuf,
    prev_find_links: Option<std::ffi::OsString>,
}

impl PythonEnvGuard {
    /// Directory holding the extracted wheels (the `--find-links` source).
    pub fn wheels_dir(&self) -> &std::path::Path {
        &self.wheels_dir
    }

    /// Backwards-compatible alias for [`Self::wheels_dir`].
    pub fn root(&self) -> &std::path::Path {
        &self.wheels_dir
    }
}

impl Drop for PythonEnvGuard {
    fn drop(&mut self) {
        match &self.prev_find_links {
            Some(prev) => std::env::set_var(UV_FIND_LINKS, prev),
            None => std::env::remove_var(UV_FIND_LINKS),
        }
    }
}

/// Env var `uv` reads for additional local/remote wheel sources.
const UV_FIND_LINKS: &str = "UV_FIND_LINKS";

/// Root of the persistent wheel cache: `~/.cache/remotemedia/python-wheels`.
fn wheel_cache_root() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    PathBuf::from(home)
        .join(".cache")
        .join("remotemedia")
        .join("python-wheels")
}

/// Normalize the distribution portion of a wheel filename per PEP 503/427.
///
/// Newer pip/uv reject wheels whose distribution name is not normalized
/// (`-` and `.` must be `_` in the filename's first segment).
fn normalized_wheel_filename(filename: &str) -> String {
    // Wheel filenames are `{dist}-{version}-...`. The distribution part may
    // itself contain hyphens/dots, so the version boundary is the first `-`
    // followed by a digit.
    let bytes = filename.as_bytes();
    let split = (0..bytes.len()).find(|&i| {
        bytes[i] == b'-'
            && bytes
                .get(i + 1)
                .map(|c| c.is_ascii_digit())
                .unwrap_or(false)
    });
    match split {
        Some(i) => {
            let dist = filename[..i].replace(['-', '.'], "_");
            format!("{dist}{}", &filename[i..])
        }
        None => filename.to_string(),
    }
}

/// Materialize an embedded (frozen) Python wheelhouse as an offline
/// `--find-links` source for the managed uv environment system.
///
/// Writes every shipped wheel into
/// `~/.cache/remotemedia/python-wheels/<wheel_set_digest>/` (persistent,
/// content-addressed, reused across requests), then prepends that dir to the
/// process `UV_FIND_LINKS` so `uv pip install` resolves the shipped wheels
/// without touching the network.
///
/// Returns `(wheels_dir, guard)`. The guard restores the previous
/// `UV_FIND_LINKS` on drop; hold it for the request / session lifetime.
pub fn materialize_embedded_python_env(
    env: &EmbeddedPythonEnv,
) -> Result<(PathBuf, PythonEnvGuard), String> {
    let digest = env.wheel_set_digest.trim().replace(['/', ':'], "_");
    if digest.is_empty() {
        return Err("embedded python env has empty wheel_set_digest".to_string());
    }
    if env.wheels.is_empty() {
        return Err("embedded python env contains no wheels".to_string());
    }

    let wheels_dir = wheel_cache_root().join(&digest);
    std::fs::create_dir_all(&wheels_dir)
        .map_err(|e| format!("create wheel cache dir {wheels_dir:?}: {e}"))?;

    for wheel in &env.wheels {
        if wheel.digest.trim().is_empty() {
            return Err(format!(
                "embedded python wheel '{}' has empty digest",
                wheel.filename
            ));
        }
        let filename = if wheel.filename.trim().is_empty() {
            format!("{}.whl", wheel.name)
        } else {
            wheel.filename.clone()
        };
        let filename = if filename.ends_with(".whl") {
            normalized_wheel_filename(&filename)
        } else {
            format!("{}.whl", normalized_wheel_filename(&filename))
        };
        let path = wheels_dir.join(&filename);
        std::fs::write(&path, &wheel.content)
            .map_err(|e| format!("write wheel {filename}: {e}"))?;
    }

    let guard = wire_uv_find_links(&wheels_dir);
    Ok((wheels_dir, guard))
}

/// Prepend `wheels_dir` to the process `UV_FIND_LINKS`, returning a guard that
/// restores the previous value on drop.
fn wire_uv_find_links(wheels_dir: &std::path::Path) -> PythonEnvGuard {
    let prev = std::env::var_os(UV_FIND_LINKS);
    let entry = wheels_dir.to_string_lossy().into_owned();
    let combined = match prev.as_ref().map(|v| v.to_string_lossy().into_owned()) {
        Some(existing) if !existing.trim().is_empty() => {
            if existing.split(' ').any(|p| p == entry) {
                existing
            } else {
                format!("{entry} {existing}")
            }
        }
        _ => entry,
    };
    std::env::set_var(UV_FIND_LINKS, combined);
    PythonEnvGuard {
        wheels_dir: wheels_dir.to_path_buf(),
        prev_find_links: prev,
    }
}

/// Convert the wire (proto) embedded python env into the decoded core type.
pub fn decode_embedded_python_env(py: &crate::generated::EmbeddedPythonEnv) -> EmbeddedPythonEnv {
    use remotemedia_core::transport::client::{EmbeddedInterpreter, EmbeddedWheel};
    EmbeddedPythonEnv {
        interpreter: EmbeddedInterpreter {
            implementation: py
                .interpreter
                .as_ref()
                .map(|i| i.implementation.clone())
                .unwrap_or_default(),
            version: py
                .interpreter
                .as_ref()
                .map(|i| i.version.clone())
                .unwrap_or_default(),
            abi: py
                .interpreter
                .as_ref()
                .map(|i| i.abi.clone())
                .unwrap_or_default(),
            accelerator: py
                .interpreter
                .as_ref()
                .map(|i| i.accelerator.clone())
                .unwrap_or_default(),
        },
        wheel_set_digest: py.wheel_set_digest.clone(),
        wheels: py
            .wheels
            .iter()
            .map(|w| EmbeddedWheel {
                name: w.name.clone(),
                filename: w.filename.clone(),
                digest: w.digest.clone(),
                content: w.content.clone(),
            })
            .collect(),
    }
}

/// Small entropy suffix to avoid temp-dir collisions.
fn uuid_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use remotemedia_core::transport::client::{EmbeddedInterpreter, EmbeddedWheel};

    #[test]
    fn normalizes_distribution_part_of_wheel_filename() {
        assert_eq!(
            normalized_wheel_filename("my-pkg-1.0.0-py3-none-any.whl"),
            "my_pkg-1.0.0-py3-none-any.whl"
        );
        assert_eq!(
            normalized_wheel_filename("numpy-2.0.0-cp311-cp311-linux_x86_64.whl"),
            "numpy-2.0.0-cp311-cp311-linux_x86_64.whl"
        );
    }

    #[test]
    fn materialize_writes_wheels_and_wires_find_links() {
        let tmp = std::env::temp_dir().join(format!("rm-pyenv-test-{}", uuid_suffix()));
        std::fs::create_dir_all(&tmp).unwrap();
        std::env::set_var("HOME", &tmp);
        std::env::remove_var(UV_FIND_LINKS);

        let env = EmbeddedPythonEnv {
            interpreter: EmbeddedInterpreter {
                implementation: "cpython".to_string(),
                version: "3.11".to_string(),
                abi: "cp311".to_string(),
                accelerator: String::new(),
            },
            wheel_set_digest: "deadbeef".to_string(),
            wheels: vec![EmbeddedWheel {
                name: "my-pkg".to_string(),
                filename: "my-pkg-1.0.0-py3-none-any.whl".to_string(),
                digest: "abc123".to_string(),
                content: b"dummy-wheel".to_vec(),
            }],
        };

        let (dir, guard) = materialize_embedded_python_env(&env).expect("materialize");
        assert!(dir.ends_with("deadbeef"));
        let written = dir.join("my_pkg-1.0.0-py3-none-any.whl");
        assert_eq!(std::fs::read(&written).unwrap(), b"dummy-wheel".to_vec());
        assert_eq!(
            std::env::var(UV_FIND_LINKS).unwrap(),
            dir.to_string_lossy().into_owned()
        );

        drop(guard);
        // Guard restores (here: clears) UV_FIND_LINKS but keeps the cache dir.
        assert!(std::env::var_os(UV_FIND_LINKS).is_none());
        assert!(written.exists());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn materialize_rejects_empty_wheelhouse() {
        let env = EmbeddedPythonEnv {
            interpreter: EmbeddedInterpreter::default(),
            wheel_set_digest: "cafe".to_string(),
            wheels: Vec::new(),
        };
        assert!(materialize_embedded_python_env(&env).is_err());
    }
}
