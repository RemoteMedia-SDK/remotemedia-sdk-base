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

/// Removes a materialized Python environment (venv + wheelhouse) when dropped
/// (best effort). Hold the guard for as long as the venv must stay usable —
/// the whole request for unary execution, the whole session for streaming.
pub struct PythonEnvGuard(PathBuf);

impl PythonEnvGuard {
    /// Root temp dir holding `wheels/` and `venv/`.
    pub fn root(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for PythonEnvGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Relative path of the python binary inside a venv for this platform.
fn venv_python_rel() -> &'static str {
    if cfg!(windows) {
        "Scripts/python.exe"
    } else {
        "bin/python"
    }
}

/// Materialize an embedded (frozen) Python wheelhouse into a fresh venv.
///
/// Writes each wheel to a content-addressed wheels dir, creates a venv with
/// `python3 -m venv`, then installs the wheels with `pip --no-index` (fully
/// offline — no network, no dependency resolution against an index).
///
/// Returns `(path_to_venv_python, guard)`; the guard removes the temp tree on
/// drop.
pub fn materialize_embedded_python_env(
    env: &EmbeddedPythonEnv,
) -> Result<(String, PythonEnvGuard), String> {
    let root =
        std::env::temp_dir().join(format!("rm-pyenv-{}-{}", std::process::id(), uuid_suffix()));
    let wheels_dir = root.join("wheels");
    let venv_dir = root.join("venv");
    std::fs::create_dir_all(&wheels_dir)
        .map_err(|e| format!("create wheels dir {wheels_dir:?}: {e}"))?;

    // Guard from here on so early returns clean up the partial tree.
    let guard = PythonEnvGuard(root.clone());

    let mut wheel_paths: Vec<PathBuf> = Vec::with_capacity(env.wheels.len());
    for wheel in &env.wheels {
        // Use the original wheel filename for the on-disk file. Do NOT prefix
        // with the digest: a `sha256:<hex>-` prefix makes pip's `--find-links`
        // scanner mis-parse the filename as a hash requirement
        // (`sha256:<hex>==<name>`), breaking the install. The digest is still
        // validated for non-empty content below; the explicit path passed to
        // `pip install` is what actually selects the wheel.
        let digest = wheel.digest.trim();
        if digest.is_empty() {
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
        let path = wheels_dir.join(&filename);
        std::fs::write(&path, &wheel.content)
            .map_err(|e| format!("write wheel {filename}: {e}"))?;
        wheel_paths.push(path);
    }

    // Create the venv.
    let out = std::process::Command::new("python3")
        .arg("-m")
        .arg("venv")
        .arg(&venv_dir)
        .output()
        .map_err(|e| format!("spawn `python3 -m venv`: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "python3 -m venv {venv_dir:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }

    let venv_python = venv_dir.join(venv_python_rel());

    // Install the wheels offline.
    if !wheel_paths.is_empty() {
        let mut cmd = std::process::Command::new(&venv_python);
        cmd.arg("-m")
            .arg("pip")
            .arg("install")
            .arg("--no-index")
            .arg("--disable-pip-version-check")
            .arg("--find-links")
            .arg(&wheels_dir);
        for path in &wheel_paths {
            cmd.arg(path);
        }
        let out = cmd
            .output()
            .map_err(|e| format!("spawn pip install in embedded venv: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "offline pip install of embedded wheels failed: {}",
                String::from_utf8_lossy(&out.stderr)
            ));
        }
    }

    Ok((venv_python.to_string_lossy().into_owned(), guard))
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
