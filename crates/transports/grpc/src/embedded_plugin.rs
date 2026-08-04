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
        std::fs::write(&path, &blob.content)
            .map_err(|e| format!("write plugin {digest}: {e}"))?;
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

/// Small entropy suffix to avoid temp-dir collisions.
fn uuid_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}")
}
