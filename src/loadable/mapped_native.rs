//! Explicit host authorization for manifest-selected native mapped workers.
use super::{
    plugin_toml::{PluginNativeExecutable, PluginRustMeta},
    resolver,
};
use crate::multiprocess::native_worker::{NativeWorkerFactory, WIRE_VERSION};
use crate::{Error, Result};
use std::{
    collections::HashSet,
    path::{Component, Path, PathBuf},
};

#[derive(Debug, Clone)]
pub struct MappedNativePluginRuntime {
    trusted_roots: HashSet<PathBuf>,
}

impl MappedNativePluginRuntime {
    /// Authorize exact package roots for direct native worker launch.
    /// Canonical paths and declared hashes select artifacts; neither establishes trust.
    ///
    /// # Safety
    /// Every executable, dependency and parameter-selected code must uphold
    /// NativeWorkerFactory::for_sessions' mapping ownership, immutable publication,
    /// startup and non-delegation contract, including platform limits. The host
    /// must prevent replacement of these artifacts throughout their use. Hash
    /// validation does not freeze files or close the validation-to-launch race.
    pub unsafe fn new(trusted_roots: Vec<PathBuf>) -> Result<Self> {
        let trusted_roots = trusted_roots
            .into_iter()
            .map(|root| root.canonicalize())
            .collect::<std::io::Result<HashSet<_>>>()
            .map_err(|e| Error::ConfigError(format!("mapped native trusted root: {e}")))?;
        Ok(Self { trusted_roots })
    }

    pub(crate) fn factories(
        &self,
        metadata: &PluginRustMeta,
        root: PathBuf,
    ) -> Result<Vec<NativeWorkerFactory>> {
        let mapped = metadata.mapped.as_ref().ok_or_else(|| {
            Error::ConfigError("mapped native runtime requires [rust.mapped]".into())
        })?;
        if mapped.wire_protocol != WIRE_VERSION {
            return Err(Error::ConfigError(format!(
                "mapped native wire protocol {} is unsupported; expected {WIRE_VERSION}",
                mapped.wire_protocol
            )));
        }
        let root = root
            .canonicalize()
            .map_err(|e| Error::ConfigError(format!("mapped native package root: {e}")))?;
        if !self.trusted_roots.contains(&root) {
            return Err(Error::ConfigError(format!(
                "mapped native package root is not host-authorized: {}",
                root.display()
            )));
        }
        let names: HashSet<_> = mapped
            .workers
            .iter()
            .map(|worker| &worker.node_type)
            .collect();
        if names.is_empty()
            || names.len() != mapped.workers.len()
            || names.iter().any(|name| name.trim().is_empty())
        {
            return Err(Error::ConfigError(
                "mapped native worker node types must be nonempty and unique".into(),
            ));
        }
        if !metadata.node_types.is_empty()
            && (metadata.node_types.len() != names.len()
                || metadata.node_types.iter().collect::<HashSet<_>>() != names)
        {
            return Err(Error::ConfigError(
                "[rust].node_types must match mapped worker declarations exactly".into(),
            ));
        }
        let platform = resolver::current_platform();
        mapped
            .workers
            .iter()
            .map(|worker| {
                let artifact = worker.executables.get(&platform).ok_or_else(|| {
                    Error::ConfigError(format!(
                        "mapped native worker {} has no executable for {platform}",
                        worker.node_type
                    ))
                })?;
                let executable = resolve_executable(&root, artifact)?;
                // SAFETY: the host authorized this exact root under new's lifetime contract.
                unsafe { NativeWorkerFactory::for_sessions(executable, worker.node_type.clone()) }
                    .with_snapshot_inputs(worker.snapshot_inputs.clone())?
                    .with_snapshot_outputs(worker.snapshot_outputs.clone())
            })
            .collect()
    }
}

fn resolve_executable(root: &Path, artifact: &PluginNativeExecutable) -> Result<PathBuf> {
    let relative = Path::new(&artifact.file);
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|part| !matches!(part, Component::Normal(_) | Component::CurDir))
    {
        return Err(Error::ConfigError(
            "mapped native executable must be a relative path inside its package".into(),
        ));
    }
    let executable = root
        .join(relative)
        .canonicalize()
        .map_err(|e| Error::ConfigError(format!("mapped native executable: {e}")))?;
    if !executable.starts_with(root) || !executable.is_file() {
        return Err(Error::ConfigError(
            "mapped native executable must remain inside its package and be a file".into(),
        ));
    }
    if artifact.sha256.len() != 64 || !artifact.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::ConfigError(
            "mapped native executable sha256 must contain 64 hexadecimal digits".into(),
        ));
    }
    let actual = resolver::file_sha256(&executable)
        .map_err(|e| Error::ConfigError(format!("mapped native executable hash: {e}")))?;
    if !actual.eq_ignore_ascii_case(&artifact.sha256) {
        return Err(Error::ConfigError(format!(
            "mapped native executable SHA256 mismatch: {}",
            executable.display()
        )));
    }
    Ok(executable)
}
