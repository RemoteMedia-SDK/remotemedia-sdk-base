//! Host authorization and manifest selection for mapped source Python plugins.
use super::plugin_toml::PluginPythonMeta;
use crate::multiprocess::mapped_python::{
    MappedPythonConfig, MappedPythonFactory, MappedPythonWheels,
};
use crate::{Error, Result};
use remotemedia_py_env::PythonEnvConfig;
use std::{collections::HashSet, path::PathBuf};

/// Runtime artifacts and exact package roots authorized by the embedding host.
/// A plugin declaration is a capability request, never a trust grant.
#[derive(Debug, Clone)]
pub struct MappedPythonPluginRuntime {
    wheels: MappedPythonWheels,
    environment: PythonEnvConfig,
    trusted_roots: HashSet<PathBuf>,
}

impl MappedPythonPluginRuntime {
    /// Authorize automatic registration for these exact resolved package roots.
    /// Roots are canonicalized now and again when loading each plugin. This is
    /// identity selection, not an integrity check or a filesystem sandbox.
    ///
    /// # Safety
    /// All code, dependencies, parameter-selected code and runtime artifacts
    /// executed from these packages must uphold MappedPythonFactory::for_sessions'
    /// immutable-publication, final-unmap and non-delegation contract throughout
    /// execution. The caller must prevent replacement of those trusted artifacts.
    pub unsafe fn new(
        wheels: MappedPythonWheels,
        environment: PythonEnvConfig,
        trusted_roots: Vec<PathBuf>,
    ) -> Result<Self> {
        let trusted_roots = trusted_roots
            .into_iter()
            .map(|root| root.canonicalize())
            .collect::<std::io::Result<HashSet<_>>>()
            .map_err(|e| Error::ConfigError(format!("mapped Python trusted root: {e}")))?;
        Ok(Self {
            wheels,
            environment,
            trusted_roots,
        })
    }

    pub(crate) fn factories(
        &self,
        metadata: &PluginPythonMeta,
        package_root: PathBuf,
    ) -> Result<Vec<MappedPythonFactory>> {
        let mapped = metadata.mapped.as_ref().ok_or_else(|| {
            Error::ConfigError("mapped Python runtime requires [python.mapped] declaration".into())
        })?;
        if mapped.runner_protocol != 3 {
            return Err(Error::ConfigError(format!(
                "mapped Python runner protocol {} is unsupported; expected 3",
                mapped.runner_protocol
            )));
        }
        let package_root = package_root
            .canonicalize()
            .map_err(|e| Error::ConfigError(format!("mapped Python package root: {e}")))?;
        if !self.trusted_roots.contains(&package_root) {
            return Err(Error::ConfigError(format!(
                "mapped Python package root is not host-authorized: {}",
                package_root.display()
            )));
        }
        let module_root = package_root
            .join(&metadata.module_root)
            .canonicalize()
            .map_err(|e| Error::ConfigError(format!("mapped Python module root: {e}")))?;
        if !module_root.starts_with(&package_root) || !module_root.is_dir() {
            return Err(Error::ConfigError(
                "mapped Python module_root must remain inside its package".into(),
            ));
        }
        metadata
            .node_types
            .iter()
            .map(|node_type| {
                let config = MappedPythonConfig {
                    node_type: node_type.clone(),
                    module_root: module_root.clone(),
                    entry_module: metadata.entry_module.clone(),
                    requirements: metadata.requires.clone(),
                    wheels: self.wheels.clone(),
                    environment: self.environment.clone(),
                };
                // SAFETY: this exact package was authorized under new's contract.
                unsafe { MappedPythonFactory::for_sessions(config) }
                    .with_snapshot_inputs(mapped.snapshot_inputs.clone())?
                    .with_snapshot_outputs(mapped.snapshot_outputs.clone())
            })
            .collect()
    }
}
